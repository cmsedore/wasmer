use std::task::Waker;

use serde::{Deserialize, Serialize};
use wasmer::FromToNativeWasmType;
use wasmer_wasix_types::wasi::{JoinFlags, JoinStatus, JoinStatusType, JoinStatusUnion, OptionPid};

use super::*;
use crate::{WasiProcess, syscalls::*};

#[derive(Serialize, Deserialize)]
enum JoinStatusResult {
    Nothing,
    ExitNormal(WasiProcessId, ExitCode),
    ExitSignal(WasiProcessId, Signal),
    Stopped(WasiProcessId, Signal),
    Err(Errno),
}

async fn wait_child_event(process: WasiProcess, wake_stopped: bool) -> JoinStatusResult {
    let pid = process.pid();
    #[cfg(all(unix, feature = "experimental-host-interrupt"))]
    if wake_stopped {
        use futures::future::{Either, select};
        return match select(
            Box::pin(process.join()),
            Box::pin(process.wait_for_stop_signal()),
        )
        .await
        {
            Either::Left((status, _)) => {
                finished_child_event(&process, status.unwrap_or_else(|_| Errno::Child.into()))
            }
            Either::Right((signal, _)) => JoinStatusResult::Stopped(pid, signal),
        };
    }
    let _ = wake_stopped;
    finished_child_event(
        &process,
        process.join().await.unwrap_or_else(|_| Errno::Child.into()),
    )
}

fn ready_child_event(process: &WasiProcess, wake_stopped: bool) -> Option<JoinStatusResult> {
    if let Some(status) = process.try_join() {
        return Some(finished_child_event(
            process,
            status.unwrap_or_else(|_| Errno::Child.into()),
        ));
    }
    #[cfg(all(unix, feature = "experimental-host-interrupt"))]
    if wake_stopped {
        if let Some(signal) = process.try_take_stop_signal() {
            return Some(JoinStatusResult::Stopped(process.pid(), signal));
        }
    }
    let _ = wake_stopped;
    None
}

fn finished_child_event(process: &WasiProcess, status: ExitCode) -> JoinStatusResult {
    match process.termination_signal() {
        Some(signal) => JoinStatusResult::ExitSignal(process.pid(), signal),
        None => JoinStatusResult::ExitNormal(process.pid(), status),
    }
}

fn finish_child_event(parent: &WasiProcess, result: JoinStatusResult) -> JoinStatusResult {
    if let JoinStatusResult::ExitNormal(pid, _) | JoinStatusResult::ExitSignal(pid, _) = result {
        parent
            .inner
            .0
            .lock()
            .unwrap()
            .children
            .retain(|child| child.pid() != pid);
    }
    result
}

/// ### `proc_join()`
/// Joins the child process, blocking this one until the other finishes
///
/// ## Parameters
///
/// * `pid` - Handle of the child process to wait on
//#[instrument(level = "trace", skip_all, fields(pid = ctx.data().process.pid().raw()), ret)]
pub fn proc_join<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    WasiEnv::do_pending_operations(&mut ctx)?;

    proc_join_internal(ctx, pid_ptr, flags, status_ptr)
}

pub(super) fn proc_join_internal<M: MemorySize + 'static>(
    mut ctx: FunctionEnvMut<'_, WasiEnv>,
    pid_ptr: WasmPtr<OptionPid, M>,
    flags: JoinFlags,
    status_ptr: WasmPtr<JoinStatus, M>,
) -> Result<Errno, WasiError> {
    ctx = wasi_try_ok!(maybe_snapshot::<M>(ctx)?);

    // This lambda will look at what we wrote in the status variable
    // and use this to determine the return code sent back to the caller
    let ret_result = {
        move |ctx: FunctionEnvMut<'_, WasiEnv>, status: JoinStatusResult| {
            let mut ret = Errno::Success;

            let view = unsafe { ctx.data().memory_view(&ctx) };
            let status = match status {
                JoinStatusResult::Nothing => JoinStatus {
                    tag: JoinStatusType::Nothing,
                    u: JoinStatusUnion { nothing: 0 },
                },
                JoinStatusResult::ExitNormal(pid, exit_code) => {
                    let option_pid = OptionPid {
                        tag: OptionTag::Some,
                        pid: pid.raw() as Pid,
                    };
                    pid_ptr.write(&view, option_pid).ok();

                    JoinStatus {
                        tag: JoinStatusType::ExitNormal,
                        u: JoinStatusUnion {
                            exit_normal: exit_code.raw() as u16,
                        },
                    }
                }
                JoinStatusResult::ExitSignal(pid, signal) => {
                    pid_ptr
                        .write(
                            &view,
                            OptionPid {
                                tag: OptionTag::Some,
                                pid: pid.raw() as Pid,
                            },
                        )
                        .ok();
                    JoinStatus {
                        tag: JoinStatusType::ExitSignal,
                        u: JoinStatusUnion {
                            exit_signal: wasmer_wasix_types::wasi::ErrnoSignal {
                                signal,
                                exit_code: Errno::Success,
                            },
                        },
                    }
                }
                JoinStatusResult::Stopped(pid, signal) => {
                    pid_ptr
                        .write(
                            &view,
                            OptionPid {
                                tag: OptionTag::Some,
                                pid: pid.raw() as Pid,
                            },
                        )
                        .ok();
                    JoinStatus {
                        tag: JoinStatusType::Stopped,
                        u: JoinStatusUnion { stopped: signal },
                    }
                }
                JoinStatusResult::Err(err) => {
                    ret = err;
                    JoinStatus {
                        tag: JoinStatusType::Nothing,
                        u: JoinStatusUnion { nothing: 0 },
                    }
                }
            };
            wasi_try_mem_ok!(status_ptr.write(&view, status));
            Ok(ret)
        }
    };

    // If we were just restored the stack then we were woken after a deep sleep
    // and the return values are already set
    if let Some(status) = unsafe { handle_rewind::<M, _>(&mut ctx) } {
        let ret = ret_result(ctx, status);
        tracing::trace!("rewound join ret={:?}", ret);
        return ret;
    }

    let env = ctx.data();
    let memory = unsafe { env.memory_view(&ctx) };
    let option_pid = wasi_try_mem_ok!(pid_ptr.read(&memory));
    let option_pid = match option_pid.tag {
        OptionTag::None => None,
        OptionTag::Some => Some(option_pid.pid),
        _ => return Ok(Errno::Inval),
    };
    tracing::trace!("filter_pid = {:?}", option_pid);

    // Clear the existing values (in case something goes wrong)
    wasi_try_mem_ok!(pid_ptr.write(
        &memory,
        OptionPid {
            tag: OptionTag::None,
            pid: 0,
        }
    ));
    wasi_try_mem_ok!(status_ptr.write(
        &memory,
        JoinStatus {
            tag: JoinStatusType::Nothing,
            u: JoinStatusUnion { nothing: 0 },
        }
    ));

    let parent = ctx.data().process.clone();
    let children = match option_pid {
        None => parent.inner.0.lock().unwrap().children.clone(),
        Some(raw_pid) => {
            let pid: WasiProcessId = raw_pid.into();
            let child = parent
                .inner
                .0
                .lock()
                .unwrap()
                .children
                .iter()
                .find(|child| child.pid() == pid)
                .cloned()
                .or_else(|| ctx.data().control_plane.get_process(pid));
            child.into_iter().collect()
        }
    };
    if children.is_empty() {
        return ret_result(ctx, JoinStatusResult::Err(Errno::Child));
    }
    let wake_stopped = flags.contains(JoinFlags::WAKE_STOPPED);
    for child in &children {
        if let Some(result) = ready_child_event(child, wake_stopped) {
            return ret_result(ctx, finish_child_event(&parent, result));
        }
    }
    if flags.contains(JoinFlags::NON_BLOCKING) {
        return ret_result(ctx, JoinStatusResult::Nothing);
    }
    let res = __asyncify_with_deep_sleep::<M, _, _>(ctx, async move {
        let waits = children
            .into_iter()
            .map(|child| Box::pin(wait_child_event(child, wake_stopped)))
            .collect::<Vec<_>>();
        let result = futures::future::select_all(waits).await.0;
        finish_child_event(&parent, result)
    })?;
    match res {
        AsyncifyAction::Finish(ctx, result) => ret_result(ctx, result),
        AsyncifyAction::Unwind => Ok(Errno::Success),
    }
}
