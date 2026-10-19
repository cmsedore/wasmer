//! Experimental, Unix-only forced cancellation of one process's running Stores.
use super::control_plane::WasiControlPlaneHandle;
use super::process::WasiProcessId;
use super::task_join_handle::OwnedTaskStatus;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use wasmer::Interrupter;
use wasmer_wasix_types::types::Signal;
use wasmer_wasix_types::wasi::ExitCode;

struct ActiveStore {
    interrupter: Interrupter,
    worker: std::thread::ThreadId,
}

#[derive(Default)]
struct State {
    cancelled: bool,
    cancelled_at: Option<Instant>,
    acknowledged: bool,
    next: u64,
    active: BTreeMap<u64, ActiveStore>,
    stopped: Option<Signal>,
    stop_reported: bool,
    stop_notified: bool,
    parked: BTreeSet<u64>,
    quiescent: BTreeSet<u64>,
    stop_waiters: Vec<Waker>,
}

pub(crate) struct ProcessExecution {
    pid: WasiProcessId,
    state: Mutex<State>,
    resumed: Condvar,
    control_plane: Option<WasiControlPlaneHandle>,
    finished: Option<Arc<OwnedTaskStatus>>,
}

impl std::fmt::Debug for ProcessExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("ProcessExecution")
            .field("pid", &self.pid)
            .field("cancelled", &state.cancelled)
            .field("active", &state.active.len())
            .finish()
    }
}

impl ProcessExecution {
    fn all_execution_parked(state: &State) -> bool {
        !state.active.is_empty()
            && state
                .active
                .keys()
                .all(|id| state.parked.contains(id) || state.quiescent.contains(id))
    }

    pub(crate) fn new(pid: WasiProcessId) -> Self {
        Self {
            pid,
            state: Mutex::new(State::default()),
            resumed: Condvar::new(),
            control_plane: None,
            finished: None,
        }
    }

    pub(crate) fn with_control_plane(
        mut self,
        control_plane: WasiControlPlaneHandle,
        finished: Arc<OwnedTaskStatus>,
    ) -> Self {
        self.control_plane = Some(control_plane);
        self.finished = Some(finished);
        self
    }

    pub(crate) fn is_active(&self) -> bool {
        !self.state.lock().unwrap().active.is_empty()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().cancelled
    }

    pub(crate) fn enter(self: &Arc<Self>, interrupter: Interrupter) -> Option<ExecutionGuard> {
        let mut state = self.state.lock().unwrap();
        while state.stopped.is_some() && !state.cancelled {
            state = self.resumed.wait(state).unwrap();
        }
        if state.cancelled {
            return None;
        }
        let id = state.next;
        state.next += 1;
        state.active.insert(
            id,
            ActiveStore {
                interrupter,
                worker: std::thread::current().id(),
            },
        );
        tracing::debug!(pid = %self.pid, id, active = state.active.len(), native_thread = ?std::thread::current().id(), "execution-enter");
        Some(ExecutionGuard {
            execution: self.clone(),
            id,
        })
    }

    fn acknowledge_if_empty(&self, state: &mut State) -> bool {
        if state.cancelled && state.active.is_empty() && !state.acknowledged {
            state.acknowledged = true;
            tracing::debug!(pid = %self.pid,
                elapsed_us = state.cancelled_at.unwrap().elapsed().as_micros(),
                "forced-interrupt-ack");
            return true;
        }
        false
    }

    fn finish_cancelled(&self) {
        if let Some(finished) = &self.finished {
            finished.set_finished(Ok(ExitCode::from(137)));
        }
        if let Some(plane) = self.control_plane.as_ref().and_then(|p| p.upgrade()) {
            plane.notify_parent_child_event(self.pid);
        }
    }

    pub(crate) fn cancel(self: &Arc<Self>) {
        let acknowledged = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            state.stopped = None;
            state.parked.clear();
            self.resumed.notify_all();
            state.cancelled_at = Some(Instant::now());
            tracing::debug!(pid = %self.pid, active = state.active.len(), "forced-interrupt-request");
            self.acknowledge_if_empty(&mut state)
        };
        if acknowledged {
            self.finish_cancelled();
        }
        // Do not wait on the caller's Store: kill(getpid(), SIGKILL) can arrive
        // inside that Store's host import. Retry outside locks until native
        // execution returns, not merely until guest wait() is satisfied.
        let execution = self.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            loop {
                let handles = execution
                    .state
                    .lock()
                    .unwrap()
                    .active
                    .values()
                    .map(|store| store.interrupter.clone())
                    .collect::<Vec<_>>();
                if handles.is_empty() {
                    tracing::debug!(pid = %execution.pid, "interrupt-worker-return");
                    return;
                }
                if start.elapsed() >= Duration::from_secs(2) {
                    tracing::warn!(pid = %execution.pid, active = handles.len(), "forced-interrupt-unacknowledged");
                    return;
                }
                for handle in handles {
                    handle.interrupt();
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });
    }

    pub(crate) fn stop(&self, signal: Signal) {
        let mut state = self.state.lock().unwrap();
        if !state.cancelled {
            state.stopped = Some(signal);
            state.stop_reported = false;
            state.stop_notified = false;
            state.parked.clear();
            tracing::debug!(pid = %self.pid, ?signal, active = state.active.len(), "process-stop-request");
        }
    }

    pub(crate) fn resume(&self) {
        let mut state = self.state.lock().unwrap();
        state.stopped = None;
        state.parked.clear();
        self.resumed.notify_all();
        tracing::debug!(pid = %self.pid, "process-continue");
    }

    /// Park an active Store at a normal host import, preserving its guest stack.
    pub(crate) fn wait_if_stopped(&self) {
        let worker = std::thread::current().id();
        let mut state = self.state.lock().unwrap();
        while state.stopped.is_some() && !state.cancelled {
            let matching: Vec<u64> = state
                .active
                .iter()
                .filter_map(|(&id, store)| (store.worker == worker).then_some(id))
                .collect();
            for id in matching {
                state.parked.insert(id);
            }
            if Self::all_execution_parked(&state) && !state.stop_notified {
                state.stop_notified = true;
                let waiters = std::mem::take(&mut state.stop_waiters);
                tracing::debug!(pid = %self.pid, active = state.active.len(), "process-stopped");
                drop(state);
                for waiter in waiters {
                    waiter.wake();
                }
                if let Some(plane) = self.control_plane.as_ref().and_then(|p| p.upgrade()) {
                    plane.notify_parent_child_event(self.pid);
                }
                state = self.state.lock().unwrap();
            }
            if state.stopped.is_some() && !state.cancelled {
                state = self.resumed.wait(state).unwrap();
            }
        }
    }

    pub(crate) fn try_take_stop(&self) -> Option<Signal> {
        let mut state = self.state.lock().unwrap();
        if state.stopped.is_some() && !state.stop_reported && Self::all_execution_parked(&state) {
            state.stop_reported = true;
            state.stopped
        } else {
            None
        }
    }

    pub(crate) fn poll_stop(&self, cx: &Context<'_>) -> Poll<Signal> {
        if let Some(signal) = self.try_take_stop() {
            return Poll::Ready(signal);
        }
        let mut state = self.state.lock().unwrap();
        if !state.stop_waiters.iter().any(|w| w.will_wake(cx.waker())) {
            state.stop_waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }

    /// An exec image waiting for its replacement is not running guest code.
    pub(crate) fn quiesce_current_worker(self: &Arc<Self>) -> QuiescenceGuard {
        let worker = std::thread::current().id();
        let mut state = self.state.lock().unwrap();
        let ids = state
            .active
            .iter()
            .filter_map(|(&id, store)| (store.worker == worker).then_some(id))
            .collect::<Vec<_>>();
        state.quiescent.extend(ids.iter().copied());
        let notify =
            state.stopped.is_some() && !state.stop_notified && Self::all_execution_parked(&state);
        let waiters = if notify {
            state.stop_notified = true;
            std::mem::take(&mut state.stop_waiters)
        } else {
            Vec::new()
        };
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
        if notify {
            if let Some(plane) = self.control_plane.as_ref().and_then(|p| p.upgrade()) {
                plane.notify_parent_child_event(self.pid);
            }
        }
        QuiescenceGuard {
            execution: self.clone(),
            ids,
        }
    }
}

pub(crate) struct QuiescenceGuard {
    execution: Arc<ProcessExecution>,
    ids: Vec<u64>,
}

impl Drop for QuiescenceGuard {
    fn drop(&mut self) {
        let mut state = self.execution.state.lock().unwrap();
        for id in &self.ids {
            state.quiescent.remove(id);
        }
    }
}

pub(crate) struct ExecutionGuard {
    execution: Arc<ProcessExecution>,
    id: u64,
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        let mut state = self.execution.state.lock().unwrap();
        state.active.remove(&self.id);
        state.parked.remove(&self.id);
        state.quiescent.remove(&self.id);
        let notify_stopped = state.stopped.is_some()
            && !state.stop_notified
            && ProcessExecution::all_execution_parked(&state);
        let waiters = if notify_stopped {
            state.stop_notified = true;
            std::mem::take(&mut state.stop_waiters)
        } else {
            Vec::new()
        };
        tracing::debug!(pid = %self.execution.pid, id = self.id, active = state.active.len(), native_thread = ?std::thread::current().id(), "execution-return");
        let acknowledged = self.execution.acknowledge_if_empty(&mut state);
        drop(state);
        if acknowledged {
            self.execution.finish_cancelled();
        }
        for waiter in waiters {
            waiter.wake();
        }
        if notify_stopped {
            if let Some(plane) = self
                .execution
                .control_plane
                .as_ref()
                .and_then(|p| p.upgrade())
            {
                plane.notify_parent_child_event(self.execution.pid);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_sticky_before_first_execution() {
        let process = Arc::new(ProcessExecution::new(7u32.into()));
        process.cancel();
        assert!(process.state.lock().unwrap().acknowledged);
        assert!(
            process
                .enter(wasmer::Store::default().interrupter())
                .is_none()
        );
        assert!(!process.is_active());
    }

    #[test]
    fn acknowledgement_requires_every_execution_to_return() {
        let process = Arc::new(ProcessExecution::new(7u32.into()));
        let store = wasmer::Store::default();
        let first = process.enter(store.interrupter()).unwrap();
        let second = process.enter(store.interrupter()).unwrap();
        process.cancel();
        assert!(!process.state.lock().unwrap().acknowledged);
        drop(first);
        assert!(process.is_active());
        assert!(!process.state.lock().unwrap().acknowledged);
        drop(second);
        assert!(!process.is_active());
        assert!(process.state.lock().unwrap().acknowledged);
    }

    #[test]
    fn cancelling_one_process_preserves_another_registry() {
        let first = Arc::new(ProcessExecution::new(7u32.into()));
        let second = Arc::new(ProcessExecution::new(8u32.into()));
        let store = wasmer::Store::default();
        let guard = second.enter(store.interrupter()).unwrap();
        first.cancel();
        assert!(second.is_active());
        assert!(!second.state.lock().unwrap().cancelled);
        drop(guard);
    }
}
