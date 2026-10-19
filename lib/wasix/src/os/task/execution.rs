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
    interrupter: Option<Interrupter>,
    worker: Option<std::thread::ThreadId>,
}

#[derive(Default)]
struct State {
    cancelled: bool,
    cancel_code: Option<ExitCode>,
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
    async_waiters: Vec<Waker>,
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

    pub(crate) fn cancellation_code(&self) -> ExitCode {
        self.state
            .lock()
            .unwrap()
            .cancel_code
            .unwrap_or(137i32.into())
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
                interrupter: Some(interrupter),
                worker: Some(std::thread::current().id()),
            },
        );
        tracing::debug!(pid = %self.pid, id, active = state.active.len(), native_thread = ?std::thread::current().id(), "execution-enter");
        Some(ExecutionGuard {
            execution: self.clone(),
            id,
        })
    }

    pub(crate) fn enter_async(self: &Arc<Self>) -> Option<ExecutionGuard> {
        let mut state = self.state.lock().unwrap();
        if state.cancelled {
            return None;
        }
        let id = state.next;
        state.next += 1;
        state.active.insert(
            id,
            ActiveStore {
                interrupter: None,
                worker: None,
            },
        );
        Some(ExecutionGuard {
            execution: self.clone(),
            id,
        })
    }

    pub(crate) fn poll_async(&self, id: u64, cx: &mut Context<'_>) -> Poll<Result<(), ExitCode>> {
        let mut state = self.state.lock().unwrap();
        if !state.async_waiters.iter().any(|w| w.will_wake(cx.waker())) {
            state.async_waiters.push(cx.waker().clone());
        }
        if state.cancelled {
            return Poll::Ready(Err(state.cancel_code.unwrap_or(137i32.into())));
        }
        if state.stopped.is_none() {
            return Poll::Ready(Ok(()));
        }
        state.parked.insert(id);
        let notify = Self::all_execution_parked(&state) && !state.stop_notified;
        let waiters = if notify {
            state.stop_notified = true;
            std::mem::take(&mut state.stop_waiters)
        } else {
            vec![]
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
        Poll::Pending
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
            finished.set_finished(Ok(self.cancellation_code()));
        }
        if let Some(plane) = self.control_plane.as_ref().and_then(|p| p.upgrade()) {
            plane.notify_parent_child_event(self.pid);
        }
    }

    pub(crate) fn cancel(self: &Arc<Self>) {
        self.cancel_with_code(137i32.into());
    }

    pub(crate) fn cancel_with_code(self: &Arc<Self>, code: ExitCode) {
        let (acknowledged, waiters) = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            state.cancel_code = Some(code);
            state.stopped = None;
            state.parked.clear();
            self.resumed.notify_all();
            state.cancelled_at = Some(Instant::now());
            let waiters = std::mem::take(&mut state.async_waiters);
            tracing::debug!(pid = %self.pid, active = state.active.len(), "forced-interrupt-request");
            (self.acknowledge_if_empty(&mut state), waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
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
                    .filter_map(|store| store.interrupter.clone())
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
            let waiters = std::mem::take(&mut state.async_waiters);
            drop(state);
            for waiter in waiters {
                waiter.wake();
            }
        }
    }

    pub(crate) fn resume(&self) {
        let mut state = self.state.lock().unwrap();
        state.stopped = None;
        state.parked.clear();
        self.resumed.notify_all();
        tracing::debug!(pid = %self.pid, "process-continue");
        let waiters = std::mem::take(&mut state.async_waiters);
        drop(state);
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Park an active Store at a normal host import, preserving its guest stack.
    pub(crate) fn wait_if_stopped(&self) {
        let worker = std::thread::current().id();
        let mut state = self.state.lock().unwrap();
        while state.stopped.is_some() && !state.cancelled {
            let matching: Vec<u64> = state
                .active
                .iter()
                .filter_map(|(&id, store)| (store.worker == Some(worker)).then_some(id))
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
            .filter_map(|(&id, store)| (store.worker == Some(worker)).then_some(id))
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

impl ExecutionGuard {
    pub(crate) fn poll_permitted(&self, cx: &mut Context<'_>) -> Poll<Result<(), ExitCode>> {
        self.execution.poll_async(self.id, cx)
    }

    pub(crate) fn commit_if_permitted<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<Option<T>, ExitCode> {
        let state = self.execution.state.lock().unwrap();
        if state.cancelled {
            return Err(state.cancel_code.unwrap_or(137i32.into()));
        }
        if state.stopped.is_some() {
            return Ok(None);
        }
        Ok(Some(commit()))
    }
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
    fn async_stop_requires_all_threads_and_cancellation_blocks_commit() {
        let process = Arc::new(ProcessExecution::new(9u32.into()));
        let first = process.enter_async().unwrap();
        let second = process.enter_async().unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        process.stop(Signal::Sigstop);
        assert!(first.poll_permitted(&mut cx).is_pending());
        assert_eq!(process.try_take_stop(), None);
        assert!(second.poll_permitted(&mut cx).is_pending());
        assert_eq!(process.try_take_stop(), Some(Signal::Sigstop));
        assert_eq!(first.commit_if_permitted(|| 17).unwrap(), None);
        process.resume();
        assert!(first.poll_permitted(&mut cx).is_ready());
        assert_eq!(first.commit_if_permitted(|| 17).unwrap(), Some(17));
        process.cancel();
        assert!(
            first
                .commit_if_permitted(|| panic!("commit after kill"))
                .is_err()
        );
        drop(first);
        drop(second);
        assert!(process.state.lock().unwrap().acknowledged);
    }

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
    fn signal_cancellation_preserves_reason_until_every_worker_returns() {
        let finished = Arc::new(OwnedTaskStatus::new(
            super::super::task_join_handle::TaskStatus::Running,
        ));
        let mut execution = ProcessExecution::new(10u32.into());
        execution.finished = Some(finished.clone());
        let process = Arc::new(execution);
        let first = process.enter_async().unwrap();
        let second = process.enter_async().unwrap();
        process.cancel_with_code(141i32.into());
        process.cancel();
        assert_eq!(process.cancellation_code(), 141i32.into());
        assert!(finished.status().is_running());
        assert_eq!(first.commit_if_permitted(|| 1), Err(141i32.into()));
        drop(first);
        assert!(finished.status().is_running());
        drop(second);
        assert_eq!(
            finished.status().into_finished().unwrap().unwrap(),
            141i32.into()
        );
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
