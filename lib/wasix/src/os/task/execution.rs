//! Experimental, Unix-only forced cancellation of one process's running Stores.
use super::process::WasiProcessId;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use wasmer::Interrupter;

#[derive(Default)]
struct State {
    cancelled: bool,
    cancelled_at: Option<Instant>,
    acknowledged: bool,
    next: u64,
    active: BTreeMap<u64, Interrupter>,
}

pub(crate) struct ProcessExecution {
    pid: WasiProcessId,
    state: Mutex<State>,
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
    pub(crate) fn new(pid: WasiProcessId) -> Self {
        Self {
            pid,
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        !self.state.lock().unwrap().active.is_empty()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().cancelled
    }

    pub(crate) fn enter(self: &Arc<Self>, interrupter: Interrupter) -> Option<ExecutionGuard> {
        let mut state = self.state.lock().unwrap();
        if state.cancelled {
            return None;
        }
        let id = state.next;
        state.next += 1;
        state.active.insert(id, interrupter);
        tracing::debug!(pid = %self.pid, id, active = state.active.len(), native_thread = ?std::thread::current().id(), "execution-enter");
        Some(ExecutionGuard {
            execution: self.clone(),
            id,
        })
    }

    fn acknowledge_if_empty(&self, state: &mut State) {
        if state.cancelled && state.active.is_empty() && !state.acknowledged {
            state.acknowledged = true;
            tracing::debug!(pid = %self.pid,
                elapsed_us = state.cancelled_at.unwrap().elapsed().as_micros(),
                "forced-interrupt-ack");
        }
    }

    pub(crate) fn cancel(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return;
            }
            state.cancelled = true;
            state.cancelled_at = Some(Instant::now());
            tracing::debug!(pid = %self.pid, active = state.active.len(), "forced-interrupt-request");
            self.acknowledge_if_empty(&mut state);
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
                    .cloned()
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
}

pub(crate) struct ExecutionGuard {
    execution: Arc<ProcessExecution>,
    id: u64,
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        let mut state = self.execution.state.lock().unwrap();
        state.active.remove(&self.id);
        tracing::debug!(pid = %self.execution.pid, id = self.id, active = state.active.len(), native_thread = ?std::thread::current().id(), "execution-return");
        self.execution.acknowledge_if_empty(&mut state);
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
