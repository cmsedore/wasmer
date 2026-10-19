use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{WasiProcess, WasiProcessId};
use wasmer_types::ModuleHash;
use wasmer_wasix_types::wasi::{Errno, Signal};

#[derive(Debug, Clone)]
pub struct WasiControlPlane {
    state: Arc<State>,
}

#[derive(Debug, Clone)]
pub struct WasiControlPlaneHandle {
    inner: std::sync::Weak<State>,
}

impl WasiControlPlaneHandle {
    fn new(inner: &Arc<State>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }

    pub fn upgrade(&self) -> Option<WasiControlPlane> {
        self.inner.upgrade().map(|state| WasiControlPlane { state })
    }

    pub fn must_upgrade(&self) -> WasiControlPlane {
        let state = self.inner.upgrade().expect("control plane unavailable");
        WasiControlPlane { state }
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    /// Total number of tasks (processes + threads) that can be spawned.
    pub max_task_count: Option<usize>,
    /// Flag that indicates if asynchronous threading is enables (opt-in)
    pub enable_asynchronous_threading: bool,
    /// Enables an exponential backoff of the process CPU usage when there
    /// are no active run tokens (when set holds the maximum amount of
    /// time that it will pause the CPU)
    /// (default = off)
    pub enable_exponential_cpu_backoff: Option<Duration>,
}

impl ControlPlaneConfig {
    pub fn new() -> Self {
        Self {
            max_task_count: None,
            enable_asynchronous_threading: false,
            enable_exponential_cpu_backoff: None,
        }
    }
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct State {
    config: ControlPlaneConfig,

    /// Total number of active tasks (threads) across all processes.
    task_count: Arc<AtomicUsize>,

    /// Mutable state.
    mutable: RwLock<MutableState>,
}

#[derive(Debug)]
struct MutableState {
    /// Seed used to generate process ID's
    process_seed: u32,
    /// The processes running on this machine
    processes: HashMap<WasiProcessId, WasiProcess>,
    groups: HashMap<WasiProcessId, ProcessGroupInfo>,
    foreground_group: Option<u32>,
    groups_enabled: bool,
    // TODO: keep a queue of terminated process ids for id reuse.
}

#[derive(Debug, Clone, Copy)]
struct ProcessGroupInfo {
    parent: Option<WasiProcessId>,
    group: u32,
    session: u32,
}

impl WasiControlPlane {
    pub fn new(config: ControlPlaneConfig) -> Self {
        Self {
            state: Arc::new(State {
                config,
                task_count: Arc::new(AtomicUsize::new(0)),
                mutable: RwLock::new(MutableState {
                    process_seed: 0,
                    processes: Default::default(),
                    groups: Default::default(),
                    foreground_group: None,
                    groups_enabled: false,
                }),
            }),
        }
    }

    pub fn handle(&self) -> WasiControlPlaneHandle {
        WasiControlPlaneHandle::new(&self.state)
    }

    /// Get the current count of active tasks (threads).
    fn active_task_count(&self) -> usize {
        self.state.task_count.load(Ordering::SeqCst)
    }

    /// Returns the configuration for this control plane
    pub(crate) fn config(&self) -> &ControlPlaneConfig {
        &self.state.config
    }

    /// Register a new task.
    ///
    // Currently just increments the task counter.
    pub(crate) fn register_task(&self) -> Result<TaskCountGuard, ControlPlaneError> {
        let count = self.state.task_count.fetch_add(1, Ordering::SeqCst);
        if let Some(max) = self.state.config.max_task_count
            && count > max
        {
            self.state.task_count.fetch_sub(1, Ordering::SeqCst);
            return Err(ControlPlaneError::TaskLimitReached { max: count });
        }
        Ok(TaskCountGuard(self.state.task_count.clone()))
    }

    /// Creates a new process
    // FIXME: De-register terminated processes!
    // Currently they just accumulate.
    pub fn new_process(&self, module_hash: ModuleHash) -> Result<WasiProcess, ControlPlaneError> {
        self.new_process_inner(module_hash, None)
    }

    pub fn new_child_process(
        &self,
        module_hash: ModuleHash,
        parent: WasiProcessId,
    ) -> Result<WasiProcess, ControlPlaneError> {
        self.new_process_inner(module_hash, Some(parent))
    }

    fn new_process_inner(
        &self,
        module_hash: ModuleHash,
        parent: Option<WasiProcessId>,
    ) -> Result<WasiProcess, ControlPlaneError> {
        if let Some(max) = self.state.config.max_task_count
            && self.active_task_count() >= max
        {
            // NOTE: task count is not incremented here, only when new threads are spawned.
            // A process will always have a main thread.
            return Err(ControlPlaneError::TaskLimitReached { max });
        }

        // Create the process first to do all the allocations before locking.
        let mut proc = WasiProcess::new(WasiProcessId::from(0), module_hash, self.handle());

        let mut mutable = self.state.mutable.write().unwrap();

        let pid = mutable.next_process_id()?;
        proc.set_pid(pid);
        let inherited = parent.and_then(|p| mutable.groups.get(&p).copied());
        mutable.groups.insert(
            pid,
            ProcessGroupInfo {
                parent,
                group: inherited.map_or(pid.raw(), |g| g.group),
                session: inherited.map_or(pid.raw(), |g| g.session),
            },
        );
        mutable.foreground_group.get_or_insert(pid.raw());
        mutable.processes.insert(pid, proc.clone());
        Ok(proc)
    }

    pub fn process_group(&self, pid: WasiProcessId) -> Result<u32, Errno> {
        let mut state = self.state.mutable.write().unwrap();
        state.groups_enabled = true;
        state.groups.get(&pid).map(|g| g.group).ok_or(Errno::Srch)
    }

    pub fn set_process_group(
        &self,
        caller: WasiProcessId,
        pid: WasiProcessId,
        group: u32,
    ) -> Result<(), Errno> {
        let mut state = self.state.mutable.write().unwrap();
        let source = *state.groups.get(&caller).ok_or(Errno::Srch)?;
        let target = *state.groups.get(&pid).ok_or(Errno::Srch)?;
        if caller != pid && target.parent != Some(caller) {
            return Err(Errno::Srch);
        }
        if source.session != target.session
            || (pid.raw() == target.session && group != target.group)
        {
            return Err(Errno::Perm);
        }
        if group != pid.raw()
            && !state
                .groups
                .values()
                .any(|g| g.group == group && g.session == target.session)
        {
            return Err(Errno::Perm);
        }
        state.groups.get_mut(&pid).unwrap().group = group;
        state.groups_enabled = true;
        Ok(())
    }

    pub fn foreground_group(&self) -> Result<u32, Errno> {
        self.state
            .mutable
            .read()
            .unwrap()
            .foreground_group
            .ok_or(Errno::Notty)
    }

    pub fn set_foreground_group(&self, caller: WasiProcessId, group: u32) -> Result<(), Errno> {
        let mut state = self.state.mutable.write().unwrap();
        let session = state.groups.get(&caller).ok_or(Errno::Srch)?.session;
        if !state
            .groups
            .values()
            .any(|g| g.group == group && g.session == session)
        {
            return Err(Errno::Perm);
        }
        state.foreground_group = Some(group);
        state.groups_enabled = true;
        Ok(())
    }

    pub fn signal_process_target(
        &self,
        caller: WasiProcessId,
        target: i32,
        signal: Signal,
    ) -> Result<(), Errno> {
        let targets = {
            let state = self.state.mutable.read().unwrap();
            if target == -1 || target == i32::MIN {
                return Err(Errno::Notsup);
            }
            let group = if target == 0 {
                Some(state.groups.get(&caller).ok_or(Errno::Srch)?.group)
            } else if target < -1 {
                Some((-target) as u32)
            } else {
                None
            };
            state
                .processes
                .iter()
                .filter(|(pid, process)| {
                    (process.try_join().is_none() || process.has_active_execution())
                        && match group {
                            Some(group) => state.groups.get(pid).is_some_and(|g| g.group == group),
                            None => pid.raw() == target as u32,
                        }
                })
                .map(|(_, process)| process.clone())
                .collect::<Vec<_>>()
        };
        if targets.is_empty() {
            return Err(Errno::Srch);
        }
        for process in targets {
            // A group signal targets group members, never their unrelated children.
            if signal != Signal::Signone {
                process.signal_process_only(signal);
            }
        }
        Ok(())
    }

    pub fn signal_foreground(&self, signal: Signal) -> bool {
        let group = {
            let state = self.state.mutable.read().unwrap();
            if !state.groups_enabled {
                return false;
            }
            state.foreground_group
        };
        if let Some(group) = group {
            let _ = self.signal_process_target(0u32.into(), -(group as i32), signal);
        }
        true
    }

    #[cfg(all(unix, feature = "experimental-host-interrupt"))]
    pub(crate) fn notify_parent_child_event(&self, pid: WasiProcessId) {
        let parent = {
            let state = self.state.mutable.read().unwrap();
            let parent_pid = state.groups.get(&pid).and_then(|info| info.parent);
            parent_pid.and_then(|id| state.processes.get(&id).cloned())
        };
        if let Some(parent) = parent {
            parent.signal_process_only(Signal::Sigchld);
        }
    }

    /// Generates a new process ID
    pub fn generate_id(&self) -> Result<WasiProcessId, ControlPlaneError> {
        let mut mutable = self.state.mutable.write().unwrap();
        mutable.next_process_id()
    }

    /// Gets a reference to a running process
    pub fn get_process(&self, pid: WasiProcessId) -> Option<WasiProcess> {
        self.state
            .mutable
            .read()
            .unwrap()
            .processes
            .get(&pid)
            .cloned()
    }
}

impl MutableState {
    fn next_process_id(&mut self) -> Result<WasiProcessId, ControlPlaneError> {
        // TODO: reuse terminated ids, handle wrap-around, ...
        let id = self.process_seed.checked_add(1).ok_or({
            ControlPlaneError::TaskLimitReached {
                max: u32::MAX as usize,
            }
        })?;
        self.process_seed = id;
        Ok(WasiProcessId::from(id))
    }
}

impl Default for WasiControlPlane {
    fn default() -> Self {
        let config = ControlPlaneConfig::default();
        Self::new(config)
    }
}

/// Guard that ensures the [`WasiControlPlane`] task counter is decremented when dropped.
#[derive(Debug)]
pub struct TaskCountGuard(Arc<AtomicUsize>);

impl Drop for TaskCountGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(thiserror::Error, PartialEq, Eq, Clone, Debug)]
pub enum ControlPlaneError {
    /// The maximum number of execution tasks has been reached.
    #[error("The maximum number of execution tasks has been reached ({max})")]
    TaskLimitReached {
        /// The maximum number of tasks.
        max: usize,
    },
}

#[cfg(test)]
mod tests {
    use wasmer_wasix_types::wasix::ThreadStartType;

    use crate::os::task::thread::WasiMemoryLayout;

    use super::*;

    #[cfg(all(unix, feature = "experimental-host-interrupt"))]
    #[test]
    fn test_finished_process_with_active_execution_can_still_be_killed() {
        let plane = WasiControlPlane::default();
        let process = plane.new_process(ModuleHash::random()).unwrap();
        let execution = process.inner.0.lock().unwrap().execution.clone();
        let store = wasmer::Store::default();
        let guard = execution.enter(store.interrupter()).unwrap();
        process.finished.set_finished(Ok(0i32.into()));
        assert!(process.try_join().is_some());
        assert!(process.has_active_execution());
        plane
            .signal_process_target(process.pid(), process.pid().raw() as i32, Signal::Sigkill)
            .unwrap();
        assert!(execution.enter(store.interrupter()).is_none());
        drop(guard);
        assert!(!process.has_active_execution());
        assert_eq!(
            plane.signal_process_target(process.pid(), process.pid().raw() as i32, Signal::Signone),
            Err(Errno::Srch)
        );
    }

    #[test]
    fn test_process_groups_inherit_and_signal_only_members() {
        let plane = WasiControlPlane::default();
        let root = plane.new_process(ModuleHash::random()).unwrap();
        let first = plane
            .new_child_process(ModuleHash::random(), root.pid())
            .unwrap();
        let other = plane
            .new_child_process(ModuleHash::random(), root.pid())
            .unwrap();
        assert_eq!(plane.process_group(first.pid()).unwrap(), root.pid().raw());
        plane
            .set_process_group(root.pid(), first.pid(), first.pid().raw())
            .unwrap();
        let descendant = plane
            .new_child_process(ModuleHash::random(), first.pid())
            .unwrap();
        assert_eq!(
            plane.process_group(descendant.pid()).unwrap(),
            first.pid().raw()
        );
        let first_thread = first
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let descendant_thread = descendant
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let other_thread = other
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        plane
            .signal_process_target(root.pid(), -(first.pid().raw() as i32), Signal::Sigterm)
            .unwrap();
        assert_eq!(
            first_thread.as_thread().pop_signals(),
            vec![Signal::Sigterm]
        );
        assert_eq!(
            descendant_thread.as_thread().pop_signals(),
            vec![Signal::Sigterm]
        );
        assert!(other_thread.as_thread().pop_signals().is_empty());
    }

    #[test]
    fn test_foreground_signal_excludes_background_and_shell() {
        let plane = WasiControlPlane::default();
        let shell = plane.new_process(ModuleHash::random()).unwrap();
        let foreground = plane
            .new_child_process(ModuleHash::random(), shell.pid())
            .unwrap();
        let background = plane
            .new_child_process(ModuleHash::random(), shell.pid())
            .unwrap();
        plane
            .set_process_group(shell.pid(), foreground.pid(), foreground.pid().raw())
            .unwrap();
        plane
            .set_process_group(shell.pid(), background.pid(), background.pid().raw())
            .unwrap();
        let a = foreground
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let b = background
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let s = shell
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        plane
            .set_foreground_group(shell.pid(), foreground.pid().raw())
            .unwrap();
        assert!(plane.signal_foreground(Signal::Sigint));
        assert_eq!(a.as_thread().pop_signals(), vec![Signal::Sigint]);
        assert!(b.as_thread().pop_signals().is_empty());
        assert!(s.as_thread().pop_signals().is_empty());
    }

    #[test]
    fn test_group_assignment_and_missing_targets_are_rejected() {
        let plane = WasiControlPlane::default();
        let shell = plane.new_process(ModuleHash::random()).unwrap();
        let a = plane
            .new_child_process(ModuleHash::random(), shell.pid())
            .unwrap();
        let b = plane
            .new_child_process(ModuleHash::random(), shell.pid())
            .unwrap();
        assert_eq!(
            plane.set_process_group(a.pid(), b.pid(), b.pid().raw()),
            Err(Errno::Srch)
        );
        assert_eq!(
            plane.set_process_group(a.pid(), a.pid(), 99999),
            Err(Errno::Perm)
        );
        assert_eq!(
            plane.signal_process_target(shell.pid(), -99999, Signal::Sigterm),
            Err(Errno::Srch)
        );
        assert_eq!(
            plane.signal_process_target(shell.pid(), i32::MIN, Signal::Sigterm),
            Err(Errno::Notsup)
        );
    }

    /// Simple test to ensure task limits are respected.
    #[test]
    fn test_control_plane_task_limits() {
        let p = WasiControlPlane::new(ControlPlaneConfig {
            max_task_count: Some(2),
            enable_asynchronous_threading: false,
            enable_exponential_cpu_backoff: None,
        });

        let p1 = p.new_process(ModuleHash::random()).unwrap();
        let _t1 = p1
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let _t2 = p1
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();

        assert_eq!(
            p.new_process(ModuleHash::random()).unwrap_err(),
            ControlPlaneError::TaskLimitReached { max: 2 }
        );
    }

    /// Simple test to ensure task limits are respected and that thread drop guards work.
    #[test]
    fn test_control_plane_task_limits_with_dropped_threads() {
        let p = WasiControlPlane::new(ControlPlaneConfig {
            max_task_count: Some(2),
            enable_asynchronous_threading: false,
            enable_exponential_cpu_backoff: None,
        });

        let p1 = p.new_process(ModuleHash::random()).unwrap();

        for _ in 0..10 {
            let _thread = p1
                .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
                .unwrap();
        }

        let _t1 = p1
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();
        let _t2 = p1
            .new_thread(WasiMemoryLayout::default(), ThreadStartType::MainThread)
            .unwrap();

        assert_eq!(
            p.new_process(ModuleHash::random()).unwrap_err(),
            ControlPlaneError::TaskLimitReached { max: 2 }
        );
    }
}
