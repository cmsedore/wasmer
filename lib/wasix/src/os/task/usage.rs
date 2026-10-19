//! Per-process worker CPU and syscall-wait accounting. No guest-memory borrows.
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    thread::ThreadId,
    time::Instant,
};

#[derive(Clone, Copy)]
struct Clock {
    #[cfg(target_os = "linux")]
    id: libc::clockid_t,
}
impl Clock {
    fn current() -> Option<Self> {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut id = 0;
            if libc::pthread_getcpuclockid(libc::pthread_self(), &mut id) == 0 {
                return Some(Self { id });
            }
        }
        None
    }
    fn read(self) -> Option<u64> {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut time = std::mem::zeroed::<libc::timespec>();
            if libc::clock_gettime(self.id, &mut time) == 0 {
                return Some(time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64);
            }
        }
        None
    }
}
struct Segment {
    clock: Option<Clock>,
    since: u64,
}
#[derive(Default)]
struct State {
    cpu_ns: u64,
    clock_failed: bool,
    active: HashMap<ThreadId, Segment>,
    next_wait: u64,
    waits: HashMap<u64, (ThreadId, &'static str)>,
}

pub struct ProcessUsage {
    born: Instant,
    state: Mutex<State>,
}
impl std::fmt::Debug for ProcessUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessUsage").finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub struct UsageSnapshot {
    pub cpu_time_ns: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub elapsed_ns: u64,
    pub running_workers: usize,
    pub wait_reasons: Vec<&'static str>,
}
thread_local! {
    static OWNERS: RefCell<Vec<(u64, Arc<ProcessUsage>)>> = const { RefCell::new(Vec::new()) };
}

impl ProcessUsage {
    pub fn new() -> Self {
        Self {
            born: Instant::now(),
            state: Mutex::new(State::default()),
        }
    }
    fn attach(&self) {
        let clock = Clock::current();
        let since = clock.and_then(Clock::read).unwrap_or(0);
        self.state
            .lock()
            .unwrap()
            .active
            .insert(std::thread::current().id(), Segment { clock, since });
    }
    fn detach(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(segment) = state.active.remove(&std::thread::current().id()) {
            let reading = segment.clock.and_then(Clock::read);
            state.clock_failed |= reading.is_none();
            state.cpu_ns += reading
                .unwrap_or(segment.since)
                .saturating_sub(segment.since);
        }
    }
    /// Charge only this lexical execution/poll. Nested owners temporarily replace
    /// their caller, so pooled workers and re-entrant guest calls cannot double bill.
    pub fn enter(self: &Arc<Self>) -> UsageGuard {
        static NEXT_SCOPE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_SCOPE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let previous = OWNERS.with(|owners| {
            let mut owners = owners.borrow_mut();
            let previous = owners.last().map(|(_, usage)| usage.clone());
            owners.push((id, self.clone()));
            previous
        });
        if let Some(previous) = previous {
            previous.detach();
        }
        self.attach();
        UsageGuard {
            id,
            _thread_bound: std::marker::PhantomData,
        }
    }
    /// vfork temporarily changes the process owning the current Wasm context.
    pub fn rebind_current(self: &Arc<Self>) {
        let previous = OWNERS.with(|owners| {
            let mut owners = owners.borrow_mut();
            owners
                .last_mut()
                .map(|(_, usage)| std::mem::replace(usage, self.clone()))
        });
        if let Some(previous) = previous {
            previous.detach();
            self.attach();
        }
    }
    pub fn waiting(self: &Arc<Self>, reason: &'static str) -> WaitGuard {
        let mut state = self.state.lock().unwrap();
        let id = state.next_wait;
        state.next_wait += 1;
        state
            .waits
            .insert(id, (std::thread::current().id(), reason));
        WaitGuard {
            usage: self.clone(),
            id,
        }
    }
    /// A signal callback runs guest code above an older suspended syscall.
    pub fn guest_callback(self: &Arc<Self>) -> CallbackGuard {
        let worker = std::thread::current().id();
        let mut state = self.state.lock().unwrap();
        let ids = state
            .waits
            .iter()
            .filter_map(|(id, (thread, _))| (*thread == worker).then_some(*id))
            .collect::<Vec<_>>();
        let paused = ids
            .into_iter()
            .map(|id| (id, state.waits.remove(&id).unwrap()))
            .collect();
        CallbackGuard {
            usage: self.clone(),
            paused,
        }
    }
    pub fn snapshot(&self) -> UsageSnapshot {
        let state = self.state.lock().unwrap();
        let mut cpu = state.cpu_ns;
        let mut clock_failed = state.clock_failed;
        for segment in state.active.values() {
            let reading = segment.clock.and_then(Clock::read);
            clock_failed |= reading.is_none();
            cpu += reading
                .unwrap_or(segment.since)
                .saturating_sub(segment.since);
        }
        let elapsed_ns = self.born.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let running_workers = state
            .active
            .keys()
            .filter(|worker| !state.waits.values().any(|(waiting, _)| waiting == *worker))
            .count();
        let wait_reasons = state
            .waits
            .values()
            .map(|(_, reason)| *reason)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        // A missing backend is unknown, not zero usage. Linux is the qualified
        // thread-clock implementation; additional hosts can supply their clock.
        let cpu_time_ns = (cfg!(target_os = "linux") && !clock_failed).then_some(cpu);
        UsageSnapshot {
            cpu_time_ns,
            cpu_percent: cpu_time_ns.map(|n| n as f64 / elapsed_ns.max(1) as f64 * 100.0),
            elapsed_ns,
            running_workers,
            wait_reasons,
        }
    }
}

pub struct UsageGuard {
    id: u64,
    _thread_bound: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for UsageGuard {
    fn drop(&mut self) {
        let changed = OWNERS.with(|owners| {
            let mut owners = owners.borrow_mut();
            let index = owners
                .iter()
                .position(|(id, _)| *id == self.id)
                .expect("usage scope owner");
            let was_top = index + 1 == owners.len();
            let (_, current) = owners.remove(index);
            was_top.then(|| (current, owners.last().map(|(_, usage)| usage.clone())))
        });
        if let Some((current, previous)) = changed {
            current.detach();
            if let Some(previous) = previous {
                previous.attach();
            }
        }
    }
}
pub struct CallbackGuard {
    usage: Arc<ProcessUsage>,
    paused: Vec<(u64, (ThreadId, &'static str))>,
}
impl Drop for CallbackGuard {
    fn drop(&mut self) {
        self.usage
            .state
            .lock()
            .unwrap()
            .waits
            .extend(self.paused.drain(..));
    }
}
pub struct WaitGuard {
    usage: Arc<ProcessUsage>,
    id: u64,
}
impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.usage.state.lock().unwrap().waits.remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn burn() {
        let start = Instant::now();
        while start.elapsed().as_millis() < 40 {
            std::hint::black_box(17u64.wrapping_mul(23));
        }
    }
    #[test]
    #[cfg(target_os = "linux")]
    fn nested_scopes_and_reused_workers_charge_one_owner() {
        let first = Arc::new(ProcessUsage::new());
        let second = Arc::new(ProcessUsage::new());
        let outer = first.enter();
        burn();
        let same = first.enter();
        burn();
        drop(same);
        let child = second.enter();
        let paused = first.snapshot().cpu_time_ns.unwrap();
        burn();
        assert_eq!(paused, first.snapshot().cpu_time_ns.unwrap());
        drop(child);
        let child_total = second.snapshot().cpu_time_ns.unwrap();
        burn();
        assert_eq!(child_total, second.snapshot().cpu_time_ns.unwrap());
        drop(outer);
        let ended = first.snapshot().cpu_time_ns.unwrap();
        burn();
        assert_eq!(ended, first.snapshot().cpu_time_ns.unwrap());
        assert!(ended > 20_000_000 && child_total > 5_000_000);
    }
    #[test]
    fn guest_callback_temporarily_resumes_above_a_wait_and_can_wait_again() {
        let usage = Arc::new(ProcessUsage::new());
        let _scope = usage.enter();
        let _outer = usage.waiting("poll_oneoff");
        let callback = usage.guest_callback();
        assert_eq!(usage.snapshot().running_workers, 1);
        assert!(usage.snapshot().wait_reasons.is_empty());
        let inner = usage.waiting("fd_read");
        assert_eq!(usage.snapshot().running_workers, 0);
        assert_eq!(usage.snapshot().wait_reasons, ["fd_read"]);
        drop(inner);
        drop(callback);
        assert_eq!(usage.snapshot().running_workers, 0);
        assert_eq!(usage.snapshot().wait_reasons, ["poll_oneoff"]);
    }
    #[test]
    fn scopes_can_retire_out_of_order_without_leaking_worker_ownership() {
        let first = Arc::new(ProcessUsage::new());
        let second = Arc::new(ProcessUsage::new());
        let outer = first.enter();
        let inner = second.enter();
        drop(outer);
        assert_eq!(second.snapshot().running_workers, 1);
        assert_eq!(first.snapshot().running_workers, 0);
        drop(inner);
        assert_eq!(second.snapshot().running_workers, 0);
        OWNERS.with(|owners| assert!(owners.borrow().is_empty()));
    }
    #[test]
    fn wait_guards_unwind_and_other_running_threads_win() {
        let usage = Arc::new(ProcessUsage::new());
        let scope = usage.enter();
        let wait = usage.waiting("fd_read");
        assert_eq!(usage.snapshot().running_workers, 0);
        assert_eq!(usage.snapshot().wait_reasons, ["fd_read"]);
        let other = usage.clone();
        std::thread::spawn(move || {
            let _scope = other.enter();
            assert_eq!(other.snapshot().running_workers, 1);
        })
        .join()
        .unwrap();
        drop(wait);
        assert_eq!(usage.snapshot().running_workers, 1);
        assert!(usage.snapshot().wait_reasons.is_empty());
        drop(scope);
    }
}
