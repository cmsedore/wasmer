//! Immutable code ranges for the asynchronous interrupt handler.
//!
//! Readers use only atomics and plain immutable memory: no locks, allocation,
//! destructible TLS, or refcounts in the signal handler. Writers publish a new
//! snapshot and reclaim the old one after all readers have left. SeqCst orders
//! reader entry, pointer publication and reclamation, including readers which
//! arrive between publication and the zero-reader check.
use std::{
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

type Ranges = Vec<(usize, usize)>;

struct CodeRegistry {
    writer: Mutex<()>,
    ranges: AtomicPtr<Ranges>,
    readers: AtomicUsize,
}

impl CodeRegistry {
    const fn new() -> Self {
        Self {
            writer: Mutex::new(()),
            ranges: AtomicPtr::new(ptr::null_mut()),
            readers: AtomicUsize::new(0),
        }
    }

    fn contains(&self, pc: usize) -> bool {
        self.readers.fetch_add(1, Ordering::SeqCst);
        let ptr = self.ranges.load(Ordering::SeqCst);
        let found = if ptr.is_null() {
            false
        } else {
            // The reader count prevents reclamation until we leave below.
            let ranges = unsafe { &*ptr };
            let index = ranges.partition_point(|&(start, _)| start <= pc);
            index != 0 && pc < ranges[index - 1].1
        };
        self.readers.fetch_sub(1, Ordering::SeqCst);
        found
    }

    fn update(&self, f: impl FnOnce(&mut Ranges)) {
        let _writer = self.writer.lock().unwrap();
        let old = self.ranges.load(Ordering::SeqCst);
        let mut ranges = if old.is_null() {
            vec![]
        } else {
            unsafe { (&*old).clone() }
        };
        f(&mut ranges);
        let new = Box::into_raw(Box::new(ranges));
        self.ranges.store(new, Ordering::SeqCst);
        while self.readers.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        if !old.is_null() {
            unsafe {
                drop(Box::from_raw(old));
            }
        }
    }
}

static CODE: CodeRegistry = CodeRegistry::new();

/// Keeps exact compiled function ranges registered while their code is alive.
pub struct CodeRegistration(Ranges);

impl CodeRegistration {
    /// Register half-open ranges. Native wrappers and trampolines are excluded.
    pub fn new(ranges: impl IntoIterator<Item = (usize, usize)>) -> Self {
        let mut ranges: Ranges = ranges
            .into_iter()
            .filter(|&(start, end)| start < end)
            .collect();
        ranges.sort_unstable();
        CODE.update(|all| {
            all.extend_from_slice(&ranges);
            all.sort_unstable();
            assert!(
                all.windows(2).all(|pair| pair[0].1 <= pair[1].0),
                "overlapping guest code ranges"
            );
        });
        Self(ranges)
    }
}

impl Drop for CodeRegistration {
    fn drop(&mut self) {
        CODE.update(|all| all.retain(|range| self.0.binary_search(range).is_err()));
    }
}

pub(crate) fn is_guest_pc(pc: usize) -> bool {
    CODE.contains(pc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_half_open_ranges_unregister_and_exclude_gaps() {
        let registration = CodeRegistration::new([(100, 120), (130, 140), (150, 150)]);
        assert!(!is_guest_pc(99));
        assert!(is_guest_pc(100));
        assert!(is_guest_pc(119));
        assert!(!is_guest_pc(120));
        assert!(!is_guest_pc(129));
        assert!(is_guest_pc(130));
        assert!(!is_guest_pc(140));
        drop(registration);
        assert!(!is_guest_pc(100));
    }

    #[test]
    fn readers_survive_snapshot_replacement() {
        let registry = CodeRegistry::new();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let registry = &registry;
                scope.spawn(move || {
                    for _ in 0..100_000 {
                        registry.contains(42);
                    }
                });
            }
            for i in 0..1000 {
                registry
                    .update(|ranges| *ranges = if i % 2 == 0 { vec![(40, 50)] } else { vec![] });
            }
        });
        registry.update(|ranges| ranges.clear());
        // This private test registry has no readers left.
        unsafe {
            drop(Box::from_raw(registry.ranges.load(Ordering::SeqCst)));
        }
    }
}
