//! A worker-shared memtable byte budget, the replacement for the previous engine's
//! write buffer manager.
//!
//! Every memtable charges its approximate memory here and releases it when dropped.
//! Writers never fail for memory: the journal thread asks [`MemtableBudget::should_flush`]
//! to freeze and flush early, and waits in [`MemtableBudget::wait_below_limit`] while the
//! budget is exhausted (a stall).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Instant;

#[derive(Debug)]
pub struct MemtableBudget {
    limit: usize,
    flush_at: usize,
    used: AtomicUsize,
    waiters: AtomicUsize,
    lock: Mutex<()>,
    below_limit: Condvar,
}

impl MemtableBudget {
    /// A budget of `limit` bytes that asks for a flush at 7/8 of it, as a write
    /// buffer manager does for mutable memtables.
    pub fn new(limit: usize) -> MemtableBudget {
        MemtableBudget::with_flush_threshold(limit, limit - limit / 8)
    }

    /// A budget that asks for a flush once `flush_at` bytes are used.
    pub fn with_flush_threshold(limit: usize, flush_at: usize) -> MemtableBudget {
        MemtableBudget {
            limit,
            flush_at: flush_at.min(limit),
            used: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            lock: Mutex::new(()),
            below_limit: Condvar::new(),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Charges `bytes`. Never blocks and never fails: the budget may run over, and the
    /// writer then stalls in [`MemtableBudget::wait_below_limit`] before its next insert.
    pub fn reserve(&self, bytes: usize) {
        self.used.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Returns `bytes` charged earlier and wakes stalled writers.
    pub fn release(&self, bytes: usize) {
        let before = self.used.fetch_sub(bytes, Ordering::SeqCst);
        debug_assert!(
            before >= bytes,
            "memtable budget released more than reserved"
        );
        // A waiter registers itself before checking `used` under the lock, so either it
        // sees this release or this load sees it and the notify below reaches it.
        if self.waiters.load(Ordering::SeqCst) > 0 {
            let _guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
            self.below_limit.notify_all();
        }
    }

    /// True once the memtables should be frozen and flushed.
    pub fn should_flush(&self) -> bool {
        self.used() >= self.flush_at
    }

    /// True while writers must stall.
    pub fn is_exhausted(&self) -> bool {
        self.used() >= self.limit
    }

    /// Blocks until usage is below the limit or `deadline` passes (`None` waits forever).
    /// Returns whether usage is below the limit.
    pub fn wait_below_limit(&self, deadline: Option<Instant>) -> bool {
        if !self.is_exhausted() {
            return true;
        }
        let mut guard = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let below = loop {
            if self.used.load(Ordering::SeqCst) < self.limit {
                break true;
            }
            match deadline {
                None => {
                    guard = self
                        .below_limit
                        .wait(guard)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let now = nabedra_rt::time::now_std();
                    if now >= deadline {
                        break false;
                    }
                    guard = self
                        .below_limit
                        .wait_timeout(guard, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
        };
        self.waiters.fetch_sub(1, Ordering::SeqCst);
        drop(guard);
        below
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn ns_unit_budget_tracks_flush_and_stall_thresholds() {
        let budget = MemtableBudget::new(800);
        budget.reserve(600);
        assert!(!budget.should_flush());
        budget.reserve(100);
        assert!(budget.should_flush());
        assert!(!budget.is_exhausted());
        budget.reserve(100);
        assert!(budget.is_exhausted());
        let start = nabedra_rt::time::now_std();
        assert!(!budget.wait_below_limit(Some(start + Duration::from_millis(20))));
        assert!(nabedra_rt::time::elapsed(start) >= Duration::from_millis(20));
        budget.release(800);
        assert_eq!(budget.used(), 0);
        assert!(budget.wait_below_limit(Some(nabedra_rt::time::now_std())));
    }

    #[test]
    #[allow(clippy::disallowed_methods)] // short-lived test threads, joined before the test ends
    fn ns_unit_stalled_writers_wake_on_release() {
        let budget = Arc::new(MemtableBudget::new(100));
        budget.reserve(150);
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let budget = budget.clone();
                std::thread::spawn(move || budget.wait_below_limit(None))
            })
            .collect();
        nabedra_rt::time::thread_sleep(Duration::from_millis(20));
        budget.release(100);
        for waiter in waiters {
            assert!(waiter.join().unwrap());
        }
    }
}
