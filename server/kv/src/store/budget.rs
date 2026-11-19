//! Store-side policy for the worker-shared memtable budget (RV-KV2 findings 5 and 6).
//!
//! Every store opened with the same [`MemtableBudget`] joins one [`BudgetGroup`]. The
//! group knows its stores, so a writer stalled on an exhausted budget can have the store
//! holding the most unflushed memtable memory flushed, even when that store is idle
//! (RocksDB's write-buffer-manager flush). It also tracks **pinned** bytes: memtables
//! that are already flushed to runs but still alive because a snapshot or iterator holds
//! them. They stay charged to the budget (its `used()` is the real memory), but writers
//! stall only on the *unpinned* charge, so one long-lived snapshot cannot stop every
//! store on the worker. Pinned memory is bounded by how long snapshots live; the pool
//! (NS6) must bound snapshot and iterator lifetimes and can watch
//! [`BudgetGroup::pinned_bytes`] (exposed in `MemoryUsage::memtables_pinned`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use crate::memtable::MemtableBudget;

use super::Inner;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The stores sharing one budget.
#[derive(Default)]
pub(crate) struct BudgetGroup {
    stores: Mutex<Vec<Weak<Inner>>>,
    /// Shared with flushed memtables, which subtract their charge when they drop.
    pinned: Arc<AtomicUsize>,
}

type Registry = Vec<(Weak<MemtableBudget>, Weak<BudgetGroup>)>;

static GROUPS: Mutex<Registry> = Mutex::new(Vec::new());

impl BudgetGroup {
    /// The group of `budget`, created on first use.
    pub fn of(budget: &Arc<MemtableBudget>) -> Arc<BudgetGroup> {
        let mut groups = lock(&GROUPS);
        groups.retain(|(b, g)| b.strong_count() > 0 && g.strong_count() > 0);
        for (b, g) in groups.iter() {
            if let (Some(b), Some(g)) = (b.upgrade(), g.upgrade())
                && Arc::ptr_eq(&b, budget)
            {
                return g;
            }
        }
        let group = Arc::new(BudgetGroup::default());
        groups.push((Arc::downgrade(budget), Arc::downgrade(&group)));
        group
    }

    pub fn join(&self, store: Weak<Inner>) {
        let mut stores = lock(&self.stores);
        stores.retain(|s| s.strong_count() > 0);
        stores.push(store);
    }

    pub fn pinned_bytes(&self) -> usize {
        self.pinned.load(Ordering::Relaxed)
    }

    /// The pinned-bytes counter a flushed memtable charges until it drops.
    pub fn pinned_counter(&self) -> &Arc<AtomicUsize> {
        &self.pinned
    }

    /// Budget bytes held by memtables that a flush can still free.
    pub fn unpinned(&self, budget: &MemtableBudget) -> usize {
        budget.used().saturating_sub(self.pinned_bytes())
    }

    /// True while writers must stall: the unpinned charge is at the limit.
    pub fn exhausted(&self, budget: &MemtableBudget) -> bool {
        self.unpinned(budget) >= budget.limit()
    }

    /// True once memtables should be frozen and flushed early.
    pub fn should_flush(&self, budget: &MemtableBudget) -> bool {
        if self.pinned_bytes() == 0 {
            return budget.should_flush();
        }
        self.unpinned(budget) >= budget.limit() - budget.limit() / 8
    }

    /// Freezes and schedules a flush of the store holding the most unflushed memtable
    /// memory. Returns whether one was found.
    pub fn flush_largest(&self) -> bool {
        let stores: Vec<Arc<Inner>> = lock(&self.stores)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let best = stores
            .iter()
            .map(|s| (s.unflushed_bytes(), s))
            .filter(|(bytes, _)| *bytes > 0)
            .max_by_key(|(bytes, _)| *bytes);
        match best {
            Some((_, store)) => {
                store.flush_for_budget();
                true
            }
            None => false,
        }
    }
}
