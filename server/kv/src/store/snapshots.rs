//! The store's live snapshot registry, sharded so that taking and dropping snapshots on
//! many threads does not serialize on one lock (NS5b item 5).
//!
//! Each thread registers into one of [`SHARDS`] shards, picked once per thread, so the
//! hot path takes one uncontended mutex. Compaction, flush and journal GC read every
//! shard. The protocol that keeps a snapshot safe without a store-wide lock is in
//! `Inner::snapshot` (`store/mod.rs`).

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::types::Seq;

/// Shards of the registry.
pub(crate) const SHARDS: usize = 16;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn my_shard() -> usize {
    SHARD.with(|s| {
        let mut shard = s.get();
        if shard == usize::MAX {
            shard = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
            s.set(shard);
        }
        shard
    })
}

type Shard = Mutex<BTreeMap<Seq, usize>>;

fn lock(shard: &Shard) -> MutexGuard<'_, BTreeMap<Seq, usize>> {
    shard.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub(crate) struct Snapshots {
    shards: Arc<[Shard; SHARDS]>,
}

impl Default for Snapshots {
    fn default() -> Self {
        Snapshots {
            shards: Arc::new(std::array::from_fn(|_| Mutex::new(BTreeMap::new()))),
        }
    }
}

impl Snapshots {
    /// Registers a snapshot at `seq` in this thread's shard.
    pub fn register(&self, seq: Seq) -> Registered {
        let shard = my_shard();
        *lock(&self.shards[shard]).entry(seq).or_insert(0) += 1;
        Registered {
            shards: self.shards.clone(),
            shard,
            seq,
        }
    }

    /// Distinct live sequences, ascending.
    pub fn sequences(&self) -> Vec<Seq> {
        let mut out: Vec<Seq> = Vec::new();
        for shard in self.shards.iter() {
            out.extend(lock(shard).keys().copied());
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The oldest live snapshot.
    pub fn oldest(&self) -> Option<Seq> {
        self.shards
            .iter()
            .filter_map(|shard| lock(shard).keys().next().copied())
            .min()
    }

    /// Live snapshot handles.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| lock(shard).values().sum::<usize>())
            .sum()
    }
}

/// Keeps one snapshot registered until dropped.
pub(crate) struct Registered {
    shards: Arc<[Shard; SHARDS]>,
    shard: usize,
    seq: Seq,
}

impl Drop for Registered {
    fn drop(&mut self) {
        let mut live = lock(&self.shards[self.shard]);
        if let Some(count) = live.get_mut(&self.seq) {
            *count -= 1;
            if *count == 0 {
                live.remove(&self.seq);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ns_unit_sharded_snapshots_merge_across_threads() {
        let snaps = Snapshots::default();
        let a = snaps.register(7);
        let handles: Vec<Registered> = std::thread::scope(|s| {
            (0..8u64)
                .map(|i| {
                    let snaps = snaps.clone();
                    s.spawn(move || snaps.register(10 + i % 3))
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });
        assert_eq!(snaps.sequences(), vec![7, 10, 11, 12]);
        assert_eq!(snaps.oldest(), Some(7));
        assert_eq!(snaps.len(), 9);
        drop(a);
        assert_eq!(snaps.oldest(), Some(10));
        drop(handles);
        assert_eq!(snaps.oldest(), None);
        assert!(snaps.sequences().is_empty());
    }
}
