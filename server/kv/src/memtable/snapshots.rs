//! The live snapshot list. A snapshot registers its sequence on creation and deregisters
//! when its last handle drops. Compaction asks [`SnapshotList::oldest`] and
//! [`SnapshotList::any_in`] before it drops an older version or a tombstone.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::types::Seq;

#[derive(Clone, Debug, Default)]
pub struct SnapshotList {
    live: Arc<Mutex<BTreeMap<Seq, usize>>>,
}

impl SnapshotList {
    pub fn new() -> SnapshotList {
        SnapshotList::default()
    }

    fn live(&self) -> MutexGuard<'_, BTreeMap<Seq, usize>> {
        self.live.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a snapshot at `seq`. It stays live until the returned handle drops.
    pub fn register(&self, seq: Seq) -> SnapshotHandle {
        *self.live().entry(seq).or_insert(0) += 1;
        SnapshotHandle {
            list: self.clone(),
            seq,
        }
    }

    /// The oldest live snapshot sequence.
    pub fn oldest(&self) -> Option<Seq> {
        self.live().keys().next().copied()
    }

    /// True when a live snapshot has a sequence in `(after, up_to]`: a version written at
    /// `after` and shadowed by one written at `up_to` is still visible to it.
    pub fn any_in(&self, after: Seq, up_to: Seq) -> bool {
        after < up_to
            && self
                .live()
                .range((Bound::Excluded(after), Bound::Included(up_to)))
                .next()
                .is_some()
    }

    /// Distinct live snapshot sequences, ascending (for one compaction pass).
    pub fn sequences(&self) -> Vec<Seq> {
        self.live().keys().copied().collect()
    }

    /// Number of live snapshot handles.
    pub fn len(&self) -> usize {
        self.live().values().sum()
    }

    pub fn is_empty(&self) -> bool {
        self.live().is_empty()
    }
}

/// Keeps one snapshot registered. Cloning registers another reference.
#[derive(Debug)]
pub struct SnapshotHandle {
    list: SnapshotList,
    seq: Seq,
}

impl SnapshotHandle {
    pub fn seq(&self) -> Seq {
        self.seq
    }
}

impl Clone for SnapshotHandle {
    fn clone(&self) -> SnapshotHandle {
        self.list.register(self.seq)
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        let mut live = self.list.live();
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
    fn ns_unit_snapshot_list_tracks_live_sequences() {
        let list = SnapshotList::new();
        assert_eq!(list.oldest(), None);
        let a = list.register(10);
        let b = list.register(20);
        let b2 = b.clone();
        assert_eq!(list.oldest(), Some(10));
        assert_eq!(list.sequences(), vec![10, 20]);
        assert_eq!(list.len(), 3);
        assert!(list.any_in(9, 10));
        assert!(!list.any_in(10, 19));
        assert!(list.any_in(10, 20));
        assert!(!list.any_in(20, 20));
        drop(a);
        assert_eq!(list.oldest(), Some(20));
        drop(b);
        assert_eq!(list.oldest(), Some(20));
        drop(b2);
        assert!(list.is_empty());
        assert_eq!(list.oldest(), None);
    }
}
