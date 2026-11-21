//! The read path's iterators (task NS3, `plans/native-storage-engine.md` section 2).
//!
//! - [`MergingIterator`] merges the internal iterators of every source of a read
//!   (memtables, runs), forward and reverse.
//! - [`FragmentedRangeTombstones`] and [`RangeDelAggregator`] answer "the newest range
//!   tombstone covering user key `k` visible at snapshot `s`" across those sources.
//! - [`DbIter`] is the user-facing iterator: one snapshot, newest visible version only,
//!   point and range tombstones applied, user-key bounds, journal values resolved through
//!   a [`ValueResolver`].

pub mod db_iter;
pub mod merging;
pub mod range_del;
#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod testutil;

use bytes::Bytes;

pub use db_iter::DbIter;
pub use merging::MergingIterator;
pub use range_del::{FragmentedRangeTombstones, RangeDelAggregator};

use crate::error::Result;
use crate::source::InternalIterator;
use crate::types::Locator;

/// Reads the value a [`crate::types::Kind::JournalPut`] entry points to. The store
/// implements it over the journal and the block cache; reads apply it lazily, so scans
/// that only look at keys never touch the journal.
pub trait ValueResolver: Send + Sync {
    fn resolve(&self, locator: &Locator) -> Result<Bytes>;
}

impl<I: InternalIterator + ?Sized> InternalIterator for Box<I> {
    fn valid(&self) -> bool {
        (**self).valid()
    }
    fn seek_to_first(&mut self) {
        (**self).seek_to_first()
    }
    fn seek_to_last(&mut self) {
        (**self).seek_to_last()
    }
    fn seek(&mut self, target: &[u8]) {
        (**self).seek(target)
    }
    fn seek_for_prev(&mut self, target: &[u8]) {
        (**self).seek_for_prev(target)
    }
    fn next(&mut self) {
        (**self).next()
    }
    fn prev(&mut self) {
        (**self).prev()
    }
    fn key(&self) -> &[u8] {
        (**self).key()
    }
    fn value(&self) -> Bytes {
        (**self).value()
    }
    fn status(&self) -> Result<()> {
        (**self).status()
    }
}
