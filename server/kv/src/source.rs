//! Shared contract between the memtables (NS3), the sorted runs (NS4) and the read path
//! that merges them (NS3/NS5). Changing it needs the lead.

use bytes::Bytes;

use crate::error::Result;
use crate::types::Seq;

/// Iterates internal keys (see [`crate::types`]) in
/// [`crate::types::compare_internal_keys`] order. It yields every stored version,
/// including point tombstones; it never yields range tombstones, which each source
/// exposes separately as [`RangeTombstone`]s.
///
/// After an error, `valid()` is false and `status()` returns it.
pub trait InternalIterator: Send {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    /// Positions at the first entry whose internal key is `>= target`.
    fn seek(&mut self, target: &[u8]);
    /// Positions at the last entry whose internal key is `<= target`.
    fn seek_for_prev(&mut self, target: &[u8]);
    /// Requires `valid()`.
    fn next(&mut self);
    /// Requires `valid()`.
    fn prev(&mut self);
    /// Internal key of the current entry. Requires `valid()`.
    fn key(&self) -> &[u8];
    /// Stored value of the current entry: the value of a `Put`, the encoded locator of a
    /// `JournalPut`, empty for a `Delete`. Requires `valid()`. Cheap to clone.
    fn value(&self) -> Bytes;
    fn status(&self) -> Result<()>;
}

/// Deletes every user key in `[start, end)` written with a sequence below `seq`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Bytes,
    pub end: Bytes,
    pub seq: Seq,
}
