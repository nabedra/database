//! Sharded memtables (task NS3, `plans/native-storage-engine.md` section 2).
//!
//! A memtable is a set of ordered maps sharded by a key prefix (by default 17 bytes,
//! `0x40 ‖ group id`), so one group's keys share a shard and a point read takes one
//! shard read lock. Keys shorter than the prefix go to shard 0. Each shard is a
//! `RwLock<BTreeMap>` ordered by internal key. The single writer, the journal thread,
//! inserts a whole [`WriteBatch`] at `first_seq`; readers see it all at once because they
//! only read at sequences the store has published after the insert returned.
//!
//! Range deletes go to a per-memtable tombstone list, fragmented lazily for reads.
//! Every insert charges its approximate memory to an optional [`MemtableBudget`], and
//! dropping the memtable releases the charge.

mod budget;
mod cursor;
mod key;
mod snapshots;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockWriteGuard};

use bytes::Bytes;

pub use budget::MemtableBudget;
pub use cursor::ShardCursor;
pub use key::MemKey;
pub use snapshots::{SnapshotHandle, SnapshotList};

use crate::batch::{BatchOp, WriteBatch};
use crate::error::{Error, Result};
use crate::iter::merging::MergingIterator;
use crate::iter::range_del::FragmentedRangeTombstones;
use crate::source::RangeTombstone;
use crate::types::{Kind, Locator, MAX_SEQ, SEEK_KIND_BYTE, Seq, pack_trailer, unpack_trailer};
use key::{KeyView, Probe};

/// Charged per point entry on top of its key and value bytes: the map key and value
/// handles and a share of the B-tree node.
pub const ENTRY_OVERHEAD: usize = 80;
/// Charged per range tombstone on top of its bounds.
pub const TOMBSTONE_OVERHEAD: usize = 96;
/// Charged per inserted batch for its shared arena.
pub const BATCH_OVERHEAD: usize = 64;
/// Below this many tombstones a point read scans the list instead of fragmenting it.
const LINEAR_TOMBSTONES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemtableOptions {
    /// Keys sharing their first `shard_prefix_len` bytes share a shard.
    pub shard_prefix_len: usize,
    pub shards: usize,
}

impl Default for MemtableOptions {
    fn default() -> Self {
        MemtableOptions {
            shard_prefix_len: 17,
            shards: 16,
        }
    }
}

/// The result of a point lookup in one memtable at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// The newest visible version is a value: a `Put` with its value, or a `JournalPut`
    /// with its encoded [`Locator`].
    Found(Kind, Bytes, Seq),
    /// The newest visible version is a point tombstone, or a range tombstone in this
    /// memtable hides every visible version. Older sources must not be consulted.
    Deleted(Seq),
    /// This memtable holds nothing visible for the key; consult older sources.
    NotFound,
}

/// Pads a shard to its own cache lines so readers of one shard do not contend with
/// writers of the next.
#[repr(align(128))]
struct Shard {
    map: RwLock<BTreeMap<MemKey, Bytes>>,
}

#[derive(Default)]
struct Tombstones {
    raw: Vec<RangeTombstone>,
    fragmented: Option<Arc<FragmentedRangeTombstones>>,
}

pub struct Memtable {
    prefix_len: usize,
    shards: Box<[Shard]>,
    tombstones: RwLock<Tombstones>,
    tombstone_count: AtomicUsize,
    entries: AtomicUsize,
    memory: AtomicUsize,
    smallest_seq: AtomicU64,
    largest_seq: AtomicU64,
    budget: Option<Arc<MemtableBudget>>,
}

impl std::fmt::Debug for Memtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable")
            .field("shards", &self.shards.len())
            .field("entries", &self.len())
            .field("tombstones", &self.tombstone_count())
            .field("memory", &self.approximate_memory())
            .finish()
    }
}

/// FNV-1a over the shard prefix. Fixed, so shard choice is stable across runs.
fn hash_prefix(prefix: &[u8]) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for &b in prefix {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// The smallest byte string greater than every string starting with `prefix`, or `None`
/// when `prefix` is all `0xFF`.
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&b| b != 0xFF)?;
    let mut out = prefix[..=last].to_vec();
    out[last] += 1;
    Some(out)
}

/// The trailer that sorts before every stored version visible at `snapshot`.
fn seek_trailer(snapshot: Seq) -> u64 {
    (snapshot.min(MAX_SEQ) << 8) | u64::from(SEEK_KIND_BYTE)
}

impl Memtable {
    pub fn new(options: MemtableOptions, budget: Option<Arc<MemtableBudget>>) -> Memtable {
        let shards = options.shards.max(1);
        Memtable {
            prefix_len: options.shard_prefix_len,
            shards: (0..shards)
                .map(|_| Shard {
                    map: RwLock::new(BTreeMap::new()),
                })
                .collect(),
            tombstones: RwLock::new(Tombstones::default()),
            tombstone_count: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            memory: AtomicUsize::new(0),
            smallest_seq: AtomicU64::new(u64::MAX),
            largest_seq: AtomicU64::new(0),
            budget,
        }
    }

    /// The shard holding `user_key`.
    pub fn shard_of(&self, user_key: &[u8]) -> usize {
        if self.shards.len() == 1 || user_key.len() < self.prefix_len {
            return 0;
        }
        (hash_prefix(&user_key[..self.prefix_len]) % self.shards.len() as u64) as usize
    }

    /// The one shard that can hold user keys inside the bounds, if there is one: when
    /// every key in the range shares the lower bound's shard prefix.
    pub fn single_shard_for(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Option<usize> {
        if self.shards.len() == 1 {
            return Some(0);
        }
        let (Bound::Included(lower) | Bound::Excluded(lower)) = lower else {
            return None;
        };
        if lower.len() < self.prefix_len {
            return None;
        }
        let prefix = &lower[..self.prefix_len];
        // A key at or above `lower` lies outside `prefix` only if it is at or above the
        // prefix's successor (shorter keys included), so an upper bound at or below the
        // successor confines the range to the prefix.
        let successor = prefix_successor(prefix)?;
        let confined = match upper {
            Bound::Excluded(upper) => upper <= successor.as_slice(),
            Bound::Included(upper) => upper < successor.as_slice(),
            Bound::Unbounded => false,
        };
        confined.then(|| self.shard_of(prefix))
    }

    /// Inserts `batch`; operation `i` takes sequence `first_seq + i`. `locators[j]` is
    /// where the journal stored the value of `batch.journal_values()[j]`; the memtable
    /// keeps the encoded locator instead of the value.
    ///
    /// Only one thread inserts at a time (the journal thread), in sequence order.
    /// Readers must not read at a sequence of this batch until this returns.
    pub fn insert(&self, batch: &WriteBatch, first_seq: Seq, locators: &[Locator]) -> Result<()> {
        if locators.len() != batch.journal_values().len() {
            return Err(Error::InvalidArgument(
                "memtable insert needs one locator per journal value",
            ));
        }
        if batch.is_empty() {
            return Ok(());
        }
        let last_seq = first_seq
            .checked_add(u64::from(batch.count()) - 1)
            .filter(|&s| s <= MAX_SEQ)
            .ok_or(Error::InvalidArgument("sequence number overflow"))?;

        // One arena per batch holds every key and value (locators instead of journal
        // values), so entries are slices of it and insertion allocates per batch only.
        let journal_bytes: usize = batch.journal_values().iter().map(|j| j.len as usize).sum();
        let mut arena = Vec::with_capacity(
            batch.encoded_bytes() - journal_bytes + locators.len() * Locator::LEN,
        );
        let mut next_locator = locators.iter();
        for op in batch.iter() {
            match op {
                BatchOp::Put { key, value } => {
                    arena.extend_from_slice(key);
                    arena.extend_from_slice(value);
                }
                BatchOp::JournalPut { key, .. } => {
                    arena.extend_from_slice(key);
                    let locator = next_locator.next().expect("locator count checked");
                    arena.extend_from_slice(&locator.encode());
                }
                BatchOp::Delete { key } => arena.extend_from_slice(key),
                BatchOp::RangeDelete { start, end } => {
                    arena.extend_from_slice(start);
                    arena.extend_from_slice(end);
                }
            }
        }
        let arena = Bytes::from(arena);
        let mut at = 0;
        let mut take = |len: usize| {
            let bytes = arena.slice(at..at + len);
            at += len;
            bytes
        };

        let mut charge = BATCH_OVERHEAD + arena.len();
        let mut entries = 0;
        let mut new_tombstones = 0;
        let mut shard_guard: Option<(usize, RwLockWriteGuard<'_, BTreeMap<MemKey, Bytes>>)> = None;
        let mut tombstone_guard: Option<RwLockWriteGuard<'_, Tombstones>> = None;
        for (i, op) in batch.iter().enumerate() {
            let seq = first_seq + i as u64;
            let (key, value) = match op {
                BatchOp::RangeDelete { start, end } => {
                    let start = take(start.len());
                    let end = take(end.len());
                    let guard = tombstone_guard.get_or_insert_with(|| {
                        self.tombstones
                            .write()
                            .unwrap_or_else(PoisonError::into_inner)
                    });
                    guard.raw.push(RangeTombstone { start, end, seq });
                    guard.fragmented = None;
                    charge += TOMBSTONE_OVERHEAD;
                    new_tombstones += 1;
                    continue;
                }
                BatchOp::Put { key, value } => (take(key.len()), take(value.len())),
                BatchOp::JournalPut { key, .. } => (take(key.len()), take(Locator::LEN)),
                BatchOp::Delete { key } => (take(key.len()), Bytes::new()),
            };
            let shard = self.shard_of(&key);
            if shard_guard.as_ref().is_none_or(|(s, _)| *s != shard) {
                drop(shard_guard.take());
                shard_guard = Some((
                    shard,
                    self.shards[shard]
                        .map
                        .write()
                        .unwrap_or_else(PoisonError::into_inner),
                ));
            }
            let (_, map) = shard_guard.as_mut().expect("guard set above");
            let trailer = pack_trailer(seq, op.kind());
            map.insert(
                MemKey {
                    user_key: key,
                    trailer,
                },
                value,
            );
            charge += ENTRY_OVERHEAD;
            entries += 1;
        }
        drop(shard_guard);
        if tombstone_guard.is_some() {
            self.tombstone_count
                .fetch_add(new_tombstones, Ordering::Release);
        }
        drop(tombstone_guard);

        self.entries.fetch_add(entries, Ordering::Relaxed);
        self.memory.fetch_add(charge, Ordering::Relaxed);
        if let Some(budget) = &self.budget {
            budget.reserve(charge);
        }
        self.smallest_seq.fetch_min(first_seq, Ordering::Relaxed);
        self.largest_seq.fetch_max(last_seq, Ordering::Relaxed);
        Ok(())
    }

    /// The newest version of `user_key` visible at `snapshot_seq`, with this memtable's
    /// range tombstones applied. O(log n) under one shard read lock.
    pub fn get(&self, user_key: &[u8], snapshot_seq: Seq) -> Lookup {
        let probe = Probe {
            user_key,
            trailer: seek_trailer(snapshot_seq),
        };
        let point = {
            let map = self.shards[self.shard_of(user_key)]
                .map
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            map.range::<dyn KeyView, _>((Bound::Included(&probe as &dyn KeyView), Bound::Unbounded))
                .next()
                .filter(|(k, _)| k.user_key.as_ref() == user_key)
                .map(|(k, v)| (k.trailer, v.clone()))
        };
        let covering = self.max_covering_tombstone(user_key, snapshot_seq);
        match point {
            Some((trailer, value)) => {
                let (seq, kind) = unpack_trailer(trailer).expect("memtable trailers are valid");
                match covering {
                    Some(t) if t > seq => Lookup::Deleted(t),
                    _ => match kind {
                        Kind::Put | Kind::JournalPut => Lookup::Found(kind, value, seq),
                        Kind::Delete | Kind::RangeDelete => Lookup::Deleted(seq),
                    },
                }
            }
            None => covering.map_or(Lookup::NotFound, Lookup::Deleted),
        }
    }

    /// The newest range tombstone of this memtable covering `user_key` at `snapshot_seq`.
    pub fn max_covering_tombstone(&self, user_key: &[u8], snapshot_seq: Seq) -> Option<Seq> {
        if self.tombstone_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        {
            let tombstones = self
                .tombstones
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(fragmented) = &tombstones.fragmented {
                return fragmented.max_covering_seq(user_key, snapshot_seq);
            }
            if tombstones.raw.len() <= LINEAR_TOMBSTONES {
                return tombstones
                    .raw
                    .iter()
                    .filter(|t| {
                        t.seq <= snapshot_seq
                            && t.start.as_ref() <= user_key
                            && user_key < t.end.as_ref()
                    })
                    .map(|t| t.seq)
                    .max();
            }
        }
        self.fragmented_tombstones()
            .max_covering_seq(user_key, snapshot_seq)
    }

    /// This memtable's range tombstones, fragmented (cached until the next range delete).
    pub fn fragmented_tombstones(&self) -> Arc<FragmentedRangeTombstones> {
        if let Some(fragmented) = &self
            .tombstones
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .fragmented
        {
            return fragmented.clone();
        }
        let mut tombstones = self
            .tombstones
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let Tombstones { raw, fragmented } = &mut *tombstones;
        fragmented
            .get_or_insert_with(|| Arc::new(FragmentedRangeTombstones::new(raw.iter(), MAX_SEQ)))
            .clone()
    }

    /// This memtable's range tombstones as written, in insertion order.
    pub fn range_tombstones(&self) -> Vec<RangeTombstone> {
        if self.tombstone_count.load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        self.tombstones
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .raw
            .clone()
    }

    /// An iterator over every entry. See [`Memtable::iter_range`].
    pub fn iter(self: &Arc<Self>) -> MemtableIter {
        self.iter_range(Bound::Unbounded, Bound::Unbounded)
    }

    /// An iterator over the entries whose user keys lie within the bounds, merging the
    /// shards that can hold them (one shard when the bounds share a shard prefix).
    /// Entries outside the bounds are never returned; positioning outside them leaves
    /// the iterator at the nearest entry inside, or invalid.
    pub fn iter_range(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemtableIter {
        let lower_key = match lower {
            Bound::Included(k) => Bound::Included(MemKey {
                user_key: Bytes::copy_from_slice(k),
                trailer: u64::MAX,
            }),
            Bound::Excluded(k) => Bound::Excluded(MemKey {
                user_key: Bytes::copy_from_slice(k),
                trailer: 0,
            }),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper_key = match upper {
            Bound::Included(k) => Bound::Included(MemKey {
                user_key: Bytes::copy_from_slice(k),
                trailer: 0,
            }),
            Bound::Excluded(k) => Bound::Excluded(MemKey {
                user_key: Bytes::copy_from_slice(k),
                trailer: u64::MAX,
            }),
            Bound::Unbounded => Bound::Unbounded,
        };
        let cursor =
            |shard| ShardCursor::new(self.clone(), shard, lower_key.clone(), upper_key.clone());
        let children = match self.single_shard_for(lower, upper) {
            Some(shard) => vec![cursor(shard)],
            None => (0..self.shards.len()).map(cursor).collect(),
        };
        MergingIterator::new(children)
    }

    /// Point entries (including point tombstones).
    pub fn len(&self) -> usize {
        self.entries.load(Ordering::Relaxed)
    }

    pub fn tombstone_count(&self) -> usize {
        self.tombstone_count.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0 && self.tombstone_count() == 0
    }

    /// Approximate bytes held: keys, values, locators, tombstones and per-entry overhead.
    pub fn approximate_memory(&self) -> usize {
        self.memory.load(Ordering::Relaxed)
    }

    /// Smallest sequence inserted, `None` while empty.
    pub fn smallest_seq(&self) -> Option<Seq> {
        let s = self.smallest_seq.load(Ordering::Relaxed);
        (s != u64::MAX).then_some(s)
    }

    /// Largest sequence inserted, `None` while empty.
    pub fn largest_seq(&self) -> Option<Seq> {
        self.smallest_seq()
            .map(|_| self.largest_seq.load(Ordering::Relaxed))
    }
}

impl Drop for Memtable {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            budget.release(*self.memory.get_mut());
        }
    }
}

/// An iterator over one memtable: the merge of its shard cursors.
pub type MemtableIter = MergingIterator<ShardCursor>;

#[cfg(test)]
mod tests;
