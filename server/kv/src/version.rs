//! Versions: the set of live runs per level, and the super-version readers pin (task NS5).
//!
//! A [`Version`] is immutable. Flush and compaction build a new one from the current one
//! plus a [`crate::manifest::VersionEdit`] and publish it in a new [`SuperVersion`]
//! together with the memtables. A reader (a `Snapshot`) clones one `Arc<SuperVersion>`,
//! so it keeps every memtable and run it can see alive.
//!
//! Runs are [`RunFile`]s. A run removed by a compaction is marked obsolete; its file is
//! deleted only when the last version referencing it drops, which is after every
//! snapshot that could read it is gone and after the manifest edit removing it synced.
//! The [`FileTracker`] collects those deletions and also records which journal segments
//! live runs and memtables still point into, for journal retirement.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;

use crate::iter::range_del::FragmentedRangeTombstones;
use crate::manifest::RunDesc;
use crate::memtable::Memtable;
use crate::types::{Kind, MAX_SEQ, Seq, parse_internal_key, user_key};

/// Levels L0 to L6.
pub const NUM_LEVELS: usize = 7;

/// Leveled compaction knobs (plan Decisions: today's RocksDB settings).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelOptions {
    /// L0 runs that trigger an L0 compaction.
    pub l0_compaction_trigger: usize,
    /// L0 runs at which writes slow down.
    pub l0_slowdown_writes: usize,
    /// L0 runs at which writes stop until compaction catches up.
    pub l0_stop_writes: usize,
    /// Compaction output is split at about this size.
    pub target_file_size: u64,
    /// Target size of L1.
    pub max_bytes_for_level_base: u64,
    /// Each level below L1 targets this many times the level above.
    pub level_size_multiplier: u64,
    /// Write rate while L0 is at the slowdown point (bytes per second).
    pub delayed_write_rate: u64,
}

impl Default for LevelOptions {
    fn default() -> Self {
        LevelOptions {
            l0_compaction_trigger: 4,
            l0_slowdown_writes: 20,
            l0_stop_writes: 36,
            target_file_size: 64 << 20,
            max_bytes_for_level_base: 256 << 20,
            level_size_multiplier: 10,
            delayed_write_rate: 16 << 20,
        }
    }
}

impl LevelOptions {
    /// Target bytes of `level` (1..).
    pub fn level_target(&self, level: usize) -> u64 {
        let mut target = self.max_bytes_for_level_base.max(1);
        for _ in 1..level {
            target = target.saturating_mul(self.level_size_multiplier.max(2));
        }
        target
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Tracks live files for deletion and journal retirement.
#[derive(Default)]
pub(crate) struct FileTracker {
    /// Lowest journal segment each live run object with journal values points into,
    /// by object token (a trivial move gives one run id two objects for a while).
    run_segments: Mutex<BTreeMap<u64, u64>>,
    /// Lowest journal segment each live memtable points into.
    mem_segments: Mutex<BTreeMap<u64, u64>>,
    /// Runs marked obsolete whose last reference dropped: delete their files.
    obsolete: Mutex<Vec<u64>>,
    next_token: AtomicU64,
}

impl FileTracker {
    /// The lowest journal segment any live run or memtable (current or pinned by a
    /// snapshot) points into.
    pub fn min_referenced_segment(&self) -> Option<u64> {
        let runs = lock(&self.run_segments).values().copied().min();
        let mems = lock(&self.mem_segments).values().copied().min();
        match (runs, mems) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub fn take_obsolete(&self) -> Vec<u64> {
        std::mem::take(&mut *lock(&self.obsolete))
    }

    fn next_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }
}

/// One live run of a version.
pub(crate) struct RunFile {
    pub desc: RunDesc,
    /// User-key range: `[lo, hi]`, or `[lo, hi)` when the largest key is a range
    /// tombstone end.
    pub lo: Bytes,
    pub hi: Bytes,
    pub hi_exclusive: bool,
    token: u64,
    tombstones: OnceLock<Arc<FragmentedRangeTombstones>>,
    obsolete: AtomicBool,
    tracker: Arc<FileTracker>,
}

impl std::fmt::Debug for RunFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunFile")
            .field("id", &self.desc.id)
            .field("level", &self.desc.level)
            .finish()
    }
}

/// `[lo, hi]` or `[lo, hi)` of a run's internal-key bounds.
pub(crate) fn user_range(smallest: &[u8], largest: &[u8]) -> (Bytes, Bytes, bool) {
    let lo = Bytes::copy_from_slice(user_key(smallest));
    let hi = Bytes::copy_from_slice(user_key(largest));
    let exclusive = parse_internal_key(largest)
        .is_some_and(|p| p.kind == Kind::RangeDelete && p.seq == MAX_SEQ);
    (lo, hi, exclusive)
}

impl RunFile {
    pub fn new(desc: RunDesc, tracker: Arc<FileTracker>) -> Arc<RunFile> {
        let (lo, hi, hi_exclusive) = user_range(&desc.smallest, &desc.largest);
        let token = tracker.next_token();
        if let Some(segment) = desc.oldest_journal_segment {
            lock(&tracker.run_segments).insert(token, segment);
        }
        Arc::new(RunFile {
            desc,
            lo,
            hi,
            hi_exclusive,
            token,
            tombstones: OnceLock::new(),
            obsolete: AtomicBool::new(false),
            tracker,
        })
    }

    pub fn id(&self) -> u64 {
        self.desc.id
    }

    pub fn level(&self) -> usize {
        self.desc.level as usize
    }

    pub fn size(&self) -> u64 {
        self.desc.file_size
    }

    /// Marks the run for deletion once nothing references it.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.lo.as_ref() <= key
            && if self.hi_exclusive {
                key < self.hi.as_ref()
            } else {
                key <= self.hi.as_ref()
            }
    }

    /// True when the run may hold keys in `[lo, hi]` (`hi = None` is unbounded).
    pub fn overlaps(&self, lo: &[u8], hi: Option<&[u8]>) -> bool {
        let below_hi = match hi {
            None => true,
            Some(hi) => self.lo.as_ref() <= hi,
        };
        let above_lo = if self.hi_exclusive {
            lo < self.hi.as_ref()
        } else {
            lo <= self.hi.as_ref()
        };
        below_hi && above_lo
    }

    /// The run's range tombstones, fragmented once.
    pub fn tombstones(
        &self,
        load: impl FnOnce() -> Arc<FragmentedRangeTombstones>,
    ) -> Arc<FragmentedRangeTombstones> {
        self.tombstones.get_or_init(load).clone()
    }
}

impl Drop for RunFile {
    fn drop(&mut self) {
        if self.desc.oldest_journal_segment.is_some() {
            lock(&self.tracker.run_segments).remove(&self.token);
        }
        if self.obsolete.load(Ordering::Acquire) {
            lock(&self.tracker.obsolete).push(self.desc.id);
        }
    }
}

/// A memtable with the bookkeeping the store keeps for it.
pub(crate) struct MemEntry {
    pub mem: Arc<Memtable>,
    id: u64,
    /// Highest sequence the journal thread applied to it.
    pub last_seq: AtomicU64,
    /// Journal payload bytes applied to it.
    pub journal_bytes: AtomicU64,
    oldest_segment: AtomicU64,
    tracker: Arc<FileTracker>,
}

impl MemEntry {
    pub fn new(mem: Memtable, tracker: Arc<FileTracker>, last_seq: Seq) -> Arc<MemEntry> {
        Arc::new(MemEntry {
            mem: Arc::new(mem),
            id: tracker.next_token(),
            last_seq: AtomicU64::new(last_seq),
            journal_bytes: AtomicU64::new(0),
            oldest_segment: AtomicU64::new(u64::MAX),
            tracker,
        })
    }

    /// Records that the memtable now holds a locator into `segment`.
    pub fn note_segment(&self, segment: u64) {
        let prev = self.oldest_segment.fetch_min(segment, Ordering::AcqRel);
        if segment < prev {
            lock(&self.tracker.mem_segments).insert(self.id, segment);
        }
    }

    pub fn last_seq(&self) -> Seq {
        self.last_seq.load(Ordering::Acquire)
    }
}

impl Drop for MemEntry {
    fn drop(&mut self) {
        if *self.oldest_segment.get_mut() != u64::MAX {
            lock(&self.tracker.mem_segments).remove(&self.id);
        }
    }
}

/// The runs of every level. L0 is ordered newest first; deeper levels by key.
#[derive(Clone, Debug, Default)]
pub(crate) struct Version {
    pub levels: [Vec<Arc<RunFile>>; NUM_LEVELS],
}

impl Version {
    pub fn sort(&mut self) {
        self.levels[0].sort_by(|a, b| {
            b.desc
                .largest_seq
                .cmp(&a.desc.largest_seq)
                .then(b.id().cmp(&a.id()))
        });
        for level in &mut self.levels[1..] {
            level.sort_by(|a, b| a.lo.cmp(&b.lo));
        }
    }

    /// A copy without `removed` and with `added` (already built).
    pub fn with_changes(&self, removed: &[u64], added: Vec<Arc<RunFile>>) -> Version {
        let mut next = self.clone();
        for level in &mut next.levels {
            level.retain(|run| !removed.contains(&run.id()));
        }
        for run in added {
            next.levels[run.level()].push(run);
        }
        next.sort();
        next
    }

    pub fn level_bytes(&self, level: usize) -> u64 {
        self.levels[level].iter().map(|r| r.size()).sum()
    }

    pub fn l0_count(&self) -> usize {
        self.levels[0].len()
    }

    /// The run of `level` (1..) whose range contains `key`.
    pub fn find_in_level(&self, level: usize, key: &[u8]) -> Option<&Arc<RunFile>> {
        let runs = &self.levels[level];
        // Runs are disjoint and sorted, so `hi` is sorted too.
        let i = runs.partition_point(|r| {
            if r.hi_exclusive {
                r.hi.as_ref() <= key
            } else {
                r.hi.as_ref() < key
            }
        });
        runs.get(i).filter(|r| r.contains(key))
    }

    pub fn overlapping(&self, level: usize, lo: &[u8], hi: Option<&[u8]>) -> Vec<Arc<RunFile>> {
        self.levels[level]
            .iter()
            .filter(|r| r.overlaps(lo, hi))
            .cloned()
            .collect()
    }

    pub fn runs(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.levels.iter().flatten()
    }
}

/// What a reader pins: the active memtable, the immutable ones (newest first) and a
/// version.
pub(crate) struct SuperVersion {
    pub mem: Arc<MemEntry>,
    pub imm: Vec<Arc<MemEntry>>,
    pub version: Arc<Version>,
}

impl SuperVersion {
    pub fn memtables(&self) -> impl Iterator<Item = &Arc<MemEntry>> {
        std::iter::once(&self.mem).chain(self.imm.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::internal_key;

    fn desc(id: u64, level: u8, lo: &[u8], hi: &[u8], hi_tombstone_end: bool) -> RunDesc {
        RunDesc {
            id,
            level,
            file_size: 100,
            smallest: Bytes::from(internal_key(lo, 5, Kind::Put)),
            largest: Bytes::from(if hi_tombstone_end {
                internal_key(hi, MAX_SEQ, Kind::RangeDelete)
            } else {
                internal_key(hi, 5, Kind::Put)
            }),
            smallest_seq: 1,
            largest_seq: id,
            num_entries: 1,
            oldest_journal_segment: None,
        }
    }

    #[test]
    fn ns_unit_level_lookup_respects_exclusive_tombstone_ends() {
        let tracker = Arc::new(FileTracker::default());
        let mut v = Version::default();
        v.levels[1] = vec![
            RunFile::new(desc(1, 1, b"a", b"f", true), tracker.clone()),
            RunFile::new(desc(2, 1, b"f", b"k", false), tracker.clone()),
        ];
        v.sort();
        assert_eq!(v.find_in_level(1, b"e").unwrap().id(), 1);
        assert_eq!(v.find_in_level(1, b"f").unwrap().id(), 2);
        assert_eq!(v.find_in_level(1, b"k").unwrap().id(), 2);
        assert!(v.find_in_level(1, b"l").is_none());
        assert!(!v.levels[1][0].overlaps(b"f", Some(b"g")));
        assert!(v.levels[1][1].overlaps(b"k", None));
    }

    #[test]
    fn ns_unit_obsolete_runs_are_collected_after_the_last_reference() {
        let tracker = Arc::new(FileTracker::default());
        let mut d = desc(3, 0, b"a", b"b", false);
        d.oldest_journal_segment = Some(7);
        let run = RunFile::new(d, tracker.clone());
        assert_eq!(tracker.min_referenced_segment(), Some(7));
        let pinned = run.clone();
        run.mark_obsolete();
        drop(run);
        assert!(tracker.take_obsolete().is_empty());
        drop(pinned);
        assert_eq!(tracker.take_obsolete(), vec![3]);
        assert_eq!(tracker.min_referenced_segment(), None);
    }
}
