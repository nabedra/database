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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;

use crate::iter::range_del::FragmentedRangeTombstones;
use crate::manifest::RunDesc;
use crate::memtable::Memtable;
use crate::run::Run;
use crate::source::RangeTombstone;
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
    /// Resident range tombstones of live runs (their raw and fragmented copies), charged
    /// to the pool overhead (plan section 5 "Memory").
    tombstone_bytes: AtomicUsize,
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

    /// Bytes of range tombstones that live runs keep resident.
    pub fn tombstone_bytes(&self) -> usize {
        self.tombstone_bytes.load(Ordering::Relaxed)
    }

    pub fn take_obsolete(&self) -> Vec<u64> {
        std::mem::take(&mut *lock(&self.obsolete))
    }

    fn next_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }
}

/// One live run file: what every version object of that run shares. The file is deleted
/// only when this drops after being marked obsolete, which is after the last version
/// (current or pinned by a snapshot) that holds the run in any level is gone. A trivial
/// move gives the run a new [`RunFile`] for its new level that shares this handle, so a
/// snapshot that still holds the run at its old level keeps the file (RV-KV2 finding 1).
pub(crate) struct RunHandle {
    id: u64,
    token: u64,
    has_segment: bool,
    tombstones: OnceLock<Arc<FragmentedRangeTombstones>>,
    /// Bytes charged to `FileTracker::tombstone_bytes` for this run.
    tombstone_charge: AtomicUsize,
    obsolete: AtomicBool,
    tracker: Arc<FileTracker>,
}

impl Drop for RunHandle {
    fn drop(&mut self) {
        let charge = *self.tombstone_charge.get_mut();
        if charge > 0 {
            self.tracker
                .tombstone_bytes
                .fetch_sub(charge, Ordering::Relaxed);
        }
        if self.has_segment {
            lock(&self.tracker.run_segments).remove(&self.token);
        }
        if self.obsolete.load(Ordering::Acquire) {
            lock(&self.tracker.obsolete).push(self.id);
        }
    }
}

/// One live run of a version, at one level.
pub(crate) struct RunFile {
    pub desc: RunDesc,
    /// User-key range: `[lo, hi]`, or `[lo, hi)` when the largest key is a range
    /// tombstone end.
    pub lo: Bytes,
    pub hi: Bytes,
    pub hi_exclusive: bool,
    /// An L0 run is kept open for its whole life at L0 (index, filter and tombstones
    /// resident), like RocksDB's `pin_l0_filter_and_index_blocks_in_cache`.
    pinned: OnceLock<Arc<Run>>,
    handle: Arc<RunHandle>,
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
        let token = tracker.next_token();
        if let Some(segment) = desc.oldest_journal_segment {
            lock(&tracker.run_segments).insert(token, segment);
        }
        let handle = Arc::new(RunHandle {
            id: desc.id,
            token,
            has_segment: desc.oldest_journal_segment.is_some(),
            tombstones: OnceLock::new(),
            tombstone_charge: AtomicUsize::new(0),
            obsolete: AtomicBool::new(false),
            tracker,
        });
        Self::with_handle(desc, handle)
    }

    /// The same run file at another level (a trivial move): shares the file handle.
    pub fn moved(&self, level: u8) -> Arc<RunFile> {
        let mut desc = self.desc.clone();
        desc.level = level;
        Self::with_handle(desc, self.handle.clone())
    }

    fn with_handle(desc: RunDesc, handle: Arc<RunHandle>) -> Arc<RunFile> {
        let (lo, hi, hi_exclusive) = user_range(&desc.smallest, &desc.largest);
        Arc::new(RunFile {
            desc,
            lo,
            hi,
            hi_exclusive,
            pinned: OnceLock::new(),
            handle,
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

    /// Marks the run file for deletion once nothing references it at any level.
    pub fn mark_obsolete(&self) {
        self.handle.obsolete.store(true, Ordering::Release);
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

    /// The run's range tombstones, fragmented once. The raw and fragmented copies are
    /// charged to the tracker's `tombstone_bytes` while the run file lives.
    pub fn tombstones(&self, run: &Run) -> Arc<FragmentedRangeTombstones> {
        let h = &self.handle;
        h.tombstones
            .get_or_init(|| {
                let raw = run.range_tombstones();
                let charge = 2 * tombstone_charge(raw);
                h.tombstone_charge.store(charge, Ordering::Relaxed);
                h.tracker
                    .tombstone_bytes
                    .fetch_add(charge, Ordering::Relaxed);
                Arc::new(FragmentedRangeTombstones::new(raw, MAX_SEQ))
            })
            .clone()
    }

    /// The fragmented range tombstones, if a reader loaded them already.
    pub fn cached_tombstones(&self) -> Option<Arc<FragmentedRangeTombstones>> {
        self.handle.tombstones.get().cloned()
    }

    /// Keeps `run` open for as long as this object lives (L0 runs).
    pub fn pin(&self, run: Arc<Run>) {
        let _ = self.pinned.set(run);
    }

    pub fn pinned(&self) -> Option<&Arc<Run>> {
        self.pinned.get()
    }
}

/// Resident bytes of a set of range tombstones (keys plus per-entry overhead).
pub(crate) fn tombstone_charge(tombstones: &[RangeTombstone]) -> usize {
    tombstones
        .iter()
        .map(|t| t.start.len() + t.end.len() + 64)
        .sum()
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
    /// Set once the memtable is flushed while readers still hold it: the budget group's
    /// pinned-bytes counter and the bytes charged to it until this drops.
    pinned: OnceLock<(Arc<AtomicUsize>, usize)>,
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
            pinned: OnceLock::new(),
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

    /// Lowest journal segment a locator in this memtable points into (`u64::MAX`: none).
    pub fn oldest_segment(&self) -> u64 {
        self.oldest_segment.load(Ordering::Acquire)
    }

    /// The memtable is flushed: from now on its budget charge counts as pinned (held only
    /// by readers), not as memory a flush can free.
    pub fn mark_flushed(&self, pinned: &Arc<AtomicUsize>) {
        let bytes = self.mem.approximate_memory();
        if self.pinned.set((pinned.clone(), bytes)).is_ok() {
            pinned.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

impl Drop for MemEntry {
    fn drop(&mut self) {
        if let Some((counter, bytes)) = self.pinned.get() {
            counter.fetch_sub(*bytes, Ordering::Relaxed);
        }
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

    #[test]
    fn ns03_unit_a_moved_run_shares_its_file_with_the_old_level() {
        let tracker = Arc::new(FileTracker::default());
        let mut d = desc(4, 1, b"a", b"b", false);
        d.oldest_journal_segment = Some(9);
        let old = RunFile::new(d, tracker.clone());
        let moved = old.moved(2);
        assert_eq!(moved.level(), 2);
        // A compaction consumes the moved run while a snapshot still holds the old one.
        moved.mark_obsolete();
        drop(moved);
        assert!(tracker.take_obsolete().is_empty());
        assert_eq!(tracker.min_referenced_segment(), Some(9));
        drop(old);
        assert_eq!(tracker.take_obsolete(), vec![4]);
        assert_eq!(tracker.min_referenced_segment(), None);
    }
}
