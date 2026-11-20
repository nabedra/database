//! The [`Store`]: one LSM over a journal, memtables and leveled runs (task NS5, plan
//! sections 2 to 4).
//!
//! # Write path
//!
//! [`Store::submit`] hands the batch to the journal, which writes it (and syncs, for
//! `Sync`) on its thread and then calls this store's sink. The sink stalls while the
//! worker's memtable budget is exhausted or L0 is at its stop trigger, inserts into the
//! active memtable and, when the memtable is full or the replay window is too large,
//! freezes it: a new memtable is published before any of its records become visible.
//!
//! # Flush and compaction
//!
//! A frozen memtable is flushed on the shared [`Background`] pool: the journal is synced
//! first (no run may point at journal bytes that are not durable), then the run is
//! written and synced, the run directory synced, and a manifest edit
//! `{add run, flushed_seq, segments}` appended and synced. Only then is the run
//! published and are covered journal segments retired. Compaction writes its outputs the
//! same way and marks its inputs obsolete after the edit syncs; their files are deleted
//! once no snapshot can read them.
//!
//! # Reads
//!
//! [`Store::snapshot`] registers the current visible sequence and pins the memtables and
//! version. Point reads apply the newest covering range tombstone across every source.

mod background;
mod budget;
mod journal_gc;
mod level_iter;
mod read;
mod snapshots;
#[cfg(test)]
mod tests;

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;

pub use background::{Background, THREAD_STACK_BYTES};
pub use read::{Iter, Snapshot};

use crate::batch::{WriteBatch, prefix_successor};
use crate::cache::BlockCache;
use crate::compaction::{self, OutputSpec, OutputStats};
use crate::error::{Damage, DamageReason, Error, FileId, Result};
use crate::fs::{Fs, FsLock};
use crate::gc;
use crate::iter::MergingIterator;
pub use crate::journal::{Commit, Durability, JournalOptions};
use crate::journal::{Journal, JournalSink, RecordKind};
use crate::manifest::{
    self, AppendError, JOURNAL_DIR, ManifestState, ManifestWriter, RUNS_DIR, RunDesc, VersionEdit,
};
use crate::memtable::{Memtable, MemtableBudget, MemtableOptions};
use crate::quarantine::{Quarantine, QuarantineLimit, single_scope};
use crate::recovery;
use crate::run::{
    DEFAULT_MAX_OPEN_RUNS, RunMeta, RunReadContext, RunStats, RunWriterOptions, TableCache,
    run_file_name,
};
use crate::source::{InternalIterator, RangeTombstone};
use crate::types::{Locator, Seq};
use crate::version::{FileTracker, LevelOptions, MemEntry, RunFile, SuperVersion, Version};

use self::background::Job;
use self::budget::BudgetGroup;
use self::journal_gc::GcState;
use self::snapshots::Snapshots;

/// How long a stalled writer sleeps between checks.
const STALL_POLL: Duration = Duration::from_millis(5);
/// Background work that failed with `NoSpace` retries after this long.
const NO_SPACE_RETRY: Duration = Duration::from_millis(200);
/// Other background failures (EIO on a run write or an input read) are retried this many
/// times, with growing delays, before the store latches `RecoveryRequired`.
const MAX_BACKGROUND_FAILURES: u32 = 3;

/// A store-wide key: every key belongs to no scope.
pub fn store_wide(_: &[u8]) -> usize {
    0
}

pub struct Options {
    pub fs: Arc<dyn Fs>,
    /// Worker-shared block cache (`cache_bytes`).
    pub cache: Arc<BlockCache>,
    /// Worker-shared memtable budget (`write_buffer_bytes`).
    pub memtables: Arc<MemtableBudget>,
    /// Worker-shared background threads.
    pub background: Arc<Background>,
    pub journal: JournalOptions,
    pub levels: LevelOptions,
    pub max_open_runs: usize,
    /// Length of a key's scope prefix; 0 means store-wide.
    pub scope_of: fn(&[u8]) -> usize,
    pub max_quarantined: QuarantineLimit,
    /// Memtable sharding.
    pub memtable: MemtableOptions,
    /// One memtable is frozen at this size even when the budget has room.
    pub memtable_bytes: usize,
    /// A memtable is frozen once the journal written into it exceeds this, which bounds
    /// replay at open.
    pub max_replay_bytes: u64,
    /// Journal GC relocates live journal values out of the oldest segments while the
    /// journal's live segments exceed this (plan section 2, "Journal values").
    pub max_journal_bytes: u64,
    pub run_writer: RunWriterOptions,
}

impl Options {
    /// Production defaults (plan Decisions) around the worker-shared resources.
    pub fn new(
        fs: Arc<dyn Fs>,
        cache: Arc<BlockCache>,
        memtables: Arc<MemtableBudget>,
        background: Arc<Background>,
    ) -> Options {
        Options {
            fs,
            cache,
            memtables,
            background,
            journal: JournalOptions::default(),
            levels: LevelOptions::default(),
            max_open_runs: DEFAULT_MAX_OPEN_RUNS,
            scope_of: store_wide,
            max_quarantined: QuarantineLimit::default(),
            memtable: MemtableOptions::default(),
            memtable_bytes: 64 << 20,
            max_replay_bytes: 64 << 20,
            max_journal_bytes: 512 << 20,
            run_writer: RunWriterOptions::default(),
        }
    }
}

/// What open found and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub replayed_records: u64,
    pub tail_truncations: u64,
    pub tail_bytes: u64,
    pub recovery_ms: u64,
    /// L3: the manifest's torn final record was dropped.
    pub manifest_torn_tail: bool,
    /// L4: unreferenced run files and stale temp files deleted.
    pub orphans_removed: u64,
    /// Sequences the manifest's `flushed_seq` covers that the journal no longer held:
    /// its end was behind the manifest (synced journal bytes lost and read as a torn
    /// tail or an end marker). Their state is in runs, so nothing is lost; open voids
    /// the numbers so they are never reused. Nonzero means the disk lost synced bytes.
    pub journal_behind_manifest: u64,
}

pub struct Opened {
    pub store: Store,
    /// Every quarantined scope, found by this open or persisted by an earlier one.
    pub quarantined: Vec<Damage>,
    /// The scopes this open quarantined, one entry per scope: damage found by this
    /// open in a scope that no earlier open or run-time quarantine had persisted. R6
    /// ("more than N scopes quarantined by one open") counts these only.
    pub quarantined_now: Vec<Damage>,
    pub recovery: RecoveryReport,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub journal_syncs: u64,
    /// Syncs of runs, manifests and directories.
    pub file_syncs: u64,
    pub commits: u64,
    pub synced_commits: u64,
    pub group_commits: u64,
    /// Encoded batch bytes submitted.
    pub user_bytes: u64,
    pub journal_bytes: u64,
    pub flush_bytes: u64,
    pub compaction_bytes: u64,
    pub relocated_bytes: u64,
    pub stall_ms: u64,
    pub memtable_bytes: u64,
    pub l0_runs: u32,
    pub recovery_ms: u64,
    pub corruption_detected: u64,
    pub tail_truncations: u64,
    pub quarantined_scopes: u32,
    /// `RecoveryReport::journal_behind_manifest` of the last open.
    pub journal_behind_manifest: u64,
    /// Reads that met a locator into a recycled segment: a journal GC invariant broke.
    /// Each one latches `RecoveryRequired`.
    pub recycled_value_reads: u64,
    /// Memory this store keeps outside the block cache and the memtable budget, charged
    /// to the pool overhead (plan section 5 "Memory"): see [`MemoryUsage`].
    pub overhead_bytes: u64,
}

/// Resident memory of one store, by budget (plan section 5 "Memory").
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryUsage {
    /// Active and immutable memtables (inside `write_buffer_bytes`).
    pub memtables: u64,
    /// Pool overhead: open run writers of flushes and compactions (write buffer, data
    /// block, about 8 B per key for the filter and one index entry per data block).
    pub run_writers: u64,
    /// Pool overhead: range tombstones of live runs, raw and fragmented.
    pub range_tombstones: u64,
    /// Pool overhead: versions and the manifest state (about 200 B per run).
    pub versions: u64,
    /// Pool overhead: the snapshot registry.
    pub snapshots: u64,
    /// Memtables already flushed but still held by snapshots or iterators, over every
    /// store sharing the memtable budget. Charged to `write_buffer_bytes`, but writers
    /// do not stall on them (see `store/budget.rs`).
    pub memtables_pinned: u64,
    /// Block cache: bytes reserved or pinned there (open runs' top-level indexes, held
    /// blocks). Shared by every store on the cache; reported here for convenience.
    pub cache_pinned: u64,
}

impl MemoryUsage {
    /// The part charged to the pool overhead.
    pub fn overhead(&self) -> u64 {
        self.run_writers + self.range_tombstones + self.versions + self.snapshots
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The manifest and the current version, changed together under one lock.
struct VersionSet {
    /// `None` until open writes the first manifest of this session.
    manifest: Option<ManifestWriter>,
    state: ManifestState,
    current: Arc<Version>,
    /// Next file number to hand out (at least `state.next_file`).
    next_file: u64,
    /// Round-robin compaction position per level.
    cursor: Vec<Bytes>,
}

#[derive(Default)]
struct Jobs {
    closing: bool,
    active: usize,
}

#[derive(Default)]
struct Counters {
    flush_bytes: AtomicU64,
    compaction_bytes: AtomicU64,
    file_syncs: AtomicU64,
    user_bytes: AtomicU64,
    open_corruptions: AtomicU64,
    relocated_bytes: AtomicU64,
    recycled_reads: AtomicU64,
    journal_behind: AtomicU64,
}

type DamageHook = Arc<dyn Fn(Damage) + Send + Sync>;

pub(crate) struct Inner {
    dir: PathBuf,
    runs_dir: PathBuf,
    fs: Arc<dyn Fs>,
    levels: LevelOptions,
    scope_of: fn(&[u8]) -> usize,
    memtable_options: MemtableOptions,
    memtable_bytes: usize,
    max_replay_bytes: u64,
    max_journal_bytes: u64,
    segment_bytes: u64,
    max_group_bytes: usize,
    run_writer: RunWriterOptions,
    budget: Arc<MemtableBudget>,
    group: Arc<BudgetGroup>,
    cache: Arc<BlockCache>,
    background: Arc<Background>,
    table_cache: TableCache,
    run_stats: Arc<RunStats>,
    tracker: Arc<FileTracker>,
    journal: OnceLock<Arc<Journal>>,
    sv: RwLock<Arc<SuperVersion>>,
    /// Bumped (inside the `sv` write lock) whenever `sv` changes; readers cache the
    /// super-version per thread under it (see `current_sv`).
    sv_version: AtomicU64,
    /// Unique per open store, for the per-thread super-version cache.
    store_id: u64,
    /// Serializes memtable inserts (journal thread) with freezes.
    active: Mutex<()>,
    replaying: AtomicBool,
    vs: Mutex<VersionSet>,
    flush_lock: Mutex<()>,
    compaction_lock: Mutex<()>,
    snapshots: Snapshots,
    /// Serializes the slow path of snapshot creation with readers of the snapshot list.
    snapshot_lock: Mutex<()>,
    gc: Mutex<GcState>,
    /// Serializes journal GC steps (background and `Store::gc_journal`).
    gc_pass: Mutex<()>,
    /// Run-writer memory of running flushes and compactions.
    writer_memory: AtomicUsize,
    /// L0 run count of the current version (write pacing reads it without a lock).
    l0_runs: AtomicUsize,
    /// Write pacing under L0 slowdown: the time (ns since `epoch`) up to which the
    /// delayed write rate is already spent.
    pace_until_ns: AtomicU64,
    epoch: Instant,
    quarantine: Quarantine,
    latched: AtomicBool,
    jobs: Mutex<Jobs>,
    jobs_done: Condvar,
    /// A flush is waiting for a journal sync: stalled sinks must let the journal thread go.
    syncs_waiting: AtomicUsize,
    /// While a sync waits, records up to this sequence pass a stall (they are queued
    /// before the sync)...
    bypass_seq: AtomicU64,
    /// ... and later records pass while this byte allowance lasts (they may share the
    /// sync's journal group, which completes only once all its records are applied).
    /// RV-KV2 finding 4: the bypass admits at most one group per sync request.
    bypass_bytes: AtomicI64,
    /// Consecutive failed background jobs (other than `NoSpace`).
    background_failures: AtomicU32,
    /// Mirrors `jobs.closing` for the journal thread's hot path.
    closing_flag: AtomicBool,
    flush_queued: AtomicBool,
    compaction_queued: AtomicBool,
    l0_changed: Condvar,
    l0_lock: Mutex<()>,
    damage_hook: RwLock<Option<DamageHook>>,
    pending_damage: Mutex<Vec<Damage>>,
    counters: Counters,
    recovery_ms: AtomicU64,
    lock_file: Mutex<Option<FsLock>>,
    self_ref: OnceLock<Weak<Inner>>,
}

/// An open store. Dropping it closes it: background work for it stops, the journal
/// writes its clean-close record and the directory lock is released.
pub struct Store {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("dir", &self.inner.dir)
            .finish()
    }
}

struct Sink(Weak<Inner>);

impl JournalSink for Sink {
    fn apply(
        &self,
        kind: RecordKind,
        first_seq: Seq,
        batch: &WriteBatch,
        locators: &[Locator],
    ) -> Result<()> {
        match self.0.upgrade() {
            Some(inner) => inner.apply(kind, first_seq, batch, locators),
            None => Err(Error::Closed),
        }
    }
}

impl Store {
    /// Opens or creates the store in `dir` and recovers it (plan section 3).
    pub fn open(dir: &Path, options: Options) -> Result<Opened> {
        let started = Instant::now();
        let fs = options.fs.clone();
        let opened = recovery::prepare(&*fs, dir, options.scope_of)?;
        let runs_dir = dir.join(RUNS_DIR);
        let ctx = RunReadContext::new(options.cache.clone());
        let run_stats = ctx.stats.clone();
        let table_cache = TableCache::new(fs.clone(), &runs_dir, ctx, options.max_open_runs);
        let mut state = opened.state;
        let mut new_damage: Vec<Damage> = Vec::new();
        let mut edit = VersionEdit::default();

        // G2 at open: referenced runs whose file is missing or whose footer is damaged.
        let mut open_corruptions = 0u64;
        for desc in state.runs.values() {
            match table_cache.get(desc.id) {
                Ok(_) => {}
                Err(e @ Error::Corruption { .. })
                | Err(e @ Error::Io(std::io::ErrorKind::NotFound)) => {
                    open_corruptions += u64::from(matches!(e, Error::Io(_)));
                    let (lo, hi, excl) = crate::version::user_range(&desc.smallest, &desc.largest);
                    let Some(scope) = single_scope(options.scope_of, &lo, &hi, excl) else {
                        return Err(match e {
                            Error::Corruption { .. } => e,
                            _ => Error::Corruption {
                                file: FileId::Run { id: desc.id },
                                offset: 0,
                                what: "a referenced run file is missing",
                            },
                        });
                    };
                    table_cache.evict(desc.id);
                    edit.quarantined_runs.push((desc.id, scope.clone()));
                    new_damage.push(Damage {
                        scope,
                        reason: DamageReason::RunFile,
                        file: FileId::Run { id: desc.id },
                        first_bad_seq: None,
                    });
                }
                Err(e) => return Err(e),
            }
        }
        state
            .apply(&edit)
            .expect("quarantining referenced runs is consistent");

        let tracker = Arc::new(FileTracker::default());
        let mut version = Version::default();
        for desc in state.runs.values() {
            let run = RunFile::new(desc.clone(), tracker.clone());
            if desc.level == 0
                && let Ok(open) = table_cache.get(desc.id)
            {
                run.pin(open);
            }
            version.levels[desc.level as usize].push(run);
        }
        version.sort();
        let budget = options.memtables.clone();
        let first_mem = MemEntry::new(
            Memtable::new(options.memtable, Some(budget.clone())),
            tracker.clone(),
            state.flushed_seq,
        );
        let persisted_scopes: Vec<Bytes> =
            state.quarantine.iter().map(|d| d.scope.clone()).collect();
        let mut quarantine_list = state.quarantine.clone();
        for d in &new_damage {
            if !quarantine_list.iter().any(|q| q.scope == d.scope) {
                quarantine_list.push(d.clone());
            }
        }
        let flushed_seq = state.flushed_seq;
        let l0_runs = version.l0_count();
        let inner = Arc::new(Inner {
            dir: dir.to_path_buf(),
            runs_dir: runs_dir.clone(),
            fs: fs.clone(),
            levels: options.levels.clone(),
            scope_of: options.scope_of,
            memtable_options: options.memtable,
            memtable_bytes: options.memtable_bytes.max(1),
            max_replay_bytes: options.max_replay_bytes.max(1),
            max_journal_bytes: options.max_journal_bytes,
            segment_bytes: options.journal.segment_bytes,
            max_group_bytes: options.journal.max_group_bytes,
            run_writer: options.run_writer,
            group: BudgetGroup::of(&budget),
            budget,
            cache: options.cache.clone(),
            background: options.background.clone(),
            table_cache,
            run_stats,
            tracker,
            journal: OnceLock::new(),
            sv: RwLock::new(Arc::new(SuperVersion {
                mem: first_mem,
                imm: Vec::new(),
                version: Arc::new(version),
            })),
            sv_version: AtomicU64::new(0),
            store_id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            active: Mutex::new(()),
            replaying: AtomicBool::new(true),
            vs: Mutex::new(VersionSet {
                manifest: None,
                state: state.clone(),
                current: Arc::new(Version::default()),
                next_file: state.next_file,
                cursor: vec![Bytes::new(); crate::version::NUM_LEVELS],
            }),
            flush_lock: Mutex::new(()),
            compaction_lock: Mutex::new(()),
            snapshots: Snapshots::default(),
            snapshot_lock: Mutex::new(()),
            gc: Mutex::new(GcState::default()),
            gc_pass: Mutex::new(()),
            writer_memory: AtomicUsize::new(0),
            l0_runs: AtomicUsize::new(l0_runs),
            pace_until_ns: AtomicU64::new(0),
            epoch: Instant::now(),
            quarantine: Quarantine::new(quarantine_list),
            latched: AtomicBool::new(false),
            jobs: Mutex::new(Jobs::default()),
            jobs_done: Condvar::new(),
            syncs_waiting: AtomicUsize::new(0),
            bypass_seq: AtomicU64::new(0),
            bypass_bytes: AtomicI64::new(0),
            background_failures: AtomicU32::new(0),
            closing_flag: AtomicBool::new(false),
            flush_queued: AtomicBool::new(false),
            compaction_queued: AtomicBool::new(false),
            l0_changed: Condvar::new(),
            l0_lock: Mutex::new(()),
            damage_hook: RwLock::new(None),
            pending_damage: Mutex::new(Vec::new()),
            counters: Counters::default(),
            recovery_ms: AtomicU64::new(0),
            lock_file: Mutex::new(Some(opened.lock)),
            self_ref: OnceLock::new(),
        });
        let _ = inner.self_ref.set(Arc::downgrade(&inner));
        inner.group.join(Arc::downgrade(&inner));
        {
            let mut vs = lock(&inner.vs);
            vs.current = inner.current_sv().version.clone();
        }
        inner
            .counters
            .open_corruptions
            .fetch_add(open_corruptions, Ordering::Relaxed);

        // Replay the journal above flushed_seq into the memtables.
        let sink: Arc<dyn JournalSink> = Arc::new(Sink(Arc::downgrade(&inner)));
        let (journal, report) = Journal::open(
            fs.clone(),
            &dir.join(JOURNAL_DIR),
            options.journal.clone(),
            flushed_seq,
            sink,
        )?;
        let journal = Arc::new(journal);
        journal.set_value_cache(options.cache.clone(), inner.table_cache.context().namespace);
        let _ = inner.journal.set(journal.clone());
        inner.replaying.store(false, Ordering::Release);
        inner
            .counters
            .journal_behind
            .store(report.voided_at_open, Ordering::Relaxed);

        // G1: journal damage of a group scope. Damage older than a release of that scope
        // is history.
        for damage in report.damage {
            let released_after = state.released.iter().any(|(scope, seq)| {
                *scope == damage.scope && damage.first_bad_seq.is_none_or(|bad| bad < *seq)
            });
            if released_after || damage.scope.is_empty() {
                continue;
            }
            if inner.quarantine.add(damage.clone()) {
                new_damage.push(damage);
            }
        }

        // The journal must still hold every segment the manifest recorded.
        let live = journal.segments();
        for &(segment, epoch) in &state.segments {
            if !live
                .iter()
                .any(|s| s.segment == segment && s.epoch == epoch)
            {
                return Err(Error::Corruption {
                    file: FileId::Journal { segment },
                    offset: 0,
                    what: "a journal segment the manifest records is missing",
                });
            }
        }

        // Scopes this open quarantined. A scope persisted by an earlier open or at run
        // time, or named twice (two damaged runs of one group), counts once or not at all.
        let mut quarantined_now: Vec<Damage> = Vec::new();
        for damage in &new_damage {
            if !persisted_scopes.contains(&damage.scope)
                && !quarantined_now.iter().any(|d| d.scope == damage.scope)
            {
                quarantined_now.push(damage.clone());
            }
        }

        // R6: damage this wide is more likely systemic than local.
        if quarantined_now.len() > options.max_quarantined.max_scopes {
            return Err(Error::Corruption {
                file: FileId::Other,
                offset: 0,
                what: "too many scopes damaged at once",
            });
        }

        // A fresh manifest holding the recovered state, then CURRENT, then cleanup.
        let mut orphans = 0;
        {
            let mut vs = lock(&inner.vs);
            let mut snapshot = vs.state.clone();
            snapshot
                .apply(&VersionEdit {
                    quarantined: new_damage.clone(),
                    segments: Some(inner.segments_to_record(&[])),
                    ..VersionEdit::default()
                })
                .expect("consistent");
            let number = snapshot.next_file;
            snapshot.next_file += 1;
            let writer = ManifestWriter::create(&*fs, dir, number, &snapshot)?;
            manifest::set_current(&*fs, dir, number)?;
            vs.next_file = snapshot.next_file;
            vs.state = snapshot;
            vs.manifest = Some(writer);
            orphans += recovery::cleanup(&*fs, dir, number, &vs.state)?;
        }

        let recovery = RecoveryReport {
            replayed_records: report.replayed_records,
            tail_truncations: report.tail_truncations,
            tail_bytes: report.tail_bytes,
            recovery_ms: started.elapsed().as_millis() as u64,
            manifest_torn_tail: opened.manifest_torn_tail,
            orphans_removed: orphans,
            journal_behind_manifest: report.voided_at_open,
        };
        inner
            .recovery_ms
            .store(recovery.recovery_ms, Ordering::Relaxed);
        if inner.should_switch(&inner.current_sv().mem) {
            inner.freeze_active();
        }
        inner.schedule_flush();
        inner.schedule_compaction();
        let quarantined = inner.quarantine.list();
        Ok(Opened {
            store: Store { inner },
            quarantined,
            quarantined_now,
            recovery,
        })
    }

    /// Queues `batch`. Never blocks on I/O. Submission order is commit order. `on_done`
    /// is dropped on the journal thread after the write is visible (and durable, for
    /// `Sync`), or after it failed, and before the returned commit completes.
    pub fn submit(
        &self,
        batch: WriteBatch,
        durability: Durability,
        on_done: Box<dyn Any + Send>,
    ) -> Result<Commit> {
        self.inner.check_usable()?;
        self.inner
            .counters
            .user_bytes
            .fetch_add(batch.encoded_bytes() as u64, Ordering::Relaxed);
        self.inner.journal().submit(batch, durability, on_done)
    }

    /// Write pacing while L0 is at its slowdown trigger (RocksDB's delayed write rate):
    /// charges `bytes` to the store's pacing budget and returns how long the caller
    /// should wait before submitting them. Zero when writes are not slowed. Only the
    /// writers that call this wait; the journal thread never sleeps for pacing, so other
    /// scopes keep their latency. An async caller awaits the delay (a timer), a blocking
    /// caller uses [`Store::pace`]. At `l0_stop_writes` the journal thread itself stalls
    /// every write until compaction catches up, paced or not.
    pub fn write_delay(&self, bytes: usize) -> Duration {
        self.inner.write_delay(bytes)
    }

    /// [`Store::write_delay`], slept on the calling thread. Must not be called on an
    /// async runtime thread.
    pub fn pace(&self, bytes: usize) {
        let delay = self.inner.write_delay(bytes);
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }

    /// Durably covers every write submitted before this call.
    pub fn sync(&self) -> Result<Commit> {
        self.inner.check_usable()?;
        self.inner.journal().sync()
    }

    /// A consistent read view at the current visible sequence.
    pub fn snapshot(&self) -> Snapshot {
        self.inner.snapshot()
    }

    /// Freezes the active memtable and flushes every memtable to runs. Blocks; must not
    /// be called on an async runtime thread.
    pub fn flush(&self) -> Result<()> {
        self.inner.check_usable()?;
        self.inner.freeze_active();
        self.inner.flush_imms()
    }

    /// Runs compactions until no level needs one. Blocks (tests and maintenance).
    pub fn compact(&self) -> Result<()> {
        self.inner.check_usable()?;
        while self.inner.compact_once()? {}
        Ok(())
    }

    /// Runs journal GC until the journal fits `max_journal_bytes` or nothing more can be
    /// done now: relocates live journal values out of the oldest segments and retires
    /// them. Returns false when retirement still waits (for a snapshot older than the
    /// relocation, typically). Blocks (tests and maintenance); the background pool runs
    /// the same steps after flushes.
    pub fn gc_journal(&self) -> Result<bool> {
        self.inner.check_usable()?;
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut backoff = Duration::from_micros(200);
        while Instant::now() < deadline {
            match self.inner.journal_gc()? {
                None => return Ok(true),
                Some(wait) => {
                    std::thread::sleep(backoff.min(wait));
                    backoff *= 2;
                }
            }
        }
        Ok(false)
    }

    /// Deletes everything under `prefix`, makes that durable, flushes past it, records the
    /// release in the manifest, lifts the scope's quarantine and drops its damaged runs
    /// (plan section 3, rule 4). Returns the release's sequence once all of that is
    /// done, so writes under `prefix` after it returns survive any crash (RV-KV2
    /// finding 8). Blocks on a sync and a flush: must not be called on an async runtime
    /// thread.
    pub fn release_scope(&self, prefix: &[u8]) -> Result<Seq> {
        self.inner.check_usable()?;
        let end = prefix_successor(prefix).ok_or(Error::InvalidArgument(
            "release_scope needs a bounded prefix",
        ))?;
        let mut batch = WriteBatch::new();
        batch.set_scope(prefix);
        batch.delete_range(prefix, &end);
        let seq = self
            .inner
            .journal()
            .submit(batch, Durability::Sync, Box::new(()))?
            .wait()?;
        self.inner
            .finish_release(Bytes::copy_from_slice(prefix), seq)?;
        Ok(seq)
    }

    /// Called for damage found at run time (G2 blocks, G3 values). Store-wide damage also
    /// latches `RecoveryRequired` (R5).
    pub fn on_damage(&self, notify: Box<dyn Fn(Damage) + Send + Sync>) {
        *self
            .inner
            .damage_hook
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::from(notify));
    }

    pub fn visible_seq(&self) -> Seq {
        self.inner.journal().visible_seq()
    }

    pub fn durable_seq(&self) -> Seq {
        self.inner.journal().durable_seq()
    }

    pub fn requires_recovery(&self) -> bool {
        self.inner.latched.load(Ordering::Acquire) || self.inner.journal().requires_recovery()
    }

    /// Scopes currently quarantined.
    pub fn quarantined(&self) -> Vec<Damage> {
        self.inner.quarantine.list()
    }

    /// True while `key` lies under a quarantined scope, found at open or at run time.
    /// One atomic load when nothing is quarantined; callers may test it on every write.
    pub fn is_quarantined(&self, key: &[u8]) -> bool {
        self.inner.quarantine.scope_of(key).is_some()
    }

    pub fn stats(&self) -> StoreStats {
        self.inner.stats()
    }

    /// Resident memory by budget.
    pub fn memory_usage(&self) -> MemoryUsage {
        self.inner.memory_usage()
    }

    /// Closes the store: waits for its background work, then closes the journal (clean
    /// close record) and releases the directory lock. Dropping does the same.
    ///
    /// **Blocks**: it waits for running flushes and compactions and joins the journal's
    /// threads (`kv-journal`, `kv-journal-prep`). An async owner must close or drop the
    /// last `Store` handle on a blocking thread (`spawn_blocking`), never on a runtime
    /// worker. Snapshots and iterators that outlive the store do not block when they drop:
    /// the journal's threads are already joined by then.
    pub fn close(self) -> Result<()> {
        self.inner.shutdown()
    }

    /// Runs per level, for tests and stats.
    pub fn level_runs(&self) -> Vec<usize> {
        self.inner
            .current_sv()
            .version
            .levels
            .iter()
            .map(Vec::len)
            .collect()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.inner.shutdown();
    }
}

impl Job for Inner {
    fn run_flush(&self) -> Option<Duration> {
        self.flush_queued.store(false, Ordering::Release);
        if !self.begin_job() {
            return None;
        }
        let result = self.flush_imms();
        self.end_job();
        match result {
            Ok(()) => {
                self.background_failures.store(0, Ordering::Relaxed);
                None
            }
            Err(Error::NoSpace) => {
                self.flush_queued.store(true, Ordering::Release);
                Some(NO_SPACE_RETRY)
            }
            Err(e) => self.background_failed(&e).inspect(|_| {
                self.flush_queued.store(true, Ordering::Release);
            }),
        }
    }

    fn run_compaction(&self) -> Option<Duration> {
        self.compaction_queued.store(false, Ordering::Release);
        if !self.begin_job() {
            return None;
        }
        let result = self.compact_once();
        // Journal GC runs on the compaction threads, after compaction (which may drop
        // shadowed locators) and never concurrently with itself.
        let gc_retry = match &result {
            Ok(_) => match self.journal_gc() {
                Ok(retry) => retry,
                Err(Error::NoSpace) => Some(NO_SPACE_RETRY),
                Err(_) => None,
            },
            Err(_) => None,
        };
        self.end_job();
        let pending_damage = !lock(&self.pending_damage).is_empty();
        match result {
            Ok(true) => {
                self.schedule_compaction();
                None
            }
            Ok(false) if pending_damage => {
                self.compaction_queued.store(true, Ordering::Release);
                Some(Duration::from_millis(10))
            }
            Ok(false) if gc_retry.is_some() => {
                self.compaction_queued.store(true, Ordering::Release);
                gc_retry
            }
            Err(Error::NoSpace) => {
                self.compaction_queued.store(true, Ordering::Release);
                Some(NO_SPACE_RETRY)
            }
            Err(e) => self.background_failed(&e).inspect(|_| {
                self.compaction_queued.store(true, Ordering::Release);
            }),
            Ok(false) => None,
        }
    }
}

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// Per-thread cache of super-versions: (store id, sv version, super-version). A hit
/// costs no lock (RV-KV2 finding 9/8b). Entries hold weak references, so a cached entry
/// never keeps old memtables or runs alive.
type SvCache = Vec<(u64, u64, Weak<SuperVersion>)>;

thread_local! {
    static SV_CACHE: std::cell::RefCell<SvCache> = const { std::cell::RefCell::new(Vec::new()) };
}

const SV_CACHE_ENTRIES: usize = 8;

impl Inner {
    fn journal(&self) -> &Arc<Journal> {
        self.journal.get().expect("journal opened")
    }

    fn weak_job(&self) -> Option<Weak<dyn Job>> {
        let weak: Weak<Inner> = self.self_ref.get()?.clone();
        Some(weak)
    }

    /// The current super-version. Usually served from a per-thread cache without a
    /// lock: the version counter is read first, and an entry cached under that version is
    /// the super-version published with it, or the caller sees a newer counter.
    pub(crate) fn current_sv(&self) -> Arc<SuperVersion> {
        let version = self.sv_version.load(Ordering::Acquire);
        let hit = SV_CACHE.with(|cache| {
            cache
                .borrow()
                .iter()
                .find(|(id, v, _)| *id == self.store_id && *v == version)
                .and_then(|(_, _, sv)| sv.upgrade())
        });
        if let Some(sv) = hit {
            return sv;
        }
        let (version, sv) = {
            let guard = self.sv.read().unwrap_or_else(PoisonError::into_inner);
            (self.sv_version.load(Ordering::Acquire), guard.clone())
        };
        SV_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.retain(|(id, _, w)| *id != self.store_id && w.strong_count() > 0);
            if cache.len() >= SV_CACHE_ENTRIES {
                cache.remove(0);
            }
            cache.push((self.store_id, version, Arc::downgrade(&sv)));
        });
        sv
    }

    /// Publishes `next` as the current super-version. The caller holds the write lock.
    fn publish_sv(&self, slot: &mut Arc<SuperVersion>, next: SuperVersion) {
        *slot = Arc::new(next);
        self.sv_version.fetch_add(1, Ordering::AcqRel);
    }

    /// A background job failed with `error` (not `NoSpace`). Retries a few times with
    /// growing delays, then latches `RecoveryRequired` so writers fail instead of
    /// stalling on memory forever (RV-KV2 finding 4).
    fn background_failed(&self, error: &Error) -> Option<Duration> {
        if matches!(error, Error::Closed | Error::RecoveryRequired) {
            return None;
        }
        let n = self.background_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if n >= MAX_BACKGROUND_FAILURES {
            self.latch();
            // Wake stalled writers so they see the latch.
            self.l0_changed.notify_all();
            return None;
        }
        Some(NO_SPACE_RETRY * n)
    }

    /// Unflushed memtable memory (active and immutable).
    fn unflushed_bytes(&self) -> usize {
        let sv = self.current_sv();
        sv.memtables().map(|m| m.mem.approximate_memory()).sum()
    }

    /// Another store sharing the budget is stalled: freeze and flush this one.
    fn flush_for_budget(&self) {
        if self.closing_flag.load(Ordering::Acquire) {
            return;
        }
        self.freeze_active();
        self.schedule_flush();
    }

    fn check_usable(&self) -> Result<()> {
        if self.latched.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        if lock(&self.jobs).closing {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn closing(&self) -> bool {
        self.closing_flag.load(Ordering::Acquire)
    }

    fn latch(&self) {
        self.latched.store(true, Ordering::Release);
    }

    /// Latches on errors whose outcome is unknown; passes the error through.
    fn latch_on(&self, error: Error) -> Error {
        if !matches!(error, Error::NoSpace | Error::Closed) {
            self.latch();
        }
        error
    }

    fn begin_job(&self) -> bool {
        let mut jobs = lock(&self.jobs);
        if jobs.closing || self.latched.load(Ordering::Acquire) {
            return false;
        }
        jobs.active += 1;
        true
    }

    fn end_job(&self) {
        let mut jobs = lock(&self.jobs);
        jobs.active -= 1;
        if jobs.active == 0 {
            self.jobs_done.notify_all();
        }
    }

    fn shutdown(&self) -> Result<()> {
        {
            let mut jobs = lock(&self.jobs);
            if jobs.closing && lock(&self.lock_file).is_none() {
                return Ok(());
            }
            jobs.closing = true;
            self.closing_flag.store(true, Ordering::Release);
            while jobs.active > 0 {
                jobs = self
                    .jobs_done
                    .wait(jobs)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }
        // Let stalled sinks go so the journal can drain.
        self.l0_changed.notify_all();
        let result = match self.journal.get() {
            Some(journal) => journal.close(),
            None => Ok(()),
        };
        lock(&self.lock_file).take();
        if self.latched.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        result
    }

    fn schedule_flush(&self) {
        if self.closing() || self.flush_queued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(job) = self.weak_job() {
            self.background.schedule_flush(job, Duration::ZERO);
        }
    }

    fn schedule_compaction(&self) {
        if self.closing() || self.compaction_queued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(job) = self.weak_job() {
            self.background.schedule_compaction(job, Duration::ZERO);
        }
    }

    // ----- write path (journal thread) -----

    fn apply(
        &self,
        kind: RecordKind,
        first_seq: Seq,
        batch: &WriteBatch,
        locators: &[Locator],
    ) -> Result<()> {
        let replaying = self.replaying.load(Ordering::Acquire);
        if !replaying {
            let last = first_seq + u64::from(batch.count()).max(1) - 1;
            self.stall(last, batch.encoded_bytes());
            if self.latched.load(Ordering::Acquire) {
                // The store needs recovery: fail the write (ambiguous) rather than grow
                // memtables that can no longer be flushed.
                return Err(Error::RecoveryRequired);
            }
        }
        if kind == RecordKind::Relocate {
            return self.apply_relocation(first_seq, batch, locators, replaying);
        }
        let _active = lock(&self.active);
        let mem = self.current_sv().mem.clone();
        mem.mem.insert(batch, first_seq, locators)?;
        if batch.count() > 0 {
            mem.last_seq
                .store(first_seq + u64::from(batch.count()) - 1, Ordering::Release);
        }
        mem.journal_bytes
            .fetch_add(batch.encoded_bytes() as u64, Ordering::Relaxed);
        if let Some(oldest) = locators.iter().map(|l| l.segment).min() {
            mem.note_segment(oldest);
        }
        if self.should_switch(&mem) {
            self.switch_locked(&mem);
            if !replaying {
                self.schedule_flush();
            }
        }
        Ok(())
    }

    fn should_switch(&self, mem: &MemEntry) -> bool {
        let size = mem.mem.approximate_memory();
        if size == 0 {
            return false;
        }
        size >= self.memtable_bytes
            || mem.journal_bytes.load(Ordering::Relaxed) >= self.max_replay_bytes
            || (self.group.should_flush(&self.budget) && size >= (self.budget.limit() / 8).max(1))
    }

    /// Publishes a new empty active memtable; `old` becomes the newest immutable one.
    /// The caller holds `active`.
    fn switch_locked(&self, old: &Arc<MemEntry>) {
        let mut sv = self.sv.write().unwrap_or_else(PoisonError::into_inner);
        if !Arc::ptr_eq(&sv.mem, old) {
            return;
        }
        let fresh = MemEntry::new(
            Memtable::new(self.memtable_options, Some(self.budget.clone())),
            self.tracker.clone(),
            old.last_seq(),
        );
        let mut imm = Vec::with_capacity(sv.imm.len() + 1);
        imm.push(old.clone());
        imm.extend(sv.imm.iter().cloned());
        let version = sv.version.clone();
        self.publish_sv(
            &mut sv,
            SuperVersion {
                mem: fresh,
                imm,
                version,
            },
        );
    }

    /// Freezes the active memtable if it holds anything.
    fn freeze_active(&self) {
        let _active = lock(&self.active);
        let mem = self.current_sv().mem.clone();
        if !mem.mem.is_empty() {
            self.switch_locked(&mem);
        }
    }

    /// Waits while the memtable budget is exhausted or L0 is at its stop trigger. Writers
    /// wait; they never fail. The L0 slowdown is not applied here: it would delay every
    /// scope behind the journal thread. Writers pace themselves ([`Store::write_delay`]).
    fn stall(&self, last_seq: Seq, bytes: usize) {
        loop {
            if self.latched.load(Ordering::Acquire) || self.closing() {
                return;
            }
            // A flush waits on a journal sync, which needs this thread: let through the
            // records queued before the sync and at most one journal group after it.
            if self.syncs_waiting.load(Ordering::Acquire) > 0
                && (last_seq <= self.bypass_seq.load(Ordering::Acquire)
                    || self.bypass_bytes.fetch_sub(bytes as i64, Ordering::AcqRel) > 0)
            {
                return;
            }
            let l0 = self.l0_runs.load(Ordering::Relaxed);
            if self.group.exhausted(&self.budget) {
                let sv = self.current_sv();
                let mine = sv.memtables().any(|m| !m.mem.is_empty());
                if sv.imm.is_empty() && !sv.mem.mem.is_empty() {
                    drop(sv);
                    self.freeze_active();
                }
                if mine {
                    self.schedule_flush();
                } else {
                    // Nothing of this store's to flush: flush whichever store sharing the
                    // budget holds the most (RV-KV2 finding 6).
                    self.group.flush_largest();
                }
                self.budget
                    .wait_below_limit(Some(Instant::now() + STALL_POLL));
                continue;
            }
            if l0 >= self.levels.l0_stop_writes {
                self.schedule_compaction();
                let guard = lock(&self.l0_lock);
                let _ = self.l0_changed.wait_timeout(guard, STALL_POLL);
                continue;
            }
            return;
        }
    }

    /// See [`Store::write_delay`]. A token bucket at `delayed_write_rate`: each caller
    /// takes its bytes' share of time after whatever earlier callers took, and waits
    /// until then. Delays under a millisecond are not returned (the debt stays in the
    /// bucket and a later caller pays it), so pacing costs no timer per small write.
    fn write_delay(&self, bytes: usize) -> Duration {
        if self.l0_runs.load(Ordering::Relaxed) < self.levels.l0_slowdown_writes {
            return Duration::ZERO;
        }
        let rate = self.levels.delayed_write_rate.max(1);
        let cost = (bytes as u64).saturating_mul(1_000_000_000) / rate;
        let now = self.epoch.elapsed().as_nanos() as u64;
        let mut prev = self.pace_until_ns.load(Ordering::Acquire);
        loop {
            let next = prev.max(now).saturating_add(cost);
            match self.pace_until_ns.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
        let until = prev.max(now).saturating_add(cost);
        let delay = until - now;
        if delay < 1_000_000 {
            Duration::ZERO
        } else {
            Duration::from_nanos(delay)
        }
    }

    // ----- flush -----

    fn sync_journal(&self) -> Result<()> {
        self.bypass_bytes
            .store(self.max_group_bytes as i64, Ordering::Release);
        self.syncs_waiting.fetch_add(1, Ordering::AcqRel);
        let result = self.journal().sync().and_then(|commit| {
            self.bypass_seq.fetch_max(commit.seq(), Ordering::AcqRel);
            commit.wait()
        });
        self.syncs_waiting.fetch_sub(1, Ordering::AcqRel);
        result.map(|_| ()).map_err(|e| self.latch_on(e))
    }

    fn snapshot_sequences(&self) -> Vec<Seq> {
        let _guard = lock(&self.snapshot_lock);
        self.snapshots.sequences()
    }

    fn alloc_file(&self) -> u64 {
        let mut vs = lock(&self.vs);
        let id = vs.next_file;
        vs.next_file += 1;
        id
    }

    fn spec<'a>(&'a self, snapshots: &'a [Seq], target: u64, bottommost: bool) -> OutputSpec<'a> {
        OutputSpec {
            fs: &*self.fs,
            runs_dir: &self.runs_dir,
            writer: self.run_writer,
            target_file_size: target,
            bottommost,
            snapshots,
            memory: &self.writer_memory,
        }
    }

    fn remove_runs(&self, ids: impl IntoIterator<Item = u64>) {
        for id in ids {
            self.table_cache.evict(id);
            let _ = self.fs.remove(&self.runs_dir.join(run_file_name(id)));
        }
    }

    fn sync_runs_dir(&self) -> Result<()> {
        self.counters.file_syncs.fetch_add(1, Ordering::Relaxed);
        self.fs.sync_dir(&self.runs_dir).map_err(Error::from)
    }

    fn flush_imms(&self) -> Result<()> {
        let _flush = lock(&self.flush_lock);
        loop {
            let Some(imm) = self.current_sv().imm.last().cloned() else {
                return Ok(());
            };
            self.flush_one(&imm)?;
        }
    }

    fn flush_one(&self, imm: &Arc<MemEntry>) -> Result<()> {
        if self.latched.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        let last_seq = imm.last_seq();
        // No run or manifest edit may reference journal bytes that are not durable.
        self.sync_journal()?;
        let snapshots = self.snapshot_sequences();
        let mut stats = OutputStats::default();
        let metas = if imm.mem.is_empty() {
            Vec::new()
        } else {
            let mut input = imm.mem.iter();
            let tombstones = imm.mem.range_tombstones();
            compaction::write_outputs(
                &mut input,
                &tombstones,
                &self.spec(&snapshots, u64::MAX, false),
                &mut || self.alloc_file(),
                &mut stats,
            )?
        };
        self.counters
            .file_syncs
            .fetch_add(metas.len() as u64, Ordering::Relaxed);
        let ids: Vec<u64> = metas.iter().map(|m| m.id).collect();
        if !metas.is_empty()
            && let Err(e) = self.sync_runs_dir()
        {
            self.remove_runs(ids);
            return Err(self.latch_on(e));
        }
        let added: Vec<Arc<RunFile>> = metas
            .iter()
            .map(|m| {
                let run = RunFile::new(desc_of(m, 0), self.tracker.clone());
                self.pin_l0(&run);
                run
            })
            .collect();
        let mut vs = lock(&self.vs);
        let flushed_seq = last_seq.max(vs.state.flushed_seq);
        let retire_ids = self.retirable_segments(flushed_seq);
        let edit = VersionEdit {
            flushed_seq: Some(flushed_seq),
            added: added.iter().map(|r| r.desc.clone()).collect(),
            segments: Some(self.segments_to_record(&retire_ids)),
            ..VersionEdit::default()
        };
        if let Err(e) = self.log_edit(&mut vs, edit) {
            drop(vs);
            drop(added);
            if self.edit_failed_cleanly() {
                self.remove_runs(ids);
            }
            return Err(e);
        }
        let version = Arc::new(vs.current.with_changes(&[], added));
        vs.current = version.clone();
        self.l0_runs.store(version.l0_count(), Ordering::Relaxed);
        {
            let mut sv = self.sv.write().unwrap_or_else(PoisonError::into_inner);
            let imm_list = sv
                .imm
                .iter()
                .filter(|m| !Arc::ptr_eq(m, imm))
                .cloned()
                .collect();
            let mem = sv.mem.clone();
            self.publish_sv(
                &mut sv,
                SuperVersion {
                    mem,
                    imm: imm_list,
                    version,
                },
            );
        }
        // Readers may still hold it; its charge now counts as pinned (finding 5).
        imm.mark_flushed(self.group.pinned_counter());
        drop(vs);
        self.counters
            .flush_bytes
            .fetch_add(stats.bytes_written, Ordering::Relaxed);
        self.retire_journal_segments(&retire_ids)?;
        if self.current_sv().version.l0_count() >= self.levels.l0_compaction_trigger
            || self.journal_over_budget()
        {
            self.schedule_compaction();
        }
        Ok(())
    }

    /// Keeps an L0 run open for its life (its index and filter stay resident).
    fn pin_l0(&self, run: &RunFile) {
        if run.level() == 0
            && let Ok(open) = self.table_cache.get(run.id())
        {
            run.pin(open);
        }
    }

    /// The live journal segments that may retire once `flushed_seq` is recorded (plan
    /// section 2 "Journal values"; the rule is in `gc.rs`). The caller holds `vs`.
    fn retirable_segments(&self, flushed_seq: Seq) -> Vec<u64> {
        let journal = self.journal();
        // Visibility is read before the snapshot registry: a snapshot registered after
        // the registry read re-checks visibility (see `snapshot`) and so is at or above
        // anything this decision relies on.
        let durable_seq = journal.durable_seq();
        let visible_seq = journal.visible_seq();
        let min_referenced = self.tracker.min_referenced_segment();
        let segments = journal.segments();
        let oldest_snapshot = self.snapshots.oldest();
        let gc = lock(&self.gc);
        let state = gc::RetireState {
            flushed_seq,
            durable_seq,
            visible_seq,
            min_referenced,
            oldest_snapshot,
            cleared: Some(&gc.cleared),
        };
        gc::retirable(&segments, &state)
            .into_iter()
            .map(|s| s.segment)
            .collect()
    }

    /// Returns segments (already dropped from the manifest's segment set) to the
    /// journal's free pool.
    fn retire_journal_segments(&self, ids: &[u64]) -> Result<()> {
        for &segment in ids {
            match self.journal().retire(segment) {
                Ok(()) => {}
                // Already retired, or no longer eligible: the next pass decides again.
                Err(Error::InvalidArgument(_)) => continue,
                Err(e) => return Err(self.latch_on(e)),
            }
        }
        if !ids.is_empty() {
            let mut gc = lock(&self.gc);
            for id in ids {
                gc.cleared.remove(id);
                gc.dense.remove(id);
            }
        }
        Ok(())
    }

    /// Retires every segment the rule allows now, with its own manifest edit.
    fn retire_unreachable(&self) -> Result<usize> {
        let mut vs = lock(&self.vs);
        let ids = self.retirable_segments(vs.state.flushed_seq);
        if ids.is_empty() {
            return Ok(0);
        }
        self.log_edit(
            &mut vs,
            VersionEdit {
                segments: Some(self.segments_to_record(&ids)),
                ..VersionEdit::default()
            },
        )?;
        drop(vs);
        self.retire_journal_segments(&ids)?;
        Ok(ids.len())
    }

    /// True while the journal's live segments exceed `max_journal_bytes`.
    fn journal_over_budget(&self) -> bool {
        self.journal().segments().len() as u64 * self.segment_bytes > self.max_journal_bytes
    }

    /// The journal segments to record in the manifest: those whose first record is
    /// durable (a torn tail at the next open can only free later ones), minus `retiring`.
    fn segments_to_record(&self, retiring: &[u64]) -> Vec<(u64, u64)> {
        let journal = self.journal();
        let durable = journal.durable_seq();
        journal
            .segments()
            .into_iter()
            .filter(|s| s.first_seq <= durable && !retiring.contains(&s.segment))
            .map(|s| (s.segment, s.epoch))
            .collect()
    }

    /// Appends `edit` (rolling to a fresh manifest when needed) and applies it to the
    /// recorded state. The caller holds `vs`.
    ///
    /// A failure whose outcome is unknown (the edit may be durable, now or after a
    /// restart) latches `RecoveryRequired`. Callers then must not delete any file the
    /// edit names (RV-KV2 finding 2): see [`Inner::edit_failed_cleanly`].
    fn log_edit(&self, vs: &mut VersionSet, mut edit: VersionEdit) -> Result<()> {
        if self.latched.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        edit.next_file = Some(vs.next_file);
        let mut next = vs.state.clone();
        next.apply(&edit).ok_or(Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "inconsistent version edit",
        })?;
        let roll = vs.manifest.as_ref().is_none_or(ManifestWriter::wants_roll);
        if roll {
            let number = vs.next_file;
            vs.next_file += 1;
            next.next_file = vs.next_file;
            let writer = match ManifestWriter::create(&*self.fs, &self.dir, number, &next) {
                Ok(w) => w,
                Err(e) => return Err(self.latch_on(e)),
            };
            match manifest::switch_current(&*self.fs, &self.dir, number) {
                Ok(()) => {}
                Err(AppendError::Retry(e)) => {
                    // CURRENT still names the old manifest.
                    let _ = self.fs.remove(writer.path());
                    return Err(self.latch_on(e));
                }
                Err(AppendError::Fatal(e)) => {
                    // CURRENT may name either manifest: keep both for the next open.
                    self.latch();
                    return Err(e);
                }
            }
            self.counters.file_syncs.fetch_add(4, Ordering::Relaxed);
            if let Some(old) = vs.manifest.replace(writer) {
                let _ = self.fs.remove(old.path());
            }
        } else {
            match vs.manifest.as_mut().expect("checked").append(&edit) {
                Ok(()) => {}
                Err(AppendError::Retry(e)) => return Err(e),
                Err(AppendError::Fatal(e)) => {
                    self.latch();
                    return Err(e);
                }
            }
            self.counters.file_syncs.fetch_add(1, Ordering::Relaxed);
        }
        vs.state = next;
        Ok(())
    }

    /// After a failed `log_edit`: true when the edit certainly did not persist, so the
    /// files it would have added may be deleted. When the store is latched the outcome
    /// may be unknown; the files stay and the next open's cleanup removes the ones no
    /// manifest names.
    fn edit_failed_cleanly(&self) -> bool {
        !self.latched.load(Ordering::Acquire)
    }

    // ----- compaction -----

    /// Persists pending damage and releases, deletes obsolete files, and runs one
    /// compaction. True when a compaction ran (more may be needed).
    fn compact_once(&self) -> Result<bool> {
        let _compaction = lock(&self.compaction_lock);
        self.persist_pending()?;
        self.purge_obsolete();
        let pick = {
            let vs = lock(&self.vs);
            compaction::pick(&vs.current, &self.levels, &vs.cursor)
        };
        let Some(pick) = pick else {
            return Ok(false);
        };
        match self.run_compaction(&pick) {
            Ok(()) => {}
            Err(Error::Corruption {
                file: FileId::Run { id },
                offset,
                what,
            }) => {
                self.quarantine_run(id, offset, what)?;
            }
            Err(e) => return Err(e),
        }
        {
            let _g = lock(&self.l0_lock);
            self.l0_changed.notify_all();
        }
        self.purge_obsolete();
        Ok(true)
    }

    fn run_compaction(&self, pick: &compaction::Pick) -> Result<()> {
        if pick.is_trivial_move() {
            let run = &pick.inputs[0];
            let moved = run.moved(pick.output_level as u8);
            let desc = moved.desc.clone();
            let mut vs = lock(&self.vs);
            self.log_edit(
                &mut vs,
                VersionEdit {
                    removed: vec![(run.desc.level, run.id())],
                    added: vec![desc],
                    ..VersionEdit::default()
                },
            )?;
            vs.cursor[pick.level] = run.hi.clone();
            let version = Arc::new(vs.current.with_changes(&[run.id()], vec![moved]));
            self.install_version(&mut vs, version);
            return Ok(());
        }
        let snapshots = self.snapshot_sequences();
        let (lo, hi) = pick.range();
        let bottommost = {
            let vs = lock(&self.vs);
            compaction::is_bottommost(&vs.current, pick.output_level, &lo, &hi)
        };
        let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
        let mut tombstones: Vec<RangeTombstone> = Vec::new();
        for input in pick.all_inputs() {
            let run = self.table_cache.get(input.id())?;
            tombstones.extend_from_slice(run.range_tombstones());
            children.push(Box::new(run.iter(false)));
        }
        let mut merged = MergingIterator::new(children);
        let mut stats = OutputStats::default();
        let metas = compaction::write_outputs(
            &mut merged,
            &tombstones,
            &self.spec(&snapshots, self.levels.target_file_size, bottommost),
            &mut || self.alloc_file(),
            &mut stats,
        )?;
        drop(merged);
        let ids: Vec<u64> = metas.iter().map(|m| m.id).collect();
        self.counters
            .file_syncs
            .fetch_add(metas.len() as u64, Ordering::Relaxed);
        if !metas.is_empty()
            && let Err(e) = self.sync_runs_dir()
        {
            self.remove_runs(ids);
            return Err(self.latch_on(e));
        }
        let added: Vec<Arc<RunFile>> = metas
            .iter()
            .map(|m| RunFile::new(desc_of(m, pick.output_level as u8), self.tracker.clone()))
            .collect();
        let removed: Vec<u64> = pick.all_inputs().map(|r| r.id()).collect();
        let mut vs = lock(&self.vs);
        let edit = VersionEdit {
            removed: pick.all_inputs().map(|r| (r.desc.level, r.id())).collect(),
            added: added.iter().map(|r| r.desc.clone()).collect(),
            ..VersionEdit::default()
        };
        if let Err(e) = self.log_edit(&mut vs, edit) {
            drop(vs);
            drop(added);
            if self.edit_failed_cleanly() {
                self.remove_runs(ids);
            }
            return Err(e);
        }
        if pick.level > 0 {
            vs.cursor[pick.level] = pick.inputs[0].hi.clone();
        }
        for input in pick.all_inputs() {
            input.mark_obsolete();
        }
        let version = Arc::new(vs.current.with_changes(&removed, added));
        self.install_version(&mut vs, version);
        drop(vs);
        self.counters
            .compaction_bytes
            .fetch_add(stats.bytes_written, Ordering::Relaxed);
        Ok(())
    }

    fn install_version(&self, vs: &mut VersionSet, version: Arc<Version>) {
        vs.current = version.clone();
        self.l0_runs.store(version.l0_count(), Ordering::Relaxed);
        let mut sv = self.sv.write().unwrap_or_else(PoisonError::into_inner);
        let (mem, imm) = (sv.mem.clone(), sv.imm.clone());
        self.publish_sv(&mut sv, SuperVersion { mem, imm, version });
    }

    /// Deletes runs that are obsolete and no longer referenced.
    fn purge_obsolete(&self) {
        let ids = self.tracker.take_obsolete();
        self.remove_runs(ids);
    }

    /// G2 found by compaction: a single-scope run leaves the version (its scope is
    /// quarantined, its file kept); damage spanning scopes latches (R5).
    fn quarantine_run(&self, id: u64, offset: u64, what: &'static str) -> Result<()> {
        let mut vs = lock(&self.vs);
        let Some(run) = vs.current.runs().find(|r| r.id() == id).cloned() else {
            return Ok(());
        };
        let Some(scope) = single_scope(self.scope_of, &run.lo, &run.hi, run.hi_exclusive) else {
            drop(vs);
            self.report_damage(Damage {
                scope: Bytes::new(),
                reason: DamageReason::RunBlock,
                file: FileId::Run { id },
                first_bad_seq: None,
            });
            return Err(Error::Corruption {
                file: FileId::Run { id },
                offset,
                what,
            });
        };
        let damage = Damage {
            scope: scope.clone(),
            reason: DamageReason::RunBlock,
            file: FileId::Run { id },
            first_bad_seq: None,
        };
        self.log_edit(
            &mut vs,
            VersionEdit {
                quarantined: vec![damage.clone()],
                quarantined_runs: vec![(id, scope)],
                ..VersionEdit::default()
            },
        )?;
        self.table_cache.evict(id);
        let version = Arc::new(vs.current.with_changes(&[id], Vec::new()));
        self.install_version(&mut vs, version);
        drop(vs);
        if self.quarantine.add(damage.clone()) {
            self.notify(damage);
        }
        Ok(())
    }

    /// Writes quarantines found at run time whose immediate edit failed.
    fn persist_pending(&self) -> Result<()> {
        let damage: Vec<Damage> = std::mem::take(&mut *lock(&self.pending_damage));
        if !damage.is_empty() {
            let mut vs = lock(&self.vs);
            if let Err(e) = self.log_edit(
                &mut vs,
                VersionEdit {
                    quarantined: damage.clone(),
                    ..VersionEdit::default()
                },
            ) {
                lock(&self.pending_damage).extend(damage);
                return Err(e);
            }
        }
        Ok(())
    }

    /// The durable part of [`Store::release_scope`]: once the release's range delete at
    /// `seq` is durable, flush past it (so a later replay never meets the scope's
    /// damaged records again), then record the release, drop the scope's quarantined
    /// runs and lift its quarantine.
    fn finish_release(&self, scope: Bytes, seq: Seq) -> Result<()> {
        let flushed = lock(&self.vs).state.flushed_seq;
        if flushed < seq {
            self.freeze_active();
            self.flush_imms()?;
        }
        let mut vs = lock(&self.vs);
        let dropped: Vec<u64> = vs
            .state
            .quarantined_runs
            .iter()
            .filter(|(_, s)| **s == scope)
            .map(|(&id, _)| id)
            .collect();
        self.log_edit(
            &mut vs,
            VersionEdit {
                released: vec![(scope.clone(), seq)],
                dropped_quarantined_runs: dropped.clone(),
                ..VersionEdit::default()
            },
        )?;
        drop(vs);
        self.remove_runs(dropped);
        self.quarantine.remove(&scope);
        Ok(())
    }

    // ----- damage -----

    fn notify(&self, damage: Damage) {
        let hook = self
            .damage_hook
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(damage);
        }
    }

    /// Damage found at run time. A scoped one is quarantined and persisted right away
    /// with its own manifest edit, before the caller sees the error, so a crash cannot
    /// forget it; if that edit fails (`NoSpace`), the background job retries it. A
    /// store-wide one latches `RecoveryRequired` (R5).
    ///
    /// Must not be called with `vs` held.
    pub(crate) fn report_damage(&self, damage: Damage) {
        if damage.scope.is_empty() {
            self.latch();
            self.notify(damage);
            return;
        }
        if self.quarantine.add(damage.clone()) {
            let persisted = {
                let mut vs = lock(&self.vs);
                self.log_edit(
                    &mut vs,
                    VersionEdit {
                        quarantined: vec![damage.clone()],
                        ..VersionEdit::default()
                    },
                )
            };
            if persisted.is_err() {
                lock(&self.pending_damage).push(damage.clone());
                self.schedule_compaction();
            }
            self.notify(damage);
        }
    }

    /// A read met a locator into a segment the journal recycled. Journal GC retires a
    /// segment only once no reader can reach its locators, so this is an invariant
    /// violation, not media damage: count it and latch `RecoveryRequired`.
    pub(crate) fn recycled_value(&self) {
        self.counters.recycled_reads.fetch_add(1, Ordering::Relaxed);
        self.latch();
    }

    /// The scope `key` belongs to (empty for store-wide keys).
    pub(crate) fn scope_bytes(&self, key: &[u8]) -> Bytes {
        let len = (self.scope_of)(key).min(key.len());
        Bytes::copy_from_slice(&key[..len])
    }

    // ----- reads -----

    /// Takes a snapshot without a store-wide lock (NS5b item 5).
    ///
    /// The snapshot list must know every snapshot that a compaction (or a GC decision)
    /// could hurt. The fast path reads `visible_seq` as `s`, registers `s` in this
    /// thread's registry shard, loads the super-version and reads `visible_seq` again. If
    /// it is still `s`, the snapshot is safe: a compaction that read the registry before
    /// the registration picked its inputs earlier, when every version in them was at or
    /// below the visible sequence of that time, which is at most the second read, so
    /// `s`; such a compaction cannot hold a version above `s` and cannot drop one `s`
    /// needs. (The registry shard's mutex orders the compaction's read before the
    /// registration, and the registration before the second read.) Otherwise it retries,
    /// and after a few tries takes `snapshot_lock`, which readers of the list also hold.
    fn snapshot(self: &Arc<Self>) -> Snapshot {
        let journal = self.journal();
        for _ in 0..4 {
            let seq = journal.visible_seq();
            let handle = self.snapshots.register(seq);
            let sv = self.current_sv();
            if journal.visible_seq() == seq {
                return Snapshot::new(self.clone(), seq, handle, sv);
            }
        }
        let _guard = lock(&self.snapshot_lock);
        let seq = journal.visible_seq();
        let handle = self.snapshots.register(seq);
        Snapshot::new(self.clone(), seq, handle, self.current_sv())
    }

    fn stats(&self) -> StoreStats {
        let journal = self.journal().stats();
        let sv = self.current_sv();
        StoreStats {
            journal_syncs: journal.journal_syncs,
            file_syncs: self.counters.file_syncs.load(Ordering::Relaxed),
            commits: journal.commits,
            synced_commits: journal.synced_commits,
            group_commits: journal.group_commits,
            user_bytes: self.counters.user_bytes.load(Ordering::Relaxed),
            journal_bytes: journal.journal_bytes,
            flush_bytes: self.counters.flush_bytes.load(Ordering::Relaxed),
            compaction_bytes: self.counters.compaction_bytes.load(Ordering::Relaxed),
            relocated_bytes: self.counters.relocated_bytes.load(Ordering::Relaxed),
            stall_ms: journal.stall_ms,
            memtable_bytes: sv
                .memtables()
                .map(|m| m.mem.approximate_memory() as u64)
                .sum(),
            l0_runs: sv.version.l0_count() as u32,
            recovery_ms: self.recovery_ms.load(Ordering::Relaxed),
            corruption_detected: self.run_stats.corruption_detected()
                + journal.value_corruptions
                + self.counters.open_corruptions.load(Ordering::Relaxed),
            tail_truncations: journal.tail_truncations,
            quarantined_scopes: self.quarantine.len() as u32,
            journal_behind_manifest: self.counters.journal_behind.load(Ordering::Relaxed),
            recycled_value_reads: self.counters.recycled_reads.load(Ordering::Relaxed),
            overhead_bytes: self.memory_usage().overhead(),
        }
    }

    fn memory_usage(&self) -> MemoryUsage {
        let sv = self.current_sv();
        let runs = sv.version.runs().count() as u64;
        MemoryUsage {
            memtables: sv
                .memtables()
                .map(|m| m.mem.approximate_memory() as u64)
                .sum(),
            run_writers: self.writer_memory.load(Ordering::Relaxed) as u64,
            range_tombstones: self.tracker.tombstone_bytes() as u64,
            versions: runs * 200,
            snapshots: self.snapshots.sequences().len() as u64 * 64,
            cache_pinned: self.cache.reserved_bytes() as u64,
            memtables_pinned: self.group.pinned_bytes() as u64,
        }
    }
}

fn desc_of(meta: &RunMeta, level: u8) -> RunDesc {
    RunDesc {
        id: meta.id,
        level,
        file_size: meta.file_size,
        smallest: Bytes::copy_from_slice(&meta.smallest),
        largest: Bytes::copy_from_slice(&meta.largest),
        smallest_seq: meta.smallest_seq,
        largest_seq: meta.largest_seq,
        num_entries: meta.num_entries,
        oldest_journal_segment: meta.oldest_journal_segment,
    }
}
