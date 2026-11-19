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
mod read;
#[cfg(test)]
mod tests;

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;

pub use background::Background;
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
use crate::memtable::{Memtable, MemtableBudget, MemtableOptions, SnapshotList};
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

/// How long a stalled writer sleeps between checks.
const STALL_POLL: Duration = Duration::from_millis(5);
/// Background work that failed with `NoSpace` retries after this long.
const NO_SPACE_RETRY: Duration = Duration::from_millis(200);

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
}

pub struct Opened {
    pub store: Store,
    /// Every quarantined scope, found by this open or persisted by an earlier one.
    pub quarantined: Vec<Damage>,
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
    run_writer: RunWriterOptions,
    budget: Arc<MemtableBudget>,
    background: Arc<Background>,
    table_cache: TableCache,
    run_stats: Arc<RunStats>,
    tracker: Arc<FileTracker>,
    journal: OnceLock<Arc<Journal>>,
    sv: RwLock<Arc<SuperVersion>>,
    /// Serializes memtable inserts (journal thread) with freezes.
    active: Mutex<()>,
    replaying: AtomicBool,
    vs: Mutex<VersionSet>,
    flush_lock: Mutex<()>,
    compaction_lock: Mutex<()>,
    snapshots: SnapshotList,
    snapshot_lock: Mutex<()>,
    quarantine: Quarantine,
    latched: AtomicBool,
    jobs: Mutex<Jobs>,
    jobs_done: Condvar,
    /// A flush is waiting for a journal sync: stalled sinks must let the journal thread go.
    syncs_waiting: AtomicUsize,
    flush_queued: AtomicBool,
    compaction_queued: AtomicBool,
    l0_changed: Condvar,
    l0_lock: Mutex<()>,
    slowdown_debt_ns: AtomicU64,
    damage_hook: RwLock<Option<DamageHook>>,
    pending_damage: Mutex<Vec<Damage>>,
    pending_release: Mutex<Vec<(Bytes, Seq)>>,
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
            version.levels[desc.level as usize].push(RunFile::new(desc.clone(), tracker.clone()));
        }
        version.sort();
        let budget = options.memtables.clone();
        let first_mem = MemEntry::new(
            Memtable::new(options.memtable, Some(budget.clone())),
            tracker.clone(),
            state.flushed_seq,
        );
        let mut quarantine_list = state.quarantine.clone();
        for d in &new_damage {
            if !quarantine_list.iter().any(|q| q.scope == d.scope) {
                quarantine_list.push(d.clone());
            }
        }
        let flushed_seq = state.flushed_seq;
        let inner = Arc::new(Inner {
            dir: dir.to_path_buf(),
            runs_dir: runs_dir.clone(),
            fs: fs.clone(),
            levels: options.levels.clone(),
            scope_of: options.scope_of,
            memtable_options: options.memtable,
            memtable_bytes: options.memtable_bytes.max(1),
            max_replay_bytes: options.max_replay_bytes.max(1),
            run_writer: options.run_writer,
            budget,
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
            snapshots: SnapshotList::new(),
            snapshot_lock: Mutex::new(()),
            quarantine: Quarantine::new(quarantine_list),
            latched: AtomicBool::new(false),
            jobs: Mutex::new(Jobs::default()),
            jobs_done: Condvar::new(),
            syncs_waiting: AtomicUsize::new(0),
            flush_queued: AtomicBool::new(false),
            compaction_queued: AtomicBool::new(false),
            l0_changed: Condvar::new(),
            l0_lock: Mutex::new(()),
            slowdown_debt_ns: AtomicU64::new(0),
            damage_hook: RwLock::new(None),
            pending_damage: Mutex::new(Vec::new()),
            pending_release: Mutex::new(Vec::new()),
            counters: Counters::default(),
            recovery_ms: AtomicU64::new(0),
            lock_file: Mutex::new(Some(opened.lock)),
            self_ref: OnceLock::new(),
        });
        let _ = inner.self_ref.set(Arc::downgrade(&inner));
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
        let _ = inner.journal.set(journal.clone());
        inner.replaying.store(false, Ordering::Release);

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

        // R6: damage this wide is more likely systemic than local.
        if new_damage.len() > options.max_quarantined.max_scopes {
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
            recovery,
        })
    }

    /// Queues `batch`. Never blocks on I/O. Submission order is commit order. `on_done`
    /// is dropped on the journal thread after the write is visible (and durable, for
    /// `Sync`), or after it failed.
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

    /// Deletes everything under `prefix` and, once that is durable and flushed, lifts
    /// its quarantine and drops its damaged runs (plan section 3, rule 4).
    pub fn release_scope(&self, prefix: &[u8]) -> Result<Commit> {
        self.inner.check_usable()?;
        let end = prefix_successor(prefix).ok_or(Error::InvalidArgument(
            "release_scope needs a bounded prefix",
        ))?;
        let mut batch = WriteBatch::new();
        batch.set_scope(prefix);
        batch.delete_range(prefix, &end);
        let commit = self
            .inner
            .journal()
            .submit(batch, Durability::Sync, Box::new(()))?;
        lock(&self.inner.pending_release).push((Bytes::copy_from_slice(prefix), commit.seq()));
        self.inner.schedule_compaction();
        Ok(commit)
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

    pub fn stats(&self) -> StoreStats {
        self.inner.stats()
    }

    /// Closes the store: waits for its background work, then closes the journal (clean
    /// close record) and releases the directory lock. Dropping does the same.
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
            Err(Error::NoSpace) => {
                self.flush_queued.store(true, Ordering::Release);
                Some(NO_SPACE_RETRY)
            }
            _ => None,
        }
    }

    fn run_compaction(&self) -> Option<Duration> {
        self.compaction_queued.store(false, Ordering::Release);
        if !self.begin_job() {
            return None;
        }
        let result = self.compact_once();
        self.end_job();
        let releases_waiting = !lock(&self.pending_release).is_empty();
        match result {
            Ok(true) => {
                self.schedule_compaction();
                None
            }
            Ok(false) if releases_waiting => {
                self.compaction_queued.store(true, Ordering::Release);
                Some(Duration::from_millis(10))
            }
            Err(Error::NoSpace) => {
                self.compaction_queued.store(true, Ordering::Release);
                Some(NO_SPACE_RETRY)
            }
            _ => None,
        }
    }
}

impl Inner {
    fn journal(&self) -> &Arc<Journal> {
        self.journal.get().expect("journal opened")
    }

    fn weak_job(&self) -> Option<Weak<dyn Job>> {
        let weak: Weak<Inner> = self.self_ref.get()?.clone();
        Some(weak)
    }

    pub(crate) fn current_sv(&self) -> Arc<SuperVersion> {
        self.sv
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
        lock(&self.jobs).closing
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
        if kind == RecordKind::Relocate {
            return Err(Error::InvalidArgument(
                "journal relocation records are not supported yet",
            ));
        }
        let replaying = self.replaying.load(Ordering::Acquire);
        if !replaying {
            self.stall(batch.encoded_bytes());
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
            || (self.budget.should_flush() && size >= (self.budget.limit() / 8).max(1))
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
        *sv = Arc::new(SuperVersion {
            mem: fresh,
            imm,
            version: sv.version.clone(),
        });
    }

    /// Freezes the active memtable if it holds anything.
    fn freeze_active(&self) {
        let _active = lock(&self.active);
        let mem = self.current_sv().mem.clone();
        if !mem.mem.is_empty() {
            self.switch_locked(&mem);
        }
    }

    /// Waits while the memtable budget is exhausted or L0 is at its stop trigger, and
    /// slows writes while L0 is at its slowdown trigger. Writers wait; they never fail.
    fn stall(&self, bytes: usize) {
        loop {
            if self.latched.load(Ordering::Acquire) || self.closing() {
                return;
            }
            // A flush waits on a journal sync, which needs this thread.
            if self.syncs_waiting.load(Ordering::Acquire) > 0 {
                return;
            }
            let sv = self.current_sv();
            let l0 = sv.version.l0_count();
            if self.budget.is_exhausted() {
                if sv.imm.is_empty() && !sv.mem.mem.is_empty() {
                    drop(sv);
                    self.freeze_active();
                }
                self.schedule_flush();
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
            if l0 >= self.levels.l0_slowdown_writes {
                let rate = self.levels.delayed_write_rate.max(1);
                let ns = (bytes as u64).saturating_mul(1_000_000_000) / rate;
                let debt = self.slowdown_debt_ns.fetch_add(ns, Ordering::Relaxed) + ns;
                if debt >= 1_000_000 {
                    self.slowdown_debt_ns.store(0, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_nanos(debt));
                }
            }
            return;
        }
    }

    // ----- flush -----

    fn sync_journal(&self) -> Result<()> {
        self.syncs_waiting.fetch_add(1, Ordering::AcqRel);
        let result = self.journal().sync().and_then(Commit::wait);
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
            .map(|m| RunFile::new(desc_of(m, 0), self.tracker.clone()))
            .collect();
        let mut vs = lock(&self.vs);
        let flushed_seq = last_seq.max(vs.state.flushed_seq);
        let retire = gc::retirable(
            &self.journal().segments(),
            flushed_seq,
            self.journal().durable_seq(),
            self.tracker.min_referenced_segment(),
        );
        let retire_ids: Vec<u64> = retire.iter().map(|s| s.segment).collect();
        let edit = VersionEdit {
            flushed_seq: Some(flushed_seq),
            added: added.iter().map(|r| r.desc.clone()).collect(),
            segments: Some(self.segments_to_record(&retire_ids)),
            ..VersionEdit::default()
        };
        if let Err(e) = self.log_edit(&mut vs, edit) {
            drop(vs);
            drop(added);
            self.remove_runs(ids);
            return Err(e);
        }
        let version = Arc::new(vs.current.with_changes(&[], added));
        vs.current = version.clone();
        {
            let mut sv = self.sv.write().unwrap_or_else(PoisonError::into_inner);
            let imm_list = sv
                .imm
                .iter()
                .filter(|m| !Arc::ptr_eq(m, imm))
                .cloned()
                .collect();
            *sv = Arc::new(SuperVersion {
                mem: sv.mem.clone(),
                imm: imm_list,
                version,
            });
        }
        drop(vs);
        self.counters
            .flush_bytes
            .fetch_add(stats.bytes_written, Ordering::Relaxed);
        for segment in retire_ids {
            if let Err(e) = self.journal().retire(segment) {
                if !matches!(e, Error::InvalidArgument(_)) {
                    return Err(self.latch_on(e));
                }
                break;
            }
        }
        if self.current_sv().version.l0_count() >= self.levels.l0_compaction_trigger {
            self.schedule_compaction();
        }
        Ok(())
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
            if let Err(e) = manifest::set_current(&*self.fs, &self.dir, number) {
                let _ = self.fs.remove(writer.path());
                return Err(self.latch_on(e));
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
            let mut desc = run.desc.clone();
            desc.level = pick.output_level as u8;
            let moved = RunFile::new(desc.clone(), self.tracker.clone());
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
            self.remove_runs(ids);
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
        let mut sv = self.sv.write().unwrap_or_else(PoisonError::into_inner);
        *sv = Arc::new(SuperVersion {
            mem: sv.mem.clone(),
            imm: sv.imm.clone(),
            version,
        });
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

    /// Writes quarantines found at run time and completed releases to the manifest.
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
        let releases: Vec<(Bytes, Seq)> = std::mem::take(&mut *lock(&self.pending_release));
        if releases.is_empty() {
            return Ok(());
        }
        let durable = self.journal().durable_seq();
        let (ready, waiting): (Vec<_>, Vec<_>) =
            releases.into_iter().partition(|(_, seq)| *seq <= durable);
        lock(&self.pending_release).extend(waiting);
        if ready.is_empty() {
            return Ok(());
        }
        // The release must be flushed, so a later replay never meets the damaged
        // records of the scope again.
        let max_seq = ready.iter().map(|(_, s)| *s).max().unwrap_or(0);
        let flushed = lock(&self.vs).state.flushed_seq;
        if flushed < max_seq {
            self.freeze_active();
            if let Err(e) = self.flush_imms() {
                lock(&self.pending_release).extend(ready);
                return Err(e);
            }
        }
        let mut vs = lock(&self.vs);
        let mut dropped = Vec::new();
        for (scope, _) in &ready {
            dropped.extend(
                vs.state
                    .quarantined_runs
                    .iter()
                    .filter(|(_, s)| *s == scope)
                    .map(|(&id, _)| id),
            );
        }
        if let Err(e) = self.log_edit(
            &mut vs,
            VersionEdit {
                released: ready.clone(),
                dropped_quarantined_runs: dropped.clone(),
                ..VersionEdit::default()
            },
        ) {
            drop(vs);
            lock(&self.pending_release).extend(ready);
            return Err(e);
        }
        drop(vs);
        self.remove_runs(dropped);
        for (scope, _) in ready {
            self.quarantine.remove(&scope);
        }
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

    /// Damage found at run time. A scoped one is quarantined (and persisted by the next
    /// compaction job); a store-wide one latches `RecoveryRequired` (R5).
    pub(crate) fn report_damage(&self, damage: Damage) {
        if damage.scope.is_empty() {
            self.latch();
            self.notify(damage);
            return;
        }
        if self.quarantine.add(damage.clone()) {
            lock(&self.pending_damage).push(damage.clone());
            self.schedule_compaction();
            self.notify(damage);
        }
    }

    /// The scope `key` belongs to (empty for store-wide keys).
    pub(crate) fn scope_bytes(&self, key: &[u8]) -> Bytes {
        let len = (self.scope_of)(key).min(key.len());
        Bytes::copy_from_slice(&key[..len])
    }

    // ----- reads -----

    fn snapshot(self: &Arc<Self>) -> Snapshot {
        let (seq, handle, sv) = {
            let _guard = lock(&self.snapshot_lock);
            let seq = self.journal().visible_seq();
            let handle = self.snapshots.register(seq);
            (seq, handle, self.current_sv())
        };
        Snapshot::new(self.clone(), seq, handle, sv)
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
            relocated_bytes: 0,
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
