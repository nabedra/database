//! The journal: segment files, one writer thread with group commit, and the recovery scan.
//! Owned by task NS2; the design is `plans/native-storage-engine.md` sections 2 and 3.
//!
//! # Writing
//!
//! [`Journal::submit`] assigns sequence numbers in submission order and queues the batch
//! without doing I/O. The journal thread drains up to `max_group_bytes`, encodes the
//! records (format in [`format`]) and makes one positioned write per segment touched. If
//! any waiter asked for [`Durability::Sync`] it appends a pad up to the next 4 KiB
//! boundary before the write and then calls `sync_data` once, on every segment with
//! unsynced bytes. Because a synced group always ends on a boundary, **no write ever
//! touches a byte that a completed sync covered**. The thread then hands each record to
//! the [`JournalSink`] in sequence order, publishes `visible_seq` and then `durable_seq`,
//! completes the waiters and drops their `on_done` payloads, all on the journal thread.
//!
//! # Errors
//!
//! The first failed write, sync, segment activation or sink call latches
//! [`Error::RecoveryRequired`]: that group's waiters, everything queued and every later
//! submit fail with it, and nothing is retried. The one exception is running out of space
//! while *preparing* the next segment file, which happens before any byte of the affected
//! records is written: those records and every submission queued at that moment fail
//! with [`Error::NoSpace`], their sequence numbers are voided by a `Pad` record written
//! ahead of the next successful record, and the journal keeps working. A failed `pwrite`
//! always latches, `ENOSPC` included, because a partial write cannot be ruled out.
//!
//! # Recovery
//!
//! [`Journal::open`] replays every record with a sequence above `replay_after` through
//! the sink and classifies the first position that is not a valid record (plan section 3,
//! cases L1, L2, G1, R1 to R3). See [`recovery`]. Before it returns, every live segment is
//! synced, so `durable_seq` covers everything the journal holds.
//!
//! # Rules for the store
//!
//! - **Journal before manifest.** A flush must make the journal durable up to the
//!   sequences it flushes (`Journal::sync()` and wait) before it writes the manifest edit
//!   that advances `flushed_seq` or adds a run holding journal-value locators. Otherwise a
//!   power loss can leave runs whose locators point at journal bytes that were never
//!   synced (RV-KV1 F5). With this rule, `replay_after` is never ahead of the journal
//!   unless synced journal bytes were damaged.
//! - **Directories.** `open` creates `dir` if needed and syncs the parent of every
//!   directory it created and of `dir` itself, so the journal's own directory entry is
//!   durable before any acknowledged write depends on it (RV-KV1 F2). It also syncs `dir`
//!   before it publishes `durable_seq`: the names it recovered from may exist only in the
//!   OS (a process killed between a roll's rename and its directory sync), and this run
//!   appends to that segment and the store records it in the manifest (KVfix4).
//! - **Background work.** Besides the `kv-journal` thread, a `kv-journal-prep` thread
//!   prepares (creates, zero-fills, syncs) the next free segment once the active one is
//!   half used, so commits never wait for a zero fill unless no spare exists (F8).

use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread::JoinHandle;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::batch::{MAX_SCOPE_LEN, WriteBatch};
use crate::cache::{BlockCache, CacheKey};
use crate::crc;
use crate::error::{Damage, Error, FileId, Result};
use crate::fs::{Fs, FsFile};
use crate::types::{Locator, MAX_SEQ, Seq};

pub mod format;
mod recovery;
mod segment;
mod writer;

#[cfg(test)]
mod crash_tests;
#[cfg(test)]
mod tests;

use format::{BLOCK, RECORD_HEADER_LEN, RECORDS_START, RecordType};
use segment::{FreeSeg, LiveSeg};

/// How durable a write must be before its commit completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Written to the OS and synced to the device.
    Sync,
    /// Written to the OS (survives a process kill). Any later completed sync covers it.
    NoSync,
}

/// Record kinds a caller can submit and the sink receives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Batch,
    /// Journal GC relocation (NS5 defines the payload's meaning).
    Relocate,
}

#[derive(Clone, Debug)]
pub struct JournalOptions {
    /// Size of each segment file. A multiple of 4096, at least 12288 (two header blocks
    /// and one block of records). Default 64 MiB.
    pub segment_bytes: u64,
    /// The thread drains at most this many payload bytes per group (at least one batch).
    /// Default 4 MiB.
    pub max_group_bytes: usize,
    /// Write zeros over a new segment file once when it is created, so later overwrites
    /// change no file metadata. Default true.
    pub zero_fill: bool,
}

impl Default for JournalOptions {
    fn default() -> Self {
        JournalOptions {
            segment_bytes: 64 << 20,
            max_group_bytes: 4 << 20,
            zero_fill: true,
        }
    }
}

/// Receives every record in sequence order: during replay at open, and on the journal
/// thread after each group's write (and sync). It may block, for example on a memtable
/// stall. An error latches [`Error::RecoveryRequired`] (or fails the open).
pub trait JournalSink: Send + Sync {
    /// `locators[i]` locates `batch.journal_values()[i]`. `batch.scope()` is the record's scope.
    fn apply(
        &self,
        kind: RecordKind,
        first_seq: Seq,
        batch: &WriteBatch,
        locators: &[Locator],
    ) -> Result<()>;
}

/// A queued write. [`Commit::wait`] or [`Commit::done`] returns the batch's last sequence
/// once it is visible (and durable, for `Sync`).
#[must_use = "a commit must be awaited to learn whether the write succeeded"]
pub struct Commit {
    seq: Seq,
    rx: oneshot::Receiver<Result<Seq>>,
}

impl Commit {
    /// The last sequence the write takes (for an empty batch, the one before it; for
    /// [`Journal::sync`], the last sequence submitted before the call).
    pub fn seq(&self) -> Seq {
        self.seq
    }

    /// Blocks until the commit completes. Must not be called from a thread that drives
    /// an async runtime; use [`Commit::done`] there.
    pub fn wait(self) -> Result<Seq> {
        self.rx
            .blocking_recv()
            .unwrap_or(Err(Error::RecoveryRequired))
    }

    pub async fn done(self) -> Result<Seq> {
        self.rx.await.unwrap_or(Err(Error::RecoveryRequired))
    }

    /// The outcome if the commit has completed, without blocking. Returns `Some` once;
    /// the caller drops the commit after that.
    pub fn try_result(&mut self) -> Option<Result<Seq>> {
        match self.rx.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(Error::RecoveryRequired)),
        }
    }
}

/// How the end of the journal looked at open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailOutcome {
    /// L1: the log ended at an end marker with nothing valid after it.
    Clean,
    /// L2: damage or a lost write with no evidence of a completed sync. The journal now
    /// ends exactly before `truncated_at_seq`.
    TornTail {
        truncated_at_seq: Seq,
        segment: u64,
        offset: u64,
        /// Bytes of the discarded records the scan could see.
        bytes_discarded: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryReport {
    pub tail: TailOutcome,
    /// G1: damaged records with a known group scope and evidence. Replay skipped each one
    /// and every later record of an overlapping scope.
    pub damage: Vec<Damage>,
    pub replayed_records: u64,
    pub replayed_bytes: u64,
    /// Valid records skipped because their scope was quarantined earlier in the replay.
    pub skipped_records: u64,
    /// Damaged records whose sequences were all at or below `replay_after`: their state
    /// is already in runs, so replay skipped them. Journal values inside them fail their
    /// CRC when read (G3).
    pub flushed_damage: u64,
    /// Last sequence taken by the journal after open (`visible_seq` and `durable_seq`).
    pub last_seq: Seq,
    pub tail_truncations: u64,
    pub tail_bytes: u64,
    pub recovery_ms: u64,
    /// Sequences at or below `replay_after` that the journal no longer held at open (its
    /// end was behind the caller's flushed state). Open voids them so numbers are never
    /// reused; their state is already in runs.
    pub voided_at_open: u64,
}

/// One live segment and the sequences its records take.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentInfo {
    pub segment: u64,
    pub epoch: u64,
    pub first_seq: Seq,
    /// For the active segment, the current `visible_seq`.
    pub last_seq: Seq,
    pub active: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    /// `sync_data` calls on journal segments (one per segment synced).
    pub journal_syncs: u64,
    pub commits: u64,
    pub synced_commits: u64,
    /// Groups that wrote at least one record.
    pub group_commits: u64,
    /// Bytes written to segments, pads included.
    pub journal_bytes: u64,
    /// Time the sink spent in calls of 1 ms or more (memtable stalls).
    pub stall_ms: u64,
    pub tail_truncations: u64,
    pub tail_bytes: u64,
    /// Journal-value reads that failed their CRC or named a recycled segment.
    pub value_corruptions: u64,
}

#[derive(Default)]
struct Counters {
    journal_syncs: AtomicU64,
    commits: AtomicU64,
    synced_commits: AtomicU64,
    group_commits: AtomicU64,
    journal_bytes: AtomicU64,
    stall_ns: AtomicU64,
    tail_truncations: AtomicU64,
    tail_bytes: AtomicU64,
    value_corruptions: AtomicU64,
}

/// `what` of the `Corruption` [`Journal::read_value`] returns when the locator's segment
/// was retired or reused (its epoch no longer matches): the value is gone because the
/// segment was recycled, not because bytes were damaged. A caller holding a visible
/// locator should never see it; it points at a bug in journal GC, not at the media.
pub const VALUE_RECYCLED: &str = "journal locator names a recycled segment";
/// `what` of the `Corruption` for a locator whose range lies outside its segment.
pub const VALUE_OUT_OF_RANGE: &str = "journal locator outside its segment";
/// `what` of the `Corruption` for a value whose bytes fail the locator's CRC (G3).
pub const VALUE_CHECKSUM: &str = "journal value checksum mismatch";
/// Completion of one queued item.
struct Waiter {
    tx: oneshot::Sender<Result<Seq>>,
    on_done: Option<Box<dyn Any + Send>>,
}

impl Waiter {
    /// Drops the payload, then completes the commit, both on the calling thread (the
    /// journal thread for every queued item). A caller that sees its commit complete can
    /// rely on the payload's guards and charges being released already.
    fn complete(self, result: Result<Seq>) {
        let Waiter { tx, on_done } = self;
        drop(on_done);
        let _ = tx.send(result);
    }
}

enum Item {
    Write {
        kind: RecordType,
        batch: WriteBatch,
        sync: bool,
        first_seq: Seq,
        waiter: Waiter,
    },
    Sync {
        target: Seq,
        waiter: Waiter,
    },
}

struct Queue {
    items: VecDeque<Item>,
    /// Sequence the next submitted record takes.
    next_seq: Seq,
    bytes: usize,
    closing: bool,
    /// Test hook: stop without the clean-close pad, as if the process died.
    crash: bool,
    writer_waiting: bool,
}

/// What the journal thread and the callers share about segments.
struct Table {
    live: BTreeMap<u64, TableSeg>,
    free: Vec<FreeSeg>,
    active: u64,
}

struct TableSeg {
    epoch: u64,
    first_seq: Seq,
    /// `None` while active.
    last_seq: Option<Seq>,
    file: Arc<dyn FsFile>,
    path: PathBuf,
    len: u64,
}

impl TableSeg {
    fn from_live(seg: &LiveSeg, last_seq: Option<Seq>) -> TableSeg {
        TableSeg {
            epoch: seg.epoch,
            first_seq: seg.first_seq,
            last_seq,
            file: seg.file.clone(),
            path: seg.path.clone(),
            len: seg.len,
        }
    }
}

struct Shared {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    options: JournalOptions,
    queue: Mutex<Queue>,
    work: Condvar,
    visible: AtomicU64,
    durable: AtomicU64,
    latched: AtomicBool,
    table: Mutex<Table>,
    next_number: AtomicU64,
    stats: Counters,
    /// The spare preparer's state, its wake-up and its completion signal.
    prep: Mutex<writer::Prep>,
    prep_work: Condvar,
    prep_done: Condvar,
    /// Set when the journal stops: a zero fill in progress gives up.
    prep_cancel: AtomicBool,
    /// Where journal values read through locators are cached, and the store namespace.
    value_cache: OnceLock<(Arc<BlockCache>, u64)>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Journal {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
    preparer: Mutex<Option<JoinHandle<()>>>,
}

/// Creates `dir` if needed and makes its entry durable: syncs the parent of every
/// directory created here and of `dir` itself.
fn ensure_durable_dir(fs: &dyn Fs, dir: &Path) -> Result<()> {
    let mut created: Vec<&Path> = Vec::new();
    for ancestor in dir.ancestors() {
        if ancestor.as_os_str().is_empty() || fs.exists(ancestor).map_err(segment::io)? {
            break;
        }
        created.push(ancestor);
    }
    fs.create_dir_all(dir).map_err(segment::io)?;
    let mut parents: Vec<&Path> = Vec::new();
    for d in created.iter().copied().chain(std::iter::once(dir)) {
        let parent = match d.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        if !parents.contains(&parent) {
            parents.push(parent);
        }
    }
    for parent in parents {
        fs.sync_dir(parent).map_err(segment::io)?;
    }
    Ok(())
}

impl Journal {
    /// Opens (or creates) the journal in `dir`, replays every record with a sequence above
    /// `replay_after` through `sink`, repairs a torn tail, and starts the journal thread,
    /// which uses the same sink afterwards.
    ///
    /// Fails with `Corruption` for R1 (a record a completed sync covered is damaged and
    /// its scope is store-wide or unknown) and R3 (structural damage), with `Io` for R2
    /// (a read failed: never treated as a torn write), and with `UnsupportedFormat` for
    /// a segment of an unknown format version.
    pub fn open(
        fs: Arc<dyn Fs>,
        dir: &Path,
        options: JournalOptions,
        replay_after: Seq,
        sink: Arc<dyn JournalSink>,
    ) -> Result<(Journal, RecoveryReport)> {
        if !options.segment_bytes.is_multiple_of(BLOCK)
            || options.segment_bytes < RECORDS_START + BLOCK
        {
            return Err(Error::InvalidArgument(
                "journal segment_bytes must be a multiple of 4096 and at least 12288",
            ));
        }
        let started = std::time::Instant::now();
        ensure_durable_dir(&*fs, dir)?;
        let recovered = recovery::recover(&*fs, dir, &options, replay_after, &*sink)?;
        let mut report = recovered.report.clone();
        let shared = Arc::new(Shared {
            fs,
            dir: dir.to_path_buf(),
            options,
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                next_seq: 0,
                bytes: 0,
                closing: false,
                crash: false,
                writer_waiting: false,
            }),
            work: Condvar::new(),
            visible: AtomicU64::new(0),
            durable: AtomicU64::new(0),
            latched: AtomicBool::new(false),
            table: Mutex::new(Table {
                live: BTreeMap::new(),
                free: Vec::new(),
                active: 0,
            }),
            next_number: AtomicU64::new(recovered.max_number + 1),
            stats: Counters::default(),
            prep: Mutex::new(writer::Prep::default()),
            prep_work: Condvar::new(),
            prep_done: Condvar::new(),
            prep_cancel: AtomicBool::new(false),
            value_cache: OnceLock::new(),
        });
        report.voided_at_open = replay_after.saturating_sub(recovered.next_seq.saturating_sub(1));
        let writer = writer::Writer::finish_open(shared.clone(), recovered, replay_after)?;
        let next_seq = writer.next_seq();
        lock(&shared.queue).next_seq = next_seq;
        shared.visible.store(next_seq - 1, Ordering::Release);
        shared.durable.store(next_seq - 1, Ordering::Release);
        let stats = &shared.stats;
        stats
            .tail_truncations
            .store(report.tail_truncations, Ordering::Relaxed);
        stats.tail_bytes.store(report.tail_bytes, Ordering::Relaxed);
        report.last_seq = next_seq - 1;
        report.recovery_ms = started.elapsed().as_millis() as u64;
        let preparer = writer::spawn_preparer(shared.clone())?;
        let journal = Journal {
            shared: shared.clone(),
            thread: Mutex::new(None),
            preparer: Mutex::new(Some(preparer)),
        };
        let thread = writer.spawn(sink)?;
        *lock(&journal.thread) = Some(thread);
        Ok((journal, report))
    }

    /// Queues `batch` as one record. Never does I/O. Sequence numbers follow submission
    /// order; the batch takes `batch.count()` of them. `on_done` is dropped on the journal
    /// thread once the outcome is known (success or failure), before the commit completes.
    pub fn submit(
        &self,
        batch: WriteBatch,
        durability: Durability,
        on_done: Box<dyn Any + Send>,
    ) -> Result<Commit> {
        self.submit_kind(RecordKind::Batch, batch, durability, on_done)
    }

    /// Like [`Journal::submit`] with an explicit record kind.
    pub fn submit_kind(
        &self,
        kind: RecordKind,
        batch: WriteBatch,
        durability: Durability,
        on_done: Box<dyn Any + Send>,
    ) -> Result<Commit> {
        if batch.leaves_scope() {
            return Err(Error::InvalidArgument("batch leaves its scope"));
        }
        let record_len = RECORD_HEADER_LEN + batch.scope().len() + batch.encoded_bytes();
        debug_assert!(batch.scope().len() <= MAX_SCOPE_LEN);
        if record_len as u64 > self.shared.options.segment_bytes - RECORDS_START {
            return Err(Error::InvalidArgument(
                "batch larger than a journal segment",
            ));
        }
        let (tx, rx) = oneshot::channel();
        let mut queue = lock(&self.shared.queue);
        self.check_open(&queue)?;
        let first_seq = queue.next_seq;
        let count = u64::from(batch.count());
        if first_seq + count > MAX_SEQ {
            return Err(Error::InvalidArgument("sequence numbers exhausted"));
        }
        queue.next_seq += count;
        queue.bytes += batch.encoded_bytes();
        queue.items.push_back(Item::Write {
            kind: match kind {
                RecordKind::Batch => RecordType::Batch,
                RecordKind::Relocate => RecordType::Relocate,
            },
            batch,
            sync: durability == Durability::Sync,
            first_seq,
            waiter: Waiter {
                tx,
                on_done: Some(on_done),
            },
        });
        self.wake(queue);
        Ok(Commit {
            seq: first_seq + count - 1,
            rx,
        })
    }

    /// Completes once every write submitted before this call is durable.
    pub fn sync(&self) -> Result<Commit> {
        let (tx, rx) = oneshot::channel();
        let mut queue = lock(&self.shared.queue);
        self.check_open(&queue)?;
        let target = queue.next_seq - 1;
        queue.items.push_back(Item::Sync {
            target,
            waiter: Waiter { tx, on_done: None },
        });
        self.wake(queue);
        Ok(Commit { seq: target, rx })
    }

    fn check_open(&self, queue: &Queue) -> Result<()> {
        if self.shared.latched.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        if queue.closing {
            return Err(Error::Closed);
        }
        Ok(())
    }

    fn wake(&self, queue: MutexGuard<'_, Queue>) {
        let waiting = queue.writer_waiting;
        drop(queue);
        if waiting {
            self.shared.work.notify_one();
        }
    }

    /// Highest sequence whose write is visible to readers (applied through the sink).
    pub fn visible_seq(&self) -> Seq {
        self.shared.visible.load(Ordering::Acquire)
    }

    /// Highest sequence a completed sync covers.
    pub fn durable_seq(&self) -> Seq {
        self.shared.durable.load(Ordering::Acquire)
    }

    /// True once a write, sync or sink call failed. Only reopening clears it.
    pub fn requires_recovery(&self) -> bool {
        self.shared.latched.load(Ordering::Acquire)
    }

    /// Live segments in order, with the sequences their records take.
    pub fn segments(&self) -> Vec<SegmentInfo> {
        let visible = self.visible_seq();
        let table = lock(&self.shared.table);
        table
            .live
            .iter()
            .map(|(&segment, seg)| SegmentInfo {
                segment,
                epoch: seg.epoch,
                first_seq: seg.first_seq,
                last_seq: seg.last_seq.unwrap_or(visible),
                active: segment == table.active,
            })
            .collect()
    }

    /// Returns `segment` to the free pool for reuse with a new epoch. The store calls it
    /// only once nothing references the segment: its state is flushed and no visible
    /// locator points into it. Refuses the active segment and segments holding records
    /// that are not yet durable. Locators into a retired segment read as `Corruption`.
    pub fn retire(&self, segment: u64) -> Result<()> {
        let durable = self.durable_seq();
        let seg = {
            let mut table = lock(&self.shared.table);
            if segment == table.active {
                return Err(Error::InvalidArgument("cannot retire the active segment"));
            }
            let Some(seg) = table.live.get(&segment) else {
                return Err(Error::InvalidArgument("no such journal segment"));
            };
            if seg.last_seq.is_none_or(|last| last > durable) {
                return Err(Error::InvalidArgument(
                    "journal segment holds records that are not durable",
                ));
            }
            table.live.remove(&segment).expect("present")
        };
        let live = LiveSeg {
            number: segment,
            epoch: seg.epoch,
            first_seq: seg.first_seq,
            file: seg.file,
            path: seg.path,
            len: seg.len,
        };
        let fs = &*self.shared.fs;
        let free = segment::release(
            fs,
            &self.shared.dir,
            live,
            self.shared.options.segment_bytes,
        )?;
        fs.sync_dir(&self.shared.dir).map_err(segment::io)?;
        if let Some(free) = free {
            lock(&self.shared.table).free.push(free);
        }
        Ok(())
    }

    /// Caches journal values read through locators in `cache`, keyed under `namespace`
    /// (the store's [`crate::run::RunReadContext::namespace`]). The first call wins.
    pub fn set_value_cache(&self, cache: Arc<BlockCache>, namespace: u64) {
        let _ = self.shared.value_cache.set((cache, namespace));
    }

    /// Reads a journal value, checks its CRC and caches it (see
    /// [`Journal::read_value_with`]).
    pub fn read_value(&self, locator: &Locator) -> Result<Bytes> {
        self.read_value_with(locator, true)
    }

    /// Reads a journal value through the block cache, when one is set: a hit returns the
    /// cached bytes (checked when they were read); a miss reads and checks the CRC and,
    /// with `fill_cache`, inserts the value under `CacheKey::Journal`.
    ///
    /// Errors are `Corruption` with `file: JournalValue { segment }` and one of these
    /// `what`s: [`VALUE_RECYCLED`] when the segment is no longer live in the locator's
    /// epoch (also when it was retired while the read was in flight), and
    /// [`VALUE_OUT_OF_RANGE`] or [`VALUE_CHECKSUM`] for damage (G3). Every one counts in
    /// `value_corruptions`. A failed read is `Io`.
    pub fn read_value_with(&self, locator: &Locator, fill_cache: bool) -> Result<Bytes> {
        let corrupt = |what| {
            self.shared
                .stats
                .value_corruptions
                .fetch_add(1, Ordering::Relaxed);
            Error::Corruption {
                file: FileId::JournalValue {
                    segment: locator.segment,
                },
                offset: locator.offset,
                what,
            }
        };
        let max_len = self.shared.options.segment_bytes - RECORDS_START;
        let file = {
            let table = lock(&self.shared.table);
            match table.live.get(&locator.segment) {
                Some(seg) if seg.epoch == locator.epoch => {
                    let in_range = locator.offset >= RECORDS_START
                        && u64::from(locator.len) <= max_len
                        && locator
                            .offset
                            .checked_add(u64::from(locator.len))
                            .is_some_and(|end| end <= seg.len);
                    if !in_range {
                        return Err(corrupt(VALUE_OUT_OF_RANGE));
                    }
                    seg.file.clone()
                }
                _ => return Err(corrupt(VALUE_RECYCLED)),
            }
        };
        let cache = self.shared.value_cache.get();
        let key = cache.map(|(_, namespace)| CacheKey::Journal {
            store: *namespace,
            segment: locator.segment,
            epoch: locator.epoch,
            offset: locator.offset,
        });
        if let (Some((cache, _)), Some(key)) = (cache, &key)
            && let Some(value) = cache.get(key)
            && value.len() == locator.len as usize
        {
            return Ok(value);
        }
        let mut value = vec![0u8; locator.len as usize];
        file.read_exact_at(&mut value, locator.offset)
            .map_err(segment::io)?;
        if crc::crc32c(&value) != locator.crc32c {
            // A retire (and a reuse) racing with this read reports as recycled.
            let still_live = lock(&self.shared.table)
                .live
                .get(&locator.segment)
                .is_some_and(|seg| seg.epoch == locator.epoch);
            return Err(corrupt(if still_live {
                VALUE_CHECKSUM
            } else {
                VALUE_RECYCLED
            }));
        }
        let value = Bytes::from(value);
        Ok(match (cache, key) {
            (Some((cache, _)), Some(key)) if fill_cache => cache.insert(key, value),
            _ => value,
        })
    }

    pub fn stats(&self) -> JournalStats {
        let s = &self.shared.stats;
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        JournalStats {
            journal_syncs: get(&s.journal_syncs),
            commits: get(&s.commits),
            synced_commits: get(&s.synced_commits),
            group_commits: get(&s.group_commits),
            journal_bytes: get(&s.journal_bytes),
            stall_ms: get(&s.stall_ns) / 1_000_000,
            tail_truncations: get(&s.tail_truncations),
            tail_bytes: get(&s.tail_bytes),
            value_corruptions: get(&s.value_corruptions),
        }
    }

    /// Finishes every queued write, syncs, writes and syncs the clean-close pad, and
    /// stops the journal thread. Later submits fail with `Closed`. Returns
    /// `RecoveryRequired` when the journal is latched (no close pad is written then).
    pub fn close(&self) -> Result<()> {
        self.stop(false)
    }

    fn stop(&self, crash: bool) -> Result<()> {
        {
            let mut queue = lock(&self.shared.queue);
            queue.closing = true;
            queue.crash |= crash;
        }
        self.shared.work.notify_one();
        if let Some(thread) = lock(&self.thread).take() {
            let _ = thread.join();
        }
        {
            let mut prep = lock(&self.shared.prep);
            prep.stop = true;
        }
        self.shared.prep_cancel.store(true, Ordering::Relaxed);
        self.shared.prep_work.notify_all();
        if let Some(thread) = lock(&self.preparer).take() {
            let _ = thread.join();
        }
        if self.requires_recovery() {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    /// Test hook: stops the thread without the clean-close pad, as if the process died.
    /// Queued items that were not written yet fail.
    #[cfg(test)]
    fn crash(&self) {
        let _ = self.stop(true);
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.stop(false);
    }
}
