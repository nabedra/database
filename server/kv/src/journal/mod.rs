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
//! cases L1, L2, G1, R1 to R3). See [`recovery`].

use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::batch::{MAX_SCOPE_LEN, WriteBatch};
use crate::crc;
use crate::error::{Damage, Error, FileId, Result};
use crate::fs::{Fs, FsFile};
use crate::types::{Locator, MAX_SEQ, Seq};

pub mod format;
mod recovery;
mod segment;
mod writer;

#[cfg(test)]
mod tests;

use format::{BLOCK, RECORD_HEADER_LEN, RecordType};
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
    /// Size of each segment file. A multiple of 4096, at least 8192. Default 64 MiB.
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

/// Completion of one queued item.
struct Waiter {
    tx: oneshot::Sender<Result<Seq>>,
    on_done: Option<Box<dyn Any + Send>>,
}

impl Waiter {
    /// Completes the commit, then drops the payload (on the calling thread, which is the
    /// journal thread for every queued item).
    fn complete(self, result: Result<Seq>) {
        let Waiter { tx, on_done } = self;
        let _ = tx.send(result);
        drop(on_done);
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
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Journal {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
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
        if !options.segment_bytes.is_multiple_of(BLOCK) || options.segment_bytes < 2 * BLOCK {
            return Err(Error::InvalidArgument(
                "journal segment_bytes must be a multiple of 4096 and at least 8192",
            ));
        }
        let started = std::time::Instant::now();
        fs.create_dir_all(dir).map_err(segment::io)?;
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
        });
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
        let thread = writer.spawn(sink)?;
        Ok((
            Journal {
                shared,
                thread: Mutex::new(Some(thread)),
            },
            report,
        ))
    }

    /// Queues `batch` as one record. Never does I/O. Sequence numbers follow submission
    /// order; the batch takes `batch.count()` of them. `on_done` is dropped on the journal
    /// thread after the commit completes (successfully or not).
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
        if record_len as u64 > self.shared.options.segment_bytes - BLOCK {
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

    /// Reads a journal value and checks its CRC.
    pub fn read_value(&self, locator: &Locator) -> Result<Bytes> {
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
        let file = {
            let table = lock(&self.shared.table);
            match table.live.get(&locator.segment) {
                Some(seg) if seg.epoch == locator.epoch => {
                    if locator.offset < BLOCK || locator.offset + u64::from(locator.len) > seg.len {
                        return Err(corrupt("journal locator outside its segment"));
                    }
                    seg.file.clone()
                }
                _ => return Err(corrupt("journal locator names a recycled segment")),
            }
        };
        let mut value = vec![0u8; locator.len as usize];
        file.read_exact_at(&mut value, locator.offset)
            .map_err(segment::io)?;
        if crc::crc32c(&value) != locator.crc32c {
            return Err(corrupt("journal value checksum mismatch"));
        }
        Ok(Bytes::from(value))
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
