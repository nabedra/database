//! The journal thread: group commit, segment rolls, sync alignment and the open-time
//! repair writes that recovery decided on.
//!
//! # Services and the journal park
//!
//! `kv-journal` ([`JournalService`]) and `kv-journal-prep` ([`PrepService`]) are stepped
//! services. In a product build each runs its loop ([`Writer::run`], [`prepare_loop`])
//! on its own thread, unchanged. Under `sim`, inside a simulated world, the runtime calls
//! `step` instead: one group (or one spare) per step, and `Idle` where the loop would
//! wait for work.
//!
//! The journal's sink may stall (the store's memtable budget or L0 stop trigger), and
//! what ends the stall is a flush or compaction, which itself waits for a journal sync.
//! Product breaks that cycle in the store: a stalled sink lets the journal through once
//! a sync is waiting. A step must not wait at all, so under `sim` the journal asks the
//! sink before each sink call whether that call would stall
//! ([`JournalSink::stall_deadline`], the same predicate). If so, it saves where the group
//! stands ([`Stage`]) and returns `Idle` until the poll deadline. The flush step that
//! runs meanwhile registers its sync and drives the journal from `Commit::wait`; the
//! journal resumes at the saved record and the predicate now lets it through. This is
//! the one place where simulated control flow differs from product (risk R1 of the
//! phase 9 design), and it is compiled only with `sim`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, MutexGuard};
use std::time::{Duration, Instant};

#[cfg(feature = "sim")]
use nabedra_rt::task::{self, Step, Stepped};
use nabedra_rt::time;

#[cfg(feature = "sim")]
use super::service::Condvar;
use super::service::ServiceHandle;
use crate::batch::WriteBatch;
use crate::crc;
use crate::error::{Error, Result};
use crate::fs::FsFile;
use crate::types::{Locator, Seq};

use super::format::{
    self, BLOCK, FLAG_CLOSE, FLAG_ROLL, FLAG_SYNC, MIN_PAD, Payload, RECORD_HEADER_LEN,
    RECORDS_START, RecordFields, RecordType,
};
use super::recovery::{End, Recovered};
use super::segment::{self, FreeSeg, LiveSeg};
use super::{Item, JournalSink, Queue, RecordKind, Shared, Table, TableSeg, lock};

/// Free segments the preparer keeps ready for the next roll (bounded: it never creates a
/// file while this many are free).
const SPARES: usize = 1;
/// A failed background preparation is retried no sooner than this.
const PREPARE_RETRY: Duration = Duration::from_secs(1);

/// Why a step failed.
enum Fail {
    /// The next segment file could not be prepared for lack of space. No byte of the
    /// affected records was written.
    NoSpace,
    /// Anything else: the journal latches.
    Fatal(Error),
}

impl From<Error> for Fail {
    fn from(error: Error) -> Fail {
        Fail::Fatal(error)
    }
}

impl Fail {
    fn into_error(self) -> Error {
        match self {
            Fail::NoSpace => Error::NoSpace,
            Fail::Fatal(error) => error,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pending,
    Placed,
    Applied,
    NoSpace,
    Failed,
}

struct Placement {
    item: usize,
    segment: u64,
    epoch: u64,
    /// File offset of the record's payload.
    payload_offset: u64,
}

pub(super) struct Writer {
    shared: Arc<Shared>,
    active: LiveSeg,
    /// File offset in `active` where `buf` will be written.
    pos: u64,
    /// `active` holds bytes no sync has covered yet.
    active_dirty: bool,
    /// Earlier segments with bytes no sync has covered yet.
    prev_unsynced: Vec<Arc<dyn FsFile>>,
    next_write_seq: Seq,
    durable: Seq,
    /// Inclusive range of sequence numbers whose writes failed with `NoSpace` and that a
    /// void pad must still cover.
    void: Option<(Seq, Seq)>,
    buf: Vec<u8>,
    placements: Vec<Placement>,
    status: Vec<Status>,
    locators: Vec<Locator>,
    /// Under `sim`: set once the runtime steps this writer, so a group parks before a
    /// sink call that would stall instead of waiting in it.
    #[cfg(feature = "sim")]
    parks: bool,
}

/// Where a written group stands. A product group runs through both stages in one call;
/// under `sim` a group may stop before a sink call and resume here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// RD3: the group is written but not synced, and the first `applied` records of its
    /// unsynced head are applied (`visible` is the last of them).
    Head {
        applied: usize,
        visible: Option<Seq>,
    },
    /// The group is written (and synced when `synced`); placements before `next` are
    /// applied.
    Publish {
        next: usize,
        synced: bool,
        counted: bool,
    },
}

/// A group parked before a sink call that would stall: where it resumes and when to
/// look again.
#[cfg(feature = "sim")]
#[derive(Debug)]
struct Park {
    stage: Stage,
    until: Instant,
}

/// A product build never parks: the journal thread waits inside the sink.
#[cfg(not(feature = "sim"))]
#[derive(Debug)]
enum Park {}

/// What [`Writer::publish_unsynced_head`] reached.
enum Head {
    /// The head is published; whether the group was counted.
    Done(bool),
    /// A sink call failed; the journal is latched.
    Failed,
    #[cfg_attr(not(feature = "sim"), allow(dead_code))]
    Parked(Park),
}

/// What [`take_group`] found.
enum Take {
    Group,
    /// The journal stops without its close pad (test hook).
    Crash,
    /// The journal is closing and its queue is empty.
    Close,
}

impl Writer {
    /// Applies the repair recovery decided on (delete unused header-less segments, free
    /// the segments after the end, fence or re-epoch the end segment, write the open pad
    /// after a clean close, sync every live segment, fill a gap up to `replay_after`),
    /// fills the segment table and returns a writer positioned for the next record.
    ///
    /// Every live segment is synced before `durable_seq` is published over it, including
    /// segments below the replay range: their records hold journal values that flushed
    /// runs may reference (RV-KV1 F5).
    pub(super) fn finish_open(
        shared: Arc<Shared>,
        recovered: Recovered,
        replay_after: Seq,
    ) -> Result<Writer> {
        let Recovered {
            mut live,
            mut free,
            end,
            next_seq,
            max_durable,
            discard,
            ..
        } = recovered;
        let fs = &*shared.fs;
        let dir = &shared.dir;
        let options = &shared.options;
        let key = options.auth_key;
        let keyed = key.is_some();
        let seg_key = |seg: &LiveSeg| if seg.auth { key } else { None };
        // A store with a journal key never appends to a segment in the CRC frame (one
        // written before its upgrade, LSM-1): it fences that segment after its clean-close
        // pad and continues in a new authenticated segment.
        let end = match end {
            End::Append { idx, pos } if keyed && !live[idx].auth => End::Fence {
                idx,
                pad_at: Some(pos),
            },
            other => other,
        };

        // Unused segment files (L4) hold nothing; delete them first.
        if !discard.is_empty() {
            for path in discard {
                fs.remove(&path).map_err(segment::io)?;
            }
            fs.sync_dir(dir).map_err(segment::io)?;
        }

        // Segments after the end hold nothing the journal keeps. Free them before any
        // fence is written, so a crash in between cannot chain into them.
        let end_idx = end.index();
        let keep = end_idx.map_or(0, |i| i + 1);
        if live.len() > keep {
            for seg in live.drain(keep..) {
                if let Some(f) = segment::release(fs, dir, seg, options.segment_bytes)? {
                    free.push(f);
                }
            }
            fs.sync_dir(dir).map_err(segment::io)?;
        }

        let sync_range = |live: &[LiveSeg], to: usize| -> Result<()> {
            for seg in &live[..to] {
                seg.file.sync_data().map_err(segment::io)?;
            }
            Ok(())
        };
        let mut new_active = false;
        let mut open_pad = false;
        let pos = match end {
            End::Fresh => {
                new_active = true;
                RECORDS_START
            }
            End::Append { idx, pos } => {
                sync_range(&live, idx + 1)?;
                open_pad = true;
                pos
            }
            End::Reepoch { idx } => {
                sync_range(&live, idx)?;
                segment::reepoch(&mut live[idx], key.as_ref())?;
                RECORDS_START
            }
            End::Fence { idx, pad_at } => {
                let seg = &live[idx];
                if let Some(at) = pad_at.filter(|&at| seg.len.saturating_sub(at) >= MIN_PAD) {
                    let mut pad = Vec::with_capacity(RECORD_HEADER_LEN);
                    format::encode_record(
                        &mut pad,
                        &RecordFields {
                            kind: RecordType::Pad,
                            flags: FLAG_ROLL,
                            epoch: seg.epoch,
                            first_seq: next_seq,
                            count: 0,
                            durable_seq: max_durable,
                        },
                        &[],
                        Payload::Zeros(0),
                        seg_key(seg).as_ref(),
                    );
                    seg.file.write_all_at(&pad, at).map_err(segment::io)?;
                }
                sync_range(&live, idx + 1)?;
                new_active = true;
                RECORDS_START
            }
        };
        if new_active {
            let spare = match free.pop() {
                Some(f) => f,
                None => segment::create_free(
                    fs,
                    dir,
                    shared.next_number.fetch_add(1, Ordering::Relaxed),
                    options.segment_bytes,
                    options.zero_fill,
                    None,
                )?,
            };
            let number = shared.next_number.fetch_add(1, Ordering::Relaxed);
            live.push(segment::activate(
                fs,
                dir,
                spare,
                number,
                next_seq,
                key.as_ref(),
            )?);
        } else {
            // The inventory saw the names of the process that stopped, which may not be
            // durable yet: a Kill between a roll's rename and its directory sync leaves the
            // new live name only in the OS. This open appends to that segment and the store
            // records it in the manifest, so its name must be durable first (KVfix4).
            // `activate` above syncs the directory itself.
            fs.sync_dir(dir).map_err(segment::io)?;
        }

        let active = live.last().expect("an active segment").clone();
        {
            let mut table = lock(&shared.table);
            *table = Table {
                live: live
                    .iter()
                    .enumerate()
                    .map(|(i, seg)| {
                        let last = live.get(i + 1).map(|next| next.first_seq - 1);
                        (seg.number, TableSeg::from_live(seg, last))
                    })
                    .collect(),
                free,
                active: active.number,
            };
        }
        let mut writer = Writer {
            shared,
            active,
            pos,
            active_dirty: false,
            prev_unsynced: Vec::new(),
            next_write_seq: next_seq,
            durable: next_seq - 1,
            void: None,
            buf: Vec::new(),
            placements: Vec::new(),
            status: Vec::new(),
            locators: Vec::new(),
            #[cfg(feature = "sim")]
            parks: false,
        };
        if open_pad {
            // Appending after a clean close: first a synced pad without FLAG_CLOSE, so a
            // record this run loses later never sits right after the close pad (F1).
            writer.push_pad(FLAG_SYNC, next_seq, 0, (BLOCK - MIN_PAD) as usize);
            writer.sync_aligned(0).map_err(Fail::into_error)?;
        }
        if replay_after >= next_seq {
            // The store flushed state the journal no longer holds (a torn unsynced tail).
            // Void those numbers so the sequence stays continuous and is never reused.
            writer.void = Some((next_seq, replay_after));
            writer
                .emit_void()
                .and_then(|()| writer.sync_aligned(0))
                .map_err(Fail::into_error)?;
        }
        Ok(writer)
    }

    pub(super) fn next_seq(&self) -> Seq {
        self.next_write_seq
    }

    /// Starts the `kv-journal` service (its thread, in a product build).
    pub(super) fn spawn(self, sink: Arc<dyn JournalSink>) -> Result<ServiceHandle> {
        let stack_bytes = crate::store::THREAD_STACK_BYTES;
        #[cfg(feature = "sim")]
        let handle = task::spawn_stepped(
            "kv-journal",
            stack_bytes,
            JournalService {
                writer: Some(self),
                sink,
                items: Vec::new(),
                parked: None,
            },
        );
        #[cfg(not(feature = "sim"))]
        let handle =
            super::service::spawn_thread("kv-journal", stack_bytes, move || self.run(&*sink));
        handle.map_err(segment::io)
    }

    /// The `kv-journal` thread body.
    fn run(mut self, sink: &dyn JournalSink) {
        let mut items: Vec<Item> = Vec::new();
        loop {
            {
                let shared = self.shared.clone();
                let mut queue = lock(&shared.queue);
                while queue.items.is_empty() && !queue.closing {
                    queue.writer_waiting = true;
                    queue = shared
                        .work
                        .wait(queue)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    queue.writer_waiting = false;
                }
                match take_group(queue, &mut items, shared.options.max_group_bytes) {
                    Take::Group => {}
                    Take::Crash => return,
                    Take::Close => {
                        self.close_cleanly();
                        return;
                    }
                }
            }
            let processed = self.process(&mut items, sink);
            debug_assert!(processed.is_ok(), "the journal thread never parks");
            self.maybe_prepare_spare();
        }
    }

    fn latched(&self) -> bool {
        self.shared.latched.load(Ordering::Acquire)
    }

    fn latch(&self) {
        self.shared.latched.store(true, Ordering::Release);
    }

    /// Sequence of the last write submitted so far.
    fn last_submitted_seq(&self) -> Seq {
        lock(&self.shared.queue).next_seq - 1
    }

    fn at(&self) -> u64 {
        self.pos + self.buf.len() as u64
    }

    /// Writes one group and publishes it. Under `sim` it may park before a sink call
    /// that would stall; [`Writer::advance`] resumes the group from the returned stage.
    fn process(&mut self, items: &mut Vec<Item>, sink: &dyn JournalSink) -> Result<(), Park> {
        if self.latched() {
            for item in items.drain(..) {
                complete(item, Err(Error::RecoveryRequired));
            }
            return Ok(());
        }
        self.placements.clear();
        self.status.clear();
        self.status.resize(items.len(), Status::Pending);
        let mut fail: Option<Status> = None;
        let mut wants_sync = false;
        for (i, item) in items.iter().enumerate() {
            let Item::Write {
                kind,
                batch,
                sync,
                first_seq,
                ..
            } = item
            else {
                wants_sync = true;
                continue;
            };
            if let Some(status) = fail {
                self.status[i] = status;
                continue;
            }
            if self.void.is_some_and(|(_, to)| *first_seq <= to) {
                self.status[i] = Status::NoSpace;
                continue;
            }
            let placed = self.emit_void().and_then(|()| {
                self.ensure_room(record_len(batch))?;
                Ok(())
            });
            match placed {
                Ok(()) => {}
                Err(Fail::NoSpace) => {
                    let to = self.last_submitted_seq();
                    let from = self.void.map_or(*first_seq, |(from, _)| from);
                    self.void = Some((from, to));
                    self.status[i] = Status::NoSpace;
                    fail = Some(Status::NoSpace);
                    continue;
                }
                Err(Fail::Fatal(_)) => {
                    self.status[i] = Status::Failed;
                    fail = Some(Status::Failed);
                    continue;
                }
            }
            debug_assert_eq!(*first_seq, self.next_write_seq);
            let offset = self.at();
            let fields = RecordFields {
                kind: *kind,
                flags: if *sync { FLAG_SYNC } else { 0 },
                epoch: self.active.epoch,
                first_seq: *first_seq,
                count: batch.count(),
                durable_seq: self.durable,
            };
            let key = self.frame_key();
            format::encode_record(
                &mut self.buf,
                &fields,
                batch.scope(),
                Payload::Bytes(batch.data()),
                key.as_ref(),
            );
            self.placements.push(Placement {
                item: i,
                segment: self.active.number,
                epoch: self.active.epoch,
                payload_offset: offset + (RECORD_HEADER_LEN + batch.scope().len()) as u64,
            });
            self.next_write_seq = first_seq + u64::from(batch.count());
            self.status[i] = Status::Placed;
            wants_sync |= *sync;
        }

        if wants_sync && fail.is_none() && self.void.is_some() {
            // A group of only `Sync` items after a `NoSpace`: void the failed numbers now,
            // so the sync can cover them once space is back (RV-KV1 F4).
            match self.emit_void() {
                Ok(()) => {}
                Err(Fail::NoSpace) => fail = Some(Status::NoSpace),
                Err(Fail::Fatal(_)) => fail = Some(Status::Failed),
            }
        }
        let mut ok = fail != Some(Status::Failed);
        // RD3: when the group syncs, its bytes are written first, then the unsynced writes
        // at its head become visible and complete before the sync (the `Head` stage).
        // They never asked for one; alone they would have been a group of their own.
        let mut head = false;
        if ok {
            let dirty = self.active_dirty || !self.prev_unsynced.is_empty() || !self.buf.is_empty();
            let written = if wants_sync && dirty {
                head = true;
                self.write_aligned(0)
            } else {
                self.flush_buf()
            };
            ok = written.is_ok();
        }
        #[cfg(feature = "sim")]
        if ok && !self.placements.is_empty() {
            let last_seq = self.next_write_seq - 1;
            crate::sim_trace::report(crate::sim_trace::JournalStep::Written { last_seq });
        }
        if !ok {
            self.fail_group(items);
            return Ok(());
        }
        let stage = if head {
            Stage::Head {
                applied: 0,
                visible: None,
            }
        } else {
            Stage::Publish {
                next: 0,
                synced: false,
                // Whether the group's commit counters were already bumped (by the
                // unsynced head).
                counted: false,
            }
        };
        self.advance(items, sink, stage)
    }

    /// Latches and fails the group and everything queued after it.
    fn fail_group(&mut self, items: &mut Vec<Item>) {
        self.latch();
        for item in items.drain(..) {
            complete(item, Err(Error::RecoveryRequired));
        }
        self.drain_queue_after_latch();
    }

    /// Runs a written group from `stage` to its end: the unsynced head and the sync (for
    /// a `Head` stage), then visibility, counters and completions. Under `sim` it returns
    /// `Err` with the stage to resume from when a sink call would stall.
    fn advance(
        &mut self,
        items: &mut Vec<Item>,
        sink: &dyn JournalSink,
        stage: Stage,
    ) -> Result<(), Park> {
        let (next, synced, counted) = match stage {
            Stage::Head { applied, visible } => {
                match self.publish_unsynced_head(items, sink, applied, visible) {
                    Head::Done(counted) => {
                        if self.sync_written().is_err() {
                            self.fail_group(items);
                            return Ok(());
                        }
                        (0, true, counted)
                    }
                    Head::Failed => {
                        self.fail_group(items);
                        return Ok(());
                    }
                    Head::Parked(park) => return Err(park),
                }
            }
            Stage::Publish {
                next,
                synced,
                counted,
            } => (next, synced, counted),
        };

        // Visibility: the sink sees each record in sequence order.
        let stats = &self.shared.stats;
        let mut visible = self.next_write_seq - 1;
        for (index, p) in self.placements.iter().enumerate().skip(next) {
            let Item::Write {
                kind,
                batch,
                first_seq,
                ..
            } = &items[p.item]
            else {
                unreachable!("placements index writes")
            };
            if self.latched() {
                break;
            }
            #[cfg(feature = "sim")]
            if let Some(until) = self.park_deadline(sink, *kind, *first_seq, batch) {
                let stage = Stage::Publish {
                    next: index,
                    synced,
                    counted,
                };
                return Err(Park { stage, until });
            }
            #[cfg(not(feature = "sim"))]
            let _ = index;
            fill_locators(&mut self.locators, batch, p);
            let started = time::now_std();
            let kind = if *kind == RecordType::Relocate {
                RecordKind::Relocate
            } else {
                RecordKind::Batch
            };
            let applied = sink.apply(kind, *first_seq, batch, &self.locators);
            let elapsed = time::now_std().duration_since(started);
            if elapsed >= Duration::from_millis(1) {
                stats
                    .stall_ns
                    .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
            }
            if applied.is_err() {
                self.latch();
                visible = first_seq - 1;
                break;
            }
            self.status[p.item] = Status::Applied;
        }
        self.shared.visible.store(visible, Ordering::Release);
        if synced {
            self.shared.durable.store(self.durable, Ordering::Release);
        }
        #[cfg(feature = "sim")]
        if !self.placements.is_empty() {
            let durable = self.shared.durable.load(Ordering::Acquire);
            crate::sim_trace::report(if synced {
                crate::sim_trace::JournalStep::Synced { synced: self.durable, visible, durable }
            } else {
                crate::sim_trace::JournalStep::Published { visible, durable }
            });
        }

        // Count the group before any waiter wakes: a caller that reads the stats right
        // after its commit returns must see that commit counted (ns08 saw 385 vs 384).
        let mut commits = 0;
        let mut synced_commits = 0;
        for (i, item) in items.iter().enumerate() {
            if let (Item::Write { sync, .. }, Status::Applied) = (item, self.status[i]) {
                commits += 1;
                synced_commits += u64::from(*sync);
            }
        }
        stats.commits.fetch_add(commits, Ordering::Relaxed);
        stats
            .synced_commits
            .fetch_add(synced_commits, Ordering::Relaxed);
        if !self.placements.is_empty() && !counted {
            stats.group_commits.fetch_add(1, Ordering::Relaxed);
        }
        for (i, item) in items.drain(..).enumerate() {
            let result = match (&item, self.status[i]) {
                (
                    Item::Write {
                        batch, first_seq, ..
                    },
                    Status::Applied,
                ) => Ok(first_seq + u64::from(batch.count()) - 1),
                (Item::Write { .. }, Status::NoSpace) => Err(Error::NoSpace),
                (Item::Write { .. }, _) => Err(Error::RecoveryRequired),
                (Item::Sync { target, .. }, _) => {
                    if self.durable >= *target {
                        Ok(*target)
                    } else if self.latched() {
                        Err(Error::RecoveryRequired)
                    } else {
                        // Only voided (failed) writes lie between durable and target.
                        Err(Error::NoSpace)
                    }
                }
            };
            complete(item, result);
        }
        if self.latched() {
            self.drain_queue_after_latch();
        }
        Ok(())
    }

    /// Under `sim`, while the runtime steps this writer: when the sink call for this
    /// record would stall, the time to look again (the journal park).
    #[cfg(feature = "sim")]
    fn park_deadline(
        &self,
        sink: &dyn JournalSink,
        kind: RecordType,
        first_seq: Seq,
        batch: &WriteBatch,
    ) -> Option<Instant> {
        if !self.parks {
            return None;
        }
        let kind = if kind == RecordType::Relocate {
            RecordKind::Relocate
        } else {
            RecordKind::Batch
        };
        sink.stall_deadline(kind, first_seq, batch)
    }

    fn drain_queue_after_latch(&self) {
        let items: Vec<Item> = lock(&self.shared.queue).items.drain(..).collect();
        for item in items {
            complete(item, Err(Error::RecoveryRequired));
        }
    }

    /// Writes the pending void pad(s), if any.
    fn emit_void(&mut self) -> Result<(), Fail> {
        while let Some((from, to)) = self.void {
            self.ensure_room(MIN_PAD)?;
            debug_assert_eq!(from, self.next_write_seq);
            let count = (to - from + 1).min(u64::from(u32::MAX)) as u32;
            self.push_pad(0, from, count, 0);
            self.next_write_seq = from + u64::from(count);
            self.void = (self.next_write_seq <= to).then_some((self.next_write_seq, to));
        }
        Ok(())
    }

    fn push_pad(&mut self, flags: u8, first_seq: Seq, count: u32, payload: usize) {
        let fields = RecordFields {
            kind: RecordType::Pad,
            flags,
            epoch: self.active.epoch,
            first_seq,
            count,
            durable_seq: self.durable,
        };
        let key = self.frame_key();
        format::encode_record(
            &mut self.buf,
            &fields,
            &[],
            Payload::Zeros(payload),
            key.as_ref(),
        );
    }

    /// The key the active segment's records are tagged with: the journal key in the
    /// authenticated frame, none in the CRC frame.
    fn frame_key(&self) -> Option<crate::mac::JournalKey> {
        if self.active.auth {
            self.shared.options.auth_key
        } else {
            None
        }
    }

    /// Makes room for `len` more bytes in the active segment, rolling to the next one
    /// when needed. The next segment is prepared before anything is written, so a
    /// `NoSpace` here leaves the records that did not fit unwritten.
    fn ensure_room(&mut self, len: u64) -> Result<(), Fail> {
        if self.at() + len <= self.active.len {
            return Ok(());
        }
        let next = self.take_spare().map_err(|e| match e {
            Error::NoSpace => Fail::NoSpace,
            other => Fail::Fatal(other),
        })?;
        if self.active.len - self.at() >= MIN_PAD {
            self.push_pad(FLAG_ROLL, self.next_write_seq, 0, 0);
        }
        if let Err(fail) = self.flush_buf() {
            lock(&self.shared.table).free.push(next);
            return Err(fail);
        }
        if self.active_dirty {
            self.prev_unsynced.push(self.active.file.clone());
            self.active_dirty = false;
        }
        let number = self.shared.next_number.fetch_add(1, Ordering::Relaxed);
        let fs = &*self.shared.fs;
        let key = self.shared.options.auth_key;
        let new = segment::activate(
            fs,
            &self.shared.dir,
            next,
            number,
            self.next_write_seq,
            key.as_ref(),
        )?;
        {
            let mut table = lock(&self.shared.table);
            if let Some(old) = table.live.get_mut(&self.active.number) {
                old.last_seq = Some(self.next_write_seq - 1);
            }
            table.live.insert(number, TableSeg::from_live(&new, None));
            table.active = number;
        }
        self.active = new;
        self.pos = RECORDS_START;
        Ok(())
    }

    /// The free segment for a roll: a ready one from the pool; else the one the preparer
    /// is finishing; else (no spare at all) one created inline on this thread.
    fn take_spare(&mut self) -> Result<FreeSeg> {
        if let Some(free) = lock(&self.shared.table).free.pop() {
            return Ok(free);
        }
        // Under `sim`, inside a world, a preparer step makes a whole spare atomically and
        // never runs during a journal step, so `busy` is false here and the loop below
        // never waits (a wait there would panic in `rt::sync::Condvar`).
        {
            let mut prep = lock(&self.shared.prep);
            while prep.busy {
                prep = self
                    .shared
                    .prep_done
                    .wait(prep)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        if let Some(free) = lock(&self.shared.table).free.pop() {
            return Ok(free);
        }
        let number = self.shared.next_number.fetch_add(1, Ordering::Relaxed);
        let options = &self.shared.options;
        segment::create_free(
            &*self.shared.fs,
            &self.shared.dir,
            number,
            options.segment_bytes,
            options.zero_fill,
            None,
        )
    }

    /// Once the active segment is half used and no free segment is ready, asks the
    /// preparer thread for one, so the roll never waits for a zero fill (RV-KV1 F8).
    fn maybe_prepare_spare(&mut self) {
        if self.latched() || self.at() < self.active.len / 2 {
            return;
        }
        if lock(&self.shared.table).free.len() >= SPARES {
            return;
        }
        let mut prep = lock(&self.shared.prep);
        if prep.busy
            || prep.wanted
            || prep
                .failed_at
                .is_some_and(|t| time::now_std().duration_since(t) < PREPARE_RETRY)
        {
            return;
        }
        prep.wanted = true;
        drop(prep);
        self.shared.prep_work.notify_one();
    }

    fn flush_buf(&mut self) -> Result<(), Fail> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.active
            .file
            .write_all_at(&self.buf, self.pos)
            .map_err(|e| Fail::Fatal(segment::io(e)))?;
        let len = self.buf.len() as u64;
        self.pos += len;
        self.active_dirty = true;
        self.buf.clear();
        self.shared
            .stats
            .journal_bytes
            .fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// RD3: after the group's bytes are written and before its sync, applies the records
    /// of the unsynced writes that lead the group to the sink in sequence order, publishes
    /// `visible_seq` over them and completes their waiters, so an unsynced write never
    /// waits for a sync that later items asked for. Every byte of these records is in the
    /// file (their locators resolve), `durable_seq` is unchanged, and visibility stays a
    /// prefix of the sequence order: exactly what a group of only these writes followed by
    /// a group of the rest would publish. Removes the completed items from `items` (and
    /// their statuses and placements). Returns whether the group was counted. A sink
    /// failure latches; the records before it stay visible and their waiters succeed.
    ///
    /// It starts after the first `applied` head records (`visible` is the last of them):
    /// zero and `None` for a new group, the parked position when a simulated group
    /// resumes. Head placements index the head items in order, so placement `applied`
    /// is the next head record.
    fn publish_unsynced_head(
        &mut self,
        items: &mut Vec<Item>,
        sink: &dyn JournalSink,
        mut applied: usize,
        mut visible: Option<Seq>,
    ) -> Head {
        let head = items
            .iter()
            .zip(&self.status)
            .take_while(|(item, status)| {
                matches!(item, Item::Write { sync: false, .. }) && **status == Status::Placed
            })
            .count();
        if head == 0 {
            return Head::Done(false);
        }
        let mut failed = false;
        for p in self
            .placements
            .iter()
            .skip(applied)
            .take_while(|p| p.item < head)
        {
            let Item::Write {
                kind,
                batch,
                first_seq,
                ..
            } = &items[p.item]
            else {
                unreachable!("placements index writes")
            };
            #[cfg(feature = "sim")]
            if let Some(until) = self.park_deadline(sink, *kind, *first_seq, batch) {
                let stage = Stage::Head { applied, visible };
                return Head::Parked(Park { stage, until });
            }
            fill_locators(&mut self.locators, batch, p);
            let started = time::now_std();
            let kind = if *kind == RecordType::Relocate {
                RecordKind::Relocate
            } else {
                RecordKind::Batch
            };
            let result = sink.apply(kind, *first_seq, batch, &self.locators);
            let elapsed = time::now_std().duration_since(started);
            if elapsed >= Duration::from_millis(1) {
                self.shared
                    .stats
                    .stall_ns
                    .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
            }
            if result.is_err() {
                self.latch();
                failed = true;
                break;
            }
            visible = Some(first_seq + u64::from(batch.count()) - 1);
            applied += 1;
        }
        if let Some(visible) = visible {
            self.shared.visible.store(visible, Ordering::Release);
            #[cfg(feature = "sim")]
            crate::sim_trace::report(crate::sim_trace::JournalStep::Published {
                visible,
                durable: self.shared.durable.load(Ordering::Acquire),
            });
        }
        // Count before any waiter wakes (ns08).
        let stats = &self.shared.stats;
        stats.commits.fetch_add(applied as u64, Ordering::Relaxed);
        if applied != 0 {
            stats.group_commits.fetch_add(1, Ordering::Relaxed);
        }
        let done: Vec<Item> = items.drain(..applied).collect();
        self.status.drain(..applied);
        self.placements.drain(..applied);
        for p in &mut self.placements {
            p.item -= applied;
        }
        for item in done {
            let Item::Write {
                batch, first_seq, ..
            } = &item
            else {
                unreachable!("the head holds writes only")
            };
            let seq = first_seq + u64::from(batch.count()) - 1;
            complete(item, Ok(seq));
        }
        if failed {
            return Head::Failed;
        }
        Head::Done(applied != 0)
    }

    /// Pads to the next 4 KiB boundary (or the segment end), writes the buffer and syncs
    /// every segment with unsynced bytes. Afterwards no later write touches a synced byte.
    fn sync_aligned(&mut self, extra_flags: u8) -> Result<(), Fail> {
        self.write_aligned(extra_flags)?;
        self.sync_written()
    }

    /// The write half of [`Self::sync_aligned`]: the pad and the buffer.
    fn write_aligned(&mut self, extra_flags: u8) -> Result<(), Fail> {
        let at = self.at();
        if let Some(len) = format::align_pad_len(at) {
            let room = self.active.len - at;
            if room >= MIN_PAD {
                let len = len.min(room);
                self.push_pad(
                    FLAG_SYNC | extra_flags,
                    self.next_write_seq,
                    0,
                    (len - MIN_PAD) as usize,
                );
            }
            // With fewer than MIN_PAD bytes left, the next record rolls to a new segment.
        }
        self.flush_buf()
    }

    /// The sync half of [`Self::sync_aligned`]: every segment with unsynced bytes.
    fn sync_written(&mut self) -> Result<(), Fail> {
        let stats = &self.shared.stats;
        for file in self.prev_unsynced.drain(..) {
            file.sync_data().map_err(|e| Fail::Fatal(segment::io(e)))?;
            stats.journal_syncs.fetch_add(1, Ordering::Relaxed);
        }
        if self.active_dirty {
            self.active
                .file
                .sync_data()
                .map_err(|e| Fail::Fatal(segment::io(e)))?;
            stats.journal_syncs.fetch_add(1, Ordering::Relaxed);
            self.active_dirty = false;
        }
        self.durable = self.next_write_seq - 1;
        Ok(())
    }

    /// Clean close: sync, then write and sync one `Pad` whose `durable_seq` covers every
    /// earlier record, filling a whole block so the next open appends on a boundary.
    fn close_cleanly(&mut self) {
        if self.latched() {
            return;
        }
        let result = (|| -> Result<(), Fail> {
            match self.emit_void() {
                Ok(()) | Err(Fail::NoSpace) => {}
                Err(fail) => return Err(fail),
            }
            self.sync_aligned(0)?;
            self.ensure_room(BLOCK)?;
            self.push_pad(
                FLAG_SYNC | FLAG_CLOSE,
                self.next_write_seq,
                0,
                (BLOCK - MIN_PAD) as usize,
            );
            self.sync_aligned(0)
        })();
        if result.is_err() {
            self.latch();
        } else {
            self.shared.durable.store(self.durable, Ordering::Release);
        }
    }
}

/// What the writer and the preparer thread share.
#[derive(Default)]
pub(super) struct Prep {
    pub(super) wanted: bool,
    pub(super) busy: bool,
    pub(super) stop: bool,
    pub(super) failed_at: Option<Instant>,
}

/// The spare preparer: creates (preallocates, zero-fills, syncs) a free segment whenever
/// the writer asks and fewer than [`SPARES`] are free. It never touches a live segment and
/// never latches: a failure (`NoSpace` included) leaves the writer to create the file
/// inline at the roll, where `NoSpace` is reported as usual.
pub(super) fn spawn_preparer(shared: Arc<Shared>) -> Result<ServiceHandle> {
    let stack_bytes = crate::store::THREAD_STACK_BYTES;
    #[cfg(feature = "sim")]
    let handle = task::spawn_stepped("kv-journal-prep", stack_bytes, PrepService { shared });
    #[cfg(not(feature = "sim"))]
    let handle = super::service::spawn_thread("kv-journal-prep", stack_bytes, move || {
        prepare_loop(&shared)
    });
    handle.map_err(segment::io)
}

/// The `kv-journal-prep` thread body.
fn prepare_loop(shared: &Shared) {
    loop {
        {
            let mut prep = lock(&shared.prep);
            while !prep.stop && !prep.wanted {
                prep = shared
                    .prep_work
                    .wait(prep)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            if prep.stop {
                return;
            }
            if !claim_spare(&mut prep, shared) {
                continue;
            }
        }
        prepare_spare(shared);
    }
}

/// Takes the writer's request for a spare. True when one must be made (the preparer is
/// then busy); false when enough are free already.
#[inline]
fn claim_spare(prep: &mut Prep, shared: &Shared) -> bool {
    prep.wanted = false;
    if lock(&shared.table).free.len() >= SPARES {
        return false;
    }
    prep.busy = true;
    true
}

/// Makes one spare segment and ends the busy state [`claim_spare`] began.
#[inline]
fn prepare_spare(shared: &Shared) {
    let number = shared.next_number.fetch_add(1, Ordering::Relaxed);
    let options = &shared.options;
    let made = segment::create_free(
        &*shared.fs,
        &shared.dir,
        number,
        options.segment_bytes,
        options.zero_fill,
        Some(&shared.prep_cancel),
    );
    let failed = made.is_err();
    if let Ok(free) = made {
        lock(&shared.table).free.push(free);
    }
    let mut prep = lock(&shared.prep);
    prep.busy = false;
    prep.failed_at = failed.then(time::now_std);
    drop(prep);
    shared.prep_done.notify_all();
}

/// Moves the next group (at most `max_group_bytes`, and at least one item) from the
/// queue into `items`, or reports why the journal stops. The caller has seen the queue
/// non-empty or closing.
#[inline]
fn take_group(
    mut queue: MutexGuard<'_, Queue>,
    items: &mut Vec<Item>,
    max_group_bytes: usize,
) -> Take {
    if queue.crash {
        let dropped: Vec<Item> = queue.items.drain(..).collect();
        drop(queue);
        drop(dropped);
        return Take::Crash;
    }
    if queue.items.is_empty() {
        drop(queue);
        return Take::Close;
    }
    let mut bytes = 0;
    while let Some(item) = queue.items.front() {
        let b = item_bytes(item);
        if !items.is_empty() && bytes + b > max_group_bytes {
            break;
        }
        bytes += b;
        items.push(queue.items.pop_front().expect("front exists"));
    }
    queue.bytes -= bytes;
    Take::Group
}

/// The `kv-journal` service: the writer, its sink and the group in hand.
#[cfg(feature = "sim")]
struct JournalService {
    /// Taken on a clean stop or when the runtime starts the threaded loop.
    writer: Option<Writer>,
    sink: Arc<dyn JournalSink>,
    /// The group being processed; empty between groups.
    items: Vec<Item>,
    /// Where the parked group in `items` resumes.
    parked: Option<Stage>,
}

#[cfg(feature = "sim")]
impl Drop for JournalService {
    fn drop(&mut self) {
        let Some(writer) = &self.writer else {
            return;
        };

        // The runtime discards the service on a node cut, even before its first step.
        // Anything still in our group or the queue has no completed outcome. The
        // bytes may be in the recovered prefix, so these commits require recovery.
        writer.latch();
        let queued = {
            let mut queue = lock(&writer.shared.queue);
            queue.closing = true;
            queue.bytes = 0;
            queue.writer_waiting = false;
            std::mem::take(&mut queue.items)
        };
        // Guards can hold the store itself. Set the latch and drop the queue lock
        // before completing them, so store teardown can reenter Journal::stop.
        for item in self.items.drain(..).chain(queued) {
            complete(item, Err(Error::RecoveryRequired));
        }
    }
}

#[cfg(feature = "sim")]
impl Stepped for JournalService {
    /// One group of [`Writer::run`]. A parked group resumes first, and a group
    /// parks (returns a timed `Idle`) before a sink call that would stall.
    fn step(&mut self) -> Step {
        let writer = self.writer.as_mut().expect("live journal service");
        writer.parks = true;
        let shared = writer.shared.clone();
        let mut queue = lock(&shared.queue);
        // Leave pending items for the abort path when the test hook requests a cut.
        if queue.crash {
            return Step::Exit;
        }
        let outcome = if let Some(stage) = self.parked.take() {
            drop(queue);
            writer.advance(&mut self.items, &*self.sink, stage)
        } else {
            if queue.items.is_empty() && !queue.closing {
                queue.writer_waiting = true;
                return Step::Idle(None);
            }
            queue.writer_waiting = false;
            match take_group(queue, &mut self.items, shared.options.max_group_bytes) {
                Take::Group => {}
                Take::Crash => unreachable!("crash checked under the queue lock"),
                Take::Close => {
                    writer.close_cleanly();
                    self.writer = None;
                    return Step::Exit;
                }
            }
            writer.process(&mut self.items, &*self.sink)
        };
        match outcome {
            Ok(()) => {
                writer.maybe_prepare_spare();
                Step::Worked
            }
            Err(park) => {
                self.parked = Some(park.stage);
                Step::Idle(Some(park.until))
            }
        }
    }

    fn doorbell(&self) -> &Condvar {
        &self
            .writer
            .as_ref()
            .expect("live journal service")
            .shared
            .work
    }

    fn run(mut self) {
        self.writer
            .take()
            .expect("live journal service")
            .run(&*self.sink);
    }
}

/// The `kv-journal-prep` service.
#[cfg(feature = "sim")]
pub(super) struct PrepService {
    shared: Arc<Shared>,
}

#[cfg(feature = "sim")]
impl Stepped for PrepService {
    /// One request of [`prepare_loop`]. Each step makes a whole spare, so the journal
    /// never finds the preparer busy while it steps (`take_spare` never waits there).
    fn step(&mut self) -> Step {
        {
            let mut prep = lock(&self.shared.prep);
            if prep.stop {
                return Step::Exit;
            }
            if !prep.wanted {
                return Step::Idle(None);
            }
            if !claim_spare(&mut prep, &self.shared) {
                return Step::Worked;
            }
        }
        prepare_spare(&self.shared);
        Step::Worked
    }

    fn doorbell(&self) -> &Condvar {
        &self.shared.prep_work
    }

    fn run(self) {
        prepare_loop(&self.shared)
    }
}

fn item_bytes(item: &Item) -> usize {
    match item {
        Item::Write { batch, .. } => batch.encoded_bytes(),
        Item::Sync { .. } => 0,
    }
}

fn record_len(batch: &WriteBatch) -> u64 {
    (RECORD_HEADER_LEN + batch.scope().len() + batch.encoded_bytes()) as u64
}

fn complete(item: Item, result: Result<Seq>) {
    match item {
        Item::Write { waiter, batch, .. } => {
            // Free the batch buffer before `complete` drops `on_done`, which may hold
            // the charge for those bytes (M02): a woken committer can build its next
            // batch only once this one's memory is gone.
            drop(batch);
            waiter.complete(result);
        }
        Item::Sync { waiter, .. } => waiter.complete(result),
    }
}

fn fill_locators(out: &mut Vec<Locator>, batch: &WriteBatch, p: &Placement) {
    out.clear();
    let data = batch.data();
    for jv in batch.journal_values() {
        let value = &data[jv.offset as usize..][..jv.len as usize];
        out.push(Locator {
            segment: p.segment,
            epoch: p.epoch,
            offset: p.payload_offset + u64::from(jv.offset),
            len: jv.len,
            crc32c: crc::crc32c(value),
        });
    }
}
