//! The journal thread: group commit, segment rolls, sync alignment and the open-time
//! repair writes that recovery decided on.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::batch::WriteBatch;
use crate::crc;
use crate::error::{Error, Result};
use crate::fs::FsFile;
use crate::types::{Locator, Seq};

use super::format::{
    self, BLOCK, FLAG_CLOSE, FLAG_ROLL, FLAG_SYNC, MIN_PAD, Payload, RECORD_HEADER_LEN,
    RecordFields, RecordType,
};
use super::recovery::{End, Recovered};
use super::segment::{self, FreeSeg, LiveSeg};
use super::{Item, JournalSink, RecordKind, Shared, Table, TableSeg, lock};

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
    spare: Option<FreeSeg>,
    spare_failed_at: Option<Instant>,
    next_write_seq: Seq,
    durable: Seq,
    /// Inclusive range of sequence numbers whose writes failed with `NoSpace` and that a
    /// void pad must still cover.
    void: Option<(Seq, Seq)>,
    buf: Vec<u8>,
    placements: Vec<Placement>,
    status: Vec<Status>,
    locators: Vec<Locator>,
}

impl Writer {
    /// Applies the repair recovery decided on (free the segments after the end, fence or
    /// re-epoch the end segment, sync the replay range, fill a gap up to `replay_after`),
    /// fills the segment table and returns a writer positioned for the next record.
    pub(super) fn finish_open(
        shared: Arc<Shared>,
        recovered: Recovered,
        replay_after: Seq,
    ) -> Result<Writer> {
        let Recovered {
            mut live,
            mut free,
            start,
            end,
            next_seq,
            max_durable,
            ..
        } = recovered;
        let fs = &*shared.fs;
        let dir = &shared.dir;
        let options = &shared.options;

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
            for seg in &live[start.min(to)..to] {
                seg.file.sync_data().map_err(segment::io)?;
            }
            Ok(())
        };
        let mut new_active = false;
        let pos = match end {
            End::Fresh => {
                new_active = true;
                BLOCK
            }
            End::Append { idx, pos } => {
                sync_range(&live, idx + 1)?;
                pos
            }
            End::Reepoch { idx } => {
                sync_range(&live, idx)?;
                segment::reepoch(&mut live[idx])?;
                BLOCK
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
                    );
                    seg.file.write_all_at(&pad, at).map_err(segment::io)?;
                }
                sync_range(&live, idx + 1)?;
                new_active = true;
                BLOCK
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
                )?,
            };
            let number = shared.next_number.fetch_add(1, Ordering::Relaxed);
            live.push(segment::activate(fs, dir, spare, number, next_seq)?);
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
            spare: None,
            spare_failed_at: None,
            next_write_seq: next_seq,
            durable: next_seq - 1,
            void: None,
            buf: Vec::new(),
            placements: Vec::new(),
            status: Vec::new(),
            locators: Vec::new(),
        };
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

    pub(super) fn spawn(self, sink: Arc<dyn JournalSink>) -> Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("kv-journal".into())
            .spawn(move || self.run(&*sink))
            .map_err(segment::io)
    }

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
                if queue.crash {
                    let dropped: Vec<Item> = queue.items.drain(..).collect();
                    drop(queue);
                    drop(dropped);
                    return;
                }
                if queue.items.is_empty() {
                    drop(queue);
                    self.close_cleanly();
                    return;
                }
                let mut bytes = 0;
                while let Some(item) = queue.items.front() {
                    let b = item_bytes(item);
                    if !items.is_empty() && bytes + b > shared.options.max_group_bytes {
                        break;
                    }
                    bytes += b;
                    items.push(queue.items.pop_front().expect("front exists"));
                }
                queue.bytes -= bytes;
            }
            self.process(&mut items, sink);
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

    fn process(&mut self, items: &mut Vec<Item>, sink: &dyn JournalSink) {
        if self.latched() {
            for item in items.drain(..) {
                complete(item, Err(Error::RecoveryRequired));
            }
            return;
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
            format::encode_record(
                &mut self.buf,
                &fields,
                batch.scope(),
                Payload::Bytes(batch.data()),
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

        let mut ok = fail != Some(Status::Failed);
        let mut synced = false;
        if ok {
            let written = (|| -> Result<bool, Fail> {
                let dirty =
                    self.active_dirty || !self.prev_unsynced.is_empty() || !self.buf.is_empty();
                if wants_sync && dirty {
                    self.sync_aligned(0)?;
                    Ok(true)
                } else {
                    self.flush_buf()?;
                    Ok(false)
                }
            })();
            match written {
                Ok(s) => synced = s,
                Err(_) => ok = false,
            }
        }
        if !ok {
            self.latch();
            for item in items.drain(..) {
                complete(item, Err(Error::RecoveryRequired));
            }
            self.drain_queue_after_latch();
            return;
        }

        // Visibility: the sink sees each record in sequence order.
        let stats = &self.shared.stats;
        let mut visible = self.next_write_seq - 1;
        for p in &self.placements {
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
            fill_locators(&mut self.locators, batch, p);
            let started = Instant::now();
            let kind = if *kind == RecordType::Relocate {
                RecordKind::Relocate
            } else {
                RecordKind::Batch
            };
            let applied = sink.apply(kind, *first_seq, batch, &self.locators);
            let elapsed = started.elapsed();
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

        let mut commits = 0;
        let mut synced_commits = 0;
        for (i, item) in items.drain(..).enumerate() {
            let result = match (&item, self.status[i]) {
                (
                    Item::Write {
                        batch,
                        first_seq,
                        sync,
                        ..
                    },
                    Status::Applied,
                ) => {
                    commits += 1;
                    synced_commits += u64::from(*sync);
                    Ok(first_seq + u64::from(batch.count()) - 1)
                }
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
        stats.commits.fetch_add(commits, Ordering::Relaxed);
        stats
            .synced_commits
            .fetch_add(synced_commits, Ordering::Relaxed);
        if !self.placements.is_empty() {
            stats.group_commits.fetch_add(1, Ordering::Relaxed);
        }
        if self.latched() {
            self.drain_queue_after_latch();
        }
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
        format::encode_record(&mut self.buf, &fields, &[], Payload::Zeros(payload));
    }

    /// Makes room for `len` more bytes in the active segment, rolling to the next one
    /// when needed. The next segment is prepared before anything is written, so a
    /// `NoSpace` here leaves the records that did not fit unwritten.
    fn ensure_room(&mut self, len: u64) -> Result<(), Fail> {
        if self.at() + len <= self.active.len {
            return Ok(());
        }
        let next = match self.spare.take() {
            Some(spare) => spare,
            None => self.prepare_spare().map_err(|e| match e {
                Error::NoSpace => Fail::NoSpace,
                other => Fail::Fatal(other),
            })?,
        };
        if self.active.len - self.at() >= MIN_PAD {
            self.push_pad(FLAG_ROLL, self.next_write_seq, 0, 0);
        }
        if let Err(fail) = self.flush_buf() {
            self.spare = Some(next);
            return Err(fail);
        }
        if self.active_dirty {
            self.prev_unsynced.push(self.active.file.clone());
            self.active_dirty = false;
        }
        let number = self.shared.next_number.fetch_add(1, Ordering::Relaxed);
        let fs = &*self.shared.fs;
        let new = segment::activate(fs, &self.shared.dir, next, number, self.next_write_seq)?;
        {
            let mut table = lock(&self.shared.table);
            if let Some(old) = table.live.get_mut(&self.active.number) {
                old.last_seq = Some(self.next_write_seq - 1);
            }
            table.live.insert(number, TableSeg::from_live(&new, None));
            table.active = number;
        }
        self.active = new;
        self.pos = BLOCK;
        Ok(())
    }

    fn prepare_spare(&mut self) -> Result<FreeSeg> {
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
        )
    }

    /// Prepares the next segment ahead of the roll once the active one is half used,
    /// after the group's waiters completed. A failure is retried a second later.
    fn maybe_prepare_spare(&mut self) {
        if self.spare.is_some() || self.latched() || self.at() < self.active.len / 2 {
            return;
        }
        if self
            .spare_failed_at
            .is_some_and(|t| t.elapsed() < Duration::from_secs(1))
        {
            return;
        }
        match self.prepare_spare() {
            Ok(spare) => {
                self.spare = Some(spare);
                self.spare_failed_at = None;
            }
            Err(_) => self.spare_failed_at = Some(Instant::now()),
        }
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

    /// Pads to the next 4 KiB boundary (or the segment end), writes the buffer and syncs
    /// every segment with unsynced bytes. Afterwards no later write touches a synced byte.
    fn sync_aligned(&mut self, extra_flags: u8) -> Result<(), Fail> {
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
        self.flush_buf()?;
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
            waiter.complete(result);
            drop(batch);
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
