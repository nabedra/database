//! Journal-value GC with relocation (plan section 2 "Journal values"; the retirement
//! rule is in `gc.rs`).
//!
//! # Relocate records
//!
//! A `Relocate` record's payload is a `WriteBatch` of pairs, one per journal value to
//! move: `put(key, old_locator)` then `put_journal(key, value)`. Pair `i` takes sequences
//! `first_seq + 2i` and `first_seq + 2i + 1`. The record's scope is the scope of its keys
//! (one record per scope), so damage to it quarantines only that scope (G1).
//!
//! The journal thread applies a pair only if the newest version of `key` at
//! `first_seq - 1`, over every memtable and run, is a `JournalPut` holding exactly
//! `old_locator`; it then inserts `JournalPut(key) -> new locator` at the pair's second
//! sequence. Otherwise the pair is skipped (the key changed since GC looked). Replay
//! applies the same check to the same state (runs hold only sequences at or below
//! `flushed_seq`, and compaction keeps the newest version of every key), so the outcome
//! is the same after a crash.

use std::collections::BTreeMap;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;

use crate::batch::{BatchOp, MAX_SCOPE_LEN, WriteBatch};
use crate::error::{Damage, DamageReason, Error, Result};
use crate::gc;
use crate::journal::format::{RECORD_HEADER_LEN, RECORDS_START};
use crate::journal::{Commit, Durability, Journal, RecordKind, VALUE_RECYCLED};
use crate::source::InternalIterator;
use crate::types::{Kind, Locator, Seq, parse_internal_key};
use crate::version::SuperVersion;

use super::{Inner, lock, read};

/// Segments one GC pass relocates at most.
const GC_SEGMENTS_PER_PASS: usize = 4;
/// Journal values per relocation record, at most (also a quarter of a segment).
const RELOCATE_BATCH_BYTES: usize = 1 << 20;

/// Upper bound on one batch operation's encoding beyond its key and value bytes: the
/// kind byte and two varint lengths.
const OP_OVERHEAD: usize = 1 + 2 * 5;
/// How soon to look again while relocations wait to become retirable.
const GC_WAIT: Duration = Duration::from_millis(20);
const GC_RETRY_AFTER_ERROR: Duration = Duration::from_secs(1);

/// Journal GC bookkeeping.
#[derive(Default)]
pub(crate) struct GcState {
    /// Segments whose live locators were relocated, with the sequence of the last
    /// relocation record (rule 2 in `gc.rs`).
    pub cleared: BTreeMap<u64, Seq>,
    /// Segments found too dense to relocate, with the journal bytes written when they
    /// were looked at. They are looked at again once the journal has written another
    /// quarter of its budget.
    pub dense: BTreeMap<u64, u64>,
    /// Relocation commits not known to have succeeded, with the segments they clear. A
    /// relocation the journal refused (`NoSpace` voids its sequences, which still become
    /// visible) moved nothing, so its segments must not retire.
    pub relocating: Vec<Relocation>,
}

pub(crate) struct Relocation {
    commits: Vec<Commit>,
    targets: Vec<u64>,
}

impl GcState {
    /// Settles finished relocation commits: a failed one un-clears its segments (the next
    /// pass scans them again). Returns the segments whose relocations are still running,
    /// which must not retire yet.
    pub fn settle_relocations(&mut self) -> Vec<u64> {
        let mut running = Vec::new();
        let mut failed = Vec::new();
        self.relocating.retain_mut(|r| {
            let mut pending = false;
            let mut ok = true;
            r.commits.retain_mut(|c| match c.try_result() {
                None => {
                    pending = true;
                    true
                }
                Some(Ok(_)) => false,
                Some(Err(_)) => {
                    ok = false;
                    false
                }
            });
            if !ok {
                failed.extend_from_slice(&r.targets);
                return false;
            }
            if pending {
                running.extend_from_slice(&r.targets);
            }
            pending
        });
        for segment in failed {
            self.cleared.remove(&segment);
        }
        running
    }
}

impl Inner {
    /// Applies a `Relocate` record on the journal thread (or during replay).
    pub(super) fn apply_relocation(
        &self,
        first_seq: Seq,
        batch: &WriteBatch,
        locators: &[Locator],
        replaying: bool,
    ) -> Result<()> {
        let sv = self.current_sv();
        let mut ops = batch.iter();
        let mut next_locator = locators.iter();
        let mut seq = first_seq;
        let mut accepted: Vec<(Seq, &[u8], Locator)> = Vec::new();
        let mut relocated = 0u64;
        while let Some(first) = ops.next() {
            let (BatchOp::Put { key, value: old }, Some(BatchOp::JournalPut { key: k2, .. })) =
                (first, ops.next())
            else {
                return Err(crate::error::Error::Corruption {
                    file: crate::error::FileId::Other,
                    offset: 0,
                    what: "malformed journal relocation record",
                });
            };
            let new = *next_locator.next().ok_or(crate::error::Error::Corruption {
                file: crate::error::FileId::Other,
                offset: 0,
                what: "journal relocation record without its locators",
            })?;
            if key == k2 && holds(self, &sv, key, first_seq - 1, old)? {
                accepted.push((seq + 1, key, new));
                relocated += u64::from(new.len);
            }
            seq += 2;
        }
        let _active = lock(&self.active);
        let mem = self.current_sv().mem.clone();
        let mut one = WriteBatch::new();
        for (at, key, locator) in &accepted {
            one.clear();
            // The memtable keeps the locator, not the value, so the value is not copied.
            one.put_journal(key, b"");
            mem.mem.insert(&one, *at, std::slice::from_ref(locator))?;
            mem.note_segment(locator.segment);
        }
        if batch.count() > 0 {
            mem.last_seq
                .store(first_seq + u64::from(batch.count()) - 1, Ordering::Release);
        }
        mem.journal_bytes
            .fetch_add(batch.encoded_bytes() as u64, Ordering::Relaxed);
        if !replaying {
            self.counters
                .relocated_bytes
                .fetch_add(relocated, Ordering::Relaxed);
        }
        if self.should_switch(&mem) {
            self.switch_locked(&mem);
            if !replaying {
                self.schedule_flush();
            }
        }
        Ok(())
    }

    /// One journal GC step, run on a compaction thread. Retires what the rule allows,
    /// then, while the journal is over budget, relocates the live journal values of its
    /// oldest segments. Returns when to run again, if there is more to do.
    ///
    /// Never waits for a relocation commit: the journal thread may be stalled on L0,
    /// which needs this thread. The next step retires once the relocations are visible
    /// and durable (syncing the journal if needed) and no snapshot predates them.
    pub(super) fn journal_gc(&self) -> Result<Option<Duration>> {
        let _pass = lock(&self.gc_pass);
        if self.latched.load(Ordering::Acquire) || self.journal().requires_recovery() {
            return Err(Error::RecoveryRequired);
        }
        self.retire_unreachable()?;
        let pending: Option<Seq> = lock(&self.gc).cleared.values().copied().max();
        if let Some(relocated) = pending {
            let journal = self.journal();
            if journal.visible_seq() >= relocated && journal.durable_seq() < relocated {
                self.sync_journal()?;
                self.retire_unreachable()?;
            }
            return Ok((!lock(&self.gc).cleared.is_empty()).then_some(GC_WAIT));
        }
        if !self.journal_over_budget() {
            return Ok(None);
        }
        self.relocation_pass()
    }

    /// Scans for live locators into the oldest segments and relocates them.
    fn relocation_pass(&self) -> Result<Option<Duration>> {
        let Some(me) = self.self_ref.get().and_then(Weak::upgrade) else {
            return Ok(None);
        };
        let journal = self.journal().clone();
        let segments = journal.segments();
        let flushed = lock(&self.vs).state.flushed_seq;
        let written = journal.stats().journal_bytes;
        let requery = (self.max_journal_bytes / 4).max(self.segment_bytes);
        let dense: Vec<u64> = lock(&self.gc)
            .dense
            .iter()
            .filter(|&(_, &at)| written < at.saturating_add(requery))
            .map(|(&s, _)| s)
            .collect();
        let mut targets = gc::gc_candidates(
            &segments,
            self.segment_bytes,
            self.max_journal_bytes,
            flushed,
            journal.durable_seq(),
            &|s| dense.contains(&s),
            GC_SEGMENTS_PER_PASS,
        );
        if targets.is_empty() {
            return Ok(None);
        }

        // Every locator into a target that is visible at the scan snapshot.
        let snap = me.snapshot();
        let scan_seq = snap.seq();
        let sv = snap.super_version().clone();
        let live = match self.live_locators(&sv, scan_seq, &targets) {
            Ok(live) => live,
            // A damaged or unreadable run: reads and compaction report it; try later.
            Err(_) => return Ok(Some(GC_RETRY_AFTER_ERROR)),
        };
        drop(sv);
        drop(snap);

        // Dense segments are not worth moving.
        let mut live_bytes: BTreeMap<u64, u64> = BTreeMap::new();
        for (_, loc) in &live {
            *live_bytes.entry(loc.segment).or_default() += u64::from(loc.len);
        }
        {
            let mut gc = lock(&self.gc);
            targets.retain(|s| {
                let dense =
                    gc::too_dense(live_bytes.get(s).copied().unwrap_or(0), self.segment_bytes);
                if dense {
                    gc.dense.insert(*s, written);
                }
                !dense
            });
        }
        if targets.is_empty() {
            return Ok(None);
        }
        let live: Vec<(Bytes, Locator)> = live
            .into_iter()
            .filter(|(_, loc)| targets.contains(&loc.segment))
            .collect();

        let commits = self.relocate(&journal, live)?;
        let relocated = commits
            .iter()
            .map(Commit::seq)
            .max()
            .unwrap_or(scan_seq)
            .max(scan_seq);
        let mut gc = lock(&self.gc);
        for &s in &targets {
            gc.cleared.insert(s, relocated);
        }
        if !commits.is_empty() {
            gc.relocating.push(Relocation { commits, targets });
        }
        Ok(Some(GC_WAIT))
    }

    /// `(user key, locator)` for every key whose newest version at `seq` is a journal
    /// value in one of `targets`, sorted by scope, segment and offset.
    fn live_locators(
        &self,
        sv: &SuperVersion,
        seq: Seq,
        targets: &[u64],
    ) -> Result<Vec<(Bytes, Locator)>> {
        let max_target = targets.iter().copied().max().unwrap_or(0);
        let mut found: Vec<(Bytes, Locator)> = Vec::new();
        let mut collect = |it: &mut dyn InternalIterator| -> Result<()> {
            it.seek_to_first();
            while it.valid() {
                if let Some(parsed) = parse_internal_key(it.key())
                    && parsed.kind == Kind::JournalPut
                    && let Some(loc) = Locator::decode(&it.value())
                    && targets.contains(&loc.segment)
                {
                    found.push((Bytes::copy_from_slice(parsed.user_key), loc));
                }
                it.next();
            }
            it.status()
        };
        // Memtables hold locators into flushed segments only through relocation records
        // of an earlier pass that are already flushed, so this rarely visits one.
        for mem in sv.memtables() {
            if mem.oldest_segment() <= max_target {
                collect(&mut mem.mem.iter())?;
            }
        }
        for file in sv.version.runs() {
            if file
                .desc
                .oldest_journal_segment
                .is_some_and(|s| s <= max_target)
            {
                let run = match file.pinned() {
                    Some(run) => run.clone(),
                    None => self.table_cache.get(file.id())?,
                };
                collect(&mut run.iter(false))?;
            }
        }
        found.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.offset.cmp(&b.1.offset)));
        found.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        // Keep what the key still holds at `seq`; a key in a quarantined scope is dead
        // (its reads fail until `release_scope` deletes it). A key that cannot be read
        // (EIO, a damaged block not yet quarantined) fails the pass, which is retried:
        // KVfix3, NS10 seed 510 lost a value whose liveness check met an injected EIO.
        let mut held = Vec::with_capacity(found.len());
        for (key, loc) in found {
            if self.quarantine.check(&key).is_ok() && holds(self, sv, &key, seq, &loc.encode())? {
                held.push((key, loc));
            }
        }
        let mut found = held;
        found.sort_by(|a, b| {
            self.scope_bytes(&a.0)
                .cmp(&self.scope_bytes(&b.0))
                .then(a.1.segment.cmp(&b.1.segment))
                .then(a.1.offset.cmp(&b.1.offset))
        });
        Ok(found)
    }

    /// Submits `Relocate` records (one scope each, bounded size) for `live`. Returns their
    /// commits, not awaited (the journal thread may need this thread, see `journal_gc`):
    /// [`GcState::settle_relocations`] checks them before anything retires.
    fn relocate(&self, journal: &Journal, live: Vec<(Bytes, Locator)>) -> Result<Vec<Commit>> {
        // The payload one record can hold: a segment's record area less a record header
        // and the longest scope. Batches are cut before an entry that would not fit, not
        // after the size passed a target (KVfix3: 12 KiB segments have a 4 KiB record
        // area, and a 3 KiB target plus one value went past it).
        let capacity = (self.segment_bytes.saturating_sub(RECORDS_START) as usize)
            .saturating_sub(RECORD_HEADER_LEN + MAX_SCOPE_LEN);
        let limit = RELOCATE_BATCH_BYTES.min(capacity / 2);
        let mut commits = Vec::new();
        let mut batch = WriteBatch::new();
        let mut scope = Bytes::new();
        let mut submit = |batch: &mut WriteBatch| -> Result<()> {
            if batch.is_empty() {
                return Ok(());
            }
            let full = std::mem::take(batch);
            let commit = journal.submit_kind(
                RecordKind::Relocate,
                full,
                Durability::NoSync,
                Box::new(()),
            )?;
            commits.push(commit);
            Ok(())
        };
        for (key, loc) in live {
            let key_scope = self.scope_bytes(&key);
            if key_scope != scope || batch.encoded_bytes() >= limit {
                submit(&mut batch)?;
                scope = key_scope.clone();
                batch.set_scope(&scope);
            }
            let value = match journal.read_value_with(&loc, false) {
                Ok(value) => value,
                Err(e @ Error::Corruption { what, .. }) if what == VALUE_RECYCLED => {
                    // A live locator into a recycled segment: GC broke its own rule.
                    self.recycled_value();
                    return Err(e);
                }
                Err(Error::Corruption { file, .. }) => {
                    // G3: the value is damaged. Its scope is quarantined (store-wide
                    // latches), after which the key is dead for GC.
                    self.report_damage(Damage {
                        scope: key_scope.clone(),
                        reason: DamageReason::JournalValue,
                        file,
                        first_bad_seq: None,
                    });
                    if key_scope.is_empty() {
                        return Err(Error::RecoveryRequired);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            // Two operations: the old locator and the value. Each costs its key, its
            // payload and at most `OP_OVERHEAD` of kind and lengths.
            let entry = 2 * (key.len() + OP_OVERHEAD) + Locator::LEN + value.len();
            if !batch.is_empty() && batch.encoded_bytes() + entry > capacity {
                submit(&mut batch)?;
                batch.set_scope(&scope);
            }
            batch.put(&key, &loc.encode());
            batch.put_journal(&key, &value);
        }
        submit(&mut batch)?;
        Ok(commits)
    }
}

/// True when the newest version of `key` at `seq` is a journal value at `old`. A read
/// error is returned, never taken for "no longer held": a live value judged dead is not
/// relocated, and its segment is then recycled under its locator.
fn holds(inner: &Inner, sv: &SuperVersion, key: &[u8], seq: Seq, old: &[u8]) -> Result<bool> {
    Ok(matches!(
        read::newest(inner, sv, key, seq, false)?,
        Some((Kind::JournalPut, ref stored)) if stored.as_ref() == old
    ))
}
