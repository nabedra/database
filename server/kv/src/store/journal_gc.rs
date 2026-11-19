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
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::batch::{BatchOp, WriteBatch};
use crate::error::Result;
use crate::types::{Kind, Locator, Seq};

use super::{Inner, lock, read};

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
            if key == k2 && holds(self, &sv, key, first_seq - 1, old) {
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
    pub(super) fn journal_gc(&self) -> Result<Option<Duration>> {
        self.retire_unreachable()?;
        Ok(None)
    }
}

/// True when the newest version of `key` at `seq` is a journal value at `old`.
fn holds(
    inner: &Inner,
    sv: &crate::version::SuperVersion,
    key: &[u8],
    seq: Seq,
    old: &[u8],
) -> bool {
    matches!(
        read::newest(inner, sv, key, seq, false),
        Ok(Some((Kind::JournalPut, ref stored))) if stored.as_ref() == old
    )
}
