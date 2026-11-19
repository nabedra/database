//! Journal segment retirement and journal-value GC planning (task NS5, plan section 2
//! "Journal values").
//!
//! A segment can return to the journal's free pool once nothing needs it:
//! - it is not the active segment and every record in it is durable;
//! - `flushed_seq` covers every record in it, so replay never needs it;
//! - no locator that any reader can still see points into it. There are two ways to
//!   know that:
//!   1. **Unreferenced.** Every live run and memtable, including those pinned by
//!      snapshots, registers the lowest segment its locators point into
//!      (`FileTracker`). A segment below that minimum holds nothing anyone can read.
//!   2. **Cleared by relocation.** Journal GC found every locator into the segment that
//!      was visible at its scan and relocated it with `Relocate` records, the last of
//!      which takes sequence `R`. Once `R` is visible and durable and every live
//!      snapshot is at or above `R`, no reader can reach the old locators: the current
//!      state never points into the segment again (new writes go to the active segment,
//!      and a relocation applies only while the key still holds the old locator), and
//!      every snapshot that could still see an old locator is gone. The old locators
//!      stay in runs, shadowed, until compaction drops them.
//!
//! Retired segments need not form a prefix: journal recovery starts at the last segment
//! whose first sequence is at or below `flushed_seq + 1` and never reads older ones.

use std::collections::BTreeMap;

use crate::journal::SegmentInfo;
use crate::types::Seq;

/// What the retirement rule looks at, read by the caller at one point in time.
#[derive(Clone, Debug, Default)]
pub(crate) struct RetireState<'a> {
    pub flushed_seq: Seq,
    pub durable_seq: Seq,
    pub visible_seq: Seq,
    /// Lowest segment any live run or memtable points into (rule 1).
    pub min_referenced: Option<u64>,
    /// Oldest live snapshot (rule 2).
    pub oldest_snapshot: Option<Seq>,
    /// Segments cleared by relocation, with the last relocation's sequence (rule 2).
    pub cleared: Option<&'a BTreeMap<u64, Seq>>,
}

impl RetireState<'_> {
    /// True when `segment`'s journal values can no longer be read by anyone.
    fn unreachable(&self, segment: u64) -> bool {
        if self.min_referenced.is_none_or(|m| segment < m) {
            return true;
        }
        let Some(&relocated) = self.cleared.and_then(|c| c.get(&segment)) else {
            return false;
        };
        relocated <= self.visible_seq
            && relocated <= self.durable_seq
            && self.oldest_snapshot.is_none_or(|s| s >= relocated)
    }
}

/// The segments of `segments` (sorted by number) that may retire.
pub(crate) fn retirable(segments: &[SegmentInfo], state: &RetireState<'_>) -> Vec<SegmentInfo> {
    segments
        .iter()
        .filter(|s| {
            !s.active
                && s.last_seq <= state.flushed_seq
                && s.last_seq <= state.durable_seq
                && state.unreachable(s.segment)
        })
        .cloned()
        .collect()
}

/// Journal GC picks the segments to relocate while the journal is over its budget:
/// oldest first, only segments whose state is flushed and durable (so replay never needs
/// them), never the active one, skipping `skip` (segments found too dense to be worth
/// relocating), and no more than `limit`. Returns nothing while the journal fits.
#[allow(dead_code)] // used by journal GC (next commit)
pub(crate) fn gc_candidates(
    segments: &[SegmentInfo],
    segment_bytes: u64,
    max_journal_bytes: u64,
    flushed_seq: Seq,
    durable_seq: Seq,
    skip: &dyn Fn(u64) -> bool,
    limit: usize,
) -> Vec<u64> {
    let total = segments.len() as u64 * segment_bytes;
    if total <= max_journal_bytes {
        return Vec::new();
    }
    let over = total - max_journal_bytes;
    let need = over.div_ceil(segment_bytes.max(1)) as usize;
    segments
        .iter()
        .filter(|s| {
            !s.active && s.last_seq <= flushed_seq && s.last_seq <= durable_seq && !skip(s.segment)
        })
        .map(|s| s.segment)
        .take(need.min(limit))
        .collect()
}

/// A segment whose live journal values fill more than this share of it is not relocated:
/// moving it would free little and, when the live set itself exceeds the budget, GC
/// would rewrite the same values forever.
#[allow(dead_code)] // used by journal GC (next commit)
pub(crate) fn too_dense(live_bytes: u64, segment_bytes: u64) -> bool {
    live_bytes.saturating_mul(4) > segment_bytes.saturating_mul(3)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(segment: u64, first_seq: Seq, last_seq: Seq, active: bool) -> SegmentInfo {
        SegmentInfo {
            segment,
            epoch: 1,
            first_seq,
            last_seq,
            active,
        }
    }

    fn ids(v: Vec<SegmentInfo>) -> Vec<u64> {
        v.into_iter().map(|s| s.segment).collect()
    }

    fn state(flushed: Seq, durable: Seq, min_referenced: Option<u64>) -> RetireState<'static> {
        RetireState {
            flushed_seq: flushed,
            durable_seq: durable,
            visible_seq: durable,
            min_referenced,
            oldest_snapshot: None,
            cleared: None,
        }
    }

    #[test]
    fn ns_unit_retire_only_flushed_unreferenced_segments() {
        let segments = [
            seg(1, 1, 10, false),
            seg(2, 11, 20, false),
            seg(3, 21, 30, true),
        ];
        assert_eq!(ids(retirable(&segments, &state(30, 30, None))), vec![1, 2]);
        assert_eq!(ids(retirable(&segments, &state(15, 30, None))), vec![1]);
        assert_eq!(ids(retirable(&segments, &state(30, 30, Some(2)))), vec![1]);
        assert!(retirable(&segments, &state(30, 30, Some(1))).is_empty());
        assert!(retirable(&segments, &state(30, 5, None)).is_empty());
    }

    #[test]
    fn ns13_unit_cleared_segments_retire_once_relocation_is_visible_durable_and_unsnapshotted() {
        let segments = [
            seg(1, 1, 10, false),
            seg(2, 11, 20, false),
            seg(3, 21, 30, false),
            seg(4, 31, 60, true),
        ];
        // Run locators still point into segment 1, so rule 1 retires nothing.
        let cleared: BTreeMap<u64, Seq> = [(1, 45), (2, 45)].into();
        let mut s = state(30, 60, Some(1));
        s.visible_seq = 60;
        s.cleared = Some(&cleared);
        // 3 is not cleared and still referenced: a gap is fine.
        assert_eq!(ids(retirable(&segments, &s)), vec![1, 2]);
        // A snapshot below the relocation still sees the old locators.
        s.oldest_snapshot = Some(44);
        assert!(retirable(&segments, &s).is_empty());
        s.oldest_snapshot = Some(45);
        assert_eq!(ids(retirable(&segments, &s)), vec![1, 2]);
        // The relocation must be visible and durable.
        s.durable_seq = 44;
        assert!(retirable(&segments, &s).is_empty());
        s.durable_seq = 60;
        s.visible_seq = 44;
        assert!(retirable(&segments, &s).is_empty());
    }

    #[test]
    fn ns13_unit_gc_picks_oldest_flushed_segments_while_over_budget() {
        let segments = [
            seg(1, 1, 10, false),
            seg(2, 11, 20, false),
            seg(3, 21, 30, false),
            seg(4, 31, 40, false),
            seg(5, 41, 50, true),
        ];
        let none = |_: u64| false;
        // 5 segments of 10 bytes, budget 50: fits.
        assert!(gc_candidates(&segments, 10, 50, 50, 50, &none, 8).is_empty());
        // Budget 25: 25 bytes over, so three segments.
        assert_eq!(
            gc_candidates(&segments, 10, 25, 50, 50, &none, 8),
            vec![1, 2, 3]
        );
        assert_eq!(
            gc_candidates(&segments, 10, 25, 50, 50, &none, 2),
            vec![1, 2]
        );
        // Only flushed and durable segments.
        assert_eq!(
            gc_candidates(&segments, 10, 25, 25, 50, &none, 8),
            vec![1, 2]
        );
        assert_eq!(gc_candidates(&segments, 10, 25, 50, 15, &none, 8), vec![1]);
        // Dense segments are skipped.
        let dense = |s: u64| s == 1;
        assert_eq!(
            gc_candidates(&segments, 10, 25, 50, 50, &dense, 8),
            vec![2, 3, 4]
        );
        assert!(too_dense(76, 100));
        assert!(!too_dense(75, 100));
    }
}
