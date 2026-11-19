//! Journal segment retirement (task NS5, plan section 2 "Journal values").
//!
//! A segment can return to the journal's free pool once nothing needs it:
//! - it is not the active segment and every record in it is durable;
//! - `flushed_seq` covers every record in it, so replay never needs it;
//! - no locator that any reader can still see points into it. Every live run and
//!   memtable, including those pinned by snapshots, registers the lowest segment its
//!   locators point into (`FileTracker`), and only segments below that minimum retire.
//!
//! Segments retire oldest first and the walk stops at the first one that must stay, so
//! the live segments always form a suffix of the log.
//!
//! Relocating live journal values out of old segments (phase B of NS5) is not
//! implemented yet: a group that never purges pins its oldest segment.

use crate::journal::SegmentInfo;
use crate::types::Seq;

/// The prefix of `segments` (sorted by number) that may retire.
pub(crate) fn retirable(
    segments: &[SegmentInfo],
    flushed_seq: Seq,
    durable_seq: Seq,
    min_referenced: Option<u64>,
) -> Vec<SegmentInfo> {
    segments
        .iter()
        .take_while(|s| {
            !s.active
                && s.last_seq <= flushed_seq
                && s.last_seq <= durable_seq
                && min_referenced.is_none_or(|m| s.segment < m)
        })
        .cloned()
        .collect()
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

    #[test]
    fn ns_unit_retire_only_flushed_unreferenced_prefix() {
        let segments = [
            seg(1, 1, 10, false),
            seg(2, 11, 20, false),
            seg(3, 21, 30, true),
        ];
        let ids = |v: Vec<SegmentInfo>| v.into_iter().map(|s| s.segment).collect::<Vec<_>>();
        assert_eq!(ids(retirable(&segments, 30, 30, None)), vec![1, 2]);
        assert_eq!(ids(retirable(&segments, 15, 30, None)), vec![1]);
        assert_eq!(ids(retirable(&segments, 30, 30, Some(2))), vec![1]);
        assert_eq!(
            ids(retirable(&segments, 30, 30, Some(1))),
            Vec::<u64>::new()
        );
        assert_eq!(ids(retirable(&segments, 30, 5, None)), Vec::<u64>::new());
    }
}
