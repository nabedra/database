//! Range tombstones fragmented into sorted, non-overlapping pieces.
//!
//! Any set of `[start, end)` tombstones splits at every start and end into fragments that
//! do not overlap. Each fragment keeps the sequences of the tombstones covering it,
//! newest first, so "the newest tombstone covering user key `k` visible at snapshot `s`"
//! is two binary searches. The memtable caches one set over its own tombstones; a
//! [`RangeDelAggregator`] builds one over every source of a read, limited to the
//! snapshot.

use std::sync::Arc;

use bytes::Bytes;

use crate::source::RangeTombstone;
use crate::types::Seq;

#[derive(Clone, Debug)]
struct Fragment {
    start: Bytes,
    end: Bytes,
    /// Range into `FragmentedRangeTombstones::seqs`, sorted descending.
    seqs_from: u32,
    seqs_to: u32,
}

/// Non-overlapping tombstone fragments sorted by start key.
#[derive(Clone, Debug, Default)]
pub struct FragmentedRangeTombstones {
    fragments: Vec<Fragment>,
    seqs: Vec<Seq>,
}

impl FragmentedRangeTombstones {
    pub fn empty() -> FragmentedRangeTombstones {
        FragmentedRangeTombstones::default()
    }

    /// Fragments every tombstone with `seq <= max_seq`. Empty and inverted ranges are
    /// ignored (they delete nothing). Costs O(n log n) plus the output size.
    pub fn new<'a, I>(tombstones: I, max_seq: Seq) -> FragmentedRangeTombstones
    where
        I: IntoIterator<Item = &'a RangeTombstone>,
    {
        FragmentedRangeTombstones::build(tombstones, max_seq, None)
    }

    /// Like [`FragmentedRangeTombstones::new`], but keeps per fragment only the newest
    /// sequence in each stripe between consecutive live snapshots (`snapshots` ascending,
    /// as [`crate::memtable::SnapshotList::sequences`] returns). This is what flush and
    /// compaction should write: an older tombstone in the same stripe is hidden by the
    /// newer one for every reader, provided every reader below the newest sequence holds
    /// a registered snapshot (true for immutable sources; never use it for the active
    /// memtable). It bounds the output to fragments × (snapshots + 1).
    pub fn collapsed<'a, I>(
        tombstones: I,
        max_seq: Seq,
        snapshots: &[Seq],
    ) -> FragmentedRangeTombstones
    where
        I: IntoIterator<Item = &'a RangeTombstone>,
    {
        FragmentedRangeTombstones::build(tombstones, max_seq, Some(snapshots))
    }

    fn build<'a, I>(
        tombstones: I,
        max_seq: Seq,
        snapshots: Option<&[Seq]>,
    ) -> FragmentedRangeTombstones
    where
        I: IntoIterator<Item = &'a RangeTombstone>,
    {
        let mut input: Vec<&RangeTombstone> = tombstones
            .into_iter()
            .filter(|t| t.seq <= max_seq && t.start < t.end)
            .collect();
        if input.is_empty() {
            return FragmentedRangeTombstones::empty();
        }
        input.sort_by(|a, b| a.start.cmp(&b.start));
        let mut bounds: Vec<&Bytes> = input.iter().flat_map(|t| [&t.start, &t.end]).collect();
        bounds.sort_unstable();
        bounds.dedup();

        let mut out = FragmentedRangeTombstones::default();
        // Tombstones covering the current fragment: (end, seq).
        let mut active: Vec<(&Bytes, Seq)> = Vec::new();
        let mut scratch: Vec<Seq> = Vec::new();
        let mut next_input = 0;
        for pair in bounds.windows(2) {
            let (from, to) = (pair[0], pair[1]);
            while next_input < input.len() && input[next_input].start <= *from {
                let t = input[next_input];
                active.push((&t.end, t.seq));
                next_input += 1;
            }
            active.retain(|(end, _)| *end > from);
            if active.is_empty() {
                continue;
            }
            scratch.clear();
            scratch.extend(active.iter().map(|&(_, seq)| seq));
            scratch.sort_unstable_by(|a, b| b.cmp(a));
            scratch.dedup();
            if let Some(snapshots) = snapshots {
                let mut last_stripe = None;
                scratch.retain(|&seq| {
                    let stripe = snapshots.partition_point(|&s| s < seq);
                    let keep = last_stripe != Some(stripe);
                    last_stripe = Some(stripe);
                    keep
                });
            }
            out.push(from, to, &scratch);
        }
        out
    }

    fn push(&mut self, start: &Bytes, end: &Bytes, seqs: &[Seq]) {
        if let Some(last) = self.fragments.last_mut()
            && last.end == start
            && self.seqs[last.seqs_from as usize..last.seqs_to as usize] == *seqs
        {
            last.end = end.clone();
            return;
        }
        let seqs_from = self.seqs.len() as u32;
        self.seqs.extend_from_slice(seqs);
        self.fragments.push(Fragment {
            start: start.clone(),
            end: end.clone(),
            seqs_from,
            seqs_to: self.seqs.len() as u32,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.fragments.is_empty()
    }

    /// Number of fragments.
    pub fn len(&self) -> usize {
        self.fragments.len()
    }

    /// Index of the fragment containing `user_key`.
    fn find(&self, user_key: &[u8]) -> Option<usize> {
        let after = self
            .fragments
            .partition_point(|f| f.start.as_ref() <= user_key);
        let i = after.checked_sub(1)?;
        (user_key < self.fragments[i].end.as_ref()).then_some(i)
    }

    fn newest_at_or_below(&self, fragment: usize, snapshot: Seq) -> Option<Seq> {
        let f = &self.fragments[fragment];
        let seqs = &self.seqs[f.seqs_from as usize..f.seqs_to as usize];
        let i = seqs.partition_point(|&s| s > snapshot);
        seqs.get(i).copied()
    }

    fn fragment_contains(&self, fragment: usize, user_key: &[u8]) -> bool {
        let f = &self.fragments[fragment];
        f.start.as_ref() <= user_key && user_key < f.end.as_ref()
    }

    /// The newest tombstone sequence `<= snapshot` covering `user_key`.
    pub fn max_covering_seq(&self, user_key: &[u8], snapshot: Seq) -> Option<Seq> {
        if self.fragments.is_empty() {
            return None;
        }
        self.newest_at_or_below(self.find(user_key)?, snapshot)
    }

    /// True when a tombstone visible at `snapshot` hides a version of `user_key`
    /// written at `seq`.
    pub fn covers(&self, user_key: &[u8], seq: Seq, snapshot: Seq) -> bool {
        self.max_covering_seq(user_key, snapshot)
            .is_some_and(|t| t > seq)
    }

    /// The fragments as tombstones, sorted by start key and then by sequence descending:
    /// the form a sorted run stores in its range-delete block.
    pub fn tombstones(&self) -> impl Iterator<Item = RangeTombstone> + '_ {
        self.fragments.iter().flat_map(|f| {
            self.seqs[f.seqs_from as usize..f.seqs_to as usize]
                .iter()
                .map(|&seq| RangeTombstone {
                    start: f.start.clone(),
                    end: f.end.clone(),
                    seq,
                })
        })
    }
}

/// The range tombstones of every source of one read, at one snapshot.
///
/// Each source contributes its own cached [`FragmentedRangeTombstones`] (the memtable
/// keeps one, a run reader builds one per run), so creating an iterator costs one `Arc`
/// clone per source that has tombstones, never a re-fragmentation. A query asks every
/// such source: O(sources × log fragments), and O(sources) while an iterator moves
/// monotonically, because each source keeps a cursor on the fragment it used last.
#[derive(Clone, Debug)]
pub struct RangeDelAggregator {
    snapshot: Seq,
    sources: Vec<(Arc<FragmentedRangeTombstones>, Option<usize>)>,
}

impl RangeDelAggregator {
    pub fn new(snapshot: Seq) -> RangeDelAggregator {
        RangeDelAggregator {
            snapshot,
            sources: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> Seq {
        self.snapshot
    }

    /// Adds one source's fragmented tombstones. Empty sets are skipped.
    pub fn add(&mut self, tombstones: Arc<FragmentedRangeTombstones>) {
        if !tombstones.is_empty() {
            self.sources.push((tombstones, None));
        }
    }

    /// Fragments loose tombstones (those visible at the snapshot) into one more source.
    pub fn add_tombstones<'a, I: IntoIterator<Item = &'a RangeTombstone>>(
        &mut self,
        tombstones: I,
    ) {
        self.add(Arc::new(FragmentedRangeTombstones::new(
            tombstones,
            self.snapshot,
        )));
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// The newest tombstone visible at the snapshot covering `user_key`, over all sources.
    pub fn max_covering_seq(&self, user_key: &[u8]) -> Option<Seq> {
        self.sources
            .iter()
            .filter_map(|(set, _)| set.max_covering_seq(user_key, self.snapshot))
            .max()
    }

    /// True when a visible tombstone hides the version of `user_key` written at `seq`.
    /// Takes `&mut self` to reuse each source's last fragment for nearby keys.
    pub fn covers(&mut self, user_key: &[u8], seq: Seq) -> bool {
        let snapshot = self.snapshot;
        self.sources.iter_mut().any(|(set, last)| {
            let hit = match *last {
                Some(i) if set.fragment_contains(i, user_key) => Some(i),
                _ => {
                    let found = set.find(user_key);
                    if found.is_some() {
                        *last = found;
                    }
                    found
                }
            };
            hit.and_then(|i| set.newest_at_or_below(i, snapshot))
                .is_some_and(|t| t > seq)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iter::testutil::Rng;

    fn t(start: &[u8], end: &[u8], seq: Seq) -> RangeTombstone {
        RangeTombstone {
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            seq,
        }
    }

    #[test]
    fn ns21_fragments_overlapping_and_nested_tombstones() {
        let input = [
            t(b"a", b"e", 5),
            t(b"c", b"g", 7),
            t(b"d", b"e", 9),
            t(b"x", b"x", 50), // empty
            t(b"z", b"y", 50), // inverted
            t(b"m", b"n", 100),
        ];
        let f = FragmentedRangeTombstones::new(&input, 99);
        let got: Vec<_> = f
            .tombstones()
            .map(|t| (t.start.to_vec(), t.end.to_vec(), t.seq))
            .collect();
        let want: Vec<(Vec<u8>, Vec<u8>, Seq)> = vec![
            (b"a".to_vec(), b"c".to_vec(), 5),
            (b"c".to_vec(), b"d".to_vec(), 7),
            (b"c".to_vec(), b"d".to_vec(), 5),
            (b"d".to_vec(), b"e".to_vec(), 9),
            (b"d".to_vec(), b"e".to_vec(), 7),
            (b"d".to_vec(), b"e".to_vec(), 5),
            (b"e".to_vec(), b"g".to_vec(), 7),
        ];
        assert_eq!(got, want);
        assert_eq!(f.max_covering_seq(b"d", 99), Some(9));
        assert_eq!(f.max_covering_seq(b"d", 8), Some(7));
        assert_eq!(f.max_covering_seq(b"d", 4), None);
        assert_eq!(f.max_covering_seq(b"g", 99), None);
        assert_eq!(f.max_covering_seq(b"m", 1000), None, "above max_seq");
        assert!(f.covers(b"b", 4, 99));
        assert!(!f.covers(b"b", 5, 99));
    }

    #[test]
    fn ns21_collapsed_fragments_keep_the_newest_tombstone_per_snapshot_stripe() {
        let input = [
            t(b"a", b"z", 3),
            t(b"a", b"z", 5),
            t(b"a", b"z", 8),
            t(b"a", b"z", 9),
        ];
        let all = FragmentedRangeTombstones::new(&input, 100);
        assert_eq!(all.tombstones().count(), 4);
        let none = FragmentedRangeTombstones::collapsed(&input, 100, &[]);
        assert_eq!(
            none.tombstones().map(|t| t.seq).collect::<Vec<_>>(),
            vec![9]
        );
        // Stripes (..5], (5..8], (8..): readers at 5 and 8 keep their answers.
        let striped = FragmentedRangeTombstones::collapsed(&input, 100, &[5, 8]);
        assert_eq!(
            striped.tombstones().map(|t| t.seq).collect::<Vec<_>>(),
            vec![9, 8, 5]
        );
        for snapshot in [5, 8, 100] {
            assert_eq!(
                striped.max_covering_seq(b"m", snapshot),
                all.max_covering_seq(b"m", snapshot)
            );
        }
    }

    #[test]
    fn ns21_fragment_queries_match_a_brute_force_model() {
        let mut rng = Rng::new(0x5EED_0021);
        for _ in 0..200 {
            let n = rng.below(12) as usize;
            let input: Vec<RangeTombstone> = (0..n)
                .map(|_| {
                    let a = rng.below(20) as u8;
                    let b = rng.below(20) as u8;
                    t(&[a], &[b], 1 + rng.below(30))
                })
                .collect();
            let max_seq = 1 + rng.below(30);
            let f = FragmentedRangeTombstones::new(&input, max_seq);
            let shared = Arc::new(f.clone());
            for snapshot in [0, 5, 15, 30] {
                // The same set split over two sources must answer the same.
                let mut agg = RangeDelAggregator::new(snapshot);
                let half = input.len() / 2;
                agg.add_tombstones(input[..half].iter().filter(|t| t.seq <= max_seq));
                agg.add_tombstones(input[half..].iter().filter(|t| t.seq <= max_seq));
                let mut whole = RangeDelAggregator::new(snapshot);
                whole.add(shared.clone());
                for key in 0..21u8 {
                    let want = input
                        .iter()
                        .filter(|t| {
                            t.seq <= max_seq
                                && t.seq <= snapshot
                                && t.start.as_ref() <= [key].as_slice()
                                && [key].as_slice() < t.end.as_ref()
                        })
                        .map(|t| t.seq)
                        .max();
                    assert_eq!(f.max_covering_seq(&[key], snapshot), want);
                    assert_eq!(agg.max_covering_seq(&[key]), want);
                    let seq = rng.below(30);
                    assert_eq!(agg.covers(&[key], seq), want.is_some_and(|w| w > seq));
                    assert_eq!(whole.covers(&[key], seq), want.is_some_and(|w| w > seq));
                }
            }
        }
    }
}
