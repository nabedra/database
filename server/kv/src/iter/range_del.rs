//! Range tombstones fragmented into sorted, non-overlapping pieces.
//!
//! Any set of `[start, end)` tombstones splits at every start and end into fragments that
//! do not overlap. Each fragment keeps the sequences of the tombstones covering it,
//! newest first, so "the newest tombstone covering user key `k` visible at snapshot `s`"
//! is two binary searches. The memtable keeps one set over its own tombstones and adds
//! each new range delete to it in place ([`FragmentedRangeTombstones::insert`]); a
//! [`RangeDelAggregator`] combines the sets of every source of a read, limited to the
//! snapshot.
//!
//! Fragments are stored in chunks of at most [`CHUNK`] fragments, each behind an `Arc`.
//! Cloning a set clones the chunk handles only, and an insert rewrites only the chunks
//! its range touches, so a reader holding an older clone keeps a consistent, unchanged
//! view while the writer moves on. Fragments are kept in canonical form: adjacent
//! fragments that touch and have the same sequences are merged. With distinct sequences
//! (always true in a memtable) an incrementally built set is therefore identical,
//! fragment for fragment, to one built in a single pass over the same tombstones.

use std::sync::Arc;

use bytes::Bytes;

use crate::source::RangeTombstone;
use crate::types::Seq;

/// Fragments per chunk. An insert rewrites one or two chunks (more only for a range that
/// spans them), and a clone copies `fragments / CHUNK` handles.
const CHUNK: usize = 64;

#[derive(Clone, Debug)]
struct Fragment {
    start: Bytes,
    end: Bytes,
    /// Range into the chunk's `seqs`, sorted descending.
    seqs_from: u32,
    seqs_to: u32,
}

/// Up to [`CHUNK`] consecutive fragments (never empty inside a set) and their sequences.
#[derive(Clone, Debug, Default)]
struct Chunk {
    fragments: Vec<Fragment>,
    seqs: Vec<Seq>,
}

impl Chunk {
    fn seqs_of(&self, f: &Fragment) -> &[Seq] {
        &self.seqs[f.seqs_from as usize..f.seqs_to as usize]
    }

    fn first_start(&self) -> &[u8] {
        &self.fragments[0].start
    }

    fn last_end(&self) -> &[u8] {
        &self.fragments[self.fragments.len() - 1].end
    }

    /// Appends a fragment, merging it into the last one when they touch and carry the
    /// same sequences.
    fn push(&mut self, start: &Bytes, end: &Bytes, seqs: &[Seq]) {
        if let Some(last) = self.fragments.last_mut()
            && last.end == start
            && self.seqs[last.seqs_from as usize..last.seqs_to as usize] == *seqs
        {
            last.end = end.clone();
            return;
        }
        self.push_unmerged(start, end, seqs);
    }

    fn push_unmerged(&mut self, start: &Bytes, end: &Bytes, seqs: &[Seq]) {
        let seqs_from = self.seqs.len() as u32;
        self.seqs.extend_from_slice(seqs);
        self.fragments.push(Fragment {
            start: start.clone(),
            end: end.clone(),
            seqs_from,
            seqs_to: self.seqs.len() as u32,
        });
    }
}

/// Where a fragment is: chunk and index within it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pos {
    chunk: u32,
    index: u32,
}

/// Non-overlapping tombstone fragments sorted by start key.
#[derive(Clone, Debug, Default)]
pub struct FragmentedRangeTombstones {
    chunks: Vec<Arc<Chunk>>,
    fragments: usize,
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

        let mut out = Builder::default();
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
        out.finish()
    }

    /// Adds one tombstone in place; empty and inverted ranges are ignored. Costs
    /// O(log n) plus the chunks the range touches (one or two for a narrow range), and
    /// one handle per chunk when a reader still holds a clone of this set (see
    /// `Arc::make_mut`). Readers holding clones are unaffected.
    ///
    /// The result answers every query exactly as a set built in one pass over all the
    /// tombstones would. When `tombstone.seq` differs from every sequence already in the
    /// set, as in a memtable, its fragments are identical too.
    pub fn insert(&mut self, tombstone: &RangeTombstone) {
        let (s, e, q) = (&tombstone.start, &tombstone.end, tombstone.seq);
        if s >= e {
            return;
        }
        let n = self.chunks.len();
        if n == 0 {
            let mut chunk = Chunk::default();
            chunk.push_unmerged(s, e, &[q]);
            self.chunks.push(Arc::new(chunk));
            self.fragments = 1;
            return;
        }
        // Chunks `a..=b` hold every fragment overlapping `[s, e)`, or the neighbours of
        // the gap it falls into.
        let lo = self.chunks.partition_point(|c| c.last_end() <= s.as_ref());
        let hi = self
            .chunks
            .partition_point(|c| c.first_start() < e.as_ref())
            .saturating_sub(1);
        let (a, b) = (lo.min(hi).min(n - 1), lo.max(hi).min(n - 1));

        let mut out = Chunk::default();
        let mut seqs: Vec<Seq> = Vec::new();
        // The start of the part of `[s, e)` not yet emitted.
        let mut pending: Option<Bytes> = Some(s.clone());
        let mut old_fragments = 0;
        for chunk in &self.chunks[a..=b] {
            old_fragments += chunk.fragments.len();
            for f in &chunk.fragments {
                let f_seqs = chunk.seqs_of(f);
                if f.end <= *s {
                    out.push(&f.start, &f.end, f_seqs);
                    continue;
                }
                if f.start >= *e {
                    if let Some(p) = pending.take() {
                        out.push(&p, e, &[q]);
                    }
                    out.push(&f.start, &f.end, f_seqs);
                    continue;
                }
                // `f` overlaps `[s, e)`.
                if f.start < *s {
                    out.push(&f.start, s, f_seqs);
                }
                let from = if f.start < *s { s } else { &f.start };
                let to = if f.end > *e { e } else { &f.end };
                if let Some(p) = pending.take()
                    && p < *from
                {
                    out.push(&p, from, &[q]);
                }
                seqs.clear();
                let at = f_seqs.partition_point(|&x| x > q);
                seqs.extend_from_slice(&f_seqs[..at]);
                if f_seqs.get(at) != Some(&q) {
                    seqs.push(q);
                }
                seqs.extend_from_slice(&f_seqs[at..]);
                out.push(from, to, &seqs);
                if to < e {
                    pending = Some(to.clone());
                }
                if f.end > *e {
                    out.push(e, &f.end, f_seqs);
                }
            }
        }
        if let Some(p) = pending {
            out.push(&p, e, &[q]);
        }
        self.fragments = self.fragments - old_fragments + out.fragments.len();
        let replacement = split_evenly(out);
        self.chunks.splice(a..=b, replacement);
    }

    pub fn is_empty(&self) -> bool {
        self.fragments == 0
    }

    /// Number of fragments.
    pub fn len(&self) -> usize {
        self.fragments
    }

    /// The fragment containing `user_key`.
    fn find(&self, user_key: &[u8]) -> Option<Pos> {
        let chunk = self
            .chunks
            .partition_point(|c| c.first_start() <= user_key)
            .checked_sub(1)?;
        let fragments = &self.chunks[chunk].fragments;
        // The chunk's first fragment starts at or before `user_key`, so this is >= 1.
        let index = fragments.partition_point(|f| f.start.as_ref() <= user_key) - 1;
        (user_key < fragments[index].end.as_ref()).then_some(Pos {
            chunk: chunk as u32,
            index: index as u32,
        })
    }

    fn newest_at_or_below(&self, pos: Pos, snapshot: Seq) -> Option<Seq> {
        let chunk = &self.chunks[pos.chunk as usize];
        let seqs = chunk.seqs_of(&chunk.fragments[pos.index as usize]);
        let i = seqs.partition_point(|&s| s > snapshot);
        seqs.get(i).copied()
    }

    fn fragment_contains(&self, pos: Pos, user_key: &[u8]) -> bool {
        let f = &self.chunks[pos.chunk as usize].fragments[pos.index as usize];
        f.start.as_ref() <= user_key && user_key < f.end.as_ref()
    }

    /// The newest tombstone sequence `<= snapshot` covering `user_key`.
    pub fn max_covering_seq(&self, user_key: &[u8], snapshot: Seq) -> Option<Seq> {
        if self.chunks.is_empty() {
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
        self.chunks.iter().flat_map(|chunk| {
            chunk.fragments.iter().flat_map(move |f| {
                chunk.seqs_of(f).iter().map(|&seq| RangeTombstone {
                    start: f.start.clone(),
                    end: f.end.clone(),
                    seq,
                })
            })
        })
    }
}

/// Builds a set fragment by fragment, in key order, filling chunks to [`CHUNK`].
#[derive(Default)]
struct Builder {
    chunks: Vec<Arc<Chunk>>,
    current: Chunk,
    fragments: usize,
}

impl Builder {
    fn push(&mut self, start: &Bytes, end: &Bytes, seqs: &[Seq]) {
        let before = self.current.fragments.len();
        self.current.push(start, end, seqs);
        if self.current.fragments.len() == before {
            return; // merged into the last fragment
        }
        self.fragments += 1;
        if self.current.fragments.len() > CHUNK {
            // The chunk was full: move the new fragment to a new one.
            let last = self.current.fragments.pop().expect("just pushed");
            self.current.seqs.truncate(last.seqs_from as usize);
            self.chunks
                .push(Arc::new(std::mem::take(&mut self.current)));
            self.current.push_unmerged(&last.start, &last.end, seqs);
        }
    }

    fn finish(mut self) -> FragmentedRangeTombstones {
        if !self.current.fragments.is_empty() {
            self.chunks.push(Arc::new(self.current));
        }
        FragmentedRangeTombstones {
            chunks: self.chunks,
            fragments: self.fragments,
        }
    }
}

/// Splits `chunk` into chunks of at most [`CHUNK`] fragments, as even as possible, so
/// chunks rewritten by inserts stay at least half full.
fn split_evenly(chunk: Chunk) -> Vec<Arc<Chunk>> {
    let n = chunk.fragments.len();
    if n == 0 {
        return Vec::new();
    }
    if n <= CHUNK {
        return vec![Arc::new(chunk)];
    }
    let parts = n.div_ceil(CHUNK);
    let mut out = Vec::with_capacity(parts);
    let mut fragments = chunk.fragments.iter();
    for part in 0..parts {
        let size = n / parts + usize::from(part < n % parts);
        let mut piece = Chunk::default();
        for f in fragments.by_ref().take(size) {
            piece.push_unmerged(&f.start, &f.end, chunk.seqs_of(f));
        }
        out.push(Arc::new(piece));
    }
    out
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
    sources: Vec<(Arc<FragmentedRangeTombstones>, Option<Pos>)>,
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

    /// Checks the chunk layout: no empty chunk, at most `CHUNK` fragments each,
    /// fragments sorted and disjoint, and the fragment count kept up to date.
    fn check_layout(f: &FragmentedRangeTombstones) {
        let mut count = 0;
        let mut prev_end: Option<&Bytes> = None;
        for chunk in &f.chunks {
            assert!(!chunk.fragments.is_empty() && chunk.fragments.len() <= CHUNK);
            for frag in &chunk.fragments {
                assert!(frag.start < frag.end);
                assert!(prev_end.is_none_or(|e| *e <= frag.start));
                let seqs = chunk.seqs_of(frag);
                assert!(!seqs.is_empty() && seqs.windows(2).all(|w| w[0] > w[1]));
                prev_end = Some(&frag.end);
                count += 1;
            }
        }
        assert_eq!(count, f.len());
    }

    /// RV-KV2 finding 7: the memtable adds each range delete in place. Inserting one at a
    /// time (narrow, wide, nested, empty and inverted ranges, across many chunks) gives
    /// exactly the fragments of a one-pass build, and clones taken along the way keep
    /// what they saw.
    #[test]
    fn ns21_incremental_inserts_match_one_pass_fragments() {
        let mut rng = Rng::new(0x1AC_2E57);
        let key = |rng: &mut Rng| {
            let k = rng.below(600) as u16;
            Bytes::from(k.to_be_bytes().to_vec())
        };
        for round in 0..6 {
            let mut set = FragmentedRangeTombstones::empty();
            let mut input: Vec<RangeTombstone> = Vec::new();
            let mut held: Vec<(Arc<FragmentedRangeTombstones>, Vec<RangeTombstone>)> = Vec::new();
            let count = 300 + rng.below(900);
            for seq in 1..=count {
                let start = key(&mut rng);
                let end = match rng.below(10) {
                    // Wide.
                    0 => key(&mut rng),
                    // Empty.
                    1 => start.clone(),
                    // Narrow: a few keys on.
                    _ => {
                        let k = u16::from_be_bytes([start[0], start[1]]) + 1 + rng.below(4) as u16;
                        Bytes::from(k.to_be_bytes().to_vec())
                    }
                };
                let t = RangeTombstone { start, end, seq };
                set.insert(&t);
                input.push(t);
                if seq % 97 == 0 || seq == count {
                    check_layout(&set);
                    let want = FragmentedRangeTombstones::new(&input, Seq::MAX);
                    assert_eq!(
                        set.tombstones().collect::<Vec<_>>(),
                        want.tombstones().collect::<Vec<_>>(),
                        "round {round} after {seq} inserts"
                    );
                    assert_eq!(set.len(), want.len());
                    for _ in 0..200 {
                        let k = key(&mut rng);
                        let snapshot = rng.below(count + 1);
                        assert_eq!(
                            set.max_covering_seq(&k, snapshot),
                            want.max_covering_seq(&k, snapshot)
                        );
                    }
                    held.push((Arc::new(set.clone()), input.clone()));
                }
            }
            assert!(set.chunks.len() > 4, "round {round} spans several chunks");
            for (clone, input) in &held {
                assert_eq!(
                    clone.tombstones().collect::<Vec<_>>(),
                    FragmentedRangeTombstones::new(input, Seq::MAX)
                        .tombstones()
                        .collect::<Vec<_>>(),
                    "a clone changed under later inserts"
                );
            }
        }
    }

    /// One-pass builds over more than `CHUNK` fragments split into chunks and still
    /// answer like a brute force, including through an aggregator's cursor.
    #[test]
    fn ns21_one_pass_builds_span_chunks() {
        let input: Vec<RangeTombstone> = (0..500u16)
            .map(|i| {
                t(
                    &(i * 4).to_be_bytes(),
                    &(i * 4 + 2 + (i % 3)).to_be_bytes(),
                    u64::from(i) + 1,
                )
            })
            .collect();
        let f = FragmentedRangeTombstones::new(&input, Seq::MAX);
        check_layout(&f);
        assert!(f.chunks.len() > 5);
        let shared = Arc::new(f.clone());
        let mut agg = RangeDelAggregator::new(Seq::MAX);
        agg.add(shared);
        for k in 0..2_100u16 {
            let key = k.to_be_bytes();
            let want = input
                .iter()
                .filter(|t| t.start.as_ref() <= key.as_slice() && key.as_slice() < t.end.as_ref())
                .map(|t| t.seq)
                .max();
            assert_eq!(f.max_covering_seq(&key, Seq::MAX), want, "key {k}");
            assert_eq!(agg.covers(&key, 0), want.is_some(), "key {k}");
        }
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
