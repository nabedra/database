//! NS01, NS02 and NS21 at the memtable and iterator level, plus a quick bench.
//!
//! [`Engine`] is a miniature read path: a list of sources, newest first, each either a
//! sharded [`Memtable`] or a frozen sorted vector standing in for a run. Reads merge them
//! exactly as the store will (point lookups newest source first; scans through
//! [`MergingIterator`], [`RangeDelAggregator`] and [`DbIter`]).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

use super::testutil::{FakeResolver, Rng, VecIter};
use super::*;
use crate::batch::{BatchOp, WriteBatch};
use crate::memtable::{Lookup, Memtable, MemtableOptions, SnapshotList};
use crate::source::RangeTombstone;
use crate::types::{
    Kind, Locator, Seq, compare_internal_keys, internal_key, parse_internal_key, seek_key,
};

// ---------------------------------------------------------------------------------------
// Miniature read path

/// A frozen source: what a sorted run will provide.
struct Run {
    entries: Arc<Vec<(Vec<u8>, Bytes)>>,
    tombstones: Vec<RangeTombstone>,
    fragmented: Arc<FragmentedRangeTombstones>,
}

impl Run {
    /// Fragments the tombstones collapsed to the live snapshots, as a flush or a
    /// compaction writes them.
    fn new(
        entries: Vec<(Vec<u8>, Bytes)>,
        tombstones: Vec<RangeTombstone>,
        snapshots: &[Seq],
    ) -> Run {
        let fragmented = Arc::new(FragmentedRangeTombstones::collapsed(
            &tombstones,
            crate::types::MAX_SEQ,
            snapshots,
        ));
        let tombstones = fragmented.tombstones().collect();
        Run {
            entries: Arc::new(entries),
            tombstones,
            fragmented,
        }
    }

    /// The same contract as [`Memtable::get`].
    fn get(&self, user_key: &[u8], snapshot: Seq) -> Lookup {
        let target = seek_key(user_key, snapshot);
        let i = self
            .entries
            .partition_point(|e| compare_internal_keys(&e.0, &target) == std::cmp::Ordering::Less);
        let covering = self.fragmented.max_covering_seq(user_key, snapshot);
        let point = self
            .entries
            .get(i)
            .map(|e| (parse_internal_key(&e.0).unwrap(), &e.1))
            .filter(|(p, _)| p.user_key == user_key);
        match point {
            Some((p, _)) if covering.is_some_and(|t| t > p.seq) => {
                Lookup::Deleted(covering.unwrap())
            }
            Some((p, value)) => match p.kind {
                Kind::Put | Kind::JournalPut => Lookup::Found(p.kind, value.clone(), p.seq),
                _ => Lookup::Deleted(p.seq),
            },
            None => covering.map_or(Lookup::NotFound, Lookup::Deleted),
        }
    }
}

enum Source {
    Mem(Arc<Memtable>),
    Run(Arc<Run>),
}

impl Source {
    fn get(&self, user_key: &[u8], snapshot: Seq) -> Lookup {
        match self {
            Source::Mem(m) => m.get(user_key, snapshot),
            Source::Run(r) => r.get(user_key, snapshot),
        }
    }

    fn iter(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Box<dyn InternalIterator> {
        match self {
            Source::Mem(m) => Box::new(m.iter_range(lower, upper)),
            Source::Run(r) => Box::new(VecIter::shared(r.entries.clone())),
        }
    }

    fn tombstones(&self) -> Vec<RangeTombstone> {
        match self {
            Source::Mem(m) => m.range_tombstones(),
            Source::Run(r) => r.tombstones.clone(),
        }
    }

    fn fragmented(&self) -> Arc<FragmentedRangeTombstones> {
        match self {
            Source::Mem(m) => m.fragmented_tombstones(),
            Source::Run(r) => r.fragmented.clone(),
        }
    }

    fn entries(&self) -> Vec<(Vec<u8>, Bytes)> {
        match self {
            Source::Mem(m) => {
                let mut it = m.iter();
                let mut out = Vec::with_capacity(m.len());
                it.seek_to_first();
                while it.valid() {
                    out.push((it.key().to_vec(), it.value()));
                    it.next();
                }
                out
            }
            Source::Run(r) => r.entries.as_ref().clone(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Source::Mem(m) => m.len() + m.tombstone_count(),
            Source::Run(r) => r.entries.len() + r.tombstones.len(),
        }
    }
}

struct Engine {
    /// Newest first; `sources[0]` is the active memtable.
    sources: Vec<Source>,
    next_seq: Seq,
    resolver: Arc<FakeResolver>,
    options: MemtableOptions,
}

impl Engine {
    fn new(options: MemtableOptions) -> Engine {
        Engine {
            sources: vec![Source::Mem(Arc::new(Memtable::new(options, None)))],
            next_seq: 1,
            resolver: Arc::new(FakeResolver::default()),
            options,
        }
    }

    fn last_seq(&self) -> Seq {
        self.next_seq - 1
    }

    fn write(&mut self, batch: &WriteBatch) {
        let locators: Vec<Locator> = batch
            .iter()
            .filter_map(|op| match op {
                BatchOp::JournalPut { value, .. } => Some(self.resolver.store(value)),
                _ => None,
            })
            .collect();
        let Source::Mem(active) = &self.sources[0] else {
            unreachable!()
        };
        active.insert(batch, self.next_seq, &locators).unwrap();
        self.next_seq += u64::from(batch.count());
    }

    fn freeze(&mut self) {
        self.sources
            .insert(0, Source::Mem(Arc::new(Memtable::new(self.options, None))));
    }

    /// Merges the adjacent pair of frozen sources with the fewest entries into a run,
    /// keeping the newest version of each key per stripe between live snapshots and
    /// every tombstone stripe (reads happen only at live snapshots or the latest
    /// sequence).
    fn compact(&mut self, snapshots: &[Seq]) {
        if self.sources.len() < 3 {
            return;
        }
        let i = (1..self.sources.len() - 1)
            .min_by_key(|&i| self.sources[i].size() + self.sources[i + 1].size())
            .unwrap();
        let older = self.sources.remove(i + 1);
        let newer = self.sources.remove(i);
        let mut entries = newer.entries();
        entries.extend(older.entries());
        entries.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
        // Keep the newest version of each key per snapshot stripe, as compaction does.
        let mut last: Option<(Vec<u8>, usize)> = None;
        entries.retain(|(ikey, _)| {
            let parsed = parse_internal_key(ikey).unwrap();
            let stripe = snapshots.partition_point(|&s| s < parsed.seq);
            let same = last
                .as_ref()
                .is_some_and(|(k, st)| k.as_slice() == parsed.user_key && *st == stripe);
            if !same {
                last = Some((parsed.user_key.to_vec(), stripe));
            }
            !same
        });
        let mut tombstones = newer.tombstones();
        tombstones.extend(older.tombstones());
        self.sources.insert(
            i,
            Source::Run(Arc::new(Run::new(entries, tombstones, snapshots))),
        );
    }

    fn get(&self, user_key: &[u8], snapshot: Seq) -> Option<Bytes> {
        for source in &self.sources {
            match source.get(user_key, snapshot) {
                Lookup::Found(Kind::JournalPut, locator, _) => {
                    let locator = Locator::decode(&locator).unwrap();
                    return Some(self.resolver.resolve(&locator).unwrap());
                }
                Lookup::Found(_, value, _) => return Some(value),
                Lookup::Deleted(_) => return None,
                Lookup::NotFound => {}
            }
        }
        None
    }

    fn iter(&self, snapshot: Seq, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> DbIter {
        iter_over(&self.sources, snapshot, lower, upper, self.resolver.clone())
    }
}

fn iter_over(
    sources: &[Source],
    snapshot: Seq,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    resolver: Arc<FakeResolver>,
) -> DbIter {
    let mut tombstones = RangeDelAggregator::new(snapshot);
    for source in sources {
        tombstones.add(source.fragmented());
    }
    let children = sources.iter().map(|s| s.iter(lower, upper)).collect();
    let inner: Box<dyn InternalIterator> = Box::new(MergingIterator::new(children));
    DbIter::new(inner, snapshot, lower, upper)
        .with_range_tombstones(tombstones)
        .with_resolver(resolver)
}

// ---------------------------------------------------------------------------------------
// Oracle and generators

type View = BTreeMap<Vec<u8>, Vec<u8>>;

const GROUPS: u64 = 6;

/// `0x40 ‖ 16-byte group id ‖ suffix`.
fn group_key(group: u64, suffix: &[u8]) -> Vec<u8> {
    let mut key = vec![0x40];
    key.extend_from_slice(&u128::from(group).to_be_bytes());
    key.extend_from_slice(suffix);
    key
}

fn gen_key(rng: &mut Rng) -> Vec<u8> {
    if rng.chance(4) {
        // Shorter than the shard prefix: shard 0.
        return vec![0x10, rng.below(16) as u8];
    }
    let suffix: Vec<u8> = (0..rng.below(3)).map(|_| rng.below(24) as u8).collect();
    group_key(rng.below(GROUPS), &suffix)
}

fn gen_value(rng: &mut Rng, max: u64) -> Vec<u8> {
    (0..rng.below(max)).map(|_| rng.next_u64() as u8).collect()
}

fn gen_bounds(rng: &mut Rng) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let any = |rng: &mut Rng| match rng.below(3) {
        0 => Bound::Unbounded,
        1 => Bound::Included(gen_key(rng)),
        _ => Bound::Excluded(gen_key(rng)),
    };
    match rng.below(10) {
        // One group: the single-shard memtable path.
        0..=3 => {
            let g = rng.below(GROUPS);
            (
                Bound::Included(group_key(g, b"")),
                Bound::Excluded(group_key(g + 1, b"")),
            )
        }
        // Inside one group with arbitrary ends.
        4..=7 => {
            let g = rng.below(GROUPS);
            let end = |rng: &mut Rng| {
                let suffix = [rng.below(24) as u8];
                if rng.chance(50) {
                    Bound::Included(group_key(g, &suffix))
                } else {
                    Bound::Excluded(group_key(g, &suffix))
                }
            };
            (end(rng), end(rng))
        }
        _ => (any(rng), any(rng)),
    }
}

fn in_bounds(key: &[u8], lower: &Bound<Vec<u8>>, upper: &Bound<Vec<u8>>) -> bool {
    let above_lower = match lower {
        Bound::Included(l) => key >= l.as_slice(),
        Bound::Excluded(l) => key > l.as_slice(),
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(u) => key <= u.as_slice(),
        Bound::Excluded(u) => key < u.as_slice(),
        Bound::Unbounded => true,
    };
    above_lower && below_upper
}

fn as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    bound.as_ref().map(Vec::as_slice)
}

fn apply_to_oracle(view: &mut View, batch: &WriteBatch) {
    for op in batch.iter() {
        match op {
            BatchOp::Put { key, value } | BatchOp::JournalPut { key, value, .. } => {
                view.insert(key.to_vec(), value.to_vec());
            }
            BatchOp::Delete { key } => {
                view.remove(key);
            }
            BatchOp::RangeDelete { start, end } => {
                if start < end {
                    let doomed: Vec<_> = view
                        .range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
                        .map(|(k, _)| k.clone())
                        .collect();
                    for key in doomed {
                        view.remove(&key);
                    }
                }
            }
        }
    }
}

fn gen_batch(rng: &mut Rng) -> WriteBatch {
    let mut batch = WriteBatch::new();
    let ops = if rng.chance(30) { 2 + rng.below(4) } else { 1 };
    for _ in 0..ops {
        let key = gen_key(rng);
        match rng.below(100) {
            0..=54 => batch.put(&key, &gen_value(rng, 24)),
            55..=69 => batch.put_journal(&key, &gen_value(rng, 200)),
            70..=93 => batch.delete(&key),
            _ => {
                // Mostly small ranges inside one group, sometimes empty or inverted.
                let end = if rng.chance(97) {
                    let mut end = key.clone();
                    end.push(rng.below(24) as u8);
                    if rng.chance(50) {
                        let last = end.len() - 2;
                        end[last] = end[last].wrapping_add(1 + rng.below(4) as u8);
                    }
                    end
                } else {
                    gen_key(rng)
                };
                batch.delete_range(&key, &end);
            }
        }
    }
    batch
}

// ---------------------------------------------------------------------------------------
// Checks

fn check_get(engine: &Engine, snapshot: Seq, view: &View, key: &[u8], ctx: &str) {
    assert_eq!(
        engine.get(key, snapshot).map(|v| v.to_vec()),
        view.get(key).cloned(),
        "{ctx}: get {key:?} at {snapshot}"
    );
}

fn current_entry(it: &DbIter) -> (Vec<u8>, Vec<u8>) {
    (it.key().to_vec(), it.value().unwrap().to_vec())
}

fn check_scan(
    engine: &Engine,
    snapshot: Seq,
    view: &View,
    lower: &Bound<Vec<u8>>,
    upper: &Bound<Vec<u8>>,
    ctx: &str,
) {
    let want: Vec<(Vec<u8>, Vec<u8>)> = view
        .iter()
        .filter(|(k, _)| in_bounds(k, lower, upper))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut it = engine.iter(snapshot, as_ref(lower), as_ref(upper));
    let mut got = Vec::new();
    it.seek_to_first();
    while it.valid() {
        got.push(current_entry(&it));
        it.next();
    }
    it.status().unwrap();
    assert_eq!(
        got, want,
        "{ctx}: forward scan {lower:?}..{upper:?} at {snapshot}"
    );

    let mut got = Vec::new();
    it.seek_to_last();
    while it.valid() {
        got.push(current_entry(&it));
        it.prev();
    }
    it.status().unwrap();
    got.reverse();
    assert_eq!(
        got, want,
        "{ctx}: reverse scan {lower:?}..{upper:?} at {snapshot}"
    );
}

/// A random walk of seeks and steps in both directions against the oracle's positions.
fn check_walk(
    engine: &Engine,
    snapshot: Seq,
    view: &View,
    lower: &Bound<Vec<u8>>,
    upper: &Bound<Vec<u8>>,
    rng: &mut Rng,
    ctx: &str,
) {
    const STEPS: usize = 30;
    let want: Vec<(&Vec<u8>, &Vec<u8>)> = view
        .iter()
        .filter(|(k, _)| in_bounds(k, lower, upper))
        .collect();
    let mut it = engine.iter(snapshot, as_ref(lower), as_ref(upper));
    let mut pos: Option<usize> = None;
    let mut trace = Vec::new();
    for _ in 0..STEPS {
        let op = rng.below(8);
        match op {
            0 => {
                it.seek_to_first();
                pos = (!want.is_empty()).then_some(0);
                trace.push("first".to_string());
            }
            1 => {
                it.seek_to_last();
                pos = want.len().checked_sub(1);
                trace.push("last".to_string());
            }
            2 => {
                let target = gen_key(rng);
                it.seek(&target);
                let i = want.partition_point(|(k, _)| k.as_slice() < target.as_slice());
                pos = (i < want.len()).then_some(i);
                trace.push(format!("seek {target:?}"));
            }
            3 => {
                let target = gen_key(rng);
                it.seek_for_prev(&target);
                let i = want.partition_point(|(k, _)| k.as_slice() <= target.as_slice());
                pos = i.checked_sub(1);
                trace.push(format!("seek_for_prev {target:?}"));
            }
            4 | 5 if pos.is_some() => {
                it.next();
                pos = pos.map(|p| p + 1).filter(|&p| p < want.len());
                trace.push("next".to_string());
            }
            6 | 7 if pos.is_some() => {
                it.prev();
                pos = pos.and_then(|p| p.checked_sub(1));
                trace.push("prev".to_string());
            }
            _ => continue,
        }
        it.status().unwrap();
        assert_eq!(
            it.valid(),
            pos.is_some(),
            "{ctx}: walk {trace:?} in {lower:?}..{upper:?} at {snapshot}"
        );
        if let Some(p) = pos {
            assert_eq!(
                current_entry(&it),
                (want[p].0.clone(), want[p].1.clone()),
                "{ctx}: walk {trace:?} in {lower:?}..{upper:?} at {snapshot}"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------
// NS01

struct Snap {
    _handle: crate::memtable::SnapshotHandle,
    seq: Seq,
    view: View,
}

fn run_ns01(seed: u64, ops: usize) {
    let mut rng = Rng::new(seed);
    let mut engine = Engine::new(MemtableOptions::default());
    let mut current = View::new();
    let snapshot_list = SnapshotList::new();
    let mut snaps: Vec<Snap> = Vec::new();
    let mut done = 0;
    let mut until_freeze = 500 + rng.below(1500) as usize;
    let (mut gets, mut scans, mut walks, mut freezes) = (0, 0, 0, 0);
    while done < ops {
        let batch = gen_batch(&mut rng);
        apply_to_oracle(&mut current, &batch);
        engine.write(&batch);
        done += batch.count() as usize;
        let ctx = format!("seed {seed:#x} op {done}");

        if rng.chance(2) && snaps.len() < 4 {
            let seq = engine.last_seq();
            snaps.push(Snap {
                _handle: snapshot_list.register(seq),
                seq,
                view: current.clone(),
            });
        }
        if rng.chance(2) && !snaps.is_empty() {
            snaps.remove(rng.below(snaps.len() as u64) as usize);
        }
        assert_eq!(
            snapshot_list.oldest(),
            snaps.iter().map(|s| s.seq).min(),
            "{ctx}"
        );

        until_freeze = until_freeze.saturating_sub(batch.count() as usize);
        if until_freeze == 0 {
            engine.freeze();
            freezes += 1;
            if engine.sources.len() > 6 {
                engine.compact(&snapshot_list.sequences());
            }
            until_freeze = 500 + rng.below(1500) as usize;
        }

        // Pick a view: the current state or a held snapshot.
        let pick = rng.below(snaps.len() as u64 + 2) as usize;
        let (seq, view) = match snaps.get(pick) {
            Some(s) => (s.seq, &s.view),
            None => (engine.last_seq(), &current),
        };
        for _ in 0..2 {
            check_get(&engine, seq, view, &gen_key(&mut rng), &ctx);
            gets += 1;
        }
        if rng.chance(2) {
            let (lower, upper) = gen_bounds(&mut rng);
            check_scan(&engine, seq, view, &lower, &upper, &ctx);
            scans += 1;
        }
        if rng.chance(3) {
            let (lower, upper) = gen_bounds(&mut rng);
            check_walk(&engine, seq, view, &lower, &upper, &mut rng, &ctx);
            walks += 1;
        }
    }
    let ctx = format!("seed {seed:#x} final");
    check_scan(
        &engine,
        engine.last_seq(),
        &current,
        &Bound::Unbounded,
        &Bound::Unbounded,
        &ctx,
    );
    for snap in &snaps {
        check_scan(
            &engine,
            snap.seq,
            &snap.view,
            &Bound::Unbounded,
            &Bound::Unbounded,
            &ctx,
        );
    }
    eprintln!(
        "ns01 seed {seed:#x}: {done} ops, {gets} gets, {scans} scans, {walks} walks, \
         {freezes} freezes, {} sources at the end",
        engine.sources.len()
    );
}

#[test]
fn ns01_memtable_iterator_model_1e5() {
    for seed in [0x0001_5EED, 0xBADC_0FFE] {
        run_ns01(seed, 50_000);
    }
}

#[test]
#[ignore = "10^6 operations; run with --ignored (release recommended)"]
fn ns01_memtable_iterator_model_1e6() {
    run_ns01(0x1E6_5EED, 1_000_000);
}

// ---------------------------------------------------------------------------------------
// NS02

const NS02_KEYS: usize = 12;

fn ns02_key(i: usize) -> Vec<u8> {
    if i % 6 == 5 {
        vec![0x10, i as u8]
    } else {
        group_key(i as u64 % 5, &[i as u8])
    }
}

struct Shared {
    sources: RwLock<Arc<Vec<Source>>>,
    visible: AtomicU64,
    completed: AtomicU64,
    resolver: Arc<FakeResolver>,
    /// The journal thread's state: the next sequence and the batch id counter.
    journal: Mutex<(Seq, u64)>,
}

/// Every key holds the same batch id, or no key exists (a pure range-delete batch).
fn check_consistent(values: &[Option<Bytes>], completed: u64, ctx: &str) {
    let first = &values[0];
    assert!(
        values.iter().all(|v| v == first),
        "{ctx}: partial batch visible: {values:?}"
    );
    if let Some(v) = first {
        let id = u64::from_be_bytes(v[..8].try_into().unwrap());
        assert!(
            id >= completed,
            "{ctx}: stale batch {id} < completed {completed}"
        );
    }
}

fn run_ns02(batches_per_writer: u64) {
    const WRITERS: u64 = 8;
    const READERS: usize = 16;
    let options = MemtableOptions::default();
    let shared = Arc::new(Shared {
        sources: RwLock::new(Arc::new(vec![Source::Mem(Arc::new(Memtable::new(
            options, None,
        )))])),
        visible: AtomicU64::new(0),
        completed: AtomicU64::new(0),
        resolver: Arc::new(FakeResolver::default()),
        journal: Mutex::new((1, 0)),
    });
    let stop = Arc::new(AtomicBool::new(false));

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let shared = shared.clone();
            std::thread::spawn(move || {
                let mut rng = Rng::new(0x0002_0000 + w);
                for _ in 0..batches_per_writer {
                    let mut journal = shared.journal.lock().unwrap();
                    let (seq, id) = (journal.0, journal.1 + 1);
                    let mut batch = WriteBatch::new();
                    let mut locators = Vec::new();
                    let pure_delete = rng.chance(5);
                    if pure_delete || rng.chance(10) {
                        batch.delete_range(&[0], &[0xFF]);
                    }
                    if !pure_delete {
                        for i in 0..NS02_KEYS {
                            let value = id.to_be_bytes();
                            if rng.chance(20) {
                                batch.put_journal(&ns02_key(i), &value);
                                locators.push(shared.resolver.store(&value));
                            } else {
                                batch.put(&ns02_key(i), &value);
                            }
                        }
                    }
                    if id % 150 == 0 {
                        // Freeze: publish the new memtable before anything lands in it.
                        let mut sources = shared.sources.write().unwrap();
                        let mut next: Vec<Source> =
                            vec![Source::Mem(Arc::new(Memtable::new(options, None)))];
                        next.extend(sources.iter().map(|s| match s {
                            Source::Mem(m) => Source::Mem(m.clone()),
                            Source::Run(r) => Source::Run(r.clone()),
                        }));
                        *sources = Arc::new(next);
                    }
                    let active = match &shared.sources.read().unwrap()[0] {
                        Source::Mem(m) => m.clone(),
                        Source::Run(_) => unreachable!(),
                    };
                    active.insert(&batch, seq, &locators).unwrap();
                    let last = seq + u64::from(batch.count()) - 1;
                    *journal = (last + 1, id);
                    shared.visible.store(last, AtomicOrdering::Release);
                    shared.completed.store(id, AtomicOrdering::Release);
                    drop(journal);
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..READERS)
        .map(|r| {
            let shared = shared.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut reads = 0u64;
                let keys: Vec<Vec<u8>> = (0..NS02_KEYS).map(ns02_key).collect();
                loop {
                    let finished = stop.load(AtomicOrdering::Acquire);
                    let completed = shared.completed.load(AtomicOrdering::Acquire);
                    let snapshot = shared.visible.load(AtomicOrdering::Acquire);
                    // Sources only grow, so every memtable holding a sequence at or
                    // below `snapshot` is in this list.
                    let sources = shared.sources.read().unwrap().clone();
                    let ctx = format!("reader {r} read {reads} at {snapshot}");
                    if r % 2 == 0 {
                        let values: Vec<Option<Bytes>> = keys
                            .iter()
                            .map(|k| {
                                for s in sources.iter() {
                                    match s.get(k, snapshot) {
                                        Lookup::Found(Kind::JournalPut, l, _) => {
                                            let l = Locator::decode(&l).unwrap();
                                            return Some(shared.resolver.resolve(&l).unwrap());
                                        }
                                        Lookup::Found(_, v, _) => return Some(v),
                                        Lookup::Deleted(_) => return None,
                                        Lookup::NotFound => {}
                                    }
                                }
                                None
                            })
                            .collect();
                        check_consistent(&values, completed, &ctx);
                    } else {
                        let mut it = iter_over(
                            &sources,
                            snapshot,
                            Bound::Unbounded,
                            Bound::Unbounded,
                            shared.resolver.clone(),
                        );
                        let mut values: Vec<Option<Bytes>> = Vec::new();
                        let mut seen = Vec::new();
                        if r % 4 == 1 {
                            it.seek_to_first();
                            while it.valid() {
                                seen.push(it.key().to_vec());
                                values.push(Some(it.value().unwrap()));
                                it.next();
                            }
                        } else {
                            it.seek_to_last();
                            while it.valid() {
                                seen.push(it.key().to_vec());
                                values.push(Some(it.value().unwrap()));
                                it.prev();
                            }
                        }
                        it.status().unwrap();
                        assert!(
                            values.is_empty() || values.len() == NS02_KEYS,
                            "{ctx}: scan saw {} of {NS02_KEYS} keys",
                            values.len()
                        );
                        if values.is_empty() {
                            values.push(None);
                        }
                        check_consistent(&values, completed, &ctx);
                    }
                    reads += 1;
                    if finished {
                        return reads;
                    }
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, AtomicOrdering::Release);
    let reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
    let journal = shared.journal.lock().unwrap();
    eprintln!(
        "ns02: {} batches, {} ops, {} memtables, {reads} reads",
        journal.1,
        journal.0 - 1,
        shared.sources.read().unwrap().len()
    );
    assert_eq!(journal.1, WRITERS * batches_per_writer);
}

#[test]
fn ns02_no_reader_sees_part_of_a_batch() {
    // 8 writers × 1100 batches × ~12 operations ≈ 10^5 operations.
    run_ns02(1100);
}

#[test]
#[ignore = "10^6 operations; run with --ignored (release recommended)"]
fn ns02_no_reader_sees_part_of_a_batch_1e6() {
    run_ns02(11_000);
}

// ---------------------------------------------------------------------------------------
// NS21

fn put_batch(engine: &mut Engine, ops: &[(&[u8], Option<&[u8]>)]) -> Seq {
    let mut batch = WriteBatch::new();
    for (k, v) in ops {
        match v {
            Some(v) => batch.put(k, v),
            None => batch.delete(k),
        }
    }
    engine.write(&batch);
    engine.last_seq()
}

fn delete_range(engine: &mut Engine, start: &[u8], end: &[u8]) -> Seq {
    let mut batch = WriteBatch::new();
    batch.delete_range(start, end);
    engine.write(&batch);
    engine.last_seq()
}

fn scan(engine: &Engine, snapshot: Seq, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Vec<Vec<u8>> {
    let mut it = engine.iter(snapshot, lower, upper);
    let mut out = Vec::new();
    it.seek_to_first();
    while it.valid() {
        out.push(it.key().to_vec());
        it.next();
    }
    let mut back = Vec::new();
    it.seek_to_last();
    while it.valid() {
        back.push(it.key().to_vec());
        it.prev();
    }
    back.reverse();
    assert_eq!(out, back, "forward and reverse scans differ");
    out
}

fn keys(list: &[&[u8]]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.to_vec()).collect()
}

#[test]
fn ns21_empty_ranges_and_bounds_at_both_ends() {
    let mut engine = Engine::new(MemtableOptions::default());
    let s = put_batch(
        &mut engine,
        &[
            (b"a", Some(b"1")),
            (b"b", Some(b"2")),
            (b"c", Some(b"3")),
            (b"d", Some(b"4")),
        ],
    );
    use Bound::{Excluded as Ex, Included as In, Unbounded as Un};
    let all = keys(&[b"a", b"b", b"c", b"d"]);
    assert_eq!(scan(&engine, s, Un, Un), all);
    assert_eq!(scan(&engine, s, In(b"b"), In(b"c")), keys(&[b"b", b"c"]));
    assert_eq!(scan(&engine, s, Ex(b"b"), Ex(b"d")), keys(&[b"c"]));
    assert_eq!(
        scan(&engine, s, Ex(b"a"), In(b"d")),
        keys(&[b"b", b"c", b"d"])
    );
    assert_eq!(scan(&engine, s, In(b"b"), In(b"b")), keys(&[b"b"]));
    for (lower, upper) in [
        (In(&b"b"[..]), Ex(&b"b"[..])),
        (Ex(b"b"), In(b"b")),
        (Ex(b"b"), Ex(b"b")),
        (In(b"c"), In(b"b")),
        (In(b"bb"), Ex(b"c")),
        (In(b"e"), Un),
        (Un, Ex(b"a")),
    ] {
        assert!(
            scan(&engine, s, lower, upper).is_empty(),
            "{lower:?}..{upper:?}"
        );
        let mut it = engine.iter(s, lower, upper);
        it.seek(b"b");
        assert!(!it.valid());
        it.seek_for_prev(b"b");
        assert!(!it.valid());
    }
    assert!(
        scan(&engine, 0, Un, Un).is_empty(),
        "nothing visible at sequence 0"
    );

    // Seeks outside the bounds clamp to them.
    let mut it = engine.iter(s, Ex(b"a"), Ex(b"d"));
    it.seek(b"");
    assert_eq!(it.key(), b"b");
    it.seek_for_prev(b"zzz");
    assert_eq!(it.key(), b"c");
    it.seek(b"d");
    assert!(!it.valid());
    it.seek_for_prev(b"a");
    assert!(!it.valid());
    it.seek_for_prev(b"bz");
    assert_eq!(it.key(), b"b");
    it.prev();
    assert!(!it.valid(), "prev at the lower bound");
    it.seek(b"bz");
    assert_eq!(it.key(), b"c");
    it.next();
    assert!(!it.valid(), "next at the upper bound");
}

#[test]
fn ns21_direction_switches_over_versions_and_hidden_entries() {
    let mut engine = Engine::new(MemtableOptions::default());
    put_batch(
        &mut engine,
        &[
            (b"a", Some(b"a1")),
            (b"b", Some(b"b1")),
            (b"c", Some(b"c1")),
        ],
    );
    engine.freeze();
    let snap = put_batch(&mut engine, &[(b"b", Some(b"b2"))]);
    // Many versions of b above the snapshot, then a delete of c, then a new d.
    for i in 0..40u8 {
        put_batch(&mut engine, &[(b"b", Some(&[b'x', i]))]);
    }
    engine.freeze();
    let now = put_batch(&mut engine, &[(b"c", None), (b"d", Some(b"d1"))]);

    let mut it = engine.iter(snap, Bound::Unbounded, Bound::Unbounded);
    it.seek(b"b");
    assert_eq!(
        (it.key(), it.value().unwrap().as_ref()),
        (&b"b"[..], &b"b2"[..])
    );
    it.next();
    assert_eq!(it.key(), b"c");
    it.prev();
    assert_eq!(
        (it.key(), it.value().unwrap().as_ref()),
        (&b"b"[..], &b"b2"[..])
    );
    it.prev();
    assert_eq!(it.key(), b"a");
    it.next();
    it.next();
    assert_eq!(it.key(), b"c");
    it.next();
    assert!(!it.valid(), "d is above the snapshot");

    let mut it = engine.iter(now, Bound::Unbounded, Bound::Unbounded);
    it.seek_to_last();
    assert_eq!(it.key(), b"d");
    it.prev();
    assert_eq!(
        (it.key(), it.value().unwrap().as_ref()),
        (&b"b"[..], &[b'x', 39][..])
    );
    it.next();
    assert_eq!(it.key(), b"d", "c is deleted");
    it.prev();
    it.prev();
    assert_eq!(it.key(), b"a");
    it.prev();
    assert!(!it.valid());
    it.seek_to_first();
    it.next();
    it.next();
    assert_eq!(it.key(), b"d");
}

#[test]
fn ns21_seek_for_prev_on_tombstoned_keys() {
    let mut engine = Engine::new(MemtableOptions::default());
    put_batch(
        &mut engine,
        &[
            (b"a", Some(b"1")),
            (b"b", Some(b"2")),
            (b"c", Some(b"3")),
            (b"e", Some(b"5")),
        ],
    );
    engine.freeze();
    put_batch(&mut engine, &[(b"c", None)]);
    let now = delete_range(&mut engine, b"d", b"f");
    let mut it = engine.iter(now, Bound::Unbounded, Bound::Unbounded);
    it.seek_for_prev(b"c");
    assert_eq!(it.key(), b"b", "point-deleted c is skipped");
    it.seek_for_prev(b"e");
    assert_eq!(it.key(), b"b", "range-deleted e is skipped");
    it.seek(b"c");
    assert!(!it.valid());
    it.seek_for_prev(b"b");
    assert_eq!(it.key(), b"b");
    it.next();
    assert!(!it.valid());
}

#[test]
fn ns21_range_tombstone_straddling_sources() {
    let mut engine = Engine::new(MemtableOptions::default());
    // Oldest source (compacted into a run below), a middle memtable, then the tombstone.
    put_batch(
        &mut engine,
        &[
            (b"k1", Some(b"old")),
            (b"k5", Some(b"old")),
            (b"z", Some(b"z")),
        ],
    );
    engine.freeze();
    put_batch(&mut engine, &[(b"k3", Some(b"mid")), (b"k7", Some(b"mid"))]);
    engine.freeze();
    put_batch(&mut engine, &[(b"k0", Some(b"keep"))]);
    engine.freeze();
    engine.compact(&[]);
    assert!(engine.sources.iter().any(|s| matches!(s, Source::Run(_))));
    let before = engine.last_seq();
    let tomb = delete_range(&mut engine, b"k1", b"k6");
    put_batch(&mut engine, &[(b"k5", Some(b"new"))]);
    let now = engine.last_seq();
    use Bound::Unbounded as Un;
    assert_eq!(
        scan(&engine, before, Un, Un),
        keys(&[b"k0", b"k1", b"k3", b"k5", b"k7", b"z"])
    );
    assert_eq!(scan(&engine, tomb, Un, Un), keys(&[b"k0", b"k7", b"z"]));
    assert_eq!(
        scan(&engine, now, Un, Un),
        keys(&[b"k0", b"k5", b"k7", b"z"])
    );
    assert_eq!(engine.get(b"k5", now).as_deref(), Some(&b"new"[..]));
    assert_eq!(engine.get(b"k3", now), None);
    assert_eq!(engine.get(b"k3", before).as_deref(), Some(&b"mid"[..]));
    let mut it = engine.iter(now, Bound::Included(b"k1"), Bound::Excluded(b"z"));
    it.seek_to_last();
    assert_eq!(it.key(), b"k7");
    it.prev();
    assert_eq!(it.key(), b"k5");
    it.prev();
    assert!(!it.valid(), "k1 and k3 are covered in older sources");
}

#[test]
fn ns21_iterator_errors_surface_in_status_and_values() {
    let mut it = DbIter::new(
        Box::new(VecIter::failing(crate::Error::Io(
            std::io::ErrorKind::Other,
        ))) as Box<dyn InternalIterator>,
        10,
        Bound::Unbounded,
        Bound::Unbounded,
    );
    it.seek_to_first();
    assert!(!it.valid());
    assert!(matches!(it.status(), Err(crate::Error::Io(_))));

    // A journal value without a resolver is an error, never a wrong value.
    let entries = vec![(
        internal_key(b"k", 1, Kind::JournalPut),
        Bytes::copy_from_slice(&FakeResolver::default().store(b"v").encode()),
    )];
    let mut it = DbIter::new(VecIter::new(entries), 1, Bound::Unbounded, Bound::Unbounded);
    it.seek_to_first();
    assert_eq!(it.kind(), Kind::JournalPut);
    assert!(matches!(it.value(), Err(crate::Error::InvalidArgument(_))));
}

// ---------------------------------------------------------------------------------------
// Bench

/// Counts every move of the inner iterator.
struct Counting<I> {
    inner: I,
    steps: Arc<AtomicU64>,
}

impl<I: InternalIterator> InternalIterator for Counting<I> {
    fn valid(&self) -> bool {
        self.inner.valid()
    }
    fn seek_to_first(&mut self) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.seek_to_first();
    }
    fn seek_to_last(&mut self) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.seek_to_last();
    }
    fn seek(&mut self, target: &[u8]) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.seek(target);
    }
    fn seek_for_prev(&mut self, target: &[u8]) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.seek_for_prev(target);
    }
    fn next(&mut self) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.next();
    }
    fn prev(&mut self) {
        self.steps.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.prev();
    }
    fn key(&self) -> &[u8] {
        self.inner.key()
    }
    fn value(&self) -> Bytes {
        self.inner.value()
    }
    fn status(&self) -> crate::error::Result<()> {
        self.inner.status()
    }
}

/// One version: `(user key, seq, value or None for a delete)`.
type Version = (Vec<u8>, Seq, Option<Vec<u8>>);

/// The entries of one sorted source holding `versions`.
fn sorted_entries(versions: &[Version]) -> Vec<(Vec<u8>, Bytes)> {
    let mut entries: Vec<(Vec<u8>, Bytes)> = versions
        .iter()
        .map(|(key, seq, value)| match value {
            Some(v) => (internal_key(key, *seq, Kind::Put), Bytes::from(v.clone())),
            None => (internal_key(key, *seq, Kind::Delete), Bytes::new()),
        })
        .collect();
    entries.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
    entries
}

/// The live keys and values at `snapshot`, by brute force.
fn visible_at(
    versions: &[Version],
    tombstones: &[RangeTombstone],
    snapshot: Seq,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut newest: BTreeMap<Vec<u8>, (Seq, Option<Vec<u8>>)> = BTreeMap::new();
    for (key, seq, value) in versions {
        if *seq <= snapshot && newest.get(key).is_none_or(|(s, _)| s < seq) {
            newest.insert(key.clone(), (*seq, value.clone()));
        }
    }
    newest
        .into_iter()
        .filter_map(|(key, (seq, value))| {
            let covered = tombstones.iter().any(|t| {
                t.seq <= snapshot
                    && t.seq > seq
                    && t.start.as_ref() <= key.as_slice()
                    && key.as_slice() < t.end.as_ref()
            });
            Some((key, value.filter(|_| !covered)?))
        })
        .collect()
}

fn counted_iter(
    entries: &Arc<Vec<(Vec<u8>, Bytes)>>,
    tombstones: &[RangeTombstone],
    snapshot: Seq,
) -> (DbIter<Counting<VecIter>>, Arc<AtomicU64>) {
    let steps = Arc::new(AtomicU64::new(0));
    let inner = Counting {
        inner: VecIter::shared(entries.clone()),
        steps: steps.clone(),
    };
    let mut agg = RangeDelAggregator::new(snapshot);
    agg.add_tombstones(tombstones);
    let it =
        DbIter::new(inner, snapshot, Bound::Unbounded, Bound::Unbounded).with_range_tombstones(agg);
    (it, steps)
}

/// NS3 concern 4: going backward, long runs of old versions of one key and of versions
/// above the snapshot are crossed with a seek, not stepped through one by one.
#[test]
fn ns21_reverse_scans_reseek_over_long_version_chains() {
    let v = |s: &str| Some(s.as_bytes().to_vec());
    let mut versions = vec![(b"a".to_vec(), 1, v("a1"))];
    // b: 1,000 visible versions.
    versions.extend((2..=1001).map(|seq| (b"b".to_vec(), seq, v(&format!("b{seq}")))));
    // c: 500 visible versions, the newest a delete.
    versions.extend((1002..1501).map(|seq| (b"c".to_vec(), seq, v("c"))));
    versions.push((b"c".to_vec(), 1501, None));
    // d: one visible version and 1,000 above the snapshot.
    versions.push((b"d".to_vec(), 1550, v("d1550")));
    versions.extend((2000..3000).map(|seq| (b"d".to_vec(), seq, v("d-new"))));
    // e: 1,000 versions, all above the snapshot.
    versions.extend((3000..4000).map(|seq| (b"e".to_vec(), seq, v("e-new"))));
    versions.push((b"f".to_vec(), 1600, v("f1600")));
    let entries = Arc::new(sorted_entries(&versions));
    let snapshot = 1600;
    let want = visible_at(&versions, &[], snapshot);
    assert_eq!(
        want.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(),
        vec![&b"a"[..], b"b", b"d", b"f"]
    );

    let (mut it, steps) = counted_iter(&entries, &[], snapshot);
    let mut got = Vec::new();
    it.seek_to_last();
    while it.valid() {
        got.push((it.key().to_vec(), it.value().unwrap().to_vec()));
        it.prev();
    }
    it.status().unwrap();
    got.reverse();
    assert_eq!(got, want);
    let reverse_steps = steps.load(AtomicOrdering::Relaxed);
    assert!(
        reverse_steps < 200,
        "a reverse scan over {} entries took {reverse_steps} inner steps",
        entries.len()
    );

    // seek_for_prev into a long chain, and direction switches around it.
    let (mut it, steps) = counted_iter(&entries, &[], snapshot);
    it.seek_for_prev(b"e");
    assert_eq!(
        (it.key(), &it.value().unwrap()[..]),
        (&b"d"[..], &b"d1550"[..])
    );
    it.prev();
    assert_eq!(
        (it.key(), &it.value().unwrap()[..]),
        (&b"b"[..], &b"b1001"[..])
    );
    it.next();
    assert_eq!(it.key(), b"d");
    it.prev();
    assert_eq!(it.key(), b"b");
    it.prev();
    assert_eq!(it.key(), b"a");
    it.prev();
    assert!(!it.valid());
    it.status().unwrap();
    assert!(steps.load(AtomicOrdering::Relaxed) < 300);

    // A range tombstone hiding the newest version of b, read through the seek path.
    let tombstones = [RangeTombstone {
        start: Bytes::from_static(b"b"),
        end: Bytes::from_static(b"c"),
        seq: 1200,
    }];
    let (mut it, _) = counted_iter(&entries, &tombstones, snapshot);
    it.seek_to_last();
    let mut got = Vec::new();
    while it.valid() {
        got.push(it.key().to_vec());
        it.prev();
    }
    assert_eq!(got, keys(&[b"f", b"d", b"a"]));
}

/// Reverse and forward scans and walks agree with brute force over few keys with many
/// versions each, deletes, range tombstones and snapshots, so both re-seek paths run
/// often.
#[test]
fn ns21_reverse_reseeks_match_a_brute_force_model() {
    let mut rng = Rng::new(0x0005_EE4B);
    for round in 0..60 {
        let key_count = 1 + rng.below(5);
        let mut versions = Vec::new();
        let mut tombstones = Vec::new();
        let total = 50 + rng.below(400);
        for seq in 1..=total {
            let key = vec![b'k', rng.below(key_count) as u8];
            match rng.below(20) {
                0 => tombstones.push(RangeTombstone {
                    start: Bytes::from(vec![b'k', rng.below(key_count) as u8]),
                    end: Bytes::from(vec![b'k', rng.below(key_count + 1) as u8]),
                    seq,
                }),
                1..=3 => versions.push((key, seq, None)),
                _ => versions.push((key, seq, Some(seq.to_le_bytes().to_vec()))),
            }
        }
        let entries = Arc::new(sorted_entries(&versions));
        for _ in 0..8 {
            let snapshot = rng.below(total + 2);
            let want = visible_at(&versions, &tombstones, snapshot);
            let ctx = format!("round {round} snapshot {snapshot}");
            let (mut it, _) = counted_iter(&entries, &tombstones, snapshot);
            let mut back = Vec::new();
            it.seek_to_last();
            while it.valid() {
                back.push((it.key().to_vec(), it.value().unwrap().to_vec()));
                it.prev();
            }
            it.status().unwrap();
            back.reverse();
            assert_eq!(back, want, "{ctx}: reverse scan");
            let mut forward = Vec::new();
            it.seek_to_first();
            while it.valid() {
                forward.push((it.key().to_vec(), it.value().unwrap().to_vec()));
                it.next();
            }
            assert_eq!(forward, want, "{ctx}: forward scan");
            // Random walks with direction switches.
            for _ in 0..10 {
                let target = vec![b'k', rng.below(key_count + 1) as u8];
                let mut pos = if rng.chance(50) {
                    it.seek(&target);
                    want.partition_point(|(k, _)| *k < target) as i64
                } else {
                    it.seek_for_prev(&target);
                    want.partition_point(|(k, _)| *k <= target) as i64 - 1
                };
                for _ in 0..8 {
                    let expect = usize::try_from(pos).ok().and_then(|p| want.get(p));
                    match expect {
                        Some((k, v)) => {
                            assert!(it.valid(), "{ctx}: walk ended early at {pos}");
                            assert_eq!(
                                (it.key(), &it.value().unwrap()[..]),
                                (k.as_slice(), v.as_slice()),
                                "{ctx}: walk at {pos}"
                            );
                        }
                        None => {
                            assert!(!it.valid(), "{ctx}: walk past the end at {pos}");
                            break;
                        }
                    }
                    if rng.chance(50) {
                        it.next();
                        pos += 1;
                    } else {
                        it.prev();
                        pos -= 1;
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "bench: cargo test --release -p nabedra-kv -- --ignored ns18_memtable --nocapture"]
fn ns18_memtable_lookup_and_scan_bench() {
    const GROUPS: u64 = 64;
    const ROWS: u64 = 4096; // 262,144 keys
    let mem = Arc::new(Memtable::new(MemtableOptions::default(), None));
    let mut seq = 1;
    let start = nabedra_rt::time::now_std();
    for g in 0..GROUPS {
        for chunk in (0..ROWS).collect::<Vec<_>>().chunks(16) {
            let mut batch = WriteBatch::new();
            for &r in chunk {
                batch.put(&group_key(g, &r.to_be_bytes()), &[0x5A; 100]);
            }
            mem.insert(&batch, seq, &[]).unwrap();
            seq += u64::from(batch.count());
        }
    }
    let n = GROUPS * ROWS;
    let insert_ns = start.elapsed().as_nanos() as f64 / n as f64;
    let snapshot = seq - 1;

    let mut rng = Rng::new(18);
    let lookups = 1_000_000;
    let probe_keys: Vec<Vec<u8>> = (0..1024)
        .map(|_| group_key(rng.below(GROUPS), &rng.below(ROWS).to_be_bytes()))
        .collect();
    let start = nabedra_rt::time::now_std();
    let mut hits = 0;
    for i in 0..lookups {
        if matches!(mem.get(&probe_keys[i % 1024], snapshot), Lookup::Found(..)) {
            hits += 1;
        }
    }
    let get_ns = start.elapsed().as_nanos() as f64 / lookups as f64;
    assert_eq!(hits, lookups);

    // Calibration on this machine: a plain BTreeMap of encoded internal keys.
    let mut plain: BTreeMap<Vec<u8>, Bytes> = BTreeMap::new();
    for g in 0..GROUPS {
        for r in 0..ROWS {
            plain.insert(
                internal_key(&group_key(g, &r.to_be_bytes()), g * ROWS + r + 1, Kind::Put),
                Bytes::from_static(&[0x5A; 100]),
            );
        }
    }
    let plain_probes: Vec<Vec<u8>> = probe_keys.iter().map(|k| seek_key(k, snapshot)).collect();
    let start = nabedra_rt::time::now_std();
    let mut plain_hits = 0;
    for i in 0..lookups {
        let probe = &plain_probes[i % 1024];
        if plain.range::<Vec<u8>, _>(probe..).next().is_some() {
            plain_hits += 1;
        }
    }
    let plain_ns = start.elapsed().as_nanos() as f64 / lookups as f64;
    assert_eq!(plain_hits, lookups);

    let sources = vec![Source::Mem(mem.clone())];
    let resolver = Arc::new(FakeResolver::default());
    let mut scanned = 0u64;
    let scans = 20_000;
    let start = nabedra_rt::time::now_std();
    for i in 0..scans {
        let g = i % GROUPS;
        let (lo, hi) = (group_key(g, b""), group_key(g + 1, b""));
        let mut it = iter_over(
            &sources,
            snapshot,
            Bound::Included(&lo),
            Bound::Excluded(&hi),
            resolver.clone(),
        );
        it.seek(&group_key(g, &rng.below(ROWS - 128).to_be_bytes()));
        for _ in 0..128 {
            assert!(it.valid());
            scanned += it.value().unwrap().len() as u64 / 100;
            it.next();
        }
    }
    let scan_elapsed = start.elapsed().as_nanos() as f64;
    let scan_ns = scan_elapsed / (scans * 128) as f64;
    let seek_ns = scan_elapsed / scans as f64;

    let start = nabedra_rt::time::now_std();
    let mut it = iter_over(
        &sources,
        snapshot,
        Bound::Unbounded,
        Bound::Unbounded,
        resolver,
    );
    it.seek_to_first();
    let mut full = 0u64;
    while it.valid() {
        full += 1;
        it.next();
    }
    let full_ns = start.elapsed().as_nanos() as f64 / full as f64;
    assert_eq!(full, n);
    assert_eq!(scanned, scans * 128);
    eprintln!(
        "ns18 memtable ({n} keys, 16 shards): insert {insert_ns:.0} ns/op, get {get_ns:.0} ns/op \
         (plain BTreeMap<Vec<u8>> range lookup: {plain_ns:.0} ns/op), \
         bounded seek+128 {seek_ns:.0} ns/scan ({scan_ns:.0} ns/entry), \
         unbounded merged scan {full_ns:.0} ns/entry, memory {} MiB",
        mem.approximate_memory() >> 20
    );
}
