//! Adversarial tests of the rules of plan section 5 and `LocalIndex.tla` at the lowest level that fails
//! when a rule goes: the row map, the stop rule, the applied position, the edges of a read point, the
//! feed in any shape, and one differential test of every retained cut against brute force, with pins
//! and scans held across churn and two codecs whose estimates differ.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

use super::super::codec::{dot, exact, goodness};
use super::*;

const DIM: usize = 4;
const COLS: [ColumnType; 2] = [ColumnType::Int, ColumnType::Text];

fn kv(hits: &[Hit]) -> Vec<(u64, u32)> {
    let k = |h: &Hit| u64::from_be_bytes((&*h.key).try_into().unwrap());
    hits.iter().map(|h| (k(h), h.score.to_bits())).collect()
}

// ----- The row map.

/// A slot is reused only when no entry, no copy in flight and no checkpoint names a version of it,
/// and a slot of a dead generation too.
#[test]
fn adv20_a_slot_is_reused_only_when_nothing_names_it() {
    let live = |n: u64| Some(Arc::new(row(&[1.0, 0.0], Vec::new(), n)));
    let mut rows = Rows::default();
    let mut keys = BTreeMap::new();
    let (s, _) = rows.insert(&mut keys, 1, &key(1), (1, 10, live(1))).unwrap();
    rows.name(s, 1); // The entry.
    rows.name(s, 1); // A copy in flight, or a checkpoint.
    rows.insert(&mut keys, 1, &key(1), (2, 20, None)).unwrap();
    rows.trim(Some(&mut keys), s, 9);
    let fresh = |rows: &mut Rows, keys: &mut BTreeMap<Key, Slot>, k: u64| rows.insert(keys, 1, &key(k), (3 + k, 30 + k, live(k))).unwrap().0;
    assert_ne!(fresh(&mut rows, &mut keys, 2), s, "named twice");
    rows.forget(Some(&mut keys), s, 1, 9);
    assert_ne!(fresh(&mut rows, &mut keys, 3), s, "named once");
    assert!(rows.census().iter().any(|c| c.slot == s));
    rows.forget(Some(&mut keys), s, 1, 9);
    assert!(rows.census().iter().all(|c| c.slot != s), "nothing names it and no cut sees it");
    assert_eq!(fresh(&mut rows, &mut keys, 4), s, "and then it is reused");

    // A void generation keeps what is named, and nothing else.
    let mut rows = Rows::default();
    let mut keys = BTreeMap::new();
    let (s, _) = rows.insert(&mut keys, 1, &key(1), (1, 10, live(1))).unwrap();
    rows.name(s, 1);
    rows.kill(s);
    assert_ne!(fresh(&mut rows, &mut BTreeMap::new(), 2), s, "named by a copy of a dead generation");
    rows.forget(None, s, 1, 0);
    assert_eq!(fresh(&mut rows, &mut BTreeMap::new(), 3), s);
}

/// A retained cut keeps a superseded version alive, and the version is gone once the cut is past it.
#[test]
fn adv20_a_superseded_version_lives_until_retention_passes_its_successor() {
    let live = |n: u64| Some(Arc::new(row(&[1.0, 0.0], Vec::new(), n)));
    let mut rows = Rows::default();
    let mut keys = BTreeMap::new();
    let (s, _) = rows.insert(&mut keys, 1, &key(1), (1, 10, live(1))).unwrap();
    rows.insert(&mut keys, 1, &key(1), (5, 50, live(2))).unwrap();
    for low in 0..5 {
        rows.trim(Some(&mut keys), s, low);
        assert!(rows.version(s, 1).is_some(), "a cut at {low} still sees the first version");
    }
    rows.trim(Some(&mut keys), s, 5);
    assert!(rows.version(s, 1).is_none(), "the cut at 5 sees the second one");
    assert!(rows.version(s, 5).is_some());
    // The visible version is the one that is not superseded at the point.
    let at = |pos, cut| Point { pos, cut };
    assert!(rows.visible(s, 5, at(5, 50)));
    assert!(!rows.visible(s, 5, at(4, u64::MAX)), "not applied yet at 4");
    assert!(!rows.visible(s, 5, at(9, 49)), "not committed at 49");
}

// ----- The stop rule.

fn enough(limit: usize, recall: f64, units: usize, origins: &[usize]) -> bool {
    RecallEstimate.enough(&Progress { limit, recall, units, origins })
}

/// The estimate is `1 - hits of the last window / limit` and the scan stops when it reaches the recall,
/// with a window of 16 units or half of what was read.
#[test]
fn adv20_the_stop_rule_stops_exactly_at_its_recall() {
    let early: Vec<usize> = vec![0, 1, 2, 3, 4, 5, 6, 7];
    // Too little read to judge, whatever the recall.
    assert!(!enough(8, 0.0, 15, &early));
    assert!(!enough(8, 0.0, 0, &early));
    // 16 units: the window is all of them, so every best row is late.
    assert!(enough(8, 0.0, 16, &early));
    assert!(!enough(8, 0.125, 16, &early));
    // 40 units: the window is 20, from unit 20 on.
    assert!(enough(8, 1.0, 40, &early));
    let mut two = early.clone();
    two[0] = 25;
    two[1] = 39;
    assert!(enough(8, 0.75, 40, &two));
    assert!(!enough(8, 0.7501, 40, &two));
    // A row from the first unit of the window is late, one from the unit before it is not.
    let mut edge = early.clone();
    edge[0] = 20;
    assert!(enough(8, 0.875, 40, &edge) && !enough(8, 0.8751, 40, &edge));
    edge[0] = 19;
    assert!(enough(8, 1.0, 40, &edge));
    // The window grows with what was read.
    let mut wide = early.clone();
    wide[0] = 99;
    assert!(enough(8, 1.0, 200, &wide), "unit 99 is outside the last 100 of 200");
    wide[0] = 100;
    assert!(!enough(8, 1.0, 200, &wide) && enough(8, 0.875, 200, &wide));
    // No panic, and no division by zero, for a limit of nothing.
    assert!(enough(0, 0.95, 40, &[]));
}

/// A stop rule is asked only while `limit` rows are there, after every unit that held a match, and
/// `units` counts those units and no other.
#[test]
fn adv20_the_stop_rule_is_asked_after_every_unit_with_a_match_and_only_then() {
    struct Spy(Arc<Mutex<Vec<(usize, usize, Vec<usize>)>>>);
    impl StopRule for Spy {
        fn enough(&mut self, p: &Progress<'_>) -> bool {
            self.0.lock().unwrap().push((p.limit, p.units, p.origins.to_vec()));
            false
        }
    }
    let cfg = Config { flat: 0, part_min: 4, part_max: 16, ..Config::new(schema(2, &[ColumnType::Int], None), 2) };
    let mut ix = serving(cfg, ExactCodec, None);
    let write = |k: u64| {
        let a = k as f32 / 160.0;
        (k, Some(row(&[a.cos(), a.sin()], vec![Value::Int(k as i64 / 100)], k)))
    };
    feed(&mut ix, RANGE, &page(0, (0..1000).map(write).collect()));
    settle(&mut ix);
    let pin = latest(&mut ix, None);
    let limit = 5;
    let q = Query { filter: Some(Filter::Eq(0, Value::Int(3))), exact: false, ..query(Op::Cosine, &[1.0, 0.0], limit) };
    let asked = Arc::new(Mutex::new(Vec::new()));
    let mut scan = ix.scan(&pin, &q, Box::new(Spy(asked.clone()))).unwrap();
    while ix.visit(&mut scan).unwrap() {}
    let total = ix.listed().len();
    let hits = ix.finish(scan).unwrap();
    assert_eq!(hits.len(), limit);
    let asked = asked.lock().unwrap();
    assert!(!asked.is_empty());
    for (i, (l, units, origins)) in asked.iter().enumerate() {
        assert_eq!(*l, limit);
        assert_eq!(origins.len(), limit, "asked before `limit` rows were there");
        assert!(origins.iter().all(|u| u <= units));
        if i > 0 {
            assert_eq!(*units, asked[i - 1].1 + 1, "every unit with a match asks once, a unit without a match never");
        }
    }
    assert!(asked.last().unwrap().1 < total, "units that held no match were counted: {} of {total}", asked.last().unwrap().1);
}

// ----- Positions.

fn record(at: Pos, k: u64) -> Record {
    let change = Change::Row { key: key(k), commit_ts: at * 10, row: Some(row(&[1.0 + k as f32, 0.5], vec![Value::Int(0)], k)) };
    Record { at, change }
}

/// A position is applied only when every core finished all of its entries, and not before an earlier
/// position is whole, however the cores interleave. A scan never sees half of a position.
#[test]
fn adv20_a_position_counts_as_applied_only_when_every_entry_of_it_is_in() {
    let cfg = Config { queue: 64, ..Config::new(schema(2, &[ColumnType::Int], None), 1) };
    let mut ix = serving(cfg, ExactCodec, None);
    let records = vec![record(1, 1), record(1, 2), record(1, 3), record(2, 4)];
    assert_eq!(ix.apply(RANGE, &Page { after: 0, through: 2, records }), Ok(2));
    ix.advance();
    assert_eq!(ix.status(RANGE).unwrap().applied, 0, "nothing is in");
    ix.pump();
    // The core takes the entry of position 2 first.
    ix.shared.queues[0].rotate_right(1);
    let seen = |ix: &mut Index| {
        let pin = latest(ix, None);
        ix.search(&pin, &query(Op::Dot, &[1.0, 1.0], 10)).unwrap().len()
    };
    for done in 1..=4 {
        assert!(ix.step(0));
        ix.advance();
        let want = if done < 4 { 0 } else { 2 };
        assert_eq!(ix.status(RANGE).unwrap().applied, want, "after {done} of 4 entries");
        assert_eq!(seen(&mut ix), if done < 4 { 0 } else { 4 }, "after {done} of 4 entries");
    }
    // And with the entries of the second position first and a gap in the positions.
    let records = vec![record(4, 5), record(4, 6), record(6, 7)];
    assert_eq!(ix.apply(RANGE, &Page { after: 2, through: 6, records }), Ok(6));
    ix.pump();
    ix.shared.queues[0].rotate_right(1);
    assert!(ix.step(0));
    ix.advance();
    assert_eq!(ix.status(RANGE).unwrap().applied, 3, "position 4 has one entry left");
    assert!(ix.step(0) && ix.step(0));
    ix.advance();
    assert_eq!(ix.status(RANGE).unwrap().applied, 6);
}

/// The edges of every read point: the oldest and the newest of each kind are served, the next ones out
/// are refused with the number that tells the caller where it is.
#[test]
fn adv20_every_read_point_is_exact_at_its_edges() {
    let cfg = Config::new(schema(2, &[ColumnType::Int], None), 2);
    let mut ix = serving(cfg, ExactCodec, None);
    let mut records: Vec<Record> = Vec::new();
    for at in 1..=10u64 {
        // Key 1 is written again at 3, so its first version goes at 3 and its commit timestamp is 30.
        let k = if at == 3 || at == 1 { 1 } else { at };
        records.push(Record {
            at,
            change: Change::Row { key: key(k), commit_ts: at * 10, row: Some(row(&[1.0, at as f32], vec![Value::Int(0)], at)) },
        });
    }
    records.push(Record { at: 11, change: Change::Closed(100) });
    feed(&mut ix, RANGE, &Page { after: 0, through: 11, records });
    settle(&mut ix);
    ix.advance_low(RANGE, 4).unwrap();
    let s = ix.status(RANGE).unwrap();
    assert_eq!((s.applied, s.low, s.bound, s.closed), (11, 4, 30, 100));
    let pin = |ix: &mut Index, at| ix.pin(&[(RANGE, at)], None).map(|p| p.ranges().to_vec());
    let got = |ix: &mut Index, at| {
        let p = ix.pin(&[(RANGE, at)], None).unwrap();
        let mut v = images(&ix.search(&p, &query(Op::Dot, &[1.0, 1.0], 20)).unwrap());
        v.sort_unstable();
        v
    };
    assert_eq!(pin(&mut ix, ReadAt::Position(3)), Err(Error::CutTooOld { oldest: 4 }));
    assert_eq!(pin(&mut ix, ReadAt::Position(12)), Err(Error::CutTooNew { newest: 11 }));
    assert_eq!(pin(&mut ix, ReadAt::Cut(29)), Err(Error::CutTooOld { oldest: 30 }));
    assert_eq!(pin(&mut ix, ReadAt::Cut(101)), Err(Error::CutTooNew { newest: 100 }));
    assert_eq!(pin(&mut ix, ReadAt::Latest { barrier: 12 }), Err(Error::Behind { range: RANGE, applied: 11 }));
    assert_eq!(pin(&mut ix, ReadAt::Latest { barrier: 11 }), Ok(vec![(RANGE, 11)]));
    assert_eq!(got(&mut ix, ReadAt::Position(4)), vec![2, 3, 4], "version 1 is superseded at 3");
    assert_eq!(got(&mut ix, ReadAt::Position(11)), vec![2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(got(&mut ix, ReadAt::Cut(30)), vec![2, 3]);
    assert_eq!(got(&mut ix, ReadAt::Cut(100)), vec![2, 3, 4, 5, 6, 7, 8, 9, 10]);
    // Retention cannot pass what is applied, and does not go back.
    assert_eq!(ix.advance_low(RANGE, 12), Err(Error::CutTooNew { newest: 11 }));
    ix.advance_low(RANGE, 2).unwrap();
    assert_eq!(ix.status(RANGE).unwrap().low, 4);
    // A range that is not held, loading or not yet caught up serves nothing.
    let other = RangeId(99);
    assert_eq!(ix.pin(&[(other, ReadAt::Latest { barrier: 0 })], None).err(), Some(Error::NotHeld(other)));
    ix.begin_load(other, 3, 0).unwrap();
    assert!(matches!(ix.pin(&[(other, ReadAt::Latest { barrier: 0 })], None), Err(Error::Behind { .. })));
    ix.finish_load(other).unwrap();
    assert!(matches!(ix.pin(&[(other, ReadAt::Latest { barrier: 0 })], None), Err(Error::Behind { .. })), "no fresh barrier yet");
    assert_eq!(ix.caught_up(other, 4), Err(Error::Behind { range: other, applied: 3 }));
    ix.caught_up(other, 3).unwrap();
    assert!(ix.pin(&[(other, ReadAt::Latest { barrier: 3 })], None).is_ok());
}

// ----- A feed of any shape.

/// Pages that overlap what was taken, repeat, end early, meet a full pipeline or skip ahead give the
/// index of one page in order, and a page that skips or is out of order takes nothing.
#[test]
fn adv21_pages_of_any_shape_give_the_index_of_the_feed_in_order() {
    let runs = Runs { cases: 150, ..Runs::default() };
    check_with(runs, "adv21_pages_of_any_shape", |g| {
        let (records, recs, last, _) = { let n = 6 + g.below(20); history(g, n) };
        let cfg = Config {
            queue: 2 + g.below(6) as usize,
            max_flight: 1 + g.below(3) as usize,
            max_flight_rows: 2 + g.below(5) as usize,
            ..Config::new(schema(DIM, &COLS, None), 1 + g.below(3) as usize)
        };
        let mut reference = serving(cfg.clone(), ExactCodec, None);
        feed(&mut reference, RANGE, &Page { after: 0, through: last, records: records.clone() });
        reference.run();
        let mut ix = serving(cfg, ExactCodec, None);
        let mut delivered = 0;
        for _ in 0..4000 {
            if delivered == last {
                break;
            }
            let (back, ahead) = (g.below(delivered.min(5) + 1), g.below(9));
            let after = delivered - back;
            let through = (after + 1 + ahead).min(last);
            let take = |lo: Pos, hi: Pos| Page { after: lo, through: hi, records: records.iter().filter(|r| lo < r.at && r.at <= hi).cloned().collect() };
            match g.below(8) {
                0 if delivered + 1 < last => {
                    // Skips a position.
                    let before = ix.status(RANGE).unwrap();
                    let skip = delivered + 1 + g.below(last - delivered - 1);
                    if ix.apply(RANGE, &take(skip, last)) != Err(Error::Gap { delivered }) {
                        return Err("a page that skips is not a gap".into());
                    }
                    if ix.status(RANGE).unwrap() != before {
                        return Err("a gap took something".into());
                    }
                }
                1 if delivered > 0 => {
                    // Out of order.
                    let before = ix.status(RANGE).unwrap();
                    let mut page = take(0, delivered + 1);
                    page.records.reverse();
                    if page.records.windows(2).all(|w| w[0].at == w[1].at) {
                        continue;
                    }
                    if !matches!(ix.apply(RANGE, &page), Err(Error::InvalidFeed(_))) {
                        return Err("a page out of order was taken".into());
                    }
                    if ix.status(RANGE).unwrap() != before {
                        return Err("a page out of order took something".into());
                    }
                }
                _ => match ix.apply(RANGE, &take(after, through)) {
                    Ok(at) => {
                        if at < delivered || at > through.max(delivered) {
                            return Err(format!("took {at} of {after}..{through}, had {delivered}"));
                        }
                        delivered = at;
                    }
                    Err(Error::Busy) => {
                        if ix.status(RANGE).unwrap().delivered != delivered {
                            return Err("Busy took something".into());
                        }
                    }
                    Err(e) => return Err(format!("{after}..{through} after {delivered}: {e}")),
                },
            }
            if ix.status(RANGE).unwrap().delivered != delivered {
                return Err("the position taken is not the one reported".into());
            }
            for _ in 0..g.below(4) {
                ix.step(g.below(4) as usize);
                ix.run();
            }
        }
        if delivered != last {
            return Err(format!("the feed stopped at {delivered} of {last}"));
        }
        ix.run();
        let (a, b) = (ix.status(RANGE).unwrap(), reference.status(RANGE).unwrap());
        if (a.applied, a.closed, a.delivered) != (b.applied, b.closed, b.delivered) {
            return Err(format!("{a:?} against {b:?}"));
        }
        for (op, v) in [(Op::Dot, [1.0, 0.5, 0.0, 0.25]), (Op::L2, [0.0, 0.5, 1.0, 0.0]), (Op::Cosine, [1.0, 1.0, 1.0, 1.0])] {
            let q = query(op, &v, 30);
            let (pa, pb) = (latest(&mut ix, None), latest(&mut reference, None));
            let (ha, hb) = (ix.search(&pa, &q).map_err(|e| e.to_string())?, reference.search(&pb, &q).map_err(|e| e.to_string())?);
            if kv(&ha) != kv(&hb) {
                return Err(format!("{op:?}: {:?} against {:?}", kv(&ha), kv(&hb)));
            }
            let want = state(&recs, last, u64::MAX);
            if kv(&ha) != best(&want, &q) {
                return Err(format!("{op:?} is not brute force"));
            }
        }
        Ok(())
    });
}

// ----- Every retained cut against brute force.

#[derive(Clone)]
struct Rec {
    at: Pos,
    ts: CommitTs,
    key: u64,
    row: Option<Row>,
}

const KEYS: u64 = 20;

fn vector(g: &mut Gen) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 1.0, 0.25, 7.0, 1e3, 1e-4, 1e30, 1e-42, 0.0, 3e38]);
    (0..DIM).map(|_| g.range_i64(-8, 8) as f32 / 8.0 * scale).collect()
}

fn cols(g: &mut Gen) -> Vec<Value> {
    vec![Value::Int(g.below(3) as i64), Value::Text(Arc::from(*g.pick(&["a", "b", "ab"])))]
}

/// A history of `positions` writes, some with several rows and some closing a timestamp, as the feed,
/// as the model, the last position and the closes.
fn history(g: &mut Gen, positions: u64) -> (Vec<Record>, Vec<Rec>, Pos, Vec<(Pos, CommitTs)>) {
    let (mut records, mut recs, mut closes) = (Vec::new(), Vec::new(), Vec::new());
    let mut at = 0;
    for _ in 0..positions {
        at += 1;
        let ts = at * 10;
        let mut used = BTreeSet::new();
        for _ in 0..1 + g.below(4) {
            let k = g.below(KEYS);
            if !used.insert(k) {
                continue;
            }
            let value = match g.below(8) {
                0 => None,
                1 => Some(Row { vec: None, cols: cols(g), image: Arc::new([]) }),
                _ => Some(row(&vector(g), cols(g), at * 100 + k)),
            };
            records.push(Record { at, change: Change::Row { key: key(k), commit_ts: ts, row: value.clone() } });
            recs.push(Rec { at, ts, key: k, row: value });
        }
        if g.chance(1, 3) {
            at += 1;
            records.push(Record { at, change: Change::Closed(ts) });
            closes.push((at, ts));
        }
    }
    (records, recs, at, closes)
}

/// What a pin at (`pos`, `cut`) shows.
fn state(recs: &[Rec], pos: Pos, cut: CommitTs) -> BTreeMap<u64, Row> {
    let mut m = BTreeMap::new();
    for r in recs.iter().filter(|r| r.at <= pos && r.ts <= cut) {
        match r.row.clone().filter(|x| x.vec.is_some()) {
            Some(x) => m.insert(r.key, x),
            None => m.remove(&r.key),
        };
    }
    m
}

/// Brute force: the best rows that match, best first and then by key, with the bits of their scores.
fn best(rows: &BTreeMap<u64, Row>, q: &Query) -> Vec<(u64, u32)> {
    let matching = rows.iter().filter(|(_, r)| q.filter.as_ref().is_none_or(|f| f.matches(&r.cols)));
    let mut all: Vec<(u64, f32)> = matching.filter_map(|(k, r)| Some((*k, exact(q.op, r.vec.as_deref()?, &q.vec)?))).collect();
    all.sort_by(|a, b| goodness(q.op, b.1).total_cmp(&goodness(q.op, a.1)).then(a.0.cmp(&b.0)));
    all.truncate(q.limit);
    all.into_iter().map(|(k, s)| (k, s.to_bits())).collect()
}

/// Estimates as wrong as their bound allows, and not those of `ExactCodec`: another kernel level.
#[derive(Clone, Copy)]
struct Wobble;

fn wobble(x: &[f32]) -> f32 {
    let bits = x.iter().fold(7u32, |h, v| (h ^ v.to_bits()).wrapping_mul(0x9e37_79b1));
    (bits >> 16) as f32 / 32_768.0 - 1.0
}

impl Codec for Wobble {
    type Block = Vec<Vec<f32>>;
    type Query = Vec<f32>;
    fn rotate(&self, dir: &[f32]) -> Vec<f32> {
        dir.to_vec()
    }
    fn block(&self) -> Self::Block {
        Vec::new()
    }
    fn push(&self, block: &mut Self::Block, dir: &[f32], _: &[f32], _: f32) {
        block.push(dir.to_vec());
    }
    fn swap_remove(&self, block: &mut Self::Block, i: usize) {
        block.swap_remove(i);
    }
    fn query(&self, dir: &[f32], _: &[f32]) -> Self::Query {
        dir.to_vec()
    }
    fn scan(&self, q: &Self::Query, block: &Self::Block, out: &mut Vec<Estimate>) {
        out.extend(block.iter().map(|d| Estimate { cos: dot(q, d) - 0.1 * wobble(d), err: 0.1001 }));
    }
    fn refine(&self, q: &Self::Query, block: &Self::Block, i: usize) -> Estimate {
        Estimate { cos: dot(q, &block[i]) + 0.02 * wobble(&block[i]), err: 0.0201 }
    }
}

fn random_filter(g: &mut Gen) -> Option<Filter> {
    let text = |t: &str| Value::Text(Arc::from(t));
    match g.below(8) {
        0..=2 => None,
        3 => Some(Filter::Eq(0, Value::Int(g.below(3) as i64))),
        4 => Some(Filter::Not(Box::new(Filter::Eq(0, Value::Int(g.below(3) as i64))))),
        5 => Some(Filter::StartsWith(1, Arc::from("a"))),
        6 => Some(Filter::And(vec![Filter::Eq(1, text("a")), Filter::Range(0, Bound::Included(Value::Int(1)), Bound::Unbounded)])),
        _ => Some(Filter::Eq(1, text("b"))),
    }
}

struct Held {
    pin: Pin,
    pos: Pos,
    cut: CommitTs,
    filter: Option<Filter>,
    scan: Option<(Scan, Vec<(u64, u32)>)>,
}

fn random_query(g: &mut Gen, filter: &Option<Filter>, exact: bool) -> Option<Query> {
    let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
    let vec = vector(g);
    (op != Op::Cosine || vec.iter().any(|x| *x != 0.0)).then(|| Query { filter: filter.clone(), exact, ..query(op, &vec, 1 + g.below(12) as usize) })
}

/// One run of a history against an index with this codec. Returns the exact answers, which must not
/// depend on the codec.
fn scenario<C: Codec>(seed: u64, codec: C) -> std::result::Result<Vec<(u64, u32)>, String> {
    let g = &mut Gen::from_seed(seed);
    let part_max = 4 + g.below(5) as usize;
    let by = [None, Some(0), Some(1)][g.below(3) as usize];
    let flat = if g.bool() { 0 } else { 20_000 };
    let cfg = Config {
        flat,
        part_min: g.below(part_max as u64 / 2) as usize,
        part_max,
        queue: 1 + g.below(15) as usize,
        max_flight: 1 + g.below(4) as usize,
        max_flight_rows: 3 + g.below(30) as usize,
        copies: 1 + g.below(6) as usize,
        reassigns: g.below(5) as usize,
        summary_pairs: if g.bool() { 1 << 22 } else { 4 },
        ..Config::new(schema(DIM, &COLS, by), 1 + g.below(3) as usize)
    };
    let shards = cfg.shards as u64;
    let (records, recs, last, _) = { let n = 8 + g.below(25); history(g, n) };
    let mut ix = serving(cfg, codec, None);
    let mut held: Vec<Held> = Vec::new();
    let mut out = Vec::new();
    let mut at = 0;
    while at < last {
        let to = (at + 1 + g.below(4)).min(last);
        let page = Page { after: at, through: to, records: records.iter().filter(|r| at < r.at && r.at <= to).cloned().collect() };
        feed(&mut ix, RANGE, &page);
        at = to;
        for _ in 0..g.below(10) {
            match g.below(5) {
                0 => {
                    ix.maintain(1 + g.below(4) as usize);
                }
                1 => {
                    ix.step(g.below(shards) as usize);
                }
                2 => ix.run(),
                3 => {
                    let s = ix.status(RANGE).map_err(|e| e.to_string())?;
                    let _ = ix.advance_low(RANGE, s.low + g.below(s.applied - s.low + 1));
                }
                _ => {
                    // The scans in flight read one more partition, while everything moves.
                    for h in held.iter_mut() {
                        let Some((scan, _)) = &mut h.scan else { continue };
                        if !ix.visit(scan).map_err(|e| e.to_string())? {
                            let (scan, want) = h.scan.take().unwrap();
                            let hits = kv(&ix.finish(scan).map_err(|e| e.to_string())?);
                            if hits != want {
                                return Err(format!("a scan that moves with the index: {hits:?}, brute force {want:?}"));
                            }
                        }
                    }
                }
            }
        }
        // A new pin at a point that is served, with a scan of it that stays open.
        if g.chance(2, 3) {
            ix.run();
            let s = ix.status(RANGE).map_err(|e| e.to_string())?;
            let filter = random_filter(g);
            let (read, pos, cut) = match g.below(3) {
                0 => (ReadAt::Latest { barrier: 0 }, s.applied, u64::MAX),
                1 => {
                    let p = s.low + g.below(s.applied - s.low + 1);
                    (ReadAt::Position(p), p, u64::MAX)
                }
                _ if s.closed >= s.bound => {
                    let ts = s.bound + g.below(s.closed - s.bound + 1);
                    (ReadAt::Cut(ts), s.applied, ts)
                }
                _ => (ReadAt::Latest { barrier: 0 }, s.applied, u64::MAX),
            };
            let pin = ix.pin(&[(RANGE, read)], filter.as_ref()).map_err(|e| format!("{read:?} of {s:?}: {e}"))?;
            let scan = match random_query(g, &filter, true) {
                Some(q) => {
                    let want = best(&state(&recs, pos, cut), &q);
                    Some((ix.scan(&pin, &q, Box::new(RecallEstimate)).map_err(|e| e.to_string())?, want))
                }
                None => None,
            };
            held.push(Held { pin, pos, cut, filter, scan });
        }
        if g.chance(1, 4) && !held.is_empty() {
            held.swap_remove(g.below(held.len() as u64) as usize);
        }
        // Every pin that is held still shows its own point, whatever was collected since.
        if g.chance(1, 3) {
            for h in &held {
                check_pin(g, &ix, h, &recs, flat, &mut out)?;
            }
        }
    }
    ix.run();
    for h in &mut held {
        while let Some((scan, _)) = &mut h.scan {
            if !ix.visit(scan).map_err(|e| e.to_string())? {
                let (scan, want) = h.scan.take().unwrap();
                let hits = kv(&ix.finish(scan).map_err(|e| e.to_string())?);
                if hits != want {
                    return Err(format!("a scan that moves with the index: {hits:?}, brute force {want:?}"));
                }
                out.extend(hits);
            }
        }
        check_pin(g, &ix, h, &recs, flat, &mut out)?;
    }
    // Nothing is held: everything that was named is let go, and nothing is lost.
    drop(held);
    ix.run();
    let s = ix.status(RANGE).map_err(|e| e.to_string())?;
    ix.advance_low(RANGE, s.applied).map_err(|e| e.to_string())?;
    settle(&mut ix);
    let rows = state(&recs, s.applied, u64::MAX);
    let census = ix.shared.rows.census();
    // Every name is an entry that is there (a version that was copied twice into one partition is
    // B-D6, and has its own test), and nothing else is left.
    let entries = |slot: Slot, at: Pos| -> u32 {
        let all = ix.shards.iter().flat_map(|s| s.parts.values()).flat_map(|p| p.entries());
        all.filter(|e| (e.slot, e.at) == (slot, at)).count() as u32
    };
    let odd = |c: &super::super::rows::Census| c.versions.len() != 1 || c.versions[0].1 == 0 || c.versions[0].1 != entries(c.slot, c.versions[0].0);
    if census.len() != rows.len() || census.iter().any(odd) {
        let held: Vec<_> = census.iter().filter(|c| odd(c)).map(|c| (c.slot, c.versions.clone())).collect();
        let mut at_: Vec<String> = Vec::new();
        for (slot, _) in &held {
            for shard in &ix.shards {
                for (part, p) in &shard.parts {
                    for e in p.entries().filter(|e| e.slot == *slot) {
                        let state = ix.shared.dir.get(part).map(|m| (m.state, m.entries, m.leaving, m.arriving));
                        at_.push(format!("slot {slot} at {} id {} in {part:?} {state:?}", e.at, e.id));
                    }
                }
            }
        }
        let queued: usize = ix.shared.queues.iter().map(|q| q.len()).sum();
        return Err(format!("{} slots for {} rows, a name without its entry or an entry without its name: {held:?} {at_:?} inbox {} queued {queued}", census.len(), rows.len(), ix.shared.inbox.len()));
    }
    let pin = latest(&mut ix, None);
    for op in [Op::Dot, Op::L2, Op::Cosine] {
        let q = query(op, &[0.5, 1.0, 0.25, 1.0], 12);
        let got = kv(&ix.search(&pin, &q).map_err(|e| e.to_string())?);
        if got != best(&rows, &q) {
            return Err(format!("{op:?} after settling: {got:?}"));
        }
    }
    Ok(out)
}

fn check_pin<C: Codec>(g: &mut Gen, ix: &Index<C>, h: &Held, recs: &[Rec], flat: usize, out: &mut Vec<(u64, u32)>) -> std::result::Result<(), String> {
    let rows = state(recs, h.pos, h.cut);
    for exact in [true, false] {
        let Some(q) = random_query(g, &h.filter, exact) else { continue };
        let want = best(&rows, &q);
        let got = kv(&ix.search(&h.pin, &q).map_err(|e| format!("{q:?}: {e}"))?);
        let keys: BTreeSet<u64> = got.iter().map(|x| x.0).collect();
        if keys.len() != got.len() {
            return Err(format!("a row twice: {got:?}"));
        }
        if exact || flat > 0 {
            if got != want {
                return Err(format!("{q:?} at ({}, {}): {got:?}, brute force {want:?}", h.pos, h.cut));
            }
            out.extend(got);
            continue;
        }
        // Approximate: `limit` rows if that many match, each one matches and has its exact score.
        let all = best(&rows, &Query { limit: 1000, ..q.clone() });
        if got.len() != all.len().min(q.limit) || got.iter().any(|x| !all.contains(x)) {
            return Err(format!("approximate {q:?} at ({}, {}): {got:?} of {all:?}", h.pos, h.cut));
        }
    }
    Ok(())
}

/// VEC-G05 and VEC-G03: at every retained point, exact search is brute force over what is visible
/// there, for pins and scans that stay open while the index splits, merges, moves and collects, and
/// two codecs give the same answers to the bit (L1).
#[test]
fn adv21_every_retained_point_equals_brute_force_with_pins_open_and_two_codecs() {
    let runs = Runs { cases: 120, ..Runs::default() };
    check_with(runs, "adv21_every_retained_point", |g| {
        let seed = g.u64();
        let a = scenario(seed, ExactCodec).map_err(|e| format!("seed {seed}, exact codec: {e}"))?;
        let b = scenario(seed, Wobble).map_err(|e| format!("seed {seed}, wobbling codec: {e}"))?;
        if a != b {
            return Err(format!("seed {seed}: two codecs, two answers"));
        }
        Ok(())
    });
}

// ----- Ties and abandoned readers.

/// Equal scores come back in key order, whatever the order of arrival, the number of cores and the
/// layout of the partitions. Rows come in mirror pairs, which score alike, and the keys are shuffled.
#[test]
fn adv22_equal_scores_are_ordered_by_key_in_every_layout() {
    for (seed, shards) in [(1u64, 1usize), (2, 3), (3, 4)] {
        let g = &mut Gen::from_seed(seed);
        let cfg = Config { flat: if seed == 2 { 0 } else { 20_000 }, part_min: 2, part_max: 8, ..Config::new(schema(2, &[ColumnType::Int], None), shards) };
        let mut ix = serving(cfg, ExactCodec, None);
        let mut keys: Vec<u64> = (0..300).collect();
        for i in (1..keys.len()).rev() {
            keys.swap(i, g.below(i as u64 + 1) as usize);
        }
        let mut rows = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            let a = (i / 2) as f32 * 0.02 + 0.01;
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            rows.insert(*k, row(&[a.cos(), sign * a.sin()], vec![Value::Int(0)], *k));
        }
        let changes = keys.iter().map(|k| (*k, Some(rows[k].clone()))).collect();
        feed(&mut ix, RANGE, &page(0, changes));
        settle(&mut ix);
        let pin = latest(&mut ix, None);
        for op in [Op::Dot, Op::L2, Op::Cosine] {
            for exact in [true, false] {
                let q = Query { exact, ..query(op, &[1.0, 0.0], 7) };
                let got = kv(&ix.search(&pin, &q).unwrap());
                let want = best(&rows, &q);
                let pairs = want.windows(2).filter(|w| w[0].1 == w[1].1).count();
                assert!(pairs >= 3, "the rows do not tie: {want:?}");
                assert_eq!(got, want, "seed {seed} {op:?} exact {exact}");
            }
        }
    }
}

/// Pins and scans that are dropped hold nothing back, and the list of readers does not grow with
/// the number that came and went.
#[test]
fn adv22_readers_that_ended_hold_nothing_and_cost_nothing() {
    let cfg = Config { part_min: 2, part_max: 8, ..Config::new(schema(2, &[ColumnType::Int], None), 2) };
    let mut ix = serving(cfg, ExactCodec, None);
    let write = |k: u64, n: u64| (k, Some(row(&[(k + n) as f32, 1.0], vec![Value::Int(0)], n * 1000 + k)));
    feed(&mut ix, RANGE, &page(0, (0..100).map(|k| write(k, 0)).collect()));
    for round in 1..=40u64 {
        let pin = latest(&mut ix, None);
        let scan = ix.scan(&pin, &query(Op::Dot, &[1.0, 1.0], 5), Box::new(RecallEstimate)).unwrap();
        let after = ix.status(RANGE).unwrap().delivered;
        feed(&mut ix, RANGE, &page(after, (0..100).map(|k| write(k, round)).collect()));
        let applied = ix.status(RANGE).unwrap().applied;
        ix.advance_low(RANGE, applied).unwrap();
        for _ in 0..20 {
            ix.maintain(8);
            ix.run();
        }
        drop(scan);
        drop(pin);
        assert!(ix.shared.readers.live.len() <= 2, "{} readers are listed", ix.shared.readers.live.len());
    }
    settle(&mut ix);
    let census = ix.shared.rows.census();
    assert_eq!(census.len(), 100, "old versions of every row stayed");
    assert!(census.iter().all(|c| c.versions.len() == 1 && c.versions[0].1 == 1), "{:?}", census.iter().map(|c| c.versions.clone()).collect::<Vec<_>>());
}

// ----- No panic on any input.

fn junk_vec(g: &mut Gen, dim: usize) -> Vec<f32> {
    let n = match g.below(8) {
        0 => 0,
        1 => dim + 1,
        2 => dim.saturating_sub(1),
        _ => dim,
    };
    (0..n)
        .map(|_| match g.below(10) {
            0 => f32::NAN,
            1 => f32::INFINITY,
            2 => f32::NEG_INFINITY,
            3 => f32::MAX,
            4 => f32::MIN_POSITIVE / 2.0,
            5 => 0.0,
            _ => g.range_i64(-4, 4) as f32,
        })
        .collect()
}

fn junk_filter(g: &mut Gen, depth: u32) -> Filter {
    let c = g.below(4) as usize;
    let text = || Arc::<str>::from("a");
    match g.below(if depth == 0 { 6 } else { 9 }) {
        0 => Filter::Eq(c, Value::Float(f64::NAN)),
        1 => Filter::Eq(c, Value::Int(1)),
        2 => Filter::Range(c, Bound::Included(Value::Text(text())), Bound::Excluded(Value::Int(0))),
        3 => Filter::Contains(c, text()),
        4 => Filter::StartsWith(c, text()),
        5 => Filter::Eq(c, Value::Tags(Arc::new([text()]))),
        6 => Filter::Not(Box::new(junk_filter(g, depth - 1))),
        7 => Filter::And((0..g.below(3)).map(|_| junk_filter(g, depth - 1)).collect()),
        _ => Filter::Or((0..g.below(3)).map(|_| junk_filter(g, depth - 1)).collect()),
    }
}

/// No sequence of calls with any arguments panics, hangs or breaks the order of the positions.
#[test]
fn adv23_no_sequence_of_calls_with_any_arguments_panics() {
    let runs = Runs { cases: 150, ..Runs::default() };
    check_with(runs, "adv23_no_panic", |g| {
        let cfg = Config { part_min: 1, part_max: 4, queue: 3, max_flight: 2, max_flight_rows: 4, ..Config::new(schema(2, &[ColumnType::Int, ColumnType::Text], Some(1)), 2) };
        let mut ix: Index = Index::new(cfg, ExactCodec, None).map_err(|e| e.to_string())?;
        let mut pins: Vec<Pin> = Vec::new();
        let mut scans: Vec<Scan> = Vec::new();
        let range = |g: &mut Gen| RangeId(g.below(3) as u128);
        let junk_row = |g: &mut Gen| {
            let vec = match g.below(6) {
                0 => None,
                1 => Some(junk_vec(g, 2).into()),
                _ => Some(vec![g.range_i64(-3, 3) as f32, g.range_i64(-3, 3) as f32].into()),
            };
            let cols = match g.below(5) {
                0 => Vec::new(),
                1 => vec![Value::Float(f64::NAN), Value::Int(0)],
                _ => vec![Value::Int(g.below(3) as i64), Value::Text(Arc::from(*g.pick(&["a", "b"])))],
            };
            Row { vec, cols, image: Arc::new([]) }
        };
        for _ in 0..120 {
            let r = range(g);
            match g.below(16) {
                0 => drop(ix.begin_load(r, g.below(6), g.below(100))),
                1 => {
                    let rows: Vec<_> = (0..g.below(4)).map(|_| (key(g.below(5)), g.below(50), junk_row(g))).collect();
                    drop(ix.load_rows(r, &rows));
                }
                2 => drop(ix.finish_load(r)),
                3 => drop(ix.caught_up(r, g.below(20))),
                4 | 5 => {
                    let after = g.below(12);
                    let through = after + g.below(5);
                    let records = (0..g.below(6))
                        .map(|_| {
                            let at = if g.chance(1, 8) { g.below(20) } else { after + 1 + g.below(through.saturating_sub(after) + 1) };
                            let change = if g.chance(1, 6) { Change::Closed(g.below(200)) } else { Change::Row { key: key(g.below(5)), commit_ts: g.below(200), row: g.bool().then(|| junk_row(g)) } };
                            Record { at, change }
                        })
                        .collect();
                    drop(ix.apply(r, &Page { after, through, records }));
                }
                6 => drop(ix.advance_low(r, g.below(30))),
                7 => drop(ix.drop_range(r)),
                8 => {
                    let reads = [ReadAt::Latest { barrier: g.below(30) }, ReadAt::Position(g.below(30)), ReadAt::Cut(g.below(300))];
                    let ranges: Vec<_> = (0..g.below(4)).map(|_| (range(g), *g.pick(&reads))).collect();
                    let filter = g.bool().then(|| junk_filter(g, 3));
                    if let Ok(p) = ix.pin(&ranges, filter.as_ref()) {
                        pins.push(p);
                    }
                }
                9 | 10 => {
                    if let Some(p) = pins.last().cloned() {
                        let q = Query {
                            op: *g.pick(&[Op::Cosine, Op::Dot, Op::L2]),
                            vec: junk_vec(g, 2),
                            limit: *g.pick(&[0, 1, 5, 1000, 1001, usize::MAX]),
                            filter: g.bool().then(|| junk_filter(g, 3)),
                            exact: g.bool(),
                            recall: *g.pick(&[0.0, 0.5, 1.0, -1.0, 2.0, f64::NAN, f64::INFINITY]),
                        };
                        match ix.scan(&p, &q, Box::new(RecallEstimate)) {
                            Ok(s) => scans.push(s),
                            Err(_) => drop(ix.search(&p, &q)),
                        }
                    }
                }
                11 => {
                    if let Some(mut s) = scans.pop() {
                        for _ in 0..g.below(6) {
                            if !ix.visit(&mut s).unwrap_or(false) {
                                break;
                            }
                        }
                        if g.bool() {
                            drop(ix.finish(s));
                        }
                    }
                }
                12 => {
                    ix.run();
                    ix.maintain(*g.pick(&[0, 1, 5, usize::MAX]));
                }
                13 => drop(ix.step(*g.pick(&[0, 1, 2, 3, usize::MAX]))),
                14 => {
                    let (a, b) = (PartId(g.below(8) as u32), PartId(g.below(8) as u32));
                    if g.bool() {
                        ix.split(a);
                    } else {
                        ix.merge(a, b);
                    }
                }
                _ => {
                    if g.bool() {
                        pins.pop();
                    }
                    drop(ix.checkpoint());
                    drop(ix.begin_checkpoint());
                }
            }
            for id in 0..3 {
                if let Ok(s) = ix.status(RangeId(id)) {
                    if s.damage.is_none() && s.serving && (s.low > s.applied || s.applied > s.delivered) {
                        return Err(format!("{s:?} breaks the order of the positions"));
                    }
                }
            }
        }
        Ok(())
    });
}

// ----- Filters that few rows match.

/// "At most 20,000 matches: score all of them." Whatever the kind of the filter, an approximate query
/// that fewer than `flat` rows match returns exactly the best of them, because every match is scored.
#[test]
fn adv24_a_filter_that_few_rows_match_is_scored_in_full_whatever_its_kind() {
    let g = &mut Gen::from_seed(5);
    let (dim, n) = (12usize, 3000u64);
    let cfg = Config { flat: 200, part_min: 8, part_max: 32, ..Config::new(schema(dim, &[ColumnType::Text], None), 2) };
    let mut ix = serving(cfg, ExactCodec, None);
    let mut rows = BTreeMap::new();
    let mut changes = Vec::new();
    for k in 0..n {
        let v: Vec<f32> = (0..dim).map(|_| g.range_i64(-100, 100) as f32).collect();
        let text = if k % 100 == 7 { format!("zz{k}") } else { "x".to_owned() };
        let r = row(&v, vec![Value::Text(Arc::from(text.as_str()))], k);
        rows.insert(k, r.clone());
        changes.push((k, Some(r)));
    }
    feed(&mut ix, RANGE, &page(0, changes));
    settle(&mut ix);
    let pin = latest(&mut ix, None);
    let filters = [
        ("eq", Filter::Eq(0, Value::Text(Arc::from("zz7")))),
        ("not", Filter::Not(Box::new(Filter::Eq(0, Value::Text(Arc::from("x")))))),
        ("starts_with", Filter::StartsWith(0, Arc::from("zz"))),
        ("range of text", Filter::Range(0, Bound::Included(Value::Text(Arc::from("zz"))), Bound::Unbounded)),
        ("or of prefixes", Filter::Or(vec![Filter::StartsWith(0, Arc::from("zz1")), Filter::StartsWith(0, Arc::from("zz2"))])),
    ];
    let mut wrong = Vec::new();
    for (name, f) in &filters {
        for round in 0..30 {
            let op = [Op::Cosine, Op::Dot, Op::L2][round % 3];
            let v: Vec<f32> = (0..dim).map(|_| g.range_i64(-100, 100) as f32).collect();
            let q = Query { filter: Some(f.clone()), exact: false, ..query(op, &v, 10) };
            let want = best(&rows, &q);
            assert!(want.len() >= 1);
            let got = kv(&ix.search(&pin, &q).unwrap());
            if got != want {
                wrong.push((*name, round));
            }
        }
    }
    assert!(wrong.is_empty(), "{} of 150 queries that fewer than `flat` rows match miss a match, for example {:?}", wrong.len(), &wrong[..wrong.len().min(6)]);
}

// ----- A filter of any depth.

/// A hostile filter nested as deep as a request can make it fails with an error, and does not
/// overflow the stack of the thread that pins or scans.
#[test]
#[ignore = "known defect B-D5: Index::pin calls Filter::only, which recurses without a bound, before anything checks the depth"]
fn adv24_a_filter_nested_without_end_is_refused_and_does_not_overflow_the_stack() {
    const CHILD: &str = "NABEDRA_DEEP_FILTER_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let mut ix = serving(Config::new(schema(2, &[ColumnType::Int, ColumnType::Text], Some(1)), 1), ExactCodec, None);
        feed(&mut ix, RANGE, &page(0, vec![(1, Some(row(&[1.0, 0.0], vec![Value::Int(0), Value::Text(Arc::from("a"))], 1)))]));
        let mut f = Filter::Eq(1, Value::Text(Arc::from("a")));
        for _ in 0..400_000 {
            f = Filter::And(vec![f]);
        }
        let outcome = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], Some(&f));
        let refused = match outcome {
            Err(_) => true,
            Ok(pin) => ix.search(&pin, &Query { filter: Some(f.clone()), ..query(Op::Dot, &[1.0, 0.0], 1) }).is_err(),
        };
        std::mem::forget(f);
        assert!(refused, "a filter 400,000 levels deep was served");
        return;
    }
    let me = std::env::current_exe().unwrap();
    let out = std::process::Command::new(me)
        .args(["--exact", "index::tests::adversary_rules::adv24_a_filter_nested_without_end_is_refused_and_does_not_overflow_the_stack", "--include-ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "the thread that served the filter died: {:?}\n{}", out.status, String::from_utf8_lossy(&out.stderr).lines().take(6).collect::<Vec<_>>().join("\n"));
}

// ----- Copies.

/// A version has one entry in a partition. A reassign moved an entry to another partition, and its
/// source waits for a pin to end. A drain of the source meanwhile must not copy the same version to
/// the same target again: both copies would stay for good, each named, counted and scanned.
#[test]
#[ignore = "known defect B-D6: a drain copies an entry that a reassign already moved, and the target keeps both"]
fn adv25_an_entry_that_a_reassign_moved_is_not_copied_again_by_a_drain_of_its_source() {
    let cfg = Config { part_min: 1, part_max: 8, ..Config::new(schema(2, &[ColumnType::Int], None), 1) };
    let mut ix = serving(cfg, ExactCodec, None);
    let angles = [0.0f32, 0.1, 0.2, 1.4, 1.5, 1.6];
    let changes = angles.iter().enumerate().map(|(k, a)| (k as u64, Some(row(&[a.cos(), a.sin()], vec![Value::Int(0)], k as u64)))).collect();
    feed(&mut ix, RANGE, &page(0, changes));
    assert!(ix.split(PartId(0)));
    settle(&mut ix);
    let parts: Vec<PartId> = ix.listed().into_iter().collect();
    assert_eq!(parts.len(), 2, "the split made two partitions");
    let (a, b) = (parts[0], parts[1]);
    let id = ix.shards[0].parts[&a].entries().next().unwrap().id;
    // A scan that began before the copy lands holds the removal back.
    let pin = latest(&mut ix, None);
    assert!(ix.reassign(a, id, b));
    ix.run();
    assert!(ix.merge(a, b), "the source drains into the partition that holds its copy");
    for _ in 0..100 {
        ix.maintain(8);
        ix.run();
    }
    drop(pin);
    settle(&mut ix);
    let census = ix.shared.rows.census();
    assert_eq!(census.len(), angles.len());
    assert!(census.iter().all(|c| c.versions.len() == 1 && c.versions[0].1 == 1), "{:?}", census.iter().map(|c| (c.slot, c.versions.clone())).collect::<Vec<_>>());
    let entries: usize = ix.shards[0].parts.values().map(|p| p.entries().count()).sum();
    assert_eq!(entries, angles.len(), "one entry for each row");
}
