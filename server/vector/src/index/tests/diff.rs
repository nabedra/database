//! Every operation against brute force: exact and approximate, filtered, with lengths that differ by
//! many powers of two, zero vectors and vectors whose length is no f32, under churn and maintenance.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

use super::super::codec::{dot, exact, goodness};
use super::super::filter::{Summary, evaluate};
use super::*;

const DIM: usize = 4;
const TAGS: [&str; 5] = ["a", "b", "c", "d", "e"];

/// Estimates that are as wrong as their bound allows, so that pruning by them has to be right.
#[derive(Clone, Copy)]
struct Rough;

fn noise(x: &[f32]) -> f32 {
    let bits = x
        .iter()
        .fold(7u32, |h, v| (h ^ v.to_bits()).wrapping_mul(0x9e37_79b1));
    (bits >> 16) as f32 / 32_768.0 - 1.0
}

impl Codec for Rough {
    type Block = Vec<Vec<f32>>;
    type Query = Vec<f32>;
    fn rotate(&self, dir: &[f32]) -> Vec<f32> {
        dir.iter().rev().copied().collect()
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
        out.extend(block.iter().map(|d| Estimate {
            cos: dot(q, d) + 0.2 * noise(d),
            err: 0.2001,
        }));
    }
    fn refine(&self, q: &Self::Query, block: &Self::Block, i: usize) -> Estimate {
        Estimate {
            cos: dot(q, &block[i]) - 0.03 * noise(&block[i]),
            err: 0.0301,
        }
    }
}

fn vector(g: &mut Gen) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 1.0, 0.25, 7.0, 1e3, 1e-4, 1e30, 1e-42, 0.0]);
    let part = |g: &mut Gen| g.range_i64(-8, 8) as f32 / 8.0 * scale;
    (0..DIM).map(|_| part(g)).collect()
}

fn columns(g: &mut Gen) -> Vec<Value> {
    let mut tags = Vec::new();
    for t in TAGS {
        if g.chance(1, 3) {
            tags.push(Arc::from(t));
        }
    }
    vec![
        Value::Text(Arc::from(*g.pick(&["x", "y", "yz"]))),
        Value::Int(g.range_i64(-3, 3)),
        Value::Float(*g.pick(&[-1.5, -0.0, 0.0, 2.5])),
        Value::Bool(g.bool()),
        Value::Tags(tags.into()),
    ]
}

const TYPES: [ColumnType; 5] = [
    ColumnType::Text,
    ColumnType::Int,
    ColumnType::Float,
    ColumnType::Bool,
    ColumnType::Tags,
];

fn filter(g: &mut Gen, depth: u32) -> Filter {
    let text = |g: &mut Gen| Arc::<str>::from(*g.pick(&["x", "y", "yz", "", "q"]));
    let tag = |g: &mut Gen| Arc::<str>::from(*g.pick(&["a", "b", "c", "d", "e", "none"]));
    let end = |g: &mut Gen, v: Value| match g.below(3) {
        0 => Bound::Included(v),
        1 => Bound::Excluded(v),
        _ => Bound::Unbounded,
    };
    match g.below(if depth == 0 { 8 } else { 11 }) {
        0 => Filter::Eq(0, Value::Text(text(g))),
        1 => Filter::Eq(1, Value::Int(g.range_i64(-4, 4))),
        2 => Filter::Eq(2, Value::Float(*g.pick(&[0.0, -0.0, 2.5]))),
        3 => Filter::Eq(3, Value::Bool(g.bool())),
        4 => {
            let (lo, hi) = (
                Value::Int(g.range_i64(-4, 4)),
                Value::Int(g.range_i64(-4, 4)),
            );
            Filter::Range(1, end(g, lo), end(g, hi))
        }
        5 => {
            let (lo, hi) = (Value::Text(text(g)), Value::Float(1.0));
            match g.bool() {
                true => Filter::Range(0, end(g, lo), Bound::Unbounded),
                false => Filter::Range(2, Bound::Unbounded, end(g, hi)),
            }
        }
        6 => Filter::StartsWith(0, text(g)),
        7 => match g.bool() {
            true => Filter::Contains(4, tag(g)),
            false => Filter::ContainsAny(4, (0..g.below(3)).map(|_| tag(g)).collect()),
        },
        8 => Filter::Not(Box::new(filter(g, depth - 1))),
        n => {
            let all = (0..g.below(3) + 1).map(|_| filter(g, depth - 1)).collect();
            if n == 9 {
                Filter::And(all)
            } else {
                Filter::Or(all)
            }
        }
    }
}

type Rows = BTreeMap<u64, Row>;

/// Brute force: the best rows that match, best first and then by key.
fn brute(rows: &Rows, q: &Query) -> Vec<(u64, f32)> {
    let matching = rows
        .iter()
        .filter(|(_, r)| q.filter.as_ref().is_none_or(|f| f.matches(&r.cols)));
    let mut all: Vec<(u64, f32)> = matching
        .filter_map(|(k, r)| Some((*k, exact(q.op, r.vec.as_deref()?, &q.vec)?)))
        .collect();
    all.sort_by(|a, b| {
        let order = goodness(q.op, b.1).total_cmp(&goodness(q.op, a.1));
        order.then(a.0.cmp(&b.0))
    });
    all.truncate(q.limit);
    all
}

fn keys(hits: &[Hit]) -> Vec<(u64, f32)> {
    let k = |h: &Hit| u64::from_be_bytes((&*h.key).try_into().unwrap());
    hits.iter().map(|h| (k(h), h.score)).collect()
}

/// Small partitions, so that splits, merges and moves take place in small indexes.
fn small(flat: usize, partition_by: Option<usize>) -> Config {
    Config {
        flat,
        part_min: 2,
        part_max: 8,
        queue: 16,
        ..Config::new(schema(DIM, &TYPES, partition_by), 3)
    }
}

/// VEC-G05: an exact query of any operation is brute force over the visible rows, and an approximate
/// one returns `limit` rows if that many match, only rows that match, with their exact scores.
#[test]
fn vs07_queries_equal_brute_force_under_churn() {
    let runs = Runs::default();
    let runs = Runs {
        cases: runs.cases / 2,
        ..runs
    };
    check_with(runs, "vs07_queries_equal_brute_force", |g| {
        let partition_by = if g.bool() { Some(0) } else { None };
        let flat = if g.bool() { 0 } else { 20_000 };
        let mut ix = serving(small(flat, partition_by), Rough, None);
        let mut rows = Rows::new();
        let mut at = 0;
        for _ in 0..g.below(12) + 1 {
            let mut changes = Vec::new();
            for _ in 0..g.below(16) {
                let k = g.below(60);
                let change = match g.below(6) {
                    0 => None,
                    // A null vector.
                    1 => Some(Row {
                        vec: None,
                        cols: columns(g),
                        image: Arc::new([]),
                    }),
                    _ => Some(row(&vector(g), columns(g), k)),
                };
                match change.clone().filter(|r| r.vec.is_some()) {
                    Some(r) => rows.insert(k, r),
                    None => rows.remove(&k),
                };
                changes.push((k, change));
            }
            let n = changes.len() as Pos;
            feed(&mut ix, RANGE, &page(at, changes));
            at += n;
            for _ in 0..g.below(20) {
                ix.maintain(3);
                ix.step(g.below(3) as usize);
            }
            if g.bool() {
                let low = g.below(at + 1);
                let _ = ix.advance_low(RANGE, low);
            }
            let filter = if g.bool() { Some(filter(g, 2)) } else { None };
            let pin = latest(&mut ix, filter.as_ref());
            let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
            let vec = vector(g);
            if op == Op::Cosine && vec.iter().all(|x| *x == 0.0) {
                continue;
            }
            let mut q = Query {
                filter,
                ..query(op, &vec, 1 + g.below(12) as usize)
            };
            let best = brute(&rows, &q);
            let got = keys(&ix.search(&pin, &q).map_err(|e| e.to_string())?);
            if got != best {
                return Err(format!("{q:?}\n got {got:?}\nwant {best:?}"));
            }
            q.exact = false;
            let got = keys(&ix.search(&pin, &q).map_err(|e| e.to_string())?);
            if got.len() != best.len() {
                return Err(format!("{} rows, {} match: {q:?}", got.len(), best.len()));
            }
            let every = brute(
                &rows,
                &Query {
                    limit: 1000,
                    ..q.clone()
                },
            );
            if let Some(wrong) = got.iter().find(|h| !every.contains(h)) {
                return Err(format!("{wrong:?} does not match or has another score"));
            }
            if flat > 0 && got != best {
                return Err("a small index is scanned in full".into());
            }
        }
        Ok(())
    });
}

/// With a stop rule that never stops, pruning by codes, bands and radius loses nothing.
#[test]
fn vs07_pruning_never_drops_a_row_that_belongs() {
    struct Never;
    impl StopRule for Never {
        fn enough(&mut self, _: &Progress<'_>) -> bool {
            false
        }
    }
    let (mut scored, mut all) = (0, 0);
    for seed in 0..60 {
        let g = &mut Gen::from_seed(seed);
        let mut ix = serving(small(0, None), Rough, None);
        let mut rows = Rows::new();
        let mut changes = Vec::new();
        for k in 0..150 {
            let r = row(&vector(g), columns(g), k);
            rows.insert(k, r.clone());
            changes.push((k, Some(r)));
        }
        feed(&mut ix, RANGE, &page(0, changes));
        settle(&mut ix);
        let pin = latest(&mut ix, None);
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            let vec: Vec<f32> = (0..DIM).map(|_| g.range_i64(1, 9) as f32 / 4.0).collect();
            let q = Query {
                exact: false,
                ..query(op, &vec, 5)
            };
            let mut scan = ix.scan(&pin, &q, Box::new(Never)).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            (scored, all) = (scored + scan.seen.len(), all + rows.len());
            assert_eq!(
                keys(&ix.finish(scan).unwrap()),
                brute(&rows, &q),
                "seed {seed} {op:?}"
            );
        }
    }
    assert!(scored * 4 < all * 3, "nothing is pruned: {scored} of {all}");
}

/// Rows around a few centres, with lengths of their own.
fn clustered(g: &mut Gen, n: usize, dim: usize, lengths: bool) -> Vec<Vec<f32>> {
    let gauss = |g: &mut Gen| {
        (0..6)
            .map(|_| g.below(1000) as f32 / 1000.0 - 0.5)
            .sum::<f32>()
    };
    let centres: Vec<Vec<f32>> = (0..40)
        .map(|_| (0..dim).map(|_| gauss(g)).collect())
        .collect();
    let mut out = Vec::new();
    for _ in 0..n {
        let c = g.pick(&centres).clone();
        let v: Vec<f32> = c.iter().map(|x| x + 0.4 * gauss(g)).collect();
        let len = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let want = if lengths {
            1.0 + g.below(300) as f32 / 100.0
        } else {
            1.0
        };
        out.push(v.iter().map(|x| x / len * want).collect());
    }
    out
}

/// The stop rule reaches its target on clustered data and reads a part of the index.
#[test]
fn vs19_approximate_scans_reach_their_recall_and_read_a_part() {
    for lengths in [false, true] {
        let g = &mut Gen::from_seed(11);
        let dim = 16;
        let all = clustered(g, 4000, dim, lengths);
        let cfg = Config {
            flat: 1000,
            part_min: 16,
            part_max: 64,
            ..Config::new(schema(dim, &[ColumnType::Int], None), 4)
        };
        let mut ix = serving(cfg, ExactCodec, None);
        let mut rows = Rows::new();
        let mut changes = Vec::new();
        for (k, v) in all.iter().enumerate() {
            let r = row(v, vec![Value::Int(k as i64 % 10)], k as u64);
            rows.insert(k as u64, r.clone());
            changes.push((k as u64, Some(r)));
        }
        feed(&mut ix, RANGE, &page(0, changes));
        settle(&mut ix);
        let parts = ix.listed().len();
        let bands = ix.shared.roots[&None].bands.len();
        assert_eq!(bands == 1, !lengths, "unit vectors have one band: {bands}");
        let pin = latest(&mut ix, None);
        let queries = clustered(&mut Gen::from_seed(11), 40, dim, false);
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            for filter in [None, Some(Filter::Eq(1 - 1, Value::Int(3)))] {
                ix.cfg.flat = if filter.is_some() { 100 } else { 1000 };
                let (mut found, mut wanted, mut visited) = (0, 0, 0);
                for v in &queries {
                    let q = Query {
                        exact: false,
                        filter: filter.clone(),
                        ..query(op, v, 10)
                    };
                    let best = brute(&rows, &q);
                    let mut scan = ix.scan(&pin, &q, Box::new(RecallEstimate)).unwrap();
                    while ix.visit(&mut scan).unwrap() {}
                    visited += scan.visited;
                    let got = keys(&ix.finish(scan).unwrap());
                    found += best.iter().filter(|b| got.contains(b)).count();
                    wanted += best.len();
                }
                let recall = found as f64 / wanted as f64;
                let units = visited as f64 / queries.len() as f64 / parts as f64;
                println!(
                    "lengths {lengths} {op:?} filter {}: recall {recall:.3}, units {units:.2} of {parts}",
                    filter.is_some()
                );
                assert!(recall >= 0.95, "{op:?} lengths {lengths}: recall {recall}");
                assert!(
                    units < if lengths { 1.5 } else { 0.6 },
                    "{op:?}: reads {units} of the partitions"
                );
            }
        }
    }
}

/// A block's bitmap is the row test, and a summary never rules a partition with a match out.
#[test]
fn vs07_filters_and_summaries_agree_with_the_rows() {
    let runs = Runs::default();
    check_with(runs, "vs07_filters_and_summaries", |g| {
        let pairs = if g.bool() { 1000 } else { 6 };
        let mut summary = Summary::new(TYPES.len(), pairs);
        let mut parts: BTreeMap<PartId, Vec<Vec<Value>>> = BTreeMap::new();
        for _ in 0..g.below(40) {
            let part = PartId(g.below(4) as u32);
            let here = parts.entry(part).or_default();
            if g.chance(1, 3) && !here.is_empty() {
                let gone = here.swap_remove(g.below(here.len() as u64) as usize);
                summary.remove(part, &gone);
            } else {
                let row = columns(g);
                summary.add(part, &row);
                here.push(row);
            }
        }
        let f = filter(g, 3);
        f.check(&schema(DIM, &TYPES, None))
            .map_err(|e| e.to_string())?;
        for (part, rows) in &parts {
            let cols: Vec<Vec<Value>> = (0..TYPES.len())
                .map(|c| rows.iter().map(|r| r[c].clone()).collect())
                .collect();
            let bits = evaluate(&f, &cols, rows.len());
            for (i, r) in rows.iter().enumerate() {
                if bits.get(i) != f.matches(r) {
                    return Err(format!("{f:?} on {r:?}"));
                }
                if f.matches(r) && !summary.may_match(&f, *part) {
                    return Err(format!("{f:?} rules {part:?} out, which holds {r:?}"));
                }
            }
        }
        // A single condition on a counted column is estimated exactly.
        if pairs == 1000 {
            let all = || parts.values().flatten();
            for t in TAGS {
                let f = Filter::Contains(4, Arc::from(t));
                let count = all().filter(|r| f.matches(r)).count();
                if summary.estimate(&f) != count as f64 {
                    return Err(format!("{t}: {} is not {count}", summary.estimate(&f)));
                }
            }
        }
        Ok(())
    });
}

/// A query that few rows match scores all of them, and one that many match stops early.
#[test]
fn vs07_the_planner_counts_matches() {
    let cfg = Config {
        flat: 50,
        part_min: 4,
        part_max: 16,
        ..Config::new(schema(2, &[ColumnType::Int], None), 2)
    };
    let mut ix = serving(cfg, ExactCodec, None);
    let mut changes = Vec::new();
    for k in 0..1000u64 {
        let a = k as f32 / 160.0;
        changes.push((
            k,
            Some(row(
                &[a.cos(), a.sin()],
                vec![Value::Int((k % 100 == 0) as i64)],
                k,
            )),
        ));
    }
    feed(&mut ix, RANGE, &page(0, changes));
    settle(&mut ix);
    let pin = latest(&mut ix, None);
    let run = |value: i64| {
        let filter = Some(Filter::Eq(0, Value::Int(value)));
        let q = Query {
            filter,
            exact: false,
            ..query(Op::Cosine, &[1.0, 0.0], 10)
        };
        let mut scan = ix.scan(&pin, &q, Box::new(RecallEstimate)).unwrap();
        while ix.visit(&mut scan).unwrap() {}
        (scan.visited, images(&ix.finish(scan).unwrap()))
    };
    let (_, few) = run(1);
    assert_eq!(few.len(), 10, "all ten rows that match: {few:?}");
    let (visited, many) = run(0);
    assert_eq!(many.len(), 10);
    assert!(
        visited < ix.listed().len(),
        "{visited} of {}",
        ix.listed().len()
    );
}

/// A scan whose stop rule asks for little still returns `limit` rows when that many match.
#[test]
fn vs07_a_scan_does_not_stop_before_limit_rows() {
    let cfg = Config {
        flat: 0,
        part_min: 4,
        part_max: 16,
        ..Config::new(schema(2, &[ColumnType::Bool], None), 2)
    };
    let mut ix = serving(cfg, ExactCodec, None);
    let write = |k: u64| {
        let a = k as f32 / 160.0;
        let mark = Value::Bool(k.is_multiple_of(25));
        (k, Some(row(&[a.cos(), a.sin()], vec![mark], k)))
    };
    feed(&mut ix, RANGE, &page(0, (0..1000).map(write).collect()));
    settle(&mut ix);
    let pin = latest(&mut ix, None);
    for limit in [1, 30, 40, 50] {
        let filter = Some(Filter::Eq(0, Value::Bool(true)));
        let q = Query {
            filter,
            exact: false,
            recall: 0.0,
            ..query(Op::L2, &[1.0, 0.0], limit)
        };
        let hits = ix.search(&pin, &q).unwrap();
        assert_eq!(hits.len(), limit.min(40));
        assert!(images(&hits).iter().all(|k| k.is_multiple_of(25)));
    }
}
