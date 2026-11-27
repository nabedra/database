//! The stop rule of approximate scans against brute force in f64, on vectors of unequal lengths.
//! Found by the local benchmark (task BH, 2026-11-27): on GloVe-100 (100K rows, lengths 0.04 to
//! 18.7), a query that asks for recall 0.99 got recall@10 of 0.89 under `l2` and 0.90 under `dot`,
//! and targets 0.95 and 0.99 stopped at the same place.

use std::sync::Arc;

use nabedra_check::rng::SplitMix64;
use nabedra_vector::index::{Config, DirectionCodec, Index, Query, ReadAt};
use nabedra_vector::types::{Change, Error, Key, Op, Page, RangeId, Record, Row, Schema};

const RANGE: RangeId = RangeId(7);
const DIM: usize = 64;
const ROWS: usize = 12_000;
const QUERIES: usize = 100;
const K: usize = 10;

fn uniform(rng: &mut SplitMix64) -> f64 {
    (rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
}

fn gaussian(rng: &mut SplitMix64) -> f64 {
    (-2.0 * (1.0 - uniform(rng)).ln()).sqrt() * (std::f64::consts::TAU * uniform(rng)).cos()
}

/// Clustered directions with log-normal lengths, as word embeddings have.
fn vectors(rng: &mut SplitMix64, centres: &[Vec<f64>], n: usize) -> Vec<Vec<f32>> {
    (0..n)
        .map(|_| {
            let c = &centres[(rng.next_u64() % centres.len() as u64) as usize];
            let v: Vec<f64> = c.iter().map(|x| x + 2.0 * gaussian(rng)).collect();
            let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            let len = (0.5 * gaussian(rng)).exp() * 5.0;
            v.iter().map(|x| (x / norm * len) as f32).collect()
        })
        .collect()
}

fn goodness(op: Op, a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    match op {
        Op::Dot => dot,
        Op::L2 => -a
            .iter()
            .zip(b)
            .map(|(x, y)| (f64::from(*x) - f64::from(*y)).powi(2))
            .sum::<f64>(),
        Op::Cosine => {
            let n = |v: &[f32]| v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
            dot / n(a) / n(b)
        }
    }
}

fn key(i: usize) -> Key {
    Arc::new((i as u64).to_be_bytes())
}

/// Feeds every row through `apply`, with maintenance between pages, then settles.
fn build(base: &[Vec<f32>]) -> Index<DirectionCodec> {
    let schema = Schema {
        dim: DIM,
        columns: Vec::new(),
        partition_by: None,
    };
    let mut cfg = Config::new(schema, 1);
    // Small enough that the scans are approximate at this size.
    cfg.flat = 500;
    let mut ix = Index::new(cfg, DirectionCodec::new(DIM, 11).unwrap(), None).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    for (p, chunk) in base.chunks(512).enumerate() {
        let after = (p * 512) as u64;
        let records = chunk.iter().enumerate().map(|(i, v)| {
            let at = after + 1 + i as u64;
            let row = Row {
                vec: Some(v.as_slice().into()),
                cols: Vec::new(),
                image: key(at as usize - 1),
            };
            Record {
                at,
                change: Change::Row {
                    key: key(at as usize - 1),
                    commit_ts: at,
                    row: Some(row),
                },
            }
        });
        let mut page = Page {
            after,
            through: after + chunk.len() as u64,
            records: records.collect(),
        };
        while page.after < page.through {
            match ix.apply(RANGE, &page) {
                Ok(at) => page.after = at,
                Err(Error::Busy) => {}
                Err(e) => panic!("{e}"),
            }
            page.records.retain(|r| r.at > page.after);
            ix.run();
        }
        for _ in 0..4 {
            ix.maintain(64);
            ix.run();
        }
    }
    let mut idle = 0;
    for _ in 0..1_000_000 {
        idle = if ix.maintain(16) { 0 } else { idle + 1 };
        ix.run();
        if idle == 3 {
            break;
        }
    }
    ix
}

/// Mean recall@10 of approximate scans that ask for `recall`.
fn recall(
    ix: &mut Index<DirectionCodec>,
    base: &[Vec<f32>],
    queries: &[Vec<f32>],
    op: Op,
    target: f64,
) -> f64 {
    let pin = ix
        .pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None)
        .unwrap();
    let mut found = 0;
    for q in queries {
        let mut truth: Vec<(f64, usize)> = base
            .iter()
            .enumerate()
            .map(|(i, v)| (goodness(op, q, v), i))
            .collect();
        truth.sort_by(|a, b| b.0.total_cmp(&a.0));
        let best: Vec<usize> = truth[..K].iter().map(|t| t.1).collect();
        let query = Query {
            op,
            vec: q.clone(),
            limit: K,
            filter: None,
            exact: false,
            recall: target,
        };
        let hits = ix.search(&pin, &query).unwrap();
        found += hits
            .iter()
            .filter(|h| best.contains(&(u64::from_be_bytes((*h.key).try_into().unwrap()) as usize)))
            .count();
    }
    found as f64 / (queries.len() * K) as f64
}

/// A scan that asks for recall 0.99 reaches at least 0.95, for every operation.
#[test]
#[ignore = "VEC-BUG-recall-estimate-unequal-lengths"]
fn vs07_an_approximate_scan_reaches_its_recall_on_unequal_lengths() {
    let mut rng = SplitMix64::new(20_261_127);
    let centres: Vec<Vec<f64>> = (0..40)
        .map(|_| (0..DIM).map(|_| gaussian(&mut rng)).collect())
        .collect();
    let base = vectors(&mut rng, &centres, ROWS);
    let queries = vectors(&mut rng, &centres, QUERIES);
    let mut ix = build(&base);
    let mut misses = Vec::new();
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        let (r95, r99) = (
            recall(&mut ix, &base, &queries, op, 0.95),
            recall(&mut ix, &base, &queries, op, 0.99),
        );
        println!("{op:?}: recall@10 {r95:.3} at target 0.95, {r99:.3} at target 0.99");
        if r99 < 0.95 {
            misses.push(format!("{op:?} {r99:.3}"));
        }
    }
    assert!(
        misses.is_empty(),
        "recall@10 below 0.95 at target 0.99: {misses:?}"
    );
}
