//! Task perf1 (2026-11-27): the build and scan costs that the local benchmark measured, with the rules
//! they must keep. Exact scans still equal brute force; the kernel only rules rows out (L1).

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use nabedra_check::Gen;

use super::super::codec::{cosine_with, exact, goodness, length};
use super::super::scan::{excess_bound, normal_quantile};
use super::*;
use crate::store::{RamVectors, Stats as StoreStats, VecKey, Vectors};

fn brute(rows: &[(u64, Row)], q: &Query) -> Vec<(u64, f32)> {
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

fn random(g: &mut Gen, dim: usize, scale: f32) -> Vec<f32> {
    (0..dim)
        .map(|_| g.range_i64(-1000, 1000) as f32 / 1000.0 * scale)
        .collect()
}

/// Rows around `centres` axes of `dim` values, each with a value of column 0.
fn clusters(g: &mut Gen, dim: usize, centres: usize, per: usize) -> Vec<(u64, Row)> {
    let mut rows = Vec::new();
    for c in 0..centres {
        for _ in 0..per {
            let mut v = random(g, dim, 0.3);
            v[c] += 10.0;
            let k = rows.len() as u64;
            rows.push((k, row(&v, vec![Value::Int((k % 10) as i64)], k)));
        }
    }
    rows
}

fn build<C: Codec>(cfg: Config, codec: C, rows: &[(u64, Row)]) -> Index<C> {
    let mut ix = serving(cfg, codec, None);
    let changes = rows.iter().map(|(k, r)| (*k, Some(r.clone()))).collect();
    feed(&mut ix, RANGE, &page(0, changes));
    settle(&mut ix);
    ix
}

/// The partition that holds the entry of a key, and the entry's instance.
fn place<C: Codec>(ix: &Index<C>, k: u64) -> Option<(PartId, u64)> {
    let slot = *ix.shared.ranges.get(&RANGE)?.keys.get(&key(k))?;
    ix.shards.iter().flat_map(|s| &s.parts).find_map(|(p, part)| {
        let e = part.entries().find(|e| e.slot == slot)?;
        Some((*p, e.id))
    })
}

/// A recheck finds an entry that sits in a partition far from it through the codes, without its
/// raw vector, and moves it back to the partition it belongs to. Before perf1 it read every raw
/// vector and walked the tree for each entry: 72 ms per partition at 768 dimensions.
#[test]
fn vs07_a_recheck_moves_a_stray_entry_by_its_codes() {
    let dim = 64;
    let rows = clusters(&mut Gen::from_seed(5), dim, 2, 400);
    let mut ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 2),
        DirectionCodec::new(dim, 3).unwrap(),
        &rows,
    );
    let (home, id) = place(&ix, 0).unwrap();
    let (other, _) = place(&ix, 799).unwrap();
    assert_ne!(home, other, "the clusters share a partition");
    assert!(ix.reassign(home, id, other));
    settle(&mut ix);
    assert_eq!(place(&ix, 0).unwrap().0, other, "nothing else moves it");
    ix.shared.recheck.insert(other);
    settle(&mut ix);
    assert_eq!(place(&ix, 0).unwrap().0, home, "the recheck leaves it astray");
}

/// One maintenance call rechecks as many partitions as it maintains: before perf1 it took one, and
/// the 16 that each split queued made the settle of 100K rows take 334 s.
#[test]
fn vs07_a_maintenance_call_rechecks_as_many_partitions_as_it_maintains() {
    let dim = 16;
    let rows = clusters(&mut Gen::from_seed(6), dim, 4, 300);
    let mut ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 1),
        ExactCodec,
        &rows,
    );
    let parts: Vec<PartId> = ix.listed().into_iter().collect();
    assert!(parts.len() >= 3, "{} partitions", parts.len());
    ix.shared.recheck.extend(parts.iter().copied());
    ix.maintain(parts.len());
    assert!(ix.shared.recheck.is_empty(), "{:?} left", ix.shared.recheck);
}

fn exact_scan<C: Codec>(ix: &mut Index<C>, q: &Query) -> (Vec<(u64, f32)>, usize, usize) {
    let pin = latest(ix, q.filter.as_ref());
    let mut scan = ix.scan(&pin, q, Box::new(RecallEstimate)).unwrap();
    while ix.visit(&mut scan).unwrap() {}
    let (seen, rescored) = (scan.seen.len(), scan.rescored);
    (keys(&ix.finish(scan).unwrap()), seen, rescored)
}

/// L1: an exact scan scores every matching row with the kernel and gives the f64 score only to
/// those whose kernel score and slack reach the result. It still equals brute force.
#[test]
fn vs07_exact_scans_rule_rows_out_by_the_kernel_and_equal_brute_force() {
    let dim = 48;
    let mut g = Gen::from_seed(7);
    let rows: Vec<(u64, Row)> = (0..3000)
        .map(|k| {
            let scale = *g.pick(&[1.0f32, 1.0, 3.0, 0.01, 200.0]);
            let cols = vec![Value::Int(k as i64 % 10)];
            (k, row(&random(&mut g, dim, scale), cols, k))
        })
        .collect();
    let mut ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 2),
        DirectionCodec::new(dim, 9).unwrap(),
        &rows,
    );
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        for filter in [None, Some(Filter::Eq(0, Value::Int(3)))] {
            for _ in 0..5 {
                let q = Query {
                    filter: filter.clone(),
                    ..query(op, &random(&mut g, dim, 2.0), 10)
                };
                let matching = rows.iter().filter(|(_, r)| q.filter.as_ref().is_none_or(|f| f.matches(&r.cols))).count();
                let (got, seen, rescored) = exact_scan(&mut ix, &q);
                assert_eq!(got, brute(&rows, &q), "{op:?} {filter:?}");
                assert_eq!(seen, matching, "every matching row is scored");
                assert!(rescored * 4 < matching, "{op:?}: {rescored} of {matching} got the f64 score");
            }
        }
    }
}

/// Rows that tie with the last one of the result are rescored, so the smaller key wins as in brute
/// force. A kernel test of `<=` instead of `<` would drop them.
#[test]
fn vs07_exact_scans_keep_ties_with_the_last_row_by_key() {
    let dim = 24;
    let v: Vec<f32> = (0..dim).map(|i| (i as f32 - 7.5) / 4.0).collect();
    // Fed in descending key order, so the first rows scored have the largest keys.
    let rows: Vec<(u64, Row)> = (0..800u64).rev().map(|k| (k, row(&v, vec![Value::Int(0)], k))).collect();
    let mut ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 1),
        DirectionCodec::new(dim, 2).unwrap(),
        &rows,
    );
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        let q = query(op, &v.iter().map(|x| x * 0.5 + 0.1).collect::<Vec<_>>(), 10);
        let (got, ..) = exact_scan(&mut ix, &q);
        assert_eq!(got, brute(&rows, &q), "{op:?}");
        let first: Vec<u64> = got.iter().map(|h| h.0).collect();
        assert_eq!(first, (0..10).collect::<Vec<u64>>(), "{op:?}");
    }
}

/// Rows whose exact scores tie while the kernel rounds them apart: permutations of one vector,
/// against a query of equal values. The kernel adds the same products in other orders, so its
/// scores differ by a few units in the last place. A kernel test without its slack drops some of
/// the smallest keys.
#[test]
fn vs07_exact_scans_keep_rows_that_the_kernel_rounds_below_a_tie() {
    let dim = 768;
    let mut g = Gen::from_seed(12);
    let base: Vec<f32> = (0..dim).map(|_| g.range_i64(1, 1_000_000) as f32 / 999_983.0).collect();
    let rows: Vec<(u64, Row)> = (0..600u64)
        .map(|k| {
            let mut v = base.clone();
            for i in (1..dim).rev() {
                v.swap(i, g.below(i as u64 + 1) as usize);
            }
            (k, row(&v, vec![Value::Int(0)], k))
        })
        .collect();
    let mut ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 1),
        DirectionCodec::new(dim, 2).unwrap(),
        &rows,
    );
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        for c in [0.3f32, 1.7, 0.0123] {
            let q = query(op, &vec![c; dim], 10);
            let (got, ..) = exact_scan(&mut ix, &q);
            assert_eq!(got, brute(&rows, &q), "{op:?} {c}");
        }
    }
}

/// VEC-G11: evaluating a filter over the rows of a band is charged by itself, one unit per row and
/// condition. 20 conditions over 300 rows are 6,000 units, far beyond a budget that scoring alone
/// (600) and the summaries (60) fit in.
#[test]
fn vs02_evaluating_a_filter_over_rows_is_charged_per_row() {
    let mut ix = serving(Config::new(schema(2, &[ColumnType::Int], None), 1), ExactCodec, None);
    let rows = (0..300).map(|k| (k, Some(row(&[k as f32, 1.0], vec![Value::Int(k as i64)], k))));
    feed(&mut ix, RANGE, &page(0, rows.collect()));
    let none = Filter::Or((0..19).map(|i| Filter::Eq(0, Value::Int(-1 - i))).collect());
    let q = Query {
        filter: Some(Filter::Not(Box::new(none))),
        ..query(Op::Dot, &[1.0, 0.0], 10)
    };
    let pin = latest(&mut ix, None);
    let run = |cpu| {
        let budget = crate::runtime::Budget { cpu, ..crate::runtime::Budget::UNLIMITED };
        let mut scan = ix.scan_within(&pin, &q, Box::new(RecallEstimate), budget).unwrap();
        let mut result = Ok(true);
        while let Ok(true) = result {
            result = ix.visit(&mut scan);
        }
        result.and_then(|_| ix.finish(scan)).map(|h| h.len())
    };
    assert_eq!(run(1_000), Err(Error::OverBudget("cpu")));
    assert_eq!(run(10_000), Ok(10));
}

#[test]
fn vs07_a_cosine_with_known_lengths_is_the_exact_one() {
    let mut g = Gen::from_seed(8);
    for dim in [1, 3, 64, 768] {
        for _ in 0..200 {
            let scale = *g.pick(&[1.0f32, 1e-3, 1e3, 1e-19, 1e19]);
            let (a, b) = (random(&mut g, dim, scale), random(&mut g, dim, 1.0));
            let (la, lb) = (length(&a), length(&b));
            if la > 0.0 && lb > 0.0 {
                assert_eq!(Some(cosine_with(&a, &b, la, lb)), exact(Op::Cosine, &a, &b));
            }
        }
    }
}

/// A store whose reads count as I/O and that counts its batches.
#[derive(Debug, Default)]
struct Counted {
    ram: RamVectors,
    batches: AtomicUsize,
    keys: AtomicUsize,
}

impl Vectors for Counted {
    fn put(&self, key: VecKey, vec: &[f32]) -> Result<()> {
        self.ram.put(key, vec)
    }
    fn get_many(&self, keys: &[VecKey]) -> Vec<Result<Arc<[f32]>>> {
        self.batches.fetch_add(1, AtomicOrdering::Relaxed);
        self.keys.fetch_add(keys.len(), AtomicOrdering::Relaxed);
        self.ram.get_many(keys)
    }
    fn release(&self, key: VecKey) {
        self.ram.release(key)
    }
    fn on_disk(&self) -> bool {
        true
    }
    fn stats(&self) -> StoreStats {
        self.ram.stats()
    }
}

/// An exact scan reads raw vectors from NVMe in batches, one key per matching row, and charges each
/// read as before. It read them one at a time before perf1.
#[test]
fn vs10_exact_scans_read_nvme_vectors_in_batches() {
    let dim = 16;
    let rows = clusters(&mut Gen::from_seed(10), dim, 3, 400);
    let store = Arc::new(Counted::default());
    let mut cfg = Config::new(schema(dim, &[ColumnType::Int], None), 2);
    cfg.vectors = Some(store.clone());
    let mut ix = build(cfg, ExactCodec, &rows);
    let q = Query {
        filter: Some(Filter::Eq(0, Value::Int(4))),
        ..query(Op::Dot, &[1.0; 16], 10)
    };
    let matching = rows.iter().filter(|(_, r)| r.cols[0] == Value::Int(4)).count();
    let (batches, asked) = (store.batches.load(AtomicOrdering::Relaxed), store.keys.load(AtomicOrdering::Relaxed));
    let pin = latest(&mut ix, q.filter.as_ref());
    let mut scan = ix.scan_within(&pin, &q, Box::new(RecallEstimate), crate::runtime::Budget::UNLIMITED).unwrap();
    while ix.visit(&mut scan).unwrap() {}
    let io = scan.usage().unwrap().io;
    assert_eq!(keys(&ix.finish(scan).unwrap()), brute(&rows, &q));
    let batches = store.batches.load(AtomicOrdering::Relaxed) - batches;
    let asked = store.keys.load(AtomicOrdering::Relaxed) - asked;
    assert_eq!(asked, matching, "one read per matching row");
    assert_eq!(io, (matching * dim * 4) as u64, "each read is charged");
    assert!(batches * 8 <= matching, "{batches} batches for {matching} rows");
}

/// `Index::stats` counts partitions, entries, rows and the bytes of each tier.
#[test]
fn vs19_stats_count_what_the_index_holds() {
    let dim = 64;
    let rows = clusters(&mut Gen::from_seed(11), dim, 3, 300);
    let ix = build(
        Config::new(schema(dim, &[ColumnType::Int], None), 2),
        DirectionCodec::new(dim, 4).unwrap(),
        &rows,
    );
    let s = ix.stats();
    let n = rows.len();
    assert_eq!((s.entries, s.rows, s.versions), (n, n, n), "{s:?}");
    assert_eq!(s.ready, ix.listed().len(), "{s:?}");
    assert!(s.ready >= 2 && s.draining == 0, "{s:?}");
    assert_eq!(s.raw_ram_bytes, (n * dim * 4) as u64);
    // 1 bit, the inline 4-bit extension and 12 bytes of factors and 8 of lengths per entry.
    assert_eq!(s.code_bytes, (n * (dim / 8 + dim / 2 + 20)) as u64, "{s:?}");
    assert_eq!((s.raw_nvme_bytes, s.ext_ram_bytes, s.ext_nvme_bytes), (0, 0, 0));
    let store = Arc::new(Counted::default());
    let mut cfg = Config::new(schema(dim, &[ColumnType::Int], None), 2);
    cfg.vectors = Some(store);
    let ix = build(cfg, DirectionCodec::new(dim, 4).unwrap(), &rows);
    let s = ix.stats();
    assert_eq!((s.raw_ram_bytes, s.raw_cached_bytes), (0, (n * dim * 4) as u64), "{s:?}");
}

/// VEC-BUG-recall-estimate-unequal-lengths: the window of hits alone moved in steps of 1/limit, so
/// every recall above 0.9 at limit 10 was one rule. The scan also reads on while the next unit of
/// some band is expected to reach the result, and that expectation grows with the recall asked.
#[test]
fn vs07_the_stop_rule_reads_on_while_a_unit_may_reach_the_result() {
    let origins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let enough = |recall, floor, reach| {
        let p = Progress { limit: 10, recall, units: 40, origins: &origins, floor, reach };
        RecallEstimate.enough(&p)
    };
    assert!(enough(0.95, 0.5, 0.4));
    assert!(!enough(0.95, 0.5, 0.6), "the next unit may hold a better row");
    assert!(!enough(0.95, 0.5, 0.5), "a tie may enter by its key");
    // The expected excess of a unit grows with the recall, by the normal quantile.
    let excess: Vec<f32> = (0..32).map(|i| 0.05 + 0.002 * i as f32).collect();
    let at = |r| excess_bound(&excess, 32, r);
    assert!(at(0.5) < at(0.9) && at(0.9) < at(0.95) && at(0.95) < at(0.99));
    // At 0.95 a unit may miss a quarter as often: the bound is at least the largest of the last 16.
    assert!(at(0.95) >= 0.05 + 0.002 * 31.0 - 1e-6, "{}", at(0.95));
    assert!(at(0.5) > 0.097 && at(0.5) < at(0.95), "above the mean of the last 16: {}", at(0.5));
    assert_eq!(at(1.0), f64::INFINITY);
    assert_eq!(excess_bound(&[], 32, 0.9), f64::INFINITY, "nothing measured yet");
    for (p, z) in [(0.5, 0.0), (0.95, 1.645), (0.99, 2.326), (0.05, -1.645)] {
        assert!((normal_quantile(p) - z).abs() < 1e-3, "{p}: {}", normal_quantile(p));
    }
}

#[test]
fn vs02_a_budgeted_scan_refuses_an_unaffordable_filter_plan_before_running_it() {
    use crate::runtime::Budget;

    let mut ix = serving(Config::new(schema(2, &[ColumnType::Int], None), 1), ExactCodec, None);
    let rows = (0..300)
        .map(|k| (k, Some(row(&[1.0, 0.0], vec![Value::Int(k as i64)], k))));
    feed(&mut ix, RANGE, &page(0, rows.collect()));
    let pin = latest(&mut ix, None);
    let q = Query {
        exact: false,
        filter: Some(Filter::Or((0..20_000).map(|i| Filter::Eq(0, Value::Int(i))).collect())),
        ..query(Op::Dot, &[1.0, 0.0], 10)
    };
    // One unit stops validation. Even funding validation and cloning must leave the estimate out.
    for cpu in [1, 2 * (20_000 + 1)] {
        let budget = Budget { cpu, ..Budget::UNLIMITED };
        assert!(
            matches!(
                ix.scan_within(&pin, &q, Box::new(RecallEstimate), budget),
                Err(Error::OverBudget("cpu"))
            ),
            "planning itself exceeds the budget of {cpu}"
        );
    }
}

#[test]
fn vs02_a_budgeted_pin_charges_filter_validation_and_routing() {
    let cfg = Config::new(schema(2, &[ColumnType::Int], Some(0)), 1);
    let rows = [(0, row(&[1.0, 0.0], vec![Value::Int(42)], 0))];
    let mut ix = build(cfg, ExactCodec, &rows);
    let ranges = [(RANGE, ReadAt::Position(1))];
    let budget = |cpu| Budget { cpu, ..Budget::UNLIMITED };
    let invalid = Filter::And(vec![Filter::Eq(1, Value::Int(42))]);
    assert!(matches!(
        ix.pin_within(&ranges, Some(&invalid), budget(1)),
        Err(Error::OverBudget("cpu"))
    ));

    let filter = Filter::And(vec![Filter::And(vec![]), Filter::Eq(0, Value::Int(42))]);
    assert!(matches!(
        ix.pin_within(&ranges, Some(&filter), budget(5)),
        Err(Error::OverBudget("cpu"))
    ));
    let pin = ix.pin_within(&ranges, Some(&filter), budget(6)).unwrap();
    assert_eq!(pin.0.group, Some(Scalar::Int(42)));
    assert_eq!(pin.0.roots.len(), 1);
}
