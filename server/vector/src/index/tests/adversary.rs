//! Adversarial tests of task B, written by someone who did not write it. They attack the author's
//! declared deviations, resource bounds, filters, exactness and the rules of checkpoints.

use std::collections::BTreeMap;
use std::sync::Arc;

use nabedra_check::Gen;

use super::super::codec::{dot, exact, goodness};
use super::*;

// ----- Resource bounds.

/// Maintenance until every partition is ready, in at most `rounds` rounds.
fn settles<C: Codec>(ix: &mut Index<C>, rounds: usize) -> bool {
    for _ in 0..rounds {
        ix.run();
        if !ix.maintain(64) && ix.shared.dir.values().all(|m| m.state == State::Ready) {
            return true;
        }
    }
    false
}

/// VEC-G05 and the resource bound "one partition that cannot split": rows of one single direction
/// cannot be told apart, and a split must still end with partitions that are no larger than
/// `part_max`, in a bounded number of rounds and partitions.
fn identical(n: u64, distinct: u64, lengths: bool) {
    let mut ix = serving(Config::new(schema(4, &[], None), 2), ExactCodec, None);
    let write = |k: u64| {
        let len = if lengths { 1.0 + (k % 3) as f32 } else { 1.0 };
        let v = if k < distinct {
            [(k + 1) as f32, 1.0, -(k as f32), 0.5]
        } else {
            [len, 2.0 * len, 3.0 * len, 4.0 * len]
        };
        (k, Some(row(&v, Vec::new(), k)))
    };
    feed(&mut ix, RANGE, &page(0, (0..n).map(write).collect()));
    let ended = settles(&mut ix, 3000);
    let parts = ix.shared.dir.len();
    let ids = ix.shared.next_part;
    let biggest = ix.shared.dir.values().map(|m| m.entries).max().unwrap_or(0);
    println!("{n} rows, {distinct} distinct: ended {ended}, {parts} partitions, {ids} ids, biggest {biggest}");
    assert!(ended, "maintenance does not end after 3000 rounds ({ids} partition ids used)");
    assert!(biggest <= 512, "a partition of {biggest} rows is left");
    assert!(ids < 200, "{ids} partitions were made for {n} rows");
    let pin = latest(&mut ix, None);
    let hits = ix.search(&pin, &query(Op::Dot, &[1.0, 0.0, 0.0, 0.0], 1000)).unwrap();
    assert_eq!(hits.len(), 1000);
}

#[test]
fn adv07_identical_vectors_split_into_bounded_partitions() {
    identical(3000, 0, false);
}

#[test]
fn adv07_vectors_of_one_direction_and_other_lengths_split_into_bounded_partitions() {
    identical(3000, 0, true);
}

#[test]
fn adv07_a_crowd_of_duplicates_among_other_rows_ends() {
    identical(1500, 100, false);
}

fn rss_mb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let line = status.lines().find(|l| l.starts_with("VmRSS:")).unwrap_or("VmRSS: 0 kB");
    line.split_whitespace().nth(1).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0) / 1024
}

fn tenants(n: u64) -> (Index, std::time::Duration) {
    let cfg = Config::new(schema(2, &[ColumnType::Int], Some(0)), 4);
    let mut ix = serving(cfg, ExactCodec, None);
    let start = std::time::Instant::now();
    let mut at = 0;
    while at < n {
        let to = (at + 20_000).min(n);
        let changes = (at..to)
            .map(|k| (k, Some(row(&[1.0, k as f32], vec![Value::Int(k as i64)], k))))
            .collect();
        feed(&mut ix, RANGE, &page(at, changes));
        at = to;
    }
    (ix, start.elapsed())
}

/// A tenant with one row, and many tenants with one row each, take bounded time and memory, and a
/// query of one tenant does not look at the others.
#[test]
fn adv07_many_tenants_with_one_row_each() {
    let n: u64 = std::env::var("ADV_TENANTS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let before = rss_mb();
    let (mut ix, took) = tenants(n);
    let after = rss_mb();
    println!("{n} tenants: feed {took:?}, memory {} MB, {} partitions", after - before.min(after), ix.shared.dir.len());
    let start = std::time::Instant::now();
    let filter = Filter::Eq(0, Value::Int(n as i64 / 2));
    let pin = latest(&mut ix, Some(&filter));
    let pinned = start.elapsed();
    let q = Query { filter: Some(filter), ..query(Op::L2, &[1.0, 0.0], 10) };
    let hits = ix.search(&pin, &q).unwrap();
    println!("one tenant: pin {pinned:?}, query {:?}", start.elapsed());
    assert_eq!(hits.len(), 1);
    let start = std::time::Instant::now();
    let _ = latest(&mut ix, None);
    println!("pin without a tenant: {:?}", start.elapsed());
    let start = std::time::Instant::now();
    for _ in 0..10 {
        ix.maintain(64);
    }
    println!("10 maintenance calls: {:?}", start.elapsed());
    assert!(after - before.min(after) < 40 * n / 1000, "memory per tenant is too large");
}

/// Each query of one tenant costs no more than a few partitions, whatever the number of tenants.
#[test]
fn adv07_a_pin_of_one_tenant_does_not_depend_on_their_number() {
    let cost = |n: u64| {
        let (mut ix, _) = tenants(n);
        let filter = Filter::Eq(0, Value::Int(3));
        let start = std::time::Instant::now();
        for _ in 0..200 {
            let pin = latest(&mut ix, Some(&filter));
            drop(pin);
        }
        start.elapsed()
    };
    let (small, large) = (cost(2_000), cost(40_000));
    println!("small {small:?}, large {large:?}");
    assert!(large < small * 20, "a pin costs {large:?} against {small:?}: it grows with the tenants");
}

// ----- Approximate search under filters.

/// Estimates that are as wrong as their bound allows.
#[derive(Clone, Copy)]
struct Rough;

fn noise(x: &[f32]) -> f32 {
    let bits = x.iter().fold(7u32, |h, v| (h ^ v.to_bits()).wrapping_mul(0x9e37_79b1));
    (bits >> 16) as f32 / 32_768.0 - 1.0
}

impl Codec for Rough {
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
        out.extend(block.iter().map(|d| Estimate { cos: dot(q, d) + 0.2 * noise(d), err: 0.2001 }));
    }
    fn refine(&self, q: &Self::Query, block: &Self::Block, i: usize) -> Estimate {
        Estimate { cos: dot(q, &block[i]) - 0.03 * noise(&block[i]), err: 0.0301 }
    }
}


/// Rows 0..n. Row `k` matches the first `m` rows' predicates when `k < m`. The matching rows lie
/// away from the query, so that every near row is a trap.
fn trap_rows(n: u64, m: u64, _g: &mut Gen) -> Vec<(u64, Row)> {
    (0..n)
        .map(|k| {
            let hit = k < m;
            // Every direction is its own: identical rows are attacked by another test.
            let a = if hit { 3.0 + k as f32 * 0.0007 } else { k as f32 * 0.0002 };
            let len = if k % 5 == 0 { 3.0 } else { 1.0 };
            let tags: Vec<Arc<str>> = if hit { vec![Arc::from("a")] } else if k % 7 == 0 { Vec::new() } else { vec![Arc::from("b"), Arc::from("c")] };
            let cols = vec![
                Value::Int(if hit { 1 } else { 0 }),
                Value::Tags(tags.into()),
                Value::Bool(hit),
                Value::Text(Arc::from(if hit { "hit" } else { "miss" })),
            ];
            (k, row(&[a.cos() * len, a.sin() * len], cols, k))
        })
        .collect()
}

const COLS: [ColumnType; 4] = [ColumnType::Int, ColumnType::Tags, ColumnType::Bool, ColumnType::Text];

fn filters() -> Vec<(&'static str, Filter)> {
    let a: Arc<str> = Arc::from("a");
    let hit = || Filter::Eq(0, Value::Int(1));
    vec![
        ("eq", hit()),
        ("bool", Filter::Eq(2, Value::Bool(true))),
        ("contains", Filter::Contains(1, a.clone())),
        ("contains_any", Filter::ContainsAny(1, vec![a.clone(), Arc::from("zzz")])),
        ("starts_with", Filter::StartsWith(3, Arc::from("hi"))),
        ("range", Filter::Range(0, std::ops::Bound::Included(Value::Int(1)), std::ops::Bound::Unbounded)),
        ("not", Filter::Not(Box::new(Filter::Eq(0, Value::Int(0))))),
        ("and", Filter::And(vec![hit(), Filter::Contains(1, a.clone())])),
        ("or", Filter::Or(vec![hit(), Filter::Eq(0, Value::Int(99))])),
        ("not and", Filter::Not(Box::new(Filter::Or(vec![Filter::Eq(0, Value::Int(0)), Filter::Eq(2, Value::Bool(false))])))),
    ]
}

/// VEC-G05: if at least `limit` rows match, `limit` rows return, and each matches. For every number
/// of matches around the limit, every kind of predicate, every operation and every recall.
#[test]
fn adv07_approximate_search_returns_limit_rows_for_every_predicate() {
    let limit = 10usize;
    let n = 1500u64;
    let mut streamed = 0;
    for summary_pairs in [1 << 22, 3] {
        for m in [0, 1, limit as u64 - 1, limit as u64, limit as u64 + 1, 100, n] {
            let g = &mut Gen::from_seed(m + 1);
            let cfg = Config {
                flat: 50,
                part_min: 4,
                part_max: 16,
                summary_pairs,
                ..Config::new(schema(2, &COLS, None), 3)
            };
            let mut ix = serving(cfg, Rough, None);
            let rows = trap_rows(n, m, g);
            let changes = rows.iter().map(|(k, r)| (*k, Some(r.clone()))).collect();
            feed(&mut ix, RANGE, &page(0, changes));
            settle(&mut ix);
            for (name, f) in filters() {
                let matching: Vec<u64> = rows.iter().filter(|(_, r)| f.matches(&r.cols)).map(|(k, _)| *k).collect();
                let pin = latest(&mut ix, Some(&f));
                for op in [Op::Cosine, Op::Dot, Op::L2] {
                    for recall in [0.0, 0.5, 0.95, 1.0] {
                        let q = Query { filter: Some(f.clone()), exact: false, recall, ..query(op, &[1.0, 0.1], limit) };
                        let mut scan = ix.scan(&pin, &q, Box::new(RecallEstimate)).unwrap();
                        while ix.visit(&mut scan).unwrap() {}
                        streamed += usize::from(scan.visited > 0);
                        // Never post-filter: no row that fails the predicate is even scored.
                        assert!(scan.seen.len() <= matching.len(), "{name} m={m}: {} rows scored, {} match", scan.seen.len(), matching.len());
                        let hits = ix.finish(scan).unwrap();
                        let want = matching.len().min(limit);
                        assert_eq!(hits.len(), want, "{name} m={m} {op:?} recall {recall} pairs {summary_pairs}: {} match", matching.len());
                        for h in &hits {
                            let k = u64::from_be_bytes((&*h.key).try_into().unwrap());
                            assert!(matching.contains(&k), "{name}: {k} does not match");
                            let r = &rows[k as usize].1;
                            assert_eq!(Some(h.score), exact(op, r.vec.as_deref().unwrap(), &q.vec), "{name}: score of {k}");
                        }
                    }
                }
            }
        }
    }
    println!("{streamed} of the searches used the stream");
    assert!(streamed > 100, "the stream was hardly used: {streamed}");
}

// ----- Exactness.

fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let v = s - a;
    (s, (a - (s - v)) + (b - v))
}

/// Sum of products in about 106 bits.
fn dd_dot(a: &[f32], b: &[f32]) -> f64 {
    let (mut hi, mut lo) = (0.0f64, 0.0f64);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        let p = x * y;
        let e = x.mul_add(y, -p);
        let (s, t) = two_sum(hi, p);
        hi = s;
        lo += t + e;
    }
    hi + lo
}

/// A reference of the three operations that does not share code with the index.
fn reference(op: Op, a: &[f32], b: &[f32]) -> Option<f64> {
    match op {
        Op::Dot => Some(dd_dot(a, b)),
        Op::L2 => {
            let d: Vec<f32> = a.iter().zip(b).map(|(x, y)| x - y).collect();
            // x - y in f32 may round, so take the difference in f64 pieces.
            let (hi, lo): (Vec<f64>, Vec<f64>) = a
                .iter()
                .zip(b)
                .map(|(&x, &y)| {
                    let (s, e) = two_sum(f64::from(x), -f64::from(y));
                    (s, e)
                })
                .unzip();
            let _ = d;
            let mut sum = 0.0;
            let mut comp = 0.0;
            for (h, l) in hi.iter().zip(&lo) {
                let sq = h * h;
                let e = h.mul_add(*h, -sq);
                let (s, t) = two_sum(sum, sq);
                sum = s;
                comp += t + e + 2.0 * h * l;
            }
            Some((sum + comp).max(0.0).sqrt())
        }
        Op::Cosine => {
            let (na, nb) = (dd_dot(a, a).sqrt(), dd_dot(b, b).sqrt());
            if na == 0.0 || nb == 0.0 {
                return None;
            }
            Some((dd_dot(a, b) / na / nb).clamp(-1.0, 1.0))
        }
    }
}

fn to_f32(x: f64) -> f32 {
    (x as f32).clamp(f32::MIN, f32::MAX) + 0.0
}

fn ulps(a: f32, b: f32) -> i64 {
    let key = |x: f32| {
        let i = i64::from(x.to_bits() as i32);
        if i < 0 { i64::from(i32::MIN) - i } else { i }
    };
    (key(a) - key(b)).abs()
}

fn nasty(g: &mut Gen, dim: usize) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 0.5, 1e-3, 3e3, 1e19, 1e-20, 1e38, 3.0e38, 1e-39, 1e-44, 0.0]);
    let kind = g.below(8);
    (0..dim)
        .map(|i| match kind {
            0 => 0.0,
            1 => scale,
            2 if i % 2 == 0 => scale,
            2 => -scale,
            3 => f32::MIN_POSITIVE * g.below(5) as f32,
            _ => g.range_i64(-8, 8) as f32 / 8.0 * scale,
        })
        .collect()
}

/// VEC-G05: an exact query equals an independent brute force, also for lengths that differ widely,
/// zero, subnormal and huge vectors, ties and duplicates. Scores may differ by one rounding.
#[test]
fn adv05_exact_search_equals_an_independent_brute_force() {
    let dim = 4;
    let mut worst = 0;
    for seed in 0..300 {
        let g = &mut Gen::from_seed(seed);
        let mut ix = serving(Config { part_min: 2, part_max: 8, ..Config::new(schema(dim, &[], None), 2) }, ExactCodec, None);
        let n = 5 + g.below(60);
        let mut vecs: Vec<Vec<f32>> = (0..n).map(|_| nasty(g, dim)).collect();
        // Duplicates of one vector, which tie in every operation.
        let twin = vecs[0].clone();
        for _ in 0..g.below(4) {
            vecs.push(twin.clone());
        }
        let changes = vecs.iter().enumerate().map(|(k, v)| (k as u64, Some(row(v, Vec::new(), k as u64)))).collect();
        feed(&mut ix, RANGE, &page(0, changes));
        for _ in 0..g.below(30) {
            ix.maintain(3);
            ix.run();
        }
        let pin = latest(&mut ix, None);
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            let qv = if g.bool() { nasty(g, dim) } else { twin.clone() };
            if op == Op::Cosine && qv.iter().all(|x| *x == 0.0) {
                continue;
            }
            let limit = 1 + g.below(12) as usize;
            let hits = ix.search(&pin, &query(op, &qv, limit)).unwrap();
            let mut want: Vec<(f32, u64)> = vecs
                .iter()
                .enumerate()
                .filter_map(|(k, v)| Some((to_f32(reference(op, v, &qv)?), k as u64)))
                .collect();
            let good = |s: f32| goodness(op, s);
            want.sort_by(|a, b| good(b.0).total_cmp(&good(a.0)).then(a.1.cmp(&b.1)));
            want.truncate(limit);
            assert_eq!(hits.len(), want.len(), "seed {seed} {op:?}");
            for (h, w) in hits.iter().zip(&want) {
                let k = u64::from_be_bytes((&*h.key).try_into().unwrap());
                let u = ulps(h.score, w.0);
                worst = worst.max(u);
                // The same score, or a row that ties with it within a rounding.
                let mine = to_f32(reference(op, &vecs[k as usize], &qv).unwrap());
                assert!(ulps(mine, h.score) <= 2, "seed {seed} {op:?}: score {} of row {k} is {mine}; {:?}", h.score, vecs[k as usize]);
                assert!(u <= 2, "seed {seed} {op:?}: position holds {} want {}", h.score, w.0);
            }
            for w in hits.windows(2) {
                let (a, b) = (goodness(op, w[0].score), goodness(op, w[1].score));
                assert!(a > b || (a == b && w[0].key < w[1].key), "seed {seed} {op:?}: order or tie rule broken");
            }
        }
    }
    println!("worst difference: {worst} ulps");
}

/// Known limit of the chosen accumulation (decision L1): a sum in f64 loses terms that cancel.
#[test]
#[ignore = "known precision limit of an f64 accumulation, kept as a record"]
fn adv05_scores_survive_cancellation() {
    let mut ix = serving(Config::new(schema(3, &[], None), 1), ExactCodec, None);
    // Row 1 has the dot product 1 with the query, row 0 has 0. A sum in f64 order loses the 1.
    let rows = [([1e30f32, 0.0, -1e30], 0u64), ([1e30, 1.0, -1e30], 1)];
    let changes = rows.iter().map(|(v, k)| (*k, Some(row(v, Vec::new(), *k)))).collect();
    feed(&mut ix, RANGE, &page(0, changes));
    let pin = latest(&mut ix, None);
    let hits = ix.search(&pin, &query(Op::Dot, &[1e30, 1.0, 1e30], 2)).unwrap();
    let got: Vec<(u64, f32)> = hits.iter().map(|h| (images(std::slice::from_ref(h))[0], h.score)).collect();
    let want = to_f32(reference(Op::Dot, &rows[1].0, &[1e30, 1.0, 1e30]).unwrap());
    println!("got {got:?}, reference {want}");
    assert_eq!(got[0].0, 1, "{got:?}");
}

// ----- Seeds.

/// Every seed makes another run, and one seed makes the same run twice.
#[test]
fn adv08_seeds_differ_and_replay() {
    let fingerprint = |seed: u64| {
        let g = &mut Gen::from_seed(seed);
        (0..64).map(|_| g.u64()).fold(0u64, |h, x| h.rotate_left(5) ^ x)
    };
    let mut all: BTreeMap<u64, u64> = BTreeMap::new();
    for seed in 0..6000 {
        assert_eq!(fingerprint(seed), fingerprint(seed));
        *all.entry(fingerprint(seed)).or_default() += 1;
    }
    assert_eq!(all.len(), 6000, "two seeds draw the same");
}

#[test]
#[ignore = "measurement: no budget is stated; run with --ignored to see the cost per tenant"]
fn adv07_a_tenant_with_one_row_costs_little_beyond_the_row() {
    let dim = 1024usize;
    let n = 4000u64;
    let cfg = Config::new(schema(dim, &[ColumnType::Int], Some(0)), 4);
    let mut ix = serving(cfg, ExactCodec, None);
    let before = rss_mb();
    let mut at = 0;
    while at < n {
        let to = (at + 1000).min(n);
        let changes = (at..to)
            .map(|k| {
                let v: Vec<f32> = (0..dim).map(|i| ((k as usize * 31 + i * 7) % 97) as f32 - 48.0).collect();
                (k, Some(row(&v, vec![Value::Int(k as i64)], k)))
            })
            .collect();
        feed(&mut ix, RANGE, &page(at, changes));
        at = to;
    }
    let used = rss_mb() - before.min(rss_mb());
    let raw = n * dim as u64 * 4 / (1 << 20);
    println!("{n} tenants of {dim} dimensions: {used} MB for {raw} MB of vectors");
    assert!(used <= raw * 4, "{used} MB for {raw} MB of vectors: more than 4 times");
}

// ----- More bounds.

/// Tenants that were deleted leave nothing behind once retention passed: no partition, tree, summary
/// entry or row, so that memory follows the live data and not the history.
#[test]
fn adv07_deleted_tenants_leave_no_partitions_behind() {
    let cfg = Config::new(schema(2, &[ColumnType::Int], Some(0)), 2);
    let mut ix = serving(cfg, ExactCodec, None);
    let n = 2000u64;
    let writes = (0..n).map(|k| (k, Some(row(&[1.0, k as f32], vec![Value::Int(k as i64)], k)))).collect();
    feed(&mut ix, RANGE, &page(0, writes));
    let deletes = (0..n).map(|k| (k, None)).collect();
    feed(&mut ix, RANGE, &page(n, deletes));
    ix.advance_low(RANGE, 2 * n).unwrap();
    for _ in 0..50 {
        ix.run();
        ix.maintain(256);
    }
    let census = ix.shared.rows.census().len();
    println!("after the delete: {} partitions, {} trees, {census} slots", ix.shared.dir.len(), ix.shared.roots.len());
    assert_eq!(census, 0, "rows of deleted keys stay");
    assert!(ix.shared.dir.len() < 100, "{} empty partitions stay for tenants that are gone", ix.shared.dir.len());
    assert!(ix.shared.roots.len() < 100, "{} empty trees stay", ix.shared.roots.len());
}

/// The same after the range is dropped.
#[test]
fn adv07_a_dropped_range_leaves_no_partitions_behind() {
    let cfg = Config::new(schema(2, &[ColumnType::Int], Some(0)), 2);
    let mut ix = serving(cfg, ExactCodec, None);
    let writes = (0..500).map(|k| (k, Some(row(&[1.0, k as f32], vec![Value::Int(k as i64)], k)))).collect();
    feed(&mut ix, RANGE, &page(0, writes));
    ix.drop_range(RANGE).unwrap();
    for _ in 0..20 {
        ix.run();
        ix.maintain(256);
    }
    assert_eq!(ix.shared.rows.census().len(), 0);
    assert!(ix.shared.dir.len() < 20, "{} empty partitions stay after a drop", ix.shared.dir.len());
}

/// A configuration that can never finish a split is refused when the index is made.
#[test]
fn adv07_a_configuration_without_copies_is_refused() {
    let cfg = Config { copies: 0, ..Config::new(schema(2, &[], None), 1) };
    assert!(Index::<ExactCodec>::new(cfg, ExactCodec, None).is_err(), "copies: 0 never ends a drain");
}

/// A queue of one message and one core still makes progress with splits, merges and moves.
#[test]
fn adv07_queues_of_one_make_progress() {
    for (shards, queue) in [(1, 1), (2, 1), (3, 2)] {
        let g = &mut Gen::from_seed(5);
        let cfg = Config {
            queue,
            max_flight: 2,
            part_min: 2,
            part_max: 6,
            ..Config::new(schema(2, &[], None), shards)
        };
        let mut ix = serving(cfg, ExactCodec, None);
        let mut at = 0;
        for _ in 0..30 {
            let changes: Vec<_> = (0..20)
                .map(|_| {
                    let k = g.below(150);
                    let a = g.below(6283) as f32 / 1000.0;
                    (k, if g.below(4) == 0 { None } else { Some(row(&[a.cos(), a.sin()], Vec::new(), k)) })
                })
                .collect();
            feed(&mut ix, RANGE, &page(at, changes));
            at += 20;
            for _ in 0..g.below(5) {
                ix.maintain(4);
                ix.step(g.below(shards as u64) as usize);
            }
            if g.bool() {
                let _ = ix.advance_low(RANGE, at - g.below(at.min(10)));
            }
        }
        assert!(settles(&mut ix, 5000), "shards {shards} queue {queue}: maintenance does not end");
        let pin = latest(&mut ix, None);
        let hits = ix.search(&pin, &query(Op::Dot, &[1.0, 0.0], 1000)).unwrap();
        let sh = &ix.shared;
        assert!(sh.queues.iter().all(|q| q.is_empty()) && sh.inbox.is_empty());
        println!("shards {shards} queue {queue}: {} rows, {} partitions", hits.len(), sh.dir.len());
    }
}

/// The summary of a column with more values than it counts (4,194,304 pairs) never rules out a
/// partition that holds a match, before, at and after the limit, and keeps estimating.
#[test]
fn adv07_a_summary_past_its_counting_limit_still_never_rules_out_a_match() {
    use super::super::filter::Summary;
    const LIMIT: usize = 1 << 22;
    let mut s = Summary::new(1, LIMIT);
    let part = |i: usize| PartId((i % 3) as u32);
    for i in 0..LIMIT + 8 {
        s.add(part(i), &[Value::Int(i as i64)]);
        if i + 1 >= LIMIT - 2 {
            for probe in [i, i - 1, 0, LIMIT / 2] {
                let f = Filter::Eq(0, Value::Int(probe as i64));
                assert!(s.may_match(&f, part(probe)), "after {} pairs: {probe} is ruled out", i + 1);
                assert!(s.estimate(&f) >= 1.0, "after {} pairs: {probe} is estimated at {}", i + 1, s.estimate(&f));
            }
        }
    }
    // Rows leave again: still no false negative, and a value that is gone may be claimed.
    for i in 0..LIMIT / 2 {
        s.remove(part(i), &[Value::Int(i as i64)]);
    }
    let f = Filter::Eq(0, Value::Int(LIMIT as i64 + 3));
    assert!(s.may_match(&f, part(LIMIT + 3)));
}

/// Pruning by the codes' error bound loses nothing at the largest dimension, where f32 sums of
/// same-signed terms drift most.
#[test]
fn adv05_the_error_bound_holds_at_4096_dimensions() {
    struct Never;
    impl StopRule for Never {
        fn enough(&mut self, _: &Progress<'_>) -> bool {
            false
        }
    }
    let dim = 4096;
    let g = &mut Gen::from_seed(3);
    let cfg = Config { flat: 0, part_min: 4, part_max: 16, ..Config::new(schema(dim, &[], None), 2) };
    let mut ix = serving(cfg, ExactCodec, None);
    // Positive, nearly equal components: every partial sum is large and rounds.
    let vec = |g: &mut Gen, spread: u64| -> Vec<f32> { (0..dim).map(|_| 1.0 + g.below(spread) as f32 / 1000.0).collect() };
    let rows: Vec<Vec<f32>> = (0..120).map(|_| { let s = 1 + g.below(300); vec(g, s) }).collect();
    let changes = rows.iter().enumerate().map(|(k, v)| (k as u64, Some(row(v, Vec::new(), k as u64)))).collect();
    feed(&mut ix, RANGE, &page(0, changes));
    settle(&mut ix);
    let pin = latest(&mut ix, None);
    for op in [Op::Cosine, Op::Dot, Op::L2] {
        for _ in 0..4 {
            let q = vec(g, 400);
            let want = ix.search(&pin, &query(op, &q, 5)).unwrap();
            let approx = Query { exact: false, ..query(op, &q, 5) };
            let mut scan = ix.scan(&pin, &approx, Box::new(Never)).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            let got = ix.finish(scan).unwrap();
            let keys = |h: &[Hit]| h.iter().map(|h| (h.key.clone(), h.score)).collect::<Vec<_>>();
            assert_eq!(keys(&got), keys(&want), "{op:?}: pruning lost a row at {dim} dimensions");
        }
    }
}

/// Directions that differ by less than what an f32 cosine resolves (about 3.5e-4 rad near 1) still
/// prune right: a query that equals a stored vector finds it, whatever the partitions look like.
#[test]
fn adv07_pruning_by_radius_survives_float_error_in_tight_clusters() {
    struct Never;
    impl StopRule for Never {
        fn enough(&mut self, _: &Progress<'_>) -> bool {
            false
        }
    }
    let mut lost = Vec::new();
    for seed in 0..40u64 {
        let g = &mut Gen::from_seed(seed);
        let cfg = Config { flat: 0, part_min: 4, part_max: 16, ..Config::new(schema(3, &[], None), 2) };
        let mut ix = serving(cfg, ExactCodec, None);
        let len = *g.pick(&[1.0f32, 1e3, 1e5]);
        let spread = *g.pick(&[2e-5f32, 1e-4, 5e-4, 2e-3]);
        let centres = [[1.0f32, 0.0, 0.0], [0.0, 1.0, 0.0], [0.6, 0.0, 0.8]];
        let rows: Vec<Vec<f32>> = (0..400)
            .map(|_| {
                let c = g.pick(&centres);
                let d: Vec<f32> = (0..3).map(|i| c[i] + (g.below(2001) as f32 / 1000.0 - 1.0) * spread).collect();
                let n = d.iter().map(|x| x * x).sum::<f32>().sqrt();
                d.iter().map(|x| x / n * len).collect()
            })
            .collect();
        let changes = rows.iter().enumerate().map(|(k, v)| (k as u64, Some(row(v, Vec::new(), k as u64)))).collect();
        feed(&mut ix, RANGE, &page(0, changes));
        // Clusters this tight may not split (another test); the index is read as it is.
        settles(&mut ix, 300);
        let pin = latest(&mut ix, None);
        for op in [Op::L2, Op::Dot, Op::Cosine] {
            for _ in 0..10 {
                let q = g.pick(&rows).clone();
                let want = ix.search(&pin, &query(op, &q, 3)).unwrap();
                let approx = Query { exact: false, ..query(op, &q, 3) };
                let mut scan = ix.scan(&pin, &approx, Box::new(Never)).unwrap();
                while ix.visit(&mut scan).unwrap() {}
                let got = ix.finish(scan).unwrap();
                let keys = |h: &[Hit]| h.iter().map(|h| (h.key.clone(), h.score)).collect::<Vec<_>>();
                if keys(&got) != keys(&want) {
                    lost.push((seed, op, len, spread));
                }
            }
        }
    }
    assert!(lost.is_empty(), "pruning lost rows in {} of 1200 queries, for example {:?}", lost.len(), &lost[..lost.len().min(5)]);
}

/// Rows that differ, but by less than an f32 cosine resolves, split like any others: maintenance
/// ends with partitions of at most  rows.
#[test]
fn adv07_near_duplicates_closer_than_f32_resolves_split_too() {
    let g = &mut Gen::from_seed(9);
    let mut ix = serving(Config::new(schema(8, &[], None), 2), ExactCodec, None);
    let base = [0.3f32, -0.2, 0.5, 0.1, 0.7, -0.4, 0.2, 0.6];
    let changes: Vec<_> = (0..2000u64)
        .map(|k| {
            let v: Vec<f32> = base.iter().map(|x| x + (g.below(2001) as f32 / 1000.0 - 1.0) * 1e-5).collect();
            (k, Some(row(&v, Vec::new(), k)))
        })
        .collect();
    feed(&mut ix, RANGE, &page(0, changes));
    let ended = settles(&mut ix, 3000);
    let biggest = ix.shared.dir.values().map(|m| m.entries).max().unwrap_or(0);
    println!("ended {ended}, {} partitions, {} ids, biggest {biggest}", ix.shared.dir.len(), ix.shared.next_part);
    assert!(ended && biggest <= 512, "ended {ended}, biggest partition {biggest}");
}

/// With a stop rule that never stops, an approximate scan of zero, subnormal, huge and tiny vectors
/// returns what the exact scan does, scores and ties included.
#[test]
fn adv07_pruning_is_right_for_zero_subnormal_and_huge_vectors() {
    struct Never;
    impl StopRule for Never {
        fn enough(&mut self, _: &Progress<'_>) -> bool {
            false
        }
    }
    let dim = 4;
    for seed in 0..400 {
        let g = &mut Gen::from_seed(seed);
        let cfg = Config { flat: 0, part_min: 2, part_max: 8, ..Config::new(schema(dim, &[], None), 2) };
        let mut ix = serving(cfg, ExactCodec, None);
        let n = 20 + g.below(80);
        let vecs: Vec<Vec<f32>> = (0..n).map(|_| nasty(g, dim)).collect();
        let changes = vecs.iter().enumerate().map(|(k, v)| (k as u64, Some(row(v, Vec::new(), k as u64)))).collect();
        feed(&mut ix, RANGE, &page(0, changes));
        for _ in 0..40 {
            ix.maintain(4);
            ix.run();
        }
        let pin = latest(&mut ix, None);
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            let q = nasty(g, dim);
            if op == Op::Cosine && q.iter().all(|x| *x == 0.0) {
                continue;
            }
            let limit = 1 + g.below(10) as usize;
            let want = ix.search(&pin, &query(op, &q, limit)).unwrap();
            let approx = Query { exact: false, ..query(op, &q, limit) };
            let mut scan = ix.scan(&pin, &approx, Box::new(Never)).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            let got = ix.finish(scan).unwrap();
            let scores = |h: &[Hit]| h.iter().map(|h| h.score).collect::<Vec<_>>();
            // Rows that tie after the rounding to f32 may be pruned by their unrounded bound: see the other test.
            assert_eq!(scores(&got), scores(&want), "seed {seed} {op:?} query {q:?}");
        }
    }
}

/// Merging only goes where there is room: with no new rows, no partition grows beyond `part_max`
/// while the index shrinks and merges, step by step.
#[test]
fn adv07_a_merge_never_makes_a_partition_larger_than_part_max() {
    for seed in 0..20 {
        let g = &mut Gen::from_seed(seed);
        let cfg = Config { part_min: 4, part_max: 12, queue: 8, ..Config::new(schema(3, &[], None), 2) };
        let mut ix = serving(cfg, ExactCodec, None);
        let n = 300u64;
        let angle = |k: u64| k as f32 * 0.0211;
        let writes = (0..n).map(|k| (k, Some(row(&[angle(k).cos(), angle(k).sin(), (k as f32 * 0.37).sin()], Vec::new(), k)))).collect();
        feed(&mut ix, RANGE, &page(0, writes));
        assert!(settles(&mut ix, 5000));
        // Most rows go: partitions fall below `part_min` and want to merge.
        let gone: Vec<_> = (0..n).filter(|k| k % 7 != 0 || g.below(3) == 0).map(|k| (k, None)).collect();
        let at = ix.status(RANGE).unwrap().delivered;
        feed(&mut ix, RANGE, &page(at, gone));
        let applied = ix.status(RANGE).unwrap().applied;
        ix.advance_low(RANGE, applied).unwrap();
        for step in 0..3000 {
            ix.run();
            ix.maintain(1 + g.below(3) as usize);
            if g.bool() {
                ix.step(g.below(2) as usize);
            }
            let biggest = ix.shared.dir.values().map(|m| m.entries).max().unwrap_or(0);
            assert!(biggest <= 12, "seed {seed} step {step}: a partition holds {biggest} entries, part_max is 12");
        }
    }
}
