//! Review rv-dense (2026-11-27): the dense index with the real direction codec, and cases the
//! earlier suites did not reach.

use std::collections::BTreeMap;

use nabedra_check::prop::{Config as Runs, check_with};

use super::super::codec::{exact, goodness};
use super::*;

type Model = BTreeMap<u64, Row>;

fn brute(rows: &Model, q: &Query) -> Vec<(u64, f32)> {
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

fn vector(g: &mut nabedra_check::Gen, dim: usize) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 1.0, 0.25, 7.0, 1e3, 1e-4, 1e30, 1e-42, 0.0]);
    let part = |g: &mut nabedra_check::Gen| g.range_i64(-8, 8) as f32 / 8.0 * scale;
    (0..dim).map(|_| part(g)).collect()
}

/// VEC-G05 and VEC-G03 with the direction codec that production uses: exact equals brute force,
/// and an approximate query returns `limit` rows when that many match, each with its exact score.
fn churn_with_direction_codes(dim: usize, name: &str) {
    let runs = Runs::default();
    let runs = Runs {
        cases: runs.cases / 4,
        ..runs
    };
    check_with(runs, name, |g| {
        let cfg = Config {
            flat: 0,
            part_min: 2,
            part_max: 8,
            queue: 16,
            ..Config::new(schema(dim, &[ColumnType::Int], None), 3)
        };
        let codec = DirectionCodec::new(dim, g.u64()).unwrap();
        let mut ix = serving(cfg, codec, None);
        let mut rows = Model::new();
        let mut at = 0;
        for _ in 0..g.below(10) + 1 {
            let mut changes = Vec::new();
            for _ in 0..g.below(24) {
                let k = g.below(80);
                let change = match g.below(6) {
                    0 => None,
                    _ => Some(row(&vector(g, dim), vec![Value::Int(g.range_i64(0, 3))], k)),
                };
                match change.clone() {
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
            ix.run();
            if g.bool() {
                let _ = ix.advance_low(RANGE, g.below(at + 1));
            }
            let filter = g
                .bool()
                .then(|| Filter::Eq(0, Value::Int(g.range_i64(0, 3))));
            let pin = latest(&mut ix, None);
            let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
            let vec = vector(g, dim);
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
                return Err(format!("exact {q:?}\n got {got:?}\nwant {best:?}"));
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
        }
        Ok(())
    });
}

#[test]
fn vs01_direction_codes_under_churn_small_dimension() {
    churn_with_direction_codes(4, "vs01_direction_codes_under_churn_small_dimension");
}

#[test]
fn vs01_direction_codes_under_churn_padded_dimension() {
    churn_with_direction_codes(70, "vs01_direction_codes_under_churn_padded_dimension");
}

/// VEC-G11: a query's CPU budget bounds its work. Filter evaluation is work too: here a filter of
/// 20,000 leaves runs over 300 rows (6 million leaf tests), and a budget of 1,000 work units, of
/// which scoring 300 rows of 2 dimensions takes 600, should end it. Today filters are never
/// charged, so it returns `Ok`.
#[test]
#[ignore = "VEC-BUG-filter-work-uncharged"]
fn vs02_filter_evaluation_counts_against_the_cpu_budget() {
    use crate::runtime::Budget;

    let mut ix = serving(
        Config::new(schema(2, &[ColumnType::Int], None), 1),
        ExactCodec,
        None,
    );
    let rows = (0..300).map(|k| {
        (
            k,
            Some(row(&[k as f32, 1.0], vec![Value::Int(k as i64)], k)),
        )
    });
    feed(&mut ix, RANGE, &page(0, rows.collect()));
    // Matches every row, so no summary can skip a partition: every leaf runs on every row.
    let none = Filter::Or(
        (0..20_000)
            .map(|i| Filter::Eq(0, Value::Int(-1 - i)))
            .collect(),
    );
    let wide = Filter::Not(Box::new(none));
    let pin = latest(&mut ix, None);
    let q = Query {
        filter: Some(wide),
        ..query(Op::Dot, &[1.0, 0.0], 10)
    };
    let budget = Budget {
        cpu: 1_000,
        ..Budget::UNLIMITED
    };
    let mut scan = ix
        .scan_within(&pin, &q, Box::new(RecallEstimate), budget)
        .unwrap();
    let mut result = Ok(true);
    while let Ok(true) = result {
        result = ix.visit(&mut scan);
    }
    let charged = scan.usage().unwrap().cpu;
    assert_eq!(
        result.and_then(|_| ix.finish(scan)),
        Err(Error::OverBudget("cpu")),
        "6,000,000 leaf tests were charged {charged} units"
    );
}

/// adv17 holds that a checkpoint file with right checksums and wrong contents (a buggy writer or a
/// hostile peer, plan section 5 "copies the checkpoint from the other search replica") may make
/// recovery fail, never panic. adv17 runs with `ExactCodec` only. With the production
/// `DirectionCodec`, a partition centroid that is not a unit vector reaches the asserts of
/// `CodeBlock::push` through `Shard::put` and recovery panics.
#[test]
fn vs03_a_forged_centroid_fails_recovery_without_a_panic() {
    recovers_forged("a centroid of length 2", |bytes| {
        bytes[24..28].copy_from_slice(&2.0f32.to_le_bytes());
    });
    recovers_forged("a centroid that is not a number", |bytes| {
        bytes[28..32].copy_from_slice(&f32::NAN.to_le_bytes());
    });
}

/// The same for a row of a directed partition whose vector has no direction (decision L3: such
/// rows live in the undirected list): `CodeBlock::push` asserts a positive length.
#[test]
fn vs03_a_forged_zero_vector_in_a_directed_partition_fails_recovery_without_a_panic() {
    recovers_forged("a zero vector", |bytes| {
        // Range (16), key length (4) and key (8), four positions (32), vector length (4).
        assert_eq!(u32::from_le_bytes(bytes[48..52].try_into().unwrap()), 8, "a key");
        assert_eq!(u32::from_le_bytes(bytes[92..96].try_into().unwrap()), 8, "a vector");
        bytes[96..104].fill(0);
    });
}

/// Writes a checkpoint of three rows in a directed partition with the direction codec, edits its
/// first frame and puts the right checksum back, and recovers: recovery must report the range
/// damaged, out of service, and never panic.
fn recovers_forged(what: &str, edit: impl FnOnce(&mut Vec<u8>)) {
    use std::path::Path;

    use nabedra_rt::fs::{FaultFs, Fs, OpenMode};

    use super::super::checkpoint::crc32c;

    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/data"));
    let store = || Store::new(Arc::new(fs.clone()), "/data/ck");
    let cfg = Config::new(schema(2, &[], None), 1);
    let codec = || DirectionCodec::new(2, 5).unwrap();
    let mut ix = serving(cfg.clone(), codec(), Some(store()));
    let rows = (1..=3).map(|k| (k, Some(row(&[1.0, k as f32], vec![], k))));
    feed(&mut ix, RANGE, &page(0, rows.collect()));
    ix.checkpoint().unwrap();
    drop(ix);

    let segment = Path::new("/data/ck/seg-0000000000000000");
    let mut bytes = fs.contents(segment).unwrap();
    // magic (8), frame length and checksum (8), partition (4), centroid length (4), centroid.
    assert_eq!(
        u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
        8,
        "a directed partition"
    );
    edit(&mut bytes);
    let len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let crc = crc32c(&bytes[16..16 + len]).to_le_bytes();
    bytes[12..16].copy_from_slice(&crc);
    let f = fs.open(segment, OpenMode::ReadWrite).unwrap();
    f.set_len(0).unwrap();
    f.write_all_at(&bytes, 0).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (ix, damage) = Index::recover(cfg.clone(), codec(), store()).unwrap();
        let ranges: Vec<RangeId> = damage.iter().flat_map(|d| d.ranges.clone()).collect();
        (ranges, ix.status(RANGE).unwrap().damage.is_some())
    }));
    let Ok((ranges, failed)) = outcome else {
        panic!("recovery of {what} panics");
    };
    assert_eq!(ranges, [RANGE], "recovery of {what} reports the damage of its range");
    assert!(failed, "recovery of {what} leaves its range in service");
}

/// A vector store whose reads can be made to fail.
#[derive(Debug, Default)]
struct Flaky {
    ram: crate::store::RamVectors,
    broken: std::sync::atomic::AtomicBool,
}

impl crate::store::Vectors for Flaky {
    fn put(&self, key: crate::store::VecKey, vec: &[f32]) -> Result<()> {
        self.ram.put(key, vec)
    }
    fn get_many(&self, keys: &[crate::store::VecKey]) -> Vec<Result<Arc<[f32]>>> {
        if self.broken.load(Ordering::SeqCst) {
            return keys
                .iter()
                .map(|_| Err(Error::Store("unreadable".into())))
                .collect();
        }
        self.ram.get_many(keys)
    }
    fn release(&self, key: crate::store::VecKey) {
        self.ram.release(key);
    }
    fn on_disk(&self) -> bool {
        false
    }
    fn stats(&self) -> crate::store::Stats {
        self.ram.stats()
    }
}

/// VEC-G11 and the slot rule of `rows.rs`: a version is held only while something names it. An
/// append whose vector the store cannot read is dropped by `Shard::put` without forgetting the name
/// that `put_version` took, so after the range fails and is dropped its slot and stored vector stay
/// forever. The same holds for a copy in `Shard::land`.
#[test]
fn vs04_an_unreadable_append_releases_its_version_once_the_range_is_gone() {
    let store = Arc::new(Flaky::default());
    let cfg = Config {
        vectors: Some(store.clone()),
        ..Config::new(schema(2, &[], None), 1)
    };
    let mut ix = serving(cfg, ExactCodec, None);
    let rows = (1..=3).map(|k| (k, Some(row(&[1.0, k as f32], vec![], k))));
    assert_eq!(ix.apply(RANGE, &page(0, rows.collect())), Ok(3));
    assert!(ix.pump());
    store.broken.store(true, Ordering::SeqCst);
    while ix.step(0) {}
    assert!(
        ix.status(RANGE).unwrap().damage.is_some(),
        "the range failed"
    );
    store.broken.store(false, Ordering::SeqCst);
    ix.drop_range(RANGE).unwrap();
    for _ in 0..100 {
        ix.run();
        ix.maintain(64);
    }
    let held: Vec<_> = ix
        .shared
        .rows
        .census()
        .into_iter()
        .map(|c| (c.slot, c.versions))
        .collect();
    assert_eq!(
        store.stats().vectors,
        0,
        "stored vectors of a dropped range; slots held: {held:?}"
    );
    assert!(held.is_empty(), "slots of a dropped range: {held:?}");
}
