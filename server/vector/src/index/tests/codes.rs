//! The direction codec on a worker (plan section 5, "Partition" and "Dense scan"): 1-bit codes in
//! RAM and charged to the code memory, 4-bit extensions in RAM while that memory lasts and on NVMe
//! after it, partitions owned by one core each, and scans 1-bit -> extension -> raw f32 whose exact
//! answers stay brute force (decision L1) wherever the extensions live.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::fs::os;

use super::super::codec::{exact, goodness};
use super::super::shard::ext_key;
use super::*;
use crate::runtime::{Budget, Memory, Tier};
use crate::store::{Backend, Disk, Extensions, open};

/// Files of one extension store, one per core, removed at the end.
struct Disks {
    dir: PathBuf,
    disks: Vec<Arc<dyn Disk>>,
}

impl Drop for Disks {
    fn drop(&mut self) {
        let _ = os::remove_dir_all(&self.dir);
    }
}

fn disks(name: &str, cores: usize) -> Disks {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/vector-codes-tests");
    let dir = PathBuf::from(base).join(format!("{name}-{}-{n}", std::process::id()));
    let _ = os::remove_dir_all(&dir);
    os::create_dir_all(&dir).unwrap();
    let disks = (0..cores)
        .map(|c| open(&dir.join(format!("ext-{c}")), Backend::Pread, 2).unwrap())
        .collect();
    Disks { dir, disks }
}

/// An extension store over fresh files whose code memory is `limit` bytes.
fn codes(name: &str, codec: &DirectionCodec, cores: usize, limit: u64) -> (Disks, Arc<Extensions>) {
    let d = disks(name, cores);
    let memory = Arc::new(Memory::new(limit));
    let ext = Extensions::new(codec.ext_bytes(), memory, d.disks.clone()).unwrap();
    (d, Arc::new(ext))
}

/// Small partitions on three cores, so that splits, merges and moves cross cores.
fn config(dim: usize, store: Option<Arc<Extensions>>) -> Config {
    Config {
        flat: 0,
        part_min: 2,
        part_max: 8,
        queue: 6,
        codes: store,
        ..Config::new(schema(dim, &[ColumnType::Int], None), 3)
    }
}

/// What the index holds of codes: bytes of all blocks, and extensions in RAM and on NVMe.
#[derive(Debug, PartialEq)]
struct Held {
    codes: u64,
    ram: usize,
    nvme: usize,
    /// Coded entries without an extension.
    missing: usize,
}

fn held(ix: &Index<DirectionCodec>) -> Held {
    let store = ix.shared.codes.as_ref().unwrap();
    let mut h = Held {
        codes: 0,
        ram: 0,
        nvme: 0,
        missing: 0,
    };
    for shard in &ix.shards {
        for (part, p) in &shard.parts {
            for b in &p.bands {
                h.codes += ix.codec.held(&b.codes);
                let coded = b.entries.iter().filter(|_| p.centroid.is_some());
                for e in coded {
                    match store.tier(&ext_key(*part, e.id)) {
                        Some(Tier::Ram) => h.ram += 1,
                        Some(Tier::Nvme) => h.nvme += 1,
                        None => h.missing += 1,
                    }
                }
            }
        }
    }
    h
}

/// The code memory is charged exactly what the blocks and the extensions in RAM hold, and the store
/// holds an extension for every coded entry and nothing else.
fn charges(ix: &Index<DirectionCodec>) -> std::result::Result<(), String> {
    let store = ix.shared.codes.as_ref().unwrap();
    let h = held(ix);
    let used = store.memory().used();
    let want = (h.codes, (h.ram * store.bytes()) as u64);
    if used != want || store.counts() != (h.ram, h.nvme) || h.missing > 0 {
        let counts = store.counts();
        return Err(format!("charged {used:?}, store {counts:?}, held {h:?}"));
    }
    Ok(())
}

fn vector(g: &mut Gen, dim: usize) -> Vec<f32> {
    let scale = *g.pick(&[1.0f32, 1.0, 0.25, 8.0, 0.0]);
    (0..dim).map(|_| g.range_i64(-8, 8) as f32 / 8.0 * scale).collect()
}

type Model = BTreeMap<u64, Row>;

/// Brute force: the best rows, best first and then by key, with scores of decision L1.
fn brute(rows: &Model, q: &Query) -> Vec<(u64, f32)> {
    let mut all: Vec<(u64, f32)> = rows
        .iter()
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

fn upserts(at: Pos, rows: &[(u64, Vec<f32>)]) -> Page {
    let changes = rows
        .iter()
        .map(|(k, v)| (*k, Some(row(v, vec![Value::Int(*k as i64 % 5)], *k))))
        .collect();
    page(at, changes)
}

/// Extensions take the code memory that the 1-bit codes leave, go to NVMe once it is gone, and
/// come back to RAM when entries leave and give memory back.
#[test]
fn vs17_extensions_spill_to_nvme_when_the_code_memory_is_gone_and_come_back() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 5).unwrap();
    let ext = codec.ext_bytes() as u64;
    // 1-bit codes of 100 rows and about 40 extensions.
    let probe = {
        let mut block = codec.block();
        let unit = vec![0.125f32; dim];
        let d = codec.rotate(&unit);
        codec.push_detached(&mut block, &d, &d, 1.0);
        codec.held(&block)
    };
    let limit = 100 * probe + 40 * ext;
    let (_d, store) = codes("spill", &codec, 3, limit);
    // Partitions that do not split, so that no split puts the extensions again (review rv-b2b).
    let cfg = Config {
        part_max: 512,
        ..config(dim, Some(store.clone()))
    };
    let mut ix = serving(cfg, codec, None);
    let g = &mut Gen::from_seed(3);
    let first: Vec<(u64, Vec<f32>)> = (0..100).map(|k| (k, vector(g, dim))).collect();
    feed(&mut ix, RANGE, &upserts(0, &first));
    settle(&mut ix);
    charges(&ix).unwrap();
    let h = held(&ix);
    let (codes_bytes, ram_bytes) = store.memory().used();
    assert!(h.ram > 0 && h.nvme > 0, "some in RAM and some on NVMe: {h:?}");
    assert!(codes_bytes + ram_bytes <= limit, "within the limit");
    assert!(codes_bytes + ram_bytes + ext > limit, "no room for one more in RAM: {h:?}");
    // Delete most rows, and let retention and maintenance collect them.
    let gone: Vec<(u64, Option<Row>)> = (0..90).map(|k| (k, None)).collect();
    feed(&mut ix, RANGE, &page(100, gone));
    ix.advance_low(RANGE, 190).unwrap();
    settle(&mut ix);
    charges(&ix).unwrap();
    let after = held(&ix);
    assert!(after.ram + after.nvme <= 10, "{after:?}");
    assert_eq!(after.nvme, 0, "the rest came back to RAM: {after:?}");
    // New entries find room in RAM again, up to the limit.
    let more: Vec<(u64, Vec<f32>)> = (200..230).map(|k| (k, vector(g, dim))).collect();
    feed(&mut ix, RANGE, &upserts(190, &more));
    settle(&mut ix);
    charges(&ix).unwrap();
    assert_eq!(held(&ix).nvme, 0, "{:?}", held(&ix));
    ix.drop_range(RANGE).unwrap();
    assert_eq!(store.memory().used(), (0, 0), "a drop gives every byte back");
    assert_eq!(store.counts(), (0, 0));
}

/// Exact and approximate answers are the same whether extensions live in the blocks, in RAM, on
/// NVMe or partly on each, and exact ones are brute force.
#[test]
fn vs07_extensions_in_ram_or_on_nvme_give_the_same_results() {
    let dim = 24;
    let codec = DirectionCodec::new(dim, 9).unwrap();
    let mut stores = Vec::new();
    let mut ixs = vec![serving(config(dim, None), codec.clone(), None)];
    // The last limit leaves room for some extensions beside the 1-bit codes (4,536 B at the end).
    for limit in [u64::MAX, 0, 6000] {
        let (d, store) = codes("same", &codec, 3, limit);
        ixs.push(serving(config(dim, Some(store)), codec.clone(), None));
        stores.push(d);
    }
    let g = &mut Gen::from_seed(17);
    let mut model = Model::new();
    let mut at = 0;
    for round in 0..4 {
        let mut changes = Vec::new();
        for _ in 0..60 {
            let k = g.below(150);
            let change = if g.below(5) == 0 {
                model.remove(&k);
                None
            } else {
                let r = row(&vector(g, dim), vec![Value::Int(k as i64 % 5)], k);
                model.insert(k, r.clone());
                Some(r)
            };
            changes.push((k, change));
        }
        let p = page(at, changes);
        at = p.through;
        for ix in &mut ixs {
            feed(ix, RANGE, &p);
            settle(ix);
        }
        let tiers: Vec<Held> = ixs[1..].iter().map(held).collect();
        assert!(tiers[0].nvme == 0 && tiers[1].ram == 0 && tiers[1].nvme > 0, "{tiers:?}");
        assert!(round < 3 || (tiers[2].ram > 0 && tiers[2].nvme > 0), "{tiers:?}");
        for _ in 0..12 {
            let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
            let vec: Vec<f32> = (0..dim).map(|_| g.range_i64(1, 9) as f32 / 4.0).collect();
            let mut q = query(op, &vec, 1 + g.below(10) as usize);
            let want = brute(&model, &q);
            for exact in [true, false] {
                q.exact = exact;
                let mut answers = Vec::new();
                for ix in &mut ixs {
                    let pin = latest(ix, None);
                    answers.push(keys(&ix.search(&pin, &q).unwrap()));
                }
                assert!(answers.iter().all(|a| *a == answers[0]), "{q:?}: {answers:?}");
                if exact {
                    assert_eq!(answers[0], want, "{q:?}");
                }
            }
        }
    }
    for ix in &ixs[1..] {
        charges(ix).unwrap();
    }
}

/// VEC-G05 with codes: under random interleavings of feed, appends on any core, splits, merges and
/// moves between cores and retention, an exact scan that runs across those steps is brute force at
/// its pin, an approximate one returns rows with their exact scores, and the code memory is
/// charged what is held after every step.
#[test]
fn vs07_exact_with_codes_equals_brute_force_while_partitions_move_between_cores() {
    let runs = Runs::default();
    let runs = Runs {
        cases: runs.cases / 4,
        ..runs
    };
    static ACROSS: AtomicU64 = AtomicU64::new(0);
    check_with(runs, "vs07_exact_with_codes", |g| {
        let dim = 8;
        let codec = DirectionCodec::new(dim, g.below(1000)).unwrap();
        let limit = *g.pick(&[0, 2000, 8000, u64::MAX]);
        let (_d, store) = codes("moves", &codec, 3, limit);
        let mut ix = serving(config(dim, Some(store)), codec, None);
        let mut model = Model::new();
        let mut at = 0;
        for _ in 0..g.below(10) + 2 {
            let mut changes = Vec::new();
            for _ in 0..g.below(20) {
                let k = g.below(50);
                let change = match g.below(5) {
                    0 => None,
                    _ => Some(row(&vector(g, dim), vec![Value::Int(k as i64 % 5)], k)),
                };
                match change.clone() {
                    Some(r) => model.insert(k, r),
                    None => model.remove(&k),
                };
                changes.push((k, change));
            }
            let p = page(at, changes);
            at = p.through;
            feed(&mut ix, RANGE, &p);
            // A scan pinned now runs across the maintenance below.
            let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
            let vec: Vec<f32> = (0..dim).map(|_| g.range_i64(-8, 8) as f32 / 4.0 + 0.1).collect();
            let q = query(op, &vec, 1 + g.below(8) as usize);
            let want = brute(&model, &q);
            let pin = latest(&mut ix, None);
            let mut scan = ix.scan(&pin, &q, Box::new(RecallEstimate)).map_err(|e| e.to_string())?;
            scan.rotate(g.below(7) as usize);
            for _ in 0..g.below(40) {
                match g.below(4) {
                    0 => {
                        ix.maintain(2);
                    }
                    1 => {
                        ix.pump();
                    }
                    2 => {
                        let core = g.below(3) as usize;
                        let across = ix.shared.queues[core]
                            .iter()
                            .filter(|m| matches!(m, Msg::Copy(c) if c.from.0 as usize % 3 != core))
                            .count();
                        ACROSS.fetch_add(across as u64, Ordering::Relaxed);
                        ix.step(core);
                    }
                    _ => {
                        let _ = ix.visit(&mut scan).map_err(|e| e.to_string())?;
                    }
                }
                charges(&ix)?;
            }
            while ix.visit(&mut scan).map_err(|e| e.to_string())? {}
            let got = keys(&ix.finish(scan).map_err(|e| e.to_string())?);
            if got != want {
                return Err(format!("{q:?}\n got {got:?}\nwant {want:?}"));
            }
            if g.bool() {
                let _ = ix.advance_low(RANGE, g.below(at + 1));
            }
            ix.run();
            let approximate = Query {
                exact: false,
                ..q.clone()
            };
            let pin = latest(&mut ix, None);
            let got = keys(&ix.search(&pin, &approximate).map_err(|e| e.to_string())?);
            let every = brute(&model, &Query { limit: 1000, ..q });
            if got.len() != want.len() || got.iter().any(|h| !every.contains(h)) {
                return Err(format!("approximate {got:?} of {every:?}"));
            }
            charges(&ix)?;
        }
        settle(&mut ix);
        charges(&ix)?;
        ix.drop_range(RANGE).map_err(|e| e.to_string())?;
        charges(&ix)?;
        Ok(())
    });
    assert!(ACROSS.load(Ordering::Relaxed) > 0, "copies crossed cores");
}

/// A partition's entries and codes live on its owner core only. A copy between cores names the
/// generation of the index, and one that a resync made void is dropped without an entry, an
/// extension or a charge left behind.
#[test]
fn vs05_copies_between_cores_name_the_generation_and_a_resync_voids_them() {
    let dim = 8;
    let codec = DirectionCodec::new(dim, 21).unwrap();
    let (_d, store) = codes("owner", &codec, 3, 4000);
    let mut ix = serving(config(dim, Some(store.clone())), codec, None);
    let g = &mut Gen::from_seed(5);
    let rows: Vec<(u64, Vec<f32>)> = (0..60).map(|k| (k, vector(g, dim))).collect();
    feed(&mut ix, RANGE, &upserts(0, &rows));
    settle(&mut ix);
    assert!(ix.shared.dir.len() > 3, "splits made several partitions");
    for (part, meta) in &ix.shared.dir {
        assert_eq!(meta.owner, part.0 as usize % 3);
        for (core, shard) in ix.shards.iter().enumerate() {
            assert_eq!(shard.parts.contains_key(part), core == meta.owner, "{part:?} on core {core}");
        }
    }
    let generation = ix.shared.ranges[&RANGE].generation;
    // Start drains until copies wait in the queue of another core than their source's.
    let mut across = Vec::new();
    for _ in 0..100 {
        let ready: Vec<PartId> = ix.shared.dir.iter().filter(|(_, m)| m.state == State::Ready && !m.undirected).map(|(p, _)| *p).collect();
        for p in ready {
            ix.split(p);
            ix.maintain(64);
        }
        across = (0..3)
            .flat_map(|core| ix.shared.queues[core].iter().map(move |m| (core, *m)))
            .filter_map(|(core, m)| match m {
                Msg::Copy(c) if ix.shared.dir[&c.from].owner != core => Some(c),
                _ => None,
            })
            .collect();
        if !across.is_empty() {
            break;
        }
    }
    assert!(!across.is_empty(), "a copy crosses cores");
    assert!(across.iter().all(|c| c.entry.generation == generation));
    // Resync: a new generation replaces the range while the copies are on their way.
    ix.begin_load(RANGE, 500, 1).unwrap();
    let fresh: Vec<(Key, CommitTs, Row)> =
        (100..105).map(|k| (key(k), 1, row(&vector(g, dim), vec![Value::Int(0)], k))).collect();
    ix.load_rows(RANGE, &fresh).unwrap();
    ix.run();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    settle(&mut ix);
    let now = ix.shared.ranges[&RANGE].generation;
    assert_ne!(now, generation);
    let entries: Vec<u64> = ix
        .shards
        .iter()
        .flat_map(|s| s.parts.values())
        .flat_map(|p| p.entries().map(|e| e.generation))
        .collect();
    assert_eq!(entries.len(), 5, "only the snapshot's rows");
    assert!(entries.iter().all(|e| *e == now), "no entry of the old generation");
    charges(&ix).unwrap();
    let pin = latest(&mut ix, None);
    let hits = ix.search(&pin, &query(Op::L2, &[0.5; 8], 10)).unwrap();
    assert_eq!(images(&hits).len(), 5);
}

/// Clustered unit rows around 64 centres in 64 dimensions, and 1.0 to 4.0 times that length when
/// `lengths` holds.
fn clustered(g: &mut Gen, n: usize, dim: usize, lengths: bool) -> Vec<Vec<f32>> {
    let gauss = |g: &mut Gen| (0..6).map(|_| g.below(1000) as f32 / 1000.0 - 0.5).sum::<f32>();
    let centres: Vec<Vec<f32>> = (0..64).map(|_| (0..dim).map(|_| gauss(g)).collect()).collect();
    (0..n)
        .map(|_| {
            let c = g.pick(&centres).clone();
            let v: Vec<f32> = c.iter().map(|x| x + 0.35 * gauss(g)).collect();
            let len = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            let want = if lengths { 1.0 + g.below(300) as f32 / 100.0 } else { 1.0 };
            v.iter().map(|x| x / len * want).collect()
        })
        .collect()
}

/// A local measurement, not a gate: the recall of the approximate path at the default recall
/// target, with half of the extensions on NVMe, and the raw rows it rescored. Every returned score
/// is the exact one, and the codes keep most rows from being rescored. `NABEDRA_RECALL_ROWS` sets
/// the rows (5,000 by default, so that the test repeats quickly).
#[test]
fn vs19_direction_codes_measure_recall_at_the_default_target() {
    let n = std::env::var("NABEDRA_RECALL_ROWS").ok().and_then(|n| n.parse().ok());
    let (dim, n) = (64, n.unwrap_or(5_000).clamp(2_000, 1_000_000));
    let codec = DirectionCodec::new(dim, 77).unwrap();
    let g = &mut Gen::from_seed(23);
    for lengths in [false, true] {
        let all = clustered(g, n, dim, lengths);
        let probe = {
            let mut block = codec.block();
            let d = codec.rotate(&all[0]
                .iter()
                .map(|x| x / all[0].iter().map(|y| y * y).sum::<f32>().sqrt())
                .collect::<Vec<_>>());
            codec.push_detached(&mut block, &d, &d, 1.0);
            codec.held(&block)
        };
        let limit = n as u64 * probe + (n / 2 * codec.ext_bytes()) as u64;
        let (_d, store) = codes("recall", &codec, 4, limit);
        let cfg = Config {
            flat: 1000,
            codes: Some(store.clone()),
            ..Config::new(schema(dim, &[ColumnType::Int], None), 4)
        };
        let mut ix = serving(cfg, codec.clone(), None);
        let mut model = Model::new();
        let rows: Vec<(u64, Vec<f32>)> = all.into_iter().enumerate().map(|(k, v)| (k as u64, v)).collect();
        for (k, v) in &rows {
            model.insert(*k, row(v, vec![Value::Int(0)], *k));
        }
        feed(&mut ix, RANGE, &upserts(0, &rows));
        settle(&mut ix);
        charges(&ix).unwrap();
        let h = held(&ix);
        assert!(h.ram > 0 && h.nvme > 0, "{h:?}");
        let queries = clustered(&mut Gen::from_seed(99), 30, dim, false);
        let pin = latest(&mut ix, None);
        for op in [Op::Cosine, Op::Dot, Op::L2] {
            let (mut found, mut wanted, mut rescored, mut io) = (0, 0, 0, 0);
            for v in &queries {
                let q = Query {
                    exact: false,
                    ..query(op, v, 10)
                };
                let want = brute(&model, &q);
                let mut scan = ix.scan_within(&pin, &q, Box::new(RecallEstimate), Budget::UNLIMITED).unwrap();
                while ix.visit(&mut scan).unwrap() {}
                rescored += scan.seen.len();
                io += scan.usage().unwrap().io;
                let got = keys(&ix.finish(scan).unwrap());
                let every = brute(&model, &Query { limit: n, ..q.clone() });
                assert!(got.iter().all(|h| every.contains(h)), "exact scores only");
                found += want.iter().filter(|w| got.contains(w)).count();
                wanted += want.len();
            }
            let recall = found as f64 / wanted as f64;
            let per = rescored as f64 / queries.len() as f64;
            let io = io as f64 / queries.len() as f64;
            println!(
                "lengths {lengths} {op:?}: recall@10 {recall:.3} at target 0.95, {per:.0} of {n} rows rescored, {io:.0} extension bytes read from NVMe per query, {h:?}"
            );
            assert!(per < n as f64 / 4.0, "{op:?}: the codes prune: {per} rescored");
            assert!(io > 0.0, "extensions on NVMe are read and counted");
        }
    }
}

// ----- Review rv-b2b.

/// Extensions take only what the 1-bit codes leave (plan section 5, "Extension in RAM while the
/// budget lasts, otherwise NVMe"; VEC-G11). Codes that grow later shrink what is left, so
/// extensions already in RAM go to NVMe: the code memory never holds more than its limit beyond
/// what the 1-bit codes alone need.
#[test]
fn vs17_extensions_in_ram_leave_when_1_bit_codes_grow_into_the_code_memory() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 5).unwrap();
    let ext = codec.ext_bytes() as u64;
    let probe = {
        let mut block = codec.block();
        let d = codec.rotate(&vec![0.125f32; dim]);
        codec.push_detached(&mut block, &d, &d, 1.0);
        codec.held(&block)
    };
    // Room for the 1-bit codes of 100 rows and 40 extensions.
    let limit = 100 * probe + 40 * ext;
    let (_d, store) = codes("grow", &codec, 3, limit);
    // Partitions of the plan's size, so that no split encodes the rows again.
    let cfg = Config {
        part_max: 512,
        ..config(dim, Some(store.clone()))
    };
    let mut ix = serving(cfg, codec, None);
    let mut all = clustered(&mut Gen::from_seed(3), 100, dim, false).into_iter();
    let first: Vec<(u64, Vec<f32>)> = (0..60).zip(all.by_ref()).collect();
    feed(&mut ix, RANGE, &upserts(0, &first));
    settle(&mut ix);
    charges(&ix).unwrap();
    let (c, e) = store.memory().used();
    assert!(c + e <= limit, "first batch within the limit: {:?}", held(&ix));
    assert_eq!(held(&ix).ram, 60, "all 60 extensions fit in RAM");
    let more: Vec<(u64, Vec<f32>)> = (60..100).zip(all).collect();
    feed(&mut ix, RANGE, &upserts(60, &more));
    settle(&mut ix);
    charges(&ix).unwrap();
    let (c, e) = store.memory().used();
    assert!(
        e <= limit.saturating_sub(c),
        "codes {c} and extensions {e} in RAM, limit {limit}: {:?}",
        held(&ix)
    );
}

/// The code memory and the extension store outlive an index (decision L8: a worker's store lives
/// as long as its process), so an index that goes gives back what it holds. A recovery handed the
/// same configuration is then charged exactly what it holds, and stays brute force.
#[test]
fn vs17_an_index_recovered_over_the_same_extension_store_is_charged_only_what_it_holds() {
    use nabedra_rt::fs::FaultFs;
    let dim = 8;
    let codec = DirectionCodec::new(dim, 21).unwrap();
    let (_d, store) = codes("recover", &codec, 3, 4000);
    let fs = FaultFs::new();
    fs.create_dir_all_durable(std::path::Path::new("/data"));
    let ck = Store::new(Arc::new(fs.clone()), "/data/ck");
    let cfg = config(dim, Some(store.clone()));
    let mut ix = serving(cfg.clone(), codec.clone(), Some(ck.clone()));
    let g = &mut Gen::from_seed(9);
    let rows: Vec<(u64, Vec<f32>)> = (0..40).map(|k| (k, vector(g, dim))).collect();
    let mut model = Model::new();
    for (k, v) in &rows {
        model.insert(*k, row(v, vec![Value::Int(*k as i64 % 5)], *k));
    }
    feed(&mut ix, RANGE, &upserts(0, &rows));
    settle(&mut ix);
    ix.checkpoint().unwrap();
    let applied = ix.status(RANGE).unwrap().applied;
    assert!(store.counts().0 > 0, "extensions in RAM");
    drop(ix);
    let (mut back, damage) = Index::recover(cfg, codec, ck).unwrap();
    assert_eq!(damage.len(), 0);
    back.caught_up(RANGE, applied).unwrap();
    settle(&mut back);
    assert_eq!(back.status(RANGE).unwrap().damage, None);
    let pin = latest(&mut back, None);
    let q = Query {
        exact: true,
        ..query(Op::L2, &[0.5; 8], 10)
    };
    let got = keys(&back.search(&pin, &q).unwrap());
    assert_eq!(got, brute(&model, &q));
    charges(&back).unwrap();
}

// ----- Review rv-b2b: the rules its surviving mutants showed untested (task perf1).

/// A disk whose reads fail while `fail` is set.
#[derive(Debug)]
struct Faulty {
    inner: Arc<dyn Disk>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl Disk for Faulty {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }
    fn read(&self, reqs: &mut [crate::store::Req]) {
        if self.fail.load(Ordering::SeqCst) {
            for r in reqs {
                r.result = Err(std::io::Error::from_raw_os_error(5));
            }
            return;
        }
        self.inner.read(reqs);
    }
    fn write(&self, offset: u64, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, buf)
    }
}

/// An extension store on one faulty file whose code memory is `limit` bytes.
fn faulty(name: &str, codec: &DirectionCodec, limit: u64) -> (Disks, Arc<Extensions>, Arc<std::sync::atomic::AtomicBool>) {
    let d = disks(name, 1);
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let disk: Arc<dyn Disk> = Arc::new(Faulty { inner: d.disks[0].clone(), fail: fail.clone() });
    let ext = Extensions::new(codec.ext_bytes(), Arc::new(Memory::new(limit)), vec![disk]).unwrap();
    (d, Arc::new(ext), fail)
}

/// Partitions that do not split, so every entry is put once.
fn whole(dim: usize, store: Arc<Extensions>) -> Config {
    Config {
        part_max: 512,
        ..config(dim, Some(store))
    }
}

fn rows_of(g: &mut Gen, n: usize, dim: usize) -> Vec<(u64, Vec<f32>)> {
    (0..n as u64).zip(clustered(g, n, dim, true)).collect()
}

/// VEC-G10: an extension that NVMe cannot read fails the scan, and its range, as a raw vector would.
#[test]
fn vs17_an_extension_that_cannot_be_read_fails_its_range() {
    let dim = 32;
    let codec = DirectionCodec::new(dim, 7).unwrap();
    let (_d, store, fail) = faulty("readfail", &codec, 0);
    let mut ix = serving(whole(dim, store.clone()), codec, None);
    let rows = rows_of(&mut Gen::from_seed(40), 200, dim);
    feed(&mut ix, RANGE, &upserts(0, &rows));
    settle(&mut ix);
    assert_eq!(held(&ix).ram, 0, "every extension on NVMe");
    store.flush().unwrap();
    fail.store(true, Ordering::SeqCst);
    let pin = latest(&mut ix, None);
    let q = Query {
        exact: false,
        recall: 1.0,
        ..query(Op::Cosine, &rows[0].1, 10)
    };
    assert!(matches!(ix.search(&pin, &q), Err(Error::Store(_))), "the read failed");
    ix.run();
    assert!(ix.status(RANGE).unwrap().damage.is_some(), "the range fails");
}

/// VEC-G10: an extension that NVMe cannot take fails the range of its entry.
#[test]
fn vs17_an_extension_that_cannot_be_written_fails_its_range() {
    let dim = 32;
    let codec = DirectionCodec::new(dim, 7).unwrap();
    let (_d, store, fail) = faulty("putfail", &codec, 0);
    let mut ix = serving(whole(dim, store.clone()), codec, None);
    let rows = rows_of(&mut Gen::from_seed(41), 40, dim);
    feed(&mut ix, RANGE, &upserts(0, &rows));
    settle(&mut ix);
    assert_eq!(ix.status(RANGE).unwrap().damage, None);
    // The last page is on disk and partly full: the next record reads it first, and that fails.
    store.flush().unwrap();
    fail.store(true, Ordering::SeqCst);
    let more = rows_of(&mut Gen::from_seed(42), 1, dim);
    let one: Vec<(u64, Vec<f32>)> = more.into_iter().map(|(_, v)| (1000, v)).collect();
    let _ = ix.apply(RANGE, &upserts(40, &one));
    ix.run();
    assert!(ix.status(RANGE).unwrap().damage.is_some(), "the range fails");
}

/// VEC-G11: reading an extension from NVMe costs its bytes of I/O, and one in RAM none.
#[test]
fn vs19_extension_reads_are_charged_by_their_tier() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 8).unwrap();
    let rows = rows_of(&mut Gen::from_seed(43), 300, dim);
    let queries = clustered(&mut Gen::from_seed(44), 20, dim, true);
    for (name, limit) in [("ram", u64::MAX / 4), ("nvme", 0)] {
        let (_d, store) = codes(name, &codec, 3, limit);
        let mut ix = serving(whole(dim, store.clone()), codec.clone(), None);
        feed(&mut ix, RANGE, &upserts(0, &rows));
        settle(&mut ix);
        let h = held(&ix);
        assert_eq!(if limit == 0 { h.ram } else { h.nvme }, 0, "{name}: {h:?}");
        let pin = latest(&mut ix, None);
        let mut io = 0;
        for v in &queries {
            let q = Query {
                exact: false,
                ..query(Op::Dot, v, 10)
            };
            let mut scan = ix.scan_within(&pin, &q, Box::new(RecallEstimate), Budget::UNLIMITED).unwrap();
            while ix.visit(&mut scan).unwrap() {}
            io += scan.usage().unwrap().io;
        }
        if limit == 0 {
            assert!(io > 0 && io % codec.ext_bytes() as u64 == 0, "{name}: {io}");
        } else {
            assert_eq!(io, 0, "{name}: extensions in RAM cost no I/O");
        }
    }
}

/// The code memory is charged what the codes take: per entry, 64 dimensions of 1 bit, the squared
/// residual, the 1-bit scale and 12 bytes of factors, and the extension when it is in RAM. Counted
/// here by hand, not by the blocks' own `held`.
#[test]
fn vs17_code_charges_are_the_sizes_of_the_codes() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 9).unwrap();
    let (_d, store) = codes("sizes", &codec, 3, u64::MAX / 4);
    let mut ix = serving(whole(dim, store.clone()), codec, None);
    let n = 150;
    feed(&mut ix, RANGE, &upserts(0, &rows_of(&mut Gen::from_seed(45), n, dim)));
    settle(&mut ix);
    assert_eq!(ix.stats().entries, n);
    assert_eq!(store.memory().used(), ((n * (8 + 4 + 4 + 12)) as u64, (n * 32) as u64));
}

/// Maintenance brings at most `copies` extensions of a partition back to RAM per call, and the
/// rest in later calls.
#[test]
fn vs17_promotion_brings_back_at_most_copies_per_partition_and_call() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 10).unwrap();
    let (_d, store) = codes("promote", &codec, 3, 1 << 20);
    // Another user of the code memory leaves no room while the rows arrive.
    store.memory().charge_codes(1 << 20);
    let cfg = Config {
        copies: 7,
        ..whole(dim, store.clone())
    };
    let mut ix = serving(cfg, codec, None);
    feed(&mut ix, RANGE, &upserts(0, &rows_of(&mut Gen::from_seed(46), 100, dim)));
    settle(&mut ix);
    assert_eq!(held(&ix).ram, 0, "{:?}", held(&ix));
    store.memory().release_codes(1 << 20);
    let parts = ix.listed().len();
    ix.maintain(parts);
    let ram = held(&ix).ram;
    assert!(ram > 0 && ram <= 7 * parts, "{ram} promoted over {parts} partitions");
    settle(&mut ix);
    assert_eq!(held(&ix).nvme, 0, "the rest came back");
    charges(&ix).unwrap();
}

/// An index refuses an extension store whose extensions have another size than its codes.
#[test]
fn vs17_an_index_refuses_an_extension_store_of_another_size() {
    let codec = DirectionCodec::new(64, 11).unwrap();
    let d = disks("size", 1);
    let other = Extensions::new(codec.ext_bytes() + 1, Arc::new(Memory::new(1 << 20)), d.disks.clone()).unwrap();
    assert!(Index::new(config(64, Some(Arc::new(other))), codec.clone(), None).is_err());
    let (_d, same) = codes("size-ok", &codec, 1, 1 << 20);
    assert!(Index::new(config(64, Some(same)), codec, None).is_ok());
}

/// VEC-BUG-ext-ram-beyond-code-memory within one batch: rows that arrive together still leave
/// extensions only what the codes of all of them leave.
#[test]
fn vs17_extensions_of_one_batch_leave_room_for_the_codes_of_the_batch() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 5).unwrap();
    let ext = codec.ext_bytes() as u64;
    let per = (8 + 4 + 4 + 12) as u64;
    let limit = 100 * per + 40 * ext;
    let (_d, store) = codes("batch", &codec, 3, limit);
    let mut ix = serving(whole(dim, store.clone()), codec, None);
    feed(&mut ix, RANGE, &upserts(0, &rows_of(&mut Gen::from_seed(47), 100, dim)));
    let (c, e) = store.memory().used();
    assert!(c + e <= limit, "codes {c} and extensions {e}, limit {limit}");
    settle(&mut ix);
    charges(&ix).unwrap();
    assert_eq!(held(&ix).ram, 40, "{:?}", held(&ix));
}

/// VEC-BUG-ext-charges-outlive-index without a checkpoint: a dropped index gives back every byte
/// and record it held in the shared store.
#[test]
fn vs17_a_dropped_index_gives_back_its_codes_and_extensions() {
    let dim = 64;
    let codec = DirectionCodec::new(dim, 12).unwrap();
    let (_d, store) = codes("drop", &codec, 3, 100 * 28 + 30 * 32);
    let mut ix = serving(config(dim, Some(store.clone())), codec, None);
    feed(&mut ix, RANGE, &upserts(0, &rows_of(&mut Gen::from_seed(48), 100, dim)));
    settle(&mut ix);
    let h = held(&ix);
    assert!(h.ram > 0 && h.nvme > 0, "{h:?}");
    drop(ix);
    assert_eq!(store.memory().used(), (0, 0));
    assert_eq!(store.counts(), (0, 0));
}

#[test]
fn vs17_two_live_indexes_cannot_corrupt_one_anothers_extensions() {
    let codec = DirectionCodec::new(8, 5).unwrap();
    let (_disks, store) = codes("shared-owner", &codec, 3, 4096);
    let cfg = whole(8, store.clone());
    let mut first = serving(cfg.clone(), codec.clone(), None);
    let page = upserts(0, &[(1, vec![1.0; 8])]);
    feed(&mut first, RANGE, &page);
    charges(&first).unwrap();

    assert!(matches!(
        Index::new(cfg.clone(), codec.clone(), None),
        Err(Error::StoreOwned),
    ));
    let checkpoints = Store::new(Arc::new(nabedra_rt::fs::FaultFs::new()), "/ck");
    assert!(matches!(
        Index::recover(cfg.clone(), codec.clone(), checkpoints),
        Err(Error::StoreOwned),
    ));
    charges(&first).expect("a refused index preserves the owner's charge and extension");

    drop(first);
    let mut second = serving(cfg, codec, None);
    feed(&mut second, RANGE, &page);
    charges(&second).expect("the store can be reused after the owner releases its extensions");
}
