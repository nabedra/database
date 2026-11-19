//! NS01 at store level: a differential test against a `BTreeMap` oracle with flushes,
//! compactions and reopens forced, snapshots held across them, point reads, and iterator
//! walks (forward, reverse, bounds, `seek_for_prev`, direction switches).

mod common;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::FaultFs;
use nabedra_kv::{Background, Durability, Snapshot, Store};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const GROUPS: u64 = 5;
const KEYS: u64 = 80;

fn random_key(rng: &mut Rng) -> Vec<u8> {
    match rng.below(10) {
        0 => format!("s/k{:03}", rng.below(KEYS)).into_bytes(),
        1 => group_prefix(rng.below(GROUPS)),
        _ => group_key(rng.below(GROUPS), rng.below(KEYS + 10)),
    }
}

fn random_bound(rng: &mut Rng) -> Bound<Vec<u8>> {
    match rng.below(4) {
        0 => Bound::Unbounded,
        1 => Bound::Excluded(random_key(rng)),
        _ => Bound::Included(random_key(rng)),
    }
}

fn in_bounds(k: &[u8], lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
    let above = match lo {
        Bound::Included(l) => k >= l.as_slice(),
        Bound::Excluded(l) => k > l.as_slice(),
        Bound::Unbounded => true,
    };
    let below = match hi {
        Bound::Included(h) => k <= h.as_slice(),
        Bound::Excluded(h) => k < h.as_slice(),
        Bound::Unbounded => true,
    };
    above && below
}

fn as_ref(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn check_gets(rng: &mut Rng, snap: &Snapshot, model: &Model, ctx: &str) {
    for _ in 0..8 {
        let key = random_key(rng);
        let got = snap.get(&key).unwrap();
        assert_eq!(
            got.as_deref(),
            model.get(&key).map(Vec::as_slice),
            "{ctx} get {:?}",
            String::from_utf8_lossy(&key)
        );
    }
}

/// A random walk compared step by step with the oracle restricted to the bounds.
fn check_walk(rng: &mut Rng, snap: &Snapshot, model: &Model, ctx: &str) {
    let lo = random_bound(rng);
    let hi = random_bound(rng);
    let expected: Vec<(&Vec<u8>, &Vec<u8>)> = model
        .iter()
        .filter(|(k, _)| in_bounds(k, &lo, &hi))
        .collect();
    let mut it = snap.iter(as_ref(&lo), as_ref(&hi), rng.chance(50));
    // Position in `expected`, or None when invalid.
    let mut pos: Option<usize>;
    let mut trace = Vec::new();
    match rng.below(4) {
        0 => {
            it.seek_to_first();
            pos = (!expected.is_empty()).then_some(0);
            trace.push("first".to_string());
        }
        1 => {
            it.seek_to_last();
            pos = expected.len().checked_sub(1);
            trace.push("last".to_string());
        }
        2 => {
            let t = random_key(rng);
            it.seek(&t);
            pos = expected
                .iter()
                .position(|(k, _)| k.as_slice() >= t.as_slice());
            trace.push(format!("seek {}", String::from_utf8_lossy(&t)));
        }
        _ => {
            let t = random_key(rng);
            it.seek_for_prev(&t);
            pos = expected
                .iter()
                .rposition(|(k, _)| k.as_slice() <= t.as_slice());
            trace.push(format!("seek_for_prev {}", String::from_utf8_lossy(&t)));
        }
    }
    for _ in 0..30 {
        it.status().unwrap();
        match pos {
            None => {
                assert!(
                    !it.valid(),
                    "{ctx} {trace:?}: iterator valid, oracle exhausted"
                );
                return;
            }
            Some(p) => {
                assert!(
                    it.valid(),
                    "{ctx} {trace:?}: iterator invalid, oracle at {p}"
                );
                assert_eq!(it.key(), expected[p].0.as_slice(), "{ctx} {trace:?}");
                assert_eq!(
                    it.value().unwrap().as_ref(),
                    expected[p].1.as_slice(),
                    "{ctx} {trace:?}"
                );
                if rng.chance(60) {
                    it.next();
                    trace.push("next".into());
                    pos = (p + 1 < expected.len()).then_some(p + 1);
                } else {
                    it.prev();
                    trace.push("prev".into());
                    pos = p.checked_sub(1);
                }
            }
        }
    }
}

fn run_model(seed: u64, ops: u64) {
    let (fs, dir) = fault_fs();
    let bg = Background::new(3);
    let options = |fs: &FaultFs| small_options(Arc::new(fs.clone()), bg.clone());
    let mut store: Option<Store> = Some(open(&dir, options(&fs)));
    let mut rng = Rng(seed);
    let mut model = Model::new();
    let mut held: Vec<(Snapshot, Model)> = Vec::new();
    let mut counts = [0u64; 6];
    for op in 0..ops {
        let s = store.as_ref().unwrap();
        let ctx = format!("seed {seed} op {op}");
        match rng.below(100) {
            0..=69 => {
                let b = random_batch(&mut rng, GROUPS, KEYS, op);
                b.apply(&mut model);
                let d = if rng.chance(20) {
                    Durability::Sync
                } else {
                    Durability::NoSync
                };
                write(s, &b, d).unwrap();
                counts[0] += 1;
            }
            70..=74 => {
                if held.len() < 4 {
                    held.push((s.snapshot(), model.clone()));
                }
            }
            75..=77 => {
                if !held.is_empty() {
                    held.remove(rng.below(held.len() as u64) as usize);
                }
            }
            78..=89 => {
                check_gets(&mut rng, &s.snapshot(), &model, &ctx);
                if let Some((snap, m)) = held.get(rng.below(held.len().max(1) as u64) as usize) {
                    check_gets(&mut rng, snap, m, &format!("{ctx} held"));
                }
                counts[1] += 1;
            }
            _ => {
                check_walk(&mut rng, &s.snapshot(), &model, &ctx);
                if let Some((snap, m)) = held.get(rng.below(held.len().max(1) as u64) as usize) {
                    check_walk(&mut rng, snap, m, &format!("{ctx} held"));
                }
                counts[2] += 1;
            }
        }
        if op % 1500 == 1499 {
            store.as_ref().unwrap().flush().unwrap();
            counts[3] += 1;
        }
        if op % 4000 == 3999 {
            store.as_ref().unwrap().compact().unwrap();
            counts[4] += 1;
        }
        if op % 20000 == 19999 {
            // Reopen: snapshots of the old instance stay readable until dropped.
            for (snap, m) in &held {
                assert_eq!(&dump(snap), m, "seed {seed} op {op} held before reopen");
            }
            held.clear();
            store.take().unwrap().close().unwrap();
            store = Some(open(&dir, options(&fs)));
            assert_eq!(
                dump(&store.as_ref().unwrap().snapshot()),
                model,
                "seed {seed} reopen"
            );
            counts[5] += 1;
        }
    }
    let s = store.as_ref().unwrap();
    assert_eq!(dump(&s.snapshot()), model, "seed {seed} final");
    for (snap, m) in &held {
        assert_eq!(&dump(snap), m, "seed {seed} final held");
    }
    let stats = s.stats();
    eprintln!(
        "seed {seed}: {} writes, {} get rounds, {} walks, {} flushes, {} compactions, {} reopens; levels {:?}; flush {} B, compaction {} B",
        counts[0],
        counts[1],
        counts[2],
        counts[3],
        counts[4],
        counts[5],
        s.level_runs(),
        stats.flush_bytes,
        stats.compaction_bytes
    );
    assert!(stats.compaction_bytes > 0 && stats.flush_bytes > 0);
}

#[test]
fn ns01_store_matches_btreemap_oracle_1e5() {
    for seed in [1, 2] {
        run_model(seed, 50_000);
    }
}

#[test]
#[ignore = "10^6 operations; run in release"]
fn ns01_store_matches_btreemap_oracle_1e6() {
    run_model(42, 1_000_000);
}
