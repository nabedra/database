//! Store microbench (task NS5, indicative only; NS11 owns the real benchmark).
//!
//! `cargo test --release -p nabedra-kv --test store_bench -- --ignored --nocapture`
//! on `RealFs` in a temp dir: point gets, short scans, synced and unsynced puts with
//! 1, 8 and 64 writers, and flush and compaction throughput.

mod common;

use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::*;
use nabedra_kv::fs::RealFs;
use nabedra_kv::{Background, BlockCache, Durability, MemtableBudget, Options, Store, WriteBatch};

const GROUPS: u64 = 64;

fn key(group: u64, k: u64) -> Vec<u8> {
    // 17-byte scope (0x40 ‖ 16-byte group) like a pool, then an 8-byte row key.
    let mut out = vec![0x40];
    out.extend_from_slice(&u128::from(group).to_be_bytes());
    out.extend_from_slice(&k.to_be_bytes());
    out
}

fn pool_scope(k: &[u8]) -> usize {
    if k.len() >= 17 && k[0] == 0x40 { 17 } else { 0 }
}

fn options(bg: &Arc<Background>) -> Options {
    let mut o = Options::new(
        Arc::new(RealFs),
        Arc::new(BlockCache::new(256 << 20)),
        Arc::new(MemtableBudget::new(64 << 20)),
        bg.clone(),
    );
    o.scope_of = pool_scope;
    o
}

fn rate(n: u64, d: Duration) -> String {
    format!(
        "{:.0} ops/s ({:.2} µs/op)",
        n as f64 / d.as_secs_f64(),
        d.as_secs_f64() * 1e6 / n as f64
    )
}

fn puts(store: &Arc<Store>, writers: u64, per_writer: u64, durability: Durability, label: &str) {
    let before = store.stats();
    let next = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let threads: Vec<_> = (0..writers)
        .map(|w| {
            let (store, next) = (store.clone(), next.clone());
            std::thread::spawn(move || {
                let value = [b'p'; 100];
                for i in 0..per_writer {
                    let n = next.fetch_add(1, Ordering::Relaxed);
                    let g = (w * 7 + i) % GROUPS;
                    let mut b = WriteBatch::new();
                    b.set_scope(&key(g, 0)[..17]);
                    b.put(&key(g, 1_000_000 + n), &value);
                    store
                        .submit(b, durability, Box::new(()))
                        .unwrap()
                        .wait()
                        .unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = start.elapsed();
    let after = store.stats();
    let commits = after.commits - before.commits;
    let syncs = after.journal_syncs - before.journal_syncs;
    eprintln!(
        "{label:>22}: {writers:>2} writers: {}, journal syncs per commit {:.3}",
        rate(writers * per_writer, elapsed),
        syncs as f64 / commits.max(1) as f64
    );
}

#[test]
#[ignore = "bench; run in release"]
fn ns5_bench_store_reads_writes_flush_compaction() {
    let tmp = TempDir::new("bench");
    let bg = Background::new(4).unwrap();
    let store = Arc::new(Store::open(tmp.path(), options(&bg)).unwrap().store);

    // Load: 64 groups × 4096 rows of 100 B, unsynced batches of 16.
    let rows = 4096u64;
    let value = [b'v'; 100];
    let start = Instant::now();
    for g in 0..GROUPS {
        for chunk in 0..rows / 16 {
            let mut b = WriteBatch::new();
            b.set_scope(&key(g, 0)[..17]);
            for k in chunk * 16..chunk * 16 + 16 {
                b.put(&key(g, k), &value);
            }
            store
                .submit(b, Durability::NoSync, Box::new(()))
                .unwrap()
                .wait()
                .unwrap();
        }
    }
    let load = start.elapsed();
    eprintln!(
        "load: {} rows in {:?} ({})",
        GROUPS * rows,
        load,
        rate(GROUPS * rows, load)
    );

    let mem = store.stats().memtable_bytes;
    let start = Instant::now();
    store.flush().unwrap();
    let flush = start.elapsed();
    let flushed = store.stats().flush_bytes;
    eprintln!(
        "flush: {:.1} MiB of memtable -> {:.1} MiB of runs in {:?} ({:.0} MiB/s written)",
        mem as f64 / (1 << 20) as f64,
        flushed as f64 / (1 << 20) as f64,
        flush,
        flushed as f64 / (1 << 20) as f64 / flush.as_secs_f64()
    );
    // Rewrite everything three times so L0 reaches its trigger with overlapping runs.
    for _ in 0..3 {
        for g in 0..GROUPS {
            for chunk in 0..rows / 16 {
                let mut b = WriteBatch::new();
                b.set_scope(&key(g, 0)[..17]);
                for k in chunk * 16..chunk * 16 + 16 {
                    b.put(&key(g, k), &value);
                }
                store
                    .submit(b, Durability::NoSync, Box::new(()))
                    .unwrap()
                    .wait()
                    .unwrap();
            }
        }
        store.flush().unwrap();
    }
    store.flush().unwrap();
    let before = store.stats().compaction_bytes;
    let start = Instant::now();
    store.compact().unwrap();
    let compact = start.elapsed();
    let written = store.stats().compaction_bytes - before;
    eprintln!(
        "compaction: {:.1} MiB written in {:?} ({:.0} MiB/s), levels {:?}",
        written as f64 / (1 << 20) as f64,
        compact,
        written as f64 / (1 << 20) as f64 / compact.as_secs_f64().max(1e-9),
        store.level_runs()
    );

    let mut rng = Rng(1);
    let n = 200_000u64;
    let snap = store.snapshot();
    let start = Instant::now();
    for _ in 0..n {
        let k = key(rng.below(GROUPS), rng.below(rows));
        assert!(snap.get(&k).unwrap().is_some());
    }
    eprintln!("point get (runs, cache hot): {}", rate(n, start.elapsed()));
    let start = Instant::now();
    for _ in 0..n {
        let k = key(rng.below(GROUPS), rows + rng.below(rows));
        assert!(snap.get(&k).unwrap().is_none());
    }
    eprintln!("point get, absent key: {}", rate(n, start.elapsed()));
    let start = Instant::now();
    let scans = 50_000u64;
    for _ in 0..scans {
        let g = rng.below(GROUPS);
        let lo = key(g, 0);
        let hi = key(g + 1, 0);
        let mut it = snap.iter(Bound::Included(&lo[..17]), Bound::Excluded(&hi[..17]), true);
        it.seek(&key(g, rng.below(rows - 10)));
        for _ in 0..10 {
            assert!(it.valid());
            let _ = it.value().unwrap();
            it.next();
        }
    }
    eprintln!(
        "short scan (iter + seek + 10): {}",
        rate(scans, start.elapsed())
    );
    drop(snap);

    for writers in [1u64, 8, 64] {
        puts(
            &store,
            writers,
            20_000 / writers,
            Durability::NoSync,
            "unsynced put 100 B",
        );
    }
    for writers in [1u64, 8, 64] {
        let per = if writers == 1 { 200 } else { 1600 / writers };
        puts(&store, writers, per, Durability::Sync, "synced put 100 B");
    }
    let stats = store.stats();
    eprintln!("stats: {stats:?}");
}
