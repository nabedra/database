//! Store microbench (task NS5, indicative only; NS11 owns the real benchmark).
//!
//! `cargo test --release -p nabedra-kv --test store_bench -- --ignored --nocapture`
//! on `RealFs` in a temp dir: point gets, short scans, synced and unsynced puts with
//! 1, 8 and 64 writers, and flush and compaction throughput.
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

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

// ---------------------------------------------------------------------------------
// NS18 (task NS11): the native microbench. Indicative engine numbers with no RocksDB
// counterpart: put latency and throughput by value size, durability and writer count,
// journal syncs per synced commit (group commit ratio), Raft-style journal appends and
// purges, hot and cold gets, scans of 10 and 128, a 256 MiB install, write
// amplification, stall time, recovery time and RSS.
//
// `cargo test --release -p nabedra-kv --test store_bench -- --ignored --nocapture ns18`

fn percentiles(mut micros: Vec<u64>) -> String {
    micros.sort_unstable();
    let at = |p: usize| micros[(micros.len() * p).div_ceil(100).max(1) - 1];
    format!(
        "p50 {} µs, p95 {} µs, p99 {} µs, max {} µs",
        at(50),
        at(95),
        at(99),
        micros[micros.len() - 1]
    )
}

/// `VmRSS` and `VmHWM` in KiB (Linux only).
fn rss() -> String {
    std::fs::read_to_string("/proc/self/status")
        .map(|status| {
            status
                .lines()
                .filter(|line| line.starts_with("VmRSS") || line.starts_with("VmHWM"))
                .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|_| "unavailable".into())
}

/// One timed batch per call, from `writers` threads; returns per-commit latencies.
fn timed_writes(
    store: &Arc<Store>,
    writers: u64,
    per_writer: u64,
    make: impl Fn(u64, u64) -> WriteBatch + Send + Sync + 'static,
    durability: Durability,
) -> (Vec<u64>, Duration) {
    let make = Arc::new(make);
    let start = Instant::now();
    let threads: Vec<_> = (0..writers)
        .map(|w| {
            let (store, make) = (store.clone(), make.clone());
            std::thread::spawn(move || {
                let mut out = Vec::with_capacity(per_writer as usize);
                for i in 0..per_writer {
                    let batch = make(w, i);
                    let t = Instant::now();
                    store
                        .submit(batch, durability, Box::new(()))
                        .unwrap()
                        .wait()
                        .unwrap();
                    out.push(t.elapsed().as_micros() as u64);
                }
                out
            })
        })
        .collect();
    let mut all = Vec::new();
    for t in threads {
        all.extend(t.join().unwrap());
    }
    (all, start.elapsed())
}

fn report_writes(
    store: &Arc<Store>,
    label: &str,
    writers: u64,
    per_writer: u64,
    make: impl Fn(u64, u64) -> WriteBatch + Send + Sync + 'static,
    durability: Durability,
) {
    let before = store.stats();
    let (lat, elapsed) = timed_writes(store, writers, per_writer, make, durability);
    let after = store.stats();
    let commits = (after.commits - before.commits).max(1);
    eprintln!(
        "ns18 {label} writers={writers:>2}: {}; {}; journal syncs/commit {:.3}; group commits {}; stall_ms +{}",
        rate(writers * per_writer, elapsed),
        percentiles(lat),
        (after.journal_syncs - before.journal_syncs) as f64 / commits as f64,
        after.group_commits - before.group_commits,
        after.stall_ms - before.stall_ms,
    );
}

#[test]
#[ignore = "bench; run in release"]
fn ns18_bench_native_microbench() {
    let tmp = TempDir::new("ns18");
    let bg = Background::new(2).unwrap();
    let opened = Store::open(tmp.path(), options(&bg)).unwrap();
    let mut store = Arc::new(opened.store);
    let next = Arc::new(AtomicU64::new(0));

    // Puts: 100 B and 1 KiB, unsynced and synced, 1 to 64 writers.
    for size in [100usize, 1024] {
        for (durability, name) in [
            (Durability::NoSync, "unsynced"),
            (Durability::Sync, "synced"),
        ] {
            for writers in [1u64, 4, 16, 64] {
                let total = match (durability, writers) {
                    (Durability::NoSync, _) => 20_000,
                    (Durability::Sync, 1) => 300,
                    (Durability::Sync, _) => 3_200,
                };
                let next = next.clone();
                report_writes(
                    &store,
                    &format!("put {name} {size} B"),
                    writers,
                    total / writers,
                    move |w, i| {
                        let g = (w * 7 + i) % GROUPS;
                        let n = next.fetch_add(1, Ordering::Relaxed);
                        let mut b = WriteBatch::new();
                        b.set_scope(&key(g, 0)[..17]);
                        b.put(&key(g, 1_000_000 + n), &vec![b'p'; size]);
                        b
                    },
                    durability,
                );
            }
        }
    }

    // Raft appends: 64 entries of 1 KiB as journal values in one synced batch, one log per writer.
    let log_key = |g: u64, index: u64| {
        let mut k = key(g, 0)[..17].to_vec();
        k.push(0x01);
        k.extend_from_slice(&index.to_be_bytes());
        k
    };
    for writers in [1u64, 8] {
        let appends = 200 / writers.min(2);
        report_writes(
            &store,
            "raft append 64 x 1 KiB synced",
            writers,
            appends,
            move |w, i| {
                let mut b = WriteBatch::new();
                b.set_scope(&key(w, 0)[..17]);
                for e in 0..64 {
                    b.put_journal(&log_key(w, i * 64 + e), &[b'r'; 1024]);
                }
                b
            },
            Durability::Sync,
        );
        // Purge each log behind the appends in 50 steps, as Raft does after a snapshot.
        report_writes(
            &store,
            "raft purge (delete_range) unsynced",
            writers,
            50,
            move |w, i| {
                let per = appends * 64 / 50;
                let mut b = WriteBatch::new();
                b.set_scope(&key(w, 0)[..17]);
                b.delete_range(&log_key(w, i * per), &log_key(w, (i + 1) * per));
                b
            },
            Durability::NoSync,
        );
    }

    // Dataset for reads: 64 groups x 4096 rows of 100 B, flushed and compacted.
    let rows = 4096u64;
    for g in 0..GROUPS {
        for chunk in 0..rows / 64 {
            let mut b = WriteBatch::new();
            b.set_scope(&key(g, 0)[..17]);
            for k in chunk * 64..chunk * 64 + 64 {
                b.put(&key(g, k), &[b'v'; 100]);
            }
            store
                .submit(b, Durability::NoSync, Box::new(()))
                .unwrap()
                .wait()
                .unwrap();
        }
    }
    store.flush().unwrap();
    store.compact().unwrap();
    let reads = |store: &Arc<Store>, label: &str| {
        let mut rng = Rng(7);
        let snap = store.snapshot();
        let n = 100_000u64;
        let mut lat = Vec::with_capacity(n as usize);
        let start = Instant::now();
        for _ in 0..n {
            let k = key(rng.below(GROUPS), rng.below(rows));
            let t = Instant::now();
            assert!(snap.get(&k).unwrap().is_some());
            lat.push(t.elapsed().as_nanos() as u64 / 1000);
        }
        eprintln!(
            "ns18 get {label}: {}; {}",
            rate(n, start.elapsed()),
            percentiles(lat)
        );
        for len in [10u64, 128] {
            let scans = 20_000u64;
            let mut lat = Vec::with_capacity(scans as usize);
            let start = Instant::now();
            for _ in 0..scans {
                let g = rng.below(GROUPS);
                let (lo, hi) = (key(g, 0), key(g + 1, 0));
                let t = Instant::now();
                let mut it =
                    snap.iter(Bound::Included(&lo[..17]), Bound::Excluded(&hi[..17]), true);
                it.seek(&key(g, rng.below(rows - len)));
                for _ in 0..len {
                    assert!(it.valid());
                    let _ = it.value().unwrap();
                    it.next();
                }
                lat.push(t.elapsed().as_micros() as u64);
            }
            eprintln!(
                "ns18 scan {len} {label}: {}; {}",
                rate(scans, start.elapsed()),
                percentiles(lat)
            );
        }
    };
    reads(&store, "hot (256 MiB cache, warmed)");
    reads(&store, "hot (256 MiB cache, second pass)");

    // A 256 MiB install: 1 KiB values in 256 KiB unsynced batches into a fresh scope, then a sync.
    let before = store.stats();
    let start = Instant::now();
    let install_group = GROUPS + 1;
    for chunk in 0..1024u64 {
        let mut b = WriteBatch::new();
        b.set_scope(&key(install_group, 0)[..17]);
        for k in chunk * 256..chunk * 256 + 256 {
            b.put(&key(install_group, k), &[b'i'; 1024]);
        }
        store
            .submit(b, Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    store.sync().unwrap().wait().unwrap();
    let elapsed = start.elapsed();
    let after = store.stats();
    eprintln!(
        "ns18 install 256 MiB: {:?} ({:.0} MiB/s), stall_ms +{}, flushes +{:.1} MiB",
        elapsed,
        256.0 / elapsed.as_secs_f64(),
        after.stall_ms - before.stall_ms,
        (after.flush_bytes - before.flush_bytes) as f64 / (1 << 20) as f64
    );

    // Recovery: unflushed writes in the journal, then reopen.
    for i in 0..20_000u64 {
        let mut b = WriteBatch::new();
        b.set_scope(&key(i % GROUPS, 0)[..17]);
        b.put(&key(i % GROUPS, 5_000_000 + i), &[b'u'; 100]);
        store
            .submit(b, Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    store.sync().unwrap().wait().unwrap();
    let stats = store.stats();
    let user = stats.user_bytes.max(1) as f64;
    eprintln!(
        "ns18 write amplification: (journal {} + flush {} + compaction {} + relocated {}) / user {} = {:.2}; \
         journal-only {:.2}; stall_ms {}; journal syncs {}, file syncs {}, commits {}, synced commits {}",
        stats.journal_bytes,
        stats.flush_bytes,
        stats.compaction_bytes,
        stats.relocated_bytes,
        stats.user_bytes,
        (stats.journal_bytes + stats.flush_bytes + stats.compaction_bytes + stats.relocated_bytes)
            as f64
            / user,
        stats.journal_bytes as f64 / user,
        stats.stall_ms,
        stats.journal_syncs,
        stats.file_syncs,
        stats.commits,
        stats.synced_commits,
    );
    eprintln!("ns18 rss before reopen: {}", rss());
    assert_eq!(Arc::strong_count(&store), 1, "one store handle");
    drop(store);
    let start = Instant::now();
    let opened = Store::open(tmp.path(), options(&bg)).unwrap();
    let reopen = start.elapsed();
    store = Arc::new(opened.store);
    eprintln!(
        "ns18 recovery: reopen {:?} (stats.recovery_ms {}), memtable after replay {:.1} MiB",
        reopen,
        store.stats().recovery_ms,
        store.stats().memtable_bytes as f64 / (1 << 20) as f64
    );
    // Cold reads: a fresh store with a 1 MiB block cache.
    assert_eq!(Arc::strong_count(&store), 1, "one store handle");
    drop(store);
    let mut cold = options(&bg);
    cold.cache = Arc::new(BlockCache::new(1 << 20));
    let store = Arc::new(Store::open(tmp.path(), cold).unwrap().store);
    reads(&store, "cold (1 MiB cache)");
    eprintln!("ns18 stats: {:?}", store.stats());
    eprintln!("ns18 rss end: {}", rss());
}
