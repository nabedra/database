//! Store-level tests for the NS5b follow-ups: NS09 (EIO latches), write pacing under L0
//! slowdown, immediate persistence of run-time quarantine, memory accounting, a journal
//! behind the manifest, and damage found by `Run::open`.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use nabedra_kv::fs::{Fault, FaultFs, Fs, FsOp, OpenMode};
use nabedra_kv::run::FOOTER_LEN;
use nabedra_kv::{Background, DamageReason, Durability, Error, Options, Store, WriteBatch};

fn options(fs: &FaultFs, bg: &Arc<Background>) -> Options {
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.memtable_bytes = 1 << 20;
    o.max_replay_bytes = 1 << 30;
    o
}

fn put_scoped(store: &Store, key: &[u8], value: &[u8], d: Durability) -> nabedra_kv::Result<u64> {
    let mut b = WriteBatch::new();
    b.set_scope(&key[..4.min(key.len())]);
    b.put(key, value);
    store.submit(b, d, Box::new(()))?.wait()
}

fn files(fs: &FaultFs, dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs
        .list(dir)
        .unwrap()
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == ext))
        .collect();
    v.sort();
    v
}

fn find(fs: &FaultFs, path: &Path, needle: &[u8]) -> u64 {
    let data = fs.contents(path).unwrap();
    data.windows(needle.len())
        .position(|w| w == needle)
        .unwrap_or_else(|| panic!("{:?} not in {path:?}", String::from_utf8_lossy(needle)))
        as u64
}

/// NS09: EIO on the journal's fdatasync latches `RecoveryRequired`. The synced write and
/// every commit pending with it fail ambiguous, nothing retries the sync, later submits
/// fail, and reopening recovers every acknowledged write from disk.
#[test]
fn ns09_eio_on_journal_sync_latches_and_recovers_from_disk() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, options(&fs, &bg));
    let mut acked = BTreeMap::new();
    for i in 0..20u32 {
        let key = format!("g01/k{i:03}").into_bytes();
        put_scoped(&store, &key, b"before", Durability::Sync).unwrap();
        acked.insert(key, b"before".to_vec());
    }
    fs.inject(Fault::on(FsOp::SyncData).path_prefix(dir.join("journal")));
    fs.clear_calls();
    // A burst of unsynced writes and one synced write behind them.
    let mut pending = Vec::new();
    for i in 0..10u32 {
        let mut b = WriteBatch::new();
        b.put(format!("g02/k{i:03}").as_bytes(), b"pending");
        pending.push(store.submit(b, Durability::NoSync, Box::new(())).unwrap());
    }
    let mut b = WriteBatch::new();
    b.put(b"g03/sync", b"synced?");
    let synced = store.submit(b, Durability::Sync, Box::new(())).unwrap();
    let result = synced.wait();
    assert!(
        result.as_ref().is_err_and(Error::is_ambiguous),
        "synced write after a failed fdatasync: {result:?}"
    );
    for c in pending {
        if let Err(e) = c.wait() {
            assert!(e.is_ambiguous(), "{e:?}");
        }
    }
    assert!(store.requires_recovery());
    assert!(matches!(
        put_scoped(&store, b"g04/after", b"x", Durability::NoSync),
        Err(Error::RecoveryRequired)
    ));
    assert!(matches!(store.sync(), Err(Error::RecoveryRequired)));
    // fsyncgate: the failed sync is never retried.
    let syncs = fs
        .calls()
        .iter()
        .filter(|c| c.op == FsOp::SyncData && c.path.starts_with(dir.join("journal")))
        .count();
    assert_eq!(syncs, 1, "journal syncs after the injected EIO");
    assert!(matches!(store.close(), Err(Error::RecoveryRequired)));
    // Recovery from disk only (a power loss keeps only what was synced).
    for image in [fs.crash(), fs.kill()] {
        let opened = Store::open(&dir, options(&image, &bg)).unwrap();
        let state = dump(&opened.store.snapshot());
        for (k, v) in &acked {
            assert_eq!(state.get(k), Some(v), "acknowledged write lost");
        }
        assert!(!opened.store.requires_recovery());
    }
}

/// NS09: a persistent EIO on run writes fails the flushes; after a bounded number of
/// attempts the store latches, writers fail instead of stalling, and reopening
/// recovers every synced write from the journal.
#[test]
fn ns09_eio_on_run_write_latches_and_recovers_from_disk() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.memtable_bytes = 8 << 10;
    let store = open(&dir, o);
    fs.inject(
        Fault::on(FsOp::Write)
            .path_prefix(dir.join("runs"))
            .repeat(),
    );
    let mut acked = BTreeMap::new();
    let started = Instant::now();
    for i in 0..10_000u32 {
        let key = format!("g01/k{i:05}").into_bytes();
        match put_scoped(&store, &key, &[7u8; 200], Durability::Sync) {
            Ok(_) => {
                acked.insert(key, vec![7u8; 200]);
            }
            Err(e) => {
                assert!(e.is_ambiguous(), "{e:?}");
                break;
            }
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "writers never failed"
        );
    }
    assert!(store.requires_recovery());
    assert!(matches!(store.flush(), Err(Error::RecoveryRequired)));
    drop(store);
    fs.clear_faults();
    let opened = Store::open(&dir, options(&fs.crash(), &bg)).unwrap();
    let state = dump(&opened.store.snapshot());
    for (k, v) in &acked {
        assert_eq!(state.get(k), Some(v));
    }
}

/// L0 slowdown paces the writer that asks (`write_delay`), never the journal thread:
/// another scope's write keeps its latency while a paced writer waits.
#[test]
fn ns18_l0_slowdown_paces_the_submitter_not_the_journal() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = options(&fs, &bg);
    o.levels.l0_compaction_trigger = 100;
    o.levels.l0_slowdown_writes = 2;
    o.levels.l0_stop_writes = 100;
    o.levels.delayed_write_rate = 1 << 20;
    let store = open(&dir, o);
    assert_eq!(store.write_delay(64 << 10), Duration::ZERO);
    for round in 0..3u32 {
        put_scoped(
            &store,
            format!("g01/r{round}").as_bytes(),
            b"v",
            Durability::NoSync,
        )
        .unwrap();
        store.flush().unwrap();
    }
    assert!(store.level_runs()[0] >= 2);
    // 64 KiB at 1 MiB/s is about 62 ms, and a second caller queues behind the first.
    let first = store.write_delay(64 << 10);
    let second = store.write_delay(64 << 10);
    assert!(first >= Duration::from_millis(50), "{first:?}");
    assert!(second >= first + Duration::from_millis(50), "{second:?}");
    // While a paced writer sleeps, the journal serves other scopes at once.
    let store = Arc::new(store);
    let paced = {
        let store = store.clone();
        std::thread::spawn(move || {
            store.pace(256 << 10);
            put_scoped(&store, b"g01/paced", b"v", Durability::NoSync).unwrap();
        })
    };
    std::thread::sleep(Duration::from_millis(5));
    let t = Instant::now();
    put_scoped(&store, b"g02/other", b"v", Durability::NoSync).unwrap();
    assert!(
        t.elapsed() < Duration::from_millis(100),
        "an unpaced write waited {:?}",
        t.elapsed()
    );
    paced.join().unwrap();
    assert_eq!(store.stats().stall_ms, 0);
}

/// Run-time G2: the quarantine is in the manifest before the failing read returns, so a
/// crash right after it cannot forget the damage.
#[test]
fn ns07_g2_run_time_quarantine_is_durable_before_the_read_returns() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, options(&fs, &bg));
    put_scoped(&store, b"g01/a", b"<<g1-damaged>>", Durability::Sync).unwrap();
    put_scoped(&store, b"g02/a", b"<<g2-fine>>", Durability::Sync).unwrap();
    store.flush().unwrap();
    store.close().unwrap();
    let runs = files(&fs, &dir.join("runs"), "run");
    let run = runs
        .iter()
        .find(|r| {
            fs.contents(r)
                .unwrap()
                .windows(14)
                .any(|w| w == b"<<g1-damaged>>")
        })
        .expect("the run holding g01")
        .clone();
    // One run holds both groups: a damaged data block is attributed to the read key.
    fs.flip_bit(&run, find(&fs, &run, b"<<g1-damaged>>") + 3, 1)
        .unwrap();
    let store = open(&dir, options(&fs, &bg));
    let err = store.snapshot().get(b"g01/a").unwrap_err();
    assert!(matches!(err, Error::Corruption { .. }), "{err:?}");
    // Power loss right after the read returned.
    let image = fs.crash();
    let reopened = Store::open(&dir, options(&image, &bg)).unwrap();
    let scopes: Vec<_> = reopened
        .quarantined
        .iter()
        .map(|d| (d.scope.clone(), d.reason))
        .collect();
    assert!(
        scopes
            .iter()
            .any(|(s, r)| s.as_ref() == b"g01/" && *r == DamageReason::RunBlock),
        "{scopes:?}"
    );
    drop(store);
}

/// Memory accounting (plan section 5): range tombstones of live runs, flushed memtables
/// pinned by a snapshot, and run writers (back to zero after the flush).
#[test]
fn ns10_memory_usage_charges_tombstones_pinned_memtables_and_writers() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, options(&fs, &bg));
    for i in 0..50u32 {
        put_scoped(
            &store,
            format!("g01/k{i:03}").as_bytes(),
            &[1u8; 100],
            Durability::NoSync,
        )
        .unwrap();
    }
    let mut b = WriteBatch::new();
    b.set_scope(b"g01/");
    b.delete_range(b"g01/k010", b"g01/k020");
    store
        .submit(b, Durability::NoSync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    let snap = store.snapshot();
    let before = store.memory_usage();
    assert!(before.memtables > 0);
    assert_eq!(before.memtables_pinned, 0);
    store.flush().unwrap();
    let after = store.memory_usage();
    assert_eq!(
        after.run_writers, 0,
        "writer memory released after the flush"
    );
    assert!(
        after.memtables_pinned > 0,
        "the snapshot pins the flushed memtable: {after:?}"
    );
    assert!(
        after.cache_pinned > 0,
        "open runs reserve their top-level index"
    );
    // A read through the run loads its range tombstones.
    drop(snap);
    assert_eq!(store.snapshot().get(b"g01/k015").unwrap(), None);
    let now = store.memory_usage();
    assert_eq!(now.memtables_pinned, 0);
    assert!(now.range_tombstones > 0, "{now:?}");
    assert_eq!(store.stats().overhead_bytes, now.overhead());
}

/// KVfix1 concern 2: synced journal bytes behind the manifest's `flushed_seq` lost at
/// open are reported (`journal_behind_manifest`), the numbers are voided and nothing is
/// lost (their state is in runs).
#[test]
fn ns06_journal_behind_the_manifest_is_reported() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, options(&fs, &bg));
    put_scoped(&store, b"g01/a", b"<<first>>", Durability::Sync).unwrap();
    put_scoped(&store, b"g01/b", b"<<second>>", Durability::Sync).unwrap();
    store.flush().unwrap();
    store.close().unwrap();
    // Destroy the journal from the second record's block on (it follows a sync, so it
    // starts on a 4 KiB boundary).
    let segment = files(&fs, &dir.join("journal"), "jnl")
        .into_iter()
        .find(|p| {
            fs.contents(p)
                .unwrap()
                .windows(10)
                .any(|w| w == b"<<second>>")
        })
        .unwrap();
    let at = find(&fs, &segment, b"<<second>>") / 4096 * 4096;
    let len = fs.contents(&segment).unwrap().len() as u64;
    let file = fs.open(&segment, OpenMode::ReadWrite).unwrap();
    file.write_all_at(&vec![0u8; (len - at) as usize], at)
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let opened = Store::open(&dir, options(&fs, &bg)).unwrap();
    assert_eq!(opened.recovery.journal_behind_manifest, 1);
    assert_eq!(opened.store.stats().journal_behind_manifest, 1);
    let snap = opened.store.snapshot();
    assert_eq!(
        snap.get(b"g01/a").unwrap().as_deref(),
        Some(&b"<<first>>"[..])
    );
    assert_eq!(
        snap.get(b"g01/b").unwrap().as_deref(),
        Some(&b"<<second>>"[..])
    );
    drop(snap);
    // New writes take fresh numbers and survive a reopen.
    let seq = put_scoped(&opened.store, b"g01/c", b"third", Durability::Sync).unwrap();
    assert!(seq > opened.store.visible_seq() - 1);
    drop(opened);
    let again = Store::open(&dir, options(&fs.crash(), &bg)).unwrap();
    assert_eq!(again.recovery.journal_behind_manifest, 0);
    assert_eq!(
        again.store.snapshot().get(b"g01/c").unwrap().as_deref(),
        Some(&b"third"[..])
    );
}

/// KVfix1 deviation 5: `Run::open` checks the top-level index, so its damage is found at
/// open. A single-scope run quarantines that scope (G2); a run spanning scopes refuses
/// the store.
#[test]
fn ns07_g2_damaged_top_level_index_found_at_open() {
    for spanning in [false, true] {
        let (fs, dir) = fault_fs();
        let bg = Background::new(2);
        let store = open(&dir, options(&fs, &bg));
        put_scoped(&store, b"g01/a", b"one", Durability::Sync).unwrap();
        if spanning {
            put_scoped(&store, b"g02/a", b"two", Durability::Sync).unwrap();
        }
        store.flush().unwrap();
        store.close().unwrap();
        let runs = files(&fs, &dir.join("runs"), "run");
        assert_eq!(runs.len(), 1);
        let len = fs.contents(&runs[0]).unwrap().len() as u64;
        // The last byte before the footer is the top-level index block's checksum.
        fs.flip_bit(&runs[0], len - FOOTER_LEN as u64 - 1, 0)
            .unwrap();
        match Store::open(&dir, options(&fs, &bg)) {
            Ok(opened) => {
                assert!(!spanning, "a run spanning scopes must refuse the store");
                assert_eq!(opened.quarantined.len(), 1);
                assert_eq!(opened.quarantined[0].scope.as_ref(), b"g01/");
                assert_eq!(opened.quarantined[0].reason, DamageReason::RunFile);
            }
            Err(e) => {
                assert!(spanning, "single-scope damage refused the store: {e:?}");
                assert!(matches!(e, Error::Corruption { .. }), "{e:?}");
            }
        }
    }
}
