//! Regressions for the RV-KV2 store review (findings 1 to 6). Each test reproduced its
//! finding on `a0bc004`; the assertions encode the correct behavior.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use common::*;
use nabedra_kv::fs::{Fault, FaultError, FaultFs, Fs, FsOp};
use nabedra_kv::{Background, Durability, MemtableBudget, Store, WriteBatch};

fn val(round: u64, i: u64, len: usize) -> Vec<u8> {
    let mut v = format!("r{round}i{i}.").into_bytes();
    v.resize(len, b'x');
    v
}

fn manifest_path(fs: &FaultFs, dir: &Path) -> std::path::PathBuf {
    fs.list(dir)
        .unwrap()
        .into_iter()
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("MANIFEST-"))
        })
        .expect("a manifest")
}

/// Finding 1: a trivial move gives one run id two `RunFile` objects. When a later
/// compaction consumes the moved object, its drop deletes the file, although a snapshot
/// taken before the move still reads it through the old object.
#[test]
fn ns03_trivial_move_then_compaction_keeps_files_old_snapshots_read() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.levels.l0_compaction_trigger = 1;
    o.memtable_bytes = 1 << 20;
    o.max_replay_bytes = 1 << 30;
    let store = open(&dir, o);
    let write_round = |prefix: &str, round: u64, n: u64| {
        for i in 0..n {
            put(
                &store,
                format!("{prefix}{i:04}").as_bytes(),
                &val(round, i, 300),
            );
        }
    };
    write_round("a", 0, 60);
    store.flush().unwrap();
    store.compact().unwrap();
    println!("after round 0: levels {:?}", store.level_runs());
    let snap = store.snapshot();
    let expect = dump(&snap);
    assert_eq!(expect.len(), 60);
    for round in 1..30u64 {
        let prefix = if round % 2 == 1 { "b" } else { "a" };
        write_round(prefix, round, 40 + round * 5);
        store.flush().unwrap();
        store.compact().unwrap();
        for (k, v) in &expect {
            match snap.get(k) {
                Ok(Some(got)) => assert_eq!(&got[..], &v[..]),
                other => panic!(
                    "round {round}: old snapshot get({}) -> {:?}; levels now {:?}",
                    String::from_utf8_lossy(k),
                    other.map(|o| o.map(|b| b.len())),
                    store.level_runs()
                ),
            }
        }
    }
    println!("final levels {:?}", store.level_runs());
}

/// Finding 2: a manifest append whose sync fails has an unknown outcome (Fatal), yet the
/// flush deletes the run files that edit references. After a restart the page cache
/// still holds the edit, so the manifest names a missing run.
#[test]
fn ns11_failed_manifest_sync_never_deletes_the_runs_it_names() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.memtable_bytes = 1 << 20;
    o.max_replay_bytes = 1 << 30;
    let store = open(&dir, o);
    for i in 0..50 {
        put(&store, format!("s/k{i:03}").as_bytes(), b"value");
    }
    let manifest = manifest_path(&fs, &dir);
    fs.inject(Fault::on(FsOp::SyncAll).path_prefix(&manifest));
    let r = store.flush();
    println!(
        "flush with failing manifest sync -> {r:?}; requires_recovery {}",
        store.requires_recovery()
    );
    let runs: Vec<_> = fs.list(&dir.join("runs")).unwrap();
    println!("run files after the failed flush: {runs:?}");
    drop(store);
    // Process restart (no power loss): the page cache keeps every completed write.
    let fs2 = fs.kill();
    let o2 = {
        let mut o = small_options(Arc::new(fs2.clone()), bg.clone());
        o.memtable_bytes = 1 << 20;
        o
    };
    match Store::open(&dir, o2) {
        Ok(opened) => {
            println!(
                "reopened; quarantined {:?}",
                opened
                    .quarantined
                    .iter()
                    .map(|d| d.scope.clone())
                    .collect::<Vec<_>>()
            );
            let snap = opened.store.snapshot();
            for i in 0..50 {
                assert_eq!(
                    snap.get(format!("s/k{i:03}").as_bytes())
                        .unwrap()
                        .as_deref(),
                    Some(&b"value"[..])
                );
            }
        }
        Err(e) => panic!("reopen after a failed manifest sync was refused: {e:?}"),
    }
}

/// Finding 3: a manifest roll whose CURRENT directory sync fails removes the new manifest
/// although the rename of CURRENT already happened, so CURRENT names a missing file.
#[test]
fn ns11_failed_current_dir_sync_never_leaves_current_dangling() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.memtable_bytes = 1 << 20;
    o.max_replay_bytes = 1 << 30;
    let store = open(&dir, o);
    for i in 0..50 {
        put(&store, format!("s/k{i:03}").as_bytes(), b"value");
    }
    // 1. ENOSPC on the manifest append: retryable, and the next edit rolls.
    let manifest = manifest_path(&fs, &dir);
    fs.inject(
        Fault::on(FsOp::Write)
            .path_prefix(&manifest)
            .error(FaultError::Enospc),
    );
    println!("flush with ENOSPC on manifest -> {:?}", store.flush());
    fs.clear_faults();
    // 2. The roll: runs dir sync (1), new manifest dir sync (2), CURRENT dir sync (3).
    fs.clear_calls();
    fs.inject(Fault::on(FsOp::SyncDir).path_prefix(&dir).nth(3));
    let r = store.flush();
    println!(
        "flush whose roll fails its CURRENT dir sync -> {r:?}; requires_recovery {}",
        store.requires_recovery()
    );
    for c in fs.calls() {
        let s = format!("{c:?}");
        if s.contains("SyncDir") || s.contains("Rename") || s.contains("Remove") {
            println!("  {s}");
        }
    }
    drop(store);
    let fs2 = fs.kill();
    let current = fs2.contents(&dir.join("CURRENT")).unwrap();
    println!("CURRENT now: {:?}", String::from_utf8_lossy(&current));
    let names: Vec<_> = fs2.list(&dir).unwrap();
    println!("store dir: {names:?}");
    let o2 = small_options(Arc::new(fs2.clone()), bg.clone());
    match Store::open(&dir, o2) {
        Ok(opened) => {
            let snap = opened.store.snapshot();
            for i in 0..50 {
                assert_eq!(
                    snap.get(format!("s/k{i:03}").as_bytes())
                        .unwrap()
                        .as_deref(),
                    Some(&b"value"[..])
                );
            }
        }
        Err(e) => panic!("reopen after a failed CURRENT dir sync was refused: {e:?}"),
    }
}

/// Runs `f` on a thread; true when it finished within `timeout`.
fn finishes_within(timeout: Duration, f: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(timeout).is_ok()
}

/// Finding 5: a snapshot pins the then-active memtable, including everything written
/// into it later. Its charge stays in the worker budget after the flush, so with the
/// budget exhausted every write stalls until the snapshot drops, even though nothing is
/// left to flush.
#[test]
fn ns10_snapshot_pinned_memtable_does_not_stall_writes() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    let budget = Arc::new(MemtableBudget::new(64 << 10));
    o.memtables = budget.clone();
    o.memtable_bytes = 1 << 20;
    o.max_replay_bytes = 1 << 30;
    o.journal.segment_bytes = 1 << 20;
    o.journal.max_group_bytes = 1 << 20;
    let store = Arc::new(open(&dir, o));
    let snap = store.snapshot(); // taken while the active memtable is empty
    put(&store, b"s/big", &vec![7u8; 70 << 10]);
    store.flush().unwrap();
    println!(
        "after flush: budget used {} of {}, levels {:?}",
        budget.used(),
        budget.limit(),
        store.level_runs()
    );
    let s2 = store.clone();
    let done = finishes_within(Duration::from_secs(2), move || put(&s2, b"s/small", b"x"));
    println!("small write finished within 2 s while the snapshot lives: {done}");
    drop(snap);
    if !done {
        // Let the stalled writer finish before the store closes.
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        done,
        "a write stalled on memory a snapshot pins after flush"
    );
}

/// Finding 6: idle stores never flush on budget pressure. Nine stores each hold a
/// memtable just under 1/8 of the shared budget; together they exhaust it, and a tenth
/// store with nothing of its own to flush stalls until one of them writes again.
#[test]
fn ns10_idle_stores_holding_the_budget_are_flushed_for_another_store() {
    let bg = Background::new(2);
    let budget = Arc::new(MemtableBudget::new(64 << 10));
    let mut stores = Vec::new();
    for s in 0..10 {
        let (fs, _) = fault_fs();
        let dir = std::path::PathBuf::from(format!("/db/kv{s}"));
        let mut o = small_options(Arc::new(fs.clone()), bg.clone());
        o.memtables = budget.clone();
        o.memtable_bytes = 1 << 20;
        o.max_replay_bytes = 1 << 30;
        stores.push(Arc::new(open(&dir, o)));
    }
    for store in &stores[..9] {
        put(store, b"s/k", &vec![1u8; 7 << 10]);
    }
    println!(
        "budget used {} of {} by 9 idle stores",
        budget.used(),
        budget.limit()
    );
    let last = stores[9].clone();
    let done = finishes_within(Duration::from_secs(2), move || put(&last, b"s/k", b"x"));
    println!("tenth store's write finished within 2 s: {done}");
    if !done {
        // Unblock: one idle store writes again, freezes and flushes its memtable.
        let mut b = WriteBatch::new();
        b.put(b"s/k2", b"y");
        let _ = stores[0].submit(b, Durability::NoSync, Box::new(()));
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(done, "a store stalled on budget held by idle stores");
}

/// Finding 4: a persistent EIO on run writes fails every flush but never latches, so the
/// store neither progresses nor reports RecoveryRequired; writers stall once the budget
/// fills.
#[test]
fn ns09_persistent_run_write_eio_latches_and_memory_stays_bounded() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    let budget = Arc::new(MemtableBudget::new(64 << 10));
    o.memtables = budget.clone();
    let store = Arc::new(open(&dir, o));
    fs.inject(
        Fault::on(FsOp::Write)
            .path_prefix(dir.join("runs"))
            .repeat(),
    );
    let s2 = store.clone();
    let done = finishes_within(Duration::from_secs(20), move || {
        for i in 0..2000 {
            let mut b = WriteBatch::new();
            b.put(format!("s/k{i:04}").as_bytes(), &[3u8; 1000]);
            match s2.submit(b, Durability::NoSync, Box::new(())) {
                Ok(c) => {
                    if c.wait().is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    let flush = store.flush();
    println!(
        "writers finished or failed within 3 s: {done}; flush -> {flush:?}; requires_recovery {}; budget used {}",
        store.requires_recovery(),
        budget.used()
    );
    fs.clear_faults();
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        done || store.requires_recovery(),
        "writers hung on a persistent run-write EIO without a latch"
    );
    assert!(
        store.requires_recovery(),
        "a persistent run-write EIO never latched RecoveryRequired"
    );
    assert!(
        budget.used() <= 2 * budget.limit(),
        "memtable memory grew past twice the budget while flushes failed"
    );
}
