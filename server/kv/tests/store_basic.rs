//! Store-level basics: reopen, range tombstones across sources, the clean break (NS22),
//! snapshots across flush and compaction (NS03), and ENOSPC (NS11).

mod common;

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::{Fault, FaultError, FaultFs, Fs, FsOp, RealFs};
use nabedra_kv::{Background, Durability, Error, Store, WriteBatch};

#[test]
fn ns_unit_store_round_trip_survives_flush_compaction_and_reopen() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, small_options(Arc::new(fs.clone()), bg.clone()));
    let mut model = BTreeMap::new();
    let mut rng = Rng(7);
    for tag in 0..400 {
        let b = random_batch(&mut rng, 4, 60, tag);
        b.apply(&mut model);
        let durability = if tag % 3 == 0 {
            Durability::Sync
        } else {
            Durability::NoSync
        };
        write(&store, &b, durability).unwrap();
        if tag % 97 == 0 {
            store.flush().unwrap();
        }
    }
    assert_eq!(dump(&store.snapshot()), model);
    store.flush().unwrap();
    store.compact().unwrap();
    assert_eq!(dump(&store.snapshot()), model);
    let runs = store.level_runs();
    assert!(runs.iter().sum::<usize>() > 0, "runs {runs:?}");
    for (k, v) in &model {
        assert_eq!(store.snapshot().get(k).unwrap().as_deref(), Some(&v[..]));
    }
    store.close().unwrap();
    let opened = Store::open(&dir, small_options(Arc::new(fs.clone()), bg.clone())).unwrap();
    assert!(opened.quarantined.is_empty());
    assert_eq!(dump(&opened.store.snapshot()), model);
    drop(opened);
    // A crash keeps every synced write, and here the clean close synced all of them.
    let crashed = fs.crash();
    let store = open(&dir, small_options(Arc::new(crashed), bg));
    assert_eq!(dump(&store.snapshot()), model);
}

/// Rule 2 of the lead update: a range tombstone in a newer source hides older versions in
/// memtables and in every level, for point reads and iterators.
#[test]
fn ns21_range_tombstones_apply_across_memtables_and_runs() {
    let (fs, dir) = fault_fs();
    let store = open(&dir, small_options(Arc::new(fs), Background::new(2)));
    for k in 0..20 {
        put(&store, &group_key(1, k), b"old");
    }
    store.flush().unwrap();
    store.compact().unwrap(); // older versions now deeper than L0
    for k in 0..20 {
        put(&store, &group_key(1, k), b"mid");
    }
    store.flush().unwrap(); // an L0 run
    let before = store.snapshot();
    let mut b = WriteBatch::new();
    b.set_scope(&group_prefix(1));
    b.delete_range(&group_key(1, 5), &group_key(1, 15));
    store
        .submit(b, Durability::Sync, Box::new(()))
        .unwrap()
        .wait()
        .unwrap();
    put(&store, &group_key(1, 10), b"new"); // newer than the tombstone
    let check = |store: &Store, label: &str| {
        let snap = store.snapshot();
        for k in 0..20 {
            let expected: Option<&[u8]> = match k {
                10 => Some(b"new"),
                5..=14 => None,
                _ => Some(b"mid"),
            };
            assert_eq!(
                snap.get(&group_key(1, k)).unwrap().as_deref(),
                expected,
                "{label} k{k}"
            );
        }
        let lo = group_key(1, 0);
        let hi = group_key(1, 99);
        let mut it = snap.iter(Bound::Included(&lo), Bound::Excluded(&hi), true);
        let mut keys = Vec::new();
        it.seek_to_first();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.next();
        }
        assert_eq!(keys.len(), 11, "{label}");
        let mut rev = Vec::new();
        it.seek_to_last();
        while it.valid() {
            rev.push(it.key().to_vec());
            it.prev();
        }
        rev.reverse();
        assert_eq!(keys, rev, "{label}");
        // The snapshot from before the delete still sees every key.
        assert_eq!(dump(&before).len(), 20, "{label}");
    };
    check(&store, "tombstone in memtable");
    store.flush().unwrap();
    check(&store, "tombstone in L0");
    store.compact().unwrap();
    check(&store, "after compaction");
}

/// NS22: a RocksDB directory, an unknown FORMAT, and a stray file are refused with
/// UnsupportedFormat naming the path, and the directory is left byte-identical.
#[test]
fn ns22_foreign_directories_are_refused_untouched() {
    let tmp = TempDir::new("ns22");
    let rocks = tmp.path().join("rocksdb");
    std::fs::create_dir_all(&rocks).unwrap();
    for (name, content) in [
        ("CURRENT", &b"MANIFEST-000005\n"[..]),
        ("MANIFEST-000005", b"\x01\x02rocks"),
        ("000004.log", b"wal bytes"),
        ("000007.sst", b"sst bytes"),
        ("OPTIONS-000009", b"[Version]"),
        ("IDENTITY", b"abc"),
        ("LOCK", b""),
    ] {
        std::fs::write(rocks.join(name), content).unwrap();
    }
    let unknown = tmp.path().join("unknown");
    std::fs::create_dir_all(&unknown).unwrap();
    std::fs::write(unknown.join("FORMAT"), b"nabedra-kv 2\n").unwrap();
    let legacy = tmp.path().join("legacy");
    std::fs::create_dir_all(legacy.join("rocksdb")).unwrap();
    std::fs::write(
        legacy.join("NABEDRA_OLTP_POOL.json"),
        b"{\"pool_format_version\":1}",
    )
    .unwrap();

    let snapshot = |dir: &Path| -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    out.insert(p.display().to_string(), Vec::new());
                    stack.push(p);
                } else {
                    out.insert(p.display().to_string(), std::fs::read(&p).unwrap());
                }
            }
        }
        out
    };
    let bg = Background::new(1);
    for dir in [&rocks, &unknown, &legacy] {
        let before = snapshot(dir);
        let fs: Arc<dyn Fs> = Arc::new(RealFs);
        match Store::open(dir, small_options(fs, bg.clone())) {
            Err(Error::UnsupportedFormat { found }) => {
                assert!(found.contains(&dir.display().to_string()), "{found}");
            }
            other => panic!(
                "{}: expected UnsupportedFormat, got {:?}",
                dir.display(),
                other.map(|_| ())
            ),
        }
        assert_eq!(snapshot(dir), before, "{} changed", dir.display());
    }
}

/// NS03 (flush and compaction part): a snapshot reads the same through flushes and
/// compactions, and the runs it pins are deleted only after it drops.
#[test]
fn ns03_snapshot_survives_flush_and_compaction_and_pins_runs() {
    let (fs, dir) = fault_fs();
    let store = open(
        &dir,
        small_options(Arc::new(fs.clone()), Background::new(2)),
    );
    let mut model = BTreeMap::new();
    let mut rng = Rng(3);
    for tag in 0..200 {
        let b = random_batch(&mut rng, 3, 40, tag);
        b.apply(&mut model);
        write(&store, &b, Durability::NoSync).unwrap();
    }
    store.flush().unwrap();
    let snap = store.snapshot();
    let expected = dump(&snap);
    assert_eq!(expected, model);
    let run_files = |fs: &FaultFs| -> Vec<String> {
        let mut v: Vec<String> = fs
            .list(&dir.join("runs"))
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        v.sort();
        v
    };
    let pinned = run_files(&fs);
    for tag in 200..600 {
        let b = random_batch(&mut rng, 3, 40, tag);
        write(&store, &b, Durability::NoSync).unwrap();
        if tag % 50 == 0 {
            store.flush().unwrap();
            store.compact().unwrap();
            assert_eq!(dump(&snap), expected, "tag {tag}");
        }
    }
    store.flush().unwrap();
    store.compact().unwrap();
    assert_eq!(dump(&snap), expected);
    let now = run_files(&fs);
    for name in &pinned {
        assert!(
            now.contains(name),
            "{name} deleted while a snapshot pins it"
        );
    }
    drop(snap);
    // Another compaction pass purges files no version references any more.
    for tag in 600..700 {
        write(
            &store,
            &random_batch(&mut rng, 3, 40, tag),
            Durability::NoSync,
        )
        .unwrap();
    }
    store.flush().unwrap();
    store.compact().unwrap();
    let after = run_files(&fs);
    assert!(
        pinned.iter().any(|n| !after.contains(n)),
        "obsolete runs were never deleted: {pinned:?} vs {after:?}"
    );
}

/// NS11: ENOSPC in a flush (run write), a compaction and a manifest write returns
/// NoSpace, corrupts nothing, and work resumes once space frees up.
#[test]
fn ns11_no_space_in_flush_compaction_and_manifest_is_recoverable() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2);
    let store = open(&dir, small_options(Arc::new(fs.clone()), bg.clone()));
    let mut model = BTreeMap::new();
    let mut rng = Rng(11);
    let mut tag = 0;
    let mut fill = |store: &Store, model: &mut BTreeMap<_, _>, n: u64| {
        for _ in 0..n {
            let b = random_batch(&mut rng, 3, 40, tag);
            tag += 1;
            b.apply(model);
            write(store, &b, Durability::Sync).unwrap();
        }
    };
    let runs = dir.join("runs");
    let manifest = |fs: &FaultFs| {
        fs.list(&dir)
            .unwrap()
            .into_iter()
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("MANIFEST-")
            })
            .unwrap()
    };
    for label in ["run write", "manifest write"] {
        let fault = match label {
            "run write" => Fault::on(FsOp::Write).path_prefix(&runs),
            _ => Fault::on(FsOp::Write).path_prefix(manifest(&fs)),
        };
        fill(&store, &mut model, 40);
        fs.inject(fault.error(FaultError::Enospc).repeat());
        let result = store.flush();
        assert_eq!(result, Err(Error::NoSpace), "{label}");
        assert!(!store.requires_recovery(), "{label}");
        assert_eq!(dump(&store.snapshot()), model, "{label}");
        // Writes keep working while background work waits for space.
        fill(&store, &mut model, 5);
        fs.clear_faults();
        store.flush().unwrap();
        assert_eq!(dump(&store.snapshot()), model, "{label}");
    }
    // Compaction.
    fill(&store, &mut model, 60);
    store.flush().unwrap();
    fs.inject(
        Fault::on(FsOp::Write)
            .path_prefix(&runs)
            .error(FaultError::Enospc)
            .repeat(),
    );
    assert_eq!(store.compact(), Err(Error::NoSpace));
    assert_eq!(dump(&store.snapshot()), model);
    fs.clear_faults();
    store.compact().unwrap();
    assert_eq!(dump(&store.snapshot()), model);
    store.close().unwrap();
    let store = open(&dir, small_options(Arc::new(fs.crash()), bg));
    assert_eq!(dump(&store.snapshot()), model);
}
