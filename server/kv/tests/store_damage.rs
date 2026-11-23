//! Store-level damage outcomes (plan section 3): NS06, NS07 and NS25.
//!
//! Bytes are found by searching for unique values, never by format offsets, except the
//! plan's invariant that a group commit following a sync starts on a 4 KiB boundary.
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use nabedra_kv::fs::{FaultFs, Fs, OpenMode};
use nabedra_kv::{
    Background, Damage, DamageReason, Durability, Error, FileId, Options, Store, WriteBatch,
};

fn options(fs: &FaultFs, bg: &Arc<Background>) -> Options {
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    // Keep runs where the test put them.
    o.levels.l0_compaction_trigger = 100;
    o
}

fn files_under(fs: &FaultFs, dir: &Path) -> Vec<PathBuf> {
    let mut v = fs.list(dir).unwrap();
    v.sort();
    v
}

/// Every `(file, offset)` in `dir` where `needle` occurs.
fn find(fs: &FaultFs, dir: &Path, needle: &[u8]) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for path in files_under(fs, dir) {
        let Ok(data) = fs.contents(&path) else {
            continue;
        };
        for (i, w) in data.windows(needle.len()).enumerate() {
            if w == needle {
                out.push((path.clone(), i as u64));
            }
        }
    }
    out
}

fn find_one(fs: &FaultFs, dir: &Path, needle: &[u8]) -> (PathBuf, u64) {
    let hits = find(fs, dir, needle);
    assert_eq!(
        hits.len(),
        1,
        "needle {:?}: {hits:?}",
        String::from_utf8_lossy(needle)
    );
    hits[0].clone()
}

fn write_kv(store: &Store, scope: &[u8], key: &[u8], value: &[u8], journal: bool, d: Durability) {
    let mut b = WriteBatch::new();
    b.set_scope(scope);
    if journal {
        b.put_journal(key, value);
    } else {
        b.put(key, value);
    }
    store.submit(b, d, Box::new(())).unwrap().wait().unwrap();
}

fn overwrite(fs: &FaultFs, path: &Path, offset: u64, bytes: &[u8]) {
    let file = fs.open(path, OpenMode::ReadWrite).unwrap();
    file.write_all_at(bytes, offset).unwrap();
}

fn is_corruption(r: &nabedra_kv::Result<Option<bytes::Bytes>>) -> bool {
    matches!(r, Err(Error::Corruption { .. }))
}

fn wait_until(what: &str, f: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn val(tag: &str) -> Vec<u8> {
    let mut v = format!("<<{tag}>>").into_bytes();
    v.resize(200, b'.');
    v
}

/// NS07 G1: a synced record of group g01 that a later record names durable is damaged.
/// The store opens, quarantines exactly g01, keeps serving g02 and store-wide keys, keeps
/// the quarantine across reopens, and lifts it after `release_scope`.
#[test]
fn ns07_g1_group_record_damage_quarantines_only_that_group() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    let g1 = group_prefix(1);
    let g2 = group_prefix(2);
    write_kv(
        &store,
        &g1,
        &group_key(1, 1),
        &val("g1-a"),
        false,
        Durability::Sync,
    );
    write_kv(
        &store,
        &g2,
        &group_key(2, 1),
        &val("g2-a"),
        false,
        Durability::Sync,
    );
    write_kv(&store, b"", b"s/k1", &val("sw-a"), false, Durability::Sync);
    let image = fs.kill();
    drop(store);
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<g1-a>>");
    image.flip_bit(&seg, off + 20, 3).unwrap();

    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    assert_eq!(opened.quarantined.len(), 1, "{:?}", opened.quarantined);
    let d = &opened.quarantined[0];
    assert_eq!(d.scope.as_ref(), g1.as_slice());
    assert_eq!(d.reason, DamageReason::JournalRecord);
    assert!(matches!(d.file, FileId::Journal { .. }));
    let s = opened.store;
    let snap = s.snapshot();
    assert!(is_corruption(&snap.get(&group_key(1, 1))));
    assert_eq!(
        snap.get(&group_key(2, 1)).unwrap().unwrap().as_ref(),
        val("g2-a")
    );
    assert_eq!(snap.get(b"s/k1").unwrap().unwrap().as_ref(), val("sw-a"));
    assert_eq!(s.stats().quarantined_scopes, 1);
    // Flush past the damaged record: the quarantine must survive in the manifest.
    write_kv(
        &s,
        &g2,
        &group_key(2, 2),
        &val("g2-b"),
        false,
        Durability::Sync,
    );
    s.flush().unwrap();
    drop(snap);
    s.close().unwrap();
    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    assert_eq!(opened.quarantined.len(), 1);
    let s = opened.store;
    assert!(is_corruption(&s.snapshot().get(&group_key(1, 1))));

    // Release: the group's keys are deleted and the quarantine is lifted.
    s.release_scope(&g1).unwrap();
    assert!(s.quarantined().is_empty());
    assert_eq!(s.snapshot().get(&group_key(1, 1)).unwrap(), None);
    // A new replica of the group can write under the same prefix.
    write_kv(
        &s,
        &g1,
        &group_key(1, 9),
        &val("g1-new"),
        false,
        Durability::Sync,
    );
    // RV-KV2 finding 8: a crash right after release_scope returned and a synced write
    // under the prefix completed must keep both (the release is recorded already, so
    // replay does not skip the scope's later records as G1).
    let cut = image.crash();
    let opened = Store::open(&dir, options(&cut, &bg)).unwrap();
    assert!(opened.quarantined.is_empty(), "{:?}", opened.quarantined);
    assert_eq!(
        opened
            .store
            .snapshot()
            .get(&group_key(1, 9))
            .unwrap()
            .unwrap()
            .as_ref(),
        val("g1-new")
    );
    drop(opened);
    s.close().unwrap();
    let opened = Store::open(&dir, options(&image.crash(), &bg)).unwrap();
    assert!(opened.quarantined.is_empty(), "{:?}", opened.quarantined);
    let snap = opened.store.snapshot();
    assert_eq!(
        snap.get(&group_key(1, 9)).unwrap().unwrap().as_ref(),
        val("g1-new")
    );
    assert_eq!(snap.get(&group_key(1, 1)).unwrap(), None);
    assert_eq!(
        snap.get(&group_key(2, 2)).unwrap().unwrap().as_ref(),
        val("g2-b")
    );
}

/// NS07 / R6 (RV-NS6 F2): `Opened::quarantined_now` names only the scopes this open
/// quarantined. Damage persisted by an earlier open stays in `quarantined` but is not
/// counted again, so two isolated damages found at different opens never add up.
#[test]
fn ns07_r6_quarantined_now_names_only_this_opens_damage() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    let sync = Durability::Sync;
    write_kv(
        &store,
        &group_prefix(1),
        &group_key(1, 1),
        &val("g1-a"),
        false,
        sync,
    );
    write_kv(
        &store,
        &group_prefix(3),
        &group_key(3, 1),
        &val("g3-a"),
        false,
        sync,
    );
    let image = fs.kill();
    drop(store);
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<g1-a>>");
    image.flip_bit(&seg, off + 20, 3).unwrap();

    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    let scopes = |list: &[Damage]| {
        let mut scopes: Vec<Vec<u8>> = list.iter().map(|d| d.scope.to_vec()).collect();
        scopes.sort();
        scopes
    };
    assert_eq!(scopes(&opened.quarantined), vec![group_prefix(1)]);
    assert_eq!(scopes(&opened.quarantined_now), vec![group_prefix(1)]);
    let s = opened.store;
    write_kv(
        &s,
        &group_prefix(2),
        &group_key(2, 1),
        &val("g2-a"),
        false,
        sync,
    );
    write_kv(
        &s,
        &group_prefix(3),
        &group_key(3, 2),
        &val("g3-b"),
        false,
        sync,
    );
    s.close().unwrap();
    let image = image.kill();
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<g2-a>>");
    image.flip_bit(&seg, off + 20, 3).unwrap();

    // The second open finds g01's record damaged again and g02's for the first time.
    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    assert_eq!(
        scopes(&opened.quarantined),
        vec![group_prefix(1), group_prefix(2)]
    );
    assert_eq!(scopes(&opened.quarantined_now), vec![group_prefix(2)]);
    assert_eq!(
        opened
            .store
            .snapshot()
            .get(&group_key(3, 2))
            .unwrap()
            .unwrap()
            .as_ref(),
        val("g3-b")
    );
    opened.store.close().unwrap();
    let opened = Store::open(&dir, options(&image.kill(), &bg)).unwrap();
    assert_eq!(opened.quarantined.len(), 2);
    assert!(
        opened.quarantined_now.is_empty(),
        "{:?}",
        opened.quarantined_now
    );
}

/// NS07 R1: damage to a durable store-wide record refuses the store, and stays refused.
#[test]
fn ns07_r1_store_wide_record_damage_refuses() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    write_kv(&store, b"", b"s/k1", &val("sw-a"), false, Durability::Sync);
    write_kv(
        &store,
        &group_prefix(2),
        &group_key(2, 1),
        &val("g2"),
        false,
        Durability::Sync,
    );
    let image = fs.kill();
    drop(store);
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<sw-a>>");
    image.flip_bit(&seg, off + 30, 0).unwrap();
    for _ in 0..2 {
        match Store::open(&dir, options(&image, &bg)) {
            Err(Error::Corruption {
                file: FileId::Journal { .. },
                ..
            }) => {}
            other => panic!("expected R1, got {:?}", other.map(|_| ())),
        }
    }
}

/// NS07 G2 / FLAKE1: a block cached by an earlier read was checked when it was read, so
/// damage to its bytes on disk afterwards is not met while it stays cached: the read
/// returns the checked value, never the damaged bytes. Once the store's cached blocks are
/// evicted, the next read goes to the file, returns Corruption and quarantines g01.
#[test]
fn ns07_g2_cached_block_was_checked_and_eviction_meets_later_damage() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let s = open(&dir, options(&fs, &bg));
    for k in 0..30 {
        write_kv(
            &s,
            &group_prefix(1),
            &group_key(1, k),
            &val(&format!("g1-{k:02}")),
            false,
            Durability::NoSync,
        );
    }
    s.flush().unwrap();
    // This read fills the block cache with the checked block.
    let key = group_key(1, 7);
    assert_eq!(
        s.snapshot().get(&key).unwrap().unwrap().as_ref(),
        val("g1-07")
    );
    let (run, off) = find_one(&fs, &dir.join("runs"), b"<<g1-07>>");
    fs.flip_bit(&run, off + 50, 6).unwrap();
    let before = s.stats().corruption_detected;
    assert_eq!(
        s.snapshot().get(&key).unwrap().unwrap().as_ref(),
        val("g1-07"),
        "the cached block was checked when it was read"
    );
    assert_eq!(s.stats().corruption_detected, before);
    assert!(s.quarantined().is_empty());

    assert!(s.evict_cached_blocks() > 0);
    let r = s.snapshot().get(&key);
    assert!(
        matches!(
            r,
            Err(Error::Corruption {
                file: FileId::Run { .. },
                ..
            })
        ),
        "{r:?}"
    );
    assert!(s.stats().corruption_detected > before);
    assert_eq!(s.quarantined().len(), 1);
    assert_eq!(
        s.quarantined()[0].scope.as_ref(),
        group_prefix(1).as_slice()
    );
}

/// NS07 G2 at read time: a damaged data block of a run holding only g01 makes the read
/// return Corruption (never a wrong value), counts it, reports g01 to `on_damage`,
/// quarantines g01 for later reads and persists that.
#[test]
fn ns07_g2_run_block_damage_quarantines_on_read() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    for k in 0..30 {
        write_kv(
            &store,
            &group_prefix(1),
            &group_key(1, k),
            &val(&format!("g1-{k:02}")),
            false,
            Durability::NoSync,
        );
    }
    store.flush().unwrap();
    for k in 0..30 {
        write_kv(
            &store,
            &group_prefix(2),
            &group_key(2, k),
            &val(&format!("g2-{k:02}")),
            false,
            Durability::NoSync,
        );
    }
    store.flush().unwrap();
    drop(store);
    let (run, off) = find_one(&fs, &dir.join("runs"), b"<<g1-07>>");
    fs.flip_bit(&run, off + 50, 6).unwrap();

    let opened = Store::open(&dir, options(&fs, &bg)).unwrap();
    assert!(opened.quarantined.is_empty());
    let s = opened.store;
    let seen: Arc<Mutex<Vec<Damage>>> = Arc::default();
    let sink = seen.clone();
    s.on_damage(Box::new(move |d| sink.lock().unwrap().push(d)));
    let before = s.stats().corruption_detected;
    let r = s.snapshot().get(&group_key(1, 7));
    assert!(
        matches!(
            r,
            Err(Error::Corruption {
                file: FileId::Run { .. },
                ..
            })
        ),
        "{r:?}"
    );
    assert!(s.stats().corruption_detected > before);
    let damage = seen.lock().unwrap().clone();
    assert_eq!(damage.len(), 1, "{damage:?}");
    assert_eq!(damage[0].scope.as_ref(), group_prefix(1).as_slice());
    assert_eq!(damage[0].reason, DamageReason::RunBlock);
    // Every later read of g01 fails; g02 serves.
    assert!(is_corruption(&s.snapshot().get(&group_key(1, 20))));
    assert_eq!(
        s.snapshot()
            .get(&group_key(2, 7))
            .unwrap()
            .unwrap()
            .as_ref(),
        val("g2-07")
    );
    wait_until("quarantine persisted", || {
        s.compact().is_ok() && !s.quarantined().is_empty()
    });
    drop(s);
    let opened = Store::open(&dir, options(&fs, &bg)).unwrap();
    assert_eq!(opened.quarantined.len(), 1);
    assert_eq!(
        opened.quarantined[0].scope.as_ref(),
        group_prefix(1).as_slice()
    );
    // RV-NS6 F2: persisted at run time, so not quarantined by this open.
    assert!(
        opened.quarantined_now.is_empty(),
        "{:?}",
        opened.quarantined_now
    );
}

/// NS07 G2 and rule 4 (KVfix3): a scope released earlier and damaged later stays
/// quarantined across every reopen. `ManifestState::apply` lifted a record's quarantines
/// for the scopes the same record released, and a fresh manifest stores the whole state
/// as one record, so the second open after the damage dropped the quarantine of any
/// scope with a past release (NS10 NS07 trials, seed 38).
#[test]
fn ns07_quarantine_of_a_once_released_scope_survives_reopens() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    store.release_scope(&group_prefix(1)).unwrap();
    for k in 0..30 {
        write_kv(
            &store,
            &group_prefix(1),
            &group_key(1, k),
            &val(&format!("g1-{k:02}")),
            false,
            Durability::NoSync,
        );
    }
    store.flush().unwrap();
    drop(store);
    let (run, off) = find_one(&fs, &dir.join("runs"), b"<<g1-07>>");
    fs.flip_bit(&run, off + 50, 6).unwrap();
    let s = Store::open(&dir, options(&fs, &bg)).unwrap().store;
    assert!(is_corruption(&s.snapshot().get(&group_key(1, 7))));
    assert_eq!(s.quarantined().len(), 1);
    drop(s);
    // Each open writes a fresh manifest holding the whole state; each round reopens a
    // crash image of the previous one, so only durable state counts.
    let mut image = fs.crash();
    for round in 0..3 {
        let opened = Store::open(&dir, options(&image, &bg)).unwrap();
        let scopes: Vec<Vec<u8>> = opened
            .quarantined
            .iter()
            .map(|d| d.scope.to_vec())
            .collect();
        assert_eq!(scopes, vec![group_prefix(1)], "open {round}");
        drop(opened);
        image = image.crash();
    }
}

/// NS07 G1 (KVfix3): an iterator that crosses a quarantined scope fails even when no
/// key of that scope is left to land on. Recovery skipped g01's only record, so g01 is
/// empty; scans of g01 returned `Ok` with nothing, and scans across it silently skipped
/// it (NS10 long campaign, seed 69: "scan of g00/ returned wrong data: missing ...").
#[test]
fn ns07_g1_scans_fail_across_a_quarantined_scope_with_no_keys_left() {
    use std::ops::Bound;
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    for (g, tag) in [(0, "g0-a"), (1, "g1-a"), (2, "g2-a")] {
        write_kv(
            &store,
            &group_prefix(g),
            &group_key(g, 1),
            &val(tag),
            false,
            Durability::Sync,
        );
    }
    let image = fs.kill();
    drop(store);
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<g1-a>>");
    image.flip_bit(&seg, off + 20, 3).unwrap();
    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    assert_eq!(opened.quarantined.len(), 1, "{:?}", opened.quarantined);
    let s = opened.store;
    let snap = s.snapshot();
    let g1 = group_prefix(1);
    let g2 = group_prefix(2);
    // Returns the keys seen and whether the scan ended in `Corruption`.
    let scan = |lower: Bound<&[u8]>, upper: Bound<&[u8]>, reverse: bool| {
        let mut it = snap.iter(lower, upper, false);
        if reverse {
            it.seek_to_last();
        } else {
            it.seek_to_first();
        }
        let mut seen = Vec::new();
        while it.valid() {
            seen.push(it.key().to_vec());
            if reverse {
                it.prev();
            } else {
                it.next();
            }
        }
        (seen, matches!(it.status(), Err(Error::Corruption { .. })))
    };
    for reverse in [false, true] {
        // The quarantined scope alone.
        let (seen, corrupt) = scan(Bound::Included(&g1), Bound::Excluded(&g2), reverse);
        assert!(seen.is_empty() && corrupt, "g01 alone, reverse {reverse}");
        // Across it: g00's key or g02's key may come first, then the scan fails.
        let (seen, corrupt) = scan(Bound::Unbounded, Bound::Unbounded, reverse);
        assert!(corrupt, "across g01, reverse {reverse}: {seen:?}");
        assert!(seen.len() <= 1, "{seen:?}");
        // A healthy scope still scans.
        let (seen, corrupt) = scan(Bound::Included(&g2), Bound::Unbounded, reverse);
        assert!(
            !corrupt && seen == vec![group_key(2, 1)],
            "g02, reverse {reverse}"
        );
    }
    // A seek past the quarantined scope does not cross it.
    let mut it = snap.iter(Bound::Unbounded, Bound::Unbounded, false);
    it.seek(&g2);
    assert!(it.valid());
    assert_eq!(it.key(), group_key(2, 1).as_slice());
}

/// A run holding only `keys` (written with `scope`) whose value tagged `damaged` has a
/// flipped bit, reopened. Returns the open store.
fn store_with_damaged_run(
    fs: &FaultFs,
    dir: &Path,
    bg: &Arc<Background>,
    scope: &[u8],
    keys: &[Vec<u8>],
    damaged: &str,
) -> Store {
    let store = open(dir, options(fs, bg));
    for (k, key) in keys.iter().enumerate() {
        write_kv(
            &store,
            scope,
            key,
            &val(&format!("v-{k:02}")),
            false,
            Durability::NoSync,
        );
    }
    store.flush().unwrap();
    drop(store);
    let (run, off) = find_one(fs, &dir.join("runs"), format!("<<{damaged}>>").as_bytes());
    fs.flip_bit(&run, off + 50, 6).unwrap();
    let opened = Store::open(dir, options(fs, bg)).unwrap();
    assert!(opened.quarantined.is_empty());
    opened.store
}

/// A full scan of `[lower, upper)`; true if it ended in `Corruption`.
fn scan_meets_corruption(store: &Store, lower: &[u8], upper: Option<&[u8]>) -> bool {
    use std::ops::Bound;
    let snap = store.snapshot();
    let upper = upper.map_or(Bound::Unbounded, Bound::Excluded);
    let mut it = snap.iter(Bound::Included(lower), upper, false);
    it.seek_to_first();
    while it.valid() {
        it.next();
    }
    matches!(it.status(), Err(Error::Corruption { .. }))
}

/// NS07 R5 (KVfix3): a scan over store-wide keys that meets a damaged block latches
/// `RecoveryRequired`. Iterators attributed run damage only when their bounds lay in
/// one group scope, so a store-wide scan returned `Corruption` and latched nothing
/// (NS10 long campaign, seed 0 damage trial 3).
#[test]
fn ns07_r5_store_wide_scan_meeting_run_damage_latches() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let keys: Vec<Vec<u8>> = (0..30).map(|k| format!("s/k{k:03}").into_bytes()).collect();
    let s = store_with_damaged_run(&fs, &dir, &bg, b"", &keys, "v-07");
    assert!(scan_meets_corruption(&s, b"s/", Some(b"s0")));
    assert!(s.requires_recovery(), "R5: store-wide damage must latch");
    assert!(s.quarantined().is_empty(), "{:?}", s.quarantined());
}

/// NS07 G2 (KVfix3): a scan whose bounds span scopes that meets a damaged block in a
/// run holding one group quarantines that group (the run's range, plan section 3)
/// instead of reporting nothing.
#[test]
fn ns07_g2_unbounded_scan_quarantines_a_single_group_run() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let keys: Vec<Vec<u8>> = (0..30).map(|k| group_key(1, k)).collect();
    let s = store_with_damaged_run(&fs, &dir, &bg, &group_prefix(1), &keys, "v-07");
    assert!(scan_meets_corruption(&s, b"", None));
    assert!(!s.requires_recovery());
    let q: Vec<Vec<u8>> = s.quarantined().iter().map(|d| d.scope.to_vec()).collect();
    assert_eq!(q, vec![group_prefix(1)]);
}

/// NS07 G2 at open: a damaged footer of a run holding one group quarantines that group;
/// one spanning groups refuses the store.
#[test]
fn ns07_g2_run_footer_damage_at_open() {
    for spanning in [false, true] {
        let (fs, dir) = fault_fs();
        let bg = Background::new(2).unwrap();
        let store = open(&dir, options(&fs, &bg));
        for k in 0..10 {
            write_kv(
                &store,
                &group_prefix(1),
                &group_key(1, k),
                &val("one"),
                false,
                Durability::NoSync,
            );
        }
        if spanning {
            write_kv(
                &store,
                &group_prefix(3),
                &group_key(3, 0),
                &val("three"),
                false,
                Durability::NoSync,
            );
        }
        store.flush().unwrap();
        write_kv(
            &store,
            &group_prefix(2),
            &group_key(2, 0),
            &val("two"),
            false,
            Durability::Sync,
        );
        drop(store);
        let runs = files_under(&fs, &dir.join("runs"));
        assert_eq!(runs.len(), 1);
        let len = fs.contents(&runs[0]).unwrap().len() as u64;
        fs.flip_bit(&runs[0], len - 1, 4).unwrap();
        let result = Store::open(&dir, options(&fs, &bg));
        if spanning {
            assert!(
                matches!(
                    result,
                    Err(Error::Corruption {
                        file: FileId::Run { .. },
                        ..
                    })
                ),
                "{:?}",
                result.map(|_| ())
            );
            continue;
        }
        let opened = result.unwrap();
        assert_eq!(opened.quarantined.len(), 1);
        assert_eq!(opened.quarantined[0].reason, DamageReason::RunFile);
        let snap = opened.store.snapshot();
        assert!(is_corruption(&snap.get(&group_key(1, 3))));
        assert_eq!(
            snap.get(&group_key(2, 0)).unwrap().unwrap().as_ref(),
            val("two")
        );
        assert!(opened.store.stats().corruption_detected >= 1);
        // The damaged file is kept until the scope is released.
        drop(snap);
        drop(opened);
        assert_eq!(files_under(&fs, &dir.join("runs")).len(), 1);
        let s = open(&dir, options(&fs, &bg));
        s.release_scope(&group_prefix(1)).unwrap();
        assert!(s.quarantined().is_empty());
        assert!(!files_under(&fs, &dir.join("runs")).contains(&runs[0]));
    }
}

/// NS07 G3: a journal value whose CRC fails when read (from a memtable or a run)
/// returns Corruption and quarantines the key's group; a store-wide one latches
/// RecoveryRequired (R5).
#[test]
fn ns07_g3_journal_value_damage() {
    for flushed in [false, true] {
        let (fs, dir) = fault_fs();
        let bg = Background::new(2).unwrap();
        let store = open(&dir, options(&fs, &bg));
        let seen: Arc<Mutex<Vec<Damage>>> = Arc::default();
        let sink = seen.clone();
        store.on_damage(Box::new(move |d| sink.lock().unwrap().push(d)));
        write_kv(
            &store,
            &group_prefix(1),
            &group_key(1, 1),
            &val("jv-g1"),
            true,
            Durability::Sync,
        );
        write_kv(&store, b"", b"s/k1", &val("jv-sw"), true, Durability::Sync);
        write_kv(
            &store,
            &group_prefix(2),
            &group_key(2, 1),
            &val("jv-g2"),
            true,
            Durability::Sync,
        );
        if flushed {
            store.flush().unwrap();
        }
        let (seg, off) = find_one(&fs, &dir.join("journal"), b"<<jv-g1>>");
        fs.flip_bit(&seg, off + 40, 1).unwrap();
        let r = store.snapshot().get(&group_key(1, 1));
        assert!(
            matches!(
                r,
                Err(Error::Corruption {
                    file: FileId::JournalValue { .. },
                    ..
                })
            ),
            "{r:?}"
        );
        assert_eq!(seen.lock().unwrap()[0].reason, DamageReason::JournalValue);
        assert_eq!(
            seen.lock().unwrap()[0].scope.as_ref(),
            group_prefix(1).as_slice()
        );
        assert!(store.stats().corruption_detected >= 1);
        assert_eq!(
            store
                .snapshot()
                .get(&group_key(2, 1))
                .unwrap()
                .unwrap()
                .as_ref(),
            val("jv-g2")
        );
        assert!(!store.requires_recovery());
        // Store-wide: R5.
        let (seg, off) = find_one(&fs, &dir.join("journal"), b"<<jv-sw>>");
        fs.flip_bit(&seg, off + 40, 1).unwrap();
        assert!(is_corruption(&store.snapshot().get(b"s/k1")));
        assert!(store.requires_recovery(), "flushed {flushed}");
        assert!(matches!(
            store.submit(WriteBatch::new(), Durability::Sync, Box::new(())),
            Err(Error::RecoveryRequired)
        ));
    }
}

/// NS07 R3: a damaged CURRENT, or a damaged manifest record followed by valid ones,
/// refuses the store.
#[test]
fn ns07_r3_current_and_manifest_damage_refuse() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, options(&fs, &bg));
    for round in 0..3 {
        write_kv(
            &store,
            &group_prefix(1),
            &group_key(1, round),
            &val("m"),
            false,
            Durability::Sync,
        );
        store.flush().unwrap();
    }
    drop(store);
    let damaged = fs.kill();
    damaged.flip_bit(&dir.join("CURRENT"), 3, 1).unwrap();
    assert!(matches!(
        Store::open(&dir, options(&damaged, &bg)),
        Err(Error::Corruption {
            file: FileId::Current,
            ..
        })
    ));
    let manifest = files_under(&fs, &dir)
        .into_iter()
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("MANIFEST-")
        })
        .unwrap();
    let damaged = fs.kill();
    damaged.flip_bit(&manifest, 12, 5).unwrap();
    assert!(matches!(
        Store::open(&dir, options(&damaged, &bg)),
        Err(Error::Corruption {
            file: FileId::Manifest { .. },
            ..
        })
    ));
    // The undamaged image still opens.
    let s = open(&dir, options(&fs, &bg));
    assert_eq!(
        s.snapshot()
            .get(&group_key(1, 2))
            .unwrap()
            .unwrap()
            .as_ref(),
        val("m")
    );
}

/// The last record before a kill: synced records first, then one unsynced batch whose
/// group starts on a fresh 4 KiB block. Returns (image, segment, block offset).
fn image_with_unsynced_tail(bg: &Arc<Background>) -> (FaultFs, PathBuf, PathBuf, u64) {
    let (fs, dir) = fault_fs();
    let store = open(&dir, options(&fs, bg));
    for k in 0..5 {
        write_kv(
            &store,
            &group_prefix(1),
            &group_key(1, k),
            &val(&format!("ok{k}")),
            false,
            Durability::Sync,
        );
    }
    write_kv(
        &store,
        &group_prefix(1),
        &group_key(1, 99),
        &val("tail"),
        false,
        Durability::NoSync,
    );
    let image = fs.kill();
    drop(store);
    let (seg, off) = find_one(&image, &dir.join("journal"), b"<<tail>>");
    (image, dir, seg, off)
}

fn check_prefix(store: &Store) {
    let snap = store.snapshot();
    for k in 0..5 {
        assert_eq!(
            snap.get(&group_key(1, k)).unwrap().unwrap().as_ref(),
            val(&format!("ok{k}"))
        );
    }
    assert_eq!(snap.get(&group_key(1, 99)).unwrap(), None);
}

/// NS06 and NS25 at store level: a torn payload, a torn header and an all-zero header
/// of the final, unsynced record recover locally to an exact prefix with the right
/// counters, and a second open is identical.
#[test]
fn ns06_ns25_torn_tails_recover_to_an_exact_prefix() {
    let bg = Background::new(2).unwrap();
    for case in ["torn payload", "torn header", "zero header"] {
        let (image, dir, seg, off) = image_with_unsynced_tail(&bg);
        let block = off / 4096 * 4096;
        match case {
            "torn payload" => image.flip_bit(&seg, off + 60, 2).unwrap(),
            "torn header" => image.flip_bit(&seg, block + 9, 2).unwrap(),
            _ => overwrite(
                &image,
                &seg,
                block,
                &vec![0u8; (off + 200 - block) as usize],
            ),
        }
        let opened = Store::open(&dir, options(&image, &bg)).unwrap();
        assert!(opened.quarantined.is_empty(), "{case}");
        let truncated = case != "zero header";
        assert_eq!(
            opened.recovery.tail_truncations,
            u64::from(truncated),
            "{case}"
        );
        assert_eq!(opened.recovery.tail_bytes > 0, truncated, "{case}");
        assert_eq!(
            opened.store.stats().tail_truncations,
            u64::from(truncated),
            "{case}"
        );
        check_prefix(&opened.store);
        let seq = opened.store.visible_seq();
        drop(opened);
        let again = Store::open(&dir, options(&image.crash(), &bg)).unwrap();
        assert_eq!(again.recovery.tail_truncations, 0, "{case}: second open");
        assert_eq!(again.store.visible_seq(), seq, "{case}");
        check_prefix(&again.store);
    }
}
