//! LSM-1: format 2 stores (authenticated journal frames) and the upgrade from format 1
//! (plans/native-storage-engine.md, section 2 "Journal format" and section 9; the
//! design is checked by specs/lsm/integrity/JournalIntegrity.tla). Every case runs on
//! `FaultFs`, a fault model.

#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::{FaultFs, Fs, OpenMode};
use nabedra_kv::journal::format::{HeaderCopy, SegmentHeader};
use nabedra_kv::{Background, Durability, Error, JournalIntegrity, Options, Store, WriteBatch};

const V1: &[u8] = b"nabedra-kv 1\n";
const V2: &[u8] = b"nabedra-kv 2\n";

fn options(fs: &FaultFs, bg: &Arc<Background>, integrity: JournalIntegrity) -> Options {
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.journal_integrity = integrity;
    o
}

fn open(dir: &Path, o: Options) -> Store {
    Store::open(dir, o).expect("open").store
}

/// Writes `n` batches tagged `tag`, synced, alternating inline and journal values, and
/// records them in `model`.
fn write_some(store: &Store, model: &mut BTreeMap<Vec<u8>, Vec<u8>>, tag: &str, n: usize) {
    for i in 0..n {
        let key = format!("g01/{tag}{i:03}").into_bytes();
        let mut value = format!("{tag}-{i}-").into_bytes();
        value.resize(300 + i * 7, b'v');
        let mut b = WriteBatch::new();
        b.set_scope(b"g01/");
        if i % 2 == 0 {
            b.put_journal(&key, &value);
        } else {
            b.put(&key, &value);
        }
        store
            .submit(b, Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
        model.insert(key, value);
    }
}

fn check(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let snap = store.snapshot();
    for (k, v) in model {
        let got = snap.get(k).unwrap_or_else(|e| panic!("{k:?}: {e}"));
        assert_eq!(
            got.as_deref(),
            Some(&v[..]),
            "{}",
            String::from_utf8_lossy(k)
        );
    }
}

fn format_of(fs: &FaultFs, dir: &Path) -> Option<Vec<u8>> {
    fs.contents(&dir.join("FORMAT")).ok()
}

/// `(file, auth)` for every journal file with a valid header copy.
fn segment_frames(fs: &FaultFs, dir: &Path) -> Vec<(PathBuf, bool)> {
    let mut out = Vec::new();
    let mut files = fs.list(&dir.join("journal")).unwrap_or_default();
    files.sort();
    for path in files {
        let bytes = fs.contents(&path).unwrap();
        let copies = SegmentHeader::decode_blocks(&bytes[..bytes.len().min(8192)]);
        if let Some(h) = copies
            .iter()
            .filter_map(|c| match c {
                HeaderCopy::Valid(h) => Some(*h),
                _ => None,
            })
            .max_by_key(|h| h.epoch)
        {
            out.push((path, h.auth));
        }
    }
    out
}

fn live_frames(fs: &FaultFs, dir: &Path) -> Vec<bool> {
    segment_frames(fs, dir)
        .into_iter()
        .filter(|(p, _)| p.extension().is_some_and(|e| e == "jnl"))
        .map(|(_, a)| a)
        .collect()
}

/// Every file under `dir` with its contents.
fn image(fs: &FaultFs, dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs.list(&d) else { continue };
        for p in entries {
            match fs.contents(&p) {
                Ok(c) => out.push((p, c)),
                Err(_) => stack.push(p),
            }
        }
    }
    out.sort();
    out
}

#[test]
fn lsm1_new_store_is_format2_and_reopens() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Authenticated));
    write_some(&store, &mut model, "a", 40);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 40);
    drop(store);
    assert_eq!(format_of(&fs, &dir).as_deref(), Some(V2));
    assert!(fs.contents(&dir.join("JOURNAL_KEY")).is_ok());
    let frames = live_frames(&fs, &dir);
    assert!(
        !frames.is_empty() && frames.iter().all(|&a| a),
        "{frames:?}"
    );

    // Crash (durable state only), reopen with either option: a format 2 store stays one.
    for integrity in [JournalIntegrity::Legacy, JournalIntegrity::Authenticated] {
        let image = fs.crash();
        let store = open(&dir, options(&image, &bg, integrity));
        check(&store, &model);
        drop(store);
        assert_eq!(format_of(&image, &dir).as_deref(), Some(V2));
    }
}

#[test]
fn lsm1_upgrade_keeps_data_and_writes_authenticated_segments() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Legacy));
    write_some(&store, &mut model, "a", 60);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 30);
    drop(store);
    assert_eq!(format_of(&fs, &dir).as_deref(), Some(V1));
    assert!(live_frames(&fs, &dir).iter().all(|&a| !a));

    let opened = Store::open(&dir, options(&fs, &bg, JournalIntegrity::Authenticated)).unwrap();
    assert!(opened.quarantined_now.is_empty());
    check(&opened.store, &model);
    write_some(&opened.store, &mut model, "c", 30);
    drop(opened);
    assert_eq!(format_of(&fs, &dir).as_deref(), Some(V2));
    let frames = live_frames(&fs, &dir);
    assert!(frames.last() == Some(&true), "{frames:?}");

    let image = fs.crash();
    let store = open(&dir, options(&image, &bg, JournalIntegrity::Legacy));
    check(&store, &model);
}

/// Partial upgrade: a kill or a power loss at every mutating filesystem operation of the
/// upgrading open. The next open (either option) opens with every acknowledged write; an
/// authenticated segment exists only where FORMAT is 2 and JOURNAL_KEY is there; a store
/// FORMAT still calls format 1 holds no authenticated segment, so a release that knows
/// only format 1 can open it (JournalIntegrity.tla: KeyCoversFrames, FormatHasKey).
#[test]
fn lsm1_upgrade_survives_a_crash_at_every_step() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Legacy));
    write_some(&store, &mut model, "a", 20);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 10);
    drop(store);
    let base = fs.crash();

    let mut steps = 0;
    for n in 1.. {
        let attempt = base.kill();
        let start = attempt.mutating_ops();
        attempt.crash_after(start + n);
        let result = Store::open(
            &dir,
            options(&attempt, &bg, JournalIntegrity::Authenticated),
        );
        let finished = !attempt.is_crashed();
        drop(result);
        for (cut, image) in [("kill", attempt.kill()), ("power", attempt.crash())] {
            let format = format_of(&image, &dir);
            let frames = live_frames(&image, &dir);
            if frames.iter().any(|&a| a) {
                assert_eq!(format.as_deref(), Some(V2), "step {n} {cut}: {frames:?}");
                assert!(image.durable_contents(&dir.join("JOURNAL_KEY")).is_some());
            }
            if format.as_deref() == Some(V2) {
                assert!(
                    image.contents(&dir.join("JOURNAL_KEY")).is_ok(),
                    "step {n} {cut}"
                );
            }
            for integrity in [JournalIntegrity::Legacy, JournalIntegrity::Authenticated] {
                let again = image.kill();
                let opened = Store::open(&dir, options(&again, &bg, integrity))
                    .unwrap_or_else(|e| panic!("step {n} {cut} {integrity:?}: {e}"));
                check(&opened.store, &model);
                drop(opened);
                if integrity == JournalIntegrity::Authenticated {
                    assert_eq!(format_of(&again, &dir).as_deref(), Some(V2), "step {n}");
                }
            }
        }
        steps = n;
        if finished {
            break;
        }
    }
    assert!(steps > 10, "the upgrade took {steps} steps");
}

/// A store the format 1 open refuses is not upgraded: FORMAT stays 1 and no key is
/// written (JournalIntegrity.tla: UpgradeValidated).
#[test]
fn lsm1_upgrade_needs_a_store_that_opens() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Legacy));
    write_some(&store, &mut model, "a", 10);
    store.flush().unwrap();
    drop(store);
    let image = fs.crash();
    // The manifest records the journal segment: without it the open refuses (R3).
    let journal = dir.join("journal");
    for p in image.list(&journal).unwrap() {
        if p.extension().is_some_and(|e| e == "jnl") {
            image.remove(&p).unwrap();
        }
    }
    let before = image_of(&image, &dir);
    assert!(Store::open(&dir, options(&image, &bg, JournalIntegrity::Authenticated)).is_err());
    assert_eq!(format_of(&image, &dir).as_deref(), Some(V1));
    assert!(image.contents(&dir.join("JOURNAL_KEY")).is_err());
    assert_eq!(
        image_of(&image, &dir),
        before,
        "the refused upgrade changed files"
    );
}

fn image_of(fs: &FaultFs, dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    image(fs, dir)
}

/// A format 2 store without its key, with a damaged key, or with another store's key
/// refuses to open before it changes any file.
#[test]
fn lsm1_missing_damaged_or_foreign_key_refuses_before_changes() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Authenticated));
    write_some(&store, &mut model, "a", 10);
    drop(store);
    let (other, _) = fault_fs();
    drop(open(
        &dir,
        options(&other, &bg, JournalIntegrity::Authenticated),
    ));
    let foreign = other.contents(&dir.join("JOURNAL_KEY")).unwrap();
    let key_path = dir.join("JOURNAL_KEY");

    type Edit = Box<dyn Fn(&FaultFs)>;
    let (k1, k2) = (dir.join("JOURNAL_KEY"), dir.join("JOURNAL_KEY"));
    let cases: Vec<(&str, Edit)> = vec![
        (
            "missing",
            Box::new(move |fs: &FaultFs| fs.remove(&k1).unwrap()),
        ),
        (
            "damaged",
            Box::new(move |fs: &FaultFs| fs.flip_bit(&k2, 12, 3).unwrap()),
        ),
        (
            "foreign",
            Box::new(move |fs: &FaultFs| {
                let f = fs.open(&key_path, OpenMode::ReadWrite).unwrap();
                f.write_all_at(&foreign, 0).unwrap();
                f.sync_all().unwrap();
            }),
        ),
    ];
    for (name, edit) in cases {
        let image = fs.crash();
        edit(&image);
        let before = image_of(&image, &dir);
        for integrity in [JournalIntegrity::Legacy, JournalIntegrity::Authenticated] {
            let err = Store::open(&dir, options(&image, &bg, integrity)).err();
            assert!(
                matches!(err, Some(Error::Corruption { .. })),
                "{name} {integrity:?}: {err:?}"
            );
        }
        assert_eq!(
            image_of(&image, &dir),
            before,
            "{name}: refusal changed files"
        );
    }
}

/// A FORMAT this build does not know refuses before any change, the way a release that
/// knows only format 1 refuses a format 2 store (rollback, plan section 9).
#[test]
fn lsm1_unknown_format_refuses_before_changes() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    drop(open(
        &dir,
        options(&fs, &bg, JournalIntegrity::Authenticated),
    ));
    let image = fs.crash();
    let f = image
        .open(&dir.join("FORMAT"), OpenMode::ReadWrite)
        .unwrap();
    f.write_all_at(b"nabedra-kv 3\n", 0).unwrap();
    f.sync_all().unwrap();
    drop(f);
    let before = image_of(&image, &dir);
    let err = Store::open(&dir, options(&image, &bg, JournalIntegrity::Authenticated)).err();
    assert!(
        matches!(err, Some(Error::UnsupportedFormat { .. })),
        "{err:?}"
    );
    assert_eq!(image_of(&image, &dir), before);
    // The format 1 check is byte equality with "nabedra-kv 1\n": format 2 never passes it.
    assert_ne!(V2, V1);
}

/// File-level backup and restore: a crash-consistent copy of a format 2 store (a volume
/// snapshot) opens with all synced writes; a logical copy into a new store (what the
/// pool's archive restore does) works into either format.
#[test]
fn lsm1_backup_and_restore() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg, JournalIntegrity::Authenticated));
    write_some(&store, &mut model, "a", 30);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 30);
    // The snapshot is taken while the store runs.
    let copy = fs.crash();
    let snap = store.snapshot();
    for integrity in [JournalIntegrity::Legacy, JournalIntegrity::Authenticated] {
        let (target, tdir) = fault_fs();
        let restored = open(&tdir, options(&target, &bg, integrity));
        for (k, v) in &model {
            assert_eq!(snap.get(k).unwrap().as_deref(), Some(&v[..]));
            let mut b = WriteBatch::new();
            b.put(k, v);
            drop(
                restored
                    .submit(b, Durability::NoSync, Box::new(()))
                    .unwrap(),
            );
        }
        restored.sync().unwrap().wait().unwrap();
        check(&restored, &model);
    }
    drop(snap);
    drop(store);
    let restored = open(&dir, options(&copy, &bg, JournalIntegrity::Legacy));
    check(&restored, &model);
}
