//! LSM-1: stores whose journal record headers are tagged under a per-store key
//! (plans/native-storage-engine.md, section 2 "Journal format"). Every case runs on
//! `FaultFs`, a fault model.

#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::{FaultFs, Fs, OpenMode};
use nabedra_kv::journal::format::{HeaderCopy, SegmentHeader};
use nabedra_kv::{Background, Durability, Error, Options, Store, WriteBatch};

fn options(fs: &FaultFs, bg: &Arc<Background>) -> Options {
    small_options(Arc::new(fs.clone()), bg.clone())
}

fn open(dir: &Path, o: Options) -> Store {
    Store::open(dir, o).expect("open").store
}

/// Writes `n` synced batches tagged `tag`, alternating inline and journal values, and
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

/// Every file under `dir` with its contents.
fn files_of(fs: &FaultFs, dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
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

fn find_at(fs: &FaultFs, dir: &Path, needle: &[u8]) -> (PathBuf, u64) {
    let mut hits = Vec::new();
    for path in fs.list(dir).unwrap() {
        let Ok(data) = fs.contents(&path) else {
            continue;
        };
        for (i, w) in data.windows(needle.len()).enumerate() {
            if w == needle {
                hits.push((path.clone(), i as u64));
            }
        }
    }
    assert_eq!(hits.len(), 1, "{hits:?}");
    hits.pop().unwrap()
}

#[test]
fn lsm1_new_store_has_a_key_and_reopens_after_crashes() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg));
    write_some(&store, &mut model, "a", 40);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 40);
    drop(store);
    assert_eq!(
        fs.contents(&dir.join("FORMAT")).unwrap(),
        b"nabedra-kv 2\n".to_vec()
    );
    let key = fs.contents(&dir.join("JOURNAL_KEY")).unwrap();
    for image in [fs.crash(), fs.kill()] {
        let store = open(&dir, options(&image, &bg));
        check(&store, &model);
        drop(store);
        assert_eq!(image.contents(&dir.join("JOURNAL_KEY")).unwrap(), key);
    }
    // Two stores get different keys.
    let (other, odir) = fault_fs();
    drop(open(&odir, options(&other, &bg)));
    assert_ne!(other.contents(&odir.join("JOURNAL_KEY")).unwrap(), key);
}

/// LSM-10 and LSM-1: creation is crash-safe at every step. JOURNAL_KEY is durable before
/// FORMAT exists, so a crash never leaves a store with FORMAT and no key. A kill right
/// after the store directory is created used to leave its entry unsynced, and the next
/// open, seeing the directory, never synced it: its synced writes were lost to the next
/// power loss (LSM-10). The next open now creates or opens, durably.
#[test]
fn lsm10_creation_survives_a_crash_at_every_step() {
    let bg = Background::new(2).unwrap();
    for n in 1..60 {
        let (fs, dir) = fault_fs();
        fs.crash_after(fs.mutating_ops() + n);
        let created = Store::open(&dir, options(&fs, &bg)).is_ok() && !fs.is_crashed();
        for (_cut, image) in [("kill", fs.kill()), ("power", fs.crash())] {
            if image.contents(&dir.join("FORMAT")).is_ok() {
                assert!(
                    image.durable_contents(&dir.join("JOURNAL_KEY")).is_some(),
                    "step {n}: FORMAT without a durable key"
                );
            }
            let mut model = BTreeMap::new();
            let store = Store::open(&dir, options(&image, &bg))
                .unwrap_or_else(|e| panic!("step {n}: {e}"))
                .store;
            write_some(&store, &mut model, "c", 3);
            drop(store);
            let again = image.crash();
            check(&open(&dir, options(&again, &bg)), &model);
        }
        if created {
            return;
        }
    }
    panic!("creation did not finish in 60 steps");
}

/// A missing, damaged or foreign JOURNAL_KEY refuses the store before any file changes.
#[test]
fn lsm1_missing_damaged_or_foreign_key_refuses_before_changes() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg));
    write_some(&store, &mut model, "a", 10);
    drop(store);
    let (other, _) = fault_fs();
    drop(open(&dir, options(&other, &bg)));
    let foreign = other.contents(&dir.join("JOURNAL_KEY")).unwrap();

    type Edit = Box<dyn Fn(&FaultFs, &Path)>;
    let cases: Vec<(&str, Edit)> = vec![
        (
            "missing",
            Box::new(|fs: &FaultFs, k: &Path| fs.remove(k).unwrap()),
        ),
        (
            "damaged",
            Box::new(|fs: &FaultFs, k: &Path| fs.flip_bit(k, 12, 3).unwrap()),
        ),
        (
            "foreign",
            Box::new(move |fs: &FaultFs, k: &Path| {
                let f = fs.open(k, OpenMode::ReadWrite).unwrap();
                f.write_all_at(&foreign, 0).unwrap();
                f.sync_all().unwrap();
            }),
        ),
    ];
    for (name, edit) in cases {
        let image = fs.crash();
        edit(&image, &dir.join("JOURNAL_KEY"));
        let before = files_of(&image, &dir);
        let err = Store::open(&dir, options(&image, &bg)).err();
        assert!(
            matches!(err, Some(Error::Corruption { .. })),
            "{name}: {err:?}"
        );
        assert_eq!(
            files_of(&image, &dir),
            before,
            "{name}: refusal changed files"
        );
    }
}

/// A store of format 1 (CRC frames, before LSM-1) is refused, not migrated, and left
/// unchanged; so is one whose FORMAT says 2 but which holds a version 1 segment.
#[test]
fn lsm1_format1_store_is_refused_untouched() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    drop(open(&dir, options(&fs, &bg)));
    let image = fs.crash();
    let f = image
        .open(&dir.join("FORMAT"), OpenMode::ReadWrite)
        .unwrap();
    f.write_all_at(b"nabedra-kv 1\n", 0).unwrap();
    f.sync_all().unwrap();
    drop(f);
    let before = files_of(&image, &dir);
    let err = Store::open(&dir, options(&image, &bg)).err();
    assert!(
        matches!(err, Some(Error::UnsupportedFormat { .. })),
        "{err:?}"
    );
    assert_eq!(files_of(&image, &dir), before);

    let image = fs.crash();
    let seg = image
        .list(&dir.join("journal"))
        .unwrap()
        .into_iter()
        .find(|p| p.extension().is_some_and(|e| e == "jnl"))
        .unwrap();
    let bytes = image.contents(&seg).unwrap();
    let [HeaderCopy::Valid(h), _] = SegmentHeader::decode_blocks(&bytes[..8192]) else {
        panic!("segment header")
    };
    let mut block = h.encode_block();
    block[8..12].copy_from_slice(&1u32.to_le_bytes());
    let crc = nabedra_kv::crc::mask(nabedra_kv::crc::crc32c(&block[..40]));
    block[40..44].copy_from_slice(&crc.to_le_bytes());
    let f = image.open(&seg, OpenMode::ReadWrite).unwrap();
    f.write_all_at(&block, 0).unwrap();
    f.write_all_at(&block, 4096).unwrap();
    f.sync_all().unwrap();
    drop(f);
    let before = files_of(&image, &dir);
    let err = Store::open(&dir, options(&image, &bg)).err();
    assert!(
        matches!(err, Some(Error::UnsupportedFormat { .. })),
        "{err:?}"
    );
    assert_eq!(files_of(&image, &dir), before);
}

/// File-level backup and restore: a crash-consistent copy of a store (a volume snapshot)
/// opens with all synced writes and its key; a logical copy into a new store (what the
/// pool's archive restore does) gets its own key and reads the same.
#[test]
fn lsm1_backup_and_restore() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let mut model = BTreeMap::new();
    let store = open(&dir, options(&fs, &bg));
    write_some(&store, &mut model, "a", 30);
    store.flush().unwrap();
    write_some(&store, &mut model, "b", 30);
    let copy = fs.crash();
    let snap = store.snapshot();
    let (target, tdir) = fault_fs();
    let restored = open(&tdir, options(&target, &bg));
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
    drop(snap);
    drop(store);
    let restored = open(&dir, options(&copy, &bg));
    check(&restored, &model);
}

/// The case that the CRC frame left open after LSM-2 even with mitigation (b): silent
/// media damage zeroes the header of a flushed record (seq 1); a flushed journal value
/// (seq 2) that a client wrote holds, at a 4 KiB boundary, a forged batch for seq 3 with
/// a plausible `durable_seq`. Recovery may cross covered damage without evidence
/// (LSM-2), so on the CRC frame it would take the forged header as its resync point,
/// replay the forged store-wide batch and truncate the genuine, synced seq 3 as a torn
/// tail. The forged header has no valid tag: recovery resyncs at seq 3 and replays it.
#[test]
fn lsm1_media_damage_never_lets_a_forged_batch_replay() {
    let bg = Background::new(2).unwrap();
    let (fs, dir) = fault_fs();
    let store = open(&dir, options(&fs, &bg));
    let journal = dir.join("journal");
    let write = |store: &Store, key: &[u8], value: &[u8], jv: bool, d: Durability| {
        let mut b = WriteBatch::new();
        if jv {
            b.put_journal(key, value);
        } else {
            b.put(key, value);
        }
        store.submit(b, d, Box::new(())).unwrap().wait().unwrap();
    };
    write(&store, b"s/one", b"<<seq-one>>", false, Durability::NoSync);
    // seq 2: a journal value whose bytes the client chose. A dry layout run on another
    // store gives the offset of the next 4 KiB boundary inside the value.
    let mut value = b"<<client-value>>".to_vec();
    value.resize(9000, 0);
    let (probe_fs, probe_dir) = fault_fs();
    {
        let probe = open(&probe_dir, options(&probe_fs, &bg));
        write(&probe, b"s/one", b"<<seq-one>>", false, Durability::NoSync);
        write(&probe, b"s/two", &value, true, Durability::Sync);
    }
    let (seg, at) = find_at(&probe_fs, &probe_dir.join("journal"), b"<<client-value>>");
    let bytes = probe_fs.contents(&seg).unwrap();
    let [HeaderCopy::Valid(h), _] = SegmentHeader::decode_blocks(&bytes[..8192]) else {
        panic!("segment header")
    };
    let boundary = (at / 4096 + 1) * 4096;
    let mut batch = WriteBatch::new();
    batch.put(b"s/forged", b"x");
    let guess = nabedra_kv::mac::JournalKey::from_bytes(*b"a guessed key!!!");
    let mut forged = Vec::new();
    nabedra_kv::journal::format::encode_record(
        &mut forged,
        &nabedra_kv::journal::format::RecordFields {
            kind: nabedra_kv::journal::format::RecordType::Batch,
            flags: 0,
            epoch: h.epoch,
            first_seq: 3,
            count: 1,
            durable_seq: 2,
        },
        &[],
        nabedra_kv::journal::format::Payload::Bytes(batch.data()),
        &guess,
    );
    let off = (boundary - at) as usize;
    value[off..off + forged.len()].copy_from_slice(&forged);
    write(&store, b"s/two", &value, true, Durability::Sync);
    store.flush().unwrap();
    write(
        &store,
        b"s/three",
        b"<<seq-three-acked>>",
        false,
        Durability::Sync,
    );
    drop(store);
    let image = fs.crash();
    let (seg, one_at) = find_at(&image, &journal, b"<<seq-one>>");
    let f = image.open(&seg, OpenMode::ReadWrite).unwrap();
    f.write_all_at(&[0u8; 48], one_at / 4096 * 4096).unwrap();
    f.sync_all().unwrap();
    drop(f);
    let opened = Store::open(&dir, options(&image, &bg)).unwrap();
    let snap = opened.store.snapshot();
    assert!(
        snap.get(b"s/forged").unwrap().is_none(),
        "forged batch replayed"
    );
    assert_eq!(
        snap.get(b"s/three").unwrap().as_deref(),
        Some(&b"<<seq-three-acked>>"[..]),
        "the acknowledged seq 3 was lost"
    );
}
