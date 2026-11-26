//! Adversarial tests of task G, checkpoints: a crash at every step on a workload with updates,
//! deletes and a moving oldest cut, every bit flip and every truncation of every file, a manifest of
//! another analyzer, hostile files with valid checksums, resume, and the copy from a peer.

use std::path::Path;

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::fs::{FaultFs, Fs, OpenMode};

use super::adv_state::*;
use super::*;

const DIR: &str = "/data/impact";

fn store(fs: &FaultFs) -> Checkpoints {
    Checkpoints::new(Arc::new(fs.clone()), DIR)
}

fn disk() -> FaultFs {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/data"));
    fs
}

fn cfg() -> Config {
    Config {
        segment_rows: 4,
        merge_factor: 2,
        ..sparse_cfg()
    }
}

/// Crc32c, written here from the definition, to forge files with valid checksums.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in data {
        crc ^= u32::from(*b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn churn(ix: &mut ImpactIndex, m: &mut RM, g: &mut Gen, v: &mut u64, pages: u64) {
    for _ in 0..pages {
        let mut changes: BTreeMap<u64, Option<u64>> = BTreeMap::new();
        for _ in 0..1 + g.below(3) {
            *v += 1;
            changes.insert(g.below(12), if g.chance(1, 4) { None } else { Some(*v) });
        }
        let page = m.page(changes.into_iter().collect(), g.bool());
        ix.apply(RANGE, &page).unwrap();
        if g.chance(1, 6) {
            let page = m.close();
            ix.apply(RANGE, &page).unwrap();
        }
        if g.bool() {
            let s = ix.status(RANGE).unwrap();
            ix.advance_low(RANGE, s.low + g.below(s.applied - s.low + 1))
                .unwrap();
        }
        if g.chance(1, 3) {
            ix.maintain().unwrap();
        }
    }
}

type Status = (Pos, Pos, CommitTs, CommitTs);

fn status(ix: &ImpactIndex) -> Status {
    let s = ix.status(RANGE).unwrap();
    (s.applied, s.low, s.bound, s.closed)
}

/// An index with a checkpoint (old), then more feed. Returns the model and the status at the
/// checkpoint.
fn prepare(fs: &FaultFs, seed: u64) -> (ImpactIndex, RM, Status) {
    let mut g = Gen::from_seed(seed);
    let mut ix = ImpactIndex::new(cfg(), RamStore::default(), Some(store(fs))).unwrap();
    bring_up(&mut ix, RANGE, 0);
    let (mut m, mut v) = (RM::new(), 0);
    let n = 5 + g.below(10);
    churn(&mut ix, &mut m, &mut g, &mut v, n);
    settle(&mut ix);
    ix.checkpoint().unwrap();
    let old = status(&ix);
    let n = 5 + g.below(10);
    churn(&mut ix, &mut m, &mut g, &mut v, n);
    settle(&mut ix);
    (ix, m, old)
}

/// Every retained cut of the recovered range equals the model, the oldest servable one is exact
/// and older ones are refused.
fn verify(
    ix: &mut ImpactIndex,
    m: &RM,
    boundaries: bool,
    why: &str,
) -> std::result::Result<(), String> {
    let s = ix.status(RANGE).map_err(|e| format!("{why}: {e}"))?;
    ix.caught_up(RANGE, s.applied)
        .map_err(|e| format!("{why}: {e}"))?;
    for pos in s.low..=s.applied {
        let pin = ix
            .pin(&[(RANGE, ReadAt::Position(pos))])
            .map_err(|e| format!("{why}: pin {pos}: {e}"))?;
        for exact in [false, true] {
            let got = shown(ix, &pin, exact).map_err(|e| format!("{why}: at {pos}: {e}"))?;
            same(&m.visible(pos, CommitTs::MAX), &got, &[(RANGE, 0)])
                .map_err(|e| format!("{why}: at {pos}: {e}"))?;
        }
    }
    if boundaries {
        if s.low > 0
            && ix.pin(&[(RANGE, ReadAt::Position(s.low - 1))]).err()
                != Some(Error::CutTooOld { oldest: s.low })
        {
            return Err(format!("{why}: a cut below low {} is served", s.low));
        }
        if s.bound > 0
            && ix.pin(&[(RANGE, ReadAt::Cut(s.bound - 1))]).err()
                != Some(Error::CutTooOld { oldest: s.bound })
        {
            return Err(format!(
                "{why}: a cut below the bound {} is served",
                s.bound
            ));
        }
        if s.closed >= s.bound && s.closed > 0 {
            for ts in [s.bound, s.closed] {
                let pin = ix
                    .pin(&[(RANGE, ReadAt::Cut(ts))])
                    .map_err(|e| format!("{why}: cut {ts}: {e}"))?;
                let got = shown(ix, &pin, false).map_err(|e| e.to_string())?;
                same(&m.visible(s.applied, ts), &got, &[(RANGE, 0)])
                    .map_err(|e| format!("{why}: cut {ts}: {e}"))?;
            }
        }
    }
    Ok(())
}

/// The feed from `after`, as a page up to the end of the model.
fn replay(m: &RM, after: Pos) -> Page {
    let records = m
        .ops
        .iter()
        .filter(|o| o.0 > after)
        .map(|(at, ts, k, v)| Record {
            at: *at,
            change: Change::Row {
                key: key(*k),
                commit_ts: *ts,
                row: v.map(vrow),
            },
        })
        .collect();
    Page {
        after,
        through: m.next,
        records,
    }
}

fn recovers(fs: &FaultFs, m: &RM, ok: &[Status], why: &str) -> std::result::Result<Status, String> {
    let (mut ix, damage) =
        ImpactIndex::recover(cfg(), RamStore::default(), store(fs)).map_err(|e| e.to_string())?;
    if !damage.is_empty() {
        return Err(format!("{why}: damage {damage:?}"));
    }
    let st = status(&ix);
    if !ok.contains(&st) {
        return Err(format!("{why}: status {st:?} is none of {ok:?}"));
    }
    if ix.status(RANGE).unwrap().serving {
        return Err(format!("{why}: serves before a fresh barrier"));
    }
    verify(&mut ix, m, true, why)?;
    // Resume: a page that starts at the oldest cut repeats what is applied and brings the rest. A
    // page that skips ahead is refused.
    let s = ix.status(RANGE).unwrap();
    if s.applied < m.next
        && ix
            .apply(
                RANGE,
                &Page {
                    after: s.applied + 1,
                    through: s.applied + 2,
                    records: vec![],
                },
            )
            .err()
            != Some(Error::Gap {
                delivered: s.applied,
            })
    {
        return Err(format!("{why}: a gap is accepted"));
    }
    ix.apply(RANGE, &replay(m, s.low))
        .map_err(|e| format!("{why}: resume: {e}"))?;
    settle(&mut ix);
    ix.advance_low(RANGE, s.low).map_err(|e| e.to_string())?;
    verify(&mut ix, m, false, &format!("{why} resumed"))?;
    Ok(st)
}

#[test]
fn vs10_adv_a_crash_at_every_step_of_a_checkpoint_with_deletes_and_a_moving_low() {
    for seed in 0..6u64 {
        let steps = {
            let fs = disk();
            let (mut ix, ..) = prepare(&fs, seed);
            let before = fs.mutating_ops();
            ix.checkpoint().unwrap();
            fs.mutating_ops() - before
        };
        let (mut old_seen, mut new_seen) = (0, 0);
        for step in 0..=steps {
            let fs = disk();
            let (mut ix, m, old) = prepare(&fs, seed);
            // The status the new checkpoint would record is the live one now.
            let new = status(&ix);
            fs.crash_after(fs.mutating_ops() + step);
            let _ = ix.checkpoint();
            for image in [fs.crash(), fs.power_loss(step), fs.kill()] {
                let got = recovers(&image, &m, &[old, new], &format!("seed {seed} step {step}"))
                    .unwrap_or_else(|e| panic!("{e}"));
                old_seen += usize::from(got == old);
                new_seen += usize::from(got == new && got != old);
            }
        }
        assert!(
            old_seen > 0 && new_seen > 0,
            "seed {seed}: old {old_seen}, new {new_seen}"
        );
    }
}

#[test]
fn vs10_adv_the_oldest_cut_is_served_exactly_after_a_whole_checkpoint() {
    check_with(
        Runs {
            cases: 25,
            ..Runs::default()
        },
        "vs10_adv_the_oldest_cut_is_served_exactly_after_a_whole_checkpoint",
        |g| {
            let fs = disk();
            let (mut ix, m, _) = prepare(&fs, g.u64());
            // Move the oldest cut to the edge of some change, then checkpoint.
            let s = ix.status(RANGE).unwrap();
            let mid = s.low + g.below(s.applied - s.low + 1);
            let edge = *g.pick(&[s.low, s.applied, mid]);
            ix.advance_low(RANGE, edge).map_err(|e| e.to_string())?;
            settle(&mut ix);
            ix.checkpoint().map_err(|e| e.to_string())?;
            let now = status(&ix);
            recovers(&fs, &m, &[now], "whole")?;
            recovers(&fs.crash(), &m, &[now], "whole, after a crash")?;
            Ok(())
        },
    );
}

fn files(fs: &FaultFs) -> Vec<std::path::PathBuf> {
    store(fs).files()
}

fn doc_files(fs: &FaultFs) -> Vec<std::path::PathBuf> {
    files(fs)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains("doc-"))
        .collect()
}

fn manifest() -> std::path::PathBuf {
    Path::new(DIR).join("MANIFEST")
}

/// A recovery that reports the damage and does not serve what it covers, or finds no checkpoint.
fn assert_detected(image: &FaultFs, what: &str, ranges_out: bool) {
    let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(image))
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    assert!(!damage.is_empty(), "{what}: not detected");
    match ix.status(RANGE) {
        Ok(s) => assert!(
            ranges_out && s.damage.is_some() && !s.serving,
            "{what}: the range is served"
        ),
        Err(_) => assert!(
            !ranges_out || damage.iter().any(|d| d.ranges.is_empty()),
            "{what}"
        ),
    }
}

#[test]
fn vs10_adv_every_bit_flip_in_every_file_is_detected() {
    let fs = disk();
    let (mut ix, ..) = prepare(&fs, 3);
    ix.checkpoint().unwrap();
    let mut all = doc_files(&fs);
    all.push(manifest());
    assert!(all.len() >= 2);
    let mut flips = 0;
    for path in &all {
        let len = fs.contents(path).unwrap().len() as u64;
        for at in 0..len {
            // Every byte at a bit that varies with it, and the first bytes at every bit.
            let bits: Vec<u8> = if at < 24 {
                (0..8).collect()
            } else {
                vec![(at % 8) as u8]
            };
            for bit in bits {
                let image = fs.kill();
                image.flip_bit(path, at, bit).unwrap();
                assert_detected(
                    &image,
                    &format!("{path:?} byte {at} bit {bit}"),
                    path != &manifest(),
                );
                flips += 1;
            }
        }
    }
    assert!(flips > 500);
}

#[test]
fn vs10_adv_truncated_extended_and_missing_files_are_detected() {
    let fs = disk();
    let (mut ix, ..) = prepare(&fs, 4);
    ix.checkpoint().unwrap();
    for path in doc_files(&fs).into_iter().chain([manifest()]) {
        let len = fs.contents(&path).unwrap().len() as u64;
        let is_manifest = path == manifest();
        for n in 0..len {
            let image = fs.kill();
            image
                .open(&path, OpenMode::ReadWrite)
                .unwrap()
                .set_len(n)
                .unwrap();
            assert_detected(&image, &format!("{path:?} cut to {n}"), !is_manifest);
        }
        for extra in [1usize, 7, 4096] {
            let image = fs.kill();
            image
                .open(&path, OpenMode::ReadWrite)
                .unwrap()
                .write_all_at(&vec![0xA5; extra], len)
                .unwrap();
            assert_detected(&image, &format!("{path:?} plus {extra}"), !is_manifest);
        }
        if !is_manifest {
            let image = fs.kill();
            image.remove(&path).unwrap();
            assert_detected(&image, &format!("{path:?} missing"), true);
        }
    }
    // No manifest is no checkpoint, with or without files.
    let image = fs.kill();
    image.remove(&manifest()).unwrap();
    let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
    assert!(damage.is_empty() && ix.status(RANGE).is_err());
}

#[test]
#[ignore = "open bug VECTOR-impact-checkpoint-number-overflow"]
fn vs10_adv_stray_files_are_ignored_and_never_break_the_next_checkpoint() {
    let fs = disk();
    let (mut ix, m, _) = prepare(&fs, 5);
    ix.checkpoint().unwrap();
    for name in [
        "doc-zz",
        "doc-",
        "doc--1",
        "MANIFEST.tmp",
        "other",
        "doc-0000000000000099",
        "doc-ffffffffffffffff",
    ] {
        let f = fs
            .open(&Path::new(DIR).join(name), OpenMode::CreateNew)
            .unwrap();
        f.write_all_at(b"junk", 0).unwrap();
        f.sync_data().unwrap();
    }
    let (mut back, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
    assert_eq!(damage, []);
    verify(&mut back, &m, true, "stray files").unwrap();
    // The next checkpoint takes a file number above every name in the directory, and cleans up.
    back.checkpoint().unwrap();
    assert!(
        files(&fs)
            .iter()
            .all(|p| !p.to_string_lossy().contains("doc-ffff"))
            || doc_files(&fs).len() <= 4
    );
    let (mut again, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
    assert_eq!(damage, []);
    verify(&mut again, &m, true, "after the next checkpoint").unwrap();
}

#[test]
fn vs10_adv_leftovers_of_a_crashed_checkpoint_are_cleaned_by_the_next_one() {
    for step in 0..40u64 {
        let fs = disk();
        let (mut ix, ..) = prepare(&fs, 6);
        fs.crash_after(fs.mutating_ops() + step);
        let _ = ix.checkpoint();
        let image = fs.kill();
        let (mut back, damage) =
            ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
        assert_eq!(damage, [], "step {step}");
        if back.status(RANGE).is_err() {
            continue;
        }
        back.checkpoint().unwrap();
        assert_eq!(
            doc_files(&image).len(),
            1,
            "step {step}: {:?}",
            files(&image)
        );
        assert!(
            !files(&image)
                .iter()
                .any(|p| p.to_string_lossy().ends_with(".tmp")),
            "step {step}"
        );
    }
}

fn text_checkpoint(language: &str, cols: Vec<ColumnType>) -> (FaultFs, Config) {
    let fs = disk();
    let cfg = Config {
        segment_rows: 2,
        ..Config::new(Kind::Text(language.into()), cols.clone())
    };
    let mut ix = ImpactIndex::new(cfg.clone(), RamStore::default(), Some(store(&fs))).unwrap();
    bring_up(&mut ix, RANGE, 0);
    let mut m = Model::default();
    let col = |c: &ColumnType| match c {
        ColumnType::Int => Value::Int(1),
        ColumnType::Bool => Value::Bool(true),
        ColumnType::Float => Value::Float(1.5),
        ColumnType::Text => Value::Text("x".into()),
        ColumnType::Tags => tags(&["t"]),
    };
    let cs: Vec<Value> = cols.iter().map(col).collect();
    let rows = (0..4)
        .map(|k| (k, Some(text("the running dogs", cs.clone(), k))))
        .collect();
    ix.apply(RANGE, &m.page(rows, false)).unwrap();
    ix.checkpoint().unwrap();
    (fs, cfg)
}

#[test]
fn vs10_adv_a_checkpoint_loads_under_its_own_analyzer_and_columns_only() {
    let tags_only = vec![ColumnType::Tags];
    for a in crate::text::LANGUAGES {
        let (fs, _) = text_checkpoint(a, tags_only.clone());
        for b in crate::text::LANGUAGES {
            let other = Config::new(Kind::Text((*b).into()), tags_only.clone());
            let (ix, damage) =
                ImpactIndex::recover(other, RamStore::default(), store(&fs)).unwrap();
            if a == b {
                assert_eq!(damage, [], "{a}");
                assert!(ix.status(RANGE).is_ok());
            } else {
                assert_eq!(damage.len(), 1, "{a} checkpoint read as {b}");
                assert!(ix.status(RANGE).is_err());
            }
        }
        let sparse = Config::new(Kind::Sparse, tags_only.clone());
        assert_eq!(
            ImpactIndex::recover(sparse, RamStore::default(), store(&fs))
                .unwrap()
                .1
                .len(),
            1
        );
    }
    let (fs, _) = text_checkpoint("english", vec![ColumnType::Tags, ColumnType::Int]);
    for cols in [
        vec![ColumnType::Tags],
        vec![ColumnType::Int, ColumnType::Tags],
        vec![ColumnType::Tags, ColumnType::Int, ColumnType::Int],
        vec![ColumnType::Tags, ColumnType::Float],
        vec![],
    ] {
        let other = Config::new(Kind::Text("english".into()), cols.clone());
        assert_eq!(
            ImpactIndex::recover(other, RamStore::default(), store(&fs))
                .unwrap()
                .1
                .len(),
            1,
            "{cols:?}"
        );
    }
    // Other segment sizes and merge settings are not part of the format.
    let same = Config {
        segment_rows: 7,
        merge_factor: 5,
        max_segment_rows: 99,
        ..Config::new(
            Kind::Text("english".into()),
            vec![ColumnType::Tags, ColumnType::Int],
        )
    };
    let (ix, damage) = ImpactIndex::recover(same, RamStore::default(), store(&fs)).unwrap();
    assert_eq!(damage, []);
    assert!(ix.status(RANGE).is_ok());
}

/// Rewrites a file's bytes in place.
fn put(fs: &FaultFs, path: &Path, bytes: &[u8]) {
    let f = fs.open(path, OpenMode::ReadWrite).unwrap();
    f.set_len(bytes.len() as u64).unwrap();
    f.write_all_at(bytes, 0).unwrap();
}

#[test]
fn vs10_adv_a_forged_manifest_of_another_analyzer_version_is_not_loaded() {
    let (fs, cfg) = text_checkpoint("english", vec![ColumnType::Tags]);
    let mut bytes = fs.contents(&manifest()).unwrap();
    let needle = b"text/english/1";
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("the manifest names its analyzer");
    // Same checksum is valid for the unchanged file, and for an edited one only once recomputed.
    let n = bytes.len() - 4;
    assert_eq!(crc32c(&bytes[..n]).to_le_bytes(), bytes[n..]);
    bytes[at + needle.len() - 1] = b'2';
    let crc = crc32c(&bytes[..n]).to_le_bytes();
    bytes[n..].copy_from_slice(&crc);
    let image = fs.kill();
    put(&image, &manifest(), &bytes);
    let (ix, damage) = ImpactIndex::recover(cfg, RamStore::default(), store(&image)).unwrap();
    assert_eq!(damage.len(), 1);
    assert!(ix.status(RANGE).is_err());
}

#[test]
fn vs10_adv_hostile_files_with_valid_checksums_never_panic() {
    let fs = disk();
    let (mut ix, m, _) = prepare(&fs, 7);
    ix.checkpoint().unwrap();
    let man = fs.contents(&manifest()).unwrap();
    let docs = doc_files(&fs);
    let bodies: Vec<Vec<u8>> = docs.iter().map(|p| fs.contents(p).unwrap()).collect();
    check_with(
        Runs {
            cases: 1500,
            ..Runs::default()
        },
        "vs10_adv_hostile_files_with_valid_checksums_never_panic",
        |g| {
            let image = fs.kill();
            if g.bool() {
                let mut b = man.clone();
                let n = b.len() - 4;
                for _ in 0..1 + g.below(3) {
                    let at = if g.bool() {
                        g.below(n as u64) as usize
                    } else {
                        8 + g.below(24.min(n as u64 - 8)) as usize
                    };
                    b[at] = match g.below(4) {
                        0 => 0xFF,
                        1 => 0,
                        2 => b[at].wrapping_add(1),
                        _ => g.u64() as u8,
                    };
                }
                let crc = crc32c(&b[..n]).to_le_bytes();
                b[n..].copy_from_slice(&crc);
                put(&image, &manifest(), &b);
            } else {
                let i = g.below(docs.len() as u64) as usize;
                let mut b = bodies[i].clone();
                // The frames of the file: edit one frame's payload and fix its checksum.
                let mut frames = Vec::new();
                let mut at = 8usize;
                while at + 8 <= b.len() {
                    let len = u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as usize;
                    frames.push((at, len));
                    at += 8 + len;
                }
                let (start, len) = *g.pick(&frames);
                for _ in 0..1 + g.below(3) {
                    let o = start + 8 + g.below(len as u64) as usize;
                    b[o] = match g.below(4) {
                        0 => 0xFF,
                        1 => 0,
                        2 => b[o].wrapping_add(1),
                        _ => g.u64() as u8,
                    };
                }
                let crc = crc32c(&b[start + 8..start + 8 + len]).to_le_bytes();
                b[start + 4..start + 8].copy_from_slice(&crc);
                put(&image, &docs[i], &b);
            }
            let t = std::time::Instant::now();
            let (mut back, _damage) =
                ImpactIndex::recover(cfg(), RamStore::default(), store(&image))
                    .map_err(|e| e.to_string())?;
            if let Ok(s) = back.status(RANGE)
                && s.damage.is_none()
            {
                // Served or not, reading it never panics: pins, searches, a replay of the feed.
                let _ = back.caught_up(RANGE, s.applied);
                for pos in s.low..=s.applied.min(s.low + 40) {
                    if let Ok(pin) = back.pin(&[(RANGE, ReadAt::Position(pos))]) {
                        let _ = shown(&back, &pin, false);
                        let _ = shown(&back, &pin, true);
                    }
                }
                let _ = back.apply(RANGE, &replay(&m, s.low));
                settle(&mut back);
                let _ = back.checkpoint();
            }
            if t.elapsed().as_secs() > 5 {
                return Err("recovery took more than 5 s".into());
            }
            Ok(())
        },
    );
}

#[test]
fn vs10_adv_a_peer_copy_is_whole_or_detected_after_a_crash_at_every_step() {
    let src = disk();
    let (mut a, m, _) = prepare(&src, 8);
    a.checkpoint().unwrap();
    let new = status(&a);
    let steps = {
        let dst = disk();
        let before = dst.mutating_ops();
        copy_checkpoint(&store(&src), &store(&dst)).unwrap();
        dst.mutating_ops() - before
    };
    // An empty destination, and one with an older checkpoint of its own.
    for older in [false, true] {
        for step in 0..=steps {
            let dst = disk();
            let mut own = None;
            if older {
                let (mut b, m9, o9) = prepare(&dst, 9);
                b.checkpoint().unwrap();
                own = Some((m9, [o9, status(&b)]));
            }
            dst.crash_after(dst.mutating_ops() + step);
            let _ = copy_checkpoint(&store(&src), &store(&dst));
            for image in [dst.crash(), dst.kill()] {
                let (mut ix, damage) =
                    ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
                if !damage.is_empty() || ix.status(RANGE).is_err() {
                    continue;
                }
                let st = status(&ix);
                let why = format!("older {older} copy step {step}");
                if st == new {
                    verify(&mut ix, &m, true, &why).unwrap_or_else(|e| panic!("{e}"));
                } else {
                    let (m9, sts) = own
                        .as_ref()
                        .unwrap_or_else(|| panic!("{why}: unknown status {st:?}"));
                    assert!(sts.contains(&st), "{why}: unknown status {st:?}");
                    verify(&mut ix, m9, true, &why).unwrap_or_else(|e| panic!("{e}"));
                }
            }
        }
    }
}

#[test]
fn vs10_adv_a_checkpoint_without_rows_is_still_of_its_columns_only() {
    let fs = disk();
    let cfg = Config::new(Kind::Text("english".into()), vec![ColumnType::Tags]);
    let mut ix = ImpactIndex::new(cfg.clone(), RamStore::default(), Some(store(&fs))).unwrap();
    bring_up(&mut ix, RANGE, 3);
    ix.checkpoint().unwrap();
    let (ok, damage) = ImpactIndex::recover(cfg, RamStore::default(), store(&fs)).unwrap();
    assert_eq!((damage.len(), ok.status(RANGE).unwrap().applied), (0, 3));
    for cols in [
        vec![],
        vec![ColumnType::Int],
        vec![ColumnType::Tags, ColumnType::Tags],
    ] {
        let other = Config::new(Kind::Text("english".into()), cols.clone());
        assert_eq!(
            ImpactIndex::recover(other, RamStore::default(), store(&fs))
                .unwrap()
                .1
                .len(),
            1,
            "{cols:?}"
        );
    }
}

#[test]
fn vs14_adv_a_dropped_range_is_forgotten_on_disk_before_the_drop_returns() {
    let fs = disk();
    let (mut ix, m, _) = prepare(&fs, 11);
    let other = RangeId(8);
    bring_up(&mut ix, other, 0);
    ix.checkpoint().unwrap();
    ix.drop_range(other).unwrap();
    for image in [fs.crash(), fs.kill()] {
        let (mut back, damage) =
            ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
        assert_eq!(damage, []);
        assert!(back.status(other).is_err(), "a dropped range came back");
        verify(&mut back, &m, true, "the other range").unwrap();
    }
}
