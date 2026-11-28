//! VS10: a crash, a power loss or an I/O error at every step of a checkpoint, of a copy from the other
//! replica and of a drop, and damage of every file.

use std::sync::Arc;

use nabedra_rt::fs::{Fault, FaultFs, Fs, FsOp, OpenMode};

use super::*;

const DIR: &str = "/data/ck";
const OTHER: RangeId = RangeId(8);

fn store(fs: &FaultFs) -> Store {
    Store::new(Arc::new(fs.clone()), DIR)
}

fn disk() -> FaultFs {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(std::path::Path::new("/data"));
    fs
}

fn cfg() -> Config {
    Config {
        part_min: 2,
        part_max: 6,
        ..Config::new(schema(2, &[ColumnType::Tags], None), 2)
    }
}

/// The feed of a range: position `n` writes, moves or deletes one of eight keys.
fn change(range: RangeId, n: Pos) -> (u64, Option<Row>) {
    let k = (n * 5) % 8 + range.0 as u64 * 100;
    let angle = (n * 7) as f32;
    let tags = Value::Tags(Arc::new([Arc::from("t")]));
    let len = if n.is_multiple_of(9) {
        0.0
    } else {
        (n % 4 + 1) as f32
    };
    let write = !n.is_multiple_of(6);
    (
        k,
        write.then(|| row(&[angle.cos() * len, angle.sin() * len], vec![tags], n)),
    )
}

fn advance(ix: &mut Index, range: RangeId, to: Pos) {
    let after = ix.status(range).unwrap().delivered;
    let changes = (after + 1..=to).map(|n| change(range, n)).collect();
    feed(ix, range, &page(after, changes));
}

/// What a scan at `pos` has to return, by image.
fn visible(range: RangeId, pos: Pos) -> Vec<u64> {
    let last = |k: u64| (1..=pos).rev().find(|n| change(range, *n).0 == k);
    let keys = (0..8).map(|k| k + range.0 as u64 * 100);
    let mut all: Vec<u64> = keys
        .filter_map(last)
        .filter(|n| change(range, *n).1.is_some())
        .collect();
    all.sort_unstable();
    all
}

/// Every retained cut of a range answers like the feed.
fn check(ix: &mut Index, range: RangeId, why: &str) {
    let s = ix.status(range).unwrap();
    ix.caught_up(range, s.applied).unwrap();
    for pos in s.low..=s.applied {
        let pin = ix.pin(&[(range, ReadAt::Position(pos))], None).unwrap();
        let mut got = images(&ix.search(&pin, &query(Op::L2, &[0.3, 0.1], 100)).unwrap());
        got.sort_unstable();
        assert_eq!(got, visible(range, pos), "{why}: range {range:?} at {pos}");
    }
}

/// Recovers, checks, replays the rest of the feed and checks again.
fn recovers(fs: &FaultFs, positions: &[Pos], why: &str) -> Vec<Pos> {
    let (mut ix, damage) = Index::recover(cfg(), ExactCodec, store(fs)).unwrap();
    assert_eq!(damage, [], "{why}");
    let mut found = Vec::new();
    for range in [RANGE, OTHER] {
        let Ok(s) = ix.status(range) else {
            continue;
        };
        assert!(positions.contains(&s.applied), "{why}: at {}", s.applied);
        found.push(s.applied);
        assert!(!s.serving, "{why}: it serves after a fresh barrier only");
        check(&mut ix, range, why);
        advance(&mut ix, range, 40);
        settle(&mut ix);
        check(&mut ix, range, why);
    }
    found
}

/// An index with a checkpoint at 20, and feed up to 30.
fn prepared(fs: &FaultFs) -> Index {
    let mut ix = serving(cfg(), ExactCodec, Some(store(fs)));
    ix.begin_load(OTHER, 0, 0).unwrap();
    ix.finish_load(OTHER).unwrap();
    ix.caught_up(OTHER, 0).unwrap();
    for range in [RANGE, OTHER] {
        advance(&mut ix, range, 20);
        ix.advance_low(range, 12).unwrap();
    }
    settle(&mut ix);
    ix.checkpoint().unwrap();
    for range in [RANGE, OTHER] {
        advance(&mut ix, range, 30);
        ix.advance_low(range, 25).unwrap();
    }
    settle(&mut ix);
    ix
}

/// Every image that a crash may leave.
fn images_of(fs: &FaultFs) -> Vec<FaultFs> {
    let mut all = vec![fs.crash(), fs.kill(), fs.kill().crash()];
    all.extend((0..24).map(|seed| fs.power_loss(seed)));
    all
}

/// A crash at each step of a checkpoint leaves the old checkpoint or the new one, whole.
#[test]
fn vs10_a_crash_at_every_step_of_a_checkpoint() {
    let steps = {
        let fs = disk();
        let mut ix = prepared(&fs);
        let before = fs.mutating_ops();
        ix.checkpoint().unwrap();
        fs.mutating_ops() - before
    };
    let (mut old, mut new) = (0, 0);
    for step in 0..=steps {
        let fs = disk();
        let mut ix = prepared(&fs);
        fs.crash_after(fs.mutating_ops() + step);
        let result = ix.checkpoint();
        // Removing old files comes last, and it may fail.
        assert!(result.is_ok() || step < steps, "step {step}: {result:?}");
        assert!(
            result.is_err() || step + 2 >= steps,
            "step {step} of {steps}"
        );
        for after in images_of(&fs) {
            let found = recovers(&after, &[20, 30], &format!("step {step} of {steps}"));
            assert_eq!(
                found.len(),
                2,
                "step {step}: one of two checkpoints is there"
            );
            assert_eq!(
                found[0], found[1],
                "step {step}: both ranges are of one checkpoint"
            );
            assert!(
                result.is_err() || found[0] == 30,
                "an installed checkpoint is the newest"
            );
            if found[0] == 20 { old += 1 } else { new += 1 }
        }
    }
    assert!(old > 20 && new > 7, "{old} {new}");
}

/// An I/O error at each call fails the checkpoint. The installed one stays, and the next succeeds.
#[test]
fn vs10_an_io_error_at_every_step_of_a_checkpoint() {
    for op in [
        FsOp::Open,
        FsOp::Write,
        FsOp::SyncData,
        FsOp::SyncDir,
        FsOp::Rename,
        FsOp::CreateDirAll,
    ] {
        let mut failures = 0;
        for nth in 1..50 {
            let fs = disk();
            let mut ix = prepared(&fs);
            fs.inject(Fault::on(op).nth(nth));
            let result = ix.checkpoint();
            fs.clear_faults();
            let found = recovers(&fs.crash(), &[20, 30], &format!("{op:?} {nth}"));
            if result.is_ok() {
                assert_eq!(found, [30, 30]);
                break;
            }
            failures += 1;
            check(&mut ix, RANGE, "after a failed checkpoint");
            ix.checkpoint().unwrap();
            assert_eq!(recovers(&fs.crash(), &[30], "the next one"), [30, 30]);
        }
        assert!(failures > 0 && failures < 48, "{op:?} {failures}");
    }
}

/// A crash while a checkpoint is copied from the other replica leaves nothing or all of it.
#[test]
fn vs10_a_crash_at_every_step_of_a_copy_from_the_peer() {
    let peer = disk();
    prepared(&peer);
    let copy = |fs: &FaultFs| copy_checkpoint(&store(&peer), &store(fs));
    let whole = disk();
    copy(&whole).unwrap();
    let steps = whole.mutating_ops();
    for step in 0..=steps {
        let fs = disk();
        fs.crash_after(step);
        assert_eq!(copy(&fs).is_ok(), step == steps);
        for after in images_of(&fs) {
            let found = recovers(&after, &[20], &format!("copy step {step}"));
            assert!(found.is_empty() || found.len() == 2);
            assert!(step < steps || found.len() == 2);
        }
    }
    // Wiped disk without a peer.
    assert!(recovers(&disk(), &[], "wiped").is_empty());
}

/// A dropped range never comes back, whenever the crash is.
#[test]
fn vs10_a_crash_at_every_step_of_a_drop() {
    for step in 0..20 {
        let fs = disk();
        let mut ix = prepared(&fs);
        fs.crash_after(fs.mutating_ops() + step);
        let result = ix.drop_range(RANGE);
        assert_eq!(
            ix.status(RANGE).is_err(),
            result.is_ok(),
            "gone from memory after the disk"
        );
        for after in images_of(&fs) {
            let found = recovers(&after, &[20], &format!("drop step {step}"));
            assert!(!found.is_empty(), "the other range stays");
            assert!(
                result.is_err() || found.len() == 1,
                "a dropped range came back"
            );
        }
        if result.is_ok() {
            assert!(step > 0);
            return;
        }
    }
    panic!("a drop does not end");
}

/// Any damaged byte is found, takes the ranges in its file out of service with an explicit error, and
/// nothing else is served wrong. An install brings the range back.
#[test]
fn vs10_a_damaged_file_takes_its_ranges_out_of_service() {
    let fs = disk();
    let mut ix = prepared(&fs);
    ix.checkpoint().unwrap();
    let files = store(&fs).files();
    assert!(files.len() >= 2, "{files:?}");
    for path in files {
        let len = fs.contents(&path).unwrap().len() as u64;
        for at in (0..len).step_by(if len > 400 { 13 } else { 1 }) {
            let bad = fs.kill();
            bad.flip_bit(&path, at, (at % 8) as u8).unwrap();
            let (mut ix, damage) = Index::recover(cfg(), ExactCodec, store(&bad)).unwrap();
            assert_eq!(damage.len(), 1, "{path:?} byte {at}");
            let manifest = path.ends_with("MANIFEST");
            assert_eq!(damage[0].ranges.is_empty(), manifest);
            for range in [RANGE, OTHER] {
                let pin = ix
                    .pin(&[(range, ReadAt::Latest { barrier: 0 })], None)
                    .err()
                    .unwrap();
                match pin {
                    Error::NotHeld(_) => assert!(manifest),
                    Error::OutOfService { reason, .. } => {
                        assert!(reason.contains("seg-"), "{reason}");
                        assert!(damage[0].ranges.contains(&range));
                    }
                    e => panic!("{e}"),
                }
                assert!(ix.apply(range, &page(30, Vec::new())).is_err());
            }
            ix.begin_load(RANGE, 0, 0).unwrap();
            ix.finish_load(RANGE).unwrap();
            advance(&mut ix, RANGE, 12);
            check(&mut ix, RANGE, "installed again");
        }
    }
}

/// A truncated, missing or unreadable file is damage too.
#[test]
fn vs10_a_missing_truncated_or_unreadable_file_is_damage() {
    let fs = disk();
    prepared(&fs);
    let store_of = |fs: &FaultFs| store(fs);
    let files = store(&fs).files();
    let segment = files
        .iter()
        .find(|p| !p.ends_with("MANIFEST"))
        .unwrap()
        .clone();
    for case in 0..3 {
        let bad = fs.kill();
        match case {
            0 => bad.remove(&segment).unwrap(),
            1 => bad
                .open(&segment, OpenMode::ReadWrite)
                .unwrap()
                .set_len(20)
                .unwrap(),
            _ => bad.inject(Fault::on(FsOp::Read).path_prefix(segment.clone()).repeat()),
        }
        let (ix, damage) = Index::recover(cfg(), ExactCodec, store_of(&bad)).unwrap();
        assert_eq!(damage.len(), 1, "case {case}");
        assert!(ix.status(RANGE).unwrap().damage.is_some());
    }
}

/// Garbage of checkpoints that never finished goes with the next one.
#[test]
fn vs10_old_files_go_away() {
    let fs = disk();
    let mut ix = prepared(&fs);
    for _ in 0..3 {
        ix.begin_checkpoint().unwrap();
        ix.write_file().unwrap();
        ix.checkpoint = None;
    }
    ix.checkpoint().unwrap();
    let files = store(&fs).files();
    assert_eq!(files.len(), 2, "{files:?}");
}

/// Ranges in different files fail apart: a damaged file does not touch the others.
#[test]
fn vs10_damage_of_one_file_leaves_other_ranges_in_service() {
    let fs = disk();
    let cfg = || Config::new(schema(2, &[ColumnType::Text], Some(0)), 2);
    let mut ix = serving(cfg(), ExactCodec, Some(store(&fs)));
    ix.begin_load(OTHER, 0, 0).unwrap();
    ix.finish_load(OTHER).unwrap();
    // A tree, and so a partition, per range.
    for (range, tenant) in [(RANGE, "a"), (OTHER, "b")] {
        let write = |k| {
            (
                k,
                Some(row(&[k as f32, 1.0], vec![Value::Text(tenant.into())], k)),
            )
        };
        feed(&mut ix, range, &page(0, (1..9).map(write).collect()));
    }
    ix.begin_checkpoint().unwrap();
    while ix.write_file().unwrap() {
        ix.checkpoint.as_mut().unwrap().seal().unwrap();
    }
    ix.install_manifest().unwrap();
    let segments = store(&fs)
        .files()
        .into_iter()
        .filter(|p| !p.ends_with("MANIFEST"));
    for path in segments.collect::<Vec<_>>() {
        let bad = fs.crash();
        bad.flip_bit(&path, 30, 3).unwrap();
        let (mut ix, damage) = Index::recover(cfg(), ExactCodec, store(&bad)).unwrap();
        assert_eq!((damage.len(), damage[0].ranges.len()), (1, 1));
        for range in [RANGE, OTHER] {
            if damage[0].ranges.contains(&range) {
                assert!(ix.status(range).unwrap().damage.is_some());
                continue;
            }
            ix.caught_up(range, 8).unwrap();
            let pin = ix
                .pin(&[(range, ReadAt::Latest { barrier: 8 })], None)
                .unwrap();
            let hits = ix.search(&pin, &query(Op::Dot, &[-1.0, 0.0], 100)).unwrap();
            assert_eq!(images(&hits), (1..9).collect::<Vec<u64>>());
        }
    }
}

/// rv-idx3: the dense twin of finding 05. A peer copy into a directory whose local checkpoint has
/// written files but not published them must not clean those files away.
#[test]
fn rv_idx3_dense_peer_copy_cannot_delete_pending_local_segments() {
    let local = disk();
    let mut writer = prepared(&local);
    writer.begin_checkpoint().unwrap();
    while writer.write_file().unwrap() {}

    let peer = disk();
    prepared(&peer);
    assert_eq!(
        copy_checkpoint(&store(&peer), &store(&local)),
        Err(Error::Busy)
    );
    writer.install_manifest().unwrap();
    assert_eq!(recovers(&local.crash(), &[30], "local checkpoint"), [30, 30]);

    copy_checkpoint(&store(&peer), &store(&local)).unwrap();
    assert_eq!(recovers(&local.crash(), &[20], "peer copy retry"), [20, 20]);
}
