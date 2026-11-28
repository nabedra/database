//! VS10 for impact data: a crash, a power loss or an I/O error at every step of a checkpoint, damage
//! of every kind of file, a copy from the other replica, a drop, and merges during a checkpoint.

use std::path::Path;

use nabedra_rt::fs::{Fault, FaultFs, Fs, FsOp};

use super::*;

const DIR: &str = "/data/impact";
const OTHER: RangeId = RangeId(8);

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
        ..text_cfg()
    }
}

/// Position `n` of a range writes, rewrites or deletes one of eight keys.
fn change(range: RangeId, n: Pos) -> (u64, Option<Row>) {
    let k = (n * 5) % 8 + range.0 as u64 * 100;
    let words = format!(
        "w{} w{} common {}",
        n % 5,
        n % 3,
        if n.is_multiple_of(4) { "" } else { "rare" }
    );
    let write = !n.is_multiple_of(6);
    (
        k,
        write.then(|| text(&words, vec![tags(&["t"])], n + range.0 as u64 * 1000)),
    )
}

fn advance(ix: &mut ImpactIndex, range: RangeId, to: Pos) {
    let after = ix.status(range).unwrap().applied;
    let records = (after + 1..=to)
        .map(|n| {
            let (k, row) = change(range, n);
            let change = Change::Row {
                key: key(k),
                commit_ts: n * 10,
                row,
            };
            Record { at: n, change }
        })
        .collect();
    ix.apply(
        range,
        &Page {
            after,
            through: to,
            records,
        },
    )
    .unwrap();
}

/// The rows of a range at a position, by brute force.
fn visible(range: RangeId, pos: Pos) -> BTreeMap<u64, Row> {
    let mut rows = BTreeMap::new();
    for n in 1..=pos {
        let (k, row) = change(range, n);
        match row {
            Some(r) => rows.insert(k, r),
            None => rows.remove(&k),
        };
    }
    rows
}

/// Every retained cut of a range answers like brute force.
fn check(ix: &mut ImpactIndex, range: RangeId, why: &str) {
    let s = ix.status(range).unwrap();
    ix.caught_up(range, s.applied).unwrap();
    for pos in s.low..=s.applied {
        let pin = ix.pin(&[(range, ReadAt::Position(pos))]).unwrap();
        for probe in ["w1 common", "rare w4", "w0"] {
            let rows = visible(range, pos);
            let stats = stats_of(
                &crate::text::Analyzer::new("english").unwrap(),
                &rows,
                probe,
            );
            let q = Query {
                stats: Some(stats.clone()),
                ..match_query(probe, 100)
            };
            let got = keys_and_scores(&ix.search(&pin, &q).unwrap());
            assert_eq!(
                got,
                brute(&ix.cfg.kind, &rows, &q, Some(&stats)),
                "{why}: range {range:?} at {pos}, {probe}"
            );
        }
    }
}

/// Recovers, checks, replays the rest of the feed and checks again.
fn recovers(fs: &FaultFs, positions: &[Pos], why: &str) -> Vec<Pos> {
    let (mut ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(fs)).unwrap();
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

/// An index with a checkpoint at 20 (low 12), and feed up to 30 (low 25).
fn prepared(fs: &FaultFs) -> ImpactIndex {
    let mut ix = ImpactIndex::new(cfg(), RamStore::default(), Some(store(fs))).unwrap();
    for range in [RANGE, OTHER] {
        ix.begin_load(range, 0, 0).unwrap();
        ix.finish_load(range).unwrap();
        ix.caught_up(range, 0).unwrap();
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

#[test]
fn vs10_a_checkpoint_recovers_every_retained_cut() {
    let fs = disk();
    let mut ix = prepared(&fs);
    ix.checkpoint().unwrap();
    assert_eq!(recovers(&fs, &[30], "whole"), [30, 30]);
    let (ix2, _) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
    for range in [RANGE, OTHER] {
        let (a, b) = (ix.status(range).unwrap(), ix2.status(range).unwrap());
        assert_eq!((a.low, a.bound, a.closed), (b.low, b.bound, b.closed));
        assert_eq!(
            ix.counts(&[range], "common rare").unwrap(),
            ix2.counts(&[range], "common rare").unwrap()
        );
    }
}

#[test]
fn vs10_impact_checkpoint_survives_creation_of_its_ancestors() {
    let fs = FaultFs::new();
    prepared(&fs).checkpoint().unwrap();
    assert_eq!(recovers(&fs.crash(), &[30], "fresh checkpoint directory"), [30, 30]);
}

/// A crash at each step of a checkpoint leaves the old checkpoint or the new one, whole.
fn crash_every_step() -> std::result::Result<(), String> {
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
        let _ = ix.checkpoint();
        for image in [fs.crash(), fs.power_loss(step), fs.kill()] {
            let found =
                std::panic::catch_unwind(|| recovers(&image, &[20, 30], &format!("step {step}")))
                    .map_err(|_| format!("step {step} does not recover"))?;
            old += found.iter().filter(|p| **p == 20).count();
            new += found.iter().filter(|p| **p == 30).count();
        }
    }
    if old == 0 || new == 0 {
        return Err(format!("old {old}, new {new}"));
    }
    Ok(())
}

#[test]
fn vs10_a_crash_at_every_step_of_a_checkpoint() {
    crash_every_step().unwrap();
}

#[test]
fn vs10_a_manifest_before_its_files_are_synced_is_caught() {
    assert!(with_mutant("manifest_before_sync", crash_every_step).is_err());
}

#[test]
fn vs10_a_checkpoint_claims_only_what_its_files_hold() {
    let run = || {
        let fs = disk();
        let mut ix = prepared(&fs);
        ix.begin_checkpoint().unwrap();
        while ix.write_file().unwrap() {}
        for range in [RANGE, OTHER] {
            advance(&mut ix, range, 34);
        }
        ix.install_manifest().unwrap();
        std::panic::catch_unwind(|| recovers(&fs, &[30], "ahead")).is_ok()
    };
    assert!(run());
    assert!(!with_mutant("checkpoint_ahead_of_files", run));
}

#[test]
fn vs10_a_checkpoint_keeps_the_oldest_servable_cut() {
    let run = || {
        let fs = disk();
        prepared(&fs).checkpoint().unwrap();
        let (ix, _) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
        ix.status(RANGE).unwrap().low
    };
    assert_eq!(run(), 25);
    assert_eq!(with_mutant("checkpoint_forgets_low", run), 0);
}

/// Merges and retention go on while a checkpoint writes. The segments it listed wait for it.
fn merges_during_a_checkpoint() -> std::result::Result<(), String> {
    let fs = disk();
    let mut ix = prepared(&fs);
    ix.begin_checkpoint().unwrap();
    let mut more = true;
    let mut to = 30;
    while more {
        more = ix.write_file().map_err(|e| e.to_string())?;
        to += 1;
        for range in [RANGE, OTHER] {
            advance(&mut ix, range, to);
            ix.advance_low(range, to).unwrap();
        }
        ix.compact().map_err(|e| e.to_string())?;
    }
    ix.install_manifest().map_err(|e| e.to_string())?;
    std::panic::catch_unwind(|| recovers(&fs, &[30], "merged"))
        .map_err(|_| "lost rows".to_owned())?;
    Ok(())
}

#[test]
fn vs10_a_checkpoint_holds_merges_back() {
    merges_during_a_checkpoint().unwrap();
    assert!(with_mutant("free_without_wait", merges_during_a_checkpoint).is_err());
}

#[test]
fn vs10_a_damaged_file_takes_its_ranges_out_of_service() {
    let fs = disk();
    prepared(&fs).checkpoint().unwrap();
    let files = store(&fs).files();
    let doc = files
        .iter()
        .find(|p| p.to_string_lossy().contains("doc-"))
        .unwrap();
    let len = fs.contents(doc).unwrap().len() as u64;
    for at in [3, 12, len / 2, len - 1] {
        let image = fs.kill();
        image.flip_bit(doc, at, 2).unwrap();
        let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
        assert_eq!(damage.len(), 1, "byte {at}");
        assert_eq!(damage[0].ranges, [RANGE, OTHER]);
        for range in [RANGE, OTHER] {
            assert!(ix.status(range).unwrap().damage.is_some());
        }
    }
    // A damaged manifest is no checkpoint, and a drop takes it away.
    let image = fs.kill();
    image
        .flip_bit(&Path::new(DIR).join("MANIFEST"), 9, 0)
        .unwrap();
    let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&image)).unwrap();
    assert_eq!((damage.len(), damage[0].ranges.len()), (1, 0));
    assert!(ix.status(RANGE).is_err());
}

#[test]
fn vs10_a_checkpoint_of_another_analysis_is_not_loaded() {
    let fs = disk();
    prepared(&fs).checkpoint().unwrap();
    let other = Config {
        kind: Kind::Text("simple".into()),
        ..cfg()
    };
    let (ix, damage) = ImpactIndex::recover(other, RamStore::default(), store(&fs)).unwrap();
    assert_eq!(damage.len(), 1);
    assert!(ix.status(RANGE).is_err());
    let columns = Config {
        columns: vec![ColumnType::Int],
        ..cfg()
    };
    assert_eq!(
        ImpactIndex::recover(columns, RamStore::default(), store(&fs))
            .unwrap()
            .1
            .len(),
        1
    );
}

#[test]
fn vs10_a_dropped_range_does_not_come_back_and_a_peer_copy_recovers() {
    let fs = disk();
    let mut ix = prepared(&fs);
    ix.checkpoint().unwrap();
    let peer = disk();
    copy_checkpoint(&store(&fs), &store(&peer)).unwrap();
    ix.drop_range(OTHER).unwrap();
    let (back, _) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs.crash())).unwrap();
    assert!(back.status(OTHER).is_err() && back.status(RANGE).is_ok());
    assert_eq!(recovers(&peer, &[30], "peer"), [30, 30]);
    assert!(
        copy_checkpoint(&store(&disk()), &store(&peer)).is_err(),
        "a peer without a checkpoint"
    );
}

#[test]
fn vs10_interrupted_peer_copy_preserves_the_installed_impact_checkpoint() {
    let local = disk();
    prepared(&local); // Installs position 20 in doc-0.
    let peer = disk();
    let mut ix = ImpactIndex::new(cfg(), RamStore::default(), Some(store(&peer))).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    advance(&mut ix, RANGE, 10);
    ix.checkpoint().unwrap(); // Installs different contents under the same file number.

    local.inject(Fault::on(FsOp::Rename));
    assert!(copy_checkpoint(&store(&peer), &store(&local)).is_err());
    let restored = local.crash();
    assert_eq!(recovers(&restored, &[20], "interrupted peer copy"), [20, 20]);

    copy_checkpoint(&store(&peer), &store(&restored)).unwrap();
    assert_eq!(recovers(&restored.crash(), &[10], "retried peer copy"), [10]);

    // The installed manifest still reserves the file number if its file is missing.
    let missing = local.crash();
    let doc = Path::new(DIR).join("doc-0000000000000000");
    missing.remove(&doc).unwrap();
    copy_checkpoint(&store(&peer), &store(&missing)).unwrap();
    assert!(!missing.crash().exists(&doc).unwrap());
    assert_eq!(recovers(&missing.crash(), &[10], "peer copy over damage"), [10]);
}

/// A checkpoint of a build before decisions L10 and L11 holds terms of unprepared text, so it is
/// refused, not misread.
#[test]
fn vs10_a_checkpoint_from_before_nfc_is_not_loaded() {
    let fs = disk();
    prepared(&fs).checkpoint().unwrap();
    let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
    assert_eq!((damage.len(), ix.status(RANGE).is_ok()), (0, true));
    store(&fs).set_identity(b"text/english/1:t").unwrap();
    let (ix, damage) = ImpactIndex::recover(cfg(), RamStore::default(), store(&fs)).unwrap();
    assert_eq!(damage.len(), 1);
    assert!(ix.status(RANGE).is_err());
}

#[test]
fn vs10_peer_copy_cannot_publish_a_checkpoint_with_missing_local_files() {
    let local = disk();
    let mut writer = prepared(&local);
    writer.begin_checkpoint().unwrap();
    while writer.write_file().unwrap() {}

    let peer = disk();
    prepared(&peer);
    let _ = copy_checkpoint(&store(&peer), &store(&local));
    // Either the copy or the pending checkpoint may be refused, but a manifest must stay whole.
    let published = writer.install_manifest().is_ok();
    let pos = if published { 30 } else { 20 };
    assert_eq!(recovers(&local.crash(), &[pos], "copy while a local checkpoint has files"), [pos, pos]);
}
