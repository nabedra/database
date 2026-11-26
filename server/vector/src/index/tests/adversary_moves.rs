//! The partition lifecycle of plan section 5, one rule per test, in a hand-made state: append before
//! remove, removal by entry instance, a row that moves back, copies of what is beyond the applied
//! position, landing in a draining target, a target that does not drain, generations, scans that fail
//! with their index, and a checkpoint that pins like a scan. The model tests cover these rules in
//! random interleavings. Here each has a state in which removing the rule gives a wrong answer.

use std::path::Path;

use nabedra_rt::fs::FaultFs;

use super::*;

/// Six rows in two clusters, split into two partitions `a` and `b` on one core.
fn two_partitions() -> (Index, PartId, PartId) {
    let cfg = Config { part_min: 1, part_max: 8, queue: 64, ..Config::new(schema(2, &[ColumnType::Int], None), 1) };
    let mut ix = serving(cfg, ExactCodec, None);
    let angles = [0.0f32, 0.1, 0.2, 1.4, 1.5, 1.6];
    let changes = angles.iter().enumerate().map(|(k, a)| (k as u64, Some(row(&[a.cos(), a.sin()], vec![Value::Int(0)], k as u64)))).collect();
    feed(&mut ix, RANGE, &page(0, changes));
    assert!(ix.split(PartId(0)));
    settle(&mut ix);
    let parts: Vec<PartId> = ix.listed().into_iter().collect();
    assert_eq!(parts.len(), 2, "the split made two partitions");
    (ix, parts[0], parts[1])
}

/// The entries of a partition: slot, version, instance.
fn entries_of(ix: &Index, part: PartId) -> Vec<(Slot, Pos, u64)> {
    let parts = ix.shards.iter().filter_map(|s| s.parts.get(&part));
    parts.flat_map(|p| p.entries().map(|e| (e.slot, e.at, e.id))).collect()
}

/// How many entries of the index name this row.
fn named(ix: &Index, slot: Slot) -> usize {
    let all = ix.shards.iter().flat_map(|s| s.parts.values()).flat_map(|p| p.entries());
    all.filter(|e| e.slot == slot).count()
}

fn found(ix: &mut Index) -> Vec<u64> {
    let pin = latest(ix, None);
    let mut all = images(&ix.search(&pin, &query(Op::Dot, &[1.0, 1.0], 50)).unwrap());
    all.sort_unstable();
    all
}

fn every_row_once(ix: &Index) {
    let census = ix.shared.rows.census();
    let bad: Vec<_> = census.iter().filter(|c| c.versions.len() != 1 || c.versions[0].1 != 1 || named(ix, c.slot) != 1).map(|c| (c.slot, c.versions.clone())).collect();
    assert!(bad.is_empty(), "{bad:?}");
}

/// A move appends to the target before it removes from the source, and the removal waits for every scan
/// that began before the copy landed. The row is found, once, at every step.
#[test]
fn adv26_a_move_appends_first_and_removes_when_the_scans_that_saw_the_source_are_gone() {
    let (mut ix, a, b) = two_partitions();
    let (slot, _, id) = entries_of(&ix, a)[0];
    let pin = latest(&mut ix, None);
    let all: Vec<u64> = (0..6).collect();
    assert!(ix.reassign(a, id, b));
    assert_eq!(found(&mut ix), all, "the copy is on its way, and the source still holds the row");
    assert_eq!(named(&ix, slot), 1);
    ix.run();
    assert_eq!(named(&ix, slot), 2, "the copy landed");
    assert_eq!(found(&mut ix), all);
    for _ in 0..50 {
        ix.maintain(8);
        ix.run();
    }
    assert_eq!(named(&ix, slot), 2, "a scan that began before the copy landed holds the source");
    let held = ix.search(&pin, &query(Op::Dot, &[1.0, 1.0], 50)).unwrap();
    assert_eq!(held.len(), 6);
    drop(pin);
    settle(&mut ix);
    assert_eq!(named(&ix, slot), 1);
    assert_eq!(found(&mut ix), all);
    every_row_once(&ix);
}

/// A removal names one entry instance. A row that moved away and back has two instances in its first
/// partition while the old one waits, and the removal of the old one takes only that one.
#[test]
fn adv26_a_row_that_moves_back_keeps_its_new_entry_when_the_old_one_is_removed() {
    let (mut ix, a, b) = two_partitions();
    let (slot, _, id) = entries_of(&ix, a)[0];
    let pin = latest(&mut ix, None);
    assert!(ix.reassign(a, id, b));
    ix.run();
    let (_, _, in_b) = *entries_of(&ix, b).iter().find(|e| e.0 == slot).unwrap();
    assert!(ix.reassign(b, in_b, a), "and back");
    ix.run();
    assert_eq!(entries_of(&ix, a).iter().filter(|e| e.0 == slot).count(), 2, "the old instance and the new one");
    drop(pin);
    settle(&mut ix);
    assert_eq!(named(&ix, slot), 1, "one instance of the row is left");
    assert_eq!(found(&mut ix), (0..6).collect::<Vec<u64>>());
    every_row_once(&ix);
}

/// A drain copies what is beyond the applied position too: cores append ahead of it, and what is
/// there when the partition retires is all that is left of the entry.
#[test]
fn adv27_a_drain_copies_the_entries_beyond_the_applied_position() {
    let (mut ix, _, _) = two_partitions();
    let near = |k: u64, angle: f32| (k, Some(row(&[angle.cos(), angle.sin()], vec![Value::Int(0)], k)));
    let after = ix.status(RANGE).unwrap().applied;
    assert_eq!(ix.apply(RANGE, &page(after, vec![near(10, 0.05), near(11, 1.45)])), Ok(after + 2));
    ix.pump();
    // The entry of the second position is in, and the first is not.
    ix.shared.queues[0].rotate_right(1);
    assert!(ix.step(0));
    ix.advance();
    assert_eq!(ix.status(RANGE).unwrap().applied, after, "position {} is still open", after + 1);
    let slot = ix.shared.ranges[&RANGE].keys[&key(11)];
    let holder = ix.listed().into_iter().find(|p| entries_of(&ix, *p).iter().any(|e| e.0 == slot)).unwrap();
    let held = ix.shared.queues[0].pop_front().unwrap();
    assert!(ix.split(holder));
    for _ in 0..200 {
        ix.maintain(8);
        ix.run();
    }
    assert!(!ix.shared.dir.contains_key(&holder), "the partition is retired and gone");
    ix.shared.queues[0].push_back(held);
    ix.run();
    assert_eq!(ix.status(RANGE).unwrap().applied, after + 2);
    let mut want: Vec<u64> = (0..6).collect();
    want.extend([10, 11]);
    assert_eq!(found(&mut ix), want, "the entry beyond the applied position was copied");
    every_row_once(&ix);
}

/// A copy that lands in a partition that began to drain meanwhile is sent on, and nothing is lost.
#[test]
fn adv28_a_copy_that_lands_in_a_draining_target_is_sent_on() {
    let (mut ix, a, b) = two_partitions();
    let (_, _, id) = entries_of(&ix, a)[0];
    assert!(ix.reassign(a, id, b));
    assert!(ix.split(b), "the target begins to drain with the copy on its way");
    for _ in 0..200 {
        ix.maintain(8);
        ix.run();
    }
    settle(&mut ix);
    assert_eq!(found(&mut ix), (0..6).collect::<Vec<u64>>());
    every_row_once(&ix);
    assert!(!ix.shared.dir.contains_key(&b), "the drained partition is gone");
}

/// A partition that another one drains into does not start to drain, until that drain is over.
#[test]
fn adv28_a_drain_target_does_not_drain_until_the_drain_into_it_is_over() {
    let (mut ix, a, b) = two_partitions();
    assert!(ix.merge(a, b), "a drains into b");
    assert!(!ix.split(b), "b is a target");
    assert!(!ix.merge(b, a), "b is a target, and a is draining");
    settle(&mut ix);
    assert!(!ix.shared.dir.contains_key(&a));
    assert!(ix.split(b), "the drain into b is over");
    settle(&mut ix);
    assert_eq!(found(&mut ix), (0..6).collect::<Vec<u64>>());
    every_row_once(&ix);
}

/// A resync makes every copy of the older generation void: one that is on its way does not land.
#[test]
fn adv29_a_copy_of_the_old_generation_does_not_land_after_a_resync() {
    let (mut ix, a, b) = two_partitions();
    let (_, _, id) = entries_of(&ix, a)[0];
    assert!(ix.reassign(a, id, b));
    ix.begin_load(RANGE, 20, 100).unwrap();
    let new = |k: u64| (key(k), 50 + k, row(&[1.0, k as f32], vec![Value::Int(0)], 100 + k));
    ix.load_rows(RANGE, &[new(1), new(2), new(3)]).unwrap();
    ix.run();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 20).unwrap();
    settle(&mut ix);
    assert_eq!(found(&mut ix), vec![101, 102, 103], "only the rows of the new generation");
    assert_eq!(ix.shared.rows.census().len(), 3, "the slots of the old generation are free");
    assert_eq!(ix.shards.iter().flat_map(|s| s.parts.values()).map(|p| p.entries().count()).sum::<usize>(), 3);
}

/// A resync or a drop of a range fails every scan that pinned it, and no other.
#[test]
fn adv29_a_resync_and_a_drop_fail_the_scans_that_pinned_the_range() {
    let other = RangeId(9);
    let mut ix = serving(Config::new(schema(2, &[ColumnType::Int], None), 2), ExactCodec, None);
    ix.begin_load(other, 0, 0).unwrap();
    ix.finish_load(other).unwrap();
    ix.caught_up(other, 0).unwrap();
    let rows = |k: u64| (k, Some(row(&[1.0, k as f32], vec![Value::Int(0)], k)));
    feed(&mut ix, RANGE, &page(0, (0..20).map(rows).collect()));
    feed(&mut ix, other, &page(0, (20..40).map(rows).collect()));
    let q = query(Op::Dot, &[1.0, 1.0], 5);
    let both = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 }), (other, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let mine = ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let theirs = ix.pin(&[(other, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let mut s_both = ix.scan(&both, &q, Box::new(RecallEstimate)).unwrap();
    let mut s_mine = ix.scan(&mine, &q, Box::new(RecallEstimate)).unwrap();
    let mut s_theirs = ix.scan(&theirs, &q, Box::new(RecallEstimate)).unwrap();
    // A resync of one range.
    ix.begin_load(other, 3, 0).unwrap();
    assert_eq!(ix.visit(&mut s_both), Err(Error::PinFailed));
    assert!(matches!(ix.finish(s_both), Err(Error::PinFailed)));
    assert_eq!(ix.visit(&mut s_theirs), Err(Error::PinFailed));
    assert!(ix.visit(&mut s_mine).is_ok(), "a scan of another range goes on");
    // A drop of the other one.
    ix.drop_range(RANGE).unwrap();
    assert_eq!(ix.visit(&mut s_mine), Err(Error::PinFailed));
    assert!(matches!(ix.finish(s_mine), Err(Error::PinFailed)));
    assert_eq!(ix.search(&mine, &q), Err(Error::PinFailed));
    assert!(matches!(ix.finish(s_theirs), Err(Error::PinFailed)));
}

/// A checkpoint pins like a scan: what a cut it serves needs stays, however much is deleted and
/// collected while it is written, and the recovered index answers that cut.
#[test]
fn adv30_a_running_checkpoint_holds_back_collection_like_a_scan() {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/data"));
    let store = Store::new(Arc::new(fs.clone()), "/data/ck");
    let cfg = Config { part_min: 1, part_max: 8, ..Config::new(schema(2, &[ColumnType::Int], None), 2) };
    let mut ix = serving(cfg.clone(), ExactCodec, Some(store.clone()));
    let write = |k: u64, n: u64| (k, Some(row(&[1.0 + k as f32, n as f32], vec![Value::Int(0)], n * 100 + k)));
    feed(&mut ix, RANGE, &page(0, (0..6).map(|k| write(k, 0)).collect()));
    settle(&mut ix);
    ix.begin_checkpoint().unwrap();
    // Everything is deleted and written again under other keys, and retention passes it all.
    feed(&mut ix, RANGE, &page(6, (0..6).map(|k| (k, None)).collect()));
    feed(&mut ix, RANGE, &page(12, (10..16).map(|k| write(k, 1)).collect()));
    let applied = ix.status(RANGE).unwrap().applied;
    ix.advance_low(RANGE, applied).unwrap();
    for _ in 0..100 {
        ix.maintain(8);
        ix.run();
    }
    assert!(ix.shared.rows.census().len() >= 12, "the rows that the checkpoint needs are still there: {}", ix.shared.rows.census().len());
    while ix.write_file().unwrap() {}
    ix.install_manifest().unwrap();
    let (mut back, damage) = Index::recover(cfg, ExactCodec, store).unwrap();
    assert!(damage.is_empty(), "{damage:?}");
    back.caught_up(RANGE, 6).unwrap();
    let pin = back.pin(&[(RANGE, ReadAt::Position(6))], None).unwrap();
    let mut got = images(&back.search(&pin, &query(Op::Dot, &[1.0, 1.0], 50)).unwrap());
    got.sort_unstable();
    assert_eq!(got, vec![0, 1, 2, 3, 4, 5], "the cut of the checkpoint");
    // Once it is over the old rows go.
    settle(&mut ix);
    assert_eq!(ix.shared.rows.census().len(), 6);
}

/// The tests above are tests of their rules: with the bug of the spec's mutant put back, each fails.
#[test]
fn adv31_each_rule_test_fails_when_its_rule_is_taken_out() {
    use super::super::mutant::MUTANT;
    let cases: [(&str, fn()); 10] = [
        ("remove_before_append", adv26_a_move_appends_first_and_removes_when_the_scans_that_saw_the_source_are_gone),
        ("remove_without_wait", adv26_a_move_appends_first_and_removes_when_the_scans_that_saw_the_source_are_gone),
        ("prefix_remove_by_slot", adv26_a_row_that_moves_back_keeps_its_new_entry_when_the_old_one_is_removed),
        ("prefix_drain_ignores_unapplied", adv27_a_drain_copies_the_entries_beyond_the_applied_position),
        ("prefix_land_needs_ready", adv28_a_copy_that_lands_in_a_draining_target_is_sent_on),
        ("prefix_drain_a_target", adv28_a_drain_target_does_not_drain_until_the_drain_into_it_is_over),
        ("late_copy_after_resync", adv29_a_copy_of_the_old_generation_does_not_land_after_a_resync),
        ("prefix_scan_survives_resync", adv29_a_resync_and_a_drop_fail_the_scans_that_pinned_the_range),
        ("prefix_checkpoint_without_epoch", adv30_a_running_checkpoint_holds_back_collection_like_a_scan),
        ("gc_without_epoch", adv30_a_running_checkpoint_holds_back_collection_like_a_scan),
    ];
    let alive: Vec<&str> = cases
        .iter()
        .filter(|(mutant, test)| {
            MUTANT.with(|m| m.set(mutant));
            let outcome = std::panic::catch_unwind(*test);
            MUTANT.with(|m| m.set("none"));
            outcome.is_ok()
        })
        .map(|(mutant, _)| *mutant)
        .collect();
    assert!(alive.is_empty(), "no test fails without its rule: {alive:?}");
}
