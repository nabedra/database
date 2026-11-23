//! NS13 (Raft log on journal values), NS03 (snapshots during journal GC) and NS04 (crash
//! at every operation of a relocation) at store level.
//!
//! Groups append log entries as journal values (`put_journal`), purge their prefix and
//! sometimes truncate their tail (`delete_range`), as the pool's `typed_log` does. Group
//! `g00` lags: it appends a few entries early and never purges, so it pins the oldest
//! segments until journal GC relocates its values. A `BTreeMap` oracle checks every read.
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::{FaultFs, Fs};
use nabedra_kv::{Background, Durability, Options, Store};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const SEGMENT: u64 = 32 << 10;
const MAX_JOURNAL: u64 = 6 * SEGMENT;

fn options(fs: &FaultFs, bg: &Arc<Background>) -> Options {
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    o.journal.segment_bytes = SEGMENT;
    o.journal.max_group_bytes = 16 << 10;
    o.max_journal_bytes = MAX_JOURNAL;
    o.memtable_bytes = 24 << 10;
    o.max_replay_bytes = 48 << 10;
    o
}

fn entry_key(group: u64, idx: u64) -> Vec<u8> {
    format!("g{group:02}/e{idx:06}").into_bytes()
}

/// The end of a group's log key space (`gNN/f`).
fn log_end(group: u64) -> Vec<u8> {
    format!("g{group:02}/f").into_bytes()
}

/// One step of the workload.
#[derive(Clone, Debug)]
enum Step {
    Write(ModelBatch, Durability),
    Flush,
    Compact,
    Gc,
}

/// A Raft-log workload: appends, prefix purges and tail truncations.
fn workload(seed: u64, appends: usize) -> (Vec<Step>, Vec<Model>) {
    let mut rng = Rng(seed);
    let mut next = [0u64; 4];
    let mut steps = Vec::new();
    let mut models = vec![Model::new()];
    let push = |steps: &mut Vec<Step>, models: &mut Vec<Model>, b: ModelBatch, d| {
        let mut m = models.last().unwrap().clone();
        b.apply(&mut m);
        models.push(m);
        steps.push(Step::Write(b, d));
    };
    for i in 0..appends {
        let group = if i < 6 { 0 } else { 1 + rng.below(3) };
        let idx = next[group as usize];
        next[group as usize] += 1;
        let mut value = format!("<{group}:{idx}:{i}>").into_bytes();
        value.resize(400 + rng.below(1000) as usize, b'a' + (i % 26) as u8);
        let durability = if i % 4 == 0 {
            Durability::Sync
        } else {
            Durability::NoSync
        };
        push(
            &mut steps,
            &mut models,
            ModelBatch {
                scope: group_prefix(group),
                ops: vec![
                    Op::PutJournal(entry_key(group, idx), value),
                    Op::Put(
                        format!("g{group:02}/tail").into_bytes(),
                        idx.to_le_bytes().to_vec(),
                    ),
                ],
            },
            durability,
        );
        if group > 0 && idx % 5 == 4 {
            // Purge all but the last three entries.
            push(
                &mut steps,
                &mut models,
                ModelBatch {
                    scope: group_prefix(group),
                    ops: vec![Op::DeleteRange(
                        entry_key(group, 0),
                        entry_key(group, idx.saturating_sub(2)),
                    )],
                },
                Durability::NoSync,
            );
        }
        if group > 0 && idx % 13 == 12 {
            // truncate_after: drop the last two entries; the next appends rewrite them.
            push(
                &mut steps,
                &mut models,
                ModelBatch {
                    scope: group_prefix(group),
                    ops: vec![Op::DeleteRange(entry_key(group, idx - 1), log_end(group))],
                },
                Durability::Sync,
            );
            next[group as usize] = idx - 1;
        }
        if i % 10 == 9 {
            steps.push(Step::Flush);
        }
        if i % 20 == 19 {
            steps.push(Step::Compact);
        }
        if i % 15 == 14 {
            steps.push(Step::Gc);
        }
    }
    steps.push(Step::Flush);
    steps.push(Step::Gc);
    (steps, models)
}

fn journal_files(fs: &FaultFs, dir: &Path) -> usize {
    fs.list(&dir.join("journal"))
        .unwrap()
        .iter()
        .filter(|p| p.extension().is_some_and(|e| e == "jnl"))
        .count()
}

struct Outcome {
    /// Writes submitted.
    submitted: usize,
    /// Writes `[0, durable)` acknowledged as durable.
    durable: usize,
    /// `(ops before, ops after, relocated bytes after)` around each GC step.
    gc_windows: Vec<(u64, u64, u64)>,
}

fn run(fs: &FaultFs, bg: &Arc<Background>, steps: &[Step]) -> Option<(Store, Outcome)> {
    let dir = PathBuf::from("/db/kv");
    let store = Store::open(&dir, options(fs, bg)).ok()?.store;
    let mut out = Outcome {
        submitted: 0,
        durable: 0,
        gc_windows: Vec::new(),
    };
    let mut writes = 0;
    for step in steps {
        let ok = match step {
            Step::Write(b, d) => {
                let Ok(commit) = store.submit(b.to_batch(), *d, Box::new(())) else {
                    break;
                };
                writes += 1;
                out.submitted = writes;
                let ok = commit.wait().is_ok();
                if ok && *d == Durability::Sync {
                    out.durable = writes;
                }
                ok
            }
            Step::Flush => store.flush().is_ok(),
            Step::Compact => store.compact().is_ok(),
            Step::Gc => {
                let before = fs.mutating_ops();
                let ok = store.gc_journal().is_ok();
                out.gc_windows
                    .push((before, fs.mutating_ops(), store.stats().relocated_bytes));
                ok
            }
        };
        if !ok {
            break;
        }
    }
    Some((store, out))
}

/// NS13: log reads match the oracle, segments are recycled while a lagging group pins
/// the oldest one, relocated bytes are counted, and a reopen reads the same.
#[test]
fn ns13_raft_log_on_journal_values_recycles_segments_past_a_lagging_group() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let (steps, models) = workload(13, 400);
    let (store, out) = run(&fs, &bg, &steps).expect("opens");
    let model = models.last().unwrap();
    assert_eq!(out.submitted, models.len() - 1);
    assert_eq!(&dump(&store.snapshot()), model);
    let stats = store.stats();
    eprintln!(
        "NS13: {} writes, journal {} bytes written, {} relocated, {} journal files, stats {stats:?}",
        out.submitted,
        stats.journal_bytes,
        stats.relocated_bytes,
        journal_files(&fs, &dir)
    );
    assert!(stats.relocated_bytes > 0, "the lagging group was relocated");
    assert!(
        stats.journal_bytes > 3 * MAX_JOURNAL,
        "the workload wrote well past the journal budget"
    );
    // The journal stays near its budget: live segments plus at most a spare and the
    // segments of the current replay window.
    let files = journal_files(&fs, &dir) as u64;
    assert!(
        files * SEGMENT <= MAX_JOURNAL + 3 * SEGMENT,
        "{files} journal files of {SEGMENT} bytes for a {MAX_JOURNAL} byte budget"
    );
    // The lagging group's first entries are still readable.
    let snap = store.snapshot();
    for idx in 0..6 {
        let key = entry_key(0, idx);
        assert_eq!(
            snap.get(&key).unwrap().as_deref(),
            model.get(&key).map(|v| &v[..])
        );
    }
    assert_eq!(stats.recycled_value_reads, 0);
    drop(snap);
    store.close().unwrap();
    // Recovery replays relocation records with the same outcome.
    let reopened = Store::open(&dir, options(&fs.crash(), &bg)).unwrap();
    assert!(reopened.quarantined.is_empty());
    assert_eq!(&dump(&reopened.store.snapshot()), model);
}

/// NS03 (journal GC part): a snapshot taken before relocation keeps reading the old
/// locators, and the relocated segments are retired only after it drops.
#[test]
fn ns03_snapshot_held_across_journal_gc_and_relocation() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let (steps, models) = workload(3, 400);
    // Run up to just before the final GC with a snapshot taken early, after the lagging
    // group wrote.
    let store = Store::open(&dir, options(&fs, &bg)).unwrap().store;
    let mut snap = None;
    let mut at_snap = None;
    let mut writes = 0;
    for step in &steps {
        match step {
            Step::Write(b, d) => {
                store
                    .submit(b.to_batch(), *d, Box::new(()))
                    .unwrap()
                    .wait()
                    .unwrap();
                writes += 1;
                if writes == 20 {
                    snap = Some(store.snapshot());
                    at_snap = Some(models[writes].clone());
                }
            }
            Step::Flush => store.flush().unwrap(),
            Step::Compact => store.compact().unwrap(),
            Step::Gc => {
                let _ = store.gc_journal().unwrap();
            }
        }
    }
    let snap = snap.unwrap();
    let expect = at_snap.unwrap();
    let held = journal_files(&fs, &dir);
    assert!(
        held as u64 * SEGMENT > MAX_JOURNAL,
        "the old snapshot pins segments ({held} files)"
    );
    // Every read at the snapshot still works, journal values included.
    assert_eq!(dump(&snap), expect);
    assert!(!store.gc_journal().unwrap(), "GC waits for the snapshot");
    assert!(store.stats().relocated_bytes > 0);
    assert_eq!(dump(&snap), expect);
    drop(snap);
    assert!(store.gc_journal().unwrap());
    store.flush().unwrap();
    store.gc_journal().unwrap();
    let after = journal_files(&fs, &dir);
    assert!(
        after < held,
        "segments retired after the snapshot dropped ({held} -> {after})"
    );
    assert_eq!(&dump(&store.snapshot()), models.last().unwrap());
}

/// Reopens a crashed image and checks the prefix rule; every journal value must resolve
/// (a dangling locator fails `dump`).
fn verify(fs: FaultFs, bg: &Arc<Background>, models: &[Model], out: &Outcome, ctx: &str) {
    let dir = PathBuf::from("/db/kv");
    let opened =
        Store::open(&dir, options(&fs, bg)).unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
    assert!(
        opened.quarantined.is_empty(),
        "{ctx}: {:?}",
        opened.quarantined
    );
    let state = dump(&opened.store.snapshot());
    let p = (out.durable..=out.submitted)
        .find(|&p| models[p] == state)
        .unwrap_or_else(|| panic!("{ctx}: no prefix in [{}, {}]", out.durable, out.submitted));
    assert_eq!(opened.store.stats().recycled_value_reads, 0, "{ctx}");
    drop(opened);
    let again = Store::open(&dir, options(&fs.crash(), bg))
        .unwrap_or_else(|e| panic!("{ctx}: second reopen failed: {e}"));
    assert_eq!(
        dump(&again.store.snapshot()),
        models[p],
        "{ctx}: second reopen"
    );
}

fn enumerate(stride: u64) {
    let bg = Background::new(2).unwrap();
    let (steps, models) = workload(17, 160);
    // Reference run: find the GC steps that relocated.
    let (fs, _) = fault_fs();
    let (store, out) = run(&fs, &bg, &steps).expect("opens");
    drop(store);
    let mut relocated = 0;
    let mut windows = Vec::new();
    for &(before, after, bytes) in &out.gc_windows {
        if bytes > relocated {
            windows.push((before, after));
            relocated = bytes;
        }
    }
    assert!(!windows.is_empty(), "the reference run relocated");
    let total = fs.mutating_ops();
    eprintln!("NS04 relocation: {total} mutating ops, relocation windows {windows:?}");
    let mut points: Vec<u64> = (0..=total + 10).step_by(stride as usize).collect();
    for &(before, after) in &windows {
        // Every operation of the relocation step and of the retirement that follows.
        points.extend(before.saturating_sub(5)..after + 40);
    }
    points.sort_unstable();
    points.dedup();
    for n in points {
        let (fs, _) = fault_fs();
        fs.crash_after(n);
        let Some((store, out)) = run(&fs, &bg, &steps) else {
            continue;
        };
        drop(store);
        verify(fs.crash(), &bg, &models, &out, &format!("crash after {n}"));
    }
}

/// NS04 for relocation boundaries: a crash at every mutating operation of a relocation
/// pass and of the retirement after it (plus every 40th operation elsewhere) recovers an
/// exact prefix with no dangling locator.
#[test]
fn ns04_ns13_crash_at_every_relocation_operation() {
    enumerate(40);
}

/// The same at every operation of the whole workload.
#[test]
#[ignore = "long: run in release"]
fn ns04_ns13_crash_at_every_operation_full() {
    enumerate(1);
}

/// NS13 (KVfix3): journal GC relocates live values in records that fit a segment's
/// record area. With 12 KiB segments that area is 4 KiB; GC cut relocation batches at a
/// 3 KiB target checked before each entry, so three live 1.4 KiB values of one group went
/// into one 4.3 KiB record and `gc_journal` failed with "batch larger than a journal
/// segment" (NS10 long campaign, seed 69).
#[test]
fn ns13_gc_relocation_records_fit_small_segments() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let mut o = options(&fs, &bg);
    o.journal.segment_bytes = 12 << 10;
    o.journal.max_group_bytes = 4 << 10;
    o.max_journal_bytes = 3 * (12 << 10);
    let store = Store::open(&dir, o).unwrap().store;
    let mut model = Model::new();
    let put = |store: &Store, model: &mut Model, group: u64, idx: u64, fill: u8| {
        let key = entry_key(group, idx);
        let mut value = format!("<{group}:{idx}>").into_bytes();
        value.resize(1400, fill);
        let b = ModelBatch {
            scope: group_prefix(group),
            ops: vec![Op::PutJournal(key, value)],
        };
        b.apply(model);
        write(store, &b, Durability::Sync).unwrap();
    };
    // g00's live values sit in the oldest segment.
    for idx in 0..3 {
        put(&store, &mut model, 0, idx, b'a');
    }
    // g01 fills many segments and deletes everything it wrote.
    for idx in 0..60 {
        put(&store, &mut model, 1, idx, b'b');
    }
    let purge = ModelBatch {
        scope: group_prefix(1),
        ops: vec![Op::DeleteRange(group_prefix(1), log_end(1))],
    };
    purge.apply(&mut model);
    write(&store, &purge, Durability::Sync).unwrap();
    store.flush().unwrap();
    store.compact().unwrap();
    for _ in 0..4 {
        store.gc_journal().unwrap();
    }
    assert!(
        journal_files(&fs, &dir) <= 6,
        "GC retired nothing: {} segments",
        journal_files(&fs, &dir)
    );
    assert!(!store.requires_recovery());
    assert_eq!(dump(&store.snapshot()), model);
}

/// NS13, NS09 (KVfix3): journal GC never drops a live value whose liveness check could
/// not read the key. `holds` took a read error for "no longer held", so the value was
/// not relocated and its segment was recycled under its locator: later reads failed with
/// "journal locator names a recycled segment" (NS10 long campaign, seed 510). EIO is
/// injected on each run read of the GC passes in turn, whichever thread makes it.
#[test]
fn ns13_gc_never_drops_a_value_whose_liveness_read_fails() {
    use nabedra_kv::fs::{Fault, FsOp};
    let mut hit = 0;
    for n in 1..=40 {
        let (fs, dir) = fault_fs();
        let bg = Background::new(2).unwrap();
        let mut o = small_options(Arc::new(fs.clone()), bg.clone());
        o.journal.segment_bytes = SEGMENT;
        o.journal.max_group_bytes = 16 << 10;
        o.max_journal_bytes = 3 * SEGMENT;
        o.memtable_bytes = 24 << 10;
        o.max_replay_bytes = 48 << 10;
        let store = Store::open(&dir, o).unwrap().store;
        let mut model = Model::new();
        let put = |store: &Store, model: &mut Model, group: u64, idx: u64| {
            let mut value = format!("<{group}:{idx}>").into_bytes();
            value.resize(900, b'a' + group as u8);
            let b = ModelBatch {
                scope: group_prefix(group),
                ops: vec![Op::PutJournal(entry_key(group, idx), value)],
            };
            b.apply(model);
            write(store, &b, Durability::Sync).unwrap();
        };
        for idx in 0..6 {
            put(&store, &mut model, 0, idx);
        }
        for idx in 0..200 {
            put(&store, &mut model, 1, idx);
        }
        let purge = ModelBatch {
            scope: group_prefix(1),
            ops: vec![Op::DeleteRange(group_prefix(1), log_end(1))],
        };
        purge.apply(&mut model);
        write(&store, &purge, Durability::Sync).unwrap();
        store.flush().unwrap();
        // Background compaction quiet: the reads below are the GC passes'.
        store.compact().unwrap();
        fs.inject(Fault::on(FsOp::Read).path_prefix(dir.join("runs")).nth(n));
        for _ in 0..4 {
            let _ = store.gc_journal();
        }
        if fs.calls().iter().any(|c| c.op == FsOp::Read && !c.ok) {
            hit += 1;
        }
        fs.clear_faults();
        if !store.requires_recovery() {
            for _ in 0..4 {
                store.gc_journal().unwrap();
            }
        }
        // Whatever GC did, every value is still there (reopen after a latch).
        drop(store);
        let reopened = Store::open(&dir, small_options(Arc::new(fs.clone()), bg.clone()))
            .unwrap()
            .store;
        assert_eq!(dump(&reopened.snapshot()), model, "EIO on run read {n}");
    }
    assert!(hit > 0, "no injected read fired");
}
