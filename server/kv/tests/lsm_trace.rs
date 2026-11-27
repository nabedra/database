//! `nabedra.trace/1` traces of the journal's group commit, checked against
//! `specs/lsm/Lsm.tla` by `specs/lsm/LsmTrace.tla` (check `lsm-lsm-trace`).
//!
//! Two writers submit puts and deletes of two keys, synced and unsynced, each waiting for
//! its commit before the next, on one store of a simulated node. Each writer logs its
//! `Submit` with the sequence `Commit::seq` gives it; the journal writer reports
//! `JournalWriteGroup`, `JournalPublish` and `JournalSync` through `nabedra_kv::sim_trace`.
//! The segment is large enough that no roll happens, and the memtable budget that no
//! flush does, so every step the store takes in the run is one of these; the trace spec
//! says what that leaves out.
//!
//! With `NABEDRA_TRACE_DIR` set, each seed's trace goes to `$NABEDRA_TRACE_DIR/accepted`,
//! and mutated copies of it, which the spec must reject at the line their name gives, to
//! `$NABEDRA_TRACE_DIR/rejected`.
#![cfg(feature = "sim")]
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::{Rng, fault_fs, small_options};
use nabedra_kv::sim_trace::{JournalProbe, JournalStep};
use nabedra_kv::{Background, Durability, Store, WriteBatch};
use nabedra_rt::sim::{NodeClock, in_node};
use nabedra_sim::tla_trace::{Action, TlaTrace};
use serde_json::{Value, json};

const NODE: u64 = 1;
const WRITERS: [&str; 2] = ["w1", "w2"];
const KEYS: [&str; 2] = ["a", "b"];
const OPS: u64 = 6;
const SEEDS: u64 = 8;

/// Runs one seed and returns its published trace.
fn trace_run(seed: u64, dir: &Path) -> PathBuf {
    nabedra_sim::runtime::run(seed, |world| async move {
        world.add_node(NODE, NodeClock::default());
        let params = json!({
            "Keys": KEYS,
            "Writers": WRITERS,
            "MaxSeq": WRITERS.len() as u64 * OPS,
            "SegCap": 1_000,
            "MaxSegs": 2,
            "MaxFile": 4,
            "MaxImm": 1,
        });
        let trace = TlaTrace::builder("Lsm", "kv journal group commit")
            .params(params)
            .node(NODE, "journal")
            .action(Action::new("Submit", &["lastSub"]).msg(&["k", "op", "sync"]))
            .action(Action::new("JournalWriteGroup", &["lastW"]))
            .action(Action::new("JournalPublish", &["vis", "durPub"]))
            .action(Action::new("JournalSync", &["synced", "vis", "durPub"]))
            .install_in(&world, dir)
            .expect("install the trace");
        let probe = trace.steps();
        world.insert_ext(JournalProbe(Box::new(move |step| {
            let (action, vars) = match step {
                JournalStep::Written { last_seq } => ("JournalWriteGroup", json!({"lastW": last_seq})),
                JournalStep::Published { visible, durable } => {
                    ("JournalPublish", json!({"vis": visible, "durPub": durable}))
                }
                JournalStep::Synced {
                    synced,
                    visible,
                    durable,
                } => (
                    "JournalSync",
                    json!({"synced": synced, "vis": visible, "durPub": durable}),
                ),
            };
            probe.step(NODE, action, vars, None);
        })));
        let (fs, kv) = fault_fs();
        let store = in_node(NODE, async move {
            let background = Background::new(1).unwrap();
            let mut options = small_options(Arc::new(fs), background);
            // No roll and no flush in the run: the trace logs neither.
            options.journal.segment_bytes = 4 << 20;
            Arc::new(Store::open(&kv, options).unwrap().store)
        })
        .await;
        let mut writers = Vec::new();
        for (index, writer) in WRITERS.into_iter().enumerate() {
            let (store, steps) = (store.clone(), trace.steps());
            let mut rng = Rng(seed * 31 + index as u64);
            writers.push(world.spawn_node(NODE, async move {
                for _ in 0..OPS {
                    let key = KEYS[rng.below(KEYS.len() as u64) as usize];
                    let put = rng.below(3) != 0;
                    let sync = rng.below(2) == 0;
                    let mut batch = WriteBatch::new();
                    if put {
                        batch.put(key.as_bytes(), writer.as_bytes());
                    } else {
                        batch.delete(key.as_bytes());
                    }
                    let durability = if sync {
                        Durability::Sync
                    } else {
                        Durability::NoSync
                    };
                    let commit = store.submit(batch, durability, Box::new(())).unwrap();
                    let op = if put { "put" } else { "del" };
                    steps.step(
                        writer,
                        "Submit",
                        json!({"lastSub": commit.seq()}),
                        Some(json!({"k": key, "op": op, "sync": sync})),
                    );
                    commit.done().await.unwrap();
                    if rng.below(2) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for writer in writers {
            writer.await.unwrap();
        }
        // Closing the store is not part of the trace.
        world.insert_ext(JournalProbe(Box::new(|_| {})));
        let path = trace.finish().expect("publish the trace");
        drop(store);
        path
    })
}

/// The steps of a trace, as JSON.
fn lines(path: &Path) -> (String, Vec<Value>) {
    let text = fs::read_to_string(path).unwrap();
    let mut lines = text.lines();
    let header = lines.next().unwrap().to_owned();
    (header, lines.map(|line| serde_json::from_str(line).unwrap()).collect())
}

/// Writes `steps`, renumbered, as a trace the spec must reject at line `at` (0-based).
fn write_rejected(dir: &Path, name: &str, header: &str, mut steps: Vec<Value>, at: usize) {
    for (seq, step) in steps.iter_mut().enumerate() {
        step["seq"] = json!(seq);
    }
    let mut text = format!("{header}\n");
    for step in &steps {
        text.push_str(&serde_json::to_string(step).unwrap());
        text.push('\n');
    }
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join(format!("{name}.at-{at}.ndjson")), text).unwrap();
}

/// Three mutations of an accepted trace, each a bug the journal must not have: a publish
/// that claims durability before any sync (ack before sync), a sync that comes before the
/// write it covers, and a group written without its submission.
fn mutate(path: &Path, out: &Path) -> usize {
    let (header, steps) = lines(path);
    let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
    let action = |i: usize| steps[i]["action"].as_str().unwrap().to_owned();
    let mut written = 0;
    if let Some(i) = (0..steps.len()).find(|&i| action(i) == "JournalPublish") {
        let mut bad = steps.clone();
        let vis = bad[i]["vars"]["vis"].clone();
        if bad[i]["vars"]["durPub"] != vis {
            bad[i]["vars"]["durPub"] = vis;
            write_rejected(out, &format!("{stem}-ack-before-sync"), &header, bad, i);
            written += 1;
        }
    }
    if let Some(i) = (1..steps.len()).find(|&i| action(i) == "JournalSync" && action(i - 1) == "JournalWriteGroup") {
        let mut bad = steps.clone();
        bad.swap(i - 1, i);
        write_rejected(out, &format!("{stem}-sync-before-write"), &header, bad, i - 1);
        written += 1;
    }
    if let Some(i) = (0..steps.len()).find(|&i| action(i) == "Submit") {
        let mut bad = steps.clone();
        bad.remove(i);
        // The next line is a Submit whose sequence no longer follows, or a group written
        // with nothing submitted.
        write_rejected(out, &format!("{stem}-unsubmitted"), &header, bad, i);
        written += 1;
    }
    written
}

#[test]
fn journal_group_commit_traces() {
    let Some(base) = std::env::var_os("NABEDRA_TRACE_DIR").filter(|dir| !dir.is_empty()) else {
        eprintln!("NABEDRA_TRACE_DIR is not set: the traces are not written");
        return;
    };
    let base = PathBuf::from(base);
    let accepted = base.join("accepted");
    let rejected = base.join("rejected");
    fs::create_dir_all(&accepted).unwrap();
    let mut mutants = 0;
    for seed in 1..=SEEDS {
        let path = trace_run(seed, &accepted);
        let (_, steps) = lines(&path);
        let count = |name: &str| steps.iter().filter(|step| step["action"] == name).count();
        assert_eq!(count("Submit") as u64, WRITERS.len() as u64 * OPS, "seed {seed}");
        assert!(count("JournalWriteGroup") > 0 && count("JournalSync") > 0, "seed {seed}");
        if seed <= 2 {
            mutants += mutate(&path, &rejected);
        }
        // The same seed gives the same bytes (the writer refuses a different rerun).
        assert_eq!(trace_run(seed, &accepted), path);
    }
    assert!(mutants >= 4, "{mutants} rejected samples");
}
