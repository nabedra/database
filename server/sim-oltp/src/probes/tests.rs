//! The planted defects that only a probe-driven invariant can catch.
//!
//! `double-apply.patch` beside this file makes every replica apply each normal Raft entry
//! a second time. The state machine's replay check turns the second apply into a no-op,
//! so clients, the history checkers and the public stats see nothing wrong. The
//! `Applied` probes show the same index twice in one incarnation, which
//! `MonotonicCommit` reports.
//!
//! `recurring-charge.patch` makes each worker that applied a direct entry hold 564 bytes of
//! its progress arena for 50 of every 51 ms, from then on. `ResourceBaseline` reports it.
//!
//! The `oltp-probes-planted` check runs the same seeds in a scratch worktree twice: first
//! without the first patch, where `clean_twin_reports_nothing` must pass, then with it. The
//! `oltp-probes-baseline` check does so with 50 clean seeds and the second patch.
//!
//! `control_digests_replay` is the failing assertion of the open bug
//! OLTP-sim-restart-key-entropy. `fixed-restart-key.patch` fixes the key, and the
//! `oltp-probes-restart-key` check shows that the assertion fails without it and passes
//! with it.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use nabedra_check::history::{Event, EventType};
use nabedra_oltp_types::{
    sim_probe::{Probe, emit},
    types::{AppliedPosition, Digest, GroupKind},
};
use nabedra_protocol::protocol::Key;
use nabedra_rt::sim::{self, NodeClock, Wipe, World, in_node};
use nabedra_sim::{shrink, sweep::workload_gen};
use serde_json::json;
use uuid::Uuid;

use super::{HARNESS, LogEntry, ProbeEvent, Probes};
use crate::{
    invariants::{
        Invariants, RULES_RAN,
        protocol::{
            BEYOND_FAULT_MODEL, MONOTONIC_COMMIT, NOT_APPLIED_NEVER_COMMITTED, RESOURCE_BASELINE,
        },
    },
    sweep::{Defect, RunResult},
    workload::run_seed,
};

fn installed(world: &Arc<World>) -> (Invariants, Probes) {
    let checks = Invariants::new();
    let probes = checks.install_probes(world, "unit");
    (checks, probes)
}

fn served(read_ts: u64) -> Probe {
    Probe::SnapshotServed {
        group: Uuid::nil(),
        read_ts,
    }
}

#[test]
fn subscriptions_see_accepted_probes_with_their_origin() {
    sim::run(1, |world| async move {
        world.add_node(1, NodeClock::default());
        let (checks, probes) = installed(&world);
        let mut even = probes.subscribe(|event: &ProbeEvent| {
            matches!(event.probe, Probe::SnapshotServed { read_ts, .. } if read_ts % 2 == 0)
        });
        in_node(1, async {
            emit(|| served(1));
            emit(|| served(2));
        })
        .await;
        world.crash(1);
        world.restart(1);
        in_node(1, async { emit(|| served(4)) }).await;

        let first = even.try_recv().expect("the accepted probe");
        assert_eq!(
            (first.seq, first.node, first.incarnation, first.live),
            (1, Some(1), 0, true)
        );
        let second = even.recv().await.expect("the probe after restart");
        assert_eq!((second.seq, second.incarnation, second.live), (2, 1, true));
        assert!(even.try_recv().is_none());
        assert!(checks.violations().is_empty() && checks.probe_invariants_hold());
    });
}

#[test]
fn a_cut_node_is_marked_dead_until_it_restarts() {
    sim::run(1, |world| async move {
        world.add_node(1, NodeClock::default());
        let (checks, probes) = installed(&world);
        let mut all = probes.subscribe(|_: &ProbeEvent| true);
        world.crash(1);
        in_node(1, async { emit(|| served(1)) }).await;
        assert!(!all.try_recv().expect("the dead node's probe").live);
        assert!(checks.probe_invariants_hold());
    });
}

#[test]
fn not_applied_answers_never_have_a_committed_dedup_record() {
    sim::run(1, |world| async move {
        world.add_node(1, NodeClock::default());
        let (checks, _) = installed(&world);
        let (table, request) = (Uuid::from_u128(3), Uuid::from_u128(4));
        in_node(1, async {
            emit(|| Probe::DedupApplied {
                group: Uuid::nil(),
                table,
                anchor: Key::Int64(5),
                request_id: request,
                commit_ts: 9,
                digest: Digest::from_bytes([1; 32]),
            })
        })
        .await;
        let event = |kind, index| {
            let mut event = Event::new(kind, 0, "insert")
                .with_value(json!({"not_applied": true}))
                .with_key(&format!("{table}/5"));
            event.request_id = Some(request.to_string());
            event.index = index;
            event
        };
        let answered = [event(EventType::Invoke, 0), event(EventType::Fail, 1)];
        // Design 3.7: a later retry of the same ID is no exemption.
        let retried = [&answered[..], &[event(EventType::Invoke, 2)]].concat();
        checks.check_outcomes(&world, &retried);
        assert!(!checks.probe_invariants_hold());
        checks.check_outcomes(&world, &answered);
        let kinds = shrink::kinds(&checks.violations());
        assert!(kinds.contains(NOT_APPLIED_NEVER_COMMITTED), "{kinds:?}");
        assert!(!checks.probe_invariants_hold());
    });
}

/// Wipes `nodes` of a group of three in turn and starts none. Returns the kinds reported, and
/// whether the probe invariants hold.
fn wiped(nodes: &'static [u64]) -> (BTreeSet<String>, bool) {
    sim::run(1, move |world| async move {
        let (checks, _) = installed(&world);
        let (group, data) = (Uuid::from_u128(8), Path::new("/sim/data"));
        let entry = AppliedPosition {
            term: 1,
            node_id: 1,
            index: 1,
        };
        for node in 1..=3 {
            world.add_node(node, NodeClock::default());
            world.mount(node, data);
            in_node(node, async {
                emit(|| Probe::Appended {
                    group,
                    kind: GroupKind::Catalog,
                    replica: node,
                    entries: vec![LogEntry {
                        at: entry,
                        voters: Some(vec![vec![1, 2, 3]]),
                    }],
                });
            })
            .await;
        }
        in_node(1, async {
            emit(|| Probe::Applied {
                group,
                kind: GroupKind::Catalog,
                replica: 1,
                entry,
                digest: Digest::from_bytes([1; 32]),
            });
        })
        .await;
        for node in nodes {
            world.crash(*node);
            world.wipe(*node, data, Wipe::Replace);
        }
        let holds = checks.probe_invariants_hold();
        (shrink::kinds(&checks.violations()), holds)
    })
}

#[test]
fn a_run_beyond_the_fault_model_is_neither_clean_nor_a_detection() {
    assert_eq!(wiped(&[2]), (BTreeSet::new(), true));
    // `sim_bugs.py` masks any run that reports the harness kind.
    let beyond = [BEYOND_FAULT_MODEL, HARNESS].map(str::to_owned);
    assert_eq!(wiped(&[2, 3]), (beyond.into(), false));
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// Runs `seed` twice and checks that it replays. Returns the first run.
fn replayed(seed: u64) -> RunResult {
    assert!(
        nabedra_sim::runtime::SEEDED,
        "build with RUSTFLAGS=\"--cfg tokio_unstable\" and CARGO_TARGET_DIR=target/sim"
    );
    let [run, replay] = [(); 2].map(|()| run_seed(seed, &mut workload_gen(seed), Defect::None));
    let rules = run
        .run
        .trace_tail
        .iter()
        .find(|line| line.contains(RULES_RAN));
    println!(
        "seed {seed}: trace {:016x} ({} events), violations {:#?}, assertions {:?}, {}",
        run.run.trace_hash,
        run.run.trace_events,
        run.run.violations,
        run.assertions,
        rules.expect("the probes were installed and the trace tail says which rules ran")
    );
    let outcome = |run: &RunResult| {
        let run = &run.run;
        (
            run.trace_hash,
            run.trace_events,
            run.history.clone(),
            run.violations.clone(),
        )
    };
    assert_eq!(
        outcome(&replay),
        outcome(&run),
        "seed {seed} did not replay"
    );
    run
}

fn seeds() -> std::ops::Range<u64> {
    let first = env_u64("NABEDRA_SEED", 1);
    first..first + env_u64("NABEDRA_SEEDS", 3)
}

/// Seed 18 is judged while a Raft message is in flight on n1: a charge that returns to its
/// baseline within 10 ms and comes back, which is no leak.
const RAFT_FRAME_SEED: u64 = 18;

#[test]
#[ignore = "needs probes installed in the smoke workload; run by the oltp-probes-planted check"]
fn clean_twin_reports_nothing() {
    for seed in seeds().chain([RAFT_FRAME_SEED]) {
        let run = replayed(seed);
        assert!(
            run.run.violations.is_empty() && run.assertions.values().all(|passed| *passed),
            "seed {seed}: the clean twin is not clean"
        );
    }
}

/// Every seed reports `rule` and nothing else, and every assertion in `black_box` passes.
fn only_detector(rule: &str, black_box: &[&str]) {
    for seed in seeds() {
        let run = replayed(seed);
        let kinds: Vec<_> = shrink::kinds(&run.run.violations).into_iter().collect();
        assert_eq!(kinds, [rule], "seed {seed}");
        for name in black_box {
            assert_eq!(
                run.assertions.get(name),
                Some(&true),
                "seed {seed}: black-box assertion {name:?}"
            );
        }
        let others = run
            .assertions
            .iter()
            .filter(|(name, _)| !black_box.contains(name));
        for (name, passed) in others {
            println!("seed {seed}: not a black-box assertion here: {name:?} {passed}");
        }
    }
}

const BLACK_BOX: [&str; 9] = [
    "cluster readiness",
    "crash teardown",
    "direct path observed",
    "linearizability and keyed dedup",
    "liveness",
    "mounted device identity",
    "original keyed retry result",
    "public HTTP workload completed",
    "registries empty",
];

#[test]
#[ignore = "needs probes/double-apply.patch applied; run by the oltp-probes-planted check"]
fn planted_double_apply_is_caught_only_by_probes() {
    let black_box = [&BLACK_BOX[..], &["public resource baseline"]].concat();
    only_detector(MONOTONIC_COMMIT, &black_box);
}

/// The public stats do not show the arena the charge is in, but the assertion named after
/// them is false for any ResourceBaseline report.
#[test]
#[ignore = "needs probes/recurring-charge.patch applied; run by the oltp-probes-baseline check"]
fn planted_recurring_charge_is_caught_only_by_probes() {
    only_detector(RESOURCE_BASELINE, &BLACK_BOX);
}

/// The Control entries the current run applied, while a test collects them.
static CONTROL: Mutex<Option<Vec<String>>> = Mutex::new(None);

pub(super) fn record_control(event: &ProbeEvent) {
    if let Probe::Applied {
        kind: GroupKind::Control,
        replica,
        entry,
        digest,
        ..
    } = &event.probe
        && let Some(entries) = CONTROL.lock().unwrap().as_mut()
    {
        entries.push(format!(
            "replica {replica} index {}: {digest:?}",
            entry.index
        ));
    }
}

#[test]
#[ignore = "red while OLTP-sim-restart-key-entropy is open; run by the oltp-probes-restart-key check"]
fn control_digests_replay() {
    let mut differing = 0;
    for seed in seeds() {
        let [first, second] = [(); 2].map(|()| {
            *CONTROL.lock().unwrap() = Some(Vec::new());
            run_seed(seed, &mut workload_gen(seed), Defect::None);
            CONTROL.lock().unwrap().take().unwrap()
        });
        assert!(
            !first.is_empty() && first.len() == second.len(),
            "seed {seed}: {} and {} Control entries applied",
            first.len(),
            second.len()
        );
        let pairs = first.iter().zip(&second).filter(|(a, b)| a != b);
        let entries: Vec<_> = pairs.collect();
        println!(
            "seed {seed}: {} of {} applied Control digests differ between two runs: {entries:#?}",
            entries.len(),
            first.len()
        );
        differing += entries.len();
    }
    assert_eq!(
        differing, 0,
        "applied Control digests differ between two runs of one seed"
    );
}
