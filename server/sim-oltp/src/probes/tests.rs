//! The planted defect that only a probe-driven invariant can catch.
//!
//! `double-apply.patch` beside this file makes every replica apply each normal Raft entry
//! a second time. The state machine's replay check turns the second apply into a no-op,
//! so clients, the history checkers and the public stats see nothing wrong. The
//! `Applied` probes show the same index twice in one incarnation, which
//! `MonotonicCommit` reports.
//!
//! The `oltp-probes-planted` check applies the patch in a scratch worktree and runs
//! this test there. On the shipped tree it fails, because nothing is applied twice; the
//! T1 `oltp-sim` shards are the clean twin.

use std::sync::Arc;

use nabedra_check::history::{Event, EventType};
use nabedra_oltp_types::{
    sim_probe::{Probe, emit},
    types::Digest,
};
use nabedra_protocol::protocol::Key;
use nabedra_rt::sim::{self, NodeClock, World, in_node};
use nabedra_sim::{shrink, sweep::workload_gen};
use serde_json::json;
use uuid::Uuid;

use super::{ProbeEvent, Probes};
use crate::{
    invariants::{
        Invariants,
        protocol::{MONOTONIC_COMMIT, NOT_APPLIED_NEVER_COMMITTED, PROBE_INVARIANTS},
    },
    sweep::Defect,
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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "needs probes/double-apply.patch applied; run by the oltp-probes-planted check"]
fn planted_double_apply_is_caught_only_by_probes() {
    assert!(
        nabedra_sim::runtime::SEEDED,
        "build with RUSTFLAGS=\"--cfg tokio_unstable\" and CARGO_TARGET_DIR=target/sim"
    );
    let first = env_u64("NABEDRA_SEED", 1);
    let count = env_u64("NABEDRA_SEEDS", 3);
    for seed in first..first + count {
        let run = run_seed(seed, &mut workload_gen(seed), Defect::None);
        let replay = run_seed(seed, &mut workload_gen(seed), Defect::None);
        let kinds = shrink::kinds(&run.run.violations);
        println!(
            "planted double-apply seed {seed}: trace {:016x} ({} events), kinds {kinds:?}, assertions {:?}",
            run.run.trace_hash, run.run.trace_events, run.assertions
        );
        assert!(
            kinds.contains(MONOTONIC_COMMIT),
            "seed {seed}: MonotonicCommit did not fire: {:?}",
            run.run.violations
        );
        assert!(
            kinds
                .iter()
                .all(|kind| PROBE_INVARIANTS.contains(&kind.as_str())),
            "seed {seed}: a non-probe check also fired, so probes are not the only detector: {kinds:?}"
        );
        for (name, passed) in &run.assertions {
            assert!(
                *passed || *name == "probe invariants",
                "seed {seed}: black-box assertion {name:?} failed"
            );
        }
        assert_eq!(
            (
                replay.run.trace_hash,
                replay.run.trace_events,
                &replay.run.history,
                &replay.run.violations
            ),
            (
                run.run.trace_hash,
                run.run.trace_events,
                &run.run.history,
                &run.run.violations
            ),
            "seed {seed}: the detection did not replay"
        );
    }
}
