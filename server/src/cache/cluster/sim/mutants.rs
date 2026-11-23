//! Directed witnesses and seeded searches for the Cache fault model (K03).

use nabedra_check::{
    checker::relaxed_kv,
    history::{reindex, write_jsonl},
};
use nabedra_sim::{PlantedSearch, runtime};

use super::super::super::clock::ClockMode;
use super::super::super::protocol::{Admin, Reply};
use super::super::harness::{Cluster, Config, memory_limit};
use super::super::history::Op;
use super::super::hooks::Knobs;
use crate::cache::{command::key_slot, store::EvictionPolicy, wire::Response};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    BoundIgnored,
    DeleteIgnored,
    ExpiryDeleteIgnored,
    EpochReused,
    LiveReset,
    ExcessDrift(i64),
    LocalSkew,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::BoundIgnored => "P1",
            Self::DeleteIgnored | Self::ExpiryDeleteIgnored => "P2",
            Self::EpochReused => "P3",
            Self::LiveReset => "P4",
            Self::ExcessDrift(_) => "E1",
            Self::LocalSkew => "E2",
        }
    }

    fn invariant(self) -> &'static str {
        match self {
            Self::BoundIgnored | Self::LiveReset | Self::ExcessDrift(_) | Self::LocalSkew => {
                "LeaseExclusive"
            }
            Self::DeleteIgnored | Self::ExpiryDeleteIgnored => "ReplicaPrefix",
            Self::EpochReused => "SlotOwnership",
        }
    }

    fn budget(self) -> u64 {
        match self {
            Self::ExcessDrift(_) | Self::LocalSkew => 200,
            _ => 500,
        }
    }

    fn config(self, enabled: bool) -> Config {
        Config {
            nodes: 4,
            groups: 3,
            actors: 1,
            slot_bytes: memory_limit(1),
            eviction: EvictionPolicy::NoEviction,
            clock_mode: if enabled && self == Self::LocalSkew {
                ClockMode::LocalDocker
            } else {
                ClockMode::ClockBound
            },
            base_error_ns: 4_450_000,
            declared_ppm: 10,
            skew: matches!(
                self,
                Self::BoundIgnored | Self::LocalSkew | Self::ExcessDrift(_)
            ),
            gradual_drift: false,
            knobs: Knobs {
                lease_ignores_error_bound: enabled && self == Self::BoundIgnored,
                replica_keeps_deleted_keys: enabled
                    && matches!(self, Self::DeleteIgnored | Self::ExpiryDeleteIgnored),
                owner_change_keeps_epoch: enabled && self == Self::EpochReused,
                cold_reset_ignores_live_lease: enabled && self == Self::LiveReset,
            },
            check_replica_prefix: true,
        }
    }
}

fn write_ok(reply: Option<Response>) -> bool {
    matches!(reply, Some(Response::Simple(ref message)) if message.as_ref() == b"OK")
}

#[derive(Debug, PartialEq, Eq)]
struct Trial {
    trace_hash: u64,
    trace_events: u64,
    history: Vec<u8>,
    violations: Vec<String>,
}

fn run_case(case: Case, seed: u64, generated: bool, enabled: bool, history_only: bool) -> Trial {
    runtime::run(seed, |world| async move {
        let mut choices = nabedra_sim::sweep::workload_gen(seed);
        let slot_max = if generated {
            1 + choices.below(250) as u16
        } else {
            32
        };
        let key = (0..)
            .map(|i| format!("{{lease-{i}}}"))
            .find(|key| key_slot(key.as_bytes()) < slot_max)
            .unwrap();
        let slot = key_slot(key.as_bytes());
        let mut config = case.config(enabled);
        config.check_replica_prefix = !history_only;
        let mut cluster = Cluster::boot(world, vec![key.clone()], config).await;
        let before = cluster.slot(slot);
        let old = before.primary.unwrap();
        let replica = before.replica.unwrap();
        assert_ne!(old.node, replica.node);
        let ttl = (case == Case::ExpiryDeleteIgnored).then_some(1_500);
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("old-value".into(), ttl), Some(old.node))
                .await
        ));
        for _ in 0..30 {
            if cluster
                .sample(Some(replica), slot)
                .await
                .is_some_and(|sample| sample.sequence > 0)
            {
                break;
            }
            cluster.step(100).await;
        }
        assert!(
            cluster
                .sample(Some(replica), slot)
                .await
                .is_some_and(|sample| sample.sequence > 0),
            "replica did not receive first write"
        );
        if case == Case::DeleteIgnored {
            assert!(write_ok(
                cluster
                    .send(&key, Op::Set("overwritten".into(), None), Some(old.node))
                    .await
            ));
            assert_eq!(
                cluster.send(&key, Op::Get, Some(old.node)).await,
                Some(Response::bulk(b"overwritten"))
            );
            assert!(matches!(
                cluster.send(&key, Op::Del, Some(old.node)).await,
                Some(Response::Integer(1))
            ));
            cluster.step(1_000).await;
        }
        if case == Case::ExpiryDeleteIgnored {
            cluster.step(2_000).await;
            assert_eq!(
                cluster.send(&key, Op::Get, Some(old.node)).await,
                Some(Response::Bulk(None))
            );
        }
        if let Case::ExcessDrift(ppm) = case {
            let ppm = if enabled {
                ppm
            } else {
                -i64::from(config.declared_ppm)
            };
            cluster.drift(old.node, ppm);
            // Age the bound before cutting renewals so 100x declared drift
            // crosses the fence even with the controller's conservative cutoff.
            cluster.step(8_000).await;
        }
        if generated {
            cluster.step(choices.below(30)).await;
        }
        cluster.isolate_controller_links(old.node);
        if case == Case::LiveReset {
            assert!(matches!(
                cluster.admin(Admin::Drain { worker: old.node }).await,
                Reply::Applied { .. }
            ));
            let reset = cluster.admin(Admin::ColdReset { slot: Some(slot) }).await;
            if enabled {
                assert!(matches!(reset, Reply::Applied { .. }), "{reset:?}");
            } else {
                assert!(
                    matches!(reset, Reply::Refused(reason) if reason == "prior authority is still live")
                );
            }
        }
        if matches!(
            case,
            Case::BoundIgnored | Case::LocalSkew | Case::ExcessDrift(_)
        ) {
            let grant = cluster
                .sample(Some(old), slot)
                .await
                .unwrap()
                .lease
                .unwrap();
            let guard_ms = if case == Case::LocalSkew { 0 } else { 5 };
            let deadline = grant.expires_ms - nabedra_rt::sim::EPOCH_MS;
            cluster
                .step((deadline + guard_ms).saturating_sub(cluster.now()))
                .await;
            cluster.immediate_delivery();
            cluster.reconcile_once().await;
            // This is also the target on the clean twin, where the
            // controller may refuse promotion at this instant.
            cluster.renew_once(replica.node).await.unwrap();
            cluster.sample_leases().await;
            for (node, value) in [
                (replica.node, "new-at-boundary"),
                (old.node, "old-at-boundary"),
            ] {
                let _ = cluster
                    .send(&key, Op::Set(value.into(), None), Some(node))
                    .await;
            }
        }
        let end = cluster.now() + 5_000;
        while cluster.now() < end {
            cluster.step(5).await;
            let placement = cluster.slot(slot);
            if placement.primary.is_some_and(|new| new != old) {
                let successor = placement.primary.unwrap();
                if cluster
                    .sample(Some(successor), slot)
                    .await
                    .is_some_and(|sample| sample.authorized)
                {
                    if case == Case::DeleteIgnored {
                        let _ = cluster.send(&key, Op::Get, Some(successor.node)).await;
                    } else if case != Case::EpochReused {
                        let _ = cluster
                            .send(
                                &key,
                                Op::Set("successor".into(), None),
                                Some(successor.node),
                            )
                            .await;
                        let _ = cluster
                            .send(&key, Op::Set("previous".into(), None), Some(old.node))
                            .await;
                    }
                    break;
                }
            }
        }
        let observed = cluster.observed.lock().unwrap();
        let mut violations: Vec<_> = observed
            .violations
            .iter()
            .map(|(name, detail)| format!("{name}: {detail}"))
            .collect();
        if history_only {
            let verdict = relaxed_kv::check(&observed.history);
            violations.extend(
                verdict
                    .anomalies
                    .into_iter()
                    .map(|anomaly| format!("{}: {}", anomaly.kind, anomaly.detail)),
            );
        }
        let mut events = observed.history.clone();
        reindex(&mut events);
        let mut history = Vec::new();
        write_jsonl(&mut history, &events).expect("serialize trial history");
        Trial {
            trace_hash: observed.trace.hash(),
            trace_events: observed.trace.events(),
            history,
            violations,
        }
    })
}

fn catches(violations: &[String], name: &str) -> bool {
    violations.iter().any(|violation| {
        violation
            .split_once(':')
            .is_some_and(|(kind, _)| kind == name)
    })
}

fn assert_clean(case: Case, seed: u64, clean: &Trial) {
    assert!(
        clean.violations.is_empty(),
        "{} seed {seed} clean twin: {:?}",
        case.name(),
        clean.violations
    );
}

fn assert_clock_assumption(case: Case, trial: &Trial) {
    if matches!(case, Case::ExcessDrift(_)) {
        assert!(catches(&trial.violations, "ClockAssumption"));
    }
}

fn scripted(case: Case) {
    let seed = 1;
    let trial = run_case(case, seed, false, true, false);
    let clean = run_case(case, seed, false, false, false);
    assert_clean(case, seed, &clean);
    eprintln!(
        "Cache fault model {} scripted seed {seed}: {:?}",
        case.name(),
        trial.violations
    );
    assert!(
        catches(&trial.violations, case.invariant()),
        "expected {}: {:?}",
        case.invariant(),
        trial.violations
    );
    assert_clock_assumption(case, &trial);
}

fn search(case: Case) {
    use std::{collections::BTreeMap, sync::Mutex};

    let mut search = PlantedSearch::from_env(case.name(), 1, case.budget(), &[case.invariant()]);
    search.budget = search.budget.min(case.budget());
    eprintln!(
        "Cache fault model {} budget: {}",
        case.name(),
        search.budget
    );
    let clean_failures = Mutex::new(Vec::new());
    let witnesses = Mutex::new(BTreeMap::new());
    let found = search
        .run(|seed| {
            let clean = run_case(case, seed, true, false, false);
            if !clean.violations.is_empty() {
                clean_failures
                    .lock()
                    .unwrap()
                    .push((seed, clean.violations));
                return Vec::new();
            }
            let trial = run_case(case, seed, true, true, false);
            if catches(&trial.violations, case.invariant()) {
                assert_clock_assumption(case, &trial);
                let violations = trial.violations.clone();
                witnesses.lock().unwrap().insert(seed, (trial, clean));
                return violations;
            }
            trial.violations
        })
        .unwrap();
    let clean_failures = clean_failures.into_inner().unwrap();
    assert!(clean_failures.is_empty(), "clean twins: {clean_failures:?}");
    assert!(found.tried <= case.budget());

    let seed = found.seed;
    let (trial, clean) = witnesses.into_inner().unwrap().remove(&seed).unwrap();
    assert_replay(seed, &trial, &run_case(case, seed, true, true, false));
    assert_replay(seed, &clean, &run_case(case, seed, true, false, false));
    println!(
        "Cache fault model {} seed {seed} budget {} tried {} invariant {} trace {:016x} clean {:016x}",
        case.name(),
        search.budget,
        found.tried,
        case.invariant(),
        trial.trace_hash,
        clean.trace_hash
    );
}

fn assert_replay(seed: u64, trial: &Trial, replay: &Trial) {
    assert_eq!(
        trial.trace_hash, replay.trace_hash,
        "seed {seed} trace hash"
    );
    assert_eq!(
        trial.trace_events, replay.trace_events,
        "seed {seed} trace events"
    );
    assert_eq!(trial.history, replay.history, "seed {seed} history");
    assert_eq!(trial.violations, replay.violations, "seed {seed} verdict");
}

#[test]
fn planted_lease_ignores_error_bound_scripted() {
    scripted(Case::BoundIgnored);
}
#[test]
fn planted_replica_keeps_deleted_keys_scripted() {
    scripted(Case::DeleteIgnored);
}
#[test]
fn planted_replica_keeps_synthesized_expiry_delete_scripted() {
    scripted(Case::ExpiryDeleteIgnored);
}
#[test]
fn planted_owner_change_keeps_epoch_scripted() {
    scripted(Case::EpochReused);
}
#[test]
fn planted_cold_reset_ignores_live_lease_scripted() {
    scripted(Case::LiveReset);
}
#[test]
fn drift_beyond_bound_scripted() {
    scripted(Case::ExcessDrift(-1000));
}
#[test]
fn drift_sensitivity_scripted() {
    let seed = 1;
    for ppm in [-20, -100, -500_000] {
        let trial = run_case(Case::ExcessDrift(ppm), seed, false, true, false);
        println!(
            "Cache fault model E1 sensitivity seed {seed}, {ppm} ppm, trace {:016x}, {:?}",
            trial.trace_hash, trial.violations,
        );
        assert!(catches(&trial.violations, "ClockAssumption"));
        assert_eq!(
            catches(&trial.violations, "LeaseExclusive"),
            ppm == -500_000
        );
    }
}
#[test]
fn local_docker_skew_scripted() {
    scripted(Case::LocalSkew);
}

#[test]
fn planted_lease_ignores_error_bound_search() {
    search(Case::BoundIgnored);
}
#[test]
fn planted_replica_keeps_deleted_keys_search() {
    search(Case::DeleteIgnored);
}
#[test]
fn planted_owner_change_keeps_epoch_search() {
    search(Case::EpochReused);
}
#[test]
fn planted_cold_reset_ignores_live_lease_search() {
    search(Case::LiveReset);
}
#[test]
fn drift_beyond_bound_search() {
    search(Case::ExcessDrift(-1000));
}
#[test]
fn local_docker_skew_search() {
    search(Case::LocalSkew);
}

#[test]
fn planted_replica_delete_history() {
    let trial = run_case(Case::DeleteIgnored, 1, false, true, true);
    assert!(
        catches(&trial.violations, "resurrection"),
        "{:?}",
        trial.violations
    );
    let clean = run_case(Case::DeleteIgnored, 1, false, false, true);
    assert_clean(Case::DeleteIgnored, 1, &clean);
    if let Some(dir) = std::env::var_os("NABEDRA_SIM_HISTORY_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).expect("create Cache history directory");
        std::fs::write(dir.join("cache-delete-clean.jsonl"), clean.history)
            .expect("write clean delete history");
    }
}
