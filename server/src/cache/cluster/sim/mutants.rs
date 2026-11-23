//! Directed witnesses and seeded searches for the Cache fault model (K03).

use nabedra_check::checker::relaxed_kv;
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
    EpochReused,
    LiveReset,
    ExcessDrift,
    LocalSkew,
    Bounded,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::BoundIgnored => "P1",
            Self::DeleteIgnored => "P2",
            Self::EpochReused => "P3",
            Self::LiveReset => "P4",
            Self::ExcessDrift => "E1",
            Self::LocalSkew => "E2",
            Self::Bounded => "bounded",
        }
    }

    fn invariant(self) -> &'static str {
        match self {
            Self::BoundIgnored
            | Self::LiveReset
            | Self::ExcessDrift
            | Self::LocalSkew
            | Self::Bounded => "LeaseExclusive",
            Self::DeleteIgnored => "ReplicaPrefix",
            Self::EpochReused => "SlotOwnership",
        }
    }

    fn budget(self) -> u64 {
        match self {
            Self::ExcessDrift | Self::LocalSkew => 200,
            _ => 500,
        }
    }

    fn config(self) -> Config {
        Config {
            nodes: 4,
            groups: 3,
            actors: 1,
            slot_bytes: memory_limit(1),
            eviction: EvictionPolicy::NoEviction,
            clock_mode: if self == Self::LocalSkew {
                ClockMode::LocalDocker
            } else {
                ClockMode::ClockBound
            },
            base_error_ns: 4_450_000,
            declared_ppm: 10,
            skew: matches!(
                self,
                Self::BoundIgnored | Self::LocalSkew | Self::Bounded | Self::ExcessDrift
            ),
            gradual_drift: false,
            knobs: Knobs {
                lease_ignores_error_bound: self == Self::BoundIgnored,
                replica_keeps_deleted_keys: self == Self::DeleteIgnored,
                owner_change_keeps_epoch: self == Self::EpochReused,
                cold_reset_ignores_live_lease: self == Self::LiveReset,
            },
            check_replica_prefix: true,
        }
    }
}

fn write_ok(reply: Option<Response>) -> bool {
    matches!(reply, Some(Response::Simple(ref message)) if message.as_ref() == b"OK")
}

fn run_case(case: Case, seed: u64, generated: bool, history_only: bool) -> Vec<String> {
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
        let mut config = case.config();
        config.check_replica_prefix = !history_only;
        let mut cluster = Cluster::boot(world, vec![key.clone()], config).await;
        let before = cluster.slot(slot);
        let old = before.primary.unwrap();
        let replica = before.replica.unwrap();
        assert_ne!(old.node, replica.node);
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("old-value".into(), None), Some(old.node))
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
            assert!(matches!(
                cluster.send(&key, Op::Del, Some(old.node)).await,
                Some(Response::Integer(1))
            ));
            cluster.step(1_000).await;
        }
        if generated {
            cluster.step(choices.below(30)).await;
        }
        cluster.isolate_controller_links(old.node);
        if case == Case::ExcessDrift {
            cluster.drift(old.node, -500_000);
        }
        if case == Case::LiveReset {
            assert!(matches!(
                cluster.admin(Admin::Drain { worker: old.node }).await,
                Reply::Applied { .. }
            ));
            assert!(matches!(
                cluster.admin(Admin::ColdReset { slot: Some(slot) }).await,
                Reply::Applied { .. }
            ));
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
        violations
    })
}

fn catches(violations: &[String], name: &str) -> bool {
    violations.iter().any(|violation| {
        violation
            .split_once(':')
            .is_some_and(|(kind, _)| kind == name)
    })
}

fn scripted(case: Case) {
    let seed = 1;
    let violations = run_case(case, seed, false, false);
    eprintln!(
        "Cache fault model {} scripted seed {seed}: {violations:?}",
        case.name()
    );
    assert!(
        catches(&violations, case.invariant()),
        "expected {}: {violations:?}",
        case.invariant()
    );
    if case == Case::ExcessDrift {
        assert!(catches(&violations, "ClockAssumption"));
        assert!(run_case(Case::Bounded, seed, false, false).is_empty());
    }
}

fn search(case: Case) {
    let mut search = PlantedSearch::from_env(case.name(), 1, case.budget(), &[case.invariant()]);
    search.budget = search.budget.min(case.budget());
    eprintln!(
        "Cache fault model {} budget: {}",
        case.name(),
        search.budget
    );
    let found = search
        .run(|seed| {
            let actual = run_case(case, seed, true, false);
            if case == Case::ExcessDrift {
                let bounded = run_case(Case::Bounded, seed, true, false);
                if !bounded.is_empty() || !catches(&actual, "ClockAssumption") {
                    return vec![format!("Twin: excessive {actual:?}; in-bound {bounded:?}")];
                }
            }
            actual
        })
        .unwrap();
    assert!(found.tried <= case.budget());
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
fn planted_owner_change_keeps_epoch_scripted() {
    scripted(Case::EpochReused);
}
#[test]
fn planted_cold_reset_ignores_live_lease_scripted() {
    scripted(Case::LiveReset);
}
#[test]
fn drift_beyond_bound_scripted() {
    scripted(Case::ExcessDrift);
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
    search(Case::ExcessDrift);
}
#[test]
fn local_docker_skew_search() {
    search(Case::LocalSkew);
}

#[test]
fn planted_replica_delete_history() {
    let violations = run_case(Case::DeleteIgnored, 1, false, true);
    assert!(catches(&violations, "resurrection"), "{violations:?}");
}
