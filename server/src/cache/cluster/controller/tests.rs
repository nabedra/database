use std::{fs, io::Write, path::PathBuf};

use openraft::{
    EntryPayload, RaftLogReader, StorageError,
    entry::RaftEntry,
    storage::RaftLogStorage,
    testing::log::{StoreBuilder, Suite},
    type_config::{TypeConfigExt, alias::EntryOf},
    vote::RaftLeaderId,
};

use super::super::clock::ClockBounds;
use super::super::protocol::{
    Admin, Frozen, Heartbeat, Member, Move, Progress, ReplicaSeed, Reply, VERSION, WorkerId,
};
use super::super::transport::Endpoint;
use super::state::{Command, State};
use super::{
    RaftConfig,
    log::Log,
    machine::Machine,
    placement::{Observations, SourceFence},
};
use uuid::Uuid;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        Self(
            std::env::temp_dir().join(format!("nabedra-cache-test-{}", nabedra_rt::ids::uuid_v4())),
        )
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Builder;

impl StoreBuilder<RaftConfig, Log, Machine, Directory> for Builder {
    async fn build(&self) -> Result<(Directory, Log, Machine), StorageError<RaftConfig>> {
        let dir = Directory::new();
        let err = |error| StorageError::read(RaftConfig::err_from_error(&error));
        let log = Log::open(&dir.0).map_err(err)?;
        let machine = Machine::open(&dir.0).map_err(err)?;
        Ok((dir, log, machine))
    }
}

#[tokio::test]
async fn k03_openraft_storage_contract() {
    Suite::test_all(Builder).await.unwrap();
}

#[tokio::test]
async fn k03_partial_tail_recovery_preserves_committed_log_and_vote() {
    let dir = Directory::new();
    let mut log = Log::open(&dir.0).unwrap();
    let id = openraft::LogId::new(openraft::impls::leader_id_adv::LeaderId::new(1, 3), 0);
    let vote = openraft::Vote::new(2, 3);
    log.save_vote(&vote).await.unwrap();
    let entry = EntryOf::<RaftConfig>::new(id, EntryPayload::Blank);
    // The helper waits for the real storage completion callback.
    openraft::storage::RaftLogStorageExt::blocking_append(&mut log, [entry])
        .await
        .unwrap();
    log.save_committed(Some(id)).await.unwrap();
    let path = dir.0.join("topology.wal");
    let valid = fs::metadata(&path).unwrap().len();
    drop(log);

    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[0, 0, 0, 63, 0xaa, 0xbb])
        .unwrap();
    let mut opened = Log::open(&dir.0).unwrap();
    assert_eq!(fs::metadata(path).unwrap().len(), valid);
    assert_eq!(opened.read_vote().await.unwrap(), Some(vote));
    assert_eq!(opened.read_committed().await.unwrap(), Some(id));
    assert_eq!(opened.get_log_state().await.unwrap().last_log_id, Some(id));
    assert_eq!(opened.try_get_log_entries(..).await.unwrap().len(), 1);
}

fn make_member(node: u64, boot: u128) -> Member {
    Member {
        id: WorkerId {
            node,
            incarnation: Uuid::from_u128(boot),
        },
        zone: format!("zone-{node}"),
        client_host: format!("w{node}.cache.test"),
        client_port: 6379,
        peer: Endpoint {
            host: format!("w{node}.cache.test"),
            port: 7402,
            fingerprints: vec![[node as u8; 32]],
        },
    }
}

fn admin(state: &mut State, id: u128, action: Admin, lower_ms: u64) -> Reply {
    state
        .apply(
            Command::Admin {
                id: Uuid::from_u128(id),
                action,
                lower_ms,
            },
            true,
        )
        .unwrap()
}

fn enroll(state: &mut State, node: u64) {
    assert!(matches!(
        admin(
            state,
            1_000 + u128::from(node),
            Admin::Enroll {
                member: make_member(node, 0),
            },
            0
        ),
        Reply::Applied { .. }
    ));
    assert!(matches!(
        state.apply(
            Command::Enroll {
                member: make_member(node, 1),
                certificate: [node as u8; 32],
            },
            true
        ),
        Some(Reply::Applied { .. })
    ));
}

fn fixture() -> State {
    let mut state = State::default();
    for node in 1..=4 {
        enroll(&mut state, node);
    }
    let reply = admin(&mut state, 1_000, Admin::Init { groups: 3 }, 0);
    assert!(matches!(reply, Reply::Applied { .. }));
    state
}

fn bounds(ms: u64) -> ClockBounds {
    ClockBounds {
        lower_ms: ms,
        upper_ms: ms,
    }
}

fn report(
    obs: &mut Observations,
    state: &State,
    worker: WorkerId,
    ms: u64,
    progress: Option<Progress>,
) {
    obs.record(
        &state.topology,
        Heartbeat {
            worker,
            version: VERSION,
            generation: state.topology.generation,
            revision: state.topology.revision,
            progress: progress.into_iter().collect(),
        },
        ms,
    );
}

fn renew(state: &mut State, worker: WorkerId, expires_ms: u64) {
    let reply = state.apply(
        Command::Renew {
            generation: state.topology.generation,
            revision: state.topology.revision,
            worker,
            expires_ms,
            staged_progress: Vec::new(),
        },
        true,
    );
    assert!(matches!(reply, Some(Reply::Update(_))));
}

#[test]
fn k03_u03_failover_fences_latest_committed_lease_and_requires_baseline() {
    let mut state = fixture();
    let primary = state.topology.slots[0].primary.unwrap();
    let replica = state.topology.slots[0].replica.unwrap();
    renew(&mut state, primary, 4_000);
    let unrelated = state.topology.slots[10_000];

    let mut observed = Observations::default();
    let mut status = Progress {
        slot: 0,
        epoch: 1,
        sequence: 23,
        baseline: false,
        serving_primary: false,
        frozen: None,
    };
    report(&mut observed, &state, replica, 3_500, Some(status));
    assert!(observed.plan(&state, bounds(4_001)).is_empty());
    status.baseline = true;
    report(&mut observed, &state, replica, 3_700, Some(status));
    assert!(observed.plan(&state, bounds(3_999)).is_empty());
    let stale = observed.plan(&state, bounds(4_000));
    assert!(!stale.is_empty());

    renew(&mut state, primary, 5_200);
    assert!(!state.reconcile(4_000, stale));
    assert!(observed.plan(&state, bounds(5_199)).is_empty());
    let allowed = observed.plan(&state, bounds(5_200));
    assert!(state.reconcile(5_200, allowed));
    let updated = state.topology.slots[0];
    assert_eq!(updated.primary, Some(replica));
    assert_eq!(updated.epoch, 2);
    assert!(!updated.cold);
    assert_eq!(updated.lease.unwrap().not_before_ms, 5_200);
    assert_eq!(state.topology.slots[10_000], unrelated);
}

#[test]
fn k03_readiness_also_requires_the_primary_to_observe_local_authority() {
    let mut state = fixture();
    let primary = state.topology.slots[0].primary.unwrap();
    renew(&mut state, primary, 5_000);
    let mut observed = Observations::default();
    let mut progress = Progress {
        slot: 0,
        epoch: 1,
        sequence: 0,
        baseline: true,
        serving_primary: false,
        frozen: None,
    };
    report(&mut observed, &state, primary, 3_000, Some(progress));
    assert_eq!(observed.ready(&state.topology, bounds(3_000)).0, 0);
    progress.serving_primary = true;
    report(&mut observed, &state, primary, 3_100, Some(progress));
    assert_eq!(observed.ready(&state.topology, bounds(3_100)).0, 1);
}

#[test]
fn k03_expired_freeze_promotes_the_replica_even_if_source_keeps_heartbeating() {
    let mut state = fixture();
    let primary = state.topology.slots[0].primary.unwrap();
    let replica = state.topology.slots[0].replica.unwrap();
    let target = make_member(4, 1).id;
    renew(&mut state, primary, 8_000);
    state.topology.slots[0].moving = Some(Move {
        operation: Uuid::from_u128(99),
        target,
        replica_only: false,
        freeze_requested: true,
        frozen: None,
    });
    let mut observed = Observations::default();
    let progress = Progress {
        slot: 0,
        epoch: 1,
        sequence: 10,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    report(&mut observed, &state, primary, 7_900, Some(progress));
    report(&mut observed, &state, replica, 7_900, Some(progress));
    let early = observed.plan(&state, bounds(7_999));
    assert!(early.is_empty());
    let fenced = observed.plan(&state, bounds(8_000));
    assert!(state.reconcile(8_000, fenced));
    assert_eq!(state.topology.slots[0].primary, Some(replica));
    assert_eq!(state.topology.slots[0].epoch, 2);
}

#[test]
fn k03_online_move_requires_freeze_and_exact_target_prefix() {
    let mut state = fixture();
    let source = state.topology.slots[0].primary.unwrap();
    let target = make_member(4, 1).id;
    let prior_replica = state.topology.slots[0].replica.unwrap();
    renew(&mut state, source, 8_000);
    assert!(matches!(
        admin(
            &mut state,
            9,
            Admin::Rebalance {
                first: 0,
                last: 0,
                target: target.node
            },
            0
        ),
        Reply::Applied { .. }
    ));
    let mut observed = Observations::default();
    let source_progress = Progress {
        slot: 0,
        epoch: 1,
        sequence: 27,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    report(&mut observed, &state, source, 6_000, Some(source_progress));
    report(
        &mut observed,
        &state,
        prior_replica,
        6_000,
        Some(source_progress),
    );
    report(&mut observed, &state, target, 6_000, None);
    let changes = observed.plan(&state, bounds(6_000));
    assert!(state.reconcile(6_000, changes));
    assert!(!state.topology.slots[0].moving.unwrap().freeze_requested);

    let mut target_progress = Progress {
        sequence: 24,
        ..source_progress
    };
    report(&mut observed, &state, target, 6_100, Some(target_progress));
    let changes = observed.plan(&state, bounds(6_100));
    assert!(state.reconcile(6_100, changes));
    assert!(state.topology.slots[0].moving.unwrap().freeze_requested);
    renew(&mut state, source, 9_000);
    assert_eq!(state.topology.slots[0].lease.unwrap().expires_ms, 8_000);

    let frozen = Frozen {
        lease_expires_ms: 8_000,
        sequence: 27,
        upper_ms: 6_200,
    };
    report(
        &mut observed,
        &state,
        source,
        6_200,
        Some(Progress {
            frozen: Some(frozen),
            ..source_progress
        }),
    );
    let changes = observed.plan(&state, bounds(6_200));
    assert!(state.reconcile(6_200, changes));
    assert!(observed.plan(&state, bounds(6_201)).is_empty());
    target_progress.sequence = 27;
    report(&mut observed, &state, target, 6_210, Some(target_progress));
    let changes = observed.plan(&state, bounds(6_210));
    assert!(state.reconcile(6_210, changes));
    let slot = state.topology.slots[0];
    assert_eq!(slot.primary, Some(target));
    assert_eq!(slot.replica, Some(source));
    assert_eq!(slot.epoch, 2);
    assert_eq!(slot.lease.unwrap().not_before_ms, frozen.upper_ms);
    assert_eq!(
        slot.replica_seed,
        Some(ReplicaSeed {
            previous_primary: source,
            previous_epoch: 1,
            sequence: 27,
        })
    );
}

#[test]
fn k03_restart_rejects_retired_incarnation_and_scoped_reset_requires_fence() {
    let mut state = fixture();
    let before = state.topology.slots[0];
    let primary = before.primary.unwrap();
    renew(&mut state, primary, 4_000);
    let valid = state.apply(
        Command::Enroll {
            member: make_member(primary.node, 2),
            certificate: [primary.node as u8; 32],
        },
        true,
    );
    assert!(matches!(valid, Some(Reply::Applied { .. })));
    let stale = state.apply(
        Command::Enroll {
            member: make_member(primary.node, 1),
            certificate: [primary.node as u8; 32],
        },
        true,
    );
    assert!(matches!(stale, Some(Reply::Refused(_))));
    assert!(matches!(
        admin(&mut state, 9, Admin::ColdReset { slot: Some(0) }, 3_999),
        Reply::Refused(_)
    ));

    let reset = admin(&mut state, 9, Admin::ColdReset { slot: Some(0) }, 4_000);
    assert!(matches!(reset, Reply::Applied { .. }));
    let slot = state.topology.slots[0];
    assert_eq!(slot.epoch, 2);
    assert!(slot.cold);
    assert_eq!(slot.primary, Some(make_member(primary.node, 2).id));
    assert!(matches!(
        admin(&mut state, 9, Admin::ColdReset { slot: Some(0) }, 10_000),
        Reply::Applied { .. }
    ));
    assert_eq!(state.topology.slots[0], slot);
    assert_eq!(state.topology.generation, 1);
    assert_eq!(state.topology.slots[1].epoch, 1);
}

#[test]
fn k03_repairs_a_replica_after_its_incarnation_retires() {
    let mut state = fixture();
    let initial = state.topology.slots[0];
    let primary = initial.primary.unwrap();
    let retired = initial.replica.unwrap();
    let restarted = make_member(retired.node, 2);
    assert!(matches!(
        state.apply(
            Command::Enroll {
                member: restarted.clone(),
                certificate: [retired.node as u8; 32],
            },
            true,
        ),
        Some(Reply::Applied { .. })
    ));
    assert!(state.topology.member(retired).is_none());

    let mut observed = Observations::default();
    let baseline = Progress {
        slot: 0,
        epoch: initial.epoch,
        sequence: 31,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    report(&mut observed, &state, primary, 1_000, Some(baseline));
    report(&mut observed, &state, restarted.id, 1_000, None);
    let changes = observed.plan(&state, bounds(1_000));
    assert!(state.reconcile(1_000, changes));
    let staged = state.topology.slots[0];
    assert_eq!(staged.replica, Some(retired));
    assert_eq!(staged.moving.unwrap().target, restarted.id);
    assert!(staged.moving.unwrap().replica_only);
    assert!(observed.plan(&state, bounds(1_010)).is_empty());

    report(&mut observed, &state, restarted.id, 1_020, Some(baseline));
    let changes = observed.plan(&state, bounds(1_020));
    assert!(state.reconcile(1_020, changes));
    assert_eq!(state.topology.slots[0].primary, Some(primary));
    assert_eq!(state.topology.slots[0].replica, Some(restarted.id));
    assert_eq!(state.topology.slots[0].epoch, initial.epoch);
    assert!(state.topology.slots[0].moving.is_none());
}

#[test]
fn k03_retired_source_does_not_hold_replica_move_capacity() {
    use super::placement::MAX_MOVES_PER_WORKER;

    let mut state = fixture();
    let retired = make_member(1, 1).id;
    let healthy = make_member(3, 1).id;
    let target = make_member(2, 1).id;
    for slot in &mut state.topology.slots[..MAX_MOVES_PER_WORKER] {
        assert_eq!(slot.primary, Some(retired));
        slot.moving = Some(Move {
            operation: Uuid::from_u128(3),
            target: healthy,
            freeze_requested: false,
            frozen: None,
            replica_only: true,
        });
    }
    let index = state
        .topology
        .slots
        .iter()
        .position(|slot| slot.primary == Some(healthy) && slot.replica == Some(retired))
        .expect("a healthy source with the retired replica");
    let restarted = make_member(retired.node, 2);
    assert!(matches!(
        state.apply(
            Command::Enroll {
                member: restarted,
                certificate: [1; 32]
            },
            true,
        ),
        Some(Reply::Applied { .. })
    ));
    assert!(state.topology.member(retired).is_none());

    let mut observed = Observations::default();
    report(
        &mut observed,
        &state,
        healthy,
        1_000,
        Some(Progress {
            slot: index as u16,
            epoch: state.topology.slots[index].epoch,
            sequence: 0,
            baseline: true,
            serving_primary: true,
            frozen: None,
        }),
    );
    report(&mut observed, &state, target, 1_000, None);
    assert!(state.reconcile(1_000, observed.plan(&state, bounds(1_000))));
    let repair = state.topology.slots[index].moving.unwrap();
    assert!(repair.replica_only);
    assert_eq!(repair.target, target);
}

#[test]
fn k03_replaces_a_retired_replica_before_resuming_same_zone_rebalance() {
    let mut state = fixture();
    let original = state.topology.slots[0];
    let primary = original.primary.unwrap();
    let retired = original.replica.unwrap();
    let target = make_member(4, 1).id;
    state.topology.workers.get_mut(&target.node).unwrap().zone =
        state.topology.member(primary).unwrap().zone.clone();
    assert!(matches!(
        admin(
            &mut state,
            9,
            Admin::Rebalance {
                first: 0,
                last: 0,
                target: target.node
            },
            0
        ),
        Reply::Applied { .. }
    ));
    let mut observed = Observations::default();
    let baseline = Progress {
        slot: 0,
        epoch: original.epoch,
        sequence: 2,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    report(&mut observed, &state, primary, 1_000, Some(baseline));
    report(&mut observed, &state, retired, 1_000, Some(baseline));
    report(&mut observed, &state, target, 1_000, None);
    assert!(state.reconcile(1_000, observed.plan(&state, bounds(1_000))));
    assert_eq!(state.topology.slots[0].moving.unwrap().target, target);

    let restarted = make_member(retired.node, 2);
    assert!(matches!(
        state.apply(
            Command::Enroll {
                member: restarted.clone(),
                certificate: [retired.node as u8; 32],
            },
            true,
        ),
        Some(Reply::Applied { .. })
    ));
    report(&mut observed, &state, restarted.id, 1_010, None);
    report(&mut observed, &state, target, 1_010, Some(baseline));
    assert!(state.reconcile(1_010, observed.plan(&state, bounds(1_010))));
    assert!(state.topology.slots[0].moving.is_none());
    assert_eq!(state.topology.slots[0].primary, Some(primary));

    assert!(state.reconcile(1_020, observed.plan(&state, bounds(1_020))));
    let repair = state.topology.slots[0].moving.unwrap();
    assert!(repair.replica_only);
    assert_eq!(repair.target, restarted.id);
    report(&mut observed, &state, restarted.id, 1_030, Some(baseline));
    assert!(state.reconcile(1_030, observed.plan(&state, bounds(1_030))));
    assert_eq!(state.topology.slots[0].replica, Some(restarted.id));

    assert!(state.reconcile(1_040, observed.plan(&state, bounds(1_040))));
    let move_again = state.topology.slots[0].moving.unwrap();
    assert!(!move_again.replica_only);
    assert_eq!(move_again.target, target);
}

#[test]
fn k03_drain_keeps_current_replica_until_verified_replacement() {
    let mut state = fixture();
    let slot = state.topology.slots[0];
    let primary = slot.primary.unwrap();
    let draining = slot.replica.unwrap();
    assert!(matches!(
        admin(
            &mut state,
            17,
            Admin::Drain {
                worker: draining.node
            },
            0
        ),
        Reply::Applied { .. }
    ));
    renew(&mut state, primary, 4_000);
    let mut observed = Observations::default();
    let primary_progress = Progress {
        slot: 0,
        epoch: 1,
        sequence: 31,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    report(
        &mut observed,
        &state,
        primary,
        1_000,
        Some(primary_progress),
    );
    report(
        &mut observed,
        &state,
        draining,
        1_000,
        Some(primary_progress),
    );
    let target = make_member(4, 1).id;
    report(&mut observed, &state, target, 1_000, None);
    let changes = observed.plan(&state, bounds(1_000));
    assert!(state.reconcile(1_000, changes));
    let staged = state.topology.slots[0];
    assert_eq!(staged.replica, Some(draining));
    let intent = staged.moving.unwrap();
    assert_eq!(intent.target, target);
    assert!(intent.replica_only);
    assert!(!intent.freeze_requested);

    let report_base = Progress {
        sequence: 29,
        baseline: false,
        serving_primary: false,
        ..primary_progress
    };
    report(&mut observed, &state, target, 1_010, Some(report_base));
    let changes = observed.plan(&state, bounds(1_010));
    let _ = state.reconcile(1_010, changes);
    assert_eq!(state.topology.slots[0], staged);
    report(
        &mut observed,
        &state,
        target,
        1_020,
        Some(Progress {
            baseline: true,
            serving_primary: true,
            ..report_base
        }),
    );
    let changes = observed.plan(&state, bounds(1_020));
    assert!(state.reconcile(1_020, changes));
    let completed = state.topology.slots[0];
    assert_eq!(completed.epoch, 1);
    assert_eq!(completed.primary, Some(primary));
    assert_eq!(completed.replica, Some(target));
    assert!(completed.moving.is_none());
    assert!(!state.topology.drained.contains(&draining.node));
}

#[test]
fn k03_stalled_move_resumes_source_under_a_new_epoch_only_after_fence() {
    let mut state = fixture();
    let source = state.topology.slots[0].primary.unwrap();
    let target = make_member(4, 1).id;
    renew(&mut state, source, 4_000);
    let frozen = Frozen {
        lease_expires_ms: 4_000,
        sequence: 100,
        upper_ms: 1_700,
    };
    let slot = &mut state.topology.slots[0];
    slot.moving = Some(super::super::protocol::Move {
        operation: Uuid::from_u128(9),
        target,
        freeze_requested: true,
        frozen: Some(frozen),
        replica_only: false,
    });
    let mut observed = Observations::default();
    report(
        &mut observed,
        &state,
        source,
        3_700,
        Some(Progress {
            slot: 0,
            epoch: 1,
            sequence: 100,
            baseline: true,
            serving_primary: true,
            frozen: Some(frozen),
        }),
    );
    report(
        &mut observed,
        &state,
        target,
        3_700,
        Some(Progress {
            slot: 0,
            epoch: 1,
            sequence: 99,
            baseline: true,
            serving_primary: true,
            frozen: None,
        }),
    );
    let prior = state.topology.slots[0];
    let changes = observed.plan(&state, bounds(3_999));
    let _ = state.reconcile(3_999, changes);
    assert_eq!(state.topology.slots[0], prior);
    let changes = observed.plan(&state, bounds(4_000));
    assert!(state.reconcile(4_000, changes));
    let resumed = state.topology.slots[0];
    assert_eq!(resumed.primary, Some(source));
    assert_eq!(resumed.epoch, 2);
    assert!(!resumed.cold);
    assert_eq!(resumed.lease.unwrap().not_before_ms, 4_000);
    assert!(resumed.moving.is_none());
}

#[test]
fn k03_new_controller_waits_for_the_first_worker_reports() {
    use super::placement::Change;

    let mut state = fixture();
    let slot = state.topology.slots[0];
    let primary = slot.primary.unwrap();
    let replica = slot.replica.unwrap();
    renew(&mut state, primary, 4_000);
    let progress = Progress {
        slot: 0,
        epoch: slot.epoch,
        sequence: 20,
        baseline: true,
        serving_primary: false,
        frozen: None,
    };

    let slot_plan = |observations: &mut Observations, at| {
        observations
            .plan(&state, bounds(at))
            .into_iter()
            .find_map(|change| {
                if let Change::Slot { index: 0, next, .. } = change {
                    Some(next)
                } else {
                    None
                }
            })
    };
    let mut failover = Observations::default();
    failover.reset_if_changed(2, state.topology.generation, bounds(4_000));
    report(&mut failover, &state, replica, 4_100, Some(progress));
    assert!(
        slot_plan(&mut failover, 6_999).is_none(),
        "a leader proposed failover before the primary's first report window elapsed"
    );
    assert_eq!(
        slot_plan(&mut failover, 7_000).unwrap().primary,
        Some(replica)
    );

    let replacement = state
        .topology
        .workers
        .values()
        .find(|member| {
            member.id != replica && member.zone != state.topology.member(primary).unwrap().zone
        })
        .unwrap()
        .id;
    let mut rebuild = Observations::default();
    rebuild.reset_if_changed(2, state.topology.generation, bounds(4_000));
    report(
        &mut rebuild,
        &state,
        primary,
        6_950,
        Some(Progress {
            serving_primary: true,
            ..progress
        }),
    );
    report(&mut rebuild, &state, replacement, 6_950, None);
    assert!(
        slot_plan(&mut rebuild, 6_999).is_none(),
        "a leader proposed replacement before the replica's first report window elapsed"
    );
    report(&mut rebuild, &state, replica, 6_999, Some(progress));
    assert!(
        slot_plan(&mut rebuild, 7_001).is_none(),
        "a late first report must age from its receipt time"
    );
}

#[test]
fn k03_staged_promotion_checks_fence_lag_and_copy_at_apply() {
    use super::placement::{Change, MAX_CUTOVER_LAG};

    let mut state = fixture();
    let source = state.topology.slots[0].primary.unwrap();
    state.topology.slots[0].replica = None;
    let target = make_member(4, 1).id;
    let report_source = Progress {
        slot: 0,
        epoch: 1,
        sequence: 100,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    let mut observations = Observations::default();
    renew(&mut state, source, 4_000);
    report(
        &mut observations,
        &state,
        source,
        1_000,
        Some(report_source),
    );
    report(&mut observations, &state, target, 1_000, None);
    let before_staging_revision = state.topology.revision;
    let changes = observations.plan(&state, bounds(1_000));
    assert!(state.reconcile(1_000, changes));
    let staged = state.topology.slots[0];
    assert_eq!(staged.moving.unwrap().target, target);

    // Commit a higher source watermark while staging, then take over with
    // a fresh observation cache and just the state machine's saved state.
    state.apply(
        Command::Renew {
            generation: state.topology.generation,
            revision: state.topology.revision,
            worker: source,
            expires_ms: 5_000,
            staged_progress: vec![Progress {
                sequence: 120,
                ..report_source
            }],
        },
        false,
    );
    let mut state: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    let mut observations = Observations::default();
    let copy = Progress {
        sequence: 120 - MAX_CUTOVER_LAG,
        serving_primary: false,
        ..report_source
    };
    // A delayed report can describe a copy that was discarded before this
    // transfer was installed, even if the source and slot epoch are unchanged.
    observations.record(
        &state.topology,
        Heartbeat {
            worker: target,
            version: VERSION,
            generation: state.topology.generation,
            revision: before_staging_revision,
            progress: vec![copy],
        },
        4_999,
    );
    let changes = observations.plan(&state, bounds(5_000));
    assert!(!state.reconcile(5_000, changes));

    report(&mut observations, &state, target, 4_999, Some(copy));
    let changes = observations.plan(&state, bounds(4_999));
    assert!(!state.reconcile(4_999, changes));
    let changes = observations.plan(&state, bounds(5_000));
    let promotion = changes
        .into_iter()
        .find(|change| {
            matches!(
                change,
                Change::Slot {
                    index: 0,
                    staged: Some(_),
                    ..
                }
            )
        })
        .expect("eligible staged promotion");

    let mut bad = promotion.clone();
    if let Change::Slot {
        staged: Some(ref mut proof),
        ..
    } = bad
    {
        proof.retained_sequence += 1;
    }
    assert!(!state.reconcile(5_000, vec![bad]));
    let mut stale_proof = promotion.clone();
    if let Change::Slot {
        staged: Some(ref mut proof),
        ..
    } = stale_proof
    {
        proof.target_revision = before_staging_revision;
    }
    assert!(!state.reconcile(5_000, vec![stale_proof]));
    assert!(!state.reconcile(4_999, vec![promotion.clone()]));

    // The prior deadline has passed, but a new leader needs time to hear
    // from the still-verified target before it can discard this epoch.
    let mut new_leader = Observations::default();
    new_leader.reset_if_changed(2, state.topology.generation, bounds(8_000));
    report(&mut new_leader, &state, make_member(3, 1).id, 8_001, None);
    let changes = new_leader.plan(&state, bounds(8_001));
    assert!(!state.clone().reconcile(8_001, changes));
    report(&mut new_leader, &state, target, 8_002, Some(copy));
    let changes = new_leader.plan(&state, bounds(8_002));
    let mut recovered = state.clone();
    assert!(recovered.reconcile(8_002, changes));
    assert_eq!(recovered.topology.slots[0].primary, Some(target));
    assert!(!recovered.topology.slots[0].cold);

    let mut no_copy = state.clone();
    let mut new_leader = Observations::default();
    new_leader.reset_if_changed(2, state.topology.generation, bounds(8_000));
    report(&mut new_leader, &state, make_member(3, 1).id, 10_999, None);
    let changes = new_leader.plan(&no_copy, bounds(10_999));
    assert!(!no_copy.reconcile(10_999, changes));
    let changes = new_leader.plan(&no_copy, bounds(11_000));
    assert!(no_copy.reconcile(11_000, changes));
    assert!(no_copy.topology.slots[0].cold);

    let mut lagged = state.clone();
    let mut late = Observations::default();
    report(
        &mut late,
        &lagged,
        target,
        7_999,
        Some(Progress {
            sequence: copy.sequence - 1,
            ..copy
        }),
    );
    let changes = late.plan(&lagged, bounds(7_999));
    assert!(!lagged.reconcile(7_999, changes));
    let changes = late.plan(&lagged, bounds(8_000));
    assert!(lagged.reconcile(8_000, changes));
    let reset = lagged.topology.slots[0];
    assert!(reset.cold);
    assert_eq!(reset.epoch, 2);
    assert!(reset.lease.is_none());
    assert!(reset.moving.is_none());

    // A new leader must defer staging promotion while the retired source
    // can still acknowledge writes under its existing lease.
    let mut retired = state.clone();
    assert!(matches!(
        retired.apply(
            Command::Enroll {
                member: make_member(source.node, 2),
                certificate: [source.node as u8; 32],
            },
            true
        ),
        Some(Reply::Applied { .. })
    ));
    let mut observations = Observations::default();
    report(&mut observations, &retired, target, 4_000, Some(copy));
    let changes = observations.plan(&retired, bounds(4_000));
    assert!(!retired.reconcile(4_000, changes));
    assert_eq!(retired.topology.slots[0], staged);

    // Previously committed reconcile entries retain their apply semantics.
    let mut legacy = retired.clone();
    let mut legacy_promotion = promotion.clone();
    let Change::Slot {
        staged: Some(proof),
        ..
    } = &mut legacy_promotion
    else {
        panic!("expected staged promotion");
    };
    proof.fence = SourceFence::Retired;
    assert!(legacy.reconcile(4_000, vec![legacy_promotion]));
    assert_eq!(legacy.topology.slots[0].primary, Some(target));

    let changes = observations.plan(&retired, bounds(5_000));
    assert!(retired.reconcile(5_000, changes));
    let next = retired.topology.slots[0];
    assert_eq!(next.primary, Some(target));
    assert_eq!(next.epoch, 2);
    assert_eq!(next.lease.unwrap().not_before_ms, 5_000);

    assert!(state.reconcile(5_000, vec![promotion]));
    assert_eq!(state.topology.slots[0], next);
}

/// A slot staging a copy on a target whose older copy was discarded, the
/// revision before that transfer, and the copy the target once reported.
fn staging_fixture() -> (State, WorkerId, WorkerId, u64, Progress) {
    let mut state = fixture();
    let source = state.topology.slots[0].primary.unwrap();
    state.topology.slots[0].replica = None;
    let target = make_member(4, 1).id;
    let copy = Progress {
        slot: 0,
        epoch: 1,
        sequence: 100,
        baseline: true,
        serving_primary: true,
        frozen: None,
    };
    let mut observations = Observations::default();
    renew(&mut state, source, 4_000);
    report(&mut observations, &state, source, 1_000, Some(copy));
    report(&mut observations, &state, target, 1_000, None);
    let before = state.topology.revision;
    let changes = observations.plan(&state, bounds(1_000));
    assert!(state.reconcile(1_000, changes));
    let moving = state.topology.slots[0].moving.unwrap();
    assert!(moving.replica_only && moving.target == target);
    assert!(state.topology.revision > before);
    let copy = Progress {
        serving_primary: false,
        ..copy
    };
    (state, source, target, before, copy)
}

fn report_at(
    obs: &mut Observations,
    state: &State,
    worker: WorkerId,
    revision: u64,
    ms: u64,
    progress: Progress,
) {
    obs.record(
        &state.topology,
        Heartbeat {
            worker,
            version: VERSION,
            generation: state.topology.generation,
            revision,
            progress: vec![progress],
        },
        ms,
    );
}

/// The planner itself ignores a staged copy reported outside this transfer.
/// Apply repeats the check, so only the planned changes show it.
#[test]
fn k03_staged_promotion_planner_requires_a_report_from_this_transfer() {
    use super::placement::Change;

    let (state, _, target, before, copy) = staging_fixture();
    let current = state.topology.revision;
    for (revision, eligible) in [(before, false), (current + 1, false), (current, true)] {
        let mut observations = Observations::default();
        report_at(&mut observations, &state, target, revision, 4_000, copy);
        let planned = observations
            .plan(&state, bounds(4_000))
            .iter()
            .any(|change| {
                matches!(
                    change,
                    Change::Slot {
                        index: 0,
                        staged: Some(_),
                        ..
                    }
                )
            });
        assert_eq!(
            planned, eligible,
            "revision {revision}, staging from {current}"
        );
    }
}

/// Completing a staged replica has no revision check at apply, so the planner
/// alone keeps a discarded copy from becoming the assigned replica.
#[test]
fn k03_staged_completion_requires_a_report_from_this_transfer() {
    let (state, source, target, before, copy) = staging_fixture();
    let current = state.topology.revision;
    let source_copy = Progress {
        serving_primary: true,
        ..copy
    };
    for (revision, eligible) in [(before, false), (current + 1, false), (current, true)] {
        let mut state = state.clone();
        let staged = state.topology.slots[0];
        let mut observations = Observations::default();
        report(&mut observations, &state, source, 1_500, Some(source_copy));
        report_at(&mut observations, &state, target, revision, 1_500, copy);
        let changes = observations.plan(&state, bounds(1_500));
        state.reconcile(1_500, changes);
        let slot = state.topology.slots[0];
        if eligible {
            assert_eq!((slot.replica, slot.moving), (Some(target), None));
            assert_eq!((slot.primary, slot.epoch), (Some(source), 1));
        } else {
            assert_eq!(slot, staged, "revision {revision}, staging from {current}");
        }
    }
}

/// Known to fail. A heartbeat carries no order, so a late older one replaces
/// the source's newer report, and a copy far beyond the lag bound completes.
#[test]
#[ignore = "open bug 2026-11-24-cache-older-source-heartbeat"]
fn k03_older_source_heartbeat_does_not_complete_a_lagging_copy() {
    let (mut state, source, target, _, copy) = staging_fixture();
    let staged = state.topology.slots[0];
    let mut observations = Observations::default();
    let source_at = |sequence| Progress {
        sequence,
        serving_primary: true,
        ..copy
    };
    report(
        &mut observations,
        &state,
        source,
        1_500,
        Some(source_at(1_000)),
    );
    report(&mut observations, &state, target, 1_500, Some(copy));
    let changes = observations.plan(&state, bounds(1_500));
    state.reconcile(1_500, changes);
    assert_eq!(state.topology.slots[0], staged, "lag 900");

    report(
        &mut observations,
        &state,
        source,
        1_501,
        Some(source_at(copy.sequence)),
    );
    let changes = observations.plan(&state, bounds(1_501));
    state.reconcile(1_501, changes);
    assert_eq!(
        state.topology.slots[0], staged,
        "lag 900 hidden by an older source heartbeat"
    );
}

/// The existing controller fixture and placement decisions, without the Raft transport.
#[cfg(feature = "sim")]
pub(in crate::cache::cluster) struct LeaseControllerFixture {
    state: State,
    observed: Observations,
}

#[cfg(feature = "sim")]
impl LeaseControllerFixture {
    pub fn new() -> Self {
        Self {
            state: fixture(),
            observed: Observations::default(),
        }
    }

    pub fn topology(&self) -> &super::super::protocol::Topology {
        &self.state.topology
    }

    pub fn renew(&mut self, worker: WorkerId, now: ClockBounds) -> super::super::protocol::Update {
        use super::super::protocol::LEASE_MS;

        let Some(Reply::Update(update)) = self.state.apply(
            Command::Renew {
                generation: self.state.topology.generation,
                // The fixture asks for the complete topology as well as its grants.
                revision: 0,
                worker,
                staged_progress: Vec::new(),
                expires_ms: now.lower_ms.checked_add(LEASE_MS).unwrap(),
            },
            true,
        ) else {
            panic!("the controller refused the lease renewal for {worker:?}");
        };
        update
    }

    pub fn record(&mut self, worker: WorkerId, now: ClockBounds, progress: Progress) {
        report(
            &mut self.observed,
            &self.state,
            worker,
            now.lower_ms,
            Some(progress),
        );
    }

    pub fn reconcile(&mut self, now: ClockBounds) {
        let changes = self.observed.plan(&self.state, now);
        if !changes.is_empty() {
            let reply = self.state.apply(
                Command::Reconcile {
                    generation: self.state.topology.generation,
                    revision: self.state.topology.revision,
                    lower_ms: now.lower_ms,
                    changes,
                },
                true,
            );
            assert!(matches!(reply, Some(Reply::Applied { .. })));
        }
    }
}
