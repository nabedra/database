use super::*;
use crate::{
    oltp::pool::mount::{PoolMountExpectation, PoolMountProfile},
    oltp_deployment::{controller::*, planner::Action},
    oltp_replication::authority::control::{
        DrainMount, LocalOperatorPermit, PoolDrainPhase, pool_drain::PoolDrainCommand,
    },
};

async fn grow(f: &mut Fixture, command: GrowthCommand) -> ControlReply {
    f.apply(ControlCommand::Growth {
        operation_id: Uuid::new_v4(),
        authorization: authorization(),
        command,
    })
    .await
}

async fn drain(
    f: &mut Fixture,
    worker: WorkerIdentity,
    operation_id: Uuid,
    command: PoolDrainCommand,
) -> ControlReply {
    f.apply(ControlCommand::PoolDrain {
        operation_id,
        worker,
        authorization: AuthorizationStamp {
            authorization_id: operation_id,
            evidence: serialized_digest(&(worker, &command)).unwrap(),
            subject: worker.certificate_sha256,
            ..authorization()
        },
        command,
    })
    .await
}

fn handle(f: &Fixture, local: WorkerIdentity) -> (Arc<ControlHandle>, LocalOperatorPermit) {
    let shared = f.machine.store().resources().clone();
    let permit = LocalOperatorPermit::from_local_bootstrap(local, shared, Arc::new(())).unwrap();
    let model = Arc::new(NativeModelConsensus {
        machine: f.machine.clone(),
        next: AtomicU64::new(f.next),
        reads: AtomicU64::new(0),
    });
    let control = ControlHandle::new(
        f.machine.clone(),
        model,
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: [local.certificate_sha256].into_iter().collect(),
        },
    );
    (control, permit)
}

#[tokio::test]
async fn o17_o26_a02_control_pool_drain_pins_staged_inventory_and_recovers_exact_closed_receipt() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    f.initialize().await;
    let enrollment = worker(pool.worker_id.as_u128());
    let local = WorkerIdentity::from(&enrollment);
    assert_eq!(
        f.apply(ControlCommand::Enroll {
            operation_id: Uuid::new_v4(),
            expected: None,
            next: enrollment,
            authorization: authorization(),
            previous_process: None,
        })
        .await,
        ControlReply::Enrolled(local)
    );
    let other = f.enroll(2).await;
    let mut members = vec![];
    f.add_ready(local, &mut members).await;
    f.add_ready(other, &mut members).await;
    let policy = GrowthPolicy {
        automatic: None,
        policy_id: Uuid::new_v4(),
        volume_cost_per_gib: 1,
        worker_cost: 1,
        initial_monthly_spend: 0,
        monthly_spend_ceiling: 5,
        max_attempts: 1,
        cooldown_seconds: 1,
        max_volume_bytes: 1 << 40,
        zones: vec!["a".into()],
        initial_replica_zones: Default::default(),
    };
    assert!(matches!(
        grow(
            &mut f,
            GrowthCommand::Configure {
                policy: policy.clone()
            }
        )
        .await,
        ControlReply::GrowthConfigured(_)
    ));
    let volume = "docker-volume:retained-test".to_owned();
    let pool_id = Uuid::new_v4();
    let claim_id = Uuid::new_v4();
    let intent = GrowthIntent {
        policy_id: policy.policy_id,
        claim_id,
        action: Action::AddPool {
            worker: local.worker_id,
            zone: "a".into(),
            bytes: 1,
        },
        worker: local,
        pool_id,
        pool_generation: 1,
        quoted_monthly_cost: 1,
        monthly_spend_ceiling: 5,
        max_attempts: 1,
        cooldown_seconds: 1,
    };
    for command in [
        GrowthCommand::Claim {
            intent,
            prior_total_bytes: None,
        },
        GrowthCommand::Attempt { claim_id, now: 100 },
        GrowthCommand::Provisioned {
            claim_id,
            resource_id: volume.clone(),
        },
        GrowthCommand::Mounted {
            claim_id,
            proof: MountedGrowthStamp {
                worker: local,
                pool_id,
                pool_generation: 1,
                provider_volume_id: volume.clone(),
                total_bytes: 1 << 30,
            },
        },
    ] {
        assert!(matches!(
            grow(&mut f, command).await,
            ControlReply::Growth(_)
        ));
    }

    let exact = DrainMount {
        expected: PoolMountExpectation {
            profile: PoolMountProfile::Docker {
                volume_marker: Uuid::new_v4(),
            },
            identity: PoolIdentity {
                pool_id,
                worker_id: local.worker_id,
                ..pool.clone()
            },
            worker: local,
            generation: 1,
            mount_path: "/secondary".into(),
            provider_volume_id: volume,
            filesystem_uuid: String::new(),
        },
        mount_id: 100,
        device_major: 8,
        device_minor: 1,
        root_inode: 10,
    };
    let other_group = GroupIdentity {
        group_id: Uuid::new_v4(),
        kind: GroupKind::Data {
            table_id: Uuid::new_v4(),
            shard_id: Uuid::new_v4(),
        },
        ..scope.clone()
    };
    let staged = Uuid::new_v4();
    assert!(matches!(
        f.apply(ControlCommand::BeginGroup {
            operation_id: Uuid::new_v4(),
            generation: staged,
            identity: other_group.clone(),
            authorization: authorization(),
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
    let id = Uuid::new_v4();
    let began = drain(
        &mut f,
        local,
        id,
        PoolDrainCommand::Begin {
            growth_claim: claim_id,
            mount: Box::new(exact.clone()),
        },
    )
    .await;
    let ControlReply::PoolDrain(record) = began else {
        panic!("{began:?}")
    };
    assert_eq!(record.phase, PoolDrainPhase::Draining);
    let stamp = PoolDrainCommand::Closed {
        id,
        mount: Box::new(exact.clone()),
    };
    assert_eq!(
        drain(&mut f, local, Uuid::new_v4(), stamp.clone()).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    // A new staged member targeting the pool is blocked at Control apply.
    let target = ReplicaMember {
        node_id: 5,
        worker_id: local.worker_id,
        pool_id,
    };
    assert_eq!(
        f.apply(ControlCommand::GroupPage {
            operation_id: Uuid::new_v4(),
            generation: staged,
            replicas: Page::new(vec![target.clone()]).unwrap(),
        })
        .await,
        ControlReply::Rejected(Fault::Busy)
    );

    // The read-only model shares the same native Control snapshot as Fixture.
    // Its policy requires the exact local mount owner for adapter permits.
    let revision = handle(&f, local)
        .0
        .prove_empty_pool_placements(&record)
        .await
        .unwrap();
    let other_replica = ReplicaMember {
        node_id: 6,
        worker_id: other.worker_id,
        pool_id: Uuid::new_v4(),
    };
    assert!(matches!(
        f.apply(ControlCommand::GroupPage {
            operation_id: Uuid::new_v4(),
            generation: staged,
            replicas: Page::new(vec![other_replica.clone()]).unwrap(),
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
    assert_eq!(
        drain(
            &mut f,
            local,
            Uuid::new_v4(),
            PoolDrainCommand::Prepare { id, revision }
        )
        .await,
        ControlReply::Rejected(Fault::Stale)
    );
    assert!(
        matches!(drain(&mut f, local, Uuid::new_v4(), PoolDrainCommand::Cancel { id }).await,
        ControlReply::PoolDrain(cancelled) if cancelled.phase == PoolDrainPhase::Cancelled)
    );
    let staged_other = Uuid::new_v4();
    assert!(matches!(
        f.apply(ControlCommand::BeginGroup {
            operation_id: Uuid::new_v4(),
            generation: staged_other,
            identity: other_group.clone(),
            authorization: authorization(),
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
    assert!(matches!(
        f.apply(ControlCommand::GroupPage {
            operation_id: Uuid::new_v4(),
            generation: staged_other,
            replicas: Page::new(vec![target]).unwrap(),
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
    let second = Uuid::new_v4();
    let result = drain(
        &mut f,
        local,
        second,
        PoolDrainCommand::Begin {
            growth_claim: claim_id,
            mount: Box::new(exact.clone()),
        },
    )
    .await;
    let ControlReply::PoolDrain(record) = result else {
        panic!("{result:?}")
    };
    let (control, permit) = handle(&f, local);
    let blocked = control
        .prove_empty_pool_placements(&record)
        .await
        .unwrap_err();
    assert!(blocked.message.contains("staged replica"), "{blocked:?}");
    assert!(control.pool_drain_blocks_placement(pool_id).await.unwrap());
    assert!(control.pool_detach_permit(&permit, second).await.is_err());
    drop(control);
    drop(permit);
    let authority = f.state().await.ready.view.authority();
    assert!(matches!(
        f.apply(ControlCommand::SealGroup {
            operation_id: Uuid::new_v4(),
            generation: staged,
            proof: GroupMembershipStamp {
                group: other_group,
                generation: staged,
                replica_count: 1,
                replica_root: group_root(1, group_step(group_seed(), &other_replica)),
                applied_at: AppliedPosition {
                    term: 1,
                    node_id: 6,
                    index: 100
                },
                authority,
            },
        })
        .await,
        ControlReply::GroupRegistered { .. }
    ));
    let revision = handle(&f, local)
        .0
        .prove_empty_pool_placements(&record)
        .await
        .unwrap();
    assert!(
        matches!(drain(&mut f, local, Uuid::new_v4(), PoolDrainCommand::Prepare {
        id: second, revision,
    }).await, ControlReply::PoolDrain(closing) if closing.phase == PoolDrainPhase::Closing)
    );
    assert_eq!(
        drain(
            &mut f,
            local,
            Uuid::new_v4(),
            PoolDrainCommand::Cancel { id: second }
        )
        .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    let mut alias = exact.clone();
    alias.root_inode += 1;
    assert_eq!(
        drain(
            &mut f,
            local,
            Uuid::new_v4(),
            PoolDrainCommand::Closed {
                id: second,
                mount: Box::new(alias),
            }
        )
        .await,
        ControlReply::Rejected(Fault::Invalid)
    );
    let completed = drain(
        &mut f,
        local,
        Uuid::new_v4(),
        PoolDrainCommand::Closed {
            id: second,
            mount: Box::new(exact),
        },
    )
    .await;
    let ControlReply::PoolDrain(closed) = completed else {
        panic!("{completed:?}")
    };
    assert_eq!(closed.phase, PoolDrainPhase::Closed);
    drop(f);
    let recovered = Fixture::open(&directory, scope, pool).await;
    let (control, permit) = handle(&recovered, local);
    assert_eq!(
        control
            .pool_detach_permit(&permit, second)
            .await
            .unwrap()
            .record(),
        closed.as_ref()
    );
    assert!(control.pool_drain_blocks_placement(pool_id).await.unwrap());
}
