//! Native persisted Control transitions, with explicit proof-model stamps.
//! These tests do not claim live queue transfer, quorum or mount authority.
use super::*;
use crate::oltp_deployment::{controller::*, planner::Action};

async fn growth(f: &mut Fixture, command: GrowthCommand) -> ControlReply {
    f.apply(ControlCommand::Growth {
        operation_id: Uuid::new_v4(),
        authorization: authorization(),
        command,
    })
    .await
}
#[tokio::test]
async fn o27_growth_attempt_uses_trusted_time_and_replays_original_operation_without_advancing() {
    use super::super::service::LocalOperatorPermit;
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope, pool.clone()).await;
    f.initialize().await;
    let enrollment = worker(pool.worker_id.as_u128());
    let local = WorkerIdentity::from(&enrollment);
    f.apply(ControlCommand::Enroll {
        operation_id: Uuid::new_v4(),
        expected: None,
        next: enrollment,
        authorization: authorization(),
        previous_process: None,
    })
    .await;
    let policy = GrowthPolicy {
        automatic: None,
        policy_id: Uuid::new_v4(),
        volume_cost_per_gib: 10,
        worker_cost: 20,
        initial_monthly_spend: 0,
        monthly_spend_ceiling: 20,
        max_attempts: 2,
        cooldown_seconds: 60,
        max_volume_bytes: 1 << 40,
        zones: vec!["a".into()],
        initial_replica_zones: Default::default(),
    };
    growth(
        &mut f,
        GrowthCommand::Configure {
            policy: policy.clone(),
        },
    )
    .await;
    let intent = GrowthIntent {
        policy_id: policy.policy_id,
        claim_id: Uuid::new_v4(),
        action: Action::AddPool {
            worker: local.worker_id,
            zone: "a".into(),
            bytes: 1,
        },
        worker: local,
        pool_id: Uuid::new_v4(),
        pool_generation: 1,
        quoted_monthly_cost: 10,
        monthly_spend_ceiling: 20,
        max_attempts: 2,
        cooldown_seconds: 60,
    };
    growth(
        &mut f,
        GrowthCommand::Claim {
            intent: intent.clone(),
            prior_total_bytes: None,
        },
    )
    .await;
    let shared = f.machine.store().resources().clone();
    shared.bind_worker(local.worker_id).unwrap();
    let permit = LocalOperatorPermit::from_local_bootstrap(local, shared, Arc::new(())).unwrap();
    let model = Arc::new(NativeModelConsensus {
        machine: f.machine.clone(),
        next: AtomicU64::new(f.next),
        reads: AtomicU64::new(0),
    });
    let handle = ControlHandle::new(
        f.machine.clone(),
        model,
        OperatorPolicy {
            policy: authorization().policy,
            certificate_pins: [local.certificate_sha256].into_iter().collect(),
        },
    );
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let operation = Uuid::new_v4();
    let first = handle
        .attempt_growth_local(&permit, operation, intent.claim_id)
        .await
        .unwrap()
        .into_parts()
        .unwrap()
        .1;
    let ControlReply::Growth(record) = &first else {
        panic!("unexpected attempt reply")
    };
    assert_eq!(record.attempts, 1);
    assert!(record.last_attempt.unwrap() >= before);
    assert_eq!(
        handle
            .attempt_growth_local(&permit, Uuid::new_v4(), intent.claim_id)
            .await
            .unwrap()
            .into_parts()
            .unwrap()
            .1,
        ControlReply::Rejected(Fault::Busy)
    );
    assert_eq!(
        handle
            .attempt_growth_local(&permit, operation, intent.claim_id)
            .await
            .unwrap()
            .into_parts()
            .unwrap()
            .1,
        first
    );
    assert!(
        handle
            .attempt_growth_local(&permit, operation, Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(
        handle
            .growth(intent.claim_id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
}
#[tokio::test]
async fn o27_growth_claim_is_durable_idempotent_bounded_and_never_publishes_provider_capacity() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    f.initialize().await;
    let worker = f.enroll(1).await;
    let policy = GrowthPolicy {
        automatic: None,
        policy_id: Uuid::new_v4(),
        volume_cost_per_gib: 10,
        worker_cost: 20,
        initial_monthly_spend: 5,
        monthly_spend_ceiling: 20,
        max_attempts: 2,
        cooldown_seconds: 60,
        max_volume_bytes: 1 << 40,
        zones: vec!["a".into()],
        initial_replica_zones: Default::default(),
    };
    assert!(matches!(
        growth(
            &mut f,
            GrowthCommand::Configure {
                policy: policy.clone()
            }
        )
        .await,
        ControlReply::GrowthConfigured(_)
    ));
    let intent = GrowthIntent {
        policy_id: policy.policy_id,
        claim_id: Uuid::new_v4(),
        action: Action::AddPool {
            worker: worker.worker_id,
            zone: "a".into(),
            bytes: 1024,
        },
        worker,
        pool_id: Uuid::new_v4(),
        pool_generation: 1,
        quoted_monthly_cost: 10,
        monthly_spend_ceiling: 20,
        max_attempts: 2,
        cooldown_seconds: 60,
    };
    let mut forged = intent.clone();
    forged.quoted_monthly_cost = 0;
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: forged,
                prior_total_bytes: None
            }
        )
        .await,
        ControlReply::Rejected(Fault::Invalid)
    );
    let mut forged = intent.clone();
    forged.monthly_spend_ceiling = 9999;
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: forged,
                prior_total_bytes: None
            }
        )
        .await,
        ControlReply::Rejected(Fault::Invalid)
    );
    let first = growth(
        &mut f,
        GrowthCommand::Claim {
            intent: intent.clone(),
            prior_total_bytes: None,
        },
    )
    .await;
    let retry_operation = Uuid::new_v4();
    let command = GrowthCommand::Claim {
        intent: intent.clone(),
        prior_total_bytes: None,
    };
    let original = f
        .apply(ControlCommand::Growth {
            operation_id: retry_operation,
            authorization: authorization(),
            command: command.clone(),
        })
        .await;
    let mut another_operator = authorization();
    another_operator.subject = Digest::from_bytes([9; 32]);
    assert_eq!(
        original,
        f.apply(ControlCommand::Growth {
            operation_id: retry_operation,
            authorization: another_operator,
            command: command.clone()
        })
        .await
    );
    let mut unauthorized = authorization();
    unauthorized.policy = Digest::from_bytes([8; 32]);
    assert_eq!(
        f.apply(ControlCommand::Growth {
            operation_id: retry_operation,
            authorization: unauthorized,
            command
        })
        .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(
        first,
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: intent.clone(),
                prior_total_bytes: None
            }
        )
        .await
    );
    let mut conflict = intent.clone();
    conflict.quoted_monthly_cost = 11;
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: conflict,
                prior_total_bytes: None
            }
        )
        .await,
        ControlReply::Rejected(Fault::Conflict)
    );
    let claim_id = intent.claim_id;
    assert!(matches!(
        growth(&mut f, GrowthCommand::Attempt { claim_id, now: 100 }).await,
        ControlReply::Growth(GrowthRecord {
            attempts: 1,
            phase: GrowthPhase::Provisioning,
            ..
        })
    ));
    assert_eq!(
        growth(&mut f, GrowthCommand::Attempt { claim_id, now: 110 }).await,
        ControlReply::Rejected(Fault::Busy)
    );
    assert!(matches!(
        growth(
            &mut f,
            GrowthCommand::Provisioned {
                claim_id,
                resource_id: "vol-0123456789abcdef0".into()
            }
        )
        .await,
        ControlReply::Growth(GrowthRecord {
            phase: GrowthPhase::AwaitingMount,
            mounted_total_bytes: None,
            ..
        })
    ));
    drop(f);
    let mut f = Fixture::open(&directory, scope, pool).await;
    let replay = growth(
        &mut f,
        GrowthCommand::Claim {
            intent: intent.clone(),
            prior_total_bytes: None,
        },
    )
    .await;
    assert!(matches!(
        replay,
        ControlReply::Growth(GrowthRecord {
            phase: GrowthPhase::AwaitingMount,
            attempts: 1,
            ..
        })
    ));
    let proof = MountedGrowthStamp {
        worker,
        pool_id: intent.pool_id,
        pool_generation: 1,
        provider_volume_id: "vol-0123456789abcdef0".into(),
        total_bytes: 4096,
    };
    // Enrollment alone cannot substitute for the published queue transition.
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Mounted {
                claim_id,
                proof: proof.clone()
            }
        )
        .await,
        ControlReply::Rejected(Fault::NotReady)
    );
    assert!(matches!(
        growth(&mut f, GrowthCommand::Attempt { claim_id, now: 160 }).await,
        ControlReply::Growth(GrowthRecord { attempts: 2, .. })
    ));
    assert_eq!(
        growth(&mut f, GrowthCommand::Attempt { claim_id, now: 220 }).await,
        ControlReply::Rejected(Fault::NotReady)
    );
    let mut another = intent;
    another.claim_id = Uuid::new_v4();
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: another.clone(),
                prior_total_bytes: None
            }
        )
        .await,
        ControlReply::Rejected(Fault::Busy)
    );
    f.add_ready(worker, &mut Vec::new()).await;
    assert!(matches!(
        growth(
            &mut f,
            GrowthCommand::Provisioned {
                claim_id,
                resource_id: proof.provider_volume_id.clone(),
            }
        )
        .await,
        ControlReply::Growth(GrowthRecord {
            phase: GrowthPhase::AwaitingMount,
            ..
        })
    ));
    assert!(matches!(
        growth(&mut f, GrowthCommand::Mounted { claim_id, proof }).await,
        ControlReply::Growth(GrowthRecord {
            phase: GrowthPhase::Complete,
            mounted_total_bytes: Some(4096),
            ..
        })
    ));
    assert_eq!(
        growth(
            &mut f,
            GrowthCommand::Claim {
                intent: another,
                prior_total_bytes: None
            }
        )
        .await,
        ControlReply::Rejected(Fault::Conflict),
    );
}

#[tokio::test]
async fn o17_replacement_claim_survives_publication_reopen_until_exact_retirement_ack() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    f.initialize().await;
    let old_worker = f.enroll(1).await;
    let new_worker = f.enroll(2).await;
    let old_member = ReplicaMember {
        node_id: 1,
        worker_id: old_worker.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let new_member = ReplicaMember {
        node_id: 2,
        worker_id: new_worker.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let old = f
        .register_group(
            GroupKind::Data {
                table_id: Uuid::new_v4(),
                shard_id: Uuid::new_v4(),
            },
            vec![old_member.clone()],
        )
        .await;
    let generation = Uuid::new_v4();
    assert!(matches!(
        f.apply(ControlCommand::BeginGroupReplacement {
            operation_id: Uuid::new_v4(),
            generation,
            expected: old.clone(),
            authorization: authorization()
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
    f.apply(ControlCommand::GroupPage {
        operation_id: Uuid::new_v4(),
        generation,
        replicas: Page::new(vec![new_member.clone()]).unwrap(),
    })
    .await;
    let position = AppliedPosition {
        term: 1,
        node_id: 2,
        index: 100,
    };
    let root = group_root(1, group_step(group_seed(), &new_member));
    let authority = f.state().await.ready.view.authority();
    let proof = GroupMembershipStamp {
        group: old.identity.clone(),
        generation,
        replica_count: 1,
        replica_root: root,
        applied_at: position,
        authority,
    };
    let seal = f
        .apply(ControlCommand::SealGroup {
            operation_id: Uuid::new_v4(),
            generation,
            proof: proof.clone(),
        })
        .await;
    assert!(matches!(seal, ControlReply::GroupRegistered { .. }));
    assert_eq!(
        seal,
        f.apply(ControlCommand::SealGroup {
            operation_id: Uuid::new_v4(),
            generation,
            proof
        })
        .await
    );
    drop(f);
    let mut f = Fixture::open(&directory, scope, pool).await;
    let published = GroupRecord {
        identity: old.identity.clone(),
        generation,
        replica_count: 1,
        replica_root: root,
        membership_position: position,
    };
    let next = Uuid::new_v4();
    assert_eq!(
        f.apply(ControlCommand::BeginGroupReplacement {
            operation_id: Uuid::new_v4(),
            generation: next,
            expected: published.clone(),
            authorization: authorization()
        })
        .await,
        ControlReply::Rejected(Fault::Busy)
    );
    let retired = ReplicaRetiredStamp {
        group: old.identity,
        generation,
        replica: old_member,
        worker: old_worker,
        applied_at: position,
    };
    assert_eq!(
        f.apply(ControlCommand::ReplicaRetired {
            operation_id: Uuid::new_v4(),
            authorization: authorization(),
            proof: retired.clone(),
            remote: None,
        })
        .await,
        ControlReply::GroupRetirement {
            generation,
            remaining: 0
        }
    );
    assert_eq!(
        f.apply(ControlCommand::ReplicaRetired {
            operation_id: Uuid::new_v4(),
            authorization: authorization(),
            proof: retired,
            remote: None,
        })
        .await,
        ControlReply::GroupRetirement {
            generation,
            remaining: 0
        }
    );
    assert!(matches!(
        f.apply(ControlCommand::BeginGroupReplacement {
            operation_id: Uuid::new_v4(),
            generation: next,
            expected: published,
            authorization: authorization()
        })
        .await,
        ControlReply::GroupBuilding { .. }
    ));
}
