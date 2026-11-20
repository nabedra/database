//! NS14 / O17 / O26: `GroupDamaged` and `GroupDamageReplaced` (plan section 3,
//! rules 2 to 4 and 6), applied to a real Control store.
use super::*;
use crate::oltp_replication::authority::control::{
    GroupDamageGate, GroupDamagePhase, GroupDamageReason, GroupDamageRecord,
    GroupDamageReplacement, GroupDamageReplacementPlan, GroupDamageReport, LocalOperatorPermit,
    PoolRefusal,
};
use crate::protocol::{ErrorCode, Outcome, ProtocolError};

struct Open;
impl GroupDamageGate for Open {
    fn group_damage_supported(&self) -> bool {
        true
    }
}

fn data_group(scope: &GroupIdentity) -> GroupIdentity {
    GroupIdentity {
        group_id: Uuid::new_v4(),
        kind: GroupKind::Data {
            table_id: Uuid::new_v4(),
            shard_id: Uuid::new_v4(),
        },
        ..scope.clone()
    }
}

fn report(group: &GroupIdentity, worker: WorkerIdentity, node: u64) -> GroupDamageReport {
    GroupDamageReport {
        group: group.clone(),
        replica: ReplicaMember {
            node_id: node,
            worker_id: worker.worker_id,
            pool_id: Uuid::new_v4(),
        },
        reason: GroupDamageReason::JournalRecord,
        first_bad_seq: Some(17),
        refusal: None,
    }
}

/// A Data group registered in Control with one member per worker (node `i+1`
/// on `workers[i]`, each in its own pool unless `pools` names it).
async fn data_placement(
    f: &mut Fixture,
    workers: &[WorkerIdentity],
    pools: &[Option<Uuid>],
) -> (GroupRecord, Vec<ReplicaMember>) {
    let members: Vec<ReplicaMember> = workers
        .iter()
        .enumerate()
        .map(|(index, worker)| ReplicaMember {
            node_id: index as u64 + 1,
            worker_id: worker.worker_id,
            pool_id: pools
                .get(index)
                .copied()
                .flatten()
                .unwrap_or_else(Uuid::new_v4),
        })
        .collect();
    let record = f
        .register_group(
            GroupKind::Data {
                table_id: Uuid::new_v4(),
                shard_id: Uuid::new_v4(),
            },
            members.clone(),
        )
        .await;
    (record, members)
}

fn damaged_report(record: &GroupRecord, member: &ReplicaMember) -> GroupDamageReport {
    GroupDamageReport {
        group: record.identity.clone(),
        replica: member.clone(),
        reason: GroupDamageReason::RunBlock,
        first_bad_seq: None,
        refusal: None,
    }
}

async fn report_damage(f: &mut Fixture, worker: WorkerIdentity, report: &GroupDamageReport) {
    let recorded = record(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: report.operation_id(worker).unwrap(),
            worker,
            report: Box::new(report.clone()),
        })
        .await,
    );
    assert_eq!(recorded.phase, GroupDamagePhase::Reported);
}

fn plan(
    record: &GroupRecord,
    members: &[ReplicaMember],
    damaged: usize,
    replacement: ReplicaMember,
) -> GroupDamageReplacementPlan {
    GroupDamageReplacementPlan {
        group: record.identity.clone(),
        damaged: members[damaged].clone(),
        replacement,
        expected: record.clone(),
        members: members.to_vec(),
    }
}

async fn begin(f: &mut Fixture, plan: &GroupDamageReplacementPlan) -> ControlReply {
    f.apply(ControlCommand::GroupDamageReplacementBegin {
        operation_id: Uuid::new_v4(),
        authorization: AuthorizationStamp {
            evidence: serialized_digest(plan).unwrap(),
            ..authorization()
        },
        plan: Box::new(plan.clone()),
    })
    .await
}

async fn published(f: &Fixture, group_id: Uuid) -> (GroupRecord, Vec<ReplicaMember>) {
    let bytes = f
        .machine
        .store()
        .read(BUDGET, move |view| {
            let record: GroupRecord = decode(view.get(&key(GROUPS, group_id))?.unwrap().as_ref())?;
            let mut members = Vec::new();
            view.scan_page(&member_prefix(record.generation), None, 16, |_, raw| {
                members.push(decode::<ReplicaMember>(raw)?);
                Ok(())
            })?;
            view.encode(&(record, members))
        })
        .await
        .unwrap();
    decode(bytes.as_ref()).unwrap()
}

fn record(reply: ControlReply) -> GroupDamageRecord {
    match reply {
        ControlReply::GroupDamage(record) => *record,
        other => panic!("unexpected reply {other:?}"),
    }
}

fn replaced_by(
    report: &GroupDamageReport,
    replacement: ReplicaMember,
    committed: u64,
) -> GroupDamageReplacement {
    GroupDamageReplacement {
        group: report.group.clone(),
        removed: report.replica.clone(),
        replacement,
        committed_at: AppliedPosition {
            term: 2,
            node_id: 1,
            index: committed,
        },
    }
}

async fn replaced(f: &mut Fixture, proof: GroupDamageReplacement) -> ControlReply {
    f.apply(ControlCommand::GroupDamageReplaced {
        operation_id: Uuid::new_v4(),
        authorization: AuthorizationStamp {
            evidence: serialized_digest(&proof).unwrap(),
            ..authorization()
        },
        proof: Box::new(proof),
    })
    .await
}

/// O17 / NS14 rules 2 and 3 in the Control machine: an enrolled worker's
/// report is recorded once per replica and every repeat returns that record;
/// another worker cannot report for it. Only an authorized proof for a
/// reported replica and a fresh identity moves it to `replaced`, which then
/// yields the rule 4 release evidence; a second, different replacement
/// conflicts.
#[tokio::test]
async fn o17_ns14_group_damaged_is_recorded_once_and_replaced_only_by_proof() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope.clone(), pool).await;
    f.initialize().await;
    let worker = f.enroll(1).await;
    let other = f.enroll(2).await;
    let group = data_group(&scope);
    let damage = report(&group, worker, 4);

    let first = record(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: damage.operation_id(worker).unwrap(),
            worker,
            report: Box::new(damage.clone()),
        })
        .await,
    );
    assert_eq!(first.report, damage);
    assert_eq!(first.worker, worker);
    assert_eq!(first.phase, GroupDamagePhase::Reported);
    assert!(
        first.removal().is_none(),
        "no release evidence before replacement"
    );
    // A later report of the same replica (another reason, after a restart)
    // returns the first record.
    let mut later = damage.clone();
    later.reason = GroupDamageReason::RunBlock;
    later.first_bad_seq = None;
    let repeat = record(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: later.operation_id(worker).unwrap(),
            worker,
            report: Box::new(later),
        })
        .await,
    );
    assert_eq!(repeat, first);
    // Another worker cannot report this replica.
    assert_eq!(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: damage.operation_id(other).unwrap(),
            worker: other,
            report: Box::new(damage.clone()),
        })
        .await,
        ControlReply::Rejected(Fault::Invalid)
    );
    // An unenrolled worker cannot report at all.
    let stranger = WorkerIdentity::from(&super::worker(f.base + 99));
    let foreign = report(&group, stranger, 5);
    assert_eq!(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: foreign.operation_id(stranger).unwrap(),
            worker: stranger,
            report: Box::new(foreign),
        })
        .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );

    // NS14b: replacements need a published placement and Control's plan.
    let third = f.enroll(3).await;
    let spare = f.enroll(4).await;
    let (placement, members) = data_placement(&mut f, &[other, third, worker], &[]).await;
    let damage = damaged_report(&placement, &members[2]);
    report_damage(&mut f, worker, &damage).await;
    let fresh = ReplicaMember {
        node_id: 9,
        worker_id: spare.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let proof = replaced_by(&damage, fresh.clone(), 40);
    // Unauthorized: the stamp does not bind this proof.
    assert_eq!(
        f.apply(ControlCommand::GroupDamageReplaced {
            operation_id: Uuid::new_v4(),
            authorization: authorization(),
            proof: Box::new(proof.clone()),
        })
        .await,
        ControlReply::Rejected(Fault::Unauthorized)
    );
    // The damaged identity cannot be its own replacement.
    assert_eq!(
        replaced(&mut f, replaced_by(&damage, damage.replica.clone(), 40)).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    // An unreported replica has nothing to replace.
    assert_eq!(
        replaced(
            &mut f,
            replaced_by(&damaged_report(&placement, &members[0]), fresh.clone(), 40)
        )
        .await,
        ControlReply::Rejected(Fault::NotFound)
    );
    // Without Control's plan nothing would publish the routes.
    assert_eq!(
        replaced(&mut f, proof.clone()).await,
        ControlReply::Rejected(Fault::NotReady)
    );
    let chosen = plan(&placement, &members, 2, fresh.clone());
    let claimed = record(begin(&mut f, &chosen).await);
    assert!(matches!(&claimed.phase, GroupDamagePhase::Replacing { plan, .. } if **plan == chosen));
    assert!(
        claimed.removal().is_none(),
        "no release evidence while replacing"
    );
    // The same plan replays; any other plan for the record conflicts.
    assert_eq!(record(begin(&mut f, &chosen).await), claimed);
    let mut other_plan = chosen.clone();
    other_plan.replacement.node_id = 11;
    assert_eq!(
        begin(&mut f, &other_plan).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    // Only the plan's replacement completes it, and only past the placement.
    let mut elsewhere = fresh.clone();
    elsewhere.node_id = 10;
    assert_eq!(
        replaced(&mut f, replaced_by(&damage, elsewhere.clone(), 41)).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(
        replaced(&mut f, replaced_by(&damage, fresh.clone(), 10)).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    let revision = f.state().await.group_inventory_revision;
    let done = record(replaced(&mut f, proof.clone()).await);
    let GroupDamagePhase::Replaced {
        replacement,
        committed_at,
        published: Some(new),
    } = done.phase.clone()
    else {
        panic!("unexpected phase {:?}", done.phase);
    };
    assert_eq!(replacement, fresh);
    assert_eq!(committed_at, proof.committed_at);
    // The new placement is published: a new generation without the damaged
    // member, whose position is the committed change.
    let (current, current_members) = published(&f, placement.identity.group_id).await;
    assert_eq!(current, new);
    assert_ne!(current.generation, placement.generation);
    assert_eq!(current.membership_position, proof.committed_at);
    assert_eq!(current.replica_count, 3);
    assert_eq!(current_members, chosen.next_members());
    assert!(!current_members.contains(&damage.replica));
    assert!(current_members.contains(&fresh));
    assert_eq!(f.state().await.group_inventory_revision, revision + 1);
    let evidence = done.removal().unwrap();
    assert_eq!(evidence.group, placement.identity);
    assert_eq!(evidence.removed, damage.replica);
    assert_eq!(evidence.committed_at, proof.committed_at);
    // Idempotent; a different replacement conflicts; a new plan is refused.
    assert_eq!(record(replaced(&mut f, proof).await), done);
    assert_eq!(
        replaced(&mut f, replaced_by(&damage, elsewhere, 41)).await,
        ControlReply::Rejected(Fault::Conflict)
    );
    assert_eq!(record(begin(&mut f, &chosen).await), done);
    // A report after the replacement still returns the replaced record, so a
    // worker that restarts reads its release evidence.
    let after = record(
        f.apply(ControlCommand::GroupDamaged {
            operation_id: Uuid::new_v4(),
            worker,
            report: Box::new(damage),
        })
        .await,
    );
    assert_eq!(after, done);
}

/// NS14b rule 3: `GroupDamageReplacementBegin` claims the group for exactly
/// one plan, never reuses a node ID, refuses placements that break the voter
/// rules or a stale placement, and applies the rate limits (one replacement
/// per group, at most 2 in the cluster, at most 1 per pool); a completed
/// replacement frees its slot.
#[tokio::test]
async fn o17_ns14b_begin_claims_once_rate_limits_and_frees_on_publication() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope, pool).await;
    f.initialize().await;
    let mut workers = Vec::new();
    for number in 1..=8 {
        workers.push(f.enroll(number).await);
    }
    let fresh = |node: u64, worker: &WorkerIdentity| ReplicaMember {
        node_id: node,
        worker_id: worker.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let shared_pool = Uuid::new_v4();
    let (a, a_members) = data_placement(&mut f, &workers[0..3], &[]).await;
    let (b, b_members) = data_placement(&mut f, &workers[3..6], &[]).await;
    let (c, c_members) = data_placement(
        &mut f,
        &[workers[0], workers[1], workers[6]],
        &[None, None, Some(shared_pool)],
    )
    .await;
    let (d, d_members) = data_placement(
        &mut f,
        &[workers[2], workers[3], workers[6]],
        &[None, None, Some(shared_pool)],
    )
    .await;
    for (record, members) in [
        (&a, &a_members),
        (&b, &b_members),
        (&c, &c_members),
        (&d, &d_members),
    ] {
        report_damage(
            &mut f,
            workers
                .iter()
                .copied()
                .find(|w| w.worker_id == members[2].worker_id)
                .unwrap(),
            &damaged_report(record, &members[2]),
        )
        .await;
    }
    // Invalid placements: on a surviving member's worker, or in the damaged pool.
    assert_eq!(
        begin(&mut f, &plan(&a, &a_members, 2, fresh(4, &workers[0]))).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    let mut same_pool = fresh(4, &workers[2]);
    same_pool.pool_id = a_members[2].pool_id;
    assert_eq!(
        begin(&mut f, &plan(&a, &a_members, 2, same_pool)).await,
        ControlReply::Rejected(Fault::Invalid)
    );
    // A placement that is no longer published is stale.
    let mut stale = plan(&a, &a_members, 2, fresh(4, &workers[7]));
    stale.expected.generation = Uuid::new_v4();
    assert_eq!(
        begin(&mut f, &stale).await,
        ControlReply::Rejected(Fault::Stale)
    );

    // The damaged worker's other pool is allowed once the damaged one leaves.
    let a_plan = plan(&a, &a_members, 2, fresh(4, &workers[2]));
    assert!(matches!(
        record(begin(&mut f, &a_plan).await).phase,
        GroupDamagePhase::Replacing { .. }
    ));
    // One replacement per group: a second damaged member of A waits.
    let second = damaged_report(&a, &a_members[1]);
    report_damage(&mut f, workers[1], &second).await;
    assert_eq!(
        begin(&mut f, &plan(&a, &a_members, 1, fresh(5, &workers[7]))).await,
        ControlReply::Rejected(Fault::Busy)
    );
    // At most one replacement per pool: C and D lost replicas in one pool.
    let c_plan = plan(&c, &c_members, 2, fresh(4, &workers[7]));
    assert!(matches!(
        record(begin(&mut f, &c_plan).await).phase,
        GroupDamagePhase::Replacing { .. }
    ));
    // At most two in the cluster: B waits although its pools are free.
    let b_plan = plan(&b, &b_members, 2, fresh(4, &workers[7]));
    assert_eq!(
        begin(&mut f, &b_plan).await,
        ControlReply::Rejected(Fault::Busy)
    );

    // A's replacement commits and frees its slot. D still waits: it shares
    // C's pool. B, whose pools are free, starts.
    let a_done = record(
        replaced(
            &mut f,
            GroupDamageReplacement {
                group: a.identity.clone(),
                removed: a_members[2].clone(),
                replacement: a_plan.replacement.clone(),
                committed_at: AppliedPosition {
                    term: 2,
                    node_id: 1,
                    index: 30,
                },
            },
        )
        .await,
    );
    assert!(matches!(a_done.phase, GroupDamagePhase::Replaced { .. }));
    let d_plan = plan(&d, &d_members, 2, fresh(4, &workers[0]));
    assert_eq!(
        begin(&mut f, &d_plan).await,
        ControlReply::Rejected(Fault::Busy)
    );
    assert!(matches!(
        record(begin(&mut f, &b_plan).await).phase,
        GroupDamagePhase::Replacing { .. }
    ));

    // A node ID used once in a group is never used again: A's second damaged
    // member cannot take the first damaged replica's ID.
    let (a_now, a_now_members) = published(&f, a.identity.group_id).await;
    let damaged_at = a_now_members
        .iter()
        .position(|member| *member == a_members[1])
        .unwrap();
    assert_eq!(
        begin(
            &mut f,
            &plan(&a_now, &a_now_members, damaged_at, fresh(3, &workers[7]))
        )
        .await,
        ControlReply::Rejected(Fault::Conflict)
    );
}

/// O17 / O26 / NS14 through the Control handle: nothing is proposed until the
/// mixed-version gate is installed; then a report (also `pool_refused` for a
/// refused pool, rule 6) is recorded, read back after a quorum read, and the
/// local operator's replacement proof yields the release evidence.
#[tokio::test]
async fn o17_o26_ns14_control_handle_gates_reports_and_serves_the_release_record() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut f = Fixture::open(&directory, scope.clone(), pool.clone()).await;
    f.initialize().await;
    let enrollment = super::worker(pool.worker_id.as_u128());
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
    let shared = f.machine.store().resources().clone();
    let permit = LocalOperatorPermit::from_local_bootstrap(local, shared, Arc::new(())).unwrap();
    let peer = f.enroll(5).await;
    let spare = f.enroll(6).await;
    let (placement, members) = data_placement(&mut f, &[peer, local], &[]).await;
    let group = placement.identity.clone();
    let mut refused = damaged_report(&placement, &members[1]);
    refused.reason = GroupDamageReason::PoolRefused;
    refused.refusal = Some(PoolRefusal::StorageIo);
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

    let gated = handle
        .report_group_damage(local, refused.clone())
        .await
        .unwrap_err();
    assert_eq!(gated.code, ErrorCode::UnsupportedOperation);
    assert_eq!(gated.outcome, Outcome::NotApplied);
    assert_eq!(handle.group_damage(group.group_id, 2).await.unwrap(), None);

    handle.install_group_damage_gate(Arc::new(Open));
    // The reporter must be the replica's worker.
    let stranger = WorkerIdentity::from(&super::worker(f.base + 7));
    assert_eq!(
        handle
            .report_group_damage(stranger, refused.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unauthorized
    );
    let reported = handle
        .report_group_damage(local, refused.clone())
        .await
        .unwrap();
    assert_eq!(reported.phase, GroupDamagePhase::Reported);
    assert_eq!(
        handle
            .report_group_damage(local, refused.clone())
            .await
            .unwrap(),
        reported
    );
    assert_eq!(
        handle.group_damage(group.group_id, 2).await.unwrap(),
        Some(reported)
    );

    // NS14b through the handle: the scheduler's plan, then the proof.
    let fresh = ReplicaMember {
        node_id: 8,
        worker_id: spare.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let premature = handle
        .group_damage_replaced_local(
            &permit,
            Uuid::new_v4(),
            replaced_by(&refused, fresh.clone(), 12),
        )
        .await
        .unwrap_err();
    assert_eq!(premature.code, ErrorCode::Conflict);
    let chosen = plan(&placement, &members, 1, fresh.clone());
    let claimed = handle
        .begin_group_damage_replacement_local(&permit, Uuid::new_v4(), chosen.clone())
        .await
        .unwrap();
    assert_eq!(claimed.plan(), Some(&chosen));
    let (page, done_listing) = handle.group_damage_page(None, 64).await.unwrap();
    assert!(done_listing);
    assert_eq!(page, vec![claimed.clone()]);
    assert_eq!(
        handle.group_damage_of_group(group.group_id).await.unwrap(),
        vec![claimed]
    );
    let done = handle
        .group_damage_replaced_local(
            &permit,
            Uuid::new_v4(),
            replaced_by(&refused, fresh.clone(), 12),
        )
        .await
        .unwrap();
    assert!(matches!(
        done.phase,
        GroupDamagePhase::Replaced {
            published: Some(_),
            ..
        }
    ));
    assert!(!done.pending());
    let read = handle
        .group_damage(group.group_id, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, done);
    assert_eq!(read.report.refusal, Some(PoolRefusal::StorageIo));
    assert_eq!(read.removal().unwrap().removed, refused.replica);
    let routed = handle.group(group.group_id).await.unwrap().unwrap();
    assert_eq!(routed.membership_position.index, 12);
}

/// Rule 6: a refused pool's error names its class.
#[test]
fn ns24_o26_pool_refusal_classes() {
    let refused = |message: &str| {
        ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::NotApplied)
    };
    for (message, class) in [
        (
            "OLTP pool refused: storage corrupt: bad CURRENT",
            PoolRefusal::StorageCorrupt,
        ),
        (
            "OLTP pool refused: storage I/O error: other",
            PoolRefusal::StorageIo,
        ),
        (
            "OLTP pool refused: damage spans too many groups",
            PoolRefusal::DamageLimit,
        ),
        ("immutable pool identity mismatch", PoolRefusal::Identity),
        (
            "retained replica pool mount or growth claim differs",
            PoolRefusal::Mount,
        ),
        ("something else", PoolRefusal::Other),
    ] {
        assert_eq!(PoolRefusal::classify(&refused(message)), class, "{message}");
    }
}
