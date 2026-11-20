//! NS14 / O17 / O26: `GroupDamaged` and `GroupDamageReplaced` (plan section 3,
//! rules 2 to 4 and 6), applied to a real Control store.
use super::*;
use crate::oltp_replication::authority::control::{
    GroupDamageGate, GroupDamagePhase, GroupDamageReason, GroupDamageRecord,
    GroupDamageReplacement, GroupDamageReport, LocalOperatorPermit,
};
use crate::protocol::{ErrorCode, Outcome};

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
    }
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

    let fresh = ReplicaMember {
        node_id: 9,
        worker_id: other.worker_id,
        pool_id: Uuid::new_v4(),
    };
    // Unauthorized: the stamp does not bind this proof.
    let proof = replaced_by(&damage, fresh.clone(), 40);
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
            replaced_by(&report(&group, worker, 6), fresh.clone(), 40)
        )
        .await,
        ControlReply::Rejected(Fault::NotFound)
    );
    let done = record(replaced(&mut f, proof.clone()).await);
    assert_eq!(
        done.phase,
        GroupDamagePhase::Replaced {
            replacement: fresh.clone(),
            committed_at: proof.committed_at,
        }
    );
    let evidence = done.removal().unwrap();
    assert_eq!(evidence.group, group);
    assert_eq!(evidence.removed, damage.replica);
    assert_eq!(evidence.committed_at, proof.committed_at);
    // Idempotent; a different replacement conflicts.
    assert_eq!(record(replaced(&mut f, proof).await), done);
    let mut elsewhere = fresh;
    elsewhere.node_id = 10;
    assert_eq!(
        replaced(&mut f, replaced_by(&damage, elsewhere, 41)).await,
        ControlReply::Rejected(Fault::Conflict)
    );
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
    let group = data_group(&scope);
    let mut refused = report(&group, local, 3);
    refused.reason = GroupDamageReason::PoolRefused;
    refused.first_bad_seq = None;

    let gated = handle
        .report_group_damage(local, refused.clone())
        .await
        .unwrap_err();
    assert_eq!(gated.code, ErrorCode::UnsupportedOperation);
    assert_eq!(gated.outcome, Outcome::NotApplied);
    assert_eq!(handle.group_damage(group.group_id, 3).await.unwrap(), None);

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
        handle.group_damage(group.group_id, 3).await.unwrap(),
        Some(reported)
    );

    let fresh = ReplicaMember {
        node_id: 8,
        worker_id: local.worker_id,
        pool_id: Uuid::new_v4(),
    };
    let done = handle
        .group_damage_replaced_local(
            &permit,
            Uuid::new_v4(),
            replaced_by(&refused, fresh.clone(), 12),
        )
        .await
        .unwrap();
    assert!(matches!(done.phase, GroupDamagePhase::Replaced { .. }));
    let read = handle
        .group_damage(group.group_id, 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, done);
    assert_eq!(read.removal().unwrap().removed, refused.replica);
}
