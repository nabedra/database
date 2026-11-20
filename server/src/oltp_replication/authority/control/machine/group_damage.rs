//! NS14: applying `GroupDamaged` and `GroupDamageReplaced` (see
//! `control/group_damage.rs`).
use super::super::group_damage::{
    GroupDamagePhase, GroupDamageRecord, GroupDamageReplacement, GroupDamageReport, record_key,
};
use super::*;

/// Rule 2: the first report of a replica is recorded; any later report of the
/// same replica returns that record unchanged, whatever its phase.
pub(super) fn report(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    worker: WorkerIdentity,
    report: &GroupDamageReport,
) -> Result<ControlReply, ProtocolError> {
    if report.validate().is_err()
        || !same_scope(scope, &report.group)
        || report.replica.worker_id != worker.worker_id
    {
        return fault(Fault::Invalid);
    }
    let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))?
        .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == worker);
    if !enrolled {
        return fault(Fault::Unauthorized);
    }
    let key = record_key(report.group.group_id, report.replica.node_id);
    if let Some(existing) = load::<GroupDamageRecord>(tx, &key)? {
        if existing.report.replica != report.replica || existing.report.group != report.group {
            return fault(Fault::Conflict);
        }
        return Ok(ControlReply::GroupDamage(Box::new(existing)));
    }
    let record = GroupDamageRecord {
        report: report.clone(),
        worker,
        reported_at: position,
        phase: GroupDamagePhase::Reported,
    };
    save(tx, &key, &record)?;
    Ok(ControlReply::GroupDamage(Box::new(record)))
}

/// Rule 3: records that the change removing the damaged replica committed.
/// Only a reported replica can be replaced, and only by a fresh identity.
pub(super) fn replaced(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    authorization: &AuthorizationStamp,
    proof: &GroupDamageReplacement,
) -> Result<ControlReply, ProtocolError> {
    if !stamp(authorization, state.policy)
        || authorization.evidence != crate::oltp_replication::owned::serialized_digest(proof)?
    {
        return fault(Fault::Unauthorized);
    }
    if !same_scope(scope, &proof.group)
        || proof.replacement.validate().is_err()
        || proof.replacement.node_id == proof.removed.node_id
        || !position_valid(proof.committed_at)
    {
        return fault(Fault::Invalid);
    }
    let key = record_key(proof.group.group_id, proof.removed.node_id);
    let Some(mut record) = load::<GroupDamageRecord>(tx, &key)? else {
        return fault(Fault::NotFound);
    };
    if record.report.group != proof.group || record.report.replica != proof.removed {
        return fault(Fault::Invalid);
    }
    let next = GroupDamagePhase::Replaced {
        replacement: proof.replacement.clone(),
        committed_at: proof.committed_at,
    };
    match &record.phase {
        GroupDamagePhase::Replaced { .. } if record.phase == next => {}
        GroupDamagePhase::Replaced { .. } => return fault(Fault::Conflict),
        GroupDamagePhase::Reported => {
            record.phase = next;
            save(tx, &key, &record)?;
        }
    }
    Ok(ControlReply::GroupDamage(Box::new(record)))
}
