use super::super::pool_drain::*;
use super::*;
use crate::oltp_deployment::{
    controller::{GrowthPhase, GrowthRecord},
    planner::Action,
};
use crate::oltp_replication::owned::serialized_digest;

pub(in crate::oltp_replication::authority::control) const REVISION: &[u8] =
    b"ct/pool-placement-revision";
pub(in crate::oltp_replication::authority::control) const BUILDS: &[u8] = b"ct/group-build/";
pub(in crate::oltp_replication::authority::control) const REPLACEMENTS: &[u8] =
    b"ct/group-replacement/";

pub(in crate::oltp_replication::authority::control) fn record_key(id: Uuid) -> Vec<u8> {
    key(b"ct/pool-drain/", id)
}
pub(in crate::oltp_replication::authority::control) fn barrier_key(pool: Uuid) -> Vec<u8> {
    key(b"ct/pool-drain-active/", pool)
}

pub(in crate::oltp_replication::authority::control) fn blocks_pool(
    tx: &mut GroupTransaction<'_>,
    pool: Uuid,
) -> Result<bool, ProtocolError> {
    Ok(tx.get(&barrier_key(pool))?.is_some())
}

/// Pin every mutation that can add, withdraw, or settle a placement or its
/// transition. Older Control stores start at revision zero.
pub(super) fn advance_revision(
    tx: &mut GroupTransaction<'_>,
    command: &ControlCommand,
) -> Result<(), ProtocolError> {
    use ControlCommand::*;
    if matches!(
        command,
        BeginGroup { .. }
            | RegisterInitialData { .. }
            | BeginGroupReplacement { .. }
            | GroupPage { .. }
            | SealGroup { .. }
            | ReplicaRetired { .. }
            | BeginReplicaMove { .. }
            | ArmReplicaMove { .. }
            | CancelReplicaMove { .. }
            | PublishReplicaMove { .. }
    ) {
        let revision = load::<u64>(tx, REVISION)?.unwrap_or_default();
        save(tx, REVISION, &revision.checked_add(1).ok_or_else(unknown)?)?;
    }
    Ok(())
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &State,
    operation_id: Uuid,
    worker: WorkerIdentity,
    authorization: &AuthorizationStamp,
    command: &PoolDrainCommand,
) -> Result<ControlReply, ProtocolError> {
    if authorization.authorization_id != operation_id
        || !stamp(authorization, state.policy)
        || authorization.subject != worker.certificate_sha256
        || authorization.evidence != serialized_digest(&(worker, command))?
        || !current_ready(tx, state, worker)?
    {
        return fault(Fault::Unauthorized);
    }
    let id = match command {
        PoolDrainCommand::Begin {
            growth_claim,
            mount,
        } => {
            let expected = &mount.expected;
            let pool = expected.identity.pool_id;
            if expected.validate().is_err()
                || expected.identity.cluster_id != scope.cluster_id
                || expected.worker != worker
                || mount.mount_id == 0
                || mount.root_inode == 0
            {
                return fault(Fault::Invalid);
            }
            if let Some(existing) = load::<PoolDrainRecord>(tx, &record_key(operation_id))? {
                return if existing.growth_claim == *growth_claim
                    && &existing.mount == mount.as_ref()
                {
                    reply(existing)
                } else {
                    fault(Fault::Conflict)
                };
            }
            if blocks_pool(tx, pool)? {
                return fault(Fault::Conflict);
            }
            let Some(claim) = load::<GrowthRecord>(tx, &super::super::growth::key(*growth_claim))?
            else {
                return fault(Fault::NotFound);
            };
            let same_owner = claim.intent.worker == worker
                && claim.intent.pool_generation == expected.generation;
            let retained_owner = claim.intent.worker.worker_id == worker.worker_id
                && claim.intent.worker.enrollment_revision < worker.enrollment_revision
                && claim.intent.pool_generation < expected.generation;
            if claim.phase != GrowthPhase::Complete
                || !matches!(claim.intent.action, Action::AddPool { .. })
                || claim.intent.pool_id != pool
                || claim.provider_resource.as_deref() != Some(&expected.provider_volume_id)
                || !(same_owner || retained_owner)
            {
                return fault(Fault::Unauthorized);
            }
            let record = PoolDrainRecord {
                id: operation_id,
                growth_claim: *growth_claim,
                mount: *mount.clone(),
                phase: PoolDrainPhase::Draining,
            };
            save(tx, &barrier_key(pool), &operation_id)?;
            save(tx, &record_key(operation_id), &record)?;
            return reply(record);
        }
        PoolDrainCommand::Prepare { id, .. }
        | PoolDrainCommand::Closed { id, .. }
        | PoolDrainCommand::Cancel { id } => *id,
    };
    let Some(mut record) = load::<PoolDrainRecord>(tx, &record_key(id))? else {
        return fault(Fault::NotFound);
    };
    if record.mount.expected.worker != worker {
        return fault(Fault::Unauthorized);
    }
    let pool = record.mount.expected.identity.pool_id;
    match command {
        PoolDrainCommand::Prepare { revision, .. } => {
            if record.phase == PoolDrainPhase::Closing {
                return reply(record);
            }
            if record.phase != PoolDrainPhase::Draining
                || state.pending.is_some()
                || state.restart.is_some()
                || load::<u64>(tx, REVISION)?.unwrap_or_default() != *revision
                || load::<Uuid>(tx, &barrier_key(pool))? != Some(id)
            {
                return fault(Fault::Stale);
            }
            if let Some(active) = load::<Uuid>(tx, super::super::growth::ACTIVE)? {
                let growth = load::<GrowthRecord>(tx, &super::super::growth::key(active))?
                    .ok_or_else(unknown)?;
                if growth.intent.pool_id == pool {
                    return fault(Fault::Busy);
                }
            }
            record.phase = PoolDrainPhase::Closing;
        }
        PoolDrainCommand::Closed { mount, .. } => {
            if &record.mount != mount.as_ref() {
                return fault(Fault::Invalid);
            }
            if record.phase == PoolDrainPhase::Closed {
                return reply(record);
            }
            if record.phase != PoolDrainPhase::Closing
                || load::<Uuid>(tx, &barrier_key(pool))? != Some(id)
            {
                return fault(Fault::Conflict);
            }
            record.phase = PoolDrainPhase::Closed;
        }
        PoolDrainCommand::Cancel { .. } => {
            if record.phase == PoolDrainPhase::Cancelled {
                return reply(record);
            }
            if record.phase != PoolDrainPhase::Draining
                || load::<Uuid>(tx, &barrier_key(pool))? != Some(id)
            {
                return fault(Fault::Conflict);
            }
            record.phase = PoolDrainPhase::Cancelled;
            tx.delete(&barrier_key(pool))?;
        }
        PoolDrainCommand::Begin { .. } => unreachable!(),
    }
    save(tx, &record_key(id), &record)?;
    reply(record)
}

fn reply(record: PoolDrainRecord) -> Result<ControlReply, ProtocolError> {
    Ok(ControlReply::PoolDrain(Box::new(record)))
}

pub(in crate::oltp_replication::authority::control) fn retired_key(
    generation: Uuid,
    node: u64,
) -> Vec<u8> {
    member_key_nested(b"ct/group-retired/", generation, node)
}
