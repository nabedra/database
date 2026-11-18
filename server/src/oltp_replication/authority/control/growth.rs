//! Replicated metadata-only capacity claims. A failed/lost provider response
//! leaves the same claim and resource identity available for reconciliation.
use super::model::{ControlReply, Fault, State, WorkerRecord};
use crate::{
    oltp::pool::GroupTransaction,
    oltp_deployment::{controller::*, live::InventoryPin, planner::Action},
    protocol::ProtocolError,
};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub(super) const ACTIVE: &[u8] = b"ct/growth-active";
pub(super) const SPEND: &[u8] = b"ct/growth-reserved-spend";
pub(super) const POLICY: &[u8] = b"ct/growth-policy";
pub(super) const LAST_ATTEMPT: &[u8] = b"ct/growth-last-attempt";
pub(super) const ACTIVE_AUTOMATIC: &[u8] = b"ct/growth-active-automatic";
pub(super) const REVISION: &[u8] = b"ct/growth-inventory-revision";
const POLICIES: &[u8] = b"ct/growth-policy-id/";

pub(super) fn pin(
    state: &State,
    policy_id: Uuid,
    growth_revision: u64,
    monthly_spend: u64,
) -> Option<InventoryPin> {
    if state.pending.is_some()
        || state.restart.is_some()
        || state.invalid_ready != 0
        || state.ready.ready_count == 0
    {
        return None;
    }
    Some(InventoryPin {
        policy_id,
        ready: state.ready,
        enrollment_revision: state.enrollment_revision,
        group_revision: state.group_inventory_revision,
        growth_revision,
        monthly_spend,
    })
}
pub(super) fn key(id: Uuid) -> Vec<u8> {
    [b"ct/growth/".as_slice(), id.as_bytes()].concat()
}
pub(super) fn pool_claim_key(id: Uuid) -> Vec<u8> {
    super::machine::key(b"ct/growth-pool/", id)
}
fn load<T: DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.get(key)?
        .map(|b| {
            serde_json::from_slice(b.as_ref())
                .map_err(|_| super::model::unavailable("unreadable growth claim"))
        })
        .transpose()
}
fn save<T: Serialize>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &T,
) -> Result<(), ProtocolError> {
    let bytes = tx.encode_value(value)?;
    tx.put(key, bytes.as_ref())
}
fn rejected(fault: Fault) -> Result<ControlReply, ProtocolError> {
    Ok(ControlReply::Rejected(fault))
}
fn advance_revision(tx: &mut GroupTransaction<'_>) -> Result<(), ProtocolError> {
    let next = load::<u64>(tx, REVISION)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| super::model::unavailable("growth inventory revision exhausted"))?;
    save(tx, REVISION, &next)
}
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    command: &GrowthCommand,
    state: &State,
) -> Result<ControlReply, ProtocolError> {
    if let GrowthCommand::Configure { policy } = command {
        if !policy.valid() {
            return rejected(Fault::Invalid);
        }
        let previous = load::<GrowthPolicy>(tx, POLICY)?;
        if previous.as_ref().is_none_or(|old| {
            old.initial_replica_zones != policy.initial_replica_zones || old.zones != policy.zones
        }) {
            use super::machine::placement;
            if let Some(id) = load::<Uuid>(tx, placement::ACTIVE)? {
                let plan = load::<super::ReplicaMove>(tx, &placement::move_key(id))?
                    .ok_or_else(|| super::model::unavailable("active replica move is missing"))?;
                if plan.failure_domains.is_some() {
                    return rejected(Fault::Busy);
                }
            }
        }
        if let Some(old) = &previous {
            if old.policy_id == policy.policy_id {
                return if old == policy {
                    Ok(ControlReply::GrowthConfigured(old.clone()))
                } else {
                    rejected(Fault::Conflict)
                };
            }
        }
        if load::<Uuid>(tx, ACTIVE)?.is_some() {
            return rejected(Fault::Busy);
        }
        let spent = load::<u64>(tx, SPEND)?
            .unwrap_or(0)
            .max(policy.initial_monthly_spend);
        if spent > policy.monthly_spend_ceiling {
            return rejected(Fault::NotReady);
        }
        let key = super::machine::key(POLICIES, policy.policy_id);
        if load::<GrowthPolicy>(tx, &key)?
            .as_ref()
            .is_some_and(|old| old != policy)
        {
            return rejected(Fault::Conflict);
        }
        if let Some(old) = previous {
            save(tx, &super::machine::key(POLICIES, old.policy_id), &old)?;
        }
        save(tx, &key, policy)?;
        save(tx, POLICY, policy)?;
        save(tx, SPEND, &spent)?;
        advance_revision(tx)?;
        return Ok(ControlReply::GrowthConfigured(policy.clone()));
    }
    let claim = match command {
        GrowthCommand::Claim {
            intent,
            prior_total_bytes,
        } => Some((intent, prior_total_bytes, None)),
        GrowthCommand::ClaimAutomatic {
            intent,
            prior_total_bytes,
            pin,
        } => Some((intent, prior_total_bytes, Some(pin))),
        _ => None,
    };
    if let Some((intent, prior_total_bytes, expected)) = claim {
        if !intent.valid() {
            return rejected(Fault::Invalid);
        }
        if let Some(old) = load::<GrowthRecord>(tx, &key(intent.claim_id))? {
            return if old.intent == *intent && old.prior_total_bytes == *prior_total_bytes {
                Ok(ControlReply::Growth(old))
            } else {
                rejected(Fault::Conflict)
            };
        }
        if load::<Uuid>(tx, ACTIVE)?.is_some()
            || super::machine::pool_drain::blocks_pool(tx, intent.pool_id)?
            || super::machine::worker_drain::blocks_worker(tx, intent.worker.worker_id)?
        {
            return rejected(Fault::Busy);
        }
        let new_pool = !matches!(intent.action, Action::Expand { .. });
        let pool_claim = pool_claim_key(intent.pool_id);
        if new_pool && load::<Uuid>(tx, &pool_claim)?.is_some() {
            return rejected(Fault::Conflict);
        }
        let Some(policy) = load::<GrowthPolicy>(tx, POLICY)? else {
            return rejected(Fault::NotReady);
        };
        if policy.cost(intent, *prior_total_bytes) != Some(intent.quoted_monthly_cost) {
            return rejected(Fault::Invalid);
        }
        let spent = load::<u64>(tx, SPEND)?.unwrap_or(0);
        if let Some(expected) = expected {
            let Some(automatic) = &policy.automatic else {
                return rejected(Fault::NotReady);
            };
            let revision = load::<u64>(tx, REVISION)?.unwrap_or(0);
            if pin(state, policy.policy_id, revision, spent).as_ref() != Some(expected) {
                return rejected(Fault::Stale);
            }
            let ready = load::<crate::oltp_replication::types::WorkerIdentity>(
                tx,
                &super::machine::ready_key(state.ready_generation, intent.worker.worker_id),
            )?;
            if matches!(intent.action, Action::AddWorker { .. }) {
                if ready.is_some()
                    || load::<WorkerRecord>(
                        tx,
                        &super::machine::key(super::machine::WORKERS, intent.worker.worker_id),
                    )?
                    .is_some()
                    || state.ready.ready_count >= automatic.max_workers as u64
                {
                    return rejected(Fault::NotReady);
                }
            } else if ready != Some(intent.worker) {
                return rejected(Fault::Stale);
            }
        }
        let Some(next) = spent.checked_add(intent.quoted_monthly_cost) else {
            return rejected(Fault::Invalid);
        };
        if next > intent.monthly_spend_ceiling {
            return rejected(Fault::NotReady);
        }
        let record = GrowthRecord {
            intent: intent.clone(),
            phase: GrowthPhase::Claimed,
            attempts: 0,
            last_attempt: None,
            provider_resource: None,
            mounted_total_bytes: None,
            prior_total_bytes: *prior_total_bytes,
        };
        save(tx, &key(intent.claim_id), &record)?;
        if new_pool {
            save(tx, &pool_claim, &intent.claim_id)?;
        }
        save(tx, ACTIVE, &intent.claim_id)?;
        if expected.is_some() {
            save(tx, ACTIVE_AUTOMATIC, &intent.claim_id)?;
        }
        save(tx, SPEND, &next)?;
        advance_revision(tx)?;
        return Ok(ControlReply::Growth(record));
    }
    let claim_id = match command {
        GrowthCommand::Attempt { claim_id, .. }
        | GrowthCommand::Provisioned { claim_id, .. }
        | GrowthCommand::Mounted { claim_id, .. } => *claim_id,
        _ => unreachable!(),
    };
    let Some(mut record) = load::<GrowthRecord>(tx, &key(claim_id))? else {
        return rejected(Fault::NotFound);
    };
    if record.phase != GrowthPhase::Complete && load::<Uuid>(tx, ACTIVE)? != Some(claim_id) {
        return rejected(Fault::Conflict);
    }
    match command {
        GrowthCommand::Attempt { now, .. } => {
            if super::machine::worker_drain::blocks_worker(tx, record.intent.worker.worker_id)? {
                return rejected(Fault::Busy);
            }
            if record.phase == GrowthPhase::Complete
                || (record.phase == GrowthPhase::AwaitingMount
                    && load::<Uuid>(tx, ACTIVE_AUTOMATIC)? == Some(claim_id))
                || record.attempts >= record.intent.max_attempts
            {
                return rejected(Fault::NotReady);
            }
            if record
                .last_attempt
                .is_some_and(|last| *now < last || *now - last < record.intent.cooldown_seconds)
            {
                return rejected(Fault::Busy);
            }
            record.attempts += 1;
            record.last_attempt = Some(*now);
            record.phase = GrowthPhase::Provisioning;
            let last = load::<u64>(tx, LAST_ATTEMPT)?.unwrap_or(0);
            save(tx, LAST_ATTEMPT, &last.max(*now))?;
        }
        GrowthCommand::Provisioned { resource_id, .. } => {
            if super::machine::worker_drain::blocks_worker(tx, record.intent.worker.worker_id)?
                && (record.phase != GrowthPhase::AwaitingMount || record.provider_resource.as_ref() != Some(resource_id))
            {
                return rejected(Fault::Busy);
            }
            if resource_id.is_empty() || resource_id.len() > 512 || record.attempts == 0 {
                return rejected(Fault::Invalid);
            }
            if record
                .provider_resource
                .as_ref()
                .is_some_and(|old| old != resource_id)
            {
                return rejected(Fault::Conflict);
            }
            if record.phase != GrowthPhase::Complete {
                record.phase = GrowthPhase::AwaitingMount;
            }
            record.provider_resource = Some(resource_id.clone());
        }
        GrowthCommand::Mounted { proof, .. } => {
            if record.phase != GrowthPhase::Complete
                && super::machine::worker_drain::blocks_worker(tx, record.intent.worker.worker_id)?
            {
                return rejected(Fault::Busy);
            }
            let enrolled = load::<WorkerRecord>(
                tx,
                &super::machine::key(super::machine::WORKERS, proof.worker.worker_id),
            )?;
            let ready = load::<crate::oltp_replication::types::WorkerIdentity>(
                tx,
                &super::machine::ready_key(state.ready_generation, proof.worker.worker_id),
            )?;
            if state.pending.is_some()
                || enrolled
                    .as_ref()
                    .map(|e| crate::oltp_replication::types::WorkerIdentity::from(&e.enrollment))
                    != Some(proof.worker)
                || ready != Some(proof.worker)
            {
                return rejected(Fault::NotReady);
            }
            if record.provider_resource.as_deref() != Some(proof.provider_volume_id.as_str())
                || proof.worker != record.intent.worker
                || proof.pool_id != record.intent.pool_id
                || proof.pool_generation != record.intent.pool_generation
                || proof.total_bytes < record.intent.requested_bytes()
                || !matches!(
                    record.phase,
                    GrowthPhase::AwaitingMount | GrowthPhase::Complete
                )
            {
                return rejected(Fault::NotReady);
            }
            if record.phase != GrowthPhase::Complete {
                tx.delete(ACTIVE)?;
                tx.delete(ACTIVE_AUTOMATIC)?;
                record.phase = GrowthPhase::Complete;
                record.mounted_total_bytes = Some(proof.total_bytes);
            }
        }
        _ => unreachable!(),
    }
    save(tx, &key(claim_id), &record)?;
    advance_revision(tx)?;
    Ok(ControlReply::Growth(record))
}

pub(super) fn publish_replacement_pool(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    drain: &super::WorkerDrainIntent,
    original: &GrowthRecord,
    permit: &super::EksReplacementPermit,
    proof: &MountedGrowthStamp,
) -> Result<ControlReply, ProtocolError> {
    let claim_id = permit.add_pool_claim;
    let record_key = key(claim_id);
    let Some(mut record) = load::<GrowthRecord>(tx, &record_key)? else {
        return rejected(Fault::NotFound);
    };
    let ready = load::<crate::oltp_replication::types::WorkerIdentity>(
        tx, &super::machine::ready_key(state.ready_generation, proof.worker.worker_id),
    )?;
    if state.pending.is_some()
        || ready != Some(proof.worker)
        || proof.worker != permit.added.mount.worker
        || load::<Uuid>(tx, ACTIVE)? != Some(claim_id)
        || !super::worker_drain::eligible_add_pool(drain, original, &record)
        || record.provider_resource.as_deref() != Some(proof.provider_volume_id.as_str())
        || record.intent.pool_id != proof.pool_id
        || record.intent.pool_generation != proof.pool_generation
        || proof.total_bytes < record.intent.requested_bytes()
    {
        return rejected(Fault::Unauthorized);
    }
    // Keep the original quote, request identity, provider resource and attempts.
    record.phase = GrowthPhase::Complete;
    record.mounted_total_bytes = Some(proof.total_bytes);
    tx.delete(ACTIVE)?;
    tx.delete(ACTIVE_AUTOMATIC)?;
    save(tx, &record_key, &record)?;
    advance_revision(tx)?;
    Ok(ControlReply::Growth(record))
}
