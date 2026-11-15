//! Reserved first-stop metadata only; callers retain live authorization duties.
use super::proof::{ConfirmedDrain, PendingStopAllocation};
use super::*;
use crate::cluster::member::{Action, Intent, IntentState, Ownership, Registration};

#[must_use]
#[allow(dead_code)] // C06c3 consumes allocation plans.
#[allow(clippy::large_enum_variant)] // Direct closed owners, bounded by Control.
pub(crate) enum StopPlan {
    Existing(ExistingStop),
    Change(DrainCandidate, PendingStopAllocation),
}
/// Retains the consumed proof and exact retained identity without granting authority.
#[must_use]
#[allow(dead_code)] // C06c3 performs fresh live checks before claim preparation.
pub(crate) struct ExistingStop {
    proof: ConfirmedDrain,
    stop_id: Uuid,
}
#[allow(dead_code)] // C06c3 consuming handoff.
impl ExistingStop {
    pub(crate) fn into_parts(self) -> (ConfirmedDrain, Uuid) {
        (self.proof, self.stop_id)
    }
}
impl std::fmt::Debug for StopPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StopPlan")
    }
}
impl std::fmt::Debug for ExistingStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExistingStop")
    }
}
/// Callers must consume check_live using actual observations/guard and perform
/// final fresh authorization before submission. This establishes no live authority.
#[allow(dead_code)] // C06c3 integration is deferred.
pub(crate) fn allocate_stop(
    root: &Control,
    owner: Owner,
    proof: ConfirmedDrain,
    stop_id: Uuid,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StopPlan, StorageError> {
    proof.check_for_stop(root, owner, proof.captured_generation())?;
    let (observation, index) = base(root, owner, proof.node_id())?;
    let node = &root.nodes[index];
    require(
        node.ownership == Ownership::Managed
            && node.launch_id.is_some()
            && node.resource_id.is_some()
            && node.incarnation.is_some()
            && node.registration != Registration::Unregistered
            && node.session.is_some(),
    )?;
    let launch = node.launch_id.ok_or(StorageError::Conflict)?;
    require(root.intents.iter().any(|i| {
        i.action == Action::Start
            && i.request_id == launch
            && i.launch_id == launch
            && i.node_id == node.node_id
    }))?;
    // Scan all retained stops before converging. Never silently select from
    // contradictory or ambiguous history, including terminal records.
    let mut retained = None;
    for intent in root.intents.iter().filter(|i| i.action == Action::Stop) {
        if intent.node_id == node.node_id
            || intent.launch_id == launch
            || intent.resource_id == node.resource_id
            || intent.incarnation_id == node.incarnation
        {
            require(
                intent.node_id == node.node_id
                    && intent.launch_id == launch
                    && intent.resource_id == node.resource_id
                    && intent.incarnation_id == node.incarnation,
            )?;
            require(retained.replace(intent.request_id).is_none())?;
        }
    }
    if let Some(stop_id) = retained {
        return Ok(StopPlan::Existing(ExistingStop { proof, stop_id }));
    }
    require(stop_id != proposal && !retained_identity(root, stop_id))?;
    let deadline_ms = now_ms
        .checked_add(root.timing.drain_ms)
        .ok_or(StorageError::LimitExceeded)?;
    let mut result = root.clone();
    result.intents.push(Intent {
        request_id: stop_id,
        node_id: node.node_id,
        launch_id: launch,
        incarnation_id: node.incarnation,
        resource_id: node.resource_id.clone(),
        action: Action::Stop,
        state: IntentState::Pending,
        attempts: 0,
        delivery: None,
        error_code: None,
        outcome: None,
        template_fingerprint: None,
        created_at_ms: now_ms,
        retry_at_ms: now_ms,
        deadline_ms,
    });
    result.intents.sort_by_key(|i| i.request_id);
    let before = root.capacity().map_err(control_error)?;
    let after = result.capacity().map_err(control_error)?;
    let sum = |a: usize, b: usize| a.checked_add(b).ok_or(StorageError::LimitExceeded);
    require(
        after.nodes == before.nodes
            && after.intents
                == before
                    .intents
                    .checked_add(1)
                    .ok_or(StorageError::LimitExceeded)?
            && after.credits
                == before
                    .credits
                    .checked_sub(1)
                    .ok_or(StorageError::LimitExceeded)?
            && sum(after.intents, after.credits)? == sum(before.intents, before.credits)?
            && sum(after.nodes, sum(after.intents, after.credits)?)?
                == sum(before.nodes, sum(before.intents, before.credits)?)?
            && after.admission_bytes == before.admission_bytes,
    )?;
    let DrainPlan::Change(candidate) = change(root, owner, observation, proposal, result)? else {
        return Err(StorageError::Conflict);
    };
    let (candidate, pending) = proof.prepare_stop_successor(candidate)?;
    Ok(StopPlan::Change(candidate, pending))
}
#[cfg(test)]
mod tests;
