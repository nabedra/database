//! Proof-bound stop delivery and conservative reconciliation. No callback authority.
use super::delivery::{self, ClaimCommand, ConfirmedClaim, PendingClaim};
use super::*;
use crate::cluster::{
    callback::{ReplyContext, StopCommand},
    drain::proof::{ConfirmedDrain, PendingStopClaim},
    store::OwnershipOutcome,
};

#[must_use]
pub(in crate::cluster) struct StopClaim {
    candidate: ProvisionCandidate,
    pending: PendingStop,
}
#[must_use]
pub(in crate::cluster) struct PendingStop {
    command: PendingClaim,
    proof: PendingStopClaim,
}
#[must_use]
pub(in crate::cluster) struct ConfirmedStop {
    command: ConfirmedClaim,
    proof: ConfirmedDrain,
}
macro_rules! redacted {
    ($($t:ty),*) => {$(impl std::fmt::Debug for $t {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(stringify!($t))
        }
    })*};
}
redacted!(StopClaim, PendingStop, ConfirmedStop);
impl StopClaim {
    #[allow(dead_code)] // Later supervised submission caller.
    pub(in crate::cluster) fn into_parts(self) -> (ProvisionCandidate, PendingStop) {
        (self.candidate, self.pending)
    }
}
impl PendingStop {
    #[allow(dead_code)] // Later supervised submission caller.
    pub(in crate::cluster) fn confirm(
        self,
        outcome: OwnershipOutcome,
    ) -> Result<ConfirmedStop, StorageError> {
        // Check both complete associations against this one actual outcome before
        // consuming it. A failed or expired proof drops the command as well.
        let command_matches = self.command.matches_outcome(&outcome);
        let proof_matches = self.proof.matches_outcome(&outcome);
        if !command_matches || !proof_matches {
            return Err(StorageError::Conflict);
        }
        let command = self
            .command
            .confirm(&outcome)
            .ok_or(StorageError::Conflict)?;
        let proof = self.proof.confirm(outcome)?;
        Ok(ConfirmedStop { command, proof })
    }
}
impl ConfirmedStop {
    /// The caller must consume check_live and freshly authorize the exact root
    /// and captured generation immediately before its one HTTP attempt.
    #[allow(dead_code)] // Later supervised delivery caller.
    pub(in crate::cluster) fn into_parts(self) -> (Vec<u8>, ReplyContext, ConfirmedDrain) {
        let (bytes, context) = self.command.into_parts();
        (bytes, context, self.proof)
    }
}
#[allow(dead_code)] // Later coordinator owns fresh authorization and submission.
#[allow(clippy::too_many_arguments)]
pub(in crate::cluster) fn claim_stop(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    proof: ConfirmedDrain,
    invocation_id: Uuid,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StopClaim, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    let i = &root.intents[idx];
    let node = root
        .nodes
        .iter()
        .find(|n| n.node_id == i.node_id && n.launch_id == Some(i.launch_id))
        .ok_or(StorageError::Conflict)?;
    if i.action != Action::Stop
        || i.state != IntentState::Pending
        || i.attempts != 0
        || i.delivery.is_some()
        || i.error_code.is_some()
        || i.outcome.is_some()
        || i.template_fingerprint.is_some()
        || node.ownership != Ownership::Managed
        || node.state != NodeState::Draining
        || node.registration == Registration::Unregistered
        || node.session.is_none()
        || node.incarnation.is_none()
        || node.resource_id.is_none()
        || i.incarnation_id != node.incarnation
        || i.resource_id != node.resource_id
        || now_ms < i.retry_at_ms
        || now_ms < root.scheduler.backoff_until_ms
        || retained_identity(root, invocation_id)
        || invocation_id == proposal
    {
        return Err(StorageError::Conflict);
    }
    // Reject contradictory histories, including no-effect/terminal starts and
    // any competing stop association. Codec validity alone proves no history.
    for other in &root.intents {
        let related = other.node_id == i.node_id
            || other.launch_id == i.launch_id
            || other.resource_id == i.resource_id
            || other.incarnation_id == i.incarnation_id;
        let contradictory = other.node_id != i.node_id
            || other.launch_id != i.launch_id
            || matches!(
                other.outcome,
                Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
            )
            || (other.action == Action::Stop && other.request_id != intent_id);
        if related && contradictory {
            return Err(StorageError::Conflict);
        }
    }
    construct_claim(
        root,
        owner,
        idx,
        proof,
        invocation_id,
        observation,
        proposal,
    )
}
#[allow(clippy::too_many_arguments)]
fn construct_claim(
    root: &Control,
    owner: Owner,
    idx: usize,
    proof: ConfirmedDrain,
    invocation_id: Uuid,
    observation: OwnershipObservation,
    proposal: Uuid,
) -> Result<StopClaim, StorageError> {
    let mut prospective = root.clone();
    if let Some(d) = &mut prospective.intents[idx].delivery {
        d.invocation_id = invocation_id;
        d.owner_id = owner.process_id;
        d.epoch = owner.epoch;
    }
    let command = StopCommand::build(
        &prospective,
        owner,
        root.intents[idx].request_id,
        invocation_id,
    )?;

    let digest = command.digest().to_owned();
    let prepared = delivery::prepare_claim(
        root,
        owner,
        idx,
        invocation_id,
        ClaimCommand::Stop(command),
        observation,
        proposal,
    )?;
    let (candidate, command) = prepared.into_parts();
    let proof = proof.prepare_stop_claim(root, &candidate.result, &digest)?;
    Ok(StopClaim {
        candidate,
        pending: PendingStop { command, proof },
    })
}

// Check retained known-resource history separately from delivery correlation.
fn target(root: &Control, intent_id: Uuid) -> Result<usize, StorageError> {
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    let i = &root.intents[idx];
    let n = root
        .nodes
        .iter()
        .find(|n| n.node_id == i.node_id && n.launch_id == Some(i.launch_id))
        .ok_or(StorageError::Conflict)?;
    if i.action != Action::Stop
        || i.outcome.is_some()
        || i.template_fingerprint.is_some()
        || n.ownership != Ownership::Managed
        || n.state != NodeState::Draining
        || n.registration == Registration::Unregistered
        || n.session.is_none()
        || n.incarnation.is_none()
        || n.resource_id.is_none()
        || i.incarnation_id != n.incarnation
        || i.resource_id != n.resource_id
        || !root.intents.iter().any(|s| {
            s.action == Action::Start
                && s.request_id == i.launch_id
                && s.node_id == i.node_id
                && s.resource_id
                    .as_ref()
                    .is_none_or(|r| Some(r) == i.resource_id.as_ref())
        })
        || root.intents.iter().any(|other| {
            let related = other.node_id == i.node_id
                || other.launch_id == i.launch_id
                || other.resource_id == i.resource_id
                || other.incarnation_id == i.incarnation_id;
            related
                && (other.node_id != i.node_id
                    || other.launch_id != i.launch_id
                    || matches!(
                        other.outcome,
                        Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
                    )
                    || (other.action == Action::Stop && other.request_id != intent_id))
        })
    {
        return Err(StorageError::Conflict);
    }
    Ok(idx)
}
fn current(root: &Control, owner: Owner, ctx: &ReplyContext) -> Result<usize, StorageError> {
    let id = ctx.intent_id().ok_or(StorageError::Conflict)?;
    ctx.matches_delivery(root, owner, id)?;
    target(root, id)
}
#[allow(dead_code)] // Later supervised delivery caller.
pub(in crate::cluster) fn complete_stop(
    root: &Control,
    owner: Owner,
    ctx: &ReplyContext,
    reply: crate::cluster::callback::CallbackReply,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    use crate::cluster::callback::CallbackReply;
    let observation = delivery::base(root, owner, proposal)?;
    let idx = current(root, owner, ctx)?;
    let mut result = root.clone();
    match reply {
        CallbackReply::Stopped => result.intents[idx].state = IntentState::Reconciling,
        CallbackReply::Failure { code, outcome } => {
            let definite = delivery::failure_is_definite(&code, &outcome)?
                && root.intents[idx].attempts == 1
                && root.intents[idx].error_code.is_none();
            delivery::failure_candidate(root, &mut result, idx, now_ms, definite)?;
        }
        _ => return Err(StorageError::Conflict),
    }
    delivery::finish(root, owner, observation, result, proposal)
}
#[allow(dead_code)] // Transport/malformed/lost evidence has no no-effect proof.
pub(in crate::cluster) fn uncertain_stop(
    root: &Control,
    owner: Owner,
    ctx: &ReplyContext,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let idx = current(root, owner, ctx)?;
    let mut result = root.clone();
    delivery::failure_candidate(root, &mut result, idx, now_ms, false)?;
    delivery::finish(root, owner, observation, result, proposal)
}
#[derive(PartialEq, Eq)]
pub(in crate::cluster) enum StopInventoryClass {
    Retryable,
    Quarantined,
    TerminalConflict,
    NeedsTermination,
}
impl std::fmt::Debug for StopInventoryClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StopInventoryClass { .. }")
    }
}
#[must_use]
#[allow(dead_code, clippy::large_enum_variant)] // Later inventory supervisor consumes the concrete plan.
pub(in crate::cluster) enum StopReconcilePlan {
    Change {
        candidate: ProvisionCandidate,
        classification: StopInventoryClass,
    },
    Unchanged(StopInventoryClass),
}
impl std::fmt::Debug for StopReconcilePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StopReconcilePlan { .. }")
    }
}
fn classification(
    root: &Control,
    owner: Owner,
    idx: usize,
    inventory: &crate::cluster::callback::InventoryEvidence,
) -> Result<StopInventoryClass, StorageError> {
    use super::reconcile::{InventoryClass, classify_inventory};
    match classify_inventory(root, owner, inventory)? {
        InventoryClass::Quarantined => return Ok(StopInventoryClass::Quarantined),
        InventoryClass::TerminalConflict => return Ok(StopInventoryClass::TerminalConflict),
        InventoryClass::Clean => (),
    }
    let i = &root.intents[idx];
    // The shared scanner establishes identities/collisions. Unselected records
    // must already be bound or remain eligible for their own live recovery.
    for r in inventory
        .records()
        .iter()
        .filter(|r| r.request_id() != i.launch_id)
    {
        let start = root
            .intents
            .iter()
            .find(|s| {
                s.action == Action::Start
                    && s.request_id == r.request_id()
                    && s.node_id == r.node_id()
            })
            .ok_or(StorageError::Conflict)?;
        let node = root
            .nodes
            .iter()
            .find(|n| n.node_id == r.node_id() && n.launch_id == Some(start.launch_id))
            .ok_or(StorageError::Conflict)?;
        let bound = start.resource_id.as_deref() == Some(r.resource_id())
            || node.resource_id.as_deref() == Some(r.resource_id());
        let recoverable = r.state() != "terminated"
            && matches!(
                node.state,
                NodeState::Starting | NodeState::Ready | NodeState::Draining
            )
            && node.retired_incarnations.is_empty()
            && matches!(
                start.state,
                IntentState::InProgress | IntentState::Reconciling
            )
            && start.delivery.is_some()
            && start.attempts > 0
            && start.outcome.is_none();
        if !bound && !recoverable {
            return Ok(StopInventoryClass::Quarantined);
        }
    }
    if inventory.records().iter().any(|r| {
        r.node_id() == i.node_id
            && r.request_id() == i.launch_id
            && Some(r.resource_id()) == i.resource_id.as_deref()
            && r.state() == "terminated"
    }) {
        return Ok(StopInventoryClass::NeedsTermination);
    }
    Ok(StopInventoryClass::Retryable)
}
#[allow(dead_code)] // Consumes exact current-owner/current-root complete evidence.
pub(in crate::cluster) fn reconcile_stop(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    inventory: crate::cluster::callback::InventoryEvidence,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StopReconcilePlan, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let idx = target(root, intent_id)?;
    let i = &root.intents[idx];
    if !matches!(i.state, IntentState::InProgress | IntentState::Reconciling)
        || i.attempts == 0
        || i.delivery.is_none()
    {
        return Err(StorageError::Conflict);
    }
    let class = classification(root, owner, idx, &inventory)?;
    let mut result = root.clone();
    match class {
        StopInventoryClass::Quarantined | StopInventoryClass::TerminalConflict => {
            super::reconcile::inventory_failure(
                root,
                &mut result,
                Some(idx),
                now_ms,
                if class == StopInventoryClass::Quarantined {
                    "inventory_quarantined"
                } else {
                    "inventory_terminal_conflict"
                },
            )?
        }
        StopInventoryClass::NeedsTermination => return Ok(StopReconcilePlan::Unchanged(class)),
        _ if i.state == IntentState::InProgress
            && i.delivery
                .as_ref()
                .is_some_and(|d| d.owner_id != owner.process_id || d.epoch != owner.epoch) =>
        {
            result.intents[idx].state = IntentState::Reconciling
        }
        _ => return Ok(StopReconcilePlan::Unchanged(class)),
    }
    Ok(StopReconcilePlan::Change {
        candidate: delivery::finish(root, owner, observation, result, proposal)?,
        classification: class,
    })
}
#[allow(dead_code, clippy::too_many_arguments)] // One consumed fresh inventory and drain round.
pub(in crate::cluster) fn retry_stop(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    proof: ConfirmedDrain,
    inventory: crate::cluster::callback::InventoryEvidence,
    invocation_id: Uuid,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StopClaim, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let idx = target(root, intent_id)?;
    let i = &root.intents[idx];
    if i.state != IntentState::Reconciling
        || i.delivery.is_none()
        || i.attempts == 0
        || now_ms < i.retry_at_ms
        || now_ms < root.scheduler.backoff_until_ms
        || retained_identity(root, invocation_id)
        || invocation_id == proposal
        || classification(root, owner, idx, &inventory)? != StopInventoryClass::Retryable
    {
        return Err(StorageError::Conflict);
    }
    construct_claim(
        root,
        owner,
        idx,
        proof,
        invocation_id,
        observation,
        proposal,
    )
}

#[cfg(test)]
mod tests;
