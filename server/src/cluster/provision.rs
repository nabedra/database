//! Pure managed launch reservations. These values grant no live authority.
use super::{
    controller::{Owner, OwnershipObservation},
    member::{
        Action, Intent, IntentOutcome, IntentState, Node, NodeState, Ownership, Registration,
    },
    state::{Control, ControlError, LeaseState, Provisioning},
};
use crate::storage::StorageError;
use uuid::Uuid;
#[must_use]
#[allow(dead_code)] // Pending provisioning runtime and later minis.
pub(crate) struct ProvisionCandidate {
    observation: OwnershipObservation,
    owner: Owner,
    result: Control,
}
impl ProvisionCandidate {
    pub(super) fn into_parts(self) -> (OwnershipObservation, Owner, Control) {
        (self.observation, self.owner, self.result)
    }
}
fn control_error(e: ControlError) -> StorageError {
    match e {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}
#[allow(dead_code)] // Pending provisioning runtime.
pub(crate) fn managed_capacity(root: &Control) -> Result<u64, StorageError> {
    root.validate().map_err(control_error)?;
    let terminal: std::collections::BTreeSet<_> = root
        .intents
        .iter()
        .filter(|i| {
            (i.action == Action::Start
                && i.state == IntentState::Failed
                && matches!(
                    i.outcome,
                    Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
                ))
                || (i.action == Action::Stop
                    && matches!(i.state, IntentState::Completed | IntentState::Failed)
                    && i.outcome == Some(IntentOutcome::Terminated))
        })
        .map(|i| (i.node_id, i.launch_id))
        .collect();
    Ok(root
        .nodes
        .iter()
        .filter(|n| {
            n.ownership == Ownership::Managed
                && !n
                    .launch_id
                    .is_some_and(|l| terminal.contains(&(n.node_id, l)))
        })
        .count() as u64)
}
fn retained_identity(root: &Control, id: Uuid) -> bool {
    id == root.cluster_id
        || id == root.change_id
        || root.admin_head == Some(id)
        || root.controller.owner_id == Some(id)
        || root.nodes.iter().any(|n| {
            n.node_id == id
                || n.launch_id == Some(id)
                || n.incarnation == Some(id)
                || n.retired_incarnations.contains(&id)
                || n.session.as_ref().is_some_and(|s| s.id == id)
                || n.drain.as_ref().is_some_and(|d| d.id == id)
        })
        || root.intents.iter().any(|i| {
            i.request_id == id
                || i.node_id == id
                || i.launch_id == id
                || i.incarnation_id == Some(id)
                || i.delivery
                    .as_ref()
                    .is_some_and(|d| d.invocation_id == id || d.owner_id == id)
        })
}
/// Metadata reservation only. Runtime must also enforce projected status,
/// complete fresh inventory reconciliation and current live authority gates.
#[allow(dead_code)] // Pending provisioning runtime.
pub(crate) fn allocate_start(
    root: &Control,
    owner: Owner,
    node_id: Uuid,
    launch_id: Uuid,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    let observation = OwnershipObservation::new(root)?;
    if root.controller.lease_state != LeaseState::Held
        || root.controller.owner_id != Some(owner.process_id)
        || root.controller.epoch != owner.epoch
        || root.provisioning != Provisioning::Managed
        || now_ms < root.scheduler.backoff_until_ms
        || managed_capacity(root)? >= root.policy.desired
        || proposal == root.change_id
        || node_id == launch_id
        || retained_identity(root, node_id)
        || retained_identity(root, launch_id)
    {
        return Err(StorageError::Conflict);
    }
    let revision = root
        .revision
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    let deadline_ms = now_ms
        .checked_add(root.timing.readiness_ms)
        .ok_or(StorageError::LimitExceeded)?;
    let mut result = root.clone();
    result.revision = revision;
    result.change_id = proposal;
    let node = Node {
        resources: Some(root.template.worker_resources()),
        node_id,
        launch_id: Some(launch_id),
        resource_id: None,
        ownership: Ownership::Managed,
        state: NodeState::Starting,
        incarnation: None,
        retired_incarnations: vec![],
        endpoint: None,
        operation_slots: crate::node::queue_runtime::coordination_capacity(&root.template)
            .map_err(|_| StorageError::LimitExceeded)? as u64,
        partition_slots: crate::node::queue_runtime::job_coordination_capacity(&root.template)
            .map_err(|_| StorageError::LimitExceeded)? as u64,
        queue_size: root.template.queue_size,
        registration: Registration::Unregistered,
        session: None,
        drain: None,
    };
    let intent = Intent {
        request_id: launch_id,
        node_id,
        launch_id,
        incarnation_id: None,
        resource_id: None,
        action: Action::Start,
        state: IntentState::Pending,
        attempts: 0,
        created_at_ms: now_ms,
        deadline_ms,
        retry_at_ms: now_ms,
        error_code: None,
        template_fingerprint: Some(root.config.template_fingerprint.clone()),
        delivery: None,
        outcome: None,
    };
    let at = result.nodes.partition_point(|n| n.node_id < node_id);
    result.nodes.insert(at, node);
    let at = result.intents.partition_point(|i| i.request_id < launch_id);
    result.intents.insert(at, intent);
    result.check_new_admission().map_err(control_error)?;
    Ok(ProvisionCandidate {
        observation,
        owner,
        result,
    })
}
#[cfg(test)]
mod tests;

mod delivery;
#[allow(unused_imports)] // Later supervised delivery caller.
pub(super) use delivery::{
    ConfirmedStart, PendingStart, StartClaim, claim_start, complete_start, uncertain_start,
};

mod reconcile;
#[allow(unused_imports)] // Later supervised inventory caller.
pub(super) use reconcile::{
    ReconcileClass, ReconcilePlan, inventory_failed_candidate, reconcile_start, retry_start,
};

mod stop;
#[allow(unused_imports)] // Later supervised stop delivery caller.
pub(super) use stop::{
    ConfirmedStop, PendingStop, StopClaim, StopInventoryClass, StopReconcilePlan, claim_stop,
    complete_stop, reconcile_stop, retry_stop, uncertain_stop,
};

mod termination;
#[allow(unused_imports)] // Later supervised terminal inventory caller.
pub(super) use termination::{TerminationClass, TerminationPlan, reconcile_termination};

/// Persist one bounded desired-capacity adjustment. Demand is observed by the
/// live owner; no accepted request record is ever changed by this transition.
pub(crate) fn demand_candidate(
    root: &Control,
    owner: Owner,
    pending: u64,
    running: u64,
    oldest_wait_ms: u64,
    now_ms: u64,
    proposal: Uuid,
) -> Result<Option<ProvisionCandidate>, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    if root.policy.mode != super::state::PolicyMode::Autoscale
        || now_ms < root.scheduler.cooldown_until_ms
        || now_ms < root.scheduler.backoff_until_ms
    {
        return Ok(None);
    }
    let mut result = root.clone();
    let pressure = pending > 0 && oldest_wait_ms >= root.timing.stabilization_ms;
    if pressure && root.policy.desired < root.policy.max_nodes {
        result.policy.desired += 1;
        result.scheduler.low_since_ms = None;
    } else if pending == 0 && running == 0 && root.policy.desired > root.policy.min_nodes {
        match root.scheduler.low_since_ms {
            None => result.scheduler.low_since_ms = Some(now_ms),
            Some(since) if now_ms.saturating_sub(since) >= root.timing.stabilization_ms => {
                result.policy.desired -= 1;
                result.scheduler.low_since_ms = None;
            }
            _ => return Ok(None),
        }
    } else if root.scheduler.low_since_ms.is_some() {
        result.scheduler.low_since_ms = None;
    } else {
        return Ok(None);
    }
    if result.policy.desired != root.policy.desired {
        result.policy.revision = result
            .policy
            .revision
            .checked_add(1)
            .ok_or(StorageError::LimitExceeded)?;
        result.scheduler.cooldown_until_ms = now_ms
            .checked_add(root.timing.cooldown_ms)
            .ok_or(StorageError::LimitExceeded)?;
    }
    Ok(Some(delivery::finish(
        root,
        owner,
        observation,
        result,
        proposal,
    )?))
}

/// Complete inventory must contain no unknown launch, resource collision or
/// contradiction with a retained terminal decision before capacity changes.
pub(super) fn inventory_clean(
    root: &Control,
    owner: Owner,
    inventory: &crate::cluster::callback::InventoryEvidence,
) -> Result<bool, StorageError> {
    Ok(matches!(
        reconcile::classify_inventory(root, owner, inventory)?,
        reconcile::InventoryClass::Clean
    ))
}
