//! Consumed complete inventory planning only; no live authority or I/O.
use super::delivery::StartClaim;
use super::*;
use crate::cluster::{attachment::AttachedConfig, callback::InventoryEvidence};
#[derive(Debug, PartialEq, Eq)]
pub(in crate::cluster) enum ReconcileClass {
    Recovered,
    Absent,
    Quarantined,
    TerminalConflict,
    AlreadyKnown,
    NeedsTerminationReconciliation(ReconcileBinding),
}
#[derive(PartialEq, Eq)]
pub(in crate::cluster) struct ReconcileBinding {
    cluster_id: Uuid,
    node_id: Uuid,
    request_id: Uuid,
    resource_id: String,
}
impl ReconcileBinding {
    /// Consumes an exact classification binding; this is not termination authority.
    #[allow(dead_code)] // C06d will consume the binding after fresh reconciliation.
    pub(in crate::cluster) fn into_parts(self) -> (Uuid, Uuid, Uuid, String) {
        (
            self.cluster_id,
            self.node_id,
            self.request_id,
            self.resource_id,
        )
    }
}
impl std::fmt::Debug for ReconcileBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReconcileBinding { .. }")
    }
}
#[must_use]
#[allow(dead_code)] // Later inventory supervisor consumes these opaque plans.
#[allow(clippy::large_enum_variant)] // Adopted API keeps the concrete consumed candidate inline.
pub(in crate::cluster) enum ReconcilePlan {
    Change {
        candidate: ProvisionCandidate,
        classification: ReconcileClass,
    },
    Unchanged(ReconcileClass),
}
impl std::fmt::Debug for ReconcilePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReconcilePlan { .. }")
    }
}
// This scan result is metadata only. Callers must still select one exact retained binding.
pub(super) enum InventoryClass {
    Clean,
    Quarantined,
    TerminalConflict,
}
#[cfg(test)]
std::thread_local! {
    static COLLISION_CHECKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
fn record_collision_check() {
    COLLISION_CHECKS.with(|count| count.set(count.get().saturating_add(1)));
}
fn terminal(root: &Control, node: Uuid, launch: Uuid) -> bool {
    root.intents.iter().any(|i| {
        i.node_id == node
            && i.launch_id == launch
            && ((i.action == Action::Start
                && i.state == IntentState::Failed
                && matches!(
                    i.outcome,
                    Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
                ))
                || (i.action == Action::Stop
                    && matches!(i.state, IntentState::Completed | IntentState::Failed)
                    && i.outcome == Some(IntentOutcome::Terminated)))
    })
}
pub(super) fn classify_inventory(
    root: &Control,
    owner: Owner,
    inventory: &InventoryEvidence,
) -> Result<InventoryClass, StorageError> {
    let observation = OwnershipObservation::new(root)?;
    if inventory.observation() != &observation
        || inventory.owner() != owner
        || root.controller.lease_state != LeaseState::Held
        || root.controller.owner_id != Some(owner.process_id)
        || root.controller.epoch != owner.epoch
        || root.provisioning != Provisioning::Managed
    {
        return Err(StorageError::Conflict);
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut quarantine = false;
    let mut contradiction = false;
    for r in inventory.records() {
        if r.cluster_id() != root.cluster_id {
            quarantine = true;
            continue;
        }
        let Some(i) = root.intents.iter().find(|i| {
            i.action == Action::Start && i.request_id == r.request_id() && i.node_id == r.node_id()
        }) else {
            quarantine = true;
            continue;
        };
        let Some(n) = root.nodes.iter().find(|n| {
            n.node_id == r.node_id()
                && n.launch_id == Some(i.launch_id)
                && n.ownership == Ownership::Managed
        }) else {
            quarantine = true;
            continue;
        };
        if !seen.insert((r.node_id(), r.request_id())) {
            quarantine = true;
        }
        if n.resource_id
            .as_deref()
            .is_some_and(|v| v != r.resource_id())
            || i.resource_id
                .as_deref()
                .is_some_and(|v| v != r.resource_id())
            || root.nodes.iter().any(|other| {
                #[cfg(test)]
                record_collision_check();
                other.node_id != n.node_id && other.resource_id.as_deref() == Some(r.resource_id())
            })
            || root.intents.iter().any(|other| {
                #[cfg(test)]
                record_collision_check();
                other.node_id != n.node_id && other.resource_id.as_deref() == Some(r.resource_id())
            })
        {
            quarantine = true;
        }
        if terminal(root, n.node_id, i.launch_id)
            && (r.state() != "terminated" || i.outcome == Some(IntentOutcome::NotInvoked))
        {
            contradiction = true;
        }
    }
    Ok(if contradiction {
        InventoryClass::TerminalConflict
    } else if quarantine {
        InventoryClass::Quarantined
    } else {
        InventoryClass::Clean
    })
}
fn target(root: &Control, intent_id: Uuid) -> Result<(usize, usize), StorageError> {
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == intent_id && i.action == Action::Start)
        .ok_or(StorageError::Conflict)?;
    let i = &root.intents[idx];
    let n = root
        .nodes
        .iter()
        .position(|n| {
            n.node_id == i.node_id
                && n.launch_id == Some(i.launch_id)
                && n.ownership == Ownership::Managed
        })
        .ok_or(StorageError::Conflict)?;
    Ok((idx, n))
}
fn recoverable_start(node: &Node, intent: &Intent) -> bool {
    matches!(
        node.state,
        NodeState::Starting | NodeState::Ready | NodeState::Draining
    ) && node.retired_incarnations.is_empty()
        && matches!(
            intent.state,
            IntentState::InProgress | IntentState::Reconciling
        )
        && intent.delivery.is_some()
        && intent.attempts > 0
        && intent.outcome.is_none()
}
fn classification(
    root: &Control,
    owner: Owner,
    idx: usize,
    n: usize,
    inventory: &InventoryEvidence,
) -> Result<ReconcileClass, StorageError> {
    match classify_inventory(root, owner, inventory)? {
        InventoryClass::Quarantined => return Ok(ReconcileClass::Quarantined),
        InventoryClass::TerminalConflict => return Ok(ReconcileClass::TerminalConflict),
        InventoryClass::Clean => (),
    }
    let i = &root.intents[idx];
    // Complete contradiction checks above cover every resource. Another exact
    // recoverable launch can wait for its own round without blocking this one.
    for r in inventory
        .records()
        .iter()
        .filter(|r| r.request_id() != i.request_id)
    {
        let (other, node) = target(root, r.request_id())?;
        let other = &root.intents[other];
        let node = &root.nodes[node];
        let bound = other.resource_id.as_deref() == Some(r.resource_id())
            || node.resource_id.as_deref() == Some(r.resource_id());
        if !bound && !(r.state() != "terminated" && recoverable_start(node, other)) {
            return Ok(ReconcileClass::Quarantined);
        }
    }
    let Some(r) = inventory
        .records()
        .iter()
        .find(|r| r.request_id() == i.request_id && r.node_id() == i.node_id)
    else {
        return Ok(ReconcileClass::Absent);
    };
    if r.state() == "terminated" {
        return Ok(ReconcileClass::NeedsTerminationReconciliation(
            ReconcileBinding {
                cluster_id: root.cluster_id,
                node_id: i.node_id,
                request_id: i.request_id,
                resource_id: r.resource_id().into(),
            },
        ));
    }
    if matches!(root.nodes[n].state, NodeState::Lost | NodeState::Stopped)
        || i.state == IntentState::Failed
        || !root.nodes[n].retired_incarnations.is_empty()
    {
        return Ok(ReconcileClass::TerminalConflict);
    }
    if i.state == IntentState::Completed
        && i.outcome == Some(IntentOutcome::ResourceKnown)
        && root.nodes[n].resource_id.as_deref() == Some(r.resource_id())
    {
        return Ok(ReconcileClass::AlreadyKnown);
    }
    if !recoverable_start(&root.nodes[n], i) {
        return Ok(ReconcileClass::Quarantined);
    }
    Ok(ReconcileClass::Recovered)
}
#[allow(dead_code)] // Later inventory supervisor.
pub(in crate::cluster) fn reconcile_start(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    inventory: InventoryEvidence,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ReconcilePlan, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let (idx, n) = target(root, intent_id)?;
    let classification = classification(root, owner, idx, n, &inventory)?;
    let i = &root.intents[idx];
    let takeover = i.state == IntentState::InProgress
        && i.delivery
            .as_ref()
            .is_some_and(|d| d.owner_id != owner.process_id || d.epoch != owner.epoch);
    let mut result = root.clone();
    match classification {
        ReconcileClass::Quarantined | ReconcileClass::TerminalConflict => inventory_failure(
            root,
            &mut result,
            Some(idx),
            now_ms,
            if classification == ReconcileClass::Quarantined {
                "inventory_quarantined"
            } else {
                "inventory_terminal_conflict"
            },
        )?,
        _ if takeover => {
            result.intents[idx].state = IntentState::Reconciling;
        }
        ReconcileClass::Recovered => {
            let r = inventory
                .records()
                .iter()
                .find(|r| r.request_id() == intent_id)
                .ok_or(StorageError::Conflict)?;
            result.nodes[n].resource_id = Some(r.resource_id().into());
            let i = &mut result.intents[idx];
            i.resource_id = Some(r.resource_id().into());
            i.state = IntentState::Completed;
            i.outcome = Some(IntentOutcome::ResourceKnown);
            i.error_code = None;
        }
        _ => return Ok(ReconcilePlan::Unchanged(classification)),
    }
    Ok(ReconcilePlan::Change {
        candidate: delivery::finish(root, owner, observation, result, proposal)?,
        classification,
    })
}
#[allow(dead_code, clippy::too_many_arguments)] // Consuming one-round retry boundary.
pub(in crate::cluster) fn retry_start(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    inventory: InventoryEvidence,
    invocation_id: Uuid,
    attached: &AttachedConfig,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StartClaim, StorageError> {
    let _ = delivery::base(root, owner, proposal)?;
    let (idx, n) = target(root, intent_id)?;
    if classification(root, owner, idx, n, &inventory)? != ReconcileClass::Absent {
        return Err(StorageError::Conflict);
    }
    delivery::claim_reconciled_start(
        root,
        owner,
        intent_id,
        invocation_id,
        attached,
        now_ms,
        proposal,
    )
}
#[allow(dead_code)] // Later inventory supervisor; failed/partial replies are never evidence.
pub(in crate::cluster) fn inventory_failed_candidate(
    root: &Control,
    owner: Owner,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    let observation = delivery::base(root, owner, proposal)?;
    let mut result = root.clone();
    inventory_failure(root, &mut result, None, now_ms, "inventory_failed")?;
    delivery::finish(root, owner, observation, result, proposal)
}
pub(super) fn inventory_failure(
    root: &Control,
    result: &mut Control,
    idx: Option<usize>,
    now_ms: u64,
    code: &'static str,
) -> Result<(), StorageError> {
    let (failures, backoff) = delivery::scheduler_backoff(root, now_ms)?;
    result.scheduler.failures = failures;
    result.scheduler.backoff_until_ms = backoff;
    if let Some(idx) = idx {
        let i = &mut result.intents[idx];
        // Irreversible witnesses and completed history cannot be rewritten by inventory failure.
        if matches!(
            i.state,
            IntentState::Pending | IntentState::InProgress | IntentState::Reconciling
        ) {
            let retry = now_ms
                .checked_add(delivery::delay(
                    root.timing.retry_initial_ms,
                    root.timing.retry_max_ms,
                    i.attempts,
                ))
                .ok_or(StorageError::LimitExceeded)?;
            i.state = IntentState::Reconciling;
            i.error_code = Some(code.into());
            i.retry_at_ms = i.retry_at_ms.max(retry);
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests;
