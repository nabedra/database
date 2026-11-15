//! Consumed exact terminal inventory planning. No provider or live authority.
use super::*;
use crate::cluster::callback::InventoryEvidence;
#[derive(Debug, PartialEq, Eq)]
pub(in crate::cluster) enum TerminationClass {
    Absent,
    Pending,
    Quarantined,
    TerminalConflict,
    AlreadyTerminated,
}
#[must_use]
#[allow(dead_code, clippy::large_enum_variant)] // Concrete consumed candidate; later runtime caller.
pub(in crate::cluster) enum TerminationPlan {
    Change(ProvisionCandidate),
    Unchanged(TerminationClass),
}
impl std::fmt::Debug for TerminationPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TerminationPlan { .. }")
    }
}
// Indexed once per invocation. Resource collision checks belong to classify_inventory.
struct History {
    start: usize,
    stop: Option<usize>,
}
fn history(root: &Control, indices: &[usize], resource: &str) -> Result<History, TerminationClass> {
    let mut start = None;
    let mut stop = None;
    let mut invocations = std::collections::BTreeSet::new();
    for &idx in indices {
        let i = &root.intents[idx];
        if let Some(d) = &i.delivery {
            if i.attempts == 0
                || i.state == IntentState::Pending
                || d.epoch > root.controller.epoch
                || (d.epoch == root.controller.epoch
                    && Some(d.owner_id) != root.controller.owner_id)
                || !invocations.insert(d.invocation_id)
            {
                return Err(TerminationClass::Quarantined);
            }
        } else if i.attempts != 0 {
            return Err(TerminationClass::Quarantined);
        }
        if i.resource_id.as_deref().is_some_and(|r| r != resource) {
            return Err(TerminationClass::Quarantined);
        }
        if i.action == Action::Start {
            if start.replace(idx).is_some() {
                return Err(TerminationClass::Quarantined);
            }
            if i.state == IntentState::Failed && i.outcome == Some(IntentOutcome::NotInvoked) {
                return Err(TerminationClass::TerminalConflict);
            }
            if i.attempts == 0 || i.delivery.is_none() || i.state == IntentState::Pending {
                return Err(TerminationClass::Quarantined);
            }
        } else if stop.replace(idx).is_some() {
            // No protocol for choosing among multiple retained stops is granted here.
            return Err(TerminationClass::Quarantined);
        }
    }
    let start = start.ok_or(TerminationClass::Quarantined)?;
    if stop.is_some() && root.intents[start].outcome == Some(IntentOutcome::Terminated) {
        return Err(TerminationClass::TerminalConflict);
    }
    Ok(History { start, stop })
}
#[allow(dead_code)] // Later supervised inventory caller.
pub(in crate::cluster) fn reconcile_termination(
    root: &Control,
    owner: Owner,
    node_id: Uuid,
    inventory: InventoryEvidence,
    now_ms: u64,
    proposal: Uuid,
) -> Result<TerminationPlan, StorageError> {
    use super::reconcile::{InventoryClass, classify_inventory, inventory_failure};
    // Validates complete root, exact held owner and complete evidence association
    // before inspecting a target or allowing unchanged convergence.
    let common = classify_inventory(root, owner, &inventory)?;
    let n = root
        .nodes
        .iter()
        .position(|n| n.node_id == node_id && n.ownership == Ownership::Managed)
        .ok_or(StorageError::Conflict)?;
    let mut by_node = std::collections::BTreeMap::<Uuid, Vec<usize>>::new();
    for (idx, i) in root.intents.iter().enumerate() {
        by_node.entry(i.node_id).or_default().push(idx);
    }
    let indices = by_node.get(&node_id).ok_or(StorageError::Conflict)?;
    let start = indices
        .iter()
        .copied()
        .find(|&idx| root.intents[idx].action == Action::Start)
        .ok_or(StorageError::Conflict)?;
    let mut class = match common {
        InventoryClass::Clean => None,
        InventoryClass::Quarantined => Some(TerminationClass::Quarantined),
        InventoryClass::TerminalConflict => Some(TerminationClass::TerminalConflict),
    };
    // Every independent record must have eligible retained history. In particular,
    // another exact ambiguous terminated launch may await its own fresh round.
    if class.is_none() {
        for r in inventory.records() {
            let indices = by_node.get(&r.node_id()).ok_or(StorageError::Conflict)?;
            if let Err(c) = history(root, indices, r.resource_id()) {
                class = Some(c);
                break;
            }
        }
    }
    if let Some(class) = class {
        let observation = delivery::base(root, owner, proposal)?;
        let mut result = root.clone();
        // Pending has no dispatched attempt to reconcile. Degrade globally only.
        let idx = (root.intents[start].state != IntentState::Pending).then_some(start);
        inventory_failure(
            root,
            &mut result,
            idx,
            now_ms,
            if class == TerminationClass::TerminalConflict {
                "inventory_terminal_conflict"
            } else {
                "inventory_quarantined"
            },
        )?;
        return Ok(TerminationPlan::Change(delivery::finish(
            root,
            owner,
            observation,
            result,
            proposal,
        )?));
    }
    let Some(r) = inventory.records().iter().find(|r| r.node_id() == node_id) else {
        return Ok(TerminationPlan::Unchanged(TerminationClass::Absent));
    };
    if r.state() != "terminated" {
        return Ok(TerminationPlan::Unchanged(TerminationClass::Pending));
    }
    // Already checked above for every record; retain the selected exact indices.
    let h = history(root, indices, r.resource_id()).map_err(|_| StorageError::Conflict)?;
    let terminal = &root.intents[h.stop.unwrap_or(h.start)];
    if terminal.outcome == Some(IntentOutcome::Terminated) {
        if root.nodes[n].state == NodeState::Stopped
            && root.nodes[n].resource_id.as_deref() == Some(r.resource_id())
            && terminal.resource_id.as_deref() == Some(r.resource_id())
        {
            return Ok(TerminationPlan::Unchanged(
                TerminationClass::AlreadyTerminated,
            ));
        }
        return Err(StorageError::Conflict);
    }
    let observation = delivery::base(root, owner, proposal)?;
    let mut result = root.clone();
    result.nodes[n].state = NodeState::Stopped;
    result.nodes[n].resource_id = Some(r.resource_id().into());
    let i = &mut result.intents[h.stop.unwrap_or(h.start)];
    i.resource_id = Some(r.resource_id().into());
    i.state = if h.stop.is_some() {
        IntentState::Completed
    } else {
        IntentState::Failed
    };
    i.outcome = Some(IntentOutcome::Terminated);
    Ok(TerminationPlan::Change(delivery::finish(
        root,
        owner,
        observation,
        result,
        proposal,
    )?))
}
#[cfg(test)]
mod tests;
