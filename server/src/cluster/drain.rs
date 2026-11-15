//! Pure retained drain metadata, with no local authority.
use super::{
    controller::{Owner, OwnershipObservation},
    member::{Drain, Node, NodeState},
    state::{Control, ControlError, LeaseState},
};
use crate::storage::StorageError;
use uuid::Uuid;
/// Exact retained member association, without current caller authority.
pub(crate) struct DrainBinding {
    node: Node,
}
#[must_use]
#[allow(dead_code)] // Drain reconciliation runtime is deferred.
pub(crate) struct DrainCandidate {
    base: OwnershipObservation,
    owner: Owner,
    result: Control,
}
#[must_use]
#[allow(dead_code)] // Drain reconciliation runtime is deferred.
#[allow(clippy::large_enum_variant)] // A21 requires direct owned variants; one bounded plan per call.
pub(crate) enum DrainPlan {
    Existing(DrainBinding),
    Change(DrainCandidate),
}
#[allow(dead_code)] // Store submission is deferred until runtime integration.
impl DrainCandidate {
    pub(super) fn into_parts(self) -> (OwnershipObservation, Owner, Control) {
        (self.base, self.owner, self.result)
    }
}
impl std::fmt::Debug for DrainBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DrainBinding")
    }
}
impl std::fmt::Debug for DrainCandidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DrainCandidate")
    }
}
impl std::fmt::Debug for DrainPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DrainPlan")
    }
}

fn require(ok: bool) -> Result<(), StorageError> {
    if ok {
        Ok(())
    } else {
        Err(StorageError::Conflict)
    }
}
fn control_error(e: ControlError) -> StorageError {
    match e {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}
fn base(
    root: &Control,
    owner: Owner,
    node_id: Uuid,
) -> Result<(OwnershipObservation, usize), StorageError> {
    let observation = OwnershipObservation::new(root)?;
    require(
        root.controller.lease_state == LeaseState::Held
            && root.controller.owner_id == Some(owner.process_id)
            && root.controller.epoch == owner.epoch,
    )?;
    let index = root
        .nodes
        .binary_search_by_key(&node_id, |n| n.node_id)
        .map_err(|_| StorageError::Conflict)?;
    Ok((observation, index))
}
fn change(
    root: &Control,
    owner: Owner,
    base: OwnershipObservation,
    proposal: Uuid,
    mut result: Control,
) -> Result<DrainPlan, StorageError> {
    require(proposal != root.change_id)?;
    result.revision = root
        .revision
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    result.change_id = proposal;
    result.encode().map_err(control_error)?;
    Ok(DrainPlan::Change(DrainCandidate {
        base,
        owner,
        result,
    }))
}
// A19 retained identity roles. This checks new drain IDs, not retained retries.
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
/// Retained metadata only. Target selection and live authority belong to the caller.
#[allow(dead_code)] // C06 reconciliation runtime caller is deferred.
pub(crate) fn begin_drain(
    root: &Control,
    owner: Owner,
    node_id: Uuid,
    drain_id: Uuid,
    now_ms: u64,
    proposal: Uuid,
) -> Result<DrainPlan, StorageError> {
    let (observation, index) = base(root, owner, node_id)?;
    let node = &root.nodes[index];
    if node.state == NodeState::Draining {
        require(node.drain.is_some())?;
        return Ok(DrainPlan::Existing(DrainBinding { node: node.clone() }));
    }
    require(matches!(node.state, NodeState::Starting | NodeState::Ready))?;
    require(drain_id != proposal && !retained_identity(root, drain_id))?;
    let mut result = root.clone();
    result.nodes[index].state = NodeState::Draining;
    result.nodes[index].drain = Some(Drain {
        id: drain_id,
        requested_at_ms: now_ms,
        ack_seq: None,
        confirmed_idle: false,
    });
    change(root, owner, observation, proposal, result)
}
/// Local invalidation must happen immediately, independently of this durable clear.
/// Retaining an acknowledgement here grants no authenticated idle or stop proof.
#[allow(dead_code)] // C06 reconciliation runtime caller is deferred.
pub(crate) fn clear_idle(
    root: &Control,
    owner: Owner,
    node_id: Uuid,
    drain_id: Uuid,
    proposal: Uuid,
) -> Result<DrainPlan, StorageError> {
    let (observation, index) = base(root, owner, node_id)?;
    let node = &root.nodes[index];
    require(node.state == NodeState::Draining)?;
    let drain = node.drain.as_ref().ok_or(StorageError::Conflict)?;
    require(drain.id == drain_id)?;
    if !drain.confirmed_idle {
        return Ok(DrainPlan::Existing(DrainBinding { node: node.clone() }));
    }
    let mut result = root.clone();
    result.nodes[index]
        .drain
        .as_mut()
        .ok_or(StorageError::Conflict)?
        .confirmed_idle = false;
    change(root, owner, observation, proposal, result)
}
#[cfg(test)]
mod tests;

pub(crate) mod proof;

mod stop;
#[allow(unused_imports)] // C06c3 consumes the private allocation plans.
pub(crate) use stop::{StopPlan, allocate_stop};
