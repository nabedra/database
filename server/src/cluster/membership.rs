//! Pure membership calculations; caller assertions grant no live authority.
use super::{
    controller::{Owner, OwnershipObservation},
    member::{
        Action, IntentOutcome, IntentState, Node, NodeState, Ownership, Registration, Session,
    },
    state::{Control, ControlError, LeaseState, Provisioning},
};
use crate::{node::auth::EnrollmentProof, storage::StorageError};
use std::fmt;
use tokio::time::Instant;
use uuid::Uuid;

fn control_error(e: ControlError) -> StorageError {
    match e {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}
fn require(ok: bool) -> Result<(), StorageError> {
    if ok {
        Ok(())
    } else {
        Err(StorageError::Conflict)
    }
}
fn base(root: &Control, owner: Owner) -> Result<OwnershipObservation, StorageError> {
    let observation = OwnershipObservation::new(root)?;
    require(
        root.controller.lease_state == LeaseState::Held
            && root.controller.owner_id == Some(owner.process_id)
            && root.controller.epoch == owner.epoch,
    )?;
    Ok(observation)
}
fn candidate(
    root: &Control,
    owner: Owner,
    base: OwnershipObservation,
    proposal: Uuid,
    mut result: Control,
) -> Result<MembershipCandidate, StorageError> {
    require(proposal != root.change_id)?;
    result.revision = root
        .revision
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    result.change_id = proposal;
    result.validate().map_err(control_error)?;
    Ok(MembershipCandidate {
        base,
        owner,
        result,
    })
}

#[allow(dead_code)] // C03b/runtime consumes this boundary later.
#[allow(clippy::large_enum_variant)] // A16 requires direct owned variants; bounded to one plan per calculation.
pub(crate) enum RegistrationPlan {
    Existing(Node),
    Change(MembershipCandidate),
}
#[allow(dead_code)] // C03b consumes this non-Clone value later.
pub(crate) struct MembershipCandidate {
    base: OwnershipObservation,
    owner: Owner,
    result: Control,
}
#[allow(dead_code)] // Runtime follows C03b.
pub(crate) fn registration_plan(
    root: &Control,
    owner: Owner,
    proof: &EnrollmentProof,
    proposal: Uuid,
) -> Result<RegistrationPlan, StorageError> {
    let observation = base(root, owner)?;
    let e = proof.claims();
    require(e.cluster_id() == root.cluster_id)?;
    require(
        !root
            .nodes
            .iter()
            .any(|n| n.retired_incarnations.contains(&e.incarnation())),
    )?;
    let verifier: String = proof
        .verifier()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let session = Session {
        id: e.session_id(),
        verifier,
    };
    let index = root.nodes.binary_search_by_key(&e.node_id(), |n| n.node_id);
    if root.provisioning == Provisioning::Managed {
        let n = &root.nodes[index.map_err(|_| StorageError::Conflict)?];
        require(
            n.ownership == Ownership::Managed
                && n.launch_id == e.request_id()
                && e.request_id().is_some(),
        )?;
        require(
            e.resources()
                .is_none_or(|r| r.validate_execution(&root.template.execution).is_ok()),
        )?;
        let start = root
            .intents
            .iter()
            .find(|i| {
                i.action == Action::Start
                    && i.node_id == n.node_id
                    && Some(i.request_id) == e.request_id()
            })
            .ok_or(StorageError::Conflict)?;
        require(start.template_fingerprint.as_ref() == Some(&root.config.template_fingerprint))?;
        require(match start.state {
            IntentState::InProgress | IntentState::Reconciling => {
                start.delivery.is_some() && start.attempts > 0
            }
            IntentState::Completed => start.outcome == Some(IntentOutcome::ResourceKnown),
            IntentState::Pending | IntentState::Failed => false,
        })?;
    } else {
        require(e.request_id().is_none())?;
    }
    if let Ok(i) = index {
        let n = &root.nodes[i];
        if n.incarnation == Some(e.incarnation()) {
            require(
                matches!(
                    n.state,
                    NodeState::Starting | NodeState::Ready | NodeState::Draining
                ) && n.registration != Registration::Unregistered
                    && n.session.as_ref() == Some(&session)
                    && n.endpoint.as_deref() == Some(e.endpoint())
                    && n.operation_slots == e.operation_slots()
                    && n.partition_slots == e.partition_slots()
                    && n.queue_size == e.queue_size()
                    && n.resources == e.resources(),
            )?;
            // Exact recovery precedes proposal checks and all advancing arithmetic.
            return Ok(RegistrationPlan::Existing(n.clone()));
        }
    }
    let mut result = root.clone();
    let new_admission = index.is_err();
    let n = match index {
        Ok(i) => {
            let n = &mut result.nodes[i];
            match n.ownership {
                Ownership::Managed => require(
                    n.registration == Registration::Unregistered
                        && matches!(n.state, NodeState::Starting | NodeState::Draining),
                )?,
                Ownership::External => {
                    require(
                        matches!(n.state, NodeState::Lost | NodeState::Stopped)
                            && n.incarnation.is_some()
                            && n.session.as_ref().is_some_and(|s| s.id != e.session_id()),
                    )?;
                    if n.retired_incarnations.len() == 16 {
                        return Err(StorageError::LimitExceeded);
                    }
                    n.retired_incarnations
                        .push(n.incarnation.ok_or(StorageError::Conflict)?);
                    n.retired_incarnations.sort_unstable();
                    n.state = NodeState::Starting;
                    n.drain = None;
                }
            }
            n
        }
        Err(i) => {
            require(root.provisioning == Provisioning::External)?;
            result.nodes.insert(
                i,
                Node {
                    resources: e.resources(),
                    node_id: e.node_id(),
                    launch_id: None,
                    resource_id: Some(format!("external:{}", e.node_id())),
                    ownership: Ownership::External,
                    state: NodeState::Starting,
                    incarnation: None,
                    retired_incarnations: Vec::new(),
                    endpoint: None,
                    operation_slots: e.operation_slots(),
                    partition_slots: e.partition_slots(),
                    queue_size: e.queue_size(),
                    registration: Registration::Unregistered,
                    session: None,
                    drain: None,
                },
            );
            &mut result.nodes[i]
        }
    };
    n.incarnation = Some(e.incarnation());
    n.session = Some(session);
    n.endpoint = Some(e.endpoint().to_owned());
    n.operation_slots = e.operation_slots();
    n.partition_slots = e.partition_slots();
    n.queue_size = e.queue_size();
    n.resources = e.resources();
    n.registration = Registration::Registered;
    let candidate = candidate(root, owner, observation, proposal, result)?;
    if new_admission {
        candidate
            .result
            .check_new_admission()
            .map_err(control_error)?;
        // LATER: route enablement also requires the bounded projected-status encoder gate.
    }
    Ok(RegistrationPlan::Change(candidate))
}
/// Caller assertions after an authenticated initialized-worker/storage probe.
#[allow(dead_code)] // Later probe owner constructs these assertions.
pub(crate) struct ReadinessInput {
    pub(crate) cluster_id: Uuid,
    /// Exact probed member, including endpoint/verifier/resource/launch/capacities.
    pub(crate) member: Node,
    pub(crate) observation: OwnershipObservation,
    pub(crate) challenge: Uuid,
    pub(crate) verified_challenge: Uuid,
    pub(crate) initialized: bool,
}
/// Caller assertions produced and finally rechecked by C03d/runtime.
#[allow(dead_code)] // C03d observation owner follows.
pub(crate) struct ExpiryInput {
    pub(crate) cluster_id: Uuid,
    pub(crate) member: Node,
    pub(crate) observation: OwnershipObservation,
    pub(crate) generation: u64,
    /// Same fresh boot UUID as Owner.process_id; not a runtime authority check.
    pub(crate) controller_boot: Uuid,
    pub(crate) grace_anchor: Instant,
    pub(crate) last_sequence: Option<u64>,
    pub(crate) last_advance: Option<Instant>,
    pub(crate) sampled_at: Instant,
}
#[allow(dead_code)] // Later runtime uses this pure calculation.
pub(crate) fn ready_candidate(
    root: &Control,
    owner: Owner,
    input: &ReadinessInput,
    proposal: Uuid,
) -> Result<MembershipCandidate, StorageError> {
    let observation = base(root, owner)?;
    require(
        input.cluster_id == root.cluster_id
            && input.observation == observation
            && input.initialized
            && input.challenge == input.verified_challenge,
    )?;
    let i = root
        .nodes
        .binary_search_by_key(&input.member.node_id, |n| n.node_id)
        .map_err(|_| StorageError::Conflict)?;
    let n = &root.nodes[i];
    require(
        n == &input.member
            && n.state == NodeState::Starting
            && n.registration == Registration::Registered
            && n.resource_id.is_some(),
    )?;
    let mut result = root.clone();
    result.nodes[i].state = NodeState::Ready;
    result.nodes[i].registration = Registration::Verified;
    candidate(root, owner, observation, proposal, result)
}
#[allow(dead_code)] // C03d/runtime follows.
pub(crate) fn loss_candidate(
    root: &Control,
    owner: Owner,
    input: &ExpiryInput,
    proposal: Uuid,
) -> Result<MembershipCandidate, StorageError> {
    let observation = base(root, owner)?;
    require(
        input.cluster_id == root.cluster_id
            && input.observation == observation
            && input.generation > 0
            && input.controller_boot == owner.process_id,
    )?;
    let i = root
        .nodes
        .binary_search_by_key(&input.member.node_id, |n| n.node_id)
        .map_err(|_| StorageError::Conflict)?;
    let n = &root.nodes[i];
    require(
        n == &input.member
            && matches!(n.state, NodeState::Starting | NodeState::Ready)
            && n.registration != Registration::Unregistered,
    )?;
    let expiry = std::time::Duration::from_millis(root.timing.expiry_ms);
    let grace_end = input
        .grace_anchor
        .checked_add(expiry)
        .ok_or(StorageError::LimitExceeded)?;
    require(input.sampled_at >= grace_end)?;
    match (input.last_sequence, input.last_advance) {
        (Some(sequence), Some(anchor)) => {
            require(sequence > 0)?;
            let end = anchor
                .checked_add(expiry)
                .ok_or(StorageError::LimitExceeded)?;
            require(input.sampled_at >= end)?;
        }
        (None, None) => {}
        _ => return Err(StorageError::Conflict),
    }
    let mut result = root.clone();
    result.nodes[i].state = NodeState::Lost;
    candidate(root, owner, observation, proposal, result)
}

#[allow(dead_code)] // C03b store child consumes this through cluster visibility.
impl MembershipCandidate {
    pub(super) fn into_parts(self) -> (OwnershipObservation, Owner, Control) {
        (self.base, self.owner, self.result)
    }
}
impl fmt::Debug for MembershipCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MembershipCandidate")
    }
}
impl fmt::Debug for RegistrationPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Existing(_) => "RegistrationPlan::Existing",
            Self::Change(_) => "RegistrationPlan::Change",
        })
    }
}
impl fmt::Debug for ReadinessInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadinessInput")
    }
}
impl fmt::Debug for ExpiryInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExpiryInput")
    }
}

#[cfg(test)]
mod tests;
