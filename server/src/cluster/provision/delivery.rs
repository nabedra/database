//! Pure closed delivery metadata; no live authority or I/O.
use super::*;
use crate::cluster::{
    attachment::AttachedConfig,
    callback::{CallbackReply, DeliveryOutcome, ReplyContext, StartCommand, StopCommand},
    member::Delivery,
    store::OwnershipOutcome,
};

// The stop branch is reserved for C06c3's proof-bound typed constructor.
pub(super) enum ClaimCommand {
    Start(StartCommand),
    #[allow(dead_code)] // Only future StopClaim may construct this after eligibility.
    Stop(StopCommand),
}
impl ClaimCommand {
    fn digest(&self) -> &str {
        match self {
            Self::Start(c) => c.digest(),
            Self::Stop(c) => c.digest(),
        }
    }
    fn context(&self) -> &ReplyContext {
        match self {
            Self::Start(c) => c.context(),
            Self::Stop(c) => c.context(),
        }
    }
    fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        match self {
            Self::Start(c) => c.into_parts(),
            Self::Stop(c) => c.into_parts(),
        }
    }
}
#[must_use]
pub(super) struct PreparedClaim {
    candidate: ProvisionCandidate,
    command: ClaimCommand,
}
#[must_use]
pub(super) struct PendingClaim {
    expected: Control,
    command: ClaimCommand,
}
#[must_use]
pub(super) struct ConfirmedClaim {
    command: ClaimCommand,
}
#[must_use]
pub(in crate::cluster) struct StartClaim {
    claim: PreparedClaim,
}
#[must_use]
pub(in crate::cluster) struct PendingStart {
    pending: PendingClaim,
}
#[must_use]
pub(in crate::cluster) struct ConfirmedStart {
    confirmed: ConfirmedClaim,
}
macro_rules! redacted {
    ($($t:ty),*) => { $(impl std::fmt::Debug for $t {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($t), " { .. }"))
        }
    })* };
}
redacted!(
    StartClaim,
    PendingStart,
    ConfirmedStart,
    ClaimCommand,
    PreparedClaim,
    PendingClaim,
    ConfirmedClaim
);
impl PreparedClaim {
    pub(super) fn into_parts(self) -> (ProvisionCandidate, PendingClaim) {
        let pending = PendingClaim {
            expected: self.candidate.result.clone(),
            command: self.command,
        };
        (self.candidate, pending)
    }
}
impl PendingClaim {
    // Borrowed check lets C06c3 validate this outcome before consuming its proof.
    pub(super) fn matches_outcome(&self, outcome: &OwnershipOutcome) -> bool {
        matches!(outcome, OwnershipOutcome::Acknowledged(v) | OwnershipOutcome::ObservedExact(v)
            if v.root() == &self.expected)
    }
    pub(super) fn confirm(self, outcome: &OwnershipOutcome) -> Option<ConfirmedClaim> {
        self.matches_outcome(outcome).then_some(ConfirmedClaim {
            command: self.command,
        })
    }
}
impl ConfirmedClaim {
    pub(super) fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        self.command.into_parts()
    }
}
impl StartClaim {
    #[allow(dead_code)] // Later supervised delivery caller.
    pub(in crate::cluster) fn into_parts(self) -> (ProvisionCandidate, PendingStart) {
        let (candidate, pending) = self.claim.into_parts();
        (candidate, PendingStart { pending })
    }
}
impl PendingStart {
    #[allow(dead_code)] // Later supervised delivery caller.
    pub(in crate::cluster) fn confirm(self, outcome: &OwnershipOutcome) -> Option<ConfirmedStart> {
        self.pending
            .confirm(outcome)
            .map(|confirmed| ConfirmedStart { confirmed })
    }
}
impl ConfirmedStart {
    #[allow(dead_code)] // One later HTTP attempt; fresh authority still required.
    pub(in crate::cluster) fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        self.confirmed.into_parts()
    }
}
pub(super) fn base(
    root: &Control,
    owner: Owner,
    proposal: Uuid,
) -> Result<OwnershipObservation, StorageError> {
    let observation = OwnershipObservation::new(root)?;
    if root.controller.lease_state != LeaseState::Held
        || root.controller.owner_id != Some(owner.process_id)
        || root.controller.epoch != owner.epoch
        || root.provisioning != Provisioning::Managed
        || retained_identity(root, proposal)
    {
        return Err(StorageError::Conflict);
    }
    Ok(observation)
}
pub(super) fn finish(
    root: &Control,
    owner: Owner,
    observation: OwnershipObservation,
    mut result: Control,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    result.revision = root
        .revision
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    result.change_id = proposal;
    result.encode().map_err(control_error)?;
    Ok(ProvisionCandidate {
        observation,
        owner,
        result,
    })
}
fn start_node(root: &Control, idx: usize) -> Result<usize, StorageError> {
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
    if i.action != Action::Start
        || i.outcome.is_some()
        || root.nodes[n].state == NodeState::Stopped
        || root.intents.iter().any(|other| {
            other.node_id == i.node_id
                && other.launch_id == i.launch_id
                && matches!(
                    other.outcome,
                    Some(IntentOutcome::Terminated | IntentOutcome::NotInvoked)
                )
                && (other.action == Action::Start
                    || other.outcome == Some(IntentOutcome::Terminated))
        })
    {
        return Err(StorageError::Conflict);
    }
    Ok(n)
}
#[allow(dead_code)] // Pure foundation; runtime and inventory retry caller are deferred.
pub(in crate::cluster) fn claim_start(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    invocation_id: Uuid,
    attached: &AttachedConfig,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StartClaim, StorageError> {
    let observation = base(root, owner, proposal)?;
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    let n = start_node(root, idx)?;
    let i = &root.intents[idx];
    let attempts = claim_attempts(root, idx)?;
    if i.state != IntentState::Pending
        || i.delivery.is_some()
        || i.attempts != 0
        || i.error_code.is_some()
        || i.resource_id.is_some()
        || root.nodes[n].resource_id.is_some()
        || root.nodes[n].registration != Registration::Unregistered
        || root.nodes[n].state != NodeState::Starting
        || now_ms < i.retry_at_ms
        || now_ms < root.scheduler.backoff_until_ms
        || retained_identity(root, invocation_id)
        || invocation_id == proposal
    {
        return Err(StorageError::Conflict);
    }
    construct_claim(
        root,
        owner,
        idx,
        invocation_id,
        attached,
        observation,
        attempts,
        proposal,
    )
}
// Separate closed eligibility branch. Only consumed complete-absence reconciliation calls it.
pub(super) fn claim_reconciled_start(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    invocation_id: Uuid,
    attached: &AttachedConfig,
    now_ms: u64,
    proposal: Uuid,
) -> Result<StartClaim, StorageError> {
    let observation = base(root, owner, proposal)?;
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    let n = start_node(root, idx)?;
    let i = &root.intents[idx];
    if i.state != IntentState::Reconciling
        || i.delivery.is_none()
        || i.attempts == 0
        || i.resource_id.is_some()
        || root.nodes[n].resource_id.is_some()
        || root.nodes[n].state != NodeState::Starting
        || root.nodes[n].registration != Registration::Unregistered
        || !root.nodes[n].retired_incarnations.is_empty()
        || now_ms < i.retry_at_ms
        || now_ms < root.scheduler.backoff_until_ms
        || retained_identity(root, invocation_id)
        || invocation_id == proposal
    {
        return Err(StorageError::Conflict);
    }
    let attempts = claim_attempts(root, idx)?;
    construct_claim(
        root,
        owner,
        idx,
        invocation_id,
        attached,
        observation,
        attempts,
        proposal,
    )
}
#[allow(clippy::too_many_arguments)]
fn construct_claim(
    root: &Control,
    owner: Owner,
    idx: usize,
    invocation_id: Uuid,
    attached: &AttachedConfig,
    observation: OwnershipObservation,
    attempts: u64,
    proposal: Uuid,
) -> Result<StartClaim, StorageError> {
    if attempts > root.timing.max_attempts {
        return Err(StorageError::LimitExceeded);
    }
    let mut result = root.clone();
    // Reconstruct against the prospective delivery identity without erasing any
    // retained state/history. The codec checks the complete reconciling root;
    // StartCommand computes the replacement digest from the exact new bytes.
    if let Some(d) = &mut result.intents[idx].delivery {
        d.invocation_id = invocation_id;
        d.owner_id = owner.process_id;
        d.epoch = owner.epoch;
    }
    let command = StartCommand::build(
        &result,
        owner,
        root.intents[idx].request_id,
        invocation_id,
        attached,
    )?;
    Ok(StartClaim {
        claim: prepare_claim(
            root,
            owner,
            idx,
            invocation_id,
            ClaimCommand::Start(command),
            observation,
            proposal,
        )?,
    })
}
fn claim_attempts(root: &Control, idx: usize) -> Result<u64, StorageError> {
    let attempts = root.intents[idx]
        .attempts
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    Ok(attempts)
}
// Only action-specific constructors call this after eligibility and reconstruction.
// It captures the original observation even when reconstruction used a prospective tuple.
#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_claim(
    root: &Control,
    owner: Owner,
    idx: usize,
    invocation_id: Uuid,
    command: ClaimCommand,
    observation: OwnershipObservation,
    proposal: Uuid,
) -> Result<PreparedClaim, StorageError> {
    let attempts = claim_attempts(root, idx)?;
    if attempts > root.timing.max_attempts {
        return Err(StorageError::LimitExceeded);
    }
    let mut result = root.clone();
    let i = &mut result.intents[idx];
    i.state = IntentState::InProgress;
    i.attempts = attempts;
    i.delivery = Some(Delivery {
        invocation_id,
        owner_id: owner.process_id,
        epoch: owner.epoch,
        command_sha256: command.digest().into(),
    });
    command
        .context()
        .matches_delivery(&result, owner, root.intents[idx].request_id)?;
    let candidate = finish(root, owner, observation, result, proposal)?;
    Ok(PreparedClaim { candidate, command })
}
pub(super) fn delay(initial: u64, cap: u64, k: u64) -> u64 {
    let mut value = initial;
    // Positive u64 initial reaches any u64 cap in at most 64 steps.
    for _ in 0..k.saturating_sub(1).min(64) {
        if value >= cap || value > cap / 2 {
            return cap;
        }
        value *= 2;
    }
    value.min(cap)
}
pub(super) fn scheduler_backoff(root: &Control, now_ms: u64) -> Result<(u64, u64), StorageError> {
    let failures = root
        .scheduler
        .failures
        .checked_add(1)
        .ok_or(StorageError::LimitExceeded)?;
    let backoff = now_ms
        .checked_add(delay(
            root.timing.retry_initial_ms,
            root.timing.retry_max_ms,
            failures,
        ))
        .ok_or(StorageError::LimitExceeded)?;
    Ok((failures, root.scheduler.backoff_until_ms.max(backoff)))
}

pub(super) fn failure_candidate(
    root: &Control,
    result: &mut Control,
    idx: usize,
    now_ms: u64,
    definite: bool,
) -> Result<(), StorageError> {
    let (failures, backoff) = scheduler_backoff(root, now_ms)?;
    let retry = now_ms
        .checked_add(delay(
            root.timing.retry_initial_ms,
            root.timing.retry_max_ms,
            result.intents[idx].attempts,
        ))
        .ok_or(StorageError::LimitExceeded)?;
    let i = &mut result.intents[idx];
    i.state = if definite {
        IntentState::Failed
    } else {
        IntentState::Reconciling
    };
    i.outcome = definite.then_some(IntentOutcome::NotInvoked);
    i.error_code = Some(
        if definite {
            "callback_not_invoked"
        } else {
            "callback_uncertain"
        }
        .into(),
    );
    i.retry_at_ms = i.retry_at_ms.max(retry);
    result.scheduler.failures = failures;
    result.scheduler.backoff_until_ms = result.scheduler.backoff_until_ms.max(backoff);
    Ok(())
}
fn current(
    root: &Control,
    owner: Owner,
    ctx: &ReplyContext,
) -> Result<(usize, usize), StorageError> {
    let id = ctx.intent_id().ok_or(StorageError::Conflict)?;
    ctx.matches_delivery(root, owner, id)?;
    let idx = root
        .intents
        .iter()
        .position(|i| i.request_id == id)
        .ok_or(StorageError::Conflict)?;
    let n = start_node(root, idx)?;
    Ok((idx, n))
}
#[allow(dead_code)] // Later one-attempt callback delivery owner.
pub(in crate::cluster) fn complete_start(
    root: &Control,
    owner: Owner,
    ctx: &ReplyContext,
    reply: CallbackReply,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    let observation = base(root, owner, proposal)?;
    let (idx, n) = current(root, owner, ctx)?;
    let mut result = root.clone();
    match reply {
        CallbackReply::Started(resource) => {
            if resource.is_empty() || resource.len() > 256 || resource.chars().any(char::is_control)
            {
                return Err(StorageError::InvalidCatalog);
            }
            if root.nodes[n]
                .resource_id
                .as_ref()
                .is_some_and(|r| r != &resource)
                || root.intents[idx]
                    .resource_id
                    .as_ref()
                    .is_some_and(|r| r != &resource)
                || root.nodes.iter().any(|other| {
                    other.node_id != root.nodes[n].node_id
                        && other.resource_id.as_ref() == Some(&resource)
                })
                || root.intents.iter().any(|other| {
                    other.node_id != root.nodes[n].node_id
                        && other.resource_id.as_ref() == Some(&resource)
                })
            {
                return Err(StorageError::Conflict);
            }
            result.nodes[n].resource_id = Some(resource.clone());
            let i = &mut result.intents[idx];
            i.resource_id = Some(resource);
            i.state = IntentState::Completed;
            i.outcome = Some(IntentOutcome::ResourceKnown);
            i.error_code = None;
        }
        CallbackReply::Failure { code, outcome } => {
            let definite_code = failure_is_definite(&code, &outcome)?;
            let definite = definite_code
                && outcome == DeliveryOutcome::NotInvoked
                && root.intents[idx].attempts == 1
                && root.nodes[n].registration == Registration::Unregistered
                && root.nodes[n].retired_incarnations.is_empty()
                && root.nodes[n].resource_id.is_none()
                && root.intents[idx].resource_id.is_none()
                && root.intents[idx].error_code.is_none();
            failure_candidate(root, &mut result, idx, now_ms, definite)?;
        }
        _ => return Err(StorageError::Conflict),
    }
    finish(root, owner, observation, result, proposal)
}
#[allow(dead_code)] // Transport/malformed/lost evidence becomes uncertainty here.
pub(in crate::cluster) fn uncertain_start(
    root: &Control,
    owner: Owner,
    ctx: &ReplyContext,
    now_ms: u64,
    proposal: Uuid,
) -> Result<ProvisionCandidate, StorageError> {
    let observation = base(root, owner, proposal)?;
    let (idx, _) = current(root, owner, ctx)?;
    let mut result = root.clone();
    failure_candidate(root, &mut result, idx, now_ms, false)?;
    finish(root, owner, observation, result, proposal)
}
pub(super) fn failure_is_definite(
    code: &str,
    outcome: &DeliveryOutcome,
) -> Result<bool, StorageError> {
    let definite_code = matches!(
        code,
        "invalid_command"
            | "unauthorized"
            | "ownership_changed"
            | "limit_exceeded"
            | "callback_capacity"
            | "bridge_not_active"
            | "ownership_unavailable"
            | "callback_timeout"
    );
    let uncertain_code = matches!(
        code,
        "callback_in_progress"
            | "callback_failed"
            | "invalid_callback_result"
            | "callback_timeout"
            | "bridge_not_active"
            | "invalid_command"
            | "unauthorized"
            | "limit_exceeded"
    );
    if !(definite_code && *outcome == DeliveryOutcome::NotInvoked
        || uncertain_code && *outcome == DeliveryOutcome::Unknown
        || code == "identity_conflict" && *outcome == DeliveryOutcome::NotInvoked)
    {
        return Err(StorageError::InvalidCatalog);
    }
    Ok(definite_code && *outcome == DeliveryOutcome::NotInvoked)
}
#[cfg(test)]
mod tests;
