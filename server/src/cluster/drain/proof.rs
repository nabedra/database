//! Closed drain composition. All encoding/hashing is outside short guard locks.
use super::*;
use crate::cluster::{
    controller::{OwnershipGuard, OwnershipSample},
    heartbeat::{DrainHeartbeat, Observations},
    member::{Action, IntentState, Ownership, Registration},
    store::OwnershipOutcome,
};
use crate::node::auth::{self, PendingReplyChecks, ReplyHandoff, RequestContext, Role, Route};
use std::time::Duration;
use tokio::time::Instant;

// Each entry point is pending C06c/runtime integration, not a public facade.
#[allow(dead_code)]
pub(crate) struct DrainRound {
    root: Control,
    owner: Owner,
    generation: u64,
    node: Node,
    context: Option<RequestContext>,
    started: OwnershipSample,
}
#[allow(dead_code)]
pub(crate) struct PositiveDrain {
    round: DrainRound,
    heartbeat: DrainHeartbeat,
    ack: u64,
    reply: auth::ReplyContextBinding,
}
#[allow(dead_code)]
#[allow(clippy::large_enum_variant)] // Direct closed owned proof.
pub(crate) enum DrainCheck {
    Pending,
    Positive(PositiveDrain),
}
#[allow(dead_code)]
impl DrainRound {
    /// Builds and owns the outgoing context against this complete root. Retain
    /// one round per probe; consume it on any failed/missing reply or read.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        root: &Control,
        owner: Owner,
        binding: DrainBinding,
        auth: auth::Auth,
        initial: Vec<u8>,
        guard: &mut OwnershipGuard,
        sample: OwnershipSample,
    ) -> Result<Self, StorageError> {
        let context =
            RequestContext::for_send(auth, initial, root).map_err(|_| StorageError::Conflict)?;
        let (observation, index) = base(root, owner, binding.node.node_id)?;
        let node = &root.nodes[index];
        let a = context.auth();
        let receiver = a.receiver();
        let drain = node.drain.as_ref().ok_or(StorageError::Conflict)?;
        let session = node.session.as_ref().ok_or(StorageError::Conflict)?;
        require(
            node == &binding.node
                && node.state == NodeState::Draining
                && node.registration != Registration::Unregistered
                && a.route() == Route::Drain
                && a.sender().role() == Role::Api
                && a.cluster_id() == root.cluster_id
                && a.sequence() > 0
                && receiver.node_id() == node.node_id
                && Some(receiver.incarnation()) == node.incarnation
                && receiver.session_id() == session.id
                && auth::Drain::decode(context.initial_bytes())
                    .map_err(|_| StorageError::Conflict)?
                    .drain_id()
                    == drain.id,
        )?;
        let generation = guard.authorization_generation();
        require(guard.authorize(generation, &observation, Ok(sample))? == owner)?;
        Ok(Self {
            root: root.clone(),
            owner,
            generation,
            node: node.clone(),
            context: Some(context),
            started: sample,
        })
    }
}
/// Owns the original round and actual consuming auth handoff together.
#[allow(dead_code)]
pub(crate) struct DrainRoundReply {
    round: DrainRound,
    reply: ReplyHandoff,
}
#[allow(dead_code)] // Sender/auth composition; no reconstructed context accepted.
impl DrainRound {
    pub(crate) fn context(&self) -> Result<&RequestContext, StorageError> {
        self.context.as_ref().ok_or(StorageError::Conflict)
    }
    pub(crate) fn bind_reply(
        mut self,
        eligible: auth::EligibleReply,
        content: auth::CheckedReplyContent,
    ) -> Result<DrainRoundReply, StorageError> {
        let context = self.context.take().ok_or(StorageError::Conflict)?;
        let observation = OwnershipObservation::new(&self.root)?;
        let reply = context
            .into_reply_handoff(eligible, content, &observation)
            .map_err(|_| StorageError::Conflict)?;
        Ok(DrainRoundReply { round: self, reply })
    }
}

#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_drain_reply(
    root: &Control,
    owner: Owner,
    bound: DrainRoundReply,
    live_api: &auth::Sender,
    heartbeat: DrainHeartbeat,
    observations: &mut Observations,
    guard: &mut OwnershipGuard,
    sample: OwnershipSample,
) -> Result<DrainCheck, StorageError> {
    let DrainRoundReply { round, reply } = bound;
    let checked = (|| {
        round.check(root, owner, guard, sample)?;
        let drain = round.node.drain.as_ref().ok_or(StorageError::Conflict)?;
        let matching = reply.context().auth().sender() == live_api
            && reply.observation() == &OwnershipObservation::new(root)?
            && reply.status() == 200
            && reply.error().is_none()
            && !reply.is_unattributed_rejection()
            && matches!(reply.pending(), PendingReplyChecks::Drain {drain_id} if *drain_id == drain.id)
            && heartbeat.observation() == reply.observation()
            && heartbeat.controller_boot() == owner.process_id
            && heartbeat.member() == &round.node
            && heartbeat.binding().cluster_id == root.cluster_id
            && heartbeat.sampled_at() >= round.started.monotonic();
        if !matching || !observations.matches_drain(&heartbeat, sample.monotonic())? {
            return Ok(None);
        }
        let Some(r) = reply.drain() else {
            return Ok(None);
        };
        let h = heartbeat.heartbeat();
        if !r.barrier()
            || !r.idle()
            || r.drain_id() != drain.id
            || r.heartbeat_sequence() == 0
            || h.sequence < r.heartbeat_sequence()
            || h.drain_id != Some(drain.id)
            || h.accepting
            || h.running_operations != 0
            || h.running_partitions != 0
            || h.queued_operations != 0
            || h.queued_partitions != 0
        {
            return Ok(None);
        }
        Ok(Some(r.heartbeat_sequence()))
    })();
    let ack = match checked {
        Ok(Some(ack)) => ack,
        refused => {
            // A superseded round can still contradict a continued local proof.
            // Invalidate before returning any refusal, including early errors.
            observations.read_failed(heartbeat.binding(), sample.monotonic())?;
            return refused.map(|_| DrainCheck::Pending);
        }
    };
    // Handoff owns original auth/content associations through this consumption.
    let (binding, _bytes, _body, _observation, _pending) = reply.into_parts();
    Ok(DrainCheck::Positive(PositiveDrain {
        round,
        heartbeat,
        ack,
        reply: binding,
    }))
}

fn exact_outcome(expected: &Control, outcome: &OwnershipOutcome) -> bool {
    match outcome {
        OwnershipOutcome::Acknowledged(c) | OwnershipOutcome::ObservedExact(c) => {
            c.root() == expected
        }
        _ => false,
    }
}

#[allow(dead_code)] // C06c/runtime consumes the closed round at use boundaries.
impl DrainRound {
    fn check(
        &self,
        root: &Control,
        owner: Owner,
        guard: &mut OwnershipGuard,
        sample: OwnershipSample,
    ) -> Result<(), StorageError> {
        require(root == &self.root && owner == self.owner)?;
        require(
            sample.monotonic() >= self.started.monotonic()
                && sample.unix_ms() >= self.started.unix_ms(),
        )?;
        let observation = OwnershipObservation::new(root)?;
        require(guard.authorize(self.generation, &observation, Ok(sample))? == owner)
    }
}
#[allow(dead_code)] // C06c/runtime evidence; never an authorization ticket.
pub(crate) struct ConfirmedDrain {
    proof: PositiveDrain,
    root: Control,
    observation: OwnershipObservation,
    stop: Option<Uuid>,
}
#[allow(dead_code)]
#[allow(clippy::large_enum_variant)] // One bounded owned plan, no heap facade.
pub(crate) enum IdlePlan {
    Existing(ConfirmedDrain),
    Change(DrainCandidate, PendingDrainConfirmation),
}
#[allow(dead_code)]
pub(crate) struct PendingDrainConfirmation {
    proof: PositiveDrain,
    expected: Control,
}
#[allow(dead_code)]
pub(crate) struct PendingStopAllocation {
    proof: ConfirmedDrain,
    expected: Control,
    stop: Uuid,
}
#[allow(dead_code)]
pub(crate) struct PendingStopClaim {
    proof: ConfirmedDrain,
    expected: Control,
}

#[allow(dead_code)] // Runtime consumes fresh observations before proposing metadata.
pub(crate) fn confirm_idle(
    root: &Control,
    owner: Owner,
    proof: PositiveDrain,
    proposal: Uuid,
    observations: &mut Observations,
    guard: &mut OwnershipGuard,
    sample: OwnershipSample,
) -> Result<IdlePlan, StorageError> {
    proof.round.check(root, owner, guard, sample)?;
    require(observations.matches_drain(&proof.heartbeat, sample.monotonic())?)?;
    let (observation, index) = base(root, owner, proof.round.node.node_id)?;
    require(root.nodes[index] == proof.round.node)?;
    let drain = root.nodes[index]
        .drain
        .as_ref()
        .ok_or(StorageError::Conflict)?;
    // Exact convergence precedes revision and proposal checks, including MAX.
    if drain.confirmed_idle && drain.ack_seq == Some(proof.ack) {
        return Ok(IdlePlan::Existing(ConfirmedDrain::new(
            proof,
            root.clone(),
            None,
        )?));
    }
    let mut result = root.clone();
    let d = result.nodes[index]
        .drain
        .as_mut()
        .ok_or(StorageError::Conflict)?;
    d.ack_seq = Some(proof.ack);
    d.confirmed_idle = true;
    let DrainPlan::Change(candidate) = change(root, owner, observation, proposal, result)? else {
        return Err(StorageError::Conflict);
    };
    let expected = candidate.result.clone();
    Ok(IdlePlan::Change(
        candidate,
        PendingDrainConfirmation { proof, expected },
    ))
}
#[allow(dead_code)] // Consumed metadata confirmation; no guard finish/authority installation.
impl PendingDrainConfirmation {
    pub(crate) fn confirm(self, outcome: OwnershipOutcome) -> Result<ConfirmedDrain, StorageError> {
        require(exact_outcome(&self.expected, &outcome))?;
        ConfirmedDrain::new(self.proof, self.expected, None)
    }
}
#[allow(dead_code)] // Later stop callers must run check_live and final guard authorization.
impl ConfirmedDrain {
    pub(super) fn node_id(&self) -> Uuid {
        self.proof.round.node.node_id
    }
    pub(super) fn captured_generation(&self) -> u64 {
        self.proof.round.generation
    }
    fn new(proof: PositiveDrain, root: Control, stop: Option<Uuid>) -> Result<Self, StorageError> {
        let deadline = proof
            .heartbeat
            .sampled_at()
            .checked_add(Duration::from_millis(root.timing.expiry_ms))
            .ok_or(StorageError::LimitExceeded)?;
        require(Instant::now() < deadline)?;
        let observation = OwnershipObservation::new(&root)?;
        Ok(Self {
            proof,
            root,
            observation,
            stop,
        })
    }
    pub(crate) fn check_for_stop(
        &self,
        root: &Control,
        owner: Owner,
        generation: u64,
    ) -> Result<(), StorageError> {
        require(
            root == &self.root
                && owner == self.proof.round.owner
                && generation == self.proof.round.generation,
        )?;
        let (_, i) = base(root, owner, self.proof.round.node.node_id)?;
        let mut expected = self.proof.round.node.clone();
        let d = expected.drain.as_mut().ok_or(StorageError::Conflict)?;
        d.ack_seq = Some(self.proof.ack);
        d.confirmed_idle = true;
        require(root.nodes[i] == expected)?;
        let end = self
            .proof
            .heartbeat
            .sampled_at()
            .checked_add(Duration::from_millis(root.timing.expiry_ms))
            .ok_or(StorageError::LimitExceeded)?;
        require(Instant::now() < end)
    }
    /// Call at every local use boundary. Failure consumes the proof so a later
    /// zero cannot restore it. Observations retains its own failure high-water.
    pub(crate) fn check_live(
        self,
        root: &Control,
        owner: Owner,
        observations: &mut Observations,
        guard: &mut OwnershipGuard,
        sample: OwnershipSample,
    ) -> Result<Self, StorageError> {
        self.check_for_stop(root, owner, guard.authorization_generation())?;
        require(observations.matches_drain(&self.proof.heartbeat, sample.monotonic())?)?;
        require(
            guard.authorize(self.proof.round.generation, &self.observation, Ok(sample))? == owner,
        )?;
        Ok(self)
    }
    /// Only the first exact stop insertion may continue this proof.
    pub(super) fn prepare_stop_successor(
        self,
        candidate: DrainCandidate,
    ) -> Result<(DrainCandidate, PendingStopAllocation), StorageError> {
        self.check_for_stop(&self.root, candidate.owner, self.proof.round.generation)?;
        require(candidate.base == self.observation && self.stop.is_none())?;
        let base = &self.root;
        let next = &candidate.result;
        successor_revision(base, next)?;
        let n = &self.proof.round.node;
        require(
            n.ownership == Ownership::Managed
                && n.launch_id.is_some()
                && n.resource_id.is_some()
                && n.incarnation.is_some(),
        )?;
        require(
            !base
                .intents
                .iter()
                .any(|i| i.action == Action::Stop && i.node_id == n.node_id),
        )?;
        let added: Vec<_> = next
            .intents
            .iter()
            .filter(|i| !base.intents.iter().any(|b| b.request_id == i.request_id))
            .collect();
        require(added.len() == 1)?;
        let i = added[0];
        require(
            i.action == Action::Stop
                && i.node_id == n.node_id
                && Some(i.launch_id) == n.launch_id
                && i.incarnation_id == n.incarnation
                && i.resource_id == n.resource_id
                && i.state == IntentState::Pending
                && i.attempts == 0
                && i.delivery.is_none()
                && i.error_code.is_none()
                && i.outcome.is_none()
                && i.template_fingerprint.is_none()
                && i.retry_at_ms == i.created_at_ms
                && i.deadline_ms
                    == i.created_at_ms
                        .checked_add(base.timing.drain_ms)
                        .ok_or(StorageError::LimitExceeded)?
                && !super::retained_identity(base, i.request_id)
                && i.request_id != next.change_id,
        )?;
        let mut restored = next.clone();
        restored.intents.retain(|v| v.request_id != i.request_id);
        restore_revision(base, &mut restored);
        require(&restored == base)?;
        let before = base.capacity().map_err(control_error)?;
        let after = next.capacity().map_err(control_error)?;
        require(
            before.nodes == after.nodes
                && after.intents == before.intents + 1
                && before.credits == after.credits + 1
                && before.admission_bytes == after.admission_bytes,
        )?;
        let pending = PendingStopAllocation {
            expected: next.clone(),
            stop: i.request_id,
            proof: self,
        };
        Ok((candidate, pending))
    }
    /// C06c validates the exact typed StopCommand before calling. Digest must be
    /// borrowed from that command, never copied from the successor delivery.
    pub(crate) fn prepare_stop_claim(
        mut self,
        base: &Control,
        successor: &Control,
        command_digest: &str,
    ) -> Result<PendingStopClaim, StorageError> {
        self.check_for_stop(base, self.proof.round.owner, self.proof.round.generation)?;
        successor_revision(base, successor)?;
        // A fresh authenticated round may start with a retained stop already
        // present. Select it only from this proof's exact complete base.
        let node = &self.proof.round.node;
        let mut stops = base.intents.iter().filter(|i| {
            i.action == Action::Stop
                && i.node_id == node.node_id
                && Some(i.launch_id) == node.launch_id
                && i.incarnation_id == node.incarnation
                && i.resource_id == node.resource_id
        });
        let stop = stops.next().ok_or(StorageError::Conflict)?.request_id;
        require(stops.next().is_none() && self.stop.is_none_or(|id| id == stop))?;
        self.stop = Some(stop);
        let index = base
            .intents
            .binary_search_by_key(&stop, |i| i.request_id)
            .map_err(|_| StorageError::Conflict)?;
        let old = &base.intents[index];
        let next = successor.intents.get(index).ok_or(StorageError::Conflict)?;
        require(
            old.action == Action::Stop
                && matches!(old.state, IntentState::Pending | IntentState::Reconciling)
                && next.state == IntentState::InProgress
                && next.attempts
                    == old
                        .attempts
                        .checked_add(1)
                        .ok_or(StorageError::LimitExceeded)?
                && next.attempts <= base.timing.max_attempts,
        )?;
        let d = next.delivery.as_ref().ok_or(StorageError::Conflict)?;
        require(
            d.owner_id == self.proof.round.owner.process_id
                && d.epoch == self.proof.round.owner.epoch
                && !super::retained_identity(base, d.invocation_id)
                && d.invocation_id != successor.change_id
                && d.command_sha256 == command_digest,
        )?;
        let mut restored = successor.clone();
        restored.intents[index].state = old.state;
        restored.intents[index].attempts = old.attempts;
        restored.intents[index].delivery = old.delivery.clone();
        restore_revision(base, &mut restored);
        require(&restored == base)?;
        Ok(PendingStopClaim {
            proof: self,
            expected: successor.clone(),
        })
    }
}
fn successor_revision(base: &Control, next: &Control) -> Result<(), StorageError> {
    next.encode().map_err(control_error)?;
    require(
        next.revision
            == base
                .revision
                .checked_add(1)
                .ok_or(StorageError::LimitExceeded)?
            && next.change_id != base.change_id,
    )
}
fn restore_revision(base: &Control, restored: &mut Control) {
    restored.revision = base.revision;
    restored.change_id = base.change_id;
}
#[allow(dead_code)] // Later allocation consumes one whole-root metadata result.
impl PendingStopAllocation {
    pub(crate) fn confirm(self, outcome: OwnershipOutcome) -> Result<ConfirmedDrain, StorageError> {
        require(exact_outcome(&self.expected, &outcome))?;
        ConfirmedDrain::new(self.proof.proof, self.expected, Some(self.stop))
    }
}
#[allow(dead_code)] // Later provision/stop releases command/context and proof together.
impl PendingStopClaim {
    pub(crate) fn matches_outcome(&self, outcome: &OwnershipOutcome) -> bool {
        exact_outcome(&self.expected, outcome)
    }
    pub(crate) fn confirm(self, outcome: OwnershipOutcome) -> Result<ConfirmedDrain, StorageError> {
        require(self.matches_outcome(&outcome))?;
        ConfirmedDrain::new(self.proof.proof, self.expected, self.proof.stop)
    }
}
macro_rules! redacted {
    ($($ty:ty),*) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(stringify!($ty)) }
    })*};
}
redacted!(
    DrainRoundReply,
    DrainRound,
    PositiveDrain,
    DrainCheck,
    ConfirmedDrain,
    IdlePlan,
    PendingDrainConfirmation,
    PendingStopAllocation,
    PendingStopClaim
);
#[cfg(test)]
mod tests;
