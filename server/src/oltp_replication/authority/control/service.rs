use super::{
    ControlMachine,
    machine::{self, BUDGET, GROUPS, STATE, WORKERS},
    model::*,
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, GroupRead},
    oltp_replication::{
        authority::consensus::{Applied, Consensus},
        multiplex::{AuthenticatedSession, EndpointFuture, Purpose},
        owned::OwnedCommand,
        types::*,
    },
    oltp_runtime::{
        replica::{QueueAuthority, QueueScope},
        worker_endpoint::AuthorityReader,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

/// Authentication configuration, supplied by the operator rather than a worker
/// payload. Empty pins deny every administration operation.
pub(crate) struct OperatorPolicy {
    pub policy: Digest,
    pub certificate_pins: BTreeSet<Digest>,
}
/// Created by the hosted manager after local TLS material and physical worker
/// ownership are validated for an explicit local operator bootstrap command.
/// Remote worker input cannot deserialize or construct this capability.
pub(crate) struct LocalOperatorPermit {
    local: WorkerIdentity,
    shared: Arc<crate::oltp::pool::WorkerSharedResources>,
    _ownership: Arc<dyn Send + Sync>,
}
impl LocalOperatorPermit {
    pub(in crate::oltp_replication) fn from_local_bootstrap(
        local: WorkerIdentity,
        shared: Arc<crate::oltp::pool::WorkerSharedResources>,
        ownership: Arc<dyn Send + Sync>,
    ) -> Result<Self, ProtocolError> {
        local.validate().map_err(|_| unauthorized())?;
        if shared.worker_id() != Some(local.worker_id) {
            return Err(unauthorized());
        }
        Ok(Self {
            local,
            shared,
            _ownership: ownership,
        })
    }
}
pub(crate) struct VerifiedExclusion {
    stamp: ExclusionStamp,
}
impl VerifiedExclusion {
    pub(crate) fn stamp(&self) -> &ExclusionStamp {
        &self.stamp
    }
}
pub(crate) struct VerifiedMembership {
    stamp: GroupMembershipStamp,
}
impl VerifiedMembership {
    pub(crate) fn from_consensus(
        observation: crate::oltp_replication::manager::GroupMembershipObservation,
        generation: Uuid,
        authority: Authority,
    ) -> Result<Self, ProtocolError> {
        if generation.is_nil() || authority.validate().is_err() || observation.replicas().is_empty()
        {
            return Err(unauthorized());
        }
        let mut chain = machine::group_seed();
        let mut last = None;
        for replica in observation.replicas() {
            if last.is_some_and(|old| replica.node_id <= old) {
                return Err(unauthorized());
            }
            last = Some(replica.node_id);
            chain = machine::group_step(chain, replica);
        }
        Ok(Self {
            stamp: GroupMembershipStamp {
                group: observation.group().clone(),
                generation,
                replica_count: observation.replicas().len() as u64,
                replica_root: machine::group_root(observation.replicas().len() as u64, chain),
                applied_at: observation.applied_at(),
                authority,
            },
        })
    }
}
pub(crate) struct VerifiedQueueCopy {
    statement: QueueCopyStatement,
}
impl VerifiedQueueCopy {
    pub(crate) fn from_empty_worker(
        ticket: JoinTicket,
        replica: &crate::oltp_runtime::replica::WorkerReplica,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            statement: replica
                .empty_bootstrap_statement(ticket)
                .map_err(crate::request_queue::QueueError::protocol)?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InventoryPage<T> {
    pub revision: u64,
    pub items: Page<T>,
    pub after: Option<Uuid>,
    pub done: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicaInventoryPage {
    pub group: GroupRecord,
    pub items: Page<ReplicaMember>,
    pub after: Option<u64>,
    pub done: bool,
}

/// This handle owns the private proposal capability. Public worker methods
/// receive AuthorityReader only; they cannot submit raw Control commands.
pub(crate) struct ControlHandle {
    machine: ControlMachine,
    consensus: Arc<dyn Consensus<ControlCommand, ControlReply>>,
    policy: OperatorPolicy,
}
impl ControlHandle {
    pub(crate) fn new(
        machine: ControlMachine,
        consensus: Arc<dyn Consensus<ControlCommand, ControlReply>>,
        policy: OperatorPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            machine,
            consensus,
            policy,
        })
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.machine.store().resources()
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    async fn submit(
        &self,
        command: ControlCommand,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.consensus.propose(command).await
    }
    fn operator(
        &self,
        session: &AuthenticatedSession,
        evidence: Digest,
        operation_id: Uuid,
    ) -> Result<AuthorizationStamp, ProtocolError> {
        if session.group() != self.identity()
            || session.purpose() != Purpose::Authority
            || session.recovery_only()
            || !self
                .policy
                .certificate_pins
                .contains(&session.remote().certificate_sha256)
        {
            return Err(unauthorized());
        }
        Ok(AuthorizationStamp {
            authorization_id: operation_id,
            policy: self.policy.policy,
            subject: session.remote().certificate_sha256,
            evidence,
        })
    }
    fn local_operator(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        evidence: Digest,
    ) -> Result<AuthorizationStamp, ProtocolError> {
        if !Arc::ptr_eq(&permit.shared, self.resources())
            || !self
                .policy
                .certificate_pins
                .contains(&permit.local.certificate_sha256)
        {
            return Err(unauthorized());
        }
        Ok(AuthorizationStamp {
            authorization_id: operation_id,
            policy: self.policy.policy,
            subject: permit.local.certificate_sha256,
            evidence,
        })
    }
    pub(crate) async fn initialize_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        run_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.local_operator(permit, operation_id, machine::hash(run_id.as_bytes()))?;
        self.submit(ControlCommand::Initialize {
            operation_id,
            run_id,
            authorization,
        })
        .await
    }
    pub(crate) async fn enroll_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        previous_process: Option<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        if WorkerIdentity::from(&next) != permit.local {
            return Err(unauthorized());
        }
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&next)?,
        )?;
        self.submit(ControlCommand::Enroll {
            operation_id,
            expected,
            next,
            authorization,
            previous_process: previous_process.map(|proof| proof.stamp),
        })
        .await
    }
    pub(crate) fn exclude_local_previous(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        previous: WorkerIdentity,
    ) -> Result<VerifiedExclusion, ProtocolError> {
        if previous.worker_id != permit.local.worker_id || previous == permit.local {
            return Err(unauthorized());
        }
        let evidence =
            crate::oltp_replication::owned::serialized_digest(&(previous, permit.local))?;
        let authorization = self.local_operator(permit, operation_id, evidence)?;
        Ok(VerifiedExclusion {
            stamp: ExclusionStamp {
                worker: previous,
                authorization,
                ownership_fence: evidence,
            },
        })
    }
    pub(crate) async fn excluded_local_page(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
        proofs: Vec<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        let workers = Page::new(proofs.into_iter().map(|proof| proof.stamp).collect())
            .map_err(|_| unauthorized())?;
        self.submit(ControlCommand::ExcludedPage {
            operation_id,
            transition_id,
            workers,
        })
        .await
    }
    pub(crate) async fn begin_group_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&identity)?,
        )?;
        self.submit(ControlCommand::BeginGroup {
            operation_id,
            generation,
            identity,
            authorization,
        })
        .await
    }
    pub(crate) async fn group_page_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(permit, operation_id, machine::hash(generation.as_bytes()))?;
        self.submit(ControlCommand::GroupPage {
            operation_id,
            generation,
            replicas,
        })
        .await
    }
    pub(crate) async fn seal_group_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        proof: VerifiedMembership,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(proof.stamp.generation.as_bytes()),
        )?;
        self.submit(ControlCommand::SealGroup {
            operation_id,
            generation: proof.stamp.generation,
            proof: proof.stamp,
        })
        .await
    }
    pub(crate) async fn begin_join_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(target.worker_id.as_bytes()),
        )?;
        self.submit(ControlCommand::BeginJoin {
            operation_id,
            expected,
            target,
        })
        .await
    }
    pub(crate) async fn ready_page_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::ReadyPage {
            operation_id,
            transition_id,
            members,
        })
        .await
    }
    pub(crate) async fn seal_ready_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::SealReady {
            operation_id,
            transition_id,
        })
        .await
    }
    pub(crate) async fn begin_owner_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: ReadyMembership,
        next_owner: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&next_owner)?,
        )?;
        self.submit(ControlCommand::BeginOwnerFence {
            operation_id,
            expected,
            next_owner,
            authorization,
        })
        .await
    }
    pub(crate) async fn begin_cold_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        expected: ReadyMembership,
        next_run: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.local_operator(permit, operation_id, machine::hash(next_run.as_bytes()))?;
        self.submit(ControlCommand::BeginColdFence {
            operation_id,
            expected,
            next_run,
            authorization,
        })
        .await
    }
    pub(crate) async fn record_group_fence(
        &self,
        operation_id: Uuid,
        receipt: GroupFenceReceipt,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.submit(ControlCommand::GroupFenced {
            operation_id,
            receipt,
        })
        .await
    }
    pub(crate) async fn publish_fence_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(transition_id.as_bytes()),
        )?;
        self.submit(ControlCommand::PublishFence {
            operation_id,
            transition_id,
        })
        .await
    }
    pub(crate) async fn initialize(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        run_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization =
            self.operator(session, machine::hash(run_id.as_bytes()), operation_id)?;
        self.submit(ControlCommand::Initialize {
            operation_id,
            run_id,
            authorization,
        })
        .await
    }
    pub(crate) async fn enroll(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        previous_process: Option<VerifiedExclusion>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let evidence = crate::oltp_replication::owned::serialized_digest(&next)?;
        let authorization = self.operator(session, evidence, operation_id)?;
        self.submit(ControlCommand::Enroll {
            operation_id,
            expected,
            next,
            authorization,
            previous_process: previous_process.map(|proof| proof.stamp),
        })
        .await
    }
    pub(crate) async fn begin_group(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.operator(
            session,
            crate::oltp_replication::owned::serialized_digest(&identity)?,
            operation_id,
        )?;
        self.submit(ControlCommand::BeginGroup {
            operation_id,
            generation,
            identity,
            authorization,
        })
        .await
    }
    pub(crate) async fn group_page(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(session, machine::hash(generation.as_bytes()), operation_id)?;
        self.submit(ControlCommand::GroupPage {
            operation_id,
            generation,
            replicas,
        })
        .await
    }
    pub(crate) async fn seal_group(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        proof: VerifiedMembership,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(proof.stamp.generation.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::SealGroup {
            operation_id,
            generation: proof.stamp.generation,
            proof: proof.stamp,
        })
        .await
    }
    pub(crate) async fn begin_join(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(target.worker_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::BeginJoin {
            operation_id,
            expected,
            target,
        })
        .await
    }
    pub(crate) async fn ready_page(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(transition_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::ReadyPage {
            operation_id,
            transition_id,
            members,
        })
        .await
    }
    pub(crate) async fn seal_ready(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        transition_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.operator(
            session,
            machine::hash(transition_id.as_bytes()),
            operation_id,
        )?;
        self.submit(ControlCommand::SealReady {
            operation_id,
            transition_id,
        })
        .await
    }
    pub(crate) async fn timestamp_fenced(
        &self,
        operation_id: Uuid,
        receipt: super::super::timestamp::ReadyFenceReceipt,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        // Called only with the timestamp handle's durable receipt, never a
        // deserialized worker boolean. The apply transition checks every field.
        self.submit(ControlCommand::TimestampFenced {
            operation_id,
            transition_id: receipt.transition_id,
            receipt,
        })
        .await
    }
    pub(crate) async fn publish_ready(
        &self,
        operation_id: Uuid,
        proof: VerifiedQueueCopy,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.submit(ControlCommand::PublishReady {
            operation_id,
            transition_id: proof.statement.ticket.ticket_id,
            proof: proof.statement,
        })
        .await
    }

    async fn read(
        &self,
        read: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine.store().read(BUDGET, read).await
    }
    pub(crate) async fn excluded(
        &self,
        transition_id: Uuid,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        let raw = self
            .read(move |view| {
                let key = machine::key(
                    &machine::key(b"ct/exclusion/", transition_id),
                    worker.worker_id,
                );
                let record: Option<ExclusionStamp> = load(view, &key)?;
                view.encode(&record.is_some_and(|record| record.worker == worker))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn worker(
        &self,
        worker_id: Uuid,
    ) -> Result<Option<WorkerEnrollment>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record: Option<WorkerRecord> = load(view, &machine::key(WORKERS, worker_id))?;
                view.encode(&record.map(|record| record.enrollment))
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn group(&self, group_id: Uuid) -> Result<Option<GroupRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record: Option<GroupRecord> = load(view, &machine::key(GROUPS, group_id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn inspect(&self) -> Result<Option<State>, ProtocolError> {
        let bytes = self
            .read(|view| {
                let state: Option<State> = load(view, STATE)?;
                view.encode(&state)
            })
            .await?;
        machine::decode(bytes.as_ref())
    }
    pub(crate) async fn workers(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.enrollment_revision != revision {
                return Err(stale());
            }
            let mut items = Vec::new();
            let mut cursor = after;
            let mut page_bytes = 4096usize;
            let mut skipped = false;
            let progress = view.scan_page(
                WORKERS,
                after.map(|id| machine::key(WORKERS, id)).as_deref(),
                128,
                |key, bytes| {
                    if skipped
                        || page_bytes
                            .checked_add(bytes.len())
                            .is_none_or(|n| n > 60 << 10)
                    {
                        skipped = true;
                        return Ok(());
                    }
                    page_bytes += bytes.len();
                    let worker: WorkerRecord = machine::decode(bytes)?;
                    cursor = Some(Uuid::from_slice(&key[WORKERS.len()..]).map_err(|_| stale())?);
                    items.push(worker.enrollment);
                    Ok(())
                },
            )?;
            view.encode(&InventoryPage {
                revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted && !skipped,
            })
        })
        .await
    }
    pub(crate) async fn groups(
        &self,
        revision: u64,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.group_inventory_revision != revision {
                return Err(stale());
            }
            let mut items = Vec::new();
            let mut cursor = after;
            let mut page_bytes = 4096usize;
            let mut skipped = false;
            let key = after.map(|id| machine::key(GROUPS, id));
            let progress = view.scan_page(GROUPS, key.as_deref(), 128, |_, bytes| {
                if skipped
                    || page_bytes
                        .checked_add(bytes.len())
                        .is_none_or(|n| n > 60 << 10)
                {
                    skipped = true;
                    return Ok(());
                }
                page_bytes += bytes.len();
                let group: GroupRecord = machine::decode(bytes)?;
                cursor = Some(group.identity.group_id);
                items.push(group);
                Ok(())
            })?;
            view.encode(&InventoryPage {
                revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted && !skipped,
            })
        })
        .await
    }
    pub(crate) async fn group_members(
        &self,
        expected: GroupRecord,
        after: Option<u64>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            if load::<GroupRecord>(view, &machine::key(GROUPS, expected.identity.group_id))?
                != Some(expected.clone())
            {
                return Err(stale());
            }
            let prefix = machine::member_prefix(expected.generation);
            let key = after.map(|node| machine::member_key(expected.generation, node));
            let mut items = Vec::new();
            let mut cursor = after;
            let progress = view.scan_page(&prefix, key.as_deref(), 128, |_, bytes| {
                let member: ReplicaMember = machine::decode(bytes)?;
                cursor = Some(member.node_id);
                items.push(member);
                Ok(())
            })?;
            view.encode(&ReplicaInventoryPage {
                group: expected,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted,
            })
        })
        .await
    }
    pub(crate) async fn ready_page_read(
        &self,
        expected: ReadyMembership,
        after: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let state = state(view)?;
            if state.ready != expected {
                return Err(stale());
            }
            let prefix = machine::ready_prefix(state.ready_generation);
            let cursor_key = after.map(|id| machine::ready_key(state.ready_generation, id));
            let mut items = Vec::new();
            let mut cursor = after;
            let progress = view.scan_page(&prefix, cursor_key.as_deref(), 128, |_, bytes| {
                let member: WorkerIdentity = machine::decode(bytes)?;
                cursor = Some(member.worker_id);
                items.push(member);
                Ok(())
            })?;
            view.encode(&InventoryPage {
                revision: expected.view.membership_revision,
                items: Page::new(items).map_err(|_| stale())?,
                after: cursor,
                done: progress.exhausted,
            })
        })
        .await
    }
    async fn authority(
        &self,
        scope: &QueueScope,
        reader: Option<(WorkerIdentity, Uuid)>,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.identity().cluster_id
            || scope.database != self.identity().database
        {
            return Err(unauthorized());
        }
        let bytes = self
            .read(move |view| {
                let state = state(view)?;
                let owner = state.owner.ok_or_else(stale)?;
                if state.ready.ready_count == 0
                    || state.invalid_ready != 0
                    || matches!(state.pending, Some(Pending::Fence(_)))
                {
                    return Err(stale());
                }
                if let Some((remote, group_id)) = reader {
                    let worker: WorkerRecord =
                        load(view, &machine::key(WORKERS, remote.worker_id))?
                            .ok_or_else(unauthorized)?;
                    let group: GroupRecord =
                        load(view, &machine::key(GROUPS, group_id))?.ok_or_else(unauthorized)?;
                    if WorkerIdentity::from(&worker.enrollment) != remote
                        || group.identity.kind != GroupKind::Timestamps
                        || group.replica_count == 0
                        || load::<ReplicaMember>(
                            view,
                            &machine::member_worker_key(group.generation, remote.worker_id),
                        )?
                        .is_none()
                    {
                        return Err(unauthorized());
                    }
                }
                view.encode(&(state.ready, owner))
            })
            .await?;
        let (ready, owner) = machine::decode(bytes.as_ref())?;
        Ok(QueueAuthority { ready, owner })
    }
}
impl AuthorityReader for ControlHandle {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, None))
    }
    fn terminal_reader_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, Some((remote, timestamps_group_id))))
    }
}
fn load<T: serde::de::DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    view.get(key)?
        .map(|v| machine::decode(v.as_ref()))
        .transpose()
}
fn state(view: &mut GroupRead<'_>) -> Result<State, ProtocolError> {
    load(view, STATE)?.ok_or_else(stale)
}
fn stale() -> ProtocolError {
    unavailable("Control authority or inventory is not ready")
}
fn unauthorized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "Control role is not authorized",
        Outcome::NotApplied,
    )
}
