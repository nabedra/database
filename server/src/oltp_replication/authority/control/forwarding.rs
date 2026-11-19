//! Private operator delegation between explicitly configured Control voters.
//! Ordinary enrollment never grants this proposal capability. Quorum barriers
//! are executed by the actual leader and the caller waits for local application
//! before any ControlHandle reads its local machine.
use super::{ControlCommand, ControlEndpoint, ControlReply, DataWorkerIntent, RestartMountKey};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    oltp_replication::{
        authority::consensus::{Applied, Consensus, ConsensusFuture, ReadBarrier},
        manager::{HostedGroupManager, ManagedControl},
        multiplex::{
            AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture, Purpose,
            ReceivePlan, TrafficClass, WorkerClient,
        },
        owned::OwnedCommand,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::time::Instant;

mod read_batch;

const HEADER: usize = 4096;
const PAYLOAD: usize = 64 << 10;
const DECODE: usize = 2 << 20;
const MAX_TOKEN: usize = 8192;
const READ_TIMEOUT: Duration = Duration::from_secs(3);
fn zero(value: &u64) -> bool { *value == 0 }
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Propose,
    Barrier,
    CompleteRestart,
    CompleteDataWorker,
    BeginDataWorkerJoin,
    CloseDataWorker,
    ReadEksWorkerReplacement,
    CompleteEksWorkerReplacement,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    control_consensus: Operation,
    to_node: u64,
    payload_bytes: u64,
    #[serde(default, skip_serializing_if = "zero")]
    credential_bytes: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    nonce: uuid::Uuid,
    node: u64,
    bytes: u64,
    error: Option<ProtocolError>,
}

pub(crate) struct DataWorkerClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
    routes: Vec<(u64, WorkerIdentity)>,
}

impl DataWorkerClient {
    pub(crate) fn new(
        client: WorkerClient,
        shared: Arc<WorkerSharedResources>,
        group: GroupIdentity,
        routes: Vec<(u64, WorkerIdentity)>,
    ) -> Self {
        Self { client, shared, group, routes }
    }

    pub(crate) async fn complete(
        &self,
        operation_id: uuid::Uuid,
        intent: &DataWorkerIntent,
        mount: &crate::oltp::pool::mount::MountedPoolObservation,
        key: &RestartMountKey,
    ) -> Result<ControlReply, ProtocolError> {
        let (mount, proof) = intent.prove(&self.group, mount, key)?;
        self.submit(Operation::CompleteDataWorker, ControlCommand::CompleteDataWorker {
            operation_id,
            intent: Box::new(intent.clone()),
            mount,
            proof,
        }).await
    }

    pub(crate) async fn begin(
        &self,
        operation_id: uuid::Uuid,
        intent: &DataWorkerIntent,
        expected: ReadyMembership,
        projected_token: Option<&str>,
    ) -> Result<ControlReply, ProtocolError> {
        let credential = projected_token.map(|token| self.shared.encode_in(ByteClass::Progress, &token, MAX_TOKEN + 2)).transpose()?;
        self.submit_with_credential(Operation::BeginDataWorkerJoin, ControlCommand::BeginDataWorkerJoin {
            operation_id,
            worker: intent.worker(),
            claim_id: intent.claim_id,
            expected,
        }, credential).await
    }

    pub(crate) async fn close_worker(
        &self,
        operation_id: uuid::Uuid,
        proof: super::WorkerCloseAttestation,
    ) -> Result<ControlReply, ProtocolError> {
        self.submit(Operation::CloseDataWorker, ControlCommand::CloseDataWorker {
            operation_id,
            proof: Box::new(proof),
        }).await
    }

    pub(crate) async fn replacement_permit(&self, projected_token: &str) -> Result<super::EksReplacementPermit, ProtocolError> {
        let credential = self.shared.encode_in(ByteClass::Progress, &projected_token, MAX_TOKEN + 2)?;
        let reply = self.call(Operation::ReadEksWorkerReplacement, None, Some(credential)).await?;
        Ok((*reply.decode_section::<super::EksReplacementPermit>(0)?).clone())
    }

    pub(crate) async fn complete_replacement(
        &self,
        operation_id: uuid::Uuid,
        permit: &super::EksReplacementPermit,
        primary: &crate::oltp::pool::mount::MountedPoolObservation,
        added: &crate::oltp::pool::mount::MountedPoolObservation,
        key: &RestartMountKey,
        projected_token: &str,
    ) -> Result<ControlReply, ProtocolError> {
        let mounted = super::worker_drain::EksMountedReplacement::from_native(&self.group, permit, primary, added, key)?;
        let command = ControlCommand::CompleteEksWorkerReplacement {
            operation_id,
            worker: permit.next.worker(),
            mounted: Box::new(mounted),
        };
        let credential = self.shared.encode_in(ByteClass::Progress, &projected_token, MAX_TOKEN + 2)?;
        self.submit_with_credential(Operation::CompleteEksWorkerReplacement, command, Some(credential)).await
    }

    async fn submit(
        &self,
        operation: Operation,
        command: ControlCommand,
    ) -> Result<ControlReply, ProtocolError> {
        self.submit_with_credential(operation, command, None).await
    }

    async fn submit_with_credential(&self, operation: Operation, command: ControlCommand,
        credential: Option<crate::oltp::pool::ChargedBytes>,
    ) -> Result<ControlReply, ProtocolError> {
        let command = OwnedCommand::new_owned(command, &self.shared, ByteClass::Progress)?;
        let payload = self.shared.encode_in(ByteClass::Progress, &command, PAYLOAD)?;
        let reply = self.call(operation, Some(payload), credential).await?;
        let applied = reply.decode_section::<Applied<ControlReply>>(0)?;
        Ok(applied.reply.clone())
    }

    async fn call(&self, operation: Operation, payload: Option<crate::oltp::pool::ChargedBytes>,
        credential: Option<crate::oltp::pool::ChargedBytes>)
        -> Result<crate::oltp_replication::multiplex::ReceivedReply, ProtocolError>
    {
        let mut failure = unavailable();
        for (node, remote) in &self.routes {
            let header = self.shared.encode_in(ByteClass::Progress, &Request {
                control_consensus: operation,
                to_node: *node,
                payload_bytes: payload.as_ref().map_or(0, |bytes| bytes.len() as u64),
                credential_bytes: credential.as_ref().map_or(0, |bytes| bytes.len() as u64),
            }, HEADER)?;
            let reply = match self.client.request(
                &self.group, remote.worker_id, Purpose::ControlDelegation, header,
                [payload.clone(), credential.clone(), None], PAYLOAD,
            ).await {
                Ok(reply) => reply,
                Err(error) => {
                    failure = error;
                    continue;
                }
            };
            if reply.remote_identity() != *remote
                || reply.group() != &self.group
                || reply.purpose() != Purpose::ControlDelegation
                || reply.sections[1].is_some()
                || reply.sections[2].is_some()
            {
                return Err(invalid());
            }
            let frame = reply.decode_header::<Reply>()?;
            if frame.node != *node || frame.nonce != reply.nonce()
                || frame.bytes != reply.sections[0].as_ref().map_or(0, |body| body.len() as u64)
            {
                return Err(invalid());
            }
            if let Some(error) = &frame.error {
                if !transient(error) || frame.bytes != 0 {
                    return Err(error.clone());
                }
                failure = error.clone();
                continue;
            }
            return Ok(reply);
        }
        Err(failure)
    }
}
#[derive(Clone)]
pub(crate) struct ForwardedControl {
    manager: Arc<HostedGroupManager>,
    local: ManagedControl,
    pins: Arc<BTreeSet<Digest>>,
    read_batch: read_batch::ReadBatch,
    _owner: Arc<ScratchLease>,
}
impl ForwardedControl {
    pub(crate) fn new(
        manager: Arc<HostedGroupManager>,
        local: ManagedControl,
        pins: BTreeSet<Digest>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if pins.is_empty()
            || local.state.store().identity().kind != GroupKind::Control
            || !Arc::ptr_eq(manager.resources(), local.state.store().resources())
        {
            return Err(invalid());
        }
        let resident_bytes = std::mem::size_of::<Self>() + pins.len() * 64;
        let resident_bytes = resident_bytes
            + std::mem::size_of::<BTreeSet<Digest>>()
            + 2 * std::mem::size_of::<usize>()
            + read_batch::RESIDENT_BYTES;
        let owner =
            Arc::new(manager.resources().reserve_scratch(ByteClass::Progress, resident_bytes)?);
        let pins = Arc::new(pins);
        Ok(Arc::new(Self {
            manager,
            local,
            pins,
            read_batch: read_batch::ReadBatch::default(),
            _owner: owner,
        }))
    }
    async fn forwarded<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        operation: Operation,
        payload: Option<ChargedBytes>,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let shared = self.manager.resources();
        let group = self.local.state.store().identity();
        let mut last = unavailable();
        for member in self.manager.leader_candidates(group.group_id)? {
            if matches!(operation, Operation::CompleteRestart)
                && member.node_id == self.local.state.resident().node_id()
            {
                continue;
            }
            let header = shared.encode_in(
                ByteClass::Progress,
                &Request {
                    control_consensus: operation,
                    to_node: member.node_id,
                    payload_bytes: payload.as_ref().map_or(0, |bytes| bytes.len() as u64),
                    credential_bytes: 0,
                },
                HEADER,
            )?;
            let received = match self
                .manager
                .client()
                .request(
                    group,
                    member.worker_id,
                    Purpose::ControlDelegation,
                    header,
                    [payload.clone(), None, None],
                    64 << 10,
                )
                .await
            {
                Ok(reply) => reply,
                Err(error) => {
                    tracing::debug!(group=%group.group_id,node=member.node_id,code=?error.code,outcome=?error.outcome,message=error.message,"Control forwarding transport failed");
                    if !transient(&error) {
                        return Err(error);
                    }
                    last = error;
                    continue;
                }
            };
            if received.group() != group
                || received.purpose() != Purpose::ControlDelegation
                || received.remote_identity().worker_id != member.worker_id
                || !self
                    .pins
                    .contains(&received.remote_identity().certificate_sha256)
                || received.sections[1].is_some()
                || received.sections[2].is_some()
            {
                return Err(invalid());
            }
            let reply = received.decode_header::<Reply>()?;
            if reply.node != member.node_id || reply.nonce != received.nonce() {
                return Err(invalid());
            }
            if let Some(error) = &reply.error {
                tracing::debug!(group=%group.group_id,node=member.node_id,code=?error.code,outcome=?error.outcome,message=error.message,"Control forwarding peer unavailable");
                if !transient(error) {
                    return Err(error.clone());
                }
                last = error.clone();
                continue;
            }
            let input = received.sections[0].as_ref().ok_or_else(invalid)?.clone();
            if input.len() as u64 != reply.bytes {
                return Err(invalid());
            }
            let required = {
                let input = input.clone();
                shared
                    .execute(ExecutionClass::Control, member.node_id, 0, move || {
                        crate::oltp::mvcc::json_decode_budget(input.as_slice())
                    })
                    .await?
            };
            let received = received.reserve_reply_decode(shared, required).await?;
            return shared
                .execute(ExecutionClass::Control, member.node_id, 0, move || {
                    let value = serde_json::from_slice(input.as_slice()).map_err(|_| invalid())?;
                    Ok((value, received.scratch.clone()))
                })
                .await;
        }
        Err(last)
    }
}
impl Consensus<ControlCommand, ControlReply> for ForwardedControl {
    fn propose(
        &self,
        command: OwnedCommand<ControlCommand>,
    ) -> ConsensusFuture<'_, Applied<ControlReply>> {
        Box::pin(async move {
            command.ensure_charged()?;
            let operation = if matches!(command.payload()?, ControlCommand::CompleteRestart { .. }) {
                Operation::CompleteRestart
            } else {
                Operation::Propose
            };
            let local_id = self.local.state.resident().node_id();
            if matches!(operation, Operation::Propose) && self
                .manager
                .leader_candidates(self.local.state.store().identity().group_id)?
                .first()
                .is_some_and(|member| member.node_id == local_id)
                && self.local.raft.current_leader().await == Some(local_id)
            {
                return self.local.propose(command).await;
            }
            let shared = self.manager.resources().clone();
            let encode = shared.clone();
            let payload = shared
                .execute(ExecutionClass::Control, local_id, 0, move || {
                    encode.encode_in(ByteClass::Progress, &command, PAYLOAD)
                })
                .await?;
            let (applied, owner): (Applied<ControlReply>, _) =
                self.forwarded(operation, Some(payload)).await?;
            Ok(Applied::owned(applied.position, applied.reply, owner))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async move {
            let deadline = Instant::now() + READ_TIMEOUT;
            if let Some(batch) = self.read_batch.join(self.manager.resources(), || {
                let forwarding = self.clone();
                async move { forwarding.read_barrier(Instant::now() + READ_TIMEOUT).await }
            }) {
                let (result, _owner) = tokio::time::timeout_at(deadline, batch)
                    .await
                    .map_err(|_| unavailable())?;
                if Instant::now() >= deadline {
                    return Err(unavailable());
                }
                return result;
            }
            self.read_barrier(deadline).await
        })
    }
}
impl ForwardedControl {
    async fn read_barrier(&self, deadline: Instant) -> Result<ReadBarrier, ProtocolError> {
        let local_id = self.local.state.resident().node_id();
        loop {
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            let read = async {
                if self.local.raft.current_leader().await == Some(local_id) {
                    return self.local.ensure_linearizable().await;
                }
                let (applied, _): (AppliedPosition, _) =
                    self.forwarded(Operation::Barrier, None).await?;
                self.local
                    .raft
                    .wait(Some(deadline.saturating_duration_since(Instant::now())))
                    .applied_index_at_least(Some(applied.index), "forwarded Control ReadIndex")
                    .await
                    .map_err(|_| unavailable())?;
                Ok(ReadBarrier { applied })
            };
            match tokio::time::timeout_at(deadline, read).await {
                _ if Instant::now() >= deadline => return Err(unavailable()),
                Ok(Ok(barrier)) => return Ok(barrier),
                Ok(Err(error)) if transient(&error) => {
                    tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(25)))
                        .await;
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => return Err(unavailable()),
            }
        }
    }
}
fn transient(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}

pub(crate) struct HostedControlEndpoint {
    reads: Arc<ControlEndpoint>,
    local: ManagedControl,
    shared: Arc<WorkerSharedResources>,
    pins: BTreeSet<Digest>,
    _owner: Arc<ScratchLease>,
}
impl HostedControlEndpoint {
    pub(crate) fn new(
        reads: Arc<ControlEndpoint>,
        local: ManagedControl,
        shared: Arc<WorkerSharedResources>,
        pins: BTreeSet<Digest>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if pins.is_empty() || !Arc::ptr_eq(local.state.store().resources(), &shared) {
            return Err(invalid());
        }
        let owner = Arc::new(shared.reserve_scratch(
            ByteClass::Progress,
            std::mem::size_of::<Self>() + pins.len() * 64,
        )?);
        Ok(Arc::new(Self {
            reads,
            local,
            shared,
            pins,
            _owner: owner,
        }))
    }
    fn request(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<Option<Request>, ProtocolError> {
        crate::protocol::json_input(bytes, HEADER).map_err(|_| invalid())?;
        let request = match serde_json::from_slice::<Request>(bytes) {
            Ok(request) => request,
            Err(_) if session.purpose() == Purpose::Authority => return Ok(None),
            Err(_) => return Err(invalid()),
        };
        let scoped_data_worker = matches!(
            request.control_consensus,
            Operation::CompleteDataWorker | Operation::BeginDataWorkerJoin | Operation::CloseDataWorker
            | Operation::ReadEksWorkerReplacement | Operation::CompleteEksWorkerReplacement
        );
        if session.group() != self.local.state.store().identity()
            || session.purpose() != Purpose::ControlDelegation
            || (!scoped_data_worker && !self.pins.contains(&session.remote().certificate_sha256))
            || (scoped_data_worker && self.pins.contains(&session.remote().certificate_sha256))
            || request.to_node != self.local.state.resident().node_id()
            || match request.control_consensus {
                Operation::Propose | Operation::CompleteRestart | Operation::CompleteDataWorker
                | Operation::BeginDataWorkerJoin | Operation::CloseDataWorker | Operation::CompleteEksWorkerReplacement => {
                    request.payload_bytes == 0 || request.payload_bytes > PAYLOAD as u64
                }
                Operation::Barrier | Operation::ReadEksWorkerReplacement => request.payload_bytes != 0,
            }
            || match request.control_consensus {
                Operation::ReadEksWorkerReplacement | Operation::CompleteEksWorkerReplacement =>
                    !(34..=(MAX_TOKEN as u64 + 2)).contains(&request.credential_bytes),
                Operation::BeginDataWorkerJoin => request.credential_bytes != 0
                    && !(34..=(MAX_TOKEN as u64 + 2)).contains(&request.credential_bytes),
                _ => request.credential_bytes != 0,
            }
            || session.recovery_only()
                && !matches!(request.control_consensus, Operation::CompleteRestart | Operation::CompleteDataWorker
                    | Operation::ReadEksWorkerReplacement | Operation::CompleteEksWorkerReplacement)
        {
            return Err(invalid());
        }
        Ok(Some(request))
    }
}
impl Endpoint for HostedControlEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let Some(request) = self.request(session, bytes)? else {
                return self.reads.preflight(session, bytes).await;
            };
            Ok(ReceivePlan {
                class: TrafficClass::ControlDelegation,
                section_lengths: [request.payload_bytes, request.credential_bytes, 0],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let Some(request) = self.request(&session, bytes.as_slice())? else {
                return self.reads.handle_with_leader(
                    session, bytes, sections, Some(self.local.as_ref()),
                ).await;
            };
            let result = async {
                let token = sections.section(1).map(|bytes| {
                    let token: &str = serde_json::from_slice(bytes.as_slice()).map_err(|_| invalid())?;
                    if !(32..=MAX_TOKEN).contains(&token.len())
                        || !token.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
                    { return Err(invalid()); }
                    Ok(super::machine::hash(token.as_bytes()))
                }).transpose()?;
                match request.control_consensus {
                    Operation::ReadEksWorkerReplacement => self.reads.read_eks_replacement(session.remote(), token.ok_or_else(invalid)?).await,
                    Operation::Barrier => {
                        let barrier = self.local.ensure_linearizable().await?;
                        self.shared
                            .encode_in(ByteClass::Progress, &barrier.applied, PAYLOAD)
                    }
                    Operation::Propose | Operation::CompleteRestart | Operation::CompleteDataWorker
                    | Operation::BeginDataWorkerJoin | Operation::CloseDataWorker | Operation::CompleteEksWorkerReplacement => {
                        let input = sections.section(0).ok_or_else(invalid)?.clone();
                        let shared = self.shared.clone();
                        let command = self
                            .shared
                            .execute(ExecutionClass::Control, request.to_node, 0, move || {
                                OwnedCommand::<ControlCommand>::decode_json(
                                    &input,
                                    &shared,
                                    ByteClass::Progress,
                                    DECODE,
                                )
                            })
                            .await?;
                        match (request.control_consensus, command.payload()?) {
                            (Operation::CompleteDataWorker, ControlCommand::CompleteDataWorker { intent, .. })
                                if intent.worker() == session.remote() => {}
                            (Operation::BeginDataWorkerJoin, ControlCommand::BeginDataWorkerJoin { worker, .. })
                                if *worker == session.remote() => {
                                    self.reads.authorize_data_worker_join(*worker, token).await?;
                                }
                            (Operation::CloseDataWorker, ControlCommand::CloseDataWorker { proof, .. })
                                if proof.worker == session.remote() => {}
                            (Operation::CompleteEksWorkerReplacement, ControlCommand::CompleteEksWorkerReplacement { worker, .. })
                                if *worker == session.remote() => {
                                    self.reads.read_eks_replacement(*worker, token.ok_or_else(invalid)?).await?;
                                }
                            (Operation::CompleteDataWorker | Operation::BeginDataWorkerJoin | Operation::CloseDataWorker | Operation::CompleteEksWorkerReplacement, _)
                            | (_, ControlCommand::CompleteDataWorker { .. } | ControlCommand::BeginDataWorkerJoin { .. } | ControlCommand::CloseDataWorker { .. } | ControlCommand::CompleteEksWorkerReplacement { .. }) =>
                                return Err(invalid()),
                            (Operation::CompleteRestart, ControlCommand::CompleteRestart { intent, .. })
                                if WorkerIdentity::from(&intent.next) == session.remote() => {}
                            (Operation::Propose, ControlCommand::CompleteRestart { .. })
                            | (Operation::CompleteRestart, _) => return Err(invalid()),
                            (_, ControlCommand::EnsureScopedQueue { .. } | ControlCommand::ScopedQueue { .. }) =>
                                return Err(invalid()),
                            (
                                Operation::Propose,
                                ControlCommand::RegisterRestartMount { worker, .. },
                            ) if *worker != session.remote() => return Err(invalid()),
                            (
                                Operation::Propose,
                                ControlCommand::RegisterInitialData { authorization, .. },
                            ) if authorization.subject != session.remote().certificate_sha256 => {
                                return Err(invalid());
                            }
                            (Operation::Propose, ControlCommand::BeginReplicaMove { plan, .. })
                                if plan.owner != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::ArmReplicaMove { worker, .. }
                                    | ControlCommand::CancelReplicaMove { worker, .. })
                                if *worker != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::PublishReplicaMove { proof, .. })
                                if proof.worker != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::BeginWorkerDrain { worker, .. }
                                    | ControlCommand::BeginWorkerDrainRemoval { worker, .. }
                                    | ControlCommand::EksWorkerReplacement { worker, .. })
                                if *worker != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::PoolDrain { worker, .. })
                                if *worker != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::ReplicaRetired { proof, remote: None, .. })
                                if proof.worker != session.remote() => return Err(invalid()),
                            (Operation::Propose, ControlCommand::ReplicaRetired { authorization, remote: Some(_), .. })
                                if authorization.subject != session.remote().certificate_sha256 => return Err(invalid()),
                            _ => {}
                        }
                        let applied = self.local.propose(command).await?;
                        self.shared
                            .encode_in(ByteClass::Progress, &applied, PAYLOAD)
                    }
                }
            }
            .await;
            let (payload, error) = match result {
                Ok(bytes) => (Some(bytes), None),
                Err(error) => (None, Some(error)),
            };
            let header = self.shared.encode_in(
                ByteClass::Progress,
                &Reply {
                    nonce: session.nonce(),
                    node: request.to_node,
                    bytes: payload.as_ref().map_or(0, |bytes| bytes.len() as u64),
                    error,
                },
                HEADER,
            )?;
            Ok(ChargedReply {
                header,
                sections: [payload, None, None],
            })
        })
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "Control operator delegation identity or request differs",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "Control leader quorum or local application unavailable",
        Outcome::Unknown,
    )
}
