use super::*;
use crate::oltp::mvcc::store::ParticipantHandle;
use crate::oltp_replication::authority::status::identity::transaction::ColdRecoveryPage;
use crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage;
use crate::oltp_replication::authority::status::{CommitPermitStatement, StatusTopology};
use crate::oltp_replication::authority::timestamp::cold::ColdApplicationPin;
use crate::oltp_replication::authority::{
    control::ScopedQueuePlacement,
    status::{
        ArchiveCursor, CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage, ScopedAdmission,
        IdentityReservation,
        StatusArchiveCutStatement, VerifiedStatusArchiveCut,
    },
    timestamp::{VerifiedArchiveClosure, archive::ArchiveClosureStatement},
};
use uuid::Uuid;

const INSTALL_POINT_MANIFEST: u8 = 26;
const POINT_CALL_PAYLOAD_LIMIT: usize = 4096;

// External tags preserve CanonicalJson's RawValue decoder.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Request {
    ArchiveMetadata {
        closure: ArchiveClosureStatement,
        after: Option<Vec<u8>>,
    },
    ArchiveCut {
        authority: Authority,
    },
    ArchiveDecisions {
        closure: ArchiveClosureStatement,
        after: Option<ArchiveCursor>,
    },
    ArchiveIdentities {
        closure: ArchiveClosureStatement,
        after: Option<IdentityArchiveCursor>,
    },
    ColdRecovery {
        authority: Authority,
        after: Option<TxnId>,
    },
    ColdApplicationPin {
        authority: Authority,
        pin: ColdApplicationPin,
    },
    Current,
    Reject {
        ticket: super::admission::OwnerAdmissionTicket,
    },
    Lookup {
        request_id: Uuid,
        digest: Option<Digest>,
    },
    Reserve {
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scoped: Option<ScopedAdmission>,
    },
    InstallPointManifest(PointManifest),
    Begin {
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participants: u64,
        result: CanonicalJson,
    },
    Page {
        authority: Authority,
        txn: TxnId,
        index: u64,
        previous: Digest,
        participants: Page<ParticipantPlan>,
    },
    Seal {
        authority: Authority,
        txn: TxnId,
    },
    Prepared {
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    },
    PointPreparedPermit {
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    },
    Resolved {
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    },
    Commit {
        authority: Authority,
        txn: TxnId,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    },
    PointCommitDecision {
        authority: Authority,
        txn: TxnId,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    },
    Abort {
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    },
    Failure {
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    },
    Inspect {
        txn: TxnId,
    },
    Permit {
        authority: Authority,
        txn: TxnId,
    },
    Manifest {
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    },
    Abandon {
        authority: Authority,
        txn: TxnId,
    },
    Fence {
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    },
    FailureSettlement {
        txn: TxnId,
        request: RequestBinding,
    },
    Retire {
        statement: TerminalCopyStatement,
    },
    Retirement {
        request_id: Uuid,
        digest: Digest,
    },
}
impl Request {
    fn request_id(&self) -> Option<Uuid> {
        match self {
            Self::Lookup { request_id, .. }
            | Self::Reserve { request_id, .. }
            | Self::Retirement { request_id, .. } => Some(*request_id),
            Self::InstallPointManifest(point) => Some(point.txn.request_id),
            Self::Begin { txn, .. }
            | Self::Page { txn, .. }
            | Self::Seal { txn, .. }
            | Self::Prepared { txn, .. }
            | Self::PointPreparedPermit { txn, .. }
            | Self::Resolved { txn, .. }
            | Self::Commit { txn, .. }
            | Self::PointCommitDecision { txn, .. }
            | Self::Abort { txn, .. }
            | Self::Inspect { txn }
            | Self::Permit { txn, .. }
            | Self::Manifest { txn, .. }
            | Self::Abandon { txn, .. }
            | Self::FailureSettlement { txn, .. } => Some(txn.request_id),
            Self::Failure { request, .. } => Some(request.request_id),
            Self::Retire { statement, .. } => Some(statement.request.request_id),
            Self::Reject { ticket } => Some(ticket.expected.reservation.request_id),
            Self::ColdRecovery { after, .. } => after.map(|txn| txn.request_id),
            Self::ColdApplicationPin { pin, .. } => Some(pin.reservation.request_id),
            Self::Current
            | Self::Fence { .. }
            | Self::ArchiveCut { .. }
            | Self::ArchiveDecisions { .. }
            | Self::ArchiveIdentities { .. }
            | Self::ArchiveMetadata { .. } => None,
        }
    }

    fn method(&self) -> Method {
        let operation = match self {
            Self::ArchiveCut { .. } => 19,
            Self::ArchiveDecisions { .. } => 20,
            Self::ArchiveIdentities { .. } => 21,
            Self::ArchiveMetadata { .. } => 22,
            Self::ColdRecovery { .. } => 23,
            Self::Current => 0,
            Self::Lookup { .. } => 1,
            Self::Reserve { .. } => 2,
            Self::InstallPointManifest(_) => INSTALL_POINT_MANIFEST,
            Self::Begin { .. } => 3,
            Self::Page { .. } => 4,
            Self::Seal { .. } => 5,
            Self::Prepared { .. } => 6,
            Self::PointPreparedPermit { .. } => POINT_PREPARED_PERMIT,
            Self::Resolved { .. } => 7,
            Self::Commit { .. } => 8,
            Self::PointCommitDecision { .. } => POINT_COMMIT_DECISION,
            Self::Abort { .. } => 9,
            Self::Failure { .. } => 10,
            Self::Inspect { .. } => 11,
            Self::Permit { .. } => 12,
            Self::Manifest { .. } => 13,
            Self::Abandon { .. } => 14,
            Self::Fence { .. } => 15,
            Self::FailureSettlement { .. } => 16,
            Self::Retire { .. } => 17,
            Self::Retirement { .. } => 24,
            Self::ColdApplicationPin { .. } => 25,
            Self::Reject { .. } => 18,
        };
        Method::StatusCall {
            operation,
            mutation: !matches!(
                self,
                Self::Current
                    | Self::Lookup { .. }
                    | Self::Inspect { .. }
                    | Self::Permit { .. }
                    | Self::Manifest { .. }
                    | Self::FailureSettlement { .. }
                    | Self::Retirement { .. }
                    | Self::ArchiveCut { .. }
                    | Self::ArchiveDecisions { .. }
                    | Self::ArchiveIdentities { .. }
                    | Self::ArchiveMetadata { .. }
                    | Self::ColdRecovery { .. }
                    | Self::ColdApplicationPin { .. }
            ),
        }
    }
}

pub(crate) struct StatusEndpoint {
    proof: Arc<StatusProofEndpoint>,
    transport: WorkerClient,
    timestamps: GroupIdentity,
    topology: StatusTopology,
    admission: std::sync::OnceLock<Arc<dyn super::admission::OwnerAdmissionReader>>,
}
impl StatusEndpoint {
    async fn archive_closure(
        &self,
        statement: &ArchiveClosureStatement,
    ) -> Result<VerifiedArchiveClosure, ProtocolError> {
        if statement.status != *self.proof.handle.identity()
            || statement.timestamps != self.timestamps
            || registered_target(&self.proof.control, self.timestamps.group_id).await?
                != self.timestamps
        {
            return Err(invalid());
        }
        let client = TimestampClient::new(
            self.timestamps.clone(),
            self.proof.shared.clone(),
            self.transport.clone(),
            self.proof.control.clone(),
            ServiceAction::StatusProof {
                source_group: self.proof.handle.identity().group_id,
            },
        )?;
        client.archive_lookup(statement.clone()).await
    }
    pub(crate) fn new(
        handle: Arc<StatusHandle>,
        node: u64,
        control: Arc<ControlClient>,
        transport: WorkerClient,
        timestamps: GroupIdentity,
        topology: StatusTopology,
    ) -> Result<Arc<Self>, ProtocolError> {
        if timestamps.kind != GroupKind::Timestamps
            || timestamps.cluster_id != handle.identity().cluster_id
            || timestamps.database != handle.identity().database
            || !topology.contains(handle.identity())
        {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            proof: StatusProofEndpoint::new(
                handle.clone(),
                node,
                control,
                handle.resources().clone(),
                Some(topology.clone()),
            )?,
            transport,
            timestamps,
            topology,
            admission: std::sync::OnceLock::new(),
        }))
    }
    pub(crate) fn install_admission_reader(
        &self,
        reader: Arc<dyn super::admission::OwnerAdmissionReader>,
    ) -> Result<(), ProtocolError> {
        self.admission.set(reader).map_err(|_| invalid())
    }
    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        let header = parse(
            session,
            self.proof.handle.identity(),
            self.proof.node,
            bytes,
        )?;
        let allowed = match header.method {
            Method::StatusCall {
                operation: 1,
                mutation: false,
            } => matches!(
                header.action,
                ServiceAction::Owner
                    | ServiceAction::TimestampProof { .. }
                    | ServiceAction::ScopedQueueRead { .. }
            ),
            Method::StatusCall {
                operation: 2,
                mutation: true,
            } => matches!(
                header.action,
                ServiceAction::Owner | ServiceAction::ScopedQueueReserve { .. }
            ),
            Method::StatusCall {
                operation: 15,
                mutation: true,
            } => matches!(
                header.action,
                ServiceAction::ControlFence { .. } | ServiceAction::Operator
            ),
            Method::StatusCall {
                operation: 0 | 12 | 16 | 19,
                mutation: false,
            } => matches!(
                header.action,
                ServiceAction::Owner | ServiceAction::TimestampProof { .. }
            ),
            Method::StatusCall {
                operation: 24,
                mutation: false,
            } => header.action == ServiceAction::QueueRetirement,
            Method::StatusCall {
                operation: 25,
                mutation: false,
            } => matches!(header.action, ServiceAction::TimestampProof { source_group }
                if source_group == self.timestamps.group_id),
            Method::StatusCall { .. } => header.action == ServiceAction::Owner,
            _ => false,
        };
        if !allowed
            || (matches!(
                header.method,
                Method::StatusCall {
                    operation: INSTALL_POINT_MANIFEST | POINT_PREPARED_PERMIT | POINT_COMMIT_DECISION,
                    ..
                }
            ) && header.payload_bytes > POINT_CALL_PAYLOAD_LIMIT as u64)
        {
            return Err(invalid());
        }
        Ok(header)
    }
    async fn participant(
        &self,
        group: GroupIdentity,
    ) -> Result<Arc<ParticipantProofClient>, ProtocolError> {
        if registered_target(&self.proof.control, group.group_id).await? != group {
            return Err(invalid());
        }
        ParticipantProofClient::new(
            group,
            self.proof.shared.clone(),
            self.transport.clone(),
            self.proof.control.clone(),
            ServiceAction::StatusProof {
                source_group: self.proof.handle.identity().group_id,
            },
        )
    }
    async fn dispatch(
        &self,
        session: &AuthenticatedSession,
        header: &Header,
        request: Request,
    ) -> Result<ChargedBytes, ProtocolError> {
        if request.method() != header.method {
            return Err(invalid());
        }
        let handle = &self.proof.handle;
        if request
            .request_id()
            .is_some_and(|id| self.topology.request(id) != handle.identity())
        {
            return Err(invalid());
        }
        match &request {
            Request::Prepared { txn, manifest, .. }
            | Request::Resolved { txn, manifest, .. }
            | Request::Manifest { txn, manifest, .. } => {
                self.topology.manifest(*txn, *manifest)?;
            }
            _ => {}
        }
        let shared = &self.proof.shared;
        match request {
            Request::ArchiveCut { authority } => {
                encode(
                    shared,
                    handle.archive_cut(authority).await?.statement().clone(),
                )
                .await
            }
            Request::ArchiveDecisions { closure, after } => {
                let proof = self.archive_closure(&closure).await?;
                encode(
                    shared,
                    handle
                        .committed_page(&proof, closure.base_exclusive, after)
                        .await?,
                )
                .await
            }
            Request::ArchiveMetadata { closure, after } => {
                let proof = self.archive_closure(&closure).await?;
                encode(shared, handle.native_archive_page(&proof, after).await?).await
            }
            Request::ArchiveIdentities { closure, after } => {
                let proof = self.archive_closure(&closure).await?;
                encode(shared, handle.identity_history_page(&proof, after).await?).await
            }
            Request::ColdRecovery { authority, after } => {
                handle.cold_recovery_page(authority, after).await
            }
            Request::ColdApplicationPin { authority, pin } => {
                handle.cold_application_pin_settled(authority, pin).await
            }
            Request::Reject { ticket } => {
                if ticket.status_group != *handle.identity() || ticket.owner != session.remote() {
                    return Err(invalid());
                }
                let proof = self
                    .admission
                    .get()
                    .ok_or_else(unavailable)?
                    .verify(&ticket)
                    .await?;
                let rejection = handle.reject_owner_observation(proof).await?;
                encode(shared, rejection.into_sealed()).await
            }
            Request::Current => encode(shared, handle.current_authority().await?).await,
            Request::Lookup { request_id, digest } => {
                if let ServiceAction::ScopedQueueRead {
                    queue_group_id,
                    queue_generation,
                    queue_view,
                    request_id: authorized_id,
                } = header.action
                {
                    if authorized_id != request_id || digest.is_none() {
                        return Err(invalid());
                    }
                    let value = handle.lookup_identity(request_id, digest).await?;
                    let reservation = match value {
                        Some(IdentityLookup::Reserved(reservation)) => reservation,
                        _ => return Err(unavailable()),
                    };
                    if reservation.original_run != queue_view.run_id
                        || reservation.scoped != Some(ScopedAdmission {
                            protocol_version: ScopedAdmission::VERSION,
                            queue_group_id,
                            queue_generation,
                            status_group_id: handle.identity().group_id,
                        })
                    {
                        return Err(unavailable());
                    }
                    return encode(shared, reservation).await;
                }
                let value = handle.lookup_identity(request_id, digest).await?;
                encode(shared, value).await
            }
            Request::Reserve {
                authority,
                request_id,
                digest,
                kind,
                scoped,
            } => {
                match (header.action, scoped) {
                    (ServiceAction::Owner, None) => {}
                    (
                        ServiceAction::ScopedQueueReserve {
                            queue_group_id,
                            queue_generation,
                            queue_view,
                            request_id: authorized_id,
                        },
                        Some(binding),
                    ) if binding.protocol_version == ScopedAdmission::VERSION
                        && binding.status_group_id == handle.identity().group_id
                        && binding.queue_group_id == queue_group_id
                        && binding.queue_generation == queue_generation
                        && queue_view.run_id == authority.run_id
                        && authorized_id == request_id =>
                    {
                        // Recheck after receiving the payload. The durable
                        // Status proposal independently checks its authority.
                        let scope = crate::oltp_runtime::replica::QueueScope {
                            cluster_id: session.group().cluster_id,
                            database: session.group().database.clone(),
                        };
                        let grant = self
                            .proof
                            .control
                            .service_role(
                                &scope,
                                session.remote(),
                                header.action,
                                session.group().clone(),
                            )
                            .await?;
                        if grant.owner != Some(session.remote())
                            || grant.effective_authority != authority
                        {
                            return Err(unavailable());
                        }
                    }
                    _ => return Err(invalid()),
                }
                encode(
                    shared,
                    handle
                        .reserve_admission(authority, request_id, digest, kind, scoped)
                        .await?,
                )
                .await
            }
            Request::InstallPointManifest(point) => {
                let installed = handle.install_point_manifest(point).await?;
                encode(shared, installed)
                    .await
                    .map_err(owner_operation_uncertain)
            }
            Request::Begin {
                authority,
                txn,
                request,
                read_ts,
                participants,
                result,
            } => {
                encode(
                    shared,
                    handle
                        .begin_manifest(authority, txn, request, read_ts, participants, result)
                        .await?,
                )
                .await
            }
            Request::Page {
                authority,
                txn,
                index,
                previous,
                participants,
            } => {
                encode(
                    shared,
                    handle
                        .append_manifest_page(authority, txn, index, previous, participants)
                        .await?,
                )
                .await
            }
            Request::Seal { authority, txn } => {
                encode(shared, handle.seal_manifest(authority, txn).await?).await
            }
            Request::Prepared {
                authority,
                group,
                txn,
                manifest,
            } => {
                let proof = self.participant(group).await?.seal(txn, manifest).await?;
                handle.observe_prepared(authority, proof).await?;
                encode(shared, ()).await
            }
            Request::PointPreparedPermit {
                authority,
                group,
                txn,
                manifest,
            } => {
                if manifest.status_group_id != handle.identity().group_id
                    || manifest.participant_count != 1
                    || !matches!(group.kind, GroupKind::Data { .. })
                {
                    return Err(invalid());
                }
                let proof = self.participant(group).await?.seal(txn, manifest).await?;
                handle.observe_prepared(authority, proof).await?;
                let permit = handle
                    .commit_permit(authority, txn)
                    .await
                    .map_err(owner_operation_uncertain)?;
                encode(shared, permit.statement().clone())
                    .await
                    .map_err(owner_operation_uncertain)
            }
            Request::Resolved {
                authority,
                group,
                txn,
                manifest,
            } => {
                let proof = self
                    .participant(group)
                    .await?
                    .resolution(txn, manifest)
                    .await?;
                handle.observe_resolved(authority, proof).await?;
                encode(shared, ()).await
            }
            Request::Permit { authority, txn } => {
                encode(
                    shared,
                    handle
                        .commit_permit(authority, txn)
                        .await?
                        .statement()
                        .clone(),
                )
                .await
            }
            Request::Commit {
                authority,
                txn,
                permit,
                stamp,
            }
            | Request::PointCommitDecision {
                authority,
                txn,
                permit,
                stamp,
            } => {
                let inline = matches!(
                    header.method,
                    Method::StatusCall { operation: POINT_COMMIT_DECISION, .. }
                );
                if inline && permit.statement().manifest.participant_count != 1 {
                    return Err(invalid());
                }
                let client = TimestampClient::new(
                    self.timestamps.clone(),
                    shared.clone(),
                    self.transport.clone(),
                    self.proof.control.clone(),
                    ServiceAction::StatusProof {
                        source_group: handle.identity().group_id,
                    },
                )?;
                let (actual, allocated) = tokio::try_join!(
                    handle.commit_permit(authority, txn),
                    client.confirmed_allocation(permit.clone()),
                )?;
                if actual != permit || allocated != stamp {
                    return Err(invalid());
                }
                let decision = handle
                    .decide_commit(authority, actual, allocated)
                    .await
                    .map_err(|error| {
                        if inline {
                            owner_operation_uncertain(error)
                        } else {
                            error
                        }
                    })?;
                if inline {
                    let outcome = decision.outcome().ok_or_else(unavailable)?;
                    encode(shared, (decision.statement().clone(), outcome.clone()))
                        .await
                        .map_err(owner_operation_uncertain)
                } else {
                    encode(shared, decision.statement().manifest).await
                }
            }
            Request::Abort {
                authority,
                txn,
                reason,
                error,
            } => {
                encode(
                    shared,
                    handle
                        .abort(authority, txn, reason, error)
                        .await?
                        .statement()
                        .manifest,
                )
                .await
            }
            Request::Abandon { authority, txn } => {
                encode(
                    shared,
                    handle
                        .abandon_fenced(authority, txn)
                        .await?
                        .statement()
                        .manifest,
                )
                .await
            }
            Request::Failure {
                authority,
                request,
                error,
            } => {
                encode(
                    shared,
                    handle.terminal_failure(authority, request, error).await?,
                )
                .await
            }
            Request::Inspect { txn } => {
                encode(shared, handle.inspect_transaction(txn).await?).await
            }
            Request::Manifest {
                txn,
                manifest,
                after,
            } => encode(shared, handle.manifest_page(txn, manifest, after).await?).await,
            Request::FailureSettlement { txn, request } => {
                encode(
                    shared,
                    handle
                        .failure_settled_proof(txn, request)
                        .await?
                        .into_sealed()
                        .statement()
                        .clone(),
                )
                .await
            }
            Request::Fence {
                transition_id,
                expected,
                next,
            } => {
                let scope = crate::oltp_runtime::replica::QueueScope {
                    cluster_id: handle.identity().cluster_id,
                    database: handle.identity().database.clone(),
                };
                let grant = self
                    .proof
                    .control
                    .service_role(
                        &scope,
                        session.remote(),
                        header.action,
                        handle.identity().clone(),
                    )
                    .await?;
                match header.action {
                    ServiceAction::ControlFence {
                        transition_id: actual,
                    } if actual == transition_id && grant.effective_authority == next => {}
                    ServiceAction::Operator
                        if expected.is_none() && grant.effective_authority == next => {}
                    _ => return Err(invalid()),
                }
                encode(shared, handle.fence(transition_id, expected, next).await?).await
            }
            Request::Retire { statement } => {
                let scope = crate::oltp_runtime::replica::QueueScope {
                    cluster_id: handle.identity().cluster_id,
                    database: handle.identity().database.clone(),
                };
                let grant = self
                    .proof
                    .control
                    .service_role(
                        &scope,
                        session.remote(),
                        header.action,
                        handle.identity().clone(),
                    )
                    .await?;
                if grant.ready != statement.ready {
                    return Err(invalid());
                }
                let release = TimestampClient::new(
                    self.timestamps.clone(),
                    shared.clone(),
                    self.transport.clone(),
                    self.proof.control.clone(),
                    ServiceAction::StatusProof {
                        source_group: handle.identity().group_id,
                    },
                )?
                .confirmed_release(statement)
                .await?;
                let retired = handle.retire_terminal(release).await?;
                encode(shared, retired.into_certificate()).await
            }
            Request::Retirement { request_id, digest } => {
                let proof = handle.retirement(request_id, digest).await?;
                encode(
                    shared,
                    proof.map(VerifiedTerminalRetirement::into_certificate),
                )
                .await
            }
        }
    }
}
impl Endpoint for StatusEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let head = parse(
                session,
                self.proof.handle.identity(),
                self.proof.node,
                bytes,
            )?;
            if !matches!(head.method, Method::StatusCall { .. }) {
                return self.proof.preflight(session, bytes).await;
            }
            let head = self.parse(session, bytes)?;
            authorize(&self.proof.control, session, &head).await?;
            Ok(ReceivePlan {
                class: head.method.class(),
                section_lengths: [head.payload_bytes, 0, 0],
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
            let head = parse(
                &session,
                self.proof.handle.identity(),
                self.proof.node,
                bytes.as_slice(),
            )?;
            if !matches!(head.method, Method::StatusCall { .. }) {
                return self.proof.handle(session, bytes, sections).await;
            }
            let head = self.parse(&session, bytes.as_slice())?;
            let result = async {
                let (request, owner): (Request, _) = decode(&self.proof.shared, sections).await?;
                match &request {
                    Request::Begin { result, .. } => result.attach_charge(owner.clone())?,
                    Request::InstallPointManifest(point) => {
                        point.result.attach_charge(owner.clone())?
                    }
                    _ => {}
                }
                let result = self.dispatch(&session, &head, request).await;
                drop(owner);
                result
            }
            .await;
            reply(&self.proof.shared, &session, self.proof.node, result).await
        })
    }
}

#[derive(Clone)]
pub(crate) struct StatusClient {
    client: Arc<Client>,
    proof: Option<Arc<StatusProofClient>>,
    admission: Arc<std::sync::OnceLock<Arc<dyn super::admission::AdmissionTicketIssuer>>>,
}
impl StatusClient {
    pub(crate) async fn cold_application_pin_settled(
        &self,
        authority: Authority,
        pin: ColdApplicationPin,
    ) -> Result<bool, ProtocolError> {
        let (settled, _) = self
            .value(Request::ColdApplicationPin { authority, pin })
            .await?;
        Ok(settled)
    }

    pub(crate) async fn cold_recovery_page(
        &self,
        authority: Authority,
        after: Option<TxnId>,
    ) -> Result<ColdRecoveryPage, ProtocolError> {
        let (page, _): (ColdRecoveryPage, _) = self
            .value(Request::ColdRecovery { authority, after })
            .await?;
        if page.after < after
            || !page.done && page.after == after
            || page.transaction.is_some_and(|txn| page.after != Some(txn))
        {
            return Err(invalid());
        }
        Ok(page)
    }

    pub(crate) async fn archive_cut(
        &self,
        authority: Authority,
    ) -> Result<VerifiedStatusArchiveCut, ProtocolError> {
        let response = self.response(Request::ArchiveCut { authority }).await?;
        let context = response.metadata_context()?;
        let (statement, _): (StatusArchiveCutStatement, _) =
            response.decoded(&self.client.shared).await?;
        VerifiedStatusArchiveCut::from_authenticated(context, statement, authority)
    }
    pub(crate) async fn committed_page(
        &self,
        closure: &VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> Result<CommittedArchivePage, ProtocolError> {
        if base_exclusive != closure.base_exclusive() || !closure.covers_status(self.identity()) {
            return Err(invalid());
        }
        let (mut page, owner): (CommittedArchivePage, _) = self
            .value(Request::ArchiveDecisions {
                closure: closure.statement().clone(),
                after,
            })
            .await?;
        if page.status_group != *self.identity()
            || page.cut != closure.cut()
            || page.base_exclusive != base_exclusive
            || page.status_applied_at != closure.status_applied_at()
        {
            return Err(invalid());
        }
        page.retain_owner(owner)?;
        Ok(page)
    }
    pub(crate) async fn identity_history_page(
        &self,
        closure: &VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> Result<IdentityArchivePage, ProtocolError> {
        if !closure.covers_status(self.identity()) {
            return Err(invalid());
        }
        let (mut page, owner): (IdentityArchivePage, _) = self
            .value(Request::ArchiveIdentities {
                closure: closure.statement().clone(),
                after,
            })
            .await?;
        if page.status_group != *self.identity()
            || page.data_cut != closure.cut()
            || page.status_applied_at != closure.status_applied_at()
        {
            return Err(invalid());
        }
        page.retain_owner(owner)?;
        Ok(page)
    }
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !matches!(group.kind, GroupKind::Status { .. }) {
            return Err(invalid());
        }
        let proof = if action == ServiceAction::QueueRetirement {
            None
        } else {
            Some(StatusProofClient::new(
                group.clone(),
                shared.clone(),
                transport.clone(),
                control.clone(),
                action,
            )?)
        };
        Ok(Arc::new(Self {
            client: Arc::new(Client::new(group, shared, transport, control, action)?),
            proof,
            admission: Arc::new(std::sync::OnceLock::new()),
        }))
    }
    pub(crate) fn install_admission_issuer(
        &self,
        issuer: Arc<dyn super::admission::AdmissionTicketIssuer>,
    ) -> Result<(), ProtocolError> {
        self.admission.set(issuer).map_err(|_| invalid())
    }
    pub(crate) async fn reject_unaccepted(
        &self,
        cap: &crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        let ticket = self
            .admission
            .get()
            .ok_or_else(unavailable)?
            .issue(cap, self.identity())
            .await?;
        if !ticket.matches(cap, self.identity()) {
            return Err(invalid());
        }
        let response = self.response(Request::Reject { ticket }).await?;
        let context = response.metadata_context()?;
        let (sealed, _): (SealedAdmissionRejection, _) = response.decoded(self.resources()).await?;
        VerifiedAdmissionRejection::from_service(context, sealed, 18)
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        &self.client.group
    }
    pub(crate) fn resources(&self) -> &Arc<WorkerSharedResources> {
        &self.client.shared
    }
    async fn response(&self, request: Request) -> Result<ServiceResponse, ProtocolError> {
        // Reserve returns the existing identity for the same UUID and digest.
        let converge = matches!(
            request,
            Request::Current | Request::Lookup { .. } | Request::Reserve { .. }
        );
        self.client.named(request.method(), request, converge).await
    }
    async fn value<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        request: Request,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        self.response(request)
            .await?
            .decoded(&self.client.shared)
            .await
    }
    pub(crate) async fn current_authority(&self) -> Result<Option<Authority>, ProtocolError> {
        Ok(self.value(Request::Current).await?.0)
    }
    pub(crate) async fn reserve_identity(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        self.reserve_admission(authority, request_id, digest, kind, None)
            .await
    }
    async fn reserve_admission(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        scoped: Option<ScopedAdmission>,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        let request = Request::Reserve {
            authority,
            request_id,
            digest,
            kind,
            scoped,
        };
        let (applied, owner): (Applied<StatusReply>, _) = match scoped {
            None => self.value(request).await?,
            Some(binding) => {
                let queue = self
                    .client
                    .control
                    .scoped_queue_by_id(binding.queue_group_id)
                    .await?
                    .ok_or_else(unavailable)?;
                if binding.protocol_version != ScopedAdmission::VERSION
                    || binding.status_group_id != self.identity().group_id
                    || binding.queue_generation != queue.queue_generation
                    || authority.run_id != queue.view.run_id
                    || queue.owner != self.client.transport.local_identity()
                {
                    return Err(unavailable());
                }
                let action = ServiceAction::ScopedQueueReserve {
                    queue_group_id: binding.queue_group_id,
                    queue_generation: binding.queue_generation,
                    queue_view: queue.view,
                    request_id,
                };
                let method = request.method();
                let payload = encode(self.resources(), request).await?;
                self.client
                    .call_as(action, method, Some(payload))
                    .await
                    .map_err(owner_operation_uncertain)?
                    .decoded(self.resources())
                    .await
                    .map_err(owner_operation_uncertain)?
            }
        };
        let mut reply = applied.reply;
        reply.retain_owner(owner.clone());
        Ok(Applied::owned(applied.position, reply, owner))
    }
    pub(crate) async fn lookup_identity(
        &self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<Option<IdentityLookup>, ProtocolError> {
        let (mut value, owner): (Option<IdentityLookup>, _) =
            self.value(Request::Lookup { request_id, digest }).await?;
        if let Some(value) = &mut value {
            value.retain_owner(owner);
        }
        Ok(value)
    }
    async fn try_preferred_owner_response(
        &self,
        request: Request,
    ) -> Result<Option<ServiceResponse>, ProtocolError> {
        let preferred = self.client.leader.load(Ordering::Relaxed);
        if self.client.action != ServiceAction::Owner || preferred == 0 {
            return Ok(None);
        }
        let cached = self
            .client
            .control
            .cached_replica_routes(self.identity())
            .and_then(|routes| {
                let receiver = routes.hinted_receiver(preferred)?;
                Some((routes, receiver))
            });
        let (routes, receiver) = match cached {
            Some((routes, receiver)) => (routes, Some(receiver)),
            None => (
                self.client.control.replica_routes(self.identity()).await?,
                None,
            ),
        };
        let Some(member) = routes
            .members
            .iter()
            .find(|member| member.node_id == preferred)
        else {
            return Ok(None);
        };
        let method = request.method();
        let shared = self.client.shared.clone();
        let payload = self
            .client
            .shared
            .execute(ExecutionClass::Control, preferred, 0, move || {
                shared.encode_in(ByteClass::Progress, &request, POINT_CALL_PAYLOAD_LIMIT)
            })
            .await?;
        // Only a verified embedded legacy decoder error is safe here. A
        // local reply-decoding error can have the same text after execution.
        let target = receiver.map_or_else(|| member.worker_id.into(), Into::into);
        let reply = match self
            .client
            .call_member_at(
                ServiceAction::Owner,
                method,
                &Some(payload),
                member.node_id,
                target,
                receiver.map(|_| routes.record.generation),
            )
            .await
        {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(error)) if error == invalid() => return Ok(None),
            Err(CallError::Transport(RequestError::PeerRejected { worker, error }))
                if receiver == Some(worker) && placement_rejected_before_execution(&error) =>
            {
                self.client.control.invalidate_replica_routes(&routes);
                return Ok(None);
            }
            Err(CallError::Transport(RequestError::OutgoingLaneFull)) => return Ok(None),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(error.into()),
        }
        .map_err(|error| {
            if route_unavailable(&error) {
                self.client.control.invalidate_replica_routes(&routes);
                let _ = self.client.leader.compare_exchange(
                    preferred,
                    0,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            owner_operation_uncertain(error)
        })?;
        routes.record_successful_node(reply.node);
        Ok(Some(reply))
    }

    async fn try_point_manifest(
        &self,
        point: PointManifest,
    ) -> Result<Option<(ManifestRef, Digest)>, ProtocolError> {
        let expected_result = MutationOutcome::from_success(&point.result)?.digest()?;
        let Some(reply) = self
            .try_preferred_owner_response(Request::InstallPointManifest(point))
            .await?
        else {
            return Ok(None);
        };
        let ((manifest, result_digest), _): ((ManifestRef, Digest), _) = reply
            .decoded(&self.client.shared)
            .await
            .map_err(owner_operation_uncertain)?;
        if manifest.status_group_id != self.identity().group_id
            || manifest.participant_count != 1
            || result_digest != expected_result
        {
            return Err(owner_operation_uncertain(invalid()));
        }
        Ok(Some((manifest, result_digest)))
    }

    pub(crate) async fn begin_manifest(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        result: CanonicalJson,
    ) -> Result<ManifestDraft, ProtocolError> {
        let (mut draft, owner): (ManifestDraft, _) = self
            .value(Request::Begin {
                authority,
                txn,
                request,
                read_ts,
                participants: participant_count,
                result,
            })
            .await?;
        draft.outcome.retain_owner(owner);
        Ok(draft)
    }
    pub(crate) async fn append_manifest_page(
        &self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> Result<ManifestDraft, ProtocolError> {
        let (mut draft, owner): (ManifestDraft, _) = self
            .value(Request::Page {
                authority,
                txn,
                index: page_index,
                previous: previous_root,
                participants,
            })
            .await?;
        draft.outcome.retain_owner(owner);
        Ok(draft)
    }
    pub(crate) async fn seal_manifest(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<ManifestRef, ProtocolError> {
        Ok(self.value(Request::Seal { authority, txn }).await?.0)
    }
    async fn record_prepared(
        &self,
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<(), ProtocolError> {
        self.value::<()>(Request::Prepared {
            authority,
            group,
            txn,
            manifest,
        })
        .await?;
        Ok(())
    }
    async fn record_resolved(
        &self,
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<(), ProtocolError> {
        self.value::<()>(Request::Resolved {
            authority,
            group,
            txn,
            manifest,
        })
        .await?;
        Ok(())
    }
    async fn prepare_point_permit(
        &self,
        authority: Authority,
        group: GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Result<SealedCommitPermit, ProtocolError>, ProtocolError> {
        let reply = self
            .try_preferred_owner_response(Request::PointPreparedPermit {
                authority,
                group: group.clone(),
                txn,
                manifest,
            })
            .await;
        match reply {
            Ok(Some(reply)) => {
                let permit = async {
                    let context = reply.metadata_context()?;
                    let (statement, _): (CommitPermitStatement, _) =
                        reply.decoded(self.resources()).await?;
                    SealedCommitPermit::from_point_prepared(
                        context, statement, authority, txn, manifest,
                    )
                }
                .await
                .map_err(owner_operation_uncertain);
                Ok(permit)
            }
            Err(error) if error.outcome == Outcome::Unknown => Ok(Err(error)),
            Err(error) => Err(error),
            Ok(None) => {
                self.record_prepared(authority, group, txn, manifest).await?;
                Ok(self.commit_permit(authority, txn).await)
            }
        }
    }
    pub(crate) async fn commit_permit(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<SealedCommitPermit, ProtocolError> {
        let response = self.response(Request::Permit { authority, txn }).await?;
        let context = response.metadata_context()?;
        let (statement, _): (CommitPermitStatement, _) =
            response.decoded(&self.client.shared).await?;
        SealedCommitPermit::from_service(context, statement)
    }
    pub(crate) async fn decide_commit(
        &self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let txn = permit.statement().txn;
        if permit.statement().manifest.participant_count == 1
            && let Some(reply) = self
                .try_preferred_owner_response(Request::PointCommitDecision {
                    authority,
                    txn,
                    permit: permit.clone(),
                    stamp,
                })
                .await?
        {
            let context = reply.metadata_context().map_err(owner_operation_uncertain)?;
            let ((statement, outcome), _): ((DecisionStatement, MutationOutcome), _) = reply
                .decoded(self.resources())
                .await
                .map_err(owner_operation_uncertain)?;
            return VerifiedStatusDecision::from_point_commit(
                context, statement, outcome, permit.statement(), stamp,
            )
            .map_err(owner_operation_uncertain);
        }
        let (manifest, _): (ManifestRef, _) = self
            .value(Request::Commit {
                authority,
                txn,
                permit,
                stamp,
            })
            .await?;
        self.lookup_decision(txn, manifest)
            .await?
            .ok_or_else(unavailable)
    }
    pub(crate) async fn abort(
        &self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let (manifest, _): (ManifestRef, _) = self
            .value(Request::Abort {
                authority,
                txn,
                reason,
                error,
            })
            .await?;
        self.lookup_decision(txn, manifest)
            .await?
            .ok_or_else(unavailable)
    }
    pub(crate) async fn abandon_fenced(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let (manifest, _): (ManifestRef, _) =
            self.value(Request::Abandon { authority, txn }).await?;
        self.lookup_decision(txn, manifest)
            .await?
            .ok_or_else(unavailable)
    }
    pub(crate) async fn lookup_decision(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        self.proof
            .as_ref()
            .ok_or_else(invalid)?
            .decision(txn, manifest)
            .await
    }
    pub(crate) async fn settled_proof(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        self.proof
            .as_ref()
            .ok_or_else(invalid)?
            .settlement(txn, manifest)
            .await
    }
    pub(crate) async fn terminal_failure(
        &self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> Result<TerminalIdentity, ProtocolError> {
        let (mut terminal, owner): (TerminalIdentity, _) = self
            .value(Request::Failure {
                authority,
                request,
                error,
            })
            .await?;
        terminal.retain_owner(owner);
        Ok(terminal)
    }
    pub(crate) async fn inspect_transaction(
        &self,
        txn: TxnId,
    ) -> Result<Option<TransactionInspection>, ProtocolError> {
        let (mut value, owner): (Option<TransactionInspection>, _) =
            self.value(Request::Inspect { txn }).await?;
        if let Some(value) = &mut value {
            value.retain_owner(owner);
        }
        Ok(value)
    }
    pub(crate) async fn manifest_page(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> Result<ManifestInventoryPage, ProtocolError> {
        let (mut value, owner): (ManifestInventoryPage, _) = self
            .value(Request::Manifest {
                txn,
                manifest,
                after,
            })
            .await?;
        value.retain_owner(owner);
        Ok(value)
    }
}

impl StatusClient {
    pub(crate) async fn lookup(
        &self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let response = self
            .response(Request::Lookup { request_id, digest })
            .await?;
        response.reply.sections[0].clone().ok_or_else(invalid)
    }
    pub(crate) async fn fence(
        &self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        let (applied, owner): (Applied<StatusReply>, _) = self
            .value(Request::Fence {
                transition_id,
                expected,
                next,
            })
            .await?;
        let mut reply = applied.reply;
        reply.retain_owner(owner.clone());
        Ok(Applied::owned(applied.position, reply, owner))
    }
    pub(crate) async fn failure_settled_proof(
        &self,
        txn: TxnId,
        request: RequestBinding,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        let response = self
            .response(Request::FailureSettlement { txn, request })
            .await?;
        let context = response.metadata_context()?;
        let (statement, _): (SettlementStatement, _) = response.decoded(self.resources()).await?;
        VerifiedSettlement::from_failure_service(context, statement, txn, request)
    }
    pub(crate) async fn retire_terminal(
        &self,
        release: VerifiedPinRelease,
    ) -> Result<VerifiedTerminalRetirement, ProtocolError> {
        let statement = release.statement().clone();
        let response = self
            .response(Request::Retire {
                statement: statement.clone(),
            })
            .await?;
        let context = response.metadata_context()?;
        let (value, _): (RetirementCertificate, _) = response.decoded(self.resources()).await?;
        VerifiedTerminalRetirement::from_service(context, value, &statement)
    }
    pub(crate) async fn retirement(
        &self,
        request_id: Uuid,
        digest: Digest,
    ) -> Result<Option<VerifiedTerminalRetirement>, ProtocolError> {
        let request = Request::Retirement { request_id, digest };
        let method = request.method();
        let payload = encode(self.resources(), request).await?;
        let response = self
            .client
            .call_as(ServiceAction::QueueRetirement, method, Some(payload))
            .await?;
        let context = response.metadata_context()?;
        let (value, _): (Option<RetirementCertificate>, _) =
            response.decoded(self.resources()).await?;
        VerifiedTerminalRetirement::from_lookup_service(context, value, request_id, digest)
    }
    pub(crate) async fn rejected_identity(
        &self,
        expected: &RejectedIdentity,
    ) -> Result<VerifiedAdmissionRejection, ProtocolError> {
        let response = self
            .response(Request::Lookup {
                request_id: expected.reservation.request_id,
                digest: Some(expected.reservation.digest),
            })
            .await?;
        let context = response.metadata_context()?;
        let (value, _): (Option<IdentityLookup>, _) = response.decoded(self.resources()).await?;
        let Some(IdentityLookup::Rejected(actual)) = value else {
            return Err(unavailable());
        };
        if actual != *expected {
            return Err(invalid());
        }
        VerifiedAdmissionRejection::from_lookup_service(context, actual)
    }
}

fn owner_operation_uncertain(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}

impl StatusService for StatusClient {
    fn native_archive_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> ConsensusFuture<'a, MetadataArchivePage> {
        Box::pin(async move {
            let (mut page, owner): (MetadataArchivePage, _) = self
                .value(Request::ArchiveMetadata {
                    closure: closure.statement().clone(),
                    after,
                })
                .await?;
            if page.group != *self.identity() || page.cut != closure.cut() {
                return Err(invalid());
            }
            page.retain_owner(owner);
            Ok(page)
        })
    }

    fn archive_cut<'a>(
        &'a self,
        authority: Authority,
    ) -> ConsensusFuture<'a, VerifiedStatusArchiveCut> {
        Box::pin(async move { StatusClient::archive_cut(self, authority).await })
    }
    fn committed_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> ConsensusFuture<'a, CommittedArchivePage> {
        Box::pin(
            async move { StatusClient::committed_page(self, closure, base_exclusive, after).await },
        )
    }
    fn identity_history_page<'a>(
        &'a self,
        closure: &'a VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> ConsensusFuture<'a, IdentityArchivePage> {
        Box::pin(async move { StatusClient::identity_history_page(self, closure, after).await })
    }
    fn identity(&self) -> &GroupIdentity {
        StatusClient::identity(self)
    }
    fn resources(&self) -> &Arc<WorkerSharedResources> {
        StatusClient::resources(self)
    }
    fn reserve_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move {
            StatusClient::reserve_identity(self, authority, request_id, digest, kind).await
        })
    }
    fn reserve_scoped_identity<'a>(
        &'a self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        kind: RequestKind,
        scoped: ScopedAdmission,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move {
            self.reserve_admission(authority, request_id, digest, kind, Some(scoped))
                .await
        })
    }
    fn lookup<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, ChargedBytes> {
        Box::pin(async move { StatusClient::lookup(self, request_id, digest).await })
    }
    fn lookup_identity<'a>(
        &'a self,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> ConsensusFuture<'a, Option<IdentityLookup>> {
        Box::pin(async move { StatusClient::lookup_identity(self, request_id, digest).await })
    }
    fn lookup_scoped_reservation<'a>(
        &'a self,
        placement: &'a ScopedQueuePlacement,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, IdentityReservation> {
        Box::pin(async move {
            placement.validate(&placement.group)?;
            let status = self.identity();
            if placement.group.cluster_id != status.cluster_id
                || placement.group.database != status.database
                || !placement.members.contains(&self.client.transport.local_identity())
                || request_id.is_nil()
            {
                return Err(invalid());
            }
            let scoped = ScopedAdmission {
                protocol_version: ScopedAdmission::VERSION,
                queue_group_id: placement.group.group_id,
                queue_generation: placement.queue_generation,
                status_group_id: status.group_id,
            };
            let action = ServiceAction::ScopedQueueRead {
                queue_group_id: scoped.queue_group_id,
                queue_generation: scoped.queue_generation,
                queue_view: placement.view,
                request_id,
            };
            let request = Request::Lookup { request_id, digest: Some(digest) };
            let method = request.method();
            let payload = encode(self.resources(), request).await?;
            let (reservation, _owner): (IdentityReservation, _) = self.client
                .call_as(action, method, Some(payload))
                .await?
                .decoded(self.resources())
                .await?;
            if reservation.request_id != request_id
                || reservation.digest != digest
                || reservation.original_run != placement.view.run_id
                || reservation.scoped != Some(scoped)
            {
                return Err(unavailable());
            }
            Ok(reservation)
        })
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>> {
        Box::pin(async move { StatusClient::current_authority(self).await })
    }
    fn fence<'a>(
        &'a self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
    ) -> ConsensusFuture<'a, Applied<StatusReply>> {
        Box::pin(async move { StatusClient::fence(self, transition_id, expected, next).await })
    }
    fn reject_unaccepted<'a>(
        &'a self,
        cap: &'a crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        Box::pin(async move { StatusClient::reject_unaccepted(self, cap).await })
    }
    fn rejected_identity<'a>(
        &'a self,
        expected: &'a RejectedIdentity,
    ) -> ConsensusFuture<'a, VerifiedAdmissionRejection> {
        Box::pin(StatusClient::rejected_identity(self, expected))
    }
    fn retire_terminal<'a>(
        &'a self,
        release: VerifiedPinRelease,
    ) -> ConsensusFuture<'a, VerifiedTerminalRetirement> {
        Box::pin(StatusClient::retire_terminal(self, release))
    }
    fn retirement<'a>(
        &'a self,
        request_id: Uuid,
        digest: Digest,
    ) -> ConsensusFuture<'a, Option<VerifiedTerminalRetirement>> {
        Box::pin(StatusClient::retirement(self, request_id, digest))
    }
    fn try_install_point_manifest<'a>(
        &'a self,
        point: PointManifest,
    ) -> ConsensusFuture<'a, Option<(ManifestRef, Digest)>> {
        Box::pin(async move { self.try_point_manifest(point).await })
    }
    fn begin_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        result: CanonicalJson,
    ) -> ConsensusFuture<'a, ManifestDraft> {
        Box::pin(async move {
            StatusClient::begin_manifest(
                self,
                authority,
                txn,
                request,
                read_ts,
                participant_count,
                result,
            )
            .await
        })
    }
    fn append_manifest_page<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> ConsensusFuture<'a, ManifestDraft> {
        Box::pin(async move {
            StatusClient::append_manifest_page(
                self,
                authority,
                txn,
                page_index,
                previous_root,
                participants,
            )
            .await
        })
    }
    fn seal_manifest<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, ManifestRef> {
        Box::pin(async move { StatusClient::seal_manifest(self, authority, txn).await })
    }
    fn record_participant_prepared<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            self.record_prepared(authority, participant.identity().clone(), txn, manifest)
                .await
        })
    }
    fn record_participant_resolved<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            self.record_resolved(authority, participant.identity().clone(), txn, manifest)
                .await
        })
    }
    fn prepare_point_permit<'a>(
        &'a self,
        authority: Authority,
        participant: &'a ParticipantHandle,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Result<SealedCommitPermit, ProtocolError>> {
        Box::pin(async move {
            StatusClient::prepare_point_permit(
                self, authority, participant.identity().clone(), txn, manifest,
            )
            .await
        })
    }
    fn commit_permit<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, SealedCommitPermit> {
        Box::pin(async move { StatusClient::commit_permit(self, authority, txn).await })
    }
    fn decide_commit<'a>(
        &'a self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusClient::decide_commit(self, authority, permit, stamp).await })
    }
    fn abort<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusClient::abort(self, authority, txn, reason, error).await })
    }
    fn lookup_decision<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, Option<VerifiedStatusDecision>> {
        Box::pin(async move { StatusClient::lookup_decision(self, txn, manifest).await })
    }
    fn settled_proof<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        Box::pin(async move { StatusClient::settled_proof(self, txn, manifest).await })
    }
    fn terminal_failure<'a>(
        &'a self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> ConsensusFuture<'a, TerminalIdentity> {
        Box::pin(
            async move { StatusClient::terminal_failure(self, authority, request, error).await },
        )
    }
    fn manifest_page<'a>(
        &'a self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> ConsensusFuture<'a, ManifestInventoryPage> {
        Box::pin(async move { StatusClient::manifest_page(self, txn, manifest, after).await })
    }
    fn failure_settled_proof<'a>(
        &'a self,
        txn: TxnId,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, VerifiedSettlement> {
        Box::pin(async move { StatusClient::failure_settled_proof(self, txn, request).await })
    }
    fn inspect_transaction<'a>(
        &'a self,
        txn: TxnId,
    ) -> ConsensusFuture<'a, Option<TransactionInspection>> {
        Box::pin(async move { StatusClient::inspect_transaction(self, txn).await })
    }
    fn abandon_fenced<'a>(
        &'a self,
        authority: Authority,
        txn: TxnId,
    ) -> ConsensusFuture<'a, VerifiedStatusDecision> {
        Box::pin(async move { StatusClient::abandon_fenced(self, authority, txn).await })
    }
}

#[cfg(test)]
mod point_manifest_tests {
    use super::*;
    use crate::{
        config::{ExecutionConfig, WorkerResources},
        oltp_runtime::{bootstrap::LocalServices, worker_endpoint::AuthorityReader},
    };
    use std::sync::atomic::AtomicBool;

    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct StaleReadHint {
        endpoint: Arc<StatusEndpoint>,
        block_next_probe: AtomicBool,
        blocked_probe: AtomicBool,
        release_probe: tokio::sync::Notify,
        block_next_read: AtomicBool,
        blocked_read: AtomicBool,
        release_read: tokio::sync::Notify,
    }

    impl Endpoint for StaleReadHint {
        fn preflight<'a>(
            &'a self,
            session: &'a AuthenticatedSession,
            header: &'a [u8],
        ) -> EndpointFuture<'a, ReceivePlan> {
            self.endpoint.preflight(session, header)
        }

        fn handle<'a>(
            &'a self,
            session: AuthenticatedSession,
            header: ChargedBytes,
            sections: ChargedSections,
        ) -> EndpointFuture<'a, ChargedReply> {
            Box::pin(async move {
                let head: Header = serde_json::from_slice(header.as_slice()).unwrap();
                if head.method == Request::Current.method() {
                    if head.placement_generation.is_some()
                        && self.block_next_probe.swap(false, Ordering::Relaxed)
                    {
                        self.blocked_probe.store(true, Ordering::Relaxed);
                        self.release_probe.notified().await;
                    } else if head.placement_generation.is_none()
                        && self.blocked_probe.load(Ordering::Relaxed)
                    {
                        self.release_probe.notify_one();
                    }
                }
                if head.placement_generation.is_some()
                    && head.method.can_repeat_read()
                    && self.block_next_read.swap(false, Ordering::Relaxed)
                {
                    self.blocked_read.store(true, Ordering::Relaxed);
                    self.release_read.notified().await;
                } else if head.placement_generation.is_none()
                    && self.blocked_read.load(Ordering::Relaxed)
                {
                    self.release_read.notify_one();
                }
                self.endpoint.handle(session, header, sections).await
            })
        }
    }

    /// O07/O10/O11. The direct Status RPC replays an old draft at every
    /// prefix and validates the original page before returning a sealed ref.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn o07_o10_o11_point_manifest_replays_legacy_prefixes_and_rejects_changed_plan() {
        tokio::time::timeout(Duration::from_secs(40), async {
            let directory =
                Directory(std::env::temp_dir().join(format!("point-status-{}", Uuid::new_v4())));
            let services = LocalServices::open(
                &directory.0,
                "point_manifest",
                WorkerResources {
                    max_threads: 1,
                    ..Default::default()
                },
                &ExecutionConfig::default(),
                false,
            )
            .await
            .unwrap();
            let (node, endpoint) = services.status_endpoint_for_test().await.unwrap();
            let endpoint = Arc::new(StaleReadHint {
                endpoint,
                block_next_probe: AtomicBool::new(true),
                blocked_probe: AtomicBool::new(false),
                release_probe: tokio::sync::Notify::new(),
                block_next_read: AtomicBool::new(false),
                blocked_read: AtomicBool::new(false),
                release_read: tokio::sync::Notify::new(),
            });
            services
                .manager
                .replace_service_for_test(services.status.identity(), node, endpoint.clone())
                .await
                .unwrap();
            let authority = services
                .control
                .current(&services.scope)
                .await
                .unwrap()
                .ready
                .view
                .authority();
            let client = StatusClient::new(
                services.status.identity().clone(),
                services.shared.clone(),
                services.manager.client(),
                services.control_client.clone(),
                ServiceAction::Owner,
            )
            .unwrap();
            let routes = services
                .control_client
                .replica_routes(client.identity())
                .await
                .unwrap();
            // O06/O14: a stalled probe must give way to the ordinary read.
            // The probe can finish only once that unmarked read reaches Status.
            assert_eq!(client.current_authority().await.unwrap(), Some(authority));
            assert!(endpoint.blocked_probe.load(Ordering::Relaxed));
            assert_eq!(routes.hinted_receiver(node), None);
            services.control_client.invalidate_replica_routes(&routes);
            let routes = services
                .control_client
                .replica_routes(client.identity())
                .await
                .unwrap();
            assert_eq!(client.current_authority().await.unwrap(), Some(authority));
            assert_eq!(routes.hinted_receiver(node), Some(services.local));
            // O06/O14: an unavailable hinted receiver must trigger a fresh
            // routing pass within the same read, even with unchanged membership.
            endpoint.block_next_read.store(true, Ordering::Relaxed);
            assert!(
                client
                    .inspect_transaction(TxnId {
                        request_id: Uuid::new_v4(),
                        attempt: 1,
                    })
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(endpoint.blocked_read.load(Ordering::Relaxed));
            let read_ts = ReadTs::new(1).unwrap();
            let plan = ParticipantPlan {
                group: GroupIdentity {
                    cluster_id: client.identity().cluster_id,
                    database: client.identity().database.clone(),
                    group_id: Uuid::new_v4(),
                    kind: GroupKind::Data {
                        table_id: Uuid::new_v4(),
                        shard_id: Uuid::new_v4(),
                    },
                    group_format_version: client.identity().group_format_version,
                },
                schema_epoch: 1,
                routing_epoch: 1,
                read_ts,
                write_root: Digest::from_bytes([42; 32]),
                write_count: 1,
                page_count: 1,
            };
            let result = CanonicalJson::from_serializable(&services.shared, &1_u64, 1024).unwrap();
            for prefix in 0..=3 {
                let request_id = Uuid::new_v4();
                let digest = Digest::from_bytes([45; 32]);
                client
                    .reserve_identity(authority, request_id, digest, RequestKind::Mutation)
                    .await
                    .unwrap();
                let txn = TxnId {
                    request_id,
                    attempt: 1,
                };
                let request = RequestBinding {
                    run_id: authority.run_id,
                    request_id,
                    digest,
                    accepted_context: Digest::from_bytes([46; 32]),
                };
                let point = |plan: ParticipantPlan| PointManifest {
                    authority,
                    txn,
                    request,
                    read_ts,
                    plan,
                    result: result.clone(),
                };

                let mut previous_manifest = None;
                if prefix >= 1 {
                    let draft = client
                        .begin_manifest(authority, txn, request, read_ts, 1, result.clone())
                        .await
                        .unwrap();
                    if prefix >= 2 {
                        client
                            .append_manifest_page(
                                authority,
                                txn,
                                0,
                                draft.initial_root,
                                Page::new(vec![plan.clone()]).unwrap(),
                            )
                            .await
                            .unwrap();
                    }
                    if prefix == 3 {
                        previous_manifest =
                            Some(client.seal_manifest(authority, txn).await.unwrap());
                    }
                }

                let installed = client
                    .try_install_point_manifest(point(plan.clone()))
                    .await
                    .unwrap()
                    .expect("Status replica supports the combined request");
                let (manifest, result_digest) = installed;
                assert!(previous_manifest.is_none_or(|old| old == manifest));
                assert_eq!(
                    result_digest,
                    MutationOutcome::from_success(&result)
                        .unwrap()
                        .digest()
                        .unwrap()
                );
                assert_eq!(
                    client
                        .try_install_point_manifest(point(plan.clone()))
                        .await
                        .unwrap(),
                    Some(installed),
                );

                let mut changed = plan.clone();
                changed.write_root = Digest::from_bytes([43; 32]);
                let error = client
                    .try_install_point_manifest(point(changed))
                    .await
                    .unwrap_err();
                assert_eq!(error.outcome, Outcome::Unknown);
                let page = client.manifest_page(txn, manifest, None).await.unwrap();
                assert_eq!(&*page.items, std::slice::from_ref(&plan));
                assert!(page.done);
            }

            // The legacy decoder's unknown-variant refusal is an
            // authenticated embedded error, distinct from a bad reply.
            let routes = services
                .control_client
                .replica_routes(client.identity())
                .await
                .unwrap();
            let member = &routes.members[0];
            let unsupported = services
                .shared
                .copy_bytes_in(ByteClass::Progress, br#"{"UnsupportedPointManifest":{}}"#)
                .unwrap();
            assert!(matches!(
                client.client.call_member(
                    ServiceAction::Owner,
                    Method::StatusCall { operation: INSTALL_POINT_MANIFEST, mutation: true },
                    &Some(unsupported),
                    member.node_id,
                    member.worker_id.into(),
                ).await,
                Ok(Err(error)) if error == invalid(),
            ));
            services.shutdown().await.unwrap();
        })
        .await
        .expect("Point manifest service watchdog");
    }
}
