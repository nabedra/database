use super::*;
use crate::oltp::mvcc::store::{VerifiedParticipantResolution, VerifiedParticipantSeal};
use crate::oltp_replication::authority::status::{CommitPermitStatement, StatusTopology};
use crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage;
use crate::oltp_replication::authority::status::identity::transaction::ColdRecoveryPage;
use crate::oltp_replication::authority::timestamp::cold::ColdApplicationPin;
use crate::oltp_replication::authority::{
    status::{
        ArchiveCursor, CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage,
        StatusArchiveCutStatement, VerifiedStatusArchiveCut,
    },
    timestamp::{VerifiedArchiveClosure, archive::ArchiveClosureStatement},
};
use uuid::Uuid;

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
    },
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
            Self::Begin { txn, .. }
            | Self::Page { txn, .. }
            | Self::Seal { txn, .. }
            | Self::Prepared { txn, .. }
            | Self::Resolved { txn, .. }
            | Self::Commit { txn, .. }
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
            Self::Current | Self::Fence { .. } | Self::ArchiveCut { .. }
            | Self::ArchiveDecisions { .. } | Self::ArchiveIdentities { .. }
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
            Self::Begin { .. } => 3,
            Self::Page { .. } => 4,
            Self::Seal { .. } => 5,
            Self::Prepared { .. } => 6,
            Self::Resolved { .. } => 7,
            Self::Commit { .. } => 8,
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
                operation: 15,
                mutation: true,
            } => matches!(
                header.action,
                ServiceAction::ControlFence { .. } | ServiceAction::Operator
            ),
            Method::StatusCall {
                operation: 0 | 1 | 12 | 16 | 19,
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
        if !allowed {
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
        if request.request_id().is_some_and(|id| self.topology.request(id) != handle.identity()) {
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
                let value = handle.lookup_identity(request_id, digest).await?;
                encode(shared, value).await
            }
            Request::Reserve {
                authority,
                request_id,
                digest,
                kind,
            } => {
                encode(
                    shared,
                    handle
                        .reserve_identity(authority, request_id, digest, kind)
                        .await?,
                )
                .await
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
            } => {
                let actual = handle.commit_permit(authority, txn).await?;
                if actual != permit {
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
                let allocated = client.confirmed_allocation(actual.clone()).await?;
                if allocated != stamp {
                    return Err(invalid());
                }
                let decision = handle.decide_commit(authority, actual, allocated).await?;
                encode(shared, decision.statement().manifest).await
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
                    .service_role(&scope, session.remote(), header.action, handle.identity().clone())
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
                encode(shared, proof.map(VerifiedTerminalRetirement::into_certificate)).await
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
                if let Request::Begin { result, .. } = &request {
                    result.attach_charge(owner.clone())?;
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
        let (settled, _) = self.value(Request::ColdApplicationPin { authority, pin }).await?;
        Ok(settled)
    }

    pub(crate) async fn cold_recovery_page(
        &self,
        authority: Authority,
        after: Option<TxnId>,
    ) -> Result<ColdRecoveryPage, ProtocolError> {
        let (page, _): (ColdRecoveryPage, _) =
            self.value(Request::ColdRecovery { authority, after }).await?;
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
        let (applied, owner): (Applied<StatusReply>, _) = self
            .value(Request::Reserve {
                authority,
                request_id,
                digest,
                kind,
            })
            .await?;
        {
            let mut reply = applied.reply;
            reply.retain_owner(owner.clone());
            Ok(Applied::owned(applied.position, reply, owner))
        }
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
    pub(crate) async fn observe_prepared(
        &self,
        authority: Authority,
        proof: VerifiedParticipantSeal,
    ) -> Result<(), ProtocolError> {
        let evidence = proof.evidence();
        self.value::<()>(Request::Prepared {
            authority,
            group: evidence.participant.group.clone(),
            txn: evidence.txn,
            manifest: evidence.manifest,
        })
        .await?;
        Ok(())
    }
    pub(crate) async fn observe_resolved(
        &self,
        authority: Authority,
        proof: VerifiedParticipantResolution,
    ) -> Result<(), ProtocolError> {
        let evidence = proof.evidence();
        self.value::<()>(Request::Resolved {
            authority,
            group: evidence.group.clone(),
            txn: evidence.txn,
            manifest: evidence.manifest,
        })
        .await?;
        Ok(())
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
            .response(Request::Retire { statement: statement.clone() })
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
        let response = self.client
            .call_as(ServiceAction::QueueRetirement, method, Some(payload))
            .await?;
        let context = response.metadata_context()?;
        let (value, _): (Option<RetirementCertificate>, _) = response.decoded(self.resources()).await?;
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
    fn observe_prepared<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantSeal,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { StatusClient::observe_prepared(self, authority, proof).await })
    }
    fn observe_resolved<'a>(
        &'a self,
        authority: Authority,
        proof: VerifiedParticipantResolution,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { StatusClient::observe_resolved(self, authority, proof).await })
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
