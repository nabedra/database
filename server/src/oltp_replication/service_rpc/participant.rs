//! Participant proof reads execute on the actual group leader. A Status/TSO
//! replica can request only the proof methods authorized by current Control.
use super::*;

#[path = "participant_timestamp_point.rs"]
pub(super) mod timestamp_point;
pub(crate) use timestamp_point::{
    VerifiedTimestampPointRejection, VerifiedTimestampPointResolution, VerifiedTimestampPointSeal,
};
use crate::oltp::mvcc::store::{
    ParticipantHandle, ResolutionEvidence, VerifiedParticipantResolution, VerifiedParticipantSeal,
    index::{IndexClosureReply, VerifiedIndexAttemptClosed},
    lifecycle::{LifecycleBinding, SealedLifecycleGuard, VerifiedLifecycleGuard},
};

pub(crate) struct ParticipantProofEndpoint {
    pub(super) handle: Arc<ParticipantHandle>,
    pub(super) node: u64,
    pub(super) control: Arc<ControlClient>,
    pub(super) shared: Arc<WorkerSharedResources>,
}
impl ParticipantProofEndpoint {
    pub(crate) fn new(
        handle: Arc<ParticipantHandle>,
        node: u64,
        control: Arc<ControlClient>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if node == 0
            || !matches!(
                handle.identity().kind,
                GroupKind::Data { .. } | GroupKind::Catalog
            )
        {
            return Err(invalid());
        }
        let shared = handle.resources().clone();
        Ok(Arc::new(Self {
            handle,
            node,
            control,
            shared,
        }))
    }
    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        let header = parse(session, self.handle.identity(), self.node, bytes)?;
        let permitted = match header.method {
            Method::ParticipantSeal { txn, manifest }
            | Method::ParticipantResolution { txn, manifest } => {
                matches!(header.action, ServiceAction::Owner)
                    || matches!(header.action,ServiceAction::StatusProof{source_group} if source_group==manifest.status_group_id)
                    || matches!(header.action, ServiceAction::ScopedQueueExecute(scope)
                        if scope.queue_group_id == self.handle.identity().group_id
                            && txn.request_id == scope.request.request_id && txn.attempt == 1)
            }
            Method::ParticipantTimestampPointSeal { .. } | Method::ParticipantTimestampPointRejection { .. } => {
                matches!(self.handle.identity().kind, GroupKind::Data { .. })
                    && matches!(header.action, ServiceAction::TimestampProof { .. })
            }
            Method::ParticipantTimestampPointResolution { .. } => {
                matches!(self.handle.identity().kind, GroupKind::Data { .. })
                    && matches!(header.action, ServiceAction::StatusProof { .. })
            }
            Method::ParticipantGuard => {
                self.handle.identity().kind == GroupKind::Catalog
                    && matches!(
                        header.action,
                        ServiceAction::Owner | ServiceAction::ParticipantProof { .. }
                    )
            }
            Method::ParticipantIndexClosed { .. } => {
                self.handle.identity().kind == GroupKind::Catalog
                    && matches!(
                        header.action,
                        ServiceAction::Owner | ServiceAction::TimestampProof { .. }
                    )
            }
            _ => false,
        };
        if !permitted {
            return Err(invalid());
        }
        Ok(header)
    }
}
impl Endpoint for ParticipantProofEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let header = self.parse(session, bytes)?;
            authorize(&self.control, session, &header).await?;
            Ok(ReceivePlan {
                class: TrafficClass::ServiceAuthority,
                section_lengths: [header.payload_bytes, 0, 0],
                decode_budget_bytes: match header.method {
                    Method::ParticipantTimestampPointSeal { .. }
                    | Method::ParticipantTimestampPointRejection { .. }
                    | Method::ParticipantTimestampPointResolution { .. } => timestamp_point::DECODE_BYTES,
                    _ => 0,
                },
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
            let header = self.parse(&session, bytes.as_slice())?;
            let result = async {
                match header.method {
                    Method::ParticipantSeal { txn, manifest } => {
                        let proof = self.handle.strong_seal(txn, manifest).await?;
                        if let ServiceAction::ScopedQueueExecute(scope) = header.action
                            && proof.evidence().participant.read_ts != scope.read_ts
                        {
                            return Err(invalid());
                        }
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, proof.evidence(), 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantResolution { txn, manifest } => {
                        let proof = self.handle.strong_resolution(txn, manifest).await?;
                        if let ServiceAction::ScopedQueueExecute(scope) = header.action
                            && proof.evidence().request != scope.request
                        {
                            return Err(invalid());
                        }
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, proof.evidence(), 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantTimestampPointSeal { .. }
                    | Method::ParticipantTimestampPointRejection { .. }
                    | Method::ParticipantTimestampPointResolution { .. } => {
                        self.read_point_evidence(&session, &header, sections).await
                    }
                    Method::ParticipantGuard => {
                        let shared = self.shared.clone();
                        let binding: LifecycleBinding = self
                            .shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                let raw = sections.section(0).ok_or_else(invalid)?;
                                let _owner = shared.reserve_scratch(
                                    ByteClass::Progress,
                                    crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
                                )?;
                                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())
                            })
                            .await?;
                        let guard = self
                            .handle
                            .lifecycle_guard(&binding)
                            .await?
                            .map(VerifiedLifecycleGuard::into_sealed);
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &guard, 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantIndexClosed { owner } => {
                        let proof = self.handle.strong_index_attempt_closed(owner).await?;
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, proof.reply(), 128 << 10)
                            })
                            .await
                    }
                    _ => Err(invalid()),
                }
            }
            .await;
            reply(&self.shared, &session, self.node, result).await
        })
    }
}

#[derive(Clone)]
pub(crate) struct ParticipantProofClient {
    pub(super) client: Arc<Client>,
}
impl ParticipantProofClient {
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !matches!(group.kind, GroupKind::Data { .. } | GroupKind::Catalog) {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            client: Arc::new(Client::new(group, shared, transport, control, action)?),
        }))
    }
    pub(crate) async fn seal(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantSeal, ProtocolError> {
        let response = self
            .client
            .read(Method::ParticipantSeal { txn, manifest })
            .await?;
        let context = response.participant_context(ParticipantProofKind::Seal)?;
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let evidence: PreparedParticipant =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                VerifiedParticipantSeal::from_authenticated(context, evidence, txn, manifest)
            })
            .await
    }
    pub(crate) async fn resolution(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantResolution, ProtocolError> {
        let response = self
            .client
            .read(Method::ParticipantResolution { txn, manifest })
            .await?;
        let context = response.participant_context(ParticipantProofKind::Resolution)?;
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let evidence: ResolutionEvidence =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                VerifiedParticipantResolution::from_authenticated(context, evidence, txn, manifest)
            })
            .await
    }
    pub(crate) async fn lifecycle(
        &self,
        binding: &LifecycleBinding,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        self.lifecycle_for(binding, false).await
    }
    pub(super) async fn lifecycle_for(
        &self,
        binding: &LifecycleBinding,
        scoped: bool,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        let payload = self
            .client
            .shared
            .encode_in(ByteClass::Progress, binding, 4096)?;
        let method = Method::ParticipantGuard;
        let response = if scoped {
            self.client.repeat_as(self.client.action, method, Some(payload)).await?
        } else {
            self.client.call(method, Some(payload)).await?
        };
        let context = response.participant_context(ParticipantProofKind::Lifecycle)?;
        let binding = binding.clone();
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let guard: Option<SealedLifecycleGuard> =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                guard
                    .map(|guard| {
                        VerifiedLifecycleGuard::from_authenticated(context, guard, &binding)
                    })
                    .transpose()
            })
            .await
    }
    pub(crate) async fn index_closed(
        &self,
        owner: MaintenanceOwnerRef,
    ) -> Result<VerifiedIndexAttemptClosed, ProtocolError> {
        let response = self
            .client
            .read(Method::ParticipantIndexClosed { owner })
            .await?;
        let context = response.participant_context(ParticipantProofKind::IndexClosed)?;
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let reply: IndexClosureReply =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                VerifiedIndexAttemptClosed::from_authenticated(context, reply, owner)
            })
            .await
    }
}
