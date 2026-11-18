//! Strong Status reads through registered replica routes. The destination
//! proves local application with ReadIndex; transport enrollment alone never
//! creates a storage-resolution capability.
use super::*;
use crate::oltp_replication::authority::status::{
    MutationOutcome, SettlementStatement, StatusHandle, StatusTopology, VerifiedSettlement, VerifiedStatusDecision,
};

pub(crate) struct StatusProofEndpoint {
    pub(super) handle: Arc<StatusHandle>,
    pub(super) node: u64,
    pub(super) control: Arc<ControlClient>,
    pub(super) shared: Arc<WorkerSharedResources>,
    topology: Option<StatusTopology>,
}
impl StatusProofEndpoint {
    pub(crate) fn new(
        handle: Arc<StatusHandle>,
        node: u64,
        control: Arc<ControlClient>,
        shared: Arc<WorkerSharedResources>,
        topology: Option<StatusTopology>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if node == 0 || !Arc::ptr_eq(handle.resources(), &shared) {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            handle,
            node,
            control,
            shared,
            topology,
        }))
    }
}
impl Endpoint for StatusProofEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let header = parse(session, self.handle.identity(), self.node, bytes)?;
            let (txn, manifest) = match header.method {
                Method::StatusDecision { txn, manifest }
                | Method::StatusSettlement { txn, manifest }
                | Method::StatusParticipant { txn, manifest, .. } => (txn, manifest),
                _ => return Err(invalid()),
            };
            if manifest.status_group_id != self.handle.identity().group_id
                || self.topology.as_ref().is_some_and(|topology| {
                    topology.request(txn.request_id) != self.handle.identity()
                })
            {
                return Err(invalid());
            }
            if !matches!(
                header.action,
                ServiceAction::Owner
                    | ServiceAction::ParticipantProof { .. }
                    | ServiceAction::TimestampProof { .. }
            ) {
                return Err(invalid());
            }
            authorize(&self.control, session, &header).await?;
            Ok(ReceivePlan {
                class: TrafficClass::ServiceAuthority,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        _sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let header = parse(
                &session,
                self.handle.identity(),
                self.node,
                bytes.as_slice(),
            )?;
            // Fresh role authorization ran before taking this lane's handler
            // permit. Do not recursively consume the same lane for Control.
            let result = async {
                match header.method {
                    Method::StatusDecision { txn, manifest } => {
                        let decision = self.handle.lookup_decision(txn, manifest).await?;
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                let value = decision
                                    .as_ref()
                                    .map(|decision| -> Result<_, ProtocolError> {
                                        Ok((
                                            decision.statement(),
                                            decision.outcome().ok_or_else(invalid)?,
                                        ))
                                    })
                                    .transpose()?;
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    Method::StatusSettlement { txn, manifest } => {
                        let proof = self.handle.settled_proof(txn, manifest).await?;
                        let shared = self.shared.clone();
                        self.shared
                            .execute(ExecutionClass::Control, self.node, 0, move || {
                                shared.encode_in(
                                    ByteClass::Progress,
                                    proof.into_sealed().statement(),
                                    128 << 10,
                                )
                            })
                            .await
                    }
                    Method::StatusParticipant {
                        txn,
                        manifest,
                        group_id,
                    } => {
                        self.handle
                            .participant_plan_bytes(txn, manifest, group_id)
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
pub(crate) struct StatusProofClient {
    client: Arc<Client>,
}
impl StatusProofClient {
    pub(crate) async fn participates(
        &self,
        group: &GroupIdentity,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<bool, ProtocolError> {
        if manifest.status_group_id != self.client.group.group_id {
            return Err(invalid());
        }
        let response = self
            .client
            .read(Method::StatusParticipant {
                txn,
                manifest,
                group_id: group.group_id,
            })
            .await?;
        let expected = group.clone();
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let plan: Option<ParticipantPlan> =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                Ok(plan.is_some_and(|plan| plan.group == expected))
            })
            .await
    }
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !matches!(group.kind, GroupKind::Status { .. })
            || !matches!(
                action,
                ServiceAction::Owner
                    | ServiceAction::ParticipantProof { .. }
                    | ServiceAction::TimestampProof { .. }
                    | ServiceAction::ControlFence { .. }
                    | ServiceAction::Operator
            )
        {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            client: Arc::new(Client::new(group, shared, transport, control, action)?),
        }))
    }
    pub(crate) async fn decision(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        if manifest.status_group_id != self.client.group.group_id {
            return Err(invalid());
        }
        let response = self
            .client
            .read(Method::StatusDecision { txn, manifest })
            .await?;
        let context = response.status_context();
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let decision: Option<(DecisionStatement, MutationOutcome)> =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                match decision {
                    Some((statement, outcome)) => VerifiedStatusDecision::from_authenticated(
                        context, statement, outcome, txn, manifest,
                    )
                    .map(Some),
                    None => {
                        context.decision(
                            txn,
                            manifest,
                            &None::<(DecisionStatement, MutationOutcome)>,
                        )?;
                        Ok(None)
                    }
                }
            })
            .await
    }
    pub(crate) async fn settlement(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        if manifest.status_group_id != self.client.group.group_id {
            return Err(invalid());
        }
        let response = self
            .client
            .read(Method::StatusSettlement { txn, manifest })
            .await?;
        let context = response.status_context();
        self.client
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                let statement: SettlementStatement =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                VerifiedSettlement::from_authenticated(context, statement, txn, manifest)
            })
            .await
    }
}
