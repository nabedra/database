//! Strong Status reads through registered replica routes. The destination
//! proves local application with ReadIndex; transport enrollment alone never
//! creates a storage-resolution capability.
use super::*;
use crate::oltp_replication::authority::{
    control::{ReplicaRoutes, ScopedCatalogRead, TimestampPointRoute},
    status::identity::transaction::timestamp_point::{
        TIMESTAMP_POINT_LOCATOR_REPLY_LIMIT, valid_outcome,
    },
};
use crate::oltp_replication::authority::status::{
    IdentityLookup, LocatedTimestampManifest, MutationOutcome, ScopedAdmission, SettlementStatement, StatusHandle,
    TimestampDecisionLocator,
    StatusTopology, TerminalIdentity, VerifiedSettlement, VerifiedStatusDecision,
};

pub(crate) struct VerifiedTimestampPointLocator {
    proof: LocatedTimestampManifest,
    _owner: Arc<ScratchLease>,
}

impl VerifiedTimestampPointLocator {
    pub(crate) fn locator(&self) -> &TimestampDecisionLocator {
        &self.proof.locator
    }

    pub(crate) fn outcome(&self) -> &MutationOutcome {
        &self.proof.outcome
    }
}

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
    async fn verify_scoped_outcome(&self, decision: &VerifiedStatusDecision) -> Result<(), ProtocolError> {
        let statement = decision.statement();
        let exact = self.handle.lookup_identity(statement.txn.request_id, Some(statement.request.digest)).await?;
        match exact {
            Some(IdentityLookup::Terminal(TerminalIdentity::Committed { decision: stored, outcome, .. }))
                if stored == *statement && Some(&outcome) == decision.outcome() => Ok(()),
            Some(IdentityLookup::Terminal(TerminalIdentity::Failed { request, outcome, recorded_at, .. }))
                if request == statement.request && recorded_at == statement.decided_at
                    && statement.manifest.participant_count == 0
                    && Some(&outcome) == decision.outcome() => Ok(()),
            _ => Err(unavailable()),
        }
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
            if let Method::StatusPointLocator {
                route,
                txn,
                request_digest,
                manifest,
                scope,
            } = header.method
            {
                if manifest.status_group_id != self.handle.identity().group_id
                    || txn.attempt != 1
                    || self.topology.as_ref().is_some_and(|topology| {
                        topology.request(txn.request_id) != self.handle.identity()
                    })
                {
                    return Err(invalid());
                }
                match (header.action, scope) {
                    (ServiceAction::TimestampProof { source_group }, None)
                        if source_group == route.source_group => {}
                    (ServiceAction::ParticipantProof { source_group }, Some(scope))
                        if source_group == route.source_group
                            && source_group == scope.queue_group_id
                            && scope.data_generation == route.source_generation
                            && scope.request.run_id == route.authority.run_id
                            && scope.request.request_id == txn.request_id
                            && scope.request.digest == request_digest => {}
                    _ => return Err(invalid()),
                }
                self.control
                    .verify_timestamp_point_route(session, self.node, route)
                    .await?;
                if let Some(scope) = scope {
                    let placement = self
                        .control
                        .scoped_queue_by_id(scope.queue_group_id)
                        .await?
                        .ok_or_else(stale_service_route)?;
                    if placement.data_generation != scope.data_generation
                        || placement.queue_generation != scope.queue_generation
                        || placement.view != scope.queue_view
                        || !matches!(placement.group.kind, GroupKind::Data { table_id, .. }
                            if table_id == scope.table_id)
                    {
                        return Err(stale_service_route());
                    }
                }
                return Ok(ReceivePlan {
                    class: TrafficClass::ServiceAuthority,
                    section_lengths: [0; 3],
                    decode_budget_bytes: 0,
                });
            }
            let (txn, manifest) = match header.method {
                Method::StatusDecision { txn, manifest }
                | Method::StatusSettlement { txn, manifest }
                | Method::StatusParticipant { txn, manifest, .. }
                | Method::ScopedStatusParticipant { txn, manifest, .. } => (txn, manifest),
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
                    | ServiceAction::ScopedQueueExecute(_)
                    | ServiceAction::ScopedQueueRead { .. }
            ) {
                return Err(invalid());
            }
            if let ServiceAction::ScopedQueueRead { request_id, .. } = header.action {
                if request_id != txn.request_id
                    || txn.attempt != 1
                    || !matches!(
                        header.method,
                        Method::StatusDecision { .. } | Method::StatusSettlement { .. }
                    )
                {
                    return Err(invalid());
                }
            }
            if let Method::ScopedStatusParticipant { scope, .. } = header.method {
                if !matches!(header.action, ServiceAction::ParticipantProof { source_group }
                    if source_group == scope.queue_group_id)
                {
                    return Err(invalid());
                }
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
                if let ServiceAction::ScopedQueueExecute(scope) = header.action {
                    let txn = match header.method {
                        Method::StatusDecision { txn, .. } | Method::StatusSettlement { txn, .. } => txn,
                        _ => return Err(invalid()),
                    };
                    if txn.request_id != scope.request.request_id || txn.attempt != 1 {
                        return Err(invalid());
                    }
                    self.handle.verify_scoped_admission(scope).await?;
                    if !self.handle.scoped_transaction_matches(scope, txn).await? {
                        if !matches!(self.handle.lookup_identity(txn.request_id, Some(scope.request.digest)).await?,
                            Some(IdentityLookup::Terminal(
                                TerminalIdentity::Failed { request, .. }
                            )) if request == scope.request)
                        {
                            return Err(unavailable());
                        }
                    }
                } else if let ServiceAction::ScopedQueueRead {
                    queue_group_id, queue_generation, queue_view, request_id,
                } = header.action {
                    let reservation = self.handle.admission_reservation(request_id).await?.ok_or_else(unavailable)?;
                    if reservation.original_run != queue_view.run_id
                        || reservation.scoped != Some(ScopedAdmission {
                            protocol_version: ScopedAdmission::VERSION,
                            queue_group_id, queue_generation, status_group_id: self.handle.identity().group_id,
                        })
                    {
                        return Err(invalid());
                    }
                }
                match header.method {
                    Method::StatusPointLocator {
                        route,
                        txn,
                        request_digest,
                        manifest,
                        scope,
                    } => {
                        self.handle
                            .read_timestamp_point_manifest(route, txn, request_digest, manifest, scope)
                            .await
                    }
                    Method::StatusDecision { txn, manifest } => {
                        let decision = self.handle.lookup_decision(txn, manifest).await?;
                        if matches!(header.action, ServiceAction::ScopedQueueExecute(_) | ServiceAction::ScopedQueueRead { .. })
                            && let Some(decision) = &decision
                        {
                            self.verify_scoped_outcome(decision).await?;
                        }
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
                    Method::ScopedStatusParticipant { scope, txn, manifest } => {
                        if txn.request_id != scope.request.request_id || txn.attempt != 1 {
                            return Err(invalid());
                        }
                        self.handle.verify_scoped_admission(scope).await?;
                        if !self.handle.scoped_transaction_matches(scope, txn).await? {
                            return Err(unavailable());
                        }
                        self.handle.participant_plan_bytes(txn, manifest, scope.queue_group_id).await
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
    pub(crate) async fn timestamp_point_locator(
        &self,
        source: &ReplicaRoutes,
        locator: &TimestampDecisionLocator,
        authority: Authority,
    ) -> Result<VerifiedTimestampPointLocator, ProtocolError> {
        let (proof, owner) = self
            .read_point_manifest(source, locator, authority, None)
            .await?;
        Ok(VerifiedTimestampPointLocator { proof, _owner: owner })
    }

    pub(crate) async fn timestamp_point_manifest(
        &self,
        authority: Authority,
        locator: &TimestampDecisionLocator,
        scope: Option<ScopedCatalogRead>,
    ) -> Result<(LocatedTimestampManifest, Arc<ScratchLease>), ProtocolError> {
        let binding = &locator.binding;
        if scope.is_some_and(|scope| !binding.matches_scope(scope)) {
            return Err(invalid());
        }
        let source = self.point_source(locator)?;
        timeout_at(Instant::now() + SERVICE_CONVERGENCE, async {
            for attempt in 0..2 {
                let routes = self.client.control.timestamp_point_routes(source).await?;
                let result = self
                    .read_point_manifest(&routes, locator, authority, scope)
                    .await;
                if attempt == 0 && result.as_ref().err() == Some(&stale_service_route()) {
                    self.client.control.invalidate_replica_routes(&routes);
                    continue;
                }
                return result;
            }
            Err(unavailable())
        }).await.map_err(|_| unavailable())?
    }

    fn point_source<'a>(&self, locator: &'a TimestampDecisionLocator) -> Result<&'a GroupIdentity, ProtocolError> {
        let binding = &locator.binding;
        match self.client.action {
            ServiceAction::TimestampProof { source_group }
                if source_group == binding.timestamps_group.group_id => Ok(&binding.timestamps_group),
            ServiceAction::ParticipantProof { source_group }
                if source_group == binding.participant.group.group_id => Ok(&binding.participant.group),
            _ => Err(invalid()),
        }
    }

    async fn read_point_manifest(
        &self,
        source: &ReplicaRoutes,
        locator: &TimestampDecisionLocator,
        authority: Authority,
        scope: Option<ScopedCatalogRead>,
    ) -> Result<(LocatedTimestampManifest, Arc<ScratchLease>), ProtocolError> {
        locator.validate()?;
        let binding = &locator.binding;
        if self.client.group != binding.status_group
            || &source.record.identity != self.point_source(locator)?
            || authority.validate().is_err()
        {
            return Err(invalid());
        }
        if scope.is_some_and(|scope| scope.data_generation != source.record.generation) {
            return Err(stale_service_route());
        }
        let target = self.client.control.timestamp_point_routes(&self.client.group).await?;
        let method = Method::StatusPointLocator {
            route: TimestampPointRoute {
                source_group: source.record.identity.group_id,
                source_generation: source.record.generation,
                target_generation: target.record.generation,
                authority,
            },
            txn: binding.txn,
            request_digest: binding.request.digest,
            manifest: locator.manifest,
            scope,
        };
        let response = self.client.call_timestamp_point_proof(method, None, source, &target).await?;
        let context = response.status_context();
        let client = &self.client;
        let (actual, owner) = client.shared.execute(ExecutionClass::Control, response.node, 0, move || {
            let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
            if raw.len() > TIMESTAMP_POINT_LOCATOR_REPLY_LIMIT || context.method != method {
                return Err(invalid());
            }
            let actual: LocatedTimestampManifest = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            context.verify(actual.locator.manifest.status_group_id, &actual)?;
            Ok((actual, context.owner()))
        }).await?;
        if actual.locator != *locator || !valid_outcome(&actual.outcome, binding.result_digest)? {
            return Err(invalid());
        }
        Ok((actual, owner))
    }

    pub(crate) async fn scoped_plan(
        &self,
        scope: super::super::authority::control::ScopedCatalogRead,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<ParticipantPlan, ProtocolError> {
        if manifest.status_group_id != self.client.group.group_id {
            return Err(invalid());
        }
        let response = self
            .client
            .read(Method::ScopedStatusParticipant {
                scope,
                txn,
                manifest,
            })
            .await?;
        let plan = response
            .reply
            .decode_section::<Option<ParticipantPlan>>(0)?;
        plan.as_ref().cloned().ok_or_else(invalid)
    }
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
                    | ServiceAction::ScopedQueueExecute(_)
                    | ServiceAction::ScopedQueueRead { .. }
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
        self.settlement_for(txn, manifest, false).await
    }
    pub(super) async fn settlement_for(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        scoped: bool,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        if manifest.status_group_id != self.client.group.group_id {
            return Err(invalid());
        }
        let method = Method::StatusSettlement { txn, manifest };
        let response = if scoped {
            self.client.repeat_as(self.client.action, method, None).await?
        } else {
            self.client.read(method).await?
        };
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
