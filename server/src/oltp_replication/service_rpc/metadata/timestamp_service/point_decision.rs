use super::*;
use crate::oltp_replication::{
    authority::{
        control::{ReplicaRoutes, TimestampPointRoute, stale_service_route},
        timestamp::{
            PointDecision, TimestampDecisionLocator, TimestampPointBinding,
            point_decision::matches_epoch,
        },
    },
    multiplex::Decoded,
    service_rpc::{VerifiedTimestampPointRejection, VerifiedTimestampPointSeal},
};

pub(super) const LOCATOR_DECODE_BYTES: usize = 64 << 10;

#[derive(Clone, Copy)]
pub(super) enum PointOperation {
    Commit,
    Abort,
}

impl PointOperation {
    fn request(self, locator: &TimestampDecisionLocator, reconcile: bool) -> Request {
        match self {
            Self::Commit => Request::CommitTimestampPoint {
                locator: locator.clone(),
                reconcile,
            },
            Self::Abort => Request::AbortTimestampPoint {
                locator: locator.clone(),
                reconcile,
            },
        }
    }
}

enum DataPointEvidence {
    Seal(VerifiedTimestampPointSeal),
    Rejection(VerifiedTimestampPointRejection),
}

/// Retain the decode charge with the exact decision verified by the read client.
pub(crate) struct VerifiedTimestampPointDecision {
    decision: PointDecision,
    charge: Arc<ScratchLease>,
}

impl VerifiedTimestampPointDecision {
    pub(crate) fn decision(&self) -> &PointDecision {
        &self.decision
    }

    pub(crate) fn into_parts(self) -> (PointDecision, Arc<ScratchLease>) {
        (self.decision, self.charge)
    }
}

fn decision_source(
    locator: &TimestampDecisionLocator,
    action: ServiceAction,
) -> Option<&GroupIdentity> {
    match action {
        ServiceAction::StatusProof { source_group }
            if source_group == locator.binding.status_group.group_id =>
        {
            Some(&locator.binding.status_group)
        }
        ServiceAction::ParticipantProof { source_group }
            if source_group == locator.binding.participant.group.group_id =>
        {
            Some(&locator.binding.participant.group)
        }
        _ => None,
    }
}

impl TimestampEndpoint {
    pub(super) async fn read_point_decision(
        &self,
        sections: ChargedSections,
        action: ServiceAction,
        route: TimestampPointRoute,
        source_digest: Digest,
        locator_digest: Digest,
    ) -> Result<ChargedBytes, ProtocolError> {
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let locator: Decoded<TimestampDecisionLocator> = self
                .handle
                .resources()
                .execute(ExecutionClass::Control, self.node, 0, move || {
                    if sections.section(1).is_some() || sections.section(2).is_some() {
                        return Err(invalid());
                    }
                    let locator = sections.decode::<TimestampDecisionLocator>(0)?;
                    locator.validate()?;
                    let source = decision_source(&locator, action).ok_or_else(invalid)?;
                    if source.group_id != route.source_group
                        || serialized_digest(source)? != source_digest
                        || serialized_digest(&*locator)? != locator_digest
                    {
                        return Err(invalid());
                    }
                    Ok(locator)
                })
                .await?;
            self.validate_point_home(&locator)?;
            if matches!(action, ServiceAction::ParticipantProof { .. }) {
                self.verify_point_data_route(&locator, route.source_generation, route.authority)
                    .await?;
            }
            self.point_decision_reply(&locator)
                .await?
                .ok_or_else(unavailable)
        })
        .await
        .map_err(|_| unavailable())?
    }

    async fn verify_point_data_route(
        &self,
        locator: &TimestampDecisionLocator,
        generation: Uuid,
        authority: Authority,
    ) -> Result<(), ProtocolError> {
        let binding = &locator.binding;
        if generation == binding.original_scope.data_generation {
            return Ok(());
        }
        if authority.run_id != binding.request.run_id {
            // The protected route already verifies the current exact voter of
            // this same Data group. A published cold fence closes the old
            // queue, so settlement must use the live durable replica lineage.
            return if self
                .control
                .published_cold_fence(authority, binding.request.run_id)
                .await?
                .is_some()
            {
                Ok(())
            } else {
                Err(stale_service_route())
            };
        }
        let placement = self
            .control
            .scoped_queue(&binding.participant.group)
            .await?
            .ok_or_else(stale_service_route)?;
        let epoch = ScopedTimestampEpoch::from_placement(&placement)?;
        if placement.data_generation != generation || !epoch.advances(binding.original_epoch) {
            return Err(stale_service_route());
        }
        Ok(())
    }

    fn validate_point_home(&self, locator: &TimestampDecisionLocator) -> Result<(), ProtocolError> {
        locator.validate()?;
        if locator.binding.timestamps_group != *self.handle.identity()
            || self
                .status
                .manifest(locator.binding.txn, locator.manifest)?
                != &locator.binding.status_group
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn scoped_point_authority(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        locator: &TimestampDecisionLocator,
    ) -> Result<(Authority, ScopedTimestampEpoch), ProtocolError> {
        let ServiceAction::ScopedQueueExecute(scope) = head.action else {
            return Err(invalid());
        };
        validate_queue_scope(scope, locator)?;
        let binding = &locator.binding;
        let (grant, placement) = tokio::try_join!(
            service_grant(&self.control, session, head),
            self.control.scoped_queue(&binding.participant.group),
        )?;
        let placement = placement.ok_or_else(unavailable)?;
        // The current publication of a replacement already certifies the
        // transfer. Apply also requires its matching local Timestamp fence.
        if grant.owner != Some(session.remote())
            || placement.owner != session.remote()
            || placement.queue_generation != scope.queue_generation
            || placement.data_generation != scope.data_generation
            || placement.view != scope.queue_view
            || !binding.accepts_authority(grant.effective_authority)
        {
            return Err(stale_service_route());
        }
        let epoch = ScopedTimestampEpoch::from_placement(&placement)?;
        if !matches_epoch(binding, epoch) {
            return Err(stale_service_route());
        }
        Ok((grant.effective_authority, epoch))
    }

    async fn point_source_routes(&self) -> Result<ReplicaRoutes, ProtocolError> {
        let routes = self
            .control
            .timestamp_point_routes(self.handle.identity())
            .await?;
        if !routes.members.iter().any(|member| {
            member.node_id == self.node
                && member.worker_id == self.transport.local_identity().worker_id
        }) {
            return Err(stale_service_route());
        }
        Ok(routes)
    }

    pub(super) async fn decide_point(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        locator: &TimestampDecisionLocator,
        operation: PointOperation,
        reconcile: bool,
    ) -> Result<ChargedBytes, ProtocolError> {
        let ServiceAction::ScopedQueueExecute(scope) = head.action else {
            return Err(invalid());
        };
        self.validate_point_home(locator)?;
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let (authority, epoch) = self.scoped_point_authority(session, head, locator).await?;
            let mut reconcile = reconcile;
            let binding = &locator.binding;
            let action = ServiceAction::TimestampProof {
                source_group: binding.timestamps_group.group_id,
            };
            let status = self.point_status_proof(locator)?;
            let data = ParticipantProofClient::new(
                binding.participant.group.clone(),
                self.handle.resources().clone(),
                self.transport.clone(),
                self.control.clone(),
                action,
            )?;

            for refresh in [false, true] {
                let routes = match self.point_source_routes().await {
                    Ok(routes) => routes,
                    Err(error) if !refresh && placement_rejected_before_execution(&error) => {
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if reconcile {
                    if let Some(decision) = self.point_decision_reply(locator).await? {
                        return Ok(decision);
                    }
                    reconcile = false;
                }
                let (status, data) = tokio::join!(
                    status.timestamp_point_locator(&routes, locator, authority),
                    async {
                        match operation {
                            PointOperation::Commit => data
                                .timestamp_point_seal(&routes, locator, authority, scope)
                                .await
                                .map(DataPointEvidence::Seal),
                            PointOperation::Abort => data
                                .timestamp_point_rejection(&routes, locator, authority, scope)
                                .await
                                .map(DataPointEvidence::Rejection),
                        }
                    },
                );
                if !refresh
                    && [status.as_ref().err(), data.as_ref().err()]
                        .into_iter()
                        .flatten()
                        .any(|error| *error == stale_service_route())
                {
                    self.control.invalidate_replica_routes(&routes);
                    continue;
                }
                match (status, data) {
                    (Ok(status), Ok(data)) => {
                        let result = match data {
                            DataPointEvidence::Seal(seal) => {
                                self.handle
                                    .commit_timestamp_point(locator, authority, epoch, status, seal)
                                    .await
                            }
                            DataPointEvidence::Rejection(rejection) => {
                                self.handle
                                    .abort_timestamp_point(
                                        locator, authority, epoch, status, rejection,
                                    )
                                    .await
                            }
                        };
                        return match result {
                            Ok(decision) => Ok(decision),
                            Err(error) => self.point_after_error(locator, error).await,
                        };
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        return self.point_after_error(locator, error).await;
                    }
                }
            }
            Err(unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) async fn verify_point_pin(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        binding: TimestampPointBinding,
    ) -> Result<ChargedBytes, ProtocolError> {
        if binding.timestamps_group != *self.handle.identity()
            || self.status.request(binding.txn.request_id) != &binding.status_group
            || head.action
                != (ServiceAction::StatusProof {
                    source_group: binding.status_group.group_id,
                })
        {
            return Err(invalid());
        }
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let (grant, placement, _) = tokio::try_join!(
                service_grant(&self.control, session, head),
                self.control.scoped_queue(&binding.participant.group),
                self.point_source_routes(),
            )?;
            let placement = placement.ok_or_else(unavailable)?;
            if grant.effective_authority != binding.created_authority
                || placement.data_generation != binding.original_scope.data_generation
                || ScopedTimestampEpoch::from_placement(&placement)? != binding.original_epoch
            {
                return Err(stale_service_route());
            }
            self.handle.verify_timestamp_point_pin(binding).await?;
            encode(self.handle.resources(), ()).await
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) async fn observe_point(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        locator: TimestampDecisionLocator,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.validate_point_home(&locator)?;
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let authorize = async {
                match head.action {
                    ServiceAction::ScopedQueueExecute(_) => {
                        self.scoped_point_authority(session, head, &locator).await?;
                    }
                    ServiceAction::Owner => {
                        service_grant(&self.control, session, head).await?;
                    }
                    _ => return Err(invalid()),
                }
                Ok(())
            };
            tokio::try_join!(authorize, self.point_source_routes())?;
            self.handle.read_timestamp_point_decision(locator).await
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) async fn abandon_point(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        locator: &TimestampDecisionLocator,
        transition_id: Uuid,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.validate_point_home(locator)?;
        if head.action != ServiceAction::Owner || transition_id.is_nil() {
            return Err(invalid());
        }
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let grant = service_grant(&self.control, session, head).await?;
            let authority = grant.effective_authority;
            if grant.owner != Some(session.remote())
                || self
                    .control
                    .published_cold_fence(authority, locator.binding.request.run_id)
                    .await?
                    != Some(transition_id)
            {
                return Err(unavailable());
            }
            let status = self.point_status_proof(locator)?;
            for refresh in [false, true] {
                let routes = self.point_source_routes().await?;
                if !refresh && let Some(decision) = self.point_decision_reply(locator).await? {
                    return Ok(decision);
                }
                let proof = match status
                    .timestamp_point_locator(&routes, locator, authority)
                    .await
                {
                    Ok(proof) => proof,
                    Err(error) if !refresh && placement_rejected_before_execution(&error) => {
                        self.control.invalidate_replica_routes(&routes);
                        continue;
                    }
                    Err(error) => return self.point_after_error(locator, error).await,
                };
                return match self
                    .handle
                    .abandon_timestamp_point(locator, authority, transition_id, proof)
                    .await
                {
                    Ok(decision) => Ok(decision),
                    Err(error) => self.point_after_error(locator, error).await,
                };
            }
            Err(unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }

    fn point_status_proof(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<Arc<StatusProofClient>, ProtocolError> {
        StatusProofClient::new(
            locator.binding.status_group.clone(),
            self.handle.resources().clone(),
            self.transport.clone(),
            self.control.clone(),
            ServiceAction::TimestampProof {
                source_group: self.handle.identity().group_id,
            },
        )
    }

    async fn point_after_error(
        &self,
        locator: &TimestampDecisionLocator,
        error: ProtocolError,
    ) -> Result<ChargedBytes, ProtocolError> {
        if let Some(decision) = self.point_decision_reply(locator).await? {
            return Ok(decision);
        }
        Err(point_unknown(error))
    }

    async fn point_decision_reply(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<Option<ChargedBytes>, ProtocolError> {
        let bytes = self
            .handle
            .read_timestamp_point_decision(locator.clone())
            .await?;
        // Option encodes Some without a wrapper, so the native bytes are the proof.
        let present = serde_json::from_slice::<Option<serde::de::IgnoredAny>>(bytes.as_slice())
            .map_err(|_| unavailable())?
            .is_some();
        Ok(present.then_some(bytes))
    }
}

fn validate_queue_scope(
    scope: ScopedCatalogRead,
    locator: &TimestampDecisionLocator,
) -> Result<(), ProtocolError> {
    if !locator.binding.matches_scope(scope) {
        return Err(invalid());
    }
    Ok(())
}

fn point_unknown(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}

impl TimestampClient {
    pub(crate) async fn verify_timestamp_point_pin(
        &self,
        binding: &TimestampPointBinding,
    ) -> Result<(), ProtocolError> {
        binding.roots()?;
        if binding.timestamps_group != *self.identity()
            || self.client.action
                != (ServiceAction::StatusProof {
                    source_group: binding.status_group.group_id,
                })
        {
            return Err(invalid());
        }
        tokio::time::timeout(
            SERVICE_CONVERGENCE,
            self.value(Request::VerifyTimestampPointPin(Box::new(binding.clone()))),
        )
        .await
        .map_err(|_| unavailable())?
    }

    pub(crate) async fn timestamp_point_decision(
        &self,
        source: &ReplicaRoutes,
        locator: &TimestampDecisionLocator,
        current_authority: Authority,
    ) -> Result<VerifiedTimestampPointDecision, ProtocolError> {
        locator.validate()?;
        if &locator.binding.timestamps_group != self.identity()
            || decision_source(locator, self.client.action) != Some(&source.record.identity)
            || current_authority.validate().is_err()
        {
            return Err(invalid());
        }
        let client = &self.client;
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let target = client.control.timestamp_point_routes(&client.group).await?;
            let method = Method::TimestampPointDecisionRead {
                route: TimestampPointRoute {
                    source_group: source.record.identity.group_id,
                    source_generation: source.record.generation,
                    target_generation: target.record.generation,
                    authority: current_authority,
                },
                source_digest: serialized_digest(&source.record.identity)?,
                locator_digest: serialized_digest(locator)?,
            };
            let input =
                client
                    .shared
                    .encode_in(ByteClass::Progress, locator, method.input_limit())?;
            let response = client
                .call_timestamp_point_proof(method, Some(input), source, &target)
                .await?;
            if response.reply.sections[0]
                .as_ref()
                .is_none_or(|bytes| bytes.len() > method.input_limit())
            {
                return Err(unavailable());
            }
            let context = response.metadata_context()?;
            let (decision, charge): (PointDecision, _) = response.decoded(&client.shared).await?;
            context.timestamp_point_decision(method, self.identity(), &decision)?;
            decision.verify_locator(locator)?;
            Ok(VerifiedTimestampPointDecision { decision, charge })
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(point_unknown)
    }

    pub(crate) async fn commit_timestamp_point(
        &self,
        locator: &TimestampDecisionLocator,
        reconcile: bool,
    ) -> Result<(PointDecision, Arc<ScratchLease>), ProtocolError> {
        self.submit_timestamp_point(locator, PointOperation::Commit, reconcile)
            .await
    }

    pub(crate) async fn abort_timestamp_point(
        &self,
        locator: &TimestampDecisionLocator,
        reconcile: bool,
    ) -> Result<(PointDecision, Arc<ScratchLease>), ProtocolError> {
        self.submit_timestamp_point(locator, PointOperation::Abort, reconcile)
            .await
    }

    async fn submit_timestamp_point(
        &self,
        locator: &TimestampDecisionLocator,
        operation: PointOperation,
        reconcile: bool,
    ) -> Result<(PointDecision, Arc<ScratchLease>), ProtocolError> {
        self.validate_point_client(locator)?;
        let ServiceAction::ScopedQueueExecute(scope) = self.client.action else {
            return Err(invalid());
        };
        validate_queue_scope(scope, locator)?;
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            if !reconcile {
                let request = operation.request(locator, false);
                let routes = self
                    .client
                    .control
                    .timestamp_point_routes(self.identity())
                    .await?;
                let preferred = routes
                    .last_successful_node()
                    .unwrap_or_else(|| self.client.leader.load(Ordering::Relaxed));
                let member = routes
                    .members
                    .iter()
                    .find(|member| member.node_id == preferred)
                    .or_else(|| routes.members.first())
                    .ok_or_else(unavailable)?;
                let method = request.method();
                let payload = Some(encode(&self.client.shared, request).await?);
                let response = tokio::time::timeout(
                    HINTED_READ_TIMEOUT,
                    self.client.call_member(
                        self.client.action,
                        method,
                        &payload,
                        member.node_id,
                        member.worker_id.into(),
                    ),
                )
                .await;
                match response {
                    Ok(Ok(Ok(response))) => {
                        routes.record_successful_node(member.node_id);
                        return self.decode_point_decision(locator, response).await;
                    }
                    Ok(Ok(Err(error)) | Err(CallError::Local(error)))
                        if !route_unavailable(&error)
                            && !placement_rejected_before_execution(&error) =>
                    {
                        return Err(error);
                    }
                    _ => self.client.control.invalidate_replica_routes(&routes),
                }
            }
            // Every call after a potentially transmitted first write carries
            // reconciliation. The receiver reads the original decision before proposing.
            let response = self.response(operation.request(locator, true)).await?;
            self.decode_point_decision(locator, response).await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(point_unknown)
    }

    pub(crate) async fn observe_timestamp_point(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<Option<(PointDecision, Arc<ScratchLease>)>, ProtocolError> {
        self.validate_point_client(locator)?;
        if let ServiceAction::ScopedQueueExecute(scope) = self.client.action {
            validate_queue_scope(scope, locator)?;
        } else if self.client.action != ServiceAction::Owner {
            return Err(invalid());
        }
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let response = self
                .response(Request::ObserveTimestampPoint {
                    locator: locator.clone(),
                })
                .await?;
            let (decision, charge): (Option<PointDecision>, _) =
                response.decoded(&self.client.shared).await?;
            if let Some(decision) = decision {
                decision.verify_locator(locator)?;
                return Ok(Some((decision, charge)));
            }
            Ok(None)
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(point_unknown)
    }

    pub(crate) async fn abandon_timestamp_point(
        &self,
        locator: &TimestampDecisionLocator,
        transition_id: Uuid,
    ) -> Result<(PointDecision, Arc<ScratchLease>), ProtocolError> {
        self.validate_point_client(locator)?;
        if self.client.action != ServiceAction::Owner || transition_id.is_nil() {
            return Err(invalid());
        }
        tokio::time::timeout(SERVICE_CONVERGENCE, async {
            let response = self
                .response(Request::AbandonTimestampPoint {
                    locator: locator.clone(),
                    transition_id,
                })
                .await?;
            self.decode_point_decision(locator, response).await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(point_unknown)
    }

    fn validate_point_client(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<(), ProtocolError> {
        locator.validate()?;
        if &locator.binding.timestamps_group != self.identity() {
            return Err(invalid());
        }
        Ok(())
    }

    async fn decode_point_decision(
        &self,
        locator: &TimestampDecisionLocator,
        response: ServiceResponse,
    ) -> Result<(PointDecision, Arc<ScratchLease>), ProtocolError> {
        let (decision, charge): (PointDecision, _) = response.decoded(&self.client.shared).await?;
        decision.verify_locator(locator)?;
        Ok((decision, charge))
    }
}
