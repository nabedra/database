use super::*;
use crate::{
    oltp::mvcc::store::{
        PointEvidenceRead,
        timestamp_resolution::{TimestampPointRejection, TimestampPointResolution},
    },
    oltp_replication::{
        authority::{
            control::{ReplicaRoutes, ScopedCatalogRead, TimestampPointRoute},
            status::TimestampDecisionLocator,
        },
        multiplex::Decoded,
    },
};

// The proof request carries a current scope alongside the original scope
// already in the locator. Charge both decoded shapes on the protected lane.
pub(in crate::oltp_replication::service_rpc) const DECODE_BYTES: usize = 96 << 10;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofRequest {
    locator: TimestampDecisionLocator,
    scope: Option<ScopedCatalogRead>,
}

pub(super) fn source_action(method: Method, locator: &TimestampDecisionLocator) -> ServiceAction {
    let binding = &locator.binding;
    match method {
        Method::ParticipantTimestampPointResolution { .. } => ServiceAction::StatusProof {
            source_group: binding.status_group.group_id,
        },
        _ => ServiceAction::TimestampProof {
            source_group: binding.timestamps_group.group_id,
        },
    }
}

impl ParticipantProofEndpoint {
    pub(super) async fn read_point_evidence(
        &self,
        session: &AuthenticatedSession,
        header: &Header,
        sections: ChargedSections,
    ) -> Result<ChargedBytes, ProtocolError> {
        nabedra_rt::time::timeout(SERVICE_CONVERGENCE, async {
            let input: Decoded<ProofRequest> =
                decode_point_input(&self.shared, self.node, sections).await?;
            let locator = &input.locator;
            let target = self.handle.identity();
            locator.validate()?;
            if target != &locator.binding.participant.group
                || header.action != source_action(header.method, locator)
            {
                return Err(invalid());
            }
            let (route, digest, kind) = match (header.method, input.scope) {
                (
                    Method::ParticipantTimestampPointSeal {
                        route,
                        locator_digest,
                    },
                    Some(scope),
                ) => (route, locator_digest, PointEvidenceRead::Seal(scope)),
                (
                    Method::ParticipantTimestampPointRejection {
                        route,
                        locator_digest,
                    },
                    Some(scope),
                ) => (route, locator_digest, PointEvidenceRead::Rejection(scope)),
                (
                    Method::ParticipantTimestampPointResolution {
                        route,
                        source_digest,
                        locator_digest,
                    },
                    None,
                ) => {
                    if source_digest != serialized_digest(&locator.binding.status_group)? {
                        return Err(invalid());
                    }
                    (route, locator_digest, PointEvidenceRead::Completion)
                }
                _ => return Err(invalid()),
            };
            let source = match source_action(header.method, locator) {
                ServiceAction::TimestampProof { source_group }
                | ServiceAction::StatusProof { source_group } => source_group,
                _ => unreachable!(),
            };
            if route.source_group != source || serialized_digest(&*input)? != digest {
                return Err(invalid());
            }
            match kind {
                PointEvidenceRead::Completion => {
                    self.control
                        .verify_timestamp_point_decision_route(
                            session,
                            self.node,
                            route,
                            header.action,
                            serialized_digest(&locator.binding.status_group)?,
                        )
                        .await?;
                }
                _ => {
                    if route.target_generation != locator.binding.original_scope.data_generation {
                        return Err(stale_service_route());
                    }
                    self.control
                        .verify_timestamp_point_route(session, self.node, route)
                        .await?;
                }
            }
            let authority = route.authority;
            self.handle
                .read_timestamp_point(input, |input| &input.locator, authority, kind)
                .await
        })
        .await
        .map_err(|_| unavailable())?
    }
}

pub(in crate::oltp_replication::service_rpc) fn matches_receiver(
    routes: &ReplicaRoutes,
    generation: uuid::Uuid,
    node: u64,
    session: &AuthenticatedSession,
) -> bool {
    routes.record.generation == generation
        && routes
            .members
            .iter()
            .any(|member| member.node_id == node && member.worker_id == session.local().worker_id)
}

pub(in crate::oltp_replication::service_rpc) async fn decode_point_input<
    T: serde::de::DeserializeOwned + Send + 'static,
>(
    shared: &Arc<WorkerSharedResources>,
    node: u64,
    sections: ChargedSections,
) -> Result<Decoded<T>, ProtocolError> {
    shared
        .execute(ExecutionClass::Control, node, 0, move || {
            if sections.section(1).is_some() || sections.section(2).is_some() {
                return Err(invalid());
            }
            sections.decode(0)
        })
        .await
}

#[derive(Clone, Copy)]
enum ProofKind {
    Seal,
    Rejection,
    Completion,
}

#[derive(Clone)]
pub(crate) struct VerifiedTimestampPointSeal {
    home: Digest,
    evidence: PreparedParticipant,
    _owner: Arc<ScratchLease>,
}

impl VerifiedTimestampPointSeal {
    pub(crate) fn evidence_for(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<&PreparedParticipant, ProtocolError> {
        if self.home != locator.digest()? || !locator.matches_seal(&self.evidence) {
            return Err(unavailable());
        }
        Ok(&self.evidence)
    }
}

pub(crate) struct VerifiedTimestampPointRejection {
    evidence: TimestampPointRejection,
    authority: Authority,
    _owner: Arc<ScratchLease>,
}

impl VerifiedTimestampPointRejection {
    pub(crate) fn evidence_for(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<&TimestampPointRejection, ProtocolError> {
        self.evidence.validate_for(locator, self.authority)?;
        Ok(&self.evidence)
    }
}

pub(crate) struct VerifiedTimestampPointResolution {
    completion: TimestampPointResolution,
    owner: Arc<ScratchLease>,
}

impl VerifiedTimestampPointResolution {
    pub(crate) fn completion(&self) -> &TimestampPointResolution {
        &self.completion
    }

    pub(crate) fn into_parts(self) -> (TimestampPointResolution, Arc<ScratchLease>) {
        (self.completion, self.owner)
    }
}

impl ParticipantProofClient {
    pub(crate) async fn timestamp_point_seal(
        &self,
        source: &ReplicaRoutes,
        locator: &TimestampDecisionLocator,
        authority: Authority,
        scope: ScopedCatalogRead,
    ) -> Result<VerifiedTimestampPointSeal, ProtocolError> {
        let (evidence, owner) = self
            .point_evidence(
                ProofKind::Seal,
                Some(source),
                locator,
                authority,
                Some(scope),
            )
            .await?;
        let proof = VerifiedTimestampPointSeal {
            home: locator.digest()?,
            evidence,
            _owner: owner,
        };
        proof.evidence_for(locator)?;
        if proof.evidence.authority.owner_term > authority.owner_term {
            return Err(unavailable());
        }
        Ok(proof)
    }

    pub(crate) async fn timestamp_point_rejection(
        &self,
        source: &ReplicaRoutes,
        locator: &TimestampDecisionLocator,
        authority: Authority,
        scope: ScopedCatalogRead,
    ) -> Result<VerifiedTimestampPointRejection, ProtocolError> {
        let (evidence, owner) = self
            .point_evidence(
                ProofKind::Rejection,
                Some(source),
                locator,
                authority,
                Some(scope),
            )
            .await?;
        let proof = VerifiedTimestampPointRejection {
            evidence,
            authority,
            _owner: owner,
        };
        proof.evidence_for(locator)?;
        Ok(proof)
    }

    pub(crate) async fn timestamp_point_resolution(
        &self,
        locator: &TimestampDecisionLocator,
        authority: Authority,
    ) -> Result<VerifiedTimestampPointResolution, ProtocolError> {
        let (completion, owner): (TimestampPointResolution, _) = self
            .point_evidence(ProofKind::Completion, None, locator, authority, None)
            .await?;
        completion.validate_for(locator)?;
        Ok(VerifiedTimestampPointResolution { completion, owner })
    }

    async fn point_evidence<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        kind: ProofKind,
        source_routes: Option<&ReplicaRoutes>,
        locator: &TimestampDecisionLocator,
        authority: Authority,
        scope: Option<ScopedCatalogRead>,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        locator.validate()?;
        let client = &self.client;
        let binding = &locator.binding;
        let (source, action) = match kind {
            ProofKind::Completion => (
                &binding.status_group,
                ServiceAction::StatusProof {
                    source_group: binding.status_group.group_id,
                },
            ),
            ProofKind::Seal | ProofKind::Rejection => (
                &binding.timestamps_group,
                ServiceAction::TimestampProof {
                    source_group: binding.timestamps_group.group_id,
                },
            ),
        };
        if client.action != action || client.group != binding.participant.group {
            return Err(invalid());
        }
        nabedra_rt::time::timeout(SERVICE_CONVERGENCE, async {
            let request = ProofRequest {
                locator: locator.clone(),
                scope,
            };
            let locator_digest = serialized_digest(&request)?;
            let payload = client
                .shared
                .encode_in(ByteClass::Progress, &request, 8 << 10)?;
            let mut result = Err(unavailable());
            for _ in 0..2 {
                let (source_route, target) = tokio::try_join!(
                    async {
                        match source_routes {
                            Some(routes) if routes.record.identity == *source => Ok(routes.clone()),
                            Some(_) => Err(invalid()),
                            None => client.control.timestamp_point_routes(source).await,
                        }
                    },
                    client.control.timestamp_point_routes(&client.group),
                )?;
                let local = client.transport.local_identity();
                if !source_route
                    .members
                    .iter()
                    .any(|member| member.worker_id == local.worker_id)
                {
                    return Err(stale_service_route());
                }
                let route = TimestampPointRoute {
                    source_group: source.group_id,
                    source_generation: source_route.record.generation,
                    target_generation: target.record.generation,
                    authority,
                };
                let method = match kind {
                    ProofKind::Seal => Method::ParticipantTimestampPointSeal {
                        route,
                        locator_digest,
                    },
                    ProofKind::Rejection => Method::ParticipantTimestampPointRejection {
                        route,
                        locator_digest,
                    },
                    ProofKind::Completion => Method::ParticipantTimestampPointResolution {
                        route,
                        source_digest: serialized_digest(source)?,
                        locator_digest,
                    },
                };
                result = client
                    .call_timestamp_point_proof(
                        method,
                        Some(payload.clone()),
                        &source_route,
                        &target,
                    )
                    .await;
                if source_routes.is_some() || result.as_ref().err() != Some(&stale_service_route())
                {
                    break;
                }
            }
            decode_point_reply(client, result?).await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(|error| {
            if error == stale_service_route() {
                error
            } else {
                unknown_outcome(error)
            }
        })
    }
}

pub(in crate::oltp_replication::service_rpc) async fn decode_point_reply<
    T: serde::de::DeserializeOwned + Send + 'static,
>(
    client: &Client,
    response: ServiceResponse,
) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
    client
        .shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
            crate::protocol::json_input(raw.as_slice(), raw.len()).map_err(|_| invalid())?;
            let value = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            Ok((value, response.reply.scratch.clone()))
        })
        .await
}

pub(in crate::oltp_replication::service_rpc) fn unknown_outcome(
    mut error: ProtocolError,
) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}
