use super::*;
use crate::{
    oltp::mvcc::store::timestamp_resolution::{self, SealedTimestampPointDecision},
    oltp_replication::{
        authority::status::TimestampDecisionLocator,
        multiplex::Decoded,
        service_rpc::participant::timestamp_point::{
            decode_point_input, decode_point_reply, matches_receiver, unknown_outcome,
        },
    },
};

pub(super) use crate::oltp_replication::service_rpc::participant::timestamp_point::DECODE_BYTES;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveRequest {
    locator: TimestampDecisionLocator,
    authority: Authority,
    target_generation: uuid::Uuid,
}

impl ParticipantEndpoint {
    pub(super) async fn resolve_timestamp_point(
        &self,
        session: &AuthenticatedSession,
        header: &Header,
        sections: ChargedSections,
    ) -> Result<ChargedBytes, ProtocolError> {
        nabedra_rt::time::timeout(SERVICE_CONVERGENCE, async {
            let input: Decoded<ResolveRequest> =
                decode_point_input(&self.proof.shared, self.proof.node, sections).await?;
            let locator = &input.locator;
            locator.validate()?;
            let group = self.proof.handle.identity();
            if &locator.binding.participant.group != group {
                return Err(invalid());
            }
            let authority = input.authority;
            let scope = match header.action {
                ServiceAction::ScopedQueueExecute(scope)
                    if timestamp_resolution::matches_scope(locator, scope)
                        && authority.run_id == locator.binding.request.run_id =>
                {
                    Some(scope)
                }
                ServiceAction::Owner if authority.run_id != locator.binding.request.run_id => None,
                _ => return Err(invalid()),
            };
            let routes = self.proof.control.timestamp_point_routes(group).await?;
            if !matches_receiver(&routes, input.target_generation, self.proof.node, session)
                || scope.is_some_and(|scope| scope.data_generation != routes.record.generation)
            {
                return Err(stale_service_route());
            }
            let timestamp = TimestampClient::new(
                locator.binding.timestamps_group.clone(),
                self.proof.shared.clone(),
                self.transport.clone(),
                self.proof.control.clone(),
                ServiceAction::ParticipantProof {
                    source_group: group.group_id,
                },
            )?;
            let verified = timestamp
                .timestamp_point_decision(&routes, locator, authority)
                .await?;
            let grant = service_grant(&self.proof.control, session, header).await?;
            if grant.effective_authority != authority || grant.owner != Some(session.remote()) {
                return Err(stale_service_route());
            }
            let (proof, charge) = SealedTimestampPointDecision::from_verified(verified)?;
            let command = OwnedCommand::new_owned(
                ParticipantCommand::ResolveTimestampPoint {
                    authority,
                    proof: Box::new(proof),
                },
                &self.proof.shared,
                ByteClass::Progress,
            )?;
            drop((input, charge, routes));
            let value = if let Some(scope) = scope {
                self.proof
                    .handle
                    .propose_verified_scoped(scope, command)
                    .await?
            } else {
                self.proof.handle.propose(command).await?
            };
            let (position, reply, owner) = value.into_parts()?;
            let ParticipantReply::TimestampPointResolved {
                evidence: first,
                writes,
            } = reply
            else {
                return Err(unavailable());
            };
            if first.completed_at.index > position.index
                || first.completed_at.term > position.term
                || (first.completed_at.index == position.index && first.completed_at != position)
            {
                return Err(unavailable());
            }
            let response = Applied::owned(
                first.completed_at,
                ParticipantReply::Resolved {
                    next_ordinal: writes,
                    complete: true,
                },
                owner,
            );
            let shared = self.proof.shared.clone();
            self.proof
                .shared
                .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                    shared.encode_in(ByteClass::Progress, &response, 4096)
                })
                .await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(unknown_outcome)
    }
}

impl ParticipantClient {
    pub(super) async fn resolve_point(
        &self,
        authority: Authority,
        locator: TimestampDecisionLocator,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        locator.validate()?;
        let client = &self.proof.client;
        let authorized = match client.action {
            ServiceAction::ScopedQueueExecute(scope) => {
                authority.run_id == locator.binding.request.run_id
                    && timestamp_resolution::matches_scope(&locator, scope)
            }
            ServiceAction::Owner => authority.run_id != locator.binding.request.run_id,
            _ => false,
        };
        if authority.validate().is_err()
            || !authorized
            || client.group != locator.binding.participant.group
        {
            return Err(invalid());
        }
        let deadline = nabedra_rt::time::now() + SERVICE_CONVERGENCE;
        let peer_deadline = matches!(client.action, ServiceAction::ScopedQueueExecute(_))
            .then_some(deadline);
        timeout_at(deadline, async {
            let method = Method::ParticipantResolveTimestampPoint;
            let mut result = Err(unavailable());
            for _ in 0..2 {
                let route = client.control.timestamp_point_routes(&client.group).await?;
                let payload = client.shared.encode_in(
                    ByteClass::Progress,
                    &ResolveRequest {
                        locator: locator.clone(),
                        authority,
                        target_generation: route.record.generation,
                    },
                    method.input_limit(),
                )?;
                let mut possibly_applied = false;
                result = client
                    .call_routed(
                        client.action,
                        method,
                        &Some(payload),
                        &mut possibly_applied,
                        &route,
                        None,
                        CallDeadlines {
                            peer_failover: peer_deadline,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(ProtocolError::from);
                if !result
                    .as_ref()
                    .err()
                    .is_some_and(placement_rejected_before_execution)
                {
                    break;
                }
                client.control.invalidate_replica_routes(&route);
            }
            let response = result?;
            let (value, owner): (Applied<ParticipantReply>, _) =
                decode_point_reply(client, response).await?;
            if !matches!(
                value.reply,
                ParticipantReply::Resolved { complete: true, .. }
            ) {
                return Err(unavailable());
            }
            Ok(Applied::owned(value.position, value.reply, owner))
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(unknown_outcome)
    }
}
