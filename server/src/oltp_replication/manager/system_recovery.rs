//! Checks a surviving RF3 quorum on the actual group leader. Raft heartbeats
//! identify the voter that acknowledged them, unlike a successful ReadIndex.
use super::*;
use crate::{
    oltp::pool::{ByteClass, ChargedBytes},
    oltp_replication::{
        authority::control::ControlHandle,
        multiplex::{
            AuthenticatedSession, ChargedReply, ChargedSections, Endpoint, EndpointFuture,
            ReceivePlan, TrafficClass,
        },
    },
};
use openraft::{Instant as _, type_config::TypeConfigExt as _};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

const HEADER: usize = 4096;
const QUORUM_TIMEOUT: Duration = Duration::from_secs(3);
const LOST_HEARTBEAT_MAX_AGE: Duration = Duration::from_secs(3);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QuorumRequest {
    pub group: GroupIdentity,
    pub voters: [ReplicaMember; 3],
    pub lost: Uuid,
    pub leader: u64,
    pub minimum: AppliedPosition,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SurvivingQuorum {
    pub leader: u64,
    pub follower: u64,
    pub applied_at: AppliedPosition,
}

pub(super) type QuorumFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SurvivingQuorum, ProtocolError>> + Send + 'a>>;

impl QuorumRequest {
    fn validate(&self, scope: &GroupIdentity) -> Result<(), ProtocolError> {
        self.group.validate().map_err(|_| invalid())?;
        validate_voters(&self.voters).map_err(|_| invalid())?;
        if self.group.cluster_id != scope.cluster_id
            || self.group.database != scope.database
            || !self
                .voters
                .iter()
                .any(|member| member.worker_id == self.lost)
            || !self
                .voters
                .iter()
                .any(|member| member.node_id == self.leader && member.worker_id != self.lost)
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn matches(&self, membership: &openraft::Membership<u64, ReplicaMember>) -> bool {
        let [voters] = membership.get_joint_config().as_slice() else {
            return false;
        };
        voters.len() == self.voters.len()
            && self.voters.iter().all(|member| {
                voters.contains(&member.node_id)
                    && membership.get_node(&member.node_id) == Some(member)
            })
    }

    fn follower(&self) -> Result<u64, ProtocolError> {
        self.voters
            .iter()
            .find(|member| member.node_id != self.leader && member.worker_id != self.lost)
            .map(|member| member.node_id)
            .ok_or_else(invalid)
    }
}

impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(super) async fn check_surviving_quorum(
        &self,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let local = self.state.resident().node_id();
        if self.state.store().identity() != &request.group || local != request.leader {
            return Err(invalid());
        }
        let lost = request
            .voters
            .iter()
            .find(|member| member.worker_id == request.lost)
            .ok_or_else(invalid)?
            .node_id;
        let follower = request.follower()?;
        let start = C::now();
        let (generation, mut metrics) = self.raft.current_metrics();
        let matches_leader = |state: &openraft::RaftMetrics<C>| {
            state.running_state.is_ok()
                && state.current_leader == Some(local)
                && request.matches(state.membership_config.membership())
                && state.membership_config == state.committed_membership_config
        };
        tokio::time::timeout(QUORUM_TIMEOUT, async {
            let term = {
                let state = metrics.borrow_watched();
                if !matches_leader(&state) {
                    return Err(unavailable());
                }
                state.current_term
            };

            // The read provides the applied barrier. The heartbeat supplies a
            // separate, fresh acknowledgment specifically from the other survivor.
            let applied_at = self.ensure_linearizable().await?.applied;
            if applied_at.index < request.minimum.index {
                return Err(unavailable());
            }
            self.raft
                .trigger()
                .heartbeat()
                .await
                .map_err(|_| unavailable())?;
            loop {
                {
                    let state = metrics.borrow_watched();
                    if !matches_leader(&state)
                        || state.current_term != term
                        || self.raft.current_metrics().0 != generation
                    {
                        return Err(unavailable());
                    }
                    let heartbeat = state.heartbeat.as_ref().ok_or_else(unavailable)?;
                    if heartbeat
                        .get(&lost)
                        .and_then(Option::as_ref)
                        .is_some_and(|last| last.elapsed() < LOST_HEARTBEAT_MAX_AGE)
                    {
                        return Err(reachable());
                    }
                    if heartbeat
                        .get(&follower)
                        .and_then(Option::as_ref)
                        .is_some_and(|last| last.into_inner() >= start)
                    {
                        return Ok(SurvivingQuorum {
                            leader: local,
                            follower,
                            applied_at,
                        });
                    }
                }
                metrics.changed().await.map_err(|_| unavailable())?;
            }
        })
        .await
        .map_err(|_| unavailable())?
    }
}

impl HostedGroupManager {
    pub(crate) async fn surviving_quorum(
        &self,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let running = {
            let routes = self.routes.lock().map_err(|_| unavailable())?;
            routes
                .get(&request.group.group_id)
                .and_then(|group| group.replicas.get(&request.leader))
                .filter(|route| !route.retiring && request.voters.contains(&route.member))
                .ok_or_else(unavailable)?
                .running
                .clone()
        };
        running.surviving_quorum(request).await
    }

    pub(crate) async fn remote_surviving_quorum(
        &self,
        control: &GroupIdentity,
        target: WorkerIdentity,
        request: &QuorumRequest,
    ) -> Result<SurvivingQuorum, ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER)?;
        let reply = self
            .client
            .request_classified(
                control,
                target,
                Purpose::SystemRecovery,
                header,
                [None, None, None],
                HEADER,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != control
            || reply.purpose() != Purpose::SystemRecovery
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(invalid());
        }
        let proof = reply.decode_header::<SurvivingQuorum>()?;
        if proof.leader != request.leader
            || proof.follower != request.follower()?
            || proof.applied_at.index < request.minimum.index
        {
            return Err(invalid());
        }
        Ok(*proof)
    }
}

pub(crate) struct QuorumEndpoint {
    pub manager: Arc<HostedGroupManager>,
    pub control: Arc<ControlHandle>,
}
impl QuorumEndpoint {
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<QuorumRequest, ProtocolError> {
        if session.group() != self.control.identity()
            || session.purpose() != Purpose::SystemRecovery
            || session.recovery_only()
            || !self.control.is_operator(&session.remote())
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "system recovery requires a registered operator peer",
                Outcome::NotApplied,
            ));
        }
        crate::protocol::json_input(header, HEADER).map_err(|_| invalid())?;
        let request: QuorumRequest = serde_json::from_slice(header).map_err(|_| invalid())?;
        request.validate(self.control.identity())?;
        Ok(request)
    }
}
impl Endpoint for QuorumEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            Ok(ReceivePlan {
                class: TrafficClass::Settlement,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request = self.parse(&session, header.as_slice())?;
            if (0..3).any(|index| sections.section(index).is_some()) {
                return Err(invalid());
            }
            let proof = self.manager.surviving_quorum(&request).await?;
            Ok(ChargedReply {
                header: self
                    .manager
                    .shared
                    .encode_in(ByteClass::Progress, &proof, HEADER)?,
                sections: [None, None, None],
            })
        })
    }
}

pub(crate) fn reachable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "lost worker is still reachable",
        Outcome::NotApplied,
    )
}
