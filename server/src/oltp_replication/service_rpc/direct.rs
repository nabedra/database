//! `Method::Direct`: one-entry point operations served by the range leader
//! (plan section 6 and the addendum of 2026-11-19).
//!
//! The gateway sends the exact request body to the leader of the key range's
//! Data group on the ingress lane. The leader executes it with one
//! `DirectCommit` (or reads it, for `table.get`) and answers one
//! `DirectResponse`. `Method::DirectForward` settles a request id at its
//! anchor before the gateway sends the request down the queued path.
//!
//! Wire shape: the service header carries a fixed-size `DirectHeader`; the
//! request body travels as the first payload section, so the body is charged
//! by the transport at its actual length. The reply payload is the JSON
//! encoding of `DirectResponse`.
//!
//! Mixed versions: an older receiver rejects the unknown method or the
//! ingress purpose on a Data group before executing anything. Gateways send
//! these methods only once `data.direct.v1` is active (T7).
use crate::{
    oltp::pool::ChargedBytes,
    oltp_replication::types::{Digest, GroupIdentity, GroupKind, Text},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use uuid::Uuid;

/// The longest deadline a gateway may ask for. Longer requests are clamped.
pub(crate) const MAX_DIRECT_DEADLINE: Duration = Duration::from_secs(30);

/// The fixed part of a direct request, carried in the service header. The
/// group and database come from the authenticated session, so they cannot
/// differ from the target replica's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectHeader {
    pub table_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub request_id: Uuid,
    /// SHA-256 of the exact request body.
    pub digest: Digest,
    /// Remaining time the gateway allows, in milliseconds. Relative, so the
    /// two clocks need not agree.
    pub deadline_ms: u32,
}

/// One request to the range leader (plan section 6).
#[derive(Clone, Debug)]
pub(crate) struct DirectRequest {
    pub database: Text<128>,
    pub table_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    /// The Data group the gateway routed to.
    pub group: Uuid,
    pub request_id: Uuid,
    /// SHA-256 of `body`.
    pub digest: Digest,
    /// The exact `/v1/operations` request body, `{request_id, operation, payload}`.
    pub body: ChargedBytes,
    /// Time left for the whole request, measured from receipt.
    pub deadline: Duration,
}
impl DirectRequest {
    pub(crate) fn header(&self) -> DirectHeader {
        DirectHeader {
            table_id: self.table_id,
            schema_epoch: self.schema_epoch,
            routing_epoch: self.routing_epoch,
            request_id: self.request_id,
            digest: self.digest,
            deadline_ms: u32::try_from(self.deadline.as_millis()).unwrap_or(u32::MAX),
        }
    }
    /// Rebuilds the request a receiver serves from the session's group, the
    /// header and the received body section.
    pub(crate) fn received(
        group: &GroupIdentity,
        header: DirectHeader,
        body: ChargedBytes,
    ) -> Result<Self, ProtocolError> {
        if !matches!(group.kind, GroupKind::Data { table_id, .. } if table_id == header.table_id)
            || header.request_id.is_nil()
            || header.schema_epoch == 0
            || header.routing_epoch == 0
            || header.deadline_ms == 0
            || body.is_empty()
        {
            return Err(invalid_request());
        }
        Ok(Self {
            database: group.database.clone(),
            table_id: header.table_id,
            schema_epoch: header.schema_epoch,
            routing_epoch: header.routing_epoch,
            group: group.group_id,
            request_id: header.request_id,
            digest: header.digest,
            body,
            deadline: Duration::from_millis(header.deadline_ms.into()).min(MAX_DIRECT_DEADLINE),
        })
    }
}

/// Why the leader sent a request back to the gateway for the queued path.
/// Every reason guarantees that this attempt committed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FallbackReason {
    /// The `data.direct.v1` capability is not active on this leader.
    Inactive,
    /// The operation is not eligible for the direct path.
    Ineligible,
    /// A queued transaction holds an intent on a key.
    Intent,
    /// A split holds the range (its barrier, a frozen source, or a child
    /// still importing). Not a stale route: a refresh returns the same route.
    SplitInProgress,
    /// The request id holds a forward marker: it belongs to the queued path.
    Forwarded,
    /// The entry is too large for one direct commit.
    ResourceLimit,
    /// The table's schema could not be read without blocking (a DDL holds
    /// the catalog) or names an index this leader cannot maintain.
    Schema,
    /// The queued-history window (T7c): the request id has no dedup record
    /// here and may belong to a queued request accepted before activation,
    /// which only the queued path can answer. Nothing was written.
    History,
}
impl FallbackReason {
    pub(crate) const ALL: [Self; 8] = [
        Self::Inactive,
        Self::Ineligible,
        Self::Intent,
        Self::SplitInProgress,
        Self::Forwarded,
        Self::ResourceLimit,
        Self::Schema,
        Self::History,
    ];
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Inactive => "inactive",
            Self::Ineligible => "ineligible",
            Self::Intent => "intent",
            Self::SplitInProgress => "split_in_progress",
            Self::Forwarded => "forwarded",
            Self::ResourceLimit => "resource_limit",
            Self::Schema => "schema",
            Self::History => "history",
        }
    }
}

/// The exact response `data` value, compared by its bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct DirectData(pub Box<RawValue>);
impl DirectData {
    pub(crate) fn get(&self) -> &str {
        self.0.get()
    }
    pub(crate) fn from_json(raw: &str) -> Result<Self, ProtocolError> {
        RawValue::from_string(raw.to_owned())
            .map(Self)
            .map_err(|_| invalid_request())
    }
}
impl PartialEq for DirectData {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl Eq for DirectData {}

/// The leader's answer (plan section 6). `Fallback` carries its reason so
/// the gateway can count it; `SplitInProgress` and `Forwarded` store replies
/// map to `Fallback`, never to `StaleRoute`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectResponse {
    /// The exact `data` of the response: committed, replayed or read.
    Done(DirectData),
    /// A definite error. Its `outcome` says whether anything may have applied.
    Failed(ProtocolError),
    /// Send the request down the queued path (after `DirectForward`).
    Fallback { reason: FallbackReason },
    /// The schema or routing epoch differs from the group's: refresh the
    /// route. Carries the group's epochs when the leader knows them.
    StaleRoute {
        schema_epoch: Option<u64>,
        routing_epoch: Option<u64>,
    },
    /// This replica does not lead the range. `hint` is the node id of the
    /// leader this replica last observed.
    NotLeader { hint: Option<u64> },
}
impl DirectResponse {
    /// The typed admission refusal (D12): nothing was executed.
    pub(crate) fn overloaded(message: &str) -> Self {
        Self::Failed(ProtocolError::new(
            ErrorCode::Overloaded,
            message,
            Outcome::NotApplied,
        ))
    }
}

/// What a `DirectForward` tells the gateway.
/// `Done` (the request already committed directly: return its result),
/// `Fallback { reason: Forwarded }` (a marker is in place: go queued),
/// `Failed(conflict)`, `StaleRoute`, `NotLeader`.
pub(crate) type ForwardResponse = DirectResponse;

/// The leader side of `Method::Direct`, installed per worker by the runtime
/// that owns the executor. A worker without one answers
/// `Fallback { Inactive }`, which commits nothing.
pub(crate) trait DirectService: Send + Sync {
    fn execute<'a>(
        &'a self,
        handle: &'a Arc<crate::oltp::mvcc::store::ParticipantHandle>,
        request: DirectRequest,
    ) -> super::EndpointFuture<'a, DirectResponse>;
    fn forward<'a>(
        &'a self,
        handle: &'a Arc<crate::oltp::mvcc::store::ParticipantHandle>,
        request: DirectRequest,
    ) -> super::EndpointFuture<'a, ForwardResponse>;
    /// The `direct` section of `/v1/runtime/stats`.
    fn stats(&self) -> serde_json::Value;
}

type Registry = Mutex<Vec<(usize, Weak<dyn DirectService>)>>;
fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}
fn resources_key(resources: &Arc<crate::oltp::pool::WorkerSharedResources>) -> usize {
    Arc::as_ptr(resources) as usize
}

/// Installs `service` for the worker that owns `resources`, replacing any
/// earlier one. The registry holds it weakly: it stays installed while the
/// caller keeps the `Arc`.
pub(crate) fn install(
    resources: &Arc<crate::oltp::pool::WorkerSharedResources>,
    service: &Arc<dyn DirectService>,
) {
    let key = resources_key(resources);
    let mut entries = registry().lock().unwrap_or_else(|p| p.into_inner());
    entries.retain(|(k, s)| *k != key && s.strong_count() > 0);
    entries.push((key, Arc::downgrade(service)));
}

/// The service installed for this worker, if any.
pub(crate) fn installed(
    resources: &Arc<crate::oltp::pool::WorkerSharedResources>,
) -> Option<Arc<dyn DirectService>> {
    let key = resources_key(resources);
    registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, s)| s.upgrade())
}

type Replicas = Mutex<Vec<(usize, Weak<crate::oltp::mvcc::store::ParticipantHandle>)>>;
fn replicas() -> &'static Replicas {
    static REPLICAS: OnceLock<Replicas> = OnceLock::new();
    REPLICAS.get_or_init(Default::default)
}

/// Records a local Data replica this worker serves on the service lane, so
/// the executor's expiry task visits every range this worker leads, not
/// only the ranges that received a direct request since it started (T5
/// concern 3). Held weakly: a replica leaves when its endpoint is dropped.
pub(crate) fn register_replica(handle: &Arc<crate::oltp::mvcc::store::ParticipantHandle>) {
    if !matches!(handle.identity().kind, GroupKind::Data { .. }) || handle.direct_clock().is_err() {
        return;
    }
    let key = resources_key(handle.resources());
    let mut entries = replicas().lock().unwrap_or_else(|p| p.into_inner());
    entries.retain(|(_, replica)| replica.strong_count() > 0);
    entries.push((key, Arc::downgrade(handle)));
}

/// The live local Data replicas registered for this worker.
pub(crate) fn registered_replicas(
    resources: &Arc<crate::oltp::pool::WorkerSharedResources>,
) -> Vec<Arc<crate::oltp::mvcc::store::ParticipantHandle>> {
    let key = resources_key(resources);
    replicas()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(k, _)| *k == key)
        .filter_map(|(_, replica)| replica.upgrade())
        .collect()
}

/// Who may send `Method::Direct`: an enrolled API ingress of this database
/// (checked against the Control group, where its enrollment lives) or the
/// current queue owner, or an advertised worker acting as gateway with
/// `DirectLeader` once `data.direct.v1` is active (T6).
pub(super) fn allowed(header: &super::Header) -> bool {
    use crate::oltp_replication::authority::control::ServiceAction;
    matches!(
        header.action,
        ServiceAction::ApiIngress | ServiceAction::Owner | ServiceAction::DirectLeader
    )
}

pub(super) async fn authorize(
    control: &Arc<crate::oltp_replication::authority::control::ControlClient>,
    session: &super::AuthenticatedSession,
    header: &super::Header,
) -> Result<(), ProtocolError> {
    use crate::oltp_replication::authority::control::ServiceAction;
    if header.action != ServiceAction::ApiIngress {
        return super::authorize(control, session, header).await;
    }
    let scope = crate::oltp_runtime::replica::QueueScope {
        cluster_id: session.group().cluster_id,
        database: session.group().database.clone(),
    };
    let control_group = control.control_group();
    // Cached like the stable roles (T6 concern 4): one Control read per
    // API process per `GRANT_TTL`, not per request. Kept warm while the API
    // sends direct requests (RD2), so an idle API's next request does not
    // wait for a Control read. AZ2: ask for the Data group first; previous
    // Control builds only accept the Control group as the ApiIngress target.
    let key = super::GrantKey::direct_ingress(session.remote(), control_group);
    let target = session.group().clone();
    control
        .service_grants()
        .role_warm(
            key,
            control.applied_revision(),
            || async {
                let caller = session.remote();
                match control
                    .service_role(&scope, caller, header.action, target.clone())
                    .await
                {
                    Err(error) if error.code == ErrorCode::Unauthorized => control
                        .service_role(&scope, caller, header.action, control_group.clone())
                        .await
                        .map(Some),
                    grant => grant.map(Some),
                }
            },
            || {
                super::warm_role_or(
                    control,
                    scope.clone(),
                    session.remote(),
                    header.action,
                    target.clone(),
                    control_group.clone(),
                )
            },
        )
        .await
}

/// Serves `Method::Direct` / `Method::DirectForward` on a local replica.
pub(super) async fn serve(
    handle: &Arc<crate::oltp::mvcc::store::ParticipantHandle>,
    session: &super::AuthenticatedSession,
    header: DirectHeader,
    forward: bool,
    sections: &super::ChargedSections,
) -> Result<ChargedBytes, ProtocolError> {
    let body = sections.section(0).ok_or_else(invalid_request)?.clone();
    let request = DirectRequest::received(session.group(), header, body)?;
    let response = match installed(handle.resources()) {
        None => DirectResponse::Fallback {
            reason: FallbackReason::Inactive,
        },
        Some(service) if forward => service.forward(handle, request).await?,
        Some(service) => service.execute(handle, request).await?,
    };
    handle
        .resources()
        .encode_in(
            crate::oltp::pool::ByteClass::Data,
            &response,
            crate::protocol::MAX_BODY_BYTES,
        )
        .map_err(|error| match response {
            DirectResponse::Done(_)
            | DirectResponse::Failed(ProtocolError {
                outcome: Outcome::Unknown,
                ..
            }) => super::mutation_uncertain(error),
            _ => error,
        })
}

/// Why a gateway's send produced no `DirectResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DirectSendError {
    /// Refused before the receiver could execute anything: the local lane
    /// was full, the request could not be encoded, or the receiver refused
    /// it typed with `not_applied` (authorization, a full lane, overload).
    NotApplied(ProtocolError),
    /// The request may have reached the leader and executed: the session
    /// failed after the request was written, or the receiver failed with an
    /// unknown outcome. The gateway resends the same id and body.
    Uncertain(ProtocolError),
}

/// The gateway side of `Method::Direct` for one Data group (plan section 3):
/// sends a request to one voter over the worker's persistent session on the
/// ingress lane and returns the leader's answer. It does not retry; the
/// gateway decides that from the answer.
pub(crate) struct DirectLink {
    client: super::Client,
}
impl DirectLink {
    /// `action` is the caller's role: `ApiIngress` for an enrolled API,
    /// `DirectLeader` (T7's role, granted once `data.direct.v1` is active)
    /// for a worker.
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<crate::oltp::pool::WorkerSharedResources>,
        transport: super::WorkerClient,
        control: Arc<crate::oltp_replication::authority::control::ControlClient>,
        action: crate::oltp_replication::authority::control::ServiceAction,
    ) -> Result<Self, ProtocolError> {
        if !matches!(group.kind, GroupKind::Data { .. }) {
            return Err(invalid_request());
        }
        Ok(Self {
            client: super::Client::new(group, shared, transport, control, action)?,
        })
    }
    /// Sends `request` to replica `node` on `worker`. `forward` sends
    /// `Method::DirectForward` instead of `Method::Direct`.
    pub(crate) async fn send(
        &self,
        node: u64,
        worker: Uuid,
        request: &DirectRequest,
        forward: bool,
    ) -> Result<DirectResponse, DirectSendError> {
        use super::{CallError, Method, Outgoing, RequestError, RequestTarget};
        if request.group != self.client.group.group_id
            || request.database != self.client.group.database
        {
            return Err(DirectSendError::NotApplied(invalid_request()));
        }
        let by_outcome = |error: ProtocolError| {
            if error.outcome == Outcome::NotApplied {
                DirectSendError::NotApplied(error)
            } else {
                DirectSendError::Uncertain(error)
            }
        };
        let _timer = crate::oltp_runtime::stage_timing::perf_stage!("gw.direct.send").timer();
        // PERF1: wait for a slot on the leader's lane instead of refusing a
        // request that finds it full. The lane keeps its bound (the leader's
        // handler slots); requests over it queue here, in arrival order, for
        // at most the request's own deadline, which the gateway set below its
        // deadline. Nothing is written while waiting, so a timeout stays
        // `not_applied`, and the leader gets only the time left. RD3: a
        // leader whose session negotiated direct admission takes up to
        // `direct_handler_slots` (its executor's in-flight bound) at once.
        let start = nabedra_rt::time::now();
        let reservation = {
            let _timer =
                crate::oltp_runtime::stage_timing::perf_stage!("gw.direct.lane_wait").timer();
            self.client
                .transport
                .reserve_outgoing(
                    &self.client.group,
                    worker,
                    super::Purpose::ApiIngress,
                    start + request.deadline,
                )
                .await
        };
        // Only local refusals come from here: a full lane, or a charge.
        let reservation = reservation.map_err(|error| by_outcome(error.into()))?;
        let mut header = request.header();
        let left = request.deadline.saturating_sub(nabedra_rt::time::elapsed(start));
        if left.is_zero() {
            return Err(DirectSendError::NotApplied(
                reservation.deadline_refusal().into(),
            ));
        }
        header.deadline_ms = u32::try_from(left.as_millis()).unwrap_or(u32::MAX).max(1);
        let method = if forward {
            Method::DirectForward(header)
        } else {
            Method::Direct(header)
        };
        let payload = Some(request.body.clone());
        match self
            .client
            .call_member_with_outgoing(
                self.client.action,
                method,
                &payload,
                node,
                RequestTarget::WorkerId(worker),
                None,
                Outgoing::Reserved(reservation),
            )
            .await
        {
            Ok(Ok(response)) => response
                .reply
                .decode_section::<DirectResponse>(0)
                .map(|decoded| (*decoded).clone())
                // A reply that does not decode came after execution.
                .map_err(DirectSendError::Uncertain),
            Ok(Err(error)) => Err(by_outcome(error)),
            // Local failures before sending keep `not_applied`; those after
            // the leader answered carry an unknown outcome.
            Err(CallError::Local(error)) => Err(by_outcome(error)),
            Err(CallError::Transport(error @ RequestError::OutgoingLaneFull { .. })) => {
                Err(DirectSendError::NotApplied(error.into()))
            }
            Err(CallError::Transport(
                RequestError::Protocol(error) | RequestError::PeerRejected { error, .. },
            )) => Err(by_outcome(error)),
            Err(CallError::Transport(RequestError::Unreachable)) => {
                Err(DirectSendError::Uncertain(RequestError::Unreachable.into()))
            }
        }
    }
}

fn invalid_request() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid direct request",
        Outcome::NotApplied,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn o02_direct_response_round_trips_and_keeps_exact_data_bytes() {
        let cases = [
            DirectResponse::Done(DirectData::from_json(r#"{"b":1.0,"a":[1,2]}"#).unwrap()),
            DirectResponse::Failed(ProtocolError::new(
                ErrorCode::Conflict,
                "request id reused",
                Outcome::NotApplied,
            )),
            DirectResponse::Fallback {
                reason: FallbackReason::SplitInProgress,
            },
            DirectResponse::Fallback {
                reason: FallbackReason::Forwarded,
            },
            DirectResponse::StaleRoute {
                schema_epoch: Some(3),
                routing_epoch: None,
            },
            DirectResponse::NotLeader { hint: Some(2) },
            DirectResponse::overloaded("full"),
        ];
        for case in cases {
            let encoded = serde_json::to_vec(&case).unwrap();
            let decoded: DirectResponse = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, case);
        }
        let done = serde_json::to_string(&DirectResponse::Done(
            DirectData::from_json(r#"{"b":1.0,"a":[1,2]}"#).unwrap(),
        ))
        .unwrap();
        assert_eq!(done, r#"{"done":{"b":1.0,"a":[1,2]}}"#, "data bytes kept");
        assert!(
            serde_json::from_str::<DirectResponse>(r#"{"fallback":{"reason":"new_reason"}}"#)
                .is_err(),
            "an unknown reason fails closed"
        );
        let names: std::collections::BTreeSet<_> =
            FallbackReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(names.len(), FallbackReason::ALL.len());
    }

    #[test]
    fn o02_direct_header_is_fixed_size_and_rejects_unknown_fields() {
        let header = DirectHeader {
            table_id: nabedra_rt::ids::uuid_v4(),
            schema_epoch: u64::MAX,
            routing_epoch: u64::MAX,
            request_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([0xab; 32]),
            deadline_ms: u32::MAX,
        };
        let encoded = serde_json::to_string(&header).unwrap();
        assert!(encoded.len() < 400, "{} header bytes", encoded.len());
        assert_eq!(
            serde_json::from_str::<DirectHeader>(&encoded).unwrap(),
            header
        );
        let extra = encoded.replacen('{', r#"{"body":"x","#, 1);
        assert!(serde_json::from_str::<DirectHeader>(&extra).is_err());
    }
}
