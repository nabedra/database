//! API ingress is a Control-enrolled service, never a queue holder or voter.
//! The receiving owner authenticates the original bearer and database again.
pub(crate) mod direct;
mod scoped;
use super::scoped_worker::ScopedWorker;
use super::{
    http_runtime::HttpRuntime,
    replica::{QueueAuthority, QueueScope, RecordVersion},
    worker_runtime::WorkerRuntime,
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, WorkerSharedResources},
    oltp_replication::{authority::control::ControlClient, multiplex::*, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::RequestRecord,
};
use scoped::ScopedRoutes;
pub(super) use scoped::{
    PointRequest, PointRouting, ScopedReceiptRoute, ScopedTarget, stale_route,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use uuid::Uuid;

// Keep small observations on the progress lane. Larger bodies use ApiIngress.
pub(crate) const INLINE_RECEIPT_BYTES: usize = 32 << 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationObservation {
    pub record: RequestRecord,
    pub response_hash: Option<Digest>,
    #[serde(default)]
    pub durable_proof: bool,
}
pub(crate) struct ObservedApplication {
    pub metadata: ApplicationObservation,
    pub body: Option<ChargedBytes>,
}

pub(super) struct ApplicationReadView {
    pub(super) authority: QueueAuthority,
    pub(super) terminal: Option<RecordVersion>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Method {
    Submit {
        bytes: u64,
    },
    Observe {
        run_id: Uuid,
        request_id: Uuid,
        #[serde(default)]
        include_result: bool,
    },
    Result {
        observed: Box<ApplicationObservation>,
    },
    RoutePoint {
        point: PointRequest,
        digest: Digest,
    },
    RouteReceipt {
        run_id: Uuid,
        request_id: Uuid,
    },
    /// The direct-path topology of a table (`direct::TableFill`). Sent only
    /// while `data.direct.v1` is active.
    DirectRoute {
        table_id: Uuid,
    },
    ScopedSubmit {
        bytes: u64,
        target: ScopedTarget,
    },
    ScopedObserve {
        target: ScopedTarget,
        run_id: Uuid,
        request_id: Uuid,
        digest: Digest,
    },
    ScopedResult {
        target: ScopedTarget,
        observed: Box<ApplicationObservation>,
    },
    Stats,
}
impl Method {
    fn purpose(&self) -> Purpose {
        match self {
            Self::Submit { .. }
            | Self::Result { .. }
            | Self::ScopedSubmit { .. }
            | Self::ScopedResult { .. } => Purpose::ApiIngress,
            _ => Purpose::ApiObservation,
        }
    }

    fn scoped_request(&self) -> Option<(ScopedTarget, Option<Uuid>)> {
        match self {
            Self::ScopedSubmit { target, .. } => Some((*target, None)),
            Self::ScopedObserve {
                target, request_id, ..
            } => Some((*target, Some(*request_id))),
            Self::ScopedResult { target, observed } => {
                Some((*target, Some(observed.record.request_id)))
            }
            _ => None,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    bearer: String,
    database: Text<128>,
    method: Method,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHeader {
    nonce: Uuid,
    bytes: u64,
    #[serde(default, skip_serializing_if = "zero")]
    inline_bytes: u64,
    error: Option<ProtocolError>,
}

fn zero(value: &u64) -> bool {
    *value == 0
}

pub(crate) struct ApplicationEndpoint {
    runtime: Weak<WorkerRuntime>,
    scoped: Option<Weak<ScopedWorker>>,
}
impl ApplicationEndpoint {
    pub(crate) fn new(runtime: &Arc<WorkerRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime: Arc::downgrade(runtime),
            scoped: runtime.scoped_worker().map(Arc::downgrade),
        })
    }

    pub(super) fn scoped(worker: &Arc<ScopedWorker>) -> Arc<Self> {
        Arc::new(Self {
            runtime: Weak::new(),
            scoped: Some(Arc::downgrade(worker)),
        })
    }

    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        if session.recovery_only() || bytes.len() > MAX_HEADER_BYTES {
            return Err(invalid());
        }
        crate::protocol::json_input(bytes, MAX_HEADER_BYTES).map_err(|_| invalid())?;
        let header: Header = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if session.purpose() != header.method.purpose() {
            return Err(invalid());
        }
        if header.method.scoped_request().is_some() {
            self.scoped_worker()?.authorize_application(
                session.group(),
                &header.bearer,
                &header.database,
            )?;
        } else {
            self.runtime
                .upgrade()
                .ok_or_else(unavailable)?
                .authorize_application(session.group(), &header.bearer, &header.database)?;
        }
        Ok(header)
    }

    fn scoped_worker(&self) -> Result<Arc<ScopedWorker>, ProtocolError> {
        self.scoped
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(stale_route)
    }

    fn finish_reply(
        shared: &WorkerSharedResources,
        session: &AuthenticatedSession,
        result: Result<(ChargedBytes, Option<ChargedBytes>), ProtocolError>,
    ) -> Result<ChargedReply, ProtocolError> {
        let (body, inline, error) = match result {
            Ok((body, inline)) => (Some(body), inline, None),
            Err(error) => (None, None, Some(error)),
        };
        let reply = ReplyHeader {
            nonce: session.nonce(),
            bytes: body.as_ref().map_or(0, |body| body.len() as u64),
            inline_bytes: inline.as_ref().map_or(0, |body| body.len() as u64),
            error,
        };
        Ok(ChargedReply {
            header: shared.encode_in(
                if session.purpose() == Purpose::ApiIngress {
                    ByteClass::Data
                } else {
                    ByteClass::Progress
                },
                &reply,
                MAX_HEADER_BYTES,
            )?,
            sections: [body, inline, None],
        })
    }
}
impl Endpoint for ApplicationEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let header = self.parse(session, bytes)?;
            let bytes = match header.method {
                Method::Submit { bytes } => {
                    validate_body_length(bytes)?;
                    let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
                    runtime.authorize_api(session.remote()).await?;
                    bytes
                }
                Method::ScopedSubmit { bytes, target } => {
                    validate_body_length(bytes)?;
                    self.scoped_worker()?
                        .authorize_ingress(session.remote(), target, None)
                        .await?;
                    bytes
                }
                _ => 0,
            };
            Ok(ReceivePlan {
                class: if session.purpose() == Purpose::ApiIngress {
                    TrafficClass::ApiIngress
                } else {
                    TrafficClass::ApiObservation
                },
                section_lengths: [bytes, 0, 0],
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
            let request = self.parse(&session, header.as_ref())?;
            if let Some((target, request_id)) = request.method.scoped_request() {
                let worker = self.scoped_worker()?;
                let shared = worker.resources();
                let result = async {
                    let placement = worker
                        .authorize_ingress(session.remote(), target, request_id)
                        .await?;
                    match request.method {
                        Method::ScopedSubmit { bytes, .. } => {
                            let body = request_body(bytes, &sections)?;
                            // T7c: settled at its anchor before any queue
                            // reserves it, as on the ready-set queue.
                            self.runtime
                                .upgrade()
                                .ok_or_else(unavailable)?
                                .settle_before_reserve(body.as_ref())
                                .await?;
                            let record = worker.submit(placement, body, session.remote()).await?;
                            Ok((shared.encode_in(ByteClass::Data, &record, 16 << 10)?, None))
                        }
                        Method::ScopedObserve {
                            run_id,
                            request_id,
                            digest,
                            ..
                        } => {
                            ensure_empty(&sections)?;
                            let observed = worker
                                .observe(placement, run_id, request_id, digest, true, None)
                                .await?;
                            Ok((
                                shared.encode_in(
                                    ByteClass::Progress,
                                    &observed.metadata,
                                    16 << 10,
                                )?,
                                observed.body,
                            ))
                        }
                        Method::ScopedResult { observed, .. } => {
                            ensure_empty(&sections)?;
                            let result = worker.result(placement, &observed).await?;
                            Ok((result, None))
                        }
                        _ => Err(invalid()),
                    }
                }
                .await;
                return Self::finish_reply(shared, &session, result);
            }
            let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
            // Bind the terminal version before the Control role check. A
            // confirmation posted during that check needs a fresh observation.
            let terminal = match &request.method {
                Method::Observe {
                    run_id, request_id, ..
                } => runtime.application_copy_confirmation(*run_id, *request_id)?,
                Method::Result { observed } => runtime.application_copy_confirmation(
                    observed.record.run_id,
                    observed.record.request_id,
                )?,
                _ => None,
            };
            // Every action checks current authority. Submit also checks before
            // it can reserve or transfer its request body.
            let view = ApplicationReadView {
                authority: runtime.authorize_api(session.remote()).await?,
                terminal,
            };
            let shared = runtime.resources().clone();
            let result: Result<(ChargedBytes, Option<ChargedBytes>), ProtocolError> = async {
                match request.method {
                    Method::Submit { bytes } => {
                        let body = request_body(bytes, &sections)?;
                        let record = runtime.submit_charged(body, session.remote()).await?;
                        Ok((shared.encode_in(ByteClass::Data, &record, 16 << 10)?, None))
                    }
                    Method::Observe {
                        run_id,
                        request_id,
                        include_result,
                    } => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        let observed = runtime
                            .application_observation(view, run_id, request_id, include_result)
                            .await?;
                        Ok((
                            shared.encode_in(ByteClass::Progress, &observed.metadata, 16 << 10)?,
                            observed.body,
                        ))
                    }
                    Method::Result { observed } => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        let result = runtime.application_result(view, &observed).await?;
                        Ok((result, None))
                    }
                    Method::Stats => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        let response = runtime.stats().await?;
                        Ok((
                            shared.copy_bytes_in(ByteClass::Progress, &response.body)?,
                            None,
                        ))
                    }
                    Method::RoutePoint { point, digest } => {
                        ensure_empty(&sections)?;
                        let route = runtime
                            .resolve_scoped_point(view.authority, &point, digest)
                            .await?;
                        Ok((
                            shared.encode_in(ByteClass::Progress, &route, 16 << 10)?,
                            None,
                        ))
                    }
                    Method::RouteReceipt { run_id, request_id } => {
                        ensure_empty(&sections)?;
                        let target = runtime
                            .resolve_scoped_receipt(view.authority, run_id, request_id)
                            .await?;
                        Ok((
                            shared.encode_in(ByteClass::Progress, &target, 16 << 10)?,
                            None,
                        ))
                    }
                    Method::DirectRoute { table_id } => {
                        ensure_empty(&sections)?;
                        let fill = runtime
                            .direct_table_fill(view.authority.ready.view.authority(), table_id)
                            .await?;
                        Ok((shared.encode_in(ByteClass::Progress, &fill, 1 << 20)?, None))
                    }
                    Method::ScopedSubmit { .. }
                    | Method::ScopedObserve { .. }
                    | Method::ScopedResult { .. } => Err(invalid()),
                }
            }
            .await;
            Self::finish_reply(&shared, &session, result)
        })
    }
}

pub(crate) struct ApplicationClient {
    shared: Arc<WorkerSharedResources>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
    group: GroupIdentity,
    bearer: String,
    scoped: Option<ScopedRoutes>,
    _metadata: crate::oltp::pool::ScratchLease,
}
impl ApplicationClient {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        group: GroupIdentity,
        bearer: String,
        scoped: bool,
    ) -> Result<Arc<Self>, ProtocolError> {
        if group.kind != GroupKind::Control || bearer.is_empty() || bearer.len() > 4096 {
            return Err(invalid());
        }
        let metadata = shared.reserve_scratch(
            ByteClass::Progress,
            bearer.capacity() + group.database.capacity_bytes() + 4096,
        )?;
        let scoped = scoped.then(|| ScopedRoutes::new(&shared)).transpose()?;
        Ok(Arc::new(Self {
            shared,
            transport,
            control,
            group,
            bearer,
            scoped,
            _metadata: metadata,
        }))
    }
    /// One Catalog read for the direct-path gateway, served by the owner.
    pub(crate) async fn direct_table(
        &self,
        table_id: Uuid,
    ) -> Result<Option<direct::TableFill>, ProtocolError> {
        let reply = self.call(Method::DirectRoute { table_id }, None).await?;
        Ok((*reply.decode_section::<Option<direct::TableFill>>(0)?).clone())
    }
    async fn call(
        &self,
        method: Method,
        body: Option<ChargedBytes>,
    ) -> Result<ReceivedReply, ProtocolError> {
        use super::worker_endpoint::AuthorityReader;
        let scope = QueueScope {
            cluster_id: self.group.cluster_id,
            database: self.group.database.clone(),
        };
        let current = self.control.current(&scope).await?;
        self.call_to(current.owner, method, body).await
    }

    async fn call_to(
        &self,
        worker: WorkerIdentity,
        method: Method,
        body: Option<ChargedBytes>,
    ) -> Result<ReceivedReply, ProtocolError> {
        let accepts_inline = matches!(
            method,
            Method::Observe {
                include_result: true,
                ..
            } | Method::ScopedObserve { .. }
        );
        let purpose = method.purpose();
        let submits = matches!(method, Method::Submit { .. } | Method::ScopedSubmit { .. });
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &Header {
                bearer: self.bearer.clone(),
                database: self.group.database.clone(),
                method,
            },
            MAX_HEADER_BYTES,
        )?;
        let reply = self
            .transport
            .request_classified(
                &self.group,
                worker,
                purpose,
                header,
                [body, None, None],
                32 << 10,
            )
            .await
            .map_err(ProtocolError::from)?;
        // The worker answered: a submission may be queued even when its reply
        // fails these checks (G1).
        let after_reply = |error: ProtocolError| {
            if submits {
                submission_unknown(error)
            } else {
                error
            }
        };
        if reply.remote_identity() != worker
            || reply.group() != &self.group
            || reply.purpose() != purpose
            || (!accepts_inline && reply.sections[1].is_some())
            || reply.sections[2].is_some()
        {
            return Err(after_reply(invalid()));
        }
        let observed = reply.decode_header::<ReplyHeader>().map_err(after_reply)?;
        if observed.nonce != reply.nonce()
            || observed.bytes != reply.sections[0].as_ref().map_or(0, |b| b.len() as u64)
            || observed.inline_bytes != reply.sections[1].as_ref().map_or(0, |b| b.len() as u64)
            || observed.inline_bytes > INLINE_RECEIPT_BYTES as u64
        {
            return Err(after_reply(invalid()));
        }
        if let Some(error) = &observed.error {
            return Err(error.clone());
        }
        drop(observed);
        Ok(reply)
    }
    async fn submit_legacy(&self, body: ChargedBytes) -> Result<RequestRecord, ProtocolError> {
        let reply = self
            .call(
                Method::Submit {
                    bytes: body.len() as u64,
                },
                Some(body),
            )
            .await?;
        Self::submitted_record(reply)
    }
    fn submitted_record(reply: ReceivedReply) -> Result<RequestRecord, ProtocolError> {
        let record = reply
            .decode_section::<RequestRecord>(0)
            .map_err(submission_unknown)?;
        if !record.accepted
            || !record.body.is_empty()
            || record.pinned_catalog.is_some()
            || record.response_body.is_some()
            || !record.replicas.is_empty()
        {
            return Err(submission_unknown(invalid()));
        }
        Ok(record.clone())
    }
    pub(crate) async fn observe(
        &self,
        run_id: Uuid,
        request_id: Uuid,
    ) -> Result<ObservedApplication, ProtocolError> {
        let reply = self
            .call_for_receipt(
                run_id,
                request_id,
                Method::Observe {
                    run_id,
                    request_id,
                    include_result: true,
                },
                |route| Method::ScopedObserve {
                    target: route.target,
                    run_id,
                    request_id,
                    digest: route.digest,
                },
            )
            .await?;
        let observed = reply.decode_section::<ApplicationObservation>(0)?;
        if observed.record.run_id != run_id
            || observed.record.request_id != request_id
            || Digest::parse(&observed.record.digest).is_err()
            || !observed.record.accepted
            || observed.record.result_expired
            || observed.record.context.is_some()
            || !observed.record.body.is_empty()
            || observed.record.pinned_catalog.is_some()
            || observed.record.response_body.is_some()
            || observed.record.state.terminal() != observed.response_hash.is_some()
            || observed.record.state.terminal() != observed.record.response_status.is_some()
            || (!observed.record.state.terminal() && reply.sections[1].is_some())
        {
            return Err(invalid());
        }
        Ok(ObservedApplication {
            metadata: observed.clone(),
            body: reply.sections[1].clone(),
        })
    }
    /// This process's local lane refusals.
    pub(crate) fn lane_refusals(&self) -> serde_json::Value {
        self.transport.lane_refusals()
    }
    pub(crate) async fn stats(&self) -> Result<ChargedBytes, ProtocolError> {
        let reply = self.call(Method::Stats, None).await?;
        reply.sections[0].as_ref().cloned().ok_or_else(invalid)
    }
    pub(crate) async fn result(
        &self,
        observed: ObservedApplication,
    ) -> Result<ChargedBytes, ProtocolError> {
        if !observed.metadata.record.state.terminal() {
            return Err(invalid());
        }
        let hash = observed.metadata.response_hash.ok_or_else(invalid)?;
        if let Some(body) = observed.body {
            verify_response_hash(&body, hash)?;
            return Ok(body);
        }
        let observed = observed.metadata;
        let reply = self
            .call_for_receipt(
                observed.record.run_id,
                observed.record.request_id,
                Method::Result {
                    observed: Box::new(observed.clone()),
                },
                |route| Method::ScopedResult {
                    target: route.target,
                    observed: Box::new(observed.clone()),
                },
            )
            .await?;
        let body = reply.sections[0].clone().ok_or_else(invalid)?;
        let held = body.clone();
        self.shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                verify_response_hash(&held, hash)
            })
            .await?;
        Ok(body)
    }
}

fn validate_body_length(bytes: u64) -> Result<(), ProtocolError> {
    if bytes == 0 || bytes > crate::protocol::MAX_BODY_BYTES as u64 {
        return Err(invalid());
    }
    Ok(())
}

fn request_body(bytes: u64, sections: &ChargedSections) -> Result<ChargedBytes, ProtocolError> {
    validate_body_length(bytes)?;
    let body = sections.section(0).ok_or_else(invalid)?;
    if body.len() as u64 != bytes || sections.section(1).is_some() || sections.section(2).is_some()
    {
        return Err(invalid());
    }
    Ok(body.clone())
}

fn ensure_empty(sections: &ChargedSections) -> Result<(), ProtocolError> {
    if (0..3).any(|index| sections.section(index).is_some()) {
        return Err(invalid());
    }
    Ok(())
}
fn verify_response_hash(body: &ChargedBytes, expected: Digest) -> Result<(), ProtocolError> {
    use sha2::{Digest as _, Sha256};
    if Digest::from_bytes(Sha256::digest(body.as_ref()).into()) != expected {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid authenticated API request",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker API authority unavailable",
        Outcome::Unknown,
    )
}
/// A submission the worker answered may be queued, whatever this side then
/// fails to check.
pub(super) fn submission_unknown(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}

#[cfg(test)]
#[test]
fn c07_m02_q04_observation_opt_in_checks_exact_bytes() {
    use sha2::{Digest as _, Sha256};

    let run = Uuid::new_v4();
    let id = Uuid::new_v4();
    let legacy = serde_json::json!({"observe": {"run_id": run, "request_id": id}});
    assert!(matches!(
        serde_json::from_value::<Method>(legacy).unwrap(),
        Method::Observe {
            include_result: false,
            ..
        }
    ));
    let header = ReplyHeader {
        nonce: run,
        bytes: 20,
        inline_bytes: 0,
        error: None,
    };
    assert!(
        serde_json::to_value(header)
            .unwrap()
            .get("inline_bytes")
            .is_none()
    );
    let shared = WorkerSharedResources::new(
        1,
        crate::config::WorkerResources::default(),
        crate::oltp::pool::SharedResourceConfig::default(),
    )
    .unwrap();
    let exact = shared
        .copy_bytes_in(ByteClass::Progress, b"result=2")
        .unwrap();
    let altered = shared
        .copy_bytes_in(ByteClass::Progress, b"result=3")
        .unwrap();
    let expected = Digest::from_bytes(Sha256::digest(exact.as_ref()).into());
    verify_response_hash(&exact, expected).unwrap();
    assert_eq!(
        verify_response_hash(&altered, expected).unwrap_err().code,
        ErrorCode::ValidationError
    );
}
