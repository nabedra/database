use super::{http_runtime::HttpRuntime, invalid, valid_database};
use crate::{
    protocol::{self, ErrorCode, Outcome, ProtocolError},
    response::{self, EncodedResponse},
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use http_body_util::BodyExt;
use std::sync::Arc;
use uuid::Uuid;

struct ApiState {
    runtime: Arc<dyn HttpRuntime>,
    token: String,
    database: String,
    admin: Option<String>,
}

pub(crate) fn router_for(
    runtime: Arc<dyn HttpRuntime>,
    token: String,
    database: String,
) -> Result<Router, ProtocolError> {
    router_for_admin(runtime, token, database, None)
}
pub(crate) fn router_for_admin(
    runtime: Arc<dyn HttpRuntime>,
    token: String,
    database: String,
    admin: Option<String>,
) -> Result<Router, ProtocolError> {
    if admin
        .as_ref()
        .is_some_and(|admin| admin.is_empty() || admin == &token)
    {
        return Err(invalid("admin and data credentials must differ"));
    }
    if token.trim().is_empty() || !valid_database(&database) {
        return Err(invalid("invalid authentication configuration"));
    }
    let scheduler = runtime.scheduler();
    let scheduler_node = runtime.scheduler_node_id();
    let operation_capacity = Arc::new(tokio::sync::Semaphore::new(256));
    let submissions = Arc::new(tokio::sync::Semaphore::new(16));
    let receipts = Arc::new(tokio::sync::Semaphore::new(8));
    let administration = Arc::new(tokio::sync::Semaphore::new(2));
    Ok(Router::new()
        .route("/v1/operations", post(operations))
        .route("/v1/requests", post(submit))
        .route("/v1/requests/{run_id}/{request_id}", get(receipt))
        .route("/v1/cluster/operations", post(unsupported))
        .route("/v1/runtime/stats", get(stats))
        .route("/health/live", get(|| async { "live" }))
        .route("/health/ready", get(health))
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let scheduler = scheduler.clone();
                let budget = if request.uri().path() == "/v1/cluster/operations" {
                    administration.clone()
                } else if request.uri().path().starts_with("/v1/requests/") {
                    receipts.clone()
                } else if request.uri().path() == "/v1/requests" {
                    submissions.clone()
                } else {
                    operation_capacity.clone()
                };
                async move {
                    let Ok(_permit) = budget.try_acquire_owned() else {
                        return failure(
                            None,
                            ProtocolError::new(
                                ErrorCode::Overloaded,
                                "HTTP request capacity exhausted",
                                Outcome::NotApplied,
                            ),
                        );
                    };
                    let response = next.run(request).await;
                    let bytes = <Body as hyper::body::Body>::size_hint(response.body())
                        .exact()
                        .unwrap_or(protocol::MAX_BODY_BYTES as u64);
                    match scheduler.charge_memory(scheduler_node, bytes) {
                        Ok(charge) => {
                            let (parts, body) = response.into_parts();
                            Response::from_parts(
                                parts,
                                Body::new(ChargedBody {
                                    body,
                                    _charge: charge,
                                }),
                            )
                        }
                        Err(_) => failure(
                            None,
                            ProtocolError::new(
                                ErrorCode::Overloaded,
                                "HTTP response memory capacity exhausted; observe original receipt",
                                Outcome::Unknown,
                            ),
                        ),
                    }
                }
            },
        ))
        .with_state(Arc::new(ApiState {
            runtime,
            token,
            database,
            admin,
        })))
}

fn authorized(headers: &HeaderMap, state: &ApiState) -> bool {
    authorized_as(headers, state, &state.token)
}
fn authorized_as(headers: &HeaderMap, state: &ApiState, expected: &str) -> bool {
    authorize_database(headers, expected, &state.database)
}
pub(super) fn authorize_database(headers: &HeaderMap, expected: &str, database: &str) -> bool {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let auth = values
        .next()
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split_once(' ').is_some_and(|(scheme, token)| {
                scheme.eq_ignore_ascii_case("Bearer") && token == expected
            })
        })
        && values.next().is_none();
    let mut selectors = headers.get_all("x-nabedra-database").iter();
    let selected = selectors
        .next()
        .is_none_or(|v| v.to_str().ok() == Some(database));
    auth && selected
        && selectors.next().is_none()
        && !headers.keys().any(|name| {
            name.as_str().starts_with("x-nabedra-") && name.as_str() != "x-nabedra-database"
        })
}
fn authorize(request: &Request, state: &ApiState) -> Result<(), ProtocolError> {
    if !authorized(request.headers(), state) {
        return Err(ProtocolError::new(
            ErrorCode::Unauthorized,
            "unauthorized",
            Outcome::NotApplied,
        ));
    }
    if request.uri().query().is_some() {
        return Err(invalid("query parameters are unsupported"));
    }
    Ok(())
}
fn content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    values
        .next()
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<mime::Mime>().ok())
        .is_some_and(|v| {
            v.type_() == mime::APPLICATION && v.subtype() == mime::JSON && v.suffix().is_none()
        })
        && values.next().is_none()
}
/// A single valid `Content-Length` within the body limit.
fn declared_length(headers: &HeaderMap) -> Option<usize> {
    let mut values = headers.get_all(header::CONTENT_LENGTH).iter();
    let length = values.next()?.to_str().ok()?.parse::<usize>().ok()?;
    (values.next().is_none() && length <= protocol::MAX_BODY_BYTES).then_some(length)
}
async fn body(body: Body) -> Result<Vec<u8>, ProtocolError> {
    bounded_body(body, protocol::MAX_BODY_BYTES).await
}
async fn bounded_body(mut body: Body, maximum: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while let Some(frame) = tokio::time::timeout_at(deadline, body.frame())
        .await
        .map_err(|_| invalid("request body read deadline exceeded"))?
    {
        let frame = frame.map_err(|_| invalid("request body could not be read"))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > maximum - bytes.len() {
                return Err(ProtocolError::new(
                    ErrorCode::LimitExceeded,
                    "request body exceeds 8 MiB",
                    Outcome::NotApplied,
                ));
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}
async fn operations(State(state): State<Arc<ApiState>>, request: Request) -> Response {
    dispatch(state, request, false).await
}
async fn submit(State(state): State<Arc<ApiState>>, request: Request) -> Response {
    dispatch(state, request, true).await
}
async fn dispatch(state: Arc<ApiState>, request: Request, asynchronous: bool) -> Response {
    let _timer = super::stage_timing::perf_stage!("http.dispatch.total").timer();
    let spawned = std::time::Instant::now();
    if let Err(error) = authorize(&request, &state) {
        return failure(None, error);
    }
    if !content_type(request.headers()) {
        return failure(None, invalid("Content-Type must be application/json"));
    }
    // While `data.direct.v1` is active, a request with a declared length is
    // charged twice that length; the queued path still charges its full
    // parsing budget before it decodes anything (plan section 6).
    let direct = state
        .runtime
        .direct_gateway()
        .filter(|gateway| gateway.mode() != super::application::direct::DirectMode::Off)
        .zip(declared_length(request.headers()));
    // PERF1: a routed request waits on its leader, not on this process, so it
    // is admitted on its own lane instead of the queued lane's two workers.
    let lane = if direct.is_some() {
        state.runtime.direct_ingress()
    } else {
        state.runtime.ingress()
    };
    let admitted = match lane.try_admit() {
        Ok(value) => value,
        Err(error) => return failure(None, error),
    };
    let operation = admitted.spawn(move || async move {
        super::stage_timing::perf_stage!("http.dispatch.spawn_wait").since(spawned);
        let full = (3 * protocol::MAX_BODY_BYTES + super::MAX_DECODED_REQUEST_BYTES) as u64;
        let mut staging = parsing_charge(
            &state,
            direct
                .as_ref()
                .map_or(full, |(_, length)| 2 * *length as u64),
        )?;
        let read = std::time::Instant::now();
        let bytes = match &direct {
            Some((_, length)) => bounded_body(request.into_body(), *length).await?,
            None => body(request.into_body()).await?,
        };
        super::stage_timing::perf_stage!("http.dispatch.body").since(read);
        let Some((gateway, _)) = direct else {
            return Ok(enqueue(state, bytes, staging).await);
        };
        use super::application::direct::Routed;
        match super::stage_timing::timed(
            super::stage_timing::perf_stage!("gw.route"),
            gateway.route(&bytes, asynchronous),
        )
        .await
        {
            Ok(Routed::Done(encoded)) => Ok((state, None, Err(Ok(encoded)))),
            Err(error) => Ok((state, scan_request_id(&bytes), Err(Err(error)))),
            Ok(Routed::Queued) => {
                // The queued remainder keeps the queued lane's bound. Nothing
                // has been applied yet (the gateway answered `Queued`), so a
                // full lane is a `not_applied` refusal like any other.
                let queued = state.runtime.ingress().try_admit()?;
                drop(staging);
                staging = parsing_charge(&state, full)?;
                queued
                    .spawn(move || async move { Ok(enqueue(state, bytes, staging).await) })?
                    .receive()
                    .await
            }
        }
    });
    let (state, id, record) = match operation {
        Ok(operation) => match operation.receive().await {
            Ok(value) => value,
            Err(error) => return failure(None, error),
        },
        Err(error) => return failure(None, error),
    };
    let mut record = match record {
        Ok(Ok(value)) => value,
        Ok(Err(error)) | Err(Err(error)) => return failure(id, error),
        // Answered on the direct path.
        Err(Ok(encoded)) => return complete(encoded),
    };
    // The RAM queue owns the complete body. HTTP waiters retain only identity.
    record.body = Vec::new();
    record.pinned_catalog = None;
    record.response_body = None;
    if asynchronous {
        return complete(EncodedResponse {
            status: 202,
            body: serde_json::to_vec(&serde_json::json!({
                "request_id": record.request_id, "run_id": record.run_id, "state": "queued"
            }))
            .expect("fixed receipt JSON"),
        });
    }
    match state.runtime.wait(&record).await {
        Ok(value) => complete(value),
        Err(error) => failure(id, accepted_failure(error)),
    }
}

/// What a dispatch operation returns: the state, the request id when known,
/// and either the queued path's record or a direct answer.
type Dispatched = (
    Arc<ApiState>,
    Option<Uuid>,
    Result<
        Result<crate::request_queue::RequestRecord, ProtocolError>,
        Result<EncodedResponse, ProtocolError>,
    >,
);

fn parsing_charge(
    state: &ApiState,
    bytes: u64,
) -> Result<crate::scheduling::MemoryCharge, ProtocolError> {
    state
        .runtime
        .scheduler()
        .charge_memory(state.runtime.scheduler_node_id(), bytes)
        .map_err(|_| {
            ProtocolError::new(
                ErrorCode::Overloaded,
                "request parsing memory capacity exhausted",
                Outcome::NotApplied,
            )
        })
}

/// The queued path: parse, then install the request in the RAM queue.
async fn enqueue(
    state: Arc<ApiState>,
    bytes: Vec<u8>,
    staging: crate::scheduling::MemoryCharge,
) -> Dispatched {
    let parsed = match protocol::parse_request(&bytes) {
        Ok(parsed) => parsed,
        Err(error) => return (state, scan_request_id(&bytes), Ok(Err(error))),
    };
    let id = Some(parsed.request_id);
    let point =
        super::application::PointRequest::from_operation(parsed.request_id, &parsed.operation);
    drop(parsed);
    let record = state.runtime.clone().submit(bytes, staging, point).await;
    (state, id, Ok(record))
}

/// The queue accepted the request, so a failed wait never shows that it was not
/// applied: a refusal here (for example a full outgoing lane on an observation
/// call) concerns the observation only (G1). The request's own terminal errors
/// arrive as a completed response, not through this path.
fn accepted_failure(error: ProtocolError) -> ProtocolError {
    if error.outcome == Outcome::Unknown {
        return error;
    }
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        format!("request accepted; outcome unavailable: {}", error.message),
        Outcome::Unknown,
    )
}

async fn receipt(
    State(state): State<Arc<ApiState>>,
    Path((run, id)): Path<(String, String)>,
    request: Request,
) -> Response {
    if let Err(error) = authorize(&request, &state) {
        return failure(None, error);
    }
    let (run, id) = match (Uuid::parse_str(&run), Uuid::parse_str(&id)) {
        (Ok(run), Ok(id)) => (run, id),
        _ => return failure(None, invalid("receipt identities must be UUIDs")),
    };
    // Receipt/control work has independent admission and does not queue behind
    // requests waiting for execution. Each reply is bounded by the wire encoder.
    let observed = tokio::time::timeout(
        super::RECEIPT_OBSERVATION_TIMEOUT,
        state.runtime.receipt(run, id),
    )
    .await
    .unwrap_or_else(|_| {
        Err(ProtocolError::new(
            ErrorCode::OutcomeUnknown,
            "original receipt observation deadline exceeded",
            Outcome::Unknown,
        ))
    });
    match observed {
        Ok(receipt) => match serde_json::to_vec(&receipt.receipt) {
            Ok(body) if body.len() <= protocol::MAX_BODY_BYTES => {
                complete(EncodedResponse { status: 200, body })
            }
            _ => failure(
                Some(id),
                ProtocolError::new(
                    ErrorCode::LimitExceeded,
                    "receipt body exceeds 8 MiB",
                    Outcome::NotApplied,
                ),
            ),
        },
        Err(error) => failure(Some(id), error),
    }
}
async fn unsupported(State(state): State<Arc<ApiState>>, request: Request) -> Response {
    if let Some(admin) = &state.admin {
        if !authorized_as(request.headers(), &state, admin) {
            return failure(
                None,
                ProtocolError::new(ErrorCode::Unauthorized, "unauthorized", Outcome::NotApplied),
            );
        }
        if request.uri().query().is_some() || !content_type(request.headers()) {
            return failure(None, invalid("invalid administration request"));
        }
        let owner = match state.runtime.admin_memory() {
            Ok(owner) => owner,
            Err(error) => return failure(None, error),
        };
        let bytes = match bounded_body(request.into_body(), 64 << 10).await {
            Ok(bytes) => bytes,
            Err(error) => return failure(None, error),
        };
        let id = scan_request_id(&bytes);
        let result = state.runtime.clone().administer(bytes).await;
        drop(owner);
        return match result {
            Ok(encoded) => complete(encoded),
            Err(error) => failure(id, error),
        };
    }
    if let Err(error) = authorize(&request, &state) {
        return failure(None, error);
    }
    failure(
        None,
        ProtocolError::new(
            ErrorCode::UnsupportedOperation,
            "OLTP standalone has no remote workers, autoscaling or cluster administration",
            Outcome::NotApplied,
        ),
    )
}
async fn stats(State(state): State<Arc<ApiState>>, request: Request) -> Response {
    if let Err(error) = authorize(&request, &state) {
        return failure(None, error);
    }
    match state.runtime.stats().await {
        Ok(encoded) => complete(encoded),
        Err(error) => failure(None, error),
    }
}
async fn health(State(state): State<Arc<ApiState>>) -> Response {
    match state.runtime.health().await {
        Ok(encoded) => complete(encoded),
        Err(error) => failure(None, error),
    }
}
fn failure(id: Option<Uuid>, error: ProtocolError) -> Response {
    complete(response::encode_error(id, error))
}
fn complete(encoded: EncodedResponse) -> Response {
    let mut response = Response::new(Body::from(encoded.body));
    *response.status_mut() = StatusCode::from_u16(encoded.status).expect("protocol HTTP status");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
}

// Correlation metadata only; the authoritative parser receives original bytes.
fn scan_request_id(bytes: &[u8]) -> Option<Uuid> {
    use serde::de::{self, IgnoredAny, MapAccess, Visitor};
    struct Id;
    impl<'de> Visitor<'de> for Id {
        type Value = Option<Uuid>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut keys = std::collections::BTreeSet::new();
            let mut id = None;
            while let Some(key) = map.next_key::<String>()? {
                if !keys.insert(key.clone()) {
                    return Err(de::Error::custom("duplicate field"));
                }
                if key == "request_id" {
                    let value: String = map.next_value()?;
                    id = Some(Uuid::parse_str(&value).map_err(de::Error::custom)?);
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(id)
        }
    }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let id = serde::Deserializer::deserialize_map(&mut decoder, Id).ok()?;
    decoder.end().ok()?;
    id
}

// The transport retains its output charge through slow sockets and disconnects.
struct ChargedBody {
    body: Body,
    _charge: crate::scheduling::MemoryCharge,
}
impl hyper::body::Body for ChargedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        application::direct::{
            DirectGateway, DirectTransport, GatewaySwitch, SendFuture, TableFill, TopologySource,
            Voter,
        },
        http_runtime::DIRECT_INGRESS_SLOTS,
    };
    use super::*;
    use crate::oltp_replication::{
        service_rpc::direct::{DirectData, DirectRequest, DirectResponse},
        types::{GroupIdentity, GroupKind, Text},
    };
    use crate::{
        executor::Executor,
        oltp_replication::multiplex::{EndpointFuture, RequestError},
        request_queue::{RequestRecord, RequestState},
        scheduling::{MemoryCharge, ResourceScheduler},
    };
    use axum::http::Request as HttpRequest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt as _;

    const TOKEN: &str = "g1-data-token";
    const DATABASE: &str = "g1db";

    /// Accepts every request and fails the wait or the submission as told.
    struct Script {
        ingress: Executor,
        direct_ingress: Executor,
        direct: Option<Arc<DirectGateway>>,
        /// Submissions wait for a permit here when set.
        submit_gate: Option<Arc<tokio::sync::Semaphore>>,
        submits: AtomicUsize,
        scheduler: ResourceScheduler,
        submit: Result<(), ProtocolError>,
        wait: ProtocolError,
    }
    impl Script {
        fn new(submit: Result<(), ProtocolError>, wait: ProtocolError) -> Self {
            Self {
                ingress: Executor::new(2, 2).unwrap(),
                direct_ingress: Executor::new(DIRECT_INGRESS_SLOTS, 0).unwrap(),
                direct: None,
                submit_gate: None,
                submits: AtomicUsize::new(0),
                scheduler: ResourceScheduler::new(vec![(
                    0,
                    crate::config::WorkerResources::default(),
                )])
                .unwrap(),
                submit,
                wait,
            }
        }
    }
    impl HttpRuntime for Script {
        fn ingress(&self) -> &Executor {
            &self.ingress
        }
        fn direct_ingress(&self) -> &Executor {
            &self.direct_ingress
        }
        fn direct_gateway(&self) -> Option<Arc<DirectGateway>> {
            self.direct.clone()
        }
        fn scheduler(&self) -> ResourceScheduler {
            self.scheduler.clone()
        }
        fn scheduler_node_id(&self) -> u64 {
            0
        }
        fn submit(
            self: Arc<Self>,
            _body: Vec<u8>,
            _charge: MemoryCharge,
            _point: Option<super::super::application::PointRequest>,
        ) -> EndpointFuture<'static, RequestRecord> {
            Box::pin(async move {
                self.submits.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = &self.submit_gate {
                    let _permit = gate.acquire().await.unwrap();
                }
                self.submit.clone()?;
                Ok(RequestRecord {
                    context: None,
                    run_id: Uuid::new_v4(),
                    scheduler_term: 1,
                    membership_revision: 1,
                    request_id: Uuid::new_v4(),
                    digest: String::new(),
                    sequence: 1,
                    version: 1,
                    body: Vec::new(),
                    accepted: true,
                    state: RequestState::Queued,
                    pinned_catalog: None,
                    response_status: None,
                    response_body: None,
                    completed_at_ms: None,
                    result_expired: false,
                    replicas: Default::default(),
                })
            })
        }
        fn receipt(
            &self,
            _run: Uuid,
            _id: Uuid,
        ) -> EndpointFuture<'_, super::super::http_runtime::OwnedReceipt> {
            Box::pin(async { Err(self.wait.clone()) })
        }
        fn wait<'a>(&'a self, _record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
            Box::pin(async { Err(self.wait.clone()) })
        }
        fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
            Box::pin(async { Err(self.wait.clone()) })
        }
    }

    async fn post(submit: Result<(), ProtocolError>, wait: ProtocolError) -> (u16, ProtocolError) {
        let runtime = Arc::new(Script::new(submit, wait));
        let app = router_for(runtime, TOKEN.into(), DATABASE.into()).unwrap();
        let body = format!(
            r#"{{"request_id":"{}","operation":"table.list","payload":{{}}}}"#,
            Uuid::new_v4()
        );
        let reply = app
            .oneshot(
                HttpRequest::post("/v1/operations")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = reply.status().as_u16();
        let bytes = axum::body::to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
            .await
            .unwrap();
        let reply: protocol::ErrorResponse = serde_json::from_slice(&bytes).unwrap();
        (status, reply.error)
    }

    /// G1: a full outgoing lane met while observing an accepted request (the
    /// NS11 queued-path 429 whose update had committed) is an unknown outcome.
    #[tokio::test]
    async fn g1_lane_full_after_acceptance_reports_outcome_unknown() {
        let lane_full = ProtocolError::from(RequestError::OutgoingLaneFull);
        assert_eq!(lane_full.outcome, Outcome::NotApplied);
        let (status, error) = post(Ok(()), lane_full.clone()).await;
        assert_eq!(status, 503, "{error:?}");
        assert_eq!(error.code, ErrorCode::OutcomeUnknown);
        assert_eq!(error.outcome, Outcome::Unknown);
        assert!(
            error.message.contains("worker outgoing lane full"),
            "{error:?}"
        );

        // Any other not_applied refusal of the observation is the same.
        let invalid_reply = ProtocolError::new(
            ErrorCode::ValidationError,
            "invalid authenticated API request",
            Outcome::NotApplied,
        );
        let (status, error) = post(Ok(()), invalid_reply).await;
        assert_eq!(
            (status, error.code, error.outcome),
            (503, ErrorCode::OutcomeUnknown, Outcome::Unknown)
        );

        // An already uncertain failure is kept as it is.
        let unknown = ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "worker transport unavailable",
            Outcome::Unknown,
        );
        let (status, error) = post(Ok(()), unknown.clone()).await;
        assert_eq!((status, error), (503, unknown));

        // A refusal before the queue accepted the request stays not_applied.
        let (status, error) = post(Err(lane_full.clone()), lane_full.clone()).await;
        assert_eq!((status, error), (429, lane_full));
    }

    /// One Data range over every key, led by node 1.
    struct OneRange {
        fill: TableFill,
    }
    impl TopologySource for OneRange {
        fn table(&self, _table_id: Uuid) -> EndpointFuture<'_, Option<TableFill>> {
            let fill = self.fill.clone();
            Box::pin(async move { Ok(Some(fill)) })
        }
        fn placement<'a>(
            &'a self,
            _group: &'a GroupIdentity,
            _refresh: bool,
        ) -> EndpointFuture<'a, Vec<Voter>> {
            Box::pin(async {
                Ok(vec![Voter {
                    node_id: 1,
                    worker_id: Uuid::nil(),
                }])
            })
        }
    }
    /// A leader that holds every request until `release` has a permit, the
    /// way a real one holds it for its Raft commit.
    struct SlowLeader {
        in_flight: AtomicUsize,
        release: tokio::sync::Semaphore,
    }
    impl DirectTransport for SlowLeader {
        fn send<'a>(
            &'a self,
            _group: &'a GroupIdentity,
            _voter: Voter,
            _request: &'a DirectRequest,
            _forward: bool,
        ) -> SendFuture<'a> {
            Box::pin(async move {
                self.in_flight.fetch_add(1, Ordering::SeqCst);
                self.release.acquire().await.unwrap().forget();
                Ok(DirectResponse::Done(DirectData::from_json("true").unwrap()))
            })
        }
    }

    fn gateway(leader: Arc<SlowLeader>, table: Uuid) -> Arc<DirectGateway> {
        let switch = Arc::new(GatewaySwitch::default());
        switch.set_active(true);
        let fill = TableFill {
            table_id: table,
            key_column: "id".into(),
            key_type: crate::protocol::ColumnType::Int64,
            schema_epoch: 1,
            shards: vec![crate::oltp::mvcc::catalog::ShardRoute {
                group: GroupIdentity {
                    cluster_id: Uuid::new_v4(),
                    database: Text::try_from(DATABASE).unwrap(),
                    group_id: Uuid::new_v4(),
                    kind: GroupKind::Data {
                        table_id: table,
                        shard_id: Uuid::new_v4(),
                    },
                    group_format_version: 1,
                },
                span: crate::oltp::mvcc::store::KeySpan {
                    start: None,
                    end: None,
                },
                schema_epoch: 1,
                routing_epoch: 1,
            }],
        };
        let shared = crate::oltp::pool::WorkerSharedResources::new(
            2,
            crate::config::WorkerResources {
                max_threads: 4,
                ..Default::default()
            },
            crate::oltp::pool::SharedResourceConfig::default(),
        )
        .unwrap();
        DirectGateway::new(
            shared,
            Text::try_from(DATABASE).unwrap(),
            switch,
            Arc::new(OneRange { fill }),
            leader,
        )
    }

    async fn send(app: axum::Router, body: String) -> (u16, serde_json::Value) {
        let reply = app
            .oneshot(
                HttpRequest::post("/v1/operations")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/json")
                    .header("content-length", body.len().to_string())
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = reply.status().as_u16();
        let bytes = axum::body::to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    /// PERF1 (BENCH2 limit 1; O03, D01): direct requests waiting on their
    /// leader's commit do not hold the queued lane's two workers. Before the
    /// fix, two in flight held both workers, two more waited and the fifth
    /// got 429 `executor admission capacity exhausted`, so a 12 ms commit
    /// capped updates near 170/s and 2 x the commit time at any rate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn perf1_direct_requests_waiting_on_the_leader_do_not_hold_the_queued_lane() {
        let table = Uuid::new_v4();
        let leader = Arc::new(SlowLeader {
            in_flight: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let mut script = Script::new(Ok(()), ProtocolError::from(RequestError::Unreachable));
        script.direct = Some(gateway(leader.clone(), table));
        let app = router_for(Arc::new(script), TOKEN.into(), DATABASE.into()).unwrap();
        const REQUESTS: usize = 12;
        let requests: Vec<_> = (0..REQUESTS)
            .map(|key| {
                let body = serde_json::json!({
                    "request_id": Uuid::new_v4(),
                    "operation": "table.update",
                    "payload": {"table_id": table, "key": key, "values": {"value": 1}},
                })
                .to_string();
                tokio::spawn(send(app.clone(), body))
            })
            .collect();
        // Every request reaches the leader while none has been answered.
        let reached = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while leader.in_flight.load(Ordering::SeqCst) < REQUESTS {
                if let Some(done) = requests.iter().find(|task| task.is_finished()) {
                    let _ = done;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        let in_flight = leader.in_flight.load(Ordering::SeqCst);
        leader.release.add_permits(REQUESTS);
        let mut answers = Vec::new();
        for request in requests {
            answers.push(request.await.unwrap());
        }
        assert!(
            reached.is_ok(),
            "only {in_flight} of {REQUESTS} reached the leader"
        );
        assert_eq!(in_flight, REQUESTS, "{answers:?}");
        for (status, body) in answers {
            assert_eq!(status, 200, "{body}");
            assert_eq!(body["data"], serde_json::json!(true), "{body}");
        }
    }

    /// PERF1: a request admitted on the direct lane that the gateway sends
    /// queued takes a queued-lane slot too, so the queued path keeps its
    /// bound and refuses beyond it with `not_applied`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn perf1_direct_lane_fallback_keeps_the_queued_lane_bound() {
        let table = Uuid::new_v4();
        let leader = Arc::new(SlowLeader {
            in_flight: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut script = Script::new(Ok(()), ProtocolError::from(RequestError::Unreachable));
        script.direct = Some(gateway(leader.clone(), table));
        script.submit_gate = Some(gate.clone());
        // Two workers and no waiting room, so the two held requests fill it.
        script.ingress = Executor::new(2, 0).unwrap();
        let script = Arc::new(script);
        let app = router_for(script.clone(), TOKEN.into(), DATABASE.into()).unwrap();
        // `table.list` is not eligible: the gateway answers `Queued`.
        let list = || {
            format!(
                r#"{{"request_id":"{}","operation":"table.list","payload":{{}}}}"#,
                Uuid::new_v4()
            )
        };
        let held: Vec<_> = (0..2)
            .map(|_| tokio::spawn(send(app.clone(), list())))
            .collect();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while script.submits.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the queued lane fills");
        let (status, body) = send(app.clone(), list()).await;
        assert_eq!(status, 429, "{body}");
        assert_eq!(body["error"]["outcome"], "not_applied", "{body}");
        assert_eq!(
            body["error"]["message"], "executor admission capacity exhausted",
            "{body}"
        );
        assert_eq!(leader.in_flight.load(Ordering::SeqCst), 0);
        gate.add_permits(2);
        for request in held {
            let (status, body) = request.await.unwrap();
            assert_eq!(status, 503, "{body}");
        }
    }
}
