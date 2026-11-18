use super::{LocalRuntime, http_runtime::HttpRuntime, invalid, valid_database};
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

pub fn router(
    runtime: Arc<LocalRuntime>,
    token: String,
    database: String,
) -> Result<Router, ProtocolError> {
    super::require_standalone(&runtime.engine)?;
    runtime.engine.bind_database(&database)?;
    router_for(runtime, token, database)
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
    if let Err(error) = authorize(&request, &state) {
        return failure(None, error);
    }
    if !content_type(request.headers()) {
        return failure(None, invalid("Content-Type must be application/json"));
    }
    let admitted = match state.runtime.ingress().try_admit() {
        Ok(value) => value,
        Err(error) => return failure(None, error),
    };
    let operation = admitted.spawn(move || async move {
        let staging = state
            .runtime
            .scheduler()
            .charge_memory(
                state.runtime.scheduler_node_id(),
                (3 * protocol::MAX_BODY_BYTES + super::MAX_DECODED_REQUEST_BYTES) as u64,
            )
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "request parsing memory capacity exhausted",
                    Outcome::NotApplied,
                )
            })?;
        let bytes = body(request.into_body()).await?;
        let parsed = match protocol::parse_request(&bytes) {
            Ok(parsed) => parsed,
            Err(error) => return Ok((state, scan_request_id(&bytes), Err(error))),
        };
        let id = Some(parsed.request_id);
        drop(parsed);
        let record = state.runtime.clone().submit(bytes, staging).await;
        Ok((state, id, record))
    });
    let (state, id, record) = match operation {
        Ok(operation) => match operation.receive().await {
            Ok(value) => value,
            Err(error) => return failure(None, error),
        },
        Err(error) => return failure(None, error),
    };
    let mut record = match record {
        Ok(value) => value,
        Err(error) => return failure(id, error),
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
        Err(error) => failure(id, error),
    }
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
