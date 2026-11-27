//! POST operations transport. Body polling, parsing, execution, encoding and
//! completion diagnostics all run inside the executor's owned operation.
mod control;
pub mod health;
mod queue;
pub mod response;
pub(crate) mod stream;

use crate::{
    engine::Engine,
    executor::Executor,
    protocol::{self, ErrorCode, MAX_BODY_BYTES, Outcome, ProtocolError},
    storage::ObjectStore,
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use http_body_util::BodyExt;
use response::{EncodedResponse, encode_error};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use std::{collections::BTreeSet, fmt, sync::Arc, time::Instant};
use uuid::Uuid;

struct ApiState<B: ObjectStore> {
    engine: Engine<B>,
    executor: Executor,
    token: String,
    queue: Arc<queue::LocalQueue>,
    streams: crate::node::stream::Local,
    stream_frontend: Arc<stream::Frontend>,
}

impl<B: ObjectStore + 'static> stream::Backend for ApiState<B> {
    const PROXY_BATCH_BYTES: usize = 0;

    fn frontend(&self) -> &stream::Frontend {
        &self.stream_frontend
    }

    fn scheduler(&self) -> &crate::scheduling::ResourceScheduler {
        self.engine.worker_pool().scheduler()
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<crate::storage::context::RequestContext>, crate::node::stream::wire::Error>
    {
        if headers.contains_key("x-nabedra-storage")
            || self.token.is_empty()
            || !crate::node::http::bearer(headers, &self.token)
        {
            return Err(not_applied(ErrorCode::Unauthorized, "unauthorized").into());
        }
        Ok(None)
    }

    async fn call(
        &self,
        request: crate::node::stream::wire::Request,
        _context: Option<crate::storage::context::RequestContext>,
    ) -> Result<crate::node::stream::wire::Reply, crate::node::stream::wire::Error> {
        let fence = self.queue.initialize(&self.engine).await?;
        self.streams.call(&self.engine, fence.run_id, request).await
    }
}

/// Build the local OLAP router over an already initialized engine.
/// Pass clones of ONE engine and executor; clones share their coordinator and
/// admission state. Retain the executor for stop/drain before runtime shutdown.
/// The returned stateless Router can be merged with R10's health routes. This
/// constructor performs no storage I/O, creates no runtime, and binds no listener.
/// The caller supplies its validated configured token. Never log this state.
pub fn router<B: ObjectStore + 'static>(
    engine: Engine<B>,
    executor: Executor,
    token: String,
) -> Router {
    router_configured(
        engine,
        executor,
        token,
        crate::config::ExecutionConfig::default(),
        false,
    )
    .expect("validated local queue configuration")
    .0
}
pub fn router_configured<B: ObjectStore + 'static>(
    engine: Engine<B>,
    executor: Executor,
    token: String,
    execution: crate::config::ExecutionConfig,
    cold_start: bool,
) -> Result<(Router, Executor), ProtocolError> {
    let (router, tasks, _queue) = router_with_queue_startup(
        engine,
        executor,
        token,
        execution,
        cold_start,
        Arc::new(stream::Frontend::new()?),
    )?;
    Ok((router, tasks))
}
pub(crate) fn router_with_queue_startup<B: ObjectStore + 'static>(
    engine: Engine<B>,
    executor: Executor,
    token: String,
    execution: crate::config::ExecutionConfig,
    cold_start: bool,
    stream_frontend: Arc<stream::Frontend>,
) -> Result<(Router, Executor, Arc<queue::LocalQueue>), ProtocolError> {
    let queue = queue::LocalQueue::new(&engine, execution, cold_start)?;
    let tasks = queue.executor();
    Ok((
        Router::new()
            .route("/v1/operations", post(operations::<B>))
            .route("/v1/cluster/operations", post(control::unsupported::<B>))
            .route("/v1/requests", post(submit_request::<B>))
            .route(
                "/v1/requests/{run_id}/{request_id}",
                get(receipt_request::<B>),
            )
            .merge(stream::routes::<ApiState<B>>())
            .with_state(Arc::new(ApiState {
                engine,
                executor,
                token,
                queue: queue.clone(),
                streams: crate::node::stream::Local::new(),
                stream_frontend,
            })),
        tasks,
        queue,
    ))
}

async fn operations<B: ObjectStore + 'static>(
    State(state): State<Arc<ApiState<B>>>,
    request: Request,
) -> Response {
    let started = nabedra_rt::time::now_std();
    if request.headers().contains_key("x-nabedra-storage")
        || !authenticated(request.headers(), &state.token)
    {
        return failure(
            None,
            None,
            started,
            not_applied(ErrorCode::Unauthorized, "unauthorized"),
        );
    }
    let admission = match state.executor.try_admit() {
        Ok(admission) => admission,
        Err(error) => return failure(None, None, started, error),
    };
    // No body extractor or preconstructed body future: even construction of work
    // is deferred until the executor has acquired an execution permit.
    let operation = match admission
        .spawn(move || async move { Ok(execute_request(state, request, started).await) })
    {
        Ok(operation) => operation,
        Err(error) => return failure(None, None, started, error),
    };
    match operation.receive().await {
        Ok(response) => response,
        // Join failures carry Unknown. No UUID is safely available here and no
        // assumption about whether a mutation committed is made.
        Err(error) => failure(None, None, started, error),
    }
}

fn authenticated(headers: &HeaderMap, token: &str) -> bool {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.split_once(' '))
        .is_some_and(|(scheme, supplied)| {
            scheme.eq_ignore_ascii_case("Bearer") && !supplied.is_empty() && supplied == token
        })
}

pub(crate) fn json_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<mime::Mime>().ok())
        .is_some_and(|value| {
            value.type_() == mime::APPLICATION
                && value.subtype() == mime::JSON
                && value.suffix().is_none()
        })
}

async fn execute_request<B: ObjectStore + 'static>(
    state: Arc<ApiState<B>>,
    request: Request,
    started: Instant,
) -> Response {
    if !json_content_type(request.headers()) {
        return failure(
            None,
            None,
            started,
            not_applied(
                ErrorCode::ValidationError,
                "Content-Type must be application/json",
            ),
        );
    }
    let _staging = match state.engine.worker_pool().scheduler().charge_memory(
        0,
        (3 * protocol::MAX_BODY_BYTES + protocol::MAX_DECODED_PAYLOAD_BYTES) as u64,
    ) {
        Ok(charge) => charge,
        Err(error) => return failure(None, None, started, error),
    };
    let bytes = match read_body(request.into_body()).await {
        Ok(bytes) => bytes,
        Err(error) => return failure(None, None, started, error),
    };
    // This scan is only correlation metadata. The authoritative parser always
    // receives the original bytes, including raw scalar spellings and duplicates.
    let request_id = scan_request_id(&bytes);
    let parsed = match protocol::parse_request(&bytes) {
        Ok(parsed) => parsed,
        Err(error) => return failure(request_id, None, started, error),
    };
    let request_id = Some(parsed.request_id);
    let name = Some(parsed.operation.name());
    drop(parsed);
    let mut record = match state.queue.submit(state.engine.clone(), bytes).await {
        Ok(r) => r,
        Err(error) => return failure(request_id, name, started, error),
    };
    record.body = Vec::new();
    record.pinned_catalog = None;
    record.response_body = None;
    drop(_staging);
    match state.queue.wait(&state.engine, &record).await {
        Ok(encoded) => complete(encoded, request_id, name, started, None),
        Err(error) => failure(request_id, name, started, error),
    }
}

async fn submit_request<B: ObjectStore + 'static>(
    State(state): State<Arc<ApiState<B>>>,
    request: Request,
) -> Response {
    let started = nabedra_rt::time::now_std();
    if request.headers().contains_key("x-nabedra-storage")
        || !authenticated(request.headers(), &state.token)
    {
        return failure(
            None,
            None,
            started,
            not_applied(ErrorCode::Unauthorized, "unauthorized"),
        );
    }
    if !json_content_type(request.headers()) {
        return failure(
            None,
            None,
            started,
            not_applied(
                ErrorCode::ValidationError,
                "Content-Type must be application/json",
            ),
        );
    }
    let admitted = match state.executor.try_admit() {
        Ok(admitted) => admitted,
        Err(error) => return failure(None, None, started, error),
    };
    let operation=admitted.spawn(move||async move{
        let _staging=state.engine.worker_pool().scheduler().charge_memory(0,(3*protocol::MAX_BODY_BYTES+protocol::MAX_DECODED_PAYLOAD_BYTES) as u64)?;
        let bytes=read_body(request.into_body()).await?;let id=scan_request_id(&bytes);
        if let Err(error)=protocol::parse_request(&bytes){return Ok(failure(id,None,started,error));}
        Ok(match state.queue.submit(state.engine.clone(),bytes).await{
            Ok(record)=>complete(EncodedResponse{status:202,body:serde_json::to_vec(&serde_json::json!({"request_id":record.request_id,"run_id":record.run_id,"state":"queued"})).unwrap()},Some(record.request_id),None,started,None),
            Err(error)=>failure(id,None,started,error),
        })
    });
    match operation {
        Ok(operation) => operation
            .receive()
            .await
            .unwrap_or_else(|e| failure(None, None, started, e)),
        Err(e) => failure(None, None, started, e),
    }
}
async fn receipt_request<B: ObjectStore + 'static>(
    State(state): State<Arc<ApiState<B>>>,
    Path((run_id, id)): Path<(String, String)>,
    request: Request,
) -> Response {
    let started = nabedra_rt::time::now_std();
    if request.headers().contains_key("x-nabedra-storage")
        || !authenticated(request.headers(), &state.token)
    {
        return failure(
            None,
            None,
            started,
            not_applied(ErrorCode::Unauthorized, "unauthorized"),
        );
    }
    let (run_id, id) = match (Uuid::parse_str(&run_id), Uuid::parse_str(&id)) {
        (Ok(run_id), Ok(id)) => (run_id, id),
        _ => {
            return failure(
                None,
                None,
                started,
                not_applied(
                    ErrorCode::ValidationError,
                    "receipt identities must be UUIDs",
                ),
            );
        }
    };
    if request.uri().query().is_some() {
        return failure(
            Some(id),
            None,
            started,
            not_applied(
                ErrorCode::ValidationError,
                "receipt query parameters are unsupported",
            ),
        );
    }
    match state.queue.receipt(&state.engine, run_id, id).await {
        Ok(receipt) => complete(
            EncodedResponse {
                status: 200,
                body: serde_json::to_vec(&receipt).unwrap(),
            },
            Some(id),
            None,
            started,
            None,
        ),
        Err(error) => failure(Some(id), None, started, error),
    }
}

async fn read_body(mut body: Body) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::new();
    let deadline = nabedra_rt::time::now() + std::time::Duration::from_secs(30);
    while let Some(frame) = nabedra_rt::time::timeout_at(deadline, body.frame())
        .await
        .map_err(|_| {
            not_applied(
                ErrorCode::ValidationError,
                "request body read deadline exceeded",
            )
        })?
    {
        let frame = frame.map_err(|_| {
            not_applied(ErrorCode::ValidationError, "request body could not be read")
        })?;
        if let Ok(data) = frame.into_data() {
            if data.len() > MAX_BODY_BYTES - bytes.len() {
                return Err(not_applied(
                    ErrorCode::LimitExceeded,
                    "request body exceeds 8 MiB",
                ));
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

/// Accept only a complete JSON object with unique top-level keys and a UUID
/// string. Ignore other values without building a Value tree or coercing scalars.
/// Malformed JSON, trailing content, duplicate keys or an invalid ID yield None.
fn scan_request_id(bytes: &[u8]) -> Option<Uuid> {
    struct IdVisitor;
    impl<'de> Visitor<'de> for IdVisitor {
        type Value = Option<Uuid>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a request object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut keys = BTreeSet::new();
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
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let id = serde::Deserializer::deserialize_map(&mut deserializer, IdVisitor).ok()?;
    deserializer.end().ok()?;
    id
}

fn not_applied(code: ErrorCode, message: &'static str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

fn failure(
    id: Option<Uuid>,
    operation: Option<&'static str>,
    started: Instant,
    error: ProtocolError,
) -> Response {
    let code = error.code;
    complete(encode_error(id, error), id, operation, started, Some(code))
}

fn complete(
    encoded: EncodedResponse,
    id: Option<Uuid>,
    operation: Option<&'static str>,
    started: Instant,
    code: Option<ErrorCode>,
) -> Response {
    let code = match encoded.status {
        413 => Some(ErrorCode::LimitExceeded),
        500 => Some(ErrorCode::InternalError),
        _ => code,
    };
    // For admitted work this runs before returning to the executor, so disconnects
    // cannot discard diagnostics. Never record error messages or request values.
    tracing::info!(
        request_id = ?id,
        operation,
        elapsed_ms = nabedra_rt::time::now_std().duration_since(started).as_millis() as u64,
        error_code = ?code,
        status = encoded.status,
        "operation completed"
    );
    let mut response = Response::new(Body::from(encoded.body));
    *response.status_mut() = StatusCode::from_u16(encoded.status).expect("protocol HTTP status");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
