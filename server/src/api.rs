//! POST operations transport. Body polling, parsing, execution, encoding and
//! completion diagnostics all run inside the executor's owned operation.
pub mod health;
pub mod response;

use crate::{
    engine::Engine,
    executor::Executor,
    protocol::{self, ErrorCode, MAX_BODY_BYTES, Outcome, ProtocolError},
    storage::ObjectStore,
};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::post,
};
use http_body_util::BodyExt;
use response::{EncodedResponse, encode_error, encode_success};
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use std::{collections::BTreeSet, fmt, sync::Arc, time::Instant};
use uuid::Uuid;

struct ApiState<B: ObjectStore> {
    engine: Engine<B>,
    executor: Executor,
    token: String,
}

/// Build only `POST /v1/operations` over an already initialized engine.
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
    Router::new()
        .route("/v1/operations", post(operations::<B>))
        .with_state(Arc::new(ApiState {
            engine,
            executor,
            token,
        }))
}

async fn operations<B: ObjectStore + 'static>(
    State(state): State<Arc<ApiState<B>>>,
    request: Request,
) -> Response {
    let started = Instant::now();
    if !authenticated(request.headers(), &state.token) {
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
    let operation = match admission.spawn(move || async move {
        Ok(execute_request(state.engine.clone(), request, started).await)
    }) {
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

fn json_content_type(headers: &HeaderMap) -> bool {
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
    engine: Engine<B>,
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
    let mutation = parsed.operation.is_mutation();
    match engine.execute(parsed.operation).await {
        Ok(data) => complete(
            encode_success(parsed.request_id, &data, mutation),
            request_id,
            name,
            started,
            None,
        ),
        Err(error) => failure(request_id, name, started, error),
    }
}

async fn read_body(mut body: Body) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
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
    tracing::info!(request_id = ?id, operation, elapsed_ms = started.elapsed().as_millis() as u64, error_code = ?code, status = encoded.status, "operation completed");
    let mut response = Response::new(Body::from(encoded.body));
    *response.status_mut() = StatusCode::from_u16(encoded.status).expect("protocol HTTP status");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
