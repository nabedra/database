//! Public OLAP stream routes shared by the local and distributed APIs.
use crate::{
    executor::{Admission, Executor},
    node::{http, stream::wire},
    protocol::{ErrorCode, MAX_PREDICATE_DEPTH, Outcome, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
    storage::context::RequestContext,
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State, rejection::PathRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::{delete, get, post},
};
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    time::{Instant, MissedTickBehavior},
};
use uuid::Uuid;

const CONTROL_BYTES: usize = 32 * 1024;
const EGRESS_CHUNK_BYTES: usize = 32 * 1024;
const EGRESS_SLOTS: usize = 8;
const BODY_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
const DELIVERY_RENEW_INTERVAL: Duration = Duration::from_millis(500);

type Result<T> = std::result::Result<T, wire::Error>;

pub(crate) struct Frontend {
    opens: Executor,
    batches: Executor,
    control: Executor,
    releases: Executor,
    egress: Arc<Semaphore>,
}

impl Frontend {
    pub(crate) fn new() -> std::result::Result<Self, ProtocolError> {
        Ok(Self {
            opens: Executor::new(8, 0)?,
            batches: Executor::new(EGRESS_SLOTS, 0)?,
            control: Executor::new(8, 0)?,
            releases: Executor::new(EGRESS_SLOTS, EGRESS_SLOTS)?,
            egress: Arc::new(Semaphore::new(EGRESS_SLOTS)),
        })
    }

    pub(crate) fn stop_accepting(&self) {
        self.opens.stop_accepting();
        self.batches.stop_accepting();
        self.control.stop_accepting();
    }

    pub(crate) async fn drain(&self) {
        tokio::join!(
            self.opens.drain(),
            self.batches.drain(),
            self.control.drain(),
            self.releases.drain()
        );
    }

    fn admit(&self, input: &Input) -> std::result::Result<Admission, ProtocolError> {
        match input {
            Input::Open | Input::OpenSnapshot => &self.opens,
            Input::Command(Ok(wire::Request::Batch { .. })) => &self.batches,
            Input::Command(_) => &self.control,
        }
        .try_admit()
    }
}

pub(crate) trait Backend: Send + Sync + 'static {
    // Only a distributed API duplicates the authenticated worker response in memory.
    const PROXY_BATCH_BYTES: usize;

    fn frontend(&self) -> &Frontend;
    fn scheduler(&self) -> &ResourceScheduler;
    fn authorize(&self, headers: &HeaderMap) -> Result<Option<RequestContext>>;
    fn call(
        &self,
        request: wire::Request,
        context: Option<RequestContext>,
    ) -> impl Future<Output = Result<wire::Reply>> + Send;
    fn renew(&self, _token: Uuid) -> impl Future<Output = bool> + Send {
        std::future::ready(false)
    }
    fn release(&self, _token: Uuid) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

pub(crate) fn routes<B: Backend>() -> Router<Arc<B>> {
    Router::new()
        .route("/v1/streams/capabilities", get(capabilities::<B>))
        .route("/v1/streams", post(open::<B>))
        .route(
            "/v1/streams/{run_id}/{stream_id}/batches/{sequence}",
            post(batch::<B>),
        )
        .route("/v1/streams/{run_id}/{stream_id}", delete(close::<B>))
        .route("/v1/stream-snapshots", post(open_snapshot::<B>))
        .route(
            "/v1/stream-snapshots/{run_id}/{snapshot_id}",
            delete(close_snapshot::<B>),
        )
}

enum Input {
    Open,
    OpenSnapshot,
    Command(std::result::Result<wire::Request, ProtocolError>),
}

async fn capabilities<B: Backend>(State(state): State<Arc<B>>, request: Request) -> Response {
    dispatch(
        state,
        request,
        Input::Command(Ok(wire::Request::Capabilities)),
    )
    .await
}

async fn open<B: Backend>(State(state): State<Arc<B>>, request: Request) -> Response {
    dispatch(state, request, Input::Open).await
}

async fn open_snapshot<B: Backend>(State(state): State<Arc<B>>, request: Request) -> Response {
    dispatch(state, request, Input::OpenSnapshot).await
}

async fn batch<B: Backend>(
    State(state): State<Arc<B>>,
    path: std::result::Result<Path<(String, String, String)>, PathRejection>,
    request: Request,
) -> Response {
    let command = path
        .map_err(|_| invalid())
        .and_then(|Path((run, id, sequence))| {
            if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            Ok(wire::Request::Batch {
                run_id: uuid(&run)?,
                stream_id: uuid(&id)?,
                sequence: sequence.parse().map_err(|_| invalid())?,
            })
        });
    dispatch(state, request, Input::Command(command)).await
}

async fn close<B: Backend>(
    State(state): State<Arc<B>>,
    path: std::result::Result<Path<(String, String)>, PathRejection>,
    request: Request,
) -> Response {
    let command = path.map_err(|_| invalid()).and_then(|Path((run, id))| {
        Ok(wire::Request::Close {
            run_id: uuid(&run)?,
            stream_id: uuid(&id)?,
        })
    });
    dispatch(state, request, Input::Command(command)).await
}

async fn close_snapshot<B: Backend>(
    State(state): State<Arc<B>>,
    path: std::result::Result<Path<(String, String)>, PathRejection>,
    request: Request,
) -> Response {
    let command = path.map_err(|_| invalid()).and_then(|Path((run, id))| {
        if id.len() > 73 {
            return Err(invalid());
        }
        Ok(wire::Request::CloseSnapshot {
            run_id: uuid(&run)?,
            snapshot_id: id,
        })
    });
    dispatch(state, request, Input::Command(command)).await
}

async fn dispatch<B: Backend>(state: Arc<B>, request: Request, input: Input) -> Response {
    let context = match state.authorize(request.headers()) {
        Ok(context) => context,
        Err(error) => return error_response(None, error),
    };
    if request.uri().query().is_some() {
        return error_response(None, invalid().into());
    }
    let work = state.frontend().admit(&input).and_then(|admission| {
        admission.spawn(move || async move { Ok(execute(state, request, input, context).await) })
    });
    match work {
        Ok(work) => work
            .receive()
            .await
            .unwrap_or_else(|error| error_response(None, error.into())),
        Err(error) => error_response(None, error.into()),
    }
}

async fn execute<B: Backend>(
    state: Arc<B>,
    request: Request,
    input: Input,
    context: Option<RequestContext>,
) -> Response {
    let mut request_id = None;
    let result = async {
        let mut request = request;
        let is_open = matches!(input, Input::Open | Input::OpenSnapshot);
        let cap = if is_open { wire::MAX_REQUEST_BYTES } else { 0 };
        if is_open {
            if !super::json_content_type(request.headers()) {
                return Err(invalid().into());
            }
        } else if !request.headers().contains_key(header::CONTENT_LENGTH) {
            request
                .headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        }
        let length = http::framing(request.headers(), cap, false)?.ok_or_else(invalid)?;
        if is_open && length == 0 {
            return Err(invalid().into());
        }
        let is_batch = matches!(input, Input::Command(Ok(wire::Request::Batch { .. })));
        let egress = if is_batch {
            Some(
                state
                    .frontend()
                    .egress
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| {
                        ProtocolError::new(
                            ErrorCode::Overloaded,
                            "stream egress is full",
                            Outcome::NotApplied,
                        )
                    })?,
            )
        } else {
            None
        };
        // The shared query decoder holds one raw JSON value per predicate level.
        let receive_bytes = if is_batch {
            B::PROXY_BATCH_BYTES + 2 * EGRESS_CHUNK_BYTES
        } else {
            0
        };
        let staged_bytes = CONTROL_BYTES + length * (MAX_PREDICATE_DEPTH + 8) + receive_bytes;
        let mut charge = state
            .scheduler()
            .charge_memory(0, staged_bytes as u64)
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "stream staging memory is full",
                    Outcome::NotApplied,
                )
            })?;
        let body =
            http::read_small(request.into_body(), length, Instant::now() + BODY_TIMEOUT).await?;
        let command = match input {
            Input::Open => {
                let opened: wire::Open = parse_open(&body)?;
                request_id = Some(opened.request_id);
                wire::Request::Open(opened)
            }
            Input::OpenSnapshot => {
                let opened: wire::OpenSnapshot = parse_open(&body)?;
                request_id = Some(opened.request_id);
                wire::Request::OpenSnapshot(opened)
            }
            Input::Command(command) => command?,
        };
        drop(body);
        let reply = state.call(command, context).await?;
        match reply {
            wire::Reply::Capabilities(value) => json(StatusCode::OK, &value),
            wire::Reply::Opened(value) => json(StatusCode::CREATED, &value),
            wire::Reply::SnapshotOpened(value) => json(StatusCode::CREATED, &value),
            wire::Reply::Batch(mut batch) => {
                // Retain the worker delivery even if validating or constructing
                // the public response fails.
                let deadline = Instant::now() + DELIVERY_TIMEOUT;
                let delivery = batch
                    .release
                    .take()
                    .map(|token| DeliveryLease::new(state, token, deadline));
                let size = batch.bytes.len();
                if size == 0 || size > wire::MAX_BATCH_BYTES {
                    return Err(internal().into());
                }
                let content_type = match batch.format {
                    wire::Format::Rows => "application/json",
                    wire::Format::Arrow => "application/vnd.apache.arrow.stream",
                };
                let proxy_bytes = B::PROXY_BATCH_BYTES.min(size + 1);
                charge.shrink_to((proxy_bytes + 2 * EGRESS_CHUNK_BYTES) as u64)?;
                let live = delivery.as_ref().map(|delivery| delivery.live.clone());
                let bytes = Bytes::from_owner(Egress {
                    batch,
                    _memory: charge,
                    _permit: egress.ok_or_else(internal)?,
                    _delivery: delivery,
                });
                let chunks = futures_util::stream::unfold(
                    (bytes, live),
                    move |(mut bytes, live)| async move {
                        if bytes.is_empty() {
                            return None;
                        }
                        if Instant::now() >= deadline
                            || live
                                .as_ref()
                                .is_some_and(|status| !status.load(Ordering::Acquire))
                        {
                            let error =
                                io::Error::new(io::ErrorKind::TimedOut, "stream delivery expired");
                            return Some((Err(error), (Bytes::new(), None)));
                        }
                        let chunk = bytes.split_to(bytes.len().min(EGRESS_CHUNK_BYTES));
                        Some((Ok(chunk), (bytes, live)))
                    },
                );
                Ok(response(
                    StatusCode::OK,
                    content_type,
                    size,
                    Body::from_stream(chunks),
                ))
            }
            wire::Reply::Closed | wire::Reply::Eof => Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::empty())
                .expect("constant stream headers")),
        }
    }
    .await;
    result.unwrap_or_else(|error| error_response(request_id, error))
}

struct Egress {
    batch: wire::Batch,
    _memory: MemoryCharge,
    _permit: OwnedSemaphorePermit,
    _delivery: Option<DeliveryLease>,
}

impl AsRef<[u8]> for Egress {
    fn as_ref(&self) -> &[u8] {
        &self.batch.bytes
    }
}

struct DeliveryLease {
    live: Arc<AtomicBool>,
    _finished: oneshot::Sender<()>,
}

impl DeliveryLease {
    fn new<B: Backend>(backend: Arc<B>, token: Uuid, deadline: Instant) -> Self {
        let live = Arc::new(AtomicBool::new(true));
        let status = live.clone();
        let (finished, mut ended) = oneshot::channel();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(
                Instant::now() + DELIVERY_RENEW_INTERVAL,
                DELIVERY_RENEW_INTERVAL,
            );
            ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut ended => break,
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = ticks.tick() => {}
                }
                let renewed = tokio::select! {
                    biased;
                    _ = &mut ended => break,
                    _ = tokio::time::sleep_until(deadline) => break,
                    result = tokio::time::timeout(DELIVERY_RENEW_INTERVAL, backend.renew(token)) => {
                        result.unwrap_or(false)
                    }
                };
                if !renewed {
                    break;
                }
            }
            status.store(false, Ordering::Release);
            if let Ok(admission) = backend.frontend().releases.try_admit() {
                let _ = admission.spawn(move || async move {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(2), backend.release(token)).await;
                    Ok(())
                });
            }
        });
        Self {
            live,
            _finished: finished,
        }
    }
}

fn parse_open<T: DeserializeOwned>(body: &[u8]) -> std::result::Result<T, ProtocolError> {
    if body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        != Some(b'{')
    {
        return Err(invalid());
    }
    serde_json::from_slice(body).map_err(|_| invalid())
}

fn uuid(value: &str) -> std::result::Result<Uuid, ProtocolError> {
    let value = Uuid::parse_str(value).map_err(|_| invalid())?;
    if value.is_nil() {
        return Err(invalid());
    }
    Ok(value)
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid streaming request",
        Outcome::NotApplied,
    )
}

fn internal() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::InternalError,
        "invalid streaming response",
        Outcome::Unknown,
    )
}

fn response(status: StatusCode, content_type: &'static str, size: usize, body: Body) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, size)
        .header(header::CACHE_CONTROL, "no-store")
        .body(body)
        .expect("validated stream headers")
}

fn json<T: Serialize>(status: StatusCode, value: &T) -> Result<Response> {
    let body = serde_json::to_vec(value).map_err(|_| internal())?;
    Ok(response(
        status,
        "application/json",
        body.len(),
        Body::from(body),
    ))
}

fn error_response(request_id: Option<Uuid>, error: wire::Error) -> Response {
    #[derive(Serialize)]
    struct Envelope {
        request_id: Option<Uuid>,
        error: wire::Error,
    }
    let status =
        StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let envelope = Envelope { request_id, error };
    let mut response = json(status, &envelope).expect("stream error envelope can always serialize");
    if status == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}
