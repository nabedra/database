//! Shared bounded HTTP/TLS listener. Engine bootstrap and operation admission
//! remain with their runtimes; transport budgets are identical for both engines.
#[cfg(feature = "olap")]
use crate::api::response;
#[cfg(feature = "oltp")]
use crate::response;
use axum_server::{
    Handle,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
};
use std::{future::Future, io};
#[cfg(feature = "olap")]
use std::{
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::Context,
};
use tokio::net::TcpListener;
#[cfg(feature = "olap")]
use tokio::time::{Instant, Sleep};

#[cfg(feature = "olap")]
#[derive(Clone, Default)]
pub(crate) struct StreamWriteDeadline(Arc<Mutex<Option<ActiveStreamWrite>>>);

#[cfg(feature = "olap")]
struct ActiveStreamWrite {
    deadline: Instant,
    response: Weak<()>,
}

#[cfg(feature = "olap")]
impl StreamWriteDeadline {
    pub(crate) fn arm(&self, deadline: Instant) -> Arc<()> {
        let response = Arc::new(());
        *self.0.lock().unwrap() = Some(ActiveStreamWrite {
            deadline,
            response: Arc::downgrade(&response),
        });
        response
    }

    fn clear_flushed(&self) -> bool {
        let mut active = self.0.lock().unwrap();
        // Hyper or TLS can buffer the last bytes after dropping the body.
        // Keep the deadline until those bytes flush.
        if active
            .as_ref()
            .is_some_and(|write| write.response.strong_count() == 0)
        {
            *active = None;
            true
        } else {
            false
        }
    }
}

struct Connection<I> {
    io: I,
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// Set on keep-alive listeners only (PERF2e).
    idle: Option<IdleTracker>,
    #[cfg(feature = "olap")]
    write_deadline: StreamWriteDeadline,
    #[cfg(feature = "olap")]
    write_timer: Option<Pin<Box<Sleep>>>,
}

#[cfg(feature = "olap")]
impl<I> Connection<I> {
    fn check_write_deadline(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let deadline = self
            .write_deadline
            .0
            .lock()
            .unwrap()
            .as_ref()
            .map(|write| write.deadline);
        let Some(deadline) = deadline else {
            self.write_timer = None;
            return Ok(());
        };
        let timer = self
            .write_timer
            .get_or_insert_with(|| Box::pin(nabedra_rt::time::sleep_until(deadline)));
        if timer.deadline() != deadline {
            timer.as_mut().reset(deadline);
        }
        if timer.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "stream delivery expired",
            ));
        }
        Ok(())
    }
}
impl<I: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Connection<I> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = &mut *self;
        match &mut this.idle {
            Some(idle) => idle.poll_read(std::pin::Pin::new(&mut this.io), cx, buffer),
            None => std::pin::Pin::new(&mut this.io).poll_read(cx, buffer),
        }
    }
}
impl<I: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Connection<I> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        #[cfg(feature = "olap")]
        self.check_write_deadline(cx)?;
        std::pin::Pin::new(&mut self.io).poll_write(cx, buffer)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        #[cfg(feature = "olap")]
        self.check_write_deadline(cx)?;
        let result = std::pin::Pin::new(&mut self.io).poll_flush(cx);
        if let (Some(idle), std::task::Poll::Ready(Ok(()))) = (&self.idle, &result) {
            idle.flushed();
        }
        #[cfg(feature = "olap")]
        if self.write_timer.is_some()
            && matches!(result, std::task::Poll::Ready(Ok(())))
            && self.write_deadline.clear_flushed()
        {
            self.write_timer = None;
        }
        result
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        #[cfg(feature = "olap")]
        self.check_write_deadline(cx)?;
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
/// Timeouts of a listener. Hyper's header read timer starts when a
/// connection waits for its next request, so on a keep-alive listener it is
/// the idle bound, and [`IdleTracker`] applies `header_read` from the first
/// byte of each later request (PERF2e).
#[derive(Clone, Copy)]
struct ListenerTimeouts {
    idle: std::time::Duration,
    header_read: std::time::Duration,
}

/// Production timeouts of the client-facing OLTP listeners: an idle
/// connection stays open 30 s, so ORM pools that reuse one for up to 25 s
/// keep it at a serial pace, and a request head still has 5 s from its
/// first byte, which bounds slow senders.
#[cfg(feature = "oltp")]
const KEEP_ALIVE_TIMEOUTS: ListenerTimeouts = ListenerTimeouts {
    idle: std::time::Duration::from_secs(30),
    header_read: std::time::Duration::from_secs(5),
};

/// Every other listener closes a connection after each response, and a
/// request head has 5 s.
const CLOSE_TIMEOUTS: ListenerTimeouts = ListenerTimeouts {
    idle: std::time::Duration::from_secs(5),
    header_read: std::time::Duration::from_secs(5),
};

/// How long a new connection waits for the slot of an idle connection it
/// evicted. The evicted connection closes on its next poll.
const EVICTION_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Where a keep-alive connection is between requests.
#[derive(Clone, Copy)]
enum Phase {
    /// Before the first request; axum-server's first request timeout bounds it.
    First,
    /// A later request's head is arriving; it must be complete by `deadline`.
    Head { deadline: tokio::time::Instant },
    /// A request was dispatched and its response is not finished.
    Busy,
    /// The response body ended; its last bytes may still be buffered.
    Flushing,
    /// The response was flushed and no byte of the next request was read.
    Idle { since: std::time::Instant },
}

struct PhaseState {
    phase: Phase,
    /// Set only while idle. The connection then reads EOF without consuming
    /// a byte, so a request sent after the eviction is never read.
    evicted: bool,
    reader: Option<std::task::Waker>,
}

/// One keep-alive connection's phase, shared by its I/O wrapper, its
/// requests (as an extension) and the listener's registry.
struct ConnectionPhase {
    state: std::sync::Mutex<PhaseState>,
    header_read: std::time::Duration,
}

impl ConnectionPhase {
    fn dispatched(&self) {
        self.state.lock().unwrap().phase = Phase::Busy;
    }

    fn responded(&self) {
        let mut state = self.state.lock().unwrap();
        if matches!(state.phase, Phase::Busy) {
            state.phase = Phase::Flushing;
        }
    }
}

/// The live keep-alive connections of one listener, so a new connection can
/// take the slot of the oldest idle one when the listener is full.
#[derive(Default)]
struct IdleRegistry {
    next: std::sync::atomic::AtomicU64,
    connections: std::sync::Mutex<std::collections::HashMap<u64, std::sync::Arc<ConnectionPhase>>>,
}

impl IdleRegistry {
    fn register(self: &std::sync::Arc<Self>, header_read: std::time::Duration) -> IdleTracker {
        let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let phase = std::sync::Arc::new(ConnectionPhase {
            state: std::sync::Mutex::new(PhaseState {
                phase: Phase::First,
                evicted: false,
                reader: None,
            }),
            header_read,
        });
        self.connections.lock().unwrap().insert(id, phase.clone());
        IdleTracker {
            registry: self.clone(),
            id,
            phase,
            head_timer: None,
        }
    }

    /// Marks the connection that has been idle longest for eviction and
    /// wakes it. A connection with a request in progress, including one
    /// whose head is arriving, is never chosen.
    fn evict_oldest_idle(&self) -> bool {
        let connections = self.connections.lock().unwrap();
        // Equal `since` (common under paused or simulated time) goes to the
        // connection registered first, not to the map's hash order.
        let oldest = connections
            .iter()
            .filter_map(|(id, connection)| {
                let state = connection.state.lock().unwrap();
                match state.phase {
                    Phase::Idle { since } if !state.evicted => Some(((since, *id), connection)),
                    _ => None,
                }
            })
            .min_by_key(|(order, _)| *order);
        let Some((_, connection)) = oldest else {
            return false;
        };
        let mut state = connection.state.lock().unwrap();
        // The phase lock was released between the scan and here.
        if !matches!(state.phase, Phase::Idle { .. }) || state.evicted {
            return false;
        }
        state.evicted = true;
        let reader = state.reader.take();
        drop(state);
        drop(connections);
        if let Some(reader) = reader {
            reader.wake();
        }
        true
    }
}

struct IdleTracker {
    registry: std::sync::Arc<IdleRegistry>,
    id: u64,
    phase: std::sync::Arc<ConnectionPhase>,
    head_timer: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
}

impl Drop for IdleTracker {
    fn drop(&mut self) {
        self.registry.connections.lock().unwrap().remove(&self.id);
    }
}

impl IdleTracker {
    /// Reads under the phase lock, so an eviction either happens before any
    /// byte of the next request is consumed or not at all.
    fn poll_read<I: tokio::io::AsyncRead>(
        &mut self,
        io: std::pin::Pin<&mut I>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let mut state = self.phase.state.lock().unwrap();
        if state.evicted {
            return std::task::Poll::Ready(Ok(()));
        }
        let filled = buffer.filled().len();
        let result = io.poll_read(cx, buffer);
        let idle = matches!(state.phase, Phase::Idle { .. });
        if idle
            && matches!(result, std::task::Poll::Ready(Ok(())))
            && buffer.filled().len() > filled
        {
            state.phase = Phase::Head {
                deadline: nabedra_rt::time::now() + self.phase.header_read,
            };
        } else if idle
            && result.is_pending()
            && !state
                .reader
                .as_ref()
                .is_some_and(|reader| reader.will_wake(cx.waker()))
        {
            state.reader = Some(cx.waker().clone());
        }
        let deadline = match state.phase {
            Phase::Head { deadline } => Some(deadline),
            _ => None,
        };
        drop(state);
        if let (Some(deadline), true) = (deadline, result.is_pending()) {
            let timer = self
                .head_timer
                .get_or_insert_with(|| Box::pin(nabedra_rt::time::sleep_until(deadline)));
            if timer.deadline() != deadline {
                timer.as_mut().reset(deadline);
            }
            if timer.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "request header read timeout",
                )));
            }
        }
        result
    }

    /// A successful flush after the response body ended makes the
    /// connection idle: every byte of the response has left the server.
    fn flushed(&self) {
        let mut state = self.phase.state.lock().unwrap();
        if matches!(state.phase, Phase::Flushing) {
            state.phase = Phase::Idle {
                since: nabedra_rt::time::now_std(),
            };
        }
    }
}

/// Marks the request's connection busy until the response body is dropped.
struct ResponseBody {
    body: axum::body::Body,
    phase: std::sync::Arc<ConnectionPhase>,
}
impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.phase.responded();
    }
}
impl hyper::body::Body for ResponseBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
    {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

#[derive(Clone)]
struct PhaseHandle(std::sync::Arc<ConnectionPhase>);

/// Only a connection's first request still holds its header charge, so this
/// counts requests on new and on reused connections (PERF2c).
#[cfg(feature = "oltp")]
fn count_request(first_on_connection: bool) {
    use crate::oltp_runtime::stage_timing::perf_stage;
    if first_on_connection {
        perf_stage!("http.request.new_connection")
    } else {
        perf_stage!("http.request.reused_connection")
    }
    .record(std::time::Duration::ZERO);
}
#[derive(Clone)]
struct HeaderCharge(std::sync::Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>);
#[derive(Clone)]
struct BoundedAccept<A> {
    inner: A,
    total: std::sync::Arc<tokio::sync::Semaphore>,
    headers: std::sync::Arc<tokio::sync::Semaphore>,
    /// Keep-alive listeners only: idle connections a full listener evicts.
    idle: Option<std::sync::Arc<IdleRegistry>>,
    header_read: std::time::Duration,
}
impl<A> axum_server::accept::Accept<tokio::net::TcpStream, axum::Router> for BoundedAccept<A>
where
    A: axum_server::accept::Accept<tokio::net::TcpStream, axum::Router> + Clone + Send + 'static,
    A::Future: Send,
    A::Stream: Send + 'static,
    A::Service: Send + 'static,
{
    type Stream = Connection<A::Stream>;
    type Service = A::Service;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;
    fn accept(&self, stream: tokio::net::TcpStream, service: axum::Router) -> Self::Future {
        // A response is one write, but the TLS handshake flight and a reused
        // connection's next exchange must not wait on Nagle's algorithm.
        let _ = stream.set_nodelay(true);
        let header = self.headers.clone().try_acquire_owned();
        let permit = self.total.clone().try_acquire_owned();
        // A full keep-alive listener first takes the slot of its oldest idle
        // connection, so idle pooled connections never cost a new client its
        // slot (PERF2e). Without an idle connection it refuses, as before.
        let evicted = header.is_ok()
            && permit.is_err()
            && self
                .idle
                .as_ref()
                .is_some_and(|idle| idle.evict_oldest_idle());
        let total = self.total.clone();
        let idle = self.idle.clone();
        let header_read = self.header_read;
        let inner = self.inner.clone();
        Box::pin(async move {
            let header = header.map_err(|_| io::Error::other("header capacity"))?;
            let permit = match permit {
                Ok(permit) => permit,
                Err(_) if evicted => {
                    #[cfg(feature = "oltp")]
                    crate::oltp_runtime::stage_timing::perf_stage!("http.accept.evicted_idle")
                        .record(std::time::Duration::ZERO);
                    nabedra_rt::time::timeout(EVICTION_WAIT, total.acquire_owned())
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .ok_or_else(|| io::Error::other("listener capacity"))?
                }
                Err(_) => return Err(io::Error::other("listener capacity")),
            };
            let idle = idle.map(|idle| idle.register(header_read));
            let service = service.layer(axum::Extension(HeaderCharge(std::sync::Arc::new(
                std::sync::Mutex::new(Some(header)),
            ))));
            let service = match &idle {
                Some(idle) => service.layer(axum::Extension(PhaseHandle(idle.phase.clone()))),
                None => service,
            };
            #[cfg(feature = "olap")]
            let write_deadline = StreamWriteDeadline::default();
            #[cfg(feature = "olap")]
            let service = service.layer(axum::Extension(write_deadline.clone()));
            #[cfg(feature = "oltp")]
            let started = nabedra_rt::time::now_std();
            let (io, service) = nabedra_rt::time::timeout(
                std::time::Duration::from_secs(5),
                inner.accept(stream, service),
            )
            .await
            .map_err(|_| io::Error::other("handshake timeout"))??;
            // TCP accept to the end of the TLS handshake (zero without TLS).
            #[cfg(feature = "oltp")]
            crate::oltp_runtime::stage_timing::perf_stage!("http.accept.handshake").since(started);
            Ok((
                Connection {
                    io,
                    _permit: permit,
                    idle,
                    #[cfg(feature = "olap")]
                    write_deadline,
                    #[cfg(feature = "olap")]
                    write_timer: None,
                },
                service,
            ))
        })
    }
}
struct NetworkBody {
    body: axum::body::Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl hyper::body::Body for NetworkBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
    {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub(crate) async fn serve_router(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_router_with_budget(listener, app, tls, 64, shutdown).await
}

/// The client-facing OLTP API listener: like [`serve_router`], but a
/// connection stays open after a successful (2xx) response so ORM pools can
/// reuse it (plan `oltp-direct-path.md` section 8). An idle connection ends
/// after 30 s, a later request's head must arrive within 5 s of its first
/// byte, and a full listener evicts its oldest idle connection for a new one
/// (PERF2e). Every other response still closes its connection, so a failed
/// or unauthenticated caller cannot hold an idle slot of the budget.
#[cfg(feature = "oltp")]
pub(crate) async fn serve_router_keep_alive(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_bounded(listener, app, tls, 64, Some(KEEP_ALIVE_TIMEOUTS), shutdown).await
}

pub(crate) async fn serve_router_with_budget(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    data_connections: usize,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    serve_bounded(listener, app, tls, data_connections, None, shutdown).await
}

async fn serve_bounded(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    data_connections: usize,
    keep_alive: Option<ListenerTimeouts>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let timeouts = keep_alive.unwrap_or(CLOSE_TIMEOUTS);
    let keep_alive = keep_alive.is_some();
    if data_connections == 0 || data_connections > 8192 {
        return Err(io::Error::other("invalid data connection budget"));
    }
    let data = std::sync::Arc::new(tokio::sync::Semaphore::new(data_connections));
    let ingress = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
    let receipts = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
    let replicas = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
    let bootstrap = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
    let controls = std::sync::Arc::new(tokio::sync::Semaphore::new(8));
    let app = app.layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let data = data.clone();
            let ingress = ingress.clone();
            let receipts = receipts.clone();
            let replicas = replicas.clone();
            let bootstrap = bootstrap.clone();
            let controls = controls.clone();
            async move {
                let path = request.uri().path();
                let stream_transport = cfg!(feature = "olap")
                    && (path == "/v1/streams"
                        || path.starts_with("/v1/streams/")
                        || path == "/v1/stream-snapshots"
                        || path.starts_with("/v1/stream-snapshots/"));
                let class = if path == "/internal/v1/queue"
                    || (cfg!(feature = "olap") && path == "/internal/v1/streams")
                {
                    request
                        .headers()
                        .get("x-nabedra-queue-class")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("progress")
                } else if path == "/internal/v1/enroll" {
                    "bootstrap"
                } else if path == "/v1/requests" && request.method() == axum::http::Method::POST {
                    "ingress"
                } else if path.starts_with("/v1/requests/")
                    || (stream_transport && request.method() != axum::http::Method::POST)
                {
                    "receipt"
                } else if path.starts_with("/health/")
                    || (path.starts_with("/internal/")
                        && path != "/internal/v1/jobs"
                        && path != "/internal/v1/operations")
                    || path.starts_with("/v1/cluster/")
                {
                    "progress"
                } else {
                    "data"
                };
                let permit = match class {
                    "ingress" => ingress,
                    "receipt" => receipts,
                    "replica" => replicas,
                    "bootstrap" => bootstrap,
                    "progress" => controls,
                    _ => data,
                }
                .try_acquire_owned();
                match permit {
                    Ok(permit) => {
                        let mut response = next.run(request).await;
                        if !stream_transport && !(keep_alive && response.status().is_success()) {
                            response.headers_mut().insert(
                                axum::http::header::CONNECTION,
                                axum::http::HeaderValue::from_static("close"),
                            );
                        }
                        response.map(|body| {
                            axum::body::Body::new(NetworkBody {
                                body,
                                _permit: permit,
                            })
                        })
                    }
                    Err(_) => {
                        let error = response::encode_error(
                            None,
                            crate::protocol::ProtocolError::new(
                                crate::protocol::ErrorCode::Overloaded,
                                "listener request class capacity exhausted",
                                crate::protocol::Outcome::NotApplied,
                            ),
                        );
                        axum::http::Response::builder()
                            .status(error.status)
                            .header("content-type", "application/json")
                            .header("cache-control", "no-store")
                            .header(
                                "connection",
                                if stream_transport {
                                    "keep-alive"
                                } else {
                                    "close"
                                },
                            )
                            .body(axum::body::Body::from(error.body))
                            .unwrap()
                    }
                }
            }
        },
    ));
    let app = app.layer(axum::middleware::from_fn(
        |request: axum::extract::Request, next: axum::middleware::Next| async move {
            if let Some(charge) = request.extensions().get::<HeaderCharge>() {
                let header = charge.0.lock().unwrap().take();
                #[cfg(feature = "oltp")]
                count_request(header.is_some());
                drop(header);
            }
            let phase = request
                .extensions()
                .get::<PhaseHandle>()
                .map(|handle| handle.0.clone());
            let Some(phase) = phase else {
                return next.run(request).await;
            };
            phase.dispatched();
            next.run(request)
                .await
                .map(|body| axum::body::Body::new(ResponseBody { body, phase }))
        },
    ));
    let handle = Handle::new();
    let mut server = axum_server::from_tcp(listener.into_std()?)?
        .http1_only()
        .handle(handle.clone())
        .first_request_timeout(std::time::Duration::from_secs(5));
    server
        .http_builder()
        .http1()
        .keep_alive(keep_alive || cfg!(feature = "olap"))
        .max_headers(32)
        .max_buf_size(32768)
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(timeouts.idle);
    let total = std::sync::Arc::new(tokio::sync::Semaphore::new(
        data_connections + 16 + 8 + 8 + 4 + 8 + 24,
    ));
    let headers = std::sync::Arc::new(tokio::sync::Semaphore::new(24));
    let idle = keep_alive.then(|| std::sync::Arc::new(IdleRegistry::default()));
    let serve = async {
        match tls {
            Some(tls) => {
                server
                    .acceptor(BoundedAccept {
                        inner: RustlsAcceptor::new(tls),
                        total,
                        headers,
                        idle: idle.clone(),
                        header_read: timeouts.header_read,
                    })
                    .serve(app.into_make_service())
                    .await
            }
            None => {
                server
                    .acceptor(BoundedAccept {
                        inner: axum_server::accept::DefaultAcceptor::new(),
                        total,
                        headers,
                        idle: idle.clone(),
                        header_read: timeouts.header_read,
                    })
                    .serve(app.into_make_service())
                    .await
            }
        }
    };
    tokio::pin!(serve);
    tokio::select! { result = &mut serve => result, _ = shutdown => { handle.graceful_shutdown(None); serve.await } }
}

#[cfg(all(test, feature = "olap"))]
mod tests {
    use super::*;
    use std::{task::Poll, time::Duration};
    use tokio::{
        io::{AsyncWrite, AsyncWriteExt},
        sync::{Semaphore, oneshot},
    };

    struct TestSocket {
        blocked: bool,
        first_poll: Option<oneshot::Sender<()>>,
    }

    impl TestSocket {
        fn poll<T>(&mut self, result: T) -> Poll<io::Result<T>> {
            if let Some(first_poll) = self.first_poll.take() {
                first_poll.send(()).unwrap();
            }
            if self.blocked {
                Poll::Pending
            } else {
                Poll::Ready(Ok(result))
            }
        }
    }

    impl AsyncWrite for TestSocket {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.poll(buffer.len())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll(())
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn connection(io: TestSocket, write_deadline: StreamWriteDeadline) -> Connection<TestSocket> {
        Connection {
            io,
            _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            idle: None,
            write_deadline,
            write_timer: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn st03_blocked_stream_socket_expires_writes_and_final_flush() {
        for writing in [true, false] {
            let deadline = StreamWriteDeadline::default();
            let response = deadline.arm(nabedra_rt::time::now() + Duration::from_secs(1));
            let (first_poll, started) = oneshot::channel();
            let mut connection = connection(
                TestSocket {
                    blocked: true,
                    first_poll: Some(first_poll),
                },
                deadline,
            );
            let writer = nabedra_rt::task::spawn(async move {
                if writing {
                    let _response = response;
                    connection.write_all(b"batch").await
                } else {
                    drop(response);
                    connection.flush().await
                }
            });
            started.await.unwrap();
            // The blocked socket never wakes the task. The write deadline must.
            let error = nabedra_rt::time::timeout(Duration::from_secs(5), writer)
                .await
                .expect("the stream write deadline did not wake the socket task")
                .unwrap()
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn st03_flushed_stream_allows_the_next_keep_alive_response() {
        let deadline = StreamWriteDeadline::default();
        let response = deadline.arm(nabedra_rt::time::now() + Duration::from_secs(1));
        let mut connection = connection(
            TestSocket {
                blocked: false,
                first_poll: None,
            },
            deadline.clone(),
        );
        connection.write_all(b"batch").await.unwrap();
        connection.flush().await.unwrap();
        assert!(deadline.0.lock().unwrap().is_some());

        drop(response);
        connection.flush().await.unwrap();
        assert!(deadline.0.lock().unwrap().is_none());

        tokio::time::advance(Duration::from_secs(2)).await;
        connection.write_all(b"next response").await.unwrap();
        connection.flush().await.unwrap();
    }
}

#[cfg(all(test, feature = "oltp"))]
mod keep_alive_tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn exchange(stream: &mut tokio::net::TcpStream, path: &str) -> String {
        let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        read_response(stream).await
    }

    async fn read_response(stream: &mut tokio::net::TcpStream) -> String {
        let mut response = Vec::new();
        let mut buffer = [0; 1024];
        // Every test response is "ok" or "no": read until its body arrives.
        while !(response.ends_with(b"ok") || response.ends_with(b"no")) {
            let read = nabedra_rt::time::timeout(Duration::from_secs(5), stream.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0, "connection closed before the response");
            response.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8(response).unwrap().to_ascii_lowercase()
    }

    async fn closed(stream: &mut tokio::net::TcpStream, within: Duration) -> bool {
        let mut buffer = [0; 64];
        matches!(
            nabedra_rt::time::timeout(within, stream.read(&mut buffer)).await,
            Ok(Ok(0)) | Ok(Err(_))
        )
    }

    /// A clean close: the server sent FIN (EOF), not a reset, and no bytes.
    async fn closed_cleanly(stream: &mut tokio::net::TcpStream, within: Duration) -> bool {
        let mut buffer = [0; 64];
        matches!(
            nabedra_rt::time::timeout(within, stream.read(&mut buffer)).await,
            Ok(Ok(0))
        )
    }

    fn app() -> axum::Router {
        axum::Router::new()
            .route("/ok", axum::routing::get(|| async { "ok" }))
            .route(
                "/no",
                axum::routing::get(|| async { (axum::http::StatusCode::UNAUTHORIZED, "no") }),
            )
    }

    async fn serve(
        app: axum::Router,
        data_connections: usize,
        keep_alive: Option<ListenerTimeouts>,
    ) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        nabedra_rt::task::spawn(async move {
            let shutdown = std::future::pending::<()>();
            serve_bounded(listener, app, None, data_connections, keep_alive, shutdown).await
        });
        address
    }

    const SHORT: ListenerTimeouts = ListenerTimeouts {
        idle: Duration::from_secs(3),
        header_read: Duration::from_secs(1),
    };

    /// P10-DET: among connections idle since the same instant, a full
    /// listener evicts the one registered first, not one in hash order.
    #[test]
    fn idle_eviction_breaks_ties_by_registration_order() {
        let registry = std::sync::Arc::new(IdleRegistry::default());
        let trackers: Vec<IdleTracker> = (0..32)
            .map(|_| registry.register(Duration::from_secs(1)))
            .collect();
        let since = nabedra_rt::time::now_std();
        for tracker in &trackers {
            tracker.phase.state.lock().unwrap().phase = Phase::Idle { since };
        }
        for tracker in &trackers {
            assert!(registry.evict_oldest_idle());
            assert!(tracker.phase.state.lock().unwrap().evicted);
        }
        assert!(!registry.evict_oldest_idle());
    }

    /// PERF2e: the client-facing OLTP listeners keep an idle connection 30 s
    /// (the ORMs reuse one for at most 25 s) and give a request head 5 s.
    #[test]
    fn o01_perf2e_keep_alive_bounds_cover_the_orm_reuse_bound() {
        assert_eq!(KEEP_ALIVE_TIMEOUTS.idle, Duration::from_secs(30));
        assert_eq!(KEEP_ALIVE_TIMEOUTS.header_read, Duration::from_secs(5));
        assert_eq!(CLOSE_TIMEOUTS.header_read, Duration::from_secs(5));
        // orm/python/src/nabedra/_http_pool.py and orm/typescript/src/transport.ts: 25 s.
        assert!(KEEP_ALIVE_TIMEOUTS.idle >= Duration::from_secs(25) + Duration::from_secs(5));
    }

    /// PERF2c / O01: the OLTP API listener keeps a connection open after a
    /// successful response, so ORM pools reuse it, and closes it after an
    /// error response and at the idle bound (PERF2e: idle, not header read).
    #[tokio::test]
    async fn o01_perf2c_api_listener_keeps_alive_after_success_only() {
        let address = serve(app(), 64, Some(SHORT)).await;
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        for _ in 0..3 {
            let response = exchange(&mut stream, "/ok").await;
            assert!(response.starts_with("http/1.1 200"), "{response}");
            assert!(!response.contains("connection: close"), "{response}");
        }
        let response = exchange(&mut stream, "/no").await;
        assert!(response.starts_with("http/1.1 401"), "{response}");
        assert!(response.contains("connection: close"), "{response}");
        assert!(closed(&mut stream, Duration::from_secs(2)).await);

        // An idle kept-alive connection ends at the idle bound.
        let mut idle = tokio::net::TcpStream::connect(address).await.unwrap();
        exchange(&mut idle, "/ok").await;
        assert!(!closed(&mut idle, Duration::from_secs(2)).await);
        assert!(closed_cleanly(&mut idle, Duration::from_secs(3)).await);
    }

    /// The production keep-alive entry point keeps an idle connection past
    /// the 5 s header read timeout, which used to close it (PERF2c).
    #[tokio::test]
    async fn o01_perf2e_api_listener_keeps_an_idle_connection_past_5_s() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        nabedra_rt::task::spawn(serve_router_keep_alive(
            listener,
            app(),
            None,
            std::future::pending(),
        ));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        exchange(&mut stream, "/ok").await;
        assert!(!closed(&mut stream, Duration::from_secs(7)).await);
        let response = exchange(&mut stream, "/ok").await;
        assert!(response.starts_with("http/1.1 200"), "{response}");
    }

    /// PERF2e: the header read timeout still applies to a request in
    /// progress. A head that started after an idle period longer than the
    /// header read timeout is served, and a slow head is cut off.
    #[tokio::test]
    async fn o01_perf2e_header_read_timeout_starts_at_the_request() {
        let address = serve(app(), 64, Some(SHORT)).await;
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        exchange(&mut stream, "/ok").await;
        // Idle longer than the header read timeout, then a prompt request.
        nabedra_rt::time::sleep(Duration::from_millis(1500)).await;
        let response = exchange(&mut stream, "/ok").await;
        assert!(response.starts_with("http/1.1 200"), "{response}");
        // A slow sender: part of a head, then nothing.
        stream.write_all(b"GET /ok HTTP/1.1\r\n").await.unwrap();
        let started = nabedra_rt::time::now_std();
        assert!(closed(&mut stream, Duration::from_secs(3)).await);
        let elapsed = nabedra_rt::time::now_std().duration_since(started);
        assert!(elapsed >= Duration::from_millis(900), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(2500), "{elapsed:?}");
    }

    /// PERF2e: when every slot is taken, a new connection is served by
    /// evicting the oldest idle keep-alive connection, which closes cleanly.
    /// A connection with a request in progress is never evicted, even when it
    /// is the oldest one.
    #[tokio::test]
    async fn o01_perf2e_full_listener_evicts_the_oldest_idle_connection() {
        let (release, released) = tokio::sync::watch::channel(false);
        let app = app().route(
            // A "progress" class path, so the request in progress does not
            // hold the data class permit the idle connections use.
            "/health/slow",
            axum::routing::get(move || {
                let mut released = released.clone();
                async move {
                    released.wait_for(|released| *released).await.unwrap();
                    "ok"
                }
            }),
        );
        let data_connections = 8;
        let slots = data_connections + 16 + 8 + 8 + 4 + 8 + 24;
        let address = serve(app, data_connections, Some(KEEP_ALIVE_TIMEOUTS)).await;

        // The oldest connection has a request in progress.
        let mut busy = tokio::net::TcpStream::connect(address).await.unwrap();
        busy.write_all(b"GET /health/slow HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .unwrap();
        nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        // Every other slot holds an idle kept-alive connection.
        let mut idle = Vec::new();
        for _ in 1..slots {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            let response = exchange(&mut stream, "/ok").await;
            assert!(response.starts_with("http/1.1 200"), "{response}");
            idle.push(stream);
        }
        nabedra_rt::time::sleep(Duration::from_millis(100)).await;

        // A new connection is served, and the oldest idle one is closed cleanly.
        let mut new = tokio::net::TcpStream::connect(address).await.unwrap();
        let response = exchange(&mut new, "/ok").await;
        assert!(response.starts_with("http/1.1 200"), "{response}");
        assert!(closed_cleanly(&mut idle[0], Duration::from_secs(2)).await);
        // The next oldest idle connection is still open and usable.
        assert!(!closed(&mut idle[1], Duration::from_millis(200)).await);
        let response = exchange(&mut idle[1], "/ok").await;
        assert!(response.starts_with("http/1.1 200"), "{response}");

        // Another new connection takes the next oldest idle slot: idle[2],
        // since idle[1] was just used.
        let mut another = tokio::net::TcpStream::connect(address).await.unwrap();
        let response = exchange(&mut another, "/ok").await;
        assert!(response.starts_with("http/1.1 200"), "{response}");
        assert!(closed_cleanly(&mut idle[2], Duration::from_secs(2)).await);
        assert!(!closed(&mut idle[1], Duration::from_millis(200)).await);

        // The request in progress was never evicted and still completes.
        release.send(true).unwrap();
        let response = read_response(&mut busy).await;
        assert!(response.starts_with("http/1.1 200"), "{response}");
    }

    /// Without an idle connection, a full listener still refuses a new one,
    /// and it never evicts a connection whose request head is arriving.
    #[tokio::test]
    async fn o01_perf2e_full_listener_never_evicts_a_request_in_progress() {
        let data_connections = 8;
        let slots = data_connections + 16 + 8 + 8 + 4 + 8 + 24;
        let address = serve(app(), data_connections, Some(KEEP_ALIVE_TIMEOUTS)).await;
        let mut partial = Vec::new();
        for _ in 0..slots {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            exchange(&mut stream, "/ok").await;
            // The next request's head has started but is not complete.
            stream.write_all(b"GET /ok HTTP/1.1\r\n").await.unwrap();
            partial.push(stream);
        }
        nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        let mut refused = tokio::net::TcpStream::connect(address).await.unwrap();
        let _ = refused
            .write_all(b"GET /ok HTTP/1.1\r\nHost: test\r\n\r\n")
            .await;
        assert!(closed(&mut refused, Duration::from_secs(3)).await);
        // Every head in progress completes on its own connection.
        for stream in &mut partial {
            stream.write_all(b"Host: test\r\n\r\n").await.unwrap();
            let response = read_response(stream).await;
            assert!(response.starts_with("http/1.1 200"), "{response}");
        }
    }

    /// The other listeners (workers' admin and stats endpoints) still close
    /// every connection after its response.
    #[tokio::test]
    async fn o01_perf2c_other_listeners_close_after_each_response() {
        let address = serve(app(), 64, None).await;
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let response = exchange(&mut stream, "/ok").await;
        assert!(response.contains("connection: close"), "{response}");
        assert!(closed(&mut stream, Duration::from_secs(2)).await);
    }
}
