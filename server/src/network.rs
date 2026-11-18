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
use tokio::net::TcpListener;

struct Connection<I> {
    io: I,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl<I: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Connection<I> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}
impl<I: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Connection<I> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, buffer)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
#[derive(Clone)]
struct HeaderCharge(std::sync::Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>);
#[derive(Clone)]
struct BoundedAccept<A> {
    inner: A,
    total: std::sync::Arc<tokio::sync::Semaphore>,
    headers: std::sync::Arc<tokio::sync::Semaphore>,
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
        let total = self.total.clone().try_acquire_owned();
        let header = self.headers.clone().try_acquire_owned();
        let inner = self.inner.clone();
        Box::pin(async move {
            let permit = total.map_err(|_| io::Error::other("listener capacity"))?;
            let header = header.map_err(|_| io::Error::other("header capacity"))?;
            let service = service.layer(axum::Extension(HeaderCharge(std::sync::Arc::new(
                std::sync::Mutex::new(Some(header)),
            ))));
            let (io, service) = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                inner.accept(stream, service),
            )
            .await
            .map_err(|_| io::Error::other("handshake timeout"))??;
            Ok((
                Connection {
                    io,
                    _permit: permit,
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

pub(crate) async fn serve_router_with_budget(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    data_connections: usize,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
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
                let class = if path == "/internal/v1/queue" {
                    request
                        .headers()
                        .get("x-nabedra-queue-class")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("progress")
                } else if path == "/internal/v1/enroll" {
                    "bootstrap"
                } else if path == "/v1/requests" && request.method() == axum::http::Method::POST {
                    "ingress"
                } else if path.starts_with("/v1/requests/") {
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
                    Ok(permit) => next.run(request).await.map(|body| {
                        axum::body::Body::new(NetworkBody {
                            body,
                            _permit: permit,
                        })
                    }),
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
                drop(charge.0.lock().unwrap().take());
            }
            next.run(request).await
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
        .keep_alive(false)
        .max_headers(32)
        .max_buf_size(32768)
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(std::time::Duration::from_secs(5));
    let total = std::sync::Arc::new(tokio::sync::Semaphore::new(
        data_connections + 16 + 8 + 8 + 4 + 8 + 24,
    ));
    let headers = std::sync::Arc::new(tokio::sync::Semaphore::new(24));
    let serve = async {
        match tls {
            Some(tls) => {
                server
                    .acceptor(BoundedAccept {
                        inner: RustlsAcceptor::new(tls),
                        total,
                        headers,
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
                    })
                    .serve(app.into_make_service())
                    .await
            }
        }
    };
    tokio::pin!(serve);
    tokio::select! { result = &mut serve => result, _ = shutdown => { handle.graceful_shutdown(None); serve.await } }
}
