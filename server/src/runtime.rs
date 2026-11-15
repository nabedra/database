//! Listener lifecycle on the caller's runtime. Keep this future alive until it
//! returns: cancellation would skip the executor drain.
use crate::{api, engine::Engine, executor::Executor, storage::ObjectStore};
use axum_server::{
    Handle,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
};
use std::{future::Future, io};
use tokio::net::TcpListener;

pub(crate) fn retained_tls(
    material: Option<&crate::cluster::startup_material::files::TlsMaterial>,
) -> crate::node::http::Result<Option<RustlsConfig>> {
    #[derive(Debug)]
    struct Certificate(std::sync::Arc<rustls::sign::CertifiedKey>);
    impl rustls::server::ResolvesServerCert for Certificate {
        fn resolve(
            &self,
            _: rustls::server::ClientHello<'_>,
        ) -> Option<std::sync::Arc<rustls::sign::CertifiedKey>> {
            Some(self.0.clone())
        }
    }
    material
        .map(|material| {
            let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| crate::node::http::invalid())?
            .with_no_client_auth()
            .with_cert_resolver(std::sync::Arc::new(Certificate(std::sync::Arc::new(
                material.certified_key.clone(),
            ))));
            Ok(RustlsConfig::from_config(std::sync::Arc::new(config)))
        })
        .transpose()
}
pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let (Ok(mut terminate), Ok(mut interrupt)) = (
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()),
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()),
        ) {
            tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

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
pub(crate) async fn serve_router(
    listener: TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
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
        .max_buf_size(16384)
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(std::time::Duration::from_secs(5));
    let total = std::sync::Arc::new(tokio::sync::Semaphore::new(64));
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

/// Serve one initialized engine over HTTP or HTTPS. Shutdown and listener errors
/// stop admission and ingress, finish connections, then drain detached work.
/// The shutdown future may return an error, which is reported after draining.
pub async fn serve<B: ObjectStore + 'static>(
    listener: TcpListener,
    engine: Engine<B>,
    executor: Executor,
    token: String,
    tls: Option<RustlsConfig>,
    shutdown: impl Future<Output = io::Result<()>>,
) -> io::Result<()> {
    let handle = Handle::new();
    let result = async {
        let app =
            api::router(engine.clone(), executor.clone(), token).merge(api::health::router(engine));
        let listener = listener.into_std()?;
        let (failed, mut failures) = tokio::sync::mpsc::channel(1);
        let server = axum_server::from_tcp(listener)?
            .handle(handle.clone())
            .accept_error_handler(move |address, error| {
                // A failed TLS handshake belongs to one client. Only errors
                // from the listening socket shut down the process.
                if address.is_none() {
                    let _ = failed.try_send(error.kind());
                }
            });
        let serving = async {
            match tls {
                Some(tls) => {
                    server
                        .acceptor(RustlsAcceptor::new(tls))
                        .serve(app.into_make_service())
                        .await
                }
                None => server.serve(app.into_make_service()).await,
            }
        };
        let shutdown = async {
            tokio::select! {
                signal = shutdown => signal,
                Some(kind) = failures.recv() => Err(io::Error::new(kind, "listener failed")),
            }
        };
        tokio::pin!(serving);
        tokio::select! {
            result = &mut serving => result,
            signal = shutdown => {
                executor.stop_accepting();
                handle.graceful_shutdown(None);
                let served = serving.await;
                signal.and(served)
            }
        }
    }
    .await;
    executor.stop_accepting();
    handle.graceful_shutdown(None);
    executor.drain().await;
    result
}
