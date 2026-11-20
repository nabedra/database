//! Listener lifecycle on the caller's runtime. Keep this future alive until it
//! returns: cancellation would skip the executor drain.
use crate::{api, engine::Engine, executor::Executor, storage::ObjectStore};
use axum_server::tls_rustls::RustlsConfig;
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
                rustls::crypto::ring::default_provider(),
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

pub(crate) use crate::network::{serve_router, serve_router_with_budget};

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
    serve_configured(
        listener,
        engine,
        executor,
        token,
        tls,
        shutdown,
        crate::config::ExecutionConfig::default(),
        false,
        false,
    )
    .await
}

pub async fn serve_configured<B: ObjectStore + 'static>(
    listener: TcpListener,
    engine: Engine<B>,
    executor: Executor,
    token: String,
    tls: Option<RustlsConfig>,
    shutdown: impl Future<Output = io::Result<()>>,
    execution: crate::config::ExecutionConfig,
    cold_start: bool,
    migrate_catalog: bool,
) -> io::Result<()> {
    let stream_frontend = std::sync::Arc::new(
        api::stream::Frontend::new()
            .map_err(|_| io::Error::other("invalid stream ingress limits"))?,
    );
    let (app, queue_tasks, queue) = api::router_with_queue_startup(
        engine.clone(),
        executor.clone(),
        token,
        execution,
        cold_start,
        migrate_catalog,
        stream_frontend.clone(),
    )
    .map_err(|_| io::Error::other("invalid queue configuration"))?;
    if let Err(error) = queue.initialize(&engine).await {
        executor.stop_accepting();
        queue_tasks.stop_accepting();
        executor.drain().await;
        queue_tasks.drain().await;
        return Err(io::Error::other(error.message));
    }
    let signal_result = std::sync::Arc::new(std::sync::Mutex::new(None));
    let outcome = signal_result.clone();
    let ingress = executor.clone();
    let stream_ingress = stream_frontend.clone();
    let result = serve_router(
        listener,
        app.merge(api::health::router(engine)),
        tls,
        async move {
            let result = shutdown.await;
            ingress.stop_accepting();
            stream_ingress.stop_accepting();
            *outcome.lock().unwrap() = Some(result);
        },
    )
    .await;
    executor.stop_accepting();
    executor.drain().await;
    queue_tasks.stop_accepting();
    queue_tasks.drain().await;
    stream_frontend.drain().await;
    let signal = signal_result.lock().unwrap().take().unwrap_or(Ok(()));
    signal.and(result)
}
