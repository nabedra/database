//! Listener lifecycle on the caller's runtime. Keep this future alive until it
//! returns: cancellation would skip the executor drain.
use crate::{api, engine::Engine, executor::Executor, storage::ObjectStore};
use axum_server::{
    Handle,
    tls_rustls::{RustlsAcceptor, RustlsConfig},
};
use std::{future::Future, io};
use tokio::net::TcpListener;

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
