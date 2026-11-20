use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use axum::Router;
use axum_server::{Handle, tls_rustls::RustlsConfig};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};

use super::{Config, Control, RESTORE_REBIND, TlsFiles, backup, forward, operate};
use crate::http;

const STOP_GRACE: Duration = Duration::from_secs(10);
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a broker whose store requires recovery keeps answering (readiness 503,
/// in-flight replies) before it stops.
const RECOVERY_GRACE: Duration = Duration::from_secs(2);
/// How long a graceful stop waits for the last users of the store before closing it.
const STORE_CLOSE_WAIT: Duration = Duration::from_secs(10);

/// The process exit code of a broker that stopped because its store requires recovery.
/// The next start reopens the store, which applies the recovery rules R1-R3 (plan
/// section 3): it recovers, or refuses to open loudly (RV-EV M2).
pub const RECOVERY_EXIT_CODE: i32 = 75;

/// `serve` stopped because the store latched `RecoveryRequired`.
#[derive(Debug, thiserror::Error)]
#[error("the Events store requires recovery; restart the broker to reopen it")]
pub struct StoreRecoveryRequired;

pub async fn serve(config: Config) -> Result<()> {
    let public_listener =
        TcpListener::bind(config.public_bind).context("bind the public HTTPS listener")?;
    let private_listener =
        TcpListener::bind(config.peer_bind).context("bind the private peer listener")?;
    let public_tls = match config.public_tls.as_ref() {
        Some(tls) => Some(tls_config(tls, None)?),
        None => None,
    };
    let peer_tls = match config.peer_tls.as_ref() {
        Some(tls) => Some(tls_config(tls, config.peer_ca.as_deref())?),
        None => None,
    };
    let broker = config.open_broker()?;
    ensure!(
        !config.data_dir.join("restore-in-progress").try_exists()?,
        "offline restore is incomplete; rerun restore before serving"
    );
    backup::cleanup_spool(&config.data_dir)?;
    let restoring = config.data_dir.join(RESTORE_REBIND).try_exists()?;
    let control = Control::new(config, broker.clone())?;
    control.reopen(restoring).await?;

    let public_routes = http::router(
        control.clone(),
        control.keyring.clone(),
        control.config.http_limits,
    )?
    .merge(operate::router(control.clone()));
    let peer_routes = broker
        .router()
        .merge(forward::router(control.clone()))
        .merge(backup::router(control.clone()));

    let public_handle = Handle::<SocketAddr>::new();
    let peer_handle = Handle::<SocketAddr>::new();
    let public = listen(
        public_listener,
        public_tls,
        public_routes,
        public_handle.clone(),
    );
    let peer = listen(private_listener, peer_tls, peer_routes, peer_handle.clone());

    let controllers = tokio::spawn(control.clone().controllers());
    let monitor = tokio::spawn(control.clone().monitor());
    let retention = tokio::spawn(control.clone().retain());
    let recovery = restoring.then(|| {
        tokio::spawn(async move {
            if let Err(error) = control.recover_seeded().await {
                tracing::error!(error = %error, "could not initialize seeded Events partitions");
            }
        })
    });

    // The listeners (and the routes that own the broker) drop at the end of this block.
    let (result, latched) = {
        let mut public = Box::pin(public);
        let mut peer = Box::pin(peer);
        tokio::select! {
            result = &mut public => {
                peer_handle.graceful_shutdown(Some(STOP_GRACE));
                let other = peer.await;
                (result.and(other), false)
            }
            result = &mut peer => {
                public_handle.graceful_shutdown(Some(STOP_GRACE));
                let other = public.await;
                (result.and(other), false)
            }
            result = shutdown_signal() => {
                public_handle.graceful_shutdown(Some(STOP_GRACE));
                peer_handle.graceful_shutdown(Some(STOP_GRACE));
                let (public, peer) = tokio::join!(public, peer);
                (result.and(public).and(peer), false)
            }
            () = broker.recovery_required() => {
                // Every group's next write fails. Report it (readiness 503, the status
                // endpoint), then stop so that a restart reopens and recovers the store.
                tracing::error!(
                    grace = ?RECOVERY_GRACE,
                    "the Events store requires recovery; stopping the broker"
                );
                tokio::time::sleep(RECOVERY_GRACE).await;
                public_handle.graceful_shutdown(Some(RECOVERY_GRACE));
                peer_handle.graceful_shutdown(Some(RECOVERY_GRACE));
                let (public, peer) = tokio::join!(public, peer);
                (public.and(peer), true)
            }
        }
    };

    for task in [Some(controllers), Some(monitor), Some(retention), recovery]
        .into_iter()
        .flatten()
    {
        task.abort();
        let _ = task.await;
    }
    // Close every group, then the store itself, so a planned stop is a clean close.
    let stopped = broker.shutdown(STORE_CLOSE_WAIT).await;
    if latched {
        return Err(StoreRecoveryRequired.into());
    }
    result.and(stopped)
}

async fn listen(
    listener: TcpListener,
    tls: Option<RustlsConfig>,
    routes: Router,
    handle: Handle<SocketAddr>,
) -> Result<()> {
    if let Some(tls) = tls {
        let mut server = axum_server::from_tcp_rustls(listener, tls)?.handle(handle);
        server
            .http_builder()
            .http1()
            .max_headers(48)
            .max_buf_size(24 * 1024)
            .header_read_timeout(HEADER_READ_TIMEOUT);
        server.http_builder().http2().max_concurrent_streams(32);
        server.serve(routes.into_make_service()).await?;
    } else {
        let mut server = axum_server::from_tcp(listener)?.handle(handle);
        server
            .http_builder()
            .http1()
            .max_headers(48)
            .max_buf_size(24 * 1024)
            .header_read_timeout(HEADER_READ_TIMEOUT);
        server.http_builder().http2().max_concurrent_streams(32);
        server.serve(routes.into_make_service()).await?;
    }
    Ok(())
}

fn tls_config(files: &TlsFiles, client_ca: Option<&Path>) -> Result<RustlsConfig> {
    let certificates =
        CertificateDer::pem_file_iter(&files.cert)?.collect::<Result<Vec<_>, _>>()?;
    ensure!(!certificates.is_empty(), "TLS certificate chain is empty");
    let private_key = PrivateKeyDer::from_pem_file(&files.key)?;
    let builder = ServerConfig::builder();
    let mut config = match client_ca {
        Some(path) => {
            let mut store = RootCertStore::empty();
            for certificate in CertificateDer::pem_file_iter(path)? {
                store.add(certificate?)?;
            }
            ensure!(!store.is_empty(), "peer CA contains no trust roots");
            let verifier = WebPkiClientVerifier::builder(Arc::new(store)).build()?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, private_key)?
        }
        None => builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?,
    };
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(RustlsConfig::from_config(Arc::new(config)))
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = terminate.recv() => {}
            result = tokio::signal::ctrl_c() => result?,
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
