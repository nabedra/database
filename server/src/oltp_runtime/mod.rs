//! Standalone OLTP server and experimental worker-memory queue components.
//! The installed server uses RF1; distributed production composition and
//! provisioning are not enabled by the CLI.
mod admission;
mod bootstrap;
pub mod coordinator;
mod http;
mod http_runtime;
mod queue;
pub mod replica;
mod transaction;
pub mod worker_endpoint;
pub(crate) mod worker_runtime;

use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp::OltpEngine,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use axum_server::tls_rustls::RustlsConfig;
use clap::Args;
use std::{
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

pub use http::router;
pub use queue::LocalRuntime;

/// Bound for decoded request structures, independent of logical table size.
pub(crate) const MAX_DECODED_REQUEST_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const RECEIPT_OBSERVATION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

/// Runtime used by the standalone CLI. Validated worker resources allow at most
/// 254 foreground native jobs, so this pool does not impose an extra four-job cap.
pub fn standalone_network_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(256)
        .enable_all()
        .build()
}

#[derive(Clone, Debug, Args)]
pub struct ServeOptions {
    /// Existing local data directory. Mount/provider verification is not implemented.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// Database selector accepted by this endpoint.
    #[arg(long, default_value = "default")]
    pub database: String,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub host: IpAddr,
    #[arg(long, default_value_t = 8000)]
    pub port: u16,
    /// Total execution threads, including two storage maintenance threads.
    #[arg(long, default_value_t = 4)]
    pub workers: usize,
    /// Additional pending slots beyond foreground executors.
    #[arg(long, default_value_t = 64)]
    pub queue_size: usize,
    #[arg(long)]
    pub execution_config: Option<PathBuf>,
    #[arg(long)]
    pub resources_config: Option<PathBuf>,
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
}

pub(crate) fn require_standalone(engine: &OltpEngine) -> Result<(), ProtocolError> {
    let manifest = engine.manifest();
    if manifest.topology != "standalone-rf1" || manifest.group_id.is_some() {
        return Err(ProtocolError::new(
            ErrorCode::UnsupportedOperation,
            "standalone runtime cannot execute a replicated store",
            Outcome::NotApplied,
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}

impl ServeOptions {
    pub fn limits(&self) -> Result<(ExecutionConfig, WorkerResources), ProtocolError> {
        if self.workers < 4
            || self.workers > 256
            || !valid_database(&self.database)
            || self.tls_cert.is_some() != self.tls_key.is_some()
        {
            return Err(invalid("invalid OLTP standalone options"));
        }
        // Require an intentional existing location. Directory existence alone
        // does not verify a provider volume or detect an absent mount.
        if !self.data_dir.is_dir() {
            return Err(invalid(
                "--data-dir must be an existing local data directory",
            ));
        }
        let resources = match &self.resources_config {
            Some(path) => read_config(path)?,
            None => WorkerResources {
                max_threads: self.workers as u64,
                ..Default::default()
            },
        };
        let capacity = resources
            .max_threads
            .saturating_sub(3)
            .checked_add(self.queue_size as u64)
            .filter(|n| *n <= 1_000_000)
            .ok_or_else(|| invalid("invalid queue capacity"))?;
        let execution = match &self.execution_config {
            Some(path) => read_config(path)?,
            None => ExecutionConfig {
                max_pending_requests: capacity,
                ..Default::default()
            },
        };
        if resources.max_threads < 4 {
            return Err(invalid(
                "OLTP requires one query, one control and two storage maintenance threads",
            ));
        }
        resources
            .validate_execution(&execution)
            .map_err(|_| invalid("invalid OLTP resource configuration"))?;
        Ok((execution, resources))
    }
}

fn read_config<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T, ProtocolError> {
    use std::io::Read;
    if !std::fs::metadata(path)
        .map_err(|_| invalid("configuration unreadable"))?
        .is_file()
    {
        return Err(invalid("configuration must be a regular file"));
    }
    let mut open = std::fs::OpenOptions::new();
    open.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        open.custom_flags(libc::O_NONBLOCK);
    }
    let file = open
        .open(path)
        .map_err(|_| invalid("configuration unreadable"))?;
    if !file
        .metadata()
        .map_err(|_| invalid("configuration unreadable"))?
        .is_file()
    {
        return Err(invalid("configuration must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("configuration unreadable"))?;
    if bytes.len() > 65536 {
        return Err(invalid("configuration exceeds 64 KiB"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("configuration invalid"))
}

pub(crate) fn valid_database(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

pub async fn run(options: ServeOptions) -> Result<(), &'static str> {
    let (execution, resources) = options.limits().map_err(|_| "configuration invalid")?;
    let token = std::env::var("NABEDRA_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .ok_or("NABEDRA_TOKEN is required")?;
    let tls = match (&options.tls_cert, &options.tls_key) {
        (Some(cert), Some(key)) => {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            Some(
                RustlsConfig::from_pem_file(cert, key)
                    .await
                    .map_err(|_| "TLS configuration invalid")?,
            )
        }
        _ => None,
    };
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "signal initialization failed")?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| "signal initialization failed")?;
    let runtime = worker_runtime::WorkerRuntime::open(
        &options.data_dir,
        &options.database,
        &token,
        execution,
        resources,
    )
    .await
    .map_err(|error| {
        eprintln!("nabedra-db: {}", error.message);
        "OLTP typed runtime initialization failed"
    })?;
    let app = http::router_for(runtime.clone(), token, options.database)
        .map_err(|_| "configuration invalid")?;
    let listener = tokio::net::TcpListener::bind((options.host, options.port))
        .await
        .map_err(|_| "listener failed")?;
    let shutdown = async {
        #[cfg(unix)]
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        runtime.stop_accepting();
    };
    let result = crate::network::serve_router(listener, app, tls, shutdown)
        .await
        .map_err(|_| "listener failed");
    runtime
        .drain()
        .await
        .map_err(|_| "OLTP typed runtime shutdown failed")?;
    result
}
