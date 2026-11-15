#[cfg(feature = "s3")]
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use meshdb_server::cli::{Cli, Command};
#[cfg(feature = "s3")]
use meshdb_server::{
    config::Config,
    engine::Engine,
    executor::Executor,
    runtime,
    storage::{S3Store, Storage},
    worker_pool::WorkerPool,
};
use std::{io, process::ExitCode};
#[cfg(feature = "s3")]
use std::{net::SocketAddr, num::NonZeroUsize};
use tracing_subscriber::{filter::Targets, prelude::*};

#[tokio::main]
async fn main() -> ExitCode {
    let command = Cli::parse().command;
    if tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::stderr)
                .with_ansi(false)
                .with_filter(
                    Targets::new().with_target("meshdb_server::api", tracing::Level::INFO),
                ),
        )
        .try_init()
        .is_err()
    {
        eprintln!("meshdb-server: diagnostics initialization failed");
        return ExitCode::FAILURE;
    }
    let result = match command {
        Command::Serve(options) => run(options).await,
        Command::Api { config } => meshdb_server::cluster::runtime::run_api(config).await,
        Command::Worker { bootstrap } => meshdb_server::node::runtime::run_worker(bootstrap).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("meshdb-server: {message}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "s3")]
async fn run(options: meshdb_server::config::ServeOptions) -> Result<(), &'static str> {
    let config = Config::from_env(options).map_err(|_| "configuration invalid")?;
    // Select the provider explicitly even if dependencies also enable ring.
    let tls = match (&config.options().tls_cert, &config.options().tls_key) {
        (Some(cert), Some(key)) => {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .map_err(|_| "TLS configuration invalid")?;
            Some(
                RustlsConfig::from_pem_file(cert, key)
                    .await
                    .map_err(|_| "TLS configuration invalid")?,
            )
        }
        _ => None,
    };
    // Register both signals before storage initialization and listener binding.
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "signal initialization failed")?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| "signal initialization failed")?;
    let shutdown = async move {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = terminate.recv() => Ok(()),
                _ = interrupt.recv() => Ok(()),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await
        }
    };
    let storage = Storage::<S3Store>::connect(config.bucket(), config.namespace().clone())
        .await
        .map_err(|_| "storage initialization failed")?;
    storage
        .initialize_catalog()
        .await
        .map_err(|_| "catalog initialization failed")?;
    let executor = Executor::new(config.options().workers, config.options().queue_size)
        .map_err(|_| "executor initialization failed")?;
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(config.options().workers).ok_or("configuration invalid")?,
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| "worker initialization failed")?;
    let engine = Engine::new(storage, pool, config.options().partition_size);
    // Keep the owner and coordinator runtime alive through every serve exit.
    // Errors inside this block cannot bypass the awaited worker shutdown below.
    let result = async {
        let listener = match tokio::net::TcpListener::bind(SocketAddr::new(
            config.options().host,
            config.options().port,
        ))
        .await
        {
            Ok(listener) => listener,
            Err(_) => {
                executor.drain().await;
                return Err("listener failed");
            }
        };
        runtime::serve(
            listener,
            engine,
            executor,
            config.token().to_owned(),
            tls,
            shutdown,
        )
        .await
        .map_err(|_| "listener failed")
    }
    .await;
    // runtime::serve drains admitted requests before returning. The bind failure
    // branch also drains the executor before reaching this teardown.
    let shutdown = workers
        .shutdown()
        .await
        .map_err(|_| "worker shutdown failed");
    result.and(shutdown)
}

#[cfg(not(feature = "s3"))]
async fn run(_: meshdb_server::config::ServeOptions) -> Result<(), &'static str> {
    Err("unsupported configuration: legacy serve requires the s3 feature")
}
