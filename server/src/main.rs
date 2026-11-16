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
#[cfg(feature = "s3")]
use std::net::SocketAddr;
use std::{io, process::ExitCode};
use tracing_subscriber::{filter::Targets, prelude::*};

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("meshdb-server: network runtime initialization failed");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run_cli())
}

async fn run_cli() -> ExitCode {
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
        Command::Doctor { config, json } => meshdb_server::maintenance::doctor(&config, json).await,
        Command::Database {
            command:
                meshdb_server::cli::DatabaseCommand::Migrate {
                    storage,
                    region,
                    cluster_id,
                    confirm_writers_stopped,
                },
        } => {
            meshdb_server::maintenance::migrate(
                &storage,
                &region,
                cluster_id,
                confirm_writers_stopped,
            )
            .await
        }
        Command::Serve(options) => run(options).await,
        Command::Api {
            config,
            cold_start,
            migrate_catalog,
        } => {
            meshdb_server::cluster::runtime::run_api_configured(config, cold_start, migrate_catalog)
                .await
        }
        Command::Worker { bootstrap } => meshdb_server::node::runtime::run_worker(bootstrap).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("meshdb-server: {message}");
            if matches!(
                message,
                "configuration invalid"
                    | "storage URI invalid"
                    | "namespace invalid"
                    | "--confirm-writers-stopped is required"
            ) {
                ExitCode::from(2)
            } else {
                ExitCode::FAILURE
            }
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
    let coordination_slots = config
        .execution()
        .max_pending_requests
        .min((config.resources().control_memory_reserve_bytes / 4).min(16 << 20) / (64 << 10))
        .min(256) as usize;
    if coordination_slots == 0 {
        return Err("control memory cannot support request coordination");
    }
    let executor = Executor::new(coordination_slots, config.options().queue_size)
        .map_err(|_| "executor initialization failed")?;
    let (pool, workers) =
        WorkerPool::start_with_resources(config.resources(), tokio::runtime::Handle::current())
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
        runtime::serve_configured(
            listener,
            engine,
            executor,
            config.token().to_owned(),
            tls,
            shutdown,
            config.execution(),
            config.options().cold_start,
            config.options().migrate_catalog,
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
async fn run(options: meshdb_server::config::ServeOptions) -> Result<(), &'static str> {
    let config =
        meshdb_server::config::Config::from_env(options).map_err(|_| "configuration invalid")?;
    if let (Some(cert), Some(key)) = (&config.options().tls_cert, &config.options().tls_key) {
        let _ = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .map_err(|_| "TLS configuration invalid")?;
    }
    Err("unsupported configuration: legacy serve requires the s3 feature")
}
