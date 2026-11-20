#[cfg(all(feature = "olap", feature = "s3"))]
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use nabedra_database_server::cli::{Cli, Command};
#[cfg(all(feature = "olap", feature = "s3"))]
use nabedra_database_server::{
    config::Config,
    engine::Engine,
    executor::Executor,
    runtime,
    storage::{S3Store, Storage},
    worker_pool::WorkerPool,
};
#[cfg(not(feature = "cache"))]
use std::io;
#[cfg(all(feature = "olap", feature = "s3"))]
use std::net::SocketAddr;
use std::process::ExitCode;
#[cfg(not(feature = "cache"))]
use tracing_subscriber::{filter::Targets, prelude::*};

#[cfg(not(feature = "cache"))]
fn main() -> ExitCode {
    #[cfg(feature = "oltp")]
    let runtime = nabedra_database_server::oltp_runtime::standalone_network_runtime();
    #[cfg(feature = "olap")]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build();
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("nabedra-db: network runtime initialization failed");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run_cli())
}

#[cfg(not(feature = "cache"))]
async fn run_cli() -> ExitCode {
    let command = Cli::parse().command;
    let rpc_trace = if std::env::var("NABEDRA_OLTP_RPC_TRACE").as_deref() == Ok("1") {
        tracing::level_filters::LevelFilter::DEBUG
    } else {
        tracing::level_filters::LevelFilter::OFF
    };
    if tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::stderr)
                .with_ansi(false)
                .with_filter(
                    Targets::new()
                        .with_target("nabedra_database_server::api", tracing::Level::INFO)
                        .with_target(
                            "nabedra_database_server::oltp_replication::hosted",
                            tracing::Level::WARN,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::typed_log",
                            tracing::Level::WARN,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::service_rpc",
                            rpc_trace,
                        )
                        .with_target("nabedra_database_server::oltp_runtime", rpc_trace)
                        .with_target("nabedra_database_server::oltp_replication::manager::data_factory", rpc_trace)
                        .with_target("nabedra_database_server::oltp_replication::authority::control::forwarding", rpc_trace)
                        .with_target("nabedra_database_server::oltp_replication::typed_network", rpc_trace)
                        .with_target("nabedra_database_server::oltp_replication::multiplex", rpc_trace),
                ),
        )
        .try_init()
        .is_err()
    {
        eprintln!("nabedra-db: diagnostics initialization failed");
        return ExitCode::FAILURE;
    }
    let result = match command {
        #[cfg(feature = "oltp")]
        Command::GrowthPlan => nabedra_database_server::oltp_deployment::live::run_planner(),
        #[cfg(feature = "olap")]
        Command::Doctor { config, json } => {
            nabedra_database_server::maintenance::doctor(&config, json).await
        }
        #[cfg(feature = "olap")]
        Command::Database {
            command:
                nabedra_database_server::cli::DatabaseCommand::Migrate {
                    storage,
                    region,
                    cluster_id,
                    confirm_writers_stopped,
                },
        } => {
            nabedra_database_server::maintenance::migrate(
                &storage,
                &region,
                cluster_id,
                confirm_writers_stopped,
            )
            .await
        }
        #[cfg(feature = "olap")]
        Command::Database {
            command:
                nabedra_database_server::cli::DatabaseCommand::Export {
                    config,
                    output,
                    confirm_writers_stopped,
                },
        } => {
            match nabedra_database_server::olap_export::run_configured(
                &config,
                &output,
                confirm_writers_stopped,
            )
            .await
            {
                Ok(summary) => {
                    println!(
                        "{}",
                        serde_json::to_string(&summary).expect("archive summary")
                    );
                    Ok(())
                }
                Err(error) => {
                    eprintln!(
                        "nabedra-db: export failed: {:?}: {}",
                        error.code, error.message
                    );
                    Err("OLAP export failed")
                }
            }
        }
        #[cfg(feature = "oltp")]
        Command::Database {
            command: nabedra_database_server::cli::DatabaseCommand::Import(options),
        } => nabedra_database_server::oltp_runtime::archive_cli::import(options, false).await,
        #[cfg(feature = "oltp")]
        Command::Database {
            command: nabedra_database_server::cli::DatabaseCommand::Restore(options),
        } => nabedra_database_server::oltp_runtime::archive_cli::import(options, true).await,
        #[cfg(feature = "oltp")]
        Command::Database {
            command:
                nabedra_database_server::cli::DatabaseCommand::Backup {
                    previous,
                    data_dir,
                    output,
                    database,
                    workers,
                    resources_config,
                    execution_config,
                    cold_start,
                },
        } => {
            nabedra_database_server::oltp_runtime::archive_cli::backup(
                nabedra_database_server::oltp_runtime::ServeOptions {
                    data_dir,
                    database,
                    cold_start,
                    host: std::net::Ipv4Addr::LOCALHOST.into(),
                    port: 0,
                    workers,
                    queue_size: 64,
                    resources_config,
                    execution_config,
                    tls_cert: None,
                    tls_key: None,
                },
                output,
                previous,
            )
            .await
        }
        #[cfg(feature = "olap")]
        Command::Serve(options) => run(options).await,
        #[cfg(feature = "oltp")]
        Command::Serve(options) => nabedra_database_server::oltp_runtime::run(options).await,
        #[cfg(feature = "oltp")]
        Command::Doctor { .. } | Command::Database { .. } => {
            Err("unsupported OLTP operator command for this deployment configuration")
        }
        #[cfg(all(feature = "oltp", unix))]
        Command::Worker { bootstrap } => {
            nabedra_database_server::oltp_runtime::registered_process::run(bootstrap).await
        }
        #[cfg(all(feature = "oltp", not(unix)))]
        Command::Worker { .. } => Err("OLTP worker processes require POSIX descriptors"),
        #[cfg(all(feature = "oltp", unix))]
        Command::DataWorker { bootstrap } => {
            nabedra_database_server::oltp_runtime::data_worker_process::run(bootstrap).await
        }
        #[cfg(all(feature = "oltp", not(unix)))]
        Command::DataWorker { .. } => Err("OLTP data workers require POSIX descriptors"),
        #[cfg(all(feature = "oltp", unix))]
        Command::Api {
            config,
            cold_start,
            migrate_catalog,
        } => {
            nabedra_database_server::oltp_runtime::api_process::run_api_configured(
                config,
                cold_start,
                migrate_catalog,
            )
            .await
        }
        #[cfg(all(feature = "oltp", not(unix)))]
        Command::Api { .. } => Err("OLTP API supervision requires POSIX descriptors"),
        #[cfg(feature = "olap")]
        Command::Api {
            config,
            cold_start,
            migrate_catalog,
        } => {
            nabedra_database_server::cluster::runtime::run_api_configured(
                config,
                cold_start,
                migrate_catalog,
            )
            .await
        }
        #[cfg(feature = "olap")]
        Command::Worker { bootstrap } => {
            nabedra_database_server::node::runtime::run_worker(bootstrap).await
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("nabedra-db: {message}");
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

#[cfg(all(feature = "olap", feature = "s3"))]
async fn run(options: nabedra_database_server::config::ServeOptions) -> Result<(), &'static str> {
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

#[cfg(all(feature = "olap", not(feature = "s3")))]
async fn run(options: nabedra_database_server::config::ServeOptions) -> Result<(), &'static str> {
    let config = nabedra_database_server::config::Config::from_env(options)
        .map_err(|_| "configuration invalid")?;
    if let (Some(cert), Some(key)) = (&config.options().tls_cert, &config.options().tls_key) {
        let _ = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .map_err(|_| "TLS configuration invalid")?;
    }
    Err("unsupported configuration: legacy serve requires the s3 feature")
}

#[cfg(feature = "cache")]
fn main() -> ExitCode {
    use nabedra_database_server::cache;
    use nabedra_database_server::cli::CacheCommand;

    let result = match Cli::parse().command {
        Command::Serve { config } => cache::run_standalone(&config),
        Command::Cache {
            command: CacheCommand::Controller { config },
        } => cache::run_controller(&config),
        Command::Cache {
            command: CacheCommand::Worker { config },
        } => cache::run_worker(&config),
        Command::Cache {
            command:
                CacheCommand::Admin {
                    config,
                    operation,
                    command,
                },
        } => cache::run_admin(&config, operation, command),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("nabedra-db: {error}");
            ExitCode::FAILURE
        }
    }
}
