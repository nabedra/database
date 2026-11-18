//! Existing Python supervisor protocol for the OLTP API. Local composition
//! starts the actual shared worker runtime; remote composition uses the same
//! HTTP/runtime contract and is added independently of the supervisor framing.
use super::{invalid, read_config, valid_database, worker_runtime::WorkerRuntime};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use axum_server::tls_rustls::RustlsConfig;
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{Instant, timeout_at},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    engine: String,
    bind: SocketAddr,
    database: String,
    auth_file: PathBuf,
    topology: Topology,
    #[serde(default)]
    tls: Option<Tls>,
    #[serde(default)]
    worker: WorkerConfig,
    #[serde(default)]
    cluster_name: Option<String>,
}
#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Topology {
    Local { data_dir: PathBuf },
    Remote(super::remote_api::RemoteConfig),
}
enum ActiveRuntime {
    Local(std::sync::Arc<WorkerRuntime>),
    Remote(std::sync::Arc<super::remote_api::RemoteRuntime>),
}
impl ActiveRuntime {
    fn cluster_id(&self) -> uuid::Uuid {
        match self {
            Self::Local(runtime) => runtime.cluster_id(),
            Self::Remote(runtime) => runtime.cluster_id(),
        }
    }
    fn application(
        &self,
        token: String,
        database: String,
        admin: String,
    ) -> Result<axum::Router, ProtocolError> {
        match self {
            Self::Local(runtime) => runtime.application_with_admin(token, admin),
            Self::Remote(runtime) => runtime.application(token, database, admin),
        }
    }
    fn stop_accepting(&self) {
        match self {
            Self::Local(runtime) => runtime.stop_accepting(),
            Self::Remote(runtime) => runtime.stop_accepting(),
        }
    }
    async fn drain(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Local(runtime) => runtime.drain().await,
            Self::Remote(runtime) => runtime.drain().await,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tls {
    cert_file: PathBuf,
    key_file: PathBuf,
    #[serde(default)]
    ca_file: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WorkerConfig {
    resources: WorkerResources,
    execution: Option<ExecutionConfig>,
    queue_size: u64,
}
impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            resources: WorkerResources::default(),
            execution: None,
            queue_size: 64,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    admin_token: String,
    data_token: String,
    enrollment_token: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialPolicy {
    mode: String,
    desired: u64,
    min_nodes: u64,
    max_nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Init {
    version: u32,
    #[serde(rename = "type")]
    kind: String,
    name: String,
    initial_policy: InitialPolicy,
    provisioning: String,
    bridge: Option<serde_json::Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    version: u32,
    #[serde(rename = "type")]
    kind: String,
}

pub async fn run_api_configured(
    path: PathBuf,
    cold_start: bool,
    migrate_catalog: bool,
) -> Result<(), &'static str> {
    let mut channel = crate::supervisor::open().map_err(|_| "protocol_error")?;
    let result = process(&path, &mut channel, cold_start, migrate_catalog).await;
    if let Err(error) = &result {
        let code = match error.code {
            ErrorCode::ValidationError | ErrorCode::UnsupportedOperation => "invalid_config",
            ErrorCode::Conflict => "incompatible_cluster",
            _ => "startup_failed",
        };
        let _ = crate::supervisor::send(
            &mut channel,
            &serde_json::json!({"version":1,"type":"fatal","code":code}),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    }
    result.map_err(|_| "OLTP API process failed")
}
async fn process(
    path: &Path,
    channel: &mut tokio::net::UnixStream,
    cold_start: bool,
    migrate_catalog: bool,
) -> Result<(), ProtocolError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let init: Init = serde_json::from_slice(
        &crate::supervisor::frame(channel, Some(deadline))
            .await
            .map_err(|_| unavailable())?,
    )
    .map_err(|_| invalid("invalid supervisor initialization"))?;
    crate::protocol::validate_identifier(&init.name)?;
    if init.version != 1 || init.kind != "init" || migrate_catalog {
        return Err(invalid("invalid OLTP supervisor initialization"));
    }
    let mut config: Config = read_config(path)?;
    if config.version != 1
        || config.engine != "oltp"
        || !valid_database(&config.database)
        || (!config.bind.ip().is_loopback() && config.tls.is_none())
        || config
            .cluster_name
            .as_ref()
            .is_some_and(|name| name != &init.name)
    {
        return Err(invalid("invalid OLTP API configuration"));
    }
    if init.provisioning != "external"
        || init.bridge.is_some()
        || init.initial_policy.mode != "fixed"
        || init.initial_policy.desired != 1
        || init.initial_policy.min_nodes != 1
        || init.initial_policy.max_nodes != 1
    {
        return Err(ProtocolError::new(
            ErrorCode::UnsupportedOperation,
            "local OLTP composition requires one local worker; use a remote deployment configuration for provider-managed workers",
            Outcome::NotApplied,
        ));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    absolute(parent, &mut config.auth_file);

    let auth: Auth = protected_json(&config.auth_file)?;
    let tokens = [&auth.admin_token, &auth.data_token, &auth.enrollment_token];
    if tokens.iter().any(|token| {
        !(43..=4096).contains(&token.len()) || !token.bytes().all(|byte| (33..=126).contains(&byte))
    }) || auth.admin_token == auth.data_token
        || auth.admin_token == auth.enrollment_token
        || auth.data_token == auth.enrollment_token
    {
        return Err(invalid("invalid protected API authentication"));
    }
    let resources = config.worker.resources;
    if resources.max_threads == 0 || resources.max_threads > 256 {
        return Err(invalid(
            "OLTP query compute ceiling must be between 1 and 256",
        ));
    }
    let execution = config.worker.execution.unwrap_or(ExecutionConfig {
        max_pending_requests: resources
            .max_threads
            .checked_add(config.worker.queue_size)
            .ok_or_else(|| invalid("invalid queue capacity"))?,
        ..Default::default()
    });
    resources
        .validate_execution(&execution)
        .map_err(|_| invalid("invalid OLTP worker resources"))?;
    let tls = if let Some(mut tls) = config.tls {
        absolute(parent, &mut tls.cert_file);
        absolute(parent, &mut tls.key_file);
        // Public CA material is not an authentication credential, but remains
        // an explicit regular bounded file when supplied in the configuration.
        if let Some(mut ca) = tls.ca_file {
            absolute(parent, &mut ca);
            regular(&ca, false)?;
        }
        regular(&tls.cert_file, false)?;
        regular(&tls.key_file, true)?;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        Some(
            RustlsConfig::from_pem_file(tls.cert_file, tls.key_file)
                .await
                .map_err(|_| invalid("invalid API TLS material"))?,
        )
    } else {
        None
    };
    let listener = TcpListener::bind(config.bind)
        .await
        .map_err(|_| unavailable())?;
    let address = listener.local_addr().map_err(|_| unavailable())?;
    let bound_url = format!(
        "{}://{}",
        if tls.is_some() { "https" } else { "http" },
        address
    );
    let runtime = timeout_at(deadline, async {
        match config.topology {
            Topology::Local { mut data_dir } => {
                absolute(parent, &mut data_dir);
                WorkerRuntime::open_configured(
                    &data_dir,
                    &config.database,
                    &auth.data_token,
                    execution,
                    resources,
                    cold_start,
                )
                .await
                .map(ActiveRuntime::Local)
            }
            Topology::Remote(remote) => {
                if cold_start {
                    return Err(invalid("API gateway cannot authorize a new worker run"));
                }
                super::remote_api::RemoteRuntime::open(
                    remote,
                    parent,
                    &config.database,
                    auth.data_token.clone(),
                    resources,
                )
                .await
                .map(ActiveRuntime::Remote)
            }
        }
    })
    .await
    .map_err(|_| unavailable())??;
    let result=async {
        let cluster_id=runtime.cluster_id();
        crate::supervisor::send(channel,&serde_json::json!({"version":1,"type":"prepared","cluster_id":cluster_id,"bound_url":bound_url,"authorize_url":null}),deadline).await.map_err(|_|unavailable())?;
        let activate:Message=serde_json::from_slice(&crate::supervisor::frame(channel,Some(deadline)).await.map_err(|_|unavailable())?).map_err(|_|invalid("invalid supervisor activation"))?;
        if activate.version!=1||activate.kind!="activate"{return Err(invalid("invalid supervisor activation"));}
        let application=runtime.application(auth.data_token,config.database,auth.admin_token)?;
        crate::supervisor::send(channel,&serde_json::json!({"version":1,"type":"ready","cluster_id":cluster_id,"bound_url":bound_url}),deadline).await.map_err(|_|unavailable())?;
        let mut terminate=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|_|unavailable())?;
        let mut interrupt=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).map_err(|_|unavailable())?;
        crate::network::serve_router(listener,application,tls,async {
            tokio::select!{_ = crate::supervisor::frame(channel,None)=>{},_ = terminate.recv()=>{},_ = interrupt.recv()=>{}}
            runtime.stop_accepting();
        }).await.map_err(|_|unavailable())
    }.await;
    runtime.stop_accepting();
    let drained = runtime.drain().await;
    result.and(drained)
}
fn absolute(parent: &Path, path: &mut PathBuf) {
    if path.is_relative() {
        *path = parent.join(&*path);
    }
}
pub(super) fn regular(path: &Path, secret: bool) -> Result<std::fs::File, ProtocolError> {
    use std::os::unix::{
        fs::PermissionsExt,
        fs::{MetadataExt, OpenOptionsExt},
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| invalid("API material unreadable"))?;
    let info = file
        .metadata()
        .map_err(|_| invalid("API material unreadable"))?;
    if !info.is_file()
        || info.len() > 65536
        || (secret
            && (info.permissions().mode() & 0o077 != 0 || info.uid() != unsafe { libc::geteuid() }))
    {
        return Err(invalid("invalid protected API material"));
    }
    Ok(file)
}
pub(super) fn protected_json<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<T, ProtocolError> {
    use std::io::Read;
    let mut bytes = Vec::new();
    regular(path, true)?
        .take(16385)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("protected API material unreadable"))?;
    if bytes.len() > 16384 {
        return Err(invalid("protected API material exceeds limit"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid protected API material"))
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "OLTP API supervisor unavailable",
        Outcome::Unknown,
    )
}
