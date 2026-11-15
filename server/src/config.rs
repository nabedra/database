//! Synchronous startup validation. AWS resolution and TLS parsing belong to bootstrap.
use crate::{partition_size::PartitionSize, storage::Namespace};
use std::{
    env::{self, VarError},
    fmt,
    fs::File,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

/// Plain inputs for the future CLI parser.
#[derive(Debug, Clone, clap::Args)]
pub struct ServeOptions {
    /// Confirm all previous catalog writers are stopped before migrating format 1/2/3.
    #[arg(long, default_value_t = false)]
    pub migrate_catalog: bool,
    /// Fence a previous stopped local run and abandon its pending memory queue.
    #[arg(long, default_value_t = false)]
    pub cold_start: bool,
    /// JSON execution limits; when supplied these override legacy queue and partition flags.
    #[arg(long)]
    pub execution_config: Option<PathBuf>,
    /// JSON worker limits; when supplied max_threads overrides --workers.
    #[arg(long)]
    pub resources_config: Option<PathBuf>,
    /// IP address to bind.
    #[arg(long, default_value_t = ServeOptions::default().host)]
    pub host: IpAddr,
    /// Port to bind.
    #[arg(long, default_value_t = ServeOptions::default().port)]
    pub port: u16,
    /// Worker count. Config requires at least one worker.
    #[arg(long, default_value_t = ServeOptions::default().workers)]
    pub workers: usize,
    /// Maximum partition size, with unit B, KB, MB, KiB or MiB (at most 64 MiB).
    #[arg(long, visible_alias = "partition_size", default_value_t = PartitionSize::default())]
    pub partition_size: PartitionSize,
    /// Maximum queued requests, including zero.
    #[arg(long, default_value_t = ServeOptions::default().queue_size)]
    pub queue_size: usize,
    /// TLS certificate path. Config requires a readable certificate and key pair.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    /// TLS private key path.
    #[arg(long)]
    pub tls_key: Option<PathBuf>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            migrate_catalog: false,
            cold_start: false,
            execution_config: None,
            resources_config: None,
            host: Ipv4Addr::LOCALHOST.into(),
            port: 8000,
            workers: 4,
            partition_size: PartitionSize::default(),
            queue_size: 64,
            tls_cert: None,
            tls_key: None,
        }
    }
}

/// Validated startup values. Accessors keep the validation invariant intact.
pub struct Config {
    options: ServeOptions,
    bucket: String,
    namespace: Namespace,
    token: String,
    admission_limit: usize,
    execution: ExecutionConfig,
    resources: WorkerResources,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("options", &self.options)
            .field("bucket", &self.bucket)
            .field("namespace", &self.namespace)
            .field("token", &"[REDACTED]")
            .field("admission_limit", &self.admission_limit)
            .finish()
    }
}

/// Static diagnostics deliberately omit environment values, paths, and OS errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigError(&'static str);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}

impl Config {
    pub fn from_env(options: ServeOptions) -> Result<Self, ConfigError> {
        Self::from_lookup(options, |name| env::var(name))
    }

    /// Reads only MeshDB settings. AWS credentials and region stay with Storage::connect.
    /// Missing prefix uses the default; an empty or non-Unicode prefix is invalid.
    pub fn from_lookup(
        mut options: ServeOptions,
        mut lookup: impl FnMut(&str) -> Result<String, VarError>,
    ) -> Result<Self, ConfigError> {
        let supplied_resources: Option<WorkerResources> = options
            .resources_config
            .as_deref()
            .map(read_resource_config)
            .transpose()?;
        if let Some(resources) = supplied_resources {
            resources.validate()?;
            options.workers = resources.max_threads as usize;
        }
        let supplied_execution: Option<ExecutionConfig> = options
            .execution_config
            .as_deref()
            .map(read_resource_config)
            .transpose()?;
        if let Some(execution) = &supplied_execution {
            execution.validate()?;
            options.partition_size = execution.partition_size;
        }
        if options.workers == 0 || options.workers > 256 {
            return Err(ConfigError("workers must be at least 1"));
        }
        let admission_limit = options
            .workers
            .checked_add(options.queue_size)
            .filter(|&limit| limit <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or(ConfigError(
                "workers plus queue size exceeds the supported limit",
            ))?;
        let resources = supplied_resources.unwrap_or(WorkerResources {
            max_threads: options.workers as u64,
            ..Default::default()
        });
        let execution = supplied_execution.unwrap_or(ExecutionConfig {
            max_pending_requests: admission_limit as u64,
            partition_size: options.partition_size,
            ..Default::default()
        });
        let admission_limit = usize::try_from(execution.max_pending_requests)
            .map_err(|_| ConfigError("invalid admission limit"))?;
        resources.validate_execution(&execution)?;
        let bucket = required(
            &mut lookup,
            "MESHDB_S3_BUCKET",
            "MESHDB_S3_BUCKET is required and must be nonempty",
        )?;
        let token = required(
            &mut lookup,
            "MESHDB_TOKEN",
            "MESHDB_TOKEN is required and must be nonempty",
        )?;
        let prefix = match lookup("MESHDB_S3_PREFIX") {
            Ok(prefix) => prefix,
            Err(VarError::NotPresent) => "meshdb-v0/".into(),
            Err(_) => return Err(ConfigError("MESHDB_S3_PREFIX is invalid")),
        };
        let namespace =
            Namespace::new(&prefix).map_err(|_| ConfigError("MESHDB_S3_PREFIX is invalid"))?;
        match (&options.tls_cert, &options.tls_key) {
            (None, None) => {}
            (Some(cert), Some(key)) => {
                readable_file(
                    cert,
                    ConfigError("TLS certificate must be a readable regular file"),
                )?;
                readable_file(key, ConfigError("TLS key must be a readable regular file"))?;
            }
            _ => {
                return Err(ConfigError(
                    "TLS certificate and key must be supplied together",
                ));
            }
        }
        Ok(Self {
            options,
            bucket,
            namespace,
            token,
            admission_limit,
            execution,
            resources,
        })
    }

    pub fn execution(&self) -> ExecutionConfig {
        self.execution.clone()
    }
    pub fn resources(&self) -> WorkerResources {
        self.resources
    }
    pub fn options(&self) -> &ServeOptions {
        &self.options
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    /// Intentional access for HTTP authentication state. Never log this value.
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn admission_limit(&self) -> usize {
        self.admission_limit
    }
}

fn required(
    lookup: &mut impl FnMut(&str) -> Result<String, VarError>,
    name: &str,
    diagnostic: &'static str,
) -> Result<String, ConfigError> {
    lookup(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ConfigError(diagnostic))
}

fn readable_file(path: &Path, error: ConfigError) -> Result<(), ConfigError> {
    // Reject special files before opening so a named pipe cannot block startup.
    if !std::fs::metadata(path).map_err(|_| error)?.is_file() {
        return Err(error);
    }
    let file = File::open(path).map_err(|_| error)?;
    if !file.metadata().map_err(|_| error)?.is_file() {
        return Err(error);
    }
    // The opened handle closes here. Bootstrap reopens and parses PEM before binding.
    Ok(())
}

/// Versioned cluster-wide queue and logical-request limits.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub max_workers_per_request: u64,
    pub max_pending_requests: u64,
    pub max_queue_bytes_per_replica: u64,
    pub max_retained_result_bytes_per_worker: u64,
    pub result_retention_ms: u64,
    pub queue_replication: QueueReplication,
    pub queue_lifetime: QueueLifetime,
    pub partition_size: PartitionSize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueReplication {
    AllReadyWorkers,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueLifetime {
    LiveCluster,
}
impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_workers_per_request: 4,
            max_pending_requests: 1024,
            max_queue_bytes_per_replica: 256 << 20,
            max_retained_result_bytes_per_worker: 64 << 20,
            result_retention_ms: 300_000,
            queue_replication: QueueReplication::AllReadyWorkers,
            queue_lifetime: QueueLifetime::LiveCluster,
            partition_size: PartitionSize::default(),
        }
    }
}
impl ExecutionConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=2048).contains(&self.max_workers_per_request)
            || !(1..=1_000_000).contains(&self.max_pending_requests)
            || self.max_queue_bytes_per_replica == 0
            || self.max_retained_result_bytes_per_worker == 0
            || self.max_queue_bytes_per_replica > isize::MAX as u64
            || self.max_retained_result_bytes_per_worker > isize::MAX as u64
            || !(1..=86_400_000).contains(&self.result_retention_ms)
            || self
                .max_queue_bytes_per_replica
                .checked_add(self.max_retained_result_bytes_per_worker)
                .is_none()
        {
            return Err(ConfigError("invalid execution limits"));
        }
        Ok(())
    }
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Local ceilings advertised in the signed enrollment and checked before readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResources {
    pub max_threads: u64,
    pub memory_limit_bytes: u64,
    pub task_memory_limit_bytes: u64,
    pub control_memory_reserve_bytes: u64,
}
impl Default for WorkerResources {
    fn default() -> Self {
        Self {
            max_threads: 4,
            memory_limit_bytes: 8 << 30,
            task_memory_limit_bytes: 512 << 20,
            control_memory_reserve_bytes: 256 << 20,
        }
    }
}
impl WorkerResources {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=256).contains(&self.max_threads)
            || self.memory_limit_bytes > isize::MAX as u64
            || self.task_memory_limit_bytes == 0
            || self.control_memory_reserve_bytes == 0
            || self.control_memory_reserve_bytes >= self.memory_limit_bytes
            || self.task_memory_limit_bytes
                > self.memory_limit_bytes - self.control_memory_reserve_bytes
        {
            return Err(ConfigError("invalid worker resource limits"));
        }
        Ok(())
    }
    pub fn working_bytes(&self) -> u64 {
        self.memory_limit_bytes
            .saturating_sub(self.control_memory_reserve_bytes)
    }
    pub fn validate_execution(&self, execution: &ExecutionConfig) -> Result<(), ConfigError> {
        self.validate()?;
        execution.validate()?;
        let reserved = execution
            .max_queue_bytes_per_replica
            .checked_add(execution.max_retained_result_bytes_per_worker)
            .and_then(|bytes| bytes.checked_add(self.control_memory_reserve_bytes))
            .ok_or(ConfigError("worker memory accounting overflow"))?;
        if reserved >= self.memory_limit_bytes
            || self.task_memory_limit_bytes > self.memory_limit_bytes - reserved
        {
            return Err(ConfigError("queue, results and task exceed worker memory"));
        }
        Ok(())
    }
}

fn read_resource_config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    use std::io::Read;
    let file =
        File::open(path).map_err(|_| ConfigError("resource configuration is unavailable"))?;
    if !file
        .metadata()
        .map_err(|_| ConfigError("resource configuration is unavailable"))?
        .is_file()
    {
        return Err(ConfigError("resource configuration must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError("resource configuration is unavailable"))?;
    if bytes.len() > 16 * 1024 {
        return Err(ConfigError("resource configuration exceeds byte limit"));
    }
    serde_json::from_slice(&bytes).map_err(|_| ConfigError("invalid resource configuration"))
}
