//! Synchronous startup validation. AWS resolution and TLS parsing belong to bootstrap.
use crate::{partition_size::PartitionSize, storage::Namespace};
#[cfg(test)]
use nabedra_resources::config::DIRECT_QUEUED_LIFETIME_ALLOWANCE_MS;
pub use nabedra_resources::config::{
    ConfigError, ExecutionConfig, MAX_RESULT_RETENTION_MS, QueueLifetime, QueueReplication,
    WorkerResources,
};
pub(crate) use nabedra_resources::config::direct_oltp_default;
#[cfg(all(feature = "oltp", test))]
use nabedra_resources::config::DIRECT_MARKER_RETENTION_MS;
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

impl Config {
    pub fn from_env(options: ServeOptions) -> Result<Self, ConfigError> {
        Self::from_lookup(options, |name| env::var(name))
    }

    /// Reads only Nabedra Database settings. AWS credentials and region stay with Storage::connect.
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
            return Err(ConfigError::new("workers must be at least 1"));
        }
        let admission_limit = options
            .workers
            .checked_add(options.queue_size)
            .filter(|&limit| limit <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or(ConfigError::new(
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
            .map_err(|_| ConfigError::new("invalid admission limit"))?;
        resources.validate_execution(&execution)?;
        let bucket = required(
            &mut lookup,
            "NABEDRA_S3_BUCKET",
            "NABEDRA_S3_BUCKET is required and must be nonempty",
        )?;
        let token = required(
            &mut lookup,
            "NABEDRA_TOKEN",
            "NABEDRA_TOKEN is required and must be nonempty",
        )?;
        let prefix = match lookup("NABEDRA_S3_PREFIX") {
            Ok(prefix) => prefix,
            Err(VarError::NotPresent) => "nabedra-v0/".into(),
            Err(_) => return Err(ConfigError::new("NABEDRA_S3_PREFIX is invalid")),
        };
        let namespace = Namespace::new(&prefix)
            .map_err(|_| ConfigError::new("NABEDRA_S3_PREFIX is invalid"))?;
        match (&options.tls_cert, &options.tls_key) {
            (None, None) => {}
            (Some(cert), Some(key)) => {
                readable_file(
                    cert,
                    ConfigError::new("TLS certificate must be a readable regular file"),
                )?;
                readable_file(
                    key,
                    ConfigError::new("TLS key must be a readable regular file"),
                )?;
            }
            _ => {
                return Err(ConfigError::new(
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
        .ok_or(ConfigError::new(diagnostic))
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

fn read_resource_config<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    use std::io::Read;
    let file =
        File::open(path).map_err(|_| ConfigError::new("resource configuration is unavailable"))?;
    if !file
        .metadata()
        .map_err(|_| ConfigError::new("resource configuration is unavailable"))?
        .is_file()
    {
        return Err(ConfigError::new(
            "resource configuration must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError::new("resource configuration is unavailable"))?;
    if bytes.len() > 16 * 1024 {
        return Err(ConfigError::new(
            "resource configuration exceeds byte limit",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|_| ConfigError::new("invalid resource configuration"))
}

#[cfg(all(test, feature = "oltp"))]
mod direct_retention_tests {
    use super::*;

    /// RV-FINAL M2, Q05/Q06, O15: a queued receipt kept longer than the
    /// direct markers minus the queued-lifetime allowance is refused at
    /// startup, so a marker outlives every receipt of a request that finished
    /// within the allowance.
    #[test]
    fn q05_q06_o15_rv_final_m2_result_retention_leaves_room_for_queued_lifetime() {
        assert_eq!(MAX_RESULT_RETENTION_MS, 43_200_000);
        assert_eq!(
            crate::oltp::mvcc::store::direct::DEFAULT_DIRECT_DEDUP_RETENTION_MS,
            DIRECT_MARKER_RETENTION_MS
        );
        let with = |result_retention_ms| ExecutionConfig {
            result_retention_ms,
            ..ExecutionConfig::default()
        };
        assert!(ExecutionConfig::default().validate().is_ok());
        assert!(with(1).validate().is_ok());
        assert!(with(MAX_RESULT_RETENTION_MS).validate().is_ok());
        assert!(
            with(MAX_RESULT_RETENTION_MS)
                .result_retention_ms
                .checked_add(DIRECT_QUEUED_LIFETIME_ALLOWANCE_MS)
                .is_some_and(|end| end <= DIRECT_MARKER_RETENTION_MS)
        );
        for refused in [0, MAX_RESULT_RETENTION_MS + 1, 86_400_000, u64::MAX] {
            assert!(with(refused).validate().is_err(), "{refused}");
        }
    }
}
