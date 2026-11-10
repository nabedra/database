//! Synchronous startup validation. AWS resolution and TLS parsing belong to bootstrap.
use crate::storage::Namespace;
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
    /// IP address to bind.
    #[arg(long, default_value_t = ServeOptions::default().host)]
    pub host: IpAddr,
    /// Port to bind.
    #[arg(long, default_value_t = ServeOptions::default().port)]
    pub port: u16,
    /// Worker count. Config requires at least one worker.
    #[arg(long, default_value_t = ServeOptions::default().workers)]
    pub workers: usize,
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
            host: Ipv4Addr::LOCALHOST.into(),
            port: 8000,
            workers: 4,
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
        options: ServeOptions,
        mut lookup: impl FnMut(&str) -> Result<String, VarError>,
    ) -> Result<Self, ConfigError> {
        if options.workers == 0 {
            return Err(ConfigError("workers must be at least 1"));
        }
        let admission_limit = options
            .workers
            .checked_add(options.queue_size)
            .filter(|&limit| limit <= tokio::sync::Semaphore::MAX_PERMITS)
            .ok_or(ConfigError(
                "workers plus queue size exceeds the supported limit",
            ))?;
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
        })
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
