//! Strict A05 outgoing worker templates and lifecycle timings, without I/O.
//! Use decode/encode/validate at trust boundaries. Path strings are adapter hints.
use crate::{partition_size::PartitionSize, protocol::deserialize_object, storage::Namespace};
use serde::{Deserialize, Deserializer, Serialize, ser::SerializeStruct};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::{self, Write},
};

pub const MAX_TEMPLATE_BYTES: usize = 64 * 1024;
/// Timing has a separate 4 KiB encoded limit, including input whitespace.
pub const MAX_TIMING_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationError {
    InvalidRecord,
    LimitExceeded,
    UnsupportedStorage,
}
impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRecord => "invalid control configuration",
            Self::LimitExceeded => "control configuration limit exceeded",
            Self::UnsupportedStorage => "unsupported template storage",
        })
    }
}
impl std::error::Error for ConfigurationError {}
fn check(ok: bool) -> Result<(), ConfigurationError> {
    if ok {
        Ok(())
    } else {
        Err(ConfigurationError::InvalidRecord)
    }
}
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}

// Field order is recursively lexical. This is the sole canonical serializer:
// derived Serialize emits complete records, required nulls and decimal integers.
macro_rules! record {
    ($name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Eq, Serialize)]
        pub struct $name { $($(#[$attr])* pub $field: $ty),* }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let w: Wire = deserialize_object(d)?;
                Ok(Self { $($field: w.$field),* })
            }
        }
    }
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComputeMode {
    #[default]
    Fixed,
    Serverless,
}
impl ComputeMode {
    pub fn is_fixed(&self) -> bool {
        *self == Self::Fixed
    }
}
record!(Template {
    #[serde(default, skip_serializing_if="ComputeMode::is_fixed")]
    mode: ComputeMode,
    #[serde(default, skip_serializing_if="crate::config::ExecutionConfig::is_default")]
    execution: crate::config::ExecutionConfig,
    #[serde(default, skip_serializing_if="Option::is_none")]
    resources: Option<crate::config::WorkerResources>,
    api_urls: Vec<String>,
    listen: String,
    operation_slots: u64,
    partition_size: String,
    partition_slots: u64,
    queue_size: u64,
    storage: Storage,
    #[serde(deserialize_with="nullable")]
    tls: Option<Tls>,
});
record!(Tls {
    ca_file: String,
    cert_file: String,
    key_file: String
});
record!(HttpOptions {
    base_url: String,
    #[serde(deserialize_with="nullable")]
    ca_file: Option<String>,
    namespace: String,
    token_file: String,
});
record!(S3Options {
    #[serde(default, skip_serializing_if="Option::is_none")]
    region: Option<String>,
    bucket: String,
    namespace: String
});

#[derive(Clone, PartialEq, Eq)]
pub enum Storage {
    Http(HttpOptions),
    S3(S3Options),
}
impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Storage { .. }")
    }
}
impl Serialize for Storage {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut record = s.serialize_struct("Storage", 2)?;
        match self {
            Self::Http(options) => {
                record.serialize_field("driver", "http")?;
                record.serialize_field("options", options)?;
            }
            Self::S3(options) => {
                record.serialize_field("driver", "s3")?;
                record.serialize_field("options", options)?;
            }
        }
        record.end()
    }
}
impl<'de> Deserialize<'de> for Storage {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            driver: String,
            options: Box<serde_json::value::RawValue>,
        }
        let wire: Wire = deserialize_object(d)?;
        // RawValue preserves duplicate fields and exact numeric spellings until
        // the selected fixed option record is parsed. No arbitrary options map.
        match wire.driver.as_str() {
            "http" => serde_json::from_str(wire.options.get()).map(Self::Http),
            "s3" => serde_json::from_str(wire.options.get()).map(Self::S3),
            _ => return Err(serde::de::Error::custom("unsupported storage")),
        }
        .map_err(|_| serde::de::Error::custom("invalid storage options"))
    }
}
record!(Timing {
    cooldown_ms: u64,
    drain_ms: u64,
    expiry_ms: u64,
    heartbeat_ms: u64,
    lease_ms: u64,
    max_attempts: u64,
    readiness_ms: u64,
    renew_ms: u64,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    stabilization_ms: u64,
});
impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat_ms: 5000,
            expiry_ms: 15000,
            readiness_ms: 300000,
            drain_ms: 300000,
            lease_ms: 15000,
            renew_ms: 5000,
            stabilization_ms: 60000,
            cooldown_ms: 30000,
            retry_initial_ms: 1000,
            retry_max_ms: 30000,
            max_attempts: 8,
        }
    }
}
fn path(s: &str) -> bool {
    !s.is_empty() && s.len() <= 2048
}
fn namespace(s: &str) -> bool {
    s.len() <= 1024 && Namespace::new(s).is_ok()
}

pub(crate) fn listen(s: &str) -> bool {
    if s.len() > 256 || !s.is_ascii() {
        return false;
    }
    let Some((host, port)) = s.rsplit_once(':') else {
        return false;
    };
    if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) || port.parse::<u16>().is_err()
    {
        return false;
    }
    if let Some(host) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return host.parse::<std::net::Ipv6Addr>().is_ok();
    }
    if host.contains('.') && host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return host.parse::<std::net::Ipv4Addr>().is_ok();
    }
    !host.is_empty()
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}
pub(crate) fn api_url(s: &str) -> bool {
    super::member::endpoint(s)
        && s.split_once("://")
            .is_some_and(|(_, tail)| tail.find('/').is_none_or(|i| &tail[i..] == "/"))
}
// These adapter syntax guards are private inside constructors in storage/http
// and storage/s3. Keep this local check aligned without constructing clients.
fn component_path(s: &str) -> bool {
    !s.is_empty()
        && s.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
        })
}
fn http_url(s: &str) -> bool {
    if s.len() > 2048
        || s.contains(['?', '#', '\\', '%'])
        || !s.bytes().all(|b| (33..=126).contains(&b))
    {
        return false;
    }
    let Ok(uri) = s.parse::<hyper::Uri>() else {
        return false;
    };
    let Some(authority) = uri.authority() else {
        return false;
    };
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || authority.as_str().contains('@')
        || uri.host().is_none()
    {
        return false;
    }
    let host = authority.host();
    if host.is_empty() {
        return false;
    }
    let Some(suffix) = authority.as_str().strip_prefix(host) else {
        return false;
    };
    if !suffix.is_empty() {
        let Some(port) = suffix.strip_prefix(':') else {
            return false;
        };
        if port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || port.parse::<u16>().is_err()
        {
            return false;
        }
    }
    let Some(path) = uri.path().strip_prefix('/') else {
        return false;
    };
    let path = path.strip_suffix('/').unwrap_or(path);
    (path.is_empty() && uri.path() == "/") || component_path(path)
}
#[cfg(feature = "s3")]
fn bucket(s: &str) -> bool {
    (3..=63).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
        && !s.contains("..")
        && s.parse::<std::net::Ipv4Addr>().is_err()
}
impl Template {
    pub fn worker_resources(&self) -> crate::config::WorkerResources {
        self.resources.unwrap_or(crate::config::WorkerResources {
            max_threads: self.partition_slots,
            ..Default::default()
        })
    }

    fn validate_fields(&self) -> Result<(), ConfigurationError> {
        self.execution
            .validate()
            .map_err(|_| ConfigurationError::InvalidRecord)?;
        check(
            self.execution.is_default()
                || self.partition_size.parse::<PartitionSize>().ok()
                    == Some(self.execution.partition_size),
        )?;
        self.worker_resources()
            .validate_execution(&self.execution)
            .map_err(|_| ConfigurationError::InvalidRecord)?;

        check(
            (1..=16).contains(&self.api_urls.len())
                && self.api_urls.iter().all(|s| api_url(s))
                && listen(&self.listen),
        )?;
        check(
            (1..=256).contains(&self.operation_slots)
                && (1..=256).contains(&self.partition_slots)
                && self.queue_size <= 4096,
        )?;
        check(
            self.partition_size.len() <= 64 && self.partition_size.parse::<PartitionSize>().is_ok(),
        )?;
        if let Some(tls) = &self.tls {
            check(path(&tls.ca_file) && path(&tls.cert_file) && path(&tls.key_file))?;
        }
        match &self.storage {
            Storage::Http(o) => {
                check(
                    http_url(&o.base_url)
                        && namespace(&o.namespace)
                        && path(&o.token_file)
                        && o.ca_file.as_deref().is_none_or(path),
                )?;
                // HTTP imposes the stricter ASCII path-component guard too.
                let ns =
                    Namespace::new(&o.namespace).map_err(|_| ConfigurationError::InvalidRecord)?;
                check(ns.as_str().strip_suffix('/').is_some_and(component_path))?;
            }
            Storage::S3(o) => {
                #[cfg(not(feature = "s3"))]
                {
                    let _ = o;
                    return Err(ConfigurationError::UnsupportedStorage);
                }
                #[cfg(feature = "s3")]
                check(
                    o.bucket.len() <= 1024
                        && bucket(&o.bucket)
                        && namespace(&o.namespace)
                        && o.region
                            .as_deref()
                            .is_none_or(crate::storage::context::valid_region),
                )?;
            }
        }
        Ok(())
    }
    /// SHA256(domain || canonical complete outgoing template), lowercase hex.
    /// Paths enter this exact fingerprint. Semantic attach identity is separate.
    pub fn fingerprint(&self) -> Result<String, ConfigurationError> {
        let bytes = self.encode()?;
        let mut hash = Sha256::new();
        // Existing control roots authenticate templates with this format domain.
        hash.update(b"meshdb.control-template.v1\0");
        hash.update(&bytes);
        Ok(hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }
}
impl Timing {
    fn validate_fields(&self) -> Result<(), ConfigurationError> {
        check(
            [
                self.heartbeat_ms,
                self.expiry_ms,
                self.readiness_ms,
                self.drain_ms,
                self.lease_ms,
                self.renew_ms,
                self.stabilization_ms,
                self.cooldown_ms,
                self.retry_initial_ms,
                self.retry_max_ms,
                self.max_attempts,
            ]
            .iter()
            .all(|v| *v > 0),
        )?;
        // Multiplication preserves strict half-lease comparison for odd leases.
        // Overflow fails closed. Future timestamp additions belong to callers.
        check(
            self.heartbeat_ms < self.expiry_ms
                && self
                    .renew_ms
                    .checked_mul(2)
                    .is_some_and(|v| v < self.lease_ms)
                && self.retry_initial_ms <= self.retry_max_ms,
        )
    }
}
// A lexical depth check before serde allocation also covers RawValue's otherwise
// opaque options. Strings/escapes are skipped; serde checks JSON syntax next.
pub(crate) fn input(bytes: &[u8], cap: usize) -> Result<(), ConfigurationError> {
    crate::protocol::json_input(bytes, cap).map_err(|error| match error {
        crate::protocol::JsonBoundsError::LimitExceeded => ConfigurationError::LimitExceeded,
        crate::protocol::JsonBoundsError::InvalidRecord => ConfigurationError::InvalidRecord,
    })
}

pub(crate) fn bounded(value: &impl Serialize, cap: usize) -> Result<Vec<u8>, ConfigurationError> {
    struct Writer {
        bytes: Vec<u8>,
        cap: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.cap - self.bytes.len() {
                return Err(io::Error::other("configuration limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| ConfigurationError::LimitExceeded)?;
    Ok(writer.bytes)
}
macro_rules! codec {
    ($name:ident, $cap:ident) => {
        impl $name {
            /// Checks encoded bytes/depth before allocating typed fields.
            pub fn decode(bytes: &[u8]) -> Result<Self, ConfigurationError> {
                input(bytes, $cap)?;
                let value: Self =
                    serde_json::from_slice(bytes).map_err(|_| ConfigurationError::InvalidRecord)?;
                value.validate_fields()?;
                Ok(value)
            }
            /// Validates constructed values before bounded canonical output.
            pub fn encode(&self) -> Result<Vec<u8>, ConfigurationError> {
                self.validate_fields()?;
                bounded(self, $cap)
            }
            pub fn validate(&self) -> Result<(), ConfigurationError> {
                self.encode().map(|_| ())
            }
        }
    };
}
codec!(Template, MAX_TEMPLATE_BYTES);
codec!(Timing, MAX_TIMING_BYTES);
