use std::fs::OpenOptions;
use std::io::{self, Read};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde::Deserialize;
use tokio_rustls::TlsAcceptor;

use super::auth::{Acl, UserConfig};
use super::wire::FrameLimits;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const MAX_CONFIG_BYTES: usize = 256 * KIB;
const MAX_PEM_BYTES: usize = 128 * KIB;
const MAX_CERT_DER_BYTES: usize = 64 * KIB;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Config {
    pub listen: SocketAddr,
    pub plaintext_loopback: bool,
    pub tls: Option<TlsConfig>,
    pub users: Vec<UserConfig>,
    pub limits: Limits,
    pub eviction: Eviction,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: (Ipv4Addr::LOCALHOST, 6379).into(),
            plaintext_loopback: false,
            tls: None,
            users: Vec::new(),
            limits: Limits::default(),
            eviction: Eviction::SampledLru,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Eviction {
    SampledLru,
    Noeviction,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Limits {
    pub memory_bytes: usize,
    pub data_bytes: usize,
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub max_pending: usize,
    pub max_frame_bytes: usize,
    pub max_pipeline_bytes: usize,
    pub max_arguments: usize,
    pub max_reply_bytes: usize,
    pub handshake_timeout_ms: u64,
    pub io_timeout_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_bytes: 512 * MIB,
            data_bytes: 128 * MIB,
            max_connections: 64,
            max_handshakes: 16,
            max_pending: 32,
            max_frame_bytes: 1088 * KIB,
            max_pipeline_bytes: 1088 * KIB,
            max_arguments: 128,
            max_reply_bytes: 1088 * KIB,
            handshake_timeout_ms: 5_000,
            io_timeout_ms: 60_000,
        }
    }
}

impl Limits {
    pub fn frame(self) -> FrameLimits {
        FrameLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_arguments: self.max_arguments,
            max_bulk_bytes: self.max_frame_bytes.min(MIB),
        }
    }

    pub fn io_timeout(self) -> Duration {
        Duration::from_millis(self.io_timeout_ms)
    }

    pub fn handshake_timeout(self) -> Duration {
        Duration::from_millis(self.handshake_timeout_ms)
    }

    fn turn_reserve(self, tls: bool) -> Option<usize> {
        // Per connection charge includes buffer capacity, exact owned arguments,
        // response work, references that can pin deleted values, and task state.
        let connection_bytes = self
            .max_pipeline_bytes
            .checked_mul(2)?
            .checked_add(self.max_frame_bytes)?
            .checked_add(self.max_reply_bytes.checked_mul(2)?)?
            .checked_add(self.max_arguments.checked_mul(640)?)?
            .checked_add(if tls { 320 * KIB } else { 64 * KIB })?;
        let connections = connection_bytes.checked_mul(self.max_connections)?;
        self.memory_bytes
            .checked_sub(connections)?
            .checked_sub(self.data_bytes)
    }
}

pub(super) struct ListenerConfig {
    pub listen: SocketAddr,
    pub tls: Option<TlsAcceptor>,
    pub acl: Acl,
    pub anonymous: bool,
    pub limits: Limits,
    pub turn_reserve_bytes: usize,
}

impl Config {
    pub fn read(path: &Path) -> io::Result<Self> {
        read_toml(path)
    }

    pub fn transport(self, root: &Path) -> io::Result<ListenerConfig> {
        let has_tls = self.tls.is_some();
        if (!has_tls && (!self.plaintext_loopback || !self.listen.ip().is_loopback()))
            || (has_tls && (self.plaintext_loopback || self.users.is_empty()))
        {
            return Err(invalid(
                "cache requires TLS and ACLs, or explicit plaintext on loopback",
            ));
        }
        let limits = self.limits;
        if limits.data_bytes == 0
            || !(1..=100_000).contains(&limits.max_connections)
            || !(1..=limits.max_connections).contains(&limits.max_handshakes)
            || !(1..=limits.max_connections).contains(&limits.max_pending)
            || !(64..=4 * MIB).contains(&limits.max_frame_bytes)
            || !(limits.max_frame_bytes..=4 * MIB).contains(&limits.max_pipeline_bytes)
            || !(2..=1024).contains(&limits.max_arguments)
            || !(64..=4 * MIB).contains(&limits.max_reply_bytes)
            || !(1..=60_000).contains(&limits.handshake_timeout_ms)
            || !(1..=86_400_000).contains(&limits.io_timeout_ms)
        {
            return Err(invalid("invalid cache resource limits"));
        }
        let turn_reserve_bytes = limits
            .turn_reserve(has_tls)
            .filter(|reserve| *reserve >= MIB)
            .ok_or_else(|| invalid("cache limits exceed the worker memory budget"))?;
        let anonymous = self.users.is_empty();
        let acl = Acl::new(self.users).map_err(|_| invalid("invalid cache ACL configuration"))?;
        let tls = self.tls.map(|tls| tls.acceptor(root)).transpose()?;
        Ok(ListenerConfig {
            listen: self.listen,
            tls,
            acl,
            anonymous,
            limits,
            turn_reserve_bytes,
        })
    }
}

impl TlsConfig {
    fn acceptor(&self, root: &Path) -> io::Result<TlsAcceptor> {
        let certs = read_bounded(&root.join(&self.certificate), MAX_PEM_BYTES)?;
        let key = read_bounded(&root.join(&self.private_key), MAX_PEM_BYTES)?;
        let certificates = CertificateDer::pem_slice_iter(&certs)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("invalid cache TLS certificate chain"))?;
        if certificates.is_empty()
            || certificates.len() > 16
            || certificates.iter().map(|cert| cert.len()).sum::<usize>() > MAX_CERT_DER_BYTES
        {
            return Err(invalid("cache TLS certificate chain exceeds its limits"));
        }
        let key = PrivateKeyDer::from_pem_slice(&key)
            .map_err(|_| invalid("invalid cache TLS private key"))?;
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid("invalid cache TLS protocols"))?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|_| invalid("invalid cache TLS server identity"))?;
        Ok(TlsAcceptor::from(Arc::new(tls)))
    }
}

pub(super) fn read_toml<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    let contents = read_bounded(path, MAX_CONFIG_BYTES)?;
    toml::from_slice(&contents).map_err(|_| invalid("invalid cache TOML configuration"))
}

pub(super) fn read_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid(
            "cache configuration material must be a regular file",
        ));
    }
    let mut contents = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut contents)?;
    if contents.len() > limit {
        return Err(invalid(
            "cache configuration material exceeds its byte limit",
        ));
    }
    Ok(contents)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
