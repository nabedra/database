//! Private RPC channels for controllers, workers, and operators.
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nabedra_rt::time::timeout;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use rustls::server::WebPkiClientVerifier;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cache::config::read_bounded;

pub(super) type Fingerprint = [u8; 32];
const RPC_TIMEOUT: Duration = Duration::from_millis(800);
const IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PEM_BYTES: usize = 128 << 10;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Endpoint {
    pub host: String,
    pub port: u16,
    pub fingerprints: Vec<Fingerprint>,
}

impl Endpoint {
    pub fn server_name(&self) -> io::Result<ServerName<'static>> {
        if self.port == 0 || !(1..=2).contains(&self.fingerprints.len()) {
            return Err(invalid("invalid peer endpoint"));
        }
        ServerName::try_from(self.host.clone()).map_err(|_| invalid("invalid peer hostname"))
    }
}

pub(super) fn controller_endpoints<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<u64, Endpoint>, D::Error> {
    use serde::de::Error;

    BTreeMap::<String, Endpoint>::deserialize(deserializer)?
        .into_iter()
        .map(|(key, endpoint)| {
            let id = key
                .parse::<u64>()
                .ok()
                .filter(|id| *id != 0 && id.to_string() == key)
                .ok_or_else(|| D::Error::custom("invalid cache controller id"))?;
            endpoint.server_name().map_err(D::Error::custom)?;
            Ok((id, endpoint))
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsConfig {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone)]
pub(super) struct PeerTls {
    server: TlsAcceptor,
    client: TlsConnector,
    pub fingerprint: Fingerprint,
}

impl PeerTls {
    pub fn load(config: &TlsConfig, root: &Path) -> io::Result<Self> {
        let chain = read_certs(&root.join(&config.certificate))?;
        let fingerprint = digest(&chain[0]);
        let key = read_bounded(&root.join(&config.private_key), MAX_PEM_BYTES)?;
        let key = PrivateKeyDer::from_pem_slice(&key).map_err(|_| invalid("invalid peer key"))?;
        let mut roots = rustls::RootCertStore::empty();
        for ca in read_certs(&root.join(&config.ca))? {
            roots.add(ca).map_err(io::Error::other)?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            Arc::clone(&provider),
        )
        .build()
        .map_err(io::Error::other)?;
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(io::Error::other)?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(io::Error::other)?;
        Ok(Self {
            server: TlsAcceptor::from(Arc::new(server)),
            client: TlsConnector::from(Arc::new(client)),
            fingerprint,
        })
    }

    pub async fn connect(&self, endpoint: &Endpoint) -> io::Result<PeerConnection> {
        let name = endpoint.server_name()?;
        timeout(RPC_TIMEOUT, async {
            let stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await?;
            stream.set_nodelay(true)?;
            let tls = self.client.connect(name, stream).await?;
            let fingerprint = peer_fingerprint(tls.get_ref().1.peer_certificates())?;
            if !endpoint.fingerprints.contains(&fingerprint) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "peer certificate mismatch",
                ));
            }
            Ok(PeerConnection(tls))
        })
        .await?
    }
}

pub(super) struct PeerConnection(tokio_rustls::client::TlsStream<TcpStream>);

impl PeerConnection {
    pub async fn exchange(&mut self, request: &[u8], limit: usize) -> io::Result<Vec<u8>> {
        timeout(RPC_TIMEOUT, async {
            write_frame(&mut self.0, request, limit).await?;
            read_frame(&mut self.0, limit).await
        })
        .await?
    }

    pub async fn json<Q: Serialize, R: DeserializeOwned>(
        &mut self,
        value: &Q,
        limit: usize,
    ) -> io::Result<R> {
        let bytes = bounded_json(value, limit)?;
        let response = self.exchange(&bytes, limit).await?;
        serde_json::from_slice(&response).map_err(io::Error::other)
    }
}

pub(super) async fn serve<H, R>(
    listener: TcpListener,
    tls: PeerTls,
    max_connections: usize,
    frame_limit: usize,
    handler: H,
    stop: impl Future<Output = ()>,
) -> io::Result<()>
where
    H: Fn(Fingerprint, Vec<u8>) -> R + Send + Sync + 'static,
    R: Future<Output = io::Result<Vec<u8>>> + Send,
{
    if max_connections == 0 || max_connections > 128 || frame_limit > 16 << 20 {
        return Err(invalid("invalid peer server limits"));
    }
    let permits = Arc::new(Semaphore::new(max_connections));
    let handler = Arc::new(handler);
    let mut sessions = JoinSet::new();
    tokio::pin!(stop);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut stop => break Ok(()),
            _ = sessions.join_next(), if !sessions.is_empty() => {},
            incoming = listener.accept() => {
                let socket = match incoming {
                    Ok((socket, _)) => socket,
                    Err(error) => break Err(error),
                };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    continue;
                };
                let acceptor = tls.server.clone();
                let handler = Arc::clone(&handler);
                sessions.spawn(async move {
                    let _permit = permit;
                    let _ = serve_session(socket, acceptor, frame_limit, &*handler).await;
                });
            }
        }
    };
    sessions.shutdown().await;
    result
}

async fn serve_session<H, R>(
    socket: TcpStream,
    acceptor: TlsAcceptor,
    limit: usize,
    handler: &H,
) -> io::Result<()>
where
    H: Fn(Fingerprint, Vec<u8>) -> R,
    R: Future<Output = io::Result<Vec<u8>>>,
{
    socket.set_nodelay(true)?;
    let mut socket = timeout(RPC_TIMEOUT, acceptor.accept(socket)).await??;
    let fingerprint = peer_fingerprint(socket.get_ref().1.peer_certificates())?;
    loop {
        let request = timeout(IDLE_TIMEOUT, read_frame(&mut socket, limit)).await??;
        let response = timeout(RPC_TIMEOUT, handler(fingerprint, request)).await??;
        timeout(RPC_TIMEOUT, write_frame(&mut socket, &response, limit)).await??;
    }
}

async fn read_frame(socket: &mut (impl AsyncRead + Unpin), limit: usize) -> io::Result<Vec<u8>> {
    let size = socket.read_u32().await? as usize;
    if size == 0 || size > limit {
        return Err(invalid("peer frame exceeds its length bound"));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(io::Error::other)?;
    bytes.resize(size, 0);
    socket.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn write_frame(
    socket: &mut (impl AsyncWrite + Unpin),
    body: &[u8],
    limit: usize,
) -> io::Result<()> {
    if body.is_empty() || body.len() > limit {
        return Err(invalid("peer frame exceeds its length bound"));
    }
    socket
        .write_u32(u32::try_from(body.len()).map_err(io::Error::other)?)
        .await?;
    socket.write_all(body).await?;
    socket.flush().await
}

pub(super) fn bounded_json(value: &impl Serialize, limit: usize) -> io::Result<Vec<u8>> {
    struct Buffer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl io::Write for Buffer {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            let needed = self
                .bytes
                .len()
                .checked_add(data.len())
                .filter(|size| *size <= self.limit)
                .ok_or_else(|| invalid("peer JSON exceeds its frame bound"))?;
            if needed > self.bytes.capacity() {
                let new = self
                    .bytes
                    .capacity()
                    .saturating_mul(2)
                    .max(needed)
                    .min(self.limit);
                self.bytes
                    .try_reserve_exact(new - self.bytes.len())
                    .map_err(io::Error::other)?;
            }
            self.bytes.extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut output = Buffer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value).map_err(io::Error::other)?;
    Ok(output.bytes)
}

fn read_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let material = read_bounded(path, MAX_PEM_BYTES)?;
    let chain = CertificateDer::pem_slice_iter(&material)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid("invalid peer certificate"))?;
    if chain.is_empty()
        || chain.len() > 16
        || chain.iter().map(|cert| cert.len()).sum::<usize>() > 64 << 10
    {
        return Err(invalid("invalid peer certificate chain length"));
    }
    Ok(chain)
}

fn peer_fingerprint(chain: Option<&[CertificateDer<'_>]>) -> io::Result<Fingerprint> {
    chain
        .and_then(|chain| chain.first())
        .map(digest)
        .ok_or_else(|| invalid("missing authenticated peer certificate"))
}

fn digest(cert: &CertificateDer<'_>) -> Fingerprint {
    Sha256::digest(cert.as_ref()).into()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
