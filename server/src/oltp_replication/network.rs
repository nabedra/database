//! Mutual TLS with certificate pins and explicit cluster/group/incarnation routing.
//! Requests and concurrent connections are bounded. Snapshots use immutable
//! files and 64 KiB transfer buffers, without a logical snapshot-size ceiling.
use super::{Peer, SnapshotFile, TypeConfig};
use openraft::{
    Raft,
    entry::RaftEntry,
    errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse, VoteRequest,
        VoteResponse,
    },
    storage::RaftStateMachine,
    type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
const SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_APPEND_ENTRIES: usize = 1024;
// Keep a replication message near one bounded application request, so a batch
// cannot monopolize the unary transport while smaller control work is waiting.
const APPEND_TARGET_BYTES: usize = crate::protocol::MAX_BODY_BYTES + 64 * 1024;
const ALPN: &[u8] = b"nabedra-raft/1";

#[derive(Clone)]
pub struct TlsMaterial {
    client: Arc<rustls::ClientConfig>,
    server: Arc<rustls::ServerConfig>,
    certificate_sha256: String,
}
impl TlsMaterial {
    /// Only a CA-authenticated mutual TLS configuration can construct a transport.
    pub fn from_pem(certificate: &[u8], private_key: &[u8], ca: &[u8]) -> io::Result<Self> {
        let certificates: Vec<_> = CertificateDer::pem_slice_iter(certificate)
            .collect::<Result<_, _>>()
            .map_err(|_| invalid("invalid peer certificate"))?;
        let fingerprint = certificates
            .first()
            .map(|cert| hex(&Sha256::digest(cert.as_ref())))
            .ok_or_else(|| invalid("missing peer certificate"))?;
        let key = PrivateKeyDer::from_pem_slice(private_key)
            .map_err(|_| invalid("invalid peer private key"))?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(cert.map_err(|_| invalid("invalid peer CA"))?)
                .map_err(|_| invalid("invalid peer CA"))?;
        }
        if roots.is_empty() {
            return Err(invalid("missing peer CA"));
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            provider.clone(),
        )
        .build()
        .map_err(|_| invalid("invalid client certificate verifier"))?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("invalid peer TLS protocols"))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates.clone(), key.clone_key())
            .map_err(|_| invalid("peer certificate and key differ"))?;
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("invalid peer TLS protocols"))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, key)
            .map_err(|_| invalid("peer certificate and key differ"))?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        server.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            certificate_sha256: fingerprint,
        })
    }
    pub fn certificate_sha256(&self) -> &str {
        &self.certificate_sha256
    }
    pub(super) fn transports(&self, alpn: &[u8]) -> (TlsConnector, TlsAcceptor) {
        let mut client = (*self.client).clone();
        let mut server = (*self.server).clone();
        client.alpn_protocols = vec![alpn.to_vec()];
        server.alpn_protocols = vec![alpn.to_vec()];
        (
            TlsConnector::from(Arc::new(client)),
            TlsAcceptor::from(Arc::new(server)),
        )
    }
}

#[derive(Clone)]
pub struct NetworkFactory {
    local: Peer,
    tls: TlsMaterial,
    outgoing: Arc<Semaphore>,
    snapshot_slots: Arc<Semaphore>,
}
impl NetworkFactory {
    pub fn new(
        local: Peer,
        tls: TlsMaterial,
        outgoing: Arc<Semaphore>,
        snapshot_slots: Arc<Semaphore>,
    ) -> io::Result<Self> {
        validate_peer(&local)?;
        if local.certificate_sha256 != tls.certificate_sha256 {
            return Err(invalid("local certificate does not match enrollment"));
        }
        Ok(Self {
            local,
            tls,
            outgoing,
            snapshot_slots,
        })
    }
}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;
    async fn new_client(&mut self, target: u64, node: &Peer) -> Network {
        Network {
            factory: self.clone(),
            target,
            peer: node.clone(),
        }
    }
}
pub struct Network {
    factory: NetworkFactory,
    target: u64,
    peer: Peer,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    version: u32,
    engine: String,
    format_version: u32,
    database: String,
    cluster_id: Uuid,
    group_id: Uuid,
    from: u64,
    from_incarnation: Uuid,
    to: u64,
    to_incarnation: Uuid,
    nonce: Uuid,
    payload: T,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
    Snapshot {
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
        bytes: u64,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Reply {
    Append(AppendEntriesResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
    Snapshot(SnapshotResponse<TypeConfig>),
    Unavailable,
}
impl Network {
    async fn connect(&self) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        validate_peer(&self.peer)?;
        if self.target != self.peer.node_id
            || self.peer.cluster_id != self.factory.local.cluster_id
            || self.peer.group_id != self.factory.local.group_id
            || self.peer.database != self.factory.local.database
        {
            return Err(invalid("foreign consensus peer"));
        }
        let stream = TcpStream::connect(&self.peer.address).await?;
        let name = ServerName::try_from(self.peer.server_name.clone())
            .map_err(|_| invalid("invalid peer server name"))?;
        let stream = TlsConnector::from(self.factory.tls.client.clone())
            .connect(name, stream)
            .await?;
        verify_cert(
            stream.get_ref().1.peer_certificates(),
            &self.peer.certificate_sha256,
        )?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(invalid("wrong consensus protocol"));
        }
        Ok(stream)
    }
    fn envelope<T>(&self, payload: T, nonce: Uuid) -> Envelope<T> {
        let local = &self.factory.local;
        Envelope {
            version: 1,
            engine: "oltp".into(),
            format_version: 2,
            database: local.database.clone(),
            cluster_id: local.cluster_id,
            group_id: local.group_id,
            from: local.node_id,
            from_incarnation: local.incarnation,
            to: self.target,
            to_incarnation: self.peer.incarnation,
            nonce,
            payload,
        }
    }
    fn check_reply(&self, reply: &Envelope<Reply>, nonce: Uuid) -> io::Result<()> {
        validate_envelope(reply, &self.factory.local, &self.peer)?;
        if reply.nonce != nonce {
            return Err(invalid("consensus reply identity mismatch"));
        }
        Ok(())
    }
    async fn unary(&self, request: Request, ttl: Duration) -> io::Result<Reply> {
        timeout(ttl, async {
            let _permit = self
                .factory
                .outgoing
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| invalid("consensus transport closed"))?;
            let mut stream = self.connect().await?;
            let nonce = Uuid::new_v4();
            write_frame(&mut stream, &self.envelope(request, nonce)).await?;
            let reply: Envelope<Reply> = read_frame(&mut stream).await?;
            self.check_reply(&reply, nonce)?;
            Ok(reply.payload)
        })
        .await
        .map_err(|_| invalid("consensus RPC timed out"))?
    }
}
impl RaftNetworkV2<TypeConfig> for Network {
    type SnapshotData = SnapshotFile;
    async fn append_entries(
        &mut self,
        mut rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        // OpenRaft permits a transport to send a prefix, provided success is
        // translated using that prefix's last ID rather than the original tail.
        let original = std::mem::take(&mut rpc.entries);
        let mut bytes = 0usize;
        for entry in original {
            let size = encoded_size(&entry)
                .map_err(|_| unreachable("Raft entry exceeds request bound"))?;
            if !rpc.entries.is_empty()
                && (rpc.entries.len() >= MAX_APPEND_ENTRIES || bytes + size > APPEND_TARGET_BYTES)
            {
                break;
            }
            bytes += size;
            rpc.entries.push(entry);
        }
        let previous = rpc.prev_log_id;
        let last = rpc.entries.last().map(RaftEntry::log_id).or(previous);
        match self
            .unary(Request::Append(rpc), option.soft_ttl())
            .await
            .map_err(|_| unreachable("append RPC unavailable"))?
        {
            Reply::Append(reply) => Ok(reply.into_append_result(previous, last)),
            _ => Err(unreachable("unexpected append reply")),
        }
    }
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self
            .unary(Request::Vote(rpc), option.soft_ttl())
            .await
            .map_err(|_| unreachable("vote RPC unavailable"))?
        {
            Reply::Vote(reply) => Ok(reply),
            _ => Err(unreachable("unexpected vote reply")),
        }
    }
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, SnapshotFile>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let transfer = async {
            let ttl = option.soft_ttl();
            let mut slot = timeout(ttl, self.factory.snapshot_slots.clone().acquire_owned())
                .await
                .map_err(|_| invalid("snapshot slot timed out"))?
                .map_err(|_| invalid("snapshot transport closed"))?;
            let mut stream = timeout(ttl, self.connect())
                .await
                .map_err(|_| invalid("snapshot connect timed out"))??;
            let nonce = Uuid::new_v4();
            let bytes = snapshot.snapshot.bytes;
            timeout(
                ttl,
                write_frame(
                    &mut stream,
                    &self.envelope(
                        Request::Snapshot {
                            vote,
                            meta: snapshot.meta,
                            bytes,
                        },
                        nonce,
                    ),
                ),
            )
            .await
            .map_err(|_| invalid("snapshot header timed out"))??;
            let mut offset = 0u64;
            let mut digest = Sha256::new();
            while offset < bytes {
                let count = (bytes - offset).min(SNAPSHOT_CHUNK_BYTES as u64) as usize;
                let file = snapshot.snapshot.file.clone();
                let (chunk, returned_slot) = tokio::task::spawn_blocking(move || {
                    read_at(&file, offset, count).map(|bytes| (bytes, slot))
                })
                .await
                .map_err(|_| invalid("snapshot reader failed"))??;
                slot = returned_slot;
                digest.update(&chunk);
                timeout(ttl, stream.write_all(&chunk))
                    .await
                    .map_err(|_| invalid("snapshot write timed out"))??;
                offset = offset
                    .checked_add(count as u64)
                    .ok_or_else(|| invalid("snapshot offset overflow"))?;
            }
            timeout(ttl, stream.write_all(&digest.finalize()))
                .await
                .map_err(|_| invalid("snapshot checksum timed out"))??;
            let reply: Envelope<Reply> = timeout(ttl, read_frame(&mut stream))
                .await
                .map_err(|_| invalid("snapshot reply timed out"))??;
            self.check_reply(&reply, nonce)?;
            match reply.payload {
                Reply::Snapshot(reply) => Ok(reply),
                _ => Err(invalid("unexpected snapshot reply")),
            }
        };
        tokio::select! {closed=cancel=>Err(StreamingError::Closed(closed)),result=transfer=>result.map_err(|_|StreamingError::Unreachable(Unreachable::from_string("snapshot transfer unavailable")))}
    }
}

/// Each server owns one group's handle. The external runtime owns membership,
/// readiness, queue replication and lifecycle. No unsecured listener is available.
pub struct Server<SM: RaftStateMachine<TypeConfig, SnapshotData = SnapshotFile>> {
    raft: Raft<TypeConfig, SM>,
    local: Peer,
    tls: TlsMaterial,
    peers: Arc<RwLock<BTreeMap<u64, Peer>>>,
    incoming: Arc<Semaphore>,
    snapshot_slots: Arc<Semaphore>,
    snapshot_dir: PathBuf,
    idle_timeout: Duration,
}
impl<SM: RaftStateMachine<TypeConfig, SnapshotData = SnapshotFile>> Server<SM> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the listener owns separate identity, peer authorization, transport and bounded resource inputs"
    )]
    pub fn new(
        raft: Raft<TypeConfig, SM>,
        local: Peer,
        tls: TlsMaterial,
        peers: Arc<RwLock<BTreeMap<u64, Peer>>>,
        incoming: Arc<Semaphore>,
        snapshot_slots: Arc<Semaphore>,
        snapshot_dir: PathBuf,
        idle_timeout: Duration,
    ) -> io::Result<Self> {
        validate_peer(&local)?;
        if !snapshot_dir.is_dir()
            || idle_timeout.is_zero()
            || local.certificate_sha256 != tls.certificate_sha256
        {
            return Err(invalid("invalid consensus listener configuration"));
        }
        Ok(Self {
            raft,
            local,
            tls,
            peers,
            incoming,
            snapshot_slots,
            snapshot_dir,
            idle_timeout,
        })
    }
    pub async fn run(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> io::Result<()> {
        validate_membership(&self.local, &*self.peers.read().await)?;
        tokio::pin!(shutdown);
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _=&mut shutdown=>break,
                result=tasks.join_next(),if !tasks.is_empty()=>{if let Some(Err(_))=result{tracing::warn!("consensus connection task failed");}},
                result=listener.accept()=>{
                    let(stream,_)=result?;
                    let permit=match self.incoming.clone().try_acquire_owned(){Ok(permit)=>permit,Err(_)=>{drop(stream);continue}};
                    let server=self.clone();tasks.spawn(async move{let _permit=permit;let _=server.serve(stream).await;});
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
    async fn serve(&self, stream: TcpStream) -> io::Result<()> {
        let ttl = self.idle_timeout;
        let mut stream = timeout(
            ttl,
            TlsAcceptor::from(self.tls.server.clone()).accept(stream),
        )
        .await
        .map_err(|_| invalid("peer handshake timed out"))??;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(invalid("wrong consensus protocol"));
        }
        let request: Envelope<Request> = timeout(ttl, read_frame(&mut stream))
            .await
            .map_err(|_| invalid("peer header timed out"))??;
        let peer = self
            .peers
            .read()
            .await
            .get(&request.from)
            .cloned()
            .ok_or_else(|| invalid("unknown consensus sender"))?;
        validate_envelope(&request, &self.local, &peer)?;
        verify_cert(
            stream.get_ref().1.peer_certificates(),
            &peer.certificate_sha256,
        )?;
        let nonce = request.nonce;
        let reply = match request.payload {
            Request::Append(rpc) => {
                if rpc.entries.len() > MAX_APPEND_ENTRIES {
                    return Err(invalid("too many append entries"));
                }
                match timeout(ttl, self.raft.append_entries(rpc)).await {
                    Ok(Ok(reply)) => Reply::Append(reply.into()),
                    _ => Reply::Unavailable,
                }
            }
            Request::Vote(rpc) => match timeout(ttl, self.raft.vote(rpc)).await {
                Ok(Ok(reply)) => Reply::Vote(reply),
                _ => Reply::Unavailable,
            },
            Request::Snapshot { vote, meta, bytes } => {
                let mut slot = self
                    .snapshot_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| invalid("snapshot capacity unavailable"))?;
                disk_headroom(&self.snapshot_dir, bytes)?;
                let path = self
                    .snapshot_dir
                    .join(format!("incoming-{}.tmp", Uuid::new_v4()));
                let cleanup = TemporarySnapshot(path.clone());
                let open_path = path.clone();
                let (mut file, returned_slot) = tokio::task::spawn_blocking(move || {
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create_new(true)
                        .open(open_path)
                        .map(|file| (file, slot))
                })
                .await
                .map_err(|_| invalid("snapshot create task failed"))??;
                slot = returned_slot;
                let mut remaining = bytes;
                let mut digest = Sha256::new();
                let mut chunk = vec![0u8; SNAPSHOT_CHUNK_BYTES];
                while remaining > 0 {
                    let count = remaining.min(chunk.len() as u64) as usize;
                    timeout(ttl, stream.read_exact(&mut chunk[..count]))
                        .await
                        .map_err(|_| invalid("snapshot receive timed out"))??;
                    digest.update(&chunk[..count]);
                    disk_headroom(&self.snapshot_dir, count as u64)?;
                    let bytes = chunk[..count].to_vec();
                    let (returned_file, returned_slot) = tokio::task::spawn_blocking(move || {
                        file.write_all(&bytes)?;
                        Ok::<_, io::Error>((file, slot))
                    })
                    .await
                    .map_err(|_| invalid("snapshot writer failed"))??;
                    file = returned_file;
                    slot = returned_slot;
                    remaining -= count as u64;
                }
                let mut expected = [0u8; 32];
                timeout(ttl, stream.read_exact(&mut expected))
                    .await
                    .map_err(|_| invalid("snapshot checksum timed out"))??;
                if digest.finalize().as_slice() != expected {
                    return Err(invalid("snapshot checksum differs"));
                }
                let (data, slot) = tokio::task::spawn_blocking(move || {
                    file.sync_all()?;
                    if file.metadata()?.len() != bytes {
                        return Err(invalid("snapshot file length differs"));
                    }
                    Ok::<_, io::Error>((
                        SnapshotFile {
                            file: Arc::new(file),
                            path,
                            bytes,
                        },
                        slot,
                    ))
                })
                .await
                .map_err(|_| invalid("snapshot sync failed"))??;
                let reply = match self
                    .raft
                    .install_full_snapshot(
                        vote,
                        openraft::storage::Snapshot {
                            meta,
                            snapshot: data,
                        },
                    )
                    .await
                {
                    Ok(reply) => Reply::Snapshot(reply),
                    Err(_) => Reply::Unavailable,
                };
                drop(slot);
                drop(cleanup);
                reply
            }
        };
        let response = Envelope {
            version: 1,
            engine: "oltp".into(),
            format_version: 2,
            database: self.local.database.clone(),
            cluster_id: self.local.cluster_id,
            group_id: self.local.group_id,
            from: self.local.node_id,
            from_incarnation: self.local.incarnation,
            to: peer.node_id,
            to_incarnation: peer.incarnation,
            nonce,
            payload: reply,
        };
        timeout(ttl, write_frame(&mut stream, &response))
            .await
            .map_err(|_| invalid("peer reply timed out"))?
    }
}
struct TemporarySnapshot(PathBuf);
impl Drop for TemporarySnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
/// Admission guard for bootstrap, learner additions and membership proposals.
/// Call this before presenting a membership change to OpenRaft.
pub fn validate_membership(local: &Peer, peers: &BTreeMap<u64, Peer>) -> io::Result<()> {
    validate_peer(local)?;
    if peers.is_empty() {
        return Err(invalid("empty consensus membership"));
    }
    let mut certificates = std::collections::BTreeSet::new();
    let mut incarnations = std::collections::BTreeSet::new();
    for (id, peer) in peers {
        validate_peer(peer)?;
        if *id != peer.node_id
            || peer.cluster_id != local.cluster_id
            || peer.group_id != local.group_id
            || peer.database != local.database
            || !certificates.insert(&peer.certificate_sha256)
            || !incarnations.insert(peer.incarnation)
        {
            return Err(invalid("invalid consensus membership enrollment"));
        }
    }
    Ok(())
}

fn validate_peer(peer: &Peer) -> io::Result<()> {
    if peer.node_id == 0
        || peer.engine != "oltp"
        || peer.format_version != 2
        || !crate::oltp::valid_database(&peer.database)
        || peer.address.is_empty()
        || ServerName::try_from(peer.server_name.clone()).is_err()
        || peer.cluster_id.is_nil()
        || peer.group_id.is_nil()
        || peer.incarnation.is_nil()
        || peer.address.len() > 1024
        || peer.server_name.len() > 253
        || peer.certificate_sha256.len() != 64
        || !peer
            .certificate_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid consensus peer identity"));
    }
    Ok(())
}
fn validate_envelope<T>(envelope: &Envelope<T>, local: &Peer, remote: &Peer) -> io::Result<()> {
    if envelope.version != 1
        || envelope.engine != "oltp"
        || envelope.format_version != 2
        || envelope.database != local.database
        || remote.database != local.database
        || envelope.cluster_id != local.cluster_id
        || envelope.group_id != local.group_id
        || remote.cluster_id != local.cluster_id
        || remote.group_id != local.group_id
        || envelope.to != local.node_id
        || envelope.to_incarnation != local.incarnation
        || envelope.from != remote.node_id
        || envelope.from_incarnation != remote.incarnation
        || envelope.nonce.is_nil()
    {
        return Err(invalid("foreign consensus envelope"));
    }
    Ok(())
}
fn verify_cert(certificates: Option<&[CertificateDer<'_>]>, expected: &str) -> io::Result<()> {
    if certificates
        .and_then(|certs| certs.first())
        .is_none_or(|cert| hex(&Sha256::digest(cert.as_ref())) != expected)
    {
        return Err(invalid("peer certificate pin differs"));
    }
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn unreachable(message: &str) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::from_string(message))
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
async fn read_frame<T: DeserializeOwned>(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<T> {
    let size = stream.read_u32().await? as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(invalid("consensus frame exceeds bound"));
    }
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    crate::protocol::json_input(&bytes, MAX_FRAME_BYTES)
        .map_err(|_| invalid("consensus frame exceeds depth bound"))?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid consensus frame"))
}
async fn write_frame(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
) -> io::Result<()> {
    let mut writer = FrameWriter(Vec::new());
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| invalid("consensus frame encoding exceeds bound"))?;
    stream.write_u32(writer.0.len() as u32).await?;
    stream.write_all(&writer.0).await?;
    stream.flush().await
}
struct FrameWriter(Vec<u8>);
impl Write for FrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_FRAME_BYTES - self.0.len() {
            return Err(invalid("consensus frame exceeds bound"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[cfg(unix)]
fn read_at(file: &std::fs::File, offset: u64, count: usize) -> io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut bytes = vec![0; count];
    file.read_exact_at(&mut bytes, offset)?;
    Ok(bytes)
}
#[cfg(not(unix))]
fn read_at(_: &std::fs::File, _: u64, _: usize) -> io::Result<Vec<u8>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "consensus snapshot transfer requires Unix",
    ))
}

#[cfg(unix)]
fn disk_headroom(path: &std::path::Path, bytes: u64) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| invalid("invalid snapshot directory"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // The path is NUL-terminated and statvfs initializes the output on success.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    let available = u128::from(stats.f_bavail)
        .checked_mul(u128::from(stats.f_frsize))
        .ok_or_else(|| invalid("snapshot disk capacity overflow"))?;
    if u128::from(bytes) > available.saturating_sub(64 * 1024 * 1024) {
        return Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "snapshot needs additional disk capacity",
        ));
    }
    Ok(())
}
#[cfg(not(unix))]
fn disk_headroom(_: &std::path::Path, _: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "consensus storage requires Unix",
    ))
}

fn encoded_size(value: &impl Serialize) -> io::Result<usize> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > 16 * 1024 * 1024 - self.0 {
                return Err(invalid("Raft entry exceeds bound"));
            }
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| invalid("Raft entry encoding exceeds bound"))?;
    Ok(counter.0)
}
