//! Immutable archive transport. A bounded root is published only after every
//! actual chunk receipt. Neither this transport nor its root proves database
//! consistency; restore also verifies the producer's complete history/footer.
mod encryption;
mod filesystem;
#[cfg(feature = "s3")]
mod s3;
#[doc(hidden)]
pub use encryption::{ArchiveEncryptionConfig, ArchiveKeys, ArchiveRepository};
#[doc(hidden)]
pub use filesystem::FilesystemRepository;

use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ScratchLease, WorkerSharedResources},
    oltp_replication::types::Digest,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{future::Future, pin::Pin, sync::Arc};
use uuid::Uuid;

pub(crate) const MAX_CHUNK_BYTES: usize = 4 << 20;
pub(crate) const MAX_ROOT_BYTES: usize = 128 << 10;
const ROOT_OBJECT: &str = "root.json";

fn plain_limit(name: &str) -> usize {
    if name == ROOT_OBJECT {
        MAX_ROOT_BYTES
    } else if name
        .strip_prefix("part-")
        .is_some_and(|ordinal| ordinal.len() == 20 && ordinal.bytes().all(|b| b.is_ascii_digit()))
    {
        MAX_CHUNK_BYTES
    } else {
        0
    }
}

fn stored_limit(name: &str) -> usize {
    let limit = plain_limit(name);
    if limit == 0 {
        0
    } else {
        limit + encryption::MAX_OVERHEAD
    }
}
pub(crate) type RepositoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;

/// Only trusted local configuration can select a provider or object prefix.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct S3ArchiveConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
}
impl S3ArchiveConfig {
    #[doc(hidden)]
    pub async fn connect(
        self,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<dyn Repository>, ProtocolError> {
        #[cfg(feature = "s3")]
        {
            let repository = s3::S3Repository::connect(
                s3::S3RepositoryConfig {
                    bucket: self.bucket,
                    prefix: self.prefix,
                    region: self.region,
                    credentials: None,
                    #[cfg(test)]
                    fixture_endpoint: None,
                },
                shared,
            )
            .await?;
            Ok(repository)
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = (self, shared);
            Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "S3 archives require the oltp-backup-s3 feature",
                Outcome::NotApplied,
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ArchiveKey {
    pub cluster_id: Uuid,
    pub archive_id: Uuid,
}
impl ArchiveKey {
    fn valid(self) -> bool {
        !self.cluster_id.is_nil() && !self.archive_id.is_nil()
    }
    fn prefix(self) -> String {
        format!("{}/{}/", self.cluster_id, self.archive_id)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ChunkSpec {
    pub ordinal: u64,
    pub len: u32,
    pub sha256: Digest,
}
impl ChunkSpec {
    fn valid(self) -> bool {
        self.len > 0 && self.len as usize <= MAX_CHUNK_BYTES
    }
}

/// Implementations are sealed: an application callback cannot manufacture a
/// successful chunk receipt or turn a JSON provider result into publication.
#[doc(hidden)]
pub trait Repository: private::Sealed + Send + Sync {
    fn shared(&self) -> &Arc<WorkerSharedResources>;
    fn put<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        body: ChargedBytes,
    ) -> RepositoryFuture<'a, Digest>;
    fn get<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        limit: usize,
    ) -> RepositoryFuture<'a, ChargedBytes>;
}
mod private {
    pub trait Sealed {}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct RootHeader {
    pub format_version: u32,
    pub key: ArchiveKey,
    pub part_count: u64,
    pub total_bytes: u64,
    pub chunk_size: u32,
    pub archive_sha256: Digest,
    pub part_chain: Digest,
    pub previous_segment: Option<Digest>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootManifest {
    header: RootHeader,
    history: Box<serde_json::value::RawValue>,
}
#[derive(Serialize)]
struct RootView<'a> {
    header: &'a RootHeader,
    history: &'a serde_json::value::RawValue,
}

#[doc(hidden)]
pub struct ReadRoot {
    manifest: RootManifest,
    digest: Digest,
    protection: encryption::ObjectProtection,
    _bytes: ChargedBytes,
    _decoded: Arc<ScratchLease>,
}
impl ReadRoot {
    #[doc(hidden)]
    pub fn header(&self) -> &RootHeader {
        &self.manifest.header
    }
    #[doc(hidden)]
    pub fn history(&self) -> &serde_json::value::RawValue {
        &self.manifest.history
    }
    #[doc(hidden)]
    pub fn root_sha256(&self) -> Digest {
        self.digest
    }
}
#[doc(hidden)]
pub struct PublishedArchive {
    root: ReadRoot,
}
impl PublishedArchive {
    pub(crate) fn key(&self) -> ArchiveKey {
        self.root.header().key
    }
    #[doc(hidden)]
    pub fn root_sha256(&self) -> Digest {
        self.root.root_sha256()
    }
    pub(crate) fn header(&self) -> &RootHeader {
        self.root.header()
    }
}

/// One upload owns at most one caller chunk plus one bounded root. No table,
/// archive byte count, or part list must fit in a control message or RAM vector.
#[doc(hidden)]
pub struct UploadSession {
    repository: ArchiveRepository,
    key: ArchiveKey,
    chunk_size: u32,
    next: u64,
    total: u64,
    last_len: u32,
    chain: Digest,
    archive_hash: Sha256,
}
impl UploadSession {
    #[doc(hidden)]
    pub fn new(
        repository: ArchiveRepository,
        key: ArchiveKey,
        chunk_size: u32,
    ) -> Result<Self, ProtocolError> {
        if !key.valid() || chunk_size == 0 || chunk_size as usize > MAX_CHUNK_BYTES {
            return Err(invalid());
        }
        Ok(Self {
            repository,
            key,
            chunk_size,
            next: 0,
            total: 0,
            last_len: chunk_size,
            chain: chain_seed(),
            archive_hash: Sha256::new(),
        })
    }
    #[doc(hidden)]
    pub async fn append(&mut self, body: ChargedBytes) -> Result<ChunkSpec, ProtocolError> {
        if body.is_empty()
            || body.len() > self.chunk_size as usize
            || self.last_len != self.chunk_size
        {
            return Err(invalid());
        }
        let spec = ChunkSpec {
            ordinal: self.next,
            len: body.len() as u32,
            sha256: hash(body.as_ref()),
        };
        let next = self.next.checked_add(1).ok_or_else(invalid)?;
        let total = self
            .total
            .checked_add(body.len() as u64)
            .ok_or_else(invalid)?;
        let mut archive_hash = self.archive_hash.clone();
        archive_hash.update(body.as_ref());
        self.repository
            .put(self.key, chunk_name(spec.ordinal), body)
            .await?;
        // Only an actual durable provider completion/reconciliation advances
        // this session. A lost reply retries the same immutable ordinal.
        self.chain = chain_step(self.chain, spec);
        self.archive_hash = archive_hash;
        self.next = next;
        self.total = total;
        self.last_len = spec.len;
        Ok(spec)
    }
    #[doc(hidden)]
    pub async fn publish(
        self,
        history: ChargedBytes,
        previous_segment: Option<Digest>,
    ) -> Result<PublishedArchive, ProtocolError> {
        if self.next == 0 || history.len() > MAX_ROOT_BYTES / 2 {
            return Err(invalid());
        }
        crate::protocol::json_input(history.as_ref(), MAX_ROOT_BYTES / 2).map_err(|_| invalid())?;
        let history: &serde_json::value::RawValue =
            serde_json::from_slice(history.as_ref()).map_err(|_| invalid())?;
        let header = RootHeader {
            format_version: 1,
            key: self.key,
            part_count: self.next,
            total_bytes: self.total,
            chunk_size: self.chunk_size,
            archive_sha256: Digest::from_bytes(self.archive_hash.finalize().into()),
            part_chain: self.chain,
            previous_segment,
        };
        let bytes = self.repository.shared().encode_in(
            ByteClass::Data,
            &RootView {
                header: &header,
                history,
            },
            MAX_ROOT_BYTES,
        )?;
        let digest = self
            .repository
            .put(self.key, ROOT_OBJECT.into(), bytes.clone())
            .await?;
        Ok(PublishedArchive {
            root: decode_root(
                self.repository.shared(),
                self.key,
                bytes,
                digest,
                self.repository.write_protection(),
            )?,
        })
    }
}
pub(crate) async fn read_root(
    repository: &ArchiveRepository,
    key: ArchiveKey,
) -> Result<ReadRoot, ProtocolError> {
    if !key.valid() {
        return Err(invalid());
    }
    let (bytes, digest, protection) = repository.get_root(key, MAX_ROOT_BYTES).await?;
    decode_root(repository.shared(), key, bytes, digest, protection)
}
pub(crate) async fn read_chunk(
    repository: &ArchiveRepository,
    key: ArchiveKey,
    spec: ChunkSpec,
) -> Result<ChargedBytes, ProtocolError> {
    if !key.valid() || !spec.valid() {
        return Err(invalid());
    }
    let root = read_root(repository, key).await?;
    let bytes = repository
        .get_part(
            key,
            chunk_name(spec.ordinal),
            spec.len as usize,
            root.protection,
        )
        .await?;
    if bytes.len() != spec.len as usize || hash(bytes.as_ref()) != spec.sha256 {
        return Err(corrupt());
    }
    Ok(bytes)
}
/// Bounded sequential verification before restore consumes the staged history.
/// Each chunk is independently checked and the final root binds their order.
#[doc(hidden)]
pub struct ArchiveReader {
    repository: ArchiveRepository,
    root: ReadRoot,
    next: u64,
    total: u64,
    chain: Digest,
    archive_hash: Sha256,
}
impl ArchiveReader {
    #[doc(hidden)]
    pub async fn open(
        repository: ArchiveRepository,
        key: ArchiveKey,
    ) -> Result<Self, ProtocolError> {
        let root = read_root(&repository, key).await?;
        Ok(Self {
            repository,
            root,
            next: 0,
            total: 0,
            chain: chain_seed(),
            archive_hash: Sha256::new(),
        })
    }
    #[doc(hidden)]
    pub fn root(&self) -> &ReadRoot {
        &self.root
    }
    #[doc(hidden)]
    pub async fn next(&mut self) -> Result<Option<ChargedBytes>, ProtocolError> {
        let header = self.root.header();
        if self.next == header.part_count {
            self.verify_complete()?;
            return Ok(None);
        }
        let bytes = self
            .repository
            .get_part(
                header.key,
                chunk_name(self.next),
                header.chunk_size as usize,
                self.root.protection,
            )
            .await?;
        let expected = if self.next + 1 < header.part_count {
            header.chunk_size as u64
        } else {
            header
                .total_bytes
                .checked_sub(self.total)
                .ok_or_else(corrupt)?
        };
        if bytes.is_empty() || bytes.len() as u64 != expected {
            return Err(corrupt());
        }
        let spec = ChunkSpec {
            ordinal: self.next,
            len: bytes.len() as u32,
            sha256: hash(bytes.as_ref()),
        };
        self.archive_hash.update(bytes.as_ref());
        self.chain = chain_step(self.chain, spec);
        self.next = self.next.checked_add(1).ok_or_else(corrupt)?;
        self.total = self
            .total
            .checked_add(bytes.len() as u64)
            .ok_or_else(corrupt)?;
        Ok(Some(bytes))
    }
    #[doc(hidden)]
    pub fn verify_complete(&self) -> Result<(), ProtocolError> {
        let header = self.root.header();
        if self.next != header.part_count
            || self.total != header.total_bytes
            || self.chain != header.part_chain
            || Digest::from_bytes(self.archive_hash.clone().finalize().into())
                != header.archive_sha256
        {
            return Err(corrupt());
        }
        Ok(())
    }
}
fn decode_root(
    shared: &Arc<WorkerSharedResources>,
    key: ArchiveKey,
    bytes: ChargedBytes,
    digest: Digest,
    protection: encryption::ObjectProtection,
) -> Result<ReadRoot, ProtocolError> {
    if bytes.len() > MAX_ROOT_BYTES {
        return Err(corrupt());
    }
    crate::protocol::json_input(bytes.as_ref(), MAX_ROOT_BYTES).map_err(|_| corrupt())?;
    let decoded = Arc::new(shared.reserve_scratch(ByteClass::Data, 2 * MAX_ROOT_BYTES)?);
    let manifest: RootManifest = serde_json::from_slice(bytes.as_ref()).map_err(|_| corrupt())?;
    let header = &manifest.header;
    if header.format_version != 1
        || header.key != key
        || header.part_count == 0
        || header.chunk_size == 0
        || header.chunk_size as usize > MAX_CHUNK_BYTES
        || header.total_bytes == 0
        || header
            .part_count
            .checked_sub(1)
            .and_then(|n| n.checked_mul(header.chunk_size as u64))
            .is_none_or(|minimum| minimum >= header.total_bytes)
        || header
            .part_count
            .checked_mul(header.chunk_size as u64)
            .is_none_or(|maximum| maximum < header.total_bytes)
        || manifest.history.get().len() > MAX_ROOT_BYTES / 2
    {
        return Err(corrupt());
    }
    Ok(ReadRoot {
        manifest,
        digest,
        protection,
        _bytes: bytes,
        _decoded: decoded,
    })
}
fn hash(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}
fn chunk_name(ordinal: u64) -> String {
    format!("part-{ordinal:020}")
}
fn chain_seed() -> Digest {
    hash(b"nabedra/backup-repository/chunks/v1\0")
}
fn chain_step(previous: Digest, spec: ChunkSpec) -> Digest {
    let mut h = Sha256::new();
    h.update(previous.as_bytes());
    h.update(spec.ordinal.to_be_bytes());
    h.update(spec.len.to_be_bytes());
    h.update(spec.sha256.as_bytes());
    Digest::from_bytes(h.finalize().into())
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid bounded immutable archive request",
        Outcome::NotApplied,
    )
}
fn corrupt() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "archive object, ordering or immutable root differs",
        Outcome::Unknown,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "archive repository requires reconciliation",
        Outcome::Unknown,
    )
}
fn conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "immutable archive key already contains different bytes",
        Outcome::NotApplied,
    )
}

#[cfg(test)]
mod tests;
