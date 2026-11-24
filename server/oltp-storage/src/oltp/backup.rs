//! A completed backup is an immutable, checksummed logical MVCC cut. Replication
//! logs, queue records and independent volume snapshots are not backup manifests.
#[doc(hidden)]
pub mod history;
#[doc(hidden)]
pub mod lifecycle;
#[doc(hidden)]
pub mod restore;
use crate::{
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::{
    owned::serialized_digest,
    types::{Digest, StoragePin},
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use uuid::Uuid;
#[doc(hidden)]
pub use nabedra_oltp_types::authority::BackupOwnerRef;
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupPublishedStatement {
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub archive_id: Uuid,
    pub manifest_sha256: Digest,
    pub catalog_root: Digest,
}
/// Constructed only after the exporter fsyncs every component and its immutable
/// final manifest. A parsed worker DTO cannot create release permission.
pub struct VerifiedBackupPublished {
    statement: BackupPublishedStatement,
}
impl VerifiedBackupPublished {
    pub fn statement(&self) -> BackupPublishedStatement {
        self.statement
    }
    pub fn published(statement: BackupPublishedStatement) -> Result<Self, ProtocolError> {
        if statement.owner.backup_id.is_nil()
            || statement.owner.attempt == 0
            || statement.archive_id.is_nil()
        {
            return Err(invalid());
        }
        Ok(Self { statement })
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedBackupPublished {
    statement: BackupPublishedStatement,
    digest: Digest,
}
impl SealedBackupPublished {
    pub fn from_verified(proof: VerifiedBackupPublished) -> Result<Self, ProtocolError> {
        let statement = proof.statement;
        Ok(Self {
            digest: serialized_digest(&(b"nabedra/published-backup/v1", statement))?,
            statement,
        })
    }
    pub fn validate(&self) -> Result<&BackupPublishedStatement, ProtocolError> {
        if self.digest != serialized_digest(&(b"nabedra/published-backup/v1", self.statement))?
            || self.statement.owner.backup_id.is_nil()
            || self.statement.owner.attempt == 0
            || self.statement.archive_id.is_nil()
        {
            return Err(invalid());
        }
        Ok(&self.statement)
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid completed backup proof",
        Outcome::NotApplied,
    )
}

use crate::{
    migration_interchange::{ArchiveHeader, ArchiveSummary, ArchiveWriter, SourceEngine},
    oltp::mvcc::{
        catalog::{IntentRef, PinnedCatalog},
        planner::{NativePlanner, ResolveFuture},
    },
};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
pub trait BackupBackend: Send + Sync {
    fn planner(&self) -> &NativePlanner;
    fn begin(&self, owner: BackupOwnerRef) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)>;
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()>;
    fn close(&self, proof: VerifiedBackupPublished) -> ResolveFuture<'_, ()>;
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub format_version: u32,
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub catalog: nabedra_oltp_types::types::GroupIdentity,
    pub catalog_root: Digest,
    pub archive_id: Uuid,
    pub archive_sha256: Digest,
    pub summary: ArchiveSummary,
    #[serde(skip)]
    _allocation: Option<Arc<crate::oltp::pool::ScratchLease>>,
}
pub struct BackupService {
    backend: Arc<dyn BackupBackend>,
}
impl BackupService {
    pub fn new(backend: Arc<dyn BackupBackend>) -> Self {
        Self { backend }
    }
    pub async fn create(
        &self,
        destination: &Path,
        owner: BackupOwnerRef,
    ) -> Result<BackupManifest, ProtocolError> {
        if owner.backup_id.is_nil() || owner.attempt == 0 || destination.exists() {
            return Err(invalid());
        }
        let (pin, catalog) = self.backend.begin(owner).await?;
        let archive_id = nabedra_rt::ids::uuid_v4();
        let root =
            serialized_digest(&(b"nabedra/backup-catalog/v1", &catalog.catalog, pin.read_ts))?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let stage = parent.join(format!(".nabedra-backup-{archive_id}.partial"));
        let shared = self.backend.planner().catalog().resources().clone();
        let create_stage = stage.clone();
        let file = shared
            .execute(crate::oltp::pool::ExecutionClass::Query, 0, 0, move || {
                private_directory(&create_stage).map_err(|_| io_failure())?;
                OpenOptions::new()
                    .create_new(true)
                    .mode(0o600)
                    .write(true)
                    .open(create_stage.join("snapshot.nab"))
                    .map_err(|_| io_failure())
            })
            .await?;
        let _cleanup = StagedDirectory(stage.clone());
        let writer = ArchiveWriter::new(
            file,
            ArchiveHeader {
                format_version: 1,
                archive_id,
                source_engine: SourceEngine::Oltp,
                source_snapshot: *root.as_bytes(),
            },
        )?;
        let (file, summary) = self
            .backend
            .planner()
            .export_logical_snapshot(&catalog, writer, self.backend.as_ref())
            .await?;
        shared
            .execute(crate::oltp::pool::ExecutionClass::Query, 0, 0, move || {
                file.sync_all().map_err(|_| io_failure())
            })
            .await?;
        let archive_path = stage.join("snapshot.nab");
        let archive_sha256 = self
            .backend
            .planner()
            .hash_backup_file(archive_path)
            .await?;
        let allocation =
            Arc::new(shared.reserve_scratch(crate::oltp::pool::ByteClass::Data, 64 << 10)?);
        let manifest = BackupManifest {
            format_version: 1,
            owner,
            pin,
            catalog: catalog.catalog,
            catalog_root: root,
            archive_id,
            archive_sha256,
            summary,
            _allocation: Some(allocation),
        };
        let destination = destination.to_path_buf();
        let parent = parent.to_path_buf();
        let (manifest, manifest_sha256) = shared
            .execute(crate::oltp::pool::ExecutionClass::Query, 0, 0, move || {
                let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| invalid())?;
                let manifest_sha256 = Digest::from_bytes(Sha256::digest(&manifest_bytes).into());
                let mut manifest_file = OpenOptions::new()
                    .create_new(true)
                    .mode(0o600)
                    .write(true)
                    .open(stage.join("manifest.json"))
                    .map_err(|_| io_failure())?;
                use std::io::Write;
                manifest_file
                    .write_all(&manifest_bytes)
                    .and_then(|_| manifest_file.sync_all())
                    .map_err(|_| io_failure())?;
                File::open(&stage)
                    .and_then(|d| d.sync_all())
                    .map_err(|_| io_failure())?;
                // create_dir reserves the destination without ever replacing a target.
                // Components become visible first; the manifest is the final commit
                // marker and is immutable. A failed copy has no advertised manifest.
                private_directory(&destination).map_err(|_| io_failure())?;
                std::fs::hard_link(stage.join("snapshot.nab"), destination.join("snapshot.nab"))
                    .map_err(|_| io_failure())?;
                File::open(&destination)
                    .and_then(|d| d.sync_all())
                    .map_err(|_| io_failure())?;
                std::fs::hard_link(
                    stage.join("manifest.json"),
                    destination.join("manifest.json"),
                )
                .map_err(|_| io_failure())?;
                File::open(&destination)
                    .and_then(|d| d.sync_all())
                    .and_then(|_| File::open(&parent)?.sync_all())
                    .map_err(|_| io_failure())?;
                Ok((manifest, manifest_sha256))
            })
            .await?;
        self.backend
            .close(VerifiedBackupPublished::published(
                BackupPublishedStatement {
                    owner,
                    pin,
                    archive_id,
                    manifest_sha256,
                    catalog_root: root,
                },
            )?)
            .await?;
        Ok(manifest)
    }
}
struct StagedDirectory(PathBuf);
impl Drop for StagedDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn io_failure() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "consistent backup I/O failed",
        Outcome::Unknown,
    )
}
/// Hash in bounded native phases rather than holding a CPU/native slot during
/// the lifetime of an arbitrarily large export.
pub(super) fn hash_chunk(file: &mut File, hash: &mut Sha256) -> Result<bool, ProtocolError> {
    let mut buffer = [0; 64 << 10];
    let n = file.read(&mut buffer).map_err(|_| io_failure())?;
    hash.update(&buffer[..n]);
    Ok(n == 0)
}

fn private_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}
