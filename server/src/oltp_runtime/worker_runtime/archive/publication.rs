use super::*;
use crate::{
    oltp::{
        backup::lifecycle::{BackupCancelledStatement, VerifiedBackupCancelled},
        pool::WorkerSharedResources,
    },
    oltp_replication::{
        authority::consensus::ConsensusFuture, service_rpc::BackupPublicationReader,
    },
};

pub(super) fn native_archive_id(
    cluster: Uuid,
    owner: BackupOwnerRef,
) -> Result<Uuid, ProtocolError> {
    let digest = serialized_digest(&(b"nabedra/native-backup-id/v1".as_slice(), cluster, owner))?;
    Uuid::from_slice(&digest.as_bytes()[..16]).map_err(|_| unknown())
}

pub(crate) struct NativeBackupPublication {
    root: PathBuf,
    cluster: Uuid,
    shared: Arc<WorkerSharedResources>,
    keys: Arc<ArchiveKeys>,
    remote: Option<ArchiveRepository>,
}
impl NativeBackupPublication {
    pub(crate) async fn open(
        root: PathBuf,
        s3: Option<crate::oltp_backup_repository::S3ArchiveConfig>,
        cluster: Uuid,
        shared: Arc<WorkerSharedResources>,
        keys: Arc<ArchiveKeys>,
    ) -> Result<Arc<Self>, ProtocolError> {
        // Verify ownership, permissions and the canonical directory before
        // installing the repository as an authority on a Timestamp voter.
        FilesystemRepository::open(&root, shared.clone())?;
        let remote = match s3 {
            Some(config) => Some(ArchiveRepository::new(
                config.connect(shared.clone()).await?,
                keys.clone(),
            )),
            None => None,
        };
        Ok(Arc::new(Self {
            root,
            cluster,
            shared,
            keys,
            remote,
        }))
    }

    pub(super) fn directory(&self, archive_id: Uuid) -> PathBuf {
        self.root.join(archive_id.to_string())
    }

    pub(super) fn retain_local_history(&self) -> bool {
        self.remote.is_none() && !self.keys.writes_encrypted()
    }

    pub(super) fn require_write(&self) -> Result<(), ProtocolError> {
        self.keys.require_write()
    }

    pub(super) fn contains(&self, directory: &Path) -> bool {
        directory.parent() == Some(self.root.as_path())
            && directory
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| Uuid::parse_str(name).ok())
                .is_some_and(|id| self.directory(id) == directory)
            && std::fs::canonicalize(directory).is_ok_and(|actual| actual == directory)
    }

    pub(super) fn repository(&self, directory: &Path) -> Result<ArchiveRepository, ProtocolError> {
        if !self.contains(directory) {
            return Err(unknown());
        }
        let local = FilesystemRepository::open(directory, self.shared.clone())?;
        Ok(self
            .remote
            .clone()
            .unwrap_or_else(|| ArchiveRepository::new(local, self.keys.clone())))
    }

    fn scoped_repository(
        &self,
        owner: BackupOwnerRef,
        archive_id: Uuid,
    ) -> Result<(ArchiveRepository, ArchiveKey), ProtocolError> {
        if native_archive_id(self.cluster, owner)? != archive_id {
            return Err(unknown());
        }
        let repository = match &self.remote {
            Some(repository) => repository.clone(),
            None => self.repository(&self.directory(archive_id))?,
        };
        Ok((
            repository,
            ArchiveKey {
                cluster_id: self.cluster,
                archive_id,
            },
        ))
    }

    pub(super) async fn verify_locator(
        &self,
        directory: &Path,
        locator: &NativeBackupManifest,
    ) -> Result<(), ProtocolError> {
        let key = ArchiveKey {
            cluster_id: self.cluster,
            archive_id: native_archive_id(self.cluster, locator.owner)?,
        };
        if key != locator.key || self.directory(key.archive_id) != directory {
            return Err(unknown());
        }
        let reader = ArchiveReader::open(self.repository(directory)?, key).await?;
        verify_locator(reader, locator).await
    }
}

const CANCEL_DOMAIN: &[u8] = b"nabedra/unpublished-native-archive/v1";
const CANCEL_LIMIT: usize = 4096;

fn cancellation_bytes(
    repository: &ArchiveRepository,
    statement: BackupCancelledStatement,
) -> Result<crate::oltp::pool::ChargedBytes, ProtocolError> {
    repository
        .shared()
        .encode_in(ByteClass::Data, &(CANCEL_DOMAIN, statement), CANCEL_LIMIT)
}

pub(super) async fn settle_cancellation(
    repository: &ArchiveRepository,
    key: ArchiveKey,
    statement: BackupCancelledStatement,
) -> Result<VerifiedBackupCancelled, ProtocolError> {
    // This conditional create competes with publication even if a delayed
    // root PUT remains in flight. A missing GET cannot settle that PUT.
    repository
        .put(
            key,
            "root.json".into(),
            cancellation_bytes(repository, statement)?,
        )
        .await?;
    read_cancellation(repository, key, statement.owner, Some(statement.pin)).await
}

pub(super) async fn read_cancellation(
    repository: &ArchiveRepository,
    key: ArchiveKey,
    owner: BackupOwnerRef,
    pin: Option<StoragePin>,
) -> Result<VerifiedBackupCancelled, ProtocolError> {
    let actual = repository.get_root_bytes(key, CANCEL_LIMIT).await?;
    let (_, statement): (Vec<u8>, BackupCancelledStatement) =
        serde_json::from_slice(actual.as_slice()).map_err(|_| unknown())?;
    if statement.owner != owner
        || statement.archive_id != key.archive_id
        || pin.is_some_and(|pin| statement.pin != pin)
        || actual.as_slice() != cancellation_bytes(repository, statement)?.as_slice()
    {
        return Err(unknown());
    }
    VerifiedBackupCancelled::after_settlement(statement)
}

pub(super) fn recorded_manifest(
    reader: &ArchiveReader,
) -> Result<NativeBackupManifest, ProtocolError> {
    let root = reader.root();
    let mut manifest: NativeBackupManifest =
        serde_json::from_str(root.history().get()).map_err(|_| unknown())?;
    manifest.validate()?;
    if manifest.key != root.header().key
        || manifest.scope.previous_segment != root.header().previous_segment
        || manifest.repository_root != Digest::from_bytes([0; 32])
    {
        return Err(unknown());
    }
    manifest.repository_root = root.root_sha256();
    Ok(manifest)
}

pub(super) async fn verify_locator(
    mut reader: ArchiveReader,
    locator: &NativeBackupManifest,
) -> Result<(), ProtocolError> {
    if recorded_manifest(&reader)? != *locator {
        return Err(unknown());
    }
    while reader.next().await?.is_some() {}
    reader.verify_complete()
}

impl BackupPublicationReader for NativeBackupPublication {
    fn verify_cancelled(
        &self,
        statement: BackupCancelledStatement,
    ) -> ConsensusFuture<'_, VerifiedBackupCancelled> {
        Box::pin(async move {
            let (repository, key) =
                self.scoped_repository(statement.owner, statement.archive_id)?;
            read_cancellation(&repository, key, statement.owner, Some(statement.pin)).await
        })
    }

    fn verify(
        &self,
        statement: BackupPublishedStatement,
    ) -> ConsensusFuture<'_, VerifiedBackupPublished> {
        Box::pin(async move {
            let (repository, key) =
                self.scoped_repository(statement.owner, statement.archive_id)?;
            let mut reader = ArchiveReader::open(repository, key).await?;
            let manifest = recorded_manifest(&reader)?;
            if manifest.owner != statement.owner
                || manifest.pin != statement.pin
                || manifest.history_root != statement.catalog_root
                || manifest.repository_root != statement.manifest_sha256
            {
                return Err(unknown());
            }
            while reader.next().await?.is_some() {}
            reader.verify_complete()?;
            VerifiedBackupPublished::published(statement)
        })
    }
}
