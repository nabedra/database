//! Exact physical topology for a sealed offline genesis. Source group UUIDs
//! survive; the target cluster, worker and pool are newly authorized identities.
use super::*;
use crate::{
    oltp::backup::restore::{OfflineRestoreTarget, RestoredGenesis},
    oltp_backup_repository::{ArchiveKeys, S3ArchiveConfig},
    oltp_replication::{owned::serialized_digest, types::CommitTs},
    oltp_runtime::worker_runtime::archive::VerifiedNativeRestore,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredRestoreConfig {
    archives: Vec<PathBuf>,
    target_cut: Option<CommitTs>,
    #[serde(default)]
    s3: Option<S3ArchiveConfig>,
}

impl RegisteredRestoreConfig {
    pub(super) async fn verify(
        &self,
        config: &RegisteredConfig,
        shared: Arc<WorkerSharedResources>,
        keys: Arc<ArchiveKeys>,
    ) -> Result<VerifiedNativeRestore, ProtocolError> {
        let control = &config.placements.control;
        let mount = config.mount.as_ref().ok_or_else(invalid)?;
        if mount.generation != 1 || self.archives.iter().any(|path| !path.is_absolute()) {
            return Err(invalid());
        }
        // The first mounted restore uses the same worker/pool set for every
        // system and Data group. Per-group Raft node numbers may differ.
        if config.placements.all().into_iter().any(|placement| {
            !placement.voters.iter().all(|member| {
                control
                    .local(member.worker_id)
                    .is_some_and(|expected| expected.pool_id == member.pool_id)
            })
        }) {
            return Err(invalid());
        }
        let source = match &self.s3 {
            Some(source) => Some(source.clone().connect(shared.clone()).await?),
            None => None,
        };
        let verified = VerifiedNativeRestore::verify(
            &self.archives,
            self.target_cut,
            control.identity.database.as_str(),
            Some(config.initial_run),
            shared,
            source,
            keys,
        )
        .await?;
        if !verified.plan.matches_registered(
            control.identity.cluster_id,
            &control.identity.database,
            config
                .placements
                .all()
                .map(|placement| placement.identity.group_id),
        ) {
            return Err(invalid());
        }
        Ok(verified)
    }
}

impl RegisteredWorker {
    pub(crate) async fn start_restored_data(
        &self,
        group: GroupIdentity,
        config: openraft::Config,
    ) -> Result<ManagedData, ProtocolError> {
        if self.restored.is_none() || !matches!(group.kind, GroupKind::Data { .. }) {
            return Err(invalid());
        }
        let placement = super::registered::GroupPlacement {
            identity: group,
            voters: self.placements.catalog.voters.clone(),
        };
        let local = placement
            .local(self.enrollment.worker_id)
            .ok_or_else(invalid)?;
        let running = self
            .manager
            .start_data(
                self.pool.group(placement.identity.clone()).await?,
                local.clone(),
                placement.members(),
                config,
            )
            .await?;
        super::registered::initialize_seed(&running, &placement, &local).await?;
        Ok(running)
    }
}

pub(super) fn registered_policy(
    config: &RegisteredConfig,
    genesis: &RestoredGenesis,
) -> Result<Digest, ProtocolError> {
    let statement = genesis.statement();
    serialized_digest(&(
        b"nabedra/registered-restore-policy/v1".as_slice(),
        config.bootstrap_id,
        config.initial_run,
        &config.placements,
        &config.control_enrollments,
        &config.operator_pins,
        statement.restore_id,
        statement.source_cluster,
        statement.source_root,
        statement.target_cut,
        statement.maximum_timestamp,
    ))
}

impl LocalWorker {
    pub(crate) fn prepare_restore(target: &OfflineRestoreTarget) -> Result<(), ProtocolError> {
        let owner = lock_directory(target.stage())?;
        let statement = target.statement();
        let (directory_dev, directory_inode, lock_dev, lock_inode) = directory_identity(&owner)?;
        if owner
            .path
            .join("NABEDRA_TYPED_WORKER.json")
            .symlink_metadata()
            .is_ok()
        {
            return Err(invalid());
        }
        let topology = LocalTopology {
            format: 1,
            cluster_id: statement.target_pool.cluster_id,
            worker_id: statement.target_pool.worker_id,
            pool_id: statement.target_pool.pool_id,
            database: statement.database.clone(),
            control: statement.control_group,
            timestamps: statement.timestamp_group,
            status: statement.initial_status_group,
            catalog: statement.catalog_group,
            node_id: 1,
            enrollment_revision: 1,
            directory_dev,
            directory_inode,
            lock_dev,
            lock_inode,
        };
        replace_topology(&owner.path, &topology)
    }
}
