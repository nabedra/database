//! Exact physical topology for a sealed offline genesis. Source group UUIDs
//! survive; the target cluster, worker and pool are newly authorized identities.
use super::*;
use std::future::Future;
use crate::{
    oltp::backup::restore::{OfflineRestoreTarget, RestoredGenesis, VerifiedRestorePlan},
    oltp_backup_repository::{ArchiveKeys, Repository, S3ArchiveConfig},
    oltp_replication::{owned::serialized_digest, types::CommitTs},
};

pub(crate) trait RegisteredRestore: Sized {
    fn verify(
        backups: &[PathBuf],
        target_cut: Option<CommitTs>,
        database: &str,
        target_run: Option<Uuid>,
        shared: Arc<WorkerSharedResources>,
        remote: Option<Arc<dyn Repository>>,
        keys: Arc<ArchiveKeys>,
    ) -> impl Future<Output = Result<Self, ProtocolError>> + Send;

    fn plan(&self) -> &VerifiedRestorePlan;

    fn install(
        self,
        target: OfflineRestoreTarget,
        pool: &OltpPool,
    ) -> impl Future<Output = Result<RestoredGenesis, ProtocolError>> + Send;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredRestoreConfig {
    archives: Vec<PathBuf>,
    target_cut: Option<CommitTs>,
    #[serde(default)]
    s3: Option<S3ArchiveConfig>,
}

impl RegisteredRestoreConfig {
    pub(super) async fn verify<R: RegisteredRestore>(
        &self,
        config: &RegisteredConfig,
        shared: Arc<WorkerSharedResources>,
        keys: Arc<ArchiveKeys>,
    ) -> Result<R, ProtocolError> {
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
        let verified = R::verify(
            &self.archives,
            self.target_cut,
            control.identity.database.as_str(),
            Some(config.initial_run),
            shared,
            source,
            keys,
        )
        .await?;
        if !verified.plan().matches_registered(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::pool::mount::{PoolMountExpectation, PoolMountProfile};
    use crate::oltp_replication::types::Page;

    struct RejectArchive;

    impl RegisteredRestore for RejectArchive {
        async fn verify(
            _: &[PathBuf],
            _: Option<CommitTs>,
            _: &str,
            _: Option<Uuid>,
            _: Arc<WorkerSharedResources>,
            _: Option<Arc<dyn Repository>>,
            _: Arc<ArchiveKeys>,
        ) -> Result<Self, ProtocolError> {
            Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "test archive unavailable",
                Outcome::NotApplied,
            ))
        }

        fn plan(&self) -> &VerifiedRestorePlan {
            unreachable!("the test verifier rejects the archive")
        }

        async fn install(
            self,
            _: OfflineRestoreTarget,
            _: &OltpPool,
        ) -> Result<RestoredGenesis, ProtocolError> {
            unreachable!("the test verifier rejects the archive")
        }
    }

    #[tokio::test]
    async fn o19_registered_restore_checks_placement_before_verifying_archive() {
        let cluster_id = Uuid::from_u128(1);
        let worker_id = Uuid::from_u128(2);
        let pool_id = Uuid::from_u128(3);
        let member = ReplicaMember {
            node_id: 1,
            worker_id,
            pool_id,
        };
        let enrollment = WorkerEnrollment {
            worker_id,
            incarnation: Uuid::from_u128(4),
            enrollment_revision: 1,
            certificate_sha256: Digest::from_bytes([1; 32]),
            endpoint: Text::try_from("127.0.0.1:1").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
        };
        let placement = |id, kind| GroupPlacement {
            identity: GroupIdentity {
                cluster_id,
                database: Text::try_from("restore").unwrap(),
                group_id: Uuid::from_u128(id),
                kind,
                group_format_version: GROUP_FORMAT_VERSION,
            },
            voters: Page::new(vec![member.clone()]).unwrap(),
        };
        let directory = PathBuf::from("/test/registered-restore");
        let pins = Page::new(vec![enrollment.certificate_sha256]).unwrap();
        let mut config = RegisteredConfig {
            format: 1,
            scoped_oltp: false,
            directory: directory.clone(),
            pool_id,
            scheduler_node_id: 1,
            bind: "127.0.0.1:1".parse().unwrap(),
            certificate: directory.join("certificate.pem"),
            private_key: directory.join("key.pem"),
            authority_ca: directory.join("ca.pem"),
            bootstrap_id: Uuid::from_u128(5),
            initial_run: Uuid::from_u128(6),
            required_voters: 1,
            placements: SystemPlacement {
                control: placement(10, GroupKind::Control),
                timestamps: placement(11, GroupKind::Timestamps),
                status: placement(12, GroupKind::Status { bucket: 0 }),
                status_partitions: Page::new(vec![]).unwrap(),
                catalog: placement(13, GroupKind::Catalog),
            },
            control_enrollments: Page::new(vec![enrollment.clone()]).unwrap(),
            operator_pins: pins,
            mount: Some(PoolMountExpectation {
                profile: PoolMountProfile::Ebs,
                identity: PoolIdentity {
                    engine: "oltp".into(),
                    pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                    cluster_id,
                    worker_id,
                    pool_id,
                },
                worker: WorkerIdentity::from(&enrollment),
                generation: 1,
                mount_path: directory.clone(),
                provider_volume_id: String::new(),
                filesystem_uuid: String::new(),
            }),
            enrollment,
            secondary_pools: Page::new(vec![]).unwrap(),
            restore: None,
            archive_repository: None,
            archive_repository_s3: None,
            archive_encryption: None,
            cold: None,
            rolling: None,
            system_replacement: None,
            bootstrap_placements: None,
        };
        let restore = RegisteredRestoreConfig {
            archives: vec![directory.join("backup")],
            target_cut: None,
            s3: None,
        };
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let keys = ArchiveKeys::load(None).unwrap();

        let valid_error = restore
            .verify::<RejectArchive>(&config, shared.clone(), keys.clone())
            .await
            .err()
            .expect("valid placement reaches the archive verifier");
        assert_eq!(valid_error.message, "test archive unavailable");

        config.placements.catalog.voters = Page::new(vec![ReplicaMember {
            pool_id: Uuid::from_u128(99),
            ..member
        }])
        .unwrap();
        let mismatch = restore
            .verify::<RejectArchive>(&config, shared, keys)
            .await
            .err()
            .expect("mismatched pool is rejected");
        assert_eq!(mismatch, invalid());
    }
}
