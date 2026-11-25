//! Sealed authority for a new offline restore target. No network DTO or live
//! application request can construct the permission used by native import.
use super::history::{HistoryScope, VerifiedHistory};
use crate::{
    oltp::{failure, pool::PoolIdentity},
    protocol::{ErrorCode, ProtocolError},
};
use nabedra_oltp_types::types::{CommitTs, Digest};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

/// A constant-memory validator for the base and contiguous archived segments.
pub struct RestorePlanValidator {
    target: CommitTs,
    base: Option<HistoryScope>,
    previous_cut: Option<CommitTs>,
    previous_root: Option<Digest>,
    maximum_timestamp: Option<CommitTs>,
    segments: u64,
}
pub struct VerifiedRestorePlan {
    base: HistoryScope,
    target: CommitTs,
    maximum_timestamp: CommitTs,
    final_root: Digest,
    segments: u64,
}
impl VerifiedRestorePlan {
    pub fn matches_registered(
        &self,
        cluster: Uuid,
        database: &nabedra_oltp_types::types::Text<128>,
        groups: impl IntoIterator<Item = Uuid>,
    ) -> bool {
        cluster != self.base.logical_cluster
            && database == &self.base.database
            && groups.into_iter().eq(self.base.system_groups())
    }
}
impl RestorePlanValidator {
    pub fn new(target: CommitTs) -> Self {
        Self {
            target,
            base: None,
            previous_cut: None,
            previous_root: None,
            maximum_timestamp: None,
            segments: 0,
        }
    }
    pub fn include(&mut self, history: &VerifiedHistory) -> Result<(), ProtocolError> {
        if !history.identities_complete() {
            return Err(invalid());
        }
        let scope = history.scope();
        match &self.base {
            None => {
                if scope.base_exclusive.is_some()
                    || scope.previous_segment.is_some()
                    || self.target < scope.cut
                {
                    return Err(invalid());
                }
                self.base = Some(scope.clone());
            }
            Some(base) => {
                if scope.archive_id == base.archive_id
                    || scope.logical_cluster != base.logical_cluster
                    || scope.database != base.database
                    || scope.control_group != base.control_group
                    || scope.timestamp_group != base.timestamp_group
                    || scope.catalog_group != base.catalog_group
                    || !scope.status_groups().eq(base.status_groups())
                    || scope.base_exclusive != self.previous_cut
                    || scope.previous_segment != self.previous_root
                    || self
                        .previous_cut
                        .is_none_or(|previous| scope.cut <= previous)
                {
                    return Err(invalid());
                }
            }
        }
        self.previous_cut = Some(scope.cut);
        self.previous_root = Some(history.root());
        self.maximum_timestamp = Some(
            self.maximum_timestamp
                .map_or(scope.maximum_timestamp, |old| {
                    old.max(scope.maximum_timestamp)
                }),
        );
        self.segments = self.segments.checked_add(1).ok_or_else(invalid)?;
        Ok(())
    }
    pub fn finish(self) -> Result<VerifiedRestorePlan, ProtocolError> {
        if self.previous_cut.is_none_or(|cut| cut < self.target) {
            return Err(invalid());
        }
        Ok(VerifiedRestorePlan {
            base: self.base.ok_or_else(invalid)?,
            target: self.target,
            maximum_timestamp: self.maximum_timestamp.ok_or_else(invalid)?,
            final_root: self.previous_root.ok_or_else(invalid)?,
            segments: self.segments,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreGenesisStatement {
    pub format_version: u32,
    pub restore_id: Uuid,
    pub source_cluster: Uuid,
    pub database: nabedra_oltp_types::types::Text<128>,
    pub source_archive: Uuid,
    pub source_root: Digest,
    pub segment_count: u64,
    pub base_cut: CommitTs,
    pub target_cut: CommitTs,
    pub maximum_timestamp: CommitTs,
    pub target_pool: PoolIdentity,
    pub control_group: Uuid,
    pub timestamp_group: Uuid,
    pub catalog_group: Uuid,
    pub initial_status_group: Uuid,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status_partitions: Vec<Uuid>,
}
struct Target {
    stage: PathBuf,
    destination: PathBuf,
    statement: RestoreGenesisStatement,
    active: AtomicBool,
    published: AtomicBool,
    publish_in_place: bool,
    _lock: File,
}
pub struct OfflineRestoreTarget(Arc<Target>);
/// This is produced only after the private target has flushed and the second
/// archive pass reproduced the exact verified segment root and target cut.
pub struct RestoredGenesis {
    statement: RestoreGenesisStatement,
    target: Arc<Target>,
}
impl OfflineRestoreTarget {
    pub fn claim(
        destination: &Path,
        plan: &VerifiedRestorePlan,
    ) -> Result<Self, ProtocolError> {
        if !plan.base.status_partitions.is_empty() {
            return Err(failure(
                ErrorCode::UnsupportedOperation,
                "partitioned Status archives require a registered restore target",
            ));
        }
        if destination.file_name().is_none() {
            return Err(invalid());
        }
        let publish_in_place = match destination.symlink_metadata() {
            Ok(metadata) => {
                if !metadata.is_dir()
                    || metadata.file_type().is_symlink()
                    || fs::read_dir(destination)
                        .map_err(|_| invalid())?
                        .next()
                        .is_some()
                {
                    return Err(invalid());
                }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return Err(invalid()),
        };
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = fs::canonicalize(parent).map_err(|_| invalid())?;
        let destination = parent.join(destination.file_name().ok_or_else(invalid)?);
        let restore_id = nabedra_rt::ids::uuid_v4();
        let stage = if publish_in_place {
            destination.clone()
        } else {
            parent.join(format!(".nabedra-native-restore-{restore_id}"))
        };
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        if !publish_in_place {
            builder.create(&stage).map_err(|_| invalid())?;
        }
        Self::claim_stage(
            stage,
            destination,
            plan,
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: nabedra_rt::ids::uuid_v4(),
                worker_id: nabedra_rt::ids::uuid_v4(),
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            restore_id,
            publish_in_place,
        )
    }

    pub fn claim_mounted(
        mount: &crate::oltp::pool::VerifiedPoolMount,
        plan: &VerifiedRestorePlan,
    ) -> Result<Self, ProtocolError> {
        let expected = &mount.observation().expected;
        let path = mount.restore_root(&expected.identity)?;
        for item in fs::read_dir(&path).map_err(|_| invalid())? {
            let item = item.map_err(|_| invalid())?;
            if !matches!(
                item.file_name().to_str(),
                Some(
                    "worker.lock"
                        | ".nabedra-volume.lock"
                        | ".nabedra-volume-v1.json"
                        | ".nabedra-docker-volume.json"
                )
            ) || !item.file_type().map_err(|_| invalid())?.is_file()
            {
                return Err(invalid());
            }
        }
        let restore_id = nabedra_oltp_types::owned::serialized_digest(&(
            b"nabedra/mounted-restore-genesis/v1".as_slice(),
            plan.base.archive_id,
            plan.final_root,
            plan.target,
            expected.identity.cluster_id,
        ))?;
        let restore_id = Uuid::from_slice(&restore_id.as_bytes()[..16]).map_err(|_| invalid())?;
        Self::claim_stage(
            path.clone(),
            path,
            plan,
            expected.identity.clone(),
            restore_id,
            true,
        )
    }

    fn claim_stage(
        stage: PathBuf,
        destination: PathBuf,
        plan: &VerifiedRestorePlan,
        target_pool: PoolIdentity,
        restore_id: Uuid,
        publish_in_place: bool,
    ) -> Result<Self, ProtocolError> {
        if target_pool.cluster_id == plan.base.logical_cluster || restore_id.is_nil() {
            return Err(invalid());
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let lock = options
            .open(stage.join(".restore-owner"))
            .map_err(|_| invalid())?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                return Err(invalid());
            }
        }
        let statement = RestoreGenesisStatement {
            format_version: plan.base.format_version,
            restore_id,
            source_cluster: plan.base.logical_cluster,
            database: plan.base.database.clone(),
            source_archive: plan.base.archive_id,
            source_root: plan.final_root,
            segment_count: plan.segments,
            base_cut: plan.base.cut,
            target_cut: plan.target,
            maximum_timestamp: plan.maximum_timestamp,
            target_pool,
            control_group: plan.base.control_group,
            timestamp_group: plan.base.timestamp_group,
            catalog_group: plan.base.catalog_group,
            initial_status_group: plan.base.initial_status_group,
            status_partitions: plan.base.status_partitions.clone(),
        };
        write_statement(&stage.join("RESTORE_IN_PROGRESS.json"), &statement)?;
        File::open(&stage)
            .and_then(|file| file.sync_all())
            .map_err(|_| invalid())?;
        Ok(Self(Arc::new(Target {
            stage,
            destination,
            statement,
            active: AtomicBool::new(true),
            published: AtomicBool::new(false),
            publish_in_place,
            _lock: lock,
        })))
    }
    pub fn stage(&self) -> &Path {
        &self.0.stage
    }
    pub fn statement(&self) -> &RestoreGenesisStatement {
        &self.0.statement
    }
    pub(in crate::oltp) fn authorize_pool(
        &self,
        identity: &PoolIdentity,
        pool_path: &Path,
    ) -> Result<(), ProtocolError> {
        if !self.0.active.load(Ordering::Acquire)
            || identity != &self.0.statement.target_pool
            || pool_path != self.0.stage.join("pool")
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn finish_images(
        self,
        second_pass: VerifiedRestorePlan,
        flush: crate::oltp::pool::restore::VerifiedRestoreFlush,
    ) -> Result<RestoredGenesis, ProtocolError> {
        let statement = &self.0.statement;
        if !flush.matches(&statement.target_pool)
            || second_pass.base.archive_id != statement.source_archive
            || second_pass.final_root != statement.source_root
            || second_pass.segments != statement.segment_count
            || second_pass.target != statement.target_cut
            || second_pass.maximum_timestamp != statement.maximum_timestamp
        {
            return Err(invalid());
        }
        if self.0.active.swap(false, Ordering::AcqRel) == false {
            return Err(invalid());
        }
        write_statement(&self.0.stage.join("RESTORED_GENESIS.json"), statement)?;
        File::open(&self.0.stage)
            .and_then(|file| file.sync_all())
            .map_err(|_| invalid())?;
        Ok(RestoredGenesis {
            statement: statement.clone(),
            target: self.0,
        })
    }
}
impl RestoredGenesis {
    pub fn statement(&self) -> &RestoreGenesisStatement {
        &self.statement
    }
    pub fn stage(&self) -> &Path {
        &self.target.stage
    }
    /// Called only after native flush/close, new-run fences and the restore
    /// oracle. A marker cannot make a partial target visible through a URL.
    pub fn publish(self) -> Result<PathBuf, ProtocolError> {
        if (!self.target.publish_in_place && self.target.destination.symlink_metadata().is_ok())
            || self.target.active.load(Ordering::Acquire)
        {
            return Err(invalid());
        }
        // Reserve an absent destination. rename must not replace an existing
        // directory; an operator's concurrent target creation is a conflict.
        // An existing empty destination publishes in place. Physical mount
        // attestation belongs to the registered restore profile. An absent
        // development target preserves its inode across the final move.
        if !self.target.publish_in_place {
            rename_new_directory(&self.target.stage, &self.target.destination)?;
        }
        let ready = self.target.destination.join("RESTORE_READY.json");
        let staged = self
            .target
            .destination
            .join(format!(".restore-ready-{}.tmp", nabedra_rt::ids::uuid_v4()));
        write_statement(&staged, &self.statement)?;
        fs::hard_link(&staged, &ready).map_err(|_| invalid())?;
        fs::remove_file(&staged).map_err(|_| invalid())?;
        File::open(&self.target.destination)
            .and_then(|file| file.sync_all())
            .map_err(|_| invalid())?;
        if !self.target.publish_in_place {
            let parent = self.target.destination.parent().ok_or_else(invalid)?;
            File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(|_| invalid())?;
        }
        self.target.published.store(true, Ordering::Release);
        Ok(self.target.destination.clone())
    }
}
pub fn verified_restore_marker(
    root: &Path,
    identity: &PoolIdentity,
) -> Result<RestoreGenesisStatement, ProtocolError> {
    fn read(path: &Path) -> Result<RestoreGenesisStatement, ProtocolError> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        }
        let file = options.open(path).map_err(|_| invalid())?;
        let metadata = file.metadata().map_err(|_| invalid())?;
        if !metadata.is_file() || metadata.len() > 64 << 10 {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        file.take((64 << 10) + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if bytes.len() as u64 != metadata.len() {
            return Err(invalid());
        }
        serde_json::from_slice(&bytes).map_err(|_| invalid())
    }
    let ready = read(&root.join("RESTORE_READY.json"))?;
    if !matches!(
        (ready.format_version, ready.status_partitions.is_empty()),
        (1, true) | (2, false)
    ) || &ready.target_pool != identity
        || ready != read(&root.join("RESTORED_GENESIS.json"))?
        || ready != read(&root.join("RESTORE_IN_PROGRESS.json"))?
    {
        return Err(invalid());
    }
    Ok(ready)
}
fn write_statement(path: &Path, statement: &RestoreGenesisStatement) -> Result<(), ProtocolError> {
    let bytes = serde_json::to_vec(statement).map_err(|_| invalid())?;
    if bytes.len() > 64 << 10 {
        return Err(invalid());
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|_| invalid())?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| invalid())
}
#[track_caller]
fn invalid() -> ProtocolError {
    nabedra_rt::diag_at("invalid", std::panic::Location::caller());
    failure(
        ErrorCode::StorageUnavailable,
        "native restore requires a verified contiguous history and an exclusive empty offline target",
    )
}

fn rename_new_directory(source: &Path, destination: &Path) -> Result<(), ProtocolError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let source =
            std::ffi::CString::new(source.as_os_str().as_bytes()).map_err(|_| invalid())?;
        let destination =
            std::ffi::CString::new(destination.as_os_str().as_bytes()).map_err(|_| invalid())?;
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        let result =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        return Err(invalid());
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        if result == 0 {
            return Ok(());
        }
    }
    Err(invalid())
}
