use crate::{PoolIdentity, types::WorkerIdentity};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PoolMountProfile {
    #[default]
    Ebs,
    Docker {
        volume_marker: uuid::Uuid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolMountExpectation {
    #[serde(default)]
    pub profile: PoolMountProfile,
    pub identity: PoolIdentity,
    pub worker: WorkerIdentity,
    pub generation: u64,
    pub mount_path: PathBuf,
    pub provider_volume_id: String,
    pub filesystem_uuid: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountedPoolObservation {
    pub expected: PoolMountExpectation,
    pub mount_id: u64,
    pub device_major: u32,
    pub device_minor: u32,
    pub root_inode: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
}

impl PoolMountExpectation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.refuse_legacy_format()?;
        self.identity.validate()?;
        self.worker.validate().map_err(|_| mount_unavailable())?;
        if self.identity.worker_id != self.worker.worker_id
            || self.generation == 0
            || !self.mount_path.is_absolute()
            || self.mount_path == Path::new("/")
            || self.mount_path.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || self.mount_path.as_os_str().len() > 4096
            || match &self.profile {
                PoolMountProfile::Ebs => {
                    !valid_volume_id(&self.provider_volume_id)
                        || uuid::Uuid::parse_str(&self.filesystem_uuid).is_err()
                }
                PoolMountProfile::Docker { volume_marker } => {
                    volume_marker.is_nil()
                        || !self.filesystem_uuid.is_empty()
                        || docker_volume_name(&self.provider_volume_id).is_none()
                }
            }
        {
            return Err(mount_unavailable());
        }
        Ok(())
    }

    pub fn refuse_legacy_format(&self) -> Result<(), ProtocolError> {
        if self.identity.engine == "oltp" && self.identity.pool_format_version == 1 {
            return Err(unsupported_pool_format(&format!(
                "{}: pool_format_version 1",
                self.mount_path.display()
            )));
        }
        Ok(())
    }
}

/// An exact mount instance. Filesystem free space can change during drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainMount {
    pub expected: PoolMountExpectation,
    pub mount_id: u64,
    pub device_major: u32,
    pub device_minor: u32,
    pub root_inode: u64,
}
impl From<MountedPoolObservation> for DrainMount {
    fn from(mount: MountedPoolObservation) -> Self {
        Self {
            expected: mount.expected,
            mount_id: mount.mount_id,
            device_major: mount.device_major,
            device_minor: mount.device_minor,
            root_inode: mount.root_inode,
        }
    }
}
impl DrainMount {
    pub fn heap_bytes(&self) -> usize {
        self.expected.provider_volume_id.capacity()
            + self.expected.filesystem_uuid.capacity()
            + self.expected.mount_path.capacity()
            + self.expected.identity.engine.capacity()
    }
}

pub fn mount_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "registered pool mount, device or exclusive ownership does not match inventory",
        Outcome::NotApplied,
    )
}
#[doc(hidden)]
pub fn valid_volume_id(value: &str) -> bool {
    value.strip_prefix("vol-").is_some_and(|id| {
        matches!(id.len(), 8 | 17)
            && id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    })
}
pub fn docker_volume_name(value: &str) -> Option<&str> {
    value.strip_prefix("docker-volume:").filter(|name| {
        !name.is_empty()
            && name.len() <= 255
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            && name.as_bytes()[0].is_ascii_alphanumeric()
    })
}

pub fn unsupported_pool_format(found: &str) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        format!(
            "unsupported store format: {found}. For RocksDB-era OLTP pools, use the archive export and restore procedure in docs/oltp-rocksdb-upgrade.md"
        ),
        Outcome::NotApplied,
    )
}
