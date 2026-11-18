use super::model::unavailable;
use crate::{
    oltp::pool::mount::{MountedPoolObservation, PoolMountExpectation},
    oltp_replication::types::WorkerIdentity,
    protocol::ProtocolError,
};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use uuid::Uuid;

/// An exact mount instance. Filesystem free space can change during drain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrainMount {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PoolDrainPhase {
    Draining,
    Closing,
    Closed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolDrainRecord {
    pub id: Uuid,
    pub growth_claim: Uuid,
    pub mount: DrainMount,
    pub phase: PoolDrainPhase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PoolDrainCommand {
    Begin {
        growth_claim: Uuid,
        mount: Box<DrainMount>,
    },
    Prepare {
        id: Uuid,
        revision: u64,
    },
    Closed {
        id: Uuid,
        mount: Box<DrainMount>,
    },
    Cancel {
        id: Uuid,
    },
}
impl PoolDrainCommand {
    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Begin { mount, .. } | Self::Closed { mount, .. } => {
                size_of::<DrainMount>() + mount.heap_bytes()
            }
            _ => 0,
        }
    }
}

/// Adapter capability. Only an authenticated exact-owner read of the durable
/// closed phase can create it. It never grants worker stop or volume deletion.
pub(crate) struct RetainedPoolDetachPermit {
    record: PoolDrainRecord,
}
impl RetainedPoolDetachPermit {
    pub(in crate::oltp_replication::authority::control) fn from_closed(
        record: PoolDrainRecord,
        worker: WorkerIdentity,
    ) -> Result<Self, ProtocolError> {
        if record.phase != PoolDrainPhase::Closed || record.mount.expected.worker != worker {
            return Err(unavailable("exact pool closure is unavailable"));
        }
        Ok(Self { record })
    }

    pub(crate) fn record(&self) -> &PoolDrainRecord {
        &self.record
    }
}
