use super::model::unavailable;
use crate::{
    oltp_replication::types::WorkerIdentity,
    protocol::ProtocolError,
};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use uuid::Uuid;

pub(crate) use nabedra_oltp_types::authority::DrainMount;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum PoolDrainPhase {
    Draining,
    Closing,
    Closed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct PoolDrainRecord {
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
#[doc(hidden)]
pub struct RetainedPoolDetachPermit {
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

    #[doc(hidden)]
    pub fn record(&self) -> &PoolDrainRecord {
        &self.record
    }
}
