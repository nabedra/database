use crate::{
    oltp_replication::types::{AuthorityView, GroupIdentity, GroupKind, WorkerIdentity},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Control publishes queue placement separately from the Data Raft placement.
/// The queue epoch names exact RAM holders, including their process incarnations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedQueuePlacement {
    pub group: GroupIdentity,
    pub data_generation: Uuid,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
    pub owner: WorkerIdentity,
    pub members: [WorkerIdentity; 3],
}

impl ScopedQueuePlacement {
    pub(crate) fn validate(&self, group: &GroupIdentity) -> Result<(), ProtocolError> {
        if &self.group != group
            || self.group.validate().is_err()
            || !matches!(self.group.kind, GroupKind::Data { .. })
            || self.data_generation.is_nil()
            || self.queue_generation.is_nil()
            || self.view.validate().is_err()
            || !self.members.contains(&self.owner)
            || self.members.iter().any(|member| member.validate().is_err())
            || self
                .members
                .windows(2)
                .any(|pair| pair[0].worker_id >= pair[1].worker_id)
        {
            return Err(stale());
        }
        Ok(())
    }
}

pub(super) fn stale() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped queue placement is unavailable or has changed",
        Outcome::Unknown,
    )
}
