use super::super::timestamp::ReadyFenceReceipt;
use crate::{
    oltp::pool::ScratchLease,
    oltp_replication::{ReadySetHasher, owned::OwnedPayload, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::{fmt, sync::Arc};
use uuid::Uuid;

/// Produced by the leader's configured operator authorization boundary. A
/// worker RPC can submit an intent, but cannot create a raw Control proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthorizationStamp {
    pub authorization_id: Uuid,
    pub policy: Digest,
    pub subject: Digest,
    pub evidence: Digest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExclusionStamp {
    pub worker: WorkerIdentity,
    pub authorization: AuthorizationStamp,
    pub ownership_fence: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupRecord {
    pub identity: GroupIdentity,
    pub generation: Uuid,
    pub replica_count: u64,
    pub replica_root: Digest,
    pub membership_position: AppliedPosition,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupMembershipStamp {
    pub group: GroupIdentity,
    pub generation: Uuid,
    pub replica_count: u64,
    pub replica_root: Digest,
    pub applied_at: AppliedPosition,
    pub authority: Authority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadyChange {
    Join,
    Remove,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReadyPhase {
    Building,
    AwaitingTimestampFence,
    AwaitingQueueProof,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadyPending {
    pub transition_id: Uuid,
    pub expected: ReadyMembership,
    pub target: WorkerIdentity,
    pub change: ReadyChange,
    pub generation: Uuid,
    pub source_after: Option<Uuid>,
    pub target_seen: bool,
    pub root: ReadySetHasher,
    pub next: Option<ReadyMembership>,
    pub phase: ReadyPhase,
    pub timestamp_fence: Option<ReadyFenceReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupBuild {
    pub generation: Uuid,
    pub identity: GroupIdentity,
    pub count: u64,
    pub last_node: Option<u64>,
    pub chain: Digest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FencePending {
    pub transition_id: Uuid,
    pub expected: ReadyMembership,
    pub next: ReadyMembership,
    pub next_owner: Option<WorkerIdentity>,
    pub cold: bool,
    pub authorization: AuthorizationStamp,
    pub group_inventory_revision: u64,
    pub required_groups: u64,
    pub fenced_groups: u64,
    pub excluded_workers: u64,
    #[serde(default)]
    pub extra_exclusion: Option<WorkerIdentity>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Pending {
    Ready(ReadyPending),
    Fence(FencePending),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub policy: Digest,
    pub ready: ReadyMembership,
    pub owner: Option<WorkerIdentity>,
    pub ready_generation: Uuid,
    pub enrollment_revision: u64,
    pub group_inventory_revision: u64,
    pub group_count: u64,
    pub invalid_ready: u64,
    pub pending: Option<Pending>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerRecord {
    pub enrollment: WorkerEnrollment,
    pub previous_process: Option<ExclusionStamp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ControlCommand {
    Initialize {
        operation_id: Uuid,
        run_id: Uuid,
        authorization: AuthorizationStamp,
    },
    Enroll {
        operation_id: Uuid,
        expected: Option<WorkerIdentity>,
        next: WorkerEnrollment,
        authorization: AuthorizationStamp,
        previous_process: Option<ExclusionStamp>,
    },
    BeginGroup {
        operation_id: Uuid,
        generation: Uuid,
        identity: GroupIdentity,
        authorization: AuthorizationStamp,
    },
    GroupPage {
        operation_id: Uuid,
        generation: Uuid,
        replicas: Page<ReplicaMember>,
    },
    SealGroup {
        operation_id: Uuid,
        generation: Uuid,
        proof: GroupMembershipStamp,
    },
    BeginJoin {
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
    },
    BeginRemoval {
        operation_id: Uuid,
        expected: AuthorityView,
        target: WorkerIdentity,
        exclusion: ExclusionStamp,
    },
    ReadyPage {
        operation_id: Uuid,
        transition_id: Uuid,
        members: Page<WorkerIdentity>,
    },
    SealReady {
        operation_id: Uuid,
        transition_id: Uuid,
    },
    TimestampFenced {
        operation_id: Uuid,
        transition_id: Uuid,
        receipt: ReadyFenceReceipt,
    },
    PublishReady {
        operation_id: Uuid,
        transition_id: Uuid,
        proof: QueueCopyStatement,
    },
    BeginOwnerFence {
        operation_id: Uuid,
        expected: ReadyMembership,
        next_owner: WorkerIdentity,
        authorization: AuthorizationStamp,
    },
    BeginColdFence {
        operation_id: Uuid,
        expected: ReadyMembership,
        next_run: Uuid,
        authorization: AuthorizationStamp,
    },
    ExcludedPage {
        operation_id: Uuid,
        transition_id: Uuid,
        workers: Page<ExclusionStamp>,
    },
    GroupFenced {
        operation_id: Uuid,
        receipt: GroupFenceReceipt,
    },
    PublishFence {
        operation_id: Uuid,
        transition_id: Uuid,
    },
}
impl ControlCommand {
    pub fn operation_id(&self) -> Uuid {
        match self {
            Self::Initialize { operation_id, .. }
            | Self::Enroll { operation_id, .. }
            | Self::BeginGroup { operation_id, .. }
            | Self::GroupPage { operation_id, .. }
            | Self::SealGroup { operation_id, .. }
            | Self::BeginJoin { operation_id, .. }
            | Self::BeginRemoval { operation_id, .. }
            | Self::ReadyPage { operation_id, .. }
            | Self::SealReady { operation_id, .. }
            | Self::TimestampFenced { operation_id, .. }
            | Self::PublishReady { operation_id, .. }
            | Self::BeginOwnerFence { operation_id, .. }
            | Self::BeginColdFence { operation_id, .. }
            | Self::ExcludedPage { operation_id, .. }
            | Self::GroupFenced { operation_id, .. }
            | Self::PublishFence { operation_id, .. } => *operation_id,
        }
    }
}
impl fmt::Display for ControlCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "control {}", self.operation_id())
    }
}
impl OwnedPayload for ControlCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(match self {
            Self::Enroll { next, .. } => next.endpoint.len() + next.server_name.len(),
            Self::BeginGroup { identity, .. } => identity.database.len(),
            Self::GroupPage { replicas, .. } => replicas.len() * size_of::<ReplicaMember>(),
            Self::ReadyPage { members, .. } => members.len() * size_of::<WorkerIdentity>(),
            Self::ExcludedPage { workers, .. } => workers.len() * size_of::<ExclusionStamp>(),
            Self::GroupFenced { receipt, .. } => receipt.group.database.len(),
            Self::SealGroup { proof, .. } => proof.group.database.len(),
            _ => 0,
        })
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Fault {
    NotInitialized,
    Invalid,
    Unauthorized,
    Stale,
    Conflict,
    Busy,
    NotFound,
    Incomplete,
    NotReady,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ControlReply {
    Initialized {
        ready: ReadyMembership,
    },
    Enrolled(WorkerIdentity),
    GroupBuilding {
        generation: Uuid,
        count: u64,
    },
    GroupRegistered {
        group_id: Uuid,
        count: u64,
        root: Digest,
    },
    JoinBegun {
        ticket: JoinTicket,
        bootstrap: bool,
    },
    ReadyBuilding {
        transition_id: Uuid,
        count: u64,
    },
    ReadySealed {
        transition_id: Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    },
    TimestampFenced {
        transition_id: Uuid,
        next: ReadyMembership,
    },
    Published {
        ready: ReadyMembership,
        owner: Option<WorkerIdentity>,
    },
    FenceBegun {
        transition_id: Uuid,
        next: ReadyMembership,
        required_groups: u64,
    },
    FenceProgress {
        transition_id: Uuid,
        fenced_groups: u64,
        excluded_workers: u64,
    },
    Rejected(Fault),
}

pub(super) fn unavailable(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::Unknown)
}
