//! Authority records shared by storage and replication.

mod history;
mod outcome;
mod pool;
mod proofs;
mod timestamp_point;

pub use history::*;
pub use outcome::*;
pub use pool::*;
pub use proofs::*;
pub use timestamp_point::*;

use crate::{owned::serialized_digest, types::*};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_resources::memory::ScratchLease;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Maximum concurrent grant records copied with a queued read.
pub const MAX_READ_GRANTS: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaRetiredStamp {
    pub group: GroupIdentity,
    pub generation: Uuid,
    pub replica: ReplicaMember,
    pub worker: WorkerIdentity,
    pub applied_at: AppliedPosition,
}

/// A current queue member reads point routing metadata at the request's
/// original timestamp. The participant checks the exact key and table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedCatalogRead {
    pub queue_group_id: Uuid,
    pub data_generation: Uuid,
    pub queue_generation: Uuid,
    pub queue_view: AuthorityView,
    pub request: RequestBinding,
    pub table_id: Uuid,
    pub read_ts: ReadTs,
}

/// Control publishes queue placement separately from the Data Raft placement.
/// The queue epoch names exact RAM holders, including their process incarnations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedQueuePlacement {
    pub group: GroupIdentity,
    pub data_generation: Uuid,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
    pub owner: WorkerIdentity,
    pub members: [WorkerIdentity; 3],
    /// A same-generation recovery may fence one logically removed process
    /// without changing the Data voters or the RF3 quorum definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_holder: Option<WorkerIdentity>,
}

impl ScopedQueuePlacement {
    pub fn validate(&self, group: &GroupIdentity) -> Result<(), ProtocolError> {
        if &self.group != group
            || self.group.validate().is_err()
            || !matches!(self.group.kind, GroupKind::Data { .. })
            || self.data_generation.is_nil()
            || self.queue_generation.is_nil()
            || self.view.validate().is_err()
            || !self.has_holder(self.owner)
            || self
                .unavailable_holder
                .is_some_and(|worker| !self.members.contains(&worker))
            || self.members.iter().any(|member| member.validate().is_err())
            || self
                .members
                .windows(2)
                .any(|pair| pair[0].worker_id >= pair[1].worker_id)
        {
            return Err(stale_queue_placement());
        }
        Ok(())
    }

    pub fn has_holder(&self, worker: WorkerIdentity) -> bool {
        self.members.contains(&worker) && self.unavailable_holder != Some(worker)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedQueueInventory {
    pub count: u64,
    pub root: Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ScopedQueueTransferReport {
    Frozen(ScopedQueueInventory),
    Installed {
        basis: Digest,
        inventory: ScopedQueueInventory,
    },
}

/// Metadata for a single frozen transfer. Array slots are in the respective
/// placement's worker order. No executable request data is stored here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedQueueTransition {
    pub id: Uuid,
    pub previous: ScopedQueuePlacement,
    pub next: ScopedQueuePlacement,
    pub frozen: [Option<ScopedQueueInventory>; 3],
    pub installed: [Option<ScopedQueueInventory>; 3],
}

impl ScopedQueueTransition {
    /// The two source cuts are immutable once a source quorum is recorded.
    pub fn basis(&self) -> Result<Option<Digest>, ProtocolError> {
        if self.frozen.iter().flatten().count() != 2 {
            return Ok(None);
        }
        serialized_digest(&(
            "nabedra/scoped-queue/transfer-basis-v1",
            self.id,
            &self.previous,
            &self.next,
            &self.frozen,
        ))
        .map(Some)
    }

    pub fn has_installed_quorum(&self) -> bool {
        let cuts = self.next.members.into_iter().zip(self.installed);
        let Some(owner_cut) = cuts
            .clone()
            .find_map(|(worker, cut)| (worker == self.next.owner).then_some(cut).flatten())
        else {
            return false;
        };
        cuts.filter(|(worker, cut)| self.next.has_holder(*worker) && *cut == Some(owner_cut))
            .count()
            >= 2
    }
}

pub fn stale_queue_placement() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped queue placement is unavailable or has changed",
        Outcome::Unknown,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Read,
    Mutation,
}
/// Persistent placement for a scoped queue admission. It has no executable
/// bytes and cannot reconstruct the request after its RAM holders stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedAdmission {
    pub protocol_version: u32,
    pub queue_group_id: Uuid,
    pub queue_generation: Uuid,
    pub status_group_id: Uuid,
}
impl ScopedAdmission {
    pub const VERSION: u32 = 2;

    pub fn valid_for(self, status_group_id: Uuid) -> bool {
        self.protocol_version == Self::VERSION
            && !self.queue_group_id.is_nil()
            && !self.queue_generation.is_nil()
            && self.queue_group_id != status_group_id
            && self.status_group_id == status_group_id
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityReservation {
    pub request_id: Uuid,
    pub digest: Digest,
    pub original_run: Uuid,
    pub reservation_id: Uuid,
    pub kind: RequestKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scoped: Option<ScopedAdmission>,
}
impl IdentityReservation {
    pub fn binds(self, request: RequestBinding) -> bool {
        self.request_id == request.request_id
            && self.digest == request.digest
            && self.original_run == request.run_id
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TerminalIdentity {
    ReadExpired {
        reservation: IdentityReservation,
        request: RequestBinding,
        terminal_version: u64,
        response_hash: Digest,
        recorded_at: AppliedPosition,
    },
    Committed {
        reservation: IdentityReservation,
        decision: DecisionStatement,
        outcome: MutationOutcome,
    },
    Failed {
        reservation: IdentityReservation,
        request: RequestBinding,
        outcome: MutationOutcome,
        recorded_at: AppliedPosition,
    },
}
impl TerminalIdentity {
    pub fn request_binding(&self) -> RequestBinding {
        match self {
            Self::ReadExpired { request, .. } | Self::Failed { request, .. } => *request,
            Self::Committed { decision, .. } => decision.request,
        }
    }

    pub fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        if let Self::Committed { outcome, .. } | Self::Failed { outcome, .. } = self {
            outcome.retain_owner(owner);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredHistoryOrigin {
    pub restore_id: Uuid,
    pub source_cluster: Uuid,
    pub source_archive: Uuid,
    pub source_root: Digest,
    pub through_cut: CommitTs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedIdentity {
    pub reservation: IdentityReservation,
    pub view: AuthorityView,
    pub identity: PinIdentity,
    pub pin: Option<StoragePin>,
    pub request: Option<RequestBinding>,
    pub failure: MutationOutcome,
    pub recorded_at: AppliedPosition,
}
impl RejectedIdentity {
    pub fn same_disposition(&self, expected: &Self) -> bool {
        self.reservation == expected.reservation
            && self.view == expected.view
            && self.identity == expected.identity
            && self.pin == expected.pin
            && self.request == expected.request
            && self.failure == expected.failure
    }
}

/// Catalog MVCC resolves this lineage at storage_pin.read_ts. A TSO allocation
/// alone cannot manufacture schema/routing lineage supplied by a caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueReadToken {
    pub identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub catalog_root: Digest,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadScope {
    pub request: RequestBinding,
    pub authority: Authority,
    pub read_ts: ReadTs,
}

/// The original issuer and exact receiver of one Data scan. The queue retains
/// this identity in RAM; the worker keeps the executable input separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    pub scope: ReadScope,
    pub owner: WorkerIdentity,
    pub worker: WorkerIdentity,
    pub group: Uuid,
    pub node: u64,
    pub ticket: Uuid,
    pub input: Digest,
}
