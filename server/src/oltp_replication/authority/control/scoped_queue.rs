use crate::{
    oltp_replication::{
        owned::serialized_digest,
        types::{Authority, AuthorityView, Digest, GroupIdentity, GroupKind, WorkerIdentity},
    },
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
    /// A same-generation recovery may fence one logically removed process
    /// without changing the Data voters or the RF3 quorum definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_holder: Option<WorkerIdentity>,
}

impl ScopedQueuePlacement {
    pub(crate) fn validate(&self, group: &GroupIdentity) -> Result<(), ProtocolError> {
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
            return Err(stale());
        }
        Ok(())
    }

    pub(crate) fn has_holder(&self, worker: WorkerIdentity) -> bool {
        self.members.contains(&worker) && self.unavailable_holder != Some(worker)
    }
}

pub(super) fn permits_new_run(
    prior_run: Uuid,
    current: AuthorityView,
    publication: Option<(Uuid, Authority, Authority)>,
    known_prior: bool,
) -> bool {
    known_prior
        && prior_run != current.run_id
        && publication.is_some_and(|(id, prior, next)| {
            !id.is_nil()
                && prior.run_id != next.run_id
                && prior.owner_term.checked_add(1) == Some(next.owner_term)
                && next.run_id == current.run_id
                && next.owner_term <= current.owner_term
        })
}

pub(crate) const SCOPED_QUEUE_VERSION: u32 = super::super::status::ScopedAdmission::VERSION;

/// A strong Control read for new identities. Runtime callers must consult
/// Status first, since an existing UUID retains its original queue policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedQueueCapability {
    pub version: u32,
    pub run_id: Uuid,
    pub enrollment_revision: u64,
    pub configuration_id: Option<Uuid>,
    pub activation: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedQueueInventory {
    pub count: u64,
    pub root: Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScopedQueueTransferReport {
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
pub(crate) struct ScopedQueueTransition {
    pub id: Uuid,
    pub previous: ScopedQueuePlacement,
    pub next: ScopedQueuePlacement,
    pub frozen: [Option<ScopedQueueInventory>; 3],
    pub installed: [Option<ScopedQueueInventory>; 3],
}

impl ScopedQueueTransition {
    /// The two source cuts are immutable once a source quorum is recorded.
    pub(crate) fn basis(&self) -> Result<Option<Digest>, ProtocolError> {
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

    pub(crate) fn has_installed_quorum(&self) -> bool {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScopedQueueOperation {
    Advertise {
        version: u32,
    },
    SetAdmission {
        expected: ScopedQueueCapability,
        enabled: bool,
    },
    Ensure {
        group: GroupIdentity,
    },
    BeginTransfer {
        expected: Box<ScopedQueuePlacement>,
        next_owner: WorkerIdentity,
        replaces: Option<Uuid>,
    },
    RebaseTransfer {
        group_id: Uuid,
        transfer_id: Uuid,
        removed: WorkerIdentity,
        next_owner: WorkerIdentity,
    },
    ReportTransfer {
        group_id: Uuid,
        transfer_id: Uuid,
        report: ScopedQueueTransferReport,
    },
    PublishTransfer {
        group_id: Uuid,
        transfer_id: Uuid,
    },
}

impl ScopedQueueOperation {
    pub(super) fn requires_operator(&self) -> bool {
        matches!(
            self,
            Self::SetAdmission { .. } | Self::BeginTransfer { .. } | Self::RebaseTransfer { .. }
        )
    }

    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Ensure { group } => group.database.capacity_bytes(),
            Self::BeginTransfer { expected, .. } => {
                std::mem::size_of::<ScopedQueuePlacement>()
                    + expected.group.database.capacity_bytes()
            }
            _ => 0,
        }
    }
}

pub(super) fn stale() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped queue placement is unavailable or has changed",
        Outcome::Unknown,
    )
}
