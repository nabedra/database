use crate::oltp_replication::types::{Authority, AuthorityView, GroupIdentity, WorkerIdentity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[doc(hidden)]
pub use nabedra_oltp_types::authority::{ScopedQueueInventory, ScopedQueuePlacement, ScopedQueueTransferReport, ScopedQueueTransition};

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
#[doc(hidden)]
pub struct ScopedQueueCapability {
    pub version: u32,
    pub run_id: Uuid,
    pub enrollment_revision: u64,
    pub configuration_id: Option<Uuid>,
    pub activation: Option<Uuid>,
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

pub(super) use nabedra_oltp_types::authority::stale_queue_placement as stale;
