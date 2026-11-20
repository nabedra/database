//! NS14: Control's side of a quarantined group (plan section 3, "Raft safety of
//! a quarantined group", rules 2 to 4 and 6).
//!
//! A worker sends `GroupDamaged { group, replica, reason, first_bad_seq }` for
//! every replica it cannot serve: a quarantined group (rule 2) or every replica
//! of a refused pool (rule 6, reason `pool_refused`). Control records it once
//! per replica (`ct/group-damaged/<group><node>`); repeating the report returns
//! the same record, so the worker retries until it reads an acknowledgment.
//!
//! The replacement (rule 3) runs on the group's leader with the existing
//! joint-membership `replace_voter` (`manager/lifecycle.rs`). Once the change
//! that removes the damaged identity commits, `GroupDamageReplaced` moves the
//! record to `replaced`. That record is the evidence the damaged worker needs
//! to release the bytes (rule 4); without it nothing is deleted (rule 5).
//!
//! Mixed versions: an older Control replica cannot decode either command, so
//! the leader proposes them only behind an installed [`GroupDamageGate`]. An
//! older Control leader rejects the new read request, and the worker retries.
use super::machine;
use crate::{
    oltp::pool::{DamageCause, DamagedReplicaRemoved},
    oltp_replication::types::*,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Why a replica cannot serve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GroupDamageReason {
    JournalRecord,
    RunFile,
    RunBlock,
    JournalValue,
    /// Its whole pool was refused at open (rule 6, O26).
    PoolRefused,
}
impl From<DamageCause> for GroupDamageReason {
    fn from(cause: DamageCause) -> Self {
        match cause {
            DamageCause::JournalRecord => Self::JournalRecord,
            DamageCause::RunFile => Self::RunFile,
            DamageCause::RunBlock => Self::RunBlock,
            DamageCause::JournalValue => Self::JournalValue,
        }
    }
}

/// The plan's `GroupDamaged { group, replica, reason, first_bad_seq }`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupDamageReport {
    pub group: GroupIdentity,
    pub replica: ReplicaMember,
    pub reason: GroupDamageReason,
    pub first_bad_seq: Option<u64>,
}
impl GroupDamageReport {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.group.validate().map_err(|_| invalid())?;
        self.replica.validate().map_err(|_| invalid())?;
        if self.group.kind == GroupKind::Control {
            // Control replicas are replaced by the Control membership flows.
            return Err(invalid());
        }
        Ok(())
    }
    /// The operation ID of `worker`'s report: repeating the same report from
    /// the same process replays Control's receipt instead of conflicting.
    pub(crate) fn operation_id(&self, worker: WorkerIdentity) -> Result<Uuid, ProtocolError> {
        let digest =
            crate::oltp_replication::owned::serialized_digest(&("group-damaged", worker, self))?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(Uuid::from_bytes(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum GroupDamagePhase {
    /// Reported; the damaged identity is still a voter (or its replacement
    /// has not committed).
    Reported,
    /// The membership change that removed the damaged identity committed.
    Replaced {
        replacement: ReplicaMember,
        committed_at: AppliedPosition,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupDamageRecord {
    pub report: GroupDamageReport,
    pub worker: WorkerIdentity,
    pub reported_at: AppliedPosition,
    pub phase: GroupDamagePhase,
}
impl GroupDamageRecord {
    /// Rule 4 evidence, from Control's applied record.
    pub(crate) fn removal(&self) -> Option<DamagedReplicaRemoved> {
        match &self.phase {
            GroupDamagePhase::Reported => None,
            GroupDamagePhase::Replaced { committed_at, .. } => Some(DamagedReplicaRemoved {
                group: self.report.group.clone(),
                removed: self.report.replica.clone(),
                committed_at: *committed_at,
            }),
        }
    }
}

/// What the replacement driver proves to Control after `replace_voter`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupDamageReplacement {
    pub group: GroupIdentity,
    pub removed: ReplicaMember,
    pub replacement: ReplicaMember,
    pub committed_at: AppliedPosition,
}

/// Mixed-version gate for `GroupDamaged` and `GroupDamageReplaced`. It must
/// return true only when every Control voter and learner decodes them. Without
/// an installed gate both are refused with `not_applied` and nothing is proposed.
pub(crate) trait GroupDamageGate: Send + Sync {
    fn group_damage_supported(&self) -> bool;
}

pub(in crate::oltp_replication::authority::control) fn record_key(
    group_id: Uuid,
    node_id: u64,
) -> Vec<u8> {
    let mut key = machine::key(b"ct/group-damaged/", group_id);
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid group damage report",
        Outcome::NotApplied,
    )
}
