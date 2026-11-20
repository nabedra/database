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
//! NS14b: Control schedules the replacement itself. The scheduler
//! (`manager/data_factory/damage_replacement.rs`) runs beside the Control
//! leader. For a `Reported` record of a Data group it picks a fresh replica
//! identity and placement and proposes `GroupDamageReplacementBegin`, which
//! claims the group, reserves the fresh node ID forever and moves the record to
//! `Replacing { plan }`. The plan is durable, so a new Control leader resumes
//! exactly that plan and never picks a second identity. Once the group's Data
//! leader commits `replace_damaged_voter`, `GroupDamageReplaced` publishes the
//! new [`GroupRecord`] (a new generation without the damaged member) in the
//! same apply, so routes never list the damaged replica again.
//!
//! Mixed versions: an older Control replica cannot decode these commands, so
//! the leader proposes them only behind an installed [`GroupDamageGate`]. An
//! older Control leader rejects the new read request, and the worker retries.
use super::{GroupRecord, machine};
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

/// Why a whole pool was refused at open (rule 6): the kv or identity error
/// class, so an operator can tell storage damage from a bad mount.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PoolRefusal {
    /// kv found a corrupt store-wide record or a bad `CURRENT` (R1, R3).
    StorageCorrupt,
    /// kv could not read the store (R2).
    StorageIo,
    /// The damage spans too many groups (R6).
    DamageLimit,
    /// The directory holds another pool, worker or cluster.
    Identity,
    /// The mount, its registration or its growth claim does not match.
    Mount,
    Other,
}
impl PoolRefusal {
    /// Classifies the error `OltpPool` or the pool inventory returned.
    pub(crate) fn classify(error: &ProtocolError) -> Self {
        let message = error.message.as_str();
        if message.contains("damage spans too many groups") {
            Self::DamageLimit
        } else if message.contains("storage corrupt") {
            Self::StorageCorrupt
        } else if message.contains("storage I/O error") {
            Self::StorageIo
        } else if message.contains("identity") {
            Self::Identity
        } else if message.contains("mount")
            || message.contains("claim")
            || message.contains("registered")
            || message.contains("generation")
        {
            Self::Mount
        } else {
            Self::Other
        }
    }
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
    /// The error class of a refused pool (reason `pool_refused` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<PoolRefusal>,
}
impl GroupDamageReport {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.group.validate().map_err(|_| invalid())?;
        self.replica.validate().map_err(|_| invalid())?;
        if self.refusal.is_some() && self.reason != GroupDamageReason::PoolRefused {
            return Err(invalid());
        }
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
    /// NS14b: Control chose the fresh identity and placement and claimed the
    /// group. Only this plan can complete it.
    Replacing {
        plan: Box<GroupDamageReplacementPlan>,
        claimed_at: AppliedPosition,
    },
    /// The membership change that removed the damaged identity committed.
    Replaced {
        replacement: ReplicaMember,
        committed_at: AppliedPosition,
        /// The placement Control published with it (NS14b).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        published: Option<GroupRecord>,
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
            GroupDamagePhase::Reported | GroupDamagePhase::Replacing { .. } => None,
            GroupDamagePhase::Replaced { committed_at, .. } => Some(DamagedReplicaRemoved {
                group: self.report.group.clone(),
                removed: self.report.replica.clone(),
                committed_at: *committed_at,
            }),
        }
    }
}

impl GroupDamageRecord {
    /// The scheduler still has to act on this record.
    pub(crate) fn pending(&self) -> bool {
        !matches!(self.phase, GroupDamagePhase::Replaced { .. })
    }
    pub(crate) fn plan(&self) -> Option<&GroupDamageReplacementPlan> {
        match &self.phase {
            GroupDamagePhase::Replacing { plan, .. } => Some(plan),
            _ => None,
        }
    }
}

/// At most this many damage replacements run at once in the cluster.
pub(crate) const MAX_ACTIVE_REPLACEMENTS: usize = 2;
/// At most this many run at once that read from or write to one pool.
pub(crate) const MAX_ACTIVE_PER_POOL: usize = 1;

/// NS14b: Control's choice for one damaged replica. `members` is every member
/// of the published placement `expected`, ordered by node ID; the fresh
/// `replacement` takes the damaged member's place.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupDamageReplacementPlan {
    pub group: GroupIdentity,
    pub damaged: ReplicaMember,
    pub replacement: ReplicaMember,
    pub expected: GroupRecord,
    pub members: Vec<ReplicaMember>,
}
impl GroupDamageReplacementPlan {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.group.validate().map_err(|_| invalid())?;
        self.damaged.validate().map_err(|_| invalid())?;
        self.replacement.validate().map_err(|_| invalid())?;
        let survivors = self
            .members
            .iter()
            .filter(|member| **member != self.damaged)
            .collect::<Vec<_>>();
        if !matches!(self.group.kind, GroupKind::Data { .. })
            || self.expected.identity != self.group
            || self.members.len() as u64 != self.expected.replica_count
            || self.members.len() > 128
            || survivors.len() + 1 != self.members.len()
            || self
                .members
                .windows(2)
                .any(|pair| pair[0].node_id >= pair[1].node_id)
            || validate_voters(&self.members).is_err()
            || self
                .members
                .iter()
                .any(|member| member.node_id == self.replacement.node_id)
            || self.replacement.pool_id == self.damaged.pool_id
            || survivors.iter().any(|member| {
                member.worker_id == self.replacement.worker_id
                    || member.pool_id == self.replacement.pool_id
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
    /// The members after the replacement, ordered by node ID.
    pub(crate) fn next_members(&self) -> Vec<ReplicaMember> {
        let mut next = self
            .members
            .iter()
            .filter(|member| **member != self.damaged)
            .cloned()
            .chain(std::iter::once(self.replacement.clone()))
            .collect::<Vec<_>>();
        next.sort_by_key(|member| member.node_id);
        next
    }
    /// Binds the fresh replica's pool scope to this plan (`placement_learner`).
    pub(crate) fn binding(&self) -> Result<Digest, ProtocolError> {
        crate::oltp_replication::owned::serialized_digest(&(
            "nabedra/group-damage-replacement/v1",
            self,
        ))
    }
    /// The generation of the placement published with the replacement. It is
    /// derived from the plan, so every replay publishes the same record.
    pub(crate) fn generation(&self) -> Result<Uuid, ProtocolError> {
        let digest = crate::oltp_replication::owned::serialized_digest(&(
            "nabedra/group-damage-generation/v1",
            self.binding()?,
        ))?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        Ok(uuid::Builder::from_random_bytes(bytes).into_uuid())
    }
}

/// One running damage replacement, for the rate limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveDamageReplacement {
    pub group_id: Uuid,
    pub node_id: u64,
    pub pools: [Uuid; 2],
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

/// The production gate (NS14b), installed like T7's ReserveWindow gate: it
/// opens once Control's `data.direct.v1` capability is active. Activation
/// needs an advert from the current incarnation of every enrolled worker and
/// API, and every Control voter and learner is an enrolled worker, so every
/// Control member then runs a build that decodes `GroupDamaged`,
/// `GroupDamageReplacementBegin` and `GroupDamageReplaced` (they ship in the
/// same release as `data.direct.v1`; a later damage command needs its own
/// capability version). Until then damage stays local: the pool quarantines
/// the group, `/v1/runtime/stats` counts it, and reports are retried. The
/// activation never reverts, so the gate never closes again.
pub(crate) struct DirectCapabilityDamageGate {
    open: std::sync::atomic::AtomicBool,
}
impl DirectCapabilityDamageGate {
    /// Polls Control every `every` until the capability is active.
    pub(crate) fn watch(
        control: std::sync::Arc<super::ControlClient>,
        every: std::time::Duration,
    ) -> std::sync::Arc<Self> {
        let gate = std::sync::Arc::new(Self {
            open: std::sync::atomic::AtomicBool::new(false),
        });
        let weak = std::sync::Arc::downgrade(&gate);
        tokio::spawn(async move {
            loop {
                let active = control
                    .direct_capability()
                    .await
                    .is_ok_and(|capability| capability.active());
                let Some(gate) = weak.upgrade() else {
                    return;
                };
                if active {
                    gate.open.store(true, std::sync::atomic::Ordering::Release);
                    tracing::info!("group damage replacement enabled: data.direct.v1 is active");
                    return;
                }
                drop(gate);
                tokio::time::sleep(every).await;
            }
        });
        gate
    }
}
impl GroupDamageGate for DirectCapabilityDamageGate {
    fn group_damage_supported(&self) -> bool {
        self.open.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// The running replacements (`Vec<ActiveDamageReplacement>`).
pub(in crate::oltp_replication::authority::control) const ACTIVE_KEY: &[u8] =
    b"ct/group-damage-active";
pub(in crate::oltp_replication::authority::control) const RECORD_PREFIX: &[u8] =
    b"ct/group-damaged/";

pub(in crate::oltp_replication::authority::control) fn record_key(
    group_id: Uuid,
    node_id: u64,
) -> Vec<u8> {
    let mut key = machine::key(RECORD_PREFIX, group_id);
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
