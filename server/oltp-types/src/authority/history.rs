//! Committed-history records shared by storage validation and authority services.
use super::{
    IdentityReservation, MutationOutcome, RejectedIdentity, ResolutionEvidence,
    RestoredHistoryOrigin, TerminalIdentity, TimestampDecisionLocator, TimestampPointResolution,
    metadata_digest,
};
use crate::{owned::serialized_digest, types::*};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_resources::memory::ScratchLease;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, fmt, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupOwnerRef {
    pub backup_id: Uuid,
    pub attempt: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveClosureStatement {
    pub timestamps: GroupIdentity,
    pub status: GroupIdentity,
    pub authority: Authority,
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub archive_id: uuid::Uuid,
    pub base_exclusive: Option<CommitTs>,
    pub status_applied_at: AppliedPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_root: Option<Digest>,
    pub committed_count: u64,
    pub decision_root: Digest,
    pub allocation_count: u64,
    pub allocation_root: Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_history: Option<PointArchiveCut>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointArchiveCut {
    pub applied: AppliedPosition,
    pub count: u64,
    pub root: Digest,
}

pub fn point_at_or_before(at: AppliedPosition, cut: AppliedPosition) -> bool {
    at.term != 0
        && at.node_id != 0
        && at.index != 0
        && at.term <= cut.term
        && at.index <= cut.index
        && (at.index != cut.index || at == cut)
}

impl PointArchiveCut {
    pub fn new(applied: AppliedPosition) -> Self {
        Self {
            applied,
            count: 0,
            root: Digest::from_bytes([0; 32]),
        }
    }

    pub fn valid(&self) -> bool {
        point_at_or_before(self.applied, self.applied)
            && self.count != 0
            && self.root != Digest::from_bytes([0; 32])
    }

    pub fn push(&mut self, proof: &TimestampPointProjection) -> Result<(), ProtocolError> {
        self.root = serialized_digest(&(
            b"nabedra/archive-timestamp-point/v1".as_slice(),
            self.root,
            proof,
        ))?;
        self.count = self.count.checked_add(1).ok_or_else(timestamp_unknown)?;
        Ok(())
    }
}

const MAX_STATUS_PARTITIONS: usize = 16;

/// Immutable, bucket-ordered identities. Changing group membership cannot
/// change the home of a request. The algorithm is part of registered format 2.
#[derive(Clone)]
pub struct StatusTopology {
    groups: Arc<[GroupIdentity]>,
}

impl StatusTopology {
    pub fn new(groups: Vec<GroupIdentity>) -> Result<Self, ProtocolError> {
        let first = groups.first().ok_or_else(invalid_status_route)?;
        let mut ids = BTreeSet::new();
        if groups.len() > MAX_STATUS_PARTITIONS
            || groups.iter().enumerate().any(|(index, group)| {
                group.validate().is_err()
                    || group.kind
                        != (GroupKind::Status {
                            bucket: index as u32,
                        })
                    || group.cluster_id != first.cluster_id
                    || group.database != first.database
                    || !ids.insert(group.group_id)
            })
        {
            return Err(invalid_status_route());
        }
        Ok(Self {
            groups: groups.into(),
        })
    }

    #[inline]
    pub fn groups(&self) -> &[GroupIdentity] {
        &self.groups
    }

    #[inline]
    pub fn bucket(&self, request_id: Uuid) -> usize {
        if self.groups.len() == 1 {
            return 0;
        }
        let mut hash = Sha256::new();
        hash.update(b"nabedra/status-routing/v1\0");
        hash.update(request_id.as_bytes());
        let hash = hash.finalize();
        let prefix = u64::from_be_bytes(hash[..8].try_into().expect("SHA-256 prefix"));
        (prefix % self.groups.len() as u64) as usize
    }

    #[inline]
    pub fn request(&self, request_id: Uuid) -> &GroupIdentity {
        &self.groups[self.bucket(request_id)]
    }

    #[inline]
    pub fn manifest(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<&GroupIdentity, ProtocolError> {
        let group = self.request(txn.request_id);
        if group.group_id != manifest.status_group_id {
            return Err(invalid_status_route());
        }
        Ok(group)
    }

    #[inline]
    pub fn contains(&self, group: &GroupIdentity) -> bool {
        match group.kind {
            GroupKind::Status { bucket } => self.groups.get(bucket as usize) == Some(group),
            _ => false,
        }
    }
}

pub fn invalid_status_route() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "request does not belong to the supplied Status topology or group",
        Outcome::Unknown,
    )
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedDecision {
    pub reservation: IdentityReservation,
    pub statement: DecisionStatement,
    pub outcome: MutationOutcome,
    pub read_ts: ReadTs,
    pub settled_at: AppliedPosition,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ArchivedIdentityDisposition {
    PreCutCommitted {
        decision: DecisionStatement,
        outcome: MutationOutcome,
    },
    RestoreExcluded {
        source_decision: DecisionStatement,
        restore_cut: CommitTs,
    },
    /// These immutable failures are idempotency history at status_applied_at,
    /// not a claim that the untimestamped failure already existed at data cut.
    CurrentFailed {
        terminal: TerminalIdentity,
    },
    CurrentRejected {
        rejection: RejectedIdentity,
    },
    Reserved,
    ExpiredRead {
        terminal: TerminalIdentity,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedIdentity {
    #[serde(default)]
    pub origin: Option<RestoredHistoryOrigin>,
    pub reservation: IdentityReservation,
    /// None denotes source records from before reservation-position tracking.
    pub reserved_at: Option<AppliedPosition>,
    pub disposition: ArchivedIdentityDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_projection: Option<Box<TimestampPointProjection>>,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampPointProjection {
    completion: TimestampPointResolution,
    projected_at: AppliedPosition,
}

impl TimestampPointProjection {
    pub fn new(completion: TimestampPointResolution, projected_at: AppliedPosition) -> Self {
        Self {
            completion,
            projected_at,
        }
    }

    pub fn validate_for(&self, locator: &TimestampDecisionLocator) -> Result<(), ProtocolError> {
        self.completion.validate_for(locator)?;
        if !point_at_or_before(self.projected_at, self.projected_at) {
            return Err(status_identity_unknown());
        }
        Ok(())
    }

    pub fn completion(&self) -> &TimestampPointResolution {
        &self.completion
    }

    pub fn projected_at(&self) -> AppliedPosition {
        self.projected_at
    }

    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        metadata_digest(b"nabedra/status-timestamp-point-projection/v1\0", self)
    }

    pub fn statement(&self) -> DecisionStatement {
        let decision = self.completion.decision();
        let home = decision.locator();
        DecisionStatement {
            txn: home.binding.txn,
            request: home.binding.request,
            manifest: home.manifest,
            decided_at: self.projected_at,
            decision: decision.decision(),
        }
    }

    pub fn evidence(&self) -> ResolutionEvidence {
        let decision = self.completion.decision();
        let home = decision.locator();
        ResolutionEvidence {
            group: home.binding.participant.group.clone(),
            txn: home.binding.txn,
            request: home.binding.request,
            manifest: home.manifest,
            decision: decision.decision(),
            completed_at: self.completion.completed_at(),
        }
    }
}

pub fn identity_inventory_seed(
    group: &GroupIdentity,
    applied: AppliedPosition,
) -> Result<Digest, ProtocolError> {
    serialized_digest(&(b"nabedra/status-identities/v1".as_slice(), group, applied))
}

impl ArchivedDecision {
    pub fn new(
        reservation: IdentityReservation,
        statement: DecisionStatement,
        outcome: MutationOutcome,
        read_ts: ReadTs,
        settled_at: AppliedPosition,
    ) -> Self {
        Self {
            reservation,
            statement,
            outcome,
            read_ts,
            settled_at,
            owner: None,
        }
    }

    pub fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        self.outcome.retain_owner(owner.clone());
        self.owner = Some(owner);
    }
}

impl ArchivedIdentity {
    pub fn new(
        origin: Option<RestoredHistoryOrigin>,
        reservation: IdentityReservation,
        reserved_at: Option<AppliedPosition>,
        disposition: ArchivedIdentityDisposition,
        point_projection: Option<Box<TimestampPointProjection>>,
    ) -> Self {
        Self {
            origin,
            reservation,
            reserved_at,
            disposition,
            point_projection,
            owner: None,
        }
    }

    pub fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        match &mut self.disposition {
            ArchivedIdentityDisposition::PreCutCommitted { outcome, .. }
            | ArchivedIdentityDisposition::CurrentFailed {
                terminal:
                    TerminalIdentity::Failed { outcome, .. }
                    | TerminalIdentity::Committed { outcome, .. },
            } => outcome.retain_owner(owner.clone()),
            ArchivedIdentityDisposition::CurrentRejected { rejection } => {
                rejection.failure.retain_owner(owner.clone())
            }
            _ => {}
        }
        self.owner = Some(owner);
    }
}

fn timestamp_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "timestamp durable state is unavailable",
        Outcome::Unknown,
    )
}

fn status_identity_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "durable Status identity cannot be read authoritatively",
        Outcome::Unknown,
    )
}

impl fmt::Debug for ArchivedDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArchivedDecision")
            .field("reservation", &self.reservation)
            .field("statement", &self.statement)
            .field("outcome", &self.outcome)
            .field("read_ts", &self.read_ts)
            .field("settled_at", &self.settled_at)
            .finish()
    }
}

impl fmt::Debug for ArchivedIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArchivedIdentity")
            .field("reservation", &self.reservation)
            .field("reserved_at", &self.reserved_at)
            .field("disposition", &self.disposition)
            .finish()
    }
}
