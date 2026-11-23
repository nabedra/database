//! Durable records describing a request's Timestamp decision home.
use super::{
    IdentityReservation, RequestKind, ScopedAdmission, ScopedCatalogRead, ScopedQueuePlacement,
    outcome::{MutationOutcome, MutationValue},
    proofs::{invalid, metadata_digest},
};
use crate::{owned::serialized_digest, types::*};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_resources::memory::ScratchLease;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const SCOPED_EPOCH_PREFIX: &[u8] = b"ts/scoped-retirement-epoch/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedTimestampEpoch {
    pub queue_group_id: Uuid,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
    placement: Digest,
}

impl ScopedTimestampEpoch {
    pub fn from_placement(placement: &ScopedQueuePlacement) -> Result<Self, ProtocolError> {
        placement.validate(&placement.group)?;
        Ok(Self {
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            view: placement.view,
            placement: serialized_digest(&("nabedra/scoped-terminal/placement-v1", placement))?,
        })
    }

    pub fn key(self) -> [u8; SCOPED_EPOCH_PREFIX.len() + 48] {
        let mut key = [0; SCOPED_EPOCH_PREFIX.len() + 48];
        let offset = SCOPED_EPOCH_PREFIX.len();
        key[..offset].copy_from_slice(SCOPED_EPOCH_PREFIX);
        key[offset..offset + 16].copy_from_slice(self.queue_group_id.as_bytes());
        key[offset + 16..offset + 32].copy_from_slice(self.queue_generation.as_bytes());
        key[offset + 32..].copy_from_slice(self.view.run_id.as_bytes());
        key
    }

    pub fn valid(self) -> bool {
        !self.queue_group_id.is_nil()
            && !self.queue_generation.is_nil()
            && self.view.validate().is_ok()
    }

    pub fn admits(self, admission: Option<ScopedAdmission>) -> bool {
        admission.is_some_and(|admission| {
            admission.protocol_version == ScopedAdmission::VERSION
                && admission.queue_group_id == self.queue_group_id
                && admission.queue_generation == self.queue_generation
        })
    }

    pub fn advances(self, old: Self) -> bool {
        self.queue_group_id == old.queue_group_id
            && self.queue_generation == old.queue_generation
            && self.view.run_id == old.view.run_id
            && self.view.owner_term > old.view.owner_term
            && self.view.membership_revision >= old.view.membership_revision
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampPointBinding {
    pub status_group: GroupIdentity,
    pub timestamps_group: GroupIdentity,
    pub participant: ParticipantPlan,
    pub created_authority: Authority,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub pin_identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub result_digest: Digest,
    pub reservation: IdentityReservation,
    pub original_epoch: ScopedTimestampEpoch,
    pub original_scope: ScopedCatalogRead,
    pub catalog_root: Digest,
}

impl TimestampPointBinding {
    pub fn roots(&self) -> Result<(Digest, ManifestRef), ProtocolError> {
        let status = &self.status_group;
        let timestamps = &self.timestamps_group;
        let data = &self.participant.group;
        let scope = self.original_scope;
        if status.validate().is_err()
            || timestamps.validate().is_err()
            || self.participant.validate().is_err()
            || !matches!(status.kind, GroupKind::Status { .. })
            || timestamps.kind != GroupKind::Timestamps
            || !matches!(data.kind, GroupKind::Data { table_id, .. } if table_id == scope.table_id)
            || timestamps.cluster_id != status.cluster_id
            || data.cluster_id != status.cluster_id
            || timestamps.database != status.database
            || data.database != status.database
            || status.group_id == timestamps.group_id
            || status.group_id == data.group_id
            || timestamps.group_id == data.group_id
            || self.participant.write_count != 1
            || self.participant.page_count != 1
            || self.created_authority.validate().is_err()
            || self.created_authority.run_id != self.request.run_id
            || self.txn.request_id.is_nil()
            || self.txn.request_id != self.request.request_id
            || self.txn.attempt != 1
            || self.pin_identity.run_id != self.request.run_id
            || self.pin_identity.request_id != self.request.request_id
            || self.pin_identity.digest != self.request.digest
            || self.pin_identity.nonce.is_nil()
            || self.storage_pin.pin_id.is_nil()
            || self.storage_pin.allocator_epoch == 0
            || self.storage_pin.read_ts != self.participant.read_ts
            || scope.request != self.request
            || scope.queue_group_id != data.group_id
            || scope.queue_generation.is_nil()
            || scope.data_generation.is_nil()
            || scope.queue_view.validate().is_err()
            || scope.queue_view.run_id != self.request.run_id
            || scope.read_ts != self.storage_pin.read_ts
            || !self.matches_reservation(self.reservation)
            || self.original_epoch.queue_group_id != scope.queue_group_id
            || self.original_epoch.queue_generation != scope.queue_generation
            || self.original_epoch.view != scope.queue_view
            || !self.original_epoch.admits(self.reservation.scoped)
        {
            return Err(invalid());
        }
        let initial = metadata_digest(
            b"nabedra/status-scoped-timestamp-point-manifest/v1\0",
            &(
                status,
                timestamps,
                self.created_authority,
                self.txn,
                self.request,
                &self.pin_identity,
                self.storage_pin,
                self.result_digest,
                self.reservation,
                self.original_epoch,
                scope,
                self.catalog_root,
            ),
        )?;
        Ok((
            initial,
            ManifestRef {
                status_group_id: status.group_id,
                digest: manifest_root_step(initial, &self.participant)?,
                participant_count: 1,
            },
        ))
    }

    pub fn matches_scope(&self, mut scope: ScopedCatalogRead) -> bool {
        if scope.queue_view.validate().is_err()
            || scope.data_generation.is_nil()
            || scope.queue_view.run_id != self.original_scope.queue_view.run_id
            || scope.queue_view.owner_term < self.original_scope.queue_view.owner_term
            || scope.queue_view.membership_revision
                < self.original_scope.queue_view.membership_revision
            || (scope.queue_view.owner_term == self.original_scope.queue_view.owner_term
                && (scope.queue_view != self.original_scope.queue_view
                    || scope.data_generation != self.original_scope.data_generation))
        {
            return false;
        }
        // Callers authenticate the replacement placement at its current fence.
        scope.queue_view = self.original_scope.queue_view;
        scope.data_generation = self.original_scope.data_generation;
        scope == self.original_scope
    }

    pub fn accepts_authority(&self, authority: Authority) -> bool {
        authority.run_id == self.created_authority.run_id
            && authority.owner_term >= self.created_authority.owner_term
    }

    pub fn heap_bytes(&self) -> usize {
        self.status_group.database.capacity_bytes()
            + self.timestamps_group.database.capacity_bytes()
            + self.participant.group.database.capacity_bytes()
    }

    pub fn matches_reservation(&self, reservation: IdentityReservation) -> bool {
        reservation == self.reservation
            && reservation.kind == RequestKind::Mutation
            && reservation.binds(self.request)
            && reservation.reservation_id == self.pin_identity.nonce
            && reservation.scoped
                == Some(ScopedAdmission {
                    protocol_version: ScopedAdmission::VERSION,
                    queue_group_id: self.original_scope.queue_group_id,
                    queue_generation: self.original_scope.queue_generation,
                    status_group_id: self.status_group.group_id,
                })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampDecisionLocator {
    pub binding: TimestampPointBinding,
    pub manifest: ManifestRef,
}

impl TimestampDecisionLocator {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.binding.roots()?.1 != self.manifest {
            return Err(invalid());
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        metadata_digest(b"nabedra/timestamp-point-home/v1\0", self)
    }

    pub fn matches_seal(&self, seal: &PreparedParticipant) -> bool {
        self.binding.txn == seal.txn
            && self.manifest == seal.manifest
            && self.binding.participant == seal.participant
            && self.binding.accepts_authority(seal.authority)
            && self.binding.result_digest == seal.result_digest
            && valid_position(seal.sealed_at)
    }

    pub fn heap_bytes(&self) -> usize {
        self.binding.heap_bytes()
    }
}

pub fn valid_outcome(outcome: &MutationOutcome, expected: Digest) -> Result<bool, ProtocolError> {
    Ok(matches!(
        outcome,
        MutationOutcome::Success(MutationValue::Changed(true))
    ) && outcome.digest()? == expected)
}

/// `decided_at` belongs to Timestamp. The later Status projection and Data
/// completion each retain their own first applied positions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointDecision {
    pub locator: TimestampDecisionLocator,
    pub authority: Authority,
    pub decided_at: AppliedPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_epoch: Option<ScopedTimestampEpoch>,
    pub seal_at: Option<AppliedPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_at: Option<AppliedPosition>,
    pub decision: TxnDecision,
    pub outcome: MutationOutcome,
}

impl PointDecision {
    pub fn locator(&self) -> &TimestampDecisionLocator {
        &self.locator
    }

    pub fn decision(&self) -> TxnDecision {
        self.decision
    }

    pub fn decided_at(&self) -> AppliedPosition {
        self.decided_at
    }

    pub fn rejection_at(&self) -> Option<AppliedPosition> {
        self.rejection_at
    }

    pub fn authority(&self) -> Authority {
        self.authority
    }

    pub fn outcome(&self) -> &MutationOutcome {
        &self.outcome
    }

    pub fn verify_locator(&self, expected: &TimestampDecisionLocator) -> Result<(), ProtocolError> {
        if &self.locator != expected || !self.valid_receipt().map_err(|_| unknown())? {
            return Err(unknown());
        }
        Ok(())
    }

    pub fn heap_bytes(&self) -> usize {
        self.locator.heap_bytes() + self.outcome.heap_bytes()
    }

    pub fn resolution_seal(&self) -> Result<AppliedPosition, ProtocolError> {
        self.verify_locator(&self.locator)?;
        self.seal_at.ok_or_else(unknown)
    }

    pub fn valid_receipt(&self) -> Result<bool, ProtocolError> {
        if self.locator.validate().is_err()
            || self.authority.validate().is_err()
            || !valid_position(self.decided_at)
        {
            return Ok(false);
        }
        let binding = &self.locator.binding;
        Ok(
            match (
                self.decision,
                self.seal_at,
                self.rejection_at,
                self.queue_epoch,
            ) {
                (TxnDecision::Commit { commit_ts }, Some(seal_at), None, Some(epoch)) => {
                    binding.accepts_authority(self.authority)
                        && matches_epoch(binding, epoch)
                        && valid_position(seal_at)
                        && commit_ts > binding.storage_pin.read_ts
                        && valid_outcome(&self.outcome, binding.result_digest)?
                }
                (
                    TxnDecision::Abort {
                        reason: AbortReason::AbandonedByFence,
                    },
                    None,
                    None,
                    None,
                ) => {
                    self.authority.owner_term > binding.created_authority.owner_term
                        && self.authority.run_id != binding.request.run_id
                        && self.outcome == abandon_outcome()?
                }
                (
                    TxnDecision::Abort {
                        reason: AbortReason::Conflict,
                    },
                    None,
                    Some(rejected_at),
                    Some(epoch),
                ) => {
                    binding.accepts_authority(self.authority)
                        && matches_epoch(binding, epoch)
                        && valid_position(rejected_at)
                        && self.outcome == conflict_outcome()?
                }
                _ => false,
            },
        )
    }
}

pub fn matches_epoch(binding: &TimestampPointBinding, epoch: ScopedTimestampEpoch) -> bool {
    epoch.valid() && (epoch == binding.original_epoch || epoch.advances(binding.original_epoch))
}

pub fn valid_position(position: AppliedPosition) -> bool {
    position.term != 0 && position.node_id != 0 && position.index != 0
}

pub fn abandon_outcome() -> Result<MutationOutcome, ProtocolError> {
    MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "original execution authority was fenced",
        Outcome::NotApplied,
    ))
}

pub fn conflict_outcome() -> Result<MutationOutcome, ProtocolError> {
    MutationOutcome::failure(ProtocolError::new(
        ErrorCode::Conflict,
        "row version changed since the pinned snapshot",
        Outcome::NotApplied,
    ))
}

pub fn manifest_root_step(
    prior: Digest,
    participant: &ParticipantPlan,
) -> Result<Digest, ProtocolError> {
    metadata_digest(
        b"nabedra/status-manifest-participant/v1\0",
        &(prior, participant),
    )
}

fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "timestamp durable state is unavailable",
        Outcome::Unknown,
    )
}

/// Exact Timestamp decision verified by a strong read with a retained charge.
pub trait TimestampPointDecisionProof {
    fn into_parts(self) -> (PointDecision, Arc<ScratchLease>);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedTimestampPointDecision {
    decision: PointDecision,
    digest: Digest,
}

impl SealedTimestampPointDecision {
    pub fn locator(&self) -> &TimestampDecisionLocator {
        self.decision.locator()
    }

    fn digest(decision: &PointDecision) -> Result<Digest, ProtocolError> {
        metadata_digest(b"nabedra/mvcc-timestamp-point-decision/v1\0", decision).map_err(|_| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid fixed MVCC participant binding",
                Outcome::NotApplied,
            )
        })
    }

    pub fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.decision.heap_bytes()
    }

    pub fn from_verified(
        verified: impl TimestampPointDecisionProof,
    ) -> Result<(Self, Arc<ScratchLease>), ProtocolError> {
        let (decision, owner) = verified.into_parts();
        decision.verify_locator(decision.locator())?;
        let digest = Self::digest(&decision)?;
        Ok((Self { decision, digest }, owner))
    }

    pub fn decision(&self) -> &PointDecision {
        &self.decision
    }

    pub fn proof_digest(&self) -> Digest {
        self.digest
    }

    pub fn validate_for(&self, locator: &TimestampDecisionLocator) -> Result<(), ProtocolError> {
        self.decision.verify_locator(locator)?;
        if Self::digest(&self.decision)? != self.digest {
            return Err(recovery_unknown());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampPointResolution {
    proof: SealedTimestampPointDecision,
    completed_at: AppliedPosition,
}

impl TimestampPointResolution {
    pub fn new(proof: SealedTimestampPointDecision, completed_at: AppliedPosition) -> Self {
        Self {
            proof,
            completed_at,
        }
    }

    pub fn proof(&self) -> &SealedTimestampPointDecision {
        &self.proof
    }

    pub fn decision(&self) -> &PointDecision {
        &self.proof.decision
    }

    pub fn completed_at(&self) -> AppliedPosition {
        self.completed_at
    }

    pub fn heap_bytes(&self) -> usize {
        self.decision().heap_bytes()
    }

    pub fn validate_for(&self, locator: &TimestampDecisionLocator) -> Result<(), ProtocolError> {
        self.proof.validate_for(locator)?;
        let before = match self.decision().decision() {
            TxnDecision::Commit { .. } => Some(self.decision().resolution_seal()?),
            TxnDecision::Abort {
                reason: AbortReason::Conflict,
            } => Some(
                self.decision()
                    .rejection_at()
                    .ok_or_else(recovery_unknown)?,
            ),
            TxnDecision::Abort {
                reason: AbortReason::AbandonedByFence,
            } => None,
            TxnDecision::Abort { .. } => return Err(recovery_unknown()),
        };
        if !valid_position(self.completed_at)
            || before.is_some_and(|before| {
                self.completed_at.index <= before.index || self.completed_at.term < before.term
            })
        {
            return Err(recovery_unknown());
        }
        Ok(())
    }

    pub fn evidence(&self) -> ResolutionEvidence {
        let locator = self.proof.locator();
        let binding = &locator.binding;
        ResolutionEvidence {
            group: binding.participant.group.clone(),
            txn: binding.txn,
            request: binding.request,
            manifest: locator.manifest,
            decision: self.decision().decision(),
            completed_at: self.completed_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionEvidence {
    pub group: GroupIdentity,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub decision: TxnDecision,
    pub completed_at: AppliedPosition,
}

fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}
