//! Status proofs contain immutable identities and decisions, never executable
//! application requests. Only a Status leader's strong read can construct the
//! local capability; its sealed form is carried by fixed Resolve commands.
use crate::{
    oltp_replication::types::{
        AppliedPosition, Authority, CommitTs, DecisionStatement, Digest, ManifestRef, ReadTs,
        RequestBinding, TxnDecision, TxnId,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::authority::{SettlementProof, StatusDecisionProof, metadata_digest};
use serde::{Deserialize, Serialize};

pub(crate) use nabedra_oltp_types::authority::{SealedSettlement, SealedStatusDecision, SettlementStatement};
#[doc(hidden)]
pub use nabedra_oltp_types::authority::MutationOutcome;
#[doc(hidden)]
pub mod identity;
mod routing;
mod service;
pub(crate) use identity::{ArchiveCursor, CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage, StatusArchiveCutStatement, VerifiedStatusArchiveCut};
#[doc(hidden)]
pub use nabedra_oltp_types::authority::ArchivedDecision;
#[allow(unused_imports)]
pub(crate) use identity::ArchivedIdentity;
#[doc(hidden)]
pub use nabedra_oltp_types::authority::ArchivedIdentityDisposition;
pub(crate) use identity::{ManifestDraft, ManifestInventoryPage, RetirementCertificate, ScopedTerminalReceipt, SealedAdmissionRejection};
#[doc(hidden)]
pub use identity::{IdentityLookup, IdentityReservation, RejectedIdentity, RequestKind, ScopedAdmission, StatusCommand, StatusFault, StatusHandle, StatusMachine, StatusReply, TerminalIdentity, TransactionInspection, VerifiedAdmissionRejection, VerifiedTerminalRetirement};
pub(crate) use identity::transaction::timestamp_point::{TimestampDecisionLocator, TimestampPointManifest, projection::TimestampPointProjection};
#[doc(hidden)]
pub use identity::transaction::timestamp_point::{LocatedTimestampManifest, TimestampPointHome};
#[allow(unused_imports)]
#[doc(hidden)]
pub use nabedra_oltp_types::authority::TimestampPointBinding;
#[doc(hidden)]
pub use routing::{PartitionedStatus, StatusTopology};
#[doc(hidden)]
pub use service::{PointManifest, StatusService};

pub(crate) const POINT_PREPARED_PERMIT: u8 = 27;
pub(crate) const POINT_COMMIT_DECISION: u8 = 28;

/// Produced only after the Status leader has strongly read every participant's
/// immutable sealed evidence. A timestamp allocation is not itself COMMIT.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitPermitStatement {
    pub authority: Authority,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub read_ts: ReadTs,
    pub prepared_count: u64,
    /// The immutable last-participant-seal position, not the later read barrier.
    pub prepared_at: AppliedPosition,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct SealedCommitPermit {
    statement: CommitPermitStatement,
    digest: Digest,
}
impl SealedCommitPermit {
    pub(crate) fn statement(&self) -> &CommitPermitStatement {
        &self.statement
    }
    pub(crate) fn digest(&self) -> Digest {
        self.digest
    }
    pub(crate) fn allocation_digest(&self) -> Result<Digest, ProtocolError> {
        // A fresh authorized owner may recover an allocation made by the old
        // owner. Identity remains bound to the original run and sealed plan.
        #[derive(Serialize)]
        struct Binding<'a> {
            txn: TxnId,
            request: &'a RequestBinding,
            manifest: &'a ManifestRef,
            read_ts: ReadTs,
            prepared_count: u64,
            prepared_at: AppliedPosition,
        }
        let s = &self.statement;
        metadata_digest(
            b"nabedra/commit-allocation-binding/v1\0",
            &Binding {
                txn: s.txn,
                request: &s.request,
                manifest: &s.manifest,
                read_ts: s.read_ts,
                prepared_count: s.prepared_count,
                prepared_at: s.prepared_at,
            },
        )
    }
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        let s = &self.statement;
        if s.authority.validate().is_err()
            || s.txn.request_id != s.request.request_id
            || s.request.run_id != s.authority.run_id
            || s.manifest.status_group_id.is_nil()
            || s.manifest.participant_count == 0
            || s.prepared_count != s.manifest.participant_count
            || self.digest != metadata_digest(b"nabedra/sealed-commit-permit/v1\0", s)?
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn from_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: CommitPermitStatement,
    ) -> Result<Self, ProtocolError> {
        context.status(12, &statement)?;
        if context.group().group_id != statement.manifest.status_group_id {
            return Err(invalid());
        }
        Self::from_strong_prepared_read(statement)
    }
    pub(crate) fn from_point_prepared(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: CommitPermitStatement,
        authority: Authority,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Self, ProtocolError> {
        context.status(POINT_PREPARED_PERMIT, &statement)?;
        if context.group().group_id != manifest.status_group_id
            || statement.authority != authority
            || statement.txn != txn
            || statement.manifest != manifest
            || manifest.participant_count != 1
        {
            return Err(invalid());
        }
        Self::from_strong_prepared_read(statement)
    }
    fn from_strong_prepared_read(statement: CommitPermitStatement) -> Result<Self, ProtocolError> {
        let digest = metadata_digest(b"nabedra/sealed-commit-permit/v1\0", &statement)?;
        let permit = Self { statement, digest };
        permit.validate()?;
        Ok(permit)
    }
    #[cfg(test)]
    pub(super) fn for_test(statement: CommitPermitStatement) -> Result<Self, ProtocolError> {
        Self::from_strong_prepared_read(statement)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct CommitStamp {
    pub permit_digest: Digest,
    pub commit_ts: CommitTs,
    pub allocated_at: AppliedPosition,
}
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct VerifiedSettlement(SealedSettlement);
impl VerifiedSettlement {
    pub(crate) fn from_failure_service(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: SettlementStatement,
        txn: TxnId,
        request: RequestBinding,
    ) -> Result<Self, ProtocolError> {
        context.status(16, &statement)?;
        if context.group().group_id != statement.manifest.status_group_id
            || statement.txn != txn
            || statement.request != request
            || statement.manifest.participant_count != 0
        {
            return Err(invalid());
        }
        Self::from_strong_settled_read(statement)
    }
    pub(crate) fn from_authenticated(
        context: crate::oltp_replication::service_rpc::AuthenticatedStatusReplyContext,
        statement: SettlementStatement,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Self, ProtocolError> {
        context.settlement(txn, manifest, &statement)?;
        if statement.txn != txn || statement.manifest != manifest {
            return Err(invalid());
        }
        Self::from_strong_settled_read(statement)
    }
    #[doc(hidden)]
    pub fn into_sealed(self) -> SealedSettlement {
        self.0
    }
    fn from_strong_settled_read(statement: SettlementStatement) -> Result<Self, ProtocolError> {
        Ok(Self(SealedSettlement::seal(statement)?))
    }
    #[cfg(test)]
    pub(crate) fn model_for_test(statement: SettlementStatement) -> Result<Self, ProtocolError> {
        Self::from_strong_settled_read(statement)
    }

    #[doc(hidden)]
    pub fn statement(&self) -> &SettlementStatement {
        self.0.statement()
    }
}

/// Not Deserialize. Parsing a matching wire shape cannot create permission to
/// resolve intents. The leader service must read this exact durable decision
/// after a quorum barrier before building its fixed storage command.
#[derive(Clone)]
#[doc(hidden)]
pub struct VerifiedStatusDecision {
    proof: SealedStatusDecision,
    outcome: Option<MutationOutcome>,
    _owner: Option<std::sync::Arc<crate::oltp::pool::ScratchLease>>,
}
impl std::fmt::Debug for VerifiedStatusDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedStatusDecision")
            .field("statement", self.statement())
            .finish_non_exhaustive()
    }
}
impl VerifiedStatusDecision {
    #[doc(hidden)]
    pub fn from_authenticated(
        context: crate::oltp_replication::service_rpc::AuthenticatedStatusReplyContext,
        statement: DecisionStatement,
        mut outcome: MutationOutcome,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Self, ProtocolError> {
        context.decision(txn, manifest, &Some((&statement, &outcome)))?;
        if statement.txn != txn || statement.manifest != manifest {
            return Err(invalid());
        }
        let owner = context.owner();
        outcome.retain_owner(owner.clone());
        Self::with_outcome(statement, outcome, Some(owner))
    }
    pub(crate) fn from_point_commit(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: DecisionStatement,
        mut outcome: MutationOutcome,
        permit: &CommitPermitStatement,
        stamp: CommitStamp,
    ) -> Result<Self, ProtocolError> {
        context.status(POINT_COMMIT_DECISION, &(&statement, &outcome))?;
        let exact_outcome = match (statement.decision, &outcome) {
            (TxnDecision::Commit { commit_ts }, MutationOutcome::Success(_)) => {
                commit_ts == stamp.commit_ts
            }
            (TxnDecision::Abort { .. }, MutationOutcome::Failure(_)) => true,
            _ => false,
        };
        if context.group().group_id != permit.manifest.status_group_id
            || statement.txn != permit.txn
            || statement.request != permit.request
            || statement.manifest != permit.manifest
            || !exact_outcome
        {
            return Err(invalid());
        }
        let owner = context.owner();
        outcome.retain_owner(owner.clone());
        Self::with_outcome(statement, outcome, Some(owner))
    }
    /// Deterministic storage-adapter model only. This is absent from production
    /// and does not establish authenticated Status authority in a fixture.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn model_for_test(statement: DecisionStatement) -> Result<Self, ProtocolError> {
        Self::from_strong_status_read(statement)
    }
    #[doc(hidden)]
    pub fn statement(&self) -> &DecisionStatement {
        self.proof.statement()
    }
    #[doc(hidden)]
    pub fn into_sealed(self) -> SealedStatusDecision {
        self.proof
    }
    #[doc(hidden)]
    pub fn outcome(&self) -> Option<&MutationOutcome> {
        self.outcome.as_ref()
    }
    fn with_outcome(
        statement: DecisionStatement,
        outcome: MutationOutcome,
        owner: Option<std::sync::Arc<crate::oltp::pool::ScratchLease>>,
    ) -> Result<Self, ProtocolError> {
        let mut verified = Self::from_strong_status_read(statement)?;
        verified.outcome = Some(outcome);
        verified._owner = owner;
        Ok(verified)
    }
    fn from_strong_status_read(statement: DecisionStatement) -> Result<Self, ProtocolError> {
        Ok(Self {
            proof: SealedStatusDecision::seal(statement)?,
            outcome: None,
            _owner: None,
        })
    }
}

impl SettlementProof for VerifiedSettlement {
    fn into_sealed(self) -> SealedSettlement {
        self.into_sealed()
    }
}

impl StatusDecisionProof for VerifiedStatusDecision {
    fn statement(&self) -> &DecisionStatement {
        self.statement()
    }

    fn into_sealed(self) -> SealedStatusDecision {
        self.into_sealed()
    }
}

#[track_caller]
fn invalid() -> ProtocolError {
    nabedra_rt::diag_at("invalid", std::panic::Location::caller());
    ProtocolError::new(
        ErrorCode::ValidationError,
        "status decision proof does not match the immutable transaction",
        Outcome::NotApplied,
    )
}
