//! Status proofs contain immutable identities and decisions, never executable
//! application requests. Only a Status leader's strong read can construct the
//! local capability; its sealed form is carried by fixed Resolve commands.
use crate::{
    oltp_replication::types::{
        AppliedPosition, Authority, CommitTs, DecisionStatement, Digest, ManifestRef, ReadTs,
        RequestBinding, TxnId,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
pub(crate) mod identity;
mod outcome;
mod routing;
mod service;
pub(crate) use identity::{
    ArchiveCursor, ArchivedDecision, ArchivedIdentity, ArchivedIdentityDisposition,
    CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage, StatusArchiveCutStatement,
    VerifiedStatusArchiveCut,
};
pub(crate) use identity::{
    IdentityLookup, IdentityReservation, ManifestDraft, ManifestInventoryPage, RejectedIdentity,
    RequestKind, SealedAdmissionRejection, StatusCommand, StatusFault, StatusHandle, StatusMachine,
    StatusReply, TerminalIdentity, TransactionInspection, VerifiedAdmissionRejection,
    RetirementCertificate, VerifiedTerminalRetirement,
};
pub(crate) use outcome::MutationOutcome;
pub(crate) use routing::{PartitionedStatus, StatusTopology};
pub(crate) use service::StatusService;

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
pub(crate) struct SealedCommitPermit {
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
pub(crate) struct CommitStamp {
    pub permit_digest: Digest,
    pub commit_ts: CommitTs,
    pub allocated_at: AppliedPosition,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SettlementStatement {
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub decision: DecisionStatement,
    pub resolved_count: u64,
    pub settled_at: AppliedPosition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedSettlement {
    statement: SettlementStatement,
    proof_digest: Digest,
}
impl SealedSettlement {
    pub(crate) fn statement(&self) -> &SettlementStatement {
        &self.statement
    }
    pub(crate) fn validate_for(
        &self,
        txn: TxnId,
        request: &RequestBinding,
        manifest: &ManifestRef,
    ) -> Result<&SettlementStatement, ProtocolError> {
        let s = &self.statement;
        if s.txn != txn
            || &s.request != request
            || &s.manifest != manifest
            || s.decision.txn != txn
            || s.decision.request != *request
            || s.decision.manifest != *manifest
            || s.resolved_count != manifest.participant_count
            || self.proof_digest != metadata_digest(b"nabedra/settled-participants/v1\0", s)?
        {
            return Err(invalid());
        }
        Ok(s)
    }
}
#[derive(Clone, Debug)]
pub(crate) struct VerifiedSettlement(SealedSettlement);
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
    pub(crate) fn into_sealed(self) -> SealedSettlement {
        self.0
    }
    fn from_strong_settled_read(statement: SettlementStatement) -> Result<Self, ProtocolError> {
        let proof_digest = metadata_digest(b"nabedra/settled-participants/v1\0", &statement)?;
        let sealed = SealedSettlement {
            statement,
            proof_digest,
        };
        let s = &sealed.statement;
        sealed.validate_for(s.txn, &s.request, &s.manifest)?;
        Ok(Self(sealed))
    }
    #[cfg(test)]
    pub(crate) fn model_for_test(statement: SettlementStatement) -> Result<Self, ProtocolError> {
        Self::from_strong_settled_read(statement)
    }

    #[cfg(test)]
    pub(crate) fn statement(&self) -> &SettlementStatement {
        self.0.statement()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedStatusDecision {
    statement: DecisionStatement,
    proof_digest: Digest,
}
impl SealedStatusDecision {
    pub(crate) fn validate_for(
        &self,
        txn: TxnId,
        manifest: &ManifestRef,
        request: &RequestBinding,
    ) -> Result<&DecisionStatement, ProtocolError> {
        if self.statement.txn != txn
            || &self.statement.manifest != manifest
            || &self.statement.request != request
            || self.proof_digest != digest(&self.statement)?
        {
            return Err(invalid());
        }
        Ok(&self.statement)
    }
    pub(crate) fn statement(&self) -> &DecisionStatement {
        &self.statement
    }
    pub(crate) fn digest(&self) -> Digest {
        self.proof_digest
    }
}

/// Not Deserialize. Parsing a matching wire shape cannot create permission to
/// resolve intents. The leader service must read this exact durable decision
/// after a quorum barrier before building its fixed storage command.
#[derive(Clone)]
pub(crate) struct VerifiedStatusDecision {
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
    pub(crate) fn from_authenticated(
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
    /// Deterministic storage-adapter model only. This is absent from production
    /// and does not establish authenticated Status authority in a fixture.
    #[cfg(test)]
    pub(crate) fn model_for_test(statement: DecisionStatement) -> Result<Self, ProtocolError> {
        Self::from_strong_status_read(statement)
    }
    pub(crate) fn statement(&self) -> &DecisionStatement {
        &self.proof.statement
    }
    pub(crate) fn into_sealed(self) -> SealedStatusDecision {
        self.proof
    }
    pub(crate) fn outcome(&self) -> Option<&MutationOutcome> {
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
        let proof_digest = digest(&statement)?;
        Ok(Self {
            proof: SealedStatusDecision {
                statement,
                proof_digest,
            },
            outcome: None,
            _owner: None,
        })
    }
}
fn digest(statement: &DecisionStatement) -> Result<Digest, ProtocolError> {
    metadata_digest(b"nabedra/status-decision/v1\0", statement)
}
fn metadata_digest(
    domain: &'static [u8],
    statement: &impl Serialize,
) -> Result<Digest, ProtocolError> {
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    writer.0.update(domain);
    serde_json::to_writer(&mut writer, statement).map_err(|_| invalid())?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "status decision proof does not match the immutable transaction",
        Outcome::NotApplied,
    )
}
