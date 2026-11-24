use crate::types::{
    AppliedPosition, DecisionStatement, Digest, ManifestRef, RequestBinding, TxnId,
};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettlementStatement {
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub decision: DecisionStatement,
    pub resolved_count: u64,
    pub settled_at: AppliedPosition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedSettlement {
    statement: SettlementStatement,
    proof_digest: Digest,
}
impl SealedSettlement {
    pub fn seal(statement: SettlementStatement) -> Result<Self, ProtocolError> {
        let proof_digest = metadata_digest(b"nabedra/settled-participants/v1\0", &statement)?;
        let sealed = Self {
            statement,
            proof_digest,
        };
        let statement = &sealed.statement;
        sealed.validate_for(statement.txn, &statement.request, &statement.manifest)?;
        Ok(sealed)
    }

    pub fn statement(&self) -> &SettlementStatement {
        &self.statement
    }
    pub fn validate_for(
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedStatusDecision {
    statement: DecisionStatement,
    proof_digest: Digest,
}
impl SealedStatusDecision {
    pub fn seal(statement: DecisionStatement) -> Result<Self, ProtocolError> {
        let proof_digest = digest(&statement)?;
        Ok(Self {
            statement,
            proof_digest,
        })
    }

    pub fn validate_for(
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
    pub fn statement(&self) -> &DecisionStatement {
        &self.statement
    }
    pub fn digest(&self) -> Digest {
        self.proof_digest
    }
}

fn digest(statement: &DecisionStatement) -> Result<Digest, ProtocolError> {
    metadata_digest(b"nabedra/status-decision/v1\0", statement)
}
pub fn metadata_digest(
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
pub(super) fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "status decision proof does not match the immutable transaction",
        Outcome::NotApplied,
    )
}

/// Capability to resolve or archive an exact decision read from Status.
pub trait StatusDecisionProof: Send + Sync {
    fn statement(&self) -> &DecisionStatement;
    fn into_sealed(self) -> SealedStatusDecision
    where
        Self: Sized;
}

/// Capability produced by a strong read of the settled Status record.
pub trait SettlementProof {
    fn into_sealed(self) -> SealedSettlement;
}

/// The authenticated participant reply operation, bound to its payload digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantProofKind {
    Seal,
    Resolution,
    Lifecycle,
    Activation,
    IndexClosed,
}
