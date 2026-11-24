//! Point decision witnesses bind their original three Raft positions to the
//! canonical Status receipt at a fixed pair of independently applied cuts.
use super::*;
use crate::oltp::backup::history::MetadataImage;
use crate::oltp_replication::authority::status::{
    TimestampPointProjection,
    identity::{
        TerminalIdentity,
        transaction::{ArchivedIdentity, ArchivedIdentityDisposition},
    },
};

pub(crate) const POINT_IMAGE: &[u8] = b"ts/point-archive/v1/";
pub(super) const MAX_POINT_IMAGE_BYTES: usize = 16 << 10;
pub(crate) use nabedra_oltp_types::authority::PointArchiveCut;

pub(super) use nabedra_oltp_types::authority::point_at_or_before as at_or_before;

pub(super) fn validate_projection(
    proof: &TimestampPointProjection,
    timestamps: &GroupIdentity,
    status: &GroupIdentity,
    timestamp_at: AppliedPosition,
    status_at: AppliedPosition,
    data_cut: CommitTs,
    statement: &DecisionStatement,
    outcome: &MutationOutcome,
) -> Result<(), ProtocolError> {
    let decision = proof.completion().decision();
    let locator = decision.locator();
    let binding = &locator.binding;
    proof.validate_for(locator)?;
    if &binding.timestamps_group != timestamps
        || &binding.status_group != status
        || binding.txn != statement.txn
        || binding.request != statement.request
        || locator.manifest != statement.manifest
        || proof.projected_at() != statement.decided_at
        || decision.decision() != statement.decision
        || decision.outcome() != outcome
        || !at_or_before(decision.decided_at(), timestamp_at)
        || !at_or_before(proof.projected_at(), status_at)
        || matches!(statement.decision, TxnDecision::Commit { commit_ts } if commit_ts > data_cut)
    {
        return Err(unknown());
    }
    Ok(())
}

pub(crate) fn verify_identity(
    closure: &ArchiveClosureStatement,
    identity: &ArchivedIdentity,
) -> Result<Option<MetadataImage>, ProtocolError> {
    let Some(proof) = &identity.point_projection else {
        return Ok(None);
    };
    let cut = closure.point_history.as_ref().ok_or_else(unknown)?;
    let binding = &proof.completion().decision().locator().binding;
    if identity.origin.is_some() || binding.reservation != identity.reservation {
        return Err(unknown());
    }
    let (statement, outcome) = match &identity.disposition {
        ArchivedIdentityDisposition::PreCutCommitted { decision, outcome } => (decision, outcome),
        ArchivedIdentityDisposition::CurrentFailed {
            terminal:
                TerminalIdentity::Committed {
                    reservation,
                    decision,
                    outcome,
                },
        } if *reservation == identity.reservation
            && matches!(decision.decision, TxnDecision::Abort { .. }) =>
        {
            (decision, outcome)
        }
        _ => return Err(unknown()),
    };
    validate_projection(
        proof,
        &closure.timestamps,
        &closure.status,
        cut.applied,
        closure.status_applied_at,
        closure.pin.read_ts,
        statement,
        outcome,
    )?;
    let image = MetadataImage {
        group: closure.timestamps.clone(),
        key: image_key(proof),
        commit_ts: commit_ts(proof),
        value: serde_json::to_string(proof).map_err(|_| unknown())?,
    };
    if image.value.len() > MAX_POINT_IMAGE_BYTES {
        return Err(unknown());
    }
    Ok(Some(image))
}

pub(super) fn image_key(proof: &TimestampPointProjection) -> Vec<u8> {
    let binding = &proof.completion().decision().locator().binding;
    [
        POINT_IMAGE,
        binding.status_group.group_id.as_bytes(),
        binding.txn.request_id.as_bytes(),
        &binding.txn.attempt.to_be_bytes(),
    ]
    .concat()
}

pub(super) fn commit_ts(proof: &TimestampPointProjection) -> Option<CommitTs> {
    match proof.completion().decision().decision() {
        TxnDecision::Commit { commit_ts } => Some(commit_ts),
        TxnDecision::Abort { .. } => None,
    }
}
