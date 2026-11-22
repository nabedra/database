//! Consumed administration publication against one immutable observed root.
use super::{LoadedControl, check_deadline, control_error};
use crate::cluster::{
    admin::admin_candidate,
    admin_history::{AdminHistoryBudget, AdminLookup},
};
use crate::storage::{
    ControlDecision, MAX_CONTROL_DECISION_BYTES, ObjectKey, ObjectStore, Storage, StorageError,
    WriteCondition,
};
use nabedra_rt::time;
use nabedra_rt::time::timeout_at;
use std::future::Future;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AdminPublicationOutcome {
    Terminal(ControlDecision),
    IdentityConflict,
    NeedsRecompute,
    Unknown,
}

/// Caller must authorize and strictly admit, capture a budget before root GET,
/// and prove complete history absence on this same base before preparation.
/// Replay precedes all candidate arithmetic. Runtime supplies a fresh proposal.
#[must_use]
pub(crate) struct PreparedAdmin {
    base: LoadedControl,
    decision: ControlDecision,
    root_bytes: Vec<u8>,
    decision_bytes: Vec<u8>,
}
impl std::fmt::Debug for PreparedAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedAdmin")
            .field("proposal_id", &self.decision.id)
            .field("revision", &self.decision.published_root_revision)
            .finish_non_exhaustive()
    }
}
// Pending G02 caller. No public facade solely for access from tests.
#[allow(dead_code)]
impl PreparedAdmin {
    pub(crate) fn prepare(
        base: LoadedControl,
        request_id: Uuid,
        exact_request: &[u8],
        proposal_id: Uuid,
    ) -> Result<Self, StorageError> {
        let (candidate, decision) =
            admin_candidate(&base.root, request_id, exact_request, proposal_id)?;
        let root_bytes = candidate.encode().map_err(control_error)?;
        let decision_bytes = decision.encode()?;
        Ok(Self {
            base,
            decision,
            root_bytes,
            decision_bytes,
        })
    }

    /// No initial scan, retry or recomputation. An error before root submission
    /// says nothing about earlier same-request work. Cancellation is no rollback
    /// proof. Caller-controlled recovery must reload/search with the same exact
    /// request and a fresh proposal; only a winning CAS fences an old token.
    pub(crate) async fn publish<B: ObjectStore>(
        self,
        storage: &Storage<B>,
        budget: AdminHistoryBudget,
    ) -> Result<AdminPublicationOutcome, StorageError> {
        let key = ObjectKey::control_decision(self.decision.id);
        match bounded(
            budget,
            storage.put(&key, self.decision_bytes.clone(), WriteCondition::Create),
        )
        .await
        {
            Ok(token) if !token.is_empty() => {}
            Ok(_) | Err(StorageError::Conflict | StorageError::Ambiguous) => {
                let object =
                    bounded(budget, storage.read(&key, MAX_CONTROL_DECISION_BYTES)).await?;
                if object.bytes != self.decision_bytes {
                    return Err(StorageError::InvalidCatalog);
                }
            }
            Err(error) => return Err(error),
        }
        check_deadline(budget.deadline())?;
        let result = timeout_at(
            budget.deadline(),
            storage.put(
                &ObjectKey::control(),
                self.root_bytes,
                WriteCondition::Match(&self.base.token),
            ),
        )
        .await;
        let conflict = match result {
            Ok(Ok(token)) if !token.is_empty() && time::now() < budget.deadline() => {
                return Ok(AdminPublicationOutcome::Terminal(self.decision));
            }
            Ok(Err(StorageError::Conflict)) => true,
            Ok(Err(StorageError::Ambiguous)) | Ok(Ok(_)) | Err(_) => false,
            Ok(Err(error)) => return Err(error),
        };
        let recovered = async {
            let root = storage.load_control(budget.deadline()).await?;
            storage
                .lookup_admin(
                    &root,
                    self.decision.request_id,
                    self.decision.request_body.as_bytes(),
                    budget,
                )
                .await
        }
        .await;
        Ok(match recovered {
            Ok(AdminLookup::Found(d)) => AdminPublicationOutcome::Terminal(d),
            Ok(AdminLookup::IdentityConflict) => AdminPublicationOutcome::IdentityConflict,
            Ok(AdminLookup::Absent) if conflict => AdminPublicationOutcome::NeedsRecompute,
            _ => AdminPublicationOutcome::Unknown,
        })
    }
}
async fn bounded<T>(
    budget: AdminHistoryBudget,
    work: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    check_deadline(budget.deadline())?;
    let result = timeout_at(budget.deadline(), work)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    check_deadline(budget.deadline())?;
    result
}
#[cfg(test)]
mod tests;
