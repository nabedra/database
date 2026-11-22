//! Bounded lookup in a captured administration chain. No publication authority.
use super::store::LoadedControl;
use crate::storage::{
    ControlDecision, MAX_CONTROL_BODY_BYTES, MAX_CONTROL_DECISION_BYTES, ObjectKey, ObjectStore,
    Storage, StorageError,
};
use nabedra_rt::time;
use nabedra_rt::time::timeout_at;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

const MAX_RECORDS: usize = 4096;
const MAX_BYTES: usize = 32 * 1024 * 1024;

/// Capture before loading the initial root; pass deadline() to load_control.
/// Copying this budget never refreshes its absolute deadline. Each lookup has
/// its own fixed record/byte allowance, excluding the separately bounded root.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdminHistoryBudget {
    deadline: Instant,
}
// Remove when C04c3/G02 callers land.
#[allow(dead_code)]
impl AdminHistoryBudget {
    pub(crate) fn new(caller_deadline: Instant) -> Result<Self, StorageError> {
        let now = time::now();
        let cap = now
            .checked_add(Duration::from_secs(5))
            .ok_or(StorageError::Unavailable)?;
        let budget = Self {
            deadline: caller_deadline.min(cap),
        };
        budget.check_deadline()?;
        Ok(budget)
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    fn check_deadline(&self) -> Result<(), StorageError> {
        if time::now() >= self.deadline {
            Err(StorageError::Unavailable)
        } else {
            Ok(())
        }
    }
}

/// Debug delegates to the sanitized decision Debug, never original bodies.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AdminLookup {
    Found(ControlDecision),
    IdentityConflict,
    Absent,
}
impl<B: ObjectStore> Storage<B> {
    /// Caller must authorize and strictly admit a scale/autoscale envelope BEFORE
    /// lookup, retaining its original bytes. This only enforces the body cap.
    /// Search only the captured root. A validated matching prefix suffices; only
    /// a validated zero/null terminus proves absence. No write or root reload.
    pub(crate) async fn lookup_admin(
        &self,
        loaded: &LoadedControl,
        request_id: Uuid,
        exact_request: &[u8],
        budget: AdminHistoryBudget,
    ) -> Result<AdminLookup, StorageError> {
        budget.check_deadline()?;
        if exact_request.len() > MAX_CONTROL_BODY_BYTES {
            return Err(StorageError::LimitExceeded);
        }
        let root = loaded.root();
        let mut next = root.admin_head;
        let mut sequence = root.admin_seq;
        let mut revision_ceiling = root.revision;
        let mut records = 0usize;
        let mut bytes = 0usize;
        while let Some(id) = next {
            budget.check_deadline()?;
            records = records.checked_add(1).ok_or(StorageError::LimitExceeded)?;
            if records > MAX_RECORDS {
                return Err(StorageError::LimitExceeded);
            }
            let remaining = MAX_BYTES
                .checked_sub(bytes)
                .ok_or(StorageError::LimitExceeded)?;
            let result = timeout_at(
                budget.deadline(),
                self.read(
                    &ObjectKey::control_decision(id),
                    MAX_CONTROL_DECISION_BYTES.min(remaining),
                ),
            )
            .await;
            budget.check_deadline()?;
            let object = result
                .map_err(|_| StorageError::Unavailable)?
                .map_err(|error| {
                    if error == StorageError::NotFound {
                        StorageError::InvalidCatalog
                    } else {
                        error
                    }
                })?;
            bytes = bytes
                .checked_add(object.bytes.len())
                .ok_or(StorageError::LimitExceeded)?;
            // Decode original bytes directly so duplicate fields remain evidence.
            let decoded = ControlDecision::decode(&object.bytes);
            budget.check_deadline()?;
            let decision = decoded?;
            if decision.id != id
                || decision.cluster_id != root.cluster_id
                || decision.admin_seq != sequence
                || decision.published_root_revision > revision_ceiling
            {
                return Err(StorageError::InvalidCatalog);
            }
            if decision.request_id == request_id {
                return Ok(if decision.request_body.as_bytes() == exact_request {
                    AdminLookup::Found(decision)
                } else {
                    AdminLookup::IdentityConflict
                });
            }
            // Strict sequence decrease also detects cycles, without a visited set.
            sequence = sequence
                .checked_sub(1)
                .ok_or(StorageError::InvalidCatalog)?;
            revision_ceiling = decision.base_root_revision;
            next = decision.parent;
        }
        budget.check_deadline()?;
        if sequence != 0 {
            return Err(StorageError::InvalidCatalog);
        }
        Ok(AdminLookup::Absent)
    }
}
#[cfg(test)]
mod tests;
