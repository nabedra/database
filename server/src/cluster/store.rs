//! Bounded control-root observation and one-attempt initial creation.
//! Loaded records establish no attachment compatibility or controller authority.
use super::state::{Control, ControlError, LeaseState, MAX_CONTROL_BYTES, PolicyMode};
use crate::storage::{ObjectKey, ObjectStore, Storage, StorageError, WriteCondition};
use tokio::time::{Instant, timeout_at};

/// One validated root paired with the nonempty version of that representation.
/// Neither the root nor its opaque backend token can be mutated by callers.
#[derive(Clone, PartialEq, Eq)]
pub struct LoadedControl {
    root: Control,
    token: String,
}
impl std::fmt::Debug for LoadedControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedControl")
            .field("revision", &self.root.revision)
            .field("change_id", &self.root.change_id)
            .finish_non_exhaustive()
    }
}
impl LoadedControl {
    pub fn root(&self) -> &Control {
        &self.root
    }
}
fn control_error(error: ControlError) -> StorageError {
    match error {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}
fn check_deadline(deadline: Instant) -> Result<(), StorageError> {
    if Instant::now() >= deadline {
        Err(StorageError::Unavailable)
    } else {
        Ok(())
    }
}
fn encode_initial(initial: &Control) -> Result<Vec<u8>, StorageError> {
    // encode includes Control::validate's structural and retained-size checks.
    let bytes = initial.encode().map_err(control_error)?;
    if initial.revision != 1
        || initial.policy.revision != 1
        || initial.admin_seq != 0
        || initial.admin_head.is_some()
        || initial.controller.epoch != 0
        || initial.controller.lease_state != LeaseState::Released
        || initial.controller.owner_id.is_some()
        || initial.controller.lease_until_ms.is_some()
        || !initial.nodes.is_empty()
        || !initial.intents.is_empty()
        || initial.scheduler.cooldown_until_ms != 0
        || initial.scheduler.backoff_until_ms != 0
        || initial.scheduler.failures != 0
        || initial.scheduler.low_since_ms.is_some()
        || (initial.policy.mode == PolicyMode::Autoscale
            && initial.policy.desired != initial.policy.min_nodes)
    {
        return Err(StorageError::InvalidCatalog);
    }
    Ok(bytes)
}
impl<B: ObjectStore> Storage<B> {
    /// One complete GET at the typed control key, bounded to 32 MiB through EOF.
    /// The original absolute deadline covers GET, body and decode/validation.
    /// Retained roots need not satisfy admission-only reservation inequalities.
    pub async fn load_control(&self, deadline: Instant) -> Result<LoadedControl, StorageError> {
        check_deadline(deadline)?;
        let object = timeout_at(
            deadline,
            self.read(&ObjectKey::control(), MAX_CONTROL_BYTES),
        )
        .await
        .map_err(|_| StorageError::Unavailable)?;
        check_deadline(deadline)?;
        let object = object?;
        let root = Control::decode(&object.bytes).map_err(control_error);
        check_deadline(deadline)?;
        let root = root?;
        if object.etag.is_empty() {
            return Err(StorageError::Unavailable);
        }
        Ok(LoadedControl {
            root,
            token: object.etag,
        })
    }

    /// Validate an initial candidate before I/O, observe first, and Create once
    /// only after definite absence. Callers supply fresh cluster/change UUIDs.
    ///
    /// An existing or reconciled root is returned unchanged, even if unrelated
    /// to the candidate. This does not prove this Create won. Before using it,
    /// startup MUST verify cluster identity/name and semantic loaded configuration
    /// including credentials/trust and backend identity. Raw template/path equality
    /// is not an attachment check. No outgoing-template paths are opened here.
    ///
    /// A possible write is never resubmitted. Cancellation supplies no permission
    /// to retry, and backend adapters must prohibit automatic HTTP PUT retries.
    pub async fn initialize_control(
        &self,
        initial: &Control,
        deadline: Instant,
    ) -> Result<LoadedControl, StorageError> {
        let bytes = encode_initial(initial)?;
        match self.load_control(deadline).await {
            Ok(loaded) => return Ok(loaded),
            Err(StorageError::NotFound) => {}
            Err(error) => return Err(error),
        }
        check_deadline(deadline)?;
        let result = timeout_at(
            deadline,
            self.put(&ObjectKey::control(), bytes, WriteCondition::Create),
        )
        .await;
        let error = match result {
            Ok(Ok(token)) if !token.is_empty() && Instant::now() < deadline => {
                return Ok(LoadedControl {
                    root: initial.clone(),
                    token,
                });
            }
            Ok(Err(StorageError::Conflict)) => StorageError::Conflict,
            Ok(Err(StorageError::Ambiguous)) | Ok(Ok(_)) | Err(_) => StorageError::Ambiguous,
            Ok(Err(error)) => return Err(error),
        };
        // One fresh observation, with no new deadline and no replacement write.
        match self.load_control(deadline).await {
            Ok(loaded) => Ok(loaded),
            Err(_) if error == StorageError::Ambiguous => Err(StorageError::Ambiguous),
            Err(error) => Err(error),
        }
    }
}

use super::controller::{OwnershipChange, ownership_candidate};
use uuid::Uuid;

/// One encoded ownership proposal bound to its exact observed base and token.
/// Consuming submission never recomputes or resends it. Drop after polling
/// submission cannot establish rollback. Caller and future guard must share
/// the same fresh proposal UUID. This value grants no live authority.
#[must_use]
pub(crate) struct PreparedOwnership {
    base: LoadedControl,
    candidate: Control,
    bytes: Vec<u8>,
}
impl std::fmt::Debug for PreparedOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedOwnership")
            .field("revision", &self.candidate.revision)
            .field("change_id", &self.candidate.change_id)
            .finish_non_exhaustive()
    }
}
/// Only Acknowledged is eligible for later guard admission checks.
/// ObservedExact confirms metadata, never live controller certainty.
#[derive(Debug)]
pub(crate) enum OwnershipOutcome {
    Acknowledged(LoadedControl),
    ObservedExact(LoadedControl),
    Conflict { current: Option<LoadedControl> },
    Unknown { current: Option<LoadedControl> },
}
// Remove the remaining impl allowance when runtime calls submit.
#[allow(dead_code)]
impl PreparedOwnership {
    pub(crate) fn prepare(
        base: LoadedControl,
        change: OwnershipChange,
        unix_ms: u64,
        proposal_id: Uuid,
    ) -> Result<Self, StorageError> {
        let candidate = ownership_candidate(&base.root, change, unix_ms, proposal_id)?;
        let bytes = candidate.encode().map_err(control_error)?;
        Ok(Self {
            base,
            candidate,
            bytes,
        })
    }

    pub(crate) async fn submit<B: ObjectStore>(
        self,
        storage: &Storage<B>,
        deadline: Instant,
    ) -> Result<OwnershipOutcome, StorageError> {
        submit_control(self.base, self.candidate, self.bytes, storage, deadline).await
    }
}
/// One captured-token Match and at most one reconciliation under the original deadline.
/// The caller supplies a validated, bounded proposal. Outcomes grant no authority.
async fn submit_control<B: ObjectStore>(
    base: LoadedControl,
    candidate: Control,
    bytes: Vec<u8>,
    storage: &Storage<B>,
    deadline: Instant,
) -> Result<OwnershipOutcome, StorageError> {
    check_deadline(deadline)?;
    let result = timeout_at(
        deadline,
        storage.put(
            &ObjectKey::control(),
            bytes,
            WriteCondition::Match(&base.token),
        ),
    )
    .await;
    let conflict = match result {
        Ok(Ok(token)) if !token.is_empty() && Instant::now() < deadline => {
            return Ok(OwnershipOutcome::Acknowledged(LoadedControl {
                root: candidate,
                token,
            }));
        }
        Ok(Err(StorageError::Conflict)) => true,
        Ok(Err(StorageError::Ambiguous)) | Ok(Ok(_)) | Err(_) => false,
        Ok(Err(error)) => return Err(error),
    };
    // Exactly one fresh read under the original budget. No reachable
    // ownership history exists; later state cannot prove this proposal won.
    let current = storage.load_control(deadline).await.ok();
    if conflict {
        return Ok(OwnershipOutcome::Conflict { current });
    }
    if let Some(loaded) = current {
        if loaded.root.change_id == candidate.change_id && loaded.root == candidate {
            return Ok(OwnershipOutcome::ObservedExact(loaded));
        }
        // Equal UUID with unequal contents is an integrity failure. Keep
        // uncertainty after a possible write, including the validated root.
        return Ok(OwnershipOutcome::Unknown {
            current: Some(loaded),
        });
    }
    Ok(OwnershipOutcome::Unknown { current: None })
}
#[cfg(test)]
mod tests;

mod admin;
// Pending G02 caller.
#[allow(unused_imports)]
pub(crate) use admin::{AdminPublicationOutcome, PreparedAdmin};

mod membership;
// Pending membership runtime caller.
#[allow(unused_imports)]
pub(crate) use membership::PreparedMembership;

mod provision;
// Pending managed provisioning runtime caller.
#[allow(unused_imports)]
pub(crate) use provision::PreparedProvision;

mod drain;
// Pending drain reconciliation runtime caller.
#[allow(unused_imports)]
pub(crate) use drain::PreparedDrain;

mod scheduler;
pub(crate) use scheduler::PreparedScheduler;
