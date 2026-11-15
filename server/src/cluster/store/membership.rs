//! Consumed membership metadata publication; no live authority is established.
use super::*;
use crate::cluster::{controller::OwnershipObservation, membership::MembershipCandidate};

#[must_use]
#[allow(dead_code)] // Pending membership runtime caller.
pub(crate) struct PreparedMembership {
    base: LoadedControl,
    candidate: Control,
    bytes: Vec<u8>,
}
impl std::fmt::Debug for PreparedMembership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedMembership")
    }
}
#[allow(dead_code)] // Pending membership runtime caller.
impl PreparedMembership {
    pub(crate) fn prepare(
        base: LoadedControl,
        candidate: MembershipCandidate,
    ) -> Result<Self, StorageError> {
        let (observation, owner, candidate) = candidate.into_parts();
        if observation != OwnershipObservation::new(&base.root)?
            || base.root.controller.lease_state != LeaseState::Held
            || base.root.controller.owner_id != Some(owner.process_id)
            || base.root.controller.epoch != owner.epoch
            || candidate.controller != base.root.controller
        {
            return Err(StorageError::Conflict);
        }
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
#[cfg(test)]
mod tests;
