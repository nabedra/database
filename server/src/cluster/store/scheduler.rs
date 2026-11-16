//! Conditional scheduler assignment bound to the currently held controller identity.
use super::*;
use crate::cluster::{
    controller::Owner,
    member::{NodeState, Registration},
};
pub(crate) struct PreparedScheduler {
    base: LoadedControl,
    candidate: Control,
    bytes: Vec<u8>,
}
impl PreparedScheduler {
    pub(crate) fn prepare(
        base: LoadedControl,
        owner: Owner,
        coordinator: Option<Uuid>,
        advance_term: bool,
    ) -> Result<Self, StorageError> {
        let root = base.root();
        if root.controller.lease_state != LeaseState::Held
            || root.controller.owner_id != Some(owner.process_id)
            || root.controller.epoch != owner.epoch
            || coordinator.is_some_and(|id| {
                !root.nodes.iter().any(|n| {
                    n.node_id == id
                        && n.state == NodeState::Ready
                        && n.registration == Registration::Verified
                })
            })
        {
            return Err(StorageError::Conflict);
        }
        let mut candidate = root.clone();
        let term = if advance_term || root.scheduler.scheduler_term == 0 {
            root.scheduler
                .scheduler_term
                .checked_add(1)
                .ok_or(StorageError::LimitExceeded)?
        } else {
            root.scheduler.scheduler_term
        };
        // Replacing an assigned coordinator always advances its fencing term.
        if root.scheduler.coordinator_node.is_some()
            && root.scheduler.coordinator_node != coordinator
            && !advance_term
        {
            return Err(StorageError::Conflict);
        }
        candidate.scheduler.coordinator_node = coordinator;
        candidate.scheduler.scheduler_term = term;
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or(StorageError::LimitExceeded)?;
        candidate.change_id = Uuid::new_v4();
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
