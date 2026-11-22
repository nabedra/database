//! Independent bounded outbound receivers share their operation's spool account.
use super::*;
pub(crate) struct ForkSource {
    slot: Arc<Slot>,
    shared: Arc<Shared>,
}
impl PayloadOwners {
    pub(crate) fn fork_source(&self, source: &PayloadLease) -> Result<ForkSource> {
        if !Arc::ptr_eq(&self.shared, &source.shared) {
            return Err(OwnerError::Invalid);
        }
        Ok(ForkSource {
            slot: source.slot.clone(),
            shared: source.shared.clone(),
        })
    }
    pub(crate) async fn fork_stream(
        &self,
        source: &ForkSource,
        deadline: Instant,
    ) -> Result<PayloadLease> {
        let permit =
            nabedra_rt::time::timeout_at(deadline, self.shared.outgoing.clone().acquire_owned())
                .await
                .map_err(|_| OwnerError::Refused)?
                .map_err(|_| OwnerError::Refused)?;
        let mut prepared = self.prepare_entry(
            Kind::Stream,
            Some(deadline),
            Admission::distributed(DistributedOwner::Stream, Some(Arc::new(permit))),
            false,
        )?;
        let mut state = self.shared.state.lock().unwrap();
        if state.legacy_mode() {
            return Err(OwnerError::Refused);
        }
        let registry = &mut state.payload;
        if registry.closed
            || registry.failed
            || source.slot.cancelled()
            || !Arc::ptr_eq(&source.shared, &self.shared)
        {
            return Err(OwnerError::Refused);
        }
        let parent = registry
            .entries
            .iter()
            .find(|e| Arc::ptr_eq(&e.slot, &source.slot))
            .and_then(|e| e.guards.parent_slot())
            .ok_or(OwnerError::Refused)?;
        if parent.kind != Kind::Operation || parent.cancelled() {
            return Err(OwnerError::Refused);
        }
        let index = registry.prepared_position(&prepared)?;
        let base = 1
            + registry.operation_bound
            + 4096
            + registry.operation_stream_bound
            + registry.dependency_stream_bound;
        let position = registry.free_position(base..base + registry.outgoing_bound)?;
        registry.claim_parent_locked(&parent)?;
        let mut entry = registry.preparations[index].take().unwrap().entry.unwrap();
        entry.position = Some(position);
        *entry.slot.operation_id.lock().unwrap() = *source.slot.operation_id.lock().unwrap();
        entry.guards.set_parent(Some(ParentHold(parent.clone())));
        registry.positions[position] = Some(entry.slot.id);
        registry.entries.push(entry);
        activate_registered(registry, &mut prepared, Initial::Stream, Some(parent));
        Ok(lease_from_prepared(&mut prepared))
    }
}
impl PayloadLease {
    /// Move prepared spool ownership on the bounded worker boundary. No raw file
    /// or spool capability crosses a command/reply or leaves its registered owner.
    pub(crate) async fn transfer_staged_to(&mut self, target: &mut PayloadLease) -> Result<()> {
        if !Arc::ptr_eq(&self.shared, &target.shared)
            || Arc::ptr_eq(&self.slot, &target.slot)
            || *self.slot.operation_id.lock().unwrap() != *target.slot.operation_id.lock().unwrap()
        {
            return Err(OwnerError::Invalid);
        }
        let source = self.slot.clone();
        let target = target.slot.clone();
        let pool = self.shared.pool.clone();
        pool.submit(0, move |_, _| {
            let _change = source.evidence.begin(false).map_err(|_| {
                crate::protocol::ProtocolError::new(
                    crate::protocol::ErrorCode::InternalError,
                    "payload ownership unavailable",
                    crate::protocol::Outcome::Unknown,
                )
            })?;
            let transfer = || -> Result<()> {
                if source.cancelled() || target.cancelled() {
                    return Err(OwnerError::Cancelled);
                }
                let (first, second) = if source.id < target.id {
                    (&source, &target)
                } else {
                    (&target, &source)
                };
                let mut a = first.disk.lock().map_err(|_| OwnerError::Invalid)?;
                let mut b = second.disk.lock().map_err(|_| OwnerError::Invalid)?;
                let (from, to) = if source.id < target.id {
                    (&mut a, &mut b)
                } else {
                    (&mut b, &mut a)
                };
                let from = from.as_mut().ok_or(OwnerError::Invalid)?;
                let to = to.as_mut().ok_or(OwnerError::Invalid)?;
                if to
                    .uploads
                    .len()
                    .checked_add(from.uploads.len())
                    .is_none_or(|n| n > super::super::job::MAX_PREPARATION_ROWS)
                {
                    return Err(OwnerError::Refused);
                }
                to.uploads
                    .try_reserve(from.uploads.len())
                    .map_err(|_| OwnerError::Refused)?;
                to.uploads.append(&mut from.uploads);
                Ok(())
            };
            Ok(transfer())
        })
        .await
        .map_err(|_| OwnerError::FailedDrain)?
        .receive()
        .await
        .map_err(|_| OwnerError::FailedDrain)?
    }
}
