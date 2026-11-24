use super::*;
use crate::{
    oltp::pool::{PoolCloseOutcome, TransactionBudget, VerifiedPoolClosed},
    oltp_replication::{
        authority::control::{DrainMount, LocalOperatorPermit, PoolDrainPhase, PoolDrainRecord},
        types::GroupIdentity,
    },
};

const INVENTORY_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 0,
    output_bytes: 64 << 10,
};

#[derive(Deserialize)]
struct LocalGroupPage {
    items: Vec<GroupIdentity>,
    after: Option<Uuid>,
    exhausted: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct PoolDrainRequest {
    pub pool_id: Uuid,
    pub generation: u64,
}

impl WorkerPools {
    #[doc(hidden)]
    pub async fn drain_secondary(
        &self,
        control: &ControlHandle,
        permit: &LocalOperatorPermit,
        id: Uuid,
        request: PoolDrainRequest,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        let _actor = self
            .drain_actor
            .try_lock()
            .map_err(|_| busy("another pool drain is advancing"))?;
        if request.pool_id == self.primary {
            return Err(busy("the primary pool cannot be detached online"));
        }

        let old_freeze = self
            .closing
            .lock()
            .map_err(|_| unavailable("worker pool drain unavailable"))?
            .get(&request.pool_id)
            .copied()
            .filter(|old| *old != id);
        if let Some(old) = old_freeze {
            if control
                .pool_drain(old)
                .await?
                .is_some_and(|record| record.phase == PoolDrainPhase::Cancelled)
            {
                self.unfreeze(request.pool_id, old)?;
            }
        }
        let record = match control.pool_drain(id).await? {
            Some(record) => record,
            None => {
                let entry = self
                    .entry(request.pool_id)?
                    .ok_or_else(|| busy("secondary pool is not open"))?;
                let claim = entry
                    .growth_claim
                    .ok_or_else(|| busy("pool has no completed secondary claim"))?;
                let mount = self.observe_for_drain(entry.pool.clone()).await?;
                if mount.expected.generation != request.generation
                    || mount.expected.worker != self.worker
                    || entry
                        .mount
                        .as_ref()
                        .is_none_or(|expected| !same_open_mount(expected, &mount))
                    || !self.approved(control, &entry, &mount).await?
                {
                    return Err(busy(
                        "secondary pool mount, generation or growth claim differs",
                    ));
                }
                control
                    .begin_pool_drain(permit, id, claim, mount.into())
                    .await?
            }
        };
        if record.mount.expected.worker != self.worker
            || record.mount.expected.identity.pool_id != request.pool_id
            || record.mount.expected.generation != request.generation
        {
            return Err(busy(
                "pool drain does not match the exact original mount generation",
            ));
        }
        if matches!(
            record.phase,
            PoolDrainPhase::Closed | PoolDrainPhase::Cancelled
        ) {
            self.unfreeze(request.pool_id, id)?;
            return Ok(record);
        }

        let revision = match record.phase {
            PoolDrainPhase::Draining => Some(control.prove_empty_pool_placements(&record).await?),
            PoolDrainPhase::Closing => None,
            _ => unreachable!(),
        };
        let Some(mut entry) = self.freeze_and_take(request.pool_id, id)? else {
            if record.phase != PoolDrainPhase::Closing {
                self.unfreeze(request.pool_id, id)?;
                return Err(busy("the exact secondary pool is missing"));
            }
            let mount = record.mount.clone();
            let verified = self
                .shared
                .execute(ExecutionClass::Control, 0, 0, move || {
                    VerifiedPoolClosed::recover_registered(id, &mount)
                })
                .await?;
            return self.publish_closed(control, permit, verified).await;
        };

        let preparation = async {
            if !entry.pool.exclusively_owned() {
                return Err(busy(
                    "pool still has a group, transaction, snapshot or native handle",
                ));
            }
            let observed = self.observe_for_drain(entry.pool.clone()).await?;
            if DrainMount::from(observed) != record.mount {
                return Err(busy("pool mount changed before close"));
            }
            self.verify_retained_groups(control, &entry.pool).await?;
            if let Some(revision) = revision {
                control.prepare_pool_close(permit, id, revision).await?;
            }
            // The Control proposal uses this worker's native Control executor.
            let prepared = self.shared.prepare(ExecutionClass::Control).await?;
            let compute = prepared.reserve_compute(0, 0).await?;
            Ok((prepared, compute))
        }
        .await;
        let (prepared, compute) = match preparation {
            Ok(prepared) => prepared,
            Err(error) => {
                self.restore_frozen(request.pool_id, entry)?;
                // A failed or cancelled CAS may have reached Raft. Keep the
                // local ownership gate until a strong read resolves it.
                if control.pool_drain(id).await?.is_some_and(|current| {
                    matches!(
                        current.phase,
                        PoolDrainPhase::Draining | PoolDrainPhase::Cancelled
                    )
                }) {
                    self.unfreeze(request.pool_id, id)?;
                }
                return Err(error);
            }
        };
        match entry.pool.close_with_native(id, prepared, compute).await? {
            PoolCloseOutcome::Closed(verified) => {
                self.publish_closed(control, permit, verified).await
            }
            PoolCloseOutcome::StillOwned(pool) => {
                entry.pool = pool;
                self.restore_frozen(request.pool_id, entry)?;
                Err(busy("native pool ownership has not settled"))
            }
            PoolCloseOutcome::Failed { pool, error } => {
                entry.pool = pool;
                self.restore_frozen(request.pool_id, entry)?;
                Err(error)
            }
        }
    }

    async fn publish_closed(
        &self,
        control: &ControlHandle,
        permit: &LocalOperatorPermit,
        verified: VerifiedPoolClosed,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let id = verified.statement().close_id;
            crate::oltp::test_hooks::pause("pool_native_closed", id).await;
            if std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|dir| {
                std::fs::remove_file(
                    std::path::PathBuf::from(dir).join(format!("pool-lose-close-{id}")),
                )
                .is_ok()
            }) {
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "native close response was lost",
                    Outcome::Unknown,
                ));
            }
        }
        let record = control.record_pool_closed(permit, verified).await?;
        self.unfreeze(record.mount.expected.identity.pool_id, record.id)?;
        Ok(record)
    }

    async fn verify_retained_groups(
        &self,
        control: &ControlHandle,
        pool: &OltpPool,
    ) -> Result<(), ProtocolError> {
        let mut after = None;
        loop {
            let page = pool.list_groups(after, 32, INVENTORY_BUDGET).await?;
            let page: LocalGroupPage = serde_json::from_slice(page.as_ref())
                .map_err(|_| unavailable("pool group inventory cannot be validated"))?;
            for group in page.items {
                let proof = pool.drain_group_retirement(group).await?;
                if !control.verify_pool_replica_retired(proof).await? {
                    return Err(busy(
                        "pool replica retirement was not acknowledged by Control",
                    ));
                }
            }
            if page.exhausted {
                return Ok(());
            }
            after = Some(
                page.after
                    .ok_or_else(|| unavailable("pool group inventory is incomplete"))?,
            );
        }
    }

    async fn observe_for_drain(
        &self,
        pool: OltpPool,
    ) -> Result<MountedPoolObservation, ProtocolError> {
        self.shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.observe_registered_mount()
            })
            .await
    }

    fn freeze_and_take(&self, pool: Uuid, id: Uuid) -> Result<Option<Entry>, ProtocolError> {
        let _registration = self
            .registration
            .lock()
            .map_err(|_| unavailable("worker pool enrollment unavailable"))?;
        let mut closing = self
            .closing
            .lock()
            .map_err(|_| unavailable("worker pool drain unavailable"))?;
        if closing.get(&pool).is_some_and(|owner| *owner != id) {
            return Err(busy("another drain owns the pool"));
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?;
        closing.insert(pool, id);
        Ok(entries.remove(&pool))
    }

    fn restore_frozen(&self, pool: Uuid, entry: Entry) -> Result<(), ProtocolError> {
        self.entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?
            .insert(pool, entry);
        Ok(())
    }

    fn unfreeze(&self, pool: Uuid, id: Uuid) -> Result<(), ProtocolError> {
        let mut closing = self
            .closing
            .lock()
            .map_err(|_| unavailable("worker pool drain unavailable"))?;
        if closing.get(&pool) == Some(&id) {
            closing.remove(&pool);
        }
        Ok(())
    }
}

fn busy(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}
