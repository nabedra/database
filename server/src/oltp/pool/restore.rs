//! Private, synchronous native genesis batches. This path is unavailable to
//! ordinary snapshot RPCs and requires a sealed exclusive empty-target owner.
use super::*;
use crate::oltp::backup::restore::OfflineRestoreTarget;
impl VerifiedPoolMount {
    pub(crate) fn restore_root(
        &self,
        identity: &PoolIdentity,
    ) -> Result<std::path::PathBuf, ProtocolError> {
        self.open_path(identity)?
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(recovery_unknown)
    }
}

impl GroupStore {
    pub(crate) async fn restore_genesis_batch(
        &self,
        target: Arc<OfflineRestoreTarget>,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<(), ProtocolError> + Send + 'static,
    ) -> Result<(), ProtocolError> {
        target.authorize_pool(self.pool().identity(), &self.pool().inner.path)?;
        let group = self.clone();
        self.mutate(
            None,
            None::<()>,
            MutationLane::Restore,
            WriteSync::Synced,
            budget,
            move |tx| {
                target.authorize_pool(group.pool().identity(), &group.pool().inner.path)?;
                // No Raft metadata/log can already refer to this genesis. A valid
                // online install continues to use strict snapshot activation.
                if group
                    .read_applied(tx.read.snapshot, &mut tx.read.scratch)?
                    .is_some()
                    || tx
                        .read
                        .snapshot
                        .get(&group_key(group.identity().group_id, b'm', &[]))
                        .map_err(storage_error)?
                        .is_some()
                {
                    return Err(recovery_unknown());
                }
                let log_prefix = group_key(group.identity().group_id, b'l', &[]);
                let mut log =
                    RawIter::bounded(tx.read.snapshot, &log_prefix, &prefix_end(&log_prefix), true);
                log.seek(&log_prefix);
                if log.key().is_some_and(|key| key.starts_with(&log_prefix)) {
                    return Err(recovery_unknown());
                }
                log.status()?;
                drop(log);
                work(tx)?;
                Ok(Transition::Commit(tx.encode(&())?))
            },
        )
        .await?;
        Ok(())
    }
}

pub(crate) struct VerifiedRestoreFlush {
    identity: PoolIdentity,
}
impl VerifiedRestoreFlush {
    pub(crate) fn matches(&self, identity: &PoolIdentity) -> bool {
        &self.identity == identity
    }
}
impl OltpPool {
    pub(crate) async fn flush_restore(
        &self,
        target: Arc<OfflineRestoreTarget>,
    ) -> Result<VerifiedRestoreFlush, ProtocolError> {
        target.authorize_pool(self.identity(), &self.inner.path)?;
        // Durable before verification: sync the journal, then flush every
        // memtable to runs. `Store::flush` blocks, so it runs off the runtime.
        self.sync_wal().await?;
        let pool = self.clone();
        tokio::task::spawn_blocking(move || {
            target.authorize_pool(pool.identity(), &pool.inner.path)?;
            pool.healthy()?;
            pool.inner.store.flush().map_err(storage_error)?;
            Ok(VerifiedRestoreFlush {
                identity: pool.identity().clone(),
            })
        })
        .await
        .map_err(|_| recovery_unknown())?
    }
}
