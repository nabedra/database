use std::sync::Arc;

use anyhow::Result;
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, storage::RaftStateMachine,
};
use serde_json::Value;

use super::snapshot::{SnapshotFile, Snapshots};
use super::store::Transaction;
use super::{Node, RaftCommand, TypeConfig, storage_error};

const APPLY_BATCH_ENTRIES: usize = 16;

/// Deterministic application of committed commands. Normal domain failures
/// must be represented in the returned value. Errors stop the local Raft group.
pub trait StateMachine: Send + Sync + 'static {
    fn apply(&self, tx: &mut Transaction<'_>, command: Value) -> Result<Value>;
}

pub(super) struct AppliedState {
    pub machine: Arc<dyn StateMachine>,
    pub snapshots: Snapshots,
}

impl RaftStateMachine<TypeConfig> for AppliedState {
    type SnapshotBuilder = Snapshots;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Node>), StorageError<u64>> {
        let store = self.snapshots.store.clone();
        self.snapshots
            .store
            .run(move |_| {
                let header = store.header()?;
                Ok((header.applied, header.membership))
            })
            .await
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Value>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut iter = entries.into_iter();
        let mut results = Vec::new();
        loop {
            // Own one bounded slice of the iterator before crossing into the
            // blocking disk pool. The Raft storage trait permits borrowed iterators.
            let batch: Vec<_> = iter.by_ref().take(APPLY_BATCH_ENTRIES).collect();
            if batch.is_empty() {
                return Ok(results);
            }
            let store = self.snapshots.store.clone();
            let machine = self.machine.clone();
            let replies = self
                .snapshots
                .store
                // Each entry commits on its own; a full device retries only the
                // commit that failed, so no entry is applied twice (RV-EV M1).
                .run_as("apply", move |_| {
                    let mut replies = Vec::with_capacity(batch.len());
                    for entry in batch {
                        let mut tx = store.transaction()?;
                        #[cfg(feature = "sim")]
                        let payload_hash = crate::sim::hooks::payload_hash(&entry.payload);
                        let reply = match entry.payload {
                            EntryPayload::Blank => Value::Null,
                            EntryPayload::Membership(membership) => {
                                tx.header.membership =
                                    StoredMembership::new(Some(entry.log_id), membership);
                                Value::Null
                            }
                            EntryPayload::Normal(RaftCommand::Single(command)) => {
                                machine.apply(&mut tx, command)?
                            }
                            EntryPayload::Normal(RaftCommand::Batch(commands)) => Value::Array(
                                commands
                                    .into_iter()
                                    .map(|command| machine.apply(&mut tx, command))
                                    .collect::<Result<Vec<_>>>()?,
                            ),
                        };
                        tx.header.applied = Some(entry.log_id);
                        tx.commit()?;
                        #[cfg(feature = "sim")]
                        crate::sim::hooks::applied(&store.id, entry.log_id, payload_hash);
                        replies.push(reply);
                    }
                    Ok(replies)
                })
                .await
                .map_err(|error| {
                    storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
                })?;
            results.extend(replies);
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshots.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotFile>, StorageError<u64>> {
        self.snapshots
            .receive()
            .await
            .map(Box::new)
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Node>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<u64>> {
        self.snapshots
            .install(meta.clone(), *snapshot)
            .await
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        self.snapshots
            .current()
            .await
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))
    }
}
