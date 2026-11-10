//! Dispatch all thirteen operations over initialized storage and immutable snapshots.
//! Success values serialize directly as wire data; HTTP owns response envelopes.
//! No table or row cache.
//! The executor must await admitted mutations through completion, including all
//! blocking work, even if the client disconnects. This module owns no runtime.
mod mutation;
mod query;
mod read;
mod write;

pub use mutation::MutationResult;
use serde::Serialize;
use std::sync::Arc;

use crate::{
    commit::Coordinator,
    protocol::{
        self, CreatePayload, ErrorCode, Operation, Outcome, ProtocolError, Row, TableDescriptor,
    },
    snapshot,
    storage::{CatalogEntry, ObjectKey, ObjectStore, Storage, StorageError},
};
use uuid::Uuid;

/// Owned success data without an operation tag or response envelope.
/// Descriptors serve create/describe, descriptors arrays serve list, and Null
/// serves drop. Mutation preserves insert/upsert null, batch count and found bool.
/// Row serves get, Rows collect, Count count, and Explain explain.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ExecuteResult {
    Descriptor(TableDescriptor),
    Descriptors(Vec<TableDescriptor>),
    Null,
    Mutation(MutationResult),
    Row(Option<Row>),
    Rows(Vec<Row>),
    Count(u64),
    Explain(String),
}

/// Clone shares the single global mutation coordinator without requiring B: Clone.
pub struct Engine<B: ObjectStore> {
    coordinator: Arc<Coordinator<B>>,
}

impl<B: ObjectStore> Clone for Engine<B> {
    fn clone(&self) -> Self {
        Self {
            coordinator: Arc::clone(&self.coordinator),
        }
    }
}

impl<B: ObjectStore> Engine<B> {
    /// Storage must have completed startup initialization. Never initializes or
    /// repairs metadata. Construct once, then clone Engine for shared execution.
    pub fn new(storage: Storage<B>) -> Self {
        Self {
            coordinator: Arc::new(Coordinator::new(storage)),
        }
    }

    /// Execute one operation, preserving downstream errors without retry or
    /// reclassification. Callers serialize this data into their bounded envelope.
    /// Admitted mutations must be awaited through completion across disconnects.
    pub async fn execute(&self, operation: Operation) -> Result<ExecuteResult, ProtocolError> {
        match operation {
            Operation::Create(payload) => self.create(payload).await.map(ExecuteResult::Descriptor),
            Operation::Describe(payload) => self
                .describe(payload.name)
                .await
                .map(ExecuteResult::Descriptor),
            Operation::List(_) => self.list().await.map(ExecuteResult::Descriptors),
            Operation::Drop(payload) => {
                self.drop(payload.table_id).await?;
                Ok(ExecuteResult::Null)
            }
            operation @ (Operation::Insert(_)
            | Operation::InsertMany(_)
            | Operation::Update(_)
            | Operation::Upsert(_)
            | Operation::Delete(_)) => self.mutate(operation).await.map(ExecuteResult::Mutation),
            Operation::Get(payload) => self.get(payload).await.map(ExecuteResult::Row),
            Operation::Collect(payload) => self.collect(payload).await.map(ExecuteResult::Rows),
            Operation::Count(payload) => self.count(payload).await.map(ExecuteResult::Count),
            Operation::Explain(payload) => self.explain(payload).await.map(ExecuteResult::Explain),
        }
    }

    /// Shared coordinator for later mutation methods and read-only inspection.
    pub fn coordinator(&self) -> &Arc<Coordinator<B>> {
        &self.coordinator
    }

    /// Unlocked storage reads for health checks and later snapshot consumers.
    /// Mutations must use the coordinator's session API.
    pub fn storage(&self) -> &Storage<B> {
        self.coordinator.storage()
    }

    /// Validate owned protocol input and publish a fresh identity with a closed,
    /// empty Parquet snapshot. Returns only after confirmed catalog publication.
    pub async fn create(&self, payload: CreatePayload) -> Result<TableDescriptor, ProtocolError> {
        let session = self.coordinator.begin().await?;
        let payload = tokio::task::spawn_blocking(move || {
            protocol::validate_schema(&payload.name, &payload.schema, &payload.key)?;
            // TableFormat has only the protocol-supported Parquet variant.
            Ok::<_, ProtocolError>(payload)
        })
        .await
        .map_err(|_| blocking_failure())??;
        if session.catalog().tables.contains_key(&payload.name) {
            return Err(failure(ErrorCode::Conflict, "table name already exists"));
        }
        let table = TableDescriptor {
            id: Uuid::new_v4(),
            name: payload.name,
            schema: payload.schema,
            key: payload.key,
            format: payload.format,
        };
        let encoding_table = table.clone();
        let bytes = tokio::task::spawn_blocking(move || snapshot::encode(&encoding_table, &[]))
            .await
            .map_err(|_| blocking_failure())??;
        let mut candidate = session.candidate();
        candidate.tables.insert(
            table.name.clone(),
            CatalogEntry {
                id: table.id,
                schema: table.schema.clone(),
                key: table.key.clone(),
                format: table.format,
                snapshot: ObjectKey::snapshot(table.id, session.commit_id())
                    .as_str()
                    .into(),
                row_count: 0,
            },
        );
        session.publish(candidate, Some(bytes)).await?;
        Ok(table)
    }

    /// Validate the name and perform exactly one fresh catalog load without
    /// acquiring the mutation lock. The returned descriptor owns its metadata.
    pub async fn describe(&self, name: String) -> Result<TableDescriptor, ProtocolError> {
        protocol::validate_identifier(&name)?;
        let loaded = self
            .storage()
            .load_catalog()
            .await
            .map_err(storage_failure)?;
        loaded
            .catalog
            .tables
            .get(&name)
            .map(|entry| entry.descriptor(&name))
            .ok_or_else(not_found)
    }

    /// One fresh unlocked catalog load, in ascending table-name order.
    pub async fn list(&self) -> Result<Vec<TableDescriptor>, ProtocolError> {
        let loaded = self
            .storage()
            .load_catalog()
            .await
            .map_err(storage_failure)?;
        // Catalog uses a BTreeMap, whose iteration order is name order.
        Ok(loaded
            .catalog
            .tables
            .iter()
            .map(|(name, entry)| entry.descriptor(name))
            .collect())
    }

    /// Remove exactly the immutable ID from a fresh mutation session. Stale IDs
    /// never resolve by name. Old snapshots remain available; no upload or delete.
    pub async fn drop(&self, table_id: Uuid) -> Result<(), ProtocolError> {
        let session = self.coordinator.begin().await?;
        let name = session
            .catalog()
            .tables
            .iter()
            .find(|(_, entry)| entry.id == table_id)
            .map(|(name, _)| name.clone())
            .ok_or_else(not_found)?;
        let mut candidate = session.candidate();
        candidate.tables.remove(&name);
        session.publish(candidate, None).await?;
        Ok(())
    }
}

fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

fn not_found() -> ProtocolError {
    failure(ErrorCode::NotFound, "table not found")
}

fn blocking_failure() -> ProtocolError {
    failure(ErrorCode::InternalError, "table preparation failed")
}

fn storage_failure(error: StorageError) -> ProtocolError {
    match error {
        StorageError::LimitExceeded => {
            failure(ErrorCode::LimitExceeded, "catalog exceeds byte limit")
        }
        _ => failure(ErrorCode::StorageUnavailable, "catalog is unavailable"),
    }
}
