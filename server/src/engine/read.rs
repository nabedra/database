//! Fresh, unlocked reads of one captured immutable snapshot per operation.
use super::{Engine, query};
use crate::{
    protocol::{
        ErrorCode, Key, KeyPayload, Operation, Outcome, ProtocolError, QueryPayload, Row, Scalar,
        TableDescriptor,
    },
    snapshot,
    storage::{Catalog, CatalogEntry, ObjectStore, Storage, StorageError},
};
use bytes::Bytes;
use uuid::Uuid;

/// The caller supplies a validated catalog. Names never substitute for identity.
pub(super) fn resolve_table(
    catalog: &Catalog,
    id: Uuid,
) -> Result<(TableDescriptor, CatalogEntry), ProtocolError> {
    catalog
        .tables
        .iter()
        .find(|(_, entry)| entry.id == id)
        .map(|(name, entry)| (entry.descriptor(name), entry.clone()))
        .ok_or_else(|| failure(ErrorCode::NotFound, "table not found"))
}

/// Descriptor and entry must match and come from the same validated catalog.
/// Cap the streaming download before buffering, then validate/decode off-thread.
pub(super) async fn load_rows<B: ObjectStore>(
    storage: &Storage<B>,
    table: TableDescriptor,
    entry: CatalogEntry,
) -> Result<Vec<Row>, ProtocolError> {
    let key = entry.snapshot_key().map_err(storage_failure)?;
    let object = storage
        .read(&key, snapshot::MAX_ENCODED_SNAPSHOT_BYTES)
        .await
        .map_err(storage_failure)?;
    tokio::task::spawn_blocking(move || {
        snapshot::decode(&table, Bytes::from(object.bytes), Some(entry.row_count))
    })
    .await
    .map_err(blocking_failure)?
}

impl<B: ObjectStore> Engine<B> {
    pub async fn get(&self, payload: KeyPayload) -> Result<Option<Row>, ProtocolError> {
        let catalog = self
            .storage()
            .load_catalog()
            .await
            .map_err(storage_failure)?;
        let (table, entry) = resolve_table(&catalog.catalog, payload.table_id)?;
        let (table, payload) = tokio::task::spawn_blocking(move || {
            Operation::Get(payload.clone()).validate_for_table(&table)?;
            Ok::<_, ProtocolError>((table, payload))
        })
        .await
        .map_err(blocking_failure)??;
        let key_column = table.key.clone();
        let rows = load_rows(self.storage(), table, entry).await?;
        tokio::task::spawn_blocking(move || {
            rows.into_iter()
                .find(|row| match (&payload.key, &row[&key_column]) {
                    (Key::Utf8(a), Scalar::Utf8(b)) => a == b,
                    (Key::Int64(a), Scalar::Int64(b)) => a == b,
                    _ => false,
                })
        })
        .await
        .map_err(blocking_failure)
    }

    pub async fn collect(&self, payload: QueryPayload) -> Result<Vec<Row>, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let rows = load_rows(self.storage(), table.clone(), entry).await?;
        tokio::task::spawn_blocking(move || query::collect(&table, &payload.query, &rows))
            .await
            .map_err(blocking_failure)?
    }

    pub async fn count(&self, payload: QueryPayload) -> Result<u64, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let rows = load_rows(self.storage(), table.clone(), entry).await?;
        tokio::task::spawn_blocking(move || query::count(&table, &payload.query, &rows))
            .await
            .map_err(blocking_failure)?
    }

    /// Metadata only: never downloads or decodes the referenced snapshot.
    pub async fn explain(&self, payload: QueryPayload) -> Result<String, ProtocolError> {
        let (table, _, payload) = self.read_query_context(payload).await?;
        tokio::task::spawn_blocking(move || query::explain(&table, &payload.query))
            .await
            .map_err(blocking_failure)?
    }

    async fn read_query_context(
        &self,
        payload: QueryPayload,
    ) -> Result<(TableDescriptor, CatalogEntry, QueryPayload), ProtocolError> {
        let catalog = self
            .storage()
            .load_catalog()
            .await
            .map_err(storage_failure)?;
        let (table, entry) = resolve_table(&catalog.catalog, payload.table_id)?;
        tokio::task::spawn_blocking(move || {
            // Collect/count/explain share the protocol's query validation rules.
            let operation = Operation::Collect(payload);
            operation.validate_for_table(&table)?;
            let Operation::Collect(payload) = operation else {
                unreachable!()
            };
            Ok((table, entry, payload))
        })
        .await
        .map_err(blocking_failure)?
    }
}

fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn storage_failure(error: StorageError) -> ProtocolError {
    match error {
        StorageError::LimitExceeded => {
            failure(ErrorCode::LimitExceeded, "stored object exceeds byte limit")
        }
        _ => failure(ErrorCode::StorageUnavailable, "stored data is unavailable"),
    }
}
fn blocking_failure(_: tokio::task::JoinError) -> ProtocolError {
    failure(ErrorCode::InternalError, "read processing failed")
}
