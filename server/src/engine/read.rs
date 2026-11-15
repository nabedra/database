//! Fresh, unlocked reads of one captured immutable snapshot per operation.
use super::partition::storage_failure;
use super::{Engine, partition, query};
use crate::{
    protocol::{
        ErrorCode, KeyPayload, Operation, Outcome, ProtocolError, QueryPayload, Row,
        TableDescriptor,
    },
    storage::{Catalog, CatalogEntry, ObjectStore},
};
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

impl<B: ObjectStore + 'static> Engine<B> {
    pub async fn get(&self, payload: KeyPayload) -> Result<Option<Row>, ProtocolError> {
        let catalog = self
            .storage()
            .load_catalog()
            .await
            .map_err(storage_failure)?;
        let (table, entry) = resolve_table(&catalog.catalog, payload.table_id)?;
        Operation::Get(payload.clone()).validate_for_table(&table)?;
        let selected = match &entry.data {
            crate::storage::TableData::Legacy { .. } => partition::files(&entry),
            crate::storage::TableData::Partitioned { files } => {
                let routed = entry.route(&payload.key).map_err(storage_failure)?;
                match routed {
                    None => return Ok(None),
                    Some(file) => vec![(
                        files.iter().position(|f| f.object == file.object).unwrap(),
                        partition::ReadFile::Partition(file.clone()),
                    )],
                }
            }
        };
        let coordinator = self.coordinator().clone();
        let mut results =
            partition::map_files(self.worker_pool(), selected, move |file, runtime| {
                let loaded = partition::load_file(runtime, coordinator.storage(), &table, &file)?;
                Ok(loaded
                    .rows
                    .into_iter()
                    .find(|row| partition::row_key(&table, row) == payload.key))
            })
            .await?;
        Ok(results.pop().flatten())
    }

    pub async fn collect(&self, payload: QueryPayload) -> Result<Vec<Row>, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let coordinator = self.coordinator().clone();
        let worker_table = table.clone();
        let worker_query = payload.query.clone();
        let parts = partition::map_files(
            self.worker_pool(),
            partition::files(&entry),
            move |file, runtime| {
                let loaded =
                    partition::load_file(runtime, coordinator.storage(), &worker_table, &file)?;
                Ok((
                    loaded.rows.len(),
                    loaded.payload_bytes,
                    query::local_collect(&worker_table, &worker_query, loaded.rows),
                ))
            },
        )
        .await?;
        check_totals(parts.iter().map(|(rows, bytes, _)| (*rows, *bytes)))?;
        self.worker_pool()
            .submit(0, move |_, _| {
                let rows: Vec<Row> = parts.into_iter().flat_map(|(_, _, rows)| rows).collect();
                query::reduce_collect(&table, &payload.query, &rows)
            })
            .await?
            .receive()
            .await
    }

    pub async fn count(&self, payload: QueryPayload) -> Result<u64, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let coordinator = self.coordinator().clone();
        let query = payload.query.clone();
        let parts = partition::map_files(
            self.worker_pool(),
            partition::files(&entry),
            move |file, runtime| {
                let loaded = partition::load_file(runtime, coordinator.storage(), &table, &file)?;
                Ok((
                    loaded.rows.len(),
                    loaded.payload_bytes,
                    query::local_count(&query, &loaded.rows),
                ))
            },
        )
        .await?;
        check_totals(parts.iter().map(|(rows, bytes, _)| (*rows, *bytes)))?;
        let total = parts.into_iter().try_fold(0u64, |sum, (_, _, count)| {
            sum.checked_add(count)
                .ok_or_else(|| failure(ErrorCode::LimitExceeded, "count exceeds limit"))
        })?;
        Ok(total.min(payload.query.limit.unwrap_or(u64::MAX)))
    }

    /// Metadata only: never downloads or decodes referenced files.
    pub async fn explain(&self, payload: QueryPayload) -> Result<String, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let mut explanation = query::explain(&table, &payload.query)?;
        if let crate::storage::TableData::Partitioned { files } = entry.data {
            explanation.push_str(&format!("\npartitions {}; worker execution", files.len()));
            query::check_body_budget(&explanation)?;
        }
        Ok(explanation)
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
        Operation::Collect(payload.clone()).validate_for_table(&table)?;
        Ok((table, entry, payload))
    }
}

fn check_totals(parts: impl Iterator<Item = (usize, usize)>) -> Result<(), ProtocolError> {
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let limit = || failure(ErrorCode::LimitExceeded, "table exceeds decoded limits");
    for (count, payload) in parts {
        rows = rows.checked_add(count).ok_or_else(limit)?;
        bytes = bytes.checked_add(payload).ok_or_else(limit)?;
        if rows > crate::protocol::MAX_TABLE_ROWS
            || bytes > crate::protocol::MAX_DECODED_PAYLOAD_BYTES
        {
            return Err(limit());
        }
    }
    Ok(())
}

fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
