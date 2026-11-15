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
        let worker_table = table.clone();
        let mut results = partition::map_prepared(
            self.worker_pool(),
            self.storage(),
            table,
            selected,
            |file| Some(file.clone()),
            move |_, loaded| {
                let row = loaded
                    .rows
                    .into_iter()
                    .find(|row| partition::row_key(&worker_table, row) == payload.key);
                let retained = row.as_ref().map_or(0, |row| {
                    crate::snapshot::memory_shape(
                        &worker_table,
                        1,
                        crate::snapshot::row_payload(row),
                        0,
                    )
                    .unwrap_or(u64::MAX)
                });
                Ok((row, retained))
            },
        )
        .await?;
        Ok(results.pop().and_then(|buffered| buffered.value))
    }

    pub async fn collect(&self, payload: QueryPayload) -> Result<Vec<Row>, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let worker_table = table.clone();
        let worker_query = payload.query.clone();
        let parts = partition::map_prepared(
            self.worker_pool(),
            self.storage(),
            table.clone(),
            partition::files(&entry),
            |file| Some(file.clone()),
            move |_, loaded| {
                let count = loaded.rows.len();
                let payload = loaded.payload_bytes;
                let rows = query::local_collect(&worker_table, &worker_query, loaded.rows);
                let retained = snapshot_retained(&worker_table, &rows)?;
                Ok(((count, payload, rows), retained))
            },
        )
        .await?;
        check_totals(parts.iter().map(|part| (part.value.0, part.value.1)))?;
        if parts.is_empty() {
            return query::reduce_collect(&table, &payload.query, &[]);
        }
        let mut charges = Vec::new();
        let mut parts_rows = Vec::new();
        for part in parts {
            charges.push(part.charge);
            parts_rows.push(part.value.2);
        }
        let (result, _charge) = self
            .worker_pool()
            .submit_buffered(
                self.worker_pool().request_id(),
                1,
                charges,
                self.worker_pool().resources().task_memory_limit_bytes,
                move |_, _| {
                    let count = parts_rows.iter().try_fold(0usize, |sum, rows| {
                        sum.checked_add(rows.len())
                            .ok_or_else(|| failure(ErrorCode::LimitExceeded, "reduction row count"))
                    })?;
                    let mut rows = Vec::new();
                    rows.try_reserve_exact(count).map_err(|_| {
                        failure(ErrorCode::LimitExceeded, "reduction allocation limit")
                    })?;
                    for part in parts_rows {
                        rows.extend(part);
                    }
                    query::reduce_collect(&table, &payload.query, &rows)
                },
            )
            .await?
            .receive()
            .await?;
        Ok(result)
    }

    pub async fn count(&self, payload: QueryPayload) -> Result<u64, ProtocolError> {
        let (table, entry, payload) = self.read_query_context(payload).await?;
        let query = payload.query.clone();
        let parts = partition::map_prepared(
            self.worker_pool(),
            self.storage(),
            table,
            partition::files(&entry),
            |file| Some(file.clone()),
            move |_, loaded| {
                Ok((
                    (
                        loaded.rows.len(),
                        loaded.payload_bytes,
                        query::local_count(&query, &loaded.rows),
                    ),
                    64,
                ))
            },
        )
        .await?;
        check_totals(parts.iter().map(|part| (part.value.0, part.value.1)))?;
        let total = parts.into_iter().try_fold(0u64, |sum, part| {
            sum.checked_add(part.value.2)
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

fn snapshot_retained(table: &TableDescriptor, rows: &[Row]) -> Result<u64, ProtocolError> {
    let payload = rows.iter().try_fold(0usize, |sum, row| {
        sum.checked_add(crate::snapshot::row_payload(row))
            .ok_or_else(|| failure(ErrorCode::LimitExceeded, "retained rows limit"))
    })?;
    crate::snapshot::memory_shape(table, rows.len(), payload, 0)
}
