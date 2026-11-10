//! Integrated row mutations over the shared coordinator and immutable snapshots.
use super::{Engine, mutation, read};
use crate::{
    protocol::{ErrorCode, Operation, Outcome, ProtocolError},
    snapshot,
    storage::{ObjectKey, ObjectStore},
};

impl<B: ObjectStore> Engine<B> {
    /// Accept only row mutations and return their typed result after confirmed
    /// publication. No-change results require no PUT. The executor must await
    /// admitted work through completion, including blocking jobs, on disconnect.
    pub async fn mutate(
        &self,
        operation: Operation,
    ) -> Result<mutation::MutationResult, ProtocolError> {
        let table_id = match &operation {
            Operation::Insert(p) | Operation::Upsert(p) => p.table_id,
            Operation::InsertMany(p) => p.table_id,
            Operation::Update(p) => p.table_id,
            Operation::Delete(p) => p.table_id,
            _ => {
                return Err(ProtocolError::new(
                    ErrorCode::UnsupportedOperation,
                    "expected a row mutation",
                    Outcome::NotApplied,
                ));
            }
        };
        // begin acquires the global lock before loading the current catalog.
        // Retain this session across every read and sequential blocking job.
        let session = self.coordinator().begin().await?;
        let (table, entry) = read::resolve_table(session.catalog(), table_id)?;
        let (table, operation) = tokio::task::spawn_blocking(move || {
            operation.validate_for_table(&table)?;
            Ok::<_, ProtocolError>((table, operation))
        })
        .await
        .map_err(blocking_failure)??;
        let rows = read::load_rows(self.storage(), table.clone(), entry).await?;
        let name = table.name.clone();
        let (result, encoded) = tokio::task::spawn_blocking(move || {
            let mutation = mutation::apply(&table, &operation, rows)?;
            let encoded = if mutation.changed {
                // encode validates the full candidate's schema, unique typed
                // keys, row limit and 64 MiB payload before building Parquet.
                Some((
                    mutation.rows.len() as u64,
                    snapshot::encode(&table, &mutation.rows)?,
                ))
            } else {
                None
            };
            Ok::<_, ProtocolError>((mutation.result, encoded))
        })
        .await
        .map_err(blocking_failure)??;
        if let Some((row_count, bytes)) = encoded {
            let mut candidate = session.candidate();
            // Resolution established this name in the session's immutable catalog.
            let entry = candidate.tables.get_mut(&name).expect("resolved table");
            entry.row_count = row_count;
            entry.snapshot = ObjectKey::snapshot(table_id, session.commit_id())
                .as_str()
                .into();
            session.publish(candidate, Some(bytes)).await?;
        }
        Ok(result)
    }
}

fn blocking_failure(_: tokio::task::JoinError) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::InternalError,
        "mutation preparation failed",
        Outcome::NotApplied,
    )
}
