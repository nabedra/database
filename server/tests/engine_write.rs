use meshdb_server::{engine::Engine, protocol, storage};
#[path = "support/storage.rs"]
mod support;

use meshdb_server::engine::MutationResult::{Count, Found, Null};
use protocol::*;
use std::{future::Future, sync::Arc, task::Poll};
use storage::{Namespace, Storage, StorageError};
use support::{Fake, ReadGate};

fn row(id: i64, text: &str, number: f64) -> Row {
    [
        ("id".into(), Scalar::Int64(id)),
        ("text".into(), Scalar::Utf8(text.into())),
        ("number".into(), Scalar::Float64(number)),
    ]
    .into()
}
async fn fixture() -> (Engine<Fake>, TableDescriptor) {
    let storage = Storage::new(Fake::default(), Namespace::new("write/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let engine = Engine::new(storage);
    let table = engine
        .create(CreatePayload {
            name: "items".into(),
            schema: [
                ("id".into(), ColumnType::Int64),
                ("text".into(), ColumnType::Utf8),
                ("number".into(), ColumnType::Float64),
            ]
            .into(),
            key: "id".into(),
            format: TableFormat::Parquet,
        })
        .await
        .unwrap();
    (engine, table)
}
fn insert(table: &TableDescriptor, row: Row) -> Operation {
    Operation::Insert(RowPayload {
        table_id: table.id,
        row,
    })
}
fn batch(table: &TableDescriptor, rows: Vec<Row>) -> Operation {
    Operation::InsertMany(BatchPayload {
        table_id: table.id,
        rows,
    })
}
fn upsert(table: &TableDescriptor, row: Row) -> Operation {
    Operation::Upsert(RowPayload {
        table_id: table.id,
        row,
    })
}
fn update(table: &TableDescriptor, id: i64, values: Row) -> Operation {
    Operation::Update(UpdatePayload {
        table_id: table.id,
        key: Key::Int64(id),
        values,
    })
}
fn key(table: &TableDescriptor, id: i64) -> KeyPayload {
    KeyPayload {
        table_id: table.id,
        key: Key::Int64(id),
    }
}
fn puts(engine: &Engine<Fake>) -> usize {
    engine.storage().backend().state.lock().unwrap().puts
}
fn objects(engine: &Engine<Fake>) -> std::collections::BTreeMap<String, (Vec<u8>, String)> {
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .clone()
}
async fn persisted(engine: &Engine<Fake>, table: &TableDescriptor) -> Vec<Row> {
    // The real reader reloads and decodes the published snapshot.
    engine
        .collect(QueryPayload {
            table_id: table.id,
            query: Query {
                select: None,
                filter: None,
                order_by: None,
                limit: None,
            },
        })
        .await
        .unwrap()
}
fn error(error: ProtocolError, code: ErrorCode, outcome: Outcome) {
    assert_eq!((error.code, error.outcome), (code, outcome));
}

#[tokio::test]
async fn workflow_persists_rows_and_exact_results() {
    let (engine, table) = fixture().await;
    assert_eq!(
        engine
            .mutate(insert(&table, row(1, "one", 0.0)))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(
        engine
            .mutate(batch(
                &table,
                vec![row(2, "two", 2.0), row(3, "three", 3.0)]
            ))
            .await
            .unwrap(),
        Count(2)
    );
    assert_eq!(
        engine
            .mutate(upsert(&table, row(2, "replacement", 2.0)))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(
        engine
            .mutate(upsert(&table, row(4, "four", 4.0)))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(
        engine
            .mutate(update(
                &table,
                1,
                [("number".into(), Scalar::Float64(-0.0))].into()
            ))
            .await
            .unwrap(),
        Found(true)
    );
    let stored = engine.get(key(&table, 1)).await.unwrap().unwrap();
    let Scalar::Float64(number) = stored["number"] else {
        panic!("float expected")
    };
    assert_eq!(number.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(
        engine
            .mutate(Operation::Delete(key(&table, 3)))
            .await
            .unwrap(),
        Found(true)
    );
    assert_eq!(
        persisted(&engine, &table).await,
        vec![
            row(1, "one", -0.0),
            row(2, "replacement", 2.0),
            row(4, "four", 4.0)
        ]
    );
    let loaded = engine.storage().load_catalog().await.unwrap();
    let entry = &loaded.catalog.tables[&table.name];
    assert_eq!(entry.row_count, 3);
    assert_eq!(
        entry.snapshot,
        storage::ObjectKey::snapshot(table.id, loaded.catalog.commit_id).as_str()
    );
    engine.drop(table.id).await.unwrap();
    let replacement = engine
        .create(CreatePayload {
            name: table.name.clone(),
            schema: table.schema.clone(),
            key: table.key.clone(),
            format: table.format,
        })
        .await
        .unwrap();
    assert_ne!(replacement.id, table.id);
    let before = objects(&engine);
    error(
        engine
            .mutate(insert(&table, row(8, "stale", 8.0)))
            .await
            .unwrap_err(),
        ErrorCode::NotFound,
        Outcome::NotApplied,
    );
    assert_eq!(objects(&engine), before);
}

#[tokio::test]
async fn batches_are_atomic_and_no_ops_skip_all_puts() {
    let (engine, table) = fixture().await;
    engine
        .mutate(insert(&table, row(1, "one", 1.0)))
        .await
        .unwrap();
    let before = objects(&engine);
    let put_count = puts(&engine);
    error(
        engine
            .mutate(batch(
                &table,
                vec![row(2, "two", 2.0), row(1, "late duplicate", 9.0)],
            ))
            .await
            .unwrap_err(),
        ErrorCode::Conflict,
        Outcome::NotApplied,
    );
    let mut invalid = row(3, "bad", 3.0);
    invalid.insert("text".into(), Scalar::Bool(false));
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    error(
        engine
            .mutate(batch(&table, vec![row(2, "two", 2.0), invalid]))
            .await
            .unwrap_err(),
        ErrorCode::ValidationError,
        Outcome::NotApplied,
    );
    assert_eq!(
        engine.storage().backend().state.lock().unwrap().gets,
        gets + 1,
        "invalid input must fail before snapshot GET"
    );
    assert_eq!(
        engine.mutate(batch(&table, vec![])).await.unwrap(),
        Count(0)
    );
    assert_eq!(
        engine.mutate(update(&table, 99, Row::new())).await.unwrap(),
        Found(false)
    );
    assert_eq!(
        engine
            .mutate(Operation::Delete(key(&table, 99)))
            .await
            .unwrap(),
        Found(false)
    );
    assert_eq!(
        engine.mutate(update(&table, 1, Row::new())).await.unwrap(),
        Found(true)
    );
    assert_eq!(
        engine
            .mutate(update(
                &table,
                1,
                [("text".into(), Scalar::Utf8("one".into()))].into()
            ))
            .await
            .unwrap(),
        Found(true)
    );
    assert_eq!(
        engine
            .mutate(upsert(&table, row(1, "one", 1.0)))
            .await
            .unwrap(),
        Null
    );
    error(
        engine
            .mutate(Operation::Drop(TablePayload { table_id: table.id }))
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedOperation,
        Outcome::NotApplied,
    );
    assert_eq!(
        objects(&engine),
        before,
        "catalog bytes, ETag and snapshot references must stay unchanged"
    );
    assert_eq!(puts(&engine), put_count);
}

#[tokio::test]
async fn concurrent_clones_serialize_before_loading_catalog() {
    let (engine, table) = fixture().await;
    let gate = Arc::new(ReadGate::default());
    engine.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
    let first = engine.clone();
    let op = insert(&table, row(1, "first", 1.0));
    let task = tokio::spawn(async move { first.mutate(op).await });
    gate.entered.acquire().await.unwrap().forget();
    let second = engine.clone();
    let mut pending = std::pin::pin!(second.mutate(insert(&table, row(2, "second", 2.0))));
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(engine.storage().backend().state.lock().unwrap().gets, gets);
    gate.release.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap(), Null);
    assert_eq!(pending.await.unwrap(), Null);
    assert_eq!(
        persisted(&engine, &table).await,
        vec![row(1, "first", 1.0), row(2, "second", 2.0)]
    );
}

#[tokio::test]
async fn candidate_limit_precedes_put_and_publication_uncertainty_propagates() {
    let (engine, table) = fixture().await;
    let before = objects(&engine);
    let put_count = puts(&engine);
    let huge = row(1, &"x".repeat(MAX_DECODED_PAYLOAD_BYTES), 1.0);
    error(
        engine.mutate(insert(&table, huge)).await.unwrap_err(),
        ErrorCode::LimitExceeded,
        Outcome::NotApplied,
    );
    assert_eq!(puts(&engine), put_count);
    assert_eq!(objects(&engine), before);
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .put_faults
        .insert(
            "write/catalog.json".into(),
            (StorageError::Ambiguous, false),
        );
    error(
        engine
            .mutate(insert(&table, row(2, "unknown", 2.0)))
            .await
            .unwrap_err(),
        ErrorCode::OutcomeUnknown,
        Outcome::Unknown,
    );
    assert_eq!(puts(&engine), put_count + 2);
    assert_eq!(
        engine.storage().backend().state.lock().unwrap().objects["write/catalog.json"],
        before["write/catalog.json"]
    );
    assert!(persisted(&engine, &table).await.is_empty());
}
