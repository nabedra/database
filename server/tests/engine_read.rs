use meshdb_server::{engine::Engine, protocol, snapshot, storage};
#[path = "support/storage.rs"]
mod support;

use protocol::*;
use std::sync::{Arc, Mutex};
use storage::{Download, Namespace, ObjectKey, ObjectStore, Storage, StorageError, WriteCondition};
use support::{Fake, ReadGate};

#[derive(Default)]
struct Backend {
    fake: Fake,
    gate: Mutex<Option<Arc<ReadGate>>>,
    keys: Mutex<Vec<String>>,
}
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.keys.lock().unwrap().push(key.into());
        let gate = if key.ends_with(".parquet") {
            self.gate.lock().unwrap().take()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.fake.put(key, bytes, condition).await
    }
}
fn row(id: i64, text: &str, number: f64, flag: bool) -> Row {
    [
        ("id".into(), Scalar::Int64(id)),
        ("text".into(), Scalar::Utf8(text.into())),
        ("number".into(), Scalar::Float64(number)),
        ("flag".into(), Scalar::Bool(flag)),
    ]
    .into()
}
async fn fixture() -> (Engine<Backend>, TableDescriptor, Vec<Row>) {
    let storage = Storage::new(Backend::default(), Namespace::new("read/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let engine = Engine::new(storage);
    let table = engine
        .create(CreatePayload {
            name: "mixed".into(),
            schema: [
                ("id".into(), ColumnType::Int64),
                ("text".into(), ColumnType::Utf8),
                ("number".into(), ColumnType::Float64),
                ("flag".into(), ColumnType::Bool),
            ]
            .into(),
            key: "id".into(),
            format: TableFormat::Parquet,
        })
        .await
        .unwrap();
    let rows = vec![
        row(3, "three", 2.5, true),
        row(1, "one", 2.5, true),
        row(2, "two", -1.0, false),
    ];
    publish(&engine, &table, &rows).await;
    (engine, table, rows)
}
async fn publish(engine: &Engine<Backend>, table: &TableDescriptor, rows: &[Row]) {
    let session = engine.coordinator().begin().await.unwrap();
    let mut candidate = session.candidate();
    let entry = candidate.tables.get_mut(&table.name).unwrap();
    entry.row_count = rows.len() as u64;
    entry.snapshot = ObjectKey::snapshot(table.id, session.commit_id())
        .as_str()
        .into();
    session
        .publish(candidate, Some(snapshot::encode(table, rows).unwrap()))
        .await
        .unwrap();
}
fn all(table: &TableDescriptor) -> QueryPayload {
    QueryPayload {
        table_id: table.id,
        query: Query {
            select: None,
            filter: None,
            order_by: None,
            limit: None,
        },
    }
}
fn key(table: &TableDescriptor, id: i64) -> KeyPayload {
    KeyPayload {
        table_id: table.id,
        key: Key::Int64(id),
    }
}
fn take_keys(engine: &Engine<Backend>) -> Vec<String> {
    std::mem::take(&mut *engine.storage().backend().keys.lock().unwrap())
}
#[tokio::test]
async fn mixed_get_composed_query_count_and_metadata_explain() {
    let (engine, table, rows) = fixture().await;
    let entry = engine
        .storage()
        .load_catalog()
        .await
        .unwrap()
        .catalog
        .tables["mixed"]
        .clone();
    take_keys(&engine);
    assert_eq!(
        engine.get(key(&table, 1)).await.unwrap(),
        Some(rows[1].clone())
    );
    assert_eq!(
        take_keys(&engine),
        vec![
            "read/catalog.json".to_owned(),
            format!("read/{}", entry.snapshot)
        ]
    );
    assert_eq!(engine.get(key(&table, 99)).await.unwrap(), None);
    let mut payload = all(&table);
    payload.query = serde_json::from_value(serde_json::json!({
        "filter": {"op":"and", "args":[{"op":"eq", "column":"flag", "value":true}, {"op":"gt", "column":"number", "value":0.0}]},
        "order_by":{"column":"number", "desc":true}, "select":["text"], "limit":1
    })).unwrap();
    assert_eq!(
        engine.collect(payload.clone()).await.unwrap(),
        vec![[("text".into(), Scalar::Utf8("one".into()))].into()]
    );
    assert_eq!(engine.count(payload.clone()).await.unwrap(), 1);
    assert_eq!(engine.count(all(&table)).await.unwrap(), 3);
    take_keys(&engine);
    assert_eq!(
        engine.explain(payload.clone()).await.unwrap(),
        format!(
            "scan mixed ({})\nfilter {}\nsort number DESC; tie-breaker id ASC\nlimit 1\nprojection text",
            table.id,
            serde_json::to_string(payload.query.filter.as_ref().unwrap()).unwrap()
        )
    );
    assert_eq!(take_keys(&engine), vec!["read/catalog.json"]);
    assert_eq!(
        engine.collect(all(&table)).await.unwrap(),
        vec![rows[1].clone(), rows[2].clone(), rows[0].clone()]
    );
}
#[tokio::test]
async fn fresh_reads_validate_before_data_and_explain_needs_no_snapshot() {
    let (engine, table, _) = fixture().await;
    publish(&engine, &table, &[row(8, "new", 8.0, false)]).await;
    assert_eq!(engine.count(all(&table)).await.unwrap(), 1);
    assert_eq!(engine.get(key(&table, 1)).await.unwrap(), None);
    let bad_key = KeyPayload {
        table_id: table.id,
        key: Key::Utf8("8".into()),
    };
    take_keys(&engine);
    assert_eq!(
        engine.get(bad_key).await.unwrap_err().code,
        ErrorCode::ValidationError
    );
    let mut bad = all(&table);
    bad.query.select = Some(vec!["missing".into()]);
    assert_eq!(
        engine.collect(bad.clone()).await.unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(
        engine.count(bad.clone()).await.unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(
        engine.explain(bad).await.unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(take_keys(&engine), vec!["read/catalog.json"; 4]);
    let loaded = engine.storage().load_catalog().await.unwrap();
    let snapshot_key = format!("read/{}", loaded.catalog.tables["mixed"].snapshot);
    let object = engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .remove(&snapshot_key)
        .unwrap();
    assert!(engine.explain(all(&table)).await.is_ok());
    assert_eq!(
        engine.get(key(&table, 8)).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        engine.collect(all(&table)).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    assert_eq!(
        engine.count(all(&table)).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
    engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(snapshot_key, (b"corrupt".to_vec(), object.1));
    let error = engine.collect(all(&table)).await.unwrap_err();
    assert_eq!(
        (error.code, error.outcome),
        (ErrorCode::StorageUnavailable, Outcome::NotApplied)
    );
    assert!(!error.message.contains("read/"));
    engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .read_failure = true;
    assert_eq!(
        engine.explain(all(&table)).await.unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
}
#[tokio::test]
async fn captured_snapshot_completes_across_update_drop_and_name_reuse() {
    let (engine, table, rows) = fixture().await;
    let captured = engine
        .storage()
        .load_catalog()
        .await
        .unwrap()
        .catalog
        .tables["mixed"]
        .snapshot
        .clone();
    let gate = Arc::new(ReadGate::default());
    *engine.storage().backend().gate.lock().unwrap() = Some(gate.clone());
    take_keys(&engine);
    let reader = engine.clone();
    let payload = all(&table);
    let task = tokio::spawn(async move { reader.collect(payload).await });
    gate.entered.acquire().await.unwrap().forget();
    assert_eq!(
        take_keys(&engine),
        vec!["read/catalog.json".to_owned(), format!("read/{captured}")]
    );
    publish(&engine, &table, &[row(9, "replacement", 9.0, true)]).await;
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
    take_keys(&engine);
    gate.release.add_permits(1);
    assert_eq!(
        task.await.unwrap().unwrap(),
        vec![rows[1].clone(), rows[2].clone(), rows[0].clone()]
    );
    assert!(take_keys(&engine).is_empty());
    assert_ne!(replacement.id, table.id);
    assert_eq!(
        engine.get(key(&table, 1)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(
        engine.collect(all(&table)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(
        engine.count(all(&table)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(
        engine.explain(all(&table)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    assert_eq!(take_keys(&engine), vec!["read/catalog.json"; 4]);
}
