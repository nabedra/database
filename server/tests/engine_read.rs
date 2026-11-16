use nabedra_database_server::{engine::Engine, protocol, snapshot, storage};
use nabedra_database_server::{
    partition_size::PartitionSize,
    worker_pool::{WorkerPool, WorkerThreads},
};
use std::num::NonZeroUsize;
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
    partition_gates: Mutex<std::collections::BTreeMap<String, Arc<ReadGate>>>,
    threads: Mutex<Vec<(String, std::thread::ThreadId, String)>>,
}
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.keys.lock().unwrap().push(key.into());
        if key.ends_with(".parquet") {
            self.threads.lock().unwrap().push((
                key.into(),
                std::thread::current().id(),
                std::thread::current().name().unwrap_or("").into(),
            ));
        }
        let partition_gate = self.partition_gates.lock().unwrap().remove(key);
        if let Some(gate) = partition_gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
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
async fn fixture() -> (Engine<Backend>, TableDescriptor, Vec<Row>, WorkerThreads) {
    let storage = Storage::new(Backend::default(), Namespace::new("read/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let table = TableDescriptor {
        id: uuid::Uuid::new_v4(),
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
    };
    let rows = vec![
        row(3, "three", 2.5, true),
        row(1, "one", 2.5, true),
        row(2, "two", -1.0, false),
    ];
    publish(&engine, &table, &rows).await;
    (engine, table, rows, workers)
}
async fn publish(engine: &Engine<Backend>, table: &TableDescriptor, rows: &[Row]) {
    let session = engine.coordinator().begin().await.unwrap();
    let mut candidate = session.candidate();
    candidate.tables.insert(
        table.name.clone(),
        storage::CatalogEntry {
            id: table.id,
            schema: table.schema.clone(),
            key: table.key.clone(),
            format: table.format,
            row_count: rows.len() as u64,
            data: storage::TableData::Legacy {
                snapshot: ObjectKey::snapshot(table.id, session.commit_id())
                    .as_str()
                    .into(),
            },
        },
    );
    session
        .publish(candidate, Some(snapshot::encode(table, rows).unwrap()))
        .await
        .unwrap();
}
async fn publish_partitioned(engine: &Engine<Backend>, table: &TableDescriptor, rows: &[Row]) {
    use nabedra_database_server::{
        commit::PendingObject,
        storage::{KeyRange, PartitionFile, TableData},
    };
    let session = engine.coordinator().begin().await.unwrap();
    let mut candidate = session.candidate();
    let mut files = Vec::new();
    let mut uploads = Vec::new();
    for (index, (part, range)) in [
        (
            rows.iter()
                .filter(|r| matches!(r["id"], Scalar::Int64(n) if n < 2))
                .cloned()
                .collect::<Vec<_>>(),
            KeyRange {
                lower: None,
                upper: Some(Key::Int64(2)),
            },
        ),
        (
            rows.iter()
                .filter(|r| matches!(r["id"], Scalar::Int64(n) if n >= 2))
                .cloned()
                .collect::<Vec<_>>(),
            KeyRange {
                lower: Some(Key::Int64(2)),
                upper: None,
            },
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let encoded =
            snapshot::encode_partition_range(table, part, range, PartitionSize::default())
                .unwrap()
                .pop()
                .unwrap();
        let key = ObjectKey::partition(table.id, session.commit_id(), index as u32);
        files.push(PartitionFile {
            object: key.as_str().into(),
            range: encoded.range,
            row_count: encoded.row_count,
            byte_size: encoded.bytes.len() as u64,
            payload_bytes: encoded.payload_bytes,
        });
        uploads.push(PendingObject {
            key,
            bytes: encoded.bytes,
        });
    }
    let entry = candidate.tables.get_mut(&table.name).unwrap();
    entry.row_count = rows.len() as u64;
    entry.data = TableData::Partitioned { files };
    session
        .publish_partitions(candidate, uploads)
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
    let (engine, table, rows, workers) = fixture().await;
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
            format!("read/{}", entry.legacy_snapshot().unwrap())
        ]
    );
    assert_eq!(engine.get(key(&table, 99)).await.unwrap(), None);
    publish_partitioned(&engine, &table, &rows).await;
    take_keys(&engine);
    assert_eq!(
        engine.get(key(&table, 3)).await.unwrap(),
        Some(rows[0].clone())
    );
    let reads = take_keys(&engine);
    assert_eq!(reads.len(), 2);
    assert!(reads[1].ends_with("/1.parquet"));
    // Object storage futures run on the network/coordinator runtime.
    assert!(
        !engine
            .storage()
            .backend()
            .threads
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .2
            .starts_with("nabedra-worker-")
    );
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
            "scan mixed ({})\nfilter {}\nsort number DESC; tie-breaker id ASC\nlimit 1\nprojection text\npartitions 2; worker execution",
            table.id,
            serde_json::to_string(payload.query.filter.as_ref().unwrap()).unwrap()
        )
    );
    assert_eq!(take_keys(&engine), vec!["read/catalog.json"]);
    assert_eq!(
        engine.collect(all(&table)).await.unwrap(),
        vec![rows[1].clone(), rows[2].clone(), rows[0].clone()]
    );
    let session = engine.coordinator().begin().await.unwrap();
    let mut candidate = session.candidate();
    let entry = candidate.tables.get_mut(&table.name).unwrap();
    entry.row_count = 0;
    entry.data = storage::TableData::Partitioned { files: vec![] };
    session.publish_partitions(candidate, vec![]).await.unwrap();
    take_keys(&engine);
    assert_eq!(engine.get(key(&table, 1)).await.unwrap(), None);
    assert!(engine.collect(all(&table)).await.unwrap().is_empty());
    assert_eq!(engine.count(all(&table)).await.unwrap(), 0);
    assert_eq!(take_keys(&engine), vec!["read/catalog.json"; 3]);
    workers.shutdown().await.unwrap();
}
#[tokio::test]
async fn fresh_reads_validate_before_data_and_explain_needs_no_snapshot() {
    let (engine, table, original, workers) = fixture().await;
    let legacy_catalog = engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects["read/catalog.json"]
        .clone();
    publish_partitioned(&engine, &table, &original).await;
    let good_catalog = engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects["read/catalog.json"]
        .clone();
    let good_objects = engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .clone();
    for corruption in 0..6 {
        {
            let mut state = engine.storage().backend().fake.state.lock().unwrap();
            state.objects = good_objects.clone();
            let mut catalog: serde_json::Value = serde_json::from_slice(&good_catalog.0).unwrap();
            let files = catalog["tables"]["mixed"]["data"]["files"]
                .as_array_mut()
                .unwrap();
            match corruption {
                0 => files[0]["byte_size"] = (files[0]["byte_size"].as_u64().unwrap() + 1).into(),
                1 => {
                    files[0]["payload_bytes"] =
                        (files[0]["payload_bytes"].as_u64().unwrap() + 1).into()
                }
                2 => {
                    files[0]["range"]["upper"] = serde_json::json!(0);
                    files[1]["range"]["lower"] = serde_json::json!(0);
                }
                3 => {
                    files[0]["row_count"] = serde_json::json!(2);
                    catalog["tables"]["mixed"]["row_count"] = serde_json::json!(4);
                }
                4 => {
                    let object = files[0]["object"].as_str().unwrap();
                    state.objects.remove(&format!("read/{object}"));
                }
                _ => {
                    // A schema-valid but descending file must fail partition validation.
                    let object = files[1]["object"].as_str().unwrap().to_owned();
                    let bytes =
                        snapshot::encode(&table, &[original[0].clone(), original[2].clone()])
                            .unwrap();
                    files[1]["byte_size"] = (bytes.len() as u64).into();
                    state.objects.get_mut(&format!("read/{object}")).unwrap().0 = bytes;
                }
            }
            state.objects.get_mut("read/catalog.json").unwrap().0 =
                serde_json::to_vec(&catalog).unwrap();
        }
        let mut limited = all(&table);
        limited.query.limit = Some(0);
        assert_eq!(
            engine.collect(limited.clone()).await.unwrap_err().code,
            ErrorCode::StorageUnavailable
        );
        assert_eq!(
            engine.count(limited).await.unwrap_err().code,
            ErrorCode::StorageUnavailable
        );
        if corruption < 4 {
            assert_eq!(
                engine
                    .get(key(&table, if corruption == 2 { -1 } else { 1 }))
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::StorageUnavailable
            );
        }
    }
    // Restore the legacy fixture for its existing validation cases.
    engine
        .storage()
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert("read/catalog.json".into(), legacy_catalog);
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
    let snapshot_key = format!(
        "read/{}",
        loaded.catalog.tables["mixed"].legacy_snapshot().unwrap()
    );
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
    workers.shutdown().await.unwrap();
}
#[tokio::test]
async fn captured_snapshot_completes_across_update_drop_and_name_reuse() {
    let (engine, table, rows, workers) = fixture().await;
    let captured = engine
        .storage()
        .load_catalog()
        .await
        .unwrap()
        .catalog
        .tables["mixed"]
        .legacy_snapshot()
        .unwrap()
        .to_owned();
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
    // A captured download does not prevent unrelated schema computation.
    let replacement = engine
        .create(CreatePayload {
            name: table.name.clone(),
            schema: table.schema.clone(),
            key: table.key.clone(),
            format: table.format,
        })
        .await
        .unwrap();
    assert!(
        !task.is_finished(),
        "captured storage request remains gated"
    );
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
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn partition_workers_capture_across_drop_and_drain_after_error() {
    for missing in [false, true] {
        let (engine, table, rows, workers) = fixture().await;
        publish_partitioned(&engine, &table, &rows).await;
        let catalog = engine.storage().load_catalog().await.unwrap().catalog;
        let storage::TableData::Partitioned { files } = &catalog.tables["mixed"].data else {
            panic!()
        };
        let gates: Vec<_> = files
            .iter()
            .map(|file| {
                let gate = Arc::new(ReadGate::default());
                engine
                    .storage()
                    .backend()
                    .partition_gates
                    .lock()
                    .unwrap()
                    .insert(format!("read/{}", file.object), gate.clone());
                gate
            })
            .collect();
        if missing {
            engine
                .storage()
                .backend()
                .fake
                .state
                .lock()
                .unwrap()
                .objects
                .remove(&format!("read/{}", files[0].object));
        }
        take_keys(&engine);
        engine.storage().backend().threads.lock().unwrap().clear();
        let reader = engine.clone();
        let payload = all(&table);
        let task = tokio::spawn(async move { reader.collect(payload).await });
        for gate in &gates {
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        let threads = engine.storage().backend().threads.lock().unwrap().clone();
        assert_eq!(threads.len(), 2);
        assert!(
            threads
                .iter()
                .all(|(_, _, name)| !name.starts_with("nabedra-worker-"))
        );
        let usage = engine.worker_pool().scheduler().usage();
        assert_eq!(usage[0].1, 0, "downloads must not consume CPU slots");
        assert!(usage[0].2 > 0, "download buffers remain charged");
        // Both storage barriers entered before either opens. Independent CPU
        // work must also complete while both downloads remain gated.
        engine
            .worker_pool()
            .submit(99, |_, _| Ok(()))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(take_keys(&engine).len(), 3);
        engine.drop(table.id).await.unwrap();
        take_keys(&engine);
        gates[0].release.add_permits(1);
        engine
            .worker_pool()
            .submit(0, |_, _| Ok(()))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert!(
            !task.is_finished(),
            "operation returned before all admitted jobs finished"
        );
        gates[1].release.add_permits(1);
        let result = task.await.unwrap();
        if missing {
            assert_eq!(result.unwrap_err().code, ErrorCode::StorageUnavailable);
        } else {
            assert_eq!(
                result.unwrap(),
                vec![rows[1].clone(), rows[2].clone(), rows[0].clone()]
            );
        }
        assert!(take_keys(&engine).is_empty());
        workers.shutdown().await.unwrap();
    }
}
