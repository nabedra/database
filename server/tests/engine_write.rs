#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::worker_pool::{WorkerPool, WorkerThreads};
use nabedra_database_server::{engine::Engine, protocol, storage};
use std::num::NonZeroUsize;
#[path = "support/storage.rs"]
mod support;

use nabedra_database_server::engine::MutationResult::{Count, Found, Null};
use protocol::*;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::Poll,
};
use storage::{Download, Namespace, ObjectStore, Storage, StorageError, WriteCondition};
use support::{Fake, ReadGate};

fn row(id: i64, text: &str, number: f64) -> Row {
    [
        ("id".into(), Scalar::Int64(id)),
        ("text".into(), Scalar::Utf8(text.into())),
        ("number".into(), Scalar::Float64(number)),
    ]
    .into()
}
async fn fixture() -> (Engine<Fake>, TableDescriptor, WorkerThreads) {
    fixture_with_legacy(false).await
}
async fn fixture_with_legacy(legacy: bool) -> (Engine<Fake>, TableDescriptor, WorkerThreads) {
    let storage = Storage::new(Fake::default(), Namespace::new("write/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, "1 B".parse().unwrap());
    let payload = CreatePayload {
        name: "items".into(),
        schema: [
            ("id".into(), ColumnType::Int64),
            ("text".into(), ColumnType::Utf8),
            ("number".into(), ColumnType::Float64),
        ]
        .into(),
        key: "id".into(),
        format: TableFormat::Parquet,
        search: Default::default(),
    };
    let table = if legacy {
        let table = TableDescriptor {
            id: uuid::Uuid::new_v4(),
            name: payload.name,
            schema: payload.schema,
            key: payload.key,
            format: payload.format,
            search: Default::default(),
        };
        let rows = vec![row(5, "five", 5.0), row(-1, "minus", 1.0)];
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
                    snapshot: storage::ObjectKey::snapshot(table.id, session.commit_id())
                        .as_str()
                        .into(),
                },
            },
        );
        session
            .publish(
                candidate,
                Some(nabedra_database_server::snapshot::encode(&table, &rows).unwrap()),
            )
            .await
            .unwrap();
        table
    } else {
        engine.create(payload).await.unwrap()
    };
    (engine, table, workers)
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
async fn manifest(engine: &Engine<Fake>, table: &TableDescriptor) -> Vec<storage::PartitionFile> {
    let catalog = engine.storage().load_catalog().await.unwrap().catalog;
    let storage::TableData::Partitioned { files } = &catalog.tables[&table.name].data else {
        panic!("changed mutations must publish partitions")
    };
    files.clone()
}
fn error(error: ProtocolError, code: ErrorCode, outcome: Outcome) {
    assert_eq!((error.code, error.outcome), (code, outcome));
}

#[tokio::test]
async fn workflow_persists_rows_and_exact_results() {
    let (engine, table, workers) = fixture().await;
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
    let old_files = manifest(&engine, &table).await;
    assert_eq!(old_files.len(), 3);
    assert_eq!(
        engine
            .mutate(upsert(&table, row(2, "replacement", 2.0)))
            .await
            .unwrap(),
        Null
    );
    let changed_files = manifest(&engine, &table).await;
    assert_eq!(changed_files[0], old_files[0]);
    assert_eq!(changed_files[2], old_files[2]);
    assert_ne!(changed_files[1].object, old_files[1].object);
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
    let before_delete = manifest(&engine, &table).await;
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
    let after_delete = manifest(&engine, &table).await;
    assert_eq!(after_delete[2].object, before_delete[3].object);
    assert_eq!(after_delete[2].byte_size, before_delete[3].byte_size);
    assert_eq!(
        after_delete[2].payload_bytes,
        before_delete[3].payload_bytes
    );
    assert_eq!(after_delete[2].range.lower, before_delete[2].range.lower);
    let loaded = engine.storage().load_catalog().await.unwrap();
    let entry = &loaded.catalog.tables[&table.name];
    assert_eq!(entry.row_count, 3);
    assert!(matches!(entry.data, storage::TableData::Partitioned { .. }));
    for id in [1, 2, 4] {
        assert_eq!(
            engine
                .mutate(Operation::Delete(key(&table, id)))
                .await
                .unwrap(),
            Found(true)
        );
    }
    assert!(manifest(&engine, &table).await.is_empty());
    assert!(persisted(&engine, &table).await.is_empty());
    let count = puts(&engine);
    assert_eq!(
        engine
            .mutate(Operation::Delete(key(&table, -9)))
            .await
            .unwrap(),
        Found(false)
    );
    assert_eq!(
        engine.mutate(update(&table, -9, Row::new())).await.unwrap(),
        Found(false)
    );
    assert_eq!(puts(&engine), count);
    assert_eq!(
        engine
            .mutate(insert(&table, row(-9, "negative", 0.0)))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(
        persisted(&engine, &table).await,
        vec![row(-9, "negative", 0.0)]
    );
    engine.drop(table.id).await.unwrap();
    let replacement = engine
        .create(CreatePayload {
            name: table.name.clone(),
            schema: table.schema.clone(),
            key: table.key.clone(),
            format: table.format,
            search: Default::default(),
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
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn batches_are_atomic_and_no_ops_skip_all_puts() {
    let (engine, table, workers) = fixture().await;
    engine
        .mutate(insert(&table, row(1, "one", 1.0)))
        .await
        .unwrap();
    engine
        .mutate(insert(&table, row(10, "ten", 10.0)))
        .await
        .unwrap();
    let before = objects(&engine);
    let put_count = puts(&engine);
    error(
        engine
            .mutate(batch(
                &table,
                vec![
                    row(-2, "new left", 2.0),
                    row(10, "late duplicate right", 9.0),
                ],
            ))
            .await
            .unwrap_err(),
        ErrorCode::Conflict,
        Outcome::NotApplied,
    );
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    error(
        engine
            .mutate(batch(&table, vec![row(-3, "a", 0.0), row(-3, "b", 0.0)]))
            .await
            .unwrap_err(),
        ErrorCode::Conflict,
        Outcome::NotApplied,
    );
    assert_eq!(
        engine.storage().backend().state.lock().unwrap().gets,
        gets + 1
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
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_clones_serialize_before_loading_catalog() {
    let (engine, table, workers) = fixture().await;
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
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn candidate_limit_precedes_put_and_publication_uncertainty_propagates() {
    let (engine, table, workers) = fixture().await;
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
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_noop_stays_legacy_then_changed_rows_migrate() {
    let (engine, table, workers) = fixture_with_legacy(true).await;
    let rows = [row(5, "five", 5.0), row(-1, "minus", 1.0)];
    let before = objects(&engine);
    let count = puts(&engine);
    assert_eq!(
        engine
            .mutate(upsert(&table, rows[0].clone()))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(objects(&engine), before);
    assert_eq!(puts(&engine), count);
    assert!(
        engine
            .storage()
            .load_catalog()
            .await
            .unwrap()
            .catalog
            .tables[&table.name]
            .legacy_snapshot()
            .is_ok()
    );
    assert_eq!(
        engine
            .mutate(update(
                &table,
                -1,
                [("text".into(), Scalar::Utf8("changed".into()))].into()
            ))
            .await
            .unwrap(),
        Found(true)
    );
    assert_eq!(manifest(&engine, &table).await.len(), 2);
    assert_eq!(
        persisted(&engine, &table).await,
        vec![row(-1, "changed", 1.0), row(5, "five", 5.0)]
    );
    workers.shutdown().await.unwrap();
}

#[derive(Default)]
struct GatedBackend {
    fake: Fake,
    gates: Mutex<std::collections::BTreeMap<String, Arc<ReadGate>>>,
    threads: Mutex<Vec<(std::thread::ThreadId, String)>>,
}
impl ObjectStore for GatedBackend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let gate = self.gates.lock().unwrap().remove(key);
        if let Some(gate) = gate {
            self.threads.lock().unwrap().push((
                std::thread::current().id(),
                std::thread::current().name().unwrap_or("").into(),
            ));
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

#[tokio::test]
async fn mutation_workers_overlap_and_drain_without_publication_on_error() {
    for missing in [false, true] {
        let storage = Storage::new(GatedBackend::default(), Namespace::new("gated/").unwrap());
        storage.initialize_catalog().await.unwrap();
        let (pool, workers) = WorkerPool::start(
            NonZeroUsize::new(2).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(storage, pool, "1 B".parse().unwrap());
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
                search: Default::default(),
            })
            .await
            .unwrap();
        engine
            .mutate(batch(
                &table,
                vec![row(-10, "left", 0.0), row(10, "right", 0.0)],
            ))
            .await
            .unwrap();
        let captured = engine.storage().load_catalog().await.unwrap().catalog;
        let storage::TableData::Partitioned { files } = &captured.tables[&table.name].data else {
            panic!()
        };
        let gates: Vec<_> = files
            .iter()
            .map(|file| {
                let gate = Arc::new(ReadGate::default());
                engine
                    .storage()
                    .backend()
                    .gates
                    .lock()
                    .unwrap()
                    .insert(format!("gated/{}", file.object), gate.clone());
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
                .remove(&format!("gated/{}", files[0].object));
        }
        let before = engine
            .storage()
            .backend()
            .fake
            .state
            .lock()
            .unwrap()
            .objects
            .clone();
        let puts = engine.storage().backend().fake.state.lock().unwrap().puts;
        let writer = engine.clone();
        // Both new keys route to different original file ordinals.
        let op = batch(
            &table,
            vec![row(-9, "new left", 1.0), row(11, "new right", 1.0)],
        );
        let task = tokio::spawn(async move { writer.mutate(op).await });
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
                .all(|(_, name)| !name.starts_with("nabedra-worker-"))
        );
        let usage = engine.worker_pool().scheduler().usage();
        assert_eq!(usage[0].1, 0, "storage waits must release CPU capacity");
        assert!(usage[0].2 > 0, "private candidate input bytes stay charged");
        engine
            .worker_pool()
            .submit(99, |_, _| Ok(()))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        gates[0].release.add_permits(1);
        engine
            .worker_pool()
            .submit(0, |_, _| Ok(()))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert!(!task.is_finished(), "must drain the second admitted worker");
        assert_eq!(
            engine.storage().backend().fake.state.lock().unwrap().puts,
            puts
        );
        gates[1].release.add_permits(1);
        let result = task.await.unwrap();
        if missing {
            error(
                result.unwrap_err(),
                ErrorCode::StorageUnavailable,
                Outcome::NotApplied,
            );
            assert_eq!(
                engine
                    .storage()
                    .backend()
                    .fake
                    .state
                    .lock()
                    .unwrap()
                    .objects,
                before
            );
            assert_eq!(
                engine.storage().backend().fake.state.lock().unwrap().puts,
                puts
            );
        } else {
            assert_eq!(result.unwrap(), Count(2));
            assert_eq!(
                engine
                    .collect(QueryPayload {
                        table_id: table.id,
                        query: Query {
                            select: None,
                            filter: None,
                            order_by: None,
                            limit: None
                        }
                    })
                    .await
                    .unwrap(),
                vec![
                    row(-10, "left", 0.0),
                    row(-9, "new left", 1.0),
                    row(10, "right", 0.0),
                    row(11, "new right", 1.0)
                ]
            );
        }
        workers.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn utf8_keys_route_by_typed_order_and_reuse_adjacent_files() {
    let (engine, _, workers) = fixture().await;
    let table = engine
        .create(CreatePayload {
            name: "strings".into(),
            schema: [
                ("id".into(), ColumnType::Utf8),
                ("flag".into(), ColumnType::Bool),
            ]
            .into(),
            key: "id".into(),
            format: TableFormat::Parquet,
            search: Default::default(),
        })
        .await
        .unwrap();
    let row = |id: &str, flag| {
        [
            ("id".into(), Scalar::Utf8(id.into())),
            ("flag".into(), Scalar::Bool(flag)),
        ]
        .into()
    };
    assert_eq!(
        engine
            .mutate(batch(
                &table,
                vec![
                    row("é", true),
                    row("", false),
                    row("10", true),
                    row("2", false)
                ]
            ))
            .await
            .unwrap(),
        Count(4)
    );
    let old = manifest(&engine, &table).await;
    assert_eq!(old.len(), 4);
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    assert_eq!(
        engine.mutate(upsert(&table, row("2", true))).await.unwrap(),
        Null
    );
    assert_eq!(
        engine.storage().backend().state.lock().unwrap().gets,
        gets + 2,
        "keyed mutation fetches catalog and owner only"
    );
    let changed = manifest(&engine, &table).await;
    assert_eq!(old[0], changed[0]);
    assert_eq!(old[1], changed[1]);
    assert_eq!(old[3], changed[3]);
    assert_eq!(
        engine
            .mutate(Operation::Delete(KeyPayload {
                table_id: table.id,
                key: Key::Utf8("10".into())
            }))
            .await
            .unwrap(),
        Found(true)
    );
    assert_eq!(
        engine
            .mutate(insert(&table, row("11", false)))
            .await
            .unwrap(),
        Null
    );
    assert_eq!(
        persisted(&engine, &table).await,
        vec![
            row("", false),
            row("11", false),
            row("2", true),
            row("é", true)
        ]
    );
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_metadata_counts_toward_global_candidate_limits() {
    for payload_limit in [false, true] {
        let (engine, table, workers) = fixture().await;
        engine
            .mutate(batch(
                &table,
                vec![row(-1, "left", 0.0), row(1, "right", 0.0)],
            ))
            .await
            .unwrap();
        // Model a large retained neighbor using catalog accounting, without
        // allocating cap-sized fixtures. Only the small left file is fetched.
        let mut catalog = engine.storage().load_catalog().await.unwrap().catalog;
        let entry = catalog.tables.get_mut(&table.name).unwrap();
        let storage::TableData::Partitioned { files } = &mut entry.data else {
            panic!()
        };
        if payload_limit {
            files[1].payload_bytes = MAX_DECODED_PAYLOAD_BYTES as u64 - files[0].payload_bytes;
        } else {
            files[1].row_count = MAX_TABLE_ROWS as u64 - files[0].row_count;
            entry.row_count = MAX_TABLE_ROWS as u64;
        }
        let bytes = catalog.encode().unwrap();
        engine
            .storage()
            .backend()
            .state
            .lock()
            .unwrap()
            .objects
            .get_mut("write/catalog.json")
            .unwrap()
            .0 = bytes;
        let before = objects(&engine);
        let count = puts(&engine);
        let op = if payload_limit {
            update(
                &table,
                -1,
                [("text".into(), Scalar::Utf8("longer left".into()))].into(),
            )
        } else {
            insert(&table, row(-2, "new", 0.0))
        };
        error(
            engine.mutate(op).await.unwrap_err(),
            ErrorCode::LimitExceeded,
            Outcome::NotApplied,
        );
        assert_eq!(puts(&engine), count);
        assert_eq!(objects(&engine), before);
        workers.shutdown().await.unwrap();
    }
}
