//! Measure ready frames and reservations at worker-state barriers.
use super::*;
use crate::{
    config::WorkerResources,
    partition_size::PartitionSize,
    protocol::parse_request,
    storage::{CatalogFence, Download, Namespace, StorageError, WriteCondition},
    test_storage::{Fake, ReadGate},
    worker_pool::WorkerThreads,
};
use serde_json::{Value, json};

const MIB: u64 = 1 << 20;

#[derive(Default)]
struct Store {
    inner: Fake,
    next_partition: std::sync::Mutex<Option<Arc<ReadGate>>>,
}
impl ObjectStore for Store {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        if key.ends_with(".parquet") {
            let gate = self.next_partition.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.add_permits(1);
                gate.release.acquire().await.unwrap().forget();
            }
        }
        self.inner.get(key).await
    }

    async fn put(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.inner.put(key, body, condition).await
    }
}

async fn seeded() -> (Engine<Store>, WorkerThreads, Uuid, Uuid) {
    let storage = Storage::new(
        Store::default(),
        Namespace::new("stream-worker-test/").unwrap(),
    );
    let run = Uuid::new_v4();
    storage
        .initialize_fenced_cluster_catalog(
            CatalogFence {
                run_id: run,
                scheduler_term: 1,
            },
            false,
            false,
        )
        .await
        .unwrap();
    let resources = WorkerResources {
        max_threads: 1,
        memory_limit_bytes: 128 * MIB,
        task_memory_limit_bytes: 32 * MIB,
        control_memory_reserve_bytes: 16 * MIB,
    };
    let (pool, threads) =
        WorkerPool::start_with_resources(resources, tokio::runtime::Handle::current()).unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let table = operation(
        &engine,
        "table.create",
        json!({
            "name": "items", "schema": {"id": "int64", "text": "utf8"},
            "key": "id", "format": "parquet"
        }),
    )
    .await;
    let id = serde_json::from_value(table["id"].clone()).unwrap();
    let data: Vec<_> = (0..16)
        .map(|id| json!({"id": id, "text": format!("row-{id}")}))
        .collect();
    operation(
        &engine,
        "table.insert_many",
        json!({"table_id": id, "rows": data}),
    )
    .await;
    (engine, threads, run, id)
}

async fn operation(engine: &Engine<Store>, name: &str, payload: Value) -> Value {
    let request = json!({"request_id": Uuid::new_v4(), "operation": name, "payload": payload});
    let bytes = serde_json::to_vec(&request).unwrap();
    let operation = parse_request(&bytes).unwrap().operation;
    let catalog = engine.storage().load_cluster_catalog().await.unwrap();
    let fence = catalog.catalog().fence.unwrap();
    let pinned = (!operation.is_mutation()).then_some(catalog);
    let result = engine
        .execute_identified(bytes, fence, pinned)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&result.body).unwrap();
    assert_eq!(result.status, 200, "{value}");
    value["data"].clone()
}

fn open(run: Uuid, table_id: Uuid, depth: u8) -> Open {
    Open {
        request_id: Uuid::new_v4(),
        expected_run_id: run,
        table_id,
        query: serde_json::from_value(json!({
            "select": null, "filter": null, "order_by": null, "limit": null,
        }))
        .unwrap(),
        batch_rows: 3,
        prefetch_batches: depth,
        format: wire::Format::Rows,
        drop_last: false,
        snapshot_id: None,
        shard: 0,
    }
}

async fn opened(local: &Local, engine: &Engine<Store>, run: Uuid, body: Open) -> Opened {
    let Reply::Opened(opened) = local.call(engine, run, Request::Open(body)).await.unwrap() else {
        panic!("stream did not open");
    };
    opened
}

fn request(opened: &Opened, sequence: u64) -> Request {
    Request::Batch {
        run_id: opened.run_id,
        stream_id: opened.stream_id,
        sequence,
    }
}

async fn settled(activity: &Activity) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while activity.speculative.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("speculation did not settle");
}

fn ready(activity: &Activity) -> Vec<u64> {
    let cache = activity.cache.lock().unwrap();
    for ahead in cache.ahead.values() {
        assert!(
            ahead
                .output
                .bytes
                .as_ref()
                .is_some_and(|data| !data.is_empty())
        );
    }
    cache.ahead.keys().copied().collect()
}

fn code(error: Error) -> ErrorCode {
    match error {
        Error::Protocol(error) => error.code,
        Error::Stream(other) => panic!("unexpected stream error {other:?}"),
    }
}

#[tokio::test]
async fn st03_prefetch_window_and_egress_lifetime() {
    let (engine, threads, run, table) = seeded().await;
    let pool = engine.worker_pool();
    let baseline = pool.scheduler().usage()[0].2;
    let local = Local::new();
    let total_outputs = local.registry.outputs.available_permits();
    let total_speculative = local.registry.speculative_outputs.available_permits();
    for depth in 0..=2 {
        let body = open(run, table, depth);
        let key = Key {
            scope: Scope::from_context(None).unwrap(),
            id: body.request_id,
        };
        let opened = opened(&local, &engine, run, body).await;
        assert_eq!(opened.prefetch_batches, depth);
        let record = local.registry.get(&key).unwrap();
        let activity = record.activity.clone();
        drop(record);
        settled(&activity).await;
        let before_demand = ready(&activity);
        assert_eq!(before_demand, (0..u64::from(depth)).collect::<Vec<_>>());
        assert_eq!(
            total_speculative - local.registry.speculative_outputs.available_permits(),
            depth as usize
        );
        let Reply::Batch(delivery) = local.call(&engine, run, request(&opened, 0)).await.unwrap()
        else {
            panic!("first demand did not return a batch");
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&delivery.bytes).unwrap(),
            json!({"rows": [{"id": 0, "text": "row-0"}, {"id": 1, "text": "row-1"}, {"id": 2, "text": "row-2"}]})
        );
        settled(&activity).await;
        assert_eq!(ready(&activity), (1..=u64::from(depth)).collect::<Vec<_>>());
        let (id, active_cpu, ready_bytes) = pool.scheduler().usage()[0];
        assert_eq!((id, active_cpu), (0, 0), "held transport must release CPU");
        assert!(ready_bytes > baseline && ready_bytes <= pool.resources().working_bytes());
        assert_eq!(
            total_outputs - local.registry.outputs.available_permits(),
            depth as usize + 1,
            "there can be one unacknowledged output and only the granted window"
        );
        let overlap = local
            .call(&engine, run, request(&opened, 0))
            .await
            .unwrap_err();
        assert_eq!(code(overlap), ErrorCode::Overloaded);
        assert_eq!(
            operation(&engine, "table.get", json!({"table_id": table, "key": 9})).await,
            json!({"id": 9, "text": "row-9"})
        );

        assert!(matches!(
            local
                .call(
                    &engine,
                    run,
                    Request::Close {
                        run_id: run,
                        stream_id: opened.stream_id
                    }
                )
                .await
                .unwrap(),
            Reply::Closed
        ));
        assert_eq!(
            local.registry.outputs.available_permits(),
            total_outputs - 1
        );
        assert!(ready(&activity).is_empty());
        drop(delivery);
        assert_eq!(local.registry.outputs.available_permits(), total_outputs);
        assert_eq!(
            local.registry.speculative_outputs.available_permits(),
            total_speculative
        );
        let tombstone = pool.scheduler().usage()[0].2 - baseline;
        assert!(tombstone > 0 && tombstone < ready_bytes - baseline);
        local.registry.reap(u64::MAX);
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, baseline)]);
        eprintln!(
            "ST03 depth {depth}, ready memory {ready_bytes} bytes, tombstone {tombstone} bytes"
        );
    }
    drop(local);
    threads.shutdown().await.unwrap();
}

#[tokio::test]
async fn st03_close_releases_in_progress_prefetch() {
    let (engine, threads, run, table) = seeded().await;
    let pool = engine.worker_pool();
    let baseline = pool.scheduler().usage()[0].2;
    let local = Local::new();
    let total_outputs = local.registry.outputs.available_permits();
    let gate = Arc::new(ReadGate::default());
    *engine.storage().backend().next_partition.lock().unwrap() = Some(gate.clone());
    let body = open(run, table, 2);
    let key = Key {
        scope: Scope::from_context(None).unwrap(),
        id: body.request_id,
    };
    let opened = opened(&local, &engine, run, body).await;
    let activity = local.registry.get(&key).unwrap().activity.clone();
    tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
        .await
        .expect("open did not start lookahead before demand")
        .unwrap()
        .forget();
    assert!(ready(&activity).is_empty());
    let (_, cpu, in_progress) = pool.scheduler().usage()[0];
    assert_eq!(cpu, 0, "partition I/O held a compute slot");
    assert!(in_progress > baseline && in_progress <= pool.resources().working_bytes());
    assert_eq!(
        local.registry.outputs.available_permits(),
        total_outputs - 1
    );
    assert_eq!(
        operation(&engine, "table.get", json!({"table_id": table, "key": 9})).await,
        json!({"id": 9, "text": "row-9"})
    );
    let closed = local
        .call(
            &engine,
            run,
            Request::Close {
                run_id: run,
                stream_id: opened.stream_id,
            },
        )
        .await
        .unwrap();
    assert!(matches!(closed, Reply::Closed));
    gate.release.add_permits(1);
    settled(&activity).await;
    assert!(ready(&activity).is_empty());
    assert_eq!(local.registry.outputs.available_permits(), total_outputs);
    let expired = local
        .call(&engine, run, request(&opened, 0))
        .await
        .unwrap_err();
    assert_eq!(expired, Error::expired());
    local.registry.reap(u64::MAX);
    assert_eq!(pool.scheduler().usage(), vec![(0, 0, baseline)]);
    eprintln!("ST03 in-progress reserved {in_progress} bytes from {baseline} baseline");
    drop(local);
    threads.shutdown().await.unwrap();
}

fn managed(prefix: &str, secret: &str) -> RequestContext {
    serde_json::from_value(json!({
        "version": 1, "bucket": "test-streaming", "region": "us-east-1", "prefix": prefix,
        "credentials": {
            "access_key_id": "synthetic-key", "secret_access_key": secret,
            "session_token": null, "expires_at": null
        }
    }))
    .unwrap()
}

#[tokio::test]
async fn st03_replica_byte_ceiling_and_expiry_release() {
    let (engine, threads, run, table) = seeded().await;
    let local = Local::new();
    let pool = engine.worker_pool();
    let body = open(run, table, 0);
    let key = Key {
        scope: Scope::from_context(None).unwrap(),
        id: body.request_id,
    };
    let _opened = opened(&local, &engine, run, body).await;
    let mut entry = local.registry.get(&key).unwrap().entry.clone();
    drop(local);
    let baseline = pool.scheduler().usage()[0].2;
    let registry = Registry::new();
    registry.set_run(run, false).unwrap();
    let context = managed("first/", "correct-secret");
    entry.key.scope = Scope::from_context(Some(&context)).unwrap();
    entry.context = Some(context);
    let expiry = entry.active_until();
    let now = expiry.saturating_sub(1);
    let id = match &entry.kind {
        Kind::Stream(stream) => stream.opened.stream_id,
        Kind::Snapshot(_) => unreachable!(),
    };
    let holder = registry
        .install(entry.clone(), Some(None), 9 * MIB, pool.scheduler(), now)
        .unwrap()
        .unwrap();
    assert!((pool.scheduler().usage()[0].2 - baseline) >= 9 * MIB);
    for other in [
        managed("other/", "correct-secret"),
        managed("first/", "different-secret"),
    ] {
        let foreign = Key {
            id,
            scope: Scope::from_context(Some(&other)).unwrap(),
        };
        match registry.handle(&foreign) {
            Err(error) => assert_eq!(code(error), ErrorCode::Unauthorized),
            Ok(_) => panic!("cross-scope handle was accepted"),
        }
    }
    let charged = pool.scheduler().usage()[0].2;
    let mut second = entry.clone();
    second.key.id = Uuid::new_v4();
    if let Kind::Stream(stream) = &mut second.kind {
        stream.opened.stream_id = Uuid::new_v4();
    }
    assert!(matches!(
        registry.install(second.clone(), Some(None), 8 * MIB, pool.scheduler(), now),
        Err(error) if error.code == ErrorCode::Overloaded
    ));
    assert!(registry.get(&second.key).is_none());
    assert_eq!(pool.scheduler().usage()[0].2, charged);

    drop(holder);
    registry.reap(expiry);
    let expired = registry.get(&entry.key).unwrap();
    assert!(expired.entry.scan.is_none() && !expired.entry.active(expiry));
    let tombstone = pool.scheduler().usage()[0].2 - baseline;
    assert!(tombstone > 0 && tombstone < MIB);
    drop(expired);
    second.idle_at = expiry + state::IDLE_MS;
    assert!(
        registry
            .install(second, Some(None), 8 * MIB, pool.scheduler(), expiry)
            .unwrap()
            .is_some()
    );
    registry.reap(u64::MAX);
    assert_eq!(pool.scheduler().usage(), vec![(0, 0, baseline)]);
    threads.shutdown().await.unwrap();
}
