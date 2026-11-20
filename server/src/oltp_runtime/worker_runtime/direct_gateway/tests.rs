//! T6 evidence on one worker (RF1), end to end: HTTP with a declared body
//! length, the gateway's Catalog fill and Control placement read, the
//! `DirectGateway` role checked by Control at the leader, `Method::Direct`
//! over the worker's own mTLS session, the T5 executor with the Catalog
//! schema. Modelled: direct commit timestamps (see the test body).
use super::*;
use crate::oltp_replication::{
    authority::{control::ServiceAction, timestamp::DirectTimestampClient},
    service_rpc::TimestampClient,
};
use crate::oltp_runtime::direct::{CatalogSchema, DirectConfig, DirectExecutor};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

/// Direct commit timestamps for this single-worker fixture.
struct Counter(std::sync::atomic::AtomicU64);
impl crate::oltp_replication::authority::timestamp::DirectTimestampSource for Counter {
    fn allocate_direct(
        &self,
        count: u64,
    ) -> crate::oltp_replication::authority::consensus::ConsensusFuture<
        '_,
        crate::oltp_replication::authority::timestamp::DirectTimestampRange,
    > {
        let first = self.0.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            crate::oltp_replication::authority::timestamp::DirectTimestampRange::new(
                CommitTs::new(first).unwrap(),
                CommitTs::new(first + count - 1).unwrap(),
            )
        })
    }
}

const TOKEN: &str = "t6-direct-gateway-data";
const DATABASE: &str = "t6_direct";

struct TestDirectory(std::path::PathBuf);
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One `/v1/operations` call. `declared` sends a `Content-Length`.
async fn call(app: &Router, body: Vec<u8>, declared: bool) -> (u16, Value) {
    let mut request = Request::post("/v1/operations")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json");
    if declared {
        request = request.header("content-length", body.len());
    }
    let reply = app
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = reply.status().as_u16();
    let body = to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn body(request_id: Uuid, operation: &str, payload: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "request_id": request_id, "operation": operation, "payload": payload,
    }))
    .unwrap()
}

/// The same program at key offset `base`.
fn program(table: &Value, base: i64) -> Vec<(&'static str, Value)> {
    let row = |id: i64, value: i64, tag: &str| json!({"id": base + id, "value": value, "tag": tag});
    let with = |mut payload: Value| {
        payload["table_id"] = table.clone();
        payload
    };
    vec![
        ("table.get", with(json!({"key": base + 1}))),
        ("table.insert", with(json!({"row": row(1, 10, "a")}))),
        ("table.get", with(json!({"key": base + 1}))),
        (
            "table.update",
            with(json!({"key": base + 1, "values": {"value": 11}})),
        ),
        (
            "table.update",
            with(json!({"key": base + 2, "values": {"value": 1}})),
        ),
        ("table.upsert", with(json!({"row": row(2, 20, "b")}))),
        ("table.upsert", with(json!({"row": row(2, 21, "c")}))),
        (
            "table.insert_many",
            with(json!({"rows": [row(4, 4, "d"), row(3, 3, "e")]})),
        ),
        ("table.delete", with(json!({"key": base + 3}))),
        ("table.delete", with(json!({"key": base + 3}))),
        ("table.get", with(json!({"key": base + 3}))),
        ("table.get", with(json!({"key": base + 4}))),
        ("table.get", with(json!({"key": base + 2}))),
        // Errors: the same codes and outcomes on both paths.
        ("table.insert", with(json!({"row": row(1, 0, "dup")}))),
        (
            "table.insert",
            with(json!({"row": {"id": base + 9, "value": "x", "tag": "t"}})),
        ),
        (
            "table.update",
            with(json!({"key": base + 1, "values": {"missing": 1}})),
        ),
        ("table.get", with(json!({"key": "1"}))),
        (
            "table.insert_many",
            with(json!({"rows": [row(7, 1, "f"), row(7, 2, "g")]})),
        ),
    ]
}

/// The envelope without the request id, with primary keys shifted back by
/// `base`, so the two runs compare.
fn normalized(status: u16, mut envelope: Value, base: i64) -> Value {
    fn shift(value: &mut Value, base: i64) {
        match value {
            Value::Object(map) => {
                if let Some(Value::Number(id)) = map.get("id")
                    && let Some(id) = id.as_i64()
                {
                    map.insert("id".into(), json!(id - base));
                }
                map.values_mut().for_each(|v| shift(v, base));
            }
            Value::Array(items) => items.iter_mut().for_each(|v| shift(v, base)),
            _ => {}
        }
    }
    envelope.as_object_mut().unwrap().remove("request_id");
    shift(&mut envelope, base);
    json!({"status": status, "envelope": envelope})
}

/// D01/O03/O02 on one worker: every eligible operation's results, errors
/// and types over the direct path equal the queued path's for the same
/// program; the topology is read once; with the capability off nothing
/// goes direct (O01).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d01_o03_rf1_direct_results_equal_the_queued_path_over_mtls() {
    tokio::time::timeout(Duration::from_secs(180), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-t6-gateway-{}", Uuid::new_v4())),
        );
        let runtime = WorkerRuntime::open(
            &directory.0,
            DATABASE,
            TOKEN,
            ExecutionConfig {
                max_pending_requests: 32,
                max_queue_bytes_per_replica: 8 << 20,
                max_retained_result_bytes_per_worker: 8 << 20,
                ..Default::default()
            },
            WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 2 << 30,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let app = runtime.application(TOKEN.into()).unwrap();
        let (status, created) = call(
            &app,
            body(
                Uuid::new_v4(),
                "table.create",
                json!({
                    "name": "items", "key": "id", "format": "parquet",
                    "schema": {"id": "int64", "value": "int64", "tag": "utf8"},
                }),
            ),
            true,
        )
        .await;
        assert_eq!(status, 200, "{created}");
        let table = created["data"]["id"].clone();
        let gateway = runtime.direct_gateway_handle().unwrap().clone();

        // The queued path (capability off), with and without a length.
        let mut queued = Vec::new();
        for (operation, payload) in program(&table, 0) {
            let (status, envelope) = call(&app, body(Uuid::new_v4(), operation, payload), true).await;
            queued.push((operation, normalized(status, envelope, 0)));
        }
        assert_eq!(gateway.stats(), Default::default(), "O01: nothing routed while off");

        // Activate: install the executor and turn the capability flag on.
        // The remote `AllocateDirect` cannot reach a single worker's own
        // Timestamp group ("worker does not host target replica node"); the
        // in-process source is T7's wiring, so a counter far above every
        // queued timestamp stands in for it here.
        let remote = TimestampClient::new(
            runtime.services.timestamps.identity().clone(),
            runtime.services.shared.clone(),
            runtime.services.manager.client(),
            runtime.services.control_client.clone(),
            ServiceAction::Owner,
        )
        .unwrap();
        assert_eq!(
            remote.allocate_direct(1).await.unwrap_err().code,
            ErrorCode::ValidationError,
            "the RF1 gap this test models; wire the local source (T7) and drop the counter"
        );
        let timestamps = Arc::new(Counter(std::sync::atomic::AtomicU64::new(1 << 60)));
        let executor = DirectExecutor::new(
            runtime.services.shared.clone(),
            DirectConfig::default(),
            runtime.clone(),
            Arc::new(CatalogSchema::new(runtime.services.catalog.clone())),
            DirectTimestampClient::new(None, timestamps),
        );
        executor.install();
        runtime.set_direct_commits_active(true);

        let base = 1_000;
        let mut latencies = Vec::new();
        for (index, (operation, payload)) in program(&table, base).into_iter().enumerate() {
            let started = std::time::Instant::now();
            let (status, envelope) = call(&app, body(Uuid::new_v4(), operation, payload), true).await;
            latencies.push(started.elapsed());
            if normalized(status, envelope.clone(), base) != queued[index].1 {
                eprintln!("d01 rf1 mismatch at {index}: leader {:?}", executor.stats());
            }
            assert_eq!(
                normalized(status, envelope, base),
                queued[index].1,
                "step {index} {operation}"
            );
        }
        let stats = gateway.stats();
        eprintln!("d01 rf1 gateway: {stats:?}");
        eprintln!("d01 rf1 leader: {:?}", executor.stats());
        // One fill for the one range; nothing per request after that.
        assert_eq!((stats.catalog_reads, stats.placement_reads), (1, 1), "{stats:?}");
        // Gets and mutations that reached the leader answered directly; the
        // utf8 key and the typed failures stayed exact.
        assert!(stats.direct_done >= 12, "{stats:?}");
        let leader = executor.stats();
        assert_eq!(leader.entries_applied, 6, "{leader:?}");

        // A second program without a declared length takes the queued path
        // and still answers the same (the full parsing charge applies).
        let base = 2_000;
        for (index, (operation, payload)) in program(&table, base).into_iter().enumerate() {
            let (status, envelope) = call(&app, body(Uuid::new_v4(), operation, payload), false).await;
            assert_eq!(normalized(status, envelope, base), queued[index].1, "step {index}");
        }
        assert_eq!(gateway.stats().requests, stats.requests);

        // The runtime stats show both sides.
        let reply = runtime.stats().await.unwrap();
        let stats: Value = serde_json::from_slice(&reply.body).unwrap();
        assert!(stats["direct_gateway"]["direct_done"].as_u64().unwrap() >= 12);
        assert!(stats["direct"].is_object());
        let total: Duration = latencies.iter().sum();
        eprintln!(
            "d01 rf1 latency (HTTP -> gateway -> mTLS -> leader -> fsync): mean {:.2} ms over {} ops, max {:.2} ms",
            total.as_secs_f64() * 1e3 / latencies.len() as f64,
            latencies.len(),
            latencies.iter().max().unwrap().as_secs_f64() * 1e3
        );
        runtime.set_direct_commits_active(false);
        drop(executor);
        runtime.drain().await.unwrap();
    })
    .await
    .expect("rf1 gateway watchdog");
}
