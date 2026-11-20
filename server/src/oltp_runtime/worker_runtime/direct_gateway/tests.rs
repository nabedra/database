//! T6 evidence on one worker (RF1), end to end: HTTP with a declared body
//! length, the gateway's Catalog fill and Control placement read, the
//! `DirectGateway` role checked by Control at the leader, `Method::Direct`
//! over the worker's own mTLS session, and the executor T7 installs on
//! activation (remote Catalog schema, in-process `AllocateDirect`).
use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

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

async fn open(path: &std::path::Path, direct_oltp: bool) -> Arc<WorkerRuntime> {
    WorkerRuntime::open(
        path,
        DATABASE,
        TOKEN,
        ExecutionConfig {
            max_pending_requests: 32,
            max_queue_bytes_per_replica: 8 << 20,
            max_retained_result_bytes_per_worker: 8 << 20,
            direct_oltp,
            ..Default::default()
        },
        WorkerResources {
            max_threads: 2,
            memory_limit_bytes: 2 << 30,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// Waits until this single worker activated `data.direct.v1` (T7: a lone
/// worker activates it on startup) and its executor is installed.
async fn activated(runtime: &WorkerRuntime) {
    let switch = runtime.direct_switch().unwrap().clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(switch.refresh().await.unwrap_or(false)
        && crate::oltp_replication::service_rpc::direct::installed(&runtime.services.shared)
            .is_some())
    {
        assert!(tokio::time::Instant::now() < deadline, "no activation");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn create(app: &Router) -> Value {
    let (status, created) = call(
        app,
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
    created["data"]["id"].clone()
}

/// D01/O03/O02 on one worker: every eligible operation's results, errors
/// and types over the direct path equal the queued path's for the same
/// program; the topology is read once per table. The baseline worker runs
/// with `direct_oltp: false`: its mutations are settled at the anchor and
/// executed queued, nothing runs directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d01_o03_rf1_direct_results_equal_the_queued_path_over_mtls() {
    tokio::time::timeout(Duration::from_secs(240), async {
        // The queued path (`direct_oltp: false`), with forward markers.
        let baseline_dir = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-t6-queued-{}", Uuid::new_v4())),
        );
        let baseline = open(&baseline_dir.0, false).await;
        activated(&baseline).await;
        let app = baseline.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let mut queued = Vec::new();
        for (operation, payload) in program(&table, 0) {
            let (status, envelope) =
                call(&app, body(Uuid::new_v4(), operation, payload), true).await;
            queued.push((operation, normalized(status, envelope, 0)));
        }
        let gateway = baseline.direct_gateway_handle().unwrap().stats();
        eprintln!("d01 rf1 queued gateway: {gateway:?}");
        assert_eq!(gateway.direct_done, 0, "{gateway:?}");
        assert!(gateway.forwards >= 8, "every eligible mutation settled first: {gateway:?}");
        baseline.drain().await.unwrap();

        // The direct path (`direct_oltp: true`).
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-t6-direct-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, true).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let gateway = runtime.direct_gateway_handle().unwrap().clone();
        let leader = || {
            crate::oltp_replication::service_rpc::direct::installed(&runtime.services.shared)
                .unwrap()
                .stats()
        };
        let mut latencies = Vec::new();
        for (index, (operation, payload)) in program(&table, 0).into_iter().enumerate() {
            let started = std::time::Instant::now();
            let (status, envelope) =
                call(&app, body(Uuid::new_v4(), operation, payload), true).await;
            latencies.push(started.elapsed());
            if normalized(status, envelope.clone(), 0) != queued[index].1 {
                eprintln!("d01 rf1 mismatch at {index}: leader {}", leader());
            }
            assert_eq!(
                normalized(status, envelope, 0),
                queued[index].1,
                "step {index} {operation}"
            );
        }
        let stats = gateway.stats();
        eprintln!("d01 rf1 gateway: {stats:?}");
        eprintln!("d01 rf1 leader: {}", leader());
        // One fill for the one range; nothing per request after that.
        assert_eq!(
            (stats.catalog_reads, stats.placement_reads),
            (1, 1),
            "{stats:?}"
        );
        // Gets and mutations that reached the leader answered directly; the
        // utf8 key went queued, typed failures came back exact.
        assert!(stats.direct_done >= 12, "{stats:?}");
        assert_eq!(stats.forwards, 0, "{stats:?}");
        assert_eq!(leader()["entries_applied"], json!(6));

        // Without a declared length the request takes the queued path and
        // still answers the same (the full parsing charge applies).
        let base = 2_000;
        for (index, (operation, payload)) in program(&table, base).into_iter().enumerate() {
            let (status, envelope) =
                call(&app, body(Uuid::new_v4(), operation, payload), false).await;
            assert_eq!(
                normalized(status, envelope, base),
                queued[index].1,
                "step {index}"
            );
        }
        assert_eq!(gateway.stats().requests, stats.requests);

        // The runtime stats show the gateway, the executor and the switch.
        let reply = runtime.stats().await.unwrap();
        let stats: Value = serde_json::from_slice(&reply.body).unwrap();
        assert!(stats["direct_gateway"]["direct_done"].as_u64().unwrap() >= 12);
        assert!(stats["direct"].is_object(), "{stats}");
        assert_eq!(stats["direct_capability"]["active"], json!(true));
        let total: Duration = latencies.iter().sum();
        eprintln!(
            "d01 rf1 latency (HTTP -> gateway -> mTLS -> leader -> fsync): mean {:.2} ms over {} ops, max {:.2} ms",
            total.as_secs_f64() * 1e3 / latencies.len() as f64,
            latencies.len(),
            latencies.iter().max().unwrap().as_secs_f64() * 1e3
        );
        runtime.drain().await.unwrap();
    })
    .await
    .expect("rf1 gateway watchdog");
}

/// T7b items 2 and 4 on one worker over mTLS: the executor's Catalog reader
/// (`DirectLeader`, remote over the worker's own session) reads the table's
/// exclusive lifecycle guard with the new `ParticipantExclusive` method;
/// while a DDL holds it, the first request does not activate the group (no
/// entry proposed, the pre-DDL lifecycle is never bound) and the request
/// waits for the DDL instead of going ahead, ending unavailable while the
/// DDL keeps the guard. The gateway's topology fill uses the same
/// `DirectLeader` reader (T6 concern 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o02_d11_t7b_rf1_activation_waits_for_the_exclusive_ddl_guard_over_mtls() {
    use crate::oltp::mvcc::{
        catalog::CatalogRead,
        store::lifecycle::{GuardMode, LifecycleBinding},
    };
    use crate::oltp_replication::types::{Digest, ReadTs, RequestBinding, TxnId};
    tokio::time::timeout(Duration::from_secs(240), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-t7b-guard-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, true).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let table_id: Uuid = serde_json::from_value(table.clone()).unwrap();
        let switch = runtime.direct_switch().unwrap().clone();
        let reader = switch
            .direct_catalog()
            .expect("DirectLeader Catalog reader");
        // Over the wire with `DirectLeader`: no guard yet.
        assert_eq!(
            reader
                .participant()
                .lifecycle_exclusive(table_id)
                .await
                .unwrap(),
            None
        );
        // A DDL takes the table's exclusive guard (its capability probe is
        // not involved here: the guard alone must stop activation).
        let authority = runtime
            .services
            .control
            .current(&runtime.services.scope)
            .await
            .unwrap()
            .ready
            .view
            .authority();
        let catalog = runtime.services.catalog.clone();
        let pin = catalog
            .snapshot(ReadTs::new(u64::MAX).unwrap(), authority)
            .unwrap();
        let (version, schema_epoch) = match catalog.describe(&pin, table_id).await.unwrap() {
            CatalogRead::Value {
                version,
                record: Some(record),
            } => (version, record.schema_epoch),
            _ => panic!("table record"),
        };
        let txn = TxnId {
            request_id: Uuid::new_v4(),
            attempt: 1,
        };
        let request = RequestBinding {
            run_id: authority.run_id,
            request_id: txn.request_id,
            digest: Digest::from_bytes([5; 32]),
            accepted_context: Digest::from_bytes([6; 32]),
        };
        let binding = LifecycleBinding {
            guard_id: Uuid::new_v4(),
            catalog_group_id: catalog.participant().identity().group_id,
            table_id,
            lifecycle_version: version,
            schema_epoch,
            txn,
            request,
            read_ts: ReadTs::new(u64::MAX - 1).unwrap(),
            authority,
        };
        let guard = catalog
            .participant()
            .acquire_lifecycle(binding.clone(), GuardMode::Exclusive)
            .await
            .unwrap()
            .expect("granted: no shared holder");
        assert_eq!(
            reader
                .participant()
                .lifecycle_exclusive(table_id)
                .await
                .unwrap(),
            Some(binding.clone())
        );
        // The first eligible mutation of the never-activated group.
        let insert = {
            let app = app.clone();
            let body = body(
                Uuid::new_v4(),
                "table.insert",
                json!({"table_id": table, "row": {"id": 1, "value": 1, "tag": "a"}}),
            );
            tokio::spawn(async move { call(&app, body, true).await })
        };
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(!insert.is_finished(), "the request waits for the DDL");
        let leader =
            crate::oltp_replication::service_rpc::direct::installed(&runtime.services.shared)
                .unwrap()
                .stats();
        assert_eq!(leader["entries_proposed"], json!(0), "{leader}");
        assert!(
            leader["fallbacks"]["schema"].as_u64().unwrap() >= 1,
            "{leader}"
        );
        // The DDL keeps its guard past the request's deadline: the request
        // ends unavailable, nothing was proposed and the group never bound
        // the pre-DDL lifecycle. (Release, activation at the published
        // lifecycle and the commit after it run in the 3-voter test
        // `o12_d11_t7b_activation_waits_for_a_ddl_that_read_the_capability_inactive`.)
        let (status, envelope) = insert.await.unwrap();
        assert_eq!(status, 503, "{envelope}");
        let leader =
            crate::oltp_replication::service_rpc::direct::installed(&runtime.services.shared)
                .unwrap()
                .stats();
        assert_eq!(leader["entries_proposed"], json!(0), "{leader}");
        assert_eq!(
            leader["proposal_failures"]["activation"],
            json!(0),
            "{leader}"
        );
        assert_eq!(leader["forward_markers_written"], json!(0), "{leader}");
        assert!(
            leader["retries"]["unavailable"].as_u64().unwrap() >= 1,
            "{leader}"
        );
        let gateway = runtime.direct_gateway_handle().unwrap().stats();
        assert_eq!(gateway.direct_done, 0, "{gateway:?}");
        assert_eq!(gateway.catalog_reads, 1, "{gateway:?}");
        assert_eq!(
            reader
                .participant()
                .lifecycle_exclusive(table_id)
                .await
                .unwrap(),
            Some(binding)
        );
        drop(guard);
        runtime.drain().await.unwrap();
    })
    .await
    .expect("rf1 guard watchdog");
}
