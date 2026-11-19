//! NS28: scenarios the deleted standalone engine tests named, now on the pool path.
//! Every request goes through `WorkerRuntime` and its HTTP application, as `serve` does.
use super::worker_runtime::WorkerRuntime;
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp::pool::{OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources},
    protocol::{self, ErrorCode},
};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tower::ServiceExt;
use uuid::Uuid;

const DATA: &str = "ns28-data";
const ADMIN: &str = "ns28-admin";

struct Directory(PathBuf);
impl Directory {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!("nabedra-ns28-{label}-{}", Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn send(
    app: &Router,
    request_id: Uuid,
    operation: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let body = json!({"request_id":request_id, "operation":operation, "payload":payload});
    let reply = app
        .clone()
        .oneshot(
            Request::post("/v1/operations")
                .header("authorization", format!("Bearer {DATA}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = reply.status();
    let bytes = to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn data(app: &Router, operation: &str, payload: Value) -> Value {
    let (status, reply) = send(app, Uuid::new_v4(), operation, payload).await;
    assert_eq!(status, StatusCode::OK, "{operation}: {reply}");
    reply["data"].clone()
}

async fn refused(app: &Router, operation: &str, payload: Value) -> Value {
    let (status, reply) = send(app, Uuid::new_v4(), operation, payload).await;
    assert_ne!(status, StatusCode::OK, "{operation}: {reply}");
    reply["error"]["code"].clone()
}

fn row(id: i64, rank: f64, text: &str) -> Value {
    json!({"id":id,"rank":rank,"text":text,"active":true})
}

fn query(table_id: &Value, filter: Value, order: Value, select: Value, limit: Value) -> Value {
    json!({"table_id":table_id,"query":{"filter":filter,"order_by":order,"select":select,"limit":limit}})
}

async fn create(app: &Router, name: &str) -> Value {
    let table = data(
        app,
        "table.create",
        json!({"name":name,
        "schema":{"id":"int64","rank":"float64","text":"utf8","active":"bool"},
        "key":"id","format":"parquet"}),
    )
    .await;
    table["id"].clone()
}

async fn open(path: &std::path::Path) -> (Arc<WorkerRuntime>, Router) {
    let runtime = WorkerRuntime::open(
        path,
        "ns28",
        DATA,
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
    let app = runtime
        .application_with_admin(DATA.into(), ADMIN.into())
        .unwrap();
    (runtime, app)
}

/// Replaces the deleted `o01_o03_o04_thirteen_operations_order_types_and_stale_handles`
/// and `o07_o08_o10_atomic_conflicts_stable_replays_and_database_binding` on the pool path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o01_o03_o04_o07_o08_o10_pool_path_thirteen_operations_types_order_conflicts_and_replay() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let dir = Directory::new("operations");
        let (runtime, app) = open(&dir.0).await;
        let id = create(&app, "items").await;

        // O01: the thirteen operations and their result shapes.
        let descriptor = data(&app, "table.describe", json!({"name":"items"})).await;
        assert_eq!(descriptor["id"], id);
        assert_eq!(descriptor["format"], "parquet");
        assert_eq!(
            data(&app, "table.list", json!({})).await,
            json!([descriptor])
        );
        assert_eq!(
            data(
                &app,
                "table.insert",
                json!({"table_id":id,"row":row(-1, 0.0, "first")})
            )
            .await,
            Value::Null
        );
        assert_eq!(
            data(
                &app,
                "table.insert_many",
                json!({"table_id":id,"rows":[
                    row(i64::MIN, -0.0, "min"), row(i64::MAX, 1.0, "max"),
                    row(0, 0.0, "zero"), row(4, 9.0, "four"),
                ]})
            )
            .await,
            json!(4)
        );
        // O03: integer extrema, signed zero, hits and misses.
        let minimum = data(&app, "table.get", json!({"table_id":id,"key":i64::MIN})).await;
        assert_eq!(minimum, row(i64::MIN, -0.0, "min"));
        assert!(minimum["rank"].as_f64().unwrap().is_sign_negative());
        assert_eq!(
            data(&app, "table.get", json!({"table_id":id,"key":5})).await,
            Value::Null
        );

        // O04: default key order, descending ties in ascending key order, top-k,
        // and/or/not filters, count with limit, signed zero equality.
        let all = query(&id, Value::Null, Value::Null, json!(["id"]), Value::Null);
        assert_eq!(
            data(&app, "query.collect", all.clone()).await,
            json!([{"id":i64::MIN},{"id":-1},{"id":0},{"id":4},{"id":i64::MAX}])
        );
        let ordered = query(
            &id,
            Value::Null,
            json!({"column":"rank","desc":true}),
            json!(["id"]),
            json!(4),
        );
        assert_eq!(
            data(&app, "query.collect", ordered).await,
            json!([{"id":4},{"id":i64::MAX},{"id":i64::MIN},{"id":-1}])
        );
        let filtered = query(
            &id,
            json!({"op":"and","args":[{"op":"ge","column":"id","value":-1},
                {"op":"not","arg":{"op":"or","args":[{"op":"eq","column":"id","value":4},
                    {"op":"eq","column":"active","value":false}]}}]}),
            Value::Null,
            json!(["id"]),
            json!(2),
        );
        assert_eq!(
            data(&app, "query.collect", filtered.clone()).await,
            json!([{"id":-1},{"id":0}])
        );
        assert_eq!(data(&app, "query.count", filtered).await, json!(2));
        let zero = query(
            &id,
            json!({"op":"eq","column":"rank","value":-0.0}),
            Value::Null,
            Value::Null,
            Value::Null,
        );
        assert_eq!(data(&app, "query.count", zero).await, json!(3));

        assert_eq!(
            data(
                &app,
                "table.update",
                json!({"table_id":id,"key":-1,"values":{"text":"changed"}})
            )
            .await,
            json!(true)
        );
        assert_eq!(
            data(
                &app,
                "table.update",
                json!({"table_id":id,"key":3,"values":{}})
            )
            .await,
            json!(false)
        );
        assert_eq!(
            data(
                &app,
                "table.upsert",
                json!({"table_id":id,"row":row(3, 3.0, "added")})
            )
            .await,
            Value::Null
        );
        assert_eq!(
            data(&app, "table.delete", json!({"table_id":id,"key":3})).await,
            json!(true)
        );
        assert_eq!(
            data(&app, "table.delete", json!({"table_id":id,"key":3})).await,
            json!(false)
        );
        let point = query(
            &id,
            json!({"op":"eq","column":"id","value":-1}),
            Value::Null,
            Value::Null,
            Value::Null,
        );
        let explanation = data(&app, "query.explain", point.clone()).await;
        assert!(
            explanation
                .as_str()
                .unwrap()
                .contains("access primary point seek on id"),
            "{explanation}"
        );
        assert_eq!(
            data(&app, "query.collect", point).await,
            json!([row(-1, 0.0, "changed")])
        );

        // O07: a batch with a conflicting key is atomic; its valid prefix stays private.
        assert_eq!(
            refused(
                &app,
                "table.insert_many",
                json!({"table_id":id,"rows":[
                    row(2, 0.0, "prefix"), row(4, 0.0, "conflict"),
                ]})
            )
            .await,
            json!("conflict")
        );
        assert_eq!(
            data(&app, "table.get", json!({"table_id":id,"key":2})).await,
            Value::Null
        );

        // O08/O10: the same identity and body replays one decision; a changed body
        // under that identity never applies.
        let request_id = Uuid::new_v4();
        let insert = json!({"table_id":id,"row":row(7, 1.0, "seven")});
        let first = send(&app, request_id, "table.insert", insert.clone()).await;
        assert_eq!(first.0, StatusCode::OK, "{}", first.1);
        let replay = send(&app, request_id, "table.insert", insert).await;
        assert_eq!(replay.0, StatusCode::OK, "{}", replay.1);
        assert_eq!(replay.1["data"], first.1["data"]);
        let changed = send(
            &app,
            request_id,
            "table.insert",
            json!({"table_id":id,"row":row(8, 1.0, "eight")}),
        )
        .await;
        assert_ne!(changed.0, StatusCode::OK, "{}", changed.1);
        assert_eq!(
            data(&app, "table.get", json!({"table_id":id,"key":8})).await,
            Value::Null
        );
        assert_eq!(
            data(
                &app,
                "query.count",
                query(&id, Value::Null, Value::Null, Value::Null, Value::Null)
            )
            .await,
            json!(6)
        );

        // O03: a dropped table's handle is stale; a recreated name gets a new identity.
        assert_eq!(
            data(&app, "table.drop", json!({"table_id":id})).await,
            Value::Null
        );
        let recreated = create(&app, "items").await;
        assert_ne!(id, recreated);
        assert_eq!(
            refused(&app, "query.collect", all).await,
            json!("not_found")
        );

        runtime.drain().await.unwrap();
        drop(app);
        drop(runtime);
    })
    .await
    .expect("NS28 operations watchdog");
}

/// Replaces `o21_growth_past_old_row_and_byte_limits_is_streamed_and_survives_reopen`:
/// one table grows past 100,000 rows and 64 MiB through the pool path, keeps exact
/// count/get/range semantics after a reopen, and still bounds collected results.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o21_pool_path_growth_past_old_row_and_byte_limits_survives_reopen() {
    tokio::time::timeout(Duration::from_secs(900), async {
        let dir = Directory::new("growth");
        let (runtime, app) = open(&dir.0).await;
        let id = create(&app, "large").await;
        let text = "x".repeat(768);
        let total = 110_001_i64;
        for first in (0..total).step_by(1000) {
            let rows: Vec<_> = (first..(first + 1000).min(total))
                .map(|key| row(key, (key % 7) as f64, &text))
                .collect();
            let inserted = data(
                &app,
                "table.insert_many",
                json!({"table_id":id,"rows":rows}),
            )
            .await;
            assert_eq!(inserted, json!((first + 1000).min(total) - first));
        }
        assert!(total as usize * text.len() > 64 << 20);
        runtime.drain().await.unwrap();
        drop(app);
        drop(runtime);

        let (runtime, app) = open(&dir.0).await;
        assert_eq!(
            data(&app, "table.get", json!({"table_id":id,"key":100_000})).await,
            row(100_000, 5.0, &text)
        );
        let all = query(&id, Value::Null, Value::Null, Value::Null, Value::Null);
        assert_eq!(data(&app, "query.count", all.clone()).await, json!(total));
        assert_eq!(
            refused(&app, "query.collect", all).await,
            json!("limit_exceeded")
        );
        assert_eq!(
            data(
                &app,
                "query.count",
                query(&id, Value::Null, Value::Null, Value::Null, json!(100_001))
            )
            .await,
            json!(100_001)
        );
        let range = query(
            &id,
            json!({"op":"ge","column":"id","value":100_000}),
            Value::Null,
            json!(["id"]),
            json!(3),
        );
        assert_eq!(
            data(&app, "query.collect", range).await,
            json!([{"id":100_000},{"id":100_001},{"id":100_002}])
        );
        runtime.drain().await.unwrap();
        drop(app);
        drop(runtime);
    })
    .await
    .expect("NS28 growth watchdog");
}

/// Replaces `o02_foreign_nonempty_directory_manifest_and_resource_limits_are_rejected`:
/// a pool refuses a foreign engine, cluster or pool identity and keeps its own.
#[tokio::test]
async fn o02_pool_refuses_foreign_engine_and_identity_and_reopens_its_own() {
    let dir = Directory::new("identity");
    let shared = WorkerSharedResources::new(
        29,
        WorkerResources {
            max_threads: 4,
            ..Default::default()
        },
        SharedResourceConfig::default(),
    )
    .unwrap();
    let identity = PoolIdentity {
        engine: "oltp".into(),
        pool_format_version: 1,
        cluster_id: Uuid::new_v4(),
        worker_id: Uuid::new_v4(),
        pool_id: Uuid::new_v4(),
    };
    drop(OltpPool::open_development(&dir.0, identity.clone(), shared.clone()).unwrap());
    for foreign in [
        PoolIdentity {
            engine: "olap".into(),
            ..identity.clone()
        },
        PoolIdentity {
            cluster_id: Uuid::new_v4(),
            ..identity.clone()
        },
        PoolIdentity {
            pool_id: Uuid::new_v4(),
            ..identity.clone()
        },
    ] {
        let error = OltpPool::open_development(&dir.0, foreign, shared.clone())
            .err()
            .expect("pool accepted a foreign identity");
        assert_eq!(error.code, ErrorCode::StorageUnavailable);
    }
    let reopened = OltpPool::open_development(&dir.0, identity.clone(), shared).unwrap();
    assert_eq!(reopened.identity(), &identity);
}
