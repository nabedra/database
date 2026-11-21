use super::*;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::time::Duration;
use tower::ServiceExt;

const DATA: &str = "index-operator-data";
const ADMIN: &str = "index-operator-admin";
const DATABASE: &str = "index-operator";

async fn call(
    app: &Router,
    path: &str,
    token: &str,
    database: &str,
    request_id: Uuid,
    operation: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let reply = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("authorization", format!("Bearer {token}"))
                .header("x-nabedra-database", database)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"request_id":request_id,"operation":operation,"payload":payload})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = reply.status();
    let body = to_bytes(reply.into_body(), protocol::MAX_BODY_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn data(app: &Router, operation: &str, payload: Value) -> Value {
    let (status, result) = call(
        app,
        "/v1/operations",
        DATA,
        DATABASE,
        Uuid::new_v4(),
        operation,
        payload,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{operation}: {result}");
    result["data"].clone()
}

async fn admin(
    app: &Router,
    request_id: Uuid,
    operation: &str,
    payload: Value,
) -> (StatusCode, Value) {
    call(
        app,
        "/v1/cluster/operations",
        ADMIN,
        DATABASE,
        request_id,
        operation,
        payload,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o12_u01_protected_index_management_uses_exact_catalog_job() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let path = std::env::temp_dir().join(format!("nabedra-index-operator-{}", Uuid::new_v4()));
        let runtime = WorkerRuntime::open(
            &path, DATABASE, DATA,
            ExecutionConfig {
                max_pending_requests: 32,
                max_queue_bytes_per_replica: 8 << 20,
                max_retained_result_bytes_per_worker: 8 << 20,
                ..Default::default()
            },
            WorkerResources { max_threads: 2, memory_limit_bytes: 2 << 30, ..Default::default() },
        ).await.unwrap();
        let app = runtime.application_with_admin(DATA.into(), ADMIN.into()).unwrap();
        let table = data(&app, "table.create", json!({
            "name":"items", "schema":{"id":"int64", "category":"int64", "label":"utf8"},
            "key":"id", "format":"parquet",
        })).await;
        let table = table["id"].clone();
        let build_id = Uuid::new_v4();
        let index_id = Uuid::new_v4();
        let create = "storage.index.create";
        let status = "storage.index.status";
        let resume = "storage.index.resume";
        let retire = "storage.index.retire";
        let definition = json!({"table_id":table,"index_id":index_id,"name":"by_category","columns":["category"],"covering":["label"]});
        let selector = json!({"table_id":table,"index_id":index_id,"build_id":build_id,"attempt":1});
        let (code, _) = call(&app, "/v1/cluster/operations", DATA, DATABASE, build_id, create, definition.clone()).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = call(&app, "/v1/cluster/operations", ADMIN, "another-db", build_id, create, definition.clone()).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        assert!(protocol::parse_request(&serde_json::to_vec(&json!({
            "request_id":build_id,"operation":create,"payload":definition,
        })).unwrap()).is_err());

        let mut malformed = definition.clone();
        malformed["columns"] = json!(["unknown"]);
        assert_eq!(admin(&app, build_id, create, malformed).await.0, StatusCode::BAD_REQUEST);
        assert!(runtime.durable_lookup(runtime.index_declaration_identity(build_id).unwrap()).await.unwrap().is_none());
        let mut forged = selector.clone();
        forged["proof"] = json!({"ready":true});
        assert_eq!(admin(&app, Uuid::new_v4(), resume, forged).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(admin(&app, Uuid::new_v4(), status, selector.clone()).await.0, StatusCode::NOT_FOUND);

        assert_eq!(data(&app, "table.insert_many", json!({"table_id":table,"rows":[
            {"id":1,"category":7,"label":"old"}, {"id":2,"category":7,"label":"keep"},
        ]})).await, json!(2));
        let occupied = runtime.index_builds.try_admit().unwrap();
        let (code, first) = admin(&app, build_id, create, definition.clone()).await;
        assert_eq!(code, StatusCode::OK, "{first}");
        assert_eq!(first["data"], json!({"state":"building","definition":definition,"build_id":build_id,"attempt":1}));
        assert_eq!(admin(&app, build_id, create, definition.clone()).await, (StatusCode::OK, first.clone()));
        let mut different = definition.clone();
        different["index_id"] = json!(Uuid::new_v4());
        different["name"] = json!("same_request_uuid");
        assert_eq!(admin(&app, build_id, create, different).await.0, StatusCode::CONFLICT);
        let mut stale = selector.clone();
        stale["attempt"] = json!(2);
        assert_eq!(admin(&app, Uuid::new_v4(), resume, stale).await.0, StatusCode::CONFLICT);
        assert_eq!(admin(&app, Uuid::new_v4(), resume, selector.clone()).await.0, StatusCode::TOO_MANY_REQUESTS);

        let query = json!({"table_id":table,"query":{
            "select":["id","label"],"filter":{"column":"category","op":"eq","value":7},
            "order_by":null,"limit":null,
        }});
        assert!(!data(&app, "query.explain", query.clone()).await.as_str().unwrap().contains("by_category"));
        data(&app, "table.update", json!({"table_id":table,"key":1,"values":{"label":"updated"}})).await;
        data(&app, "table.insert", json!({"table_id":table,"row":{"id":3,"category":7,"label":"inserted"}})).await;
        drop(occupied);
        let (code, started) = admin(&app, Uuid::new_v4(), resume, selector.clone()).await;
        assert_eq!(code, StatusCode::OK, "{started}");
        assert_eq!(started["data"]["state"], "building");
        loop {
            let (code, observed) = admin(&app, Uuid::new_v4(), status, selector.clone()).await;
            if code == StatusCode::OK && observed["data"]["state"] == "ready" {
                break;
            }
            assert!(code == StatusCode::SERVICE_UNAVAILABLE || (code == StatusCode::OK && observed["data"]["state"] == "building"), "{code} {observed}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(data(&app, "query.explain", query.clone()).await.as_str().unwrap().contains("by_category"));
        let rows = json!([{ "id":1,"label":"updated" },{ "id":2,"label":"keep" },{ "id":3,"label":"inserted" }]);
        assert_eq!(data(&app, "query.collect", query.clone()).await, rows);

        let catalog = runtime.services.catalog.participant();
        let owner = MaintenanceOwnerRef {
            catalog_group_id: catalog.identity().group_id,
            table_id: serde_json::from_value(table).unwrap(), index_id, build_id, attempt: 1,
        };
        let ready = catalog.strong_index_attempt_closed(owner).await.unwrap();
        assert_eq!(ready.statement().kind, IndexClosedKind::Ready);
        let retirement_id = Uuid::new_v4();
        let retired = loop {
            let (code, reply) = admin(&app, retirement_id, retire, selector.clone()).await;
            if code == StatusCode::OK { break reply; }
            assert_eq!(code, StatusCode::TOO_MANY_REQUESTS, "{reply}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(retired["data"]["state"], "retired");
        assert_eq!(admin(&app, retirement_id, retire, selector.clone()).await, (StatusCode::OK, retired));
        assert_eq!(admin(&app, Uuid::new_v4(), resume, selector).await.0, StatusCode::CONFLICT);
        let live_closure = catalog.strong_index_attempt_closed(owner).await.unwrap();
        assert_eq!(live_closure.archive_evidence(), ready.archive_evidence());
        let authority = runtime.index_authority(None).await.unwrap();
        let txn = ready.archive_evidence().txn;
        let Some(IdentityLookup::Terminal(TerminalIdentity::Committed {
            reservation, decision, ..
        })) = runtime.durable_lookup(txn.request_id).await.unwrap() else {
            panic!("Ready transaction has no durable identity");
        };
        let timestamps = &runtime.services.timestamps;
        let pin = timestamps.index_metadata_pin(authority, owner, txn, reservation, decision.request)
            .await.unwrap();
        assert!(pin.released);
        let other = MaintenanceOwnerRef { index_id: Uuid::new_v4(), ..owner };
        assert!(timestamps.index_metadata_pin(authority, other, txn, reservation, decision.request).await.is_err());
        let different_nonce = IdentityReservation { reservation_id: Uuid::new_v4(), ..reservation };
        assert!(timestamps.index_metadata_pin(authority, owner, txn, different_nonce, decision.request).await.is_err());
        assert!(!data(&app, "query.explain", query.clone()).await.as_str().unwrap().contains("by_category"));
        assert_eq!(data(&app, "query.collect", query).await, rows);
        drop((app, live_closure, ready));
        runtime.stop_accepting();
        runtime.drain().await.unwrap();
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }).await.unwrap();
}

/// IDX1 (D11, O12): `storage.index.status` is a read. The real build actor is held
/// after Prepare and after the commit decision of each metadata transaction, while
/// its Catalog intents are unresolved. Every status poll at a hold answers the
/// current state, or a typed retryable `not_applied` error while the Ready
/// publication is undecided, and returns promptly. It never answers outcome_unknown.
/// After the Ready decision it helps the decided intent and answers `ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d11_o12_index_status_during_ready_publication_is_never_outcome_unknown() {
    use crate::oltp_runtime::recovery_tests::index_holds::{self, Stage};
    tokio::time::timeout(Duration::from_secs(120), async {
        let path = std::env::temp_dir().join(format!("nabedra-index-status-{}", Uuid::new_v4()));
        let runtime = WorkerRuntime::open(
            &path, DATABASE, DATA,
            ExecutionConfig {
                max_pending_requests: 32,
                max_queue_bytes_per_replica: 8 << 20,
                max_retained_result_bytes_per_worker: 8 << 20,
                ..Default::default()
            },
            WorkerResources { max_threads: 2, memory_limit_bytes: 2 << 30, ..Default::default() },
        ).await.unwrap();
        let app = runtime.application_with_admin(DATA.into(), ADMIN.into()).unwrap();
        let table = data(&app, "table.create", json!({
            "name":"items", "schema":{"id":"int64", "category":"int64", "label":"utf8"},
            "key":"id", "format":"parquet",
        })).await["id"].clone();
        assert_eq!(data(&app, "table.insert_many", json!({"table_id":table,"rows":[
            {"id":1,"category":7,"label":"one"}, {"id":2,"category":3,"label":"two"},
        ]})).await, json!(2));
        let build_id = Uuid::new_v4();
        let index_id = Uuid::new_v4();
        let definition = json!({"table_id":table,"index_id":index_id,"name":"by_category","columns":["category"],"covering":["label"]});
        let selector = json!({"table_id":table,"index_id":index_id,"build_id":build_id,"attempt":1});
        // Declare without an actor, then hold every later metadata commit of this Catalog.
        let occupied = runtime.index_builds.try_admit().unwrap();
        let (code, created) = admin(&app, build_id, "storage.index.create", definition).await;
        assert_eq!(code, StatusCode::OK, "{created}");
        assert_eq!(created["data"]["state"], "building");
        let catalog = runtime.services.catalog.participant().identity().group_id;
        let mut holds = index_holds::register(catalog);
        drop(occupied);
        let resume = tokio::spawn({
            let (app, selector) = (app.clone(), selector.clone());
            async move { admin(&app, Uuid::new_v4(), "storage.index.resume", selector).await }
        });

        let (mut retryable, mut held_ready) = (Vec::new(), None);
        while held_ready.is_none() {
            let held = tokio::time::timeout(Duration::from_secs(60), holds.recv())
                .await.expect("the build actor reached no further metadata commit")
                .expect("hold channel closed");
            let started = tokio::time::Instant::now();
            let (code, reply) = admin(&app, Uuid::new_v4(), "storage.index.status", selector.clone()).await;
            let observed = format!("{:?} {:?}: {code} {reply}", held.stage, held.txn);
            assert!(started.elapsed() < Duration::from_secs(5), "status waited for the held actor: {observed}");
            assert_ne!(reply["error"]["code"], "outcome_unknown", "{observed}");
            assert_ne!(reply["error"]["outcome"], "unknown", "{observed}");
            match code {
                StatusCode::OK if reply["data"]["state"] == "ready" => {
                    // Only a decided Ready publication can be helped to visibility.
                    assert_eq!(held.stage, Stage::Decided, "{observed}");
                    held_ready = Some(held.txn);
                }
                StatusCode::OK => assert_eq!(reply["data"]["state"], "building", "{observed}"),
                StatusCode::SERVICE_UNAVAILABLE => {
                    assert_eq!(reply["error"]["code"], "storage_unavailable", "{observed}");
                    assert_eq!(reply["error"]["outcome"], "not_applied", "{observed}");
                    // An undecided intent cannot be helped; a decided one always can.
                    assert_eq!(held.stage, Stage::Prepared, "{observed}");
                    retryable.push(held.txn);
                }
                _ => panic!("unexpected status answer: {observed}"),
            }
            held.release.send(()).ok();
        }
        index_holds::unregister(catalog);
        drop(holds);
        // The poll that was answered retryable is the Ready publication it then helped.
        assert_eq!(retryable, held_ready.into_iter().collect::<Vec<_>>());
        let (code, resumed) = resume.await.unwrap();
        assert_eq!(code, StatusCode::OK, "{resumed}");
        let owner = MaintenanceOwnerRef {
            catalog_group_id: catalog,
            table_id: serde_json::from_value(table.clone()).unwrap(), index_id, build_id, attempt: 1,
        };
        let closed = runtime.services.catalog.participant().strong_index_attempt_closed(owner).await.unwrap();
        assert_eq!(closed.statement().kind, IndexClosedKind::Ready);
        assert_eq!(Some(closed.archive_evidence().txn), held_ready);
        let (code, observed) = admin(&app, Uuid::new_v4(), "storage.index.status", selector).await;
        assert_eq!((code, observed["data"]["state"].clone()), (StatusCode::OK, json!("ready")), "{observed}");
        let query = json!({"table_id":table,"query":{
            "select":["id","label"],"filter":{"column":"category","op":"eq","value":7},
            "order_by":null,"limit":null,
        }});
        assert!(data(&app, "query.explain", query.clone()).await.as_str().unwrap().contains("by_category"));
        assert_eq!(data(&app, "query.collect", query).await, json!([{"id":1,"label":"one"}]));
        drop((app, closed));
        runtime.stop_accepting();
        runtime.drain().await.unwrap();
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }).await.unwrap();
}
