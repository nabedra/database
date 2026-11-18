use super::*;
use crate::oltp::mvcc::index::IndexDefinition;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;

const DATA: &str = "query-plan-data";
const ADMIN: &str = "query-plan-admin";

async fn request(
    app: &Router,
    path: &str,
    token: &str,
    operation: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let body = json!({"request_id":Uuid::new_v4(), "operation":operation, "payload":payload});
    let reply = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("authorization", format!("Bearer {token}"))
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
    let (status, reply) = request(app, "/v1/operations", DATA, operation, payload).await;
    assert_eq!(status, StatusCode::OK, "{operation}: {reply}");
    reply["data"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o03_o04_native_query_plan_residual_order_and_operator_observations() {
    tokio::time::timeout(std::time::Duration::from_secs(90), async {
        let path = std::env::temp_dir().join(format!("nabedra-query-plan-{}", Uuid::new_v4()));
        let runtime = WorkerRuntime::open(
            &path, "query_plan", DATA,
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
            "name":"items", "schema":{"id":"int64","label":"utf8","score":"float64"},
            "key":"id", "format":"parquet",
        })).await;
        let table_id = &table["id"];
        assert_eq!(data(&app, "table.insert_many", json!({"table_id":table_id,"rows":[
            {"id":i64::MIN,"label":"private-point-literal","score":2.0},
            {"id":-2,"label":"same","score":-0.0},
            {"id":-1,"label":"other","score":0.0},
            {"id":0,"label":"hidden","score":3.0},
            {"id":1,"label":"same","score":5.0},
            {"id":i64::MAX,"label":"same","score":6.0},
        ]})).await, json!(6));

        let point = json!({"table_id":table_id,"query":{
            "select":["id"],"order_by":null,"limit":null,
            "filter":{"op":"and","args":[
                {"column":"id","op":"eq","value":i64::MIN},
                {"column":"label","op":"eq","value":"private-point-literal"},
            ]},
        }});
        let explain = data(&app, "query.explain", point.clone()).await;
        let explain = explain.as_str().unwrap();
        assert!(explain.contains("access primary point seek on id\nscope shards 1; owning shard "));
        assert!(explain.contains("merge none; one candidate"));
        assert!(explain.contains("Catalog leader quorum/applied barrier; Data leader quorum/applied barrier"));
        assert_eq!(data(&app, "query.collect", point.clone()).await, json!([{"id":i64::MIN}]));
        let mut miss = point;
        miss["query"]["filter"]["args"][1]["value"] = json!("another-private-literal");
        assert_eq!(data(&app, "query.collect", miss).await, json!([]));

        let range = json!({"table_id":table_id,"query":{
            "select":["id"],"order_by":{"column":"score","desc":true},"limit":2,
            "filter":{"op":"and","args":[
                {"column":"id","op":"ge","value":-2},
                {"column":"id","op":"lt","value":1},
                {"op":"or","args":[
                    {"column":"label","op":"eq","value":"same"},
                    {"op":"not","arg":{"column":"label","op":"eq","value":"hidden"}},
                ]},
            ]},
        }});
        let explain = data(&app, "query.explain", range.clone()).await;
        let explain = explain.as_str().unwrap();
        assert!(explain.contains("access primary range seek on id\nscope shards 1; primary-key lower inclusive, upper exclusive; linked routing pages read 1"));
        assert!(explain.contains("merge bounded top-k (2 candidates); score descending; primary-key ascending ties"));
        assert_eq!(data(&app, "query.collect", range.clone()).await, json!([{"id":-2},{"id":-1}]));
        let mut limited_count = range;
        limited_count["query"]["limit"] = json!(1);
        assert_eq!(data(&app, "query.count", limited_count).await, json!(1));

        let empty = json!({"table_id":table_id,"query":{
            "select":null,"order_by":null,"limit":null,
            "filter":{"op":"and","args":[
                {"column":"id","op":"ge","value":i64::MAX},
                {"column":"id","op":"lt","value":i64::MAX},
            ]},
        }});
        let explain = data(&app, "query.explain", empty.clone()).await;
        assert!(explain.as_str().unwrap().contains("access empty result\nscope shards 0\nresidual none\nmerge none"));
        assert_eq!(data(&app, "query.collect", empty).await, json!([]));

        let scan = json!({"table_id":table_id,"query":{
            "select":["id"],"order_by":null,"limit":null,
            "filter":{"column":"label","op":"eq","value":"private-point-literal"},
        }});
        let explain = data(&app, "query.explain", scan.clone()).await;
        assert!(explain.as_str().unwrap().contains("access primary scan on id\nscope shards 1; primary-key lower unbounded, upper unbounded"));
        assert_eq!(data(&app, "query.collect", scan.clone()).await, json!([{"id":i64::MIN}]));

        let endpoint = "/v1/cluster/operations";
        let (denied, _) = request(&app, endpoint, DATA, "query.statistics", json!({})).await;
        assert!(matches!(denied, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN));
        let (allowed, report) = request(&app, endpoint, ADMIN, "query.statistics", json!({})).await;
        assert_eq!(allowed, StatusCode::OK, "{report}");
        assert!(!report.to_string().contains("private-"));
        let stats = &report["data"]["statistics"];
        assert_eq!(stats["executions"], 6);
        let shapes = stats["shapes"].as_array().unwrap();
        assert_eq!(shapes.len(), 5);
        let point = shapes.iter().find(|entry| entry["plans"]["point"] == 2).unwrap();
        assert_eq!(point["shard_visits"], 2);
        let empty = shapes.iter().find(|entry| entry["plans"]["empty"] == 1).unwrap();
        assert_eq!(empty["shard_visits"], 0);
        let scan_observation = shapes.iter().find(|entry| entry["plans"]["primary_scan"] == 1).unwrap();
        assert_eq!(scan_observation["consider_index_on"], "label");
        for range in shapes.iter().filter(|entry| entry["plans"]["primary_range"] == 1) {
            assert_eq!(range["shard_visits"], 1);
            assert!(range.get("consider_index_on").is_none());
        }

        let table_uuid = serde_json::from_value(table_id.clone()).unwrap();
        runtime.create_index(IndexDefinition {
            index_id: Uuid::new_v4(), table_id: table_uuid,
            name: Text::try_from("by_label").unwrap(),
            columns: Page::new(vec![Text::try_from("label").unwrap()]).unwrap(),
            covering: Page::new(Vec::new()).unwrap(),
        }, Uuid::new_v4(), 1).await.unwrap();
        let covering = data(&app, "query.explain", scan.clone()).await;
        assert!(covering.as_str().unwrap().contains("access secondary by_label seek; composite-prefix 1; lower unbounded; upper unbounded; covering true\nscope shards 1"));
        assert_eq!(data(&app, "query.collect", scan.clone()).await, json!([{"id":i64::MIN}]));
        let mut lookup = scan;
        lookup["query"]["select"] = json!(null);
        let noncover = data(&app, "query.explain", lookup.clone()).await;
        assert!(noncover.as_str().unwrap().contains("access secondary by_label seek; composite-prefix 1; lower unbounded; upper unbounded; covering false\nscope shards 1"));
        assert_eq!(data(&app, "query.collect", lookup).await, json!([{
            "id":i64::MIN, "label":"private-point-literal", "score":2.0,
        }]));
        let (allowed, report) = request(&app, endpoint, ADMIN, "query.statistics", json!({})).await;
        assert_eq!(allowed, StatusCode::OK, "{report}");
        let stats = &report["data"]["statistics"];
        assert_eq!(stats["executions"], 8);
        let shapes = stats["shapes"].as_array().unwrap();
        let improved = shapes.iter().find(|entry| entry["plans"]["covering_index"] == 1).unwrap();
        assert_eq!(improved["plans"]["primary_scan"], 1);
        assert!(improved.get("consider_index_on").is_none());
        assert!(shapes.iter().any(|entry| entry["plans"]["secondary_index"] == 1));

        runtime.drain().await.unwrap();
        drop(app);
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }).await.expect("native query plan watchdog");
}
