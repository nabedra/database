//! U01/U02 and Q04-Q06 exercise the public queue routes with real compute gates.
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use meshdb_server::{
    api,
    config::ExecutionConfig,
    engine::Engine,
    executor::Executor,
    partition_size::PartitionSize,
    storage::{Namespace, Storage},
    worker_pool::WorkerPool,
};
use serde_json::{Value, json};
use std::{num::NonZeroUsize, time::Duration};
use tower::ServiceExt;
use uuid::Uuid;
#[path = "support/storage.rs"]
mod support;
use support::Fake;
async fn send(app: Router, path: &str, body: Option<Value>, token: &str) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(path)
        .header("authorization", format!("Bearer {token}"));
    let body = if let Some(value) = body {
        request = request.header("content-type", "application/json");
        Body::from(value.to_string())
    } else {
        Body::empty()
    };
    let response = app.oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn create(id: Uuid, name: &str) -> Value {
    json!({"request_id":id,"operation":"table.create","payload":{"name":name,"schema":{"id":"int64"},"key":"id","format":"parquet"}})
}
async fn terminal(app: Router, path: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, value) = send(app.clone(), path, None, "test-token").await;
            assert_eq!(status, 200);
            if matches!(
                value["state"].as_str(),
                Some("succeeded" | "failed" | "abandoned")
            ) {
                break value;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn q02_q04_f03_task_admission_precedes_acceptance_and_actor_memory_survives_disconnect() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let storage = Storage::new(Fake::default(), Namespace::new("actor-admission/").unwrap());
        let (pool, threads) = WorkerPool::start(
            NonZeroUsize::new(1).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(storage, pool.clone(), PartitionSize::default());
        let mut config = ExecutionConfig::default();
        config.max_pending_requests = 1;
        let ingress = Executor::new(1, 0).unwrap();
        let (app, tasks) = api::router_with_migration(
            engine.clone(),
            ingress.clone(),
            "test-token".into(),
            config,
            false,
            false,
        )
        .unwrap();
        let base = pool.scheduler().usage()[0].2;
        // Hold the same internal admission that a just-settled task retains
        // before Executor releases its guard. CPU and queue count are free.
        let settlement = tasks.try_admit().unwrap();
        let id = Uuid::new_v4();
        let body = create(id, "exactly_once");
        let (status, rejected) = send(
            app.clone(),
            "/v1/requests",
            Some(body.clone()),
            "test-token",
        )
        .await;
        assert_eq!(status, 429);
        assert_eq!(rejected["error"]["code"], "overloaded");
        let run = engine
            .storage()
            .load_cluster_catalog()
            .await
            .unwrap()
            .catalog()
            .fence
            .unwrap()
            .run_id;
        let path = format!("/v1/requests/{run}/{id}");
        assert_eq!(
            send(app.clone(), &path, None, "test-token").await.0,
            404,
            "no unowned accepted/proposed identity may remain"
        );
        assert_eq!(pool.scheduler().usage()[0].2, base);
        drop(settlement);
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let occupied = pool
            .submit(0, move |_, _| {
                entered.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(10)).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        waiting.await.unwrap();
        let pending_base = pool.scheduler().usage()[0].2;
        let (status, accepted) = send(
            app.clone(),
            "/v1/requests",
            Some(body.clone()),
            "test-token",
        )
        .await;
        assert_eq!(status, 202);
        assert_eq!(accepted["run_id"], run.to_string());
        drop(accepted);
        assert!(
            pool.scheduler().usage()[0].2 > pending_base,
            "pending actor request/catalog/result buffers need their own charge"
        );
        let transport = tokio::spawn(send(
            app.clone(),
            "/v1/operations",
            Some(body),
            "test-token",
        ));
        while ingress.try_admit().is_ok() {
            tokio::task::yield_now().await;
        }
        transport.abort();
        assert!(transport.await.unwrap_err().is_cancelled());
        assert!(
            pool.scheduler().usage()[0].2 > pending_base,
            "disconnect must retain actor ownership"
        );
        release.send(()).unwrap();
        occupied.receive().await.unwrap();
        assert_eq!(terminal(app.clone(), &path).await["state"], "succeeded");
        tasks.drain().await;
        ingress.drain().await;
        assert_eq!(
            engine
                .storage()
                .load_cluster_catalog()
                .await
                .unwrap()
                .catalog()
                .tables
                .len(),
            1
        );
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, base)]);
        threads.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn q04_q05_u01_accepted_work_outlives_its_transport_and_deduplicates() {
    let storage = Storage::new(Fake::default(), Namespace::new("queued-api/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let (release, gate) = std::sync::mpsc::channel();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let occupied = pool
        .submit(0, move |_, _| {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Ok(())
        })
        .await
        .unwrap();
    waiting.await.unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let (app, tasks) = api::router_with_migration(
        engine.clone(),
        Executor::new(4, 4).unwrap(),
        "test-token".into(),
        ExecutionConfig::default(),
        false,
        true,
    )
    .unwrap();
    let id = Uuid::new_v4();
    let body = create(id, "items");
    let (status, receipt) = send(
        app.clone(),
        "/v1/requests",
        Some(body.clone()),
        "test-token",
    )
    .await;
    assert_eq!(status, 202);
    let path = format!(
        "/v1/requests/{}/{}",
        receipt["run_id"].as_str().unwrap(),
        id
    );
    let (status, pending) = send(app.clone(), &path, None, "test-token").await;
    assert_eq!(status, 200);
    assert!(matches!(
        pending["state"].as_str(),
        Some("queued" | "running")
    ));
    let (status, duplicate) = send(app.clone(), "/v1/requests", Some(body), "test-token").await;
    assert_eq!(status, 202);
    assert_eq!(duplicate["run_id"], receipt["run_id"]);
    let (status, conflict) = send(
        app.clone(),
        "/v1/requests",
        Some(create(id, "other")),
        "test-token",
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(conflict["error"]["code"], "conflict");
    let (status, _) = send(app.clone(), &path, None, "different-scope").await;
    assert_eq!(status, 401);
    release.send(()).unwrap();
    occupied.receive().await.unwrap();
    let done = terminal(app, &path).await;
    assert_eq!(done["state"], "succeeded");
    assert_eq!(done["result"]["name"], "items");
    let catalog = engine.storage().load_cluster_catalog().await.unwrap();
    assert_eq!(catalog.catalog().tables.len(), 1);
    assert!(catalog.catalog().decision_head.is_some());
    tasks.stop_accepting();
    tasks.drain().await;
    threads.shutdown().await.unwrap();
}
#[tokio::test]
async fn q06_r04_receipts_reconcile_commits_and_expired_reads_are_explicit() {
    let storage = Storage::new(Fake::default(), Namespace::new("cold-queue/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let mut execution = ExecutionConfig::default();
    execution.result_retention_ms = 100;
    let (app, tasks) = api::router_with_migration(
        engine.clone(),
        Executor::new(4, 4).unwrap(),
        "test-token".into(),
        execution.clone(),
        false,
        true,
    )
    .unwrap();
    let mutation = Uuid::new_v4();
    let (_, receipt) = send(
        app.clone(),
        "/v1/requests",
        Some(create(mutation, "durable")),
        "test-token",
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    let path = format!("/v1/requests/{run}/{mutation}");
    assert_eq!(terminal(app.clone(), &path).await["state"], "succeeded");
    let read = Uuid::new_v4();
    let (_, r) = send(
        app.clone(),
        "/v1/requests",
        Some(json!({"request_id":read,"operation":"table.list","payload":{}})),
        "test-token",
    )
    .await;
    let read_path = format!("/v1/requests/{}/{read}", r["run_id"].as_str().unwrap());
    assert_eq!(
        terminal(app.clone(), &read_path).await["state"],
        "succeeded"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (status, value) = send(app, &read_path, None, "test-token").await;
    assert_eq!(status, 404);
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("expired")
    );
    tasks.stop_accepting();
    tasks.drain().await;
    let (new, tasks) = api::router_with_migration(
        engine.clone(),
        Executor::new(4, 4).unwrap(),
        "test-token".into(),
        execution,
        true,
        false,
    )
    .unwrap();
    let (status, committed) = send(new.clone(), &path, None, "test-token").await;
    assert_eq!(status, 200);
    assert_eq!(committed["state"], "succeeded");
    let (status, abandoned) = send(new, &read_path, None, "test-token").await;
    assert_eq!(status, 200);
    assert_eq!(abandoned["state"], "abandoned");
    assert!(abandoned["result"].is_null());
    assert_eq!(
        engine
            .storage()
            .load_cluster_catalog()
            .await
            .unwrap()
            .catalog()
            .tables
            .len(),
        1
    );
    tasks.stop_accepting();
    tasks.drain().await;
    threads.shutdown().await.unwrap();
}

#[tokio::test]
async fn r05_cold_fence_prevents_a_paused_local_read_from_settling_as_success() {
    let storage = Storage::new(Fake::default(), Namespace::new("fenced-read/").unwrap());
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool.clone(), PartitionSize::default());
    let (app, tasks) = api::router_with_migration(
        engine.clone(),
        Executor::new(4, 4).unwrap(),
        "test-token".into(),
        ExecutionConfig::default(),
        false,
        false,
    )
    .unwrap();
    let (status, created) = send(
        app.clone(),
        "/v1/operations",
        Some(create(Uuid::new_v4(), "items")),
        "test-token",
    )
    .await;
    assert_eq!(status, 200);
    let table = created["data"]["id"].as_str().unwrap();
    let (status,_)=send(app.clone(),"/v1/operations",Some(json!({"request_id":Uuid::new_v4(),"operation":"table.insert","payload":{"table_id":table,"row":{"id":1}}})),"test-token").await;
    assert_eq!(status, 200);
    let (release, gate) = std::sync::mpsc::channel();
    let (started, waiting) = tokio::sync::oneshot::channel();
    let occupied = pool
        .submit(0, move |_, _| {
            started.send(()).unwrap();
            gate.recv().unwrap();
            Ok(())
        })
        .await
        .unwrap();
    waiting.await.unwrap();
    let id = Uuid::new_v4();
    let (status, receipt) = send(
        app.clone(),
        "/v1/requests",
        Some(json!({"request_id":id,"operation":"table.get","payload":{"table_id":table,"key":1}})),
        "test-token",
    )
    .await;
    assert_eq!(status, 202);
    let path = format!("/v1/requests/{}/{id}", receipt["run_id"].as_str().unwrap());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, r) = send(app.clone(), &path, None, "test-token").await;
            if r["state"] == "running" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (new, new_tasks) = api::router_with_migration(
        engine.clone(),
        Executor::new(4, 4).unwrap(),
        "test-token".into(),
        ExecutionConfig::default(),
        true,
        false,
    )
    .unwrap();
    let (status, abandoned) = send(new, &path, None, "test-token").await;
    assert_eq!(status, 200);
    assert_eq!(abandoned["state"], "abandoned");
    assert!(abandoned["result"].is_null());
    release.send(()).unwrap();
    occupied.receive().await.unwrap();
    let old = terminal(app, &path).await;
    assert_eq!(old["state"], "failed");
    assert_eq!(old["error"]["code"], "outcome_unknown");
    assert_eq!(old["error"]["outcome"], "unknown");
    assert!(old["result"].is_null());
    tasks.stop_accepting();
    tasks.drain().await;
    new_tasks.stop_accepting();
    new_tasks.drain().await;
    threads.shutdown().await.unwrap();
}

#[tokio::test]
async fn a01_local_fixed_worker_explicitly_rejects_provisioning_without_changing_work() {
    let storage = Storage::new(
        Fake::default(),
        Namespace::new("manual-capability/").unwrap(),
    );
    storage.initialize_catalog().await.unwrap();
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(1).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let (app, tasks) = api::router_with_migration(
        engine,
        Executor::new(2, 0).unwrap(),
        "test-token".into(),
        ExecutionConfig::default(),
        false,
        true,
    )
    .unwrap();
    let body = json!({"request_id":Uuid::new_v4(),"operation":"cluster.autoscale","payload":{"min_nodes":1,"max_nodes":2}});
    let (status, error) = send(
        app.clone(),
        "/v1/cluster/operations",
        Some(body.clone()),
        "invalid",
    )
    .await;
    assert_eq!(status, 401);
    assert_eq!(error["error"]["code"], "unauthorized");
    let (status, error) = send(
        app.clone(),
        "/v1/cluster/operations",
        Some(body),
        "test-token",
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(error["error"]["code"], "unsupported_capability");
    assert_eq!(error["error"]["outcome"], "not_applied");
    let id = Uuid::new_v4();
    let (status, receipt) = send(
        app.clone(),
        "/v1/requests",
        Some(create(id, "after_unsupported")),
        "test-token",
    )
    .await;
    assert_eq!(status, 202);
    let path = format!(
        "/v1/requests/{}/{}",
        receipt["run_id"].as_str().unwrap(),
        id
    );
    assert_eq!(terminal(app, &path).await["state"], "succeeded");
    tasks.stop_accepting();
    tasks.drain().await;
    threads.shutdown().await.unwrap();
}
