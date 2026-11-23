#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use nabedra_database_server::{
    api,
    engine::Engine,
    executor::Executor,
    protocol::MAX_BODY_BYTES,
    storage::{Namespace, Storage},
};
use nabedra_database_server::{
    partition_size::PartitionSize,
    worker_pool::{WorkerPool, WorkerThreads},
};
use serde_json::{Value, json};
use std::num::NonZeroUsize;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tower::ServiceExt;
#[path = "support/storage.rs"]
mod support;
use support::{Fake, ReadGate};
const ID: &str = "11111111-1111-4111-8111-111111111111";

async fn fixture(queue: usize) -> (Router, Engine<Fake>, Executor, WorkerThreads) {
    let storage = Storage::new(Fake::default(), Namespace::new("api/").unwrap());
    storage.initialize_catalog().await.unwrap();
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    let executor = Executor::new(1, queue).unwrap();
    (
        api::router_with_migration(
            engine.clone(),
            executor.clone(),
            "test-token".into(),
            Default::default(),
            false,
            true,
        )
        .unwrap()
        .0,
        engine,
        executor,
        workers,
    )
}
fn wire(operation: &str, payload: Value) -> String {
    wire_with_id(ID, operation, payload)
}
fn wire_with_id(id: &str, operation: &str, payload: Value) -> String {
    json!({"request_id":id,"operation":operation,"payload":payload}).to_string()
}
fn request(body: Body, token: Option<&str>, content_type: Option<&str>) -> Request<Body> {
    let mut builder = Request::post("/v1/operations");
    if let Some(token) = token {
        builder = builder.header("authorization", token);
    }
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder.body(body).unwrap()
}
async fn send(app: Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(bytes.len() <= MAX_BODY_BYTES);
    (status, serde_json::from_slice(&bytes).unwrap())
}
async fn json_request(app: Router, body: String) -> (StatusCode, Value) {
    send(
        app,
        request(
            Body::from(body),
            Some("Bearer test-token"),
            Some("application/json; charset=utf-8"),
        ),
    )
    .await
}
fn error(value: &Value, code: &str, id: Value) {
    assert_eq!(value["request_id"], id);
    assert_eq!(value["error"]["code"], code);
    assert_eq!(value["error"]["outcome"], "not_applied");
}

#[tokio::test]
async fn framing_protocol_errors_and_strict_request_ids() {
    let (app, engine, _, workers) = fixture(0).await;
    let schema_id = uuid::Uuid::new_v4().to_string();
    let missing_id = uuid::Uuid::new_v4().to_string();
    let (status, value) = json_request(app.clone(), wire("table.list", json!({}))).await;
    assert_eq!(status, 200);
    assert_eq!(value, json!({"request_id":ID,"data":[]}));
    for (body, code, id) in [
        (
            wire("unknown", json!({})),
            "unsupported_operation",
            json!(ID),
        ),
        (
            wire_with_id(
                &schema_id,
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"missing","format":"parquet"}),
            ),
            "validation_error",
            json!(schema_id),
        ),
        (
            wire_with_id(&missing_id, "table.describe", json!({"name":"missing"})),
            "not_found",
            json!(missing_id),
        ),
        (
            format!(
                r#"{{"request_id":"{ID}","request_id":"{ID}","operation":"table.list","payload":{{}}}}"#
            ),
            "validation_error",
            Value::Null,
        ),
        (
            format!(
                r#"{{"request_id":"{ID}","operation":"table.list","operation":"table.list","payload":{{}}}}"#
            ),
            "validation_error",
            Value::Null,
        ),
        (
            r#"{"request_id":12,"operation":"table.list","payload":{}}"#.into(),
            "validation_error",
            Value::Null,
        ),
        ("{".into(), "validation_error", Value::Null),
        (
            format!(
                r#"{{"request_id":"{ID}","operation":"table.describe","payload":{{"name":"items","name":"other"}}}}"#
            ),
            "validation_error",
            json!(ID),
        ),
        (
            wire("table.list", json!({"extra":true})),
            "validation_error",
            json!(ID),
        ),
    ] {
        let expected = nabedra_database_server::protocol::parse_request(body.as_bytes()).err();
        let (status, value) = json_request(app.clone(), body).await;
        assert_eq!(status.as_u16(), if code == "not_found" { 404 } else { 400 });
        error(&value, code, id);
        if let Some(expected) = expected {
            assert_eq!(value["error"], serde_json::to_value(expected).unwrap());
        } else {
            assert_eq!(value["error"]["message"], "table not found");
        }
    }
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    for ct in [
        None,
        Some("text/plain"),
        Some("application/jsonx"),
        Some("application/json; broken"),
    ] {
        let (status, value) = send(
            app.clone(),
            request(
                Body::from(wire("table.list", json!({}))),
                Some("Bearer test-token"),
                ct,
            ),
        )
        .await;
        assert_eq!(status, 400);
        error(&value, "validation_error", Value::Null);
    }
    assert_eq!(engine.storage().backend().state.lock().unwrap().gets, gets);
    workers.shutdown().await.unwrap();
}

struct Frames {
    chunks: VecDeque<Result<bytes::Bytes, std::io::Error>>,
    polls: Arc<AtomicUsize>,
}
impl http_body::Body for Frames {
    type Data = bytes::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(
            self.chunks
                .pop_front()
                .map(|v| v.map(http_body::Frame::data)),
        )
    }
}
fn frames(chunks: Vec<Result<bytes::Bytes, std::io::Error>>) -> (Body, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    (
        Body::new(Frames {
            chunks: chunks.into(),
            polls: polls.clone(),
        }),
        polls,
    )
}
fn counted_body() -> (Body, Arc<AtomicUsize>) {
    frames(vec![Ok(wire("table.list", json!({})).into())])
}

#[tokio::test]
async fn inclusive_body_limit_and_unreadable_frames() {
    let (app, engine, _, workers) = fixture(0).await;
    let bytes = wire("table.list", json!({}));
    for (extra, expected) in [(0, 200), (1, 413)] {
        let (body, _) = frames(vec![
            Ok(bytes.clone().into()),
            Ok(vec![b' '; MAX_BODY_BYTES - bytes.len() + extra].into()),
        ]);
        let mut req = request(body, Some("Bearer test-token"), Some("application/json"));
        req.headers_mut()
            .insert("content-length", "1".parse().unwrap());
        let (status, value) = send(app.clone(), req).await;
        assert_eq!(status, expected);
        if extra != 0 {
            error(&value, "limit_exceeded", Value::Null);
        }
    }
    let gets = engine.storage().backend().state.lock().unwrap().gets;
    let (body, _) = frames(vec![Err(std::io::Error::other(
        "private transport diagnostic",
    ))]);
    let (status, value) = send(
        app,
        request(body, Some("Bearer test-token"), Some("application/json")),
    )
    .await;
    assert_eq!(status, 400);
    error(&value, "validation_error", Value::Null);
    assert!(!value.to_string().contains("private"));
    assert_eq!(engine.storage().backend().state.lock().unwrap().gets, gets);
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn authentication_overload_and_queue_do_not_poll_bodies_early() {
    let (app, _, executor, workers) = fixture(1).await;
    let gate = Arc::new(ReadGate::default());
    let worker_gate = gate.clone();
    let running = executor
        .try_admit()
        .unwrap()
        .spawn(move || async move {
            worker_gate.entered.add_permits(1);
            worker_gate.release.acquire().await.unwrap().forget();
            Ok(())
        })
        .unwrap();
    gate.entered.acquire().await.unwrap().forget();
    for token in [None, Some("Bearer wrong")] {
        let (body, polls) = counted_body();
        let (status, value) =
            send(app.clone(), request(body, token, Some("application/json"))).await;
        assert_eq!(status, 401);
        error(&value, "unauthorized", Value::Null);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let (body, queued_polls) = counted_body();
    let mut queued = Box::pin(app.clone().oneshot(request(
        body,
        Some("Bearer test-token"),
        Some("application/json"),
    )));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(queued_polls.load(Ordering::SeqCst), 0);
    let (body, polls) = counted_body();
    let (status, value) = send(
        app.clone(),
        request(body, Some("Bearer test-token"), Some("application/json")),
    )
    .await;
    assert_eq!(status, 429);
    error(&value, "overloaded", Value::Null);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    gate.release.add_permits(1);
    running.receive().await.unwrap();
    assert_eq!(queued.await.unwrap().status(), 200);
    assert!(queued_polls.load(Ordering::SeqCst) > 0);
    executor.stop_accepting();
    let (body, polls) = counted_body();
    assert_eq!(
        send(
            app,
            request(body, Some("Bearer test-token"), Some("application/json"))
        )
        .await
        .0,
        429
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_handler_retains_mutation_and_drain_until_completion() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let (app, engine, executor, workers) = fixture(0).await;
        let gate = Arc::new(ReadGate::default());
        engine.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
        let create = wire(
            "table.create",
            json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
        );
        let handler = tokio::spawn(app.oneshot(request(
            Body::from(create),
            Some("Bearer test-token"),
            Some("application/json"),
        )));
        gate.entered.acquire().await.unwrap().forget();
        handler.abort();
        assert!(handler.await.unwrap_err().is_cancelled());
        assert!(executor.try_admit().is_err());
        let mut drain = Box::pin(executor.drain());
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        gate.release.add_permits(1);
        drain.await;
        let loaded = engine.storage().load_cluster_catalog().await.unwrap();
        assert_eq!(loaded.catalog().revision, 1);
        assert!(loaded.catalog().tables.contains_key("items"));
        let decision = engine
            .storage()
            .lookup_decision_by_id(
                &loaded,
                uuid::Uuid::parse_str(ID).unwrap(),
                nabedra_database_server::storage::HistoryBudget::new(
                    8,
                    64 * 1024,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decision.response_status, 200);
        workers.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
