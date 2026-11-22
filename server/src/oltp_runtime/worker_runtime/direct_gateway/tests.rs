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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o07_o13_q01_accepted_batch_waits_for_prepare_storage_lane() {
    use crate::{
        oltp_replication::{
            multiplex::TrafficClass,
            types::{TxnDecision, TxnId},
        },
        oltp_runtime::recovery_tests::index_holds::{Checkpoint, register_checkpoint},
        request_queue::RequestState,
    };

    tokio::time::timeout(Duration::from_secs(90), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-o07-prepare-lane-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, ExecutionConfig::default().direct_oltp).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let id = Uuid::new_v4();
        let batch = body(
            id,
            "table.insert_many",
            json!({
                "table_id": table,
                "rows": [
                    {"id": -1, "value": 10, "tag": "left"},
                    {"id": 1, "value": 20, "tag": "right"},
                ],
            }),
        );
        let preparing = register_checkpoint(id, Checkpoint::QueuedPrepare);
        let mut result = Box::pin(call(&app, batch.clone(), false));
        let checkpoint = tokio::select! {
            checkpoint = preparing => checkpoint.unwrap(),
            reply = &mut result => panic!("batch ended before queued Prepare: {reply:?}"),
        };
        let record = runtime.services.replica.lock().unwrap().observation(id).unwrap();
        assert!(record.accepted);
        assert_eq!(record.state, RequestState::Running);
        let client = runtime.services.manager.client();
        let storage = client.hold_outgoing_for_test(TrafficClass::Storage);
        checkpoint.release.send(false).unwrap();
        tokio::select! {
            () = client.outgoing_lane_full_for_test(TrafficClass::Storage) => {}
            reply = &mut result => panic!("batch ended with Storage held: {reply:?}"),
        }
        drop(storage);

        let original = result.await;
        assert_eq!(original, (200, json!({"request_id": id, "data": 2})));
        assert_eq!(call(&app, batch, false).await, original);
        let txn = TxnId {
            request_id: id,
            attempt: 1,
        };
        let committed = runtime.services.status.inspect_transaction(txn).await.unwrap().unwrap();
        assert!(matches!(committed.decision.unwrap().decision, TxnDecision::Commit { .. }));
        assert!(runtime.services.status.inspect_transaction(TxnId { attempt: 2, ..txn })
            .await.unwrap().is_none());
        for (key, value) in [(-1, 10), (1, 20)] {
            let key_read = body(Uuid::new_v4(), "table.get", json!({"table_id":table,"key":key}));
            let (status, read) = call(&app, key_read, true).await;
            assert_eq!(status, 200, "{read}");
            assert_eq!(read["data"]["value"], value);
        }
        runtime.drain().await.unwrap();
    }).await.expect("queued Prepare saturation watchdog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d03_q05_bug1_direct_retry_without_content_length_replays_original_result() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-bug1-streamed-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, true).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let id = Uuid::new_v4();
        let insert = |value| {
            body(
                id,
                "table.insert",
                json!({
                    "table_id": table, "row": {"id": 7, "value": value, "tag": "a"},
                }),
            )
        };
        let original = call(&app, insert(1), true).await;
        assert_eq!(original, (200, json!({"request_id": id, "data": null})));
        let replay = call(&app, insert(1), false).await;
        let conflict = call(&app, insert(2), false).await;
        assert_eq!(conflict.0, 409, "{conflict:?}");
        assert_eq!(conflict.1["error"]["code"], "conflict");
        let get = body(
            Uuid::new_v4(),
            "table.get",
            json!({"table_id": table, "key": 7}),
        );
        assert_eq!(call(&app, get, true).await.1["data"]["value"], 1);
        runtime.drain().await.unwrap();
        assert_eq!(replay, original);
    })
    .await
    .expect("streamed retry watchdog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d10_o10_bug1_reply_capacity_after_apply_cannot_report_not_applied() {
    use crate::oltp::mvcc::store::ParticipantHandle;
    use crate::oltp::pool::{ByteClass, ChargedBuffer};
    use crate::oltp_replication::authority::control::ServiceAction;
    use crate::oltp_replication::multiplex::EndpointFuture;
    use crate::oltp_replication::service_rpc::direct::{
        self, DirectLink, DirectRequest, DirectResponse, DirectSendError, DirectService,
    };
    use sha2::{Digest as _, Sha256};
    use std::sync::Mutex;

    struct ReplyPressure {
        service: Arc<dyn DirectService>,
        id: Uuid,
        hold: Mutex<Option<ChargedBuffer>>,
    }
    impl DirectService for ReplyPressure {
        fn execute<'a>(
            &'a self,
            handle: &'a Arc<ParticipantHandle>,
            request: DirectRequest,
        ) -> EndpointFuture<'a, DirectResponse> {
            Box::pin(async move {
                let inject = request.request_id == self.id;
                let response = self.service.execute(handle, request).await?;
                if inject {
                    assert!(matches!(response, DirectResponse::Done(_)), "{response:?}");
                    let shared = handle.resources();
                    let config = shared.config();
                    let free = config.control_bytes
                        - config.progress_bytes
                        - shared.buffer_bytes_used(ByteClass::Data);
                    let hold = shared.allocate_bytes(ByteClass::Data, free)?;
                    assert_eq!(
                        shared.buffer_bytes_used(ByteClass::Data),
                        config.control_bytes - config.progress_bytes
                    );
                    *self.hold.lock().unwrap() = Some(hold);
                }
                Ok(response)
            })
        }

        fn forward<'a>(
            &'a self,
            handle: &'a Arc<ParticipantHandle>,
            request: DirectRequest,
        ) -> EndpointFuture<'a, DirectResponse> {
            self.service.forward(handle, request)
        }

        fn stats(&self) -> Value {
            self.service.stats()
        }
    }

    tokio::time::timeout(Duration::from_secs(90), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-bug1-reply-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, true).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let get = || {
            body(
                Uuid::new_v4(),
                "table.get",
                json!({"table_id": table, "key": 7}),
            )
        };
        assert_eq!(call(&app, get(), true).await.0, 200);
        let table_id = serde_json::from_value(table.clone()).unwrap();
        let route = runtime
            .direct_gateway_handle()
            .unwrap()
            .cache()
            .get(table_id, Some(&crate::protocol::Key::Int64(7)))
            .unwrap();
        let range = &route.range;
        let shared = &runtime.services.shared;
        let id = Uuid::new_v4();
        let insert = body(
            id,
            "table.insert",
            json!({
                "table_id": table, "row": {"id": 7, "value": 1, "tag": "a"},
            }),
        );
        let request = DirectRequest {
            database: runtime.services.scope.database.clone(),
            table_id,
            schema_epoch: route.schema_epoch,
            routing_epoch: range.shard.routing_epoch,
            group: range.group().group_id,
            request_id: id,
            digest: Digest::from_bytes(Sha256::digest(&insert).into()),
            body: shared.copy_bytes(&insert).unwrap(),
            deadline: Duration::from_secs(5),
        };
        let link = DirectLink::new(
            range.group().clone(),
            shared.clone(),
            runtime.services.manager.client(),
            runtime.services.control_client.clone(),
            ServiceAction::DirectLeader,
        )
        .unwrap();
        let pressure = Arc::new(ReplyPressure {
            service: direct::installed(shared).unwrap(),
            id,
            hold: Mutex::new(None),
        });
        direct::install(shared, &(pressure.clone() as Arc<dyn DirectService>));
        let result = link
            .send(
                range.leader_hint().unwrap(),
                runtime.services.local.worker_id,
                &request,
                false,
            )
            .await;
        direct::install(shared, &pressure.service);
        assert!(
            pressure.hold.lock().unwrap().take().is_some(),
            "post-apply hold reached"
        );
        drop(request);
        drop(link);
        drop(route);
        assert_eq!(call(&app, get(), true).await.1["data"]["value"], 1);
        assert_eq!(
            call(&app, insert, true).await,
            (200, json!({"request_id": id, "data": null}))
        );
        let changed = body(
            id,
            "table.insert",
            json!({
                "table_id": table, "row": {"id": 7, "value": 2, "tag": "a"},
            }),
        );
        let conflict = call(&app, changed, true).await;
        assert_eq!(conflict.0, 409, "{conflict:?}");
        runtime.drain().await.unwrap();
        let Err(DirectSendError::Uncertain(error)) = result else {
            panic!("{result:?}");
        };
        assert!(
            error.message.contains("encode class=Data requested="),
            "{error:?}"
        );
    })
    .await
    .expect("reply pressure watchdog");
}

/// D01/O03/O02 on one worker: every eligible operation's results, errors
/// and types over the direct path equal the queued path's for the same
/// program; the topology is read once per table. The baseline worker runs
/// with `direct_oltp: false`: its mutations are settled at the anchor and
/// executed queued, nothing runs directly. Gets are never settled (user
/// decision 2026-11-21, RV-FINAL M3).
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
        let mutations = program(&table, 0)
            .iter()
            .filter(|(operation, _)| *operation != "table.get")
            .count() as u64;
        // The gateway and then the queue owner settle each mutation.
        assert!(gateway.forwards <= 2 * mutations, "no get settled: {gateway:?}");
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

        // A body without a declared length still takes the queued path with
        // the full parsing charge. Both the gateway and queue owner settle
        // eligible mutations, but only the first writes the marker.
        let markers = leader()["forward_markers_written"].as_u64().unwrap();
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
        let after = gateway.stats();
        assert_eq!(after.direct_done, stats.direct_done, "{after:?}");
        assert!(after.forwards >= 8, "{after:?}");
        assert!(after.forwards - stats.forwards <= 2 * mutations, "no get settled: {after:?}");
        let written = leader()["forward_markers_written"].as_u64().unwrap() - markers;
        assert!((8..=mutations).contains(&written), "{}", leader());

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
        let _guard = catalog
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
        runtime.drain().await.unwrap();
    })
    .await
    .expect("rf1 guard watchdog");
}

/// One measurement line that libtest does not capture, so CI logs show it
/// for a passing test too (it writes to the stderr handle, not `eprintln!`).
fn report(line: &str) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{line}");
}

/// O15/D03, RV-FINAL M3 (user decision 2026-11-21): in rollback mode
/// (`direct_oltp: false` while `data.direct.v1` is active) a queued get has
/// no exactly-once identity of its own. Through the gateway (declared
/// length or streamed body) it proposes no
/// Data entry and writes no forward marker, so no dedup record or expiry
/// key. A mutation in the same mode is still settled (one marker). The run
/// reports per-get Data entries, forwards, journal bytes and wall time; the
/// same test on the previous rule (`fb20718`) recorded the baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o15_d03_m3_rollback_queued_gets_propose_no_data_entry() {
    const GETS: usize = 200;
    tokio::time::timeout(Duration::from_secs(240), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-m3-rollback-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, false).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let (status, inserted) = call(
            &app,
            body(
                Uuid::new_v4(),
                "table.insert",
                json!({"table_id": table, "row": {"id": 1, "value": 10, "tag": "a"}}),
            ),
            true,
        )
        .await;
        assert_eq!(status, 200, "{inserted}");
        let snapshot = || async {
            let reply = runtime.stats().await.unwrap();
            let stats: Value = serde_json::from_slice(&reply.body).unwrap();
            let count = |value: &Value| value.as_u64().unwrap_or(0);
            (
                count(&stats["direct"]["entries_proposed"]),
                count(&stats["direct"]["forward_markers_written"]),
                count(&stats["direct_gateway"]["forwards"]),
                count(&stats["storage"]["journal_bytes"]),
                count(&stats["storage"]["commits"]),
            )
        };
        let before = snapshot().await;
        let started = std::time::Instant::now();
        for index in 0..GETS {
            let (status, envelope) = call(
                &app,
                body(
                    Uuid::new_v4(),
                    "table.get",
                    json!({"table_id": table, "key": 1}),
                ),
                index % 2 == 0,
            )
            .await;
            assert_eq!(status, 200, "{envelope}");
            assert_eq!(
                envelope["data"],
                json!({"id": 1, "value": 10, "tag": "a"}),
                "{envelope}"
            );
        }
        let elapsed = started.elapsed();
        let after = snapshot().await;
        let (entries, markers, forwards, journal, commits) = (
            after.0 - before.0,
            after.1 - before.1,
            after.2 - before.2,
            after.3 - before.3,
            after.4 - before.4,
        );
        report(&format!(
            "M3 rollback-mode queued gets: {GETS} gets, {:.3} ms/get, Data entries {} \
             ({:.2}/get: DirectCommit {entries}, DirectForward markers {markers}), forward RPCs \
             {forwards}, journal bytes {journal} ({:.0}/get), store commits {commits} ({:.2}/get)",
            elapsed.as_secs_f64() * 1e3 / GETS as f64,
            entries + markers,
            (entries + markers) as f64 / GETS as f64,
            journal as f64 / GETS as f64,
            commits as f64 / GETS as f64,
        ));
        assert_eq!((entries, markers, forwards), (0, 0, 0));
        // Mutations keep the T7c rule in the same mode: settled, one marker.
        let (status, updated) = call(
            &app,
            body(
                Uuid::new_v4(),
                "table.update",
                json!({"table_id": table, "key": 1, "values": {"value": 11}}),
            ),
            true,
        )
        .await;
        assert_eq!((status, &updated["data"]), (200, &json!(true)), "{updated}");
        let settled = snapshot().await;
        assert_eq!(settled.1 - after.1, 1, "one forward marker");
        assert_eq!(
            settled.2 - after.2,
            2,
            "the gateway's forward, then the queue owner's"
        );
        runtime.drain().await.unwrap();
    })
    .await
    .expect("m3 rollback watchdog");
}

/// D01 (PERF2b item 3) on one worker over mTLS: the leader frames a small
/// direct reply, and the gateway checks and decodes it, on their own tasks.
/// Before, these were three Control-class native jobs per request (the
/// leader's reply frame, the gateway's header decode and its digest and
/// decode budget); the leader's get is one Query-class job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d01_perf2b_rf1_gateway_decodes_a_small_reply_without_native_jobs() {
    use crate::oltp::pool::ExecutionClass;
    const GETS: u64 = 20;
    tokio::time::timeout(Duration::from_secs(240), async {
        let directory = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-perf2b-gw-{}", Uuid::new_v4())),
        );
        let runtime = open(&directory.0, true).await;
        activated(&runtime).await;
        let app = runtime.application(TOKEN.into()).unwrap();
        let table = create(&app).await;
        let (status, inserted) = call(
            &app,
            body(
                Uuid::new_v4(),
                "table.insert",
                json!({"table_id": table, "row": {"id": 1, "value": 10, "tag": "a"}}),
            ),
            true,
        )
        .await;
        assert_eq!(status, 200, "{inserted}");
        let get = || {
            body(
                Uuid::new_v4(),
                "table.get",
                json!({"table_id": table, "key": 1}),
            )
        };
        // Warm the topology cache and the session.
        assert_eq!(call(&app, get(), true).await.0, 200);
        // These tasks share the native job counters with the mTLS request.
        // The retirement task also runs the index reclamation sweep. Join
        // both tasks after stopping them so a slow runner cannot count a
        // periodic inventory scan or enrollment refresh as reply decoding.
        let retirement = runtime.retirement.lock().unwrap().take().unwrap();
        retirement.abort();
        assert!(retirement.await.unwrap_err().is_cancelled());
        runtime.services.stop_directory_refresh_for_test().await;
        let gateway = runtime.direct_gateway_handle().unwrap().clone();
        let before = gateway.stats();
        let shared = runtime.services.shared.clone();
        let jobs = || {
            (
                shared.native_jobs(ExecutionClass::Query),
                shared.native_jobs(ExecutionClass::Control),
            )
        };
        let (query, control) = jobs();
        for _ in 0..GETS {
            let (status, envelope) = call(&app, get(), true).await;
            assert_eq!(status, 200, "{envelope}");
            assert_eq!(envelope["data"]["value"], json!(10), "{envelope}");
        }
        let (query_after, control_after) = jobs();
        let after = gateway.stats();
        assert_eq!(after.direct_done - before.direct_done, GETS);
        assert_eq!(after.catalog_reads, before.catalog_reads);
        assert_eq!(after.placement_reads, before.placement_reads);
        eprintln!(
            "perf2b rf1: {GETS} gets took {} Query and {} Control jobs",
            query_after - query,
            control_after - control
        );
        // Each native reply codec job would contribute one job per GET.
        assert!(
            control_after - control < GETS / 2,
            "Control jobs for {GETS} gets: {}",
            control_after - control
        );
        runtime.drain().await.unwrap();
    })
    .await
    .expect("rf1 gateway watchdog");
}
