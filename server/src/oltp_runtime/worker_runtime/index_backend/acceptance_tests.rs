use super::*;
use crate::oltp::mvcc::{catalog::RouteRead, store::ParticipantHandle};
use crate::oltp_runtime::recovery_tests::index_holds::{self, BuildHold, Checkpoint};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tower::ServiceExt;

const TOKEN: &str = "index-test";
const ADMIN: &str = "index-acceptance-admin";
const WAIT: Duration = Duration::from_secs(60);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-o12-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    runtime: Arc<WorkerRuntime>,
    app: Router,
    definition: IndexDefinition,
    build_id: Uuid,
    dir: Directory,
}

impl Fixture {
    async fn new() -> Self {
        let dir = Directory::new();
        let runtime = open(&dir.0, false).await;
        let app = runtime
            .application_with_admin(TOKEN.into(), ADMIN.into())
            .unwrap();
        let table = tests::call(
            &app,
            "table.create",
            json!({
                "name":"items", "schema":{"id":"int64","category":"int64","label":"utf8"},
                "key":"id", "format":"parquet",
            }),
        )
        .await;
        let definition = serde_json::from_value(json!({
            "index_id":Uuid::new_v4(), "table_id":table["id"], "name":"by_category",
            "columns":["category"], "covering":["label"],
        }))
        .unwrap();
        Self {
            runtime,
            app,
            definition,
            build_id: Uuid::new_v4(),
            dir,
        }
    }

    async fn declare(&self) -> IndexRecord {
        let occupied = self.runtime.index_builds.try_admit().unwrap();
        let (status, body) = self
            .admin(
                self.build_id,
                "storage.index.create",
                json!(self.definition),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"]["state"], "building");
        drop(occupied);
        self.record().await
    }

    async fn record(&self) -> IndexRecord {
        let pin = self
            .runtime
            .services
            .catalog
            .snapshot(
                ReadTs::new(u64::MAX).unwrap(),
                self.runtime.index_authority(None).await.unwrap(),
            )
            .unwrap();
        let read = self
            .runtime
            .services
            .catalog
            .read::<IndexRecord>(
                &pin,
                crate::oltp::mvcc::MvccKey::CatalogIndex {
                    table_id: self.definition.table_id,
                    index_id: self.definition.index_id,
                },
            )
            .await
            .unwrap();
        match read {
            crate::oltp::mvcc::catalog::CatalogRead::Value {
                record: Some(record),
                ..
            } => (*record).clone(),
            _ => panic!("index record missing or undecided"),
        }
    }

    async fn admin(&self, id: Uuid, op: &str, payload: Value) -> (StatusCode, Value) {
        post(&self.app, "/v1/cluster/operations", ADMIN, id, op, payload).await
    }

    fn selector(&self, attempt: u64) -> Value {
        json!({
            "table_id":self.definition.table_id, "index_id":self.definition.index_id,
            "build_id":self.build_id, "attempt":attempt,
        })
    }

    async fn point(&self, key: i64) -> Value {
        tests::call(
            &self.app,
            "table.get",
            json!({"table_id":self.definition.table_id,"key":key}),
        )
        .await
    }

    fn spawn_resume(
        &self,
        record: IndexRecord,
    ) -> tokio::task::JoinHandle<Result<(), ProtocolError>> {
        let runtime = self.runtime.clone();
        tokio::spawn(async move { runtime.resume_index(&record).await })
    }

    async fn participant(&self) -> Arc<ParticipantHandle> {
        let pin = self
            .runtime
            .services
            .catalog
            .snapshot(
                ReadTs::new(u64::MAX).unwrap(),
                self.runtime.index_authority(None).await.unwrap(),
            )
            .unwrap();
        let RouteRead::Route { page, index } = self
            .runtime
            .services
            .catalog
            .route(&pin, self.definition.table_id, &protocol::Key::Int64(1))
            .await
            .unwrap()
        else {
            panic!("table has no owning range")
        };
        self.runtime
            .services
            .resolver
            .participant(&page.shards[index].group)
            .await
            .unwrap()
    }

    async fn close(self) {
        self.runtime.drain().await.unwrap();
    }
}

async fn open(path: &Path, cold: bool) -> Arc<WorkerRuntime> {
    let runtime = WorkerRuntime::open_configured(
        path,
        TOKEN,
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
        cold,
    )
    .await
    .unwrap();
    let switch = runtime.direct_switch().unwrap();
    tokio::time::timeout(WAIT, async {
        while switch.direct_stats().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("direct capability and executor must activate");
    assert!(
        !switch.history_settling(),
        "the fixture must allow direct mutations"
    );
    runtime
}

async fn post(
    app: &Router,
    path: &str,
    token: &str,
    id: Uuid,
    operation: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let request = Request::post(path)
        .header("authorization", format!("Bearer {token}"))
        .header("x-nabedra-database", TOKEN)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"request_id":id,"operation":operation,"payload":payload}).to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), protocol::MAX_BODY_BYTES)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn reached(receiver: tokio::sync::oneshot::Receiver<BuildHold>) -> BuildHold {
    tokio::time::timeout(WAIT, receiver)
        .await
        .expect("index actor did not reach checkpoint")
        .expect("index checkpoint closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o12_failed_ready_validation_preserves_direct_schema() {
    let fixture = Fixture::new().await;
    let record = fixture.declare().await;
    let data = fixture.participant().await;
    let before = data.epochs().await.unwrap().direct_schema;
    let hold =
        index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Validation);
    let build = fixture.spawn_resume(record);
    reached(hold).await.release.send(true).unwrap();
    let failed = build.await.unwrap().unwrap_err();
    assert_eq!(failed.message, "index fixture rejected Ready validation");
    assert_eq!(
        data.epochs().await.unwrap().direct_schema,
        before,
        "validation failed before Ready but left the Data fence bound to its unpublished lifecycle",
    );
    drop(data);
    fixture.close().await;
}

const INDEX: &str = "secondary by_category";

fn row(id: i64, category: i64, label: &str) -> Value {
    json!({"id":id,"category":category,"label":label})
}

fn original(category: i64) -> Value {
    match category {
        7 => json!([
            row(1, 7, "one"),
            row(2, 7, "two"),
            row(3, 7, "three"),
            row(5, 7, "five")
        ]),
        8 => json!([row(4, 8, "four"), row(6, 8, "six")]),
        _ => unreachable!(),
    }
}

fn changed(category: i64) -> Value {
    match category {
        7 => json!([row(5, 7, "five"), row(7, 7, "direct insert")]),
        8 => json!([
            row(1, 8, "direct move"),
            row(3, 8, "queued move"),
            row(6, 8, "six"),
            row(8, 8, "queued insert")
        ]),
        _ => unreachable!(),
    }
}

impl Fixture {
    fn query(&self, category: i64) -> Value {
        json!({"table_id":self.definition.table_id,"query":{
            "select":["id","category","label"],
            "filter":{"column":"category","op":"eq","value":category},
            "order_by":null,"limit":null,
        }})
    }

    async fn seed(&self, size: i64) {
        assert!(size >= 6);
        let mut rows = original(7).as_array().unwrap().clone();
        rows.extend(original(8).as_array().unwrap().iter().cloned());
        rows.extend((7..=size).map(|id| row(-id, 99, "another backfill row")));
        let result = tests::call(
            &self.app,
            "table.insert_many",
            json!({"table_id":self.definition.table_id,"rows":rows}),
        )
        .await;
        assert_eq!(result, json!(size));
    }

    async fn assert_status(&self, attempt: u64, state: &str, may_be_undecided: bool) {
        let (status, body) = tokio::time::timeout(
            Duration::from_secs(5),
            self.admin(
                Uuid::new_v4(),
                "storage.index.status",
                self.selector(attempt),
            ),
        )
        .await
        .expect("status blocked on the paused index actor");
        if may_be_undecided && status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"]["code"], "storage_unavailable", "{body}");
            assert_eq!(body["error"]["outcome"], "not_applied", "{body}");
        } else {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["data"]["state"], state, "{body}");
            assert_eq!(body["data"]["attempt"], attempt, "{body}");
        }
    }

    async fn assert_query(&self, category: i64, expected: &Value, indexed: bool) {
        let query = self.query(category);
        let plan = tests::call(&self.app, "query.explain", query.clone()).await;
        let plan = plan.as_str().unwrap();
        assert_eq!(plan.contains(INDEX), indexed, "{plan}");
        if indexed {
            assert!(plan.contains("covering true"), "{plan}");
        }
        assert_eq!(
            tests::call(&self.app, "query.collect", query.clone()).await,
            *expected
        );
        assert_eq!(
            tests::call(&self.app, "query.count", query).await,
            json!(expected.as_array().unwrap().len())
        );
    }

    async fn assert_point(&self, key: i64, expected: Value) {
        let switch = self.runtime.direct_switch().unwrap();
        let before = switch.direct_stats().unwrap();
        assert_eq!(self.point(key).await, expected);
        let after = switch.direct_stats().unwrap();
        assert_eq!(
            after.gets_done - before.gets_done,
            u64::from(switch.routes_direct()),
            "the point read must use the configured path",
        );
        assert_eq!(
            after.fallbacks, before.fallbacks,
            "point read fell back to the queue"
        );
        assert_eq!(after.failed, before.failed, "direct point read failed");
    }

    async fn assert_building(&self, expected: &Value) {
        self.assert_status(1, "building", false).await;
        self.assert_query(7, expected, false).await;
        self.assert_point(5, row(5, 7, "five")).await;
    }

    async fn mutate(&self) {
        let table = self.definition.table_id;
        let switch = self.runtime.direct_switch().unwrap();
        let before = switch.direct_stats().unwrap();
        for (op, payload) in [
            (
                "table.update",
                json!({"table_id":table,"key":1,"values":{"category":8,"label":"direct move"}}),
            ),
            ("table.delete", json!({"table_id":table,"key":2})),
            (
                "table.insert",
                json!({"table_id":table,"row":row(7, 7, "direct insert")}),
            ),
        ] {
            let id = Uuid::new_v4();
            let (status, reply) =
                post(&self.app, "/v1/operations", TOKEN, id, op, payload.clone()).await;
            assert_eq!(status, StatusCode::OK, "{op}: {reply}");
            let duplicate = post(&self.app, "/v1/operations", TOKEN, id, op, payload).await;
            assert_eq!(duplicate.0, status, "{op}: {}", duplicate.1);
            assert_eq!(duplicate.1["data"], reply["data"], "{op} retry");
        }
        let direct_commits =
            switch.direct_stats().unwrap().entries_applied - before.entries_applied;
        assert_eq!(
            direct_commits,
            if switch.routes_direct() { 3 } else { 0 },
            "one Data commit per new direct request and none for the retries",
        );

        for (op, payload) in [
            (
                "table.update",
                json!({"table_id":table,"key":3,"values":{"category":8,"label":"queued move"}}),
            ),
            ("table.delete", json!({"table_id":table,"key":4})),
            (
                "table.insert",
                json!({"table_id":table,"row":row(8, 8, "queued insert")}),
            ),
        ] {
            let id = Uuid::new_v4();
            let (status, accepted) =
                post(&self.app, "/v1/requests", TOKEN, id, op, payload.clone()).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{op}: {accepted}");
            let run = serde_json::from_value(accepted["run_id"].clone()).unwrap();
            let mut receipt = self.runtime.observe(run, id).await.unwrap();
            while !receipt.state.terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                receipt = self.runtime.observe(run, id).await.unwrap();
            }
            assert_eq!(
                receipt.state,
                RequestState::Succeeded,
                "{op}: {:?}",
                receipt.receipt
            );
            let (status, replayed) = post(&self.app, "/v1/requests", TOKEN, id, op, payload).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{op} retry: {replayed}");
            assert_eq!(replayed["run_id"], accepted["run_id"]);
            assert_eq!(
                self.runtime.observe(run, id).await.unwrap().receipt,
                receipt.receipt
            );
        }
        assert_eq!(
            switch.direct_stats().unwrap().entries_applied - before.entries_applied,
            direct_commits,
            "asynchronous mutations must stay queued",
        );
    }

    async fn assert_ready(&self, attempt: u64, expected: fn(i64) -> Value) {
        self.assert_status(attempt, "ready", false).await;
        for category in [7, 8] {
            self.assert_query(category, &expected(category), true).await;
        }
        self.assert_point(5, row(5, 7, "five")).await;
    }

    async fn pin_query(&self, operation: &'static str) -> HeldQuery {
        let id = Uuid::new_v4();
        let hold = index_holds::register_checkpoint(id, Checkpoint::ReadPin);
        let (app, query) = (self.app.clone(), self.query(7));
        let task =
            tokio::spawn(
                async move { post(&app, "/v1/operations", TOKEN, id, operation, query).await },
            );
        let hold = reached(hold).await;
        let replica = self.runtime.services.replica.lock().unwrap();
        assert!(replica.record(id).is_some_and(|record| record.accepted));
        let (_, input, _) = replica.input(id).unwrap();
        let context: AcceptedContext = serde_json::from_slice(input.as_ref()).unwrap();
        HeldQuery {
            hold,
            task,
            read_ts: context.read.unwrap().storage_pin.read_ts,
        }
    }

    async fn reopen(self) -> Self {
        let Self {
            runtime,
            app,
            definition,
            build_id,
            dir,
        } = self;
        runtime.drain().await.unwrap();
        drop((app, runtime));
        let runtime = open(&dir.0, true).await;
        let app = runtime
            .application_with_admin(TOKEN.into(), ADMIN.into())
            .unwrap();
        Self {
            runtime,
            app,
            definition,
            build_id,
            dir,
        }
    }

    async fn stored_index_rows(&self, read_ts: ReadTs, category: i64) -> Vec<Value> {
        use crate::oltp::mvcc::{
            index::IndexValue,
            store::scan::{ScanDomain, ScanPage, ScanRequest},
        };
        let data = self.participant().await;
        let scan = data
            .scan_page(ScanRequest {
                domain: ScanDomain::Index {
                    table_id: self.definition.table_id,
                    index_id: self.definition.index_id,
                    equality: Page::new(vec![IndexValue::Int64(category)]).unwrap(),
                    complete: true,
                    lower: None,
                    upper: None,
                },
                read_ts,
                after: None,
                after_owner: None,
                lower: None,
                upper: None,
                max_items: 128,
                max_bytes: 1 << 20,
            })
            .await
            .unwrap();
        let ScanPage::Records {
            records,
            exhausted: true,
        } = scan.payload().unwrap()
        else {
            panic!("small index scan must fit in one page");
        };
        records
            .iter()
            .filter_map(|record| {
                record.value.as_ref().map(|value| {
                    serde_json::from_str::<Value>(value.as_str()).unwrap()["cover"].clone()
                })
            })
            .collect()
    }
}

struct HeldQuery {
    hold: BuildHold,
    task: tokio::task::JoinHandle<(StatusCode, Value)>,
    read_ts: ReadTs,
}

impl HeldQuery {
    async fn result(self) -> Value {
        self.hold.release.send(false).unwrap();
        let (status, body) = self.task.await.unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        body["data"].clone()
    }
}

fn assert_page_bounds(page: &index_holds::PageBuffers) {
    assert!(page.rows <= 128, "{page:?}");
    assert!(
        page.page_bytes > 0 && page.page_bytes + page.cursor_bytes <= 128 << 20,
        "{page:?}"
    );
}

async fn backfill_and_retirement() -> (usize, usize) {
    let fixture = Fixture::new().await;
    fixture.seed(129).await;
    let record = fixture.declare().await;
    let phase = index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Backfill);
    let actor = fixture.spawn_resume(record.clone());
    let first = reached(phase).await;
    let page = first.buffers.as_ref().unwrap();
    assert_page_bounds(page);
    assert_eq!(page.rows, 128);
    assert!(!page.exhausted);
    fixture.assert_building(&original(7)).await;
    let historical = fixture.pin_query("query.collect").await;
    fixture.mutate().await;
    fixture.assert_building(&changed(7)).await;
    first.release.send(true).unwrap();
    assert_eq!(
        actor.await.unwrap().unwrap_err().message,
        "index fixture interrupted backfill"
    );

    let first = index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Backfill);
    let actor = fixture.spawn_resume(record);
    let first = reached(first).await;
    let page = first.buffers.as_ref().unwrap();
    assert_page_bounds(page);
    let first_bytes = page.page_bytes;
    fixture.assert_status(1, "building", false).await;
    let second =
        index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Backfill);
    first.release.send(false).unwrap();
    let second = reached(second).await;
    let page = second.buffers.as_ref().unwrap();
    assert_page_bounds(page);
    assert_eq!(page.rows, 1);
    assert!(page.exhausted);
    assert!(page.cursor_bytes > 0);
    let page_peak = first_bytes.max(page.page_bytes + page.cursor_bytes);
    fixture.assert_status(1, "building", false).await;
    second.release.send(false).unwrap();
    actor.await.unwrap().unwrap();
    fixture.assert_ready(1, changed).await;
    assert_eq!(historical.result().await, original(7));

    let ready_snapshot = fixture.pin_query("query.collect").await;
    let ready_plan = fixture.pin_query("query.explain").await;
    let read_ts = ready_snapshot.read_ts;
    for category in [7, 8] {
        let entries = fixture.stored_index_rows(read_ts, category).await;
        assert_eq!(entries, *changed(category).as_array().unwrap());
    }
    let (status, retired) = fixture
        .admin(Uuid::new_v4(), "storage.index.retire", fixture.selector(1))
        .await;
    assert_eq!(status, StatusCode::OK, "{retired}");
    fixture.assert_status(1, "retired", false).await;
    tests::call(
        &fixture.app,
        "table.update",
        json!({
            "table_id":fixture.definition.table_id,"key":5,"values":{"label":"after retirement"}
        }),
    )
    .await;
    fixture
        .assert_query(
            7,
            &json!([row(5, 7, "after retirement"), row(7, 7, "direct insert")]),
            false,
        )
        .await;
    assert_eq!(ready_snapshot.result().await, changed(7));
    let plan = ready_plan.result().await;
    assert!(plan.as_str().unwrap().contains(INDEX), "{plan}");
    let retained = fixture
        .stored_index_rows(ReadTs::new(u64::MAX).unwrap(), 7)
        .await
        .len();
    fixture.close().await;
    (page_peak, retained)
}

async fn interrupt_refresh(cold: bool) {
    let mut fixture = Fixture::new().await;
    fixture.seed(6).await;
    let record = fixture.declare().await;
    let phase = index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Refresh);
    let actor = fixture.spawn_resume(record.clone());
    let held = reached(phase).await;
    fixture.assert_building(&original(7)).await;
    fixture.mutate().await;
    fixture.assert_building(&changed(7)).await;
    if cold {
        let old = fixture.runtime.index_authority(None).await.unwrap();
        actor.abort();
        assert!(actor.await.unwrap_err().is_cancelled());
        drop(held);
        fixture = fixture.reopen().await;
        assert_ne!(
            fixture.runtime.index_authority(None).await.unwrap().run_id,
            old.run_id
        );
        fixture.assert_building(&changed(7)).await;
        let error = fixture.runtime.resume_index(&record).await.unwrap_err();
        assert_eq!(
            error.message,
            "old-run index build requires a fresh fenced attempt"
        );
        fixture.runtime.restart_index(&record, 2).await.unwrap();
        fixture.assert_ready(2, changed).await;
    } else {
        held.release.send(true).unwrap();
        assert_eq!(
            actor.await.unwrap().unwrap_err().message,
            "index fixture interrupted refresh"
        );
        fixture.runtime.resume_index(&record).await.unwrap();
        fixture.assert_ready(1, changed).await;
    }
    fixture.close().await;
}

async fn interrupt_validation() {
    let fixture = Fixture::new().await;
    fixture.seed(6).await;
    let record = fixture.declare().await;
    let phase =
        index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Validation);
    let actor = fixture.spawn_resume(record.clone());
    let held = reached(phase).await;
    fixture.assert_building(&original(7)).await;
    held.release.send(true).unwrap();
    assert_eq!(
        actor.await.unwrap().unwrap_err().message,
        "index fixture rejected Ready validation"
    );
    fixture.assert_building(&original(7)).await;
    fixture.runtime.resume_index(&record).await.unwrap();
    fixture.assert_ready(1, original).await;
    fixture.close().await;
}

async fn interrupt_publication() {
    use index_holds::Stage;
    let fixture = Fixture::new().await;
    fixture.seed(6).await;
    let record = fixture.declare().await;
    let phase =
        index_holds::register_checkpoint(fixture.definition.index_id, Checkpoint::Validation);
    let actor = fixture.spawn_resume(record);
    let held = reached(phase).await;
    fixture.assert_building(&original(7)).await;
    let historical = fixture.pin_query("query.explain").await;
    let catalog = fixture.runtime.services.catalog.participant();
    let catalog_id = catalog.identity().group_id;
    let mut holds = index_holds::register(catalog_id);
    held.release.send(false).unwrap();

    let prepared = tokio::time::timeout(WAIT, holds.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.stage, Stage::Prepared);
    fixture.assert_status(1, "building", true).await;
    let plan = historical.result().await;
    assert!(!plan.as_str().unwrap().contains(INDEX), "{plan}");
    prepared.release.send(()).unwrap();

    let decided = tokio::time::timeout(WAIT, holds.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decided.stage, Stage::Decided);
    assert_eq!(decided.txn, prepared.txn);
    fixture.assert_status(1, "ready", false).await;
    actor.abort();
    assert!(actor.await.unwrap_err().is_cancelled());
    drop(decided);
    index_holds::unregister(catalog_id);
    let (status, body) = fixture
        .admin(Uuid::new_v4(), "storage.index.resume", fixture.selector(1))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["state"], "ready");
    let owner = MaintenanceOwnerRef {
        catalog_group_id: catalog_id,
        table_id: fixture.definition.table_id,
        index_id: fixture.definition.index_id,
        build_id: fixture.build_id,
        attempt: 1,
    };
    let closure = catalog.strong_index_attempt_closed(owner).await.unwrap();
    assert_eq!(closure.statement().kind, IndexClosedKind::Ready);
    assert_eq!(closure.archive_evidence().txn, prepared.txn);
    let authority = fixture.runtime.index_authority(None).await.unwrap();
    let expired = fixture
        .runtime
        .services
        .timestamps
        .begin_maintenance(authority, owner)
        .await
        .unwrap_err();
    assert!(expired.message.contains("PinExpired"), "{expired:?}");
    fixture.assert_ready(1, original).await;
    fixture.close().await;
}

fn marker(assertion: &str, status: &str, detail: Value) {
    eprintln!(
        "NABEDRA_OLTP_ASSERT {}",
        json!({"scenario":"O12","assertion":assertion,"status":status,"detail":detail})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run through scripts/acceptance_oltp.py"]
async fn acceptance_o12() {
    tokio::time::timeout(Duration::from_secs(300), async {
        let (page_peak, retained) = backfill_and_retirement().await;
        interrupt_refresh(false).await;
        interrupt_refresh(true).await;
        interrupt_validation().await;
        interrupt_publication().await;

        marker("never-plan-incomplete-index", "passed", json!("queued query plans, exact results and configured point-read path while Building; typed status at validation and the Prepared Ready transaction"));
        marker("concurrent-writes-exactly-once", "passed", json!("same-ID retries on synchronous and queued insert, move and delete; exact covering query results, counts and native index entries"));
        marker("recovery-and-retired-snapshots", "passed", json!("same-run actor recovery, cold worker restart, interrupted Ready reconciliation and held queries spanning publication and retirement"));
        marker("bounded-resources", "not-run", json!({
            "backfill_page_and_cursor_bytes":page_peak, "bound_bytes":128 << 20,
            "retired_entries_still_visible_in_data_store":retained,
            "reason":"retirement has no snapshot-safe Data reclamation",
        }));
        // The original runner predates assertion markers. Keep it from
        // counting the scenario as passed while retired disk data remains.
        eprintln!("NABEDRA_OLTP_ACCEPTANCE {}", json!({"scenario":"O12","status":"not-run","reason":"retired index Data reclamation is missing"}));
    }).await.expect("O12 acceptance watchdog");
}
