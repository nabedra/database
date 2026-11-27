//! S4 regressions exercise actual conditional root updates and local execution.
#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::{
    engine::Engine,
    partition_size::PartitionSize,
    protocol::ErrorCode,
    storage::*,
    worker_pool::{WorkerPool, WorkerThreads},
};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::Semaphore,
    time::{Duration, Instant},
};
use uuid::Uuid;
#[path = "support/storage.rs"]
mod fake;
use fake::Fake;
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn budget() -> HistoryBudget {
    HistoryBudget::new(
        100,
        8 * 1024 * 1024,
        Instant::now() + Duration::from_secs(10),
    )
    .unwrap()
}
fn fence(run: u128, term: u64) -> CatalogFence {
    CatalogFence {
        run_id: id(run),
        scheduler_term: term,
    }
}
fn body(n: u128, op: &str, payload: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"request_id":id(n),"operation":op,"payload":payload})).unwrap()
}
fn decision(base: &LoadedClusterCatalog, n: u128) -> Decision {
    Decision {
        version: 1,
        id: id(n),
        parent: base.catalog().decision_head,
        base_revision: base.catalog().revision,
        revision: base.catalog().revision + 1,
        request_id: id(n + 1000),
        request_body: String::from_utf8(body(n + 1000, "table.drop", json!({"table_id":id(999)})))
            .unwrap(),
        response_status: 200,
        response_body: format!(r#"{{"request_id":"{}","data":null}}"#, id(n + 1000)),
    }
}
#[tokio::test]
async fn r05_v3_explicit_migration_preserves_tables_and_decision_history() {
    let s = Storage::new(Fake::default(), Namespace::new("fenced").unwrap());
    let old = s.initialize_cluster_catalog().await.unwrap();
    let d = decision(&old, 10);
    let p =
        PreparedPublication::prepare(old.clone(), old.catalog().tables.clone(), d.clone()).unwrap();
    p.publish(&s, budget()).await.unwrap();
    let old = s.load_cluster_catalog().await.unwrap();
    let migrated = s
        .fence_cluster_catalog(&old, fence(1, 1), FenceTransition::Migrate)
        .await
        .unwrap();
    assert_eq!(migrated.catalog().format_version, 4);
    assert_ne!(migrated.etag(), old.etag());
    assert_eq!(migrated.catalog().decision_head, Some(d.id));
    assert_eq!(migrated.catalog().revision, old.catalog().revision);
    assert_eq!(
        s.lookup_decision_by_id(&migrated, d.request_id, budget())
            .await
            .unwrap(),
        Some(d)
    );
    assert!(
        PreparedPublication::prepare(
            migrated.clone(),
            migrated.catalog().tables.clone(),
            decision(&migrated, 11)
        )
        .is_err()
    );
    assert!(
        s.fence_cluster_catalog(&migrated, fence(2, 1), FenceTransition::Takeover)
            .await
            .is_err()
    );
    assert!(
        s.fence_cluster_catalog(&migrated, fence(1, 1), FenceTransition::Takeover)
            .await
            .is_err()
    );
}
struct Gate {
    entered: Semaphore,
    release: Semaphore,
    after_apply: bool,
}
impl Gate {
    fn new(after_apply: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            after_apply,
        })
    }
    async fn stop(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    async fn wait(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
}
#[derive(Default)]
struct Gated {
    fake: Fake,
    root: Mutex<Option<Arc<Gate>>>,
    fail_table_at: AtomicUsize,
    table_attempts: AtomicUsize,
}
impl ObjectStore for Gated {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        if key.contains("/tables/") {
            let attempt = self.table_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_table_at.load(Ordering::SeqCst) == attempt {
                return Err(StorageError::Unavailable);
            }
        }
        let gate = if key.ends_with("/catalog.json") {
            self.root.lock().unwrap().take()
        } else {
            None
        };
        if let Some(g) = gate.as_ref().filter(|g| !g.after_apply) {
            g.stop().await;
        }
        let result = self.fake.put(key, bytes, condition).await;
        if let Some(g) = gate.as_ref().filter(|g| g.after_apply) {
            g.stop().await;
            return Err(StorageError::Ambiguous);
        }
        result
    }
}
async fn delayed_order(after_apply: bool) {
    let s = Storage::new(Gated::default(), Namespace::new("fenced").unwrap());
    let old = s.initialize_cluster_catalog().await.unwrap();
    let old = s
        .fence_cluster_catalog(&old, fence(1, 1), FenceTransition::Migrate)
        .await
        .unwrap();
    let d = decision(&old, 10);
    let pending = PreparedPublication::prepare_fenced(
        old.clone(),
        old.catalog().tables.clone(),
        d.clone(),
        fence(1, 1),
    )
    .unwrap();
    let gate = Gate::new(after_apply);
    *s.backend().root.lock().unwrap() = Some(gate.clone());
    let (outcome, ()) = tokio::join!(pending.publish(&s, budget()), async {
        gate.wait().await;
        let current = s.load_cluster_catalog().await.unwrap();
        let new = s
            .fence_cluster_catalog(&current, fence(2, 1), FenceTransition::ColdStart)
            .await
            .unwrap();
        assert_eq!(new.catalog().fence, Some(fence(2, 1)));
        gate.release.add_permits(1);
    });
    let current = s.load_cluster_catalog().await.unwrap();
    assert_eq!(current.catalog().fence, Some(fence(2, 1)));
    if after_apply {
        assert_eq!(outcome.unwrap(), PublicationOutcome::Terminal(d.clone()));
        assert_eq!(
            s.lookup_decision_by_id(&current, d.request_id, budget())
                .await
                .unwrap(),
            Some(d)
        );
    } else {
        assert_eq!(outcome.unwrap(), PublicationOutcome::NeedsRecompute);
        assert!(
            s.lookup_decision_by_id(&current, d.request_id, budget())
                .await
                .unwrap()
                .is_none()
        );
    }
}
#[tokio::test]
async fn t05_delayed_old_put_after_cold_fence_fails_same_cas() {
    delayed_order(false).await;
}
#[tokio::test]
async fn t04_t05_applied_put_before_cold_fence_survives_lost_reply() {
    delayed_order(true).await;
}
async fn engine() -> (Engine<Fake>, WorkerThreads) {
    let s = Storage::new(Fake::default(), Namespace::new("fenced").unwrap());
    let old = s.initialize_cluster_catalog().await.unwrap();
    s.fence_cluster_catalog(&old, fence(1, 1), FenceTransition::Migrate)
        .await
        .unwrap();
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    (Engine::new(s, pool, PartitionSize::default()), threads)
}
async fn run(e: &Engine<Fake>, n: u128, op: &str, payload: Value) -> (u16, Value) {
    let r = e
        .execute_identified(body(n, op, payload), fence(1, 1), None)
        .await
        .unwrap();
    (r.status, serde_json::from_slice(&r.body).unwrap())
}
#[tokio::test]
async fn t03_t06_local_identified_workflow_replay_noops_and_atomic_batch() {
    let (e, threads) = engine().await;
    let create =
        json!({"name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"});
    let (_, created) = run(&e, 100, "table.create", create.clone()).await;
    let table = created["data"]["id"].clone();
    let (_, replayed) = run(&e, 100, "table.create", create).await;
    assert_eq!(created, replayed);
    let batch = json!({"table_id":table,"rows":[{"id":1,"text":"a"},{"id":2,"text":"b"}]});
    assert_eq!(
        run(&e, 101, "table.insert_many", batch.clone()).await.0,
        200
    );
    assert_eq!(run(&e, 101, "table.insert_many", batch).await.0, 200);
    let before = e.storage().load_cluster_catalog().await.unwrap();
    let conflict =
        json!({"table_id":table,"rows":[{"id":3,"text":"c"},{"id":1,"text":"duplicate"}]});
    assert_eq!(
        run(&e, 102, "table.insert_many", conflict.clone()).await.0,
        409
    );
    assert_eq!(run(&e, 102, "table.insert_many", conflict).await.0, 409);
    let after = e.storage().load_cluster_catalog().await.unwrap();
    assert_eq!(before.catalog().tables, after.catalog().tables);
    assert_eq!(
        run(
            &e,
            103,
            "table.insert_many",
            json!({"table_id":table,"rows":[]})
        )
        .await
        .1["data"],
        json!(0)
    );
    let pin = e.storage().load_cluster_catalog().await.unwrap();
    let query = body(
        104,
        "query.count",
        json!({"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}),
    );
    run(
        &e,
        105,
        "table.insert",
        json!({"table_id":table,"row":{"id":3,"text":"new"}}),
    )
    .await;
    let old = e
        .execute_identified(query, fence(1, 1), Some(pin))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&old.body).unwrap()["data"],
        2
    );
    let wrong = e
        .execute_identified(
            body(100, "table.drop", json!({"table_id":table})),
            fence(1, 1),
            None,
        )
        .await
        .err()
        .unwrap();
    assert_eq!(wrong.code, ErrorCode::Conflict);
    assert!(
        e.storage()
            .backend()
            .state
            .lock()
            .unwrap()
            .put_keys
            .iter()
            .all(|k| k.ends_with("catalog.json")
                || k.contains("/tables/")
                || k.contains("/decisions/")
                // OLAP-1: immutable decision index pages.
                || k.contains("/decision-index/"))
    );
    threads.shutdown().await.unwrap();
}
#[tokio::test]
async fn t03_concurrent_local_creates_recompute_cas_loser() {
    let (e, threads) = engine().await;
    let first = json!({"name":"alpha","schema":{"id":"int64"},"key":"id","format":"parquet"});
    let second = json!({"name":"beta","schema":{"id":"int64"},"key":"id","format":"parquet"});
    let (a, b) = tokio::join!(
        run(&e, 100, "table.create", first),
        run(&e, 101, "table.create", second)
    );
    assert_eq!((a.0, b.0), (200, 200));
    let root = e.storage().load_cluster_catalog().await.unwrap();
    assert_eq!(root.catalog().tables.len(), 2);
    assert_eq!(root.catalog().revision, 2);
    threads.shutdown().await.unwrap();
}

#[tokio::test]
async fn t01_t02_local_upload_failure_leaves_entire_prior_catalog_visible() {
    for failed_upload in [1, 2] {
        let s = Storage::new(Gated::default(), Namespace::new("fenced").unwrap());
        let old = s.initialize_cluster_catalog().await.unwrap();
        s.fence_cluster_catalog(&old, fence(1, 1), FenceTransition::Migrate)
            .await
            .unwrap();
        let (pool, threads) = WorkerPool::start(
            NonZeroUsize::new(2).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let e = Engine::new(s, pool, "4 KiB".parse().unwrap());
        let created=e.execute_identified(body(100,"table.create",json!({"name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"})),fence(1,1),None).await.unwrap();
        let table = serde_json::from_slice::<Value>(&created.body).unwrap()["data"]["id"].clone();
        let before = e.storage().load_cluster_catalog().await.unwrap();
        e.storage()
            .backend()
            .fail_table_at
            .store(failed_upload, Ordering::SeqCst);
        let rows = (0..100)
            .map(|n| json!({"id":n,"text":format!("row{n:04}{}","x".repeat(500))}))
            .collect::<Vec<_>>();
        assert!(
            e.execute_identified(
                body(
                    101,
                    "table.insert_many",
                    json!({"table_id":table,"rows":rows})
                ),
                fence(1, 1),
                None
            )
            .await
            .is_err()
        );
        let after = e.storage().load_cluster_catalog().await.unwrap();
        assert_eq!(before, after);
        assert_eq!(
            e.storage().backend().table_attempts.load(Ordering::SeqCst),
            failed_upload
        );
        let object_count = e
            .storage()
            .backend()
            .fake
            .state
            .lock()
            .unwrap()
            .objects
            .keys()
            .filter(|k| k.contains("/tables/"))
            .count();
        assert_eq!(object_count, failed_upload - 1);
        threads.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn r04_bootstrap_creates_v4_directly_attaches_and_cold_fences_once() {
    let s = Storage::new(Fake::default(), Namespace::new("bootstrap").unwrap());
    s.backend().state.lock().unwrap().lose_put_response = true;
    let created = s
        .initialize_fenced_cluster_catalog(fence(1, 1), false, false)
        .await
        .unwrap();
    assert_eq!(created.catalog().format_version, 4);
    assert_eq!(created.catalog().fence, Some(fence(1, 1)));
    assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    let attached = s
        .initialize_fenced_cluster_catalog(fence(999, 999), false, false)
        .await
        .unwrap();
    assert_eq!(attached, created);
    assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    let cold = s
        .initialize_fenced_cluster_catalog(fence(2, 1), false, true)
        .await
        .unwrap();
    assert_eq!(cold.catalog().fence, Some(fence(2, 1)));
    assert_eq!(s.backend().state.lock().unwrap().puts, 2);
}

#[tokio::test]
async fn r05_bootstrap_requires_explicit_migration_even_for_empty_existing_v3() {
    for legacy in [true, false] {
        let s = Storage::new(Fake::default(), Namespace::new("migration").unwrap());
        if legacy {
            s.initialize_catalog().await.unwrap();
        } else {
            s.initialize_cluster_catalog().await.unwrap();
        }
        let puts = s.backend().state.lock().unwrap().puts;
        assert_eq!(
            s.initialize_fenced_cluster_catalog(fence(1, 1), false, false)
                .await
                .err(),
            Some(StorageError::UnsupportedCatalog)
        );
        assert_eq!(s.backend().state.lock().unwrap().puts, puts);
        let migrated = s
            .initialize_fenced_cluster_catalog(fence(1, 1), true, false)
            .await
            .unwrap();
        assert_eq!(migrated.catalog().format_version, 4);
        assert_eq!(migrated.catalog().revision, 0);
        assert_eq!(s.backend().state.lock().unwrap().puts, puts + 1);
    }
}
