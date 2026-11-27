#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::storage::*;
#[path = "support/storage.rs"]
mod fake;
use fake::Fake;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

fn store() -> Storage<Fake> {
    Storage::new(Fake::default(), Namespace::new("test").unwrap())
}
fn seed(s: &Storage<Fake>, key: ObjectKey, bytes: Vec<u8>) {
    s.backend().state.lock().unwrap().objects.insert(
        format!("test/{}", key.as_str()),
        (bytes, "original-token".into()),
    );
}
fn budget() -> HistoryBudget {
    HistoryBudget::new(100, 1024 * 1024, Instant::now() + Duration::from_secs(30)).unwrap()
}
fn decision(n: u128, base: u64, parent: Option<Uuid>) -> Decision {
    let request_id = Uuid::from_u128(n + 1000);
    let body = format!(
        " {{\"request_id\":\"{request_id}\",\"operation\":\"table.insert\",\"payload\":{{\"table_id\":\"{}\",\"row\":{{\"id\":1.00e0}}}}}} ",
        Uuid::from_u128(99)
    );
    Decision {
        version: 1,
        id: Uuid::from_u128(n),
        parent,
        base_revision: base,
        revision: base + 1,
        request_id,
        request_body: body,
        response_status: 200,
        response_body: format!(" {{\"request_id\":\"{request_id}\",\"data\":null}} \t"),
    }
}
fn root(d: &Decision) -> ClusterCatalog {
    let mut c = ClusterCatalog::from_catalog(&Catalog::empty()).unwrap();
    c.revision = d.revision;
    c.commit_id = d.id;
    c.decision_head = Some(d.id);
    c
}
fn chain(s: &Storage<Fake>) -> (Decision, Decision) {
    let first = decision(1, 7, None);
    let last = decision(2, 8, Some(first.id));
    for d in [&first, &last] {
        seed(s, ObjectKey::decision(d.id), d.encode().unwrap());
    }
    seed(s, ObjectKey::catalog(), root(&last).encode().unwrap());
    (first, last)
}

#[tokio::test]
async fn initialize_attach_and_migrate_preserve_exact_legacy_identity() {
    let s = store();
    let initial = s.initialize_cluster_catalog().await.unwrap();
    assert_eq!(
        initial.catalog(),
        &ClusterCatalog::from_catalog(&Catalog::empty()).unwrap()
    );
    assert!(!initial.etag().is_empty());
    assert_eq!(s.initialize_cluster_catalog().await.unwrap(), initial);
    assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    assert!(s.load_catalog().await.is_err());
    for version in [1, 2] {
        let s = store();
        let table = Uuid::from_u128(55);
        let commit = Uuid::from_u128(56);
        let data = if version == 1 {
            format!(
                "\"snapshot\":\"{}\"",
                ObjectKey::snapshot(table, commit).as_str()
            )
        } else {
            format!(
                "\"data\":{{\"layout\":\"partitioned\",\"files\":[{{\"object\":\"{}\",\"range\":{{\"lower\":null,\"upper\":null}},\"row_count\":1,\"byte_size\":100,\"payload_bytes\":8}}]}}",
                ObjectKey::partition(table, commit, 0).as_str()
            )
        };
        let bytes = format!("{{\"format_version\":{version},\"revision\":42,\"commit_id\":\"{commit}\",\"tables\":{{\"users\":{{\"id\":\"{table}\",\"schema\":{{\"id\":\"int64\"}},\"key\":\"id\",\"format\":\"parquet\",{data},\"row_count\":1}}}}}}").into_bytes();
        let legacy = Catalog::decode(&bytes).unwrap();
        seed(&s, ObjectKey::catalog(), bytes);
        let loaded = s.initialize_cluster_catalog().await.unwrap();
        assert_eq!(
            loaded.catalog(),
            &ClusterCatalog::from_catalog(&legacy).unwrap()
        );
        assert_ne!(loaded.etag(), "original-token");
        assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    }
}

#[tokio::test]
async fn applied_lost_responses_reconcile_without_second_put() {
    for migrate in [false, true] {
        let s = store();
        let mut legacy = Catalog::empty();
        legacy.revision = 19;
        if migrate {
            seed(&s, ObjectKey::catalog(), legacy.encode().unwrap());
        }
        s.backend().state.lock().unwrap().lose_put_response = true;
        let loaded = s.initialize_cluster_catalog().await.unwrap();
        let state = s.backend().state.lock().unwrap();
        assert_eq!(state.puts, 1);
        assert_eq!(state.generation, 1); // The write actually applied before response loss.
        assert_eq!(
            ClusterCatalog::decode(&state.objects["test/catalog.json"].0).unwrap(),
            *loaded.catalog()
        );
        assert_eq!(loaded.catalog().revision, if migrate { 19 } else { 0 });
    }
}

#[tokio::test]
async fn invalid_roots_and_failed_reconciliation_never_reset_state() {
    let c = ClusterCatalog::from_catalog(&Catalog::empty()).unwrap();
    let mut bad_head = c.clone();
    bad_head.decision_head = Some(Uuid::from_u128(9));
    let malformed = [
        b"{".to_vec(),
        c.encode().unwrap().into_iter().take(25).collect(),
    ];
    for bytes in malformed.into_iter().chain([
        serde_json::to_vec(&bad_head).unwrap(),
        format!("{{\"format_version\":99,\"revision\":0,\"commit_id\":\"{}\",\"tables\":{{}}}}",c.commit_id).into_bytes(),
        format!("{{\"format_version\":2,\"format_version\":1,\"revision\":0,\"commit_id\":\"{}\",\"tables\":{{}}}}",c.commit_id).into_bytes(),
    ]) {
        let s = store(); seed(&s, ObjectKey::catalog(), bytes.clone());
        assert!(s.initialize_cluster_catalog().await.is_err());
        let state = s.backend().state.lock().unwrap();
        assert_eq!(state.puts, 0); assert_eq!(state.objects["test/catalog.json"].0, bytes);
    }
    let s = store();
    seed(&s, ObjectKey::catalog(), c.encode().unwrap());
    s.backend()
        .state
        .lock()
        .unwrap()
        .objects
        .get_mut("test/catalog.json")
        .unwrap()
        .1
        .clear();
    assert!(s.initialize_cluster_catalog().await.is_err());
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    for error in [StorageError::Conflict, StorageError::Ambiguous] {
        let s = store();
        seed(&s, ObjectKey::catalog(), Catalog::empty().encode().unwrap());
        s.backend()
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert("test/catalog.json".into(), (error, false));
        assert_eq!(s.initialize_cluster_catalog().await.unwrap_err(), error);
        assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    }
}

#[tokio::test]
async fn reachable_terminal_replay_identity_conflict_orphan_and_absence() {
    let s = store();
    let (first, _) = chain(&s);
    let orphan = decision(3, 9, None);
    seed(&s, ObjectKey::decision(orphan.id), orphan.encode().unwrap());
    let loaded = s.load_cluster_catalog().await.unwrap();
    assert!(!format!("{loaded:?}").contains("original-token"));
    assert_eq!(s.initialize_cluster_catalog().await.unwrap(), loaded);
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    assert_eq!(
        s.lookup_decision(
            &loaded,
            first.request_id,
            first.request_body.as_bytes(),
            budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Found(first.clone())
    );
    assert_eq!(
        s.lookup_decision(
            &loaded,
            first.request_id,
            format!("{} ", first.request_body).as_bytes(),
            budget()
        )
        .await
        .unwrap(),
        HistoryLookup::IdentityConflict
    );
    assert_eq!(
        s.lookup_decision(
            &loaded,
            orphan.request_id,
            orphan.request_body.as_bytes(),
            budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Absent
    );
    let s = store();
    let loaded = s.initialize_cluster_catalog().await.unwrap();
    assert_eq!(
        s.lookup_decision(&loaded, orphan.request_id, b"", budget())
            .await
            .unwrap(),
        HistoryLookup::Absent
    );
}

#[tokio::test]
async fn corrupt_missing_partial_relationships_and_budgets_never_prove_absence() {
    for mode in 0..7 {
        let s = store();
        let (mut first, mut last) = chain(&s);
        match mode {
            0 => {
                first.id = Uuid::from_u128(999);
                seed(
                    &s,
                    ObjectKey::decision(Uuid::from_u128(1)),
                    first.encode().unwrap(),
                );
            }
            1 => {
                first.base_revision = 5;
                first.revision = 6;
                seed(&s, ObjectKey::decision(first.id), first.encode().unwrap());
            }
            2 => {
                first.parent = Some(last.id);
                seed(&s, ObjectKey::decision(first.id), first.encode().unwrap());
            }
            3 => {
                s.backend()
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .remove("test/decisions/00000000-0000-0000-0000-000000000001.json");
            }
            4 => {
                seed(&s, ObjectKey::decision(first.id), b"{".to_vec());
            }
            5 => {
                last.base_revision += 1;
                last.revision += 1;
                seed(&s, ObjectKey::decision(last.id), last.encode().unwrap());
            }
            _ => {
                seed(&s, ObjectKey::decision(first.id), b"[]".to_vec());
            }
        }
        let loaded = s.load_cluster_catalog().await.unwrap();
        assert!(
            s.lookup_decision(&loaded, Uuid::nil(), b"", budget())
                .await
                .is_err(),
            "mode {mode}"
        );
        if mode < 5 {
            // A matching validated prefix needs no older records.
            assert!(matches!(
                s.lookup_decision(
                    &loaded,
                    last.request_id,
                    last.request_body.as_bytes(),
                    budget()
                )
                .await
                .unwrap(),
                HistoryLookup::Found(_)
            ));
        }
    }
    let s = store();
    let (first, last) = chain(&s);
    let loaded = s.load_cluster_catalog().await.unwrap();
    for (count, bytes) in [
        (1, 100000),
        (10, last.encode().unwrap().len() - 1),
        (
            10,
            last.encode().unwrap().len() + first.encode().unwrap().len() - 1,
        ),
    ] {
        let b = HistoryBudget::new(count, bytes, Instant::now() + Duration::from_secs(30)).unwrap();
        assert_eq!(
            s.lookup_decision(&loaded, Uuid::nil(), b"", b)
                .await
                .unwrap_err(),
            StorageError::LimitExceeded
        );
    }
    let exact = HistoryBudget::new(
        2,
        last.encode().unwrap().len() + first.encode().unwrap().len(),
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(
        s.lookup_decision(&loaded, Uuid::nil(), b"", exact)
            .await
            .unwrap(),
        HistoryLookup::Absent
    );
    assert!(HistoryBudget::new(0, 1, Instant::now() + Duration::from_secs(1)).is_err());
    assert!(HistoryBudget::new(1, 0, Instant::now() + Duration::from_secs(1)).is_err());
    assert!(HistoryBudget::new(1, 1, Instant::now()).is_err());
}

// Captures both initial GET results before either initializer can reach its PUT.
struct Concurrent {
    fake: Fake,
    barrier: tokio::sync::Barrier,
    reads: AtomicUsize,
}
impl ObjectStore for Concurrent {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let result = self.fake.get(key).await;
        if self.reads.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        result
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.fake.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn concurrent_create_and_migration_use_backend_cas() {
    for migrate in [false, true] {
        let backend = Concurrent {
            fake: Fake::default(),
            barrier: tokio::sync::Barrier::new(2),
            reads: AtomicUsize::new(0),
        };
        if migrate {
            backend.fake.state.lock().unwrap().objects.insert(
                "test/catalog.json".into(),
                (Catalog::empty().encode().unwrap(), "legacy".into()),
            );
        }
        let s = Storage::new(backend, Namespace::new("test").unwrap());
        let (a, b) = tokio::join!(
            s.initialize_cluster_catalog(),
            s.initialize_cluster_catalog()
        );
        assert_eq!(a.unwrap(), b.unwrap());
        let state = s.backend().fake.state.lock().unwrap();
        assert_eq!(state.puts, 2);
        assert_eq!(state.generation, 1);
    }
}

#[derive(Clone, Copy)]
enum Fault {
    GetPending,
    BodyPending,
    Partial,
}
struct FaultStore {
    fake: Fake,
    fault: Mutex<Option<(String, Fault)>>,
}
struct BrokenBody {
    pending: bool,
}
impl tokio::io::AsyncRead for BrokenBody {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(Err(std::io::Error::other("incomplete transfer")))
        }
    }
}
impl ObjectStore for FaultStore {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let fault = self
            .fault
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(k, _)| k == key)
            .map(|(_, f)| *f);
        match fault {
            Some(Fault::GetPending) => std::future::pending().await,
            Some(Fault::BodyPending) => {
                Ok(Download::new(BrokenBody { pending: true }, "token".into()))
            }
            Some(Fault::Partial) => Ok(Download::new(
                tokio::io::AsyncReadExt::chain(
                    std::io::Cursor::new(b"{".to_vec()),
                    BrokenBody { pending: false },
                ),
                "token".into(),
            )),
            None => self.fake.get(key).await,
        }
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.fake.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn unreadable_roots_and_slow_or_partial_history_fail_safely() {
    let s = store();
    let (_, last) = chain(&s);
    let fake = Fake {
        state: Mutex::new(std::mem::take(&mut *s.backend().state.lock().unwrap())),
    };
    let s = Storage::new(
        FaultStore {
            fake,
            fault: Mutex::new(None),
        },
        Namespace::new("test").unwrap(),
    );
    let loaded = s.load_cluster_catalog().await.unwrap();
    for fault in [Fault::GetPending, Fault::BodyPending, Fault::Partial] {
        *s.backend().fault.lock().unwrap() = Some((
            format!("test/{}", ObjectKey::decision(last.id).as_str()),
            fault,
        ));
        let b = HistoryBudget::new(10, 100000, Instant::now() + Duration::from_millis(10)).unwrap();
        assert_eq!(
            s.lookup_decision(&loaded, Uuid::nil(), b"", b)
                .await
                .unwrap_err(),
            StorageError::Unavailable
        );
    }
    *s.backend().fault.lock().unwrap() = Some(("test/catalog.json".into(), Fault::Partial));
    assert_eq!(
        s.initialize_cluster_catalog().await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(s.backend().fake.state.lock().unwrap().puts, 0);
}

#[tokio::test]
async fn sl05_owner_cluster_is_conditional_and_legacy_migration_is_explicit() {
    let s = store();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let fence = CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    };
    let first = s
        .initialize_owned_cluster_catalog(a, fence, false, false)
        .await
        .unwrap();
    assert_eq!(first.catalog().owner_cluster, Some(a));
    assert_eq!(first.catalog().format_version, 5);
    assert_eq!(
        s.initialize_owned_cluster_catalog(a, fence, false, false)
            .await
            .unwrap(),
        first
    );
    let puts = s.backend().state.lock().unwrap().puts;
    assert_eq!(
        s.initialize_owned_cluster_catalog(b, fence, true, true)
            .await,
        Err(StorageError::Conflict)
    );
    assert_eq!(
        s.initialize_cluster_catalog().await,
        Err(StorageError::Conflict)
    );
    assert_eq!(s.backend().state.lock().unwrap().puts, puts);
    let other = store();
    let legacy = other.initialize_cluster_catalog().await.unwrap();
    assert_eq!(
        other
            .initialize_owned_cluster_catalog(a, fence, false, false)
            .await,
        Err(StorageError::UnsupportedCatalog)
    );
    let owned = other
        .initialize_owned_cluster_catalog(a, fence, true, false)
        .await
        .unwrap();
    assert_eq!(owned.catalog().tables, legacy.catalog().tables);
    assert_eq!(
        owned.catalog().decision_head,
        legacy.catalog().decision_head
    );
    assert_eq!(owned.catalog().commit_id, legacy.catalog().commit_id);
    assert_eq!(owned.catalog().owner_cluster, Some(a));
}

#[tokio::test]
async fn sl05_t05_old_publication_cannot_cross_owned_catalog_cold_fence() {
    let s = store();
    let cluster = Uuid::new_v4();
    let old = CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    };
    let base = s
        .initialize_owned_cluster_catalog(cluster, old, false, false)
        .await
        .unwrap();
    let mut d = decision(551, base.catalog().revision, base.catalog().decision_head);
    d.id = Uuid::new_v4();
    assert_eq!(
        PreparedPublication::prepare_owned(
            base.clone(),
            base.catalog().tables.clone(),
            d.clone(),
            Uuid::new_v4(),
            old
        )
        .unwrap_err(),
        StorageError::Conflict
    );
    assert_eq!(
        PreparedPublication::prepare_fenced(
            base.clone(),
            base.catalog().tables.clone(),
            d.clone(),
            old
        )
        .unwrap_err(),
        StorageError::Conflict
    );
    let candidate = PreparedPublication::prepare_owned(
        base.clone(),
        base.catalog().tables.clone(),
        d,
        cluster,
        old,
    )
    .unwrap();
    let fresh = CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    };
    assert_eq!(
        s.initialize_owned_cluster_catalog(cluster, fresh, false, false)
            .await,
        Err(StorageError::Conflict)
    );
    let fenced = s
        .initialize_owned_cluster_catalog(cluster, fresh, false, true)
        .await
        .unwrap();
    assert_eq!(fenced.catalog().owner_cluster, Some(cluster));
    assert_eq!(fenced.catalog().fence, Some(fresh));
    assert!(matches!(
        candidate.publish(&s, budget()).await.unwrap(),
        PublicationOutcome::NeedsRecompute
    ));
    let current = s.load_cluster_catalog().await.unwrap();
    assert_eq!(current, fenced);
    assert_eq!(
        PreparedPublication::prepare_owned(
            current,
            base.catalog().tables.clone(),
            decision(552, 0, None),
            cluster,
            old
        )
        .unwrap_err(),
        StorageError::Conflict
    );
}

#[tokio::test]
async fn sl05_explicit_owned_migration_keeps_reachable_decisions() {
    let s = store();
    let (first, last) = chain(&s);
    let fence = CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    };
    let owner = Uuid::new_v4();
    let owned = s
        .initialize_owned_cluster_catalog(owner, fence, true, false)
        .await
        .unwrap();
    assert_eq!(owned.catalog().commit_id, last.id);
    assert_eq!(owned.catalog().decision_head, Some(last.id));
    assert_eq!(owned.catalog().revision, last.revision);
    assert_eq!(
        s.lookup_decision(
            &owned,
            first.request_id,
            first.request_body.as_bytes(),
            budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Found(first)
    );
    assert_eq!(
        s.lookup_decision(
            &owned,
            last.request_id,
            last.request_body.as_bytes(),
            budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Found(last)
    );
}

#[derive(Default)]
struct SelectiveGrant {
    backend: Fake,
    deny_tables: std::sync::atomic::AtomicBool,
}
impl ObjectStore for SelectiveGrant {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        if key.contains("/tables/") && self.deny_tables.load(Ordering::Acquire) {
            return Err(StorageError::Unavailable);
        }
        self.backend.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.backend.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn sl02_warm_catalog_does_not_authorize_denied_backing_objects() {
    let s = Storage::new(SelectiveGrant::default(), Namespace::new("test").unwrap());
    let table_id = Uuid::new_v4();
    let object = ObjectKey::snapshot(table_id, Uuid::new_v4());
    let entry: CatalogEntry = serde_json::from_value(serde_json::json!({"id":table_id,"schema":{"id":"int64"},"key":"id","format":"parquet","data":{"layout":"legacy","snapshot":object.as_str()},"row_count":0})).unwrap();
    let mut catalog = Catalog::empty();
    catalog.tables.insert("rows".into(), entry);
    let mut catalog = ClusterCatalog::from_catalog(&catalog).unwrap();
    catalog.format_version = 5;
    catalog.owner_cluster = Some(Uuid::new_v4());
    catalog.fence = Some(CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    });
    s.put(
        &ObjectKey::catalog(),
        catalog.encode().unwrap(),
        WriteCondition::Create,
    )
    .await
    .unwrap();
    s.put(&object, vec![1, 2, 3], WriteCondition::Create)
        .await
        .unwrap();
    let warmed = s.load_cluster_catalog().await.unwrap();
    s.authorize_catalog_objects(&warmed, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    s.backend().deny_tables.store(true, Ordering::Release);
    // The same grant still reads the root. Its warm root cannot authorize rows.
    assert_eq!(s.load_cluster_catalog().await.unwrap(), warmed);
    assert_eq!(
        s.authorize_catalog_objects(&warmed, Instant::now() + Duration::from_secs(1))
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(s.backend().backend.state.lock().unwrap().puts, 2);
}

// OLAP-1: the decision history must not bound the number of writes. With the
// production lookup budget (4096 records, 64 MiB, 60 s), a fenced legacy chain
// longer than the budget must still let a fresh request ID prove absence, and
// later mutations must cost a bounded number of object requests.
fn production_budget() -> HistoryBudget {
    HistoryBudget::new(
        4096,
        64 * 1024 * 1024,
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap()
}
fn spread(n: u128) -> Uuid {
    // Request IDs that spread over the whole UUID space, unlike from_u128(n).
    Uuid::from_u128(n.wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835) ^ 0x5bd1_e995)
}
fn padded_decision(
    n: u128,
    base: u64,
    parent: Option<Uuid>,
    pad: usize,
    request_id: Uuid,
) -> Decision {
    let body = format!(
        "{}{{\"request_id\":\"{request_id}\",\"operation\":\"table.insert\",\"payload\":{{\"table_id\":\"{}\",\"row\":{{\"id\":1.00e0}}}}}} ",
        " ".repeat(pad),
        Uuid::from_u128(99)
    );
    Decision {
        version: 1,
        id: Uuid::from_u128(n),
        parent,
        base_revision: base,
        revision: base + 1,
        request_id,
        request_body: body,
        response_status: 200,
        response_body: format!(" {{\"request_id\":\"{request_id}\",\"data\":null}} \t"),
    }
}
fn olap1_fence() -> CatalogFence {
    CatalogFence {
        run_id: Uuid::from_u128(0x77),
        scheduler_term: 1,
    }
}
/// Seeds a fenced format 4 root whose chain holds `len` decisions.
fn fenced_chain(
    s: &Storage<Fake>,
    len: u128,
    pad: usize,
    ids: fn(u128) -> Uuid,
) -> (Vec<Decision>, LoadedClusterCatalog) {
    let mut parent = None;
    let mut all = Vec::new();
    for n in 1..=len {
        let d = padded_decision(n, (n - 1) as u64, parent, pad, ids(n));
        seed(s, ObjectKey::decision(d.id), d.encode().unwrap());
        parent = Some(d.id);
        all.push(d);
    }
    let last = all.last().unwrap();
    let mut c = root(last);
    c.format_version = 4;
    c.fence = Some(olap1_fence());
    seed(s, ObjectKey::catalog(), c.encode().unwrap());
    let loaded = LoadedClusterCatalog::new(c, "original-token".into()).unwrap();
    (all, loaded)
}
async fn fresh_absent(s: &Storage<Fake>, loaded: &LoadedClusterCatalog, id: Uuid) -> HistoryLookup {
    // The first lookups may only advance a resumable migration; each attempt
    // gets its own production budget, as the coordinator's attempts do.
    let mut last = None;
    for _ in 0..8 {
        let current = s
            .load_cluster_catalog()
            .await
            .unwrap_or_else(|_| loaded.clone());
        match s
            .lookup_decision(&current, id, b"{}", production_budget())
            .await
        {
            Ok(found) => return found,
            Err(error) => last = Some(error),
        }
    }
    panic!("fresh lookup never completed: {last:?}");
}

#[tokio::test]
async fn olap_1_fresh_request_proves_absence_past_4096_decisions() {
    let s = store();
    let (all, loaded) = fenced_chain(&s, 5000, 0, spread);
    let fresh = Uuid::from_u128(7);
    assert_eq!(
        fresh_absent(&s, &loaded, fresh).await,
        HistoryLookup::Absent
    );
    // Replay of the oldest and the newest decision stays exact after migration.
    let current = s.load_cluster_catalog().await.unwrap();
    assert_eq!(current.catalog().format_version, 6);
    assert_eq!(
        current.catalog().decision_head,
        loaded.catalog().decision_head
    );
    assert_eq!(current.catalog().tables, loaded.catalog().tables);
    assert_eq!(current.catalog().fence, loaded.catalog().fence);
    for d in [&all[0], &all[2500], &all[4999]] {
        assert_eq!(
            s.lookup_decision(
                &current,
                d.request_id,
                d.request_body.as_bytes(),
                production_budget()
            )
            .await
            .unwrap(),
            HistoryLookup::Found(d.clone())
        );
        assert_eq!(
            s.lookup_decision(&current, d.request_id, b"{}", production_budget())
                .await
                .unwrap(),
            HistoryLookup::IdentityConflict
        );
    }
}

#[tokio::test]
async fn olap_1_fresh_request_proves_absence_past_64_mib_of_decisions() {
    let s = store();
    let (all, loaded) = fenced_chain(&s, 70, 1024 * 1024, spread);
    assert_eq!(
        fresh_absent(&s, &loaded, Uuid::from_u128(7)).await,
        HistoryLookup::Absent
    );
    let current = s.load_cluster_catalog().await.unwrap();
    assert_eq!(current.catalog().format_version, 6);
    let first = &all[0];
    assert_eq!(
        s.lookup_decision(
            &current,
            first.request_id,
            first.request_body.as_bytes(),
            production_budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Found(first.clone())
    );
}

#[tokio::test]
async fn olap_1_mutations_after_a_long_history_cost_bounded_requests() {
    let s = store();
    let (_, loaded) = fenced_chain(&s, 5000, 0, spread);
    let _ = fresh_absent(&s, &loaded, Uuid::from_u128(7)).await;
    let mut counts = Vec::new();
    for n in 0..20u128 {
        let base = s.load_cluster_catalog().await.unwrap();
        let request = spread(1_000_000 + n);
        let d = padded_decision(
            2_000_000 + n,
            base.catalog().revision,
            base.catalog().decision_head,
            0,
            request,
        );
        let (gets, puts) = {
            let state = s.backend().state.lock().unwrap();
            (state.gets, state.puts)
        };
        let outcome = PreparedPublication::prepare_fenced(
            base.clone(),
            base.catalog().tables.clone(),
            d.clone(),
            olap1_fence(),
        )
        .unwrap()
        .publish(&s, production_budget())
        .await
        .unwrap();
        assert_eq!(outcome, PublicationOutcome::Terminal(d.clone()));
        let state = s.backend().state.lock().unwrap();
        counts.push((state.gets - gets, state.puts - puts));
    }
    println!(
        "olap-1 requests per commit after 5000 decisions, warm cache (gets, puts): {counts:?}"
    );
    // One lookup path and one copy-on-write path, never a history walk.
    for (gets, puts) in counts {
        assert!(gets <= 16, "gets {gets}");
        assert!(puts <= 8, "puts {puts}");
    }
    // A cold worker: a new Storage (empty page cache) over the same objects.
    let cold = Storage::new(Fake::default(), Namespace::new("test").unwrap());
    cold.backend().state.lock().unwrap().objects =
        s.backend().state.lock().unwrap().objects.clone();
    let root = cold.load_cluster_catalog().await.unwrap();
    let mut cold_gets = Vec::new();
    for n in 0..20u128 {
        let before = cold.backend().state.lock().unwrap().gets;
        let fresh = Storage::new(Fake::default(), Namespace::new("test").unwrap());
        fresh.backend().state.lock().unwrap().objects =
            cold.backend().state.lock().unwrap().objects.clone();
        assert_eq!(
            fresh
                .lookup_decision(&root, spread(3_000_000 + n), b"{}", production_budget())
                .await
                .unwrap(),
            HistoryLookup::Absent
        );
        cold_gets.push(fresh.backend().state.lock().unwrap().gets);
        let _ = before;
    }
    println!("olap-1 cold absence proofs after 5020 decisions (page gets): {cold_gets:?}");
    for gets in cold_gets {
        assert!((1..=4).contains(&gets), "cold gets {gets}");
    }
}
