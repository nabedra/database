#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
//! Catalog root initialization, fencing and the indexed decision history
//! (format 6, the only format; OLAP-1).
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
/// The coordinator's budget (server/src/node/coordinator.rs).
fn production_budget() -> HistoryBudget {
    HistoryBudget::new(
        4096,
        64 * 1024 * 1024,
        Instant::now() + Duration::from_secs(60),
    )
    .unwrap()
}
fn fence() -> CatalogFence {
    CatalogFence {
        run_id: Uuid::from_u128(0x77),
        scheduler_term: 1,
    }
}
/// Request IDs that spread over the whole UUID space, unlike from_u128(n).
fn spread(n: u128) -> Uuid {
    Uuid::from_u128(n.wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835) ^ 0x5bd1_e995)
}
fn body(request_id: Uuid, pad: usize) -> String {
    format!(
        "{}{{\"request_id\":\"{request_id}\",\"operation\":\"table.insert\",\"payload\":{{\"table_id\":\"{}\",\"row\":{{\"id\":1.00e0}}}}}} ",
        " ".repeat(pad),
        Uuid::from_u128(99)
    )
}
fn decision_at(n: u128, base: u64, parent: Option<Uuid>) -> Decision {
    let request_id = Uuid::from_u128(n + 1000);
    Decision {
        version: 1,
        id: Uuid::from_u128(n),
        parent,
        base_revision: base,
        revision: base + 1,
        request_id,
        request_body: body(request_id, 0),
        response_status: 200,
        response_body: format!(" {{\"request_id\":\"{request_id}\",\"data\":null}} \t"),
    }
}
fn next_decision(base: &LoadedClusterCatalog, id: Uuid, request_id: Uuid, pad: usize) -> Decision {
    Decision {
        version: 1,
        id,
        parent: base.catalog().decision_head,
        base_revision: base.catalog().revision,
        revision: base.catalog().revision + 1,
        request_id,
        request_body: body(request_id, pad),
        response_status: 200,
        response_body: format!(" {{\"request_id\":\"{request_id}\",\"data\":null}} \t"),
    }
}
/// One committed mutation on the current root, through the real publication.
async fn commit<B: ObjectStore>(
    s: &Storage<B>,
    id: Uuid,
    request_id: Uuid,
    pad: usize,
) -> Decision {
    // Like the coordinator, every attempt uses a fresh commit UUID.
    let mut id = id;
    loop {
        let base = s.load_cluster_catalog().await.unwrap();
        let d = next_decision(&base, id, request_id, pad);
        let outcome = PreparedPublication::prepare_fenced(
            base.clone(),
            base.catalog().tables.clone(),
            d.clone(),
            fence(),
        )
        .unwrap()
        .publish(s, production_budget())
        .await
        .unwrap();
        match outcome {
            PublicationOutcome::Terminal(found) => return found,
            PublicationOutcome::NeedsRecompute => {
                id = Uuid::from_u128(id.as_u128().wrapping_add(1 << 100));
                tokio::task::yield_now().await
            }
            other => panic!("{other:?}"),
        }
    }
}
async fn init(s: &Storage<Fake>) -> LoadedClusterCatalog {
    s.initialize_fenced_cluster_catalog(fence(), false)
        .await
        .unwrap()
}
fn counts(s: &Storage<Fake>) -> (usize, usize) {
    let state = s.backend().state.lock().unwrap();
    (state.gets, state.puts)
}
/// A new Storage (cold page cache) over a copy of the same objects.
fn cold_copy(s: &Storage<Fake>) -> Storage<Fake> {
    let cold = store();
    cold.backend().state.lock().unwrap().objects =
        s.backend().state.lock().unwrap().objects.clone();
    cold
}
fn index_pages(s: &Storage<Fake>) -> Vec<String> {
    let state = s.backend().state.lock().unwrap();
    state
        .objects
        .keys()
        .filter(|k| k.starts_with("test/decision-index/"))
        .cloned()
        .collect()
}

#[tokio::test]
async fn lost_create_response_reconciles_without_second_put() {
    let s = store();
    s.backend().state.lock().unwrap().lose_put_response = true;
    let loaded = init(&s).await;
    let state = s.backend().state.lock().unwrap();
    assert_eq!(state.puts, 1);
    assert_eq!(state.generation, 1); // The write applied before the response was lost.
    assert_eq!(
        ClusterCatalog::decode(&state.objects["test/catalog.json"].0).unwrap(),
        *loaded.catalog()
    );
    assert_eq!(loaded.catalog().format_version, 6);
    assert_eq!(loaded.catalog().revision, 0);
    assert_eq!(loaded.catalog().decision_index, None);
}

#[tokio::test]
async fn invalid_or_older_roots_are_refused_without_writes() {
    let c = ClusterCatalog::empty(fence(), None).unwrap();
    let value = |f: &dyn Fn(&mut serde_json::Value)| {
        let mut v: serde_json::Value = serde_json::from_slice(&c.encode().unwrap()).unwrap();
        f(&mut v);
        serde_json::to_vec(&v).unwrap()
    };
    let head = Uuid::from_u128(9);
    let cases = [
        b"{".to_vec(),
        c.encode().unwrap().into_iter().take(25).collect(),
        Catalog::empty().encode().unwrap(),
        value(&|v| v["format_version"] = 5.into()),
        value(&|v| v["format_version"] = 4.into()),
        value(&|v| v["format_version"] = 99.into()),
        value(&|v| {
            v["decision_head"] = head.to_string().into();
            v["commit_id"] = head.to_string().into();
        }),
        value(&|v| v["decision_index"] = "00".repeat(32).into()),
        value(&|v| {
            v["decision_head"] = head.to_string().into();
            v["commit_id"] = head.to_string().into();
            v["decision_index"] = "AB".repeat(32).into();
        }),
        value(&|v| {
            v.as_object_mut().unwrap().remove("fence");
        }),
        value(&|v| {
            v.as_object_mut().unwrap().remove("decision_index");
        }),
        format!(
            "{{\"format_version\":6,\"format_version\":2,\"revision\":0,\"commit_id\":\"{}\",\"tables\":{{}}}}",
            c.commit_id
        )
        .into_bytes(),
    ];
    for (i, bytes) in cases.into_iter().enumerate() {
        let s = store();
        seed(&s, ObjectKey::catalog(), bytes.clone());
        assert!(s.load_cluster_catalog().await.is_err(), "case {i}");
        for cold in [false, true] {
            assert!(
                s.initialize_fenced_cluster_catalog(fence(), cold)
                    .await
                    .is_err(),
                "case {i}"
            );
            assert!(
                s.initialize_owned_cluster_catalog(Uuid::from_u128(5), fence(), cold)
                    .await
                    .is_err(),
                "case {i}"
            );
        }
        let state = s.backend().state.lock().unwrap();
        assert_eq!(state.puts, 0, "case {i}");
        assert_eq!(state.objects["test/catalog.json"].0, bytes);
    }
    // An empty version is never accepted as a root's ETag.
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
    assert!(
        s.initialize_fenced_cluster_catalog(fence(), false)
            .await
            .is_err()
    );
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    // A failed create on an absent root is never retried.
    for error in [StorageError::Conflict, StorageError::Ambiguous] {
        let s = store();
        s.backend()
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert("test/catalog.json".into(), (error, false));
        assert_eq!(
            s.initialize_fenced_cluster_catalog(fence(), false)
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(s.backend().state.lock().unwrap().puts, 1);
    }
}

#[tokio::test]
async fn indexed_replay_identity_conflict_orphan_and_absence() {
    let s = store();
    init(&s).await;
    let first = commit(&s, Uuid::from_u128(1), Uuid::from_u128(1001), 0).await;
    let last = commit(&s, Uuid::from_u128(2), Uuid::from_u128(1002), 0).await;
    let orphan = decision_at(3, 9, None);
    seed(&s, ObjectKey::decision(orphan.id), orphan.encode().unwrap());
    let loaded = s.load_cluster_catalog().await.unwrap();
    assert!(!format!("{loaded:?}").contains("opaque"));
    assert_eq!(loaded.catalog().decision_head, Some(last.id));
    assert_eq!(init(&s).await, loaded);
    for d in [&first, &last] {
        assert_eq!(
            s.lookup_decision(&loaded, d.request_id, d.request_body.as_bytes(), budget())
                .await
                .unwrap(),
            HistoryLookup::Found(d.clone())
        );
        assert_eq!(
            s.lookup_decision(
                &loaded,
                d.request_id,
                format!("{} ", d.request_body).as_bytes(),
                budget()
            )
            .await
            .unwrap(),
            HistoryLookup::IdentityConflict
        );
        assert_eq!(
            s.lookup_decision_by_id(&loaded, d.request_id, budget())
                .await
                .unwrap(),
            Some(d.clone())
        );
    }
    // A decision object that no index reaches is invisible.
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
    // An older root does not see a later commit.
    let s2 = store();
    let empty = init(&s2).await;
    let early = commit(&s2, Uuid::from_u128(7), Uuid::from_u128(1007), 0).await;
    assert_eq!(
        s2.lookup_decision_by_id(&empty, early.request_id, budget())
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn damaged_index_or_decision_never_proves_absence() {
    // Sequential request IDs share 15 bytes: the deepest path, 16 pages.
    let s = store();
    init(&s).await;
    let a = commit(&s, Uuid::from_u128(1), Uuid::from_u128(1001), 0).await;
    let superseded = index_pages(&s);
    commit(&s, Uuid::from_u128(2), Uuid::from_u128(1002), 0).await;
    let loaded = s.load_cluster_catalog().await.unwrap();
    // The pages of the current index: the second commit's copied path.
    let pages: Vec<_> = index_pages(&s)
        .into_iter()
        .filter(|p| !superseded.contains(p))
        .collect();
    assert!(pages.len() >= 16, "{}", pages.len());
    // Every page, missing or with one flipped bit, is damage on both a hit and
    // an absence proof.
    for page in &pages {
        for damage in 0..2 {
            let cold = cold_copy(&s);
            {
                let mut state = cold.backend().state.lock().unwrap();
                if damage == 0 {
                    state.objects.remove(page);
                } else {
                    state.objects.get_mut(page).unwrap().0[8] ^= 1;
                }
            }
            for request in [a.request_id, Uuid::from_u128(1003)] {
                let got = cold.lookup_decision(&loaded, request, b"", budget()).await;
                assert_ne!(got, Ok(HistoryLookup::Absent), "{page} damage {damage}");
            }
        }
    }
    // A missing or mismatched decision behind an entry is damage too.
    let cold = cold_copy(&s);
    cold.backend()
        .state
        .lock()
        .unwrap()
        .objects
        .remove(&format!("test/{}", ObjectKey::decision(a.id).as_str()));
    assert_eq!(
        cold.lookup_decision_by_id(&loaded, a.request_id, budget())
            .await,
        Err(StorageError::InvalidCatalog)
    );
    let cold = cold_copy(&s);
    let mut wrong = decision_at(9, 0, None);
    wrong.id = a.id;
    seed(&cold, ObjectKey::decision(a.id), wrong.encode().unwrap());
    assert_eq!(
        cold.lookup_decision_by_id(&loaded, a.request_id, budget())
            .await,
        Err(StorageError::InvalidCatalog)
    );
    // Budgets bound uncached reads and never turn into absence.
    let cold = cold_copy(&s);
    for b in [
        HistoryBudget::new(10, 1 << 20, Instant::now() + Duration::from_secs(30)).unwrap(),
        HistoryBudget::new(100, 100, Instant::now() + Duration::from_secs(30)).unwrap(),
    ] {
        assert_eq!(
            cold.lookup_decision(&loaded, Uuid::from_u128(1003), b"", b)
                .await,
            Err(StorageError::LimitExceeded)
        );
    }
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
async fn concurrent_create_uses_backend_cas() {
    let backend = Concurrent {
        fake: Fake::default(),
        barrier: tokio::sync::Barrier::new(2),
        reads: AtomicUsize::new(0),
    };
    let s = Storage::new(backend, Namespace::new("test").unwrap());
    let (a, b) = tokio::join!(
        s.initialize_fenced_cluster_catalog(fence(), false),
        s.initialize_fenced_cluster_catalog(fence(), false)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let state = s.backend().fake.state.lock().unwrap();
    assert_eq!(state.puts, 2);
    assert_eq!(state.generation, 1);
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
async fn unreadable_roots_and_slow_or_partial_pages_fail_safely() {
    let s = store();
    init(&s).await;
    commit(&s, Uuid::from_u128(1), spread(1), 0).await;
    let root_page = index_pages(&s).pop().unwrap();
    let puts = counts(&s).1;
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
        *s.backend().fault.lock().unwrap() = Some((root_page.clone(), fault));
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
        s.initialize_fenced_cluster_catalog(fence(), false)
            .await
            .unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(s.backend().fake.state.lock().unwrap().puts, puts);
}

#[tokio::test]
async fn sl05_owner_cluster_is_conditional() {
    let s = store();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let run = CatalogFence {
        run_id: Uuid::new_v4(),
        scheduler_term: 1,
    };
    let first = s
        .initialize_owned_cluster_catalog(a, run, false)
        .await
        .unwrap();
    assert_eq!(first.catalog().owner_cluster, Some(a));
    assert_eq!(first.catalog().format_version, 6);
    assert_eq!(
        s.initialize_owned_cluster_catalog(a, run, false)
            .await
            .unwrap(),
        first
    );
    let puts = s.backend().state.lock().unwrap().puts;
    assert_eq!(
        s.initialize_owned_cluster_catalog(b, run, true).await,
        Err(StorageError::Conflict)
    );
    assert_eq!(
        s.initialize_fenced_cluster_catalog(fence(), false).await,
        Err(StorageError::Conflict)
    );
    assert_eq!(s.backend().state.lock().unwrap().puts, puts);
    // An unowned root never becomes owned.
    let other = store();
    init(&other).await;
    assert_eq!(
        other.initialize_owned_cluster_catalog(a, run, true).await,
        Err(StorageError::Conflict)
    );
    assert_eq!(other.backend().state.lock().unwrap().puts, 1);
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
        .initialize_owned_cluster_catalog(cluster, old, false)
        .await
        .unwrap();
    let mut d = decision_at(551, base.catalog().revision, base.catalog().decision_head);
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
        s.initialize_owned_cluster_catalog(cluster, fresh, false)
            .await,
        Err(StorageError::Conflict)
    );
    let fenced = s
        .initialize_owned_cluster_catalog(cluster, fresh, true)
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
            decision_at(552, 0, None),
            cluster,
            old
        )
        .unwrap_err(),
        StorageError::Conflict
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
fn legacy_table(n: u128) -> (CatalogEntry, ObjectKey) {
    let table_id = Uuid::from_u128(0x5000 + n);
    let object = ObjectKey::snapshot(table_id, Uuid::from_u128(0x9000 + n));
    let entry = serde_json::from_value(serde_json::json!({"id":table_id,"schema":{"id":"int64"},"key":"id","format":"parquet","data":{"layout":"legacy","snapshot":object.as_str()},"row_count":0})).unwrap();
    (entry, object)
}
#[tokio::test]
async fn sl02_warm_catalog_does_not_authorize_denied_backing_objects() {
    let s = Storage::new(SelectiveGrant::default(), Namespace::new("test").unwrap());
    let (entry, object) = legacy_table(1);
    let mut catalog = ClusterCatalog::empty(fence(), Some(Uuid::new_v4())).unwrap();
    catalog.tables.insert("rows".into(), entry);
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

// OLAP-1: the decision history must not bound the number of writes. The
// shipped lookup walked the whole chain within the production budget (4096
// records, 64 MiB, 60 s), so the 4097th decision, or the 65th of 1 MiB, could
// no longer prove a fresh request absent, and every mutation failed for good.
#[tokio::test]
async fn olap_1_writes_continue_past_the_old_history_budget() {
    let s = store();
    init(&s).await;
    let mut all = Vec::new();
    let mut costs = Vec::new();
    for n in 1..=5000u128 {
        let before = counts(&s);
        all.push(commit(&s, Uuid::from_u128(n), spread(n), 0).await);
        let after = counts(&s);
        if [1, 10, 100, 1000, 4096, 4097, 5000].contains(&n) {
            costs.push((n, after.0 - before.0, after.1 - before.1));
        }
    }
    // (commit number, GETs, PUTs) with this Storage's warm page cache; the
    // GET is the root load. The shipped chain walk needed 2N more GETs per
    // commit and failed past N = 4096.
    println!("olap-1 commit cost, warm (n, gets, puts): {costs:?}");
    for (_, gets, puts) in &costs {
        assert!(*gets <= 2, "gets {gets}");
        assert!(*puts <= 8, "puts {puts}");
    }
    let loaded = s.load_cluster_catalog().await.unwrap();
    assert_eq!(loaded.catalog().revision, 5000);
    for d in [&all[0], &all[2499], &all[4096], &all[4999]] {
        assert_eq!(
            s.lookup_decision(
                &loaded,
                d.request_id,
                d.request_body.as_bytes(),
                production_budget()
            )
            .await
            .unwrap(),
            HistoryLookup::Found(d.clone())
        );
        assert_eq!(
            s.lookup_decision(&loaded, d.request_id, b"{}", production_budget())
                .await
                .unwrap(),
            HistoryLookup::IdentityConflict
        );
    }
    // A cold worker: an empty page cache over the same objects.
    let mut cold_gets = Vec::new();
    for n in 0..20u128 {
        let cold = cold_copy(&s);
        assert_eq!(
            cold.lookup_decision(&loaded, spread(3_000_000 + n), b"{}", production_budget())
                .await
                .unwrap(),
            HistoryLookup::Absent
        );
        cold_gets.push(counts(&cold).0);
    }
    println!("olap-1 cold absence proofs after 5000 decisions (page gets): {cold_gets:?}");
    for gets in &cold_gets {
        assert!((1..=4).contains(gets), "cold gets {gets}");
    }
    let cold = cold_copy(&s);
    let before = counts(&cold);
    commit(&cold, Uuid::from_u128(9_000_001), spread(9_000_001), 0).await;
    let after = counts(&cold);
    println!(
        "olap-1 cold commit after 5000 decisions: {} gets, {} puts",
        after.0 - before.0,
        after.1 - before.1
    );
    assert!(after.0 - before.0 <= 6);
}

#[tokio::test]
async fn olap_1_large_decisions_do_not_bound_the_history() {
    let s = store();
    init(&s).await;
    let mut all = Vec::new();
    for n in 1..=70u128 {
        all.push(commit(&s, Uuid::from_u128(n), spread(n), 1024 * 1024).await);
    }
    let loaded = s.load_cluster_catalog().await.unwrap();
    let cold = cold_copy(&s);
    assert_eq!(
        cold.lookup_decision(&loaded, Uuid::from_u128(7), b"{}", production_budget())
            .await
            .unwrap(),
        HistoryLookup::Absent
    );
    assert_eq!(
        cold.lookup_decision(
            &loaded,
            all[0].request_id,
            all[0].request_body.as_bytes(),
            production_budget()
        )
        .await
        .unwrap(),
        HistoryLookup::Found(all[0].clone())
    );
}

/// Yields before every request so that concurrent writers interleave.
#[derive(Default)]
struct Yielding {
    fake: Fake,
}
impl ObjectStore for Yielding {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        tokio::task::yield_now().await;
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        tokio::task::yield_now().await;
        self.fake.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn olap_1_publication_cost_under_contention() {
    let s = Storage::new(Yielding::default(), Namespace::new("test").unwrap());
    s.initialize_fenced_cluster_catalog(fence(), false)
        .await
        .unwrap();
    for n in 0..1000u128 {
        commit(&s, Uuid::from_u128(1 + n), spread(1 + n), 0).await;
    }
    let (gets, puts) = {
        let state = s.backend().fake.state.lock().unwrap();
        (state.gets, state.puts)
    };
    let writers = 4u128;
    let each = 25u128;
    futures_util::future::join_all((0..writers).map(|w| {
        let s = &s;
        async move {
            for i in 0..each {
                let n = 10_000 + w * 1000 + i;
                commit(s, Uuid::from_u128(n), spread(n), 0).await;
            }
        }
    }))
    .await;
    let (per_get, per_put) = {
        let state = s.backend().fake.state.lock().unwrap();
        let done = (writers * each) as f64;
        (
            (state.gets - gets) as f64 / done,
            (state.puts - puts) as f64 / done,
        )
    };
    println!(
        "olap-1 contention, {writers} writers x {each} commits: {per_get:.1} gets and {per_put:.1} puts per commit"
    );
    let loaded = s.load_cluster_catalog().await.unwrap();
    assert_eq!(loaded.catalog().revision, 1000 + 100);
    // Every committed identity is replayable from the final index.
    for w in 0..writers {
        for i in 0..each {
            let n = 10_000 + w * 1000 + i;
            assert!(
                s.lookup_decision_by_id(&loaded, spread(n), production_budget())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
    assert!(per_put < 30.0);
}

// OLAP-5: reauthorizing a customer catalog probes every backing object. With
// 1,500 objects at 20 ms per probe the old sequential loop needed 30 s, the
// whole deadline, and the caller answered `unauthorized` for a slow store.
#[derive(Default)]
struct SlowGrant {
    backend: Fake,
    probes: AtomicUsize,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}
impl ObjectStore for SlowGrant {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        if key.contains("/tables/") {
            self.probes.fetch_add(1, Ordering::SeqCst);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            return Ok(Download::from_bytes(vec![1], "\"t\"".into()));
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
#[tokio::test(start_paused = true)]
async fn olap_5_authorization_of_many_objects_fits_its_deadline() {
    let s = Storage::new(SlowGrant::default(), Namespace::new("test").unwrap());
    let mut catalog = ClusterCatalog::empty(fence(), Some(Uuid::from_u128(1))).unwrap();
    for n in 0..1500u128 {
        catalog.tables.insert(format!("t{n}"), legacy_table(n).0);
    }
    let loaded = LoadedClusterCatalog::new(catalog, "e".into()).unwrap();
    let start = Instant::now();
    let result = s
        .authorize_catalog_objects(&loaded, start + Duration::from_secs(30))
        .await;
    let elapsed = start.elapsed();
    let grant = s.backend();
    println!(
        "olap-5 1500 probes: {result:?} in {elapsed:?} (virtual), {} probes, peak {} in flight",
        grant.probes.load(Ordering::SeqCst),
        grant.peak.load(Ordering::SeqCst)
    );
    assert_eq!(result, Ok(()));
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    assert!(grant.peak.load(Ordering::SeqCst) <= 16);
}
