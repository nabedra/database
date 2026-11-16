use nabedra_database_server::storage::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Barrier, Semaphore},
    time::{Duration, Instant},
};
use uuid::Uuid;

struct Gate {
    entered: Semaphore,
    release: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.acquire().await.unwrap().forget();
    }
    async fn stop(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    fn open(&self) {
        self.release.add_permits(1);
    }
}
#[derive(Default)]
struct State {
    objects: BTreeMap<String, (Vec<u8>, String)>,
    generation: usize,
    puts: Vec<String>,
    race: Option<Arc<Barrier>>,
    lost_root: Option<Arc<Gate>>,
    pending_root: Option<Arc<Gate>>,
    pending_decision: Option<Arc<Gate>>,
    lost_decision: bool,
    root_absent_reply: bool,
    empty_root_token: bool,
    fail_reads: bool,
}
#[derive(Default)]
struct Backend(Mutex<State>);
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let s = self.0.lock().unwrap();
        if s.fail_reads {
            return Err(StorageError::Unavailable);
        }
        let (bytes, token) = s.objects.get(key).ok_or(StorageError::NotFound)?;
        Ok(Download::from_bytes(bytes.clone(), token.clone()))
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let root = key == "test/catalog.json";
        let (race, pending, lost) = {
            let mut s = self.0.lock().unwrap();
            s.puts.push(key.into());
            if root {
                (s.race.clone(), s.pending_root.take(), s.lost_root.take())
            } else {
                (None, s.pending_decision.take(), None)
            }
        };
        if let Some(gate) = pending {
            gate.stop().await;
        }
        if let Some(barrier) = race {
            barrier.wait().await;
        }
        let (token, empty) = {
            let mut s = self.0.lock().unwrap();
            if root && s.root_absent_reply {
                return Err(StorageError::Ambiguous);
            }
            match condition {
                WriteCondition::Create if s.objects.contains_key(key) => {
                    return Err(StorageError::Conflict);
                }
                WriteCondition::Match(token)
                    if s.objects.get(key).is_none_or(|(_, actual)| actual != token) =>
                {
                    return Err(StorageError::Conflict);
                }
                _ => {}
            }
            s.generation += 1;
            let token = format!("private-token-{}", s.generation);
            s.objects.insert(key.into(), (bytes, token.clone()));
            if !root && s.lost_decision {
                return Err(StorageError::Ambiguous);
            }
            (token, root && s.empty_root_token)
        };
        if let Some(gate) = lost {
            gate.stop().await;
            return Err(StorageError::Ambiguous);
        }
        Ok(if empty { String::new() } else { token })
    }
}
fn budget() -> HistoryBudget {
    HistoryBudget::new(
        100,
        2 * MAX_DECISION_BYTES,
        Instant::now() + Duration::from_secs(10),
    )
    .unwrap()
}
async fn store() -> (Storage<Backend>, LoadedClusterCatalog) {
    let s = Storage::new(Backend::default(), Namespace::new("test").unwrap());
    let base = s.initialize_cluster_catalog().await.unwrap();
    s.backend().0.lock().unwrap().puts.clear();
    (s, base)
}
fn decision(base: &LoadedClusterCatalog, n: u128) -> Decision {
    let request_id = Uuid::from_u128(n + 1000);
    Decision {
        version: 1,
        id: Uuid::from_u128(n),
        parent: base.catalog().decision_head,
        base_revision: base.catalog().revision,
        revision: base.catalog().revision + 1,
        request_id,
        request_body: format!(
            " {{\"request_id\":\"{request_id}\",\"operation\":\"table.insert\",\"payload\":{{\"table_id\":\"{}\",\"row\":{{\"id\":1.00e0}}}}}} ",
            Uuid::from_u128(99)
        ),
        response_status: 200,
        response_body: format!(" {{\"request_id\":\"{request_id}\",\"data\":null}} \t"),
    }
}
fn entry(n: u128) -> CatalogEntry {
    serde_json::from_value(serde_json::json!({"id":Uuid::from_u128(n),"schema":{"id":"int64"},"key":"id","format":"parquet","data":{"layout":"partitioned","files":[]},"row_count":0})).unwrap()
}
fn prepare(base: &LoadedClusterCatalog, d: Decision) -> PreparedPublication {
    PreparedPublication::prepare(base.clone(), base.catalog().tables.clone(), d).unwrap()
}
fn seed(s: &Storage<Backend>, key: ObjectKey, bytes: Vec<u8>) {
    s.backend().0.lock().unwrap().objects.insert(
        format!("test/{}", key.as_str()),
        (bytes, "seed-token".into()),
    );
}
fn roots(s: &Storage<Backend>) -> usize {
    s.backend()
        .0
        .lock()
        .unwrap()
        .puts
        .iter()
        .filter(|s| s.as_str() == "test/catalog.json")
        .count()
}

#[tokio::test]
async fn atomic_tables_and_exact_replay_including_noop_and_error() {
    let (s, base) = store().await;
    let d = decision(&base, 1);
    let mut tables = base.catalog().tables.clone();
    tables.insert("users".into(), entry(50));
    let p = PreparedPublication::prepare(base, tables.clone(), d.clone()).unwrap();
    assert!(!format!("{p:?}").contains("private-token"));
    assert!(!format!("{p:?}").contains("users"));
    assert!(!format!("{p:?}").contains("request_id"));
    assert_eq!(p.commit_id(), d.id);
    assert_eq!(
        p.publish(&s, budget()).await.unwrap(),
        PublicationOutcome::Terminal(d.clone())
    );
    let base = s.load_cluster_catalog().await.unwrap();
    assert_eq!(base.catalog().tables, tables);
    assert_eq!(base.catalog().decision_head, Some(d.id));
    let mut retry = decision(&base, 2);
    retry.request_id = d.request_id;
    retry.request_body = d.request_body.clone();
    retry.response_body = format!("{{\"request_id\":\"{}\",\"data\":false}}", d.request_id);
    assert_eq!(
        prepare(&base, retry).publish(&s, budget()).await.unwrap(),
        PublicationOutcome::Terminal(d)
    );
    assert_eq!(roots(&s), 1);
    for error in [false, true] {
        let base = s.load_cluster_catalog().await.unwrap();
        let mut d = decision(&base, if error { 4 } else { 3 });
        if error {
            d.response_status = 409;
            d.response_body = format!(
                "{{\"request_id\":\"{}\",\"error\":{{\"code\":\"conflict\",\"message\":\"duplicate key\",\"outcome\":\"not_applied\"}}}}",
                d.request_id
            );
        }
        assert_eq!(
            prepare(&base, d.clone())
                .publish(&s, budget())
                .await
                .unwrap(),
            PublicationOutcome::Terminal(d.clone())
        );
        let current = s.load_cluster_catalog().await.unwrap();
        assert_eq!(current.catalog().tables, tables);
        assert_eq!(current.catalog().revision, d.revision);
    }
}

#[tokio::test]
async fn preparation_rejects_relationships_tables_and_bounds_without_puts() {
    let (s, base) = store().await;
    let d = decision(&base, 1);
    let mut bad = d.clone();
    bad.id = base.catalog().commit_id;
    assert!(PreparedPublication::prepare(base.clone(), BTreeMap::new(), bad).is_err());
    let mut bad = d.clone();
    bad.parent = Some(Uuid::from_u128(88));
    assert!(PreparedPublication::prepare(base.clone(), BTreeMap::new(), bad).is_err());
    let mut bad = d.clone();
    bad.base_revision += 1;
    bad.revision += 1;
    assert!(PreparedPublication::prepare(base.clone(), BTreeMap::new(), bad).is_err());
    let mut bad = d.clone();
    bad.revision = 0;
    assert!(PreparedPublication::prepare(base.clone(), BTreeMap::new(), bad).is_err());
    let mut tables = BTreeMap::from([("a".into(), entry(51)), ("b".into(), entry(52))]);
    assert!(PreparedPublication::prepare(base.clone(), tables.clone(), d.clone()).is_err());
    tables.remove("b");
    let mut bad = d.clone();
    bad.response_status = 409;
    bad.response_body = format!(
        "{{\"request_id\":\"{}\",\"error\":{{\"code\":\"conflict\",\"message\":\"duplicate\",\"outcome\":\"not_applied\"}}}}",
        d.request_id
    );
    assert!(PreparedPublication::prepare(base.clone(), tables.clone(), bad).is_err());
    tables.get_mut("a").unwrap().key = "missing".into();
    assert!(PreparedPublication::prepare(base.clone(), tables, d.clone()).is_err());
    let mut bad = d;
    bad.request_body
        .push_str(&" ".repeat(nabedra_database_server::protocol::MAX_BODY_BYTES));
    assert!(matches!(
        PreparedPublication::prepare(base.clone(), BTreeMap::new(), bad),
        Err(StorageError::LimitExceeded)
    ));
    let mut c = base.catalog().clone();
    c.revision = u64::MAX;
    seed(&s, ObjectKey::catalog(), c.encode().unwrap());
    let base = s.load_cluster_catalog().await.unwrap();
    let mut bad = decision_overflow(&base);
    bad.revision = 0;
    assert!(PreparedPublication::prepare(base, BTreeMap::new(), bad).is_err());
    assert!(s.backend().0.lock().unwrap().puts.is_empty());
}
fn decision_overflow(base: &LoadedClusterCatalog) -> Decision {
    let mut d = Decision {
        version: 1,
        id: Uuid::from_u128(3),
        parent: None,
        base_revision: 0,
        revision: 1,
        request_id: Uuid::nil(),
        request_body: String::new(),
        response_status: 200,
        response_body: String::new(),
    };
    d.base_revision = base.catalog().revision;
    d
}

#[tokio::test]
async fn immutable_collision_rejected_and_applied_lost_upload_confirmed_once() {
    for corrupt in [false, true] {
        let (s, base) = store().await;
        let d = decision(&base, 1);
        let mut bytes = d.encode().unwrap();
        bytes.push(b' '); // Valid same decision, but not the exact encoded object.
        if corrupt {
            bytes = b"corrupt".to_vec();
        }
        seed(&s, ObjectKey::decision(d.id), bytes);
        assert!(prepare(&base, d).publish(&s, budget()).await.is_err());
        assert_eq!(roots(&s), 0);
    }
    let (s, base) = store().await;
    let d = decision(&base, 2);
    s.backend().0.lock().unwrap().lost_decision = true;
    assert_eq!(
        prepare(&base, d.clone())
            .publish(&s, budget())
            .await
            .unwrap(),
        PublicationOutcome::Terminal(d)
    );
    assert_eq!(s.backend().0.lock().unwrap().puts.len(), 2);
}

#[tokio::test]
async fn shared_base_races_serialize_and_reconcile_request_identity() {
    for same in [0, 1, 2] {
        let (s, base) = store().await;
        let a = decision(&base, 1);
        let mut b = decision(&base, 2);
        if same > 0 {
            b.request_id = a.request_id;
            b.request_body = a.request_body.clone();
            b.response_body = a.response_body.clone();
        }
        if same == 2 {
            b.request_body.push(' ');
        }
        let pa = prepare(&base, a.clone());
        let pb = prepare(&base, b.clone());
        s.backend().0.lock().unwrap().race = Some(Arc::new(Barrier::new(2)));
        let (ra, rb) = tokio::join!(pa.publish(&s, budget()), pb.publish(&s, budget()));
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        let current = s.load_cluster_catalog().await.unwrap();
        assert_eq!(current.catalog().revision, 1);
        let winner = if current.catalog().commit_id == a.id {
            a
        } else {
            b
        };
        if same == 1 {
            assert_eq!(ra, PublicationOutcome::Terminal(winner.clone()));
            assert_eq!(rb, PublicationOutcome::Terminal(winner));
        } else {
            let loser = if ra == PublicationOutcome::Terminal(winner.clone()) {
                rb
            } else {
                assert_eq!(rb, PublicationOutcome::Terminal(winner));
                ra
            };
            assert_eq!(
                loser,
                if same == 0 {
                    PublicationOutcome::NeedsRecompute
                } else {
                    PublicationOutcome::IdentityConflict
                }
            );
        }
        assert_eq!(roots(&s), 2);
    }
}

#[tokio::test]
async fn applied_a_loses_reply_after_b_commits_and_replays_through_b() {
    let (s, base) = store().await;
    let a = decision(&base, 1);
    let p = prepare(&base, a.clone());
    let gate = Gate::new();
    s.backend().0.lock().unwrap().lost_root = Some(gate.clone());
    let work = async { p.publish(&s, budget()).await.unwrap() };
    let later = async {
        gate.wait().await;
        let current = s.load_cluster_catalog().await.unwrap();
        assert_eq!(current.catalog().commit_id, a.id);
        let b = decision(&current, 2);
        assert_eq!(
            prepare(&current, b.clone())
                .publish(&s, budget())
                .await
                .unwrap(),
            PublicationOutcome::Terminal(b.clone())
        );
        assert_eq!(
            s.load_cluster_catalog()
                .await
                .unwrap()
                .catalog()
                .decision_head,
            Some(b.id)
        );
        gate.open();
    };
    let (result, ()) = tokio::join!(work, later);
    assert_eq!(result, PublicationOutcome::Terminal(a));
    assert_eq!(roots(&s), 2);
}

#[tokio::test]
async fn orphan_and_incomplete_history_and_ambiguous_absence_are_safe() {
    let (s, base) = store().await;
    let d = decision(&base, 1);
    seed(&s, ObjectKey::decision(d.id), d.encode().unwrap());
    assert_eq!(
        prepare(&base, d.clone())
            .publish(&s, budget())
            .await
            .unwrap(),
        PublicationOutcome::Terminal(d)
    );
    let base = s.load_cluster_catalog().await.unwrap();
    let d = decision(&base, 2);
    s.backend().0.lock().unwrap().fail_reads = true;
    assert!(
        prepare(&base, d.clone())
            .publish(&s, budget())
            .await
            .is_err()
    );
    assert_eq!(roots(&s), 1);
    s.backend().0.lock().unwrap().fail_reads = false;
    let small = HistoryBudget::new(1, 1, Instant::now() + Duration::from_secs(1)).unwrap();
    assert!(prepare(&base, d).publish(&s, small).await.is_err());
    assert_eq!(roots(&s), 1);
    let (s, base) = store().await;
    let d = decision(&base, 3);
    s.backend().0.lock().unwrap().root_absent_reply = true;
    assert_eq!(
        prepare(&base, d).publish(&s, budget()).await.unwrap(),
        PublicationOutcome::Unknown
    );
    assert_eq!(s.load_cluster_catalog().await.unwrap(), base);
    assert_eq!(roots(&s), 1);
    let (s, base) = store().await;
    let d = decision(&base, 4);
    s.backend().0.lock().unwrap().empty_root_token = true;
    assert_eq!(
        prepare(&base, d.clone())
            .publish(&s, budget())
            .await
            .unwrap(),
        PublicationOutcome::Terminal(d)
    );
    assert_eq!(roots(&s), 1);
}

#[tokio::test]
async fn deadline_before_root_is_attempt_error_and_pending_root_is_unknown() {
    for root in [false, true] {
        let (s, base) = store().await;
        let d = decision(&base, 1);
        let gate = Gate::new();
        if root {
            s.backend().0.lock().unwrap().pending_root = Some(gate.clone());
        } else {
            s.backend().0.lock().unwrap().pending_decision = Some(gate.clone());
        }
        let b = HistoryBudget::new(
            10,
            MAX_DECISION_BYTES,
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
        let (result, ()) = tokio::join!(prepare(&base, d).publish(&s, b), gate.wait());
        if root {
            assert_eq!(result.unwrap(), PublicationOutcome::Unknown);
        } else {
            assert_eq!(result.unwrap_err(), StorageError::Unavailable);
        }
        assert_eq!(roots(&s), usize::from(root));
        assert_eq!(s.load_cluster_catalog().await.unwrap(), base);
    }
}

#[tokio::test]
async fn encoded_root_bound_is_checked_before_upload() {
    use nabedra_database_server::protocol::Key;
    let (s, base) = store().await;
    let d = decision(&base, 1);
    let mut table = entry(50);
    table.row_count = 6000;
    table.data = TableData::Partitioned {
        files: (0..6000)
            .map(|i| PartitionFile {
                object: ObjectKey::partition(table.id, d.id, i).as_str().into(),
                range: KeyRange {
                    lower: (i > 0).then_some(Key::Int64(i as i64)),
                    upper: (i < 5999).then_some(Key::Int64(i as i64 + 1)),
                },
                row_count: 1,
                byte_size: 100,
                payload_bytes: 8,
            })
            .collect(),
    };
    let mut view = base.catalog().clone();
    view.tables.insert("users".into(), table);
    view.validate().unwrap(); // Valid manifest, specifically the encoded cap fails.
    assert!(matches!(
        PreparedPublication::prepare(base, view.tables, d),
        Err(StorageError::LimitExceeded)
    ));
    assert!(s.backend().0.lock().unwrap().puts.is_empty());
}

#[tokio::test]
async fn pending_original_is_fenced_by_same_id_recovery() {
    for different_body in [false, true] {
        let (s, base) = store().await;
        let a = decision(&base, 1);
        let mut b = decision(&base, 2);
        b.request_id = a.request_id;
        b.request_body = a.request_body.clone();
        b.response_body = a.response_body.clone();
        if different_body {
            b.request_body.push(' ');
        }
        let pa = prepare(&base, a);
        let pb = prepare(&base, b.clone());
        let gate = Gate::new();
        s.backend().0.lock().unwrap().pending_root = Some(gate.clone());
        let original = pa.publish(&s, budget());
        let recovery = async {
            gate.wait().await;
            assert_eq!(s.load_cluster_catalog().await.unwrap(), base);
            assert_eq!(
                pb.publish(&s, budget()).await.unwrap(),
                PublicationOutcome::Terminal(b.clone())
            );
            gate.open();
        };
        let (result, ()) = tokio::join!(original, recovery);
        assert_eq!(
            result.unwrap(),
            if different_body {
                PublicationOutcome::IdentityConflict
            } else {
                PublicationOutcome::Terminal(b.clone())
            }
        );
        let current = s.load_cluster_catalog().await.unwrap();
        assert_eq!(current.catalog().revision, 1);
        assert_eq!(current.catalog().decision_head, Some(b.id));
        assert_eq!(roots(&s), 2);
    }
}

#[tokio::test]
async fn failed_reconciliation_after_applied_root_remains_unknown() {
    let (s, base) = store().await;
    let d = decision(&base, 1);
    let p = prepare(&base, d.clone());
    let gate = Gate::new();
    s.backend().0.lock().unwrap().lost_root = Some(gate.clone());
    let fault = async {
        gate.wait().await;
        assert_eq!(
            s.load_cluster_catalog().await.unwrap().catalog().commit_id,
            d.id
        );
        s.backend().0.lock().unwrap().fail_reads = true;
        gate.open();
    };
    let (result, ()) = tokio::join!(p.publish(&s, budget()), fault);
    assert_eq!(result.unwrap(), PublicationOutcome::Unknown);
    assert_eq!(roots(&s), 1);
    s.backend().0.lock().unwrap().fail_reads = false;
    let base = s.load_cluster_catalog().await.unwrap();
    let next = decision(&base, 2);
    s.backend()
        .0
        .lock()
        .unwrap()
        .objects
        .remove(&format!("test/{}", ObjectKey::decision(d.id).as_str()));
    assert_eq!(
        prepare(&base, next)
            .publish(&s, budget())
            .await
            .unwrap_err(),
        StorageError::InvalidCatalog
    );
    assert_eq!(roots(&s), 1);
}
