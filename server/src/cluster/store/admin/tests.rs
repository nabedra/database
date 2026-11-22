use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    state::Control,
};
use crate::storage::{Download, Namespace};
use crate::test_storage as support;
use nabedra_rt::time;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use support::Fake;
use tokio::{sync::Semaphore, time::Duration};
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn root() -> Value {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let hash = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    json!({"version":1,"cluster_id":id(1),"name":"Fleet_1","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":hash,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":"meshdb.callbacks.v1"},"provisioning":"managed","template":template,"timing":Timing::default(),"revision":1,"change_id":id(2),"policy":{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[],"intents":[]})
}
fn initial() -> Control {
    Control::decode(&serde_json::to_vec(&root()).unwrap()).unwrap()
}
fn budget() -> AdminHistoryBudget {
    AdminHistoryBudget::new(time::now() + Duration::from_secs(5)).unwrap()
}
fn body(request: u128, nodes: u64) -> Vec<u8> {
    format!(
        r#"{{"request_id":"{}","operation":"cluster.scale","payload":{{"nodes":{nodes}}}}}"#,
        id(request)
    )
    .into_bytes()
}
fn decision_key(n: u128) -> String {
    format!("test/{}", ObjectKey::control_decision(id(n)).as_str())
}
#[derive(Default)]
struct Backend {
    fake: Fake,
    before: Mutex<Vec<Arc<Gate>>>,
    after: Mutex<Option<Arc<Gate>>>,
    empty_ack: Mutex<Option<String>>,
    pad_decision: Mutex<bool>,
    matches: Mutex<Vec<String>>,
    delayed: Mutex<bool>,
    pending: Mutex<Option<(Vec<u8>, String)>>,
    reads: Mutex<Vec<String>>,
}
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
    async fn hold(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    async fn entered(&self) -> bool {
        time::timeout(Duration::from_secs(2), self.entered.acquire())
            .await
            .map(|v| v.unwrap().forget())
            .is_ok()
    }
}
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.reads.lock().unwrap().push(key.into());
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        mut bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        if key == KEY {
            let WriteCondition::Match(token) = condition else {
                panic!("root must Match")
            };
            self.matches.lock().unwrap().push(token.into());
        } else {
            assert!(matches!(condition, WriteCondition::Create));
            if *self.pad_decision.lock().unwrap() {
                bytes.push(b' ');
            }
        }
        if key == KEY && std::mem::take(&mut *self.delayed.lock().unwrap()) {
            let WriteCondition::Match(token) = condition else {
                panic!("expected Match")
            };
            *self.pending.lock().unwrap() = Some((bytes, token.into()));
            return Err(StorageError::Ambiguous);
        }
        let before = if key == KEY {
            self.before.lock().unwrap().pop()
        } else {
            None
        };
        if let Some(gate) = before {
            gate.hold().await;
        }
        let result = self.fake.put(key, bytes, condition).await;
        let gate = if key == KEY {
            self.after.lock().unwrap().take()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.hold().await;
        }
        if self.empty_ack.lock().unwrap().as_deref() == Some(key) && result.is_ok() {
            Ok(String::new())
        } else {
            result
        }
    }
}
fn store(root: &Control) -> Storage<Backend> {
    let b = Backend::default();
    b.fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (root.encode().unwrap(), "private-token".into()));
    Storage::new(b, Namespace::new("test").unwrap())
}
fn counts(s: &Storage<Backend>) -> (usize, usize) {
    let f = s.backend().fake.state.lock().unwrap();
    (f.puts, s.backend().reads.lock().unwrap().len())
}
async fn prepare(
    s: &Storage<Backend>,
    r: u128,
    n: u64,
    p: u128,
    b: AdminHistoryBudget,
) -> PreparedAdmin {
    let loaded = s.load_control(b.deadline()).await.unwrap();
    assert_eq!(
        s.lookup_admin(&loaded, id(r), &body(r, n), b)
            .await
            .unwrap(),
        AdminLookup::Absent
    );
    PreparedAdmin::prepare(loaded, id(r), &body(r, n), id(p)).unwrap()
}
// Test-only caller: fixtures supply already authorized, strictly admitted bodies.
async fn call(
    s: &Storage<Backend>,
    r: u128,
    n: u64,
    p: u128,
) -> Result<AdminPublicationOutcome, StorageError> {
    let b = budget();
    let loaded = s.load_control(b.deadline()).await?;
    match s.lookup_admin(&loaded, id(r), &body(r, n), b).await? {
        AdminLookup::Found(d) => Ok(AdminPublicationOutcome::Terminal(d)),
        AdminLookup::IdentityConflict => Ok(AdminPublicationOutcome::IdentityConflict),
        AdminLookup::Absent => {
            PreparedAdmin::prepare(loaded, id(r), &body(r, n), id(p))?
                .publish(s, b)
                .await
        }
    }
}
fn terminal(v: AdminPublicationOutcome) -> ControlDecision {
    let AdminPublicationOutcome::Terminal(d) = v else {
        panic!("expected terminal: {v:?}")
    };
    d
}
#[tokio::test]
async fn competing_captured_roots_require_explicit_recompute() {
    let s = store(&initial());
    let b = budget();
    let base = s.load_control(b.deadline()).await.unwrap();
    assert_eq!(
        s.lookup_admin(&base, id(10), &body(10, 2), b)
            .await
            .unwrap(),
        AdminLookup::Absent
    );
    assert_eq!(
        s.lookup_admin(&base, id(11), &body(11, 3), b)
            .await
            .unwrap(),
        AdminLookup::Absent
    );
    let a = PreparedAdmin::prepare(base.clone(), id(10), &body(10, 2), id(20)).unwrap();
    let c = PreparedAdmin::prepare(base, id(11), &body(11, 3), id(21)).unwrap();
    let gate = Gate::new();
    *s.backend().before.lock().unwrap() = vec![gate.clone(), gate.clone()];
    let (a, c, entered) = tokio::join!(a.publish(&s, b), c.publish(&s, b), async {
        let entered = gate.entered().await && gate.entered().await;
        gate.release.add_permits(2);
        entered
    });
    assert!(entered);
    let a = a.unwrap();
    let c = c.unwrap();
    let loser = match (a, c) {
        (AdminPublicationOutcome::Terminal(_), AdminPublicationOutcome::NeedsRecompute) => (11, 3),
        (AdminPublicationOutcome::NeedsRecompute, AdminPublicationOutcome::Terminal(_)) => (10, 2),
        other => panic!("expected exactly one winner: {other:?}"),
    };
    assert_eq!(counts(&s), (4, 3)); // base + reconciliation root + winning history
    terminal(call(&s, loser.0, loser.1, 22).await.unwrap());
    assert_eq!(counts(&s), (6, 5));
    let loaded = s.load_control(b.deadline()).await.unwrap();
    assert_eq!(loaded.root().policy.desired, loser.1);
    assert_eq!(loaded.root().admin_seq, 2);
}
#[tokio::test]
async fn immutable_confirmation_requires_entire_original_encoding() {
    for changed in [false, true] {
        let s = store(&initial());
        let b = budget();
        let p = prepare(&s, 10, 2, 20, b).await;
        let (_, d) = admin_candidate(&initial(), id(10), &body(10, 2), id(20)).unwrap();
        let mut bytes = d.encode().unwrap();
        if changed {
            bytes.push(b' ');
        }
        assert_eq!(ControlDecision::decode(&bytes).unwrap(), d);
        s.backend()
            .fake
            .state
            .lock()
            .unwrap()
            .objects
            .insert(decision_key(20), (bytes, "immutable".into()));
        let result = p.publish(&s, b).await;
        if changed {
            assert_eq!(result, Err(StorageError::InvalidCatalog));
            assert_eq!(counts(&s), (1, 2));
        } else {
            assert_eq!(terminal(result.unwrap()), d);
            assert_eq!(counts(&s), (2, 2));
        }
    }
    for changed in [false, true] {
        let s = store(&initial());
        let b = budget();
        let p = prepare(&s, 10, 2, 20, b).await;
        *s.backend().pad_decision.lock().unwrap() = changed;
        s.backend()
            .fake
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert(decision_key(20), (StorageError::Ambiguous, true));
        let result = p.publish(&s, b).await;
        if changed {
            assert_eq!(result, Err(StorageError::InvalidCatalog));
            assert_eq!(counts(&s), (1, 2));
            let bytes = s.backend().fake.state.lock().unwrap().objects[&decision_key(20)]
                .0
                .clone();
            ControlDecision::decode(&bytes).unwrap();
        } else {
            terminal(result.unwrap());
            assert_eq!(counts(&s), (2, 2));
        }
    }
}
#[tokio::test]
async fn replay_precedes_counters_and_incomplete_history_never_writes() {
    let s = store(&initial());
    let mut d = terminal(call(&s, 10, 0, 20).await.unwrap());
    let mut root = s
        .load_control(budget().deadline())
        .await
        .unwrap()
        .root()
        .clone();
    assert_eq!(root.policy.revision, 1);
    assert_eq!(root.admin_seq, 1);
    root.revision = u64::MAX;
    root.policy.revision = u64::MAX;
    root.admin_seq = u64::MAX;
    // A matching validated prefix can replay even with unavailable older ancestry.
    d.admin_seq = u64::MAX;
    d.parent = Some(id(99));
    s.backend().fake.state.lock().unwrap().objects.insert(
        decision_key(20),
        (d.encode().unwrap(), "exhausted-history".into()),
    );
    s.backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (root.encode().unwrap(), "exhausted".into()));
    let before = counts(&s).0;
    assert_eq!(terminal(call(&s, 10, 0, 30).await.unwrap()), d);
    assert_eq!(
        call(&s, 10, 1, 31).await.unwrap(),
        AdminPublicationOutcome::IdentityConflict
    );
    s.backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .remove(&decision_key(20));
    assert_eq!(call(&s, 11, 1, 32).await, Err(StorageError::InvalidCatalog));
    assert_eq!(counts(&s).0, before);
}
#[tokio::test]
async fn lost_root_reply_replays_earlier_response_after_later_commits() {
    let s = store(&initial());
    let b = budget();
    let p = prepare(&s, 10, 2, 20, b).await;
    let (_, expected) = admin_candidate(&initial(), id(10), &body(10, 2), id(20)).unwrap();
    s.backend()
        .fake
        .state
        .lock()
        .unwrap()
        .put_faults
        .insert(KEY.into(), (StorageError::Ambiguous, true));
    let gate = Gate::new();
    *s.backend().after.lock().unwrap() = Some(gate.clone());
    let (result, ()) = tokio::join!(p.publish(&s, b), async {
        let entered = gate.entered().await;
        if !entered {
            gate.release.add_permits(1);
            return;
        }
        let result = async {
            let loaded = s.load_control(b.deadline()).await?;
            let mut later = loaded.root().clone();
            later.revision += 1;
            later.change_id = id(40);
            later.scheduler.failures += 1;
            s.put(
                &ObjectKey::control(),
                later.encode().unwrap(),
                WriteCondition::Match(&loaded.token),
            )
            .await?;
            call(&s, 11, 3, 21).await?;
            Ok::<_, StorageError>(())
        }
        .await;
        gate.release.add_permits(1);
        assert!(result.is_ok());
    });
    assert_eq!(terminal(result.unwrap()), expected);
    assert_eq!(counts(&s), (5, 7));
}
#[tokio::test]
async fn delayed_original_is_fenced_only_by_a_successful_cas() {
    for original_first in [false, true] {
        let s = store(&initial());
        let b = budget();
        let p = prepare(&s, 10, 2, 20, b).await;
        *s.backend().delayed.lock().unwrap() = true;
        assert_eq!(
            p.publish(&s, b).await.unwrap(),
            AdminPublicationOutcome::Unknown
        );
        assert_eq!(counts(&s), (1, 2));
        let fresh = prepare(&s, 10, 2, 21, b).await;
        let pending = s.backend().pending.lock().unwrap().take().unwrap();
        // Drive the deferred provider work locally. It outlives the ambiguous
        // client reply, but owns no detached task and still executes Fake CAS.
        let gate = Gate::new();
        let apply = async {
            gate.hold().await;
            s.backend()
                .fake
                .put(KEY, pending.0, WriteCondition::Match(&pending.1))
                .await
        };
        if original_first {
            let (applied, entered) = tokio::join!(apply, async {
                let entered = gate.entered().await;
                gate.release.add_permits(1);
                entered
            });
            assert!(entered);
            applied.unwrap();
            assert_eq!(terminal(fresh.publish(&s, b).await.unwrap()).id, id(20));
            assert_eq!(counts(&s), (4, 5));
        } else {
            let (applied, (entered, recovery)) = tokio::join!(apply, async {
                let entered = gate.entered().await;
                let recovery = fresh.publish(&s, b).await;
                gate.release.add_permits(1);
                (entered, recovery)
            });
            assert!(entered);
            assert_eq!(terminal(recovery.unwrap()).id, id(21));
            assert_eq!(applied, Err(StorageError::Conflict));
            assert_eq!(counts(&s), (4, 3));
        }
        assert_eq!(
            *s.backend().matches.lock().unwrap(),
            vec!["private-token", "private-token"]
        );
    }
}

#[tokio::test]
async fn reconciliation_failures_and_definite_adapter_errors_stay_distinct() {
    for error in [StorageError::Conflict, StorageError::Ambiguous] {
        for fault in 0..6 {
            let s = store(&initial());
            let b = budget();
            let p = prepare(&s, 10, 2, 20, b).await;
            s.backend()
                .fake
                .state
                .lock()
                .unwrap()
                .put_faults
                .insert(KEY.into(), (error, false));
            let gate = Gate::new();
            *s.backend().after.lock().unwrap() = Some(gate.clone());
            let (result, entered) = tokio::join!(p.publish(&s, b), async {
                let entered = gate.entered().await;
                if entered {
                    let mut f = s.backend().fake.state.lock().unwrap();
                    match fault {
                        0 => {
                            f.objects.remove(KEY);
                        }
                        1 => {
                            f.objects.insert(KEY.into(), (b"{}".to_vec(), "bad".into()));
                        }
                        2 => {
                            let mut r = initial();
                            r.admin_seq = 1;
                            r.admin_head = Some(id(99));
                            r.revision = 2;
                            f.objects.insert(
                                KEY.into(),
                                (r.encode().unwrap(), "missing-history".into()),
                            );
                        }
                        3 => f.read_failure = true,
                        5 => {
                            let (r, d) =
                                admin_candidate(&initial(), id(10), &body(10, 3), id(30)).unwrap();
                            f.objects
                                .insert(KEY.into(), (r.encode().unwrap(), "other-body".into()));
                            f.objects
                                .insert(decision_key(30), (d.encode().unwrap(), "decision".into()));
                        }
                        _ => (),
                    }
                }
                gate.release.add_permits(1);
                entered
            });
            assert!(entered);
            assert_eq!(
                result.unwrap(),
                if fault == 5 {
                    AdminPublicationOutcome::IdentityConflict
                } else if fault == 4 && error == StorageError::Conflict {
                    AdminPublicationOutcome::NeedsRecompute
                } else {
                    AdminPublicationOutcome::Unknown
                }
            );
            assert_eq!(
                counts(&s),
                (2, if fault == 2 || fault == 5 { 3 } else { 2 })
            );
            assert_eq!(s.backend().matches.lock().unwrap().len(), 1);
        }
    }
    for key in [KEY.to_owned(), decision_key(20)] {
        let s = store(&initial());
        let b = budget();
        let p = prepare(&s, 10, 2, 20, b).await;
        s.backend()
            .fake
            .state
            .lock()
            .unwrap()
            .put_faults
            .insert(key.clone(), (StorageError::Unavailable, false));
        assert_eq!(p.publish(&s, b).await, Err(StorageError::Unavailable));
        assert_eq!(counts(&s), (if key == KEY { 2 } else { 1 }, 1));
    }
}
#[tokio::test]
async fn empty_ack_caps_and_deadlines_do_not_resubmit() {
    for key in [KEY.to_owned(), decision_key(20)] {
        let s = store(&initial());
        let b = budget();
        let p = prepare(&s, 10, 2, 20, b).await;
        let debug = format!("{p:?}");
        for secret in [
            "private-token",
            "request_body",
            "fingerprint",
            "/token",
            "nodes",
        ] {
            assert!(!debug.contains(secret));
        }
        *s.backend().empty_ack.lock().unwrap() = Some(key.clone());
        terminal(p.publish(&s, b).await.unwrap());
        assert_eq!(counts(&s), (2, if key == KEY { 3 } else { 2 }));
        assert_eq!(*s.backend().matches.lock().unwrap(), vec!["private-token"]);
    }
    let s = store(&initial());
    let b = budget();
    let p = prepare(&s, 10, 2, 20, b).await;
    let mut oversized = vec![b' '; MAX_CONTROL_DECISION_BYTES + 1];
    oversized[0] = b'{';
    s.backend()
        .fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(decision_key(20), (oversized, "oversized".into()));
    assert_eq!(p.publish(&s, b).await, Err(StorageError::LimitExceeded));
    assert_eq!(counts(&s), (1, 2));
    assert!(s.backend().matches.lock().unwrap().is_empty());
    for after in [false, true] {
        let s = store(&initial());
        let b = AdminHistoryBudget::new(time::now() + Duration::from_millis(100)).unwrap();
        let p = prepare(&s, 10, 2, 20, b).await;
        let gate = Gate::new();
        if after {
            *s.backend().after.lock().unwrap() = Some(gate.clone());
        } else {
            *s.backend().before.lock().unwrap() = vec![gate.clone()];
        }
        let (result, entered) = tokio::join!(p.publish(&s, b), gate.entered());
        gate.release.add_permits(1);
        assert!(entered);
        assert_eq!(result.unwrap(), AdminPublicationOutcome::Unknown);
        assert_eq!(counts(&s), (if after { 2 } else { 1 }, 1));
        assert_eq!(s.backend().matches.lock().unwrap().len(), 1);
    }
}
