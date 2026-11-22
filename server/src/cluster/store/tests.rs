use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    controller::Owner,
};
use crate::storage::{Download, Namespace};
use crate::test_storage as support;
use nabedra_rt::{task, time};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use support::{Fake, ReadGate};
use tokio::sync::Semaphore;
use tokio::time::Duration;
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
pub(super) fn root() -> Value {
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
fn deadline() -> Instant {
    time::now() + Duration::from_secs(5)
}

struct Gate {
    entered: Semaphore,
    release: Semaphore,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}
impl Gate {
    async fn hold(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    async fn entered(&self) {
        time::timeout(Duration::from_secs(5), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
}
#[derive(Default)]
struct Backend {
    fake: Fake,
    before: Mutex<Option<Arc<Gate>>>,
    after: Mutex<Option<Arc<Gate>>>,
    empty_ack: Mutex<bool>,
    conditions: Mutex<Vec<String>>,
}
impl ObjectStore for Backend {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let WriteCondition::Match(token) = condition else {
            panic!("ownership must Match")
        };
        self.conditions.lock().unwrap().push(token.to_owned());
        let gate = self.before.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.hold().await;
        }
        let result = self.fake.put(key, bytes, condition).await;
        let gate = self.after.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.hold().await;
        }
        if *self.empty_ack.lock().unwrap() && result.is_ok() {
            Ok(String::new())
        } else {
            result
        }
    }
}
fn store(root: &Control) -> Storage<Backend> {
    let b = Backend::default();
    b.fake.state.lock().unwrap().objects.insert(
        KEY.into(),
        (root.encode().unwrap(), "captured-private-token".into()),
    );
    Storage::new(b, Namespace::new("test").unwrap())
}
fn prepare(
    base: LoadedControl,
    change: OwnershipChange,
    now: u64,
    proposal: u128,
) -> PreparedOwnership {
    PreparedOwnership::prepare(base, change, now, id(proposal)).unwrap()
}
fn acquire() -> OwnershipChange {
    OwnershipChange::Acquire { process_id: id(10) }
}
fn owner() -> Owner {
    Owner {
        process_id: id(10),
        epoch: 1,
    }
}
fn held() -> Control {
    ownership_candidate(&initial(), acquire(), 100, id(3)).unwrap()
}
fn counts(s: &Storage<Backend>) -> (usize, usize) {
    let f = s.backend().fake.state.lock().unwrap();
    (f.puts, f.gets)
}
struct Task<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn spawn<T: Send + 'static>(
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> Task<T> {
    Task(task::spawn(future))
}
async fn finish<T>(task: Task<T>) -> T {
    // Abort and join on watchdog expiry; no detached fixture tasks.
    let mut task = task;
    match time::timeout(Duration::from_secs(5), &mut task.0).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            task.0.abort();
            let _ = (&mut task.0).await;
            panic!("submission watchdog expired")
        }
    }
}
#[tokio::test]
async fn captured_token_preservation_and_local_rejections() {
    let mut value = root();
    value["nodes"] = json!([{"node_id":id(100),"launch_id":id(101),"resource_id":"resource/one","ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}]);
    value["intents"] = json!([{"request_id":id(101),"node_id":id(100),"launch_id":id(101),"incarnation_id":null,"resource_id":"resource/one","action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":value["config"]["template_fingerprint"],"delivery":null,"outcome":"resource_known"}]);
    let mut base = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
    base.revision = 8;
    base.policy.revision = 4;
    base.admin_seq = 2;
    base.admin_head = Some(id(55));
    base.scheduler.failures = 7;
    base.scheduler.backoff_until_ms = 800;
    let s = store(&base);
    let loaded = s.load_control(deadline()).await.unwrap();
    for (change, now, proposal, error) in [
        (
            OwnershipChange::Renew(owner()),
            200,
            id(4),
            StorageError::Conflict,
        ),
        (acquire(), 100, base.change_id, StorageError::Conflict),
        (acquire(), u64::MAX, id(4), StorageError::LimitExceeded),
    ] {
        assert_eq!(
            PreparedOwnership::prepare(loaded.clone(), change, now, proposal).unwrap_err(),
            error
        );
    }
    let mut invalid = loaded.clone();
    invalid.root.name = "invalid name".into();
    assert_eq!(
        PreparedOwnership::prepare(invalid, acquire(), 100, id(4)).unwrap_err(),
        StorageError::InvalidCatalog
    );
    let mut exhausted = loaded.clone();
    exhausted.root.revision = u64::MAX;
    assert_eq!(
        PreparedOwnership::prepare(exhausted, acquire(), 100, id(4)).unwrap_err(),
        StorageError::LimitExceeded
    );
    assert_eq!(counts(&s), (0, 1));
    let p = prepare(loaded, acquire(), 100, 4);
    let dbg = format!("{p:?}");
    assert!(
        !dbg.contains("captured-private-token")
            && !dbg.contains("fingerprint")
            && !dbg.contains("/token")
    );
    let expected = ownership_candidate(&base, acquire(), 100, id(4)).unwrap();
    let OwnershipOutcome::Acknowledged(loaded) = p.submit(&s, deadline()).await.unwrap() else {
        panic!("expected ack")
    };
    assert_eq!(loaded.root(), &expected);
    let mut restored = loaded.root().clone();
    restored.revision = base.revision;
    restored.change_id = base.change_id;
    restored.controller = base.controller.clone();
    assert_eq!(restored.encode().unwrap(), base.encode().unwrap());
    assert_eq!(
        *s.backend().conditions.lock().unwrap(),
        vec!["captured-private-token"]
    );
    assert_eq!(counts(&s), (1, 1));
    assert_eq!(s.backend().fake.state.lock().unwrap().put_keys, vec![KEY]);
    assert_eq!(loaded, s.load_control(deadline()).await.unwrap());
}
#[tokio::test]
async fn two_acquisitions_have_exactly_one_winner() {
    let s = store(&initial());
    let base = s.load_control(deadline()).await.unwrap();
    let a = prepare(base.clone(), acquire(), 100, 4);
    let b = prepare(
        base,
        OwnershipChange::Acquire { process_id: id(11) },
        100,
        5,
    );
    let (a, b) = tokio::join!(a.submit(&s, deadline()), b.submit(&s, deadline()));
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|v| matches!(v, OwnershipOutcome::Acknowledged(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|v| matches!(v, OwnershipOutcome::Conflict { current: Some(_) }))
            .count(),
        1
    );
    assert_eq!(counts(&s), (2, 2));
    assert_eq!(s.backend().fake.state.lock().unwrap().generation, 1);
}
#[tokio::test]
async fn takeover_fences_renewal_held_before_match() {
    let root = held();
    let s = Arc::new(store(&root));
    let base = s.load_control(deadline()).await.unwrap();
    let stale = prepare(base.clone(), OwnershipChange::Renew(owner()), 200, 4);
    let gate = Arc::new(Gate::default());
    *s.backend().before.lock().unwrap() = Some(gate.clone());
    let task = {
        let s = s.clone();
        spawn(async move { stale.submit(&s, deadline()).await })
    };
    gate.entered().await;
    let takeover = prepare(
        base,
        OwnershipChange::Acquire { process_id: id(11) },
        root.controller.lease_until_ms.unwrap(),
        5,
    );
    assert!(matches!(
        takeover.submit(&s, deadline()).await.unwrap(),
        OwnershipOutcome::Acknowledged(_)
    ));
    gate.release.add_permits(1);
    let OwnershipOutcome::Conflict {
        current: Some(current),
    } = finish(task).await.unwrap()
    else {
        panic!("stale Match won")
    };
    assert_eq!(current.root().controller.owner_id, Some(id(11)));
    assert_eq!(counts(&s), (2, 2));
}
#[tokio::test]
async fn lost_acquire_renew_release_replies_confirm_only_exact_current() {
    for (root, change, now) in [
        (initial(), acquire(), 100),
        (held(), OwnershipChange::Renew(owner()), 200),
        (held(), OwnershipChange::Release(owner()), 200),
    ] {
        for later in [false, true] {
            let s = Arc::new(store(&root));
            let base = s.load_control(deadline()).await.unwrap();
            let p = prepare(base, change, now, 4);
            let candidate = ownership_candidate(&root, change, now, id(4)).unwrap();
            s.backend().fake.state.lock().unwrap().lose_put_response = true;
            let gate = Arc::new(Gate::default());
            *s.backend().after.lock().unwrap() = Some(gate.clone());
            let task = {
                let s = s.clone();
                spawn(async move { p.submit(&s, deadline()).await })
            };
            gate.entered().await;
            assert_eq!(counts(&s), (1, 1));
            if later {
                let mut next = candidate.clone();
                next.revision += 1;
                next.change_id = id(5);
                next.scheduler.failures += 1;
                s.backend()
                    .fake
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .insert(KEY.into(), (next.encode().unwrap(), "later".into()));
            }
            gate.release.add_permits(1);
            let result = finish(task).await.unwrap();
            if later {
                assert!(matches!(
                    result,
                    OwnershipOutcome::Unknown { current: Some(_) }
                ));
            } else {
                let OwnershipOutcome::ObservedExact(current) = result else {
                    panic!("not exact")
                };
                assert_eq!(current.root(), &candidate);
            }
            assert_eq!(counts(&s), (1, 2));
        }
    }
}
#[tokio::test]
async fn recovery_is_a_separate_explicit_owner_checked_renewal() {
    let s = store(&initial());
    let base = s.load_control(deadline()).await.unwrap();
    s.backend().fake.state.lock().unwrap().lose_put_response = true;
    let OwnershipOutcome::ObservedExact(current) = prepare(base, acquire(), 100, 4)
        .submit(&s, deadline())
        .await
        .unwrap()
    else {
        panic!("expected exact")
    };
    assert_eq!(counts(&s), (1, 2));
    s.backend().fake.state.lock().unwrap().lose_put_response = false;
    assert!(matches!(
        prepare(current, OwnershipChange::Renew(owner()), 200, 5)
            .submit(&s, deadline())
            .await
            .unwrap(),
        OwnershipOutcome::Acknowledged(_)
    ));
    assert_eq!(counts(&s), (2, 2));
}
#[tokio::test]
async fn reconcile_failures_and_integrity_keep_application_uncertain() {
    for mode in 0..6 {
        let s = store(&initial());
        let base = s.load_control(deadline()).await.unwrap();
        let p = prepare(base, acquire(), 100, 4);
        {
            let mut f = s.backend().fake.state.lock().unwrap();
            f.lose_put_response = true;
            if mode == 0 {
                f.read_failure = true;
            }
        }
        let gate = Arc::new(ReadGate::default());
        s.backend().fake.state.lock().unwrap().read_gate = Some(gate.clone());
        let s = Arc::new(s);
        let task = {
            let s = s.clone();
            spawn(async move { p.submit(&s, deadline()).await })
        };
        time::timeout(Duration::from_secs(5), gate.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        {
            let mut f = s.backend().fake.state.lock().unwrap();
            match mode {
                1 => {
                    f.objects.remove(KEY);
                }
                2 => {
                    f.objects.insert(KEY.into(), (b"{}".to_vec(), "bad".into()));
                }
                3 => {
                    let mut root = ownership_candidate(&initial(), acquire(), 100, id(4)).unwrap();
                    root.scheduler.failures = 1;
                    f.objects
                        .insert(KEY.into(), (root.encode().unwrap(), "integrity".into()));
                }
                4 => {
                    let (_, token) = f.objects.get_mut(KEY).unwrap();
                    token.clear();
                }
                5 => {
                    let (bytes, _) = f.objects.get_mut(KEY).unwrap();
                    *bytes = br#"{"change_id":"00000000-0000-0000-0000-000000000004"}"#.to_vec();
                }
                _ => {}
            }
        }
        gate.release.add_permits(1);
        let result = finish(task).await.unwrap();
        assert!(matches!(result, OwnershipOutcome::Unknown { .. }));
        if mode == 3 {
            assert!(matches!(
                result,
                OwnershipOutcome::Unknown { current: Some(_) }
            ));
        }
        assert_eq!(counts(&s), (1, 2));
    }
}
#[tokio::test]
async fn definite_conflict_and_definite_failure_are_not_ambiguous() {
    for exact in [false, true] {
        let s = store(&initial());
        let base = s.load_control(deadline()).await.unwrap();
        let winner = if exact {
            ownership_candidate(&initial(), acquire(), 100, id(4)).unwrap()
        } else {
            held()
        };
        s.backend().fake.state.lock().unwrap().race = Some(winner.encode().unwrap());
        let OwnershipOutcome::Conflict {
            current: Some(current),
        } = prepare(base, acquire(), 100, 4)
            .submit(&s, deadline())
            .await
            .unwrap()
        else {
            panic!("definite rejection must stay Conflict")
        };
        assert_eq!(current.root(), &winner);
        assert_eq!(counts(&s), (1, 2));
    }
    for error in [StorageError::Conflict, StorageError::Unavailable] {
        let s = store(&initial());
        let base = s.load_control(deadline()).await.unwrap();
        {
            let mut f = s.backend().fake.state.lock().unwrap();
            f.put_faults.insert(KEY.into(), (error, false));
            f.read_failure = true;
        }
        let result = prepare(base, acquire(), 100, 4)
            .submit(&s, deadline())
            .await;
        if error == StorageError::Conflict {
            assert!(matches!(
                result.unwrap(),
                OwnershipOutcome::Conflict { current: None }
            ));
            assert_eq!(counts(&s), (1, 2));
        } else {
            assert_eq!(result.unwrap_err(), error);
            assert_eq!(counts(&s), (1, 1));
        }
    }
}
#[tokio::test]
async fn empty_ack_reconciles_once() {
    let s = store(&initial());
    *s.backend().empty_ack.lock().unwrap() = true;
    let base = s.load_control(deadline()).await.unwrap();
    assert!(matches!(
        prepare(base, acquire(), 100, 4)
            .submit(&s, deadline())
            .await
            .unwrap(),
        OwnershipOutcome::ObservedExact(_)
    ));
    assert_eq!(counts(&s), (1, 2));
}
#[tokio::test]
async fn deadlines_cover_original_write_and_reconcile_without_reset() {
    for after_apply in [false, true] {
        let s = Arc::new(store(&initial()));
        let base = s.load_control(deadline()).await.unwrap();
        let gate = Arc::new(Gate::default());
        if after_apply {
            *s.backend().after.lock().unwrap() = Some(gate.clone());
        } else {
            *s.backend().before.lock().unwrap() = Some(gate.clone());
        }
        let end = time::now() + Duration::from_millis(150);
        let task = {
            let s = s.clone();
            spawn(async move { prepare(base, acquire(), 100, 4).submit(&s, end).await })
        };
        gate.entered().await;
        assert_eq!(counts(&s).0, usize::from(after_apply));
        assert!(matches!(
            finish(task).await.unwrap(),
            OwnershipOutcome::Unknown { current: None }
        ));
        assert_eq!(counts(&s), (usize::from(after_apply), 1));
    }
    let s = store(&initial());
    let base = s.load_control(deadline()).await.unwrap();
    assert_eq!(
        prepare(base, acquire(), 100, 4)
            .submit(&s, time::now())
            .await
            .unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(counts(&s), (0, 1));
    // A reconciliation that is polled must still use the original deadline.
    let s = Arc::new(store(&initial()));
    let base = s.load_control(deadline()).await.unwrap();
    let gate = Arc::new(ReadGate::default());
    {
        let mut f = s.backend().fake.state.lock().unwrap();
        f.lose_put_response = true;
        f.read_gate = Some(gate.clone());
    }
    let end = time::now() + Duration::from_millis(150);
    let task = {
        let s = s.clone();
        spawn(async move { prepare(base, acquire(), 100, 4).submit(&s, end).await })
    };
    time::timeout(Duration::from_secs(5), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(matches!(
        finish(task).await.unwrap(),
        OwnershipOutcome::Unknown { current: None }
    ));
    assert!(time::now() >= end);
    assert_eq!(counts(&s), (1, 1)); // Fake counts GET only after its gate.
}
#[tokio::test]
async fn cancellation_after_observed_application_never_resubmits() {
    let s = Arc::new(store(&initial()));
    let base = s.load_control(deadline()).await.unwrap();
    let gate = Arc::new(Gate::default());
    *s.backend().after.lock().unwrap() = Some(gate.clone());
    let mut task = {
        let s = s.clone();
        spawn(async move {
            prepare(base, acquire(), 100, 4)
                .submit(&s, deadline())
                .await
        })
    };
    gate.entered().await;
    assert_eq!(counts(&s), (1, 1));
    task.0.abort();
    assert!((&mut task.0).await.unwrap_err().is_cancelled());
    assert_eq!(
        s.load_control(deadline()).await.unwrap().root().change_id,
        id(4)
    );
    assert_eq!(counts(&s), (1, 2));
}

// Combined guard/CAS cases use the existing before/after Match wrapper gates.
use crate::cluster::controller::{
    OwnershipEvidence, OwnershipGuard, OwnershipObservation, OwnershipProposal, OwnershipSample,
};
use std::time::UNIX_EPOCH;
fn guard_sample(t: Instant, elapsed: u64, wall: u64) -> Result<OwnershipSample, StorageError> {
    OwnershipSample::from_system_time(
        t + Duration::from_millis(elapsed),
        UNIX_EPOCH + Duration::from_millis(wall),
    )
}
#[tokio::test]
async fn guard_late_ack_error_cancel_cannot_disturb_fresh_recovery() {
    for late in 0..3 {
        let s = Arc::new(store(&initial()));
        let t = time::now();
        let base = s.load_control(deadline()).await.unwrap();
        let p =
            OwnershipProposal::new(base.root(), acquire(), id(4), guard_sample(t, 0, 100)).unwrap();
        let mut guard = OwnershipGuard::new(id(10));
        let old = guard.begin(&p).unwrap();
        let gate = Arc::new(Gate::default());
        *s.backend().after.lock().unwrap() = Some(gate.clone());
        let mut task = {
            let s = s.clone();
            spawn(async move { p.prepare(base).unwrap().submit(&s, deadline()).await })
        };
        gate.entered().await; // Applied, acknowledgment still withheld.
        guard.invalidate().unwrap();
        let fresh = s.load_control(deadline()).await.unwrap();
        let change = OwnershipChange::Renew(owner());
        let p =
            OwnershipProposal::new(fresh.root(), change, id(5), guard_sample(t, 1, 101)).unwrap();
        let current = guard.begin(&p).unwrap();
        let result = p
            .prepare(fresh)
            .unwrap()
            .submit(&s, deadline())
            .await
            .and_then(OwnershipEvidence::new);
        // Even a bad obsolete time/error cannot release the current slot.
        assert!(
            !guard
                .finish(
                    old,
                    Err(StorageError::Unavailable),
                    Err(StorageError::LimitExceeded)
                )
                .unwrap()
        );
        assert_eq!(guard.begin(&p), Err(StorageError::Conflict));
        assert!(
            guard
                .finish(current, result, guard_sample(t, 2, 102))
                .unwrap()
        );
        let result = if late == 2 {
            task.0.abort();
            assert!((&mut task.0).await.unwrap_err().is_cancelled());
            Err(StorageError::Unavailable)
        } else {
            gate.release.add_permits(1);
            let result = finish(task).await.and_then(OwnershipEvidence::new);
            if late == 1 {
                Err(StorageError::Unavailable)
            } else {
                result
            }
        };
        assert!(
            !guard
                .finish(old, result, Err(StorageError::Unavailable))
                .unwrap()
        );
        assert_eq!(guard.check(guard_sample(t, 3, 103)).unwrap(), owner());
    }
}
#[tokio::test]
async fn guard_lost_acquire_renew_release_ack_requires_distinct_fresh_renewal() {
    for change_kind in 0..3 {
        let s = store(&initial());
        let t = time::now();
        let mut guard = OwnershipGuard::new(id(10));
        if change_kind != 0 {
            let p = OwnershipProposal::new(&initial(), acquire(), id(3), guard_sample(t, 0, 100))
                .unwrap();
            let stamp = guard.begin(&p).unwrap();
            let base = s.load_control(deadline()).await.unwrap();
            let acknowledged = p
                .prepare(base)
                .unwrap()
                .submit(&s, deadline())
                .await
                .unwrap();
            assert!(
                guard
                    .finish(
                        stamp,
                        OwnershipEvidence::new(acknowledged),
                        guard_sample(t, 0, 100)
                    )
                    .unwrap()
            );
            assert_eq!(guard.check(guard_sample(t, 0, 100)).unwrap(), owner());
        }
        let change = match change_kind {
            0 => acquire(),
            1 => OwnershipChange::Renew(owner()),
            _ => OwnershipChange::Release(owner()),
        };
        let base = s.load_control(deadline()).await.unwrap();
        let p =
            OwnershipProposal::new(base.root(), change, id(4), guard_sample(t, 0, 101)).unwrap();
        let stamp = guard.begin(&p).unwrap();
        s.backend().fake.state.lock().unwrap().lose_put_response = true;
        let result = p
            .prepare(base)
            .unwrap()
            .submit(&s, deadline())
            .await
            .unwrap();
        assert!(matches!(result, OwnershipOutcome::ObservedExact(_)));
        assert!(
            !guard
                .finish(
                    stamp,
                    OwnershipEvidence::new(result),
                    guard_sample(t, 1, 102)
                )
                .unwrap()
        );
        assert_eq!(
            guard.check(guard_sample(t, 1, 102)),
            Err(StorageError::Unavailable)
        );
        let fresh = s.load_control(deadline()).await.unwrap();
        guard
            .observe(&OwnershipObservation::new(fresh.root()).unwrap())
            .unwrap();
        assert!(guard.check(guard_sample(t, 1, 102)).is_err());
        if change_kind != 2 {
            s.backend().fake.state.lock().unwrap().lose_put_response = false;
            let change = OwnershipChange::Renew(owner());
            let p = OwnershipProposal::new(fresh.root(), change, id(5), guard_sample(t, 2, 103))
                .unwrap();
            let stamp = guard.begin(&p).unwrap();
            let result = p
                .prepare(fresh)
                .unwrap()
                .submit(&s, deadline())
                .await
                .and_then(OwnershipEvidence::new);
            assert!(
                guard
                    .finish(stamp, result, guard_sample(t, 3, 104))
                    .unwrap()
            );
            assert_eq!(guard.check(guard_sample(t, 4, 105)).unwrap(), owner());
        }
    }
    // The paired real sampling boundary is usable without a clock service.
    assert!(OwnershipSample::from_system_time(time::now(), time::wall_now()).is_ok());
}
#[tokio::test]
async fn guard_gated_ack_after_observed_takeover_cannot_restore_owner() {
    let s = Arc::new(store(&initial()));
    let t = time::now();
    let base = s.load_control(deadline()).await.unwrap();
    let p = OwnershipProposal::new(base.root(), acquire(), id(4), guard_sample(t, 0, 100)).unwrap();
    let mut guard = OwnershipGuard::new(id(10));
    let stamp = guard.begin(&p).unwrap();
    let gate = Arc::new(Gate::default());
    *s.backend().after.lock().unwrap() = Some(gate.clone());
    let task = {
        let s = s.clone();
        spawn(async move { p.prepare(base).unwrap().submit(&s, deadline()).await })
    };
    gate.entered().await;
    let base = s.load_control(deadline()).await.unwrap();
    let expiry = base.root().controller.lease_until_ms.unwrap();
    let takeover = prepare(
        base,
        OwnershipChange::Acquire { process_id: id(11) },
        expiry,
        5,
    )
    .submit(&s, deadline())
    .await
    .unwrap();
    let OwnershipOutcome::Acknowledged(current) = takeover else {
        panic!("takeover must win")
    };
    guard
        .observe(&OwnershipObservation::new(current.root()).unwrap())
        .unwrap();
    gate.release.add_permits(1);
    assert!(
        !guard
            .finish(
                stamp,
                finish(task).await.and_then(OwnershipEvidence::new),
                guard_sample(t, 1, 101)
            )
            .unwrap()
    );
    assert_eq!(
        guard.check(guard_sample(t, 2, 102)),
        Err(StorageError::Unavailable)
    );
}

#[tokio::test]
async fn guard_actual_preparation_failure_completes_only_its_stamp() {
    let s = store(&initial());
    let t = time::now();
    let base = s.load_control(deadline()).await.unwrap();
    let p = OwnershipProposal::new(base.root(), acquire(), id(4), guard_sample(t, 0, 100)).unwrap();
    let mut guard = OwnershipGuard::new(id(10));
    let stamp = guard.begin(&p).unwrap();
    let mut different = base.clone();
    different.root.scheduler.failures = 1;
    different.root.validate().unwrap();
    let failure = p.prepare(different).unwrap_err();
    assert_eq!(failure, StorageError::Conflict);
    assert_eq!(
        guard.finish(stamp, Err(failure), guard_sample(t, 1, 101)),
        Err(StorageError::Conflict)
    );
    let stamp = guard.begin(&p).unwrap();
    let result = p
        .prepare(base)
        .unwrap()
        .submit(&s, deadline())
        .await
        .and_then(OwnershipEvidence::new);
    assert!(
        guard
            .finish(stamp, result, guard_sample(t, 2, 102))
            .unwrap()
    );
    assert_eq!(counts(&s), (1, 1));
}
