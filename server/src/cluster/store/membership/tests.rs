use super::*;
use crate::{
    cluster::{
        controller::{Owner, OwnershipObservation},
        member::{Drain, NodeState},
        membership::{ReadinessInput, RegistrationPlan, ready_candidate, registration_plan},
    },
    node::auth::{EnrollmentProof, verify_enrollment},
    storage::{Download, Namespace},
    test_storage::{Fake, ReadGate},
};
use nabedra_rt::{task, time};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
fn deadline() -> Instant {
    time::now() + Duration::from_secs(5)
}
fn root() -> Control {
    let mut v = super::super::tests::root();
    v["provisioning"] = json!("external");
    v["config"]["adapter_contract"] = Value::Null;
    v["controller"] = json!({"owner_id":id(90),"epoch":7,"lease_state":"held","lease_until_ms":1});
    Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn proof(session: u128) -> EnrollmentProof {
    proof_for(session, 4)
}
fn proof_for(session: u128, incarnation: u128) -> EnrollmentProof {
    let key = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
    // Independent canonical enrollment text, signed with a synthetic key.
    let unsigned = format!(
        "{{\"version\":1,\"cluster_id\":\"{}\",\"node_id\":\"{}\",\"request_id\":null,\"incarnation\":\"{}\",\"session_id\":\"{}\",\"public_key\":\"{}\",\"endpoint\":\"http://worker:8000\",\"operation_slots\":4,\"partition_slots\":4,\"queue_size\":64}}",
        id(1),
        id(3),
        id(incarnation),
        id(session),
        hex(key.public_key().as_ref())
    );
    let mut message = b"nabedra.enroll.v1\0".to_vec();
    message.extend_from_slice(unsigned.as_bytes());
    let signed = format!(
        "{},\"proof\":\"{}\"}}",
        &unsigned[..unsigned.len() - 1],
        hex(key.sign(&message).as_ref())
    );
    verify_enrollment(signed.as_bytes()).unwrap()
}
fn candidate(root: &Control, proposal: u128) -> MembershipCandidate {
    let RegistrationPlan::Change(c) =
        registration_plan(root, owner(), &proof(5), id(proposal)).unwrap()
    else {
        panic!("expected change")
    };
    c
}
#[derive(Default)]
struct Backend {
    fake: Arc<Fake>,
    conditions: Mutex<Vec<String>>,
    pending_gate: Mutex<Option<Arc<ReadGate>>>,
    pending_task: Mutex<Option<tokio::task::JoinHandle<Result<String, StorageError>>>>,
    before: Mutex<Option<Arc<ReadGate>>>,
    after: Mutex<Option<Arc<ReadGate>>>,
}
async fn hold(g: Arc<ReadGate>) {
    g.entered.add_permits(1);
    g.release.acquire().await.unwrap().forget();
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
            panic!("membership must Match")
        };
        self.conditions.lock().unwrap().push(token.to_owned());
        let gate = self.before.lock().unwrap().take();
        if let Some(g) = gate {
            hold(g).await;
        }
        let pending = self.pending_gate.lock().unwrap().take();
        if let Some(g) = pending {
            let fake = self.fake.clone();
            let key = key.to_owned();
            let token = token.to_owned();
            *self.pending_task.lock().unwrap() = Some(task::spawn(async move {
                hold(g).await;
                fake.put(&key, bytes, WriteCondition::Match(&token)).await
            }));
            return Err(StorageError::Ambiguous);
        }
        let result = self.fake.put(key, bytes, condition).await;
        let gate = self.after.lock().unwrap().take();
        if let Some(g) = gate {
            hold(g).await;
        }
        result
    }
}
fn store(c: &Control) -> Storage<Backend> {
    let b = Backend::default();
    b.fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (c.encode().unwrap(), "captured".into()));
    Storage::new(b, Namespace::new("test").unwrap())
}
fn counts(s: &Storage<Backend>) -> (usize, usize) {
    let f = s.backend().fake.state.lock().unwrap();
    (f.puts, f.gets)
}
async fn entered(g: &ReadGate) -> bool {
    match time::timeout(Duration::from_secs(5), g.entered.acquire()).await {
        Ok(Ok(p)) => {
            p.forget();
            true
        }
        _ => false,
    }
}
async fn finish<T>(mut t: tokio::task::JoinHandle<T>) -> T {
    match time::timeout(Duration::from_secs(5), &mut t).await {
        Ok(r) => r.unwrap(),
        Err(_) => {
            t.abort();
            let _ = t.await;
            panic!("watchdog");
        }
    }
}
#[tokio::test]
async fn competing_registrations_and_exact_binding_recovery() {
    let c = root();
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    let a = PreparedMembership::prepare(base.clone(), candidate(&c, 10)).unwrap();
    let b = PreparedMembership::prepare(base, candidate(&c, 11)).unwrap();
    assert_eq!(format!("{a:?}"), "PreparedMembership");
    let (a, b) = tokio::join!(a.submit(&s, deadline()), b.submit(&s, deadline()));
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, OwnershipOutcome::Acknowledged(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, OwnershipOutcome::Conflict { current: Some(_) }))
            .count(),
        1
    );
    assert_eq!(counts(&s), (2, 2));
    let loaded = s.load_control(deadline()).await.unwrap();
    let RegistrationPlan::Existing(n) =
        registration_plan(loaded.root(), owner(), &proof(5), id(12)).unwrap()
    else {
        panic!("retry")
    };
    assert_eq!(n, loaded.root().nodes[0]);
    assert!(matches!(
        registration_plan(loaded.root(), owner(), &proof(6), id(12)),
        Err(StorageError::Conflict)
    ));
}
#[tokio::test]
async fn preparation_rejects_complete_base_and_owner_mismatches_before_io() {
    let c = root();
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    for mode in 0..4 {
        let mut different = base.clone();
        match mode {
            0 => different.root.scheduler.failures += 1,
            1 => different.root.controller.epoch += 1,
            2 => different.root.controller.owner_id = Some(id(91)),
            _ => {
                different.root.controller.lease_state = LeaseState::Released;
                different.root.controller.owner_id = None;
                different.root.controller.lease_until_ms = None;
            }
        }
        different.root.validate().unwrap();
        assert!(matches!(
            PreparedMembership::prepare(different, candidate(&c, 10)),
            Err(StorageError::Conflict)
        ));
    }
    assert_eq!(counts(&s), (0, 1));
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <PreparedMembership as AmbiguousIfClone<_>>::marker;
}
#[tokio::test]
async fn committed_reply_loss_exact_or_later_root_and_binding_retry() {
    for later in [false, true] {
        let c = root();
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let p = PreparedMembership::prepare(base, candidate(&c, 10)).unwrap();
        let expected = p.candidate.clone();
        let mut changed = expected.clone();
        changed.revision += 1;
        changed.change_id = id(11);
        changed.scheduler.failures += 1;
        let later_bytes = changed.encode().unwrap();
        s.backend().fake.state.lock().unwrap().lose_put_response = true;
        let g = Arc::new(ReadGate::default());
        *s.backend().after.lock().unwrap() = Some(g.clone());
        let t = {
            let s = s.clone();
            task::spawn(async move { p.submit(&s, deadline()).await })
        };
        let reached = entered(&g).await;
        if reached && later {
            s.backend()
                .fake
                .state
                .lock()
                .unwrap()
                .objects
                .insert(KEY.into(), (later_bytes, "later".into()));
        }
        g.release.add_permits(1);
        let result = finish(t).await;
        assert!(reached);
        let result = result.unwrap();
        if later {
            assert!(matches!(
                result,
                OwnershipOutcome::Unknown { current: Some(_) }
            ));
        } else {
            let OwnershipOutcome::ObservedExact(current) = result else {
                panic!("not exact")
            };
            assert_eq!(current.root(), &expected);
        }
        assert_eq!(counts(&s), (1, 2));
        let loaded = s.load_control(deadline()).await.unwrap();
        let RegistrationPlan::Existing(n) =
            registration_plan(loaded.root(), owner(), &proof(5), id(12)).unwrap()
        else {
            panic!("binding lost")
        };
        assert_eq!(n, expected.nodes[0]);
    }
}
fn registered() -> Control {
    candidate(&root(), 10).into_parts().2
}
fn ready(c: &Control) -> MembershipCandidate {
    let evidence = ReadinessInput {
        cluster_id: c.cluster_id,
        member: c.nodes[0].clone(),
        observation: OwnershipObservation::new(c).unwrap(),
        challenge: id(99),
        verified_challenge: id(99),
        initialized: true,
    };
    ready_candidate(c, owner(), &evidence, id(12)).unwrap()
}
#[tokio::test]
async fn gated_registration_and_readiness_lose_to_takeover_drain_or_replacement() {
    for mode in 0..4 {
        let c = if mode < 2 { root() } else { registered() };
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let proposed = if mode < 2 {
            candidate(&c, 10)
        } else {
            ready(&c)
        };
        let p = PreparedMembership::prepare(base.clone(), proposed).unwrap();
        let mut winner = c.clone();
        winner.revision += 1;
        winner.change_id = id(50);
        if mode == 0 {
            winner = ownership_candidate(
                &c,
                OwnershipChange::Acquire { process_id: id(91) },
                1,
                id(50),
            )
            .unwrap();
        } else if mode == 3 {
            winner.nodes[0].state = NodeState::Lost;
            winner.validate().unwrap();
            let RegistrationPlan::Change(replacement) =
                registration_plan(&winner, owner(), &proof_for(9, 8), id(51)).unwrap()
            else {
                panic!("replacement")
            };
            winner = replacement.into_parts().2;
        } else {
            if mode == 1 {
                winner = candidate(&c, 50).into_parts().2;
            }
            winner.nodes[0].state = NodeState::Draining;
            winner.nodes[0].drain = Some(Drain {
                id: id(77),
                requested_at_ms: 0,
                ack_seq: None,
                confirmed_idle: false,
            });
        }
        let bytes = winner.encode().unwrap();
        let g = Arc::new(ReadGate::default());
        *s.backend().before.lock().unwrap() = Some(g.clone());
        let t = {
            let s = s.clone();
            task::spawn(async move { p.submit(&s, deadline()).await })
        };
        let reached = entered(&g).await;
        let competing = if reached {
            Some(
                s.put(
                    &ObjectKey::control(),
                    bytes,
                    WriteCondition::Match("captured"),
                )
                .await,
            )
        } else {
            None
        };
        g.release.add_permits(1);
        let result = finish(t).await;
        assert!(reached);
        assert!(competing.unwrap().is_ok());
        let OwnershipOutcome::Conflict {
            current: Some(current),
        } = result.unwrap()
        else {
            panic!("stale won")
        };
        assert_eq!(current.root(), &winner);
        assert_eq!(counts(&s), (2, 2));
    }
}
#[tokio::test]
async fn reconciliation_failures_and_equal_uuid_unequal_contents_remain_unknown() {
    for mode in 0..5 {
        let c = root();
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let p = PreparedMembership::prepare(base, candidate(&c, 10)).unwrap();
        let mut changed = p.candidate.clone();
        changed.scheduler.failures += 1;
        let different_bytes = changed.encode().unwrap();
        s.backend().fake.state.lock().unwrap().lose_put_response = true;
        let g = Arc::new(ReadGate::default());
        s.backend().fake.state.lock().unwrap().read_gate = Some(g.clone());
        let t = {
            let s = s.clone();
            task::spawn(async move { p.submit(&s, deadline()).await })
        };
        let reached = entered(&g).await;
        if reached {
            let mut f = s.backend().fake.state.lock().unwrap();
            match mode {
                0 => f.read_failure = true,
                1 => {
                    f.objects.remove(KEY);
                }
                2 => {
                    f.objects.insert(KEY.into(), (b"{}".to_vec(), "bad".into()));
                }
                3 => {
                    f.objects
                        .insert(KEY.into(), (different_bytes, "different".into()));
                }
                _ => {
                    if let Some((_, token)) = f.objects.get_mut(KEY) {
                        token.clear();
                    }
                }
            }
        }
        g.release.add_permits(1);
        let result = finish(t).await;
        assert!(reached);
        assert!(matches!(result.unwrap(), OwnershipOutcome::Unknown { .. }));
        assert_eq!(counts(&s), (1, 2));
    }
}
#[tokio::test]
async fn delayed_original_remains_possible_until_a_fresh_match_fences_it() {
    for fence in [false, true] {
        let c = root();
        let s = store(&c);
        let base = s.load_control(deadline()).await.unwrap();
        let p = PreparedMembership::prepare(base.clone(), candidate(&c, 10)).unwrap();
        let fresh = PreparedMembership::prepare(base, candidate(&c, 11)).unwrap();
        let g = Arc::new(ReadGate::default());
        *s.backend().pending_gate.lock().unwrap() = Some(g.clone());
        let result = p.submit(&s, deadline()).await;
        let reached = entered(&g).await;
        let before = counts(&s);
        let submitted_conditions = s.backend().conditions.lock().unwrap().clone();
        let recovery = if fence && reached {
            Some(fresh.submit(&s, deadline()).await)
        } else {
            None
        };
        g.release.add_permits(1);
        let provider = s.backend().pending_task.lock().unwrap().take();
        let completed = if let Some(t) = provider {
            Some(finish(t).await)
        } else {
            None
        };
        assert!(reached);
        assert!(matches!(
            result.unwrap(),
            OwnershipOutcome::Unknown { current: Some(_) }
        ));
        assert_eq!(submitted_conditions, vec!["captured"]);
        assert_eq!(before, (0, 2)); // GET saw the old root while the original PUT was still pending.
        if fence {
            assert!(matches!(
                recovery.unwrap().unwrap(),
                OwnershipOutcome::Acknowledged(_)
            ));
            assert_eq!(completed.unwrap(), Err(StorageError::Conflict));
        } else {
            assert!(completed.unwrap().is_ok());
        }
        assert_eq!(counts(&s), (if fence { 2 } else { 1 }, 2));
        let current = s.load_control(deadline()).await.unwrap();
        assert_eq!(current.root().change_id, id(if fence { 11 } else { 10 }));
    }
}
#[tokio::test]
async fn expired_before_submission_is_definite_and_performs_no_write() {
    let c = root();
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    let p = PreparedMembership::prepare(base, candidate(&c, 10)).unwrap();
    assert_eq!(
        p.submit(&s, time::now()).await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(counts(&s), (0, 1));
}

#[tokio::test]
async fn st04_r03_readiness_and_abort_match_the_same_captured_root() {
    for readiness_wins in [false, true] {
        let root = registered();
        let storage = store(&root);
        let base = storage.load_control(deadline()).await.unwrap();
        let readiness = PreparedMembership::prepare(base.clone(), ready(&root)).unwrap();
        let abort = PreparedMembership::prepare(
            base.clone(),
            crate::cluster::membership::fence_readiness(&root, owner(), id(30)).unwrap(),
        )
        .unwrap();
        let gate = Arc::new(ReadGate::default());
        *storage.backend().pending_gate.lock().unwrap() = Some(gate.clone());
        let unknown = readiness.submit(&storage, deadline()).await.unwrap();
        assert!(matches!(
            unknown,
            OwnershipOutcome::Unknown { current: Some(current) } if current == base
        ));
        assert!(entered(&gate).await);

        let provider = storage
            .backend()
            .pending_task
            .lock()
            .unwrap()
            .take()
            .unwrap();
        let (applied, renewal) = if readiness_wins {
            gate.release.add_permits(1);
            let applied = finish(provider).await;
            (applied, abort.submit(&storage, deadline()).await.unwrap())
        } else {
            let renewal = abort.submit(&storage, deadline()).await.unwrap();
            gate.release.add_permits(1);
            (finish(provider).await, renewal)
        };

        let observed = storage.load_control(deadline()).await.unwrap();
        assert_eq!(observed.root().revision, root.revision + 1);
        if readiness_wins {
            assert!(applied.is_ok());
            assert!(matches!(renewal, OwnershipOutcome::Conflict { .. }));
            assert_eq!(observed.root().nodes[0].state, NodeState::Ready);
        } else {
            assert_eq!(applied, Err(StorageError::Conflict));
            assert!(matches!(renewal, OwnershipOutcome::Acknowledged(_)));
            assert_eq!(observed.root().nodes[0].state, NodeState::Starting);
        }
    }
}
