use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    controller::Owner,
    drain::{DrainPlan, begin_drain},
    member::*,
    membership::{RegistrationPlan, registration_plan},
    state::*,
};
use crate::node::auth::{EnrollmentProof, verify_enrollment};
use crate::{
    storage::{Download, Namespace},
    test_storage::{Fake, ReadGate},
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn claims(request: Option<Uuid>) -> Value {
    let key = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
    json!({"version":1,"cluster_id":id(1),"node_id":id(3),"request_id":request,"incarnation":id(4),"session_id":id(5),"public_key":hex(key.public_key().as_ref()),"endpoint":"http://worker:8000","operation_slots":4,"partition_slots":4,"queue_size":64})
}
// Independently assemble the signed canonical text. No production signing helper.
fn proof(v: &Value) -> EnrollmentProof {
    let key_seed = if v["public_key"]
        == hex(Ed25519KeyPair::from_seed_unchecked(&[43; 32])
            .unwrap()
            .public_key()
            .as_ref())
    {
        43
    } else {
        42
    };
    let key = Ed25519KeyPair::from_seed_unchecked(&[key_seed; 32]).unwrap();
    let names = [
        "version",
        "cluster_id",
        "node_id",
        "request_id",
        "incarnation",
        "session_id",
        "public_key",
        "endpoint",
        "operation_slots",
        "partition_slots",
        "queue_size",
    ];
    let unsigned = format!(
        "{{{}}}",
        names
            .iter()
            .map(|n| format!("\"{n}\":{}", v[n]))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut message = b"meshdb.enroll.v1\0".to_vec();
    message.extend_from_slice(unsigned.as_bytes());
    let signed = format!(
        "{},\"proof\":\"{}\"}}",
        &unsigned[..unsigned.len() - 1],
        hex(key.sign(&message).as_ref())
    );
    verify_enrollment(signed.as_bytes()).unwrap()
}
fn root(external: bool) -> Control {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/synthetic-token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let hash = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    let mut v = json!({"version":1,"cluster_id":id(1),"name":"Fleet","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":hash,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":"meshdb.callbacks.v1"},"provisioning":"managed","template":template,"timing":Timing::default(),"revision":33,"change_id":id(2),"policy":{"revision":9,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":id(90),"epoch":7,"lease_state":"held","lease_until_ms":1},"scheduler":{"cooldown_until_ms":81,"backoff_until_ms":82,"failures":3,"low_since_ms":80},"admin_seq":8,"admin_head":id(88),"nodes":[],"intents":[]});
    if external {
        v["provisioning"] = json!("external");
        v["config"]["adapter_contract"] = Value::Null;
    }
    Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap()
}
fn managed() -> Control {
    let mut c = root(false);
    c.nodes.push(Node {
        resources: None,
        node_id: id(3),
        launch_id: Some(id(6)),
        resource_id: None,
        ownership: Ownership::Managed,
        state: NodeState::Starting,
        incarnation: None,
        retired_incarnations: vec![],
        endpoint: None,
        operation_slots: 4,
        partition_slots: 4,
        queue_size: 64,
        registration: Registration::Unregistered,
        session: None,
        drain: None,
    });
    c.intents.push(Intent {
        request_id: id(6),
        node_id: id(3),
        launch_id: id(6),
        incarnation_id: None,
        resource_id: None,
        action: Action::Start,
        state: IntentState::Reconciling,
        attempts: 1,
        created_at_ms: 0,
        deadline_ms: 1,
        retry_at_ms: 0,
        error_code: None,
        template_fingerprint: Some(c.config.template_fingerprint.clone()),
        delivery: Some(Delivery {
            invocation_id: id(7),
            owner_id: id(90),
            epoch: 7,
            command_sha256: "a".repeat(64),
        }),
        outcome: None,
    });
    c.validate().unwrap();
    c
}

fn registered() -> Control {
    let c = managed();
    let RegistrationPlan::Change(candidate) =
        registration_plan(&c, owner(), &proof(&claims(Some(id(6)))), id(10)).unwrap()
    else {
        panic!("change")
    };
    candidate.into_parts().2
}

const KEY: &str = "test/cluster/control.json";
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn candidate(c: &Control, drain: u128, proposal: u128) -> DrainCandidate {
    let DrainPlan::Change(v) =
        begin_drain(c, owner(), id(3), id(drain), 123, id(proposal)).unwrap()
    else {
        panic!("change")
    };
    v
}
#[derive(Default)]
struct Backend {
    fake: Fake,
    before: Mutex<Option<Arc<ReadGate>>>,
    after: Mutex<Option<Arc<ReadGate>>>,
    conditions: Mutex<Vec<String>>,
}
async fn hold(g: Arc<ReadGate>) -> Result<(), StorageError> {
    g.entered.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), g.release.acquire())
        .await
        .map_err(|_| StorageError::Unavailable)?
        .map_err(|_| StorageError::Unavailable)?
        .forget();
    Ok(())
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
            panic!("Match required")
        };
        self.conditions.lock().unwrap().push(token.to_owned());
        let before = self.before.lock().unwrap().take();
        if let Some(g) = before {
            hold(g).await?;
        }
        let result = self.fake.put(key, bytes, condition).await;
        let after = self.after.lock().unwrap().take();
        if let Some(g) = after {
            hold(g).await?;
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
    match tokio::time::timeout(Duration::from_secs(5), g.entered.acquire()).await {
        Ok(Ok(p)) => {
            p.forget();
            true
        }
        _ => false,
    }
}
async fn settle<T>(mut task: tokio::task::JoinHandle<T>) -> Result<T, &'static str> {
    match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
        Ok(v) => v.map_err(|_| "join failed"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("watchdog")
        }
    }
}
#[tokio::test]
async fn consumed_prepare_checks_complete_observation_and_owner() {
    let c = registered();
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    let p = PreparedDrain::prepare(base.clone(), candidate(&c, 77, 12)).unwrap();
    assert_eq!(format!("{p:?}"), "PreparedDrain");
    for mode in 0..4 {
        let mut wrong = base.clone();
        match mode {
            0 => wrong.root.scheduler.failures += 1,
            1 => wrong.root.controller.epoch += 1,
            2 => wrong.root.controller.owner_id = Some(id(91)),
            _ => {
                wrong.root.controller.owner_id = None;
                wrong.root.controller.lease_state = LeaseState::Released;
                wrong.root.controller.lease_until_ms = None;
            }
        }
        wrong.root.validate().unwrap();
        assert!(matches!(
            PreparedDrain::prepare(wrong, candidate(&c, 77, 12)),
            Err(StorageError::Conflict)
        ));
    }
    assert_eq!(
        p.submit(&s, Instant::now()).await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(counts(&s), (0, 1));
}
#[tokio::test]
async fn gated_competing_drain_and_takeover_reload_winner() {
    for takeover in [false, true] {
        let c = registered();
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let stale = PreparedDrain::prepare(base.clone(), candidate(&c, 77, 12)).unwrap();
        let drain_winner = PreparedDrain::prepare(base.clone(), candidate(&c, 78, 13)).unwrap();
        let takeover_winner = PreparedOwnership::prepare(
            base,
            OwnershipChange::Acquire { process_id: id(91) },
            1,
            id(14),
        )
        .unwrap();
        let g = Arc::new(ReadGate::default());
        *s.backend().before.lock().unwrap() = Some(g.clone());
        let original_deadline = deadline();
        let task = {
            let s = s.clone();
            tokio::spawn(async move { stale.submit(&s, original_deadline).await })
        };
        let reached = entered(&g).await;
        let winner = if reached {
            Some(if takeover {
                takeover_winner.submit(&s, original_deadline).await
            } else {
                drain_winner.submit(&s, original_deadline).await
            })
        } else {
            None
        };
        g.release.add_permits(1);
        let stale = settle(task).await;
        let loaded = s.load_control(deadline()).await;
        assert!(reached);
        let OwnershipOutcome::Acknowledged(winner) = winner.unwrap().unwrap() else {
            panic!("winner")
        };
        let OwnershipOutcome::Conflict {
            current: Some(current),
        } = stale.unwrap().unwrap()
        else {
            panic!("stale won")
        };
        assert_eq!(current.root(), winner.root());
        assert_eq!(loaded.unwrap().root(), winner.root());
        assert_eq!(counts(&s), (2, 3));
        assert_eq!(
            *s.backend().conditions.lock().unwrap(),
            vec!["captured", "captured"]
        );
        if !takeover {
            assert!(matches!(
                begin_drain(current.root(), owner(), id(3), id(77), 999, id(15)),
                Ok(DrainPlan::Existing(_))
            ));
        }
    }
}
#[tokio::test]
async fn late_verified_enrollment_recomputes_after_drain_wins() {
    let c = managed();
    let s = Arc::new(store(&c));
    let base = s.load_control(deadline()).await.unwrap();
    let evidence = proof(&claims(Some(id(6))));
    let RegistrationPlan::Change(registration) =
        registration_plan(&c, owner(), &evidence, id(10)).unwrap()
    else {
        panic!("change")
    };
    let stale =
        crate::cluster::store::PreparedMembership::prepare(base.clone(), registration).unwrap();
    let drain_winner = PreparedDrain::prepare(base, candidate(&c, 77, 12)).unwrap();
    let g = Arc::new(ReadGate::default());
    *s.backend().before.lock().unwrap() = Some(g.clone());
    let end = deadline();
    let task = {
        let s = s.clone();
        tokio::spawn(async move { stale.submit(&s, end).await })
    };
    let reached = entered(&g).await;
    let winner = if reached {
        Some(drain_winner.submit(&s, end).await)
    } else {
        None
    };
    g.release.add_permits(1);
    let stale = settle(task).await;
    let loaded = s.load_control(deadline()).await;
    assert!(reached);
    assert!(matches!(
        winner.unwrap().unwrap(),
        OwnershipOutcome::Acknowledged(_)
    ));
    assert!(matches!(
        stale.unwrap().unwrap(),
        OwnershipOutcome::Conflict { .. }
    ));
    let loaded = loaded.unwrap();
    let retained_drain = loaded.root.nodes[0].drain.clone();
    let RegistrationPlan::Change(recomputed) =
        registration_plan(loaded.root(), owner(), &evidence, id(13)).unwrap()
    else {
        panic!("change")
    };
    let result = crate::cluster::store::PreparedMembership::prepare(loaded, recomputed)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let OwnershipOutcome::Acknowledged(current) = result else {
        panic!("registration")
    };
    assert_eq!(current.root.nodes[0].state, NodeState::Draining);
    assert_eq!(current.root.nodes[0].drain, retained_drain);
    assert_eq!(current.root.nodes[0].session, registered().nodes[0].session);
    assert_eq!(current.root.intents, c.intents);
    assert_eq!(counts(&s), (3, 3));
}
#[tokio::test]
async fn applied_lost_reply_exact_or_superseded_uses_one_read_and_no_resend() {
    for later in [false, true] {
        let c = registered();
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let p = PreparedDrain::prepare(base, candidate(&c, 77, 12)).unwrap();
        let expected = p.candidate.clone();
        let mut successor = expected.clone();
        successor.revision += 1;
        successor.change_id = id(13);
        successor.scheduler.failures += 1;
        let bytes = successor.encode().unwrap();
        s.backend().fake.state.lock().unwrap().lose_put_response = true;
        let g = Arc::new(ReadGate::default());
        *s.backend().after.lock().unwrap() = Some(g.clone());
        let end = deadline();
        let task = {
            let s = s.clone();
            tokio::spawn(async move { p.submit(&s, end).await })
        };
        let reached = entered(&g).await;
        let superseding = if reached && later {
            let token = {
                let mut f = s.backend().fake.state.lock().unwrap();
                f.lose_put_response = false;
                f.objects.get(KEY).map(|(_, token)| token.clone())
            };
            if let Some(token) = token {
                Some(
                    s.put(&ObjectKey::control(), bytes, WriteCondition::Match(&token))
                        .await,
                )
            } else {
                Some(Err(StorageError::NotFound))
            }
        } else {
            None
        };
        g.release.add_permits(1);
        let result = settle(task).await;
        assert!(reached);
        if let Some(r) = superseding {
            assert!(r.is_ok());
        }
        if later {
            let OwnershipOutcome::Unknown { current: Some(v) } = result.unwrap().unwrap() else {
                panic!("unknown")
            };
            assert_eq!(v.root(), &successor);
        } else {
            let OwnershipOutcome::ObservedExact(v) = result.unwrap().unwrap() else {
                panic!("exact")
            };
            assert_eq!(v.root(), &expected);
        }
        assert_eq!(counts(&s), (if later { 2 } else { 1 }, 2));
        assert_eq!(
            *s.backend().conditions.lock().unwrap(),
            if later {
                vec!["captured", "\"opaque-1\""]
            } else {
                vec!["captured"]
            }
        );
    }
}
