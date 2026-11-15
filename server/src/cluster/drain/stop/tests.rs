use super::*;
use crate::cluster::configuration::{Template, Timing};
use crate::cluster::drain::proof::*;
use crate::cluster::member::{Action, IntentState, Ownership};
use crate::cluster::store::OwnershipOutcome;
use crate::node::auth::*;
use crate::{
    storage::{Namespace, Storage},
    test_storage::Fake,
};
use serde_json::json;
use tokio::time::{Duration, Instant};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn fixture() -> (Control, WorkerSigningKey) {
    let key = WorkerSigningKey::new().unwrap();
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    let value = json!({"version":1,"cluster_id":id(1),"name":"Synthetic","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":hex(&worker_verifier(id(1),id(3),id(4),id(5),&key.public_key()))},"drain":null}],"intents":[]});
    let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();

    (root, key)
}
use crate::cluster::{
    controller::{
        OwnershipChange, OwnershipEvidence, OwnershipGuard, OwnershipProposal, OwnershipSample,
    },
    heartbeat::{HeartbeatBinding, HeartbeatSample, HeartbeatWriter, Observations, load_heartbeat},
};
use std::time::{Duration as StdDuration, UNIX_EPOCH};
fn sample(t: Instant, n: u64) -> OwnershipSample {
    OwnershipSample::from_system_time(
        t + Duration::from_millis(n),
        UNIX_EPOCH + StdDuration::from_millis(100 + n),
    )
    .unwrap()
}
fn store(root: &Control) -> Storage<Fake> {
    let s = Storage::new(Fake::default(), Namespace::new("test").unwrap());
    s.backend().state.lock().unwrap().objects.insert(
        "test/cluster/control.json".into(),
        (root.encode().unwrap(), "base".into()),
    );
    s
}
async fn setup_with(
    edit: impl FnOnce(&mut Control),
) -> (
    Control,
    WorkerSigningKey,
    Storage<Fake>,
    OwnershipGuard,
    Instant,
) {
    let (mut root, key) = fixture();
    root.provisioning = crate::cluster::state::Provisioning::Managed;
    root.config.adapter_contract = Some("meshdb.callbacks.v1".into());
    root.nodes[0].ownership = Ownership::Managed;
    root.nodes[0].launch_id = Some(id(6));
    root.intents.push(crate::cluster::member::Intent {
        request_id: id(6),
        node_id: id(3),
        launch_id: id(6),
        incarnation_id: None,
        resource_id: root.nodes[0].resource_id.clone(),
        action: Action::Start,
        state: IntentState::Completed,
        attempts: 1,
        created_at_ms: 0,
        deadline_ms: 1,
        retry_at_ms: 0,
        error_code: None,
        template_fingerprint: Some(root.config.template_fingerprint.clone()),
        delivery: None,
        outcome: Some(crate::cluster::member::IntentOutcome::ResourceKnown),
    });
    root.nodes[0].state = NodeState::Draining;
    root.nodes[0].drain = Some(crate::cluster::member::Drain {
        id: id(30),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    edit(&mut root);
    let mut root = Control::decode(&root.encode().unwrap()).unwrap();
    let s = store(&root);
    let t = Instant::now();
    let p = OwnershipProposal::new(
        &root,
        OwnershipChange::Acquire { process_id: id(90) },
        id(31),
        Ok(sample(t, 0)),
    )
    .unwrap();
    let mut g = OwnershipGuard::new(id(90));
    let stamp = g.begin(&p).unwrap();
    let result = p
        .prepare(s.load_control(t + Duration::from_secs(5)).await.unwrap())
        .unwrap()
        .submit(&s, t + Duration::from_secs(5))
        .await
        .unwrap();
    let loaded = s.load_control(t + Duration::from_secs(5)).await.unwrap();
    root = loaded.root().clone();
    assert!(
        g.finish(stamp, OwnershipEvidence::new(result), Ok(sample(t, 1)))
            .unwrap()
    );
    (root, key, s, g, t)
}
fn context(_root: &Control, seq: u64) -> (Auth, Vec<u8>) {
    let body = format!(r#"{{"version":1,"drain_id":"{}"}}"#, id(30)).into_bytes();
    let digest = hex(ring::digest::digest(&ring::digest::SHA256, &body).as_ref());
    let a = format!(
        r#"{{"version":1,"cluster_id":"{}","scope":"drain","sender":{{"role":"api","process_id":"{}","node_id":null,"incarnation":null,"session_id":null,"public_key":"{}"}},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":{seq},"method":"POST","path":"/internal/v1/drain","operation_id":null,"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":null,"control_sha256":"{digest}","dependency_id":null}}"#,
        id(1),
        id(2),
        "ab".repeat(32),
        id(3),
        id(4),
        id(5)
    );
    let a = Auth::decode(
        &hex(a.as_bytes()),
        &ActualRoute::new("POST", "/internal/v1/drain", None).unwrap(),
    )
    .unwrap();
    (a, body)
}
fn reply_proofs(
    root: &Control,
    key: &WorkerSigningKey,
    c: &RequestContext,
    idle: bool,
    seq: u64,
) -> (EligibleReply, CheckedReplyContent) {
    let body=format!(r#"{{"version":1,"drain_id":"{}","barrier":true,"idle":{idle},"heartbeat_sequence":{seq}}}"#,id(30)).into_bytes();
    let request = String::from_utf8(c.auth().canonical_bytes().unwrap()).unwrap();
    let receiver = request
        .split("\"sender\":")
        .nth(1)
        .unwrap()
        .split(",\"receiver\":")
        .next()
        .unwrap();
    let digest = hex(ring::digest::digest(&ring::digest::SHA256, &body).as_ref());
    let a = format!(
        r#"{{"version":1,"cluster_id":"{}","request_sequence":{},"scope":"drain","sender":{{"role":"worker","process_id":"{}","node_id":"{}","incarnation":"{}","session_id":"{}","public_key":"{}"}},"receiver":{},"status":200,"control_sha256":"{digest}"}}"#,
        id(1),
        c.auth().sequence(),
        id(4),
        id(3),
        id(4),
        id(5),
        hex(&key.public_key()),
        receiver
    );
    let a = ReplyAuth::decode(&hex(a.as_bytes())).unwrap();
    let signed = verify_reply(
        &hex(&a.canonical_bytes().unwrap()),
        &hex(&key.sign_response(&a).unwrap()),
        200,
        c,
    )
    .unwrap();
    let eligible = check_reply_authority(&signed, root, c).unwrap();
    let content = check_reply_content(&signed, body).unwrap();
    (eligible, content)
}
fn handoff(
    root: &Control,
    key: &WorkerSigningKey,
    round: DrainRound,
    idle: bool,
    seq: u64,
) -> DrainRoundReply {
    let (e, c) = reply_proofs(root, key, round.context().unwrap(), idle, seq);
    round.bind_reply(e, c).unwrap()
}

fn hb_binding(root: &Control) -> HeartbeatBinding {
    let n = &root.nodes[0];
    HeartbeatBinding {
        cluster_id: root.cluster_id,
        node_id: n.node_id,
        incarnation: n.incarnation.unwrap(),
        session_id: n.session.as_ref().unwrap().id,
        verifier: n.session.as_ref().unwrap().verifier.clone(),
        operation_slots: n.operation_slots,
        partition_slots: n.partition_slots,
        queue_size: n.queue_size,
    }
}
async fn receipt(
    root: &Control,
    s: &Storage<Fake>,
    o: &mut Observations,
    t: Instant,
) -> crate::cluster::heartbeat::DrainHeartbeat {
    let deadline = Instant::now() + Duration::from_secs(10);
    let b = hb_binding(root);
    let mut w = HeartbeatWriter::new(b.clone()).unwrap();
    let h = HeartbeatSample {
        running_operations: 0,
        running_partitions: 0,
        queued_operations: 0,
        queued_partitions: 0,
        drain_id: Some(id(30)),
        accepting: false,
    };
    w.publish(s, h.clone(), deadline).await.unwrap();
    o.reconcile_members(root, t).unwrap();
    let loaded = load_heartbeat(s, &b, deadline).await.unwrap().unwrap();
    assert!(
        o.drain_heartbeat(
            root,
            &b,
            &loaded,
            t,
            Duration::from_millis(root.timing.expiry_ms)
        )
        .is_err()
    );
    w.publish(s, h, deadline).await.unwrap();
    let loaded = load_heartbeat(s, &b, deadline).await.unwrap().unwrap();
    o.drain_heartbeat(
        root,
        &b,
        &loaded,
        t + Duration::from_millis(1),
        Duration::from_millis(root.timing.expiry_ms),
    )
    .unwrap()
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 1,
    }
}
fn round(root: &Control, g: &mut OwnershipGuard, t: Instant, seq: u64) -> DrainRound {
    let binding = match crate::cluster::drain::begin_drain(root, owner(), id(3), id(30), 0, id(33))
        .unwrap()
    {
        DrainPlan::Existing(b) => b,
        _ => panic!(),
    };
    let (a, bytes) = context(root, seq);
    DrainRound::new(root, owner(), binding, a, bytes, g, sample(t, 2)).unwrap()
}
async fn positive(
    root: &Control,
    key: &WorkerSigningKey,
    s: &Storage<Fake>,
    g: &mut OwnershipGuard,
    t: Instant,
    o: &mut Observations,
) -> PositiveDrain {
    let r = round(root, g, t, 1);
    let api = r.context().unwrap().auth().sender().clone();
    let h = receipt(root, s, o, t + Duration::from_millis(2)).await;
    match check_drain_reply(
        root,
        owner(),
        handoff(root, key, r, true, 2),
        &api,
        h,
        o,
        g,
        sample(t, 5),
    )
    .unwrap()
    {
        DrainCheck::Positive(p) => p,
        _ => panic!("positive"),
    }
}
async fn confirmed_fixture() -> (
    Control,
    ConfirmedDrain,
    Storage<Fake>,
    OwnershipGuard,
    Observations,
    Instant,
) {
    let (root, key, s, mut g, t) = setup_with(|_| {}).await;
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let IdlePlan::Change(c, pending) =
        confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
    else {
        panic!()
    };
    let expected = c.result.clone();
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        c,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    g.observe(&OwnershipObservation::new(&expected).unwrap())
        .unwrap();
    (expected, proof, s, g, o, t)
}

#[tokio::test]
async fn signed_idle_cas_then_reserved_stop_cas_preserves_root_and_live_authority() {
    let (root, proof, s, mut g, mut o, t) = confirmed_fixture().await;
    let proof = proof
        .check_live(&root, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    let plan = allocate_stop(&root, owner(), proof, id(40), 100, id(71)).unwrap();
    let StopPlan::Change(candidate, pending) = plan else {
        panic!("first stop must allocate")
    };
    let next = candidate.result.clone();
    let before = root.capacity().unwrap();
    let after = next.capacity().unwrap();
    assert_eq!(after.nodes, before.nodes);
    assert_eq!(after.intents, before.intents + 1);
    assert_eq!(after.credits + 1, before.credits);
    assert_eq!(
        after.intents + after.credits,
        before.intents + before.credits
    );
    assert_eq!(after.admission_bytes, before.admission_bytes);
    let mut restored = next.clone();
    restored.intents.retain(|i| i.request_id != id(40));
    restored.revision = root.revision;
    restored.change_id = root.change_id;
    assert_eq!(restored, root);
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        candidate,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    g.observe(&OwnershipObservation::new(&next).unwrap())
        .unwrap();
    let proof = proof
        .check_live(&next, owner(), &mut o, &mut g, sample(t, 8))
        .unwrap();
    assert_eq!(
        g.authorize(
            proof.captured_generation(),
            &OwnershipObservation::new(&next).unwrap(),
            Ok(sample(t, 9))
        )
        .unwrap(),
        owner()
    );
}

// A new authenticated round against a complete root that already retains idle.
async fn fresh_confirmed(
    root: &Control,
    key: &WorkerSigningKey,
    s: &Storage<Fake>,
    g: &mut OwnershipGuard,
    t: Instant,
) -> (ConfirmedDrain, Observations) {
    g.observe(&OwnershipObservation::new(root).unwrap())
        .unwrap();
    let mut o = Observations::new(id(90), t);
    let p = positive(root, key, s, g, t, &mut o).await;
    let IdlePlan::Existing(proof) =
        confirm_idle(root, owner(), p, root.change_id, &mut o, g, sample(t, 6)).unwrap()
    else {
        panic!("retained idle")
    };
    (proof, o)
}
fn retained_idle(root: &mut Control) {
    let d = root.nodes[0].drain.as_mut().unwrap();
    d.ack_seq = Some(2);
    d.confirmed_idle = true;
}
fn retained_stop(root: &Control, request_id: Uuid) -> crate::cluster::member::Intent {
    crate::cluster::member::Intent {
        request_id,
        node_id: id(3),
        launch_id: id(6),
        incarnation_id: Some(id(4)),
        resource_id: root.nodes[0].resource_id.clone(),
        action: Action::Stop,
        state: IntentState::Pending,
        attempts: 0,
        created_at_ms: 100,
        deadline_ms: 100 + root.timing.drain_ms,
        retry_at_ms: 100,
        error_code: None,
        template_fingerprint: None,
        delivery: None,
        outcome: None,
    }
}
#[tokio::test]
async fn saturation_spends_reserved_credit_without_new_admission() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.timing.expiry_ms = 60000;
        r.timing.lease_ms = 60000;
        r.admin_seq = 8;
        r.admin_head = Some(id(88));
        r.scheduler.failures = 3;
        for n in 0..1364u128 {
            let mut node = r.nodes[0].clone();
            node.node_id = id(1000 + n * 4);
            node.launch_id = Some(id(1001 + n * 4));
            node.incarnation = Some(id(1002 + n * 4));
            node.resource_id = Some(format!("resource-{n}"));
            node.state = NodeState::Ready;
            node.drain = None;
            let mut start = r.intents[0].clone();
            start.request_id = node.launch_id.unwrap();
            start.launch_id = start.request_id;
            start.node_id = node.node_id;
            start.resource_id = node.resource_id.clone();
            r.nodes.push(node);
            r.intents.push(start);
        }
    })
    .await;
    root.check_new_admission().unwrap();
    let mut unreserved = root.clone();
    let mut node = root.nodes.last().unwrap().clone();
    node.node_id = id(9000);
    node.launch_id = Some(id(9001));
    node.incarnation = Some(id(9002));
    node.resource_id = Some("extra".into());
    let mut start = root.intents.last().unwrap().clone();
    start.node_id = node.node_id;
    start.launch_id = id(9001);
    start.request_id = id(9001);
    start.resource_id = node.resource_id.clone();
    unreserved.nodes.push(node);
    unreserved.intents.push(start);
    unreserved.encode().unwrap();
    assert_eq!(
        unreserved.check_new_admission(),
        Err(crate::cluster::state::ControlError::LimitExceeded)
    );
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let StopPlan::Change(candidate, pending) =
        allocate_stop(&root, owner(), proof, id(40), 100, id(71)).unwrap()
    else {
        panic!()
    };
    let next = candidate.result.clone();
    let a = root.capacity().unwrap();
    let b = next.capacity().unwrap();
    assert_eq!(b.nodes, a.nodes);
    assert_eq!(b.intents, a.intents + 1);
    assert_eq!(b.credits + 1, a.credits);
    assert_eq!(b.intents + b.credits, a.intents + a.credits);
    assert_eq!(
        b.nodes + b.intents + b.credits,
        a.nodes + a.intents + a.credits
    );
    assert_eq!(b.admission_bytes, a.admission_bytes);
    assert!(
        next.intents
            .windows(2)
            .all(|w| w[0].request_id < w[1].request_id)
    );
    let mut restored = next.clone();
    restored.intents.retain(|i| i.request_id != id(40));
    restored.revision = root.revision;
    restored.change_id = root.change_id;
    assert_eq!(restored, root);
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(60)).await.unwrap(),
        candidate,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(60))
    .await
    .unwrap();
    pending.confirm(outcome).unwrap();
    let loaded = s.load_control(t + Duration::from_secs(60)).await.unwrap();
    assert_eq!(loaded.root(), &next);
    // Codec-valid failure/acceptance shapes are accounting controls only. No
    // callback is executed and no terminal provider evidence is invented.
    for failed in [false, true] {
        let mut retained = next.clone();
        let stop = retained
            .intents
            .iter_mut()
            .find(|i| i.request_id == id(40))
            .unwrap();
        if failed {
            stop.state = IntentState::Failed;
            stop.outcome = Some(crate::cluster::member::IntentOutcome::NotInvoked);
        } else {
            stop.state = IntentState::Reconciling;
        }
        let decoded = Control::decode(&retained.encode().unwrap()).unwrap();
        assert_eq!(decoded.capacity().unwrap(), b);
        assert_eq!(decoded.nodes, next.nodes);
    }
}
#[tokio::test]
async fn competing_actual_cas_then_fresh_round_converges_on_winner() {
    let (root, key, s, mut g, t) = setup_with(retained_idle).await;
    let (p1, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let (p2, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let StopPlan::Change(c1, p1) = allocate_stop(&root, owner(), p1, id(40), 100, id(71)).unwrap()
    else {
        panic!()
    };
    let StopPlan::Change(c2, p2) = allocate_stop(&root, owner(), p2, id(41), 100, id(72)).unwrap()
    else {
        panic!()
    };
    let d = t + Duration::from_secs(5);
    let a = crate::cluster::store::PreparedDrain::prepare(s.load_control(d).await.unwrap(), c1)
        .unwrap();
    let b = crate::cluster::store::PreparedDrain::prepare(s.load_control(d).await.unwrap(), c2)
        .unwrap();
    let (a, b) = tokio::join!(a.submit(&s, d), b.submit(&s, d));
    let a = a.unwrap();
    let b = b.unwrap();
    let accepted = usize::from(p1.confirm(a).is_ok()) + usize::from(p2.confirm(b).is_ok());
    let loaded = s.load_control(d).await.unwrap();
    let winner = loaded.root();
    let ids: Vec<_> = winner
        .intents
        .iter()
        .filter(|i| i.action == Action::Stop)
        .map(|i| i.request_id)
        .collect();
    let (proof, mut o) = fresh_confirmed(winner, &key, &s, &mut g, t).await;
    let StopPlan::Existing(existing) =
        allocate_stop(winner, owner(), proof, id(3), u64::MAX, winner.change_id).unwrap()
    else {
        panic!()
    };
    let (proof, stop) = existing.into_parts();
    proof
        .check_live(winner, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    assert_eq!(accepted, 1);
    assert_eq!(ids, vec![stop]);
    assert_eq!(winner.intents.len(), root.intents.len() + 1);
}
#[tokio::test]
async fn retained_failure_and_exact_max_revision_converge_before_growth_checks() {
    for failed in [false, true] {
        let (root, key, s, mut g, t) = setup_with(|r| {
            retained_idle(r);
            r.revision = u64::MAX - 1;
            let mut stop = retained_stop(r, id(40));
            if failed {
                stop.state = IntentState::Failed;
                stop.outcome = Some(crate::cluster::member::IntentOutcome::NotInvoked);
            }
            r.intents.push(stop);
        })
        .await;
        let (proof, mut o) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
        let plan = allocate_stop(&root, owner(), proof, id(3), u64::MAX, root.change_id).unwrap();
        assert_eq!(format!("{plan:?}"), "StopPlan");
        let StopPlan::Existing(existing) = plan else {
            panic!()
        };
        assert_eq!(format!("{existing:?}"), "ExistingStop");
        let (proof, stop) = existing.into_parts();
        proof
            .check_live(&root, owner(), &mut o, &mut g, sample(t, 7))
            .unwrap();
        assert_eq!(stop, id(40));
        assert_eq!(root.revision, u64::MAX);
        assert_eq!(root.capacity().unwrap().credits, 0);
    }
}
#[tokio::test]
async fn changed_overflow_and_retained_id_collisions_have_distinct_errors() {
    for case in 0..5 {
        let (root, key, s, mut g, t) = setup_with(|r| {
            retained_idle(r);
            if case == 0 {
                r.revision = u64::MAX - 1;
            }
        })
        .await;
        let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
        let stop = if case == 2 {
            id(3)
        } else if case == 3 {
            id(71)
        } else {
            id(40)
        };
        let proposal = if case == 4 { root.change_id } else { id(71) };
        let now = if case == 1 { u64::MAX } else { 100 };
        let result = allocate_stop(&root, owner(), proof, stop, now, proposal);
        if case < 2 {
            assert!(matches!(result, Err(StorageError::LimitExceeded)));
        } else {
            assert!(matches!(result, Err(StorageError::Conflict)));
        }
    }
}
#[tokio::test]
async fn ambiguous_retained_stops_refuse_without_a_new_identity() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.intents.push(retained_stop(r, id(40)));
        r.intents.push(retained_stop(r, id(41)));
    })
    .await;
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    assert!(matches!(
        allocate_stop(&root, owner(), proof, id(42), 100, id(71)),
        Err(StorageError::Conflict)
    ));
}
#[tokio::test]
async fn external_proof_refuses_at_allocator_and_unknown_unregistered_refuse_earlier() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.provisioning = crate::cluster::state::Provisioning::External;
        r.config.adapter_contract = None;
        r.nodes[0].ownership = Ownership::External;
        r.nodes[0].launch_id = None;
        r.intents.clear();
    })
    .await;
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    assert!(matches!(
        allocate_stop(&root, owner(), proof, id(40), 100, id(71)),
        Err(StorageError::Conflict)
    ));
    let (mut root, _, _, mut g, t) = setup_with(|_| {}).await;
    root.nodes[0].resource_id = None;
    root.intents[0].resource_id = None;
    root.intents[0].state = IntentState::Reconciling;
    root.intents[0].outcome = None;
    root.nodes[0].registration = crate::cluster::member::Registration::Unregistered;
    root.nodes[0].incarnation = None;
    root.nodes[0].session = None;
    root.nodes[0].endpoint = None;
    let root = Control::decode(&root.encode().unwrap()).unwrap();
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap_err(); // Same revision changed contents closes authority.
    let binding = match crate::cluster::drain::begin_drain(&root, owner(), id(3), id(30), 0, id(33))
        .unwrap()
    {
        DrainPlan::Existing(b) => b,
        _ => panic!(),
    };
    let (a, bytes) = context(&root, 1);
    assert!(matches!(
        DrainRound::new(&root, owner(), binding, a, bytes, &mut g, sample(t, 2)),
        Err(StorageError::Conflict)
    ));
}
#[tokio::test]
async fn pending_allocation_refuses_wrong_expected_root_unknown_and_conflict() {
    for case in 0..3 {
        let (root, proof, s, _, _, t) = confirmed_fixture().await;
        let StopPlan::Change(candidate, pending) =
            allocate_stop(&root, owner(), proof, id(40), 100, id(71)).unwrap()
        else {
            panic!()
        };
        let d = t + Duration::from_secs(5);
        let outcome = if case == 0 {
            OwnershipOutcome::Acknowledged(s.load_control(d).await.unwrap())
        } else {
            let outcome = crate::cluster::store::PreparedDrain::prepare(
                s.load_control(d).await.unwrap(),
                candidate,
            )
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
            assert!(matches!(outcome, OwnershipOutcome::Acknowledged(_)));
            let loaded = s.load_control(d).await.unwrap();
            if case == 1 {
                OwnershipOutcome::Unknown {
                    current: Some(loaded),
                }
            } else {
                OwnershipOutcome::Conflict {
                    current: Some(loaded),
                }
            }
        };
        assert!(pending.confirm(outcome).is_err());
    }
}
#[test]
fn plans_and_retained_owner_are_not_clone_or_serde() {
    trait NotClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> NotClone<()> for T {}
    impl<T: Clone> NotClone<u8> for T {}
    trait NotSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> NotSerialize<()> for T {}
    impl<T: serde::Serialize> NotSerialize<u8> for T {}
    trait NotDeserialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> NotDeserialize<()> for T {}
    impl<T: serde::de::DeserializeOwned> NotDeserialize<u8> for T {}
    macro_rules! opaque {($($t:ty),*)=>{$(let _=<$t as NotClone<_>>::marker;let _=<$t as NotSerialize<_>>::marker;let _=<$t as NotDeserialize<_>>::marker;)*}}
    opaque!(StopPlan, ExistingStop);
}

#[tokio::test]
async fn lost_allocation_reply_exact_read_can_confirm_but_gated_takeover_cannot() {
    for takeover in [false, true] {
        let (root, proof, s, _, _, t) = confirmed_fixture().await;
        let StopPlan::Change(candidate, pending) =
            allocate_stop(&root, owner(), proof, id(40), 100, id(71)).unwrap()
        else {
            panic!()
        };
        let expected = candidate.result.clone();
        let d = t + Duration::from_secs(5);
        let prepared = crate::cluster::store::PreparedDrain::prepare(
            s.load_control(d).await.unwrap(),
            candidate,
        )
        .unwrap();
        let gate = std::sync::Arc::new(crate::test_storage::ReadGate::default());
        {
            let mut state = s.backend().state.lock().unwrap();
            state.lose_put_response = true;
            state.read_gate = Some(gate.clone());
        }
        let s = std::sync::Arc::new(s);
        let submitted = s.clone();
        let mut task = tokio::spawn(async move { prepared.submit(&submitted, d).await });
        let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.acquire())
            .await
            .is_ok();
        // Capture failures as results until the original submission is released,
        // settled or aborted and joined. No assertion may abandon this gate.
        s.backend().state.lock().unwrap().lose_put_response = false;
        let changed = if entered && takeover {
            async {
                let loaded = s.load_control(d).await?;
                let proposal = OwnershipProposal::new(
                    loaded.root(),
                    OwnershipChange::Acquire { process_id: id(92) },
                    id(99),
                    Ok(sample(t, 20000)),
                )?;
                proposal.prepare(loaded)?.submit(&s, d).await
            }
            .await
            .map(Some)
        } else {
            Ok(None)
        };
        gate.release.add_permits(1);
        let settled = match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
            Ok(result) => Some(result),
            Err(_) => {
                task.abort();
                let _ = task.await;
                None
            }
        };
        s.backend().state.lock().unwrap().read_gate = None;
        let current = s
            .load_control(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(entered);
        let changed = changed.unwrap();
        if takeover {
            assert!(matches!(changed, Some(OwnershipOutcome::Acknowledged(_))));
        }
        let outcome = settled
            .expect("bounded submission settlement")
            .unwrap()
            .unwrap();
        assert_eq!(
            matches!(&outcome, OwnershipOutcome::ObservedExact(_)),
            !takeover
        );
        assert_eq!(
            matches!(&outcome, OwnershipOutcome::Unknown { .. }),
            takeover
        );
        assert_eq!(pending.confirm(outcome).is_ok(), !takeover);
        let current = current.unwrap();
        assert_eq!(current.root().intents, expected.intents);
        if takeover {
            assert_eq!(current.root().controller.owner_id, Some(id(92)));
        }
    }
}
#[tokio::test]
async fn current_binding_or_owner_change_refuses_consumed_proof() {
    for resource_change in [false, true] {
        let (root, proof, _, _, _, _) = confirmed_fixture().await;
        let mut current = root.clone();
        let mut current_owner = owner();
        if resource_change {
            current.nodes[0].resource_id = Some("replacement".into());
            current.intents[0].resource_id = Some("replacement".into());
            current.revision += 1;
            current.change_id = id(75);
        } else {
            current_owner.epoch += 1;
        }
        let current = Control::decode(&current.encode().unwrap()).unwrap();
        assert!(matches!(
            allocate_stop(&current, current_owner, proof, id(40), 100, id(71)),
            Err(StorageError::Conflict)
        ));
    }
    let (mut root, _, _, _, _, _) = confirmed_fixture().await;
    let mut bad = retained_stop(&root, id(40));
    bad.incarnation_id = Some(id(999));
    root.intents.push(bad);
    assert!(root.encode().is_err()); // Contradictory stop cannot reach a valid proof root.
}

#[tokio::test]
async fn registered_unknown_resource_proof_is_refused_by_allocator() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.nodes[0].registration = crate::cluster::member::Registration::Registered;
        r.nodes[0].resource_id = None;
        r.intents[0].resource_id = None;
        r.intents[0].state = IntentState::Reconciling;
        r.intents[0].outcome = None;
    })
    .await;
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    assert!(matches!(
        allocate_stop(&root, owner(), proof, id(40), 100, id(71)),
        Err(StorageError::Conflict)
    ));
}
