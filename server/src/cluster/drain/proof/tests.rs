use super::*;
use crate::cluster::configuration::{Template, Timing};
use crate::node::auth::*;
use crate::{
    storage::{Namespace, ObjectKey, Storage, WriteCondition},
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
#[tokio::test]
async fn whole_root_confirmation_accepts_actual_loaded_ack() {
    let (root, _) = fixture();
    let fake = Fake::default();
    let storage = Storage::new(fake, Namespace::new("meshdb/").unwrap());
    storage
        .put(
            &ObjectKey::control(),
            root.encode().unwrap(),
            WriteCondition::Create,
        )
        .await
        .unwrap();
    let loaded = storage
        .load_control(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(exact_outcome(
        &root,
        &OwnershipOutcome::Acknowledged(loaded)
    ));
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
async fn setup() -> (
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
    w.publish(s, h.clone(), t + Duration::from_secs(5))
        .await
        .unwrap();
    o.reconcile_members(root, t).unwrap();
    let loaded = load_heartbeat(s, &b, t + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
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
    w.publish(s, h, t + Duration::from_secs(5)).await.unwrap();
    let loaded = load_heartbeat(s, &b, t + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    o.drain_heartbeat(
        root,
        &b,
        &loaded,
        t + Duration::from_millis(1),
        Duration::from_millis(root.timing.expiry_ms),
    )
    .unwrap()
}
#[tokio::test]
async fn authenticated_loaded_positive_baseline() {
    let (root, key, s, mut g, t) = setup().await;
    let owner = Owner {
        process_id: id(90),
        epoch: 1,
    };
    let binding = match super::super::begin_drain(&root, owner, id(3), id(30), 0, id(33)).unwrap() {
        DrainPlan::Existing(b) => b,
        _ => panic!(),
    };
    let (a, bytes) = context(&root, 1);
    let round = DrainRound::new(&root, owner, binding, a, bytes, &mut g, sample(t, 2)).unwrap();
    let reply = handoff(&root, &key, round, true, 2);
    let mut o = Observations::new(id(90), t);
    let h = receipt(&root, &s, &mut o, t + Duration::from_millis(2)).await;
    assert!(matches!(
        check_drain_reply(&root, owner, reply, h, &mut o, &mut g, sample(t, 5)).unwrap(),
        DrainCheck::Positive(_)
    ));
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 1,
    }
}
fn round(root: &Control, g: &mut OwnershipGuard, t: Instant, seq: u64) -> DrainRound {
    let binding = match super::super::begin_drain(root, owner(), id(3), id(30), 0, id(33)).unwrap()
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
    let h = receipt(root, s, o, t + Duration::from_millis(2)).await;
    match check_drain_reply(
        root,
        owner(),
        handoff(root, key, r, true, 2),
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
#[tokio::test]
async fn reply_and_live_observation_refusals_are_independent() {
    for case in 0..9 {
        let (root, key, s, mut g, t) = setup().await;
        let r = round(&root, &mut g, t, 1);
        let mut o = Observations::new(id(90), t);
        let h = receipt(&root, &s, &mut o, t + Duration::from_millis(2)).await;
        let mut current = root.clone();
        let mut own = owner();
        let mut at = 5;
        match case {
            0 => {}
            1 => {}
            2 => {
                current.scheduler.failures += 1;
                current.revision += 1;
                current.change_id = id(60);
            }
            3 => own.epoch += 1,
            4 => {
                g.invalidate().unwrap();
            }
            5 => {
                o.read_failed(&hb_binding(&root), t + Duration::from_millis(4))
                    .unwrap();
            }
            6 => {
                at = root.timing.expiry_ms + 5;
            }
            7 => {
                let mut bad = h.heartbeat().clone();
                bad.sequence += 1;
                bad.running_operations = 1;
                o.observe(&hb_binding(&root), &bad, t + Duration::from_millis(4))
                    .unwrap();
            }
            8 => {
                let mut bad = h.heartbeat().clone();
                bad.sequence -= 1;
                assert!(
                    o.observe(&hb_binding(&root), &bad, t + Duration::from_millis(4))
                        .is_err()
                );
            }
            _ => unreachable!(),
        }
        let reply = handoff(&root, &key, r, case != 0, if case == 1 { 3 } else { 2 });
        let result = check_drain_reply(&current, own, reply, h, &mut o, &mut g, sample(t, at));
        assert!(
            !matches!(result, Ok(DrainCheck::Positive(_))),
            "case {case}"
        );
    }
}
#[tokio::test]
async fn contradiction_then_zero_cannot_restore_retained_positive() {
    for failed_read in [false, true] {
        let (root, key, s, mut g, t) = setup().await;
        let mut o = Observations::new(id(90), t);
        let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
        let mut later = p.heartbeat.heartbeat().clone();
        later.sequence += 1;
        later.queued_partitions = 1;
        if failed_read {
            o.read_failed(&hb_binding(&root), t + Duration::from_millis(6))
                .unwrap();
        } else {
            o.observe(&hb_binding(&root), &later, t + Duration::from_millis(6))
                .unwrap();
        }
        later.sequence += 1;
        later.queued_partitions = 0;
        o.observe(&hb_binding(&root), &later, t + Duration::from_millis(7))
            .unwrap();
        assert!(confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 8)).is_err());
        // A new round needs actual later loaded evidence, not the old receipt.
        let r = round(&root, &mut g, t, 2);
        s.backend().state.lock().unwrap().objects.insert(
            format!("test/cluster/heartbeats/{}.json", id(4)),
            (later.encode().unwrap(), "fresh".into()),
        );
        // Publish through a new real writer, which reconciles the retained object.
        let b = hb_binding(&root);
        let mut w = HeartbeatWriter::new(b.clone()).unwrap();
        let hs = HeartbeatSample {
            running_operations: 0,
            running_partitions: 0,
            queued_operations: 0,
            queued_partitions: 0,
            drain_id: Some(id(30)),
            accepting: false,
        };
        for _ in 0..2 {
            w.publish(&s, hs.clone(), t + Duration::from_secs(5))
                .await
                .unwrap();
        }
        let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        let h = o
            .drain_heartbeat(
                &root,
                &b,
                &loaded,
                t + Duration::from_millis(9),
                Duration::from_millis(root.timing.expiry_ms),
            )
            .unwrap();
        let seq = h.heartbeat().sequence;
        assert!(matches!(
            check_drain_reply(
                &root,
                owner(),
                handoff(&root, &key, r, true, seq),
                h,
                &mut o,
                &mut g,
                sample(t, 10)
            )
            .unwrap(),
            DrainCheck::Positive(_)
        ));
    }
}
#[tokio::test]
async fn idle_confirmation_preserves_root_and_refuses_competing_metadata() {
    let (root, key, s, mut g, t) = setup().await;
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let IdlePlan::Change(c, pending) =
        confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
    else {
        panic!()
    };
    let expected = c.result.clone();
    let mut restored = expected.clone();
    restored.revision = root.revision;
    restored.change_id = root.change_id;
    restored.nodes[0].drain = root.nodes[0].drain.clone();
    assert_eq!(restored, root);
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        c,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let confirmed = pending.confirm(outcome).unwrap();
    assert!(
        confirmed
            .check_for_stop(&expected, owner(), g.authorization_generation())
            .is_ok()
    );
    let mut other = expected.clone();
    other.scheduler.failures += 1;
    assert!(
        confirmed
            .check_for_stop(&other, owner(), g.authorization_generation())
            .is_err()
    );
    assert!(
        confirmed
            .check_for_stop(&expected, owner(), g.authorization_generation())
            .is_ok()
    );
    g.observe(&OwnershipObservation::new(&expected).unwrap())
        .unwrap();
    let _confirmed = confirmed
        .check_live(&expected, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    let loaded = s.load_control(t + Duration::from_secs(5)).await.unwrap();
    assert!(exact_outcome(
        &expected,
        &OwnershipOutcome::ObservedExact(loaded.clone())
    ));
    assert!(!exact_outcome(
        &other,
        &OwnershipOutcome::Acknowledged(loaded.clone())
    ));
    assert!(!exact_outcome(
        &expected,
        &OwnershipOutcome::Conflict {
            current: Some(loaded.clone())
        }
    ));
    assert!(!exact_outcome(
        &expected,
        &OwnershipOutcome::Unknown {
            current: Some(loaded)
        }
    ));
}
#[tokio::test]
async fn exact_idle_at_exhausted_revision_converges_before_arithmetic() {
    let (mut root, key, s, mut g, t) = setup().await;
    root.revision = u64::MAX;
    root.change_id = id(80);
    let d = root.nodes[0].drain.as_mut().unwrap();
    d.ack_seq = Some(2);
    d.confirmed_idle = true;
    root.encode().unwrap();
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    assert!(matches!(
        confirm_idle(
            &root,
            owner(),
            p,
            root.change_id,
            &mut o,
            &mut g,
            sample(t, 6)
        )
        .unwrap(),
        IdlePlan::Existing(_)
    ));
}
#[tokio::test]
async fn original_context_cannot_be_replaced_by_identical_auth() {
    let (root, key, _s, mut g, t) = setup().await;
    let r = round(&root, &mut g, t, 1);
    let other = round(&root, &mut g, t, 1);
    let (e, c) = reply_proofs(&root, &key, other.context().unwrap(), true, 2);
    assert!(r.bind_reply(e, c).is_err());
    assert!(
        handoff(&root, &key, other, true, 2)
            .reply
            .drain()
            .unwrap()
            .idle()
    );
}
async fn confirmed_fixture() -> (
    Control,
    ConfirmedDrain,
    Storage<Fake>,
    OwnershipGuard,
    Observations,
    Instant,
) {
    let (root, key, s, mut g, t) = setup().await;
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
fn stop_candidate(root: &Control) -> DrainCandidate {
    let mut next = root.clone();
    next.revision += 1;
    next.change_id = id(71);
    next.intents.push(crate::cluster::member::Intent {
        request_id: id(40),
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
    });
    next.encode().unwrap();
    DrainCandidate {
        base: OwnershipObservation::new(root).unwrap(),
        owner: owner(),
        result: next,
    }
}
#[tokio::test]
async fn exact_allocation_and_claim_chain_reaches_fresh_guard_authorization() {
    let (root, proof, s, mut g, mut o, t) = confirmed_fixture().await;
    let proof = proof
        .check_live(&root, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    let (candidate, pending) = proof.prepare_stop_successor(stop_candidate(&root)).unwrap();
    let allocated = candidate.result.clone();
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        candidate,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    g.observe(&OwnershipObservation::new(&allocated).unwrap())
        .unwrap();
    let proof = proof
        .check_live(&allocated, owner(), &mut o, &mut g, sample(t, 8))
        .unwrap();
    let mut claimed = allocated.clone();
    claimed.revision += 1;
    claimed.change_id = id(72);
    let stop = claimed
        .intents
        .iter_mut()
        .find(|i| i.action == Action::Stop)
        .unwrap();
    stop.state = IntentState::InProgress;
    stop.attempts = 1;
    stop.delivery = Some(crate::cluster::member::Delivery {
        invocation_id: id(41),
        owner_id: id(90),
        epoch: 1,
        command_sha256: "a".repeat(64),
    });
    let pending = proof
        .prepare_stop_claim(&allocated, &claimed, &"a".repeat(64))
        .unwrap();
    let candidate = DrainCandidate {
        base: OwnershipObservation::new(&allocated).unwrap(),
        owner: owner(),
        result: claimed.clone(),
    };
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        candidate,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    assert!(pending.matches_outcome(&outcome));
    let proof = pending.confirm(outcome).unwrap();
    let observed = OwnershipObservation::new(&claimed).unwrap();
    g.observe(&observed).unwrap();
    let proof = proof
        .check_live(&claimed, owner(), &mut o, &mut g, sample(t, 9))
        .unwrap();
    assert!(
        proof
            .check_for_stop(&claimed, owner(), g.authorization_generation())
            .is_ok()
    );
    assert_eq!(
        g.authorize(g.authorization_generation(), &observed, Ok(sample(t, 10)))
            .unwrap(),
        owner()
    );
}
#[tokio::test]
async fn allocation_successor_rejects_unrelated_changes_and_reused_proposals() {
    for case in 0..5 {
        let (root, proof, _s, _g, _o, _t) = confirmed_fixture().await;
        let mut c = stop_candidate(&root);
        match case {
            0 => c.result.scheduler.failures += 1,
            1 => c.result.nodes[0].drain.as_mut().unwrap().ack_seq = Some(3),
            2 => c.result.change_id = root.change_id,
            3 => c.owner.epoch += 1,
            4 => c.result.intents.last_mut().unwrap().created_at_ms += 1,
            _ => unreachable!(),
        }
        assert!(proof.prepare_stop_successor(c).is_err(), "case {case}");
    }
}
#[tokio::test]
async fn exact_outcome_competition_is_metadata_only() {
    let (root, key, s, mut g, t) = setup().await;
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let IdlePlan::Change(c, pending) =
        confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
    else {
        panic!()
    };
    let prepared = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        c,
    )
    .unwrap();
    let mut competing = root.clone();
    competing.revision += 1;
    competing.change_id = id(81);
    competing.scheduler.failures += 1;
    s.backend().state.lock().unwrap().objects.insert(
        "test/cluster/control.json".into(),
        (competing.encode().unwrap(), "competitor".into()),
    );
    let result = prepared
        .submit(&s, t + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(pending.confirm(result).is_err());
    // Metadata submission never changes the guard's generation or establishes a lease.
    assert_eq!(g.check(Ok(sample(t, 7))).unwrap(), owner());
}
#[tokio::test]
async fn late_idle_false_invalidates_other_retained_positive() {
    let (root, key, s, mut g, t) = setup().await;
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let r = round(&root, &mut g, t, 2);
    let b = hb_binding(&root);
    let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    let h = o
        .drain_heartbeat(
            &root,
            &b,
            &loaded,
            t + Duration::from_millis(6),
            Duration::from_millis(root.timing.expiry_ms),
        )
        .unwrap();
    let refused = check_drain_reply(
        &root,
        owner(),
        handoff(&root, &key, r, false, 2),
        h,
        &mut o,
        &mut g,
        sample(t, 7),
    )
    .unwrap();
    assert!(matches!(refused, DrainCheck::Pending));
    assert!(confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 8)).is_err());
}

// Default fixture's live API identity; foreign-boot cases call the production API directly.
fn check_drain_reply(
    root: &Control,
    owner: Owner,
    bound: DrainRoundReply,
    h: crate::cluster::heartbeat::DrainHeartbeat,
    o: &mut Observations,
    g: &mut OwnershipGuard,
    sample: OwnershipSample,
) -> Result<DrainCheck, StorageError> {
    let api = bound.reply.context().auth().sender().clone();
    super::check_drain_reply(root, owner, bound, &api, h, o, g, sample)
}
#[tokio::test]
async fn current_api_boot_and_key_are_required() {
    for key_change in [false, true] {
        let (root, key, s, mut g, t) = setup().await;
        let r = round(&root, &mut g, t, 1);
        let mut o = Observations::new(id(90), t);
        let h = receipt(&root, &s, &mut o, t + Duration::from_millis(2)).await;
        let bound = handoff(&root, &key, r, true, 2);
        let (a, _) = context(&root, 1);
        let text = String::from_utf8(a.canonical_bytes().unwrap()).unwrap();
        let text = if key_change {
            text.replace(&"ab".repeat(32), &"ac".repeat(32))
        } else {
            text.replace(&id(2).to_string(), &id(99).to_string())
        };
        let a = Auth::decode(
            &hex(text.as_bytes()),
            &ActualRoute::new("POST", "/internal/v1/drain", None).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            super::check_drain_reply(
                &root,
                owner(),
                bound,
                a.sender(),
                h,
                &mut o,
                &mut g,
                sample(t, 5)
            )
            .unwrap(),
            DrainCheck::Pending
        ));
    }
}
#[tokio::test]
async fn real_lost_reply_gate_distinguishes_exact_from_superseding_root() {
    for supersede in [false, true] {
        let (root, key, s, mut g, t) = setup().await;
        let mut o = Observations::new(id(90), t);
        let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
        let IdlePlan::Change(c, pending) =
            confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
        else {
            panic!()
        };
        let expected = c.result.clone();
        let prepared = crate::cluster::store::PreparedDrain::prepare(
            s.load_control(t + Duration::from_secs(5)).await.unwrap(),
            c,
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
        let mut task = tokio::spawn(async move {
            prepared
                .submit(&submitted, Instant::now() + Duration::from_secs(5))
                .await
        });
        let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.acquire())
            .await
            .is_ok();
        if supersede {
            let mut newer = expected.clone();
            newer.revision += 1;
            newer.change_id = id(99);
            newer.scheduler.failures += 1;
            s.backend().state.lock().unwrap().objects.insert(
                "test/cluster/control.json".into(),
                (newer.encode().unwrap(), "newer".into()),
            );
            g.observe(&OwnershipObservation::new(&newer).unwrap())
                .unwrap();
        }
        gate.release.add_permits(1);
        // Timeout never detaches the original submission or abandons a Fake effect.
        let completion = match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
            Ok(v) => v,
            Err(_) => task.await,
        };
        let outcome = completion.unwrap().unwrap();
        let was_exact = matches!(&outcome, OwnershipOutcome::ObservedExact(_));
        let result = pending.confirm(outcome);
        assert!(entered);
        assert_eq!(was_exact, !supersede);
        assert_eq!(result.is_ok(), !supersede);
    }
}
#[test]
fn evidence_has_no_clone_or_serde_and_debug_is_redacted() {
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
    opaque!(
        DrainRound,
        DrainRoundReply,
        PositiveDrain,
        ConfirmedDrain,
        PendingDrainConfirmation,
        PendingStopAllocation,
        PendingStopClaim
    );
    assert_eq!(format!("{:?}", DrainCheck::Pending), "DrainCheck");
}
#[tokio::test]
async fn all_four_loaded_occupancy_metrics_refuse_independently() {
    for metric in 0..4 {
        let (root, key, s, mut g, t) = setup().await;
        let mut o = Observations::new(id(90), t);
        let r = round(&root, &mut g, t, 1);
        let h = receipt(&root, &s, &mut o, t + Duration::from_millis(2)).await;
        let mut loaded = h.heartbeat().clone();
        loaded.sequence += 1;
        match metric {
            0 => loaded.running_operations = 1,
            1 => loaded.running_partitions = 1,
            2 => loaded.queued_operations = 1,
            3 => loaded.queued_partitions = 1,
            _ => unreachable!(),
        }
        s.backend().state.lock().unwrap().objects.insert(
            format!("test/cluster/heartbeats/{}.json", id(4)),
            (loaded.encode().unwrap(), "occupied".into()),
        );
        let b = hb_binding(&root);
        let l = load_heartbeat(&s, &b, t + Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        let h = o
            .drain_heartbeat(
                &root,
                &b,
                &l,
                t + Duration::from_millis(4),
                Duration::from_millis(root.timing.expiry_ms),
            )
            .unwrap();
        assert!(matches!(
            check_drain_reply(
                &root,
                owner(),
                handoff(&root, &key, r, true, 2),
                h,
                &mut o,
                &mut g,
                sample(t, 5)
            )
            .unwrap(),
            DrainCheck::Pending
        ));
    }
}
async fn allocated_fixture() -> (Control, ConfirmedDrain, Storage<Fake>) {
    let (root, proof, s, _g, _o, t) = confirmed_fixture().await;
    let (candidate, pending) = proof.prepare_stop_successor(stop_candidate(&root)).unwrap();
    let expected = candidate.result.clone();
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        candidate,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    (expected, pending.confirm(outcome).unwrap(), s)
}
fn claim_successor(root: &Control) -> Control {
    let mut c = root.clone();
    c.revision += 1;
    c.change_id = id(72);
    let i = c
        .intents
        .iter_mut()
        .find(|i| i.action == Action::Stop)
        .unwrap();
    i.state = IntentState::InProgress;
    i.attempts += 1;
    i.delivery = Some(crate::cluster::member::Delivery {
        invocation_id: id(41),
        owner_id: id(90),
        epoch: 1,
        command_sha256: "a".repeat(64),
    });
    c
}
#[tokio::test]
async fn claim_successor_refuses_binding_counter_owner_digest_and_unrelated_changes() {
    for case in 0..7 {
        let (root, proof, _s) = allocated_fixture().await;
        let mut c = claim_successor(&root);
        let i = c
            .intents
            .iter_mut()
            .find(|i| i.action == Action::Stop)
            .unwrap();
        match case {
            0 => i.attempts += 1,
            1 => i.delivery.as_mut().unwrap().epoch += 1,
            2 => i.delivery.as_mut().unwrap().invocation_id = id(6),
            3 => i.delivery.as_mut().unwrap().command_sha256 = "b".repeat(64),
            4 => c.scheduler.failures += 1,
            5 => c.nodes[0].drain.as_mut().unwrap().ack_seq = Some(3),
            6 => c.change_id = root.change_id,
            _ => unreachable!(),
        }
        assert!(
            proof
                .prepare_stop_claim(&root, &c, &"a".repeat(64))
                .is_err(),
            "case {case}"
        );
    }
}
#[tokio::test]
async fn claim_confirmation_requires_entire_expected_root_and_outcome_kind() {
    for case in 0..4 {
        let (root, proof, _s) = allocated_fixture().await;
        let mut c = claim_successor(&root);
        let pending = proof
            .prepare_stop_claim(&root, &c, &"a".repeat(64))
            .unwrap();
        if case == 0 {
            c.scheduler.failures += 1;
        }
        let s = store(&c);
        let l = s
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let outcome = match case {
            0 => OwnershipOutcome::Acknowledged(l),
            1 => OwnershipOutcome::Conflict { current: Some(l) },
            2 => OwnershipOutcome::Unknown { current: Some(l) },
            3 => OwnershipOutcome::ObservedExact(l),
            _ => unreachable!(),
        };
        assert_eq!(pending.matches_outcome(&outcome), case == 3);
        assert_eq!(pending.confirm(outcome).is_ok(), case == 3);
    }
}
#[tokio::test]
async fn takeover_requires_fresh_loaded_sequence_advancement() {
    let (root, key, s, mut g, t) = setup().await;
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let mut next = root.clone();
    next.revision += 1;
    next.change_id = id(90);
    next.controller.owner_id = Some(id(91));
    next.controller.epoch += 1;
    g.observe(&OwnershipObservation::new(&next).unwrap())
        .unwrap();
    assert!(confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).is_err());
    let mut successor = Observations::new(id(91), t + Duration::from_millis(7));
    let b = hb_binding(&next);
    let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    let expiry = Duration::from_millis(next.timing.expiry_ms);
    assert!(
        successor
            .drain_heartbeat(&next, &b, &loaded, t + Duration::from_millis(8), expiry)
            .is_err()
    );
    assert!(
        successor
            .drain_heartbeat(&next, &b, &loaded, t + Duration::from_millis(9), expiry)
            .is_err()
    );
    let mut h = loaded.heartbeat().clone();
    h.sequence += 1;
    s.backend().state.lock().unwrap().objects.insert(
        format!("test/cluster/heartbeats/{}.json", id(4)),
        (h.encode().unwrap(), "advanced".into()),
    );
    let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert!(
        successor
            .drain_heartbeat(&next, &b, &loaded, t + Duration::from_millis(10), expiry)
            .is_ok()
    );
}
#[tokio::test]
async fn changed_idle_at_exhausted_revision_refuses_with_sanitized_error() {
    let (mut root, key, s, mut g, t) = setup().await;
    root.revision = u64::MAX;
    root.change_id = id(80);
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let result = confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6));
    assert!(matches!(result, Err(StorageError::LimitExceeded)));
    assert_eq!(
        format!("{:?}", StorageError::LimitExceeded),
        "LimitExceeded"
    );
}
#[tokio::test]
async fn fresh_round_can_continue_an_existing_retained_stop() {
    let (root, key, s, mut g, t) = setup().await;
    let root = stop_candidate(&root).result;
    s.backend().state.lock().unwrap().objects.insert(
        "test/cluster/control.json".into(),
        (root.encode().unwrap(), "retained-stop".into()),
    );
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    let mut o = Observations::new(id(90), t);
    let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
    let IdlePlan::Change(c, pending) =
        confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
    else {
        panic!()
    };
    let current = c.result.clone();
    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        c,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    let successor = claim_successor(&current);
    assert!(
        proof
            .prepare_stop_claim(&current, &successor, &"a".repeat(64))
            .is_ok()
    );
}

#[tokio::test]
async fn delayed_idle_false_invalidates_confirmed_successor_before_and_after_later_zero() {
    let mut results = Vec::new();
    for later_zero in [false, true] {
        let (root, key, s, mut g, t) = setup().await;
        let mut o = Observations::new(id(90), t);
        let p = positive(&root, &key, &s, &mut g, t, &mut o).await;
        let late_round = round(&root, &mut g, t, 2);
        let b = hb_binding(&root);
        let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        let h = o
            .drain_heartbeat(
                &root,
                &b,
                &loaded,
                t + Duration::from_millis(6),
                Duration::from_millis(root.timing.expiry_ms),
            )
            .unwrap();
        let delayed = handoff(&root, &key, late_round, false, 2);
        let IdlePlan::Change(candidate, pending) =
            confirm_idle(&root, owner(), p, id(70), &mut o, &mut g, sample(t, 6)).unwrap()
        else {
            panic!("expected idle confirmation change")
        };
        let current = candidate.result.clone();
        let outcome = crate::cluster::store::PreparedDrain::prepare(
            s.load_control(t + Duration::from_secs(5)).await.unwrap(),
            candidate,
        )
        .unwrap()
        .submit(&s, t + Duration::from_secs(5))
        .await
        .unwrap();
        let acknowledged = matches!(&outcome, OwnershipOutcome::Acknowledged(_));
        let confirmed = pending.confirm(outcome).unwrap();
        let observed = OwnershipObservation::new(&current).unwrap();
        g.observe(&observed).unwrap();
        // Positive control: exact continuation is usable before the delayed reply.
        let confirmed = confirmed
            .check_live(&current, owner(), &mut o, &mut g, sample(t, 7))
            .unwrap();
        let negative =
            check_drain_reply(&current, owner(), delayed, h, &mut o, &mut g, sample(t, 8));
        let invalidated = !o
            .matches_drain(&confirmed.proof.heartbeat, t + Duration::from_millis(9))
            .unwrap();
        let mut fresh_zero = true;
        if later_zero {
            let mut writer = HeartbeatWriter::new(b.clone()).unwrap();
            let heartbeat = HeartbeatSample {
                running_operations: 0,
                running_partitions: 0,
                queued_operations: 0,
                queued_partitions: 0,
                drain_id: Some(id(30)),
                accepting: false,
            };
            // The new writer first reconciles the unchanged stored sample.
            for _ in 0..2 {
                writer
                    .publish(&s, heartbeat.clone(), t + Duration::from_secs(5))
                    .await
                    .unwrap();
            }
            let loaded = load_heartbeat(&s, &b, t + Duration::from_secs(5))
                .await
                .unwrap()
                .unwrap();
            o.observe(&b, loaded.heartbeat(), t + Duration::from_millis(10))
                .unwrap();
            fresh_zero = loaded.heartbeat().sequence > 2
                && o.fresh(
                    &b,
                    t + Duration::from_millis(10),
                    Duration::from_millis(current.timing.expiry_ms),
                )
                .unwrap();
        }
        let proof_refused = confirmed
            .check_live(&current, owner(), &mut o, &mut g, sample(t, 11))
            .is_err();
        let guard_still_valid = g
            .authorize(g.authorization_generation(), &observed, Ok(sample(t, 12)))
            .is_ok();
        results.push((
            acknowledged && matches!(negative, Err(StorageError::Conflict)),
            invalidated,
            proof_refused,
            fresh_zero,
            guard_still_valid,
        ));
    }
    assert_eq!(results, vec![(true, true, true, true, true); 2]);
}
