use super::*;
use crate::cluster::configuration::{Template, Timing};
use crate::cluster::drain::proof::*;
use crate::cluster::drain::{DrainPlan, StopPlan, allocate_stop};
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
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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
        resources: None,
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
        available_threads: None,
        available_memory_bytes: None,
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
    let binding =
        match crate::cluster::drain::begin_drain(root, owner_for(root), id(3), id(30), 0, id(33))
            .unwrap()
        {
            DrainPlan::Existing(b) => b,
            _ => panic!(),
        };
    let (a, bytes) = context(root, seq);
    DrainRound::new(
        root,
        owner_for(root),
        binding,
        a,
        bytes,
        g,
        proof_sample(root, t, 2),
    )
    .unwrap()
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
    let h = receipt(
        root,
        s,
        o,
        t + Duration::from_millis(round_offset(root) + 2),
    )
    .await;
    match check_drain_reply(
        root,
        owner_for(root),
        handoff(root, key, r, true, 2),
        &api,
        h,
        o,
        g,
        proof_sample(root, t, 5),
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

    let outcome = crate::cluster::store::PreparedDrain::prepare(
        s.load_control(t + Duration::from_secs(5)).await.unwrap(),
        c,
    )
    .unwrap()
    .submit(&s, t + Duration::from_secs(5))
    .await
    .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    let expected = s
        .load_control(t + Duration::from_secs(5))
        .await
        .unwrap()
        .root()
        .clone();
    g.observe(&OwnershipObservation::new(&expected).unwrap())
        .unwrap();
    (expected, proof, s, g, o, t)
}

async fn allocated() -> (
    Control,
    ConfirmedDrain,
    Storage<Fake>,
    OwnershipGuard,
    Observations,
    Instant,
) {
    let (root, proof, s, mut g, mut o, t) = confirmed_fixture().await;
    let proof = proof
        .check_live(&root, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    let StopPlan::Change(c, pending) =
        allocate_stop(&root, owner(), proof, id(40), 100, id(71)).unwrap()
    else {
        panic!()
    };
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedDrain::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let proof = pending.confirm(outcome).unwrap();
    let root = s.load_control(d).await.unwrap().root().clone();
    g.observe(&OwnershipObservation::new(&root).unwrap())
        .unwrap();
    let proof = proof
        .check_live(&root, owner(), &mut o, &mut g, sample(t, 8))
        .unwrap();
    (root, proof, s, g, o, t)
}
#[tokio::test]
async fn initial_claim_accepts_actual_allocated_proof() {
    let (root, proof, s, mut g, mut o, t) = allocated().await;
    let generation = g.authorization_generation();
    let original =
        crate::cluster::callback::StopCommand::build(&root, owner(), id(40), id(80)).unwrap();
    let bytes = original.bytes().to_vec();
    let digest = original.digest().to_owned();
    let claim = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72)).unwrap();
    assert_eq!(format!("{claim:?}"), "StopClaim");
    let (c, pending) = claim.into_parts();
    let expected = c.result.clone();
    let mut restored = expected.clone();
    restored.revision = root.revision;
    restored.change_id = root.change_id;
    restored.intents = root.intents.clone();
    assert_eq!(restored, root);
    let mut intent = expected.intents[1].clone();
    intent.state = root.intents[1].state;
    intent.attempts = root.intents[1].attempts;
    intent.delivery = root.intents[1].delivery.clone();
    assert_eq!(intent, root.intents[1]);
    assert_eq!(expected.capacity().unwrap(), root.capacity().unwrap());
    assert_eq!(
        expected.intents[1]
            .delivery
            .as_ref()
            .unwrap()
            .command_sha256,
        digest
    );
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    assert_eq!(format!("{pending:?}"), "PendingStop");
    let confirmed = pending.confirm(outcome).unwrap();
    assert_eq!(format!("{confirmed:?}"), "ConfirmedStop");
    let (actual, ctx, proof) = confirmed.into_parts();
    assert_eq!(actual, bytes);
    assert_eq!(ctx.intent_id(), original.context().intent_id());
    original
        .context()
        .matches_delivery(&expected, owner(), id(40))
        .unwrap();
    let current = s.load_control(d).await.unwrap();
    assert_eq!(current.root(), &expected);
    assert_eq!(
        crate::cluster::callback::validate_stop_bytes(&actual, current.root()).unwrap(),
        digest
    );
    ctx.matches_delivery(current.root(), owner(), id(40))
        .unwrap();
    g.observe(&OwnershipObservation::new(current.root()).unwrap())
        .unwrap();
    let _proof = proof
        .check_live(current.root(), owner(), &mut o, &mut g, sample(t, 10))
        .unwrap();
    assert_eq!(
        g.authorize(
            generation,
            &OwnershipObservation::new(current.root()).unwrap(),
            Ok(sample(t, 11))
        )
        .unwrap(),
        owner()
    );
}

async fn fresh_confirmed(
    root: &Control,
    key: &WorkerSigningKey,
    s: &Storage<Fake>,
    g: &mut OwnershipGuard,
    t: Instant,
) -> (ConfirmedDrain, Observations) {
    g.observe(&OwnershipObservation::new(root).unwrap())
        .unwrap();
    let mut o = Observations::new(
        owner_for(root).process_id,
        t + Duration::from_millis(round_offset(root)),
    );
    let p = positive(root, key, s, g, t, &mut o).await;
    let IdlePlan::Existing(proof) = confirm_idle(
        root,
        owner_for(root),
        p,
        root.change_id,
        &mut o,
        g,
        proof_sample(root, t, 6),
    )
    .unwrap() else {
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

async fn retained_fixture(
    edit: impl FnOnce(&mut Control),
) -> (
    Control,
    ConfirmedDrain,
    Storage<Fake>,
    OwnershipGuard,
    Observations,
    Instant,
) {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.intents.push(retained_stop(r, id(40)));
        edit(r);
    })
    .await;
    let (proof, o) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    (root, proof, s, g, o, t)
}
#[tokio::test]
async fn fresh_retained_stop_existing_path_claims_and_confirms() {
    let (root, proof, s, mut g, mut o, t) = retained_fixture(|_| {}).await;
    let StopPlan::Existing(existing) =
        allocate_stop(&root, owner(), proof, id(999), 100, id(71)).unwrap()
    else {
        panic!()
    };
    let (proof, stop) = existing.into_parts();
    assert_eq!(stop, id(40));
    let proof = proof
        .check_live(&root, owner(), &mut o, &mut g, sample(t, 7))
        .unwrap();
    let (c, p) = claim_stop(&root, owner(), stop, proof, id(80), 100, id(72))
        .unwrap()
        .into_parts();
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (bytes, ctx, proof) = p.confirm(outcome).unwrap().into_parts();
    let current = s.load_control(d).await.unwrap();
    crate::cluster::callback::validate_stop_bytes(&bytes, current.root()).unwrap();
    ctx.matches_delivery(current.root(), owner(), stop).unwrap();
    let generation = g.authorization_generation();
    g.observe(&OwnershipObservation::new(current.root()).unwrap())
        .unwrap();
    let _proof = proof
        .check_live(current.root(), owner(), &mut o, &mut g, sample(t, 9))
        .unwrap();
    g.authorize(
        generation,
        &OwnershipObservation::new(current.root()).unwrap(),
        Ok(sample(t, 10)),
    )
    .unwrap();
}
#[tokio::test]
async fn pending_eligibility_backoffs_and_checked_limits() {
    for case in 0..11 {
        let (root, proof, _, _, _, _) = retained_fixture(|r| match case {
            0 => r.intents[1].retry_at_ms = 101,
            1 => r.scheduler.backoff_until_ms = 101,
            2 => r.intents[1].state = IntentState::Reconciling,
            3 => {
                r.intents[1].state = IntentState::InProgress;
                r.intents[1].attempts = 1;
                r.intents[1].delivery = Some(crate::cluster::member::Delivery {
                    invocation_id: id(81),
                    owner_id: id(90),
                    epoch: 1,
                    command_sha256: "a".repeat(64),
                });
            }
            4 => r.intents[1].error_code = Some("callback_uncertain".into()),
            5 => {
                r.intents[1].state = IntentState::Failed;
                r.intents[1].outcome = Some(IntentOutcome::NotInvoked);
            }
            6 => r.intents[1].attempts = 1,
            7 => r.revision = u64::MAX - 1,
            8 => {
                r.intents[0].state = IntentState::Failed;
                r.intents[0].outcome = Some(IntentOutcome::NotInvoked);
            }
            9 => {
                r.intents[1].delivery = Some(crate::cluster::member::Delivery {
                    invocation_id: id(81),
                    owner_id: id(90),
                    epoch: 1,
                    command_sha256: "a".repeat(64),
                })
            }
            10 => r.intents[1].attempts = u64::MAX,
            _ => unreachable!(),
        })
        .await;
        let result = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72));
        if case == 7 {
            assert!(matches!(result, Err(StorageError::LimitExceeded)));
        } else {
            assert!(matches!(result, Err(StorageError::Conflict)), "case {case}");
        }
    }
}
#[tokio::test]
async fn invocation_and_proposal_cannot_alias_retained_identity_roles() {
    for identity in [1, 3, 4, 5, 6, 30, 31, 40, 90, 72] {
        let (root, proof, _, _, _, _) = retained_fixture(|_| {}).await;
        assert!(
            matches!(
                claim_stop(&root, owner(), id(40), proof, id(identity), 100, id(72)),
                Err(StorageError::Conflict)
            ),
            "{identity}"
        );
    }
    let (root, proof, _, _, _, _) = retained_fixture(|_| {}).await;
    assert!(claim_stop(&root, owner(), id(40), proof, id(80), 100, root.change_id).is_err());
}
#[tokio::test]
async fn exact_proof_rejects_independently_valid_root_target_and_owner_substitution() {
    for case in 0..7 {
        let (root, proof, _, _, _, _) = retained_fixture(|_| {}).await;
        let mut other = root.clone();
        let mut held = owner();
        let mut stop = id(40);
        match case {
            0 => {
                other.controller.owner_id = Some(id(91));
                other.controller.epoch += 1;
                held.process_id = id(91);
                held.epoch += 1;
            }
            1 => other.policy.revision += 1,
            2 => {
                other.nodes[0].resource_id = Some("different".into());
                other.intents[0].resource_id = other.nodes[0].resource_id.clone();
                other.intents[1].resource_id = other.nodes[0].resource_id.clone();
            }
            3 => {
                other.nodes[0].incarnation = Some(id(44));
                other.intents[1].incarnation_id = Some(id(44));
            }
            4 => {
                other.intents[1].request_id = id(41);
                stop = id(41);
            }
            5 => {
                other.nodes[0].node_id = id(33);
                for i in &mut other.intents {
                    i.node_id = id(33);
                }
            }
            6 => {
                other.nodes[0].launch_id = Some(id(7));
                other.intents[0].request_id = id(7);
                for i in &mut other.intents {
                    i.launch_id = id(7);
                }
            }
            _ => unreachable!(),
        }
        other.revision += 1;
        other.change_id = id(74);
        let other = Control::decode(&other.encode().expect("codec-valid substitution")).unwrap();
        assert!(
            matches!(
                claim_stop(&other, held, stop, proof, id(80), 100, id(72)),
                Err(StorageError::Conflict)
            ),
            "case {case}"
        );
    }
}
#[tokio::test]
async fn wrong_successor_and_same_uuid_different_complete_contents_release_nothing() {
    for different_contents in [false, true] {
        let (root, proof, s, _, _, t) = allocated().await;
        let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
            .unwrap()
            .into_parts();
        let expected = c.result.clone();
        let d = t + Duration::from_secs(5);
        let outcome = if different_contents {
            let mut wrong = expected.clone();
            wrong.scheduler.failures += 1;
            let bytes = wrong.encode().unwrap();
            s.backend().state.lock().unwrap().objects.insert(
                "test/cluster/control.json".into(),
                (bytes, "different".into()),
            );
            OwnershipOutcome::Acknowledged(s.load_control(d).await.unwrap())
        } else {
            OwnershipOutcome::Acknowledged(s.load_control(d).await.unwrap())
        };
        assert!(matches!(p.confirm(outcome), Err(StorageError::Conflict)));
    }
}

#[test]
fn claim_owners_are_not_clone_or_serde() {
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
    opaque!(StopClaim, PendingStop, ConfirmedStop);
}

#[tokio::test]
async fn lost_claim_reply_exact_read_jointly_confirms_but_gated_takeover_releases_nothing() {
    for takeover in [false, true] {
        let (root, proof, s, _, _, t) = allocated().await;
        let (candidate, pending) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
            .unwrap()
            .into_parts();
        let expected = candidate.result.clone();
        let d = t + Duration::from_secs(5);
        let prepared = crate::cluster::store::PreparedProvision::prepare(
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
        let confirmed = pending.confirm(outcome);
        assert_eq!(confirmed.is_ok(), !takeover);
        if let Ok(confirmed) = confirmed {
            let (bytes, context, _proof) = confirmed.into_parts();
            crate::cluster::callback::validate_stop_bytes(&bytes, &expected).unwrap();
            context
                .matches_delivery(&expected, owner(), id(40))
                .unwrap();
        }
        let current = current.unwrap();
        assert_eq!(current.root().intents, expected.intents);
        if takeover {
            assert_eq!(current.root().controller.owner_id, Some(id(92)));
        }
    }
}

#[tokio::test]
async fn explicit_unknown_or_conflict_with_expected_current_still_releases_nothing() {
    for unknown in [false, true] {
        let (root, proof, s, _, _, t) = allocated().await;
        let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
            .unwrap()
            .into_parts();
        let d = t + Duration::from_secs(5);
        let outcome =
            crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
                .unwrap()
                .submit(&s, d)
                .await
                .unwrap();
        assert!(matches!(outcome, OwnershipOutcome::Acknowledged(_)));
        let current = Some(s.load_control(d).await.unwrap());
        let outcome = if unknown {
            OwnershipOutcome::Unknown { current }
        } else {
            OwnershipOutcome::Conflict { current }
        };
        assert!(p.confirm(outcome).is_err());
    }
}
#[tokio::test]
async fn expired_intent_deadline_does_not_replace_live_proof_or_extend_round() {
    let (root, proof, s, mut g, mut o, t) = retained_fixture(|r| {
        r.intents[1].deadline_ms = 101;
    })
    .await;
    let generation = g.authorization_generation();
    let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 102, id(72))
        .unwrap()
        .into_parts();
    assert_eq!(c.result.intents[1].deadline_ms, 101);
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (_, _, proof) = p.confirm(outcome).unwrap().into_parts();
    let current = s.load_control(d).await.unwrap();
    g.observe(&OwnershipObservation::new(current.root()).unwrap())
        .unwrap();
    // The original authority deadline remains exhausted at this paired sample.
    assert!(
        proof
            .check_live(current.root(), owner(), &mut o, &mut g, sample(t, 20000))
            .is_err()
    );
    assert!(
        g.authorize(
            generation,
            &OwnershipObservation::new(current.root()).unwrap(),
            Ok(sample(t, 20001))
        )
        .is_err()
    );
}

#[tokio::test]
async fn expired_original_round_at_joint_confirmation_releases_nothing() {
    let (root, proof, s, _, _, t) = retained_fixture(|r| {
        r.timing.heartbeat_ms = 100;
        r.timing.expiry_ms = 1000;
        r.timing.max_attempts = 1;
    })
    .await;
    let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
        .unwrap()
        .into_parts();
    assert_eq!(c.result.intents[1].attempts, 1);
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    // An actual clock deadline expires the original heartbeat round. No field
    // or receipt is modified, and no authority deadline is extended.
    let expired = tokio::time::timeout_at(
        t + Duration::from_millis(1010),
        std::future::pending::<()>(),
    )
    .await;
    assert!(expired.is_err());
    assert!(matches!(p.confirm(outcome), Err(StorageError::Conflict)));
}

#[tokio::test]
async fn proof_for_first_target_cannot_claim_another_retained_target_in_same_root() {
    let (root, proof, _, _, _, _) = retained_fixture(|r| {
        let mut node = r.nodes[0].clone();
        node.node_id = id(34);
        node.launch_id = Some(id(7));
        node.incarnation = Some(id(45));
        node.resource_id = Some("other-resource".into());
        node.session.as_mut().unwrap().id = id(55);
        node.drain.as_mut().unwrap().id = id(35);
        let mut start = r.intents[0].clone();
        start.node_id = id(34);
        start.request_id = id(7);
        start.launch_id = id(7);
        start.resource_id = node.resource_id.clone();
        let mut stop = r.intents[1].clone();
        stop.request_id = id(41);
        stop.node_id = id(34);
        stop.launch_id = id(7);
        stop.incarnation_id = Some(id(45));
        stop.resource_id = node.resource_id.clone();
        r.nodes.push(node);
        r.intents.push(start);
        r.intents.push(stop);
        r.intents.sort_by_key(|i| i.request_id);
    })
    .await;
    assert!(matches!(
        claim_stop(&root, owner(), id(41), proof, id(80), 100, id(72)),
        Err(StorageError::Conflict)
    ));
}
#[tokio::test]
async fn multiple_retained_stops_refuse_initial_claim_without_selecting_one() {
    let (root, proof, _, _, _, _) =
        retained_fixture(|r| r.intents.push(retained_stop(r, id(41)))).await;
    assert!(matches!(
        claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72)),
        Err(StorageError::Conflict)
    ));
}

async fn delivered() -> (Control, ReplyContext, Storage<Fake>) {
    let (root, proof, s, _, _, t) = allocated().await;
    let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
        .unwrap()
        .into_parts();
    let d = t + Duration::from_secs(5);
    let outcome =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (_, ctx, _) = p.confirm(outcome).unwrap().into_parts();
    (s.load_control(d).await.unwrap().root().clone(), ctx, s)
}
#[tokio::test]
async fn stop_null_acceptance_preserves_everything_except_state_and_root_version() {
    let (root, ctx, s) = delivered().await;
    let reply = crate::cluster::callback::decode_reply(
        200,
        &serde_json::to_vec(&json!({"version":1,"invocation_id":id(80),"result":null})).unwrap(),
        &ctx,
    )
    .unwrap();
    let c = complete_stop(&root, owner(), &ctx, reply, 100, id(73)).unwrap();
    let mut expected = root.clone();
    expected.revision += 1;
    expected.change_id = id(73);
    expected.intents[1].state = IntentState::Reconciling;
    assert_eq!(c.result, expected);
    let d = Instant::now() + Duration::from_secs(5);
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    assert!(matches!(out, OwnershipOutcome::Acknowledged(v) if v.root() == &expected));
    assert_eq!(root.capacity().unwrap(), expected.capacity().unwrap());
}

fn owner_for(root: &Control) -> Owner {
    Owner {
        process_id: root.controller.owner_id.unwrap(),
        epoch: root.controller.epoch,
    }
}
fn inventory(
    root: &Control,
    records: serde_json::Value,
) -> crate::cluster::callback::InventoryEvidence {
    use crate::cluster::callback::{CallbackReply, InventoryCommand, decode_reply};
    let (_, ctx) = InventoryCommand::build(root, owner_for(root), id(200))
        .unwrap()
        .into_parts();
    let wire =
        serde_json::to_vec(&json!({"version":1,"invocation_id":id(200),"result":records})).unwrap();
    let CallbackReply::Inventory(e) = decode_reply(200, &wire, &ctx).unwrap() else {
        panic!()
    };
    e
}
fn inventory_record(root: &Control, resource: &str, state: &str) -> serde_json::Value {
    json!({"cluster_id":root.cluster_id,"node_id":id(3),"request_id":id(6),"resource_id":resource,"state":state})
}
async fn persist(s: &Storage<Fake>, c: ProvisionCandidate) -> Control {
    let d = Instant::now() + Duration::from_secs(5);
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(s, d)
            .await
            .unwrap();
    let OwnershipOutcome::Acknowledged(v) = out else {
        panic!()
    };
    v.root().clone()
}
#[tokio::test]
async fn correlated_denials_and_unrelated_wire_errors_keep_capacity() {
    use crate::cluster::callback::{CallbackReply, DeliveryOutcome, decode_reply};
    for (status, code, outcome, definite) in [
        (401, "unauthorized", "not_invoked", true),
        (409, "identity_conflict", "not_invoked", false),
        (409, "callback_in_progress", "unknown", false),
        (502, "callback_failed", "unknown", false),
    ] {
        let (root, ctx, _) = delivered().await;
        let wire = serde_json::to_vec(
            &json!({"version":1,"invocation_id":id(80),"error":{"code":code,"outcome":outcome}}),
        )
        .unwrap();
        let reply = decode_reply(status, &wire, &ctx).unwrap();
        let c = complete_stop(&root, owner(), &ctx, reply, 100, id(73)).unwrap();
        assert_eq!(
            c.result.intents[1].outcome,
            definite.then_some(IntentOutcome::NotInvoked)
        );
        assert_eq!(
            c.result.intents[1].state,
            if definite {
                IntentState::Failed
            } else {
                IntentState::Reconciling
            }
        );
        assert_eq!(c.result.nodes, root.nodes);
        assert_eq!(
            managed_capacity(&c.result).unwrap(),
            managed_capacity(&root).unwrap()
        );
        assert_eq!(c.result.capacity().unwrap(), root.capacity().unwrap());
        for prior in [false, true] {
            let mut later = root.clone();
            if prior {
                later.intents[1].error_code = Some("callback_uncertain".into());
            } else {
                later.intents[1].attempts = 2;
            }
            let later = Control::decode(&later.encode().unwrap()).unwrap();
            let reply = decode_reply(401,&serde_json::to_vec(&json!({"version":1,"invocation_id":id(80),"error":{"code":"unauthorized","outcome":"not_invoked"}})).unwrap(),&ctx).unwrap();
            assert_eq!(
                complete_stop(&later, owner(), &ctx, reply, 100, id(73))
                    .unwrap()
                    .result
                    .intents[1]
                    .outcome,
                None
            );
        }
        let reply = decode_reply(401,&serde_json::to_vec(&json!({"version":1,"invocation_id":null,"error":{"code":"unauthorized","outcome":"not_invoked"}})).unwrap(),&ctx).unwrap();
        assert_eq!(
            complete_stop(&root, owner(), &ctx, reply, 100, id(73))
                .unwrap()
                .result
                .intents[1]
                .outcome,
            None
        );
        assert!(matches!(
            complete_stop(
                &root,
                owner(),
                &ctx,
                CallbackReply::Failure {
                    code: "identity_conflict".into(),
                    outcome: DeliveryOutcome::Unknown
                },
                100,
                id(73)
            ),
            Err(StorageError::InvalidCatalog)
        ));
    }
}
#[tokio::test]
async fn uncertain_delivery_fresh_round_inventory_retry_joint_confirmation_and_authorization() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.intents.push(retained_stop(r, id(40)));
    })
    .await;
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
        .unwrap()
        .into_parts();
    let d = Instant::now() + Duration::from_secs(5);
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (_, old_ctx, old_proof) = p.confirm(out).unwrap().into_parts();
    let root = s.load_control(d).await.unwrap().root().clone();
    let c = uncertain_stop(&root, owner(), &old_ctx, 100, id(73)).unwrap();
    let root = persist(&s, c).await;
    assert!(
        old_proof
            .check_for_stop(&root, owner(), g.authorization_generation())
            .is_err()
    );
    assert!(uncertain_stop(&root, owner(), &old_ctx, 100, id(74)).is_err());
    let (proof, mut o) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let now = root.intents[1]
        .retry_at_ms
        .max(root.scheduler.backoff_until_ms);
    let (c, p) = retry_stop(
        &root,
        owner(),
        id(40),
        proof,
        inventory(&root, json!([])),
        id(81),
        now,
        id(74),
    )
    .unwrap()
    .into_parts();
    let mut restored = c.result.clone();
    restored.revision = root.revision;
    restored.change_id = root.change_id;
    restored.intents[1].state = root.intents[1].state;
    restored.intents[1].attempts = root.intents[1].attempts;
    restored.intents[1].delivery = root.intents[1].delivery.clone();
    assert_eq!(restored, root);
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (bytes, ctx, proof) = p.confirm(out).unwrap().into_parts();
    let next = s.load_control(d).await.unwrap().root().clone();
    crate::cluster::callback::validate_stop_bytes(&bytes, &next).unwrap();
    ctx.matches_delivery(&next, owner(), id(40)).unwrap();
    assert!(uncertain_stop(&next, owner(), &old_ctx, now, id(75)).is_err());
    let generation = g.authorization_generation();
    g.observe(&OwnershipObservation::new(&next).unwrap())
        .unwrap();
    let _proof = proof
        .check_live(&next, owner(), &mut o, &mut g, sample(t, 9))
        .unwrap();
    g.authorize(
        generation,
        &OwnershipObservation::new(&next).unwrap(),
        Ok(sample(t, 10)),
    )
    .unwrap();
}
#[tokio::test]
async fn inventory_negatives_termination_and_same_owner_in_progress_are_closed() {
    let (root, ctx, _) = delivered().await;
    assert!(matches!(
        reconcile_stop(
            &root,
            owner(),
            id(40),
            inventory(&root, json!([])),
            100,
            id(73)
        )
        .unwrap(),
        StopReconcilePlan::Unchanged(StopInventoryClass::Retryable)
    ));
    for mode in 0..4 {
        let mut record = inventory_record(
            &root,
            root.nodes[0].resource_id.as_ref().unwrap(),
            "running",
        );
        match mode {
            0 => record["request_id"] = json!(id(999)),
            1 => record["resource_id"] = json!("unknown"),
            2 => record["node_id"] = json!(id(999)),
            3 => record["state"] = json!("terminated"),
            _ => unreachable!(),
        }
        let plan = reconcile_stop(
            &root,
            owner(),
            id(40),
            inventory(&root, json!([record])),
            100,
            id(73),
        )
        .unwrap();
        if mode == 3 {
            assert!(matches!(
                plan,
                StopReconcilePlan::Unchanged(StopInventoryClass::NeedsTermination)
            ));
        } else {
            let StopReconcilePlan::Change {
                candidate,
                classification,
            } = plan
            else {
                panic!()
            };
            assert_eq!(classification, StopInventoryClass::Quarantined);
            assert_eq!(candidate.result.nodes, root.nodes);
            assert_eq!(candidate.result.intents[1].outcome, None);
            assert_eq!(
                candidate.result.intents[1].error_code.as_deref(),
                Some("inventory_quarantined")
            );
        }
    }
    let old = inventory(&root, json!([]));
    let next = uncertain_stop(&root, owner(), &ctx, 100, id(73))
        .unwrap()
        .result;
    assert!(reconcile_stop(&next, owner(), id(40), old, 100, id(74)).is_err());
}

fn round_offset(root: &Control) -> u64 {
    if root.controller.epoch > 1 { 20000 } else { 0 }
}
fn proof_sample(root: &Control, t: Instant, n: u64) -> OwnershipSample {
    sample(t, round_offset(root) + n)
}
#[tokio::test]
async fn takeover_adopts_then_requires_new_inventory_and_authenticated_round() {
    let (root, key, s, mut g, t) = setup_with(|r| {
        retained_idle(r);
        r.intents.push(retained_stop(r, id(40)));
    })
    .await;
    let (proof, _) = fresh_confirmed(&root, &key, &s, &mut g, t).await;
    let (c, p) = claim_stop(&root, owner(), id(40), proof, id(80), 100, id(72))
        .unwrap()
        .into_parts();
    let d = Instant::now() + Duration::from_secs(5);
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (_, old_ctx, old_proof) = p.confirm(out).unwrap().into_parts();
    let root = s.load_control(d).await.unwrap().root().clone();
    let old_inventory = inventory(&root, json!([]));
    let proposal = OwnershipProposal::new(
        &root,
        OwnershipChange::Acquire { process_id: id(92) },
        id(99),
        Ok(sample(t, 20000)),
    )
    .unwrap();
    let mut guard = OwnershipGuard::new(id(92));
    let stamp = guard.begin(&proposal).unwrap();
    let out = proposal
        .prepare(s.load_control(d).await.unwrap())
        .unwrap()
        .submit(&s, d)
        .await
        .unwrap();
    assert!(
        guard
            .finish(stamp, OwnershipEvidence::new(out), Ok(sample(t, 20001)))
            .unwrap()
    );
    let taken = s.load_control(d).await.unwrap().root().clone();
    let held = owner_for(&taken);
    assert!(reconcile_stop(&taken, held, id(40), old_inventory, 20102, id(73)).is_err());
    assert!(uncertain_stop(&taken, held, &old_ctx, 20102, id(73)).is_err());
    assert!(
        old_proof
            .check_for_stop(&taken, held, guard.authorization_generation())
            .is_err()
    );
    let pre_adoption = inventory(&taken, json!([]));
    let StopReconcilePlan::Change {
        candidate,
        classification,
    } = reconcile_stop(
        &taken,
        held,
        id(40),
        inventory(&taken, json!([])),
        20102,
        id(73),
    )
    .unwrap()
    else {
        panic!()
    };
    assert_eq!(classification, StopInventoryClass::Retryable);
    let mut expected = taken.clone();
    expected.revision += 1;
    expected.change_id = id(73);
    expected.intents[1].state = IntentState::Reconciling;
    assert_eq!(candidate.result, expected);
    let adopted = persist(&s, candidate).await;
    assert!(reconcile_stop(&adopted, held, id(40), pre_adoption, 20102, id(74)).is_err());
    assert!(matches!(
        retry_stop(
            &adopted,
            held,
            id(40),
            old_proof,
            inventory(&adopted, json!([])),
            id(81),
            20102,
            id(74)
        ),
        Err(StorageError::Conflict)
    ));
    let (proof, mut o) = fresh_confirmed(&adopted, &key, &s, &mut guard, t).await;
    let (c, p) = retry_stop(
        &adopted,
        held,
        id(40),
        proof,
        inventory(&adopted, json!([])),
        id(81),
        20102,
        id(74),
    )
    .unwrap()
    .into_parts();
    assert_eq!(c.result.intents[1].resource_id, root.intents[1].resource_id);
    assert_eq!(c.result.intents[1].launch_id, id(6));
    assert_eq!(c.result.intents[1].request_id, id(40));
    let out =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap()
            .submit(&s, d)
            .await
            .unwrap();
    let (_, ctx, proof) = p.confirm(out).unwrap().into_parts();
    let next = s.load_control(d).await.unwrap().root().clone();
    assert!(old_ctx.matches_delivery(&next, held, id(40)).is_err());
    ctx.matches_delivery(&next, held, id(40)).unwrap();
    let generation = guard.authorization_generation();
    guard
        .observe(&OwnershipObservation::new(&next).unwrap())
        .unwrap();
    let _proof = proof
        .check_live(&next, held, &mut o, &mut guard, proof_sample(&next, t, 9))
        .unwrap();
    guard
        .authorize(
            generation,
            &OwnershipObservation::new(&next).unwrap(),
            Ok(proof_sample(&next, t, 10)),
        )
        .unwrap();
}
#[tokio::test]
async fn stop_checked_failure_arithmetic_and_retry_backoffs_limits_preserve_base() {
    let (base, ctx, _) = delivered().await;
    for mode in 0..3 {
        let mut root = base.clone();
        let now = if mode == 1 { u64::MAX } else { 100 };
        if mode == 0 {
            root.scheduler.failures = u64::MAX;
        }
        if mode == 2 {
            root.revision = u64::MAX;
        }
        let root = Control::decode(&root.encode().unwrap()).unwrap();
        let saved = root.clone();
        assert!(matches!(
            uncertain_stop(&root, owner(), &ctx, now, id(73)),
            Err(StorageError::LimitExceeded)
        ));
        assert_eq!(root, saved);
    }
    for mode in 0..7 {
        let (root, proof, _, _, _, _) = retained_fixture(|r| {
            let i = &mut r.intents[1];
            i.state = IntentState::Reconciling;
            i.attempts = 1;
            i.error_code = Some("callback_uncertain".into());
            i.delivery = Some(crate::cluster::member::Delivery {
                invocation_id: id(80),
                owner_id: id(90),
                epoch: 1,
                command_sha256: "a".repeat(64),
            });
            match mode {
                0 => i.retry_at_ms = 101,
                1 => r.scheduler.backoff_until_ms = 101,
                2 => i.attempts = r.timing.max_attempts,
                3 => i.attempts = u64::MAX,
                4 => i.state = IntentState::InProgress,
                5 => {
                    i.state = IntentState::Failed;
                    i.outcome = Some(IntentOutcome::NotInvoked);
                }
                6 => {}
                _ => unreachable!(),
            }
        })
        .await;
        let saved = root.clone();
        let e = if mode == 6 {
            inventory(
                &root,
                json!([inventory_record(
                    &root,
                    root.nodes[0].resource_id.as_ref().unwrap(),
                    "terminated"
                )]),
            )
        } else {
            inventory(&root, json!([]))
        };
        let result = retry_stop(&root, owner(), id(40), proof, e, id(81), 100, id(74));
        if mode == 2 || mode == 3 {
            assert!(matches!(result, Err(StorageError::LimitExceeded)));
        } else {
            assert!(matches!(result, Err(StorageError::Conflict)));
        }
        assert_eq!(root, saved);
    }
}
#[tokio::test]
async fn gated_prepared_completion_loses_cas_to_real_takeover() {
    let (root, ctx, s) = delivered().await;
    let d = Instant::now() + Duration::from_secs(5);
    let c = uncertain_stop(&root, owner(), &ctx, 100, id(73)).unwrap();
    let prepared =
        crate::cluster::store::PreparedProvision::prepare(s.load_control(d).await.unwrap(), c)
            .unwrap();
    let s = std::sync::Arc::new(s);
    let submitted = s.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mut task = tokio::spawn(async move {
        let _ = entered_tx.send(());
        let _ = release_rx.await;
        prepared.submit(&submitted, d).await
    });
    let entered = tokio::time::timeout(Duration::from_secs(2), entered_rx).await;
    let changed = async {
        let p = OwnershipProposal::new(
            &root,
            OwnershipChange::Acquire { process_id: id(92) },
            id(99),
            Ok(sample(Instant::now(), 20000)),
        )?;
        p.prepare(s.load_control(d).await?)?.submit(&s, d).await
    }
    .await;
    let _ = release_tx.send(());
    let result = match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
        Ok(v) => Some(v),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    };
    let current = s
        .load_control(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(entered.unwrap().is_ok());
    assert!(matches!(changed, Ok(OwnershipOutcome::Acknowledged(_))));
    assert!(matches!(
        result.unwrap().unwrap().unwrap(),
        OwnershipOutcome::Conflict { .. }
    ));
    let current = current.unwrap();
    assert_eq!(current.root().intents, root.intents);
    assert!(
        uncertain_stop(
            current.root(),
            owner_for(current.root()),
            &ctx,
            20100,
            id(74)
        )
        .is_err()
    );
}
#[tokio::test]
async fn acceptance_retains_history_and_completion_rejects_changed_binding() {
    let (root, ctx, _) = delivered().await;
    let mut history = root.clone();
    history.intents[1].error_code = Some("callback_uncertain".into());
    history.intents[1].attempts = 2;
    history.intents[1].retry_at_ms = 99999;
    history.scheduler.failures = 4;
    history.scheduler.backoff_until_ms = 99998;
    let history = Control::decode(&history.encode().unwrap()).unwrap();
    let reply = crate::cluster::callback::decode_reply(
        200,
        &serde_json::to_vec(&json!({"version":1,"invocation_id":id(80),"result":null})).unwrap(),
        &ctx,
    )
    .unwrap();
    let mut expected = history.clone();
    expected.revision += 1;
    expected.change_id = id(73);
    expected.intents[1].state = IntentState::Reconciling;
    assert_eq!(
        complete_stop(&history, owner(), &ctx, reply, 100, id(73))
            .unwrap()
            .result,
        expected
    );
    for mode in 0..3 {
        let mut other = root.clone();
        match mode {
            0 => {
                other.nodes[0].resource_id = Some("different".into());
                other.intents[0].resource_id = Some("different".into());
                other.intents[1].resource_id = Some("different".into());
            }
            1 => {
                other.nodes[0].incarnation = Some(id(44));
                other.intents[1].incarnation_id = Some(id(44));
            }
            2 => other.intents[1].delivery.as_mut().unwrap().command_sha256 = "f".repeat(64),
            _ => unreachable!(),
        }
        let other = Control::decode(&other.encode().unwrap()).unwrap();
        assert!(uncertain_stop(&other, owner(), &ctx, 100, id(73)).is_err());
    }
}
#[tokio::test]
async fn collision_quarantines_while_unrelated_recoverable_launch_does_not_block_stop() {
    let (root, ctx, _) = delivered().await;
    let root = uncertain_stop(&root, owner(), &ctx, 100, id(73))
        .unwrap()
        .result;
    let mut other = root.clone();
    let mut node = other.nodes[0].clone();
    node.node_id = id(34);
    node.launch_id = Some(id(7));
    node.incarnation = None;
    node.endpoint = None;
    node.session = None;
    node.resource_id = None;
    node.state = NodeState::Starting;
    node.registration = crate::cluster::member::Registration::Unregistered;
    node.drain = None;
    let mut start = other.intents[0].clone();
    start.request_id = id(7);
    start.launch_id = id(7);
    start.node_id = id(34);
    start.resource_id = None;
    start.state = IntentState::Reconciling;
    start.outcome = None;
    start.delivery = Some(crate::cluster::member::Delivery {
        invocation_id: id(82),
        owner_id: id(90),
        epoch: 1,
        command_sha256: "a".repeat(64),
    });
    other.nodes.push(node);
    other.intents.push(start);
    other.intents.sort_by_key(|i| i.request_id);
    let other = Control::decode(&other.encode().unwrap()).unwrap();
    let unrelated = json!({"cluster_id":other.cluster_id,"node_id":id(34),"request_id":id(7),"resource_id":"recoverable","state":"running"});
    assert!(matches!(
        reconcile_stop(
            &other,
            owner(),
            id(40),
            inventory(&other, json!([unrelated.clone()])),
            100,
            id(74)
        )
        .unwrap(),
        StopReconcilePlan::Unchanged(StopInventoryClass::Retryable)
    ));
    let mut terminated = unrelated.clone();
    terminated["state"] = json!("terminated");
    let StopReconcilePlan::Change { classification, .. } = reconcile_stop(
        &other,
        owner(),
        id(40),
        inventory(&other, json!([terminated])),
        100,
        id(74),
    )
    .unwrap() else {
        panic!("unbound unrelated termination must quarantine")
    };
    assert_eq!(classification, StopInventoryClass::Quarantined);
    let mut collision = unrelated;
    collision["resource_id"] = json!(other.nodes[0].resource_id.as_ref().unwrap());
    let StopReconcilePlan::Change {
        candidate,
        classification,
    } = reconcile_stop(
        &other,
        owner(),
        id(40),
        inventory(&other, json!([collision])),
        100,
        id(74),
    )
    .unwrap()
    else {
        panic!()
    };
    assert_eq!(classification, StopInventoryClass::Quarantined);
    assert_eq!(candidate.result.nodes, other.nodes);
    assert_eq!(
        candidate.result.capacity().unwrap(),
        other.capacity().unwrap()
    );
}
#[test]
fn new_stop_plans_are_not_clone_or_serde_and_debug_is_redacted() {
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
    macro_rules! opaque{($($t:ty),*)=>{$(let _=<$t as NotClone<_>>::marker;let _=<$t as NotSerialize<_>>::marker;let _=<$t as NotDeserialize<_>>::marker;)*}}
    opaque!(StopReconcilePlan, StopInventoryClass);
    assert_eq!(
        format!(
            "{:?}",
            StopReconcilePlan::Unchanged(StopInventoryClass::NeedsTermination)
        ),
        "StopReconcilePlan { .. }"
    );
}
