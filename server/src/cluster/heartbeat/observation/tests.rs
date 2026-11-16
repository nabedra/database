use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::*,
    state::*,
};
use serde_json::{Value, json};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn root(external: bool) -> Control {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic-token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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

fn setup() -> (Control, HeartbeatBinding) {
    let mut r = root(true);
    let b = HeartbeatBinding {
        resources: None,
        cluster_id: r.cluster_id,
        node_id: id(3),
        incarnation: id(4),
        session_id: id(5),
        verifier: "ab".repeat(32),
        operation_slots: 4,
        partition_slots: 4,
        queue_size: 64,
    };
    r.nodes.push(Node {
        resources: None,
        node_id: b.node_id,
        launch_id: None,
        resource_id: Some(format!("external:{}", b.node_id)),
        ownership: Ownership::External,
        state: NodeState::Ready,
        incarnation: Some(b.incarnation),
        retired_incarnations: vec![],
        endpoint: Some("http://worker:8000".into()),
        operation_slots: 4,
        partition_slots: 4,
        queue_size: 64,
        registration: Registration::Verified,
        session: Some(Session {
            id: b.session_id,
            verifier: b.verifier.clone(),
        }),
        drain: None,
    });
    let r = Control::decode(&r.encode().unwrap()).unwrap();
    (r, b)
}
fn beat(b: &HeartbeatBinding, seq: u64) -> Heartbeat {
    b.record(
        &HeartbeatSample {
            available_threads: None,
            available_memory_bytes: None,
            running_operations: 0,
            running_partitions: 0,
            queued_operations: 0,
            queued_partitions: 0,
            drain_id: None,
            accepting: true,
        },
        seq,
    )
}
#[test]
fn repeats_expire_at_original_advance() {
    let (r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 1), t).unwrap();
    o.observe(&b, &beat(&b, 2), t).unwrap();
    assert!(o.fresh(&b, t, e).unwrap());
    o.observe(&b, &beat(&b, 2), t + e - Duration::from_nanos(1))
        .unwrap();
    assert!(!o.fresh(&b, t + e, e).unwrap());
    assert!(o.expiry_input(&b, t + e, e).unwrap().is_some());
}
#[test]
fn failures_keep_original_loss_anchors_and_require_advance() {
    let (r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 1), t).unwrap();
    o.observe(&b, &beat(&b, 2), t).unwrap();
    o.read_failed(&b, t + e / 2).unwrap();
    assert!(!o.fresh(&b, t + e / 2, e).unwrap());
    o.observe(&b, &beat(&b, 2), t + e / 2).unwrap();
    assert!(!o.fresh(&b, t + e / 2, e).unwrap());
    o.read_failed(&b, t + e).unwrap();
    let captured = o.expiry_input(&b, t + e, e).unwrap().unwrap();
    assert!(o.matches_expiry(&b, &captured, t + e, e).unwrap());
    o.observe(&b, &beat(&b, 3), t + e).unwrap();
    assert!(!o.matches_expiry(&b, &captured, t + e, e).unwrap());
    assert!(o.fresh(&b, t + e, e).unwrap());
    assert!(o.expiry_input(&b, t + e, e).unwrap().is_none());
}
#[test]
fn both_query_time_regressions_persist_and_keep_high_water() {
    for via_fresh in [false, true] {
        let (r, b) = setup();
        let t = Instant::now();
        let e = Duration::from_millis(r.timing.expiry_ms);
        let mut o = Observations::new(id(90), t);
        o.reconcile_members(&r, t).unwrap();
        o.observe(&b, &beat(&b, 8), t).unwrap();
        o.observe(&b, &beat(&b, 9), t + Duration::from_secs(1))
            .unwrap();
        if via_fresh {
            assert!(o.fresh(&b, t, e).is_err());
        } else {
            assert!(o.expiry_input(&b, t, e).is_err());
        }
        assert!(!o.fresh(&b, t + e, e).unwrap());
        assert!(o.expiry_input(&b, t + e, e).unwrap().is_none());
        assert!(o.observe(&b, &beat(&b, 8), t + e).is_err());
        o.observe(&b, &beat(&b, 9), t + e).unwrap();
        assert!(!o.fresh(&b, t + e, e).unwrap());
        o.observe(&b, &beat(&b, 10), t + e).unwrap();
        assert!(o.fresh(&b, t + e, e).unwrap());
    }
}
#[test]
fn root_order_and_integrity_preserve_accepted_binding() {
    let (r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 4), t).unwrap();
    o.observe(&b, &beat(&b, 5), t).unwrap();
    let mut r2 = r.clone();
    r2.revision += 1;
    r2.change_id = id(99);
    o.reconcile_members(&r2, t + e / 2).unwrap();
    assert!(o.reconcile_members(&r, t).is_err());
    o.reconcile_members(&r2, t + e / 2).unwrap();
    assert!(o.expiry_input(&b, t + e, e).unwrap().is_some());
    let mut corrupt = r2.clone();
    corrupt.name = "Changed".into();
    assert!(matches!(
        o.reconcile_members(&corrupt, t + e),
        Err(StorageError::InvalidCatalog)
    ));
    o.reconcile_members(&r2, t + e).unwrap();
    o.observe(&b, &beat(&b, 5), t + e).unwrap();
    assert!(!o.fresh(&b, t + e, e).unwrap());
    assert!(o.observe(&b, &beat(&b, 4), t + e).is_err());
}
#[test]
fn binding_and_capacity_controls_do_not_restore_freshness() {
    let (r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_secs(15);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 2), t).unwrap();
    o.observe(&b, &beat(&b, 3), t).unwrap();
    for k in 0..6 {
        let mut bad = b.clone();
        match k {
            0 => bad.incarnation = id(17),
            1 => bad.session_id = id(17),
            2 => bad.verifier = "cd".repeat(32),
            3 => bad.operation_slots += 1,
            4 => bad.cluster_id = id(17),
            _ => bad.node_id = id(17),
        }
        assert!(o.observe(&bad, &beat(&bad, 4), t).is_err());
    }
    let mut h = beat(&b, 4);
    h.partition_slots += 1;
    assert!(o.observe(&b, &h, t).is_err());
    o.observe(&b, &beat(&b, 3), t).unwrap();
    assert!(!o.fresh(&b, t, e).unwrap());
}
fn next(root: &mut Control) {
    root.revision += 1;
    root.change_id = id(1000 + u128::from(root.revision));
    root.validate().unwrap();
}
fn draining(root: &mut Control) {
    root.nodes[0].state = NodeState::Draining;
    root.nodes[0].drain = Some(Drain {
        id: id(77),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    next(root);
}
#[test]
fn takeover_restart_baseline_and_full_grace() {
    let (mut r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 100), t).unwrap();
    assert!(!o.fresh(&b, t, e).unwrap());
    o.observe(&b, &beat(&b, 101), t).unwrap();
    r.controller.epoch += 1;
    r.controller.owner_id = Some(id(91));
    next(&mut r);
    o.reconcile_members(&r, t + e / 2).unwrap();
    o.observe(&b, &beat(&b, 101), t + e / 2).unwrap();
    assert!(!o.fresh(&b, t + e / 2, e).unwrap());
    assert!(o.expiry_input(&b, t + e, e).unwrap().is_none());
    assert!(o.expiry_input(&b, t + e + e / 2, e).unwrap().is_some());
    o.observe(&b, &beat(&b, 102), t + e + e / 2).unwrap();
    assert!(o.fresh(&b, t + e + e / 2, e).unwrap());
    let mut restarted = Observations::new(id(91), t + e);
    restarted.reconcile_members(&r, t + e).unwrap();
    restarted.observe(&b, &beat(&b, 102), t + e).unwrap();
    assert!(!restarted.fresh(&b, t + e, e).unwrap());
    restarted.observe(&b, &beat(&b, 103), t + e).unwrap();
    assert!(restarted.fresh(&b, t + e, e).unwrap());
}
#[test]
fn overflow_and_generation_exhaustion_close_evidence() {
    let (r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_secs(15);
    for via_fresh in [false, true] {
        let mut o = Observations::new(id(90), t);
        o.reconcile_members(&r, t).unwrap();
        o.observe(&b, &beat(&b, 1), t).unwrap();
        o.observe(&b, &beat(&b, 2), t).unwrap();
        if via_fresh {
            assert!(matches!(
                o.fresh(&b, t, Duration::MAX),
                Err(StorageError::LimitExceeded)
            ));
        } else {
            assert!(o.expiry_input(&b, t, Duration::MAX).is_err());
        }
        assert!(!o.fresh(&b, t, e).unwrap());
        o.observe(&b, &beat(&b, 2), t).unwrap();
        assert!(!o.fresh(&b, t, e).unwrap());
        o.generation = u64::MAX;
        assert!(o.read_failed(&b, t).is_err());
        assert!(o.observe(&b, &beat(&b, 3), t).is_err());
        assert!(o.fresh(&b, t, e).is_err());
        assert!(o.expiry_input(&b, t + e, e).is_err());
        assert_eq!(o.entries[0].sequence, Some(2));
    }
}
#[test]
fn replacement_pruning_bounds_and_delayed_old_root() {
    let (mut r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_secs(15);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    let old = r.clone();
    r.nodes[0].retired_incarnations.push(b.incarnation);
    r.nodes[0].incarnation = Some(id(45));
    r.nodes[0].session.as_mut().unwrap().id = id(46);
    next(&mut r);
    let replacement = binding(&r, &r.nodes[0]).unwrap();
    o.reconcile_members(&r, t).unwrap();
    o.observe(&replacement, &beat(&replacement, 10), t).unwrap();
    o.observe(&replacement, &beat(&replacement, 11), t).unwrap();
    assert!(o.reconcile_members(&old, t).is_err());
    o.reconcile_members(&r, t).unwrap();
    o.observe(&replacement, &beat(&replacement, 11), t + e)
        .unwrap();
    assert!(!o.fresh(&replacement, t + e, e).unwrap());
    assert!(o.observe(&b, &beat(&b, 99), t + e).is_err());
    let base = r.nodes[0].clone();
    r.nodes.clear();
    for k in 0..MAX_NODES {
        let mut n = base.clone();
        n.node_id = id(10000 + k as u128);
        n.resource_id = Some(format!("external:{}", n.node_id));
        n.incarnation = Some(id(20000 + k as u128));
        n.retired_incarnations.clear();
        n.session.as_mut().unwrap().id = id(30000 + k as u128);
        r.nodes.push(n);
    }
    next(&mut r);
    o.reconcile_members(&r, t + e).unwrap();
    assert_eq!(o.entries.len(), MAX_NODES);
    r.nodes.clear();
    next(&mut r);
    o.reconcile_members(&r, t + e).unwrap();
    assert!(o.entries.is_empty());
}
#[test]
fn loss_candidate_preserves_root_and_draining_never_expires() {
    use crate::cluster::{controller::Owner, membership::loss_candidate};
    let (mut r, b) = setup();
    // Add a retained completed managed launch so loss preservation covers real intent/credit state.
    r.provisioning = Provisioning::Managed;
    r.config.adapter_contract = Some("meshdb.callbacks.v1".into());
    r.nodes[0].ownership = Ownership::Managed;
    r.nodes[0].launch_id = Some(id(6));
    r.nodes[0].resource_id = Some("synthetic-resource".into());
    r.intents.push(serde_json::from_value(json!({"request_id":id(6),"node_id":b.node_id,"launch_id":id(6),"incarnation_id":null,"resource_id":"synthetic-resource","action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":1,"retry_at_ms":0,"error_code":null,"template_fingerprint":r.config.template_fingerprint,"delivery":null,"outcome":"resource_known"})).unwrap());
    r.validate().unwrap();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    let captured = o.expiry_input(&b, t + e, e).unwrap().unwrap();
    assert!(o.matches_expiry(&b, &captured, t + e, e).unwrap());
    let (_, _, mut lost) = loss_candidate(
        &r,
        Owner {
            process_id: id(90),
            epoch: 7,
        },
        &captured,
        id(101),
    )
    .unwrap()
    .into_parts();
    assert_eq!(lost.nodes[0].state, NodeState::Lost);
    assert_eq!(
        r.capacity().unwrap().credits,
        lost.capacity().unwrap().credits
    );
    lost.nodes = r.nodes.clone();
    lost.revision = r.revision;
    lost.change_id = r.change_id;
    assert_eq!(lost, r);
    draining(&mut r);
    o.reconcile_members(&r, t + e).unwrap();
    assert!(o.expiry_input(&b, t + e + e, e).unwrap().is_none());
    assert!(
        loss_candidate(
            &r,
            Owner {
                process_id: id(90),
                epoch: 7
            },
            &captured,
            id(102)
        )
        .is_err()
    );
    assert_eq!(r.nodes[0].state, NodeState::Draining);
}
async fn loaded(
    seq: u64,
    b: &HeartbeatBinding,
    drain: Option<Uuid>,
    accepting: bool,
) -> LoadedHeartbeat {
    use crate::{storage::Namespace, test_storage::Fake};
    let storage = Storage::new(Fake::default(), Namespace::new("synthetic").unwrap());
    let mut h = beat(b, seq);
    h.drain_id = drain;
    h.accepting = accepting;
    storage
        .put(
            &ObjectKey::heartbeat(b.incarnation),
            h.encode().unwrap(),
            WriteCondition::Create,
        )
        .await
        .unwrap();
    super::super::load_heartbeat(&storage, b, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn drain_receipt_requires_progress_and_current_configured_evidence() {
    let (mut r, b) = setup();
    draining(&mut r);
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    let baseline = loaded(100, &b, Some(id(77)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &baseline, t, e).is_err());
    let advanced = loaded(101, &b, Some(id(77)), false).await;
    let receipt = o.drain_heartbeat(&r, &b, &advanced, t, e).unwrap();
    assert_eq!(receipt.binding(), &b);
    assert_eq!(receipt.sampled_at(), t);
    assert!(o.matches_drain(&receipt, t).unwrap());
    assert_eq!(receipt.heartbeat().sequence, 101);
    assert_eq!(receipt.member(), &r.nodes[0]);
    assert_eq!(receipt.controller_boot(), id(90));
    assert!(receipt.generation() > 0);
    assert!(receipt.observation() == &OwnershipObservation::new(&r).unwrap());
    assert_eq!(format!("{receipt:?}"), "DrainHeartbeat");
    assert_eq!(format!("{o:?}"), "Observations");
    assert!(o.drain_heartbeat(&r, &b, &advanced, t + e, e).is_err());
    o.read_failed(&b, t + e).unwrap();
    assert!(!o.matches_drain(&receipt, t + e).unwrap());
    assert!(o.drain_heartbeat(&r, &b, &advanced, t + e, e).is_err());
    let newer = loaded(102, &b, Some(id(77)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e).is_ok());
    assert!(o.drain_heartbeat(&r, &b, &baseline, t + e, e).is_err());
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e).is_err());
    let newer = loaded(103, &b, Some(id(77)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e + e).is_err());
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e).is_ok());
    let mut wrong_boot = Observations::new(id(91), t);
    assert!(wrong_boot.drain_heartbeat(&r, &b, &newer, t, e).is_err());
    let mut wrong_root = r.clone();
    wrong_root.name = "Changed".into();
    assert!(
        o.drain_heartbeat(&wrong_root, &b, &newer, t + e, e)
            .is_err()
    );
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e).is_err());
    r.controller.epoch += 1;
    next(&mut r);
    assert!(o.drain_heartbeat(&r, &b, &newer, t + e, e).is_err());
    let post = loaded(104, &b, Some(id(77)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &post, t + e, e).is_ok());
    let wrong_drain = loaded(105, &b, Some(id(78)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &wrong_drain, t + e, e).is_err());
    let wrong_binding = HeartbeatBinding {
        resources: None,
        session_id: id(111),
        ..b.clone()
    };
    let wrong = loaded(106, &wrong_binding, Some(id(77)), false).await;
    assert!(o.drain_heartbeat(&r, &b, &wrong, t + e, e).is_err());
}
#[test]
fn receipt_traits_are_closed() {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <DrainHeartbeat as AmbiguousIfClone<_>>::marker;
    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: serde::Serialize> AmbiguousIfSerialize<u8> for T {}
    let _ = <DrainHeartbeat as AmbiguousIfSerialize<_>>::marker;
}

#[tokio::test]
async fn drain_receipt_requires_present_durable_drain_identity() {
    let (mut root, binding) = setup();
    draining(&mut root);
    let now = Instant::now();
    let expiry = Duration::from_millis(root.timing.expiry_ms);
    for drain_id in [Some(id(77)), None] {
        let mut candidate = root.clone();
        if drain_id.is_none() {
            candidate.nodes[0].drain = None;
        }
        // Both roots and both matching heartbeat sequences are codec-valid.
        let candidate = Control::decode(&candidate.encode().unwrap()).unwrap();
        let baseline = loaded(100, &binding, drain_id, false).await;
        let advanced = loaded(101, &binding, drain_id, false).await;
        let mut observations = Observations::new(id(90), now);
        observations.reconcile_members(&candidate, now).unwrap();
        observations
            .observe(&binding, baseline.heartbeat(), now)
            .unwrap();
        observations
            .observe(&binding, advanced.heartbeat(), now)
            .unwrap();
        assert!(observations.fresh(&binding, now, expiry).unwrap());
        let receipt = observations.drain_heartbeat(&candidate, &binding, &advanced, now, expiry);
        if let Some(id) = drain_id {
            let receipt = receipt.unwrap();
            assert_eq!(receipt.member().drain.as_ref().unwrap().id, id);
            assert_eq!(receipt.heartbeat().drain_id, Some(id));
        } else {
            assert!(matches!(receipt, Err(StorageError::Conflict)));
        }
    }
}
#[test]
fn takeover_grace_does_not_manufacture_sequence_advancement_time() {
    let (mut r, b) = setup();
    let t = Instant::now();
    let e = Duration::from_millis(r.timing.expiry_ms);
    let mut o = Observations::new(id(90), t);
    o.reconcile_members(&r, t).unwrap();
    o.observe(&b, &beat(&b, 1), t).unwrap();
    o.observe(&b, &beat(&b, 2), t).unwrap();
    r.controller.epoch += 1;
    next(&mut r);
    o.reconcile_members(&r, t + e).unwrap();
    let captured = o.expiry_input(&b, t + e + e, e).unwrap().unwrap();
    assert_eq!(captured.grace_anchor, t + e);
    assert_eq!(captured.last_advance, Some(t));
}

fn auto_sample(t: Instant, ms: u64) -> Result<OwnershipSample, StorageError> {
    OwnershipSample::from_system_time(t, std::time::UNIX_EPOCH + Duration::from_millis(ms))
}
fn auto_owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
fn auto_root() -> (Control, HeartbeatBinding) {
    let (mut r, b) = setup();
    r.provisioning = Provisioning::Managed;
    r.config.adapter_contract = Some("meshdb.callbacks.v1".into());
    r.policy = Policy {
        revision: 9,
        mode: PolicyMode::Autoscale,
        desired: 1,
        min_nodes: 1,
        max_nodes: 3,
    };
    r.nodes[0].ownership = Ownership::Managed;
    r.nodes[0].resource_id = Some("synthetic-resource".into());
    r.nodes[0].launch_id = Some(id(40));
    r.intents.push(Intent::decode(&serde_json::to_vec(&json!({"request_id":id(40),"node_id":b.node_id,"launch_id":id(40),"incarnation_id":null,"resource_id":"synthetic-resource","action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":r.config.template_fingerprint,"delivery":null,"outcome":"resource_known"})).unwrap()).unwrap());
    (Control::decode(&r.encode().unwrap()).unwrap(), b)
}
#[tokio::test]
async fn autoscale_loaded_progress_yields_snapshot() {
    let (r, b) = auto_root();
    let t = Instant::now();
    let mut o = Observations::new(id(90), t);
    o.autoscale_observe(&r, loaded(1, &b, None, true).await, auto_sample(t, 1000))
        .unwrap();
    o.autoscale_observe(&r, loaded(2, &b, None, true).await, auto_sample(t, 1000))
        .unwrap();
    o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
        .unwrap();
}

async fn auto_ingest(
    o: &mut Observations,
    r: &Control,
    b: &HeartbeatBinding,
    h: Heartbeat,
    t: Instant,
    ms: u64,
) -> Result<(), StorageError> {
    use crate::{storage::Namespace, test_storage::Fake};
    let storage = Storage::new(Fake::default(), Namespace::new("synthetic").unwrap());
    storage
        .put(
            &ObjectKey::heartbeat(b.incarnation),
            h.encode().unwrap(),
            WriteCondition::Create,
        )
        .await
        .unwrap();
    let h = load_heartbeat(&storage, b, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    o.autoscale_observe(r, h, auto_sample(t, ms))
}
async fn auto_healthy(t: Instant) -> (Observations, Control, HeartbeatBinding) {
    let (r, b) = auto_root();
    let mut o = Observations::new(id(90), t);
    auto_ingest(&mut o, &r, &b, beat(&b, 1), t, 1000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b, beat(&b, 2), t, 1000)
        .await
        .unwrap();
    assert_eq!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .unwrap()
            .monotonic(),
        t
    );
    (o, r, b)
}
#[tokio::test]
async fn autoscale_busy_between_polls_resets_each_load_dimension() {
    for dimension in 0..5 {
        let t = Instant::now();
        let (mut o, r, b) = auto_healthy(t).await;
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        let mut h = beat(&b, 3);
        match dimension {
            0 => h.running_operations = 1,
            1 => h.running_partitions = 1,
            2 => h.queued_operations = 1,
            3 => h.queued_partitions = 1,
            _ => h.accepting = false,
        }
        auto_ingest(&mut o, &r, &b, h, t + Duration::from_secs(1), 2000)
            .await
            .unwrap();
        auto_ingest(
            &mut o,
            &r,
            &b,
            beat(&b, 5),
            t + Duration::from_secs(2),
            3000,
        )
        .await
        .unwrap();
        let new = o
            .autoscale_snapshot(
                &r,
                auto_owner(),
                auto_sample(t + Duration::from_secs(2), 3000),
            )
            .unwrap();
        assert_eq!(
            new.low_anchor().unwrap().monotonic(),
            t + Duration::from_secs(2)
        );
        assert!(
            !o.matches_autoscale(&old, auto_sample(t + Duration::from_secs(2), 3000))
                .unwrap()
        );
    }
}
#[tokio::test]
async fn autoscale_duplicates_preserve_but_cannot_refresh_or_restore() {
    let t = Instant::now();
    let (mut o, r, b) = auto_healthy(t).await;
    let expiry = Duration::from_millis(r.timing.expiry_ms);
    for ms in [1000, 2000, 5000, 14000] {
        auto_ingest(
            &mut o,
            &r,
            &b,
            beat(&b, 2),
            t + Duration::from_millis(ms),
            1000 + ms,
        )
        .await
        .unwrap();
        assert_eq!(
            o.autoscale_snapshot(
                &r,
                auto_owner(),
                auto_sample(t + Duration::from_millis(ms), 1000 + ms)
            )
            .unwrap()
            .low_anchor()
            .unwrap()
            .monotonic(),
            t
        );
    }
    auto_ingest(&mut o, &r, &b, beat(&b, 2), t + expiry, 16000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + expiry, 16000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b, beat(&b, 4), t + expiry, 16000)
        .await
        .unwrap();
    assert_eq!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + expiry, 16000))
            .unwrap()
            .low_anchor()
            .unwrap()
            .monotonic(),
        t + expiry
    );
    o.autoscale_failed(Err(StorageError::Unavailable))
        .unwrap_err();
    auto_ingest(&mut o, &r, &b, beat(&b, 4), t + expiry, 16000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + expiry, 16000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
}
#[tokio::test]
async fn autoscale_missed_deadline_checked_before_advancing_load() {
    for nanos in [0, 1] {
        let t = Instant::now();
        let (mut o, r, b) = auto_healthy(t).await;
        let d = Duration::from_millis(r.timing.expiry_ms);
        let now = t + d - Duration::from_nanos(nanos);
        auto_ingest(&mut o, &r, &b, beat(&b, 3), now, 1000 + r.timing.expiry_ms)
            .await
            .unwrap();
        let s = o
            .autoscale_snapshot(
                &r,
                auto_owner(),
                auto_sample(now, 1000 + r.timing.expiry_ms),
            )
            .unwrap();
        assert_eq!(
            s.low_anchor().unwrap().monotonic(),
            if nanos == 0 { now } else { t }
        );
    }
}
#[tokio::test]
async fn autoscale_rejections_close_old_snapshots_and_require_new_progress() {
    for case in 0..7 {
        let t = Instant::now();
        let (mut o, mut r, b) = auto_healthy(t).await;
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        match case {
            0 => {
                o.read_failed(&b, t).unwrap();
            }
            1 => {
                assert!(
                    auto_ingest(&mut o, &r, &b, beat(&b, 1), t, 1000)
                        .await
                        .is_err()
                );
            }
            2 => {
                let mut h = beat(&b, 2);
                h.queued_operations = 1;
                assert!(auto_ingest(&mut o, &r, &b, h, t, 1000).await.is_err());
            }
            3 => {
                let mut stale = r.clone();
                stale.revision -= 1;
                assert!(o.reconcile_members(&stale, t).is_err());
                assert!(o.observation.as_ref() == Some(old.observation()));
            }
            4 => {
                let mut bad = r.clone();
                bad.scheduler.failures += 1;
                assert!(o.reconcile_members(&bad, t).is_err());
            }
            5 => {
                let mut bad = r.clone();
                bad.nodes[0].operation_slots = 0;
                assert!(o.reconcile_members(&bad, t).is_err());
            }
            _ => {
                o.observe(&b, &beat(&b, 2), t).unwrap();
            }
        }
        assert!(!o.matches_autoscale(&old, auto_sample(t, 1000)).unwrap());
        auto_ingest(&mut o, &r, &b, beat(&b, 2), t, 1000)
            .await
            .unwrap();
        assert!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
                .unwrap()
                .low_anchor()
                .is_none()
        );
        next(&mut r);
        auto_ingest(&mut o, &r, &b, beat(&b, 3), t, 1000)
            .await
            .unwrap();
        assert!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
                .unwrap()
                .low_anchor()
                .is_some()
        );
    }
}
#[tokio::test]
async fn autoscale_identity_and_housekeeping_are_distinct() {
    for case in 0..7 {
        let t = Instant::now();
        let (mut o, mut r, b) = auto_healthy(t).await;
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        match case {
            0 => r.scheduler.low_since_ms = Some(999),
            1 => {
                r.scheduler.failures += 1;
                r.scheduler.backoff_until_ms += 1;
            }
            2 => r.intents[0].retry_at_ms += 1,
            3 => r.policy.revision += 1,
            4 => r.admin_seq += 1,
            5 => r.nodes[0].state = NodeState::Lost,
            _ => r.controller.epoch += 1,
        }
        next(&mut r);
        o.reconcile_members(&r, t).unwrap();
        assert!(!o.matches_autoscale(&old, auto_sample(t, 1000)).unwrap());
        let owner = Owner {
            epoch: r.controller.epoch,
            ..auto_owner()
        };
        let s = o
            .autoscale_snapshot(&r, owner, auto_sample(t, 1000))
            .unwrap();
        assert_eq!(s.low_anchor().is_some(), case < 3);
        assert_eq!(format!("{s:?}"), "AutoscaleSnapshot");
        if case < 3 {
            assert_eq!(s.low_anchor().unwrap().unix_ms(), 1000);
        }
        assert_eq!(s.members().len(), 1);
        assert_eq!(s.members()[0].member(), &r.nodes[0]);
        if case < 3 {
            assert!(s.members()[0].binding() == Some(&b));
            assert_eq!(s.members()[0].heartbeat().unwrap().sequence, 2);
        }
    }
}
#[tokio::test]
async fn autoscale_time_drift_and_exhaustion_fail_closed() {
    for offset in [1999, 2000, 2001, -1999, -2000, -2001] {
        let t = Instant::now();
        let (mut o, r, _) = auto_healthy(t).await;
        let result = o.autoscale_snapshot(
            &r,
            auto_owner(),
            auto_sample(t + Duration::from_secs(5), (6000_i64 + offset) as u64),
        );
        assert_eq!(result.is_ok(), offset.abs() <= 2000);
    }
    let t = Instant::now();
    let (mut o, r, _) = auto_healthy(t).await;
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 999))
            .is_err()
    );
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    o.autoscale.serial = u64::MAX;
    assert!(matches!(
        o.autoscale_failed(auto_sample(t, 1000)),
        Err(StorageError::LimitExceeded)
    ));
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .is_err()
    );
}
#[tokio::test]
async fn autoscale_restart_never_inherits_persisted_diagnostic() {
    let (mut r, b) = auto_root();
    r.scheduler.low_since_ms = Some(1);
    let t = Instant::now();
    let mut o = Observations::new(id(90), t);
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b, beat(&b, 100), t, 1000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .members()[0]
            .heartbeat()
            .is_none()
    );
}

#[tokio::test]
async fn autoscale_real_guard_renewals_preserve_original_sixty_second_anchor() {
    use crate::cluster::controller::{
        OwnershipChange, OwnershipEvidence, OwnershipGuard, OwnershipProposal,
    };
    use crate::{storage::Namespace, test_storage::Fake};
    let t = Instant::now();
    let (mut r, b) = auto_root();
    r.controller = Controller {
        owner_id: None,
        epoch: 6,
        lease_state: LeaseState::Released,
        lease_until_ms: None,
    };
    let storage = Storage::new(Fake::default(), Namespace::new("synthetic").unwrap());
    storage
        .put(
            &ObjectKey::control(),
            r.encode().unwrap(),
            WriteCondition::Create,
        )
        .await
        .unwrap();
    let mut guard = OwnershipGuard::new(id(90));
    let mut o = Observations::new(id(90), t);
    for k in 0..=12_u64 {
        let now = t + Duration::from_secs(k * 5);
        let ms = 1000 + k * 5000;
        if k > 0 {
            assert_eq!(guard.check(auto_sample(now, ms)).unwrap(), auto_owner());
        }
        let base = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let change = if k == 0 {
            OwnershipChange::Acquire { process_id: id(90) }
        } else {
            OwnershipChange::Renew(auto_owner())
        };
        let proposal = OwnershipProposal::new(
            base.root(),
            change,
            id(500 + u128::from(k)),
            auto_sample(now, ms),
        )
        .unwrap();
        let stamp = guard.begin(&proposal).unwrap();
        let pending_generation = guard.authorization_generation();
        assert!(matches!(
            guard.check(auto_sample(now, ms)),
            Err(StorageError::Unavailable)
        ));
        assert_eq!(guard.authorization_generation(), pending_generation);
        // The caller makes no eligible snapshot/match/submission request while
        // this real guard operation is pending. It continues heartbeat reads.
        if k > 0 {
            auto_ingest(&mut o, &r, &b, beat(&b, k * 2 + 1), now, ms)
                .await
                .unwrap();
            assert_eq!(o.autoscale.low.unwrap().monotonic(), t);
        }
        let result = proposal
            .prepare(base)
            .unwrap()
            .submit(&storage, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            guard
                .finish(stamp, OwnershipEvidence::new(result), auto_sample(now, ms))
                .unwrap()
        );
        assert_eq!(guard.check(auto_sample(now, ms)).unwrap(), auto_owner());
        r = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .root()
            .clone();
        o.reconcile_members(&r, now).unwrap();
        if k > 0 {
            assert!(
                o.autoscale_snapshot(&r, auto_owner(), auto_sample(now, ms))
                    .unwrap()
                    .low_anchor()
                    .is_none()
            );
        }
        auto_ingest(&mut o, &r, &b, beat(&b, k * 2 + 2), now, ms)
            .await
            .unwrap();
        if k == 0 {
            auto_ingest(&mut o, &r, &b, beat(&b, 3), now, ms)
                .await
                .unwrap();
        }
        let snapshot = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(now, ms))
            .unwrap();
        assert_eq!(snapshot.low_anchor().unwrap().monotonic(), t);
        assert!(
            o.matches_autoscale(&snapshot, auto_sample(now, ms))
                .unwrap()
        );
    }
    assert_eq!(o.autoscale.low.unwrap().unix_ms(), 1000);
}
#[tokio::test]
async fn autoscale_missing_corrupt_and_failed_load_notifications() {
    use crate::{storage::Namespace, test_storage::Fake};
    for case in 0..4 {
        let t = Instant::now();
        let (mut o, r, b) = auto_healthy(t).await;
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        let storage = Storage::new(Fake::default(), Namespace::new("synthetic").unwrap());
        if case == 1 {
            storage
                .put(
                    &ObjectKey::heartbeat(b.incarnation),
                    b"corrupt".to_vec(),
                    WriteCondition::Create,
                )
                .await
                .unwrap();
        }
        if case == 3 {
            storage.backend().state.lock().unwrap().read_failure = true;
        }
        let deadline = if case == 2 {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(5)
        };
        let result = load_heartbeat(&storage, &b, deadline).await;
        if case == 0 {
            assert!(result.unwrap().is_none());
        } else {
            assert!(result.is_err());
        }
        o.read_failed(&b, t).unwrap();
        assert!(!o.matches_autoscale(&old, auto_sample(t, 1000)).unwrap());
        auto_ingest(&mut o, &r, &b, beat(&b, 2), t, 1000)
            .await
            .unwrap();
        assert!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
                .unwrap()
                .low_anchor()
                .is_none()
        );
    }
}

#[tokio::test]
async fn autoscale_real_guard_failed_ambiguous_expired_and_obsolete_renewals_reset() {
    use crate::cluster::controller::{
        OwnershipChange, OwnershipEvidence, OwnershipGuard, OwnershipProposal,
    };
    use crate::{storage::Namespace, test_storage::Fake};
    for case in 0..4 {
        let t = Instant::now();
        let (mut r, b) = auto_root();
        r.controller = Controller {
            owner_id: None,
            epoch: 6,
            lease_state: LeaseState::Released,
            lease_until_ms: None,
        };
        let storage = Storage::new(Fake::default(), Namespace::new("synthetic").unwrap());
        storage
            .put(
                &ObjectKey::control(),
                r.encode().unwrap(),
                WriteCondition::Create,
            )
            .await
            .unwrap();
        let mut guard = OwnershipGuard::new(id(90));
        let base = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let p = OwnershipProposal::new(
            base.root(),
            OwnershipChange::Acquire { process_id: id(90) },
            id(500),
            auto_sample(t, 1000),
        )
        .unwrap();
        let stamp = guard.begin(&p).unwrap();
        let result = p
            .prepare(base)
            .unwrap()
            .submit(&storage, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            guard
                .finish(stamp, OwnershipEvidence::new(result), auto_sample(t, 1000))
                .unwrap()
        );
        r = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .root()
            .clone();
        let mut o = Observations::new(id(90), t);
        for seq in [1, 2] {
            auto_ingest(&mut o, &r, &b, beat(&b, seq), t, 1000)
                .await
                .unwrap();
        }
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        let now = t + Duration::from_secs(5);
        assert_eq!(guard.check(auto_sample(now, 6000)).unwrap(), auto_owner());
        let base = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let p = OwnershipProposal::new(
            base.root(),
            OwnershipChange::Renew(auto_owner()),
            id(501),
            auto_sample(now, 6000),
        )
        .unwrap();
        let stamp = guard.begin(&p).unwrap();
        let generation = guard.authorization_generation();
        if case == 1 {
            storage.backend().state.lock().unwrap().lose_put_response = true;
        }
        let result = if case == 0 {
            Err(StorageError::Unavailable)
        } else {
            p.prepare(base)
                .unwrap()
                .submit(&storage, Instant::now() + Duration::from_secs(5))
                .await
                .and_then(OwnershipEvidence::new)
        };
        if case == 2 {
            assert!(
                guard
                    .check(auto_sample(
                        now + Duration::from_millis(r.timing.lease_ms),
                        6000 + r.timing.lease_ms
                    ))
                    .is_err()
            );
            assert_ne!(generation, guard.authorization_generation());
        }
        if case == 3 {
            guard.invalidate().unwrap();
        }
        let finished = guard.finish(stamp, result, auto_sample(now, 6000));
        assert!(!matches!(finished, Ok(true)));
        // Immediate caller handoff, before any decision/recovery read.
        o.autoscale_failed(auto_sample(now, 6000)).unwrap();
        r = storage
            .load_control(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .root()
            .clone();
        assert!(!o.matches_autoscale(&old, auto_sample(now, 6000)).unwrap());
        auto_ingest(&mut o, &r, &b, beat(&b, 2), now, 6000)
            .await
            .unwrap();
        assert!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(now, 6000))
                .unwrap()
                .low_anchor()
                .is_none()
        );
        auto_ingest(&mut o, &r, &b, beat(&b, 3), now, 6000)
            .await
            .unwrap();
        assert_eq!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(now, 6000))
                .unwrap()
                .low_anchor()
                .unwrap()
                .monotonic(),
            now
        );
    }
}
#[test]
fn autoscale_snapshot_traits_are_closed() {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <AutoscaleSnapshot as AmbiguousIfClone<_>>::marker;
    let _ = <AutoscaleMember as AmbiguousIfClone<_>>::marker;
    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: serde::Serialize> AmbiguousIfSerialize<u8> for T {}
    let _ = <AutoscaleSnapshot as AmbiguousIfSerialize<_>>::marker;
    let _ = <AutoscaleMember as AmbiguousIfSerialize<_>>::marker;
}

#[tokio::test]
async fn autoscale_all_required_members_recover_after_any_freshness_gap() {
    let t = Instant::now();
    let (mut o, mut r, b) = auto_healthy(t).await;
    let mut n = r.nodes[0].clone();
    n.node_id = id(60);
    n.launch_id = Some(id(61));
    n.incarnation = Some(id(62));
    n.resource_id = Some("second".into());
    n.session.as_mut().unwrap().id = id(63);
    let mut start = r.intents[0].clone();
    start.request_id = id(61);
    start.node_id = n.node_id;
    start.launch_id = id(61);
    start.resource_id = n.resource_id.clone();
    r.nodes.push(n);
    r.intents.push(start);
    next(&mut r);
    r.validate().unwrap();
    let b2 = binding(&r, &r.nodes[1]).unwrap();
    auto_ingest(&mut o, &r, &b, beat(&b, 3), t, 1000)
        .await
        .unwrap();
    auto_ingest(&mut o, &r, &b2, beat(&b2, 1), t, 1000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b2, beat(&b2, 2), t, 1000)
        .await
        .unwrap();
    let d = Duration::from_millis(r.timing.expiry_ms);
    // First member stays fresh; the second member misses its original deadline.
    auto_ingest(&mut o, &r, &b, beat(&b, 4), t + d / 2, 8500)
        .await
        .unwrap();
    auto_ingest(&mut o, &r, &b2, beat(&b2, 3), t + d, 16000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + d, 16000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b, beat(&b, 4), t + d, 16000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + d, 16000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    auto_ingest(&mut o, &r, &b, beat(&b, 5), t + d, 16000)
        .await
        .unwrap();
    assert_eq!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t + d, 16000))
            .unwrap()
            .low_anchor()
            .unwrap()
            .monotonic(),
        t + d
    );
}
#[tokio::test]
async fn autoscale_unavailable_members_and_terminal_witnesses_are_not_empty_idle() {
    for state in [NodeState::Starting, NodeState::Lost, NodeState::Stopped] {
        let t = Instant::now();
        let (mut o, mut r, _) = auto_healthy(t).await;
        r.nodes[0].state = state;
        next(&mut r);
        assert!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
                .unwrap()
                .low_anchor()
                .is_none()
        );
        assert_eq!(o.autoscale.members.len(), 1);
    }
    let t = Instant::now();
    let (mut o, mut r, _) = auto_healthy(t).await;
    let mut stop = r.intents[0].clone();
    stop.request_id = id(80);
    stop.action = Action::Stop;
    stop.state = IntentState::Completed;
    stop.outcome = Some(IntentOutcome::Terminated);
    stop.incarnation_id = r.nodes[0].incarnation;
    stop.template_fingerprint = None;
    r.intents.push(stop);
    next(&mut r);
    assert_eq!(crate::cluster::provision::managed_capacity(&r).unwrap(), 0);
    let s = o
        .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
        .unwrap();
    assert!(s.members().is_empty());
    assert!(s.low_anchor().is_none());
    let mut empty = root(false);
    empty.controller = r.controller.clone();
    let mut fresh = Observations::new(id(90), t);
    assert!(
        fresh
            .autoscale_snapshot(&empty, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    let (external, b) = setup();
    let mut fresh = Observations::new(id(90), t);
    for seq in [1, 2] {
        auto_ingest(&mut fresh, &external, &b, beat(&b, seq), t, 1000)
            .await
            .unwrap();
    }
    assert!(
        fresh
            .autoscale_snapshot(&external, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
}

#[tokio::test]
async fn autoscale_foreign_replaced_capacity_and_backward_time_close_receipts() {
    for case in 0..4 {
        let t = Instant::now();
        let (mut o, mut r, b) = auto_healthy(t).await;
        let old = o
            .autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap();
        if case < 3 {
            let mut wrong = b.clone();
            match case {
                0 => wrong.cluster_id = id(999),
                1 => {
                    r.nodes[0].session.as_mut().unwrap().id = id(999);
                    next(&mut r);
                }
                _ => wrong.operation_slots += 1,
            }
            assert!(
                o.autoscale_observe(
                    &r,
                    loaded(3, &wrong, None, true).await,
                    auto_sample(t, 1000)
                )
                .is_err()
            );
        } else {
            assert!(
                o.autoscale_snapshot(
                    &r,
                    auto_owner(),
                    auto_sample(t - Duration::from_nanos(1), 1000)
                )
                .is_err()
            );
        }
        assert!(!o.matches_autoscale(&old, auto_sample(t, 1000)).unwrap());
    }
    for extra_nano in [false, true] {
        let t = Instant::now();
        let (mut o, r, _) = auto_healthy(t).await;
        let now = t + Duration::from_secs(5) + Duration::from_nanos(u64::from(extra_nano));
        assert_eq!(
            o.autoscale_snapshot(&r, auto_owner(), auto_sample(now, 4000))
                .is_ok(),
            !extra_nano
        );
    }
}

#[tokio::test]
async fn autoscale_loaded_duplicate_retains_contradiction_check_after_raw_failure() {
    let t = Instant::now();
    let (mut o, r, b) = auto_healthy(t).await;
    o.observe(&b, &beat(&b, 3), t).unwrap();
    o.read_failed(&b, t).unwrap();
    auto_ingest(&mut o, &r, &b, beat(&b, 3), t, 1000)
        .await
        .unwrap();
    assert!(
        o.autoscale_snapshot(&r, auto_owner(), auto_sample(t, 1000))
            .unwrap()
            .low_anchor()
            .is_none()
    );
    let mut contradictory = beat(&b, 3);
    contradictory.queued_operations = 1;
    assert!(matches!(
        auto_ingest(&mut o, &r, &b, contradictory, t, 1000).await,
        Err(StorageError::InvalidCatalog)
    ));
}

#[test]
fn r01_r05_lease_renewal_does_not_postpone_worker_loss() {
    let (mut root, binding) = setup();
    let now = Instant::now();
    let expiry = Duration::from_millis(root.timing.expiry_ms);
    let mut observed = Observations::new(id(90), now);
    observed.reconcile_members(&root, now).unwrap();
    observed.observe(&binding, &beat(&binding, 1), now).unwrap();
    observed.observe(&binding, &beat(&binding, 2), now).unwrap();
    for round in 1..=4 {
        root.controller.lease_until_ms = Some(round + 1);
        next(&mut root);
        let at = now + expiry * round as u32 / 4;
        observed.reconcile_members(&root, at).unwrap();
        observed.read_failed(&binding, at).unwrap();
    }
    let expired = observed
        .expiry_input(&binding, now + expiry, expiry)
        .unwrap()
        .unwrap();
    assert_eq!(expired.grace_anchor, now);
    assert_eq!(expired.last_advance, Some(now));
    let owner = crate::cluster::controller::Owner {
        process_id: id(90),
        epoch: root.controller.epoch,
    };
    let candidate =
        crate::cluster::membership::loss_candidate(&root, owner, &expired, id(999)).unwrap();
    let (_, _, lost) = candidate.into_parts();
    assert_eq!(lost.nodes[0].state, NodeState::Lost);
}
