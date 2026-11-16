use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::*,
    state::*,
};
use crate::node::auth::verify_enrollment;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
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
    let mut message = b"nabedra.enroll.v1\0".to_vec();
    message.extend_from_slice(unsigned.as_bytes());
    let signed = format!(
        "{},\"proof\":\"{}\"}}",
        &unsigned[..unsigned.len() - 1],
        hex(key.sign(&message).as_ref())
    );
    verify_enrollment(signed.as_bytes()).unwrap()
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
fn change(plan: RegistrationPlan) -> Control {
    match plan {
        RegistrationPlan::Change(c) => c.result,
        RegistrationPlan::Existing(_) => panic!("expected change"),
    }
}
#[test]
fn late_registration_and_exact_reply_loss_recovery() {
    let c = managed();
    let p = proof(&claims(Some(id(6))));
    let out = change(registration_plan(&c, owner(), &p, id(10)).unwrap());
    assert_eq!(out.nodes[0].registration, Registration::Registered);
    assert_eq!(out.nodes[0].state, NodeState::Starting);
    assert_eq!(out.nodes[0].resource_id, None);
    assert_eq!(out.intents, c.intents);
    match registration_plan(&out, owner(), &p, id(10)).unwrap() {
        RegistrationPlan::Existing(n) => assert_eq!(n, out.nodes[0]),
        _ => panic!("retry changed root"),
    }
}
fn registered() -> Control {
    change(registration_plan(&managed(), owner(), &proof(&claims(Some(id(6)))), id(10)).unwrap())
}
fn preservation(before: &Control, after: &Control) {
    let mut restored = after.clone();
    restored.nodes = before.nodes.clone();
    restored.revision = before.revision;
    restored.change_id = before.change_id;
    assert_eq!(restored.encode().unwrap(), before.encode().unwrap());
    assert_eq!(
        before.capacity().unwrap().credits,
        after.capacity().unwrap().credits
    );
}
fn drain(c: &mut Control) {
    c.nodes[0].state = NodeState::Draining;
    c.nodes[0].drain = Some(Drain {
        id: id(77),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    c.validate().unwrap();
}
fn readiness(c: &Control) -> ReadinessInput {
    ReadinessInput {
        cluster_id: c.cluster_id,
        member: c.nodes[0].clone(),
        observation: OwnershipObservation::new(c).unwrap(),
        challenge: id(99),
        verified_challenge: id(99),
        initialized: true,
    }
}
fn expiry(c: &Control) -> ExpiryInput {
    let t = Instant::now();
    ExpiryInput {
        cluster_id: c.cluster_id,
        member: c.nodes[0].clone(),
        observation: OwnershipObservation::new(c).unwrap(),
        generation: 1,
        controller_boot: owner().process_id,
        grace_anchor: t,
        last_sequence: Some(8),
        last_advance: Some(t),
        sampled_at: t + std::time::Duration::from_millis(c.timing.expiry_ms),
    }
}
#[test]
fn managed_eligibility_and_exact_retry_conflicts() {
    let p = proof(&claims(Some(id(6))));
    for state in [
        IntentState::InProgress,
        IntentState::Reconciling,
        IntentState::Completed,
    ] {
        let mut c = managed();
        c.intents[0].state = state;
        if state == IntentState::Completed {
            c.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
            c.intents[0].resource_id = Some("resource".into());
            c.nodes[0].resource_id = Some("resource".into());
        }
        c.validate().unwrap();
        let out = change(registration_plan(&c, owner(), &p, id(10)).unwrap());
        preservation(&c, &out);
    }
    let mut c = managed();
    drain(&mut c);
    let out = change(registration_plan(&c, owner(), &p, id(10)).unwrap());
    assert_eq!(out.nodes[0].drain, c.nodes[0].drain);
    assert_eq!(out.nodes[0].state, NodeState::Draining);
    assert!(matches!(
        registration_plan(&out, owner(), &p, id(11)),
        Ok(RegistrationPlan::Existing(_))
    ));
    for kind in 0..3 {
        let mut c = managed();
        match kind {
            0 => {
                c.intents[0].state = IntentState::Pending;
                c.intents[0].delivery = None;
                c.intents[0].attempts = 0
            }
            1 => c.intents[0].delivery = None,
            2 => {
                c.intents[0].state = IntentState::Failed;
                c.intents[0].outcome = Some(IntentOutcome::NotInvoked)
            }
            _ => unreachable!(),
        }
        c.validate().unwrap();
        assert!(registration_plan(&c, owner(), &p, id(10)).is_err());
    }
    for (field, value) in [("request_id", json!(id(66))), ("cluster_id", json!(id(66)))] {
        let mut v = claims(Some(id(6)));
        v[field] = value;
        assert!(registration_plan(&managed(), owner(), &proof(&v), id(10)).is_err());
    }
    let mut heterogeneous = claims(Some(id(6)));
    heterogeneous["operation_slots"] = json!(5);
    heterogeneous["queue_size"] = json!(65);
    let out =
        change(registration_plan(&managed(), owner(), &proof(&heterogeneous), id(10)).unwrap());
    assert_eq!(out.nodes[0].operation_slots, 5);
    assert_eq!(out.nodes[0].queue_size, 65);
    let mut c = registered();
    c.nodes[0].resource_id = Some("resource".into());
    c.nodes[0].registration = Registration::Verified;
    c.nodes[0].state = NodeState::Ready;
    c.validate().unwrap();
    for phase in [NodeState::Ready, NodeState::Draining] {
        if phase == NodeState::Draining {
            drain(&mut c);
        }
        let before = c.encode().unwrap();
        match registration_plan(&c, owner(), &p, id(11)).unwrap() {
            RegistrationPlan::Existing(n) => assert_eq!(n, c.nodes[0]),
            _ => panic!("retry mutated"),
        };
        assert_eq!(before, c.encode().unwrap());
        for (field, value) in [
            ("session_id", json!(id(66))),
            ("endpoint", json!("http://other:8000")),
            ("queue_size", json!(65)),
            (
                "public_key",
                json!(hex(Ed25519KeyPair::from_seed_unchecked(&[43; 32])
                    .unwrap()
                    .public_key()
                    .as_ref())),
            ),
        ] {
            let mut v = claims(Some(id(6)));
            v[field] = value;
            assert!(registration_plan(&c, owner(), &proof(&v), id(11)).is_err());
        }
    }
    for phase in [NodeState::Lost, NodeState::Stopped] {
        c.nodes[0].state = phase;
        c.validate().unwrap();
        assert!(registration_plan(&c, owner(), &p, id(11)).is_err());
    }
}
#[test]
fn external_admission_replacement_and_permanent_retirement() {
    let c = root(true);
    let v = claims(None);
    let p = proof(&v);
    let mut out = change(registration_plan(&c, owner(), &p, id(10)).unwrap());
    preservation(&c, &out);
    assert_eq!(
        out.nodes[0].resource_id,
        Some(format!("external:{}", id(3)))
    );
    let mut next = v.clone();
    next["incarnation"] = json!(id(8));
    next["session_id"] = json!(id(9));
    assert!(registration_plan(&out, owner(), &proof(&next), id(11)).is_err());
    for phase in [NodeState::Lost, NodeState::Stopped] {
        out.nodes[0].state = phase;
        out.nodes[0].retired_incarnations = vec![id(2), id(7)];
        out.validate().unwrap();
        let replaced = change(registration_plan(&out, owner(), &proof(&next), id(11)).unwrap());
        assert_eq!(
            replaced.nodes[0].retired_incarnations,
            vec![id(2), id(4), id(7)]
        );
        assert_eq!(replaced.nodes[0].registration, Registration::Registered);
        assert_eq!(replaced.nodes[0].state, NodeState::Starting);
        assert!(replaced.nodes[0].drain.is_none());
        assert!(registration_plan(&replaced, owner(), &p, id(12)).is_err());
        preservation(&out, &replaced);
    }
    next["session_id"] = json!(id(5));
    assert!(registration_plan(&out, owner(), &proof(&next), id(11)).is_err());
    next["session_id"] = json!(id(9));
    out.nodes[0].retired_incarnations = (100..116).map(id).collect();
    out.validate().unwrap();
    assert!(matches!(
        registration_plan(&out, owner(), &proof(&next), id(11)),
        Err(StorageError::LimitExceeded)
    ));
    assert!(registration_plan(&c, owner(), &proof(&claims(Some(id(6)))), id(10)).is_err());
}
#[test]
fn readiness_binds_initialized_challenge_resource_and_complete_root() {
    let mut c = registered();
    assert!(ready_candidate(&c, owner(), &readiness(&c), id(11)).is_err());
    c.nodes[0].resource_id = Some("resource".into());
    c.validate().unwrap();
    let out = ready_candidate(&c, owner(), &readiness(&c), id(11))
        .unwrap()
        .result;
    assert_eq!(out.nodes[0].state, NodeState::Ready);
    assert_eq!(out.nodes[0].registration, Registration::Verified);
    preservation(&c, &out);
    for k in 0..9 {
        let mut e = readiness(&c);
        match k {
            0 => e.initialized = false,
            1 => e.verified_challenge = id(2),
            2 => e.member.incarnation = Some(id(2)),
            3 => e.member.resource_id = Some("other".into()),
            4 => e.member.endpoint = Some("http://other:8000".into()),
            5 => e.member.session.as_mut().unwrap().verifier = "f".repeat(64),
            6 => e.cluster_id = id(2),
            7 => e.member.operation_slots = 5,
            _ => {
                let mut other = c.clone();
                other.scheduler.failures += 1;
                e.observation = OwnershipObservation::new(&other).unwrap();
            }
        }
        assert!(ready_candidate(&c, owner(), &e, id(11)).is_err());
    }
    for state in [NodeState::Draining, NodeState::Lost, NodeState::Stopped] {
        c.nodes[0].state = state;
        c.validate().unwrap();
        assert!(ready_candidate(&c, owner(), &readiness(&c), id(11)).is_err());
    }
}
#[test]
fn expiry_is_phase_safe_and_checks_monotonic_assertions() {
    let mut c = registered();
    for phase in [NodeState::Starting, NodeState::Ready] {
        if phase == NodeState::Ready {
            c.nodes[0].resource_id = Some("resource".into());
            c.nodes[0].registration = Registration::Verified;
        }
        c.nodes[0].state = phase;
        c.validate().unwrap();
        let e = expiry(&c);
        let out = loss_candidate(&c, owner(), &e, id(11)).unwrap().result;
        let mut expected = c.nodes[0].clone();
        expected.state = NodeState::Lost;
        assert_eq!(out.nodes[0], expected);
        preservation(&c, &out);
        let mut missing = expiry(&c);
        missing.last_sequence = None;
        missing.last_advance = None;
        assert!(loss_candidate(&c, owner(), &missing, id(11)).is_ok());
        for k in 0..11 {
            let mut e = expiry(&c);
            match k {
                0 => e.sampled_at = e.grace_anchor,
                1 => e.last_sequence = Some(0),
                2 => e.generation = 0,
                3 => e.last_advance = Some(e.sampled_at),
                4 => e.last_sequence = None,
                5 => e.member.session.as_mut().unwrap().id = id(99),
                6 => e.grace_anchor = e.sampled_at + std::time::Duration::from_secs(1),
                7 => e.controller_boot = id(200),
                8 => e.member.queue_size += 1,
                9 => e.cluster_id = id(200),
                _ => {
                    let mut other = c.clone();
                    other.scheduler.failures += 1;
                    e.observation = OwnershipObservation::new(&other).unwrap();
                }
            }
            assert!(loss_candidate(&c, owner(), &e, id(11)).is_err());
        }
    }
    drain(&mut c);
    let before = c.encode().unwrap();
    assert!(loss_candidate(&c, owner(), &expiry(&c), id(11)).is_err());
    assert_eq!(before, c.encode().unwrap());
    let c = managed();
    assert!(loss_candidate(&c, owner(), &expiry(&c), id(11)).is_err());
}
#[test]
fn owner_proposal_and_revision_exhaustion() {
    let mut c = registered();
    let p = proof(&claims(Some(id(6))));
    c.revision = u64::MAX;
    c.validate().unwrap();
    assert!(matches!(
        registration_plan(&c, owner(), &p, c.change_id),
        Ok(RegistrationPlan::Existing(_))
    ));
    c.nodes[0].resource_id = Some("resource".into());
    assert!(matches!(
        ready_candidate(&c, owner(), &readiness(&c), id(11)),
        Err(StorageError::LimitExceeded)
    ));
    let mut base = managed();
    base.revision = u64::MAX;
    assert!(matches!(
        registration_plan(&base, owner(), &p, id(11)),
        Err(StorageError::LimitExceeded)
    ));
    assert!(registration_plan(&managed(), owner(), &p, id(2)).is_err());
    assert!(
        registration_plan(
            &c,
            Owner {
                epoch: 8,
                ..owner()
            },
            &p,
            id(11)
        )
        .is_err()
    );
    let mut invalid = managed();
    invalid.config.template_fingerprint = "f".repeat(64);
    assert!(matches!(
        registration_plan(&invalid, owner(), &p, id(11)),
        Err(StorageError::InvalidCatalog)
    ));
}
#[test]
fn saturated_admission_preserves_retained_recovery_and_unrelated_bytes() {
    let mut c = managed();
    for k in 1..1366 {
        let mut n = c.nodes[0].clone();
        n.node_id = id(1000 + k);
        n.launch_id = Some(id(10000 + k));
        let mut i = c.intents[0].clone();
        i.node_id = n.node_id;
        i.request_id = id(10000 + k);
        i.launch_id = i.request_id;
        c.nodes.push(n);
        c.intents.push(i);
    }
    c.nodes.sort_by_key(|n| n.node_id);
    c.intents.sort_by_key(|i| i.request_id);
    c.validate().unwrap();
    assert!(c.check_new_admission().is_err());
    let out = change(registration_plan(&c, owner(), &proof(&claims(Some(id(6)))), id(10)).unwrap());
    preservation(&c, &out);
    assert_eq!(out.nodes[1..], c.nodes[1..]);
    assert_eq!(out.intents, c.intents);
    let mut external = root(true);
    let seed =
        change(registration_plan(&external, owner(), &proof(&claims(None)), id(10)).unwrap())
            .nodes
            .remove(0);
    for k in 0..2048 {
        let mut n = seed.clone();
        n.node_id = id(1000 + k);
        n.resource_id = Some(format!("external:{}", n.node_id));
        n.incarnation = Some(id(10000 + k));
        n.session.as_mut().unwrap().id = id(20000 + k);
        n.state = NodeState::Lost;
        external.nodes.push(n);
    }
    external.validate().unwrap();
    external.check_new_admission().unwrap();
    let before = external.encode().unwrap();
    assert!(matches!(
        registration_plan(&external, owner(), &proof(&claims(None)), id(11)),
        Err(StorageError::LimitExceeded)
    ));
    assert_eq!(before, external.encode().unwrap());
    let mut v = claims(None);
    v["node_id"] = json!(id(1000));
    let out = change(registration_plan(&external, owner(), &proof(&v), id(11)).unwrap());
    preservation(&external, &out);
    assert_eq!(out.nodes[1..], external.nodes[1..]);
}

#[test]
fn consumed_parts_diagnostics_and_retained_stop_evidence() {
    let mut c = registered();
    c.nodes[0].resource_id = Some("resource".into());
    let mut stop = c.intents[0].clone();
    stop.action = Action::Stop;
    stop.request_id = id(150);
    stop.incarnation_id = c.nodes[0].incarnation;
    stop.resource_id = c.nodes[0].resource_id.clone();
    stop.template_fingerprint = None;
    c.intents.push(stop);
    c.validate().unwrap();
    let evidence = expiry(&c);
    let candidate = loss_candidate(&c, owner(), &evidence, id(11)).unwrap();
    assert_eq!(format!("{candidate:?}"), "MembershipCandidate");
    assert_eq!(format!("{evidence:?}"), "ExpiryInput");
    assert_eq!(format!("{:?}", readiness(&c)), "ReadinessInput");
    let (base, expected_owner, result) = candidate.into_parts();
    assert!(base == OwnershipObservation::new(&c).unwrap());
    assert_eq!(expected_owner, owner());
    assert_eq!(result.revision, c.revision + 1);
    assert_eq!(result.change_id, id(11));
    assert_eq!(result.intents, c.intents);
    preservation(&c, &result);
    let p = proof(&claims(Some(id(6))));
    let plan = registration_plan(&c, owner(), &p, id(11)).unwrap();
    assert_eq!(format!("{plan:?}"), "RegistrationPlan::Existing");
    let mut terminal = c.clone();
    terminal.intents[0].state = IntentState::Failed;
    terminal.intents[0].outcome = Some(IntentOutcome::NotInvoked);
    terminal.validate().unwrap();
    assert!(registration_plan(&terminal, owner(), &p, id(11)).is_err());
    let mut bad_template = managed();
    bad_template.intents[0].template_fingerprint = Some("f".repeat(64));
    assert!(matches!(
        registration_plan(&bad_template, owner(), &p, id(11)),
        Err(StorageError::InvalidCatalog)
    ));
    let mut released = c.clone();
    released.controller.lease_state = LeaseState::Released;
    released.controller.owner_id = None;
    released.controller.lease_until_ms = None;
    released.validate().unwrap();
    assert!(registration_plan(&released, owner(), &p, id(11)).is_err());
    // A negative Clone bound detects an accidental capability duplication API.
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <MembershipCandidate as AmbiguousIfClone<_>>::marker;
}
