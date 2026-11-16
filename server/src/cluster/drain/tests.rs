use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::*,
    membership::{ReadinessInput, RegistrationPlan, ready_candidate, registration_plan},
    state::*,
};
use crate::node::auth::{EnrollmentProof, verify_enrollment};
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

fn registered() -> Control {
    let c = managed();
    let RegistrationPlan::Change(candidate) =
        registration_plan(&c, owner(), &proof(&claims(Some(id(6)))), id(10)).unwrap()
    else {
        panic!("change")
    };
    candidate.into_parts().2
}
fn changed(plan: DrainPlan) -> Control {
    let DrainPlan::Change(c) = plan else {
        panic!("change")
    };
    c.into_parts().2
}
#[test]
fn begin_preserves_full_root_and_credits_for_retained_phases() {
    let starting = registered();
    let evidence = ReadinessInput {
        cluster_id: starting.cluster_id,
        member: starting.nodes[0].clone(),
        observation: OwnershipObservation::new(&starting).unwrap(),
        challenge: id(99),
        verified_challenge: id(99),
        initialized: true,
    };
    let mut ready_base = starting.clone();
    ready_base.nodes[0].resource_id = Some("resource".into());
    let ready_evidence = ReadinessInput {
        member: ready_base.nodes[0].clone(),
        observation: OwnershipObservation::new(&ready_base).unwrap(),
        ..evidence
    };
    let ready = ready_candidate(&ready_base, owner(), &ready_evidence, id(11))
        .unwrap()
        .into_parts()
        .2;
    for base in [managed(), starting, ready] {
        let result = changed(begin_drain(&base, owner(), id(3), id(77), 123, id(12)).unwrap());
        let mut expected = base.clone();
        expected.revision += 1;
        expected.change_id = id(12);
        expected.nodes[0].state = NodeState::Draining;
        expected.nodes[0].drain = Some(Drain {
            id: id(77),
            requested_at_ms: 123,
            ack_seq: None,
            confirmed_idle: false,
        });
        assert_eq!(result.encode().unwrap(), expected.encode().unwrap());
        assert_eq!(
            base.capacity().unwrap().credits,
            result.capacity().unwrap().credits
        );
    }
}
#[test]
fn existing_converges_before_growth_and_clear_retains_ack() {
    let base = registered();
    let mut c = changed(begin_drain(&base, owner(), id(3), id(77), 123, id(12)).unwrap());
    c.revision = u64::MAX;
    c.nodes[0].drain.as_mut().unwrap().ack_seq = Some(8);
    for drain_id in [id(77), id(3)] {
        let DrainPlan::Existing(binding) =
            begin_drain(&c, owner(), id(3), drain_id, 999, c.change_id).unwrap()
        else {
            panic!("existing")
        };
        assert_eq!(binding.node, c.nodes[0]);
    }
    assert!(matches!(
        clear_idle(&c, owner(), id(3), id(77), c.change_id),
        Ok(DrainPlan::Existing(_))
    ));
    c.nodes[0].drain.as_mut().unwrap().confirmed_idle = true;
    assert!(matches!(
        clear_idle(&c, owner(), id(3), id(77), id(13)),
        Err(StorageError::LimitExceeded)
    ));
    c.revision = 40;
    let out = changed(clear_idle(&c, owner(), id(3), id(77), id(13)).unwrap());
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(13);
    expected.nodes[0].drain.as_mut().unwrap().confirmed_idle = false;
    assert_eq!(out, expected);
    assert_eq!(out.nodes[0].drain.as_ref().unwrap().ack_seq, Some(8));
}
#[test]
fn independent_refusals_validate_base_owner_target_and_fresh_identity() {
    let c = registered();
    for state in [NodeState::Lost, NodeState::Stopped, NodeState::Draining] {
        let mut bad = c.clone();
        bad.nodes[0].state = state;
        bad.validate().unwrap();
        assert!(matches!(
            begin_drain(&bad, owner(), id(3), id(77), 0, id(12)),
            Err(StorageError::Conflict)
        ));
    }
    for wrong in [
        Owner {
            process_id: id(91),
            ..owner()
        },
        Owner {
            epoch: 8,
            ..owner()
        },
    ] {
        assert!(matches!(
            begin_drain(&c, wrong, id(3), id(77), 0, id(12)),
            Err(StorageError::Conflict)
        ));
    }
    for reused in [
        id(1),
        c.change_id,
        id(3),
        id(4),
        id(5),
        id(6),
        id(7),
        id(88),
        id(90),
        id(12),
    ] {
        assert!(matches!(
            begin_drain(&c, owner(), id(3), reused, 0, id(12)),
            Err(StorageError::Conflict)
        ));
    }
    let mut max = c.clone();
    max.revision = u64::MAX;
    assert!(matches!(
        begin_drain(&max, owner(), id(3), id(77), 0, id(12)),
        Err(StorageError::LimitExceeded)
    ));
    assert!(begin_drain(&c, owner(), id(9), id(77), 0, id(12)).is_err());
    assert!(begin_drain(&c, owner(), id(3), id(77), 0, c.change_id).is_err());
    let draining = changed(begin_drain(&c, owner(), id(3), id(77), 0, id(12)).unwrap());
    assert!(clear_idle(&draining, owner(), id(3), id(78), id(13)).is_err());
    let mut invalid = c;
    invalid.config.generation = 1;
    assert!(begin_drain(&invalid, owner(), id(3), id(77), 0, id(12)).is_err());
}

// Added after the initial red/green cycle as boundary coverage.
#[test]
fn external_and_unrelated_records_remain_exact_and_existing_still_checks_owner() {
    let mut c = registered();
    c.provisioning = Provisioning::External;
    c.config.adapter_contract = None;
    c.intents.clear();
    c.nodes[0].ownership = Ownership::External;
    c.nodes[0].launch_id = None;
    c.nodes[0].resource_id = Some(format!("external:{}", id(3)));
    c.nodes[0].retired_incarnations = vec![id(42)];
    let mut other = c.nodes[0].clone();
    other.node_id = id(30);
    other.incarnation = Some(id(31));
    other.session.as_mut().unwrap().id = id(32);
    other.resource_id = Some(format!("external:{}", id(30)));
    other.retired_incarnations.clear();
    other.state = NodeState::Draining;
    other.drain = Some(Drain {
        id: id(33),
        requested_at_ms: 1,
        ack_seq: Some(2),
        confirmed_idle: true,
    });
    c.nodes.push(other);
    let c = Control::decode(&c.encode().unwrap()).unwrap();
    for reused in [id(30), id(31), id(32), id(33), id(42)] {
        assert!(matches!(
            begin_drain(&c, owner(), id(3), reused, 0, id(12)),
            Err(StorageError::Conflict)
        ));
    }
    let result = changed(begin_drain(&c, owner(), id(3), id(77), 3, id(12)).unwrap());
    let mut restored = result.clone();
    restored.nodes[0] = c.nodes[0].clone();
    restored.revision = c.revision;
    restored.change_id = c.change_id;
    assert_eq!(restored.encode().unwrap(), c.encode().unwrap());
    assert_eq!(
        format!(
            "{:?}",
            begin_drain(&result, owner(), id(3), id(77), 3, id(12)).unwrap()
        ),
        "DrainPlan"
    );
    for bad_owner in [
        Owner {
            epoch: 8,
            ..owner()
        },
        Owner {
            process_id: id(91),
            ..owner()
        },
    ] {
        assert!(matches!(
            begin_drain(&result, bad_owner, id(3), id(77), 3, id(12)),
            Err(StorageError::Conflict)
        ));
        assert!(matches!(
            clear_idle(&result, bad_owner, id(3), id(77), id(13)),
            Err(StorageError::Conflict)
        ));
    }
    for state in [NodeState::Starting, NodeState::Lost, NodeState::Stopped] {
        let mut bad = result.clone();
        bad.nodes[0].state = state;
        bad.nodes[0].drain = None;
        bad.validate().unwrap();
        assert!(matches!(
            clear_idle(&bad, owner(), id(3), id(77), id(13)),
            Err(StorageError::Conflict)
        ));
    }
    let mut missing = result.clone();
    missing.nodes[0].drain = None;
    missing.validate().unwrap();
    assert!(matches!(
        clear_idle(&missing, owner(), id(3), id(77), id(13)),
        Err(StorageError::Conflict)
    ));
    let mut released = result;
    released.controller.owner_id = None;
    released.controller.lease_state = LeaseState::Released;
    released.controller.lease_until_ms = None;
    released.validate().unwrap();
    assert!(matches!(
        begin_drain(&released, owner(), id(3), id(77), 3, id(12)),
        Err(StorageError::Conflict)
    ));
}

#[test]
fn candidates_bind_complete_observation_and_are_not_clone_or_serializable() {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: serde::Serialize> AmbiguousIfSerialize<u8> for T {}
    let _ = <DrainCandidate as AmbiguousIfClone<_>>::marker;
    let _ = <DrainBinding as AmbiguousIfClone<_>>::marker;
    let _ = <DrainPlan as AmbiguousIfClone<_>>::marker;
    let _ = <DrainCandidate as AmbiguousIfSerialize<_>>::marker;
    let _ = <DrainBinding as AmbiguousIfSerialize<_>>::marker;
    let c = managed();
    let DrainPlan::Change(candidate) = begin_drain(&c, owner(), id(3), id(77), 0, id(12)).unwrap()
    else {
        panic!("change")
    };
    assert_eq!(format!("{candidate:?}"), "DrainCandidate");
    let (observation, captured_owner, _) = candidate.into_parts();
    assert!(observation == OwnershipObservation::new(&c).unwrap());
    assert_eq!(captured_owner, owner());
}
