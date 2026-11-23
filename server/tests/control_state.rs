#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::cluster::{
    configuration::{Template, Timing},
    state::{Control, ControlError, MAX_CONTROL_BYTES},
};
use serde_json::{Value, json};
use uuid::Uuid;
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
fn decode(v: &Value) -> Result<Control, ControlError> {
    Control::decode(&serde_json::to_vec(v).unwrap())
}
fn fleet(n: usize) -> Control {
    let mut v = root();
    let hash = v["config"]["template_fingerprint"].clone();
    for k in 0..n {
        let node = id(100 + k as u128);
        let launch = id(10000 + k as u128);
        v["nodes"].as_array_mut().unwrap().push(json!({"node_id":node,"launch_id":launch,"resource_id":format!("resource/{k}"),"ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}));
        v["intents"].as_array_mut().unwrap().push(json!({"request_id":launch,"node_id":node,"launch_id":launch,"incarnation_id":null,"resource_id":format!("resource/{k}"),"action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":hash,"delivery":null,"outcome":"resource_known"}));
    }
    decode(&v).unwrap()
}
fn stop(c: &mut Control, k: usize, seq: u128) {
    let mut v = serde_json::to_value(&c.intents[k]).unwrap();
    v["request_id"] = json!(id(seq));
    v["action"] = json!("stop");
    v["state"] = json!("pending");
    v["outcome"] = Value::Null;
    v["template_fingerprint"] = Value::Null;
    c.intents.push(serde_json::from_value(v).unwrap());
    c.intents.sort_by_key(|i| i.request_id);
}
#[test]
fn initial_roots_and_independent_sequences_roundtrip() {
    let mut v = root();
    for external in [false, true] {
        if external {
            v["provisioning"] = json!("external");
            v["config"]["adapter_contract"] = Value::Null;
        }
        let c = decode(&v).unwrap();
        assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
        c.check_new_admission().unwrap();
    }
    let mut c = fleet(1);
    c.revision = 99;
    c.policy.revision = 7;
    c.admin_seq = 2;
    c.admin_head = Some(id(4));
    c.controller.epoch = 9;
    assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
}
#[test]
fn strict_objects_nulls_numbers_and_identities() {
    let original = root();
    for pointer in [
        "",
        "/config",
        "/policy",
        "/controller",
        "/scheduler",
        "/template",
        "/timing",
        "/template/storage/options",
    ] {
        let mut v = original.clone();
        v.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(0));
        assert!(decode(&v).is_err(), "{pointer}");
        let mut v = original.clone();
        *v.pointer_mut(pointer).unwrap() = json!([]);
        assert!(decode(&v).is_err());
    }
    for (obj, key) in [
        ("", "admin_head"),
        ("/config", "adapter_contract"),
        ("/controller", "owner_id"),
        ("/controller", "lease_until_ms"),
        ("/scheduler", "low_since_ms"),
    ] {
        let mut v = original.clone();
        v.pointer_mut(obj)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(decode(&v).is_err());
    }
    let raw = original.to_string();
    for (needle, replacement) in [
        ("\"version\":1", "\"version\":1,\"version\":1"),
        ("\"generation\":2", "\"generation\":2,\"generation\":2"),
        (
            "\"mode\":\"fixed\"",
            "\"mode\":\"fixed\",\"mode\":\"fixed\"",
        ),
        ("\"epoch\":0", "\"epoch\":0,\"epoch\":0"),
        ("\"failures\":0", "\"failures\":0,\"failures\":0"),
    ] {
        assert!(Control::decode(raw.replace(needle, replacement).as_bytes()).is_err());
    }
    for bad in ["1.0", "1e0", "true", "-1", "18446744073709551616"] {
        assert!(
            Control::decode(
                raw.replace("\"version\":1", &format!("\"version\":{bad}"))
                    .as_bytes()
            )
            .is_err()
        );
    }
    for bad in [
        "00000000000000000000000000000001",
        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
    ] {
        let mut v = original.clone();
        v["cluster_id"] = json!(bad);
        assert!(decode(&v).is_err());
    }
}
#[test]
fn independent_corruptions_reject_decode_and_constructed_encode() {
    let c = fleet(2);
    let good = serde_json::to_value(&c).unwrap();
    for (pointer, bad) in [
        ("/version", json!(2)),
        ("/name", json!("bad-name")),
        ("/config/generation", json!(1)),
        ("/config/auth_fingerprint", json!("A".repeat(64))),
        ("/config/template_fingerprint", json!("a".repeat(64))),
        ("/config/adapter_contract", Value::Null),
        ("/revision", json!(0)),
        ("/policy/revision", json!(0)),
        ("/policy/desired", json!(1)),
        ("/controller/owner_id", json!(id(3))),
        ("/controller/lease_state", json!("held")),
        ("/admin_seq", json!(1)),
        (
            "/nodes/1/resource_id",
            good["nodes"][0]["resource_id"].clone(),
        ),
        ("/nodes/1/node_id", good["nodes"][0]["node_id"].clone()),
        ("/intents/0/node_id", json!(id(9))),
        ("/intents/0/launch_id", json!(id(9))),
        ("/intents/0/resource_id", json!("wrong")),
        ("/intents/0/template_fingerprint", json!("a".repeat(64))),
    ] {
        let mut v = good.clone();
        *v.pointer_mut(pointer).unwrap() = bad;
        assert!(decode(&v).is_err(), "{pointer}");
        let constructed: Control = serde_json::from_value(v).unwrap();
        assert!(constructed.encode().is_err(), "{pointer}");
    }
    let mut c = c.clone();
    c.intents.remove(0);
    assert!(c.validate().is_err());
}
#[test]
fn fleet_first_stops_preserve_reserved_capacity() {
    let mut c = fleet(1024);
    let before = c.capacity().unwrap();
    assert_eq!(before.credits, 1024);
    assert_eq!(before.retained_bytes, 12845056);
    assert_eq!(before.admission_bytes, 17039360);
    c.check_new_admission().unwrap();
    for node in &mut c.nodes {
        node.state = nabedra_database_server::cluster::member::NodeState::Draining;
    }
    assert_eq!(c.capacity().unwrap(), before);
    for k in 0..1024 {
        stop(&mut c, k, 20000 + k as u128);
    }
    let after = c.capacity().unwrap();
    assert_eq!(after.credits, 0);
    assert_eq!(after.admission_bytes, before.admission_bytes);
    assert_eq!(after.retained_bytes, after.admission_bytes);
    assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
}
#[test]
fn credits_follow_retained_witnesses_instead_of_phase_or_stop_result() {
    use nabedra_database_server::cluster::member::{IntentOutcome, IntentState, NodeState};
    let mut c = fleet(1);
    for state in [NodeState::Lost, NodeState::Stopped, NodeState::Draining] {
        c.nodes[0].state = state;
        assert_eq!(c.capacity().unwrap().credits, 1);
    }
    c.intents[0].state = IntentState::Reconciling;
    c.intents[0].outcome = None;
    c.intents[0].resource_id = None;
    assert_eq!(c.capacity().unwrap().credits, 1);
    for outcome in [IntentOutcome::NotInvoked, IntentOutcome::Terminated] {
        c.intents[0].state = IntentState::Failed;
        c.intents[0].outcome = Some(outcome);
        assert_eq!(c.capacity().unwrap().credits, 0);
    }
    c = fleet(1);
    stop(&mut c, 0, 20000);
    for (state, outcome) in [
        (IntentState::Failed, IntentOutcome::NotInvoked),
        (IntentState::Completed, IntentOutcome::Terminated),
    ] {
        c.intents[1].state = state;
        c.intents[1].outcome = Some(outcome);
        assert_eq!(c.capacity().unwrap().credits, 0);
        Control::decode(&c.encode().unwrap()).unwrap();
    }
}
#[test]
fn saturated_reservations_allow_growth_and_first_stop_but_refuse_extra_identity() {
    let mut c = fleet(1024);
    for k in 0..1025 {
        stop(&mut c, 0, 20000 + k);
    }
    c.check_new_admission().unwrap();
    let before = c.capacity().unwrap();
    assert_eq!(before.intents + before.credits, 3072);
    let mut unreserved = c.clone();
    stop(&mut unreserved, 0, 24000);
    assert_eq!(
        unreserved.check_new_admission(),
        Err(ControlError::LimitExceeded)
    );
    // Admission exhaustion is not a load-phase constraint.
    Control::decode(&unreserved.encode().unwrap()).unwrap();
    // Maximum permitted mutable strings/identities and decimal widths use the
    // existing member reservation. No historical evidence is claimed here.
    let resource = format!("rr{}", "\\\"".repeat(127));
    c.nodes[1].resource_id = Some(resource.clone());
    c.intents[1].resource_id = Some(resource);
    let incarnation = id(50000);
    let n = &mut c.nodes[1];
    n.state = nabedra_database_server::cluster::member::NodeState::Draining;
    n.registration = nabedra_database_server::cluster::member::Registration::Registered;
    n.incarnation = Some(incarnation);
    n.endpoint = Some(format!("http://worker/{}", "x".repeat(2034)));
    n.retired_incarnations = (0..16).map(|k| id(60000 + k)).collect();
    n.session = Some(nabedra_database_server::cluster::member::Session {
        id: id(70000),
        verifier: "f".repeat(64),
    });
    n.drain = Some(nabedra_database_server::cluster::member::Drain {
        id: id(70001),
        requested_at_ms: u64::MAX,
        ack_seq: Some(u64::MAX),
        confirmed_idle: true,
    });
    let i = &mut c.intents[1];
    i.attempts = u64::MAX;
    i.created_at_ms = u64::MAX;
    i.deadline_ms = u64::MAX;
    i.retry_at_ms = u64::MAX;
    i.error_code = Some("e".repeat(256));
    i.delivery = Some(nabedra_database_server::cluster::member::Delivery {
        invocation_id: id(70002),
        owner_id: id(70003),
        epoch: u64::MAX,
        command_sha256: "e".repeat(64),
    });
    assert_eq!(c.capacity().unwrap(), before);
    stop(&mut c, 1, 25000);
    c.intents.last_mut().unwrap().incarnation_id = Some(incarnation);
    c.check_new_admission().unwrap();
    assert_eq!(
        c.capacity().unwrap().admission_bytes,
        before.admission_bytes
    );
    assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
    for k in 2..1024 {
        stop(&mut c, k, 26000 + k as u128);
    }
    let full = c.capacity().unwrap();
    assert_eq!((full.nodes, full.intents, full.credits), (1024, 3072, 0));
    assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
}
#[test]
fn cross_bindings_incarnations_external_members_and_late_recovery() {
    let mut c = fleet(2);
    for k in 0..2 {
        c.nodes[k].incarnation = Some(id(80000 + k as u128));
        c.nodes[k].endpoint = Some("http://worker/".into());
        c.nodes[k].session = Some(nabedra_database_server::cluster::member::Session {
            id: id(90000 + k as u128),
            verifier: "a".repeat(64),
        });
        c.nodes[k].registration =
            nabedra_database_server::cluster::member::Registration::Registered;
        c.nodes[k].state = nabedra_database_server::cluster::member::NodeState::Draining;
    }
    c.nodes[0].resource_id = None;
    c.intents[0].resource_id = None;
    c.intents[0].state = nabedra_database_server::cluster::member::IntentState::Reconciling;
    c.intents[0].outcome = None;
    Control::decode(&c.encode().unwrap()).unwrap();
    let mut bad = c.clone();
    bad.nodes[0].retired_incarnations = vec![c.nodes[1].incarnation.unwrap()];
    assert!(bad.encode().is_err());
    let mut bad = c.clone();
    bad.nodes[0].incarnation = c.nodes[1].incarnation;
    assert!(bad.encode().is_err());
    let mut heterogeneous = c.clone();
    heterogeneous.nodes[1].operation_slots = 5;
    Control::decode(&heterogeneous.encode().unwrap()).unwrap();
    let mut bad = heterogeneous.clone();
    bad.nodes[1].operation_slots = 0;
    assert!(bad.encode().is_err());
    bad.nodes[1].operation_slots = 257;
    assert!(bad.encode().is_err());
    stop(&mut c, 1, 20000);
    c.intents.last_mut().unwrap().incarnation_id = c.nodes[1].incarnation;
    c.validate().unwrap();
    let mut bad = c.clone();
    bad.intents.last_mut().unwrap().incarnation_id = Some(id(42));
    assert!(bad.encode().is_err());
    let mut ext = root();
    ext["provisioning"] = json!("external");
    ext["config"]["adapter_contract"] = Value::Null;
    let mut node = serde_json::to_value(&c.nodes[1]).unwrap();
    node["ownership"] = json!("external");
    node["launch_id"] = Value::Null;
    node["resource_id"] = json!(format!("external:{}", c.nodes[1].node_id));
    ext["nodes"] = json!([node]);
    let e = decode(&ext).unwrap();
    assert_eq!(e.capacity().unwrap().credits, 0);
    ext["intents"] = json!([c.intents.last().unwrap()]);
    assert!(decode(&ext).is_err());
}
#[test]
fn bounds_and_diagnostics_are_sanitized() {
    assert_eq!(MAX_CONTROL_BYTES, 32 * 1024 * 1024);
    assert_eq!(
        Control::decode(&vec![b' '; MAX_CONTROL_BYTES + 1]),
        Err(ControlError::LimitExceeded)
    );
    let mut c = fleet(1);
    c.name = "PRIVATE-SENTINEL".into();
    let err = c.encode().unwrap_err();
    assert_eq!(err.to_string(), "invalid control record");
    let debug = format!(
        "{c:?} {:?} {:?} {:?} {:?}",
        c.config, c.policy, c.controller, c.scheduler
    );
    assert!(!debug.contains("PRIVATE"));
    assert!(!debug.contains("resource"));
    let mut v = root();
    v["nodes"] = json!(vec![
        serde_json::to_value(&fleet(1).nodes[0]).unwrap();
        2049
    ]);
    assert!(decode(&v).is_err());
    let mut c = fleet(1);
    let i = c.intents[0].clone();
    c.intents = vec![i; 3073];
    assert_eq!(c.encode(), Err(ControlError::LimitExceeded));
}

#[test]
fn policy_lease_nested_strictness_and_template_hash_use_real_codecs() {
    let mut v = root();
    v["policy"] =
        json!({"revision":u64::MAX,"mode":"autoscale","desired":5,"min_nodes":1,"max_nodes":1024});
    v["controller"] =
        json!({"owner_id":id(5),"epoch":u64::MAX,"lease_state":"held","lease_until_ms":0});
    let c = decode(&v).unwrap();
    c.validate().unwrap();
    for (pointer, value) in [
        ("/policy/min_nodes", json!(0)),
        ("/policy/max_nodes", json!(1025)),
        ("/controller/epoch", json!(0)),
        ("/controller/lease_until_ms", Value::Null),
        ("/controller/owner_id", Value::Null),
        ("/provisioning", json!("external")),
    ] {
        let mut bad = v.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(decode(&bad).is_err());
        let c: Control = serde_json::from_value(bad).unwrap();
        assert!(c.encode().is_err());
    }
    let mut changed = c.clone();
    changed.template.queue_size += 1;
    assert!(changed.encode().is_err());
    changed.config.template_fingerprint = changed.template.fingerprint().unwrap();
    changed.validate().unwrap();
    for (pointer, bad) in [
        ("/policy/mode", json!({"autoscale":null})),
        ("/controller/lease_state", json!("unknown")),
        (
            "/controller/owner_id",
            json!("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"),
        ),
    ] {
        let mut bad_root = v.clone();
        *bad_root.pointer_mut(pointer).unwrap() = bad;
        assert!(decode(&bad_root).is_err());
    }
    let mut c = fleet(1);
    c.nodes[0].incarnation = Some(id(123));
    c.nodes[0].endpoint = Some("http://worker/".into());
    c.nodes[0].registration = nabedra_database_server::cluster::member::Registration::Registered;
    c.nodes[0].session = Some(nabedra_database_server::cluster::member::Session {
        id: id(124),
        verifier: "d".repeat(64),
    });
    c.nodes[0].drain = Some(nabedra_database_server::cluster::member::Drain {
        id: id(125),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    c.intents[0].delivery = Some(nabedra_database_server::cluster::member::Delivery {
        invocation_id: id(126),
        owner_id: id(127),
        epoch: 1,
        command_sha256: "e".repeat(64),
    });
    let original = serde_json::to_value(&c).unwrap();
    for (pointer, key) in [
        ("/nodes/0/session", "verifier"),
        ("/nodes/0/drain", "ack_seq"),
        ("/intents/0/delivery", "command_sha256"),
        ("/intents/0", "outcome"),
        ("/nodes/0", "resource_id"),
    ] {
        let mut missing = original.clone();
        missing
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(decode(&missing).is_err());
        let mut unknown = original.clone();
        unknown
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(null));
        assert!(decode(&unknown).is_err());
        let value = &original.pointer(pointer).unwrap()[key];
        let needle = format!("\"{key}\":{value}");
        let raw = original
            .to_string()
            .replacen(&needle, &format!("{needle},{needle}"), 1);
        assert!(Control::decode(raw.as_bytes()).is_err());
    }
}

#[test]
fn generation_two_requires_worker_semantic_identity() {
    let good = root();
    let c = decode(&good).unwrap();
    assert_eq!(c.version, 1);
    assert_eq!(c.config.generation, 2);
    c.validate().unwrap();
    assert_eq!(Control::decode(&c.encode().unwrap()).unwrap(), c);
    let constructed: Control = serde_json::from_value(good.clone()).unwrap();
    constructed.validate().unwrap();
    assert_eq!(Control::decode(&constructed.encode().unwrap()).unwrap(), c);
    let config = serde_json::to_vec(&good["config"]).unwrap();
    let config = nabedra_database_server::cluster::state::Config::decode(&config).unwrap();
    config.validate().unwrap();
    assert_eq!(
        nabedra_database_server::cluster::state::Config::decode(&config.encode().unwrap()).unwrap(),
        config
    );

    for generation in [0, 1, 3, u64::MAX] {
        let mut bad = good.clone();
        bad["config"]["generation"] = json!(generation);
        assert!(decode(&bad).is_err());
        let constructed: Control = serde_json::from_value(bad.clone()).unwrap();
        assert!(constructed.validate().is_err());
        assert!(constructed.encode().is_err());
        if generation == 1 {
            bad["config"]
                .as_object_mut()
                .unwrap()
                .remove("worker_semantic_fingerprint");
            assert!(decode(&bad).is_err());
        }
    }
    let mut missing = good.clone();
    missing["config"]
        .as_object_mut()
        .unwrap()
        .remove("worker_semantic_fingerprint");
    assert!(decode(&missing).is_err());
    let mut null = good.clone();
    null["config"]["worker_semantic_fingerprint"] = Value::Null;
    assert!(decode(&null).is_err());
    for digest in [
        "".into(),
        "d".repeat(63),
        "d".repeat(65),
        "g".repeat(64),
        "D".repeat(64),
    ] {
        let mut bad = good.clone();
        bad["config"]["worker_semantic_fingerprint"] = json!(digest);
        assert!(decode(&bad).is_err());
        let constructed: Control = serde_json::from_value(bad).unwrap();
        assert!(constructed.validate().is_err());
        assert!(constructed.encode().is_err());
        assert!(constructed.config.validate().is_err());
        assert!(constructed.config.encode().is_err());
    }
    // Preserve duplicates in original bytes, before any Value-map decoding.
    let needle = format!("\"worker_semantic_fingerprint\":\"{}\"", "d".repeat(64));
    let raw = good.to_string();
    assert!(raw.contains(&needle));
    let duplicate = raw.replacen(&needle, &format!("{needle},{needle}"), 1);
    assert!(Control::decode(duplicate.as_bytes()).is_err());
}
