use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    state::{ControlError, LeaseState},
};
use serde_json::{Value, json};
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

fn base() -> Control {
    let mut c = fleet(2);
    stop(&mut c, 0, 20000);
    c.revision = 41;
    c.policy.revision = 7;
    c.admin_seq = 3;
    c.admin_head = Some(id(3));
    c.controller.owner_id = Some(id(4));
    c.controller.epoch = 9;
    c.controller.lease_state = LeaseState::Held;
    c.controller.lease_until_ms = Some(500);
    c.scheduler.cooldown_until_ms = 71;
    c.scheduler.backoff_until_ms = 72;
    c.scheduler.failures = 2;
    c.scheduler.low_since_ms = Some(70);
    Control::decode(&c.encode().unwrap()).unwrap()
}
fn request(op: &str, payload: &str) -> Vec<u8> {
    format!(
        r#"{{"request_id":"{}","operation":"cluster.{op}","payload":{payload}}}"#,
        id(90)
    )
    .into_bytes()
}
fn prepare(c: &Control, bytes: &[u8]) -> (Control, ControlDecision) {
    let before = c.encode().unwrap();
    let (next, d) = admin_candidate(c, id(90), bytes, id(91)).unwrap();
    assert_eq!(c.encode().unwrap(), before);
    assert_eq!(Control::decode(&next.encode().unwrap()).unwrap(), next);
    assert_eq!(ControlDecision::decode(&d.encode().unwrap()).unwrap(), d);
    assert_eq!(d.request_body.as_bytes(), bytes);
    assert_eq!(
        (d.id, d.parent, d.cluster_id, d.request_id),
        (id(91), c.admin_head, c.cluster_id, id(90))
    );
    assert_eq!(
        (d.base_root_revision, d.published_root_revision, d.admin_seq),
        (c.revision, c.revision + 1, c.admin_seq + 1)
    );
    let mut expected = c.clone();
    expected.policy = next.policy.clone();
    expected.revision += 1;
    expected.admin_seq += 1;
    expected.change_id = id(91);
    expected.admin_head = Some(id(91));
    assert_eq!(next, expected);
    assert_eq!(next.capacity().unwrap(), c.capacity().unwrap());
    let response: Value = serde_json::from_str(&d.response_body).unwrap();
    assert_eq!(response["request_id"], json!(id(90)));
    assert!(d.response_body.len() <= MAX_CONTROL_BODY_BYTES);
    if d.response_status == 200 {
        assert_eq!(
            response["data"],
            serde_json::to_value(&next.policy).unwrap()
        );
    }
    (next, d)
}
#[test]
fn fixed_auto_transitions_clamp_zero_and_max() {
    let mut c = base();
    for (op, payload, mode, desired, low, high) in [
        (
            "scale",
            r#"{"nodes":1024}"#,
            PolicyMode::Fixed,
            1024,
            1024,
            1024,
        ),
        (
            "autoscale",
            r#"{"min_nodes":2,"max_nodes":10}"#,
            PolicyMode::Autoscale,
            10,
            2,
            10,
        ),
        ("scale", r#"{"nodes":0}"#, PolicyMode::Fixed, 0, 0, 0),
        (
            "autoscale",
            r#"{"min_nodes":1,"max_nodes":1024}"#,
            PolicyMode::Autoscale,
            1,
            1,
            1024,
        ),
        ("scale", r#"{"nodes":4}"#, PolicyMode::Fixed, 4, 4, 4),
        (
            "autoscale",
            r#"{"min_nodes":1,"max_nodes":10}"#,
            PolicyMode::Autoscale,
            4,
            1,
            10,
        ),
    ] {
        // Each preparation is independent; runtime supplies globally fresh IDs.
        c.change_id = id(2);
        c.admin_head = Some(id(3));
        let revision = c.policy.revision;
        let (n, d) = prepare(&c, &request(op, payload));
        assert_eq!(d.response_status, 200);
        assert_eq!(
            (
                n.policy.mode,
                n.policy.desired,
                n.policy.min_nodes,
                n.policy.max_nodes,
                n.policy.revision
            ),
            (mode, desired, low, high, revision + 1)
        );
        c = n;
    }
}
#[test]
fn identical_policy_keeps_its_independent_revision_even_at_max() {
    for auto in [false, true] {
        let mut c = base();
        c.policy.revision = u64::MAX;
        let bytes = if auto {
            c.policy.mode = PolicyMode::Autoscale;
            c.policy.desired = 4;
            c.policy.min_nodes = 2;
            c.policy.max_nodes = 8;
            request("autoscale", r#"{"min_nodes":2,"max_nodes":8}"#)
        } else {
            request("scale", r#"{"nodes":0}"#)
        };
        let (n, d) = prepare(&c, &bytes);
        assert_eq!(n.policy, c.policy);
        assert_eq!(d.response_status, 200);
    }
    // Bounds or mode alone are effective changes even when desired is unchanged.
    let mut c = base();
    c.policy.mode = PolicyMode::Autoscale;
    c.policy.desired = 4;
    c.policy.min_nodes = 2;
    c.policy.max_nodes = 8;
    let (n, _) = prepare(
        &c,
        &request("autoscale", r#"{"min_nodes":1,"max_nodes":8}"#),
    );
    assert_eq!(n.policy.revision, 8);
    let (n, _) = prepare(&c, &request("scale", r#"{"nodes":4}"#));
    assert_eq!(n.policy.revision, 8);
}
#[test]
fn external_fixed_goals_and_auto_capability() {
    let mut c = decode(&root()).unwrap();
    c.provisioning = Provisioning::External;
    c.config.adapter_contract = None;
    let (n, d) = prepare(&c, &request("scale", r#"{"nodes":1024}"#));
    assert_eq!(n.policy.desired, 1024);
    assert_eq!(d.response_status, 200);
    let (n, d) = prepare(
        &c,
        &request("autoscale", r#"{"min_nodes":1,"max_nodes":5}"#),
    );
    assert_eq!(n.policy, c.policy);
    assert_eq!(d.response_status, 400);
    let r: Value = serde_json::from_str(&d.response_body).unwrap();
    assert_eq!(r["error"]["code"], "unsupported_capability");
}
#[test]
fn typed_invalid_bounds_are_deterministic_terminal_errors_before_capability() {
    for external in [false, true] {
        let mut c = decode(&root()).unwrap();
        if external {
            c.provisioning = Provisioning::External;
            c.config.adapter_contract = None;
        }
        c.policy.revision = u64::MAX;
        for (op, p) in [
            ("scale", r#"{"nodes":1025}"#),
            ("scale", r#"{"nodes":18446744073709551615}"#),
            ("autoscale", r#"{"min_nodes":0,"max_nodes":5}"#),
            ("autoscale", r#"{"min_nodes":5,"max_nodes":4}"#),
            ("autoscale", r#"{"min_nodes":1,"max_nodes":1025}"#),
        ] {
            let bytes = request(op, p);
            let (n, d) = prepare(&c, &bytes);
            assert_eq!(n.policy, c.policy);
            assert_eq!(d.response_status, 400);
            let r: Value = serde_json::from_str(&d.response_body).unwrap();
            assert_eq!(r["error"]["code"], "validation_error");
            assert_eq!(r["error"]["outcome"], "not_applied");
            assert_eq!(admin_candidate(&c, id(90), &bytes, id(91)).unwrap(), (n, d));
        }
    }
}
#[test]
fn exact_original_whitespace_escapes_and_boundary_size() {
    let bytes = format!(
        r#" {{ "request_id":"{}", "operation":"cluster.\u0073cale", "payload":{{"no\u0064es":0}} }} "#,
        id(90)
    );
    let mut bytes = bytes.into_bytes();
    bytes.resize(MAX_CONTROL_BODY_BYTES, b' ');
    prepare(&base(), &bytes);
    bytes.push(b' ');
    assert_eq!(
        admin_candidate(&base(), id(90), &bytes, id(91)),
        Err(StorageError::LimitExceeded)
    );
}
#[test]
fn malformed_original_envelopes_never_become_decisions() {
    let c = base();
    let valid = String::from_utf8(request("scale", r#"{"nodes":0}"#)).unwrap();
    for bad in [
        "true",
        "-1",
        "1.0",
        "1e0",
        "18446744073709551616",
        "null",
        "[]",
    ] {
        let bytes = request("scale", &format!(r#"{{"nodes":{bad}}}"#));
        assert_eq!(
            admin_candidate(&c, id(90), &bytes, id(91)),
            Err(StorageError::InvalidCatalog)
        );
    }
    let alpha = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let canonical = valid.replace(&id(90).to_string(), &alpha.to_string());
    admin_candidate(&c, alpha, canonical.as_bytes(), id(91)).unwrap();
    assert_eq!(
        admin_candidate(
            &c,
            alpha,
            canonical
                .replace(&alpha.to_string(), &alpha.to_string().to_uppercase())
                .as_bytes(),
            id(91)
        ),
        Err(StorageError::InvalidCatalog)
    );
    for bad in [
        "[]".to_owned(),
        valid.replace(r#""nodes":0"#, r#""nodes":0,"nodes":0"#),
        valid.replace(r#""nodes":0"#, r#""nodes":0,"extra":0"#),
        valid.replace(r#""nodes":0"#, ""),
        valid.replace(
            r#""operation":"cluster.scale""#,
            r#""operation":"cluster.scale","operation":"cluster.scale""#,
        ),
        valid.replace("cluster.scale", "cluster.status"),
        valid.replace(&id(90).to_string(), &id(89).to_string()),
        format!("{valid} false"),
        valid.replace(r#"{"nodes":0}"#, r#"[0]"#),
        valid.replace(
            r#"{"nodes":0}"#,
            &format!("{}0{}", "[".repeat(200), "]".repeat(200)),
        ),
    ] {
        assert_eq!(
            admin_candidate(&c, id(90), bad.as_bytes(), id(91)),
            Err(StorageError::InvalidCatalog),
            "{bad}"
        );
    }
}
#[test]
fn freshness_invalid_base_and_each_counter_exhaustion() {
    let c = base();
    let bytes = request("scale", r#"{"nodes":1}"#);
    for proposal in [c.change_id, c.admin_head.unwrap()] {
        assert_eq!(
            admin_candidate(&c, id(90), &bytes, proposal),
            Err(StorageError::InvalidCatalog)
        );
    }
    for counter in 0..3 {
        let mut c = c.clone();
        match counter {
            0 => c.revision = u64::MAX,
            1 => c.admin_seq = u64::MAX,
            _ => c.policy.revision = u64::MAX,
        };
        c.encode().unwrap();
        let before = c.clone();
        assert_eq!(
            admin_candidate(&c, id(90), &bytes, id(91)),
            Err(StorageError::LimitExceeded)
        );
        assert_eq!(c, before);
    }
    let mut bad = c;
    bad.policy.revision = 0;
    assert_eq!(
        admin_candidate(&bad, id(90), &bytes, id(91)),
        Err(StorageError::InvalidCatalog)
    );
}
#[test]
fn saturated_retained_root_accepts_policy_without_spending_credits() {
    let mut c = fleet(1024);
    for k in 0..1026 {
        stop(&mut c, 0, 20000 + k);
    }
    assert_eq!(c.check_new_admission(), Err(ControlError::LimitExceeded));
    assert_eq!(c.capacity().unwrap().credits, 1023);
    prepare(&c, &request("scale", r#"{"nodes":1024}"#));
}
