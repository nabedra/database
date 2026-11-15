use meshdb_server::cluster::member::{Heartbeat, Intent, MemberError, Node};
use serde_json::{Value, json};

const ID: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";
fn node() -> Value {
    json!({"node_id":ID,"launch_id":ID,"resource_id":null,"ownership":"managed",
        "state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,
        "operation_slots":4,"partition_slots":4,"queue_size":0,"registration":"unregistered",
        "session":null,"drain":null})
}
fn intent() -> Value {
    json!({"request_id":ID,"node_id":ID,"launch_id":ID,"incarnation_id":null,
        "resource_id":null,"action":"start","state":"pending","attempts":0,
        "created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,
        "template_fingerprint":"a".repeat(64),"delivery":null,"outcome":null})
}
fn heartbeat() -> Value {
    json!({"version":1,"cluster_id":ID,"node_id":ID,"incarnation":OTHER,"sequence":1,
        "operation_slots":4,"partition_slots":4,"queue_size":2,"running_operations":4,
        "running_partitions":4,"queued_operations":2,"queued_partitions":2,
        "drain_id":null,"accepting":false})
}
fn bytes(v: &Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap()
}
fn registered() -> Value {
    let mut n = node();
    n["incarnation"] = json!(OTHER);
    n["endpoint"] = json!("https://[::1]:443/worker");
    n["session"] = json!({"id":ID,"verifier":"b".repeat(64)});
    n["resource_id"] = json!("provider/resource");
    n["registration"] = json!("verified");
    n
}
#[test]
fn lifecycle_records_roundtrip_without_discarding_recovery_state() {
    let n = Node::decode(&bytes(&node())).unwrap();
    assert_eq!(Node::decode(&n.encode().unwrap()).unwrap(), n);
    for state in ["ready", "draining", "lost", "stopped"] {
        let mut v = registered();
        v["state"] = json!(state);
        v["drain"] = json!({"id":ID,"requested_at_ms":u64::MAX,"ack_seq":1,"confirmed_idle":true});
        let n = Node::decode(&bytes(&v)).unwrap();
        n.validate().unwrap();
        assert_eq!(Node::decode(&n.encode().unwrap()).unwrap(), n);
        let debug = format!("{n:?} {:?}", n.session);
        for secret in ["provider/resource", "https://", &"b".repeat(64)] {
            assert!(!debug.contains(secret));
        }
    }
    let mut external = node();
    external["ownership"] = json!("external");
    external["launch_id"] = Value::Null;
    external["resource_id"] = json!(format!("external:{ID}"));
    Node::decode(&bytes(&external)).unwrap();
    let mut i = intent();
    i["attempts"] = json!(7);
    Intent::decode(&bytes(&i)).unwrap();
    i["delivery"] =
        json!({"invocation_id":ID,"owner_id":OTHER,"epoch":1,"command_sha256":"c".repeat(64)});
    for state in ["in_progress", "reconciling"] {
        i["state"] = json!(state);
        i["error_code"] = json!("provider:timeout");
        let record = Intent::decode(&bytes(&i)).unwrap();
        assert_eq!(Intent::decode(&record.encode().unwrap()).unwrap(), record);
    }
    i["state"] = json!("completed");
    i["outcome"] = json!("resource_known");
    i["resource_id"] = json!("resource");
    Intent::decode(&bytes(&i)).unwrap();
    i["action"] = json!("stop");
    i["request_id"] = json!(OTHER);
    i["template_fingerprint"] = Value::Null;
    i["outcome"] = json!("terminated");
    Intent::decode(&bytes(&i)).unwrap();
    i["state"] = json!("failed");
    i["outcome"] = json!("not_invoked");
    Intent::decode(&bytes(&i)).unwrap();
    let h = Heartbeat::decode(&bytes(&heartbeat())).unwrap();
    assert_eq!(Heartbeat::decode(&h.encode().unwrap()).unwrap(), h);
}
#[test]
fn strict_shapes_required_nulls_and_numeric_spellings() {
    for field in [
        "launch_id",
        "resource_id",
        "incarnation",
        "endpoint",
        "session",
        "drain",
    ] {
        let mut n = node();
        n.as_object_mut().unwrap().remove(field);
        assert!(Node::decode(&bytes(&n)).is_err(), "{field}");
    }
    for field in [
        "incarnation_id",
        "resource_id",
        "error_code",
        "template_fingerprint",
        "delivery",
        "outcome",
    ] {
        let mut i = intent();
        i.as_object_mut().unwrap().remove(field);
        assert!(Intent::decode(&bytes(&i)).is_err(), "{field}");
    }
    for field in ["session", "drain"] {
        let mut n = registered();
        n[field] = json!({"id":ID,"unexpected":null});
        assert!(Node::decode(&bytes(&n)).is_err());
    }
    let mut claimed = intent();
    claimed["delivery"] =
        json!({"invocation_id":ID,"owner_id":OTHER,"epoch":1,"command_sha256":"c".repeat(64)});
    let raw = String::from_utf8(bytes(&claimed)).unwrap();
    assert!(
        Intent::decode(
            raw.replace("\"epoch\":1", "\"epoch\":1,\"epoch\":1")
                .as_bytes()
        )
        .is_err()
    );
    claimed["delivery"]["extra"] = json!(0);
    assert!(Intent::decode(&bytes(&claimed)).is_err());
    let mut h = heartbeat();
    h.as_object_mut().unwrap().remove("drain_id");
    assert!(Heartbeat::decode(&bytes(&h)).is_err());
    for raw in ["[]", "null", "true", "{}"] {
        assert!(Node::decode(raw.as_bytes()).is_err());
    }
    for spelling in ["true", "1.0", "1e0", "-0", "18446744073709551616"] {
        let raw = String::from_utf8(bytes(&heartbeat()))
            .unwrap()
            .replace("\"sequence\":1", &format!("\"sequence\":{spelling}"));
        assert!(Heartbeat::decode(raw.as_bytes()).is_err(), "{spelling}");
    }
    let mut n = registered();
    for session in [
        json!([]),
        json!({"id":ID,"verifier":"b".repeat(64),"extra":0}),
    ] {
        n["session"] = session;
        assert!(Node::decode(&bytes(&n)).is_err());
    }
    let raw = String::from_utf8(bytes(&registered())).unwrap();
    for bad in [
        raw.replace("\"verifier\":", "\"id\":null,\"verifier\":"),
        raw.replace("\"node_id\":", "\"node_id\":null,\"node_id\":"),
    ] {
        assert!(Node::decode(bad.as_bytes()).is_err());
    }
    for (field, value) in [
        ("ownership", json!({"managed":null})),
        ("node_id", json!("AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")),
        ("version", json!(1)),
    ] {
        let mut n = node();
        n[field] = value;
        assert!(Node::decode(&bytes(&n)).is_err());
    }
}
#[test]
fn local_invariants_reject_inconsistent_candidates_on_decode_and_encode() {
    for (field, value) in [
        ("launch_id", Value::Null),
        ("state", json!("ready")),
        ("operation_slots", json!(0)),
        ("partition_slots", json!(257)),
        ("queue_size", json!(4097)),
        ("incarnation", json!(OTHER)),
        ("resource_id", json!("bad\nresource")),
    ] {
        let mut n = node();
        n[field] = value;
        assert!(Node::decode(&bytes(&n)).is_err(), "{field}");
    }
    for list in [json!([OTHER]), json!([ID, ID]), json!([OTHER, ID])] {
        let mut n = registered();
        n["retired_incarnations"] = list;
        assert!(Node::decode(&bytes(&n)).is_err());
    }
    for drain in [
        json!({"id":ID,"requested_at_ms":0,"ack_seq":0,"confirmed_idle":false}),
        json!({"id":ID,"requested_at_ms":0,"ack_seq":null,"confirmed_idle":true}),
    ] {
        let mut n = registered();
        n["drain"] = drain;
        assert!(Node::decode(&bytes(&n)).is_err());
    }
    for endpoint in [
        "http://host:0/",
        "http://host:65536/",
        "http://host:abc/",
        "http://host:/",
        "http://foo[bar]/",
        "https://foo[bar]:443/",
        "http://user@host/",
        "http://host/?q=1",
        "http://host/#x",
        "http:///",
        "ftp://host/",
        "http://höst/",
    ] {
        let mut n = registered();
        n["endpoint"] = json!(endpoint);
        assert!(Node::decode(&bytes(&n)).is_err(), "{endpoint}");
        let mut constructed = Node::decode(&bytes(&registered())).unwrap();
        constructed.endpoint = Some(endpoint.into());
        assert!(constructed.encode().is_err(), "{endpoint}");
        assert!(constructed.validate().is_err(), "{endpoint}");
    }
    for (field, value) in [
        ("request_id", json!(OTHER)),
        ("incarnation_id", json!(OTHER)),
        ("template_fingerprint", Value::Null),
        ("outcome", json!("not_invoked")),
        ("state", json!("completed")),
        ("state", json!("failed")),
        ("state", json!("in_progress")),
        ("error_code", json!("secret text")),
    ] {
        let mut i = intent();
        i[field] = value;
        assert!(Intent::decode(&bytes(&i)).is_err(), "{field}");
    }
    for (field, value) in [
        ("sequence", json!(0)),
        ("version", json!(2)),
        ("running_operations", json!(5)),
        ("running_partitions", json!(5)),
        ("queued_operations", json!(3)),
        ("queued_partitions", json!(3)),
        ("drain_id", json!(ID)),
    ] {
        let mut h = heartbeat();
        h[field] = value;
        if field == "drain_id" {
            h["accepting"] = json!(true);
        }
        assert!(Heartbeat::decode(&bytes(&h)).is_err(), "{field}");
    }
    let mut n = registered();
    n["session"]["verifier"] = json!("B".repeat(64));
    assert!(Node::decode(&bytes(&n)).is_err());
    let mut n = node();
    n["drain"] = json!({"id":ID,"requested_at_ms":0,"ack_seq":1,"confirmed_idle":true});
    assert!(Node::decode(&bytes(&n)).is_err());
    let mut stop = intent();
    stop["action"] = json!("stop");
    assert!(Intent::decode(&bytes(&stop)).is_err());
    let mut n = Node::decode(&bytes(&node())).unwrap();
    n.operation_slots = 0;
    assert!(n.encode().is_err());
    assert!(n.validate().is_err());
    let mut i = Intent::decode(&bytes(&intent())).unwrap();
    i.attempts = u64::MAX;
    i.error_code = Some("private diagnostic!".into());
    assert!(i.encode().is_err());
    let mut h = Heartbeat::decode(&bytes(&heartbeat())).unwrap();
    h.sequence = 0;
    assert!(h.encode().is_err());
}
#[test]
fn limits_allow_maximum_fields_and_bound_input_before_parsing() {
    let mut n = registered();
    n["resource_id"] = json!("é".repeat(128));
    n["endpoint"] = json!(format!("http://host/{}", "a".repeat(2048 - 12)));
    let retired: Vec<String> = (1..=16)
        .map(|v| format!("00000000-0000-4000-8000-{v:012x}"))
        .collect();
    n["retired_incarnations"] = json!(retired);
    n["operation_slots"] = json!(256);
    n["partition_slots"] = json!(256);
    n["queue_size"] = json!(4096);
    let record = Node::decode(&bytes(&n)).unwrap();
    assert!(record.encode().unwrap().len() <= 8192);
    n["retired_incarnations"]
        .as_array_mut()
        .unwrap()
        .push(json!(ID));
    assert!(Node::decode(&bytes(&n)).is_err());
    for endpoint in [
        "http://host:80/",
        "https://host:443/",
        "http://[::1]:80/",
        "http://host/worker@one",
    ] {
        let mut n = registered();
        n["endpoint"] = json!(endpoint);
        assert_eq!(
            Node::decode(&bytes(&n)).unwrap().endpoint.as_deref(),
            Some(endpoint)
        );
    }
    let mut i = intent();
    i["error_code"] = json!("a".repeat(256));
    i["resource_id"] = json!("\\".repeat(256));
    i["attempts"] = json!(u64::MAX);
    let record = Intent::decode(&bytes(&i)).unwrap();
    assert!(record.encode().unwrap().len() <= 4096);
    let mut padded = bytes(&node());
    padded.resize(8192, b' ');
    Node::decode(&padded).unwrap();
    padded.push(b' ');
    assert_eq!(
        Node::decode(&padded).unwrap_err(),
        MemberError::LimitExceeded
    );
    let mut padded = bytes(&intent());
    padded.resize(4096, b' ');
    Intent::decode(&padded).unwrap();
    padded.push(b' ');
    assert_eq!(
        Intent::decode(&padded).unwrap_err(),
        MemberError::LimitExceeded
    );
    let mut padded = bytes(&heartbeat());
    padded.resize(8192, b' ');
    Heartbeat::decode(&padded).unwrap();
    padded.push(b' ');
    assert_eq!(
        Heartbeat::decode(&padded).unwrap_err(),
        MemberError::LimitExceeded
    );
    assert!(Node::decode(&vec![b' '; 8193]).is_err());
    assert!(Intent::decode(&vec![b' '; 4097]).is_err());
    assert!(Heartbeat::decode(&vec![b' '; 8193]).is_err());
    let mut n = registered();
    n["resource_id"] = json!("a".repeat(257));
    assert!(Node::decode(&bytes(&n)).is_err());
    n = registered();
    n["endpoint"] = json!(format!("http://host/{}", "a".repeat(2037)));
    assert!(Node::decode(&bytes(&n)).is_err());
}
