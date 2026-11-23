#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::storage::{
    ControlDecision, MAX_CONTROL_BODY_BYTES, MAX_CONTROL_DECISION_BYTES, StorageError,
};
use serde_json::{Value, json};
const ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const OTHER: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
fn request(op: &str, payload: &str) -> String {
    format!(r#" {{"request_id":"{ID}","operation":"{op}","payload":{payload}}} "#)
}
fn success(mode: &str, desired: u64, low: u64, high: u64) -> String {
    format!(
        r#"{{"request_id":"{ID}","data":{{"revision":7,"mode":"{mode}","desired":{desired},"min_nodes":{low},"max_nodes":{high}}}}}"#
    )
}
fn error(code: &str) -> String {
    format!(
        r#"{{"request_id":"{ID}","error":{{"code":"{code}","message":"sanitized","outcome":"not_applied"}}}}"#
    )
}
fn record() -> Value {
    json!({"version":1,"id":OTHER,"parent":null,"admin_seq":1,"cluster_id":ID,
        "base_root_revision":30,"published_root_revision":31,"request_id":ID,
        "request_body":request("cluster.scale", r#"{"nodes":2}"#),
        "response_status":200,"response_body":success("fixed",2,2,2)})
}
fn decode(v: &Value) -> Result<ControlDecision, StorageError> {
    ControlDecision::decode(&serde_json::to_vec(v).unwrap())
}
fn rejects(v: &Value) {
    assert!(decode(v).is_err(), "accepted invalid fixture");
}
#[test]
fn terminal_success_errors_and_independent_policy_revision() {
    for n in [0, 1024] {
        let mut boundary = record();
        boundary["request_body"] = json!(request("cluster.scale", &format!(r#"{{"nodes":{n}}}"#)));
        boundary["response_body"] = json!(success("fixed", n, n, n));
        decode(&boundary).unwrap();
    }
    let mut v = record();
    let d = decode(&v).unwrap();
    assert_eq!(ControlDecision::decode(&d.encode().unwrap()).unwrap(), d);
    v["admin_seq"] = json!(9);
    v["parent"] = json!(ID);
    decode(&v).unwrap(); // Same positive policy revision need not advance with admin/root.
    v["request_body"] = json!(request(
        "cluster.autoscale",
        r#"{"min_nodes":1,"max_nodes":4}"#
    ));
    v["response_body"] = json!(success("autoscale", 3, 1, 4));
    decode(&v).unwrap();
    v["response_status"] = json!(400);
    v["response_body"] = json!(error("unsupported_capability"));
    decode(&v).unwrap();
    for (code, status) in [
        ("validation_error", 400),
        ("conflict", 409),
        ("limit_exceeded", 413),
    ] {
        v["response_status"] = json!(status);
        v["response_body"] = json!(error(code));
        decode(&v).unwrap();
    }
    for payload in [r#"{"nodes":1025}"#, r#"{"nodes":18446744073709551615}"#] {
        v["request_body"] = json!(request("cluster.scale", payload));
        v["response_status"] = json!(400);
        v["response_body"] = json!(error("validation_error"));
        decode(&v).unwrap();
        v["response_body"] = json!(error("unsupported_capability"));
        rejects(&v);
        v["response_status"] = json!(200);
        v["response_body"] = json!(success("fixed", 2, 2, 2));
        rejects(&v);
    }
    for payload in [
        r#"{"min_nodes":0,"max_nodes":1}"#,
        r#"{"min_nodes":4,"max_nodes":2}"#,
        r#"{"min_nodes":1,"max_nodes":1025}"#,
    ] {
        v["request_body"] = json!(request("cluster.autoscale", payload));
        v["response_status"] = json!(400);
        v["response_body"] = json!(error("validation_error"));
        decode(&v).unwrap();
        v["response_status"] = json!(409);
        v["response_body"] = json!(error("conflict"));
        rejects(&v);
    }
}
#[test]
fn strict_original_nested_shapes_and_integer_tokens() {
    for payload in [
        r#"{"nodes":2,"nodes":2}"#,
        r#"{"nodes":2,"extra":0}"#,
        r#"[2]"#,
        r#"{"nodes":true}"#,
        r#"{"nodes":-0}"#,
        r#"{"nodes":-1}"#,
        r#"{"nodes":2.0}"#,
        r#"{"nodes":2e0}"#,
        r#"{"nodes":18446744073709551616}"#,
    ] {
        let mut v = record();
        v["request_body"] = json!(request("cluster.scale", payload));
        rejects(&v);
    }
    for (from, to) in [
        ("cluster.scale", "cluster.status"),
        ("cluster.scale", "table.insert"),
        ("cluster.scale", "unknown"),
        ("\"request_id\":", "\"request_id\":null,\"request_id\":"),
        ("\"cluster.scale\"", "{\"cluster.scale\":null}"),
        ("\"payload\":", "\"extra\":0,\"payload\":"),
        ("\"nodes\":2", "\"nodes\":2,\"nodes\":2"),
        (ID, OTHER),
    ] {
        let mut v = record();
        v["request_body"] = json!(v["request_body"].as_str().unwrap().replace(from, to));
        rejects(&v);
    }
    for (from, to) in [
        ("\"data\":", "\"data\":{},\"data\":"),
        ("\"revision\":7", "\"revision\":7,\"revision\":7"),
        ("\"revision\":7", "\"revision\":7e0"),
        ("\"revision\":7", "\"revision\":0"),
        ("\"fixed\"", "{\"fixed\":null}"),
        ("\"desired\":2", "\"desired\":3"),
        ("\"min_nodes\":2", "\"min_nodes\":1"),
        ("\"max_nodes\":2", "\"max_nodes\":3"),
        ("\"mode\":", "\"unknown\":0,\"mode\":"),
        (ID, OTHER),
    ] {
        let mut v = record();
        v["response_body"] = json!(v["response_body"].as_str().unwrap().replace(from, to));
        rejects(&v);
    }
    for key in ["request_body", "response_body"] {
        let mut v = record();
        let body = v[key].as_str().unwrap().to_string();
        for bad in [
            body.replace(ID, &ID.to_uppercase()),
            body.replace(ID, &ID.replace('-', "")),
            format!("{body} {{}}"),
            "[]".into(),
        ] {
            v[key] = json!(bad);
            rejects(&v);
        }
    }
    for data in ["[]", "null", r#"[7,"fixed",2,2,2]"#] {
        let mut v = record();
        v["response_body"] = json!(format!(r#"{{"request_id":"{ID}","data":{data}}}"#));
        rejects(&v);
    }
    for payload in [
        r#"{"min_nodes":1,"min_nodes":1,"max_nodes":4}"#,
        r#"{"min_nodes":1,"max_nodes":4,"extra":0}"#,
        r#"{"min_nodes":1e0,"max_nodes":4}"#,
    ] {
        let mut v = record();
        v["request_body"] = json!(request("cluster.autoscale", payload));
        v["response_body"] = json!(success("autoscale", 2, 1, 4));
        rejects(&v);
    }
    let mut v = record();
    v["request_body"] = json!(request(
        "cluster.autoscale",
        r#"{"min_nodes":1,"max_nodes":4}"#
    ));
    for body in [
        success("fixed", 2, 2, 2),
        success("autoscale", 5, 1, 4),
        success("autoscale", 2, 2, 4),
        success("autoscale", 2, 1, 3),
    ] {
        v["response_body"] = json!(body);
        rejects(&v);
    }
}
#[test]
fn error_allowlist_and_string_only_outcome() {
    for code in [
        "not_found",
        "authentication_error",
        "unauthorized",
        "overloaded",
        "storage_error",
        "internal_error",
        "outcome_unknown",
        "future",
    ] {
        let mut v = record();
        v["response_status"] = json!(400);
        v["response_body"] = json!(error(code));
        rejects(&v);
    }
    for (from, to) in [
        ("\"conflict\"", "{\"conflict\":null}"),
        ("\"not_applied\"", "{\"not_applied\":null}"),
        ("not_applied", "unknown"),
        ("\"sanitized\"", "{}"),
        ("\"code\":", "\"code\":\"conflict\",\"code\":"),
        ("\"message\":", "\"extra\":0,\"message\":"),
        (ID, OTHER),
        (&format!("\"{ID}\""), "null"),
    ] {
        let mut v = record();
        v["response_status"] = json!(409);
        v["response_body"] = json!(error("conflict").replace(from, to));
        rejects(&v);
    }
    for status in [100, 200, 400, 500, 599] {
        let mut v = record();
        v["response_status"] = json!(status);
        v["response_body"] = json!(error("conflict"));
        rejects(&v);
    }
}
#[test]
fn metadata_relationships_canonical_ids_and_encode_parity() {
    for (key, value) in [
        ("version", json!(2)),
        ("version", json!(1.0)),
        ("admin_seq", json!(0)),
        ("admin_seq", json!(2)),
        ("base_root_revision", json!(0)),
        ("base_root_revision", json!(u64::MAX)),
        ("published_root_revision", json!(32)),
        ("id", json!(OTHER.to_uppercase())),
        ("cluster_id", json!(ID.replace('-', ""))),
        ("request_id", json!(ID.to_uppercase())),
        ("extra", json!(0)),
    ] {
        let mut v = record();
        v[key] = value;
        rejects(&v);
    }
    let mut self_parent = record();
    self_parent["admin_seq"] = json!(2);
    self_parent["parent"] = json!(ID);
    decode(&self_parent).unwrap();
    self_parent["parent"] = self_parent["id"].clone();
    rejects(&self_parent);
    for key in record().as_object().unwrap().keys() {
        let mut missing = record();
        missing.as_object_mut().unwrap().remove(key);
        rejects(&missing);
    }
    let mut valid = record();
    valid["admin_seq"] = json!(u64::MAX);
    valid["parent"] = json!(ID);
    valid["base_root_revision"] = json!(u64::MAX - 1);
    valid["published_root_revision"] = json!(u64::MAX);
    decode(&valid).unwrap();
    valid["parent"] = json!(ID.to_uppercase());
    rejects(&valid);
    let mut v = record();
    v["parent"] = json!(ID);
    rejects(&v);
    v.as_object_mut().unwrap().remove("parent");
    rejects(&v);
    let raw = serde_json::to_string(&record()).unwrap();
    assert!(
        ControlDecision::decode(
            raw.replace("\"admin_seq\":1", "\"admin_seq\":1,\"admin_seq\":1")
                .as_bytes()
        )
        .is_err()
    );
    assert!(
        ControlDecision::decode(
            raw.replace("\"admin_seq\":1", "\"admin_seq\":1e0")
                .as_bytes()
        )
        .is_err()
    );
    assert!(ControlDecision::decode(b"[]").is_err());
    let d = decode(&record()).unwrap();
    let mut bad = d.clone();
    bad.response_body = error("conflict");
    assert!(bad.encode().is_err());
    bad = d.clone();
    bad.admin_seq = 0;
    assert!(bad.validate().is_err());
    bad = d;
    bad.request_body = "secret-provider-token".into();
    bad.response_body = "secret-provider-token".into();
    assert!(!format!("{bad:?}").contains("secret-provider-token"));
    let err = bad.encode().unwrap_err();
    assert_eq!(err, StorageError::InvalidCatalog);
    assert!(!format!("{err:?} {err}").contains("secret-provider-token"));
}
#[test]
fn exact_megabyte_bodies_outer_quoting_and_limits() {
    assert_eq!(MAX_CONTROL_BODY_BYTES, 1024 * 1024);
    assert_eq!(MAX_CONTROL_DECISION_BYTES, 4 * 1024 * 1024 + 64 * 1024);
    // Raw policy whitespace belongs to the 1 MiB envelope, not the small
    // standalone Policy codec's 4 KiB input budget.
    let mut padded = record();
    let body = success("fixed", 2, 2, 2).replace(
        "\"revision\":",
        &format!("{}\"revision\":", " ".repeat(8192)),
    );
    padded["response_body"] = json!(body);
    assert_eq!(decode(&padded).unwrap().response_body, body);
    let mut v = record();
    let req = request("cluster.scale", r#"{"nodes":2}"#)
        .replace("cluster.scale", r#"cluster.\u0073cale"#);
    let req = format!(
        "{}{}",
        req,
        " \t\n".repeat((MAX_CONTROL_BODY_BYTES - req.len()) / 3)
    );
    let req = format!("{}{}", req, " ".repeat(MAX_CONTROL_BODY_BYTES - req.len()));
    let mut resp = error("conflict").replace(
        "sanitized",
        &"\\\"".repeat((MAX_CONTROL_BODY_BYTES - error("conflict").len()) / 2),
    );
    resp.push_str(&" ".repeat(MAX_CONTROL_BODY_BYTES - resp.len()));
    v["request_body"] = json!(req);
    v["response_body"] = json!(resp);
    v["response_status"] = json!(409);
    let d = decode(&v).unwrap();
    let bytes = d.encode().unwrap();
    assert!(bytes.len() > 2 * 1024 * 1024);
    let decoded = ControlDecision::decode(&bytes).unwrap();
    assert_eq!(decoded.request_body, req);
    assert_eq!(decoded.response_body, resp);
    for key in ["request_body", "response_body"] {
        let mut bad = v.clone();
        bad[key] = json!(format!("{} ", bad[key].as_str().unwrap()));
        assert_eq!(decode(&bad).unwrap_err(), StorageError::LimitExceeded);
    }
    let mut bad = d;
    bad.request_body.push(' ');
    assert_eq!(bad.encode().unwrap_err(), StorageError::LimitExceeded);
    assert_eq!(
        ControlDecision::decode(&vec![b' '; MAX_CONTROL_DECISION_BYTES + 1]).unwrap_err(),
        StorageError::LimitExceeded
    );
}
