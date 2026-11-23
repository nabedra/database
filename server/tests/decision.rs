#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::storage::{Catalog, ClusterCatalog};

#[test]
fn cluster_catalog_requires_head_and_keeps_standalone_separate() {
    let legacy = Catalog::empty();
    let cluster = ClusterCatalog::from_catalog(&legacy).unwrap();
    let bytes = cluster.encode().unwrap();
    assert_eq!(ClusterCatalog::decode(&bytes).unwrap(), cluster);
    assert!(Catalog::decode(&bytes).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value.as_object_mut().unwrap().remove("decision_head");
    assert!(ClusterCatalog::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}

use nabedra_database_server::{
    protocol::{MAX_BODY_BYTES, MAX_CATALOG_BYTES},
    storage::{Decision, MAX_DECISION_BYTES, StorageError},
};
use serde_json::{Value, json};
use uuid::Uuid;
const ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const TABLE: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
fn record() -> Value {
    json!({"version":1,"id":TABLE,"parent":null,"base_revision":0,"revision":1,"request_id":ID,
        "request_body":format!(" \n{{\"payload\":{{\"table_id\":\"{TABLE}\",\"row\":{{\"id\":1.00e0}}}},\"operation\":\"table.insert\",\"request_id\":\"{ID}\"}} \t"),
        "response_status":200,"response_body":format!(" \n{{\"request_id\":\"{ID}\",\"data\":null}} \t")})
}
fn decode(v: &Value) -> Result<Decision, StorageError> {
    Decision::decode(&serde_json::to_vec(v).unwrap())
}
#[test]
fn exact_bodies_noops_and_terminal_errors_roundtrip() {
    let original = record();
    let d = decode(&original).unwrap();
    assert_eq!(d.request_body, original["request_body"].as_str().unwrap());
    assert_eq!(d.response_body, original["response_body"].as_str().unwrap());
    assert_eq!(Decision::decode(&d.encode().unwrap()).unwrap(), d);
    assert!(!format!("{d:?}").contains("request_body"));
    let mut different = d.clone();
    different.request_body.push(' ');
    assert_ne!(different, d);
    assert!(different.encode().is_ok());
    let mut normalized = record();
    normalized["request_body"] = normalized["request_body"]
        .as_str()
        .unwrap()
        .replace(ID, &ID.to_uppercase())
        .into();
    assert_eq!(decode(&normalized).unwrap().request_id, d.request_id);
    for data in ["null", "false", "0", "1.00e0"] {
        let mut v = record();
        v["response_body"] = format!(" {{\"data\":{data},\"request_id\":\"{ID}\"}} ").into();
        assert!(decode(&v).is_ok());
    }
    for (code, status) in [
        ("validation_error", 400),
        ("not_found", 404),
        ("conflict", 409),
        ("limit_exceeded", 413),
    ] {
        let mut v = record();
        v["response_status"] = status.into();
        v["response_body"] = format!("{{\"request_id\":\"{ID}\",\"error\":{{\"code\":\"{code}\",\"message\":\"operation failed\",\"outcome\":\"not_applied\"}}}}").into();
        assert!(decode(&v).is_ok(), "{code}");
    }
}
#[test]
fn rejects_nonterminal_uncorrelated_and_malformed_records() {
    for (field, bad) in [
        ("request_id", json!(TABLE)),
        ("parent", json!(TABLE)),
        ("revision", json!(0)),
        ("revision", json!(1.0)),
        ("version", json!(2)),
        ("response_status", json!(200.0)),
        (
            "request_body",
            json!(format!(
                "{{\"request_id\":\"{ID}\",\"operation\":\"table.list\",\"payload\":{{}}}}"
            )),
        ),
        ("request_body", json!("{}")),
        ("response_body", json!("[]")),
        (
            "response_body",
            json!(format!("{{\"request_id\":\"{TABLE}\",\"data\":null}}")),
        ),
        (
            "response_body",
            json!(format!(
                "{{\"request_id\":\"{ID}\",\"data\":null,\"extra\":0}}"
            )),
        ),
    ] {
        let mut v = record();
        v[field] = bad;
        assert!(decode(&v).is_err(), "{field}");
    }
    for (code, status) in [
        ("unauthorized", 401),
        ("unsupported_operation", 400),
        ("overloaded", 429),
        ("storage_unavailable", 503),
        ("outcome_unknown", 503),
        ("internal_error", 500),
        ("conflict", 409),
    ] {
        let mut v = record();
        v["response_status"] = status.into();
        v["response_body"]=format!("{{\"request_id\":\"{ID}\",\"error\":{{\"code\":\"{code}\",\"message\":\"x\",\"outcome\":\"unknown\"}}}}").into();
        assert!(decode(&v).is_err());
        let body = v["response_body"]
            .as_str()
            .unwrap()
            .replace("unknown\"", "not_applied\"");
        v["response_body"] = body.into();
        if code != "conflict" {
            assert!(decode(&v).is_err());
        }
    }
    for (field, bad) in [
        ("code", json!({"conflict": null})),
        ("outcome", json!({"not_applied": null})),
    ] {
        let mut response = json!({"request_id": ID, "error": {
            "code": "conflict", "message": "operation failed", "outcome": "not_applied"
        }});
        response["error"][field] = bad;
        let mut v = record();
        v["response_status"] = 409.into();
        v["response_body"] = serde_json::to_string(&response).unwrap().into();
        assert!(decode(&v).is_err(), "object-valued {field} accepted");
    }
    let mut v = record();
    v["base_revision"] = u64::MAX.into();
    v["revision"] = 0.into();
    assert!(decode(&v).is_err());
    v = record();
    v.as_object_mut().unwrap().remove("parent");
    assert!(decode(&v).is_err());
    let text = serde_json::to_string(&record()).unwrap();
    assert!(
        Decision::decode(
            text.replacen("\"version\":1", "\"version\":1,\"version\":1", 1)
                .as_bytes()
        )
        .is_err()
    );
    assert!(Decision::decode(text.replacen('{', "{\"extra\":0,", 1).as_bytes()).is_err());
    let mut d = decode(&record()).unwrap();
    d.response_status = 409;
    assert!(d.encode().is_err());
}
fn legacy() -> Catalog {
    Catalog::decode(format!(r#"{{"format_version":1,"revision":4,"commit_id":"{ID}","tables":{{"users":{{"id":"{TABLE}","schema":{{"id":"utf8"}},"key":"id","format":"parquet","snapshot":"tables/{TABLE}/{ID}.parquet","row_count":1}}}}}}"#).as_bytes()).unwrap()
}
#[test]
fn legacy_and_partition_manifest_validation_is_shared() {
    let v1 = legacy();
    let v2 = Catalog::decode(&v1.encode().unwrap()).unwrap();
    for c in [v1, v2] {
        let cluster = ClusterCatalog::from_catalog(&c).unwrap();
        assert_eq!(cluster.tables, c.tables);
        assert_eq!(cluster.commit_id, c.commit_id);
        assert_eq!(cluster.revision, c.revision);
    }
    // Same partition fixture shape as storage.rs, with a single full-range file.
    let mut v: Value = serde_json::from_slice(
        &ClusterCatalog::from_catalog(&legacy())
            .unwrap()
            .encode()
            .unwrap(),
    )
    .unwrap();
    v["tables"]["users"]["data"] = json!({"layout":"partitioned","files":[{"object":format!("tables/{TABLE}/{ID}/0.parquet"),"range":{"lower":null,"upper":null},"row_count":1,"byte_size":100,"payload_bytes":8}]});
    assert!(ClusterCatalog::decode(&serde_json::to_vec(&v).unwrap()).is_ok());
    for (field, bad) in [("row_count", json!(2)), ("byte_size", json!(0))] {
        let mut bad_doc = v.clone();
        bad_doc["tables"]["users"]["data"]["files"][0][field] = bad;
        assert!(ClusterCatalog::decode(&serde_json::to_vec(&bad_doc).unwrap()).is_err());
    }
    for (field, bad) in [
        ("tables", json!([])),
        ("revision", json!(1.0)),
        ("extra", json!(0)),
        ("format_version", json!(2)),
    ] {
        let mut bad_doc = v.clone();
        bad_doc[field] = bad;
        assert!(ClusterCatalog::decode(&serde_json::to_vec(&bad_doc).unwrap()).is_err());
    }
    let text = serde_json::to_string(&v).unwrap();
    for (a, b) in [
        (
            "\"decision_head\":null",
            "\"decision_head\":null,\"decision_head\":null",
        ),
        ("\"id\":\"utf8\"", "\"id\":\"utf8\",\"id\":\"utf8\""),
        ("\"lower\":null", "\"lower\":null,\"lower\":null"),
    ] {
        assert!(ClusterCatalog::decode(text.replace(a, b).as_bytes()).is_err());
    }
}
#[test]
fn independent_bounds_and_escaping_above_control_limit() {
    assert_eq!(MAX_DECISION_BYTES, 4 * MAX_BODY_BYTES + 64 * 1024);
    assert!(matches!(
        Decision::decode(&vec![b' '; MAX_DECISION_BYTES + 1]),
        Err(StorageError::LimitExceeded)
    ));
    assert!(matches!(
        ClusterCatalog::decode(&vec![b' '; MAX_CATALOG_BYTES + 1]),
        Err(StorageError::LimitExceeded)
    ));
    let mut d = decode(&record()).unwrap();
    d.request_body.push_str(&" ".repeat(MAX_BODY_BYTES));
    assert!(matches!(d.encode(), Err(StorageError::LimitExceeded)));
    d = decode(&record()).unwrap();
    d.response_body.push_str(&" ".repeat(MAX_BODY_BYTES));
    assert!(matches!(d.encode(), Err(StorageError::LimitExceeded)));
    let mut c = ClusterCatalog::from_catalog(&legacy()).unwrap();
    let entry = c.tables["users"].clone();
    for i in 0..6000u128 {
        let mut table = entry.clone();
        table.id = Uuid::from_u128(i);
        table.data = nabedra_database_server::storage::TableData::Legacy {
            snapshot: nabedra_database_server::storage::ObjectKey::snapshot(table.id, c.commit_id)
                .as_str()
                .into(),
        };
        c.tables.insert(format!("table_{i}"), table);
    }
    c.validate().unwrap();
    assert!(matches!(c.encode(), Err(StorageError::LimitExceeded)));
    // Valid JSON whitespace tabs need two bytes each in the outer JSON string.
    d = decode(&record()).unwrap();
    d.request_body
        .push_str(&"\t".repeat(MAX_BODY_BYTES - d.request_body.len()));
    d.response_body
        .push_str(&"\t".repeat(4096 - d.response_body.len()));
    let bytes = d.encode().unwrap();
    assert!(bytes.len() > 16 * 1024 * 1024);
    assert!(bytes.len() <= MAX_DECISION_BYTES);
    let decoded = Decision::decode(&bytes).unwrap();
    assert_eq!(decoded.request_body.len(), MAX_BODY_BYTES);
    assert!(
        decoded.request_body == d.request_body,
        "request body changed"
    );
    assert!(
        decoded.response_body == d.response_body,
        "response body changed"
    );
    assert_eq!(d.request_id, Uuid::parse_str(ID).unwrap());
}
