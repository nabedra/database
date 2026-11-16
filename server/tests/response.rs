use nabedra_database_server::protocol;
#[path = "../src/api/response.rs"]
mod response;

use protocol::{ErrorCode, MAX_BODY_BYTES, Outcome, ProtocolError};
use response::{encode_error, encode_success};
use serde::{Serialize, Serializer};
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn ordinary_envelopes_preserve_framing_status_and_request_id() {
    let id = Uuid::nil();
    let encoded = encode_success(id, &json!({"text":"é\n\""}), false);
    assert_eq!(encoded.status, 200);
    assert_eq!(
        std::str::from_utf8(&encoded.body).unwrap(),
        format!(r#"{{"request_id":"{id}","data":{{"text":"é\n\""}}}}"#)
    );
    for request_id in [None, Some(id)] {
        let error = ProtocolError::new(ErrorCode::Conflict, "conflict\n\"é", Outcome::Unknown);
        let encoded = encode_error(request_id, error);
        assert_eq!(encoded.status, 409);
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded.body).unwrap(),
            json!({"request_id":request_id,"error":{
                "code":"conflict","message":"conflict\n\"é","outcome":"unknown"
            }})
        );
        assert!(!encoded.body.contains(&b'\n'));
    }
}

#[test]
fn limit_counts_complete_utf8_envelope_and_accepts_exact_boundary() {
    let id = Uuid::nil();
    let overhead = encode_success(id, &"", false).body.len();
    // Each unit needs five JSON bytes: two UTF-8 bytes, an escaped quote, and x.
    let available = MAX_BODY_BYTES - overhead;
    let mut data = "é\"x".repeat(available / 5);
    data.push_str(&"x".repeat(available % 5));
    for (mutation, outcome) in [(false, "not_applied"), (true, "unknown")] {
        let encoded = encode_success(id, &data, mutation);
        assert_eq!(encoded.status, 200);
        assert_eq!(encoded.body.len(), MAX_BODY_BYTES);
        let decoded: Value = serde_json::from_slice(&encoded.body).unwrap();
        assert_eq!(decoded["data"], data);
        let encoded = encode_success(id, &format!("{data}x"), mutation);
        assert_eq!(encoded.status, 413);
        assert!(encoded.body.len() < 512);
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded.body).unwrap(),
            json!({
                "request_id":id,"error":{"code":"limit_exceeded",
                "message":"response body exceeds 8 MiB","outcome":outcome}
            })
        );
    }
}

struct Fails;
impl Serialize for Fails {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private diagnostic and data"))
    }
}

#[test]
fn failures_are_sanitized_and_preserve_certainty() {
    let id = Uuid::nil();
    for (mutation, outcome) in [(false, "not_applied"), (true, "unknown")] {
        let encoded = encode_success(id, &Fails, mutation);
        assert_eq!(encoded.status, 500);
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded.body).unwrap(),
            json!({
                "request_id":id,"error":{"code":"internal_error",
                "message":"response encoding failed","outcome":outcome}
            })
        );
    }
    for request_id in [None, Some(id)] {
        for outcome in [Outcome::NotApplied, Outcome::Unknown] {
            let encoded = encode_error(
                request_id,
                ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "private\n".repeat(MAX_BODY_BYTES / 8),
                    outcome,
                ),
            );
            assert_eq!(encoded.status, 413);
            assert!(encoded.body.len() < 512);
            assert_eq!(
                serde_json::from_slice::<Value>(&encoded.body).unwrap(),
                json!({
                    "request_id":request_id,"error":{"code":"limit_exceeded",
                    "message":"response body exceeds 8 MiB","outcome":outcome}
                })
            );
        }
    }
}
