use super::*;
#[test]
fn opaque_success_keeps_exact_data_and_checks_uuid() {
    let bytes = br#" {"request_id":"00000000-0000-0000-0000-000000000006","data": {"number":1.00,"x":[true]} } "#;
    let data = operation_success(bytes, Uuid::from_u128(6)).unwrap();
    assert_eq!(data.get(), r#"{"number":1.00,"x":[true]}"#);
    assert!(operation_success(bytes, Uuid::from_u128(7)).is_err());
}
use crate::cluster::configuration::{Template, Timing};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn pair(n: u8) -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[n; 32]).unwrap()
}
fn sign(key: &Ed25519KeyPair, prefix: &[u8], body: &[u8]) -> String {
    let mut input = prefix.to_vec();
    input.extend_from_slice(body);
    hex(key.sign(&input).as_ref())
}
fn sender(worker: bool) -> Value {
    if worker {
        json!({"role":"worker","process_id":id(4),"node_id":id(3),"incarnation":id(4),"session_id":id(5),"public_key":hex(pair(2).public_key().as_ref())})
    } else {
        json!({"role":"api","process_id":id(2),"node_id":null,"incarnation":null,"session_id":null,"public_key":hex(pair(1).public_key().as_ref())})
    }
}
fn root() -> Control {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    let public: [u8; 32] = pair(2).public_key().as_ref().try_into().unwrap();
    let verifier = hex(&worker_verifier(id(1), id(3), id(4), id(5), &public));
    let value = json!({"version":1,"cluster_id":id(1),"name":"Synthetic","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":verifier},"drain":null}],"intents":[]});
    let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
    Control::decode(&root.encode().unwrap()).unwrap()
}
// Construct canonical signing input independently in displayed wire order.
fn auth_text(scope: &str, path: &str, body: &[u8], seq: u64) -> String {
    let worker = matches!(scope, "job" | "dependency_open" | "dependency_close");
    let op = matches!(
        scope,
        "operation" | "job" | "dependency_open" | "dependency_close"
    );
    let s = sender(worker);
    let sender = format!(
        r#"{{"role":{},"process_id":{},"node_id":{},"incarnation":{},"session_id":{},"public_key":{}}}"#,
        s["role"],
        s["process_id"],
        s["node_id"],
        s["incarnation"],
        s["session_id"],
        s["public_key"]
    );
    format!(
        r#"{{"version":1,"cluster_id":"{}","scope":"{scope}","sender":{sender},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":{seq},"method":"POST","path":"{path}","operation_id":{},"admission_id":{},"job_id":{},"attempt_id":{},"request_digest":{},"control_sha256":"{}","dependency_id":{}}}"#,
        id(1),
        id(3),
        id(4),
        id(5),
        if op { json!(id(6)) } else { Value::Null },
        if worker { json!(id(7)) } else { Value::Null },
        if scope == "job" {
            json!(id(8))
        } else {
            Value::Null
        },
        if scope == "job" {
            json!(id(9))
        } else {
            Value::Null
        },
        if op {
            json!(if scope == "operation" {
                hash(body)
            } else {
                "a".repeat(64)
            })
        } else {
            Value::Null
        },
        hash(body),
        if scope == "job" {
            json!(id(10))
        } else {
            Value::Null
        }
    )
}
fn actual(path: &str) -> ActualRoute {
    ActualRoute::new(
        "POST",
        path,
        if path.ends_with("/jobs") {
            Some("00000000-0000-0000-0000-00000000000a")
        } else {
            None
        },
    )
    .unwrap()
}
fn checked(text: &str, path: &str) -> Auth {
    Auth::decode(&hex(text.as_bytes()), &actual(path)).unwrap()
}
fn signed(text: &str, path: &str) -> SignedRequest {
    let seed = if text.contains(r#""role":"api""#) {
        1
    } else {
        2
    };
    verify_request(
        &hex(text.as_bytes()),
        &sign(&pair(seed), b"meshdb.http-request.v1\0", text.as_bytes()),
        &actual(path),
    )
    .unwrap()
}
fn context(scope: &str, path: &str, body: &[u8], seq: u64) -> RequestContext {
    RequestContext::for_send(
        checked(&auth_text(scope, path, body, seq), path),
        body.to_vec(),
        &root(),
    )
    .unwrap()
}
fn reply_text(ctx: &RequestContext, body: &[u8], status: u16) -> String {
    let s = sender(true);
    let sender = format!(
        r#"{{"role":{},"process_id":{},"node_id":{},"incarnation":{},"session_id":{},"public_key":{}}}"#,
        s["role"],
        s["process_id"],
        s["node_id"],
        s["incarnation"],
        s["session_id"],
        s["public_key"]
    );
    let original: Value = serde_json::from_slice(&ctx.request.canonical).unwrap();
    let receiver = &original["sender"];
    let receiver = format!(
        r#"{{"role":{},"process_id":{},"node_id":{},"incarnation":{},"session_id":{},"public_key":{}}}"#,
        receiver["role"],
        receiver["process_id"],
        receiver["node_id"],
        receiver["incarnation"],
        receiver["session_id"],
        receiver["public_key"]
    );
    format!(
        r#"{{"version":1,"cluster_id":"{}","request_sequence":{},"scope":{},"sender":{sender},"receiver":{receiver},"status":{status},"control_sha256":"{}"}}"#,
        id(1),
        ctx.auth().sequence(),
        original["scope"],
        hash(body)
    )
}
fn reply(ctx: &RequestContext, body: &[u8], status: u16) -> SignedReply {
    let text = reply_text(ctx, body, status);
    verify_reply(
        &hex(text.as_bytes()),
        &sign(&pair(2), b"meshdb.http-response.v1\0", text.as_bytes()),
        status,
        ctx,
    )
    .unwrap()
}
const OP: &[u8] = br#" {"request_id":"00000000-0000-0000-0000-000000000006","operation":"table.list","payload":{}} "#;
#[test]
fn exact_request_hash_claims_and_invocation_association() {
    let path = "/internal/v1/operations";
    let text = auth_text("operation", path, OP, 1);
    let a = signed(&text, path);
    let b = signed(&text, path);
    let content = check_request_content(&a, OP.to_vec()).unwrap();
    assert!(Arc::ptr_eq(&content.request, &a.request));
    assert!(!Arc::ptr_eq(&content.request, &b.request));
    assert_eq!(content.bytes.as_ref(), OP);
    assert_eq!(content.operation().unwrap().request_id, id(6));
    let mut changed = OP.to_vec();
    changed.push(b' ');
    assert!(check_request_content(&a, changed).is_err());
    let mismatch = OP.to_vec();
    let mismatch = String::from_utf8(mismatch)
        .unwrap()
        .replace(&id(6).to_string(), &id(11).to_string())
        .into_bytes();
    let s = signed(&auth_text("operation", path, &mismatch, 2), path);
    assert!(check_request_content(&s, mismatch).is_err());
    let c = context("operation", path, OP, 3);
    assert_eq!(c.initial_bytes(), OP);
    assert_eq!(
        c.auth().canonical_bytes().unwrap(),
        c.request.canonical.as_ref()
    );
    let bad = text.replace(&hash(OP), &"0".repeat(64));
    assert!(check_request_content(&signed(&bad, path), OP.to_vec()).is_err());
}
#[test]
fn reply_correlates_sequence_status_sender_scope_target_and_domain() {
    let body = format!(
        r#"{{"version":1,"challenge":"{}","initialized":true}}"#,
        id(11)
    )
    .into_bytes();
    let initial = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let c = context("readiness", "/internal/v1/readiness", &initial, 1);
    let text = reply_text(&c, &body, 200);
    let signature = sign(&pair(2), b"meshdb.http-response.v1\0", text.as_bytes());
    let s = verify_reply(&hex(text.as_bytes()), &signature, 200, &c).unwrap();
    let content = check_reply_content(&s, body.clone()).unwrap();
    assert!(Arc::ptr_eq(&content.reply, &s.reply));
    assert!(Arc::ptr_eq(&s.reply.request, &c.request));
    let other = context("readiness", "/internal/v1/readiness", &initial, 2);
    assert!(verify_reply(&hex(text.as_bytes()), &signature, 200, &other).is_err());
    assert!(verify_reply(&hex(text.as_bytes()), &signature, 201, &c).is_err());
    for changed in [
        text.replace("\"request_sequence\":1", "\"request_sequence\":2"),
        text.replace("\"scope\":\"readiness\"", "\"scope\":\"drain\""),
        text.replace(&id(2).to_string(), &id(22).to_string()),
        text.replace(&id(3).to_string(), &id(23).to_string()),
        text.replace(&id(5).to_string(), &id(25).to_string()),
        text.replace(
            &hex(pair(2).public_key().as_ref()),
            &hex(pair(3).public_key().as_ref()),
        ),
    ] {
        let key = if changed.contains(&hex(pair(3).public_key().as_ref())) {
            pair(3)
        } else {
            pair(2)
        };
        let sig = sign(&key, b"meshdb.http-response.v1\0", changed.as_bytes());
        assert!(verify_reply(&hex(changed.as_bytes()), &sig, 200, &c).is_err());
    }
    for prefix in [
        b"meshdb.http-response.v1".as_slice(),
        b"meshdb.http-response.v1\0\0",
        b"meshdb.http-request.v1\0",
    ] {
        assert!(
            verify_reply(
                &hex(text.as_bytes()),
                &sign(&pair(2), prefix, text.as_bytes()),
                200,
                &c
            )
            .is_err()
        );
    }
    let mut wrong = body.clone();
    wrong.push(b' ');
    assert!(check_reply_content(&s, wrong).is_err());
    let wrong = String::from_utf8(body)
        .unwrap()
        .replace(&id(11).to_string(), &id(12).to_string())
        .into_bytes();
    assert!(check_reply_content(&reply(&c, &wrong, 200), wrong).is_err());
    let again = reply(&c, &initial, 200);
    assert!(!Arc::ptr_eq(&again.reply, &s.reply));
}
#[test]
fn small_echoes_preserve_pending_and_idle_false() {
    for (scope, path, field, extra) in [
        (
            "dependency_open",
            "/internal/v1/dependencies/open",
            "permit_id",
            "",
        ),
        (
            "dependency_close",
            "/internal/v1/dependencies/close",
            "permit_id",
            ",\"closed\":false",
        ),
        (
            "drain",
            "/internal/v1/drain",
            "drain_id",
            ",\"barrier\":true,\"idle\":false,\"heartbeat_sequence\":1",
        ),
    ] {
        let initial = format!(r#"{{"version":1,"{field}":"{}"}}"#, id(11)).into_bytes();
        let c = context(scope, path, &initial, 1);
        let body = format!(r#"{{"version":1,"{field}":"{}"{extra}}}"#, id(11)).into_bytes();
        let content = check_reply_content(&reply(&c, &body, 200), body.clone()).unwrap();
        if scope == "dependency_close" {
            assert!(!content.close().unwrap().closed());
        }
        if scope == "drain" {
            let d = content.drain().unwrap();
            assert!(!d.idle());
            assert!(d.barrier());
            assert_eq!(d.heartbeat_sequence(), 1);
        }
        let wrong = String::from_utf8(body)
            .unwrap()
            .replace(&id(11).to_string(), &id(12).to_string())
            .into_bytes();
        assert!(check_reply_content(&reply(&c, &wrong, 200), wrong).is_err());
    }
}
#[test]
fn target_binding_rejects_missing_replaced_retired_and_cluster() {
    let initial = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let path = "/internal/v1/readiness";
    let text = auth_text("readiness", path, &initial, 1);
    for n in 0..5 {
        let mut r = root();
        match n {
            0 => r.nodes.clear(),
            1 => r.nodes[0].session.as_mut().unwrap().id = id(20),
            2 => r.nodes[0].incarnation = Some(id(21)),
            3 => {
                r.nodes[0].incarnation = Some(id(21));
                r.nodes[0].retired_incarnations.push(id(4));
            }
            _ => r.cluster_id = id(22),
        }
        assert!(RequestContext::for_send(checked(&text, path), initial.clone(), &r).is_err());
    }
}
#[test]
fn strict_error_envelopes_and_unattributed_operation_rejections() {
    let c = context("operation", "/internal/v1/operations", OP, 1);
    for outcome in ["not_applied", "unknown"] {
        for request in [json!(id(6)), Value::Null] {
            let bytes=serde_json::to_vec(&json!({"request_id":request,"error":{"code":"internal_error","message":"REMOTE DATA","outcome":outcome}})).unwrap();
            let result = check_reply_content(&reply(&c, &bytes, 500), bytes).unwrap();
            assert_eq!(result.is_unattributed_rejection(), request.is_null());
            assert_eq!(
                result.error().unwrap().outcome,
                if outcome == "unknown" {
                    Outcome::Unknown
                } else {
                    Outcome::NotApplied
                }
            );
            assert_eq!(format!("{result:?}"), "CheckedReplyContent");
        }
    }
    let good = format!(
        r#"{{"request_id":"{}","error":{{"code":"internal_error","message":"REMOTE DATA","outcome":"unknown"}}}}"#,
        id(6)
    );
    error_envelope(good.as_bytes(), 500).unwrap();
    for bad in [
        good.replace("\"request_id\":", "\"other\":"),
        good.replace(
            "\"code\":\"internal_error\"",
            "\"code\":{\"internal_error\":null}",
        ),
        good.replace("\"outcome\":\"unknown\"", "\"outcome\":{\"unknown\":null}"),
        good.replace("\"message\":", "\"message\":\"dup\",\"message\":"),
        format!("{good} null"),
        good.replace("internal_error", "bogus"),
        good.replace("unknown", "bogus"),
    ] {
        assert!(error_envelope(bad.as_bytes(), 500).is_err());
    }
    assert!(error_envelope(good.as_bytes(), 503).is_err());
    let bad = good
        .replace(&id(6).to_string(), &id(12).to_string())
        .into_bytes();
    assert!(check_reply_content(&reply(&c, &bad, 500), bad).is_err());
    for bad in [
        r#"{"request_id":null,"data":null}"#.to_string(),
        format!(r#"{{"request_id":"{}","data":0,"data":1}}"#, id(6)),
        format!(r#"{{"request_id":"{}","data":0,"extra":1}}"#, id(6)),
        format!(r#"{{"request_id":"{}","data":0}} false"#, id(6)),
    ] {
        assert!(operation_success(bad.as_bytes(), id(6)).is_err());
    }
    let deep = format!(
        r#"{{"request_id":"{}","data":{}0{}}}"#,
        id(6),
        "[".repeat(128),
        "]".repeat(128)
    );
    assert!(matches!(
        operation_success(deep.as_bytes(), id(6)),
        Err(Error::Limit)
    ));
}
// Independent digest implementation for advertised request/reply hashes.
fn hash(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex(&sha2::Sha256::digest(bytes))
}
fn job_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({"version":1,"cluster_id":id(1),"operation_id":id(6),"job_id":id(8),"attempt_id":id(9),"request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":id(15),"catalog_etag":"token"},"ordinal":0,"table":{"id":id(16),"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":format!("tables/{}/{}.parquet",id(16),id(17)),"row_count":1}},"key":1}}})).unwrap()
}
#[test]
fn jobs_keep_full_expectations_and_signed_claim_mismatches() {
    let path = "/internal/v1/jobs";
    let body = job_bytes();
    let text = auth_text("job", path, &body, 1);
    let signed = signed(&text, path);
    assert!(
        check_request_content(&signed, body.clone())
            .unwrap()
            .job()
            .is_some()
    );
    let c = context("job", path, &body, 2);
    let mut value: Value = serde_json::from_slice(&body).unwrap();
    value.as_object_mut().unwrap().remove("table");
    value.as_object_mut().unwrap().remove("task");
    value["result"] =
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":8,"found":false}});
    let result = serde_json::to_vec(&value).unwrap();
    assert!(
        check_reply_content(&reply(&c, &result, 200), result.clone())
            .unwrap()
            .job()
            .is_some()
    );
    // Same execution UUIDs, different complete expected task/file/snapshot/ordinal.
    for field in ["snapshot", "ordinal", "task"] {
        let mut changed: Value = serde_json::from_slice(&body).unwrap();
        match field {
            "snapshot" => changed["snapshot"]["catalog_revision"] = json!(3),
            "ordinal" => changed["ordinal"] = json!(1),
            _ => changed["task"]["payload"]["file"]["payload"]["row_count"] = json!(2),
        }
        let changed = serde_json::to_vec(&changed).unwrap();
        let other = context("job", path, &changed, 3);
        assert!(check_reply_content(&reply(&other, &result, 200), result.clone()).is_err());
    }
    for field in [
        "cluster_id",
        "operation_id",
        "job_id",
        "attempt_id",
        "request_digest",
    ] {
        let mut changed: Value = serde_json::from_slice(&body).unwrap();
        changed[field] = if field == "request_digest" {
            json!("b".repeat(64))
        } else {
            json!(id(30))
        };
        let changed = serde_json::to_vec(&changed).unwrap();
        let s = super::tests::signed(&auth_text("job", path, &changed, 4), path);
        assert!(check_request_content(&s, changed).is_err());
    }
    value["result"] =
        json!({"kind":"error","payload":{"code":"storage_unavailable","message":"DATA"}});
    let result = serde_json::to_vec(&value).unwrap();
    assert!(
        check_reply_content(&reply(&c, &result, 200), result)
            .unwrap()
            .job()
            .is_some()
    );
}
#[test]
fn different_execution_contexts_use_distinct_sequences() {
    let path = "/internal/v1/jobs";
    let body = job_bytes();
    let c = context("job", path, &body, 1);
    let result=br#"{"request_id":null,"error":{"code":"overloaded","message":"DATA","outcome":"not_applied"}}"#;
    let text = reply_text(&c, result, 429);
    let sig = sign(&pair(2), b"meshdb.http-response.v1\0", text.as_bytes());
    for field in ["operation_id", "admission_id", "attempt_id"] {
        let mut changed: Value = serde_json::from_slice(&body).unwrap();
        if field != "admission_id" {
            changed[field] = json!(id(40));
        }
        let changed = serde_json::to_vec(&changed).unwrap();
        let mut auth = auth_text("job", path, &changed, 2);
        if field == "admission_id" {
            auth = auth.replace(
                &format!(r#""admission_id":"{}""#, id(7)),
                &format!(r#""admission_id":"{}""#, id(40)),
            );
        }
        // Keep wire claims consistent with the altered CONTROL.
        if field != "admission_id" {
            auth = auth.replace(
                &format!(
                    r#""{field}":"{}""#,
                    if field == "operation_id" {
                        id(6)
                    } else {
                        id(9)
                    }
                ),
                &format!(r#""{field}":"{}""#, id(40)),
            );
        }
        let other = RequestContext::for_send(checked(&auth, path), changed, &root()).unwrap();
        assert!(verify_reply(&hex(text.as_bytes()), &sig, 429, &other).is_err());
    }
}
#[test]
fn bounds_have_valid_at_cap_controls_and_cap_plus_one_rejections() {
    for (scope, path, mut body, cap) in [
        (
            "operation",
            "/internal/v1/operations",
            OP.to_vec(),
            MAX_BODY_BYTES,
        ),
        ("job", "/internal/v1/jobs", job_bytes(), MAX_CONTROL_BYTES),
        (
            "readiness",
            "/internal/v1/readiness",
            format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes(),
            SMALL_CAP,
        ),
    ] {
        body.resize(cap, b' ');
        let text = auth_text(scope, path, &body, 1);
        let s = signed(&text, path);
        check_request_content(&s, body.clone()).unwrap();
        body.push(b' ');
        assert!(matches!(check_request_content(&s, body), Err(Error::Limit)));
    }
    let c = context("operation", "/internal/v1/operations", OP, 1);
    let mut body = format!(r#"{{"request_id":"{}","data":1.00}}"#, id(6)).into_bytes();
    body.resize(MAX_BODY_BYTES, b' ');
    let s = reply(&c, &body, 200);
    check_reply_content(&s, body.clone()).unwrap();
    body.push(b' ');
    assert!(matches!(check_reply_content(&s, body), Err(Error::Limit)));
    let initial = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let c = context("readiness", "/internal/v1/readiness", &initial, 2);
    let mut body=br#"{"request_id":null,"error":{"code":"overloaded","message":"data","outcome":"not_applied"}}"#.to_vec();
    body.resize(SMALL_CAP, b' ');
    let s = reply(&c, &body, 429);
    check_reply_content(&s, body.clone()).unwrap();
    body.push(b' ');
    assert!(matches!(check_reply_content(&s, body), Err(Error::Limit)));
}
#[test]
fn proof_traits_and_redaction() {
    trait NoClone<M> {
        fn marker() {}
    }
    impl<T> NoClone<()> for T {}
    impl<T: Clone> NoClone<u8> for T {}
    trait NoSerialize<M> {
        fn marker() {}
    }
    impl<T> NoSerialize<()> for T {}
    impl<T: serde::Serialize> NoSerialize<u8> for T {}
    trait NoDeserialize<M> {
        fn marker() {}
    }
    impl<T> NoDeserialize<()> for T {}
    impl<T: serde::Deserialize<'static>> NoDeserialize<u8> for T {}
    macro_rules! guard {($($t:ty),*)=>{$(let _=<$t as NoClone<_>>::marker;let _=<$t as NoSerialize<_>>::marker;let _=<$t as NoDeserialize<_>>::marker;)*};}
    guard!(
        RequestContext,
        SignedReply,
        CheckedContent,
        CheckedReplyContent
    );
    let c = context("operation", "/internal/v1/operations", OP, 1);
    assert_eq!(format!("{c:?}"), "RequestContext");
    let body = format!(r#"{{"request_id":"{}","data":"SECRET DATA"}}"#, id(6)).into_bytes();
    let s = reply(&c, &body, 200);
    assert_eq!(format!("{s:?}"), "SignedReply");
    assert_eq!(
        format!("{:?}", check_reply_content(&s, body).unwrap()),
        "CheckedReplyContent"
    );
}
#[test]
fn original_numeric_spelling_and_encoder_fallback_outcomes() {
    let path = "/internal/v1/operations";
    let bytes=format!(r#" {{"request_id":"{}","operation":"table.insert","payload":{{"table_id":"{}","row":{{"id":1,"score":1.00}}}}}} "#,id(6),id(16)).into_bytes();
    let text = auth_text("operation", path, &bytes, 1);
    let s = signed(&text, path);
    let content = check_request_content(&s, bytes.clone()).unwrap();
    assert_eq!(content.bytes.as_ref(), bytes);
    let changed = String::from_utf8(bytes.clone())
        .unwrap()
        .replace("1.00", "1.0")
        .into_bytes();
    assert!(check_request_content(&s, changed).is_err());
    let text = text.replace(
        &format!(r#""request_digest":"{}""#, hash(&bytes)),
        &format!(r#""request_digest":"{}""#, "f".repeat(64)),
    );
    assert!(check_request_content(&signed(&text, path), bytes).is_err());
    struct Fails;
    impl serde::Serialize for Fails {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("DATA"))
        }
    }
    let c = context("operation", path, OP, 1);
    for mutation in [false, true] {
        let encoded = crate::api::response::encode_success(id(6), &Fails, mutation);
        let checked =
            check_reply_content(&reply(&c, &encoded.body, encoded.status), encoded.body).unwrap();
        assert_eq!(checked.error().unwrap().code, ErrorCode::InternalError);
        assert_eq!(
            checked.error().unwrap().outcome,
            if mutation {
                Outcome::Unknown
            } else {
                Outcome::NotApplied
            }
        );
    }
    let encoded = crate::api::response::encode_error(
        Some(id(6)),
        ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "x".repeat(MAX_BODY_BYTES),
            Outcome::Unknown,
        ),
    );
    let checked =
        check_reply_content(&reply(&c, &encoded.body, encoded.status), encoded.body).unwrap();
    assert_eq!(checked.error().unwrap().code, ErrorCode::LimitExceeded);
    assert_eq!(checked.error().unwrap().outcome, Outcome::Unknown);
    let good = format!(
        r#"{{"request_id":"{}","error":{{"code":"overloaded","message":"data","outcome":"not_applied"}}}}"#,
        id(6)
    );
    for bad in [
        "[]".to_owned(),
        good.replace("{\"code\":", "[\"code\","),
        good.replace("\"request_id\":", "\"request_id\":null,\"request_id\":"),
        good.replace("\"outcome\":", "\"unknown_field\":0,\"outcome\":"),
        "{".to_owned(),
    ] {
        assert!(error_envelope(bad.as_bytes(), 429).is_err());
    }
}
#[test]
fn transport_error_rejects_a_mismatched_known_operation_uuid() {
    let body = job_bytes();
    let c = context("job", "/internal/v1/jobs", &body, 1);
    for request_id in [id(6), id(60)] {
        let body=serde_json::to_vec(&json!({"request_id":request_id,"error":{"code":"overloaded","message":"DATA","outcome":"not_applied"}})).unwrap();
        assert_eq!(
            check_reply_content(&reply(&c, &body, 429), body).is_ok(),
            request_id == id(6)
        );
    }
}
