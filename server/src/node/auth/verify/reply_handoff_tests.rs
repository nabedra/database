use super::consume_tests::{Fixture, hex, id, roundtrip};
use super::*;
use crate::cluster::{member::NodeState, state::Control};
use serde_json::json;

const OP: &[u8] = br#"{ "request_id":"00000000-0000-0000-0000-000000000006", "operation":"table.list", "payload":{} }"#;
fn context(f: &Fixture, scope: &str, body: &[u8]) -> RequestContext {
    let (text, route) = f.auth(scope, body);
    RequestContext::for_send(
        Auth::decode(&hex(text.as_bytes()), &route).unwrap(),
        body.to_vec(),
        &f.root,
    )
    .unwrap()
}
fn signed(f: &Fixture, c: &RequestContext, body: &[u8], status: u16) -> SignedReply {
    use sha2::Digest;
    let original = String::from_utf8(c.request.canonical.to_vec()).unwrap();
    let receiver = original
        .split("\"sender\":")
        .nth(1)
        .unwrap()
        .split(",\"receiver\":")
        .next()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&c.request.canonical).unwrap();
    let sender = format!(
        r#"{{"role":"worker","process_id":"{}","node_id":"{}","incarnation":"{}","session_id":"{}","public_key":"{}"}}"#,
        id(4),
        id(3),
        id(4),
        id(5),
        hex(&f.key.public_key())
    );
    let text = format!(
        r#"{{"version":1,"cluster_id":"{}","request_sequence":1,"scope":{},"sender":{sender},"receiver":{receiver},"status":{status},"control_sha256":"{}"}}"#,
        id(1),
        value["scope"],
        hex(&sha2::Sha256::digest(body))
    );
    let auth = ReplyAuth::decode(&hex(text.as_bytes())).unwrap();
    verify_reply(
        &hex(text.as_bytes()),
        &hex(&f.key.sign_response(&auth).unwrap()),
        status,
        c,
    )
    .unwrap()
}
fn proofs(
    f: &Fixture,
    c: &RequestContext,
    body: Vec<u8>,
    status: u16,
    root: &Control,
) -> (EligibleReply, CheckedReplyContent) {
    let s = signed(f, c, &body, status);
    (
        check_reply_authority(&s, root, c).unwrap(),
        check_reply_content(&s, body).unwrap(),
    )
}
fn success() -> Vec<u8> {
    format!(r#"{{"request_id":"{}","data": [1, 2]}}"#, id(6)).into_bytes()
}

#[test]
fn transfers_original_allocation_after_unrelated_root_progress() {
    let f = Fixture::new();
    let c = context(&f, "operation", OP);
    let expected = Arc::clone(&c.expected);
    let request = Arc::clone(&c.request);
    let target = Arc::clone(&c.target);
    let initial_ptr = c.initial_bytes().as_ptr();
    let mut root = f.root.clone();
    root.revision += 1;
    root.change_id = id(900);
    root.scheduler.failures += 1;
    let root = roundtrip(&root);
    let (e, content) = proofs(&f, &c, success(), 200, &root);
    let observation = e.observation().clone();
    assert!(observation != c.target.observation);
    let bytes_ptr = content.bytes.as_ptr();
    let parsed_ptr = content.operation().unwrap().get().as_ptr();
    let h = c.into_reply_handoff(e, content, &observation).unwrap();
    assert_eq!(h.context().initial_bytes().as_ptr(), initial_ptr);
    assert_eq!(h.bytes().as_ptr(), bytes_ptr);
    assert_eq!(h.operation().unwrap().get().as_ptr(), parsed_ptr);
    assert_eq!(h.status(), 200);
    assert_eq!(h.auth().request_sequence(), 1);
    assert!(
        matches!(h.pending(), PendingReplyChecks::AcceptedOperation { operation_id, .. } if *operation_id == id(6))
    );
    let (binding, bytes, body, observed, _) = h.into_parts();
    assert!(Arc::ptr_eq(&binding.context.request, &request));
    assert!(Arc::ptr_eq(&binding.context.expected, &expected));
    assert!(Arc::ptr_eq(&binding.context.target, &target));
    assert_eq!(bytes.as_ptr(), bytes_ptr);
    assert!(observed == observation);
    match body {
        ReplyBody::Operation(v) => assert_eq!(v.get().as_ptr(), parsed_ptr),
        _ => panic!("operation"),
    }
}

#[test]
fn rejects_different_reply_invocations_contexts_and_complete_observations() {
    let f = Fixture::new();
    let c = context(&f, "operation", OP);
    let (e, _) = proofs(&f, &c, success(), 200, &f.root);
    let (_, content) = proofs(&f, &c, success(), 200, &f.root);
    let current = e.observation().clone();
    assert!(matches!(
        c.into_reply_handoff(e, content, &current),
        Err(Error::Invalid)
    ));
    let c = context(&f, "operation", OP);
    let other = context(&f, "operation", OP);
    let (e, content) = proofs(&f, &c, success(), 200, &f.root);
    assert!(matches!(
        other.into_reply_handoff(e, content, &current),
        Err(Error::Invalid)
    ));
    for advance in [false, true] {
        let c = context(&f, "operation", OP);
        let (e, content) = proofs(&f, &c, success(), 200, &f.root);
        let mut root = f.root.clone();
        root.scheduler.failures += 1;
        if advance {
            root.revision += 1;
            root.change_id = id(901);
        }
        let root = roundtrip(&root);
        let current = OwnershipObservation::new(&root).unwrap();
        assert!(matches!(
            c.into_reply_handoff(e, content, &current),
            Err(Error::Invalid)
        ));
    }
}

#[test]
fn small_observations_and_remote_errors_keep_their_meaning() {
    let mut f = Fixture::new();
    for (scope, field, extra) in [
        ("readiness", "challenge", ",\"initialized\":true"),
        ("dependency_open", "permit_id", ""),
        ("dependency_close", "permit_id", ",\"closed\":false"),
        (
            "drain",
            "drain_id",
            ",\"barrier\":true,\"idle\":false,\"heartbeat_sequence\":1",
        ),
    ] {
        if scope == "drain" {
            f.root.nodes[0].state = NodeState::Draining;
            f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
                id: id(11),
                requested_at_ms: 0,
                ack_seq: None,
                confirmed_idle: false,
            });
            f.root = roundtrip(&f.root);
        }
        let initial = format!(r#"{{"version":1,"{field}":"{}"}}"#, id(11)).into_bytes();
        let bytes = format!(r#"{{"version":1,"{field}":"{}"{extra}}}"#, id(11)).into_bytes();
        let c = context(&f, scope, &initial);
        if scope == "readiness" {
            // The existing checked wire contract requires initialized:true.
            let invalid = String::from_utf8(bytes.clone())
                .unwrap()
                .replace("true", "false")
                .into_bytes();
            let signed = signed(&f, &c, &invalid, 200);
            check_reply_authority(&signed, &f.root, &c).unwrap();
            assert!(matches!(
                check_reply_content(&signed, invalid),
                Err(Error::Invalid)
            ));
        }
        let (e, content) = proofs(&f, &c, bytes.clone(), 200, &f.root);
        let current = e.observation().clone();
        let h = c.into_reply_handoff(e, content, &current).unwrap();
        assert_eq!(h.bytes(), bytes);
        let (_, _, body, _, pending) = h.into_parts();
        match (body, pending) {
            (ReplyBody::Readiness(v), PendingReplyChecks::Probe { challenge }) => {
                assert!(v.initialized());
                assert_eq!(v.challenge(), challenge);
            }
            (ReplyBody::Open(v), PendingReplyChecks::Open { permit_id, .. }) => {
                assert_eq!(v.permit_id(), permit_id)
            }
            (ReplyBody::Close(v), PendingReplyChecks::Close { permit_id, .. }) => {
                assert!(!v.closed());
                assert_eq!(v.permit_id(), permit_id);
            }
            (ReplyBody::Drain(v), PendingReplyChecks::Drain { drain_id }) => {
                assert!(!v.idle());
                assert!(v.barrier());
                assert_eq!(v.heartbeat_sequence(), 1);
                assert_eq!(v.drain_id(), drain_id);
            }
            _ => panic!("body and pending kind"),
        }
    }
    // Draining targets can still reply to retained accepted operations.
    for unattributed in [false, true] {
        let c = context(&f, "operation", OP);
        let bytes = serde_json::to_vec(&json!({"request_id": if unattributed { None } else { Some(id(6)) }, "error":{"code":"internal_error", "message":"remote data", "outcome":"unknown"}})).unwrap();
        let (e, content) = proofs(&f, &c, bytes, 500, &f.root);
        let current = e.observation().clone();
        let h = c.into_reply_handoff(e, content, &current).unwrap();
        assert_eq!(h.is_unattributed_rejection(), unattributed);
        assert_eq!(h.error().unwrap().message, "remote data");
        assert_eq!(h.status(), 500);
        let (_, _, body, _, _) = h.into_parts();
        let err = match body {
            ReplyBody::UnattributedRejection(e) if unattributed => e,
            ReplyBody::Rejection(e) if !unattributed => e,
            _ => panic!("error classification"),
        };
        assert_eq!(err.code, crate::protocol::ErrorCode::InternalError);
        assert_eq!(err.outcome, crate::protocol::Outcome::Unknown);
        assert_eq!(err.message, "remote data");
    }
}

fn job_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({"version":1,"cluster_id":id(1),"operation_id":id(6),"job_id":id(8),"attempt_id":id(9),"request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":id(15),"catalog_etag":"token"},"ordinal":0,"table":{"id":id(16),"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":format!("tables/{}/{}.parquet",id(16),id(17)),"row_count":1}},"key":1}}})).unwrap()
}
#[test]
fn job_transfer_retains_the_complete_original_expectation() {
    let f = Fixture::new();
    let initial = job_bytes();
    let c = context(&f, "job", &initial);
    let expected = Arc::clone(&c.expected);
    let mut result: serde_json::Value = serde_json::from_slice(&initial).unwrap();
    result.as_object_mut().unwrap().remove("table");
    result.as_object_mut().unwrap().remove("task");
    result["result"] =
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":8,"found":false}});
    let bytes = serde_json::to_vec(&result).unwrap();
    let (e, content) = proofs(&f, &c, bytes, 200, &f.root);
    let current = e.observation().clone();
    let bytes_ptr = content.bytes.as_ptr();
    let parsed_ptr = content.job().unwrap() as *const ResultEnvelope;
    let h = c.into_reply_handoff(e, content, &current).unwrap();
    assert_eq!(h.job().unwrap() as *const ResultEnvelope, parsed_ptr);
    assert!(
        matches!(h.pending(), PendingReplyChecks::AcceptedJob { binding, permit_id, job_id, attempt_id } if binding.operation_id() == id(6) && binding.admission_id() == id(7) && *permit_id == id(10) && *job_id == id(8) && *attempt_id == id(9))
    );
    let (binding, bytes, body, _, _) = h.into_parts();
    assert!(Arc::ptr_eq(&binding.context.expected, &expected));
    let ExpectedContent::Job(original) = expected.as_ref() else {
        panic!("job")
    };
    assert_eq!(binding.expected_job().unwrap(), original.as_ref());
    assert_eq!(
        binding.expected_job().unwrap() as *const JobEnvelope,
        original.as_ref() as *const JobEnvelope
    );
    assert_eq!(binding.initial_bytes(), initial);
    assert_eq!(bytes.as_ptr(), bytes_ptr);
    match body {
        ReplyBody::Job(v) => assert_eq!(v.as_ref() as *const ResultEnvelope, parsed_ptr),
        _ => panic!("job result"),
    }
}

#[test]
fn opaque_ownership_traits_and_value_free_debug() {
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
    macro_rules! guard { ($($t:ty),*) => {$(let _=<$t as NoClone<_>>::marker; let _=<$t as NoSerialize<_>>::marker; let _=<$t as NoDeserialize<_>>::marker;)*}; }
    guard!(RequestContext, ReplyHandoff, ReplyContextBinding, ReplyBody);
    let f = Fixture::new();
    let c = context(&f, "operation", OP);
    assert_eq!(format!("{c:?}"), "RequestContext");
    let (e, content) = proofs(&f, &c, success(), 200, &f.root);
    let current = e.observation().clone();
    let h = c.into_reply_handoff(e, content, &current).unwrap();
    assert_eq!(format!("{h:?}"), "ReplyHandoff");
    let (binding, _, body, _, _) = h.into_parts();
    assert_eq!(format!("{binding:?}"), "ReplyContextBinding");
    assert_eq!(format!("{body:?}"), "ReplyBody");
}
