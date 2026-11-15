use super::*;
const AUTH: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"readiness","sender":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/readiness","operation_id":null,"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":null,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const REPLY: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","request_sequence":1,"scope":"readiness","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"receiver":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"status":200,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
const UNSIGNED: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","node_id":"00000000-0000-0000-0000-000000000003","request_id":null,"incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","endpoint":"http://127.0.0.1:8000","operation_slots":1,"partition_slots":256,"queue_size":4096}"#;
fn header(s: &str) -> String {
    s.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}
fn readiness() -> ActualRoute {
    ActualRoute::new("POST", "/internal/v1/readiness", None).unwrap()
}
fn auth(s: &str) -> Result<Auth, Error> {
    Auth::decode(&header(s), &readiness())
}
fn enrollment() -> String {
    format!(
        "{},\"proof\":\"{}\"}}",
        &UNSIGNED[..UNSIGNED.len() - 1],
        "c".repeat(128)
    )
}
#[test]
fn validated_records_require_checked_construction() {
    // An exposed Deserialize implementation makes the marker type ambiguous,
    // failing compilation before callers can bypass a checked byte constructor.
    trait AmbiguousIfDeserializable<Marker> {
        fn marker() {}
    }
    impl<T> AmbiguousIfDeserializable<()> for T {}
    impl<T: serde::Deserialize<'static>> AmbiguousIfDeserializable<u8> for T {}
    let _ = <Sender as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Receiver as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Auth as AmbiguousIfDeserializable<_>>::marker;
    let _ = <ReplyAuth as AmbiguousIfDeserializable<_>>::marker;
    let _ = <EnrollmentUnsigned as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Enrollment as AmbiguousIfDeserializable<_>>::marker;
    let _ = <EnrollmentReply as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Readiness as AmbiguousIfDeserializable<_>>::marker;
    let _ = <ReadinessReply as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Dependency as AmbiguousIfDeserializable<_>>::marker;
    let _ = <DependencyCloseReply as AmbiguousIfDeserializable<_>>::marker;
    let _ = <Drain as AmbiguousIfDeserializable<_>>::marker;
    let _ = <DrainReply as AmbiguousIfDeserializable<_>>::marker;
    let valid = ReplyAuth::decode(&header(REPLY)).unwrap();
    assert_eq!(valid.status(), 200);
    let malformed = REPLY.replace("\"status\":200", "\"status\":65536");
    assert!(ReplyAuth::decode(&header(&malformed)).is_err());
}

#[test]
fn literal_canonical_records_and_enrollment_reconstruction() {
    assert_eq!(
        EnrollmentUnsigned::decode(UNSIGNED.as_bytes())
            .unwrap()
            .public_key()
            .unwrap(),
        [0xaa; 32]
    );
    let request = auth(AUTH).unwrap();
    assert_eq!(request.sequence(), 1);
    assert_eq!(request.sender().public_key().unwrap(), [0xaa; 32]);
    let response = ReplyAuth::decode(&header(REPLY)).unwrap();
    assert!(response.receiver() == request.sender());
    assert_eq!(response.request_sequence(), request.sequence());
    assert_eq!(
        response.sender().node_id(),
        Some(request.receiver().node_id())
    );
    let enrolled = Enrollment::decode(enrollment().as_bytes()).unwrap();
    assert_eq!(enrolled.public_key().unwrap(), [0xaa; 32]);
    assert_eq!(enrolled.proof().unwrap(), [0xcc; 64]);

    assert_eq!(
        auth(AUTH).unwrap().canonical_bytes().unwrap(),
        AUTH.as_bytes()
    );
    assert_eq!(
        ReplyAuth::decode(&header(REPLY))
            .unwrap()
            .canonical_bytes()
            .unwrap(),
        REPLY.as_bytes()
    );
    let e = Enrollment::decode(enrollment().as_bytes()).unwrap();
    assert_eq!(e.unsigned_bytes().unwrap(), UNSIGNED.as_bytes());
    assert_eq!(e.encode().unwrap(), enrollment().as_bytes());
    let spaced = enrollment().replace(",", ", ");
    assert_eq!(
        Enrollment::decode(spaced.as_bytes())
            .unwrap()
            .unsigned_bytes()
            .unwrap(),
        UNSIGNED.as_bytes()
    );
    assert!(auth(&AUTH.replace(",", ", ")).is_err());
    assert!(
        auth(&AUTH.replacen(
            "\"version\":1,\"cluster_id\":\"00000000-0000-0000-0000-000000000001\"",
            "\"cluster_id\":\"00000000-0000-0000-0000-000000000001\",\"version\":1",
            1
        ))
        .is_err()
    );
    assert!(auth(&AUTH.replace("readiness", "readin\\u0065ss")).is_err());
    assert!(ReplyAuth::decode(&header(&REPLY.replace(",", ", "))).is_err());
}
#[test]
fn strict_objects_required_nulls_strings_and_exact_numbers() {
    for (from, to) in [
        ("\"version\":1", "\"version\":1,\"version\":1"),
        ("\"version\":1", "\"extra\":0,\"version\":1"),
        ("\"node_id\":null,", ""),
        ("\"role\":\"api\"", "\"role\":{\"api\":null}"),
        (
            "\"sequence\":1",
            "\"sequence\":{\"$serde_json::private::Number\":\"1\"}",
        ),
        ("\"sender\":{", "\"sender\":["),
        ("\"public_key\":\"aaaa", "\"public_key\":\"Aaaa"),
    ] {
        assert!(auth(&AUTH.replacen(from, to, 1)).is_err(), "{from}");
    }
    for n in [
        "0",
        "-0",
        "-1",
        "+1",
        "1.0",
        "1e0",
        "18446744073709551616",
        "null",
        "true",
    ] {
        assert!(
            auth(&AUTH.replace("\"sequence\":1", &format!("\"sequence\":{n}"))).is_err(),
            "{n}"
        );
    }
    assert!(auth(&AUTH.replace("\"sequence\":1", "\"sequence\":18446744073709551615")).is_ok());
    for bad in [
        UNSIGNED.replace("\"queue_size\":4096", "\"queue_size\":1e0"),
        UNSIGNED.replace("\"request_id\":null,", ""),
        UNSIGNED.replace("http://127.0.0.1:8000", "http://127.0.0.1/prefix"),
        UNSIGNED.replace("\"operation_slots\":1", "\"operation_slots\":0"),
    ] {
        assert!(EnrollmentUnsigned::decode(bad.as_bytes()).is_err());
    }
}
#[test]
fn actual_route_role_claims_and_dependency_agree() {
    for (m, p, d) in [
        ("GET", "/internal/v1/readiness", None),
        ("POST", "/internal/v1/readiness?x", None),
        ("POST", "/internal/v1/readiness/", None),
        ("POST", "/internal/v1/enroll", None),
        (
            "POST",
            "/internal/v1/readiness",
            Some("00000000-0000-0000-0000-000000000001"),
        ),
    ] {
        assert!(ActualRoute::new(m, p, d).is_err());
    }
    let op = AUTH
        .replace("readiness", "operation")
        .replace("/internal/v1/operation\"", "/internal/v1/operations\"")
        .replace(
            "\"operation_id\":null",
            "\"operation_id\":\"00000000-0000-0000-0000-000000000001\"",
        )
        .replace(
            "\"request_digest\":null",
            &format!("\"request_digest\":\"{}\"", "b".repeat(64)),
        );
    let route = ActualRoute::new("POST", "/internal/v1/operations", None).unwrap();
    assert!(Auth::decode(&header(&op), &route).is_ok());
    assert!(Auth::decode(&header(&op), &readiness()).is_err());
    assert!(
        auth(&AUTH.replace(
            "\"operation_id\":null",
            "\"operation_id\":\"00000000-0000-0000-0000-000000000001\""
        ))
        .is_err()
    );
    assert!(auth(&AUTH.replace("\"role\":\"api\"", "\"role\":\"worker\"")).is_err());
}
#[test]
fn small_body_shapes_and_enrollment_reply_are_separate() {
    let id = "00000000-0000-0000-0000-000000000001";
    let probe = format!(r#"{{"version":1,"challenge":"{id}"}}"#);
    assert_eq!(
        Readiness::decode(probe.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        probe.as_bytes()
    );
    let ready = format!(r#"{{"version":1,"challenge":"{id}","initialized":true}}"#);
    assert_eq!(
        ReadinessReply::decode(ready.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        ready.as_bytes()
    );
    assert!(ReadinessReply::decode(ready.replace("true", "false").as_bytes()).is_err());
    let permit = format!(r#"{{"version":1,"permit_id":"{id}"}}"#);
    assert_eq!(
        Dependency::decode(permit.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        permit.as_bytes()
    );
    for closed in [true, false] {
        let s = format!(r#"{{"version":1,"permit_id":"{id}","closed":{closed}}}"#);
        assert_eq!(
            DependencyCloseReply::decode(s.as_bytes())
                .unwrap()
                .encode()
                .unwrap(),
            s.as_bytes()
        );
    }
    let drain = format!(r#"{{"version":1,"drain_id":"{id}"}}"#);
    assert_eq!(
        Drain::decode(drain.as_bytes()).unwrap().encode().unwrap(),
        drain.as_bytes()
    );
    let reply = format!(
        r#"{{"version":1,"drain_id":"{id}","barrier":true,"idle":false,"heartbeat_sequence":1}}"#
    );
    assert_eq!(
        DrainReply::decode(reply.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        reply.as_bytes()
    );
    for s in [
        reply.replace("\"barrier\":true", "\"barrier\":false"),
        reply.replace("\"idle\":false,", ""),
        reply.replace("\"heartbeat_sequence\":1", "\"heartbeat_sequence\":0"),
    ] {
        assert!(DrainReply::decode(s.as_bytes()).is_err());
    }
    let e = format!(
        r#"{{"version":1,"cluster_id":"{id}","node_id":"{id}","incarnation":"{id}","session_id":"{id}","registration":"registered","api_public_key":"{}"}}"#,
        "a".repeat(64)
    );
    assert_eq!(
        EnrollmentReply::decode(e.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        e.as_bytes()
    );
    assert!(EnrollmentReply::decode(e.replace("registered", "verified").as_bytes()).is_ok());
    assert!(EnrollmentReply::decode(e.replace("registered", "ready").as_bytes()).is_err());
    assert!(EnrollmentReply::decode(REPLY.as_bytes()).is_err());
}
#[test]
fn early_bounds_hex_depth_and_fixed_errors() {
    assert_eq!(decode_header(&"a".repeat(8194)), Err(Error::Limit));
    for s in ["a", "AA", "gg"] {
        assert!(decode_header(s).is_err());
    }
    assert_eq!(decode_signature(&"a".repeat(128)).unwrap(), [0xaa; 64]);
    for s in ["a".repeat(127), "a".repeat(129), "A".repeat(128)] {
        assert!(decode_signature(&s).is_err());
    }
    assert_eq!(encode(&"x".repeat(14), 16).unwrap().len(), 16);
    assert_eq!(encode(&"x".repeat(15), 16), Err(Error::Limit));
    assert_eq!(
        preflight(&vec![b' '; SMALL_CAP + 1], SMALL_CAP),
        Err(Error::Limit)
    );
    assert!(
        preflight(
            format!("{}0{}", "[".repeat(16), "]".repeat(16)).as_bytes(),
            SMALL_CAP
        )
        .is_ok()
    );
    assert_eq!(
        preflight(
            format!("{}0{}", "[".repeat(17), "]".repeat(17)).as_bytes(),
            SMALL_CAP
        ),
        Err(Error::Limit)
    );
    for e in [
        Error::Invalid,
        Error::Limit,
        Error::Replay,
        Error::Exhausted,
    ] {
        assert_eq!(format!("{e}"), format!("{e:?}"));
        assert!(std::error::Error::source(&e).is_none());
    }
}
#[test]
fn worker_claims_and_dependency_bind_to_actual_headers() {
    let id = "00000000-0000-0000-0000-000000000001";
    let worker = r#"{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#;
    let start = AUTH.find("{\"role\"").unwrap();
    let end = start + AUTH[start..].find('}').unwrap() + 1;
    let base = format!("{}{}{}", &AUTH[..start], worker, &AUTH[end..]);
    for (scope, path, job) in [
        ("job", "jobs", true),
        ("dependency_open", "dependencies/open", false),
        ("dependency_close", "dependencies/close", false),
    ] {
        let mut s = base
            .replace("\"scope\":\"readiness\"", &format!("\"scope\":\"{scope}\""))
            .replace("/internal/v1/readiness", &format!("/internal/v1/{path}"));
        for field in ["operation_id", "admission_id"].into_iter().chain(if job {
            vec!["job_id", "attempt_id", "dependency_id"]
        } else {
            vec![]
        }) {
            s = s.replace(
                &format!("\"{field}\":null"),
                &format!("\"{field}\":\"{id}\""),
            );
        }
        s = s.replace(
            "\"request_digest\":null",
            &format!("\"request_digest\":\"{}\"", "b".repeat(64)),
        );
        let route =
            ActualRoute::new("POST", &format!("/internal/v1/{path}"), job.then_some(id)).unwrap();
        assert!(Auth::decode(&header(&s), &route).is_ok(), "{scope}");
        assert!(
            Auth::decode(
                &header(&s.replace(
                    &format!("\"admission_id\":\"{id}\""),
                    "\"admission_id\":null"
                )),
                &route
            )
            .is_err()
        );
        assert!(
            Auth::decode(
                &header(&s.replacen(
                    "\"process_id\":\"00000000-0000-0000-0000-000000000004\"",
                    "\"process_id\":\"00000000-0000-0000-0000-000000000003\"",
                    1
                )),
                &route
            )
            .is_err()
        );
        if job {
            let other = ActualRoute::new(
                "POST",
                "/internal/v1/jobs",
                Some("00000000-0000-0000-0000-000000000002"),
            )
            .unwrap();
            assert!(Auth::decode(&header(&s), &other).is_err());
        }
    }
    assert!(ActualRoute::new("POST", "/internal/v1/jobs", None).is_err());
}
#[test]
fn nested_objects_and_small_input_bounds() {
    for (from, to) in [
        ("\"node_id\":null", "\"node_id\":null,\"node_id\":null"),
        ("\"role\":\"api\"", "\"role\":\"api\",\"unknown\":0"),
        ("\"scope\":\"readiness\"", "\"scope\":{\"readiness\":null}"),
        ("\"sequence\":1", "\"sequence\":\"1\""),
    ] {
        assert!(auth(&AUTH.replacen(from, to, 1)).is_err());
    }
    for (from, to) in [
        ("\"partition_slots\":256", "\"partition_slots\":257"),
        ("\"queue_size\":4096", "\"queue_size\":4097"),
        ("http://127.0.0.1:8000", "http://user@host"),
        ("http://127.0.0.1:8000", "http://host:0"),
        ("http://127.0.0.1:8000", "http://host/?q"),
        (
            "\"request_id\":null",
            "\"request_id\":\"AAAAAAAA-0000-0000-0000-000000000001\"",
        ),
    ] {
        assert!(EnrollmentUnsigned::decode(UNSIGNED.replace(from, to).as_bytes()).is_err());
    }
    assert!(
        EnrollmentUnsigned::decode(
            UNSIGNED
                .replace(
                    "http://127.0.0.1:8000",
                    &format!("http://{}", "a".repeat(2042))
                )
                .as_bytes()
        )
        .is_err()
    );
    assert!(Readiness::decode(&vec![b' '; SMALL_CAP + 1]).is_err());
    assert!(
        Readiness::decode(format!("{}0{}", "[".repeat(17), "]".repeat(17)).as_bytes()).is_err()
    );
    assert_eq!(decode_header(&"aa".repeat(4096)).unwrap().len(), 4096);
    assert!(
        ReplyAuth::decode(&header(
            &REPLY.replace("\"request_sequence\":1", "\"request_sequence\":1e0")
        ))
        .is_err()
    );
}
