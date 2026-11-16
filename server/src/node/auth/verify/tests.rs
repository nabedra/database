use super::*;
use ring::signature::Ed25519KeyPair;
use uuid::Uuid;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn bytes<const N: usize>(s: &str) -> [u8; N] {
    (0..N)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}
fn route(path: &str) -> ActualRoute {
    ActualRoute::new("POST", path, None).unwrap()
}
fn enrollment(unsigned: &str, signature: &str) -> Vec<u8> {
    format!(
        "{},\"proof\":\"{}\"}}",
        &unsigned[..unsigned.len() - 1],
        signature
    )
    .into_bytes()
}
// Independent synthetic signer, without production encoders/signing helpers.
fn sign(seed: &str, prefix: &[u8], body: &[u8]) -> String {
    let mut input = prefix.to_vec();
    input.extend_from_slice(body);
    hex(Ed25519KeyPair::from_seed_unchecked(&bytes::<32>(seed))
        .unwrap()
        .sign(&input)
        .as_ref())
}
// Literals copied unchanged from reviewed R03a2a credential vectors.
const SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
const API_SEED: &str = "b36a6d0f436b1e08393c0c9f65ea850039debec28a69c5971512125eb3e6f625";
const WORKER_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
const ENROLL: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","node_id":"00000000-0000-0000-0000-000000000003","request_id":null,"incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a","endpoint":"http://127.0.0.1:8000","operation_slots":1,"partition_slots":256,"queue_size":4096}"#;
const ENROLL_SIG: &str = "324a8622ab75f207f19f8bd987d901f09b41988608dc36e1723031d3d2fa0eba10d986358908a6783e362b709862b3c04992ba45f84ef056559a5d37fbc80d0c";
const API_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"readiness","sender":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"41f19e73e483776f4b552779a73e361242210c5a26b04825db62e69b52db0d95"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/readiness","operation_id":null,"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":null,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const API_REQUEST_SIG: &str = "7f5559ac5447ee0e35f85c51e7e0a4c4db580d842a3128c78984acfa1b8760d3415461694d6ce6d8aabbc1dd2b44fd40b5af2b9f04c517c6005514eadab1130f";
const WORKER_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"dependency_open","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/dependencies/open","operation_id":"00000000-0000-0000-0000-000000000006","admission_id":"00000000-0000-0000-0000-000000000007","job_id":null,"attempt_id":null,"request_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const WORKER_REQUEST_SIG: &str = "f88ceddd5dca2a0807a62348ff6c073bfca62118ffcf3a8da7306e8f7c0875adfe687a6b7a30d6295f13582ae0ebfc68678d6d1544ad0c8a2e0e1de69e5e6101";
#[test]
fn reviewed_enrollment_and_claims() {
    let p = verify_enrollment(&enrollment(ENROLL, ENROLL_SIG)).unwrap();
    assert_eq!(p.public_key(), bytes::<32>(WORKER_PUBLIC));
    assert_eq!(
        p.verifier(),
        bytes::<32>("1214ed9c37ee28fe7e9af7c1731deef2dedd682bfdf2979f81e3fa3b286f11e8")
    );
    let e = p.claims();
    assert_eq!(e.cluster_id(), Uuid::from_u128(1));
    assert_eq!(e.node_id(), Uuid::from_u128(3));
    assert_eq!(e.incarnation(), Uuid::from_u128(4));
    assert_eq!(e.session_id(), Uuid::from_u128(5));
    assert_eq!(e.request_id(), None);
    assert_eq!(e.endpoint(), "http://127.0.0.1:8000");
    assert_eq!(
        (e.operation_slots(), e.partition_slots(), e.queue_size()),
        (1, 256, 4096)
    );
    assert_eq!(format!("{p:?}"), "EnrollmentProof");
}
#[test]
fn reviewed_requests_and_distinct_invocations() {
    for (body, sig, path) in [
        (API_REQUEST, API_REQUEST_SIG, "/internal/v1/readiness"),
        (
            WORKER_REQUEST,
            WORKER_REQUEST_SIG,
            "/internal/v1/dependencies/open",
        ),
    ] {
        let a = verify_request(&hex(body.as_bytes()), sig, &route(path)).unwrap();
        let b = verify_request(&hex(body.as_bytes()), sig, &route(path)).unwrap();
        assert_eq!(a.auth().sequence(), 1);
        assert_eq!(a.request.canonical.as_ref(), body.as_bytes());
        assert!(!Arc::ptr_eq(&a.request, &b.request));
        assert_eq!(format!("{a:?}"), "SignedRequest");
    }
}

#[test]
fn enrollment_rejects_signature_domain_and_changed_claims() {
    verify_enrollment(&enrollment(ENROLL, ENROLL_SIG)).unwrap();
    let mut bad = bytes::<64>(ENROLL_SIG);
    bad[0] ^= 1;
    assert!(matches!(
        verify_enrollment(&enrollment(ENROLL, &hex(&bad))),
        Err(Error::Invalid)
    ));
    for prefix in [
        b"meshdb.http-request.v1\0".as_slice(),
        b"meshdb.enroll.v1",
        b"meshdb.enroll.v1\0\0",
    ] {
        let sig = sign(SEED, prefix, ENROLL.as_bytes());
        assert!(matches!(
            verify_enrollment(&enrollment(ENROLL, &sig)),
            Err(Error::Invalid)
        ));
    }
    let changed = ENROLL.replace("\"queue_size\":4096", "\"queue_size\":4095");
    // Syntax remains valid; only the bytes covered by the old signature changed.
    Enrollment::decode(&enrollment(&changed, ENROLL_SIG)).unwrap();
    assert!(matches!(
        verify_enrollment(&enrollment(&changed, ENROLL_SIG)),
        Err(Error::Invalid)
    ));
    let sig = sign(SEED, b"meshdb.enroll.v1\0", changed.as_bytes());
    assert!(verify_enrollment(&enrollment(&changed, &sig)).is_ok());
}
#[test]
fn c02_signed_capacity_and_incarnation_cannot_be_forged() {
    let resources = r#""resources":{"max_threads":2,"memory_limit_bytes":1000,"task_memory_limit_bytes":800,"control_memory_reserve_bytes":100},"#;
    let unsigned = ENROLL.replace("\"partition_slots\":256", "\"partition_slots\":2");
    let unsigned = format!("{{{resources}{}", &unsigned[1..]);
    let signature = sign(SEED, b"meshdb.enroll.v1\0", unsigned.as_bytes());
    let valid = verify_enrollment(&enrollment(&unsigned, &signature)).unwrap();
    assert_eq!(valid.claims().resources().unwrap().max_threads, 2);
    for changed in [
        unsigned.replace("\"memory_limit_bytes\":1000", "\"memory_limit_bytes\":2000"),
        unsigned.replace(
            "\"task_memory_limit_bytes\":800",
            "\"task_memory_limit_bytes\":700",
        ),
        unsigned
            .replace("\"max_threads\":2", "\"max_threads\":3")
            .replace("\"partition_slots\":2", "\"partition_slots\":3"),
        unsigned.replace(
            &Uuid::from_u128(4).to_string(),
            &Uuid::from_u128(40).to_string(),
        ),
    ] {
        // Each changed claim is structurally valid. The old signature itself fails.
        Enrollment::decode(&enrollment(&changed, &signature)).unwrap();
        assert!(matches!(
            verify_enrollment(&enrollment(&changed, &signature)),
            Err(Error::Invalid)
        ));
    }
    for changed in [
        unsigned.replace("\"max_threads\":2", "\"max_threads\":0"),
        unsigned.replace("\"max_threads\":2", "\"max_threads\":18446744073709551615"),
        unsigned.replace(
            "\"task_memory_limit_bytes\":800",
            "\"task_memory_limit_bytes\":901",
        ),
        unsigned.replace("\"partition_slots\":2", "\"partition_slots\":1025"),
    ] {
        let signed = sign(SEED, b"meshdb.enroll.v1\0", changed.as_bytes());
        assert!(verify_enrollment(&enrollment(&changed, &signed)).is_err());
    }
    let prefetch = unsigned.replace("\"partition_slots\":2", "\"partition_slots\":3");
    let signature = sign(SEED, b"meshdb.enroll.v1\0", prefetch.as_bytes());
    assert!(verify_enrollment(&enrollment(&prefetch, &signature)).is_ok());
}
#[test]
fn enrollment_verifier_tracks_each_checked_identity_and_key() {
    let baseline = verify_enrollment(&enrollment(ENROLL, ENROLL_SIG)).unwrap();
    for n in [1, 3, 4, 5] {
        let changed = ENROLL.replace(
            &Uuid::from_u128(n).to_string(),
            &Uuid::from_u128(9).to_string(),
        );
        let sig = sign(SEED, b"meshdb.enroll.v1\0", changed.as_bytes());
        let proof = verify_enrollment(&enrollment(&changed, &sig)).unwrap();
        assert_ne!(proof.verifier(), baseline.verifier());
    }
    use ring::signature::KeyPair;
    let key = Ed25519KeyPair::from_seed_unchecked(&bytes::<32>(API_SEED)).unwrap();
    let changed = ENROLL.replace(WORKER_PUBLIC, &hex(key.public_key().as_ref()));
    let sig = sign(API_SEED, b"meshdb.enroll.v1\0", changed.as_bytes());
    let proof = verify_enrollment(&enrollment(&changed, &sig)).unwrap();
    assert_ne!(proof.verifier(), baseline.verifier());
    assert_ne!(proof.public_key(), baseline.public_key());
}
#[test]
fn request_rejects_signature_domain_and_changed_canonical_bytes() {
    let actual = route("/internal/v1/readiness");
    let header = hex(API_REQUEST.as_bytes());
    verify_request(&header, API_REQUEST_SIG, &actual).unwrap();
    let mut bad = bytes::<64>(API_REQUEST_SIG);
    bad[0] ^= 1;
    assert!(matches!(
        verify_request(&header, &hex(&bad), &actual),
        Err(Error::Invalid)
    ));
    for prefix in [
        b"meshdb.http-response.v1\0".as_slice(),
        b"meshdb.http-request.v1",
        b"meshdb.http-request.v1\0\0",
    ] {
        let sig = sign(API_SEED, prefix, API_REQUEST.as_bytes());
        assert!(matches!(
            verify_request(&header, &sig, &actual),
            Err(Error::Invalid)
        ));
    }
    let changed = API_REQUEST.replace("\"sequence\":1", "\"sequence\":2");
    Auth::decode(&hex(changed.as_bytes()), &actual).unwrap();
    assert!(matches!(
        verify_request(&hex(changed.as_bytes()), API_REQUEST_SIG, &actual),
        Err(Error::Invalid)
    ));
    let sig = sign(API_SEED, b"meshdb.http-request.v1\0", changed.as_bytes());
    assert!(verify_request(&hex(changed.as_bytes()), &sig, &actual).is_ok());
}
#[test]
fn reordered_header_and_actual_route_dependency_mismatch() {
    let actual = route("/internal/v1/readiness");
    let changed = API_REQUEST.replacen(
        "{\"version\":1,\"cluster_id\":\"00000000-0000-0000-0000-000000000001\"",
        "{\"cluster_id\":\"00000000-0000-0000-0000-000000000001\",\"version\":1",
        1,
    );
    assert_ne!(changed, API_REQUEST);
    // Both a canonical-message signature and a signature of the reordered bytes fail.
    let sig = sign(API_SEED, b"meshdb.http-request.v1\0", changed.as_bytes());
    for sig in [sig.as_str(), API_REQUEST_SIG] {
        assert!(matches!(
            verify_request(&hex(changed.as_bytes()), sig, &actual),
            Err(Error::Invalid)
        ));
    }
    assert!(matches!(
        verify_request(
            &hex(API_REQUEST.as_bytes()),
            API_REQUEST_SIG,
            &route("/internal/v1/drain")
        ),
        Err(Error::Invalid)
    ));
    let dep = Uuid::from_u128(10).to_string();
    let job = WORKER_REQUEST
        .replace("dependency_open", "job")
        .replace("/internal/v1/dependencies/open", "/internal/v1/jobs")
        .replace(
            "\"job_id\":null",
            &format!("\"job_id\":\"{}\"", Uuid::from_u128(8)),
        )
        .replace(
            "\"attempt_id\":null",
            &format!("\"attempt_id\":\"{}\"", Uuid::from_u128(9)),
        )
        .replace(
            "\"dependency_id\":null",
            &format!("\"dependency_id\":\"{dep}\""),
        );
    let sig = sign(SEED, b"meshdb.http-request.v1\0", job.as_bytes());
    let actual = ActualRoute::new("POST", "/internal/v1/jobs", Some(&dep)).unwrap();
    verify_request(&hex(job.as_bytes()), &sig, &actual).unwrap();
    let other = Uuid::from_u128(11).to_string();
    let actual = ActualRoute::new("POST", "/internal/v1/jobs", Some(&other)).unwrap();
    assert!(matches!(
        verify_request(&hex(job.as_bytes()), &sig, &actual),
        Err(Error::Invalid)
    ));
}
#[test]
fn input_and_fixed_domain_allocation_bounds() {
    assert!(matches!(
        verify_enrollment(&vec![b' '; 16 * 1024 + 1]),
        Err(Error::Limit)
    ));
    assert!(matches!(
        verify_enrollment(&vec![b' '; 16 * 1024]),
        Err(Error::Invalid)
    ));
    let actual = route("/internal/v1/readiness");
    assert!(matches!(
        verify_request(&"00".repeat(4097), API_REQUEST_SIG, &actual),
        Err(Error::Limit)
    ));
    assert!(matches!(
        verify_request(&"00".repeat(4096), API_REQUEST_SIG, &actual),
        Err(Error::Invalid)
    ));
    for (domain, cap) in [(Domain::Enrollment, 16 * 1024), (Domain::Request, 4096)] {
        let body = vec![0; cap];
        let prefix: &[u8] = match domain {
            Domain::Enrollment => b"meshdb.enroll.v1\0",
            Domain::Request => b"meshdb.http-request.v1\0",
        };
        let sig = bytes::<64>(&sign(SEED, prefix, &body));
        verify(bytes(WORKER_PUBLIC), sig, domain, &body).unwrap();
        assert!(matches!(
            verify(bytes(WORKER_PUBLIC), sig, domain, &vec![0; cap + 1]),
            Err(Error::Limit)
        ));
    }
}
#[test]
fn opaque_proofs_have_no_serialization_or_clone() {
    trait AmbiguousIfClone<M> {
        fn marker() {}
    }
    impl<T> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    trait AmbiguousIfSerialize<M> {
        fn marker() {}
    }
    impl<T> AmbiguousIfSerialize<()> for T {}
    impl<T: serde::Serialize> AmbiguousIfSerialize<u8> for T {}
    trait AmbiguousIfDeserialize<M> {
        fn marker() {}
    }
    impl<T> AmbiguousIfDeserialize<()> for T {}
    impl<T: serde::Deserialize<'static>> AmbiguousIfDeserialize<u8> for T {}
    let _ = <EnrollmentProof as AmbiguousIfClone<_>>::marker;
    let _ = <SignedRequest as AmbiguousIfClone<_>>::marker;
    let _ = <EnrollmentProof as AmbiguousIfSerialize<_>>::marker;
    let _ = <SignedRequest as AmbiguousIfSerialize<_>>::marker;
    let _ = <EnrollmentProof as AmbiguousIfDeserialize<_>>::marker;
    let _ = <SignedRequest as AmbiguousIfDeserialize<_>>::marker;
}
