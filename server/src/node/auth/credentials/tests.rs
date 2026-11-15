use super::*;
use crate::node::auth::ActualRoute;
use ring::signature::{ED25519, UnparsedPublicKey};
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
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn worker() -> WorkerSigningKey {
    WorkerSigningKey::from_test_seed(bytes(SEED)).unwrap()
}
fn api() -> ApiSigningKey {
    ApiSigningKey::derive(id(1), &"A".repeat(43)).unwrap()
}
fn request(s: &str, path: &str) -> Auth {
    Auth::decode(
        &hex(s.as_bytes()),
        &ActualRoute::new("POST", path, None).unwrap(),
    )
    .unwrap()
}
fn verify(public: [u8; 32], domain: &[u8], body: &str, sig: [u8; 64]) {
    let mut input = domain.to_vec();
    input.extend_from_slice(body.as_bytes());
    UnparsedPublicKey::new(&ED25519, public)
        .verify(&input, &sig)
        .unwrap();
    input.push(0);
    assert!(
        UnparsedPublicKey::new(&ED25519, public)
            .verify(&input, &sig)
            .is_err()
    );
    let mut input = domain.to_vec();
    input.extend_from_slice(body.as_bytes());
    input[0] ^= 1;
    assert!(
        UnparsedPublicKey::new(&ED25519, public)
            .verify(&input, &sig)
            .is_err()
    );
    let mut correct = domain.to_vec();
    correct.extend_from_slice(body.as_bytes());
    let mut other = public;
    other[0] ^= 1;
    assert!(
        UnparsedPublicKey::new(&ED25519, other)
            .verify(&correct, &sig)
            .is_err()
    );
    for suffix in [b"".as_slice(), b"\0\0".as_slice()] {
        let mut wrong = domain[..domain.len() - 1].to_vec();
        wrong.extend_from_slice(suffix);
        wrong.extend_from_slice(body.as_bytes());
        assert!(
            UnparsedPublicKey::new(&ED25519, public)
                .verify(&wrong, &sig)
                .is_err()
        );
    }
}
const SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
const API_SEED: &str = "b36a6d0f436b1e08393c0c9f65ea850039debec28a69c5971512125eb3e6f625";
const API_PUBLIC: &str = "41f19e73e483776f4b552779a73e361242210c5a26b04825db62e69b52db0d95";
const WORKER_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
const ENROLL: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","node_id":"00000000-0000-0000-0000-000000000003","request_id":null,"incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a","endpoint":"http://127.0.0.1:8000","operation_slots":1,"partition_slots":256,"queue_size":4096}"#;
const ENROLL_SIG: &str = "324a8622ab75f207f19f8bd987d901f09b41988608dc36e1723031d3d2fa0eba10d986358908a6783e362b709862b3c04992ba45f84ef056559a5d37fbc80d0c";
const API_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"readiness","sender":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"41f19e73e483776f4b552779a73e361242210c5a26b04825db62e69b52db0d95"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/readiness","operation_id":null,"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":null,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const API_REQUEST_SIG: &str = "7f5559ac5447ee0e35f85c51e7e0a4c4db580d842a3128c78984acfa1b8760d3415461694d6ce6d8aabbc1dd2b44fd40b5af2b9f04c517c6005514eadab1130f";
const WORKER_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"dependency_open","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/dependencies/open","operation_id":"00000000-0000-0000-0000-000000000006","admission_id":"00000000-0000-0000-0000-000000000007","job_id":null,"attempt_id":null,"request_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const WORKER_REQUEST_SIG: &str = "f88ceddd5dca2a0807a62348ff6c073bfca62118ffcf3a8da7306e8f7c0875adfe687a6b7a30d6295f13582ae0ebfc68678d6d1544ad0c8a2e0e1de69e5e6101";
const RESPONSE: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","request_sequence":1,"scope":"readiness","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"},"receiver":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"status":200,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
const RESPONSE_SIG: &str = "cb6891ba8be2ad91873bc19b79a0c68b8808c9770d550cc511742713dda793cd766ca5e986ce3b9feabcf2593a719dcb237a9a20587bb52a1bbe603ebf39a506";

#[test]
fn published_ed25519_and_independent_derivation() {
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
    let _ = <WorkerSigningKey as AmbiguousIfClone<_>>::marker;
    let _ = <ApiSigningKey as AmbiguousIfClone<_>>::marker;
    let _ = <WorkerSigningKey as AmbiguousIfSerialize<_>>::marker;
    let _ = <ApiSigningKey as AmbiguousIfSerialize<_>>::marker;
    let _ = <WorkerSigningKey as AmbiguousIfDeserialize<_>>::marker;
    let _ = <ApiSigningKey as AmbiguousIfDeserialize<_>>::marker;
    let w = worker();
    assert_eq!(hex(&w.public_key()), WORKER_PUBLIC);
    // RFC 8032 section 7.1 test 1. Seed/public copied from cached ring binary fixtures.
    let sig = bytes::<64>(
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
    );
    assert_eq!(w.pair.sign(b"").as_ref(), sig);
    UnparsedPublicKey::new(&ED25519, w.public_key())
        .verify(b"", &sig)
        .unwrap();
    let a = api();
    assert_eq!(hex(&a.public_key()), API_PUBLIC);
    assert_eq!(
        Ed25519KeyPair::from_seed_unchecked(&bytes::<32>(API_SEED))
            .unwrap()
            .public_key()
            .as_ref(),
        a.public_key()
    );
    assert_eq!(api().public_key(), a.public_key());
    assert_ne!(
        ApiSigningKey::derive(id(2), &"A".repeat(43))
            .unwrap()
            .public_key(),
        a.public_key()
    );
    for secret in ["B".repeat(43), "C".repeat(43)] {
        assert_ne!(
            ApiSigningKey::derive(id(1), &secret).unwrap().public_key(),
            a.public_key()
        );
    }
    for bad in [
        "A".repeat(42),
        "A".repeat(4097),
        " ".repeat(43),
        "é".repeat(43),
        "\n".repeat(43),
    ] {
        assert!(matches!(
            ApiSigningKey::derive(id(1), &bad),
            Err(Error::Invalid)
        ));
    }
    assert!(ApiSigningKey::derive(id(1), &"~".repeat(4096)).is_ok());
    assert_eq!(format!("{w:?}"), "WorkerSigningKey");
    assert_eq!(format!("{a:?}"), "ApiSigningKey");
    assert_eq!(Error::Invalid.to_string(), "Invalid");
    assert_eq!(format!("{:?}", Error::Invalid), "Invalid");
    assert!(std::error::Error::source(&Error::Invalid).is_none());
    assert_eq!(WorkerSigningKey::new().unwrap().public_key().len(), 32);
}
#[test]
fn exact_verifier_uuid_bytes_and_separation() {
    let public = worker().public_key();
    let expected = bytes::<32>("1214ed9c37ee28fe7e9af7c1731deef2dedd682bfdf2979f81e3fa3b286f11e8");
    assert_eq!(
        worker_verifier(id(1), id(3), id(4), id(5), &public),
        expected
    );
    for ids in [
        [2, 3, 4, 5],
        [1, 2, 4, 5],
        [1, 3, 2, 5],
        [1, 3, 4, 2],
        [3, 1, 4, 5],
    ] {
        assert_ne!(
            worker_verifier(id(ids[0]), id(ids[1]), id(ids[2]), id(ids[3]), &public),
            expected
        );
    }
    let mut other = public;
    other[0] ^= 1;
    assert_ne!(
        worker_verifier(id(1), id(3), id(4), id(5), &other),
        expected
    );
}
#[test]
fn exact_typed_domain_signatures_and_local_mismatch() {
    let w = worker();
    let a = api();
    let e = EnrollmentUnsigned::decode(ENROLL.as_bytes()).unwrap();
    let ar = request(API_REQUEST, "/internal/v1/readiness");
    let wr = request(WORKER_REQUEST, "/internal/v1/dependencies/open");
    let r = ReplyAuth::decode(&hex(RESPONSE.as_bytes())).unwrap();
    let es = w.sign_enrollment(&e).unwrap();
    assert_eq!(es, bytes(ENROLL_SIG));
    verify(w.public_key(), b"meshdb.enroll.v1\0", ENROLL, es);
    let asig = a.sign_request(&ar).unwrap();
    assert_eq!(asig, bytes(API_REQUEST_SIG));
    verify(
        a.public_key(),
        b"meshdb.http-request.v1\0",
        API_REQUEST,
        asig,
    );
    let ws = w.sign_request(&wr).unwrap();
    assert_eq!(ws, bytes(WORKER_REQUEST_SIG));
    verify(
        w.public_key(),
        b"meshdb.http-request.v1\0",
        WORKER_REQUEST,
        ws,
    );
    let rs = w.sign_response(&r).unwrap();
    assert_eq!(rs, bytes(RESPONSE_SIG));
    verify(w.public_key(), b"meshdb.http-response.v1\0", RESPONSE, rs);
    assert!(matches!(w.sign_request(&ar), Err(Error::Invalid)));
    assert!(matches!(a.sign_request(&wr), Err(Error::Invalid)));
    let other = WorkerSigningKey::from_test_seed([1; 32]).unwrap();
    assert!(matches!(other.sign_enrollment(&e), Err(Error::Invalid)));
    assert!(matches!(other.sign_request(&wr), Err(Error::Invalid)));
    assert!(matches!(other.sign_response(&r), Err(Error::Invalid)));
    let changed = API_REQUEST.replace(
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000009",
    );
    assert!(matches!(
        a.sign_request(&request(&changed, "/internal/v1/readiness")),
        Err(Error::Invalid)
    ));
    let changed = API_REQUEST.replace(API_PUBLIC, WORKER_PUBLIC);
    assert!(matches!(
        a.sign_request(&request(&changed, "/internal/v1/readiness")),
        Err(Error::Invalid)
    ));
    let changed = API_REQUEST.replace("\"sequence\":1", "\"sequence\":2");
    let altered = request(&changed, "/internal/v1/readiness");
    assert_ne!(a.sign_request(&altered).unwrap(), asig);
    let changed = API_REQUEST.replace(
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000008",
    );
    assert!(
        a.sign_request(&request(&changed, "/internal/v1/readiness"))
            .is_ok()
    );
}

#[test]
fn fixed_signing_caps() {
    let w = worker();
    for (domain, cap) in [
        (Domain::Enrollment, 16 * 1024),
        (Domain::Request, 4096),
        (Domain::Response, 4096),
    ] {
        assert!(matches!(
            sign(&w.pair, domain, &vec![0; cap + 1]),
            Err(Error::Limit)
        ));
    }
    assert!(sign(&w.pair, Domain::Enrollment, &vec![0; 16 * 1024]).is_ok());
    assert!(sign(&w.pair, Domain::Request, &vec![0; 4096]).is_ok());
    assert!(sign(&w.pair, Domain::Response, &vec![0; 4096]).is_ok());
}
