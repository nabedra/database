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
const API_SEED: &str = "7fa1c9ef87fe2936feb1213c0bc26125ad89f9491e6dd73e434c5630d4bc663e";
const API_PUBLIC: &str = "ec08a2e3c989ad1713ff26c02d71342c46b11dc83c777f37a6df0501462982e1";
const WORKER_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
const ENROLL: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","node_id":"00000000-0000-0000-0000-000000000003","request_id":null,"incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a","endpoint":"http://127.0.0.1:8000","operation_slots":1,"partition_slots":256,"queue_size":4096}"#;
const ENROLL_SIG: &str = "a45f14873fd5072bed0263c34935af842ed2ff5cb6a184ea7ec9af27f5696ef13cc703f7dd7592448272949bd1fa522838bd21689d327c9755f27e1ce657ea06";
const API_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"readiness","sender":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"ec08a2e3c989ad1713ff26c02d71342c46b11dc83c777f37a6df0501462982e1"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/readiness","operation_id":null,"admission_id":null,"job_id":null,"attempt_id":null,"request_digest":null,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const API_REQUEST_SIG: &str = "8638257c2e478e319ca320064b7228e264cda40eb37cf1293e822982422e9ec8c14296dae636144b88c2f875f58855be61e392abf2d088b200de00fcc7a26406";
const WORKER_REQUEST: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","scope":"dependency_open","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"},"receiver":{"node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005"},"sequence":1,"method":"POST","path":"/internal/v1/dependencies/open","operation_id":"00000000-0000-0000-0000-000000000006","admission_id":"00000000-0000-0000-0000-000000000007","job_id":null,"attempt_id":null,"request_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","dependency_id":null}"#;
const WORKER_REQUEST_SIG: &str = "17b4684bae6717986361bb987ee5a183fc575af87d43edd0e1f778feaccbc642cb0f1ac54a7316f2d4e627ea9abd6680c73234c40cc7e4705b82a8db27d8380f";
const RESPONSE: &str = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","request_sequence":1,"scope":"readiness","sender":{"role":"worker","process_id":"00000000-0000-0000-0000-000000000004","node_id":"00000000-0000-0000-0000-000000000003","incarnation":"00000000-0000-0000-0000-000000000004","session_id":"00000000-0000-0000-0000-000000000005","public_key":"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"},"receiver":{"role":"api","process_id":"00000000-0000-0000-0000-000000000002","node_id":null,"incarnation":null,"session_id":null,"public_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"status":200,"control_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
const RESPONSE_SIG: &str = "31b9d8c3f37418ce5fcdf62838695c5c3bdd4106f454f7d8c48d5816412c3de32b90e4e9732dc3c15be297904bb9b8c29e5cd963d3c0e6b7f6a4c6df62bfcf0a";

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
    verify(w.public_key(), b"nabedra.enroll.v1\0", ENROLL, es);
    let asig = a.sign_request(&ar).unwrap();
    assert_eq!(asig, bytes(API_REQUEST_SIG));
    verify(
        a.public_key(),
        b"nabedra.http-request.v1\0",
        API_REQUEST,
        asig,
    );
    let ws = w.sign_request(&wr).unwrap();
    assert_eq!(ws, bytes(WORKER_REQUEST_SIG));
    verify(
        w.public_key(),
        b"nabedra.http-request.v1\0",
        WORKER_REQUEST,
        ws,
    );
    let rs = w.sign_response(&r).unwrap();
    assert_eq!(rs, bytes(RESPONSE_SIG));
    verify(w.public_key(), b"nabedra.http-response.v1\0", RESPONSE, rs);
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
