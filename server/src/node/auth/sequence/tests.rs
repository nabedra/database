use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    state::Control,
};
use crate::node::auth::{
    ActualRoute, RequestContext, check_request_content, verify_request, worker_verifier,
};
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
fn hash(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex(&sha2::Sha256::digest(bytes))
}
fn sender(worker: bool, public: [u8; 32]) -> Value {
    if worker {
        json!({"role":"worker","process_id":id(40),"node_id":id(30),"incarnation":id(40),"session_id":id(50),"public_key":hex(&public)})
    } else {
        json!({"role":"api","process_id":id(2),"node_id":null,"incarnation":null,"session_id":null,"public_key":hex(&public)})
    }
}
fn actual(path: &str) -> ActualRoute {
    ActualRoute::new("POST", path, None).unwrap()
}
fn checked(text: &str, path: &str) -> Auth {
    Auth::decode(&hex(text.as_bytes()), &actual(path)).unwrap()
}
fn template(scope: &str, path: &str, body: &[u8], s: &Value) -> Auth {
    checked(&auth_text(scope, path, body, 99, s), path)
}
fn api() -> ApiSigningKey {
    ApiSigningKey::derive(id(1), &"A".repeat(43)).unwrap()
}
fn root() -> Control {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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
fn auth_text(scope: &str, path: &str, body: &[u8], seq: u64, sender: &Value) -> String {
    let worker = matches!(scope, "job" | "dependency_open" | "dependency_close");
    let op = matches!(
        scope,
        "operation" | "job" | "dependency_open" | "dependency_close"
    );
    let s = sender;
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

#[test]
fn one_counter_composes_with_context_signing_and_content_across_routes_and_targets() {
    let worker = WorkerSigningKey::new().unwrap();
    let api = api();
    for is_worker in [false, true] {
        let s = sender(
            is_worker,
            if is_worker {
                worker.public_key()
            } else {
                api.public_key()
            },
        );
        let (scope, path) = if is_worker {
            ("dependency_open", "/internal/v1/dependencies/open")
        } else {
            ("readiness", "/internal/v1/readiness")
        };
        let body = format!(
            r#" {{"version":1,"{}":"{}"}} "#,
            if is_worker { "permit_id" } else { "challenge" },
            id(11)
        )
        .into_bytes();
        let a = template(scope, path, &body, &s);
        let mut seq = if is_worker {
            SenderSequence::for_worker(id(1), a.sender(), &worker)
        } else {
            SenderSequence::for_api(id(1), a.sender(), &api)
        }
        .unwrap();
        assert_eq!(seq.last, 0);
        for n in 1..=4 {
            let (scope, path) = if is_worker {
                if n % 2 == 0 {
                    ("dependency_close", "/internal/v1/dependencies/close")
                } else {
                    ("dependency_open", "/internal/v1/dependencies/open")
                }
            } else if n % 2 == 0 {
                ("drain", "/internal/v1/drain")
            } else {
                ("readiness", "/internal/v1/readiness")
            };
            let body = format!(
                r#" {{"version":1,"{}":"{}"}} "#,
                if is_worker {
                    "permit_id"
                } else if n % 2 == 0 {
                    "drain_id"
                } else {
                    "challenge"
                },
                id(11)
            )
            .into_bytes();
            let mut root = root();
            if n % 2 == 0 {
                let session = root.nodes[0].session.as_mut().unwrap();
                session.id = id(55);
                session.verifier = hex(&worker_verifier(
                    id(1),
                    id(3),
                    id(4),
                    id(55),
                    &pair(2).public_key().as_ref().try_into().unwrap(),
                ));
            }
            let root = Control::decode(&root.encode().unwrap()).unwrap();
            let text = auth_text(scope, path, &body, 99, &s).replace(
                &format!(r#""session_id":"{}""#, id(5)),
                &format!(
                    r#""session_id":"{}""#,
                    root.nodes[0].session.as_ref().unwrap().id
                ),
            );
            let auth = checked(&text, path);
            let mut expected: Value =
                serde_json::from_slice(&auth.canonical_bytes().unwrap()).unwrap();
            let stamped = seq.stamp(auth).unwrap();
            expected["sequence"] = json!(n);
            assert_eq!(
                serde_json::from_slice::<Value>(&stamped.canonical_bytes().unwrap()).unwrap(),
                expected
            );
            let ctx = RequestContext::for_send(stamped, body.clone(), &root).unwrap();
            assert_eq!(ctx.auth().sequence(), n);
            assert_eq!(ctx.initial_bytes(), body);
            let sig = if is_worker {
                worker.sign_request(ctx.auth())
            } else {
                api.sign_request(ctx.auth())
            }
            .unwrap();
            let signed = verify_request(
                &hex(&ctx.auth().canonical_bytes().unwrap()),
                &hex(&sig),
                &actual(path),
            )
            .unwrap();
            assert!(signed.auth() == ctx.auth());
            check_request_content(&signed, ctx.initial_bytes().to_vec()).unwrap();
        }
    }
}
#[test]
fn constructors_bind_role_public_key_and_api_retained_cluster() {
    let w = WorkerSigningKey::new().unwrap();
    let other = WorkerSigningKey::new().unwrap();
    let api = api();
    let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let a = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, api.public_key()),
    );
    assert!(SenderSequence::for_api(id(1), a.sender(), &api).is_ok());
    assert!(matches!(
        SenderSequence::for_api(id(2), a.sender(), &api),
        Err(Error::Invalid)
    ));
    let foreign = ApiSigningKey::derive(id(2), &"A".repeat(43)).unwrap();
    let foreign_auth = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, foreign.public_key()),
    );
    assert!(matches!(
        SenderSequence::for_api(id(1), foreign_auth.sender(), &foreign),
        Err(Error::Invalid)
    ));
    assert!(matches!(
        SenderSequence::for_worker(id(1), a.sender(), &w),
        Err(Error::Invalid)
    ));
    let same_key_api = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, w.public_key()),
    );
    assert!(matches!(
        SenderSequence::for_worker(id(1), same_key_api.sender(), &w),
        Err(Error::Invalid)
    ));
    let b = format!(r#"{{"version":1,"permit_id":"{}"}}"#, id(11)).into_bytes();
    let worker = template(
        "dependency_open",
        "/internal/v1/dependencies/open",
        &b,
        &sender(true, w.public_key()),
    );
    assert!(SenderSequence::for_worker(id(1), worker.sender(), &w).is_ok());
    assert!(matches!(
        SenderSequence::for_worker(id(1), worker.sender(), &other),
        Err(Error::Invalid)
    ));
    assert!(matches!(
        SenderSequence::for_api(id(1), worker.sender(), &api),
        Err(Error::Invalid)
    ));
    let same_key_worker = template(
        "dependency_open",
        "/internal/v1/dependencies/open",
        &b,
        &sender(true, api.public_key()),
    );
    assert!(matches!(
        SenderSequence::for_api(id(1), same_key_worker.sender(), &api),
        Err(Error::Invalid)
    ));
    let wrong_api = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, w.public_key()),
    );
    assert!(matches!(
        SenderSequence::for_api(id(1), wrong_api.sender(), &api),
        Err(Error::Invalid)
    ));
}
#[test]
fn mismatches_spend_nothing_and_later_failures_do_not_restore_issued_sequences() {
    let key = WorkerSigningKey::new().unwrap();
    let s = sender(true, key.public_key());
    let body = format!(r#"{{"version":1,"permit_id":"{}"}}"#, id(11)).into_bytes();
    let path = "/internal/v1/dependencies/open";
    let text = auth_text("dependency_open", path, &body, 99, &s);
    let good = checked(&text, path);
    let mut seq = SenderSequence::for_worker(id(1), good.sender(), &key).unwrap();
    let mut changes = vec![text.replace(
        &format!(r#""cluster_id":"{}""#, id(1)),
        &format!(r#""cluster_id":"{}""#, id(22)),
    )];
    for field in ["node_id", "session_id", "public_key", "process_id"] {
        let mut changed = s.clone();
        changed[field] = if field == "public_key" {
            json!(hex(&WorkerSigningKey::new().unwrap().public_key()))
        } else {
            json!(id(80))
        };
        if field == "process_id" {
            changed["incarnation"] = json!(id(80));
        }
        changes.push(auth_text("dependency_open", path, &body, 99, &changed));
    }
    for (i, bad) in changes.iter().enumerate() {
        assert!(matches!(seq.stamp(checked(bad, path)), Err(Error::Invalid)));
        assert_eq!(seq.stamp(good.clone()).unwrap().sequence(), i as u64 + 1);
    }
    let issued = seq.stamp(good.clone()).unwrap();
    let n = issued.sequence();
    assert!(RequestContext::for_send(issued, b"{}".to_vec(), &root()).is_err());
    let issued = seq.stamp(good.clone()).unwrap();
    assert_eq!(issued.sequence(), n + 1);
    assert!(
        WorkerSigningKey::new()
            .unwrap()
            .sign_request(&issued)
            .is_err()
    );
    assert_eq!(seq.stamp(good).unwrap().sequence(), n + 2);

    let api = api();
    let path = "/internal/v1/readiness";
    let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let original = sender(false, api.public_key());
    let good = template("readiness", path, &body, &original);
    let mut seq = SenderSequence::for_api(id(1), good.sender(), &api).unwrap();
    for (i, field) in ["process_id", "public_key"].iter().enumerate() {
        let mut changed = original.clone();
        changed[*field] = if *field == "process_id" {
            json!(id(80))
        } else {
            json!(hex(&key.public_key()))
        };
        assert!(matches!(
            seq.stamp(template("readiness", path, &body, &changed)),
            Err(Error::Invalid)
        ));
        assert_eq!(seq.stamp(good.clone()).unwrap().sequence(), i as u64 + 1);
    }
    // A checked worker record can carry the API public key but still has the wrong role.
    let body = format!(r#"{{"version":1,"permit_id":"{}"}}"#, id(11)).into_bytes();
    let wrong_role = template(
        "dependency_open",
        "/internal/v1/dependencies/open",
        &body,
        &sender(true, api.public_key()),
    );
    assert!(matches!(seq.stamp(wrong_role), Err(Error::Invalid)));
    assert_eq!(seq.stamp(good).unwrap().sequence(), 3);
}
#[test]
fn max_is_issued_once_and_exhaustion_is_permanent() {
    let key = api();
    let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let a = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, key.public_key()),
    );
    assert!(matches!(a.clone().with_sequence(0), Err(Error::Invalid)));
    let mut seq = SenderSequence::for_api(id(1), a.sender(), &key).unwrap();
    seq.last = u64::MAX - 1;
    let max = seq.stamp(a.clone()).unwrap();
    assert_eq!(max.sequence(), u64::MAX);
    assert!(
        Auth::decode(
            &hex(&max.canonical_bytes().unwrap()),
            &actual("/internal/v1/readiness")
        )
        .unwrap()
            == max
    );
    let wrong_cluster = String::from_utf8(a.canonical_bytes().unwrap())
        .unwrap()
        .replace(
            &format!(r#""cluster_id":"{}""#, id(1)),
            &format!(r#""cluster_id":"{}""#, id(22)),
        );
    assert!(matches!(
        seq.stamp(checked(&wrong_cluster, "/internal/v1/readiness")),
        Err(Error::Invalid)
    ));
    for _ in 0..3 {
        assert!(matches!(seq.stamp(a.clone()), Err(Error::Exhausted)));
        assert_eq!(seq.last, u64::MAX);
    }
}
#[test]
fn encapsulation_and_value_free_debug() {
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
    let _ = <SenderSequence as NoClone<_>>::marker;
    let _ = <SenderSequence as NoSerialize<_>>::marker;
    let _ = <SenderSequence as NoDeserialize<_>>::marker;
    let key = api();
    let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let a = template(
        "readiness",
        "/internal/v1/readiness",
        &body,
        &sender(false, key.public_key()),
    );
    let mut seq = SenderSequence::for_api(id(1), a.sender(), &key).unwrap();
    assert_eq!(format!("{seq:?}"), "SenderSequence");
    seq.stamp(a).unwrap();
    assert_eq!(format!("{seq:?}"), "SenderSequence");
}
