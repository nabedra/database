use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::{Drain as RootDrain, Registration},
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
struct Fixture {
    key: WorkerSigningKey,
    api: Ed25519KeyPair,
    root: Control,
}
impl Fixture {
    fn new() -> Self {
        let key = WorkerSigningKey::new().unwrap();
        let api = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
        let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
            .unwrap()
            .fingerprint()
            .unwrap();
        let value = json!({"version":1,"cluster_id":id(1),"name":"Synthetic","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":hex(&worker_verifier(id(1),id(3),id(4),id(5),&key.public_key()))},"drain":null}],"intents":[]});
        let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
        Self {
            key,
            api,
            root: roundtrip(&root),
        }
    }
    fn local(&self) -> ReceiverIdentity {
        ReceiverIdentity::new(id(1), id(3), id(4), id(5), &self.key)
    }
    fn pin(&self) -> [u8; 32] {
        self.api.public_key().as_ref().try_into().unwrap()
    }
    fn auth(&self, scope: &str, body: &[u8]) -> (String, ActualRoute) {
        let worker = matches!(scope, "job" | "dependency_open" | "dependency_close");
        let path = match scope {
            "operation" => "/internal/v1/operations",
            "job" => "/internal/v1/jobs",
            "dependency_open" => "/internal/v1/dependencies/open",
            "dependency_close" => "/internal/v1/dependencies/close",
            "readiness" => "/internal/v1/readiness",
            _ => "/internal/v1/drain",
        };
        let public = if worker {
            self.key.public_key()
        } else {
            self.pin()
        };
        let sender = format!(
            r#"{{"role":"{}","process_id":"{}","node_id":{},"incarnation":{},"session_id":{},"public_key":"{}"}}"#,
            if worker { "worker" } else { "api" },
            if worker { id(4) } else { id(2) },
            if worker { json!(id(3)) } else { json!(null) },
            if worker { json!(id(4)) } else { json!(null) },
            if worker { json!(id(5)) } else { json!(null) },
            hex(&public)
        );
        let op = worker || scope == "operation";
        let hash = hex(ring::digest::digest(&ring::digest::SHA256, body).as_ref());
        let text = format!(
            r#"{{"version":1,"cluster_id":"{}","scope":"{scope}","sender":{sender},"receiver":{{"node_id":"{}","incarnation":"{}","session_id":"{}"}},"sequence":1,"method":"POST","path":"{path}","operation_id":{},"admission_id":{},"job_id":{},"attempt_id":{},"request_digest":{},"control_sha256":"{hash}","dependency_id":{}}}"#,
            id(1),
            id(3),
            id(4),
            id(5),
            if op { json!(id(6)) } else { json!(null) },
            if worker { json!(id(7)) } else { json!(null) },
            if scope == "job" {
                json!(id(8))
            } else {
                json!(null)
            },
            if scope == "job" {
                json!(id(9))
            } else {
                json!(null)
            },
            if op {
                json!(if scope == "operation" {
                    hash.clone()
                } else {
                    "a".repeat(64)
                })
            } else {
                json!(null)
            },
            if scope == "job" {
                json!(id(10))
            } else {
                json!(null)
            }
        );
        (
            text,
            ActualRoute::new(
                "POST",
                path,
                if scope == "job" {
                    Some("00000000-0000-0000-0000-00000000000a")
                } else {
                    None
                },
            )
            .unwrap(),
        )
    }
    fn signed(&self, scope: &str, body: &[u8]) -> SignedRequest {
        let (text, route) = self.auth(scope, body);
        self.sign_text(&text, &route, false)
    }
    fn sign_text(&self, text: &str, route: &ActualRoute, worker_as_api: bool) -> SignedRequest {
        let auth = Auth::decode(&hex(text.as_bytes()), route).unwrap();
        let sig = if auth.sender().role() == Role::Worker || worker_as_api {
            let mut input = b"nabedra.http-request.v1\0".to_vec();
            input.extend_from_slice(text.as_bytes());
            if worker_as_api {
                // ring synthetic key, valid API claim signature
                Ed25519KeyPair::from_seed_unchecked(&[8; 32])
                    .unwrap()
                    .sign(&input)
                    .as_ref()
                    .to_vec()
            } else {
                self.key.sign_request(&auth).unwrap().to_vec()
            }
        } else {
            let mut input = b"nabedra.http-request.v1\0".to_vec();
            input.extend_from_slice(text.as_bytes());
            self.api.sign(&input).as_ref().to_vec()
        };
        verify_request(&hex(text.as_bytes()), &hex(&sig), route).unwrap()
    }
}
fn roundtrip(r: &Control) -> Control {
    Control::decode(&r.encode().unwrap()).unwrap()
}
fn draining(r: &mut Control, registration: Registration) {
    r.nodes[0].state = NodeState::Draining;
    r.nodes[0].registration = registration;
    r.nodes[0].drain = Some(RootDrain {
        id: id(11),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    *r = roundtrip(r);
}
const OP:&[u8]=br#"{"request_id":"00000000-0000-0000-0000-000000000006","operation":"table.list","payload":{}}"#;
#[test]
fn request_phase_and_exact_invocation() {
    let mut f = Fixture::new();
    let s = f.signed("operation", OP);
    let c = check_request_content(&s, OP.to_vec()).unwrap();
    let e = check_request_authority(&s, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(e.matches_content(&c));
    assert!(
        !e.matches_content(
            &check_request_content(&f.signed("operation", OP), OP.to_vec()).unwrap()
        )
    );
    draining(&mut f.root, Registration::Verified);
    assert!(check_request_authority(&s, &f.root, &f.local(), &f.pin()).is_err());
}
fn small(field: &str) -> Vec<u8> {
    format!(r#"{{"version":1,"{field}":"{}"}}"#, id(11)).into_bytes()
}
#[test]
fn readiness_recovery_and_pending_route_requirements() {
    let mut f = Fixture::new();
    let b = small("challenge");
    let s = f.signed("readiness", &b);
    assert!(matches!(
        check_request_authority(&s, &f.root, &f.local(), &f.pin())
            .unwrap()
            .pending(),
        PendingLocalChecks::Probe
    ));
    f.root.nodes[0].state = NodeState::Starting;
    f.root.nodes[0].registration = Registration::Registered;
    f.root = roundtrip(&f.root);
    check_request_authority(&s, &f.root, &f.local(), &f.pin()).unwrap();
    draining(&mut f.root, Registration::Registered);
    assert!(check_request_authority(&s, &f.root, &f.local(), &f.pin()).is_err());
    let d = f.signed("drain", &small("drain_id"));
    let e = check_request_authority(&d, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(matches!(e.pending(),PendingLocalChecks::Drain{drain_id} if *drain_id==id(11)));
    // The different parsed ID remains an explicit final-combination obligation.
    let b = format!(r#"{{"version":1,"drain_id":"{}"}}"#, id(12)).into_bytes();
    let d = f.signed("drain", &b);
    let e = check_request_authority(&d, &f.root, &f.local(), &f.pin()).unwrap();
    let c = check_request_content(&d, b).unwrap();
    assert_ne!(
        c.drain().unwrap().drain_id(),
        match e.pending() {
            PendingLocalChecks::Drain { drain_id } => *drain_id,
            _ => panic!(),
        }
    );
    let close = f.signed("dependency_close", &small("permit_id"));
    assert!(
        matches!(check_request_authority(&close,&f.root,&f.local(),&f.pin()).unwrap().pending(),PendingLocalChecks::Close{binding} if binding.operation_id()==id(6)&&binding.admission_id()==id(7)&&binding.coordinator().node_id()==Some(id(3)))
    );
    let job_body = job_bytes();
    let job = f.signed("job", &job_body);
    check_request_content(&job, job_body).unwrap();
    assert!(check_request_authority(&job, &f.root, &f.local(), &f.pin()).is_err());
    draining(&mut f.root, Registration::Verified);
    let e = check_request_authority(&job, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(
        matches!(e.pending(),PendingLocalChecks::Job{binding,permit_id,job_id,attempt_id} if *permit_id==id(10)&&*job_id==id(8)&&*attempt_id==id(9)&&binding.request_digest()==&[0xaa;32])
    );
    let open = f.signed("dependency_open", &small("permit_id"));
    assert!(check_request_authority(&open, &f.root, &f.local(), &f.pin()).is_err());
}
#[test]
fn exact_current_local_binding_and_pinned_api() {
    let f = Fixture::new();
    let s = f.signed("operation", OP);
    for n in 0..7 {
        let mut r = f.root.clone();
        match n {
            0 => r.cluster_id = id(40),
            1 => r.nodes[0].session.as_mut().unwrap().id = id(40),
            2 => r.nodes[0].session.as_mut().unwrap().verifier = "0".repeat(64),
            3 => r.nodes[0].incarnation = Some(id(40)),
            4 => {
                r.nodes[0].incarnation = Some(id(40));
                r.nodes[0].retired_incarnations.push(id(4));
            }
            5 => r.nodes[0].state = NodeState::Lost,
            _ => r.nodes[0].state = NodeState::Stopped,
        };
        let r = roundtrip(&r);
        assert!(check_request_authority(&s, &r, &f.local(), &f.pin()).is_err());
    }
    let key = WorkerSigningKey::new().unwrap();
    for (c, n, i, se, k) in [
        (id(40), id(3), id(4), id(5), &f.key),
        (id(1), id(40), id(4), id(5), &f.key),
        (id(1), id(3), id(40), id(5), &f.key),
        (id(1), id(3), id(4), id(40), &f.key),
        (id(1), id(3), id(4), id(5), &key),
    ] {
        let local = ReceiverIdentity::new(c, n, i, se, k);
        assert!(check_request_authority(&s, &f.root, &local, &f.pin()).is_err());
    }
    let (text, route) = f.auth("operation", OP);
    let fake = Ed25519KeyPair::from_seed_unchecked(&[8; 32]).unwrap();
    let text = text.replace(&hex(&f.pin()), &hex(fake.public_key().as_ref()));
    let signed = f.sign_text(&text, &route, true);
    assert!(check_request_authority(&signed, &f.root, &f.local(), &f.pin()).is_err());
    // Shared API key with another replica process UUID remains eligible.
    let (text, route) = f.auth("operation", OP);
    let text = text.replace(&id(2).to_string(), &id(22).to_string());
    check_request_authority(
        &f.sign_text(&text, &route, false),
        &f.root,
        &f.local(),
        &f.pin(),
    )
    .unwrap();
}
fn reply(f: &Fixture, c: &RequestContext, body: &[u8]) -> SignedReply {
    let hash = hex(ring::digest::digest(&ring::digest::SHA256, body).as_ref());
    let sender = format!(
        r#"{{"role":"worker","process_id":"{}","node_id":"{}","incarnation":"{}","session_id":"{}","public_key":"{}"}}"#,
        id(4),
        id(3),
        id(4),
        id(5),
        hex(&f.key.public_key())
    );
    let v: serde_json::Value = serde_json::from_slice(&c.request.canonical).unwrap();
    // Use retained canonical sender slice, preserving the checked wire field order.
    let original = String::from_utf8(c.request.canonical.to_vec()).unwrap();
    let receiver = original
        .split("\"sender\":")
        .nth(1)
        .unwrap()
        .split(",\"receiver\":")
        .next()
        .unwrap();
    let text = format!(
        r#"{{"version":1,"cluster_id":"{}","request_sequence":1,"scope":{},"sender":{sender},"receiver":{receiver},"status":200,"control_sha256":"{hash}"}}"#,
        id(1),
        v["scope"]
    );
    let a = ReplyAuth::decode(&hex(text.as_bytes())).unwrap();
    verify_reply(
        &hex(text.as_bytes()),
        &hex(&f.key.sign_response(&a).unwrap()),
        200,
        c,
    )
    .unwrap()
}
#[test]
fn replies_allow_accepted_draining_work_but_recheck_context_and_current_binding() {
    let mut f = Fixture::new();
    let (text, route) = f.auth("operation", OP);
    let make = || {
        RequestContext::for_send(
            Auth::decode(&hex(text.as_bytes()), &route).unwrap(),
            OP.to_vec(),
            &f.root,
        )
        .unwrap()
    };
    let c = make();
    let wrong = make();
    let bytes = format!(r#"{{"request_id":"{}","data":[]}}"#, id(6)).into_bytes();
    let s = reply(&f, &c, &bytes);
    let content = check_reply_content(&s, bytes.clone()).unwrap();
    assert!(check_reply_authority(&s, &f.root, &wrong).is_err());
    draining(&mut f.root, Registration::Verified);
    let e = check_reply_authority(&s, &f.root, &c).unwrap();
    assert!(e.matches_content(&content));
    assert!(
        matches!(e.pending(),PendingReplyChecks::AcceptedOperation{operation_id,..} if *operation_id==id(6))
    );
    let other = reply(&f, &c, &bytes);
    assert!(!e.matches_content(&check_reply_content(&other, bytes).unwrap()));
    for n in 0..5 {
        let mut r = f.root.clone();
        match n {
            0 => r.nodes[0].session.as_mut().unwrap().id = id(40),
            1 => r.nodes[0].session.as_mut().unwrap().verifier = "0".repeat(64),
            2 => {
                r.nodes[0].incarnation = Some(id(40));
                r.nodes[0].retired_incarnations.push(id(4));
            }
            3 => r.nodes[0].state = NodeState::Lost,
            _ => r.nodes[0].state = NodeState::Stopped,
        };
        let r = roundtrip(&r);
        assert!(check_reply_authority(&s, &r, &c).is_err());
    }
    let mut r = f.root.clone();
    r.scheduler.failures += 1;
    let r = roundtrip(&r);
    let changed = check_reply_authority(&s, &r, &c).unwrap();
    assert!(e.observation() != changed.observation());
}

fn job_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({"version":1,"cluster_id":id(1),"operation_id":id(6),"job_id":id(8),"attempt_id":id(9),"request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":id(15),"catalog_etag":"token"},"ordinal":0,"table":{"id":id(16),"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":format!("tables/{}/{}.parquet",id(16),id(17)),"row_count":1}},"key":1}}})).unwrap()
}
#[test]
fn accepted_job_reply_and_distinct_coordinator_recheck() {
    let mut f = Fixture::new();
    let coordinator_key = WorkerSigningKey::new().unwrap();
    assert_ne!(coordinator_key.public_key(), f.key.public_key());
    let sign_coordinator = |text: &str, route: &ActualRoute| {
        let auth_hex = hex(text.as_bytes());
        let auth = Auth::decode(&auth_hex, route).unwrap();
        let signature = coordinator_key.sign_request(&auth).unwrap();
        verify_request(&auth_hex, &hex(&signature), route).unwrap()
    };
    // Separate identity and key so substituting the target key cannot pass.
    let mut coordinator = f.root.nodes[0].clone();
    coordinator.node_id = id(13);
    coordinator.incarnation = Some(id(14));
    coordinator.resource_id = Some(format!("external:{}", id(13)));
    coordinator.session.as_mut().unwrap().id = id(15);
    coordinator.session.as_mut().unwrap().verifier = hex(&worker_verifier(
        id(1),
        id(13),
        id(14),
        id(15),
        &coordinator_key.public_key(),
    ));
    f.root.nodes.push(coordinator);
    f.root = roundtrip(&f.root);
    let body = job_bytes();
    let (text, route) = f.auth("job", &body);
    let (sender, rest) = text.split_once(",\"receiver\":").unwrap();
    let sender = sender
        .replace(&id(3).to_string(), &id(13).to_string())
        .replace(&id(4).to_string(), &id(14).to_string())
        .replace(&id(5).to_string(), &id(15).to_string())
        .replace(
            &hex(&f.key.public_key()),
            &hex(&coordinator_key.public_key()),
        );
    let text = format!("{sender},\"receiver\":{rest}");
    let signed = sign_coordinator(&text, &route);
    check_request_content(&signed, body.clone()).unwrap();
    let c = RequestContext::for_send(
        Auth::decode(&hex(text.as_bytes()), &route).unwrap(),
        body.clone(),
        &f.root,
    )
    .unwrap();
    let mut result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    result.as_object_mut().unwrap().remove("table");
    result.as_object_mut().unwrap().remove("task");
    result["result"] =
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":8,"found":false}});
    let result = serde_json::to_vec(&result).unwrap();
    let s = reply(&f, &c, &result);
    check_reply_content(&s, result).unwrap();
    draining(&mut f.root, Registration::Verified);
    f.root.nodes[1].state = NodeState::Draining;
    f.root.nodes[1].drain = f.root.nodes[0].drain.clone();
    f.root = roundtrip(&f.root);
    let e = check_request_authority(&signed, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(
        matches!(e.pending(),PendingLocalChecks::Job{binding,..} if binding.coordinator().node_id()==Some(id(13)) && binding.coordinator().public_key().unwrap()==coordinator_key.public_key())
    );
    assert!(
        matches!(check_reply_authority(&s,&f.root,&c).unwrap().pending(),PendingReplyChecks::AcceptedJob{permit_id,..} if *permit_id==id(10))
    );
    for n in 0..3 {
        let mut r = f.root.clone();
        match n {
            0 => r.nodes[1].registration = Registration::Registered,
            1 => r.nodes[1].session.as_mut().unwrap().verifier = "0".repeat(64),
            _ => r.nodes[1].state = NodeState::Lost,
        };
        let r = roundtrip(&r);
        assert!(check_request_authority(&signed, &r, &f.local(), &f.pin()).is_err());
        assert!(check_reply_authority(&s, &r, &c).is_err());
    }
    // A draining verified coordinator can open on a still-ready target, pending
    // its retained live operation and the target's open local barrier/permit room.
    f.root.nodes[0].state = NodeState::Ready;
    f.root.nodes[0].drain = None;
    f.root = roundtrip(&f.root);
    let body = small("permit_id");
    let (text, route) = f.auth("dependency_open", &body);
    let (sender, rest) = text.split_once(",\"receiver\":").unwrap();
    let sender = sender
        .replace(&id(3).to_string(), &id(13).to_string())
        .replace(&id(4).to_string(), &id(14).to_string())
        .replace(&id(5).to_string(), &id(15).to_string())
        .replace(
            &hex(&f.key.public_key()),
            &hex(&coordinator_key.public_key()),
        );
    let text = format!("{sender},\"receiver\":{rest}");
    let s = sign_coordinator(&text, &route);
    let content = check_request_content(&s, body).unwrap();
    let e = check_request_authority(&s, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(e.matches_content(&content));
    assert!(
        matches!(e.pending(), PendingLocalChecks::Open { binding } if binding.coordinator().public_key().unwrap()==coordinator_key.public_key())
    );
}
#[test]
fn traits_redaction_and_complete_observation() {
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
        ReceiverIdentity,
        EligibleRequest,
        EligibleReply,
        DependencyBinding,
        PendingLocalChecks,
        PendingReplyChecks
    );
    let f = Fixture::new();
    let s = f.signed("operation", OP);
    let e = check_request_authority(&s, &f.root, &f.local(), &f.pin()).unwrap();
    assert!(e.local().same_boot(&f.local()));
    assert_eq!(format!("{:?}", e.local()), "ReceiverIdentity");
    assert_eq!(format!("{e:?}"), "EligibleRequest");
    assert_eq!(format!("{:?}", e.pending()), "PendingLocalChecks");
    assert!(
        matches!(e.pending(),PendingLocalChecks::NewOperation{operation_id,request_digest} if *operation_id==id(6)&&*request_digest==bytes32(s.auth().request_digest().unwrap()).unwrap())
    );
    let mut r = f.root.clone();
    r.scheduler.failures += 1;
    let r = roundtrip(&r);
    let other = check_request_authority(&s, &r, &f.local(), &f.pin()).unwrap();
    assert!(e.observation() != other.observation());
}
#[test]
fn small_reply_requirements_preserve_recovery_and_exact_context_ids() {
    let mut f = Fixture::new();
    for (scope, field) in [
        ("readiness", "challenge"),
        ("dependency_open", "permit_id"),
        ("dependency_close", "permit_id"),
        ("drain", "drain_id"),
    ] {
        if scope == "drain" {
            draining(&mut f.root, Registration::Registered);
        }
        let body = small(field);
        let (text, route) = f.auth(scope, &body);
        let c = RequestContext::for_send(
            Auth::decode(&hex(text.as_bytes()), &route).unwrap(),
            body,
            &f.root,
        )
        .unwrap();
        let body=match scope {"readiness"=>format!(r#"{{"version":1,"challenge":"{}","initialized":true}}"#,id(11)),"dependency_close"=>format!(r#"{{"version":1,"permit_id":"{}","closed":true}}"#,id(11)),"drain"=>format!(r#"{{"version":1,"drain_id":"{}","barrier":true,"idle":false,"heartbeat_sequence":1}}"#,id(11)),_=>String::from_utf8(small(field)).unwrap()}.into_bytes();
        let s = reply(&f, &c, &body);
        let content = check_reply_content(&s, body).unwrap();
        let e = check_reply_authority(&s, &f.root, &c).unwrap();
        assert!(e.matches_content(&content));
        assert_eq!(format!("{e:?}"), "EligibleReply");
        assert_eq!(format!("{:?}", e.pending()), "PendingReplyChecks");
        match e.pending() {
            PendingReplyChecks::Probe { challenge } => assert_eq!(*challenge, id(11)),
            PendingReplyChecks::Open { binding, permit_id }
            | PendingReplyChecks::Close { binding, permit_id } => {
                assert_eq!(*permit_id, id(11));
                assert_eq!(binding.operation_id(), id(6));
                assert_eq!(format!("{binding:?}"), "DependencyBinding");
            }
            PendingReplyChecks::Drain { drain_id } => assert_eq!(*drain_id, id(11)),
            _ => panic!(),
        }
        if scope == "dependency_close" {
            draining(&mut f.root, Registration::Registered);
            check_reply_authority(&s, &f.root, &c).unwrap();
        }
        if scope == "readiness" {
            let mut r = f.root.clone();
            r.nodes[0].state = NodeState::Starting;
            r.nodes[0].registration = Registration::Registered;
            let r = roundtrip(&r);
            check_reply_authority(&s, &r, &c).unwrap();
            let mut r = r;
            draining(&mut r, Registration::Registered);
            assert!(check_reply_authority(&s, &r, &c).is_err());
        }
        if scope == "drain" {
            let mut r = f.root.clone();
            r.nodes[0].drain.as_mut().unwrap().id = id(12);
            let r = roundtrip(&r);
            assert!(check_reply_authority(&s, &r, &c).is_err());
        }
    }
}

fn accepted(f: &Fixture) -> RequestBinding {
    let signed = f.signed("operation", OP);
    let content = check_request_content(&signed, OP.to_vec()).unwrap();
    let eligible = check_request_authority(&signed, &f.root, &f.local(), &f.pin()).unwrap();
    let mut replay = crate::node::auth::ReplayState::new(f.local());
    replay.observe(eligible.observation()).unwrap();
    replay
        .consume(eligible, content)
        .unwrap()
        .into_handoff()
        .into_parts()
        .0
}
#[test]
fn accepted_authority_keeps_invocation_and_allows_draining() {
    let mut f = Fixture::new();
    let binding = accepted(&f);
    let other = accepted(&f);
    let authority = binding
        .check_accepted_authority(&f.root, &f.local(), &f.pin())
        .unwrap();
    assert!(authority.matches_binding(&binding));
    assert!(!authority.matches_binding(&other));
    assert!(authority.local().same_boot(&f.local()));
    assert!(authority.observation() == &OwnershipObservation::new(&f.root).unwrap());
    draining(&mut f.root, Registration::Verified);
    assert!(
        binding
            .check_accepted_authority(&f.root, &f.local(), &f.pin())
            .is_ok()
    );
    assert!(
        check_request_authority(&f.signed("operation", OP), &f.root, &f.local(), &f.pin()).is_err()
    );
    assert!(
        binding
            .check_accepted_authority(&f.root, &f.local(), &[9; 32])
            .is_err()
    );
    draining(&mut f.root, Registration::Registered);
    assert!(
        binding
            .check_accepted_authority(&f.root, &f.local(), &f.pin())
            .is_err()
    );
}

#[test]
fn accepted_authority_rechecks_target_and_coordinator_independently() {
    let mut f = Fixture::new();
    let target_key = WorkerSigningKey::new().unwrap();
    let mut target = f.root.nodes[0].clone();
    target.node_id = id(23);
    target.incarnation = Some(id(24));
    target.session.as_mut().unwrap().id = id(25);
    target.resource_id = Some(format!("external:{}", id(23)));
    target.session.as_mut().unwrap().verifier = hex(&worker_verifier(
        id(1),
        id(23),
        id(24),
        id(25),
        &target_key.public_key(),
    ));
    f.root.nodes.push(target);
    f.root = roundtrip(&f.root);
    let body = job_bytes();
    let (text, route) = f.auth("job", &body);
    let (sender, rest) = text.split_once(",\"receiver\":").unwrap();
    let text = format!(
        "{sender},\"receiver\":{}",
        rest.replace(&id(3).to_string(), &id(23).to_string())
            .replace(&id(4).to_string(), &id(24).to_string())
            .replace(&id(5).to_string(), &id(25).to_string())
    );
    let signed = f.sign_text(&text, &route, false);
    let local = ReceiverIdentity::new(id(1), id(23), id(24), id(25), &target_key);
    let content = check_request_content(&signed, body).unwrap();
    let eligible = check_request_authority(&signed, &f.root, &local, &f.pin()).unwrap();
    let mut replay = crate::node::auth::ReplayState::new(ReceiverIdentity::new(
        id(1),
        id(23),
        id(24),
        id(25),
        &target_key,
    ));
    replay.observe(eligible.observation()).unwrap();
    let binding = replay
        .consume(eligible, content)
        .unwrap()
        .into_handoff()
        .into_parts()
        .0;
    binding
        .check_accepted_authority(&f.root, &local, &f.pin())
        .unwrap();
    for index in 0..2 {
        for change in 0..5 {
            let mut root = f.root.clone();
            let n = &mut root.nodes[index];
            match change {
                0 => n.session.as_mut().unwrap().id = id(101),
                1 => n.session.as_mut().unwrap().verifier = "0".repeat(64),
                2 => {
                    let retired = n.incarnation.replace(id(102)).unwrap();
                    n.retired_incarnations.push(retired);
                }
                3 => n.state = NodeState::Lost,
                _ => {
                    n.state = NodeState::Draining;
                    n.registration = Registration::Registered;
                    n.drain = Some(RootDrain {
                        id: id(103),
                        requested_at_ms: 0,
                        ack_seq: None,
                        confirmed_idle: false,
                    });
                }
            }
            let root = roundtrip(&root);
            assert!(
                binding
                    .check_accepted_authority(&root, &local, &f.pin())
                    .is_err()
            );
        }
    }
    let mut newer = f.root.clone();
    newer.revision += 1;
    newer.change_id = id(104);
    newer.scheduler.failures += 1;
    let authority = binding
        .check_accepted_authority(&roundtrip(&newer), &local, &f.pin())
        .unwrap();
    assert!(authority.matches_binding(&binding));
    assert!(authority.observation() != &OwnershipObservation::new(&f.root).unwrap());
    assert!(
        binding
            .check_accepted_authority(&f.root, &f.local(), &f.pin())
            .is_err()
    );
    let mut foreign = f.root.clone();
    foreign.cluster_id = id(105);
    assert!(
        binding
            .check_accepted_authority(&roundtrip(&foreign), &local, &f.pin())
            .is_err()
    );
}

#[test]
fn checked_capability_is_opaque() {
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
    let _ = <AcceptedRequestAuthority as NoClone<_>>::marker;
    let _ = <AcceptedRequestAuthority as NoSerialize<_>>::marker;
    let _ = <AcceptedRequestAuthority as NoDeserialize<_>>::marker;
}
