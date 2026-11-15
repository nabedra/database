use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::Registration,
};
use crate::{
    cluster::{member::NodeState, state::Control},
    node::auth::{Role, WorkerSigningKey, replay::ReplayState},
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use uuid::Uuid;
pub(in crate::node::auth) fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
pub(in crate::node::auth) fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
pub(in crate::node::auth) struct Fixture {
    pub(in crate::node::auth) key: WorkerSigningKey,
    api: Ed25519KeyPair,
    pub(in crate::node::auth) root: Control,
}
impl Fixture {
    pub(in crate::node::auth) fn new() -> Self {
        let key = WorkerSigningKey::new().unwrap();
        let api = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
        let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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
    pub(in crate::node::auth) fn local(&self) -> ReceiverIdentity {
        ReceiverIdentity::new(id(1), id(3), id(4), id(5), &self.key)
    }
    pub(in crate::node::auth) fn pin(&self) -> [u8; 32] {
        self.api.public_key().as_ref().try_into().unwrap()
    }
    pub(in crate::node::auth) fn auth(&self, scope: &str, body: &[u8]) -> (String, ActualRoute) {
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
    pub(in crate::node::auth) fn signed(&self, scope: &str, body: &[u8]) -> SignedRequest {
        let (text, route) = self.auth(scope, body);
        self.sign_text(&text, &route)
    }
    pub(in crate::node::auth) fn sign_text(
        &self,
        text: &str,
        route: &ActualRoute,
    ) -> SignedRequest {
        let auth = Auth::decode(&hex(text.as_bytes()), route).unwrap();
        let sig = if auth.sender().role() == Role::Worker {
            self.key.sign_request(&auth).unwrap().to_vec()
        } else {
            let mut input = b"meshdb.http-request.v1\0".to_vec();
            input.extend_from_slice(text.as_bytes());
            self.api.sign(&input).as_ref().to_vec()
        };
        verify_request(&hex(text.as_bytes()), &hex(&sig), route).unwrap()
    }
}

pub(in crate::node::auth) fn roundtrip(r: &Control) -> Control {
    Control::decode(&r.encode().unwrap()).unwrap()
}

pub(in crate::node::auth) fn pair(f: &Fixture, seq: u64) -> (EligibleRequest, CheckedContent) {
    let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
    let (text, route) = f.auth("readiness", &body);
    let text = text.replace("\"sequence\":1", &format!("\"sequence\":{seq}"));
    let signed = f.sign_text(&text, &route);
    (
        check_request_authority(&signed, &f.root, &f.local(), &f.pin()).unwrap(),
        check_request_content(&signed, body).unwrap(),
    )
}
#[test]
fn consumes_original_bytes_once() {
    let f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let (eligible, content) = pair(&f, 1);
    state.observe(eligible.observation()).unwrap();
    let handoff = state.consume(eligible, content).unwrap().into_handoff();
    assert_eq!(
        handoff.bytes(),
        format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).as_bytes()
    );
}

pub(in crate::node::auth) fn identity_pair(
    f: &mut Fixture,
    worker: bool,
    n: usize,
    sequence: u64,
) -> (EligibleRequest, CheckedContent) {
    let body = format!(
        r#"{{"version":1,"{}":"{}"}}"#,
        if worker { "permit_id" } else { "challenge" },
        id(11)
    )
    .into_bytes();
    let (text, route) = f.auth(
        if worker {
            "dependency_open"
        } else {
            "readiness"
        },
        &body,
    );
    let (sender, rest) = text.split_once(",\"receiver\":").unwrap();
    let process = id(10_000 + n as u128);
    let sender = if worker {
        let mut node = f.root.nodes[0].clone();
        node.node_id = id(20_000 + n as u128);
        node.incarnation = Some(process);
        node.session.as_mut().unwrap().id = id(30_000 + n as u128);
        node.session.as_mut().unwrap().verifier = hex(&worker_verifier(
            id(1),
            node.node_id,
            process,
            node.session.as_ref().unwrap().id,
            &f.key.public_key(),
        ));
        node.resource_id = Some(format!("external:{}", node.node_id));
        if f.root.nodes.len() == 1 {
            f.root.nodes.push(node);
        } else {
            f.root.nodes[1] = node;
        }
        f.root.revision += 1;
        f.root.change_id = id(40_000 + f.root.revision as u128);
        f.root = roundtrip(&f.root);
        sender
            .replace(&id(3).to_string(), &id(20_000 + n as u128).to_string())
            .replace(&id(4).to_string(), &process.to_string())
            .replace(&id(5).to_string(), &id(30_000 + n as u128).to_string())
    } else {
        sender.replace(&id(2).to_string(), &process.to_string())
    };
    let text = format!("{sender},\"receiver\":{rest}")
        .replace("\"sequence\":1", &format!("\"sequence\":{sequence}"));
    let signed = f.sign_text(&text, &route);
    (
        check_request_authority(&signed, &f.root, &f.local(), &f.pin()).unwrap(),
        check_request_content(&signed, body).unwrap(),
    )
}
#[test]
fn drain_cross_claim_failure_does_not_spend_sequence() {
    let mut f = Fixture::new();
    f.root.nodes[0].state = NodeState::Draining;
    f.root.nodes[0].registration = Registration::Verified;
    f.root.nodes[0].drain = Some(crate::cluster::member::Drain {
        id: id(11),
        requested_at_ms: 0,
        ack_seq: None,
        confirmed_idle: false,
    });
    f.root = roundtrip(&f.root);
    let mut state = ReplayState::new(f.local());
    state
        .observe(&OwnershipObservation::new(&f.root).unwrap())
        .unwrap();
    for (drain, ok) in [(12, false), (11, true)] {
        let body = format!(r#"{{"version":1,"drain_id":"{}"}}"#, id(drain)).into_bytes();
        let s = f.signed("drain", &body);
        let e = check_request_authority(&s, &f.root, &f.local(), &f.pin()).unwrap();
        let c = check_request_content(&s, body).unwrap();
        assert_eq!(state.consume(e, c).is_ok(), ok);
    }
}
#[test]
fn local_and_complete_observation_mismatch_spend_nothing() {
    let f = Fixture::new();
    let other_key = WorkerSigningKey::new().unwrap();
    let (e, c) = pair(&f, 1);
    let mut state = ReplayState::new(ReceiverIdentity::new(
        id(1),
        id(3),
        id(4),
        id(5),
        &other_key,
    ));
    state.observe(e.observation()).unwrap();
    assert!(matches!(state.consume(e, c), Err(Error::Invalid)));
    let mut other = Fixture::new();
    other.root.revision += 1;
    other.root.change_id = id(888);
    other.root = roundtrip(&other.root);
    let mut state = ReplayState::new(f.local());
    state
        .observe(&OwnershipObservation::new(&other.root).unwrap())
        .unwrap();
    let (e, c) = pair(&f, 1);
    assert!(matches!(state.consume(e, c), Err(Error::Invalid)));
    let mut f = f;
    f.root.revision = other.root.revision + 1;
    f.root.change_id = id(889);
    f.root = roundtrip(&f.root);
    let (e, c) = pair(&f, 1);
    state.observe(e.observation()).unwrap();
    state.consume(e, c).unwrap();
}

fn job_bytes() -> Vec<u8> {
    serde_json::to_vec(&json!({"version":1,"cluster_id":id(1),"operation_id":id(6),"job_id":id(8),"attempt_id":id(9),"request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":id(15),"catalog_etag":"token"},"ordinal":0,"table":{"id":id(16),"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":format!("tables/{}/{}.parquet",id(16),id(17)),"row_count":1}},"key":1}}})).unwrap()
}

#[test]
fn owned_transfer_preserves_each_closed_body_and_association() {
    let mut f = Fixture::new();
    for (scope,body) in [
        ("operation",br#"{ "request_id":"00000000-0000-0000-0000-000000000006", "operation":"table.list", "payload":{} }"#.to_vec()),
        ("job",job_bytes()),
        ("readiness",format!(r#"{{"version":1,"challenge":"{}"}}"#,id(11)).into_bytes()),
        ("dependency_open",format!(r#"{{"version":1,"permit_id":"{}"}}"#,id(11)).into_bytes()),
        ("dependency_close",format!(r#"{{"version":1,"permit_id":"{}"}}"#,id(11)).into_bytes()),
        ("drain",format!(r#"{{"version":1,"drain_id":"{}"}}"#,id(11)).into_bytes()),
    ] {
        if scope=="drain" {
            f.root.nodes[0].state=NodeState::Draining;
            f.root.nodes[0].drain=Some(crate::cluster::member::Drain {id:id(11),requested_at_ms:0,ack_seq:None,confirmed_idle:false});
            f.root=roundtrip(&f.root);
        }
        let signed=f.signed(scope,&body);
        let e=check_request_authority(&signed,&f.root,&f.local(),&f.pin()).unwrap();
        let c=check_request_content(&signed,body.clone()).unwrap();
        let bytes_ptr=c.bytes.as_ptr();
        let parsed_ptr=match &c.parsed { ExpectedContent::Operation(r)=>Some(r.as_ref() as *const Request as usize), ExpectedContent::Job(j)=>Some(j.as_ref() as *const JobEnvelope as usize), _=>None };
        let mut state=ReplayState::new(f.local());
        let observation=e.observation().clone();
        state.observe(&observation).unwrap();
        let replayed=state.consume(e,c).unwrap();
        assert_eq!(format!("{replayed:?}"),"ReplayedRequest");
        let handoff=replayed.into_handoff();
        assert!(Arc::ptr_eq(&handoff.binding().request,&signed.request));
        assert_eq!(handoff.binding().canonical_bytes(),signed.request.canonical.as_ref());
        assert!(handoff.binding().auth() == signed.auth());
        assert_eq!(handoff.bytes(),body);
        assert_eq!(handoff.bytes().as_ptr(),bytes_ptr);
        assert_eq!(format!("{handoff:?}"),"RequestHandoff");
        assert_eq!(format!("{:?}",handoff.binding()),"RequestBinding");
        assert_eq!(format!("{:?}",handoff.pending()),"PendingLocalChecks");
        let (binding,bytes,body,root,local,pending)=handoff.into_parts();
        assert!(Arc::ptr_eq(&binding.request,&signed.request));
        assert_eq!(bytes.as_ptr(),bytes_ptr);
        assert!(root==observation);
        assert!(local.same_boot(&f.local()));
        match (scope,body,pending) {
            ("operation",RequestBody::Operation(r),PendingLocalChecks::NewOperation{..})=>{ assert_eq!(Some(r.as_ref() as *const Request as usize),parsed_ptr); assert_eq!(r.request_id,id(6)); },
            ("job",RequestBody::Job(j),PendingLocalChecks::Job{..})=>{ assert_eq!(Some(j.as_ref() as *const JobEnvelope as usize),parsed_ptr); assert_eq!(j.job_id,id(8)); },
            ("readiness",RequestBody::Readiness(r),PendingLocalChecks::Probe)=>assert_eq!(r.challenge(),id(11)),
            ("dependency_open",RequestBody::Open(r),PendingLocalChecks::Open{..}) | ("dependency_close",RequestBody::Close(r),PendingLocalChecks::Close{..})=>assert_eq!(r.permit_id(),id(11)),
            ("drain",RequestBody::Drain(r),PendingLocalChecks::Drain{drain_id})=>assert_eq!(r.drain_id(),drain_id),
            _=>panic!("wrong owned body/pending alternative"),
        }
    }
}
#[test]
fn opaque_negative_traits_and_debug() {
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
    guard!(
        CombinedRequest,
        RequestHandoff,
        RequestBinding,
        RequestBody,
        ReplayState,
        crate::node::auth::ReplayedRequest
    );
    let f = Fixture::new();
    let (e, c) = pair(&f, 1);
    assert_eq!(
        format!("{:?}", combine_request(e, c).unwrap()),
        "CombinedRequest"
    );
    assert_eq!(format!("{:?}", ReplayState::new(f.local())), "ReplayState");
}

#[test]
fn valid_alternate_receiver_boot_and_key_spend_nothing() {
    let mut f = Fixture::new();
    let mut state = ReplayState::new(f.local());
    let other_key = WorkerSigningKey::new().unwrap();
    for change_key in [false, true] {
        let original = f.root.clone();
        f.root.revision += 1;
        f.root.change_id = id(600_000 + f.root.revision as u128);
        let (node, incarnation, session, key) = if change_key {
            (id(3), id(4), id(5), &other_key)
        } else {
            (id(3), id(4), id(55), &f.key)
        };
        f.root.nodes[0].session.as_mut().unwrap().id = session;
        f.root.nodes[0].session.as_mut().unwrap().verifier = hex(&worker_verifier(
            id(1),
            node,
            incarnation,
            session,
            &key.public_key(),
        ));
        f.root = roundtrip(&f.root);
        let local = ReceiverIdentity::new(id(1), node, incarnation, session, key);
        let body = format!(r#"{{"version":1,"challenge":"{}"}}"#, id(11)).into_bytes();
        let (text, route) = f.auth("readiness", &body);
        let text = if change_key {
            text
        } else {
            text.replace(
                &format!(r#""session_id":"{}""#, id(5)),
                &format!(r#""session_id":"{session}""#),
            )
        };
        let signed = f.sign_text(&text, &route);
        let eligible = check_request_authority(&signed, &f.root, &local, &f.pin()).unwrap();
        let content = check_request_content(&signed, body).unwrap();
        // The supplied observation matches exactly, isolating fixed-local rejection.
        state.observe(eligible.observation()).unwrap();
        assert!(matches!(
            state.consume(eligible, content),
            Err(Error::Invalid)
        ));
        let revision = f.root.revision + 1;
        f.root = original;
        f.root.revision = revision;
        f.root.change_id = id(600_000 + revision as u128);
        f.root = roundtrip(&f.root);
    }
    let (e, c) = pair(&f, 1);
    state.observe(e.observation()).unwrap();
    state.consume(e, c).unwrap();
}
