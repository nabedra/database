use super::*;
use crate::{
    cluster::configuration::{Template, Timing},
    config::WorkerResources,
    scheduling::ResourceScheduler,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn setup() -> (Control, Signer) {
    let key = Arc::new(WorkerSigningKey::new().unwrap());
    let sender = Sender::worker(id(3), id(4), id(5), hex(&key.public_key())).unwrap();
    let verifier = hex(&auth::worker_verifier(
        id(1),
        id(3),
        id(4),
        id(5),
        &key.public_key(),
    ));
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/synthetic","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let fingerprint = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    let root = json!({"version":1,"cluster_id":id(1),"name":"Control_retries","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":fingerprint,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":verifier},"drain":null}],"intents":[]});
    (
        Control::decode(&serde_json::to_vec(&root).unwrap()).unwrap(),
        Signer::worker(id(1), sender, key).unwrap(),
    )
}
fn context(signer: &Signer, root: &Control, permit: Uuid) -> RequestContext {
    signer
        .context(
            root,
            &root.nodes[0],
            auth::Route::DependencyOpen,
            RequestIds {
                operation: Some(id(6)),
                admission: Some(id(7)),
                digest: Some("a".repeat(64)),
                ..Default::default()
            },
            serde_json::to_vec(&json!({"version":1,"permit_id":permit})).unwrap(),
        )
        .unwrap()
}
#[tokio::test(start_paused = true)]
async fn b02_c07_unsigned_overload_retries_same_permit_with_fresh_sequence_and_owned_allocation() {
    let (root, signer) = setup();
    let scheduler = ResourceScheduler::new(vec![(
        1,
        WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 4096,
            task_memory_limit_bytes: 2048,
            control_memory_reserve_bytes: 1024,
        },
    )])
    .unwrap();
    let grant = scheduler.reserve_remote_memory(1, 1, 2048).await.unwrap();
    let calls = AtomicUsize::new(0);
    let sequences = Mutex::new(Vec::new());
    let permit = id(40);
    let dependency = control_backpressure(|| {
        let context = context(&signer, &root, permit);
        sequences.lock().unwrap().push(context.auth().sequence());
        let payload: serde_json::Value = serde_json::from_slice(context.initial_bytes()).unwrap();
        assert_eq!(payload["permit_id"], permit.to_string());
        assert_eq!(scheduler.usage(), vec![(1, 0, 2048)]);
        let attempt = calls.fetch_add(1, Ordering::SeqCst);
        let bytes = serde_json::to_vec(&json!({"version":1,"permit_id":permit})).unwrap();
        let response = if attempt == 0 {
            Response::builder().status(429).body(Body::empty()).unwrap()
        } else {
            signer
                .response(
                    context.auth(),
                    200,
                    &bytes,
                    Body::from(bytes.clone()),
                    false,
                )
                .unwrap()
        };
        let root = root.clone();
        async move {
            let (status, a, signature, length) = small_response_headers(&response)?;
            let body = read_small(
                response.into_body(),
                length,
                nabedra_rt::time::now() + Duration::from_secs(1),
            )
            .await?;
            checked_small_reply(&a, &signature, status, body, context, &root)
        }
    })
    .await
    .unwrap();
    assert_eq!(dependency.dependency().unwrap().permit_id(), permit);
    assert_eq!(*sequences.lock().unwrap(), [1, 2]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(scheduler.usage(), vec![(1, 0, 2048)]);
    drop(grant);
    assert_eq!(scheduler.usage(), vec![(1, 0, 0)]);
}
#[tokio::test(start_paused = true)]
async fn f03_control_retry_exhaustion_is_finite_and_not_settlement_proof() {
    let scheduler = ResourceScheduler::new(vec![(
        1,
        WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 4096,
            task_memory_limit_bytes: 2048,
            control_memory_reserve_bytes: 1024,
        },
    )])
    .unwrap();
    let grant = scheduler.reserve_remote_memory(1, 1, 2048).await.unwrap();
    let calls = AtomicUsize::new(0);
    let error = control_backpressure(|| async {
        calls.fetch_add(1, Ordering::SeqCst);
        checked_status(429)
    })
    .await
    .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 32);
    assert_eq!(error.code, ErrorCode::Overloaded);
    assert_eq!(error.outcome, Outcome::Unknown);
    assert_eq!(scheduler.usage(), vec![(1, 0, 2048)]);
    drop(grant);
    assert_eq!(scheduler.usage(), vec![(1, 0, 0)]);
}
#[test]
fn u02_unsigned_success_or_bad_signature_cannot_prove_open_or_closed() {
    let response = Response::builder().status(200).body(Body::empty()).unwrap();
    assert_eq!(
        small_response_headers(&response).unwrap_err().code,
        ErrorCode::ValidationError
    );
    let (root, signer) = setup();
    for route in [auth::Route::DependencyOpen, auth::Route::DependencyClose] {
        let bytes = serde_json::to_vec(&if route == auth::Route::DependencyOpen {
            json!({"version":1,"permit_id":id(40)})
        } else {
            json!({"version":1,"permit_id":id(40),"closed":true})
        })
        .unwrap();
        let context = signer
            .context(
                &root,
                &root.nodes[0],
                route,
                RequestIds {
                    operation: Some(id(6)),
                    admission: Some(id(7)),
                    digest: Some("a".repeat(64)),
                    ..Default::default()
                },
                serde_json::to_vec(&json!({"version":1,"permit_id":id(40)})).unwrap(),
            )
            .unwrap();
        let response = signer
            .response(context.auth(), 200, &bytes, Body::empty(), false)
            .unwrap();
        let (status, a, _, _) = small_response_headers(&response).unwrap();
        assert!(checked_small_reply(&a, &"0".repeat(128), status, bytes, context, &root).is_err());
    }
}
