use axum::{Router, body::Body, http::Request};
use http_body_util::BodyExt;
use meshdb_server::{
    api::health,
    engine::Engine,
    storage::{Catalog, Namespace, Storage},
};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

#[path = "support/storage.rs"]
mod support;
use support::{Fake, ReadGate};

fn fixture() -> (Router, Engine<Fake>) {
    let storage = Storage::new(Fake::default(), Namespace::new("health/").unwrap());
    let engine = Engine::new(storage);
    (health::router(engine.clone()), engine)
}

async fn status(app: Router, path: &str) -> u16 {
    let response = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    status
}

#[tokio::test]
async fn live_needs_no_authentication_or_storage_access() {
    let (app, engine) = fixture();
    engine.storage().backend().state.lock().unwrap().denied = true;
    assert_eq!(status(app, "/health/live").await, 200);
    let state = engine.storage().backend().state.lock().unwrap();
    assert_eq!(state.gets, 0);
    assert_eq!(state.puts, 0);
}

#[tokio::test]
async fn readiness_reads_fresh_validated_catalogs_without_writes() {
    let (app, engine) = fixture();
    let valid = Catalog::empty().encode().unwrap();
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert(
            "health/catalog.json".into(),
            (valid.clone(), "opaque".into()),
        );
    assert_eq!(status(app.clone(), "/health/ready").await, 200);
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .read_failure = true;
    assert_eq!(status(app.clone(), "/health/ready").await, 503);
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .read_failure = false;

    let mut unsupported: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    unsupported["format_version"] = 2.into();
    for bytes in [serde_json::to_vec(&unsupported).unwrap(), b"{}".to_vec()] {
        engine
            .storage()
            .backend()
            .state
            .lock()
            .unwrap()
            .objects
            .insert("health/catalog.json".into(), (bytes, "opaque".into()));
        assert_eq!(status(app.clone(), "/health/ready").await, 503);
    }
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .clear();
    assert_eq!(status(app.clone(), "/health/ready").await, 503);
    engine
        .storage()
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert("health/catalog.json".into(), (valid, "opaque".into()));
    assert_eq!(status(app, "/health/ready").await, 200);
    let state = engine.storage().backend().state.lock().unwrap();
    assert_eq!(state.gets, 6);
    assert_eq!(state.puts, 0);
    assert!(state.put_keys.is_empty());
}

#[tokio::test]
async fn readiness_times_out_a_gated_read_while_live_stays_available() {
    tokio::time::timeout(Duration::from_secs(7), async {
        let (app, engine) = fixture();
        let gate = Arc::new(ReadGate::default());
        engine.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
        let started = tokio::time::Instant::now();
        let ready = tokio::spawn(status(app.clone(), "/health/ready"));
        gate.entered.acquire().await.unwrap().forget();
        assert_eq!(status(app, "/health/live").await, 200);
        assert!(!ready.is_finished());
        assert_eq!(ready.await.unwrap(), 503);
        assert!(started.elapsed() >= Duration::from_secs(5));
        let state = engine.storage().backend().state.lock().unwrap();
        assert_eq!(state.gets, 0);
        assert_eq!(state.puts, 0);
    })
    .await
    .expect("health read must finish within seven seconds");
}
