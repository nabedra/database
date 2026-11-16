use axum::{Router, body::Body, http::Request};
use http_body_util::BodyExt;
use nabedra_database_server::{
    api::health,
    engine::Engine,
    storage::{Catalog, Namespace, Storage},
};
use nabedra_database_server::{
    partition_size::PartitionSize,
    worker_pool::{WorkerPool, WorkerThreads},
};
use std::num::NonZeroUsize;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

#[path = "support/storage.rs"]
mod support;
use support::{Fake, ReadGate};

fn fixture() -> (Router, Engine<Fake>, WorkerThreads) {
    let storage = Storage::new(Fake::default(), Namespace::new("health/").unwrap());
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool, PartitionSize::default());
    (health::router(engine.clone()), engine, workers)
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
    let (app, engine, workers) = fixture();
    engine.storage().backend().state.lock().unwrap().denied = true;
    assert_eq!(status(app, "/health/live").await, 200);
    {
        let state = engine.storage().backend().state.lock().unwrap();
        assert_eq!(state.gets, 0);
        assert_eq!(state.puts, 0);
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn readiness_reads_fresh_validated_catalogs_without_writes() {
    let (app, engine, workers) = fixture();
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
    // Readiness validates all supported formats without authorizing migration.
    let mut legacy: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    legacy["format_version"] = 1.into();
    let mut cluster =
        nabedra_database_server::storage::ClusterCatalog::from_catalog(&Catalog::empty()).unwrap();
    let v3 = cluster.encode().unwrap();
    cluster.format_version = 4;
    cluster.fence = Some(nabedra_database_server::storage::CatalogFence {
        run_id: uuid::Uuid::new_v4(),
        scheduler_term: 1,
    });
    for bytes in [
        serde_json::to_vec(&legacy).unwrap(),
        v3,
        cluster.encode().unwrap(),
    ] {
        engine
            .storage()
            .backend()
            .state
            .lock()
            .unwrap()
            .objects
            .insert("health/catalog.json".into(), (bytes, "opaque".into()));
        assert_eq!(status(app.clone(), "/health/ready").await, 200);
    }
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
    unsupported["format_version"] = 3.into();
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
    {
        let state = engine.storage().backend().state.lock().unwrap();
        assert_eq!(state.gets, 9);
        assert_eq!(state.puts, 0);
        assert!(state.put_keys.is_empty());
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn readiness_times_out_a_gated_read_while_live_stays_available() {
    tokio::time::timeout(Duration::from_secs(7), async {
        let (app, engine, workers) = fixture();
        let gate = Arc::new(ReadGate::default());
        engine.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
        let started = tokio::time::Instant::now();
        let ready = tokio::spawn(status(app.clone(), "/health/ready"));
        gate.entered.acquire().await.unwrap().forget();
        assert_eq!(status(app, "/health/live").await, 200);
        assert!(!ready.is_finished());
        assert_eq!(ready.await.unwrap(), 503);
        assert!(started.elapsed() >= Duration::from_secs(5));
        {
            let state = engine.storage().backend().state.lock().unwrap();
            assert_eq!(state.gets, 0);
            assert_eq!(state.puts, 0);
        }
        workers.shutdown().await.unwrap();
    })
    .await
    .expect("health read must finish within seven seconds");
}
