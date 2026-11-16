//! Unauthenticated health routes, independent of operation admission.
use crate::{engine::Engine, storage::ObjectStore};
use axum::{Router, extract::State, http::StatusCode, routing::get};
use std::time::Duration;

/// Returns a Router<()> with its own supplied state, ready to merge with the
/// operations router. Pass a clone of the same initialized Engine used there.
/// Construction performs no I/O or initialization and creates no coordinator.
/// Readiness gives one fresh, byte-bounded, validated catalog load five seconds
/// to complete, including any retries already performed by the storage adapter.
/// Both routes return empty bodies and emit no diagnostics or configuration.
pub fn router<B: ObjectStore + 'static>(engine: Engine<B>) -> Router {
    Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(ready::<B>))
        .with_state(engine)
}

async fn ready<B: ObjectStore + 'static>(State(engine): State<Engine<B>>) -> StatusCode {
    match tokio::time::timeout(Duration::from_secs(5), async {
        let object = engine
            .storage()
            .read(
                &crate::storage::ObjectKey::catalog(),
                crate::protocol::MAX_CATALOG_BYTES,
            )
            .await?;
        crate::storage::Catalog::decode(&object.bytes)
            .map(|_| ())
            .or_else(|_| crate::storage::ClusterCatalog::decode(&object.bytes).map(|_| ()))
    })
    .await
    {
        Ok(Ok(_)) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}
