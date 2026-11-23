//! Observe pool maintenance and request a snapshot transfer in O05.
use crate::{
    oltp::{
        pool::OltpPool,
        test_hooks::maintenance::{FlushMarker, directory},
    },
    oltp_replication::types::GroupIdentity,
    oltp_runtime::bootstrap::LocalServices,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde_json::{Value, json};
use std::fs;
use uuid::Uuid;


fn failed(error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        format!("maintenance fixture: {error}"),
        Outcome::Unknown,
    )
}

fn sample(pool: &OltpPool) -> Value {
    let store = pool.kv_store();
    let usage = store.memory_usage();
    let ((flush, compact), (writes, bytes)) = store.queued_io();
    json!({
        "pool": pool.identity().pool_id,
        "merge_and_flush_writers_bytes": usage.run_writers,
        "overhead_bytes": usage.overhead(),
        "flush_queue": flush,
        "compaction_queue": compact,
        "journal_queue_items": writes,
        "journal_queue_bytes": bytes,
    })
}

pub(crate) async fn observe(services: &LocalServices) -> Result<Option<Value>, ProtocolError> {
    let Some(directory) = directory() else {
        return Ok(None);
    };
    let pools = services
        .pools
        .pool_ids()?
        .into_iter()
        .filter_map(|id| services.pools.get(id).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    let flush = directory.join("maintenance-flush");
    if flush.is_file() && fs::remove_file(flush).is_ok() {
        let pool = pools
            .first()
            .ok_or_else(|| failed("missing local pool"))?
            .clone();
        nabedra_rt::task::spawn_blocking(move || {
            let _restore = FlushMarker::new();
            pool.kv_store().flush()
        })
        .await
        .map_err(failed)?
        .map_err(failed)?;
    }
    let transfer = directory.join("maintenance-transfer.json");
    let snapshot_cut = if let Ok(raw) = fs::read(&transfer) {
        fs::remove_file(transfer).map_err(failed)?;
        let (group, target): (GroupIdentity, Uuid) =
            serde_json::from_slice(&raw).map_err(failed)?;
        Some(
            services
                .manager
                .test_transfer_data_snapshot(&group, target)
                .await?,
        )
    } else {
        None
    };
    Ok(Some(
        json!({ "pools": pools.iter().map(sample).collect::<Vec<_>>(),
                   "snapshot_cut": snapshot_cut }),
    ))
}
