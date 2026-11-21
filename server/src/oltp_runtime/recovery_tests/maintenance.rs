//! Gates at real run sync and snapshot chunk acknowledgements for O05.
use crate::{
    oltp::pool::OltpPool,
    oltp_replication::types::GroupIdentity,
    oltp_runtime::bootstrap::LocalServices,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use uuid::Uuid;

thread_local! {
    static FIXTURE_FLUSH: Cell<bool> = const { Cell::new(false) };
}

fn directory() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    path.join("enable-maintenance-observation")
        .is_file()
        .then_some(path)
}

fn reached(directory: &Path, stage: &str, bytes: &[u8]) -> bool {
    if !directory.join(format!("hold-{stage}")).is_file() {
        return false;
    }
    let marker = directory.join(format!("reached-{stage}"));
    // Claim just one instance of each stage, even if a second compaction
    // or retry arrives while the first one is paused.
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
    {
        Ok(mut file) => {
            use std::io::Write as _;
            file.write_all(bytes).unwrap();
            true
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
        Err(error) => panic!("write maintenance checkpoint {stage}: {error}"),
    }
}

pub(super) fn hold_run_sync(directory: &Path, bytes: u64) -> io::Result<()> {
    let current = std::thread::current();
    let stage = match current.name() {
        Some("kv-compact") => "compaction",
        Some("kv-flush") => "flush",
        _ if FIXTURE_FLUSH.get() => "flush",
        _ => return Ok(()),
    };
    if reached(directory, stage, bytes.to_string().as_bytes()) {
        let release = directory.join(format!("release-{stage}"));
        let deadline = Instant::now() + Duration::from_secs(90);
        while !release.is_file() {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "O05 run sync gate"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    Ok(())
}

pub(crate) async fn hold_snapshot(group: Uuid, transferred: u64, total: u64) {
    let Some(directory) = directory() else { return };
    let stage = format!("snapshot-{group}");
    if reached(
        &directory,
        &stage,
        &serde_json::to_vec(&(transferred, total)).unwrap(),
    ) {
        let release = directory.join(format!("release-{stage}"));
        tokio::time::timeout(Duration::from_secs(90), async {
            while !release.is_file() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("snapshot chunk watchdog");
    }
}

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
        tokio::task::spawn_blocking(move || {
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

struct FlushMarker;

impl FlushMarker {
    fn new() -> Self {
        FIXTURE_FLUSH.set(true);
        Self
    }
}

impl Drop for FlushMarker {
    fn drop(&mut self) {
        FIXTURE_FLUSH.set(false);
    }
}
