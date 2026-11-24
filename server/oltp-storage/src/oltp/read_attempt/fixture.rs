use nabedra_oltp_types::{authority::ReadScope, types::WorkerIdentity};
use std::path::PathBuf;
use uuid::Uuid;

pub fn marker(scope: ReadScope, group: Uuid, stage: &str) -> Option<PathBuf> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    Some(directory.join(format!("read-{}-{group}-{stage}", scope.request.request_id)))
}

pub fn record(
    scope: ReadScope,
    group: Uuid,
    stage: &str,
    ticket: Uuid,
    worker: WorkerIdentity,
) {
    let Some(path) = marker(scope, group, stage) else {
        return;
    };
    if !path.with_file_name("read-scheduler-fixture").is_file() {
        return;
    }
    let bytes = serde_json::to_vec(&serde_json::json!({
        "ticket": ticket, "worker": worker, "scope": scope,
    }))
    .unwrap();
    let temporary = path.with_extension(format!("{}.tmp", nabedra_rt::ids::uuid_v4()));
    std::fs::write(&temporary, bytes).unwrap();
    std::fs::rename(temporary, path).unwrap();
}
