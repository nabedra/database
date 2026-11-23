use super::{ReadGrant, ReadScope};
use crate::oltp_replication::{multiplex::RequestError, service_rpc::CallError};
use nabedra_oltp_types::types::WorkerIdentity;
use std::time::Duration;
use uuid::Uuid;
use std::future::Future;
use crate::oltp::read_attempt::fixture::marker;
pub(crate) use crate::oltp::read_attempt::fixture::record;

pub(crate) async fn reply_ready(
    scope: ReadScope,
    group: Uuid,
    ticket: Uuid,
    worker: WorkerIdentity,
) {
    record(scope, group, "reply-ready", ticket, worker);
    wait_for_release(scope, group, "hold", "release").await;
}

pub(crate) fn target(scope: ReadScope, group: Uuid, worker: WorkerIdentity) -> bool {
    marker(scope, group, "target")
        .and_then(|path| std::fs::read_to_string(path).ok())
        .is_none_or(|id| Uuid::parse_str(id.trim()).unwrap() == worker.worker_id)
}

pub(crate) async fn running(grant: ReadGrant) {
    record(
        grant.scope,
        grant.group,
        "running",
        grant.ticket,
        grant.worker,
    );
    wait_for_release(grant.scope, grant.group, "hold-running", "release-running").await;
}

async fn wait_for_release(scope: ReadScope, group: Uuid, hold: &str, release: &str) {
    let Some(hold) = marker(scope, group, hold) else {
        return;
    };
    let release = marker(scope, group, release).unwrap();
    let deadline = nabedra_rt::time::now() + Duration::from_secs(90);
    while hold.is_file() && !release.is_file() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "scheduled page reply fixture timed out"
        );
        nabedra_rt::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(in crate::oltp_replication::service_rpc) async fn disconnect<T>(
    scope: ReadScope,
    group: Uuid,
    response: impl Future<Output = Result<T, CallError>>,
) -> Result<T, CallError> {
    let Some(path) = marker(scope, group, "disconnect") else {
        return response.await;
    };
    if !path.with_file_name("read-scheduler-fixture").is_file() {
        return response.await;
    }
    tokio::select! {
        reply = response => reply,
        _ = async {
            while !path.is_file() {
                nabedra_rt::time::sleep(Duration::from_millis(10)).await;
            }
        } => Err(CallError::Transport(RequestError::Unreachable)),
    }
}
