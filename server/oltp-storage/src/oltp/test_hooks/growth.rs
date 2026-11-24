//! Scan and Data Raft checkpoints for the growth acceptance fixtures.
use std::{path::PathBuf, time::Duration};

pub(crate) async fn pause_routing_page(read_ts: u64) {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").map(PathBuf::from)
    else {
        return;
    };
    let control = directory.join(format!("routing-page-{read_ts}.pause"));
    if !control.is_file() {
        return;
    }
    let reached = directory.join(format!("routing-page-{read_ts}.reached"));
    std::fs::write(reached, b"").expect("record first routing page traversal");
    let deadline = nabedra_rt::time::now() + Duration::from_secs(120);
    while control.is_file() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "routing page watchdog expired"
        );
        nabedra_rt::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn data_raft_isolated(group: uuid::Uuid) -> bool {
    if std::env::var_os("NABEDRA_OLTP_TEST_SHORT_DATA_SNAPSHOTS").is_none() {
        return false;
    }
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")
        .map(PathBuf::from)
        .is_some_and(|dir| dir.join(format!("data-raft-{group}.isolate")).is_file())
}

pub(crate) fn count_overflow_requested(read_ts: u64) -> bool {
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")
        .map(PathBuf::from)
        .is_some_and(|dir| dir.join(format!("count-overflow-{read_ts}")).is_file())
}
