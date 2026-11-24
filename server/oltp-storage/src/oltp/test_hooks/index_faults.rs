//! Index checkpoint and observation files for the mounted index fault fixtures.
use crate::oltp::mvcc::store::lifecycle::LifecycleBinding;
use nabedra_oltp_types::types::{PinIdentity, StoragePin, TxnId};
use serde::Serialize;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

const DIRECTORY: &str = "NABEDRA_OLTP_TEST_INDEX_FAULT_DIR";
pub const WATCHDOG: Duration = Duration::from_secs(120);

pub fn observe_attempt(txn: TxnId, identity: &PinIdentity, pin: StoragePin) {
    if let Some(directory) = std::env::var_os(DIRECTORY) {
        write(
            &PathBuf::from(directory).join(format!("index-attempt-{}.json", txn.request_id)),
            &json!({"txn": txn, "identity": identity, "pin": pin}),
        );
    }
}

pub(crate) fn observe_lifecycle(binding: &LifecycleBinding) {
    if let Some(directory) = std::env::var_os(DIRECTORY).map(PathBuf::from) {
        let request = binding.txn.request_id;
        if directory
            .join(format!("inspect-lifecycle-{request}"))
            .is_file()
        {
            write(
                &directory.join(format!("lifecycle-{request}.json")),
                binding,
            );
        }
    }
}

pub(crate) async fn checkpoint(stage: &str, index: Uuid, observed: &impl Serialize) -> bool {
    let Some(directory) = std::env::var_os(DIRECTORY).map(PathBuf::from) else {
        return false;
    };
    let gate = format!("{index}-{stage}");
    if !directory.join(format!("hold-{gate}")).is_file() {
        return false;
    }
    let reached = directory.join(format!("index-{gate}.json"));
    if reached.exists() {
        return false;
    }
    write(&reached, observed);
    let release = directory.join(format!("release-{gate}"));
    let deadline = nabedra_rt::time::now() + WATCHDOG;
    while !release.is_file() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "mounted index checkpoint watchdog expired: {gate}"
        );
        nabedra_rt::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::read_to_string(&release)
        .expect("read mounted index checkpoint release")
        .trim()
        == "reject"
}

pub fn write(path: &Path, value: &impl Serialize) {
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".next");
    let bytes = serde_json::to_vec(value).expect("encode mounted index observation");
    std::fs::write(&temp, bytes).expect("write mounted index observation");
    std::fs::rename(&temp, path).expect("publish mounted index observation");
}
