//! File commands and checkpoints for the mounted index fault test executable.
use crate::{
    oltp::mvcc::{
        index::{IndexDefinition, IndexRecord},
        store::lifecycle::LifecycleBinding,
    },
    oltp_replication::types::{GroupIdentity, MaintenanceOwnerRef, PinIdentity, StoragePin, TxnId},
    oltp_runtime::worker_runtime::WorkerRuntime,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

const DIRECTORY: &str = "NABEDRA_OLTP_TEST_INDEX_FAULT_DIR";
const WATCHDOG: Duration = Duration::from_secs(120);

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "command", rename_all = "snake_case")]
pub(crate) enum Command {
    Create {
        definition: IndexDefinition,
        build_id: Uuid,
        attempt: u64,
    },
    Resume {
        record: IndexRecord,
    },
    Authority {
        data: Vec<GroupIdentity>,
    },
    ValidationFailure {
        owner: MaintenanceOwnerRef,
        guard: LifecycleBinding,
    },
    RecoverPublication {
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        guard: LifecycleBinding,
        identity: PinIdentity,
        pin: StoragePin,
    },
    Closure {
        owner: MaintenanceOwnerRef,
    },
    Retire {
        owner: MaintenanceOwnerRef,
    },
    Transaction {
        txn: TxnId,
    },
}

impl Command {
    fn watchdog(&self) -> Duration {
        let held_checkpoints = match self {
            Self::Create { .. } => 2, // Backfill and validation.
            Self::Resume { .. } => 2, // Ready plan and transaction decision.
            _ => 0,
        };
        // Budget each hold and the work before, between, and after holds.
        WATCHDOG * (2 * held_checkpoints + 1)
    }
}

pub(crate) fn observe_attempt(txn: TxnId, identity: &PinIdentity, pin: StoragePin) {
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

pub(in crate::oltp_runtime) async fn serve(runtime: Arc<WorkerRuntime>, directory: PathBuf) {
    let metadata =
        std::fs::symlink_metadata(&directory).expect("stat mounted index fault directory");
    assert!(metadata.is_dir() && metadata.permissions().mode() & 0o077 == 0);
    let commands = directory.join("commands");
    while let Ok(entries) = std::fs::read_dir(&commands) {
        for entry in entries {
            let input = entry.expect("list mounted index command").path();
            let Some(id) = input
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<Uuid>().ok())
            else {
                continue;
            };
            if input.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let active = input.with_extension("running");
            std::fs::rename(&input, &active).expect("claim mounted index command");
            let file = std::fs::symlink_metadata(&active).expect("stat mounted index command");
            assert!(file.is_file() && file.len() <= 8192 && file.permissions().mode() & 0o077 == 0);
            let bytes = std::fs::read(&active).expect("read mounted index command");
            let Ok(command) = serde_json::from_slice::<Command>(&bytes) else {
                write(
                    &input.with_extension("result"),
                    &json!({"fixture_error": "invalid command"}),
                );
                continue;
            };
            let watchdog = command.watchdog();
            let result = match nabedra_rt::time::timeout(
                watchdog,
                runtime.run_index_fault_command(command),
            )
            .await
            {
                Ok(Ok(data)) => json!({"data": data}),
                Ok(Err(error)) => json!({"error": error}),
                Err(_) => json!({"fixture_error": "index command watchdog expired"}),
            };
            write(&input.with_extension("result"), &result);
        }
        nabedra_rt::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("mounted index fault command directory became unreadable");
}

fn write(path: &Path, value: &impl Serialize) {
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".next");
    let bytes = serde_json::to_vec(value).expect("encode mounted index observation");
    std::fs::write(&temp, bytes).expect("write mounted index observation");
    std::fs::rename(&temp, path).expect("publish mounted index observation");
}

#[tokio::test(start_paused = true)]
async fn o12_mounted_index_create_watchdog_covers_held_backfill() {
    let create: Command = serde_json::from_value(json!({
        "command": "create",
        "definition": {
            "index_id": nabedra_rt::ids::uuid_v4(),
            "table_id": nabedra_rt::ids::uuid_v4(),
            "name": "by_category_score",
            "columns": ["category", "score"],
            "covering": ["label"],
        },
        "build_id": nabedra_rt::ids::uuid_v4(),
        "attempt": 1,
    }))
    .unwrap();
    let watchdog = create.watchdog();
    let start = nabedra_rt::time::now();
    nabedra_rt::time::timeout(watchdog, async {
        // The mounted RF3 run reached backfill after 48s and held it for 71s.
        nabedra_rt::time::sleep(Duration::from_secs(48)).await;
        nabedra_rt::time::sleep(Duration::from_secs(71)).await;
        nabedra_rt::time::sleep(Duration::from_secs(2)).await;
    })
    .await
    .expect("the command must be able to continue after the backfill release");
    assert!(nabedra_rt::time::elapsed(start) > WATCHDOG);
    assert_eq!(Command::Authority { data: Vec::new() }.watchdog(), WATCHDOG);
    assert!(
        nabedra_rt::time::timeout(watchdog, std::future::pending::<()>())
            .await
            .is_err()
    );
}
