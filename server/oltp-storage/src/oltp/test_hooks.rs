//! Private test checkpoints shared by storage and the server's acceptance fixtures.
use std::{path::PathBuf, sync::OnceLock, time::Duration};
use uuid::Uuid;

#[doc(hidden)]
pub mod continuation;
#[doc(hidden)]
pub mod growth;
#[doc(hidden)]
pub mod index_faults;
#[doc(hidden)]
pub mod index_holds;
#[doc(hidden)]
pub mod maintenance;
#[doc(hidden)]
pub mod pool_sync;

struct Barrier {
    stage: String,
    request: Uuid,
    directory: PathBuf,
}
static BARRIER: OnceLock<Option<Barrier>> = OnceLock::new();

pub fn block_raft_for_test(group: Uuid) -> bool {
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|directory| {
        PathBuf::from(directory)
            .join(format!("hold-raft-{group}"))
            .exists()
    })
}

pub async fn pause(stage: &str, request: Uuid) {
    if stage == "queued" {
        index_holds::checkpoint(request, index_holds::Checkpoint::ReadPin, || None).await;
    }
    if let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") {
        let directory = PathBuf::from(directory);
        if let Ok(raw) = std::fs::read(directory.join("gates.json")) {
            assert!(raw.len() <= 4096);
            let gates: std::collections::BTreeMap<Uuid, String> =
                serde_json::from_slice(&raw).unwrap();
            assert!(gates.len() <= 16);
            if gates
                .get(&request)
                .is_some_and(|selected| selected == stage)
            {
                use std::io::Write;
                let mut attempts = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(directory.join(format!("attempts-{request}")))
                    .unwrap();
                writeln!(attempts, "{stage}").unwrap();
                drop(attempts);
                std::fs::write(directory.join(format!("reached-{request}")), stage).unwrap();
                let deadline = nabedra_rt::time::now() + Duration::from_secs(120);
                while !directory.join(format!("release-{request}")).exists() {
                    assert!(
                        nabedra_rt::time::now() < deadline,
                        "registered barrier watchdog expired"
                    );
                    nabedra_rt::time::sleep(Duration::from_millis(10)).await;
                }
            }
            return;
        }
    }
    let barrier = BARRIER.get_or_init(|| {
        Some(Barrier {
            stage: std::env::var("NABEDRA_OLTP_TEST_STAGE").ok()?,
            request: std::env::var("NABEDRA_OLTP_TEST_REQUEST")
                .ok()?
                .parse()
                .ok()?,
            directory: std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?.into(),
        })
    });
    let Some(barrier) = barrier else { return };
    if stage != barrier.stage || request != barrier.request {
        return;
    }
    std::fs::write(barrier.directory.join("reached"), stage.as_bytes())
        .expect("write deterministic process barrier");
    let deadline = nabedra_rt::time::now() + Duration::from_secs(60);
    while !barrier.directory.join("release").exists() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "process barrier watchdog expired"
        );
        nabedra_rt::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Each selected checkpoint belongs to the first actor that reaches it.
/// A reader helping the same transaction must be able to pass a paused actor.
pub async fn transaction_checkpoint(stage: &str, request: Uuid, observed: &impl serde::Serialize) {
    if stage == "participants" || stage.starts_with("retry-participants-") {
        index_holds::checkpoint(request, index_holds::Checkpoint::RefreshPrepare, || None).await;
    }
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let Ok(raw) = std::fs::read(directory.join("transaction-gates.json")) else {
        return;
    };
    assert!(raw.len() <= 8192);
    use std::{collections::BTreeMap, io::Write as _};
    let gates: BTreeMap<Uuid, BTreeMap<String, bool>> = serde_json::from_slice(&raw).unwrap();
    let Some(&hold) = gates.get(&request).and_then(|stages| stages.get(stage)) else {
        return;
    };
    let checkpoint = format!("transaction-{request}-{stage}");
    let mut file = match std::fs::File::create_new(directory.join(format!("{checkpoint}.json"))) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if stage.starts_with("resolve-") {
                let mut bytes = serde_json::to_vec(observed).unwrap();
                bytes.push(b'\n');
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(directory.join(format!("{checkpoint}-repeated.jsonl")))
                    .unwrap()
                    .write_all(&bytes)
                    .unwrap();
            }
            return;
        }
        Err(error) => panic!("open transaction checkpoint: {error}"),
    };
    file.write_all(&serde_json::to_vec(observed).unwrap())
        .unwrap();
    drop(file);
    if hold {
        let deadline = nabedra_rt::time::now() + Duration::from_secs(120);
        while !directory.join(format!("release-{checkpoint}")).is_file() {
            assert!(
                nabedra_rt::time::now() < deadline,
                "transaction checkpoint watchdog expired: {checkpoint}"
            );
            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub async fn refuse_staged_copy(request: Uuid) -> bool {
    const STAGE: &str = "staged-copy-refused";
    if BARRIER
        .get()
        .and_then(Option::as_ref)
        .is_some_and(|barrier| barrier.request == request && barrier.stage == STAGE)
    {
        pause(STAGE, request).await;
        return true;
    }
    false
}

pub fn mark(name: &str, request: Uuid) {
    if let Some(Some(barrier)) = BARRIER.get() {
        if barrier.request == request {
            std::fs::write(barrier.directory.join(name), name.as_bytes())
                .expect("write native settlement observation");
        }
    }
}
