//! Deterministic process pauses around real runtime transitions. These hooks
//! exist only in the test executable and never create storage proof objects.
use std::{path::PathBuf, sync::OnceLock, time::Duration};
use uuid::Uuid;

pub(crate) mod index_faults;
pub(crate) mod index_holds;

struct Barrier {
    stage: String,
    request: Uuid,
    directory: PathBuf,
}
static BARRIER: OnceLock<Option<Barrier>> = OnceLock::new();

pub(crate) fn block_raft_for_test(group: Uuid) -> bool {
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|directory| {
        PathBuf::from(directory).join(format!("hold-raft-{group}")).exists()
    })
}

pub(super) async fn build_archive_split_index(
    runtime: std::sync::Arc<super::worker_runtime::WorkerRuntime>,
    directory: PathBuf,
) {
    use crate::oltp::mvcc::index::IndexDefinition;
    let input = directory.join("split-index.json");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let raw = loop {
        match std::fs::read(&input) {
            Ok(raw) => break raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no split index request"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("read private split index request: {error}"),
        }
    };
    assert!(raw.len() <= 4096);
    let definition: IndexDefinition = serde_json::from_slice(&raw).unwrap();
    let build = tokio::time::timeout_at(
        deadline,
        runtime.create_index(definition, Uuid::new_v4(), 1),
    )
    .await
    .expect("split fixture index build deadline");
    let result = match build {
        Ok(()) => serde_json::json!({ "ready": true }),
        Err(error) => serde_json::json!({ "ready": false, "error": error }),
    };
    std::fs::write(
        directory.join("split-index-result.json"),
        serde_json::to_vec(&result).unwrap(),
    )
    .unwrap();
}

/// Capture a real complete RAM record at an observation boundary. Only the
/// private process fixture can enable this; it grants no runtime authority.
pub(super) fn observe_record(replica: &super::replica::WorkerReplica) {
    if let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") {
        let directory = PathBuf::from(directory);
        if let Ok(raw) = std::fs::read(directory.join("inspect.json")) {
            assert!(raw.len() <= 4096);
            let ids: Vec<Uuid> = serde_json::from_slice(&raw).unwrap();
            assert!(ids.len() <= 16);
            for id in ids {
                write_record(replica, &directory, id);
            }
            return;
        }
    }
    let Some(id) = std::env::var("NABEDRA_OLTP_TEST_REQUEST")
        .ok()
        .and_then(|id| id.parse::<Uuid>().ok())
    else {
        return;
    };
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return;
    };
    write_record(replica, &PathBuf::from(directory), id);
}
pub(super) fn observe_data_voters(manager: &crate::oltp_replication::manager::HostedGroupManager) {
    use crate::oltp_replication::types::GroupIdentity;
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").map(PathBuf::from)
    else {
        return;
    };
    let Ok(raw) = std::fs::read(directory.join("inspect-data-voters.json")) else {
        return;
    };
    assert!(raw.len() <= 4096);
    let groups: Vec<(GroupIdentity, u64)> = serde_json::from_slice(&raw).unwrap();
    assert!(groups.len() <= 8);
    let state = groups
        .iter()
        .map(|(group, node)| manager.data_voter_state(group, *node))
        .collect::<Vec<_>>();
    std::fs::write(
        directory.join("data-voters.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
}

pub(super) fn observe_control_leader(
    manager: &crate::oltp_replication::manager::HostedGroupManager,
    group: Uuid,
) {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").map(PathBuf::from)
    else {
        return;
    };
    if directory.join("inspect-control-leader").exists() {
        let leading = manager.local_leads(group);
        std::fs::write(directory.join("control-leader.json"), leading.to_string()).unwrap();
    }
}

pub(super) fn observe_application_status_lookup(run: Uuid, id: Uuid) {
    if let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") {
        use std::io::Write;
        let directory = PathBuf::from(directory);
        if directory.join(format!("reached-{id}")).exists() {
            let mut trace = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join(format!("application-status-lookups-{id}")))
                .unwrap();
            writeln!(trace, "{run}").unwrap();
        }
    }
}
fn write_record(replica: &super::replica::WorkerReplica, directory: &std::path::Path, id: Uuid) {
    let Some(record) = replica.record(id) else {
        return;
    };
    let context_bytes = record
        .pinned_catalog
        .as_deref()
        .expect("complete accepted context");
    let context: serde_json::Value = serde_json::from_slice(context_bytes).unwrap();
    use sha2::{Digest as _, Sha256};
    let value = serde_json::json!({"worker":replica.local_identity(),
        "authority":replica.authority(),"request_id":id,"run_id":record.run_id,
        "body":String::from_utf8(record.body.clone()).unwrap(),
        "body_hash":crate::oltp_replication::types::Digest::from_bytes(Sha256::digest(&record.body).into()).to_string(),
        "digest":record.digest,"context":context,
        "context_hash":crate::oltp_replication::types::Digest::from_bytes(Sha256::digest(context_bytes).into()).to_string(),
        "accepted":record.accepted,
        "copies_complete":replica.copies_complete(id),
        "terminal":replica.terminal_copy_statement(id).ok(),
        "state":record.state,"version":record.version,"sequence":record.sequence,
        "observed_at_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()});
    let path = directory.join(format!("record-{id}.json"));
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

// An immutable allowlist installed before this child starts. The trace never
// stores a body, context, credential or application result.
fn load_trace_directory(id: Uuid) -> Option<&'static std::path::Path> {
    use std::collections::BTreeSet;
    static LOAD_TRACE: OnceLock<Option<(PathBuf, BTreeSet<Uuid>)>> = OnceLock::new();
    let (directory, selected) = LOAD_TRACE
        .get_or_init(|| {
            let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
            let raw = match std::fs::read(directory.join("identity-load-ids.json")) {
                Ok(raw) => raw,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
                Err(error) => panic!("read identity load selection: {error}"),
            };
            assert!(raw.len() <= 48 << 10);
            let selected: BTreeSet<Uuid> = serde_json::from_slice(&raw).unwrap();
            assert!(selected.len() <= 1100);
            Some((directory, selected))
        })
        .as_ref()?;
    selected.contains(&id).then_some(directory.as_path())
}

pub(super) fn observe_load_copy(
    id: Uuid,
    frame: &super::replica::RecordFrame,
    ack: Option<(crate::oltp_replication::types::WorkerIdentity, Duration)>,
) {
    use sha2::{Digest as _, Sha256};
    use std::io::Write as _;
    let Some(directory) = load_trace_directory(id) else {
        return;
    };
    let header = frame.decode_header().unwrap();
    assert_eq!(header.request_id, id);
    let record = frame.record_version().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(format!("identity-copy-{id}.jsonl")))
        .unwrap();
    let context = frame.sections[1].as_ref().expect("complete copied context");
    let row = serde_json::json!({
        "at_unix_ns": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64,
        "request_id": id, "digest": header.digest, "owner": header.owner,
        "ready": header.ready, "sequence": header.sequence, "version": record.version,
        "record_digest": record.digest,
        "state": header.state, "accepted": header.accepted,
        "context_hash": crate::oltp_replication::types::Digest::from_bytes(Sha256::digest(context.as_ref()).into()),
        "record_header_bytes": frame.header.len(), "section_bytes": header.section_lengths,
        "target": ack.map(|(target, _)| target),
        "elapsed_ns": ack.map(|(_, elapsed)| elapsed.as_nanos() as u64),
    });
    let mut encoded = serde_json::to_vec(&row).unwrap();
    encoded.push(b'\n');
    assert!(file.metadata().unwrap().len() + encoded.len() as u64 <= 64 << 10);
    file.write_all(&encoded).unwrap();
}
pub(super) fn observe_rolling_copy(
    replica: &super::replica::WorkerReplica,
    statement: &crate::oltp_replication::types::QueueCopyStatement,
) {
    let Some((directory, _)) = rolling_test() else {
        return;
    };
    observe_record(replica);
    let copied = serde_json::json!({"statement": statement, "queue":replica.stats()});
    std::fs::write(directory.join("rolling-copy.json"), serde_json::to_vec(&copied).unwrap()).unwrap();
}
pub(super) async fn pause_rolling_copy() {
    if let Some((_, id)) = rolling_test() {
        pause("rolling_copy_complete", id).await;
    }
}
pub(super) async fn refuse_staged_copy(request: Uuid) -> bool {
    const STAGE: &str = "staged-copy-refused";
    if BARRIER.get().and_then(Option::as_ref).is_some_and(|barrier| {
        barrier.request == request && barrier.stage == STAGE
    }) {
        pause(STAGE, request).await;
        return true;
    }
    false
}
fn rolling_test() -> Option<(PathBuf, Uuid)> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    let input = std::fs::read_to_string(directory.join("rolling-test-id")).ok()?;
    Some((directory, input.trim().parse().expect("rolling fixture UUID")))
}
pub(crate) async fn pause(stage: &str, request: Uuid) {
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
                let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
                while !directory.join(format!("release-{request}")).exists() {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "registered barrier watchdog expired"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !barrier.directory.join("release").exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "process barrier watchdog expired"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Allow the process fixture to stop the cold-start coordinator immediately
/// after a real group fence is recorded, before it can publish the run.
pub(super) async fn cold_fence_checkpoint(
    receipt: &crate::oltp_replication::types::GroupFenceReceipt,
) {
    use crate::oltp_replication::types::GroupKind;
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return;
    };
    let kind = match receipt.group.kind {
        GroupKind::Timestamps => "allocator",
        GroupKind::Status { .. } => "status",
        GroupKind::Data { .. } => "shard",
        _ => return,
    };
    let directory = PathBuf::from(directory);
    let hold = directory.join(format!("cold-fence-{kind}.hold"));
    if !hold.is_file() {
        return;
    }
    let observation = directory.join(format!("cold-fence-{kind}.json"));
    let pending = observation.with_extension("tmp");
    std::fs::write(&pending, serde_json::to_vec(receipt).unwrap()).unwrap();
    std::fs::rename(pending, observation).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while hold.is_file() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cold {kind} fence watchdog expired"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Each selected checkpoint belongs to the first actor that reaches it.
/// A reader helping the same transaction must be able to pass a paused actor.
pub(crate) async fn transaction_checkpoint(
    stage: &str,
    request: Uuid,
    observed: &impl serde::Serialize,
) {
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !directory.join(format!("release-{checkpoint}")).is_file() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "transaction checkpoint watchdog expired: {checkpoint}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub(super) fn mark(name: &str, request: Uuid) {
    if let Some(Some(barrier)) = BARRIER.get() {
        if barrier.request == request {
            std::fs::write(barrier.directory.join(name), name.as_bytes())
                .expect("write native settlement observation");
        }
    }
}

pub(super) async fn lose_admission_rejection(request: Uuid) -> bool {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return false;
    };
    let loss = PathBuf::from(directory).join(format!("lose-admission-rejection-{request}"));
    if std::fs::remove_file(loss).is_err() {
        return false;
    }
    pause("admission_rejection_applied", request).await;
    true
}

pub(super) async fn lose_terminal_copy(request: Uuid) -> bool {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return false;
    };
    if std::fs::remove_file(PathBuf::from(directory).join(format!("lose-terminal-copy-{request}")))
        .is_err()
    {
        return false;
    }
    transaction_checkpoint("terminal-copy-lost", request, &request).await;
    true
}

pub(crate) async fn lose_status_inspection(request: Uuid) -> bool {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return false;
    };
    if !PathBuf::from(directory)
        .join(format!("lose-status-inspection-{request}"))
        .is_file()
    {
        return false;
    }
    transaction_checkpoint("status-inspect-unavailable", request, &request).await;
    true
}

pub(super) async fn lose_reservation_reply(
    request: Uuid,
    reply: &crate::oltp_replication::authority::status::StatusReply,
) -> bool {
    let Some(directory) = take_fault("lose-reservation", request) else {
        return false;
    };
    let path = directory.join(format!("reservation-{request}.json"));
    std::fs::write(path, serde_json::to_vec(reply).unwrap()).unwrap();
    pause("reservation-applied", request).await;
    true
}

pub(super) async fn lose_receipt_observation(request: Uuid) -> bool {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return false;
    };
    let repeated = PathBuf::from(directory).join(format!("lose-all-observations-{request}"));
    if !repeated.is_file() && take_fault("lose-observation", request).is_none() {
        return false;
    }
    pause("observation-refused", request).await;
    true
}

pub(super) fn refuse_prepared_commit(request: Uuid) -> bool {
    take_fault("refuse-prepared-commit", request).is_some()
}

fn take_fault(name: &str, request: Uuid) -> Option<PathBuf> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    std::fs::remove_file(directory.join(format!("{name}-{request}"))).ok()?;
    Some(directory)
}

pub(super) fn count_submission(request: Uuid) {
    use std::io::Write as _;
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return;
    };
    let path = PathBuf::from(directory).join(format!("submissions-{request}"));
    if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path) {
        writeln!(file, "{request}").unwrap();
    }
}

pub(super) fn observe_decision(
    stage: &str,
    statement: &crate::oltp_replication::types::DecisionStatement,
) {
    let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    if load_trace_directory(statement.txn.request_id).is_some() {
        use std::io::Write as _;
        static DECISION_OBSERVATION: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _observation = DECISION_OBSERVATION.lock().unwrap();
        let path = directory.join(format!("{stage}-{}.json", statement.txn.request_id));
        let bytes = serde_json::to_vec(statement).unwrap();
        assert!(bytes.len() <= 4 << 10);
        match std::fs::File::create_new(&path) {
            Ok(mut file) => file.write_all(&bytes).unwrap(),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
            Err(error) => panic!("write identity load decision: {error}"),
        }
        return;
    }
    let Ok(raw) = std::fs::read(directory.join("inspect.json")) else {
        return;
    };
    assert!(raw.len() <= 4096);
    let inspected: Vec<Uuid> = serde_json::from_slice(&raw).unwrap();
    assert!(inspected.len() <= 16);
    if inspected.contains(&statement.txn.request_id) {
        let path = directory.join(format!("{stage}-{}.json", statement.txn.request_id));
        std::fs::write(path, serde_json::to_vec(statement).unwrap()).unwrap();
    }
}

#[test]
#[ignore = "internal child process, launched by acceptance_o15"]
fn actual_runtime_child() {
    use tracing_subscriber::prelude::*;
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_filter(
                    tracing_subscriber::filter::Targets::new()
                        .with_target(
                            "nabedra_database_server::oltp_replication::hosted",
                            tracing::Level::WARN,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::typed_log",
                            tracing::Level::WARN,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::service_rpc",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::worker_runtime::recovery",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::worker_runtime",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::authority_fence",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::scoped_worker::maintenance",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::coordinator",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_runtime::bootstrap::registered",
                            tracing::Level::DEBUG,
                        )
                        .with_target(
                            "nabedra_database_server::oltp_replication::manager::data_factory",
                            tracing::Level::DEBUG,
                        ),
                ),
        )
        .try_init();
    if let Some(config) = std::env::var_os("NABEDRA_OLTP_TEST_REPLACEMENT_PEER") {
        super::standalone_network_runtime()
            .unwrap()
            .block_on(replacement_peer_probe(config.into()));
        return;
    }
    if let Some(config) = std::env::var_os("NABEDRA_OLTP_TEST_DATA_WORKER_CONFIG") {
        super::standalone_network_runtime()
            .unwrap()
            .block_on(super::data_worker_process::run(config.into()))
            .unwrap();
        return;
    }
    if let Some(config) = std::env::var_os("NABEDRA_OLTP_TEST_REGISTERED_CONFIG") {
        super::standalone_network_runtime()
            .unwrap()
            .block_on(super::registered_process::run(config.into()))
            .unwrap();
        return;
    }
    let directory =
        std::env::var_os("NABEDRA_OLTP_TEST_DATA_DIR").expect("private child data directory");
    let port = std::env::var("NABEDRA_OLTP_TEST_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let options = super::ServeOptions {
        data_dir: directory.into(),
        database: "test".into(),
        cold_start: std::env::var_os("NABEDRA_OLTP_TEST_COLD").is_some(),
        host: std::net::Ipv4Addr::LOCALHOST.into(),
        port,
        workers: 2,
        queue_size: 8,
        execution_config: std::env::var_os("NABEDRA_OLTP_TEST_EXECUTION_FILE").map(PathBuf::from),
        resources_config: None,
        tls_cert: None,
        tls_key: None,
    };
    super::standalone_network_runtime()
        .unwrap()
        .block_on(super::run(options))
        .unwrap();
}

async fn replacement_peer_probe(config: PathBuf) {
    use crate::{
        config::WorkerResources,
        oltp::pool::{ByteClass, SharedResourceConfig, WorkerSharedResources},
        oltp_replication::{
            multiplex::{
                AppliedPeerDirectory, EndpointRegistry, Purpose, TransportConfig, WorkerTransport,
            },
            types::{GroupIdentity, WorkerEnrollment, WorkerIdentity},
        },
    };
    use std::sync::Arc;

    #[derive(serde::Deserialize)]
    struct Config {
        group: GroupIdentity,
        from: WorkerEnrollment,
        targets: Vec<WorkerEnrollment>,
        certificate: PathBuf,
        private_key: PathBuf,
        authority_ca: PathBuf,
        directory: PathBuf,
    }
    let config: Config = serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let tls = crate::oltp_replication::TlsMaterial::from_pem(
        &std::fs::read(config.certificate).unwrap(),
        &std::fs::read(config.private_key).unwrap(),
        &std::fs::read(config.authority_ca).unwrap(),
    )
    .unwrap();
    let shared = WorkerSharedResources::new(
        1,
        WorkerResources::default(),
        SharedResourceConfig::default(),
    )
    .unwrap();
    let directory = AppliedPeerDirectory::from_bootstrap(
        config.group.clone(),
        config.targets.clone(),
        shared.clone(),
    )
    .unwrap();
    let client = WorkerTransport::new(
        &config.from,
        &tls,
        shared.clone(),
        directory,
        Arc::new(EndpointRegistry::new(shared.clone())),
        TransportConfig::default(),
    )
    .unwrap()
    .client();
    let probe = |target: WorkerIdentity| {
        let header = shared
            .encode_in(ByteClass::Progress, &"advertise", 4096)
            .unwrap();
        let client = client.clone();
        let group = config.group.clone();
        async move {
            client
                .request_classified(
                    &group,
                    target,
                    Purpose::SystemRecovery,
                    header,
                    [None, None, None],
                    16 << 10,
                )
                .await
                .is_ok()
        }
    };
    for phase in ["before", "after"] {
        let marker = config.directory.join(format!("probe-{phase}"));
        tokio::time::timeout(Duration::from_secs(240), async {
            while !marker.exists() {
                if phase == "after" {
                    // Keep the original sessions open until Control revokes
                    // their identity. Both sessions use the same client below.
                    for target in &config.targets {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            probe(WorkerIdentity::from(target)),
                        )
                        .await;
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        })
        .await
        .unwrap();
        let mut observed = Vec::new();
        for target in &config.targets {
            observed.push(probe(WorkerIdentity::from(target)).await);
        }
        let output = config.directory.join(format!("probe-{phase}.json"));
        let temporary = output.with_extension("tmp");
        std::fs::write(&temporary, serde_json::to_vec(&observed).unwrap()).unwrap();
        std::fs::rename(temporary, output).unwrap();
    }
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o15() {
    run_acceptance("O15", "failure-boundaries");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o14() {
    run_acceptance("O14", "queue-survival");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o13() {
    run_acceptance("O13", "admission");
}

fn run_acceptance(scenario: &str, variation: &str) {
    let Some(binary) = std::env::var_os("NABEDRA_OLTP_BINARY") else {
        eprintln!(
            "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"{scenario}\",\"status\":\"not-run\",\"reason\":\"missing actual server executable\"}}"
        );
        return;
    };
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_runtime_acceptance.py");
    let status = std::process::Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(script)
    .args(["--scenario", variation, "--test-executable"])
    .arg(std::env::current_exe().unwrap())
    .arg("--binary")
    .arg(binary)
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch OLTP process assertions");
    assert!(status.success(), "{scenario} process assertions failed");
}
