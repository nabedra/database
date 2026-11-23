//! Deterministic process pauses around real runtime transitions. These hooks
//! exist only in the test executable and never create storage proof objects.
use std::{path::PathBuf, sync::OnceLock, time::Duration};
use uuid::Uuid;

pub(crate) mod index_faults;
pub(crate) mod maintenance;
pub(crate) mod pool_recovery;

pub(crate) use crate::oltp::test_hooks::{
    block_raft_for_test, index_holds, mark, pause, pool_sync, refuse_staged_copy,
    transaction_checkpoint,
};

pub(super) async fn build_archive_split_index(
    runtime: std::sync::Arc<super::worker_runtime::WorkerRuntime>,
    directory: PathBuf,
) {
    use crate::oltp::mvcc::index::IndexDefinition;
    let input = directory.join("split-index.json");
    let deadline = nabedra_rt::time::now() + Duration::from_secs(90);
    let raw = loop {
        match std::fs::read(&input) {
            Ok(raw) => break raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                assert!(
                    nabedra_rt::time::now() < deadline,
                    "no split index request"
                );
                nabedra_rt::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("read private split index request: {error}"),
        }
    };
    assert!(raw.len() <= 4096);
    let definition: IndexDefinition = serde_json::from_slice(&raw).unwrap();
    let build = nabedra_rt::time::timeout_at(
        deadline,
        runtime.create_index(definition, nabedra_rt::ids::uuid_v4(), 1),
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
        "observed_at_ms":nabedra_rt::time::wall_now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()});
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
        "at_unix_ns": nabedra_rt::time::wall_now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64,
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
    std::fs::write(
        directory.join("rolling-copy.json"),
        serde_json::to_vec(&copied).unwrap(),
    )
    .unwrap();
}
pub(super) async fn pause_rolling_copy() {
    if let Some((_, id)) = rolling_test() {
        pause("rolling_copy_complete", id).await;
    }
}
fn rolling_test() -> Option<(PathBuf, Uuid)> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    let input = std::fs::read_to_string(directory.join("rolling-test-id")).ok()?;
    Some((
        directory,
        input.trim().parse().expect("rolling fixture UUID"),
    ))
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
    let deadline = nabedra_rt::time::now() + Duration::from_secs(60);
    while hold.is_file() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "cold {kind} fence watchdog expired"
        );
        nabedra_rt::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Private test-process observation of the local Timestamp service. A follower
/// must pass the same lease/ReadIndex check as the owner of a cached window.
pub(super) async fn timestamp_probe(
    source: &dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource,
) -> Option<serde_json::Value> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    if !directory.join("probe-timestamp").is_file() {
        return None;
    }
    let wall_seconds = nabedra_rt::time::wall_now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let result = nabedra_rt::time::timeout(Duration::from_secs(2), source.allocate_direct(4)).await;
    Some(match result {
        Ok(Ok(range)) => serde_json::json!({ "window": range, "wall_seconds": wall_seconds }),
        other => serde_json::json!({
            "window": null, "wall_seconds": wall_seconds, "error": format!("{other:?}"),
        }),
    })
}

pub(super) fn group_leader_probe() -> Option<Uuid> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    std::fs::read_to_string(directory.join("probe-group-leader"))
        .ok()?
        .parse()
        .ok()
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

pub(crate) fn elect_move_source(request: Uuid) -> bool {
    take_fault("move-elect-source", request).is_some()
}

pub(super) fn take_fault(name: &str, request: Uuid) -> Option<PathBuf> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    std::fs::remove_file(directory.join(format!("{name}-{request}"))).ok()?;
    Some(directory)
}

pub(super) fn hold_o21_ready_view() -> bool {
    o21_ready_hold().is_some()
}

fn o21_ready_hold() -> Option<PathBuf> {
    let directory = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    directory
        .join("o21-hold-ready-view")
        .is_file()
        .then_some(directory)
}

pub(super) fn observe_o21_stale_copy(request: Uuid) {
    if let Some(directory) = o21_ready_hold() {
        std::fs::write(directory.join(format!("o21-stale-copy-{request}")), b"").unwrap();
    }
}

pub(super) fn observe_o21_stale_proposal(body: &[u8]) {
    if let Some(directory) = o21_ready_hold() {
        let request = crate::protocol::parse_request(body).unwrap().request_id;
        std::fs::write(directory.join(format!("o21-stale-proposal-{request}")), b"").unwrap();
    }
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
                            "nabedra_database_server::oltp_runtime::data_worker_process",
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
    const PROBE_SAMPLES: usize = 200;
    use crate::{
        config::WorkerResources,
        oltp::pool::{ByteClass, SharedResourceConfig, WorkerSharedResources},
        oltp_replication::{
            authority::control::{ControlClient, ControlRoutes, PublishedControlRoutes},
            manager::system_recovery::ReplacementCapabilityEndpoint,
            multiplex::{
                AppliedPeerDirectory, EndpointRegistry, PeerDirectory, Purpose, TransportConfig,
                WorkerTransport,
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
        #[serde(default)]
        control_actor: Option<WorkerEnrollment>,
        #[serde(default)]
        revoked_bootstrap_peer: Option<Uuid>,
        #[serde(default)]
        track_control_voters: bool,
        #[serde(default)]
        queue_view: Option<crate::oltp_replication::types::AuthorityView>,
    }
    struct Routes(Vec<Uuid>);
    impl ControlRoutes for Routes {
        fn next(
            &self,
            after: Option<Uuid>,
        ) -> Result<Option<Uuid>, crate::protocol::ProtocolError> {
            Ok(self
                .0
                .iter()
                .find(|id| after.is_none_or(|after| **id > after))
                .copied())
        }
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
    if let Some(view) = config.queue_view {
        use super::{replica::QueueScope, worker_endpoint::QueueRequest};
        use crate::oltp_replication::multiplex::RequestError;

        let [target] = config.targets.as_slice() else {
            panic!("queue enrollment probe requires one target");
        };
        // The fixture pins the outbound receiver. The real receiver checks
        // the caller against its applied Control enrollment.
        directory
            .install_system_trust(1, config.targets.clone(), None, false, false)
            .unwrap();
        for stale in [false, true] {
            let mut enrollment = config.from.clone();
            if stale {
                enrollment.incarnation = nabedra_rt::ids::uuid_v4();
            }
            let client = WorkerTransport::new(
                &enrollment,
                &tls,
                shared.clone(),
                directory.clone(),
                Arc::new(EndpointRegistry::new(shared.clone())),
                TransportConfig::default(),
            )
            .unwrap()
            .client();
            let ping = QueueRequest::Ping {
                scope: QueueScope {
                    cluster_id: config.group.cluster_id,
                    database: config.group.database.clone(),
                },
                view,
            };
            let header = shared.encode_in(ByteClass::Progress, &ping, 4096).unwrap();
            let result = nabedra_rt::time::timeout(
                Duration::from_secs(8),
                client.request_classified(
                    &config.group,
                    WorkerIdentity::from(target),
                    Purpose::QueueControl,
                    header,
                    [None, None, None],
                    16 << 10,
                ),
            )
            .await
            .expect("queue peer probe watchdog");
            if stale {
                assert!(matches!(result, Err(RequestError::Unreachable)));
            } else {
                assert!(result.is_ok(), "current enrollment's queue ping failed");
            }
        }
        return;
    }
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let transport = Arc::new(
        WorkerTransport::new(
            &config.from,
            &tls,
            shared.clone(),
            directory.clone(),
            registry.clone(),
            TransportConfig::default(),
        )
        .unwrap(),
    );
    let client = transport.client();
    if let Some(actor) = config.control_actor {
        registry
            .reserve(config.group.clone())
            .unwrap()
            .bind(
                Purpose::SystemRecovery,
                Arc::new(ReplacementCapabilityEndpoint {
                    group: config.group.clone(),
                    shared: shared.clone(),
                }),
            )
            .unwrap();
        let mut voters: Vec<_> = config.targets.iter().map(|peer| peer.worker_id).collect();
        voters.sort_unstable();
        let published = config
            .track_control_voters
            .then(|| PublishedControlRoutes::new(voters.clone()).unwrap());
        let routes: Arc<dyn ControlRoutes> = match &published {
            Some(published) => published.clone(),
            None => Arc::new(Routes(voters)),
        };
        let (authority_resources, authority) = if published.is_some() {
            // The inventory decoder must use the same resource owner as its
            // Control replies. This listener shares the enrolled actor's
            // identity and has its own listening address.
            assert_eq!(
                WorkerIdentity::from(&actor),
                WorkerIdentity::from(&config.from)
            );
            (shared.clone(), client.clone())
        } else {
            let authority_resources = WorkerSharedResources::new(
                2,
                WorkerResources::default(),
                SharedResourceConfig::default(),
            )
            .unwrap();
            let authority = WorkerTransport::new(
                &actor,
                &tls,
                authority_resources.clone(),
                directory.clone(),
                Arc::new(EndpointRegistry::new(authority_resources.clone())),
                TransportConfig::default(),
            )
            .unwrap()
            .client();
            (authority_resources, authority)
        };
        let control =
            ControlClient::new(authority, authority_resources, config.group.clone(), routes)
                .unwrap();
        if std::env::var_os("NABEDRA_REP5_REFRESH_BOOTSTRAP").is_some() {
            let peers: Vec<_> = config.targets.iter().map(WorkerIdentity::from).collect();
            directory
                .exclude_replaced_bootstrap_peers(&control, &peers)
                .await
                .unwrap();
            assert!(
                directory
                    .resolve(config.revoked_bootstrap_peer.unwrap())
                    .is_err()
            );
        }
        // REP6: the Data-only worker's view, seeded from a Control majority
        // and refreshed by its own task, as in `data_worker_process`.
        directory.seed_remote_revocations(&*control).await.unwrap();
        let (_refresh_stop, refresh_stopped) = tokio::sync::watch::channel(false);
        if let Some(routes) = &published {
            nabedra_rt::task::spawn(
                routes
                    .clone()
                    .run_refresh(control.clone(), refresh_stopped.clone()),
            );
            nabedra_rt::task::spawn(
                directory
                    .clone()
                    .run_refresh(control.clone(), refresh_stopped.clone()),
            );
        }
        nabedra_rt::task::spawn(
            directory
                .clone()
                .run_revocation_refresh(control.clone(), refresh_stopped),
        );
        // D01-style budget evidence: this process's Control reads, by kind.
        let stats = config.directory.join("stats.json");
        let voters_path = config.directory.join("control-voters.json");
        let revoked_path = config.directory.join("revoked-workers.json");
        let directory_view = directory.clone();
        nabedra_rt::task::spawn(async move {
            loop {
                if let Some(routes) = &published {
                    let voters = routes.quorum_voters().unwrap();
                    let temporary = voters_path.with_extension("tmp");
                    std::fs::write(&temporary, serde_json::to_vec(&voters).unwrap()).unwrap();
                    std::fs::rename(&temporary, &voters_path).unwrap();
                    let revoked: Vec<_> = config
                        .targets
                        .iter()
                        .filter(|peer| directory_view.revoked(WorkerIdentity::from(*peer)))
                        .map(|peer| peer.worker_id)
                        .collect();
                    let temporary = revoked_path.with_extension("tmp");
                    std::fs::write(&temporary, serde_json::to_vec(&revoked).unwrap()).unwrap();
                    std::fs::rename(&temporary, &revoked_path).unwrap();
                }
                let temporary = stats.with_extension("tmp");
                let snapshot = crate::oltp_runtime::stage_timing::snapshot();
                if std::fs::write(&temporary, serde_json::to_vec(&snapshot).unwrap()).is_ok() {
                    let _ = std::fs::rename(&temporary, &stats);
                }
                nabedra_rt::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let listener = tokio::net::TcpListener::bind(config.from.endpoint.as_str())
            .await
            .unwrap();
        std::fs::write(config.directory.join("listening"), b"ready").unwrap();
        transport
            .run(listener, std::future::pending())
            .await
            .unwrap();
        return;
    }
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
    for phase in ["before", "after", "restarted"] {
        let marker = config.directory.join(format!("probe-{phase}"));
        nabedra_rt::time::timeout(Duration::from_secs(240), async {
            while !marker.exists() {
                if phase == "after" {
                    // Keep the original sessions open until Control revokes
                    // their identity. Both sessions use the same client below.
                    for target in &config.targets {
                        let _ = nabedra_rt::time::timeout(
                            Duration::from_secs(2),
                            probe(WorkerIdentity::from(target)),
                        )
                        .await;
                    }
                    nabedra_rt::time::sleep(Duration::from_millis(300)).await;
                } else {
                    nabedra_rt::time::sleep(Duration::from_millis(20)).await;
                }
            }
        })
        .await
        .unwrap();
        if phase == "before" {
            // Exchange latency per target, sequential, on established sessions.
            let mut latency = serde_json::Map::new();
            for target in &config.targets {
                let identity = WorkerIdentity::from(target);
                assert!(probe(identity).await);
                let mut samples = Vec::with_capacity(PROBE_SAMPLES);
                for _ in 0..PROBE_SAMPLES {
                    let started = nabedra_rt::time::now_std();
                    assert!(probe(identity).await);
                    samples.push(nabedra_rt::time::elapsed(started).as_micros() as u64);
                }
                latency.insert(target.worker_id.to_string(), samples.into());
            }
            let output = config.directory.join("probe-before-latency.json");
            let temporary = output.with_extension("tmp");
            std::fs::write(&temporary, serde_json::to_vec(&latency).unwrap()).unwrap();
            std::fs::rename(temporary, output).unwrap();
        }
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
#[ignore = "Linux process test with passwordless sudo, run through the CI tests input"]
fn o17_control_voter_refresh_process() {
    let binary = std::env::var_os("NABEDRA_BIN").expect("CI server binary");
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_recovery_operations.py");
    let status = std::process::Command::new("python3")
        .arg(script)
        .arg("--binary")
        .arg(binary)
        .arg("--test-executable")
        .arg(std::env::current_exe().unwrap())
        .args(["--suite", "replacement-voter-refresh"])
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .status()
        .expect("launch O17 Control voter refresh fixture");
    assert!(status.success(), "O17 Control voter refresh fixture failed");
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
