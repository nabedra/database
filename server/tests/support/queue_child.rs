//! Fixture-only worker process. Authority/fences and deterministic reads here
//! are supplied by the test, not production Control Raft or a data engine.
use nabedra_database_server::{
    config::WorkerResources,
    oltp::pool::{SharedResourceConfig, WorkerSharedResources},
    oltp_replication::{TlsMaterial, multiplex::*, types::*},
    oltp_runtime::{coordinator::QueueCoordinator, replica::*, worker_endpoint::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, QueueLimits, RequestState},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use uuid::Uuid;

pub const CONFIG_ENV: &str = "NABEDRA_QUEUE_CHILD_CONFIG";
pub const TLS_LISTENER_ENV: &str = "NABEDRA_QUEUE_CHILD_TLS_FD";
pub const ADMIN_LISTENER_ENV: &str = "NABEDRA_QUEUE_CHILD_ADMIN_FD";
#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub local: WorkerEnrollment,
    pub members: Vec<WorkerIdentity>,
    pub enrollments: Vec<WorkerEnrollment>,
    pub ready: ReadyMembership,
    pub owner: WorkerIdentity,
    pub group: GroupIdentity,
    pub ticket: Option<JoinTicket>,
    pub fixture_cold_authorization: Uuid,
    pub directory: PathBuf,
    pub tls_index: u64,
    pub admin_endpoint: String,
    pub admin_token: Uuid,
    pub proxy_upstream: Option<(String, Uuid)>,
    pub memory_limit_bytes: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Payload,
    Acceptance,
    Terminal,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Admin {
    Inspect {
        request_id: Uuid,
        run_id: Option<Uuid>,
    },
    Admit {
        body: String,
    },
    AdmitWide {
        request_id: Uuid,
        body_bytes: usize,
    },
    Fingerprint {
        request_id: Uuid,
    },
    Resume {
        request_id: Uuid,
    },
    Execute {
        request_id: Uuid,
    },
    Gate {
        stage: Option<Stage>,
    },
    GateState,
    Fence {
        ready: ReadyMembership,
        owner: WorkerIdentity,
        members: Vec<WorkerIdentity>,
    },
    BeginJoin {
        ticket: JoinTicket,
    },
    Page {
        snapshot: JoinSnapshot,
        phase: JoinPhase,
        after: Option<u64>,
        target: WorkerIdentity,
    },
    PreviewPage {
        snapshot: JoinSnapshot,
        phase: JoinPhase,
        after: Option<u64>,
    },
    Seal {
        snapshot: JoinSnapshot,
    },
    JoinStatement,
    PeerState {
        target: WorkerIdentity,
        request_id: Uuid,
    },
    PeerResult {
        target: WorkerIdentity,
        request_id: Uuid,
        version: u64,
    },
}
#[derive(Serialize, Deserialize)]
struct AdminEnvelope {
    token: Uuid,
    command: Admin,
}

pub async fn command(endpoint: &str, token: Uuid, command: Admin) -> Result<Value, String> {
    let mut socket = TcpStream::connect(endpoint)
        .await
        .map_err(|e| e.to_string())?;
    let bytes = serde_json::to_vec(&AdminEnvelope { token, command }).unwrap();
    if bytes.len() > 128 << 10 {
        return Err("fixture command exceeds limit".into());
    }
    socket
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|e| e.to_string())?;
    socket.write_all(&bytes).await.map_err(|e| e.to_string())?;
    let len = socket.read_u32().await.map_err(|e| e.to_string())? as usize;
    if len > 256 << 10 {
        return Err("fixture response exceeds limit".into());
    }
    let mut bytes = vec![0; len];
    socket
        .read_exact(&mut bytes)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

struct FixtureAuthority(Mutex<QueueAuthority>);
impl AuthorityReader for FixtureAuthority {
    fn current<'a>(&'a self, _: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(async { Ok(*self.0.lock().unwrap()) })
    }
}
struct Directory {
    group: GroupIdentity,
    enrolled: BTreeMap<Uuid, WorkerEnrollment>,
}
impl PeerDirectory for Directory {
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if group != &self.group
            || !matches!(purpose, Purpose::QueueControl | Purpose::QueueData)
            || self.enrolled.get(&peer.worker_id).map(WorkerIdentity::from) != Some(peer)
        {
            return Err(error("fixture enrollment differs"));
        }
        Ok(TransportTrust::AppliedEnrollment)
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.enrolled
            .get(&worker)
            .cloned()
            .ok_or_else(|| error("fixture worker is not enrolled"))
    }
}
struct Gate {
    stage: Mutex<Option<Stage>>,
    entered: AtomicBool,
    release: Notify,
}
impl Gate {
    fn set(&self, stage: Option<Stage>) {
        *self.stage.lock().unwrap() = stage;
        self.entered.store(false, Ordering::Release);
        self.release.notify_waiters();
    }
    async fn wait(&self, stage: Option<Stage>) {
        if stage.is_none() {
            return;
        }
        loop {
            let released = self.release.notified();
            if *self.stage.lock().unwrap() != stage {
                return;
            }
            self.entered.store(true, Ordering::Release);
            released.await;
        }
    }
}
struct GatedEndpoint {
    endpoint: Arc<QueueEndpoint>,
    gate: Arc<Gate>,
}
impl Endpoint for GatedEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        self.endpoint.preflight(session, header)
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: nabedra_database_server::oltp::pool::ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request: QueueRequest = serde_json::from_slice(header.as_ref())
                .map_err(|_| error("invalid fixture header"))?;
            let stage = match request {
                QueueRequest::Install { record } if record.state.terminal() => {
                    Some(Stage::Terminal)
                }
                QueueRequest::Install { record }
                    if record.accepted && record.state == RequestState::Queued =>
                {
                    Some(Stage::Acceptance)
                }
                QueueRequest::Install { record } if !record.accepted => Some(Stage::Payload),
                _ => None,
            };
            let result = self.endpoint.handle(session, header, sections).await?;
            self.gate.wait(stage).await;
            Ok(result)
        })
    }
}
struct Worker {
    shared: Arc<WorkerSharedResources>,
    config: Config,
    authority: Arc<FixtureAuthority>,
    replica: Arc<Mutex<WorkerReplica>>,
    coordinator: Arc<QueueCoordinator>,
    peers: QueuePeerClient,
    gate: Arc<Gate>,
}
impl Worker {
    fn current(&self) -> QueueAuthority {
        *self.authority.0.lock().unwrap()
    }
    async fn admin(&self, command: Admin) -> Result<Value, ProtocolError> {
        match command {
            Admin::Gate { stage } => {
                self.gate.set(stage);
                Ok(json!({"ok":true}))
            }
            Admin::GateState => Ok(json!({"entered":self.gate.entered.load(Ordering::Acquire),
                "resources":self.shared.scheduler().usage(),"queue_ready":self.replica.lock().unwrap().is_ready_member()})),
            Admin::Inspect { request_id, run_id } => {
                if run_id.is_some_and(|run| run != self.current().ready.view.run_id) {
                    return Err(QueueError::Abandoned.protocol());
                }
                let replica = self.replica.lock().unwrap();
                Ok(match replica.record(request_id) {
                    None => json!({"record":null}),
                    Some(record) => {
                        json!({"run_id":record.run_id,"request_id":request_id,"accepted":record.accepted,
                        "state":record.state,"version":record.version,"sequence":record.sequence,"body":String::from_utf8_lossy(&record.body),
                        "context":record.pinned_catalog.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
                        "response":record.response_body.as_ref().map(|v| String::from_utf8_lossy(v).into_owned()),
                        "copies_complete":replica.copies_complete(request_id)})
                    }
                })
            }
            Admin::Admit { body } => {
                let parsed = nabedra_database_server::protocol::parse_request(body.as_bytes())?;
                if !matches!(
                    parsed.operation,
                    nabedra_database_server::protocol::Operation::List(_)
                        | nabedra_database_server::protocol::Operation::Count(_)
                        | nabedra_database_server::protocol::Operation::Collect(_)
                        | nabedra_database_server::protocol::Operation::Explain(_)
                ) {
                    return Err(error("fixture admission accepts read requests only"));
                }
                let current = self.current();
                let context = self.admission_context(parsed.request_id, body.as_bytes(), true);
                let accepted = self
                    .coordinator
                    .admit(body.into_bytes(), context, now())
                    .await?;
                Ok(
                    json!({"status":202,"request_id":accepted.request_id,"run_id":current.ready.view.run_id,"version":accepted.version}),
                )
            }
            Admin::AdmitWide {
                request_id,
                body_bytes,
            } => {
                if !(4096..=nabedra_database_server::protocol::MAX_BODY_BYTES).contains(&body_bytes)
                {
                    return Err(error("invalid fixture wide body length"));
                }
                let prefix = format!(
                    r#"{{"request_id":"{request_id}","operation":"table.insert","payload":{{"table_id":"{}","row":{{"id":"wide","text":""#,
                    Uuid::from_u128(99)
                );
                let suffix = "\"}}}";
                let text = "x".repeat(body_bytes - prefix.len() - suffix.len());
                let body = format!("{prefix}{text}{suffix}");
                assert_eq!(body.len(), body_bytes);
                let context = self.admission_context(request_id, body.as_bytes(), false);
                let digest = Digest::from_bytes(Sha256::digest(body.as_bytes()).into());
                let accepted = self
                    .coordinator
                    .admit(body.into_bytes(), context, now())
                    .await?;
                Ok(
                    json!({"status":202,"request_id":accepted.request_id,"body_bytes":body_bytes,"digest":digest}),
                )
            }
            Admin::Fingerprint { request_id } => {
                let replica = self.replica.lock().unwrap();
                let record = replica
                    .record(request_id)
                    .ok_or_else(|| error("missing fixture record"))?;
                let actual = Digest::from_bytes(Sha256::digest(&record.body).into());
                Ok(
                    json!({"body_bytes":record.body.len(),"digest":record.digest,"actual_digest":actual,
                    "accepted":record.accepted,"state":record.state}),
                )
            }
            Admin::Resume { request_id } => Ok(
                json!({"record":self.coordinator.ensure_copies(self.current(),request_id).await?}),
            ),
            Admin::Execute { request_id } => {
                {
                    let replica = self.replica.lock().unwrap();
                    let record = replica
                        .record(request_id)
                        .ok_or_else(|| error("missing fixture record"))?;
                    let request = nabedra_database_server::protocol::parse_request(&record.body)?;
                    if !matches!(
                        request.operation,
                        nabedra_database_server::protocol::Operation::List(_)
                    ) {
                        return Err(error(
                            "deterministic fixture execution accepts table.list only",
                        ));
                    }
                }
                self.coordinator.start(request_id).await?;
                let context: AcceptedContext = {
                    let replica = self.replica.lock().unwrap();
                    let record = replica
                        .record(request_id)
                        .ok_or_else(|| error("missing fixture record"))?;
                    serde_json::from_slice(record.pinned_catalog.as_ref().unwrap()).unwrap()
                };
                let pin = context
                    .read
                    .ok_or_else(|| error("missing fixture read pin"))?;
                if pin.storage_pin.read_ts.get() != 1
                    || pin.catalog_root != Digest::from_bytes([8; 32])
                {
                    return Err(error("fixture pin lineage changed"));
                }
                let response = json!({"request_id":request_id,"data":{"snapshot":pin.storage_pin.read_ts.get(),"value":"fixture-read"}});
                let version = self
                    .coordinator
                    .finish(
                        request_id,
                        200,
                        serde_json::to_vec(&response).unwrap(),
                        now(),
                    )
                    .await?;
                Ok(json!({"response":response,"record":version}))
            }
            Admin::Fence {
                ready,
                owner,
                members,
            } => {
                let previous = self.current();
                let next = QueueAuthority { ready, owner };
                self.replica
                    .lock()
                    .unwrap()
                    .fenced_takeover(previous, next, members)
                    .map_err(QueueError::protocol)?;
                *self.authority.0.lock().unwrap() = next;
                Ok(json!({"ok":true}))
            }
            Admin::BeginJoin { ticket } => Ok(
                json!({"snapshot":self.replica.lock().unwrap().begin_join(self.current(),ticket).map_err(QueueError::protocol)?}),
            ),
            Admin::Seal { snapshot } => Ok(
                json!({"cut":self.replica.lock().unwrap().seal_join(self.current(),snapshot).map_err(QueueError::protocol)?}),
            ),
            Admin::Page {
                snapshot,
                phase,
                after,
                target,
            } => {
                let page = self
                    .replica
                    .lock()
                    .unwrap()
                    .join_page(self.current(), snapshot, phase, after)
                    .map_err(QueueError::protocol)?;
                self.peers.install_join_page(target, &page).await?;
                Ok(
                    json!({"done":page.done,"next":page.next_sequence,"has_record":page.record.is_some()}),
                )
            }
            Admin::PreviewPage {
                snapshot,
                phase,
                after,
            } => {
                let page = self
                    .replica
                    .lock()
                    .unwrap()
                    .join_page(self.current(), snapshot, phase, after)
                    .map_err(QueueError::protocol)?;
                Ok(
                    json!({"done":page.done,"next":page.next_sequence,"has_record":page.record.is_some()}),
                )
            }
            Admin::JoinStatement => Ok(
                json!({"statement":self.replica.lock().unwrap().join_statement(self.current()).map_err(QueueError::protocol)?}),
            ),
            Admin::PeerState { target, request_id } => Ok(
                json!({"state":self.peers.state(target,self.scope(),self.current().ready.view,request_id).await?}),
            ),
            Admin::PeerResult {
                target,
                request_id,
                version,
            } => {
                let reply = self
                    .peers
                    .result(
                        target,
                        self.scope(),
                        self.current().ready.view,
                        request_id,
                        version,
                    )
                    .await?;
                Ok(
                    json!({"body":String::from_utf8_lossy(reply.sections[0].as_ref().unwrap().as_ref())}),
                )
            }
        }
    }
    fn scope(&self) -> QueueScope {
        QueueScope {
            cluster_id: self.config.group.cluster_id,
            database: self.config.group.database.clone(),
        }
    }
    fn admission_context(&self, request_id: Uuid, body: &[u8], read: bool) -> AcceptedContext {
        AcceptedContext {
            authorization: OriginalAuthorization {
                database: self.config.group.database.clone(),
                principal: Digest::from_bytes([7; 32]),
                policy_revision: 1,
                ingress_id: self.config.local.worker_id,
                ingress_incarnation: self.config.local.incarnation,
                credential_expires_at_ms: None,
            },
            read: read.then(|| QueueReadToken {
                identity: PinIdentity {
                    run_id: self.current().ready.view.run_id,
                    request_id,
                    digest: Digest::from_bytes(Sha256::digest(body).into()),
                    nonce: Uuid::new_v4(),
                },
                storage_pin: StoragePin {
                    pin_id: Uuid::new_v4(),
                    read_ts: Timestamp::new(1).unwrap(),
                    allocator_epoch: 1,
                },
                catalog_root: Digest::from_bytes([8; 32]),
                schema_epoch: 1,
                routing_epoch: 1,
            }),
        }
    }
}

pub async fn run(config: Config) {
    if config.proxy_upstream.is_some() {
        run_proxy(config).await;
        return;
    }
    assert!(
        !config.fixture_cold_authorization.is_nil(),
        "fixture bootstrap requires explicit authorization"
    );
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut resources = WorkerResources::default();
    if let Some(memory) = config.memory_limit_bytes {
        resources.memory_limit_bytes = memory;
    }
    let shared = WorkerSharedResources::new(1, resources, SharedResourceConfig::default()).unwrap();
    let current = QueueAuthority {
        ready: config.ready,
        owner: config.owner,
    };
    let authority = Arc::new(FixtureAuthority(Mutex::new(current)));
    let scope = QueueScope {
        cluster_id: config.group.cluster_id,
        database: config.group.database.clone(),
    };
    let limits = QueueLimits {
        max_pending_requests: 32,
        max_queue_bytes_per_replica: 32 << 20,
        max_retained_result_bytes_per_worker: 2 << 20,
        result_retention_ms: 300_000,
    };
    let local = WorkerIdentity::from(&config.local);
    let replica = Arc::new(Mutex::new(
        match config.ticket {
            Some(ticket) => WorkerReplica::new_joining(
                shared.clone(),
                scope.clone(),
                local,
                current,
                config.members.clone(),
                limits,
                ticket,
            ),
            None => WorkerReplica::new(
                shared.clone(),
                scope.clone(),
                local,
                current,
                config.members.clone(),
                limits,
            ),
        }
        .unwrap(),
    ));
    let queue = QueueEndpoint::new(
        replica.clone(),
        shared.clone(),
        scope,
        authority.clone(),
        WorkerResources::default().task_memory_limit_bytes,
    )
    .unwrap();
    let gate = Arc::new(Gate {
        stage: Mutex::new(None),
        entered: AtomicBool::new(false),
        release: Notify::new(),
    });
    let endpoint = Arc::new(GatedEndpoint {
        endpoint: queue,
        gate: gate.clone(),
    });
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    let resident = registry.reserve(config.group.clone()).unwrap();
    resident
        .bind(Purpose::QueueControl, endpoint.clone())
        .unwrap();
    resident.bind(Purpose::QueueData, endpoint).unwrap();
    let tls = TlsMaterial::from_pem(
        &fs::read(
            config
                .directory
                .join(format!("node{}.pem", config.tls_index)),
        )
        .unwrap(),
        &fs::read(
            config
                .directory
                .join(format!("node{}.key", config.tls_index)),
        )
        .unwrap(),
        &fs::read(config.directory.join("ca.pem")).unwrap(),
    )
    .unwrap();
    let directory = Arc::new(Directory {
        group: config.group.clone(),
        enrolled: config
            .enrollments
            .iter()
            .cloned()
            .map(|w| (w.worker_id, w))
            .collect(),
    });
    let transport = Arc::new(
        WorkerTransport::new(
            &config.local,
            &tls,
            shared.clone(),
            directory,
            registry,
            TransportConfig::default(),
        )
        .unwrap(),
    );
    let peers =
        QueuePeerClient::new(transport.client(), shared.clone(), config.group.clone()).unwrap();
    let coordinator =
        QueueCoordinator::new(replica.clone(), authority.clone(), Arc::new(peers.clone()));
    let listener = inherited_listener(TLS_LISTENER_ENV);
    tokio::spawn(transport.run(listener, std::future::pending()));
    let admin = inherited_listener(ADMIN_LISTENER_ENV);
    let worker = Arc::new(Worker {
        shared,
        config,
        authority,
        replica,
        coordinator,
        peers,
        gate,
    });
    loop {
        let (socket, _) = admin.accept().await.unwrap();
        let worker = worker.clone();
        tokio::spawn(async move {
            serve_admin(worker, socket).await;
        });
    }
}
async fn run_proxy(config: Config) {
    // This API fixture owns no queue. Killing it cannot cancel worker admission.
    drop(inherited_listener(TLS_LISTENER_ENV));
    let listener = inherited_listener(ADMIN_LISTENER_ENV);
    let config = Arc::new(config);
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let config = config.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(std::time::Duration::from_secs(20), async {
                let length = socket.read_u32().await.ok()? as usize;
                if length > 128 << 10 {
                    return None;
                }
                let mut bytes = vec![0; length];
                socket.read_exact(&mut bytes).await.ok()?;
                let envelope: AdminEnvelope = serde_json::from_slice(&bytes).ok()?;
                if envelope.token != config.admin_token {
                    return None;
                }
                let (upstream, token) = config.proxy_upstream.as_ref()?;
                let value = command(upstream, *token, envelope.command).await.ok()?;
                let bytes = serde_json::to_vec(&value).ok()?;
                socket.write_u32(bytes.len() as u32).await.ok()?;
                socket.write_all(&bytes).await.ok()?;
                Some(())
            })
            .await;
            if !matches!(result, Ok(Some(()))) {
                eprintln!("fixture API observer closed or timed out");
            }
        });
    }
}
fn inherited_listener(name: &str) -> TcpListener {
    use std::os::fd::FromRawFd;
    let fd: std::os::fd::RawFd = std::env::var(name).unwrap().parse().unwrap();
    // The fixture parent transfers this descriptor exactly once at spawn.
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    listener.set_nonblocking(true).unwrap();
    TcpListener::from_std(listener).unwrap()
}
async fn serve_admin(worker: Arc<Worker>, mut socket: TcpStream) {
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let length = socket
            .read_u32()
            .await
            .map_err(|_| error("fixture read failed"))? as usize;
        if length > 128 << 10 {
            return Err(error("fixture command too large"));
        }
        let mut body = vec![0; length];
        socket
            .read_exact(&mut body)
            .await
            .map_err(|_| error("fixture read failed"))?;
        let envelope: AdminEnvelope =
            serde_json::from_slice(&body).map_err(|_| error("fixture command malformed"))?;
        if envelope.token != worker.config.admin_token {
            return Err(error("fixture admin unauthorized"));
        }
        let reply = match worker.admin(envelope.command).await {
            Ok(value) => value,
            Err(error) => json!({"error":error}),
        };
        let bytes = serde_json::to_vec(&reply).unwrap();
        socket
            .write_u32(bytes.len() as u32)
            .await
            .map_err(|_| error("fixture write failed"))?;
        socket
            .write_all(&bytes)
            .await
            .map_err(|_| error("fixture write failed"))?;
        Ok::<_, ProtocolError>(())
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        eprintln!("fixture admin observer closed or timed out");
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn error(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::Unknown)
}
