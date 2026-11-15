//! Configured API process, retained controller ownership and worker enrollment.
use super::{
    attachment::{AttachedConfig, check_attach},
    controller::{
        Owner, OwnershipChange, OwnershipEvidence, OwnershipGuard, OwnershipObservation,
        OwnershipProposal, OwnershipSample,
    },
    heartbeat::{HeartbeatBinding, Observations, load_heartbeat},
    member::{Node, NodeState, Registration},
    membership::{ReadinessInput, RegistrationPlan, ready_candidate, registration_plan},
    startup_material::composition::prepare_api,
    state::{Config, Control, Controller, LeaseState, Policy, PolicyMode, Provisioning, Scheduler},
    store::{LoadedControl, OwnershipOutcome, PreparedMembership},
};
use crate::{
    api::response::{EncodedResponse, encode_error},
    executor::Executor,
    node::{
        auth,
        http::{self, HttpClient, Signer, cpu, failure, invalid, unavailable},
    },
    protocol::{self, ErrorCode},
    storage::{HttpStore, Namespace, Storage, StorageError},
    worker_pool::WorkerPool,
};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UnixStream},
    sync::watch,
    time::{Instant, timeout_at},
};
use uuid::Uuid;
type Result<T> = http::Result<T>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialPolicy {
    mode: String,
    desired: u64,
    min_nodes: u64,
    max_nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Init {
    version: u64,
    #[serde(rename = "type")]
    kind: String,
    name: String,
    initial_policy: InitialPolicy,
    provisioning: String,
    bridge: Option<serde_json::Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    version: u64,
    #[serde(rename = "type")]
    kind: String,
}
fn sample() -> std::result::Result<OwnershipSample, StorageError> {
    OwnershipSample::from_system_time(Instant::now(), SystemTime::now())
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

struct Api {
    attached: AttachedConfig,
    storage: Arc<Storage<HttpStore>>,
    pool: WorkerPool,
    client: HttpClient,
    signer: Arc<Signer>,
    process: Uuid,
    guard: Mutex<OwnershipGuard>,
    changes: tokio::sync::Mutex<()>,
    observations: Arc<Mutex<Observations>>,
    data: Executor,
    controls: Executor,
    accepting: AtomicBool,
    next: AtomicUsize,
}
impl Api {
    async fn load(&self) -> Result<LoadedControl> {
        let storage = self.storage.clone();
        cpu(&self.pool, move |runtime| {
            runtime
                .block_on(storage.load_control(deadline()))
                .map_err(|_| unavailable())
        })
        .await
    }
    async fn ownership(&self) -> Result<Owner> {
        let _serial = self.changes.lock().await;
        let loaded = self.load().await?;
        let root = loaded.root().clone();
        let observation = cpu(&self.pool, {
            let root = root.clone();
            move |_| OwnershipObservation::new(&root).map_err(|_| unavailable())
        })
        .await?;
        let now = sample().map_err(|_| unavailable())?;
        let existing = {
            let mut guard = self.guard.lock().unwrap();
            guard.observe(&observation).map_err(|_| unavailable())?;
            guard.check(Ok(now)).ok()
        };
        if let Some(owner) = existing
            && root.controller.lease_until_ms.is_some_and(|until| {
                until.saturating_sub(now.unix_ms()) > root.timing.lease_ms - root.timing.renew_ms
            })
        {
            return Ok(owner);
        }
        let change = if root.controller.owner_id == Some(self.process)
            && root.controller.lease_state == LeaseState::Held
        {
            OwnershipChange::Renew(Owner {
                process_id: self.process,
                epoch: root.controller.epoch,
            })
        } else {
            OwnershipChange::Acquire {
                process_id: self.process,
            }
        };
        let proposal = cpu(&self.pool, move |_| {
            OwnershipProposal::new(&root, change, Uuid::new_v4(), Ok(now))
                .map_err(|_| unavailable())
        })
        .await?;
        let stamp = self
            .guard
            .lock()
            .unwrap()
            .begin(&proposal)
            .map_err(|_| unavailable())?;
        let storage = self.storage.clone();
        let result = cpu(&self.pool, move |runtime| {
            Ok(proposal
                .prepare(loaded)
                .and_then(|prepared| runtime.block_on(prepared.submit(&storage, deadline())))
                .and_then(OwnershipEvidence::new))
        })
        .await;
        let result = result.unwrap_or(Err(StorageError::Unavailable));
        let mut guard = self.guard.lock().unwrap();
        if !guard
            .finish(stamp, result, sample())
            .map_err(|_| unavailable())?
        {
            return Err(unavailable());
        }
        guard.check(sample()).map_err(|_| unavailable())
    }
    async fn publish(
        &self,
        loaded: LoadedControl,
        candidate: super::membership::MembershipCandidate,
    ) -> Result<LoadedControl> {
        let observed = {
            let root = loaded.root().clone();
            cpu(&self.pool, move |_| {
                OwnershipObservation::new(&root).map_err(|_| unavailable())
            })
            .await?
        };
        let prepared = cpu(&self.pool, move |_| {
            PreparedMembership::prepare(loaded, candidate).map_err(|_| unavailable())
        })
        .await?;
        {
            let mut guard = self.guard.lock().unwrap();
            guard.observe(&observed).map_err(|_| unavailable())?;
            let generation = guard.authorization_generation();
            guard
                .authorize(generation, &observed, sample())
                .map_err(|_| unavailable())?;
        }
        let storage = self.storage.clone();
        let outcome = cpu(&self.pool, move |runtime| {
            runtime
                .block_on(prepared.submit(&storage, deadline()))
                .map_err(|_| unavailable())
        })
        .await?;
        match outcome {
            OwnershipOutcome::Acknowledged(root) | OwnershipOutcome::ObservedExact(root) => {
                Ok(root)
            }
            _ => Err(unavailable()),
        }
    }
    async fn enroll(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        let proof = cpu(&self.pool, move |_| {
            auth::verify_enrollment(&bytes).map_err(|_| failure(ErrorCode::Unauthorized))
        })
        .await?;
        self.ownership().await?;
        let _serial = self.changes.lock().await;
        let loaded = self.load().await?;
        let owner = self
            .guard
            .lock()
            .unwrap()
            .check(sample())
            .map_err(|_| unavailable())?;
        let cluster = proof.claims().cluster_id();
        let node_id = proof.claims().node_id();
        let incarnation = proof.claims().incarnation();
        let session = proof.claims().session_id();
        let root = loaded.root().clone();
        let plan = cpu(&self.pool, move |_| {
            registration_plan(&root, owner, &proof, Uuid::new_v4())
                .map_err(|_| failure(ErrorCode::Conflict))
        })
        .await?;
        let node = match plan {
            RegistrationPlan::Existing(node) => node,
            RegistrationPlan::Change(candidate) => self
                .publish(loaded, candidate)
                .await?
                .root()
                .nodes
                .iter()
                .find(|n| n.node_id == node_id)
                .cloned()
                .ok_or_else(unavailable)?,
        };
        let bytes = serde_json::to_vec(&json!({"version":1,"cluster_id":cluster,"node_id":node_id,"incarnation":incarnation,"session_id":session,
            "registration":if node.registration == Registration::Verified {"verified"} else {"registered"},"api_public_key":http::hex(&self.signer.public_key())})).map_err(|_| invalid())?;
        auth::EnrollmentReply::decode(&bytes).map_err(|_| invalid())?;
        Ok(bytes)
    }
    async fn probe(&self, root: Control, node: Node) -> Result<()> {
        let challenge = Uuid::new_v4();
        let initial = serde_json::to_vec(&json!({"version":1,"challenge":challenge}))
            .map_err(|_| invalid())?;
        let signer = self.signer.clone();
        let endpoint = node.endpoint.clone().ok_or_else(unavailable)?;
        let context = cpu(&self.pool, {
            let node = node.clone();
            move |_| {
                signer.context(
                    &root,
                    &node,
                    auth::Route::Readiness,
                    auth::RequestIds::default(),
                    initial,
                )
            }
        })
        .await?;
        let reply = http::small_exchange(
            &self.client,
            &self.signer,
            context,
            &endpoint,
            &self.storage,
            &self.pool,
        )
        .await?;
        let checked = reply.readiness().ok_or_else(unavailable)?;
        if checked.challenge() != challenge || !checked.initialized() {
            return Err(unavailable());
        }
        let _serial = self.changes.lock().await;
        let loaded = self.load().await?;
        let owner = self
            .guard
            .lock()
            .unwrap()
            .check(sample())
            .map_err(|_| unavailable())?;
        let root = loaded.root().clone();
        let observation = reply.observation().clone();
        let candidate = cpu(&self.pool, move |_| {
            let input = ReadinessInput {
                cluster_id: root.cluster_id,
                member: node,
                observation,
                challenge,
                verified_challenge: challenge,
                initialized: true,
            };
            ready_candidate(&root, owner, &input, Uuid::new_v4()).map_err(|_| unavailable())
        })
        .await?;
        self.publish(loaded, candidate).await?;
        Ok(())
    }
    async fn route(&self, bytes: Vec<u8>) -> Result<EncodedResponse> {
        let original = bytes.clone();
        let (id, mutation) = cpu(&self.pool, move |_| {
            let parsed = protocol::parse_request(&original)?;
            Ok((parsed.request_id, parsed.operation.is_mutation()))
        })
        .await?;
        match self.route_inner(bytes).await {
            Err(_) if mutation => Ok(encode_error(Some(id), unknown())),
            result => result,
        }
    }
    async fn route_inner(&self, bytes: Vec<u8>) -> Result<EncodedResponse> {
        let loaded = self.load().await?;
        let root = loaded.root().clone();
        let storage = self.storage.clone();
        let observations = self.observations.clone();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let signer = self.signer.clone();
        let (context, endpoint) = cpu(&self.pool, move |runtime| {
            let parsed = protocol::parse_request(&bytes)?;
            observations
                .lock()
                .unwrap()
                .reconcile_members(&root, Instant::now())
                .map_err(|_| unavailable())?;
            for step in 0..root.nodes.len() {
                let node = &root.nodes[(start.wrapping_add(step)) % root.nodes.len()];
                if node.state != NodeState::Ready || node.registration != Registration::Verified {
                    continue;
                }
                let binding = heartbeat_binding(&root, node)?;
                let loaded = runtime.block_on(load_heartbeat(&storage, &binding, deadline()));
                let fresh = {
                    let mut observed = observations.lock().unwrap();
                    match loaded {
                        Ok(Some(h)) if h.heartbeat().accepting => {
                            observed
                                .observe(&binding, h.heartbeat(), Instant::now())
                                .map_err(|_| unavailable())?;
                            observed
                                .fresh(
                                    &binding,
                                    Instant::now(),
                                    Duration::from_millis(root.timing.expiry_ms),
                                )
                                .unwrap_or(false)
                        }
                        _ => {
                            let _ = observed.read_failed(&binding, Instant::now());
                            false
                        }
                    }
                };
                if !fresh {
                    continue;
                }
                let ids = auth::RequestIds {
                    operation: Some(parsed.request_id),
                    digest: Some(http::digest(&bytes)),
                    ..Default::default()
                };
                let context = signer.context(&root, node, auth::Route::Operations, ids, bytes)?;
                return Ok((context, node.endpoint.clone().ok_or_else(unavailable)?));
            }
            Err(unavailable())
        })
        .await?;
        let id = context.auth().operation_id();
        match http::small_exchange(
            &self.client,
            &self.signer,
            context,
            &endpoint,
            &self.storage,
            &self.pool,
        )
        .await
        {
            Ok(reply) => Ok(EncodedResponse {
                status: reply.status(),
                body: reply.bytes().to_vec(),
            }),
            Err(_) => Ok(encode_error(
                id,
                protocol::ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "worker outcome unavailable",
                    protocol::Outcome::Unknown,
                ),
            )),
        }
    }
}
pub(crate) fn heartbeat_binding(root: &Control, node: &Node) -> Result<HeartbeatBinding> {
    let session = node.session.as_ref().ok_or_else(unavailable)?;
    Ok(HeartbeatBinding {
        cluster_id: root.cluster_id,
        node_id: node.node_id,
        incarnation: node.incarnation.ok_or_else(unavailable)?,
        session_id: session.id,
        verifier: session.verifier.clone(),
        operation_slots: node.operation_slots,
        partition_slots: node.partition_slots,
        queue_size: node.queue_size,
    })
}
fn response(value: EncodedResponse) -> Response {
    Response::builder()
        .status(value.status)
        .header("content-type", "application/json")
        .header("content-length", value.body.len())
        .header("connection", "close")
        .body(Body::from(value.body))
        .unwrap()
}
async fn enroll(State(api): State<Arc<Api>>, request: Request) -> Response {
    if !api.accepting.load(Ordering::Acquire) {
        return response(encode_error(None, unavailable()));
    }
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.enrollment_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let admitted = match api.controls.try_admit() {
        Ok(v) => v,
        Err(e) => return response(encode_error(None, e)),
    };
    let work = admitted.spawn(move || async move {
        let result = async {
            let length = http::framing(request.headers(), 16 * 1024, false)?.ok_or_else(invalid)?;
            let bytes = http::read_small(
                request.into_body(),
                length,
                Instant::now() + Duration::from_secs(30),
            )
            .await?;
            api.enroll(bytes).await
        }
        .await;
        Ok(response(match result {
            Ok(body) => EncodedResponse { status: 200, body },
            Err(e) => encode_error(None, e),
        }))
    });
    match work {
        Ok(work) => work
            .receive()
            .await
            .unwrap_or_else(|e| response(encode_error(None, e))),
        Err(e) => response(encode_error(None, e)),
    }
}
fn unknown() -> protocol::ProtocolError {
    protocol::ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "logical request outcome unavailable",
        protocol::Outcome::Unknown,
    )
}
async fn operations(State(api): State<Arc<Api>>, request: Request) -> Response {
    if !api.accepting.load(Ordering::Acquire) {
        return response(encode_error(None, unknown()));
    }
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.data_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let admitted = match api.data.try_admit() {
        Ok(v) => v,
        Err(_) => return response(encode_error(None, unknown())),
    };
    let work = admitted.spawn(move || async move {
        let result = async {
            let length = http::framing(request.headers(), protocol::MAX_BODY_BYTES, false)?
                .ok_or_else(invalid)?;
            let bytes = http::read_small(
                request.into_body(),
                length,
                Instant::now() + Duration::from_secs(30),
            )
            .await?;
            api.route(bytes).await
        }
        .await;
        Ok(response(result.unwrap_or_else(|e| encode_error(None, e))))
    });
    match work {
        Ok(work) => work
            .receive()
            .await
            .unwrap_or_else(|e| response(encode_error(None, e))),
        Err(e) => response(encode_error(None, e)),
    }
}

async fn frame(stream: &mut UnixStream, end: Option<Instant>) -> Result<Vec<u8>> {
    let mut first = [0; 1];
    match end {
        Some(end) => timeout_at(end, stream.read_exact(&mut first))
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?,
        None => stream
            .read_exact(&mut first)
            .await
            .map_err(|_| unavailable())?,
    };
    let end = end.unwrap_or_else(|| Instant::now() + Duration::from_secs(2));
    let mut prefix = [0; 4];
    prefix[0] = first[0];
    timeout_at(end, stream.read_exact(&mut prefix[1..]))
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > 16384 {
        return Err(invalid());
    }
    let mut body = vec![0; length];
    timeout_at(end, stream.read_exact(&mut body))
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
    if !body
        .iter()
        .copied()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| b == b'{')
    {
        return Err(invalid());
    }
    Ok(body)
}
async fn send(stream: &mut UnixStream, value: serde_json::Value, end: Instant) -> Result<()> {
    let bytes = serde_json::to_vec(&value).map_err(|_| invalid())?;
    timeout_at(end, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| unavailable())?
    .map_err(|_| unavailable())
}
fn supervisor() -> Result<UnixStream> {
    use std::os::fd::FromRawFd;
    let fd: i32 = std::env::var("MESHDB_SUPERVISOR_FD")
        .map_err(|_| invalid())?
        .parse()
        .map_err(|_| invalid())?;
    if fd < 3 {
        return Err(invalid());
    }
    // This explicit inherited descriptor is transferred by the owning supervisor.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(invalid());
    }
    stream.set_nonblocking(true).map_err(|_| invalid())?;
    UnixStream::from_std(stream).map_err(|_| invalid())
}
pub async fn run_api(path: PathBuf) -> std::result::Result<(), &'static str> {
    let mut channel = supervisor().map_err(|_| "protocol_error")?;
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| "startup_failed")?;
    let result = api_process(path, &mut channel, pool).await;
    if result.is_err() {
        let _ = send(
            &mut channel,
            json!({"version":1,"type":"fatal","code":"startup_failed"}),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    }
    let joined = threads.shutdown().await.map_err(|_| "startup_failed");
    result.map_err(|_| "startup_failed").and(joined)
}
async fn api_process(path: PathBuf, channel: &mut UnixStream, pool: WorkerPool) -> Result<()> {
    let startup_end = Instant::now() + Duration::from_secs(30);
    let init: Init =
        serde_json::from_slice(&frame(channel, Some(startup_end)).await?).map_err(|_| invalid())?;
    if init.version != 1
        || init.kind != "init"
        || init.provisioning != "external"
        || init.bridge.is_some()
        || init.initial_policy.mode != "fixed"
        || init.initial_policy.desired != init.initial_policy.min_nodes
        || init.initial_policy.desired != init.initial_policy.max_nodes
        || init.initial_policy.desired > 1024
    {
        return Err(invalid());
    }
    protocol::validate_identifier(&init.name)?;
    let local = cpu(&pool, move |_| {
        prepare_api(&path, startup_end.into_std())
            .and_then(|p| p.connect())
            .map_err(|_| {
                eprintln!("meshdb-server: startup material invalid");
                invalid()
            })
    })
    .await?;
    let template = &local.prepared.input.value.template;
    let namespace = match &template.storage {
        super::configuration::Storage::Http(h) => {
            Namespace::new(&h.namespace).map_err(|_| invalid())?
        }
        _ => return Err(invalid()),
    };
    let client = HttpClient::new(local.prepared.tls.as_ref())?;
    let process = Uuid::new_v4();
    let signer = Arc::new(Signer::api(
        local.prepared.input.value.cluster_id,
        process,
        &local.prepared.auth.admin_token,
    )?);
    let listener = TcpListener::bind(&local.prepared.input.value.bind)
        .await
        .map_err(|_| unavailable())?;
    let bound_url = format!(
        "{}://{}",
        if local.prepared.tls.is_some() {
            "https"
        } else {
            "http"
        },
        listener.local_addr().map_err(|_| unavailable())?
    );
    // The connected backend must stay the same object used by attachment identity.
    let storage = Arc::new(Storage::new(local.backend.client.clone(), namespace));
    let initial = Control {
        version: 1,
        cluster_id: local.prepared.input.value.cluster_id,
        name: init.name.clone(),
        config: Config {
            generation: 2,
            storage_fingerprint: local.backend.storage.as_str().into(),
            auth_fingerprint: local.authentication.as_str().into(),
            template_fingerprint: template.fingerprint().map_err(|_| invalid())?,
            lifecycle_fingerprint: local.lifecycle.as_str().into(),
            worker_semantic_fingerprint: local.backend.worker.as_str().into(),
            adapter_contract: None,
        },
        provisioning: Provisioning::External,
        template: template.clone(),
        timing: local.prepared.input.value.lifecycle.clone(),
        revision: 1,
        change_id: Uuid::new_v4(),
        policy: Policy {
            revision: 1,
            mode: PolicyMode::Fixed,
            desired: init.initial_policy.desired,
            min_nodes: init.initial_policy.min_nodes,
            max_nodes: init.initial_policy.max_nodes,
        },
        controller: Controller {
            owner_id: None,
            epoch: 0,
            lease_state: LeaseState::Released,
            lease_until_ms: None,
        },
        scheduler: Scheduler {
            cooldown_until_ms: 0,
            backoff_until_ms: 0,
            failures: 0,
            low_since_ms: None,
        },
        admin_seq: 0,
        admin_head: None,
        nodes: vec![],
        intents: vec![],
    };
    let attached = cpu(&pool, {
        let storage = storage.clone();
        move |runtime| {
            runtime
                .block_on(storage.initialize_cluster_catalog())
                .map_err(|e| {
                    eprintln!("meshdb-server: catalog initialization {e}");
                    unavailable()
                })?;
            let loaded = runtime
                .block_on(storage.initialize_control(&initial, startup_end))
                .map_err(|e| {
                    eprintln!("meshdb-server: control initialization {e}");
                    unavailable()
                })?;
            check_attach(loaded, local, &init.name, Provisioning::External).map_err(|e| {
                eprintln!("meshdb-server: attachment {e}");
                invalid()
            })
        }
    })
    .await?;
    let tls = crate::runtime::retained_tls(attached.local().prepared.tls.as_ref())?;
    let api = Arc::new(Api {
        attached,
        storage,
        pool,
        client,
        signer,
        process,
        guard: Mutex::new(OwnershipGuard::new(process)),
        changes: tokio::sync::Mutex::new(()),
        observations: Arc::new(Mutex::new(Observations::new(process, Instant::now()))),
        data: Executor::new(16, 0)?,
        controls: Executor::new(8, 0)?,
        accepting: AtomicBool::new(false),
        next: AtomicUsize::new(0),
    });
    send(channel, json!({"version":1,"type":"prepared","cluster_id":api.attached.root().cluster_id,"bound_url":bound_url,"authorize_url":null}), startup_end).await?;
    let activate: Message =
        serde_json::from_slice(&frame(channel, Some(startup_end)).await?).map_err(|_| invalid())?;
    if activate.version != 1 || activate.kind != "activate" {
        return Err(invalid());
    }
    // A replacement process must own the retained lease before advertising readiness.
    while api.ownership().await.is_err() {
        if Instant::now() >= startup_end {
            return Err(unavailable());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (stop, mut renew_stop) = watch::channel(false);
    let renewal_interval = Duration::from_millis(api.attached.root().timing.renew_ms);
    let owner = api.clone();
    let renewal = tokio::spawn(async move {
        loop {
            let _ = owner.ownership().await;
            tokio::select! { _ = renew_stop.changed() => break, _ = tokio::time::sleep(renewal_interval) => {} }
        }
    });
    let owner = api.clone();
    let mut probe_stop = stop.subscribe();
    let probes = tokio::spawn(async move {
        loop {
            if let Ok(loaded) = owner.load().await {
                let root = loaded.root();
                for node in root.nodes.iter().filter(|n| {
                    n.state == NodeState::Starting && n.registration == Registration::Registered
                }) {
                    if *probe_stop.borrow() {
                        break;
                    }
                    let _ = owner.probe(root.clone(), node.clone()).await;
                }
            }
            tokio::select! { _ = probe_stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(250)) => {} }
        }
    });
    api.accepting.store(true, Ordering::Release);
    let ready = send(channel, json!({"version":1,"type":"ready","cluster_id":api.attached.root().cluster_id,"bound_url":bound_url}), startup_end).await;
    let app = Router::new()
        .route("/v1/operations", post(operations))
        .route("/internal/v1/enroll", post(enroll))
        .route("/health/live", get(|| async { "true" }))
        .route("/health/ready", get(|| async { "true" }))
        .with_state(api.clone());
    let served = if ready.is_ok() {
        crate::runtime::serve_router(listener, app, tls, async {
            tokio::select! { _ = frame(channel, None) => {}, _ = crate::runtime::shutdown_signal() => {} }
            api.accepting.store(false,Ordering::Release); api.data.stop_accepting(); api.controls.stop_accepting();
        }).await.map_err(|_| unavailable())
    } else {
        ready
    };
    api.accepting.store(false, Ordering::Release);
    api.data.stop_accepting();
    api.controls.stop_accepting();
    stop.send_replace(true);
    let (renewed, probed) = tokio::join!(renewal, probes);
    api.data.drain().await;
    api.controls.drain().await;
    renewed.map_err(|_| unavailable())?;
    probed.map_err(|_| unavailable())?;
    served
}
