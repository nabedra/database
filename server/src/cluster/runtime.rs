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
    storage::{Namespace, RuntimeStore, Storage, StorageError},
    worker_pool::WorkerPool,
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State},
    response::Response,
    routing::{get, post},
};
use nabedra_rt::{ids, task, time};
use serde::Deserialize;
use serde_json::json;
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixStream},
    sync::watch,
    time::Instant,
};
use uuid::Uuid;
type Result<T> = http::Result<T>;
#[path = "provider_runtime.rs"]
mod provider_runtime;
#[path = "receipt_observation.rs"]
mod receipt_observation;

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
    bridge: Option<provider_runtime::Bridge>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    version: u64,
    #[serde(rename = "type")]
    kind: String,
}
fn sample() -> std::result::Result<OwnershipSample, StorageError> {
    OwnershipSample::from_system_time(time::now(), time::wall_now())
}
fn deadline() -> Instant {
    time::now() + Duration::from_secs(10)
}

struct Api {
    bridge: Option<provider_runtime::Bridge>,
    attached: AttachedConfig,
    storage: Arc<Storage<RuntimeStore>>,
    pool: WorkerPool,
    client: HttpClient,
    signer: Arc<Signer>,
    process: Uuid,
    guard: Mutex<OwnershipGuard>,
    changes: tokio::sync::Mutex<()>,
    observations: Arc<Mutex<Observations>>,
    status_observations: Mutex<Observations>,
    data: Executor,
    controls: Executor,
    submissions: Executor,
    stream_frontend: crate::api::stream::Frontend,
    accepting: AtomicBool,
    cold_start: AtomicBool,
    authority_epoch: AtomicU64,
    new_run: AtomicBool,
}
impl Api {
    async fn load(&self) -> Result<LoadedControl> {
        let storage = self.storage.clone();
        cpu(&self.pool, move |runtime| {
            runtime
                .block_on(storage.load_control(deadline()))
                .map_err(|error| match error {
                    StorageError::InvalidCatalog | StorageError::UnsupportedCatalog => invalid(),
                    _ => unavailable(),
                })
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
            self.ensure_catalog_fence(owner).await?;
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
            OwnershipProposal::new(&root, change, ids::uuid_v4(), Ok(now))
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
        let owner = {
            let mut guard = self.guard.lock().unwrap();
            if !guard
                .finish(stamp, result, sample())
                .map_err(|_| unavailable())?
            {
                return Err(unavailable());
            }
            guard.check(sample()).map_err(|_| unavailable())?
        };
        self.ensure_catalog_fence(owner).await?;
        Ok(owner)
    }
    async fn ensure_catalog_fence(&self, owner: Owner) -> Result<()> {
        use crate::storage::{CatalogFence, FenceTransition};
        let loaded = self
            .storage
            .load_cluster_catalog()
            .await
            .map_err(|_| unavailable())?;
        let old = loaded.catalog().fence;
        let cold = self.cold_start.load(Ordering::Acquire);
        let control = self.load().await?;
        if control.root().controller.owner_id != Some(owner.process_id)
            || control.root().controller.epoch != owner.epoch
        {
            return Err(unavailable());
        }
        let previous = control.root().scheduler.coordinator_node;
        let coordinator = previous
            .filter(|id| {
                control.root().nodes.iter().any(|n| {
                    n.node_id == *id
                        && n.state == NodeState::Ready
                        && n.registration == Registration::Verified
                })
            })
            .or_else(|| {
                control
                    .root()
                    .nodes
                    .iter()
                    .find(|n| {
                        n.state == NodeState::Ready && n.registration == Registration::Verified
                    })
                    .map(|n| n.node_id)
            });
        let newly_owned = self.authority_epoch.load(Ordering::Acquire) != owner.epoch;
        let advance = (newly_owned && old.is_some() && control.root().scheduler.scheduler_term > 0)
            || cold
            || (previous.is_some() && previous != coordinator);
        let changed =
            control.root().scheduler.scheduler_term == 0 || previous != coordinator || advance;
        let term = if changed {
            let prepared =
                super::store::PreparedScheduler::prepare(control, owner, coordinator, advance)
                    .map_err(|_| unavailable())?;
            match prepared
                .submit(&self.storage, deadline())
                .await
                .map_err(|_| unavailable())?
            {
                OwnershipOutcome::Acknowledged(root) | OwnershipOutcome::ObservedExact(root) => {
                    root.root().scheduler.scheduler_term
                }
                _ => return Err(unavailable()),
            }
        } else {
            control.root().scheduler.scheduler_term
        };
        if !cold && old.is_some_and(|f| f.scheduler_term == term) {
            self.authority_epoch.store(owner.epoch, Ordering::Release);
            return Ok(());
        }
        let transition = if old.is_none() {
            FenceTransition::Migrate
        } else if cold {
            FenceTransition::ColdStart
        } else {
            FenceTransition::Takeover
        };
        let new_run = cold || old.is_none();
        let run_id = if new_run {
            ids::uuid_v4()
        } else {
            old.unwrap().run_id
        };
        self.storage
            .fence_cluster_catalog(
                &loaded,
                CatalogFence {
                    run_id,
                    scheduler_term: term,
                },
                transition,
            )
            .await
            .map_err(|_| unavailable())?;
        if new_run {
            self.new_run.store(true, Ordering::Release);
        }
        self.cold_start.store(false, Ordering::Release);
        self.authority_epoch.store(owner.epoch, Ordering::Release);
        Ok(())
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
        if proof.claims().resources().is_none() {
            return Err(invalid());
        }
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
        let resource = proof.claims().resources().ok_or_else(invalid)?;
        resource
            .validate_execution(&root.template.execution)
            .map_err(|_| invalid())?;
        let mut advertised = root.template.clone();
        advertised.resources = Some(resource);
        if proof.claims().operation_slots()
            != crate::node::queue_runtime::coordination_capacity(&advertised)? as u64
            || proof.claims().partition_slots()
                != crate::node::queue_runtime::job_coordination_capacity(&advertised)? as u64
        {
            return Err(invalid());
        }
        crate::node::queue_runtime::validate_coordination_capacity(
            &root,
            Some((node_id, resource)),
            None,
        )?;
        let plan = cpu(&self.pool, move |_| {
            registration_plan(&root, owner, &proof, ids::uuid_v4())
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
        let challenge = ids::uuid_v4();
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
        let join = if let Ok(scheduler) = crate::node::queue_runtime::scheduler(&root) {
            let bytes = serde_json::to_vec(&crate::node::queue_runtime::Command::Join {
                node_id: node.node_id,
            })
            .map_err(|_| invalid())?;
            let result = crate::node::queue_transport::exchange(
                &self.client,
                &self.signer,
                &root,
                scheduler,
                bytes,
            )
            .await?;
            match serde_json::from_slice::<crate::node::queue_runtime::Reply>(&result)
                .map_err(|_| invalid())?
            {
                crate::node::queue_runtime::Reply::Joined { token } => {
                    Some((scheduler.clone(), token))
                }
                crate::node::queue_runtime::Reply::Error { error } => return Err(error),
                _ => return Err(invalid()),
            }
        } else {
            None
        };
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
            ready_candidate(&root, owner, &input, ids::uuid_v4()).map_err(|_| unavailable())
        })
        .await?;
        let publication = self.publish(loaded, candidate).await;
        if publication.is_ok() {
            self.ensure_catalog_fence(owner).await?;
        }
        if let Some((scheduler, token)) = join {
            let current = self.load().await?;
            let bytes = serde_json::to_vec(&crate::node::queue_runtime::Command::Unseal { token })
                .map_err(|_| invalid())?;
            let response = crate::node::queue_transport::exchange(
                &self.client,
                &self.signer,
                current.root(),
                &scheduler,
                bytes,
            )
            .await?;
            if let crate::node::queue_runtime::Reply::Error { error } =
                serde_json::from_slice(&response).map_err(|_| invalid())?
            {
                return Err(error);
            }
        }
        publication?;
        self.new_run.store(false, Ordering::Release);
        Ok(())
    }
    async fn observe_losses(&self) -> Result<()> {
        let _serial = self.changes.lock().await;
        let loaded = self.load().await?;
        let root = loaded.root().clone();
        let owner = self
            .guard
            .lock()
            .unwrap()
            .check(sample())
            .map_err(|_| unavailable())?;
        if root.controller.owner_id != Some(owner.process_id)
            || root.controller.epoch != owner.epoch
        {
            return Err(unavailable());
        }
        self.observations
            .lock()
            .unwrap()
            .reconcile_members(&root, time::now())
            .map_err(|_| unavailable())?;
        for node in root.nodes.iter().filter(|n| {
            matches!(n.state, NodeState::Ready | NodeState::Starting)
                && n.registration != Registration::Unregistered
        }) {
            let binding = heartbeat_binding(&root, node)?;
            let heartbeat = load_heartbeat(&self.storage, &binding, deadline()).await;
            let input = {
                let mut observed = self.observations.lock().unwrap();
                match heartbeat {
                    Ok(Some(h)) => {
                        observed
                            .observe(&binding, h.heartbeat(), time::now())
                            .map_err(|_| unavailable())?;
                    }
                    _ => {
                        observed
                            .read_failed(&binding, time::now())
                            .map_err(|_| unavailable())?;
                    }
                }
                observed
                    .expiry_input(
                        &binding,
                        time::now(),
                        Duration::from_millis(root.timing.expiry_ms),
                    )
                    .map_err(|_| unavailable())?
            };
            if let Some(input) = input {
                if !self
                    .observations
                    .lock()
                    .unwrap()
                    .matches_expiry(
                        &binding,
                        &input,
                        time::now(),
                        Duration::from_millis(root.timing.expiry_ms),
                    )
                    .map_err(|_| unavailable())?
                {
                    continue;
                }
                let candidate =
                    super::membership::loss_candidate(&root, owner, &input, ids::uuid_v4())
                        .map_err(|_| unavailable())?;
                self.publish(loaded, candidate).await?;
                self.ensure_catalog_fence(owner).await?;
                return Ok(());
            }
        }
        Ok(())
    }
    async fn queue_call(
        &self,
        command: crate::node::queue_runtime::Command,
    ) -> Result<crate::node::queue_runtime::Reply> {
        let loaded = self.load().await?;
        let root = loaded.root();
        let node = crate::node::queue_runtime::scheduler(root)?;
        let bytes = serde_json::to_vec(&command).map_err(|_| invalid())?;
        let submission = matches!(command, crate::node::queue_runtime::Command::Submit { .. });
        let bytes =
            crate::node::queue_transport::exchange(&self.client, &self.signer, root, node, bytes)
                .await
                .map_err(|error| if submission { unknown() } else { error })?;
        let reply = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        match reply {
            crate::node::queue_runtime::Reply::Error { error } => Err(error),
            other => Ok(other),
        }
    }
    fn request_context(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<crate::storage::context::RequestContext>> {
        let now = time::wall_now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| unavailable())?
            .as_secs();
        crate::request_queue::admission::decode_context(
            &self.attached.local().prepared.input.value.template,
            headers,
            now,
        )
    }
    async fn submit(
        &self,
        bytes: Vec<u8>,
        context: Option<crate::storage::context::RequestContext>,
    ) -> Result<crate::request_queue::RequestRecord> {
        protocol::parse_request(&bytes)?;
        match self
            .queue_call(crate::node::queue_runtime::Command::Submit {
                body: bytes,
                context,
            })
            .await?
        {
            crate::node::queue_runtime::Reply::Record { record } => Ok(record),
            _ => Err(invalid()),
        }
    }
    async fn lookup(
        &self,
        run_id: Uuid,
        request_id: Uuid,
        context: Option<crate::storage::context::RequestContext>,
    ) -> Result<crate::request_queue::Receipt> {
        match self
            .queue_call(crate::node::queue_runtime::Command::Lookup {
                run_id,
                request_id,
                context,
            })
            .await?
        {
            crate::node::queue_runtime::Reply::Receipt { receipt } => Ok(receipt),
            _ => Err(invalid()),
        }
    }
    async fn route(
        &self,
        bytes: Vec<u8>,
        context: Option<crate::storage::context::RequestContext>,
        staging: crate::scheduling::MemoryCharge,
    ) -> Result<EncodedResponse> {
        receipt_observation::route(self, bytes, context, staging).await
    }
}
impl crate::api::stream::Backend for Api {
    const PROXY_BATCH_BYTES: usize = crate::node::stream::wire::MAX_BATCH_BYTES + 1;

    fn frontend(&self) -> &crate::api::stream::Frontend {
        &self.stream_frontend
    }

    fn scheduler(&self) -> &crate::scheduling::ResourceScheduler {
        self.pool.scheduler()
    }

    fn authorize(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> std::result::Result<
        Option<crate::storage::context::RequestContext>,
        crate::node::stream::wire::Error,
    > {
        if !http::bearer(headers, &self.attached.local().prepared.auth.data_token) {
            return Err(failure(ErrorCode::Unauthorized).into());
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable().into());
        }
        Ok(self.request_context(headers)?)
    }

    async fn call(
        &self,
        request: crate::node::stream::wire::Request,
        context: Option<crate::storage::context::RequestContext>,
    ) -> std::result::Result<crate::node::stream::wire::Reply, crate::node::stream::wire::Error>
    {
        let control = self.load().await.map_err(|error| {
            if error.code == ErrorCode::StorageUnavailable {
                failure(ErrorCode::Overloaded)
            } else {
                error
            }
        })?;
        crate::node::stream::remote(&self.client, &self.signer, control.root(), request, context)
            .await
    }
    async fn renew(&self, token: Uuid) -> bool {
        let Ok(_charge) = self.pool.scheduler().charge_memory(0, 32 * 1024) else {
            return false;
        };
        let Ok(control) = self.load().await else {
            return false;
        };
        crate::node::stream::renew(&self.client, &self.signer, control.root(), token)
            .await
            .is_ok()
    }

    async fn release(&self, token: Uuid) {
        if let (Ok(_charge), Ok(control)) = (
            self.pool.scheduler().charge_memory(0, 32 * 1024),
            self.load().await,
        ) {
            let _ = crate::node::stream::release(&self.client, &self.signer, control.root(), token)
                .await;
        }
    }
}

impl receipt_observation::QueueAccess for Api {
    async fn submit(
        &self,
        body: Vec<u8>,
        context: Option<crate::storage::context::RequestContext>,
    ) -> Result<crate::request_queue::RequestRecord> {
        Api::submit(self, body, context).await
    }
    async fn lookup(
        &self,
        run_id: Uuid,
        request_id: Uuid,
        context: Option<crate::storage::context::RequestContext>,
    ) -> Result<crate::request_queue::Receipt> {
        Api::lookup(self, run_id, request_id, context).await
    }
}
pub(crate) fn heartbeat_binding(root: &Control, node: &Node) -> Result<HeartbeatBinding> {
    let session = node.session.as_ref().ok_or_else(unavailable)?;
    Ok(HeartbeatBinding {
        resources: node.resources,
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
                time::now() + Duration::from_secs(30),
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
fn route_failure(
    id: Uuid,
    mutation: bool,
    accepted: bool,
    error: protocol::ProtocolError,
) -> EncodedResponse {
    encode_error(
        Some(id),
        if accepted
            || mutation
                && matches!(
                    error.code,
                    ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown
                )
        {
            unknown()
        } else {
            error
        },
    )
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
    let context = match api.request_context(request.headers()) {
        Ok(context) => context,
        Err(error) => return response(encode_error(None, error)),
    };
    if request.uri().query().is_some() {
        return response(encode_error(None, invalid()));
    }

    if !crate::api::json_content_type(request.headers()) {
        return response(encode_error(None, invalid()));
    }
    let admitted = match api.data.try_admit() {
        Ok(v) => v,
        Err(_) => return response(encode_error(None, unknown())),
    };
    let work = admitted.spawn(move || async move {
        let result = async {
            let _staging = api.pool.scheduler().charge_memory(
                0,
                (3 * protocol::MAX_BODY_BYTES + protocol::MAX_DECODED_PAYLOAD_BYTES) as u64,
            )?;
            let length = http::framing(request.headers(), protocol::MAX_BODY_BYTES, false)?
                .ok_or_else(invalid)?;
            let bytes = http::read_small(
                request.into_body(),
                length,
                time::now() + Duration::from_secs(30),
            )
            .await?;
            api.route(bytes, context, _staging).await
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

async fn run_initialization(State(api): State<Arc<Api>>, request: Request) -> Response {
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.enrollment_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let result=async{let loaded=api.storage.load_cluster_catalog().await.map_err(|_|unavailable())?;Ok::<_,protocol::ProtocolError>(EncodedResponse{status:200,body:serde_json::to_vec(&json!({"fence":loaded.catalog().fence,"initialization_allowed":api.new_run.load(Ordering::Acquire)})).map_err(|_|invalid())?})}.await;
    response(result.unwrap_or_else(|e| encode_error(None, e)))
}

async fn submit_request(State(api): State<Arc<Api>>, request: Request) -> Response {
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.data_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let context = match api.request_context(request.headers()) {
        Ok(context) => context,
        Err(error) => return response(encode_error(None, error)),
    };
    if request.uri().query().is_some() {
        return response(encode_error(None, invalid()));
    }

    if !crate::api::json_content_type(request.headers()) {
        return response(encode_error(None, invalid()));
    }
    let admitted = match api.submissions.try_admit() {
        Ok(admitted) => admitted,
        Err(error) => return response(encode_error(None, error)),
    };
    let operation=admitted.spawn(move||async move{
        if !api.accepting.load(Ordering::Acquire){return Err(unavailable());}
        let _staging=api.pool.scheduler().charge_memory(0,(3*protocol::MAX_BODY_BYTES+protocol::MAX_DECODED_PAYLOAD_BYTES) as u64)?;
        let length=http::framing(request.headers(),protocol::MAX_BODY_BYTES,false)?.ok_or_else(invalid)?;
        let bytes=http::read_small(request.into_body(),length,time::now()+Duration::from_secs(30)).await?;
        let parsed=protocol::parse_request(&bytes)?;let id=parsed.request_id;drop(parsed);
        Ok(match api.submit(bytes, context).await{
            Ok(record)=>EncodedResponse{status:202,body:serde_json::to_vec(&json!({"request_id":record.request_id,"run_id":record.run_id,"state":"queued"})).map_err(|_|invalid())?},
            Err(error)=>encode_error(Some(id),error),
        })
    });
    let result = match operation {
        Ok(operation) => operation.receive().await,
        Err(error) => Err(error),
    };
    response(result.unwrap_or_else(|error| encode_error(None, error)))
}
async fn receipt_request(
    State(api): State<Arc<Api>>,
    Path((run_id, request_id)): Path<(String, String)>,
    request: Request,
) -> Response {
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.data_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let context = match api.request_context(request.headers()) {
        Ok(context) => context,
        Err(error) => return response(encode_error(None, error)),
    };
    if request.uri().query().is_some() {
        return response(encode_error(None, invalid()));
    }

    let (run_id, request_id) = match (Uuid::parse_str(&run_id), Uuid::parse_str(&request_id)) {
        (Ok(run), Ok(id)) => (run, id),
        _ => return response(encode_error(None, invalid())),
    };
    if request.uri().query().is_some() {
        return response(encode_error(Some(request_id), invalid()));
    }
    response(match api.lookup(run_id, request_id, context).await {
        Ok(receipt) => EncodedResponse {
            status: 200,
            body: serde_json::to_vec(&receipt).unwrap(),
        },
        Err(error) => encode_error(Some(request_id), error),
    })
}

async fn frame(stream: &mut UnixStream, end: Option<Instant>) -> Result<Vec<u8>> {
    crate::supervisor::frame(stream, end)
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::InvalidData {
                invalid()
            } else {
                unavailable()
            }
        })
}
async fn send(stream: &mut UnixStream, value: serde_json::Value, end: Instant) -> Result<()> {
    crate::supervisor::send(stream, &value, end)
        .await
        .map_err(|_| unavailable())
}
fn supervisor() -> Result<UnixStream> {
    crate::supervisor::open().map_err(|_| invalid())
}
pub async fn run_api(path: PathBuf) -> std::result::Result<(), &'static str> {
    run_api_configured(path, false, false).await
}
pub async fn run_api_with_cold_start(
    path: PathBuf,
    cold_start: bool,
) -> std::result::Result<(), &'static str> {
    run_api_configured(path, cold_start, false).await
}
pub async fn run_api_configured(
    path: PathBuf,
    cold_start: bool,
    migrate_catalog: bool,
) -> std::result::Result<(), &'static str> {
    let mut channel = supervisor().map_err(|_| "protocol_error")?;
    let (pool, threads) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| "startup_failed")?;
    let result = api_process(path, &mut channel, pool, cold_start, migrate_catalog).await;
    if result.is_err() {
        let _ = send(
            &mut channel,
            json!({"version":1,"type":"fatal","code":"startup_failed"}),
            time::now() + Duration::from_secs(2),
        )
        .await;
    }
    let joined = threads.shutdown().await.map_err(|_| "startup_failed");
    result.map_err(|_| "startup_failed").and(joined)
}
async fn api_process(
    path: PathBuf,
    channel: &mut UnixStream,
    pool: WorkerPool,
    cold_start: bool,
    migrate_catalog: bool,
) -> Result<()> {
    let startup_end = time::now() + Duration::from_secs(30);
    let init: Init =
        serde_json::from_slice(&frame(channel, Some(startup_end)).await?).map_err(|_| invalid())?;
    let provisioning = match (init.provisioning.as_str(), init.bridge.as_ref()) {
        ("external", None) if init.initial_policy.mode == "fixed" => Provisioning::External,
        ("managed", Some(bridge)) => {
            bridge.validate()?;
            Provisioning::Managed
        }
        _ => return Err(invalid()),
    };
    let policy_mode = match init.initial_policy.mode.as_str() {
        "fixed" => PolicyMode::Fixed,
        "autoscale" => PolicyMode::Autoscale,
        _ => return Err(invalid()),
    };
    if init.version != 1
        || init.kind != "init"
        || init.initial_policy.max_nodes > 1024
        || init.initial_policy.min_nodes > init.initial_policy.desired
        || init.initial_policy.desired > init.initial_policy.max_nodes
        || (policy_mode == PolicyMode::Fixed
            && (init.initial_policy.desired != init.initial_policy.min_nodes
                || init.initial_policy.desired != init.initial_policy.max_nodes))
        || (policy_mode == PolicyMode::Autoscale
            && (init.initial_policy.min_nodes == 0
                || init.initial_policy.desired != init.initial_policy.min_nodes))
    {
        return Err(invalid());
    }
    protocol::validate_identifier(&init.name)?;
    let local = cpu(&pool, move |runtime| {
        prepare_api(&path, startup_end.into_std())
            .and_then(|p| runtime.block_on(p.connect_async()))
            .map_err(|_| {
                eprintln!("nabedra-db: startup material invalid");
                invalid()
            })
    })
    .await?;
    let template = &local.prepared.input.value.template;
    let namespace = match &template.storage {
        super::configuration::Storage::Http(h) => {
            Namespace::new(&h.namespace).map_err(|_| invalid())?
        }
        super::configuration::Storage::S3(s) => {
            Namespace::new(&s.namespace).map_err(|_| invalid())?
        }
    };
    let client = HttpClient::new(local.prepared.tls.as_ref())?;
    let process = ids::uuid_v4();
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
            adapter_contract: (provisioning == Provisioning::Managed)
                .then(|| "meshdb.callbacks.v1".into()),
        },
        provisioning,
        template: template.clone(),
        timing: local.prepared.input.value.lifecycle.clone(),
        revision: 1,
        change_id: ids::uuid_v4(),
        policy: Policy {
            revision: 1,
            mode: policy_mode,
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
            coordinator_node: None,
            scheduler_term: 0,
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
    let proposed_fence = crate::storage::CatalogFence {
        run_id: ids::uuid_v4(),
        scheduler_term: 1,
    };
    let (attached, initialization_new) = cpu(&pool, {
        let storage = storage.clone();
        move |runtime| {
            let catalog = runtime
                .block_on(storage.initialize_fenced_cluster_catalog(
                    proposed_fence,
                    migrate_catalog,
                    false,
                ))
                .map_err(|e| {
                    eprintln!("nabedra-db: catalog initialization {e}");
                    unavailable()
                })?;
            let loaded = runtime
                .block_on(storage.initialize_control(&initial, startup_end))
                .map_err(|e| {
                    eprintln!("nabedra-db: control initialization {e}");
                    unavailable()
                })?;
            let attached = check_attach(loaded, local, &init.name, provisioning).map_err(|e| {
                eprintln!("nabedra-db: attachment {e}");
                invalid()
            })?;
            Ok((attached, catalog.catalog().fence == Some(proposed_fence)))
        }
    })
    .await?;
    let tls = crate::runtime::retained_tls(attached.local().prepared.tls.as_ref())?;
    let bridge = init.bridge;
    let data_coordination =
        crate::node::queue_runtime::coordination_capacity(&attached.root().template)?;
    let api = Arc::new(Api {
        bridge,
        attached,
        storage,
        pool,
        client,
        signer,
        process,
        guard: Mutex::new(OwnershipGuard::new(process)),
        changes: tokio::sync::Mutex::new(()),
        observations: Arc::new(Mutex::new(Observations::new(process, time::now()))),
        status_observations: Mutex::new(Observations::new(process, time::now())),
        data: Executor::new(data_coordination, 0)?,
        controls: Executor::new(8, 0)?,
        submissions: Executor::new(16, 0)?,
        stream_frontend: crate::api::stream::Frontend::new()?,
        accepting: AtomicBool::new(false),
        cold_start: AtomicBool::new(cold_start),
        authority_epoch: AtomicU64::new(0),
        new_run: AtomicBool::new(initialization_new),
    });
    let authorizer = if api.bridge.is_some() {
        Some(
            TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|_| unavailable())?,
        )
    } else {
        None
    };
    let authorize_url = authorizer.as_ref().map(|l| {
        format!(
            "http://127.0.0.1:{}/private/v1/authorize",
            l.local_addr().unwrap().port()
        )
    });
    send(channel, json!({"version":1,"type":"prepared","cluster_id":api.attached.root().cluster_id,"bound_url":bound_url,"authorize_url":authorize_url}), startup_end).await?;
    let activate: Message =
        serde_json::from_slice(&frame(channel, Some(startup_end)).await?).map_err(|_| invalid())?;
    if activate.version != 1 || activate.kind != "activate" {
        return Err(invalid());
    }
    // Additional API processes route to the existing fenced scheduler. Only an
    // explicit new/cold run must acquire controller ownership before readiness.
    while api.ownership().await.is_err() {
        if !cold_start && !initialization_new {
            if let (Ok(control), Ok(catalog), Ok(now)) = (
                api.load().await,
                api.storage.load_cluster_catalog().await,
                sample(),
            ) {
                if follower_ready(
                    &control.root().controller,
                    control.root().scheduler.scheduler_term,
                    catalog.catalog().fence,
                    now.unix_ms(),
                    false,
                ) {
                    break;
                }
            }
        }
        if time::now() >= startup_end {
            return Err(unavailable());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    let (stop, mut renew_stop) = watch::channel(false);
    let authorizer = authorizer.map(|listener| {
        task::spawn(provider_runtime::authorizer(
            api.clone(),
            listener,
            stop.subscribe(),
        ))
    });
    let renewal_interval = Duration::from_millis(api.attached.root().timing.renew_ms);
    let owner = api.clone();
    let renewal = task::spawn(async move {
        loop {
            let _ = owner.ownership().await;
            tokio::select! { _ = renew_stop.changed() => break, _ = time::sleep(renewal_interval) => {} }
        }
    });
    let owner = api.clone();
    let mut probe_stop = stop.subscribe();
    let probes = task::spawn(async move {
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
            let _ = owner.observe_losses().await;
            let _ = owner.provider_tick().await;
            tokio::select! { _ = probe_stop.changed() => break, _ = time::sleep(Duration::from_millis(250)) => {} }
        }
    });
    api.accepting.store(true, Ordering::Release);
    let ready = send(channel, json!({"version":1,"type":"ready","cluster_id":api.attached.root().cluster_id,"bound_url":bound_url}), startup_end).await;
    let app = Router::new()
        .route("/v1/operations", post(operations))
        .route(
            "/v1/cluster/operations",
            post(provider_runtime::cluster_operations),
        )
        .route("/v1/requests", post(submit_request))
        .route("/v1/requests/{run_id}/{request_id}", get(receipt_request))
        .route("/internal/v1/enroll", post(enroll))
        .route("/internal/v1/queue/run", get(run_initialization))
        .route("/health/live", get(|| async { "true" }))
        .route("/health/ready", get(|| async { "true" }))
        .merge(crate::api::stream::routes::<Api>())
        .with_state(api.clone());
    let served = if ready.is_ok() {
        crate::runtime::serve_router(listener, app, tls, async {
            tokio::select! { _ = frame(channel, None) => {}, _ = crate::runtime::shutdown_signal() => {} }
            api.accepting.store(false,Ordering::Release); api.data.stop_accepting(); api.controls.stop_accepting();api.submissions.stop_accepting();
            api.stream_frontend.stop_accepting();
        }).await.map_err(|_| unavailable())
    } else {
        ready
    };
    api.accepting.store(false, Ordering::Release);
    api.data.stop_accepting();
    api.controls.stop_accepting();
    api.submissions.stop_accepting();
    stop.send_replace(true);
    let (renewed, probed) = tokio::join!(renewal, probes);
    api.data.drain().await;
    api.controls.drain().await;
    api.submissions.drain().await;
    api.stream_frontend.drain().await;
    renewed.map_err(|_| unavailable())?;
    probed.map_err(|_| unavailable())?;
    if let Some(authorizer) = authorizer {
        authorizer.await.map_err(|_| unavailable())??;
    }
    served
}

fn follower_ready(
    controller: &Controller,
    scheduler_term: u64,
    fence: Option<crate::storage::CatalogFence>,
    now_ms: u64,
    requires_ownership: bool,
) -> bool {
    !requires_ownership
        && controller.owner_id.is_some()
        && controller.lease_state == LeaseState::Held
        && controller
            .lease_until_ms
            .is_some_and(|until| until > now_ms)
        && scheduler_term > 0
        && fence.is_some_and(|fence| fence.scheduler_term == scheduler_term)
}

#[cfg(test)]
mod queue_response_tests {
    use super::*;
    use nabedra_rt::ids;
    #[test]
    fn u02_q05_follower_api_attaches_only_to_live_matching_authority() {
        let mut controller = Controller {
            owner_id: Some(ids::uuid_v4()),
            epoch: 3,
            lease_state: LeaseState::Held,
            lease_until_ms: Some(2000),
        };
        let fence = crate::storage::CatalogFence {
            run_id: ids::uuid_v4(),
            scheduler_term: 7,
        };
        assert!(follower_ready(&controller, 7, Some(fence), 1000, false));
        assert!(!follower_ready(&controller, 7, Some(fence), 1000, true));
        assert!(!follower_ready(&controller, 8, Some(fence), 1000, false));
        assert!(!follower_ready(&controller, 7, None, 1000, false));
        assert!(!follower_ready(&controller, 0, Some(fence), 1000, false));
        assert!(!follower_ready(&controller, 7, Some(fence), 2000, false));
        controller.lease_state = LeaseState::Released;
        assert!(!follower_ready(&controller, 7, Some(fence), 1000, false));
        controller.lease_state = LeaseState::Held;
        controller.owner_id = None;
        assert!(!follower_ready(&controller, 7, Some(fence), 1000, false));
    }
    #[test]
    fn q04_t04_post_accept_lookup_failure_preserves_identity_and_unknown_outcome() {
        let id = ids::uuid_v4();
        for mutation in [false, true] {
            let encoded =
                route_failure(id, mutation, true, http::failure(ErrorCode::LimitExceeded));
            let value: serde_json::Value = serde_json::from_slice(&encoded.body).unwrap();
            assert_eq!(encoded.status, 503);
            assert_eq!(value["request_id"], id.to_string());
            assert_eq!(value["error"]["code"], "outcome_unknown");
            assert_eq!(value["error"]["outcome"], "unknown");
        }
        let encoded = route_failure(id, true, false, http::failure(ErrorCode::Overloaded));
        assert_eq!(encoded.status, 429);
        let value: serde_json::Value = serde_json::from_slice(&encoded.body).unwrap();
        assert_eq!(value["error"]["outcome"], "not_applied");
    }
}
