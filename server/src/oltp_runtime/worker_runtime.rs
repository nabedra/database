//! The actual API runtime composes durable authority/participants with the
//! worker-memory queue. Bodies are never submitted to Raft or a request WAL.
use super::{
    admission::{DurableAdmission, ReservationResult},
    bootstrap::LocalServices,
    http_runtime::HttpRuntime,
    replica::*,
    transaction::TransactionCoordinator,
    worker_endpoint::AuthorityReader,
};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    executor::Executor,
    oltp::{
        mvcc::{
            catalog::{CaptureRead, IntentRef},
            planner::{NativePlanner, PlanError, PlannedOperation},
        },
        pool::{ByteClass, ChargedBytes, ExecutionClass, ScratchLease},
    },
    oltp_replication::{
        authority::status::{IdentityLookup, TerminalIdentity},
        multiplex::EndpointFuture,
        types::*,
    },
    protocol::{self, ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, Receipt, RequestRecord, RequestState},
    response::{self, EncodedResponse},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

pub(crate) struct WorkerRuntime {
    services: LocalServices,
    admission: DurableAdmission,
    planner: NativePlanner,
    transactions: TransactionCoordinator,
    ingress: Executor,
    actors: Executor,
    active: Mutex<BTreeSet<Uuid>>,
    gates: Mutex<BTreeMap<Uuid, Weak<AsyncMutex<()>>>>,
    changed: Notify,
    accepting: AtomicBool,
    authorization: OriginalAuthorization,
    _actor_memory: MemoryCharge,
}
struct DecodedInput {
    request: protocol::Request,
    context: AcceptedContext,
    owner: Arc<ScratchLease>,
}
impl WorkerRuntime {
    pub(crate) async fn open(
        path: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<Arc<Self>, ProtocolError> {
        if resources.max_threads < 4 || resources.max_threads > 256 {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "typed OLTP requires one query, one control and two storage maintenance threads",
                Outcome::NotApplied,
            ));
        }
        resources.validate_execution(&execution).map_err(|_| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid typed worker execution resources",
                Outcome::NotApplied,
            )
        })?;
        let services = LocalServices::open(path, database, resources, &execution).await?;
        let admission = DurableAdmission::new(
            services.shared.clone(),
            services.control.clone(),
            services.status.clone(),
            services.timestamps.clone(),
            services.scope.clone(),
            services.queue.clone(),
        )?;
        let planner = NativePlanner::new(services.catalog.clone(), services.resolver.clone());
        let transactions = TransactionCoordinator::new(
            services.shared.clone(),
            services.status.clone(),
            services.timestamps.clone(),
            services.resolver.clone(),
            services.catalog.participant().clone(),
        );
        let authorization = OriginalAuthorization {
            database: services.scope.database.clone(),
            principal: Digest::from_bytes(Sha256::digest(token.as_bytes()).into()),
            policy_revision: 1,
            ingress_id: services.local.worker_id,
            ingress_incarnation: services.local.incarnation,
            credential_expires_at_ms: None,
        };
        let actor_memory = services.shared.scheduler().charge_memory(
            services.shared.scheduler_node_id(),
            execution
                .max_pending_requests
                .checked_mul(64 << 10)
                .ok_or_else(unknown)?,
        )?;
        Ok(Arc::new(Self {
            services,
            admission,
            planner,
            transactions,
            ingress: Executor::new(2, 16)?,
            actors: Executor::new(execution.max_pending_requests as usize, 0)?,
            active: Mutex::new(BTreeSet::new()),
            gates: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
            accepting: AtomicBool::new(true),
            authorization,
            _actor_memory: actor_memory,
        }))
    }
    pub(crate) fn application(
        self: &Arc<Self>,
        token: String,
    ) -> Result<axum::Router, ProtocolError> {
        if Digest::from_bytes(Sha256::digest(token.as_bytes()).into())
            != self.authorization.principal
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "HTTP bearer scope differs from runtime",
                Outcome::NotApplied,
            ));
        }
        super::http::router_for(
            self.clone(),
            token,
            self.services.scope.database.as_str().to_owned(),
        )
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        &self.services.shared
    }
    pub(crate) async fn admit(
        self: &Arc<Self>,
        body: Vec<u8>,
    ) -> Result<RequestRecord, ProtocolError> {
        self.clone().submit_owned(body).await
    }
    pub(crate) async fn observe(&self, run: Uuid, id: Uuid) -> Result<Receipt, ProtocolError> {
        self.receipt_owned(run, id).await
    }
    fn gate(&self, id: Uuid) -> Result<Arc<AsyncMutex<()>>, ProtocolError> {
        let mut gates = self.gates.lock().map_err(|_| unknown())?;
        gates.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = gates.get(&id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id, Arc::downgrade(&gate));
        Ok(gate)
    }
    pub(crate) fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        self.ingress.stop_accepting();
        self.actors.stop_accepting();
    }
    pub(crate) async fn drain(&self) -> Result<(), ProtocolError> {
        self.accepting.store(false, Ordering::Release);
        self.ingress.drain().await;
        self.actors.drain().await;
        self.services.shutdown().await
    }
    async fn submit_owned(self: Arc<Self>, body: Vec<u8>) -> Result<RequestRecord, ProtocolError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        // The HTTP ingress owns this entire future independently of its socket.
        // Reserve an execution actor before creating durable pin metadata.
        let actor = self.actors.try_admit()?;
        let reserved = match self
            .admission
            .reserve(body, self.authorization.clone(), now())
            .await?
        {
            ReservationResult::Reserved(reserved) => reserved,
            ReservationResult::Abandoned(_) => return Err(QueueError::Abandoned.protocol()),
            ReservationResult::Terminal(terminal) => return terminal_record(&terminal),
        };
        let id = reserved.reservation.request_id;
        let gate = self.gate(id)?;
        let _gate = gate.lock().await;
        let existing = {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            replica.record(id).map(|record| record.accepted)
        };
        if existing.is_some() {
            let input = self.decode(id).await?;
            self.services
                .queue
                .admit(reserved.body.as_ref().to_vec(), input.context, now())
                .await?;
        } else {
            let raw = reserved.body.clone();
            let shared = self.services.shared.clone();
            let (operation, owner) = self.decode_raw(raw).await?;
            let pin = self.admission.begin_snapshot(&reserved).await?;
            let current = self.services.control.current(&self.services.scope).await?;
            let catalog = self
                .services
                .catalog
                .snapshot(pin.read_ts, current.ready.view.authority())?;
            let capture = loop {
                match self
                    .services
                    .catalog
                    .capture_lineage(&catalog, &operation.operation)
                    .await?
                {
                    CaptureRead::Captured(capture) => break capture,
                    CaptureRead::Blocked(intent) => {
                        self.help_intent(intent).await?;
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    }
                }
            };
            let read = QueueReadToken {
                identity: reserved.pin_identity(),
                storage_pin: pin,
                catalog_root: capture.catalog_root,
                schema_epoch: capture.schema_epoch,
                routing_epoch: capture.routing_epoch,
            };
            self.admission.accept(reserved, read, now()).await?;
            drop((operation, owner, shared));
        }
        let observation = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .observation(id)
            .map_err(QueueError::protocol)?;
        let start = self.active.lock().map_err(|_| unknown())?.insert(id);
        if start && !observation.state.terminal() {
            let runtime = self.clone();
            match actor.spawn(move || async move {
                let mut settlement = Settlement {
                    runtime: runtime.clone(),
                    id,
                    armed: true,
                };
                runtime.execute(id).await.map_err(|error| {
                    eprintln!(
                        "nabedra-db: request {id} execution failed: {:?}: {}",
                        error.code, error.message
                    );
                    error
                })?;
                settlement.armed = false;
                Ok(())
            }) {
                Ok(operation) => drop(operation),
                Err(error) => {
                    self.mark_unknown(id);
                    return Err(error);
                }
            }
        } else if start {
            self.active.lock().map_err(|_| unknown())?.remove(&id);
        }
        Ok(observation)
    }
    async fn decode_raw(
        &self,
        body: ChargedBytes,
    ) -> Result<(protocol::Request, Arc<ScratchLease>), ProtocolError> {
        let shared = self.services.shared.clone();
        let resources = shared.clone();
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let budget = crate::oltp::mvcc::request_decode_budget(body.as_ref())?
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(64 << 10))
                    .and_then(|n| {
                        super::worker_endpoint::request_raw_clone_bytes(body.as_ref())
                            .ok()
                            .and_then(|extra| n.checked_add(extra))
                    })
                    .ok_or_else(unknown)?;
                let owner = Arc::new(resources.reserve_scratch(ByteClass::Data, budget)?);
                let request = protocol::parse_request(body.as_ref())?;
                Ok((request, owner))
            })
            .await
    }
    async fn decode(&self, id: Uuid) -> Result<DecodedInput, ProtocolError> {
        let (body, context, record) = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .input(id)
            .map_err(QueueError::protocol)?;
        let (request, owner) = self.decode_raw(body).await?;
        let parsed =
            serde_json::from_slice::<AcceptedContext>(context.as_ref()).map_err(|_| unknown())?;
        if request.request_id != record.request_id
            || parsed
                .read
                .as_ref()
                .is_none_or(|read| read.identity.run_id != record.run_id)
        {
            return Err(unknown());
        }
        Ok(DecodedInput {
            request,
            context: parsed,
            owner,
        })
    }
    async fn binding(&self, id: Uuid) -> Result<(DecodedInput, RequestBinding), ProtocolError> {
        let input = self.decode(id).await?;
        let token = input.context.read.as_ref().ok_or_else(unknown)?;
        let binding = RequestBinding {
            run_id: token.identity.run_id,
            request_id: id,
            digest: token.identity.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(&input.context)?,
        };
        Ok((input, binding))
    }
    async fn execute(&self, id: Uuid) -> Result<(), ProtocolError> {
        self.services.queue.start(id).await?;
        let (input, request) = self.binding(id).await?;
        let read = input.context.read.as_ref().ok_or_else(unknown)?;
        let current = self.services.control.current(&self.services.scope).await?;
        let authority = current.ready.view.authority();
        let pin = self
            .services
            .catalog
            .snapshot(read.storage_pin.read_ts, authority)?;
        let txn = TxnId {
            request_id: id,
            attempt: 1,
        };
        let mutation = input.request.operation.is_mutation();
        let mut guards = BTreeMap::new();
        let planned = loop {
            let error = match self
                .planner
                .plan(
                    input.request.operation.clone(),
                    &pin,
                    txn,
                    request,
                    input.owner.clone(),
                )
                .await
            {
                Ok(plan) => break Ok(plan),
                Err(error) => error,
            };
            let mut error = error;
            loop {
                match error {
                    PlanError::Guarded {
                        cause,
                        guards: held,
                    } => {
                        for guard in held {
                            guards.insert(guard.binding().guard_id, guard);
                        }
                        error = *cause;
                    }
                    _ => break,
                }
            }
            match error {
                PlanError::Blocked(intent) => {
                    self.help_intent(intent).await?;
                }
                PlanError::LifecyclePending(binding) => {
                    if let Some(holder) = self
                        .services
                        .catalog
                        .participant()
                        .lifecycle_holder(binding.table_id)
                        .await?
                    {
                        self.help_lifecycle(holder).await?;
                    }
                }
                other => break Err(other),
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        };
        let encoded = match planned {
            Ok(PlannedOperation::Read(bytes)) => {
                let shared = self.services.shared.clone();
                shared
                    .execute(ExecutionClass::Query, 0, 0, move || {
                        let raw: &serde_json::value::RawValue =
                            serde_json::from_slice(bytes.as_ref()).map_err(|_| unknown())?;
                        Ok(response::encode_success(id, raw, false))
                    })
                    .await?
            }
            Ok(PlannedOperation::Mutation(plan)) => {
                let decision = self
                    .transactions
                    .commit(authority, txn, request, read.storage_pin.read_ts, plan)
                    .await?;
                match decision.outcome().ok_or_else(unknown)?.result() {
                    Ok(value) => response::encode_success(id, &value, true),
                    Err(error) => response::encode_error(Some(id), error),
                }
            }
            Err(PlanError::Protocol(error)) => {
                if mutation {
                    let terminal = self
                        .services
                        .status
                        .terminal_failure(authority, request, error.clone())
                        .await?;
                    if !guards.is_empty() {
                        let settled = self
                            .services
                            .status
                            .failure_settled_proof(txn, request)
                            .await?;
                        for (_, guard) in guards {
                            self.services
                                .catalog
                                .participant()
                                .release_lifecycle(authority, guard, settled.clone())
                                .await?;
                        }
                    }
                    terminal_response(id, &terminal)?
                } else {
                    response::encode_error(Some(id), error)
                }
            }
            Err(_) => return Err(unknown()),
        };
        self.services
            .queue
            .finish(id, encoded.status, encoded.body, now())
            .await?;
        self.changed.notify_waiters();
        Ok(())
    }
    async fn help_intent(&self, intent: IntentRef) -> Result<(), ProtocolError> {
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        if let Some(decision) = self
            .services
            .status
            .lookup_decision(intent.txn, intent.manifest)
            .await?
        {
            self.transactions.resolve(authority, &decision).await?;
        }
        Ok(())
    }
    async fn help_lifecycle(
        &self,
        binding: crate::oltp::mvcc::store::lifecycle::LifecycleBinding,
    ) -> Result<(), ProtocolError> {
        // Settlement capabilities are obtained from Status, never synthesized
        // from a timeout or the disappearance of an API observer.
        let _ = binding;
        Ok(())
    }
    fn mark_unknown(&self, id: Uuid) {
        if let Ok(mut replica) = self.services.replica.lock() {
            let current = replica.authority();
            let _ = replica.reconcile(current, id);
        }
        self.changed.notify_waiters();
    }
    async fn durable_lookup(&self, id: Uuid) -> Result<Option<IdentityLookup>, ProtocolError> {
        let raw = self.services.status.lookup(id, None).await?;
        let shared = self.services.shared.clone();
        let resources = shared.clone();
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = resources.reserve_scratch(
                    ByteClass::Data,
                    crate::oltp::mvcc::json_decode_budget(raw.as_ref())?,
                )?;
                serde_json::from_slice(raw.as_ref()).map_err(|_| unknown())
            })
            .await
    }
    async fn receipt_owned(&self, run: Uuid, id: Uuid) -> Result<Receipt, ProtocolError> {
        let local = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .response(run, id, now());
        match local {
            Ok((record, Some(bytes))) => {
                let shared = self.services.shared.clone();
                let resources = shared.clone();
                return shared
                    .execute(ExecutionClass::Query, 0, 0, move || {
                        let _owner = resources.reserve_scratch(
                            ByteClass::Data,
                            crate::oltp::mvcc::json_decode_budget(bytes.as_ref())?,
                        )?;
                        let value: serde_json::Value =
                            serde_json::from_slice(bytes.as_ref()).map_err(|_| unknown())?;
                        Ok(Receipt {
                            request_id: id,
                            run_id: run,
                            state: record.state,
                            result: value.get("data").filter(|value| !value.is_null()).cloned(),
                            error: value
                                .get("error")
                                .map(|value| {
                                    serde_json::from_value(value.clone()).map_err(|_| unknown())
                                })
                                .transpose()?,
                        })
                    })
                    .await;
            }
            Ok((record, None)) if record.state != RequestState::Reconciling => {
                return Ok(Receipt {
                    request_id: id,
                    run_id: run,
                    state: record.state,
                    result: None,
                    error: None,
                });
            }
            _ => {}
        }
        match self.durable_lookup(id).await? {
            Some(IdentityLookup::Terminal(terminal)) => {
                let record = terminal_record(&terminal)?;
                if record.run_id != run {
                    return Err(QueueError::Abandoned.protocol());
                }
                let response = terminal_response(id, &terminal)?;
                let value: serde_json::Value =
                    serde_json::from_slice(&response.body).map_err(|_| unknown())?;
                Ok(Receipt {
                    request_id: id,
                    run_id: run,
                    state: record.state,
                    result: value.get("data").filter(|value| !value.is_null()).cloned(),
                    error: value
                        .get("error")
                        .map(|value| serde_json::from_value(value.clone()).map_err(|_| unknown()))
                        .transpose()?,
                })
            }
            Some(IdentityLookup::Abandoned(_)) => Err(QueueError::Abandoned.protocol()),
            Some(IdentityLookup::Reserved(reservation)) if reservation.original_run != run => {
                Err(QueueError::Abandoned.protocol())
            }
            _ => match local {
                Ok((record, _)) => Ok(Receipt {
                    request_id: id,
                    run_id: run,
                    state: record.state,
                    result: None,
                    error: None,
                }),
                Err(error) => Err(error.protocol()),
            },
        }
    }
}
impl HttpRuntime for WorkerRuntime {
    fn ingress(&self) -> &Executor {
        &self.ingress
    }
    fn scheduler(&self) -> ResourceScheduler {
        self.services.shared.scheduler()
    }
    fn scheduler_node_id(&self) -> u64 {
        self.services.shared.scheduler_node_id()
    }
    fn submit(self: Arc<Self>, body: Vec<u8>) -> EndpointFuture<'static, RequestRecord> {
        Box::pin(self.submit_owned(body))
    }
    fn receipt(&self, run: Uuid, id: Uuid) -> EndpointFuture<'_, Receipt> {
        Box::pin(self.receipt_owned(run, id))
    }
    fn wait<'a>(&'a self, record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let receipt = self.receipt_owned(record.run_id, record.request_id).await?;
                if receipt.state == RequestState::Reconciling {
                    return Err(unknown());
                }
                if receipt.state.terminal() {
                    return Ok(match receipt.error {
                        Some(error) => response::encode_error(Some(record.request_id), error),
                        None => response::encode_success(record.request_id, &receipt.result, false),
                    });
                }
                changed.await;
            }
        })
    }
    fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            let current = self.services.control.current(&self.services.scope).await?;
            Ok(EncodedResponse {status:200,body:serde_json::to_vec(&serde_json::json!({"status":"ready","engine":"oltp","topology":"typed-groups-rf1","replication_factor":1,"run_id":current.ready.view.run_id,"cluster_id":self.services.scope.cluster_id,"native_thread_budget":self.services.shared.thread_budget()})).map_err(|_|unknown())?})
        })
    }
}
struct Settlement {
    runtime: Arc<WorkerRuntime>,
    id: Uuid,
    armed: bool,
}
impl Drop for Settlement {
    fn drop(&mut self) {
        if self.armed {
            self.runtime.mark_unknown(self.id)
        }
        if let Ok(mut active) = self.runtime.active.lock() {
            active.remove(&self.id);
        }
        self.runtime.changed.notify_waiters();
    }
}
fn terminal_response(
    id: Uuid,
    terminal: &TerminalIdentity,
) -> Result<EncodedResponse, ProtocolError> {
    match terminal {
        TerminalIdentity::Committed { outcome, .. } | TerminalIdentity::Failed { outcome, .. } => {
            Ok(match outcome.result() {
                Ok(value) => response::encode_success(id, &value, true),
                Err(error) => response::encode_error(Some(id), error),
            })
        }
        TerminalIdentity::ReadExpired { .. } => Err(QueueError::NotFound.protocol()),
    }
}
fn terminal_record(terminal: &TerminalIdentity) -> Result<RequestRecord, ProtocolError> {
    let reservation = match terminal {
        TerminalIdentity::Committed { reservation, .. }
        | TerminalIdentity::Failed { reservation, .. } => reservation,
        TerminalIdentity::ReadExpired { .. } => return Err(QueueError::NotFound.protocol()),
    };
    let response = terminal_response(reservation.request_id, terminal)?;
    Ok(RequestRecord {
        context: None,
        run_id: reservation.original_run,
        scheduler_term: 1,
        membership_revision: 1,
        request_id: reservation.request_id,
        digest: reservation.digest.to_string(),
        sequence: 1,
        version: 1,
        body: Vec::new(),
        accepted: true,
        state: if response.status == 200 {
            RequestState::Succeeded
        } else {
            RequestState::Failed
        },
        pinned_catalog: None,
        response_status: Some(response.status),
        response_body: None,
        completed_at_ms: Some(now()),
        result_expired: false,
        replicas: Default::default(),
    })
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "original accepted request requires reconciliation",
        Outcome::Unknown,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker runtime is not accepting requests",
        Outcome::NotApplied,
    )
}
