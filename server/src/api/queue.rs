//! Single-worker composition of the same replicated request state machine.
use super::response::EncodedResponse;
use crate::{
    engine::Engine,
    executor::Executor,
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueLimits, Receipt, RequestQueue, RequestRecord},
    storage::{CatalogFence, HistoryBudget, ObjectStore},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

pub(crate) struct LocalQueue {
    state: Mutex<Option<RequestQueue>>,
    initialization: AsyncMutex<()>,
    submissions: Mutex<()>,
    changed: Notify,
    member: Uuid,
    fence: Mutex<Option<CatalogFence>>,
    active: Mutex<std::collections::BTreeSet<Uuid>>,
    tasks: Executor,
    config: crate::config::ExecutionConfig,
    cold_start: bool,
    migration_allowed: bool,
    execution_headroom: u64,
    _charge: crate::scheduling::MemoryCharge,
}
impl LocalQueue {
    pub(super) fn new<B: ObjectStore>(
        engine: &Engine<B>,
        config: crate::config::ExecutionConfig,
        cold_start: bool,
        migration_allowed: bool,
    ) -> Result<Arc<Self>, ProtocolError> {
        config.validate().map_err(|_| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid execution configuration",
                Outcome::NotApplied,
            )
        })?;
        let bytes = config
            .max_queue_bytes_per_replica
            .checked_add(config.max_retained_result_bytes_per_worker)
            .ok_or_else(unavailable)?;
        let resources = engine.worker_pool().resources();
        // Execution stages cannot occupy the bytes needed to admit the remaining
        // queue actors, stage one ingress body, and run a maximum-size child.
        let execution_headroom = config
            .max_queue_bytes_per_replica
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(config.max_pending_requests.checked_mul(64 << 10)?))
            .and_then(|bytes| bytes.checked_add(resources.task_memory_limit_bytes))
            .and_then(|bytes| {
                bytes.checked_add(
                    (3 * crate::protocol::MAX_BODY_BYTES
                        + crate::protocol::MAX_DECODED_PAYLOAD_BYTES) as u64,
                )
            })
            .ok_or_else(unavailable)?;
        if bytes
            .checked_add(execution_headroom)
            .and_then(|bytes| bytes.checked_add(execution_working_bytes()))
            .is_none_or(|bytes| bytes > resources.working_bytes())
        {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "worker RAM cannot fit pending actors, ingress, child progress and an execution stage",
                Outcome::NotApplied,
            ));
        }
        let charge = engine.worker_pool().scheduler().charge_memory(0, bytes)?;
        Ok(Arc::new(Self {
            state: Mutex::new(None),
            initialization: AsyncMutex::new(()),
            submissions: Mutex::new(()),
            changed: Notify::new(),
            member: nabedra_rt::ids::uuid_v4(),
            fence: Mutex::new(None),
            active: Mutex::new(std::collections::BTreeSet::new()),
            tasks: Executor::new(config.max_pending_requests as usize, 0)?,
            config,
            cold_start,
            migration_allowed,
            execution_headroom,
            _charge: charge,
        }))
    }
    pub(crate) async fn initialize<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
    ) -> Result<CatalogFence, ProtocolError> {
        let _guard = self.initialization.lock().await;
        if let Some(fence) = *self.fence.lock().unwrap() {
            return Ok(fence);
        }
        let proposed = CatalogFence {
            run_id: nabedra_rt::ids::uuid_v4(),
            scheduler_term: 1,
        };
        // Catalog format 6 is the only format, so there is nothing to migrate.
        if self.migration_allowed {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "--migrate-catalog is not supported: catalog format 6 is the only format",
                Outcome::NotApplied,
            ));
        }
        let loaded = engine
            .storage()
            .initialize_fenced_cluster_catalog(proposed, self.cold_start)
            .await
            .map_err(|error| {
                ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    if error == crate::storage::StorageError::UnsupportedCatalog {
                        "catalog has an unsupported format; only format 6 is read"
                    } else {
                        "request queue initialization unavailable"
                    },
                    Outcome::Unknown,
                )
            })?;
        let fence = loaded.catalog().fence.ok_or_else(unavailable)?;
        if fence != proposed && !self.cold_start {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "no live queue remains; restart serve with --cold-start to fence old work",
                Outcome::Unknown,
            ));
        }
        let term = fence.scheduler_term;
        let limits = QueueLimits {
            max_pending_requests: self.config.max_pending_requests as usize,
            max_queue_bytes_per_replica: self.config.max_queue_bytes_per_replica as usize,
            max_retained_result_bytes_per_worker: self.config.max_retained_result_bytes_per_worker
                as usize,
            result_retention_ms: self.config.result_retention_ms,
        };
        let queue = RequestQueue::new(
            fence.run_id,
            term,
            1,
            limits,
            [self.member].into_iter().collect(),
        )
        .map_err(|e| e.protocol())?;
        *self.state.lock().unwrap() = Some(queue);
        *self.fence.lock().unwrap() = Some(fence);
        Ok(fence)
    }
    pub(super) async fn submit<B: ObjectStore + 'static>(
        self: &Arc<Self>,
        engine: Engine<B>,
        body: Vec<u8>,
    ) -> Result<RequestRecord, ProtocolError> {
        let fence = self.initialize(&engine).await?;
        // The queue owns its configured byte reserve. This extra owner retains
        // only the detached actor's actual request clone and bounded metadata.
        // Maximum decode/catalog/response stages wait inside the accepted task.
        let bytes_reserved = pending_working_bytes(body.len())?;
        let charge = engine
            .worker_pool()
            .scheduler()
            .charge_memory(0, bytes_reserved)
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "request actor memory capacity exhausted",
                    Outcome::NotApplied,
                )
            })?;
        let parsed = crate::protocol::parse_request(&body)?;
        let id = parsed.request_id;
        drop(parsed);
        // No await occurs while this short admission transition is serialized.
        let _submission = self.submissions.lock().unwrap();
        {
            let state = self.state.lock().unwrap();
            let q = state.as_ref().ok_or_else(unavailable)?;
            if q.record(id).is_some_and(|record| {
                record.accepted
                    && (record.state.terminal() || self.active.lock().unwrap().contains(&id))
            }) {
                drop(state);
                return self
                    .state
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or_else(unavailable)?
                    .propose(body, now())
                    .map_err(|error| error.protocol());
            }
        }
        // A finished request may have released its queue count before Executor
        // releases its admission guard. Reject here, before installing or ACKing
        // a new identity, rather than accepting a request without an owner.
        let admitted = self.tasks.try_admit()?;
        let proposal = self
            .state
            .lock()
            .unwrap()
            .as_mut()
            .ok_or_else(unavailable)?
            .propose(body, now())
            .map_err(|error| error.protocol())?;
        let bytes = proposal.body.clone();
        self.active.lock().unwrap().insert(id);
        let actor = ActiveLocalRequest {
            owner: self.clone(),
            id,
            sequence: proposal.sequence,
            _charge: charge,
        };
        let (start, start_rx) = tokio::sync::oneshot::channel();
        let owner = self.clone();
        let task = admitted.spawn(move || async move {
            let _actor = actor;
            let bytes = start_rx.await.map_err(|_| unavailable())?;
            owner.execute_record(engine, bytes, fence, id).await
        });
        if let Err(error) = task {
            let _ = self
                .state
                .lock()
                .unwrap()
                .as_mut()
                .ok_or_else(unavailable)?
                .discard_unaccepted(id, proposal.version);
            return Err(error);
        }
        // The detached task and all its resource ownership exist before this ACK.
        let record = {
            let mut state = self.state.lock().unwrap();
            let q = state.as_mut().ok_or_else(unavailable)?;
            match q.acknowledge_version(id, proposal.version, self.member) {
                Ok(record) => record,
                Err(error) => {
                    let _ = q.discard_unaccepted(id, proposal.version);
                    return Err(error.protocol());
                }
            }
        };
        if start.send(bytes).is_err() {
            self.fail_unsettled(id, proposal.sequence, false);
            return Err(unavailable());
        }
        Ok(record)
    }
    async fn execute_record<B: ObjectStore + 'static>(
        self: &Arc<Self>,
        engine: Engine<B>,
        bytes: Vec<u8>,
        fence: CatalogFence,
        id: Uuid,
    ) -> Result<(), ProtocolError> {
        let _execution = engine
            .worker_pool()
            .scheduler()
            .reserve_coordinator_memory(
                engine.worker_pool().request_id(),
                0,
                execution_working_bytes(),
                self.execution_headroom,
            )
            .await?;
        let result = async {
            let loaded = engine
                .storage()
                .load_cluster_catalog()
                .await
                .map_err(|_| unavailable())?;
            let mutation = crate::protocol::parse_request(&bytes)?
                .operation
                .is_mutation();
            let pinned = if mutation { None } else { Some(loaded) };
            let pin = pinned
                .as_ref()
                .map(|loaded| loaded.catalog().encode())
                .transpose()
                .map_err(|_| unavailable())?;
            {
                let mut state = self.state.lock().unwrap();
                let q = state.as_mut().ok_or_else(unavailable)?;
                let record = q.start(id, pin).map_err(|error| error.protocol())?;
                q.acknowledge_version(id, record.version, self.member)
                    .map_err(|error| error.protocol())?;
            }
            let encoded = if mutation {
                self.settle_mutation(&engine, bytes, fence, id).await?
            } else {
                engine.execute_identified(bytes, fence, pinned).await?
            };
            if !mutation
                && engine
                    .storage()
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?
                    .catalog()
                    .fence
                    != Some(fence)
            {
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "request run was fenced before result settlement",
                    Outcome::Unknown,
                ));
            }
            Ok(encoded)
        }
        .await;
        let encoded = match result {
            Ok(encoded) => encoded,
            // An unknown mutation outcome is never settled as terminal
            // `failed`: the publication may have landed, and its receipt would
            // later read `succeeded` (SCHED-12). The record stays reconciling.
            Err(error) if error.outcome == Outcome::Unknown && self.record_is_mutation(id) => {
                return Err(error);
            }
            Err(error) => super::response::encode_error(Some(id), error),
        };
        let mut state = self.state.lock().unwrap();
        let q = state.as_mut().ok_or_else(unavailable)?;
        let record = q
            .finish(id, encoded.status, encoded.body, now())
            .map_err(|error| error.protocol())?;
        q.acknowledge_version(id, record.version, self.member)
            .map_err(|error| error.protocol())?;
        Ok(())
    }
    /// Execute a mutation until its outcome is definite. Each retry carries
    /// the same identity and body, so publication finds an earlier attempt's
    /// durable decision instead of applying twice. While the outcome is
    /// unknown the record is `reconciling`, as in the distributed runtime.
    async fn settle_mutation<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
        bytes: Vec<u8>,
        fence: CatalogFence,
        id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        let mut attempts = 0u32;
        loop {
            match engine.execute_identified(bytes.clone(), fence, None).await {
                Err(error) if error.outcome == Outcome::Unknown => {
                    attempts = attempts.saturating_add(1);
                    self.mark_reconciling(id);
                    // A fenced run cannot settle here: keep the record
                    // reconciling and let a receipt settle it from history.
                    if attempts >= MAX_LOCAL_SETTLE_ATTEMPTS
                        || engine
                            .storage()
                            .load_cluster_catalog()
                            .await
                            .is_ok_and(|loaded| loaded.catalog().fence != Some(fence))
                    {
                        return Err(error);
                    }
                    let delay = Duration::from_millis(100)
                        .saturating_mul(1u32 << attempts.saturating_sub(1).min(16))
                        .min(Duration::from_secs(30));
                    nabedra_rt::time::sleep(delay).await;
                }
                other => return other,
            }
        }
    }
    fn mark_reconciling(&self, id: Uuid) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(q) = state.as_mut()
            && q.record(id).is_some_and(|record| {
                record.accepted
                    && !record.state.terminal()
                    && record.state != crate::request_queue::RequestState::Reconciling
            })
            && let Ok(record) = q.transition(id, crate::request_queue::RequestState::Reconciling)
        {
            let _ = q.acknowledge_version(id, record.version, self.member);
        }
        drop(state);
        self.changed.notify_waiters();
    }
    fn record_is_mutation(&self, id: Uuid) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(|q| q.record(id))
            .is_some_and(|record| {
                crate::protocol::parse_request(&record.body)
                    .is_ok_and(|request| request.operation.is_mutation())
            })
    }
    fn fail_unsettled(&self, id: Uuid, sequence: u64, retire: bool) {
        if self.record_is_mutation(id) {
            // The actor ended before a definite outcome. A mutation may have
            // published: it stays reconciling until history settles it.
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(q) = state.as_mut()
                && q.record(id).is_some_and(|record| {
                    record.sequence == sequence
                        && record.accepted
                        && !record.state.terminal()
                        && record.state != crate::request_queue::RequestState::Reconciling
                })
                && let Ok(record) =
                    q.transition(id, crate::request_queue::RequestState::Reconciling)
            {
                let _ = q.acknowledge_version(id, record.version, self.member);
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(q) = state.as_mut() {
            if q.record(id).is_some_and(|record| {
                record.sequence == sequence
                    && record.accepted
                    && !record.state.terminal()
                    && record.state != crate::request_queue::RequestState::Reconciling
            }) {
                let encoded = super::response::encode_error(
                    Some(id),
                    ProtocolError::new(
                        ErrorCode::OutcomeUnknown,
                        "request actor ended before terminal settlement; reconcile its outcome",
                        Outcome::Unknown,
                    ),
                );
                if let Ok(record) = q.finish(id, encoded.status, encoded.body, now()) {
                    let _ = q.acknowledge_version(id, record.version, self.member);
                }
            }
            // An unaccepted proposal can be rolled back and retried before its
            // old task polls the closed start gate. That task must not settle or
            // retire the newer owner of the same logical request ID.
            if retire
                && q.record(id)
                    .is_none_or(|record| record.sequence == sequence)
            {
                self.active
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&id);
            }
        }
    }
    pub(super) async fn receipt<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
        run: Uuid,
        id: Uuid,
    ) -> Result<Receipt, ProtocolError> {
        let current = self.initialize(engine).await?;
        let result = self
            .state
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(unavailable)?
            .lookup(run, id, now());
        match result {
            Ok(receipt) if receipt.state == crate::request_queue::RequestState::Reconciling => {
                self.settle_receipt(engine, receipt).await
            }
            Ok(receipt) => Ok(receipt),
            Err(error)
                if run != current.run_id || error == crate::request_queue::QueueError::NotFound =>
            {
                let loaded = engine
                    .storage()
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?;
                let budget = HistoryBudget::new(
                    4096,
                    64 << 20,
                    nabedra_rt::time::now() + Duration::from_secs(30),
                )
                .map_err(|_| unavailable())?;
                if let Some(d) = engine
                    .storage()
                    .lookup_decision_by_id(&loaded, id, budget)
                    .await
                    .map_err(|_| unavailable())?
                {
                    let value: serde_json::Value =
                        serde_json::from_str(&d.response_body).map_err(|_| unavailable())?;
                    return Ok(Receipt {
                        run_id: run,
                        request_id: id,
                        state: if d.response_status == 200 {
                            crate::request_queue::RequestState::Succeeded
                        } else {
                            crate::request_queue::RequestState::Failed
                        },
                        result: value.get("data").filter(|v| !v.is_null()).cloned(),
                        error: value
                            .get("error")
                            .map(|v| serde_json::from_value(v.clone()))
                            .transpose()
                            .map_err(|_| unavailable())?,
                    });
                }
                if run != current.run_id {
                    // A serve process that outlived a newer run's cold start
                    // still caches the old fence. It cannot tell an old run
                    // from the newer one: only a current fence may answer
                    // `abandoned` (SCHED-11, the local twin of OLAP-2).
                    if loaded.catalog().fence != Some(current) {
                        return Err(ProtocolError::new(
                            ErrorCode::StorageUnavailable,
                            "this serve process was fenced by a newer run",
                            Outcome::Unknown,
                        ));
                    }
                    return Ok(Receipt {
                        run_id: run,
                        request_id: id,
                        state: crate::request_queue::RequestState::Abandoned,
                        result: None,
                        error: None,
                    });
                }
                Err(error.protocol())
            }
            Err(error) => Err(error.protocol()),
        }
    }
    /// A reconciling mutation settles from its durable decision, if any.
    async fn settle_receipt<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
        receipt: Receipt,
    ) -> Result<Receipt, ProtocolError> {
        let loaded = engine
            .storage()
            .load_cluster_catalog()
            .await
            .map_err(|_| unavailable())?;
        let budget = HistoryBudget::new(
            4096,
            64 << 20,
            nabedra_rt::time::now() + Duration::from_secs(30),
        )
        .map_err(|_| unavailable())?;
        let Some(decision) = engine
            .storage()
            .lookup_decision_by_id(&loaded, receipt.request_id, budget)
            .await
            .map_err(|_| unavailable())?
        else {
            return Ok(receipt);
        };
        {
            let mut state = self.state.lock().unwrap();
            let q = state.as_mut().ok_or_else(unavailable)?;
            if q.record(receipt.request_id).is_some_and(|record| {
                record.state == crate::request_queue::RequestState::Reconciling
                    && crate::node::http::digest(decision.request_body.as_bytes()) == record.digest
            }) {
                let record = q
                    .finish(
                        receipt.request_id,
                        decision.response_status,
                        decision.response_body.as_bytes().to_vec(),
                        now(),
                    )
                    .map_err(|error| error.protocol())?;
                q.acknowledge_version(receipt.request_id, record.version, self.member)
                    .map_err(|error| error.protocol())?;
            }
        }
        self.changed.notify_waiters();
        let value: serde_json::Value =
            serde_json::from_str(&decision.response_body).map_err(|_| unavailable())?;
        Ok(Receipt {
            state: if decision.response_status == 200 {
                crate::request_queue::RequestState::Succeeded
            } else {
                crate::request_queue::RequestState::Failed
            },
            result: value.get("data").filter(|v| !v.is_null()).cloned(),
            error: value
                .get("error")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()
                .map_err(|_| unavailable())?,
            ..receipt
        })
    }
    pub(super) async fn wait<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
        record: &RequestRecord,
    ) -> Result<EncodedResponse, ProtocolError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let receipt = self
                .receipt(engine, record.run_id, record.request_id)
                .await?;
            if receipt.state == crate::request_queue::RequestState::Abandoned {
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "request was abandoned after the worker run ended",
                    Outcome::Unknown,
                ));
            }
            // A reconciling mutation whose actor ended cannot settle while
            // this caller waits: answer with the unknown outcome.
            if receipt.state == crate::request_queue::RequestState::Reconciling
                && !self.active.lock().unwrap().contains(&record.request_id)
            {
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "mutation outcome is reconciling; retry the same identity and body",
                    Outcome::Unknown,
                ));
            }
            if receipt.state.terminal() {
                return Ok(if let Some(e) = receipt.error {
                    super::response::encode_error(Some(record.request_id), e)
                } else {
                    EncodedResponse{status:200,body:serde_json::to_vec(&serde_json::json!({"request_id":record.request_id,"data":receipt.result})).map_err(|_|unavailable())?}
                });
            }
            changed.await;
        }
    }
    pub(crate) fn executor(&self) -> Executor {
        self.tasks.clone()
    }
}
/// Attempts of one local mutation before its actor leaves it reconciling.
/// About five minutes at the capped backoff.
const MAX_LOCAL_SETTLE_ATTEMPTS: u32 = 16;

fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "request queue unavailable",
        Outcome::Unknown,
    )
}
fn now() -> u64 {
    crate::node::queue_runtime::now_ms()
}

// The guard is captured before spawning, so even cancellation before its first
// poll retains and then settles the original resources exactly once.
struct ActiveLocalRequest {
    owner: Arc<LocalQueue>,
    id: Uuid,
    sequence: u64,
    _charge: crate::scheduling::MemoryCharge,
}
impl Drop for ActiveLocalRequest {
    fn drop(&mut self) {
        self.owner.fail_unsettled(self.id, self.sequence, true);
        self.owner.changed.notify_waiters();
    }
}
fn pending_working_bytes(body: usize) -> Result<u64, ProtocolError> {
    (body as u64)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(64 << 10))
        .ok_or_else(unavailable)
}
fn execution_working_bytes() -> u64 {
    (2 * crate::protocol::MAX_DECODED_PAYLOAD_BYTES
        + 4 * crate::protocol::MAX_CATALOG_BYTES
        + 3 * crate::protocol::MAX_BODY_BYTES) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Download, Namespace, Storage, StorageError, WriteCondition};
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Default)]
    struct PanicOnce {
        store: crate::test_storage::Fake,
        panic: AtomicBool,
    }
    impl ObjectStore for PanicOnce {
        async fn get(&self, key: &str) -> Result<Download, StorageError> {
            assert!(
                !self.panic.swap(false, Ordering::SeqCst),
                "injected request actor unwind"
            );
            self.store.get(key).await
        }
        async fn put(
            &self,
            key: &str,
            bytes: Vec<u8>,
            condition: WriteCondition<'_>,
        ) -> Result<String, StorageError> {
            self.store.put(key, bytes, condition).await
        }
    }
    #[test]
    fn q02_rollback_cannot_discard_an_accepted_or_different_version() {
        let member = nabedra_rt::ids::uuid_v4();
        let mut queue = RequestQueue::new(
            nabedra_rt::ids::uuid_v4(),
            1,
            1,
            QueueLimits {
                max_pending_requests: 2,
                max_queue_bytes_per_replica: 1 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 1000,
            },
            [member].into_iter().collect(),
        )
        .unwrap();
        let id = nabedra_rt::ids::uuid_v4();
        let body = serde_json::to_vec(
            &serde_json::json!({"request_id":id,"operation":"table.list","payload":{}}),
        )
        .unwrap();
        let proposal = queue.propose(body.clone(), 0).unwrap();
        assert!(queue.discard_unaccepted(id, proposal.version + 1).is_err());
        assert!(queue.record(id).is_some());
        queue.discard_unaccepted(id, proposal.version).unwrap();
        assert!(queue.record(id).is_none());
        let proposal = queue.propose(body, 0).unwrap();
        let accepted = queue
            .acknowledge_version(id, proposal.version, member)
            .unwrap();
        assert!(queue.discard_unaccepted(id, accepted.version).is_err());
        assert!(queue.record(id).unwrap().accepted);
    }
    #[tokio::test]
    async fn q02_f03_stale_unaccepted_actor_cannot_retire_same_id_retry() {
        let (pool, threads) = crate::worker_pool::WorkerPool::start(
            std::num::NonZeroUsize::new(1).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(
            Storage::new(
                PanicOnce::default(),
                Namespace::new("actor-retry/").unwrap(),
            ),
            pool.clone(),
            Default::default(),
        );
        let queue = LocalQueue::new(&engine, Default::default(), false, false).unwrap();
        queue.initialize(&engine).await.unwrap();
        let id = nabedra_rt::ids::uuid_v4();
        let body = serde_json::to_vec(
            &serde_json::json!({"request_id":id,"operation":"table.list","payload":{}}),
        )
        .unwrap();
        let (old, current) = {
            let mut state = queue.state.lock().unwrap();
            let q = state.as_mut().unwrap();
            let old = q.propose(body.clone(), 0).unwrap();
            q.discard_unaccepted(id, old.version).unwrap();
            let retry = q.propose(body, 0).unwrap();
            let current = q
                .acknowledge_version(id, retry.version, queue.member)
                .unwrap();
            (old.sequence, current.sequence)
        };
        queue.active.lock().unwrap().insert(id);
        drop(ActiveLocalRequest {
            owner: queue.clone(),
            id,
            sequence: old,
            _charge: pool.scheduler().charge_memory(0, 1).unwrap(),
        });
        assert!(queue.active.lock().unwrap().contains(&id));
        assert!(
            !queue
                .state
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .record(id)
                .unwrap()
                .state
                .terminal()
        );
        drop(ActiveLocalRequest {
            owner: queue.clone(),
            id,
            sequence: current,
            _charge: pool.scheduler().charge_memory(0, 1).unwrap(),
        });
        assert!(queue.active.lock().unwrap().is_empty());
        assert!(
            queue
                .state
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .record(id)
                .unwrap()
                .state
                .terminal()
        );
        threads.shutdown().await.unwrap();
    }
    async fn local_fixture(
        name: &str,
    ) -> (
        Engine<PanicOnce>,
        Arc<LocalQueue>,
        CatalogFence,
        crate::worker_pool::WorkerThreads,
    ) {
        let (pool, threads) = crate::worker_pool::WorkerPool::start(
            std::num::NonZeroUsize::new(1).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(
            Storage::new(PanicOnce::default(), Namespace::new(name).unwrap()),
            pool,
            Default::default(),
        );
        let queue = LocalQueue::new(&engine, Default::default(), false, false).unwrap();
        let fence = queue.initialize(&engine).await.unwrap();
        (engine, queue, fence, threads)
    }
    #[tokio::test]
    async fn r04_stale_serve_receipt_for_newer_run_is_not_abandoned() {
        let (engine, queue, fence, threads) = local_fixture("stale-serve/").await;
        // An old run's request, while this serve still holds the current fence.
        let old_run = nabedra_rt::ids::uuid_v4();
        let receipt = queue
            .receipt(&engine, old_run, nabedra_rt::ids::uuid_v4())
            .await
            .unwrap();
        assert_eq!(receipt.state, crate::request_queue::RequestState::Abandoned);
        // A newer serve cold-starts and fences the catalog. A client asks this
        // stale process about a live request of the newer run.
        let loaded = engine.storage().load_cluster_catalog().await.unwrap();
        let newer = CatalogFence {
            run_id: nabedra_rt::ids::uuid_v4(),
            scheduler_term: fence.scheduler_term + 1,
        };
        engine
            .storage()
            .fence_cluster_catalog(&loaded, newer, crate::storage::FenceTransition::ColdStart)
            .await
            .unwrap();
        let answer = queue
            .receipt(&engine, newer.run_id, nabedra_rt::ids::uuid_v4())
            .await;
        assert!(
            matches!(&answer, Err(error) if error.outcome == Outcome::Unknown),
            "a fenced serve must not answer abandoned: {answer:?}"
        );
        threads.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn t04_u01_local_unknown_mutation_stays_reconciling() {
        let (engine, queue, fence, threads) = local_fixture("local-unknown/").await;
        let id = nabedra_rt::ids::uuid_v4();
        let body = serde_json::to_vec(&serde_json::json!({"request_id":id,"operation":"table.drop","payload":{"table_id":nabedra_rt::ids::uuid_v4()}})).unwrap();
        // The mutation started and its actor ended before a definite outcome,
        // as after a lost catalog CAS reply.
        let sequence = {
            let mut state = queue.state.lock().unwrap();
            let q = state.as_mut().unwrap();
            let proposal = q.propose(body, 0).unwrap();
            let accepted = q
                .acknowledge_version(id, proposal.version, queue.member)
                .unwrap();
            let running = q.start(id, None).unwrap();
            q.acknowledge_version(id, running.version, queue.member)
                .unwrap();
            accepted.sequence
        };
        queue.active.lock().unwrap().insert(id);
        drop(ActiveLocalRequest {
            owner: queue.clone(),
            id,
            sequence,
            _charge: engine
                .worker_pool()
                .scheduler()
                .charge_memory(0, 1)
                .unwrap(),
        });
        let receipt = queue.receipt(&engine, fence.run_id, id).await.unwrap();
        assert_eq!(
            receipt.state,
            crate::request_queue::RequestState::Reconciling,
            "an unknown mutation must not settle as terminal failed"
        );
        assert!(queue.active.lock().unwrap().is_empty());
        threads.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn q01_f02_f03_actor_unwind_settles_unknown_and_releases_original_memory() {
        nabedra_rt::time::timeout(Duration::from_secs(10), async {
            let (pool, threads) = crate::worker_pool::WorkerPool::start(
                std::num::NonZeroUsize::new(1).unwrap(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let engine = Engine::new(
                Storage::new(
                    PanicOnce::default(),
                    Namespace::new("actor-unwind/").unwrap(),
                ),
                pool.clone(),
                Default::default(),
            );
            let queue = LocalQueue::new(&engine, Default::default(), false, false).unwrap();
            let fence = queue.initialize(&engine).await.unwrap();
            let baseline = pool.scheduler().usage();
            engine
                .storage()
                .backend()
                .panic
                .store(true, Ordering::SeqCst);
            let id = nabedra_rt::ids::uuid_v4();
            let body = serde_json::to_vec(
                &serde_json::json!({"request_id":id,"operation":"table.list","payload":{}}),
            )
            .unwrap();
            let record = queue.submit(engine.clone(), body).await.unwrap();
            assert!(record.accepted);
            queue.executor().drain().await;
            assert!(queue.active.lock().unwrap().is_empty());
            let receipt = queue.receipt(&engine, fence.run_id, id).await.unwrap();
            assert_eq!(receipt.state, crate::request_queue::RequestState::Failed);
            let error = receipt.error.unwrap();
            assert_eq!(error.code, ErrorCode::OutcomeUnknown);
            assert_eq!(error.outcome, Outcome::Unknown);
            assert_eq!(pool.scheduler().usage(), baseline);
            threads.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }
}
