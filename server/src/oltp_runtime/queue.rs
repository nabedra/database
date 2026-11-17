use crate::{
    config::{ExecutionConfig, WorkerResources},
    executor::Executor,
    oltp::OltpEngine,
    protocol::{self, ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueLimits, Receipt, RequestQueue, RequestRecord, RequestState},
    response::{self, EncodedResponse},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use uuid::Uuid;

/// One RF1 worker and its volatile queue. Clones share all admission and receipts.
pub struct LocalRuntime {
    pub(crate) engine: OltpEngine,
    state: Mutex<RequestQueue>,
    member: Uuid,
    changed: Notify,
    accepting: AtomicBool,
    tasks: Executor,
    pub(crate) ingress: Executor,
    pub(crate) scheduler: ResourceScheduler,
    task_bytes: u64,
    _native_charge: MemoryCharge,
    _queue_charge: MemoryCharge,
}

fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "OLTP request outcome requires reconciliation",
        Outcome::Unknown,
    )
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

impl LocalRuntime {
    pub fn new(
        engine: OltpEngine,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<Arc<Self>, ProtocolError> {
        super::require_standalone(&engine)?;
        resources.validate_execution(&execution).map_err(|_| {
            ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid resource configuration",
                Outcome::NotApplied,
            )
        })?;
        let native = engine.resources();
        if native
            .executor_threads
            .checked_add(native.background_threads as usize)
            .is_none_or(|n| n > resources.max_threads as usize)
            || native
                .query_memory_bytes
                .checked_add(super::MAX_DECODED_REQUEST_BYTES + 2 * protocol::MAX_BODY_BYTES)
                .is_none_or(|n| n > resources.task_memory_limit_bytes as usize)
        {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "native execution exceeds worker resource limits",
                Outcome::NotApplied,
            ));
        }
        // Maintenance threads are part of the worker budget. Query scheduling
        // uses only the remaining native executor capacity.
        let scheduler = ResourceScheduler::new(vec![(
            0,
            WorkerResources {
                max_threads: native.executor_threads as u64,
                ..resources
            },
        )])?;
        let reserved = execution
            .max_queue_bytes_per_replica
            .checked_add(execution.max_retained_result_bytes_per_worker)
            .ok_or_else(unavailable)?;
        let native_bytes = native
            .cache_bytes
            .checked_add(native.write_buffer_bytes)
            .and_then(|n| n.checked_add(64 << 20))
            .and_then(|n| n.checked_add(engine.thread_budget().total_stack_reservation_bytes))
            .ok_or_else(unavailable)? as u64;
        let native_charge = scheduler.charge_memory(0, native_bytes)?;
        let charge = scheduler.charge_memory(0, reserved)?;
        // Detached actors retain their body clone and bounded coordination
        // allocation. Leave capacity for two ingress stages and one executing
        // request even when every accepted actor is queued.
        let actors = execution
            .max_pending_requests
            .checked_mul(64 << 10)
            .and_then(|n| n.checked_add(execution.max_queue_bytes_per_replica))
            .ok_or_else(unavailable)?;
        let staging = (3 * protocol::MAX_BODY_BYTES + super::MAX_DECODED_REQUEST_BYTES) as u64;
        if reserved
            .checked_add(native_bytes)
            .and_then(|n| n.checked_add(actors))
            .and_then(|n| n.checked_add(staging.checked_mul(2)?))
            .and_then(|n| n.checked_add(resources.task_memory_limit_bytes))
            .is_none_or(|n| n > resources.working_bytes())
        {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "RAM cannot fit queue, pending actors, ingress and execution",
                Outcome::NotApplied,
            ));
        }
        let member = Uuid::new_v4();
        let queue = RequestQueue::new(
            Uuid::new_v4(),
            1,
            1,
            QueueLimits {
                max_pending_requests: execution.max_pending_requests as usize,
                max_queue_bytes_per_replica: execution.max_queue_bytes_per_replica as usize,
                max_retained_result_bytes_per_worker: execution.max_retained_result_bytes_per_worker
                    as usize,
                result_retention_ms: execution.result_retention_ms,
            },
            [member].into_iter().collect(),
        )
        .map_err(|e| e.protocol())?;
        Ok(Arc::new(Self {
            engine,
            state: Mutex::new(queue),
            member,
            changed: Notify::new(),
            accepting: AtomicBool::new(true),
            tasks: Executor::new(execution.max_pending_requests as usize, 0)?,
            ingress: Executor::new(2, 16)?,
            scheduler,
            task_bytes: resources.task_memory_limit_bytes,
            _native_charge: native_charge,
            _queue_charge: charge,
        }))
    }
    pub fn run_id(&self) -> Uuid {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .run_id()
    }
    pub fn accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        self.ingress.stop_accepting();
        self.tasks.stop_accepting();
    }
    pub async fn drain(&self) {
        self.accepting.store(false, Ordering::Release);
        self.ingress.drain().await;
        self.tasks.drain().await;
    }

    pub async fn submit(self: &Arc<Self>, body: Vec<u8>) -> Result<RequestRecord, ProtocolError> {
        let parsed = protocol::parse_request(&body)?;
        let id = parsed.request_id;
        let mutation = parsed.operation.is_mutation();
        drop(parsed);
        {
            let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if queue.record(id).is_some() {
                return queue.propose(body, now()).map_err(|e| e.protocol());
            }
        }
        if self.engine.requires_recovery() {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "OLTP store requires authoritative reopen before new admission",
                Outcome::NotApplied,
            ));
        }
        // Bound new identity work before native metadata lookup can wait.
        let admission = self.tasks.try_admit()?;
        // Check committed identities before installing a fresh RAM record. An
        // invalid replay must not occupy that UUID and block a later valid one.
        if let Some(decision) = self.engine.lookup_decision(id).await? {
            use sha2::{Digest, Sha256};
            let digest: String = Sha256::digest(&body)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            if decision.digest != digest {
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "request identity has a different body",
                    Outcome::NotApplied,
                ));
            }
        }
        let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if queue.record(id).is_some() {
            return queue.propose(body, now()).map_err(|e| e.protocol());
        }
        let actor_charge = self
            .scheduler
            .charge_memory(
                0,
                body.len().checked_add(64 << 10).ok_or_else(unavailable)? as u64,
            )
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "request actor memory capacity exhausted",
                    Outcome::NotApplied,
                )
            })?;
        let proposal = queue.propose(body, now()).map_err(|e| e.protocol())?;
        let (start, started) = tokio::sync::oneshot::channel();
        let owner = self.clone();
        let record = proposal.clone();
        // Install an owned detached task before acknowledging admission. The
        // start gate keeps it behind the accepted-record transition.
        let spawned = admission.spawn(move || async move {
            let mut settlement = Settlement {
                runtime: owner.clone(),
                id: record.request_id,
                armed: false,
                _actor_charge: actor_charge,
                mutation,
            };
            started.await.map_err(|_| unavailable())?;
            settlement.armed = true;
            let id = record.request_id;
            if let Err(error) = owner.execute(record, mutation).await {
                owner.settle(id, response::encode_error(Some(id), error), mutation)?;
            }
            settlement.armed = false;
            Ok(())
        });
        if let Err(error) = spawned {
            let _ = queue.discard_unaccepted(id, proposal.version);
            return Err(error);
        }
        let accepted = queue
            .acknowledge_version(id, proposal.version, self.member)
            .map_err(|e| e.protocol())?;
        // Keep queue lock through opening the gate, so receipt lookup never sees
        // acceptance before the actor has its execution trigger.
        start.send(()).map_err(|_| unavailable())?;
        Ok(accepted)
    }

    async fn execute(
        self: &Arc<Self>,
        record: RequestRecord,
        mutation: bool,
    ) -> Result<(), ProtocolError> {
        // Await writer ownership and native capacity with only the actor's raw
        // bytes retained. Neither wait owns a scheduler compute allocation.
        let prepared = self
            .engine
            .prepare_identified_body(record.body, record.digest, record.run_id, mutation)
            .await?;
        let reservation = self
            .scheduler
            .reserve(record.sequence, 1, self.task_bytes)
            .await?;
        {
            let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
            // Local reads are never retried/recomputed after result expiry. The
            // engine creates the actual RocksDB snapshot when execution starts;
            // this typed marker does not represent a portable MVCC snapshot.
            let pin = (!mutation).then(|| br#"{"engine":"oltp","version":1,"read":"local_execution_snapshot","retry":false}"#.to_vec());
            let running = queue
                .start(record.request_id, pin)
                .map_err(|e| e.protocol())?;
            queue
                .acknowledge_version(record.request_id, running.version, self.member)
                .map_err(|e| e.protocol())?;
        }
        // Native work owns the reservation even if its awaiting future ends.
        // On return, keep it through response encoding and queue settlement.
        let (result, _reservation) = prepared.execute_with_guard(reservation).await?;
        let encoded = match result {
            Ok(value) => response::encode_success(record.request_id, &value, mutation),
            Err(error) => response::encode_error(Some(record.request_id), error),
        };
        self.settle(record.request_id, encoded, mutation)
    }

    fn reconcile(&self, id: Uuid) -> Result<(), ProtocolError> {
        let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let state = queue.record(id).ok_or_else(unavailable)?.state;
        if state.terminal() || state == RequestState::Reconciling {
            return Ok(());
        }
        let record = queue
            .transition(id, RequestState::Reconciling)
            .map_err(|e| e.protocol())?;
        queue
            .acknowledge_version(id, record.version, self.member)
            .map_err(|e| e.protocol())?;
        self.changed.notify_waiters();
        Ok(())
    }

    fn settle(
        &self,
        id: Uuid,
        encoded: EncodedResponse,
        mutation: bool,
    ) -> Result<(), ProtocolError> {
        let uncertain = mutation
            && encoded.status != 200
            && serde_json::from_slice::<protocol::ErrorResponse>(&encoded.body)
                .is_ok_and(|envelope| envelope.error.outcome == Outcome::Unknown);
        if uncertain {
            self.reconcile(id)
        } else {
            self.finish(id, encoded)
        }
    }

    fn finish(&self, id: Uuid, encoded: EncodedResponse) -> Result<(), ProtocolError> {
        self.finish_inner(id, encoded, false)
    }

    fn finish_inner(
        &self,
        id: Uuid,
        encoded: EncodedResponse,
        durable_proof: bool,
    ) -> Result<(), ProtocolError> {
        let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(record) = queue.record(id).filter(|record| record.state.terminal()) {
            // Concurrent observers reuse the immutable terminal version. An
            // expired mutation result can be returned from durable proof without
            // reinstalling a RAM result or renewing its retention interval.
            if record.response_status == Some(encoded.status)
                && (record.response_body.as_ref() == Some(&encoded.body)
                    || durable_proof && record.result_expired)
            {
                return Ok(());
            }
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "terminal receipt differs from immutable outcome",
                Outcome::NotApplied,
            ));
        }
        let terminal = queue
            .finish(id, encoded.status, encoded.body, now())
            .map_err(|e| e.protocol())?;
        queue
            .acknowledge_version(id, terminal.version, self.member)
            .map_err(|e| e.protocol())?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn receipt(&self, run: Uuid, id: Uuid) -> Result<Receipt, ProtocolError> {
        let local = {
            let mut queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
            queue.expire(now());
            queue.lookup(run, id, now())
        };
        let reconciling =
            matches!(&local, Ok(receipt) if receipt.state == RequestState::Reconciling);
        if let Ok(receipt) = &local
            && !reconciling
        {
            return Ok(receipt.clone());
        }
        // Absence while the native store requires recovery is not evidence of
        // non-commit. Keep accepted reconciling records and their admission count.
        let decision = match self.engine.lookup_decision(id).await {
            Ok(decision) => decision,
            Err(_) if reconciling => return local.map_err(|e| e.protocol()),
            Err(error) => return Err(error),
        };
        if let Some(decision) = decision {
            let binding = {
                let queue = self.state.lock().unwrap_or_else(|e| e.into_inner());
                queue
                    .record(id)
                    .filter(|record| record.run_id == run)
                    .map(|record| record.digest == decision.digest)
            };
            if binding == Some(false) {
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "durable decision differs from accepted request",
                    Outcome::NotApplied,
                ));
            }
            if decision.run_id == run || binding == Some(true) {
                let receipt = Receipt {
                    request_id: id,
                    run_id: run,
                    state: if decision.result.is_ok() {
                        RequestState::Succeeded
                    } else {
                        RequestState::Failed
                    },
                    result: decision
                        .result
                        .as_ref()
                        .ok()
                        .filter(|v| !v.is_null())
                        .cloned(),
                    error: decision.result.as_ref().err().cloned(),
                };
                if reconciling {
                    let encoded = match &decision.result {
                        Ok(value) => response::encode_success(id, value, true),
                        Err(error) => response::encode_error(Some(id), error.clone()),
                    };
                    self.finish_inner(id, encoded, true)?;
                }
                return Ok(receipt);
            }
        }
        local.map_err(|error| error.protocol())
    }

    pub async fn wait(&self, record: &RequestRecord) -> Result<EncodedResponse, ProtocolError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let receipt = tokio::time::timeout(
                super::RECEIPT_OBSERVATION_TIMEOUT,
                self.receipt(record.run_id, record.request_id),
            )
            .await
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "original receipt observation deadline exceeded",
                    Outcome::Unknown,
                )
            })??;
            if receipt.state == RequestState::Reconciling {
                // One fresh durable-proof observation bounds this synchronous
                // transport wait. The accepted request remains in RAM unchanged.
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "accepted mutation is reconciling; observe its original receipt",
                    Outcome::Unknown,
                ));
            }
            if receipt.state.terminal() {
                return Ok(match receipt.error {
                    Some(error) => response::encode_error(Some(record.request_id), error),
                    None => response::encode_success(record.request_id, &receipt.result, false),
                });
            }
            changed.await;
        }
    }
}

struct Settlement {
    runtime: Arc<LocalRuntime>,
    id: Uuid,
    armed: bool,
    _actor_charge: MemoryCharge,
    mutation: bool,
}
impl Drop for Settlement {
    fn drop(&mut self) {
        if self.armed {
            // This outer boundary also runs on unwinding panic. A durable commit
            // remains discoverable by receipt even if response settlement fails.
            let _ = self.runtime.settle(
                self.id,
                response::encode_error(Some(self.id), unavailable()),
                self.mutation,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::OltpResources;
    use serde_json::json;
    use std::path::PathBuf;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("nabedra-runtime-{}", Uuid::new_v4()));
            std::fs::create_dir(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn engine(dir: &Directory) -> OltpEngine {
        OltpEngine::open(
            &dir.0,
            OltpResources {
                cache_bytes: 8 << 20,
                write_buffer_bytes: 4 << 20,
                query_memory_bytes: 16 << 20,
                executor_threads: 1,
                background_threads: 2,
            },
        )
        .unwrap()
    }
    fn runtime(dir: &Directory, pending: u64) -> Arc<LocalRuntime> {
        LocalRuntime::new(
            engine(dir),
            ExecutionConfig {
                max_pending_requests: pending,
                ..Default::default()
            },
            WorkerResources {
                max_threads: 3,
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn create(id: Uuid, name: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({"request_id":id,"operation":"table.create","payload":{"name":name,"schema":{"id":"int64"},"key":"id","format":"parquet"}})).unwrap()
    }

    #[test]
    fn c02_resources_file_cannot_advertise_more_than_cli_executor_capacity() {
        use clap::Parser;
        let data = Directory::new();
        let settings = Directory::new();
        let path = settings.0.join("resources.json");
        for max_threads in [1, 2, 257, 1024] {
            std::fs::write(
                &path,
                serde_json::to_vec(&WorkerResources {
                    max_threads,
                    ..Default::default()
                })
                .unwrap(),
            )
            .unwrap();
            let cli = crate::cli::Cli::try_parse_from([
                "nabedra-db",
                "serve",
                "--data-dir",
                data.0.to_str().unwrap(),
                "--resources-config",
                path.to_str().unwrap(),
            ])
            .unwrap();
            let crate::cli::Command::Serve(options) = cli.command else {
                panic!("serve options expected")
            };
            assert_eq!(
                options.limits().unwrap_err().code,
                ErrorCode::ValidationError
            );
            assert!(std::fs::read_dir(&data.0).unwrap().next().is_none());
        }
    }

    #[tokio::test]
    async fn q02_q05_rf1_bounded_admission_and_duplicate_identity() {
        let dir = Directory::new();
        let runtime = runtime(&dir, 1);
        let gate = runtime.scheduler.reserve(9999, 1, 1).await.unwrap();
        let id = Uuid::new_v4();
        let bytes = create(id, "users");
        let accepted = runtime.submit(bytes.clone()).await.unwrap();
        assert!(accepted.accepted);
        assert_eq!(accepted.state, RequestState::Queued);
        let duplicate = runtime.submit(bytes).await.unwrap();
        assert_eq!(accepted.sequence, duplicate.sequence);
        assert_eq!(runtime.state.lock().unwrap().pending(), 1);
        assert_eq!(
            runtime
                .submit(create(id, "different"))
                .await
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        assert_eq!(
            runtime
                .submit(create(Uuid::new_v4(), "other"))
                .await
                .unwrap_err()
                .code,
            ErrorCode::Overloaded
        );
        assert_eq!(
            runtime.receipt(accepted.run_id, id).await.unwrap().state,
            RequestState::Queued
        );
        drop(gate);
        let terminal =
            tokio::time::timeout(std::time::Duration::from_secs(10), runtime.wait(&accepted))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(terminal.status, 200);
        runtime.drain().await;
        assert_eq!(runtime.state.lock().unwrap().pending(), 0);
        assert_eq!(runtime.scheduler.usage()[0].1, 0);
    }

    #[tokio::test]
    async fn q06_r04_rf1_decisions_survive_run_and_queries_expire() {
        let dir = Directory::new();
        let runtime = runtime(&dir, 8);
        let id = Uuid::new_v4();
        let accepted = runtime.submit(create(id, "users")).await.unwrap();
        let result = runtime.wait(&accepted).await.unwrap();
        let before: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
        // Expire in RAM deterministically. Mutations reconcile from disk.
        runtime.state.lock().unwrap().expire(u64::MAX);
        assert_eq!(
            runtime.receipt(accepted.run_id, id).await.unwrap().result,
            Some(before["data"].clone())
        );
        let query_id = Uuid::new_v4();
        let query = runtime
            .submit(
                serde_json::to_vec(
                    &json!({"request_id":query_id,"operation":"table.list","payload":{}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        runtime.wait(&query).await.unwrap();
        runtime.state.lock().unwrap().expire(u64::MAX);
        assert_eq!(
            runtime
                .receipt(query.run_id, query_id)
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        runtime.drain().await;
        let old_run = runtime.run_id();
        drop(runtime);
        let new_runtime = self::runtime(&dir, 8);
        assert_ne!(old_run, new_runtime.run_id());
        assert_eq!(new_runtime.state.lock().unwrap().pending(), 0);
        let receipt = new_runtime.receipt(old_run, id).await.unwrap();
        assert_eq!(receipt.result, Some(before["data"].clone()));
        assert_eq!(
            new_runtime
                .receipt(old_run, query_id)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(
            new_runtime
                .submit(create(id, "wrong_replay"))
                .await
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        assert!(new_runtime.state.lock().unwrap().record(id).is_none());
        let replay = new_runtime.submit(create(id, "users")).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &new_runtime.wait(&replay).await.unwrap().body
            )
            .unwrap(),
            before
        );
        new_runtime.drain().await;
    }
    #[test]
    fn r04_rf1_queued_work_is_abandoned_after_last_runtime_stops() {
        let dir = Directory::new();
        let first = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (owner, gate, queued) = first.block_on(async {
            let owner = runtime(&dir, 4);
            let gate = owner.scheduler.reserve(9999, 1, 1).await.unwrap();
            let queued = owner
                .submit(create(Uuid::new_v4(), "never_created"))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            assert_eq!(
                owner
                    .receipt(queued.run_id, queued.request_id)
                    .await
                    .unwrap()
                    .state,
                RequestState::Queued
            );
            (owner, gate, queued)
        });
        // Teardown aborts owned pending futures, as a complete RF1 stop does.
        // This is runtime teardown evidence, not an OS/process-kill assertion.
        drop(first);
        drop(gate);
        drop(owner);
        let second = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        second.block_on(async {
            let owner = runtime(&dir, 4);
            assert_ne!(owner.run_id(), queued.run_id);
            assert_eq!(owner.state.lock().unwrap().pending(), 0);
            assert_eq!(
                owner
                    .receipt(queued.run_id, queued.request_id)
                    .await
                    .unwrap_err()
                    .outcome,
                Outcome::Unknown
            );
            let list = owner
                .engine
                .execute(protocol::Operation::List(protocol::EmptyPayload {}))
                .await
                .unwrap();
            assert_eq!(list, json!([]));
            owner.drain().await;
        });
    }
    #[tokio::test]
    async fn replica_engine_cannot_enter_standalone_runtime_or_bind_its_api() {
        let dir = Directory::new();
        let engine = OltpEngine::open_replica(
            &dir.0,
            OltpResources {
                executor_threads: 1,
                background_threads: 2,
                ..Default::default()
            },
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .unwrap();
        assert_eq!(
            super::super::require_standalone(&engine).unwrap_err().code,
            ErrorCode::UnsupportedOperation
        );
        let result = LocalRuntime::new(
            engine.clone(),
            ExecutionConfig::default(),
            WorkerResources::default(),
        );
        assert!(
            matches!(result, Err(error) if error.code == ErrorCode::UnsupportedOperation && error.outcome == Outcome::NotApplied)
        );
        let local_dir = Directory::new();
        let mut owner = runtime(&local_dir, 4);
        // Exercise the router's independent defense by deliberately replacing
        // the private engine inside this test, after normal construction.
        Arc::get_mut(&mut owner).unwrap().engine = engine;
        let router = super::super::router(owner, "token".into(), "test".into());
        assert!(matches!(router, Err(error) if error.code == ErrorCode::UnsupportedOperation));
    }
    #[tokio::test]
    async fn c07_o22_waiting_writers_hold_no_compute_and_read_control_progress() {
        let dir = Directory::new();
        let owner = runtime(&dir, 8);
        let baseline = owner.scheduler.usage()[0];
        let held = owner.engine.writer_ownership().await;
        let first = owner.submit(create(Uuid::new_v4(), "first")).await.unwrap();
        let second = owner
            .submit(create(Uuid::new_v4(), "second"))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while owner.engine.waiting_writers() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(owner.scheduler.usage()[0].1, 0);
        assert_eq!(
            owner
                .receipt(first.run_id, first.request_id)
                .await
                .unwrap()
                .state,
            RequestState::Queued
        );
        let read_id = Uuid::new_v4();
        let read = owner
            .submit(
                serde_json::to_vec(
                    &json!({"request_id":read_id,"operation":"table.list","payload":{}}),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(5), owner.wait(&read))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["data"],
            json!([])
        );
        assert_eq!(owner.engine.waiting_writers(), 2);
        assert_eq!(owner.scheduler.usage()[0].1, 0);
        drop(held);
        for record in [&first, &second] {
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(10), owner.wait(record))
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                200
            );
        }
        owner.drain().await;
        assert_eq!(owner.engine.waiting_writers(), 0);
        assert_eq!(owner.scheduler.usage()[0], baseline);
        assert_eq!(owner.state.lock().unwrap().pending(), 0);
        let names = owner
            .engine
            .execute(protocol::Operation::List(protocol::EmptyPayload {}))
            .await
            .unwrap();
        assert_eq!(
            names
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
    }
    #[tokio::test]
    async fn o29_uncertainty_keeps_admission_body_and_receipt_until_authoritative_reopen() {
        use tower::ServiceExt;
        let dir = Directory::new();
        let owner = runtime(&dir, 2);
        let app = super::super::router(owner.clone(), "token".into(), "test".into()).unwrap();
        let gate = owner.engine.writer_ownership().await;
        let body = create(Uuid::new_v4(), "uncertain");
        let accepted = owner.submit(body.clone()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while owner.engine.waiting_writers() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Model an uncertain native outcome with no currently visible decision.
        // This sets the fail-closed latch, not a filesystem/fsync fault injector.
        owner.engine.test_require_recovery();
        drop(gate);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if owner
                    .state
                    .lock()
                    .unwrap()
                    .record(accepted.request_id)
                    .unwrap()
                    .state
                    == RequestState::Reconciling
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error =
            match tokio::time::timeout(std::time::Duration::from_secs(5), owner.wait(&accepted))
                .await
                .unwrap()
            {
                Ok(_) => panic!("uncertain mutation must not fabricate a terminal response"),
                Err(error) => error,
            };
        assert_eq!(error.code, ErrorCode::OutcomeUnknown);
        assert_eq!(error.outcome, Outcome::Unknown);
        let receipt = owner
            .receipt(accepted.run_id, accepted.request_id)
            .await
            .unwrap();
        assert_eq!(receipt.state, RequestState::Reconciling);
        assert!(receipt.result.is_none() && receipt.error.is_none());
        let duplicate = owner.submit(body.clone()).await.unwrap();
        assert_eq!(
            (duplicate.sequence, duplicate.digest.as_str()),
            (accepted.sequence, accepted.digest.as_str())
        );
        let rejected = owner
            .submit(create(Uuid::new_v4(), "new_work"))
            .await
            .unwrap_err();
        assert_eq!(rejected.code, ErrorCode::StorageUnavailable);
        assert_eq!(rejected.outcome, Outcome::NotApplied);
        {
            let mut queue = owner.state.lock().unwrap();
            queue.expire(u64::MAX);
            assert_eq!(queue.pending(), 1);
            let record = queue.record(accepted.request_id).unwrap();
            assert_eq!(record.state, RequestState::Reconciling);
            assert_eq!(record.body, body);
            assert!(!record.result_expired);
        }
        let ready = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health/ready")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        owner.drain().await;
        drop(owner);
        let reopened = runtime(&dir, 2);
        assert!(!reopened.engine.requires_recovery());
        assert_eq!(reopened.state.lock().unwrap().pending(), 0);
        assert_eq!(
            reopened
                .receipt(accepted.run_id, accepted.request_id)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(
            reopened
                .engine
                .execute(protocol::Operation::List(protocol::EmptyPayload {}))
                .await
                .unwrap(),
            json!([])
        );
        reopened.drain().await;
    }

    #[tokio::test]
    async fn o29_q06_t04_matching_durable_proof_settles_reconciling_receipt_once() {
        let dir = Directory::new();
        let owner = runtime(&dir, 4);
        let id = Uuid::new_v4();
        let body = create(id, "committed");
        let accepted = {
            let mut queue = owner.state.lock().unwrap();
            let proposed = queue.propose(body.clone(), now()).unwrap();
            queue
                .acknowledge_version(id, proposed.version, owner.member)
                .unwrap();
            let running = queue.start(id, None).unwrap();
            queue
                .acknowledge_version(id, running.version, owner.member)
                .unwrap()
        };
        let expected = owner
            .engine
            .execute_identified(
                protocol::parse_request(&body).unwrap(),
                accepted.digest.clone(),
                accepted.run_id,
            )
            .await
            .unwrap();
        // The committed result was lost before terminal queue settlement.
        owner.reconcile(id).unwrap();
        let before = owner.state.lock().unwrap().record(id).unwrap().clone();
        let (first, second) = tokio::join!(
            owner.receipt(accepted.run_id, id),
            owner.receipt(accepted.run_id, id)
        );
        for receipt in [first.unwrap(), second.unwrap()] {
            assert_eq!(receipt.state, RequestState::Succeeded);
            assert_eq!(receipt.result, Some(expected.clone()));
        }
        let after = owner.state.lock().unwrap().record(id).unwrap().clone();
        assert_eq!(after.version, before.version + 1);
        assert_eq!(after.context, before.context);
        assert_eq!(after.digest, before.digest);
        assert_eq!(owner.state.lock().unwrap().pending(), 0);
        assert_eq!(
            owner
                .finish(
                    id,
                    response::encode_success(id, &json!({"wrong":true}), true)
                )
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        owner.state.lock().unwrap().expire(u64::MAX);
        assert_eq!(
            owner.receipt(accepted.run_id, id).await.unwrap().result,
            Some(expected)
        );
        owner.drain().await;
    }
}
