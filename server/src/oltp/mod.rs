//! Experimental, local RocksDB OLTP execution. This is a standalone RF1 store.
//! It provides atomic synchronous local writes and snapshot reads. It does not
//! implement Raft, distributed timestamps, cross-pool commits or worker failover.
mod codec;
mod native;
#[cfg(test)]
mod migration_acceptance;
pub use native::NativeThreadBudget;
pub(crate) mod backup;
pub mod mvcc;
pub mod pool;
mod query;
mod snapshot;
mod state_machine;
pub use snapshot::{SnapshotHeader, StoreSnapshot};
pub use state_machine::{StateMachine, StateSnapshotBuilder};

use crate::protocol::{
    self, ErrorCode, Operation, Outcome, ProtocolError, Request, Row, TableDescriptor,
};
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, DBRecoveryMode, Direction, Env, IteratorMode,
    Options, ReadOptions, Snapshot, WriteBatch, WriteBufferManager, WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    },
};
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const MANIFEST_FILE: &str = "NABEDRA_OLTP.json";
const DATABASE_KEY: &[u8] = b"\x00b";
// RocksDB's default Env is process-global even when its Rust wrapper is new.
// Configure it once; another store may share but cannot increase this budget.
static NATIVE_ENV: Mutex<Option<(i32, Env)>> = Mutex::new(None);

#[derive(Debug, Clone, Copy)]
pub struct OltpResources {
    pub cache_bytes: usize,
    pub write_buffer_bytes: usize,
    pub query_memory_bytes: usize,
    pub executor_threads: usize,
    pub background_threads: i32,
}
impl Default for OltpResources {
    fn default() -> Self {
        Self {
            cache_bytes: 16 * 1024 * 1024,
            write_buffer_bytes: 16 * 1024 * 1024,
            query_memory_bytes: 64 * 1024 * 1024,
            executor_threads: 2,
            background_threads: 2,
        }
    }
}
impl OltpResources {
    fn validate(self) -> Result<Self, ProtocolError> {
        if self.cache_bytes < 1024 * 1024
            || self.write_buffer_bytes < 1024 * 1024
            || self.query_memory_bytes < 1024
            || self.executor_threads == 0
            || self.executor_threads > 1024
            || !(2..=128).contains(&self.background_threads)
            || self
                .cache_bytes
                .checked_add(self.write_buffer_bytes)
                .is_none()
            || self
                .query_memory_bytes
                .checked_mul(self.executor_threads)
                .is_none()
        {
            return Err(failure(
                ErrorCode::ValidationError,
                "invalid OLTP memory or thread resources",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreManifest {
    pub engine: String,
    pub format_version: u32,
    pub topology: String,
    pub cluster_id: Uuid,
    pub pool_id: Uuid,
    pub group_id: Option<Uuid>,
    pub database: Option<String>,
}
impl StoreManifest {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.engine != "oltp"
            || !matches!(
                (
                    self.format_version,
                    self.topology.as_str(),
                    self.group_id,
                    self.database.as_deref()
                ),
                (1, "standalone-rf1", None, None)
                    | (2, "replicated-one-group-v2", Some(_), Some(_))
            )
            || self.cluster_id.is_nil()
            || self.pool_id.is_nil()
            || self.group_id.is_some_and(|id| id.is_nil())
            || self
                .database
                .as_deref()
                .is_some_and(|name| !valid_database(name))
        {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "foreign or unsupported OLTP store format",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableDecision {
    pub digest: String,
    pub result: Result<Value, ProtocolError>,
    pub run_id: Uuid,
}

struct Inner {
    db: Arc<DB>,
    write_lock: Mutex<()>,
    writer_owner: Arc<tokio::sync::Mutex<()>>,
    waiting_writers: AtomicUsize,
    active_executions: AtomicUsize,
    recovery: Arc<AtomicBool>,
    resources: OltpResources,
    thread_budget: NativeThreadBudget,
    slots: Arc<Semaphore>,
    manifest: StoreManifest,
}

#[derive(Clone)]
pub struct OltpEngine {
    inner: Arc<Inner>,
}

enum PreparedInput {
    Typed {
        operation: Operation,
        identity: Option<(Uuid, String, Uuid)>,
    },
    Body {
        body: Vec<u8>,
        digest: String,
        run_id: Uuid,
        mutation: bool,
    },
}

/// Execution ownership is acquired before the runtime grants a compute slot.
/// Waiting retains raw request bytes, rather than a decoded table-sized buffer.
pub struct PreparedExecution {
    engine: OltpEngine,
    input: PreparedInput,
    owner: Option<OwnedMutexGuard<()>>,
    permit: OwnedSemaphorePermit,
}
impl PreparedExecution {
    pub async fn execute(self) -> Result<Value, ProtocolError> {
        self.execute_with_guard(()).await?.0
    }

    /// Move admission charges into native work so cancellation cannot release
    /// them while a blocking task still owns buffers or a durable write.
    pub async fn execute_with_guard<G: Send + 'static>(
        self,
        guard: G,
    ) -> Result<(Result<Value, ProtocolError>, G), ProtocolError> {
        let Self {
            engine,
            input,
            owner,
            permit,
        } = self;
        let worker = engine.clone();
        engine
            .blocking_reserved(permit, move || {
                let _owner = owner;
                let result=(||{
                let (operation, identity) = match input {
                    PreparedInput::Typed {
                        operation,
                        identity,
                    } => (operation, identity),
                    PreparedInput::Body {
                        body,
                        digest,
                        run_id,
                        mutation,
                    } => {
                        let request = protocol::parse_request(&body)?;
                        if request.operation.is_mutation() != mutation {
                            return Err(failure(
                                ErrorCode::ValidationError,
                                "prepared request mutation classification does not match its body",
                            ));
                        }
                        let actual = Sha256::digest(&body)
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>();
                        if actual != digest {
                            return Err(failure(
                                ErrorCode::Conflict,
                                "prepared request digest does not match its body",
                            ));
                        }
                        (
                            request.operation,
                            Some((request.request_id, digest, run_id)),
                        )
                    }
                };
                worker.execute_sync(operation, identity)
                })();
                Ok((result,guard))
            })
            .await
    }
}

impl OltpEngine {
    /// Open only an empty directory or an explicitly identified OLTP store.
    /// RocksDB owns the directory lock. Existing foreign files are never adopted.
    pub fn open(path: impl AsRef<Path>, resources: OltpResources) -> Result<Self, ProtocolError> {
        Self::open_with_identity(path.as_ref(), resources, None)
    }

    /// Experimental single-group replicated storage. Existing RF1 directories
    /// are deliberately incompatible and require an explicit migration.
    pub fn open_replica(
        path: impl AsRef<Path>,
        resources: OltpResources,
        cluster_id: Uuid,
        group_id: Uuid,
    ) -> Result<Self, ProtocolError> {
        Self::open_replica_with_database(path, resources, cluster_id, group_id, "default")
    }

    /// Replica database scope is immutable enrollment identity. Production
    /// bootstrap must supply it explicitly and agree across group members.
    pub fn open_replica_with_database(
        path: impl AsRef<Path>,
        resources: OltpResources,
        cluster_id: Uuid,
        group_id: Uuid,
        database: &str,
    ) -> Result<Self, ProtocolError> {
        if cluster_id.is_nil() || group_id.is_nil() {
            return Err(failure(
                ErrorCode::ValidationError,
                "replicated identities cannot be nil",
            ));
        }
        if !valid_database(database) {
            return Err(failure(
                ErrorCode::ValidationError,
                "invalid replica database identity",
            ));
        }
        Self::open_with_identity(
            path.as_ref(),
            resources,
            Some((cluster_id, group_id, database)),
        )
    }

    fn open_with_identity(
        path: &Path,
        resources: OltpResources,
        identity: Option<(Uuid, Uuid, &str)>,
    ) -> Result<Self, ProtocolError> {
        let resources = resources.validate()?;
        let thread_budget =
            NativeThreadBudget::new(resources.executor_threads, 0, resources.background_threads)?;
        let manifest = open_manifest(path, identity)?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
        options.set_write_buffer_size(resources.write_buffer_bytes / 2);
        options.set_max_write_buffer_number(2);
        configure_native_jobs(&mut options, resources.background_threads);
        options.set_max_open_files(128);
        options.set_max_total_wal_size(resources.write_buffer_bytes as u64);
        let native_env = native_environment(resources.background_threads)?;
        options.set_env(&native_env);
        let write_buffers =
            WriteBufferManager::new_write_buffer_manager(resources.write_buffer_bytes, true);
        options.set_write_buffer_manager(&write_buffers);
        let cache = Cache::new_lru_cache(resources.cache_bytes);
        let mut blocks = BlockBasedOptions::default();
        blocks.set_block_cache(&cache);
        blocks.set_cache_index_and_filter_blocks(true);
        blocks.set_bloom_filter(10.0, false);
        options.set_block_based_table_factory(&blocks);
        let db = DB::open(&options, path).map_err(storage_error)?;
        let bytes = encode(&manifest)?;
        match db.get(codec::MANIFEST).map_err(storage_error)? {
            Some(existing)
                if serde_json::from_slice::<StoreManifest>(&existing)
                    .ok()
                    .as_ref()
                    == Some(&manifest) => {}
            Some(_) => {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "disk and RocksDB identities disagree",
                ));
            }
            None => {
                if db.iterator(IteratorMode::Start).next().is_some() {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "unidentified nonempty RocksDB store",
                    ));
                }
                let mut batch = WriteBatch::default();
                batch.put(codec::MANIFEST, bytes);
                batch.put(codec::ACTIVE, 0_u64.to_be_bytes());
                db.write_opt(batch, &sync_options())
                    .map_err(storage_error)?;
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                db: Arc::new(db),
                write_lock: Mutex::new(()),
                writer_owner: Arc::new(tokio::sync::Mutex::new(())),
                waiting_writers: AtomicUsize::new(0),
                active_executions: AtomicUsize::new(0),
                recovery: Arc::new(AtomicBool::new(false)),
                resources,
                thread_budget,
                slots: Arc::new(Semaphore::new(resources.executor_threads)),
                manifest,
            }),
        })
    }

    pub fn manifest(&self) -> &StoreManifest {
        &self.inner.manifest
    }
    pub fn resources(&self) -> OltpResources {
        self.inner.resources
    }
    pub fn thread_budget(&self) -> NativeThreadBudget {
        self.inner.thread_budget
    }

    pub(crate) fn shared_db(&self) -> Arc<DB> {
        self.inner.db.clone()
    }

    pub fn requires_recovery(&self) -> bool {
        self.inner.recovery.load(AtomicOrdering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn test_require_recovery(&self) {
        self.inner.recovery.store(true, AtomicOrdering::Release);
    }
    pub(crate) fn recovery_marker(&self) -> Arc<AtomicBool> {
        self.inner.recovery.clone()
    }

    fn durable_write(&self, batch: WriteBatch) -> Result<(), ProtocolError> {
        if self.requires_recovery() {
            return Err(recovery_unknown());
        }
        self.inner
            .db
            .write_opt(batch, &sync_options())
            .map_err(|_| {
                // WAL sync can precede live memtable insertion. No live absence
                // observation after this error authorizes recomputing an operation.
                self.inner.recovery.store(true, AtomicOrdering::Release);
                recovery_unknown()
            })
    }

    fn observed_decision(
        &self,
        bytes: Option<Vec<u8>>,
    ) -> Result<Option<DurableDecision>, ProtocolError> {
        match bytes {
            Some(bytes) => decode_decision(&bytes).map(Some),
            None if self.requires_recovery() => Err(recovery_unknown()),
            None => Ok(None),
        }
    }

    fn writer(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.inner.write_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // This mutex holds only (), not a mutable candidate. A panic
                // drops its private WriteBatch; the atomic database remains the
                // authority, including an ambiguously completed durable write.
                self.inner.write_lock.clear_poison();
                poisoned.into_inner()
            }
        }
    }

    pub fn waiting_writers(&self) -> usize {
        self.inner.waiting_writers.load(AtomicOrdering::Acquire)
    }

    pub fn active_executions(&self) -> usize {
        self.inner.active_executions.load(AtomicOrdering::Acquire)
    }

    pub(super) async fn writer_ownership(&self) -> OwnedMutexGuard<()> {
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, AtomicOrdering::AcqRel);
            }
        }
        self.inner
            .waiting_writers
            .fetch_add(1, AtomicOrdering::AcqRel);
        let waiting = Waiting(&self.inner.waiting_writers);
        let owner = self.inner.writer_owner.clone().lock_owned().await;
        drop(waiting);
        owner
    }

    /// Startup binds one data directory to one database selector. Credential
    /// authorization belongs to the API; rotating a bearer does not change data.
    pub fn bind_database(&self, name: &str) -> Result<(), ProtocolError> {
        if !valid_database(name) {
            return Err(failure(
                ErrorCode::ValidationError,
                "invalid database selector",
            ));
        }
        if self.manifest().group_id.is_some() {
            return if self.manifest().database.as_deref() == Some(name) {
                Ok(())
            } else {
                Err(failure(
                    ErrorCode::Conflict,
                    "replica database identity is immutable",
                ))
            };
        }
        let _owner = self
            .inner
            .writer_owner
            .clone()
            .try_lock_owned()
            .map_err(|_| {
                failure(
                    ErrorCode::Overloaded,
                    "bind database only while local mutation execution is idle",
                )
            })?;
        let _guard = self.writer();
        match self.inner.db.get(DATABASE_KEY).map_err(storage_error)? {
            Some(value) if value.as_slice() == name.as_bytes() => Ok(()),
            Some(_) => Err(failure(
                ErrorCode::Conflict,
                "data directory belongs to another database",
            )),
            None => {
                let mut batch = WriteBatch::default();
                batch.put(DATABASE_KEY, name.as_bytes());
                self.durable_write(batch)
            }
        }
    }

    pub async fn execute(&self, operation: Operation) -> Result<Value, ProtocolError> {
        let mutation = operation.is_mutation();
        self.prepare_input(
            PreparedInput::Typed {
                operation,
                identity: None,
            },
            mutation,
        )
        .await?
        .execute()
        .await
    }

    /// The caller supplies the SHA-256 of the exact accepted body and original
    /// run. No application request queue is read from or written to this store.
    pub async fn execute_identified(
        &self,
        request: Request,
        digest: String,
        run_id: Uuid,
    ) -> Result<Value, ProtocolError> {
        self.prepare_execution(request, digest, run_id)
            .await?
            .execute()
            .await
    }

    pub async fn prepare_execution(
        &self,
        request: Request,
        digest: String,
        run_id: Uuid,
    ) -> Result<PreparedExecution, ProtocolError> {
        validate_identity(&digest, run_id)?;
        let mutation = request.operation.is_mutation();
        self.prepare_input(
            PreparedInput::Typed {
                operation: request.operation,
                identity: Some((request.request_id, digest, run_id)),
            },
            mutation,
        )
        .await
    }

    pub async fn prepare_identified_body(
        &self,
        body: Vec<u8>,
        digest: String,
        run_id: Uuid,
        mutation: bool,
    ) -> Result<PreparedExecution, ProtocolError> {
        validate_identity(&digest, run_id)?;
        if body.len() > protocol::MAX_BODY_BYTES {
            return Err(failure(
                ErrorCode::LimitExceeded,
                "request body exceeds 8 MiB",
            ));
        }
        self.prepare_input(
            PreparedInput::Body {
                body,
                digest,
                run_id,
                mutation,
            },
            mutation,
        )
        .await
    }

    async fn prepare_input(
        &self,
        input: PreparedInput,
        mutation: bool,
    ) -> Result<PreparedExecution, ProtocolError> {
        if self.manifest().group_id.is_some() {
            return Err(failure(
                ErrorCode::UnsupportedOperation,
                "replicated storage requires consensus execution and read authority",
            ));
        }
        let owner = if mutation {
            Some(self.writer_ownership().await)
        } else {
            None
        };
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| failure(ErrorCode::StorageUnavailable, "OLTP executor stopped"))?;
        Ok(PreparedExecution {
            engine: self.clone(),
            input,
            owner,
            permit,
        })
    }

    pub async fn lookup_decision(
        &self,
        id: Uuid,
    ) -> Result<Option<DurableDecision>, ProtocolError> {
        let engine = self.clone();
        self.blocking(move || {
            let snapshot = engine.inner.db.snapshot();
            let namespace = active_namespace(&snapshot).map_err(|_| decision_unknown())?;
            let bytes = snapshot
                .get(namespace.decision(id))
                .map_err(|_| decision_unknown())?;
            engine.observed_decision(bytes)
        })
        .await
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| failure(ErrorCode::StorageUnavailable, "OLTP executor stopped"))?;
        self.blocking_reserved(permit, work).await
    }

    async fn blocking_reserved<T: Send + 'static>(
        &self,
        permit: OwnedSemaphorePermit,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let activity = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            // Ownership remains in the closure when a client future is dropped.
            let _permit = permit;
            struct Active(Arc<Inner>);
            impl Drop for Active {
                fn drop(&mut self) {
                    self.0
                        .active_executions
                        .fetch_sub(1, AtomicOrdering::AcqRel);
                }
            }
            activity
                .active_executions
                .fetch_add(1, AtomicOrdering::AcqRel);
            let _active = Active(activity);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).unwrap_or_else(|_| {
                Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "OLTP execution panicked; reconcile its durable decision",
                    Outcome::Unknown,
                ))
            })
        })
        .await
        .map_err(|_| {
            ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "OLTP executor task failed; reconcile its durable decision",
                Outcome::Unknown,
            )
        })?
    }

    fn execute_sync(
        &self,
        operation: Operation,
        identity: Option<(Uuid, String, Uuid)>,
    ) -> Result<Value, ProtocolError> {
        if self.inner.manifest.group_id.is_some() {
            return Err(failure(
                ErrorCode::UnsupportedOperation,
                "replicated storage requires consensus execution and read authority",
            ));
        }
        if !operation.is_mutation() {
            if self.requires_recovery() {
                return Err(recovery_unavailable());
            }
            operation.validate()?;
            query::check_body(&operation, protocol::MAX_BODY_BYTES)?;
            return self.read(&self.inner.db.snapshot(), &operation);
        }
        let _guard = self.writer();
        let snapshot = self.inner.db.snapshot();
        let namespace = active_namespace(&snapshot).map_err(|error| {
            if identity.is_some() {
                decision_unknown()
            } else {
                error
            }
        })?;
        if let Some((id, digest, _)) = &identity
            && let Some(bytes) = self
                .inner
                .db
                .get(namespace.decision(*id))
                .map_err(|_| decision_unknown())?
        {
            let decision = decode_decision(&bytes)?;
            return if &decision.digest == digest {
                decision.result
            } else {
                Err(failure(
                    ErrorCode::Conflict,
                    "request identity has a different body",
                ))
            };
        }
        if self.requires_recovery() {
            return Err(recovery_unknown());
        }
        let mut batch = WriteBatch::default();
        let result = validate_operation(&snapshot, namespace, &operation)
            .and_then(|()| {
                let input = query::check_body(&operation, protocol::MAX_BODY_BYTES)?
                    .checked_mul(8)
                    .ok_or_else(|| {
                        failure(ErrorCode::LimitExceeded, "request working memory overflow")
                    })?;
                let available = self
                    .inner
                    .resources
                    .query_memory_bytes
                    .checked_sub(input)
                    .filter(|n| *n >= 4096)
                    .ok_or_else(|| {
                        failure(
                            ErrorCode::LimitExceeded,
                            "request exceeds task working memory",
                        )
                    })?;
                self.prepare(&snapshot, &operation, &mut batch, available, None)
            })
            .and_then(|value| {
                query::check_body(&value, protocol::MAX_BODY_BYTES - 128)?;
                Ok(value)
            });
        // A storage/allocator failure is not a stable business result. It must
        // remain retryable. Deterministic conflicts/no-ops retain exact outcomes.
        if let Err(error) = &result {
            batch = WriteBatch::default();
            if !matches!(
                error.code,
                ErrorCode::ValidationError
                    | ErrorCode::NotFound
                    | ErrorCode::Conflict
                    | ErrorCode::LimitExceeded
                    | ErrorCode::UnsupportedOperation
            ) {
                return Err(error.clone());
            }
        }
        if let Some((id, digest, run_id)) = identity {
            let decision = DurableDecision {
                digest,
                result: result.clone(),
                run_id,
            };
            batch.put(namespace.decision(id), encode(&decision)?);
        }
        // Data, schema changes, and the exact terminal result are one WAL batch.
        // Any write error is uncertain, even when it probably preceded fsync.
        if !batch.is_empty() {
            self.durable_write(batch)?;
        }
        result
    }

    fn read(&self, snapshot: &Snapshot<'_>, operation: &Operation) -> Result<Value, ProtocolError> {
        if self.requires_recovery() {
            return Err(recovery_unavailable());
        }
        let input = query::check_body(operation, protocol::MAX_BODY_BYTES)?
            .checked_mul(8)
            .ok_or_else(|| {
                failure(
                    ErrorCode::LimitExceeded,
                    "read request working memory overflow",
                )
            })?;
        let budget = self
            .inner
            .resources
            .query_memory_bytes
            .checked_sub(input)
            .filter(|available| *available >= 4096)
            .ok_or_else(|| {
                failure(
                    ErrorCode::LimitExceeded,
                    "read request exceeds task working memory",
                )
            })?;
        let namespace = active_namespace(snapshot)?;
        match operation {
            Operation::Describe(payload) => {
                let id = snapshot
                    .get(namespace.name(&payload.name))
                    .map_err(storage_error)?
                    .ok_or_else(|| failure(ErrorCode::NotFound, "table not found"))?;
                let id = Uuid::from_slice(&id).map_err(|_| {
                    failure(ErrorCode::StorageUnavailable, "invalid table name index")
                })?;
                to_value(&resolve(snapshot, namespace, id)?)
            }
            Operation::List(_) => {
                let mut result = Vec::new();
                let mut options = ReadOptions::default();
                options.set_iterate_upper_bound(codec::end(&namespace.category(b'n')));
                options.fill_cache(false);
                let mut bytes = 0_usize;
                for entry in snapshot.iterator_opt(
                    IteratorMode::From(&namespace.category(b'n'), Direction::Forward),
                    options,
                ) {
                    let (_, id) = entry.map_err(storage_error)?;
                    let id = Uuid::from_slice(&id).map_err(|_| {
                        failure(ErrorCode::StorageUnavailable, "invalid table index")
                    })?;
                    let table = resolve(snapshot, namespace, id)?;
                    bytes = bytes
                        .checked_add(query::check_body(&table, protocol::MAX_BODY_BYTES)? * 8)
                        .ok_or_else(|| {
                            failure(
                                ErrorCode::LimitExceeded,
                                "table listing working memory overflow",
                            )
                        })?;
                    if bytes > budget {
                        return Err(failure(
                            ErrorCode::LimitExceeded,
                            "table listing exceeds task working memory",
                        ));
                    }
                    result.push(table);
                }
                query::check_body(&result, protocol::MAX_BODY_BYTES - 128)?;
                to_value(&result)
            }
            Operation::Get(payload) => {
                let table = resolve(snapshot, namespace, payload.table_id)?;
                operation.validate_for_table(&table)?;
                let row = snapshot
                    .get(namespace.row(table.id, &payload.key))
                    .map_err(storage_error)?
                    .map(|bytes| query::decode_row(&table, &bytes, budget))
                    .transpose()?;
                query::check_body(&row, protocol::MAX_BODY_BYTES - 128)?;
                to_value(&row)
            }
            Operation::Collect(payload)
            | Operation::Count(payload)
            | Operation::Explain(payload) => {
                let table = resolve(snapshot, namespace, payload.table_id)?;
                operation.validate_for_table(&table)?;
                match operation {
                    Operation::Collect(_) => {
                        query::collect(snapshot, namespace, &table, &payload.query, budget)
                    }
                    Operation::Count(_) => {
                        query::count(snapshot, namespace, &table, &payload.query, budget)
                    }
                    _ => Ok(query::explain(&table, &payload.query)),
                }
            }
            _ => Err(failure(
                ErrorCode::UnsupportedOperation,
                "expected read operation",
            )),
        }
    }

    fn prepare(
        &self,
        snapshot: &Snapshot<'_>,
        operation: &Operation,
        batch: &mut WriteBatch,
        budget: usize,
        create_table_id: Option<Uuid>,
    ) -> Result<Value, ProtocolError> {
        let namespace = active_namespace(snapshot)?;
        if let Operation::Create(payload) = operation {
            if snapshot
                .get(namespace.name(&payload.name))
                .map_err(storage_error)?
                .is_some()
            {
                return Err(failure(ErrorCode::Conflict, "table name already exists"));
            }
            let table = TableDescriptor {
                id: create_table_id.unwrap_or_else(Uuid::new_v4),
                name: payload.name.clone(),
                schema: payload.schema.clone(),
                key: payload.key.clone(),
                format: payload.format,
            };
            batch.put(namespace.descriptor(table.id), encode(&table)?);
            batch.put(namespace.name(&table.name), table.id.as_bytes());
            return to_value(&table);
        }
        let table_id = match operation {
            Operation::Drop(p) => p.table_id,
            Operation::Insert(p) | Operation::Upsert(p) => p.table_id,
            Operation::InsertMany(p) => p.table_id,
            Operation::Update(p) => p.table_id,
            Operation::Delete(p) => p.table_id,
            _ => {
                return Err(failure(
                    ErrorCode::UnsupportedOperation,
                    "expected mutation operation",
                ));
            }
        };
        let table = resolve(snapshot, namespace, table_id)?;
        operation.validate_for_table(&table)?;
        match operation {
            Operation::Drop(_) => {
                batch.delete(namespace.descriptor(table.id));
                batch.delete(namespace.name(&table.name));
                let prefix = namespace.rows(table.id);
                batch.delete_range(prefix.clone(), codec::end(&prefix));
                Ok(Value::Null)
            }
            Operation::Insert(p) | Operation::Upsert(p) => {
                let charge = self.row_write_charge(&table, &p.row)?;
                check_memory(charge, budget)?;
                let key = namespace.row(table.id, &codec::scalar_key(&p.row[&table.key]));
                if matches!(operation, Operation::Insert(_))
                    && snapshot.get(&key).map_err(storage_error)?.is_some()
                {
                    return Err(failure(ErrorCode::Conflict, "primary key already exists"));
                }
                batch.put(key, encode(&p.row)?);
                Ok(Value::Null)
            }
            Operation::InsertMany(p) => {
                let mut charge = 0_usize;
                // Count all native-batch bytes, transient index keys and map
                // state before allocating a key set or growing WriteBatch.
                for row in &p.rows {
                    charge = charge
                        .checked_add(self.row_write_charge(&table, row)?)
                        .ok_or_else(|| {
                            failure(ErrorCode::LimitExceeded, "batch working memory overflow")
                        })?;
                    check_memory(charge, budget)?;
                }
                let mut keys = BTreeSet::new();
                for row in &p.rows {
                    let key = namespace.row(table.id, &codec::scalar_key(&row[&table.key]));
                    if !keys.insert(key.clone())
                        || snapshot.get(&key).map_err(storage_error)?.is_some()
                    {
                        return Err(failure(ErrorCode::Conflict, "duplicate primary key"));
                    }
                    batch.put(key, encode(row)?);
                }
                Ok(Value::from(p.rows.len() as u64))
            }
            Operation::Update(p) => {
                let key = namespace.row(table.id, &p.key);
                match snapshot.get(&key).map_err(storage_error)? {
                    Some(bytes) => {
                        let values_len = query::check_body(&p.values, protocol::MAX_BODY_BYTES)?;
                        let combined = bytes.len().checked_add(values_len).ok_or_else(|| {
                            failure(ErrorCode::LimitExceeded, "update working memory overflow")
                        })?;
                        // Reserve the decoded old row, cloned replacements,
                        // new serialization and WriteBatch before either grows.
                        check_memory(
                            query::row_charge(&table, combined)?.saturating_mul(2),
                            budget,
                        )?;
                        let mut row = query::decode_row(&table, &bytes, budget / 2)?;
                        row.extend(p.values.clone());
                        check_memory(self.row_write_charge(&table, &row)?, budget / 2)?;
                        batch.put(key, encode(&row)?);
                        Ok(Value::Bool(true))
                    }
                    None => Ok(Value::Bool(false)),
                }
            }
            Operation::Delete(p) => {
                let key = namespace.row(table.id, &p.key);
                let found = snapshot.get(&key).map_err(storage_error)?.is_some();
                if found {
                    batch.delete(key);
                }
                Ok(Value::Bool(found))
            }
            _ => unreachable!("mutation dispatch"),
        }
    }

    fn row_write_charge(&self, table: &TableDescriptor, row: &Row) -> Result<usize, ProtocolError> {
        let len = query::check_body(row, protocol::MAX_BODY_BYTES)?;
        let key_len = match &row[&table.key] {
            protocol::Scalar::Utf8(value) => value
                .len()
                .checked_mul(2)
                .and_then(|len| len.checked_add(28)),
            _ => Some(35),
        }
        .ok_or_else(|| {
            failure(
                ErrorCode::LimitExceeded,
                "index key working memory overflow",
            )
        })?;
        let charge = query::row_charge(table, len)?
            .checked_add(key_len.checked_mul(4).ok_or_else(|| {
                failure(
                    ErrorCode::LimitExceeded,
                    "index key working memory overflow",
                )
            })?)
            .ok_or_else(|| failure(ErrorCode::LimitExceeded, "row working memory overflow"))?;
        if charge > self.inner.resources.query_memory_bytes {
            return Err(failure(
                ErrorCode::LimitExceeded,
                "row exceeds task working memory",
            ));
        }
        Ok(charge)
    }
}

fn check_memory(charge: usize, budget: usize) -> Result<(), ProtocolError> {
    if charge > budget {
        Err(failure(
            ErrorCode::LimitExceeded,
            "operation exceeds task working memory",
        ))
    } else {
        Ok(())
    }
}

// The pinned native GetBGJobLimits forces HIGH>=1 and LOW>=1. Explicit
// compatibility limits are required to keep HIGH1 when the total exceeds4.
#[allow(deprecated)]
fn configure_native_jobs(options: &mut Options, threads: i32) {
    options.set_max_background_jobs(threads);
    options.set_max_background_flushes(1);
    options.set_max_background_compactions(threads - 1);
    options.set_max_subcompactions(1);
}

fn native_environment(threads: i32) -> Result<Env, ProtocolError> {
    let mut shared = NATIVE_ENV.lock().map_err(|_| {
        failure(
            ErrorCode::StorageUnavailable,
            "native environment lock unavailable",
        )
    })?;
    if let Some((configured, env)) = &*shared {
        if *configured != threads {
            return Err(failure(
                ErrorCode::ValidationError,
                "all local pools must share the existing native background thread budget",
            ));
        }
        return Ok(env.clone());
    }
    let mut env = Env::new().map_err(storage_error)?;
    env.set_high_priority_background_threads(1);
    env.set_bottom_priority_background_threads(0);
    env.set_low_priority_background_threads(threads - 1);
    *shared = Some((threads, env.clone()));
    Ok(env)
}

fn resolve(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
    id: Uuid,
) -> Result<TableDescriptor, ProtocolError> {
    let bytes = snapshot
        .get(namespace.descriptor(id))
        .map_err(storage_error)?
        .ok_or_else(|| failure(ErrorCode::NotFound, "table not found or stale table handle"))?;
    let table: TableDescriptor = serde_json::from_slice(&bytes)
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid table descriptor"))?;
    table
        .validate()
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid stored schema"))?;
    if table.id != id {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "table identity mismatch",
        ));
    }
    Ok(table)
}

fn decode_decision(bytes: &[u8]) -> Result<DurableDecision, ProtocolError> {
    serde_json::from_slice(bytes).map_err(|_| decision_unknown())
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, ProtocolError> {
    serde_json::to_vec(value)
        .map_err(|_| failure(ErrorCode::InternalError, "OLTP value encoding failed"))
}
fn to_value(value: &impl Serialize) -> Result<Value, ProtocolError> {
    serde_json::to_value(value)
        .map_err(|_| failure(ErrorCode::InternalError, "OLTP result encoding failed"))
}
fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    options
}
pub(crate) fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
pub(crate) fn storage_error(_: rocksdb::Error) -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "OLTP storage unavailable or corrupt",
    )
}

fn open_manifest(
    path: &Path,
    identity: Option<(Uuid, Uuid, &str)>,
) -> Result<StoreManifest, ProtocolError> {
    let io_error = |_| {
        failure(
            ErrorCode::StorageUnavailable,
            "cannot access OLTP data directory",
        )
    };
    if path.exists() {
        let metadata = fs::symlink_metadata(path).map_err(io_error)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "OLTP data path must be a directory",
            ));
        }
    } else {
        fs::create_dir_all(path).map_err(io_error)?;
    }
    let manifest_path = path.join(MANIFEST_FILE);
    if manifest_path.exists() {
        let metadata = fs::symlink_metadata(&manifest_path).map_err(io_error)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > 4096 {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "invalid OLTP disk identity file",
            ));
        }
        let mut bytes = Vec::new();
        File::open(manifest_path)
            .map_err(io_error)?
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() > 4096 {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "OLTP disk identity exceeds limit",
            ));
        }
        let manifest: StoreManifest = serde_json::from_slice(&bytes)
            .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid OLTP disk identity"))?;
        manifest.validate()?;
        match identity {
            None if manifest.topology == "standalone-rf1" => {}
            Some((cluster, group, database))
                if manifest.topology == "replicated-one-group-v2"
                    && manifest.cluster_id == cluster
                    && manifest.group_id == Some(group)
                    && manifest.database.as_deref() == Some(database) => {}
            _ => {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "store topology, group or cluster identity mismatch",
                ));
            }
        }
        return Ok(manifest);
    }
    if fs::read_dir(path).map_err(io_error)?.next().is_some() {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "refusing foreign or unidentified data directory",
        ));
    }
    let manifest = StoreManifest {
        engine: "oltp".into(),
        format_version: if identity.is_some() { 2 } else { 1 },
        topology: if identity.is_some() {
            "replicated-one-group-v2"
        } else {
            "standalone-rf1"
        }
        .into(),
        cluster_id: identity.map_or_else(Uuid::new_v4, |identity| identity.0),
        pool_id: Uuid::new_v4(),
        group_id: identity.map(|identity| identity.1),
        database: identity.map(|identity| identity.2.to_owned()),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(manifest_path)
        .map_err(io_error)?;
    file.write_all(&encode(&manifest)?).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    File::open(path)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)?;
    Ok(manifest)
}

pub(crate) fn valid_database(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn validate_operation(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
    operation: &Operation,
) -> Result<(), ProtocolError> {
    operation.validate()?;
    let id = match operation {
        Operation::Drop(p) => p.table_id,
        Operation::Insert(p) | Operation::Upsert(p) => p.table_id,
        Operation::InsertMany(p) => p.table_id,
        Operation::Update(p) => p.table_id,
        Operation::Delete(p) => p.table_id,
        _ => return Ok(()),
    };
    operation.validate_for_table(&resolve(snapshot, namespace, id)?)
}

fn active_namespace(snapshot: &Snapshot<'_>) -> Result<codec::Namespace, ProtocolError> {
    let bytes = snapshot
        .get(codec::ACTIVE)
        .map_err(storage_error)?
        .ok_or_else(|| failure(ErrorCode::StorageUnavailable, "missing active generation"))?;
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid active generation"))?;
    Ok(codec::Namespace(u64::from_be_bytes(bytes)))
}

fn validate_identity(digest: &str, run_id: Uuid) -> Result<(), ProtocolError> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) || run_id.is_nil()
    {
        return Err(failure(
            ErrorCode::ValidationError,
            "invalid durable request identity",
        ));
    }
    Ok(())
}

fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}

fn decision_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "durable mutation decision cannot be read; outcome requires reconciliation",
        Outcome::Unknown,
    )
}

fn recovery_unavailable() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "OLTP reads require reopening after an uncertain native write",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TestDir(std::path::PathBuf);
    impl TestDir {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("oltp-unit-{}", Uuid::new_v4())))
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn operation(name: &str, payload: Value) -> Operation {
        protocol::parse_request(
            &serde_json::to_vec(
                &json!({"request_id":Uuid::new_v4(),"operation":name,"payload":payload}),
            )
            .unwrap(),
        )
        .unwrap()
        .operation
    }

    #[tokio::test]
    async fn o07_o11_prepared_batch_is_private_and_old_snapshot_pins_schema_and_rows() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        let descriptor = engine
            .execute(operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ))
            .await
            .unwrap();
        let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
        engine
            .execute(operation(
                "table.insert",
                json!({"table_id":id,"row":{"id":1}}),
            ))
            .await
            .unwrap();
        let snapshot = engine.inner.db.snapshot();
        let mut batch = WriteBatch::default();
        engine
            .prepare(
                &snapshot,
                &operation(
                    "table.insert_many",
                    json!({"table_id":id,"rows":[{"id":2},{"id":3}]}),
                ),
                &mut batch,
                1 << 20,
                None,
            )
            .unwrap();
        assert!(!batch.is_empty());
        drop(batch);
        let get2 = operation("table.get", json!({"table_id":id,"key":2}));
        assert_eq!(engine.execute(get2).await.unwrap(), Value::Null);
        engine
            .execute(operation("table.drop", json!({"table_id":id})))
            .await
            .unwrap();
        let get1 = operation("table.get", json!({"table_id":id,"key":1}));
        assert_eq!(engine.read(&snapshot, &get1).unwrap(), json!({"id":1}));
        assert_eq!(
            engine.execute(get1).await.unwrap_err().code,
            ErrorCode::NotFound
        );
        drop(snapshot);
        drop(engine);
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        assert_eq!(
            engine
                .execute(operation("table.list", json!({})))
                .await
                .unwrap(),
            json!([])
        );
    }

    #[tokio::test]
    async fn m01_f01_preflight_rejects_memory_without_partial_write_and_releases_slot() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                query_memory_bytes: 32768,
                executor_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let descriptor=engine.execute(operation("table.create",json!({"name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"}))).await.unwrap();
        let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
        let rows: Vec<_> = (0..10)
            .map(|id| json!({"id":id,"text":"x".repeat(1024)}))
            .collect();
        assert_eq!(
            engine
                .execute(operation(
                    "table.insert_many",
                    json!({"table_id":id,"rows":rows})
                ))
                .await
                .unwrap_err()
                .code,
            ErrorCode::LimitExceeded
        );
        assert_eq!(engine.execute(operation("query.count",json!({"table_id":id,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}))).await.unwrap(),json!(0));
        engine
            .execute(operation(
                "table.insert",
                json!({"table_id":id,"row":{"id":10,"text":"ok"}}),
            ))
            .await
            .unwrap();
        assert_eq!(
            engine
                .execute(operation("table.get", json!({"table_id":id,"key":10})))
                .await
                .unwrap(),
            json!({"id":10,"text":"ok"})
        );
    }

    #[tokio::test]
    async fn o01_o03_constructed_nonfinite_rows_fail_validation_before_serialization() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        let descriptor=engine.execute(operation("table.create",json!({"name":"items","schema":{"id":"int64","rank":"float64"},"key":"id","format":"parquet"}))).await.unwrap();
        let table_id = serde_json::from_value(descriptor["id"].clone()).unwrap();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let operation = Operation::Insert(protocol::RowPayload {
                table_id,
                row: [
                    ("id".into(), protocol::Scalar::Int64(1)),
                    ("rank".into(), protocol::Scalar::Float64(value)),
                ]
                .into(),
            });
            assert_eq!(
                engine.execute(operation).await.unwrap_err().code,
                ErrorCode::ValidationError
            );
        }
        assert_eq!(
            engine
                .execute(operation("table.get", json!({"table_id":table_id,"key":1})))
                .await
                .unwrap(),
            Value::Null
        );
    }

    #[tokio::test]
    async fn f02_o10_panic_after_durable_write_stays_unknown_and_reconciles() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let descriptor = engine
            .execute(operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ))
            .await
            .unwrap();
        let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
        let request_id = Uuid::new_v4();
        let run = Uuid::new_v4();
        let digest = "a".repeat(64);
        let worker = engine.clone();
        let saved_digest = digest.clone();
        let result: Result<Value, ProtocolError> = engine
            .blocking(move || {
                worker.execute_sync(
                    operation("table.insert", json!({"table_id":id,"row":{"id":1}})),
                    Some((request_id, saved_digest, run)),
                )?;
                panic!("deterministic fault after sync and before terminal delivery");
            })
            .await;
        assert_eq!(result.unwrap_err().outcome, Outcome::Unknown);
        assert_eq!(
            engine
                .lookup_decision(request_id)
                .await
                .unwrap()
                .unwrap()
                .result,
            Ok(Value::Null)
        );
        assert_eq!(
            engine
                .execute(operation("table.get", json!({"table_id":id,"key":1})))
                .await
                .unwrap(),
            json!({"id":1})
        );
    }

    #[tokio::test]
    async fn f02_panic_inside_unit_writer_mutex_does_not_disable_future_mutations() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let descriptor = engine
            .execute(operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ))
            .await
            .unwrap();
        let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
        let worker = engine.clone();
        let result: Result<Value, ProtocolError> = engine
            .blocking(move || {
                let _guard = worker.writer();
                let mut private = WriteBatch::default();
                private.put(b"not-visible", b"private");
                panic!("deterministic fault while holding only the unit writer guard");
            })
            .await;
        assert_eq!(result.unwrap_err().outcome, Outcome::Unknown);
        assert!(engine.inner.write_lock.is_poisoned());
        engine
            .execute(operation(
                "table.insert",
                json!({"table_id":id,"row":{"id":1}}),
            ))
            .await
            .unwrap();
        assert!(!engine.inner.write_lock.is_poisoned());
        assert!(engine.inner.db.get(b"not-visible").unwrap().is_none());
        assert_eq!(
            engine
                .execute(operation("table.get", json!({"table_id":id,"key":1})))
                .await
                .unwrap(),
            json!({"id":1})
        );
    }

    #[tokio::test]
    async fn c07_raw_preparation_checks_mutation_flag_and_cancelled_waiters_release_ownership() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let request = Request {
            request_id: Uuid::new_v4(),
            operation: operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ),
        };
        let body = serde_json::to_vec(&request).unwrap();
        let digest = Sha256::digest(&body)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let run = Uuid::new_v4();
        let owner = engine.writer_ownership().await;
        let error = engine
            .prepare_identified_body(body.clone(), digest.clone(), run, false)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ValidationError);
        assert_eq!(engine.waiting_writers(), 0);
        let worker = engine.clone();
        let waiting_body = body.clone();
        let waiting_digest = digest.clone();
        let waiting = tokio::spawn(async move {
            worker
                .prepare_identified_body(waiting_body, waiting_digest, run, true)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while engine.waiting_writers() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(engine.waiting_writers(), 0);
        drop(owner);
        let result = engine
            .prepare_identified_body(body, digest, run, true)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(result["name"], "items");
        assert_eq!(engine.inner.slots.available_permits(), 1);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // Deliberate deterministic native fault barrier.
    async fn f03_cancelled_execution_retains_external_charge_until_native_work_settles() {
        struct Charge(Arc<AtomicUsize>);
        impl Drop for Charge {
            fn drop(&mut self) {
                self.0.fetch_sub(1, AtomicOrdering::AcqRel);
            }
        }
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let descriptor = engine
            .execute(operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ))
            .await
            .unwrap();
        let id: Uuid = serde_json::from_value(descriptor["id"].clone()).unwrap();
        let request = Request {
            request_id: Uuid::new_v4(),
            operation: operation("table.insert", json!({"table_id":id,"row":{"id":1}})),
        };
        let prepared = engine
            .prepare_execution(request, "a".repeat(64), Uuid::new_v4())
            .await
            .unwrap();
        let barrier = engine.writer();
        let live = Arc::new(AtomicUsize::new(1));
        let charge = Charge(live.clone());
        let running = tokio::spawn(async move { prepared.execute_with_guard(charge).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while engine.active_executions() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.abort();
        let _ = running.await;
        assert_eq!(live.load(AtomicOrdering::Acquire), 1);
        assert_eq!(engine.active_executions(), 1);
        drop(barrier);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while live.load(AtomicOrdering::Acquire) != 0
                || engine.active_executions() != 0
                || engine.inner.slots.available_permits() != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            engine
                .execute(operation("table.get", json!({"table_id":id,"key":1})))
                .await
                .unwrap(),
            json!({"id":1})
        );
    }

    #[tokio::test]
    async fn o29_o11_model_durable_but_unobserved_native_write_requires_reopen_before_absence_is_authoritative()
     {
        let dir = TestDir::new();
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        let request = Request {
            request_id: Uuid::new_v4(),
            operation: operation(
                "table.create",
                json!({"name":"committed","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ),
        };
        let digest = "a".repeat(64);
        let run = Uuid::new_v4();
        // An older snapshot supplies the absent observation in this model. The
        // real write below succeeds and is already visible to normal gets. This
        // is NOT injected failure between native WAL sync and memtable apply.
        let before = engine.inner.db.snapshot();
        let namespace = active_namespace(&before).unwrap();
        let result = engine
            .execute_identified(request.clone(), digest.clone(), run)
            .await
            .unwrap();
        let alias = engine.clone();
        let healthy_dir = TestDir::new();
        let healthy = OltpEngine::open(&healthy_dir.0, OltpResources::default()).unwrap();
        let missing = before.get(namespace.decision(request.request_id)).unwrap();
        assert!(missing.is_none());
        engine
            .recovery_marker()
            .store(true, AtomicOrdering::Release);
        assert!(alias.requires_recovery());
        assert_eq!(
            engine.observed_decision(missing).unwrap_err().outcome,
            Outcome::Unknown
        );
        let unknown_id = Uuid::new_v4();
        assert_eq!(
            engine
                .lookup_decision(unknown_id)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(
            alias
                .lookup_decision(Uuid::new_v4())
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(
            healthy
                .execute(operation(
                    "table.create",
                    json!({"name":"healthy","schema":{"id":"int64"},"key":"id","format":"parquet"})
                ))
                .await
                .unwrap()["name"],
            "healthy"
        );
        let sequence = engine.inner.db.latest_sequence_number();
        let other = Request {
            request_id: unknown_id,
            operation: operation(
                "table.create",
                json!({"name":"must_not_recompute","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ),
        };
        assert_eq!(
            engine
                .execute_identified(other, "b".repeat(64), run)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(engine.inner.db.latest_sequence_number(), sequence);
        assert_eq!(
            engine
                .execute_identified(request.clone(), digest.clone(), Uuid::new_v4())
                .await
                .unwrap(),
            result
        );
        assert_eq!(
            engine
                .execute(operation("table.list", json!({})))
                .await
                .unwrap_err()
                .code,
            ErrorCode::StorageUnavailable
        );
        drop(alias);
        drop(before);
        drop(engine);
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        assert!(!engine.requires_recovery());
        assert_eq!(
            engine
                .execute_identified(request, digest, Uuid::new_v4())
                .await
                .unwrap(),
            result
        );
        assert_eq!(
            engine
                .execute(operation(
                    "table.describe",
                    json!({"name":"must_not_recompute"})
                ))
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
    }

    #[test]
    fn c01_cli_runtime_dispatches_seven_native_threads_before_barrier_release() {
        const JOBS: usize = 7;
        struct Release(Arc<(Mutex<bool>, std::sync::Condvar)>);
        impl Release {
            fn release(&self) {
                let (lock, wake) = &*self.0;
                *lock.lock().unwrap() = true;
                wake.notify_all();
            }
        }
        impl Drop for Release {
            fn drop(&mut self) {
                self.release();
            }
        }
        let runtime = crate::oltp_runtime::standalone_network_runtime().unwrap();
        let dir = TestDir::new();
        let engine = OltpEngine::open(
            &dir.0,
            OltpResources {
                executor_threads: JOBS,
                background_threads: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let release = Release(Arc::new((Mutex::new(false), std::sync::Condvar::new())));
        let (sender, receiver) = std::sync::mpsc::sync_channel(JOBS);
        let mut jobs = Vec::new();
        for ordinal in 0..JOBS {
            let worker = engine.clone();
            let gate = release.0.clone();
            let sender = sender.clone();
            jobs.push(runtime.spawn(async move {
                worker
                    .blocking(move || {
                        sender.send(std::thread::current().id()).map_err(|_| {
                            failure(ErrorCode::InternalError, "native barrier observer stopped")
                        })?;
                        let (lock, wake) = &*gate;
                        let mut released = lock.lock().unwrap();
                        while !*released {
                            released = wake.wait(released).unwrap();
                        }
                        Ok(ordinal)
                    })
                    .await
            }));
        }
        drop(sender);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut threads = std::collections::HashSet::new();
        let mut entered = 0;
        for _ in 0..JOBS {
            match receiver
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            {
                Ok(thread) => {
                    threads.insert(thread);
                    entered += 1;
                }
                Err(_) => break,
            }
        }
        let active_at_barrier = engine.active_executions();
        // Release every native task even when the observation watchdog fails.
        release.release();
        let mut completed = [false; JOBS];
        let mut results = Vec::new();
        let joined = runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                for (index, job) in jobs.iter_mut().enumerate() {
                    results.push(job.await);
                    completed[index] = true;
                }
            })
            .await
        });
        if joined.is_err() {
            for (index, job) in jobs.iter().enumerate() {
                if !completed[index] {
                    job.abort();
                }
            }
            runtime.block_on(async {
                for (index, job) in jobs.iter_mut().enumerate() {
                    if !completed[index] {
                        let _ = job.await;
                    }
                }
            });
        }
        let settled = runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                engine.inner.slots.clone().acquire_many_owned(JOBS as u32),
            )
            .await
        });
        assert!(
            matches!(&settled, Ok(Ok(_))),
            "native jobs did not release execution ownership"
        );
        drop(settled);
        assert!(
            joined.is_ok(),
            "native jobs did not settle after the barrier opened"
        );
        assert_eq!(entered, JOBS);
        assert_eq!(threads.len(), JOBS);
        assert_eq!(active_at_barrier, JOBS);
        let mut outcomes: Vec<_> = results
            .into_iter()
            .map(|result| result.unwrap().unwrap())
            .collect();
        outcomes.sort_unstable();
        assert_eq!(outcomes, (0..JOBS).collect::<Vec<_>>());
        assert_eq!(engine.active_executions(), 0);
        eprintln!(
            "C01 native capacity: configured_foreground={JOBS}, entered={entered}, unique_thread_ids={}, active_before_release={active_at_barrier}",
            threads.len()
        );
    }

    #[tokio::test]
    async fn o10_o29_unreadable_decision_proof_remains_unknown_without_reapplying() {
        let dir = TestDir::new();
        let engine = OltpEngine::open(&dir.0, OltpResources::default()).unwrap();
        let request = Request {
            request_id: Uuid::new_v4(),
            operation: operation(
                "table.create",
                json!({"name":"committed","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ),
        };
        let digest = "a".repeat(64);
        let run = Uuid::new_v4();
        let committed = engine
            .execute_identified(request.clone(), digest.clone(), run)
            .await
            .unwrap();
        let namespace = active_namespace(&engine.inner.db.snapshot()).unwrap();
        // Model invalid persisted proof encoding. This does not inject an SST
        // checksum failure or claim a native disk-corruption fault fixture.
        engine
            .inner
            .db
            .put_opt(
                namespace.decision(request.request_id),
                b"invalid decision JSON",
                &sync_options(),
            )
            .unwrap();
        let sequence = engine.inner.db.latest_sequence_number();
        for error in [
            engine
                .lookup_decision(request.request_id)
                .await
                .unwrap_err(),
            engine
                .execute_identified(request.clone(), digest.clone(), run)
                .await
                .unwrap_err(),
        ] {
            assert_eq!(error.code, ErrorCode::OutcomeUnknown);
            assert_eq!(error.outcome, Outcome::Unknown);
        }
        assert_eq!(engine.inner.db.latest_sequence_number(), sequence);
        assert_eq!(
            engine
                .execute(operation("table.describe", json!({"name":"committed"})))
                .await
                .unwrap(),
            committed
        );
        // An unreadable namespace also makes an old decision unavailable. It
        // must not turn replay uncertainty into a not_applied classification.
        engine
            .inner
            .db
            .put_opt(codec::ACTIVE, [0_u8], &sync_options())
            .unwrap();
        let sequence = engine.inner.db.latest_sequence_number();
        assert_eq!(
            engine
                .lookup_decision(request.request_id)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        assert_eq!(
            engine
                .execute_identified(request, digest, run)
                .await
                .unwrap_err()
                .outcome,
            Outcome::Unknown
        );
        let read_error = engine
            .execute(operation("table.list", json!({})))
            .await
            .unwrap_err();
        assert_eq!(read_error.code, ErrorCode::StorageUnavailable);
        assert_eq!(read_error.outcome, Outcome::NotApplied);
        assert_eq!(engine.inner.db.latest_sequence_number(), sequence);
    }

    #[test]
    fn o05_native_background_floor_is_validated_before_directory_creation() {
        let dir = TestDir::new();
        let result = OltpEngine::open(
            &dir.0,
            OltpResources {
                background_threads: 1,
                ..Default::default()
            },
        );
        assert!(result.is_err());
        assert!(!dir.0.exists());
        assert!(
            pool::WorkerSharedResources::new(
                0,
                crate::config::WorkerResources::default(),
                pool::SharedResourceConfig {
                    background_threads: 1,
                    ..Default::default()
                }
            )
            .is_err()
        );
        let shared = pool::WorkerSharedResources::new(
            0,
            crate::config::WorkerResources::default(),
            pool::SharedResourceConfig::default(),
        )
        .unwrap();
        let config = shared.config();
        assert_eq!(
            config.query_threads + config.control_threads + config.background_threads as usize,
            4
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn o05_isolated_native_open_observes_shared_background_pools_and_housekeeping() {
        const FLAG: &str = "NABEDRA_OLTP_NATIVE_THREAD_PROBE";
        const NAME: &str = "oltp::tests::o05_isolated_native_open_observes_shared_background_pools_and_housekeeping";
        if std::env::var_os(FLAG).is_none() {
            for background in [2, 8] {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", NAME, "--nocapture"])
                    .env(FLAG, background.to_string())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
                while child.try_wait().unwrap().is_none() {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("native thread probe child exceeded watchdog");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                let output = child.wait_with_output().unwrap();
                assert!(
                    output.status.success(),
                    "native probe stdout={} stderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                print!("{}", String::from_utf8_lossy(&output.stdout));
                eprint!("{}", String::from_utf8_lossy(&output.stderr));
            }
            return;
        }
        fn count() -> usize {
            #[cfg(target_os = "linux")]
            {
                fs::read_dir("/proc/self/task").unwrap().count()
            }
            #[cfg(target_os = "macos")]
            {
                // proc_pidinfo writes exactly the initialized task-info buffer.
                let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::uninit();
                let size = std::mem::size_of::<libc::proc_taskinfo>();
                let copied = unsafe {
                    libc::proc_pidinfo(
                        libc::getpid(),
                        libc::PROC_PIDTASKINFO,
                        0,
                        info.as_mut_ptr().cast(),
                        size as i32,
                    )
                };
                assert_eq!(copied as usize, size);
                unsafe { info.assume_init().pti_threadnum as usize }
            }
        }
        let background: i32 = std::env::var(FLAG).unwrap().parse().unwrap();
        let resources = OltpResources {
            background_threads: background,
            ..Default::default()
        };
        let before = count();
        let first_dir = TestDir::new();
        let second_dir = TestDir::new();
        let first = OltpEngine::open(&first_dir.0, resources).unwrap();
        let after_first = count();
        let second = OltpEngine::open(&second_dir.0, resources).unwrap();
        let after_second = count();
        // Native source pins HIGH1/LOW1 plus one process-shared periodic timer.
        // Network/executor threads are deliberately absent from this child.
        assert_eq!(after_first - before, background as usize + 1);
        assert_eq!(after_second, after_first);
        #[cfg(target_os = "linux")]
        {
            let names: Vec<_> = fs::read_dir("/proc/self/task")
                .unwrap()
                .map(|entry| fs::read_to_string(entry.unwrap().path().join("comm")).unwrap())
                .collect();
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.trim() == "rocksdb:high")
                    .count(),
                1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.trim() == "rocksdb:low")
                    .count(),
                background as usize - 1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.trim() == "rocksdb:bottom")
                    .count(),
                0
            );
        }
        eprintln!(
            "O05 native isolated probe: before={before}, after_first={after_first}, after_second={after_second}, maintenance_HIGH=1, maintenance_LOW={}, shared_housekeeping_timer=1",
            background - 1
        );
        eprintln!("O05 stack reservation: {:?}", first.thread_budget());
        drop(first);
        drop(second);
    }
}
