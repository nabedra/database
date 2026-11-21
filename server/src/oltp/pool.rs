//! Typed multi-group pool format. Legacy RF1 and experimental group stores are
//! deliberately separate directory formats and are never silently adopted.
mod activation;
mod closed;
mod damage;
pub(crate) use activation::{ReplicaActivationStatement, VerifiedReplicaActivation};
pub use damage::ACCEPT_DAMAGE_FILE;
use damage::group_of_scope;
pub(crate) use damage::{DamageCause, DamagedReplicaRemoved, ReleaseOutcome, quarantined_group};
pub(crate) mod mount;
pub(crate) use closed::{PoolCloseOutcome, PoolClosedStatement, VerifiedPoolClosed};
mod placement;
mod resources;
pub(crate) mod restore;
mod snapshot;
mod storage;
pub use snapshot::{GroupSnapshot, GroupSnapshotHeader, IncomingSnapshot, PoolSnapshotFile};
#[cfg(test)]
mod tests;
pub use resources::{
    ByteClass, ChargedBuffer, ChargedBytes, ExecutionClass, NativeComputeGuard, PreparedNative,
    QueueLease, ScratchLease, SharedResourceConfig, WorkerSharedResources,
};
use resources::{ByteReservation, capacity};
pub(crate) use resources::{GroupCapacityLease, RAFT_REPLICA_BYTES};

use super::{failure, recovery_unknown};
use crate::{
    oltp_replication::types::{Digest, GroupIdentity},
    protocol::{ErrorCode, ProtocolError},
    scheduling::MemoryCharge,
};
use bytes::Bytes;
use nabedra_kv::{Durability, Snapshot, Store, WriteBatch};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
pub use storage::StorageStats;
use storage::{
    GROUP_SCOPE_BYTES, RawIter, commit_error, group_scope, prefix_end, storage_error,
    unsupported_format, wait_commit,
};
pub(crate) use storage::{is_no_space, no_space};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

const POOL_MANIFEST: &str = "NABEDRA_OLTP_POOL.json";
const POOL_KEY: &[u8] = b"\0typed-pool-v1";
const MAX_KEY_BYTES: usize = 2 * crate::protocol::MAX_BODY_BYTES + 256;
const MAX_IDENTITY_BYTES: u64 = 4096;
const BATCH_OVERHEAD: usize = 32;
/// Pool format 2: a `nabedra-kv` store in `kv/`. Format 1 (the previous engine) is
/// refused (Q1).
pub const POOL_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolIdentity {
    pub engine: String,
    pub pool_format_version: u32,
    pub cluster_id: Uuid,
    pub worker_id: Uuid,
    pub pool_id: Uuid,
}
impl PoolIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.engine != "oltp"
            || self.pool_format_version != POOL_FORMAT_VERSION
            || self.cluster_id.is_nil()
            || self.worker_id.is_nil()
            || self.pool_id.is_nil()
        {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "invalid typed OLTP pool identity",
            ));
        }
        Ok(())
    }
}

struct PoolInner {
    /// Declared first so the store closes before the slot, charges and mount drop.
    store: OwnedStore,
    identity: PoolIdentity,
    path: std::path::PathBuf,
    shared: Arc<WorkerSharedResources>,
    recovery: Arc<AtomicBool>,
    groups: Arc<tokio::sync::Mutex<BTreeMap<Uuid, Weak<GroupInner>>>>,
    synced_requests: AtomicU64,
    /// Quarantine bookkeeping (NS14): run-time damage events, refusals, R6.
    damage: Arc<damage::DamageState>,
    _pool_slot: OwnedSemaphorePermit,
    _overhead: MemoryCharge,
    _mount: Option<VerifiedPoolMount>,
}
#[derive(Clone)]
pub struct OltpPool {
    inner: Arc<PoolInner>,
}
struct GroupInner {
    pool: OltpPool,
    identity: GroupIdentity,
    order: Arc<tokio::sync::Mutex<()>>,
    generation: Arc<tokio::sync::RwLock<()>>,
    log_order: Arc<tokio::sync::Mutex<()>>,
    log_metadata_order: Arc<tokio::sync::Mutex<()>>,
    /// The [`DeferredApplies`] owner that lets application writes skip their
    /// fsync, or 0. Set only by the Raft host after it proves this group's log
    /// lives in the same physical pool, and therefore the same journal; cleared
    /// when that host's token drops (RV-T2 Q5, RV-NS6 F6).
    deferred_apply: AtomicU64,
    /// Last [`DeferredApplies`] owner number handed out.
    deferral_owners: AtomicU64,
    /// Last unsynced application write: (applied index, journal sequence).
    deferred_tail: std::sync::Mutex<Option<(u64, u64)>>,
    synced_requests: AtomicU64,
    deferred_writes: AtomicU64,
    _handle_slot: GroupCapacityLease,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PoolWriteStats {
    /// Real journal fsyncs of this pool (RV-T2 Q1). Group commit lets one fsync
    /// cover synced writes of every group and lane on the pool.
    pub synced_writes: u64,
    /// Synced write and sync requests that completed (each needed an fsync to cover it).
    pub synced_requests: u64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GroupWriteStats {
    /// Synced write requests issued by this replica's log and state transactions.
    /// Group commit may cover several with one journal fsync; real fsyncs are
    /// [`PoolWriteStats::synced_writes`] (RV-NS6 F10).
    pub synced_requests: u64,
    /// Unsynced application writes made durable by a later synced write.
    pub deferred_writes: u64,
    /// Newest applied index a crash could still lose, if any.
    pub unsynced_applied_index: Option<u64>,
}
#[derive(Clone, Copy)]
enum MutationLane {
    State,
    Restore,
    Log,
    LogMetadata,
}
/// Durability of one group write. `ApplyHint` is unsynced only for a Raft
/// application write of a group whose host proved a shared WAL with its log.
#[derive(Clone, Copy)]
enum WriteSync {
    Synced,
    Unsynced,
    ApplyHint,
}
/// What a group mutation left for its caller to await after the executor.
enum Submitted {
    /// A replayed applied entry: its stored reply, nothing written.
    Done(ChargedBytes),
    /// The transaction wrote nothing.
    Empty { response: ChargedBytes, sync: bool },
    /// Queued in the journal; the caller awaits the commit.
    Pending {
        response: ChargedBytes,
        commit: nabedra_kv::Commit,
        deferred: bool,
    },
    /// L0 slowdown: a detached task, spawned by the executor, waits out the delay
    /// and then submits the batch. It owns the batch and its guards, so the write
    /// lands even when the caller is gone (RV-NS6 F4).
    Paced {
        response: ChargedBytes,
        task: tokio::task::JoinHandle<Result<nabedra_kv::Commit, ProtocolError>>,
        deferred: bool,
    },
}
#[derive(Clone, Copy)]
enum ReplyLane {
    Query,
    Control,
    DataApply,
}
impl From<ExecutionClass> for ReplyLane {
    fn from(value: ExecutionClass) -> Self {
        match value {
            ExecutionClass::Query => Self::Query,
            ExecutionClass::Control => Self::Control,
        }
    }
}
#[derive(Clone)]
pub struct GroupStore {
    inner: Arc<GroupInner>,
}
/// A Raft host's claim on deferred application sync, from
/// [`GroupStore::defer_apply_sync`]. Dropping it syncs later application
/// writes again, since no running log replays them (RV-T2 Q5). Only the claim
/// that set the switch clears it: an old host dropped after a new one started
/// leaves the new host's deferral alone (RV-NS6 F6).
pub(crate) struct DeferredApplies {
    group: Arc<GroupInner>,
    owner: u64,
}
impl Drop for DeferredApplies {
    fn drop(&mut self) {
        let _ = self.group.deferred_apply.compare_exchange(
            self.owner,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

pub use mount::VerifiedPoolMount;

impl OltpPool {
    /// Explicit development directory profile. It proves local identity and
    /// durability only, not a registered provider volume or failure domain.
    pub fn open_development(
        path: impl AsRef<Path>,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        Self::open_path(path.as_ref(), identity, shared, None)
    }
    /// Test-only: a development pool whose store runs on `fs` (for example a
    /// `FaultFs`, to inject I/O errors or to drop the unsynced journal tail).
    /// Journal segments are 1 MiB so an in-memory filesystem stays small.
    #[cfg(test)]
    pub(crate) fn open_development_on(
        path: impl AsRef<Path>,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
        fs: Arc<dyn nabedra_kv::fs::Fs>,
    ) -> Result<Self, ProtocolError> {
        Self::open_development_tuned(path, identity, shared, fs, |_| {})
    }
    /// Test-only: [`Self::open_development_on`] with store options adjusted by `tune`.
    #[cfg(test)]
    pub(crate) fn open_development_tuned(
        path: impl AsRef<Path>,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
        fs: Arc<dyn nabedra_kv::fs::Fs>,
        tune: fn(&mut nabedra_kv::Options),
    ) -> Result<Self, ProtocolError> {
        Self::open_path_on(path.as_ref(), identity, shared, None, fs, &|options| {
            options.journal.segment_bytes = 1 << 20;
            tune(options);
        })
    }
    /// Test-only: the pool's store, for engine stats and blocking maintenance
    /// (`flush`, `compact`, `gc_journal`; call those off the async runtime).
    #[cfg(test)]
    pub(crate) fn kv_store(&self) -> &Store {
        &self.inner.store
    }
    fn open_path(
        path: &Path,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
        mount: Option<VerifiedPoolMount>,
    ) -> Result<Self, ProtocolError> {
        Self::open_path_on(
            path,
            identity,
            shared,
            mount,
            nabedra_kv::fs::real(),
            &|_| {},
        )
    }
    fn open_path_on(
        path: &Path,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
        mount: Option<VerifiedPoolMount>,
        fs: Arc<dyn nabedra_kv::fs::Fs>,
        tune: &dyn Fn(&mut nabedra_kv::Options),
    ) -> Result<Self, ProtocolError> {
        identity.validate()?;
        super::native::validate_thread_defaults(shared.thread_budget())?;
        shared.bind_worker(identity.worker_id)?;
        let pool_slot = shared
            .pool_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let overhead = shared.pool_overhead()?;
        open_pool_manifest(path, &identity)?;
        let mut options =
            storage::store_options(&shared.storage, fs, shared.config.write_buffer_bytes);
        tune(&mut options);
        let opened = Store::open(&path.join(storage::STORE_DIR), options).map_err(|error| {
            tracing::error!(path = %path.display(), %error, "OLTP pool refused at open");
            damage::open_refused(error)
        })?;
        let store = OwnedStore(Some(opened.store));
        // R6 at pool level, sticky (NS14, `damage.rs`): new damage of more than 8
        // groups, or more than 10% of the pool's groups (always allowing one), is
        // more likely systemic than local, so the pool is refused instead (plan
        // section 3). Groups this pool already accepted are not counted again
        // (RV-NS6 F2); a refused open accepts nothing, so it refuses again.
        let accepted = if opened.quarantined.is_empty() {
            damage::admit(path, &store, &[], 0)?
        } else {
            let healthy = count_healthy_groups(&store)?;
            damage::admit(path, &store, &opened.quarantined, healthy)?
        };
        if opened.recovery.tail_truncations > 0 {
            tracing::warn!(
                path = %path.display(),
                bytes = opened.recovery.tail_bytes,
                "OLTP pool recovered a torn journal tail"
            );
        }
        let recovery = Arc::new(AtomicBool::new(false));
        let damage_state = Arc::new(damage::DamageState::new());
        {
            let path = path.to_owned();
            let damage_state = damage_state.clone();
            store.on_damage(Box::new(move |damage| {
                tracing::error!(
                    path = %path.display(),
                    group = ?group_of_scope(&damage.scope),
                    reason = ?damage.reason,
                    file = ?damage.file,
                    "OLTP storage damage found at run time; group quarantined, its writes are refused"
                );
                // Rule 1: the group's host stops; R6: the group is accepted.
                damage_state.found(&damage);
            }));
        }
        let snapshot = store.snapshot();
        match snapshot.get(POOL_KEY).map_err(storage_error)? {
            Some(value)
                if serde_json::from_slice::<PoolIdentity>(&value).ok().as_ref()
                    == Some(&identity) => {}
            Some(_) => {
                return Err(failure(
                    ErrorCode::StorageUnavailable,
                    "physical pool identities disagree",
                ));
            }
            None => {
                let mut iter = RawIter::new(&snapshot, &[], true);
                iter.seek(&[]);
                if iter.valid() {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "unidentified nonempty physical pool",
                    ));
                }
                iter.status()?;
                let bytes = shared.encode(&identity, MAX_IDENTITY_BYTES as usize)?;
                let mut batch = WriteBatch::new();
                batch.put(POOL_KEY, bytes.as_slice());
                let commit = store
                    .submit(batch, Durability::Sync, Box::new(()))
                    .map_err(|error| commit_error(error).1)?;
                wait_commit(commit).map_err(|error| commit_error(error).1)?;
            }
        }
        drop(snapshot);
        if let Some(accepted) = accepted {
            damage::persist_accepted(path, &store, &accepted)?;
        }
        let inner = Arc::new(PoolInner {
            store,
            identity,
            path: path.to_owned(),
            shared,
            recovery,
            groups: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
            synced_requests: AtomicU64::new(0),
            damage: damage_state,
            _pool_slot: pool_slot,
            _overhead: overhead,
            _mount: mount,
        });
        inner.shared.storage.register(&inner);
        inner.damage.bind(Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    pub fn open_registered(
        mount: VerifiedPoolMount,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        let path = mount.open_path(&identity)?;
        Self::open_path(&path, identity, shared, Some(mount))
    }
    pub fn identity(&self) -> &PoolIdentity {
        &self.inner.identity
    }
    pub fn resources(&self) -> &Arc<WorkerSharedResources> {
        &self.inner.shared
    }
    pub(crate) fn observe_registered_mount(
        &self,
    ) -> Result<mount::MountedPoolObservation, ProtocolError> {
        self.healthy()?;
        let observed = self
            .inner
            ._mount
            .as_ref()
            .ok_or_else(|| {
                failure(
                    ErrorCode::UnsupportedOperation,
                    "development directory is not a registered mounted pool",
                )
            })?
            .reobserve()?;
        if observed.expected.identity != self.inner.identity
            || self.inner.shared.worker_id() != Some(observed.expected.worker.worker_id)
        {
            return Err(storage_error_message());
        }
        Ok(observed)
    }
    /// True once a write's outcome is unknown: this handle or the store latched.
    /// Every alias of the pool shares the latch (O29, NS09).
    pub fn requires_recovery(&self) -> bool {
        self.inner.recovery.load(Ordering::Acquire) || self.inner.store.requires_recovery()
    }
    fn healthy(&self) -> Result<(), ProtocolError> {
        if self.requires_recovery() {
            Err(recovery_unknown())
        } else {
            Ok(())
        }
    }
    fn latch(&self) {
        self.inner.recovery.store(true, Ordering::Release);
    }
    /// A consistent read view of the whole pool.
    fn snapshot(&self) -> Snapshot {
        self.inner.store.snapshot()
    }
    /// The newest value of `key`.
    fn get_latest(&self, key: &[u8]) -> Result<Option<Bytes>, ProtocolError> {
        self.snapshot().get(key).map_err(storage_error)
    }
    /// Groups quarantined by damage, at open or at run time. They never start.
    pub fn quarantined_groups(&self) -> Vec<Uuid> {
        self.inner
            .store
            .quarantined()
            .iter()
            .filter_map(|damage| group_of_scope(&damage.scope))
            .collect()
    }
    /// True once the group's scope is quarantined, at open or at run time. One
    /// atomic load while nothing is quarantined.
    fn group_quarantined(&self, id: Uuid) -> bool {
        self.inner.store.is_quarantined(&group_scope(id))
    }
    /// Section 3 rule 1 (RV-NS6 F3): a quarantined group never writes again under
    /// its old identity, so its Raft node can neither append nor vote. Only the
    /// group is refused; the pool and its other groups keep serving. NS14 adds
    /// `GroupDamaged` and stops the host.
    fn refuse_quarantined(&self, id: Uuid) -> Result<(), ProtocolError> {
        if self.group_quarantined(id) {
            self.inner.damage.count_refusal();
            return Err(quarantined_group());
        }
        Ok(())
    }
    /// Queues `batch`, confined to `scope`. Never blocks on I/O. A batch for a
    /// quarantined group is refused before the journal, so nothing is written.
    fn submit(
        &self,
        scope: &[u8],
        mut batch: WriteBatch,
        durability: Durability,
        on_done: Box<dyn std::any::Any + Send>,
    ) -> Result<nabedra_kv::Commit, ProtocolError> {
        self.healthy()?;
        if let Some(group) = group_of_scope(scope) {
            self.refuse_quarantined(group)?;
        }
        batch.set_scope(scope);
        self.inner
            .store
            .submit(batch, durability, on_done)
            .map_err(|error| self.failed(error))
    }
    /// L0 slowdown pacing for a synchronous writer (executor threads). The store
    /// paces only callers that ask; a caller on an async runtime thread never
    /// sleeps here (it awaits [`Store::write_delay`] instead).
    fn pace_blocking(&self, bytes: usize) {
        let delay = self.inner.store.write_delay(bytes);
        if !delay.is_zero() && tokio::runtime::Handle::try_current().is_err() {
            std::thread::sleep(delay);
        }
    }
    /// The protocol error for a failed write; ambiguous failures latch the pool.
    fn failed(&self, error: nabedra_kv::Error) -> ProtocolError {
        if let nabedra_kv::Error::InvalidArgument(_) = error {
            // Refused before the journal: nothing was written.
            return failure(ErrorCode::StorageUnavailable, "invalid storage batch");
        }
        let (ambiguous, error) = commit_error(error);
        if ambiguous {
            self.latch();
        }
        error
    }
    /// A synced write from a synchronous pool path (executor threads, and pool
    /// open). Ordinary group transactions use the asynchronous commit instead.
    fn durable_write(&self, scope: &[u8], batch: WriteBatch) -> Result<(), ProtocolError> {
        self.pace_blocking(batch.encoded_bytes());
        let commit = self.submit(scope, batch, Durability::Sync, Box::new(()))?;
        wait_commit(commit).map_err(|error| self.failed(error))?;
        self.inner.synced_requests.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    /// Makes every write submitted before the call durable (`flush_wal(true)`).
    pub(crate) async fn sync_wal(&self) -> Result<(), ProtocolError> {
        self.healthy()?;
        let commit = self
            .inner
            .store
            .sync()
            .map_err(|error| self.failed(error))?;
        commit.done().await.map_err(|error| self.failed(error))?;
        self.inner.synced_requests.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    /// True while a visible write may still be lost by a crash.
    pub(crate) fn has_unsynced_writes(&self) -> bool {
        self.inner.store.visible_seq() > self.inner.store.durable_seq()
    }
    fn seq_durable(&self, seq: u64) -> bool {
        self.inner.store.durable_seq() >= seq
    }
    /// Fsync count of this physical pool, for `/v1/runtime/stats`.
    pub fn write_stats(&self) -> PoolWriteStats {
        PoolWriteStats {
            synced_writes: self.inner.store.stats().journal_syncs,
            synced_requests: self.inner.synced_requests.load(Ordering::Relaxed),
        }
    }
    pub async fn group(&self, identity: GroupIdentity) -> Result<GroupStore, ProtocolError> {
        self.open_group(identity, true, None).await
    }
    pub(crate) async fn retained_group(
        &self,
        identity: GroupIdentity,
    ) -> Result<GroupStore, ProtocolError> {
        self.open_group(identity, false, None).await
    }
    async fn open_group(
        &self,
        identity: GroupIdentity,
        create: bool,
        learner: Option<Digest>,
    ) -> Result<GroupStore, ProtocolError> {
        identity
            .validate()
            .map_err(|_| failure(ErrorCode::ValidationError, "invalid group identity"))?;
        if identity.cluster_id != self.identity().cluster_id {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "group belongs to another cluster",
            ));
        }
        self.healthy()?;
        let pool = self.clone();
        let mut groups = pool.inner.groups.clone().lock_owned().await;
        let shared = self.inner.shared.clone();
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.healthy()?;
                if let Some(binding) = learner {
                    pool.check_placement_learner(&identity, binding)?;
                }

                groups.retain(|_, group| group.strong_count() > 0);
                // Section 3 rule 1: a damaged group never starts under its old
                // identity, also when run-time damage found it while a handle is
                // still open (RV-NS6 F3). NS14 replaces and releases it.
                pool.refuse_quarantined(identity.group_id)?;
                if let Some(group) = groups.get(&identity.group_id).and_then(Weak::upgrade) {
                    if group.identity != identity {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "group identity differs from existing handle",
                        ));
                    }
                    return Ok(GroupStore { inner: group });
                }
                let slot = pool.inner.shared.reserve_group_handle()?;
                let key = identity_key(identity.group_id);
                match pool.get_latest(&key)? {
                    Some(bytes)
                        if serde_json::from_slice::<GroupIdentity>(&bytes)
                            .ok()
                            .as_ref()
                            == Some(&identity) => {}
                    Some(_) => {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "immutable group identity mismatch",
                        ));
                    }
                    None if create => {
                        let value = pool
                            .inner
                            .shared
                            .encode(&identity, MAX_IDENTITY_BYTES as usize)?;
                        let mut batch = WriteBatch::new();
                        batch.put(&key, value.as_slice());
                        batch.put(
                            &group_key(identity.group_id, b'a', &[]),
                            &0u64.to_be_bytes(),
                        );
                        if let Some(binding) = learner {
                            batch.put(
                                &placement::learner_key(identity.group_id),
                                binding.as_bytes(),
                            );
                        }
                        pool.durable_write(&group_scope(identity.group_id), batch)?;
                    }
                    None => {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "retained group identity is missing from the mounted pool",
                        ));
                    }
                }
                snapshot::recover_group(&pool, &identity)?;
                let group = Arc::new(GroupInner {
                    pool: pool.clone(),
                    identity,
                    order: Arc::new(tokio::sync::Mutex::new(())),
                    generation: Arc::new(tokio::sync::RwLock::new(())),
                    log_order: Arc::new(tokio::sync::Mutex::new(())),
                    log_metadata_order: Arc::new(tokio::sync::Mutex::new(())),
                    deferred_apply: AtomicU64::new(0),
                    deferral_owners: AtomicU64::new(0),
                    deferred_tail: std::sync::Mutex::new(None),
                    synced_requests: AtomicU64::new(0),
                    deferred_writes: AtomicU64::new(0),
                    _handle_slot: slot,
                });
                groups.insert(group.identity.group_id, Arc::downgrade(&group));
                Ok(GroupStore { inner: group })
            })
            .await
    }
    pub async fn resident_storage_handles(&self) -> usize {
        let mut groups = self.inner.groups.lock().await;
        groups.retain(|_, group| group.strong_count() > 0);
        groups.len()
    }
    /// A bounded inventory page seeks directly past each group's entire key
    /// range. Row count and log length do not enlarge metadata enumeration.
    pub async fn list_groups(
        &self,
        after: Option<Uuid>,
        max_items: usize,
        budget: TransactionBudget,
    ) -> Result<ChargedBytes, ProtocolError> {
        if max_items == 0 || max_items > 128 {
            return Err(capacity());
        }
        let output = self
            .resources()
            .reply_budget(ExecutionClass::Control, budget.output_bytes)
            .await?;
        let prepared = self.resources().prepare(ExecutionClass::Control).await?;
        let (mut scratch, _batch, output) = prepared
            .buffers(
                TransactionBudget {
                    batch_bytes: 0,
                    ..budget
                },
                output,
            )
            .await?;
        let _vector = scratch.split(
            max_items
                .checked_mul(
                    std::mem::size_of::<GroupIdentity>() + std::mem::size_of::<ByteReservation>(),
                )
                .ok_or_else(capacity)?,
        )?;
        let pool = self.clone();
        prepared
            .execute(0, 0, move || {
                let _vector = _vector;
                pool.healthy()?;
                let snapshot = pool.snapshot();
                let start = after
                    .map(|id| prefix_end(&group_scope(id)))
                    .unwrap_or_else(|| vec![0x40]);
                let mut iter = RawIter::bounded(&snapshot, &start, &[0x41], true);
                iter.seek(&start);
                let mut items = Vec::new();
                items.try_reserve_exact(max_items).map_err(|_| capacity())?;
                let mut last = None;
                let mut decoded_charges = Vec::new();
                decoded_charges
                    .try_reserve_exact(max_items)
                    .map_err(|_| capacity())?;
                while iter.valid() && items.len() < max_items {
                    let key = iter.key().ok_or_else(recovery_unknown)?;
                    if key.first() != Some(&0x40) {
                        break;
                    }
                    if key.len() < 18 {
                        return Err(recovery_unknown());
                    }
                    let id = Uuid::from_slice(&key[1..17]).map_err(|_| recovery_unknown())?;
                    let encoded = snapshot
                        .get(&identity_key(id))
                        .map_err(storage_error)?
                        .ok_or_else(recovery_unknown)?;
                    let decode =
                        scratch.split(encoded.len().checked_mul(2).ok_or_else(capacity)?)?;
                    let identity: GroupIdentity =
                        serde_json::from_slice(&encoded).map_err(|_| recovery_unknown())?;
                    identity.validate().map_err(|_| recovery_unknown())?;
                    items.push(identity);
                    decoded_charges.push(decode);
                    last = Some(id);
                    let next = prefix_end(&key[..GROUP_SCOPE_BYTES]);
                    iter.seek(&next);
                }
                iter.status()?;
                let exhausted =
                    !iter.valid() || iter.key().is_none_or(|key| key.first() != Some(&0x40));
                #[derive(Serialize)]
                struct InventoryPage {
                    items: Vec<GroupIdentity>,
                    after: Option<Uuid>,
                    exhausted: bool,
                }
                let result = output.encode(&InventoryPage {
                    items,
                    after: last,
                    exhausted,
                })?;
                pool.healthy()?;
                Ok(result)
            })
            .await
    }
}

pub use crate::oltp_replication::types::AppliedPosition;
#[derive(Clone, Debug)]
pub struct ApplyMeta {
    pub position: AppliedPosition,
    pub command_digest: Digest,
    pub membership: Option<ChargedBytes>,
}
#[derive(Clone, Debug)]
pub struct AppliedState {
    pub position: AppliedPosition,
    pub command_digest: Digest,
    pub membership: Option<ChargedBytes>,
}
#[derive(Clone, Copy, Debug)]
pub struct TransactionBudget {
    pub scratch_bytes: usize,
    pub batch_bytes: usize,
    pub output_bytes: usize,
}
pub enum Transition {
    Commit(ChargedBytes),
    Reject(ChargedBytes),
}
#[derive(Clone, Debug)]
pub struct ScanProgress {
    pub visited: usize,
    pub last_key: Option<ChargedBytes>,
    pub exhausted: bool,
}

impl GroupStore {
    pub fn identity(&self) -> &GroupIdentity {
        &self.inner.identity
    }
    pub fn pool(&self) -> &OltpPool {
        &self.inner.pool
    }
    pub fn resources(&self) -> &Arc<WorkerSharedResources> {
        self.pool().resources()
    }

    pub async fn transaction(
        &self,
        meta: ApplyMeta,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.mutate(
            Some(meta),
            None::<()>,
            MutationLane::State,
            WriteSync::ApplyHint,
            budget,
            work,
        )
        .await
    }
    /// Membership is serialized from an already owned Raft entry inside the
    /// private native apply reservation. Transport arenas are not prerequisites
    /// for applying a committed membership change.
    pub async fn transaction_with_membership<T: Serialize + Send + 'static>(
        &self,
        meta: ApplyMeta,
        budget: TransactionBudget,
        membership: Option<T>,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        if meta.membership.is_some() && membership.is_some() {
            return Err(failure(
                ErrorCode::ValidationError,
                "duplicate membership sources",
            ));
        }
        self.mutate(
            Some(meta),
            membership,
            MutationLane::State,
            WriteSync::ApplyHint,
            budget,
            work,
        )
        .await
    }
    pub async fn log_transaction(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.mutate(
            None,
            None::<()>,
            MutationLane::Log,
            WriteSync::Synced,
            budget,
            work,
        )
        .await
    }
    /// Small retained node/vote/commit metadata has independent execution and
    /// order from large log bodies and application state. Adapters must retain
    /// their log writer order for overlapping tail/purge/truncation updates.
    pub async fn log_transaction_metadata(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.mutate(
            None,
            None::<()>,
            MutationLane::LogMetadata,
            WriteSync::Synced,
            budget,
            work,
        )
        .await
    }
    /// Unsynced log metadata. Only recovery hints that OpenRaft may lose, such
    /// as the commit marker, use it.
    pub async fn log_transaction_metadata_unsynced(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.mutate(
            None,
            None::<()>,
            MutationLane::LogMetadata,
            WriteSync::Unsynced,
            budget,
            work,
        )
        .await
    }
    /// Lets Raft application writes skip their own fsync while the returned
    /// token lives. The caller is the Raft host and passes the store that holds
    /// this group's synced log; the switch is refused (`None`) unless both are
    /// the same group in the same physical pool, so the log and the state share
    /// one journal and one sync domain. Dropping the token turns deferral off,
    /// unless a newer host took it over in the meantime (RV-NS6 F6).
    #[must_use = "application writes are synced again once the token drops"]
    pub(crate) fn defer_apply_sync(&self, log: &GroupStore) -> Option<DeferredApplies> {
        if !Arc::ptr_eq(&self.pool().inner, &log.pool().inner) || self.identity() != log.identity()
        {
            return None;
        }
        let owner = self.inner.deferral_owners.fetch_add(1, Ordering::AcqRel) + 1;
        self.inner.deferred_apply.store(owner, Ordering::Release);
        Some(DeferredApplies {
            group: self.inner.clone(),
            owner,
        })
    }
    /// Submits a group transaction's batch and records the deferred tail.
    fn submit_group(
        &self,
        batch: WriteBatch,
        durability: Durability,
        on_done: Box<dyn std::any::Any + Send>,
        deferred_index: Option<Option<u64>>,
    ) -> Result<nabedra_kv::Commit, ProtocolError> {
        let commit = self.pool().submit(
            &group_scope(self.identity().group_id),
            batch,
            durability,
            on_done,
        )?;
        if let Some(Some(index)) = deferred_index {
            self.record_deferred_tail(index, commit.seq());
        }
        Ok(commit)
    }
    /// Keeps the newest unsynced application write, by journal sequence. The tail
    /// is stored after `submit` returns, and by then the journal may already have
    /// released this write's order guard, so the next apply can store its newer
    /// tail first (RV-NS6 F5). The tail never moves backwards.
    fn record_deferred_tail(&self, index: u64, seq: u64) {
        let mut tail = self
            .inner
            .deferred_tail
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if tail.is_none_or(|(_, newest)| newest < seq) {
            *tail = Some((index, seq));
        }
    }
    pub fn applies_deferred(&self) -> bool {
        self.inner.deferred_apply.load(Ordering::Acquire) != 0
    }
    /// Synced-request and unsynced-write counts of this replica, for
    /// `/v1/runtime/stats`. Real fsyncs are counted per pool.
    pub fn write_stats(&self) -> GroupWriteStats {
        GroupWriteStats {
            synced_requests: self.inner.synced_requests.load(Ordering::Relaxed),
            deferred_writes: self.inner.deferred_writes.load(Ordering::Relaxed),
            unsynced_applied_index: self.unsynced_applied_index(),
        }
    }
    /// The newest applied index whose state a crash could still lose.
    fn unsynced_applied_index(&self) -> Option<u64> {
        let tail = *self
            .inner
            .deferred_tail
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        tail.filter(|(_, seq)| !self.pool().seq_durable(*seq))
            .map(|(index, _)| index)
    }
    /// Log purge calls this first: it refuses a purge past the applied index.
    /// No sync is needed (RV-T2 Q2): the applied write is visible, so it precedes
    /// the purge record in the journal, and the purge record is itself synced.
    /// A durable purge therefore implies durable applied state up to its index.
    pub(crate) async fn ensure_applied_durable(&self, index: u64) -> Result<(), ProtocolError> {
        if !self.applies_deferred() && self.unsynced_applied_index().is_none() {
            return Ok(());
        }
        let applied = self
            .applied_state(TransactionBudget {
                scratch_bytes: 256 << 10,
                batch_bytes: 0,
                output_bytes: 256 << 10,
            })
            .await?;
        if applied.is_none_or(|applied| applied.position.index < index) {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "log purge passes the applied state",
            ));
        }
        Ok(())
    }
    /// Snapshot export calls this while it holds the group's application order,
    /// before it takes a compute permit, so the exported applied index is durable
    /// before any snapshot names it. It keys on unsynced writes in the journal,
    /// not on the deferral switch: a tail written while a host deferred stays
    /// unsynced after that host stops (RV-NS6 F6). Nor on the recorded tail,
    /// which is stored only after `submit` returns, possibly after the journal
    /// already released the order lock this caller holds.
    pub(super) async fn sync_deferred_applies(&self) -> Result<(), ProtocolError> {
        if self.pool().has_unsynced_writes() {
            self.pool().sync_wal().await?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    async fn mutate<T: Serialize + Send + 'static>(
        &self,
        meta: Option<ApplyMeta>,
        membership: Option<T>,
        lane: MutationLane,
        sync: WriteSync,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        let (order, log, execution_class) = match lane {
            MutationLane::State => (&self.inner.order, false, self.execution_class()),
            MutationLane::Restore => (&self.inner.order, false, ExecutionClass::Query),
            MutationLane::Log => (&self.inner.log_order, true, self.execution_class()),
            MutationLane::LogMetadata => (
                &self.inner.log_metadata_order,
                true,
                ExecutionClass::Control,
            ),
        };
        // A placement activation lease pins both the native generation and
        // its stable voter set through the Control publication. Ordinary Data
        // entries still advance while that bounded lease is held.
        let generation_owner = if !log
            && (membership.is_some() || meta.as_ref().is_some_and(|meta| meta.membership.is_some()))
        {
            Some(self.inner.generation.clone().write_owned().await)
        } else {
            None
        };
        let owner = order.clone().lock_owned().await;
        let group = self.clone();
        let output = match execution_class {
            ExecutionClass::Query => {
                self.resources()
                    .data_apply_reply_budget(budget.output_bytes)
                    .await?
            }
            ExecutionClass::Control => {
                self.resources()
                    .reply_budget(ExecutionClass::Control, budget.output_bytes)
                    .await?
            }
        };
        let prepared = self.resources().prepare(execution_class).await?;
        let reservations = prepared.buffers(budget, output).await?;
        let runtime = tokio::runtime::Handle::try_current().ok();
        // The executor builds the batch and submits it, or hands it to a paced
        // task; it never waits for storage. Either way the write is dispatched
        // before the executor returns, so a caller cancelled meanwhile cannot drop
        // it. The journal thread drops the order guard, the generation guard and
        // the batch charge after the write is visible (and durable, for synced
        // writes) and before the commit completes, so a cancelled caller cannot
        // release ordering early and a returned write has released both (plan
        // section 5).
        let submitted = prepared
            .execute(0, 0, move || {
                let guards = (owner, generation_owner);
                group.pool().healthy()?;
                // Also refuses an empty synced write (a vote or append that changed
                // nothing) of a quarantined group: it would acknowledge durability.
                group.pool().refuse_quarantined(group.identity().group_id)?;
                let write_started = std::cell::Cell::new(false);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = group.pool().snapshot();
                    let prefix = group.prefix(&snapshot, log)?;
                    let mut transaction =
                        GroupTransaction::new(&snapshot, prefix, budget, reservations);
                    if let Some(meta) = &meta
                        && let Some(existing) =
                            group.read_applied(&snapshot, &mut transaction.read.scratch)?
                    {
                        if existing.position.index == meta.position.index {
                            if existing.position != meta.position
                                || existing.command_digest != meta.command_digest
                            {
                                return Err(failure(
                                    ErrorCode::StorageUnavailable,
                                    "conflicting applied entry replay",
                                ));
                            }
                            return group
                                .read_meta_bytes(&snapshot, b'r', &mut transaction.read.output)?
                                .ok_or_else(recovery_unknown)
                                .map(Submitted::Done);
                        }
                        if existing.position.index.checked_add(1) != Some(meta.position.index) {
                            return Err(failure(
                                ErrorCode::StorageUnavailable,
                                "nonsequential applied entry",
                            ));
                        }
                    }
                    let transition = work(&mut transaction)?;
                    let response = match transition {
                        Transition::Commit(response) => response,
                        Transition::Reject(response) => {
                            transaction.batch.clear();
                            transaction.batch_used = 12;
                            response
                        }
                    };
                    let response = transaction.read.output.rehome(response)?;
                    if let Some(meta) = &meta {
                        let mut encoded = [0u8; 56];
                        encoded[..8].copy_from_slice(&meta.position.term.to_be_bytes());
                        encoded[8..16].copy_from_slice(&meta.position.node_id.to_be_bytes());
                        encoded[16..24].copy_from_slice(&meta.position.index.to_be_bytes());
                        encoded[24..].copy_from_slice(meta.command_digest.as_bytes());
                        transaction
                            .put_meta(group_key(group.identity().group_id, b'p', &[]), &encoded)?;
                        transaction.put_meta(
                            group_key(group.identity().group_id, b'r', &[]),
                            response.as_slice(),
                        )?;
                        let encoded_membership = membership
                            .as_ref()
                            .map(|value| transaction.read.scratch.encode_value(value))
                            .transpose()?;
                        if let Some(membership) =
                            encoded_membership.as_ref().or(meta.membership.as_ref())
                        {
                            transaction.put_meta(
                                group_key(group.identity().group_id, b'm', &[]),
                                membership.as_slice(),
                            )?;
                        }
                    }
                    let deferred_index = match sync {
                        WriteSync::Synced => None,
                        WriteSync::Unsynced => Some(None),
                        WriteSync::ApplyHint => meta
                            .as_ref()
                            .filter(|_| group.applies_deferred())
                            .map(|meta| Some(meta.position.index)),
                    };
                    let GroupTransaction {
                        batch,
                        _batch_charge: batch_charge,
                        ..
                    } = transaction;
                    if batch.is_empty() {
                        // Nothing to order. A synced empty write still means
                        // "every earlier write is durable".
                        return Ok(Submitted::Empty {
                            response,
                            sync: deferred_index.is_none(),
                        });
                    }
                    write_started.set(true);
                    let durability = if deferred_index.is_some() {
                        Durability::NoSync
                    } else {
                        Durability::Sync
                    };
                    let on_done: Box<dyn std::any::Any + Send> = Box::new((guards, batch_charge));
                    // L0 slowdown pacing is opt-in per writer (NS5b). A slowed
                    // write leaves the executor and waits on a timer instead.
                    let delay = group.pool().inner.store.write_delay(batch.encoded_bytes());
                    if let Some(runtime) = runtime.as_ref().filter(|_| !delay.is_zero()) {
                        let paced = group.clone();
                        let task = runtime.spawn(async move {
                            tokio::time::sleep(delay).await;
                            paced.submit_group(batch, durability, on_done, deferred_index)
                        });
                        return Ok(Submitted::Paced {
                            response,
                            task,
                            deferred: deferred_index.is_some(),
                        });
                    }
                    if !delay.is_zero() {
                        // No runtime to put a timer on: pace here, on the executor.
                        std::thread::sleep(delay);
                    }
                    let commit = group.submit_group(batch, durability, on_done, deferred_index)?;
                    Ok(Submitted::Pending {
                        response,
                        commit,
                        deferred: deferred_index.is_some(),
                    })
                }));
                match result {
                    Ok(result) => result,
                    Err(_) => {
                        if write_started.get() {
                            group.pool().latch();
                        }
                        Err(recovery_unknown())
                    }
                }
            })
            .await?;
        let submitted = match submitted {
            Submitted::Paced {
                response,
                task,
                deferred,
            } => {
                // Dropping the handle on cancellation detaches the task, which
                // still submits the write.
                let commit = task.await.map_err(|_| {
                    self.pool().latch();
                    recovery_unknown()
                })??;
                Submitted::Pending {
                    response,
                    commit,
                    deferred,
                }
            }
            submitted => submitted,
        };
        match submitted {
            Submitted::Paced { .. } => unreachable!("paced writes were submitted above"),
            Submitted::Done(response) => Ok(response),
            Submitted::Empty { response, sync } => {
                if sync {
                    self.pool().sync_wal().await?;
                    self.inner.synced_requests.fetch_add(1, Ordering::Relaxed);
                }
                Ok(response)
            }
            Submitted::Pending {
                response,
                commit,
                deferred,
            } => {
                // The journal thread drops the order and generation guards and the
                // batch charge before it completes the commit (e655139), so the
                // caller's next request sees them released (RV-NS6 F7).
                commit
                    .done()
                    .await
                    .map_err(|error| self.pool().failed(error))?;
                if deferred {
                    self.inner.deferred_writes.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.inner.synced_requests.fetch_add(1, Ordering::Relaxed);
                    self.pool()
                        .inner
                        .synced_requests
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(response)
            }
        }
    }
    pub async fn read(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(
            false,
            self.execution_class(),
            self.execution_class().into(),
            budget,
            move |view| {
                let result = work(view)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    /// Bounded current application metadata such as durable participant seals
    /// uses the protected execution lane and one native snapshot.
    pub(crate) async fn read_metadata(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(
            false,
            ExecutionClass::Control,
            ReplyLane::Control,
            budget,
            move |view| {
                let result = work(view)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    /// Bulk backup/history traversal uses query execution and reply budgets,
    /// even for Status/TSO groups. It cannot consume their protected metadata
    /// lane while waiting for an owned working-memory decode reservation.
    pub(crate) async fn read_archive(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>, AppliedPosition) -> Result<ChargedBytes, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        let group = self.clone();
        self.read_namespace(
            false,
            ExecutionClass::Query,
            ReplyLane::Query,
            budget,
            move |view| {
                let applied = group
                    .read_applied(view.snapshot, &mut view.scratch)?
                    .ok_or_else(recovery_unknown)?
                    .position;
                let result = work(view, applied)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    pub(crate) async fn read_shard_usage<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut GroupRead<'_>, u64) -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        self.read_namespace(
            false,
            ExecutionClass::Query,
            ReplyLane::Query,
            TransactionBudget {
                scratch_bytes: 40 << 20,
                batch_bytes: 0,
                output_bytes: 0,
            },
            move |view| {
                let generation = u64::from_be_bytes(
                    view.prefix[18..]
                        .try_into()
                        .map_err(|_| recovery_unknown())?,
                );
                work(view, generation)
            },
        )
        .await
    }
    pub async fn log_read(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(
            true,
            self.execution_class(),
            self.execution_class().into(),
            budget,
            move |view| {
                let result = work(view)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    async fn read_namespace<T: Send + 'static>(
        &self,
        log: bool,
        execution_class: ExecutionClass,
        reply_class: ReplyLane,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let output = match reply_class {
            ReplyLane::Query => {
                self.resources()
                    .reply_budget(ExecutionClass::Query, budget.output_bytes)
                    .await?
            }
            ReplyLane::Control => {
                self.resources()
                    .reply_budget(ExecutionClass::Control, budget.output_bytes)
                    .await?
            }
            ReplyLane::DataApply => {
                self.resources()
                    .data_apply_reply_budget(budget.output_bytes)
                    .await?
            }
        };
        let prepared = self.resources().prepare(execution_class).await?;
        let (scratch, _batch, output) = prepared.buffers(budget, output).await?;
        let group = self.clone();
        prepared
            .execute(0, 0, move || {
                group.pool().healthy()?;
                let snapshot = group.pool().snapshot();
                let prefix = group.prefix(&snapshot, log)?;
                let mut read = GroupRead {
                    snapshot: &snapshot,
                    prefix,
                    scratch,
                    output,
                    key_buffer: KeyBuffer::default(),
                };
                let result = work(&mut read)?;
                group.pool().healthy()?;
                Ok(result)
            })
            .await
    }
    pub async fn log_read_metadata(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(
            true,
            ExecutionClass::Control,
            ReplyLane::Control,
            budget,
            move |view| {
                let result = work(view)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    /// A full log-body probe stays on the group's execution lane. Only its
    /// small footprint/hash response uses protected reply bytes.
    pub async fn log_read_probe(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(
            true,
            self.execution_class(),
            match self.execution_class() {
                ExecutionClass::Query => ReplyLane::DataApply,
                ExecutionClass::Control => ReplyLane::Control,
            },
            budget,
            move |view| {
                let result = work(view)?;
                view.output.rehome(result)
            },
        )
        .await
    }
    /// The caller reserves actual decode ownership before entering native work.
    /// Only a validated attached typed command may leave this callback; native
    /// apply buffers never become a Raft log payload or membership allocation.
    pub async fn log_read_owned<D: crate::oltp_replication::owned::OwnedPayload>(
        &self,
        budget: TransactionBudget,
        owner: Arc<ScratchLease>,
        work: impl FnOnce(
            &mut GroupRead<'_>,
            Arc<ScratchLease>,
        )
            -> Result<crate::oltp_replication::owned::OwnedCommand<D>, ProtocolError>
        + Send
        + 'static,
    ) -> Result<crate::oltp_replication::owned::OwnedCommand<D>, ProtocolError> {
        self.read_namespace(
            true,
            self.execution_class(),
            ReplyLane::Control,
            TransactionBudget {
                output_bytes: 0,
                ..budget
            },
            move |view| {
                let result = work(view, owner)?;
                result.ensure_charged()?;
                Ok(result)
            },
        )
        .await
    }
    pub(crate) async fn read_owned<D: crate::oltp_replication::owned::OwnedPayload>(
        &self,
        budget: TransactionBudget,
        owner: Arc<ScratchLease>,
        work: impl FnOnce(
            &mut GroupRead<'_>,
            Arc<ScratchLease>,
        )
            -> Result<crate::oltp_replication::owned::OwnedCommand<D>, ProtocolError>
        + Send
        + 'static,
    ) -> Result<crate::oltp_replication::owned::OwnedCommand<D>, ProtocolError> {
        self.read_namespace(
            false,
            self.execution_class(),
            ReplyLane::Control,
            TransactionBudget {
                output_bytes: 0,
                ..budget
            },
            move |view| {
                let result = work(view, owner)?;
                result.ensure_charged()?;
                Ok(result)
            },
        )
        .await
    }
    pub async fn applied_state(
        &self,
        budget: TransactionBudget,
    ) -> Result<Option<AppliedState>, ProtocolError> {
        let group = self.clone();
        self.read_namespace(
            false,
            ExecutionClass::Control,
            ReplyLane::Control,
            budget,
            move |view| {
                let mut result = group.read_applied(view.snapshot, &mut view.scratch)?;
                if let Some(state) = &mut result
                    && let Some(membership) = state.membership.take()
                {
                    state.membership = Some(view.output.rehome(membership)?);
                }
                Ok(result)
            },
        )
        .await
    }
    fn read_applied(
        &self,
        snapshot: &Snapshot,
        scratch: &mut ByteReservation,
    ) -> Result<Option<AppliedState>, ProtocolError> {
        let Some(bytes) = self.read_meta_bytes(snapshot, b'p', scratch)? else {
            return Ok(None);
        };
        let data = bytes.as_slice();
        if data.len() != 56 {
            return Err(recovery_unknown());
        }
        let number = |offset: usize| {
            u64::from_be_bytes(
                data[offset..offset + 8]
                    .try_into()
                    .expect("validated fixed applied length"),
            )
        };
        let position = AppliedPosition {
            term: number(0),
            node_id: number(8),
            index: number(16),
        };
        let command_digest = Digest::from_bytes(
            data[24..]
                .try_into()
                .expect("validated fixed digest length"),
        );
        let membership = self.read_meta_bytes(snapshot, b'm', scratch)?;
        Ok(Some(AppliedState {
            position,
            command_digest,
            membership,
        }))
    }
    fn read_meta_bytes(
        &self,
        snapshot: &Snapshot,
        tag: u8,
        reservation: &mut ByteReservation,
    ) -> Result<Option<ChargedBytes>, ProtocolError> {
        snapshot
            .get(&group_key(self.identity().group_id, tag, &[]))
            .map_err(|_| recovery_unknown())?
            .map(|value| reservation.copy(&value))
            .transpose()
    }
    fn prefix(&self, snapshot: &Snapshot, log: bool) -> Result<Vec<u8>, ProtocolError> {
        if log {
            return Ok(group_key(self.identity().group_id, b'l', &[]));
        }
        let bytes = snapshot
            .get(&group_key(self.identity().group_id, b'a', &[]))
            .map_err(storage_error)?
            .ok_or_else(recovery_unknown)?;
        let generation: u64 =
            u64::from_be_bytes(bytes.as_ref().try_into().map_err(|_| recovery_unknown())?);
        Ok(group_key(
            self.identity().group_id,
            b's',
            &generation.to_be_bytes(),
        ))
    }
    fn execution_class(&self) -> ExecutionClass {
        match self.identity().kind {
            crate::oltp_replication::types::GroupKind::Control
            | crate::oltp_replication::types::GroupKind::Timestamps
            | crate::oltp_replication::types::GroupKind::Status { .. } => ExecutionClass::Control,
            _ => ExecutionClass::Query,
        }
    }
}

pub struct GroupRead<'a> {
    snapshot: &'a Snapshot,
    prefix: Vec<u8>,
    scratch: ByteReservation,
    output: ByteReservation,
    key_buffer: KeyBuffer,
}
/// One record found by a bounded seek in the group's snapshot. Its value was
/// read (and checked) when the record was found.
pub struct ScopedRecord<'a> {
    iterator: nabedra_kv::Iter,
    value: Bytes,
    scope_length: usize,
    _snapshot: std::marker::PhantomData<&'a Snapshot>,
}
impl ScopedRecord<'_> {
    pub fn key(&self) -> &[u8] {
        &self.iterator.key()[self.scope_length..]
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}
#[derive(Default)]
struct KeyBuffer {
    data: Vec<u8>,
    charges: Vec<ByteReservation>,
}
impl<'a> GroupRead<'a> {
    pub fn with_scratch_buffer<T>(
        &mut self,
        length: usize,
        work: impl FnOnce(&mut Self, &mut [u8]) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        let mut buffer = self.scratch.allocate(length)?;
        let result = work(self, buffer.as_mut_slice());
        self.scratch.reclaim_buffer(buffer)?;
        result
    }
    /// Inspect a pinned value without copying a full log entry to a reply.
    /// The borrow ends with this callback; retain only explicitly owned output.
    pub fn inspect<T>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(Option<&[u8]>) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        let value = self.pinned(key)?;
        read(value.as_ref().map(|value| value.as_ref()))
    }
    /// A value shared with the store's memtable or block cache. Reading it copies
    /// nothing, which avoids copying a large private row merely to encode its
    /// fixed resolution.
    pub fn pinned(&mut self, key: &[u8]) -> Result<Option<Bytes>, ProtocolError> {
        let snapshot = self.snapshot;
        let key = self.key(key)?;
        snapshot.get(key).map_err(storage_error)
    }
    /// Seek directly to the first logical record at or after `start` within
    /// `prefix`. Versioned point reads need not scan newer versions or copy an
    /// entire row to choose the version visible at their read timestamp.
    pub fn first_from(
        &mut self,
        prefix: &[u8],
        start: &[u8],
    ) -> Result<Option<ScopedRecord<'a>>, ProtocolError> {
        if !start.starts_with(prefix) || prefix.len() > MAX_KEY_BYTES {
            return Err(failure(
                ErrorCode::ValidationError,
                "seek outside scoped prefix",
            ));
        }
        let snapshot = self.snapshot;
        let scope_length = self.prefix.len();
        let upper = prefix_end(&[self.prefix.as_slice(), prefix].concat());
        let start = self.key(start)?;
        let mut iterator = RawIter::bounded(snapshot, start, &upper, true);
        iterator.seek(start);
        iterator.status()?;
        if iterator.key().is_none_or(|key| {
            !key.starts_with(&self.prefix) || !key[scope_length..].starts_with(prefix)
        }) {
            return Ok(None);
        }
        let value = iterator.value()?;
        Ok(Some(ScopedRecord {
            iterator: iterator.into_inner(),
            value,
            scope_length,
            _snapshot: std::marker::PhantomData,
        }))
    }
    /// Snapshot-only. Candidate writes are deliberately not consulted.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<ChargedBytes>, ProtocolError> {
        self.pinned(key)?
            .map(|value| self.scratch.copy(&value))
            .transpose()
    }
    pub fn encode<T: Serialize>(&mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        self.output.encode_value(value)
    }
    pub fn encode_value<T: Serialize>(&mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        self.scratch.encode_value(value)
    }
    pub fn copy_output(&mut self, bytes: &[u8]) -> Result<ChargedBytes, ProtocolError> {
        self.output.copy(bytes)
    }
    fn key(&mut self, key: &[u8]) -> Result<&[u8], ProtocolError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        let length = self
            .prefix
            .len()
            .checked_add(key.len())
            .ok_or_else(capacity)?;
        if length > self.key_buffer.data.capacity() {
            let grow = length
                .max(
                    self.key_buffer
                        .data
                        .capacity()
                        .saturating_add(self.key_buffer.data.capacity().min(64 << 10)),
                )
                .min(MAX_KEY_BYTES + self.prefix.len());
            let charge = self.scratch.split(
                grow - self.key_buffer.data.capacity() + std::mem::size_of::<ByteReservation>(),
            )?;
            self.key_buffer
                .charges
                .try_reserve(1)
                .map_err(|_| capacity())?;
            self.key_buffer
                .data
                .try_reserve_exact(grow - self.key_buffer.data.len())
                .map_err(|_| capacity())?;
            self.key_buffer.charges.push(charge);
        }
        self.key_buffer.data.clear();
        self.key_buffer.data.extend_from_slice(&self.prefix);
        self.key_buffer.data.extend_from_slice(key);
        Ok(&self.key_buffer.data)
    }
    /// Values borrow the native iterator for this callback only. Callers encode
    /// retained outputs through this view's precharged output budget.
    pub fn scan_page(
        &mut self,
        prefix: &[u8],
        after: Option<&[u8]>,
        max_items: usize,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<(), ProtocolError>,
    ) -> Result<ScanProgress, ProtocolError> {
        self.scan_page_until(prefix, after, max_items, |key, value| {
            visit(key, value)?;
            Ok(false)
        })
    }
    pub(crate) fn scan_page_until(
        &mut self,
        prefix: &[u8],
        after: Option<&[u8]>,
        max_items: usize,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<bool, ProtocolError>,
    ) -> Result<ScanProgress, ProtocolError> {
        if max_items == 0 || max_items > 128 || prefix.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        let native = self.snapshot;
        let scope_length = self.prefix.len();
        let lower = self.scratch.join(&[&self.prefix, prefix])?;
        let start = self.key(after.unwrap_or(prefix))?;
        if !start.starts_with(lower.as_slice()) {
            return Err(failure(
                ErrorCode::ValidationError,
                "scan cursor outside prefix",
            ));
        }
        let upper = prefix_end(lower.as_slice());
        let mut iterator = RawIter::bounded(native, start, &upper, true);
        iterator.seek(start);
        if after.is_some() && iterator.key() == Some(start) {
            iterator.next();
        }
        let mut progress = ScanProgress {
            visited: 0,
            last_key: None,
            exhausted: false,
        };
        while iterator.valid() {
            let key = iterator.key().ok_or_else(recovery_unknown)?;
            if !key.starts_with(lower.as_slice()) {
                break;
            }
            let value = iterator.value()?;
            let stop = visit(&key[scope_length..], &value)?;
            progress.visited += 1;
            if stop || progress.visited == max_items {
                let cursor = self.scratch.copy(&key[scope_length..])?;
                iterator.next();
                iterator.status()?;
                progress.exhausted = !iterator.valid()
                    || iterator
                        .key()
                        .is_none_or(|key| !key.starts_with(lower.as_slice()));
                if !progress.exhausted {
                    progress.last_key = Some(cursor);
                }
                return Ok(progress);
            }
            iterator.next();
        }
        iterator.status()?;
        progress.exhausted = true;
        Ok(progress)
    }
}
/// A transaction's batch starts this small and grows as it is filled; `batch_limit`
/// still bounds it. Reserving the whole budget up front made every mvcc apply
/// (budget 32 MiB + 128 KiB, above glibc's mmap threshold) map and unmap 32 MiB
/// for a point write of a few KiB, a 2 MiB huge page under THP `always` (FLAKE1).
const INITIAL_BATCH_CAPACITY: usize = 4 << 10;

pub struct GroupTransaction<'a> {
    read: GroupRead<'a>,
    batch: WriteBatch,
    batch_used: usize,
    batch_limit: usize,
    _batch_charge: ByteReservation,
}
impl<'a> GroupTransaction<'a> {
    fn new(
        snapshot: &'a Snapshot,
        prefix: Vec<u8>,
        budget: TransactionBudget,
        (scratch, batch, output): (ByteReservation, ByteReservation, ByteReservation),
    ) -> Self {
        Self {
            read: GroupRead {
                snapshot,
                prefix,
                scratch,
                output,
                key_buffer: KeyBuffer::default(),
            },
            batch: WriteBatch::with_capacity_bytes(budget.batch_bytes.min(INITIAL_BATCH_CAPACITY)),
            batch_used: 12,
            batch_limit: budget.batch_bytes,
            _batch_charge: batch,
        }
    }
    /// Temporary private encoding has one owner, is copied into the native
    /// batch, then returns its scratch allowance for the next fixed write.
    pub fn put_serialized(
        &mut self,
        key: &[u8],
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        let encoded = self.read.scratch.encode_value(value)?;
        let result = self.put(key, encoded.as_slice());
        self.read.scratch.reclaim_value(encoded)?;
        result
    }
    /// The borrowed temporary cannot escape the callback. Its allocation and
    /// byte allowance are released before another page record is processed.
    pub fn with_scratch_buffer<T>(
        &mut self,
        length: usize,
        work: impl FnOnce(&mut Self, &mut [u8]) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        let mut buffer = self.read.scratch.allocate(length)?;
        let result = work(self, buffer.as_mut_slice());
        self.read.scratch.reclaim_buffer(buffer)?;
        result
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<ChargedBytes>, ProtocolError> {
        self.read.get(key)
    }
    pub fn first_from(
        &mut self,
        prefix: &[u8],
        start: &[u8],
    ) -> Result<Option<ScopedRecord<'a>>, ProtocolError> {
        self.read.first_from(prefix, start)
    }
    pub fn encode<T: Serialize>(&mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        self.read.encode(value)
    }
    pub fn encode_value<T: Serialize>(&mut self, value: &T) -> Result<ChargedBytes, ProtocolError> {
        self.read.encode_value(value)
    }
    pub fn copy_output(&mut self, bytes: &[u8]) -> Result<ChargedBytes, ProtocolError> {
        self.read.copy_output(bytes)
    }
    pub fn scan_page(
        &mut self,
        prefix: &[u8],
        after: Option<&[u8]>,
        max_items: usize,
        visit: impl FnMut(&[u8], &[u8]) -> Result<(), ProtocolError>,
    ) -> Result<ScanProgress, ProtocolError> {
        self.read.scan_page(prefix, after, max_items, visit)
    }
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ProtocolError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        self.preflight(self.read.prefix.len() + key.len(), value.len())?;
        let key = self.read.key(key)?;
        self.batch.put(key, value);
        Ok(())
    }
    /// Like [`Self::put`], but `value` is stored once, in the journal record, and the
    /// LSM holds only a 32-byte locator (plan section 2, "Journal values"). For large
    /// values deleted soon after they are written: Raft log entries. Reads resolve the
    /// locator transparently.
    pub fn put_journal(&mut self, key: &[u8], value: &[u8]) -> Result<(), ProtocolError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        self.preflight(self.read.prefix.len() + key.len(), value.len())?;
        let key = self.read.key(key)?;
        self.batch.put_journal(key, value);
        Ok(())
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<(), ProtocolError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        self.preflight(self.read.prefix.len() + key.len(), 0)?;
        let key = self.read.key(key)?;
        self.batch.delete(key);
        Ok(())
    }
    /// Both boundaries are logical keys in this one group/generation domain.
    /// They cannot name native metadata, another group, or an active pointer.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<(), ProtocolError> {
        if start >= end || start.len() > MAX_KEY_BYTES || end.len() > MAX_KEY_BYTES {
            return Err(failure(ErrorCode::ValidationError, "invalid scoped range"));
        }
        let cost = self
            .read
            .prefix
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(start.len()))
            .and_then(|n| n.checked_add(end.len()))
            .ok_or_else(capacity)?;
        self.preflight(cost, 0)?;
        let start = self.read.scratch.join(&[&self.read.prefix, start])?;
        let end = self.read.scratch.join(&[&self.read.prefix, end])?;
        self.batch.delete_range(start.as_slice(), end.as_slice());
        Ok(())
    }
    pub fn pinned(&mut self, key: &[u8]) -> Result<Option<Bytes>, ProtocolError> {
        self.read.pinned(key)
    }
    fn put_meta(&mut self, key: impl AsRef<[u8]>, value: &[u8]) -> Result<(), ProtocolError> {
        self.preflight(key.as_ref().len(), value.len())?;
        self.batch.put(key.as_ref(), value);
        Ok(())
    }
    fn preflight(&mut self, key: usize, value: usize) -> Result<(), ProtocolError> {
        let next = self
            .batch_used
            .checked_add(key)
            .and_then(|n| n.checked_add(value))
            .and_then(|n| n.checked_add(BATCH_OVERHEAD))
            .filter(|&n| n <= self.batch_limit)
            .ok_or_else(capacity)?;
        self.batch_used = next;
        Ok(())
    }
}

fn group_key(id: Uuid, tag: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(18 + suffix.len());
    key.push(0x40);
    key.extend_from_slice(id.as_bytes());
    key.push(tag);
    key.extend_from_slice(suffix);
    key
}
fn identity_key(id: Uuid) -> Vec<u8> {
    group_key(id, b'i', &[])
}
fn open_pool_manifest(path: &Path, identity: &PoolIdentity) -> Result<(), ProtocolError> {
    if path.exists() {
        let meta = fs::symlink_metadata(path).map_err(|_| storage_error_message())?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(storage_error_message());
        }
    } else {
        fs::create_dir_all(path).map_err(|_| storage_error_message())?;
    }
    let manifest = path.join(POOL_MANIFEST);
    if manifest.exists() {
        let file = File::open(&manifest).map_err(|_| storage_error_message())?;
        if !file
            .metadata()
            .map_err(|_| storage_error_message())?
            .is_file()
        {
            return Err(storage_error_message());
        }
        let mut bytes = Vec::new();
        file.take(MAX_IDENTITY_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| storage_error_message())?;
        if bytes.len() as u64 > MAX_IDENTITY_BYTES {
            return Err(storage_error_message());
        }
        let stored = serde_json::from_slice::<PoolIdentity>(&bytes).ok();
        if let Some(stored) = &stored
            && stored.pool_format_version != POOL_FORMAT_VERSION
        {
            // Q1 clean break: a format 1 pool (previous engine) is refused by name.
            return Err(unsupported_format(&format!(
                "{}: pool_format_version {}",
                manifest.display(),
                stored.pool_format_version
            )));
        }
        if stored.as_ref() != Some(identity) {
            return Err(storage_error_message());
        }
        return Ok(());
    }
    if fs::read_dir(path)
        .map_err(|_| storage_error_message())?
        .next()
        .is_some()
    {
        if let Some(found) = storage::legacy_store_marker(path) {
            return Err(unsupported_format(&found));
        }
        return Err(storage_error_message());
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&manifest)
        .map_err(|_| storage_error_message())?;
    serde_json::to_writer(&mut file, identity).map_err(|_| storage_error_message())?;
    file.flush()
        .and_then(|_| file.sync_all())
        .map_err(|_| storage_error_message())?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| storage_error_message())?;
    Ok(())
}
fn storage_error_message() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "typed physical pool unavailable or identity mismatch",
    )
}
/// Owns a pool's store. Closing a store waits for its background jobs and joins
/// its journal thread, so the last pool owner, which may be an async task, never
/// closes it while holding a multi-threaded runtime worker: the worker's other
/// tasks move to another thread first (KVfix1 note 8). A current-thread runtime
/// has no other worker; the close needs nothing from the runtime, so it only
/// blocks that thread briefly.
struct OwnedStore(Option<Store>);
impl std::ops::Deref for OwnedStore {
    type Target = Store;
    fn deref(&self) -> &Store {
        self.0
            .as_ref()
            .expect("the store is open until its owner drops")
    }
}
impl Drop for OwnedStore {
    fn drop(&mut self) {
        let Some(store) = self.0.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime)
                if runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
            {
                tokio::task::block_in_place(move || drop(store));
            }
            _ => drop(store),
        }
    }
}
/// Groups with a readable identity record, skipping quarantined scopes (R6).
fn count_healthy_groups(store: &Store) -> Result<usize, ProtocolError> {
    let snapshot = store.snapshot();
    let mut quarantined: Vec<Vec<u8>> = store
        .quarantined()
        .into_iter()
        .map(|damage| damage.scope.to_vec())
        .collect();
    quarantined.sort();
    // Each range between quarantined scopes is read with bounds that exclude
    // them, so the iterator never lands on a quarantined key.
    let mut ranges = Vec::new();
    let mut lower = vec![0x40];
    for scope in quarantined {
        if scope.as_slice() > lower.as_slice() {
            ranges.push((lower, scope.clone()));
        }
        lower = prefix_end(&scope);
    }
    ranges.push((lower, vec![0x41]));
    let mut groups = 0;
    for (lower, upper) in ranges {
        let mut iter = RawIter::bounded(&snapshot, &lower, &upper, false);
        iter.seek(&lower);
        while let Some(key) = iter.key() {
            if key.len() < GROUP_SCOPE_BYTES {
                return Err(recovery_unknown());
            }
            if key.len() == GROUP_SCOPE_BYTES + 1 && key[GROUP_SCOPE_BYTES] == b'i' {
                groups += 1;
            }
            // Identity keys sort after `a` (active generation) and before every
            // other per-group tag, so seeking to the identity then past the group
            // counts each group once.
            let scope = key[..GROUP_SCOPE_BYTES].to_vec();
            let mut identity = scope.clone();
            identity.push(b'i');
            if key < identity.as_slice() {
                iter.seek(&identity);
            } else {
                iter.seek(&prefix_end(&scope));
            }
        }
        iter.status()?;
    }
    Ok(groups)
}
