//! Typed multi-group pool format. Legacy RF1 and experimental group stores are
//! deliberately separate directory formats and are never silently adopted.
mod resources;
mod snapshot;
pub use snapshot::{GroupSnapshot, GroupSnapshotHeader};
#[cfg(test)]
mod tests;
pub use resources::{
    ByteClass, ChargedBuffer, ChargedBytes, ExecutionClass, PreparedNative, QueueLease,
    ScratchLease, SharedResourceConfig, WorkerSharedResources,
};
use resources::{ByteReservation, capacity};

use super::{failure, recovery_unknown, storage_error, sync_options};
use crate::{
    oltp_replication::types::{Digest, GroupIdentity},
    protocol::{ErrorCode, ProtocolError},
    scheduling::MemoryCharge,
};
use rocksdb::{
    BlockBasedOptions, DB, DBCompressionType, DBRecoveryMode, Options, Snapshot, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

const POOL_MANIFEST: &str = "NABEDRA_OLTP_POOL.json";
const POOL_KEY: &[u8] = b"\0typed-pool-v1";
const MAX_KEY_BYTES: usize = 2 * crate::protocol::MAX_BODY_BYTES + 256;
const MAX_IDENTITY_BYTES: u64 = 4096;
const BATCH_OVERHEAD: usize = 32;

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
            || self.pool_format_version != 1
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
    db: DB,
    identity: PoolIdentity,
    path: std::path::PathBuf,
    shared: Arc<WorkerSharedResources>,
    recovery: Arc<AtomicBool>,
    groups: Arc<tokio::sync::Mutex<BTreeMap<Uuid, Weak<GroupInner>>>>,
    _pool_slot: OwnedSemaphorePermit,
    _overhead: MemoryCharge,
}
#[derive(Clone)]
pub struct OltpPool {
    inner: Arc<PoolInner>,
}
struct GroupInner {
    pool: OltpPool,
    identity: GroupIdentity,
    order: Arc<tokio::sync::Mutex<()>>,
    _handle_slot: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct GroupStore {
    inner: Arc<GroupInner>,
}

/// A registered mount can only be created by the platform verifier. There is no
/// raw-path registered constructor or root-disk directory fallback. Provider
/// enrollment and anchored mount verification are not implemented here yet.
pub struct VerifiedPoolMount {
    _private: (),
}

impl OltpPool {
    /// Explicit development directory profile. It proves local identity and
    /// durability only, not a registered provider volume or failure domain.
    pub fn open_development(
        path: impl AsRef<Path>,
        identity: PoolIdentity,
        shared: Arc<WorkerSharedResources>,
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
        let path = path.as_ref();
        open_pool_manifest(path, &identity)?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(DBCompressionType::None);
        options.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
        options.set_write_buffer_size(shared.config.write_buffer_bytes / 2);
        options.set_max_write_buffer_number(2);
        options.set_write_buffer_manager(&shared.write_buffers);
        options.set_env(&shared.env);
        super::configure_native_jobs(&mut options, shared.config.background_threads);
        options.set_max_open_files(128);
        options.set_max_total_wal_size(shared.config.write_buffer_bytes as u64);
        let mut blocks = BlockBasedOptions::default();
        blocks.set_block_cache(&shared.cache);
        blocks.set_cache_index_and_filter_blocks(true);
        blocks.set_bloom_filter(10.0, false);
        options.set_block_based_table_factory(&blocks);
        let db = DB::open(&options, path).map_err(storage_error)?;
        match db.get_pinned(POOL_KEY).map_err(storage_error)? {
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
                let mut iter = db.raw_iterator();
                iter.seek_to_first();
                if iter.valid() {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "unidentified nonempty physical pool",
                    ));
                }
                iter.status().map_err(storage_error)?;
                let bytes = shared.encode(&identity, MAX_IDENTITY_BYTES as usize)?;
                let mut batch = WriteBatch::default();
                batch.put(POOL_KEY, bytes.as_slice());
                db.write_opt(batch, &sync_options())
                    .map_err(|_| recovery_unknown())?;
            }
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                db,
                identity,
                path: path.to_owned(),
                shared,
                recovery: Arc::new(AtomicBool::new(false)),
                groups: Arc::new(tokio::sync::Mutex::new(BTreeMap::new())),
                _pool_slot: pool_slot,
                _overhead: overhead,
            }),
        })
    }

    pub fn open_registered(
        _mount: VerifiedPoolMount,
        _identity: PoolIdentity,
        _shared: Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        Err(failure(
            ErrorCode::UnsupportedOperation,
            "registered volume verifier is not implemented",
        ))
    }
    pub fn identity(&self) -> &PoolIdentity {
        &self.inner.identity
    }
    pub fn resources(&self) -> &Arc<WorkerSharedResources> {
        &self.inner.shared
    }
    pub fn requires_recovery(&self) -> bool {
        self.inner.recovery.load(Ordering::Acquire)
    }
    fn healthy(&self) -> Result<(), ProtocolError> {
        if self.requires_recovery() {
            Err(recovery_unknown())
        } else {
            Ok(())
        }
    }
    fn durable_write(&self, batch: WriteBatch) -> Result<(), ProtocolError> {
        self.healthy()?;
        self.inner
            .db
            .write_opt(batch, &sync_options())
            .map_err(|_| {
                self.inner.recovery.store(true, Ordering::Release);
                recovery_unknown()
            })
    }
    pub async fn group(&self, identity: GroupIdentity) -> Result<GroupStore, ProtocolError> {
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

                groups.retain(|_, group| group.strong_count() > 0);
                if let Some(group) = groups.get(&identity.group_id).and_then(Weak::upgrade) {
                    if group.identity != identity {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "group identity differs from existing handle",
                        ));
                    }
                    return Ok(GroupStore { inner: group });
                }
                let slot = pool
                    .inner
                    .shared
                    .handle_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| capacity())?;
                let key = identity_key(identity.group_id);
                match pool.inner.db.get_pinned(&key).map_err(storage_error)? {
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
                    None => {
                        let value = pool
                            .inner
                            .shared
                            .encode(&identity, MAX_IDENTITY_BYTES as usize)?;
                        let mut batch = WriteBatch::default();
                        batch.put(&key, value.as_slice());
                        batch.put(group_key(identity.group_id, b'a', &[]), 0u64.to_be_bytes());
                        pool.durable_write(batch)?;
                    }
                }
                snapshot::recover_group(&pool, &identity)?;
                let group = Arc::new(GroupInner {
                    pool: pool.clone(),
                    identity,
                    order: Arc::new(tokio::sync::Mutex::new(())),
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
        let (mut scratch, _batch, output) = prepared.buffers(
            TransactionBudget {
                batch_bytes: 0,
                ..budget
            },
            output,
        )?;
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
                let snapshot = pool.inner.db.snapshot();
                let mut iter = snapshot.raw_iterator();
                let start = after
                    .map(|id| super::codec::end(&group_key(id, 0, &[])[..17]))
                    .unwrap_or_else(|| vec![0x40]);
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
                        .get_pinned(identity_key(id))
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
                    iter.seek(super::codec::end(&key[..17]));
                }
                iter.status().map_err(storage_error)?;
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
        self.mutate(Some(meta), false, budget, work).await
    }
    pub async fn log_transaction(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.mutate(None, true, budget, work).await
    }
    async fn mutate(
        &self,
        meta: Option<ApplyMeta>,
        log: bool,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
        + Send
        + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        let owner = self.inner.order.clone().lock_owned().await;
        let group = self.clone();
        let output = self
            .resources()
            .reply_budget(ExecutionClass::Control, budget.output_bytes)
            .await?;
        let prepared = self.resources().prepare(self.execution_class()).await?;
        let reservations = prepared.buffers(budget, output)?;
        prepared
            .execute(0, 0, move || {
                let _owner = owner;
                group.pool().healthy()?;
                let write_started = std::cell::Cell::new(false);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = group.pool().inner.db.snapshot();
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
                                .ok_or_else(recovery_unknown);
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
                        if let Some(membership) = &meta.membership {
                            transaction.put_meta(
                                group_key(group.identity().group_id, b'm', &[]),
                                membership.as_slice(),
                            )?;
                        }
                    }
                    write_started.set(true);
                    group.pool().durable_write(transaction.batch)?;
                    Ok(response)
                }));
                match result {
                    Ok(result) => result,
                    Err(_) => {
                        if write_started.get() {
                            group.pool().inner.recovery.store(true, Ordering::Release);
                        }
                        Err(recovery_unknown())
                    }
                }
            })
            .await
    }
    pub async fn read(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(false, self.execution_class(), budget, move |view| {
            let result = work(view)?;
            view.output.rehome(result)
        })
        .await
    }
    pub async fn log_read(
        &self,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<ChargedBytes, ProtocolError> + Send + 'static,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read_namespace(true, self.execution_class(), budget, move |view| {
            let result = work(view)?;
            view.output.rehome(result)
        })
        .await
    }
    async fn read_namespace<T: Send + 'static>(
        &self,
        log: bool,
        reply_class: ExecutionClass,
        budget: TransactionBudget,
        work: impl FnOnce(&mut GroupRead<'_>) -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let output = self
            .resources()
            .reply_budget(reply_class, budget.output_bytes)
            .await?;
        let prepared = self.resources().prepare(self.execution_class()).await?;
        let (scratch, _batch, output) = prepared.buffers(budget, output)?;
        let group = self.clone();
        prepared
            .execute(0, 0, move || {
                group.pool().healthy()?;
                let snapshot = group.pool().inner.db.snapshot();
                let prefix = group.prefix(&snapshot, log)?;
                let mut read = GroupRead {
                    snapshot: &snapshot,
                    prefix,
                    scratch,
                    output,
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
        self.read_namespace(true, ExecutionClass::Control, budget, move |view| {
            let result = work(view)?;
            view.output.rehome(result)
        })
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
            ExecutionClass::Control,
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
        self.read_namespace(false, ExecutionClass::Control, budget, move |view| {
            let mut result = group.read_applied(view.snapshot, &mut view.scratch)?;
            if let Some(state) = &mut result
                && let Some(membership) = state.membership.take()
            {
                state.membership = Some(view.output.rehome(membership)?);
            }
            Ok(result)
        })
        .await
    }
    fn read_applied(
        &self,
        snapshot: &Snapshot<'_>,
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
        snapshot: &Snapshot<'_>,
        tag: u8,
        reservation: &mut ByteReservation,
    ) -> Result<Option<ChargedBytes>, ProtocolError> {
        snapshot
            .get_pinned(group_key(self.identity().group_id, tag, &[]))
            .map_err(|_| recovery_unknown())?
            .map(|value| reservation.copy(&value))
            .transpose()
    }
    fn prefix(&self, snapshot: &Snapshot<'_>, log: bool) -> Result<Vec<u8>, ProtocolError> {
        if log {
            return Ok(group_key(self.identity().group_id, b'l', &[]));
        }
        let bytes = snapshot
            .get_pinned(group_key(self.identity().group_id, b'a', &[]))
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
    snapshot: &'a Snapshot<'a>,
    prefix: Vec<u8>,
    scratch: ByteReservation,
    output: ByteReservation,
}
impl GroupRead<'_> {
    /// Inspect a pinned value without copying a full log entry to a reply.
    /// The borrow ends with this callback; retain only explicitly owned output.
    pub fn inspect<T>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(Option<&[u8]>) -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        let key = self.key(key)?;
        let value = self
            .snapshot
            .get_pinned(key.as_slice())
            .map_err(storage_error)?;
        read(value.as_ref().map(|value| value.as_ref()))
    }
    /// Snapshot-only. Candidate writes are deliberately not consulted.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<ChargedBytes>, ProtocolError> {
        let key = self.key(key)?;
        self.snapshot
            .get_pinned(key.as_slice())
            .map_err(storage_error)?
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
    fn key(&mut self, key: &[u8]) -> Result<ChargedBytes, ProtocolError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(capacity());
        }
        self.scratch.join(&[&self.prefix, key])
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
        if max_items == 0 || max_items > 128 {
            return Err(capacity());
        }
        let lower = self.key(prefix)?;
        let start = self.key(after.unwrap_or(prefix))?;
        if !start.as_slice().starts_with(lower.as_slice()) {
            return Err(failure(
                ErrorCode::ValidationError,
                "scan cursor outside prefix",
            ));
        }
        let mut iterator = self.snapshot.raw_iterator();
        iterator.seek(start.as_slice());
        if after.is_some() && iterator.key() == Some(start.as_slice()) {
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
            if progress.visited == max_items {
                iterator.status().map_err(storage_error)?;
                return Ok(progress);
            }
            let value = iterator.value().ok_or_else(recovery_unknown)?;
            visit(&key[self.prefix.len()..], value)?;
            progress.last_key = Some(self.scratch.copy(&key[self.prefix.len()..])?);
            progress.visited += 1;
            iterator.next();
        }
        iterator.status().map_err(storage_error)?;
        progress.exhausted = true;
        Ok(progress)
    }
}
pub struct GroupTransaction<'a> {
    read: GroupRead<'a>,
    batch: WriteBatch,
    batch_used: usize,
    batch_limit: usize,
    _batch_charge: ByteReservation,
}
impl<'a> GroupTransaction<'a> {
    fn new(
        snapshot: &'a Snapshot<'a>,
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
            },
            batch: WriteBatch::with_capacity_bytes(budget.batch_bytes),
            batch_used: 12,
            batch_limit: budget.batch_bytes,
            _batch_charge: batch,
        }
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<ChargedBytes>, ProtocolError> {
        self.read.get(key)
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
        let key = self.read.key(key)?;
        self.put_meta(key.as_slice(), value)
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<(), ProtocolError> {
        let key = self.read.key(key)?;
        self.preflight(key.len(), 0)?;
        self.batch.delete(key.as_slice());
        Ok(())
    }
    fn put_meta(&mut self, key: impl AsRef<[u8]>, value: &[u8]) -> Result<(), ProtocolError> {
        self.preflight(key.as_ref().len(), value.len())?;
        self.batch.put(key, value);
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
        if bytes.len() as u64 > MAX_IDENTITY_BYTES
            || serde_json::from_slice::<PoolIdentity>(&bytes).ok().as_ref() != Some(identity)
        {
            return Err(storage_error_message());
        }
        return Ok(());
    }
    if fs::read_dir(path)
        .map_err(|_| storage_error_message())?
        .next()
        .is_some()
    {
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
