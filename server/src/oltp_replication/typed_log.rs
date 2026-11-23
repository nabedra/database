//! Typed Raft WAL backed by a hosted group's shared physical pool. Reads probe
//! bounded metadata, leave native/CPU capacity, reserve decode ownership, then
//! verify the same bytes before decoding. Streaming recovery owns one entry at
//! a time; a full-range API fails explicitly rather than allocating without bound.
//!
//! Entry bodies (`e‖index`) are journal values: the journal record holds the bytes
//! and the LSM a locator, so an entry is written to disk once and journal GC
//! recycles its segment after the purge. IDs, the tail and the purge marker stay
//! small keys; truncation and purge are range deletes.
use super::{
    entry::{CommandData, LeaderId, LogId, Membership, OwnedRaftEntry, Payload, Vote},
    owned::OwnedCommand,
    types::{Digest, GroupKind, ReplicaMember},
};
use crate::{
    oltp::{
        mvcc::{MAX_FIXED_COMMAND_BYTES, json_decode_budget},
        pool::{ByteClass, ExecutionClass, GroupStore, TransactionBudget, Transition},
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use futures_util::{Stream, StreamExt};
use openraft::{
    RaftLogReader, RaftTypeConfig,
    entry::RaftEntry,
    errors::LeaderChanged,
    storage::{IOFlushed, LeaderBoundedStreamError, LogState, RaftLogStorage},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use std::{
    fmt::Debug,
    io,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    pin::Pin,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::Mutex;

use super::hosted::transient::{HostRetry, RaftWrite};

pub trait TypedConfig:
    RaftTypeConfig<
        NodeId = u64,
        Node = ReplicaMember,
        Term = u64,
        LeaderId = LeaderId,
        Vote = Vote,
        Membership = Membership,
        MembershipEdit = openraft::impls::ChangeMembers<u64, ReplicaMember>,
        D = OwnedCommand<Self::Command>,
        Payload = Payload<Self::Command>,
        Entry = OwnedRaftEntry<Self::Command>,
    >
{
    type Command: CommandData;
    fn accepts(_kind: &GroupKind) -> bool {
        false
    }
    fn requires_index_reclamation(_command: &Self::Command) -> bool {
        false
    }
    const MAX_REPLY_DECODE_BYTES: usize = 256 << 10;
    type Reply: Serialize + DeserializeOwned + Clone + Debug + Send + Sync + 'static;
}

const ENTRY: u8 = b'e';
const ID: u8 = b'i';
const TAIL: &[u8] = b"tail";
const PURGED: &[u8] = b"purged";
const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 64 << 10,
    batch_bytes: 64 << 10,
    output_bytes: 16 << 10,
};
const MAX_COLLECTED: usize = 128;
const MAX_COLLECTED_BYTES: usize = 32 << 20;
/// One synced append and one replication read hold at most this many entries.
pub(crate) const MAX_APPEND_ENTRIES: usize = 64;
/// ... and at most this many encoded bytes, unless a single entry is larger.
pub(crate) const MAX_APPEND_BYTES: usize = 1 << 20;

/// Splits consecutive encoded entry sizes into synced write batches of at most
/// `MAX_APPEND_ENTRIES` entries and `MAX_APPEND_BYTES` bytes. An entry larger
/// than the byte bound forms its own batch.
pub(crate) fn append_chunks(sizes: &[usize]) -> Vec<std::ops::Range<usize>> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut bytes = 0usize;
    for (index, size) in sizes.iter().enumerate() {
        if index > start
            && (index - start == MAX_APPEND_ENTRIES
                || bytes.saturating_add(*size) > MAX_APPEND_BYTES)
        {
            chunks.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes = bytes.saturating_add(*size);
    }
    if start < sizes.len() {
        chunks.push(start..sizes.len());
    }
    chunks
}

pub struct TypedLogStore<C: TypedConfig> {
    store: GroupStore,
    writes: Arc<Mutex<()>>,
    _config: PhantomData<C>,
    pending: Arc<Pending>,
    /// Appended entries not yet in a synced write (PERF2d).
    overlay: Arc<overlay::Overlay<C::Command>>,
    resident: Option<super::multiplex::ReplicaLease>,
    node_id: u64,
    _writer: Arc<WriterState>,
    /// Appends waiting for their synced write (GC1).
    appends: Arc<AppendQueue<C>>,
    /// Shared with the host's state machine (RV-APPLY F1).
    retry: Arc<HostRetry>,
    #[cfg(test)]
    append_hook: Arc<std::sync::Mutex<Option<AppendHook>>>,
    /// Test hook: the next append fails before its write.
    #[cfg(test)]
    fail_append: Arc<std::sync::atomic::AtomicBool>,
}
impl<C: TypedConfig> Clone for TypedLogStore<C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            writes: self.writes.clone(),
            _config: PhantomData,
            pending: self.pending.clone(),
            overlay: self.overlay.clone(),
            resident: self.resident.clone(),
            node_id: self.node_id,
            _writer: self._writer.clone(),
            appends: self.appends.clone(),
            retry: self.retry.clone(),
            #[cfg(test)]
            append_hook: self.append_hook.clone(),
            #[cfg(test)]
            fail_append: self.fail_append.clone(),
        }
    }
}
#[cfg(test)]
type AppendHook = Arc<dyn Fn() + Send + Sync>;
#[derive(Default)]
struct WriterState {
    order: Arc<Mutex<()>>,
    pending: Arc<Pending>,
    /// The group's `overlay::Overlay<C::Command>`, shared by every handle.
    overlay: OnceLock<Arc<dyn std::any::Any + Send + Sync>>,
    /// The group's `AppendQueue<C>`, shared by every handle (GC1).
    appends: OnceLock<Arc<dyn std::any::Any + Send + Sync>>,
}

/// Test switch for GC1 measurements: off, every append is its own synced
/// write, as before GC1.
#[cfg(test)]
pub(crate) static COALESCE_APPENDS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Appends that took their place in the writer order and wait for their
/// synced write, in log order (GC1, group commit inside one group).
///
/// The append that gets the writer order first writes every append queued
/// behind it in one synced write, up to `MAX_APPEND_ENTRIES` entries, and
/// fires their `IOFlushed` callbacks in order after that write. The later
/// appends' tasks then find nothing left and return. This is ScyllaDB's
/// commitlog group commit: concurrent mutations share one segment write and
/// one sync. It changes no durability rule: each callback still fires only
/// after a synced write that holds all of its entries, and a crash still
/// leaves a consecutive durable prefix (one synced batch per chunk, in log
/// order).
///
/// A vote, truncation or purge starts a new epoch when it takes its place in
/// the writer order, and an append never shares a write with one from another
/// epoch, so those operations keep their submission order relative to the
/// appends (openraft orders vote and log IO).
struct AppendQueue<C: TypedConfig> {
    state: std::sync::Mutex<AppendQueueState<C>>,
}
struct AppendQueueState<C: TypedConfig> {
    epoch: u64,
    jobs: std::collections::VecDeque<AppendJob<C>>,
}
struct AppendJob<C: TypedConfig> {
    epoch: u64,
    batch: Arc<Vec<OwnedRaftEntry<C::Command>>>,
    callback: IOFlushed<C>,
    #[cfg(test)]
    hook: Option<AppendHook>,
}
impl<C: TypedConfig> AppendJob<C> {
    fn first(&self) -> u64 {
        self.batch[0].index()
    }
    fn last(&self) -> u64 {
        self.batch[self.batch.len() - 1].index()
    }
}
impl<C: TypedConfig> Default for AppendQueue<C> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(AppendQueueState {
                epoch: 0,
                jobs: std::collections::VecDeque::new(),
            }),
        }
    }
}
impl<C: TypedConfig> AppendQueue<C> {
    fn lock(&self) -> std::sync::MutexGuard<'_, AppendQueueState<C>> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    /// A vote, truncation or purge is about to take its writer place.
    fn barrier(&self) {
        let mut state = self.lock();
        state.epoch = state.epoch.wrapping_add(1);
    }
    fn push(&self, mut job: AppendJob<C>) -> u64 {
        let mut state = self.lock();
        job.epoch = state.epoch;
        state.jobs.push_back(job);
        state.epoch
    }
    /// Called while holding the writer order: the head append and the
    /// consecutive appends in the task's epoch, up to `MAX_APPEND_ENTRIES`
    /// entries. If an earlier task covered this append, a vote, truncate or
    /// purge may still be ahead of the next epoch's jobs.
    fn take(&self, epoch: u64) -> Vec<AppendJob<C>> {
        let mut state = self.lock();
        let mut taken: Vec<AppendJob<C>> = Vec::new();
        let mut entries = 0usize;
        while let Some(next) = state.jobs.front().filter(|job| job.epoch == epoch) {
            if let Some(previous) = taken.last() {
                #[cfg(test)]
                if !COALESCE_APPENDS.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                if previous.last().checked_add(1) != Some(next.first())
                    || entries + next.batch.len() > MAX_APPEND_ENTRIES
                {
                    break;
                }
            }
            entries += next.batch.len();
            taken.extend(state.jobs.pop_front());
        }
        taken
    }
}
#[derive(Default)]
struct Pending {
    count: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}
impl Pending {
    fn start(self: &Arc<Self>) -> PendingGuard {
        self.count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        PendingGuard(self.clone())
    }
}
struct PendingGuard(Arc<Pending>);
/// Spawns `task` as a pending operation. The guard is released only after the
/// task's future, and with it everything the task holds (the group store, its
/// charged entries, and through them the worker's resource owner), is dropped.
/// A caller that `drain`s and then drops its handles may therefore reopen the
/// worker's pools at once (KVfix4: a local binding of the guard dropped before
/// the task's captured store, so `bind_worker` met the old owner).
fn spawn_pending(pending: PendingGuard, task: impl Future<Output = ()> + Send + 'static) {
    nabedra_rt::task::spawn(async move {
        task.await;
        drop(pending);
    });
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0
            .count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl<C: TypedConfig> TypedLogStore<C> {
    fn byte_class(&self) -> ByteClass {
        match self.store.identity().kind {
            GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
                ByteClass::Progress
            }
            _ => ByteClass::Data,
        }
    }
    /// A durable node ID binds this replica's retained WAL. The pool/group
    /// identity already binds worker and pool; replacing the disk needs a new ID.
    pub async fn new(store: GroupStore, node_id: u64) -> io::Result<Self> {
        if node_id == 0 || !C::accepts(&store.identity().kind) {
            return Err(invalid("zero Raft node ID"));
        }
        type Registry = std::sync::Mutex<
            std::collections::BTreeMap<(uuid::Uuid, uuid::Uuid, uuid::Uuid), Weak<WriterState>>,
        >;
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        let writer = {
            let mut registry = REGISTRY
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| invalid("Raft writer registry unavailable"))?;
            registry.retain(|_, writer| writer.strong_count() != 0);
            let pool = store.pool().identity();
            let key = (pool.cluster_id, pool.pool_id, store.identity().group_id);
            registry
                .get(&key)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let writes = Arc::new(WriterState::default());
                    registry.insert(key, Arc::downgrade(&writes));
                    writes
                })
        };
        store
            .log_transaction_metadata(META, move |tx| {
                if let Some(existing) = tx.get(b"node")? {
                    if existing.as_slice() != node_id.to_be_bytes() {
                        return Err(protocol("foreign retained replica ID"));
                    }
                } else {
                    tx.put(b"node", &node_id.to_be_bytes())?;
                }
                Ok(Transition::Commit(tx.copy_output(b"")?))
            })
            .await
            .map_err(disk)?;
        let overlay = writer
            .overlay
            .get_or_init(|| Arc::new(overlay::Overlay::<C::Command>::default()))
            .clone()
            .downcast::<overlay::Overlay<C::Command>>()
            .map_err(|_| invalid("Raft writer overlay has another entry type"))?;
        let appends = writer
            .appends
            .get_or_init(|| Arc::new(AppendQueue::<C>::default()))
            .clone()
            .downcast::<AppendQueue<C>>()
            .map_err(|_| invalid("Raft writer queue has another config"))?;
        Ok(Self {
            store,
            writes: writer.order.clone(),
            _config: PhantomData,
            pending: writer.pending.clone(),
            overlay,
            resident: None,
            node_id,
            _writer: writer,
            appends,
            retry: Arc::default(),
            #[cfg(test)]
            append_hook: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            fail_append: Arc::default(),
        })
    }
    pub fn group(&self) -> &GroupStore {
        &self.store
    }
    pub(crate) fn with_resident(
        mut self,
        resident: super::multiplex::ReplicaLease,
    ) -> Result<Self, ProtocolError> {
        if resident.node_id() != self.node_id
            || resident.identity() != self.store.identity()
            || !resident.same_resources(self.store.resources())
        {
            return Err(protocol("Raft log residency belongs to another group"));
        }
        self.resident = Some(resident);
        Ok(self)
    }
    pub(crate) fn has_resident(&self) -> bool {
        self.resident.is_some()
    }
    pub(in crate::oltp_replication) fn set_retry(&mut self, retry: Arc<HostRetry>) {
        self.retry = retry;
    }
    pub async fn drain(&self) {
        loop {
            let changed = self.pending.changed.notified();
            if self
                .pending
                .count
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
            {
                return;
            }
            changed.await;
        }
    }
    async fn metadata<T: DeserializeOwned>(&self, key: &'static [u8]) -> io::Result<Option<T>> {
        let bytes = self
            .store
            .log_read_metadata(META, move |read| match read.get(key)? {
                Some(value) => read.copy_output(value.as_slice()),
                None => read.copy_output(b"null"),
            })
            .await
            .map_err(disk)?;
        serde_json::from_slice(bytes.as_slice()).map_err(|_| invalid("corrupt Raft metadata"))
    }
    async fn save<T: Serialize + Send + 'static>(
        &self,
        key: &'static [u8],
        value: T,
    ) -> io::Result<()> {
        self.store
            .log_transaction_metadata(META, move |tx| {
                let bytes = tx.encode_value(&value)?;
                tx.put(key, bytes.as_slice())?;
                Ok(Transition::Commit(tx.copy_output(b"")?))
            })
            .await
            .map_err(disk)?;
        Ok(())
    }
    /// The first entry in `start..end`. A pending entry (PERF2d) is served
    /// from the overlay, or with `durable` only once its synced write
    /// finished: applying an entry never runs ahead of this replica's log.
    async fn next(
        &self,
        start: u64,
        end: Option<u64>,
        durable: bool,
    ) -> io::Result<Option<(OwnedRaftEntry<C::Command>, usize)>> {
        if durable {
            let mut waited = None;
            loop {
                let settled = self.overlay.settled.notified();
                tokio::pin!(settled);
                settled.as_mut().enable();
                if !self.overlay.contains(start) {
                    break;
                }
                waited.get_or_insert_with(nabedra_rt::time::now_std);
                settled.await;
            }
            let Some(waited) = waited else {
                return self.next_stored(start, end).await;
            };
            crate::oltp_runtime::stage_timing::by_kind(
                &self.store.identity().kind,
                [
                    "raft.data.read_durable_wait",
                    "raft.ts.read_durable_wait",
                    "raft.meta.read_durable_wait",
                ],
            )
            .since(waited);
            let found = self.next_stored(start, end).await?;
            if found
                .as_ref()
                .is_none_or(|(entry, _)| entry.index() != start)
            {
                return Err(io::Error::other(
                    "Raft append failed before the entry became durable",
                ));
            }
            return Ok(found);
        }
        // Read the overlay first: an entry missing from it now is durable
        // or not appended yet, so the store read below cannot miss it.
        let pending = self.overlay.first_from(start, end);
        if let Some(entry) = pending.as_ref().filter(|entry| entry.index() == start) {
            return Ok(Some((entry.clone(), pending_bytes(entry))));
        }
        let stored = self.next_stored(start, end).await?;
        Ok(match (stored, pending) {
            (Some(stored), Some(pending)) if pending.index() < stored.0.index() => {
                let bytes = pending_bytes(&pending);
                Some((pending, bytes))
            }
            (None, Some(pending)) => {
                let bytes = pending_bytes(&pending);
                Some((pending, bytes))
            }
            (stored, _) => stored,
        })
    }
    async fn next_stored(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> io::Result<Option<(OwnedRaftEntry<C::Command>, usize)>> {
        #[derive(Serialize, Deserialize)]
        struct Probe {
            id: LogId,
            wire_bytes: usize,
            decode_bytes: usize,
            digest: Digest,
        }
        for _ in 0..8 {
            let probe_bytes = self
                .store
                .log_read_probe(META, move |read| {
                    let mut id = None;
                    let after = start.checked_sub(1).map(|index| index_key(ID, index));
                    read.scan_page(
                        &[ID],
                        after.as_ref().map(|key| &key[..]),
                        1,
                        |key, value| {
                            let found: LogId = serde_json::from_slice(value)
                                .map_err(|_| protocol("corrupt Raft entry ID"))?;
                            if key != index_key(ID, found.index) || found.index < start {
                                return Err(protocol("Raft entry index disagrees with ID"));
                            }
                            if end.is_none_or(|end| found.index < end) {
                                id = Some(found);
                            }
                            Ok(())
                        },
                    )?;
                    let probe = if let Some(id) = id {
                        read.inspect(&index_key(ENTRY, id.index), |bytes| {
                            let bytes =
                                bytes.ok_or_else(|| protocol("Raft entry ID without entry"))?;
                            if bytes.len() > MAX_FIXED_COMMAND_BYTES {
                                return Err(protocol("oversized retained Raft entry"));
                            }
                            Ok(Some(Probe {
                                id,
                                wire_bytes: bytes.len(),
                                decode_bytes: json_decode_budget(bytes)?
                                    .checked_add(4096)
                                    .ok_or_else(|| protocol("decode overflow"))?,
                                digest: Digest::from_bytes(Sha256::digest(bytes).into()),
                            }))
                        })?
                    } else {
                        None
                    };
                    read.encode(&probe)
                })
                .await
                .map_err(disk)?;
            let probe: Option<Probe> = serde_json::from_slice(probe_bytes.as_slice())
                .map_err(|_| invalid("corrupt Raft probe"))?;
            drop(probe_bytes);
            let Some(probe) = probe else { return Ok(None) };
            let owner = Arc::new(
                self.store
                    .resources()
                    .reserve_scratch_wait(self.byte_class(), probe.decode_bytes, probe.id.index)
                    .await
                    .map_err(disk)?,
            );
            let id = probe.id;
            let wire_bytes = probe.wire_bytes;
            let decoded = self
                .store
                .log_read_owned(
                    TransactionBudget {
                        scratch_bytes: wire_bytes
                            .checked_add(4096)
                            .ok_or_else(|| invalid("read overflow"))?,
                        batch_bytes: 0,
                        output_bytes: 0,
                    },
                    owner,
                    move |read, owner| {
                        let bytes = read.get(&index_key(ENTRY, id.index))?.ok_or_else(changed)?;
                        if bytes.len() != probe.wire_bytes
                            || Digest::from_bytes(Sha256::digest(bytes.as_slice()).into())
                                != probe.digest
                        {
                            return Err(changed());
                        }
                        OwnedCommand::<OwnedRaftEntry<C::Command>>::decode_with_owner(
                            &bytes,
                            owner.clone(),
                            owner.bytes(),
                        )
                    },
                )
                .await;
            match decoded {
                Ok(entry) => {
                    let entry = OwnedRaftEntry::from_owned(entry).map_err(disk)?;
                    if entry.log_id != id {
                        return Err(invalid("entry identity disagrees with its index"));
                    }
                    return Ok(Some((entry, wire_bytes)));
                }
                Err(error) if error.message == "uncommitted Raft range changed" => continue,
                Err(error) => return Err(disk(error)),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "Raft range kept changing during bounded decode",
        ))
    }
    async fn collect(
        &self,
        start: u64,
        end: Option<u64>,
        limited: bool,
    ) -> io::Result<Vec<OwnedRaftEntry<C::Command>>> {
        // A limited read feeds one replication payload, bounded like a synced
        // append. A full-range read keeps the larger recovery collection bound.
        let (max_entries, max_bytes) = if limited {
            (MAX_APPEND_ENTRIES, MAX_APPEND_BYTES)
        } else {
            (MAX_COLLECTED, MAX_COLLECTED_BYTES)
        };
        let mut out = Vec::with_capacity(max_entries);
        let mut cursor = start;
        let mut bytes = 0usize;
        while let Some((entry, size)) = self.next(cursor, end, false).await? {
            if out.len() == max_entries
                || (!out.is_empty() && bytes.checked_add(size).is_none_or(|n| n > max_bytes))
            {
                if limited {
                    break;
                }
                return Err(invalid(
                    "Raft full-range read exceeds bounded collection; use entries_stream",
                ));
            }
            cursor = entry.index().saturating_add(1);
            let final_entry = entry.index() == u64::MAX;
            bytes += size;
            out.push(entry);
            // Stop at the exclusive end without another native read (PERF2d:
            // a replication read of pending entries needs no store read).
            if final_entry || end.is_some_and(|end| cursor >= end) {
                break;
            }
        }
        Ok(out)
    }
    fn stream(
        &self,
        start: u64,
        end: Option<u64>,
        durable: bool,
    ) -> Pin<Box<dyn Stream<Item = io::Result<OwnedRaftEntry<C::Command>>> + Send>> {
        Box::pin(futures_util::stream::try_unfold(
            (self.clone(), Some(start)),
            move |(reader, cursor)| async move {
                let Some(cursor) = cursor else {
                    return Ok(None);
                };
                let Some((entry, _)) = reader.next(cursor, end, durable).await? else {
                    return Ok(None);
                };
                // Stop at the exclusive end without another read: OpenRaft
                // answers proposers while it consumes this stream, and the
                // apply must not need a native slot after the last answer.
                let next = entry
                    .index()
                    .checked_add(1)
                    .filter(|next| end.is_none_or(|end| *next < end));
                Ok(Some((entry, (reader, next))))
            },
        ))
    }
}
impl<C: TypedConfig> RaftLogReader<C> for TypedLogStore<C> {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> io::Result<Vec<OwnedRaftEntry<C::Command>>> {
        match bounds(&range) {
            Some((start, end)) => self.collect(start, end, false).await,
            None => Ok(Vec::new()),
        }
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> io::Result<Vec<OwnedRaftEntry<C::Command>>> {
        if start >= end {
            Ok(Vec::new())
        } else {
            self.collect(start, Some(end), true).await
        }
    }
    async fn entries_stream<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = io::Result<OwnedRaftEntry<C::Command>>> + Send {
        match bounds(&range) {
            Some((start, end)) => self.stream(start, end, true),
            None => Box::pin(futures_util::stream::empty()),
        }
    }
    async fn leader_bounded_stream<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        leader: LeaderId,
        range: R,
    ) -> impl Stream<Item = Result<OwnedRaftEntry<C::Command>, LeaderBoundedStreamError<C>>> + Send
    {
        let stream = match bounds(&range) {
            Some((start, end)) => self.stream(start, end, true),
            None => Box::pin(futures_util::stream::empty()),
        };
        futures_util::stream::try_unfold(
            (stream, self.clone(), leader),
            |(mut stream, mut reader, leader)| async move {
                let vote = reader.read_vote().await?;
                if vote
                    .as_ref()
                    .is_none_or(|vote| !vote.is_committed() || vote.leader_id() != &leader)
                {
                    return Err(LeaderBoundedStreamError::LeaderChanged(LeaderChanged::new(
                        leader, vote,
                    )));
                }
                let Some(entry) = stream.next().await else {
                    return Ok(None);
                };
                let entry = entry?;
                let after = reader.read_vote().await?;
                if after != vote {
                    return Err(LeaderBoundedStreamError::LeaderChanged(LeaderChanged::new(
                        leader, after,
                    )));
                }
                Ok(Some((entry, (stream, reader, leader))))
            },
        )
    }
    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        self.metadata(b"vote").await
    }
}
impl<C: TypedConfig> RaftLogStorage<C> for TypedLogStore<C> {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        // A core reads the log state when it starts. With no write pending,
        // it starts on the durable log, after an earlier failed append too.
        if self
            .pending
            .count
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            self.overlay.reset();
        }
        let bytes = self
            .store
            .log_read_metadata(META, |read| {
                let purged: Option<LogId> = read
                    .get(PURGED)?
                    .map(|v| {
                        serde_json::from_slice(v.as_slice())
                            .map_err(|_| protocol("corrupt purge marker"))
                    })
                    .transpose()?;
                let tail: Option<LogId> = read
                    .get(TAIL)?
                    .map(|v| {
                        serde_json::from_slice(v.as_slice())
                            .map_err(|_| protocol("corrupt tail marker"))
                    })
                    .transpose()?
                    .flatten();
                let tail = tail
                    .filter(|tail| purged.is_none_or(|purged| tail.index > purged.index))
                    .or(purged);
                read.encode(&(purged, tail))
            })
            .await
            .map_err(disk)?;
        let (last_purged_log_id, last_log_id) =
            serde_json::from_slice(bytes.as_slice()).map_err(|_| invalid("corrupt log state"))?;
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        let vote = *vote;
        // OpenRaft requires vote and log IO in submission order: a newer
        // vote's flush marks every earlier IO flushed, so it waits for the
        // appends still syncing (PERF2d). A queued commit marker lands
        // first, so this synced write persists it too.
        self.overlay.markers_written().await;
        self.appends.barrier();
        let _order = self.writes.lock().await;
        let (store, retry) = (&self.store, &self.retry);
        retry
            .retry(RaftWrite::Vote, move || async move {
                #[cfg(test)]
                if retry.take_vote_fault() {
                    return Err(crate::oltp::pool::no_space());
                }
                store
                    .log_transaction_metadata(META, move |tx| {
                        if let Some(old) = tx.get(b"vote")? {
                            let old: Vote = serde_json::from_slice(old.as_slice())
                                .map_err(|_| protocol("corrupt durable vote"))?;
                            if vote < old {
                                return Err(protocol("Raft vote moves backwards"));
                            }
                        }
                        let bytes = tx.encode_value(&vote)?;
                        tx.put(b"vote", bytes.as_slice())?;
                        Ok(Transition::Commit(tx.copy_output(b"")?))
                    })
                    .await
            })
            .await
            .map_err(disk)?;
        Ok(())
    }
    /// Unsynced: OpenRaft treats the commit marker as a recovery hint and
    /// raises it to the applied index on startup. The next synced write of
    /// this pool's WAL persists it.
    async fn save_committed(&mut self, id: Option<LogId>) -> io::Result<()> {
        let _timer = crate::oltp_runtime::stage_timing::by_kind(
            &self.store.identity().kind,
            [
                "raft.data.save_committed",
                "raft.ts.save_committed",
                "raft.meta.save_committed",
            ],
        )
        .timer();
        // RD3: the core saves the commit just before it hands the entries to
        // the apply worker; with `propose_to_first_ack` and
        // `propose_to_apply` this splits quorum -> commit -> apply.
        if id.is_some() && matches!(self.store.identity().kind, GroupKind::Data { .. }) {
            super::hosted::proposal_clock::mark(crate::oltp_runtime::stage_timing::perf_stage!(
                "raft.data.propose_to_commit"
            ));
        }
        // A quorum of other replicas can commit an entry before this replica's
        // own sync. Its writer records the marker after that sync (PERF2d).
        if id.is_some_and(|id| self.overlay.defer_commit(id)) {
            return Ok(());
        }
        // A durable entry's marker goes to the background writer, so the
        // core hands this commit to the apply worker at once (PERF2d item 2).
        if let Some(id) = id {
            if self.overlay.queue_marker(id) {
                let (store, retry, overlay) =
                    (self.store.clone(), self.retry.clone(), self.overlay.clone());
                let pending = self.pending.start();
                spawn_pending(pending, async move {
                    while let Some(id) = overlay.next_marker() {
                        match save_commit_marker(&store, &retry, Some(id), true).await {
                            Ok(()) => tracing::debug!(group=%store.identity().group_id,
                                index=id.index, "typed Raft durable commit saved"),
                            Err(error) => tracing::warn!(group=%store.identity().group_id,
                                code=?error.code, index=id.index, "Raft commit marker not saved"),
                        }
                    }
                });
            }
            return Ok(());
        }
        save_commit_marker(&self.store, &self.retry, id, false)
            .await
            .map_err(disk)?;
        if matches!(
            self.store.identity().kind,
            GroupKind::Data { .. } | GroupKind::Catalog
        ) {
            tracing::debug!(group=%self.store.identity().group_id, node=self.node_id,
                index=id.map(|id| id.index), "typed Raft durable commit saved");
        }
        Ok(())
    }
    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        self.overlay.markers_written().await;
        Ok(self
            .metadata::<Option<LogId>>(b"committed")
            .await?
            .flatten())
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
    where
        I: IntoIterator<Item = OwnedRaftEntry<C::Command>> + Send,
        I::IntoIter: Send,
    {
        let submit = nabedra_rt::time::now_std();
        // Own a bounded append before the first write. The typed host config
        // admits at most `MAX_APPEND_ENTRIES` entries per append; the writer
        // splits them into synced batches of at most `MAX_APPEND_BYTES`.
        // This is a transaction/frame bound, never a total log-size bound.
        let mut batch = Vec::new();
        let mut previous: Option<u64> = None;
        for entry in entries {
            if batch.len() == MAX_COLLECTED
                || previous.is_some_and(|n| n.checked_add(1) != Some(entry.index()))
            {
                let error = invalid("nonconsecutive or oversized Raft append batch");
                callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                return Err(error);
            }
            previous = Some(entry.index());
            let charged = match &self.resident {
                Some(resident) => {
                    self.retry
                        .or_stopped(entry.charge_resident_wait(resident))
                        .await
                }
                None => entry.charge_leader(self.store.resources(), self.byte_class()),
            };
            let entry = match charged {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::error!(group=%self.store.identity().group_id,kind=?self.store.identity().kind,stage="append_entry_metadata",progress_used=self.store.resources().buffer_bytes_used(ByteClass::Progress),resident_entry_used=self.resident.as_ref().map(|resident|resident.log_metadata_used()),code=?error.code,outcome=?error.outcome,"typed append metadata reservation failed");
                    let error = disk(error);
                    callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                    return Err(error);
                }
            };
            if batch.try_reserve_exact(1).is_err() {
                let error =
                    invalid("native append container allocation failed inside charged residency");
                callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                return Err(error);
            }
            batch.push(entry);
        }
        if batch.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }
        if self.overlay.failed() {
            // An earlier append of this core failed: a later one would leave a hole.
            let error = invalid("an earlier Raft append failed before its sync");
            callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
            return Err(error);
        }
        let batch = Arc::new(batch);
        let store = self.store.clone();
        let writer = self._writer.clone();
        let retry = self.retry.clone();
        let overlay = self.overlay.clone();
        let appends = self.appends.clone();
        // Dropping this method's awaiter cannot cancel the owned sync operation
        // or discard its completion callback. The host joins pending operations
        // before releasing replica residency and stopping its runtime.
        let pending = self.pending.start();
        #[cfg(test)]
        let append_hook = self.append_hook.lock().expect("append test hook").take();
        #[cfg(test)]
        let fail_append = self.fail_append.clone();
        // PERF2d: take this append's place in the writer order (Tokio's mutex
        // is FIFO from the first poll), publish the entries and return, so
        // replication reads them while the sync runs. Truncation, purge and
        // votes queue behind the sync; `IOFlushed` fires only after it.
        let mut order = Box::pin(self.writes.clone().lock_owned());
        let held = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match std::future::Future::poll(order.as_mut(), cx) {
                std::task::Poll::Ready(guard) => Some(guard),
                std::task::Poll::Pending => None,
            })
        })
        .await;
        overlay.publish(&batch);
        // GC1: queued in the same order as the writer place just taken. The
        // task that gets the order first writes this append and the ones
        // queued behind it together (see `AppendQueue`).
        let epoch = appends.push(AppendJob {
            epoch: 0,
            batch,
            callback,
            #[cfg(test)]
            hook: append_hook,
        });
        crate::oltp_runtime::stage_timing::by_kind(
            &self.store.identity().kind,
            [
                "raft.data.append_submit",
                "raft.ts.append_submit",
                "raft.meta.append_submit",
            ],
        )
        .since(submit);
        // KVfix4 + GC1: one guard per append, released only after this task
        // has dropped everything it captured (the store, the merged jobs'
        // entries, the worker's resource owner). A queued job needs no guard
        // of its own: it is pushed and its task spawned in this same
        // synchronous step, and only a running task (which still holds its
        // own guard) takes it, so `drain` waits for every job's write.
        spawn_pending(pending, async move {
            use crate::oltp_runtime::stage_timing::{by_kind, perf_stage};
            let kind = store.identity().kind.clone();
            let started = nabedra_rt::time::now_std();
            let data = matches!(kind, crate::oltp_replication::types::GroupKind::Data { .. });
            if data {
                super::hosted::proposal_clock::mark(perf_stage!("raft.data.propose_to_append"));
            }
            let _writer = writer;
            let _order = match held {
                Some(guard) => guard,
                None => order.await,
            };
            let jobs = appends.take(epoch);
            if jobs.is_empty() {
                // An earlier write held this append's entries and fired its
                // callback.
                return;
            }
            by_kind(
                &kind,
                [
                    "raft.data.append_order_wait",
                    "raft.ts.append_order_wait",
                    "raft.meta.append_order_wait",
                ],
            )
            .since(started);
            if jobs.len() > 1 {
                let merged = by_kind(
                    &kind,
                    [
                        "raft.data.append_merged",
                        "raft.ts.append_merged",
                        "raft.meta.append_merged",
                    ],
                );
                for _ in 1..jobs.len() {
                    merged.record(std::time::Duration::ZERO);
                }
            }
            let batch: Arc<Vec<OwnedRaftEntry<C::Command>>> = if jobs.len() == 1 {
                jobs[0].batch.clone()
            } else {
                Arc::new(
                    jobs.iter()
                        .flat_map(|job| job.batch.iter().cloned())
                        .collect(),
                )
            };
            #[cfg(test)]
            let append_hook = jobs.iter().find_map(|job| job.hook.clone());
            let ordered = nabedra_rt::time::now_std();
            let result: io::Result<()> = async {
                if overlay.failed() {
                    return Err(invalid("an earlier Raft append failed before its sync"));
                }
                let class = match store.identity().kind {
                    GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
                        ExecutionClass::Control
                    }
                    _ => ExecutionClass::Query,
                };
                let request = batch[0].index();
                // RD3: an unknown size keeps the native job.
                let estimate = batch
                    .iter()
                    .map(|entry| {
                        use crate::oltp_replication::owned::OwnedPayload;
                        entry.heap_bytes().unwrap_or(usize::MAX)
                    })
                    .fold(0usize, usize::saturating_add);
                let (batch, sizes) = super::hosted::execute_small(
                    store.resources(),
                    class,
                    request,
                    estimate,
                    move || {
                        struct Counter(usize);
                        impl io::Write for Counter {
                            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                                self.0 = self
                                    .0
                                    .checked_add(bytes.len())
                                    .filter(|n| *n <= MAX_FIXED_COMMAND_BYTES)
                                    .ok_or_else(|| invalid("oversized Raft append entry"))?;
                                Ok(bytes.len())
                            }
                            fn flush(&mut self) -> io::Result<()> {
                                Ok(())
                            }
                        }
                        let mut sizes = Vec::with_capacity(batch.len());
                        for entry in batch.iter() {
                            let mut counter = Counter(0);
                            serde_json::to_writer(&mut counter, entry)
                                .map_err(|_| protocol("oversized or invalid Raft append entry"))?;
                            sizes.push(counter.0);
                        }
                        Ok((batch, sizes))
                    },
                )
                .await
                .map_err(disk)?;
                let written = nabedra_rt::time::now_std();
                by_kind(
                    &kind,
                    [
                        "raft.data.append_encode",
                        "raft.ts.append_encode",
                        "raft.meta.append_encode",
                    ],
                )
                .since(ordered);
                #[cfg(test)]
                if fail_append.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    return Err(invalid("test append fault"));
                }
                // One synced WriteBatch per bounded chunk, in log order. A
                // crash between chunks leaves a consecutive durable prefix.
                for chunk in append_chunks(&sizes) {
                    let wire_bytes: usize = sizes[chunk.clone()].iter().sum();
                    // A full device voids the chunk: put the same entries again.
                    retry
                        .retry(RaftWrite::Append, || {
                            let batch = batch.clone();
                            let chunk = chunk.clone();
                            #[cfg(test)]
                            let append_hook = append_hook.clone();
                            store.log_transaction(
                                TransactionBudget {
                                    scratch_bytes: wire_bytes + (64 << 10),
                                    batch_bytes: wire_bytes + (64 << 10),
                                    output_bytes: 0,
                                },
                                move |tx| {
                                    #[cfg(test)]
                                    if let Some(hook) = append_hook {
                                        hook();
                                    }
                                    for entry in &batch[chunk] {
                                        entry.ensure_charged()?;
                                        let index = entry.index();
                                        let bytes = tx.encode_value(&entry)?;
                                        let id = tx.encode_value(&entry.log_id)?;
                                        // The entry body lives once, in the journal; the
                                        // LSM keeps a 32-byte locator (NS7).
                                        tx.put_journal(&index_key(ENTRY, index), bytes.as_slice())?;
                                        tx.put(&index_key(ID, index), id.as_slice())?;
                                        tx.put(TAIL, id.as_slice())?;
                                    }
                                    Ok(Transition::Commit(tx.copy_output(b"")?))
                                },
                            )
                        })
                        .await
                        .map_err(disk)?;
                }
                by_kind(
                    &kind,
                    [
                        "raft.data.append_write",
                        "raft.ts.append_write",
                        "raft.meta.append_write",
                    ],
                )
                .since(written);
                Ok(())
            }
            .await;
            by_kind(
                &kind,
                ["raft.data.append", "raft.ts.append", "raft.meta.append"],
            )
            .since(started);
            if data {
                super::hosted::proposal_clock::mark(perf_stage!("raft.data.propose_to_flush"));
            }
            if let Err(error) = &result {
                tracing::error!(group=%store.identity().group_id,kind=?store.identity().kind,io_kind=?error.kind(),error=%error,"typed durable log append failed before IOFlushed");
            }
            // Durable and visible in the store, or failed: leave the overlay
            // before reporting, so a flushed entry is always read back stored.
            // Callbacks fire in log order.
            let mut committed = None;
            for job in jobs {
                let AppendJob {
                    batch: job_batch,
                    callback,
                    ..
                } = job;
                let (first, last) = (job_batch[0].index(), job_batch[job_batch.len() - 1].index());
                if let Some(id) = overlay.settle(first, last, result.is_ok()) {
                    committed = Some(id);
                }
                callback.io_completed(
                    result.as_ref().map(|_| ()).map_err(|error: &io::Error| {
                        io::Error::new(error.kind(), error.to_string())
                    }),
                );
            }
            if let Some(id) = committed
                && let Err(error) = save_commit_marker(&store, &retry, Some(id), true).await
            {
                tracing::warn!(group=%store.identity().group_id,code=?error.code,"deferred Raft commit marker not saved");
            }
        });
        Ok(())
    }

    async fn truncate_after(&mut self, last: Option<LogId>) -> io::Result<()> {
        self.appends.barrier();
        let _order = self.writes.lock().await;
        self.retry
            .retry(RaftWrite::Truncate, || {
                self.store.log_transaction(META, move |tx| {
                    let purged: Option<LogId> = tx
                        .get(PURGED)?
                        .map(|v| {
                            serde_json::from_slice(v.as_slice())
                                .map_err(|_| protocol("corrupt purge marker"))
                        })
                        .transpose()?;
                    if let Some(last) = last {
                        if Some(last) != purged {
                            let retained = tx
                                .get(&index_key(ID, last.index))?
                                .ok_or_else(|| protocol("truncate boundary not retained"))?;
                            let id: LogId = serde_json::from_slice(retained.as_slice())
                                .map_err(|_| protocol("invalid truncate boundary"))?;
                            if id != last {
                                return Err(protocol("conflicting truncate boundary"));
                            }
                        }
                    } else if purged.is_some() {
                        return Err(protocol("truncate cannot erase purged committed prefix"));
                    }
                    if let Some(start) = last.map_or(Some(0), |id| id.index.checked_add(1)) {
                        tx.delete_range(&index_key(ENTRY, start), &[ENTRY + 1])?;
                        tx.delete_range(&index_key(ID, start), &[ID + 1])?;
                    }
                    let tail = tx.encode_value(&last)?;
                    tx.put(TAIL, tail.as_slice())?;
                    Ok(Transition::Commit(tx.copy_output(b"")?))
                })
            })
            .await
            .map_err(disk)?;
        Ok(())
    }
    async fn purge(&mut self, id: LogId) -> io::Result<()> {
        self.appends.barrier();
        let _order = self.writes.lock().await;
        // Application state may be unsynced. Never purge entries whose applied
        // state a crash could still lose: they are its only replay source.
        let store = &self.store;
        self.retry
            .retry(RaftWrite::Purge, move || async move {
                store.ensure_applied_durable(id.index).await?;
                store
                    .log_transaction(META, move |tx| {
                        let previous: Option<LogId> = tx
                            .get(PURGED)?
                            .map(|v| {
                                serde_json::from_slice(v.as_slice())
                                    .map_err(|_| protocol("corrupt purge marker"))
                            })
                            .transpose()?;
                        if previous.is_some_and(|previous| previous.index > id.index) {
                            return Err(protocol("purge moves backwards"));
                        }
                        // Delete only `(previous, id]`: the prefix up to `previous` is already
                        // gone, and re-covering it would stack ever longer range tombstones.
                        let start = match previous {
                            None => Some(0),
                            Some(previous) => previous.index.checked_add(1),
                        };
                        if let Some(start) = start.filter(|start| *start <= id.index) {
                            for prefix in [ENTRY, ID] {
                                let end = id
                                    .index
                                    .checked_add(1)
                                    .map(|n| index_key(prefix, n).to_vec())
                                    .unwrap_or_else(|| vec![prefix + 1]);
                                tx.delete_range(&index_key(prefix, start), &end)?;
                            }
                        }
                        let bytes = tx.encode_value(&id)?;
                        tx.put(PURGED, bytes.as_slice())?;
                        Ok(Transition::Commit(tx.copy_output(b"")?))
                    })
                    .await
            })
            .await
            .map_err(disk)?;
        Ok(())
    }
}
/// Unsynced: OpenRaft treats the commit marker as a recovery hint and raises
/// it to the applied index on startup. The next synced write of this pool's
/// WAL persists it. `catch_up` writes a deferred marker, which a newer one
/// may already cover.
async fn save_commit_marker(
    store: &GroupStore,
    retry: &HostRetry,
    id: Option<LogId>,
    catch_up: bool,
) -> Result<(), ProtocolError> {
    retry
        .retry(RaftWrite::Committed, || {
            store.log_transaction_metadata_unsynced(META, move |tx| {
                if let Some(raw) = tx.get(b"committed")? {
                    let old: Option<LogId> = serde_json::from_slice(raw.as_slice())
                        .map_err(|_| protocol("corrupt durable commit marker"))?;
                    if old.is_some_and(|old| id.is_none_or(|next| next.index < old.index)) {
                        if catch_up {
                            return Ok(Transition::Commit(tx.copy_output(b"")?));
                        }
                        return Err(protocol("commit marker moves backwards"));
                    }
                }
                if let Some(id) = id {
                    let exact = tx
                        .get(&index_key(ID, id.index))?
                        .or(tx.get(PURGED)?)
                        .ok_or_else(|| protocol("commit marker is not durably retained"))?;
                    let retained: LogId = serde_json::from_slice(exact.as_slice())
                        .map_err(|_| protocol("corrupt retained commit boundary"))?;
                    if retained != id {
                        return Err(protocol("commit marker does not match retained log"));
                    }
                }
                let bytes = tx.encode_value(&id)?;
                tx.put(b"committed", bytes.as_slice())?;
                Ok(Transition::Commit(tx.copy_output(b"")?))
            })
        })
        .await
        .map(|_| ())
}
/// The replication byte bound for a pending entry, whose encoded size is not
/// known before its write. The network encoder applies the exact bound.
fn pending_bytes<D: super::entry::CommandData>(entry: &OwnedRaftEntry<D>) -> usize {
    use crate::oltp_replication::owned::OwnedPayload;
    entry.heap_bytes().unwrap_or(0)
}
fn index_key(prefix: u8, index: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = prefix;
    key[1..].copy_from_slice(&index.to_be_bytes());
    key
}
fn bounds<R: RangeBounds<u64>>(range: &R) -> Option<(u64, Option<u64>)> {
    let start = match range.start_bound() {
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n.checked_add(1)?,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(n) => n.checked_add(1),
        Bound::Excluded(n) => Some(*n),
        Bound::Unbounded => None,
    };
    if end.is_some_and(|n| start >= n) {
        None
    } else {
        Some((start, end))
    }
}
fn disk(error: ProtocolError) -> io::Error {
    tracing::error!(code=?error.code,outcome=?error.outcome,"typed native log operation failed");
    io::Error::other(error.to_string())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn protocol(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::Unknown)
}
fn changed() -> ProtocolError {
    protocol("uncommitted Raft range changed")
}

mod overlay;
#[cfg(test)]
pub(crate) mod tests;
