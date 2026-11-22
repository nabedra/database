//! Actual typed OpenRaft application over shared pool groups. Application
//! effects, applied position, membership and last response use one batch per
//! entry. That batch is unsynced when the group's synced log shares its WAL:
//! a crash loses only a WAL suffix, and the surviving log replays it.
use super::{
    authority::consensus::{Applied, Consensus, ConsensusFuture, ReadBarrier},
    entry::{CommittedLeaderId, LogId, Payload, stored_membership},
    multiplex::{ReplicaLease, ReplicaMetadata},
    owned::{OwnedCommand, serialized_digest},
    typed_log::{MAX_APPEND_ENTRIES, TypedConfig, TypedLogStore},
    types::{AppliedPosition, Digest, GroupKind},
};
use crate::{
    oltp::pool::{
        ApplyMeta, ChargedBytes, ExecutionClass, GroupSnapshot, GroupSnapshotHeader, GroupStore,
        SnapshotCut, TransactionBudget, Transition,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use futures_util::{Stream, StreamExt};
use openraft::{
    OptionalSend, Raft, RaftSnapshotBuilder, ReadPolicy,
    errors::{ClientWriteError, RaftError},
    raft::ClientWriteResponse,
    storage::{EntryResponder, RaftStateMachine},
    type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
    vote::RaftLeaderId,
};
use sha2::{Digest as _, Sha256};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use transient::{HostRetry, RaftHealthStats, RaftWrite, transient};

mod disk_admission;

const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 128 << 10,
    output_bytes: 128 << 10,
};

pub(crate) trait TypedMachine<C: TypedConfig>: Clone + Send + Sync + 'static {
    fn store(&self) -> &GroupStore;
    fn installing_snapshot(&self) {}
    fn snapshot_installed(&self) -> impl Future<Output = Result<(), ProtocolError>> + Send {
        std::future::ready(Ok(()))
    }
    fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<C::Command>,
    ) -> impl Future<Output = Result<ChargedBytes, ProtocolError>> + Send;

    fn client_write(
        &self,
        raft: &HostRaft<C, Self>,
        command: OwnedCommand<C::Command>,
    ) -> impl Future<Output = Result<ClientWriteResponse<C>, ProtocolError>> + Send
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    {
        async move { raft.client_write(command).await.map_err(client_write_error) }
    }
}
impl TypedMachine<super::authority::config::DataConfig>
    for crate::oltp::mvcc::store::ParticipantMachine
{
    fn store(&self) -> &GroupStore {
        self.store()
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<crate::oltp::mvcc::store::ParticipantCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.apply(meta, command).await
    }
    async fn client_write(
        &self,
        raft: &HostRaft<super::authority::config::DataConfig, Self>,
        command: OwnedCommand<crate::oltp::mvcc::store::ParticipantCommand>,
    ) -> Result<ClientWriteResponse<super::authority::config::DataConfig>, ProtocolError> {
        disk_admission::client_write(raft, self.store(), command).await
    }
}
impl TypedMachine<super::authority::config::CatalogConfig>
    for crate::oltp::mvcc::store::ParticipantMachine
{
    fn store(&self) -> &GroupStore {
        self.store()
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<crate::oltp::mvcc::store::ParticipantCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.apply(meta, command).await
    }
}
#[derive(Clone)]
pub(crate) struct HostedState<C: TypedConfig, M: TypedMachine<C>> {
    machine: M,
    _resident: ReplicaLease,
    /// Shared with the host's log store (RV-APPLY F1).
    retry: Arc<HostRetry>,
    _config: std::marker::PhantomData<C>,
}
impl<C: TypedConfig, M: TypedMachine<C>> HostedState<C, M> {
    pub(crate) fn new(machine: M, resident: ReplicaLease) -> Result<Self, ProtocolError> {
        if !C::accepts(&machine.store().identity().kind)
            || machine.store().identity() != resident.identity()
            || !resident.same_resources(machine.store().resources())
            || machine.store().resources().config().reply_decode_bytes < C::MAX_REPLY_DECODE_BYTES
        {
            return Err(invalid("resident lease differs from typed state group"));
        }
        Ok(Self {
            machine,
            _resident: resident,
            retry: Arc::default(),
            _config: std::marker::PhantomData,
        })
    }
    pub(crate) fn store(&self) -> &GroupStore {
        self.machine.store()
    }
    pub(crate) fn resident(&self) -> ReplicaLease {
        self._resident.clone()
    }
    /// A received or installed snapshot file, checked against its header.
    pub(crate) async fn snapshot(
        &self,
        file: GroupSnapshot,
    ) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        let raw = self
            .store()
            .snapshot_membership(file.clone(), META)
            .await
            .map_err(disk)?;
        let header = file.header.clone();
        self.typed(
            &header,
            raw.as_ref().map(ChargedBytes::as_slice),
            SnapshotSource::File(Box::new(file)),
        )
        .await
    }
    /// SNAP1: the Raft snapshot of a cut; its metadata comes from the cut's
    /// header and membership, which one `nabedra-kv` view produced.
    async fn of_cut(&self, cut: SnapshotCut) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        let cut = Arc::new(cut);
        let header = cut.header.clone();
        self.typed(
            &header,
            cut.membership.as_ref().map(ChargedBytes::as_slice),
            SnapshotSource::Cut(cut.clone()),
        )
        .await
    }
    async fn typed(
        &self,
        header: &GroupSnapshotHeader,
        raw: Option<&[u8]>,
        source: SnapshotSource,
    ) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        // Waits for room: a refusal here would stop the core (RV-APPLY F3).
        // Every other holder releases on drop.
        let owner = Arc::new(
            self.retry
                .or_stopped(self._resident.metadata_wait(256 << 10))
                .await
                .map_err(disk)?,
        );
        let membership = match raw {
            Some(raw) => stored_membership(raw).map_err(disk)?,
            None => StoredMembershipOf::<C>::default(),
        };
        let meta = SnapshotMetaOf::<C> {
            last_log_id: header.applied.map(log_id),
            last_membership: membership,
        };
        Ok(openraft::Snapshot {
            meta,
            snapshot: TypedSnapshot {
                source,
                _owner: owner,
            },
        })
    }
    async fn builder(&self) -> TypedSnapshotBuilder<C, M> {
        // Capture the cut when OpenRaft asks for a builder, not when a later
        // background build polls it after application has advanced.
        let cut = self.cut().await.map_err(disk);
        TypedSnapshotBuilder {
            owner: self.clone(),
            cut: Some(cut),
        }
    }
    /// SNAP1: a policy build is a header-only cut. OpenRaft keeps only the
    /// metadata of a built snapshot (to purge the log up to it), so nothing
    /// is written and no view stays pinned; a follower that needs the state
    /// gets a fresh cut from [`RaftStateMachine::get_current_snapshot`].
    async fn cut(&self) -> Result<SnapshotCut, ProtocolError> {
        #[cfg(test)]
        if self.retry.take_export_fault() {
            return Err(crate::oltp::pool::no_space());
        }
        self.store().cut_snapshot(META, false).await
    }
}
/// Scratch for writing a cut's file: one 64 KiB write buffer per job.
const MATERIALIZE: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 0,
    output_bytes: 0,
};
#[derive(Clone)]
pub(crate) struct TypedSnapshot {
    source: SnapshotSource,
    _owner: Arc<ReplicaMetadata>,
}
#[derive(Clone)]
enum SnapshotSource {
    /// A received file, or one written from a cut elsewhere.
    File(Box<GroupSnapshot>),
    /// SNAP1: a cut; its file is written on first use.
    Cut(Arc<SnapshotCut>),
}
impl TypedSnapshot {
    /// The transferable file. A cut writes it here, on the caller's task: the
    /// replication task streaming it to a follower, never the state machine's
    /// task, so applies do not wait for it.
    pub(crate) async fn file(&self) -> Result<GroupSnapshot, ProtocolError> {
        match &self.source {
            SnapshotSource::File(file) => Ok((**file).clone()),
            SnapshotSource::Cut(cut) => cut.file(MATERIALIZE).await,
        }
    }
    #[cfg(test)]
    pub(crate) fn cut(&self) -> Option<&Arc<SnapshotCut>> {
        match &self.source {
            SnapshotSource::Cut(cut) => Some(cut),
            SnapshotSource::File(_) => None,
        }
    }
}
pub(crate) struct TypedSnapshotBuilder<C: TypedConfig, M: TypedMachine<C>> {
    owner: HostedState<C, M>,
    cut: Option<io::Result<SnapshotCut>>,
}
impl<C: TypedConfig, M: TypedMachine<C>> RaftSnapshotBuilder<C> for TypedSnapshotBuilder<C, M> {
    type SnapshotData = TypedSnapshot;
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        let cut = match self.cut.take() {
            Some(cut) => cut?,
            None => self.owner.cut().await.map_err(disk)?,
        };
        self.owner.of_cut(cut).await
    }
}
impl<C, M> RaftStateMachine<C> for HostedState<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    type SnapshotData = TypedSnapshot;
    type SnapshotBuilder = TypedSnapshotBuilder<C, M>;
    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, StoredMembershipOf<C>)> {
        let Some(applied) = self.store().applied_state(META).await.map_err(disk)? else {
            return Ok((None, StoredMembershipOf::<C>::default()));
        };
        let membership = match applied.membership {
            Some(raw) => stored_membership(raw.as_slice()).map_err(disk)?,
            None => StoredMembershipOf::<C>::default(),
        };
        Ok((Some(log_id(applied.position)), membership))
    }
    async fn apply<S>(&mut self, mut entries: S) -> io::Result<()>
    where
        S: Stream<Item = io::Result<EntryResponder<C>>> + Unpin + OptionalSend,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            if responder.is_some() && matches!(self.store().identity().kind, GroupKind::Data { .. })
            {
                proposal_clock::applying();
            }
            let _timer = crate::oltp_runtime::stage_timing::by_kind(
                &self.store().identity().kind,
                [
                    "raft.data.apply_entry",
                    "raft.ts.apply_entry",
                    "raft.meta.apply_entry",
                ],
            )
            .timer();
            entry.ensure_charged().map_err(disk)?;
            #[cfg(test)]
            if self.retry.take_apply_fault() {
                return Err(disk(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "test: injected apply failure",
                    Outcome::Unknown,
                )));
            }
            let position = position(entry.log_id);
            let class = execution_class(self.store());
            let shared = self.store().resources().clone();
            let inline = match entry.payload() {
                Payload::Normal(command) => bounded_digest(command, INLINE_BYTES),
                Payload::Membership(_) => None,
                Payload::Blank => Some(Ok(blank_digest())),
            };
            let digest_entry = entry.clone();
            let digest = match inline {
                Some(digest) => digest.map_err(disk)?,
                None => shared
                    .execute(class, position.index, 0, move || {
                        match digest_entry.payload() {
                            Payload::Normal(command) => serialized_digest(command),
                            Payload::Membership(membership) => {
                                serialized_digest(&StoredMembershipOf::<C>::new(
                                    Some(digest_entry.log_id),
                                    membership.clone(),
                                ))
                            }
                            Payload::Blank => Ok(blank_digest()),
                        }
                    })
                    .await
                    .map_err(disk)?,
            };
            let meta = ApplyMeta {
                position,
                command_digest: digest,
                membership: None,
            };
            let raw = match entry.payload() {
                // A transient failure voided the write; the replay check in
                // `GroupStore::mutate` makes a repeated entry idempotent (F1).
                Payload::Normal(command) => Some(
                    crate::oltp_runtime::stage_timing::timed(
                        crate::oltp_runtime::stage_timing::by_kind(
                            &self.store().identity().kind,
                            ["raft.data.apply_machine", "raft.ts.apply_machine", "raft.meta.apply_machine"],
                        ),
                        self.retry.retry(RaftWrite::Apply, || {
                            self.machine.apply(meta.clone(), command.clone())
                        }),
                    )
                    .await
                        .map_err(|error| {
                            tracing::error!(group=%self.store().identity().group_id,kind=?self.store().identity().kind,node=self._resident.node_id(),index=position.index,code=?error.code,outcome=?error.outcome,"typed state application failed");
                            disk(error)
                        })?,
                ),
                Payload::Membership(membership) => {
                    let store = self.store();
                    self.retry
                        .retry(RaftWrite::Apply, || {
                            store.transaction_with_membership(
                                meta.clone(),
                                META,
                                Some(StoredMembershipOf::<C>::new(
                                    Some(entry.log_id),
                                    membership.clone(),
                                )),
                                |tx| Ok(Transition::Commit(tx.copy_output(b"null")?)),
                            )
                        })
                        .await
                        .map_err(disk)?;
                    None
                }
                Payload::Blank => {
                    let store = self.store();
                    self.retry
                        .retry(RaftWrite::Apply, || {
                            store.transaction(meta.clone(), META, |tx| {
                                Ok(Transition::Commit(tx.copy_output(b"null")?))
                            })
                        })
                        .await
                        .map_err(disk)?;
                    None
                }
            };
            if matches!(
                self.store().identity().kind,
                GroupKind::Data { .. } | GroupKind::Catalog
            ) {
                tracing::debug!(group=%self.store().identity().group_id, node=self._resident.node_id(),
                    index=position.index, "typed Raft state applied");
            }
            if let Some(responder) = responder {
                // PERF2b: a small reply is measured and decoded on this
                // task; a large one still runs as native jobs.
                let small = raw.as_ref().is_none_or(|raw| raw.len() <= INLINE_BYTES);
                let required = match &raw {
                    Some(raw) if small => {
                        crate::oltp::mvcc::json_decode_budget(raw.as_slice()).map_err(disk)?
                    }
                    Some(raw) => {
                        let input = raw.clone();
                        shared
                            .execute(class, position.index, 0, move || {
                                crate::oltp::mvcc::json_decode_budget(input.as_slice())
                            })
                            .await
                            .map_err(disk)?
                    }
                    None => 1,
                };
                // The entry applied durably on every replica; only this
                // proposer's answer cannot be decoded within its budget. Drop
                // the responder (the caller sees an unknown outcome) and keep
                // applying instead of stopping the core (RV-APPLY F6).
                if required > C::MAX_REPLY_DECODE_BYTES {
                    tracing::error!(group=%self.store().identity().group_id, node=self._resident.node_id(),
                        index=position.index, required, limit=C::MAX_REPLY_DECODE_BYTES,
                        "typed durable response exceeds the validated decode budget; dropping the proposer's reply");
                    RaftHealthStats::bump(&self.retry.stats().replies_dropped);
                    drop(responder);
                    continue;
                }
                // Independent precharged reply-decode capacity is acquired
                // before native/CPU. Neither transport wire arena is needed.
                let owner = Arc::new(shared.reserve_reply_decode(required).await.map_err(disk)?);
                let decode = move || {
                    let reply = raw
                        .map(|raw| {
                            serde_json::from_slice::<C::Reply>(raw.as_slice())
                                .map_err(|_| invalid("corrupt typed durable response"))
                        })
                        .transpose()?;
                    Ok((reply, owner))
                };
                let (reply, owner) = if small {
                    decode().map_err(disk)?
                } else {
                    shared
                        .execute(class, position.index, 0, decode)
                        .await
                        .map_err(disk)?
                };
                responder.send(Applied::owned(position, reply, owner));
            }
        }
        Ok(())
    }
    /// RV-APPLY F2. OpenRaft builds policy snapshots with `force == false`;
    /// a transient export failure (a full device, capacity) defers the build
    /// instead of stopping the core, and builds pause for
    /// [`SNAPSHOT_DEFERRAL`]. Any other failure (corruption, a latched pool,
    /// quarantine) still reaches OpenRaft and stops the core. A forced build
    /// retries transient failures.
    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        if !force && self.retry.snapshots_deferred() {
            RaftHealthStats::bump(&self.retry.stats().snapshot_builds_deferred);
            return None;
        }
        let cut = if force {
            self.retry
                .retry(RaftWrite::BuildSnapshot, || self.cut())
                .await
        } else {
            self.cut().await
        };
        match cut {
            Err(error) if !force && transient(&error).is_some() && !self.retry.is_stopping() => {
                tracing::warn!(group=%self.store().identity().group_id, node=self._resident.node_id(),
                    code=?error.code, %error, "typed snapshot build deferred after a transient error");
                RaftHealthStats::bump(&self.retry.stats().snapshot_builds_deferred);
                self.retry.defer_snapshots(SNAPSHOT_DEFERRAL);
                None
            }
            cut => Some(TypedSnapshotBuilder {
                owner: self.clone(),
                cut: Some(cut.map_err(disk)),
            }),
        }
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.builder().await
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<C>,
        snapshot: TypedSnapshot,
    ) -> io::Result<()> {
        let file = snapshot.file().await.map_err(disk)?;
        let checked = self.snapshot(file.clone()).await?;
        if checked.meta != *meta {
            return Err(io::Error::other(
                "snapshot bytes differ from typed Raft metadata",
            ));
        }
        let store = self.store();
        self.machine.installing_snapshot();
        self.retry
            .retry(RaftWrite::InstallSnapshot, || {
                store.install_snapshot(file.clone(), META)
            })
            .await
            .map_err(disk)?;
        self.machine.snapshot_installed().await.map_err(disk)
    }
    /// SNAP1: a fresh cut of the current state, which covers every purged
    /// index (a purge never passes the applied one). It pins its view until
    /// the replication task that asked for it writes the file, or drops it.
    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf<C, TypedSnapshot>>> {
        let store = self.store();
        let cut = self
            .retry
            .retry(RaftWrite::BuildSnapshot, || store.cut_snapshot(META, true))
            .await
            .map_err(disk)?;
        if cut.header.applied.is_none() {
            return Ok(None);
        }
        self.of_cut(cut).await.map(Some)
    }
}

/// Entries and replies up to this many encoded bytes are hashed or decoded
/// on the applying task instead of in a native job (PERF2b). That is a few
/// microseconds of bounded CPU, against a cross-thread handoff of 0.3-0.5 ms
/// on a loaded host. Larger ones keep their native jobs.
const INLINE_BYTES: usize = 4 << 10;

fn blank_digest() -> Digest {
    Digest::from_bytes(Sha256::digest(b"nabedra/typed-raft-blank/v1").into())
}

/// `serialized_digest` of `value` if its encoding fits in `limit` bytes:
/// the serialization stops at the limit, so at most `limit` bytes are
/// hashed here. `None` asks the caller for the native job.
fn bounded_digest(
    value: &impl serde::Serialize,
    limit: usize,
) -> Option<Result<Digest, ProtocolError>> {
    struct Writer {
        hash: Sha256,
        left: usize,
        over: bool,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.left {
                self.over = true;
                return Err(io::Error::other("inline digest limit"));
            }
            self.left -= bytes.len();
            self.hash.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        hash: Sha256::new(),
        left: limit,
        over: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Some(Ok(Digest::from_bytes(writer.hash.finalize().into()))),
        Err(_) if writer.over => None,
        Err(_) => Some(Err(invalid("typed Raft entry does not serialize"))),
    }
}

/// SNAP1: the snapshot policy of the production Raft profiles, set
/// explicitly. A policy build is now a header-only cut (a sync and three point
/// reads under the application order), so the interval no longer trades apply
/// stalls for log length; it only sets how often the log is purged. The values
/// stay OpenRaft's defaults: a group retains 1,000-6,000 entries, as every
/// existing recovery, move and catch-up scenario was validated against, and a
/// cut every 5,000 entries costs one sync and one purge write.
pub(crate) const SNAPSHOT_LOGS_SINCE_LAST: u64 = 5000;
pub(crate) const SNAPSHOT_LOGS_TO_KEEP: u64 = 1000;
/// How long policy snapshot builds pause after a transient failure (F2).
const SNAPSHOT_DEFERRAL: Duration = Duration::from_secs(1);
/// Cores one host can run over its lifetime: the first and the supervisor's
/// restarts (RV-APPLY F4).
pub(crate) const MAX_CORES: usize = 8;
type HostRaft<C, M> = Raft<C, HostedState<C, M>>;
type Respawn<C, M> = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<HostRaft<C, M>, ProtocolError>> + Send>>
        + Send
        + Sync,
>;
/// The current OpenRaft core of a host. It dereferences to that core, so
/// callers and the replica's network endpoint follow a supervisor restart
/// (F4) without holding a stale handle. Earlier cores stay allocated (stopped)
/// until the host drops; there are at most [`MAX_CORES`].
pub(crate) struct RaftCores<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    cores: [OnceLock<HostRaft<C, M>>; MAX_CORES],
    current: AtomicUsize,
    /// The current core's index, published after each replacement so
    /// watchers of a core's metrics follow a restart (RV-FINAL H1).
    generation: tokio::sync::watch::Sender<usize>,
}
impl<C, M> RaftCores<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn new(raft: HostRaft<C, M>) -> Self {
        let cores: [OnceLock<HostRaft<C, M>>; MAX_CORES] = std::array::from_fn(|_| OnceLock::new());
        let _ = cores[0].set(raft);
        Self {
            cores,
            current: AtomicUsize::new(0),
            generation: tokio::sync::watch::Sender::new(0),
        }
    }
    /// How many times this host's core was replaced.
    pub(crate) fn restarts(&self) -> usize {
        self.current.load(Ordering::Acquire)
    }
    fn exhausted(&self) -> bool {
        self.restarts() + 1 >= MAX_CORES
    }
    fn replace(&self, raft: HostRaft<C, M>) -> Result<(), HostRaft<C, M>> {
        let next = self.restarts() + 1;
        let Some(slot) = self.cores.get(next) else {
            return Err(raft);
        };
        slot.set(raft)?;
        self.current.store(next, Ordering::Release);
        self.generation.send_replace(next);
        Ok(())
    }
    /// The current core's index and that same core's metrics.
    pub(crate) fn current_metrics(
        &self,
    ) -> (
        usize,
        openraft::type_config::alias::WatchReceiverOf<C, openraft::RaftMetrics<C>>,
    ) {
        let index = self.current.load(Ordering::Acquire);
        let core = self.cores[index]
            .get()
            .expect("the current Raft core is set before it is published");
        (index, core.metrics())
    }
    /// Changes to the current core's index. Closes when the host drops.
    pub(crate) fn subscribe_restarts(&self) -> tokio::sync::watch::Receiver<usize> {
        self.generation.subscribe()
    }
    /// The current core has not stopped (`RaftMetrics::running_state`).
    pub(crate) fn is_running(&self) -> bool {
        use openraft::type_config::async_runtime::watch::WatchReceiver;
        self.metrics().borrow_watched().running_state.is_ok()
    }
}
impl<C, M> std::ops::Deref for RaftCores<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    type Target = HostRaft<C, M>;
    fn deref(&self) -> &Self::Target {
        self.cores[self.current.load(Ordering::Acquire)]
            .get()
            .expect("the current Raft core is set before it is published")
    }
}

/// Constructed only by the hosted-replica manager after validating placement,
/// permanent node identity, membership and the matching resident lease.
pub(crate) struct RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(crate) raft: Arc<RaftCores<C, M>>,
    pub(crate) log: TypedLogStore<C>,
    pub(crate) state: HostedState<C, M>,
    pub(in crate::oltp_replication) membership_changes: tokio::sync::Mutex<()>,
    pub(super) peer_features: OnceLock<super::multiplex::WorkerClient>,
    /// Guards the single direct-leadership watcher of a Data replica. The
    /// watcher follows every core of the host (RV-FINAL H1).
    pub(in crate::oltp_replication) direct_watch: std::sync::Once,
    /// This host's claim on deferred application sync. Released on shutdown,
    /// or when the host drops (RV-T2 Q5); clears only this host's claim.
    deferred_applies: std::sync::Mutex<Option<crate::oltp::pool::DeferredApplies>>,
    retry: Arc<HostRetry>,
    respawn: Respawn<C, M>,
    restarts: tokio::sync::Mutex<()>,
    closed: AtomicBool,
}
/// Entries per AppendEntries RPC. A peer whose `nabedra-otp/2` session
/// negotiated multi-entry appends receives up to this many entries (and about
/// 1 MiB) per RPC, which its log writes as synced batches. The typed network
/// sends one entry per RPC to an older one-shot peer.
pub(crate) const REPLICATION_PAYLOAD_ENTRIES: u64 = MAX_APPEND_ENTRIES as u64;
/// Byte-bounded batching. Client writes coalesce into one log append of at
/// most `MAX_APPEND_ENTRIES` entries without lingering, and the log writes
/// each append as synced batches of at most 1 MiB. Bounds transient batches
/// independently of total retained log/table size.
pub(crate) fn batched_config(
    mut config: openraft::Config,
) -> Result<Arc<openraft::Config>, ProtocolError> {
    config.max_payload_entries = REPLICATION_PAYLOAD_ENTRIES;
    config.max_append_entries = Some(MAX_APPEND_ENTRIES as u64);
    config.api_batch_capacity = Some(MAX_APPEND_ENTRIES as u64);
    config.api_channel_size = Some(64);
    config.api_batch_linger_ms = Some(0);
    // RD1: lease reads (Timestamp `AllocateDirect`, direct gets and the
    // direct leader's reads) check the lease on the caller's task. Only
    // `ReadPolicy::LeaseRead` uses it; ReadIndex keeps the quorum round.
    config.enable_local_lease_read = Some(true);
    config
        .validate()
        .map(Arc::new)
        .map_err(|_| invalid("invalid typed Raft configuration"))
}
impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    /// Production passes the authenticated `TypedNetworkFactory`; a
    /// single-voter test may pass a network that is never called.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn start<N>(
        config: Arc<openraft::Config>,
        network: N,
        log: TypedLogStore<C>,
        state: HostedState<C, M>,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        N: openraft::network::RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
        N::Network: openraft::network::NetSnapshot<C, SnapshotData = TypedSnapshot>,
    {
        Self::start_with(config, network, log, state, Arc::default()).await
    }
    /// [`Self::start`] counting into the worker's `stats` (`/v1/runtime/stats`).
    pub(crate) async fn start_with<N>(
        config: Arc<openraft::Config>,
        network: N,
        mut log: TypedLogStore<C>,
        mut state: HostedState<C, M>,
        stats: Arc<RaftHealthStats>,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        N: openraft::network::RaftNetworkFactory<C> + Clone + Send + Sync + 'static,
        N::Network: openraft::network::NetSnapshot<C, SnapshotData = TypedSnapshot>,
    {
        if config.max_payload_entries != REPLICATION_PAYLOAD_ENTRIES
            || config.max_append_entries != Some(MAX_APPEND_ENTRIES as u64)
            || config.api_batch_capacity != Some(MAX_APPEND_ENTRIES as u64)
            || config.api_batch_linger_ms != Some(0)
            || !log.has_resident()
            || state.store().identity() != log.group().identity()
            || !Arc::ptr_eq(state.store().resources(), log.group().resources())
        {
            return Err(invalid(
                "typed host requires matching group/pool and validated bounded batching",
            ));
        }
        // Before Raft can replay or apply anything: unsynced application is
        // safe only when the log shares this state's WAL. Otherwise every
        // application write stays synced.
        // The token lives in the host. If `Raft::new` fails it drops here, so no
        // host that never started leaves deferral on (RV-NS6 F6).
        let deferred_applies = state.store().defer_apply_sync(log.group());
        if deferred_applies.is_none() {
            tracing::warn!(group=%state.store().identity().group_id,
                "typed Raft log and state use different pools; application stays synced");
        }
        let retry = Arc::new(HostRetry::new(stats));
        log.set_retry(retry.clone());
        state.retry = retry.clone();
        // Every core of this host runs on this same log and state (F4).
        let respawn: Respawn<C, M> = {
            let node = state._resident.node_id();
            let (log, state) = (log.clone(), state.clone());
            Box::new(move || {
                let (config, network) = (config.clone(), network.clone());
                let (log, state) = (log.clone(), state.clone());
                Box::pin(async move {
                    Raft::new(node, config, network, log, state)
                        .await
                        .map_err(|_| unavailable())
                })
            })
        };
        let raft = respawn().await?;
        Ok(Arc::new(Self {
            raft: Arc::new(RaftCores::new(raft)),
            log,
            state,
            membership_changes: tokio::sync::Mutex::new(()),
            peer_features: OnceLock::new(),
            direct_watch: std::sync::Once::new(),
            deferred_applies: std::sync::Mutex::new(deferred_applies),
            retry,
            respawn,
            restarts: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
        }))
    }
    /// This host's retry and health counters.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn health(&self) -> &Arc<RaftHealthStats> {
        self.retry.stats()
    }
    #[cfg(test)]
    pub(crate) fn retry(&self) -> &Arc<HostRetry> {
        &self.retry
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    /// RV-APPLY F4: runs a stopped core again on the same log and state. The
    /// old core's storage loops end first, so it never writes beside the new
    /// one. No-op while the current core runs; fails after shutdown or once
    /// the host used all [`MAX_CORES`].
    pub(crate) async fn restart(&self) -> Result<(), ProtocolError> {
        let _restart = self.restarts.lock().await;
        if self.is_closed() || self.raft.exhausted() {
            return Err(unavailable());
        }
        if self.raft.is_running() {
            return Ok(());
        }
        self.retry.stop();
        let _ = self.raft.shutdown().await;
        self.log.drain().await;
        if self.is_closed() {
            return Err(unavailable());
        }
        self.retry.resume();
        let raft = (self.respawn)().await?;
        if let Err(raft) = self.raft.replace(raft) {
            self.retry.stop();
            let _ = raft.shutdown().await;
            return Err(unavailable());
        }
        tracing::warn!(group=%self.state.store().identity().group_id, node=self.state._resident.node_id(),
            restarts=self.raft.restarts(), "typed Raft core restarted on its log and state");
        Ok(())
    }
    pub(crate) async fn shutdown(&self) -> Result<(), ProtocolError> {
        // Storage retries and waits end before OpenRaft is asked to stop, so a
        // core blocked in storage never holds shutdown up (F1). A restart in
        // progress finishes first; the second stop covers what it resumed.
        self.closed.store(true, Ordering::Release);
        self.retry.stop();
        let _restart = self.restarts.lock().await;
        self.retry.stop();
        let stopped = self.raft.shutdown().await;
        self.log.drain().await;
        // No running log replays unsynced application any more (RV-T2 Q5).
        drop(
            self.deferred_applies
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take(),
        );
        stopped.map_err(|_| unavailable())
    }
}
/// RD2 breakdown of a Data `client_write` on its leader: the time from the
/// latest Data proposal on this process to the leader's log append start and
/// flush (`raft.data.propose_to_append`, `propose_to_flush`), to sending and
/// acknowledging the AppendEntries that carries it (`propose_to_send`,
/// `propose_to_ack`), and to the apply of an entry this replica proposed
/// (`propose_to_apply`). Exact only while one proposal is in flight (the
/// serial benchmark); a diagnostic, not a per-entry measurement.
pub(crate) mod proposal_clock {
    use std::{
        sync::{
            OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };
    static LAST: AtomicU64 = AtomicU64::new(0);
    fn now_us() -> u64 {
        static BASE: OnceLock<Instant> = OnceLock::new();
        BASE.get_or_init(Instant::now).elapsed().as_micros() as u64 + 1
    }
    pub(super) fn proposed() {
        LAST.store(now_us(), Ordering::Relaxed);
    }
    pub(super) fn applying() {
        mark(crate::oltp_runtime::stage_timing::perf_stage!(
            "raft.data.propose_to_apply"
        ));
    }
    /// Records the time since the latest Data proposal in `stage`.
    pub(crate) fn mark(stage: &'static crate::oltp_runtime::stage_timing::Stage) {
        let last = LAST.load(Ordering::Relaxed);
        if last != 0 {
            stage.record(Duration::from_micros(now_us().saturating_sub(last)));
        }
    }
}

impl<C, M> Consensus<C::Command, C::Reply> for RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn propose(&self, command: OwnedCommand<C::Command>) -> ConsensusFuture<'_, Applied<C::Reply>> {
        Box::pin(async move {
            command.ensure_charged()?;
            let _membership = if C::requires_index_reclamation(command.payload()?) {
                let guard = self.membership_changes.lock().await;
                self.check_index_reclamation_voters()?;
                Some(guard)
            } else {
                None
            };
            // A full device: nothing would commit until space frees up, so
            // the leader says so before proposing (RV-APPLY F1).
            if self.retry.out_of_space() {
                RaftHealthStats::bump(&self.retry.stats().proposals_refused_no_space);
                return Err(crate::oltp::pool::no_space());
            }
            // Reserve before client_write: saturation is a typed pre-proposal
            // outcome, never a fatal WriteLogs error after Raft accepts it.
            command.reserve_raft_metadata(
                &self.state._resident,
                super::entry::OwnedRaftEntry::<C::Command>::normal_metadata_bytes(),
            )?;
            let kind = self.state.store().identity().kind.clone();
            if matches!(kind, GroupKind::Data { .. }) {
                proposal_clock::proposed();
            }
            let _timer = crate::oltp_runtime::stage_timing::by_kind(
                &kind,
                [
                    "raft.data.client_write",
                    "raft.ts.client_write",
                    "raft.meta.client_write",
                ],
            )
            .timer();
            let response = self.state.machine.client_write(&self.raft, command).await?;
            let (position, reply, owner) = response.data.into_parts()?;
            Ok(Applied::owned(
                position,
                reply.ok_or_else(unavailable)?,
                owner,
            ))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(self.read_barrier(ReadPolicy::ReadIndex))
    }
    fn ensure_linearizable_lease(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async move {
            match self.read_barrier(ReadPolicy::LeaseRead).await {
                Ok(barrier) => Ok(barrier),
                // A stale lease (or any lease failure) falls back to a
                // quorum round trip.
                Err(_) => self.read_barrier(ReadPolicy::ReadIndex).await,
            }
        })
    }
    fn lease_read_index(&self) -> ConsensusFuture<'_, u64> {
        Box::pin(async move {
            match self.read_index(ReadPolicy::LeaseRead).await {
                Ok(index) => Ok(index),
                Err(_) => self.read_index(ReadPolicy::ReadIndex).await,
            }
        })
    }
    fn lease_read_position(&self) -> ConsensusFuture<'_, AppliedPosition> {
        Box::pin(async move {
            match self.read_position(ReadPolicy::LeaseRead).await {
                Ok(position) => Ok(position),
                Err(_) => self.read_position(ReadPolicy::ReadIndex).await,
            }
        })
    }
    fn current_leader(&self) -> Option<u64> {
        use openraft::type_config::async_runtime::watch::WatchReceiver;
        // A stopped core's metrics may still name a leader (RV-APPLY F4).
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        metrics
            .running_state
            .is_ok()
            .then_some(metrics.current_leader)
            .flatten()
    }
    fn local_node(&self) -> Option<u64> {
        Some(self.state._resident.node_id())
    }
    fn applied_index(&self) -> Option<u64> {
        use openraft::type_config::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        if metrics.running_state.is_err() {
            return None;
        }
        metrics.last_applied.as_ref().map(|id| id.index)
    }
}

impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn check_index_reclamation_voters(&self) -> Result<(), ProtocolError> {
        use super::multiplex::{PeerFeature, TrafficClass};
        use openraft::type_config::async_runtime::watch::WatchReceiver;

        let local = self.state.resident().node_id();
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        if metrics.current_leader != Some(local)
            || metrics.membership_config != metrics.committed_membership_config
        {
            return Err(unavailable());
        }
        let membership = metrics.membership_config.membership();
        if membership.nodes().any(|(node, member)| {
            *node != local
                && self.peer_features.get().is_none_or(|client| {
                    [TrafficClass::RaftProgress, TrafficClass::DataConsensus]
                        .into_iter()
                        .all(|class| {
                            client
                                .peer_supports(
                                    member.worker_id,
                                    class,
                                    PeerFeature::IndexReclamation,
                                )
                                .is_none()
                        })
                })
        }) {
            return Err(unavailable());
        }
        Ok(())
    }
}
impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    /// RD1: `policy`'s linearizer once this replica applied its read index.
    ///
    /// A lease read checks the leader lease on the calling task, against the
    /// leader state openraft publishes (`enable_local_lease_read`, set in
    /// [`batched_config`]), with no round trip through the RaftCore task. The
    /// lease and its length are the core path's: openraft publishes the
    /// committed index before it dispatches the apply, and clears the state
    /// before the node gives up leadership. A missing or expired lease takes
    /// the core path, which refuses a stale lease; ReadIndex always confirms
    /// through the core with a quorum round. Both then wait until this
    /// replica's state machine applied the read index.
    async fn linearized(
        &self,
        policy: ReadPolicy,
    ) -> Result<openraft::raft::linearizable_read::LinearizeState<C>, ProtocolError> {
        let lease = matches!(policy, ReadPolicy::LeaseRead);
        let failed = |error: &dyn std::fmt::Display| {
            tracing::warn!(group=%self.state.store().identity().group_id,kind=?self.state.store().identity().kind,node=self.state._resident.node_id(),error=%error,lease,"typed linearizable read failed");
            unavailable()
        };
        let raft: &HostRaft<C, M> = &self.raft;
        let linearizer = raft
            .get_local_read_linearizer(policy)
            .await
            .map_err(|error| failed(&error))?;
        linearizer
            .await_ready(raft)
            .await
            .map_err(|error| failed(&error))
    }
    /// Counts one linearizable read of `policy` and times it under `lease`
    /// or `index` (data, timestamp, metadata).
    fn read_timer(
        &self,
        policy: &ReadPolicy,
        lease: [&'static str; 3],
        index: [&'static str; 3],
    ) -> crate::oltp_runtime::stage_timing::Timer {
        let leased = matches!(policy, ReadPolicy::LeaseRead);
        let kind = self.state.store().identity().kind.clone();
        crate::oltp_runtime::stage_timing::variant(
            if leased {
                "raft.lease_read_by_kind."
            } else {
                "raft.read_index_by_kind."
            },
            &kind,
        )
        .record(std::time::Duration::ZERO);
        crate::oltp_runtime::stage_timing::by_kind(&kind, if leased { lease } else { index })
            .timer()
    }
    /// `read_barrier`'s Raft half: the index a read must have applied. The
    /// caller checks it against its own snapshot (`lease_read_index`).
    async fn read_index(&self, policy: ReadPolicy) -> Result<u64, ProtocolError> {
        let _timer = self.read_timer(
            &policy,
            [
                "raft.data.lease_index",
                "raft.ts.lease_index",
                "raft.meta.lease_index",
            ],
            [
                "raft.data.read_index_only",
                "raft.ts.read_index_only",
                "raft.meta.read_index_only",
            ],
        );
        Ok(self.linearized(policy).await?.read_log_id().index())
    }
    /// RD1: the barrier's applied position from openraft's applied log id
    /// instead of a store read. `await_ready` returned only once the state
    /// machine applied the read index, and the machine applies an entry's
    /// effects before openraft records it as applied, so every effect up to
    /// the returned position is visible to the caller's in-memory reads.
    async fn read_position(&self, policy: ReadPolicy) -> Result<AppliedPosition, ProtocolError> {
        let _timer = self.read_timer(
            &policy,
            [
                "raft.data.lease_position",
                "raft.ts.lease_position",
                "raft.meta.lease_position",
            ],
            [
                "raft.data.read_position",
                "raft.ts.read_position",
                "raft.meta.read_position",
            ],
        );
        let state = self.linearized(policy).await?;
        let required = state.read_log_id().index();
        match state.applied() {
            Some(applied) if applied.index >= required => Ok(position(*applied)),
            applied => {
                tracing::error!(group=%self.state.store().identity().group_id,node=self.state._resident.node_id(),?applied,required,"typed lease read applied below its read index");
                Err(unavailable())
            }
        }
    }
    async fn read_barrier(&self, policy: ReadPolicy) -> Result<ReadBarrier, ProtocolError> {
        {
            let _timer = self.read_timer(
                &policy,
                [
                    "raft.data.lease_read",
                    "raft.ts.lease_read",
                    "raft.meta.lease_read",
                ],
                [
                    "raft.data.read_index",
                    "raft.ts.read_index",
                    "raft.meta.read_index",
                ],
            );
            let barrier = *self.linearized(policy).await?.read_log_id();
            let applied = self
                .state
                .store()
                .applied_state(META)
                .await?
                .ok_or_else(unavailable)?;
            if applied.position.index < barrier.index() {
                tracing::error!(group=%self.state.store().identity().group_id,node=self.state._resident.node_id(),applied=applied.position.index,required=barrier.index(),"typed ReadIndex application barrier incomplete");
                return Err(unavailable());
            }
            Ok(ReadBarrier {
                applied: applied.position,
            })
        }
    }
}
pub(crate) fn position(id: LogId) -> AppliedPosition {
    AppliedPosition {
        term: id.leader_id.term,
        node_id: id.leader_id.node_id,
        index: id.index,
    }
}
pub(crate) fn log_id(position: AppliedPosition) -> LogId {
    LogId::new(
        CommittedLeaderId::new(position.term, position.node_id),
        position.index,
    )
}
fn disk(error: ProtocolError) -> io::Error {
    io::Error::other(error.to_string())
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "typed Raft quorum, application or leader authority unavailable",
        Outcome::Unknown,
    )
}

fn client_write_error<C: TypedConfig>(error: RaftError<C, ClientWriteError<C>>) -> ProtocolError {
    // OpenRaft forwards before appending. LogEntryDiscarded and Fatal
    // can also follow an append, so they retain the unknown outcome.
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => proposal_forwarded(),
        other => {
            tracing::error!(error = %other, "typed client_write failed");
            unavailable()
        }
    }
}

pub(super) fn proposal_forwarded() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "typed Raft leader refused the proposal before append",
        Outcome::NotApplied,
    )
}

fn execution_class(store: &GroupStore) -> ExecutionClass {
    match store.identity().kind {
        GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
            ExecutionClass::Control
        }
        _ => ExecutionClass::Query,
    }
}

pub(crate) mod direct_leadership;
#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::acceptance_o11_storage;
pub(crate) mod transient;
