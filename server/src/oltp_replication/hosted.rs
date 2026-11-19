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
        ApplyMeta, ChargedBytes, ExecutionClass, GroupSnapshot, GroupStore, TransactionBudget,
        Transition,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use futures_util::{Stream, StreamExt};
use openraft::{
    OptionalSend, Raft, RaftSnapshotBuilder, ReadPolicy,
    storage::{EntryResponder, RaftStateMachine},
    type_config::alias::{SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
    vote::RaftLeaderId,
};
use sha2::{Digest as _, Sha256};
use std::{future::Future, io, sync::Arc};

const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 128 << 10,
    output_bytes: 128 << 10,
};

pub(crate) trait TypedMachine<C: TypedConfig>: Clone + Send + Sync + 'static {
    fn store(&self) -> &GroupStore;
    fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<C::Command>,
    ) -> impl Future<Output = Result<ChargedBytes, ProtocolError>> + Send;
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
            _config: std::marker::PhantomData,
        })
    }
    pub(crate) fn store(&self) -> &GroupStore {
        self.machine.store()
    }
    pub(crate) fn resident(&self) -> ReplicaLease {
        self._resident.clone()
    }
    pub(crate) async fn snapshot(
        &self,
        file: GroupSnapshot,
    ) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        let raw = self
            .store()
            .snapshot_membership(file.clone(), META)
            .await
            .map_err(disk)?;
        let owner = Arc::new(self._resident.metadata(256 << 10).map_err(disk)?);
        let membership = match raw {
            Some(raw) => stored_membership(raw.as_slice()).map_err(disk)?,
            None => StoredMembershipOf::<C>::default(),
        };
        let meta = SnapshotMetaOf::<C> {
            last_log_id: file.header.applied.map(log_id),
            last_membership: membership,
        };
        Ok(openraft::Snapshot {
            meta,
            snapshot: TypedSnapshot {
                file,
                _owner: owner,
            },
        })
    }
    async fn builder(&self) -> TypedSnapshotBuilder<C, M> {
        // Capture the cut when OpenRaft asks for a builder, not when a later
        // background build polls it after application has advanced.
        let cut = self.store().export_snapshot(META).await.map_err(disk);
        TypedSnapshotBuilder {
            owner: self.clone(),
            cut: Some(cut),
        }
    }
}
#[derive(Clone)]
pub(crate) struct TypedSnapshot {
    pub(crate) file: GroupSnapshot,
    _owner: Arc<ReplicaMetadata>,
}
pub(crate) struct TypedSnapshotBuilder<C: TypedConfig, M: TypedMachine<C>> {
    owner: HostedState<C, M>,
    cut: Option<io::Result<GroupSnapshot>>,
}
impl<C: TypedConfig, M: TypedMachine<C>> RaftSnapshotBuilder<C> for TypedSnapshotBuilder<C, M> {
    type SnapshotData = TypedSnapshot;
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<C, TypedSnapshot>> {
        let file = match self.cut.take() {
            Some(cut) => cut?,
            None => self
                .owner
                .store()
                .export_snapshot(META)
                .await
                .map_err(disk)?,
        };
        self.owner.snapshot(file).await
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
            entry.ensure_charged().map_err(disk)?;
            let position = position(entry.log_id);
            let class = execution_class(self.store());
            let shared = self.store().resources().clone();
            let digest_entry = entry.clone();
            let digest = shared
                .execute(class, position.index, 0, move || {
                    match digest_entry.payload() {
                        Payload::Normal(command) => serialized_digest(command),
                        Payload::Membership(membership) => {
                            serialized_digest(&StoredMembershipOf::<C>::new(
                                Some(digest_entry.log_id),
                                membership.clone(),
                            ))
                        }
                        Payload::Blank => Ok(Digest::from_bytes(
                            Sha256::digest(b"nabedra/typed-raft-blank/v1").into(),
                        )),
                    }
                })
                .await
                .map_err(disk)?;
            let meta = ApplyMeta {
                position,
                command_digest: digest,
                membership: None,
            };
            let raw = match entry.payload() {
                Payload::Normal(command) => Some(
                    self.machine
                        .apply(meta, command.clone())
                        .await
                        .map_err(|error| {
                            tracing::error!(group=%self.store().identity().group_id,kind=?self.store().identity().kind,node=self._resident.node_id(),index=position.index,code=?error.code,outcome=?error.outcome,"typed state application failed");
                            disk(error)
                        })?,
                ),
                Payload::Membership(membership) => {
                    self.store()
                        .transaction_with_membership(
                            meta,
                            META,
                            Some(StoredMembershipOf::<C>::new(
                                Some(entry.log_id),
                                membership.clone(),
                            )),
                            |tx| Ok(Transition::Commit(tx.copy_output(b"null")?)),
                        )
                        .await
                        .map_err(disk)?;
                    None
                }
                Payload::Blank => {
                    self.store()
                        .transaction(meta, META, |tx| {
                            Ok(Transition::Commit(tx.copy_output(b"null")?))
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
                let required = if let Some(raw) = &raw {
                    let input = raw.clone();
                    shared
                        .execute(class, position.index, 0, move || {
                            let bytes = crate::oltp::mvcc::json_decode_budget(input.as_slice())?;
                            if bytes > C::MAX_REPLY_DECODE_BYTES {
                                return Err(invalid(
                                    "typed durable response exceeds validated decode budget",
                                ));
                            }
                            Ok(bytes)
                        })
                        .await
                        .map_err(disk)?
                } else {
                    1
                };
                // Independent precharged reply-decode capacity is acquired
                // before native/CPU. Neither transport wire arena is needed.
                let owner = Arc::new(shared.reserve_reply_decode(required).await.map_err(disk)?);
                let (reply, owner) = shared
                    .execute(class, position.index, 0, move || {
                        let reply = raw
                            .map(|raw| {
                                serde_json::from_slice::<C::Reply>(raw.as_slice())
                                    .map_err(|_| invalid("corrupt typed durable response"))
                            })
                            .transpose()?;
                        Ok((reply, owner))
                    })
                    .await
                    .map_err(disk)?;
                responder.send(Applied::owned(position, reply, owner));
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.builder().await
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<C>,
        snapshot: TypedSnapshot,
    ) -> io::Result<()> {
        let checked = self.snapshot(snapshot.file.clone()).await?;
        if checked.meta != *meta {
            return Err(io::Error::other(
                "snapshot bytes differ from typed Raft metadata",
            ));
        }
        self.store()
            .install_snapshot(snapshot.file, META)
            .await
            .map_err(disk)
    }
    async fn get_current_snapshot(&mut self) -> io::Result<Option<SnapshotOf<C, TypedSnapshot>>> {
        match self.store().current_snapshot(META).await.map_err(disk)? {
            Some(file) => self.snapshot(file).await.map(Some),
            None => Ok(None),
        }
    }
}

/// Constructed only by the hosted-replica manager after validating placement,
/// permanent node identity, membership and the matching resident lease.
pub(crate) struct RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(crate) raft: Raft<C, HostedState<C, M>>,
    pub(crate) log: TypedLogStore<C>,
    pub(crate) state: HostedState<C, M>,
    pub(in crate::oltp_replication) membership_changes: tokio::sync::Mutex<()>,
}
/// Entries per AppendEntries RPC. The typed Raft wire carries one entry per
/// frame, and an older peer cannot decode more, so replication stays at one
/// until a negotiated multi-entry frame exists. Reading more here would only
/// decode entries the network then drops.
pub(crate) const REPLICATION_PAYLOAD_ENTRIES: u64 = 1;
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
    pub(crate) async fn start<N>(
        config: Arc<openraft::Config>,
        network: N,
        log: TypedLogStore<C>,
        state: HostedState<C, M>,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        N: openraft::network::RaftNetworkFactory<C>,
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
        if !state.store().defer_apply_sync(log.group()) {
            tracing::warn!(group=%state.store().identity().group_id,
                "typed Raft log and state use different pools; application stays synced");
        }
        let raft = Raft::new(
            state._resident.node_id(),
            config,
            network,
            log.clone(),
            state.clone(),
        )
        .await
        .map_err(|_| unavailable())?;
        Ok(Arc::new(Self {
            raft,
            log,
            state,
            membership_changes: tokio::sync::Mutex::new(()),
        }))
    }
    pub(crate) async fn shutdown(&self) -> Result<(), ProtocolError> {
        let stopped = self.raft.shutdown().await;
        self.log.drain().await;
        // No running log replays unsynced application any more (RV-T2 Q5).
        self.state.store().stop_deferred_applies();
        stopped.map_err(|_| unavailable())
    }
}
impl<C, M> Drop for RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn drop(&mut self) {
        // Clearing the flag only makes later application writes synced again.
        self.state.store().stop_deferred_applies();
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
            // Reserve before client_write: saturation is a typed pre-proposal
            // outcome, never a fatal WriteLogs error after Raft accepts it.
            command.reserve_raft_metadata(
                &self.state._resident,
                super::entry::OwnedRaftEntry::<C::Command>::normal_metadata_bytes(),
            )?;
            let response = self
                .raft
                .client_write(command)
                .await
                .map_err(|error| {
                    tracing::error!(group=%self.state.store().identity().group_id,kind=?self.state.store().identity().kind,node=self.state._resident.node_id(),error=%error,"typed client_write failed");
                    unavailable()
                })?;
            let (position, reply, owner) = response.data.into_parts()?;
            Ok(Applied::owned(
                position,
                reply.ok_or_else(unavailable)?,
                owner,
            ))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async move {
            let barrier = self
                .raft
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .map_err(|error| {
                    tracing::warn!(group=%self.state.store().identity().group_id,kind=?self.state.store().identity().kind,node=self.state._resident.node_id(),error=%error,"typed ReadIndex failed");
                    unavailable()
                })?;
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
        })
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

fn execution_class(store: &GroupStore) -> ExecutionClass {
    match store.identity().kind {
        GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
            ExecutionClass::Control
        }
        _ => ExecutionClass::Query,
    }
}

#[cfg(test)]
mod tests;
