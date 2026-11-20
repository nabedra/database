//! Local RF1 composition uses the same durable typed groups, authenticated
//! transport, admission authority and participant services as remote workers.
use super::{
    coordinator::QueueCoordinator,
    replica::*,
    worker_endpoint::{QueueEndpoint, QueuePeerClient},
};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp::{
        mvcc::{
            catalog::{CatalogHandle, PinnedCatalog, ShardRoute},
            planner::{ParticipantResolver, ResolveFuture},
            store::{KeySpan, ParticipantHandle, ParticipantMachine, ParticipantReply},
        },
        pool::{ByteClass, OltpPool, SharedResourceConfig, WorkerSharedResources},
    },
    oltp_replication::{
        authority::{
            consensus::{Applied, Consensus},
            control::{
                ControlClient, ControlEndpoint, ControlHandle, ControlReply, ControlRoutes,
                LocalOperatorPermit, OperatorPolicy, Pending, VerifiedMembership,
                VerifiedQueueCopy,
            },
            status::{StatusCommand, StatusHandle, StatusMachine, StatusReply, StatusService},
            timestamp::{TimestampHandle, TimestampMachine, TimestampReply, TimestampService},
        },
        hosted::{RaftConsensus, TypedMachine},
        manager::{
            HostedGroupManager, LocalWorker, ManagedCatalog, ManagedControl, ManagedStatus,
            ManagedTimestamps,
        },
        multiplex::Purpose,
        owned::OwnedCommand,
        typed_log::TypedConfig,
        types::*,
    },
    oltp_deployment::inventory::WorkerPools,
    protocol::{ErrorCode, Outcome, ProtocolError, TableDescriptor},
    request_queue::{QueueError, QueueLimits},
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, OnceLock, RwLock, Weak},
};
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;
pub(super) mod registered;
mod restore;

pub(crate) struct LocalServices {
    pub shared: Arc<WorkerSharedResources>,
    pub pools: Arc<WorkerPools>,
    pub scope: QueueScope,
    pub local: WorkerIdentity,
    pub replication_factor: u8,
    pub(super) cold_history: Option<super::worker_runtime::cold_history::Sources>,
    pub control: Arc<ControlHandle>,
    pub operator: Arc<LocalOperatorPermit>,
    pub timestamps: Arc<dyn TimestampService>,
    /// This process's own Timestamp replica for `AllocateDirect`, when the
    /// Timestamp service is not reachable over the service lane (a single
    /// worker). `None` for registered workers.
    pub direct_timestamps:
        Option<Arc<dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource>>,
    pub status: Arc<dyn StatusService>,
    pub catalog: CatalogHandle,
    pub resolver: Arc<dyn ParticipantResolver>,
    pub control_client: Arc<ControlClient>,
    pub admission_issuer: Option<Arc<super::admission_tickets::AdmissionTickets>>,
    pub remote_status: Vec<Arc<crate::oltp_replication::service_rpc::StatusClient>>,
    pub archive_repository: Option<Arc<super::worker_runtime::archive::NativeBackupPublication>>,
    pub split: Option<crate::oltp_replication::manager::shard_split::SplitClient>,
    pub placement: Option<Arc<crate::oltp_replication::manager::ReplicaPlacement>>,
    pub replica: Arc<Mutex<WorkerReplica>>,
    pub queue: Arc<QueueCoordinator>,
    pub(super) scoped: Option<super::scoped_worker::ScopedWorkerServices>,
    pub manager: Arc<HostedGroupManager>,
    pub directory: Arc<crate::oltp_replication::multiplex::AppliedPeerDirectory>,
    refresh: Mutex<Option<JoinHandle<()>>>,
    membership: Mutex<Option<JoinHandle<()>>>,
    stop: watch::Sender<bool>,
    transport: Mutex<Option<JoinHandle<Result<(), ProtocolError>>>>,
}
impl Drop for LocalServices {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
impl LocalServices {
    #[cfg(test)]
    pub(crate) async fn status_endpoint_for_test(
        &self,
    ) -> Result<
        (
            u64,
            Arc<crate::oltp_replication::service_rpc::StatusEndpoint>,
        ),
        ProtocolError,
    > {
        use crate::oltp_replication::{
            authority::status::StatusTopology, service_rpc::StatusEndpoint,
        };
        let group = self.status.identity();
        let routes = self.control_client.replica_routes(group).await?;
        let [member] = routes.members.as_slice() else {
            return Err(unavailable());
        };
        let pool = self.pools.get(member.pool_id)?.ok_or_else(unavailable)?;
        let store = pool.group(group.clone()).await?;
        let running = self
            .manager
            .start_status(
                store.clone(),
                member.clone(),
                BTreeMap::from([(member.node_id, member.clone())]),
                raft_config(),
            )
            .await?;
        let handle = StatusHandle::new(StatusMachine::new(store)?, running);
        let endpoint = StatusEndpoint::new(
            handle,
            member.node_id,
            self.control_client.clone(),
            self.manager.client(),
            self.timestamps.identity().clone(),
            StatusTopology::new(vec![group.clone()])?,
        )?;
        Ok((member.node_id, endpoint))
    }

    pub(crate) async fn open(
        path: &Path,
        database: &str,
        resources: WorkerResources,
        execution: &ExecutionConfig,
        cold_start: bool,
    ) -> Result<Self, ProtocolError> {
        let worker = LocalWorker::open(
            path,
            database,
            resources,
            SharedResourceConfig {
                query_threads: resources.max_threads as usize,
                ..Default::default()
            },
        )
        .await?;
        let LocalWorker {
            manager,
            shared,
            pool,
            topology,
            enrollment,
            operator,
            transport,
            listener,
            directory,
            ..
        } = worker;
        let operator = Arc::new(operator);
        let local = WorkerIdentity::from(&enrollment);
        let member = topology.member();
        let peers = [(member.node_id, member.clone())]
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let scope = QueueScope {
            cluster_id: topology.cluster_id,
            database: topology.database.clone(),
        };
        let pools = WorkerPools::new(pool.clone(), local, None)?;
        let (stop, mut stopped) = watch::channel(false);
        let transport_owner = transport.clone();
        let task = tokio::spawn(async move {
            transport_owner
                .run(listener, async move {
                    while !*stopped.borrow_and_update() {
                        if stopped.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
        });
        let mut directory_refresh = None;
        let result = async {
            let control_store = pool.group(topology.group(GroupKind::Control)?).await?;
            let control_raft = manager
                .start_control(
                    control_store.clone(),
                    member.clone(),
                    peers.clone(),
                    raft_config(),
                )
                .await?;
            initialize(&control_raft, &peers).await?;
            let policy = digest(&("nabedra/local-control-policy/v1", topology.cluster_id))?;
            let control = ControlHandle::new(
                crate::oltp_replication::authority::control::ControlMachine::new(control_store)?,
                control_raft.clone(),
                OperatorPolicy {
                    policy,
                    certificate_pins: [local.certificate_sha256].into_iter().collect(),
                },
            );
            let initial = control.inspect().await?;
            if initial.is_some() && !cold_start {
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "--cold-start is required after the previous local worker stopped",
                    Outcome::NotApplied,
                ));
            }
            if initial.is_none() {
                accepted(
                    control
                        .initialize_local(&operator, Uuid::new_v4(), Uuid::new_v4())
                        .await?
                        .reply,
                )?;
            }
            let mut state = control.inspect().await?.ok_or_else(unavailable)?;
            // The explicit local launch plus retained lifetime flock proves the
            // previous RF1 process is excluded. No heartbeat timeout is used.
            let pending_cold = if initial.is_some() {
                match &state.pending {
                    Some(Pending::Fence(fence)) => Some(fence.clone()),
                    _ => {
                        accepted(
                            control
                                .begin_cold_fence_local(
                                    &operator,
                                    Uuid::new_v4(),
                                    state.ready,
                                    Uuid::new_v4(),
                                )
                                .await?
                                .reply,
                        )?;
                        state = control.inspect().await?.ok_or_else(unavailable)?;
                        match &state.pending {
                            Some(Pending::Fence(fence)) => Some(fence.clone()),
                            _ => return Err(unavailable()),
                        }
                    }
                }
            } else {
                None
            };
            let old_worker = control.worker(local.worker_id).await?;
            let exclusion = old_worker
                .as_ref()
                .map(|old| {
                    control.exclude_local_previous(
                        &operator,
                        Uuid::new_v4(),
                        WorkerIdentity::from(old),
                    )
                })
                .transpose()?;
            accepted(
                control
                    .enroll_local(
                        &operator,
                        Uuid::new_v4(),
                        old_worker.as_ref().map(WorkerIdentity::from),
                        enrollment.clone(),
                        exclusion,
                    )
                    .await?
                    .reply,
            )?;

            let timestamps_store = pool.group(topology.group(GroupKind::Timestamps)?).await?;
            let timestamp_raft = manager
                .start_timestamps(
                    timestamps_store.clone(),
                    member.clone(),
                    peers.clone(),
                    raft_config(),
                )
                .await?;
            initialize(&timestamp_raft, &peers).await?;
            let timestamps = TimestampHandle::new(
                TimestampMachine::new(timestamps_store)?,
                timestamp_raft.clone(),
            );
            let status_store = pool
                .group(topology.group(GroupKind::Status { bucket: 0 })?)
                .await?;
            let status_raft = manager
                .start_status(
                    status_store.clone(),
                    member.clone(),
                    peers.clone(),
                    raft_config(),
                )
                .await?;
            initialize(&status_raft, &peers).await?;
            let status = StatusHandle::new(StatusMachine::new(status_store)?, status_raft.clone());
            let catalog_store = pool.group(topology.group(GroupKind::Catalog)?).await?;
            let catalog_raft = manager
                .start_catalog(
                    catalog_store.clone(),
                    member.clone(),
                    peers.clone(),
                    raft_config(),
                )
                .await?;
            initialize(&catalog_raft, &peers).await?;
            let catalog_machine = ParticipantMachine::new(catalog_store)?;
            let catalog_seed = catalog_machine.restore_seed().await?;
            let catalog_participant = Arc::new(ParticipantHandle::new(
                catalog_machine,
                catalog_raft.clone(),
            ));
            let old = state.ready.view.authority();
            // A newly opened group must first be enrolled at the current
            // authority. Existing groups replay the exact persisted transition.
            if timestamps.current_authority().await?.is_none() {
                timestamps
                    .fence(
                        id(topology.timestamps, b"initial-fence"),
                        None,
                        old,
                        state.ready,
                    )
                    .await?;
            }
            if status.current_authority().await?.is_none() {
                let reply = status_raft
                    .propose(OwnedCommand::new_owned(
                        StatusCommand::Fence {
                            transition_id: id(topology.status, b"initial-fence"),
                            expected: None,
                            next: old,
                        },
                        &shared,
                        ByteClass::Progress,
                    )?)
                    .await?;
                if !matches!(reply.reply, StatusReply::Fenced(_)) {
                    return Err(unavailable());
                }
            }
            if catalog_participant.activation().await?.is_none() {
                catalog_participant
                    .fence_initial(
                        id(topology.catalog, b"initial-fence"),
                        old,
                        topology.catalog,
                        catalog_seed.as_ref().map_or(1, |seed| seed.schema_epoch),
                        catalog_seed.as_ref().map_or(1, |seed| seed.routing_epoch),
                        catalog_seed.as_ref().map_or(
                            KeySpan {
                                start: None,
                                end: None,
                            },
                            |seed| seed.span.clone(),
                        ),
                    )
                    .await?;
            }
            if pending_cold.is_none() {
                register(&control, &operator, &control_raft, old).await?;
                register(&control, &operator, &timestamp_raft, old).await?;
                register(&control, &operator, &status_raft, old).await?;
                register(&control, &operator, &catalog_raft, old).await?;
            }
            let authority = Arc::new(RwLock::new(old));
            let resolver = Arc::new(LocalParticipants {
                manager: manager.clone(),
                pool: pool.clone(),
                control: control.clone(),
                operator: operator.clone(),
                member: member.clone(),
                peers: peers.clone(),
                catalog: topology.catalog,
                catalog_participant: catalog_participant.clone(),
                authority: authority.clone(),
                cache: Mutex::new(BTreeMap::new()),
                remote_cache: Mutex::new(BTreeMap::new()),
                rpc: OnceLock::new(),
                wired: Mutex::new(std::collections::BTreeSet::new()),
            });
            restore::activate(&resolver).await?;
            if let Some(fence) = pending_cold {
                let next = fence.next.view.authority();
                let timestamp = timestamps
                    .fence(fence.transition_id, Some(old), next, fence.next)
                    .await?;
                let TimestampReply::Fenced { receipt, .. } = timestamp.reply else {
                    return Err(unavailable());
                };
                if control.group(receipt.group.group_id).await?.is_some() {
                    accepted(
                        control
                            .record_group_fence(Uuid::new_v4(), receipt)
                            .await?
                            .reply,
                    )?;
                }
                let status_receipt = status_raft
                    .propose(OwnedCommand::new_owned(
                        StatusCommand::Fence {
                            transition_id: fence.transition_id,
                            expected: Some(old),
                            next,
                        },
                        &shared,
                        ByteClass::Progress,
                    )?)
                    .await?;
                let StatusReply::Fenced(receipt) = status_receipt.reply else {
                    return Err(unavailable());
                };
                if control.group(receipt.group.group_id).await?.is_some() {
                    accepted(
                        control
                            .record_group_fence(Uuid::new_v4(), receipt)
                            .await?
                            .reply,
                    )?;
                }
                let receipt = catalog_participant
                    .fence_current(fence.transition_id, old, next)
                    .await?;
                if control.group(receipt.group.group_id).await?.is_some() {
                    accepted(
                        control
                            .record_group_fence(Uuid::new_v4(), receipt)
                            .await?
                            .reply,
                    )?;
                }
                let mut after = None;
                loop {
                    let bytes = control
                        .groups(fence.group_inventory_revision, after)
                        .await?;
                    let page: crate::oltp_replication::authority::control::InventoryPage<
                        crate::oltp_replication::authority::control::GroupRecord,
                    > = serde_json::from_slice(bytes.as_ref()).map_err(|_| unavailable())?;
                    for group in page
                        .items
                        .iter()
                        .filter(|group| matches!(group.identity.kind, GroupKind::Data { .. }))
                    {
                        let participant = resolver.participant(&group.identity).await?;
                        let receipt = participant
                            .fence_current(fence.transition_id, old, next)
                            .await?;
                        accepted(
                            control
                                .record_group_fence(Uuid::new_v4(), receipt)
                                .await?
                                .reply,
                        )?;
                    }
                    if page.done {
                        break;
                    }
                    after = page.after;
                }
                let mut after = None;
                loop {
                    let bytes = control.ready_page_read(fence.expected, after).await?;
                    let page: crate::oltp_replication::authority::control::InventoryPage<
                        WorkerIdentity,
                    > = serde_json::from_slice(bytes.as_ref()).map_err(|_| unavailable())?;
                    let mut excluded = Vec::new();
                    for worker in page.items.iter() {
                        if !control.excluded(fence.transition_id, *worker).await? {
                            excluded.push(control.exclude_local_previous(
                                &operator,
                                Uuid::new_v4(),
                                *worker,
                            )?);
                        }
                    }
                    if !excluded.is_empty() {
                        accepted(
                            control
                                .excluded_local_page(
                                    &operator,
                                    Uuid::new_v4(),
                                    fence.transition_id,
                                    excluded,
                                )
                                .await?
                                .reply,
                        )?;
                    }
                    if page.done {
                        break;
                    }
                    after = page.after;
                }
                if let Some(worker) = fence.extra_exclusion {
                    if !control.excluded(fence.transition_id, worker).await? {
                        let proof =
                            control.exclude_local_previous(&operator, Uuid::new_v4(), worker)?;
                        accepted(
                            control
                                .excluded_local_page(
                                    &operator,
                                    Uuid::new_v4(),
                                    fence.transition_id,
                                    vec![proof],
                                )
                                .await?
                                .reply,
                        )?;
                    }
                }
                accepted(
                    control
                        .publish_fence_local(&operator, Uuid::new_v4(), fence.transition_id)
                        .await?
                        .reply,
                )?;
                *authority.write().map_err(|_| unavailable())? = next;
                state = control.inspect().await?.ok_or_else(unavailable)?;
                // Groups created before an interrupted bootstrap but not yet
                // registered are registered only after the cold transition.
                register(&control, &operator, &control_raft, next).await?;
                register(&control, &operator, &timestamp_raft, next).await?;
                register(&control, &operator, &status_raft, next).await?;
                register(&control, &operator, &catalog_raft, next).await?;
            }
            let ticket_id = Uuid::new_v4();
            let ControlReply::JoinBegun {
                ticket,
                bootstrap: true,
            } = control
                .begin_join_local(&operator, ticket_id, state.ready.view, local)
                .await?
                .reply
            else {
                return Err(unavailable());
            };
            accepted(
                control
                    .ready_page_local(
                        &operator,
                        Uuid::new_v4(),
                        ticket_id,
                        Page::new(vec![local]).map_err(|_| unavailable())?,
                    )
                    .await?
                    .reply,
            )?;
            let ControlReply::ReadySealed { expected, next, .. } = control
                .seal_ready_local(&operator, Uuid::new_v4(), ticket_id)
                .await?
                .reply
            else {
                return Err(unavailable());
            };
            let mut replica = WorkerReplica::new_empty_bootstrap(
                shared.clone(),
                scope.clone(),
                local,
                ticket,
                queue_limits(execution),
            )
            .map_err(QueueError::protocol)?;
            let receipt = timestamps.fence_ready(ticket_id, expected, next).await?;
            accepted(
                control
                    .timestamp_fenced(Uuid::new_v4(), receipt)
                    .await?
                    .reply,
            )?;
            let proof = VerifiedQueueCopy::from_empty_worker(ticket, &replica)?;
            accepted(control.publish_ready(Uuid::new_v4(), proof).await?.reply)?;
            use super::worker_endpoint::AuthorityReader;
            replica
                .activate_empty_bootstrap(control.current(&scope).await?)
                .map_err(QueueError::protocol)?;
            let replica = Arc::new(Mutex::new(replica));
            shared.bind_read_replica(&replica)?;
            let group = topology.group(GroupKind::Control)?;
            let registration = manager.registry().reserve(group.clone())?;
            registration.bind(
                Purpose::Authority,
                ControlEndpoint::new(control.clone(), shared.clone())?,
            )?;
            let endpoint = QueueEndpoint::new(
                replica.clone(),
                shared.clone(),
                scope.clone(),
                control.clone(),
                resources.task_memory_limit_bytes,
            )?;
            endpoint.install_retirement_reader(status.clone())?;
            registration.bind(Purpose::QueueControl, endpoint.clone())?;
            registration.bind(Purpose::QueueData, endpoint)?;
            let routes = Arc::new(LocalControlRoutes {
                manager: manager.clone(),
                group: group.group_id,
            });
            let control_client =
                ControlClient::new(manager.client(), shared.clone(), group.clone(), routes)?;
            directory.refresh(&control_client).await?;
            directory_refresh = Some(tokio::spawn(
                directory
                    .clone()
                    .run_refresh(control_client.clone(), stop.subscribe()),
            ));
            let peer = Arc::new(QueuePeerClient::new(
                manager.client(),
                shared.clone(),
                group,
            )?);
            manager.bind_service(
                status.identity(),
                member.node_id,
                crate::oltp_replication::service_rpc::StatusProofEndpoint::new(
                    status.clone(),
                    member.node_id,
                    control_client.clone(),
                    shared.clone(),
                    None,
                )?,
            )?;
            resolver
                .rpc
                .set(control_client.clone())
                .map_err(|_| unavailable())?;
            resolver.bind_native(catalog_participant.clone()).await?;
            let catalog =
                CatalogHandle::new(resolver.participant(catalog_participant.identity()).await?)?;
            timestamps.install_release_reader(control_client.clone(), peer.clone())?;
            // T3's ReserveWindow predicate (T7): data.direct.v1 is active.
            timestamps.install_direct_gate(
                super::worker_runtime::direct_capability::ControlReserveWindowGate::new(
                    control_client.clone(),
                ),
            );
            let queue = QueueCoordinator::new(replica.clone(), control.clone(), peer);
            shared.bind_read_queue(&queue)?;
            Ok((
                control,
                timestamps,
                status,
                catalog,
                resolver,
                replica,
                queue,
                control_client,
            ))
        }
        .await;
        match result {
            Ok((
                control,
                timestamps,
                status,
                catalog,
                resolver,
                replica,
                queue,
                control_client,
            )) => Ok(Self {
                shared,
                pools,
                scope,
                local,
                replication_factor: 1,
                cold_history: cold_start.then(|| {
                    super::worker_runtime::cold_history::Sources::Local {
                        timestamps: timestamps.clone(),
                        status: status.clone(),
                    }
                }),
                control,
                operator,
                direct_timestamps: Some(timestamps.clone()
                    as Arc<dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource>),
                timestamps,
                status,
                catalog,
                resolver,
                control_client,
                admission_issuer: None,
                remote_status: Vec::new(),
                archive_repository: None,
                split: None,
                placement: None,
                replica,
                queue,
                scoped: None,
                manager,
                directory,
                refresh: Mutex::new(directory_refresh),
                membership: Mutex::new(None),
                stop,
                transport: Mutex::new(Some(task)),
            }),
            Err(error) => {
                let _ = manager.shutdown().await;
                let _ = stop.send(true);
                let _ = task.await;
                Err(error)
            }
        }
    }
    pub(crate) async fn refresh_enrollment(&self) -> Result<(), ProtocolError> {
        self.directory
            .refresh(&self.control_client)
            .await
            .map(|_| ())
    }
    pub(crate) async fn shutdown(&self) -> Result<(), ProtocolError> {
        self.manager.shutdown().await?;
        let _ = self.stop.send(true);
        let membership = self.membership.lock().map_err(|_| unavailable())?.take();
        if let Some(membership) = membership {
            membership.await.map_err(|_| unavailable())?;
        }
        let refresh = self.refresh.lock().map_err(|_| unavailable())?.take();
        if let Some(refresh) = refresh {
            refresh.await.map_err(|_| unavailable())?;
        }
        let task = self.transport.lock().map_err(|_| unavailable())?.take();
        if let Some(task) = task {
            task.await.map_err(|_| unavailable())??;
        }
        Ok(())
    }
}

struct LocalControlRoutes {
    manager: Arc<HostedGroupManager>,
    group: Uuid,
}
impl ControlRoutes for LocalControlRoutes {
    fn preferred(&self) -> Option<Uuid> {
        self.manager
            .leader_candidates(self.group)
            .ok()
            .and_then(|members| members.first().map(|member| member.worker_id))
    }
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(self
            .manager
            .leader_candidates(self.group)?
            .into_iter()
            .map(|member| member.worker_id)
            .filter(|worker| after.is_none_or(|after| *worker > after))
            .min())
    }
}

pub(crate) struct LocalParticipants {
    manager: Arc<HostedGroupManager>,
    pool: OltpPool,
    control: Arc<ControlHandle>,
    operator: Arc<LocalOperatorPermit>,
    member: ReplicaMember,
    peers: BTreeMap<u64, ReplicaMember>,
    catalog: Uuid,
    catalog_participant: Arc<ParticipantHandle>,
    authority: Arc<RwLock<Authority>>,
    cache: Mutex<BTreeMap<Uuid, Weak<ParticipantHandle>>>,
    remote_cache: Mutex<BTreeMap<Uuid, Weak<ParticipantHandle>>>,
    rpc: OnceLock<Arc<ControlClient>>,
    wired: Mutex<std::collections::BTreeSet<Uuid>>,
}
impl LocalParticipants {
    async fn native_participant(
        &self,
        group: &GroupIdentity,
    ) -> Result<Arc<ParticipantHandle>, ProtocolError> {
        if group == self.catalog_participant.identity() {
            return Ok(self.catalog_participant.clone());
        }
        if let Some(participant) = self
            .cache
            .lock()
            .map_err(|_| unavailable())?
            .get(&group.group_id)
            .and_then(Weak::upgrade)
        {
            return Ok(participant);
        }
        if group.cluster_id != self.control.identity().cluster_id
            || group.database != self.control.identity().database
            || !matches!(group.kind, GroupKind::Data { .. })
        {
            return Err(unavailable());
        }
        let store = self.pool.group(group.clone()).await?;
        let running = self
            .manager
            .start_data(
                store.clone(),
                self.member.clone(),
                self.peers.clone(),
                raft_config(),
            )
            .await?;
        initialize(&running, &self.peers).await?;
        let participant = Arc::new(ParticipantHandle::new(
            ParticipantMachine::new(store)?,
            running,
        ));
        self.cache
            .lock()
            .map_err(|_| unavailable())?
            .insert(group.group_id, Arc::downgrade(&participant));
        Ok(participant)
    }
    async fn bind_native(&self, participant: Arc<ParticipantHandle>) -> Result<(), ProtocolError> {
        let Some(control) = self.rpc.get() else {
            return Ok(());
        };
        let mut wired = self.wired.lock().map_err(|_| unavailable())?;
        if wired.contains(&participant.identity().group_id) {
            return Ok(());
        }
        self.manager.bind_service(
            participant.identity(),
            self.member.node_id,
            crate::oltp_replication::service_rpc::ParticipantEndpoint::new(
                participant.clone(),
                self.member.node_id,
                control.clone(),
                self.manager.client(),
                None,
            )?,
        )?;
        wired.insert(participant.identity().group_id);
        Ok(())
    }
}
impl ParticipantResolver for LocalParticipants {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>> {
        Box::pin(async move {
            let Some(control) = self.rpc.get() else {
                return self.native_participant(group).await;
            };
            if let Some(participant) = self
                .remote_cache
                .lock()
                .map_err(|_| unavailable())?
                .get(&group.group_id)
                .and_then(Weak::upgrade)
            {
                return Ok(participant);
            }
            let routes = control.replica_routes(group).await?;
            if routes.members.iter().any(|member| {
                member.worker_id == self.member.worker_id
                    && member.pool_id == self.member.pool_id
                    && member.node_id == self.member.node_id
            }) {
                let native = self.native_participant(group).await?;
                self.bind_native(native).await?;
            }
            let client = crate::oltp_replication::service_rpc::ParticipantClient::new(
                group.clone(),
                self.pool.resources().clone(),
                self.manager.client(),
                control.clone(),
                crate::oltp_replication::authority::control::ServiceAction::Owner,
            )?;
            let participant = Arc::new(ParticipantHandle::remote(
                group.clone(),
                self.pool.resources().clone(),
                client,
            )?);
            let mut cache = self.remote_cache.lock().map_err(|_| unavailable())?;
            cache.retain(|_, value| value.strong_count() != 0);
            cache.insert(group.group_id, Arc::downgrade(&participant));
            Ok(participant)
        })
    }
    fn create_data<'a>(
        &'a self,
        table: &'a TableDescriptor,
        pin: &'a PinnedCatalog,
    ) -> ResolveFuture<'a, ShardRoute> {
        Box::pin(async move {
            let shard_id = id(table.id, b"initial-shard");
            let group = GroupIdentity {
                cluster_id: pin.catalog.cluster_id,
                database: pin.catalog.database.clone(),
                group_id: id(shard_id, b"data-group"),
                kind: GroupKind::Data {
                    table_id: table.id,
                    shard_id,
                },
                group_format_version: GROUP_FORMAT_VERSION,
            };
            let participant = self.native_participant(&group).await?;
            let authority = *self.authority.read().map_err(|_| unavailable())?;
            if participant.activation().await?.is_none() {
                participant
                    .fence_initial(
                        id(group.group_id, b"initial-fence"),
                        authority,
                        self.catalog,
                        1,
                        1,
                        KeySpan {
                            start: None,
                            end: None,
                        },
                    )
                    .await?;
            }
            if self.control.group(group.group_id).await?.is_none() {
                let store = self.pool.group(group.clone()).await?;
                let running = self
                    .manager
                    .start_data(
                        store,
                        self.member.clone(),
                        self.peers.clone(),
                        raft_config(),
                    )
                    .await?;
                register(&self.control, &self.operator, &running, authority).await?;
            }
            self.bind_native(participant.clone()).await?;
            let epochs = participant.epochs().await?;
            Ok(ShardRoute {
                group,
                span: KeySpan {
                    start: None,
                    end: None,
                },
                schema_epoch: epochs.schema_epoch,
                routing_epoch: epochs.routing_epoch,
            })
        })
    }
}
async fn initialize<C, M>(
    group: &Arc<RaftConsensus<C, M>>,
    peers: &BTreeMap<u64, ReplicaMember>,
) -> Result<(), ProtocolError>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    if !group
        .raft
        .is_initialized()
        .await
        .map_err(|_| unavailable())?
    {
        group
            .raft
            .initialize(peers.clone())
            .await
            .map_err(|_| unavailable())?;
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if group.ensure_linearizable().await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(unavailable());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
async fn register<C, M>(
    control: &Arc<ControlHandle>,
    operator: &LocalOperatorPermit,
    group: &Arc<RaftConsensus<C, M>>,
    authority: Authority,
) -> Result<(), ProtocolError>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    let observation = group.observe_membership().await?;
    if control
        .group(observation.group().group_id)
        .await?
        .is_some_and(|record| record.replica_count != 0)
    {
        return Ok(());
    }
    let generation = id(observation.group().group_id, b"control-membership");
    let reply = control
        .begin_group_local(
            operator,
            Uuid::new_v4(),
            generation,
            observation.group().clone(),
        )
        .await?
        .reply;
    let ControlReply::GroupBuilding { count, .. } = reply else {
        return Err(unavailable());
    };
    for page in observation
        .replicas()
        .chunks(128)
        .skip((count as usize).div_ceil(128))
    {
        accepted(
            control
                .group_page_local(
                    operator,
                    Uuid::new_v4(),
                    generation,
                    Page::new(page.to_vec()).map_err(|_| unavailable())?,
                )
                .await?
                .reply,
        )?;
    }
    let proof = VerifiedMembership::from_consensus(observation, generation, authority)?;
    accepted(
        control
            .seal_group_local(operator, Uuid::new_v4(), proof)
            .await?
            .reply,
    )
}
// Static peers share this profile. The RF1/component profile remains explicit
// in bootstrap::raft_config; a throttled one-CPU worker needs time to schedule
// all metadata groups without repeatedly electing a new leader.
pub(super) fn registered_raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    }
}

fn raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        ..Default::default()
    }
}
pub(super) fn queue_limits(execution: &ExecutionConfig) -> QueueLimits {
    QueueLimits {
        max_pending_requests: execution.max_pending_requests as usize,
        max_queue_bytes_per_replica: execution.max_queue_bytes_per_replica as usize,
        max_retained_result_bytes_per_worker: execution.max_retained_result_bytes_per_worker
            as usize,
        result_retention_ms: execution.result_retention_ms,
    }
}

pub(super) fn queue_limits_for_formats(
    execution: &ExecutionConfig,
    scoped: bool,
) -> (QueueLimits, Option<QueueLimits>) {
    let mut legacy = queue_limits(execution);
    if !scoped {
        return (legacy, None);
    }
    let scoped = QueueLimits {
        max_pending_requests: legacy.max_pending_requests / 2,
        max_queue_bytes_per_replica: legacy.max_queue_bytes_per_replica / 2,
        max_retained_result_bytes_per_worker: legacy.max_retained_result_bytes_per_worker / 2,
        result_retention_ms: legacy.result_retention_ms,
    };
    legacy.max_pending_requests -= scoped.max_pending_requests;
    legacy.max_queue_bytes_per_replica -= scoped.max_queue_bytes_per_replica;
    legacy.max_retained_result_bytes_per_worker -= scoped.max_retained_result_bytes_per_worker;
    (legacy, Some(scoped))
}
fn accepted(reply: ControlReply) -> Result<(), ProtocolError> {
    use crate::oltp_replication::authority::control::Fault;
    let ControlReply::Rejected(fault) = reply else {
        return Ok(());
    };
    let code = match fault {
        Fault::Unauthorized => ErrorCode::Unauthorized,
        Fault::Invalid => ErrorCode::ValidationError,
        Fault::Stale | Fault::Conflict | Fault::Incomplete => ErrorCode::Conflict,
        Fault::NotFound => ErrorCode::NotFound,
        Fault::NotInitialized | Fault::Busy | Fault::NotReady => ErrorCode::StorageUnavailable,
        // `data.direct.v1` is active and this worker did not advertise it.
        Fault::DirectCapabilityRequired => ErrorCode::UnsupportedOperation,
    };
    Err(ProtocolError::new(
        code,
        format!("Control bootstrap transition rejected: {fault:?}"),
        Outcome::NotApplied,
    ))
}
fn digest(value: &impl serde::Serialize) -> Result<Digest, ProtocolError> {
    crate::oltp_replication::owned::serialized_digest(value)
}
fn id(parent: Uuid, label: &[u8]) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(parent.as_bytes());
    hash.update(label);
    let bytes: [u8; 32] = hash.finalize().into();
    Uuid::from_slice(&bytes[..16]).expect("SHA256 UUID")
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "OLTP typed runtime bootstrap is unavailable",
        Outcome::Unknown,
    )
}
