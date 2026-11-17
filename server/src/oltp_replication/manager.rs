//! One worker hosts many typed Raft replicas through its existing mTLS listener.
//! Replica activation owns physical capacity before starting timers/tasks.
use super::{
    authority::{
        config::*,
        consensus::{Applied, Consensus},
        control::ControlMachine,
        status::StatusMachine,
        timestamp::TimestampMachine,
    },
    hosted::{HostedState, RaftConsensus, TypedMachine, single_entry_config},
    multiplex::{EndpointRegistry, Purpose, WorkerClient},
    typed_log::{TypedConfig, TypedLogStore},
    typed_network::{ReplicaDispatcher, ReplicaEndpoint, TypedNetworkFactory},
    types::*,
};
use crate::{
    oltp::{
        mvcc::store::ParticipantMachine,
        pool::{GroupStore, WorkerSharedResources},
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use openraft::type_config::async_runtime::watch::WatchReceiver;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub(crate) mod local;
pub(crate) use local::{LocalOperatorPermit, LocalWorker};
pub(crate) type Managed<C, M> = Arc<RaftConsensus<C, M>>;
pub(crate) type ManagedControl = Managed<ControlConfig, ControlMachine>;
pub(crate) type ManagedTimestamps = Managed<TimestampConfig, TimestampMachine>;
pub(crate) type ManagedStatus = Managed<StatusConfig, StatusMachine>;
pub(crate) type ManagedData = Managed<DataConfig, ParticipantMachine>;
pub(crate) type ManagedCatalog = Managed<CatalogConfig, ParticipantMachine>;

trait Running: Send + Sync {
    fn leader(&self) -> Option<u64>;
    fn members(&self) -> Vec<ReplicaMember>;
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>>;
}
impl<C, M> Running for RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn leader(&self) -> Option<u64> {
        self.raft.metrics().borrow_watched().current_leader
    }
    fn members(&self) -> Vec<ReplicaMember> {
        self.raft
            .metrics()
            .borrow_watched()
            .membership_config
            .membership()
            .nodes()
            .map(|(_, member)| member.clone())
            .collect()
    }
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + '_>> {
        Box::pin(self.shutdown())
    }
}
struct GroupRoutes {
    dispatcher: Arc<ReplicaDispatcher>,
    replicas: BTreeMap<u64, ReplicaRoute>,
}
struct ReplicaRoute {
    member: ReplicaMember,
    running: Arc<dyn Running>,
    typed: Arc<dyn std::any::Any + Send + Sync>,
    bootstrap: BTreeMap<u64, ReplicaMember>,
    config: Digest,
}
pub(crate) struct HostedGroupManager {
    shared: Arc<WorkerSharedResources>,
    client: WorkerClient,
    registry: Arc<EndpointRegistry>,
    routes: Mutex<BTreeMap<uuid::Uuid, GroupRoutes>>,
    ownership: Option<Arc<dyn Send + Sync>>,
    starts: tokio::sync::Mutex<()>,
}
impl HostedGroupManager {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        client: WorkerClient,
        registry: Arc<EndpointRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            client,
            registry,
            routes: Mutex::new(BTreeMap::new()),
            ownership: None,
            starts: tokio::sync::Mutex::new(()),
        })
    }
    pub(crate) fn resources(&self) -> &Arc<WorkerSharedResources> {
        &self.shared
    }
    pub(crate) fn client(&self) -> WorkerClient {
        self.client.clone()
    }
    pub(crate) fn registry(&self) -> &Arc<EndpointRegistry> {
        &self.registry
    }
    pub(crate) fn leader_candidates(
        &self,
        group: uuid::Uuid,
    ) -> Result<Vec<ReplicaMember>, ProtocolError> {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        let routes = routes.get(&group).ok_or_else(unavailable)?;
        let leader = routes
            .replicas
            .values()
            .find_map(|route| route.running.leader());
        let mut peers = routes
            .replicas
            .values()
            .find_map(|route| {
                let peers = route.running.members();
                (!peers.is_empty()).then_some(peers)
            })
            .unwrap_or_else(|| {
                routes
                    .replicas
                    .values()
                    .next()
                    .map(|route| route.bootstrap.values().cloned().collect())
                    .unwrap_or_default()
            });
        peers.sort_by_key(|member| (Some(member.node_id) != leader, member.node_id));
        Ok(peers)
    }
    async fn start<C, M>(
        &self,
        machine: M,
        local: ReplicaMember,
        bootstrap: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<Managed<C, M>, ProtocolError>
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
        M: TypedMachine<C>,
    {
        local.validate().map_err(|_| invalid())?;
        if !Arc::ptr_eq(machine.store().resources(), &self.shared)
            || machine.store().pool().identity().pool_id != local.pool_id
            || Some(local.worker_id) != self.shared.worker_id()
        {
            return Err(invalid());
        }
        let members: Vec<_> = bootstrap.values().cloned().collect();
        validate_voters(&members).map_err(|_| invalid())?;
        if bootstrap
            .iter()
            .any(|(node, member)| *node != member.node_id)
        {
            return Err(invalid());
        }
        let config = single_entry_config(config)?;
        let config_digest = super::owned::serialized_digest(config.as_ref())?;
        if let Some(existing) =
            self.existing::<C, M>(machine.store(), &local, &bootstrap, config_digest)?
        {
            return Ok(existing);
        }
        let _start = self.starts.lock().await;
        if let Some(existing) =
            self.existing::<C, M>(machine.store(), &local, &bootstrap, config_digest)?
        {
            return Ok(existing);
        }
        let identity = machine.store().identity().clone();
        let group = self.registry.reserve(identity.clone())?;
        let resident = group.activate_replica(local.node_id, &self.shared)?;
        if let Some(owner) = &self.ownership {
            resident.retain_owner(owner.clone())?;
        }
        let dispatcher = {
            let mut routes = self.routes.lock().map_err(|_| unavailable())?;
            if let Some(routes) = routes.get(&identity.group_id) {
                routes.dispatcher.clone()
            } else {
                let dispatcher = Arc::new(ReplicaDispatcher::new());
                group.bind(Purpose::Raft, dispatcher.clone())?;
                routes.insert(
                    identity.group_id,
                    GroupRoutes {
                        dispatcher: dispatcher.clone(),
                        replicas: BTreeMap::new(),
                    },
                );
                dispatcher
            }
        };
        let network = TypedNetworkFactory::<C>::new(
            self.client.clone(),
            local.clone(),
            resident.clone(),
            self.shared.clone(),
        )?;
        let log = TypedLogStore::<C>::new(machine.store().clone(), local.node_id)
            .await
            .map_err(|_| unavailable())?;
        let state = HostedState::new(machine, resident.clone())?;
        let running = RaftConsensus::start(config, network, log, state).await?;
        let endpoint = ReplicaEndpoint::new(
            running.raft.clone(),
            resident,
            self.shared.clone(),
            bootstrap.clone(),
        )?;
        if let Err(error) = dispatcher.add(local.node_id, Arc::new(endpoint)) {
            running.shutdown().await?;
            return Err(error);
        }
        self.routes
            .lock()
            .map_err(|_| unavailable())?
            .get_mut(&identity.group_id)
            .ok_or_else(unavailable)?
            .replicas
            .insert(
                local.node_id,
                ReplicaRoute {
                    member: local,
                    running: running.clone(),
                    typed: running.clone(),
                    bootstrap,
                    config: config_digest,
                },
            );
        Ok(running)
    }
    fn existing<C, M>(
        &self,
        store: &GroupStore,
        local: &ReplicaMember,
        bootstrap: &BTreeMap<u64, ReplicaMember>,
        config: Digest,
    ) -> Result<Option<Managed<C, M>>, ProtocolError>
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
        M: TypedMachine<C>,
    {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        let Some(route) = routes
            .get(&store.identity().group_id)
            .and_then(|routes| routes.replicas.get(&local.node_id))
        else {
            return Ok(None);
        };
        if route.member != *local || route.bootstrap != *bootstrap || route.config != config {
            return Err(invalid());
        }
        let typed = route
            .typed
            .clone()
            .downcast::<RaftConsensus<C, M>>()
            .map_err(|_| invalid())?;
        if typed.state.store().identity() != store.identity()
            || !std::ptr::eq(
                typed.state.store().pool().identity(),
                store.pool().identity(),
            )
        {
            return Err(invalid());
        }
        Ok(Some(typed))
    }
    pub(crate) async fn start_control(
        &self,
        store: GroupStore,
        local: ReplicaMember,
        peers: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<ManagedControl, ProtocolError> {
        self.start(ControlMachine::new(store)?, local, peers, config)
            .await
    }
    pub(crate) async fn start_timestamps(
        &self,
        store: GroupStore,
        local: ReplicaMember,
        peers: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<ManagedTimestamps, ProtocolError> {
        self.start(TimestampMachine::new(store)?, local, peers, config)
            .await
    }
    pub(crate) async fn start_status(
        &self,
        store: GroupStore,
        local: ReplicaMember,
        peers: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<ManagedStatus, ProtocolError> {
        self.start(StatusMachine::new(store)?, local, peers, config)
            .await
    }
    pub(crate) async fn start_data(
        &self,
        store: GroupStore,
        local: ReplicaMember,
        peers: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<ManagedData, ProtocolError> {
        self.start(ParticipantMachine::new(store)?, local, peers, config)
            .await
    }
    pub(crate) async fn start_catalog(
        &self,
        store: GroupStore,
        local: ReplicaMember,
        peers: BTreeMap<u64, ReplicaMember>,
        config: openraft::Config,
    ) -> Result<ManagedCatalog, ProtocolError> {
        self.start(ParticipantMachine::new(store)?, local, peers, config)
            .await
    }
    pub(crate) async fn shutdown(&self) -> Result<(), ProtocolError> {
        let running: Vec<_> = self
            .routes
            .lock()
            .map_err(|_| unavailable())?
            .values()
            .flat_map(|routes| routes.replicas.values().map(|route| route.running.clone()))
            .collect();
        for replica in running {
            replica.stop().await?;
        }
        Ok(())
    }
}
/// A quorum barrier plus the durable membership from that same hosted replica.
/// No caller-provided membership array can construct this capability.
pub(crate) struct GroupMembershipObservation {
    group: GroupIdentity,
    replicas: Page<ReplicaMember>,
    applied_at: AppliedPosition,
}
impl GroupMembershipObservation {
    pub(crate) fn group(&self) -> &GroupIdentity {
        &self.group
    }
    pub(crate) fn replicas(&self) -> &[ReplicaMember] {
        &self.replicas
    }
    pub(crate) fn applied_at(&self) -> AppliedPosition {
        self.applied_at
    }
}
impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(crate) async fn observe_membership(
        &self,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.ensure_linearizable().await?;
        let mut state = self.state.clone();
        let (applied, membership) =
            openraft::storage::RaftStateMachine::<C>::applied_state(&mut state)
                .await
                .map_err(|_| unavailable())?;
        let membership = membership.membership();
        for voters in membership.get_joint_config() {
            let members = voters
                .iter()
                .map(|id| {
                    membership
                        .nodes()
                        .find(|(node, _)| *node == id)
                        .map(|(_, member)| member.clone())
                        .ok_or_else(invalid)
                })
                .collect::<Result<Vec<_>, _>>()?;
            validate_voters(&members).map_err(|_| invalid())?;
        }
        let replicas = Page::new(
            membership
                .nodes()
                .map(|(_, member)| member.clone())
                .collect(),
        )
        .map_err(|_| invalid())?;
        Ok(GroupMembershipObservation {
            group: self.state.store().identity().clone(),
            replicas,
            applied_at: super::hosted::position(applied.ok_or_else(unavailable)?),
        })
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid hosted group placement or identity",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "hosted group authority or lifecycle unavailable",
        Outcome::Unknown,
    )
}
