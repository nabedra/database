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
    hosted::{HostedState, RaftConsensus, TypedMachine, batched_config},
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

mod data_factory;
mod lifecycle;
pub(crate) use lifecycle::{
    AppliedPlacementMembership, DamageSink, ReplicatedLearner, ReportingDamageSink,
    SettlingReplicas, spawn_settlement,
};
#[allow(unused_imports)]
pub(crate) use lifecycle::{DamageControl, DamagedReplacement, settle_damage};
#[cfg(test)]
mod damage_tests;
#[cfg(test)]
mod damage_scheduler_tests;
mod retirement;
mod supervision;
pub(crate) use retirement::{ReplicaRetired, ReplicaRetirementPermit};
pub(crate) mod local;
pub(crate) use data_factory::placement::{
    PlacementCatalog, RemotePlacementTarget, ReplicaPlacement,
};
pub(crate) use data_factory::split as shard_split;
pub(crate) use data_factory::damage_replacement::{
    DamageHost, DamageReplacementEndpoint, DamageScheduler, FactoryDamageHost,
};
pub(crate) use data_factory::{DataGroupClient, DataGroupFactory, DataGroupSpec};
pub(crate) use local::{
    GroupPlacement, LocalOperatorPermit, LocalWorker, RegisteredConfig, RegisteredSystemGroups,
    RegisteredWorker, SystemPlacement,
};
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
    fn removed_applied(
        &self,
        removed: ReplicaMember,
        published: super::authority::control::GroupRecord,
    ) -> Pin<Box<dyn Future<Output = Result<AppliedPosition, ProtocolError>> + Send + '_>>;
}
impl<C, M> Running for RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    fn leader(&self) -> Option<u64> {
        // A stopped core never counts as the leader (RV-APPLY F4).
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        metrics
            .running_state
            .is_ok()
            .then_some(metrics.current_leader)
            .flatten()
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
    fn removed_applied(
        &self,
        removed: ReplicaMember,
        published: super::authority::control::GroupRecord,
    ) -> Pin<Box<dyn Future<Output = Result<AppliedPosition, ProtocolError>> + Send + '_>> {
        Box::pin(self.verify_removed(removed, published))
    }
}
struct GroupRoutes {
    dispatcher: Arc<ReplicaDispatcher>,
    services: Option<Arc<ReplicaDispatcher>>,
    replicas: BTreeMap<u64, ReplicaRoute>,
}
struct ReplicaRoute {
    member: ReplicaMember,
    running: Arc<dyn Running>,
    typed: Arc<dyn std::any::Any + Send + Sync>,
    bootstrap: BTreeMap<u64, ReplicaMember>,
    config: Digest,
    retiring: bool,
}
pub(crate) struct HostedGroupManager {
    shared: Arc<WorkerSharedResources>,
    client: WorkerClient,
    registry: Arc<EndpointRegistry>,
    /// Shared with the run-time damage watchers (NS14), which hold it weakly.
    routes: Arc<Mutex<BTreeMap<uuid::Uuid, GroupRoutes>>>,
    ownership: Option<Arc<dyn Send + Sync>>,
    starts: tokio::sync::Mutex<()>,
    /// Where a replica stopped by run-time damage is reported (NS14 rule 2).
    damage_sink: Arc<Mutex<Option<Arc<dyn DamageSink>>>>,
    /// Retry, restart and stop counters of every core (`/v1/runtime/stats`).
    health: Arc<super::hosted::transient::RaftHealthStats>,
}
impl HostedGroupManager {
    fn hosted_data(
        &self,
        identity: &GroupIdentity,
        node: u64,
    ) -> Result<ManagedData, ProtocolError> {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        let route = routes
            .get(&identity.group_id)
            .and_then(|group| group.replicas.get(&node))
            .ok_or_else(unavailable)?;
        let running = route
            .typed
            .clone()
            .downcast::<RaftConsensus<DataConfig, ParticipantMachine>>()
            .map_err(|_| invalid())?;
        if running.state.store().identity() != identity {
            return Err(invalid());
        }
        Ok(running)
    }

    #[cfg(test)]
    pub(crate) fn data_voter_state(
        &self,
        group: &GroupIdentity,
        node: u64,
    ) -> Option<(Vec<u64>, u64)> {
        let hosted = self.hosted_data(group, node).ok()?;
        let metrics = hosted.raft.metrics();
        let metrics = metrics.borrow_watched();
        let index = metrics.last_applied.as_ref()?.index;
        metrics.running_state.as_ref().ok()?;
        let voters = metrics.membership_config.membership().voter_ids().collect();
        Some((voters, index))
    }

    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        client: WorkerClient,
        registry: Arc<EndpointRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            client,
            registry,
            routes: Arc::new(Mutex::new(BTreeMap::new())),
            ownership: None,
            starts: tokio::sync::Mutex::new(()),
            damage_sink: Arc::new(Mutex::new(None)),
            health: Arc::default(),
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
    /// Service routing includes the permanent local node ID, so a colocated
    /// replacement learner cannot overwrite the old replica's service route.
    pub(crate) fn bind_service(
        &self,
        identity: &GroupIdentity,
        node: u64,
        endpoint: Arc<dyn super::multiplex::Endpoint>,
    ) -> Result<(), ProtocolError> {
        let routes = self.routes.lock().map_err(|_| unavailable())?;
        let group = routes.get(&identity.group_id).ok_or_else(invalid)?;
        if group.replicas.get(&node).is_none_or(|route| route.retiring) {
            return Err(invalid());
        }
        self.registry.reserve(identity.clone())?;
        group
            .services
            .as_ref()
            .ok_or_else(invalid)?
            .add(node, endpoint)
    }
    #[cfg(test)]
    pub(crate) async fn replace_service_for_test(
        &self,
        identity: &GroupIdentity,
        node: u64,
        endpoint: Arc<dyn super::multiplex::Endpoint>,
    ) -> Result<(), ProtocolError> {
        let dispatcher = self
            .routes
            .lock()
            .map_err(|_| unavailable())?
            .get(&identity.group_id)
            .and_then(|group| group.services.clone())
            .ok_or_else(invalid)?;
        dispatcher.retire(node).await?;
        self.bind_service(identity, node, endpoint)
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
        // Section 3 rule 1: a quarantined group never starts under its old
        // identity: no vote, append acknowledgment, ReadIndex, snapshot export
        // or route.
        if machine
            .store()
            .pool()
            .is_group_quarantined(machine.store().identity().group_id)
        {
            return Err(crate::oltp::pool::quarantined_group());
        }
        let config = batched_config(config)?;
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
        let node_id = local.node_id;
        let (group, resident) = self
            .registry
            .reserve_replica(identity.clone(), local.node_id)?;
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
                if matches!(identity.kind, GroupKind::Data { .. } | GroupKind::Catalog) {
                    group.bind(Purpose::RaftProgress, dispatcher.clone())?;
                }
                group.bind(Purpose::Snapshot, dispatcher.clone())?;
                let services = if identity.kind == GroupKind::Control {
                    None
                } else {
                    let services = Arc::new(ReplicaDispatcher::new());
                    group.bind(Purpose::Authority, services.clone())?;
                    if matches!(identity.kind, GroupKind::Data { .. } | GroupKind::Catalog) {
                        group.bind(Purpose::Storage, services.clone())?;
                        group.bind(Purpose::Settlement, services.clone())?;
                        if matches!(identity.kind, GroupKind::Data { .. }) {
                            // The ingress lane for `Method::Direct` (T5).
                            group.bind(Purpose::ApiIngress, services.clone())?;
                        }
                    } else {
                        group.bind(Purpose::ServiceMutation, services.clone())?;
                    }
                    Some(services)
                };
                routes.insert(
                    identity.group_id,
                    GroupRoutes {
                        dispatcher: dispatcher.clone(),
                        services,
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
            .map_err(|_| unavailable())?
            .with_resident(resident.clone())?;
        let state = HostedState::new(machine, resident.clone())?;
        let running =
            RaftConsensus::start_with(config, network, log, state, self.health.clone()).await?;
        let endpoint = ReplicaEndpoint::new(
            running.raft.clone(),
            running.state.clone(),
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
                    retiring: false,
                },
            );
        self.stop_on_damage(&running, node_id);
        self.supervise(&running, node_id);
        Ok(running)
    }
    /// Node IDs of the replicas of `group` this worker currently hosts.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn hosted_replicas(&self, group: uuid::Uuid) -> Vec<u64> {
        self.routes
            .lock()
            .map(|routes| {
                routes
                    .get(&group)
                    .map(|group| group.replicas.keys().copied().collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
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
        if route.retiring {
            return Err(unavailable());
        }
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
        let running = self
            .start(ParticipantMachine::new(store)?, local, peers, config)
            .await?;
        super::hosted::direct_leadership::watch(&running);
        Ok(running)
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
        let _starts = self.starts.lock().await;
        let running: Vec<_> = self
            .routes
            .lock()
            .map_err(|_| unavailable())?
            .values()
            .flat_map(|routes| routes.replicas.values().map(|route| route.running.clone()))
            .collect();
        let mut failed = None;
        for replica in running {
            if let Err(error) = replica.stop().await {
                failed.get_or_insert(error);
            }
        }
        // Endpoints can own service clients whose route providers retain this
        // manager. Closing the whole worker must break those cycles after the
        // Raft tasks join, otherwise their directory ownership survives drain.
        if let Err(error) = self.registry.close_worker() {
            failed.get_or_insert(error);
        }
        self.routes.lock().map_err(|_| unavailable())?.clear();
        failed.map_or(Ok(()), Err)
    }
}
/// A quorum barrier plus the durable membership from that same hosted replica.
/// No caller-provided membership array can construct this capability.
pub(crate) struct GroupMembershipObservation {
    group: GroupIdentity,
    replicas: Page<ReplicaMember>,
    applied_at: AppliedPosition,
    _owner: Arc<crate::oltp::pool::ScratchLease>,
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
        self.observe_group_membership(false).await
    }

    pub(crate) async fn observe_stable_membership(
        &self,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.observe_group_membership(true).await
    }

    async fn observe_group_membership(
        &self,
        membership_position_only: bool,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.ensure_linearizable().await?;
        let mut state = self.state.clone();
        let (applied, membership) =
            openraft::storage::RaftStateMachine::<C>::applied_state(&mut state)
                .await
                .map_err(|_| unavailable())?;
        let position = if membership_position_only {
            *membership.log_id()
        } else {
            applied
        };
        let membership = membership.membership();
        // A route is a stable voting placement. Learners cannot become read or
        // write destinations merely by appearing in Raft's node metadata, and
        // a joint configuration must finish before publishing its new epoch.
        if membership.get_joint_config().len() != 1 {
            return Err(unavailable());
        }
        let voters = &membership.get_joint_config()[0];
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
        let owner = Arc::new(self.state.store().resources().reserve_scratch(
            crate::oltp::pool::ByteClass::Progress,
            membership.nodes().count() * std::mem::size_of::<ReplicaMember>() + 4096,
        )?);
        let replicas = Page::new(
            membership
                .nodes()
                .filter(|(node, _)| voters.contains(node))
                .map(|(_, member)| member.clone())
                .collect(),
        )
        .map_err(|_| invalid())?;
        Ok(GroupMembershipObservation {
            group: self.state.store().identity().clone(),
            replicas,
            applied_at: super::hosted::position(position.ok_or_else(unavailable)?),
            _owner: owner,
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
