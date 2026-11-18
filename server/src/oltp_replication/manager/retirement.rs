//! Replica closure is distinct from pool detach or worker termination. The
//! placement controller must keep its durable per-group transition exclusive
//! through this step and join the storage/queue proofs before infrastructure
//! removal. No caller-supplied wire DTO constructs the capabilities below.
use super::super::authority::control::{
    ControlClient, ControlHandle, GroupRecord, ReplicaMove, ReplicaMovePhase, ReplicaRetiredStamp,
};
use super::*;
use crate::oltp::pool::ExecutionClass;
use std::time::Duration;

pub(crate) struct ReplicaRetirementPermit {
    publication: RetirementPublication,
    published: GroupRecord,
    removed: ReplicaMember,
    worker: WorkerIdentity,
    observation: Option<GroupMembershipObservation>,
}

enum RetirementPublication {
    Local(Arc<ControlHandle>),
    Remote {
        control: Arc<ControlClient>,
        binding: Digest,
        pool_generation: u64,
    },
}

impl ReplicaRetirementPermit {
    pub(crate) async fn from_published(
        control: Arc<ControlHandle>,
        worker: WorkerIdentity,
        removed: ReplicaMember,
        observation: GroupMembershipObservation,
    ) -> Result<Self, ProtocolError> {
        worker.validate().map_err(|_| invalid())?;
        removed.validate().map_err(|_| invalid())?;
        if removed.worker_id != worker.worker_id
            || observation
                .replicas
                .iter()
                .any(|member| member.node_id == removed.node_id)
            || observation.group.kind == GroupKind::Control
        {
            return Err(invalid());
        }
        let published = control
            .group(observation.group.group_id)
            .await?
            .ok_or_else(unavailable)?;
        let mut root = super::super::authority::control::group_seed();
        for member in observation.replicas.iter() {
            root = super::super::authority::control::group_step(root, member);
        }
        if published.identity != observation.group
            || published.replica_count != observation.replicas.len() as u64
            || published.replica_root
                != super::super::authority::control::group_root(published.replica_count, root)
            || published.membership_position.index > observation.applied_at.index
        {
            return Err(invalid());
        }
        Ok(Self {
            publication: RetirementPublication::Local(control),
            published,
            removed,
            worker,
            observation: Some(observation),
        })
    }

    pub(crate) async fn from_remote(
        control: Arc<ControlClient>,
        plan: &ReplicaMove,
    ) -> Result<Self, ProtocolError> {
        let source = plan.remote_source.as_ref().ok_or_else(invalid)?;
        let ReplicaMovePhase::Published { placement, .. } = &plan.phase else {
            return Err(invalid());
        };
        let permit = Self {
            publication: RetirementPublication::Remote {
                control,
                binding: plan.binding()?,
                pool_generation: source.pool_generation,
            },
            published: placement.clone(),
            removed: plan.removed().ok_or_else(invalid)?.clone(),
            worker: source.worker,
            observation: None,
        };
        permit.verify_publication().await?;
        Ok(permit)
    }

    async fn verify_publication(&self) -> Result<(), ProtocolError> {
        let valid = match &self.publication {
            RetirementPublication::Local(control) => {
                control.group(self.published.identity.group_id).await?.as_ref() == Some(&self.published)
            }
            RetirementPublication::Remote { control, binding, .. } => {
                let view = control.remote_replica_move(self.published.generation).await?;
                view.eligible
                    && view.plan.binding()? == *binding
                    && view.plan.remote_source.as_ref().is_some_and(|source| source.worker == self.worker)
                    && view.plan.removed() == Some(&self.removed)
                    && matches!(&view.plan.phase, ReplicaMovePhase::Published { placement, .. }
                        if placement == &self.published)
            }
        };
        if !valid {
            return Err(invalid());
        }
        Ok(())
    }

    fn retired(self, applied_at: AppliedPosition) -> ReplicaRetired {
        ReplicaRetired {
            stamp: ReplicaRetiredStamp {
                group: self.published.identity,
                generation: self.published.generation,
                replica: self.removed,
                worker: self.worker,
                applied_at,
            },
            _observation: self.observation,
        }
    }
}
/// Only these scoped facts are attested: the permanent local routes are closed,
/// admitted handlers have returned, and hosted Raft/native-log tasks are joined.
/// Other pool-native continuations keep their own ownership. This capability
/// alone never authorizes detaching a volume or terminating a queue holder.
pub(crate) struct ReplicaRetired {
    stamp: ReplicaRetiredStamp,
    _observation: Option<GroupMembershipObservation>,
}
impl ReplicaRetired {
    pub(crate) fn stamp(&self) -> &ReplicaRetiredStamp {
        &self.stamp
    }
}
impl HostedGroupManager {
    pub(crate) async fn retire_removed_replica(
        &self,
        permit: ReplicaRetirementPermit,
        store: &GroupStore,
    ) -> Result<ReplicaRetired, ProtocolError> {
        let _starts = self.starts.lock().await;
        if self.client.local_identity() != permit.worker
            || store.identity() != &permit.published.identity
            || store.pool().identity().pool_id != permit.removed.pool_id
            || store.pool().identity().worker_id != permit.removed.worker_id
        {
            return Err(invalid());
        }
        if let RetirementPublication::Remote { pool_generation, .. } = &permit.publication {
            let pool = store.pool().clone();
            let observed = self.resources().execute(ExecutionClass::Control, 0, 0, move || {
                pool.observe_registered_mount()
            }).await?;
            if observed.expected.worker != permit.worker
                || observed.expected.generation != *pool_generation
                || &observed.expected.identity != store.pool().identity()
            {
                return Err(invalid());
            }
        }
        permit.verify_publication().await?;
        if let Some(retired) = store
            .replica_retirement(permit.published.generation)
            .await?
        {
            if retired.group != permit.published.identity
                || retired.generation != permit.published.generation
                || retired.replica != permit.removed
                || retired.worker != permit.worker
                || retired.applied_at.index < permit.published.membership_position.index
            {
                return Err(invalid());
            }
            return Ok(permit.retired(retired.applied_at));
        }
        let (running, raft, services) = {
            let mut routes = self.routes.lock().map_err(|_| unavailable())?;
            let group = routes
                .get_mut(&permit.published.identity.group_id)
                .ok_or_else(unavailable)?;
            let route = group
                .replicas
                .get_mut(&permit.removed.node_id)
                .ok_or_else(unavailable)?;
            if route.member != permit.removed {
                return Err(invalid());
            }
            route.retiring = true;
            (
                route.running.clone(),
                group.dispatcher.clone(),
                group.services.clone(),
            )
        };
        // Catch up to the actual published membership before closing traffic.
        running
            .removed_applied(permit.removed.clone(), permit.published.clone())
            .await?;
        if let Some(services) = services {
            services.retire(permit.removed.node_id).await?;
        }
        raft.retire(permit.removed.node_id).await?;
        let applied_at = running
            .removed_applied(permit.removed.clone(), permit.published.clone())
            .await?;
        running.stop().await?;
        store
            .record_replica_retirement(ReplicaRetiredStamp {
                group: permit.published.identity.clone(),
                generation: permit.published.generation,
                replica: permit.removed.clone(),
                worker: permit.worker,
                applied_at,
            })
            .await?;
        self.routes
            .lock()
            .map_err(|_| unavailable())?
            .get_mut(&permit.published.identity.group_id)
            .ok_or_else(unavailable)?
            .replicas
            .remove(&permit.removed.node_id);
        permit.verify_publication().await?;
        Ok(permit.retired(applied_at))
    }
}
impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(super) async fn verify_removed(
        &self,
        removed: ReplicaMember,
        published: GroupRecord,
    ) -> Result<AppliedPosition, ProtocolError> {
        if self.state.store().identity() != &published.identity
            || self.state.resident().node_id() != removed.node_id
            || self.state.store().pool().identity().pool_id != removed.pool_id
            || self.state.store().pool().identity().worker_id != removed.worker_id
        {
            return Err(invalid());
        }
        self.raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(
                Some(published.membership_position.index),
                "retired replica applied published placement",
            )
            .await
            .map_err(|_| unavailable())?;
        let mut state = self.state.clone();
        let (applied, membership) =
            openraft::storage::RaftStateMachine::<C>::applied_state(&mut state)
                .await
                .map_err(|_| unavailable())?;
        let membership = membership.membership();
        if membership.get_joint_config().len() != 1 {
            return Err(invalid());
        }
        let voters = &membership.get_joint_config()[0];
        if voters.contains(&removed.node_id) || voters.len() as u64 != published.replica_count {
            return Err(invalid());
        }
        let mut root = super::super::authority::control::group_seed();
        for node in voters {
            let member = membership
                .nodes()
                .find(|(candidate, _)| *candidate == node)
                .map(|(_, member)| member)
                .ok_or_else(invalid)?;
            root = super::super::authority::control::group_step(root, member);
        }
        if super::super::authority::control::group_root(published.replica_count, root)
            != published.replica_root
        {
            return Err(invalid());
        }
        Ok(super::super::hosted::position(
            applied.ok_or_else(unavailable)?,
        ))
    }
}
