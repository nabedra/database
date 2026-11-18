//! Native membership operations used by the placement controller. These do
//! not publish routes or authorize volume/worker removal. The controller must
//! retain its durable per-group transition while using these capabilities.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};
use openraft::raft::Precondition;
use std::{collections::BTreeSet, time::Duration};

pub(crate) type PlacementCatchUp<'a> = dyn Fn(AppliedPosition) -> Pin<Box<dyn Future<Output = Result<(), ProtocolError>> + Send + 'a>>
    + Sync
    + 'a;

/// Applied stable membership on one specific physical target. This does not
/// attest to a quorum or authorize a Control write.
pub(crate) struct AppliedPlacementMembership {
    group: GroupIdentity,
    voters: [ReplicaMember; 3],
    position: AppliedPosition,
}

impl AppliedPlacementMembership {
    pub(crate) fn group(&self) -> &GroupIdentity {
        &self.group
    }
    pub(crate) fn voters(&self) -> &[ReplicaMember] {
        &self.voters
    }
    pub(crate) fn position(&self) -> AppliedPosition {
        self.position
    }
}

impl RaftConsensus<DataConfig, ParticipantMachine> {
    pub(super) async fn applied_placement(
        &self,
        group: &GroupIdentity,
        voters: &[ReplicaMember; 3],
        target: &ReplicaMember,
        minimum: AppliedPosition,
        target_is_voter: bool,
    ) -> Result<AppliedPlacementMembership, ProtocolError> {
        if self.state.store().identity() != group
            || self.state.resident().node_id() != target.node_id
            || self.state.store().pool().identity().pool_id != target.pool_id
            || self.state.store().pool().identity().worker_id != target.worker_id
            || target_is_voter != voters.contains(target)
        {
            return Err(invalid());
        }
        let (applied, membership) =
            openraft::storage::RaftStateMachine::<DataConfig>::applied_state(
                &mut self.state.clone(),
            )
            .await
            .map_err(|_| unavailable())?;
        let membership_point = membership.log_id().map(super::super::hosted::position);
        let membership = membership.membership();
        if applied.is_none_or(|applied| applied.index < minimum.index)
            || membership_point != Some(minimum)
            || membership.get_joint_config().len() != 1
            || membership.get_joint_config()[0]
                .iter()
                .copied()
                .ne(voters.iter().map(|member| member.node_id))
            || voters.iter().chain(std::iter::once(target)).any(|member| {
                !membership
                    .nodes()
                    .any(|(node, actual)| *node == member.node_id && actual == member)
            })
        {
            return Err(unavailable());
        }
        Ok(AppliedPlacementMembership {
            group: group.clone(),
            voters: (*voters).clone(),
            position: minimum,
        })
    }
}

/// The actual leader has durably replicated through the learner membership
/// entry. This proves log catch-up, never application or read eligibility.
pub(crate) struct ReplicatedLearner {
    group: GroupIdentity,
    target: ReplicaMember,
    expected: Page<ReplicaMember>,
    caught_up_through: AppliedPosition,
    _owner: Arc<ScratchLease>,
}
impl ReplicatedLearner {
    pub(crate) fn target(&self) -> &ReplicaMember {
        &self.target
    }
    pub(crate) fn caught_up_through(&self) -> AppliedPosition {
        self.caught_up_through
    }
}
impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<
            R = Applied<Option<<C as TypedConfig>::Reply>>,
            MembershipEdit = openraft::impls::ChangeMembers<u64, ReplicaMember>,
        >,
    M: TypedMachine<C>,
{
    pub(crate) async fn replicate_learner(
        &self,
        expected: GroupMembershipObservation,
        target: ReplicaMember,
    ) -> Result<ReplicatedLearner, ProtocolError> {
        let _change = self.membership_changes.lock().await;
        target.validate().map_err(|_| invalid())?;
        let current = self.observe_membership().await?;
        if expected.group != current.group
            || expected.replicas != current.replicas
            || current
                .replicas
                .iter()
                .any(|member| member.node_id == target.node_id)
        {
            return Err(invalid());
        }
        {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow_watched();
            let membership = metrics.membership_config.membership();
            if membership.get_joint_config().len() != 1
                || !membership.get_joint_config()[0]
                    .iter()
                    .copied()
                    .eq(current.replicas.iter().map(|member| member.node_id))
                || membership
                    .nodes()
                    .any(|(node, existing)| *node == target.node_id && existing != &target)
            {
                return Err(invalid());
            }
        }
        let owner = Arc::new(self.state.store().resources().reserve_scratch(
            ByteClass::Progress,
            std::mem::size_of::<ReplicatedLearner>()
                + expected.replicas.capacity() * std::mem::size_of::<ReplicaMember>()
                + 4096,
        )?);
        let caught_up_through = self.replicate_exact_learner(&target).await?;
        Ok(ReplicatedLearner {
            group: current.group,
            target,
            expected: current.replicas,
            caught_up_through,
            _owner: owner,
        })
    }

    async fn replicate_exact_learner(
        &self,
        target: &ReplicaMember,
    ) -> Result<AppliedPosition, ProtocolError> {
        tokio::time::timeout(Duration::from_secs(25), async {
            let reply = self
                .raft
                .add_learner(target.node_id, target.clone(), false)
                .await
                .map_err(|_| unavailable())?;
            let minimum = *reply.log_id();
            self.raft
                .wait(Some(Duration::from_secs(25)))
                .replication(target.node_id, Some(minimum.index), u64::MAX)
                .await
                .map_err(|_| unavailable())?;
            Ok(super::super::hosted::position(minimum))
        })
        .await
        .map_err(|_| unavailable())?
    }
    pub(crate) async fn replace_voter(
        &self,
        learner: ReplicatedLearner,
        removed: u64,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.replace_voter_applied(learner, removed, None).await
    }

    async fn replace_voter_applied(
        &self,
        learner: ReplicatedLearner,
        removed: u64,
        target_applied: Option<&PlacementCatchUp<'_>>,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let _change = self.membership_changes.lock().await;
        let current = self.observe_membership().await?;
        if learner.group != current.group
            || learner.expected != current.replicas
            || !current
                .replicas
                .iter()
                .any(|member| member.node_id == removed)
        {
            return Err(invalid());
        }
        let mut next = current
            .replicas
            .iter()
            .filter(|member| member.node_id != removed)
            .cloned()
            .collect::<Vec<_>>();
        next.push(learner.target.clone());
        next.sort_by_key(|member| member.node_id);
        validate_voters(&next).map_err(|_| invalid())?;
        let next = Page::new(next).map_err(|_| invalid())?;
        self.finish_replacement(&current.replicas, &next, &learner.target, target_applied)
            .await
    }
    /// Recover an interrupted joint membership using the SAME durable planned
    /// old/new voter identities. No route is published while it is joint.
    pub(crate) async fn resume_voter_replacement(
        &self,
        expected: Page<ReplicaMember>,
        next: Page<ReplicaMember>,
        target: ReplicaMember,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.resume_voter_replacement_applied(expected, next, target, None)
            .await
    }

    async fn resume_voter_replacement_applied(
        &self,
        expected: Page<ReplicaMember>,
        next: Page<ReplicaMember>,
        target: ReplicaMember,
        target_applied: Option<&PlacementCatchUp<'_>>,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let _change = self.membership_changes.lock().await;
        validate_voters(&expected).map_err(|_| invalid())?;
        validate_voters(&next).map_err(|_| invalid())?;
        if expected.len() != next.len()
            || !next.iter().any(|member| member == &target)
            || expected
                .iter()
                .any(|member| member.node_id == target.node_id)
            || expected
                .iter()
                .filter(|member| !next.contains(member))
                .count()
                != 1
            || next
                .iter()
                .filter(|member| !expected.contains(member))
                .count()
                != 1
        {
            return Err(invalid());
        }
        self.finish_replacement(&expected, &next, &target, target_applied)
            .await
    }

    pub(crate) async fn reconcile_voter_replacement(
        &self,
        old: &[ReplicaMember],
        next: &[ReplicaMember],
        target: &ReplicaMember,
        removed: u64,
        target_applied: Option<&PlacementCatchUp<'_>>,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        if let Ok(observed) = self.observe_membership().await {
            if observed.replicas() == old {
                let learner = self.replicate_learner(observed, target.clone()).await?;
                self.replace_voter_applied(learner, removed, target_applied)
                    .await?;
            } else if observed.replicas() != next {
                return Err(invalid());
            }
        } else {
            self.resume_voter_replacement_applied(
                Page::new(old.to_vec()).map_err(|_| invalid())?,
                Page::new(next.to_vec()).map_err(|_| invalid())?,
                target.clone(),
                target_applied,
            )
            .await?;
        }
        let observed = self.observe_stable_membership().await?;
        if observed.replicas() != next {
            return Err(unavailable());
        }
        Ok(observed)
    }

    async fn finish_replacement(
        &self,
        expected: &[ReplicaMember],
        next: &[ReplicaMember],
        target: &ReplicaMember,
        target_applied: Option<&PlacementCatchUp<'_>>,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.ensure_linearizable().await?;
        let old_ids = expected
            .iter()
            .map(|member| member.node_id)
            .collect::<BTreeSet<_>>();
        let new_ids = next
            .iter()
            .map(|member| member.node_id)
            .collect::<BTreeSet<_>>();
        let (mut guarded_id, joint_started, already_final) = {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow_watched();
            let membership = metrics.membership_config.membership();
            let joint = membership.get_joint_config();
            if joint.is_empty()
                || joint.len() > 2
                || joint
                    .iter()
                    .any(|voters| voters != &old_ids && voters != &new_ids)
                || membership.nodes().any(|(node, member)| {
                    next.iter()
                        .chain(expected)
                        .find(|desired| desired.node_id == *node)
                        .is_some_and(|desired| desired != member)
                })
                || !membership
                    .nodes()
                    .any(|(node, member)| *node == target.node_id && member == target)
            {
                return Err(invalid());
            }
            (
                *metrics.membership_config.log_id(),
                joint.len() == 2,
                joint.len() == 1 && joint[0] == new_ids,
            )
        };
        if !already_final {
            // Re-establish durable learner progress after an interrupted caller
            // or election. A caller-side timeout never fabricates catch-up.
            if !joint_started {
                let minimum = self.replicate_exact_learner(target).await?;
                if let Some(target_applied) = target_applied {
                    target_applied(minimum).await?;
                }
                self.ensure_linearizable().await?;
                let metrics = self.raft.metrics();
                let metrics = metrics.borrow_watched();
                let membership = metrics.membership_config.membership();
                let joint = membership.get_joint_config();
                if joint.is_empty()
                    || joint.len() > 2
                    || joint
                        .iter()
                        .any(|voters| voters != &old_ids && voters != &new_ids)
                    || membership.nodes().any(|(node, member)| {
                        next.iter()
                            .chain(expected)
                            .find(|desired| desired.node_id == *node)
                            .is_some_and(|desired| desired != member)
                    })
                    || !membership
                        .nodes()
                        .any(|(node, member)| *node == target.node_id && member == target)
                {
                    return Err(invalid());
                }
                guarded_id = *metrics.membership_config.log_id();
                if target_applied.is_some()
                    && (joint.len() != 1
                        || joint[0] != old_ids
                        || guarded_id.map(super::super::hosted::position) != Some(minimum))
                {
                    return Err(unavailable());
                }
            }
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                self.raft.change_membership_if(
                    new_ids,
                    true,
                    [Precondition::LastMembershipLogId {
                        last_membership_log_id: guarded_id,
                    }],
                ),
            )
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
            self.raft
                .wait(Some(Duration::from_secs(10)))
                .applied_index_at_least(
                    Some(result.log_id().index),
                    "replacement membership applied",
                )
                .await
                .map_err(|_| unavailable())?;
        }
        let observed = self.observe_membership().await?;
        if observed.replicas() != next {
            return Err(invalid());
        }
        Ok(observed)
    }
}
