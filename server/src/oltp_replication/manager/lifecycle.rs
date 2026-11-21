//! Native membership operations used by the placement controller. These do
//! not publish routes or authorize volume/worker removal. The controller must
//! retain its durable per-group transition while using these capabilities.
use super::*;
use crate::oltp::pool::{ByteClass, DamagedReplicaRemoved, OltpPool, ReleaseOutcome, ScratchLease};
use crate::oltp_replication::authority::control::{
    ControlClient, GroupDamageRecord, GroupDamageReplacement, GroupDamageReport,
};
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
            #[cfg(test)]
            crate::oltp_runtime::recovery_tests::pause(
                "replacement_learner_appended", self.state.store().identity().group_id,
            ).await;
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
    #[cfg(test)]
    pub(crate) async fn replace_voter(
        &self,
        learner: ReplicatedLearner,
        removed: u64,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        self.replace_voter_applied(learner, removed, None, true)
            .await
    }

    async fn replace_voter_applied(
        &self,
        learner: ReplicatedLearner,
        removed: u64,
        target_applied: Option<&PlacementCatchUp<'_>>,
        retain_removed: bool,
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
        self.finish_replacement(
            &current.replicas,
            &next,
            &learner.target,
            target_applied,
            retain_removed,
        )
        .await
    }
    /// Drive or resume the exact durable replacement plan. A stable voter set
    /// is evidence for this plan only if it replaces the specified voter.
    ///
    /// `retain_removed` keeps the removed voter as a learner in the final
    /// membership. A live removed replica needs that: it must apply the final
    /// membership to retire (`verify_removed`), and the leader stops
    /// replicating to a node the final membership no longer names. Only a
    /// lost voter, which never retires (system replacement), is dropped.
    pub(crate) async fn reconcile_voter_replacement(
        &self,
        old: &[ReplicaMember],
        next: &[ReplicaMember],
        target: &ReplicaMember,
        removed: u64,
        target_applied: Option<&PlacementCatchUp<'_>>,
        retain_removed: bool,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        validate_voters(old).map_err(|_| invalid())?;
        validate_voters(next).map_err(|_| invalid())?;
        if old.len() != next.len()
            || !next.contains(target)
            || !old.iter().any(|member| member.node_id == removed)
            || old.iter().any(|member| member.node_id == target.node_id)
            || old
                .iter()
                .filter(|member| member.node_id != removed)
                .ne(next.iter().filter(|member| *member != target))
        {
            return Err(invalid());
        }

        if let Ok(observed) = self.observe_membership().await {
            if observed.replicas() == old {
                let learner = self.replicate_learner(observed, target.clone()).await?;
                self.replace_voter_applied(learner, removed, target_applied, retain_removed)
                    .await?;
            } else if observed.replicas() != next {
                return Err(invalid());
            }
        } else {
            let _change = self.membership_changes.lock().await;
            self.finish_replacement(old, next, target, target_applied, retain_removed)
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
        retain_removed: bool,
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
                    retain_removed,
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

// ----- NS14: quarantined groups (plan section 3, "Raft safety of a quarantined group") -----

/// Where a worker sends `GroupDamaged` and reads Control's record: the
/// [`ControlClient`](super::super::authority::control::ControlClient).
pub(crate) trait DamageControl: Send + Sync {
    fn report(
        &self,
        report: GroupDamageReport,
    ) -> Pin<Box<dyn Future<Output = Result<GroupDamageRecord, ProtocolError>> + Send + '_>>;
    fn record(
        &self,
        group_id: uuid::Uuid,
        node_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GroupDamageRecord>, ProtocolError>> + Send + '_>>;
}
impl DamageControl for ControlClient {
    fn report(
        &self,
        report: GroupDamageReport,
    ) -> Pin<Box<dyn Future<Output = Result<GroupDamageRecord, ProtocolError>> + Send + '_>> {
        Box::pin(self.report_group_damage(report))
    }
    fn record(
        &self,
        group_id: uuid::Uuid,
        node_id: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GroupDamageRecord>, ProtocolError>> + Send + '_>>
    {
        Box::pin(self.group_damage(group_id, node_id))
    }
}
/// Receives the replicas the manager stopped after run-time damage.
pub(crate) trait DamageSink: Send + Sync {
    fn damaged(&self, group: GroupIdentity, replica: ReplicaMember, pool: OltpPool);
    /// RV-APPLY F4: the supervisor stopped this replica after its Raft core
    /// kept failing. Control replaces the replica; once that change commits
    /// the replica's bytes on `pool` are released (RV-FINAL M4).
    fn gave_up(&self, _group: GroupIdentity, _replica: ReplicaMember, _pool: OltpPool) {}
}

/// Rules 2 and 4 on the damaged worker. Reports the replica until Control
/// acknowledges it, then polls Control's record. Once the record says the
/// change removing this replica committed, releases the group's bytes and
/// returns. A refused pool (`pool` is `None`) has nothing to release, so the
/// acknowledgment ends it. Never deletes anything without that record (rule 5).
pub(crate) async fn settle_damage(
    control: Arc<dyn DamageControl>,
    report: GroupDamageReport,
    pool: Option<OltpPool>,
    retry: Duration,
) -> ReleaseOutcome {
    let mut delay = retry;
    let mut acknowledged = false;
    loop {
        let step = async {
            let record = if acknowledged {
                control
                    .record(report.group.group_id, report.replica.node_id)
                    .await?
                    .ok_or_else(unavailable)?
            } else {
                control.report(report.clone()).await?
            };
            acknowledged = true;
            match (record.removal(), &pool) {
                (_, None) => Ok(Some(ReleaseOutcome::AlreadyReleased)),
                (Some(removed), Some(pool)) => pool.release_damaged_group(removed).await.map(Some),
                (None, Some(_)) => Ok(None),
            }
        }
        .await;
        match step {
            Ok(Some(outcome)) => return outcome,
            Ok(None) => {}
            Err(error) => tracing::warn!(
                group = %report.group.group_id,
                node = report.replica.node_id,
                %error,
                "group damage report not settled yet; retrying"
            ),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

/// Replicas whose damage report is being settled, so each settles once.
pub(crate) type SettlingReplicas = Arc<Mutex<BTreeSet<(uuid::Uuid, u64)>>>;

/// Reports and settles one damaged replica in the background, once per
/// replica at a time.
pub(crate) fn spawn_settlement(
    control: Arc<ControlClient>,
    settling: SettlingReplicas,
    report: GroupDamageReport,
    pool: Option<OltpPool>,
) {
    let key = (report.group.group_id, report.replica.node_id);
    if !settling
        .lock()
        .is_ok_and(|mut settling| settling.insert(key))
    {
        return;
    }
    tokio::spawn(async move {
        let outcome = settle_damage(control, report, pool, Duration::from_secs(1)).await;
        tracing::warn!(group = %key.0, node = key.1, ?outcome, "group damage settled");
        if let Ok(mut settling) = settling.lock() {
            settling.remove(&key);
        }
    });
}

/// NS14b: the run-time damage reporter. It is installed before the system
/// groups start, so a run-time stop of a Timestamps, Status or Catalog
/// replica is reported too (rule 2); reports wait in memory until the worker
/// has a Control client (`connect`). Control replicas are never reported:
/// Control's own membership flows replace them.
pub(crate) struct ReportingDamageSink {
    control: Mutex<Option<std::sync::Weak<ControlClient>>>,
    waiting: Mutex<Vec<(GroupDamageReport, Option<OltpPool>)>>,
    settling: SettlingReplicas,
}
impl ReportingDamageSink {
    const MAX_WAITING: usize = 1024;

    pub(crate) fn new(settling: SettlingReplicas) -> Arc<Self> {
        Arc::new(Self {
            control: Mutex::new(None),
            waiting: Mutex::new(Vec::new()),
            settling,
        })
    }
    /// Sends every waiting report, and every later one, through `control`.
    pub(crate) fn connect(&self, control: &Arc<ControlClient>) {
        if let Ok(mut slot) = self.control.lock() {
            *slot = Some(Arc::downgrade(control));
        }
        let waiting = self
            .waiting
            .lock()
            .map(|mut waiting| std::mem::take(&mut *waiting))
            .unwrap_or_default();
        for (report, pool) in waiting {
            spawn_settlement(control.clone(), self.settling.clone(), report, pool);
        }
    }
}
impl DamageSink for ReportingDamageSink {
    fn damaged(&self, group: GroupIdentity, replica: ReplicaMember, pool: OltpPool) {
        if group.kind == GroupKind::Control {
            tracing::error!(group = %group.group_id, node = replica.node_id,
                "a Control replica stopped after storage damage; replace it through Control membership");
            return;
        }
        let Some(damage) = pool.group_damage(group.group_id) else {
            return;
        };
        let report = GroupDamageReport {
            group,
            replica,
            reason: damage.cause.into(),
            first_bad_seq: damage.first_bad_seq,
            refusal: None,
        };
        self.send(report, Some(pool));
    }
    /// Reported with the existing wire reason `pool_refused` (class `other`),
    /// which every Control of this release decodes; a dedicated reason needs
    /// a gated variant (RV-FINAL M4 follow-up). The log line names the real
    /// cause. Unlike a refused pool, the pool is open, so the replica's
    /// bytes are released once Control's replacement commits.
    fn gave_up(&self, group: GroupIdentity, replica: ReplicaMember, pool: OltpPool) {
        if group.kind == GroupKind::Control {
            tracing::error!(group = %group.group_id, node = replica.node_id,
                "a Control replica was given up; replace it through Control membership");
            return;
        }
        tracing::error!(group = %group.group_id, node = replica.node_id,
            "reporting a given-up replica (its Raft core kept stopping) as pool_refused/other; \
             its bytes are released after the replacement commits");
        let report = GroupDamageReport {
            group,
            replica,
            reason: crate::oltp_replication::authority::control::GroupDamageReason::PoolRefused,
            first_bad_seq: None,
            refusal: Some(crate::oltp_replication::authority::control::PoolRefusal::Other),
        };
        self.send(report, Some(pool));
    }
}
impl ReportingDamageSink {
    fn send(&self, report: GroupDamageReport, pool: Option<OltpPool>) {
        let control = self
            .control
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(std::sync::Weak::upgrade));
        match control {
            Some(control) => spawn_settlement(control, self.settling.clone(), report, pool),
            None => {
                if let Ok(mut waiting) = self.waiting.lock()
                    && waiting.len() < Self::MAX_WAITING
                {
                    waiting.push((report, pool));
                }
            }
        }
    }
}

impl HostedGroupManager {
    /// Installs where run-time damage is reported (rule 2).
    pub(crate) fn install_damage_sink(&self, sink: Arc<dyn DamageSink>) {
        if let Ok(mut slot) = self.damage_sink.lock() {
            *slot = Some(sink);
        }
    }
    /// A sink is installed already (the registered worker's early reporter).
    pub(crate) fn has_damage_sink(&self) -> bool {
        self.damage_sink.lock().is_ok_and(|slot| slot.is_some())
    }

    /// Section 3 rule 1 at run time: when kv quarantines this replica's group,
    /// the host stops. Its routes close first, so the replica never votes,
    /// acknowledges, serves a ReadIndex or exports a snapshot again; then Raft
    /// shuts down, and the replica is reported. The watcher holds the host,
    /// the routes and the sink weakly, and ends when the pool closes.
    pub(super) fn stop_on_damage<C, M>(&self, running: &Managed<C, M>, node: u64)
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
        M: TypedMachine<C>,
    {
        let store = running.state.store();
        let group = store.identity().group_id;
        let mut damage = store.pool().watch_damage();
        let routes = Arc::downgrade(&self.routes);
        let sink = Arc::downgrade(&self.damage_sink);
        let host = Arc::downgrade(running);
        tokio::spawn(async move {
            let damaged = damage
                .wait_for(|groups| groups.contains(&group))
                .await
                .is_ok();
            if !damaged {
                return;
            }
            let Some(running) = host.upgrade() else {
                return;
            };
            let Some(routes) = routes.upgrade() else {
                return;
            };
            let stopped = {
                let Ok(mut routes) = routes.lock() else {
                    return;
                };
                let Some(entry) = routes.get_mut(&group) else {
                    return;
                };
                let same = entry.replicas.get(&node).is_some_and(|route| {
                    std::ptr::addr_eq(Arc::as_ptr(&route.typed), Arc::as_ptr(&running))
                });
                if !same {
                    return;
                }
                let route = entry.replicas.remove(&node);
                (entry.dispatcher.clone(), entry.services.clone(), route)
            };
            let (dispatcher, services, route) = stopped;
            tracing::error!(
                %group,
                node,
                "stopping the Raft host of a group damaged at run time; this identity never \
                 votes or acknowledges again"
            );
            if let Some(services) = services {
                let _ = services.retire(node).await;
            }
            let _ = dispatcher.retire(node).await;
            if let Err(error) = running.shutdown().await {
                tracing::warn!(%group, node, %error, "damaged host stopped with an error");
            }
            let sink = sink
                .upgrade()
                .and_then(|sink| sink.lock().ok().and_then(|sink| sink.clone()));
            if let (Some(sink), Some(route)) = (sink, route) {
                let store = running.state.store();
                sink.damaged(store.identity().clone(), route.member, store.pool().clone());
            }
        });
    }
}

/// Rule 3 evidence after the leader replaced a damaged voter.
pub(crate) struct DamagedReplacement {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) membership: GroupMembershipObservation,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) removed: DamagedReplicaRemoved,
    pub(crate) proof: GroupDamageReplacement,
}

impl<C, M> RaftConsensus<C, M>
where
    C: TypedConfig<
            R = Applied<Option<<C as TypedConfig>::Reply>>,
            MembershipEdit = openraft::impls::ChangeMembers<u64, ReplicaMember>,
        >,
    M: TypedMachine<C>,
{
    /// Section 3 rule 3 on the group's leader: replaces the damaged voter with
    /// a fresh identity through joint consensus. A retry uses the same planned
    /// members whether the group is still on the old configuration, already
    /// in joint consensus, or on the final configuration.
    pub(crate) async fn replace_damaged_voter(
        &self,
        old: &[ReplicaMember],
        next: &[ReplicaMember],
        damaged: &ReplicaMember,
        fresh: ReplicaMember,
    ) -> Result<DamagedReplacement, ProtocolError> {
        if !old.contains(damaged) {
            return Err(invalid());
        }
        let membership = self
            // As before REP5: the removed voter stays a learner.
            .reconcile_voter_replacement(old, next, &fresh, damaged.node_id, None, true)
            .await?;
        let proof = GroupDamageReplacement {
            group: membership.group.clone(),
            removed: damaged.clone(),
            replacement: fresh,
            committed_at: membership.applied_at,
        };
        Ok(DamagedReplacement {
            removed: DamagedReplicaRemoved {
                group: membership.group.clone(),
                removed: damaged.clone(),
                committed_at: membership.applied_at,
            },
            membership,
            proof,
        })
    }
}
