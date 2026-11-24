//! NS14b: Control replaces damaged Data replicas on its own (plan section 3,
//! "Raft safety of a quarantined group", rule 3).
//!
//! [`DamageScheduler`] runs in every process that hosts a Control replica and
//! acts only while that replica leads Control and the [`GroupDamageGate`]
//! (`control/group_damage.rs`) is open. For each damage record:
//!
//! 1. `Reported` (Data groups only): it asks workers which of their pools can
//!    host a fresh replica (`Offer`), preferring the damaged worker's other
//!    pools, then picks a node ID above every member's and proposes
//!    `GroupDamageReplacementBegin`. Control stores the plan, claims the group,
//!    reserves the node ID and applies the rate limits.
//! 2. `Replacing { plan }`: it asks the plan's worker to host the fresh replica
//!    as a learner (`Host`), then asks the surviving members until the Data
//!    leader runs `replace_damaged_voter` (`Replace`). It proposes
//!    `GroupDamageReplaced` with that proof; Control publishes the new
//!    placement in the same apply, so routes drop the damaged member.
//!
//! Every step is idempotent and keyed by Control's durable plan, so a new
//! Control leader resumes the same plan and never picks a second identity.
//! The damaged worker releases its bytes only after reading the `replaced`
//! record (`settle_damage`, rule 4); without a quorum that excludes the damaged
//! replica the change never commits and nothing is released (rule 5).
//!
//! The worker side is [`DamageReplacementEndpoint`] on the Control group's
//! `GroupDamage` purpose. Its actions are bound to Control's plan, so a caller
//! can only make a worker do what Control already decided; it must also be an
//! operator-pinned worker.
use super::*;
use crate::oltp_replication::{
    authority::control::{
        ControlHandle, GroupDamagePhase, GroupDamageRecord, GroupDamageReplacement,
        GroupDamageReplacementPlan, LocalOperatorPermit, ReplicaInventoryPage,
    },
    manager::{DamageControl, ManagedControl},
};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

const MAX_OFFERED_POOLS: usize = 16;
/// How long one attempt waits for the Data leader to commit the change.
const REPLACE_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    /// Which local pools could host a fresh replica of the record's group.
    Offer,
    /// Host the plan's fresh replica as a learner (its worker only).
    Host,
    /// Run `replace_damaged_voter` on the local surviving member (its leader).
    Replace,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    group_id: Uuid,
    node_id: u64,
    action: Action,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Answer {
    Pools(Vec<Uuid>),
    Hosted,
    Replaced(GroupDamageReplacement),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    group_id: Uuid,
    node_id: u64,
    nonce: Uuid,
    result: Result<Answer, ProtocolError>,
}

/// What one worker needs to serve the scheduler's actions.
#[doc(hidden)]
pub trait DamageHost: Send + Sync {
    fn manager(&self) -> &Arc<HostedGroupManager>;
    fn raft_config(&self) -> openraft::Config;
    /// Control's applied record (a quorum read).
    fn control(&self) -> &dyn DamageControl;
    /// `peer` may drive damage replacements (an operator-pinned worker).
    fn authorized(&self, peer: &WorkerIdentity) -> bool;
    /// Healthy local pools that may receive new replicas.
    fn placement_pools(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<OltpPool>, ProtocolError>> + Send + '_>>;
    /// Binds the hosted replica's service routes.
    fn wire(
        &self,
        group: &GroupIdentity,
        node: u64,
        running: ManagedData,
    ) -> Result<(), ProtocolError>;
}

/// The worker side of the scheduler's RPCs (`Purpose::GroupDamage`).
#[doc(hidden)]
pub struct DamageReplacementEndpoint {
    host: Arc<dyn DamageHost>,
    control_group: GroupIdentity,
    hosting: tokio::sync::Mutex<()>,
}

impl DamageReplacementEndpoint {
    #[doc(hidden)]
    pub fn new(host: Arc<dyn DamageHost>, control_group: GroupIdentity) -> Arc<Self> {
        Arc::new(Self {
            host,
            control_group,
            hosting: tokio::sync::Mutex::new(()),
        })
    }

    fn local(&self) -> WorkerIdentity {
        self.host.manager().client().local_identity()
    }

    async fn record(
        &self,
        group_id: Uuid,
        node_id: u64,
    ) -> Result<GroupDamageRecord, ProtocolError> {
        self.host
            .control()
            .record(group_id, node_id)
            .await?
            .ok_or_else(not_found)
    }

    async fn act(&self, request: &Request) -> Result<Answer, ProtocolError> {
        let record = self.record(request.group_id, request.node_id).await?;
        match request.action {
            Action::Offer => self.offer(&record).await,
            Action::Host => self.host_learner(&record).await,
            Action::Replace => self.replace(&record).await,
        }
    }

    /// Local pools that do not hold the group and are not the damaged pool.
    async fn offer(&self, record: &GroupDamageRecord) -> Result<Answer, ProtocolError> {
        if !record.pending() {
            return Err(conflict("the damaged replica is already replaced"));
        }
        let group = &record.report.group;
        let mut offered = Vec::new();
        for pool in self.host.placement_pools().await? {
            let id = pool.identity().pool_id;
            if id == record.report.replica.pool_id
                || pool.group_damage(group.group_id).is_some()
                || pool.unused_placement_target(group.clone()).await.is_err()
            {
                continue;
            }
            offered.push(id);
            if offered.len() == MAX_OFFERED_POOLS {
                break;
            }
        }
        Ok(Answer::Pools(offered))
    }

    /// Opens the fresh replica's scope (bound to the plan) and starts it as a
    /// learner; repeating it reopens the same scope.
    async fn host_learner(&self, record: &GroupDamageRecord) -> Result<Answer, ProtocolError> {
        let plan = record
            .plan()
            .ok_or_else(|| conflict("the damage record has no replacement plan"))?;
        let local = self.local();
        if plan.replacement.worker_id != local.worker_id {
            return Err(unauthorized());
        }
        let _hosting = self.hosting.lock().await;
        if self
            .host
            .manager()
            .hosted_data(&plan.group, plan.replacement.node_id)
            .is_ok()
        {
            return Ok(Answer::Hosted);
        }
        let pool = self
            .host
            .placement_pools()
            .await?
            .into_iter()
            .find(|pool| pool.identity().pool_id == plan.replacement.pool_id)
            .ok_or_else(unavailable)?;
        let store = pool
            .placement_learner(plan.group.clone(), plan.binding()?)
            .await?;
        let running = self
            .host
            .manager()
            .start_data(
                store,
                plan.replacement.clone(),
                plan.members
                    .iter()
                    .map(|member| (member.node_id, member.clone()))
                    .collect(),
                self.host.raft_config(),
            )
            .await?;
        self.host
            .wire(&plan.group, plan.replacement.node_id, running)?;
        tracing::info!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement",
            group = %plan.group.group_id,
            node = plan.replacement.node_id,
            pool = %plan.replacement.pool_id,
            "hosting the fresh replica of a damaged group"
        );
        Ok(Answer::Hosted)
    }

    /// Only the group's leader can commit the change; a follower answers an
    /// error and the scheduler asks the next member.
    async fn replace(&self, record: &GroupDamageRecord) -> Result<Answer, ProtocolError> {
        let plan = record
            .plan()
            .ok_or_else(|| conflict("the damage record has no replacement plan"))?;
        let local = self.local();
        let member = plan
            .members
            .iter()
            .find(|member| member.worker_id == local.worker_id && **member != plan.damaged)
            .ok_or_else(unauthorized)?;
        let running = self
            .host
            .manager()
            .hosted_data(&plan.group, member.node_id)?;
        let replaced = running
            .replace_damaged_voter(
                &plan.members,
                &plan.next_members(),
                &plan.damaged,
                plan.replacement.clone(),
            )
            .await?;
        Ok(Answer::Replaced(replaced.proof))
    }

    fn parse(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<Request, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::GroupDamage
            || session.group() != &self.control_group
        {
            return Err(unauthorized());
        }
        crate::protocol::json_input(bytes, HEADER).map_err(|_| invalid())?;
        let request: Request = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if request.group_id.is_nil() || request.node_id == 0 {
            return Err(invalid());
        }
        Ok(request)
    }
}

impl Endpoint for DamageReplacementEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            if !self.host.authorized(&session.remote()) {
                return Err(unauthorized());
            }
            Ok(ReceivePlan {
                class: TrafficClass::Settlement,
                section_lengths: [0; 3],
                decode_budget_bytes: 0,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request = self.parse(&session, header.as_slice())?;
            if (0..3).any(|index| sections.section(index).is_some())
                || !self.host.authorized(&session.remote())
            {
                return Err(unauthorized());
            }
            let result = self.act(&request).await;
            let reply = self.host.manager().resources().encode_in(
                ByteClass::Progress,
                &Reply {
                    group_id: request.group_id,
                    node_id: request.node_id,
                    nonce: session.nonce(),
                    result,
                },
                HEADER,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [None, None, None],
            })
        })
    }
}

/// Counters for `/v1/runtime/stats`.
#[derive(Default)]
pub(crate) struct DamageSchedulerStats {
    pub(crate) begun: AtomicU64,
    pub(crate) replaced: AtomicU64,
    pub(crate) failures: AtomicU64,
}

/// Control's damage-replacement scheduler (see the module documentation).
#[doc(hidden)]
pub struct DamageScheduler {
    control: Arc<ControlHandle>,
    raw_control: ManagedControl,
    permit: Arc<LocalOperatorPermit>,
    local: Arc<DamageReplacementEndpoint>,
    interval: Duration,
    running: Mutex<BTreeSet<(Uuid, u64)>>,
    after: Mutex<Option<(Uuid, u64)>>,
    pub(crate) stats: DamageSchedulerStats,
    /// Test only: after hosting the fresh replica, signal the first `Notify`
    /// and wait for the second (a Control leader that stops mid-replacement).
    #[cfg(test)]
    pub(crate) hold_after_host: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}

impl DamageScheduler {
    #[doc(hidden)]
    pub fn new(
        control: Arc<ControlHandle>,
        raw_control: ManagedControl,
        permit: Arc<LocalOperatorPermit>,
        local: Arc<DamageReplacementEndpoint>,
        interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            control,
            raw_control,
            permit,
            local,
            interval,
            running: Mutex::new(BTreeSet::new()),
            after: Mutex::new(None),
            stats: DamageSchedulerStats::default(),
            #[cfg(test)]
            hold_after_host: Mutex::new(None),
        })
    }

    /// Runs until `stop` turns true or its sender drops.
    #[doc(hidden)]
    pub fn spawn(self: Arc<Self>, mut stop: watch::Receiver<bool>) -> JoinHandle<()> {
        nabedra_rt::task::spawn(async move {
            loop {
                tokio::select! {
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                    _ = nabedra_rt::time::sleep(self.interval) => {}
                }
                if let Err(error) = self.tick().await {
                    tracing::debug!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement", %error, "damage replacement scheduler pass failed");
                }
            }
        })
    }

    /// This process's Control replica leads Control.
    async fn leads(&self) -> bool {
        let local = self.raw_control.state.resident().node_id();
        self.raw_control.raft.current_leader().await == Some(local)
    }

    /// Visit one page per tick so a lost worker with many ranges cannot
    /// create an unbounded number of competing repair tasks.
    pub(crate) async fn tick(self: &Arc<Self>) -> Result<(), ProtocolError> {
        if !self.control.group_damage_enabled() || !self.leads().await {
            return Ok(());
        }
        let after = *self.after.lock().map_err(|_| unavailable())?;
        let (records, done) = self.control.group_damage_page(after, 64).await?;
        let next = records.last().map(|record| (record.report.group.group_id, record.report.replica.node_id));
        for record in records {
            if record.pending() && matches!(record.report.group.kind, GroupKind::Data { .. }) {
                self.start(record);
            }
        }
        *self.after.lock().map_err(|_| unavailable())? = if done { None } else { next };
        Ok(())
    }

    fn start(self: &Arc<Self>, record: GroupDamageRecord) {
        let key = (record.report.group.group_id, record.report.replica.node_id);
        if !self
            .running
            .lock()
            .is_ok_and(|mut running| {
                running.len() < crate::oltp_replication::authority::control::MAX_ACTIVE_DAMAGE_REPLACEMENTS
                    && running.insert(key)
            })
        {
            return;
        }
        let scheduler = self.clone();
        nabedra_rt::task::spawn(async move {
            let outcome = scheduler.settle(record).await;
            match &outcome {
                Ok(true) => {
                    scheduler.stats.replaced.fetch_add(1, Ordering::Relaxed);
                }
                Ok(false) => {}
                Err(error) => {
                    scheduler.stats.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement",
                        group = %key.0,
                        node = key.1,
                        %error,
                        "damaged replica not replaced yet; the scheduler retries"
                    );
                }
            }
            if let Ok(mut running) = scheduler.running.lock() {
                running.remove(&key);
            }
        });
    }

    /// Drives one record as far as it can. `Ok(true)` once replaced,
    /// `Ok(false)` when it has to wait (a rate limit, no placement yet).
    async fn settle(&self, record: GroupDamageRecord) -> Result<bool, ProtocolError> {
        let plan = match &record.phase {
            GroupDamagePhase::Replaced { .. } => return Ok(true),
            GroupDamagePhase::Replacing { plan, .. } => (**plan).clone(),
            GroupDamagePhase::Reported => match self.begin(&record).await? {
                Some(plan) => plan,
                None => return Ok(false),
            },
        };
        if !self.leads().await {
            return Ok(false);
        }
        self.call(
            &plan.replacement.worker_id,
            &plan.group,
            plan.damaged.node_id,
            Action::Host,
        )
        .await?;
        #[cfg(test)]
        if let Some((reached, release)) = self
            .hold_after_host
            .lock()
            .ok()
            .and_then(|hold| hold.clone())
        {
            reached.notify_one();
            release.notified().await;
        }
        let proof = self.replace(&plan).await?;
        let operation = derived_id(b"nabedra/group-damage-replaced/v1", &proof)?;
        let done = self
            .control
            .group_damage_replaced_local(&self.permit, operation, proof)
            .await?;
        if let GroupDamagePhase::Replaced { published, .. } = &done.phase {
            tracing::warn!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement",
                group = %plan.group.group_id,
                damaged = plan.damaged.node_id,
                replacement = plan.replacement.node_id,
                generation = ?published.as_ref().map(|record| record.generation),
                "damaged replica replaced and the new placement published"
            );
            return Ok(true);
        }
        Err(unavailable())
    }

    /// Chooses the fresh identity and placement and claims the group.
    async fn begin(
        &self,
        record: &GroupDamageRecord,
    ) -> Result<Option<GroupDamageReplacementPlan>, ProtocolError> {
        let group = &record.report.group;
        let damaged = &record.report.replica;
        let Some(expected) = self.control.group(group.group_id).await? else {
            return Ok(None);
        };
        if expected.identity != *group {
            return Ok(None);
        }
        let members = self.members(&expected).await?;
        if !members.contains(damaged) {
            tracing::warn!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement", group = %group.group_id, node = damaged.node_id,
                "damaged replica is no longer a published member; nothing to replace");
            return Ok(None);
        }
        let Some((worker, pool)) = self.placement(record, &members).await? else {
            tracing::warn!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement", group = %group.group_id, node = damaged.node_id,
                "no pool can host a fresh replica yet; the group stays quarantined");
            return Ok(None);
        };
        let mut node_id = members
            .iter()
            .map(|member| member.node_id)
            .max()
            .unwrap_or(0)
            .max(damaged.node_id);
        for _ in 0..16 {
            node_id = node_id.checked_add(1).ok_or_else(invalid)?;
            let plan = GroupDamageReplacementPlan {
                group: group.clone(),
                damaged: damaged.clone(),
                replacement: ReplicaMember {
                    node_id,
                    worker_id: worker,
                    pool_id: pool,
                },
                expected: expected.clone(),
                members: members.clone(),
            };
            let operation = derived_id(b"nabedra/group-damage-begin/v1", &plan)?;
            match self
                .control
                .begin_group_damage_replacement_local(&self.permit, operation, plan.clone())
                .await
            {
                Ok(claimed) => {
                    self.stats.begun.fetch_add(1, Ordering::Relaxed);
                    return Ok(claimed.plan().cloned());
                }
                // The node ID was used before: take the next one.
                Err(error)
                    if error.code == ErrorCode::Conflict
                        && error.outcome == Outcome::NotApplied =>
                {
                    let current = self
                        .control
                        .group_damage(group.group_id, damaged.node_id)
                        .await?;
                    if let Some(plan) = current.as_ref().and_then(GroupDamageRecord::plan) {
                        return Ok(Some(plan.clone()));
                    }
                    if current.as_ref().is_none_or(|record| !record.pending()) {
                        return Ok(None);
                    }
                }
                Err(error) if error.code == ErrorCode::Overloaded => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    async fn members(&self, expected: &GroupRecord) -> Result<Vec<ReplicaMember>, ProtocolError> {
        let mut members = Vec::new();
        let mut after = None;
        loop {
            let raw = self.control.group_members(expected.clone(), after).await?;
            let page: ReplicaInventoryPage =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            if page.group != *expected {
                return Err(unavailable());
            }
            members.extend(page.items.iter().cloned());
            if page.done || page.items.is_empty() {
                break;
            }
            after = page.after;
        }
        members.sort_by_key(|member| member.node_id);
        Ok(members)
    }

    /// The damaged worker's other pools first (placement allows a second
    /// replica there once the damaged one leaves), then every other enrolled
    /// worker that hosts no surviving member, in worker order.
    async fn placement(
        &self,
        record: &GroupDamageRecord,
        members: &[ReplicaMember],
    ) -> Result<Option<(Uuid, Uuid)>, ProtocolError> {
        let damaged = &record.report.replica;
        let survivors = members
            .iter()
            .filter(|member| *member != damaged)
            .map(|member| member.worker_id)
            .collect::<BTreeSet<_>>();
        let pools = members
            .iter()
            .map(|member| member.pool_id)
            .collect::<BTreeSet<_>>();
        let mut candidates = vec![damaged.worker_id];
        candidates.extend(
            self.workers()
                .await?
                .into_iter()
                .filter(|worker| *worker != damaged.worker_id),
        );
        for worker in candidates {
            if survivors.contains(&worker) {
                continue;
            }
            let offered = match self
                .call(
                    &worker,
                    &record.report.group,
                    damaged.node_id,
                    Action::Offer,
                )
                .await
            {
                Ok(Answer::Pools(offered)) => offered,
                Ok(_) => continue,
                Err(error) => {
                    tracing::debug!(target: "nabedra_database_server::oltp_replication::manager::data_factory::damage_replacement", %worker, %error, "worker offered no pool for a damaged group");
                    continue;
                }
            };
            if let Some(pool) = offered.into_iter().find(|pool| !pools.contains(pool)) {
                return Ok(Some((worker, pool)));
            }
        }
        Ok(None)
    }

    async fn workers(&self) -> Result<Vec<Uuid>, ProtocolError> {
        let revision = self
            .control
            .inspect()
            .await?
            .ok_or_else(unavailable)?
            .enrollment_revision;
        let mut workers = Vec::new();
        let mut after = None;
        loop {
            let raw = self.control.workers(revision, after).await?;
            let page: InventoryPage<WorkerEnrollment> =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            workers.extend(page.items.iter().map(|worker| worker.worker_id));
            if page.done || page.items.is_empty() {
                break;
            }
            after = page.after;
        }
        workers.sort();
        Ok(workers)
    }

    /// Asks every surviving member until the Data leader commits the change.
    async fn replace(
        &self,
        plan: &GroupDamageReplacementPlan,
    ) -> Result<GroupDamageReplacement, ProtocolError> {
        let deadline = nabedra_rt::time::now() + REPLACE_DEADLINE;
        let survivors = plan
            .members
            .iter()
            .filter(|member| **member != plan.damaged)
            .collect::<Vec<_>>();
        let mut last = unavailable();
        while nabedra_rt::time::now() < deadline {
            if !self.leads().await {
                return Err(unavailable());
            }
            for member in &survivors {
                match self
                    .call(
                        &member.worker_id,
                        &plan.group,
                        plan.damaged.node_id,
                        Action::Replace,
                    )
                    .await
                {
                    Ok(Answer::Replaced(proof))
                        if proof.group == plan.group
                            && proof.removed == plan.damaged
                            && proof.replacement == plan.replacement =>
                    {
                        return Ok(proof);
                    }
                    Ok(_) => last = invalid(),
                    Err(error) => last = error,
                }
            }
            nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        }
        Err(last)
    }

    /// Runs `action` on `worker`: directly when it is this worker.
    async fn call(
        &self,
        worker: &Uuid,
        group: &GroupIdentity,
        node_id: u64,
        action: Action,
    ) -> Result<Answer, ProtocolError> {
        let request = Request {
            group_id: group.group_id,
            node_id,
            action,
        };
        let manager = self.local.host.manager();
        let client = manager.client();
        let local = client.local_identity();
        if *worker == local.worker_id {
            return self.local.act(&request).await;
        }
        let enrollment = self
            .control
            .worker(*worker)
            .await?
            .ok_or_else(unavailable)?;
        let target = WorkerIdentity::from(&enrollment);
        let control = self.control.identity();
        let header = manager
            .resources()
            .encode_in(ByteClass::Progress, &request, HEADER)?;
        let reply = client
            .request_classified(
                control,
                target,
                Purpose::GroupDamage,
                header,
                [None, None, None],
                DECODE,
            )
            .await
            .map_err(ProtocolError::from)?;
        let decoded = reply.decode_header::<Reply>()?;
        if reply.group() != control
            || reply.purpose() != Purpose::GroupDamage
            || reply.remote_identity() != target
            || reply.sections.iter().any(Option::is_some)
            || decoded.group_id != request.group_id
            || decoded.node_id != request.node_id
            || decoded.nonce != reply.nonce()
        {
            return Err(invalid());
        }
        decoded.result.clone()
    }
}

/// The production [`DamageHost`]: the worker's Data factory and its pools.
#[doc(hidden)]
pub struct FactoryDamageHost {
    factory: Arc<DataGroupFactory>,
    pools: Arc<crate::oltp_deployment::inventory::WorkerPools>,
}
impl FactoryDamageHost {
    #[doc(hidden)]
    pub fn new(
        factory: Arc<DataGroupFactory>,
        pools: Arc<crate::oltp_deployment::inventory::WorkerPools>,
    ) -> Arc<Self> {
        Arc::new(Self { factory, pools })
    }
}
impl DamageHost for FactoryDamageHost {
    fn manager(&self) -> &Arc<HostedGroupManager> {
        &self.factory.manager
    }
    fn raft_config(&self) -> openraft::Config {
        self.factory.config.clone()
    }
    fn control(&self) -> &dyn DamageControl {
        &*self.factory.rpc
    }
    fn authorized(&self, peer: &WorkerIdentity) -> bool {
        self.factory.control.is_operator(peer)
    }
    fn placement_pools(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<OltpPool>, ProtocolError>> + Send + '_>> {
        Box::pin(async move {
            let mut eligible = Vec::new();
            for id in self.pools.pool_ids()? {
                if self.pools.refused(id).is_some()
                    || self.factory.control.pool_drain_blocks_placement(id).await?
                {
                    continue;
                }
                // Only a mounted pool with a completed Control claim, which a
                // restart rehosts from the published placement.
                if let Ok(pool) = self
                    .pools
                    .retained_for_group(id, &self.factory.control)
                    .await
                {
                    eligible.push(pool);
                }
            }
            Ok(eligible)
        })
    }
    fn wire(
        &self,
        group: &GroupIdentity,
        node: u64,
        running: ManagedData,
    ) -> Result<(), ProtocolError> {
        self.factory.wire_participant(group, node, running)
    }
}

fn derived_id(domain: &[u8], value: &impl Serialize) -> Result<Uuid, ProtocolError> {
    let digest = crate::oltp_replication::owned::serialized_digest(&(domain, value))?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(uuid::Builder::from_random_bytes(bytes).into_uuid())
}

fn not_found() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::NotFound,
        "no damage record for that replica",
        Outcome::NotApplied,
    )
}
fn conflict(message: &str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}
