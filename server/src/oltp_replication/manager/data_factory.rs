//! Authenticated, fixed-placement Data group creation. Hosting and activation
//! are separate: an incomplete set of startup acknowledgements cannot publish
//! a route, and activation requires the real group's durable quorum.
use super::*;
use crate::{
    oltp::{
        mvcc::store::{KeySpan, ParticipantHandle},
        pool::{ByteClass, ChargedBytes, ExecutionClass, OltpPool, ScratchLease},
    },
    oltp_replication::{
        authority::control::{
            ControlClient, ControlHandle, GroupDamageReason, GroupDamageReport, GroupRecord,
            InventoryPage,
        },
        multiplex::*,
    },
    oltp_runtime::{replica::QueueScope, worker_endpoint::AuthorityReader},
};
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use uuid::Uuid;
pub(crate) mod split;
pub(crate) mod placement;
mod restart;

const HEADER: usize = 4096;
const PAYLOAD: usize = 64 << 10;
const DECODE: usize = 1 << 20;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DataGroupSpec {
    pub group: GroupIdentity,
    pub authority: Authority,
    pub catalog_group_id: Uuid,
    pub transition_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub span: KeySpan,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Method {
    Host,
    Activate,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    to_node: u64,
    method: Method,
    payload_bytes: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHeader {
    node: u64,
    nonce: Uuid,
    bytes: u64,
    error: Option<ProtocolError>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    spec: DataGroupSpec,
    voters: Page<ReplicaMember>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Membership {
    group: GroupIdentity,
    replicas: Page<ReplicaMember>,
    applied_at: AppliedPosition,
}
pub(crate) struct DataGroupFactory {
    manager: Arc<HostedGroupManager>,
    pool: OltpPool,
    local: ReplicaMember,
    voters: Page<ReplicaMember>,
    catalog: GroupIdentity,
    control: Arc<ControlHandle>,
    rpc: Arc<ControlClient>,
    config: openraft::Config,
    status_topology: crate::oltp_replication::authority::status::StatusTopology,
    starts: tokio::sync::Mutex<()>,
    wired: Mutex<std::collections::BTreeSet<(Uuid, u64)>>,
    /// Replicas whose damage report is already being settled (NS14).
    damaged: Arc<Mutex<std::collections::BTreeSet<(Uuid, u64)>>>,
    _owner: Arc<ScratchLease>,
}
impl DataGroupFactory {
    pub(crate) fn new(
        manager: Arc<HostedGroupManager>,
        pool: OltpPool,
        local: ReplicaMember,
        voters: Page<ReplicaMember>,
        catalog: GroupIdentity,
        control: Arc<ControlHandle>,
        rpc: Arc<ControlClient>,
        status_topology: crate::oltp_replication::authority::status::StatusTopology,
        config: openraft::Config,
    ) -> Result<Arc<Self>, ProtocolError> {
        validate_voters(&voters).map_err(|_| invalid())?;
        if catalog.kind != GroupKind::Catalog
            || !voters.iter().any(|member| member == &local)
            || local.pool_id != pool.identity().pool_id
            || local.worker_id != pool.identity().worker_id
            || !Arc::ptr_eq(manager.resources(), pool.resources())
            || control.identity().cluster_id != catalog.cluster_id
            || control.identity().database != catalog.database
        {
            return Err(invalid());
        }
        ordered(&voters)?;
        let owner = Arc::new(manager.resources().reserve_scratch(
            ByteClass::Progress,
            std::mem::size_of::<Self>()
                + voters.capacity() * std::mem::size_of::<ReplicaMember>()
                + 4096,
        )?);
        let damaged = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
        manager.install_damage_sink(Arc::new(FactoryDamageSink {
            control: Arc::downgrade(&rpc),
            damaged: damaged.clone(),
        }));
        Ok(Arc::new(Self {
            manager,
            pool,
            local,
            voters,
            catalog,
            control,
            rpc,
            config,
            status_topology,
            starts: tokio::sync::Mutex::new(()),
            wired: Mutex::new(Default::default()),
            damaged,
            _owner: owner,
        }))
    }
    /// NS14 rules 2 and 4: report a replica this worker cannot serve and
    /// release its bytes once Control records its replacement.
    fn settle_damage(&self, report: GroupDamageReport, pool: Option<OltpPool>) {
        spawn_settlement(self.rpc.clone(), self.damaged.clone(), report, pool);
    }
    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        crate::protocol::json_input(bytes, HEADER).map_err(|_| invalid())?;
        let head: Header = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        if session.recovery_only()
            || session.purpose() != Purpose::GroupLifecycle
            || session.group() != self.control.identity()
            || head.to_node != self.local.node_id
            || head.payload_bytes == 0
            || head.payload_bytes > PAYLOAD as u64
        {
            return Err(invalid());
        }
        Ok(head)
    }
    async fn authorize(&self, session: &AuthenticatedSession) -> Result<Authority, ProtocolError> {
        let current = self
            .control
            .current(&QueueScope {
                cluster_id: self.catalog.cluster_id,
                database: self.catalog.database.clone(),
            })
            .await?;
        if current.owner != session.remote() {
            return Err(unauthorized());
        }
        Ok(current.ready.view.authority())
    }
    fn validate(&self, request: &Request) -> Result<(), ProtocolError> {
        let spec = &request.spec;
        spec.group.validate().map_err(|_| invalid())?;
        spec.authority.validate().map_err(|_| invalid())?;
        if !matches!(spec.group.kind, GroupKind::Data { .. })
            || spec.group.cluster_id != self.catalog.cluster_id
            || spec.group.database != self.catalog.database
            || spec.catalog_group_id != self.catalog.group_id
            || spec.transition_id.is_nil()
            || spec.schema_epoch == 0
            || spec.routing_epoch == 0
            || request.voters != self.voters
        {
            return Err(invalid());
        }
        // The fixed/manual topology starts full-range shards. Split/move uses
        // its separately fenced lineage transition, never this startup RPC.
        if spec.span.start.is_some()
            || spec.span.end.is_some()
            || spec.schema_epoch != 1
            || spec.routing_epoch != 1
        {
            return Err(invalid());
        }
        Ok(())
    }
    async fn host(&self, spec: &DataGroupSpec) -> Result<(), ProtocolError> {
        self.host_group(&spec.group, true).await
    }
    pub(crate) async fn start_retained(&self, strict: bool) -> Result<(), ProtocolError> {
        let revision = self
            .control
            .inspect()
            .await?
            .ok_or_else(unavailable)?
            .group_inventory_revision;
        let mut after = None;
        loop {
            let raw = self.control.groups(revision, after).await?;
            let bytes = crate::oltp::mvcc::json_decode_budget(raw.as_slice())?;
            let _owner = self
                .manager
                .resources()
                .reserve_scratch(ByteClass::Progress, bytes)?;
            let page: InventoryPage<GroupRecord> =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            if page.revision != revision || (!page.done && page.after <= after) {
                return Err(invalid());
            }
            for record in page
                .items
                .iter()
                .filter(|record| matches!(record.identity.kind, GroupKind::Data { .. }))
            {
                let routes = self.rpc.replica_routes(&record.identity).await?;
                if routes.record != *record {
                    return Err(unavailable());
                }
                if routes.members.as_slice() == self.voters.as_ref() {
                    // Section 3 rules 1 and 2: a quarantined group is reported,
                    // not started; the worker keeps hosting the others.
                    if let Some(damage) = self.pool.group_damage(record.identity.group_id) {
                        self.settle_damage(
                            GroupDamageReport {
                                group: record.identity.clone(),
                                replica: self.local.clone(),
                                reason: damage.cause.into(),
                                first_bad_seq: damage.first_bad_seq,
                            },
                            Some(self.pool.clone()),
                        );
                        continue;
                    }
                    self.host_group(&record.identity, false).await?;
                } else if strict {
                    return Err(ProtocolError::new(
                        ErrorCode::Conflict,
                        "registered Data placement differs from the fixed cold-start placement",
                        Outcome::NotApplied,
                    ));
                }
            }
            if page.done {
                return Ok(());
            }
            after = page.after;
        }
    }
    pub(crate) async fn host_group(
        &self,
        group: &GroupIdentity,
        initialize: bool,
    ) -> Result<(), ProtocolError> {
        let _start = self.starts.lock().await;
        let running = start_data_group(
            &self.manager, &self.config, &self.pool, group, &self.local, &self.voters, initialize,
        ).await?;
        self.wire_participant(group, self.local.node_id, running)
    }
    fn wire_participant(
        &self,
        group: &GroupIdentity,
        node_id: u64,
        running: ManagedData,
    ) -> Result<(), ProtocolError> {
        let mut wired = self.wired.lock().map_err(|_| unavailable())?;
        if !wired.contains(&(group.group_id, node_id)) {
            wire_participant(
                &self.manager,
                &self.rpc,
                &self.status_topology,
                group,
                node_id,
                running,
            )?;
            wired.insert((group.group_id, node_id));
        }
        Ok(())
    }
    async fn activate(&self, spec: &DataGroupSpec) -> Result<Membership, ProtocolError> {
        let running = self.manager.hosted_data(&spec.group, self.local.node_id)?;
        // Only the leader can produce this observation after real ReadIndex.
        let started = tokio::time::Instant::now();
        let observed = running.observe_membership().await;
        tracing::debug!(group_id = %spec.group.group_id, node = self.local.node_id, stage = "membership_read", ms = started.elapsed().as_millis(), ok = observed.is_ok(), "initial Data activation");
        let observation = observed?;
        if observation.replicas() != self.voters.as_ref() {
            return Err(invalid());
        }
        let participant = ParticipantHandle::new(
            ParticipantMachine::new(running.state.store().clone())?,
            running,
        );
        let started = tokio::time::Instant::now();
        let activation = participant.activation().await;
        tracing::debug!(group_id = %spec.group.group_id, node = self.local.node_id, stage = "fence_read", ms = started.elapsed().as_millis(), ok = activation.is_ok(), "initial Data activation");
        if let Some(existing) = activation? {
            if existing.authority != spec.authority
                || existing.catalog_group_id != spec.catalog_group_id
                || existing.schema_epoch != spec.schema_epoch
                || existing.routing_epoch != spec.routing_epoch
            {
                return Err(invalid());
            }
        } else {
            let started = tokio::time::Instant::now();
            let fenced = participant
                .fence_initial(
                    spec.transition_id,
                    spec.authority,
                    spec.catalog_group_id,
                    spec.schema_epoch,
                    spec.routing_epoch,
                    spec.span.clone(),
                )
                .await;
            tracing::debug!(group_id = %spec.group.group_id, node = self.local.node_id, stage = "fence_write", ms = started.elapsed().as_millis(), ok = fenced.is_ok(), "initial Data activation");
            fenced?;
        }
        Ok(Membership {
            group: observation.group,
            replicas: observation.replicas,
            applied_at: observation.applied_at,
        })
    }
}

async fn start_data_group(
    manager: &HostedGroupManager,
    config: &openraft::Config,
    pool: &OltpPool,
    group: &GroupIdentity,
    local: &ReplicaMember,
    voters: &[ReplicaMember],
    initialize: bool,
) -> Result<ManagedData, ProtocolError> {
    if pool.identity().pool_id != local.pool_id
        || pool.identity().worker_id != local.worker_id
        || !voters.contains(local)
    {
        return Err(invalid());
    }
    let store = if initialize {
        pool.group(group.clone()).await?
    } else {
        pool.retained_group(group.clone()).await?
    };
    let members: BTreeMap<_, _> = voters.iter().map(|member| (member.node_id, member.clone())).collect();
    let running = manager.start_data(store, local.clone(), members.clone(), config.clone()).await?;
    if initialize
        && voters.first() == Some(local)
        && !running.raft.is_initialized().await.map_err(|_| unavailable())?
        && running.raft.initialize(members).await.is_err()
        && !running.raft.is_initialized().await.map_err(|_| unavailable())?
    {
        return Err(unavailable());
    }
    Ok(running)
}

fn wire_participant(
    manager: &HostedGroupManager,
    control: &Arc<ControlClient>,
    topology: &crate::oltp_replication::authority::status::StatusTopology,
    group: &GroupIdentity,
    node_id: u64,
    running: ManagedData,
) -> Result<(), ProtocolError> {
    let participant = Arc::new(ParticipantHandle::new(
        ParticipantMachine::new(running.state.store().clone())?,
        running,
    ));
    manager.bind_service(
        group,
        node_id,
        crate::oltp_replication::service_rpc::ParticipantEndpoint::new(
            participant,
            node_id,
            control.clone(),
            manager.client(),
            Some(topology.clone()),
        )?,
    )
}
impl Endpoint for DataGroupFactory {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let header = self.parse(session, bytes)?;
            self.authorize(session).await?;
            Ok(ReceivePlan {
                class: TrafficClass::Settlement,
                section_lengths: [header.payload_bytes, 0, 0],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let header = self.parse(&session, bytes.as_slice())?;
            let result = async {
                let current = self.authorize(&session).await?;
                let input = sections.section(0).ok_or_else(invalid)?.clone();
                let resources = self.manager.resources().clone();
                let (request, owner): (Request, _) = self
                    .manager
                    .resources()
                    .execute(ExecutionClass::Control, self.local.node_id, 0, move || {
                        let needed = crate::oltp::mvcc::json_decode_budget(input.as_slice())?;
                        if needed > DECODE {
                            return Err(invalid());
                        }
                        let owner =
                            Arc::new(resources.reserve_scratch(ByteClass::Progress, needed)?);
                        let request =
                            serde_json::from_slice(input.as_slice()).map_err(|_| invalid())?;
                        Ok((request, owner))
                    })
                    .await?;
                self.validate(&request)?;
                if request.spec.authority != current {
                    return Err(unauthorized());
                }
                let output = match header.method {
                    Method::Host => {
                        self.host(&request.spec).await?;
                        self.manager.resources().encode_in(
                            ByteClass::Progress,
                            &(&request.spec, &self.local),
                            PAYLOAD,
                        )?
                    }
                    Method::Activate => {
                        let membership = self.activate(&request.spec).await?;
                        self.manager.resources().encode_in(
                            ByteClass::Progress,
                            &membership,
                            PAYLOAD,
                        )?
                    }
                };
                drop(owner);
                Ok(output)
            }
            .await;
            let (payload, error) = match result {
                Ok(bytes) => (Some(bytes), None),
                Err(error) => (None, Some(error)),
            };
            let reply = self.manager.resources().encode_in(
                ByteClass::Progress,
                &ReplyHeader {
                    node: self.local.node_id,
                    nonce: session.nonce(),
                    bytes: payload.as_ref().map_or(0, |bytes| bytes.len() as u64),
                    error,
                },
                HEADER,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [payload, None, None],
            })
        })
    }
}
pub(crate) struct DataGroupClient {
    manager: Arc<HostedGroupManager>,
    control: GroupIdentity,
    voters: Page<ReplicaMember>,
    _owner: Arc<ScratchLease>,
}
impl DataGroupClient {
    pub(crate) fn new(
        manager: Arc<HostedGroupManager>,
        control: GroupIdentity,
        voters: Page<ReplicaMember>,
    ) -> Result<Arc<Self>, ProtocolError> {
        control.validate().map_err(|_| invalid())?;
        validate_voters(&voters).map_err(|_| invalid())?;
        ordered(&voters)?;
        if control.kind != GroupKind::Control {
            return Err(invalid());
        }
        let owner = Arc::new(manager.resources().reserve_scratch(
            ByteClass::Progress,
            std::mem::size_of::<Self>()
                + voters.capacity() * std::mem::size_of::<ReplicaMember>()
                + 4096,
        )?);
        Ok(Arc::new(Self {
            manager,
            control,
            voters,
            _owner: owner,
        }))
    }
    async fn call<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        member: &ReplicaMember,
        method: Method,
        spec: DataGroupSpec,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let request = Request {
            spec,
            voters: self.voters.clone(),
        };
        let shared = self.manager.resources().clone();
        let encode = shared.clone();
        let payload = shared
            .execute(ExecutionClass::Control, member.node_id, 0, move || {
                encode.encode_in(ByteClass::Progress, &request, PAYLOAD)
            })
            .await?;
        let header = shared.encode_in(
            ByteClass::Progress,
            &Header {
                to_node: member.node_id,
                method,
                payload_bytes: payload.len() as u64,
            },
            HEADER,
        )?;
        let reply = self
            .manager
            .client()
            .request(
                &self.control,
                member.worker_id,
                Purpose::GroupLifecycle,
                header,
                [Some(payload), None, None],
                64 << 10,
            )
            .await?;
        if reply.remote_identity().worker_id != member.worker_id
            || reply.group() != &self.control
            || reply.purpose() != Purpose::GroupLifecycle
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid());
        }
        let header = reply.decode_header::<ReplyHeader>()?;
        if header.nonce != reply.nonce() || header.node != member.node_id {
            return Err(invalid());
        }
        if let Some(error) = &header.error {
            return Err(error.clone());
        }
        let input = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        if input.len() as u64 != header.bytes {
            return Err(invalid());
        }
        let required = {
            let input = input.clone();
            shared
                .execute(ExecutionClass::Control, member.node_id, 0, move || {
                    crate::oltp::mvcc::json_decode_budget(input.as_slice())
                })
                .await?
        };
        let reply = reply.reserve_reply_decode(&shared, required).await?;
        shared
            .execute(ExecutionClass::Control, member.node_id, 0, move || {
                let value = serde_json::from_slice(input.as_slice()).map_err(|_| invalid())?;
                Ok((value, reply.scratch.clone()))
            })
            .await
    }
    pub(crate) async fn host_all(
        &self,
        spec: DataGroupSpec,
        control: &ControlHandle,
        ready: &[WorkerIdentity],
    ) -> Result<(), ProtocolError> {
        for member in self.voters.iter() {
            if !ready.iter().any(|worker| worker.worker_id == member.worker_id)
                || control.pool_drain_blocks_placement(member.pool_id).await?
            {
                tracing::debug!(group_id = %spec.group.group_id, node = member.node_id, "initial Data voter or pool is ineligible");
                return Err(initial_placement_unavailable());
            }
        }
        let host = |member: &ReplicaMember| {
            let spec = spec.clone();
            let member = member.clone();
            async move {
                let started = tokio::time::Instant::now();
                let verified = self
                    .call::<(DataGroupSpec, ReplicaMember)>(&member, Method::Host, spec.clone())
                    .await
                    .and_then(|((observed, hosted), _)| {
                        if observed == spec && hosted == member {
                            Ok(())
                        } else {
                            Err(invalid())
                        }
                    });
                tracing::debug!(group_id = %spec.group.group_id, node = member.node_id, ms = started.elapsed().as_millis(), ok = verified.is_ok(), error = ?verified.as_ref().err(), "initial Data host RPC");
                verified.map_err(|error| {
                    // Host can leave a private replica after a lost reply. This
                    // stage has no authority to publish a Catalog route.
                    if error.outcome == Outcome::Unknown
                        || matches!(
                            error.code,
                            ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown
                        )
                    {
                        initial_placement_unavailable()
                    } else {
                        error
                    }
                })
            }
        };
        // The seed initializes after the follower acknowledgements. Limit
        // parallel starts independently of the number of configured voters.
        futures_util::stream::iter(self.voters.iter().skip(1).map(&host))
            .buffer_unordered(2)
            .try_for_each(|()| async { Ok(()) })
            .await?;
        host(self.voters.first().ok_or_else(invalid)?).await
    }
    pub(crate) async fn activate(
        &self,
        spec: DataGroupSpec,
    ) -> Result<GroupMembershipObservation, ProtocolError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut last = unavailable();
        loop {
            for member in self.voters.iter() {
                let started = tokio::time::Instant::now();
                let completed = tokio::time::timeout_at(
                    deadline,
                    self.call(member, Method::Activate, spec.clone()),
                )
                .await;
                tracing::debug!(group_id = %spec.group.group_id, node = member.node_id, ms = started.elapsed().as_millis(), ok = completed.as_ref().is_ok_and(Result::is_ok), "initial Data activation RPC");
                let observed: Result<(Membership, _), _> = match completed {
                    Ok(result) => result,
                    Err(_) => return Err(last),
                };
                let (observed, owner) = match observed {
                    Ok(value) => value,
                    Err(error) => {
                        if !matches!(
                            error.code,
                            ErrorCode::StorageUnavailable
                                | ErrorCode::OutcomeUnknown
                                | ErrorCode::Overloaded
                        ) {
                            return Err(error);
                        }
                        last = error;
                        continue;
                    }
                };
                if observed.group != spec.group
                    || observed.replicas != self.voters
                    || !observed.replicas.iter().any(|replica| replica == member)
                    || observed.applied_at.index == 0
                {
                    return Err(invalid());
                }
                // This constructor runs only after matching the actual mTLS peer,
                // node, purpose, nonce, exact specification and configured voters.
                return Ok(GroupMembershipObservation {
                    group: observed.group,
                    replicas: observed.replicas,
                    applied_at: observed.applied_at,
                    _owner: owner,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(last);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}
fn initial_placement_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "initial Data placement capacity unavailable",
        Outcome::NotApplied,
    )
}
fn ordered(voters: &[ReplicaMember]) -> Result<(), ProtocolError> {
    if voters
        .windows(2)
        .any(|pair| pair[0].node_id >= pair[1].node_id)
    {
        return Err(invalid());
    }
    Ok(())
}
fn unauthorized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "Data group creation requires the current fenced scheduler owner",
        Outcome::NotApplied,
    )
}

/// Reports replicas stopped by run-time damage (NS14 rule 2).
struct FactoryDamageSink {
    control: std::sync::Weak<ControlClient>,
    damaged: Arc<Mutex<std::collections::BTreeSet<(Uuid, u64)>>>,
}
impl super::DamageSink for FactoryDamageSink {
    fn damaged(&self, group: GroupIdentity, replica: ReplicaMember, pool: OltpPool) {
        let Some(control) = self.control.upgrade() else {
            return;
        };
        let Some(damage) = pool.group_damage(group.group_id) else {
            return;
        };
        let report = GroupDamageReport {
            group,
            replica,
            reason: damage.cause.into(),
            first_bad_seq: damage.first_bad_seq,
        };
        spawn_settlement(control, self.damaged.clone(), report, Some(pool));
    }
}

fn spawn_settlement(
    control: Arc<ControlClient>,
    damaged: Arc<Mutex<std::collections::BTreeSet<(Uuid, u64)>>>,
    report: GroupDamageReport,
    pool: Option<OltpPool>,
) {
    let key = (report.group.group_id, report.replica.node_id);
    if !damaged.lock().is_ok_and(|mut damaged| damaged.insert(key)) {
        return;
    }
    tokio::spawn(async move {
        let outcome = super::settle_damage(
            control,
            report,
            pool,
            std::time::Duration::from_secs(1),
        )
        .await;
        tracing::warn!(group = %key.0, node = key.1, ?outcome, "group damage settled");
        if let Ok(mut damaged) = damaged.lock() {
            damaged.remove(&key);
        }
    });
}
