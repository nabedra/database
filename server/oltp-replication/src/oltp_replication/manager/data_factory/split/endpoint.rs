use super::*;
use crate::oltp::mvcc::catalog::{CatalogHandle, CatalogRead};
use crate::oltp_replication::{
    authority::status::{IdentityLookup, StatusTopology},
    service_rpc::StatusClient,
};

use super::automatic::{
    CapacityRequest, LocalUsageScan, UsageRequest, check_disk_headroom, check_headroom,
};
use crate::{
    oltp::pool::mount::MountedPoolObservation, oltp_deployment::inventory::WorkerPools,
    oltp_replication::manager::RemotePlacementTarget,
};

enum Host {
    System {
        factory: Arc<DataGroupFactory>,
        pools: Arc<WorkerPools>,
        timestamps: Option<Arc<TimestampHandle>>,
        catalog: Option<CatalogHandle>,
    },
    Data(Arc<RemotePlacementTarget>),
}

#[doc(hidden)]
pub struct SplitEndpoint {
    host: Host,
    client: SplitClient,
    catalog: GroupIdentity,
    status: StatusTopology,
    usage: tokio::sync::Mutex<Option<LocalUsageScan>>,
}
impl SplitEndpoint {
    #[doc(hidden)]
    pub fn new(
        factory: Arc<DataGroupFactory>,
        pools: Arc<WorkerPools>,
        client: SplitClient,
        timestamps: Option<Arc<TimestampHandle>>,
        catalog: Option<Arc<ParticipantHandle>>,
        status: StatusTopology,
    ) -> Result<Arc<Self>, ProtocolError> {
        Ok(Arc::new(Self {
            catalog: client.catalog.clone(),
            host: Host::System {
                factory,
                pools,
                timestamps,
                catalog: catalog.map(CatalogHandle::new).transpose()?,
            },
            client,
            status,
            usage: Default::default(),
        }))
    }

    #[doc(hidden)]
    pub fn for_data_worker(
        host: Arc<RemotePlacementTarget>,
        manager: Arc<HostedGroupManager>,
        control_group: GroupIdentity,
        bootstrap_voters: Page<ReplicaMember>,
        control: Arc<ControlClient>,
        timestamps: GroupIdentity,
        catalog: GroupIdentity,
        status: StatusTopology,
    ) -> Result<Arc<Self>, ProtocolError> {
        let data = DataGroupClient::new(manager, control_group, bootstrap_voters)?;
        Ok(Arc::new(Self {
            host: Host::Data(host),
            client: SplitClient::new(data, control, timestamps, catalog.clone()),
            catalog,
            status,
            usage: Default::default(),
        }))
    }

    fn manager(&self) -> &HostedGroupManager {
        &self.client.data.manager
    }

    fn scope(&self) -> QueueScope {
        QueueScope {
            cluster_id: self.catalog.cluster_id,
            database: self.catalog.database.clone(),
        }
    }

    fn fixed_peer(&self, session: &AuthenticatedSession) -> bool {
        if let Host::System { factory, .. } = &self.host {
            return factory.control.is_operator(&session.remote());
        }
        self.client
            .data
            .voters
            .iter()
            .any(|member| member.worker_id == session.remote().worker_id)
    }

    async fn authorize(&self, session: &AuthenticatedSession) -> Result<(), ProtocolError> {
        if let Host::System { factory, .. } = &self.host {
            factory.authorize(session).await?;
        } else if self.client.control.current(&self.scope()).await?.owner != session.remote() {
            return Err(unauthorized());
        }
        Ok(())
    }

    async fn authorize_proof(
        &self,
        session: &AuthenticatedSession,
        source: Uuid,
    ) -> Result<(), ProtocolError> {
        if !self.fixed_peer(session) {
            self.client
                .control
                .service_role(
                    &self.scope(),
                    session.remote(),
                    ServiceAction::ParticipantProof {
                        source_group: source,
                    },
                    self.catalog.clone(),
                )
                .await?;
        }
        Ok(())
    }

    async fn selected_pool(
        &self,
        voter: &SplitVoter,
        expected: Option<u64>,
    ) -> Result<(OltpPool, MountedPoolObservation), ProtocolError> {
        if self.manager().client().local_identity() != voter.worker {
            return Err(unauthorized());
        }
        let (pool, mounted) = match &self.host {
            Host::System { factory, pools, .. } => {
                if voter.registered_pool_generation.is_some() {
                    return Err(unauthorized());
                }
                let pool = pools.get(voter.member.pool_id)?.ok_or_else(unavailable)?;
                let mounted = self
                    .client
                    .shared()
                    .execute(ExecutionClass::Control, 0, 0, move || {
                        pool.observe_registered_mount()
                    })
                    .await?;
                let pool = pools
                    .eligible(
                        voter.member.pool_id,
                        voter.worker,
                        mounted.expected.generation,
                        &factory.control,
                    )
                    .await?;
                (pool, mounted)
            }
            Host::Data(target) => target.split_pool(voter).await?,
        };
        if expected.is_some_and(|expected| mounted.expected.generation != expected) {
            return Err(conflict("automatic split pool generation changed"));
        }
        Ok((pool, mounted))
    }

    async fn host_child(
        &self,
        pool: &OltpPool,
        child: &GroupIdentity,
        voter: &SplitVoter,
        members: &[ReplicaMember],
    ) -> Result<(), ProtocolError> {
        match &self.host {
            Host::System { factory, .. } => {
                let _start = factory.starts.lock().await;
                let local = &voter.member;
                let running = start_data_group(
                    &factory.manager,
                    &factory.config,
                    pool,
                    child,
                    local,
                    members,
                    true,
                )
                .await?;
                factory.wire_participant(child, local.node_id, running)
            }
            Host::Data(target) => target.host_split(child, voter, members).await,
        }
    }

    fn parse(
        &self,
        session: &AuthenticatedSession,
        raw: &[u8],
    ) -> Result<SplitHeader, ProtocolError> {
        crate::protocol::json_input(raw, HEADER).map_err(|_| invalid())?;
        let header: SplitHeader = serde_json::from_slice(raw).map_err(|_| invalid())?;
        let system = matches!(
            header.operation,
            Operation::Timestamp
                | Operation::Job
                | Operation::Publication
                | Operation::FailureSettlement
        );
        let system_destination = match &self.host {
            Host::System { factory, .. } => header.worker_node == factory.local.node_id,
            Host::Data(_) => false,
        };
        if session.recovery_only()
            || session.group() != &self.client.data.control
            || session.purpose() != header.operation.purpose()
            || header.worker_node == 0
            || (system && !system_destination)
            || header.payload_bytes == 0
            || header.payload_bytes > header.operation.input_limit() as u64
        {
            return Err(invalid());
        }
        Ok(header)
    }
    async fn current(&self) -> Result<Authority, ProtocolError> {
        Ok(self
            .client
            .control
            .current(&self.scope())
            .await?
            .ready
            .view
            .authority())
    }
    async fn job(&self, id: Uuid) -> Result<OwnedSplitAttempt, ProtocolError> {
        self.client.job(id).await?.ok_or_else(invalid)
    }
    async fn verify_guard(
        &self,
        attempt: &SplitAttempt,
        guard: &crate::oltp::mvcc::store::lifecycle::SealedLifecycleGuard,
        participant: &GroupIdentity,
    ) -> Result<(), ProtocolError> {
        if !attempt.exact_guard(guard) || self.current().await? != attempt.authority {
            return Err(invalid());
        }
        let proof = ParticipantProofClient::new(
            self.catalog.clone(),
            self.client.shared().clone(),
            self.manager().client(),
            self.client.control.clone(),
            ServiceAction::ParticipantProof {
                source_group: participant.group_id,
            },
        )?
        .lifecycle(guard.binding())
        .await?
        .ok_or_else(|| conflict("split Catalog guard is no longer active"))?;
        if proof.into_sealed() != *guard {
            return Err(invalid());
        }
        Ok(())
    }
    async fn participant(
        &self,
        group: &GroupIdentity,
        node: u64,
    ) -> Result<ParticipantHandle, ProtocolError> {
        let routes = self.client.control.replica_routes(group).await?;
        let member = routes
            .members
            .iter()
            .find(|member| {
                member.node_id == node
                    && member.worker_id == self.manager().client().local_identity().worker_id
            })
            .ok_or_else(unavailable)?;
        let running = self.manager().hosted_data(group, node)?;
        if running.state.store().pool().identity().pool_id != member.pool_id {
            return Err(unavailable());
        }
        Ok(ParticipantHandle::new(
            ParticipantMachine::new(running.state.store().clone())?,
            running,
        ))
    }
    fn validate_source(
        &self,
        attempt: &SplitAttempt,
        request: &SourceRequest,
    ) -> Result<(), ProtocolError> {
        if attempt.intent.source != request.source || request.split_id != attempt.intent.split_id {
            return Err(invalid());
        }
        Ok(())
    }
    fn binding<'a>(
        &self,
        attempt: &'a SplitAttempt,
        request: &TargetRequest,
    ) -> Result<&'a SplitBinding, ProtocolError> {
        attempt
            .children()?
            .iter()
            .find(|child| child.target == request.target && child.split_id == request.split_id)
            .ok_or_else(invalid)
    }
    async fn open_source(
        &self,
        attempt: &SplitAttempt,
        request: &SourceRequest,
        node: u64,
    ) -> Result<ParticipantHandle, ProtocolError> {
        self.validate_source(attempt, request)?;
        self.participant(&request.source, node).await
    }
    async fn verify_publication(&self, request: Publication) -> Result<(), ProtocolError> {
        let attempt = self.job(request.source.split_id).await?;
        self.validate_source(&attempt, &request.source)?;
        let settlement = self
            .settlement(
                &attempt,
                request.manifest,
                ServiceAction::ParticipantProof {
                    source_group: attempt.catalog_group_id,
                },
            )
            .await?;
        if settlement.statement().decision.decision
            != (TxnDecision::Commit {
                commit_ts: request.at,
            })
        {
            return Err(invalid());
        }
        let children = attempt.children()?;
        let table_id = children[0].guard.binding().table_id;
        let Host::System {
            catalog: Some(catalog),
            ..
        } = &self.host
        else {
            return Err(unauthorized());
        };
        let pin = catalog.snapshot(request.at, self.current().await?)?;
        let CatalogRead::Value {
            version: Some(closed_at),
            record: Some(closed),
        } = catalog
            .read::<crate::oltp::mvcc::store::split_closure::CatalogSplitClosed>(
                &pin,
                crate::oltp::mvcc::MvccKey::CatalogSplitClosed {
                    table_id,
                    split_id: attempt.intent.split_id,
                },
            )
            .await?
        else {
            return Err(unavailable());
        };
        closed.validate(table_id, attempt.intent.split_id)?;
        if closed_at != request.at
            || closed.restored.is_some()
            || closed.source != attempt.intent.source
            || closed.source_pin != attempt.image.as_ref().ok_or_else(invalid)?.pin
            || closed.guard != attempt.image.as_ref().ok_or_else(invalid)?.guard
        {
            return Err(invalid());
        }
        let CatalogRead::Value {
            version: Some(version),
            record: Some(root),
        } = catalog.routing_root(&pin, table_id).await?
        else {
            return Err(unavailable());
        };
        if version != request.at || root.routing_epoch != closed.published_routing_epoch {
            return Err(invalid());
        }
        let mut cursor = root.first_page;
        let mut next_start = None;
        let mut first = true;
        let mut next_child = 0;
        while let Some(page_id) = cursor {
            let CatalogRead::Value {
                version: Some(version),
                record: Some(page),
            } = catalog.routing_page(&pin, table_id, page_id).await?
            else {
                return Err(unavailable());
            };
            if page.shards.is_empty() {
                return Err(invalid());
            }
            for route in page.shards.iter() {
                if route.group == attempt.intent.source
                    || route.span.start != next_start
                    || (!first && next_start.is_none())
                {
                    return Err(invalid());
                }
                if let Some(index) = children
                    .iter()
                    .position(|child| child.target == route.group)
                {
                    let child = &children[index];
                    if index != next_child
                        || version != request.at
                        || route.span != child.target_span
                        || route.schema_epoch != child.source_schema_epoch
                        || Some(route.routing_epoch) != child.source_routing_epoch.checked_add(1)
                    {
                        return Err(invalid());
                    }
                    next_child += 1;
                } else if next_child == 1 {
                    return Err(invalid());
                }
                first = false;
                next_start = route.span.end.clone();
            }
            cursor = page.next_page;
        }
        if next_child != children.len() || next_start.is_some() {
            return Err(invalid());
        }
        Ok(())
    }
    async fn live_source(
        &self,
        attempt: &SplitAttempt,
        group: &GroupIdentity,
    ) -> Result<VerifiedSplitSource, ProtocolError> {
        let source = self.client.source(attempt).await?;
        if source.published_at().is_some()
            || attempt.closed.is_some()
            || self.current().await? != attempt.authority
        {
            return Err(invalid());
        }
        let cut = attempt.image.as_ref().ok_or_else(invalid)?;
        self.verify_guard(attempt, &cut.guard, group).await?;
        Ok(source)
    }
    async fn settlement(
        &self,
        attempt: &SplitAttempt,
        manifest: ManifestRef,
        action: ServiceAction,
    ) -> Result<SealedSettlement, ProtocolError> {
        let group = self.status.manifest(attempt.txn, manifest)?;
        let status = StatusProofClient::new(
            group.clone(),
            self.client.shared().clone(),
            self.manager().client(),
            self.client.control.clone(),
            action,
        )?;
        let proof = status
            .settlement(attempt.txn, manifest)
            .await?
            .into_sealed();
        proof.validate_for(attempt.txn, &attempt.request, &manifest)?;
        if !status
            .participates(&self.catalog, attempt.txn, manifest)
            .await?
        {
            return Err(invalid());
        }
        Ok(proof)
    }
    async fn failure_settlement(
        &self,
        attempt: &SplitAttempt,
    ) -> Result<SealedSettlement, ProtocolError> {
        let proof = StatusClient::new(
            self.status.request(attempt.txn.request_id).clone(),
            self.client.shared().clone(),
            self.manager().client(),
            self.client.control.clone(),
            ServiceAction::TimestampProof {
                source_group: self.client.timestamps.group_id,
            },
        )?
        .failure_settled_proof(attempt.txn, attempt.request)
        .await?
        .into_sealed();
        Ok(proof)
    }
    async fn apply_timestamp(
        &self,
        command: SplitTimestampCommand,
    ) -> Result<TimestampReply, ProtocolError> {
        if command.authority() != self.current().await? {
            return Err(unauthorized());
        }
        let Host::System {
            timestamps: Some(timestamps),
            ..
        } = &self.host
        else {
            return Err(unauthorized());
        };
        let authority = ServiceAction::TimestampProof {
            source_group: self.client.timestamps.group_id,
        };
        match &command {
            SplitTimestampCommand::Open {
                intent,
                catalog_group_id,
                reservation,
                ..
            } => {
                if *catalog_group_id != self.catalog.group_id {
                    return Err(invalid());
                }
                let status = StatusClient::new(
                    self.status.request(intent.split_id).clone(),
                    self.client.shared().clone(),
                    self.manager().client(),
                    self.client.control.clone(),
                    authority,
                )?;
                let lookup = status
                    .lookup_identity(intent.split_id, Some(intent.digest()?))
                    .await?;
                if !matches!(lookup, Some(IdentityLookup::Reserved(actual)) if actual == *reservation)
                {
                    return Err(invalid());
                }
            }
            SplitTimestampCommand::Cut {
                split_id, guard, ..
            } => {
                let attempt = self.job(*split_id).await?;
                self.verify_guard(&attempt, guard, &self.catalog).await?;
            }
            SplitTimestampCommand::Bind {
                split_id,
                children,
                placement,
                ..
            } => {
                let attempt = self.job(*split_id).await?;
                let cut = attempt.image.as_ref().ok_or_else(invalid)?;
                self.verify_guard(&attempt, &cut.guard, &self.catalog)
                    .await?;
                if let Some(placement) = placement {
                    self.client
                        .verify_source_placement(&placement.source, attempt.authority)
                        .await?;
                }
                for (index, child) in children.iter().enumerate() {
                    if crate::oltp::mvcc::planner::split::child_group(
                        &attempt.intent.source,
                        *split_id,
                        index,
                    )? != child.target
                    {
                        return Err(invalid());
                    }
                }
            }
            SplitTimestampCommand::Close {
                split_id, settled, ..
            } => {
                let attempt = self.job(*split_id).await?;
                let manifest = settled.statement().manifest;
                let proof = if manifest.participant_count == 0 {
                    self.failure_settlement(&attempt).await?
                } else {
                    self.settlement(&attempt, manifest, authority).await?
                };
                if serialized_digest(&proof)? != serialized_digest(settled)? {
                    return Err(invalid());
                }
                if let TxnDecision::Commit { commit_ts } = proof.statement().decision.decision {
                    if self.client.source(&attempt).await?.published_at() != Some(commit_ts) {
                        return Err(invalid());
                    }
                }
            }
        }
        timestamps.split_timestamp(command).await
    }
    async fn dispatch(
        &self,
        session: &AuthenticatedSession,
        operation: Operation,
        node: u64,
        raw: ChargedBytes,
    ) -> Result<ChargedBytes, ProtocolError> {
        let shared = self.client.shared();
        let class = operation.traffic().byte_class();
        let decode = || async {
            let input = raw.clone();
            let bytes = shared
                .execute(ExecutionClass::Control, 0, 0, move || {
                    json_decode_budget(input.as_slice())
                })
                .await?;
            Ok::<_, ProtocolError>(shared.reserve_scratch(class, bytes)?)
        };
        let _owner = decode().await?;
        macro_rules! input {
            ($ty:ty) => {
                serde_json::from_slice::<$ty>(raw.as_slice()).map_err(|_| invalid())?
            };
        }
        macro_rules! output {
            ($value:expr) => {
                shared.encode_in(class, &$value, MAX_METADATA)
            };
        }
        match operation {
            Operation::Capacity => {
                let request = input!(CapacityRequest);
                self.client
                    .verify_source_placement(&request.source, request.authority)
                    .await?;
                let voter = request
                    .source
                    .voters
                    .iter()
                    .find(|voter| voter.member.node_id == node)
                    .ok_or_else(invalid)?;
                let (_, mounted) = self.selected_pool(voter, None).await?;
                let capacity = check_headroom(
                    shared,
                    &mounted,
                    request.source.voters.len(),
                    request.required_bytes,
                    request.page_bytes,
                )?;
                output!(capacity)
            }
            Operation::Usage => {
                let request = input!(UsageRequest);
                self.client
                    .verify_source_placement(&request.source, request.authority)
                    .await?;
                let voter = request
                    .source
                    .voters
                    .iter()
                    .find(|voter| voter.member.node_id == node)
                    .ok_or_else(invalid)?;
                let local = self.manager().client().local_identity();
                if voter.worker != local || request.target == 0 {
                    return Err(unauthorized());
                }
                self.selected_pool(voter, voter.registered_pool_generation)
                    .await?;
                let running = self
                    .manager()
                    .hosted_data(&request.source.record.identity, node)?;
                if running.state.store().pool().identity().pool_id != voter.member.pool_id {
                    return Err(unavailable());
                }
                let membership = running.observe_membership().await?;
                if membership.applied_at.index < request.source.record.membership_position.index
                    || !membership.replicas().iter().eq(request
                        .source
                        .voters
                        .iter()
                        .map(|voter| &voter.member))
                {
                    return Err(unavailable());
                }
                drop(membership);
                let machine = ParticipantMachine::new(running.state.store().clone())?;
                output!(LocalUsageScan::observe(&self.usage, request, machine).await?)
            }
            Operation::Job | Operation::FailureSettlement => {
                let Host::System {
                    timestamps: Some(timestamps),
                    ..
                } = &self.host
                else {
                    return Err(unauthorized());
                };
                if operation == Operation::Job {
                    let bytes = timestamps.split_attempt_raw(input!(Uuid)).await?;
                    if !self.fixed_peer(session) {
                        let _decode =
                            shared.reserve_scratch(class, json_decode_budget(bytes.as_slice())?)?;
                        let job: SplitAttempt =
                            serde_json::from_slice::<Option<SplitAttempt>>(bytes.as_slice())
                                .map_err(|_| invalid())?
                                .ok_or_else(unauthorized)?;
                        self.authorize_proof(session, job.intent.source.group_id)
                            .await?;
                    }
                    return Ok(bytes);
                }
                let request = input!(SourceRequest);
                self.authorize_proof(session, request.source.group_id)
                    .await?;
                let _raw = timestamps.split_attempt_raw(request.split_id).await?;
                let _decode =
                    shared.reserve_scratch(class, json_decode_budget(_raw.as_slice())?)?;
                let attempt = serde_json::from_slice::<Option<SplitAttempt>>(_raw.as_slice())
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?;
                self.validate_source(&attempt, &request)?;
                output!(self.failure_settlement(&attempt).await?)
            }
            Operation::Timestamp => {
                output!(self.apply_timestamp(input!(SplitTimestampCommand)).await?)
            }
            Operation::Freeze => {
                let request = input!(SourceRequest);
                let attempt = self.job(request.split_id).await?;
                let participant = self.open_source(&attempt, &request, node).await?;
                let cut = attempt.image.as_ref().ok_or_else(invalid)?;
                if attempt.closed.is_some() {
                    return Err(invalid());
                }
                if let Some(placement) = attempt.placement() {
                    self.client
                        .verify_source_placement(&placement.source, attempt.authority)
                        .await?;
                }
                self.verify_guard(&attempt, &cut.guard, &request.source)
                    .await?;
                let applied = participant
                    .propose(OwnedCommand::new_owned(
                        ParticipantCommand::SplitFreeze {
                            authority: attempt.authority,
                            children: Box::new(attempt.children()?.clone()),
                        },
                        shared,
                        ByteClass::Progress,
                    )?)
                    .await?;
                match applied.reply {
                    ParticipantReply::SplitSourceFrozen(_) => output!(()),
                    ParticipantReply::Rejected(crate::oltp::mvcc::store::Fault::Incomplete) => {
                        Err(unavailable())
                    }
                    _ => Err(conflict("source refused the exact split freeze")),
                }
            }
            Operation::Source => {
                let request = input!(SourceRequest);
                self.authorize_proof(session, request.source.group_id)
                    .await?;
                let participant = self.participant(&request.source, node).await?;
                let proof = participant
                    .strong_split_source_local(request.split_id)
                    .await?;
                output!((proof.statement(), proof.published_at()))
            }
            Operation::Host | Operation::Activate => {
                let request = input!(TargetRequest);
                let attempt = self.job(request.split_id).await?;
                let binding = self.binding(&attempt, &request)?;
                if attempt.authority != self.current().await? || attempt.closed.is_some() {
                    return Err(invalid());
                }
                let (local, voters) = if let Some(placement) = attempt.placement() {
                    self.client
                        .verify_source_placement(&placement.source, attempt.authority)
                        .await?;
                    self.verify_guard(&attempt, &binding.guard, &binding.source)
                        .await?;
                    let (index, voter) = placement
                        .source
                        .voters
                        .iter()
                        .enumerate()
                        .find(|(_, voter)| voter.member.node_id == node)
                        .ok_or_else(unauthorized)?;
                    let expected = *placement.pool_generations.get(index).ok_or_else(invalid)?;
                    let (pool, mounted) = self.selected_pool(voter, Some(expected)).await?;
                    let voters: Vec<_> = placement
                        .source
                        .voters
                        .iter()
                        .map(|voter| voter.member.clone())
                        .collect();
                    if operation == Operation::Host {
                        check_disk_headroom(&mounted, voters.len(), placement.required_bytes)?;
                        self.host_child(&pool, &binding.target, voter, &voters)
                            .await?;
                        return output!((&binding.target, &voter.member));
                    }
                    (voter.member.clone(), voters)
                } else {
                    let Host::System { factory, .. } = &self.host else {
                        return Err(unauthorized());
                    };
                    let voters = factory.initial_voters().await?.into_inner();
                    let local = voters.iter().find(|member| member.node_id == node
                        && member.worker_id == factory.local.worker_id
                        && member.pool_id == factory.local.pool_id)
                        .ok_or_else(unauthorized)?;
                    if operation == Operation::Host {
                        factory.host_at(&binding.target, local, &voters, true).await?;
                        return output!((&binding.target, local));
                    }
                    (local.clone(), voters)
                };
                let spec = DataGroupSpec {
                    group: binding.target.clone(),
                    authority: attempt.authority,
                    catalog_group_id: attempt.catalog_group_id,
                    transition_id: split_id(binding.target.group_id, b"fence"),
                    schema_epoch: binding.source_schema_epoch,
                    routing_epoch: binding
                        .source_routing_epoch
                        .checked_add(1)
                        .ok_or_else(invalid)?,
                    span: binding.target_span.clone(),
                };
                let running = self.manager().hosted_data(&spec.group, node)?;
                let observed = running.observe_membership().await?;
                if observed.replicas() != voters.as_slice()
                    || running.state.store().pool().identity().pool_id != local.pool_id
                {
                    return Err(invalid());
                }
                let participant = ParticipantHandle::new(
                    ParticipantMachine::new(running.state.store().clone())?,
                    running.clone(),
                );
                let fence = participant
                    .fence_initial(
                        spec.transition_id,
                        spec.authority,
                        spec.catalog_group_id,
                        spec.schema_epoch,
                        spec.routing_epoch,
                        spec.span,
                    )
                    .await?;
                let observed = running.observe_membership().await?;
                if observed.replicas() != voters.as_slice()
                    || observed.applied_at.index < fence.applied_at.index
                {
                    return Err(invalid());
                }
                output!(Membership {
                    group: observed.group,
                    replicas: observed.replicas,
                    applied_at: observed.applied_at
                })
            }
            Operation::Stage => {
                let request = input!(TargetRequest);
                let attempt = self.job(request.split_id).await?;
                let binding = self.binding(&attempt, &request)?;
                let participant = self.participant(&binding.target, node).await?;
                participant.epochs().await?;
                self.live_source(&attempt, &binding.target).await?;
                let applied = participant
                    .propose(OwnedCommand::new_owned(
                        ParticipantCommand::SplitStage {
                            authority: attempt.authority,
                            binding: Box::new(binding.clone()),
                        },
                        shared,
                        ByteClass::Progress,
                    )?)
                    .await?;
                match applied.reply {
                    ParticipantReply::SplitStaged => output!(()),
                    _ => Err(conflict("child refused the exact split staging binding")),
                }
            }
            Operation::Progress | Operation::Imported => {
                let request = input!(TargetRequest);
                let attempt = self.job(request.split_id).await?;
                let binding = self.binding(&attempt, &request)?;
                let participant = self.participant(&binding.target, node).await?;
                if operation == Operation::Imported {
                    return output!(
                        participant
                            .strong_split_imported_local(binding.clone())
                            .await?
                            .statement()
                    );
                }
                let progress = participant
                    .strong_split_progress_local(binding, cursor_bytes(&attempt))
                    .await?;
                let progress = progress.payload()?;
                output!(SplitCursor {
                    pages: progress.pages,
                    root: progress.root,
                    after: progress.after_digest,
                    complete: progress.complete
                })
            }
            Operation::Advance => {
                let request = input!(Advance);
                let attempt = self.job(request.child.split_id).await?;
                let binding = self.binding(&attempt, &request.child)?;
                let participant = self.participant(&binding.target, node).await?;
                let progress = participant
                    .strong_split_progress_local(binding, cursor_bytes(&attempt))
                    .await?;
                let progress = progress.payload()?;
                let current = SplitCursor {
                    pages: progress.pages,
                    root: progress.root,
                    after: progress.after_digest,
                    complete: progress.complete,
                };
                if current != request.expected
                    || current.complete
                    || progress.binding != child_digest(binding)?
                {
                    return Err(conflict("split import cursor changed"));
                }
                self.live_source(&attempt, &binding.target).await?;
                let image = self
                    .client
                    .image(ImageRead {
                        source: source(&attempt),
                        target: binding.target.clone(),
                        after: progress.after.clone(),
                    })
                    .await?;
                let (command, expected) = binding
                    .import_page_from_image(
                        shared,
                        current.pages,
                        current.root,
                        current.after,
                        image,
                    )
                    .await?;
                let applied = participant.propose(command).await?;
                match applied.reply {
                    ParticipantReply::SplitImported { pages, root, .. }
                        if current.pages.checked_add(1) == Some(pages) && expected == root =>
                    {
                        output!(())
                    }
                    _ => Err(conflict("child refused the verified source image")),
                }
            }
            Operation::Images => {
                let request = input!(ImageRead);
                let attempt = self.job(request.source.split_id).await?;
                let participant = self.open_source(&attempt, &request.source, node).await?;
                let binding = self.binding(
                    &attempt,
                    &TargetRequest {
                        split_id: request.source.split_id,
                        target: request.target,
                    },
                )?;
                self.client
                    .control
                    .service_role(
                        &self.scope(),
                        session.remote(),
                        ServiceAction::ParticipantProof {
                            source_group: binding.target.group_id,
                        },
                        binding.source.clone(),
                    )
                    .await?;
                let proof = participant
                    .strong_split_source_local(attempt.intent.split_id)
                    .await?;
                verify_source(&proof, &attempt)?;
                if proof.published_at().is_some() || attempt.closed.is_some() {
                    return Err(invalid());
                }
                self.verify_guard(&attempt, &binding.guard, &binding.source)
                    .await?;
                let image = participant
                    .committed_image_page(OwnedCommand::<ImageRequest>::new_owned(
                        binding.image_request(request.after, attempt.intent.page_bytes),
                        shared,
                        ByteClass::Data,
                    )?)
                    .await?;
                shared.encode_in(ByteClass::Data, &image, MAX_FIXED_COMMAND_BYTES)
            }
            Operation::Publication => {
                let request = input!(Publication);
                self.authorize_proof(session, request.source.source.group_id)
                    .await?;
                self.verify_publication(request).await?;
                output!(())
            }
            Operation::Finish => {
                let request = input!(Finish);
                let attempt = self.job(request.source.split_id).await?;
                let participant = self.open_source(&attempt, &request.source, node).await?;
                let proof = if request.manifest.participant_count == 0 {
                    let proof = if matches!(&self.host, Host::Data(_)) {
                        self.client
                            .typed::<SealedSettlement>(
                                &self.client.timestamps,
                                Operation::FailureSettlement,
                                &request.source,
                            )
                            .await?
                    } else {
                        self.failure_settlement(&attempt).await?
                    };
                    proof.validate_for(attempt.txn, &attempt.request, &request.manifest)?;
                    if proof.statement().manifest != request.manifest {
                        return Err(invalid());
                    }
                    proof
                } else {
                    self.settlement(
                        &attempt,
                        request.manifest,
                        ServiceAction::ParticipantProof {
                            source_group: request.source.source.group_id,
                        },
                    )
                    .await?
                };
                if let TxnDecision::Commit { commit_ts } = proof.statement().decision.decision {
                    verify_source(
                        &participant
                            .strong_split_source_local(attempt.intent.split_id)
                            .await?,
                        &attempt,
                    )?;
                    self.client
                        .typed::<()>(
                            &self.catalog,
                            Operation::Publication,
                            &Publication {
                                source: request.source,
                                manifest: request.manifest,
                                at: commit_ts,
                            },
                        )
                        .await?;
                }
                let applied = participant
                    .propose(OwnedCommand::new_owned(
                        ParticipantCommand::SplitFinishSource {
                            authority: self.current().await?,
                            split_id: attempt.intent.split_id,
                            settled: proof,
                        },
                        shared,
                        ByteClass::Progress,
                    )?)
                    .await?;
                match applied.reply {
                    ParticipantReply::SplitSourceFinished(_) => output!(()),
                    _ => Err(conflict("source refused the exact split settlement")),
                }
            }
        }
    }
}
impl Endpoint for SplitEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        raw: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let header = self.parse(session, raw)?;
            if header.operation.owner_only() {
                self.authorize(session).await?;
            }
            Ok(ReceivePlan {
                class: header.operation.traffic(),
                section_lengths: [header.payload_bytes, 0, 0],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        raw: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let header = self.parse(&session, raw.as_slice())?;
            let result = async {
                if header.operation.owner_only() {
                    self.authorize(&session).await?;
                }
                self.dispatch(
                    &session,
                    header.operation,
                    header.worker_node,
                    sections.section(0).ok_or_else(invalid)?.clone(),
                )
                .await
            }
            .await;
            let (payload, error) = match result {
                Ok(payload) => (Some(payload), None),
                Err(error) => (None, Some(error)),
            };
            use sha2::{Digest as _, Sha256};
            let reply = self.client.shared().encode_in(
                header.operation.traffic().byte_class(),
                &SplitReply {
                    worker_node: header.worker_node,
                    nonce: session.nonce(),
                    payload_bytes: payload.as_ref().map_or(0, ChargedBytes::len),
                    digest: Digest::from_bytes(
                        Sha256::digest(payload.as_ref().map_or(&[][..], ChargedBytes::as_slice))
                            .into(),
                    ),
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
fn cursor_bytes(attempt: &SplitAttempt) -> usize {
    (attempt.intent.page_bytes + (16 << 10)).min(MAX_FIXED_COMMAND_BYTES)
}
fn verify_source(proof: &VerifiedSplitSource, attempt: &SplitAttempt) -> Result<(), ProtocolError> {
    let statement = proof.statement();
    let [left, right] = attempt.children()?;
    if statement.group != attempt.intent.source
        || statement.split_id != attempt.intent.split_id
        || statement.txn != attempt.txn
        || statement.request != attempt.request
        || statement.read_ts != left.read_ts
        || statement.children != [child_digest(left)?, child_digest(right)?]
    {
        return Err(invalid());
    }
    Ok(())
}
