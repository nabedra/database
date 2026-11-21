//! Application work at the current owner of a scoped RF3 RAM queue.
mod maintenance;
use super::{
    application::{
        ApplicationObservation, INLINE_RECEIPT_BYTES, ObservedApplication, ScopedTarget,
        stale_route,
    },
    replica::{
        AcceptedContext, MAX_RECORD_HEADER_BYTES, OriginalAuthorization, QUEUE_FORMAT_VERSION,
        QueueReadToken, QueueScope, ReadGrantSet, RecordFrame, RecordHeader, WorkerReplica,
        scoped_store::{ScopedAdmissionPermit, ScopedSurvivor, scoped_authority},
    },
    transaction::TransactionCoordinator,
    worker_endpoint::{QueueEndpoint, QueuePeerClient},
};
use crate::{
    executor::Executor,
    oltp::{
        mvcc::{
            catalog::{CatalogCapture, CatalogHandle, PointCatalog, RouteRead, ScopedCapture},
            planner::{
                NativePlanner, ParticipantResolver, PlanError, PlannedOperation, ResolveFuture,
            },
            store::{ParticipantHandle, lifecycle::VerifiedLifecycleGuard},
        },
        pool::{ByteClass, ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    },
    oltp_replication::{
        authority::{
            control::{ControlClient, ScopedCatalogRead, ScopedQueuePlacement, ServiceAction},
            status::{
                IdentityLookup, IdentityReservation, RequestKind, ScopedAdmission, StatusFault,
                StatusReply, StatusService,
            },
        },
        multiplex::WorkerClient,
        service_rpc::{ParticipantClient, TimestampClient},
        types::*,
    },
    protocol::{self, ErrorCode, Operation, Outcome, ProtocolError},
    request_queue::{QueueError, RequestRecord, RequestState},
    response::{self, EncodedResponse},
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
use uuid::Uuid;

pub(super) const COPY_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) struct ScopedWorkerServices {
    pub local: WorkerIdentity,
    pub control: Arc<ControlClient>,
    pub timestamps: Arc<TimestampClient>,
    pub status: Arc<dyn StatusService>,
    pub catalog: CatalogHandle,
    pub resolver: Arc<dyn ParticipantResolver>,
    pub replica: Arc<Mutex<WorkerReplica>>,
    pub endpoint: Arc<QueueEndpoint>,
    pub peer: Arc<QueuePeerClient>,
    pub operator: bool,
    pub max_result_bytes: usize,
}

pub(super) struct ScopedParticipants {
    shared: Arc<WorkerSharedResources>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
}

impl ScopedParticipants {
    pub(super) fn new(
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            transport,
            control,
        })
    }
}

impl ParticipantResolver for ScopedParticipants {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>> {
        Box::pin(async move {
            let client = ParticipantClient::new(
                group.clone(),
                self.shared.clone(),
                self.transport.clone(),
                self.control.clone(),
                ServiceAction::Owner,
            )?;
            Ok(Arc::new(ParticipantHandle::remote(
                group.clone(),
                self.shared.clone(),
                client,
            )?))
        })
    }

    fn create_data<'a>(
        &'a self,
        _table: &'a protocol::TableDescriptor,
        _pin: &'a crate::oltp::mvcc::catalog::PinnedCatalog,
    ) -> ResolveFuture<'a, crate::oltp::mvcc::catalog::ShardRoute> {
        Box::pin(async { Err(unavailable()) })
    }
}

pub(super) struct ScopedIngress {
    placement: ScopedQueuePlacement,
    ready: ReadyMembership,
}

enum ScopedReservation {
    Pending(IdentityReservation, ScopedAdmissionPermit),
    Terminal,
}

pub(super) struct ScopedWorker {
    services: ScopedWorkerServices,
    planner: NativePlanner,
    transactions: TransactionCoordinator,
    principal: Digest,
    scope: QueueScope,
    group: GroupIdentity,
    ingress: Executor,
    actors: Executor,
    sequence: Mutex<u64>,
    gates: Mutex<BTreeMap<Uuid, Weak<AsyncMutex<()>>>>,
    active: Mutex<BTreeMap<Uuid, Uuid>>,
    stopping: AtomicBool,
    maintenance: Mutex<Option<tokio::task::JoinHandle<()>>>,
    retention: Duration,
}

impl ScopedWorker {
    pub(super) fn new(
        services: ScopedWorkerServices,
        group: GroupIdentity,
        bearer: &str,
        max_workers_per_request: u64,
        result_retention_ms: u64,
        ingress: Executor,
        actors: Executor,
    ) -> Result<Arc<Self>, ProtocolError> {
        let shared = services.catalog.resources().clone();
        let planner = NativePlanner::new(
            services.catalog.clone(),
            services.resolver.clone(),
            max_workers_per_request,
        )?;
        let transactions = TransactionCoordinator::new(
            shared,
            services.status.clone(),
            services.timestamps.clone(),
            services.resolver.clone(),
            services.catalog.participant().clone(),
        );
        Ok(Arc::new(Self {
            services,
            planner,
            transactions,
            principal: digest(bearer.as_bytes()),
            scope: QueueScope {
                cluster_id: group.cluster_id,
                database: group.database.clone(),
            },
            group,
            ingress,
            actors,
            sequence: Mutex::new(1),
            gates: Mutex::new(BTreeMap::new()),
            active: Mutex::new(BTreeMap::new()),
            stopping: AtomicBool::new(false),
            maintenance: Mutex::new(None),
            retention: Duration::from_millis(result_retention_ms),
        }))
    }

    pub(super) fn resources(&self) -> &Arc<WorkerSharedResources> {
        self.services.catalog.resources()
    }

    pub(super) fn authorize_application(
        &self,
        group: &GroupIdentity,
        bearer: &str,
        database: &Text<128>,
    ) -> Result<(), ProtocolError> {
        if group != &self.group
            || database != &self.scope.database
            || bearer.len() > 4096
            || digest(bearer.as_bytes()) != self.principal
        {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "unauthorized API database scope",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }

    pub(super) async fn authorize_ingress(
        &self,
        api: WorkerIdentity,
        target: ScopedTarget,
        request_id: Option<Uuid>,
    ) -> Result<ScopedIngress, ProtocolError> {
        if target.owner != self.services.local || request_id.is_some_and(|id| id.is_nil()) {
            return Err(stale_route());
        }
        let grant = self
            .services
            .control
            .service_role(
                &self.scope,
                api,
                ServiceAction::ScopedQueueIngress {
                    queue_group_id: target.group_id,
                    queue_generation: target.queue_generation,
                    queue_view: target.view,
                    owner: target.owner,
                },
                self.group.clone(),
            )
            .await
            .map_err(|error| {
                if error.code == ErrorCode::StorageUnavailable {
                    stale_route()
                } else {
                    error
                }
            })?;
        let placement = grant
            .scoped_ingress
            .ok_or_else(stale_route)?
            .placement(&self.group)?;
        if grant.owner != Some(self.services.local)
            || grant.ready.view.run_id != target.view.run_id
            || ScopedTarget::from(&placement) != target
        {
            return Err(stale_route());
        }
        Ok(ScopedIngress {
            placement,
            ready: grant.ready,
        })
    }

    pub(super) fn stop_accepting(&self) {
        self.stopping.store(true, Ordering::Release);
        self.ingress.stop_accepting();
    }

    pub(super) async fn drain(&self) {
        self.stop_accepting();
        let maintenance = self
            .maintenance
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = maintenance {
            let _ = task.await;
        }
        self.ingress.drain().await;
        self.actors.drain().await;
    }

    pub(super) async fn submit(
        self: &Arc<Self>,
        ingress: ScopedIngress,
        body: ChargedBytes,
        api: WorkerIdentity,
    ) -> Result<RequestRecord, ProtocolError> {
        let runtime = self.clone();
        self.ingress
            .try_admit()?
            .spawn(move || async move { runtime.admit(ingress, body, api).await })?
            .receive()
            .await
    }

    fn gate(&self, id: Uuid) -> Result<Arc<AsyncMutex<()>>, ProtocolError> {
        let mut gates = self.gates.lock().map_err(|_| unavailable())?;
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(&id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id, Arc::downgrade(&gate));
        Ok(gate)
    }

    async fn decode(
        &self,
        body: ChargedBytes,
    ) -> Result<(protocol::Request, Digest, Arc<ScratchLease>), ProtocolError> {
        let shared = self.resources().clone();
        let task = shared.clone();
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let bytes = crate::oltp::mvcc::request_decode_budget(body.as_ref())?
                    .checked_mul(2)
                    .ok_or_else(unavailable)?;
                let held = Arc::new(task.reserve_scratch(ByteClass::Data, bytes)?);
                Ok((
                    protocol::parse_request(body.as_ref())?,
                    digest(body.as_ref()),
                    held,
                ))
            })
            .await
    }

    fn frame(
        &self,
        placement: &ScopedQueuePlacement,
        reservation: IdentityReservation,
        body: ChargedBytes,
        context: &AcceptedContext,
    ) -> Result<RecordFrame, ProtocolError> {
        let current = scoped_authority(placement).map_err(QueueError::protocol)?;
        let context = self.resources().encode(context, MAX_RECORD_HEADER_BYTES)?;
        let sequence = {
            let mut next = self.sequence.lock().map_err(|_| unavailable())?;
            let sequence = *next;
            *next = sequence.checked_add(1).ok_or_else(unavailable)?;
            sequence
        };
        let header = RecordHeader {
            queue_format_version: QUEUE_FORMAT_VERSION,
            scope: self.scope.clone(),
            ready: current.ready,
            owner: current.owner,
            request_id: reservation.request_id,
            digest: reservation.digest,
            sequence,
            version: 1,
            accepted: false,
            state: RequestState::Queued,
            response_status: None,
            completed_at_ms: None,
            section_lengths: [body.len() as u64, context.len() as u64, 0],
            all_copy_evidence: false,
            read_grants: ReadGrantSet::initial(),
        };
        Ok(RecordFrame {
            header: self.resources().encode(&header, MAX_RECORD_HEADER_BYTES)?,
            sections: [Some(body), Some(context), None],
        })
    }

    async fn existing(
        &self,
        placement: &ScopedQueuePlacement,
        id: Uuid,
    ) -> Result<Option<ScopedSurvivor>, ProtocolError> {
        match self.services.endpoint.scoped_record(placement, id).await {
            Ok(record) => Ok(Some(record)),
            Err(error) if error == QueueError::NotFound.protocol() => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn admit(
        self: &Arc<Self>,
        ingress: ScopedIngress,
        body: ChargedBytes,
        api: WorkerIdentity,
    ) -> Result<RequestRecord, ProtocolError> {
        let deadline = Instant::now() + COPY_TIMEOUT;
        let placement = ingress.placement;
        let (parsed, digest, decoded) = self.decode(body.clone()).await?;
        let id = parsed.request_id;
        let (table, key, kind) = match &parsed.operation {
            Operation::Get(request) => (request.table_id, &request.key, RequestKind::Read),
            Operation::Update(request) => (request.table_id, &request.key, RequestKind::Mutation),
            _ => return Err(stale_route()),
        };
        if !matches!(placement.group.kind, GroupKind::Data { table_id, .. } if table_id == table) {
            return Err(stale_route());
        }
        let gate = self.gate(id)?;
        let _guard = gate.lock().await;
        let actor = self.actors.try_admit()?;
        let mut captured = None;
        let (frame, admission) = match self.existing(&placement, id).await? {
            Some(record) => {
                if record.reservation.digest != digest || record.reservation.kind != kind {
                    return Err(QueueError::Conflict.protocol());
                }
                let payload = record
                    .payload_frame(self.resources())
                    .map_err(QueueError::protocol)?;
                if !payload.sections[0]
                    .as_ref()
                    .is_some_and(|raw| raw.as_slice() == body.as_slice())
                {
                    return Err(QueueError::Conflict.protocol());
                }
                if record.acceptance.is_some() {
                    self.dispatch(placement.clone(), ingress.ready, id, actor, None)?;
                    return Ok(observation_record(
                        &placement,
                        &record,
                        None,
                        RequestState::Queued,
                    )?);
                }
                (payload, None)
            }
            None => {
                let reservation = self
                    .reserve(
                        &placement,
                        ingress.ready.view,
                        id,
                        digest,
                        kind,
                        key,
                        body.len(),
                    )
                    .await?;
                let ScopedReservation::Pending(reservation, admission) = reservation else {
                    return Ok(self
                        .durable_observation(&placement, id, digest, false, None)
                        .await?
                        .metadata
                        .record);
                };
                let identity = pin_identity(reservation);
                let snapshot = self
                    .services
                    .timestamps
                    .begin_scoped_snapshot_with_origin(
                        &placement,
                        ingress.ready.view,
                        identity.clone(),
                        reservation,
                        OriginalAuthorization {
                            database: self.scope.database.clone(),
                            principal: self.principal,
                            policy_revision: 1,
                            ingress_id: api.worker_id,
                            ingress_incarnation: api.incarnation,
                            credential_expires_at_ms: None,
                        },
                    )
                    .await;
                let (storage_pin, authorization) = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        let closure = self
                            .services
                            .timestamps
                            .lookup_scoped_admission_closure(
                                &placement,
                                identity.clone(),
                                reservation,
                            )
                            .await?;
                        let Some(closure) = closure else {
                            return Err(error);
                        };
                        return Err(self
                            .reconcile_superseded_admission(
                                &placement,
                                ingress.ready.view.authority(),
                                reservation,
                                &closure,
                            )
                            .await?);
                    }
                };
                let capture = self
                    .services
                    .catalog
                    .capture_scoped_lineage(
                        &placement,
                        reservation,
                        &identity,
                        storage_pin,
                        key,
                        ingress.ready.view.authority(),
                    )
                    .await?;
                let capture = match capture {
                    ScopedCapture::Captured(capture) => capture,
                    ScopedCapture::ScopeSuperseded => {
                        let closure = self
                            .services
                            .timestamps
                            .close_superseded_scoped_admission(
                                &placement,
                                ingress.ready.view,
                                identity,
                                reservation,
                                storage_pin,
                            )
                            .await?;
                        return Err(self
                            .reconcile_superseded_admission(
                                &placement,
                                ingress.ready.view.authority(),
                                reservation,
                                &closure,
                            )
                            .await?);
                    }
                };
                let context = AcceptedContext {
                    authorization,
                    read: Some(QueueReadToken {
                        identity: identity.clone(),
                        storage_pin,
                        catalog_root: capture.catalog_root,
                        schema_epoch: capture.schema_epoch,
                        routing_epoch: capture.routing_epoch,
                    }),
                };
                let binding = RequestBinding {
                    run_id: placement.view.run_id,
                    request_id: id,
                    digest,
                    accepted_context: crate::oltp_replication::owned::serialized_digest(&context)?,
                };
                self.services
                    .timestamps
                    .bind_scoped_snapshot_context(
                        &placement,
                        ingress.ready.view,
                        identity,
                        storage_pin,
                        binding,
                    )
                    .await?;
                captured = Some(capture);
                (
                    self.frame(&placement, reservation, body, &context)?,
                    Some(admission),
                )
            }
        };
        drop((parsed, decoded));
        self.services
            .endpoint
            .accept_scoped(&self.services.peer, &placement, &frame, deadline, admission)
            .await?;
        let record = self.services.endpoint.scoped_record(&placement, id).await?;
        self.dispatch(placement.clone(), ingress.ready, id, actor, captured)?;
        observation_record(&placement, &record, None, RequestState::Queued)
    }

    async fn reserve(
        &self,
        placement: &ScopedQueuePlacement,
        view: AuthorityView,
        id: Uuid,
        digest: Digest,
        kind: RequestKind,
        key: &protocol::Key,
        body_bytes: usize,
    ) -> Result<ScopedReservation, ProtocolError> {
        let scoped = ScopedAdmission {
            protocol_version: ScopedAdmission::VERSION,
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            status_group_id: self.services.status.group_for_request(id).group_id,
        };
        let existing = self
            .services
            .status
            .lookup_identity_for_scoped(placement, id, digest)
            .await?;
        let reserve_memory = || self.services.endpoint.reserve_scoped_payload(body_bytes);
        let (reservation, admission) = match existing {
            Some(
                IdentityLookup::Reserved(reservation) | IdentityLookup::TimestampPoint(reservation),
            ) => {
                if reservation.scoped != Some(scoped) || reservation.original_run != view.run_id {
                    return Err(stale_route());
                }
                (reservation, reserve_memory()?)
            }
            Some(IdentityLookup::Abandoned(_)) => return Err(QueueError::Abandoned.protocol()),
            Some(IdentityLookup::Terminal(terminal)) => {
                let reservation = terminal_reservation(&terminal);
                if reservation.scoped != Some(scoped) || reservation.original_run != view.run_id {
                    return Err(stale_route());
                }
                return Ok(ScopedReservation::Terminal);
            }
            Some(IdentityLookup::Rejected(rejection)) => {
                return Err(rejection.failure.result().err().ok_or_else(unavailable)?);
            }
            None => {
                let request = RequestBinding {
                    run_id: placement.view.run_id,
                    request_id: id,
                    digest,
                    accepted_context: Digest::from_bytes([0; 32]),
                };
                match self
                    .services
                    .catalog
                    .scoped_admission_route(placement, view.authority(), request, key)
                    .await?
                {
                    RouteRead::Route { page, index }
                        if page.shards[index].group == placement.group => {}
                    RouteRead::Blocked(_) => return Err(unavailable()),
                    _ => return Err(stale_route()),
                }
                if self
                    .services
                    .control
                    .scoped_queue_capability()
                    .await?
                    .activation
                    .is_none()
                {
                    return Err(stale_route());
                }
                let admission = reserve_memory()?;
                let applied = self
                    .services
                    .status
                    .reserve_scoped_identity(view.authority(), id, digest, kind, scoped)
                    .await?;
                let (_, reply, _owner) = applied.into_parts()?;
                match reply {
                    StatusReply::Identity(
                        IdentityLookup::Reserved(reservation)
                        | IdentityLookup::TimestampPoint(reservation),
                    ) => (reservation, admission),
                    StatusReply::Rejected(
                        StatusFault::DigestConflict | StatusFault::TerminalConflict,
                    ) => {
                        return Err(QueueError::Conflict.protocol());
                    }
                    StatusReply::Identity(IdentityLookup::Abandoned(_)) => {
                        return Err(QueueError::Abandoned.protocol());
                    }
                    StatusReply::Identity(IdentityLookup::Terminal(_)) => {
                        return Ok(ScopedReservation::Terminal);
                    }
                    StatusReply::Identity(IdentityLookup::Rejected(rejection)) => {
                        return Err(rejection.failure.result().err().ok_or_else(unavailable)?);
                    }
                    _ => return Err(unavailable()),
                }
            }
        };
        if reservation.request_id != id
            || reservation.digest != digest
            || reservation.kind != kind
            || reservation.original_run != placement.view.run_id
            || reservation.scoped != Some(scoped)
        {
            return Err(QueueError::Conflict.protocol());
        }
        Ok(ScopedReservation::Pending(reservation, admission))
    }

    async fn reconcile_superseded_admission(
        &self,
        placement: &ScopedQueuePlacement,
        authority: Authority,
        reservation: IdentityReservation,
        closure: &crate::oltp_replication::authority::timestamp::ScopedAdmissionClosure,
    ) -> Result<ProtocolError, ProtocolError> {
        let rejection = self
            .services
            .status
            .reject_scoped_route_moved(placement, authority, reservation, closure)
            .await?;
        rejection.failure.result().err().ok_or_else(unavailable)
    }

    fn dispatch(
        self: &Arc<Self>,
        placement: ScopedQueuePlacement,
        ready: ReadyMembership,
        id: Uuid,
        actor: crate::executor::Admission,
        captured: Option<CatalogCapture>,
    ) -> Result<(), ProtocolError> {
        {
            let mut active = self.active.lock().map_err(|_| unavailable())?;
            if active.contains_key(&id) {
                return Ok(());
            }
            active.insert(id, placement.group.group_id);
        }
        let runtime = self.clone();
        if let Err(error) = actor.spawn(move || async move {
            let _active = ActiveRequest {
                runtime: runtime.clone(),
                id,
            };
            runtime.execute(placement, ready, id, captured).await
        }) {
            self.active.lock().map_err(|_| unavailable())?.remove(&id);
            return Err(error);
        }
        Ok(())
    }

    async fn execute(
        &self,
        placement: ScopedQueuePlacement,
        ready: ReadyMembership,
        id: Uuid,
        captured: Option<CatalogCapture>,
    ) -> Result<(), ProtocolError> {
        let record = self.services.endpoint.scoped_record(&placement, id).await?;
        if record.terminal_quorum.is_some() {
            return self.release_terminal(&placement, &record).await;
        }
        if record.acceptance.is_none() {
            return Err(unavailable());
        }
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint(
            "scoped-dispatch",
            id,
            &record.stamp.binding(),
        )
        .await;
        let (mut frame, terminal) = record
            .frames(self.resources())
            .map_err(QueueError::protocol)?;
        let body = frame.sections[0].as_ref().ok_or_else(unavailable)?.clone();
        let (parsed, digest, owner) = self.decode(body).await?;
        let context_bytes = frame.sections[1].as_ref().ok_or_else(unavailable)?;
        let context: AcceptedContext =
            serde_json::from_slice(context_bytes.as_ref()).map_err(|_| unavailable())?;
        let read = context.read.as_ref().ok_or_else(unavailable)?;
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint("snapshot", id, &read.storage_pin.read_ts)
            .await;
        let request = record.stamp.binding();
        let GroupKind::Data { table_id, .. } = placement.group.kind else {
            return Err(unavailable());
        };
        if parsed.request_id != id
            || request.request_id != id
            || request.digest != digest
            || !record.reservation.binds(request)
        {
            return Err(QueueError::Conflict.protocol());
        }
        let scope = ScopedCatalogRead {
            queue_group_id: placement.group.group_id,
            data_generation: placement.data_generation,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request,
            table_id,
            read_ts: read.storage_pin.read_ts,
        };
        let pin = self
            .services
            .catalog
            .snapshot(scope.read_ts, ready.view.authority())?;
        let point = captured
            .as_ref()
            .and_then(|capture| capture.point_for(&pin, read.catalog_root));
        let txn = TxnId {
            request_id: id,
            attempt: 1,
        };
        let mutation = parsed.operation.is_mutation();
        let recovered = if mutation {
            self.transactions
                .recover_scoped(pin.authority, scope, txn)
                .await?
        } else {
            None
        };
        let decision = if terminal.is_some() {
            recovered
                .as_ref()
                .map(|decision| (txn, decision.statement().manifest))
        } else {
            let (encoded, decision) = match recovered {
                Some(decision) => {
                    #[cfg(test)]
                    super::recovery_tests::transaction_checkpoint(
                        "scoped-decision",
                        id,
                        decision.statement(),
                    )
                    .await;
                    let outcome = decision.outcome().ok_or_else(unavailable)?;
                    (
                        encode_outcome(id, outcome.result()),
                        Some((txn, decision.statement().manifest)),
                    )
                }
                None => match self
                    .plan(scope, parsed.operation, &pin, txn, owner, point)
                    .await
                {
                    Ok(PlannedOperation::Read(value)) => {
                        let bytes = self
                            .resources()
                            .execute(ExecutionClass::Query, 0, 0, move || {
                                let raw: &serde_json::value::RawValue =
                                    serde_json::from_slice(value.as_ref())
                                        .map_err(|_| unavailable())?;
                                Ok(response::encode_success(id, raw, false))
                            })
                            .await?;
                        (bytes, None)
                    }
                    Ok(PlannedOperation::Mutation(plan)) => {
                        let decision = self
                            .transactions
                            .commit_scoped(
                                pin.authority,
                                scope,
                                txn,
                                plan,
                                record.reservation,
                                read,
                                crate::oltp_replication::authority::timestamp::ScopedTimestampEpoch::from_placement(&placement)?,
                            )
                            .await?;
                        #[cfg(test)]
                        super::recovery_tests::transaction_checkpoint(
                            "scoped-decision",
                            id,
                            decision.statement(),
                        )
                        .await;
                        let outcome = decision.outcome().ok_or_else(unavailable)?;
                        (
                            encode_outcome(id, outcome.result()),
                            Some((txn, decision.statement().manifest)),
                        )
                    }
                    Err((error, guards)) if error.outcome == Outcome::NotApplied && mutation => {
                        let decision = self
                            .transactions
                            .fail_scoped(pin.authority, scope, txn, error, guards)
                            .await?;
                        let outcome = decision.outcome().ok_or_else(unavailable)?;
                        (
                            encode_outcome(id, outcome.result()),
                            Some((txn, decision.statement().manifest)),
                        )
                    }
                    Err((error, _)) if error.outcome == Outcome::NotApplied && !mutation => {
                        (response::encode_error(Some(id), error), None)
                    }
                    Err((error, _)) => return Err(error),
                },
            };
            let encoded = if !mutation && encoded.body.len() > self.services.max_result_bytes {
                response::encode_error(
                    Some(id),
                    ProtocolError::new(
                        ErrorCode::LimitExceeded,
                        "point result exceeds the configured scoped receipt limit",
                        Outcome::NotApplied,
                    ),
                )
            } else {
                encoded
            };
            self.encode_terminal(&mut frame, encoded)?;
            decision
        };
        let frame = terminal.as_ref().unwrap_or(&frame);
        self.services
            .endpoint
            .confirm_scoped_terminal(
                &self.services.peer,
                &placement,
                frame,
                decision,
                Instant::now() + COPY_TIMEOUT,
            )
            .await?;
        let record = self.services.endpoint.scoped_record(&placement, id).await?;
        self.release_terminal(&placement, &record).await
    }

    async fn release_terminal(
        &self,
        placement: &ScopedQueuePlacement,
        record: &ScopedSurvivor,
    ) -> Result<(), ProtocolError> {
        if record.terminal_quorum.is_none() {
            return Err(unavailable());
        }
        if record.pin_released() {
            return Ok(());
        }
        let statement = record
            .terminal_statement(placement)
            .map_err(QueueError::protocol)?
            .ok_or_else(unavailable)?;
        let release = self
            .services
            .timestamps
            .release_scoped_terminal(statement.clone())
            .await?;
        self.services
            .endpoint
            .note_scoped_pin_release(placement, &statement, &release)?;
        Ok(())
    }

    async fn plan(
        &self,
        scope: ScopedCatalogRead,
        operation: Operation,
        pin: &crate::oltp::mvcc::catalog::PinnedCatalog,
        txn: TxnId,
        owner: Arc<ScratchLease>,
        point: Option<&PointCatalog>,
    ) -> Result<PlannedOperation, (ProtocolError, Vec<VerifiedLifecycleGuard>)> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut guards = BTreeMap::new();
        loop {
            let mut error = match self
                .planner
                .plan_scoped(scope, operation.clone(), pin, txn, owner.clone(), point)
                .await
            {
                Ok(plan) => return Ok(plan),
                Err(error) => error,
            };
            while let PlanError::Guarded {
                cause,
                guards: held,
            } = error
            {
                for guard in held {
                    guards.insert(guard.binding().guard_id, guard);
                }
                error = *cause;
            }
            match error {
                PlanError::Blocked(intent) if Instant::now() < deadline => {
                    #[cfg(test)]
                    super::recovery_tests::transaction_checkpoint(
                        "blocked", txn.request_id, &intent,
                    ).await;
                    #[cfg(not(test))]
                    let _ = intent;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                PlanError::LifecyclePending(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                PlanError::Protocol(error) => return Err((error, guards.into_values().collect())),
                _ => return Err((unavailable(), guards.into_values().collect())),
            }
        }
    }

    fn encode_terminal(
        &self,
        frame: &mut RecordFrame,
        result: EncodedResponse,
    ) -> Result<(), ProtocolError> {
        let mut header = frame.decode_header().map_err(QueueError::protocol)?;
        let response = self.resources().copy_bytes(&result.body)?;
        header.accepted = true;
        header.version = header.version.checked_add(1).ok_or_else(unavailable)?;
        header.state = if result.status == 200 {
            RequestState::Succeeded
        } else {
            RequestState::Failed
        };
        header.response_status = Some(result.status);
        header.completed_at_ms = Some(now());
        header.section_lengths[2] = response.len() as u64;
        frame.header = self.resources().encode(&header, MAX_RECORD_HEADER_BYTES)?;
        frame.sections[2] = Some(response);
        Ok(())
    }

    pub(super) async fn observe(
        &self,
        ingress: ScopedIngress,
        run: Uuid,
        id: Uuid,
        expected_digest: Digest,
        inline: bool,
        expected: Option<&ApplicationObservation>,
    ) -> Result<ObservedApplication, ProtocolError> {
        let placement = ingress.placement;
        if run != placement.view.run_id {
            return Err(QueueError::Abandoned.protocol());
        }
        let Some(survivor) = self.existing(&placement, id).await? else {
            return self
                .durable_observation(&placement, id, expected_digest, inline, expected)
                .await;
        };
        if survivor.reservation.digest != expected_digest {
            return Err(QueueError::Conflict.protocol());
        }
        if survivor.acceptance.is_none() {
            return Err(unavailable());
        }
        let terminal_ready = match (survivor.terminal_quorum, survivor.reservation.kind) {
            (None, _) => false,
            (Some(_), RequestKind::Mutation) => true,
            (Some(_), RequestKind::Read) if survivor.pin_released() => true,
            (Some(_), RequestKind::Read) => {
                let terminal = survivor
                    .terminal_statement(&placement)
                    .map_err(QueueError::protocol)?
                    .ok_or_else(unavailable)?;
                self.services
                    .status
                    .scoped_terminal_receipt(&placement, id, Some(expected_digest))
                    .await?
                    .is_some_and(|receipt| receipt.marker.proof.statement() == &terminal)
            }
        };
        if !terminal_ready {
            if expected.is_some() {
                return Err(unavailable());
            }
            let state = if survivor.terminal_stamp().is_some() {
                RequestState::Committing
            } else if self
                .active
                .lock()
                .map_err(|_| unavailable())?
                .contains_key(&id)
            {
                RequestState::Running
            } else {
                RequestState::Reconciling
            };
            return Ok(ObservedApplication {
                metadata: ApplicationObservation {
                    record: observation_record(&placement, &survivor, None, state)?,
                    response_hash: None,
                    durable_proof: false,
                },
                body: None,
            });
        }
        let (_, frame) = survivor
            .frames(self.resources())
            .map_err(QueueError::protocol)?;
        let frame = frame.ok_or_else(unavailable)?;
        let header = frame.decode_header().map_err(QueueError::protocol)?;
        let completed = header.completed_at_ms.ok_or_else(unavailable)?;
        if now().saturating_sub(completed) as u128 >= self.retention.as_millis() {
            return self
                .durable_observation(&placement, id, expected_digest, inline, expected)
                .await;
        }
        let body = frame.sections[2].as_ref().ok_or_else(unavailable)?;
        let metadata = ApplicationObservation {
            record: observation_record(&placement, &survivor, Some(&header), header.state)?,
            response_hash: Some(digest(body.as_ref())),
            durable_proof: false,
        };
        if expected.is_some_and(|expected| {
            expected.record.run_id != run
                || expected.record.request_id != id
                || expected.record.digest != metadata.record.digest
                || expected.record.state != metadata.record.state
                || expected.record.response_status != metadata.record.response_status
                || expected.record.version > metadata.record.version
                || expected.response_hash != metadata.response_hash
        }) {
            return Err(unavailable());
        }
        let body = (expected.is_some() || inline && body.len() <= INLINE_RECEIPT_BYTES)
            .then(|| body.clone());
        Ok(ObservedApplication { metadata, body })
    }

    async fn durable_observation(
        &self,
        placement: &ScopedQueuePlacement,
        id: Uuid,
        expected_digest: Digest,
        inline: bool,
        expected: Option<&ApplicationObservation>,
    ) -> Result<ObservedApplication, ProtocolError> {
        let receipt = self
            .services
            .status
            .scoped_terminal_receipt(placement, id, Some(expected_digest))
            .await?
            .ok_or_else(unavailable)?;
        let statement = receipt.marker.proof.statement();
        let terminal = &receipt.terminal;
        let reservation = terminal_reservation(terminal);
        if reservation.request_id != id
            || reservation.digest != expected_digest
            || reservation.original_run != placement.view.run_id
        {
            return Err(QueueError::Conflict.protocol());
        }
        let response = super::worker_runtime::terminal_response(id, terminal)?;
        if response.status != statement.response_status
            || digest(&response.body) != statement.response_hash
        {
            return Err(unavailable());
        }
        let mut record =
            super::worker_runtime::terminal_record_with_status(reservation, response.status);
        record.scheduler_term = statement.epoch.view.owner_term;
        record.membership_revision = statement.epoch.view.membership_revision;
        record.version = statement.terminal.version;
        record.completed_at_ms = Some(statement.completed_at_ms);
        let metadata = ApplicationObservation {
            record,
            response_hash: Some(statement.response_hash),
            durable_proof: true,
        };
        super::worker_runtime::attach_result(metadata, expected, || {
            if expected.is_some() || inline && response.body.len() <= INLINE_RECEIPT_BYTES {
                Ok(Some(self.resources().copy_bytes(&response.body)?))
            } else {
                Ok(None)
            }
        })
    }

    pub(super) async fn result(
        &self,
        ingress: ScopedIngress,
        expected: &ApplicationObservation,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.observe(
            ingress,
            expected.record.run_id,
            expected.record.request_id,
            Digest::parse(&expected.record.digest).map_err(|_| unavailable())?,
            false,
            Some(expected),
        )
        .await?
        .body
        .ok_or_else(unavailable)
    }
}

fn encode_outcome(id: Uuid, outcome: Result<serde_json::Value, ProtocolError>) -> EncodedResponse {
    match outcome {
        Ok(value) => response::encode_success(id, &value, true),
        Err(error) => response::encode_error(Some(id), error),
    }
}

struct ActiveRequest {
    runtime: Arc<ScopedWorker>,
    id: Uuid,
}
impl Drop for ActiveRequest {
    fn drop(&mut self) {
        if let Ok(mut active) = self.runtime.active.lock() {
            active.remove(&self.id);
        }
    }
}

fn pin_identity(reservation: IdentityReservation) -> PinIdentity {
    PinIdentity {
        run_id: reservation.original_run,
        request_id: reservation.request_id,
        digest: reservation.digest,
        nonce: reservation.reservation_id,
    }
}

fn observation_record(
    placement: &ScopedQueuePlacement,
    record: &ScopedSurvivor,
    header: Option<&RecordHeader>,
    state: RequestState,
) -> Result<RequestRecord, ProtocolError> {
    let (_, payload) = record.stamp.payload_claim().map_err(QueueError::protocol)?;
    Ok(RequestRecord {
        context: None,
        run_id: placement.view.run_id,
        scheduler_term: placement.view.owner_term,
        membership_revision: placement.view.membership_revision,
        request_id: record.reservation.request_id,
        digest: record.reservation.digest.to_string(),
        sequence: record.sequence(),
        version: header.map_or(payload.version, |header| header.version),
        body: Vec::new(),
        accepted: record.acceptance.is_some(),
        state,
        pinned_catalog: None,
        response_status: header.and_then(|header| header.response_status),
        response_body: None,
        completed_at_ms: header.and_then(|header| header.completed_at_ms),
        result_expired: false,
        replicas: Default::default(),
    })
}

pub(super) fn terminal_reservation(
    terminal: &crate::oltp_replication::authority::status::TerminalIdentity,
) -> &IdentityReservation {
    use crate::oltp_replication::authority::status::TerminalIdentity::*;
    match terminal {
        ReadExpired { reservation, .. }
        | Failed { reservation, .. }
        | Committed { reservation, .. } => reservation,
    }
}

fn digest(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped OLTP outcome or owner unavailable",
        Outcome::Unknown,
    )
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |now| now.as_millis().try_into().unwrap_or(u64::MAX))
}
