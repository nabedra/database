use super::*;
use crate::oltp_replication::authority::proof::SealedTerminalProof;
use crate::oltp_replication::authority::timestamp::{
    DirectTimestampRange, DirectTimestampSource, IndexMetadataPin, MAX_DIRECT_COUNT, PointDecision,
    ScopedAdmissionClosure, ScopedCopyProof, ScopedPinRelease, ScopedTerminalStatement,
    ScopedTimestampEpoch, TimestampDecisionLocator, TimestampPointBinding, VerifiedArchiveClosure,
    archive::ArchiveClosureStatement,
    cold::{ColdPinCursor, ColdPinStep},
};
use crate::oltp_replication::authority::{
    control::{ScopedCatalogRead, ScopedQueuePlacement, ScopedQueueTransition},
    status::StatusTopology,
};
use crate::oltp_runtime::replica::OriginalAuthorization;
use tokio::sync::Semaphore;
use uuid::Uuid;

mod point_decision;
pub(crate) use point_decision::VerifiedTimestampPointDecision;

// The owner shares the four outgoing service mutation slots with foreground
// timestamp and Status operations. Cleanup can occupy at most two of them.
const RELEASE_CALLS: usize = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Request {
    ArchiveMetadata {
        closure: ArchiveClosureStatement,
        after: Option<Vec<u8>>,
    },
    RetainArchive {
        authority: Authority,
        statement: crate::oltp::backup::BackupPublishedStatement,
        previous_root: Option<Digest>,
    },
    CancelBackup {
        authority: Authority,
        statement: crate::oltp::backup::lifecycle::BackupCancelledStatement,
    },
    ArchiveCreate {
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
        pin: StoragePin,
        archive_id: Uuid,
        base_exclusive: Option<CommitTs>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<GroupIdentity>,
    },
    ArchiveLookup {
        statement: ArchiveClosureStatement,
    },
    Current,
    Cancel {
        rejection: SealedAdmissionRejection,
    },
    ColdPins {
        authority: Authority,
        transition_id: Uuid,
        after: Option<ColdPinCursor>,
    },
    CloseBackup {
        authority: Authority,
        statement: crate::oltp::backup::BackupPublishedStatement,
    },
    Begin {
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    },
    RetrySnapshot {
        view: AuthorityView,
        identity: PinIdentity,
        previous: crate::oltp_replication::authority::status::SealedSettlement,
    },
    Bind {
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    BeginScoped {
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        origin: OriginalAuthorization,
    },
    BindScoped {
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    CloseSupersededScopedAdmission {
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin: StoragePin,
    },
    ScopedAdmissionClosure {
        identity: PinIdentity,
        reservation: IdentityReservation,
    },
    ScopedSnapshot {
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
        reservation: IdentityReservation,
    },
    ScopedReadPin {
        scope: ScopedCatalogRead,
        kind: Option<RequestKind>,
        admission_read: bool,
    },
    ReleaseScoped {
        statement: ScopedTerminalStatement,
    },
    ConfirmedScopedRelease {
        statement: ScopedTerminalStatement,
    },
    ScopedCopyProof {
        statement: ScopedTerminalStatement,
    },
    FenceScopedQueue {
        group_id: Uuid,
        transition_id: Uuid,
    },
    ScopedQueueFenceReceipt {
        transition_id: Uuid,
        previous: ScopedTimestampEpoch,
        next: ScopedTimestampEpoch,
    },
    Allocate {
        permit: SealedCommitPermit,
    },
    VerifyTimestampPointPin(Box<TimestampPointBinding>),
    CommitTimestampPoint {
        locator: TimestampDecisionLocator,
        reconcile: bool,
    },
    AbortTimestampPoint {
        locator: TimestampDecisionLocator,
        reconcile: bool,
    },
    ObserveTimestampPoint {
        locator: TimestampDecisionLocator,
    },
    AbandonTimestampPoint {
        locator: TimestampDecisionLocator,
        transition_id: Uuid,
    },
    Allocation {
        permit: SealedCommitPermit,
    },
    Release {
        statement: TerminalCopyStatement,
    },
    Released {
        statement: TerminalCopyStatement,
    },
    Fence {
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    },
    Ready {
        transition_id: Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    },
    Maintenance {
        authority: Authority,
        owner: MaintenanceOwnerRef,
    },
    CloseMaintenance {
        authority: Authority,
        owner: MaintenanceOwnerRef,
    },
    IndexMetadata {
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    },
    IndexMetadataPin {
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        request: RequestBinding,
    },
    ImportMetadata {
        authority: Authority,
        archive_id: Uuid,
        target_id: Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    },
    ReleaseMetadata {
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        statement: SettlementStatement,
        import: bool,
    },
    Backup {
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    },
    /// Sent only after the caller's `data.direct.v1` view is active, so an
    /// older Timestamp leader never receives it in normal operation. An older
    /// leader rejects the unknown variant at decode.
    AllocateDirect {
        count: u64,
    },
    AdvanceFloor {
        authority: Authority,
        retired_at: Option<CommitTs>,
    },
}
impl Request {
    fn method(&self) -> Method {
        let operation = match self {
            Self::ArchiveLookup { .. } => 18,
            Self::ArchiveMetadata { .. } => 22,
            Self::RetainArchive { .. } => 20,
            Self::CancelBackup { .. } => 21,
            Self::ArchiveCreate { .. } => 19,
            Self::Current => 0,
            Self::Begin { .. } => 1,
            Self::Bind { .. } => 2,
            Self::Allocate { .. } => 3,
            Self::Allocation { .. } => 4,
            Self::Release { .. } => 5,
            Self::Released { .. } => 6,
            Self::Fence { .. } => 7,
            Self::Ready { .. } => 8,
            Self::Maintenance { .. } => 9,
            Self::CloseMaintenance { .. } => 10,
            Self::IndexMetadata { .. } => 11,
            Self::IndexMetadataPin { .. } => 23,
            Self::ColdPins { .. } => 24,
            Self::ScopedSnapshot { .. } => 25,
            Self::ScopedReadPin { .. } => 26,
            Self::ReleaseScoped { .. } => 27,
            Self::ScopedCopyProof { .. } => 28,
            Self::FenceScopedQueue { .. } => 29,
            Self::ScopedQueueFenceReceipt { .. } => 30,
            Self::ConfirmedScopedRelease { .. } => 31,
            Self::CommitTimestampPoint { .. } => 32,
            Self::ObserveTimestampPoint { .. } => 33,
            Self::AbandonTimestampPoint { .. } => 34,
            Self::AbortTimestampPoint { .. } => 35,
            Self::VerifyTimestampPointPin(_) => 36,
            Self::BeginScoped { .. } => 37,
            Self::BindScoped { .. } => 38,
            Self::CloseSupersededScopedAdmission { .. } => 39,
            Self::ScopedAdmissionClosure { .. } => 40,
            Self::ImportMetadata { .. } => 12,
            Self::ReleaseMetadata { .. } => 13,
            Self::Backup { .. } => 14,
            Self::Cancel { .. } => 15,
            Self::CloseBackup { .. } => 16,
            Self::AllocateDirect { .. } => 41,
            Self::AdvanceFloor { .. } => 42,
            Self::RetrySnapshot { .. } => 43,
        };
        Method::TimestampCall {
            operation,
            mutation: !matches!(
                self,
                Self::Current
                    | Self::Allocation { .. }
                    | Self::Released { .. }
                    | Self::ArchiveLookup { .. }
                    | Self::ArchiveMetadata { .. }
                    | Self::IndexMetadataPin { .. }
                    | Self::ScopedSnapshot { .. }
                    | Self::ScopedReadPin { .. }
                    | Self::ScopedCopyProof { .. }
                    | Self::ScopedQueueFenceReceipt { .. }
                    | Self::ConfirmedScopedRelease { .. }
                    | Self::ObserveTimestampPoint { .. }
                    | Self::VerifyTimestampPointPin(_)
                    | Self::ScopedAdmissionClosure { .. }
            ),
        }
    }
}
pub(crate) struct TimestampEndpoint {
    handle: Arc<TimestampHandle>,
    node: u64,
    control: Arc<ControlClient>,
    transport: WorkerClient,
    status: StatusTopology,
    backup: std::sync::OnceLock<Arc<dyn BackupPublicationReader>>,
}
impl TimestampEndpoint {
    pub(crate) fn new(
        handle: Arc<TimestampHandle>,
        node: u64,
        control: Arc<ControlClient>,
        transport: WorkerClient,
        status: StatusTopology,
    ) -> Result<Arc<Self>, ProtocolError> {
        if node == 0
            || status.groups()[0].cluster_id != handle.identity().cluster_id
            || status.groups()[0].database != handle.identity().database
        {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            handle,
            node,
            control,
            transport,
            status,
            backup: std::sync::OnceLock::new(),
        }))
    }
    pub(crate) fn install_backup_reader(
        &self,
        reader: Arc<dyn BackupPublicationReader>,
    ) -> Result<(), ProtocolError> {
        self.backup.set(reader).map_err(|_| invalid())
    }
    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        let head = parse(session, self.handle.identity(), self.node, bytes)?;
        let permitted = match head.method {
            Method::TimestampCall {
                operation: 7 | 8,
                mutation: true,
            } => matches!(
                head.action,
                ServiceAction::ControlFence { .. } | ServiceAction::Operator
            ),
            Method::TimestampCall {
                operation: 0 | 4 | 6 | 18,
                mutation: false,
            } => {
                head.action == ServiceAction::Owner
                    || matches!(head.action, ServiceAction::StatusProof { source_group } if self.status.groups().iter().any(|status| status.group_id == source_group))
            }
            Method::TimestampCall {
                operation: 25,
                mutation: false,
            } => {
                matches!(head.action, ServiceAction::ScopedQueueRead { .. })
                    && head.placement_generation.is_none()
            }
            Method::TimestampCall {
                operation: 26,
                mutation: false,
            } => matches!(
                head.action,
                ServiceAction::StatusProof { .. }
                    | ServiceAction::ParticipantProof { .. }
                    | ServiceAction::ScopedQueueExecute(_)
            ),
            Method::TimestampCall {
                operation: 1 | 2,
                mutation: true,
            } => head.action == ServiceAction::Owner,
            Method::TimestampCall {
                operation: 37 | 38 | 39,
                mutation: true,
            } => {
                head.placement_generation.is_none()
                    && matches!(head.action, ServiceAction::ScopedQueueReserve { .. })
            }
            Method::TimestampCall {
                operation: 40,
                mutation: false,
            } => {
                head.placement_generation.is_none()
                    && matches!(
                        head.action,
                        ServiceAction::StatusProof { .. } | ServiceAction::ScopedQueueRead { .. }
                    )
            }
            // A Data/Catalog replica (the range leader) or the queue owner.
            Method::TimestampCall {
                operation: 41,
                mutation: true,
            } => allocate_direct_permitted(&head.action),
            Method::TimestampCall {
                operation: 3,
                mutation: true,
            } => matches!(
                head.action,
                ServiceAction::Owner | ServiceAction::ScopedQueueExecute(_)
            ),
            Method::TimestampCall { operation: 27, mutation: true } => {
                matches!(head.action, ServiceAction::ScopedQueueReserve { .. })
            }
            Method::TimestampCall { operation: 28, mutation: false } => {
                matches!(head.action, ServiceAction::StatusProof { .. })
            }
            Method::TimestampCall { operation: 29, mutation: true } => {
                matches!(head.action, ServiceAction::ParticipantProof { .. })
            }
            Method::TimestampCall { operation: 30, mutation: false } => {
                matches!(head.action, ServiceAction::ScopedQueueFenceReceipt { .. })
            }
            Method::TimestampCall { operation: 31, mutation: false } => {
                matches!(head.action, ServiceAction::ScopedQueueRead { .. })
            }
            Method::TimestampCall { operation: 32 | 35, mutation: true } => {
                matches!(head.action, ServiceAction::ScopedQueueExecute(_))
            }
            Method::TimestampCall { operation: 33, mutation: false } => {
                matches!(head.action, ServiceAction::Owner | ServiceAction::ScopedQueueExecute(_))
            }
            Method::TimestampCall { operation: 36, mutation: false } => {
                matches!(head.action, ServiceAction::StatusProof { source_group }
                    if self.status.groups().iter().any(|status| status.group_id == source_group))
            }
            Method::TimestampPointDecisionRead { route, .. } => {
                head.placement_generation.is_none()
                    && matches!(
                        head.action,
                        ServiceAction::ParticipantProof { source_group }
                            | ServiceAction::StatusProof { source_group }
                            if source_group == route.source_group
                    )
            }
            Method::TimestampCall { .. } => head.action == ServiceAction::Owner,
            _ => false,
        };
        if !permitted {
            return Err(invalid());
        }
        Ok(head)
    }
    async fn status_client(&self, status: &GroupIdentity) -> Result<Arc<StatusClient>, ProtocolError> {
        if !self.status.contains(status)
            || registered_target(&self.control, status.group_id).await? != *status
        {
            return Err(invalid());
        }
        StatusClient::new(
            status.clone(),
            self.handle.resources().clone(),
            self.transport.clone(),
            self.control.clone(),
            ServiceAction::TimestampProof {
                source_group: self.handle.identity().group_id,
            },
        )
    }
    async fn reservation(&self, expected: IdentityReservation) -> Result<(), ProtocolError> {
        match self
            .status_client(self.status.request(expected.request_id))
            .await?
            .lookup_identity(expected.request_id, Some(expected.digest))
            .await?
        {
            Some(IdentityLookup::Reserved(actual)) if actual == expected => Ok(()),
            _ => Err(invalid()),
        }
    }
    async fn snapshot_epoch(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        view: AuthorityView,
        identity: &PinIdentity,
    ) -> Result<ScopedTimestampEpoch, ProtocolError> {
        let ServiceAction::ScopedQueueReserve {
            queue_group_id,
            queue_generation,
            queue_view,
            request_id,
        } = head.action
        else {
            return Err(invalid());
        };
        let (grant, placement) = tokio::try_join!(
            service_grant(&self.control, session, head),
            self.control.scoped_queue_by_id(queue_group_id),
        )?;
        let placement = placement.ok_or_else(stale_service_route)?;
        if grant.ready.view != view
            || grant.owner != Some(session.remote())
            || grant.effective_authority != view.authority()
            || placement.owner != session.remote()
            || placement.queue_generation != queue_generation
            || placement.view != queue_view
            || request_id != identity.request_id
            || identity.run_id != queue_view.run_id
            || queue_view.run_id != view.run_id
        {
            return Err(stale_service_route());
        }
        ScopedTimestampEpoch::from_placement(&placement)
    }

    async fn scoped_placement(
        &self,
        statement: &ScopedTerminalStatement,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let placement = self.control.scoped_queue_by_id(statement.epoch.queue_group_id)
            .await?
            .ok_or_else(unavailable)?;
        statement.for_placement(&placement)?;
        Ok(placement)
    }

    async fn pending_scoped_transfer(
        &self,
        group_id: Uuid,
        transition_id: Uuid,
    ) -> Result<ScopedQueueTransition, ProtocolError> {
        let transfer = self.control.scoped_queue_transition_for_replica(group_id, self.handle.identity().group_id).await?
            .filter(|transfer| transfer.id == transition_id)
            .ok_or_else(unavailable)?;
        if transfer.basis()?.is_none() {
            return Err(invalid());
        }
        Ok(transfer)
    }

    /// `AllocateDirect` without native-executor hops (PERF2a): the request
    /// and the range are a few bytes of fixed-shape JSON, decoded and
    /// encoded on this task under the same charges as [`decode`] and
    /// [`encode`]. The handle still passes its lease barrier first.
    async fn serve_allocate_direct(
        &self,
        sections: &ChargedSections,
    ) -> Result<ChargedBytes, ProtocolError> {
        let shared = self.handle.resources();
        let raw = sections.section(0).ok_or_else(invalid)?.as_slice();
        let budget = crate::oltp::mvcc::json_decode_budget(raw)?;
        let owner = shared
            .reserve_scratch_wait(ByteClass::Progress, budget, 0)
            .await?;
        let request: Request = serde_json::from_slice(raw).map_err(|_| invalid())?;
        drop(owner);
        let Request::AllocateDirect { count } = request else {
            return Err(invalid());
        };
        let range = self.handle.allocate_direct(count).await?;
        shared.encode_in(ByteClass::Progress, &range, LIMIT)
    }
    async fn dispatch(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        request: Request,
    ) -> Result<ChargedBytes, ProtocolError> {
        let shared = self.handle.resources();
        match request {
            Request::ArchiveMetadata { closure, after } => {
                let proof = self.handle.archive_lookup(&closure).await?;
                encode(
                    shared,
                    self.handle.native_archive_page(&proof, after).await?,
                )
                .await
            }
            Request::RetainArchive {
                authority,
                statement,
                previous_root,
            } => {
                let proof = self
                    .backup
                    .get()
                    .ok_or_else(unavailable)?
                    .verify(statement)
                    .await?;
                self.handle
                    .retain_archive(authority, proof, previous_root)
                    .await?;
                encode(shared, ()).await
            }
            Request::CancelBackup {
                authority,
                statement,
            } => {
                let proof = self
                    .backup
                    .get()
                    .ok_or_else(unavailable)?
                    .verify_cancelled(statement)
                    .await?;
                self.handle.cancel_backup(authority, proof).await?;
                encode(shared, ()).await
            }
            Request::ArchiveCreate {
                authority,
                owner,
                pin,
                archive_id,
                base_exclusive,
                status,
            } => {
                let group = match status.as_ref() {
                    Some(group) => group,
                    None => &self.status.groups()[0],
                };
                let status: Arc<dyn StatusService> = self.status_client(group).await?;
                let closure = self
                    .handle
                    .archive_closure(authority, owner, pin, archive_id, base_exclusive, status)
                    .await?;
                encode(shared, closure.statement().clone()).await
            }
            Request::ArchiveLookup { statement } => {
                if !self.status.contains(&statement.status)
                    || matches!(head.action, ServiceAction::StatusProof { source_group } if source_group != statement.status.group_id)
                {
                    return Err(invalid());
                }
                encode(
                    shared,
                    self.handle
                        .archive_lookup(&statement)
                        .await?
                        .statement()
                        .clone(),
                )
                .await
            }
            Request::Cancel { rejection } => {
                rejection.validate()?;
                let group = self.status.request(rejection.rejection().reservation.request_id);
                if rejection.status_group() != group {
                    return Err(invalid());
                }
                let actual = self
                    .status_client(group)
                    .await?
                    .rejected_identity(rejection.rejection())
                    .await?;
                self.handle.cancel_unaccepted(actual).await?;
                encode(shared, ()).await
            }
            Request::CloseBackup {
                authority,
                statement,
            } => {
                let actual = self
                    .backup
                    .get()
                    .ok_or_else(unavailable)?
                    .verify(statement)
                    .await?;
                if serialized_digest(&actual.statement())? != serialized_digest(&statement)? {
                    return Err(invalid());
                }
                self.handle.release_backup(authority, actual).await?;
                encode(shared, ()).await
            }
            Request::Current => encode(shared, self.handle.current_authority().await?).await,
            Request::ScopedSnapshot {
                identity,
                pin,
                request,
                reservation,
            } => {
                let ServiceAction::ScopedQueueRead {
                    queue_group_id,
                    queue_generation,
                    queue_view,
                    request_id,
                } = head.action
                else {
                    return Err(invalid());
                };
                if request.request_id != request_id
                    || request.run_id != queue_view.run_id
                    || reservation.scoped
                        != Some(ScopedAdmission {
                            protocol_version: ScopedAdmission::VERSION,
                            queue_group_id,
                            queue_generation,
                            status_group_id: self.status.request(request_id).group_id,
                        })
                {
                    return Err(invalid());
                }
                self.handle
                    .verify_scoped_snapshot(identity, pin, request, reservation)
                    .await?;
                authorize(&self.control, session, head).await?;
                encode(shared, ()).await
            }
            Request::ColdPins {
                authority,
                transition_id,
                after,
            } => {
                let step = self
                    .handle
                    .sweep_cold_application_pin(authority, transition_id, after, |pin| async move {
                        if self
                            .control
                            .published_cold_fence(authority, pin.identity.run_id)
                            .await?
                            != Some(transition_id)
                        {
                            return Err(invalid());
                        }
                        self.status_client(self.status.request(pin.reservation.request_id))
                            .await?
                            .cold_application_pin_settled(authority, pin)
                            .await
                    })
                    .await?;
                encode(shared, step).await
            }
            Request::ScopedReadPin {
                scope,
                kind,
                admission_read,
            } => {
                let home = self.status.request(scope.request.request_id);
                match head.action {
                    ServiceAction::StatusProof { source_group }
                        if source_group == home.group_id && !admission_read => {}
                    ServiceAction::ParticipantProof { source_group } => {
                        let source = registered_target(&self.control, source_group).await?;
                        if admission_read
                            && (source.kind != GroupKind::Catalog
                                || kind.is_some()
                                || !scoped_admission_read(scope))
                        {
                            return Err(invalid());
                        }
                        if source.kind != GroupKind::Catalog
                            && (source.group_id != scope.queue_group_id
                                || !matches!(source.kind, GroupKind::Data { table_id, .. }
                                    if table_id == scope.table_id))
                        {
                            return Err(invalid());
                        }
                    }
                    ServiceAction::ScopedQueueExecute(expected)
                        if expected == scope && !admission_read => {}
                    _ => return Err(invalid()),
                }
                self.handle
                    .verify_scoped_read_pin(scope, home.group_id, kind, admission_read)
                    .await?;
                encode(shared, ()).await
            }
            Request::Begin {
                view,
                identity,
                reservation,
            } => {
                if reservation.scoped.is_some() {
                    return Err(invalid());
                }
                self.reservation(reservation).await?;
                encode(
                    shared,
                    self.handle
                        .begin_snapshot(view, identity, reservation)
                        .await?,
                )
                .await
            }
            Request::RetrySnapshot {
                view,
                identity,
                previous,
            } => {
                let statement = previous.statement();
                let group = self.status.manifest(statement.txn, statement.manifest)?;
                let actual = self
                    .status_client(group)
                    .await?
                    .settled_proof(statement.txn, statement.manifest)
                    .await?;
                if actual.statement() != statement {
                    return Err(invalid());
                }
                encode(
                    shared,
                    self.handle.retry_snapshot(view, identity, actual).await?,
                )
                .await
            }
            Request::Bind {
                view,
                identity,
                pin,
                request,
            } => {
                // Application, index and import use this unscoped bind.
                self.handle
                    .verify_unscoped_bind_pin(identity.clone(), pin)
                    .await?;
                self.handle
                    .bind_snapshot_context(view, identity, pin, request)
                    .await?;
                encode(shared, ()).await
            }
            Request::BeginScoped {
                view,
                identity,
                reservation,
                origin,
            } => {
                let epoch = self.snapshot_epoch(session, head, view, &identity).await?;
                if reservation.scoped != Some(ScopedAdmission {
                    protocol_version: ScopedAdmission::VERSION,
                    queue_group_id: epoch.queue_group_id,
                    queue_generation: epoch.queue_generation,
                    status_group_id: self.status.request(identity.request_id).group_id,
                }) {
                    return Err(invalid());
                }
                self.reservation(reservation).await?;
                encode(
                    shared,
                    self.handle
                        .begin_scoped_snapshot_with_origin(view, identity, reservation, epoch, origin)
                        .await?,
                )
                .await
            }
            Request::BindScoped {
                view,
                identity,
                pin,
                request,
            } => {
                let epoch = self.snapshot_epoch(session, head, view, &identity).await?;
                self.handle
                    .bind_scoped_snapshot_context(view, identity, pin, request, epoch)
                    .await?;
                encode(shared, ()).await
            }
            Request::CloseSupersededScopedAdmission {
                view,
                identity,
                reservation,
                pin,
            } => {
                let epoch = self.snapshot_epoch(session, head, view, &identity).await?;
                encode(
                    shared,
                    self.handle
                        .close_superseded_scoped_admission(view, identity, reservation, pin, epoch)
                        .await?,
                )
                .await
            }
            Request::ScopedAdmissionClosure {
                identity,
                reservation,
            } => {
                let scoped = reservation.scoped.ok_or_else(invalid)?;
                if identity.run_id != reservation.original_run
                    || identity.request_id != reservation.request_id
                    || identity.digest != reservation.digest
                    || identity.nonce != reservation.reservation_id
                    || scoped.protocol_version != ScopedAdmission::VERSION
                    || scoped.status_group_id != self.status.request(identity.request_id).group_id
                {
                    return Err(invalid());
                }
                match head.action {
                    ServiceAction::StatusProof { source_group }
                        if source_group == scoped.status_group_id => {}
                    ServiceAction::ScopedQueueRead {
                        queue_group_id,
                        queue_generation,
                        queue_view,
                        request_id,
                    } if queue_group_id == scoped.queue_group_id
                        && queue_generation == scoped.queue_generation
                        && queue_view.run_id == identity.run_id
                        && request_id == identity.request_id => {}
                    _ => return Err(invalid()),
                }
                let closure = self
                    .handle
                    .scoped_admission_closure(identity, reservation)
                    .await?;
                authorize(&self.control, session, head).await?;
                Ok(closure)
            }
            Request::VerifyTimestampPointPin(binding) => {
                self.verify_point_pin(session, head, *binding).await
            }
            Request::CommitTimestampPoint { .. } | Request::AbortTimestampPoint { .. } => Err(invalid()),
            Request::ObserveTimestampPoint { locator } => {
                self.observe_point(session, head, locator).await
            }
            Request::AbandonTimestampPoint { locator, transition_id } => {
                self.abandon_point(session, head, &locator, transition_id).await
            }
            Request::Allocate { permit } => {
                let statement = permit.statement();
                let group = self.status.manifest(statement.txn, statement.manifest)?;
                if let ServiceAction::ScopedQueueExecute(scope) = head.action {
                    let grant = service_grant(&self.control, session, head).await?;
                    if grant.owner != Some(session.remote())
                        || grant.effective_authority != statement.authority
                        || scope.request != statement.request
                        || scope.read_ts != statement.read_ts
                    {
                        return Err(invalid());
                    }
                    self.handle
                        .verify_scoped_read_pin(
                            scope,
                            group.group_id,
                            Some(RequestKind::Mutation),
                            false,
                        )
                        .await?;
                }
                let actual = self
                    .status_client(group)
                    .await?
                    .commit_permit(statement.authority, statement.txn)
                    .await?;
                if actual != permit {
                    return Err(invalid());
                }
                encode(shared, self.handle.allocate_commit(actual).await?).await
            }
            Request::AllocateDirect { count } => {
                encode(shared, self.handle.allocate_direct(count).await?).await
            }
            Request::AdvanceFloor {
                authority,
                retired_at,
            } => {
                encode(
                    shared,
                    self.handle.advance_floor(authority, retired_at).await?,
                )
                .await
            }
            Request::Allocation { permit } => {
                let statement = permit.statement();
                let group = self.status.manifest(statement.txn, statement.manifest)?;
                if matches!(head.action, ServiceAction::StatusProof { source_group } if source_group != group.group_id) {
                    return Err(invalid());
                }
                encode(shared, self.handle.confirmed_allocation(permit).await?).await
            }
            Request::ReleaseScoped { statement } => {
                if head.action != scoped_retirement_owner(&statement) {
                    return Err(invalid());
                }
                let placement = self.scoped_placement(&statement).await?;
                if placement.owner != session.remote() {
                    return Err(invalid());
                }
                if let Some(release) = self.handle.confirmed_scoped_release(statement.clone()).await? {
                    return encode(shared, release).await;
                }
                let proof = self.handle.witness_scoped_terminal(&placement, statement.clone()).await?;
                let status = self.status_client(self.status.request(statement.request.request_id)).await?;
                let marker = status.record_scoped_retirement(statement.clone()).await?;
                if marker.proof != proof || self.scoped_placement(&statement).await? != placement {
                    return Err(unavailable());
                }
                encode(shared, self.handle.complete_scoped_release(marker).await?).await
            }
            Request::ConfirmedScopedRelease { statement } => {
                if head.action != scoped_retirement_holder(&statement) {
                    return Err(invalid());
                }
                let placement = self.scoped_placement(&statement).await?;
                if !placement.has_holder(session.remote()) {
                    return Err(invalid());
                }
                encode(shared, self.handle.confirmed_scoped_release(statement).await?).await
            }
            Request::ScopedCopyProof { statement } => {
                let status = self.status.request(statement.request.request_id);
                if head.action != (ServiceAction::StatusProof { source_group: status.group_id }) {
                    return Err(invalid());
                }
                self.scoped_placement(&statement).await?;
                let proof = self.handle.scoped_copy_proof(statement).await?.ok_or_else(unavailable)?;
                encode(shared, proof).await
            }
            Request::FenceScopedQueue { group_id, transition_id } => {
                if head.action != (ServiceAction::ParticipantProof { source_group: group_id }) {
                    return Err(invalid());
                }
                let transfer = self.pending_scoped_transfer(group_id, transition_id).await?;
                if transfer.next.owner != session.remote()
                    || !has_installed_scoped_quorum(&transfer)
                {
                    return Err(invalid());
                }
                encode(shared, self.handle.fence_scoped_queue(
                    transition_id,
                    ScopedTimestampEpoch::from_placement(&transfer.previous)?,
                    ScopedTimestampEpoch::from_placement(&transfer.next)?,
                ).await?).await
            }
            Request::ScopedQueueFenceReceipt { transition_id, previous, next } => {
                if next.queue_group_id != previous.queue_group_id
                    || head.action != (ServiceAction::ScopedQueueFenceReceipt {
                        queue_group_id: next.queue_group_id,
                        transfer_id: transition_id,
                    })
                {
                    return Err(invalid());
                }
                encode(shared, self.handle.scoped_queue_fence_receipt(transition_id, previous, next).await?).await
            }
            Request::Release { statement } => {
                let release = self.handle.release(statement).await?;
                #[cfg(test)]
                crate::oltp_runtime::recovery_tests::transaction_checkpoint(
                    "terminal-pin-release",
                    release.statement().request.request_id,
                    &release.wire_parts(),
                )
                .await;
                encode(shared, release.wire_parts()).await
            }
            Request::Released { statement } => {
                encode(
                    shared,
                    self.handle.confirmed_release(statement).await?.wire_parts(),
                )
                .await
            }
            Request::Fence {
                transition_id,
                expected,
                next,
                ready,
            } => {
                let scope = crate::oltp_runtime::replica::QueueScope {
                    cluster_id: self.handle.identity().cluster_id,
                    database: self.handle.identity().database.clone(),
                };
                let grant = self
                    .control
                    .service_role(
                        &scope,
                        session.remote(),
                        head.action,
                        self.handle.identity().clone(),
                    )
                    .await?;
                match head.action {
                    ServiceAction::ControlFence {
                        transition_id: actual,
                    } if actual == transition_id
                        && grant.effective_authority == next
                        && grant.pending_ready == Some(ready) => {}
                    ServiceAction::Operator
                        if expected.is_none() && grant.effective_authority == next => {}
                    _ => return Err(invalid()),
                }
                encode(
                    shared,
                    self.handle
                        .fence(transition_id, expected, next, ready)
                        .await?,
                )
                .await
            }
            Request::Ready {
                transition_id,
                expected,
                next,
            } => {
                tracing::debug!(group = %self.handle.identity().group_id, %transition_id, stage = "ready-control-start", "timestamp ready transition");
                let scope = crate::oltp_runtime::replica::QueueScope {
                    cluster_id: self.handle.identity().cluster_id,
                    database: self.handle.identity().database.clone(),
                };
                let grant = self
                    .control
                    .service_role(
                        &scope,
                        session.remote(),
                        head.action,
                        self.handle.identity().clone(),
                    )
                    .await?;
                tracing::debug!(group = %self.handle.identity().group_id, %transition_id, stage = "ready-control-complete", "timestamp ready transition");
                if !matches!(head.action, ServiceAction::ControlFence { transition_id: actual } if actual == transition_id)
                    || grant.pending_ready != Some(next)
                {
                    return Err(invalid());
                }
                tracing::debug!(group = %self.handle.identity().group_id, %transition_id, stage = "ready-raft-start", "timestamp ready transition");
                let receipt = self
                    .handle
                    .fence_ready(transition_id, expected, next)
                    .await?;
                tracing::debug!(group = %self.handle.identity().group_id, %transition_id, stage = "ready-raft-complete", "timestamp ready transition");
                encode(shared, receipt).await
            }
            Request::Maintenance { authority, owner } => {
                encode(
                    shared,
                    self.handle.begin_maintenance(authority, owner).await?,
                )
                .await
            }
            Request::CloseMaintenance { authority, owner } => {
                let catalog = registered_target(&self.control, owner.catalog_group_id).await?;
                let proof = ParticipantProofClient::new(
                    catalog,
                    shared.clone(),
                    self.transport.clone(),
                    self.control.clone(),
                    ServiceAction::TimestampProof {
                        source_group: self.handle.identity().group_id,
                    },
                )?
                .index_closed(owner)
                .await?;
                self.handle.release_maintenance(authority, proof).await?;
                encode(shared, ()).await
            }
            Request::IndexMetadata {
                authority,
                owner,
                txn,
                reservation,
            } => {
                self.reservation(reservation).await?;
                encode(
                    shared,
                    self.handle
                        .begin_index_metadata(authority, owner, txn, reservation)
                        .await?,
                )
                .await
            }
            Request::IndexMetadataPin { authority, owner, txn, reservation, request } => {
                let catalog = registered_target(&self.control, owner.catalog_group_id).await?;
                if catalog.kind != GroupKind::Catalog
                    || catalog.cluster_id != self.handle.identity().cluster_id
                    || catalog.database != self.handle.identity().database
                {
                    return Err(invalid());
                }
                let terminal = self.status_client(self.status.request(txn.request_id)).await?
                    .lookup_identity(txn.request_id, Some(request.digest)).await?;
                if !matches!(terminal, Some(IdentityLookup::Terminal(TerminalIdentity::Committed {
                    reservation: actual, decision, ..
                })) if actual == reservation && decision.txn == txn && decision.request == request
                    && matches!(decision.decision, TxnDecision::Commit { .. }))
                {
                    return Err(invalid());
                }
                encode(shared, self.handle.index_metadata_pin(authority, owner, txn, reservation, request).await?).await
            }
            Request::ImportMetadata {
                authority,
                archive_id,
                target_id,
                txn,
                reservation,
            } => {
                self.reservation(reservation).await?;
                encode(
                    shared,
                    self.handle
                        .begin_import_metadata(authority, archive_id, target_id, txn, reservation)
                        .await?,
                )
                .await
            }
            Request::ReleaseMetadata {
                authority,
                identity,
                pin,
                statement,
                import,
            } => {
                let group = self.status.manifest(statement.txn, statement.manifest)?;
                if registered_target(&self.control, group.group_id).await? != *group {
                    return Err(invalid());
                }
                let actual = StatusProofClient::new(
                    group.clone(),
                    shared.clone(),
                    self.transport.clone(),
                    self.control.clone(),
                    ServiceAction::TimestampProof {
                        source_group: self.handle.identity().group_id,
                    },
                )?
                .settlement(statement.txn, statement.manifest)
                .await?;
                if actual.clone().into_sealed().statement() != &statement {
                    return Err(invalid());
                }
                if import {
                    self.handle
                        .release_import_metadata(authority, identity, pin, actual)
                        .await?;
                } else {
                    self.handle
                        .release_index_metadata(authority, identity, pin, actual)
                        .await?;
                }
                encode(shared, ()).await
            }
            Request::Backup { authority, owner } => {
                encode(shared, self.handle.begin_backup(authority, owner).await?).await
            }
        }
    }
}
impl Endpoint for TimestampEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let head = self.parse(session, bytes)?;
            if let Method::TimestampPointDecisionRead { route, source_digest, .. } = head.method {
                tokio::time::timeout(
                    SERVICE_CONVERGENCE,
                    self.control.verify_timestamp_point_decision_route(
                        session, self.node, route, head.action, source_digest,
                    ),
                )
                .await
                .map_err(|_| unavailable())??;
                return Ok(ReceivePlan {
                    class: head.method.class(),
                    section_lengths: [head.payload_bytes, 0, 0],
                    decode_budget_bytes: point_decision::LOCATOR_DECODE_BYTES,
                });
            }
            authorize(&self.control, session, &head).await?;
            Ok(ReceivePlan {
                class: head.method.class(),
                section_lengths: [head.payload_bytes, 0, 0],
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
            let _timer = crate::oltp_runtime::stage_timing::perf_stage!("ts.serve.total").timer();
            let head = self.parse(&session, bytes.as_slice())?;
            if let Method::TimestampPointDecisionRead { route, source_digest, locator_digest } = head.method {
                let result = self.read_point_decision(
                    sections, head.action, route, source_digest, locator_digest,
                ).await;
                return reply(self.handle.resources(), &session, self.node, result).await;
            }
            if head.method.inline_small_reply()
                && sections
                    .section(0)
                    .is_some_and(|section| section.len() <= INLINE_REPLY_BYTES)
            {
                let result = Box::pin(self.serve_allocate_direct(&sections)).await;
                drop(sections);
                return reply_inline(self.handle.resources(), &session, self.node, result);
            }
            let result = async {
                let (request, owner) = decode::<Request>(self.handle.resources(), sections).await?;
                if request.method() != head.method {
                    return Err(invalid());
                }
                // Keep the decision's nested quorum reads off the metadata
                // dispatcher's stack. Both paths retain the decode charge.
                let result = match request {
                    Request::CommitTimestampPoint { locator, reconcile } => {
                        Box::pin(self.decide_point(
                            &session,
                            &head,
                            &locator,
                            point_decision::PointOperation::Commit,
                            reconcile,
                        ))
                        .await
                    }
                    Request::AbortTimestampPoint { locator, reconcile } => {
                        Box::pin(self.decide_point(
                            &session,
                            &head,
                            &locator,
                            point_decision::PointOperation::Abort,
                            reconcile,
                        ))
                        .await
                    }
                    request => Box::pin(self.dispatch(&session, &head, request)).await,
                };
                drop(owner);
                result
            }
            .await;
            reply(self.handle.resources(), &session, self.node, result).await
        })
    }
}
fn scoped_retirement_owner(statement: &ScopedTerminalStatement) -> ServiceAction {
    ServiceAction::ScopedQueueReserve {
        queue_group_id: statement.epoch.queue_group_id,
        queue_generation: statement.epoch.queue_generation,
        queue_view: statement.epoch.view,
        request_id: statement.request.request_id,
    }
}

fn scoped_retirement_holder(statement: &ScopedTerminalStatement) -> ServiceAction {
    ServiceAction::ScopedQueueRead {
        queue_group_id: statement.epoch.queue_group_id,
        queue_generation: statement.epoch.queue_generation,
        queue_view: statement.epoch.view,
        request_id: statement.request.request_id,
    }
}

fn has_installed_scoped_quorum(transfer: &ScopedQueueTransition) -> bool {
    let Some(index) = transfer.next.members.iter().position(|member| *member == transfer.next.owner)
    else {
        return false;
    };
    transfer.installed[index].is_some_and(|owner_report| {
        transfer.installed.iter().zip(transfer.next.members).filter(|(report, worker)| {
            transfer.next.has_holder(*worker) && **report == Some(owner_report)
        }).count() >= 2
    })
}

fn verify_scoped_fence_reply(
    transfer: &ScopedQueueTransition,
    reply: &TimestampReply,
) -> Result<(), ProtocolError> {
    let epoch = ScopedTimestampEpoch::from_placement(&transfer.next)?;
    match reply {
        TimestampReply::ScopedQueueFenced { transition_id, epoch: stored, applied_at }
            if *transition_id == transfer.id && *stored == epoch
                && applied_at.term != 0 && applied_at.index != 0 => Ok(()),
        _ => Err(unavailable()),
    }
}

#[derive(Clone)]
pub(crate) struct TimestampClient {
    client: Arc<Client>,
    releases: Arc<Semaphore>,
}
impl TimestampClient {
    pub(crate) async fn cold_pin_step(
        &self,
        authority: Authority,
        transition_id: Uuid,
        after: Option<ColdPinCursor>,
    ) -> Result<ColdPinStep, ProtocolError> {
        let step: ColdPinStep = self
            .value(Request::ColdPins {
                authority,
                transition_id,
                after,
            })
            .await?;
        if step.after < after || !step.done && step.after == after {
            return Err(invalid());
        }
        Ok(step)
    }

    pub(crate) async fn archive_lookup(
        &self,
        statement: ArchiveClosureStatement,
    ) -> Result<VerifiedArchiveClosure, ProtocolError> {
        let response = self
            .response(Request::ArchiveLookup {
                statement: statement.clone(),
            })
            .await?;
        let context = response.metadata_context()?;
        let (actual, _): (ArchiveClosureStatement, _) =
            response.decoded(&self.client.shared).await?;
        VerifiedArchiveClosure::from_authenticated(context, actual, &statement)
    }
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Arc<Self>, ProtocolError> {
        if group.kind != GroupKind::Timestamps {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            client: Arc::new(Client::new(group, shared, transport, control, action)?),
            releases: Arc::new(Semaphore::new(RELEASE_CALLS)),
        }))
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        &self.client.group
    }
    pub(crate) fn resources(&self) -> &Arc<WorkerSharedResources> {
        &self.client.shared
    }
    async fn response(&self, request: Request) -> Result<ServiceResponse, ProtocolError> {
        // These reads also arrive from Status or participant proof clients.
        // A point decision write can repeat only in reconciliation mode.
        // Fences return the receipt stored for the same transition and command.
        // AllocateDirect repeats safely (values above every completed
        // allocation), so a stalled leader gets a per-replica deadline and
        // the call moves on to the new leader (D05/LIVE1).
        // AllocateDirect is answered only by the Timestamp leader after its
        // read barrier, whichever member the route names: reuse a recently
        // confirmed member list instead of reading Control per call (PERF1).
        if let Request::AllocateDirect { .. } = request {
            // Boxed: every Timestamp call awaits `response`, so this branch
            // must not grow its future (debug-build stack depth).
            return Box::pin(self.allocate_direct_response(request)).await;
        }
        if matches!(
            request,
            Request::AllocateDirect { .. }
                | Request::ScopedReadPin { .. }
                | Request::ScopedCopyProof { .. }
                | Request::VerifyTimestampPointPin(_)
                | Request::CommitTimestampPoint { reconcile: true, .. }
                | Request::AbortTimestampPoint { reconcile: true, .. }
                | Request::Fence { .. }
                | Request::Ready { .. }
        ) {
            let method = request.method();
            let payload = encode(&self.client.shared, request).await?;
            return self.client.repeat_as(self.client.action, method, Some(payload)).await;
        }
        // Snapshot transitions reuse the reservation nonce and exact context.
        // Allocate returns the existing stamp for the same transaction permit.
        let converge = matches!(
            request,
            Request::Current
                | Request::Begin { .. }
                | Request::RetrySnapshot { .. }
                | Request::Bind { .. }
                | Request::Allocate { .. }
                | Request::Allocation { .. }
                | Request::Released { .. }
        );
        self.client.named(request.method(), request, converge).await
    }
    async fn allocate_direct_response(
        &self,
        request: Request,
    ) -> Result<ServiceResponse, ProtocolError> {
        use crate::oltp_runtime::stage_timing::{perf_stage, timed};
        let method = request.method();
        // PERF2a: the request is a few bytes of fixed-shape JSON (the same
        // bytes as before, so every peer decodes it): encode it on this task.
        let payload = {
            let _timer = perf_stage!("ts.rpc.encode").timer();
            self.client
                .shared
                .encode_in(ByteClass::Progress, &request, LIMIT)?
        };
        timed(
            perf_stage!("ts.rpc.converge"),
            self.client
                .allocate_direct_as(self.client.action, method, payload),
        )
        .await
    }
    /// `AllocateDirect` routing counters (PERF2a, tests).
    #[cfg(test)]
    pub(crate) fn direct_routing_stats(
        &self,
    ) -> crate::oltp_replication::service_rpc::DirectRoutingStats {
        self.client.direct_routing_stats()
    }
    async fn value<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        request: Request,
    ) -> Result<T, ProtocolError> {
        Ok(self
            .response(request)
            .await?
            .decoded(&self.client.shared)
            .await?
            .0)
    }
    pub(crate) async fn current_authority(&self) -> Result<Option<Authority>, ProtocolError> {
        self.value(Request::Current).await
    }
    pub(crate) async fn begin_snapshot(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        self.value(Request::Begin {
            view,
            identity,
            reservation,
        })
        .await
    }
    pub(crate) async fn bind_snapshot_context(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> Result<(), ProtocolError> {
        self.value(Request::Bind {
            view,
            identity,
            pin,
            request,
        })
        .await
    }
    async fn scoped_snapshot_value<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        placement: &ScopedQueuePlacement,
        request_id: Uuid,
        request: Request,
    ) -> Result<T, ProtocolError> {
        let action = ServiceAction::ScopedQueueReserve {
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request_id,
        };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        // Each transition reuses the original reservation nonce and pin identity.
        Ok(self
            .client
            .repeat_as(action, method, Some(payload))
            .await?
            .decoded(&self.client.shared)
            .await
            .map_err(mutation_uncertain)?
            .0)
    }

    pub(crate) async fn begin_scoped_snapshot_with_origin(
        &self,
        placement: &ScopedQueuePlacement,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        origin: OriginalAuthorization,
    ) -> Result<(StoragePin, OriginalAuthorization), ProtocolError> {
        self.scoped_snapshot_value(
            placement,
            identity.request_id,
            Request::BeginScoped {
                view,
                identity,
                reservation,
                origin,
            },
        )
        .await
    }

    pub(crate) async fn bind_scoped_snapshot_context(
        &self,
        placement: &ScopedQueuePlacement,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> Result<(), ProtocolError> {
        self.scoped_snapshot_value(
            placement,
            identity.request_id,
            Request::BindScoped {
                view,
                identity,
                pin,
                request,
            },
        )
        .await
    }

    pub(crate) async fn close_superseded_scoped_admission(
        &self,
        placement: &ScopedQueuePlacement,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin: StoragePin,
    ) -> Result<ScopedAdmissionClosure, ProtocolError> {
        self.scoped_snapshot_value(
            placement,
            identity.request_id,
            Request::CloseSupersededScopedAdmission {
                view,
                identity,
                reservation,
                pin,
            },
        )
        .await
    }

    pub(crate) async fn lookup_scoped_admission_closure(
        &self,
        placement: &ScopedQueuePlacement,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> Result<Option<ScopedAdmissionClosure>, ProtocolError> {
        let action = ServiceAction::ScopedQueueRead {
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request_id: identity.request_id,
        };
        let request = Request::ScopedAdmissionClosure {
            identity,
            reservation,
        };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        Ok(self
            .client
            .call_as(action, method, Some(payload))
            .await?
            .decoded(&self.client.shared)
            .await?
            .0)
    }

    pub(crate) async fn verify_scoped_admission_closure(
        &self,
        closure: &ScopedAdmissionClosure,
    ) -> Result<(), ProtocolError> {
        let scoped = closure.reservation.scoped.ok_or_else(invalid)?;
        if self.client.action
            != (ServiceAction::StatusProof {
                source_group: scoped.status_group_id,
            })
        {
            return Err(invalid());
        }
        let response = self
            .response(Request::ScopedAdmissionClosure {
                identity: closure.identity.clone(),
                reservation: closure.reservation,
            })
            .await?;
        let context = response.metadata_context()?;
        let (actual, _): (Option<ScopedAdmissionClosure>, _) =
            response.decoded(&self.client.shared).await?;
        context.timestamp(40, &actual)?;
        if actual.as_ref() != Some(closure) {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) async fn verify_scoped_read_pin(
        &self,
        scope: ScopedCatalogRead,
        kind: Option<RequestKind>,
    ) -> Result<(), ProtocolError> {
        self.value(Request::ScopedReadPin {
            scope,
            kind,
            admission_read: false,
        })
        .await
    }

    pub(crate) async fn verify_scoped_admission_pin(
        &self,
        scope: ScopedCatalogRead,
    ) -> Result<(), ProtocolError> {
        self.value(Request::ScopedReadPin {
            scope,
            kind: None,
            admission_read: true,
        })
        .await
    }

    pub(crate) async fn verify_scoped_snapshot(
        &self,
        placement: &crate::oltp_replication::authority::control::ScopedQueuePlacement,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
        reservation: IdentityReservation,
    ) -> Result<(), ProtocolError> {
        let action = ServiceAction::ScopedQueueRead {
            queue_group_id: placement.group.group_id,
            queue_generation: placement.queue_generation,
            queue_view: placement.view,
            request_id: request.request_id,
        };
        let request = Request::ScopedSnapshot {
            identity,
            pin,
            request,
            reservation,
        };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        self.client
            .call_as(action, method, Some(payload))
            .await?
            .decoded::<()>(&self.client.shared)
            .await
            .map(|((), _)| ())
    }
    pub(crate) async fn allocate_commit(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        self.value(Request::Allocate { permit }).await
    }
    /// `AllocateDirect { count }` on the Timestamp leader. Repeating a call
    /// after a lost reply is safe: unused reserved values are skipped, and the
    /// repeat returns values above every completed allocation.
    pub(crate) async fn allocate_direct(
        &self,
        count: u64,
    ) -> Result<DirectTimestampRange, ProtocolError> {
        if count == 0 || count > MAX_DIRECT_COUNT {
            return Err(invalid());
        }
        let response = self.response(Request::AllocateDirect { count }).await?;
        let _timer = crate::oltp_runtime::stage_timing::perf_stage!("ts.rpc.decode").timer();
        // PERF2a: a range is about 60 bytes; decode it on this task. Its
        // decode charge was reserved with the reply (`member_reply`).
        let section = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
        let range: DirectTimestampRange = if section.len() <= INLINE_REPLY_BYTES {
            serde_json::from_slice(section.as_slice()).map_err(|_| invalid())?
        } else {
            response.decoded(&self.client.shared).await?.0
        };
        range.expect_len(count)
    }
    pub(crate) async fn confirmed_allocation(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        self.value(Request::Allocation { permit }).await
    }
    pub(crate) async fn release_scoped_terminal(
        &self,
        statement: ScopedTerminalStatement,
    ) -> Result<ScopedPinRelease, ProtocolError> {
        statement.validate()?;
        let _release = self.releases.try_acquire().map_err(|_| {
            ProtocolError::new(
                ErrorCode::Overloaded,
                "timestamp terminal release calls already in progress",
                Outcome::NotApplied,
            )
        })?;
        let action = scoped_retirement_owner(&statement);
        let request = Request::ReleaseScoped { statement: statement.clone() };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        // Timestamp returns the existing release for this terminal statement.
        let response = self.client.repeat_as(action, method, Some(payload)).await?;
        let context = response.metadata_context().map_err(mutation_uncertain)?;
        let (release, _): (ScopedPinRelease, _) = response
            .decoded(&self.client.shared)
            .await
            .map_err(mutation_uncertain)?;
        context.timestamp(27, &release).map_err(mutation_uncertain)?;
        release.validate(&statement).map_err(mutation_uncertain)?;
        Ok(release)
    }

    pub(crate) async fn confirm_scoped_terminal_release(
        &self,
        statement: ScopedTerminalStatement,
    ) -> Result<Option<ScopedPinRelease>, ProtocolError> {
        statement.validate()?;
        let action = scoped_retirement_holder(&statement);
        let request = Request::ConfirmedScopedRelease { statement: statement.clone() };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        let response = self.client.call_as(action, method, Some(payload)).await?;
        let context = response.metadata_context()?;
        let (release, _): (Option<ScopedPinRelease>, _) = response.decoded(&self.client.shared).await?;
        context.timestamp(31, &release)?;
        if let Some(release) = &release {
            release.validate(&statement)?;
        }
        Ok(release)
    }

    pub(crate) async fn scoped_copy_proof(
        &self,
        statement: ScopedTerminalStatement,
    ) -> Result<ScopedCopyProof, ProtocolError> {
        let response = self.response(Request::ScopedCopyProof { statement: statement.clone() }).await?;
        let context = response.metadata_context()?;
        let (proof, _): (ScopedCopyProof, _) = response.decoded(&self.client.shared).await?;
        context.timestamp(28, &proof)?;
        proof.validate()?;
        if proof.statement() != &statement {
            return Err(invalid());
        }
        Ok(proof)
    }

    pub(crate) async fn fence_scoped_queue(
        &self,
        transfer: &ScopedQueueTransition,
    ) -> Result<(), ProtocolError> {
        let group_id = transfer.next.group.group_id;
        let request = Request::FenceScopedQueue { group_id, transition_id: transfer.id };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        let response = self
            .client
            .repeat_as(
                ServiceAction::ParticipantProof { source_group: group_id },
                method,
                Some(payload),
            )
            .await?;
        let context = response.metadata_context().map_err(mutation_uncertain)?;
        let (applied, _): (Applied<TimestampReply>, _) = response
            .decoded(&self.client.shared)
            .await
            .map_err(mutation_uncertain)?;
        context.timestamp(29, &applied).map_err(mutation_uncertain)?;
        verify_scoped_fence_reply(transfer, &applied.reply).map_err(mutation_uncertain)
    }

    pub(crate) async fn scoped_queue_fence_receipt(
        &self,
        transfer: &ScopedQueueTransition,
    ) -> Result<Option<TimestampReply>, ProtocolError> {
        let group_id = transfer.next.group.group_id;
        let request = Request::ScopedQueueFenceReceipt {
            transition_id: transfer.id,
            previous: ScopedTimestampEpoch::from_placement(&transfer.previous)?,
            next: ScopedTimestampEpoch::from_placement(&transfer.next)?,
        };
        let method = request.method();
        let payload = encode(&self.client.shared, request).await?;
        let response = self.client.call_as(
            ServiceAction::ScopedQueueFenceReceipt { queue_group_id: group_id, transfer_id: transfer.id },
            method, Some(payload),
        ).await?;
        let context = response.metadata_context()?;
        let (proof, _): (Option<TimestampReply>, _) = response.decoded(&self.client.shared).await?;
        context.timestamp(30, &proof)?;
        if let Some(reply) = &proof {
            verify_scoped_fence_reply(transfer, reply)?;
        }
        Ok(proof)
    }

    pub(crate) async fn release(
        &self,
        statement: TerminalCopyStatement,
    ) -> Result<VerifiedPinRelease, ProtocolError> {
        let _release = self.releases.try_acquire().map_err(|_| {
            ProtocolError::new(
                ErrorCode::Overloaded,
                "timestamp terminal release calls already in progress",
                Outcome::NotApplied,
            )
        })?;
        self.release_call(statement, false).await
    }
    pub(crate) async fn confirmed_release(
        &self,
        statement: TerminalCopyStatement,
    ) -> Result<VerifiedPinRelease, ProtocolError> {
        self.release_call(statement, true).await
    }
    async fn release_call(
        &self,
        statement: TerminalCopyStatement,
        confirm: bool,
    ) -> Result<VerifiedPinRelease, ProtocolError> {
        let request = if confirm {
            Request::Released {
                statement: statement.clone(),
            }
        } else {
            Request::Release {
                statement: statement.clone(),
            }
        };
        let response = self.response(request).await?;
        let context = response.metadata_context()?;
        let ((proof, position), _): ((SealedTerminalProof, AppliedPosition), _) =
            response.decoded(&self.client.shared).await?;
        VerifiedPinRelease::from_service(
            context,
            proof,
            position,
            &statement,
            if confirm { 6 } else { 5 },
        )
    }
    pub(crate) async fn fence(
        &self,
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        let response = self
            .response(Request::Fence {
                transition_id,
                expected,
                next,
                ready,
            })
            .await?;
        let (applied, owner): (Applied<TimestampReply>, _) = response
            .decoded(&self.client.shared)
            .await
            .map_err(mutation_uncertain)?;
        Ok(Applied::owned(applied.position, applied.reply, owner))
    }
    pub(crate) async fn fence_ready(
        &self,
        transition_id: Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> Result<ReadyFenceReceipt, ProtocolError> {
        let response = self
            .response(Request::Ready {
                transition_id,
                expected,
                next,
            })
            .await?;
        let (receipt, _): (ReadyFenceReceipt, _) = response
            .decoded(&self.client.shared)
            .await
            .map_err(mutation_uncertain)?;
        if receipt.transition_id != transition_id
            || receipt.expected != expected
            || receipt.next != next
        {
            return Err(mutation_uncertain(invalid()));
        }
        Ok(receipt)
    }
    pub(crate) async fn begin_maintenance(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> Result<StoragePin, ProtocolError> {
        self.value(Request::Maintenance { authority, owner }).await
    }
    pub(crate) async fn release_maintenance(
        &self,
        authority: Authority,
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> Result<(), ProtocolError> {
        self.value(Request::CloseMaintenance {
            authority,
            owner: *proof.owner(),
        })
        .await
    }
    pub(crate) async fn begin_index_metadata(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        self.value(Request::IndexMetadata {
            authority,
            owner,
            txn,
            reservation,
        })
        .await
    }
    pub(crate) async fn begin_import_metadata(
        &self,
        authority: Authority,
        archive_id: Uuid,
        target_id: Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        self.value(Request::ImportMetadata {
            authority,
            archive_id,
            target_id,
            txn,
            reservation,
        })
        .await
    }
    pub(crate) async fn release_index_metadata(
        &self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        self.value(Request::ReleaseMetadata {
            authority,
            identity,
            pin,
            statement: settled.into_sealed().statement().clone(),
            import: false,
        })
        .await
    }
    pub(crate) async fn release_import_metadata(
        &self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        self.value(Request::ReleaseMetadata {
            authority,
            identity,
            pin,
            statement: settled.into_sealed().statement().clone(),
            import: true,
        })
        .await
    }
    pub(crate) async fn begin_backup(
        &self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    ) -> Result<StoragePin, ProtocolError> {
        self.value(Request::Backup { authority, owner }).await
    }
}

/// Installed by the real backup backend. The implementation must read the
/// fsynced publication, not turn a caller-supplied digest into a capability.
pub(crate) trait BackupPublicationReader: Send + Sync {
    fn verify_cancelled<'a>(
        &'a self,
        statement: crate::oltp::backup::lifecycle::BackupCancelledStatement,
    ) -> ConsensusFuture<'a, crate::oltp::backup::lifecycle::VerifiedBackupCancelled>;
    fn verify<'a>(
        &'a self,
        statement: crate::oltp::backup::BackupPublishedStatement,
    ) -> crate::oltp_replication::authority::consensus::ConsensusFuture<
        'a,
        crate::oltp::backup::VerifiedBackupPublished,
    >;
}
impl TimestampClient {
    pub(crate) async fn cancel_unaccepted(
        &self,
        rejection: VerifiedAdmissionRejection,
    ) -> Result<(), ProtocolError> {
        self.value(Request::Cancel {
            rejection: rejection.into_sealed(),
        })
        .await
    }
    pub(crate) async fn release_backup(
        &self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
    ) -> Result<(), ProtocolError> {
        self.value(Request::CloseBackup {
            authority,
            statement: proof.statement(),
        })
        .await
    }
}

impl DirectTimestampSource for TimestampClient {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        Box::pin(TimestampClient::allocate_direct(self, count))
    }
}
impl TimestampService for TimestampClient {
    fn advance_floor(
        &self,
        authority: Authority,
        retired_at: Option<CommitTs>,
    ) -> ConsensusFuture<'_, u64> {
        Box::pin(self.value(Request::AdvanceFloor {
            authority,
            retired_at,
        }))
    }
    fn native_archive_page<'a>(&'a self,closure:&'a VerifiedArchiveClosure,after:Option<Vec<u8>>)->ConsensusFuture<'a,crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage>{
        Box::pin(async move {
            let(mut page,owner):(crate::oltp_replication::authority::status::identity::transaction::archive_images::MetadataArchivePage,_)=self.response(Request::ArchiveMetadata{closure:closure.statement().clone(),after}).await?.decoded(&self.client.shared).await?;
            if page.group != *self.identity() || page.cut != closure.cut() {
                return Err(invalid());
            }
            page.retain_owner(owner);
            Ok(page)
        })
    }
    fn retain_archive<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
        previous_root: Option<Digest>,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            self.value(Request::RetainArchive {
                authority,
                statement: proof.statement(),
                previous_root,
            })
            .await
        })
    }
    fn cancel_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::lifecycle::VerifiedBackupCancelled,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            self.value(Request::CancelBackup {
                authority,
                statement: proof.statement(),
            })
            .await
        })
    }
    fn archive_closure<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
        pin: StoragePin,
        archive_id: Uuid,
        base_exclusive: Option<CommitTs>,
        status: Arc<dyn StatusService>,
    ) -> ConsensusFuture<'a, VerifiedArchiveClosure> {
        Box::pin(async move {
            if status.identity().cluster_id != self.identity().cluster_id
                || status.identity().database != self.identity().database
            {
                return Err(invalid());
            }
            let response = self
                .response(Request::ArchiveCreate {
                    authority,
                    owner,
                    pin,
                    archive_id,
                    base_exclusive,
                    status: (status.identity().kind != (GroupKind::Status { bucket: 0 }))
                        .then(|| status.identity().clone()),
                })
                .await?;
            let context = response.metadata_context()?;
            let (statement, _): (ArchiveClosureStatement, _) =
                response.decoded(&self.client.shared).await?;
            VerifiedArchiveClosure::from_created(
                context,
                statement,
                authority,
                owner,
                pin,
                archive_id,
                base_exclusive,
                status.identity(),
            )
        })
    }
    fn with_scoped_execution(
        &self,
        scope: ScopedCatalogRead,
    ) -> Result<Arc<dyn TimestampService>, ProtocolError> {
        Ok(Self::new(
            self.identity().clone(),
            self.resources().clone(),
            self.client.transport.clone(),
            self.client.control.clone(),
            ServiceAction::ScopedQueueExecute(scope),
        )?)
    }
    fn commit_timestamp_point<'a>(
        &'a self,
        locator: &'a TimestampDecisionLocator,
        reconcile: bool,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(TimestampClient::commit_timestamp_point(self, locator, reconcile))
    }
    fn abort_timestamp_point<'a>(
        &'a self,
        locator: &'a TimestampDecisionLocator,
        reconcile: bool,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(TimestampClient::abort_timestamp_point(self, locator, reconcile))
    }
    fn observe_timestamp_point<'a>(
        &'a self,
        locator: &'a TimestampDecisionLocator,
    ) -> ConsensusFuture<'a, Option<(PointDecision, Arc<ScratchLease>)>> {
        Box::pin(TimestampClient::observe_timestamp_point(self, locator))
    }
    fn abandon_timestamp_point<'a>(
        &'a self,
        locator: &'a TimestampDecisionLocator,
        transition_id: Uuid,
    ) -> ConsensusFuture<'a, (PointDecision, Arc<ScratchLease>)> {
        Box::pin(TimestampClient::abandon_timestamp_point(self, locator, transition_id))
    }
    fn identity(&self) -> &GroupIdentity {
        TimestampClient::identity(self)
    }
    fn resources(&self) -> &Arc<WorkerSharedResources> {
        TimestampClient::resources(self)
    }
    fn begin_import_metadata<'a>(
        &'a self,
        authority: Authority,
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move {
            TimestampClient::begin_import_metadata(
                self,
                authority,
                archive_id,
                target_id,
                txn,
                reservation,
            )
            .await
        })
    }
    fn release_import_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampClient::release_import_metadata(self, authority, identity, pin, settled).await
        })
    }
    fn begin_backup<'a>(
        &'a self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move { TimestampClient::begin_backup(self, authority, owner).await })
    }
    fn release_backup<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampClient::release_backup(self, authority, proof).await })
    }
    fn begin_index_metadata<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move {
            TimestampClient::begin_index_metadata(self, authority, owner, txn, reservation).await
        })
    }
    fn index_metadata_pin<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, IndexMetadataPin> {
        Box::pin(async move {
            self.value(Request::IndexMetadataPin { authority, owner, txn, reservation, request }).await
        })
    }
    fn release_index_metadata<'a>(
        &'a self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampClient::release_index_metadata(self, authority, identity, pin, settled).await
        })
    }
    fn begin_maintenance<'a>(
        &'a self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move { TimestampClient::begin_maintenance(self, authority, owner).await })
    }
    fn release_maintenance<'a>(
        &'a self,
        authority: Authority,
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampClient::release_maintenance(self, authority, proof).await })
    }
    fn cancel_unaccepted<'a>(
        &'a self,
        rejection: crate::oltp_replication::authority::status::VerifiedAdmissionRejection,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move { TimestampClient::cancel_unaccepted(self, rejection).await })
    }
    fn current_authority<'a>(&'a self) -> ConsensusFuture<'a, Option<Authority>> {
        Box::pin(async move { TimestampClient::current_authority(self).await })
    }
    fn release<'a>(
        &'a self,
        statement: TerminalCopyStatement,
    ) -> ConsensusFuture<'a, VerifiedPinRelease> {
        Box::pin(async move { TimestampClient::release(self, statement).await })
    }
    fn begin_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(
            async move { TimestampClient::begin_snapshot(self, view, identity, reservation).await },
        )
    }
    fn retry_snapshot<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        previous: crate::oltp_replication::authority::status::VerifiedSettlement,
    ) -> ConsensusFuture<'a, StoragePin> {
        Box::pin(async move {
            self.value(Request::RetrySnapshot {
                view,
                identity,
                previous: previous.into_sealed(),
            })
            .await
        })
    }
    fn bind_snapshot_context<'a>(
        &'a self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> ConsensusFuture<'a, ()> {
        Box::pin(async move {
            TimestampClient::bind_snapshot_context(self, view, identity, pin, request).await
        })
    }
    fn allocate_commit<'a>(
        &'a self,
        permit: SealedCommitPermit,
    ) -> ConsensusFuture<'a, CommitStamp> {
        Box::pin(async move { TimestampClient::allocate_commit(self, permit).await })
    }
    fn fence_ready<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> ConsensusFuture<'a, ReadyFenceReceipt> {
        Box::pin(
            async move { TimestampClient::fence_ready(self, transition_id, expected, next).await },
        )
    }
    fn fence<'a>(
        &'a self,
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> ConsensusFuture<'a, Applied<TimestampReply>> {
        Box::pin(
            async move { TimestampClient::fence(self, transition_id, expected, next, ready).await },
        )
    }
}

/// Who may call `AllocateDirect` (operation 41): the queue owner, a participant
/// proof, or a direct range leader (T7's `DirectLeader`, granted by Control only
/// while `data.direct.v1` is active). It is the only arm for operation 41, so no
/// other arm can shadow it (T9 finding: the `DirectLeader` grant sat in a later,
/// unreachable arm and every remote `AllocateDirect` was refused as invalid).
fn allocate_direct_permitted(action: &ServiceAction) -> bool {
    matches!(
        action,
        ServiceAction::Owner | ServiceAction::ParticipantProof { .. } | ServiceAction::DirectLeader
    )
}

#[cfg(test)]
mod tests;
