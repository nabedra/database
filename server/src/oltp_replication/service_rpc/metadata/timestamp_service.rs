use crate::oltp_replication::authority::status::StatusTopology;
use super::*;
use crate::oltp_replication::authority::proof::SealedTerminalProof;
use crate::oltp_replication::authority::timestamp::{
    IndexMetadataPin, VerifiedArchiveClosure, archive::ArchiveClosureStatement,
    cold::{ColdPinCursor, ColdPinStep},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

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
    Bind {
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    Allocate {
        permit: SealedCommitPermit,
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
            Self::ImportMetadata { .. } => 12,
            Self::ReleaseMetadata { .. } => 13,
            Self::Backup { .. } => 14,
            Self::Cancel { .. } => 15,
            Self::CloseBackup { .. } => 16,
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
    async fn dispatch(
        &self,
        session: &AuthenticatedSession,
        head: &Header,
        request: Request,
    ) -> Result<ChargedBytes, ProtocolError> {
        if request.method() != head.method {
            return Err(invalid());
        }
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
            Request::Begin {
                view,
                identity,
                reservation,
            } => {
                self.reservation(reservation).await?;
                encode(
                    shared,
                    self.handle
                        .begin_snapshot(view, identity, reservation)
                        .await?,
                )
                .await
            }
            Request::Bind {
                view,
                identity,
                pin,
                request,
            } => {
                self.handle
                    .bind_snapshot_context(view, identity, pin, request)
                    .await?;
                encode(shared, ()).await
            }
            Request::Allocate { permit } => {
                let statement = permit.statement();
                let group = self.status.manifest(statement.txn, statement.manifest)?;
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
            Request::Allocation { permit } => {
                let statement = permit.statement();
                let group = self.status.manifest(statement.txn, statement.manifest)?;
                if matches!(head.action, ServiceAction::StatusProof { source_group } if source_group != group.group_id) {
                    return Err(invalid());
                }
                encode(shared, self.handle.confirmed_allocation(permit).await?).await
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
            let head = self.parse(&session, bytes.as_slice())?;
            let result = async {
                let (request, owner) = decode(self.handle.resources(), sections).await?;
                let result = self.dispatch(&session, &head, request).await;
                drop(owner);
                result
            }
            .await;
            reply(self.handle.resources(), &session, self.node, result).await
        })
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
        // Snapshot transitions reuse the reservation nonce and exact context.
        // Allocate returns the existing stamp for the same transaction permit.
        let converge = matches!(
            request,
            Request::Current
                | Request::Begin { .. }
                | Request::Bind { .. }
                | Request::Allocate { .. }
                | Request::Allocation { .. }
                | Request::Released { .. }
        );
        self.client.named(request.method(), request, converge).await
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
    pub(crate) async fn allocate_commit(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        self.value(Request::Allocate { permit }).await
    }
    pub(crate) async fn confirmed_allocation(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        self.value(Request::Allocation { permit }).await
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
        let (applied, owner): (Applied<TimestampReply>, _) =
            response.decoded(&self.client.shared).await?;
        Ok(Applied::owned(applied.position, applied.reply, owner))
    }
    pub(crate) async fn fence_ready(
        &self,
        transition_id: Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> Result<ReadyFenceReceipt, ProtocolError> {
        self.value(Request::Ready {
            transition_id,
            expected,
            next,
        })
        .await
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

impl TimestampService for TimestampClient {
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

#[cfg(test)]
mod tests;
