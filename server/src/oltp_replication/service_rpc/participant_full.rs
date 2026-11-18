//! Logical participant calls. Leaders validate dependent proofs through their
//! own authenticated reads before submitting a fixed command to local Raft.
use super::*;
use crate::oltp::mvcc::{
    MAX_FIXED_COMMAND_BYTES, MvccKey,
    store::{
        ParticipantCommand, ParticipantEpochs, ParticipantFuture, ParticipantHandle,
        ParticipantReply, RemoteParticipant, VerifiedParticipantResolution,
        VerifiedParticipantSeal,
        archive::ArchiveEffects,
        image_archive::{ImagePage, ImageRequest},
        index::VerifiedIndexAttemptClosed,
        lifecycle::{GuardMode, LifecycleBinding, LifecycleCommand, VerifiedLifecycleGuard},
        point::PointReadRequest,
        scan::{ScanDomain, ScanPage, ScanRequest},
    },
};
use crate::oltp_replication::{
    authority::{consensus::Applied, control::ScopedCatalogRead, status::StatusTopology},
    owned::{OwnedCommand, OwnedPayload},
};

const SCOPED_CATALOG_KEY_BYTES: usize = 512;

#[cfg(test)]
#[path = "participant_full/continuation_fixture.rs"]
pub(crate) mod continuation_fixture;

pub(crate) struct ParticipantEndpoint {
    proof: Arc<ParticipantProofEndpoint>,
    transport: WorkerClient,
    status_topology: Option<StatusTopology>,
}
impl ParticipantEndpoint {
    pub(crate) fn new(
        handle: Arc<ParticipantHandle>,
        node: u64,
        control: Arc<ControlClient>,
        transport: WorkerClient,
        status_topology: Option<StatusTopology>,
    ) -> Result<Arc<Self>, ProtocolError> {
        Ok(Arc::new(Self {
            proof: ParticipantProofEndpoint::new(handle, node, control)?,
            transport,
            status_topology,
        }))
    }
    fn parse(&self, session: &AuthenticatedSession, bytes: &[u8]) -> Result<Header, ProtocolError> {
        let head = parse(
            session,
            self.proof.handle.identity(),
            self.proof.node,
            bytes,
        )?;
        let allowed = match head.method {
            Method::ParticipantRead { read_ts } => match head.action {
                ServiceAction::Owner => true,
                ServiceAction::ScopedCatalogRead(scope) => {
                    self.proof.handle.identity().kind == GroupKind::Catalog
                        && scope.read_ts == read_ts
                        && head.payload_bytes <= SCOPED_CATALOG_KEY_BYTES as u64
                }
                _ => false,
            },
            Method::ParticipantReadPage
            | Method::ParticipantImages
            | Method::ParticipantArchiveEffects { .. }
            | Method::ParticipantScan
            | Method::ParticipantScanOpen { .. }
            | Method::ParticipantScanRun { .. }
            | Method::ParticipantActivation
            | Method::ParticipantAcquire
            | Method::ParticipantHolder { .. } => head.action == ServiceAction::Owner,
            Method::ParticipantPropose { .. } => {
                head.action == ServiceAction::Owner
                    || matches!(head.action,ServiceAction::ParticipantProof{source_group} if source_group==self.proof.handle.identity().group_id)
            }
            Method::ParticipantFence { transition_id, .. } => {
                matches!(head.action,ServiceAction::ControlFence{transition_id:actual} if actual==transition_id)
            }
            _ => false,
        };
        if !allowed {
            return Err(invalid());
        }
        Ok(head)
    }
    fn read_scope(
        shared: &WorkerSharedResources,
        session: &AuthenticatedSession,
        scope: ReadScope,
    ) -> Result<(), ProtocolError> {
        if !matches!(session.group().kind, GroupKind::Data { .. }) {
            return Err(invalid());
        }
        let replica = shared.read_replica()?;
        let context = replica
            .lock()
            .map_err(|_| unavailable())?
            .scan_context(scope, session.remote(), session.local())
            .map_err(crate::request_queue::QueueError::protocol)?;
        let parsed: crate::oltp_runtime::replica::AcceptedContext =
            serde_json::from_slice(context.as_slice()).map_err(|_| invalid())?;
        if serialized_digest(&parsed)? != scope.request.accepted_context
            || parsed
                .read
                .as_ref()
                .is_none_or(|read| read.storage_pin.read_ts != scope.read_ts)
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn scan(
        handle: Arc<ParticipantHandle>,
        node: u64,
        input: ChargedBytes,
        expected: Option<(ReadTs, Digest)>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let shared = handle.resources().clone();
        let request = shared
            .execute(ExecutionClass::Query, node, 0, {
                let shared = shared.clone();
                move || {
                    if let Some((_, digest)) = expected {
                        use sha2::{Digest as _, Sha256};
                        if Digest::from_bytes(Sha256::digest(input.as_slice()).into()) != digest {
                            return Err(invalid());
                        }
                    }
                    OwnedCommand::<ScanPayload>::decode_json(
                        &input,
                        &shared,
                        ByteClass::Data,
                        usize::MAX,
                    )
                }
            })
            .await?;
        let mut scan = request.payload()?.0.clone();
        if expected.is_some_and(|(read_ts, _)| scan.read_ts != read_ts) {
            return Err(invalid());
        }
        scan.after_owner = Some(request.decode_owner()?);
        let page = if expected.is_some() {
            handle.scan_page_wait(scan).await?
        } else {
            handle.scan_page(scan).await?
        };
        shared
            .execute(ExecutionClass::Query, node, 0, {
                let shared = shared.clone();
                move || shared.encode_in(ByteClass::Data, &page, MAX_FIXED_COMMAND_BYTES)
            })
            .await
    }

    fn check_status(&self, txn: TxnId, manifest: ManifestRef) -> Result<(), ProtocolError> {
        if let Some(topology) = &self.status_topology {
            topology.manifest(txn, manifest)?;
        }
        Ok(())
    }

    async fn status(&self, txn: TxnId, manifest: ManifestRef) -> Result<Arc<StatusProofClient>, ProtocolError> {
        self.check_status(txn, manifest)?;
        let group = registered_target(&self.proof.control, manifest.status_group_id).await?;
        if self.status_topology.as_ref().is_some_and(|topology| !topology.contains(&group)) {
            return Err(invalid());
        }
        StatusProofClient::new(
            group,
            self.proof.shared.clone(),
            self.transport.clone(),
            self.proof.control.clone(),
            ServiceAction::ParticipantProof {
                source_group: self.proof.handle.identity().group_id,
            },
        )
    }
    async fn validate(
        &self,
        session: &AuthenticatedSession,
        command: &ParticipantCommand,
        head: &Header,
    ) -> Result<(), ProtocolError> {
        match command {
            ParticipantCommand::SplitFreeze { .. }
            | ParticipantCommand::SplitStage { .. }
            | ParticipantCommand::SplitImportPage { .. }
            | ParticipantCommand::SplitFinishSource { .. } => {
                // Split transitions require proof reads through their own service.
                return Err(invalid());
            }
            ParticipantCommand::PreparePage { txn, manifest, guards, .. } => {
                self.check_status(*txn, *manifest)?;
                if head.action != ServiceAction::Owner {
                    return Err(invalid());
                }
                for guard in guards.iter() {
                    let binding = guard.binding();
                    let group =
                        registered_target(&self.proof.control, binding.catalog_group_id).await?;
                    let client = ParticipantProofClient::new(
                        group,
                        self.proof.shared.clone(),
                        self.transport.clone(),
                        self.proof.control.clone(),
                        ServiceAction::ParticipantProof {
                            source_group: self.proof.handle.identity().group_id,
                        },
                    )?;
                    let actual = client
                        .lifecycle(binding)
                        .await?
                        .ok_or_else(invalid)?
                        .into_sealed();
                    if actual != *guard {
                        return Err(invalid());
                    }
                }
            }
            ParticipantCommand::Seal { txn, manifest, .. } => {
                if head.action != ServiceAction::Owner {
                    return Err(invalid());
                }
                self.check_status(*txn, *manifest)?;
            }
            ParticipantCommand::Resolve { authority, proof, .. } => {
                let statement = proof.statement();
                self.check_status(statement.txn, statement.manifest)?;
                if !self
                    .proof
                    .handle
                    .has_resolution_witness(*authority, proof)
                    .await?
                {
                    let client = self.status(statement.txn, statement.manifest).await?;
                    if !client
                        .participates(
                            self.proof.handle.identity(),
                            statement.txn,
                            statement.manifest,
                        )
                        .await?
                    {
                        return Err(invalid());
                    }
                    let actual = client
                        .decision(statement.txn, statement.manifest)
                        .await?
                        .ok_or_else(unavailable)?;
                    if actual.statement() != statement {
                        return Err(invalid());
                    }
                }
            }
            ParticipantCommand::Lifecycle(LifecycleCommand::Release { guard, settled, .. }) => {
                if head.action != ServiceAction::Owner {
                    return Err(invalid());
                }
                let binding = guard.binding();
                let statement = settled.statement();
                let actual = self
                    .status(statement.txn, statement.manifest)
                    .await?
                    .settlement(statement.txn, statement.manifest)
                    .await?
                    .into_sealed();
                if actual.statement() != statement
                    || settled
                        .validate_for(binding.txn, &binding.request, &statement.manifest)
                        .is_err()
                {
                    return Err(invalid());
                }
            }
            ParticipantCommand::Lifecycle(
                LifecycleCommand::CancelPending {
                    binding, settled, ..
                }
                | LifecycleCommand::RecoverIndexPublication {
                    binding, settled, ..
                },
            ) => {
                if head.action != ServiceAction::Owner {
                    return Err(invalid());
                }
                if let ParticipantCommand::Lifecycle(LifecycleCommand::RecoverIndexPublication {
                    authority,
                    ..
                }) = command
                {
                    let scope = crate::oltp_runtime::replica::QueueScope {
                        cluster_id: session.group().cluster_id,
                        database: session.group().database.clone(),
                    };
                    let grant = self
                        .proof
                        .control
                        .service_role(
                            &scope,
                            session.remote(),
                            head.action,
                            session.group().clone(),
                        )
                        .await?;
                    if grant.effective_authority != *authority {
                        return Err(invalid());
                    }
                }
                let statement = settled.statement();
                let actual = self
                    .status(statement.txn, statement.manifest)
                    .await?
                    .settlement(statement.txn, statement.manifest)
                    .await?
                    .into_sealed();
                if actual.statement() != statement
                    || settled
                        .validate_for(binding.txn, &binding.request, &statement.manifest)
                        .is_err()
                {
                    return Err(invalid());
                }
            }
            ParticipantCommand::Fence { .. } => return Err(invalid()),
            _ if head.action != ServiceAction::Owner => return Err(invalid()),
            _ => {}
        }
        if matches!(head.method, Method::ParticipantPropose { progress: true })
            && matches!(
                command,
                ParticipantCommand::PreparePage { .. } | ParticipantCommand::SplitImportPage { .. }
            )
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn proof_method(method: Method) -> bool {
    matches!(
        method,
        Method::ParticipantSeal { .. }
            | Method::ParticipantResolution { .. }
            | Method::ParticipantGuard
            | Method::ParticipantIndexClosed { .. }
    )
}
impl Endpoint for ParticipantEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let generic = parse(
                session,
                self.proof.handle.identity(),
                self.proof.node,
                bytes,
            )?;
            if proof_method(generic.method) {
                return self.proof.preflight(session, bytes).await;
            }
            let head = self.parse(session, bytes)?;
            // Unused tickets reserve bounded metadata. Only the owner can
            // Run, after the exact grant reaches the ready queue replicas.
            if !matches!(head.method, Method::ParticipantScanOpen { .. }) {
                authorize(&self.proof.control, session, &head).await?;
            }
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
            let generic = parse(
                &session,
                self.proof.handle.identity(),
                self.proof.node,
                bytes.as_slice(),
            )?;
            if proof_method(generic.method) {
                return self.proof.handle(session, bytes, sections).await;
            }
            let head = self.parse(&session, bytes.as_slice())?;
            if let Method::ParticipantScanRun { ticket } = head.method {
                let handle = self.proof.handle.clone();
                let shared = self.proof.shared.clone();
                let control = self.proof.control.clone();
                let node = self.proof.node;
                let input = sections.section(0).ok_or_else(invalid)?.clone();
                let (grant, running) = shared.read_attempts.claim(
                    ticket,
                    session.remote(),
                    session.group().group_id,
                    node,
                )?;
                let scope = grant.scope;
                // The transport deadline and requester disconnect cannot
                // cancel a native job or release its ticket.
                return tokio::spawn(async move {
                    let result = async {
                        let queue_scope = crate::oltp_runtime::replica::QueueScope {
                            cluster_id: session.group().cluster_id,
                            database: session.group().database.clone(),
                        };
                        let authority = control.service_role(
                            &queue_scope, session.remote(), ServiceAction::Owner, session.group().clone(),
                        ).await?;
                        if authority.effective_authority != scope.authority {
                            return Err(unavailable());
                        }
                        Self::read_scope(&shared, &session, scope)?;
                        if !shared.read_replica()?.lock().map_err(|_| unavailable())?.knows_read_grant(grant) {
                            return Err(invalid());
                        }
                        #[cfg(test)]
                        read_attempt::fixture::running(grant).await;
                        let page = Self::scan(handle, node, input, Some((scope.read_ts, grant.input)));
                        #[cfg(test)]
                        let page = continuation_fixture::GRANT.scope(grant, page);
                        page.await
                    }
                    .await;
                    #[cfg(test)]
                    let success = result.is_ok();
                    #[cfg(test)]
                    if success {
                        read_attempt::fixture::reply_ready(
                            scope,
                            session.group().group_id,
                            ticket,
                            session.local(),
                        )
                        .await;
                    }
                    let result = reply(&shared, &session, node, result).await;
                    drop(running);
                    #[cfg(test)]
                    read_attempt::fixture::record(scope, grant.group, "run-finished", ticket, grant.worker);
                    #[cfg(test)]
                    if success {
                        read_attempt::fixture::record(
                            scope,
                            session.group().group_id,
                            "settled",
                            ticket,
                            session.local(),
                        );
                    }
                    result
                })
                .await
                .map_err(|_| unavailable())?;
            }
            let result = async {
                match head.method {
                    Method::ParticipantArchiveEffects {
                        txn,
                        manifest,
                        after,
                        max_items,
                        max_bytes,
                    } => {
                        let status = self.status(txn, manifest).await?;
                        if !status
                            .participates(self.proof.handle.identity(), txn, manifest)
                            .await?
                        {
                            return Err(invalid());
                        }
                        let decision = status
                            .decision(txn, manifest)
                            .await?
                            .ok_or_else(unavailable)?;
                        let page = self
                            .proof
                            .handle
                            .archive_effect_page(decision, after, max_items, max_bytes)
                            .await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Data, &page, MAX_FIXED_COMMAND_BYTES)
                            })
                            .await
                    }
                    Method::ParticipantRead { read_ts } => {
                        let shared = self.proof.shared.clone();
                        let key = shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, {
                                let shared = shared.clone();
                                move || {
                                    OwnedCommand::<KeyPayload>::decode_json(
                                        sections.section(0).ok_or_else(invalid)?,
                                        &shared,
                                        ByteClass::Data,
                                        usize::MAX,
                                    )
                                }
                            })
                            .await?;
                        let key = key.payload()?.0.clone();
                        if let ServiceAction::ScopedCatalogRead(scope) = head.action {
                            authorize(&self.proof.control, &session, &head).await?;
                            self.proof.handle.read_scoped_catalog(scope, key).await
                        } else {
                            self.proof.handle.read_at(read_ts, key).await
                        }
                    }
                    Method::ParticipantReadPage => {
                        let shared = self.proof.shared.clone();
                        let request = shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, {
                                let shared = shared.clone();
                                move || {
                                    OwnedCommand::<PointReadRequest>::decode_json(
                                        sections.section(0).ok_or_else(invalid)?,
                                        &shared,
                                        ByteClass::Data,
                                        usize::MAX,
                                    )
                                }
                            })
                            .await?;
                        let page = self.proof.handle.read_page(request).await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Data, &page, MAX_FIXED_COMMAND_BYTES)
                            })
                            .await
                    }
                    Method::ParticipantImages => {
                        let shared = self.proof.shared.clone();
                        let request = shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, {
                                let shared = shared.clone();
                                move || {
                                    OwnedCommand::<ImageRequest>::decode_json(
                                        sections.section(0).ok_or_else(invalid)?,
                                        &shared,
                                        ByteClass::Data,
                                        usize::MAX,
                                    )
                                }
                            })
                            .await?;
                        let page = self.proof.handle.committed_image_page(request).await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Query, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Data, &page, MAX_FIXED_COMMAND_BYTES)
                            })
                            .await
                    }
                    Method::ParticipantScan => {
                        Self::scan(
                            self.proof.handle.clone(),
                            self.proof.node,
                            sections.section(0).ok_or_else(invalid)?.clone(),
                            None,
                        )
                        .await
                    }
                    Method::ParticipantScanOpen { scope, input } => {
                        Self::read_scope(&self.proof.shared, &session, scope)?;
                        let grant = self.proof.shared.read_attempts.open(
                            &self.proof.shared,
                            ReadGrant {
                                scope,
                                owner: session.remote(),
                                worker: session.local(),
                                group: session.group().group_id,
                                node: self.proof.node,
                                ticket: uuid::Uuid::nil(),
                                input,
                            },
                        )?;
                        self.proof
                            .shared
                            .encode_in(ByteClass::Progress, &grant.ticket, 128)
                    }
                    Method::ParticipantActivation => {
                        let value = self.proof.handle.activation().await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantHolder { table_id } => {
                        let value = self.proof.handle.lifecycle_holder(table_id).await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantAcquire => {
                        let shared = self.proof.shared.clone();
                        let (binding, mode): (LifecycleBinding, GuardMode) = shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, {
                                let shared = shared.clone();
                                move || {
                                    let raw = sections.section(0).ok_or_else(invalid)?;
                                    let _scratch = shared.reserve_scratch(
                                        ByteClass::Progress,
                                        crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
                                    )?;
                                    serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())
                                }
                            })
                            .await?;
                        let value = self
                            .proof
                            .handle
                            .acquire_lifecycle(binding, mode)
                            .await?
                            .map(VerifiedLifecycleGuard::into_sealed);
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantFence {
                        transition_id,
                        expected,
                        next,
                    } => {
                        let scope = crate::oltp_runtime::replica::QueueScope {
                            cluster_id: session.group().cluster_id,
                            database: session.group().database.clone(),
                        };
                        let grant = self
                            .proof
                            .control
                            .service_role(
                                &scope,
                                session.remote(),
                                head.action,
                                session.group().clone(),
                            )
                            .await?;
                        if grant.effective_authority != next {
                            return Err(invalid());
                        }
                        let value = self
                            .proof
                            .handle
                            .fence_current(transition_id, expected, next)
                            .await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    Method::ParticipantPropose { progress } => {
                        let shared = self.proof.shared.clone();
                        let class = if progress {
                            ByteClass::Progress
                        } else {
                            ByteClass::Data
                        };
                        let command = shared
                            .execute(
                                if progress {
                                    ExecutionClass::Control
                                } else {
                                    ExecutionClass::Query
                                },
                                self.proof.node,
                                0,
                                {
                                    let shared = shared.clone();
                                    move || {
                                        OwnedCommand::<ParticipantCommand>::decode_json(
                                            sections.section(0).ok_or_else(invalid)?,
                                            &shared,
                                            class,
                                            usize::MAX,
                                        )
                                    }
                                },
                            )
                            .await?;
                        self.validate(&session, command.payload()?, &head).await?;
                        let value = self.proof.handle.propose(command).await?;
                        let shared = self.proof.shared.clone();
                        self.proof
                            .shared
                            .execute(ExecutionClass::Control, self.proof.node, 0, move || {
                                shared.encode_in(ByteClass::Progress, &value, 128 << 10)
                            })
                            .await
                    }
                    _ => Err(invalid()),
                }
            }
            .await;
            reply(&self.proof.shared, &session, self.proof.node, result).await
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct KeyPayload(MvccKey);
impl OwnedPayload for KeyPayload {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(self.0.heap_bytes())
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct ScanPayload(ScanRequest);
impl OwnedPayload for ScanPayload {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        fn key(key: &crate::protocol::Key) -> usize {
            match key {
                crate::protocol::Key::Utf8(value) => value.capacity(),
                _ => 0,
            }
        }
        fn value(value: &crate::oltp::mvcc::index::IndexValue) -> usize {
            match value {
                crate::oltp::mvcc::index::IndexValue::Utf8(value) => value.capacity(),
                _ => 0,
            }
        }
        let mut bytes = self.0.after.as_ref().map_or(0, MvccKey::heap_bytes)
            + self.0.lower.as_ref().map_or(0, |(v, _)| key(v))
            + self.0.upper.as_ref().map_or(0, |(v, _)| key(v));
        if let ScanDomain::Index {
            equality,
            lower,
            upper,
            ..
        } = &self.0.domain
        {
            bytes += equality.capacity()
                * std::mem::size_of::<crate::oltp::mvcc::index::IndexValue>()
                + equality.iter().map(value).sum::<usize>()
                + lower.as_ref().map_or(0, |(v, _)| value(v))
                + upper.as_ref().map_or(0, |(v, _)| value(v));
        }
        Ok(bytes)
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}

pub(crate) struct ParticipantClient {
    proof: Arc<ParticipantProofClient>,
}
impl ParticipantClient {
    pub(crate) fn new(
        group: GroupIdentity,
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        action: ServiceAction,
    ) -> Result<Arc<Self>, ProtocolError> {
        Ok(Arc::new(Self {
            proof: ParticipantProofClient::new(group, shared, transport, control, action)?,
        }))
    }
    async fn decoded<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        method: Method,
        payload: Option<ChargedBytes>,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let response = self.proof.client.call(method, payload).await?;
        self.proof
            .client
            .shared
            .execute(
                if method.purpose() == Purpose::Storage {
                    ExecutionClass::Query
                } else {
                    ExecutionClass::Control
                },
                0,
                0,
                move || {
                    let raw = response.reply.sections[0].as_ref().ok_or_else(invalid)?;
                    let value = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                    Ok((value, response.reply.scratch.clone()))
                },
            )
            .await
    }
    async fn read_at_as(
        &self,
        action: ServiceAction,
        read_ts: ReadTs,
        key: MvccKey,
        max_key_bytes: usize,
    ) -> Result<ChargedBytes, ProtocolError> {
        let client = &self.proof.client;
        let shared = client.shared.clone();
        let payload = client
            .shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                shared.encode_in(ByteClass::Data, &KeyPayload(key), max_key_bytes)
            })
            .await?;
        let response = client
            .call_as(action, Method::ParticipantRead { read_ts }, Some(payload))
            .await?;
        response.reply.sections[0].clone().ok_or_else(invalid)
    }
}
impl RemoteParticipant for ParticipantClient {
    fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> ParticipantFuture<'_, Applied<ParticipantReply>> {
        Box::pin(async move {
            let progress = !matches!(
                command.payload()?,
                ParticipantCommand::PreparePage { .. } | ParticipantCommand::SplitImportPage { .. }
            );
            let shared = self.proof.client.shared.clone();
            let payload = self
                .proof
                .client
                .shared
                .execute(
                    if progress {
                        ExecutionClass::Control
                    } else {
                        ExecutionClass::Query
                    },
                    0,
                    0,
                    move || {
                        shared.encode_in(
                            if progress {
                                ByteClass::Progress
                            } else {
                                ByteClass::Data
                            },
                            &command,
                            if progress {
                                128 << 10
                            } else {
                                MAX_FIXED_COMMAND_BYTES
                            },
                        )
                    },
                )
                .await?;
            let (value, owner): (Applied<ParticipantReply>, _) = self
                .decoded(Method::ParticipantPropose { progress }, Some(payload))
                .await?;
            Ok(Applied::owned(value.position, value.reply, owner))
        })
    }
    fn read_at(&self, read_ts: ReadTs, key: MvccKey) -> ParticipantFuture<'_, ChargedBytes> {
        Box::pin(self.read_at_as(self.proof.client.action, read_ts, key, MAX_FIXED_COMMAND_BYTES))
    }
    fn read_scoped_catalog(
        &self,
        scope: ScopedCatalogRead,
        key: MvccKey,
    ) -> ParticipantFuture<'_, ChargedBytes> {
        Box::pin(self.read_at_as(
            ServiceAction::ScopedCatalogRead(scope),
            scope.read_ts,
            key,
            SCOPED_CATALOG_KEY_BYTES,
        ))
    }
    fn read_page(
        &self,
        request: OwnedCommand<PointReadRequest>,
    ) -> ParticipantFuture<'_, OwnedCommand<ScanPage>> {
        Box::pin(async move {
            let shared = self.proof.client.shared.clone();
            let payload = self
                .proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    shared.encode_in(ByteClass::Data, &request, 1 << 20)
                })
                .await?;
            let response = self
                .proof
                .client
                .call(Method::ParticipantReadPage, Some(payload))
                .await?;
            self.proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    OwnedCommand::decode_with_owner(
                        response.reply.sections[0].as_ref().ok_or_else(invalid)?,
                        response.reply.scratch.clone(),
                        response.reply.scratch.bytes(),
                    )
                })
                .await
        })
    }
    fn scheduled_scan_page(
        &self,
        scope: ReadScope,
        request: ScanRequest,
    ) -> crate::oltp::mvcc::store::ScheduledReadFuture<'_> {
        Box::pin(async move {
            let client = &self.proof.client;
            let continuing = request.after.is_some();
            let (payload, digest) = client
                .shared
                .execute(ExecutionClass::Query, 0, 0, {
                    let shared = client.shared.clone();
                    move || {
                        use sha2::{Digest as _, Sha256};
                        let payload = shared.encode_in(
                            ByteClass::Data,
                            &ScanPayload(request),
                            MAX_FIXED_COMMAND_BYTES,
                        )?;
                        let digest = Digest::from_bytes(Sha256::digest(payload.as_slice()).into());
                        Ok((payload, digest))
                    }
                })
                .await?;
            let replica = client.shared.read_replica()?;
            let queue = client.shared.read_queue()?;
            #[cfg(test)]
            let continuation = continuation_fixture::checkpoint(scope, client, &payload, digest).await;
            if continuing {
                queue.resume_read_continuation(scope).await?;
            }
            #[cfg(test)]
            if let Some(continuation) = continuation {
                continuation.probe(client, &queue, scope, &payload, digest).await?;
            }
            let routes = client.control.replica_routes(&client.group).await?;
            let preferred = client.leader.load(Ordering::Relaxed);
            let payload = Some(payload);
            for prefer in [true, false] {
                for member in routes
                    .members
                    .iter()
                    .filter(|m| (m.node_id == preferred) == prefer)
                {
                    let worker = replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .read_member(scope.authority, member.worker_id);
                    let Some(worker) = worker else { continue };
                    #[cfg(test)]
                    if !read_attempt::fixture::target(scope, client.group.group_id, worker) {
                        continue;
                    }
                    let opened = match client
                        .call_member(
                            ServiceAction::Owner,
                            Method::ParticipantScanOpen {
                                scope,
                                input: digest,
                            },
                            &None,
                            member.node_id,
                            worker.into(),
                        )
                        .await
                    {
                        Ok(Ok(opened)) => opened,
                        Ok(Err(error)) if route_unavailable(&error) => continue,
                        Ok(Err(error)) => return Err(error.into()),
                        Err(CallError::Transport(RequestError::Unreachable)) => continue,
                        Err(error) => {
                            let error: ProtocolError = error.into();
                            if route_unavailable(&error) {
                                continue;
                            }
                            return Err(error.into());
                        }
                    };
                    let ticket = *opened.reply.decode_section::<uuid::Uuid>(0)?;
                    if ticket.is_nil() {
                        return Err(invalid().into());
                    }
                    drop(opened);
                    let read = UnsettledRead {
                        queue: queue.clone(),
                        grant: Box::new(ReadGrant {
                            scope,
                            owner: replica.lock().map_err(|_| unavailable())?.local_identity(),
                            worker,
                            group: client.group.group_id,
                            node: member.node_id,
                            ticket,
                            input: digest,
                        }),
                    };
                    if let Err(cause) = queue.open_read_grant(*read.grant).await {
                        if read.settle().await {
                            return Err(cause.into());
                        }
                        return Err(ScheduledReadError::Uncertain { cause, read });
                    }
                    #[cfg(test)]
                    read.record("run-dispatched");
                    let current = client.call_member(
                        ServiceAction::Owner,
                        Method::ParticipantScanRun { ticket },
                        &payload,
                        member.node_id,
                        worker.into(),
                    );
                    #[cfg(test)]
                    let current =
                        read_attempt::fixture::disconnect(scope, client.group.group_id, current);
                    let response = match current.await.map_err(ProtocolError::from).and_then(|reply| reply) {
                        Ok(response) => response,
                        Err(cause) => {
                            if !read.settle().await {
                                return Err(ScheduledReadError::Uncertain { cause, read });
                            }
                            if route_unavailable(&cause) {
                                continue;
                            }
                            return Err(cause.into());
                        }
                    };
                    if let Err(cause) = queue.finish_read_grant(*read.grant).await {
                        return Err(ScheduledReadError::Uncertain { cause, read });
                    }
                    let page = client
                        .shared
                        .execute(ExecutionClass::Query, 0, 0, move || {
                            OwnedCommand::decode_with_owner(
                                response.reply.sections[0].as_ref().ok_or_else(invalid)?,
                                response.reply.scratch.clone(),
                                response.reply.scratch.bytes(),
                            )
                        })
                        .await?;
                    #[cfg(test)]
                    continuation_fixture::page(*read.grant, payload.as_ref().unwrap(), page.payload()?);
                    return Ok(page);
                }
            }
            client.control.invalidate_replica_routes(&routes);
            Err(unavailable().into())
        })
    }

    fn scan_page(&self, request: ScanRequest) -> ParticipantFuture<'_, OwnedCommand<ScanPage>> {
        Box::pin(async move {
            let shared = self.proof.client.shared.clone();
            let payload = self
                .proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    shared.encode_in(
                        ByteClass::Data,
                        &ScanPayload(request),
                        MAX_FIXED_COMMAND_BYTES,
                    )
                })
                .await?;
            let response = self
                .proof
                .client
                .call(Method::ParticipantScan, Some(payload))
                .await?;
            self.proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    OwnedCommand::decode_with_owner(
                        response.reply.sections[0].as_ref().ok_or_else(invalid)?,
                        response.reply.scratch.clone(),
                        response.reply.scratch.bytes(),
                    )
                })
                .await
        })
    }
    fn committed_image_page(
        &self,
        request: OwnedCommand<ImageRequest>,
    ) -> ParticipantFuture<'_, OwnedCommand<ImagePage>> {
        Box::pin(async move {
            let shared = self.proof.client.shared.clone();
            let payload = self
                .proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    shared.encode_in(ByteClass::Data, &request, MAX_FIXED_COMMAND_BYTES)
                })
                .await?;
            let response = self
                .proof
                .client
                .call(Method::ParticipantImages, Some(payload))
                .await?;
            self.proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    OwnedCommand::decode_with_owner(
                        response.reply.sections[0].as_ref().ok_or_else(invalid)?,
                        response.reply.scratch.clone(),
                        response.reply.scratch.bytes(),
                    )
                })
                .await
        })
    }
    fn archive_effect_page(
        &self,
        proof: crate::oltp_replication::authority::status::VerifiedStatusDecision,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    ) -> ParticipantFuture<'_, OwnedCommand<ArchiveEffects>> {
        Box::pin(async move {
            let expected = proof.statement().clone();
            let response = self
                .proof
                .client
                .call(
                    Method::ParticipantArchiveEffects {
                        txn: expected.txn,
                        manifest: expected.manifest,
                        after,
                        max_items,
                        max_bytes,
                    },
                    None,
                )
                .await?;
            let group = self.proof.client.group.clone();
            self.proof
                .client
                .shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    let page: OwnedCommand<ArchiveEffects> = OwnedCommand::decode_with_owner(
                        response.reply.sections[0].as_ref().ok_or_else(invalid)?,
                        response.reply.scratch.clone(),
                        response.reply.scratch.bytes(),
                    )?;
                    let value = page.payload()?;
                    if value.group != group
                        || value.txn != expected.txn
                        || value.manifest != expected.manifest
                        || (crate::oltp_replication::types::TxnDecision::Commit {
                            commit_ts: value.commit_ts,
                        }) != expected.decision
                        || value.next_ordinal < after
                    {
                        return Err(invalid());
                    }
                    Ok(page)
                })
                .await
        })
    }
    fn activation(&self) -> ParticipantFuture<'_, Option<ParticipantEpochs>> {
        Box::pin(async move {
            self.decoded(Method::ParticipantActivation, None)
                .await
                .map(|(value, _)| value)
        })
    }
    fn strong_seal(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ParticipantFuture<'_, VerifiedParticipantSeal> {
        Box::pin(async move { self.proof.seal(txn, manifest).await })
    }
    fn strong_resolution(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> ParticipantFuture<'_, VerifiedParticipantResolution> {
        Box::pin(async move { self.proof.resolution(txn, manifest).await })
    }
    fn strong_index_attempt_closed(
        &self,
        owner: MaintenanceOwnerRef,
    ) -> ParticipantFuture<'_, VerifiedIndexAttemptClosed> {
        Box::pin(async move { self.proof.index_closed(owner).await })
    }
    fn acquire_lifecycle(
        &self,
        binding: LifecycleBinding,
        mode: GuardMode,
    ) -> ParticipantFuture<'_, Option<VerifiedLifecycleGuard>> {
        Box::pin(async move {
            let payload =
                self.proof
                    .client
                    .shared
                    .encode_in(ByteClass::Progress, &(&binding, mode), 4096)?;
            let response = self
                .proof
                .client
                .call(Method::ParticipantAcquire, Some(payload))
                .await?;
            let context = response.participant_context(ParticipantProofKind::Lifecycle)?;
            self.proof
                .client
                .shared
                .execute(ExecutionClass::Control, 0, 0, move || {
                    let guard: Option<crate::oltp::mvcc::store::lifecycle::SealedLifecycleGuard> =
                        serde_json::from_slice(
                            response.reply.sections[0]
                                .as_ref()
                                .ok_or_else(invalid)?
                                .as_slice(),
                        )
                        .map_err(|_| invalid())?;
                    guard
                        .map(|guard| {
                            if guard.mode() != mode {
                                return Err(invalid());
                            }
                            VerifiedLifecycleGuard::from_authenticated(context, guard, &binding)
                        })
                        .transpose()
                })
                .await
        })
    }
    fn lifecycle_guard(
        &self,
        binding: LifecycleBinding,
    ) -> ParticipantFuture<'_, Option<VerifiedLifecycleGuard>> {
        Box::pin(async move { self.proof.lifecycle(&binding).await })
    }
    fn lifecycle_holder(
        &self,
        table_id: uuid::Uuid,
    ) -> ParticipantFuture<'_, Option<LifecycleBinding>> {
        Box::pin(async move {
            self.decoded(Method::ParticipantHolder { table_id }, None)
                .await
                .map(|(value, _)| value)
        })
    }
    fn fence_current(
        &self,
        transition_id: uuid::Uuid,
        expected: Authority,
        next: Authority,
    ) -> ParticipantFuture<'_, GroupFenceReceipt> {
        Box::pin(async move {
            self.decoded(
                Method::ParticipantFence {
                    transition_id,
                    expected,
                    next,
                },
                None,
            )
            .await
            .map(|(value, _)| value)
        })
    }
}
