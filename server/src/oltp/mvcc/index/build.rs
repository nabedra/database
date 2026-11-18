//! Operator index builds use ordinary fixed Status transactions. This service
//! never publishes a native side manifest or replays an application body.
use super::*;
use crate::oltp::{
    mvcc::{
        MvccKey,
        catalog::{CatalogRead, IntentRef},
        planner::{MutationPlan, NativePlanner, PlanError, ResolveFuture},
        store::{
            index::VerifiedIndexAttemptClosed,
            lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
            scan::{ScanDomain, ScanPage, ScanRequest},
        },
    },
    pool::ScratchLease,
};
use crate::oltp_replication::{
    owned::serialized_digest,
    types::{Authority, Digest, MaintenanceOwnerRef, RequestBinding, TxnId},
};
use std::sync::Arc;

pub(crate) struct IndexAttempt {
    pub pin: PinnedCatalog,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub owner: Arc<ScratchLease>,
    pub identity: crate::oltp_replication::types::PinIdentity,
    pub storage_pin: StoragePin,
}
/// One physical worker's existing queue/Status coordinator implements this.
/// Metadata jobs have distinct durable identities and exact snapshot bindings;
/// they are not represented as an invented public ORM operation.
pub(crate) trait IndexBackend: Send + Sync {
    fn planner(&self) -> &NativePlanner;
    fn begin(
        &self,
        job: Uuid,
        digest: Digest,
        maintenance: MaintenanceOwnerRef,
        fixed_identity: Option<Uuid>,
    ) -> ResolveFuture<'_, IndexAttempt>;
    fn checkpoint(&self) -> ResolveFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn commit(&self, attempt: IndexAttempt, plan: MutationPlan) -> ResolveFuture<'_, ()>;
    fn source_pin(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)>;
    /// Empty Update through actual RAM admission reads the current base version,
    /// maintains every Building/Ready index, and preserves concurrent changes.
    fn refresh(&self, table: Uuid, key: crate::protocol::Key, job: Uuid) -> ResolveFuture<'_, ()>;
    fn close_source(
        &self,
        authority: Authority,
        proof: VerifiedIndexAttemptClosed,
    ) -> ResolveFuture<'_, ()>;
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()>;
    fn help_lifecycle(
        &self,
        binding: crate::oltp::mvcc::store::lifecycle::LifecycleBinding,
    ) -> ResolveFuture<'_, ()>;
    fn fail(
        &self,
        attempt: IndexAttempt,
        guards: Vec<VerifiedLifecycleGuard>,
        pending: Option<LifecycleBinding>,
        error: ProtocolError,
    ) -> ResolveFuture<'_, ()>;
}
pub(crate) struct IndexService {
    backend: Arc<dyn IndexBackend>,
}
impl IndexService {
    pub(crate) fn new(backend: Arc<dyn IndexBackend>) -> Self {
        Self { backend }
    }
    /// The source pin is allocated after exclusive acquisition. That cut includes
    /// every earlier writer which could have omitted this new index.
    pub(crate) async fn create(
        &self,
        definition: IndexDefinition,
        build_id: Uuid,
        attempt: u64,
    ) -> Result<(), ProtocolError> {
        let (maintenance, source) = self.declare(definition, build_id, attempt, None).await?;
        self.backfill(maintenance, source).await
    }

    pub(crate) async fn declare(
        &self,
        definition: IndexDefinition,
        build_id: Uuid,
        attempt: u64,
        fixed_identity: Option<Uuid>,
    ) -> Result<(MaintenanceOwnerRef, PinnedCatalog), ProtocolError> {
        if build_id.is_nil() || attempt == 0 {
            return Err(invalid("invalid index build attempt"));
        }
        let maintenance = MaintenanceOwnerRef {
            catalog_group_id: self
                .backend
                .planner()
                .catalog()
                .participant()
                .identity()
                .group_id,
            table_id: definition.table_id,
            index_id: definition.index_id,
            build_id,
            attempt,
        };
        let (job, digest) = declaration_job(maintenance, &definition)?;
        let metadata = self
            .backend
            .begin(job, digest, maintenance, fixed_identity)
            .await?;
        let mut tries = 0;
        let guard = loop {
            tries += 1;
            match self
                .backend
                .planner()
                .acquire_index_guard(
                    &metadata.pin,
                    metadata.txn,
                    metadata.request,
                    definition.table_id,
                    GuardMode::Exclusive,
                )
                .await
            {
                Ok(guard) => break guard,
                Err(PlanError::Blocked(intent)) if tries < 8 => self.backend.help(intent).await?,
                Err(PlanError::LifecyclePending(binding)) if tries < 8 => {
                    self.backend.help_lifecycle(binding).await?
                }
                Err(error) => return self.failed(metadata, error).await,
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        };
        if let Err(cause) = self.backend.planner().validate_index_declaration(&metadata.pin, &definition).await {
            return self.failed(metadata, PlanError::Guarded {
                cause: Box::new(cause),
                guards: vec![guard],
            }).await;
        }
        let (pin, source) = match self
            .backend
            .source_pin(metadata.pin.authority, maintenance)
            .await
        {
            Ok(pin) => pin,
            Err(error) => {
                self.backend
                    .fail(metadata, vec![guard], None, error.clone())
                    .await?;
                return Err(error);
            }
        };
        let plan = self
            .backend
            .planner()
            .plan_declare_index(
                &metadata.pin,
                metadata.txn,
                metadata.request,
                definition.clone(),
                IndexPhase::Building {
                    build_id,
                    attempt,
                    pin,
                    source: source.clone(),
                },
                guard,
                metadata.owner.clone(),
            )
            .await;
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => return self.failed(metadata, error).await,
        };
        self.backend.commit(metadata, plan).await?;
        Ok((maintenance, source))
    }
    /// Resume an existing Building attempt under its original pinned source.
    /// The caller obtains the record at a fresh Catalog snapshot; abandoned
    /// attempts need an explicit fenced replacement, never a renewed old pin.
    pub(crate) async fn resume(&self, record: &IndexRecord) -> Result<(), ProtocolError> {
        let IndexPhase::Building {
            build_id,
            attempt,
            source,
            ..
        } = &record.phase
        else {
            return Err(invalid("index is not building"));
        };
        if self
            .backend
            .planner()
            .catalog()
            .participant()
            .epochs()
            .await?
            .authority
            .run_id
            != source.authority.run_id
        {
            return Err(invalid(
                "old-run index build requires a fresh fenced attempt",
            ));
        }
        let owner = MaintenanceOwnerRef {
            catalog_group_id: source.catalog.group_id,
            table_id: record.definition.table_id,
            index_id: record.definition.index_id,
            build_id: *build_id,
            attempt: *attempt,
        };
        self.backfill(owner, source.clone()).await
    }
    pub(crate) async fn restart(
        &self,
        record: &IndexRecord,
        next_attempt: u64,
    ) -> Result<(), ProtocolError> {
        let (build_id, attempt, _) = record.phase.attempt_binding();
        if !matches!(record.phase, IndexPhase::Building { .. }) || next_attempt <= attempt {
            return Err(invalid("index restart requires a newer Building attempt"));
        }
        let old = MaintenanceOwnerRef {
            catalog_group_id: self
                .backend
                .planner()
                .catalog()
                .participant()
                .identity()
                .group_id,
            table_id: record.definition.table_id,
            index_id: record.definition.index_id,
            build_id,
            attempt,
        };
        let next = MaintenanceOwnerRef {
            attempt: next_attempt,
            ..old
        };
        let metadata = self
            .backend
            .begin(
                job_id(next, b"restart", None)?,
                serialized_digest(&(b"index-restart/v1", old, next))?,
                next,
                None,
            )
            .await?;
        let guard = loop {
            match self
                .backend
                .planner()
                .acquire_index_guard(
                    &metadata.pin,
                    metadata.txn,
                    metadata.request,
                    next.table_id,
                    GuardMode::Exclusive,
                )
                .await
            {
                Ok(guard) => break guard,
                Err(PlanError::Blocked(i)) => self.backend.help(i).await?,
                Err(PlanError::LifecyclePending(binding)) => {
                    self.backend.help_lifecycle(binding).await?
                }
                Err(error) => return self.failed(metadata, error).await,
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        };
        let (pin, source) = match self.backend.source_pin(metadata.pin.authority, next).await {
            Ok(source) => source,
            Err(error) => {
                self.backend
                    .fail(metadata, vec![guard], None, error.clone())
                    .await?;
                return Err(error);
            }
        };
        let authority = metadata.pin.authority;
        let plan = self
            .backend
            .planner()
            .plan_restart_index(
                &metadata.pin,
                metadata.txn,
                metadata.request,
                old,
                IndexPhase::Building {
                    build_id,
                    attempt: next_attempt,
                    pin,
                    source: source.clone(),
                },
                guard,
                metadata.owner.clone(),
            )
            .await;
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => return self.failed(metadata, error).await,
        };
        self.backend.commit(metadata, plan).await?;
        self.backend
            .close_source(
                authority,
                self.backend
                    .planner()
                    .catalog()
                    .participant()
                    .strong_index_attempt_closed(old)
                    .await?,
            )
            .await?;
        self.backfill(next, source).await
    }
    pub(crate) async fn retire(&self, record: &IndexRecord) -> Result<(), ProtocolError> {
        let (build_id, attempt, _) = record.phase.attempt_binding();
        let maintenance = MaintenanceOwnerRef {
            catalog_group_id: self
                .backend
                .planner()
                .catalog()
                .participant()
                .identity()
                .group_id,
            table_id: record.definition.table_id,
            index_id: record.definition.index_id,
            build_id,
            attempt,
        };
        let metadata = self
            .backend
            .begin(
                job_id(maintenance, b"retire", None)?,
                serialized_digest(&(b"index-retire/v1", maintenance))?,
                maintenance,
                None,
            )
            .await?;
        let authority = metadata.pin.authority;
        let plan = self
            .backend
            .planner()
            .plan_retire_index(
                &metadata.pin,
                metadata.txn,
                metadata.request,
                maintenance,
                metadata.owner.clone(),
            )
            .await;
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => return self.failed(metadata, error).await,
        };
        self.backend.commit(metadata, plan).await?;
        self.backend
            .close_source(
                authority,
                self.backend
                    .planner()
                    .catalog()
                    .participant()
                    .strong_index_attempt_closed(maintenance)
                    .await?,
            )
            .await
    }
    async fn backfill(
        &self,
        maintenance: MaintenanceOwnerRef,
        source: PinnedCatalog,
    ) -> Result<(), ProtocolError> {
        let mut next = self.routing_root(&source, maintenance.table_id).await?;
        while let Some(page_id) = next {
            self.backend.checkpoint().await?;
            let mut blocked = 0;
            let page = loop {
                match self
                    .backend
                    .planner()
                    .catalog()
                    .routing_page(&source, maintenance.table_id, page_id)
                    .await?
                {
                    CatalogRead::Value {
                        record: Some(page), ..
                    } => break page,
                    CatalogRead::Blocked(intent) if blocked < 8 => {
                        blocked += 1;
                        self.backend.help(intent).await?;
                    }
                    CatalogRead::Blocked(_) => return Err(blocked_index()),
                    _ => return Err(invalid("index source routing is absent")),
                }
            };
            for route in page.shards.iter() {
                self.backend.checkpoint().await?;
                let current = self.backend.planner().catalog().snapshot(
                    crate::oltp_replication::types::ReadTs::new(u64::MAX)
                        .map_err(|_| blocked_index())?,
                    source.authority,
                )?;
                let completed = self.backend.planner().catalog().read::<IndexProgress>(
                    &current,
                    MvccKey::CatalogIndexProgress {
                        table_id: maintenance.table_id,
                        progress_id: progress_id(maintenance.index_id, route.group.group_id),
                    },
                ).await?;
                if matches!(completed, CatalogRead::Value { record: Some(progress), .. }
                    if progress.done && progress.build_id == maintenance.build_id
                    && progress.attempt == maintenance.attempt && progress.group_id == route.group.group_id
                    && progress.span == route.span)
                {
                    continue;
                }
                let participant = self
                    .backend
                    .planner()
                    .index_participant(&route.group)
                    .await?;
                let mut after = None;
                let mut after_owner = None;
                let mut blocked = 0;
                loop {
                    self.backend.checkpoint().await?;
                    let (scan_after, scan_owner) =
                        crate::oltp::mvcc::store::scan::clone_scan_cursor(
                            self.backend.planner().catalog().resources(),
                            &after,
                            &after_owner,
                        )?;
                    let response = participant
                        .scan_page(ScanRequest {
                            domain: ScanDomain::Rows {
                                table_id: maintenance.table_id,
                            },
                            read_ts: source.read_ts,
                            after: scan_after,
                            after_owner: scan_owner,
                            lower: route.span.start.clone().map(|key| (key, true)),
                            upper: route.span.end.clone().map(|key| (key, false)),
                            max_items: 128,
                            max_bytes: 32 << 20,
                        })
                        .await?;
                    match response.payload()? {
                        ScanPage::Blocked { txn, manifest } => {
                            if blocked == 8 {
                                return Err(blocked_index());
                            }
                            blocked += 1;
                            self.backend
                                .help(IntentRef {
                                    txn: *txn,
                                    manifest: *manifest,
                                })
                                .await?;
                        }
                        ScanPage::Records { records, exhausted } => {
                            blocked = 0;
                            for record in records.iter() {
                                if record.value.is_some() {
                                    let MvccKey::Row { primary_key, .. } = &record.key else {
                                        return Err(invalid("index source row key"));
                                    };
                                    let job = job_id(
                                        maintenance,
                                        b"refresh",
                                        Some(serialized_digest(primary_key)?),
                                    )?;
                                    self.backend
                                        .refresh(maintenance.table_id, primary_key.clone(), job)
                                        .await?;
                                }
                            }
                            if *exhausted {
                                break;
                            }
                            after = Some(
                                records
                                    .last()
                                    .ok_or_else(|| invalid("empty nonterminal index scan"))?
                                    .key
                                    .clone(),
                            );
                            after_owner = Some(response.decode_owner()?);
                        }
                    }
                }
                let job = job_id(
                    maintenance,
                    b"progress",
                    Some(serialized_digest(&route.group.group_id)?),
                )?;
                let metadata = self
                    .backend
                    .begin(
                        job,
                        serialized_digest(&(
                            b"index-progress/v1",
                            maintenance,
                            route.group.group_id,
                        ))?,
                        maintenance,
                        None,
                    )
                    .await?;
                let plan = self
                    .backend
                    .planner()
                    .plan_index_progress(
                        &metadata.pin,
                        metadata.txn,
                        metadata.request,
                        maintenance,
                        IndexProgress {
                            build_id: maintenance.build_id,
                            attempt: maintenance.attempt,
                            group_id: route.group.group_id,
                            span: route.span.clone(),
                            done: true,
                        },
                        metadata.owner.clone(),
                    )
                    .await;
                let plan = match plan {
                    Ok(plan) => plan,
                    Err(error) => return self.failed(metadata, error).await,
                };
                self.backend.commit(metadata, plan).await?;
            }
            next = page.next_page;
        }
        let (job, digest) = ready_job(maintenance)?;
        let metadata = self
            .backend
            .begin(job, digest, maintenance, None)
            .await?;
        let authority = metadata.pin.authority;
        let plan = self
            .backend
            .planner()
            .plan_finish_index(
                &metadata.pin,
                metadata.txn,
                metadata.request,
                maintenance,
                IndexClosedKind::Ready,
                metadata.owner.clone(),
            )
            .await;
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => return self.failed(metadata, error).await,
        };
        self.backend.commit(metadata, plan).await?;
        let proof = self
            .backend
            .planner()
            .catalog()
            .participant()
            .strong_index_attempt_closed(maintenance)
            .await?;
        self.backend.close_source(authority, proof).await
    }
    async fn routing_root(
        &self,
        source: &PinnedCatalog,
        table: Uuid,
    ) -> Result<Option<Uuid>, ProtocolError> {
        for _ in 0..8 {
            self.backend.checkpoint().await?;
            match self
                .backend
                .planner()
                .catalog()
                .routing_root(source, table)
                .await?
            {
                CatalogRead::Value {
                    record: Some(root), ..
                } => return Ok(root.first_page),
                CatalogRead::Blocked(intent) => self.backend.help(intent).await?,
                _ => return Err(invalid("index source routing is absent")),
            }
        }
        Err(blocked_index())
    }
    async fn failed<T>(&self, attempt: IndexAttempt, error: PlanError) -> Result<T, ProtocolError> {
        let mut error = error;
        let mut guards = Vec::new();
        loop {
            match error {
                PlanError::Guarded {
                    cause,
                    guards: held,
                } => {
                    guards.extend(held);
                    error = *cause;
                }
                PlanError::Protocol(error) => {
                    self.backend.fail(attempt, guards, None, error.clone()).await?;
                    return Err(error);
                }
                error => {
                    let pending = match error {
                        PlanError::LifecyclePending(binding) => Some(binding),
                        _ => None,
                    };
                    let error = blocked_index();
                    self.backend.fail(attempt, guards, pending, error.clone()).await?;
                    return Err(error);
                }
            }
        }
    }
}

fn blocked_index() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable, "index dependency is still active")
}

pub(crate) fn ready_job(owner: MaintenanceOwnerRef) -> Result<(Uuid, Digest), ProtocolError> {
    Ok((job_id(owner, b"ready", None)?, serialized_digest(&(b"index-ready/v1", owner))?))
}

pub(crate) fn declaration_job(owner: MaintenanceOwnerRef, definition: &IndexDefinition) -> Result<(Uuid, Digest), ProtocolError> {
    Ok((job_id(owner, b"declare", None)?, serialized_digest(&(b"index-declare/v1", definition, owner))?))
}
fn job_id(
    owner: MaintenanceOwnerRef,
    stage: &[u8],
    key: Option<Digest>,
) -> Result<Uuid, ProtocolError> {
    let digest = serialized_digest(&(b"nabedra/index-job/v1", owner, stage, key))?;
    Ok(Uuid::from_bytes(
        digest.as_bytes()[..16].try_into().expect("fixed digest"),
    ))
}
