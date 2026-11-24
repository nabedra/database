//! Cross-group mutation publication. Status fixes the complete manifest and
//! exact outcome before any Prepare; COMMIT follows every strong participant
//! seal, and Resolve applies only the immutable Status decision.
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson, PreparedWrite,
            planner::{MutationPlan, ParticipantResolver, ScopedParticipantResolver},
            store::{
                self, ParticipantCommand, ParticipantHandle, ParticipantReply,
                lifecycle::SealedLifecycleGuard,
            },
        },
        pool::{ByteClass, ExecutionClass, WorkerSharedResources},
    },
    oltp_replication::{
        authority::{
            control::{ControlHandle, ScopedCatalogRead},
            status::{
                IdentityLookup, IdentityReservation, PointManifest, StatusService,
                TerminalIdentity, VerifiedSettlement, VerifiedStatusDecision,
            },
            timestamp::{ScopedTimestampEpoch, TimestampService},
        },
        owned::OwnedCommand,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::sync::Arc;

mod point;
use super::{replica::QueueReadToken, worker_runtime::direct_capability::DirectSwitch};

pub(super) const RETRY_PROGRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
use point::PointAdmission;

struct ParticipantBatch {
    participant: Arc<ParticipantHandle>,
    plan: ParticipantPlan,
    pages: Vec<Page<PreparedWrite>>,
    guards: Page<SealedLifecycleGuard>,
}
struct PreparedMutation {
    participants: Vec<ParticipantBatch>,
    result: CanonicalJson,
    original: MutationPlan,
}
enum CommitMode<'a> {
    Standard,
    Recoverable,
    Scoped(&'a PointAdmission<'a>),
    Queued(Option<&'a DirectSwitch>),
}

pub(crate) struct TransactionCoordinator {
    shared: Arc<WorkerSharedResources>,
    status: Arc<dyn StatusService>,
    timestamps: Arc<dyn TimestampService>,
    resolver: Arc<dyn ParticipantResolver>,
    catalog: Arc<ParticipantHandle>,
    scope: Option<ScopedCatalogRead>,
}
impl TransactionCoordinator {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        status: Arc<dyn StatusService>,
        timestamps: Arc<dyn TimestampService>,
        resolver: Arc<dyn ParticipantResolver>,
        catalog: Arc<ParticipantHandle>,
    ) -> Self {
        Self {
            shared,
            status,
            timestamps,
            resolver,
            catalog,
            scope: None,
        }
    }

    fn scoped(&self, scope: ScopedCatalogRead) -> Result<Self, ProtocolError> {
        Ok(Self {
            shared: self.shared.clone(),
            status: self.status.with_scoped_execution(scope)?,
            timestamps: self.timestamps.with_scoped_execution(scope)?,
            resolver: Arc::new(ScopedParticipantResolver {
                resolver: self.resolver.clone(),
                scope,
            }),
            catalog: Arc::new(self.catalog.with_scoped_execution(scope)?),
            scope: Some(scope),
        })
    }

    pub(crate) async fn commit_scoped(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
        mut operation: MutationPlan,
        reservation: IdentityReservation,
        read: &QueueReadToken,
        epoch: ScopedTimestampEpoch,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        check_scope(authority, scope, txn)?;
        let coordinator = self.scoped(scope)?;
        let [participant] = operation.participants.as_mut_slice() else {
            return Err(scope_mismatch());
        };
        if participant.participant.identity().group_id != scope.queue_group_id
            || !matches!(participant.participant.identity().kind, GroupKind::Data { table_id, .. }
                if table_id == scope.table_id)
            || participant.writes.is_empty()
            || participant.guards.len() != 1
            || !operation.result.is_boolean()
            || operation.lifecycle.len() != 1
        {
            return Err(scope_mismatch());
        }
        participant.participant = Arc::new(participant.participant.with_scoped_execution(scope)?);
        let admission = PointAdmission {
            reservation,
            read,
            epoch,
        };
        coordinator
            .commit_inner(
                authority,
                txn,
                scope.request,
                scope.read_ts,
                operation,
                CommitMode::Scoped(&admission),
            )
            .await
    }

    pub(crate) async fn recover_scoped(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        check_scope(authority, scope, txn)?;
        self.scoped(scope)?.recover(authority, txn).await
    }

    pub(crate) async fn fail_scoped(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
        error: ProtocolError,
        guards: Vec<store::lifecycle::VerifiedLifecycleGuard>,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        check_scope(authority, scope, txn)?;
        if error.outcome != Outcome::NotApplied {
            return Err(error);
        }
        let coordinator = self.scoped(scope)?;
        if let Some(decision) = coordinator.recover(authority, txn).await? {
            return Ok(decision);
        }
        coordinator
            .status
            .terminal_failure(authority, scope.request, error)
            .await?;
        let settled = coordinator
            .status
            .failure_settled_proof(txn, scope.request)
            .await?;
        for guard in guards {
            let released = coordinator
                .catalog
                .release_lifecycle(authority, guard, settled.clone())
                .await?;
            if !matches!(
                released.reply,
                ParticipantReply::Lifecycle(store::lifecycle::LifecycleReply::Released)
            ) {
                return Err(unknown());
            }
        }
        coordinator
            .status
            .lookup_decision(txn, settled.statement().manifest)
            .await?
            .ok_or_else(unknown)
    }

    pub(crate) async fn commit(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        operation: MutationPlan,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        self.commit_inner(
            authority,
            txn,
            request,
            read_ts,
            operation,
            CommitMode::Standard,
        )
        .await
    }
    pub(super) async fn commit_recoverable(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        operation: MutationPlan,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        self.commit_inner(
            authority,
            txn,
            request,
            read_ts,
            operation,
            CommitMode::Recoverable,
        )
        .await
    }
    pub(super) async fn commit_queued(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        operation: MutationPlan,
        switch: Option<&DirectSwitch>,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        self.commit_inner(
            authority,
            txn,
            request,
            read_ts,
            operation,
            CommitMode::Queued(switch),
        )
        .await
    }

    async fn commit_inner(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        mut operation: MutationPlan,
        mode: CommitMode<'_>,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let recoverable = matches!(mode, CommitMode::Scoped(_) | CommitMode::Recoverable);
        let (admission, retry_switch) = match mode {
            CommitMode::Scoped(admission) => (Some(admission), None),
            CommitMode::Queued(switch) => (None, switch),
            _ => (None, None),
        };
        let resources = self.shared.clone();
        let status_group = self.status.group_for_request(txn.request_id).group_id;
        let prepared = self
            .shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                let result = CanonicalJson::from_serializable(
                    &resources,
                    &operation.result,
                    crate::protocol::MAX_BODY_BYTES,
                )?;
                let mut participants = Vec::new();
                operation
                    .participants
                    .sort_by_key(|part| part.participant.identity().group_id);
                for mut participant in std::mem::take(&mut operation.participants) {
                    participant
                        .writes
                        .sort_by(|a, b| store::compare_keys(&a.key, &b.key));
                    let mut plan = ParticipantPlan {
                        group: participant.participant.identity().clone(),
                        schema_epoch: participant.schema_epoch,
                        routing_epoch: participant.routing_epoch,
                        read_ts,
                        write_root: Digest::from_bytes([0; 32]),
                        write_count: participant.writes.len() as u64,
                        page_count: 0,
                    };
                    let mut root = store::initial_root(txn, &plan)?;
                    // Count fields at their widest representation during admission;
                    // the final manifest has the same fixed-size digest/group IDs.
                    let ceiling = ManifestRef {
                        status_group_id: status_group,
                        digest: Digest::from_bytes([255; 32]),
                        participant_count: u64::MAX,
                    };
                    let mut pages = Vec::new();
                    let mut page = Vec::new();
                    let mut budget = store::PreparePageBudget::new(&txn, &ceiling);
                    for write in participant.writes {
                        if page.len() == 128 || !budget.try_add(&write)? {
                            if page.is_empty() {
                                return Err(resource_limit());
                            }
                            root = store::page_root(root, pages.len() as u64, &page)?;
                            pages.push(
                                Page::new(std::mem::take(&mut page))
                                    .map_err(|_| resource_limit())?,
                            );
                            budget = store::PreparePageBudget::new(&txn, &ceiling);
                            if !budget.try_add(&write)? {
                                return Err(resource_limit());
                            }
                        }
                        page.push(write);
                    }
                    if !page.is_empty() {
                        root = store::page_root(root, pages.len() as u64, &page)?;
                        pages.push(Page::new(page).map_err(|_| resource_limit())?);
                    }
                    plan.write_root = root;
                    plan.page_count = pages.len() as u64;
                    plan.validate().map_err(|_| resource_limit())?;
                    participants.push(ParticipantBatch {
                        participant: participant.participant,
                        plan,
                        pages,
                        guards: Page::new(participant.guards).map_err(|_| resource_limit())?,
                    });
                }
                Ok(PreparedMutation {
                    participants,
                    result,
                    original: operation,
                })
            })
            .await?;
        if let (Some(scope), Some(admission)) = (self.scope, admission)
            && let Some(decision) = self
                .try_commit_timestamp_point(authority, scope, txn, admission, &prepared)
                .await?
        {
            return Ok(decision);
        }
        let point = match prepared.participants.as_slice() {
            [point] if PointManifest::qualifies(&point.plan, &prepared.result) => Some(point),
            _ => None,
        };
        let installed = if let Some(point) = point {
            self.status
                .try_install_point_manifest(PointManifest {
                    authority,
                    txn,
                    request,
                    read_ts,
                    plan: point.plan.clone(),
                    result: prepared.result.clone(),
                })
                .await
                .map_err(|error| stage(error, "point manifest"))?
        } else {
            None
        };
        let (manifest, result_digest) = if let Some(installed) = installed {
            installed
        } else {
            let mut draft = self
                .status
                .begin_manifest(
                    authority,
                    txn,
                    request,
                    read_ts,
                    prepared.participants.len() as u64,
                    prepared.result.clone(),
                )
                .await
                .map_err(|error| stage(error, "begin manifest"))?;
            let mut previous_root = draft.initial_root;
            for (index, page) in prepared.participants.chunks(128).enumerate() {
                draft = self
                    .status
                    .append_manifest_page(
                        authority,
                        txn,
                        index as u64,
                        previous_root,
                        Page::new(page.iter().map(|part| part.plan.clone()).collect())
                            .map_err(|_| resource_limit())?,
                    )
                    .await?;
                previous_root = draft.current_root;
            }
            let manifest = self
                .status
                .seal_manifest(authority, txn)
                .await
                .map_err(|error| stage(error, "seal manifest"))?;
            (manifest, draft.result_digest)
        };
        #[cfg(test)]
        let retry_participants = format!("retry-participants-{}", txn.attempt);
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint(
            if txn.attempt == 1 {
                "participants"
            } else {
                &retry_participants
            },
            txn.request_id,
            &prepared
                .participants
                .iter()
                .map(|part| &part.plan)
                .collect::<Vec<_>>(),
        )
        .await;
        #[cfg(test)]
        if matches!(mode, CommitMode::Queued(_)) {
            use super::recovery_tests::index_holds::{Checkpoint, checkpoint};
            checkpoint(txn.request_id, Checkpoint::QueuedPrepare, || None).await;
        }
        let mut stale_prepare = false;
        let preparation = async {
            let mut point_permit = None;
            for participant in &prepared.participants {
                let mut previous = store::initial_root(txn, &participant.plan)?;
                for (index, writes) in participant.pages.iter().enumerate() {
                    let command = OwnedCommand::new_owned(
                        ParticipantCommand::PreparePage {
                            authority,
                            txn,
                            request,
                            manifest,
                            plan: participant.plan.clone(),
                            page_index: index as u64,
                            previous_root: previous,
                            writes: writes.clone(),
                            guards: participant.guards.clone(),
                        },
                        &self.shared,
                        ByteClass::Data,
                    )?;
                    let reply = participant.participant.prepare_page(command).await?;
                    if !matches!(reply.reply, ParticipantReply::PreparedPage { .. }) {
                        stale_prepare = matches!(
                            reply.reply,
                            ParticipantReply::Rejected(store::Fault::Conflict)
                        );
                        return Err(conflict());
                    }
                    #[cfg(test)]
                    super::recovery_tests::transaction_checkpoint(
                        &format!("prepare-{}-{index}", participant.plan.group.group_id),
                        txn.request_id,
                        &reply,
                    )
                    .await;
                    previous = store::page_root(previous, index as u64, writes)?;
                }
                let reply = participant
                    .participant
                    .seal(authority, txn, manifest, result_digest)
                    .await?;
                let ParticipantReply::Sealed(seal) = &reply.reply else {
                    return Err(conflict());
                };
                if seal.txn != txn
                    || seal.manifest != manifest
                    || seal.participant != participant.plan
                    || seal.result_digest != result_digest
                {
                    return Err(unknown());
                }
                // Status obtains a fresh quorum proof of the durable seal.
                // A local Status handle reads it directly from the participant.
                if point.is_some() {
                    point_permit = Some(
                        self.status
                            .prepare_point_permit(
                                authority,
                                &participant.participant,
                                txn,
                                manifest,
                            )
                            .await?,
                    );
                } else {
                    self.status
                        .record_participant_prepared(
                            authority,
                            &participant.participant,
                            txn,
                            manifest,
                        )
                        .await?;
                }
                #[cfg(test)]
                super::recovery_tests::transaction_checkpoint(
                    &format!("seal-{}", participant.plan.group.group_id),
                    txn.request_id,
                    seal,
                )
                .await;
            }
            Ok::<_, ProtocolError>(point_permit)
        }
        .await;
        #[cfg(test)]
        let preparation = if preparation.is_ok() {
            super::recovery_tests::pause("prepared", txn.request_id).await;
            super::recovery_tests::index_holds::prepared(txn).await;
            if super::recovery_tests::refuse_prepared_commit(txn.request_id) {
                Err(conflict())
            } else {
                preparation
            }
        } else {
            preparation
        };
        let decision = match preparation {
            Err(error) => {
                let disk_refusal = crate::oltp::pool::is_disk_reservation_refusal(&error);
                if recoverable
                    && !disk_refusal
                    && (error.outcome == Outcome::Unknown
                        || matches!(
                            error.code,
                            ErrorCode::StorageUnavailable
                                | ErrorCode::Overloaded
                                | ErrorCode::OutcomeUnknown
                        ))
                {
                    return Err(error);
                }
                eprintln!(
                    "nabedra-db: transaction {} preparation failed: {:?}: {}",
                    txn.request_id, error.code, error.message
                );
                let mut reason = match error.code {
                    _ if disk_refusal => AbortReason::ResourceLimit,
                    ErrorCode::Overloaded | ErrorCode::LimitExceeded => AbortReason::ResourceLimit,
                    _ => AbortReason::Conflict,
                };
                let mut error = error;
                if stale_prepare && let Some(switch) = retry_switch {
                    if txn.attempt < MAX_QUEUED_MUTATION_ATTEMPTS {
                        if switch.queued_snapshot_retry_enabled().await {
                            reason = AbortReason::RetrySnapshot;
                        }
                    } else {
                        error = ProtocolError::new(
                            ErrorCode::Conflict,
                            "transaction preparation conflicted after exhausting snapshot retries",
                            Outcome::NotApplied,
                        );
                    }
                }
                self.status.abort(authority, txn, reason, error).await?
            }
            Ok(point_permit) => {
                let permit = match point_permit {
                    Some(permit) => permit.map_err(|error| stage(error, "commit permit"))?,
                    None => self
                        .status
                        .commit_permit(authority, txn)
                        .await
                        .map_err(|error| stage(error, "commit permit"))?,
                };
                let stamp = self
                    .timestamps
                    .allocate_commit(permit.clone())
                    .await
                    .map_err(|error| stage(error, "commit timestamp"))?;
                #[cfg(test)]
                super::recovery_tests::transaction_checkpoint(
                    "commit-stamp",
                    txn.request_id,
                    &stamp,
                )
                .await;
                self.status
                    .decide_commit(authority, permit, stamp)
                    .await
                    .map_err(|error| stage(error, "commit decision"))?
            }
        };
        let statement = decision.statement();
        if statement.txn != txn || statement.request != request || statement.manifest != manifest {
            return Err(unknown());
        }
        let retry = statement.decision.retries_snapshot();
        #[cfg(test)]
        let retry_aborted = format!("retry-aborted-{}", txn.attempt);
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint(
            if retry { &retry_aborted } else { "decision" },
            txn.request_id,
            decision.statement(),
        )
        .await;
        #[cfg(test)]
        if matches!(decision.statement().decision, TxnDecision::Commit { .. }) {
            super::recovery_tests::observe_decision("decision", decision.statement());
            super::recovery_tests::pause("committed", txn.request_id).await;
        }
        #[cfg(test)]
        super::recovery_tests::index_holds::decided(txn).await;
        // An acknowledged decision survives a stopped participant. Its fixed
        // intents and durable lifecycle guards remain discoverable for helpers.
        // A failed Resolve never changes COMMIT into an application failure.
        let resolved = async {
            for participant in &prepared.participants {
                self.resolve_participant(
                    authority,
                    &decision,
                    &participant.participant,
                    participant.plan.write_count,
                )
                .await?;
            }
            Ok::<(), ProtocolError>(())
        }
        .await;
        if let Err(error) = &resolved
            && (self.scope.is_some() || retry)
        {
            let mut error = error.clone();
            error.outcome = Outcome::Unknown;
            return Err(error);
        }
        if resolved.is_ok() {
            let settled = self.status.settled_proof(txn, manifest).await?;
            for guard in prepared.original.lifecycle {
                let released = self
                    .catalog
                    .release_lifecycle(authority, guard, settled.clone())
                    .await?;
                if (self.scope.is_some() || retry)
                    && !matches!(
                        released.reply,
                        ParticipantReply::Lifecycle(store::lifecycle::LifecycleReply::Released)
                    )
                {
                    return Err(unknown());
                }
            }
        }
        Ok(decision)
    }
    async fn release_scoped_guard(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
        settled: VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        if let Some(guard) = self.catalog.scoped_lifecycle_guard(scope, txn).await? {
            let released = self
                .catalog
                .release_lifecycle(authority, guard, settled)
                .await?;
            if !matches!(
                released.reply,
                ParticipantReply::Lifecycle(store::lifecycle::LifecycleReply::Released)
            ) {
                return Err(unknown());
            }
        }
        Ok(())
    }

    /// A fresh fenced owner may help immutable decisions or abandon an older
    /// undecided attempt. It never reconstructs writes from a client body.
    pub(crate) async fn recover(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        self.recover_queued(authority, txn, None).await
    }

    /// A surviving queue copy lets a same-run owner retry a sealed attempt
    /// after Status has decided its abort and the participants have settled.
    pub(super) async fn recover_queued(
        &self,
        authority: Authority,
        txn: TxnId,
        retry_control: Option<&ControlHandle>,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        if let Some(scope) = self.scope {
            match self.recover_timestamp_point(authority, scope, txn).await? {
                point::PointRecovery::Legacy => {}
                point::PointRecovery::Recorded(decision) => return Ok(decision),
            }
        }
        let Some(state) = self.status.inspect_transaction(txn).await? else {
            if let Some(scope) = self.scope {
                let terminal = self
                    .status
                    .lookup_identity(txn.request_id, Some(scope.request.digest))
                    .await?;
                if matches!(terminal,
                    Some(IdentityLookup::Terminal(TerminalIdentity::Failed { request, .. }))
                    if request == scope.request)
                {
                    let settled = self
                        .status
                        .failure_settled_proof(txn, scope.request)
                        .await?;
                    self.release_scoped_guard(authority, scope, txn, settled.clone())
                        .await?;
                    return Ok(Some(
                        self.status
                            .lookup_decision(txn, settled.statement().manifest)
                            .await?
                            .ok_or_else(unknown)?,
                    ));
                }
                if !matches!(terminal, Some(IdentityLookup::Reserved(_))) {
                    return Err(unknown());
                }
            }
            return Ok(None);
        };
        if state.current_authority != authority {
            return Err(unknown());
        }
        let decision = if let Some(statement) = state.decision {
            self.status
                .lookup_decision(txn, statement.manifest)
                .await?
                .ok_or_else(unknown)?
        } else if authority.owner_term > state.created_authority.owner_term {
            // This also closes unsealed drafts. A newer fenced owner cannot
            // infer missing writes or rerun an abandoned application request.
            let _ = retry_control;
            self.status.abandon_fenced(authority, txn).await?
        } else {
            return Ok(None);
        };
        if let Some(scope) = self.scope
            && (decision.statement().request != scope.request || decision.statement().txn != txn)
        {
            return Err(unknown());
        }
        self.resolve(authority, &decision).await?;
        if let Some(scope) = self.scope {
            let settled = self
                .status
                .settled_proof(txn, decision.statement().manifest)
                .await?;
            self.release_scoped_guard(authority, scope, txn, settled)
                .await?;
        }
        #[cfg(test)]
        if decision.statement().decision.retries_snapshot() {
            super::recovery_tests::observe_decision(
                &format!("retry-aborted-{}", txn.attempt),
                decision.statement(),
            );
        }
        #[cfg(test)]
        super::recovery_tests::observe_decision("resolved", decision.statement());
        Ok(Some(decision))
    }
    pub(crate) async fn resolve(
        &self,
        authority: Authority,
        decision: &VerifiedStatusDecision,
    ) -> Result<(), ProtocolError> {
        let statement = decision.statement();
        let mut after = None;
        loop {
            let page = self
                .status
                .manifest_page(statement.txn, statement.manifest, after)
                .await?;
            for plan in page.items.iter() {
                let participant = self.resolver.participant(&plan.group).await?;
                self.resolve_participant(authority, decision, &participant, plan.write_count)
                    .await?;
            }
            if page.done {
                break;
            }
            after = page.after;
        }
        Ok(())
    }
    async fn resolve_participant(
        &self,
        authority: Authority,
        decision: &VerifiedStatusDecision,
        participant: &ParticipantHandle,
        write_count: u64,
    ) -> Result<(), ProtocolError> {
        let statement = decision.statement();
        let retry = statement.decision.retries_snapshot();
        let mut last_ordinal = 0;
        loop {
            // Each applied page persists its ordinal. A slow page has a
            // deadline, while a large valid batch may continue making progress.
            let page = participant.resolve(authority, decision.clone(), 128);
            let result = if retry {
                nabedra_rt::time::timeout(RETRY_PROGRESS_TIMEOUT, page)
                    .await
                    .map_err(|_| unknown())??
            } else {
                page.await?
            };
            match result.reply {
                ParticipantReply::Resolved { complete: true, .. } => break,
                ParticipantReply::Resolved {
                    complete: false,
                    next_ordinal,
                } if next_ordinal > last_ordinal && next_ordinal < write_count => {
                    last_ordinal = next_ordinal;
                }
                _ => return Err(unknown()),
            }
        }
        #[cfg(test)]
        let resolved = participant
            .strong_resolution(statement.txn, statement.manifest)
            .await?
            .evidence()
            .clone();
        self.status
            .record_participant_resolved(authority, participant, statement.txn, statement.manifest)
            .await?;
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint(
            &format!("resolve-{}", participant.identity().group_id),
            statement.txn.request_id,
            &resolved,
        )
        .await;
        Ok(())
    }
}
fn scope_mismatch() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "transaction differs from scoped request",
        Outcome::NotApplied,
    )
}
fn check_scope(
    authority: Authority,
    scope: ScopedCatalogRead,
    txn: TxnId,
) -> Result<(), ProtocolError> {
    if authority.run_id != scope.queue_view.run_id
        || authority.run_id != scope.request.run_id
        || txn.request_id != scope.request.request_id
        || txn.attempt != 1
    {
        return Err(scope_mismatch());
    }
    Ok(())
}
fn resource_limit() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::LimitExceeded,
        "fixed transaction page exceeds admitted resources",
        Outcome::NotApplied,
    )
}
fn conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "transaction preparation conflicted with an existing version",
        Outcome::NotApplied,
    )
}
fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "transaction decision requires reconciliation",
        Outcome::Unknown,
    )
}

fn stage(mut error: ProtocolError, phase: &str) -> ProtocolError {
    error.message = format!("{phase}: {}", error.message);
    error
}
