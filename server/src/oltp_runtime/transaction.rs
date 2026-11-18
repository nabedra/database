//! Cross-group mutation publication. Status fixes the complete manifest and
//! exact outcome before any Prepare; COMMIT follows every strong participant
//! seal, and Resolve applies only the immutable Status decision.
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson, PreparedWrite,
            planner::{MutationPlan, ParticipantResolver},
            store::{
                self, ParticipantCommand, ParticipantHandle, ParticipantReply,
                lifecycle::SealedLifecycleGuard,
            },
        },
        pool::{ByteClass, ExecutionClass, WorkerSharedResources},
    },
    oltp_replication::{
        authority::{
            status::{PointManifest, StatusService, VerifiedStatusDecision},
            timestamp::TimestampService,
        },
        owned::OwnedCommand,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::sync::Arc;

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
pub(crate) struct TransactionCoordinator {
    shared: Arc<WorkerSharedResources>,
    status: Arc<dyn StatusService>,
    timestamps: Arc<dyn TimestampService>,
    resolver: Arc<dyn ParticipantResolver>,
    catalog: Arc<ParticipantHandle>,
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
        }
    }
    pub(crate) async fn commit(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        operation: MutationPlan,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        self.commit_inner(authority, txn, request, read_ts, operation, false)
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
        self.commit_inner(authority, txn, request, read_ts, operation, true)
            .await
    }
    async fn commit_inner(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        mut operation: MutationPlan,
        recoverable: bool,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
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
        if preparation.is_ok() {
            super::recovery_tests::pause("prepared", txn.request_id).await;
        }
        let decision = match preparation {
            Err(error) => {
                if recoverable
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
                self.status
                    .abort(
                        authority,
                        txn,
                        if error.code == ErrorCode::Overloaded
                            || error.code == ErrorCode::LimitExceeded
                        {
                            AbortReason::ResourceLimit
                        } else {
                            AbortReason::Conflict
                        },
                        error,
                    )
                    .await?
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
        #[cfg(test)]
        super::recovery_tests::transaction_checkpoint(
            "decision",
            txn.request_id,
            decision.statement(),
        )
        .await;
        #[cfg(test)]
        if matches!(decision.statement().decision, TxnDecision::Commit { .. }) {
            super::recovery_tests::observe_decision("decision", decision.statement());
            super::recovery_tests::pause("committed", txn.request_id).await;
        }
        // An acknowledged decision survives a stopped participant. Its fixed
        // intents and durable lifecycle guards remain discoverable for helpers.
        // A failed Resolve never changes COMMIT into an application failure.
        let resolved = async {
            for participant in &prepared.participants {
                self.resolve_participant(authority, &decision, &participant.participant)
                    .await?;
            }
            Ok::<(), ProtocolError>(())
        }
        .await;
        if resolved.is_ok() {
            let settled = self.status.settled_proof(txn, manifest).await?;
            for guard in prepared.original.lifecycle {
                self.catalog
                    .release_lifecycle(authority, guard, settled.clone())
                    .await?;
            }
        }
        Ok(decision)
    }
    /// A fresh fenced owner may help immutable decisions or abandon an older
    /// undecided attempt. It never reconstructs writes from a client body.
    pub(crate) async fn recover(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        let Some(state) = self.status.inspect_transaction(txn).await? else {
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
            self.status.abandon_fenced(authority, txn).await?
        } else {
            return Ok(None);
        };
        self.resolve(authority, &decision).await?;
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
                self.resolve_participant(authority, decision, &participant)
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
    ) -> Result<(), ProtocolError> {
        let statement = decision.statement();
        loop {
            let result = participant
                .resolve(authority, decision.clone(), 128)
                .await?;
            match result.reply {
                ParticipantReply::Resolved { complete: true, .. } => break,
                ParticipantReply::Resolved {
                    complete: false, ..
                } => {}
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
