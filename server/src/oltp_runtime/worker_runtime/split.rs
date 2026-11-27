//! Manual shard splits execute over the registered worker's live quorum services.
use super::*;
use crate::{
    oltp::mvcc::{catalog::PinnedCatalog, planner::split::PlannedSplit},
    oltp_replication::{
        authority::{
            status::{RequestKind, StatusReply, VerifiedSettlement, VerifiedStatusDecision},
            timestamp::{
                TimestampReply,
                split::{SplitAttempt, SplitIntent, SplitPlacement, SplitTimestampCommand},
            },
        },
        manager::shard_split::{SplitClient, SplitCursor},
    },
};
use serde::{Deserialize, Serialize};

const PAGES_PER_STEP: usize = 16;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    source: GroupIdentity,
    boundary: protocol::Key,
    page_bytes: usize,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub(crate) enum SplitResult {
    Importing {
        pages: u64,
    },
    Published {
        at: CommitTs,
        children: [GroupIdentity; 2],
    },
}
impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_split_admin(
        operation: &str,
        payload: &str,
    ) -> Result<bool, ProtocolError> {
        if operation == "storage.shard.split.status" {
            return if payload.trim() == "{}" {
                Ok(true)
            } else {
                Err(invalid())
            };
        }
        if operation != "storage.shard.split" {
            return Ok(false);
        }
        let request: Request = serde_json::from_str(payload).map_err(|_| invalid())?;
        let intent = SplitIntent {
            split_id: Uuid::from_u128(1),
            source: request.source,
            boundary: request.boundary,
            page_bytes: request.page_bytes,
            automatic: None,
        };
        intent.validate()?;
        Ok(true)
    }
    pub(in crate::oltp_runtime) async fn split_admin(
        self: &Arc<Self>,
        split_id: Uuid,
        payload: &str,
    ) -> Result<EncodedResponse, ProtocolError> {
        let request: Request = serde_json::from_str(payload).map_err(|_| invalid())?;
        let result = self
            .split_shard(SplitIntent {
                split_id,
                source: request.source,
                boundary: request.boundary,
                page_bytes: request.page_bytes,
                automatic: None,
            })
            .await?;
        let published = matches!(result, SplitResult::Published { .. });
        Ok(response::encode_success(split_id, &result, published))
    }

    pub(crate) async fn split_shard(
        &self,
        intent: SplitIntent,
    ) -> Result<SplitResult, ProtocolError> {
        self.split_with_placement(intent, None).await
    }

    pub(super) async fn split_with_placement(
        &self,
        intent: SplitIntent,
        placement: Option<&SplitPlacement>,
    ) -> Result<SplitResult, ProtocolError> {
        intent.validate()?;
        if intent.source.cluster_id != self.services.scope.cluster_id
            || intent.source.database != self.services.scope.database
        {
            return Err(invalid());
        }
        let client = self.services.split.as_ref().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "manual shard split requires registered worker placement",
                Outcome::NotApplied,
            )
        })?;
        let gate = self.gate(intent.split_id)?;
        let _split = gate.lock().await;
        let current = self.services.control.current(&self.services.scope).await?;
        if current.owner != self.services.local || !self.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let authority = current.ready.view.authority();
        let split_id = intent.split_id;
        let attempt = match client.job(split_id).await? {
            Some(attempt) => attempt,
            None => {
                let reservation = match self
                    .services
                    .status
                    .reserve_identity(authority, split_id, intent.digest()?, RequestKind::Mutation)
                    .await?
                    .reply
                {
                    StatusReply::Identity(IdentityLookup::Reserved(reservation)) => reservation,
                    _ => return Err(invalid()),
                };
                client
                    .timestamp(SplitTimestampCommand::Open {
                        authority,
                        intent: intent.clone(),
                        catalog_group_id: self.services.catalog.participant().identity().group_id,
                        reservation,
                        pin_id: nabedra_rt::ids::uuid_v4(),
                    })
                    .await?;
                client.job(split_id).await?.ok_or_else(unknown)?
            }
        };
        if attempt.intent != intent {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "split ID already binds different parameters",
                Outcome::NotApplied,
            ));
        }
        if let Some(decision) = attempt.closed {
            return self.split_result(&attempt, decision).await;
        }
        if let Some(decision) = self.transactions.recover(authority, attempt.txn).await? {
            return self
                .settle_split(client, &attempt, authority, decision)
                .await;
        }
        if let Some(IdentityLookup::Terminal(TerminalIdentity::Failed { request, .. })) =
            self.durable_lookup(split_id).await?
        {
            if request != attempt.request {
                return Err(unknown());
            }
            let proof = self
                .services
                .status
                .failure_settled_proof(attempt.txn, attempt.request)
                .await?;
            return self
                .settle_split_proof(client, &attempt, authority, proof)
                .await;
        }
        if attempt.authority != authority {
            if attempt.intent.automatic.is_some()
                || attempt.authority.run_id != authority.run_id
                || attempt.authority.owner_term >= authority.owner_term
            {
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "split attempt belongs to a fenced scheduler owner",
                    Outcome::Unknown,
                ));
            }
            // The current owner has already reconciled any durable decision
            // above. The fence prevents the old owner from creating one now.
            // A retry of an interrupted manual split can settle its abort.
            let error = crate::oltp::failure(
                ErrorCode::Conflict,
                "manual split interrupted by worker recovery",
            );
            self.services
                .status
                .terminal_failure(authority, attempt.request, error)
                .await?;
            let proof = self
                .services
                .status
                .failure_settled_proof(attempt.txn, attempt.request)
                .await?;
            return self
                .settle_split_proof(client, &attempt, authority, proof)
                .await;
        }
        let needs_placement = attempt.children().is_err();
        if let Some(placement) = placement {
            if needs_placement {
                client
                    .verify_capacity(placement, authority, intent.page_bytes)
                    .await?;
            } else if attempt.placement() != Some(placement) {
                return Err(unknown());
            }
        }
        let pin = self
            .services
            .catalog
            .snapshot(attempt.snapshot.read_ts, authority)?;
        let plan = match self.plan_split(&attempt, &pin).await {
            Ok(plan) => plan,
            Err(error)
                if attempt.image.is_none()
                    && matches!(
                        error.code,
                        ErrorCode::ValidationError | ErrorCode::NotFound | ErrorCode::Conflict
                    ) =>
            {
                self.services
                    .status
                    .terminal_failure(authority, attempt.request, error.clone())
                    .await?;
                let proof = self
                    .services
                    .status
                    .failure_settled_proof(attempt.txn, attempt.request)
                    .await?;
                self.close_split(client, &attempt, authority, proof).await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let guard = plan.guard.clone().into_sealed();
        let cut = if let Some(image) = &attempt.image {
            if image.guard != guard {
                return Err(unknown());
            }
            image.pin
        } else {
            // RANGES-1: the barrier comes before the cut, so no direct commit
            // stamped after the cut applies before the freeze. An older source
            // worker cannot decode the request: split without it, as before.
            let mut paused = client.pause(&attempt).await;
            // A late copy of a settled attempt's pause may hold the source:
            // release that one and take the barrier again.
            if paused.as_ref().is_err_and(|error| error.code == ErrorCode::Conflict)
                && self
                    .release_settled_barrier(client, &attempt.intent.source, authority)
                    .await?
            {
                paused = client.pause(&attempt).await;
            }
            match paused {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::ValidationError | ErrorCode::UnsupportedOperation
                    ) =>
                {
                    tracing::warn!(%split_id, error = %error.message, "split without the direct barrier");
                }
                Err(error) => return Err(error),
            }
            snapshot(
                client
                    .timestamp(SplitTimestampCommand::Cut {
                        authority,
                        split_id,
                        guard,
                        pin_id: nabedra_rt::ids::uuid_v4(),
                    })
                    .await?,
            )?
        };
        let children = plan.bindings(cut.read_ts)?;
        if needs_placement {
            client
                .timestamp(SplitTimestampCommand::Bind {
                    authority,
                    split_id,
                    children: Box::new(children.clone()),
                    placement: placement.cloned().map(Box::new),
                })
                .await?;
        } else if attempt.children()? != &children {
            return Err(unknown());
        }
        let attempt = client.job(split_id).await?.ok_or_else(unknown)?;
        client.freeze(&attempt).await?;
        for child in &children {
            // Rechecking exact placement also resumes a lost hosting/fence or
            // Control registration reply after the binding became durable.
            client
                .place(
                    &self.services.control,
                    &self.services.operator,
                    &attempt,
                    child,
                )
                .await?;
            client.stage(child).await?;
        }
        for _ in 0..PAGES_PER_STEP {
            let left = client.progress(&children[0]).await?;
            let right = client.progress(&children[1]).await?;
            let result = match compare_progress(&left, &right)? {
                Progress::Complete => {
                    #[cfg(test)]
                    super::super::recovery_tests::pause("split_children_imported", split_id).await;
                    let source = client.source(&attempt).await?;
                    let left = client.imported(&children[0]).await?;
                    let right = client.imported(&children[1]).await?;
                    let owner = Arc::new(
                        self.services
                            .shared
                            .reserve_scratch(ByteClass::Data, 4 << 20)?,
                    );
                    let mutation = self
                        .planner
                        .plan_publish_split(
                            &pin,
                            attempt.txn,
                            attempt.request,
                            plan.guard,
                            source,
                            left,
                            right,
                            cut,
                            owner,
                        )
                        .await
                        .map_err(split_plan_error)?;
                    let decision = self
                        .transactions
                        .commit_recoverable(
                            authority,
                            attempt.txn,
                            attempt.request,
                            pin.read_ts,
                            mutation,
                        )
                        .await?;
                    return self
                        .settle_split(client, &attempt, authority, decision)
                        .await;
                }
                Progress::Left => client.advance(&children[0], left).await,
                Progress::Right => client.advance(&children[1], right).await,
                Progress::Both => {
                    async {
                        client.advance(&children[0], left).await?;
                        #[cfg(test)]
                        if lose_left_reply(split_id) {
                            return Err(unknown());
                        }
                        client.advance(&children[1], right).await
                    }
                    .await
                }
            };
            if let Err(error) = result {
                if error.outcome == Outcome::NotApplied && error.code == ErrorCode::LimitExceeded {
                    self.services
                        .status
                        .terminal_failure(authority, attempt.request, error)
                        .await?;
                    let proof = self
                        .services
                        .status
                        .failure_settled_proof(attempt.txn, attempt.request)
                        .await?;
                    return self
                        .settle_split_proof(client, &attempt, authority, proof)
                        .await;
                }
                return Err(error);
            }
        }
        let left = client.progress(&children[0]).await?;
        let right = client.progress(&children[1]).await?;
        Ok(SplitResult::Importing {
            pages: left.pages.min(right.pages),
        })
    }

    /// RANGES-1: release the source's direct barrier when the split holding
    /// it is already settled (a late copy of its pause). Returns whether it
    /// released one. The owner's sweep does the same for an idle source.
    pub(super) async fn release_settled_barrier(
        &self,
        client: &SplitClient,
        source: &GroupIdentity,
        authority: Authority,
    ) -> Result<bool, ProtocolError> {
        use crate::oltp_replication::manager::shard_split::Usage;
        let Some(record) = self.services.control.group(source.group_id).await? else {
            return Ok(false);
        };
        let placement = client.source_placement(&record, authority).await?;
        let Usage::Frozen { split_id, .. } =
            client.observe_usage(&placement, authority, u64::MAX, None).await?
        else {
            return Ok(false);
        };
        let Some(holder) = client.job(split_id).await? else {
            return Ok(false);
        };
        if holder.closed.is_none() || holder.intent.source != *source {
            return Ok(false);
        }
        client.release(&holder).await?;
        Ok(true)
    }

    async fn plan_split(
        &self,
        attempt: &SplitAttempt,
        pin: &PinnedCatalog,
    ) -> Result<PlannedSplit, ProtocolError> {
        for _ in 0..8 {
            match self
                .planner
                .begin_split(
                    pin,
                    attempt.txn,
                    attempt.request,
                    &attempt.intent.source,
                    attempt.intent.boundary.clone(),
                    attempt.intent.split_id,
                    attempt.intent.automatic.map(|origin| origin.epoch),
                )
                .await
            {
                Ok(plan) => return Ok(plan),
                Err(PlanError::Blocked(intent)) => self.help_intent(intent).await?,
                Err(PlanError::LifecyclePending(binding)) => {
                    if let Some(holder) = self
                        .services
                        .catalog
                        .participant()
                        .lifecycle_holder(binding.table_id)
                        .await?
                    {
                        self.help_lifecycle(holder).await?;
                    }
                    nabedra_rt::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(split_plan_error(error)),
            }
        }
        Err(unavailable())
    }
    pub(super) async fn settle_split(
        &self,
        client: &SplitClient,
        attempt: &SplitAttempt,
        authority: Authority,
        decision: VerifiedStatusDecision,
    ) -> Result<SplitResult, ProtocolError> {
        let statement = decision.statement();
        if statement.txn != attempt.txn || statement.request != attempt.request {
            return Err(unknown());
        }
        self.transactions.resolve(authority, &decision).await?;
        let proof = self
            .services
            .status
            .settled_proof(attempt.txn, statement.manifest)
            .await?;
        self.settle_split_proof(client, attempt, authority, proof)
            .await
    }
    pub(super) async fn settle_split_proof(
        &self,
        client: &SplitClient,
        attempt: &SplitAttempt,
        authority: Authority,
        proof: VerifiedSettlement,
    ) -> Result<SplitResult, ProtocolError> {
        let sealed = proof.clone().into_sealed();
        let statement = sealed.statement();
        if statement.txn != attempt.txn || statement.request != attempt.request {
            return Err(unknown());
        }
        let aborted = matches!(statement.decision.decision, TxnDecision::Abort { .. });
        if aborted && attempt.image.as_ref().is_none_or(|cut| cut.children.is_none()) {
            // RANGES-1: an attempt aborted before its children were bound may
            // hold the source's direct barrier. The abort's FinishSource
            // releases it and records the settlement, so a late pause of this
            // attempt is refused. A source that cannot take it holds none.
            match client.finish(attempt, statement.manifest).await {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.code,
                        ErrorCode::ValidationError
                            | ErrorCode::UnsupportedOperation
                            | ErrorCode::NotFound
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        if let Some(cut) = &attempt.image {
            if cut.children.is_some() {
                client.finish(attempt, statement.manifest).await?;
            }
            if let Some(guard) = self
                .services
                .catalog
                .participant()
                .lifecycle_guard(cut.guard.binding())
                .await?
            {
                self.services
                    .catalog
                    .participant()
                    .release_lifecycle(authority, guard, proof.clone())
                    .await?;
            }
        } else if let GroupKind::Data { table_id, .. } = attempt.intent.source.kind
            && let Some(binding) = self
                .services
                .catalog
                .participant()
                .lifecycle_holder(table_id)
                .await?
            && binding.txn == attempt.txn
            && binding.request == attempt.request
        {
            self.help_lifecycle(binding).await?;
        }
        self.close_split(client, attempt, authority, proof).await?;
        self.split_result(attempt, statement.decision.decision)
            .await
    }
    async fn close_split(
        &self,
        client: &SplitClient,
        attempt: &SplitAttempt,
        authority: Authority,
        proof: VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        match client
            .timestamp(SplitTimestampCommand::Close {
                authority,
                split_id: attempt.intent.split_id,
                settled: proof.into_sealed(),
            })
            .await?
        {
            TimestampReply::Released => Ok(()),
            _ => Err(unknown()),
        }
    }
    async fn split_result(
        &self,
        attempt: &SplitAttempt,
        decision: TxnDecision,
    ) -> Result<SplitResult, ProtocolError> {
        match decision {
            TxnDecision::Commit { commit_ts } => Ok(SplitResult::Published {
                at: commit_ts,
                children: attempt.children()?.clone().map(|binding| binding.target),
            }),
            TxnDecision::Abort { .. } => {
                match self.durable_lookup(attempt.intent.split_id).await? {
                    Some(IdentityLookup::Terminal(TerminalIdentity::Failed {
                        request,
                        outcome,
                        ..
                    })) if request == attempt.request => {
                        Err(outcome.result().err().unwrap_or_else(unknown))
                    }
                    Some(IdentityLookup::Terminal(TerminalIdentity::Committed {
                        decision: settled,
                        outcome,
                        ..
                    })) if settled.txn == attempt.txn
                        && settled.request == attempt.request
                        && settled.decision == decision =>
                    {
                        Err(outcome.result().err().unwrap_or_else(unknown))
                    }
                    _ => Err(unknown()),
                }
            }
        }
    }
}
#[cfg(test)]
fn lose_left_reply(split_id: Uuid) -> bool {
    std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR").is_some_and(|directory| {
        std::fs::remove_file(
            std::path::PathBuf::from(directory).join(format!("split-lose-left-{split_id}")),
        )
        .is_ok()
    })
}
fn snapshot(reply: TimestampReply) -> Result<StoragePin, ProtocolError> {
    match reply {
        TimestampReply::Snapshot(pin) => Ok(pin),
        _ => Err(unknown()),
    }
}
enum Progress {
    Left,
    Right,
    Both,
    Complete,
}
fn compare_progress(left: &SplitCursor, right: &SplitCursor) -> Result<Progress, ProtocolError> {
    if left == right {
        return Ok(if left.complete {
            Progress::Complete
        } else {
            Progress::Both
        });
    }
    if !left.complete && left.pages.checked_add(1) == Some(right.pages) {
        return Ok(Progress::Left);
    }
    if !right.complete && right.pages.checked_add(1) == Some(left.pages) {
        return Ok(Progress::Right);
    }
    Err(ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "split sibling cursors disagree",
        Outcome::Unknown,
    ))
}
fn split_plan_error(error: PlanError) -> ProtocolError {
    match error {
        PlanError::Protocol(error) => error,
        PlanError::Guarded { cause, .. } => split_plan_error(*cause),
        PlanError::Blocked(_) | PlanError::LifecyclePending(_) => unavailable(),
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid manual shard split request",
        Outcome::NotApplied,
    )
}
