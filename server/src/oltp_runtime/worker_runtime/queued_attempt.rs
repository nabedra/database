use super::*;
use crate::oltp::mvcc::store::{ParticipantReply, lifecycle::LifecycleReply};
use crate::oltp_replication::authority::status::VerifiedStatusDecision;
use crate::oltp_runtime::transaction::RETRY_PROGRESS_TIMEOUT;
use std::time::Duration;

#[derive(Clone, Copy)]
pub(super) struct QueuedAttempt {
    pub(super) txn: TxnId,
    pub(super) pin: StoragePin,
}

impl QueuedAttempt {
    pub(super) fn original(id: Uuid, pin: StoragePin) -> Self {
        Self {
            txn: TxnId {
                request_id: id,
                attempt: 1,
            },
            pin,
        }
    }
}

impl WorkerRuntime {
    // Intent and lifecycle helpers can reach a fenced attempt before the
    // recovery actor. Use the same decision policy for an accepted RAM copy.
    pub(super) async fn recover_queued_transaction(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        let retained = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .record(txn.request_id)
            .is_some_and(|record| {
                record.run_id == authority.run_id
                    && record.accepted
                    && !record.state.terminal()
                    && !record.body.is_empty()
                    && record.pinned_catalog.is_some()
            });
        let retry_control = retained.then_some(self.services.control.as_ref());
        self.transactions
            .recover_queued(authority, txn, retry_control)
            .await
    }

    pub(super) async fn advance_queued_attempt(
        &self,
        current: QueueAuthority,
        identity: &PinIdentity,
        decision: &VerifiedStatusDecision,
    ) -> Result<QueuedAttempt, ProtocolError> {
        let statement = decision.statement();
        let txn = statement.txn;
        if !statement.decision.retries_snapshot() || txn.attempt >= MAX_QUEUED_MUTATION_ATTEMPTS {
            return Err(unknown());
        }

        // The original queue context and authorization identify every pin
        // and every attempt. A stale owner cannot advance the Timestamp view.
        nabedra_rt::time::sleep(Duration::from_millis(5 * txn.attempt)).await;
        let pin = nabedra_rt::time::timeout(RETRY_PROGRESS_TIMEOUT, async {
            let settled = self
                .services
                .status
                .settled_proof(txn, statement.manifest)
                .await?;
            self.services
                .timestamps
                .retry_snapshot(current.ready.view, identity.clone(), settled)
                .await
        })
        .await
        .map_err(|_| unknown())??;
        Ok(QueuedAttempt {
            txn: TxnId {
                attempt: txn.attempt + 1,
                ..txn
            },
            pin,
        })
    }

    // Resolve recovery by attempt until the first missing or live manifest.
    // A retry abort is visible to helpers but never becomes a queue result.
    pub(super) async fn recover_queued_attempt(
        &self,
        current: QueueAuthority,
        input: &DecodedInput,
        request: RequestBinding,
    ) -> Result<(QueuedAttempt, Option<VerifiedStatusDecision>), ProtocolError> {
        let read = input.context.read.as_ref().ok_or_else(unknown)?;
        let authority = current.ready.view.authority();
        let mut attempt = QueuedAttempt::original(input.request.request_id, read.storage_pin);
        loop {
            let decision = self
                .recover_queued_transaction(authority, attempt.txn)
                .await?;
            let Some(prior) = decision.as_ref() else {
                return Ok((attempt, None));
            };
            let statement = prior.statement();
            if statement.request != request || statement.txn != attempt.txn {
                return Err(unknown());
            }
            if !statement.decision.retries_snapshot() {
                return Ok((attempt, decision));
            }

            // The actor could have stopped between resolving the abort and
            // releasing its Catalog guard. Recover its original binding.
            let state = self
                .services
                .status
                .inspect_transaction(attempt.txn)
                .await?
                .ok_or_else(unknown)?;
            let pin = self
                .services
                .catalog
                .snapshot(attempt.pin.read_ts, authority)?;
            let binding = self
                .planner
                .queued_lifecycle_binding(
                    &input.request.operation,
                    &pin,
                    attempt.txn,
                    request,
                    state.created_authority,
                )
                .await
                .map_err(|_| unknown())?;
            let catalog = self.services.catalog.participant();
            if let Some(guard) = catalog.lifecycle_guard(&binding).await? {
                let settled = self
                    .services
                    .status
                    .settled_proof(attempt.txn, statement.manifest)
                    .await?;
                let released = catalog.release_lifecycle(authority, guard, settled).await?;
                if !matches!(
                    released.reply,
                    ParticipantReply::Lifecycle(LifecycleReply::Released)
                ) {
                    return Err(unknown());
                }
            }
            attempt = self
                .advance_queued_attempt(current, &read.identity, prior)
                .await?;
        }
    }
}
