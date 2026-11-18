//! Retirement releases bounded RAM after Status records exact all-ready
//! terminal evidence. Each holder can finish cleanup after a lost reply.
use super::*;
use crate::oltp_replication::authority::status::{StatusService, VerifiedTerminalRetirement};

const PAGE: usize = 16;

impl WorkerRuntime {
    pub(super) async fn retire_expired(&self) -> Result<(), ProtocolError> {
        {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            let active = self.active.lock().map_err(|_| unknown())?;
            if !replica.has_retirement_work(now(), &active) {
                return Ok(());
            }
        }
        let current = self.services.control.current(&self.services.scope).await?;
        let _candidates = self
            .services
            .shared
            .reserve_scratch(ByteClass::Progress, 8192)?;
        if current.owner == self.services.local {
            self.reconcile_terminal_decisions(current).await?;
        }
        let candidates = candidates(&self.services.replica, &self.retirement_cursor)?;
        for (id, sequence) in candidates {
            advance_cursor(&self.retirement_cursor, sequence)?;
            let gate = self.gate(id)?;
            let Ok(_gate) = gate.try_lock() else { continue };
            if self.active.lock().map_err(|_| unknown())?.contains(&id) {
                continue;
            }
            if let Err(error) = self.retire_candidate(current, id).await {
                tracing::debug!(%id, code=?error.code, message=error.message,
                    "terminal record awaits retirement");
            }
        }
        Ok(())
    }

    async fn reconcile_terminal_decisions(
        &self,
        current: QueueAuthority,
    ) -> Result<(), ProtocolError> {
        let ids = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .reconciling_ids(PAGE);
        for id in ids {
            let gate = self.gate(id)?;
            let Ok(_gate) = gate.try_lock() else { continue };
            if self.active.lock().map_err(|_| unknown())?.contains(&id) {
                continue;
            }
            let Ok(Some(IdentityLookup::Terminal(terminal))) = self.durable_lookup(id).await else {
                continue;
            };
            if self.services.control.current(&self.services.scope).await? != current {
                return Err(QueueError::Stale.protocol());
            }
            let response = terminal_response(id, &terminal)?;
            self.services
                .queue
                .finish(id, response.status, response.body, now())
                .await?;
            self.changed.notify_waiters();
        }
        Ok(())
    }

    async fn retire_candidate(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<(), ProtocolError> {
        let (statement, owned) = {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica.authority() != current || !replica.is_ready_member() {
                return Err(QueueError::Stale.protocol());
            }
            (
                replica
                    .terminal_copy_statement(id)
                    .map_err(QueueError::protocol)?,
                replica.can_certify_retirement(current),
            )
        };
        let existing = self
            .services
            .status
            .retirement(id, statement.request.digest)
            .await?;
        let proof = match existing {
            Some(proof) => proof,
            None if owned => {
                let state = self.services.control.inspect().await?.ok_or_else(unknown)?;
                if state.owner != Some(self.services.local)
                    || state.ready != statement.ready
                    || state.pending.is_some()
                {
                    return Err(QueueError::Stale.protocol());
                }
                self.services.queue.ensure_copies(current, id).await?;
                let release = self.services.timestamps.release(statement).await?;
                #[cfg(test)]
                super::super::recovery_tests::pause("retirement_release", id).await;
                self.services.status.retire_terminal(release).await?
            }
            None => return Ok(()),
        };
        #[cfg(test)]
        super::super::recovery_tests::pause("retirement_certificate", id).await;
        apply(
            &self.services.replica,
            self.services.control.as_ref(),
            &self.services.scope,
            current,
            proof,
        )
        .await?;
        self.changed.notify_waiters();
        Ok(())
    }
}

pub(crate) fn candidates(
    replica: &Mutex<WorkerReplica>,
    cursor: &Mutex<Option<u64>>,
) -> Result<Vec<(Uuid, u64)>, ProtocolError> {
    let replica = replica.lock().map_err(|_| unknown())?;
    let mut cursor = cursor.lock().map_err(|_| unknown())?;
    let time = now();
    let mut page = replica.expired_terminals(time, *cursor, PAGE);
    if page.is_empty() && cursor.take().is_some() {
        page = replica.expired_terminals(time, None, PAGE);
    }
    Ok(page)
}

pub(crate) fn advance_cursor(
    cursor: &Mutex<Option<u64>>,
    sequence: u64,
) -> Result<(), ProtocolError> {
    *cursor.lock().map_err(|_| unknown())? = Some(sequence);
    Ok(())
}

pub(crate) async fn prune_replica(
    replica: &Mutex<WorkerReplica>,
    cursor: &Mutex<Option<u64>>,
    control: &dyn AuthorityReader,
    scope: &QueueScope,
    status: &dyn StatusService,
) -> Result<(), ProtocolError> {
    let _memory = replica
        .lock()
        .map_err(|_| unknown())?
        .shared_resources()
        .reserve_scratch(ByteClass::Progress, 8192)?;
    let candidates = candidates(replica, cursor)?;
    if candidates.is_empty() {
        return Ok(());
    }
    let current = control.current(scope).await?;
    for (id, sequence) in candidates {
        advance_cursor(cursor, sequence)?;
        let statement = replica
            .lock()
            .map_err(|_| unknown())?
            .terminal_copy_statement(id)
            .map_err(QueueError::protocol)?;
        if let Some(proof) = status.retirement(id, statement.request.digest).await? {
            apply(replica, control, scope, current, proof).await?;
        }
    }
    Ok(())
}

async fn apply(
    replica: &Mutex<WorkerReplica>,
    control: &dyn AuthorityReader,
    scope: &QueueScope,
    expected: QueueAuthority,
    proof: VerifiedTerminalRetirement,
) -> Result<(), ProtocolError> {
    let fresh = control.current(scope).await?;
    if fresh != expected {
        return Err(QueueError::Stale.protocol());
    }
    replica
        .lock()
        .map_err(|_| unknown())?
        .retire(fresh, proof, now())
        .map_err(QueueError::protocol)
}
