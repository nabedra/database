use super::*;
use crate::oltp_replication::authority::timestamp::{
    ScopedCopyProof, ScopedRetirementMarker, ScopedTerminalStatement, ScopedTimestampEpoch,
};

const SCOPED_RETIREMENTS: &[u8] = b"status/scoped-retirement/";

/// Same-run application receipts need a copy marker in addition to the
/// transaction outcome. The marker carries no copy of the application result.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ScopedTerminalReceipt {
    pub marker: ScopedRetirementMarker,
    pub terminal: TerminalIdentity,
}

impl ScopedTerminalReceipt {
    pub(crate) fn validate(
        &self,
        epoch: Option<ScopedTimestampEpoch>,
        run: Uuid,
        group: Uuid,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<(), ProtocolError> {
        let statement = self.marker.proof.statement();
        self.marker.validate(statement)?;
        if statement.request.request_id != request_id
            || digest.is_some_and(|digest| digest != statement.request.digest)
            || statement.request.run_id != run
            || epoch
                .is_some_and(|epoch| statement.epoch != epoch && !epoch.advances(statement.epoch))
            || !matches_terminal(&self.terminal, statement, group)?
        {
            return Err(invalid());
        }
        Ok(())
    }
}

fn matches_terminal(
    terminal: &TerminalIdentity,
    statement: &ScopedTerminalStatement,
    group: Uuid,
) -> Result<bool, ProtocolError> {
    let (reservation, outcome) = match terminal {
        TerminalIdentity::ReadExpired {
            reservation,
            terminal_version,
            response_hash,
            ..
        } if reservation.kind == RequestKind::Read
            && *terminal_version == statement.terminal.version
            && *response_hash == statement.response_hash =>
        {
            (reservation, None)
        }
        TerminalIdentity::Committed {
            reservation,
            decision,
            outcome,
        } if reservation.kind == RequestKind::Mutation
            && decision.txn.request_id == statement.request.request_id
            && decision.txn.attempt == 1
            && decision.manifest.status_group_id == group
            && (!matches!(
                decision.decision,
                TxnDecision::Abort {
                    reason: AbortReason::AbandonedByFence
                }
            ) || outcome.is_not_applied()) =>
        {
            (reservation, Some(outcome))
        }
        TerminalIdentity::Failed {
            reservation,
            outcome,
            ..
        } if reservation.kind == RequestKind::Mutation => (reservation, Some(outcome)),
        _ => return Ok(false),
    };
    if !statement.matches_reservation(*reservation)
        || !reservation
            .scoped
            .is_some_and(|admission| admission.valid_for(group))
        || terminal.request_binding() != statement.request
    {
        return Ok(false);
    }
    if let Some(outcome) = outcome {
        let status = outcome
            .result()
            .err()
            .map_or(200, |error| error.status_code());
        return Ok(status == statement.response_status
            && outcome.response_hash(statement.request.request_id)? == statement.response_hash);
    }
    Ok(true)
}

pub(super) fn record(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    epoch: Option<ScopedStatusEpoch>,
    proof: &ScopedCopyProof,
) -> Result<Transition, ProtocolError> {
    let statement = proof.statement();
    // The active epoch and reservation tie the witnesses to the original run.
    // Cold cleanup cannot create a retirement marker for that run.
    if proof.validate().is_err()
        || epoch
            != Some(ScopedStatusEpoch {
                queue_group_id: statement.epoch.queue_group_id,
                queue_generation: statement.epoch.queue_generation,
                view: statement.epoch.view,
            })
    {
        return reject(tx, StatusFault::InvalidBinding);
    }
    let request_id = statement.request.request_id;
    let identity_key = key(IDENTITIES, request_id);
    let marker_key = key(SCOPED_RETIREMENTS, request_id);
    let Some(mut record) = load::<IdentityRecord>(tx, &identity_key)? else {
        return reject(tx, StatusFault::UnknownIdentity);
    };
    if record.restore_origin.is_some()
        || record.rejection.is_some()
        || !statement.matches_reservation(record.reservation)
    {
        return reject(tx, StatusFault::InvalidBinding);
    }
    verify_lookup_record(scope, &record, |decision, outcome| {
        transaction::verify_settled_terminal_in_apply(tx, decision, outcome)
    })?;
    if let Some(home) = load::<LocatedTimestampManifest>(tx, &home_key(request_id))? {
        let current = load::<Authority>(tx, STATE)?.ok_or_else(unknown)?;
        let binding = &home.locator.binding;
        if statement.pin_identity != binding.pin_identity
            || statement.storage_pin != binding.storage_pin
            || statement.request != binding.request
            || (statement.epoch != binding.original_epoch
                && !statement.epoch.advances(binding.original_epoch))
            || projection::verify_in_apply(tx, scope, &record, &home, current, Some(position))?
                .is_none()
        {
            return reject(tx, StatusFault::TerminalConflict);
        }
    }
    if let Some(existing) = load::<ScopedRetirementMarker>(tx, &marker_key)? {
        let statement = existing.proof.statement();
        existing.validate(statement)?;
        let terminal = record.terminal.as_ref().ok_or_else(unknown)?;
        if !matches_terminal(terminal, statement, scope.group_id)? {
            return Err(unknown());
        }
        if existing.proof == *proof {
            return commit(tx, StatusReply::ScopedRetired(existing));
        }
        if !proof.statement().rebases(statement) {
            return reject(tx, StatusFault::TerminalConflict);
        }
    }
    if record.terminal.is_none() && record.reservation.kind == RequestKind::Read {
        record.terminal = Some(TerminalIdentity::ReadExpired {
            reservation: record.reservation,
            request: statement.request,
            terminal_version: statement.terminal.version,
            response_hash: statement.response_hash,
            recorded_at: position,
        });
        put(tx, &identity_key, &record)?;
    }
    if let Some(terminal) = &record.terminal {
        if !matches_terminal(terminal, statement, scope.group_id)? {
            return reject(tx, StatusFault::TerminalConflict);
        }
    } else {
        return reject(tx, StatusFault::TerminalConflict);
    }
    let marker = ScopedRetirementMarker {
        proof: proof.clone(),
        applied_at: position,
    };
    put(tx, &marker_key, &marker)?;
    commit(tx, StatusReply::ScopedRetired(marker))
}

impl StatusHandle {
    pub(crate) async fn record_scoped_retirement(
        &self,
        proof: ScopedCopyProof,
    ) -> Result<ScopedRetirementMarker, ProtocolError> {
        let applied = self
            .submit(StatusCommand::RecordScopedRetirement {
                proof: proof.clone(),
            })
            .await?;
        match applied.reply {
            StatusReply::ScopedRetired(marker) if marker.proof == proof => {
                marker.validate(proof.statement())?;
                Ok(marker)
            }
            _ => Err(unknown()),
        }
    }

    pub(crate) async fn prior_scoped_receipt(
        &self,
        authority: Authority,
        run: Uuid,
        request_id: Uuid,
        digest: Option<Digest>,
        published_cold: bool,
    ) -> Result<ChargedBytes, ProtocolError> {
        if run.is_nil() || run == authority.run_id || request_id.is_nil() {
            return Err(invalid());
        }
        let barrier = self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        self.machine
            .store()
            .read(BUDGET, move |view| {
                if read::<Authority>(view, STATE)? != Some(authority) {
                    return Err(unknown());
                }
                let record: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                let home: Option<LocatedTimestampManifest> = read(view, &home_key(request_id))?;
                if let Some(record) = &record {
                    if !published_cold && record.restore_origin.is_none() {
                        return Err(unknown());
                    }
                    if record.reservation.request_id != request_id
                        || record.reservation.original_run != run
                        || record.reservation.scoped.is_none()
                        || digest.is_some_and(|digest| digest != record.reservation.digest)
                    {
                        return Err(invalid());
                    }
                    verify_lookup_record(&scope, record, |decision, outcome| {
                        transaction::verify_terminal_in_read(view, decision, outcome)
                    })?;
                    if let Some(home) = &home {
                        projection::verify_in_read(view, &scope, record, home, authority, Some(barrier.applied))?;
                    }
                } else if home.is_some() {
                    return Err(unknown());
                }
                view.encode(&record.map(|record| classify_home(authority, record, home.is_some())))
            })
            .await
    }

    pub(crate) async fn scoped_terminal_receipt(
        &self,
        epoch: Option<ScopedTimestampEpoch>,
        run: Uuid,
        request_id: Uuid,
        digest: Option<Digest>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        self.machine
            .store()
            .read(BUDGET, move |view| {
                let current: Option<Authority> = read(view, STATE)?;
                if current.is_none_or(|authority| authority.run_id != run) {
                    return Err(unknown());
                }
                let marker: Option<ScopedRetirementMarker> =
                    read(view, &key(SCOPED_RETIREMENTS, request_id))?;
                let Some(marker) = marker else {
                    return view.encode(&None::<ScopedTerminalReceipt>);
                };
                let status_epoch = epoch.unwrap_or(marker.proof.statement().epoch);
                let status_epoch = ScopedStatusEpoch {
                    queue_group_id: status_epoch.queue_group_id,
                    queue_generation: status_epoch.queue_generation,
                    view: status_epoch.view,
                };
                let current_epoch = read::<ScopedStatusEpoch>(view, &status_epoch.key())?;
                if !status_epoch.valid(current)
                    || !current_epoch.is_some_and(|stored| {
                        stored == status_epoch
                            || epoch.is_none()
                                && stored.view.owner_term > status_epoch.view.owner_term
                    })
                    || marker.applied_at.index > barrier.applied.index
                    || marker.applied_at.term > barrier.applied.term
                {
                    return Err(unknown());
                }
                let record: IdentityRecord =
                    read(view, &key(IDENTITIES, request_id))?.ok_or_else(unknown)?;
                if record.restore_origin.is_some() || record.reservation.request_id != request_id {
                    return Err(unknown());
                }
                verify_lookup_record(&scope, &record, |decision, outcome| {
                    transaction::verify_terminal_in_read(view, decision, outcome)
                })?;
                if let Some(home) = read::<LocatedTimestampManifest>(view, &home_key(request_id))? {
                    let current = current.ok_or_else(unknown)?;
                    let statement = marker.proof.statement();
                    let binding = &home.locator.binding;
                    if statement.pin_identity != binding.pin_identity
                        || statement.storage_pin != binding.storage_pin
                        || statement.request != binding.request
                        || (statement.epoch != binding.original_epoch
                            && !statement.epoch.advances(binding.original_epoch))
                        || projection::verify_in_read(view, &scope, &record, &home, current, Some(barrier.applied))?.is_none()
                    {
                        return Err(unknown());
                    }
                }
                let terminal = record.terminal.ok_or_else(unknown)?;
                let receipt = ScopedTerminalReceipt { marker, terminal };
                receipt.validate(epoch, run, scope.group_id, request_id, digest)?;
                view.encode(&Some(receipt))
            })
            .await
    }
}
