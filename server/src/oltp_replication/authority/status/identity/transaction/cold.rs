use super::*;
use crate::oltp_replication::authority::timestamp::cold::ColdApplicationPin;

/// One unsettled transaction from a bounded native key scan. The cursor also
/// advances over settled records and records from the current run.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColdRecoveryPage {
    pub after: Option<TxnId>,
    pub transaction: Option<TxnId>,
    pub done: bool,
}

impl StatusHandle {
    pub(crate) async fn cold_application_pin_settled(
        &self,
        authority: Authority,
        pin: ColdApplicationPin,
    ) -> Result<ChargedBytes, ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let status_group = self.identity().clone();
        let status_group_id = status_group.group_id;
        self.machine
            .store()
            .read_metadata(BUDGET, move |view| {
                if read::<Authority>(view, STATE)? != Some(authority) {
                    return Err(unknown());
                }
                let reservation = pin.reservation;
                let identity: IdentityRecord =
                    read(view, &key(IDENTITIES, reservation.request_id))?.ok_or_else(invalid)?;
                if identity.reservation != reservation
                    || identity.restore_origin.is_some()
                    || pin.identity.nonce != reservation.reservation_id
                    || pin.identity.request_id != reservation.request_id
                    || pin.identity.digest != reservation.digest
                    || pin.identity.run_id != reservation.original_run
                    || pin.identity.run_id == authority.run_id
                    || identity.rejection.as_ref().is_some_and(|rejected| {
                        rejected.reservation != reservation
                            || rejected.identity != pin.identity
                            || rejected.pin.is_some_and(|saved| saved != pin.pin)
                            || rejected.request != pin.request
                    })
                    || pin.request.is_some_and(|request| {
                        request.run_id != reservation.original_run
                            || request.request_id != reservation.request_id
                            || request.digest != reservation.digest
                    })
                {
                    return Err(invalid());
                }
                if let Some(home) = read::<LocatedTimestampManifest>(view, &home_key(reservation.request_id))? {
                    let binding = &home.locator.binding;
                    let stored = projection::verify_in_read(
                        view, &status_group, &identity, &home, authority, Some(barrier.applied),
                    )?;
                    let safe = binding.reservation == reservation
                        && binding.pin_identity == pin.identity
                        && binding.storage_pin == pin.pin
                        && Some(binding.request) == pin.request
                        && stored.is_some();
                    return view.encode(&safe);
                }
                // Check every attempt against the original pin and request.
                // Retry aborts authorize no publication even if no later attempt started.
                let mut transaction: Option<SettlementFields> = None;
                let mut chain_valid = true;
                let progress = view.scan_page(
                    &transaction_prefix(reservation.request_id),
                    None,
                    MAX_QUEUED_MUTATION_ATTEMPTS as usize,
                    |key, raw| {
                        let fields: SettlementFields =
                            serde_json::from_slice(raw).map_err(|_| unknown())?;
                        if key != txn_key(b't', fields.txn, &[]) {
                            return Err(unknown());
                        }
                        let follows_prior = match &transaction {
                            None => fields.txn.attempt == 1 && fields.read_ts == pin.pin.read_ts,
                            Some(prior) => {
                                prior.txn.attempt.checked_add(1) == Some(fields.txn.attempt)
                                    && prior.read_ts < fields.read_ts
                                    && prior
                                        .decision
                                        .as_ref()
                                        .is_some_and(|d| d.decision.retries_snapshot())
                            }
                        };
                        chain_valid &= follows_prior
                            && reservation.kind == RequestKind::Mutation
                            && fields.created_authority.run_id == reservation.original_run
                            && fields.created_authority.owner_term < authority.owner_term
                            && Some(fields.request) == pin.request
                            && fields
                                .settled_at
                                .is_some_and(|p| p.term != 0 && p.index != 0)
                            && fields.resolved == fields.expected
                            && fields.decision.as_ref().is_some_and(|d| {
                                d.txn == fields.txn
                                    && d.request == fields.request
                                    && d.manifest.status_group_id == status_group_id
                                    && d.manifest.participant_count == fields.expected
                                    && (!d.decision.retries_snapshot()
                                        || reservation.scoped.is_none())
                            });
                        transaction = Some(fields);
                        Ok(())
                    },
                )?;
                if !chain_valid || !progress.exhausted {
                    return view.encode(&false);
                }
                let safe = match transaction {
                    None => match identity.terminal {
                        None => true,
                        Some(TerminalIdentity::ReadExpired {
                            reservation: terminal,
                            request,
                            ..
                        }) => {
                            reservation.kind == RequestKind::Read
                                && terminal == reservation
                                && Some(request) == pin.request
                        }
                        Some(TerminalIdentity::Failed {
                            reservation: terminal,
                            request,
                            ..
                        }) => {
                            reservation.kind == RequestKind::Mutation
                                && terminal == reservation
                                && Some(request) == pin.request
                        }
                        Some(TerminalIdentity::Committed { .. }) => false,
                    },
                    Some(transaction) => {
                        let decision = transaction.decision.ok_or_else(unknown)?;
                        if decision.decision.retries_snapshot() {
                            match identity.terminal {
                                None => true,
                                Some(TerminalIdentity::Failed {
                                    reservation: terminal,
                                    request,
                                    ..
                                }) => terminal == reservation && Some(request) == pin.request,
                                _ => false,
                            }
                        } else {
                            matches!(identity.terminal, Some(TerminalIdentity::Committed {
                                reservation: terminal_reservation, decision: terminal, ..
                            }) if terminal_reservation == reservation && terminal == decision)
                        }
                    }
                };
                view.encode(&safe)
            })
            .await
    }

    pub(crate) async fn cold_recovery_page(
        &self,
        authority: Authority,
        after: Option<TxnId>,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(BUDGET, move |view| {
                if read::<Authority>(view, STATE)? != Some(authority) {
                    return Err(unknown());
                }
                #[derive(Deserialize)]
                struct RecoveryFields {
                    txn: TxnId,
                    created_authority: Authority,
                    settled_at: Option<AppliedPosition>,
                }
                let mut prefix = TXNS.to_vec();
                prefix.push(b't');
                let key = after.map(|txn| txn_key(b't', txn, &[]));
                let mut cursor = after;
                let mut transaction = None;
                let mut skipped_after_match = false;
                let progress = view.scan_page(&prefix, key.as_deref(), 64, |key, value| {
                    if transaction.is_some() {
                        skipped_after_match = true;
                        return Ok(());
                    }
                    let fields: RecoveryFields =
                        serde_json::from_slice(value).map_err(|_| unknown())?;
                    if key != txn_key(b't', fields.txn, &[]) {
                        return Err(unknown());
                    }
                    cursor = Some(fields.txn);
                    if fields.settled_at.is_none()
                        && fields.created_authority.run_id != authority.run_id
                        && fields.created_authority.owner_term < authority.owner_term
                    {
                        transaction = cursor;
                    }
                    Ok(())
                })?;
                view.encode(&ColdRecoveryPage {
                    after: cursor,
                    transaction,
                    done: progress.exhausted && !skipped_after_match,
                })
            })
            .await
    }
}

#[derive(Deserialize)]
struct SettlementFields {
    txn: TxnId,
    created_authority: Authority,
    request: RequestBinding,
    read_ts: ReadTs,
    expected: u64,
    resolved: u64,
    settled_at: Option<AppliedPosition>,
    decision: Option<DecisionStatement>,
}
