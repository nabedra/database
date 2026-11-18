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
        self.consensus.ensure_linearizable().await?;
        let status_group_id = self.identity().group_id;
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
                // Application execution has one durable transaction attempt per
                // reserved UUID. Check the UUID prefix for both absence and
                // ambiguity so an unexpected second attempt cannot release a pin.
                let mut prefix = txn_key(
                    b't',
                    TxnId {
                        request_id: reservation.request_id,
                        attempt: 0,
                    },
                    &[],
                );
                prefix.truncate(prefix.len() - 8);
                let mut transaction = None;
                view.scan_page(&prefix, None, 2, |key, raw| {
                    if transaction.is_some() {
                        return Err(invalid());
                    }
                    let fields: SettlementFields =
                        serde_json::from_slice(raw).map_err(|_| unknown())?;
                    if key != txn_key(b't', fields.txn, &[]) {
                        return Err(unknown());
                    }
                    transaction = Some(fields);
                    Ok(())
                })?;
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
                        let Some(decision) = transaction.decision else {
                            return view.encode(&false);
                        };
                        reservation.kind == RequestKind::Mutation
                            && transaction.created_authority.run_id == reservation.original_run
                            && transaction.created_authority.owner_term < authority.owner_term
                            && transaction.read_ts == pin.pin.read_ts
                            && Some(transaction.request) == pin.request
                            && transaction
                                .settled_at
                                .is_some_and(|position| position.term != 0 && position.index != 0)
                            && transaction.resolved == transaction.expected
                            && decision.txn == transaction.txn
                            && decision.request == transaction.request
                            && decision.manifest.status_group_id == status_group_id
                            && decision.manifest.participant_count == transaction.expected
                            && matches!(identity.terminal, Some(TerminalIdentity::Committed {
                            reservation: terminal_reservation, decision: terminal, ..
                        }) if terminal_reservation == reservation && terminal == decision)
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
