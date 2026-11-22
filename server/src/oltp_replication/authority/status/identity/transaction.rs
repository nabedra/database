//! Fixed participant manifests and immutable transaction decisions. Every
//! counter is updated with its unique evidence in the same native batch.
use super::super::{
    CommitPermitStatement, CommitStamp, MutationOutcome, SealedCommitPermit, SettlementStatement,
    VerifiedSettlement, VerifiedStatusDecision,
};
use super::*;
use crate::oltp::{
    mvcc::{
        CanonicalJson,
        store::{ResolutionEvidence, VerifiedParticipantResolution, VerifiedParticipantSeal},
    },
    pool::ExecutionClass,
};

const TXNS: &[u8] = b"status/txn/";
mod archive;
pub(crate) mod archive_images;
mod cold;
pub(crate) mod timestamp_point;
pub(crate) use archive::{
    ArchiveCursor, ArchivedDecision, ArchivedIdentity, ArchivedIdentityDisposition,
    CommittedArchivePage, IdentityArchiveCursor, IdentityArchivePage, StatusArchiveCutStatement,
    VerifiedStatusArchiveCut,
};
pub(crate) use cold::ColdRecoveryPage;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestDraft {
    pub txn: TxnId,
    pub initial_root: Digest,
    pub current_root: Digest,
    pub next_page: u64,
    pub participant_count: u64,
    pub expected_count: u64,
    pub result_digest: Digest,
    pub outcome: MutationOutcome,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Transaction {
    txn: TxnId,
    created_authority: Authority,
    request: RequestBinding,
    read_ts: ReadTs,
    expected: u64,
    initial: Digest,
    root: Digest,
    pages: u64,
    count: u64,
    last_group: Option<Uuid>,
    manifest: Option<ManifestRef>,
    prepared: u64,
    last_seal: Option<AppliedPosition>,
    resolved: u64,
    settled_at: Option<AppliedPosition>,
    decision: Option<DecisionStatement>,
    result_digest: Digest,
    outcome: MutationOutcome,
    #[serde(skip)]
    decode_owner: Option<Arc<ScratchLease>>,
}
impl Transaction {
    fn draft(&self) -> ManifestDraft {
        ManifestDraft {
            txn: self.txn,
            initial_root: self.initial,
            current_root: self.root,
            next_page: self.pages,
            participant_count: self.count,
            expected_count: self.expected,
            result_digest: self.result_digest,
            outcome: self.outcome.clone(),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct PageReceipt {
    digest: Digest,
    draft: ManifestDraft,
}
fn txn_key(kind: u8, txn: TxnId, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(TXNS.len() + 25 + suffix.len());
    key.extend_from_slice(TXNS);
    key.push(kind);
    key.extend_from_slice(txn.request_id.as_bytes());
    key.extend_from_slice(&txn.attempt.to_be_bytes());
    key.extend_from_slice(suffix);
    key
}
pub(super) fn verify_terminal_in_read(
    view: &mut GroupRead<'_>,
    decision: &DecisionStatement,
    outcome: &MutationOutcome,
) -> Result<(), ProtocolError> {
    let state = read(view, &txn_key(b't', decision.txn, &[]))?;
    verify_terminal_transaction(state, decision, outcome)
}

pub(super) fn verify_terminal_in_apply(
    tx: &mut GroupTransaction<'_>,
    decision: &DecisionStatement,
    outcome: &MutationOutcome,
) -> Result<(), ProtocolError> {
    let state = load(tx, &txn_key(b't', decision.txn, &[]))?;
    verify_terminal_transaction(state, decision, outcome)
}

pub(super) fn verify_settled_terminal_in_apply(
    tx: &mut GroupTransaction<'_>,
    decision: &DecisionStatement,
    outcome: &MutationOutcome,
) -> Result<(), ProtocolError> {
    let state: Transaction = load(tx, &txn_key(b't', decision.txn, &[]))?.ok_or_else(unknown)?;
    if state.resolved != state.expected
        || state.settled_at.is_none_or(|position| {
            position.term == 0 || position.index < decision.decided_at.index
        })
    {
        return Err(unknown());
    }
    verify_terminal_transaction(Some(state), decision, outcome)
}

fn verify_terminal_transaction(
    state: Option<Transaction>,
    decision: &DecisionStatement,
    outcome: &MutationOutcome,
) -> Result<(), ProtocolError> {
    let state = state.ok_or_else(unknown)?;
    if state.txn != decision.txn
        || state.request != decision.request
        || state.manifest != Some(decision.manifest)
        || state.decision.as_ref() != Some(decision)
        || state.outcome != *outcome
    {
        return Err(unknown());
    }
    Ok(())
}

pub(super) fn has_transaction(
    tx: &mut GroupTransaction<'_>,
    request_id: Uuid,
) -> Result<bool, ProtocolError> {
    let mut found = false;
    tx.scan_page(&transaction_prefix(request_id), None, 1, |_, _| {
        found = true;
        Ok(())
    })?;
    Ok(found)
}

fn transaction_prefix(request_id: Uuid) -> Vec<u8> {
    let mut prefix = txn_key(b't', TxnId { request_id, attempt: 0 }, &[]);
    prefix.truncate(prefix.len() - 8);
    prefix
}

fn root_step(prior: Digest, participant: &ParticipantPlan) -> Result<Digest, ProtocolError> {
    super::super::metadata_digest(
        b"nabedra/status-manifest-participant/v1\0",
        &(prior, participant),
    )
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    current: Option<Authority>,
    command: &StatusCommand,
) -> Result<Transition, ProtocolError> {
    let Some(current) = current else {
        return reject(tx, StatusFault::NotInitialized);
    };
    let authority = match command {
        StatusCommand::BeginManifest { authority, .. }
        | StatusCommand::ManifestPage { authority, .. }
        | StatusCommand::SealManifest { authority, .. }
        | StatusCommand::RecordPrepared { authority, .. }
        | StatusCommand::DecideCommit { authority, .. }
        | StatusCommand::Abort { authority, .. }
        | StatusCommand::RecordResolved { authority, .. }
        | StatusCommand::TerminalFailure { authority, .. } => *authority,
        _ => return Err(invalid()),
    };
    if current != authority {
        return reject(tx, StatusFault::StaleAuthority);
    }
    if let StatusCommand::BeginManifest {
        txn,
        request,
        read_ts,
        participant_count,
        outcome,
        result_digest,
        ..
    } = command
    {
        if txn.request_id != request.request_id
            || txn.attempt == 0
            || *participant_count == 0
            || outcome.digest()? != *result_digest
        {
            return reject(tx, StatusFault::InvalidBinding);
        }
        let Some(identity) = load::<IdentityRecord>(tx, &key(IDENTITIES, txn.request_id))? else {
            return reject(tx, StatusFault::IdentityNotReserved);
        };
        if identity.rejection.is_some() {
            return reject(tx, StatusFault::TerminalConflict);
        }
        if identity.reservation.kind != RequestKind::Mutation
            || identity.reservation.digest != request.digest
            || identity.reservation.original_run != request.run_id
        {
            return reject(tx, StatusFault::InvalidBinding);
        }
        if request.run_id != current.run_id {
            return reject(tx, StatusFault::Abandoned);
        }
        if identity.terminal.is_some() {
            return reject(tx, StatusFault::AlreadyDecided);
        }
        let record_key = txn_key(b't', *txn, &[]);
        if let Some(old) = load::<Transaction>(tx, &record_key)? {
            if old.request != *request
                || old.read_ts != *read_ts
                || old.expected != *participant_count
                || old.result_digest != *result_digest
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            return commit(tx, StatusReply::Manifest(old.draft()));
        }
        // A scoped UUID has exactly one attempt and remains at its original
        // Status home. A retry must recover that attempt instead of minting
        // a second manifest after an ambiguous RPC.
        if identity.reservation.scoped.is_some() && (*participant_count > 1 || txn.attempt != 1) {
            return reject(tx, StatusFault::InvalidBinding);
        }
        if txn.attempt > 1 {
            if txn.attempt > MAX_QUEUED_MUTATION_ATTEMPTS {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let prior = TxnId {
                request_id: txn.request_id,
                attempt: txn.attempt - 1,
            };
            let Some(previous) = load::<Transaction>(tx, &txn_key(b't', prior, &[]))? else {
                return reject(tx, StatusFault::Incomplete);
            };
            if previous.txn != prior
                || !settled_retry(&previous, *request)
                || *read_ts <= previous.read_ts
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
        }
        let initial = super::super::metadata_digest(
            b"nabedra/status-manifest/v1\0",
            &(
                scope.group_id,
                txn,
                request,
                read_ts,
                participant_count,
                result_digest,
            ),
        )?;
        let state = Transaction {
            txn: *txn,
            created_authority: current,
            request: *request,
            read_ts: *read_ts,
            expected: *participant_count,
            initial,
            root: initial,
            pages: 0,
            count: 0,
            last_group: None,
            manifest: None,
            prepared: 0,
            last_seal: None,
            resolved: 0,
            settled_at: None,
            decision: None,
            result_digest: *result_digest,
            outcome: outcome.clone(),
            decode_owner: None,
        };
        put(tx, &record_key, &state)?;
        return commit(tx, StatusReply::Manifest(state.draft()));
    }
    if let StatusCommand::TerminalFailure {
        request, outcome, ..
    } = command
    {
        let identity_key = key(IDENTITIES, request.request_id);
        let Some(mut identity) = load::<IdentityRecord>(tx, &identity_key)? else {
            return reject(tx, StatusFault::IdentityNotReserved);
        };
        if identity.reservation.request_id != request.request_id {
            return Err(unknown());
        }
        verify_lookup_record(scope, &identity, |decision, outcome| {
            verify_terminal_in_apply(tx, decision, outcome)
        })?;
        if identity.rejection.is_some() {
            return reject(tx, StatusFault::TerminalConflict);
        }
        if identity.reservation.original_run != current.run_id || request.run_id != current.run_id {
            return reject(tx, StatusFault::Abandoned);
        }
        if identity.reservation.kind != RequestKind::Mutation
            || identity.reservation.digest != request.digest
            || !matches!(outcome, MutationOutcome::Failure(_))
        {
            return reject(tx, StatusFault::InvalidBinding);
        }
        if let Some(terminal) = &identity.terminal {
            if terminal.request_binding() != *request {
                return reject(tx, StatusFault::InvalidBinding);
            }
            return commit(
                tx,
                StatusReply::Identity(IdentityLookup::Terminal(terminal.clone())),
            );
        }
        // Planning may fail on the fresh snapshot, for example if another
        // request inserted the key. All earlier attempts must be closed.
        let mut attempts = 0;
        let mut closed = true;
        let progress = tx.scan_page(
            &transaction_prefix(request.request_id),
            None,
            MAX_QUEUED_MUTATION_ATTEMPTS as usize,
            |key, raw| {
                attempts += 1;
                #[derive(Deserialize)]
                struct Prior {
                    txn: TxnId,
                    request: RequestBinding,
                    resolved: u64,
                    expected: u64,
                    settled_at: Option<AppliedPosition>,
                    decision: Option<DecisionStatement>,
                }
                let prior: Prior = serde_json::from_slice(raw).map_err(|_| unknown())?;
                closed &= prior.txn.attempt == attempts
                    && key == txn_key(b't', prior.txn, &[])
                    && prior.request == *request
                    && prior.resolved == prior.expected
                    && prior.settled_at.is_some()
                    && prior
                        .decision
                        .as_ref()
                        .is_some_and(|d| d.decision.retries_snapshot());
                Ok(())
            },
        )?;
        if !closed || !progress.exhausted || attempts >= MAX_QUEUED_MUTATION_ATTEMPTS {
            return reject(tx, StatusFault::AlreadyDecided);
        }
        let terminal = TerminalIdentity::Failed {
            reservation: identity.reservation,
            request: *request,
            outcome: outcome.clone(),
            recorded_at: position,
        };
        identity.terminal = Some(terminal.clone());
        put(tx, &identity_key, &identity)?;
        return commit(
            tx,
            StatusReply::Identity(IdentityLookup::Terminal(terminal)),
        );
    }
    let txn = match command {
        StatusCommand::ManifestPage { txn, .. }
        | StatusCommand::SealManifest { txn, .. }
        | StatusCommand::Abort { txn, .. } => *txn,
        StatusCommand::RecordPrepared { evidence, .. } => evidence.txn,
        StatusCommand::DecideCommit { permit, .. } => permit.statement().txn,
        StatusCommand::RecordResolved { evidence, .. } => evidence.txn,
        _ => return Err(invalid()),
    };
    let record_key = txn_key(b't', txn, &[]);
    let Some(mut state) = load::<Transaction>(tx, &record_key)? else {
        return reject(tx, StatusFault::UnknownIdentity);
    };
    // Cold fencing abandons execution, not an immutable decision. Current
    // authority may finish resolving an already decided old-run transaction.
    let fenced_abandon = matches!(
        command,
        StatusCommand::Abort {
            reason: AbortReason::AbandonedByFence,
            ..
        }
    ) && current.owner_term > state.created_authority.owner_term;
    if state.request.run_id != current.run_id
        && !(matches!(command, StatusCommand::RecordResolved { .. }) && state.decision.is_some())
        && !fenced_abandon
    {
        return reject(tx, StatusFault::Abandoned);
    }
    match command {
        StatusCommand::ManifestPage {
            page_index,
            previous_root,
            participants,
            ..
        } => {
            let receipt_key = txn_key(b'p', txn, &page_index.to_be_bytes());
            let digest = super::super::metadata_digest(
                b"nabedra/status-manifest-page/v1\0",
                &(txn, page_index, previous_root, participants),
            )?;
            if let Some(receipt) = load::<PageReceipt>(tx, &receipt_key)? {
                return if receipt.digest == digest {
                    commit(tx, StatusReply::Manifest(receipt.draft))
                } else {
                    reject(tx, StatusFault::TransitionConflict)
                };
            }
            if state.manifest.is_some()
                || state.pages != *page_index
                || state.root != *previous_root
                || participants.is_empty()
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            for plan in participants.iter() {
                if plan.validate().is_err()
                    || plan.read_ts != state.read_ts
                    || plan.group.cluster_id != scope.cluster_id
                    || plan.group.database != scope.database
                    || state
                        .last_group
                        .is_some_and(|last| last >= plan.group.group_id)
                    || state.count >= state.expected
                {
                    return reject(tx, StatusFault::InvalidBinding);
                }
                state.root = root_step(state.root, plan)?;
                state.count += 1;
                state.last_group = Some(plan.group.group_id);
                put(
                    tx,
                    &txn_key(b'g', txn, plan.group.group_id.as_bytes()),
                    plan,
                )?;
                put(
                    tx,
                    &txn_key(b'o', txn, &(state.count - 1).to_be_bytes()),
                    plan,
                )?;
            }
            state.pages = state.pages.checked_add(1).ok_or_else(unknown)?;
            let draft = state.draft();
            put(tx, &record_key, &state)?;
            put(
                tx,
                &receipt_key,
                &PageReceipt {
                    digest,
                    draft: draft.clone(),
                },
            )?;
            commit(tx, StatusReply::Manifest(draft))
        }
        StatusCommand::SealManifest { .. } => {
            if let Some(manifest) = state.manifest {
                return commit(tx, StatusReply::ManifestSealed(manifest));
            }
            if state.count != state.expected {
                return reject(tx, StatusFault::Incomplete);
            }
            let manifest = ManifestRef {
                status_group_id: scope.group_id,
                digest: state.root,
                participant_count: state.count,
            };
            state.manifest = Some(manifest);
            put(tx, &record_key, &state)?;
            commit(tx, StatusReply::ManifestSealed(manifest))
        }
        StatusCommand::RecordPrepared { evidence, .. } => {
            let Some(manifest) = state.manifest else {
                return reject(tx, StatusFault::Incomplete);
            };
            if state.decision.is_some() {
                return reject(tx, StatusFault::AlreadyDecided);
            }
            let plan: Option<ParticipantPlan> = load(
                tx,
                &txn_key(b'g', txn, evidence.participant.group.group_id.as_bytes()),
            )?;
            if plan.as_ref() != Some(&evidence.participant)
                || evidence.manifest != manifest
                || evidence.result_digest != state.result_digest
                || evidence.authority.run_id != state.request.run_id
                || evidence.authority.owner_term > current.owner_term
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let evidence_key = txn_key(b'e', txn, evidence.participant.group.group_id.as_bytes());
            if let Some(old) = load::<PreparedParticipant>(tx, &evidence_key)? {
                if old != *evidence {
                    return reject(tx, StatusFault::TransitionConflict);
                }
            } else {
                put(tx, &evidence_key, evidence)?;
                state.prepared += 1;
                if state.prepared == state.expected {
                    state.last_seal = Some(position);
                }
                put(tx, &record_key, &state)?;
            }
            commit(
                tx,
                StatusReply::Prepared {
                    count: state.prepared,
                    complete: state.prepared == state.expected,
                },
            )
        }
        StatusCommand::DecideCommit { permit, stamp, .. } => {
            if let Some(decision) = state.decision {
                return commit(tx, StatusReply::Decided(decision));
            }
            let Some(manifest) = state.manifest else {
                return reject(tx, StatusFault::Incomplete);
            };
            let binding = permit.statement();
            if permit.validate().is_err()
                || binding.authority != current
                || binding.txn != txn
                || binding.request != state.request
                || binding.manifest != manifest
                || binding.read_ts != state.read_ts
                || binding.prepared_count != state.expected
                || state.prepared != state.expected
                || Some(binding.prepared_at) != state.last_seal
                || stamp.permit_digest != permit.allocation_digest()?
                || stamp.commit_ts <= state.read_ts
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let decision = DecisionStatement {
                txn,
                request: state.request,
                manifest,
                decided_at: position,
                decision: TxnDecision::Commit {
                    commit_ts: stamp.commit_ts,
                },
            };
            decide(tx, &record_key, &mut state, decision.clone())?;
            commit(tx, StatusReply::Decided(decision))
        }
        StatusCommand::Abort {
            reason, outcome, ..
        } => {
            if let Some(decision) = state.decision {
                return commit(tx, StatusReply::Decided(decision));
            }
            if !matches!(outcome, MutationOutcome::Failure(_)) {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let manifest = if let Some(manifest) = state.manifest {
                manifest
            } else {
                // No participant can prepare before the complete manifest is
                // sealed. A strictly newer fenced owner may terminate this
                // draft without inventing its missing participant pages.
                if !fenced_abandon {
                    return reject(tx, StatusFault::Incomplete);
                }
                let digest = super::super::metadata_digest(
                    b"nabedra/abandoned-unsealed-manifest/v1\0",
                    &(scope.group_id, txn, state.request, state.root),
                )?;
                let manifest = ManifestRef {
                    status_group_id: scope.group_id,
                    digest,
                    participant_count: 0,
                };
                state.manifest = Some(manifest);
                state.expected = 0;
                state.count = 0;
                state.prepared = 0;
                state.resolved = 0;
                state.settled_at = Some(position);
                manifest
            };
            let decision = DecisionStatement {
                txn,
                request: state.request,
                manifest,
                decided_at: position,
                decision: TxnDecision::Abort { reason: *reason },
            };
            if *reason == AbortReason::RetrySnapshot {
                let identity: IdentityRecord =
                    load(tx, &key(IDENTITIES, txn.request_id))?.ok_or_else(unknown)?;
                if identity.reservation.scoped.is_some()
                    || identity.terminal.is_some()
                    || state.created_authority.run_id != current.run_id
                    || state.created_authority.owner_term > current.owner_term
                    || txn.attempt >= MAX_QUEUED_MUTATION_ATTEMPTS
                    || !outcome.is_not_applied()
                {
                    return reject(tx, StatusFault::InvalidBinding);
                }
                state.outcome = outcome.clone();
                state.decision = Some(decision.clone());
                put(tx, &record_key, &state)?;
            } else {
                state.outcome = outcome.clone();
                decide(tx, &record_key, &mut state, decision.clone())?;
            }
            commit(tx, StatusReply::Decided(decision))
        }
        StatusCommand::RecordResolved { evidence, .. } => {
            let Some(decision) = &state.decision else {
                return reject(tx, StatusFault::Incomplete);
            };
            let plan: Option<ParticipantPlan> =
                load(tx, &txn_key(b'g', txn, evidence.group.group_id.as_bytes()))?;
            if plan.as_ref().map(|plan| &plan.group) != Some(&evidence.group)
                || evidence.request != state.request
                || evidence.manifest != decision.manifest
                || evidence.decision != decision.decision
            {
                return reject(tx, StatusFault::InvalidBinding);
            }
            let evidence_key = txn_key(b'r', txn, evidence.group.group_id.as_bytes());
            if let Some(old) = load::<ResolutionEvidence>(tx, &evidence_key)? {
                if old != *evidence {
                    return reject(tx, StatusFault::TransitionConflict);
                }
            } else {
                put(tx, &evidence_key, evidence)?;
                state.resolved += 1;
                if state.resolved == state.expected {
                    state.settled_at = Some(position)
                }
                put(tx, &record_key, &state)?;
            }
            commit(
                tx,
                StatusReply::Resolved {
                    count: state.resolved,
                    complete: state.resolved == state.expected,
                },
            )
        }
        _ => Err(invalid()),
    }
}
fn settled_retry(state: &Transaction, request: RequestBinding) -> bool {
    state.request == request
        && state.resolved == state.expected
        && state.settled_at.is_some()
        && state
            .decision
            .as_ref()
            .is_some_and(|d| d.decision.retries_snapshot())
}

fn decide(
    tx: &mut GroupTransaction<'_>,
    record_key: &[u8],
    state: &mut Transaction,
    decision: DecisionStatement,
) -> Result<(), ProtocolError> {
    let identity_key = key(IDENTITIES, state.txn.request_id);
    let mut identity: IdentityRecord = load(tx, &identity_key)?.ok_or_else(unknown)?;
    if identity.terminal.is_some() {
        return Err(unknown());
    }
    identity.terminal = Some(TerminalIdentity::Committed {
        reservation: identity.reservation,
        decision: decision.clone(),
        outcome: state.outcome.clone(),
    });
    state.decision = Some(decision);
    put(tx, record_key, state)?;
    put(tx, &identity_key, &identity)
}

impl StatusHandle {
    pub(crate) async fn participant_plan_bytes(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        group_id: Uuid,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(BUDGET, move |view| {
                if read::<LocatedTimestampManifest>(view, &home_key(txn.request_id))?.is_some() {
                    return Err(unknown());
                }
                let state: Transaction =
                    read(view, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
                if state.manifest != Some(manifest) {
                    return Err(invalid());
                }
                let plan: Option<ParticipantPlan> = if state.expected == 0 {
                    None
                } else {
                    read(view, &txn_key(b'g', txn, group_id.as_bytes()))?
                };
                view.encode(&plan)
            })
            .await
    }
    pub(crate) async fn abandon_fenced(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let (current, state) = self.read_transaction(txn).await?;
        if current != authority {
            return Err(invalid());
        }
        if let Some(decision) = state.decision {
            return VerifiedStatusDecision::with_outcome(
                decision,
                state.outcome,
                state.decode_owner,
            );
        }
        if current.owner_term <= state.created_authority.owner_term {
            return Err(invalid());
        }
        self.submit(StatusCommand::Abort {
            authority,
            txn,
            reason: AbortReason::AbandonedByFence,
            outcome: MutationOutcome::failure(ProtocolError::new(
                ErrorCode::Conflict,
                "original execution authority was fenced",
                Outcome::NotApplied,
            ))?,
        })
        .await?;
        let (_, state) = self.read_transaction(txn).await?;
        VerifiedStatusDecision::with_outcome(
            state.decision.ok_or_else(unknown)?,
            state.outcome,
            state.decode_owner,
        )
    }
    pub(super) async fn submit(
        &self,
        command: StatusCommand,
    ) -> Result<Applied<StatusReply>, ProtocolError> {
        let command = match self.scoped {
            Some(epoch) => StatusCommand::Scoped {
                epoch,
                command: Box::new(command),
            },
            None => command,
        };
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.propose_owned(command).await
    }
    pub(crate) async fn install_point_manifest(
        &self,
        point: PointManifest,
    ) -> Result<(ManifestRef, Digest), ProtocolError> {
        let PointManifest {
            authority,
            txn,
            request,
            read_ts,
            plan,
            result,
        } = point;
        let scope = self.identity();
        if !PointManifest::qualifies(&plan, &result)
            || plan.validate().is_err()
            || plan.read_ts != read_ts
            || plan.group.cluster_id != scope.cluster_id
            || plan.group.database != scope.database
        {
            return Err(invalid());
        }
        let page = Page::new(vec![plan]).map_err(|_| invalid())?;
        let installed = async {
            let draft = self
                .begin_manifest(authority, txn, request, read_ts, 1, result)
                .await?;
            // Start from the original root. A retry after the page applied
            // must replay its exact receipt before it can seal the draft.
            let page = self
                .append_manifest_page(authority, txn, 0, draft.initial_root, page)
                .await?;
            let manifest = self.seal_manifest(authority, txn).await?;
            if manifest.status_group_id != scope.group_id
                || manifest.participant_count != 1
                || manifest.digest != page.current_root
                || draft.result_digest != page.result_digest
            {
                return Err(invalid());
            }
            Ok((manifest, draft.result_digest))
        }
        .await;
        // Once Begin can have executed, a refusal cannot authorize a legacy
        // fallback. A surviving caller can retry this exact durable prefix.
        installed.map_err(|mut error: ProtocolError| {
            error.outcome = Outcome::Unknown;
            error
        })
    }

    pub(crate) async fn begin_manifest(
        &self,
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        read_ts: ReadTs,
        participant_count: u64,
        result: CanonicalJson,
    ) -> Result<ManifestDraft, ProtocolError> {
        let outcome = MutationOutcome::from_success(&result)?;
        let result_digest = outcome.digest()?;
        match self
            .submit(StatusCommand::BeginManifest {
                authority,
                txn,
                request,
                read_ts,
                participant_count,
                outcome,
                result_digest,
            })
            .await?
            .reply
        {
            StatusReply::Manifest(draft) => Ok(draft),
            _ => Err(invalid()),
        }
    }
    pub(crate) async fn append_manifest_page(
        &self,
        authority: Authority,
        txn: TxnId,
        page_index: u64,
        previous_root: Digest,
        participants: Page<ParticipantPlan>,
    ) -> Result<ManifestDraft, ProtocolError> {
        match self
            .submit(StatusCommand::ManifestPage {
                authority,
                txn,
                page_index,
                previous_root,
                participants,
            })
            .await?
            .reply
        {
            StatusReply::Manifest(draft) => Ok(draft),
            _ => Err(invalid()),
        }
    }
    pub(crate) async fn seal_manifest(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<ManifestRef, ProtocolError> {
        match self
            .submit(StatusCommand::SealManifest { authority, txn })
            .await?
            .reply
        {
            StatusReply::ManifestSealed(manifest) => Ok(manifest),
            _ => Err(invalid()),
        }
    }
    pub(crate) async fn observe_prepared(
        &self,
        authority: Authority,
        proof: VerifiedParticipantSeal,
    ) -> Result<(), ProtocolError> {
        let evidence = proof.evidence().clone();
        match self
            .submit(StatusCommand::RecordPrepared {
                authority,
                evidence,
            })
            .await?
            .reply
        {
            StatusReply::Prepared { .. } => Ok(()),
            _ => Err(invalid()),
        }
    }
    pub(crate) async fn observe_resolved(
        &self,
        authority: Authority,
        proof: VerifiedParticipantResolution,
    ) -> Result<(), ProtocolError> {
        let evidence = proof.evidence().clone();
        match self
            .submit(StatusCommand::RecordResolved {
                authority,
                evidence,
            })
            .await?
            .reply
        {
            StatusReply::Resolved { .. } => Ok(()),
            _ => Err(invalid()),
        }
    }
    async fn read_transaction(
        &self,
        txn: TxnId,
    ) -> Result<(Authority, Transaction), ProtocolError> {
        let barrier = self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let authority: Authority = read(view, STATE)?.ok_or_else(unknown)?;
                let state: Transaction =
                    read(view, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
                if let Some(home) = read::<LocatedTimestampManifest>(view, &home_key(txn.request_id))? {
                    if home.locator.binding.txn != txn {
                        return Err(unknown());
                    }
                    let identity = read::<IdentityRecord>(view, &key(IDENTITIES, txn.request_id))?
                        .ok_or_else(unknown)?;
                    projection::verify_in_read(view, &scope, &identity, &home, authority, Some(barrier.applied))?;
                }
                view.encode(&(authority, state))
            })
            .await?;
        let owner = Arc::new(
            self.machine
                .store()
                .resources()
                .reserve_reply_decode(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        let shared = self.machine.store().resources().clone();
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let (authority, mut state): (Authority, Transaction) =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                state.outcome.retain_owner(owner.clone());
                state.decode_owner = Some(owner);
                Ok((authority, state))
            })
            .await
    }
    pub(crate) async fn commit_permit(
        &self,
        authority: Authority,
        txn: TxnId,
    ) -> Result<SealedCommitPermit, ProtocolError> {
        let (current, state) = self.read_transaction(txn).await?;
        if current != authority
            || state.request.run_id != current.run_id
            || state.decision.is_some()
            || state.prepared != state.expected
        {
            return Err(invalid());
        }
        SealedCommitPermit::from_strong_prepared_read(CommitPermitStatement {
            authority,
            txn,
            request: state.request,
            manifest: state.manifest.ok_or_else(invalid)?,
            read_ts: state.read_ts,
            prepared_count: state.prepared,
            prepared_at: state.last_seal.ok_or_else(invalid)?,
        })
    }
    pub(crate) async fn decide_commit(
        &self,
        authority: Authority,
        permit: SealedCommitPermit,
        stamp: CommitStamp,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let txn = permit.statement().txn;
        let manifest = permit.statement().manifest;
        let applied = self.submit(StatusCommand::DecideCommit {
            authority,
            permit,
            stamp,
        })
        .await?;
        if applied.reply == StatusReply::Rejected(StatusFault::TransitionConflict) {
            return Err(timestamp_point::mode_conflict());
        }
        self.lookup_decision(txn, manifest)
            .await?
            .ok_or_else(unknown)
    }
    pub(crate) async fn abort(
        &self,
        authority: Authority,
        txn: TxnId,
        reason: AbortReason,
        error: ProtocolError,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let applied = self.submit(StatusCommand::Abort {
            authority,
            txn,
            reason,
            outcome: MutationOutcome::failure(error)?,
        })
        .await?;
        if applied.reply == StatusReply::Rejected(StatusFault::TransitionConflict) {
            return Err(timestamp_point::mode_conflict());
        }
        let (_, state) = self.read_transaction(txn).await?;
        VerifiedStatusDecision::with_outcome(
            state.decision.ok_or_else(unknown)?,
            state.outcome,
            state.decode_owner,
        )
    }
    pub(crate) async fn lookup_decision(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        if manifest.participant_count == 0 && txn.attempt == 1 {
            if let Some(IdentityLookup::Terminal(TerminalIdentity::Failed {
                reservation,
                request,
                outcome,
                ..
            })) = self.lookup_identity(txn.request_id, None).await?
                && reservation.scoped.is_some()
            {
                let proof = self.failure_settled_proof(txn, request).await?;
                if proof.statement().manifest != manifest {
                    return Err(invalid());
                }
                return VerifiedStatusDecision::with_outcome(
                    proof.statement().decision.clone(),
                    outcome,
                    None,
                )
                .map(Some);
            }
        }
        let (_, state) = self.read_transaction(txn).await?;
        if state.manifest != Some(manifest) {
            return Err(invalid());
        }
        state
            .decision
            .map(|decision| {
                VerifiedStatusDecision::with_outcome(decision, state.outcome, state.decode_owner)
            })
            .transpose()
    }
    pub(crate) async fn settled_proof(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        // A planning failure becomes durable before any participant manifest.
        // Its identity CAS forbids a later BeginManifest, so this exact native
        // terminal record proves there was no executable preparation to await.
        if manifest.participant_count == 0 {
            if let Some(IdentityLookup::Terminal(TerminalIdentity::Failed { request, .. })) =
                self.lookup_identity(txn.request_id, None).await?
            {
                let proof = self.failure_settled_proof(txn, request).await?;
                if proof.clone().into_sealed().statement().manifest != manifest {
                    return Err(invalid());
                }
                return Ok(proof);
            }
        }
        let (_, state) = self.read_transaction(txn).await?;
        if state.manifest != Some(manifest) || state.resolved != state.expected {
            return Err(invalid());
        }
        VerifiedSettlement::from_strong_settled_read(SettlementStatement {
            txn,
            request: state.request,
            manifest,
            decision: state.decision.ok_or_else(invalid)?,
            resolved_count: state.resolved,
            settled_at: state.settled_at.ok_or_else(invalid)?,
        })
    }
    pub(crate) async fn terminal_failure(
        &self,
        authority: Authority,
        request: RequestBinding,
        error: ProtocolError,
    ) -> Result<TerminalIdentity, ProtocolError> {
        match self
            .submit(StatusCommand::TerminalFailure {
                authority,
                request,
                outcome: MutationOutcome::failure(error)?,
            })
            .await?
            .reply
        {
            StatusReply::Identity(IdentityLookup::Terminal(terminal)) => Ok(terminal),
            _ => Err(invalid()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestInventoryPage {
    pub items: Page<ParticipantPlan>,
    pub after: Option<u64>,
    pub done: bool,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}
impl ManifestInventoryPage {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        self.owner = Some(owner);
    }
}
impl std::fmt::Debug for ManifestInventoryPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestInventoryPage")
            .field("items", &self.items)
            .field("after", &self.after)
            .field("done", &self.done)
            .finish()
    }
}
impl StatusHandle {
    pub(crate) async fn manifest_page(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
        after: Option<u64>,
    ) -> Result<ManifestInventoryPage, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state: Transaction =
                    read(view, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
                if state.manifest != Some(manifest) {
                    return Err(invalid());
                }
                if state.expected == 0 {
                    return view.encode(&ManifestInventoryPage {
                        items: Page::new(Vec::new()).map_err(|_| invalid())?,
                        after: None,
                        done: true,
                        owner: None,
                    });
                }
                let prefix = txn_key(b'o', txn, &[]);
                let cursor = after.map(|after| txn_key(b'o', txn, &after.to_be_bytes()));
                let mut items = Vec::new();
                let mut after = after;
                let progress = view.scan_page(&prefix, cursor.as_deref(), 64, |key, bytes| {
                    let index = key.strip_prefix(prefix.as_slice()).ok_or_else(unknown)?;
                    if index.len() != 8 {
                        return Err(unknown());
                    }
                    after = Some(u64::from_be_bytes(index.try_into().map_err(|_| unknown())?));
                    let plan: ParticipantPlan =
                        serde_json::from_slice(bytes).map_err(|_| unknown())?;
                    items.push(plan);
                    Ok(())
                })?;
                view.encode(&ManifestInventoryPage {
                    items: Page::new(items).map_err(|_| invalid())?,
                    after,
                    done: progress.exhausted,
                    owner: None,
                })
            })
            .await?;
        let shared = self.machine.store().resources().clone();
        let owner = Arc::new(
            shared
                .reserve_reply_decode(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let mut page: ManifestInventoryPage =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                page.owner = Some(owner);
                Ok(page)
            })
            .await
    }
    pub(crate) async fn failure_settled_proof(
        &self,
        txn: TxnId,
        request: RequestBinding,
    ) -> Result<VerifiedSettlement, ProtocolError> {
        if txn.request_id != request.request_id {
            return Err(invalid());
        }
        self.consensus.ensure_linearizable().await?;
        let status_group = self.identity().group_id;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let identity: IdentityRecord =
                    read(view, &key(IDENTITIES, request.request_id))?.ok_or_else(unknown)?;
                let Some(TerminalIdentity::Failed {
                    request: original,
                    outcome,
                    recorded_at,
                    ..
                }) = identity.terminal
                else {
                    return Err(invalid());
                };
                if original != request || view.get(&txn_key(b't', txn, &[]))?.is_some() {
                    return Err(invalid());
                }
                let digest = super::super::metadata_digest(
                    b"nabedra/failed-plan-settlement/v1\0",
                    &(txn, request, recorded_at, outcome.digest()?),
                )?;
                let manifest = ManifestRef {
                    status_group_id: status_group,
                    digest,
                    participant_count: 0,
                };
                let decision = DecisionStatement {
                    txn,
                    request,
                    manifest,
                    decided_at: recorded_at,
                    decision: TxnDecision::Abort {
                        reason: AbortReason::Validation,
                    },
                };
                view.encode(&SettlementStatement {
                    txn,
                    request,
                    manifest,
                    decision,
                    resolved_count: 0,
                    settled_at: recorded_at,
                })
            })
            .await?;
        let statement: SettlementStatement =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        VerifiedSettlement::from_strong_settled_read(statement)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransactionInspection {
    pub current_authority: Authority,
    pub created_authority: Authority,
    pub draft: ManifestDraft,
    pub manifest: Option<ManifestRef>,
    pub prepared_count: u64,
    pub resolved_count: u64,
    pub decision: Option<DecisionStatement>,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}
impl TransactionInspection {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        self.draft.outcome.retain_owner(owner.clone());
        self.owner = Some(owner);
    }
}
impl StatusHandle {
    pub(crate) async fn scoped_transaction_matches(
        &self,
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
        txn: TxnId,
    ) -> Result<bool, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state: Option<Transaction> = read(view, &txn_key(b't', txn, &[]))?;
                if let Some(state) = &state {
                    if state.txn != txn
                        || state.request != scope.request
                        || state.read_ts != scope.read_ts
                    {
                        return Err(invalid());
                    }
                }
                view.encode(&state.is_some())
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    pub(crate) async fn inspect_transaction(
        &self,
        txn: TxnId,
    ) -> Result<Option<TransactionInspection>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let current_authority: Authority = read(view, STATE)?.ok_or_else(unknown)?;
                let state: Option<Transaction> = read(view, &txn_key(b't', txn, &[]))?;
                let inspection = state.map(|state| TransactionInspection {
                    current_authority,
                    created_authority: state.created_authority,
                    draft: state.draft(),
                    manifest: state.manifest,
                    prepared_count: state.prepared,
                    resolved_count: state.resolved,
                    decision: state.decision,
                    owner: None,
                });
                view.encode(&inspection)
            })
            .await?;
        let shared = self.machine.store().resources().clone();
        let owner = Arc::new(
            shared
                .reserve_reply_decode(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let mut state: Option<TransactionInspection> =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                if let Some(state) = &mut state {
                    state.draft.outcome.retain_owner(owner.clone());
                    state.owner = Some(owner)
                }
                Ok(state)
            })
            .await
    }
}
