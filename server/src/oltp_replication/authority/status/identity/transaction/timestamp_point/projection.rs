//! Materialize the exact Timestamp decision and first Data completion in Status.
use super::*;
use crate::oltp::mvcc::store::timestamp_resolution::TimestampPointResolution;
use crate::oltp_replication::service_rpc::VerifiedTimestampPointResolution;

pub(crate) const MAX_POINT_PROJECTION_BYTES: usize = 8 << 10;

const READ_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 128 << 10,
    batch_bytes: 0,
    output_bytes: MAX_POINT_PROJECTION_BYTES,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampPointProjection {
    completion: TimestampPointResolution,
    projected_at: AppliedPosition,
}

impl TimestampPointProjection {
    pub(crate) fn validate_for(
        &self,
        locator: &TimestampDecisionLocator,
    ) -> Result<(), ProtocolError> {
        self.completion.validate_for(locator)?;
        if !valid_position(self.projected_at) {
            return Err(unknown());
        }
        Ok(())
    }

    pub(crate) fn completion(&self) -> &TimestampPointResolution {
        &self.completion
    }

    pub(crate) fn projected_at(&self) -> AppliedPosition {
        self.projected_at
    }

    fn digest(&self) -> Result<Digest, ProtocolError> {
        super::super::super::super::metadata_digest(
            b"nabedra/status-timestamp-point-projection/v1\0",
            self,
        )
    }

    fn statement(&self) -> DecisionStatement {
        let decision = self.completion.decision();
        let home = decision.locator();
        DecisionStatement {
            txn: home.binding.txn,
            request: home.binding.request,
            manifest: home.manifest,
            decided_at: self.projected_at,
            decision: decision.decision(),
        }
    }

    fn evidence(&self) -> ResolutionEvidence {
        let decision = self.completion.decision();
        let home = decision.locator();
        ResolutionEvidence {
            group: home.binding.participant.group.clone(),
            txn: home.binding.txn,
            request: home.binding.request,
            manifest: home.manifest,
            decision: decision.decision(),
            completed_at: self.completion.completed_at(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredProjection {
    digest: Digest,
    record: TimestampPointProjection,
}

fn resolution_key(locator: &TimestampDecisionLocator) -> Vec<u8> {
    txn_key(
        b'r',
        locator.binding.txn,
        locator.binding.participant.group.group_id.as_bytes(),
    )
}

fn validate_completion(
    completion: &TimestampPointResolution,
    home: &LocatedTimestampManifest,
    current: Authority,
) -> Result<(), ProtocolError> {
    completion
        .validate_for(&home.locator)
        .map_err(|_| unknown())?;
    let decision = completion.decision();
    let authority = decision.authority();
    let created = home.locator.binding.created_authority;
    if authority.owner_term < created.owner_term
        || (authority.owner_term == created.owner_term && authority != created)
        || authority.owner_term > current.owner_term
        || (authority.owner_term == current.owner_term && authority != current)
    {
        return Err(unknown());
    }
    match decision.decision() {
        TxnDecision::Commit { commit_ts }
            if authority.run_id == created.run_id
                && commit_ts > home.locator.binding.storage_pin.read_ts
                && decision.outcome() == &home.outcome =>
        {
            Ok(())
        }
        TxnDecision::Abort { .. } if decision.outcome().is_not_applied() => Ok(()),
        _ => Err(unknown()),
    }
}

pub(super) fn valid_position(position: AppliedPosition) -> bool {
    position.term != 0 && position.node_id != 0 && position.index != 0
}

fn validate_projection(
    stored: Option<StoredProjection>,
    identity: &IdentityRecord,
    state: &Transaction,
    home: &LocatedTimestampManifest,
    evidence: Option<ResolutionEvidence>,
    current: Authority,
    applied: Option<AppliedPosition>,
) -> Result<Option<TimestampPointProjection>, ProtocolError> {
    let Some(StoredProjection { digest, record }) = stored else {
        if state.decision.is_some()
            || state.settled_at.is_some()
            || state.resolved != 0
            || identity.terminal.is_some()
            || evidence.is_some()
            || state.outcome != home.outcome
        {
            return Err(unknown());
        }
        return Ok(None);
    };
    let position = record.projected_at();
    let statement = record.statement();
    let outcome = record.completion().decision().outcome();
    if digest != record.digest()?
        || !valid_position(position)
        || applied.is_some_and(|applied| {
            position.index > applied.index
                || position.term > applied.term
                || (position.index == applied.index && position != applied)
        })
        || state.decision.as_ref() != Some(&statement)
        || state.settled_at != Some(position)
        || state.resolved != 1
        || &state.outcome != outcome
        || evidence.as_ref() != Some(&record.evidence())
        || !matches!(&identity.terminal, Some(TerminalIdentity::Committed {
            reservation, decision, outcome: terminal_outcome,
        }) if *reservation == identity.reservation && *decision == statement && terminal_outcome == outcome)
    {
        return Err(unknown());
    }
    validate_completion(record.completion(), home, current)?;
    Ok(Some(record))
}

pub(in super::super::super) fn verify_in_apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    identity: &IdentityRecord,
    home: &LocatedTimestampManifest,
    current: Authority,
    applied: Option<AppliedPosition>,
) -> Result<Option<TimestampPointProjection>, ProtocolError> {
    let txn = home.locator.binding.txn;
    let state: Transaction = load(tx, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
    validate_stored_home(group, identity, home, &state)?;
    check_participant_indexes(&home.locator, |key| load(tx, key))?;
    let stored = load(tx, &txn_key(b'q', txn, &[]))?;
    let evidence = load(tx, &resolution_key(&home.locator))?;
    validate_projection(stored, identity, &state, home, evidence, current, applied)
}

pub(in super::super::super) fn verify_in_read(
    view: &mut GroupRead<'_>,
    group: &GroupIdentity,
    identity: &IdentityRecord,
    home: &LocatedTimestampManifest,
    current: Authority,
    applied: Option<AppliedPosition>,
) -> Result<Option<TimestampPointProjection>, ProtocolError> {
    let txn = home.locator.binding.txn;
    let state: Transaction = read(view, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
    validate_stored_home(group, identity, home, &state)?;
    check_participant_indexes(&home.locator, |key| read(view, key))?;
    let stored = read(view, &txn_key(b'q', txn, &[]))?;
    let evidence = read(view, &resolution_key(&home.locator))?;
    validate_projection(stored, identity, &state, home, evidence, current, applied)
}

/// Read the sidecar and its canonical Status records in the same snapshot.
pub(crate) fn read_for_transaction(
    view: &mut GroupRead<'_>,
    group: &GroupIdentity,
    txn: TxnId,
    applied: Option<AppliedPosition>,
) -> Result<Option<TimestampPointProjection>, ProtocolError> {
    let Some(home) = read::<LocatedTimestampManifest>(view, &home_key(txn.request_id))? else {
        if read::<StoredProjection>(view, &txn_key(b'q', txn, &[]))?.is_some() {
            return Err(unknown());
        }
        return Ok(None);
    };
    if home.locator.binding.txn != txn {
        return Err(unknown());
    }
    let current = read::<Authority>(view, STATE)?.ok_or_else(unknown)?;
    let identity =
        read::<IdentityRecord>(view, &key(IDENTITIES, txn.request_id))?.ok_or_else(unknown)?;
    verify_in_read(view, group, &identity, &home, current, applied)
}

pub(in super::super::super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    position: AppliedPosition,
    current: Authority,
    completion: &TimestampPointResolution,
) -> Result<Transition, ProtocolError> {
    let locator = completion.decision().locator();
    let txn = locator.binding.txn;
    let Some(home) = load::<LocatedTimestampManifest>(tx, &home_key(txn.request_id))? else {
        return reject(tx, StatusFault::InvalidBinding);
    };
    if home.locator != *locator {
        return reject(tx, StatusFault::TransitionConflict);
    }
    let identity =
        load::<IdentityRecord>(tx, &key(IDENTITIES, txn.request_id))?.ok_or_else(unknown)?;
    if let Some(record) = verify_in_apply(tx, group, &identity, &home, current, Some(position))? {
        if record.completion() != completion {
            return reject(tx, StatusFault::TransitionConflict);
        }
        return commit(tx, StatusReply::TimestampPointProjected(Box::new(record)));
    }

    validate_completion(completion, &home, current)?;
    if !valid_position(position) {
        return Err(unknown());
    }
    let record = TimestampPointProjection {
        completion: completion.clone(),
        projected_at: position,
    };
    let encoded = tx.encode_value(&StoredProjection {
        digest: record.digest()?,
        record: record.clone(),
    })?;
    if encoded.as_slice().len() > MAX_POINT_PROJECTION_BYTES {
        return Err(unknown());
    }
    let state_key = txn_key(b't', txn, &[]);
    let mut state: Transaction = load(tx, &state_key)?.ok_or_else(unknown)?;
    state.outcome = completion.decision().outcome().clone();
    state.resolved = 1;
    state.settled_at = Some(position);
    tx.put(&txn_key(b'q', txn, &[]), encoded.as_slice())?;
    put(tx, &resolution_key(locator), &record.evidence())?;
    decide(tx, &state_key, &mut state, record.statement())?;
    commit(tx, StatusReply::TimestampPointProjected(Box::new(record)))
}

impl StatusHandle {
    pub(crate) async fn apply_timestamp_point_projection(
        &self,
        authority: Authority,
        proof: VerifiedTimestampPointResolution,
    ) -> Result<(TimestampPointProjection, Arc<ScratchLease>), ProtocolError> {
        if &proof.completion().decision().locator().binding.status_group != self.identity() {
            return Err(invalid());
        }
        let (completion, charge) = proof.into_parts();
        let applied = self
            .submit(StatusCommand::ProjectTimestampPoint {
                authority,
                completion: Box::new(completion),
            })
            .await;
        drop(charge);
        let (_, reply, owner) = applied?.into_parts()?;
        let StatusReply::TimestampPointProjected(record) = reply else {
            return Err(unknown());
        };
        Ok((*record, owner))
    }

    pub(crate) async fn read_timestamp_point_projection(
        &self,
        authority: Authority,
        locator: TimestampDecisionLocator,
    ) -> Result<Option<(TimestampPointProjection, Arc<ScratchLease>)>, ProtocolError> {
        let raw = self
            .timestamp_point_projection_bytes(authority, locator.clone())
            .await?;
        let (record, owner): (Option<TimestampPointProjection>, _) =
            self.decode_timestamp_point(raw).await?;
        if let Some(record) = &record {
            record.validate_for(&locator)?;
        }
        Ok(record.map(|record| (record, owner)))
    }

    pub(crate) async fn timestamp_point_projection_bytes(
        &self,
        authority: Authority,
        locator: TimestampDecisionLocator,
    ) -> Result<ChargedBytes, ProtocolError> {
        locator.validate()?;
        if &locator.binding.status_group != self.identity() {
            return Err(invalid());
        }
        let group = self.identity().clone();
        let epoch = self.scoped;
        let barrier = self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(READ_BUDGET, move |view| {
                check_fence(view, authority, epoch)?;
                let txn = locator.binding.txn;
                let home: LocatedTimestampManifest =
                    read(view, &home_key(txn.request_id))?.ok_or_else(unknown)?;
                if home.locator != locator {
                    return Err(invalid());
                }
                let identity: IdentityRecord =
                    read(view, &key(IDENTITIES, txn.request_id))?.ok_or_else(unknown)?;
                let record = verify_in_read(
                    view,
                    &group,
                    &identity,
                    &home,
                    authority,
                    Some(barrier.applied),
                )?;
                view.encode(&record)
            })
            .await
    }
}
