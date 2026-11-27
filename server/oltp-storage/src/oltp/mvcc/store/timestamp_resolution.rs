//! Native Data settlement and exact evidence for a Timestamp decision home.
use super::consensus::Applied;
use super::*;
use crate::oltp::pool::{ByteClass, GroupRead};
use nabedra_oltp_types::{
    authority::{PointDecision, ScopedCatalogRead},
    types::AbortReason,
};

#[doc(hidden)]
pub use nabedra_oltp_types::authority::{SealedTimestampPointDecision, TimestampPointResolution};

const REJECTION: u8 = b'j';
const COMPLETION: u8 = b'P';

pub(super) fn locator_heap_bytes(locator: &TimestampDecisionLocator) -> usize {
    let binding = &locator.binding;
    std::mem::size_of_val(locator)
        + binding.status_group.database.capacity_bytes()
        + binding.timestamps_group.database.capacity_bytes()
        + binding.participant.group.database.capacity_bytes()
}

pub fn matches_scope(locator: &TimestampDecisionLocator, scope: ScopedCatalogRead) -> bool {
    locator.binding.matches_scope(scope)
}

pub(super) fn eligible_config(config: &Config, locator: &TimestampDecisionLocator) -> bool {
    config.schema_epoch == locator.binding.participant.schema_epoch
        && config.routing_epoch == locator.binding.participant.routing_epoch
        && config.split_source.is_none()
        && config.split_child.is_none_or(|child| {
            child.complete && locator.binding.participant.read_ts >= child.read_ts
        })
}

fn authority_in_range(authority: Authority, original: Authority, current: Authority) -> bool {
    authority.run_id == original.run_id
        && authority.owner_term >= original.owner_term
        && authority.owner_term <= current.owner_term
        && (authority.owner_term != current.owner_term || authority == current)
}

/// The fixed page must leave room for the point locator and scoped Raft
/// envelope before the coordinator installs the immutable Status home.
pub fn point_page_fits(page: &ParticipantCommand) -> Result<bool, ProtocolError> {
    let ParticipantCommand::PreparePage {
        txn,
        manifest,
        plan,
        writes,
        guards,
        ..
    } = page
    else {
        return Ok(false);
    };
    if !eligible_page(plan, writes, guards) || !page_fits(txn, manifest, writes)? {
        return Ok(false);
    }
    Ok(encoded_length(page, super::super::MAX_FIXED_COMMAND_BYTES - (9 << 10)).is_ok())
}

fn eligible_page(
    plan: &ParticipantPlan,
    writes: &[PreparedWrite],
    guards: &[lifecycle::SealedLifecycleGuard],
) -> bool {
    plan.page_count == 1
        && plan.write_count == 1
        && writes.len() == 1
        && guards.len() == 1
        && guards[0].mode() == lifecycle::GuardMode::Shared
        && guards[0].binding().lifecycle_version.is_some()
        && guards[0].binding().schema_epoch == plan.schema_epoch
        && matches!(&writes[0].key, MvccKey::Row { table_id, .. }
            if *table_id == guards[0].binding().table_id)
        && matches!(&writes[0].change, Change::Put(_))
        && writes[0]
            .expected_version
            .is_some_and(|version| version <= plan.read_ts)
}

pub fn matches_page(
    locator: &TimestampDecisionLocator,
    page: &ParticipantCommand,
) -> Result<bool, ProtocolError> {
    let ParticipantCommand::PreparePage {
        page_index,
        previous_root,
        writes,
        ..
    } = page
    else {
        return Ok(false);
    };
    if !matches_page_binding(locator, page)? {
        return Ok(false);
    }
    Ok(page_root(*previous_root, *page_index, writes)? == locator.binding.participant.write_root)
}

pub(super) fn matches_page_binding(
    locator: &TimestampDecisionLocator,
    page: &ParticipantCommand,
) -> Result<bool, ProtocolError> {
    let ParticipantCommand::PreparePage {
        txn,
        request,
        manifest,
        plan,
        page_index,
        previous_root,
        writes,
        guards,
        ..
    } = page
    else {
        return Ok(false);
    };
    locator.validate()?;
    let binding = &locator.binding;
    Ok(*txn == binding.txn
        && *request == binding.request
        && *manifest == locator.manifest
        && *plan == binding.participant
        && *page_index == 0
        && eligible_page(plan, writes, guards)
        && *previous_root == initial_root(*txn, plan)?)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampPointRejection {
    home: Digest,
    authority: Authority,
    rejected_at: AppliedPosition,
}

impl TimestampPointRejection {
    pub fn rejected_at(&self) -> AppliedPosition {
        self.rejected_at
    }

    pub fn validate_for(
        &self,
        locator: &TimestampDecisionLocator,
        current: Authority,
    ) -> Result<(), ProtocolError> {
        if self.home != locator.digest()?
            || !authority_in_range(self.authority, locator.binding.created_authority, current)
            || !valid_position(self.rejected_at)
        {
            return Err(recovery_unknown());
        }
        Ok(())
    }
}

pub(super) fn rejection_exists(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
) -> Result<bool, ProtocolError> {
    Ok(tx.pinned(&txn_key(REJECTION, txn))?.is_some())
}

pub(super) fn completion_exists(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
) -> Result<bool, ProtocolError> {
    Ok(tx.pinned(&txn_key(COMPLETION, txn))?.is_some())
}

pub(super) fn apply_rejection(
    tx: &mut GroupTransaction<'_>,
    locator: &TimestampDecisionLocator,
    authority: Authority,
    position: AppliedPosition,
    write: &PreparedWrite,
) -> Result<Transition, ProtocolError> {
    let txn = locator.binding.txn;
    let key = txn_key(REJECTION, txn);
    if let Some(stored) = load::<TimestampPointRejection>(tx, &key)? {
        stored.validate_for(locator, authority)?;
        if !at_or_before(stored.rejected_at, position) {
            return Err(recovery_unknown());
        }
        return commit(
            tx,
            &ParticipantReply::TimestampPointRejected(stored.rejected_at),
        );
    }
    if tx.pinned(&txn_key(b't', txn))?.is_some()
        || tx.pinned(&txn_key(b'd', txn))?.is_some()
        || tx.pinned(&txn_key(RESOLUTION_WITNESS, txn))?.is_some()
        || tx.pinned(&txn_key(b'b', txn))?.is_some()
        || completion_exists(tx, txn)?
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let length = key_length(&write.key)?;
    let permanent = tx.with_scratch_buffer(length + 9, |tx, buffer| {
        let end = length + 1;
        buffer[0] = HEAD;
        encode_key(&write.key, &mut buffer[1..end]);
        let head = load::<Head>(tx, &buffer[..end])?.unwrap_or_default();
        // A competing intent or a missing head leaves the attempt reconciling.
        let Some(newer) = head
            .version
            .filter(|version| head.intent.is_none() && Some(*version) > write.expected_version)
        else {
            return Ok(false);
        };
        buffer[0] = VALUE;
        buffer[end..].copy_from_slice(&(!newer.get()).to_be_bytes());
        if tx.pinned(buffer)?.is_none() {
            return Err(recovery_unknown());
        }
        Ok(true)
    })?;
    if !permanent {
        return reject(tx, Fault::Incomplete);
    }
    let record = TimestampPointRejection {
        home: locator.digest()?,
        authority,
        rejected_at: position,
    };
    tx.put_serialized(&key, &record)?;
    commit(tx, &ParticipantReply::TimestampPointRejected(position))
}

pub(super) fn apply_resolution(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    position: AppliedPosition,
    scope: Option<ScopedCatalogRead>,
    proof: &SealedTimestampPointDecision,
) -> Result<Transition, ProtocolError> {
    let locator = proof.locator();
    let binding = &locator.binding;
    let authority = config.authority;
    let original = binding.created_authority;
    proof.validate_for(locator)?;
    if &binding.participant.group != group
        || authority.owner_term < proof.decision().authority().owner_term
        || (authority.owner_term == proof.decision().authority().owner_term
            && authority != proof.decision().authority())
        || match scope {
            Some(scope) => !matches_scope(locator, scope) || authority.run_id != original.run_id,
            None => {
                authority.run_id == original.run_id || authority.owner_term <= original.owner_term
            }
        }
    {
        return reject(tx, Fault::InvalidBinding);
    }

    let txn = binding.txn;
    let key = txn_key(b't', txn);
    let completion_key = txn_key(COMPLETION, txn);
    let resolution_key = txn_key(b'd', txn);
    let rollback_key = txn_key(b'b', txn);
    let decision = proof.decision().decision();
    let mut state = load::<Txn>(tx, &key)?;
    match decision {
        TxnDecision::Commit { .. } if rejection_exists(tx, txn)? => {
            return Err(recovery_unknown());
        }
        TxnDecision::Abort {
            reason: AbortReason::Conflict,
        } => {
            let rejection = load(tx, &txn_key(REJECTION, txn))?.ok_or_else(recovery_unknown)?;
            verify_rejection(locator, proof.decision(), &rejection)?;
            if state.is_some() {
                return Err(recovery_unknown());
            }
        }
        _ => {}
    }
    if tx.pinned(&txn_key(RESOLUTION_WITNESS, txn))?.is_some() {
        return Err(recovery_unknown());
    }
    if let Some(completed) = load::<TimestampPointResolution>(tx, &completion_key)? {
        let evidence = completed.evidence();
        completed.validate_for(locator)?;
        let resolution = load::<ResolutionEvidence>(tx, &resolution_key)?;
        let rollback = load::<Digest>(tx, &rollback_key)?;
        if completed.proof() != proof
            || resolution.as_ref() != Some(&evidence)
            || !at_or_before(completed.completed_at(), position)
        {
            return Err(recovery_unknown());
        }
        check_completion_state(locator, &completed, state.as_ref(), rollback)?;
        let writes = state.as_ref().map_or(0, |state| state.writes);
        return commit(
            tx,
            &ParticipantReply::TimestampPointResolved { evidence, writes },
        );
    }
    if !eligible_config(config, locator)
        || tx.pinned(&resolution_key)?.is_some()
        || tx.pinned(&rollback_key)?.is_some()
    {
        return Err(recovery_unknown());
    }
    if let Some(state) = &state {
        check_state(locator, state)?;
        if state.decision.is_some() || state.resolved != 0 {
            return Err(recovery_unknown());
        }
    }

    if matches!(decision, TxnDecision::Commit { .. }) {
        let state = state.as_ref().ok_or_else(recovery_unknown)?;
        let seal = check_seal(locator, state, proof.decision().authority())?;
        if seal.sealed_at != proof.decision().resolution_seal()? {
            return Err(recovery_unknown());
        }
    }

    let completed = TimestampPointResolution::new(proof.clone(), position);
    completed.validate_for(locator)?;
    encoded_length(&completed, METADATA_BYTES).map_err(|_| recovery_unknown())?;
    if let Some(state) = &mut state {
        resolve_writes(tx, state, txn, state.writes, decision)?;
        state.decision = Some(decision);
        state.resolved = state.writes;
        tx.put_serialized(&key, state)?;
    }
    if matches!(decision, TxnDecision::Abort { .. }) {
        tx.put_serialized(&rollback_key, &proof.proof_digest())?;
    }
    let evidence = completed.evidence();
    tx.put_serialized(&completion_key, &completed)?;
    tx.put_serialized(&resolution_key, &evidence)?;
    let writes = state.as_ref().map_or(0, |state| state.writes);
    commit(
        tx,
        &ParticipantReply::TimestampPointResolved { evidence, writes },
    )
}

fn check_state(locator: &TimestampDecisionLocator, state: &Txn) -> Result<(), ProtocolError> {
    let binding = &locator.binding;
    if state.point_home != Some(locator.digest()?)
        || state.scoped_queue
            != Some(scoped_queue::QueueBinding::from_scope(
                binding.original_scope,
            ))
        || state.request != binding.request
        || state.manifest != locator.manifest
        || state.plan != binding.participant
        || state.writes != 1
        || state.next_page != 1
        || state.root != binding.participant.write_root
    {
        return Err(recovery_unknown());
    }
    Ok(())
}

pub(super) fn check_seal<'a>(
    locator: &TimestampDecisionLocator,
    state: &'a Txn,
    authority: Authority,
) -> Result<&'a PreparedParticipant, ProtocolError> {
    check_state(locator, state)?;
    let seal = state.sealed.as_ref().ok_or_else(recovery_unknown)?;
    if seal.txn != locator.binding.txn
        || seal.manifest != locator.manifest
        || seal.participant != locator.binding.participant
        || seal.result_digest != locator.binding.result_digest
        || !authority_in_range(seal.authority, locator.binding.created_authority, authority)
        || !valid_position(seal.sealed_at)
    {
        return Err(recovery_unknown());
    }
    Ok(seal)
}

fn check_completion_state(
    locator: &TimestampDecisionLocator,
    completed: &TimestampPointResolution,
    state: Option<&Txn>,
    rollback: Option<Digest>,
) -> Result<(), ProtocolError> {
    let decision = completed.decision().decision();
    if let Some(state) = state {
        check_state(locator, state)?;
        if state.decision != Some(decision) || state.resolved != state.writes {
            return Err(recovery_unknown());
        }
    }
    match decision {
        TxnDecision::Commit { .. } => {
            let seal = check_seal(
                locator,
                state.ok_or_else(recovery_unknown)?,
                completed.decision().authority(),
            )?;
            if rollback.is_some() || seal.sealed_at != completed.decision().resolution_seal()? {
                return Err(recovery_unknown());
            }
        }
        TxnDecision::Abort {
            reason: AbortReason::Conflict,
        } if rollback == Some(completed.proof().proof_digest()) && state.is_none() => {}
        TxnDecision::Abort {
            reason: AbortReason::AbandonedByFence,
        } if rollback == Some(completed.proof().proof_digest()) => {}
        _ => return Err(recovery_unknown()),
    }
    Ok(())
}

pub(super) fn read_completion(
    view: &mut GroupRead<'_>,
    locator: &TimestampDecisionLocator,
    applied: AppliedPosition,
) -> Result<ChargedBytes, ProtocolError> {
    let txn = locator.binding.txn;
    let completed: TimestampPointResolution = read_record(view, &txn_key(COMPLETION, txn))?;
    let evidence: ResolutionEvidence = read_record(view, &txn_key(b'd', txn))?;
    let state = read_optional(view, &txn_key(b't', txn))?;
    let rollback = read_optional(view, &txn_key(b'b', txn))?;
    completed.validate_for(locator)?;
    check_completion_state(locator, &completed, state.as_ref(), rollback)?;
    match completed.decision().decision() {
        TxnDecision::Commit { .. } if view.pinned(&txn_key(REJECTION, txn))?.is_some() => {
            return Err(recovery_unknown());
        }
        TxnDecision::Abort {
            reason: AbortReason::Conflict,
        } => {
            let rejection = read_record(view, &txn_key(REJECTION, txn))?;
            verify_rejection(locator, completed.decision(), &rejection)?;
        }
        _ => {}
    }
    if evidence != completed.evidence()
        || !at_or_before(completed.completed_at(), applied)
        || view.pinned(&txn_key(RESOLUTION_WITNESS, txn))?.is_some()
    {
        return Err(recovery_unknown());
    }
    view.encode(&completed)
}

fn verify_rejection(
    locator: &TimestampDecisionLocator,
    decision: &PointDecision,
    rejection: &TimestampPointRejection,
) -> Result<(), ProtocolError> {
    rejection.validate_for(locator, decision.authority())?;
    if Some(rejection.rejected_at()) != decision.rejection_at() {
        return Err(recovery_unknown());
    }
    Ok(())
}

pub(super) fn read_rejection(
    view: &mut GroupRead<'_>,
    locator: &TimestampDecisionLocator,
    authority: Authority,
    applied: AppliedPosition,
) -> Result<ChargedBytes, ProtocolError> {
    let txn = locator.binding.txn;
    let rejection: TimestampPointRejection = read_record(view, &txn_key(REJECTION, txn))?;
    rejection.validate_for(locator, authority)?;
    if !at_or_before(rejection.rejected_at, applied) || view.pinned(&txn_key(b't', txn))?.is_some()
    {
        return Err(recovery_unknown());
    }
    view.encode(&rejection)
}

pub(super) fn at_or_before(position: AppliedPosition, applied: AppliedPosition) -> bool {
    valid_position(position)
        && position.term <= applied.term
        && (position.index < applied.index || position == applied)
}

fn valid_position(position: AppliedPosition) -> bool {
    position.term != 0 && position.index != 0 && position.node_id != 0
}

pub(super) fn read_record<T: serde::de::DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<T, ProtocolError> {
    read_optional(view, key)?.ok_or_else(recovery_unknown)
}

fn read_optional<T: serde::de::DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    view.pinned(key)?
        .map(|raw| {
            if raw.len() > METADATA_BYTES {
                return Err(recovery_unknown());
            }
            serde_json::from_slice(&raw).map_err(|_| recovery_unknown())
        })
        .transpose()
}

impl ParticipantHandle {
    pub async fn prepare_timestamp_point(
        &self,
        locator: TimestampDecisionLocator,
        page: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::PrepareTimestampPoint {
                locator: Box::new(locator),
                page,
            },
            self.resources(),
            ByteClass::Data,
        )?)
        .await
    }

    pub async fn reject_timestamp_point(
        &self,
        locator: TimestampDecisionLocator,
        page: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::RejectTimestampPoint {
                locator: Box::new(locator),
                page,
            },
            self.resources(),
            ByteClass::Data,
        )?)
        .await
    }

    pub async fn seal_timestamp_point(
        &self,
        authority: Authority,
        locator: TimestampDecisionLocator,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::SealTimestampPoint {
                authority,
                locator: Box::new(locator),
            },
            self.resources(),
            ByteClass::Progress,
        )?)
        .await
    }

    pub async fn resolve_timestamp_point(
        &self,
        authority: Authority,
        locator: TimestampDecisionLocator,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.remote
            .as_ref()
            .ok_or_else(invalid)?
            .resolve_timestamp_point(authority, locator)
            .await
    }
}
