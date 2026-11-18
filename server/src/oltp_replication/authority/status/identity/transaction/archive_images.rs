//! Native metadata images for an offline committed-history genesis. The fixed
//! allowed key/type set excludes query bodies, authorization and pending work.
use super::*;
use crate::oltp::{
    backup::{
        history::{HistoryFrame, MetadataImage},
        restore::OfflineRestoreTarget,
    },
    pool::GroupStore,
};
use crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure;

const RESTORED_TRANSACTIONS: &[u8] = b"status/restored-history/transactions/";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoredTransactionRecord {
    origin: RestoredHistoryOrigin,
    source_group: GroupIdentity,
    commit_ts: CommitTs,
    original_key: Vec<u8>,
    original_value: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataArchivePage {
    pub group: GroupIdentity,
    pub cut: CommitTs,
    pub after: Option<Vec<u8>>,
    pub exhausted: bool,
    pub items: Page<MetadataImage>,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}
impl MetadataArchivePage {
    pub(crate) fn new(
        group: GroupIdentity,
        cut: CommitTs,
        after: Option<Vec<u8>>,
        exhausted: bool,
        items: Page<MetadataImage>,
    ) -> Self {
        Self {
            group,
            cut,
            after,
            exhausted,
            items,
            owner: None,
        }
    }

    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) {
        self.owner = Some(owner);
    }
}
impl StatusHandle {
    pub(crate) async fn native_archive_page(
        &self,
        closure: &VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> Result<MetadataArchivePage, ProtocolError> {
        if !closure.covers_status(self.identity())
            || after
                .as_ref()
                .is_some_and(|key| key.len() > 4096 || !key.starts_with(b"status/"))
        {
            return Err(invalid());
        }
        self.consensus.ensure_linearizable().await?;
        let group = self.identity().clone();
        let authority = closure.authority();
        let cut = closure.cut();
        let applied = closure.status_applied_at();
        let raw = self
            .machine
            .store()
            .read_archive(super::archive::ARCHIVE_BUDGET, move |view, current| {
                if current.index < applied.index
                    || read::<Authority>(view, STATE)?
                        .is_none_or(|current| current.owner_term < authority.owner_term)
                {
                    return Err(unknown());
                }
                let mut cursor = after.clone().unwrap_or_else(|| b"status/".to_vec());
                if after.is_some() {
                    cursor.push(0);
                }
                let mut next = after;
                let mut items = Vec::new();
                let mut used = 4096usize;
                for _ in 0..64 {
                    let Some(row) = view.first_from(b"status/", &cursor)? else {
                        break;
                    };
                    let key = row.key();
                    let mut projected = None;
                    let mut committed_at = None;
                    let include = if key.starts_with(IDENTITIES) {
                        if key.len() != IDENTITIES.len() + 16 || row.value().len() > 64 << 10 {
                            return Err(unknown());
                        }
                        let identity: IdentityRecord =
                            serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                        if key != super::super::key(IDENTITIES, identity.reservation.request_id) {
                            return Err(unknown());
                        }
                        if let Some(identity) = project_identity(identity, applied) {
                            projected =
                                Some(serde_json::to_string(&identity).map_err(|_| unknown())?);
                            true
                        } else {
                            false
                        }
                    } else if key.starts_with(RESTORED_TRANSACTIONS) {
                        if row.value().len() > 64 << 10 {
                            return Err(unknown());
                        }
                        let history: RestoredTransactionRecord =
                            serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                        validate_restored(&history)?;
                        if history.origin.through_cut > cut {
                            return Err(unknown());
                        }
                        committed_at = Some(history.commit_ts);
                        true
                    } else if key.starts_with(TXNS) {
                        if key.len() < TXNS.len() + 25
                            || key.len() > 4096
                            || row.value().len() > 64 << 10
                        {
                            return Err(unknown());
                        }
                        let offset = TXNS.len() + 1;
                        let txn = TxnId {
                            request_id: Uuid::from_slice(&key[offset..offset + 16])
                                .map_err(|_| unknown())?,
                            attempt: u64::from_be_bytes(
                                key[offset + 16..offset + 24]
                                    .try_into()
                                    .map_err(|_| unknown())?,
                            ),
                        };
                        let state: Transaction =
                            read(view, &txn_key(b't', txn, &[]))?.ok_or_else(unknown)?;
                        if state.txn != txn {
                            return Err(unknown());
                        }
                        if let Some(
                            decision @ DecisionStatement {
                                decision: TxnDecision::Commit { commit_ts },
                                ..
                            },
                        ) = &state.decision
                        {
                            if *commit_ts <= cut {
                                if state.resolved != state.expected
                                    || state.settled_at.is_none()
                                    || decision.decided_at.index > applied.index
                                    || state.settled_at.is_some_and(|at| at.index > applied.index)
                                {
                                    return Err(unknown());
                                }
                                validate_auxiliary(key[TXNS.len()], row.value())?;
                                committed_at = Some(*commit_ts);
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if include {
                        let value = match projected {
                            Some(value) => value,
                            None => std::str::from_utf8(row.value())
                                .map_err(|_| unknown())?
                                .to_owned(),
                        };
                        let cost = value
                            .len()
                            .checked_mul(2)
                            .and_then(|n| n.checked_add(key.len() * 4 + 4096))
                            .ok_or_else(unknown)?;
                        if used
                            .checked_add(cost)
                            .is_none_or(|n| n > (128 << 10) - 4096)
                        {
                            if items.is_empty() {
                                return Err(unknown());
                            }
                            break;
                        }
                        used += cost;
                        items.push(MetadataImage {
                            group: group.clone(),
                            key: key.to_vec(),
                            commit_ts: committed_at,
                            value,
                        });
                    }
                    next = Some(key.to_vec());
                    cursor = key.to_vec();
                    cursor.push(0);
                }
                let exhausted = view.first_from(b"status/", &cursor)?.is_none();
                view.encode(&MetadataArchivePage {
                    group,
                    cut,
                    after: next,
                    exhausted,
                    items: Page::new(items).map_err(|_| unknown())?,
                    owner: None,
                })
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch_wait(
                    ByteClass::Data,
                    crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
                    0,
                )
                .await?,
        );
        let mut page: MetadataArchivePage =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        page.owner = Some(owner);
        Ok(page)
    }
}
fn validate_auxiliary(kind: u8, value: &[u8]) -> Result<(), ProtocolError> {
    match kind {
        b't' => {
            let _: Transaction = serde_json::from_slice(value).map_err(|_| unknown())?;
        }
        b'g' | b'o' => {
            let plan: ParticipantPlan = serde_json::from_slice(value).map_err(|_| unknown())?;
            plan.validate().map_err(|_| unknown())?;
        }
        b'e' => {
            let _: PreparedParticipant = serde_json::from_slice(value).map_err(|_| unknown())?;
        }
        b'r' => {
            let _: ResolutionEvidence = serde_json::from_slice(value).map_err(|_| unknown())?;
        }
        b'p' => {
            let _: PageReceipt = serde_json::from_slice(value).map_err(|_| unknown())?;
        }
        _ => return Err(unknown()),
    }
    Ok(())
}
fn validate_restored(history: &RestoredTransactionRecord) -> Result<(), ProtocolError> {
    if history.origin.restore_id.is_nil()
        || history.origin.source_archive.is_nil()
        || history.origin.source_cluster != history.source_group.cluster_id
        || !matches!(history.source_group.kind, GroupKind::Status { .. })
        || history.commit_ts > history.origin.through_cut
        || !history.original_key.starts_with(TXNS)
        || history.original_key.len() < TXNS.len() + 25
        || history.original_value.len() > 64 << 10
    {
        return Err(invalid());
    }
    validate_auxiliary(
        history.original_key[TXNS.len()],
        history.original_value.as_bytes(),
    )
}
pub(crate) async fn install_metadata(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    frames: Vec<OwnedCommand<HistoryFrame>>,
) -> Result<(), ProtocolError> {
    if frames.is_empty()
        || frames.len() > 128
        || !matches!(store.identity().kind, GroupKind::Status { .. })
    {
        return Err(invalid());
    }
    let statement = target.statement().clone();
    let group = store.identity().clone();
    let origin = RestoredHistoryOrigin {
        restore_id: statement.restore_id,
        source_cluster: statement.source_cluster,
        source_archive: statement.source_archive,
        source_root: statement.source_root,
        through_cut: statement.target_cut,
    };
    store.restore_genesis_batch(target, TransactionBudget { scratch_bytes: 32 << 20, batch_bytes: 32 << 20, output_bytes: 4096 }, move |tx| {
        if tx.pinned(STATE)?.is_some() { return Err(unknown()); }
        for frame in &frames {
            let HistoryFrame::Metadata(image) = frame.payload()? else { return Err(invalid()); };
            if image.group.group_id != group.group_id || image.group.cluster_id != statement.source_cluster || image.group.database != group.database
                || image.value.len() > 64 << 10 || image.key.len() > 4096 { return Err(invalid()); }
            if image.commit_ts.is_some_and(|commit_ts| commit_ts > statement.target_cut) { continue; }
            if image.key.starts_with(IDENTITIES) {
                if image.commit_ts.is_some() { return Err(invalid()); }
                let mut identity: IdentityRecord = serde_json::from_str(&image.value).map_err(|_| unknown())?;
                if image.key != key(IDENTITIES, identity.reservation.request_id) { return Err(invalid()); }
                if identity.terminal.as_ref().is_some_and(|terminal| matches!(terminal, TerminalIdentity::Committed { decision: DecisionStatement { decision: TxnDecision::Commit { commit_ts }, .. }, .. } if *commit_ts > statement.target_cut)) {
                    identity.terminal = None;
                }
                if identity.restore_origin.is_none() { identity.restore_origin = Some(origin.clone()); }
                tx.put_serialized(&image.key, &identity)?;
            } else if image.key.starts_with(RESTORED_TRANSACTIONS) {
                let history: RestoredTransactionRecord = serde_json::from_str(&image.value).map_err(|_| unknown())?;
                validate_restored(&history)?;
                if image.commit_ts != Some(history.commit_ts) { return Err(invalid()); }
                tx.put(&image.key, image.value.as_bytes())?;
            } else if image.key.starts_with(TXNS) {
                if image.key.len() < TXNS.len() + 25 { return Err(invalid()); }
                let kind = image.key[TXNS.len()];
                validate_auxiliary(kind, image.value.as_bytes())?;
                if kind == b't' {
                    let state: Transaction = serde_json::from_str(&image.value).map_err(|_| unknown())?;
                    let Some(DecisionStatement { decision: TxnDecision::Commit { commit_ts }, .. }) = state.decision else { return Err(invalid()); };
                    if image.commit_ts != Some(commit_ts) { return Err(invalid()); }
                    if state.resolved != state.expected || state.settled_at.is_none() { return Err(invalid()); }
                }
                let commit_ts = image.commit_ts.ok_or_else(invalid)?;
                let restored = RestoredTransactionRecord { origin: origin.clone(), source_group: image.group.clone(), commit_ts,
                    original_key: image.key.clone(), original_value: image.value.clone() };
                let key = [RESTORED_TRANSACTIONS, origin.source_root.as_bytes(), image.key.as_slice()].concat();
                tx.put_serialized(&key, &restored)?;
            } else { return Err(invalid()); }
        }
        Ok(())
    }).await
}

/// Preserve the source reservation and exact terminal result. Nonterminal and
/// excluded identities remain metadata-only tombstones in the target's new run.
pub(crate) async fn install_identity_history(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    frame: OwnedCommand<HistoryFrame>,
) -> Result<(), ProtocolError> {
    let statement = target.statement().clone();
    let group = store.identity().clone();
    store.restore_genesis_batch(target, TransactionBudget { scratch_bytes: 32 << 20, batch_bytes: 32 << 20, output_bytes: 4096 }, move |tx| {
        if tx.pinned(STATE)?.is_some() { return Err(unknown()); }
        let HistoryFrame::Identity { status, identity, .. } = frame.payload()? else { return Err(invalid()); };
        if status.group_id != group.group_id || status.cluster_id != statement.source_cluster || status.database != group.database
            || !matches!(status.kind, GroupKind::Status { .. }) || identity.reservation.original_run.is_nil()
            || identity.reservation.request_id.is_nil() { return Err(invalid()); }
        let mut terminal = None;
        let mut rejection = None;
        match &identity.disposition {
            ArchivedIdentityDisposition::PreCutCommitted { decision, outcome } => {
                if decision.txn.request_id != identity.reservation.request_id || decision.request.digest != identity.reservation.digest
                    || decision.manifest.status_group_id != status.group_id { return Err(invalid()); }
                if matches!(decision.decision, TxnDecision::Commit { commit_ts } if commit_ts <= statement.target_cut) {
                    terminal = Some(TerminalIdentity::Committed { reservation: identity.reservation, decision: decision.clone(), outcome: outcome.clone() });
                }
            }
            ArchivedIdentityDisposition::CurrentFailed { terminal: original } => {
                terminal = Some(original.clone());
                if matches!(original, TerminalIdentity::Committed { decision: DecisionStatement { decision: TxnDecision::Commit { commit_ts }, .. }, .. } if *commit_ts > statement.target_cut) { terminal = None; }
            }
            ArchivedIdentityDisposition::CurrentRejected { rejection: original } => rejection = Some(original.clone()),
            ArchivedIdentityDisposition::ExpiredRead { terminal: original } => {
                if !matches!(original, TerminalIdentity::ReadExpired { .. }) { return Err(invalid()); }
                terminal = Some(original.clone());
            }
            ArchivedIdentityDisposition::RestoreExcluded { .. } | ArchivedIdentityDisposition::Reserved => {}
        }
        let origin = identity.origin.clone().unwrap_or(RestoredHistoryOrigin {
            restore_id: statement.restore_id, source_cluster: statement.source_cluster,
            source_archive: statement.source_archive, source_root: statement.source_root,
            through_cut: statement.target_cut,
        });
        if origin.restore_id.is_nil() || origin.source_cluster.is_nil() || origin.source_archive.is_nil() || origin.through_cut > statement.target_cut { return Err(invalid()); }
        let restored = IdentityRecord { reservation: identity.reservation, restore_origin: Some(origin), reserved_at: identity.reserved_at, terminal, rejection, retirement: None };
        tx.put_serialized(&key(IDENTITIES, restored.reservation.request_id), &restored)?;
        Ok(())
    }).await
}

pub(crate) async fn install_archived_decision(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    frame: OwnedCommand<HistoryFrame>,
    source: GroupIdentity,
) -> Result<(), ProtocolError> {
    let statement = target.statement().clone();
    let group = store.identity().clone();
    store
        .restore_genesis_batch(
            target,
            TransactionBudget {
                scratch_bytes: 32 << 20,
                batch_bytes: 32 << 20,
                output_bytes: 4096,
            },
            move |tx| {
                let HistoryFrame::BeginTransaction(decision) = frame.payload()? else {
                    return Err(invalid());
                };
                if source.group_id != group.group_id
                    || source.cluster_id != statement.source_cluster
                    || source.database != group.database
                    || decision.statement.manifest.status_group_id != source.group_id
                {
                    return Err(invalid());
                }
                let TxnDecision::Commit { commit_ts } = decision.statement.decision else {
                    return Err(invalid());
                };
                if commit_ts > statement.target_cut {
                    return Ok(());
                }
                let key = [
                    b"status/restored-history/archive-decisions/".as_slice(),
                    statement.source_root.as_bytes(),
                    decision.statement.txn.request_id.as_bytes(),
                    &decision.statement.txn.attempt.to_be_bytes(),
                ]
                .concat();
                // These original immutable source statements are never visible to
                // lookup_decision or Resolve in the target's live TXNS namespace.
                tx.put_serialized(&key, &(source, decision))?;
                Ok(())
            },
        )
        .await
}

/// One immutable metadata projection at the advertised Status applied cut.
/// Data-cut clipping happens only when restoring the known identity, so the
/// coverage digest also protects metadata-only pending/post-cut UUIDs.
fn project_identity(
    mut record: IdentityRecord,
    applied: AppliedPosition,
) -> Option<IdentityRecord> {
    let restored = record.restore_origin.is_some();
    if !restored
        && record
            .reserved_at
            .is_some_and(|at| at.index > applied.index)
    {
        return None;
    }
    record.terminal = record.terminal.filter(|terminal| {
        restored
            || match terminal {
                TerminalIdentity::Committed { decision, .. } => {
                    decision.decided_at.index <= applied.index
                }
                TerminalIdentity::Failed { recorded_at, .. }
                | TerminalIdentity::ReadExpired { recorded_at, .. } => {
                    recorded_at.index <= applied.index
                }
            }
    });
    record.rejection = record
        .rejection
        .filter(|rejected| restored || rejected.recorded_at.index <= applied.index);
    record.retirement = None;
    Some(record)
}
pub(crate) fn identity_inventory_seed(
    group: &GroupIdentity,
    applied: AppliedPosition,
) -> Result<Digest, ProtocolError> {
    serialized_digest(&(b"nabedra/status-identities/v1".as_slice(), group, applied))
}
fn identity_step(root: Digest, record: &IdentityRecord) -> Result<Digest, ProtocolError> {
    serialized_digest(&(b"nabedra/status-identity/v1".as_slice(), root, record))
}
pub(crate) fn identity_metadata_step(
    shared: &crate::oltp::pool::WorkerSharedResources,
    root: Digest,
    image: &MetadataImage,
) -> Result<Option<(Uuid, Digest)>, ProtocolError> {
    if !image.key.starts_with(IDENTITIES) {
        return Ok(None);
    }
    let _owner = shared.reserve_scratch(
        ByteClass::Data,
        crate::oltp::mvcc::json_decode_budget(image.value.as_bytes())?,
    )?;
    let record: IdentityRecord = serde_json::from_str(&image.value).map_err(|_| unknown())?;
    if image.key != key(IDENTITIES, record.reservation.request_id) || image.commit_ts.is_some() {
        return Err(invalid());
    }
    Ok(Some((
        record.reservation.request_id,
        identity_step(root, &record)?,
    )))
}
#[derive(Serialize, Deserialize)]
struct InventoryFoldPage {
    count: u64,
    root: Digest,
    after: Option<Uuid>,
    exhausted: bool,
}
pub(super) async fn identity_inventory(
    handle: &StatusHandle,
    cut: &StatusArchiveCutStatement,
) -> Result<(u64, Digest), ProtocolError> {
    let mut count = 0u64;
    let mut root = identity_inventory_seed(&cut.group, cut.applied)?;
    let mut after = None;
    loop {
        let applied = cut.applied;
        let authority = cut.authority;
        let raw = handle
            .machine
            .store()
            .read_archive(super::archive::ARCHIVE_BUDGET, move |view, current| {
                if current.index < applied.index
                    || read::<Authority>(view, STATE)?
                        .is_none_or(|actual| actual.owner_term < authority.owner_term)
                {
                    return Err(unknown());
                }
                let mut cursor = after
                    .map(|id| key(IDENTITIES, id))
                    .unwrap_or_else(|| IDENTITIES.to_vec());
                if after.is_some() {
                    cursor.push(0);
                }
                let mut next = after;
                let mut folded = root;
                let mut added = 0u64;
                for _ in 0..64 {
                    let Some(row) = view.first_from(IDENTITIES, &cursor)? else {
                        break;
                    };
                    if row.key().len() != IDENTITIES.len() + 16 || row.value().len() > 64 << 10 {
                        return Err(unknown());
                    }
                    let record: IdentityRecord =
                        serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                    if row.key() != key(IDENTITIES, record.reservation.request_id) {
                        return Err(unknown());
                    }
                    next = Some(record.reservation.request_id);
                    cursor = row.key().to_vec();
                    cursor.push(0);
                    if let Some(record) = project_identity(record, applied) {
                        folded = identity_step(folded, &record)?;
                        added = added.checked_add(1).ok_or_else(unknown)?;
                    }
                }
                let exhausted = view.first_from(IDENTITIES, &cursor)?.is_none();
                view.encode(&InventoryFoldPage {
                    count: added,
                    root: folded,
                    after: next,
                    exhausted,
                })
            })
            .await?;
        let page: InventoryFoldPage =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        count = count.checked_add(page.count).ok_or_else(unknown)?;
        root = page.root;
        if page.exhausted {
            break;
        }
        if page.after == after {
            return Err(unknown());
        }
        after = page.after;
    }
    Ok((count, root))
}
