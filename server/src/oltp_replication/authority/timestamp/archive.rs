//! A backup cut closes only after every older allocated commit timestamp has
//! an immutable, fully resolved Status decision. Closure is durable metadata.
pub(crate) mod metadata;
use super::*;
use crate::oltp::backup::BackupOwnerRef;
use crate::oltp::pool::ByteClass;
use crate::oltp_replication::{authority::status::StatusService, owned::serialized_digest};
const CLOSURES: &[u8] = b"ts/archive-closure/v1/";
const PAGE_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 0,
    output_bytes: 128 << 10,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveClosureStatement {
    pub timestamps: GroupIdentity,
    pub status: GroupIdentity,
    pub authority: Authority,
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub archive_id: uuid::Uuid,
    pub base_exclusive: Option<CommitTs>,
    pub status_applied_at: AppliedPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_root: Option<Digest>,
    pub committed_count: u64,
    pub decision_root: Digest,
    pub allocation_count: u64,
    pub allocation_root: Digest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedArchiveClosure {
    statement: ArchiveClosureStatement,
    digest: Digest,
}
impl SealedArchiveClosure {
    fn verified(statement: ArchiveClosureStatement) -> Result<Self, ProtocolError> {
        let digest = serialized_digest(&(b"nabedra/archive-closure/v1", &statement))?;
        Ok(Self { statement, digest })
    }
    pub(super) fn heap_bytes(&self) -> usize {
        self.statement.timestamps.database.capacity_bytes()
            + self.statement.status.database.capacity_bytes()
    }
    fn validate(&self) -> Result<&ArchiveClosureStatement, ProtocolError> {
        if self.digest != serialized_digest(&(b"nabedra/archive-closure/v1", &self.statement))? {
            return Err(unknown());
        }
        Ok(&self.statement)
    }
}
impl VerifiedArchiveClosure {
    fn from_local(statement: ArchiveClosureStatement, owner: Arc<ScratchLease>) -> Self {
        Self {
            cut: statement.pin.read_ts,
            authority: statement.authority,
            status: statement.status.clone(),
            status_applied_at: statement.status_applied_at,
            archive_id: statement.archive_id,
            statement,
            _owner: owner,
        }
    }
    pub(crate) fn from_created(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: ArchiveClosureStatement,
        authority: Authority,
        owner: BackupOwnerRef,
        pin: StoragePin,
        archive_id: uuid::Uuid,
        base_exclusive: Option<CommitTs>,
        status: &GroupIdentity,
    ) -> Result<Self, ProtocolError> {
        context.timestamp(19, &statement)?;
        if context.group() != &statement.timestamps
            || statement.authority != authority
            || statement.owner != owner
            || statement.pin != pin
            || statement.archive_id != archive_id
            || statement.base_exclusive != base_exclusive
            || &statement.status != status
        {
            return Err(unknown());
        }
        Ok(Self::from_local(statement, context.owner()))
    }
    pub(crate) fn from_authenticated(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: ArchiveClosureStatement,
        expected: &ArchiveClosureStatement,
    ) -> Result<Self, ProtocolError> {
        context.timestamp(18, &statement)?;
        if &statement != expected || context.group() != &statement.timestamps {
            return Err(unknown());
        }
        Ok(Self::from_local(statement, context.owner()))
    }
}
#[derive(Serialize, Deserialize)]
struct AllocationPage {
    items: Page<CommitAllocation>,
    after: Option<TxnId>,
    exhausted: bool,
}
impl TimestampHandle {
    pub(crate) async fn archive_lookup(
        &self,
        expected: &ArchiveClosureStatement,
    ) -> Result<VerifiedArchiveClosure, ProtocolError> {
        if expected.timestamps != *self.identity() {
            return Err(unknown());
        }
        self.consensus.ensure_linearizable().await?;
        let key = closure_key(expected.archive_id, expected.status.group_id);
        let raw = self
            .machine
            .store()
            .read_metadata(PAGE_BUDGET, move |view| {
                let record = view.pinned(&key)?.ok_or_else(unknown)?;
                let statement: ArchiveClosureStatement =
                    serde_json::from_slice(&record).map_err(|_| unknown())?;
                view.encode(&statement)
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch(ByteClass::Data, 16 << 10)?,
        );
        let statement: ArchiveClosureStatement =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        if &statement != expected {
            return Err(unknown());
        }
        Ok(VerifiedArchiveClosure::from_local(statement, owner))
    }
    pub(crate) async fn archive_closure(
        &self,
        authority: Authority,
        owner: BackupOwnerRef,
        pin: StoragePin,
        archive_id: uuid::Uuid,
        base_exclusive: Option<CommitTs>,
        status: Arc<dyn StatusService>,
    ) -> Result<VerifiedArchiveClosure, ProtocolError> {
        if archive_id.is_nil()
            || base_exclusive.is_some_and(|base| base > pin.read_ts)
            || status.identity().cluster_id != self.identity().cluster_id
            || status.identity().database != self.identity().database
            || !matches!(status.identity().kind, GroupKind::Status { .. })
        {
            return Err(unknown());
        }
        self.consensus.ensure_linearizable().await?;
        let group = status.identity().clone();
        let key = closure_key(archive_id, group.group_id);
        let scope = self.identity().clone();
        let check_group = group.clone();
        let raw = self
            .machine
            .store()
            .read_metadata(PAGE_BUDGET, move |view| {
                if let Some(raw) = view.pinned(&key)? {
                    let previous: ArchiveClosureStatement =
                        serde_json::from_slice(&raw).map_err(|_| unknown())?;
                    if previous.timestamps != scope
                        || previous.status != check_group
                        || previous.owner != owner
                        || previous.pin != pin
                        || previous.authority.owner_term > authority.owner_term
                        || previous.base_exclusive != base_exclusive
                    {
                        return Err(unknown());
                    }
                    return view.encode(&Some(previous));
                }
                let state: State =
                    serde_json::from_slice(&view.pinned(STATE)?.ok_or_else(unknown)?)
                        .map_err(|_| unknown())?;
                let backup: BackupRecord =
                    serde_json::from_slice(&view.pinned(&backup_key(owner))?.ok_or_else(unknown)?)
                        .map_err(|_| unknown())?;
                if state.authority != authority
                    || backup.pin != pin
                    || backup.published.is_some()
                    || state.high_watermark < pin.read_ts.get()
                    || state.gc_safe_point > pin.read_ts.get()
                {
                    return Err(unknown());
                }
                view.encode(&None::<ArchiveClosureStatement>)
            })
            .await?;
        if let Some(statement) =
            serde_json::from_slice::<Option<ArchiveClosureStatement>>(raw.as_slice())
                .map_err(|_| unknown())?
        {
            return Ok(VerifiedArchiveClosure::from_local(
                statement,
                Arc::new(
                    self.resources()
                        .reserve_scratch(ByteClass::Data, 16 << 10)?,
                ),
            ));
        }
        let mut root = serialized_digest(&(
            b"nabedra/archive-allocation-seed/v1".as_slice(),
            self.identity(),
            &group,
            owner,
            pin,
            archive_id,
        ))?;
        let mut count = 0u64;
        let mut committed_count = 0u64;
        let mut decision_root = serialized_digest(&b"nabedra/archive-decisions/v1".as_slice())?;
        let mut after = None;
        loop {
            let prefix = [COMMIT_PREFIX, group.group_id.as_bytes()].concat();
            let page_prefix = prefix.clone();
            let raw = self
                .machine
                .store()
                .read_archive(PAGE_BUDGET, move |view, _| {
                    let mut cursor = after
                        .map(|txn| commit_key(group.group_id, txn).to_vec())
                        .unwrap_or_else(|| page_prefix.clone());
                    if after.is_some() {
                        cursor.push(0);
                    }
                    let mut items = Vec::with_capacity(64);
                    let mut next = after;
                    for _ in 0..64 {
                        let Some(row) = view.first_from(&page_prefix, &cursor)? else {
                            break;
                        };
                        if row.key().len() != COMMIT_PREFIX.len() + 40
                            || row.value().len() > 16 << 10
                        {
                            return Err(unknown());
                        }
                        let allocation: CommitAllocation =
                            serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                        let binding = allocation.permit.statement();
                        if binding.manifest.status_group_id != group.group_id
                            || row.key() != commit_key(group.group_id, binding.txn)
                            || allocation.stamp.permit_digest
                                != allocation.permit.allocation_digest()?
                        {
                            return Err(unknown());
                        }
                        next = Some(binding.txn);
                        cursor = row.key().to_vec();
                        cursor.push(0);
                        if allocation.stamp.commit_ts <= pin.read_ts {
                            items.push(allocation);
                        }
                    }
                    let exhausted = view.first_from(&page_prefix, &cursor)?.is_none();
                    view.encode(&AllocationPage {
                        items: Page::new(items).map_err(|_| unknown())?,
                        after: next,
                        exhausted,
                    })
                })
                .await?;
            let decode = Arc::new(
                self.resources()
                    .reserve_scratch_wait(
                        ByteClass::Data,
                        crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
                        0,
                    )
                    .await?,
            );
            let page: AllocationPage =
                serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
            for allocation in page.items.iter() {
                let binding = allocation.permit.statement();
                let decision = status
                    .lookup_decision(binding.txn, binding.manifest)
                    .await?
                    .ok_or_else(unsettled)?;
                let statement = decision.statement();
                if statement.request != binding.request
                    || matches!(statement.decision, TxnDecision::Commit { commit_ts } if commit_ts != allocation.stamp.commit_ts)
                {
                    return Err(unknown());
                }
                let settled = status
                    .settled_proof(binding.txn, binding.manifest)
                    .await?
                    .into_sealed();
                let evidence =
                    settled.validate_for(binding.txn, &binding.request, &binding.manifest)?;
                if evidence.decision != *statement {
                    return Err(unknown());
                }
                if matches!(statement.decision, TxnDecision::Commit { commit_ts } if base_exclusive.is_none_or(|base| commit_ts > base))
                {
                    decision_root = serialized_digest(&(
                        b"nabedra/archive-decision/v1".as_slice(),
                        decision_root,
                        statement,
                    ))?;
                    committed_count = committed_count.checked_add(1).ok_or_else(unknown)?;
                }
                root = serialized_digest(&(
                    b"nabedra/archive-allocation-step/v1".as_slice(),
                    root,
                    &allocation.permit,
                    allocation.stamp,
                    evidence,
                ))?;
                count = count.checked_add(1).ok_or_else(unknown)?;
            }
            drop(decode);
            if page.exhausted {
                break;
            }
            if page.after == after {
                return Err(unknown());
            }
            after = page.after;
        }
        let cut = status.archive_cut(authority).await?;
        let statement = ArchiveClosureStatement {
            timestamps: self.identity().clone(),
            status: status.identity().clone(),
            authority,
            owner,
            pin,
            archive_id,
            base_exclusive,
            status_applied_at: cut.statement().applied,
            identity_count: cut.statement().identity_count,
            identity_root: cut.statement().identity_root,
            committed_count,
            decision_root,
            allocation_count: count,
            allocation_root: root,
        };
        let proof = SealedArchiveClosure::verified(statement.clone())?;
        match self
            .submit(TimestampCommand::SealArchive { proof })
            .await?
            .reply
        {
            TimestampReply::Archive(actual) if actual == statement => {
                Ok(VerifiedArchiveClosure::from_local(actual, cut.owner()))
            }
            _ => Err(unknown()),
        }
    }
}
fn unsettled() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "archive cut waits for an immutable fully settled pre-cut allocation",
        Outcome::Unknown,
    )
}
fn closure_key(archive_id: uuid::Uuid, status: uuid::Uuid) -> Vec<u8> {
    [CLOSURES, archive_id.as_bytes(), status.as_bytes()].concat()
}
pub(super) fn apply_closure(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: Option<State>,
    proof: &SealedArchiveClosure,
) -> Result<Transition, ProtocolError> {
    let statement = proof.validate()?;
    let Some(state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if statement.timestamps != *scope
        || statement.authority != state.authority
        || statement.archive_id.is_nil()
        || !matches!(statement.status.kind, GroupKind::Status { .. })
        || statement.status.cluster_id != scope.cluster_id
        || statement.status.database != scope.database
        || statement
            .base_exclusive
            .is_some_and(|base| base > statement.pin.read_ts)
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = closure_key(statement.archive_id, statement.status.group_id);
    if let Some(old) = load::<ArchiveClosureStatement>(tx, &key)? {
        return if old == *statement {
            commit(tx, &TimestampReply::Archive(old))
        } else {
            reject(tx, Fault::TransitionConflict)
        };
    }
    let Some(backup) = load::<BackupRecord>(tx, &backup_key(statement.owner))? else {
        return reject(tx, Fault::UnknownPin);
    };
    if backup.pin != statement.pin
        || backup.owner != statement.owner
        || backup.authority != statement.authority
        || backup.published.is_some()
        || state.high_watermark < statement.pin.read_ts.get()
        || state.gc_safe_point > statement.pin.read_ts.get()
    {
        return reject(tx, Fault::PinExpired);
    }
    put(tx, &key, statement)?;
    commit(tx, &TimestampReply::Archive(statement.clone()))
}

const RESTORE_WATERMARK: &[u8] = b"ts/restored-history/genesis-watermark/v1";
/// Imported timestamp history never enters COMMIT_PREFIX and cannot produce a
/// live allocation proof. Only the first target Fence consumes this watermark.
pub(super) fn restored_watermark(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
) -> Result<u64, ProtocolError> {
    let Some(seed) = load::<(GroupIdentity, u64)>(tx, RESTORE_WATERMARK)? else {
        return Ok(0);
    };
    if &seed.0 != scope || seed.1 == u64::MAX {
        return Err(unknown());
    }
    seed.1.checked_add(1).ok_or_else(unknown)
}
