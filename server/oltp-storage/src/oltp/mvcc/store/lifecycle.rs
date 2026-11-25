//! Catalog lifecycle guards remain durable until Status proves every declared
//! participant has settled. Exclusive-pending blocks new shared holders before
//! a drop waits; the wait itself occurs outside native execution.
use super::*;
use crate::oltp::mvcc::index::{IndexAttemptClosedRecord, IndexClosedKind, closure_id};
use crate::oltp::mvcc::store::consensus::Applied;
use nabedra_oltp_types::types::MaintenanceOwnerRef;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardMode {
    Shared,
    Exclusive,
    MetadataCleanup,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleBinding {
    pub guard_id: Uuid,
    pub catalog_group_id: Uuid,
    pub table_id: Uuid,
    pub lifecycle_version: Option<CommitTs>,
    pub schema_epoch: u64,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub read_ts: ReadTs,
    pub authority: Authority,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedLifecycleGuard {
    binding: LifecycleBinding,
    mode: GuardMode,
    acquired_at: AppliedPosition,
    proof_digest: Digest,
}
impl SealedLifecycleGuard {
    pub fn binding(&self) -> &LifecycleBinding {
        &self.binding
    }
    pub fn mode(&self) -> GuardMode {
        self.mode
    }
    fn new(
        binding: LifecycleBinding,
        mode: GuardMode,
        acquired_at: AppliedPosition,
    ) -> Result<Self, ProtocolError> {
        let proof_digest = hash(
            b"nabedra/catalog-lifecycle-guard/v1\0",
            &(&binding, mode, acquired_at),
        )?;
        Ok(Self {
            binding,
            mode,
            acquired_at,
            proof_digest,
        })
    }
    pub(super) fn valid(&self) -> Result<bool, ProtocolError> {
        Ok(self.binding.guard_id != Uuid::nil()
            && self.proof_digest
                == hash(
                    b"nabedra/catalog-lifecycle-guard/v1\0",
                    &(&self.binding, self.mode, self.acquired_at),
                )?)
    }
}
/// A raw decoded shape cannot create this service capability.
#[derive(Clone, Debug)]
pub struct VerifiedLifecycleGuard {
    guard: SealedLifecycleGuard,
}
impl VerifiedLifecycleGuard {
    pub fn from_authenticated(
        context: impl super::ParticipantReplyProof,
        guard: SealedLifecycleGuard,
        binding: &LifecycleBinding,
    ) -> Result<Self, ProtocolError> {
        if context.group().group_id != binding.catalog_group_id
            || guard.binding() != binding
            || !guard.valid()?
        {
            return Err(invalid());
        }
        context.verify_payload(
            nabedra_oltp_types::authority::ParticipantProofKind::Lifecycle,
            &guard,
        )?;
        Ok(Self { guard })
    }

    pub fn binding(&self) -> &LifecycleBinding {
        self.guard.binding()
    }
    pub fn mode(&self) -> GuardMode {
        self.guard.mode()
    }

    pub fn into_sealed(self) -> SealedLifecycleGuard {
        self.guard
    }
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn model_for_test(binding: LifecycleBinding, mode: GuardMode) -> Self {
        Self {
            guard: SealedLifecycleGuard::new(
                binding,
                mode,
                AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                },
            )
            .unwrap(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LifecycleCommand {
    CancelPending {
        authority: Authority,
        binding: LifecycleBinding,
        settled: nabedra_oltp_types::authority::SealedSettlement,
    },
    Release {
        authority: Authority,
        guard: SealedLifecycleGuard,
        settled: nabedra_oltp_types::authority::SealedSettlement,
    },
    RecoverIndexPublication {
        authority: Authority,
        owner: MaintenanceOwnerRef,
        binding: LifecycleBinding,
        settled: nabedra_oltp_types::authority::SealedSettlement,
    },
    Acquire {
        authority: Authority,
        binding: LifecycleBinding,
        mode: GuardMode,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LifecycleReply {
    Granted,
    Released,
    Pending { holder: TxnId },
    Rejected(Fault),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GuardRecord {
    binding: LifecycleBinding,
    mode: GuardMode,
    granted_at: Option<AppliedPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scoped_queue: Option<scoped_queue::QueueBinding>,
}
fn holder_key(binding: &LifecycleBinding) -> [u8; 41] {
    table_holder_key(binding.table_id, binding.txn)
}
fn table_holder_key(table_id: Uuid, txn: TxnId) -> [u8; 41] {
    let mut key = [0; 41];
    key[0] = b'G';
    key[1..17].copy_from_slice(table_id.as_bytes());
    key[17..].copy_from_slice(&txn_key(b'_', txn)[1..]);
    key
}
fn exclusive_key(table: Uuid) -> [u8; 17] {
    let mut key = [0; 17];
    key[0] = b'X';
    key[1..].copy_from_slice(table.as_bytes());
    key
}
fn holder_prefix(table: Uuid) -> [u8; 17] {
    let mut key = exclusive_key(table);
    key[0] = b'G';
    key
}
fn retired_key(binding: &LifecycleBinding) -> [u8; 41] {
    let mut key = holder_key(binding);
    key[0] = b'R';
    key
}
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    position: AppliedPosition,
    command: &LifecycleCommand,
    scoped_queue: Option<scoped_queue::QueueBinding>,
) -> Result<Transition, ProtocolError> {
    if let LifecycleCommand::RecoverIndexPublication {
        authority,
        owner,
        binding,
        settled,
    } = command
    {
        return recover_index_publication(tx, group, config, *authority, owner, binding, settled);
    }
    if let LifecycleCommand::CancelPending {
        authority,
        binding,
        settled,
    } = command
    {
        if !matches!(group.kind, GroupKind::Catalog)
            || *authority != config.authority
            || binding.catalog_group_id != group.group_id
        {
            return reject(tx, Fault::StaleAuthority);
        }
        let statement =
            settled.validate_for(binding.txn, &binding.request, &settled.statement().manifest)?;
        if !matches!(statement.decision.decision, TxnDecision::Abort { .. }) {
            return reject(tx, Fault::InvalidBinding);
        }
        let key = exclusive_key(binding.table_id);
        if let Some(record) = load::<GuardRecord>(tx, &key)? {
            if record.binding != *binding || record.granted_at.is_some() {
                return reject(tx, Fault::InvalidBinding);
            }
            tx.delete(&key)?;
        }
        tx.put_serialized(&retired_key(binding), binding)?;
        return reply(tx, LifecycleReply::Released);
    }
    if let LifecycleCommand::Release {
        authority,
        guard,
        settled,
    } = command
    {
        if !matches!(group.kind, GroupKind::Catalog)
            || *authority != config.authority
            || guard.binding.catalog_group_id != group.group_id
            || !guard.valid()?
        {
            return reject(tx, Fault::StaleAuthority);
        }
        let binding = &guard.binding;
        let statement =
            settled.validate_for(binding.txn, &binding.request, &settled.statement().manifest)?;
        if statement.resolved_count != statement.manifest.participant_count {
            return reject(tx, Fault::Incomplete);
        }
        let key = match guard.mode {
            GuardMode::Shared => holder_key(binding).to_vec(),
            GuardMode::Exclusive | GuardMode::MetadataCleanup => {
                exclusive_key(binding.table_id).to_vec()
            }
        };
        if let Some(record) = load::<GuardRecord>(tx, &key)? {
            if record.binding != *binding
                || record.mode != guard.mode
                || record.granted_at != Some(guard.acquired_at)
                || (binding.request.run_id == authority.run_id
                    && record.scoped_queue != scoped_queue)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            tx.delete(&key)?;
        }
        tx.put_serialized(&retired_key(binding), binding)?;
        return reply(tx, LifecycleReply::Released);
    }
    let LifecycleCommand::Acquire {
        authority,
        binding,
        mode,
    } = command
    else {
        unreachable!()
    };
    if !matches!(group.kind, GroupKind::Catalog)
        || *authority != config.authority
        || binding.authority != *authority
    {
        return reject(tx, Fault::StaleAuthority);
    }
    if binding.catalog_group_id != group.group_id
        || binding.guard_id.is_nil()
        || binding.table_id.is_nil()
        || binding.schema_epoch == 0
        || binding.txn.request_id != binding.request.request_id
        || binding.request.run_id != authority.run_id
        || binding
            .lifecycle_version
            .is_some_and(|ts| ts > binding.read_ts)
    {
        return reject(tx, Fault::InvalidBinding);
    }
    if tx.pinned(&retired_key(binding))?.is_some() {
        return reject(tx, Fault::RolledBack);
    }
    let key = match mode {
        GuardMode::Shared => holder_key(binding).to_vec(),
        GuardMode::Exclusive | GuardMode::MetadataCleanup => {
            exclusive_key(binding.table_id).to_vec()
        }
    };
    let old = load::<GuardRecord>(tx, &key)?;
    if let Some(old) = &old {
        let mut adopted = old.binding.clone();
        let same_run = old.binding.authority.run_id == binding.authority.run_id
            && old.binding.authority.owner_term <= binding.authority.owner_term;
        adopted.authority = binding.authority;
        if !same_run
            || adopted != *binding
            || old.mode != *mode
            || old.scoped_queue != scoped_queue
        {
            return reject(tx, Fault::Conflict);
        }
        if old.granted_at.is_some() {
            if old.binding != *binding {
                tx.put_serialized(
                    &key,
                    &GuardRecord {
                        binding: binding.clone(),
                        mode: *mode,
                        granted_at: Some(position),
                        scoped_queue,
                    },
                )?;
            }
            return reply(tx, LifecycleReply::Granted);
        }
    }
    if matches!(mode, GuardMode::Shared) && tx.pinned(&exclusive_key(binding.table_id))?.is_some() {
        return reject(tx, Fault::Conflict);
    }
    // Validate schema identity against the latest catalog table version while
    // holding the same state order as its MVCC prepare and resolution.
    let table_key = MvccKey::CatalogTable {
        table_id: binding.table_id,
    };
    let length = key_length(&table_key)?;
    let valid = tx.with_scratch_buffer(length + 9, |tx, buffer| {
        encode_key(&table_key, &mut buffer[1..1 + length]);
        let end = length + 1;
        buffer[0] = HEAD;
        let head = load::<Head>(tx, &buffer[..end])?.unwrap_or_default();
        if head.intent.is_some() || head.version != binding.lifecycle_version {
            return Ok(false);
        }
        if let Some(version) = head.version {
            buffer[0] = VALUE;
            buffer[end..].copy_from_slice(&(!version.get()).to_be_bytes());
            let row = tx.pinned(buffer)?.ok_or_else(recovery_unknown)?;
            #[derive(Deserialize)]
            struct Version<'a> {
                #[serde(borrow)]
                value: Option<&'a RawValue>,
            }
            let row: Version<'_> = serde_json::from_slice(&row).map_err(|_| recovery_unknown())?;
            let Some(value) = row.value else {
                return Ok(matches!(mode, GuardMode::MetadataCleanup));
            };
            if matches!(mode, GuardMode::MetadataCleanup) {
                return Ok(false);
            }
            #[derive(Deserialize)]
            struct Schema {
                schema_epoch: u64,
            }
            let schema: Schema =
                serde_json::from_str(value.get()).map_err(|_| recovery_unknown())?;
            Ok(schema.schema_epoch == binding.schema_epoch)
        } else {
            Ok(matches!(mode, GuardMode::Exclusive))
        }
    })?;
    if !valid {
        return reject(tx, Fault::Conflict);
    }
    let mut record = old.unwrap_or(GuardRecord {
        binding: binding.clone(),
        mode: *mode,
        granted_at: None,
        scoped_queue,
    });
    record.binding = binding.clone();
    record.scoped_queue = scoped_queue;
    if matches!(mode, GuardMode::Exclusive | GuardMode::MetadataCleanup) {
        let mut blocked = None;
        tx.scan_page(&holder_prefix(binding.table_id), None, 1, |_, raw| {
            let holder: GuardRecord =
                serde_json::from_slice(raw).map_err(|_| recovery_unknown())?;
            blocked = Some(holder.binding.txn);
            Ok(())
        })?;
        if let Some(holder) = blocked {
            tx.put_serialized(&key, &record)?;
            return reply(tx, LifecycleReply::Pending { holder });
        }
    }
    record.granted_at = Some(position);
    tx.put_serialized(&key, &record)?;
    reply(tx, LifecycleReply::Granted)
}
fn recover_index_publication(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    authority: Authority,
    owner: &MaintenanceOwnerRef,
    binding: &LifecycleBinding,
    settled: &nabedra_oltp_types::authority::SealedSettlement,
) -> Result<Transition, ProtocolError> {
    if group.kind != GroupKind::Catalog
        || config.authority != authority
        || binding.authority.run_id != authority.run_id
        || binding.authority.owner_term >= authority.owner_term
    {
        return reject(tx, Fault::StaleAuthority);
    }
    if binding.catalog_group_id != group.group_id
        || owner.catalog_group_id != group.group_id
        || owner.table_id != binding.table_id
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let Ok(statement) =
        settled.validate_for(binding.txn, &binding.request, &settled.statement().manifest)
    else {
        return reject(tx, Fault::InvalidBinding);
    };
    let TxnDecision::Commit { commit_ts } = statement.decision.decision else {
        return reject(tx, Fault::InvalidBinding);
    };
    let Some(resolution) = load::<ResolutionEvidence>(tx, &txn_key(b'd', binding.txn))? else {
        return reject(tx, Fault::Incomplete);
    };
    if statement.resolved_count != 1
        || resolution.group != *group
        || resolution.txn != binding.txn
        || resolution.request != binding.request
        || resolution.manifest != statement.manifest
        || resolution.decision != statement.decision.decision
        || commit_ts <= binding.read_ts
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = MvccKey::CatalogIndexClosed {
        table_id: owner.table_id,
        closure_id: closure_id(owner)?,
    };
    let length = key_length(&key)?;
    let closed = tx.with_scratch_buffer(length + 9, |tx, buffer| {
        let end = length + 1;
        buffer[0] = VALUE;
        encode_key(&key, &mut buffer[1..end]);
        buffer[end..].copy_from_slice(&(!commit_ts.get()).to_be_bytes());
        #[derive(Deserialize)]
        struct Version {
            value: Option<IndexAttemptClosedRecord>,
        }
        Ok(load::<Version>(tx, buffer)?.and_then(|row| row.value))
    })?;
    if !closed.is_some_and(|row| {
        row.owner == *owner && row.closed_by == binding.txn && row.kind == IndexClosedKind::Ready
    }) {
        return reject(tx, Fault::InvalidBinding);
    }
    let retired = retired_key(binding);
    if let Some(previous) = load::<LifecycleBinding>(tx, &retired)? {
        return if previous == *binding {
            reply(tx, LifecycleReply::Released)
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    let key = exclusive_key(binding.table_id);
    if !load::<GuardRecord>(tx, &key)?.is_some_and(|record| {
        record.binding == *binding
            && record.mode == GuardMode::Exclusive
            && record.granted_at.is_some()
    }) {
        return reject(tx, Fault::InvalidBinding);
    }
    tx.delete(&key)?;
    tx.put_serialized(&retired, binding)?;
    reply(tx, LifecycleReply::Released)
}
fn reply(
    tx: &mut GroupTransaction<'_>,
    result: LifecycleReply,
) -> Result<Transition, ProtocolError> {
    commit(tx, &ParticipantReply::Lifecycle(result))
}
pub(super) fn validate_guards(
    config: &Config,
    group: &GroupIdentity,
    txn: TxnId,
    request: &RequestBinding,
    plan: &ParticipantPlan,
    writes: &[PreparedWrite],
    guards: &[SealedLifecycleGuard],
) -> Result<bool, ProtocolError> {
    for guard in guards {
        let b = guard.binding();
        if !guard.valid()?
            || b.catalog_group_id != config.catalog_group_id
            || b.txn != txn
            || &b.request != request
            || b.read_ts != plan.read_ts
            || b.authority != config.authority
        {
            return Ok(false);
        }
    }
    if guards
        .iter()
        .any(|guard| guard.mode == GuardMode::MetadataCleanup)
    {
        if !matches!(group.kind, GroupKind::Catalog)
            || guards
                .iter()
                .any(|guard| guard.mode != GuardMode::MetadataCleanup)
        {
            return Ok(false);
        }
        for write in writes {
            let table = match (&write.key, &write.change) {
                (MvccKey::CatalogTable { table_id }, Change::Keep) => {
                    if !guards.iter().any(|g| {
                        g.binding.table_id == *table_id
                            && g.binding.lifecycle_version == write.expected_version
                            && write.expected_version.is_some()
                    }) {
                        return Ok(false);
                    }
                    *table_id
                }
                (MvccKey::CatalogIndex { table_id, index_id }, Change::Put(raw)) => {
                    let Ok(record) =
                        serde_json::from_str::<crate::oltp::mvcc::index::IndexRecord>(raw.as_str())
                    else {
                        return Ok(false);
                    };
                    if record.definition.table_id != *table_id
                        || record.definition.index_id != *index_id
                        || !matches!(
                            record.phase,
                            crate::oltp::mvcc::index::IndexPhase::Retired { .. }
                                | crate::oltp::mvcc::index::IndexPhase::Failed { .. }
                        )
                    {
                        return Ok(false);
                    }
                    *table_id
                }
                (MvccKey::CatalogIndexClosed { table_id, .. }, Change::Put(raw)) => {
                    let Ok(record) = serde_json::from_str::<
                        crate::oltp::mvcc::index::IndexAttemptClosedRecord,
                    >(raw.as_str()) else {
                        return Ok(false);
                    };
                    if record.owner.table_id != *table_id
                        || record.closed_by != txn
                        || !matches!(
                            record.kind,
                            crate::oltp::mvcc::index::IndexClosedKind::Retired
                                | crate::oltp::mvcc::index::IndexClosedKind::Failed
                        )
                    {
                        return Ok(false);
                    }
                    *table_id
                }
                _ => return Ok(false),
            };
            if !guards.iter().any(|g| {
                g.binding.table_id == table
                    && g.binding.schema_epoch == plan.schema_epoch
                    && g.binding.lifecycle_version.is_some()
            }) {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    for write in writes {
        let (table, exclusive) = match &write.key {
            // Dedup records are written only by direct commits.
            MvccKey::DirectDedup { .. } => return Ok(false),
            MvccKey::Row { table_id, .. } | MvccKey::IndexEntry { table_id, .. } => {
                (*table_id, false)
            }
            MvccKey::CatalogIndexProgress { table_id, .. } => (*table_id, false),
            MvccKey::CatalogTable { table_id } => {
                (*table_id, !matches!(write.change, Change::Keep))
            }
            MvccKey::CatalogIndex { table_id, .. }
            | MvccKey::CatalogIndexName { table_id, .. }
            | MvccKey::CatalogRoutingRoot { table_id }
            | MvccKey::CatalogRoutingPage { table_id, .. }
            | MvccKey::CatalogIndexesRoot { table_id }
            | MvccKey::CatalogIndexesPage { table_id, .. }
            | MvccKey::CatalogIndexClosed { table_id, .. } => {
                (*table_id, !matches!(write.change, Change::Keep))
            }
            MvccKey::CatalogSplitClosed { table_id, .. } => {
                let Change::Put(value) = &write.change else {
                    return Ok(false);
                };
                let Some(closure) =
                    super::split_closure::split_closure_row(&write.key, Some(value))?
                else {
                    return Ok(false);
                };
                if write.expected_version.is_some()
                    || closure.restored.is_some()
                    || closure.guard.binding().txn != txn
                    || &closure.guard.binding().request != request
                    || closure.guard.binding().catalog_group_id != group.group_id
                    || closure.source.cluster_id != group.cluster_id
                    || closure.source.database != group.database
                    || !guards.contains(&closure.guard)
                {
                    return Ok(false);
                }
                (*table_id, true)
            }
            MvccKey::CatalogName { .. } => {
                if !guards
                    .iter()
                    .any(|g| matches!(g.mode, GuardMode::Exclusive))
                {
                    return Ok(false);
                }
                continue;
            }
        };
        if !guards.iter().any(|g| {
            g.binding.table_id == table
                && g.binding.schema_epoch == plan.schema_epoch
                && (!exclusive || matches!(g.mode, GuardMode::Exclusive))
        }) {
            return Ok(false);
        }
    }
    // A Data group must never accidentally accept catalog guards for another
    // table, even if a coordinator supplied an otherwise valid capability.
    if let GroupKind::Data { table_id, .. } = group.kind {
        if guards.iter().any(|g| g.binding.table_id != table_id) {
            return Ok(false);
        }
    }
    Ok(true)
}
impl ParticipantHandle {
    pub async fn recover_index_publication(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        binding: LifecycleBinding,
        settled: impl nabedra_oltp_types::authority::SettlementProof,
    ) -> Result<(), ProtocolError> {
        let applied = self
            .propose(OwnedCommand::new_owned(
                ParticipantCommand::Lifecycle(LifecycleCommand::RecoverIndexPublication {
                    authority,
                    owner,
                    binding,
                    settled: settled.into_sealed(),
                }),
                self.resources(),
                crate::oltp::pool::ByteClass::Progress,
            )?)
            .await?;
        if !matches!(
            applied.reply,
            ParticipantReply::Lifecycle(LifecycleReply::Released)
        ) {
            return Err(recovery_unknown());
        }
        Ok(())
    }
    pub async fn release_lifecycle(
        &self,
        authority: Authority,
        guard: VerifiedLifecycleGuard,
        settled: impl nabedra_oltp_types::authority::SettlementProof,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Lifecycle(LifecycleCommand::Release {
                authority,
                guard: guard.into_sealed(),
                settled: settled.into_sealed(),
            }),
            self.resources(),
            crate::oltp::pool::ByteClass::Progress,
        )?)
        .await
    }
    pub async fn cancel_pending_lifecycle(
        &self,
        authority: Authority,
        binding: LifecycleBinding,
        settled: impl nabedra_oltp_types::authority::SettlementProof,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Lifecycle(LifecycleCommand::CancelPending {
                authority,
                binding,
                settled: settled.into_sealed(),
            }),
            self.resources(),
            crate::oltp::pool::ByteClass::Progress,
        )?)
        .await
    }
    pub async fn scoped_lifecycle_guard(
        &self,
        scope: nabedra_oltp_types::authority::ScopedCatalogRead,
        txn: TxnId,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        if self.identity().kind != GroupKind::Catalog || txn.request_id != scope.request.request_id || txn.attempt != 1 {
            return Err(invalid());
        }
        if let Some(remote) = &self.remote {
            return remote.scoped_lifecycle_guard(txn).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        let group_id = self.identity().group_id;
        let raw = self.local_machine().store.read_metadata(
            TransactionBudget { scratch_bytes: 64 << 10, batch_bytes: 0, output_bytes: 16 << 10 },
            move |view| {
                let Some(bytes) = view.pinned(&table_holder_key(scope.table_id, txn))? else {
                    return view.encode(&Option::<SealedLifecycleGuard>::None);
                };
                if bytes.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let record: GuardRecord = serde_json::from_slice(&bytes).map_err(|_| recovery_unknown())?;
                let binding = record.binding;
                if record.mode != GuardMode::Shared
                    || binding.catalog_group_id != group_id || binding.table_id != scope.table_id
                    || binding.txn != txn || binding.request != scope.request || binding.read_ts != scope.read_ts
                    || record.scoped_queue != Some(scoped_queue::QueueBinding::from_scope(scope))
                {
                    return Err(invalid());
                }
                let guard = SealedLifecycleGuard::new(binding, record.mode, record.granted_at.ok_or_else(recovery_unknown)?)?;
                view.encode(&Some(guard))
            },
        ).await?;
        let guard: Option<SealedLifecycleGuard> = serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(guard.map(|guard| VerifiedLifecycleGuard { guard }))
    }

    /// One durable blocking holder at a time. Drop helpers consult its current
    /// Status outside native capacity, settle declared participants, then retry.
    pub async fn lifecycle_holder(
        &self,
        table_id: Uuid,
    ) -> Result<Option<LifecycleBinding>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.lifecycle_holder(table_id).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    let mut holder = None;
                    view.scan_page(&holder_prefix(table_id), None, 1, |_, bytes| {
                        if bytes.len() > METADATA_BYTES {
                            return Err(recovery_unknown());
                        }
                        let record: GuardRecord =
                            serde_json::from_slice(bytes).map_err(|_| recovery_unknown())?;
                        holder = Some(record.binding);
                        Ok(())
                    })?;
                    view.encode(&holder)
                },
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    /// The table's exclusive lifecycle guard, granted or pending, read
    /// linearizably. A direct range leader binds a table lifecycle to its
    /// group only while none exists (T7b).
    pub async fn lifecycle_exclusive(
        &self,
        table_id: Uuid,
    ) -> Result<Option<LifecycleBinding>, ProtocolError> {
        if !matches!(self.identity().kind, GroupKind::Catalog) {
            return Err(invalid());
        }
        if let Some(remote) = &self.remote {
            return remote.lifecycle_exclusive(table_id).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    let holder = match view.pinned(&exclusive_key(table_id))? {
                        None => None,
                        Some(bytes) => {
                            if bytes.len() > METADATA_BYTES {
                                return Err(recovery_unknown());
                            }
                            let record: GuardRecord =
                                serde_json::from_slice(&bytes).map_err(|_| recovery_unknown())?;
                            Some(record.binding)
                        }
                    };
                    view.encode(&holder)
                },
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    /// Read an already granted holder without adopting its authority. Recovery
    /// may settle an earlier run, but only Status settlement can release it.
    pub async fn lifecycle_guard(
        &self,
        binding: &LifecycleBinding,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        if binding.catalog_group_id != self.identity().group_id
            || !matches!(self.identity().kind, GroupKind::Catalog)
        {
            return Err(invalid());
        }
        if let Some(remote) = &self.remote {
            return remote.lifecycle_guard(binding.clone()).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        let binding = binding.clone();
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    let mut guard = None;
                    for key in [
                        holder_key(&binding).to_vec(),
                        exclusive_key(binding.table_id).to_vec(),
                    ] {
                        if let Some(bytes) = view.pinned(&key)? {
                            if bytes.len() > METADATA_BYTES {
                                return Err(recovery_unknown());
                            }
                            let record: GuardRecord =
                                serde_json::from_slice(&bytes).map_err(|_| recovery_unknown())?;
                            if record.binding == binding {
                                if let Some(position) = record.granted_at {
                                    guard = Some(SealedLifecycleGuard::new(
                                        record.binding,
                                        record.mode,
                                        position,
                                    )?);
                                    break;
                                }
                            }
                        }
                    }
                    view.encode(&guard)
                },
            )
            .await?;
        let guard: Option<SealedLifecycleGuard> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(guard.map(|guard| VerifiedLifecycleGuard { guard }))
    }
    pub async fn acquire_lifecycle(
        &self,
        binding: LifecycleBinding,
        mode: GuardMode,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.acquire_lifecycle(binding, mode).await;
        }
        self.acquire_verified_lifecycle(binding, mode, None).await
    }

    pub async fn acquire_verified_lifecycle(
        &self,
        binding: LifecycleBinding,
        mode: GuardMode,
        scope: Option<nabedra_oltp_types::authority::ScopedCatalogRead>,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        let command = ParticipantCommand::Lifecycle(LifecycleCommand::Acquire {
            authority: binding.authority,
            binding: binding.clone(),
            mode,
        });
        let command = OwnedCommand::new_owned(
            command,
            self.resources(),
            crate::oltp::pool::ByteClass::Progress,
        )?;
        let applied = match scope {
            Some(scope) => self.propose_verified_scoped(scope, command).await?,
            None => self.propose(command).await?,
        };
        nabedra_rt::diag(&format!("LIFECYCLE {:?}", applied.reply));
        match applied.reply {
            ParticipantReply::Lifecycle(LifecycleReply::Granted) => {}
            ParticipantReply::Lifecycle(LifecycleReply::Pending { .. }) => return Ok(None),
            ParticipantReply::Rejected(Fault::StaleAuthority) if scope.is_some() => {
                return Err(recovery_unknown());
            }
            ParticipantReply::Rejected(Fault::Conflict | Fault::RolledBack)
            | ParticipantReply::Lifecycle(LifecycleReply::Rejected(
                Fault::Conflict | Fault::RolledBack,
            )) => {
                return Err(failure(
                    ErrorCode::Conflict,
                    "captured schema or lifecycle changed",
                ));
            }
            _ => return Err(invalid()),
        }
        self.local_consensus().ensure_linearizable().await?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    let key = match mode {
                        GuardMode::Shared => holder_key(&binding).to_vec(),
                        GuardMode::Exclusive | GuardMode::MetadataCleanup => {
                            exclusive_key(binding.table_id).to_vec()
                        }
                    };
                    let raw = view.pinned(&key)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let record: GuardRecord =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if record.binding != binding || record.mode != mode {
                        return Err(invalid());
                    }
                    view.encode(&SealedLifecycleGuard::new(
                        record.binding,
                        record.mode,
                        record.granted_at.ok_or_else(invalid)?,
                    )?)
                },
            )
            .await?;
        let guard: SealedLifecycleGuard =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(Some(VerifiedLifecycleGuard { guard }))
    }
}

#[cfg(test)]
mod index_recovery_tests {
    use super::*;
    use crate::oltp::pool::{OltpPool, PoolIdentity, SharedResourceConfig, WorkerSharedResources};
    use nabedra_oltp_types::{
        authority::{SealedSettlement, SettlementStatement},
        types::{DecisionStatement, StoragePin, Text},
    };
    use nabedra_resources::config::WorkerResources;

    #[tokio::test]
    async fn o08_o12_t06_recover_exact_settled_ready_guard() {
        let path = std::env::temp_dir().join(format!("nabedra-ready-guard-{}", nabedra_rt::ids::uuid_v4()));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        let cluster_id = nabedra_rt::ids::uuid_v4();
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(
            &path,
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id,
                worker_id: nabedra_rt::ids::uuid_v4(),
                pool_id: nabedra_rt::ids::uuid_v4(),
            },
            shared,
        )
        .unwrap();
        let catalog = GroupIdentity {
            cluster_id,
            database: Text::try_from("test").unwrap(),
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Catalog,
            group_format_version: 1,
        };
        let store = pool.group(catalog.clone()).await.unwrap();
        let original = Authority {
            run_id: nabedra_rt::ids::uuid_v4(),
            owner_term: 1,
        };
        let successor = Authority {
            owner_term: 2,
            ..original
        };
        let owner = MaintenanceOwnerRef {
            catalog_group_id: catalog.group_id,
            table_id: nabedra_rt::ids::uuid_v4(),
            index_id: nabedra_rt::ids::uuid_v4(),
            build_id: nabedra_rt::ids::uuid_v4(),
            attempt: 1,
        };
        let txn = TxnId {
            request_id: nabedra_rt::ids::uuid_v4(),
            attempt: 1,
        };
        let binding = LifecycleBinding {
            guard_id: nabedra_rt::ids::uuid_v4(),
            catalog_group_id: catalog.group_id,
            table_id: owner.table_id,
            lifecycle_version: Some(CommitTs::new(10).unwrap()),
            schema_epoch: 1,
            txn,
            request: RequestBinding {
                run_id: original.run_id,
                request_id: txn.request_id,
                digest: Digest::from_bytes([1; 32]),
                accepted_context: Digest::from_bytes([2; 32]),
            },
            read_ts: ReadTs::new(11).unwrap(),
            authority: original,
        };
        let position = AppliedPosition {
            term: 1,
            node_id: 1,
            index: 20,
        };
        let manifest = ManifestRef {
            status_group_id: nabedra_rt::ids::uuid_v4(),
            digest: Digest::from_bytes([3; 32]),
            participant_count: 1,
        };
        let commit_ts = CommitTs::new(12).unwrap();
        let decision = TxnDecision::Commit { commit_ts };
        let settled = SealedSettlement::seal(SettlementStatement {
            txn,
            request: binding.request,
            manifest,
            decision: DecisionStatement {
                txn,
                request: binding.request,
                manifest,
                decided_at: position,
                decision,
            },
            resolved_count: 1,
            settled_at: position,
        })
        .unwrap();
        let closure = IndexAttemptClosedRecord {
            owner,
            pin: StoragePin {
                pin_id: nabedra_rt::ids::uuid_v4(),
                read_ts: ReadTs::new(9).unwrap(),
                allocator_epoch: 1,
            },
            closed_by: txn,
            kind: IndexClosedKind::Ready,
        };
        let evidence = ResolutionEvidence {
            group: catalog.clone(),
            txn,
            request: binding.request,
            manifest,
            decision,
            completed_at: position,
        };
        let mut guarded = GuardRecord {
            binding: binding.clone(),
            mode: GuardMode::Exclusive,
            granted_at: None,
            scoped_queue: None,
        };
        let mut index = 0;
        async fn apply(
            store: &GroupStore,
            index: &mut u64,
            work: impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError>
            + Send
            + 'static,
        ) -> ParticipantReply {
            *index += 1;
            let bytes = store
                .transaction(
                    ApplyMeta {
                        position: AppliedPosition {
                            term: 1,
                            node_id: 1,
                            index: *index,
                        },
                        command_digest: Digest::from_bytes([*index as u8; 32]),
                        membership: None,
                    },
                    TransactionBudget {
                        scratch_bytes: 64 << 10,
                        batch_bytes: 64 << 10,
                        output_bytes: 4 << 10,
                    },
                    work,
                )
                .await
                .unwrap();
            serde_json::from_slice(bytes.as_slice()).unwrap()
        }
        let config = Config {
            catalog_group_id: catalog.group_id,
            authority: successor,
            schema_epoch: 1,
            routing_epoch: 1,
            span: KeySpan {
                start: None,
                end: None,
            },
            gc_floor: 0,
            split_source: None,
            split_child: None,
            direct_pause: None,
            direct_schema: None,
        };
        let recover = |binding: LifecycleBinding, authority: Authority| {
            let catalog = catalog.clone();
            let config = config.clone();
            let settled = settled.clone();
            move |tx: &mut GroupTransaction<'_>| {
                recover_index_publication(
                    tx, &catalog, &config, authority, &owner, &binding, &settled,
                )
            }
        };
        assert!(matches!(
            apply(&store, &mut index, recover(binding.clone(), successor)).await,
            ParticipantReply::Rejected(Fault::Incomplete)
        ));
        let key = exclusive_key(owner.table_id);
        let pending = guarded.clone();
        apply(&store, &mut index, move |tx| {
            tx.put_serialized(&txn_key(b'd', txn), &evidence)?;
            tx.put_serialized(&key, &pending)?;
            let closure_key = MvccKey::CatalogIndexClosed {
                table_id: owner.table_id,
                closure_id: closure_id(&owner)?,
            };
            let length = key_length(&closure_key)?;
            tx.with_scratch_buffer(length + 9, |tx, buffer| {
                let end = length + 1;
                buffer[0] = VALUE;
                encode_key(&closure_key, &mut buffer[1..end]);
                buffer[end..].copy_from_slice(&(!commit_ts.get()).to_be_bytes());
                tx.put_serialized(buffer, &serde_json::json!({"value": closure}))
            })?;
            let table_key = MvccKey::CatalogTable {
                table_id: owner.table_id,
            };
            let length = key_length(&table_key)?;
            tx.with_scratch_buffer(length + 9, |tx, buffer| {
                let end = length + 1;
                buffer[0] = HEAD;
                encode_key(&table_key, &mut buffer[1..end]);
                tx.put_serialized(
                    &buffer[..end],
                    &Head {
                        version: Some(commit_ts),
                        intent: None,
                    },
                )?;
                buffer[0] = VALUE;
                buffer[end..].copy_from_slice(&(!commit_ts.get()).to_be_bytes());
                tx.put_serialized(buffer, &serde_json::json!({"value": {"schema_epoch": 1}}))
            })?;
            reply(tx, LifecycleReply::Granted)
        })
        .await;
        let mut wrong = binding.clone();
        wrong.guard_id = nabedra_rt::ids::uuid_v4();
        assert!(matches!(
            apply(&store, &mut index, recover(binding.clone(), successor)).await,
            ParticipantReply::Rejected(Fault::InvalidBinding)
        ));
        guarded.granted_at = Some(position);
        apply(&store, &mut index, move |tx| {
            tx.put_serialized(&key, &guarded)?;
            reply(tx, LifecycleReply::Granted)
        })
        .await;
        assert!(matches!(
            apply(&store, &mut index, recover(wrong.clone(), successor)).await,
            ParticipantReply::Rejected(Fault::InvalidBinding)
        ));
        let mut wrong_table = binding.clone();
        wrong_table.table_id = nabedra_rt::ids::uuid_v4();
        let mut wrong_txn = binding.clone();
        wrong_txn.txn.request_id = nabedra_rt::ids::uuid_v4();
        for mismatch in [wrong_table, wrong_txn] {
            assert!(matches!(
                apply(&store, &mut index, recover(mismatch, successor)).await,
                ParticipantReply::Rejected(Fault::InvalidBinding)
            ));
        }
        let mut live = binding.clone();
        live.authority = successor;
        let mut old_run = binding.clone();
        old_run.authority.run_id = nabedra_rt::ids::uuid_v4();
        for mismatch in [live, old_run] {
            assert!(matches!(
                apply(&store, &mut index, recover(mismatch, successor)).await,
                ParticipantReply::Rejected(Fault::StaleAuthority)
            ));
        }
        for _ in 0..2 {
            assert!(matches!(
                apply(&store, &mut index, recover(binding.clone(), successor)).await,
                ParticipantReply::Lifecycle(LifecycleReply::Released)
            ));
        }
        assert!(matches!(
            apply(&store, &mut index, recover(wrong, successor)).await,
            ParticipantReply::Rejected(Fault::InvalidBinding)
        ));
        let mut writer = binding.clone();
        writer.authority = successor;
        writer.guard_id = nabedra_rt::ids::uuid_v4();
        writer.txn.request_id = nabedra_rt::ids::uuid_v4();
        writer.request.request_id = writer.txn.request_id;
        let acquire = |binding: LifecycleBinding| {
            let catalog = catalog.clone();
            let config = config.clone();
            move |tx: &mut GroupTransaction<'_>| {
                super::apply(
                    tx,
                    &catalog,
                    &config,
                    position,
                    &LifecycleCommand::Acquire {
                        authority: successor,
                        binding,
                        mode: GuardMode::Shared,
                    },
                    None,
                )
            }
        };
        assert!(matches!(
            apply(&store, &mut index, acquire(writer.clone())).await,
            ParticipantReply::Rejected(Fault::Conflict)
        ));
        writer.lifecycle_version = Some(commit_ts);
        writer.read_ts = ReadTs::new(13).unwrap();
        assert!(matches!(
            apply(&store, &mut index, acquire(writer)).await,
            ParticipantReply::Lifecycle(LifecycleReply::Granted)
        ));
        let observed = store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 4 << 10,
                },
                move |view| {
                    let missing = view.pinned(&key)?.is_none();
                    let retired = view.pinned(&retired_key(&binding))?.ok_or_else(invalid)?;
                    let retired: LifecycleBinding =
                        serde_json::from_slice(&retired).map_err(|_| invalid())?;
                    view.encode(&(missing && retired == binding))
                },
            )
            .await
            .unwrap();
        assert!(serde_json::from_slice::<bool>(observed.as_slice()).unwrap());
    }
}
