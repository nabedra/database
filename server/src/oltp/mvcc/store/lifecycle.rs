//! Catalog lifecycle guards remain durable until Status proves every declared
//! participant has settled. Exclusive-pending blocks new shared holders before
//! a drop waits; the wait itself occurs outside native execution.
use super::*;
use crate::oltp_replication::authority::consensus::Applied;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GuardMode {
    Shared,
    Exclusive,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LifecycleBinding {
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
pub(crate) struct SealedLifecycleGuard {
    binding: LifecycleBinding,
    mode: GuardMode,
    acquired_at: AppliedPosition,
    proof_digest: Digest,
}
impl SealedLifecycleGuard {
    pub(crate) fn binding(&self) -> &LifecycleBinding {
        &self.binding
    }
    pub(crate) fn mode(&self) -> GuardMode {
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
    fn valid(&self) -> Result<bool, ProtocolError> {
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
pub(crate) struct VerifiedLifecycleGuard {
    guard: SealedLifecycleGuard,
}
impl VerifiedLifecycleGuard {
    pub(crate) fn binding(&self) -> &LifecycleBinding {
        self.guard.binding()
    }
    pub(crate) fn mode(&self) -> GuardMode {
        self.guard.mode()
    }

    pub(crate) fn into_sealed(self) -> SealedLifecycleGuard {
        self.guard
    }
    #[cfg(test)]
    pub(crate) fn model_for_test(binding: LifecycleBinding, mode: GuardMode) -> Self {
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
pub(crate) enum LifecycleCommand {
    CancelPending {
        authority: Authority,
        binding: LifecycleBinding,
        settled: crate::oltp_replication::authority::status::SealedSettlement,
    },
    Release {
        authority: Authority,
        guard: SealedLifecycleGuard,
        settled: crate::oltp_replication::authority::status::SealedSettlement,
    },
    Acquire {
        authority: Authority,
        binding: LifecycleBinding,
        mode: GuardMode,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LifecycleReply {
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
}
fn holder_key(binding: &LifecycleBinding) -> [u8; 41] {
    let mut key = [0; 41];
    key[0] = b'G';
    key[1..17].copy_from_slice(binding.table_id.as_bytes());
    key[17..].copy_from_slice(&txn_key(b'_', binding.txn)[1..]);
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
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    position: AppliedPosition,
    command: &LifecycleCommand,
) -> Result<Transition, ProtocolError> {
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
        let mut retired = holder_key(binding);
        retired[0] = b'R';
        tx.put_serialized(&retired, binding)?;
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
            GuardMode::Exclusive => exclusive_key(binding.table_id).to_vec(),
        };
        if let Some(record) = load::<GuardRecord>(tx, &key)? {
            if record.binding != *binding
                || record.mode != guard.mode
                || record.granted_at != Some(guard.acquired_at)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            tx.delete(&key)?;
        }
        let mut retired = holder_key(binding);
        retired[0] = b'R';
        tx.put_serialized(&retired, binding)?;
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
    let mut retired = holder_key(binding);
    retired[0] = b'R';
    if tx.pinned(&retired)?.is_some() {
        return reject(tx, Fault::RolledBack);
    }
    let key = match mode {
        GuardMode::Shared => holder_key(binding).to_vec(),
        GuardMode::Exclusive => exclusive_key(binding.table_id).to_vec(),
    };
    let old = load::<GuardRecord>(tx, &key)?;
    if let Some(old) = &old {
        let mut adopted = old.binding.clone();
        let same_run = old.binding.authority.run_id == binding.authority.run_id
            && old.binding.authority.owner_term <= binding.authority.owner_term;
        adopted.authority = binding.authority;
        if !same_run || adopted != *binding || old.mode != *mode {
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
                return Ok(false);
            };
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
    });
    record.binding = binding.clone();
    if matches!(mode, GuardMode::Exclusive) {
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
    for write in writes {
        let (table, exclusive) = match &write.key {
            MvccKey::Row { table_id, .. } => (*table_id, false),
            MvccKey::CatalogTable { table_id } => {
                (*table_id, !matches!(write.change, Change::Keep))
            }
            MvccKey::CatalogIndex { table_id, .. }
            | MvccKey::CatalogIndexName { table_id, .. }
            | MvccKey::CatalogRoutingRoot { table_id }
            | MvccKey::CatalogRoutingPage { table_id, .. } => {
                (*table_id, !matches!(write.change, Change::Keep))
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
    pub(crate) async fn release_lifecycle(
        &self,
        authority: Authority,
        guard: VerifiedLifecycleGuard,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
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
    pub(crate) async fn cancel_pending_lifecycle(
        &self,
        authority: Authority,
        binding: LifecycleBinding,
        settled: crate::oltp_replication::authority::status::VerifiedSettlement,
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
    /// One durable blocking holder at a time. Drop helpers consult its current
    /// Status outside native capacity, settle declared participants, then retry.
    pub(crate) async fn lifecycle_holder(
        &self,
        table_id: Uuid,
    ) -> Result<Option<LifecycleBinding>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
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
    pub(crate) async fn acquire_lifecycle(
        &self,
        binding: LifecycleBinding,
        mode: GuardMode,
    ) -> Result<Option<VerifiedLifecycleGuard>, ProtocolError> {
        let command = ParticipantCommand::Lifecycle(LifecycleCommand::Acquire {
            authority: binding.authority,
            binding: binding.clone(),
            mode,
        });
        let applied = self
            .propose(OwnedCommand::new_owned(
                command,
                self.resources(),
                crate::oltp::pool::ByteClass::Progress,
            )?)
            .await?;
        match applied.reply {
            ParticipantReply::Lifecycle(LifecycleReply::Granted) => {}
            ParticipantReply::Lifecycle(LifecycleReply::Pending { .. }) => return Ok(None),
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
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
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
                        GuardMode::Exclusive => exclusive_key(binding.table_id).to_vec(),
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
