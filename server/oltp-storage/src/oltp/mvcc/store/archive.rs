//! Committed participant images for a verified history archive. A Keep has no
//! version at this transaction's unique commit timestamp and emits no effect.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};
use nabedra_oltp_types::{authority::StatusDecisionProof, types::DecisionStatement};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEffect {
    pub ordinal: u64,
    pub key: MvccKey,
    pub value: Option<super::super::CanonicalJson>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEffects {
    pub group: GroupIdentity,
    pub txn: TxnId,
    pub manifest: ManifestRef,
    pub commit_ts: CommitTs,
    pub plan: ParticipantPlan,
    pub span: KeySpan,
    pub next_ordinal: u64,
    pub complete: bool,
    pub effects: Page<ArchiveEffect>,
}
impl ParticipantMachine {
    pub(crate) async fn archive_effect_page(
        &self,
        proof: impl StatusDecisionProof,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    ) -> Result<OwnedCommand<ArchiveEffects>, ProtocolError> {
        self.archive_effect_page_from_statement(proof.statement(), after, max_items, max_bytes)
            .await
    }

    pub(super) async fn archive_effect_page_from_statement(
        &self,
        statement: &DecisionStatement,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    ) -> Result<OwnedCommand<ArchiveEffects>, ProtocolError> {
        if max_items == 0
            || max_items > 128
            || max_bytes < 64 << 10
            || max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
        {
            return Err(invalid());
        }
        let statement = statement.clone();
        let TxnDecision::Commit { commit_ts } = statement.decision else {
            return Err(invalid());
        };
        let shared = self.store.resources();
        let owner = Arc::new(
            shared
                .reserve_scratch_wait(
                    ByteClass::Data,
                    max_bytes
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(128 << 10))
                        .ok_or_else(invalid)?,
                    0,
                )
                .await?,
        );
        let group = self.store.identity().clone();
        self.store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let raw = view
                        .pinned(&txn_key(b't', statement.txn))?
                        .ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    #[derive(Deserialize)]
                    struct AuditVersion {
                        #[serde(default)]
                        archive_audit_version: Option<u32>,
                    }
                    if serde_json::from_slice::<AuditVersion>(&raw)
                        .map_err(|_| recovery_unknown())?
                        .archive_audit_version
                        != Some(1)
                    {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "participant transaction predates verified archive audit coverage",
                        ));
                    }
                    let state: Txn =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if state.request != statement.request
                        || state.manifest != statement.manifest
                        || state.decision != Some(statement.decision)
                        || state.resolved != state.writes
                        || state.sealed.is_none()
                        || after > state.writes
                        || state.plan.group != group
                    {
                        return Err(invalid());
                    }
                    let resolution = view
                        .pinned(&txn_key(b'd', statement.txn))?
                        .ok_or_else(invalid)?;
                    let resolution: ResolutionEvidence =
                        serde_json::from_slice(&resolution).map_err(|_| recovery_unknown())?;
                    if resolution.group != group
                        || resolution.txn != statement.txn
                        || resolution.manifest != statement.manifest
                        || resolution.request != statement.request
                        || resolution.decision != statement.decision
                    {
                        return Err(invalid());
                    }
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if config.schema_epoch != state.plan.schema_epoch
                        || config.routing_epoch != state.plan.routing_epoch
                    {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "archived participant lineage changed",
                        ));
                    }
                    if commit_ts.get() < config.gc_floor {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "archived transaction is below the retained history floor",
                        ));
                    }
                    let mut effects = Vec::with_capacity(max_items as usize);
                    let mut used = 4096
                        + span_heap_bytes(&config.span)
                        + effects.capacity() * std::mem::size_of::<ArchiveEffect>();
                    let mut next = after;
                    while next < state.writes && next - after < u64::from(max_items) {
                        let logical = view
                            .pinned(&ordinal_key(b'w', statement.txn, next))?
                            .ok_or_else(recovery_unknown)?;
                        let length = logical.len();
                        #[derive(Deserialize)]
                        struct Audit {
                            change: IntentChange,
                            expected_version: Option<CommitTs>,
                            value_digest: Option<Digest>,
                        }
                        let audit = view
                            .pinned(&ordinal_key(b'a', statement.txn, next))?
                            .ok_or_else(recovery_unknown)?;
                        let audit: Audit =
                            serde_json::from_slice(&audit).map_err(|_| recovery_unknown())?;
                        if audit
                            .expected_version
                            .is_some_and(|version| version > state.plan.read_ts)
                        {
                            return Err(recovery_unknown());
                        }
                        let effect = view.with_scratch_buffer(length + 9, |view, key| {
                            key[0] = VALUE;
                            key[1..length + 1].copy_from_slice(&logical);
                            key[length + 1..].copy_from_slice(&(!commit_ts.get()).to_be_bytes());
                            let version = view.pinned(key)?;
                            if matches!(audit.change, IntentChange::Keep) {
                                if version.is_some() || audit.value_digest.is_some() {
                                    return Err(recovery_unknown());
                                }
                                return Ok(Some(None));
                            }
                            let version = version.ok_or_else(recovery_unknown)?;
                            #[derive(Deserialize)]
                            struct Version<'a> {
                                #[serde(borrow)]
                                value: Option<&'a RawValue>,
                            }
                            let version: Version<'_> =
                                serde_json::from_slice(&version).map_err(|_| recovery_unknown())?;
                            match (audit.change, version.value, audit.value_digest) {
                                (IntentChange::Put, Some(value), Some(expected))
                                    if Digest::from_bytes(
                                        Sha256::digest(value.get().as_bytes()).into(),
                                    ) == expected => {}
                                (IntentChange::Delete, None, None) => {}
                                _ => return Err(recovery_unknown()),
                            }
                            let cost = logical
                                .len()
                                .checked_add(version.value.map_or(0, |v| v.get().len()))
                                .and_then(|n| n.checked_add(8192))
                                .ok_or_else(invalid)?;
                            if used.checked_add(cost).is_none_or(|n| n > max_bytes) {
                                return Ok(None);
                            }
                            used += cost;
                            let key = decode_logical(&logical)?;
                            let value = version
                                .value
                                .map(|value| {
                                    super::super::CanonicalJson::from_stored_json(
                                        value.get().as_bytes(),
                                        owner.clone(),
                                    )
                                })
                                .transpose()?;
                            Ok(Some(Some(ArchiveEffect {
                                ordinal: next,
                                key,
                                value,
                            })))
                        })?;
                        match effect {
                            None => {
                                if effects.is_empty() {
                                    return Err(failure(
                                        ErrorCode::LimitExceeded,
                                        "one archive effect exceeds page working budget",
                                    ));
                                }
                                break;
                            }
                            Some(None) => {}
                            Some(Some(effect)) => effects.push(effect),
                        }
                        next += 1;
                    }
                    owner.shrink_unused(used)?;
                    OwnedCommand::new_with_owner(
                        ArchiveEffects {
                            group,
                            txn: statement.txn,
                            manifest: statement.manifest,
                            commit_ts,
                            plan: state.plan,
                            span: config.span,
                            next_ordinal: next,
                            complete: next == state.writes,
                            effects: Page::new(effects).map_err(|_| invalid())?,
                        },
                        owner,
                    )
                },
            )
            .await
    }
}
impl ParticipantHandle {
    pub async fn archive_effect_page(
        &self,
        proof: impl StatusDecisionProof,
        after: u64,
        max_items: u16,
        max_bytes: usize,
    ) -> Result<OwnedCommand<ArchiveEffects>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote
                .archive_effect_page(&proof, after, max_items, max_bytes)
                .await;
        }
        self.local_consensus().ensure_linearizable().await?;
        self.local_machine()
            .archive_effect_page(proof, after, max_items, max_bytes)
            .await
    }
}
pub(super) fn decode_logical(bytes: &[u8]) -> Result<MvccKey, ProtocolError> {
    if bytes.is_empty() {
        return Err(recovery_unknown());
    }
    if matches!(bytes[0], 1 | 2 | 10 | super::direct::DEDUP_TAG) {
        return super::scan::decode_key(bytes);
    }
    if bytes.len() < 33 {
        return Err(recovery_unknown());
    }
    let table_id = Uuid::from_slice(&bytes[1..17]).map_err(|_| recovery_unknown())?;
    let id = Uuid::from_slice(&bytes[17..33]).map_err(|_| recovery_unknown())?;
    if table_id.is_nil() {
        return Err(recovery_unknown());
    }
    if bytes[0] == 5 {
        let name = super::scan::decode_text(&bytes[33..])?;
        return Ok(MvccKey::CatalogIndexName {
            table_id,
            name: nabedra_oltp_types::types::Text::new(name)
                .map_err(|_| recovery_unknown())?,
        });
    }
    if bytes.len() != 33 {
        return Err(recovery_unknown());
    }
    Ok(match bytes[0] {
        3 if id.is_nil() => MvccKey::CatalogTable { table_id },
        4 if !id.is_nil() => MvccKey::CatalogIndex {
            table_id,
            index_id: id,
        },
        6 if id.is_nil() => MvccKey::CatalogRoutingRoot { table_id },
        7 if !id.is_nil() => MvccKey::CatalogRoutingPage {
            table_id,
            page_id: id,
        },
        8 if id.is_nil() => MvccKey::CatalogIndexesRoot { table_id },
        9 if !id.is_nil() => MvccKey::CatalogIndexesPage {
            table_id,
            page_id: id,
        },
        11 if !id.is_nil() => MvccKey::CatalogIndexProgress {
            table_id,
            progress_id: id,
        },
        12 if !id.is_nil() => MvccKey::CatalogIndexClosed {
            table_id,
            closure_id: id,
        },
        13 if !id.is_nil() => MvccKey::CatalogSplitClosed {
            table_id,
            split_id: id,
        },
        _ => return Err(recovery_unknown()),
    })
}
impl OwnedPayload for ArchiveEffects {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.effects.iter().try_fold(
            self.group.database.capacity_bytes()
                + self.plan.group.database.capacity_bytes()
                + span_heap_bytes(&self.span)
                + self.effects.capacity() * std::mem::size_of::<ArchiveEffect>(),
            |n, e| {
                n.checked_add(e.key.heap_bytes())
                    .and_then(|n| {
                        e.value
                            .as_ref()
                            .map_or(Ok(0), |v| v.heap_bytes())
                            .ok()
                            .and_then(|v| n.checked_add(v))
                    })
                    .ok_or_else(invalid)
            },
        )
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        for effect in self.effects.iter() {
            if let Some(value) = &effect.value {
                value.attach_charge(owner.clone())?;
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        self.effects.iter().all(|effect| {
            effect
                .value
                .as_ref()
                .is_none_or(|v| v.ensure_charged().is_ok())
        })
    }
}
fn span_heap_bytes(span: &KeySpan) -> usize {
    [&span.start, &span.end]
        .into_iter()
        .filter_map(|key| match key {
            Some(Key::Utf8(value)) => Some(value.capacity()),
            _ => None,
        })
        .sum()
}
