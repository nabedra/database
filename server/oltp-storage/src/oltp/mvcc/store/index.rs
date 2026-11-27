//! Catalog certifies index closure against committed MVCC state. Offline
//! restore keeps the original closure separate from live resolution records.
use super::*;
use crate::oltp::{
    backup::{history::HistoryScope, restore::OfflineRestoreTarget},
    mvcc::{
        CanonicalJson,
        index::{IndexAttemptClosedRecord, IndexClosedKind, IndexPhase, closure_id},
    },
    pool::{ByteClass, ScratchLease},
};
use nabedra_oltp_types::types::{MaintenanceOwnerRef, StoragePin};

// Its serialized shape is also the immutable TSO release digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexClosureStatement {
    pub owner: MaintenanceOwnerRef,
    pub pin: StoragePin,
    pub terminal_position: AppliedPosition,
    pub kind: IndexClosedKind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexClosureArchiveOrigin {
    pub archive_id: Uuid,
    pub history_root: Digest,
    pub cut: CommitTs,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedIndexClosure {
    pub source: GroupIdentity,
    pub statement: IndexClosureStatement,
    pub txn: TxnId,
    pub commit_ts: CommitTs,
    pub manifest: ManifestRef,
    pub first_archive: Option<IndexClosureArchiveOrigin>,
}
impl ArchivedIndexClosure {
    fn validate(&self, catalog: &GroupIdentity) -> Result<(), ProtocolError> {
        let owner = self.statement.owner;
        let pin = self.statement.pin;
        if catalog.kind != GroupKind::Catalog
            || self.source.validate().is_err()
            || self.source.kind != GroupKind::Catalog
            || self.source.group_id != catalog.group_id
            || self.source.database != catalog.database
            || self.source.group_format_version != catalog.group_format_version
            || owner.catalog_group_id != catalog.group_id
            || owner.table_id.is_nil()
            || owner.index_id.is_nil()
            || owner.build_id.is_nil()
            || owner.attempt == 0
            || pin.pin_id.is_nil()
            || pin.allocator_epoch == 0
            || pin.read_ts >= self.commit_ts
            || self.txn.request_id.is_nil()
            || self.txn.attempt == 0
            || self.manifest.status_group_id.is_nil()
            || self.manifest.participant_count != 1
            || !valid_applied(self.statement.terminal_position)
            || match self.first_archive {
                None => self.source != *catalog,
                Some(origin) => {
                    self.source.cluster_id == catalog.cluster_id
                        || origin.archive_id.is_nil()
                        || origin.history_root == Digest::from_bytes([0; 32])
                        || origin.cut < self.commit_ts
                }
            }
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn validate_archive(
        &self,
        scope: &HistoryScope,
        catalog: &GroupIdentity,
        row: &IndexAttemptClosedRecord,
        version: CommitTs,
    ) -> Result<(), ProtocolError> {
        self.validate(catalog)?;
        if catalog.group_id != scope.catalog_group
            || catalog.cluster_id != scope.logical_cluster
            || catalog.database != scope.database
            || self.commit_ts > scope.cut
            || self
                .first_archive
                .is_some_and(|origin| origin.cut > scope.cut)
        {
            return Err(invalid());
        }
        self.matches_row(row, version)
    }

    fn matches_row(
        &self,
        row: &IndexAttemptClosedRecord,
        version: CommitTs,
    ) -> Result<(), ProtocolError> {
        if version != self.commit_ts
            || row.owner != self.statement.owner
            || row.pin != self.statement.pin
            || row.closed_by != self.txn
            || row.kind != self.statement.kind
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredIndexClosureObservation {
    pub target: GroupIdentity,
    pub restore_id: Uuid,
    pub applied: AppliedPosition,
}
impl RestoredIndexClosureObservation {
    fn validate(&self, catalog: &GroupIdentity) -> Result<(), ProtocolError> {
        if self.target != *catalog || self.restore_id.is_nil() || !valid_applied(self.applied) {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexClosureReply {
    original: ArchivedIndexClosure,
    restored_observation: Option<RestoredIndexClosureObservation>,
}
impl IndexClosureReply {
    fn validate(
        &self,
        catalog: &GroupIdentity,
        expected: MaintenanceOwnerRef,
    ) -> Result<(), ProtocolError> {
        self.original.validate(catalog)?;
        if self.original.statement.owner != expected
            || self.restored_observation.is_some() != self.original.first_archive.is_some()
        {
            return Err(invalid());
        }
        if let Some(observation) = &self.restored_observation {
            observation.validate(catalog)?;
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct VerifiedIndexAttemptClosed {
    reply: Arc<IndexClosureReply>,
    _owner: Arc<ScratchLease>,
}
impl std::fmt::Debug for VerifiedIndexAttemptClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.reply.fmt(f)
    }
}
impl VerifiedIndexAttemptClosed {
    pub fn from_authenticated(
        context: impl super::ParticipantReplyProof,
        reply: IndexClosureReply,
        expected: MaintenanceOwnerRef,
    ) -> Result<Self, ProtocolError> {
        reply.validate(context.group(), expected)?;
        context.verify_payload(
            nabedra_oltp_types::authority::ParticipantProofKind::IndexClosed,
            &reply,
        )?;
        Ok(Self {
            reply: Arc::new(reply),
            _owner: context.owner(),
        })
    }
    pub fn owner(&self) -> &MaintenanceOwnerRef {
        &self.reply.original.statement.owner
    }
    pub fn pin(&self) -> StoragePin {
        self.statement().pin
    }
    pub fn terminal_position(&self) -> AppliedPosition {
        self.statement().terminal_position
    }
    pub fn statement(&self) -> IndexClosureStatement {
        self.reply.original.statement
    }
    pub fn archive_evidence(&self) -> &ArchivedIndexClosure {
        &self.reply.original
    }
    pub fn restored_observation(&self) -> Option<&RestoredIndexClosureObservation> {
        self.reply.restored_observation.as_ref()
    }
    pub fn reply(&self) -> &IndexClosureReply {
        &self.reply
    }
}

const RESTORED_INDEX_CLOSURE: &[u8] = b"mvcc/restored-history/index-closures/v1/";
pub const RESTORED_INDEX_CLOSURE_TSO: &[u8] = b"ts/restored-history/index-closures/v1/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredIndexClosure {
    original: ArchivedIndexClosure,
    restore_id: Uuid,
}
impl RestoredIndexClosure {
    pub fn confirms(
        &self,
        catalog: &GroupIdentity,
        statement: IndexClosureStatement,
        observation: &RestoredIndexClosureObservation,
    ) -> Result<bool, ProtocolError> {
        self.original.validate(catalog)?;
        Ok(observation.validate(catalog).is_ok()
            && self.restore_id == observation.restore_id
            && self.original.statement == statement)
    }
}

pub fn restored_index_closure_key(
    prefix: &[u8],
    owner: &MaintenanceOwnerRef,
) -> Result<Vec<u8>, ProtocolError> {
    Ok([prefix, closure_id(owner)?.as_bytes()].concat())
}

pub async fn install_restored_index_closure(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    catalog: &GroupIdentity,
    mut original: ArchivedIndexClosure,
    incoming: IndexClosureArchiveOrigin,
) -> Result<(), ProtocolError> {
    let genesis = target.statement();
    original.validate(catalog)?;
    if catalog.cluster_id != genesis.source_cluster
        || catalog.group_id != genesis.catalog_group
        || catalog.database != genesis.database
        || original.commit_ts > genesis.target_cut
        || original.commit_ts > incoming.cut
    {
        return Err(invalid());
    }
    original.first_archive.get_or_insert(incoming);
    let mut target_catalog = catalog.clone();
    target_catalog.cluster_id = genesis.target_pool.cluster_id;
    original.validate(&target_catalog)?;
    let destination = store.identity();
    let prefix = if destination == &target_catalog {
        RESTORED_INDEX_CLOSURE
    } else if destination.kind == GroupKind::Timestamps
        && destination.group_id == genesis.timestamp_group
        && destination.cluster_id == target_catalog.cluster_id
        && destination.database == target_catalog.database
    {
        RESTORED_INDEX_CLOSURE_TSO
    } else {
        return Err(invalid());
    };
    let key = restored_index_closure_key(prefix, &original.statement.owner)?;
    let record = RestoredIndexClosure {
        original,
        restore_id: genesis.restore_id,
    };
    store
        .restore_genesis_batch(
            target,
            TransactionBudget {
                scratch_bytes: 64 << 10,
                batch_bytes: 64 << 10,
                output_bytes: 4096,
            },
            move |tx| {
                if let Some(previous) = load::<RestoredIndexClosure>(tx, &key)? {
                    if previous != record {
                        return Err(invalid());
                    }
                    return Ok(());
                }
                tx.put_serialized(&key, &record)?;
                Ok(())
            },
        )
        .await
}

pub fn index_closure_row(
    key: &MvccKey,
    value: Option<&CanonicalJson>,
) -> Result<Option<IndexAttemptClosedRecord>, ProtocolError> {
    let MvccKey::CatalogIndexClosed {
        table_id,
        closure_id: id,
    } = key
    else {
        return Ok(None);
    };
    let raw = value.ok_or_else(invalid)?.as_str();
    if raw.len() > 4096 {
        return Err(invalid());
    }
    let row: IndexAttemptClosedRecord = serde_json::from_str(raw).map_err(|_| invalid())?;
    if row.owner.table_id != *table_id || closure_id(&row.owner)? != *id {
        return Err(invalid());
    }
    Ok(Some(row))
}

pub fn phase_certifies_closure(
    phase: &IndexPhase,
    closure: &IndexAttemptClosedRecord,
) -> bool {
    let (build_id, attempt, pin) = phase.attempt_binding();
    if build_id != closure.owner.build_id {
        return false;
    }
    if attempt > closure.owner.attempt {
        return closure.kind == IndexClosedKind::Superseded;
    }
    if attempt != closure.owner.attempt || pin != closure.pin {
        return false;
    }
    match phase {
        IndexPhase::Building { .. } => false,
        IndexPhase::Ready { .. } => closure.kind == IndexClosedKind::Ready,
        IndexPhase::Failed { .. } => closure.kind == IndexClosedKind::Failed,
        IndexPhase::Retired { .. } => closure.kind != IndexClosedKind::Superseded,
    }
}

fn valid_applied(applied: AppliedPosition) -> bool {
    applied.term != 0 && applied.node_id != 0 && applied.index != 0
}

impl ParticipantHandle {
    pub async fn strong_index_attempt_closed(
        &self,
        owner: MaintenanceOwnerRef,
    ) -> Result<VerifiedIndexAttemptClosed, ProtocolError> {
        if owner.catalog_group_id != self.identity().group_id
            || !matches!(self.identity().kind, GroupKind::Catalog)
        {
            return Err(invalid());
        }
        if let Some(remote) = &self.remote {
            return remote.strong_index_attempt_closed(owner).await;
        }
        let barrier = self.local_consensus().ensure_linearizable().await?;
        let group = self.identity().clone();
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 128 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    #[derive(Deserialize)]
                    struct PhaseOnly {
                        phase: IndexPhase,
                    }
                    let (closed, version): (IndexAttemptClosedRecord, _) = committed_index_record(
                        view,
                        &MvccKey::CatalogIndexClosed {
                            table_id: owner.table_id,
                            closure_id: closure_id(&owner)?,
                        },
                    )?;
                    let (phase, _): (PhaseOnly, _) = committed_index_record(
                        view,
                        &MvccKey::CatalogIndex {
                            table_id: owner.table_id,
                            index_id: owner.index_id,
                        },
                    )?;
                    if closed.owner != owner || !phase_certifies_closure(&phase.phase, &closed) {
                        return Err(invalid());
                    }
                    let restored =
                        view.pinned(&restored_index_closure_key(RESTORED_INDEX_CLOSURE, &owner)?)?;
                    let resolution = view.pinned(&txn_key(b'd', closed.closed_by))?;
                    let reply = match (restored, resolution) {
                        (None, Some(resolution)) => {
                            let resolution: ResolutionEvidence =
                                serde_json::from_slice(&resolution)
                                    .map_err(|_| recovery_unknown())?;
                            if resolution.group != group
                                || resolution.txn != closed.closed_by
                                || resolution.manifest.participant_count != 1
                                || resolution.decision
                                    != (TxnDecision::Commit { commit_ts: version })
                            {
                                return Err(invalid());
                            }
                            IndexClosureReply {
                                original: ArchivedIndexClosure {
                                    source: group.clone(),
                                    statement: IndexClosureStatement {
                                        owner,
                                        pin: closed.pin,
                                        terminal_position: resolution.completed_at,
                                        kind: closed.kind,
                                    },
                                    txn: closed.closed_by,
                                    commit_ts: version,
                                    manifest: resolution.manifest,
                                    first_archive: None,
                                },
                                restored_observation: None,
                            }
                        }
                        (Some(restored), None) => {
                            let restored: RestoredIndexClosure = serde_json::from_slice(&restored)
                                .map_err(|_| recovery_unknown())?;
                            let seed = view
                                .pinned(super::restore::RESTORE_GENESIS)?
                                .ok_or_else(invalid)?;
                            let seed: super::restore::ParticipantGenesis =
                                serde_json::from_slice(&seed).map_err(|_| recovery_unknown())?;
                            if seed.target_group != group
                                || seed.restore_id != restored.restore_id
                                || seed.cut < version
                            {
                                return Err(invalid());
                            }
                            IndexClosureReply {
                                original: restored.original,
                                restored_observation: Some(RestoredIndexClosureObservation {
                                    target: group.clone(),
                                    restore_id: seed.restore_id,
                                    applied: barrier.applied,
                                }),
                            }
                        }
                        _ => return Err(invalid()),
                    };
                    reply.original.matches_row(&closed, version)?;
                    reply.validate(&group, owner)?;
                    view.encode(&reply)
                },
            )
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch(ByteClass::Progress, 4 << 10)?,
        );
        let reply = serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(VerifiedIndexAttemptClosed {
            reply: Arc::new(reply),
            _owner: owner,
        })
    }
}

fn committed_index_record<T: serde::de::DeserializeOwned>(
    view: &mut crate::oltp::pool::GroupRead<'_>,
    logical: &MvccKey,
) -> Result<(T, CommitTs), ProtocolError> {
    let length = key_length(logical)?;
    view.with_scratch_buffer(length + 9, |view, key| {
        let end = length + 1;
        encode_key(logical, &mut key[1..end]);
        key[0] = HEAD;
        let head: Head = serde_json::from_slice(&view.pinned(&key[..end])?.ok_or_else(invalid)?)
            .map_err(|_| recovery_unknown())?;
        if head.intent.is_some() {
            return Err(recovery_unknown());
        }
        let version = head.version.ok_or_else(invalid)?;
        key[0] = VALUE;
        key[end..].copy_from_slice(&(!version.get()).to_be_bytes());
        #[derive(Deserialize)]
        struct Version<'a> {
            #[serde(borrow)]
            value: Option<&'a RawValue>,
        }
        let raw = view.pinned(key)?.ok_or_else(invalid)?;
        let row: Version<'_> = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
        let value = serde_json::from_str(row.value.ok_or_else(invalid)?.get())
            .map_err(|_| recovery_unknown())?;
        Ok((value, version))
    })
}
