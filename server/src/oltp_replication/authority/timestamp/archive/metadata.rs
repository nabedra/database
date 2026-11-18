//! Source allocations and maintenance identities remain historical metadata.
//! They never enter live COMMIT_PREFIX or authorize a target Resolve proof.
use super::super::*;
use crate::oltp::{
    backup::{
        history::{HistoryFrame, MetadataImage},
        restore::OfflineRestoreTarget,
    },
    mvcc::{MvccKey, index::IndexRecord},
    pool::ByteClass,
};
use crate::oltp_replication::authority::status::identity::{
    RestoredHistoryOrigin, transaction::archive_images::MetadataArchivePage,
};
const HISTORY: &[u8] = b"ts/restored-history/records/";
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Historical {
    Allocation {
        origin: RestoredHistoryOrigin,
        source: GroupIdentity,
        key: Vec<u8>,
        allocation: CommitAllocation,
    },
    Maintenance {
        origin: RestoredHistoryOrigin,
        source: GroupIdentity,
        owner: MaintenanceOwnerRef,
        pin: StoragePin,
    },
}
impl Historical {
    fn timestamp(&self) -> CommitTs {
        match self {
            Self::Allocation { allocation, .. } => allocation.stamp.commit_ts,
            Self::Maintenance { pin, .. } => pin.read_ts,
        }
    }
    fn validate(&self) -> Result<(), ProtocolError> {
        let (origin, source) = match self {
            Self::Allocation {
                origin,
                source,
                key,
                allocation,
            } => {
                if source.kind != GroupKind::Timestamps
                    || allocation.stamp.permit_digest != allocation.permit.allocation_digest()?
                    || key
                        != &commit_key(
                            allocation.permit.statement().manifest.status_group_id,
                            allocation.permit.statement().txn,
                        )
                {
                    return Err(unknown());
                }
                (origin, source)
            }
            Self::Maintenance {
                origin,
                source,
                owner,
                pin,
            } => {
                if source.kind != GroupKind::Catalog
                    || source.group_id != owner.catalog_group_id
                    || owner.attempt == 0
                    || pin.pin_id.is_nil()
                    || owner.table_id.is_nil()
                    || owner.index_id.is_nil()
                    || owner.build_id.is_nil()
                {
                    return Err(unknown());
                }
                (origin, source)
            }
        };
        if source.cluster_id != origin.source_cluster
            || origin.restore_id.is_nil()
            || origin.source_archive.is_nil()
            || self.timestamp() > origin.through_cut
        {
            return Err(unknown());
        }
        Ok(())
    }
}
impl TimestampHandle {
    pub(crate) async fn native_archive_page(
        &self,
        closure: &VerifiedArchiveClosure,
        after: Option<Vec<u8>>,
    ) -> Result<MetadataArchivePage, ProtocolError> {
        if &closure.statement().timestamps != self.identity()
            || after
                .as_ref()
                .is_some_and(|key| key.len() > 4096 || !key.starts_with(b"ts/"))
        {
            return Err(unknown());
        }
        self.archive_lookup(closure.statement()).await?;
        let group = self.identity().clone();
        let cut = closure.cut();
        let raw = self
            .machine
            .store()
            .read_archive(
                TransactionBudget {
                    scratch_bytes: 2 << 20,
                    batch_bytes: 0,
                    output_bytes: 128 << 10,
                },
                move |view, _| {
                    let mut cursor = after.clone().unwrap_or_else(|| b"ts/".to_vec());
                    if after.is_some() {
                        cursor.push(0);
                    }
                    let mut next = after;
                    let mut items = Vec::new();
                    let mut used = 4096usize;
                    for _ in 0..64 {
                        let Some(row) = view.first_from(b"ts/", &cursor)? else {
                            break;
                        };
                        let commit_ts = if row.key().starts_with(COMMIT_PREFIX) {
                            if row.value().len() > 16 << 10 {
                                return Err(unknown());
                            }
                            let allocation: CommitAllocation =
                                serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                            if row.key()
                                != commit_key(
                                    allocation.permit.statement().manifest.status_group_id,
                                    allocation.permit.statement().txn,
                                )
                                || allocation.stamp.permit_digest
                                    != allocation.permit.allocation_digest()?
                            {
                                return Err(unknown());
                            }
                            Some(allocation.stamp.commit_ts)
                        } else if row.key().starts_with(HISTORY) {
                            if row.value().len() > 64 << 10 {
                                return Err(unknown());
                            }
                            let history: Historical =
                                serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                            history.validate()?;
                            Some(history.timestamp())
                        } else {
                            None
                        };
                        if let Some(commit_ts) = commit_ts.filter(|ts| *ts <= cut) {
                            let cost = row
                                .value()
                                .len()
                                .checked_mul(2)
                                .and_then(|n| n.checked_add(row.key().len() * 4 + 4096))
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
                                key: row.key().to_vec(),
                                commit_ts: Some(commit_ts),
                                value: std::str::from_utf8(row.value())
                                    .map_err(|_| unknown())?
                                    .to_owned(),
                            });
                        }
                        next = Some(row.key().to_vec());
                        cursor = row.key().to_vec();
                        cursor.push(0);
                    }
                    let exhausted = view.first_from(b"ts/", &cursor)?.is_none();
                    view.encode(&MetadataArchivePage::new(
                        group,
                        cut,
                        next,
                        exhausted,
                        Page::new(items).map_err(|_| unknown())?,
                    ))
                },
            )
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
        page.retain_owner(owner);
        Ok(page)
    }
}
fn origin(target: &OfflineRestoreTarget) -> RestoredHistoryOrigin {
    let s = target.statement();
    RestoredHistoryOrigin {
        restore_id: s.restore_id,
        source_cluster: s.source_cluster,
        source_archive: s.source_archive,
        source_root: s.source_root,
        through_cut: s.target_cut,
    }
}
pub(crate) async fn install_metadata(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    frame: OwnedCommand<HistoryFrame>,
) -> Result<(), ProtocolError> {
    let statement = target.statement().clone();
    let group = store.identity().clone();
    let origin = origin(&target);
    store
        .restore_genesis_batch(
            target,
            TransactionBudget {
                scratch_bytes: 2 << 20,
                batch_bytes: 2 << 20,
                output_bytes: 4096,
            },
            move |tx| {
                let HistoryFrame::Metadata(image) = frame.payload()? else {
                    return Err(unknown());
                };
                if image.group.group_id != group.group_id
                    || image.group.cluster_id != statement.source_cluster
                    || image.group.database != group.database
                    || group.kind != GroupKind::Timestamps
                {
                    return Err(unknown());
                }
                if image.commit_ts.is_some_and(|ts| ts > statement.target_cut) {
                    return Ok(());
                }
                if image.key.starts_with(COMMIT_PREFIX) {
                    let allocation: CommitAllocation =
                        serde_json::from_str(&image.value).map_err(|_| unknown())?;
                    if image.commit_ts != Some(allocation.stamp.commit_ts) {
                        return Err(unknown());
                    }
                    let history = Historical::Allocation {
                        origin,
                        source: image.group.clone(),
                        key: image.key.clone(),
                        allocation,
                    };
                    history.validate()?;
                    let key = [
                        HISTORY,
                        b"allocation/",
                        statement.source_root.as_bytes(),
                        image.key.as_slice(),
                    ]
                    .concat();
                    tx.put_serialized(&key, &history)?;
                } else if image.key.starts_with(HISTORY) {
                    let history: Historical =
                        serde_json::from_str(&image.value).map_err(|_| unknown())?;
                    history.validate()?;
                    if image.commit_ts != Some(history.timestamp()) {
                        return Err(unknown());
                    }
                    tx.put(&image.key, image.value.as_bytes())?;
                } else {
                    return Err(unknown());
                }
                Ok(())
            },
        )
        .await
}
pub(crate) async fn install_index_maintenance(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    source: GroupIdentity,
    key: &MvccKey,
    value: &crate::oltp::mvcc::CanonicalJson,
) -> Result<(), ProtocolError> {
    let MvccKey::CatalogIndex { table_id, index_id } = key else {
        return Ok(());
    };
    let _decode = store.resources().reserve_scratch(
        ByteClass::Data,
        crate::oltp::mvcc::json_decode_budget(value.as_str().as_bytes())?,
    )?;
    let record: IndexRecord = serde_json::from_str(value.as_str()).map_err(|_| unknown())?;
    let (build_id, attempt, pin) = record.phase.attempt_binding();
    let owner = MaintenanceOwnerRef {
        catalog_group_id: source.group_id,
        table_id: *table_id,
        index_id: *index_id,
        build_id,
        attempt,
    };
    let history = Historical::Maintenance {
        origin: origin(&target),
        source,
        owner,
        pin,
    };
    history.validate()?;
    store
        .restore_genesis_batch(
            target,
            TransactionBudget {
                scratch_bytes: 128 << 10,
                batch_bytes: 128 << 10,
                output_bytes: 4096,
            },
            move |tx| {
                let key = [HISTORY, b"maintenance/", maintenance_key(&owner).as_slice()].concat();
                if let Some(previous) = load::<Historical>(tx, &key)? {
                    previous.validate()?;
                    if let Historical::Maintenance {
                        owner: old,
                        pin: oldpin,
                        ..
                    } = previous
                    {
                        if old == owner && oldpin == pin {
                            return Ok(());
                        }
                    }
                    return Err(unknown());
                }
                tx.put_serialized(&key, &history)?;
                Ok(())
            },
        )
        .await
}
pub(in crate::oltp_replication::authority::timestamp) fn has_restored_maintenance(
    tx: &mut GroupTransaction<'_>,
    owner: MaintenanceOwnerRef,
) -> Result<bool, ProtocolError> {
    use crate::oltp::mvcc::store::index::{
        RESTORED_INDEX_CLOSURE_TSO, restored_index_closure_key,
    };
    let closure = restored_index_closure_key(RESTORED_INDEX_CLOSURE_TSO, &owner)?;
    let identity = [HISTORY, b"maintenance/", maintenance_key(&owner).as_slice()].concat();
    Ok(tx.pinned(&closure)?.is_some() || tx.pinned(&identity)?.is_some())
}

/// Match the original Catalog terminal statement and this target's observation.
pub(in crate::oltp_replication::authority::timestamp) fn restored_index_closure(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    statement: crate::oltp::mvcc::store::index::IndexClosureStatement,
    observation: &crate::oltp::mvcc::store::index::RestoredIndexClosureObservation,
) -> Result<Option<bool>, ProtocolError> {
    use crate::oltp::mvcc::store::index::{
        RESTORED_INDEX_CLOSURE_TSO, RestoredIndexClosure, restored_index_closure_key,
    };
    if scope.kind != GroupKind::Timestamps {
        return Err(unknown());
    }
    let key = restored_index_closure_key(RESTORED_INDEX_CLOSURE_TSO, &statement.owner)?;
    let Some(history) = load::<RestoredIndexClosure>(tx, &key)? else {
        return Ok(None);
    };
    let catalog = GroupIdentity {
        cluster_id: scope.cluster_id,
        database: scope.database.clone(),
        group_id: statement.owner.catalog_group_id,
        kind: GroupKind::Catalog,
        group_format_version: scope.group_format_version,
    };
    Ok(Some(history.confirms(&catalog, statement, observation)?))
}
