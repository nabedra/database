//! Immutable logical lineage for a published, completely imported split.
use super::{
    image_archive::CommittedImage,
    lifecycle::{GuardMode, SealedLifecycleGuard},
    lineage::{SplitBinding, child_digest},
    *,
};
use nabedra_oltp_types::types::{DecisionStatement, StoragePin};

pub(crate) const MAX_SPLIT_CLOSURE_BYTES: usize = 48 << 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitChildImage {
    pub group: GroupIdentity,
    pub span: KeySpan,
    pub binding: Digest,
    pub image_count: u64,
    pub image_root: Digest,
    pub imported_at: AppliedPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogSplitClosed {
    pub split_id: Uuid,
    pub guard: SealedLifecycleGuard,
    pub source: GroupIdentity,
    pub source_span: KeySpan,
    pub source_schema_epoch: u64,
    pub source_routing_epoch: u64,
    pub source_frozen_at: AppliedPosition,
    pub source_pin: StoragePin,
    pub source_root: Digest,
    pub source_pages: u64,
    pub source_images: u64,
    pub published_routing_epoch: u64,
    pub children: [SplitChildImage; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored: Option<ArchivedSplitPublication>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitArchiveOrigin {
    pub archive_id: Uuid,
    pub history_root: Digest,
    pub cut: CommitTs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchivedSplitPublication {
    pub status: DecisionStatement,
    pub catalog: ResolutionEvidence,
    pub first_archive: Option<SplitArchiveOrigin>,
}

impl CatalogSplitClosed {
    pub(crate) fn child_epoch(&self) -> Result<u64, ProtocolError> {
        self.source_routing_epoch.checked_add(1).ok_or_else(invalid)
    }

    pub(crate) fn validate(&self, table_id: Uuid, split_id: Uuid) -> Result<(), ProtocolError> {
        let guard = self.guard.binding();
        let [left, right] = &self.children;
        let child_epoch = self.child_epoch()?;
        if table_id.is_nil()
            || split_id.is_nil()
            || self.split_id != split_id
            || guard.table_id != table_id
            || guard.txn.request_id != split_id
            || guard.txn.attempt == 0
            || guard.request.request_id != split_id
            || guard.request.run_id != guard.authority.run_id
            || guard.catalog_group_id.is_nil()
            || guard
                .lifecycle_version
                .is_none_or(|version| version > guard.read_ts)
            || self.guard.mode() != GuardMode::Exclusive
            || !self.guard.valid()?
            || self.source.validate().is_err()
            || !matches!(self.source.kind, GroupKind::Data { table_id: id, .. } if id == table_id)
            || self.source_schema_epoch == 0
            || self.source_schema_epoch != guard.schema_epoch
            || self.source_routing_epoch == 0
            || self.published_routing_epoch < child_epoch
            || !valid_span(&self.source_span)
            || !valid_position(self.source_frozen_at)
            || self.source_pin.pin_id.is_nil()
            || self.source_pin.allocator_epoch == 0
            || self.source_pin.read_ts <= guard.read_ts
            || self.source_pages == 0
            || self.source_root == Digest::from_bytes([0; 32])
            || left.span.start != self.source_span.start
            || right.span.end != self.source_span.end
            || left.span.end.is_none()
            || left.span.end != right.span.start
            || left.image_count.checked_add(right.image_count) != Some(self.source_images)
        {
            return Err(invalid());
        }
        for (index, child) in self.children.iter().enumerate() {
            let binding = SplitBinding {
                split_id,
                source: self.source.clone(),
                source_span: self.source_span.clone(),
                source_schema_epoch: self.source_schema_epoch,
                source_routing_epoch: self.source_routing_epoch,
                target: child.group.clone(),
                target_span: child.span.clone(),
                read_ts: self.source_pin.read_ts,
                guard: self.guard.clone(),
            };
            if child.group
                != crate::oltp::mvcc::planner::split::child_group(&self.source, split_id, index)?
                || child.group.group_id == self.source.group_id
                || !valid_span(&child.span)
                || child.binding != child_digest(&binding)?
                || !valid_position(child.imported_at)
                || child.image_root == Digest::from_bytes([0; 32])
                || (child.image_count == 0 && child.image_root != image_seed()?)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    pub(crate) fn key(&self) -> MvccKey {
        MvccKey::CatalogSplitClosed {
            table_id: self.guard.binding().table_id,
            split_id: self.split_id,
        }
    }
}

impl ArchivedSplitPublication {
    pub(crate) fn validate(
        &self,
        row: &CatalogSplitClosed,
        catalog: &GroupIdentity,
        version: CommitTs,
        cut: CommitTs,
    ) -> Result<(), ProtocolError> {
        let guard = row.guard.binding();
        let original = &self.catalog.group;
        if catalog.kind != GroupKind::Catalog
            || original.validate().is_err()
            || original.kind != GroupKind::Catalog
            || original.cluster_id != row.source.cluster_id
            || original.database != row.source.database
            || original.group_id != guard.catalog_group_id
            || original.group_id != catalog.group_id
            || original.database != catalog.database
            || original.group_format_version != catalog.group_format_version
            || version <= row.source_pin.read_ts
            || version > cut
            || self.status.txn != guard.txn
            || self.status.request != guard.request
            || self.status.manifest.status_group_id.is_nil()
            || self.status.manifest.participant_count != 1
            || self.status.decision != (TxnDecision::Commit { commit_ts: version })
            || !valid_position(self.status.decided_at)
            || self.catalog.txn != self.status.txn
            || self.catalog.request != self.status.request
            || self.catalog.manifest != self.status.manifest
            || self.catalog.decision != self.status.decision
            || !valid_position(self.catalog.completed_at)
            || match self.first_archive {
                None => original != catalog || row.restored.is_some(),
                Some(origin) => {
                    original.cluster_id == catalog.cluster_id
                        || row.restored.as_ref() != Some(self)
                        || origin.archive_id.is_nil()
                        || origin.history_root == Digest::from_bytes([0; 32])
                        || origin.cut < version
                        || origin.cut > cut
                }
            }
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(crate) fn split_closure_row(
    key: &MvccKey,
    value: Option<&super::super::CanonicalJson>,
) -> Result<Option<CatalogSplitClosed>, ProtocolError> {
    let MvccKey::CatalogSplitClosed { table_id, split_id } = key else {
        return Ok(None);
    };
    let raw = value.ok_or_else(invalid)?.as_str();
    if raw.len() > MAX_SPLIT_CLOSURE_BYTES {
        return Err(invalid());
    }
    let row: CatalogSplitClosed = serde_json::from_str(raw).map_err(|_| invalid())?;
    row.validate(*table_id, *split_id)?;
    Ok(Some(row))
}

pub(crate) fn image_seed() -> Result<Digest, ProtocolError> {
    hash(b"nabedra/split-child-image-seed/v1\0", &())
}

pub(crate) fn fold_image(root: Digest, image: &CommittedImage) -> Result<Digest, ProtocolError> {
    hash(b"nabedra/split-child-image/v1\0", &(root, image))
}

fn valid_position(position: AppliedPosition) -> bool {
    position.term != 0 && position.node_id != 0 && position.index != 0
}
