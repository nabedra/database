//! Checksummed streaming committed-image/history interchange. UUID history and
//! immutable decisions are distinct from the application's abandoned RAM queue.
use crate::{
    oltp::{
        failure,
        mvcc::{
            CanonicalJson,
            index::IndexAttemptClosedRecord,
            store::{
                archive::ArchiveEffects,
                encoded_length,
                image_archive::CommittedImage,
                index::{ArchivedIndexClosure, index_closure_row},
                restore::validate_archive_image,
                split_closure::{
                    ArchivedSplitPublication, CatalogSplitClosed, SplitArchiveOrigin, fold_image,
                    image_seed, split_closure_row,
                },
            },
        },
        pool::{
            ByteClass, DEFAULT_REPLY_DECODE_BYTES, GroupRead, ScratchLease, WorkerSharedResources,
        },
    },
    oltp_replication::{
        authority::{
            status::{ArchivedDecision, ArchivedIdentity, StatusTopology},
            timestamp::archive::ArchiveClosureStatement,
        },
        owned::{OwnedCommand, OwnedPayload, serialized_digest},
        types::*,
    },
    protocol::{ErrorCode, ProtocolError},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    sync::Arc,
};
use uuid::Uuid;
const MAGIC: &[u8; 16] = b"NABEDRA-HST-0001";
const MAX_FRAME: usize = 32 << 20;

pub(crate) const ARCHIVE_PAGE_WIRE_BYTES: usize = 128 << 10;

/// Metadata authority reads share a bounded reply decoder. A backup can
/// retain its current page while asking for a proof or participant manifest.
pub(crate) struct ArchivePageBudget {
    wire: usize,
    decode: usize,
}
impl ArchivePageBudget {
    const WIRE_HEADROOM: usize = 8 << 10;
    const DECODE_HEADROOM: usize = 64 << 10;

    pub(crate) fn new() -> Self {
        Self {
            wire: Self::WIRE_HEADROOM,
            decode: Self::DECODE_HEADROOM,
        }
    }

    /// Return false at a page boundary, before advancing the caller's cursor.
    pub(crate) fn include(
        &mut self,
        view: &mut GroupRead<'_>,
        item: &impl Serialize,
    ) -> Result<bool, ProtocolError> {
        let fits = || {
            failure(ErrorCode::LimitExceeded, "one archive record exceeds a page budget")
        };
        let bytes = encoded_length(item, ARCHIVE_PAGE_WIRE_BYTES).map_err(|_| fits())?;
        let wire = self.wire.checked_add(bytes + 1).ok_or_else(fits)?;
        if wire <= ARCHIVE_PAGE_WIRE_BYTES {
            let item_decode = view.with_scratch_buffer(bytes, |_, scratch| {
                let mut destination = &mut *scratch;
                serde_json::to_writer(&mut destination, item).map_err(|_| invalid())?;
                if !destination.is_empty() {
                    return Err(invalid());
                }
                crate::oltp::mvcc::json_decode_budget(scratch)
            })?;
            // The item estimator includes 4 KiB of per-message headroom,
            // more than the extra comma and per-item container accounting.
            let decode = self.decode.checked_add(item_decode).ok_or_else(fits)?;
            if decode <= DEFAULT_REPLY_DECODE_BYTES / 2 {
                self.wire = wire;
                self.decode = decode;
                return Ok(true);
            }
        }
        if self.wire == Self::WIRE_HEADROOM {
            return Err(fits());
        }
        Ok(false)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryScope {
    pub format_version: u32,
    pub archive_id: Uuid,
    pub logical_cluster: Uuid,
    pub database: Text<128>,
    pub control_group: Uuid,
    pub timestamp_group: Uuid,
    pub catalog_group: Uuid,
    pub initial_status_group: Uuid,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status_partitions: Vec<Uuid>,
    pub base_exclusive: Option<CommitTs>,
    pub cut: CommitTs,
    pub maximum_timestamp: CommitTs,
    pub previous_segment: Option<Digest>,
}
impl HistoryScope {
    pub(crate) fn status_groups(&self) -> impl Iterator<Item = Uuid> + '_ {
        std::iter::once(self.initial_status_group).chain(self.status_partitions.iter().copied())
    }

    pub(crate) fn system_groups(&self) -> impl Iterator<Item = Uuid> + '_ {
        [self.control_group, self.timestamp_group].into_iter()
            .chain(self.status_groups())
            .chain(std::iter::once(self.catalog_group))
    }

    fn status_topology(&self) -> Result<StatusTopology, ProtocolError> {
        StatusTopology::new(self.status_groups().enumerate().map(|(index, group_id)| GroupIdentity {
            cluster_id: self.logical_cluster,
            database: self.database.clone(),
            group_id,
            kind: GroupKind::Status { bucket: index as u32 },
            group_format_version: GROUP_FORMAT_VERSION,
        }).collect())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupImageScope {
    pub group: GroupIdentity,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub span: crate::oltp::mvcc::store::KeySpan,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetadataImage {
    pub group: GroupIdentity,
    pub key: Vec<u8>,
    pub commit_ts: Option<CommitTs>,
    /// Exact bounded native metadata JSON, distinct from canonical row JSON.
    pub value: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum HistoryFrame {
    Scope(HistoryScope),
    StatusClosure(ArchiveClosureStatement),
    BeginGroup(GroupImageScope),
    Image(CommittedImage),
    CatalogIndexClosure(ArchivedIndexClosure),
    CatalogSplitPublication(ArchivedSplitPublication),
    BeginSplit {
        row: Box<CatalogSplitClosed>,
        publication: ArchivedSplitPublication,
    },
    SplitImage(CommittedImage),
    EndSplitChild {
        group_id: Uuid,
    },
    EndGroup {
        group_id: Uuid,
        image_count: u64,
        root: Digest,
    },
    Metadata(MetadataImage),
    Identity {
        status: GroupIdentity,
        applied_at: AppliedPosition,
        identity: ArchivedIdentity,
    },
    BeginTransaction(ArchivedDecision),
    ParticipantEffects(ArchiveEffects),
    ParticipantManifest(ParticipantPlan),
    EndTransaction {
        txn: TxnId,
        participants: u64,
        manifest: ManifestRef,
    },
    EndArchive {
        frames: u64,
        root: Digest,
    },
}
pub(crate) struct OwnedImage {
    image: CommittedImage,
    _source: OwnedCommand<HistoryFrame>,
    _key_owner: ScratchLease,
}
impl OwnedImage {
    pub(crate) fn from_base(
        source: OwnedCommand<HistoryFrame>,
        shared: &WorkerSharedResources,
    ) -> Result<Self, ProtocolError> {
        let image = match source.payload()? {
            HistoryFrame::Image(image) | HistoryFrame::SplitImage(image) => image,
            _ => return Err(invalid()),
        };
        let owner = shared.reserve_scratch(
            ByteClass::Data,
            image
                .key
                .heap_bytes()
                .checked_add(1024)
                .ok_or_else(invalid)?,
        )?;
        let image = image.clone();
        Ok(Self {
            image,
            _source: source,
            _key_owner: owner,
        })
    }
    pub(crate) fn from_effect(
        source: OwnedCommand<HistoryFrame>,
        index: usize,
        shared: &WorkerSharedResources,
    ) -> Result<Self, ProtocolError> {
        let HistoryFrame::ParticipantEffects(page) = source.payload()? else {
            return Err(invalid());
        };
        let effect = page.effects.get(index).ok_or_else(invalid)?;
        let owner = shared.reserve_scratch(
            ByteClass::Data,
            effect
                .key
                .heap_bytes()
                .checked_add(1024)
                .ok_or_else(invalid)?,
        )?;
        let image = CommittedImage {
            key: effect.key.clone(),
            commit_ts: page.commit_ts,
            value: effect.value.clone(),
        };
        Ok(Self {
            image,
            _source: source,
            _key_owner: owner,
        })
    }
    pub(crate) fn map_catalog(
        &mut self,
        shared: &WorkerSharedResources,
        source: Uuid,
        target: Uuid,
    ) -> Result<(), ProtocolError> {
        if let (crate::oltp::mvcc::MvccKey::CatalogRoutingPage { .. }, Some(value)) =
            (&self.image.key, &self.image.value)
        {
            let _decoded = shared.reserve_scratch(
                ByteClass::Data,
                crate::oltp::mvcc::json_decode_budget(value.as_str().as_bytes())?,
            )?;
            let mut page: crate::oltp::mvcc::catalog::RoutingPage =
                serde_json::from_str(value.as_str()).map_err(|_| invalid())?;
            let mut routes = std::mem::replace(
                &mut page.shards,
                Page::new(Vec::new()).map_err(|_| invalid())?,
            )
            .into_inner();
            for route in &mut routes {
                if route.group.cluster_id != source {
                    return Err(invalid());
                }
                route.group.cluster_id = target;
            }
            page.shards = Page::new(routes).map_err(|_| invalid())?;
            self.image.value = Some(CanonicalJson::from_serializable(shared, &page, MAX_FRAME)?);
        }
        Ok(())
    }
    pub(crate) fn restore_split_publication(
        &mut self,
        shared: &WorkerSharedResources,
        mut row: CatalogSplitClosed,
        mut publication: ArchivedSplitPublication,
        origin: SplitArchiveOrigin,
    ) -> Result<(), ProtocolError> {
        if self.image.key != row.key() {
            return Err(invalid());
        }
        publication.first_archive.get_or_insert(origin);
        row.restored = Some(publication);
        self.image.value = Some(CanonicalJson::from_serializable(
            shared,
            &row,
            crate::oltp::mvcc::store::split_closure::MAX_SPLIT_CLOSURE_BYTES,
        )?);
        Ok(())
    }
    pub(crate) fn image(&self) -> Result<&CommittedImage, ProtocolError> {
        self._source.ensure_charged()?;
        Ok(&self.image)
    }
    pub(crate) fn compare(&self, other: &Self) -> std::cmp::Ordering {
        crate::oltp::mvcc::store::compare_keys(&self.image.key, &other.image.key)
            .then_with(|| self.image.commit_ts.cmp(&other.image.commit_ts))
    }
}
impl OwnedPayload for HistoryFrame {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        Ok(match self {
            Self::Scope(scope) => scope.database.capacity_bytes(),
            Self::StatusClosure(statement) => {
                statement.timestamps.database.capacity_bytes()
                    + statement.status.database.capacity_bytes()
            }
            Self::BeginGroup(group) => {
                group.group.database.capacity_bytes() + span_bytes(&group.span)
            }
            Self::Image(image) | Self::SplitImage(image) => {
                image.key.heap_bytes()
                    + image
                        .value
                        .as_ref()
                        .map_or(Ok(0), CanonicalJson::heap_bytes)?
            }
            Self::CatalogIndexClosure(closure) => closure.source.database.capacity_bytes(),
            Self::CatalogSplitPublication(publication) => {
                publication.catalog.group.database.capacity_bytes()
            }
            Self::BeginSplit { row, publication } => serialized_size(&(row, publication))?,
            Self::Metadata(image) => {
                image.group.database.capacity_bytes()
                    + image.key.capacity()
                    + image.value.capacity()
            }
            Self::ParticipantManifest(plan) => plan.group.database.capacity_bytes(),
            Self::Identity { identity, .. } => serialized_size(identity)?,
            Self::BeginTransaction(decision) => serialized_size(decision)?,
            Self::ParticipantEffects(effects) => effects.heap_bytes()?,
            _ => 0,
        })
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        match self {
            Self::Image(CommittedImage {
                value: Some(value), ..
            })
            | Self::SplitImage(CommittedImage {
                value: Some(value), ..
            }) => value.attach_charge(owner),
            Self::ParticipantEffects(effects) => effects.attach_fields(owner),
            _ => Ok(()),
        }
    }
    fn fields_attached(&self) -> bool {
        match self {
            Self::Image(CommittedImage {
                value: Some(value), ..
            })
            | Self::SplitImage(CommittedImage {
                value: Some(value), ..
            }) => value.is_charged(),
            Self::ParticipantEffects(effects) => effects.fields_attached(),
            _ => true,
        }
    }
}
fn span_bytes(span: &crate::oltp::mvcc::store::KeySpan) -> usize {
    [&span.start, &span.end]
        .into_iter()
        .map(|key| match key {
            Some(crate::protocol::Key::Utf8(key)) => key.capacity(),
            _ => 0,
        })
        .sum()
}
fn serialized_size(value: &impl Serialize) -> Result<usize, ProtocolError> {
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("history frame size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| invalid())?;
    Ok(counter.0)
}
struct GroupProgress {
    scope: GroupImageScope,
    count: u64,
    root: Digest,
}
struct TxnProgress {
    decision: ArchivedDecision,
    participants: u64,
    previous_group: Option<Uuid>,
    current: Option<(GroupIdentity, ParticipantPlan, u64)>,
    manifest_root: Digest,
}
struct PendingIndexClosure {
    catalog: GroupIdentity,
    row: IndexAttemptClosedRecord,
    version: CommitTs,
    manifest: Option<ManifestRef>,
}
struct PendingSplitPublication {
    row: CatalogSplitClosed,
    version: CommitTs,
}
struct SplitProgress {
    row: CatalogSplitClosed,
    child: usize,
    count: u64,
    root: Digest,
    last: Option<crate::oltp::mvcc::MvccKey>,
}
impl SplitProgress {
    fn new(row: CatalogSplitClosed) -> Result<Self, ProtocolError> {
        Ok(Self {
            row,
            child: 0,
            count: 0,
            root: image_seed()?,
            last: None,
        })
    }

    fn image(&mut self, image: &CommittedImage) -> Result<(), ProtocolError> {
        let child = &self.row.children[self.child];
        validate_archive_image(&split_child_scope(&self.row, self.child)?, &image.key)?;
        if image.commit_ts > self.row.source_pin.read_ts
            || self.last.as_ref().is_some_and(|last| {
                crate::oltp::mvcc::store::compare_keys(last, &image.key).is_ge()
            })
            || self.count >= child.image_count
        {
            return Err(invalid());
        }
        self.root = fold_image(self.root, image)?;
        self.count = self.count.checked_add(1).ok_or_else(invalid)?;
        self.last = Some(image.key.clone());
        Ok(())
    }

    fn end_child(&mut self, group_id: Uuid) -> Result<bool, ProtocolError> {
        let child = &self.row.children[self.child];
        if child.group.group_id != group_id
            || self.count != child.image_count
            || self.root != child.image_root
        {
            return Err(invalid());
        }
        self.child += 1;
        self.count = 0;
        self.root = image_seed()?;
        self.last = None;
        Ok(self.child == self.row.children.len())
    }
}

pub(crate) fn split_child_scope(
    row: &CatalogSplitClosed,
    index: usize,
) -> Result<GroupImageScope, ProtocolError> {
    let child = row.children.get(index).ok_or_else(invalid)?;
    Ok(GroupImageScope {
        group: child.group.clone(),
        schema_epoch: row.source_schema_epoch,
        routing_epoch: row.child_epoch()?,
        span: child.span.clone(),
    })
}

struct Chain {
    scope: Option<HistoryScope>,
    status: Option<ArchiveClosureStatement>,
    status_topology: Option<StatusTopology>,
    status_count: usize,
    group: Option<GroupProgress>,
    previous_group: Option<(bool, Uuid)>,
    index_closures: VecDeque<PendingIndexClosure>,
    split_publication: Option<PendingSplitPublication>,
    split: Option<SplitProgress>,
    last_split: Option<(Uuid, Uuid)>,
    transactions_started: bool,
    committed_count: u64,
    identity_count: u64,
    identity_root: Digest,
    previous_identity: Option<Uuid>,
    identities_complete: bool,
    decision_root: Digest,
    _retained: Option<ScratchLease>,
    txn: Option<TxnProgress>,
    frames: u64,
    root: Digest,
    finished: bool,
}
impl Chain {
    fn new() -> Self {
        Self {
            scope: None,
            status: None,
            status_topology: None,
            status_count: 0,
            group: None,
            previous_group: None,
            index_closures: VecDeque::new(),
            split_publication: None,
            split: None,
            last_split: None,
            transactions_started: false,
            committed_count: 0,
            identity_count: 0,
            identity_root: Digest::from_bytes([0; 32]),
            previous_identity: None,
            identities_complete: true,
            decision_root: serialized_digest(&b"nabedra/archive-decisions/v1".as_slice())
                .expect("fixed digest"),
            _retained: None,
            txn: None,
            frames: 0,
            root: Digest::from_bytes([0; 32]),
            finished: false,
        }
    }
    fn apply(
        &mut self,
        frame: &HistoryFrame,
        payload_digest: Digest,
        bytes: usize,
        shared: &WorkerSharedResources,
    ) -> Result<(), ProtocolError> {
        if self._retained.is_none() {
            self._retained = Some(shared.reserve_scratch(ByteClass::Data, MAX_FRAME + (64 << 10))?);
        }
        let expects_index_closure = !self.index_closures.is_empty();
        if self.finished
            || expects_index_closure != matches!(frame, HistoryFrame::CatalogIndexClosure(_))
            || self.split_publication.is_some()
                != matches!(frame, HistoryFrame::CatalogSplitPublication(_))
            || self.split.is_some()
                != matches!(
                    frame,
                    HistoryFrame::SplitImage(_) | HistoryFrame::EndSplitChild { .. }
                )
        {
            return Err(invalid());
        }
        match frame {
            HistoryFrame::Scope(scope) => {
                if self.scope.is_some()
                    || self.frames != 0
                    || !matches!((scope.format_version, scope.status_partitions.is_empty()), (1, true) | (2, false))
                    || scope.archive_id.is_nil()
                    || scope.logical_cluster.is_nil()
                    || scope.system_groups().any(|group| group.is_nil())
                    || scope.maximum_timestamp < scope.cut
                    || scope.base_exclusive.is_some_and(|base| base >= scope.cut)
                    || scope.base_exclusive.is_some() != scope.previous_segment.is_some()
                {
                    return Err(invalid());
                }
                let topology = scope.status_topology()?;
                let unique = scope.system_groups().collect::<std::collections::BTreeSet<_>>();
                if unique.len() != scope.system_groups().count() {
                    return Err(invalid());
                }
                self.status_topology = Some(topology);
                self.scope = Some(scope.clone());
            }
            HistoryFrame::StatusClosure(closure) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                if self.status.as_ref().is_some_and(|previous| {
                    previous.status.group_id >= closure.status.group_id
                        || previous.committed_count != self.committed_count
                        || previous.decision_root != self.decision_root
                        || previous
                            .identity_count
                            .is_some_and(|count| count != self.identity_count)
                        || previous
                            .identity_root
                            .is_some_and(|root| root != self.identity_root)
                }) || self.group.is_some()
                    || self.txn.is_some()
                    || closure.archive_id != scope.archive_id
                    || closure.pin.read_ts != scope.cut
                    || closure.base_exclusive != scope.base_exclusive
                    || closure.timestamps.group_id != scope.timestamp_group
                    || closure.timestamps.cluster_id != scope.logical_cluster
                    || closure.timestamps.database != scope.database
                    || closure.status.cluster_id != scope.logical_cluster
                    || closure.status.database != scope.database
                    || !self.status_topology.as_ref().is_some_and(|topology| topology.contains(&closure.status))
                {
                    return Err(invalid());
                }
                self.identity_count = 0;
                self.previous_identity = None;
                self.identity_root=crate::oltp_replication::authority::status::identity::transaction::archive_images::identity_inventory_seed(&closure.status,closure.status_applied_at)?;
                self.identities_complete &=
                    closure.identity_count.is_some() && closure.identity_root.is_some();
                self.status = Some(closure.clone());
                self.status_count += 1;
                self.committed_count = 0;
                self.decision_root =
                    serialized_digest(&b"nabedra/archive-decisions/v1".as_slice())?;
            }
            HistoryFrame::BeginGroup(group) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let order = (
                    matches!(group.group.kind, GroupKind::Data { .. }),
                    group.group.group_id,
                );
                if self.status.is_some()
                    || self.transactions_started
                    || scope.base_exclusive.is_some()
                    || self.group.is_some()
                    || self.txn.is_some()
                    || group.group.cluster_id != scope.logical_cluster
                    || group.group.database != scope.database
                    || !matches!(
                        group.group.kind,
                        GroupKind::Catalog | GroupKind::Data { .. }
                    )
                    || group.schema_epoch == 0
                    || group.routing_epoch == 0
                    || (matches!(group.group.kind, GroupKind::Data { .. })
                        && scope.system_groups().any(|id| id == group.group.group_id))
                    || (matches!(group.group.kind, GroupKind::Catalog)
                        && group.group.group_id != scope.catalog_group)
                {
                    return Err(invalid());
                }
                group.group.validate().map_err(|_| invalid())?;
                self.previous_group = Some(order);
                self.group = Some(GroupProgress {
                    scope: group.clone(),
                    count: 0,
                    root: serialized_digest(&(b"nabedra/history-group/v1", group))?,
                });
            }
            HistoryFrame::Image(image) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let group = self.group.as_mut().ok_or_else(invalid)?;
                if image.commit_ts > scope.cut {
                    return Err(invalid());
                }
                if let Some(row) = index_closure_row(&image.key, image.value.as_ref())? {
                    if group.scope.group.kind != GroupKind::Catalog {
                        return Err(invalid());
                    }
                    self.index_closures.push_back(PendingIndexClosure {
                        catalog: group.scope.group.clone(),
                        row,
                        version: image.commit_ts,
                        manifest: None,
                    });
                }
                if let Some(row) = split_closure_row(&image.key, image.value.as_ref())? {
                    if group.scope.group.kind != GroupKind::Catalog {
                        return Err(invalid());
                    }
                    self.split_publication = Some(PendingSplitPublication {
                        row,
                        version: image.commit_ts,
                    });
                }
                group.root = serialized_digest(&(b"nabedra/history-image/v1", group.root, image))?;
                group.count = group.count.checked_add(1).ok_or_else(invalid)?;
            }
            HistoryFrame::CatalogSplitPublication(publication) => {
                let pending = self.split_publication.take().ok_or_else(invalid)?;
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let catalog = &self.group.as_ref().ok_or_else(invalid)?.scope.group;
                publication.validate(&pending.row, catalog, pending.version, scope.cut)?;
            }
            HistoryFrame::BeginSplit { row, publication } => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let base = scope.base_exclusive.ok_or_else(invalid)?;
                let TxnDecision::Commit { commit_ts } = publication.status.decision else {
                    return Err(invalid());
                };
                let table = row.guard.binding().table_id;
                let order = (table, row.split_id);
                if self.status.is_some()
                    || self.group.is_some()
                    || self.txn.is_some()
                    || self.last_split.is_some_and(|prev| prev >= order)
                    || commit_ts <= base
                    || publication.first_archive.is_some()
                    || publication.catalog.group.cluster_id != scope.logical_cluster
                    || publication.catalog.group.database != scope.database
                    || publication.catalog.group.group_id != scope.catalog_group
                    || row.children.iter().any(|child| {
                        scope
                            .system_groups()
                            .any(|group| group == child.group.group_id)
                    })
                    || !scope
                        .status_groups()
                        .any(|id| id == publication.status.manifest.status_group_id)
                {
                    return Err(invalid());
                }
                crate::oltp::mvcc::store::encoded_length(
                    row.as_ref(),
                    crate::oltp::mvcc::store::split_closure::MAX_SPLIT_CLOSURE_BYTES,
                )?;
                row.validate(table, row.split_id)?;
                publication.validate(row, &publication.catalog.group, commit_ts, scope.cut)?;
                self.last_split = Some(order);
                self.split = Some(SplitProgress::new(row.as_ref().clone())?);
            }
            HistoryFrame::SplitImage(image) => {
                self.split.as_mut().ok_or_else(invalid)?.image(image)?;
            }
            HistoryFrame::EndSplitChild { group_id } => {
                if self
                    .split
                    .as_mut()
                    .ok_or_else(invalid)?
                    .end_child(*group_id)?
                {
                    self.split = None;
                }
            }
            HistoryFrame::CatalogIndexClosure(closure) => {
                let expected = self.index_closures.pop_front().ok_or_else(invalid)?;
                closure.validate_archive(
                    self.scope.as_ref().ok_or_else(invalid)?,
                    &expected.catalog,
                    &expected.row,
                    expected.version,
                )?;
                if expected.manifest.is_some_and(|manifest| {
                    closure.manifest != manifest || closure.first_archive.is_some()
                }) {
                    return Err(invalid());
                }
            }
            HistoryFrame::EndGroup {
                group_id,
                image_count,
                root,
            } => {
                let group = self.group.take().ok_or_else(invalid)?;
                if *group_id != group.scope.group.group_id
                    || *image_count != group.count
                    || *root != group.root
                {
                    return Err(invalid());
                }
            }
            HistoryFrame::Metadata(image) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                if self.group.is_some()
                    || self.txn.is_some()
                    || image.key.len() > 4096
                    || image.group.cluster_id != scope.logical_cluster
                    || image.group.database != scope.database
                    || image
                        .commit_ts
                        .is_some_and(|commit_ts| commit_ts > scope.cut)
                    || !matches!(
                        image.group.kind,
                        GroupKind::Status { .. } | GroupKind::Timestamps
                    )
                {
                    return Err(invalid());
                }
                if matches!(image.group.kind, GroupKind::Status { .. }) {
                    let closure = self.status.as_ref().ok_or_else(invalid)?;
                    if image.group != closure.status {
                        return Err(invalid());
                    }
                    if let Some((id,root))=crate::oltp_replication::authority::status::identity::transaction::archive_images::identity_metadata_step(shared,self.identity_root,image)?{
                        if self.previous_identity.is_some_and(|old| old >= id)
                            || self.status_topology.as_ref().ok_or_else(invalid)?.request(id) != &closure.status
                        {
                            return Err(invalid());
                        }
                        self.previous_identity=Some(id);self.identity_root=root;self.identity_count=self.identity_count.checked_add(1).ok_or_else(invalid)?;
                    }
                } else if image.group.group_id != scope.timestamp_group {
                    return Err(invalid());
                }
            }
            HistoryFrame::Identity {
                status,
                applied_at,
                identity,
            } => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let closure = self.status.as_ref().ok_or_else(invalid)?;
                if self.group.is_some()
                    || self.txn.is_some()
                    || status != &closure.status
                    || *applied_at != closure.status_applied_at
                    || identity.reservation.request_id.is_nil()
                    || identity.reservation.original_run.is_nil()
                    || self.status_topology.as_ref().ok_or_else(invalid)?.request(identity.reservation.request_id) != status
                    || identity.origin.as_ref().is_some_and(|origin| {
                        origin.source_cluster.is_nil() || origin.through_cut > scope.cut
                    })
                {
                    return Err(invalid());
                }
            }
            HistoryFrame::BeginTransaction(decision) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let status = self.status.as_ref().ok_or_else(invalid)?;
                let TxnDecision::Commit { commit_ts } = decision.statement.decision else {
                    return Err(invalid());
                };
                if self.txn.is_some()
                    || self.group.is_some()
                    || commit_ts > scope.cut
                    || scope.base_exclusive.is_some_and(|base| commit_ts <= base)
                    || self.status_topology.as_ref().ok_or_else(invalid)?
                        .manifest(decision.statement.txn, decision.statement.manifest)? != &status.status
                    || decision.statement.manifest.participant_count == 0
                    || decision.reservation.request_id != decision.statement.txn.request_id
                    || decision.reservation.digest != decision.statement.request.digest
                {
                    return Err(invalid());
                }
                let initial = status_digest(
                    b"nabedra/status-manifest/v1\0",
                    &(
                        status.status.group_id,
                        decision.statement.txn,
                        decision.statement.request,
                        decision.read_ts,
                        decision.statement.manifest.participant_count,
                        decision.outcome.digest()?,
                    ),
                )?;
                self.transactions_started = true;
                self.committed_count = self.committed_count.checked_add(1).ok_or_else(invalid)?;
                self.decision_root = serialized_digest(&(
                    b"nabedra/archive-decision/v1".as_slice(),
                    self.decision_root,
                    &decision.statement,
                ))?;
                self.txn = Some(TxnProgress {
                    decision: decision.clone(),
                    participants: 0,
                    previous_group: None,
                    current: None,
                    manifest_root: initial,
                });
            }
            HistoryFrame::ParticipantManifest(plan) => {
                let scope = self.scope.as_ref().ok_or_else(invalid)?;
                let txn = self.txn.as_mut().ok_or_else(invalid)?;
                if scope.base_exclusive.is_some()
                    || txn.current.is_some()
                    || txn
                        .previous_group
                        .is_some_and(|old| old >= plan.group.group_id)
                    || plan.read_ts != txn.decision.read_ts
                {
                    return Err(invalid());
                }
                plan.validate().map_err(|_| invalid())?;
                txn.manifest_root = status_digest(
                    b"nabedra/status-manifest-participant/v1\0",
                    &(txn.manifest_root, plan),
                )?;
                txn.participants = txn.participants.checked_add(1).ok_or_else(invalid)?;
                txn.previous_group = Some(plan.group.group_id);
            }
            HistoryFrame::ParticipantEffects(page) => {
                let txn = self.txn.as_mut().ok_or_else(invalid)?;
                let TxnDecision::Commit { commit_ts } = txn.decision.statement.decision else {
                    return Err(invalid());
                };
                if page.txn != txn.decision.statement.txn
                    || page.manifest != txn.decision.statement.manifest
                    || page.commit_ts != commit_ts
                    || page.plan.group != page.group
                    || page.plan.read_ts != txn.decision.read_ts
                    || page.next_ordinal > page.plan.write_count
                    || page.complete != (page.next_ordinal == page.plan.write_count)
                {
                    return Err(invalid());
                }
                let before = match &txn.current {
                    Some((group, plan, after)) if group == &page.group && plan == &page.plan => {
                        *after
                    }
                    None if txn
                        .previous_group
                        .is_none_or(|old| old < page.group.group_id) =>
                    {
                        0
                    }
                    _ => return Err(invalid()),
                };
                if page.next_ordinal <= before
                    && !(before == 0
                        && page.complete
                        && page.plan.write_count == 0
                        && page.effects.is_empty())
                {
                    return Err(invalid());
                }
                let mut previous = None;
                for effect in page.effects.iter() {
                    if effect.ordinal < before
                        || effect.ordinal >= page.next_ordinal
                        || previous.is_some_and(|old| old >= effect.ordinal)
                    {
                        return Err(invalid());
                    }
                    if let Some(row) = index_closure_row(&effect.key, effect.value.as_ref())? {
                        if page.group.kind != GroupKind::Catalog
                            || page.manifest.participant_count != 1
                            || page.txn != row.closed_by
                        {
                            return Err(invalid());
                        }
                        self.index_closures.push_back(PendingIndexClosure {
                            catalog: page.group.clone(),
                            row,
                            version: page.commit_ts,
                            manifest: Some(page.manifest),
                        });
                    }
                    previous = Some(effect.ordinal);
                }
                if page.complete {
                    txn.manifest_root = status_digest(
                        b"nabedra/status-manifest-participant/v1\0",
                        &(txn.manifest_root, &page.plan),
                    )?;
                    txn.participants = txn.participants.checked_add(1).ok_or_else(invalid)?;
                    txn.previous_group = Some(page.group.group_id);
                    txn.current = None;
                } else {
                    txn.current = Some((page.group.clone(), page.plan.clone(), page.next_ordinal));
                }
            }
            HistoryFrame::EndTransaction {
                txn: identity,
                participants,
                manifest,
            } => {
                let txn = self.txn.take().ok_or_else(invalid)?;
                if txn.current.is_some()
                    || *identity != txn.decision.statement.txn
                    || *manifest != txn.decision.statement.manifest
                    || *participants != txn.participants
                    || *participants != manifest.participant_count
                    || txn.manifest_root != manifest.digest
                {
                    return Err(invalid());
                }
            }
            HistoryFrame::EndArchive { frames, root } => {
                if self.status_topology.as_ref().is_none_or(|topology| topology.groups().len() != self.status_count)
                    || self.status.as_ref().is_none_or(|closure| {
                        closure.committed_count != self.committed_count
                            || closure.decision_root != self.decision_root
                            || closure
                                .identity_count
                                .is_some_and(|count| count != self.identity_count)
                            || closure
                                .identity_root
                                .is_some_and(|root| root != self.identity_root)
                    })
                    || self.group.is_some()
                    || self.txn.is_some()
                    || *frames != self.frames
                    || *root != self.root
                {
                    return Err(invalid());
                }
                self.finished = true;
                return Ok(());
            }
        }
        self.frames = self.frames.checked_add(1).ok_or_else(invalid)?;
        self.root = serialized_digest(&(
            b"nabedra/history-chain/v1",
            self.root,
            self.frames,
            bytes as u64,
            payload_digest,
        ))?;
        Ok(())
    }
}
fn status_digest(domain: &[u8], value: &impl Serialize) -> Result<Digest, ProtocolError> {
    struct Hash(Sha256);
    impl Write for Hash {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut hash = Hash(Sha256::new());
    hash.0.update(domain);
    serde_json::to_writer(&mut hash, value).map_err(|_| invalid())?;
    Ok(Digest::from_bytes(hash.0.finalize().into()))
}
pub(crate) struct HistoryWriter<W> {
    output: W,
    chain: Chain,
}
impl<W: Write> HistoryWriter<W> {
    pub(crate) fn new(mut output: W) -> Result<Self, ProtocolError> {
        output.write_all(MAGIC).map_err(|_| invalid())?;
        Ok(Self {
            output,
            chain: Chain::new(),
        })
    }
    pub(crate) fn write(
        &mut self,
        frame: &HistoryFrame,
        shared: &WorkerSharedResources,
    ) -> Result<(), ProtocolError> {
        let encoded = shared.encode_in(ByteClass::Data, frame, MAX_FRAME)?;
        let digest = Digest::from_bytes(Sha256::digest(encoded.as_slice()).into());
        self.chain.apply(frame, digest, encoded.len(), shared)?;
        self.output
            .write_all(&(encoded.len() as u64).to_be_bytes())
            .and_then(|_| self.output.write_all(digest.as_bytes()))
            .and_then(|_| self.output.write_all(encoded.as_slice()))
            .map_err(|_| invalid())
    }
    pub(crate) fn finish(
        mut self,
        shared: &WorkerSharedResources,
    ) -> Result<(W, Digest), ProtocolError> {
        let root = self.chain.root;
        self.write(
            &HistoryFrame::EndArchive {
                frames: self.chain.frames,
                root,
            },
            shared,
        )?;
        self.output.flush().map_err(|_| invalid())?;
        Ok((self.output, root))
    }
}
pub(crate) struct VerifiedHistory {
    scope: HistoryScope,
    root: Digest,
    frames: u64,
    identities_complete: bool,
}
impl VerifiedHistory {
    pub(crate) fn identities_complete(&self) -> bool {
        self.identities_complete
    }
    pub(crate) fn scope(&self) -> &HistoryScope {
        &self.scope
    }
    pub(crate) fn root(&self) -> Digest {
        self.root
    }
    pub(crate) fn frames(&self) -> u64 {
        self.frames
    }
}
pub(crate) struct HistoryReader<R> {
    input: R,
    chain: Chain,
    eof_checked: bool,
}
impl<R: Read> HistoryReader<R> {
    pub(crate) fn new(mut input: R) -> Result<Self, ProtocolError> {
        let mut magic = [0; 16];
        input.read_exact(&mut magic).map_err(|_| invalid())?;
        if &magic != MAGIC {
            return Err(invalid());
        }
        Ok(Self {
            input,
            chain: Chain::new(),
            eof_checked: false,
        })
    }
    pub(crate) fn next(
        &mut self,
        shared: &WorkerSharedResources,
    ) -> Result<Option<OwnedCommand<HistoryFrame>>, ProtocolError> {
        if self.chain.finished {
            let mut extra = [0];
            if self.input.read(&mut extra).map_err(|_| invalid())? != 0 {
                return Err(invalid());
            }
            self.eof_checked = true;
            return Ok(None);
        }
        let mut length = [0; 8];
        self.input.read_exact(&mut length).map_err(|_| invalid())?;
        let length = usize::try_from(u64::from_be_bytes(length)).map_err(|_| invalid())?;
        if length == 0 || length > MAX_FRAME {
            return Err(invalid());
        }
        let mut digest = [0; 32];
        self.input.read_exact(&mut digest).map_err(|_| invalid())?;
        let mut buffer = shared.allocate_bytes(ByteClass::Data, length)?;
        self.input
            .read_exact(buffer.as_mut_slice())
            .map_err(|_| invalid())?;
        let buffer = buffer.freeze();
        if <[u8; 32]>::from(Sha256::digest(buffer.as_slice())) != digest {
            return Err(invalid());
        }
        let frame = OwnedCommand::<HistoryFrame>::decode_json(
            &buffer,
            shared,
            ByteClass::Data,
            usize::MAX,
        )?;
        self.chain
            .apply(frame.payload()?, Digest::from_bytes(digest), length, shared)?;
        Ok(Some(frame))
    }
    pub(crate) fn verified(&self) -> Result<VerifiedHistory, ProtocolError> {
        if !self.chain.finished || !self.eof_checked {
            return Err(invalid());
        }
        Ok(VerifiedHistory {
            scope: self.chain.scope.clone().ok_or_else(invalid)?,
            root: self.chain.root,
            frames: self.chain.frames,
            identities_complete: self.chain.identities_complete,
        })
    }
}
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "committed history archive is incomplete, corrupt or inconsistent with its verified cut",
    )
}
