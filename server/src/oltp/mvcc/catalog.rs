//! Catalog reads share the application's pinned MVCC timestamp. The snapshot
//! token is bounded; table and routing pages stay on disk until requested.
use super::{
    CanonicalJson, Change, MvccKey, PreparedWrite, json_decode_budget,
    store::{
        KeySpan, ParticipantHandle,
        lifecycle::{GuardMode, VerifiedLifecycleGuard},
    },
};
use crate::{
    oltp::{
        failure,
        pool::{ChargedBytes, ExecutionClass, ScratchLease, WorkerSharedResources},
    },
    oltp_replication::types::{
        Authority, CommitTs, GroupIdentity, GroupKind, ManifestRef, Page, ReadTs, Text, TxnId,
    },
    protocol::{ErrorCode, Key, ProtocolError, TableDescriptor},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::sync::Arc;
use uuid::Uuid;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinnedCatalog {
    pub catalog: GroupIdentity,
    pub read_ts: ReadTs,
    pub authority: Authority,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableRecord {
    pub descriptor: TableDescriptor,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    /// Routing-only versions retain this identity. Schema/index transitions
    /// replace it, so an old pin cannot omit a newly Building index.
    #[serde(default, skip_serializing_if = "Uuid::is_nil")]
    pub lifecycle_id: Uuid,
}
impl TableRecord {
    pub(super) fn with_lifecycle(&self, id: Uuid) -> Self {
        Self {
            lifecycle_id: id,
            ..self.clone()
        }
    }
}
pub(super) fn lifecycle_anchor(
    table_id: Uuid,
    schema_epoch: u64,
    version: Option<CommitTs>,
    stored: Uuid,
) -> Result<Uuid, ProtocolError> {
    if !stored.is_nil() {
        return Ok(stored);
    }
    let version = version.ok_or_else(invalid)?;
    let digest = crate::oltp_replication::owned::serialized_digest(&(
        b"nabedra/legacy-table-lifecycle/v1".as_slice(),
        table_id,
        schema_epoch,
        version,
    ))?;
    Uuid::from_slice(&digest.as_bytes()[..16]).map_err(|_| invalid())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NameRecord {
    pub table_id: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoutingRoot {
    pub first_page: Option<Uuid>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShardRoute {
    pub group: GroupIdentity,
    pub span: KeySpan,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoutingPage {
    pub shards: Page<ShardRoute>,
    pub next_page: Option<Uuid>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct IntentRef {
    pub txn: TxnId,
    pub manifest: ManifestRef,
}
#[derive(Clone)]
pub(crate) struct CatalogHandle {
    participant: Arc<ParticipantHandle>,
}
pub(crate) struct OwnedRecord<T> {
    inner: Arc<RecordInner<T>>,
}
struct RecordInner<T> {
    value: T,
    _owner: Arc<ScratchLease>,
}
impl<T> Clone for OwnedRecord<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T> std::ops::Deref for OwnedRecord<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner.value
    }
}
pub(crate) enum CatalogRead<T> {
    Value {
        version: Option<CommitTs>,
        record: Option<OwnedRecord<T>>,
    },
    Blocked(IntentRef),
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireRead<'a> {
    Value {
        version: Option<CommitTs>,
        #[serde(borrow)]
        value: Option<&'a RawValue>,
    },
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
}
impl CatalogHandle {
    pub(crate) fn new(participant: Arc<ParticipantHandle>) -> Result<Self, ProtocolError> {
        if !matches!(participant.identity().kind, GroupKind::Catalog) {
            return Err(invalid());
        }
        Ok(Self { participant })
    }
    pub(crate) fn participant(&self) -> &Arc<ParticipantHandle> {
        &self.participant
    }
    pub(crate) fn resources(&self) -> &Arc<WorkerSharedResources> {
        self.participant.resources()
    }
    pub(crate) fn snapshot(
        &self,
        read_ts: ReadTs,
        authority: Authority,
    ) -> Result<PinnedCatalog, ProtocolError> {
        authority.validate().map_err(|_| invalid())?;
        Ok(PinnedCatalog {
            catalog: self.participant.identity().clone(),
            read_ts,
            authority,
        })
    }
    fn check(&self, snapshot: &PinnedCatalog) -> Result<(), ProtocolError> {
        if snapshot.catalog != *self.participant.identity() {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    pub(crate) async fn read<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        snapshot: &PinnedCatalog,
        key: MvccKey,
    ) -> Result<CatalogRead<T>, ProtocolError> {
        self.check(snapshot)?;
        decode_record(
            self.resources(),
            self.participant.read_at(snapshot.read_ts, key).await?,
        )
        .await
    }
    pub(crate) async fn describe(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<CatalogRead<TableRecord>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogTable { table_id })
            .await
    }
    pub(crate) async fn describe_name(
        &self,
        snapshot: &PinnedCatalog,
        name: &str,
    ) -> Result<CatalogRead<TableRecord>, ProtocolError> {
        let name = Text::try_from(name).map_err(|_| invalid())?;
        match self
            .read::<NameRecord>(snapshot, MvccKey::CatalogName { name })
            .await?
        {
            CatalogRead::Blocked(intent) => Ok(CatalogRead::Blocked(intent)),
            CatalogRead::Value {
                record: Some(record),
                ..
            } => self.describe(snapshot, record.table_id).await,
            CatalogRead::Value {
                version,
                record: None,
            } => Ok(CatalogRead::Value {
                version,
                record: None,
            }),
        }
    }
    pub(crate) async fn routing_root(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<CatalogRead<RoutingRoot>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogRoutingRoot { table_id })
            .await
    }
    pub(crate) async fn routing_page(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        page_id: Uuid,
    ) -> Result<CatalogRead<RoutingPage>, ProtocolError> {
        self.read(snapshot, MvccKey::CatalogRoutingPage { table_id, page_id })
            .await
    }
    /// Linked bounded pages permit unlimited shard growth. A malformed cycle
    /// cannot be published by the guarded routing writer; no whole routing map
    /// is allocated by a point lookup.
    pub(crate) async fn route(
        &self,
        snapshot: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
    ) -> Result<RouteRead, ProtocolError> {
        let mut next = match self.routing_root(snapshot, table_id).await? {
            CatalogRead::Blocked(i) => return Ok(RouteRead::Blocked(i)),
            CatalogRead::Value {
                record: Some(root), ..
            } => root.first_page,
            _ => return Ok(RouteRead::Missing),
        };
        while let Some(page_id) = next {
            match self.routing_page(snapshot, table_id, page_id).await? {
                CatalogRead::Blocked(i) => return Ok(RouteRead::Blocked(i)),
                CatalogRead::Value {
                    record: Some(page), ..
                } => {
                    for (index, route) in page.shards.iter().enumerate() {
                        if contains(&route.span, key) {
                            return Ok(RouteRead::Route { page, index });
                        }
                    }
                    next = page.next_page;
                }
                _ => return Err(invalid()),
            }
        }
        Ok(RouteRead::Missing)
    }
    /// Create publishes four MVCC keys together through the ordinary Status
    /// manifest. The new Data group must already be enrolled and fenced before
    /// this plan is proposed; there is no direct catalog visibility write.
    pub(crate) fn create_writes(
        &self,
        descriptor: TableDescriptor,
        route: ShardRoute,
        page_id: Uuid,
        guard: &VerifiedLifecycleGuard,
    ) -> Result<Vec<PreparedWrite>, ProtocolError> {
        descriptor.validate()?;
        let binding = guard.binding();
        if binding.table_id != descriptor.id
            || binding.catalog_group_id != self.participant.identity().group_id
            || binding.lifecycle_version.is_some()
            || guard.mode() != GuardMode::Exclusive
            || page_id.is_nil()
            || !matches!(route.group.kind,GroupKind::Data{table_id,..}if table_id==descriptor.id)
            || route.group.cluster_id != self.participant.identity().cluster_id
            || route.group.database != self.participant.identity().database
        {
            return Err(invalid());
        }
        let table_id = descriptor.id;
        let name = Text::try_from(descriptor.name.as_str()).map_err(|_| invalid())?;
        let root = RoutingRoot {
            first_page: Some(page_id),
            schema_epoch: route.schema_epoch,
            routing_epoch: route.routing_epoch,
        };
        let page = RoutingPage {
            shards: Page::new(vec![route.clone()]).map_err(|_| invalid())?,
            next_page: None,
        };
        let record = TableRecord {
            descriptor,
            schema_epoch: route.schema_epoch,
            routing_epoch: route.routing_epoch,
            lifecycle_id: table_id,
        };
        let entries = [
            (
                MvccKey::CatalogName { name },
                self.canonical(&NameRecord { table_id })?,
            ),
            (MvccKey::CatalogTable { table_id }, self.canonical(&record)?),
            (
                MvccKey::CatalogRoutingRoot { table_id },
                self.canonical(&root)?,
            ),
            (
                MvccKey::CatalogRoutingPage { table_id, page_id },
                self.canonical(&page)?,
            ),
        ];
        Ok(entries
            .into_iter()
            .map(|(key, value)| PreparedWrite {
                key,
                expected_version: None,
                change: Change::Put(value),
            })
            .collect())
    }
    fn canonical(&self, value: &impl Serialize) -> Result<CanonicalJson, ProtocolError> {
        CanonicalJson::from_serializable(self.resources(), value, crate::protocol::MAX_BODY_BYTES)
    }
}
pub(crate) enum RouteRead {
    Route {
        page: OwnedRecord<RoutingPage>,
        index: usize,
    },
    Blocked(IntentRef),
    Missing,
}
impl RouteRead {
    pub(crate) fn route(&self) -> Option<&ShardRoute> {
        match self {
            Self::Route { page, index } => page.shards.get(*index),
            _ => None,
        }
    }
}
pub(crate) async fn decode_record<T: DeserializeOwned + Send + Sync + 'static>(
    shared: &Arc<WorkerSharedResources>,
    raw: ChargedBytes,
) -> Result<CatalogRead<T>, ProtocolError> {
    let owner = Arc::new(
        shared
            .reserve_scratch_wait(
                crate::oltp::pool::ByteClass::Data,
                json_decode_budget(raw.as_slice())?,
                0,
            )
            .await?,
    );
    shared
        .execute(ExecutionClass::Query, 0, 0, move || {
            let decoded: WireRead<'_> =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            match decoded {
                WireRead::Blocked { txn, manifest } => {
                    Ok(CatalogRead::Blocked(IntentRef { txn, manifest }))
                }
                WireRead::Value { version, value } => {
                    let record = value
                        .map(|v| serde_json::from_str(v.get()).map_err(|_| invalid()))
                        .transpose()?;
                    Ok(CatalogRead::Value {
                        version,
                        record: record.map(|value| OwnedRecord {
                            inner: Arc::new(RecordInner {
                                value,
                                _owner: owner,
                            }),
                        }),
                    })
                }
            }
        })
        .await
}
pub(crate) async fn decode_canonical<T: DeserializeOwned + Send + Sync + 'static>(
    shared: &Arc<WorkerSharedResources>,
    raw: CanonicalJson,
) -> Result<OwnedRecord<T>, ProtocolError> {
    let owner = Arc::new(
        shared
            .reserve_scratch_wait(
                crate::oltp::pool::ByteClass::Data,
                json_decode_budget(raw.as_str().as_bytes())?,
                0,
            )
            .await?,
    );
    shared
        .execute(ExecutionClass::Query, 0, 0, move || {
            let value = serde_json::from_str(raw.as_str()).map_err(|_| invalid())?;
            Ok(OwnedRecord {
                inner: Arc::new(RecordInner {
                    value,
                    _owner: owner,
                }),
            })
        })
        .await
}

pub(super) fn contains(span: &KeySpan, key: &Key) -> bool {
    span.start
        .as_ref()
        .is_none_or(|a| primary(a, key) != std::cmp::Ordering::Greater)
        && span
            .end
            .as_ref()
            .is_none_or(|b| primary(key, b) == std::cmp::Ordering::Less)
}
pub(super) fn primary(a: &Key, b: &Key) -> std::cmp::Ordering {
    match (a, b) {
        (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
        (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
        (Key::Int64(_), _) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Greater,
    }
}
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::ValidationError,
        "invalid pinned catalog or routing lineage",
    )
}

/// The root commits to an actual namespaced timestamp cut and the requested
/// table's immutable identities/versions. Capturing it does not clone all tables.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogCapture {
    pub pin: PinnedCatalog,
    pub catalog_root: crate::oltp_replication::types::Digest,
    pub table_id: Option<Uuid>,
    pub name_version: Option<CommitTs>,
    pub table_version: Option<CommitTs>,
    pub routing_version: Option<CommitTs>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
pub(crate) enum CaptureRead {
    Captured(CatalogCapture),
    Blocked(IntentRef),
}
impl CatalogHandle {
    pub(crate) async fn capture_lineage(
        &self,
        pin: &PinnedCatalog,
        operation: &crate::protocol::Operation,
    ) -> Result<CaptureRead, ProtocolError> {
        self.check(pin)?;
        let namespace = self.participant.epochs().await?;
        if namespace.authority != pin.authority {
            return Err(invalid());
        }
        let mut capture = CatalogCapture {
            pin: pin.clone(),
            catalog_root: crate::oltp_replication::types::Digest::from_bytes([0; 32]),
            table_id: None,
            name_version: None,
            table_version: None,
            routing_version: None,
            schema_epoch: namespace.schema_epoch,
            routing_epoch: namespace.routing_epoch,
        };
        use crate::protocol::Operation;
        let name = match operation {
            Operation::Create(p) => Some(p.name.as_str()),
            Operation::Describe(p) => Some(p.name.as_str()),
            _ => None,
        };
        if let Some(name) = name {
            match self
                .read::<NameRecord>(
                    pin,
                    MvccKey::CatalogName {
                        name: Text::try_from(name).map_err(|_| invalid())?,
                    },
                )
                .await?
            {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.name_version = version;
                    capture.table_id = record.map(|r| r.table_id);
                }
            }
        } else {
            capture.table_id = match operation {
                Operation::Drop(p) => Some(p.table_id),
                Operation::Insert(p) | Operation::Upsert(p) => Some(p.table_id),
                Operation::InsertMany(p) => Some(p.table_id),
                Operation::Get(p) | Operation::Delete(p) => Some(p.table_id),
                Operation::Update(p) => Some(p.table_id),
                Operation::Collect(p) | Operation::Count(p) | Operation::Explain(p) => {
                    Some(p.table_id)
                }
                _ => None,
            };
        }
        if let Some(table_id) = capture.table_id {
            match self.describe(pin, table_id).await? {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.table_version = version;
                    if let Some(table) = record {
                        capture.schema_epoch = table.schema_epoch;
                        capture.routing_epoch = table.routing_epoch;
                    }
                }
            }
            match self.routing_root(pin, table_id).await? {
                CatalogRead::Blocked(i) => return Ok(CaptureRead::Blocked(i)),
                CatalogRead::Value { version, record } => {
                    capture.routing_version = version;
                    if let Some(root) = record {
                        if root.schema_epoch != capture.schema_epoch
                            || root.routing_epoch != capture.routing_epoch
                        {
                            return Err(invalid());
                        }
                    }
                }
            }
        }
        // Authority terms fence execution but do not change an immutable cut.
        // A same-run takeover must reconstruct the same pre-accept lineage.
        // List/create-absent uses this genuine namespace identity/epoch plus
        // the allocator-pinned read timestamp. Every catalog key is MVCC at s.
        capture.catalog_root = crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/pinned-catalog-lineage/v1".as_slice(),
            &pin.catalog,
            pin.read_ts,
            pin.authority.run_id,
            capture.table_id,
            capture.name_version,
            capture.table_version,
            capture.routing_version,
            capture.schema_epoch,
            capture.routing_epoch,
        ))?;
        Ok(CaptureRead::Captured(capture))
    }
}
