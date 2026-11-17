//! ORM operation planning. Reads produce bounded encoded payloads; mutations
//! produce fixed changes and their exact result, never a replayable Operation.
use super::{
    CanonicalJson, Change, MvccKey, PreparedWrite,
    catalog::{
        CatalogHandle, CatalogRead, IntentRef, NameRecord, OwnedRecord, PinnedCatalog, RouteRead,
        RoutingRoot, ShardRoute, TableRecord, decode_record,
    },
    store::{
        ParticipantHandle,
        lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
    },
};
use crate::{
    oltp::{failure, pool::ChargedBytes},
    oltp_replication::{
        owned::OwnedCommand,
        types::{Digest, GroupIdentity, Page, ParticipantPlan, RequestBinding, TxnId},
    },
    protocol::{self, ErrorCode, Key, Operation, ProtocolError, Row, TableDescriptor},
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};
use uuid::Uuid;
mod query;
pub(crate) type ResolveFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;
pub(crate) trait ParticipantResolver: Send + Sync {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>>;
    /// Must enroll/fence the deterministic new group and its required voters
    /// before returning. This does not publish its route in the catalog.
    fn create_data<'a>(
        &'a self,
        table: &'a TableDescriptor,
        pin: &'a PinnedCatalog,
    ) -> ResolveFuture<'a, ShardRoute>;
}
#[derive(Debug)]
pub(crate) enum PlanError {
    Protocol(ProtocolError),
    Blocked(IntentRef),
    LifecyclePending(LifecycleBinding),
    Guarded {
        cause: Box<PlanError>,
        guards: Vec<VerifiedLifecycleGuard>,
    },
}
impl From<ProtocolError> for PlanError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
pub(crate) enum PlannedOperation {
    Read(ChargedBytes),
    Mutation(MutationPlan),
}
pub(crate) struct MutationPlan {
    pub participants: Vec<ParticipantWrites>,
    pub lifecycle: Vec<VerifiedLifecycleGuard>,
    pub result: serde_json::Value,
    _owner: Arc<crate::oltp::pool::ScratchLease>,
}
pub(crate) struct ParticipantWrites {
    pub participant: Arc<ParticipantHandle>,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub writes: Vec<PreparedWrite>,
    pub guards: Vec<super::store::lifecycle::SealedLifecycleGuard>,
}
pub(crate) struct NativePlanner {
    catalog: CatalogHandle,
    resolver: Arc<dyn ParticipantResolver>,
}
impl NativePlanner {
    pub(crate) fn new(catalog: CatalogHandle, resolver: Arc<dyn ParticipantResolver>) -> Self {
        Self { catalog, resolver }
    }
    pub(crate) fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }
    pub(crate) async fn plan(
        &self,
        operation: Operation,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<PlannedOperation, PlanError> {
        let operation = self
            .cpu(owner.clone(), move || {
                operation.validate()?;
                Ok(operation)
            })
            .await?;
        let encoded_operation = self
            .cpu(owner.clone(), move || {
                let bytes =
                    crate::oltp::query::check_body(&operation, super::MAX_FIXED_COMMAND_BYTES)?;
                Ok((operation, bytes))
            })
            .await?;
        let (operation, encoded) = encoded_operation;
        let owner = self.catalog.resources().ensure_scratch_budget(
            owner,
            encoded
                .checked_mul(4)
                .and_then(|n| n.checked_add(64 << 10))
                .ok_or_else(not_found)?,
        )?;
        match operation {
            Operation::Create(payload) => {
                return self
                    .create(payload, pin, txn, request, owner.clone())
                    .await
                    .map(PlannedOperation::Mutation);
            }
            Operation::Describe(payload) => {
                let (_, table) = value(self.catalog.describe_name(pin, &payload.name).await?)?;
                let table = table.ok_or_else(not_found)?;
                return Ok(PlannedOperation::Read(
                    self.catalog
                        .resources()
                        .encode(&table.descriptor, protocol::MAX_BODY_BYTES - 128)?,
                ));
            }
            Operation::List(_) => return self.list(pin, owner).await.map(PlannedOperation::Read),
            _ => {}
        }
        let id = table_id(&operation).ok_or_else(not_found)?;
        let (version, record) = value(self.catalog.describe(pin, id).await?)?;
        let table = record.ok_or_else(not_found)?;
        let checked_table = table.clone();
        let operation = self
            .cpu(owner.clone(), move || {
                operation.validate_for_table(&checked_table.descriptor)?;
                Ok(operation)
            })
            .await?;
        if let Operation::Drop(_) = operation {
            return self
                .drop_table(pin, txn, request, table, version, owner.clone())
                .await
                .map(PlannedOperation::Mutation);
        }
        if let Operation::Get(payload) = operation {
            let (_, row) = self.row(pin, &table, &payload.key).await?;
            let shared = self.catalog.resources().clone();
            return Ok(PlannedOperation::Read(
                self.cpu(owner.clone(), move || match row {
                    Some(row) => shared.encode(&*row, protocol::MAX_BODY_BYTES - 128),
                    None => shared.copy_bytes(b"null"),
                })
                .await?,
            ));
        }
        match operation {
            Operation::Collect(p) => {
                return self
                    .query(pin, table, p.query, false, owner)
                    .await
                    .map(PlannedOperation::Read);
            }
            Operation::Count(p) => {
                return self
                    .query(pin, table, p.query, true, owner)
                    .await
                    .map(PlannedOperation::Read);
            }
            Operation::Explain(_p) => {
                let text = format!(
                    "engine oltp; replicated MVCC\nprimary index {}; pinned timestamp {}\nlookup point/range/stream; residual predicate exact\nmerge bounded top-k; primary-key ascending ties\nsecondary indexes require a published catalog definition",
                    table.descriptor.key,
                    pin.read_ts.get()
                );
                return Ok(PlannedOperation::Read(
                    self.catalog
                        .resources()
                        .encode(&text, protocol::MAX_BODY_BYTES - 128)?,
                ));
            }
            _ => {}
        }
        let guard = self
            .guard(
                pin,
                txn,
                request,
                id,
                version,
                table.schema_epoch,
                GuardMode::Shared,
            )
            .await?;
        let planned = async {
            let sealed = guard.clone().into_sealed();
            let mut grouped: BTreeMap<GroupIdentity, (Arc<ParticipantHandle>, Vec<PreparedWrite>)> =
                BTreeMap::new();
            let mut result = serde_json::Value::Null;
            let rows = match operation {
                Operation::Insert(p) => vec![(p.row, true)],
                Operation::Upsert(p) => vec![(p.row, false)],
                Operation::InsertMany(p) => {
                    result = serde_json::json!(p.rows.len());
                    p.rows.into_iter().map(|r| (r, true)).collect()
                }
                Operation::Update(p) => {
                    let (expected, row) = self.row(pin, &table, &p.key).await?;
                    let change = if let Some(row) = row {
                        let shared = self.catalog.resources().clone();
                        let values = p.values;
                        let canonical = self
                            .cpu(owner.clone(), move || {
                                let mut candidate = (*row).clone();
                                for (k, v) in values {
                                    candidate.insert(k, v);
                                }
                                CanonicalJson::from_serializable(
                                    &shared,
                                    &candidate,
                                    super::MAX_FIXED_COMMAND_BYTES,
                                )
                            })
                            .await?;
                        result = serde_json::Value::Bool(true);
                        Change::Put(canonical)
                    } else {
                        result = serde_json::Value::Bool(false);
                        Change::Keep
                    };
                    let (participant, group) = self.route(pin, id, &p.key).await?;
                    grouped
                        .entry(group)
                        .or_insert_with(|| (participant, Vec::new()))
                        .1
                        .push(PreparedWrite {
                            key: MvccKey::Row {
                                table_id: id,
                                primary_key: p.key,
                            },
                            expected_version: expected,
                            change,
                        });
                    Vec::new()
                }
                Operation::Delete(p) => {
                    let (expected, row) = self.row(pin, &table, &p.key).await?;
                    let found = row.is_some();
                    result = serde_json::Value::Bool(found);
                    let (participant, group) = self.route(pin, id, &p.key).await?;
                    grouped
                        .entry(group)
                        .or_insert_with(|| (participant, Vec::new()))
                        .1
                        .push(PreparedWrite {
                            key: MvccKey::Row {
                                table_id: id,
                                primary_key: p.key,
                            },
                            expected_version: expected,
                            change: if found { Change::Delete } else { Change::Keep },
                        });
                    Vec::new()
                }
                _ => {
                    return Err(
                        failure(ErrorCode::UnsupportedOperation, "invalid row mutation").into(),
                    );
                }
            };
            let mut seen = std::collections::BTreeSet::new();
            for (row, insert) in rows {
                let column = table.descriptor.key.clone();
                let shared = self.catalog.resources().clone();
                let (key, key_order, canonical) = self
                    .cpu(owner.clone(), move || {
                        let key = crate::oltp::codec::scalar_key(&row[&column]);
                        let order = key_sort(&key);
                        let canonical = CanonicalJson::from_serializable(
                            &shared,
                            &row,
                            super::MAX_FIXED_COMMAND_BYTES,
                        )?;
                        Ok((key, order, canonical))
                    })
                    .await?;
                if !seen.insert(key_order) {
                    return Err(
                        failure(ErrorCode::Conflict, "duplicate primary key in batch").into(),
                    );
                }
                let (expected, old) = self.row(pin, &table, &key).await?;
                if insert && old.is_some() {
                    return Err(failure(ErrorCode::Conflict, "primary key already exists").into());
                }
                let (participant, group) = self.route(pin, id, &key).await?;
                let change = Change::Put(canonical);
                grouped
                    .entry(group)
                    .or_insert_with(|| (participant, Vec::new()))
                    .1
                    .push(PreparedWrite {
                        key: MvccKey::Row {
                            table_id: id,
                            primary_key: key,
                        },
                        expected_version: expected,
                        change,
                    });
            }
            // Empty insert_many still validates and locks the captured table
            // dependency, then records its exact zero result through Status.
            if grouped.is_empty() {
                grouped.insert(
                    self.catalog.participant().identity().clone(),
                    (
                        self.catalog.participant().clone(),
                        vec![PreparedWrite {
                            key: MvccKey::CatalogTable { table_id: id },
                            expected_version: version,
                            change: Change::Keep,
                        }],
                    ),
                );
            }
            let catalog_epochs = self.catalog.participant().epochs().await?;
            let mut participants = Vec::new();
            for (_, (participant, mut writes)) in grouped {
                writes.sort_by(|a, b| super::store::compare_keys(&a.key, &b.key));
                let (schema_epoch, routing_epoch) = if matches!(
                    participant.identity().kind,
                    crate::oltp_replication::types::GroupKind::Catalog
                ) {
                    (catalog_epochs.schema_epoch, catalog_epochs.routing_epoch)
                } else {
                    (table.schema_epoch, table.routing_epoch)
                };
                participants.push(ParticipantWrites {
                    schema_epoch,
                    routing_epoch,
                    participant,
                    writes,
                    guards: vec![sealed.clone()],
                });
            }
            Ok(PlannedOperation::Mutation(MutationPlan {
                participants,
                lifecycle: vec![guard.clone()],
                result,
                _owner: owner.clone(),
            }))
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn create(
        &self,
        payload: protocol::CreatePayload,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let name = crate::oltp_replication::types::Text::try_from(payload.name.as_str())
            .map_err(|_| not_found())?;
        let (name_version, existing) = value(
            self.catalog
                .read::<NameRecord>(pin, MvccKey::CatalogName { name })
                .await?,
        )?;
        if existing.is_some() {
            return Err(failure(ErrorCode::Conflict, "table name already exists").into());
        }
        let id = derived_uuid(
            b"nabedra/table-id/v1",
            self.catalog.participant().identity().group_id,
            txn.request_id,
        );
        let descriptor = TableDescriptor {
            id,
            name: payload.name,
            schema: payload.schema,
            key: payload.key,
            format: payload.format,
        };
        descriptor.validate()?;
        let guard = self
            .guard(pin, txn, request, id, None, 1, GuardMode::Exclusive)
            .await?;
        let planned = async {
            let epochs = self.catalog.participant().epochs().await?;
            let route = self.resolver.create_data(&descriptor, pin).await?;
            let page = derived_uuid(b"nabedra/route-page/v1", id, txn.request_id);
            let mut writes = self
                .catalog
                .create_writes(descriptor.clone(), route, page, &guard)?;
            writes[0].expected_version = name_version;
            Ok(MutationPlan {
                participants: vec![ParticipantWrites {
                    participant: self.catalog.participant().clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    writes,
                    guards: vec![guard.clone().into_sealed()],
                }],
                lifecycle: vec![guard.clone()],
                result: serde_json::to_value(descriptor).map_err(|_| not_found())?,
                _owner: owner.clone(),
            })
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn drop_table(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table: OwnedRecord<TableRecord>,
        version: Option<crate::oltp_replication::types::CommitTs>,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let id = table.descriptor.id;
        let guard = self
            .guard(
                pin,
                txn,
                request,
                id,
                version,
                table.schema_epoch,
                GuardMode::Exclusive,
            )
            .await?;
        let planned = async {
            let epochs = self.catalog.participant().epochs().await?;
            let name =
                crate::oltp_replication::types::Text::try_from(table.descriptor.name.as_str())
                    .map_err(|_| not_found())?;
            let (name_version, name_record) = value(
                self.catalog
                    .read::<NameRecord>(pin, MvccKey::CatalogName { name: name.clone() })
                    .await?,
            )?;
            if name_record.is_none_or(|r| r.table_id != id) {
                return Err(not_found().into());
            }
            let (root_version, _) = value(
                self.catalog
                    .read::<RoutingRoot>(pin, MvccKey::CatalogRoutingRoot { table_id: id })
                    .await?,
            )?;
            let writes = vec![
                PreparedWrite {
                    key: MvccKey::CatalogName { name },
                    expected_version: name_version,
                    change: Change::Delete,
                },
                PreparedWrite {
                    key: MvccKey::CatalogTable { table_id: id },
                    expected_version: version,
                    change: Change::Delete,
                },
                PreparedWrite {
                    key: MvccKey::CatalogRoutingRoot { table_id: id },
                    expected_version: root_version,
                    change: Change::Delete,
                },
            ];
            Ok(MutationPlan {
                participants: vec![ParticipantWrites {
                    participant: self.catalog.participant().clone(),
                    schema_epoch: epochs.schema_epoch,
                    routing_epoch: epochs.routing_epoch,
                    writes,
                    guards: vec![guard.clone().into_sealed()],
                }],
                lifecycle: vec![guard.clone()],
                result: serde_json::Value::Null,
                _owner: owner.clone(),
            })
        }
        .await;
        planned.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn cpu<T: Send + 'static>(
        &self,
        owner: Arc<crate::oltp::pool::ScratchLease>,
        work: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
    ) -> Result<T, ProtocolError> {
        let shared = self.catalog.resources();
        let native = shared
            .prepare(crate::oltp::pool::ExecutionClass::Query)
            .await?;
        let cpu = shared
            .scheduler()
            .reserve_on(0, 1, shared.scheduler_node_id(), 0)
            .await?;
        let (result, _guard) = native.run_with_guard((owner, cpu), work).await?;
        result
    }
    async fn guard(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        table_id: Uuid,
        lifecycle_version: Option<crate::oltp_replication::types::CommitTs>,
        schema_epoch: u64,
        mode: GuardMode,
    ) -> Result<VerifiedLifecycleGuard, PlanError> {
        let current = self.catalog.participant().epochs().await?.authority;
        if current.run_id != pin.authority.run_id {
            return Err(failure(
                ErrorCode::StorageUnavailable,
                "pinned request belongs to an abandoned run",
            )
            .into());
        }
        let binding = LifecycleBinding {
            guard_id: derived_uuid(b"nabedra/lifecycle-guard/v1", table_id, txn.request_id),
            catalog_group_id: self.catalog.participant().identity().group_id,
            table_id,
            lifecycle_version,
            schema_epoch,
            txn,
            request,
            read_ts: pin.read_ts,
            authority: current,
        };
        self.catalog
            .participant()
            .acquire_lifecycle(binding.clone(), mode)
            .await?
            .ok_or(PlanError::LifecyclePending(binding))
    }
    async fn route(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        key: &Key,
    ) -> Result<(Arc<ParticipantHandle>, GroupIdentity), PlanError> {
        match self.catalog.route(pin, table_id, key).await? {
            RouteRead::Blocked(i) => Err(PlanError::Blocked(i)),
            RouteRead::Route { page, index } => {
                let group = page.shards[index].group.clone();
                Ok((self.resolver.participant(&group).await?, group))
            }
            RouteRead::Missing => Err(not_found().into()),
        }
    }
    async fn row(
        &self,
        pin: &PinnedCatalog,
        table: &TableRecord,
        key: &Key,
    ) -> Result<
        (
            Option<crate::oltp_replication::types::CommitTs>,
            Option<OwnedRecord<Row>>,
        ),
        PlanError,
    > {
        let (participant, _) = self.route(pin, table.descriptor.id, key).await?;
        value(
            decode_record(
                self.catalog.resources(),
                participant
                    .read_at(
                        pin.read_ts,
                        MvccKey::Row {
                            table_id: table.descriptor.id,
                            primary_key: key.clone(),
                        },
                    )
                    .await?,
            )
            .await?,
        )
    }
}
fn value<T>(
    read: CatalogRead<T>,
) -> Result<
    (
        Option<crate::oltp_replication::types::CommitTs>,
        Option<OwnedRecord<T>>,
    ),
    PlanError,
> {
    match read {
        CatalogRead::Value { version, record } => Ok((version, record)),
        CatalogRead::Blocked(i) => Err(PlanError::Blocked(i)),
    }
}
fn table_id(op: &Operation) -> Option<Uuid> {
    match op {
        Operation::Drop(p) => Some(p.table_id),
        Operation::Insert(p) | Operation::Upsert(p) => Some(p.table_id),
        Operation::InsertMany(p) => Some(p.table_id),
        Operation::Get(p) | Operation::Delete(p) => Some(p.table_id),
        Operation::Update(p) => Some(p.table_id),
        Operation::Collect(p) | Operation::Count(p) | Operation::Explain(p) => Some(p.table_id),
        _ => None,
    }
}
fn key_sort(key: &Key) -> Vec<u8> {
    crate::oltp::codec::Namespace(0).row(Uuid::nil(), key)
}
fn derived_uuid(domain: &[u8], namespace: Uuid, request: Uuid) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(namespace.as_bytes());
    hash.update(request.as_bytes());
    let digest = hash.finalize();
    Uuid::from_bytes(digest[..16].try_into().expect("fixed hash prefix"))
}
fn not_found() -> ProtocolError {
    failure(ErrorCode::NotFound, "table or shard not found")
}
