//! One Catalog/Status transaction publishes two completely imported children.
//! Existing MVCC routing pages and the source remain readable at old pins.
use super::*;
use crate::oltp::mvcc::{
    index::{IndexPage, IndexPhase, IndexRecord, IndexRoot},
    store::{
        KeySpan,
        lineage::{SplitBinding, VerifiedSplitImported, VerifiedSplitSource, child_digest},
        split_closure::{CatalogSplitClosed, MAX_SPLIT_CLOSURE_BYTES, SplitChildImage},
    },
};
use nabedra_oltp_types::types::{GroupKind, ReadTs, StoragePin};

pub struct PlannedSplit {
    split_id: Uuid,
    source: ShardRoute,
    pub children: [ShardRoute; 2],
    pub guard: VerifiedLifecycleGuard,
}
pub fn child_group(
    source: &GroupIdentity,
    split_id: Uuid,
    index: usize,
) -> Result<GroupIdentity, ProtocolError> {
    let GroupKind::Data { table_id, .. } = source.kind else {
        return Err(not_found());
    };
    let domain = [
        b"nabedra/split-left/v1".as_slice(),
        b"nabedra/split-right/v1".as_slice(),
    ]
    .get(index)
    .copied()
    .ok_or_else(not_found)?;
    let shard_id = derived_uuid(domain, source.group_id, split_id);
    Ok(GroupIdentity {
        group_id: derived_uuid(b"nabedra/split-data-group/v1", table_id, shard_id),
        kind: GroupKind::Data { table_id, shard_id },
        ..source.clone()
    })
}
impl PlannedSplit {
    pub fn bindings(&self, read_ts: ReadTs) -> Result<[SplitBinding; 2], ProtocolError> {
        if read_ts <= self.guard.binding().read_ts {
            return Err(failure(
                ErrorCode::Conflict,
                "split image cut must follow the Catalog guard",
            ));
        }
        Ok(std::array::from_fn(|index| SplitBinding {
            split_id: self.split_id,
            source: self.source.group.clone(),
            source_schema_epoch: self.source.schema_epoch,
            source_routing_epoch: self.source.routing_epoch,
            source_span: self.source.span.clone(),
            target: self.children[index].group.clone(),
            target_span: self.children[index].span.clone(),
            read_ts,
            guard: self.guard.clone().into_sealed(),
        }))
    }
}

impl NativePlanner {
    pub async fn begin_split(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        source: &GroupIdentity,
        boundary: Key,
        split_id: Uuid,
        expected_epoch: Option<crate::oltp::mvcc::store::split_usage::ShardEpoch>,
    ) -> Result<PlannedSplit, PlanError> {
        let GroupKind::Data { table_id, .. } = source.kind else {
            return Err(not_found().into());
        };
        if split_id.is_nil()
            || source.cluster_id != pin.catalog.cluster_id
            || source.database != pin.catalog.database
        {
            return Err(not_found().into());
        }
        let (table_version, table) = value(self.catalog.describe(pin, table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        boundary.validate(
            *table
                .descriptor
                .schema
                .get(&table.descriptor.key)
                .ok_or_else(not_found)?,
        )?;
        self.check_split_indexes(pin, table_id).await?;
        let (_, root) = value(self.catalog.routing_root(pin, table_id).await?)?;
        let root = root.ok_or_else(not_found)?;
        if root.schema_epoch != table.schema_epoch || root.routing_epoch != table.routing_epoch {
            return Err(failure(ErrorCode::Conflict, "split routing root changed").into());
        }
        // The interior boundary belongs to the source at the pinned epoch.
        let route = match self.catalog.route(pin, table_id, &boundary).await? {
            RouteRead::Route { page, index } => page.shards[index].clone(),
            RouteRead::Blocked(i) => return Err(PlanError::Blocked(i)),
            RouteRead::Missing => return Err(not_found().into()),
        };
        if route.group != *source
            || route.span.start.as_ref() == Some(&boundary)
            || route.schema_epoch != table.schema_epoch
            || route.routing_epoch > table.routing_epoch
            || expected_epoch.is_some_and(|epoch| {
                epoch.schema != route.schema_epoch || epoch.routing != route.routing_epoch
            })
        {
            return Err(failure(ErrorCode::Conflict, "split boundary or source changed").into());
        }
        let child_epoch = route.routing_epoch.checked_add(1).ok_or_else(not_found)?;
        let children = [
            ShardRoute {
                group: child_group(source, split_id, 0)?,
                span: KeySpan {
                    start: route.span.start.clone(),
                    end: Some(boundary.clone()),
                },
                schema_epoch: route.schema_epoch,
                routing_epoch: child_epoch,
            },
            ShardRoute {
                group: child_group(source, split_id, 1)?,
                span: KeySpan {
                    start: Some(boundary),
                    end: route.span.end.clone(),
                },
                schema_epoch: route.schema_epoch,
                routing_epoch: child_epoch,
            },
        ];
        let guard = self
            .guard(
                pin,
                txn,
                request,
                table_id,
                table_version,
                table.schema_epoch,
                GuardMode::Exclusive,
            )
            .await?;
        Ok(PlannedSplit {
            split_id,
            source: route,
            children,
            guard,
        })
    }

    pub async fn plan_publish_split(
        &self,
        pin: &PinnedCatalog,
        txn: TxnId,
        request: RequestBinding,
        guard: VerifiedLifecycleGuard,
        source: VerifiedSplitSource,
        left: VerifiedSplitImported,
        right: VerifiedSplitImported,
        image_pin: StoragePin,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<MutationPlan, PlanError> {
        let result = async {
            let a = left.statement();
            let b = right.statement();
            let source_state = source.statement();
            let binding = guard.binding();
            let table_id = binding.table_id;
            if guard.mode() != GuardMode::Exclusive
                || binding.txn != txn
                || binding.request != request
                || binding.authority != pin.authority
                || binding.read_ts != pin.read_ts
                || binding.catalog_group_id != pin.catalog.group_id
                || a.binding.source.cluster_id != pin.catalog.cluster_id
                || a.binding.source.database != pin.catalog.database
                || a.binding.guard != guard.clone().into_sealed()
                || b.binding.guard != a.binding.guard
                || source.published_at().is_some()
                || source_state.group != a.binding.source
                || source_state.group != b.binding.source
                || source_state.split_id != a.binding.split_id
                || source_state.split_id != b.binding.split_id
                || source_state.txn != txn
                || source_state.request != request
                || source_state.read_ts != a.binding.read_ts
                || source_state.read_ts != b.binding.read_ts
                || source_state.read_ts != image_pin.read_ts
                || source_state.children != [child_digest(&a.binding)?, child_digest(&b.binding)?]
                || a.source_root != b.source_root
                || a.pages != b.pages
                || a.source_images != b.source_images
                || a.images.checked_add(b.images) != Some(a.source_images)
                || a.binding.target == b.binding.target
                || a.binding.target_span.start != a.binding.source_span.start
                || b.binding.target_span.end != a.binding.source_span.end
                || a.binding.target_span.end.is_none()
                || a.binding.target_span.end != b.binding.target_span.start
            {
                return Err(failure(
                    ErrorCode::Conflict,
                    "split children do not close the exact source lineage",
                )
                .into());
            }
            let (table_version, table) = value(self.catalog.describe(pin, table_id).await?)?;
            let table = table.ok_or_else(not_found)?;
            if table_version != binding.lifecycle_version
                || table.schema_epoch != binding.schema_epoch
                || table.schema_epoch != a.binding.source_schema_epoch
                || table.routing_epoch < a.binding.source_routing_epoch
            {
                return Err(failure(ErrorCode::Conflict, "split Catalog source changed").into());
            }
            self.check_split_indexes(pin, table_id).await?;
            let (root_version, root) = value(self.catalog.routing_root(pin, table_id).await?)?;
            let root = root.ok_or_else(not_found)?;
            if root.schema_epoch != table.schema_epoch || root.routing_epoch != table.routing_epoch
            {
                return Err(failure(ErrorCode::Conflict, "split routing root changed").into());
            }
            let next_epoch = table.routing_epoch.checked_add(1).ok_or_else(not_found)?;
            let mut next = root.first_page;
            let mut selected = None;
            while let Some(page_id) = next {
                let (version, page) =
                    value(self.catalog.routing_page(pin, table_id, page_id).await?)?;
                let page = page.ok_or_else(not_found)?;
                if let Some(index) = page
                    .shards
                    .iter()
                    .position(|route| route.group == a.binding.source)
                {
                    let route = &page.shards[index];
                    if route.span != a.binding.source_span
                        || route.schema_epoch != a.binding.source_schema_epoch
                        || route.routing_epoch != a.binding.source_routing_epoch
                    {
                        return Err(
                            failure(ErrorCode::Conflict, "split source route changed").into()
                        );
                    }
                    selected = Some((page_id, version, page, index));
                    break;
                }
                next = page.next_page;
            }
            let (page_id, page_version, page, index) = selected.ok_or_else(not_found)?;
            let child_epoch = a
                .binding
                .source_routing_epoch
                .checked_add(1)
                .ok_or_else(not_found)?;
            let mut routes = Vec::with_capacity(page.shards.len() + 1);
            for (offset, route) in page.shards.iter().enumerate() {
                if offset != index {
                    routes.push(route.clone());
                    continue;
                }
                for child in [a, b] {
                    routes.push(ShardRoute {
                        group: child.binding.target.clone(),
                        span: child.binding.target_span.clone(),
                        schema_epoch: table.schema_epoch,
                        routing_epoch: child_epoch,
                    });
                }
            }
            let closure = CatalogSplitClosed {
                split_id: a.binding.split_id,
                guard: guard.clone().into_sealed(),
                source: a.binding.source.clone(),
                source_span: a.binding.source_span.clone(),
                source_schema_epoch: a.binding.source_schema_epoch,
                source_routing_epoch: a.binding.source_routing_epoch,
                source_frozen_at: source_state.frozen_at,
                source_pin: image_pin,
                source_root: a.source_root,
                source_pages: a.pages,
                source_images: a.source_images,
                published_routing_epoch: next_epoch,
                children: std::array::from_fn(|index| {
                    let child = [a, b][index];
                    SplitChildImage {
                        group: child.binding.target.clone(),
                        span: child.binding.target_span.clone(),
                        binding: source_state.children[index],
                        image_count: child.images,
                        image_root: child.image_root,
                        imported_at: child.completed_at,
                    }
                }),
                restored: None,
            };
            closure.validate(table_id, closure.split_id)?;
            let mut writes = Vec::with_capacity(5);
            writes.push(PreparedWrite {
                key: closure.key(),
                expected_version: None,
                change: Change::Put(CanonicalJson::from_serializable(
                    self.catalog.resources(),
                    &closure,
                    MAX_SPLIT_CLOSURE_BYTES,
                )?),
            });
            let new_next = if routes.len() > 128 {
                let mut hash = Sha256::new();
                hash.update(b"nabedra/split-routing-page/v1\0");
                hash.update(a.binding.split_id.as_bytes());
                let digest = hash.finalize();
                let overflow_id = Uuid::from_slice(&digest[..16]).map_err(|_| not_found())?;
                let key = MvccKey::CatalogRoutingPage {
                    table_id,
                    page_id: overflow_id,
                };
                let (version, old) =
                    value(self.catalog.read::<RoutingPage>(pin, key.clone()).await?)?;
                if old.is_some() || version.is_some() {
                    return Err(failure(
                        ErrorCode::Conflict,
                        "split routing overflow already exists",
                    )
                    .into());
                }
                let overflow = routes.split_off(64);
                writes.push(PreparedWrite {
                    key,
                    expected_version: None,
                    change: self.index_put(&RoutingPage {
                        shards: Page::new(overflow).map_err(|_| not_found())?,
                        next_page: page.next_page,
                    })?,
                });
                Some(overflow_id)
            } else {
                page.next_page
            };
            writes.push(PreparedWrite {
                key: MvccKey::CatalogRoutingPage { table_id, page_id },
                expected_version: page_version,
                change: self.index_put(&RoutingPage {
                    shards: Page::new(routes).map_err(|_| not_found())?,
                    next_page: new_next,
                })?,
            });
            writes.push(PreparedWrite {
                key: MvccKey::CatalogRoutingRoot { table_id },
                expected_version: root_version,
                change: self.index_put(&RoutingRoot {
                    first_page: root.first_page,
                    schema_epoch: table.schema_epoch,
                    routing_epoch: next_epoch,
                })?,
            });
            writes.push(PreparedWrite {
                key: MvccKey::CatalogTable { table_id },
                expected_version: table_version,
                change: self.index_put(&TableRecord {
                    descriptor: table.descriptor.clone(),
                    schema_epoch: table.schema_epoch,
                    routing_epoch: next_epoch,
                    lifecycle_id: super::super::catalog::lifecycle_anchor(
                        table_id,
                        table.schema_epoch,
                        table_version,
                        table.lifecycle_id,
                    )?,
                })?,
            });
            writes.sort_by(|a, b| super::super::store::compare_keys(&a.key, &b.key));
            let epochs = self.catalog.participant().epochs().await?;
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
                _owner: owner,
            })
        }
        .await;
        result.map_err(|cause| PlanError::Guarded {
            cause: Box::new(cause),
            guards: vec![guard],
        })
    }

    async fn check_split_indexes(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
    ) -> Result<(), PlanError> {
        let (_, indexes) = value(
            self.catalog
                .read::<IndexRoot>(pin, MvccKey::CatalogIndexesRoot { table_id })
                .await?,
        )?;
        let mut index_page = indexes.and_then(|root| root.first_page);
        while let Some(page_id) = index_page {
            let (_, page) = value(
                self.catalog
                    .read::<IndexPage>(pin, MvccKey::CatalogIndexesPage { table_id, page_id })
                    .await?,
            )?;
            let page = page.ok_or_else(not_found)?;
            for index_id in page.indexes.iter() {
                let (_, record) = value(
                    self.catalog
                        .read::<IndexRecord>(
                            pin,
                            MvccKey::CatalogIndex {
                                table_id,
                                index_id: *index_id,
                            },
                        )
                        .await?,
                )?;
                if record.is_some_and(|record| matches!(record.phase, IndexPhase::Building { .. }))
                {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "split waits for the active index build",
                    )
                    .into());
                }
            }
            index_page = page.next_page;
        }
        Ok(())
    }
}
