//! Pinned point routes for the scoped queue prototype.
use super::*;
use nabedra_oltp_types::types::{Authority, GroupKind, ReadTs};

/// Owns one charged catalog page. Placement selection can borrow the selected
/// range, including long UTF-8 bounds, without allocating a second copy.
pub(crate) struct PointRoute {
    page: OwnedRecord<RoutingPage>,
    index: usize,
    read_ts: ReadTs,
    authority: Authority,
}

impl PointRoute {
    pub(crate) fn shard(&self) -> &ShardRoute {
        &self.page.shards[self.index]
    }

    pub(crate) fn read_ts(&self) -> ReadTs {
        self.read_ts
    }

    pub(crate) fn authority(&self) -> Authority {
        self.authority
    }
}

impl NativePlanner {
    /// Resolve Get and Update at their original timestamp. The full planner
    /// still validates the operation, guards, index effects and participant
    /// fences before execution.
    pub(crate) async fn point_route(
        &self,
        pin: &PinnedCatalog,
        operation: &Operation,
    ) -> Result<Option<PointRoute>, PlanError> {
        let (table_id, key) = match operation {
            Operation::Get(payload) => (payload.table_id, &payload.key),
            Operation::Update(payload) => (payload.table_id, &payload.key),
            _ => return Ok(None),
        };
        let (_, table) = value(self.catalog.describe(pin, table_id).await?)?;
        let table = table.ok_or_else(not_found)?;
        let key_type = table
            .descriptor
            .schema
            .get(&table.descriptor.key)
            .copied()
            .ok_or_else(invalid_route)?;
        key.validate(key_type)?;

        match self.catalog.route(pin, table_id, key).await? {
            RouteRead::Blocked(intent) => Err(PlanError::Blocked(intent)),
            RouteRead::Missing => Err(not_found().into()),
            RouteRead::Route { page, index } => {
                let route = &page.shards[index];
                if table.descriptor.id != table_id
                    || route.group.validate().is_err()
                    || route.group.cluster_id != pin.catalog.cluster_id
                    || route.group.database != pin.catalog.database
                    || !matches!(route.group.kind, GroupKind::Data { table_id: id, .. } if id == table_id)
                    || route.schema_epoch != table.schema_epoch
                    || route.routing_epoch == 0
                    || route.routing_epoch > table.routing_epoch
                {
                    return Err(invalid_route().into());
                }
                for bound in [route.span.start.as_ref(), route.span.end.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    bound.validate(key_type).map_err(|_| invalid_route())?;
                }
                Ok(Some(PointRoute {
                    page,
                    index,
                    read_ts: pin.read_ts,
                    authority: pin.authority,
                }))
            }
        }
    }
}

fn invalid_route() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "point route differs from its pinned catalog",
    )
}
