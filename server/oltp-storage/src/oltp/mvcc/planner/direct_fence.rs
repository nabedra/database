//! The DDL step of plan section 7. Direct commits take no Catalog lifecycle
//! guard, so `table.drop` and every index publication first commit a
//! `DirectSchema` fence to each Data group of the table. A `DirectCommit`
//! computed from the previous descriptor then fails at apply with
//! `StaleEpoch`, and the leader refreshes its descriptor.
//!
//! No DDL path bumped a Data group's `schema_epoch` before publication when
//! this was written: the table keeps its creation epoch, the run `Fence`
//! cannot change it, and drop and index publication only rewrite Catalog
//! records. The fence therefore bumps a separate direct epoch, which leaves
//! the queued path's epoch checks unchanged.
use super::*;
use crate::oltp::mvcc::{
    catalog::CatalogRead,
    store::direct::{DirectSchema, DirectSchemaChange},
};
use std::sync::OnceLock;

/// Whether Control activated `data.direct.v1`. The DDL coordinator asks it
/// after acquiring the table's exclusive lifecycle guard; `false` must come
/// from a strong Control read, because a cached `false` could hide an
/// activation and let a direct writer keep a retired descriptor.
pub trait DirectActivationProbe: Send + Sync {
    fn active(&self) -> ResolveFuture<'_, bool>;
}

/// Shared by a planner and its scoped clones; set once at startup.
#[derive(Clone, Default)]
pub(crate) struct DirectProbeSlot(Arc<OnceLock<Arc<dyn DirectActivationProbe>>>);

impl NativePlanner {
    /// Installs the worker's `data.direct.v1` probe. Without one the DDL
    /// coordinator never fences (no direct commit can exist).
    pub fn set_direct_probe(
        &self,
        probe: Arc<dyn DirectActivationProbe>,
    ) -> Result<(), ProtocolError> {
        self.direct
            .0
            .set(probe)
            .map_err(|_| failure(ErrorCode::Conflict, "direct probe already installed"))
    }

    /// Commits the direct schema fence for `lifecycle` (the table record's
    /// next `lifecycle_id`, or `None` for a drop) to every Data group the
    /// table routes to at `pin`. Call it while holding the table's exclusive
    /// lifecycle guard and before the Catalog publication commits. Returns
    /// the number of groups fenced (0 while `data.direct.v1` is inactive).
    #[allow(clippy::result_large_err)] // `PlanError`, like the rest of the planner.
    pub(crate) async fn fence_direct_writers(
        &self,
        pin: &PinnedCatalog,
        table_id: Uuid,
        lifecycle: Option<Uuid>,
    ) -> Result<u64, PlanError> {
        let Some(probe) = self.direct.0.get() else {
            return Ok(0);
        };
        if !probe.active().await? {
            return Ok(0);
        }
        let change = DirectSchemaChange::Fence { lifecycle };
        let mut fenced = 0u64;
        let mut next = match self.catalog.routing_root(pin, table_id).await? {
            CatalogRead::Value {
                record: Some(root), ..
            } => root.first_page,
            CatalogRead::Value { record: None, .. } => None,
            CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
        };
        while let Some(page_id) = next {
            let page = match self.catalog.routing_page(pin, table_id, page_id).await? {
                CatalogRead::Value {
                    record: Some(page), ..
                } => page,
                CatalogRead::Value { record: None, .. } => return Err(not_found().into()),
                CatalogRead::Blocked(intent) => return Err(PlanError::Blocked(intent)),
            };
            for route in page.shards.iter() {
                let participant = self.resolver.participant(&route.group).await?;
                let schema: DirectSchema = participant.change_direct_schema(change).await?;
                if schema.lifecycle != lifecycle {
                    return Err(failure(
                        ErrorCode::Conflict,
                        "direct schema fence was overtaken by another DDL",
                    )
                    .into());
                }
                fenced += 1;
            }
            next = page.next_page;
        }
        Ok(fenced)
    }
}
