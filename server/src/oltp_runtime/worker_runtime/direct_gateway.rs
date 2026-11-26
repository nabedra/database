//! A worker as direct-path gateway (T6): its HTTP API routes eligible
//! operations to the range leader with T7's `DirectLeader` role, and its
//! application endpoint serves the Catalog read an API process fills its
//! topology cache from.
use super::*;
use crate::oltp_runtime::application::direct::{
    self, DirectGateway, DirectMode, DirectRouting, PointFill, TableFill, TopologySource, Voter,
    WireTransport,
};
use crate::{oltp_runtime::worker_endpoint::AuthorityReader, protocol::Key};

/// The gateway reads T7's switch, which the runtime installs after the
/// gateway exists: off until it is there.
struct WorkerRouting {
    runtime: Weak<WorkerRuntime>,
}
impl DirectRouting for WorkerRouting {
    fn mode(&self) -> DirectMode {
        self.runtime
            .upgrade()
            .and_then(|runtime| runtime.direct_switch().map(|switch| switch.mode()))
            .unwrap_or(DirectMode::Off)
    }
    fn vector(&self) -> bool {
        self.runtime
            .upgrade()
            .and_then(|runtime| runtime.direct_switch().map(|switch| switch.vector_enabled()))
            .unwrap_or(false)
    }
}

struct WorkerTopology {
    runtime: Weak<WorkerRuntime>,
}
impl TopologySource for WorkerTopology {
    fn point<'a>(
        &'a self,
        table_id: Uuid,
        key: Option<&'a Key>,
    ) -> EndpointFuture<'a, Option<PointFill>> {
        Box::pin(async move {
            let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
            let current = runtime
                .services
                .control
                .current(&runtime.services.scope)
                .await?;
            // The `DirectLeader` Catalog reader: a worker that is not the
            // queue owner cannot read through the owner's role (T6 concern
            // 3). Before activation (no reader yet) the owner path stays.
            match runtime
                .direct_switch()
                .and_then(|switch| switch.direct_catalog())
            {
                Some(catalog) => {
                    direct::catalog_point(&catalog, current.ready.view.authority(), table_id, key)
                        .await
                }
                None => {
                    runtime
                        .direct_point_fill(current.ready.view.authority(), table_id, key)
                        .await
                }
            }
        })
    }
    fn placement<'a>(
        &'a self,
        group: &'a GroupIdentity,
        refresh: bool,
    ) -> EndpointFuture<'a, Vec<Voter>> {
        Box::pin(async move {
            let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
            direct::control_placement(&runtime.services.control_client, group, refresh).await
        })
    }
}

impl WorkerRuntime {
    /// Builds this worker's gateway once the runtime exists.
    pub(super) fn install_direct_gateway(self: &Arc<Self>) {
        let gateway = DirectGateway::new(
            self.services.shared.clone(),
            self.services.scope.database.clone(),
            Arc::new(WorkerRouting {
                runtime: Arc::downgrade(self),
            }),
            Arc::new(WorkerTopology {
                runtime: Arc::downgrade(self),
            }),
            Arc::new(WireTransport::new(
                self.services.shared.clone(),
                self.services.manager.client(),
                self.services.control_client.clone(),
                crate::oltp_replication::authority::control::ServiceAction::DirectLeader,
            )),
        );
        let _ = self.direct_gateway.set(gateway);
    }
    pub(crate) fn direct_gateway_handle(&self) -> Option<&Arc<DirectGateway>> {
        self.direct_gateway.get()
    }
    pub(crate) async fn direct_point_fill(
        &self,
        authority: Authority,
        table_id: Uuid,
        key: Option<&Key>,
    ) -> Result<Option<PointFill>, ProtocolError> {
        direct::catalog_point(&self.services.catalog, authority, table_id, key).await
    }

    /// One Catalog read for a gateway's topology cache.
    pub(crate) async fn direct_table_fill(
        &self,
        authority: Authority,
        table_id: Uuid,
    ) -> Result<Option<TableFill>, ProtocolError> {
        direct::catalog_fill(&self.services.catalog, authority, table_id).await
    }
}

#[cfg(test)]
mod tests;
