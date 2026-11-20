//! A worker as direct-path gateway (T6): its HTTP API routes eligible
//! operations to the range leader with the `DirectGateway` role, and its
//! application endpoint serves the Catalog read an API process fills its
//! topology cache from.
use super::*;
use crate::oltp_runtime::application::direct::{
    self, DirectGateway, DirectMode, DirectSwitch, TableFill, TopologySource, Voter, WireTransport,
};
use crate::oltp_runtime::worker_endpoint::AuthorityReader;

/// `data.direct.v1` from the worker flag T7 sets on activation, and the
/// `direct_oltp` operator switch (on by default).
pub(crate) struct WorkerSwitch {
    runtime: Weak<WorkerRuntime>,
    direct_oltp: AtomicBool,
}
impl WorkerSwitch {
    #[allow(dead_code)] // T7 wires the `direct_oltp` setting.
    pub(crate) fn set_direct_oltp(&self, enabled: bool) {
        self.direct_oltp.store(enabled, Ordering::Release);
    }
}
impl DirectSwitch for WorkerSwitch {
    fn mode(&self) -> DirectMode {
        let active = self
            .runtime
            .upgrade()
            .is_some_and(|runtime| runtime.direct_commits_active());
        direct::mode(active, self.direct_oltp.load(Ordering::Acquire))
    }
}

struct WorkerTopology {
    runtime: Weak<WorkerRuntime>,
}
impl TopologySource for WorkerTopology {
    fn table(&self, table_id: Uuid) -> EndpointFuture<'_, Option<TableFill>> {
        Box::pin(async move {
            let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
            let current = runtime
                .services
                .control
                .current(&runtime.services.scope)
                .await?;
            runtime
                .direct_table_fill(current.ready.view.authority(), table_id)
                .await
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
        let switch = Arc::new(WorkerSwitch {
            runtime: Arc::downgrade(self),
            direct_oltp: AtomicBool::new(true),
        });
        let gateway = DirectGateway::new(
            self.services.shared.clone(),
            self.services.scope.database.clone(),
            switch.clone(),
            Arc::new(WorkerTopology {
                runtime: Arc::downgrade(self),
            }),
            Arc::new(WireTransport::new(
                self.services.shared.clone(),
                self.services.manager.client(),
                self.services.control_client.clone(),
                crate::oltp_replication::authority::control::ServiceAction::DirectGateway,
            )),
        );
        let _ = self.direct_gateway.set((gateway, switch));
    }
    pub(crate) fn direct_gateway_handle(&self) -> Option<&Arc<DirectGateway>> {
        self.direct_gateway.get().map(|(gateway, _)| gateway)
    }
    /// The `direct_oltp` switch of this worker's gateway (T7 wires it).
    #[allow(dead_code)]
    pub(crate) fn direct_gateway_switch(&self) -> Option<&Arc<WorkerSwitch>> {
        self.direct_gateway.get().map(|(_, switch)| switch)
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
