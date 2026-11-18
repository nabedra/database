use super::*;
use crate::oltp_deployment::inventory::WorkerPools;

impl DataGroupFactory {
    pub(crate) async fn rehost_retained(&self, pools: &WorkerPools) -> Result<u64, ProtocolError> {
        let revision = self
            .control
            .inspect()
            .await?
            .ok_or_else(unavailable)?
            .group_inventory_revision;
        let mut after = None;
        loop {
            let raw = self.control.groups(revision, after).await?;
            let bytes = crate::oltp::mvcc::json_decode_budget(raw.as_slice())?;
            let _owner = self
                .manager
                .resources()
                .reserve_scratch(ByteClass::Progress, bytes)?;
            let page: InventoryPage<GroupRecord> =
                serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
            if page.revision != revision || (!page.done && page.after <= after) {
                return Err(invalid());
            }
            for record in page
                .items
                .iter()
                .filter(|record| matches!(record.identity.kind, GroupKind::Data { .. }))
            {
                let routes = self.rpc.replica_routes(&record.identity).await?;
                if routes.record != *record {
                    return Err(unavailable());
                }
                for local in routes
                    .members
                    .iter()
                    .filter(|member| member.worker_id == self.local.worker_id)
                {
                    let pool = pools.retained_for_group(local.pool_id, &self.control).await?;
                    let store = pool.retained_group(record.identity.clone()).await?;
                    let running = self
                        .manager
                        .start_data(
                            store,
                            local.clone(),
                            routes
                                .members
                                .iter()
                                .map(|member| (member.node_id, member.clone()))
                                .collect(),
                            self.config.clone(),
                        )
                        .await?;
                    if !running
                        .raft
                        .is_initialized()
                        .await
                        .map_err(|_| unavailable())?
                    {
                        return Err(invalid());
                    }
                    self.wire_participant(&record.identity, local.node_id, running)?;
                }
            }
            if page.done {
                return Ok(revision);
            }
            after = page.after;
        }
    }
}
