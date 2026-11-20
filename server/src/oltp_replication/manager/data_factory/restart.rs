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
                    // Section 3 rules 1, 2 and 6: a replica on a refused pool or
                    // of a quarantined group is not started. It is reported to
                    // Control, and the worker keeps hosting everything else.
                    let pool = match pools.retained_for_group(local.pool_id, &self.control).await {
                        Ok(pool) => pool,
                        Err(error) if pools.refused(local.pool_id).is_some() => {
                            tracing::error!(group = %record.identity.group_id, node = local.node_id,
                                %error, "replica on a refused pool is not started");
                            self.settle_damage(
                                GroupDamageReport {
                                    group: record.identity.clone(),
                                    replica: local.clone(),
                                    reason: GroupDamageReason::PoolRefused,
                                    first_bad_seq: None,
                                },
                                None,
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    if let Some(damage) = pool.group_damage(record.identity.group_id) {
                        self.settle_damage(
                            GroupDamageReport {
                                group: record.identity.clone(),
                                replica: local.clone(),
                                reason: damage.cause.into(),
                                first_bad_seq: damage.first_bad_seq,
                            },
                            Some(pool),
                        );
                        continue;
                    }
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
