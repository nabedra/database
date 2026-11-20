use super::*;
use crate::oltp_deployment::inventory::WorkerPools;
use crate::oltp_replication::authority::control::PoolRefusal;

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
                            let refusal = pools
                                .refused(local.pool_id)
                                .map(|refused| PoolRefusal::classify(&refused));
                            tracing::error!(group = %record.identity.group_id, node = local.node_id,
                                pool = %local.pool_id, ?refusal, %error,
                                "replica on a refused pool is not started");
                            self.settle_damage(
                                GroupDamageReport {
                                    group: record.identity.clone(),
                                    replica: local.clone(),
                                    reason: GroupDamageReason::PoolRefused,
                                    first_bad_seq: None,
                                    refusal,
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
                                refusal: None,
                            },
                            Some(pool),
                        );
                        continue;
                    }
                    let started = match pool.retained_group(record.identity.clone()).await {
                        Ok(store) => {
                            self.manager
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
                                .await
                        }
                        Err(error) => Err(error),
                    };
                    let running = match started {
                        Ok(running) => running,
                        // NS14b: Raft's own reads while starting the replica can
                        // meet damage (a journal value, G3). kv quarantined the
                        // group: report it and keep hosting the others.
                        Err(error) => match pool.group_damage(record.identity.group_id) {
                            Some(damage) => {
                                tracing::error!(group = %record.identity.group_id,
                                    node = local.node_id, %error,
                                    "replica found damaged while starting; it is not hosted");
                                self.settle_damage(
                                    GroupDamageReport {
                                        group: record.identity.clone(),
                                        replica: local.clone(),
                                        reason: damage.cause.into(),
                                        first_bad_seq: damage.first_bad_seq,
                                        refusal: None,
                                    },
                                    Some(pool),
                                );
                                continue;
                            }
                            None => return Err(error),
                        },
                    };
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
                self.settle_unpublished_damage(pools).await?;
                return Ok(revision);
            }
            after = page.after;
        }
    }

    /// NS14b rule 4 after a restart: once Control published the placement
    /// without a damaged replica, the routes above no longer name it, so its
    /// quarantined bytes would never be released. Every quarantined group of a
    /// local pool is settled from Control's records of this worker's replicas.
    async fn settle_unpublished_damage(&self, pools: &WorkerPools) -> Result<(), ProtocolError> {
        for id in pools.pool_ids()? {
            let Some(pool) = pools.get(id)? else {
                continue;
            };
            for group_id in pool.quarantined_groups() {
                let records = match self.control.group_damage_of_group(group_id).await {
                    Ok(records) => records,
                    // An older Control or a closed gate: the group stays quarantined.
                    Err(error) => {
                        tracing::debug!(group = %group_id, %error, "damage records unavailable");
                        continue;
                    }
                };
                for record in records.into_iter().filter(|record| {
                    record.report.replica.worker_id == self.local.worker_id
                        && record.report.replica.pool_id == id
                }) {
                    self.settle_damage(record.report, Some(pool.clone()));
                }
            }
        }
        Ok(())
    }
}
