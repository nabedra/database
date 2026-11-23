//! Admin-token protected metadata commands. Mounted completion reads a live
//! locally owned registered pool; request JSON cannot contain a storage proof.
use super::*;
use crate::{
    oltp::pool::mount::PoolMountExpectation,
    oltp_deployment::{
        controller::{GrowthIntent, GrowthPhase, GrowthPolicy},
        inventory::SecondaryPoolConfig,
        live::{InventoryPin, UsageSample, process_counters},
        planner::Action,
    },
    oltp_replication::authority::control::{ControlReply, DataWorkerIntent},
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    intent: GrowthIntent,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AutomaticClaim {
    intent: GrowthIntent,
    pin: InventoryPin,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacitySample {
    pin: InventoryPin,
    worker: WorkerIdentity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerPage {
    pin: InventoryPin,
    after: Option<Uuid>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configure {
    policy: GrowthPolicy,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    claim_id: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mounted {
    claim_id: Uuid,
    #[serde(default)]
    mount: Option<PoolMountExpectation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    claim_id: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Provisioned {
    claim_id: Uuid,
    resource_id: String,
}
fn parse<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, ProtocolError> {
    serde_json::from_str(raw).map_err(|_| invalid_growth())
}
fn bounds_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "automatic growth inventory unavailable",
        Outcome::NotApplied,
    )
}
impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_growth_admin(
        operation: &str,
        raw: &str,
    ) -> Result<bool, ProtocolError> {
        match operation {
            "storage.growth.configure" => {
                let c: Configure = parse(raw)?;
                if !c.policy.valid() {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.claim" => {
                let c: Claim = parse(raw)?;
                if !c.intent.valid() {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.claim_automatic" => {
                let c: AutomaticClaim = parse(raw)?;
                if !c.intent.valid() || c.pin.policy_id != c.intent.policy_id {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.sample" => {
                let c: CapacitySample = parse(raw)?;
                c.worker.validate().map_err(|_| invalid_growth())?;
            }
            "storage.growth.workers" => {
                let c: WorkerPage = parse(raw)?;
                if c.after.is_some_and(|id| id.is_nil()) {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.snapshot" if raw.trim() == "{}" => {}
            "storage.growth.attempt" => {
                let c: Attempt = parse(raw)?;
                if c.claim_id.is_nil() {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.provisioned" => {
                let c: Provisioned = parse(raw)?;
                if c.claim_id.is_nil() || c.resource_id.is_empty() || c.resource_id.len() > 512 {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.status" => {
                let c: Identity = parse(raw)?;
                if c.claim_id.is_nil() {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.mounted" => {
                let c: Mounted = parse(raw)?;
                if c.claim_id.is_nil()
                    || c.mount
                        .as_ref()
                        .is_some_and(|mount| mount.validate().is_err())
                {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.prepare_worker" => {
                let intent: DataWorkerIntent = parse(raw)?;
                if intent.claim_id.is_nil() || intent.run_id.is_nil() {
                    return Err(invalid_growth());
                }
            }
            "storage.growth.inventory" if raw.trim() == "{}" => {}
            _ => return Ok(false),
        }
        Ok(true)
    }
    pub(in crate::oltp_runtime) async fn growth_admin(
        &self,
        operation_id: Uuid,
        operation: &str,
        raw: &str,
    ) -> Result<EncodedResponse, ProtocolError> {
        let control = &self.services.control;
        if operation == "storage.growth.snapshot" {
            let inventory = control.growth_inventory().await?;
            return Ok(response::encode_success(operation_id, &inventory, false));
        }
        if operation == "storage.growth.workers" {
            let c: WorkerPage = parse(raw)?;
            let page = control.growth_workers(c.pin, c.after).await?;
            return Ok(response::encode_success(operation_id, &page, false));
        }
        if operation == "storage.growth.sample" {
            let c: CapacitySample = parse(raw)?;
            if c.worker != self.services.local || c.pin != control.growth_inventory().await?.pin {
                return Err(bounds_unavailable());
            }
            let inventory = self.services.pools.observe(control).await?;
            let shared = &self.services.shared;
            let (monotonic_ns, cpu_time_ns, read_bytes, write_bytes) = shared
                .execute(ExecutionClass::Control, 0, 0, || {
                    process_counters().ok_or_else(bounds_unavailable)
                })
                .await?;
            let (allocated_query_threads, charged_memory_bytes, limits) = shared
                .scheduler()
                .oltp_capacity(shared.scheduler_node_id())
                .ok_or_else(bounds_unavailable)?;
            let (resident_group_count, resident_group_limit) = shared.resident_group_usage();
            let (queued_count, oldest_queued) = self
                .services
                .replica
                .lock()
                .map_err(|_| bounds_unavailable())?
                .growth_queue_observation()
                .ok_or_else(bounds_unavailable)?;
            let (split_deficit, replica_move) = self.split_growth_observations(c.pin.ready).await?;
            if c.pin != control.growth_inventory().await?.pin {
                return Err(bounds_unavailable());
            }
            let sample = UsageSample {
                monotonic_ns,
                cpu_time_ns,
                read_bytes,
                write_bytes,
                allocated_query_threads,
                query_thread_limit: limits.max_threads,
                charged_memory_bytes,
                memory_limit_bytes: limits.memory_limit_bytes,
                resident_group_count,
                resident_group_limit,
                queued_count,
                oldest_queued,
            };
            let mut data =
                serde_json::json!({"pin": c.pin, "inventory": inventory, "usage": sample});
            if let Some(split_deficit) = split_deficit {
                data["split_deficit"] = serde_json::json!(split_deficit);
            }
            if let Some(replica_move) = replica_move {
                data["split_replica_move"] = serde_json::json!(replica_move);
            }
            return Ok(response::encode_success(operation_id, &data, false));
        }
        if operation == "storage.growth.inventory" {
            let inventory = self.services.pools.observe(&self.services.control).await?;
            return Ok(response::encode_success(operation_id, &inventory, false));
        }
        if operation == "storage.growth.status" {
            let c: Identity = parse(raw)?;
            let record = self
                .services
                .control
                .growth(c.claim_id)
                .await?
                .ok_or_else(|| {
                    ProtocolError::new(
                        ErrorCode::NotFound,
                        "growth claim does not exist",
                        Outcome::NotApplied,
                    )
                })?;
            return Ok(response::encode_success(operation_id, &record, false));
        }
        let permit = &self.services.operator;
        let applied = match operation {
            "storage.growth.configure" => {
                let c: Configure = parse(raw)?;
                control
                    .configure_growth_local(permit, operation_id, c.policy)
                    .await?
            }
            "storage.growth.claim" | "storage.growth.claim_automatic" => {
                let (intent, pin) = if operation == "storage.growth.claim_automatic" {
                    let c: AutomaticClaim = parse(raw)?;
                    (c.intent, Some(c.pin))
                } else {
                    let c: Claim = parse(raw)?;
                    (c.intent, None)
                };
                let pools = &self.services.pools;
                let mut pool = pools.get(intent.pool_id)?;
                let stored = control.growth(intent.claim_id).await?;
                if let (Some(pin), None) = (pin, stored.as_ref()) {
                    let snapshot = control.growth_inventory().await?;
                    let auto = snapshot
                        .policy
                        .automatic
                        .as_ref()
                        .ok_or_else(bounds_unavailable)?;
                    if pin != snapshot.pin
                        || (!matches!(intent.action, Action::AddWorker { .. })
                            && intent.worker != self.services.local)
                    {
                        return Err(bounds_unavailable());
                    }
                    let now = nabedra_rt::time::wall_now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| bounds_unavailable())?
                        .as_secs();
                    if snapshot.last_attempt.is_some_and(|last| {
                        now.checked_sub(last)
                            .is_none_or(|age| age < intent.cooldown_seconds)
                    }) {
                        return Err(bounds_unavailable());
                    }
                    if matches!(intent.action, Action::AddPool { .. }) {
                        let (open, limit) = pools.open_pool_counts()?;
                        if open >= limit.min(auto.max_pools_per_worker) {
                            return Err(bounds_unavailable());
                        }
                    }
                }
                match (&intent.action, stored.as_ref()) {
                    (Action::AddPool { .. }, old)
                        if pool.is_some() && old.is_none_or(|record| record.intent != intent) =>
                    {
                        return Err(ProtocolError::new(
                            ErrorCode::Conflict,
                            "growth cannot reuse a registered pool identity",
                            Outcome::NotApplied,
                        ));
                    }
                    (Action::Expand { .. }, None) => {
                        pool = Some(
                            pools
                                .eligible(
                                    intent.pool_id,
                                    intent.worker,
                                    intent.pool_generation,
                                    control,
                                )
                                .await?,
                        );
                    }
                    _ => {}
                }
                control
                    .claim_growth_local(permit, operation_id, intent, pool.as_ref(), pin)
                    .await?
            }
            "storage.growth.attempt" => {
                let c: Attempt = parse(raw)?;
                control
                    .attempt_growth_local(permit, operation_id, c.claim_id)
                    .await?
            }
            "storage.growth.provisioned" => {
                let c: Provisioned = parse(raw)?;
                control
                    .provisioned_growth_local(permit, operation_id, c.claim_id, c.resource_id)
                    .await?
            }
            "storage.growth.mounted" => {
                let c: Mounted = parse(raw)?;
                let record = control
                    .growth(c.claim_id)
                    .await?
                    .ok_or_else(invalid_growth)?;
                if let Some(mount) = c.mount {
                    if !matches!(record.intent.action, Action::AddPool { .. })
                        || !matches!(
                            record.phase,
                            GrowthPhase::AwaitingMount | GrowthPhase::Complete
                        )
                        || mount.identity.pool_id != record.intent.pool_id
                        || mount.worker != record.intent.worker
                        || mount.worker != self.services.local
                        || mount.generation != record.intent.pool_generation
                        || record.provider_resource.as_deref()
                            != Some(mount.provider_volume_id.as_str())
                        || !control.growth_worker_ready(mount.worker).await?
                    {
                        return Err(ProtocolError::new(
                            ErrorCode::StorageUnavailable,
                            "mount does not match the authoritative growth claim and ready worker",
                            Outcome::NotApplied,
                        ));
                    }
                    self.services
                        .pools
                        .register_growth(SecondaryPoolConfig {
                            mount,
                            growth_claim: c.claim_id,
                        }, control)
                        .await?;
                }
                let pool = self.services.pools.growth_target(&record)?;
                control
                    .mounted_growth_local(permit, operation_id, c.claim_id, &pool)
                    .await?
            }
            "storage.growth.prepare_worker" => {
                let intent: DataWorkerIntent = parse(raw)?;
                intent.validate(control.identity())?;
                control.prepare_data_worker(permit, operation_id, intent).await?
            }
            _ => return Err(invalid_growth()),
        };
        let (_, reply, _owner) = applied.into_parts()?;
        match reply {
            ControlReply::GrowthConfigured(policy) => {
                Ok(response::encode_success(operation_id, &policy, true))
            }
            ControlReply::Growth(record) => {
                Ok(response::encode_success(operation_id, &record, true))
            }
            ControlReply::DataWorkerPrepared(identity) => {
                self.services.refresh_enrollment().await?;
                Ok(response::encode_success(
                    operation_id, &serde_json::json!({"identity": identity}), true,
                ))
            }
            ControlReply::Rejected(fault) => Err(ProtocolError::new(
                ErrorCode::Conflict,
                format!("growth claim refused: {fault:?}"),
                Outcome::NotApplied,
            )),
            _ => Err(unknown()),
        }
    }
}
fn invalid_growth() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid metadata-only storage growth command",
        Outcome::NotApplied,
    )
}
