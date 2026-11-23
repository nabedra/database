use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

const MAX_WORKERS: usize = 4096;
const MAX_POOLS: usize = 64;
const MAX_ZONES: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub zones: Vec<String>,
    pub max_workers: usize,
    pub max_pools_per_worker: usize,
    pub max_volume_bytes: u64,
    pub pool_increment_bytes: u64,
    pub settlement_reserve_bytes: u64,
    pub compaction_reserve_bytes: u64,
    pub provisioning_seconds: u64,
    pub observation_max_age_seconds: u64,
    pub cooldown_seconds: u64,
    pub max_attempts: u32,
    /// Explicit operator price quote, in the same units as the spend ceiling.
    pub volume_cost_per_gib: u64,
    pub worker_cost: u64,
    pub monthly_spend_ceiling: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pool {
    pub pool_id: Uuid,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub reserved_bytes: u64,
    pub write_bytes_per_second: u64,
    pub expandable: bool,
    pub healthy: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub worker_id: Uuid,
    pub zone: String,
    pub observed_at: u64,
    pub ready: bool,
    pub attachment_slots_free: u32,
    /// Sustained measurements and configured ceilings, in the same units.
    pub io_used: u64,
    pub io_limit: u64,
    pub cpu_used: u64,
    pub cpu_limit: u64,
    pub memory_used: u64,
    pub memory_limit: u64,
    pub resident_shards: u64,
    pub resident_shard_limit: u64,
    pub pools: Vec<Pool>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Expand {
        worker: Uuid,
        pool: Uuid,
        target_bytes: u64,
    },
    AddPool {
        worker: Uuid,
        zone: String,
        bytes: u64,
    },
    AddWorker {
        zone: String,
        bytes: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub actions: Vec<Action>,
    pub action_monthly_costs: Vec<u64>,
    pub additional_monthly_cost: u64,
}
/// A demand computed from the installed voters of one RF3 source generation.
/// The worker and pool identify the only in-place expansion that helps it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SplitPoolNeed {
    pub worker_id: Uuid,
    pub pool_id: Uuid,
    pub zone: String,
    pub required_bytes: u64,
    pub replacement_bytes: u64,
    pub can_move_to_local_pool: bool,
    pub can_move_to_new_worker: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Refusal {
    InvalidInventory,
    StaleInventory,
    Unsupported,
    Cooldown,
    RetryLimit,
    InfrastructureLimit,
    PlacementUnavailable,
    SpendLimit,
    Overflow,
}

fn add(a: u64, b: u64) -> Result<u64, Refusal> {
    a.checked_add(b).ok_or(Refusal::Overflow)
}
fn mul(a: u64, b: u64) -> Result<u64, Refusal> {
    a.checked_mul(b).ok_or(Refusal::Overflow)
}
fn storage_cost(bytes: u64, price: u64) -> Result<u64, Refusal> {
    mul(add(bytes, (1 << 30) - 1)? >> 30, price)
}
fn host_headroom(worker: &Worker) -> bool {
    worker.io_used < worker.io_limit
        && worker.cpu_used < worker.cpu_limit
        && worker.memory_used < worker.memory_limit
        && worker.resident_shards < worker.resident_shard_limit
}
/// The inventory must come from authenticated mounted-pool and worker reports.
/// A plan only requests infrastructure. It does not publish usable capacity.
pub fn plan_growth(
    limits: &Limits,
    workers: &[Worker],
    now: u64,
    last_attempt: Option<u64>,
    attempts: u32,
    monthly_spend: u64,
    provider_available: bool,
) -> Result<Plan, Refusal> {
    plan_candidates(
        limits,
        workers,
        workers.len(),
        now,
        last_attempt,
        attempts,
        monthly_spend,
        provider_available,
        &[],
    )
}
fn plan_candidates(
    limits: &Limits,
    workers: &[Worker],
    actual_worker_count: usize,
    now: u64,
    last_attempt: Option<u64>,
    attempts: u32,
    monthly_spend: u64,
    provider_available: bool,
    unavailable_worker_zones: &[String],
) -> Result<Plan, Refusal> {
    if !provider_available {
        return Err(Refusal::Unsupported);
    }
    if limits.zones.is_empty()
        || limits.zones.len() > MAX_ZONES
        || limits.zones.iter().collect::<BTreeSet<_>>().len() != limits.zones.len()
        || limits.zones.iter().any(|s| s.is_empty() || s.len() > 128)
        || limits.max_workers == 0
        || limits.max_pools_per_worker == 0
        || limits.max_pools_per_worker > MAX_POOLS
        || limits.pool_increment_bytes == 0
        || limits.max_volume_bytes < limits.pool_increment_bytes
        || limits.max_attempts == 0
        || limits.provisioning_seconds == 0
        || limits.observation_max_age_seconds == 0
        || actual_worker_count > limits.max_workers
        || workers.len() > actual_worker_count
        || workers.len() > MAX_WORKERS
    {
        return Err(Refusal::InvalidInventory);
    }
    if attempts >= limits.max_attempts {
        return Err(Refusal::RetryLimit);
    }
    if last_attempt.is_some_and(|last| now < last || now - last < limits.cooldown_seconds) {
        return Err(Refusal::Cooldown);
    }
    let mut worker_ids = BTreeSet::new();
    let mut pool_ids = BTreeSet::new();
    for worker in workers {
        if worker.worker_id.is_nil()
            || !worker_ids.insert(worker.worker_id)
            || !limits.zones.contains(&worker.zone)
            || worker.pools.len() > limits.max_pools_per_worker
            || worker.io_limit == 0
            || worker.cpu_limit == 0
            || worker.memory_limit == 0
            || worker.resident_shard_limit == 0
        {
            return Err(Refusal::InvalidInventory);
        }
        if worker.observed_at > now || now - worker.observed_at > limits.observation_max_age_seconds
        {
            return Err(Refusal::StaleInventory);
        }
        for pool in &worker.pools {
            if pool.pool_id.is_nil()
                || !pool_ids.insert(pool.pool_id)
                || pool.total_bytes == 0
                || pool.free_bytes > pool.total_bytes
                || pool.reserved_bytes > pool.free_bytes
            {
                return Err(Refusal::InvalidInventory);
            }
        }
    }
    let reserve = add(
        limits.settlement_reserve_bytes,
        limits.compaction_reserve_bytes,
    )?;
    let mut result = Plan {
        actions: Vec::new(),
        action_monthly_costs: Vec::new(),
        additional_monthly_cost: 0,
    };
    let mut new_workers = 0;
    // Every required replica zone receives headroom before new replicated data
    // can be assigned. Two pools on one worker do not create two failure domains.
    for zone in &limits.zones {
        let mut candidates = workers
            .iter()
            .filter(|w| w.ready && &w.zone == zone)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|w| w.worker_id);
        let mut sufficient = false;
        for worker in &candidates {
            if !host_headroom(worker) {
                continue;
            }
            for pool in worker.pools.iter().filter(|p| p.healthy) {
                let required = add(
                    reserve,
                    mul(pool.write_bytes_per_second, limits.provisioning_seconds)?,
                )?;
                if pool.free_bytes - pool.reserved_bytes >= required {
                    sufficient = true;
                }
            }
        }
        if sufficient {
            continue;
        }
        let mut choice = None;
        for worker in &candidates {
            if !host_headroom(worker) {
                continue;
            }
            let mut pools = worker
                .pools
                .iter()
                .filter(|p| p.healthy && p.expandable)
                .collect::<Vec<_>>();
            pools.sort_by_key(|p| p.pool_id);
            for pool in pools {
                let required = add(
                    reserve,
                    mul(pool.write_bytes_per_second, limits.provisioning_seconds)?,
                )?;
                let deficit = required.saturating_sub(pool.free_bytes - pool.reserved_bytes);
                let increment = limits.pool_increment_bytes.max(deficit);
                let target = add(pool.total_bytes, increment)?;
                if target <= limits.max_volume_bytes {
                    choice = Some((
                        Action::Expand {
                            worker: worker.worker_id,
                            pool: pool.pool_id,
                            target_bytes: target,
                        },
                        storage_cost(increment, limits.volume_cost_per_gib)?,
                    ));
                    break;
                }
            }
            if choice.is_some() {
                break;
            }
            if worker.attachment_slots_free > 0 && worker.pools.len() < limits.max_pools_per_worker
            {
                let bytes = limits.pool_increment_bytes.max(reserve);
                if bytes <= limits.max_volume_bytes {
                    choice = Some((
                        Action::AddPool {
                            worker: worker.worker_id,
                            zone: zone.clone(),
                            bytes,
                        },
                        storage_cost(bytes, limits.volume_cost_per_gib)?,
                    ));
                    break;
                }
            }
        }
        let (action, cost) = match choice {
            Some(choice) => choice,
            None => {
                if unavailable_worker_zones.contains(zone) {
                    return Err(Refusal::Unsupported);
                }
                if actual_worker_count
                    .checked_add(new_workers)
                    .ok_or(Refusal::Overflow)?
                    >= limits.max_workers
                {
                    return Err(Refusal::InfrastructureLimit);
                }
                let bytes = limits.pool_increment_bytes.max(reserve);
                if bytes > limits.max_volume_bytes {
                    return Err(Refusal::InfrastructureLimit);
                }
                new_workers += 1;
                (
                    Action::AddWorker {
                        zone: zone.clone(),
                        bytes,
                    },
                    add(
                        limits.worker_cost,
                        storage_cost(bytes, limits.volume_cost_per_gib)?,
                    )?,
                )
            }
        };
        result.additional_monthly_cost = add(result.additional_monthly_cost, cost)?;
        result.action_monthly_costs.push(cost);
        result.actions.push(action);
    }
    if add(monthly_spend, result.additional_monthly_cost)? > limits.monthly_spend_ceiling {
        return Err(Refusal::SpendLimit);
    }
    Ok(result)
}

/// Fold a revision-pinned, authenticated ordered inventory stream. Only one
/// useful candidate per required zone stays resident. The cluster can exceed
/// the message/page size without changing a logical table's limits.
pub struct InventoryFold {
    limits: Limits,
    now: u64,
    revision: u64,
    last: Option<Uuid>,
    count: usize,
    done: bool,
    candidates: Vec<Worker>,
    split_needs: Vec<SplitPoolNeed>,
}
impl InventoryFold {
    pub fn new(limits: Limits, now: u64, revision: u64) -> Result<Self, Refusal> {
        if revision == 0 || limits.zones.is_empty() || limits.zones.len() > MAX_ZONES {
            return Err(Refusal::InvalidInventory);
        }
        Ok(Self {
            limits,
            now,
            revision,
            last: None,
            count: 0,
            done: false,
            candidates: Vec::new(),
            split_needs: Vec::new(),
        })
    }

    pub(super) fn with_split_needs(mut self, needs: Vec<SplitPoolNeed>) -> Result<Self, Refusal> {
        if needs.len() > 3
            || needs.iter().enumerate().any(|(index, need)| {
                need.worker_id.is_nil()
                    || need.pool_id.is_nil()
                    || need.required_bytes == 0
                    || need.replacement_bytes == 0
                    || !self.limits.zones.contains(&need.zone)
                    || needs[..index].iter().any(|old| {
                        old.worker_id == need.worker_id
                            || old.pool_id == need.pool_id
                            || old.zone == need.zone
                    })
            })
        {
            return Err(Refusal::InvalidInventory);
        }
        self.split_needs = needs;
        Ok(self)
    }
    pub fn push_page(
        &mut self,
        revision: u64,
        workers: Vec<Worker>,
        done: bool,
    ) -> Result<(), Refusal> {
        if self.done
            || revision != self.revision
            || workers.len() > 128
            || (!done && workers.is_empty())
        {
            return Err(Refusal::StaleInventory);
        }
        for worker in workers {
            if self.last.is_some_and(|last| worker.worker_id <= last)
                || worker.worker_id.is_nil()
                || !self.limits.zones.contains(&worker.zone)
                || worker.pools.len() > self.limits.max_pools_per_worker
                || worker.pools.len() > MAX_POOLS
                || worker.io_limit == 0
                || worker.cpu_limit == 0
                || worker.memory_limit == 0
                || worker.resident_shard_limit == 0
            {
                return Err(Refusal::InvalidInventory);
            }
            if worker.observed_at > self.now
                || self.now - worker.observed_at > self.limits.observation_max_age_seconds
            {
                return Err(Refusal::StaleInventory);
            }
            let mut pools = BTreeSet::new();
            if worker.pools.iter().any(|p| {
                p.pool_id.is_nil()
                    || !pools.insert(p.pool_id)
                    || p.total_bytes == 0
                    || p.free_bytes > p.total_bytes
                    || p.reserved_bytes > p.free_bytes
            }) {
                return Err(Refusal::InvalidInventory);
            }
            self.count = self.count.checked_add(1).ok_or(Refusal::Overflow)?;
            self.last = Some(worker.worker_id);
            if !self.split_needs.is_empty() {
                if self
                    .split_needs
                    .iter()
                    .any(|need| need.worker_id == worker.worker_id)
                {
                    self.candidates.push(worker);
                }
                continue;
            }
            let rank = self.rank(&worker)?;
            if let Some(index) = self
                .candidates
                .iter()
                .position(|old| old.zone == worker.zone)
            {
                if rank < self.rank(&self.candidates[index])? {
                    self.candidates[index] = worker;
                }
            } else {
                self.candidates.push(worker);
            }
        }
        self.done = done;
        Ok(())
    }
    fn rank(&self, worker: &Worker) -> Result<u8, Refusal> {
        if !worker.ready || !host_headroom(worker) {
            return Ok(3);
        }
        let reserve = add(
            self.limits.settlement_reserve_bytes,
            self.limits.compaction_reserve_bytes,
        )?;
        let mut rank = 3;
        for pool in worker.pools.iter().filter(|p| p.healthy) {
            let required = add(
                reserve,
                mul(
                    pool.write_bytes_per_second,
                    self.limits.provisioning_seconds,
                )?,
            )?;
            if pool.free_bytes - pool.reserved_bytes >= required {
                return Ok(0);
            }
            let target = add(
                pool.total_bytes,
                self.limits
                    .pool_increment_bytes
                    .max(required.saturating_sub(pool.free_bytes - pool.reserved_bytes)),
            )?;
            if pool.expandable && target <= self.limits.max_volume_bytes {
                rank = 1;
            }
        }
        if rank > 1
            && worker.attachment_slots_free > 0
            && worker.pools.len() < self.limits.max_pools_per_worker
        {
            rank = 2;
        }
        Ok(rank)
    }
    pub fn finish(
        self,
        last_attempt: Option<u64>,
        attempts: u32,
        monthly_spend: u64,
        provider_available: bool,
    ) -> Result<Plan, Refusal> {
        if !self.done {
            return Err(Refusal::StaleInventory);
        }
        plan_candidates(
            &self.limits,
            &self.candidates,
            self.count,
            self.now,
            last_attempt,
            attempts,
            monthly_spend,
            provider_available,
            &[],
        )
    }
    pub(super) fn finish_with_unavailable_workers(
        self,
        last_attempt: Option<u64>,
        retained_workers: usize,
        monthly_spend: u64,
        unavailable_worker_zones: &[String],
    ) -> Result<Plan, Refusal> {
        if !self.done {
            return Err(Refusal::StaleInventory);
        }
        let worker_count = self
            .count
            .checked_add(retained_workers)
            .ok_or(Refusal::Overflow)?;
        if !self.split_needs.is_empty() {
            return self.finish_split(
                last_attempt,
                worker_count,
                monthly_spend,
                unavailable_worker_zones,
            );
        }
        plan_candidates(
            &self.limits,
            &self.candidates,
            worker_count,
            self.now,
            last_attempt,
            0,
            monthly_spend,
            true,
            unavailable_worker_zones,
        )
    }

    fn finish_split(
        self,
        last_attempt: Option<u64>,
        mut worker_count: usize,
        monthly_spend: u64,
        unavailable_worker_zones: &[String],
    ) -> Result<Plan, Refusal> {
        let limits = &self.limits;
        if worker_count > limits.max_workers {
            return Err(Refusal::InfrastructureLimit);
        }
        if last_attempt
            .is_some_and(|last| self.now < last || self.now - last < limits.cooldown_seconds)
        {
            return Err(Refusal::Cooldown);
        }
        let reserve = add(
            limits.settlement_reserve_bytes,
            limits.compaction_reserve_bytes,
        )?;
        let mut plan = Plan {
            actions: Vec::new(),
            action_monthly_costs: Vec::new(),
            additional_monthly_cost: 0,
        };
        for need in &self.split_needs {
            let worker = self
                .candidates
                .iter()
                .find(|worker| {
                    worker.worker_id == need.worker_id && worker.zone == need.zone && worker.ready
                })
                .ok_or(Refusal::StaleInventory)?;
            let pool = worker
                .pools
                .iter()
                .find(|pool| pool.pool_id == need.pool_id)
                .ok_or(Refusal::StaleInventory)?;
            let interval = mul(pool.write_bytes_per_second, limits.provisioning_seconds)?;
            let required = add(need.required_bytes, add(reserve, interval)?)?;
            let free = pool.free_bytes - pool.reserved_bytes;
            if free >= need.required_bytes {
                return Err(Refusal::StaleInventory);
            }
            let increment = limits.pool_increment_bytes.max(required - free);
            let target = add(pool.total_bytes, increment)?;
            let bytes = limits
                .pool_increment_bytes
                .max(add(need.replacement_bytes, add(reserve, interval)?)?);
            let can_host = host_headroom(worker)
                && worker
                    .resident_shards
                    .checked_add(2)
                    .is_some_and(|used| used <= worker.resident_shard_limit);
            let (action, cost) =
                if can_host && pool.healthy && pool.expandable && target <= limits.max_volume_bytes
                {
                    (
                        Action::Expand {
                            worker: worker.worker_id,
                            pool: pool.pool_id,
                            target_bytes: target,
                        },
                        storage_cost(increment, limits.volume_cost_per_gib)?,
                    )
                } else if can_host
                    && need.can_move_to_local_pool
                    && worker.attachment_slots_free > 0
                    && worker.pools.len() < limits.max_pools_per_worker
                    && bytes <= limits.max_volume_bytes
                {
                    (
                        Action::AddPool {
                            worker: worker.worker_id,
                            zone: need.zone.clone(),
                            bytes,
                        },
                        storage_cost(bytes, limits.volume_cost_per_gib)?,
                    )
                } else {
                    if !need.can_move_to_new_worker && !need.can_move_to_local_pool {
                        return Err(Refusal::PlacementUnavailable);
                    }
                    if bytes > limits.max_volume_bytes || worker_count >= limits.max_workers {
                        return Err(Refusal::InfrastructureLimit);
                    }
                    if !need.can_move_to_new_worker || unavailable_worker_zones.contains(&need.zone)
                    {
                        return Err(Refusal::Unsupported);
                    }
                    worker_count += 1;
                    (
                        Action::AddWorker {
                            zone: need.zone.clone(),
                            bytes,
                        },
                        add(
                            limits.worker_cost,
                            storage_cost(bytes, limits.volume_cost_per_gib)?,
                        )?,
                    )
                };
            plan.actions.push(action);
            plan.action_monthly_costs.push(cost);
            plan.additional_monthly_cost = add(plan.additional_monthly_cost, cost)?;
        }
        if add(monthly_spend, plan.additional_monthly_cost)? > limits.monthly_spend_ceiling {
            return Err(Refusal::SpendLimit);
        }
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Limits {
        Limits {
            zones: vec!["a".into(), "b".into(), "c".into()],
            max_workers: 6,
            max_pools_per_worker: 2,
            max_volume_bytes: 8 << 30,
            pool_increment_bytes: 1 << 30,
            settlement_reserve_bytes: 100,
            compaction_reserve_bytes: 200,
            provisioning_seconds: 10,
            observation_max_age_seconds: 30,
            cooldown_seconds: 60,
            max_attempts: 3,
            volume_cost_per_gib: 5,
            worker_cost: 20,
            monthly_spend_ceiling: 100,
        }
    }
    fn workers() -> Vec<Worker> {
        ["a", "b", "c"]
            .iter()
            .map(|zone| Worker {
                worker_id: nabedra_rt::ids::uuid_v4(),
                zone: (*zone).into(),
                observed_at: 100,
                ready: true,
                attachment_slots_free: 1,
                io_used: 1,
                io_limit: 10,
                cpu_used: 1,
                cpu_limit: 10,
                memory_used: 1,
                memory_limit: 10,
                resident_shards: 1,
                resident_shard_limit: 10,
                pools: vec![Pool {
                    pool_id: nabedra_rt::ids::uuid_v4(),
                    total_bytes: 1 << 30,
                    free_bytes: 250,
                    reserved_bytes: 50,
                    write_bytes_per_second: 10,
                    expandable: true,
                    healthy: true,
                }],
            })
            .collect()
    }
    #[test]
    fn o28_capacity_inventory_folds_beyond_one_message_without_retaining_the_cluster() {
        let mut limits = limits();
        limits.zones = vec!["a".into()];
        limits.max_workers = 10000;
        let mut fold = InventoryFold::new(limits.clone(), 100, 7).unwrap();
        let mut template = workers()[0].clone();
        template.pools[0].free_bytes = 1000;
        for first in (1..=5000).step_by(128) {
            let end = (first + 127).min(5000);
            let page = (first..=end)
                .map(|id| {
                    let mut worker = template.clone();
                    worker.worker_id = Uuid::from_u128(id as u128);
                    worker.pools[0].pool_id = Uuid::from_u128(10000 + id as u128);
                    worker
                })
                .collect();
            fold.push_page(7, page, end == 5000).unwrap();
            assert_eq!(fold.candidates.len(), 1);
        }
        assert_eq!(fold.count, 5000);
        assert!(fold.finish(None, 0, 0, true).unwrap().actions.is_empty());
        let mut incomplete = InventoryFold::new(limits, 100, 7).unwrap();
        incomplete.push_page(7, vec![template], false).unwrap();
        assert_eq!(
            incomplete.finish(None, 0, 0, true),
            Err(Refusal::StaleInventory)
        );
    }
    #[test]
    fn o27_capacity_selects_expansion_pool_and_worker_by_measured_host_limits() {
        let mut ws = workers();
        ws[1].pools[0].expandable = false;
        ws[2].io_used = ws[2].io_limit;
        let plan = plan_growth(&limits(), &ws, 100, None, 0, 0, true).unwrap();
        assert_eq!(plan.actions.len(), 3);
        assert!(matches!(plan.actions[0], Action::Expand { .. }));
        assert!(matches!(plan.actions[1], Action::AddPool { .. }));
        assert!(matches!(plan.actions[2], Action::AddWorker { .. }));
        assert_eq!(plan.additional_monthly_cost, 35);
        assert_eq!(ws[0].pools[0].total_bytes, 1 << 30);
    }
    #[test]
    fn o16_o17_o21_o26_o27_o28_exact_split_never_uses_unrelated_zone_headroom() {
        let mut ws = workers();
        let mut unrelated = ws[0].clone();
        unrelated.worker_id = nabedra_rt::ids::uuid_v4();
        unrelated.pools[0].pool_id = nabedra_rt::ids::uuid_v4();
        unrelated.pools[0].free_bytes = 1 << 30;
        ws.push(unrelated);
        let requirement = 2 << 30;
        let mut need = SplitPoolNeed {
            worker_id: ws[0].worker_id,
            pool_id: ws[0].pools[0].pool_id,
            zone: ws[0].zone.clone(),
            required_bytes: requirement,
            replacement_bytes: requirement + 1000,
            can_move_to_local_pool: true,
            can_move_to_new_worker: true,
        };
        let plan = |ws: &[Worker], need: &SplitPoolNeed, last, spend, unavailable: &[String]| {
            let mut sorted = ws.to_vec();
            sorted.sort_by_key(|worker| worker.worker_id);
            let mut fold = InventoryFold::new(limits(), 100, 7)
                .unwrap()
                .with_split_needs(vec![need.clone()])
                .unwrap();
            fold.push_page(7, sorted, true).unwrap();
            fold.finish_with_unavailable_workers(last, 0, spend, unavailable)
        };
        assert_eq!(
            plan(&ws, &need, None, 0, &[]).unwrap().actions,
            [Action::Expand {
                worker: need.worker_id,
                pool: need.pool_id,
                target_bytes: (1 << 30) + requirement + 200,
            }]
        );
        assert_eq!(plan(&ws, &need, Some(99), 0, &[]), Err(Refusal::Cooldown));
        assert_eq!(plan(&ws, &need, None, 99, &[]), Err(Refusal::SpendLimit));
        ws[0].pools[0].expandable = false;
        assert!(matches!(
            plan(&ws, &need, None, 0, &[]).unwrap().actions.as_slice(),
            [Action::AddPool { bytes, .. }] if *bytes == requirement + 1400
        ));
        ws[0].attachment_slots_free = 0;
        assert!(matches!(
            plan(&ws, &need, None, 0, &[]).unwrap().actions.as_slice(),
            [Action::AddWorker { bytes, .. }] if *bytes == requirement + 1400
        ));
        assert_eq!(
            plan(&ws, &need, None, 0, &["a".into()]),
            Err(Refusal::Unsupported)
        );
        need.can_move_to_local_pool = false;
        need.can_move_to_new_worker = false;
        assert_eq!(
            plan(&ws, &need, None, 0, &[]),
            Err(Refusal::PlacementUnavailable)
        );
    }

    #[test]
    fn a01_a02_o27_capacity_refuses_missing_adapter_stale_inventory_retries_and_spend() {
        let ws = workers();
        let l = limits();
        assert_eq!(
            plan_growth(&l, &ws, 100, None, 0, 0, false),
            Err(Refusal::Unsupported)
        );
        assert_eq!(
            plan_growth(&l, &ws, 131, None, 0, 0, true),
            Err(Refusal::StaleInventory)
        );
        assert_eq!(
            plan_growth(&l, &ws, 100, Some(90), 1, 0, true),
            Err(Refusal::Cooldown)
        );
        assert_eq!(
            plan_growth(&l, &ws, 100, None, 3, 0, true),
            Err(Refusal::RetryLimit)
        );
        assert_eq!(
            plan_growth(&l, &ws, 100, None, 0, 99, true),
            Err(Refusal::SpendLimit)
        );
        let mut capped = l;
        capped.max_workers = 3;
        let mut full = ws;
        full[0].attachment_slots_free = 0;
        full[0].pools[0].expandable = false;
        assert_eq!(
            plan_growth(&capped, &full, 100, None, 0, 0, true),
            Err(Refusal::InfrastructureLimit)
        );
    }
}
