//! Control inventory pins and the local operator's bounded planner input.
use super::{
    controller::{GrowthPolicy, GrowthRecord},
    planner::{InventoryFold, Limits, Refusal, SplitPoolNeed, Worker},
};
use crate::oltp_replication::types::{ReadyMembership, WorkerEnrollment};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::{self, BufRead, Read, Write};
use uuid::Uuid;

const MAX_PAGE_BYTES: usize = 256 << 10;
pub(crate) const MAX_AUTO_WORKERS: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomaticPolicy {
    pub adapter_id: Uuid,
    pub max_workers: usize,
    pub max_pools_per_worker: usize,
    pub pool_increment_bytes: u64,
    pub settlement_reserve_bytes: u64,
    pub compaction_reserve_bytes: u64,
    pub provisioning_seconds: u64,
    pub observation_max_age_seconds: u64,
    pub sample_seconds: u64,
    pub cpu_limit_millicores: u64,
    pub io_limit_bytes_per_second: u64,
    pub queue_pressure_count: u64,
}
impl AutomaticPolicy {
    pub(crate) fn valid(&self, policy: &GrowthPolicy) -> bool {
        !self.adapter_id.is_nil()
            && (policy.zones.len()..=MAX_AUTO_WORKERS).contains(&self.max_workers)
            && (1..=8).contains(&self.max_pools_per_worker)
            && (1..=policy.max_volume_bytes).contains(&self.pool_increment_bytes)
            && self
                .settlement_reserve_bytes
                .checked_add(self.compaction_reserve_bytes)
                .is_some_and(|reserve| reserve > 0 && reserve <= policy.max_volume_bytes)
            && (1..=3600).contains(&self.provisioning_seconds)
            && (1..=30).contains(&self.sample_seconds)
            && (self.sample_seconds + 1..=120).contains(&self.observation_max_age_seconds)
            && self.cpu_limit_millicores > 0
            && self.io_limit_bytes_per_second > 0
            && self.queue_pressure_count > 0
    }

    fn limits(&self, policy: &GrowthPolicy) -> Limits {
        Limits {
            zones: policy.zones.clone(),
            max_workers: self.max_workers,
            max_pools_per_worker: self.max_pools_per_worker,
            max_volume_bytes: policy.max_volume_bytes,
            pool_increment_bytes: self.pool_increment_bytes,
            settlement_reserve_bytes: self.settlement_reserve_bytes,
            compaction_reserve_bytes: self.compaction_reserve_bytes,
            provisioning_seconds: self.provisioning_seconds,
            observation_max_age_seconds: self.observation_max_age_seconds,
            cooldown_seconds: policy.cooldown_seconds,
            max_attempts: policy.max_attempts,
            volume_cost_per_gib: policy.volume_cost_per_gib,
            worker_cost: policy.worker_cost,
            monthly_spend_ceiling: policy.monthly_spend_ceiling,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InventoryPin {
    pub policy_id: Uuid,
    pub ready: ReadyMembership,
    pub enrollment_revision: u64,
    pub group_revision: u64,
    pub growth_revision: u64,
    pub monthly_spend: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GrowthInventory {
    pub cluster_id: Uuid,
    pub pin: InventoryPin,
    pub policy: GrowthPolicy,
    pub last_attempt: Option<u64>,
    pub active: Option<GrowthRecord>,
    pub active_automatic: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerPage {
    pub pin: InventoryPin,
    pub workers: Vec<WorkerEnrollment>,
    pub after: Option<Uuid>,
    pub done: bool,
}

#[derive(Serialize)]
pub(crate) struct UsageSample {
    pub monotonic_ns: u64,
    pub cpu_time_ns: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub allocated_query_threads: u64,
    pub query_thread_limit: u64,
    pub charged_memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub resident_group_count: usize,
    pub resident_group_limit: usize,
    pub queued_count: usize,
    pub oldest_queued: Option<(Uuid, u64)>,
}

pub(crate) fn process_counters() -> Option<(u64, u64, u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        fn clock_ns(clock: libc::clockid_t) -> Option<u64> {
            let mut sample = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            if unsafe { libc::clock_gettime(clock, &mut sample) } != 0 {
                return None;
            }
            u64::try_from(sample.tv_sec)
                .ok()?
                .checked_mul(1_000_000_000)?
                .checked_add(u64::try_from(sample.tv_nsec).ok()?)
        }
        let io = std::fs::read_to_string("/proc/self/io").ok()?;
        let read = io
            .lines()
            .find_map(|line| line.strip_prefix("read_bytes: "))?
            .parse()
            .ok()?;
        let write = io
            .lines()
            .find_map(|line| line.strip_prefix("write_bytes: "))?
            .parse()
            .ok()?;
        Some((
            clock_ns(libc::CLOCK_MONOTONIC)?,
            clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)?,
            read,
            write,
        ))
    }
    #[cfg(not(target_os = "linux"))]
    None
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlannerInput {
    policy: GrowthPolicy,
    now: u64,
    revision: u64,
    last_attempt: Option<u64>,
    monthly_spend: u64,
    retained_workers: usize,
    unavailable_worker_zones: Vec<String>,
    #[serde(default)]
    split_needs: Vec<SplitPoolNeed>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlannerPage {
    revision: u64,
    workers: Vec<Worker>,
    done: bool,
}

fn line<T: DeserializeOwned>(reader: &mut impl BufRead) -> Result<T, &'static str> {
    let mut bytes = Vec::new();
    Read::take(reader, (MAX_PAGE_BYTES + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .map_err(|_| "cannot read growth planner input")?;
    if bytes.len() > MAX_PAGE_BYTES || bytes.last() != Some(&b'\n') {
        return Err("invalid growth planner page size");
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid growth planner input")
}

fn plan(mut reader: impl BufRead) -> Result<Result<super::planner::Plan, Refusal>, &'static str> {
    let header: PlannerInput = line(&mut reader)?;
    let automatic = header
        .policy
        .automatic
        .as_ref()
        .ok_or("automatic growth policy is absent")?;
    if !header.policy.valid()
        || header.unavailable_worker_zones.len() > header.policy.zones.len()
        || header
            .unavailable_worker_zones
            .iter()
            .any(|zone| !header.policy.zones.contains(zone))
    {
        return Err("invalid automatic growth policy");
    }
    let mut fold = InventoryFold::new(
        automatic.limits(&header.policy),
        header.now,
        header.revision,
    )
    .map_err(|_| "invalid growth inventory revision")?;
    fold = match fold.with_split_needs(header.split_needs) {
        Ok(fold) => fold,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let mut remaining_workers = automatic.max_workers;
    for _ in 0..=automatic.max_workers {
        let page: PlannerPage = line(&mut reader)?;
        let Some(remaining) = remaining_workers.checked_sub(page.workers.len()) else {
            return Ok(Err(Refusal::InfrastructureLimit));
        };
        remaining_workers = remaining;
        if let Err(refusal) = fold.push_page(page.revision, page.workers, page.done) {
            return Ok(Err(refusal));
        }
        if page.done {
            let mut trailing = [0];
            if reader
                .read(&mut trailing)
                .map_err(|_| "cannot read growth planner input")?
                != 0
            {
                return Err("trailing growth planner input");
            }
            return Ok(fold.finish_with_unavailable_workers(
                header.last_attempt,
                header.retained_workers,
                header.monthly_spend,
                &header.unavailable_worker_zones,
            ));
        }
    }
    Err("growth planner page limit reached")
}

pub fn run_planner() -> Result<(), &'static str> {
    let result = match plan(io::stdin().lock())? {
        Ok(plan) => serde_json::json!({"plan": plan}),
        Err(refusal) => serde_json::json!({"refusal": refusal}),
    };
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, &result).map_err(|_| "cannot encode growth plan")?;
    output
        .write_all(b"\n")
        .map_err(|_| "cannot publish growth plan")
}

#[cfg(test)]
mod tests {
    use super::super::planner::{Action, Pool};
    use super::*;

    fn mixed_plan(
        unavailable: &[&str],
        retained: usize,
        spend: u64,
        second_revision: u64,
    ) -> Result<super::super::planner::Plan, Refusal> {
        let gib = 1 << 30;
        let zones = ["healthy", "expand", "pool", "worker"];
        let policy = GrowthPolicy {
            policy_id: Uuid::from_u128(1),
            volume_cost_per_gib: 5,
            worker_cost: 20,
            initial_monthly_spend: 0,
            monthly_spend_ceiling: 40,
            max_attempts: 3,
            cooldown_seconds: 1,
            max_volume_bytes: 4 * gib,
            zones: zones.iter().map(ToString::to_string).collect(),
            initial_replica_zones: Default::default(),
            automatic: Some(AutomaticPolicy {
                adapter_id: Uuid::from_u128(2),
                max_workers: 5,
                max_pools_per_worker: 2,
                pool_increment_bytes: gib,
                settlement_reserve_bytes: 200,
                compaction_reserve_bytes: 300,
                provisioning_seconds: 10,
                observation_max_age_seconds: 5,
                sample_seconds: 1,
                cpu_limit_millicores: 1000,
                io_limit_bytes_per_second: 100,
                queue_pressure_count: 1,
            }),
        };
        let workers: Vec<_> = zones
            .iter()
            .enumerate()
            .map(|(index, zone)| Worker {
                worker_id: Uuid::from_u128(10 + index as u128),
                zone: (*zone).into(),
                observed_at: 100,
                ready: true,
                attachment_slots_free: u32::from(*zone == "pool"),
                io_used: if *zone == "worker" { 100 } else { 10 },
                io_limit: 100,
                cpu_used: 10,
                cpu_limit: 1000,
                memory_used: 100,
                memory_limit: 200,
                resident_shards: 1,
                resident_shard_limit: 5,
                pools: vec![Pool {
                    pool_id: Uuid::from_u128(20 + index as u128),
                    total_bytes: gib,
                    free_bytes: if *zone == "healthy" { 1000 } else { 300 },
                    reserved_bytes: 100,
                    write_bytes_per_second: 10,
                    expandable: *zone == "expand",
                    healthy: true,
                }],
            })
            .collect();
        let header = serde_json::json!({
            "policy": policy, "now": 101, "revision": 7, "last_attempt": 100,
            "monthly_spend": spend, "retained_workers": retained,
            "unavailable_worker_zones": unavailable,
        });
        let page_a = serde_json::json!({"revision": 7, "workers": &workers[..2], "done": false});
        let page_b = serde_json::json!({"revision": second_revision, "workers": &workers[2..], "done": true});
        let input = format!("{header}\n{page_a}\n{page_b}\n");
        plan(input.as_bytes()).unwrap()
    }

    #[test]
    fn o27_o28_a01_a02_live_input_folds_actions_and_refuses_stale_spend_or_provider_inventory() {
        let plan = mixed_plan(&[], 0, 5, 7).unwrap();
        assert!(matches!(
            plan.actions.as_slice(),
            [
                Action::Expand { .. },
                Action::AddPool { .. },
                Action::AddWorker { .. },
            ]
        ));
        assert_eq!(plan.action_monthly_costs, [5, 5, 25]);
        assert_eq!(plan.additional_monthly_cost, 35);
        assert_eq!(mixed_plan(&[], 0, 6, 7), Err(Refusal::SpendLimit));
        assert_eq!(mixed_plan(&[], 1, 5, 7), Err(Refusal::InfrastructureLimit));
        assert_eq!(mixed_plan(&["worker"], 0, 5, 7), Err(Refusal::Unsupported));
        assert_eq!(mixed_plan(&[], 0, 5, 8), Err(Refusal::StaleInventory));
    }
}
