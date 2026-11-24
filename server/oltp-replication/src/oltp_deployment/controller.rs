//! Durable growth state. The Control service constructs completion evidence
//! from a live registered store, never from a provider result or HTTP payload.
use super::planner::Action;
use crate::oltp_replication::types::WorkerIdentity;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct GrowthPolicy {
    pub policy_id: Uuid,
    pub volume_cost_per_gib: u64,
    pub worker_cost: u64,
    pub initial_monthly_spend: u64,
    pub monthly_spend_ceiling: u64,
    pub max_attempts: u32,
    pub cooldown_seconds: u64,
    pub max_volume_bytes: u64,
    pub zones: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub initial_replica_zones: BTreeMap<Uuid, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic: Option<super::live::AutomaticPolicy>,
}
impl GrowthPolicy {
    #[doc(hidden)]
    pub fn valid(&self) -> bool {
        !self.policy_id.is_nil()
            && self.initial_monthly_spend <= self.monthly_spend_ceiling
            && (1..=32).contains(&self.max_attempts)
            && (1..=86400).contains(&self.cooldown_seconds)
            && self.max_volume_bytes > 0
            && !self.zones.is_empty()
            && self.zones.len() <= 16
            && self
                .zones
                .iter()
                .all(|zone| !zone.is_empty() && zone.len() <= 128)
            && self
                .zones
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.zones.len()
            && self.initial_replica_zones.len() <= 16
            && self
                .initial_replica_zones
                .iter()
                .all(|(worker, zone)| !worker.is_nil() && self.zones.contains(zone))
            && self
                .automatic
                .as_ref()
                .is_none_or(|policy| policy.valid(self))
    }
    pub(crate) fn cost(&self, intent: &GrowthIntent, prior: Option<u64>) -> Option<u64> {
        if intent.policy_id != self.policy_id
            || intent.max_attempts != self.max_attempts
            || intent.cooldown_seconds != self.cooldown_seconds
            || intent.monthly_spend_ceiling != self.monthly_spend_ceiling
            || intent.requested_bytes() > self.max_volume_bytes
        {
            return None;
        }
        let bytes = match &intent.action {
            Action::Expand { target_bytes, .. } => target_bytes
                .checked_sub(prior?)
                .filter(|growth| *growth > 0)?,
            Action::AddPool { zone, bytes, .. } | Action::AddWorker { zone, bytes } => {
                if prior.is_some() || !self.zones.contains(zone) {
                    return None;
                }
                *bytes
            }
        };
        let gib = bytes.checked_add((1 << 30) - 1)? >> 30;
        let storage = gib.checked_mul(self.volume_cost_per_gib)?;
        storage.checked_add(if matches!(intent.action, Action::AddWorker { .. }) {
            self.worker_cost
        } else {
            0
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct GrowthIntent {
    pub policy_id: Uuid,
    pub claim_id: Uuid,
    pub action: Action,
    pub worker: WorkerIdentity,
    pub pool_id: Uuid,
    pub pool_generation: u64,
    pub quoted_monthly_cost: u64,
    pub monthly_spend_ceiling: u64,
    pub max_attempts: u32,
    pub cooldown_seconds: u64,
}
impl GrowthIntent {
    #[doc(hidden)]
    pub fn valid(&self) -> bool {
        !self.policy_id.is_nil()
            && !self.claim_id.is_nil()
            && !self.pool_id.is_nil()
            && self.pool_generation > 0
            && self.worker.validate().is_ok()
            && self.max_attempts > 0
            && self.max_attempts <= 32
            && self.cooldown_seconds > 0
            && self.cooldown_seconds <= 86400
            && self.quoted_monthly_cost <= self.monthly_spend_ceiling
            && match &self.action {
                Action::Expand {
                    worker,
                    pool,
                    target_bytes,
                } => *worker == self.worker.worker_id && *pool == self.pool_id && *target_bytes > 0,
                Action::AddPool {
                    worker,
                    zone,
                    bytes,
                } => {
                    *worker == self.worker.worker_id
                        && !zone.is_empty()
                        && zone.len() <= 128
                        && *bytes > 0
                }
                Action::AddWorker { zone, bytes } => {
                    !zone.is_empty() && zone.len() <= 128 && *bytes > 0
                }
            }
    }
    pub(crate) fn requested_bytes(&self) -> u64 {
        match self.action {
            Action::Expand { target_bytes, .. } => target_bytes,
            Action::AddPool { bytes, .. } | Action::AddWorker { bytes, .. } => bytes,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[doc(hidden)]
pub enum GrowthPhase {
    Claimed,
    Provisioning,
    AwaitingMount,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct GrowthRecord {
    pub intent: GrowthIntent,
    pub phase: GrowthPhase,
    pub attempts: u32,
    pub last_attempt: Option<u64>,
    pub provider_resource: Option<String>,
    pub mounted_total_bytes: Option<u64>,
    pub prior_total_bytes: Option<u64>,
}

/// Metadata written by an authenticated service only after inspecting the live
/// opened pool. It is private to the replicated Control command boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MountedGrowthStamp {
    pub worker: WorkerIdentity,
    pub pool_id: Uuid,
    pub pool_generation: u64,
    pub provider_volume_id: String,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum GrowthCommand {
    Configure {
        policy: GrowthPolicy,
    },
    Claim {
        intent: GrowthIntent,
        prior_total_bytes: Option<u64>,
    },
    ClaimAutomatic {
        intent: GrowthIntent,
        prior_total_bytes: Option<u64>,
        pin: super::live::InventoryPin,
    },
    Attempt {
        claim_id: Uuid,
        now: u64,
    },
    Provisioned {
        claim_id: Uuid,
        resource_id: String,
    },
    Mounted {
        claim_id: Uuid,
        proof: MountedGrowthStamp,
    },
}
impl GrowthCommand {
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Configure { policy } => {
                policy.zones.capacity() * std::mem::size_of::<String>()
                    + policy.zones.iter().map(String::capacity).sum::<usize>()
                    + policy
                        .initial_replica_zones
                        .values()
                        .map(|zone| zone.capacity() + 128)
                        .sum::<usize>()
            }
            Self::Claim { intent, .. } | Self::ClaimAutomatic { intent, .. } => {
                match &intent.action {
                    Action::Expand { .. } => 0,
                    Action::AddPool { zone, .. } | Action::AddWorker { zone, .. } => {
                        zone.capacity()
                    }
                }
            }
            Self::Provisioned { resource_id, .. } => resource_id.capacity(),
            Self::Mounted { proof, .. } => proof.provider_volume_id.capacity(),
            _ => 0,
        }
    }
}
