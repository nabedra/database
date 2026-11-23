//! Versioned queue, execution and resource limits.

use crate::partition_size::PartitionSize;
use std::fmt;

/// Static diagnostics deliberately omit environment values, paths, and OS errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigError(&'static str);

impl ConfigError {
    /// A static diagnostic that omits untrusted configuration values.
    pub const fn new(message: &'static str) -> Self {
        Self(message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}

/// How long forward markers and direct dedup records are kept: 24 hours
/// (`DEFAULT_DIRECT_DEDUP_RETENTION_MS`, plan "Exactly-once across paths").
pub const DIRECT_MARKER_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
/// The time a queued request may take from settling its id at the anchor
/// (forward marker or history window) to finishing (queue wait plus
/// execution) while the markers still answer a retry (RV-FINAL M2).
pub const DIRECT_QUEUED_LIFETIME_ALLOWANCE_MS: u64 = 12 * 60 * 60 * 1000;
/// `result_retention_ms` at most. A queued receipt then never outlives its
/// marker unless the request took longer than the allowance, so a direct
/// retry of a queued id finds the marker and never executes it again
/// (Q05/Q06). `data.direct.v1` can activate on any cluster of this release,
/// so the bound always applies.
pub const MAX_RESULT_RETENTION_MS: u64 =
    DIRECT_MARKER_RETENTION_MS - DIRECT_QUEUED_LIFETIME_ALLOWANCE_MS;

/// Versioned cluster-wide queue and logical-request limits.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub max_workers_per_request: u64,
    pub max_pending_requests: u64,
    pub max_queue_bytes_per_replica: u64,
    pub max_retained_result_bytes_per_worker: u64,
    pub result_retention_ms: u64,
    pub queue_replication: QueueReplication,
    pub queue_lifetime: QueueLifetime,
    pub partition_size: PartitionSize,
    /// Eligible point operations take the direct path once `data.direct.v1`
    /// is active (plan "Rollout"). `false` keeps the capability but routes
    /// them queued; they still settle their request id at the anchor first.
    #[serde(default = "direct_oltp_default", skip_serializing_if = "is_true")]
    pub direct_oltp: bool,
}
pub fn direct_oltp_default() -> bool {
    // T9: the library suite runs once more with `direct_oltp` off by default
    // (`NABEDRA_TEST_DIRECT_OLTP=off`). Release builds always default to on.
    #[cfg(any(test, feature = "test-defaults"))]
    if std::env::var_os("NABEDRA_TEST_DIRECT_OLTP").is_some_and(|value| value == "off") {
        return false;
    }
    true
}
fn is_true(value: &bool) -> bool {
    *value
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueReplication {
    AllReadyWorkers,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueLifetime {
    LiveCluster,
}
impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            max_workers_per_request: 4,
            max_pending_requests: 1024,
            max_queue_bytes_per_replica: 256 << 20,
            max_retained_result_bytes_per_worker: 64 << 20,
            result_retention_ms: 300_000,
            queue_replication: QueueReplication::AllReadyWorkers,
            queue_lifetime: QueueLifetime::LiveCluster,
            partition_size: PartitionSize::default(),
            direct_oltp: direct_oltp_default(),
        }
    }
}
impl ExecutionConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=2048).contains(&self.max_workers_per_request)
            || !(1..=1_000_000).contains(&self.max_pending_requests)
            || self.max_queue_bytes_per_replica == 0
            || self.max_retained_result_bytes_per_worker == 0
            || self.max_queue_bytes_per_replica > isize::MAX as u64
            || self.max_retained_result_bytes_per_worker > isize::MAX as u64
            || self.result_retention_ms == 0
            || self
                .max_queue_bytes_per_replica
                .checked_add(self.max_retained_result_bytes_per_worker)
                .is_none()
        {
            return Err(ConfigError("invalid execution limits"));
        }
        if self.result_retention_ms > MAX_RESULT_RETENTION_MS {
            return Err(ConfigError(
                "result_retention_ms must not exceed 43200000 (12 h): queued receipts must expire \
                 before the 24 h direct forward markers",
            ));
        }
        Ok(())
    }
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

/// Local ceilings advertised in the signed enrollment and checked before readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResources {
    pub max_threads: u64,
    pub memory_limit_bytes: u64,
    pub task_memory_limit_bytes: u64,
    pub control_memory_reserve_bytes: u64,
}
impl Default for WorkerResources {
    fn default() -> Self {
        Self {
            max_threads: 4,
            memory_limit_bytes: 8 << 30,
            task_memory_limit_bytes: 512 << 20,
            control_memory_reserve_bytes: 256 << 20,
        }
    }
}
impl WorkerResources {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=256).contains(&self.max_threads)
            || self.memory_limit_bytes > isize::MAX as u64
            || self.task_memory_limit_bytes == 0
            || self.control_memory_reserve_bytes == 0
            || self.control_memory_reserve_bytes >= self.memory_limit_bytes
            || self.task_memory_limit_bytes
                > self.memory_limit_bytes - self.control_memory_reserve_bytes
        {
            return Err(ConfigError("invalid worker resource limits"));
        }
        Ok(())
    }
    pub fn working_bytes(&self) -> u64 {
        self.memory_limit_bytes
            .saturating_sub(self.control_memory_reserve_bytes)
    }
    pub fn validate_execution(&self, execution: &ExecutionConfig) -> Result<(), ConfigError> {
        self.validate()?;
        execution.validate()?;
        let reserved = execution
            .max_queue_bytes_per_replica
            .checked_add(execution.max_retained_result_bytes_per_worker)
            .and_then(|bytes| bytes.checked_add(self.control_memory_reserve_bytes))
            .ok_or(ConfigError("worker memory accounting overflow"))?;
        if reserved >= self.memory_limit_bytes
            || self.task_memory_limit_bytes > self.memory_limit_bytes - reserved
        {
            return Err(ConfigError("queue, results and task exceed worker memory"));
        }
        Ok(())
    }
}
