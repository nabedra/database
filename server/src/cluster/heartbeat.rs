//! Bounded heartbeat publication. No freshness or drain authority.
use super::member::{Heartbeat, MAX_HEARTBEAT_BYTES, MemberError};
use crate::storage::{ObjectKey, ObjectStore, Storage, StorageError, WriteCondition};
use nabedra_rt::time;
use nabedra_rt::time::timeout_at;
use tokio::time::Instant;
use uuid::Uuid;
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct HeartbeatBinding {
    pub(crate) resources: Option<crate::config::WorkerResources>,
    pub(crate) cluster_id: Uuid,
    pub(crate) node_id: Uuid,
    pub(crate) incarnation: Uuid,
    pub(crate) session_id: Uuid,
    pub(crate) verifier: String,
    pub(crate) operation_slots: u64,
    pub(crate) partition_slots: u64,
    pub(crate) queue_size: u64,
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct HeartbeatSample {
    pub(crate) available_threads: Option<u64>,
    pub(crate) available_memory_bytes: Option<u64>,
    pub(crate) running_operations: u64,
    pub(crate) running_partitions: u64,
    pub(crate) queued_operations: u64,
    pub(crate) queued_partitions: u64,
    pub(crate) drain_id: Option<Uuid>,
    pub(crate) accepting: bool,
}
// Pending R04c and C03d consumers. Keep receipts opaque and immutable.
#[allow(dead_code)]
pub(crate) struct AcknowledgedHeartbeat {
    binding: HeartbeatBinding,
    sample: HeartbeatSample,
    sequence: u64,
}
#[allow(dead_code)]
impl AcknowledgedHeartbeat {
    pub(crate) fn binding(&self) -> &HeartbeatBinding {
        &self.binding
    }
    pub(crate) fn sample(&self) -> &HeartbeatSample {
        &self.sample
    }
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}
pub(crate) struct LoadedHeartbeat {
    binding: HeartbeatBinding,
    heartbeat: Heartbeat,
    token: String,
}
impl LoadedHeartbeat {
    pub(crate) fn binding(&self) -> &HeartbeatBinding {
        &self.binding
    }
    pub(crate) fn heartbeat(&self) -> &Heartbeat {
        &self.heartbeat
    }
}
// Pending serialized runtime writer and outcome handling.
#[allow(dead_code)]
pub(crate) struct HeartbeatWriter {
    binding: HeartbeatBinding,
    token: Option<String>,
    attempted: u64,
    observed: u64,
    confirmed: Option<HeartbeatSample>,
    pending: Option<Heartbeat>,
    reconcile: bool,
}
#[allow(dead_code)]
pub(crate) enum HeartbeatWriteResult {
    Acknowledged(AcknowledgedHeartbeat),
    ObservedExact,
    Conflict,
    Unknown,
    Exhausted,
}
#[allow(dead_code)]
impl HeartbeatWriteResult {
    pub(crate) fn into_acknowledged(self) -> Option<AcknowledgedHeartbeat> {
        match self {
            Self::Acknowledged(receipt) => Some(receipt),
            _ => None,
        }
    }
}
macro_rules! redacted {
    ($($ty:ty),*) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(stringify!($ty))
        }
    })*};
}
redacted!(
    HeartbeatBinding,
    HeartbeatSample,
    HeartbeatWriter,
    AcknowledgedHeartbeat,
    LoadedHeartbeat,
    HeartbeatWriteResult
);
fn member_error(error: MemberError) -> StorageError {
    match error {
        MemberError::InvalidRecord => StorageError::InvalidCatalog,
        MemberError::LimitExceeded => StorageError::LimitExceeded,
    }
}
fn check_deadline(deadline: Instant) -> Result<(), StorageError> {
    if time::now() >= deadline {
        Err(StorageError::Unavailable)
    } else {
        Ok(())
    }
}
impl HeartbeatBinding {
    fn record(&self, sample: &HeartbeatSample, sequence: u64) -> Heartbeat {
        Heartbeat {
            available_threads: sample.available_threads,
            available_memory_bytes: sample.available_memory_bytes,
            resources: self.resources,
            version: 1,
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            incarnation: self.incarnation,
            sequence,
            operation_slots: self.operation_slots,
            partition_slots: self.partition_slots,
            queue_size: self.queue_size,
            running_operations: sample.running_operations,
            running_partitions: sample.running_partitions,
            queued_operations: sample.queued_operations,
            queued_partitions: sample.queued_partitions,
            drain_id: sample.drain_id,
            accepting: sample.accepting,
        }
    }
    fn validate(&self) -> Result<(), StorageError> {
        if self.verifier.len() != 64
            || !self
                .verifier
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StorageError::InvalidCatalog);
        }
        self.record(
            &HeartbeatSample {
                available_threads: None,
                available_memory_bytes: None,
                running_operations: 0,
                running_partitions: 0,
                queued_operations: 0,
                queued_partitions: 0,
                drain_id: None,
                accepting: false,
            },
            1,
        )
        .validate()
        .map_err(member_error)
    }
    fn matches(&self, h: &Heartbeat) -> bool {
        h.cluster_id == self.cluster_id
            && h.node_id == self.node_id
            && h.incarnation == self.incarnation
            && h.operation_slots == self.operation_slots
            && h.partition_slots == self.partition_slots
            && h.queue_size == self.queue_size
            && h.resources == self.resources
    }
}
/// Definite absence is Ok(None). All other failures retain their sanitized category.
/// Session/verifier are retained local evidence; the wire has no such fields.
#[allow(dead_code)] // C03d consumes this complete named-key read.
pub(crate) async fn load_heartbeat<B: ObjectStore>(
    storage: &Storage<B>,
    binding: &HeartbeatBinding,
    deadline: Instant,
) -> Result<Option<LoadedHeartbeat>, StorageError> {
    binding.validate()?;
    check_deadline(deadline)?;
    let object = timeout_at(
        deadline,
        storage.read(
            &ObjectKey::heartbeat(binding.incarnation),
            MAX_HEARTBEAT_BYTES,
        ),
    )
    .await
    .map_err(|_| StorageError::Unavailable)?;
    check_deadline(deadline)?;
    let object = match object {
        Ok(object) => object,
        Err(StorageError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    if object.etag.is_empty() {
        return Err(StorageError::Unavailable);
    }
    let heartbeat = Heartbeat::decode(&object.bytes).map_err(member_error)?;
    if !binding.matches(&heartbeat) {
        return Err(StorageError::InvalidCatalog);
    }
    check_deadline(deadline)?;
    Ok(Some(LoadedHeartbeat {
        binding: binding.clone(),
        heartbeat,
        token: object.etag,
    }))
}
#[allow(dead_code)] // Future runtime must retain exactly one writer for this boot.
impl HeartbeatWriter {
    pub(crate) fn new(binding: HeartbeatBinding) -> Result<Self, StorageError> {
        binding.validate()?;
        Ok(Self {
            binding,
            token: None,
            attempted: 0,
            observed: 0,
            confirmed: None,
            pending: None,
            reconcile: false,
        })
    }
    fn install(&mut self, loaded: Option<LoadedHeartbeat>) {
        if let Some(loaded) = loaded {
            self.observed = self.observed.max(loaded.heartbeat.sequence);
            self.token = Some(loaded.token);
        } else {
            self.token = None;
        }
    }
    pub(crate) async fn publish<B: ObjectStore>(
        &mut self,
        storage: &Storage<B>,
        sample: HeartbeatSample,
        deadline: Instant,
    ) -> Result<HeartbeatWriteResult, StorageError> {
        // Validate even before reconciliation. Callers cannot submit identity or sequence.
        self.binding
            .record(&sample, 1)
            .validate()
            .map_err(member_error)?;
        check_deadline(deadline)?;
        let reloaded = self.reconcile;
        if reloaded {
            let loaded = match load_heartbeat(storage, &self.binding, deadline).await {
                Ok(loaded) => loaded,
                Err(_) if self.pending.is_some() => return Ok(HeartbeatWriteResult::Unknown),
                Err(error) => return Err(error),
            };
            if loaded
                .as_ref()
                .is_some_and(|loaded| self.pending.as_ref() == Some(&loaded.heartbeat))
            {
                self.pending = None;
            }
            self.install(loaded);
        }
        let Some(sequence) = self.attempted.max(self.observed).checked_add(1) else {
            return Ok(if self.pending.is_some() {
                HeartbeatWriteResult::Unknown
            } else {
                HeartbeatWriteResult::Exhausted
            });
        };
        let candidate = self.binding.record(&sample, sequence);
        let bytes = candidate.encode().map_err(member_error)?;
        check_deadline(deadline)?;
        // Set before polling a future that may enter the provider. Cancellation
        // cannot restore a direct-write condition or forget an attempted sequence.
        self.attempted = sequence;
        self.pending = Some(candidate.clone());
        self.reconcile = true;
        let condition = self
            .token
            .as_deref()
            .map_or(WriteCondition::Create, WriteCondition::Match);
        let result = timeout_at(
            deadline,
            storage.put(
                &ObjectKey::heartbeat(self.binding.incarnation),
                bytes,
                condition,
            ),
        )
        .await;
        let conflict = match result {
            Ok(Ok(token)) if !token.is_empty() && time::now() < deadline => {
                self.token = Some(token);
                self.confirmed = Some(sample.clone());
                self.pending = None;
                self.reconcile = false;
                return Ok(HeartbeatWriteResult::Acknowledged(AcknowledgedHeartbeat {
                    binding: self.binding.clone(),
                    sample,
                    sequence,
                }));
            }
            Ok(Err(StorageError::Conflict)) => {
                self.pending = None;
                true
            }
            Ok(Err(StorageError::Ambiguous)) | Ok(Ok(_)) | Err(_) => false,
            Ok(Err(error)) => {
                self.pending = None;
                return Err(error);
            }
        };
        if reloaded {
            return Ok(if conflict {
                HeartbeatWriteResult::Conflict
            } else {
                HeartbeatWriteResult::Unknown
            });
        }
        let loaded = load_heartbeat(storage, &self.binding, deadline).await;
        if let Ok(loaded) = loaded {
            let exact = loaded.as_ref().is_some_and(|h| h.heartbeat == candidate);
            self.install(loaded);
            if !conflict && exact {
                self.confirmed = Some(sample);
                self.pending = None;
                // Readback is evidence of bytes, never an acknowledgement receipt.
                return Ok(HeartbeatWriteResult::ObservedExact);
            }
        }
        Ok(if conflict {
            HeartbeatWriteResult::Conflict
        } else {
            HeartbeatWriteResult::Unknown
        })
    }
}
#[cfg(test)]
mod tests;

mod observation;
#[allow(unused_imports)] // Pending C06b, C07b/c and coordinator consumers.
pub(crate) use observation::{AutoscaleMember, AutoscaleSnapshot, DrainHeartbeat, Observations};
