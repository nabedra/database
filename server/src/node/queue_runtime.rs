//! Worker memory owns accepted requests independently of API connections.
use super::{
    http::{self, Result, invalid, unavailable},
    queue_transport,
    runtime::Worker,
};
use crate::{
    cluster::{
        member::{Node, NodeState, Registration},
        state::Control,
    },
    request_queue::{
        QueueLimits, QueueSnapshot, Receipt, RequestKey, RequestQueue, RequestRecord, RequestState,
    },
    storage::{CatalogFence, ClusterCatalog, LoadedClusterCatalog},
};
use axum::{
    body::Body,
    extract::State,
    http::{Request, Response},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, UNIX_EPOCH},
};
use uuid::Uuid;

// The last possible cursor has no successor in a legacy queue snapshot.
pub(super) const STREAM_CAPABILITY_CURSOR: u64 = u64::MAX;

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JoinBase {
    cluster: Uuid,
    revision: u64,
    change_id: Uuid,
}

impl JoinBase {
    pub(crate) fn of(root: &Control) -> Self {
        Self {
            cluster: root.cluster_id,
            revision: root.revision,
            change_id: root.change_id,
        }
    }

    fn is_fenced_by(self, successor: Self) -> bool {
        self.cluster == successor.cluster && self.revision < successor.revision
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Command {
    Submit {
        body: Vec<u8>,
        #[serde(default)]
        context: Option<crate::storage::context::RequestContext>,
    },
    Stats,
    Seal,
    Lookup {
        run_id: Uuid,
        request_id: Uuid,
        #[serde(default)]
        context: Option<crate::storage::context::RequestContext>,
    },
    Install {
        record: RequestRecord,
    },
    Snapshot {
        after: Option<u64>,
    },
    Bootstrap {
        snapshot: QueueSnapshot,
    },
    Join {
        node_id: Uuid,
    },
    FencedJoin {
        node_id: Uuid,
        base: JoinBase,
    },
    Unseal {
        token: Uuid,
    },
}
impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::Submit { .. } => "submit",
            Self::Lookup { .. } => "lookup",
            Self::Install { .. } => "install",
            Self::Snapshot { .. } => "snapshot",
            Self::Bootstrap { .. } => "bootstrap",
            Self::Stats => "stats",
            Self::Seal => "seal",
            Self::Join { .. } => "join",
            Self::FencedJoin { .. } => "fenced_join",
            Self::Unseal { .. } => "unseal",
        }
    }
}
fn decision_receipt(
    run_id: Uuid,
    request_id: Uuid,
    decision: &crate::storage::Decision,
) -> Result<Receipt> {
    let value: serde_json::Value =
        serde_json::from_str(&decision.response_body).map_err(|_| invalid())?;
    Ok(Receipt {
        run_id,
        request_id,
        state: if decision.response_status == 200 {
            RequestState::Succeeded
        } else {
            RequestState::Failed
        },
        result: value.get("data").filter(|v| !v.is_null()).cloned(),
        error: value
            .get("error")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|_| invalid())?,
    })
}
fn reconcile_attempt_error(
    reconcile_mutation: bool,
    error: crate::protocol::ProtocolError,
) -> crate::protocol::ProtocolError {
    if reconcile_mutation && error.outcome == crate::protocol::Outcome::NotApplied {
        crate::protocol::ProtocolError::new(
            crate::protocol::ErrorCode::OutcomeUnknown,
            "accepted mutation requires durable reconciliation",
            crate::protocol::Outcome::Unknown,
        )
    } else {
        error
    }
}
/// Durable reconciliation may race terminal replication or another lookup.
/// Preserve an identical terminal version and its original completion time/ACKs.
fn settle_reconciled_decision(
    queue: &mut RequestQueue,
    observed: &RequestRecord,
    decision: &crate::storage::Decision,
    now: u64,
) -> std::result::Result<RequestRecord, crate::request_queue::QueueError> {
    use crate::request_queue::QueueError;
    let key = observed.identity();
    let current = queue.record(key.clone()).ok_or(QueueError::NotFound)?;
    if !observed.accepted
        || !current.accepted
        || current.identity() != key
        || current.run_id != observed.run_id
        || current.scheduler_term != observed.scheduler_term
        || current.membership_revision != observed.membership_revision
        || current.sequence != observed.sequence
        || current.body != observed.body
        || current.digest != observed.digest
        || current.context != observed.context
        || decision.request_id != current.request_id
        || http::digest(decision.request_body.as_bytes()) != current.digest
    {
        return Err(QueueError::Conflict);
    }
    if current.state.terminal() {
        if current.response_status != Some(decision.response_status)
            || current.response_body.as_deref() != Some(decision.response_body.as_bytes())
            || current.result_expired
        {
            return Err(QueueError::Conflict);
        }
        return Ok(current.clone());
    }
    queue.finish(
        key,
        decision.response_status,
        decision.response_body.as_bytes().to_vec(),
        now,
    )
}

fn validate_context_mode(
    root: &Control,
    context: Option<&crate::storage::context::RequestContext>,
    now: u64,
) -> Result<()> {
    crate::request_queue::admission::validate_context(&root.template, context, now)
}
fn validate_command_class(command: &Command, expected: &str, class: &str) -> Result<()> {
    if command.name() != expected
        || (queue_transport::command_class(command.name())? != class
            && !(command.name() == "install" && class == "bootstrap"))
    {
        return Err(invalid());
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Reply {
    Record {
        record: RequestRecord,
    },
    Receipt {
        receipt: Receipt,
    },
    Snapshot {
        snapshot: QueueSnapshot,
        done: bool,
    },
    StreamCapable,
    Stats {
        pending: u64,
        running: u64,
        oldest_wait_ms: u64,
        queue_bytes: u64,
        result_bytes: u64,
    },
    Joined {
        token: Uuid,
    },
    FencedJoined {
        base: JoinBase,
    },
    Error {
        error: crate::protocol::ProtocolError,
    },
}
pub(super) struct QueueRuntime {
    pub(super) streams: Arc<super::stream::Registry>,
    queue: Mutex<Option<RequestQueue>>,
    retained_data: Mutex<BTreeMap<RequestKey, super::runtime::DataGrant>>,
    pub(super) seen: Mutex<BTreeMap<Uuid, u64>>,
    active: Mutex<BTreeSet<RequestKey>>,
    changes: Arc<tokio::sync::Mutex<()>>,
    stream_changes: Arc<tokio::sync::Mutex<()>>,
    seals: Mutex<BTreeMap<Uuid, QueueSeal>>,
    changed: tokio::sync::Notify,
    retries: Mutex<BTreeMap<RequestKey, (u32, tokio::time::Instant)>>,
    first_seen: Mutex<BTreeMap<RequestKey, u64>>,
    ingress: Arc<tokio::sync::Semaphore>,
    receipts: Arc<tokio::sync::Semaphore>,
    replicas: Arc<tokio::sync::Semaphore>,
    bootstrap: Arc<tokio::sync::Semaphore>,
    progress: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone, Copy)]
enum SealRelease {
    Deadline(tokio::time::Instant),
    Successor { base: JoinBase, joiner: Uuid },
}

struct QueueSeal {
    release: SealRelease,
    _changes: tokio::sync::OwnedMutexGuard<()>,
    _stream_changes: tokio::sync::OwnedMutexGuard<()>,
}

impl QueueRuntime {
    pub(super) fn new() -> Self {
        Self {
            streams: Arc::new(super::stream::Registry::new()),
            queue: Mutex::new(None),
            retained_data: Mutex::new(BTreeMap::new()),
            seen: Mutex::new(BTreeMap::new()),
            active: Mutex::new(BTreeSet::new()),
            changes: Arc::new(tokio::sync::Mutex::new(())),
            stream_changes: Arc::new(tokio::sync::Mutex::new(())),
            seals: Mutex::new(BTreeMap::new()),
            changed: tokio::sync::Notify::new(),
            retries: Mutex::new(BTreeMap::new()),
            first_seen: Mutex::new(BTreeMap::new()),
            ingress: Arc::new(tokio::sync::Semaphore::new(8)),
            receipts: Arc::new(tokio::sync::Semaphore::new(8)),
            replicas: Arc::new(tokio::sync::Semaphore::new(8)),
            bootstrap: Arc::new(tokio::sync::Semaphore::new(4)),
            progress: Arc::new(tokio::sync::Semaphore::new(8)),
        }
    }
    #[cfg(test)]
    pub(super) fn install_fixture_queue(&self, queue: RequestQueue) {
        *self.queue.lock().unwrap() = Some(queue);
    }
    pub(super) async fn lock_changes(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_unsealed(&self.changes).await
    }
    pub(super) async fn lock_stream_changes(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_unsealed(&self.stream_changes).await
    }
    async fn lock_unsealed(
        &self,
        changes: &Arc<tokio::sync::Mutex<()>>,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            // Register before inspecting seals so a seal installed by an owner
            // currently copying a join cannot lose its expiration notification.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let nearest = self.reap_timed_seals();
            let expiration = async {
                match nearest {
                    Some(until) => nabedra_rt::time::sleep_until(until).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                guard = changes.clone().lock_owned() => return guard,
                _ = changed => {},
                _ = expiration => {},
            }
        }
    }
    fn retain_seal(
        &self,
        changes: tokio::sync::OwnedMutexGuard<()>,
        stream_changes: tokio::sync::OwnedMutexGuard<()>,
    ) -> Uuid {
        self.insert_seal(
            changes,
            stream_changes,
            SealRelease::Deadline(nabedra_rt::time::now() + Duration::from_secs(30)),
        )
    }

    fn insert_seal(
        &self,
        changes: tokio::sync::OwnedMutexGuard<()>,
        stream_changes: tokio::sync::OwnedMutexGuard<()>,
        release: SealRelease,
    ) -> Uuid {
        let token = nabedra_rt::ids::uuid_v4();
        self.seals.lock().unwrap().insert(
            token,
            QueueSeal {
                release,
                _changes: changes,
                _stream_changes: stream_changes,
            },
        );
        self.changed.notify_waiters();
        token
    }

    fn reap_timed_seals(&self) -> Option<tokio::time::Instant> {
        let mut seals = self.seals.lock().unwrap();
        let now = nabedra_rt::time::now();
        seals.retain(
            |_, seal| !matches!(seal.release, SealRelease::Deadline(until) if until <= now),
        );
        seals
            .values()
            .filter_map(|seal| match seal.release {
                SealRelease::Deadline(until) => Some(until),
                SealRelease::Successor { .. } => None,
            })
            .min()
    }

    fn fenced_join(&self) -> Option<(Uuid, JoinBase, Uuid)> {
        self.seals
            .lock()
            .unwrap()
            .iter()
            .find_map(|(token, seal)| match seal.release {
                SealRelease::Successor { base, joiner } => Some((*token, base, joiner)),
                SealRelease::Deadline(_) => None,
            })
    }

    fn release_join(&self, token: Uuid, successor: JoinBase) {
        let mut seals = self.seals.lock().unwrap();
        if seals.get(&token).is_some_and(|seal| {
            matches!(seal.release, SealRelease::Successor { base, .. } if base.is_fenced_by(successor))
        }) {
            seals.remove(&token);
            self.changed.notify_waiters();
        }
    }

    pub(super) fn transport(&self, class: &str) -> Result<tokio::sync::OwnedSemaphorePermit> {
        match class {
            "ingress" => self.ingress.clone(),
            "receipt" => self.receipts.clone(),
            "replica" => self.replicas.clone(),
            "bootstrap" => self.bootstrap.clone(),
            "progress" => self.progress.clone(),
            _ => return Err(invalid()),
        }
        .try_acquire_owned()
        .map_err(|_| http::failure(crate::protocol::ErrorCode::Overloaded))
    }
    pub(super) fn has_pending(&self) -> bool {
        self.queue
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|q| q.pending() > 0)
    }
    pub(super) fn checked_execution(
        &self,
        id: impl Into<RequestKey>,
        digest: &str,
    ) -> Result<CatalogFence> {
        let id = id.into();
        let lock = self.queue.lock().unwrap();
        let q = lock.as_ref().ok_or_else(unavailable)?;
        let record = q.record(id.clone()).ok_or_else(unavailable)?;
        if !record.accepted
            || record.state != RequestState::Running
            || record.digest != digest
            || !q.ready(id)
        {
            return Err(unavailable());
        }
        Ok(CatalogFence {
            run_id: record.run_id,
            scheduler_term: record.scheduler_term,
        })
    }
    pub(super) fn current_binding(
        &self,
        id: impl Into<RequestKey>,
    ) -> Result<super::job::QueueBinding> {
        let id = id.into();
        let lock = self.queue.lock().unwrap();
        let q = lock.as_ref().ok_or_else(unavailable)?;
        let record = q.record(id.clone()).ok_or_else(unavailable)?;
        if !record.accepted || record.state != RequestState::Running || !q.ready(id) {
            return Err(unavailable());
        }
        let (storage_digest, grant_digest) = record.binding_digests().map_err(|_| invalid())?;
        Ok(super::job::QueueBinding {
            storage_digest,
            grant_digest,
            run_id: record.run_id,
            scheduler_term: record.scheduler_term,
            membership_revision: record.membership_revision,
            sequence: record.sequence,
        })
    }
    pub(super) fn record(&self, id: impl Into<RequestKey>) -> Result<RequestRecord> {
        self.queue
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(unavailable)?
            .record(id)
            .cloned()
            .ok_or_else(unavailable)
    }
    pub(super) fn record_for_job(&self, job: &super::job::JobEnvelope) -> Result<RequestRecord> {
        let lock = self.queue.lock().unwrap();
        let q = lock.as_ref().ok_or_else(unavailable)?;
        let binding = job.queue_binding.ok_or_else(unavailable)?;
        let record = q
            .records()
            .find(|record| {
                record.request_id == job.operation_id
                    && record.binding_digests().is_ok_and(|(storage, grant)| {
                        storage == binding.storage_digest && grant == binding.grant_digest
                    })
            })
            .ok_or_else(unavailable)?;
        let fence = CatalogFence {
            run_id: q.run_id(),
            scheduler_term: q.scheduler_term(),
        };
        if !q.ready(record.identity()) {
            return Err(unavailable());
        }
        super::job::validate_queue_binding(job, record, fence).map_err(|_| unavailable())?;
        Ok(record.clone())
    }
    pub(super) fn validate_job(
        &self,
        job: &super::job::JobEnvelope,
        fence: CatalogFence,
    ) -> Result<()> {
        let record = self.record_for_job(job)?;
        super::job::validate_queue_binding(job, &record, fence).map_err(|_| unavailable())
    }
    pub(super) fn request_sequence(&self, id: impl Into<RequestKey>) -> Option<u64> {
        let id = id.into();
        self.queue
            .lock()
            .unwrap()
            .as_ref()?
            .record(id)
            .map(|r| r.sequence)
    }
    pub(super) fn pinned(&self, id: impl Into<RequestKey>) -> Option<LoadedClusterCatalog> {
        let id = id.into();
        self.queue
            .lock()
            .unwrap()
            .as_ref()?
            .record(id)?
            .pinned_catalog
            .as_ref()
            .and_then(|b| decode_pin(b).ok())
    }
    pub(super) fn fence(&self) -> Result<CatalogFence> {
        let lock = self.queue.lock().unwrap();
        let queue = lock.as_ref().ok_or_else(unavailable)?;
        Ok(CatalogFence {
            run_id: queue.run_id(),
            scheduler_term: queue.scheduler_term(),
        })
    }
}
/// OLAP-5: a denial is `unauthorized`; a slow or failed probe is retryable.
fn access_failure(error: crate::storage::ObjectAccessError) -> crate::protocol::ProtocolError {
    match error {
        crate::storage::ObjectAccessError::Denied => {
            http::failure(crate::protocol::ErrorCode::Unauthorized)
        }
        crate::storage::ObjectAccessError::Storage(_) => unavailable(),
    }
}
fn encode_pin(loaded: &LoadedClusterCatalog) -> Result<Vec<u8>> {
    let catalog = loaded.catalog().encode().map_err(|_| invalid())?;
    let etag = loaded.etag().as_bytes();
    if etag.len() > 4096 {
        return Err(invalid());
    }
    let mut bytes = Vec::with_capacity(4 + etag.len() + catalog.len());
    bytes.extend_from_slice(&(etag.len() as u32).to_be_bytes());
    bytes.extend_from_slice(etag);
    bytes.extend_from_slice(&catalog);
    Ok(bytes)
}
fn decode_pin(bytes: &[u8]) -> Result<LoadedClusterCatalog> {
    let prefix: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > 4096 {
        return Err(invalid());
    }
    let etag = std::str::from_utf8(bytes.get(4..4 + length).ok_or_else(invalid)?)
        .map_err(|_| invalid())?
        .to_owned();
    let catalog = ClusterCatalog::decode(bytes.get(4 + length..).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    LoadedClusterCatalog::new(catalog, etag).map_err(|_| invalid())
}
pub(crate) fn now_ms() -> u64 {
    nabedra_rt::time::wall_now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
pub(crate) fn members(root: &Control) -> BTreeSet<Uuid> {
    root.nodes
        .iter()
        .filter(|n| n.state == NodeState::Ready && n.registration == Registration::Verified)
        .map(|n| n.node_id)
        .collect()
}
pub(crate) fn scheduler(root: &Control) -> Result<&Node> {
    let id = root.scheduler.coordinator_node.ok_or_else(unavailable)?;
    root.nodes
        .iter()
        .find(|n| {
            n.node_id == id
                && n.state == NodeState::Ready
                && n.registration == Registration::Verified
        })
        .ok_or_else(unavailable)
}
pub(crate) fn coordination_capacity(
    template: &crate::cluster::configuration::Template,
) -> Result<usize> {
    let budget = (template.worker_resources().control_memory_reserve_bytes / 4).min(16 << 20);
    let slots = template
        .execution
        .max_pending_requests
        .min(budget / (64 << 10))
        .min(256);
    if slots == 0 {
        return Err(invalid());
    }
    Ok(slots as usize)
}
pub(crate) fn job_coordination_capacity(
    template: &crate::cluster::configuration::Template,
) -> Result<usize> {
    let slots = coordination_capacity(template)?.max(
        usize::try_from(template.worker_resources().max_threads)
            .map_err(|_| invalid())?
            .checked_mul(2)
            .and_then(|n| n.checked_add(2))
            .ok_or_else(invalid)?,
    );
    if slots > 1024 {
        return Err(invalid());
    }
    Ok(slots)
}
pub(crate) fn outgoing_capacity(
    template: &crate::cluster::configuration::Template,
) -> Result<usize> {
    let slots = (template.worker_resources().control_memory_reserve_bytes / 8 / (32 << 10))
        .min(8192) as usize;
    if slots < 16 {
        return Err(invalid());
    }
    Ok(slots)
}
pub(crate) fn coordination_bytes(
    template: &crate::cluster::configuration::Template,
) -> Result<u64> {
    let bytes = (coordination_capacity(template)? as u64
        + job_coordination_capacity(template)? as u64)
        * (64 << 10)
        + outgoing_capacity(template)? as u64 * (32 << 10);
    if bytes > template.worker_resources().control_memory_reserve_bytes / 2 {
        return Err(invalid());
    }
    Ok(bytes)
}
pub(crate) fn validate_coordination_capacity(
    root: &Control,
    joining: Option<(Uuid, crate::config::WorkerResources)>,
    desired: Option<u64>,
) -> Result<usize> {
    let coordinator = root
        .scheduler
        .coordinator_node
        .and_then(|id| root.nodes.iter().find(|n| n.node_id == id))
        .and_then(|n| n.resources)
        .or_else(|| {
            root.nodes
                .iter()
                .find(|n| n.state == NodeState::Ready)
                .and_then(|n| n.resources)
        })
        .or_else(|| joining.map(|(_, r)| r))
        .unwrap_or(root.template.worker_resources());
    let outgoing = (coordinator.control_memory_reserve_bytes / 8 / (32 << 10)).min(8192) as usize;
    let cap = root.template.execution.max_workers_per_request as usize;
    let mut threads = root
        .nodes
        .iter()
        .filter(|n| n.state == NodeState::Ready && joining.is_none_or(|(id, _)| id != n.node_id))
        .map(|n| n.resources.map_or(n.partition_slots, |r| r.max_threads) as usize)
        .collect::<Vec<_>>();
    if let Some((_, resource)) = joining {
        threads.push(resource.max_threads as usize);
    }
    if let Some(desired) = desired {
        while threads.len() < (desired as usize).min(cap) {
            threads.push(root.template.worker_resources().max_threads as usize);
        }
    }
    threads.sort_unstable_by(|a, b| b.cmp(a));
    let cpu = threads
        .into_iter()
        .take(cap)
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .ok_or_else(invalid)?;
    let frontier = cpu.checked_mul(2).ok_or_else(invalid)?;
    if frontier > outgoing.saturating_sub(2) / 3 {
        let minimum = cpu
            .checked_mul(6)
            .and_then(|n| n.checked_add(2))
            .and_then(|n| n.checked_mul(32 << 10))
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(invalid)?;
        return Err(crate::protocol::ProtocolError::new(
            crate::protocol::ErrorCode::UnsupportedOperation,
            format!(
                "coordinator control_memory_reserve_bytes must be at least {minimum} for the advertised worker capacity"
            ),
            crate::protocol::Outcome::NotApplied,
        ));
    }
    Ok(frontier.max(1))
}
pub(crate) fn progress_bytes(template: &crate::cluster::configuration::Template) -> u64 {
    (template.worker_resources().control_memory_reserve_bytes / 8).min(16 << 20)
}
fn limits(worker: &Worker) -> QueueLimits {
    let c = &worker.ready.prepared.input.value.template.execution;
    QueueLimits {
        max_pending_requests: c.max_pending_requests as usize,
        max_queue_bytes_per_replica: c.max_queue_bytes_per_replica as usize,
        max_retained_result_bytes_per_worker: c.max_retained_result_bytes_per_worker as usize,
        result_retention_ms: c.result_retention_ms,
    }
}
impl Worker {
    /// Preserve the original accepted grant until actual terminal settlement.
    /// Polling credentials never enter this map. Expiry is checked even when a
    /// client was retained, so a fresh receipt cannot renew queued work.
    pub(super) async fn queue_data_for_record(
        &self,
        record: &RequestRecord,
    ) -> Result<super::runtime::DataGrant> {
        if let Some(context) = &record.context {
            context
                .validate(now_ms() / 1000)
                .map_err(|_| http::failure(crate::protocol::ErrorCode::Unauthorized))?;
        }
        if let Some(grant) = self
            .queue
            .retained_data
            .lock()
            .unwrap()
            .get(&record.identity())
            .cloned()
        {
            return Ok(grant);
        }
        let grant = self.data_for_record(record).await?;
        if record.state.terminal() {
            return Ok(grant);
        }
        let mut grants = self.queue.retained_data.lock().unwrap();
        Ok(grants.entry(record.identity()).or_insert(grant).clone())
    }
    pub(super) async fn owned_catalog(
        &self,
        root: &Control,
        grant: &super::runtime::DataGrant,
    ) -> Result<LoadedClusterCatalog> {
        let storage = grant.storage();
        if root.template.mode.is_fixed() {
            return storage
                .load_cluster_catalog()
                .await
                .map_err(|_| unavailable());
        }
        let fence = self.queue.fence()?;
        let cold = match storage.load_cluster_catalog().await {
            Ok(loaded)
                if loaded
                    .catalog()
                    .fence
                    .is_some_and(|old| old.run_id != fence.run_id) =>
            {
                self.initialization_authorized(fence).await?
            }
            Ok(_) | Err(crate::storage::StorageError::NotFound) => false,
            Err(_) => return Err(unavailable()),
        };
        storage
            .initialize_owned_cluster_catalog(root.cluster_id, fence, cold)
            .await
            .map_err(|_| unavailable())
    }
    pub(super) async fn initialize_queue(&self) -> Result<()> {
        let root = self.root().await?;
        let local = self.signer.sender().node_id().ok_or_else(invalid)?;
        let loaded = self
            .storage
            .load_cluster_catalog()
            .await
            .map_err(|_| unavailable())?;
        let fence = loaded.catalog().fence.ok_or_else(unavailable)?;
        if fence.scheduler_term != root.scheduler.scheduler_term {
            return Err(unavailable());
        }
        let ready = members(&root);
        let mut restored = None;
        for node in root
            .nodes
            .iter()
            .filter(|n| ready.contains(&n.node_id) && n.node_id != local)
        {
            let mut after = None;
            let mut copied: Option<QueueSnapshot> = None;
            loop {
                match self
                    .queue_exchange(&root, node, &Command::Snapshot { after })
                    .await
                {
                    Ok(Reply::Snapshot { snapshot, done }) => {
                        if let Some(existing) = copied.as_mut() {
                            if existing.run_id != snapshot.run_id
                                || existing.scheduler_term != snapshot.scheduler_term
                                || existing.membership_revision != snapshot.membership_revision
                            {
                                copied = None;
                                break;
                            }
                            existing.records.extend(snapshot.records);
                        } else {
                            copied = Some(snapshot);
                        }
                        if done {
                            break;
                        }
                        after = copied
                            .as_ref()
                            .and_then(|s| s.records.last().map(|r| r.sequence));
                    }
                    _ => {
                        copied = None;
                        break;
                    }
                }
            }
            if copied.is_some() {
                match self.load_stream_replicas(&root, node, fence.run_id).await {
                    Ok(()) => {
                        restored = copied;
                        break;
                    }
                    Err(_) => {}
                }
            }
        }
        // A committed new run has no worker copies until its first enrollment.
        // Existing ready peers must supply a complete copy before this node initializes.
        if restored.is_none() && !self.initialization_authorized(fence).await? {
            return Err(unavailable());
        }
        self.queue
            .streams
            .set_run(fence.run_id, restored.is_none())?;
        let mut initial = ready.clone();
        initial.insert(local);
        let mut q = if let Some(snapshot) = restored {
            if snapshot.run_id != fence.run_id {
                return Err(unavailable());
            }
            let mut q = RequestQueue::new(
                snapshot.run_id,
                snapshot.scheduler_term,
                snapshot.membership_revision,
                limits(self),
                snapshot.members.clone(),
            )
            .map_err(|e| e.protocol())?;
            q.install_snapshot(snapshot).map_err(|e| e.protocol())?;
            q
        } else {
            RequestQueue::new(
                fence.run_id,
                fence.scheduler_term,
                root.revision,
                limits(self),
                initial,
            )
            .map_err(|e| e.protocol())?
        };
        q.expire(now_ms());
        *self.queue.queue.lock().unwrap() = Some(q);
        Ok(())
    }
    async fn initialization_authorized(&self, fence: CatalogFence) -> Result<bool> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Authorization {
            fence: Option<CatalogFence>,
            initialization_allowed: bool,
        }
        for endpoint in &self.ready.prepared.input.value.template.api_urls {
            let request = Request::builder()
                .method("GET")
                .uri(format!(
                    "{}/internal/v1/queue/run",
                    endpoint.trim_end_matches('/')
                ))
                .header("connection", "close")
                .header(
                    "authorization",
                    format!(
                        "Bearer {}",
                        self.ready.prepared.input.value.enrollment_token
                    ),
                )
                .body(Body::empty())
                .map_err(|_| invalid())?;
            if let Ok(response) = self
                .client
                .request(request, nabedra_rt::time::now() + Duration::from_secs(5))
                .await
            {
                if response.status() != 200 {
                    continue;
                }
                let length = http::framing(response.headers(), 4096, false)?.ok_or_else(invalid)?;
                let body = http::read_small(
                    response.into_body(),
                    length,
                    nabedra_rt::time::now() + Duration::from_secs(5),
                )
                .await?;
                let value: Authorization = serde_json::from_slice(&body).map_err(|_| invalid())?;
                if value.fence == Some(fence) && value.initialization_allowed {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
    pub(super) async fn mark_reconciling(&self, id: impl Into<RequestKey>) -> Result<()> {
        let id = id.into();
        let _change = self.queue.lock_changes().await;
        let root = self.root().await?;
        let record = {
            let mut lock = self.queue.queue.lock().unwrap();
            let q = lock.as_mut().ok_or_else(unavailable)?;
            match q.record(id.clone()) {
                Some(record) if record.state.terminal() => return Ok(()),
                Some(record) if record.state == RequestState::Reconciling => record.clone(),
                Some(_) => q
                    .transition(id, RequestState::Reconciling)
                    .map_err(|e| e.protocol())?,
                None => return Err(unavailable()),
            }
        };
        self.replicate_record(&root, record).await?;
        self.queue.changed.notify_waiters();
        Ok(())
    }
    pub(super) async fn queue_exchange(
        &self,
        root: &Control,
        node: &Node,
        command: &Command,
    ) -> Result<Reply> {
        self.queue_exchange_class(root, node, command, None).await
    }
    async fn queue_exchange_class(
        &self,
        root: &Control,
        node: &Node,
        command: &Command,
        class: Option<&str>,
    ) -> Result<Reply> {
        let logical = match command {
            Command::Install { record } => {
                record
                    .body
                    .len()
                    .saturating_add(record.context_bytes().unwrap_or(usize::MAX))
                    + record.pinned_catalog.as_ref().map_or(0, Vec::len)
                    + record.response_body.as_ref().map_or(0, Vec::len)
            }
            Command::Submit { body, context } => body.len().saturating_add(
                context
                    .as_ref()
                    .and_then(|c| serde_json::to_vec(c).ok())
                    .map_or(0, |v| v.len()),
            ),
            _ => 0,
        };
        let class = class.unwrap_or(queue_transport::command_class(command.name())?);
        let encoded = queue_transport::encoded_len(command, queue_transport::MAX_BYTES)?;
        let capacity = self
            .ready
            .prepared
            .input
            .value
            .template
            .worker_resources()
            .control_memory_reserve_bytes
            .saturating_sub(coordination_bytes(
                &self.ready.prepared.input.value.template,
            )?)
            .saturating_sub(progress_bytes(&self.ready.prepared.input.value.template))
            .saturating_sub(1 << 20);
        let response_cap = match command {
            Command::Install { .. } => encoded.saturating_add(1024),
            Command::Snapshot { .. } => (capacity as usize).saturating_sub(encoded) / 2,
            _ => 64 << 10,
        }
        .min(queue_transport::MAX_BYTES);
        let budget = if matches!(command, Command::Snapshot { .. }) {
            encoded as u64 + 2 * response_cap as u64 + 128 * 1024
        } else {
            encoded as u64 + response_cap as u64 + 2 * logical as u64 + 128 * 1024
        };
        let _staging = if class == "bootstrap" {
            self.control_budget.charge_memory(0, budget)?
        } else if class == "progress" || class == "receipt" && budget <= 256 << 10 {
            self.progress_budget.charge_memory(0, budget)?
        } else {
            self.cpu.scheduler().charge_memory(0, budget)?
        };
        let body = queue_transport::encode_exact(command, encoded)?;
        let bytes = queue_transport::exchange_class(
            &self.client,
            &self.signer,
            root,
            node,
            body,
            response_cap,
            Some(class),
        )
        .await?;
        serde_json::from_slice(&bytes).map_err(|_| invalid())
    }
    async fn refresh_queue(&self, root: &Control) -> Result<()> {
        let catalog = self
            .storage
            .load_cluster_catalog()
            .await
            .map_err(|_| unavailable())?;
        let fence = catalog.catalog().fence.ok_or_else(unavailable)?;
        if fence.scheduler_term != root.scheduler.scheduler_term {
            return Err(unavailable());
        }
        let desired = members(root);
        if desired.is_empty() {
            return Err(unavailable());
        }
        let mut lock = self.queue.queue.lock().unwrap();
        let q = lock.as_mut().ok_or_else(unavailable)?;
        if q.run_id() != fence.run_id {
            return Err(unavailable());
        }
        if q.scheduler_term() != fence.scheduler_term || q.members() != &desired {
            q.takeover(fence.scheduler_term, root.revision, desired)
                .map_err(|e| e.protocol())?;
            self.queue.retries.lock().unwrap().clear();
        }
        Ok(())
    }
    pub(super) async fn replicate_record(
        &self,
        _root: &Control,
        record: RequestRecord,
    ) -> Result<RequestRecord> {
        self.replicate_record_class(_root, record, false).await
    }
    async fn replicate_record_class(
        &self,
        _root: &Control,
        mut record: RequestRecord,
        bootstrap: bool,
    ) -> Result<RequestRecord> {
        #[cfg(test)]
        if record.state.terminal() {
            let barrier = self
                .test_terminal_replication_barrier
                .lock()
                .unwrap()
                .take();
            if let Some((arrived, release)) = barrier {
                arrived.add_permits(1);
                release.acquire().await.map_err(|_| unavailable())?.forget();
            }
        }
        let fresh = self.root().await?;
        let root = &fresh;
        if root.scheduler.scheduler_term != record.scheduler_term
            || Some(scheduler(root)?.node_id) != self.signer.sender().node_id()
        {
            return Err(unavailable());
        }
        let own = self.signer.sender().node_id().ok_or_else(invalid)?;
        let required = {
            self.queue
                .queue
                .lock()
                .unwrap()
                .as_ref()
                .ok_or_else(unavailable)?
                .members()
                .clone()
        };
        for id in required {
            if id != own {
                let node = root
                    .nodes
                    .iter()
                    .find(|n| n.node_id == id)
                    .ok_or_else(unavailable)?;
                match self
                    .queue_exchange_class(
                        root,
                        node,
                        &Command::Install {
                            record: record.clone(),
                        },
                        if bootstrap { Some("bootstrap") } else { None },
                    )
                    .await?
                {
                    Reply::Record { record: installed }
                        if installed.identity() == record.identity()
                            && installed.context == record.context
                            && installed.digest == record.digest
                            && installed.version == record.version => {}
                    Reply::Error { error } => return Err(error),
                    _ => return Err(invalid()),
                }
            }
            record = self
                .queue
                .queue
                .lock()
                .unwrap()
                .as_mut()
                .ok_or_else(unavailable)?
                .acknowledge_version(record.identity(), record.version, id)
                .map_err(|e| e.protocol())?;
        }
        // Replicate acceptance evidence as well as complete payload bytes.
        for node in root
            .nodes
            .iter()
            .filter(|n| record.replicas.contains(&n.node_id) && n.node_id != own)
        {
            match self
                .queue_exchange_class(
                    root,
                    node,
                    &Command::Install {
                        record: record.clone(),
                    },
                    if bootstrap { Some("bootstrap") } else { None },
                )
                .await?
            {
                Reply::Record { record: installed }
                    if installed.identity() == record.identity()
                        && installed.context == record.context
                        && installed.digest == record.digest
                        && installed.version == record.version => {}
                Reply::Error { error } => return Err(error),
                _ => return Err(invalid()),
            }
        }
        if record.state.terminal() {
            self.queue
                .retained_data
                .lock()
                .unwrap()
                .remove(&record.identity());
        }
        Ok(record)
    }
    fn reply_working_bytes(&self, command: &Command) -> u64 {
        let record_bytes = |r: &RequestRecord| {
            r.body
                .len()
                .saturating_add(r.context_bytes().unwrap_or(usize::MAX))
                .saturating_add(r.pinned_catalog.as_ref().map_or(0, Vec::len))
                .saturating_add(r.response_body.as_ref().map_or(0, Vec::len))
        };
        let bytes = match command {
            Command::Install { record } => record_bytes(record).saturating_mul(2),
            Command::Submit { body, context } => body
                .len()
                .saturating_add(
                    context
                        .as_ref()
                        .and_then(|c| serde_json::to_vec(c).ok())
                        .map_or(0, |v| v.len()),
                )
                .saturating_mul(32),
            Command::Lookup {
                request_id,
                context,
                ..
            } => self
                .queue
                .queue
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|q| {
                    RequestKey::new(*request_id, context.as_ref())
                        .ok()
                        .and_then(|key| q.record(key))
                })
                .and_then(|r| r.response_body.as_ref())
                .map_or(0, |body| body.len().saturating_mul(32)),
            Command::Snapshot { after } => self
                .queue
                .queue
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|q| {
                    q.records()
                        .filter(|r| after.is_none_or(|after| r.sequence > after))
                        .min_by_key(|r| r.sequence)
                })
                .map_or(0, |r| record_bytes(r).saturating_mul(2)),
            _ => 0,
        };
        (bytes as u64).saturating_add(64 << 10)
    }
    pub(super) async fn queue_command(
        self: &Arc<Self>,
        root: &Control,
        sender: Option<Uuid>,
        command: Command,
    ) -> Result<Reply> {
        match command {
            Command::Stats => {
                let q = self.queue.queue.lock().unwrap();
                let q = q.as_ref().ok_or_else(unavailable)?;
                let now = now_ms();
                let mut first = self.queue.first_seen.lock().unwrap();
                let mut oldest = 0;
                for record in q.records().filter(|r| r.accepted && !r.state.terminal()) {
                    let seen = first.entry(record.identity()).or_insert(now);
                    oldest = oldest.max(now.saturating_sub(*seen));
                }
                first.retain(|id, _| q.record(id.clone()).is_some_and(|r| !r.state.terminal()));
                let (queue_bytes, result_bytes) = q.charged_bytes();
                Ok(Reply::Stats {
                    pending: q.pending() as u64,
                    running: self.queue.active.lock().unwrap().len() as u64,
                    oldest_wait_ms: oldest,
                    queue_bytes: queue_bytes as u64,
                    result_bytes: result_bytes as u64,
                })
            }
            Command::Seal => {
                if sender.is_some()
                    || Some(scheduler(root)?.node_id) != self.signer.sender().node_id()
                {
                    return Err(unavailable());
                }
                let stream_view = self.recover_streams_before_handoff(root).await?;
                // Handoffs lock streams before the ordinary queue so a
                // pending replica ACK leaves ordinary operations free to run.
                let stream_seal = self.queue.lock_stream_changes().await;
                let seal = self.queue.lock_changes().await;
                self.verify_stream_handoff(stream_view).await?;
                let token = self.queue.retain_seal(seal, stream_seal);
                Ok(Reply::Joined { token })
            }
            Command::Join { node_id } | Command::FencedJoin { node_id, .. } => {
                let join_base = match command {
                    Command::FencedJoin { base, .. } if base == JoinBase::of(root) => Some(base),
                    Command::FencedJoin { .. } => return Err(unavailable()),
                    _ => None,
                };
                if sender.is_some()
                    || Some(scheduler(root)?.node_id) != self.signer.sender().node_id()
                {
                    return Err(unavailable());
                }
                let stream_view = self.recover_streams_before_handoff(root).await?;
                let stream_seal = self.queue.lock_stream_changes().await;
                let seal = self.queue.lock_changes().await;
                self.verify_stream_handoff(stream_view).await?;
                self.refresh_queue(root).await?;
                let node = root
                    .nodes
                    .iter()
                    .find(|n| {
                        n.node_id == node_id
                            && n.state == NodeState::Starting
                            && n.registration == Registration::Registered
                    })
                    .ok_or_else(unavailable)?;
                let (mut metadata, _) = self
                    .queue
                    .queue
                    .lock()
                    .unwrap()
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .snapshot_page(None);
                metadata.records.clear();
                match self
                    .queue_exchange_class(
                        root,
                        node,
                        &Command::Bootstrap { snapshot: metadata },
                        Some("bootstrap"),
                    )
                    .await?
                {
                    Reply::Snapshot { .. } => {}
                    Reply::Error { error } => return Err(error),
                    _ => return Err(invalid()),
                }
                let mut after = None;
                loop {
                    let (page, done) = self
                        .queue
                        .queue
                        .lock()
                        .unwrap()
                        .as_ref()
                        .ok_or_else(unavailable)?
                        .snapshot_page(after);
                    for record in page.records {
                        after = Some(record.sequence);
                        match self
                            .queue_exchange_class(
                                root,
                                node,
                                &Command::Install { record },
                                Some("bootstrap"),
                            )
                            .await?
                        {
                            Reply::Record { .. } => {}
                            Reply::Error { error } => return Err(error),
                            _ => return Err(invalid()),
                        }
                    }
                    if done {
                        break;
                    }
                }
                self.copy_stream_replicas(root, node, self.queue.fence()?.run_id)
                    .await?;
                if let Some(base) = join_base {
                    // An ownership or membership change while copying already
                    // fenced this attempt. It cannot authorize a new-root CAS.
                    if JoinBase::of(&self.root().await?) != base {
                        return Err(unavailable());
                    }
                    self.queue.insert_seal(
                        seal,
                        stream_seal,
                        SealRelease::Successor {
                            base,
                            joiner: node_id,
                        },
                    );
                    Ok(Reply::FencedJoined { base })
                } else {
                    let token = self.queue.retain_seal(seal, stream_seal);
                    Ok(Reply::Joined { token })
                }
            }
            Command::Unseal { token } => {
                if sender.is_some() {
                    return Err(http::failure(crate::protocol::ErrorCode::Unauthorized));
                }
                if self
                    .queue
                    .seals
                    .lock()
                    .unwrap()
                    .get(&token)
                    .is_some_and(|seal| matches!(seal.release, SealRelease::Successor { .. }))
                {
                    return Err(invalid());
                }
                self.refresh_queue(root).await?;
                self.refresh_join_records(root).await?;
                self.queue
                    .seals
                    .lock()
                    .unwrap()
                    .remove(&token)
                    .ok_or_else(invalid)?;
                self.queue.changed.notify_waiters();
                Ok(Reply::Joined { token })
            }
            Command::Bootstrap { snapshot } => {
                for record in &snapshot.records {
                    validate_context_mode(root, record.context.as_ref(), 0)?;
                }
                if !snapshot.records.is_empty() {
                    return Err(invalid());
                }
                let own = self.signer.sender().node_id().ok_or_else(invalid)?;
                if sender != Some(scheduler(root)?.node_id)
                    || root
                        .nodes
                        .iter()
                        .find(|n| n.node_id == own)
                        .is_none_or(|n| n.state != NodeState::Starting)
                {
                    return Err(http::failure(crate::protocol::ErrorCode::Unauthorized));
                }
                let mut q = RequestQueue::new(
                    snapshot.run_id,
                    snapshot.scheduler_term,
                    snapshot.membership_revision,
                    limits(self),
                    snapshot.members.clone(),
                )
                .map_err(|e| e.protocol())?;
                q.install_snapshot(snapshot).map_err(|e| e.protocol())?;
                let snapshot = q.snapshot();
                *self.queue.queue.lock().unwrap() = Some(q);
                Ok(Reply::Snapshot {
                    snapshot,
                    done: true,
                })
            }
            Command::Snapshot {
                after: Some(STREAM_CAPABILITY_CURSOR),
            } => Ok(Reply::StreamCapable),
            Command::Snapshot { after } => {
                let q = self.queue.queue.lock().unwrap();
                let (snapshot, done) = q.as_ref().ok_or_else(unavailable)?.snapshot_page(after);
                Ok(Reply::Snapshot { snapshot, done })
            }
            Command::Install { record } => {
                validate_context_mode(root, record.context.as_ref(), 0)?;
                if sender != Some(scheduler(root)?.node_id) {
                    return Err(http::failure(crate::protocol::ErrorCode::Unauthorized));
                }
                let catalog = self
                    .storage
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?;
                if catalog.catalog().fence
                    != Some(CatalogFence {
                        run_id: record.run_id,
                        scheduler_term: record.scheduler_term,
                    })
                    || root.scheduler.scheduler_term != record.scheduler_term
                {
                    return Err(unavailable());
                }
                {
                    let mut lock = self.queue.queue.lock().unwrap();
                    let q = lock.as_mut().ok_or_else(unavailable)?;
                    if q.run_id() != record.run_id {
                        return Err(unavailable());
                    }
                    if q.scheduler_term() != record.scheduler_term
                        || q.members() != &members(root)
                        || q.membership_revision() != record.membership_revision
                    {
                        q.takeover(
                            record.scheduler_term,
                            record.membership_revision,
                            members(root),
                        )
                        .map_err(|e| e.protocol())?;
                    }
                }
                // Preflight count/bytes/immutable identity before reserving a
                // registry client. An invalid record cannot grow the grant map.
                self.queue
                    .queue
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or_else(unavailable)?
                    .install(record.clone())
                    .map_err(|e| e.protocol())?;
                if !record.state.terminal() && !record.accepted {
                    let authorized = async {
                        let grant = self.queue_data_for_record(&record).await?;
                        self.owned_catalog(root, &grant).await?;
                        Ok::<(), crate::protocol::ProtocolError>(())
                    }
                    .await;
                    if let Err(error) = authorized {
                        let _ = self
                            .queue
                            .queue
                            .lock()
                            .unwrap()
                            .as_mut()
                            .ok_or_else(unavailable)?
                            .discard_unaccepted(record.identity(), record.version);
                        self.queue
                            .retained_data
                            .lock()
                            .unwrap()
                            .remove(&record.identity());
                        return Err(error);
                    }
                }
                if record.state.terminal() {
                    self.queue
                        .retained_data
                        .lock()
                        .unwrap()
                        .remove(&record.identity());
                }
                self.queue
                    .first_seen
                    .lock()
                    .unwrap()
                    .entry(record.identity())
                    .or_insert(now_ms());
                self.queue
                    .queue
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or_else(unavailable)?
                    .install(record.clone())
                    .map_err(|e| e.protocol())?;
                Ok(Reply::Record { record })
            }
            Command::Submit { body, context } => {
                // Validate the exact original operation before touching customer
                // storage, including on a forged trusted-peer submission.
                crate::protocol::parse_request(&body)?;
                validate_context_mode(root, context.as_ref(), now_ms() / 1000)?;
                if sender.is_some()
                    || Some(scheduler(root)?.node_id) != self.signer.sender().node_id()
                {
                    return Err(unavailable());
                }
                let _ = self.queue.reap_timed_seals();
                let fresh_grant = if let Some(context) = context.as_ref() {
                    let grant = self.open_database(context.clone()).await?;
                    let loaded = self.owned_catalog(root, &grant).await?;
                    grant
                        .storage()
                        .authorize_catalog_access(
                            &loaded,
                            nabedra_rt::time::now() + Duration::from_secs(30),
                        )
                        .await
                        .map_err(access_failure)?;
                    Some(grant)
                } else {
                    None
                };
                // Customer I/O cannot hold the global admission/join seal.
                let _serial = self.queue.lock_changes().await;
                self.refresh_queue(root).await?;
                let fresh_context = context.clone();
                let record = self
                    .queue
                    .queue
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or_else(unavailable)?
                    .propose_with_context(body, context, now_ms())
                    .map_err(|e| e.protocol())?;
                if !record.state.terminal() {
                    if let Some(grant) = fresh_grant {
                        let existing = self
                            .queue
                            .retained_data
                            .lock()
                            .unwrap()
                            .contains_key(&record.identity());
                        if !existing && record.context == fresh_context {
                            self.queue
                                .retained_data
                                .lock()
                                .unwrap()
                                .insert(record.identity(), grant);
                        } else {
                            // Free the transient fresh authorization client
                            // before recreating an original accepted snapshot.
                            drop(grant);
                            if !existing {
                                self.queue_data_for_record(&record).await?;
                            }
                        }
                    }
                }
                self.queue
                    .first_seen
                    .lock()
                    .unwrap()
                    .entry(record.identity())
                    .or_insert(now_ms());
                let record = self.replicate_record(root, record).await.map_err(|_|crate::protocol::ProtocolError::new(crate::protocol::ErrorCode::OutcomeUnknown,"request replication outcome is uncertain; retry the same identity and body",crate::protocol::Outcome::Unknown))?;
                self.queue.changed.notify_waiters();
                Ok(Reply::Record { record })
            }
            Command::Lookup {
                run_id,
                request_id,
                context,
            } => {
                validate_context_mode(root, context.as_ref(), now_ms() / 1000)?;
                let key = RequestKey::new(request_id, context.as_ref()).map_err(|_| invalid())?;
                let fresh_grant = match context {
                    Some(context) => Some(self.open_database(context).await?),
                    None => None,
                };
                let storage = fresh_grant
                    .as_ref()
                    .map(|g| g.storage())
                    .unwrap_or_else(|| self.storage.clone());
                let loaded = storage
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?;
                if !root.template.mode.is_fixed() {
                    if loaded.catalog().owner_cluster != Some(root.cluster_id) {
                        return Err(http::failure(crate::protocol::ErrorCode::Unauthorized));
                    }
                    let deadline = nabedra_rt::time::now() + Duration::from_secs(30);
                    storage
                        .authorize_catalog_access(&loaded, deadline)
                        .await
                        .map_err(access_failure)?;
                    if let Some(pinned) = self.queue.pinned(key.clone()) {
                        storage
                            .authorize_catalog_access(&pinned, deadline)
                            .await
                            .map_err(access_failure)?;
                    }
                }
                let result = {
                    let q = self.queue.queue.lock().unwrap();
                    let q = q.as_ref().ok_or_else(unavailable)?;
                    if run_id == q.run_id()
                        && q.record(key.clone())
                            .is_some_and(|r| r.state.terminal() && !r.result_expired)
                        && !q.ready(key.clone())
                    {
                        Ok(Receipt {
                            run_id,
                            request_id,
                            state: RequestState::Reconciling,
                            result: None,
                            error: None,
                        })
                    } else {
                        q.lookup(run_id, key.clone(), now_ms())
                    }
                };
                // Fresh receipt grants can reconcile a commit even when the
                // immutable accepted snapshot expired and attempts have paused.
                if result.as_ref().is_ok_and(|receipt| {
                    matches!(
                        receipt.state,
                        RequestState::Reconciling | RequestState::Committing
                    )
                }) {
                    let observed = self.queue.record(key.clone())?;
                    let budget = crate::storage::HistoryBudget::new(
                        4096,
                        64 << 20,
                        nabedra_rt::time::now() + Duration::from_secs(30),
                    )
                    .map_err(|_| unavailable())?;
                    if let Some(decision) = storage
                        .lookup_decision_by_id(&loaded, request_id, budget)
                        .await
                        .map_err(|_| unavailable())?
                    {
                        let receipt = decision_receipt(run_id, request_id, &decision)?;
                        let finished = {
                            let mut lock = self.queue.queue.lock().unwrap();
                            settle_reconciled_decision(
                                lock.as_mut().ok_or_else(unavailable)?,
                                &observed,
                                &decision,
                                now_ms(),
                            )
                            .map_err(|e| e.protocol())?
                        };
                        self.replicate_record(root, finished).await?;
                        return Ok(Reply::Receipt { receipt });
                    }
                }
                match result {
                    Ok(receipt) => Ok(Reply::Receipt { receipt }),
                    Err(error)
                        if matches!(
                            error,
                            crate::request_queue::QueueError::Abandoned
                                | crate::request_queue::QueueError::NotFound
                        ) =>
                    {
                        // OLAP-2: `abandoned` needs a root fenced by the current
                        // run, so that no PUT of the request's run can still
                        // apply. Fence it first, as dispatch does; without that
                        // proof the receipt stays retryable.
                        let loaded = if error == crate::request_queue::QueueError::Abandoned {
                            let run = self.queue.fence()?.run_id;
                            if loaded.catalog().fence.is_some_and(|f| f.run_id == run) {
                                loaded
                            } else {
                                let grant = fresh_grant
                                    .as_ref()
                                    .filter(|_| !root.template.mode.is_fixed())
                                    .ok_or_else(unavailable)?;
                                let fenced = self.owned_catalog(root, grant).await?;
                                if fenced.catalog().fence.is_none_or(|f| f.run_id != run) {
                                    return Err(unavailable());
                                }
                                fenced
                            }
                        } else {
                            loaded
                        };
                        let budget = crate::storage::HistoryBudget::new(
                            4096,
                            64 << 20,
                            nabedra_rt::time::now() + Duration::from_secs(30),
                        )
                        .map_err(|_| unavailable())?;
                        if let Some(d) = storage
                            .lookup_decision_by_id(&loaded, request_id, budget)
                            .await
                            .map_err(|_| unavailable())?
                        {
                            let value: serde_json::Value =
                                serde_json::from_str(&d.response_body).map_err(|_| invalid())?;
                            return Ok(Reply::Receipt {
                                receipt: Receipt {
                                    run_id,
                                    request_id,
                                    state: if d.response_status == 200 {
                                        RequestState::Succeeded
                                    } else {
                                        RequestState::Failed
                                    },
                                    result: value.get("data").filter(|v| !v.is_null()).cloned(),
                                    error: value
                                        .get("error")
                                        .map(|v| serde_json::from_value(v.clone()))
                                        .transpose()
                                        .map_err(|_| invalid())?,
                                },
                            });
                        }
                        if error == crate::request_queue::QueueError::Abandoned {
                            return Ok(Reply::Receipt {
                                receipt: Receipt {
                                    run_id,
                                    request_id,
                                    state: RequestState::Abandoned,
                                    result: None,
                                    error: None,
                                },
                            });
                        }
                        Err(error.protocol())
                    }
                    Err(error) => Err(error.protocol()),
                }
            }
        }
    }
    async fn refresh_join_records(&self, root: &Control) -> Result<()> {
        let mut after = None;
        loop {
            let (page, done) = self
                .queue
                .queue
                .lock()
                .unwrap()
                .as_ref()
                .ok_or_else(unavailable)?
                .snapshot_page(after);
            for record in page.records {
                after = Some(record.sequence);
                self.replicate_record_class(root, record, true).await?;
            }
            if done {
                return Ok(());
            }
        }
    }

    async fn resolve_fenced_join(&self, root: &Control) -> Result<()> {
        let Some((token, base, joiner)) = self.queue.fenced_join() else {
            return Ok(());
        };
        let successor = JoinBase::of(root);
        if !base.is_fenced_by(successor) {
            return Ok(());
        }
        if self.owns_queue_scheduler(root) {
            self.refresh_queue(root).await?;
            if members(root).contains(&joiner) {
                self.refresh_join_records(root).await?;
            }
        }
        self.queue.release_join(token, successor);
        Ok(())
    }

    fn owns_queue_scheduler(&self, root: &Control) -> bool {
        let sender = self.signer.sender();
        scheduler(root).is_ok_and(|node| {
            Some(node.node_id) == sender.node_id()
                && node.incarnation == sender.incarnation()
                && node.session.as_ref().map(|session| session.id) == sender.session_id()
        })
    }

    pub(super) async fn queue_pump(
        self: Arc<Self>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            if *stop.borrow() {
                break;
            }
            self.queue.streams.reap(now_ms());
            if self.queue.has_pending() && super::stream::memory_pressure(&self.cpu) {
                self.queue.streams.evict_speculation();
            }
            let _ = self.queue.reap_timed_seals();
            if let Ok(root) = self.root().await
                && self.resolve_fenced_join(&root).await.is_ok()
                && self.queue.fenced_join().is_none()
                && self.owns_queue_scheduler(&root)
                && self.refresh_queue(&root).await.is_ok()
            {
                self.queue.streams.scheduler_owner(true);
                let records = {
                    let q = self.queue.queue.lock().unwrap();
                    let active = self.queue.active.lock().unwrap();
                    q.as_ref()
                        .map(|q| {
                            q.records()
                                .filter(|r| {
                                    r.accepted
                                        && (!r.state.terminal() || !q.ready(r.identity()))
                                        && !active.contains(&r.identity())
                                })
                                .filter_map(|record| {
                                    let bytes = record
                                        .body
                                        .len()
                                        .checked_mul(2)?
                                        .checked_add(record.context_bytes().ok()?.checked_mul(2)?)?
                                        .checked_add(
                                            record.pinned_catalog.as_ref().map_or(0, Vec::len),
                                        )?
                                        .checked_add(
                                            record.response_body.as_ref().map_or(0, Vec::len),
                                        )?;
                                    let charge = self
                                        .cpu
                                        .scheduler()
                                        .charge_memory(0, (bytes as u64).max(1))
                                        .ok()?;
                                    Some((record.clone(), charge))
                                })
                                .take(self.coordination_slots.saturating_sub(active.len()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                };
                for (record, charge) in records {
                    if self
                        .queue
                        .retries
                        .lock()
                        .unwrap()
                        .get(&record.identity())
                        .is_some_and(|(count, until)| {
                            *count >= 3 || *until > nabedra_rt::time::now()
                        })
                    {
                        continue;
                    }
                    let mut active = self.queue.active.lock().unwrap();
                    if active.len() >= self.coordination_slots {
                        break;
                    }
                    if !active.insert(record.identity()) {
                        continue;
                    }
                    drop(active);
                    let owner = self.clone();
                    let root = root.clone();
                    let id = record.identity();
                    nabedra_rt::task::spawn_in(&mut tasks, async move {
                        let _active = ActiveRequest {
                            owner: owner.clone(),
                            id: id.clone(),
                            _charge: charge,
                        };
                        // Once admitted, any failed mutation attempt may have
                        // reached publication before its error was delivered.
                        let reconcile_mutation = crate::protocol::parse_request(&record.body)
                            .is_ok_and(|request| request.operation.is_mutation());
                        if let Err(mut error) = owner.run_queued(root.clone(), record).await {
                            error = reconcile_attempt_error(reconcile_mutation, error);
                            let attempts = {
                                let mut retry = owner.queue.retries.lock().unwrap();
                                let count = retry.get(&id).map_or(1, |(n, _)| n + 1);
                                retry.insert(
                                    id.clone(),
                                    (
                                        count,
                                        nabedra_rt::time::now()
                                            + Duration::from_millis(50 * (1u64 << count.min(5))),
                                    ),
                                );
                                count
                            };
                            let record = {
                                let mut lock = owner.queue.queue.lock().unwrap();
                                let q = lock.as_mut().ok_or_else(unavailable)?;
                                if q.record(id.clone()).is_some_and(|r| r.state.terminal()) {
                                    None
                                } else if error.outcome == crate::protocol::Outcome::NotApplied
                                    && attempts >= 3
                                {
                                    let encoded = crate::api::response::encode_error(
                                        Some(id.request_id),
                                        error,
                                    );
                                    Some(
                                        q.finish(
                                            id.clone(),
                                            encoded.status,
                                            encoded.body,
                                            now_ms(),
                                        )
                                        .map_err(|e| e.protocol())?,
                                    )
                                } else {
                                    Some(
                                        q.transition(id.clone(), RequestState::Reconciling)
                                            .map_err(|e| e.protocol())?,
                                    )
                                }
                            };
                            if let Some(record) = record {
                                let _ = owner.replicate_record(&root, record).await;
                            }
                        } else {
                            owner.queue.retries.lock().unwrap().remove(&id);
                        }
                        Ok::<(), crate::protocol::ProtocolError>(())
                    });
                }
            } else if self.queue.fenced_join().is_none() {
                self.queue.streams.scheduler_owner(false);
            }
            tokio::select! {_=stop.changed()=>{},_=self.queue.changed.notified()=>{},_=tasks.join_next(),if !tasks.is_empty()=>{},_=nabedra_rt::time::sleep(Duration::from_millis(100))=>{}}
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
    pub(super) async fn run_queued(
        self: &Arc<Self>,
        root: Control,
        record: RequestRecord,
    ) -> Result<()> {
        let id = record.identity();
        let record = self.replicate_record(&root, record).await?;
        if record.state.terminal() {
            return Ok(());
        }
        // Authorize/fence this database before taking the global queue seal.
        // An inaccessible target must not block unrelated database admission.
        let grant = self.queue_data_for_record(&record).await?;
        let storage = grant.storage();
        let loaded = self.owned_catalog(&root, &grant).await?;
        // A join seal must cover the first recoverable read snapshot as well as
        // new identities. No compute permit exists while this handshake waits.
        let starting = self.queue.lock_changes().await;
        let root = self.root().await?;
        self.refresh_queue(&root).await?;
        let record = self
            .queue
            .queue
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(unavailable)?
            .record(id.clone())
            .ok_or_else(unavailable)?
            .clone();
        let record = if matches!(
            record.state,
            RequestState::Queued | RequestState::Reconciling
        ) {
            let mutation = crate::protocol::parse_request(&record.body)?
                .operation
                .is_mutation();
            let pin = if mutation || record.pinned_catalog.is_some() {
                None
            } else {
                Some(encode_pin(&loaded)?)
            };
            let started = self
                .queue
                .queue
                .lock()
                .unwrap()
                .as_mut()
                .ok_or_else(unavailable)?
                .start(id.clone(), pin)
                .map_err(|e| e.protocol())?;
            self.replicate_record(&root, started).await?
        } else {
            return Err(unavailable());
        };
        drop(starting);
        let node = root
            .nodes
            .iter()
            .find(|n| Some(n.node_id) == self.signer.sender().node_id())
            .ok_or_else(unavailable)?;
        let attempt_fence = CatalogFence {
            run_id: record.run_id,
            scheduler_term: record.scheduler_term,
        };
        let parsed = crate::protocol::parse_request(&record.body)?;
        let ids = super::auth::RequestIds {
            operation: Some(parsed.request_id),
            digest: Some(http::digest(&record.body)),
            ..Default::default()
        };
        let context = self.signer.context(
            &root,
            node,
            super::auth::Route::Operations,
            ids,
            record.body,
        )?;
        let mut request = self.signer.request(
            node.endpoint.as_ref().ok_or_else(unavailable)?,
            &context,
            Body::from(context.initial_bytes().to_vec()),
            false,
        )?;
        request.extensions_mut().insert(id.clone());
        let transport = self
            .operation_transport
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| unavailable())?;
        let response = self.clone().dispatch(request, transport).await?;
        let status = response.status().as_u16();
        let body = axum::body::to_bytes(response.into_body(), crate::protocol::MAX_BODY_BYTES)
            .await
            .map_err(|_| unavailable())?
            .to_vec();
        if status == 503 {
            let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
            let error = value
                .get("error")
                .cloned()
                .and_then(|error| {
                    serde_json::from_value::<crate::protocol::ProtocolError>(error).ok()
                })
                .ok_or_else(invalid)?;
            if matches!(
                error.code,
                crate::protocol::ErrorCode::StorageUnavailable
                    | crate::protocol::ErrorCode::OutcomeUnknown
            ) {
                return Err(crate::protocol::ProtocolError::new(
                    error.code,
                    error.message,
                    crate::protocol::Outcome::Unknown,
                ));
            }
        }
        if self.queue.fence()? != attempt_fence
            || storage
                .load_cluster_catalog()
                .await
                .map_err(|_| unavailable())?
                .catalog()
                .fence
                != Some(attempt_fence)
        {
            return Err(unavailable());
        }
        let finished = self
            .queue
            .queue
            .lock()
            .unwrap()
            .as_mut()
            .ok_or_else(unavailable)?
            .finish(id.clone(), status, body, now_ms())
            .map_err(|e| e.protocol())?;
        self.replicate_record(&root, finished).await?;
        Ok(())
    }
}
struct ActiveRequest {
    owner: Arc<Worker>,
    id: RequestKey,
    _charge: crate::scheduling::MemoryCharge,
}
impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.owner.queue.active.lock().unwrap().remove(&self.id);
        self.owner.queue.changed.notify_waiters();
    }
}

struct ChargedResponse {
    body: Body,
    _charges: Vec<crate::scheduling::MemoryCharge>,
    _transport: tokio::sync::OwnedSemaphorePermit,
}
impl hyper::body::Body for ChargedResponse {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
    {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub(super) async fn handler(
    State(worker): State<Arc<Worker>>,
    request: Request<Body>,
) -> Response<Body> {
    let result = async {
        let headers = request.headers().clone();
        let root = worker.root().await?;
        let (nonce, sender, digest, expected_command, class) = queue_transport::preauthenticate(
            &headers,
            &root,
            worker.signer.sender(),
            &worker.pin,
            &mut worker.queue.seen.lock().unwrap(),
        )?;
        let transport = worker.queue.transport(&class)?;
        let length =
            http::framing(&headers, queue_transport::MAX_BYTES, false)?.ok_or_else(invalid)?;
        let staging = (length as u64).checked_mul(2).ok_or_else(invalid)?;
        let incoming_charge = if class == "bootstrap" {
            worker.control_budget.charge_memory(0, staging)?
        } else if class == "progress" || class == "receipt" && staging <= 128 << 10 {
            worker.progress_budget.charge_memory(0, staging)?
        } else {
            worker.cpu.scheduler().charge_memory(0, staging)?
        };
        let bytes = http::read_small(
            request.into_body(),
            length,
            nabedra_rt::time::now() + Duration::from_secs(30),
        )
        .await?;
        if http::digest(&bytes) != digest {
            return Err(invalid());
        }
        let command = http::cpu(&worker.control_cpu, move |_| {
            serde_json::from_slice::<Command>(&bytes).map_err(|_| invalid())
        })
        .await;
        drop(incoming_charge);
        if let Ok(command) = command.as_ref() {
            validate_command_class(command, &expected_command, &class)?;
        }
        let native_bytes = command
            .as_ref()
            .map_or(64 << 10, |command| worker.reply_working_bytes(command));
        let native_charge = Some(if class == "bootstrap" {
            worker.control_budget.charge_memory(0, native_bytes)?
        } else if class == "progress" || class == "receipt" && native_bytes <= 128 << 10 {
            worker.progress_budget.charge_memory(0, native_bytes)?
        } else {
            worker.cpu.scheduler().charge_memory(0, native_bytes)?
        });
        let reply = match command {
            Ok(command) => worker
                .queue_command(&root, sender, command)
                .await
                .unwrap_or_else(|error| Reply::Error { error }),
            Err(error) => Reply::Error { error },
        };
        let (reply, len) = http::cpu(&worker.control_cpu, move |_| {
            let len = queue_transport::encoded_len(&reply, queue_transport::MAX_BYTES)?;
            Ok((reply, len))
        })
        .await?;
        let encoded_bytes = len as u64 + 4096;
        let reply_charge = if class == "bootstrap" {
            worker.control_budget.charge_memory(0, encoded_bytes)?
        } else if class == "progress" || class == "receipt" && encoded_bytes <= 128 << 10 {
            worker.progress_budget.charge_memory(0, encoded_bytes)?
        } else {
            worker.cpu.scheduler().charge_memory(0, encoded_bytes)?
        };
        let bytes = http::cpu(&worker.control_cpu, move |_| {
            queue_transport::encode_exact(&reply, len)
        })
        .await?;
        let response = queue_transport::reply(&worker.signer, &root, &headers, nonce, bytes)?;
        Ok(response.map(|body| {
            Body::new(ChargedResponse {
                body,
                _transport: transport,
                _charges: native_charge
                    .into_iter()
                    .chain(std::iter::once(reply_charge))
                    .collect(),
            })
        }))
    }
    .await;
    result.unwrap_or_else(|error| {
        let e = crate::api::response::encode_error(None, error);
        Response::builder()
            .status(e.status)
            .header("content-type", "application/json")
            .body(Body::from(e.body))
            .unwrap()
    })
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn c01_c02_c07_coordination_budget_exposes_cpu_frontier_or_rejects_before_enrollment() {
        let id = |n| Uuid::from_u128(n);
        let template = json!({"api_urls":["http://api/"],"listen":"127.0.0.1:0","operation_slots":1,"partition_slots":1,"queue_size":16,"partition_size":"4 KiB","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"capacity/","token_file":"/token","ca_file":null}},"tls":null});
        let template_typed: crate::cluster::configuration::Template =
            crate::cluster::configuration::Template::decode(
                &serde_json::to_vec(&template).unwrap(),
            )
            .unwrap();
        let nodes=[1,2,4].into_iter().enumerate().map(|(index,threads)|{
            let node=id(index as u128+10);let resources=crate::config::WorkerResources{max_threads:threads,..Default::default()};
            json!({"node_id":node,"launch_id":null,"resource_id":format!("external:{node}"),"ownership":"external","state":"ready","incarnation":id(index as u128+100),"retired_incarnations":[],"endpoint":"http://worker/","operation_slots":256,"partition_slots":256,"resources":resources,"queue_size":16,"registration":"verified","session":{"id":id(index as u128+200),"verifier":"a".repeat(64)},"drain":null})
        }).collect::<Vec<_>>();
        let wire = json!({"version":1,"cluster_id":id(1),"name":"Capacity","config":{"generation":2,"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":template_typed.fingerprint().unwrap(),"lifecycle_fingerprint":"c".repeat(64),"worker_semantic_fingerprint":"d".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":crate::cluster::configuration::Timing::default(),"revision":1,"change_id":id(2),"policy":{"revision":1,"mode":"fixed","desired":3,"min_nodes":3,"max_nodes":3},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"coordinator_node":id(10),"scheduler_term":1,"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":nodes,"intents":[]});
        let root = Control::decode(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert_eq!(
            validate_coordination_capacity(&root, None, None).unwrap(),
            14
        );
        let error = validate_coordination_capacity(
            &root,
            Some((
                id(400),
                crate::config::WorkerResources {
                    max_threads: 200,
                    ..Default::default()
                },
            )),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, crate::protocol::ErrorCode::UnsupportedOperation);
        assert!(
            error
                .message
                .contains("control_memory_reserve_bytes must be at least")
        );
        assert_eq!(root.nodes.len(), 3);
        let mut independent = template_typed;
        independent.resources = Some(crate::config::WorkerResources {
            max_threads: 4,
            ..Default::default()
        });
        assert!(independent.validate().is_ok());
        assert!(job_coordination_capacity(&independent).unwrap() >= 10);
    }
}

#[cfg(test)]
mod class_capacity_tests {
    use super::*;
    #[test]
    fn c07_r03_ingress_and_replica_saturation_preserves_bootstrap_and_progress_capacity() {
        let queue = QueueRuntime::new();
        let ingress = (0..8)
            .map(|_| queue.transport("ingress").unwrap())
            .collect::<Vec<_>>();
        let replicas = (0..8)
            .map(|_| queue.transport("replica").unwrap())
            .collect::<Vec<_>>();
        let receipts = (0..8)
            .map(|_| queue.transport("receipt").unwrap())
            .collect::<Vec<_>>();
        assert!(queue.transport("ingress").is_err());
        assert!(queue.transport("replica").is_err());
        assert!(queue.transport("receipt").is_err());
        let bootstrap = (0..4)
            .map(|_| queue.transport("bootstrap").unwrap())
            .collect::<Vec<_>>();
        let progress = (0..8)
            .map(|_| queue.transport("progress").unwrap())
            .collect::<Vec<_>>();
        assert!(
            validate_command_class(&Command::Snapshot { after: None }, "snapshot", "bootstrap")
                .is_ok()
        );
        assert!(
            validate_command_class(
                &Command::Submit {
                    body: Vec::new(),
                    context: None
                },
                "snapshot",
                "bootstrap"
            )
            .is_err()
        );
        assert!(
            validate_command_class(
                &Command::Submit {
                    body: Vec::new(),
                    context: None
                },
                "submit",
                "bootstrap"
            )
            .is_err()
        );
        drop((ingress, replicas, receipts, bootstrap, progress));
        assert!(queue.transport("ingress").is_ok());
        assert!(queue.transport("bootstrap").is_ok());
    }
    #[tokio::test(start_paused = true)]
    async fn f03_r03_expired_join_seal_releases_waiter_without_pump_or_later_commands() {
        let queue = Arc::new(QueueRuntime::new());
        let stream_copying = queue.lock_stream_changes().await;
        let copying = queue.lock_changes().await;
        let waiter_queue = queue.clone();
        let waiter = nabedra_rt::task::spawn(async move { waiter_queue.lock_changes().await });
        let waiter_queue = queue.clone();
        let stream_waiter =
            nabedra_rt::task::spawn(async move { waiter_queue.lock_stream_changes().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert!(!stream_waiter.is_finished());
        // The waiting actor started while a join was still copying, before its
        // seal/deadline existed. No pump or second admission runs in this test.
        queue.retain_seal(copying, stream_copying);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(29)).await;
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "join remains sealed before its deadline"
        );
        assert!(!stream_waiter.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        let admitted = nabedra_rt::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("expired seal must wake the owned actor without a pump")
            .unwrap();
        let stream_admitted = nabedra_rt::time::timeout(Duration::from_secs(1), stream_waiter)
            .await
            .expect("expired seal must wake the stream actor without a pump")
            .unwrap();
        assert!(queue.seals.lock().unwrap().is_empty());
        assert!(queue.changes.try_lock().is_err());
        assert!(queue.stream_changes.try_lock().is_err());
        drop((admitted, stream_admitted));
        assert!(queue.changes.try_lock().is_ok());
        assert!(queue.stream_changes.try_lock().is_ok());
    }
    #[tokio::test(start_paused = true)]
    async fn st04_r03_expiry_cannot_unseal_a_delayed_readiness_match() {
        let queue = Arc::new(QueueRuntime::new());
        let base = JoinBase {
            cluster: Uuid::from_u128(1),
            revision: 7,
            change_id: Uuid::from_u128(2),
        };
        let stream_seal = queue.lock_stream_changes().await;
        let seal = queue.lock_changes().await;
        let token = queue.insert_seal(
            seal,
            stream_seal,
            SealRelease::Successor {
                base,
                joiner: Uuid::from_u128(3),
            },
        );

        let queue_waiter = {
            let queue = queue.clone();
            nabedra_rt::task::spawn(async move { queue.lock_changes().await })
        };
        let stream_waiter = {
            let queue = queue.clone();
            nabedra_rt::task::spawn(async move { queue.lock_stream_changes().await })
        };
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        let _ = queue.reap_timed_seals();
        queue.release_join(token, base);
        queue.release_join(
            token,
            JoinBase {
                cluster: Uuid::from_u128(4),
                revision: 8,
                ..base
            },
        );
        let still_sealed = queue.fenced_join().is_some();
        let blocked = !queue_waiter.is_finished() && !stream_waiter.is_finished();

        queue.release_join(
            token,
            JoinBase {
                revision: 8,
                change_id: Uuid::from_u128(5),
                ..base
            },
        );
        for waiter in [queue_waiter, stream_waiter] {
            let guard = nabedra_rt::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("a successor must unblock both admission paths")
                .unwrap();
            drop(guard);
        }
        assert!(still_sealed && blocked);
        assert!(queue.seals.lock().unwrap().is_empty());
    }

    #[test]
    fn m02_r03_working_memory_pressure_does_not_consume_bootstrap_or_small_control_bytes() {
        let data = crate::scheduling::ResourceScheduler::new(vec![(
            0,
            crate::config::WorkerResources {
                max_threads: 1,
                memory_limit_bytes: 4096,
                task_memory_limit_bytes: 2048,
                control_memory_reserve_bytes: 1024,
            },
        )])
        .unwrap();
        let full = data.charge_memory(0, 3072).unwrap();
        assert!(data.charge_memory(0, 1).is_err());
        let controls = crate::scheduling::ResourceScheduler::new(vec![(
            0,
            crate::config::WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 256 << 20,
                task_memory_limit_bytes: (256 << 20) - 1,
                control_memory_reserve_bytes: 1,
            },
        )])
        .unwrap();
        let actors = controls.charge_memory(0, 64 << 20).unwrap();
        let progress = controls.charge_memory(0, 16 << 20).unwrap();
        let bootstrap = controls.charge_memory(0, 128 << 20).unwrap();
        assert!(controls.usage()[0].2 <= 256 << 20);
        drop((full, actors, progress, bootstrap));
        assert_eq!(data.usage()[0].2, 0);
        assert_eq!(controls.usage()[0].2, 0);
    }
}

#[cfg(test)]
mod expiry_tests {
    use super::*;
    use crate::storage::context::RequestContext;
    #[test]
    fn t04_q05_terminal_reconciliation_reuses_original_version_and_rejects_mismatches() {
        let member = nabedra_rt::ids::uuid_v4();
        let id = nabedra_rt::ids::uuid_v4();
        let context:RequestContext=serde_json::from_value(serde_json::json!({"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"original","secret_access_key":"original-secret","session_token":null,"expires_at":null}})).unwrap();
        let body=serde_json::to_vec(&serde_json::json!({"request_id":id,"operation":"table.drop","payload":{"table_id":nabedra_rt::ids::uuid_v4()}})).unwrap();
        let response =
            serde_json::to_vec(&serde_json::json!({"request_id":id,"data":null})).unwrap();
        let mut queue = RequestQueue::new(
            nabedra_rt::ids::uuid_v4(),
            1,
            1,
            QueueLimits {
                max_pending_requests: 4,
                max_queue_bytes_per_replica: 1 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 1000,
            },
            [member].into(),
        )
        .unwrap();
        let proposed = queue
            .propose_with_context(body.clone(), Some(context.clone()), 0)
            .unwrap();
        let key = proposed.identity();
        let observed = queue.acknowledge(key.clone(), member).unwrap();
        let decision = crate::storage::Decision {
            version: 1,
            id: nabedra_rt::ids::uuid_v4(),
            parent: None,
            base_revision: 0,
            revision: 1,
            request_id: id,
            request_body: String::from_utf8(body).unwrap(),
            response_status: 200,
            response_body: String::from_utf8(response.clone()).unwrap(),
        };
        let terminal = queue.finish(key.clone(), 200, response, 10).unwrap();
        assert!(!queue.ready(key.clone()));
        let reused = settle_reconciled_decision(&mut queue, &observed, &decision, 20).unwrap();
        assert_eq!(reused, terminal);
        assert_eq!(reused.completed_at_ms, Some(10));
        assert_eq!(reused.context, Some(context));
        for field in 0..9 {
            let mut changed = observed.clone();
            let mut d = decision.clone();
            match field {
                0 => d.request_id = nabedra_rt::ids::uuid_v4(),
                1 => d.request_body.push(' '),
                2 => d.response_status = 409,
                3 => d.response_body.push(' '),
                4 => changed.scheduler_term += 1,
                5 => changed.membership_revision += 1,
                6 => changed.sequence += 1,
                7 => {
                    let mut c = serde_json::to_value(changed.context.as_ref().unwrap()).unwrap();
                    c["credentials"]["secret_access_key"] = serde_json::json!("rotated");
                    changed.context = Some(serde_json::from_value(c).unwrap());
                }
                8 => {
                    let mut c = serde_json::to_value(changed.context.as_ref().unwrap()).unwrap();
                    c["bucket"] = serde_json::json!("foreign-data");
                    changed.context = Some(serde_json::from_value(c).unwrap());
                }
                _ => unreachable!(),
            }
            assert!(
                settle_reconciled_decision(&mut queue, &changed, &d, 30).is_err(),
                "mismatched accepted record or decision field {field}"
            );
            assert_eq!(queue.record(key.clone()), Some(&terminal));
        }
        queue.acknowledge(key.clone(), member).unwrap();
        assert!(queue.ready(key));
    }

    #[test]
    fn sl03_t04_expired_reconciling_mutation_keeps_unknown_and_original_snapshot() {
        let member = nabedra_rt::ids::uuid_v4();
        let run = nabedra_rt::ids::uuid_v4();
        let request_id = nabedra_rt::ids::uuid_v4();
        let context: RequestContext = serde_json::from_value(serde_json::json!({"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"original-access","secret_access_key":"original-secret","session_token":null,"expires_at":"2026-11-16T20:00:00Z"}})).unwrap();
        let body = serde_json::to_vec(&serde_json::json!({"request_id":request_id,"operation":"table.drop","payload":{"table_id":nabedra_rt::ids::uuid_v4()}})).unwrap();
        let mut queue = RequestQueue::new(
            run,
            1,
            1,
            QueueLimits {
                max_pending_requests: 4,
                max_queue_bytes_per_replica: 1 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 1000,
            },
            [member].into(),
        )
        .unwrap();
        let record = queue
            .propose_with_context(body.clone(), Some(context.clone()), 0)
            .unwrap();
        let key = record.identity();
        queue.acknowledge(key.clone(), member).unwrap();
        queue
            .transition(key.clone(), RequestState::Reconciling)
            .unwrap();
        queue.acknowledge(key.clone(), member).unwrap();
        assert!(context.validate(u64::MAX).is_err());
        let record = queue.record(key.clone()).unwrap();
        let mutation = crate::protocol::parse_request(&record.body)
            .unwrap()
            .operation
            .is_mutation();
        let expired = http::failure(crate::protocol::ErrorCode::Unauthorized);
        let error =
            reconcile_attempt_error(record.state != RequestState::Queued && mutation, expired);
        assert_eq!(error.code, crate::protocol::ErrorCode::OutcomeUnknown);
        assert_eq!(error.outcome, crate::protocol::Outcome::Unknown);
        assert_eq!(
            queue.lookup(run, key.clone(), 1).unwrap().state,
            RequestState::Reconciling
        );
        // Fresh credentials attach to the existing identity and cannot renew it.
        let mut fresh = serde_json::to_value(&context).unwrap();
        fresh["credentials"]["expires_at"] = serde_json::Value::Null;
        fresh["credentials"]["secret_access_key"] = serde_json::json!("fresh-secret");
        let replay = queue
            .propose_with_context(body, Some(serde_json::from_value(fresh).unwrap()), 1)
            .unwrap();
        assert_eq!(replay.context, Some(context));
        assert_eq!(replay.state, RequestState::Reconciling);
        assert_eq!(queue.pending(), 1);
    }
}
