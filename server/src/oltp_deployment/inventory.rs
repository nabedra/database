//! One worker owns all its pools, their shared resources and the mount bindings.
use super::{
    controller::{GrowthPhase, GrowthRecord},
    planner::Action,
};
use crate::{
    oltp::{
        NativeThreadBudget,
        pool::{
            ByteClass, ExecutionClass, OltpPool, ScratchLease, WorkerSharedResources,
            mount::{
                MountedPoolObservation, PoolMountExpectation, PoolMountProfile, VerifiedMountAnchor,
            },
        },
    },
    oltp_replication::{
        authority::control::{
            ControlHandle, DataWorkerGrowthProof, ReplicaMove, ReplicaMovePhase, VerifiedExclusion,
        },
        types::WorkerIdentity,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};
use uuid::Uuid;

mod drain;
pub(crate) use drain::PoolDrainRequest;
#[cfg(test)]
mod tests;

const MAX_POOLS: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecondaryPoolConfig {
    pub mount: PoolMountExpectation,
    pub growth_claim: Uuid,
}
impl SecondaryPoolConfig {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.mount.validate()?;
        if self.growth_claim.is_nil() {
            return Err(unavailable(
                "a secondary pool requires its exact growth claim",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Entry {
    pool: OltpPool,
    mount: Option<MountedPoolObservation>,
    growth_claim: Option<Uuid>,
    retained_startup: bool,
}

pub(crate) struct WorkerPools {
    primary: Uuid,
    worker: WorkerIdentity,
    shared: Arc<WorkerSharedResources>,
    limit: usize,
    entries: Mutex<BTreeMap<Uuid, Entry>>,
    registration: Mutex<()>,
    closing: Mutex<BTreeMap<Uuid, Uuid>>,
    /// Secondary pools refused at open (plan section 3 rule 6, O26): the worker
    /// keeps serving its other pools and reports these pools' replicas lost.
    refused: Mutex<BTreeMap<Uuid, ProtocolError>>,
    drain_actor: tokio::sync::Mutex<()>,
    _owner: ScratchLease,
}

impl WorkerPools {
    pub(crate) fn new(
        primary: OltpPool,
        worker: WorkerIdentity,
        mount: Option<MountedPoolObservation>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if primary.identity().worker_id != worker.worker_id
            || mount.as_ref().is_some_and(|observation| {
                observation.expected.worker != worker
                    || &observation.expected.identity != primary.identity()
            })
        {
            return Err(unavailable("primary pool does not match the worker"));
        }
        let id = primary.identity().pool_id;
        let shared = primary.resources().clone();
        let limit = shared.config().max_open_pools.min(MAX_POOLS);
        let owner = shared.reserve_scratch(ByteClass::Progress, (limit + 1) * (12 << 10))?;
        Ok(Arc::new(Self {
            primary: id,
            worker,
            shared,
            limit,
            entries: Mutex::new(BTreeMap::from([(
                id,
                Entry {
                    pool: primary,
                    mount,
                    growth_claim: None,
                    retained_startup: false,
                },
            )])),
            registration: Mutex::new(()),
            closing: Mutex::new(BTreeMap::new()),
            refused: Mutex::new(BTreeMap::new()),
            drain_actor: tokio::sync::Mutex::new(()),
            _owner: owner,
        }))
    }

    pub(crate) fn worker_identity(&self) -> WorkerIdentity {
        self.worker
    }

    /// Rule 6: records a secondary pool that could not be opened. Its replicas
    /// are reported lost; the worker keeps serving every other pool.
    pub(crate) fn mark_refused(&self, id: Uuid, error: ProtocolError) {
        if let Ok(mut refused) = self.refused.lock() {
            refused.insert(id, error);
        }
    }
    /// The open error of a refused secondary pool.
    pub(crate) fn refused(&self, id: Uuid) -> Option<ProtocolError> {
        self.refused.lock().ok()?.get(&id).cloned()
    }

    /// Every open pool of this worker, primary first.
    pub(crate) fn pool_ids(&self) -> Result<Vec<Uuid>, ProtocolError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?;
        let mut ids = vec![self.primary];
        ids.extend(entries.keys().copied().filter(|id| *id != self.primary));
        Ok(ids)
    }
    /// Rule 6 in `/v1/runtime/stats`: every refused secondary pool, with the
    /// error class and message that refused it.
    pub(crate) fn refused_report(&self) -> Vec<serde_json::Value> {
        let Ok(refused) = self.refused.lock() else {
            return Vec::new();
        };
        refused
            .iter()
            .map(|(id, error)| {
                serde_json::json!({
                    "pool_id": id,
                    "class": crate::oltp_replication::authority::control::PoolRefusal::classify(error),
                    "code": error.code,
                    "message": error.message,
                })
            })
            .collect()
    }

    pub(crate) fn get(&self, id: Uuid) -> Result<Option<OltpPool>, ProtocolError> {
        Ok(self.entry(id)?.map(|entry| entry.pool))
    }

    pub(crate) fn open_pool_counts(&self) -> Result<(usize, usize), ProtocolError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?;
        Ok((entries.len(), self.limit))
    }

    pub(crate) fn growth_target(&self, record: &GrowthRecord) -> Result<OltpPool, ProtocolError> {
        let entry = self
            .entry(record.intent.pool_id)?
            .ok_or_else(|| unavailable("growth pool is not open and registered on this worker"))?;
        if matches!(record.intent.action, Action::AddPool { .. })
            && entry.growth_claim != Some(record.intent.claim_id)
        {
            return Err(unavailable(
                "open pool does not belong to this growth claim",
            ));
        }
        Ok(entry.pool)
    }

    fn entry(&self, id: Uuid) -> Result<Option<Entry>, ProtocolError> {
        let closing = self.closing.lock().map_err(|_| unavailable("worker pool drain unavailable"))?;
        if closing.contains_key(&id) {
            return Ok(None);
        }
        Ok(self
            .entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?
            .get(&id)
            .cloned())
    }

    pub(crate) fn validate_secondary_set(
        config: &[SecondaryPoolConfig],
        primary: Option<&MountedPoolObservation>,
        limit: usize,
    ) -> Result<Vec<VerifiedMountAnchor>, ProtocolError> {
        if config.is_empty() {
            return Ok(Vec::new());
        }
        let primary =
            primary.ok_or_else(|| unavailable("secondary pools require a mounted primary pool"))?;
        if config.len() >= limit.min(MAX_POOLS) {
            return Err(unavailable("worker pool inventory limit reached"));
        }
        let mut anchors: Vec<VerifiedMountAnchor> = Vec::with_capacity(config.len());
        for next in config {
            next.validate()?;
            same_worker(primary, &next.mount)?;
            if config
                .iter()
                .filter(|entry| entry.growth_claim == next.growth_claim)
                .count()
                != 1
            {
                return Err(unavailable("secondary pools reuse the same growth claim"));
            }
            let anchor = next.mount.clone().inspect_mount()?;
            distinct(primary, anchor.observation())?;
            for old in &anchors {
                distinct(old.observation(), anchor.observation())?;
            }
            anchors.push(anchor);
        }
        Ok(anchors)
    }

    /// Startup may advance an existing Docker mount only after proving
    /// exclusive previous-owner exclusion in the registered worker bootstrap.
    pub(crate) fn open_configured(
        &self,
        config: &SecondaryPoolConfig,
        anchor: VerifiedMountAnchor,
        exclusion: Option<&VerifiedExclusion>,
    ) -> Result<(), ProtocolError> {
        self.open_secondary(config, anchor, exclusion, true)
            .map(drop)
    }

    pub(crate) async fn register_growth(
        self: &Arc<Self>,
        config: SecondaryPoolConfig,
        control: &ControlHandle,
    ) -> Result<OltpPool, ProtocolError> {
        config.validate()?;
        if control.pool_drain_blocks_placement(config.mount.identity.pool_id).await? {
            return Err(unavailable("pool generation is draining or closed"));
        }
        let pools = self.clone();
        self.shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let anchor = config.mount.clone().inspect_mount()?;
                pools.open_secondary(&config, anchor, None, false)
            })
            .await
    }

    fn open_secondary(
        &self,
        config: &SecondaryPoolConfig,
        anchor: VerifiedMountAnchor,
        exclusion: Option<&VerifiedExclusion>,
        retained_startup: bool,
    ) -> Result<OltpPool, ProtocolError> {
        let _registration = self
            .registration
            .lock()
            .map_err(|_| unavailable("worker pool enrollment unavailable"))?;
        let observed = anchor.observation();
        if observed.expected != config.mount {
            return Err(unavailable(
                "secondary pool mount does not match its configuration",
            ));
        }
        let primary = self
            .entry(self.primary)?
            .and_then(|entry| entry.mount)
            .ok_or_else(|| unavailable("secondary pools require a mounted primary pool"))?;
        same_worker(&primary, &config.mount)?;
        let id = config.mount.identity.pool_id;
        if self.closing.lock().map_err(|_| unavailable("worker pool drain unavailable"))?
            .contains_key(&id)
        {
            return Err(unavailable("pool generation is closing"));
        }
        {
            let entries = self
                .entries
                .lock()
                .map_err(|_| unavailable("worker pool inventory unavailable"))?;
            if let Some(existing) = entries.get(&id) {
                if existing.growth_claim != Some(config.growth_claim)
                    || existing
                        .mount
                        .as_ref()
                        .is_none_or(|mount| !same_open_mount(mount, observed))
                {
                    return Err(unavailable(
                        "pool identity already has a different mount or growth claim",
                    ));
                }
                return Ok(existing.pool.clone());
            }
            if entries.len() >= self.limit {
                return Err(unavailable("worker pool inventory limit reached"));
            }
            for existing in entries.values() {
                if existing.growth_claim == Some(config.growth_claim) {
                    return Err(unavailable("growth claim already owns a pool mount"));
                }
                if let Some(mount) = &existing.mount {
                    distinct(mount, observed)?;
                }
            }
        }
        let observed = observed.clone();
        let mount = crate::oltp::pool::VerifiedPoolMount::from_anchor(anchor, exclusion)?;
        let pool =
            OltpPool::open_registered(mount, config.mount.identity.clone(), self.shared.clone())?;
        self.entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?
            .insert(
                id,
                Entry {
                    pool: pool.clone(),
                    mount: Some(observed),
                    growth_claim: Some(config.growth_claim),
                    retained_startup,
                },
            );
        Ok(pool)
    }

    async fn approved(
        &self,
        control: &ControlHandle,
        entry: &Entry,
        mount: &MountedPoolObservation,
    ) -> Result<bool, ProtocolError> {
        let Some(claim) = entry.growth_claim else {
            return Ok(true);
        };
        let record = control.growth(claim).await?;
        Ok(completion_matches(record.as_ref(), mount, entry.retained_startup))
    }

    /// A retained Control placement can reopen its replica before the worker
    /// joins the queue. It still needs the currently verified physical mount
    /// and the secondary pool's completed Control claim.
    pub(crate) async fn retained_for_group(
        &self,
        id: Uuid,
        control: &ControlHandle,
    ) -> Result<OltpPool, ProtocolError> {
        let entry = self
            .entry(id)?
            .ok_or_else(|| unavailable("retained replica pool is not registered"))?;
        let pool = entry.pool.clone();
        let observed = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.observe_registered_mount()
            })
            .await?;
        if observed.expected.worker != self.worker
            || observed.expected.identity.pool_id != id
            || entry
                .mount
                .as_ref()
                .is_none_or(|mount| !same_open_mount(mount, &observed))
            || !self.approved(control, &entry, &observed).await?
        {
            return Err(unavailable("retained replica pool mount or growth claim differs"));
        }
        Ok(entry.pool)
    }

    /// Placement callers also bind the result to their current Control
    /// authority and the proposed quorum's failure domains.
    pub(crate) async fn eligible(
        &self,
        id: Uuid,
        worker: WorkerIdentity,
        generation: u64,
        control: &ControlHandle,
    ) -> Result<OltpPool, ProtocolError> {
        if control.pool_drain_blocks_placement(id).await? {
            return Err(unavailable("placement pool generation is draining or closed"));
        }
        self.eligible_mount(id, worker, generation, control).await
    }

    /// The older durable claimant blocks the drain proof until it settles.
    pub(crate) async fn eligible_for_claimed_move(
        &self,
        plan: &ReplicaMove,
        control: &ControlHandle,
    ) -> Result<OltpPool, ProtocolError> {
        if plan.phase != ReplicaMovePhase::Moving
            || control.active_replica_move().await?.as_ref() != Some(plan)
        {
            return Err(unavailable("placement intent no longer owns this pool"));
        }
        self.eligible_mount(plan.request.target.pool_id, plan.request.worker,
            plan.request.pool_generation, control).await
    }

    async fn eligible_mount(
        &self,
        id: Uuid,
        worker: WorkerIdentity,
        generation: u64,
        control: &ControlHandle,
    ) -> Result<OltpPool, ProtocolError> {
        if self.worker != worker || !control.growth_worker_ready(worker).await? {
            return Err(unavailable("placement worker incarnation is not ready"));
        }
        let entry = self
            .entry(id)?
            .ok_or_else(|| unavailable("placement pool is not registered"))?;
        let pool = entry.pool.clone();
        let mount = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.observe_registered_mount()
            })
            .await?;
        if mount.expected.worker != worker
            || mount.expected.generation != generation
            || !self.approved(control, &entry, &mount).await?
        {
            return Err(unavailable("placement pool generation is not eligible"));
        }
        Ok(entry.pool)
    }

    pub(crate) async fn observe(
        &self,
        control: &ControlHandle,
    ) -> Result<WorkerPoolInventory, ProtocolError> {
        let _inventory = self
            .shared
            .reserve_scratch(ByteClass::Progress, (self.limit + 1) * (12 << 10))?;
        let entries: Vec<_> = {
            let closing = self.closing.lock().map_err(|_| unavailable("worker pool drain unavailable"))?;
            self.entries
            .lock()
            .map_err(|_| unavailable("worker pool inventory unavailable"))?
            .iter()
            .filter(|(pool, _)| !closing.contains_key(pool))
            .map(|(_, entry)| entry.clone())
            .collect()
        };
        let snapshots = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                Ok(entries
                    .into_iter()
                    .map(|entry| {
                        let mount = entry.pool.observe_registered_mount().ok();
                        (entry, mount)
                    })
                    .collect::<Vec<_>>())
            })
            .await?;
        let ready = control.growth_worker_ready(self.worker).await?;
        let mut pools = Vec::with_capacity(self.limit);
        for (entry, observed) in snapshots {
            let eligible = match &observed {
                Some(mount) => ready
                    && !control.pool_drain_blocks_placement(entry.pool.identity().pool_id).await?
                    && self.approved(control, &entry, mount).await?,
                None => false,
            };
            pools.push(PoolCapacity {
                pool_id: entry.pool.identity().pool_id,
                primary: entry.pool.identity().pool_id == self.primary,
                growth_claim: entry.growth_claim,
                expected: entry.mount.map(|mount| mount.expected),
                mount_id: observed.as_ref().map(|mount| mount.mount_id),
                device: observed
                    .as_ref()
                    .map(|mount| (mount.device_major, mount.device_minor)),
                observed_total_bytes: observed.as_ref().map(|mount| mount.total_bytes),
                observed_available_bytes: observed.as_ref().map(|mount| mount.available_bytes),
                placement_eligible: eligible,
            });
        }
        let scheduler_node_id = self.shared.scheduler_node_id();
        let (_, _, charged_memory_bytes) = self
            .shared
            .scheduler()
            .usage()
            .into_iter()
            .find(|(node_id, _, _)| *node_id == scheduler_node_id)
            .ok_or_else(|| unavailable("worker scheduler capacity unavailable"))?;
        WorkerPoolInventory::from_pools(
            &self.shared,
            self.worker,
            ready,
            self.limit,
            pools,
            charged_memory_bytes,
        )
    }
}

fn same_worker(
    primary: &MountedPoolObservation,
    next: &PoolMountExpectation,
) -> Result<(), ProtocolError> {
    if next.worker != primary.expected.worker
        || next.identity.cluster_id != primary.expected.identity.cluster_id
        || next.identity.worker_id != primary.expected.identity.worker_id
    {
        return Err(unavailable(
            "secondary pool does not match the exact registered worker",
        ));
    }
    Ok(())
}

fn same_open_mount(a: &MountedPoolObservation, b: &MountedPoolObservation) -> bool {
    a.expected == b.expected
        && a.mount_id == b.mount_id
        && (a.device_major, a.device_minor, a.root_inode)
            == (b.device_major, b.device_minor, b.root_inode)
}

fn distinct(a: &MountedPoolObservation, b: &MountedPoolObservation) -> Result<(), ProtocolError> {
    if a.expected.identity.pool_id == b.expected.identity.pool_id
        || a.expected.provider_volume_id == b.expected.provider_volume_id
        || a.expected.mount_path.starts_with(&b.expected.mount_path)
        || b.expected.mount_path.starts_with(&a.expected.mount_path)
        || a.mount_id == b.mount_id
        || (a.device_major, a.device_minor, a.root_inode)
            == (b.device_major, b.device_minor, b.root_inode)
        || (!a.expected.filesystem_uuid.is_empty()
            && a.expected
                .filesystem_uuid
                .eq_ignore_ascii_case(&b.expected.filesystem_uuid))
        || matches!((a.expected.profile, b.expected.profile),
            (PoolMountProfile::Docker { volume_marker: a }, PoolMountProfile::Docker { volume_marker: b }) if a == b)
    {
        return Err(unavailable(
            "pool mount, filesystem root or provider identity is reused",
        ));
    }
    Ok(())
}

fn completion_matches(
    record: Option<&GrowthRecord>,
    mount: &MountedPoolObservation,
    retained_startup: bool,
) -> bool {
    let Some(record) = record else {
        return false;
    };
    let expected = &mount.expected;
    let intent = &record.intent;
    let exact = intent.worker == expected.worker && intent.pool_generation == expected.generation;
    let retained = retained_startup
        && intent.worker.worker_id == expected.worker.worker_id
        && intent.worker.enrollment_revision < expected.worker.enrollment_revision
        && intent.pool_generation < expected.generation;
    record.phase == GrowthPhase::Complete
        && matches!(intent.action, Action::AddPool { .. })
        && intent.pool_id == expected.identity.pool_id
        && record.provider_resource.as_deref() == Some(expected.provider_volume_id.as_str())
        && record
            .mounted_total_bytes
            .is_some_and(|total| total <= mount.total_bytes)
        && (exact || retained)
}

#[derive(Serialize)]
pub(crate) struct WorkerPoolInventory {
    worker: WorkerIdentity,
    ready: bool,
    observed_at: u64,
    scheduler_node_id: u64,
    thread_budget: NativeThreadBudget,
    cache_bytes: usize,
    write_buffer_bytes: usize,
    native_baseline_bytes: u64,
    charged_memory_bytes: u64,
    local_open_pool_limit: usize,
    pools: Vec<PoolCapacity>,
    backing_filesystems: Vec<BackingFilesystem>,
    total_available_bytes: u64,
}

impl WorkerPoolInventory {
    pub(crate) fn for_joined_data_worker(
        shared: &WorkerSharedResources,
        proof: &DataWorkerGrowthProof,
        primary: MountedPoolObservation,
        added: Option<MountedPoolObservation>,
        charged_memory_bytes: u64,
    ) -> Result<Self, ProtocolError> {
        let mount_error = || unavailable("native mount differs from Control growth registration");
        if !proof.primary.matches_mount(&primary) {
            return Err(mount_error());
        }
        let mut pools = vec![PoolCapacity::registered(
            primary,
            proof.primary.claim_id,
            true,
        )];
        match (&proof.added, added) {
            (Some(registered), Some(mount)) if registered.matches_mount(&mount) => {
                pools.push(PoolCapacity::registered(mount, registered.claim_id, false));
            }
            (None, None) => {}
            _ => return Err(mount_error()),
        }
        // A joined process opens a fixed mount set at bootstrap. An EKS pool
        // addition requires a different process and a new queue cut.
        Self::from_pools(
            shared,
            proof.primary.mount.worker,
            true,
            pools.len(),
            pools,
            charged_memory_bytes,
        )
    }

    fn from_pools(
        shared: &WorkerSharedResources,
        worker: WorkerIdentity,
        ready: bool,
        local_open_pool_limit: usize,
        pools: Vec<PoolCapacity>,
        charged_memory_bytes: u64,
    ) -> Result<Self, ProtocolError> {
        let mut backing = BTreeMap::<(u32, u32), BackingFilesystem>::new();
        for pool in pools.iter().filter(|pool| pool.placement_eligible) {
            let (Some(device), Some(total_bytes), Some(available_bytes)) = (
                pool.device,
                pool.observed_total_bytes,
                pool.observed_available_bytes,
            ) else {
                return Err(unavailable("eligible pool has no filesystem observation"));
            };
            backing
                .entry(device)
                .and_modify(|filesystem| {
                    filesystem.total_bytes = filesystem.total_bytes.min(total_bytes);
                    filesystem.available_bytes = filesystem.available_bytes.min(available_bytes);
                })
                .or_insert(BackingFilesystem {
                    device_major: device.0,
                    device_minor: device.1,
                    total_bytes,
                    available_bytes,
                });
        }
        let total_available_bytes = backing
            .values()
            .try_fold(0u64, |total, filesystem| {
                total.checked_add(filesystem.available_bytes)
            })
            .ok_or_else(|| unavailable("available filesystem bytes exceed inventory range"))?;
        let config = shared.config();
        Ok(Self {
            worker,
            ready,
            observed_at: nabedra_rt::time::wall_now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| unavailable("worker inventory clock unavailable"))?
                .as_secs(),
            scheduler_node_id: shared.scheduler_node_id(),
            thread_budget: shared.thread_budget(),
            cache_bytes: config.cache_bytes,
            write_buffer_bytes: config.write_buffer_bytes,
            native_baseline_bytes: shared.native_baseline_bytes(),
            charged_memory_bytes,
            local_open_pool_limit,
            pools,
            backing_filesystems: backing.into_values().collect(),
            total_available_bytes,
        })
    }
}

#[derive(Serialize)]
struct PoolCapacity {
    pool_id: Uuid,
    primary: bool,
    growth_claim: Option<Uuid>,
    expected: Option<PoolMountExpectation>,
    mount_id: Option<u64>,
    device: Option<(u32, u32)>,
    observed_total_bytes: Option<u64>,
    observed_available_bytes: Option<u64>,
    placement_eligible: bool,
}
impl PoolCapacity {
    fn registered(mount: MountedPoolObservation, claim_id: Uuid, primary: bool) -> Self {
        Self {
            pool_id: mount.expected.identity.pool_id,
            primary,
            growth_claim: Some(claim_id),
            expected: Some(mount.expected),
            mount_id: Some(mount.mount_id),
            device: Some((mount.device_major, mount.device_minor)),
            observed_total_bytes: Some(mount.total_bytes),
            observed_available_bytes: Some(mount.available_bytes),
            placement_eligible: true,
        }
    }
}
/// Measure each backing filesystem once, even when several Docker volumes
/// bind distinct directories from that filesystem into this worker.
#[derive(Serialize)]
struct BackingFilesystem {
    device_major: u32,
    device_minor: u32,
    total_bytes: u64,
    available_bytes: u64,
}

fn unavailable(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::NotApplied)
}
