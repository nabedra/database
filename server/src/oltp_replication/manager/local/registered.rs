//! Operator-pinned directory-profile multi-worker startup. This constructor
//! proves exclusive local process/directory ownership. It does not attest an
//! EBS/CSI mount or an AZ, and never grants queue readiness by itself.
use super::*;
use crate::config::ExecutionConfig;
use crate::oltp::backup::restore::{
    OfflineRestoreTarget, RestoredGenesis, verified_restore_marker,
};
use crate::oltp::pool::mount::{PoolMountExpectation, PoolMountProfile, VerifiedPoolMount};
use crate::oltp_backup_repository::{ArchiveEncryptionConfig, ArchiveKeys};
use crate::oltp_deployment::inventory::{SecondaryPoolConfig, WorkerPools};
use crate::oltp_replication::authority::{control::{OperatorPolicy, ReplacementRecord}, status::StatusTopology};
use crate::oltp_replication::types::{Page, validate_voters};
use std::{collections::BTreeSet, net::SocketAddr};
mod cold;
mod multi_pool;
mod restart;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupPlacement {
    pub identity: GroupIdentity,
    pub voters: Page<ReplicaMember>,
}
impl GroupPlacement {
    pub(crate) fn local(&self, worker: Uuid) -> Option<ReplicaMember> {
        self.voters
            .iter()
            .find(|member| member.worker_id == worker)
            .cloned()
    }
    pub(crate) fn members(&self) -> BTreeMap<u64, ReplicaMember> {
        self.voters
            .iter()
            .map(|member| (member.node_id, member.clone()))
            .collect()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SystemPlacement {
    pub control: GroupPlacement,
    pub timestamps: GroupPlacement,
    pub status: GroupPlacement,
    #[serde(default = "empty_status_partitions", skip_serializing_if = "status_partitions_empty")]
    pub status_partitions: Page<GroupPlacement>,
    pub catalog: GroupPlacement,
}

fn empty_status_partitions() -> Page<GroupPlacement> {
    Page::new(Vec::new()).expect("empty placement page")
}

fn status_partitions_empty(placements: &Page<GroupPlacement>) -> bool {
    placements.is_empty()
}

impl SystemPlacement {
    pub(crate) fn status_groups(&self) -> impl Iterator<Item = &GroupPlacement> {
        std::iter::once(&self.status).chain(self.status_partitions.iter())
    }

    pub(crate) fn status_topology(&self) -> Result<StatusTopology, ProtocolError> {
        StatusTopology::new(self.status_groups().map(|group| group.identity.clone()).collect())
    }

    pub(crate) fn all(&self) -> impl Iterator<Item = &GroupPlacement> {
        [&self.control, &self.timestamps].into_iter()
            .chain(self.status_groups())
            .chain(std::iter::once(&self.catalog))
    }

    pub(crate) fn validate(&self, required_voters: usize) -> Result<(), ProtocolError> {
        self.status_topology()?;
        let control = &self.control.identity;
        let mut ids = BTreeSet::new();
        let kinds = [GroupKind::Control, GroupKind::Timestamps].into_iter()
            .chain(self.status_groups().map(|group| group.identity.kind.clone()))
            .chain(std::iter::once(GroupKind::Catalog));
        for (group, kind) in self.all().zip(kinds) {
            group.identity.validate().map_err(|_| invalid())?;
            if group.identity.cluster_id != control.cluster_id
                || group.identity.database != control.database
                || group.identity.kind != kind
                || !ids.insert(group.identity.group_id)
                || group.voters.len() != required_voters
            {
                return Err(invalid());
            }
            validate_voters(&group.voters).map_err(|_| invalid())?;
            let mut last = None;
            for member in group.voters.iter() {
                if last.is_some_and(|previous| member.node_id <= previous) {
                    return Err(invalid());
                }
                last = Some(member.node_id);
            }
        }
        Ok(())
    }
}
/// A bounded, explicitly authorized fresh topology. Stable group and replica
/// identities are configuration, never chosen by the first responding peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredConfig {
    pub format: u32,
    #[serde(default, skip_serializing_if = "disabled")]
    pub scoped_oltp: bool,
    pub directory: PathBuf,
    pub pool_id: Uuid,
    pub scheduler_node_id: u64,
    pub enrollment: WorkerEnrollment,
    pub bind: SocketAddr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub authority_ca: PathBuf,
    pub bootstrap_id: Uuid,
    pub initial_run: Uuid,
    pub required_voters: u8,
    pub placements: SystemPlacement,
    pub control_enrollments: Page<WorkerEnrollment>,
    pub operator_pins: Page<Digest>,
    #[serde(default)]
    pub mount: Option<PoolMountExpectation>,
    #[serde(default = "multi_pool::empty")]
    pub secondary_pools: Page<SecondaryPoolConfig>,
    #[serde(default)]
    pub restore: Option<RegisteredRestoreConfig>,
    #[serde(default)]
    pub archive_repository: Option<PathBuf>,
    #[serde(default)]
    pub archive_repository_s3: Option<crate::oltp_backup_repository::S3ArchiveConfig>,
    #[serde(default)]
    pub archive_encryption: Option<ArchiveEncryptionConfig>,
    #[serde(default)]
    pub cold: Option<cold::ColdStart>,
    #[serde(default)]
    pub rolling: Option<restart::RollingStart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_replacement: Option<ReplacementRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_placements: Option<SystemPlacement>,
}

fn disabled(value: &bool) -> bool {
    !*value
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedIdentity {
    format: u32,
    bootstrap_id: Uuid,
    worker: Uuid,
    pool: Uuid,
    placements: SystemPlacement,
    directory_dev: u64,
    directory_inode: u64,
    lock_dev: u64,
    lock_inode: u64,
    previous: WorkerIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    system_replacement: Option<Uuid>,
    #[serde(default, skip_serializing_if = "disabled")]
    replacement_requires_restart: bool,
    #[serde(default)]
    restore_policy: Option<Digest>,
    #[serde(default)]
    cold: Option<cold::RetainedCold>,
}
pub(crate) struct RegisteredWorker {
    pub manager: Arc<HostedGroupManager>,
    pub shared: Arc<WorkerSharedResources>,
    pub pool: OltpPool,
    pub pools: Arc<WorkerPools>,
    secondary: Vec<(SecondaryPoolConfig, crate::oltp::pool::mount::VerifiedMountAnchor)>,
    pub enrollment: WorkerEnrollment,
    pub placements: SystemPlacement,
    pub bootstrap_id: Uuid,
    pub initial_run: Uuid,
    pub bootstrap_policy: Digest,
    pub restored: Option<RestoredGenesis>,
    pub required_voters: u8,
    pub previous_process: Option<WorkerIdentity>,
    pub cold: Option<cold::OpenedCold>,
    pub rolling: Option<restart::OpenedRestart>,
    pub system_replacement: Option<ReplacementRecord>,
    pub replacement_mount_committed: bool,
    replacement_retained: Option<(Arc<DirectoryOwner>, RetainedIdentity)>,
    pub restart_mount: Option<restart::RestartMount>,
    pub operator: LocalOperatorPermit,
    pub transport: Arc<WorkerTransport>,
    pub listener: Option<TcpListener>,
    pub directory: Arc<crate::oltp_replication::multiplex::AppliedPeerDirectory>,
}
/// The system groups are hosted on each configured voter, but are initialized
/// by one deterministic bootstrap voter. Starting them grants neither queue
/// readiness nor authority to execute application work.
pub(crate) struct RegisteredSystemGroups {
    /// Reports run-time damage of any hosted replica once connected to Control.
    pub damage: Arc<crate::oltp_replication::manager::ReportingDamageSink>,
    pub control: Option<ManagedControl>,
    pub timestamps: Option<ManagedTimestamps>,
    pub status: Vec<ManagedStatus>,
    pub catalog: Option<ManagedCatalog>,
}
impl RegisteredWorker {
    pub(crate) fn seal_replacement_identity(&mut self) -> Result<(), ProtocolError> {
        if let Some((owner, mut record)) = self.replacement_retained.take() {
            // Once this incarnation may join the queue, a process restart
            // must use the regular retained-mount protocol and a new identity.
            record.replacement_requires_restart = true;
            retain_identity(&owner, &record)?;
        }
        Ok(())
    }

    pub(crate) async fn open_secondary_pools(
        &mut self,
        control: &crate::oltp_replication::authority::control::ControlHandle,
        certificate_pins: BTreeSet<Digest>,
    ) -> Result<(), ProtocolError> {
        multi_pool::open_configured(
            &self.pools,
            &mut self.secondary,
            control,
            &self.operator,
            OperatorPolicy { policy: self.bootstrap_policy, certificate_pins },
        ).await
    }

    async fn open_system_store(
        &self,
        identity: GroupIdentity,
    ) -> Result<GroupStore, ProtocolError> {
        if self.cold.is_some() || self.rolling.is_some() {
            self.pool.retained_group(identity).await
        } else {
            self.pool.group(identity).await
        }
    }

    async fn check_system_group<C, M>(
        &self,
        running: &Managed<C, M>,
        placement: &GroupPlacement,
        local: &ReplicaMember,
    ) -> Result<(), ProtocolError>
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
        M: TypedMachine<C>,
    {
        if self.system_replacement.is_some() {
            return Ok(());
        }
        if self.cold.is_none() && self.rolling.is_none() {
            return initialize_seed(running, placement, local).await;
        }
        if !running.raft.is_initialized().await.map_err(|_| unavailable())? {
            return Err(invalid());
        }
        Ok(())
    }

    /// Start the listener before calling this method. All groups start before
    /// waiting for a leader so another process can complete their quorum.
    pub(crate) async fn start_system_groups(
        &self,
        config: openraft::Config,
    ) -> Result<RegisteredSystemGroups, ProtocolError> {
        let worker = self.enrollment.worker_id;
        // NS14b: the reporter exists before any system group starts, so a
        // run-time stop of one is reported once Control is reachable.
        let damage = crate::oltp_replication::manager::ReportingDamageSink::new(Default::default());
        self.manager.install_damage_sink(damage.clone());
        let control = if let Some(local) = self.placements.control.local(worker) {
            let group = &self.placements.control;
            let running = self
                .manager
                .start_registered_control(
                    self.open_system_store(group.identity.clone()).await?,
                    local.clone(),
                    group.members(),
                    config.clone(),
                    self.directory.clone(),
                )
                .await?;
            self.check_system_group(&running, group, &local).await?;
            Some(running)
        } else {
            None
        };
        let timestamps = if let Some(local) = self.placements.timestamps.local(worker) {
            let group = &self.placements.timestamps;
            let running = self
                .manager
                .start_timestamps(
                    self.open_system_store(group.identity.clone()).await?,
                    local.clone(),
                    group.members(),
                    config.clone(),
                )
                .await?;
            self.check_system_group(&running, group, &local).await?;
            Some(running)
        } else {
            None
        };
        let mut status = Vec::new();
        for group in self.placements.status_groups() {
            if let Some(local) = group.local(worker) {
                let running = self.manager.start_status(
                    self.open_system_store(group.identity.clone()).await?,
                    local.clone(),
                    group.members(),
                    config.clone(),
                ).await?;
                self.check_system_group(&running, group, &local).await?;
                status.push(running);
            }
        }
        let catalog = if let Some(local) = self.placements.catalog.local(worker) {
            let group = &self.placements.catalog;
            let running = self
                .manager
                .start_catalog(
                    self.open_system_store(group.identity.clone()).await?,
                    local.clone(),
                    group.members(),
                    config,
                )
                .await?;
            self.check_system_group(&running, group, &local).await?;
            Some(running)
        } else {
            None
        };
        Ok(RegisteredSystemGroups {
            damage,
            control,
            timestamps,
            status,
            catalog,
        })
    }
    pub(crate) async fn open_development(
        mut config: RegisteredConfig,
        limits: WorkerResources,
        resources: SharedResourceConfig,
        execution: &ExecutionConfig,
        archive_keys: Arc<ArchiveKeys>,
    ) -> Result<Self, ProtocolError> {
        if !matches!(
            (config.format, config.placements.status_partitions.is_empty()),
            (1, true) | (2, false)
        )
            || config.bootstrap_id.is_nil()
            || config.initial_run.is_nil()
            || config.pool_id.is_nil()
            || config.scheduler_node_id == 0
            || !matches!(config.required_voters, 1 | 3 | 5)
        {
            return Err(invalid());
        }
        cold::validate(&config)?;
        restart::validate(&config)?;
        config
            .placements
            .validate(config.required_voters as usize)?;
        config.enrollment.validate().map_err(|_| invalid())?;
        let replacement = config.system_replacement.clone();
        let mut placements = config.placements.clone();
        if let Some(record) = &replacement {
            install_replacement_placements(&config, record, &mut placements)?;
        }
        let control = &config.placements.control;
        let mut enrolled = BTreeSet::new();
        for enrollment in config.control_enrollments.iter() {
            enrollment.validate().map_err(|_| invalid())?;
            if !enrolled.insert(enrollment.worker_id)
                || !control
                    .voters
                    .iter()
                    .any(|voter| voter.worker_id == enrollment.worker_id)
            {
                return Err(invalid());
            }
        }
        if enrolled.len() != control.voters.len()
            || (replacement.is_none() && !config
                .control_enrollments
                .iter()
                .any(|member| {
                    if config.rolling.is_some() {
                        member.worker_id == config.enrollment.worker_id && member != &config.enrollment
                    } else {
                        member == &config.enrollment
                    }
                }))
        {
            return Err(invalid());
        }
        // A system voter must have an authenticated Control enrollment.
        if replacement.is_none()
            && config.placements.all().any(|placement| {
                placement
                    .voters
                    .iter()
                    .any(|voter| !enrolled.contains(&voter.worker_id))
            })
        {
            return Err(invalid());
        }
        let bootstrap = config.bootstrap_placements.as_ref().unwrap_or(&config.placements);
        bootstrap.validate(config.required_voters as usize)?;
        if !bootstrap.all().map(|group| &group.identity).eq(config.placements.all().map(|group| &group.identity)) {
            return Err(invalid());
        }
        let policy = if config.format == 1 {
            crate::oltp_replication::owned::serialized_digest(&(
                b"nabedra/registered-policy/v1",
                config.bootstrap_id,
            ))?
        } else {
            crate::oltp_replication::owned::serialized_digest(&(
                b"nabedra/registered-policy/v2",
                config.bootstrap_id,
                bootstrap,
            ))?
        };
        let mut local_groups = 0;
        for placement in placements.all() {
            if let Some(local) = placement.local(config.enrollment.worker_id) {
                if local.pool_id != config.pool_id {
                    return Err(invalid());
                }
                local_groups += 1;
            }
        }
        if local_groups == 0 {
            return Err(invalid());
        }
        if let Some(record) = &replacement {
            local_groups = record.groups.len();
        }
        let tls = TlsMaterial::from_pem(
            &bounded_read(&config.certificate, 64 << 10).map_err(storage)?,
            &bounded_read(&config.private_key, 64 << 10).map_err(storage)?,
            &bounded_read(&config.authority_ca, 64 << 10).map_err(storage)?,
        )
        .map_err(storage)?;
        if Digest::parse(tls.certificate_sha256()).map_err(|_| invalid())?
            != config.enrollment.certificate_sha256
        {
            return Err(invalid());
        }
        let operator_pins = config
            .operator_pins
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if operator_pins.len() != config.operator_pins.len()
            || (replacement.is_none() && !operator_pins.contains(&config.enrollment.certificate_sha256))
        {
            return Err(invalid());
        }
        let identity = PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
            cluster_id: control.identity.cluster_id,
            worker_id: config.enrollment.worker_id,
            pool_id: config.pool_id,
        };
        // Inspect the real mount before creating a worker/control identity or
        // lock file. A missing mount must never become a root-disk database.
        let anchor = config
            .mount
            .clone()
            .map(|expected| {
                expected.refuse_legacy_format()?;
                if expected.identity != identity
                    || expected.worker != WorkerIdentity::from(&config.enrollment)
                    || expected.mount_path != config.directory
                {
                    return Err(invalid());
                }
                expected.inspect_mount()
            })
            .transpose()?;
        let primary_mount = anchor.as_ref().map(|mount| mount.observation().clone());
        let secondary_anchors = WorkerPools::validate_secondary_set(
            &config.secondary_pools,
            primary_mount.as_ref(),
            resources.max_open_pools,
        )?;
        let shared = WorkerSharedResources::new(config.scheduler_node_id, limits, resources)?;
        shared.validate_registered_group_capacity(
            execution,
            1 + config.secondary_pools.len(),
            local_groups,
        )?;
        shared.bind_worker(config.enrollment.worker_id)?;
        let restore = match &config.restore {
            Some(input) => Some(
                input
                    .verify(&config, shared.clone(), archive_keys)
                    .await?,
            ),
            None => None,
        };
        let ownership = Arc::new(lock_directory(&config.directory)?);
        let mounted_worker = if let Some(anchor) = &anchor {
            verify_directory_anchor(&ownership, anchor.observation())?;
            anchor.retained_worker_if_initialized()?
        } else {
            None
        };
        let control_identity = control.identity.clone();
        config.placements = placements.clone();
        let (previous_process, mut retained) = inspect_identity(&ownership, &config, &identity)?;
        if restore.is_some() && previous_process.is_some() {
            return Err(invalid());
        }
        let cold = cold::OpenedCold::new(&config, ownership.clone(), previous_process, &retained)?;
        let rolling = restart::OpenedRestart::new(&config, ownership.clone(), previous_process, &retained)?;
        let operator = LocalOperatorPermit::from_local_bootstrap(
            WorkerIdentity::from(&config.enrollment),
            shared.clone(),
            ownership.clone(),
        )?;
        let listener = TcpListener::bind(config.bind).await.map_err(storage)?;
        if listener.local_addr().map_err(storage)?.port() != config.bind.port()
            || config.bind.port() == 0
        {
            return Err(invalid());
        }
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let mut bootstrap_peers = config.control_enrollments.to_vec();
        if rolling.is_some() {
            bootstrap_peers.retain(|peer| peer.worker_id != config.enrollment.worker_id);
        }
        if let Some(record) = &replacement {
            bootstrap_peers.retain(|peer| peer.worker_id != record.intent.previous.worker_id);
        }
        let directory = crate::oltp_replication::multiplex::AppliedPeerDirectory::from_bootstrap(
            control_identity.clone(),
            bootstrap_peers,
            shared.clone(),
        )?;
        directory.set_cold_bootstrap(cold.is_some());
        if let Some(record) = &replacement {
            directory.allow_replacement_bootstrap(record)?;
        }
        let transport = Arc::new(WorkerTransport::new(
            &config.enrollment,
            &tls,
            shared.clone(),
            directory.clone(),
            registry.clone(),
            TransportConfig::default(),
        )?);
        let manager = Arc::new(HostedGroupManager {
            shared: shared.clone(),
            client: transport.client(),
            registry,
            routes: Arc::new(Mutex::new(BTreeMap::new())),
            ownership: Some(ownership.clone()),
            starts: tokio::sync::Mutex::new(()),
            damage_sink: Arc::new(Mutex::new(None)),
            health: Arc::default(),
        });
        let mut restart_mount = None;
        let mut replacement_mount_committed = false;
        if let Some(record) = &replacement {
            let mount = restart::RestartMount::open(&ownership, primary_mount.as_ref(), true)?
                .ok_or_else(invalid)?;
            let proof = crate::oltp_replication::authority::control::ReplacementMountProof::sign(
                &control_identity,
                record,
                &record.intent.mount,
                &mount.key,
            )?;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            replacement_mount_committed = tokio::time::timeout_at(deadline, async {
                loop {
                    for peer in config
                        .control_enrollments
                        .iter()
                        .filter(|peer| peer.worker_id != record.intent.previous.worker_id)
                    {
                        match manager
                            .check_replacement_mount(
                                &control_identity,
                                WorkerIdentity::from(peer),
                                &proof,
                                previous_process.is_some(),
                            )
                            .await
                        {
                            Ok(mounted) => return Ok(mounted),
                            Err(error) if error.code == ErrorCode::Conflict => return Err(error),
                            Err(_) => {}
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            })
            .await
            .map_err(|_| unavailable())??;
            restart_mount = Some(mount);
        }
        let mut restored = None;
        let pool = if let Some(anchor) = anchor {
            let exclusion = match (previous_process, anchor.observation().expected.profile) {
                (Some(_), PoolMountProfile::Docker { .. }) if replacement.is_none() => {
                    // The mount manifest can advance before Control is
                    // reached. Recover the last opened mount separately
                    // from the journal of eligible queue holders.
                    let previous = mounted_worker.ok_or_else(invalid)?;
                    Some(operator.exclude_previous_before_open(
                        &OperatorPolicy {
                            policy: retained.restore_policy.unwrap_or(policy),
                            certificate_pins: operator_pins,
                        },
                        Uuid::new_v4(),
                        previous,
                    )?)
                }
                (Some(_), PoolMountProfile::Ebs) => {
                    return Err(ProtocolError::new(
                        ErrorCode::UnsupportedOperation,
                        "EBS pool reopen requires an external previous-owner exclusion proof",
                        Outcome::NotApplied,
                    ));
                }
                _ => None,
            };
            let mount = VerifiedPoolMount::from_anchor(anchor, exclusion.as_ref())?;
            let target = restore
                .as_ref()
                .map(|verified| OfflineRestoreTarget::claim_mounted(&mount, &verified.plan))
                .transpose()?;
            let pool = OltpPool::open_registered(mount, identity, shared.clone())?;
            if let Some((verified, target)) = restore.zip(target) {
                let (genesis, _) = verified.install(target, &pool).await?;
                retained.restore_policy =
                    Some(super::restore::registered_policy(&config, &genesis)?);
                restored = Some(genesis);
            }
            pool
        } else {
            OltpPool::open_development(ownership.path.join("pool"), identity, shared.clone())?
        };
        let bootstrap_policy = retained.restore_policy.unwrap_or(policy);
        if replacement.is_none() {
            restart_mount =
                restart::RestartMount::open(&ownership, primary_mount.as_ref(), rolling.is_some())?;
        }
        let pools = WorkerPools::new(
            pool.clone(),
            WorkerIdentity::from(&config.enrollment),
            primary_mount,
        )?;
        let secondary = config
            .secondary_pools
            .into_inner()
            .into_iter()
            .zip(secondary_anchors)
            .collect();
        if cold.is_none() && rolling.is_none() {
            retain_identity(&ownership, &retained)?;
        }
        let replacement_retained = replacement.as_ref().map(|_| (ownership.clone(), retained));
        Ok(Self {
            manager,
            shared,
            pool,
            pools,
            secondary,
            enrollment: config.enrollment,
            placements: config.placements,
            bootstrap_id: config.bootstrap_id,
            initial_run: config.initial_run,
            bootstrap_policy,
            restored,
            required_voters: config.required_voters,
            previous_process,
            cold,
            rolling,
            system_replacement: replacement,
            replacement_mount_committed,
            replacement_retained,
            restart_mount,
            operator,
            transport,
            listener: Some(listener),
            directory,
        })
    }
}
pub(crate) async fn initialize_seed<C, M>(
    running: &Managed<C, M>,
    placement: &GroupPlacement,
    local: &ReplicaMember,
) -> Result<(), ProtocolError>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    if placement
        .voters
        .first()
        .is_some_and(|member| member.node_id == local.node_id)
        && !running
            .raft
            .is_initialized()
            .await
            .map_err(|_| unavailable())?
    {
        // Another invocation may have initialized this very same static group.
        // An initialization failure cannot be hidden unless durable membership
        // now exists; later membership changes use the normal Raft protocol.
        if running.raft.initialize(placement.members()).await.is_err()
            && !running
                .raft
                .is_initialized()
                .await
                .map_err(|_| unavailable())?
        {
            return Err(unavailable());
        }
    }
    Ok(())
}
fn inspect_identity(
    owner: &DirectoryOwner,
    config: &RegisteredConfig,
    identity: &PoolIdentity,
) -> Result<(Option<WorkerIdentity>, RetainedIdentity), ProtocolError> {
    let path = owner.path.join("NABEDRA_REGISTERED_WORKER.json");
    let (directory_dev, directory_inode, lock_dev, lock_inode) = directory_identity(owner)?;
    let (previous, restore_policy, cold) = match bounded_read(&path, 128 << 10) {
        Ok(bytes) => {
            let old: RetainedIdentity = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            let resume = config.system_replacement.as_ref().is_some_and(|record|
                old.system_replacement == Some(record.intent.id)
                    && !old.replacement_requires_restart
                    && old.previous == WorkerIdentity::from(&config.enrollment));
            if old.format != 1
                || old.bootstrap_id != config.bootstrap_id
                || old.worker != config.enrollment.worker_id
                || old.pool != config.pool_id
                || !same_local_roots(
                    &old.placements,
                    &config.placements,
                    config.enrollment.worker_id,
                )
                || (
                    old.directory_dev,
                    old.directory_inode,
                    old.lock_dev,
                    old.lock_inode,
                ) != (directory_dev, directory_inode, lock_dev, lock_inode)
                // The pool's store exists: `pool/kv/CURRENT` (format 2). A format 1
                // `pool/CURRENT` passes here so the pool open refuses it with
                // `UnsupportedFormat` (as `local.rs` does). T9 finding: requiring only
                // `pool/CURRENT` refused every registered worker restart on `nabedra-kv`.
                || !(owner.path.join("pool/kv/CURRENT").is_file()
                    || owner.path.join("pool/CURRENT").is_file())
                || (!resume && (config.enrollment.enrollment_revision <= old.previous.enrollment_revision
                    || config.enrollment.incarnation == old.previous.incarnation))
            {
                return Err(invalid());
            }
            if old.restore_policy.is_some() {
                let restore = verified_restore_marker(&owner.path, identity)?;
                let status_ids = std::iter::once(restore.initial_status_group)
                    .chain(restore.status_partitions);
                if !config.placements.status_groups().map(|group| group.identity.group_id).eq(status_ids) {
                    return Err(invalid());
                }
            } else if owner
                .path
                .join("RESTORE_IN_PROGRESS.json")
                .symlink_metadata()
                .is_ok()
            {
                return Err(invalid());
            }
            (Some(old.previous), old.restore_policy, old.cold)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if owner.path.join("NABEDRA_TYPED_WORKER.json").exists()
                || owner.path.join("NABEDRA_OLTP.json").exists()
                || owner.path.join("pool").exists()
                || owner.path.join("CURRENT").exists()
                || owner
                    .path
                    .join("RESTORE_IN_PROGRESS.json")
                    .symlink_metadata()
                    .is_ok()
            {
                return Err(invalid());
            }
            (None, None, None)
        }
        Err(error) => return Err(storage(error)),
    };
    Ok((
        previous,
        RetainedIdentity {
            format: 1,
            bootstrap_id: config.bootstrap_id,
            worker: config.enrollment.worker_id,
            pool: config.pool_id,
            placements: config.placements.clone(),
            directory_dev,
            directory_inode,
            lock_dev,
            lock_inode,
            previous: WorkerIdentity::from(&config.enrollment),
            system_replacement: config.system_replacement.as_ref().map(|record| record.intent.id),
            replacement_requires_restart: false,
            restore_policy,
            cold,
        },
    ))
}
fn retain_identity(owner: &DirectoryOwner, next: &RetainedIdentity) -> Result<(), ProtocolError> {
    let path = owner.path.join("NABEDRA_REGISTERED_WORKER.json");
    let temporary = owner
        .path
        .join(format!("registered-{}.next", Uuid::new_v4()));
    let mut output = private_file(&temporary, true).map_err(storage)?;
    let encoded = serde_json::to_vec(next).map_err(|_| invalid())?;
    if encoded.len() > 128 << 10 {
        return Err(invalid());
    }
    output.write_all(&encoded).map_err(storage)?;
    output.sync_all().map_err(storage)?;
    drop(output);
    fs::rename(temporary, path).map_err(storage)?;
    File::open(&owner.path)
        .and_then(|file| file.sync_all())
        .map_err(storage)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn verify_directory_anchor(
    owner: &DirectoryOwner,
    observation: &crate::oltp::pool::mount::MountedPoolObservation,
) -> Result<(), ProtocolError> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(&owner.path).map_err(storage)?;
    if libc::major(metadata.dev()) != observation.device_major
        || libc::minor(metadata.dev()) != observation.device_minor
        || metadata.ino() != observation.root_inode
    {
        return Err(invalid());
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn verify_directory_anchor(
    _: &DirectoryOwner,
    _: &crate::oltp::pool::mount::MountedPoolObservation,
) -> Result<(), ProtocolError> {
    Err(invalid())
}

fn same_local_roots(previous: &SystemPlacement, next: &SystemPlacement, worker: Uuid) -> bool {
    previous.status_groups().count() == next.status_groups().count()
        && previous
            .all()
            .zip(next.all())
            .all(|(old, new)| old.identity == new.identity && old.local(worker) == new.local(worker))
}

#[cfg(test)]
mod status_tests {
    use super::*;

    #[test]
    fn o17_replacement_leaves_system_groups_without_the_lost_worker_unchanged() {
        use crate::oltp_replication::authority::control::ReplacementIntent;
        let lost = Uuid::new_v4();
        let cluster_id = Uuid::new_v4();
        let enrolled = |worker_id| WorkerEnrollment {
            worker_id,
            incarnation: Uuid::new_v4(),
            enrollment_revision: 1,
            certificate_sha256: Digest::from_bytes([1; 32]),
            endpoint: Text::try_from("127.0.0.1:1").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
        };
        let next = enrolled(Uuid::new_v4());
        let mut group = GroupPlacement {
            identity: GroupIdentity {
                cluster_id,
                database: Text::try_from("routing").unwrap(),
                group_id: Uuid::new_v4(),
                kind: GroupKind::Catalog,
                group_format_version: GROUP_FORMAT_VERSION,
            },
            voters: Page::new(
                (1..=3)
                    .map(|node_id| ReplicaMember {
                        node_id,
                        worker_id: Uuid::new_v4(),
                        pool_id: Uuid::new_v4(),
                    })
                    .collect(),
            )
            .unwrap(),
        };
        let record = ReplacementRecord {
            applied_at: AppliedPosition {
                term: 1,
                node_id: 1,
                index: 3,
            },
            intent: ReplacementIntent {
                id: Uuid::new_v4(),
                run_id: Uuid::new_v4(),
                previous: WorkerIdentity::from(&enrolled(lost)),
                mount_public_key: None,
                mount: PoolMountExpectation {
                    profile: PoolMountProfile::Docker {
                        volume_marker: Uuid::new_v4(),
                    },
                    identity: PoolIdentity {
                        engine: "oltp".into(),
                        pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                        cluster_id,
                        worker_id: next.worker_id,
                        pool_id: Uuid::new_v4(),
                    },
                    worker: WorkerIdentity::from(&next),
                    generation: 1,
                    mount_path: "/test/replacement".into(),
                    provider_volume_id: "docker-volume:replacement".into(),
                    filesystem_uuid: String::new(),
                },
                next,
            },
            groups: Vec::new(),
        };
        let original = group.clone();
        install_replacement_group(&mut group, &record).unwrap();
        assert_eq!(group, original);
        let mut voters = group.voters.to_vec();
        voters[0].worker_id = lost;
        group.voters = Page::new(voters).unwrap();
        assert!(install_replacement_group(&mut group, &record).is_err());
    }

    #[test]
    fn o10_o11_r05_retained_system_topology_keeps_historical_status_ownership() {
        let local = ReplicaMember {
            node_id: 1,
            worker_id: Uuid::from_u128(100),
            pool_id: Uuid::from_u128(101),
        };
        let placement = |id, kind| GroupPlacement {
            identity: GroupIdentity {
                cluster_id: Uuid::from_u128(42),
                database: Text::try_from("routing").unwrap(),
                group_id: Uuid::from_u128(id),
                kind,
                group_format_version: GROUP_FORMAT_VERSION,
            },
            voters: Page::new(vec![local.clone()]).unwrap(),
        };
        let legacy = SystemPlacement {
            control: placement(10, GroupKind::Control),
            timestamps: placement(11, GroupKind::Timestamps),
            status: placement(12, GroupKind::Status { bucket: 0 }),
            status_partitions: empty_status_partitions(),
            catalog: placement(14, GroupKind::Catalog),
        };
        let legacy_json = serde_json::to_value(&legacy).unwrap();
        assert!(!legacy_json.as_object().unwrap().contains_key("status_partitions"));
        let loaded = serde_json::from_value::<SystemPlacement>(legacy_json).unwrap();
        assert_eq!(loaded, legacy);
        assert!(legacy.validate(1).is_ok());

        let mut multi = legacy.clone();
        multi.status_partitions = Page::new(vec![placement(13, GroupKind::Status { bucket: 1 })]).unwrap();
        assert!(multi.validate(1).is_ok());
        assert_eq!(multi.all().count(), 5);
        assert!(!same_local_roots(&legacy, &multi, local.worker_id));
        assert!(!same_local_roots(&multi, &legacy, local.worker_id));
        assert!(same_local_roots(&multi, &multi, local.worker_id));
        let mut changed = multi.clone();
        changed.status_partitions = Page::new(vec![placement(15, GroupKind::Status { bucket: 1 })]).unwrap();
        assert!(changed.validate(1).is_ok());
        assert!(!same_local_roots(&multi, &changed, local.worker_id));
    }
}

fn install_replacement_placements(
    config: &RegisteredConfig,
    record: &ReplacementRecord,
    placements: &mut SystemPlacement,
) -> Result<(), ProtocolError> {
    let intent = &record.intent;
    intent.validate()?;
    if config.required_voters != 3
        || config.cold.is_some() || config.rolling.is_some() || config.restore.is_some()
        || !config.secondary_pools.is_empty()
        || config.enrollment != intent.next
        || config.mount.as_ref() != Some(&intent.mount)
        || config.initial_run != intent.run_id
        || intent.mount_public_key.is_none()
        || record.groups.is_empty()
        || record.groups.len() > crate::oltp_replication::authority::control::MAX_SYSTEM_REPLACEMENT_GROUPS
        || record.groups.iter().any(|plan| plan.expected.identity.validate().is_err()
            || plan.expected.identity.cluster_id != placements.control.identity.cluster_id
            || plan.expected.identity.database != placements.control.identity.database
            || plan.learner.worker_id != config.enrollment.worker_id
            || plan.learner.pool_id != config.pool_id)
    {
        return Err(invalid());
    }
    install_replacement_group(&mut placements.control, record)?;
    install_replacement_group(&mut placements.timestamps, record)?;
    install_replacement_group(&mut placements.status, record)?;
    let mut partitions = placements.status_partitions.to_vec();
    for group in &mut partitions {
        install_replacement_group(group, record)?;
    }
    placements.status_partitions = Page::new(partitions).map_err(|_| invalid())?;
    install_replacement_group(&mut placements.catalog, record)?;
    placements.validate(3)
}

fn install_replacement_group(
    group: &mut GroupPlacement,
    record: &ReplacementRecord,
) -> Result<(), ProtocolError> {
    let lost = record.intent.previous.worker_id;
    let plan = record
        .groups
        .iter()
        .find(|plan| plan.expected.identity == group.identity);
    let Some(plan) = plan else {
        return if group.voters.iter().any(|voter| voter.worker_id == lost)
            || group.identity.kind == GroupKind::Control
        {
            Err(invalid())
        } else {
            Ok(())
        };
    };
    if group.voters.as_ref() != plan.old.as_slice() {
        return Err(invalid());
    }
    group.voters = Page::new(plan.next(lost)?.to_vec()).map_err(|_| invalid())?;
    Ok(())
}
