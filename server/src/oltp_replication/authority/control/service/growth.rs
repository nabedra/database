use super::super::{
    data_worker::{DataWorkerGrowthPool, DataWorkerGrowthProof, DataWorkerRegistration},
    growth as durable,
};
use super::*;
use crate::oltp::pool::{ExecutionClass, OltpPool, mount::MountedPoolObservation};
use crate::oltp_deployment::controller::*;
use crate::oltp_deployment::live::{GrowthInventory, InventoryPin, WorkerPage};
use crate::oltp_deployment::planner::Action;

impl ControlHandle {
    pub(crate) async fn configure_growth_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        policy: GrowthPolicy,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.growth_submit_local(permit, operation_id, GrowthCommand::Configure { policy })
            .await
    }
    pub(crate) async fn acknowledge_replica_retired_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        retired: &crate::oltp_replication::manager::ReplicaRetired,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let proof = retired.stamp().clone();
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&proof)?,
        )?;
        self.submit(ControlCommand::ReplicaRetired {
            operation_id,
            authorization,
            proof,
            remote: None,
        })
        .await
    }
    pub(crate) async fn growth(
        &self,
        claim_id: Uuid,
    ) -> Result<Option<GrowthRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record: Option<GrowthRecord> =
                    load(view, &machine::key(b"ct/growth/", claim_id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn growth_inventory(&self) -> Result<GrowthInventory, ProtocolError> {
        let cluster_id = self.identity().cluster_id;
        let raw = self
            .read(move |view| {
                let (policy, pin, _) = growth_pin(view)?;
                let active_id: Option<Uuid> = load(view, durable::ACTIVE)?;
                let active_automatic: Option<Uuid> = load(view, durable::ACTIVE_AUTOMATIC)?;
                let active = active_id
                    .map(|id| load::<GrowthRecord>(view, &durable::key(id))?.ok_or_else(stale))
                    .transpose()?;
                let last_attempt = load(view, durable::LAST_ATTEMPT)?;
                view.encode(&GrowthInventory {
                    cluster_id,
                    pin,
                    policy,
                    last_attempt,
                    active,
                    active_automatic: active_automatic.is_some() && active_automatic == active_id,
                })
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(crate) async fn growth_workers(
        &self,
        expected: InventoryPin,
        after: Option<Uuid>,
    ) -> Result<WorkerPage, ProtocolError> {
        let raw = self
            .read(move |view| {
                let (_, pin, state) = growth_pin(view)?;
                if pin != expected {
                    return Err(stale());
                }
                let prefix = machine::ready_prefix(state.ready_generation);
                let cursor = after.map(|id| machine::ready_key(state.ready_generation, id));
                let mut cursor_out = after;
                let mut workers = Vec::new();
                // Read identifiers first so the scan has no nested read on its view.
                let mut members = Vec::new();
                let progress = view.scan_page(&prefix, cursor.as_deref(), 32, |_, raw| {
                    members.push(machine::decode::<WorkerIdentity>(raw)?);
                    Ok(())
                })?;
                for member in members {
                    let record: WorkerRecord =
                        load(view, &machine::key(WORKERS, member.worker_id))?.ok_or_else(stale)?;
                    if WorkerIdentity::from(&record.enrollment) != member {
                        return Err(stale());
                    }
                    cursor_out = Some(member.worker_id);
                    workers.push(record.enrollment);
                }
                view.encode(&WorkerPage {
                    pin,
                    workers,
                    after: cursor_out,
                    done: progress.exhausted,
                })
            })
            .await?;
        machine::decode(raw.as_ref())
    }
    pub(in crate::oltp_replication::authority::control) async fn data_worker_growth_proof(
        &self,
        worker: WorkerIdentity,
        claim_id: Uuid,
        expected: InventoryPin,
    ) -> Result<ChargedBytes, ProtocolError> {
        let scope = self.identity().clone();
        self.read(move |view| {
            let (_, pin, state) = growth_pin(view)?;
            if pin != expected {
                return Err(stale());
            }
            let member: Option<WorkerIdentity> = load(
                view,
                &machine::ready_key(state.ready_generation, worker.worker_id),
            )?;
            if member != Some(worker)
                || view
                    .get(&machine::worker_drain::barrier_key(worker.worker_id))?
                    .is_some()
            {
                return Err(stale());
            }
            let enrolled: WorkerRecord =
                load(view, &machine::key(WORKERS, worker.worker_id))?.ok_or_else(unauthorized)?;
            let registered: DataWorkerRegistration = load(
                view,
                &machine::key(machine::data_worker::REGISTERED, worker.worker_id),
            )?
            .ok_or_else(unauthorized)?;
            if registered.intent.worker() != worker
                || registered.intent.enrollment != enrolled.enrollment
                || registered.intent.claim_id != claim_id
                || registered.intent.run_id != pin.ready.view.run_id
            {
                return Err(unauthorized());
            }
            registered.intent.validate(&scope)?;
            let claim: GrowthRecord = load(view, &durable::key(claim_id))?.ok_or_else(stale)?;
            if !registered.completed(&claim)
                || view
                    .get(&machine::pool_drain::barrier_key(registered.mount.pool_id))?
                    .is_some()
            {
                return Err(stale());
            }
            let added = if let Some(mount) = registered.replacement {
                if mount.worker != worker
                    || mount.pool_id == registered.mount.pool_id
                    || view
                        .get(&machine::pool_drain::barrier_key(mount.pool_id))?
                        .is_some()
                {
                    return Err(stale());
                }
                let id = load::<Uuid>(view, &durable::pool_claim_key(mount.pool_id))?
                    .ok_or_else(stale)?;
                let added: GrowthRecord = load(view, &durable::key(id))?.ok_or_else(stale)?;
                if added.phase != GrowthPhase::Complete
                    || !matches!(added.intent.action, Action::AddPool { .. })
                    || added.intent.claim_id != id
                    || added.intent.worker.worker_id != worker.worker_id
                    || added.intent.worker.enrollment_revision >= worker.enrollment_revision
                    || added.intent.pool_id != mount.pool_id
                    || added.intent.pool_generation != mount.pool_generation
                    || added.provider_resource.as_deref() != Some(mount.provider_volume_id.as_str())
                    || added.mounted_total_bytes != Some(mount.total_bytes)
                {
                    return Err(stale());
                }
                Some(DataWorkerGrowthPool {
                    claim_id: id,
                    mount,
                })
            } else {
                None
            };
            view.encode(&DataWorkerGrowthProof {
                pin,
                authority: QueueAuthority {
                    ready: pin.ready,
                    owner: state.owner.ok_or_else(stale)?,
                },
                primary: DataWorkerGrowthPool {
                    claim_id,
                    mount: registered.mount,
                },
                added,
            })
        })
        .await
    }
    async fn growth_submit_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        command: GrowthCommand,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&command)?,
        )?;
        self.submit(ControlCommand::Growth {
            operation_id,
            authorization,
            command,
        })
        .await
    }
    pub(crate) async fn claim_growth_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        intent: GrowthIntent,
        pool: Option<&OltpPool>,
        pin: Option<InventoryPin>,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let prior_total_bytes = if let Some(old) = self.growth(intent.claim_id).await? {
            old.prior_total_bytes
        } else if matches!(
            intent.action,
            crate::oltp_deployment::planner::Action::Expand { .. }
        ) {
            let observed = observe_mount(pool.ok_or_else(unauthorized)?).await?;
            if observed.expected.identity.pool_id != intent.pool_id
                || observed.expected.worker != intent.worker
                || observed.expected.generation != intent.pool_generation
            {
                return Err(unauthorized());
            }
            Some(observed.total_bytes)
        } else {
            None
        };
        let command = match pin {
            Some(pin) => GrowthCommand::ClaimAutomatic {
                intent,
                prior_total_bytes,
                pin,
            },
            None => GrowthCommand::Claim {
                intent,
                prior_total_bytes,
            },
        };
        self.growth_submit_local(permit, operation_id, command)
            .await
    }
    pub(crate) async fn attempt_growth_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        claim_id: Uuid,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        // Derive new attempt time here. An exact old operation is reconstructed
        // from its authoritative receipt before applying freshness/cooldown.
        // The full command digest below prevents reusing another operation.
        let raw = self
            .read(move |view| {
                let old: Option<machine::Receipt> =
                    load(view, &machine::key(b"ct/op/", operation_id))?;
                view.encode(&old)
            })
            .await?;
        let old: Option<machine::Receipt> = machine::decode(raw.as_ref())?;
        let now = match old.as_ref().map(|receipt| &receipt.reply) {
            Some(ControlReply::Growth(record))
                if record.intent.claim_id == claim_id
                    && record.phase == GrowthPhase::Provisioning =>
            {
                record.last_attempt.ok_or_else(unauthorized)?
            }
            _ => nabedra_rt::time::wall_now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| unauthorized())?
                .as_secs(),
        };
        let command = GrowthCommand::Attempt { claim_id, now };
        let semantic =
            crate::oltp_replication::owned::serialized_digest(&(operation_id, &command))?;
        let authorization = self.local_operator(
            permit,
            operation_id,
            crate::oltp_replication::owned::serialized_digest(&command)?,
        )?;
        let command = ControlCommand::Growth {
            operation_id,
            authorization,
            command,
        };
        if old.is_some_and(|old| {
            old.growth_digest != Some(semantic)
                && crate::oltp_replication::owned::serialized_digest(&command).ok()
                    != Some(old.digest)
        }) {
            return Err(unauthorized());
        }
        self.submit(command).await
    }
    pub(crate) async fn provisioned_growth_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        claim_id: Uuid,
        resource_id: String,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        self.growth_submit_local(
            permit,
            operation_id,
            GrowthCommand::Provisioned {
                claim_id,
                resource_id,
            },
        )
        .await
    }
    /// Completion consumes observations from the currently open registered
    /// store. Provider results and caller-supplied capacity have no such method.
    pub(crate) async fn mounted_growth_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        claim_id: Uuid,
        pool: &OltpPool,
    ) -> Result<Applied<ControlReply>, ProtocolError> {
        let observation = observe_mount(pool).await?;
        let expected = &observation.expected;
        if expected.identity.cluster_id != self.identity().cluster_id
            || expected.worker != permit.local
            || !self.growth_worker_ready(expected.worker).await?
        {
            return Err(unauthorized());
        }
        let record = self.growth(claim_id).await?.ok_or_else(unauthorized)?;
        if record.intent.worker != expected.worker
            || record.intent.pool_id != expected.identity.pool_id
            || record.intent.pool_generation != expected.generation
            || record.provider_resource.as_deref() != Some(expected.provider_volume_id.as_str())
            || observation.total_bytes < record.intent.requested_bytes()
            || record
                .mounted_total_bytes
                .is_some_and(|previous| observation.total_bytes < previous)
        {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "the registered filesystem has not reached the claimed capacity",
                Outcome::NotApplied,
            ));
        }
        let proof = MountedGrowthStamp {
            worker: expected.worker,
            pool_id: expected.identity.pool_id,
            pool_generation: expected.generation,
            provider_volume_id: expected.provider_volume_id.clone(),
            // Preserve the exact receipt digest if this filesystem later grows.
            total_bytes: record
                .mounted_total_bytes
                .unwrap_or(observation.total_bytes),
        };
        self.growth_submit_local(
            permit,
            operation_id,
            GrowthCommand::Mounted { claim_id, proof },
        )
        .await
    }

    pub(crate) async fn growth_worker_ready(
        &self,
        worker: WorkerIdentity,
    ) -> Result<bool, ProtocolError> {
        // New worker capacity is only advertised after Control has published
        // its real all-ready queue transition. A running process is insufficient.
        let raw = self
            .read(move |view| {
                let state: State = load(view, STATE)?.ok_or_else(unauthorized)?;
                let member: Option<WorkerIdentity> = load(
                    view,
                    &machine::ready_key(state.ready_generation, worker.worker_id),
                )?;
                let enrolled: Option<WorkerRecord> =
                    load(view, &machine::key(WORKERS, worker.worker_id))?;
                view.encode(
                    &(state.pending.is_none()
                        && member == Some(worker)
                        && enrolled.is_some_and(|entry| {
                            WorkerIdentity::from(&entry.enrollment) == worker
                        })),
                )
            })
            .await?;
        machine::decode(raw.as_ref())
    }
}

fn growth_pin(
    view: &mut GroupRead<'_>,
) -> Result<(GrowthPolicy, InventoryPin, State), ProtocolError> {
    let state = state(view)?;
    let policy: GrowthPolicy = load(view, durable::POLICY)?.ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::UnsupportedOperation,
            "automatic growth requires configured policy",
            Outcome::NotApplied,
        )
    })?;
    let spend = load(view, durable::SPEND)?.ok_or_else(stale)?;
    let revision = load(view, durable::REVISION)?.unwrap_or(0);
    let pin = durable::pin(&state, policy.policy_id, revision, spend).ok_or_else(stale)?;
    Ok((policy, pin, state))
}

async fn observe_mount(pool: &OltpPool) -> Result<MountedPoolObservation, ProtocolError> {
    let shared = pool.resources().clone();
    let pool = pool.clone();
    shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            pool.observe_registered_mount()
        })
        .await
}
