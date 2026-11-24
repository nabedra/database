use super::super::{
    WorkerDrainIntent, WorkerDrainPhase, WorkerDrainRecord,
    data_worker::DataWorkerRegistration,
    worker_drain::{WorkerReplacementCheck, WorkerStopPermit},
};
use super::*;
use crate::{
    oltp_deployment::{
        controller::{GrowthPolicy, GrowthRecord},
        planner::Action,
    },
    oltp_replication::owned::serialized_digest,
};
use machine::worker_drain::{ACTIVE, barrier_key, record_key};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::oltp_replication::authority::control) struct PlacementPin {
    pub revision: u64,
    pub policy: Uuid,
    growth_revision: u64,
}

impl ControlHandle {
    #[doc(hidden)]
    pub async fn begin_worker_drain(
        &self,
        operator: &LocalOperatorPermit,
        id: Uuid,
        intent: WorkerDrainIntent,
    ) -> Result<WorkerDrainRecord, ProtocolError> {
        let worker = operator.local;
        let authorization =
            self.local_operator(operator, id, serialized_digest(&(worker, &intent))?)?;
        match self
            .submit(ControlCommand::BeginWorkerDrain {
                operation_id: id,
                worker,
                authorization,
                intent,
            })
            .await?
            .into_parts()?
            .1
        {
            ControlReply::WorkerDrain(record) => Ok(*record),
            ControlReply::Rejected(_) => Err(blocked(
                "worker drain requires an exact enrolled data worker and three surviving workers",
            )),
            _ => Err(unavailable("worker drain response awaits reconciliation")),
        }
    }

    #[doc(hidden)]
    pub async fn worker_drain(
        &self,
        id: Uuid,
    ) -> Result<Option<WorkerDrainRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record = load::<WorkerDrainRecord>(view, &record_key(id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    #[doc(hidden)]
    pub async fn active_worker_drain(
        &self,
    ) -> Result<Option<WorkerDrainRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record = match load::<Uuid>(view, ACTIVE)? {
                    Some(id) => {
                        Some(load::<WorkerDrainRecord>(view, &record_key(id))?.ok_or_else(stale)?)
                    }
                    None => None,
                };
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(in crate::oltp_replication::authority::control) async fn own_worker_drain(
        &self,
        worker: WorkerIdentity,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.read(move |view| {
            let record = match load::<Uuid>(view, &barrier_key(worker.worker_id))? {
                Some(id) => {
                    let record =
                        load::<WorkerDrainRecord>(view, &record_key(id))?.ok_or_else(stale)?;
                    if record.intent.worker != worker || !current_worker(view, worker)? {
                        return Err(unauthorized());
                    }
                    Some(record)
                }
                None => None,
            };
            view.encode(&record)
        })
        .await
    }

    #[doc(hidden)]
    pub async fn begin_worker_drain_removal(
        &self,
        operator: &LocalOperatorPermit,
        record: &WorkerDrainRecord,
        expected: ReadyMembership,
    ) -> Result<(), ProtocolError> {
        let pin = self.prove_worker_placements(record, expected).await?;
        let id = record.id;
        let digest = serialized_digest(&("planned-worker-removal", id, expected, pin))?;
        let operation_id = Uuid::from_slice(&digest.as_bytes()[..16]).map_err(|_| stale())?;
        let worker = operator.local;
        let evidence = serialized_digest(&(
            worker,
            (id, expected, pin.revision, pin.policy, pin.growth_revision),
        ))?;
        let authorization = self.local_operator(operator, operation_id, evidence)?;
        match self
            .submit(ControlCommand::BeginWorkerDrainRemoval {
                operation_id,
                worker,
                authorization,
                id,
                expected,
                placement_revision: pin.revision,
                policy: pin.policy,
                growth_revision: pin.growth_revision,
            })
            .await?
            .into_parts()?
            .1
        {
            ControlReply::JoinBegun { .. } => Ok(()),
            ControlReply::Rejected(_) => Err(stale()),
            _ => Err(unavailable("planned ready removal awaits reconciliation")),
        }
    }

    #[doc(hidden)]
    pub async fn worker_stop_permit(
        &self,
        operator: &LocalOperatorPermit,
        id: Uuid,
    ) -> Result<WorkerStopPermit, ProtocolError> {
        self.local_operator(operator, id, serialized_digest(&id)?)?;
        let record = self.worker_drain(id).await?.ok_or_else(stale)?;
        let WorkerDrainPhase::Closed { ready, .. } = record.phase else {
            return Err(blocked(
                "worker queue and native pool closure have not settled",
            ));
        };
        let pin = self.prove_worker_placements(&record, ready).await?;
        let expected = record.clone();
        let local = operator.local;
        let native = self
            .read(move |view| {
                let (current, current_pin, _) = drain_view(view, id, ready, &expected)?;
                let member = load::<WorkerIdentity>(
                    view,
                    &machine::ready_key(current.ready_generation, local.worker_id),
                )?;
                let target = load::<WorkerIdentity>(
                    view,
                    &machine::ready_key(current.ready_generation, expected.intent.worker.worker_id),
                )?;
                if pin != current_pin
                    || member != Some(local)
                    || target.is_some()
                    || !current_worker(view, local)?
                {
                    return Err(stale());
                }
                let native = if expected.intent.native_target.is_some() {
                    Some(native_stop_enrollment(view, &expected)?)
                } else {
                    None
                };
                view.encode(&native)
            })
            .await?;
        WorkerStopPermit::from_closed(record, machine::decode(native.as_ref())?)
    }

    #[doc(hidden)]
    pub async fn check_worker_replacement(
        &self,
        operator: &LocalOperatorPermit,
        id: Uuid,
        next: &super::super::DataWorkerIntent,
    ) -> Result<WorkerReplacementCheck, ProtocolError> {
        if self
            .policy
            .certificate_pins
            .contains(&next.worker().certificate_sha256)
        {
            return Err(unauthorized());
        }
        self.worker_stop_permit(operator, id)
            .await?
            .check_replacement(self.identity(), next)
    }

    async fn prove_worker_placements(
        &self,
        expected: &WorkerDrainRecord,
        ready: ReadyMembership,
    ) -> Result<PlacementPin, ProtocolError> {
        let id = expected.id;
        let worker = expected.intent.worker.worker_id;
        let expected_record = expected.clone();
        let raw = self
            .read(move |view| {
                let (_, pin, _) = drain_view(view, id, ready, &expected_record)?;
                view.encode(&pin)
            })
            .await?;
        let pin: PlacementPin = machine::decode(raw.as_ref())?;
        for prefix in [GROUPS, machine::pool_drain::BUILDS] {
            let mut after = None;
            loop {
                let cursor = after.map(|id| machine::key(prefix, id));
                let expected_record = expected.clone();
                let raw = self
                    .read(move |view| {
                        let (current, observed, policy) =
                            drain_view(view, id, ready, &expected_record)?;
                        if observed != pin {
                            return Err(stale());
                        }
                        let mut records = Vec::with_capacity(16);
                        let progress =
                            view.scan_page(prefix, cursor.as_deref(), 16, |key, value| {
                                let uid =
                                    Uuid::from_slice(&key[prefix.len()..]).map_err(|_| stale())?;
                                records.push((
                                    uid,
                                    if prefix == GROUPS {
                                        GroupEntry::Published(machine::decode(value)?)
                                    } else {
                                        GroupEntry::Building(machine::decode(value)?)
                                    },
                                ));
                                Ok(())
                            })?;
                        for (uid, value) in records {
                            match value {
                                GroupEntry::Published(group) => {
                                    if group.identity.group_id != uid {
                                        return Err(stale());
                                    }
                                    check_group(view, &current, &policy, worker, &group)?;
                                }
                                GroupEntry::Building(build) => {
                                    if build.generation != uid {
                                        return Err(stale());
                                    }
                                    check_build(view, worker, &build)?;
                                }
                            }
                        }
                        let after = progress
                            .last_key
                            .as_ref()
                            .map(|key| {
                                Uuid::from_slice(&key.as_ref()[prefix.len()..]).map_err(|_| stale())
                            })
                            .transpose()?;
                        view.encode(&(after, progress.exhausted))
                    })
                    .await?;
                let (last, done): (Option<Uuid>, bool) = machine::decode(raw.as_ref())?;
                if done {
                    break;
                }
                after = Some(last.ok_or_else(stale)?);
            }
        }
        Ok(pin)
    }
}

fn native_stop_enrollment(
    view: &mut GroupRead<'_>,
    record: &WorkerDrainRecord,
) -> Result<(String, Digest), ProtocolError> {
    let WorkerDrainPhase::Closed { mount, native_mounts, .. } = &record.phase else {
        return Err(stale());
    };
    let registration = load::<DataWorkerRegistration>(
        view,
        &machine::key(
            machine::data_worker::REGISTERED,
            record.intent.worker.worker_id,
        ),
    )?
    .ok_or_else(stale)?;
    let claim = load::<GrowthRecord>(view, &super::super::growth::key(record.growth_claim))?
        .ok_or_else(stale)?;
    if !record.intent.matches_registration(&registration.intent)
        || !record.matches_closure(&registration.intent.mount, mount, native_mounts.as_deref())
        || !registration.completed(&claim)
    {
        return Err(stale());
    }
    let Action::AddWorker { zone, .. } = claim.intent.action else {
        return Err(stale());
    };
    match (&record.retained_eks_pool, &registration.replacement) {
        (None, None) => {}
        (Some(retained), Some(added)) => {
            let growth = load::<GrowthRecord>(view, &super::super::growth::key(retained.claim_id))?
                .ok_or_else(stale)?;
            if !retained.matches_claim(added, &growth, &zone) {
                return Err(stale());
            }
        }
        _ => return Err(stale()),
    }
    Ok((zone, registration.intent.mount_public_key))
}

enum GroupEntry {
    Published(GroupRecord),
    Building(GroupBuild),
}

fn check_group(
    view: &mut GroupRead<'_>,
    state: &State,
    policy: &GrowthPolicy,
    target: Uuid,
    group: &GroupRecord,
) -> Result<(), ProtocolError> {
    if group.replica_count != 3 {
        return Err(blocked(
            "worker stop requires three replicas for every registered group",
        ));
    }
    if load::<ReplicaMember>(view, &machine::member_worker_key(group.generation, target))?.is_some()
    {
        return Err(blocked("worker still hosts a required replica"));
    }
    let mut members = Vec::with_capacity(3);
    let progress = view.scan_page(
        &machine::member_prefix(group.generation),
        None,
        4,
        |key, value| {
            let member: ReplicaMember = machine::decode(value)?;
            if key != machine::member_key(group.generation, member.node_id) {
                return Err(stale());
            }
            members.push(member);
            Ok(())
        },
    )?;
    if !progress.exhausted
        || members.len() != 3
        || validate_voters(&members).is_err()
        || machine::group_root(
            3,
            members
                .iter()
                .fold(machine::group_seed(), machine::group_step),
        ) != group.replica_root
    {
        return Err(stale());
    }
    let mut zones = BTreeSet::new();
    for member in members {
        if member.worker_id == target {
            return Err(blocked("worker still hosts a required replica"));
        }
        let current = load::<WorkerIdentity>(
            view,
            &machine::ready_key(state.ready_generation, member.worker_id),
        )?;
        if let Some(identity) = current {
            if !current_worker(view, identity)? {
                return Err(stale());
            }
        } else {
            return Err(blocked("a required survivor has no exact ready enrollment"));
        }
        let zone = match load::<super::super::data_worker::DataWorkerRegistration>(
            view,
            &machine::key(machine::data_worker::REGISTERED, member.worker_id),
        )? {
            Some(registration) => {
                if !matches!(group.identity.kind, GroupKind::Data { .. })
                    || Some(registration.intent.worker()) != current
                {
                    return Err(blocked(
                        "system voters must remain fixed on their enrolled workers",
                    ));
                }
                let claim = load::<GrowthRecord>(
                    view,
                    &super::super::growth::key(registration.intent.claim_id),
                )?
                .ok_or_else(stale)?;
                if !registration.completed(&claim) || registration.mounted_pool(member.pool_id).is_none() {
                    return Err(stale());
                }
                match claim.intent.action {
                    Action::AddWorker { zone, .. } => zone,
                    _ => return Err(stale()),
                }
            }
            None => policy
                .initial_replica_zones
                .get(&member.worker_id)
                .cloned()
                .ok_or_else(|| blocked("the survivor failure domain is unknown"))?,
        };
        if !policy.zones.contains(&zone) || !zones.insert(zone) {
            return Err(blocked(
                "required replicas need three distinct configured failure domains",
            ));
        }
    }
    Ok(())
}

fn check_build(
    view: &mut GroupRead<'_>,
    worker: Uuid,
    build: &GroupBuild,
) -> Result<(), ProtocolError> {
    let group = build.identity.group_id;
    let published = load::<GroupRecord>(view, &machine::key(GROUPS, group))?;
    let replacement = load::<Uuid>(
        view,
        &machine::key(machine::pool_drain::REPLACEMENTS, group),
    )?;
    let target_generation = if published
        .as_ref()
        .is_some_and(|record| record.generation == build.generation)
    {
        if replacement != Some(build.generation) {
            return Ok(());
        }
        build.expected.as_ref().ok_or_else(stale)?.generation
    } else if match build.expected.as_ref() {
        Some(old) => replacement == Some(build.generation) && published.as_ref() == Some(old),
        None => published
            .as_ref()
            .is_none_or(|record| record.replica_count == 0),
    } {
        build.generation
    } else {
        return Ok(());
    };
    if load::<ReplicaMember>(view, &machine::member_worker_key(target_generation, worker))?
        .is_some()
    {
        return Err(blocked("worker has a staged replica or pending retirement"));
    }
    Ok(())
}

fn current_worker(view: &mut GroupRead<'_>, worker: WorkerIdentity) -> Result<bool, ProtocolError> {
    Ok(
        load::<WorkerRecord>(view, &machine::key(WORKERS, worker.worker_id))?
            .is_some_and(|entry| WorkerIdentity::from(&entry.enrollment) == worker),
    )
}

fn drain_view(
    view: &mut GroupRead<'_>,
    id: Uuid,
    ready: ReadyMembership,
    expected: &WorkerDrainRecord,
) -> Result<(State, PlacementPin, GrowthPolicy), ProtocolError> {
    let current = state(view)?;
    let target = expected.intent.worker;
    if ready != current.ready
        || current.invalid_ready != 0
        || current.ready.ready_count < 3
        || current.pending.is_some()
        || current.restart.is_some()
        || load::<Uuid>(view, machine::placement::ACTIVE)?.is_some()
        || view.get(machine::data_worker::PREPARED)?.is_some()
        || load::<Uuid>(view, &barrier_key(target.worker_id))? != Some(id)
        || load::<WorkerDrainRecord>(view, &record_key(id))?.as_ref() != Some(expected)
        || !current_worker(view, target)?
    {
        return Err(stale());
    }
    let active_growth = load::<Uuid>(view, super::super::growth::ACTIVE)?;
    if let Some(id) = active_growth {
        let growth =
            load::<GrowthRecord>(view, &super::super::growth::key(id))?.ok_or_else(stale)?;
        if growth.intent.worker == target {
            let original = load::<GrowthRecord>(view, &super::super::growth::key(expected.growth_claim))?
                .ok_or_else(stale)?;
            if !super::super::worker_drain::eligible_add_pool(&expected.intent, &original, &growth) {
                return Err(blocked("worker has pending storage provisioning"));
            }
        }
    }
    if expected.intent.replacement_growth_claim.is_some_and(|id| Some(id) != active_growth) {
        return Err(stale());
    }
    let policy = load::<GrowthPolicy>(view, super::super::growth::POLICY)?
        .ok_or_else(|| blocked("worker stop requires a persisted failure-domain policy"))?;
    let pin = PlacementPin {
        revision: load::<u64>(view, machine::pool_drain::REVISION)?.unwrap_or_default(),
        growth_revision: load::<u64>(view, super::super::growth::REVISION)?.unwrap_or_default(),
        policy: policy.policy_id,
    };
    Ok((current, pin, policy))
}

fn stale() -> ProtocolError {
    blocked("the exact worker drain, inventory or ready revision changed")
}
fn blocked(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}
