use super::super::pool_drain::{
    DrainMount, PoolDrainCommand, PoolDrainPhase, PoolDrainRecord, RetainedPoolDetachPermit,
};
use super::*;
use crate::oltp::pool::VerifiedPoolClosed;
use crate::oltp_replication::owned::serialized_digest;
use machine::pool_drain::{BUILDS, REPLACEMENTS, REVISION, barrier_key, record_key, retired_key};

#[derive(Serialize, Deserialize)]
struct PlacementPage {
    items: Vec<(Uuid, u64)>,
    after: Option<Uuid>,
    done: bool,
}

#[derive(Serialize, Deserialize)]
struct MemberPage {
    blocked: bool,
    after: Option<u64>,
    done: bool,
    visited: u64,
}

impl ControlHandle {
    #[doc(hidden)]
    pub async fn pool_drain(
        &self,
        id: Uuid,
    ) -> Result<Option<PoolDrainRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record = load::<PoolDrainRecord>(view, &record_key(id))?;
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn pool_drain_blocks_placement(
        &self,
        pool: Uuid,
    ) -> Result<bool, ProtocolError> {
        let raw = self
            .read(move |view| {
                let blocked = load::<Uuid>(view, &barrier_key(pool))?.is_some();
                view.encode(&blocked)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn pool_drain_allows_retained_open(
        &self,
        pool: Uuid,
    ) -> Result<bool, ProtocolError> {
        let raw = self
            .read(move |view| {
                let id: Option<Uuid> = load(view, &barrier_key(pool))?;
                let allowed = match id {
                    Some(id) => load::<PoolDrainRecord>(view, &record_key(id))?
                        .is_some_and(|record| record.phase == PoolDrainPhase::Draining),
                    None => true,
                };
                view.encode(&allowed)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn begin_pool_drain(
        &self,
        permit: &LocalOperatorPermit,
        id: Uuid,
        growth_claim: Uuid,
        mount: DrainMount,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        self.pool_drain_submit(
            permit,
            id,
            PoolDrainCommand::Begin {
                growth_claim,
                mount: Box::new(mount),
            },
        )
        .await
    }

    /// Every page is read against the same durable placement and transition
    /// revision. The final Raft command checks that revision again.
    pub(crate) async fn prove_empty_pool_placements(
        &self,
        record: &PoolDrainRecord,
    ) -> Result<u64, ProtocolError> {
        let id = record.id;
        let pool = record.mount.expected.identity.pool_id;
        let raw = self
            .read(move |view| {
                let revision = drain_revision(view, id, pool)?;
                if let Some(move_id) = load::<Uuid>(view, machine::placement::ACTIVE)? {
                    let movement = load::<super::super::ReplicaMove>(
                        view,
                        &machine::placement::move_key(move_id),
                    )?
                    .ok_or_else(stale)?;
                    if movement
                        .old
                        .iter()
                        .chain(&movement.next)
                        .any(|member| member.pool_id == pool)
                    {
                        return Err(blocked("pool has a pending replica move or retirement"));
                    }
                }
                view.encode(&revision)
            })
            .await?;
        let revision = machine::decode(raw.as_ref())?;
        for prefix in [GROUPS, BUILDS] {
            let mut after = None;
            loop {
                let cursor = after.map(|id| machine::key(prefix, id));
                let raw = self
                    .read(move |view| {
                        check_revision(view, id, pool, revision)?;
                        let mut groups = Vec::with_capacity(32);
                        let progress =
                            view.scan_page(prefix, cursor.as_deref(), 32, |key, raw| {
                                let identifier =
                                    Uuid::from_slice(&key[prefix.len()..]).map_err(|_| stale())?;
                                groups.push(if prefix == GROUPS {
                                    let group: GroupRecord = machine::decode(raw)?;
                                    if group.identity.group_id != identifier {
                                        return Err(stale());
                                    }
                                    (identifier, group.generation, group.replica_count, None)
                                } else {
                                    let build: GroupBuild = machine::decode(raw)?;
                                    if build.generation != identifier {
                                        return Err(stale());
                                    }
                                    (
                                        build.identity.group_id,
                                        identifier,
                                        build.count,
                                        Some(build),
                                    )
                                });
                                Ok(())
                            })?;
                        let mut items = Vec::with_capacity(32);
                        for (group, generation, count, build) in groups {
                            let Some(build) = build else {
                                items.push((generation, count));
                                continue;
                            };
                            let published: Option<GroupRecord> =
                                load(view, &machine::key(GROUPS, group))?;
                            let replacement =
                                load::<Uuid>(view, &machine::key(REPLACEMENTS, group))?;
                            if published
                                .as_ref()
                                .is_some_and(|value| value.generation == generation)
                            {
                                if replacement == Some(generation) {
                                    let old = build.expected.ok_or_else(stale)?;
                                    items.push((old.generation, old.replica_count));
                                }
                            } else if match build.expected.as_ref() {
                                Some(old) => {
                                    replacement == Some(generation)
                                        && published.as_ref() == Some(old)
                                }
                                None => published.is_none_or(|value| value.replica_count == 0),
                            } {
                                items.push((generation, count));
                            }
                        }
                        let after = progress
                            .last_key
                            .as_ref()
                            .map(|bytes| {
                                Uuid::from_slice(&bytes.as_ref()[prefix.len()..]).map_err(|_| stale())
                            })
                            .transpose()?;
                        view.encode(&PlacementPage {
                            items,
                            after,
                            done: progress.exhausted,
                        })
                    })
                    .await?;
                let page: PlacementPage = machine::decode(raw.as_ref())?;
                for (generation, count) in page.items {
                    self.check_drain_members(id, pool, revision, generation, count)
                        .await?;
                }
                if page.done {
                    break;
                }
                after = Some(page.after.ok_or_else(stale)?);
            }
        }
        Ok(revision)
    }

    async fn check_drain_members(
        &self,
        id: Uuid,
        pool: Uuid,
        revision: u64,
        generation: Uuid,
        count: u64,
    ) -> Result<(), ProtocolError> {
        let prefix = machine::member_prefix(generation);
        let mut after = None;
        let mut visited = 0u64;
        loop {
            let prefix = prefix.clone();
            let raw = self
                .read(move |view| {
                    check_revision(view, id, pool, revision)?;
                    let mut blocked = false;
                    let cursor = after.map(|node| machine::member_key(generation, node));
                    let mut last = after;
                    let progress =
                        view.scan_page_until(&prefix, cursor.as_deref(), 128, |key, raw| {
                            let member: ReplicaMember = machine::decode(raw)?;
                            if key != machine::member_key(generation, member.node_id) {
                                return Err(stale());
                            }
                            blocked = member.pool_id == pool;
                            last = Some(member.node_id);
                            Ok(blocked)
                        })?;
                    view.encode(&MemberPage {
                        blocked,
                        after: last,
                        done: progress.exhausted,
                        visited: progress.visited as u64,
                    })
                })
                .await?;
            let page: MemberPage = machine::decode(raw.as_ref())?;
            if page.blocked {
                return Err(blocked("pool has a required or staged replica"));
            }
            visited = visited.checked_add(page.visited).ok_or_else(stale)?;
            if visited > count || page.done && visited != count {
                return Err(stale());
            }
            if page.done {
                return Ok(());
            }
            after = Some(page.after.ok_or_else(stale)?);
        }
    }

    pub(crate) async fn verify_pool_replica_retired(
        &self,
        proof: ReplicaRetiredStamp,
    ) -> Result<bool, ProtocolError> {
        use super::super::{ReplicaMove, ReplicaMovePhase};
        let raw = self.read(move |view| {
            let confirmed = match load::<ReplicaMove>(view, &machine::placement::move_key(proof.generation))? {
                Some(plan) => {
                    plan.request.group == proof.group
                        && plan.source_worker() == proof.worker
                        && plan.removed() == Some(&proof.replica)
                        && matches!(plan.phase, ReplicaMovePhase::Retired { retired_at, .. } if retired_at == proof.applied_at)
                }
                None => load::<ReplicaRetiredStamp>(view, &retired_key(proof.generation, proof.replica.node_id))?
                    .as_ref() == Some(&proof),
            };
            view.encode(&confirmed)
        }).await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn prepare_pool_close(
        &self,
        permit: &LocalOperatorPermit,
        id: Uuid,
        revision: u64,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        self.pool_drain_submit(
            permit,
            nabedra_rt::ids::uuid_v4(),
            PoolDrainCommand::Prepare { id, revision },
        )
        .await
    }

    pub(crate) async fn record_pool_closed(
        &self,
        permit: &LocalOperatorPermit,
        verified: VerifiedPoolClosed,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        let closed = verified.statement();
        let observed = closed.registered.as_ref().ok_or_else(unauthorized)?;
        if observed.expected.worker != permit.local || observed.expected.identity != closed.identity
        {
            return Err(unauthorized());
        }
        self.pool_drain_submit(
            permit,
            nabedra_rt::ids::uuid_v4(),
            PoolDrainCommand::Closed {
                id: closed.close_id,
                mount: Box::new(observed.clone().into()),
            },
        )
        .await
    }

    #[doc(hidden)]
    pub async fn cancel_pool_drain(
        &self,
        permit: &LocalOperatorPermit,
        operation: Uuid,
        id: Uuid,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        self.pool_drain_submit(permit, operation, PoolDrainCommand::Cancel { id })
            .await
    }

    #[doc(hidden)]
    pub async fn pool_detach_permit(
        &self,
        operator: &LocalOperatorPermit,
        id: Uuid,
    ) -> Result<RetainedPoolDetachPermit, ProtocolError> {
        self.local_operator(operator, id, serialized_digest(&id)?)?;
        let worker = operator.local;
        let raw = self
            .read(move |view| {
                let current = state(view)?;
                let ready: Option<WorkerIdentity> = load(
                    view,
                    &machine::ready_key(current.ready_generation, worker.worker_id),
                )?;
                let enrolled: Option<WorkerRecord> =
                    load(view, &machine::key(WORKERS, worker.worker_id))?;
                if current.pending.is_some()
                    || current.restart.is_some()
                    || ready != Some(worker)
                    || enrolled
                        .is_none_or(|entry| WorkerIdentity::from(&entry.enrollment) != worker)
                {
                    return Err(unauthorized());
                }
                let record = load::<PoolDrainRecord>(view, &record_key(id))?.ok_or_else(stale)?;
                if load::<Uuid>(view, &barrier_key(record.mount.expected.identity.pool_id))?
                    != Some(id)
                {
                    return Err(unauthorized());
                }
                view.encode(&record)
            })
            .await?;
        RetainedPoolDetachPermit::from_closed(machine::decode(raw.as_ref())?, operator.local)
    }

    async fn pool_drain_submit(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        command: PoolDrainCommand,
    ) -> Result<PoolDrainRecord, ProtocolError> {
        let worker = permit.local;
        let authorization = self.local_operator(
            permit,
            operation_id,
            serialized_digest(&(worker, &command))?,
        )?;
        match self
            .submit(ControlCommand::PoolDrain {
                operation_id,
                worker,
                authorization,
                command,
            })
            .await?
            .into_parts()?
            .1
        {
            ControlReply::PoolDrain(record) => Ok(*record),
            ControlReply::Rejected(Fault::Unauthorized) => Err(unauthorized()),
            ControlReply::Rejected(Fault::NotFound) => Err(ProtocolError::new(
                ErrorCode::NotFound,
                "pool drain is unknown",
                Outcome::NotApplied,
            )),
            ControlReply::Rejected(_) => Err(blocked("pool drain authority or phase changed")),
            _ => Err(unavailable("pool drain response awaits reconciliation")),
        }
    }
}

fn drain_revision(view: &mut GroupRead<'_>, id: Uuid, pool: Uuid) -> Result<u64, ProtocolError> {
    let current = state(view)?;
    if current.pending.is_some()
        || current.restart.is_some()
        || load::<Uuid>(view, &barrier_key(pool))? != Some(id)
        || load::<PoolDrainRecord>(view, &record_key(id))?
            .is_none_or(|record| record.phase != PoolDrainPhase::Draining)
    {
        return Err(blocked(
            "pool drain is fenced by a pending Control transition",
        ));
    }
    Ok(load::<u64>(view, REVISION)?.unwrap_or_default())
}

fn check_revision(
    view: &mut GroupRead<'_>,
    id: Uuid,
    pool: Uuid,
    expected: u64,
) -> Result<(), ProtocolError> {
    if drain_revision(view, id, pool)? != expected {
        return Err(blocked("pool placement inventory revision changed"));
    }
    Ok(())
}

fn blocked(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}
