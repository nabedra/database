use super::*;
#[cfg(test)]
use crate::oltp_replication::authority::control::machine::scoped_queue::creation_digest;
use crate::oltp_replication::authority::control::{
    ScopedQueueCapability, ScopedQueuePlacement, ScopedQueueTransition,
    machine::scoped_queue::{
        capability::Admission, completed_key, placement_key, replacement_key, transfer_key,
    },
    scoped_queue::{ScopedQueueOperation, permits_new_run, stale as stale_placement},
};

impl ControlHandle {
    pub(crate) async fn scoped_queue_capability(
        &self,
    ) -> Result<ScopedQueueCapability, ProtocolError> {
        let raw = self
            .read(|view| {
                let state = state(view)?;
                let admission = load(view, Admission::key())?;
                view.encode(&Admission::capability(admission, &state))
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(in crate::oltp_replication::authority::control) async fn read_scoped_queue_transition(
        &self,
        worker: WorkerIdentity,
        group_id: Uuid,
        source_group: Option<Uuid>,
    ) -> Result<ChargedBytes, ProtocolError> {
        if group_id.is_nil() {
            return Err(unauthorized());
        }
        let operator = self
            .policy
            .certificate_pins
            .contains(&worker.certificate_sha256);
        self.read(move |view| {
            let state = state(view)?;
            let enrolled = load::<WorkerRecord>(view, &machine::key(WORKERS, worker.worker_id))?
                .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == worker);
            if !enrolled {
                return Err(unauthorized());
            }
            let Some(transition) = load::<ScopedQueueTransition>(view, &transfer_key(group_id))?
                .filter(|transition| transition.previous.view.run_id == state.ready.view.run_id)
            else {
                return view.encode(&Option::<ScopedQueueTransition>::None);
            };
            let holder = (transition.previous.has_holder(worker)
                || transition.next.has_holder(worker))
                && load::<WorkerIdentity>(
                    view,
                    &machine::ready_key(state.ready_generation, worker.worker_id),
                )? == Some(worker);
            if !operator && !holder
                && !scoped_transfer_replica(view, source_group, &transition.next.group, worker)?
            {
                return Err(unauthorized());
            }
            if matches!(state.pending, Some(Pending::Fence(_))) {
                return Err(stale_placement());
            }
            view.encode(&Some(transition))
        })
        .await
    }

    pub(in crate::oltp_replication::authority::control) async fn read_scoped_queue_epoch_retired(
        &self,
        worker: WorkerIdentity,
        group_id: Uuid,
        queue_generation: Uuid,
        old_view: AuthorityView,
    ) -> Result<ChargedBytes, ProtocolError> {
        if group_id.is_nil() || queue_generation.is_nil() || old_view.validate().is_err() {
            return Err(unauthorized());
        }
        let scope = self.identity().clone();
        self.read(move |view| {
            if !load::<WorkerRecord>(view, &machine::key(WORKERS, worker.worker_id))?
                .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == worker)
            {
                return Err(unauthorized());
            }
            let state = state(view)?;
            let retired = if old_view.run_id == state.ready.view.run_id {
                load::<ScopedQueuePlacement>(view, &placement_key(group_id))?.is_some_and(
                    |placement| {
                        placement.group.group_id == group_id
                            && placement.group.cluster_id == scope.cluster_id
                            && placement.group.database == scope.database
                            && placement.validate(&placement.group).is_ok()
                            && placement.queue_generation == queue_generation
                            && placement.view.run_id == old_view.run_id
                            && placement.view.owner_term > old_view.owner_term
                            && placement.view.membership_revision >= old_view.membership_revision
                    },
                )
            } else {
                published_new_run(view, &state, old_view.run_id)?
            };
            view.encode(&retired)
        })
        .await
    }

    pub(in crate::oltp_replication::authority::control) async fn scoped_queue_publication_replayed(
        &self,
        worker: WorkerIdentity,
        group_id: Uuid,
        transfer_id: Uuid,
    ) -> Result<bool, ProtocolError> {
        let scope = self.identity().clone();
        let raw = self
            .read(move |view| {
                let state = state(view)?;
                let completed = load::<(Uuid, AuthorityView)>(view, &completed_key(group_id))?;
                let replayed = match completed {
                    Some((id, epoch)) if id == transfer_id && epoch.run_id == state.ready.view.run_id => {
                        current_placement(view, &scope, &state, group_id)?.is_some_and(|queue| {
                            queue.view == epoch && queue.owner == worker
                        })
                    }
                    _ => false,
                };
                view.encode(&replayed)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(in crate::oltp_replication::authority::control) async fn read_scoped_queue_predecessor(
        &self,
        worker: WorkerIdentity,
        group_id: Uuid,
    ) -> Result<ChargedBytes, ProtocolError> {
        if group_id.is_nil()
            || !self
                .policy
                .certificate_pins
                .contains(&worker.certificate_sha256)
        {
            return Err(unauthorized());
        }
        self.read(move |view| {
            let state = state(view)?;
            let enrolled = load::<WorkerRecord>(view, &machine::key(WORKERS, worker.worker_id))?
                .is_some_and(|record| WorkerIdentity::from(&record.enrollment) == worker);
            if !enrolled {
                return Err(unauthorized());
            }
            let placement = load::<ScopedQueuePlacement>(view, &placement_key(group_id))?
                .filter(|queue| queue.view.run_id == state.ready.view.run_id);
            view.encode(&placement)
        })
        .await
    }

    pub(in crate::oltp_replication::authority::control) async fn apply_scoped_queue_rpc(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        action: ScopedQueueOperation,
        leader: Option<&dyn Consensus<ControlCommand, ControlReply>>,
    ) -> Result<ControlReply, ProtocolError> {
        use crate::oltp_replication::owned::serialized_digest;
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.identity()
            || operation_id.is_nil()
        {
            return Err(unauthorized());
        }
        let actor = session.remote();
        let authorization = if action.requires_operator() {
            let evidence = serialized_digest(&(self.identity(), actor, &action))?;
            Some(self.operator(session, evidence, operation_id)?)
        } else {
            None
        };
        let command = ControlCommand::ScopedQueue {
            operation_id,
            actor,
            action: Box::new(action),
            authorization,
        };
        // The accepting Control leader binds the transport identity to the
        // command. Generic delegated proposals cannot carry scoped commands.
        let applied = if let Some(leader) = leader {
            let owned = OwnedCommand::new_owned(command, self.resources(), ByteClass::Progress)?;
            leader.propose(owned).await?
        } else {
            self.submit(command).await?
        };
        let reply = applied.reply;
        if let ControlReply::ScopedQueue(placement) = &reply {
            if self.scoped_queue(&placement.group).await?.as_ref() != Some(placement.as_ref()) {
                return Err(stale_placement());
            }
        }
        Ok(reply)
    }

    /// Control administration can initialize a placement on a published Data
    /// group. Membership moves in the same run require an explicit handoff.
    #[cfg(test)]
    pub(crate) async fn ensure_scoped_queue_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        group: &GroupIdentity,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let expected = self
            .group(group.group_id)
            .await?
            .filter(|record| record.identity == *group)
            .ok_or_else(stale_placement)?;
        let run_id = self
            .inspect()
            .await?
            .ok_or_else(stale_placement)?
            .ready
            .view
            .run_id;
        let authorization =
            self.local_operator(permit, operation_id, creation_digest(&expected, run_id)?)?;
        let reply = self
            .submit(ControlCommand::EnsureScopedQueue {
                operation_id,
                expected: Box::new(expected),
                run_id,
                authorization,
            })
            .await?;
        let ControlReply::ScopedQueue(placement) = reply.reply else {
            return Err(stale_placement());
        };
        // An operation receipt can replay across a fence. Only the current
        // linearized record grants execution and copy authority.
        self.scoped_queue(group)
            .await?
            .filter(|current| current == placement.as_ref())
            .ok_or_else(stale_placement)
    }

    pub(crate) async fn scoped_queue(
        &self,
        group: &GroupIdentity,
    ) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
        if group.cluster_id != self.identity().cluster_id
            || group.database != self.identity().database
            || group.validate().is_err()
            || !matches!(group.kind, GroupKind::Data { .. })
        {
            return Err(unauthorized());
        }
        let raw = self.read_scoped_queue(group.group_id).await?;
        let placement = machine::decode::<Option<ScopedQueuePlacement>>(raw.as_ref())?;
        if let Some(placement) = &placement {
            placement.validate(group)?;
        }
        Ok(placement)
    }

    pub(in crate::oltp_replication::authority::control) async fn read_scoped_queue(
        &self,
        group_id: Uuid,
    ) -> Result<ChargedBytes, ProtocolError> {
        if group_id.is_nil() {
            return Err(unauthorized());
        }
        let scope = self.identity().clone();
        self.read(move |view| {
            let current = state(view)?;
            let placement = current_placement(view, &scope, &current, group_id)?;
            view.encode(&placement)
        })
        .await
    }
}

fn scoped_transfer_replica(
    view: &mut GroupRead<'_>,
    source_group: Option<Uuid>,
    data: &GroupIdentity,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    let Some(source_group) = source_group else {
        return Ok(false);
    };
    let Some(record) = load::<GroupRecord>(view, &machine::key(GROUPS, source_group))? else {
        return Ok(false);
    };
    Ok(record.identity.group_id == source_group
        && record.identity.cluster_id == data.cluster_id
        && record.identity.database == data.database
        && (matches!(
            record.identity.kind,
            GroupKind::Catalog | GroupKind::Timestamps | GroupKind::Status { .. }
        ) || record.identity == *data)
        && record.replica_count > 0
        && load::<ReplicaMember>(
            view,
            &machine::member_worker_key(record.generation, worker.worker_id),
        )?
        .is_some_and(|member| member.worker_id == worker.worker_id && member.validate().is_ok()))
}

pub(super) fn current_placement(
    view: &mut GroupRead<'_>,
    scope: &GroupIdentity,
    current: &State,
    group_id: Uuid,
) -> Result<Option<ScopedQueuePlacement>, ProtocolError> {
    let Some(placement) = load::<ScopedQueuePlacement>(view, &placement_key(group_id))? else {
        return Ok(None);
    };
    let group = &placement.group;
    if group.group_id != group_id
        || group.cluster_id != scope.cluster_id
        || group.database != scope.database
    {
        return Err(stale_placement());
    }
    placement.validate(group)?;
    if matches!(current.pending, Some(Pending::Fence(_))) {
        return Err(stale_placement());
    }
    if current.ready.view.run_id != placement.view.run_id {
        return if published_new_run(view, current, placement.view.run_id)? {
            Ok(None)
        } else {
            Err(stale_placement())
        };
    }
    if matches!(&current.pending, Some(Pending::Ready(pending))
            if pending.change == ReadyChange::Remove
                && placement.has_holder(pending.target))
        || view.get(&transfer_key(group_id))?.is_some()
    {
        return Err(stale_placement());
    }
    let record = load::<GroupRecord>(view, &machine::key(GROUPS, group.group_id))?
        .filter(|record| {
            &record.identity == group
                && record.generation == placement.data_generation
                && record.replica_count == 3
        })
        .ok_or_else(stale_placement)?;
    if load::<Uuid>(view, &replacement_key(group_id))?.is_some_and(|next| next != record.generation)
    {
        return Err(stale_placement());
    }
    let mut replicas = Vec::with_capacity(3);
    for holder in placement.members {
        let member = load::<ReplicaMember>(
            view,
            &machine::member_worker_key(placement.data_generation, holder.worker_id),
        )?
        .filter(|member| member.worker_id == holder.worker_id && member.validate().is_ok())
        .ok_or_else(stale_placement)?;
        if placement.has_holder(holder)
            && (load::<WorkerIdentity>(
                view,
                &machine::ready_key(current.ready_generation, holder.worker_id),
            )? != Some(holder)
                || !load::<WorkerRecord>(view, &machine::key(WORKERS, holder.worker_id))?
                    .is_some_and(|entry| WorkerIdentity::from(&entry.enrollment) == holder)
                || view
                    .get(&machine::pool_drain::barrier_key(member.pool_id))?
                    .is_some()
                || view
                    .get(&machine::worker_drain::barrier_key(holder.worker_id))?
                    .is_some())
        {
            return Err(stale_placement());
        }
        replicas.push(member);
    }
    replicas.sort_unstable_by_key(|member| member.node_id);
    if validate_voters(&replicas).is_err()
        || machine::group_root(
            3,
            replicas
                .iter()
                .fold(machine::group_seed(), machine::group_step),
        ) != record.replica_root
    {
        return Err(stale_placement());
    }
    Ok(Some(placement))
}

fn published_new_run(
    view: &mut GroupRead<'_>,
    state: &State,
    prior_run: Uuid,
) -> Result<bool, ProtocolError> {
    Ok(!matches!(state.pending, Some(Pending::Fence(_)))
        && permits_new_run(
            prior_run,
            state.ready.view,
            load(view, machine::COLD_PUBLICATION)?,
            load::<bool>(view, &machine::key(machine::RUNS, prior_run))? == Some(true),
        ))
}
