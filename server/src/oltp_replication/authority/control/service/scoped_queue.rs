use super::*;
use crate::oltp_replication::authority::control::{
    ScopedQueuePlacement,
    machine::scoped_queue::{placement_key, replacement_key},
    scoped_queue::stale as stale_placement,
};
#[cfg(test)]
use crate::oltp_replication::authority::control::machine::scoped_queue::creation_digest;

impl ControlHandle {
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
    if current.ready.view.run_id != placement.view.run_id
        || matches!(current.pending, Some(Pending::Fence(_)))
        || matches!(&current.pending, Some(Pending::Ready(pending))
            if pending.change == ReadyChange::Remove
                && placement.members.contains(&pending.target))
        || view.get(&replacement_key(group.group_id))?.is_some()
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
    let mut replicas = Vec::with_capacity(3);
    for holder in placement.members {
        let member = load::<ReplicaMember>(
            view,
            &machine::member_worker_key(placement.data_generation, holder.worker_id),
        )?
        .filter(|member| member.worker_id == holder.worker_id && member.validate().is_ok())
        .ok_or_else(stale_placement)?;
        if load::<WorkerIdentity>(
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
                .is_some()
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
