use super::*;
use crate::oltp_replication::authority::control::{
    GroupRecord, InventoryPage, ScopedQueueTransferReport, ScopedQueueTransition,
};
use crate::oltp_runtime::{
    authority_fence::transition_id,
    worker_endpoint::{AuthorityReader, PeerReachability},
};

const PASS_DELAY: Duration = Duration::from_millis(750);
const GROUP_DEADLINE: Duration = Duration::from_secs(30);
const RECORDS_PER_TURN: usize = 8;
const RECORD_CURSORS: usize = 64;

#[derive(Default)]
struct Progress {
    after: Option<Uuid>,
    records: std::collections::VecDeque<(Uuid, Uuid)>,
}

impl Progress {
    fn remember(&mut self, group: Uuid, after: Option<Uuid>) {
        self.records.retain(|(id, _)| *id != group);
        if let Some(after) = after {
            if self.records.len() == RECORD_CURSORS {
                self.records.pop_front();
            }
            self.records.push_back((group, after));
        }
    }
}

impl ScopedWorker {
    pub(in crate::oltp_runtime) fn start_maintenance(
        self: &Arc<Self>,
    ) -> Result<(), ProtocolError> {
        let mut slot = self.maintenance.lock().map_err(|_| unavailable())?;
        if slot.is_some() {
            return Err(unavailable());
        }
        let memory = self.resources().reserve_scratch(ByteClass::Data, 4096)?;
        let weak = Arc::downgrade(self);
        *slot = Some(nabedra_rt::task::spawn(async move {
            let _memory = memory;
            let mut progress = Progress::default();
            loop {
                nabedra_rt::time::sleep(PASS_DELAY).await;
                let Some(worker) = weak.upgrade() else {
                    break;
                };
                if worker.stopping.load(Ordering::Acquire) {
                    break;
                }
                match nabedra_rt::time::timeout(GROUP_DEADLINE, worker.advance(&mut progress)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::debug!(
                        group = ?progress.after, code = ?error.code, message = error.message,
                        "scoped queue maintenance waits"
                    ),
                    Err(_) => tracing::debug!(group = ?progress.after, "scoped queue maintenance deadline elapsed"),
                }
            }
        }));
        Ok(())
    }

    async fn advance(self: &Arc<Self>, progress: &mut Progress) -> Result<(), ProtocolError> {
        let control = &self.services.control;
        let revision = control.group_inventory_revision().await?;
        let reply = control.groups(revision, progress.after).await?;
        let body = reply.sections[0].as_ref().ok_or_else(unavailable)?;
        let budget = crate::oltp::mvcc::json_decode_budget(body.as_slice())?;
        let reply = reply.reserve_reply_decode(self.resources(), budget).await?;
        let page = reply.decode_section::<InventoryPage<GroupRecord>>(0)?;
        if page.revision != revision || !page.done && page.after <= progress.after {
            return Err(unavailable());
        }
        let next = page.items.iter().find(|group| {
            matches!(group.identity.kind, GroupKind::Data { .. })
                && progress
                    .after
                    .is_none_or(|after| group.identity.group_id > after)
        });
        let Some(group) = next.cloned() else {
            progress.after = (!page.done).then_some(page.after).flatten();
            return Ok(());
        };
        let id = group.identity.group_id;
        progress.after = Some(id);
        drop((page, reply));
        self.advance_group(&group, progress).await
    }

    async fn advance_group(
        self: &Arc<Self>,
        group: &GroupRecord,
        progress: &mut Progress,
    ) -> Result<(), ProtocolError> {
        let control = &self.services.control;
        let id = group.identity.group_id;
        self.services.endpoint.cleanup_scoped_group(id).await?;
        if let Some(transfer) = control.scoped_queue_transition(id).await? {
            if self.services.operator && self.replace_stale_transfer(group, &transfer).await? {
                return Ok(());
            }
            return self.advance_transfer(transfer).await;
        }
        let current = match control.scoped_queue(&group.identity).await {
            Ok(Some(placement)) => placement,
            Ok(None) => return Ok(()),
            Err(error) if self.services.operator && error.code == ErrorCode::StorageUnavailable => {
                let Some(previous) = control.scoped_queue_predecessor(id).await? else {
                    return Ok(());
                };
                return self.maybe_begin_transfer(group, previous, true).await;
            }
            Err(error) => return Err(error),
        };
        let owner = current.owner == self.services.local;
        if !owner && self.services.operator {
            self.maybe_begin_transfer(group, current.clone(), false)
                .await?;
        }
        if !current.has_holder(self.services.local) {
            return Ok(());
        }
        let mut cursor = progress
            .records
            .iter()
            .find(|(group, _)| *group == id)
            .map(|(_, after)| *after);
        let mut ready = None;
        for _ in 0..RECORDS_PER_TURN {
            let record = self
                .services
                .replica
                .lock()
                .map_err(|_| unavailable())?
                .scoped_queues()
                .map_err(QueueError::protocol)?
                .export_next(&current, cursor)
                .map_err(QueueError::protocol)?;
            let Some(record) = record else {
                progress.remember(id, None);
                return Ok(());
            };
            let request_id = record.reservation.request_id;
            cursor = Some(request_id);
            progress.remember(id, cursor);
            if owner && record.terminal_quorum.is_some() && !record.pin_released() {
                self.release_terminal(&current, &record).await?;
            }
            if let Some(statement) = record
                .terminal_statement(&current)
                .map_err(QueueError::protocol)?
                && (!owner || record.terminal_quorum.is_some())
                && now().saturating_sub(statement.completed_at_ms) as u128
                    >= self.retention.as_millis()
            {
                self.services
                    .endpoint
                    .retire_scoped_terminal(&current, &statement, now())
                    .await?;
                continue;
            }
            if !owner {
                continue;
            }
            if record.terminal_quorum.is_some()
                || self
                    .active
                    .lock()
                    .map_err(|_| unavailable())?
                    .contains_key(&request_id)
            {
                continue;
            }
            if record.acceptance.is_none() {
                let gate = self.gate(request_id)?;
                let Ok(_guard) = gate.try_lock() else {
                    continue;
                };
                let deadline = nabedra_rt::time::now() + Duration::from_secs(5);
                if record.recovered_marker {
                    self.services
                        .endpoint
                        .restore_scoped_acceptance(
                            &self.services.peer,
                            &current,
                            request_id,
                            deadline,
                        )
                        .await?;
                } else {
                    let frame = record
                        .payload_frame(self.resources())
                        .map_err(QueueError::protocol)?;
                    self.services
                        .endpoint
                        .accept_scoped(&self.services.peer, &current, &frame, deadline, None)
                        .await?;
                }
            }
            let ready = match &mut ready {
                Some(ready) => *ready,
                slot => *slot.insert(control.current(&self.scope).await?.ready),
            };
            self.dispatch(
                current.clone(),
                ready,
                request_id,
                self.actors.try_admit()?,
                None,
            )?;
        }
        Ok(())
    }

    async fn advance_transfer(&self, transfer: ScopedQueueTransition) -> Result<(), ProtocolError> {
        let local = self.services.local;
        let control = &self.services.control;
        let group = transfer.previous.group.group_id;
        if transfer.previous.has_holder(local)
            && let Some(index) = transfer
                .previous
                .members
                .iter()
                .position(|worker| *worker == local)
        {
            if self
                .active
                .lock()
                .map_err(|_| unavailable())?
                .values()
                .any(|&owned| owned == group)
            {
                return Err(unavailable());
            }
            if transfer.frozen[index].is_none() && transfer.basis()?.is_none() {
                let inventory = self
                    .services
                    .endpoint
                    .freeze_scoped_transfer(group, transfer.id)
                    .await?;
                let report = ScopedQueueTransferReport::Frozen(inventory);
                let operation =
                    transition_id("scoped-queue-frozen", &(transfer.id, local, report))?;
                control
                    .report_scoped_queue_transfer(operation, group, transfer.id, report)
                    .await?;
                return Ok(());
            }
        }
        if !transfer.next.has_holder(local) {
            return Ok(());
        }
        let Some(index) = transfer
            .next
            .members
            .iter()
            .position(|worker| *worker == local)
        else {
            return Ok(());
        };
        if transfer.basis()?.is_none() {
            return Ok(());
        }
        if transfer.installed[index].is_none() {
            let (basis, inventory) = self
                .services
                .endpoint
                .install_scoped_transfer(
                    &self.services.peer,
                    group,
                    transfer.id,
                    nabedra_rt::time::now() + GROUP_DEADLINE,
                )
                .await?;
            let report = ScopedQueueTransferReport::Installed { basis, inventory };
            let operation = transition_id("scoped-queue-installed", &(transfer.id, local, report))?;
            control
                .report_scoped_queue_transfer(operation, group, transfer.id, report)
                .await?;
            return Ok(());
        }
        if transfer.next.owner == local && transfer.has_installed_quorum() {
            self.services.status.fence_scoped_queue(&transfer).await?;
            self.services
                .timestamps
                .fence_scoped_queue(&transfer)
                .await?;
            let participant = self
                .services
                .resolver
                .participant(&transfer.next.group)
                .await?;
            participant.fence_scoped_queue(&transfer).await?;
            self.services
                .catalog
                .participant()
                .fence_scoped_queue(&transfer)
                .await?;
            let operation = transition_id("scoped-queue-publish", &transfer.id)?;
            control
                .publish_scoped_queue_transfer(operation, group, transfer.id)
                .await?;
        }
        Ok(())
    }

    async fn replace_stale_transfer(
        &self,
        group: &GroupRecord,
        transfer: &ScopedQueueTransition,
    ) -> Result<bool, ProtocolError> {
        let local = self.services.local;
        let removed = self.removed_transfer_destination(group, transfer)?;
        let ready = scoped_authority(&transfer.previous)
            .map_err(QueueError::protocol)?
            .ready;
        let reachable = |worker| {
            let ready = ready.clone();
            async move {
                if worker == local {
                    PeerReachability::Reachable
                } else {
                    self.services
                        .peer
                        .probe(worker, self.scope.clone(), ready)
                        .await
                }
            }
        };
        let owner = reachable(transfer.next.owner).await;
        let replace_destination = removed.is_some() || owner == PeerReachability::Unreachable;
        let needs_sources = transfer.basis()?.is_some() && !transfer.has_installed_quorum();
        if !replace_destination && !needs_sources {
            return Ok(false);
        }
        let old = transfer.previous.members;
        let replies = tokio::join!(reachable(old[0]), reachable(old[1]), reachable(old[2]));
        let reachability = [replies.0, replies.1, replies.2];
        let lost_source = transfer
            .frozen
            .iter()
            .zip(reachability)
            .any(|(cut, reply)| cut.is_some() && reply == PeerReachability::Unreachable);
        if !replace_destination && !lost_source {
            return Ok(false);
        }
        let candidates = old.into_iter().zip(reachability).filter(|(worker, reply)| {
            Some(*worker) != removed
                && transfer.previous.has_holder(*worker)
                && *reply == PeerReachability::Reachable
        });
        if candidates.clone().count() < 2 {
            if removed.is_some() {
                return Err(unavailable());
            }
            return Ok(false);
        }
        let routes = self
            .services
            .control
            .replica_routes(&group.identity)
            .await?;
        let Some((next, _)) = candidates
            .filter(|(worker, _)| {
                routes
                    .members
                    .iter()
                    .any(|member| member.worker_id == worker.worker_id)
            })
            .min_by_key(|(worker, _)| {
                (
                    removed.is_some() && *worker != transfer.next.owner,
                    worker.worker_id,
                )
            })
        else {
            return Ok(false);
        };
        if let Some(removed) = removed {
            let operation = transition_id(
                "scoped-queue-rebase-transfer",
                &(transfer.id, group.generation, local, removed, next),
            )?;
            self.services
                .control
                .rebase_scoped_queue_transfer(
                    operation,
                    group.identity.group_id,
                    transfer.id,
                    removed,
                    next,
                )
                .await?;
            tracing::debug!(group=%group.identity.group_id, previous_transfer=%transfer.id,
                next_transfer=%operation, removed=%removed.worker_id, owner=%next.worker_id,
                "scoped queue transfer rebased after ready removal");
            return Ok(true);
        }
        let operation = transition_id(
            "scoped-queue-replace-transfer",
            &(transfer.id, group.generation, next),
        )?;
        self.services
            .control
            .begin_scoped_queue_transfer(
                operation,
                transfer.previous.clone(),
                next,
                Some(transfer.id),
            )
            .await?;
        Ok(true)
    }

    fn removed_transfer_destination(
        &self,
        group: &GroupRecord,
        transfer: &ScopedQueueTransition,
    ) -> Result<Option<WorkerIdentity>, ProtocolError> {
        let next = &transfer.next;
        if next.unavailable_holder.is_some()
            || next.data_generation != group.generation
            || transfer.previous.data_generation != group.generation
            || transfer.previous.members != next.members
        {
            return Ok(None);
        }
        let replica = self.services.replica.lock().map_err(|_| unavailable())?;
        let authority = replica.authority().ready.view.authority();
        if authority.run_id != next.view.run_id || !replica.is_ready_member() {
            return Ok(None);
        }
        let mut removed = None;
        for worker in next.members {
            match replica.read_member(authority, worker.worker_id) {
                Some(ready) if ready == worker => {}
                None if removed.is_none() => removed = Some(worker),
                _ => return Ok(None),
            }
        }
        Ok(removed)
    }

    async fn maybe_begin_transfer(
        &self,
        group: &GroupRecord,
        previous: ScopedQueuePlacement,
        fenced: bool,
    ) -> Result<(), ProtocolError> {
        let local = self.services.local;
        let control = &self.services.control;
        let ready = crate::oltp_runtime::replica::scoped_store::scoped_authority(&previous)
            .map_err(QueueError::protocol)?
            .ready;
        if !fenced
            && group.generation == previous.data_generation
            && (previous.owner == local
                || self
                    .services
                    .peer
                    .probe(previous.owner, self.scope.clone(), ready)
                    .await
                    != PeerReachability::Unreachable)
        {
            return Ok(());
        }
        let mut live = [None; 3];
        for (index, worker) in previous.members.into_iter().enumerate() {
            if !fenced && worker == previous.owner && group.generation == previous.data_generation {
                continue;
            }
            if worker == local
                || self
                    .services
                    .peer
                    .probe(worker, self.scope.clone(), ready)
                    .await
                    == PeerReachability::Reachable
            {
                live[index] = Some(worker);
            }
        }
        if live.iter().flatten().count() < 2 {
            return Ok(());
        }
        let next = if group.generation != previous.data_generation {
            let routes = control.replica_routes(&group.identity).await?;
            live.into_iter().flatten().find(|worker| {
                routes
                    .members
                    .iter()
                    .any(|member| member.worker_id == worker.worker_id)
            })
        } else {
            live.into_iter().flatten().next()
        }
        .ok_or_else(unavailable)?;
        let operation = transition_id(
            "scoped-queue-takeover",
            &(
                previous.view,
                previous.group.group_id,
                group.generation,
                next,
            ),
        )?;
        control
            .begin_scoped_queue_transfer(operation, previous, next, None)
            .await?;
        Ok(())
    }
}
