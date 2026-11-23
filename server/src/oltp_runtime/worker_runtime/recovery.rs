//! Recovery dispatch consumes surviving RAM records, never durable identity
//! history as executable input. The existing actor budget bounds each attempt.
use super::*;
use crate::oltp_runtime::authority_fence::{fence_groups, publish_fence, transition_id};
use crate::oltp_replication::{
    authority::control::{
        ControlReply, FencePending, InventoryPage, Pending, ReadyChange, ReadyPending,
        ReadyPhase, ServiceAction,
    },
    service_rpc::TimestampClient,
};
use crate::oltp_runtime::worker_endpoint::{PeerReachability, QueuePeerClient};

#[derive(Default)]
pub(super) struct RecoveryProbe {
    next_turn: Option<tokio::time::Instant>,
    next_at: Option<tokio::time::Instant>,
    after: Option<Uuid>,
    failed: Option<(WorkerIdentity, u8)>,
    pub(super) removal: Option<(Uuid, WorkerIdentity, ReadyMembership)>,
}

impl RecoveryProbe {
    fn observe(&mut self, worker: WorkerIdentity, reachability: PeerReachability) -> Option<u8> {
        match reachability {
            PeerReachability::Indeterminate => None,
            PeerReachability::Reachable => {
                if self.failed.is_some_and(|(failed, _)| failed == worker) {
                    self.failed = None;
                }
                None
            }
            PeerReachability::Unreachable => {
                let misses = match self.failed {
                    Some((failed, misses)) if failed == worker => misses.saturating_add(1),
                    _ => 1,
                };
                self.failed = Some((worker, misses));
                Some(misses)
            }
        }
    }
}

#[cfg(test)]
#[test]
fn r05_indeterminate_probes_preserve_genuine_misses() {
    use PeerReachability::{Indeterminate, Reachable, Unreachable};

    let worker = WorkerIdentity {
        worker_id: nabedra_rt::ids::uuid_v4(),
        incarnation: nabedra_rt::ids::uuid_v4(),
        certificate_sha256: Digest::from_bytes([1; 32]),
        enrollment_revision: 1,
    };
    let mut probe = RecoveryProbe::default();
    assert_eq!(probe.observe(worker, Unreachable), Some(1));
    assert_eq!(probe.observe(worker, Unreachable), Some(2));
    for _ in 0..3 {
        assert_eq!(probe.observe(worker, Indeterminate), None);
        assert_eq!(probe.failed, Some((worker, 2)));
    }
    assert_eq!(probe.observe(worker, Unreachable), Some(3));
    assert_eq!(probe.observe(worker, Reachable), None);
    assert_eq!(probe.failed, None);
    assert_eq!(probe.observe(worker, Unreachable), Some(1));
}

fn accepted_control(reply: ControlReply) -> Result<(), ProtocolError> {
    match reply {
        ControlReply::Rejected(_) => Err(unknown()),
        _ => Ok(()),
    }
}

impl WorkerRuntime {
    /// The timer observes this operation without owning its lifetime. A slow
    /// legal frame keeps its exact charges/task through ticks and caller loss;
    /// shutdown drains the single slot before shutting down hosted services.
    pub(super) fn tick_supervision(self: &Arc<Self>) -> Result<(), ProtocolError> {
        let actor = match self.supervision.try_admit() {
            Ok(actor) => actor,
            Err(_) => return Ok(()),
        };
        let runtime = self.clone();
        let operation = actor.spawn(move || async move {
            if let Err(error) = runtime.supervise_membership().await {
                tracing::debug!(code=?error.code, message=error.message,
                    "queue recovery membership progress waits");
            }
            Ok(())
        })?;
        drop(operation);
        Ok(())
    }
    pub(super) async fn supervise_membership(&self) -> Result<(), ProtocolError> {
        if self.services.replication_factor == 1 {
            return Ok(());
        }
        {
            let mut probe = self.recovery_probe.lock().map_err(|_| unknown())?;
            let now = nabedra_rt::time::now();
            if probe.next_turn.is_some_and(|next| now < next) {
                return Ok(());
            }
            probe.next_turn = Some(now + std::time::Duration::from_secs(1));
        }
        let state = self.services.control.inspect().await?.ok_or_else(unknown)?;
        if let Some(pending) = state.pending {
            return match pending {
                Pending::Fence(fence)
                    if !fence.cold && fence.next_owner == Some(self.services.local) =>
                {
                    self.advance_owner_fence(&fence).await
                }
                Pending::Ready(pending)
                    if pending.change == ReadyChange::Remove
                        && state.owner == Some(self.services.local) =>
                {
                    self.advance_removal(&pending).await
                }
                _ => Ok(()),
            };
        }
        if state.owner == Some(self.services.local) {
            let control = &self.services.control;
            if let Some(record) = control.active_worker_drain().await?
                && record.phase == crate::oltp_replication::authority::control::WorkerDrainPhase::Draining
            {
                match control.begin_worker_drain_removal(&self.services.operator, &record, state.ready).await {
                    Ok(()) => return Ok(()),
                    Err(error) if error.code == ErrorCode::Conflict => {
                        #[cfg(test)]
                        if let Some(dir) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") {
                            let path = std::path::PathBuf::from(dir).join(format!("worker-drain-blocked-{}", record.id));
                            let _ = std::fs::write(path, error.message.as_bytes());
                        }
                        tracing::debug!(drain_id=%record.id, message=error.message, "worker drain is waiting for its placement requirements");
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let peer = QueuePeerClient::new(
            self.services.manager.client(),
            self.services.shared.clone(),
            self.services.control.identity().clone(),
        )?;
        let removal = self.recovery_probe.lock().map_err(|_| unknown())?.removal;
        if let Some((fence, target, expected)) = removal {
            let mut fenced = expected;
            fenced.view.owner_term = fenced.view.owner_term.checked_add(1).ok_or_else(unknown)?;
            let owns_fenced_view = state.owner == Some(self.services.local) && state.ready == fenced;
            if state.ready == expected || owns_fenced_view {
                // A quorum fence can take long enough for the exact peer to
                // recover. Confirm it again before changing ready membership.
                match peer
                    .probe(target, self.services.scope.clone(), state.ready)
                    .await
                {
                    PeerReachability::Reachable => {
                        let mut probe = self.recovery_probe.lock().map_err(|_| unknown())?;
                        probe.removal = None;
                        probe.failed = None;
                        return Ok(());
                    }
                    PeerReachability::Indeterminate => return Ok(()),
                    PeerReachability::Unreachable => {}
                }
                if !owns_fenced_view {
                    let applied = self
                        .services
                        .control
                        .begin_owner_fence_local(
                            &self.services.operator,
                            fence,
                            expected,
                            self.services.local,
                        )
                        .await?;
                    return accepted_control(applied.reply);
                }
                let op = transition_id("queue-remove", &(fence, target))?;
                let applied = self
                    .services
                    .control
                    .begin_queue_removal_local(
                        &self.services.operator,
                        op,
                        state.ready.view,
                        target,
                        fence,
                    )
                    .await?;
                accepted_control(applied.reply)?;
                self.recovery_probe.lock().map_err(|_| unknown())?.removal = None;
                return Ok(());
            }
            self.recovery_probe.lock().map_err(|_| unknown())?.removal = None;
        }
        let current = self.services.control.current(&self.services.scope).await?;
        let target = {
            let mut probe = self.recovery_probe.lock().map_err(|_| unknown())?;
            let now = nabedra_rt::time::now();
            if probe.next_at.is_some_and(|next| now < next) {
                return Ok(());
            }
            probe.next_at = Some(now + std::time::Duration::from_secs(1));
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica.authority() != current {
                return Ok(());
            }
            let target = replica
                .member_after(probe.after)
                .or_else(|| replica.member_after(None));
            probe.after = target.map(|worker| worker.worker_id);
            target
        };
        let Some(target) = target.filter(|worker| *worker != self.services.local) else {
            return Ok(());
        };
        let started = nabedra_rt::time::now();
        let live = peer
            .probe(target, self.services.scope.clone(), current.ready)
            .await;
        let Some(failures) = self
            .recovery_probe
            .lock()
            .map_err(|_| unknown())?
            .observe(target, live)
        else {
            return Ok(());
        };
        tracing::debug!(worker=%target.worker_id, incarnation=%target.incarnation,
            owner_term=current.ready.view.owner_term, membership_revision=current.ready.view.membership_revision,
            failures, elapsed_ms=nabedra_rt::time::elapsed(started).as_millis(), "worker reachability probe missed");
        if failures < 3 {
            return Ok(());
        }
        // Probes trigger an attempt, never authority or physical exclusion.
        // If the owner is unreachable, the lowest responding ready holder
        // attempts the real quorum fence. Other holders keep their RAM copies.
        if target == current.owner {
            let mut after = None;
            loop {
                let candidate = self
                    .services
                    .replica
                    .lock()
                    .map_err(|_| unknown())?
                    .member_after(after);
                let Some(candidate) = candidate else {
                    return Ok(());
                };
                after = Some(candidate.worker_id);
                if candidate == target {
                    continue;
                }
                if self.services.control.data_worker(candidate).await?.is_some() {
                    continue;
                }
                if candidate == self.services.local {
                    break;
                }
                if peer
                    .probe(candidate, self.services.scope.clone(), current.ready)
                    .await
                    != PeerReachability::Unreachable
                {
                    return Ok(());
                }
            }
        } else if current.owner != self.services.local {
            return Ok(());
        }
        let transition = transition_id(
            "owner-recovery",
            &(current.ready, self.services.local, target),
        )?;
        self.recovery_probe.lock().map_err(|_| unknown())?.removal =
            Some((transition, target, current.ready));
        let reply = self
            .services
            .control
            .begin_owner_fence_local(
                &self.services.operator,
                transition,
                current.ready,
                self.services.local,
            )
            .await?;
        accepted_control(reply.reply)
    }

    async fn advance_owner_fence(&self, fence: &FencePending) -> Result<(), ProtocolError> {
        let control = &self.services.control;
        fence_groups(control, &self.services.manager, &self.services.control_client, fence).await?;
        publish_fence(control, &self.services.operator, fence).await
    }

    async fn advance_removal(&self, pending: &ReadyPending) -> Result<(), ProtocolError> {
        let control = &self.services.control;
        match pending.phase {
            ReadyPhase::Building => {
                // Replay uses the same command identity and exact original
                // member page; each bounded page follows Control's cursor.
                let raw = control
                    .ready_page_read(pending.expected, pending.source_after)
                    .await?;
                let page: InventoryPage<WorkerIdentity> =
                    serde_json::from_slice(raw.as_ref()).map_err(|_| unknown())?;
                let members = page
                    .items
                    .iter()
                    .filter(|worker| **worker != pending.target)
                    .copied()
                    .collect::<Vec<_>>();
                if !members.is_empty() {
                    let op = transition_id(
                        "remove-ready-page",
                        &(pending.transition_id, pending.source_after),
                    )?;
                    accepted_control(
                        control
                            .ready_page_local(
                                &self.services.operator,
                                op,
                                pending.transition_id,
                                Page::new(members).map_err(|_| unknown())?,
                            )
                            .await?
                            .reply,
                    )?;
                }
                if page.done {
                    accepted_control(
                        control
                            .seal_ready_local(
                                &self.services.operator,
                                transition_id("remove-ready-seal", &pending.transition_id)?,
                                pending.transition_id,
                            )
                            .await?
                            .reply,
                    )?;
                }
                Ok(())
            }
            ReadyPhase::AwaitingTimestampFence => {
                let group = self.services.timestamps.identity().clone();
                let client = TimestampClient::new(
                    group,
                    self.services.shared.clone(),
                    self.services.manager.client(),
                    self.services.control_client.clone(),
                    ServiceAction::ControlFence {
                        transition_id: pending.transition_id,
                    },
                )?;
                let receipt = client
                    .fence_ready(
                        pending.transition_id,
                        pending.expected,
                        pending.next.ok_or_else(unknown)?,
                    )
                    .await?;
                accepted_control(
                    control
                        .timestamp_fenced(
                            transition_id("remove-tso-fence", &pending.transition_id)?,
                            receipt,
                        )
                        .await?
                        .reply,
                )
            }
            ReadyPhase::AwaitingQueueProof => {
                tracing::debug!(transition = %pending.transition_id, "collecting surviving legacy queue copies");
                let peer = QueuePeerClient::new(
                    self.services.manager.client(),
                    self.services.shared.clone(),
                    control.identity().clone(),
                )?;
                let ticket = JoinTicket {
                    ticket_id: pending.transition_id,
                    target: pending.target,
                    source_ready: pending.expected,
                };
                let proof = self.services.queue.copy_removal(ticket, &peer, self.services.status.as_ref()).await?;
                accepted_control(
                    control
                        .publish_ready(
                            transition_id("remove-publish", &pending.transition_id)?,
                            proof,
                        )
                        .await?
                        .reply,
                )?;
                tracing::debug!(transition = %pending.transition_id, "published surviving legacy queue copies");
                self.recovery_probe.lock().map_err(|_| unknown())?.removal = None;
                Ok(())
            }
        }
    }
    pub(super) async fn dispatch_retained(self: &Arc<Self>) -> Result<(), ProtocolError> {
        {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            let active = self.active.lock().map_err(|_| unknown())?;
            if replica.authority().owner != self.services.local || !replica.has_recovery_work(now(), &active) {
                return Ok(());
            }
        }
        let state = self.services.control.inspect().await?.ok_or_else(unknown)?;
        if state.owner != Some(self.services.local)
            || state.pending.is_some()
            || state.invalid_ready != 0
            || state.ready.ready_count == 0
            || self
                .recovery_probe
                .lock()
                .map_err(|_| unknown())?
                .removal
                .is_some()
        {
            return Ok(());
        }
        let current = QueueAuthority {
            ready: state.ready,
            owner: self.services.local,
        };
        let ids = {
            let replica = self.services.replica.lock().map_err(|_| unknown())?;
            if replica.authority() != current {
                return Ok(());
            }
            let mut cursor = self.recovery_cursor.lock().map_err(|_| unknown())?;
            let ids = replica.recovery_ids(now(), *cursor, 128);
            *cursor = ids.last().copied();
            ids
        };
        for id in ids {
            let Ok(gate) = self.gate(id)?.try_lock_owned() else { continue };
            {
                let active = self.active.lock().map_err(|_| unknown())?;
                if active.contains(&id) {
                    continue;
                }
                // Recovery must not fill the transport's bounded lanes with
                // one actor per historical terminal record in a single turn.
                if active.len() >= 4 {
                    break;
                }
            }
            let actor = match self.actors.try_admit() {
                Ok(actor) => actor,
                Err(_) => break,
            };
            if !self.active.lock().map_err(|_| unknown())?.insert(id) {
                continue;
            }
            let runtime = self.clone();
            let operation = actor.spawn(move || async move {
                let mut settlement = Settlement {
                    runtime: runtime.clone(),
                    id,
                    armed: false,
                };
                // One bounded attempt owns the original actor reservation.
                // Transient failures retain RAM and retry on a later turn.
                let result = runtime
                    .recover_retained(current, id, gate, &mut settlement)
                    .await;
                match result {
                    Ok(()) => settlement.armed = false,
                    Err(error) => tracing::debug!(%id, code=?error.code,
                        message=error.message, "retained request awaits recovery"),
                }
                Ok(())
            });
            match operation {
                Ok(operation) => drop(operation),
                Err(error) => {
                    self.active.lock().map_err(|_| unknown())?.remove(&id);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn recover_retained(
        &self,
        current: QueueAuthority,
        id: Uuid,
        gate: tokio::sync::OwnedMutexGuard<()>,
        settlement: &mut Settlement,
    ) -> Result<(), ProtocolError> {
        if self.services.control.current(&self.services.scope).await? != current {
            return Err(unknown());
        }
        let tentative = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .record(id)
            .is_some_and(|record| !record.accepted);
        if tentative {
            self.resume_staged(current, id).await?;
        }
        drop(gate);
        self.services.queue.settle_request_read_grants(id).await?;
        self.services.queue.ensure_copies(current, id).await?;
        let record = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .observation(id)
            .map_err(QueueError::protocol)?;
        if record.state.terminal() {
            return self.release_retained(id).await;
        }
        let (input, binding) = self.binding(id).await?;
        let mut resumed = None;
        if input.request.operation.is_mutation() {
            let (attempt, decision) = self
                .recover_queued_attempt(current, &input, binding)
                .await?;
            resumed = Some(attempt);
            let encoded = if let Some(decision) = decision {
                if decision.statement().request != binding {
                    return Err(unknown());
                }
                Some(match decision.outcome().ok_or_else(unknown)?.result() {
                    Ok(value) => response::encode_success(id, &value, true),
                    Err(error) => response::encode_error(Some(id), error),
                })
            } else {
                match self.durable_lookup(id).await? {
                    Some(IdentityLookup::Terminal(terminal @ TerminalIdentity::Failed { .. }))
                        if terminal.request_binding() == binding =>
                    {
                        Some(terminal_response(id, &terminal)?)
                    }
                    Some(IdentityLookup::Reserved(reservation))
                        if reservation.kind == RequestKind::Mutation
                            && reservation.scoped.is_none()
                            && reservation.binds(binding) =>
                    {
                        None
                    }
                    _ => return Err(unknown()),
                }
            };
            if let Some(encoded) = encoded {
                self.services
                    .queue
                    .finish(id, encoded.status, encoded.body, now())
                    .await?;
                self.changed.notify_waiters();
                return self.release_retained(id).await;
            }
        }
        drop(input);
        settlement.armed = true;
        self.execute(id, None, resumed).await
    }

    async fn release_retained(&self, id: Uuid) -> Result<(), ProtocolError> {
        let statement = self
            .services
            .replica
            .lock()
            .map_err(|_| unknown())?
            .terminal_statement(id)
            .map_err(QueueError::protocol)?;
        self.services.timestamps.release(statement).await?;
        Ok(())
    }
}
