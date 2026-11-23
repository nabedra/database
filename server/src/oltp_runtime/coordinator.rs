//! Owner-side RAM copy barriers. Data execution is deliberately a separate
//! backend: this component never proposes application bodies to a Raft group.
use super::{
    replica::*,
    worker_endpoint::{AuthorityReader, QueuePeerClient, QueueState},
};
use crate::{
    oltp::pool::ExecutionClass,
    oltp_replication::{
        authority::status::StatusService,
        multiplex::EndpointFuture,
        service_rpc::{ReadGrant, ReadScope},
        types::{AuthorityView, JoinTicket, WorkerIdentity},
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, RequestState},
};
use futures_util::{StreamExt, stream::FuturesOrdered};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use nabedra_rt::time::{sleep_until, timeout_at};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use uuid::Uuid;

const READ_GRANT_COPY_DEADLINE: Duration = Duration::from_secs(30);
const READ_GRANT_COPY_BACKOFF: Duration = Duration::from_millis(25);
const MAX_PARALLEL_QUEUE_COPIES: usize = 4;
const QUEUE_COPY_SLOT_TIMEOUT: Duration = Duration::from_secs(30);

pub trait ReplicaTransport: Send + Sync + 'static {
    fn install<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
    ) -> EndpointFuture<'a, CopyAcknowledgement>;
    fn install_version<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
        _expected: RecordVersion,
    ) -> EndpointFuture<'a, CopyAcknowledgement> {
        self.install(target, frame)
    }
    fn state(
        &self,
        _target: WorkerIdentity,
        _scope: QueueScope,
        _view: AuthorityView,
        _id: Uuid,
    ) -> EndpointFuture<'_, QueueState> {
        Box::pin(async { Err(QueueError::Unavailable.protocol()) })
    }
    fn copy_read_grants(
        &self, _target: WorkerIdentity, _scope: QueueScope, _view: AuthorityView,
        _id: Uuid, _set: ReadGrantSet,
    ) -> EndpointFuture<'_, ()> {
        Box::pin(async { Err(QueueError::Unavailable.protocol()) })
    }
    fn settle_read_grant(
        &self, _scope: QueueScope, _view: AuthorityView, _grant: ReadGrant,
    ) -> EndpointFuture<'_, ()> {
        Box::pin(async { Err(QueueError::Unavailable.protocol()) })
    }
}
impl ReplicaTransport for QueuePeerClient {
    fn install<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
    ) -> EndpointFuture<'a, CopyAcknowledgement> {
        Box::pin(QueuePeerClient::install(self, target, frame))
    }
    fn install_version<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
        expected: RecordVersion,
    ) -> EndpointFuture<'a, CopyAcknowledgement> {
        Box::pin(QueuePeerClient::install_version(self, target, frame, expected))
    }
    fn state(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        view: AuthorityView,
        id: Uuid,
    ) -> EndpointFuture<'_, QueueState> {
        Box::pin(QueuePeerClient::state(self, target, scope, view, id))
    }
    fn copy_read_grants(
        &self, target: WorkerIdentity, scope: QueueScope, view: AuthorityView,
        id: Uuid, set: ReadGrantSet,
    ) -> EndpointFuture<'_, ()> {
        Box::pin(QueuePeerClient::copy_read_grants(self, target, scope, view, id, set))
    }
    fn settle_read_grant(
        &self, scope: QueueScope, view: AuthorityView, grant: ReadGrant,
    ) -> EndpointFuture<'_, ()> {
        Box::pin(QueuePeerClient::settle_read_grant(self, scope, view, grant))
    }
}

pub struct QueueCoordinator {
    replica: Arc<Mutex<WorkerReplica>>,
    authority: Arc<dyn AuthorityReader>,
    peers: Arc<dyn ReplicaTransport>,
    progress: Mutex<BTreeMap<Uuid, Weak<AsyncMutex<()>>>>,
    copy_slots: Semaphore,
}
impl QueueCoordinator {
    /// Collect survivor records before sealing the source inventory. Readiness
    /// requires a fresh statement from every surviving incarnation after repair.
    pub(crate) async fn copy_removal(
        &self,
        ticket: JoinTicket,
        peer: &QueuePeerClient,
        status: &dyn StatusService,
    ) -> Result<crate::oltp_replication::authority::control::VerifiedQueueCopy, ProtocolError> {
        let gate = self.gate(ticket.ticket_id)?;
        let _copy = gate.lock().await;
        let (scope, local) = {
            let source = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            (source.scope().clone(), source.local_identity())
        };
        let current = self.authority.removal_view(&scope, local, ticket).await?;
        self.merge_removal_inventory(ticket, peer, status, &scope, local, current)
            .await?;
        if self.authority.removal_view(&scope, local, ticket).await? != current {
            return Err(QueueError::Stale.protocol());
        }
        let mut verifier = {
            let mut source = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            source
                .begin_removal(current, ticket)
                .map_err(QueueError::protocol)?;
            crate::oltp_replication::authority::control::VerifiedQueueCopy::begin_removal(
                &source, ticket,
            )?
        };
        loop {
            let (target, after_record) = {
                let source = self
                    .replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?;
                let (after_worker, after_record) = source
                    .removal_progress(ticket)
                    .map_err(QueueError::protocol)?;
                (
                    source
                        .removal_member(ticket, after_worker)
                        .map_err(QueueError::protocol)?,
                    after_record,
                )
            };
            let Some(target) = target else { break };
            if target != local {
                let next = {
                    let source = self
                        .replica
                        .lock()
                        .map_err(|_| QueueError::Unavailable.protocol())?;
                    source
                        .record_after(after_record)
                        .map(|id| source.frame(id).map(|frame| (id, frame)))
                        .transpose()
                        .map_err(QueueError::protocol)?
                };
                if let Some((id, frame)) = next {
                    let acknowledgement = peer.install(target, &frame).await?;
                    self.replica
                        .lock()
                        .map_err(|_| QueueError::Unavailable.protocol())?
                        .acknowledge_removal_record(
                            ticket,
                            target,
                            after_record,
                            acknowledgement.record(),
                        )
                        .map_err(QueueError::protocol)?;
                    tracing::debug!(ticket=%ticket.ticket_id,worker=%target.worker_id,%id,
                        "queue removal record copied");
                    continue;
                }
            }
            self.replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?
                .acknowledge_removal_worker(ticket, target)
                .map_err(QueueError::protocol)?;
        }
        // Re-read every surviving full inventory after transfer. A cached
        // cursor is never itself evidence that the target still holds a copy.
        let mut after_worker = None;
        loop {
            let target = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?
                .removal_member(ticket, after_worker)
                .map_err(QueueError::protocol)?;
            let Some(target) = target else { break };
            if target == local {
                let source = self
                    .replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?;
                verifier.observe_local(&source)?;
            } else {
                verifier.observe(
                    peer.removal_statement(target, scope.clone(), ticket)
                        .await?,
                )?;
            }
            after_worker = Some(target.worker_id);
        }
        if self.authority.removal_view(&scope, local, ticket).await? != current {
            return Err(QueueError::Stale.protocol());
        }
        let source = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?;
        verifier.finish(&source)
    }

    async fn merge_removal_inventory(
        &self,
        ticket: JoinTicket,
        peer: &QueuePeerClient,
        status: &dyn StatusService,
        scope: &QueueScope,
        local: WorkerIdentity,
        current: QueueAuthority,
    ) -> Result<(), ProtocolError> {
        self.replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .begin_removal_merge(current, ticket)
            .map_err(QueueError::protocol)?;
        loop {
            let next = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?
                .removal_merge_step(ticket)
                .map_err(QueueError::protocol)?;
            let Some((worker, after)) = next else {
                return Ok(());
            };
            let record = if worker == local {
                None
            } else {
                peer.removal_record(worker, scope.clone(), current, ticket, after)
                    .await?
            };
            if let Some((header, frame)) = record {
                let (environment, retirement) = {
                    let source = self
                        .replica
                        .lock()
                        .map_err(|_| QueueError::Unavailable.protocol())?;
                    (
                        source
                            .install_environment(current, local)
                            .map_err(QueueError::protocol)?,
                        source.install_retirement_check(&header),
                    )
                };
                if retirement.is_some()
                    && status
                        .retirement(header.request_id, header.digest)
                        .await?
                        .is_some()
                {
                    // A survivor still has a retired result. Its retirement
                    // actor applies the same certificate before the scan retries.
                    return Err(QueueError::Stale.protocol());
                }
                let (prepared, _memory) = peer.prepare_removal_record(environment, frame).await?;
                if self.authority.removal_view(scope, local, ticket).await? != current {
                    return Err(QueueError::Stale.protocol());
                }
                self.replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?
                    .merge_removal_record(ticket, worker, after, prepared, retirement)
                    .map_err(QueueError::protocol)?;
            } else {
                self.replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?
                    .acknowledge_removal_inventory(ticket, worker, after)
                    .map_err(QueueError::protocol)?;
            }
        }
    }
    pub fn new(
        replica: Arc<Mutex<WorkerReplica>>,
        authority: Arc<dyn AuthorityReader>,
        peers: Arc<dyn ReplicaTransport>,
    ) -> Arc<Self> {
        Arc::new(Self {
            replica,
            authority,
            peers,
            progress: Mutex::new(BTreeMap::new()),
            copy_slots: Semaphore::new(MAX_PARALLEL_QUEUE_COPIES),
        })
    }
    fn gate(&self, id: Uuid) -> Result<Arc<AsyncMutex<()>>, ProtocolError> {
        let mut gates = self
            .progress
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?;
        gates.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = gates.get(&id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(id, Arc::downgrade(&gate));
        Ok(gate)
    }
    async fn current(&self) -> Result<QueueAuthority, ProtocolError> {
        let scope = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .scope()
            .clone();
        self.authority.current(&scope).await
    }

    pub(super) async fn owner_record_state(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<QueueState, ProtocolError> {
        let scope = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .scope()
            .clone();
        self.peers
            .state(current.owner, scope, current.ready.view, id)
            .await
    }

    /// Transfer a live queue with bounded pages, then obtain a transport-bound
    /// completion statement. Publishing readiness additionally requires the
    /// timestamp release-revision fence recorded by Control.
    pub(crate) async fn copy_join(
        &self,
        ticket: crate::oltp_replication::types::JoinTicket,
        peer: &QueuePeerClient,
    ) -> Result<crate::oltp_replication::authority::control::VerifiedQueueCopy, ProtocolError> {
        let gate = self.gate(ticket.ticket_id)?;
        let _copy = gate.lock().await;
        let current = self.current().await?;
        if current.ready != ticket.source_ready {
            return Err(QueueError::Stale.protocol());
        }
        let (snapshot, scope) = {
            let mut source = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            let snapshot = source
                .begin_join(current, ticket)
                .map_err(QueueError::protocol)?;
            (snapshot, source.scope().clone())
        };
        tracing::debug!(ticket = %ticket.ticket_id, "queue join export opened");
        for phase in [JoinPhase::Snapshot, JoinPhase::Delta] {
            if phase == JoinPhase::Delta {
                self.replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?
                    .seal_join(current, snapshot)
                    .map_err(QueueError::protocol)?;
            }
            loop {
                let page = {
                    let source = self
                        .replica
                        .lock()
                        .map_err(|_| QueueError::Unavailable.protocol())?;
                    let (after, done) = source
                        .join_export_progress(current, snapshot, phase)
                        .map_err(QueueError::protocol)?;
                    if done {
                        break;
                    }
                    source
                        .join_page(current, snapshot, phase, after)
                        .map_err(QueueError::protocol)?
                };
                tracing::debug!(ticket = %ticket.ticket_id, ?phase, after = ?page.after_sequence, done = page.done, "queue join page send");
                peer.install_join_page(ticket.target, &page).await?;
                tracing::debug!(ticket = %ticket.ticket_id, ?phase, "queue join page acknowledged");
                self.replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?
                    .acknowledge_join_page(current, page.header())
                    .map_err(QueueError::protocol)?;
            }
        }
        let expected = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .join_statement(current)
            .map_err(QueueError::protocol)?;
        let observed = peer.join_statement(ticket.target, scope, &expected).await?;
        tracing::debug!(ticket = %ticket.ticket_id, "queue join target completion observed");
        if self.current().await? != current {
            return Err(QueueError::Stale.protocol());
        }
        let source = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?;
        crate::oltp_replication::authority::control::VerifiedQueueCopy::from_join(
            &source, current, observed,
        )
    }

    /// Returns only after both the payload and acceptance evidence reach every
    /// current ready holder. Cancelling the observer leaves RAM copies intact.
    pub async fn admit(
        &self,
        body: Vec<u8>,
        context: AcceptedContext,
        now_ms: u64,
    ) -> Result<RecordVersion, ProtocolError> {
        let current = self.current().await?;
        let frame = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .propose(current, body, context, now_ms)
            .map_err(QueueError::protocol)?;
        let id = frame
            .decode_header()
            .map_err(QueueError::protocol)?
            .request_id;
        drop(frame);
        self.ensure_copies(current, id).await
    }

    pub async fn ensure_copies(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<RecordVersion, ProtocolError> {
        self.ensure_phase(current, id, true).await
    }
    pub(crate) async fn stage(
        &self,
        current: QueueAuthority,
        body: Vec<u8>,
        context: AcceptedContext,
        now_ms: u64,
    ) -> Result<RecordVersion, ProtocolError> {
        #[cfg(test)]
        if self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .authority()
            .view_is_behind(current.ready.view)
        {
            super::recovery_tests::observe_o21_stale_proposal(&body);
        }
        // The captured snapshot names Control's published view. Wait for the
        // local queue to install it before any copy phase can start.
        wait_for_local_ready_view(&self.replica, current)
            .await
            .map_err(|error| {
                // submit_original still proves absence under its UUID gate
                // and seals the rejection in Status before exposing this.
                let mut error = error.protocol();
                error.outcome = Outcome::NotApplied;
                error
            })?;
        if self.current().await? != current {
            return Err(QueueError::Stale.protocol());
        }
        let frame = self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .propose(current, body, context, now_ms)
            .map_err(QueueError::protocol)?;
        let id = frame
            .record_version()
            .map_err(QueueError::protocol)?
            .request_id;
        drop(frame);
        #[cfg(test)]
        if super::recovery_tests::refuse_staged_copy(id).await {
            let mut error = QueueError::Overloaded.protocol();
            error.outcome = Outcome::Unknown;
            return Err(error);
        }
        self.ensure_staged(current, id).await
    }
    pub(crate) async fn ensure_staged(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<RecordVersion, ProtocolError> {
        self.ensure_phase(current, id, false).await
    }
    pub(crate) async fn accept_staged(
        &self,
        current: QueueAuthority,
        id: Uuid,
    ) -> Result<RecordVersion, ProtocolError> {
        if self.current().await? != current {
            return Err(QueueError::Stale.protocol());
        }
        self.replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .accept_staged(current, id)
            .map_err(QueueError::protocol)?;
        self.ensure_copies(current, id).await
    }
    async fn ensure_phase(
        &self,
        current: QueueAuthority,
        id: Uuid,
        promote: bool,
    ) -> Result<RecordVersion, ProtocolError> {
        let gate = self.gate(id)?;
        let _progress = gate.lock().await;
        loop {
            let (frame, local, targets) = {
                let mut replica = self
                    .replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?;
                if replica.authority() != current {
                    return Err(QueueError::Stale.protocol());
                }
                if if promote {
                    replica.copies_complete(id)
                } else {
                    replica.payloads_complete(id)
                } {
                    (
                        replica.frame(id).map_err(QueueError::protocol)?,
                        replica.local_identity(),
                        None,
                    )
                } else {
                    let frame = replica.resume(current, id).map_err(QueueError::protocol)?;
                    let targets = replica
                        .next_copy_batch(id, MAX_PARALLEL_QUEUE_COPIES)
                        .map_err(QueueError::protocol)?;
                    (frame, replica.local_identity(), Some(targets))
                }
            };
            let Some(targets) = targets else {
                if self.current().await? != current {
                    return Err(QueueError::Stale.protocol());
                }
                let version = frame.record_version().map_err(QueueError::protocol)?;
                // The result section is present only on terminal frames.
                if promote && frame.sections[2].is_some() {
                    self.replica
                        .lock()
                        .map_err(|_| QueueError::Unavailable.protocol())?
                        .confirm_terminal_copy(current, version)
                        .map_err(QueueError::protocol)?;
                }
                #[cfg(test)]
                super::recovery_tests::observe_load_copy(id, &frame, None);
                return Ok(version);
            };
            // All sends borrow the same charged frame. Apply their authenticated
            // replies in ready-set order, regardless of which send finishes first.
            let version = frame.record_version().map_err(QueueError::protocol)?;
            let mut copies = FuturesOrdered::new();
            for target in targets {
                let frame = &frame;
                copies.push_back(async move {
                    #[cfg(test)]
                    let started = nabedra_rt::time::now_std();
                    let ack = if target == local {
                        CopyAcknowledgement::authenticated(local, version)
                    } else {
                        let deadline = nabedra_rt::time::now() + QUEUE_COPY_SLOT_TIMEOUT;
                        let _slot = timeout_at(deadline, self.copy_slots.acquire())
                            .await
                            .map_err(|_| QueueError::Unavailable.protocol())?
                            .map_err(|_| QueueError::Unavailable.protocol())?;
                        self.peers
                            .install_version(target, frame, version)
                            .await
                            .map_err(|mut error| {
                                // This send cannot establish that no peer
                                // accepted or applied the logical request.
                                error.outcome = Outcome::Unknown;
                                error
                            })?
                    };
                    #[cfg(test)]
                    let timing = (target, nabedra_rt::time::elapsed(started));
                    #[cfg(not(test))]
                    let timing = ();
                    Ok::<_, ProtocolError>((ack, timing))
                });
            }
            while let Some(copy) = copies.next().await {
                let (ack, _timing) = copy?;
                self.replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?
                    .acknowledge_phase(current, ack, promote)
                    .map_err(QueueError::protocol)?;
                #[cfg(test)]
                super::recovery_tests::observe_load_copy(id, &frame, Some(_timing));
            }
        }
    }

    /// The backend may dispatch only after the pinned Running version itself
    /// reaches every ready holder. Reconciliation retains the original token.
    pub async fn start(&self, id: Uuid) -> Result<RecordVersion, ProtocolError> {
        let current = self.current().await?;
        self.replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .start(current, id)
            .map_err(QueueError::protocol)?;
        self.ensure_copies(current, id).await
    }

    /// A ready-set change can rebase a live read into Reconciling while its
    /// original planner and reducer still own the next cursor on this process.
    /// Re-copy the same record before accepting another grant at that cursor.
    pub(crate) async fn resume_read_continuation(
        &self,
        scope: ReadScope,
    ) -> Result<(), ProtocolError> {
        let id = scope.request.request_id;
        if self
            .replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .read_continuation_copied(scope)
        {
            return Ok(());
        }
        let current = self.current().await?;
        let grants = {
            let replica = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            let state = replica
                .check_read_continuation(current, scope)
                .map_err(QueueError::protocol)?;
            if replica.read_continuation_copied(scope) {
                return Ok(());
            }
            if state == RequestState::Reconciling {
                replica.pending_read_grants(id)
            } else {
                Vec::new()
            }
        };
        for grant in grants {
            self.settle_read_grant(grant).await?;
        }
        self.ensure_copies(current, id).await?;
        {
            let mut replica = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            replica
                .check_read_continuation(current, scope)
                .map_err(QueueError::protocol)?;
            if replica.read_continuation_copied(scope) {
                return Ok(());
            }
            replica.start(current, id).map_err(QueueError::protocol)?;
        }
        self.ensure_copies(current, id).await?;
        Ok(())
    }

    pub(crate) async fn open_read_grant(&self, grant: ReadGrant) -> Result<(), ProtocolError> {
        self.change_read_grant(grant, true).await
    }

    pub(crate) async fn finish_read_grant(&self, grant: ReadGrant) -> Result<(), ProtocolError> {
        self.change_read_grant(grant, false).await
    }

    async fn change_read_grant(&self, grant: ReadGrant, add: bool) -> Result<(), ProtocolError> {
        let id = grant.scope.request.request_id;
        let gate = self.gate(id)?;
        let _progress = gate.lock().await;
        let current = self.current().await?;
        let set = self.replica.lock().map_err(|_| QueueError::Unavailable.protocol())?
            .update_read_grant(current, grant, add).map_err(QueueError::protocol)?;
        if let Some(set) = set {
            self.replicate_read_grants(current, id, set).await?;
        }
        Ok(())
    }

    async fn replicate_read_grants(
        &self,
        current: QueueAuthority,
        id: Uuid,
        set: ReadGrantSet,
    ) -> Result<(), ProtocolError> {
        // One exchange's normal transport deadline bounds the whole ready-set
        // walk, including retries and fresh Control checks.
        let deadline = nabedra_rt::time::now() + READ_GRANT_COPY_DEADLINE;
        let mut after = None;
        let mut delay = READ_GRANT_COPY_BACKOFF;
        let mut possibly_copied = false;
        loop {
            let (target, scope) = {
                let replica = self
                    .replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?;
                if replica.authority() != current
                    || replica.local_identity() != current.owner
                    || replica.pending_read_grant_copy(id).as_ref() != Some(&set)
                {
                    return Err(QueueError::Stale.protocol());
                }
                (replica.member_after(after), replica.scope().clone())
            };
            let Some(target) = target else { break };
            if target != current.owner {
                if nabedra_rt::time::now() >= deadline {
                    return Err(QueueError::Unavailable.protocol());
                }
                let reply = timeout_at(
                    deadline,
                    self.peers
                        .copy_read_grants(target, scope, current.ready.view, id, set.clone()),
                )
                .await
                .map_err(|_| QueueError::Unavailable.protocol())?;
                if let Err(mut error) = reply {
                    if !matches!(
                        error.code,
                        ErrorCode::Overloaded | ErrorCode::OutcomeUnknown
                    ) {
                        return Err(error);
                    }
                    possibly_copied |= error.outcome == Outcome::Unknown;
                    if possibly_copied {
                        error.outcome = Outcome::Unknown;
                    }
                    tracing::debug!(%id, worker=%target.worker_id, revision=set.revision,
                        code=?error.code, outcome=?error.outcome, message=%error.message,
                        "read grant copy awaits retry");
                    let retry_at = nabedra_rt::time::now() + delay;
                    if retry_at >= deadline {
                        return Err(error);
                    }
                    sleep_until(retry_at).await;
                    let fresh = match timeout_at(deadline, self.current()).await {
                        Ok(Ok(fresh)) => fresh,
                        Ok(Err(mut failure)) => {
                            if possibly_copied {
                                failure.outcome = Outcome::Unknown;
                            }
                            return Err(failure);
                        }
                        Err(_) => return Err(error),
                    };
                    if fresh != current {
                        return Err(QueueError::Stale.protocol());
                    }
                    if nabedra_rt::time::now() >= deadline {
                        return Err(error);
                    }
                    // Leave the cursor at the last acknowledged worker. The
                    // next request carries the same set under a new TLS nonce.
                    delay = (delay * 2).min(Duration::from_millis(250));
                    continue;
                }
            }
            after = Some(target.worker_id);
            delay = READ_GRANT_COPY_BACKOFF;
            possibly_copied = false;
        }
        if timeout_at(deadline, self.current())
            .await
            .map_err(|_| QueueError::Unavailable.protocol())??
            != current
        {
            return Err(QueueError::Stale.protocol());
        }
        self.replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .complete_read_grant_copy(current, id, &set)
            .map_err(QueueError::protocol)
    }

    pub(crate) async fn settle_read_grant(&self, grant: ReadGrant) -> Result<(), ProtocolError> {
        let current = self.current().await?;
        let (scope, shared) = {
            let replica = self.replica.lock().map_err(|_| QueueError::Unavailable.protocol())?;
            if replica.authority() != current
                || replica.local_identity() != current.owner
                || grant.scope.request.run_id != current.ready.view.run_id
            {
                return Err(QueueError::Stale.protocol());
            }
            (replica.scope().clone(), replica.shared_resources())
        };
        let settled = if current.owner == grant.worker {
            shared.read_attempts.settle(grant)
        } else {
            nabedra_rt::time::timeout(std::time::Duration::from_secs(2),
                self.peers.settle_read_grant(scope, current.ready.view, grant),
            ).await.unwrap_or_else(|_| Err(QueueError::Unavailable.protocol()))
        };
        if let Err(error) = settled {
            // Only a committed proof for this exact process can substitute
            // for the receiver's authenticated confirmation.
            if !matches!(nabedra_rt::time::timeout(std::time::Duration::from_secs(2),
                self.authority.excludes_process(grant.worker),
            ).await, Ok(Ok(true))) {
                return Err(error);
            }
        }
        self.finish_read_grant(grant).await
    }

    pub(crate) async fn settle_request_read_grants(&self, id: Uuid) -> Result<(), ProtocolError> {
        let grants = self.replica.lock().map_err(|_| QueueError::Unavailable.protocol())?
            .pending_read_grants(id);
        for grant in grants {
            #[cfg(test)]
            crate::oltp_replication::service_rpc::read_fixture::record(
                grant.scope, grant.group, "owner-recovering", grant.ticket, grant.worker,
            );
            self.settle_read_grant(grant).await?;
            #[cfg(test)]
            crate::oltp_replication::service_rpc::read_fixture::record(
                grant.scope, grant.group, "owner-recovered", grant.ticket, grant.worker,
            );
        }
        let gate = self.gate(id)?;
        let _progress = gate.lock().await;
        let set = self.replica.lock().map_err(|_| QueueError::Unavailable.protocol())?
            .pending_read_grant_copy(id);
        if let Some(set) = set {
            let current = self.current().await?;
            self.replicate_read_grants(current, id, set).await?;
        }
        Ok(())
    }

    pub async fn finish(
        &self,
        id: Uuid,
        status: u16,
        body: Vec<u8>,
        now_ms: u64,
    ) -> Result<RecordVersion, ProtocolError> {
        let current = self.current().await?;
        let (input, shared) = {
            let replica = self
                .replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?;
            (
                replica.terminal_input(id).map_err(QueueError::protocol)?,
                replica.shared_resources(),
            )
        };
        let bytes = (body.len() + super::replica::MAX_CONTEXT_BYTES + 4096) as u64;
        let prepared = shared.prepare(ExecutionClass::Query).await?;
        let grant = shared
            .scheduler()
            .reserve_on(
                u64::from_le_bytes(id.as_bytes()[..8].try_into().expect("UUID prefix")),
                1,
                shared.scheduler_node_id(),
                bytes,
            )
            .await?;
        let (terminal, grant) = prepared
            .run_with_guard(grant, move || {
                PreparedTerminal::prepare(input, status, body, now_ms).map_err(QueueError::protocol)
            })
            .await?;
        self.replica
            .lock()
            .map_err(|_| QueueError::Unavailable.protocol())?
            .finish_prepared(current, terminal?)
            .map_err(QueueError::protocol)?;
        drop(grant);
        self.ensure_copies(current, id).await
    }
}
