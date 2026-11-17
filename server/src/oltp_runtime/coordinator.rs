//! Owner-side RAM copy barriers. Data execution is deliberately a separate
//! backend: this component never proposes application bodies to a Raft group.
use super::{
    replica::*,
    worker_endpoint::{AuthorityReader, QueuePeerClient},
};
use crate::{
    oltp::pool::ExecutionClass,
    oltp_replication::{multiplex::EndpointFuture, types::WorkerIdentity},
    protocol::ProtocolError,
    request_queue::QueueError,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

pub trait ReplicaTransport: Send + Sync + 'static {
    fn install<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
    ) -> EndpointFuture<'a, CopyAcknowledgement>;
}
impl ReplicaTransport for QueuePeerClient {
    fn install<'a>(
        &'a self,
        target: WorkerIdentity,
        frame: &'a RecordFrame,
    ) -> EndpointFuture<'a, CopyAcknowledgement> {
        Box::pin(QueuePeerClient::install(self, target, frame))
    }
}

pub struct QueueCoordinator {
    replica: Arc<Mutex<WorkerReplica>>,
    authority: Arc<dyn AuthorityReader>,
    peers: Arc<dyn ReplicaTransport>,
    progress: Mutex<BTreeMap<Uuid, Weak<AsyncMutex<()>>>>,
}
impl QueueCoordinator {
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
        let gate = self.gate(id)?;
        let _progress = gate.lock().await;
        loop {
            let (frame, local, target) = {
                let mut replica = self
                    .replica
                    .lock()
                    .map_err(|_| QueueError::Unavailable.protocol())?;
                if replica.authority() != current {
                    return Err(QueueError::Stale.protocol());
                }
                if replica.copies_complete(id) {
                    (
                        replica.frame(id).map_err(QueueError::protocol)?,
                        replica.local_identity(),
                        None,
                    )
                } else {
                    let frame = replica.resume(current, id).map_err(QueueError::protocol)?;
                    let target = replica.next_copy_member(id).map_err(QueueError::protocol)?;
                    (frame, replica.local_identity(), Some(target))
                }
            };
            let Some(target) = target else {
                if self.current().await? != current {
                    return Err(QueueError::Stale.protocol());
                }
                return frame.record_version().map_err(QueueError::protocol);
            };
            // No queue lock, compute grant or native execution slot crosses
            // this network wait. The exact raw frame retains only byte charges.
            let ack = if target == local {
                CopyAcknowledgement::authenticated(
                    local,
                    frame.record_version().map_err(QueueError::protocol)?,
                )
            } else {
                self.peers
                    .install(target, &frame)
                    .await
                    .map_err(|mut error| {
                        // This RPC describes one RAM installation, not proof that
                        // the logical request was never accepted/applied elsewhere.
                        error.outcome = crate::protocol::Outcome::Unknown;
                        error
                    })?
            };
            self.replica
                .lock()
                .map_err(|_| QueueError::Unavailable.protocol())?
                .acknowledge(current, ack)
                .map_err(QueueError::protocol)?;
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
