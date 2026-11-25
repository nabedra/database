//! Definitive admission rejection stays tied to the live UUID guard. Durable
//! metadata cannot claim that no RAM copy exists without asking that owner.
use super::{replica::*, worker_endpoint::AuthorityReader, worker_runtime::VerifiedNeverAccepted};
use crate::{
    oltp::pool::{ByteClass, ScratchLease, WorkerSharedResources},
    oltp_replication::{
        authority::{
            consensus::ConsensusFuture,
            control::ControlHandle,
            status::{MutationOutcome, RejectedIdentity},
        },
        queue_access::NeverAcceptedAdmission,
        service_rpc::{AdmissionTicketIssuer, OwnerAdmissionTicket},
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;

pub(crate) struct LiveAdmission {
    ticket: OwnerAdmissionTicket,
    _gate: Arc<tokio::sync::OwnedMutexGuard<()>>,
    _memory: ScratchLease,
}
pub(crate) struct AdmissionTickets {
    shared: Arc<WorkerSharedResources>,
    local: WorkerIdentity,
    scope: QueueScope,
    control: Arc<ControlHandle>,
    replica: Arc<Mutex<WorkerReplica>>,
    records: Mutex<BTreeMap<Uuid, Weak<LiveAdmission>>>,
}
impl AdmissionTickets {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        local: WorkerIdentity,
        scope: QueueScope,
        control: Arc<ControlHandle>,
        replica: Arc<Mutex<WorkerReplica>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            local,
            scope,
            control,
            replica,
            records: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) async fn verify(
        &self,
        reader: WorkerIdentity,
        ticket: &OwnerAdmissionTicket,
    ) -> Result<(Uuid, RejectedIdentity), ProtocolError> {
        let current = self.control.current(&self.scope).await?;
        let grant = self
            .control
            .service_role(
                &self.scope,
                reader,
                crate::oltp_replication::authority::control::ServiceAction::StatusProof {
                    source_group: ticket.status_group.group_id,
                },
                ticket.status_group.clone(),
            )
            .await?;
        if grant.ready != current.ready || grant.owner != Some(current.owner) {
            return Err(invalid());
        }
        self.verify_current(current, ticket)
    }
    pub(crate) fn verify_current(
        &self,
        current: QueueAuthority,
        ticket: &OwnerAdmissionTicket,
    ) -> Result<(Uuid, RejectedIdentity), ProtocolError> {
        if current.owner != self.local
            || ticket.owner != self.local
            || ticket.expected.view != current.ready.view
            || ticket.status_group.cluster_id != self.scope.cluster_id
            || ticket.status_group.database != self.scope.database
            || !matches!(ticket.status_group.kind, GroupKind::Status { .. })
        {
            return Err(invalid());
        }
        let entry = self
            .records
            .lock()
            .map_err(|_| invalid())?
            .get(&ticket.nonce)
            .and_then(Weak::upgrade)
            .ok_or_else(invalid)?;
        if entry.ticket.nonce != ticket.nonce
            || entry.ticket.expected != ticket.expected
            || entry.ticket.status_group != ticket.status_group
            || self
                .replica
                .lock()
                .map_err(|_| invalid())?
                .record(ticket.expected.reservation.request_id)
                .is_some()
        {
            return Err(invalid());
        }
        Ok((ticket.nonce, ticket.expected.clone()))
    }
}
impl AdmissionTicketIssuer for AdmissionTickets {
    fn issue<'a>(
        &'a self,
        cap: &'a dyn NeverAcceptedAdmission,
        status: &'a GroupIdentity,
    ) -> ConsensusFuture<'a, OwnerAdmissionTicket> {
        Box::pin(async move {
            let cap = (cap as &dyn std::any::Any)
                .downcast_ref::<VerifiedNeverAccepted>()
                .ok_or_else(invalid)?;
            let current = self.control.current(&self.scope).await?;
            if cap.scope() != &self.scope
                || current.owner != self.local
                || cap.view() != current.ready.view
                || status.cluster_id != self.scope.cluster_id
                || status.database != self.scope.database
                || !matches!(status.kind, GroupKind::Status { .. })
                || self
                    .replica
                    .lock()
                    .map_err(|_| invalid())?
                    .record(cap.reservation().request_id)
                    .is_some()
            {
                return Err(invalid());
            }
            if let Some(entry) = cap.live_admission.get() {
                if entry.ticket.status_group != *status {
                    return Err(invalid());
                }
                return Ok(entry.ticket.clone());
            }
            let expected = RejectedIdentity {
                reservation: cap.reservation(),
                view: cap.view(),
                identity: cap.identity().clone(),
                pin: cap.pin(),
                request: cap.request(),
                failure: MutationOutcome::failure(cap.failure().clone())?,
                recorded_at: AppliedPosition {
                    term: 0,
                    node_id: 0,
                    index: 0,
                },
            };
            // The nonce binds the remote proof to this live guard. Keep OS randomness in simulation.
            #[allow(clippy::disallowed_methods)]
            let nonce = Uuid::new_v4();
            let ticket = OwnerAdmissionTicket {
                owner: self.local,
                nonce,
                status_group: status.clone(),
                expected,
            };
            let bytes = serde_json::to_vec(&ticket)
                .map_err(|_| invalid())?
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(invalid)?;
            let entry = Arc::new(LiveAdmission {
                ticket: ticket.clone(),
                _gate: cap.admission_gate.clone(),
                _memory: self.shared.reserve_scratch(ByteClass::Progress, bytes)?,
            });
            cap.live_admission
                .set(entry.clone())
                .map_err(|_| invalid())?;
            let mut records = self.records.lock().map_err(|_| invalid())?;
            records.retain(|_, entry| entry.strong_count() != 0);
            records.insert(ticket.nonce, Arc::downgrade(&entry));
            Ok(ticket)
        })
    }
}
#[track_caller]
fn invalid() -> ProtocolError {
    nabedra_rt::diag_at("invalid", std::panic::Location::caller());
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "original live admission guard is unavailable",
        Outcome::Unknown,
    )
}
