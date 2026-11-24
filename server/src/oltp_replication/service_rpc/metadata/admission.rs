//! Owner-RAM proof for a definitive admission rejection. The ticket contains no
//! executable body; the owner retains the real UUID gate until Status settles.
use super::*;
use crate::oltp_replication::authority::consensus::ConsensusFuture;
use crate::oltp_runtime::worker_runtime::VerifiedNeverAccepted;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerAdmissionTicket {
    pub owner: WorkerIdentity,
    pub nonce: Uuid,
    pub status_group: GroupIdentity,
    pub expected: RejectedIdentity,
}
impl OwnerAdmissionTicket {
    pub(crate) fn matches(&self, cap: &VerifiedNeverAccepted, group: &GroupIdentity) -> bool {
        self.nonce != Uuid::nil()
            && self.status_group == *group
            && self.expected.reservation == cap.reservation()
            && self.expected.view == cap.view()
            && self.expected.identity == *cap.identity()
            && self.expected.pin == cap.pin()
            && self.expected.request == cap.request()
            && self.expected.failure.result().err().as_ref() == Some(cap.failure())
    }
}
/// Implemented by the owner registry. It registers only a live, owned gate.
pub(crate) trait AdmissionTicketIssuer: Send + Sync {
    fn issue<'a>(
        &'a self,
        cap: &'a VerifiedNeverAccepted,
        status: &'a GroupIdentity,
    ) -> ConsensusFuture<'a, OwnerAdmissionTicket>;
}
/// Implemented by the Status leader's authenticated QueueControl client.
/// It never accepts an observation supplied by an ordinary service caller.
pub(crate) trait OwnerAdmissionReader: Send + Sync {
    fn verify<'a>(
        &'a self,
        ticket: &'a OwnerAdmissionTicket,
    ) -> ConsensusFuture<'a, OwnerAdmissionObservation>;
}
pub(crate) struct OwnerAdmissionObservation {
    ticket: OwnerAdmissionTicket,
    _owner: Arc<ScratchLease>,
}
impl OwnerAdmissionObservation {
    /// Call on the charged native metadata codec after TLS, reply-prefix and
    /// current-owner checks. The exact ticket nonce is echoed in section zero.
    pub(crate) fn from_queue_reply(
        reply: ReceivedReply,
        ticket: &OwnerAdmissionTicket,
    ) -> Result<Self, ProtocolError> {
        if reply.remote_identity() != ticket.owner
            || reply.purpose() != Purpose::QueueControl
            || reply.group().cluster_id != ticket.status_group.cluster_id
            || reply.group().database != ticket.status_group.database
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid());
        }
        let observed = reply.decode_section::<(Uuid, RejectedIdentity)>(0)?;
        if observed.0 != ticket.nonce || observed.1 != ticket.expected {
            return Err(invalid());
        }
        Ok(Self {
            ticket: ticket.clone(),
            _owner: reply.scratch_lease(),
        })
    }
    pub(crate) fn status_group(&self) -> &GroupIdentity {
        &self.ticket.status_group
    }
    pub(crate) fn expected(&self) -> &RejectedIdentity {
        &self.ticket.expected
    }
    pub(crate) fn owner(&self) -> WorkerIdentity {
        self.ticket.owner
    }
}
