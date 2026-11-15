//! Replay evidence for one immutable receiver boot. Freshness is caller supplied.
//! Runtime must embed this with live barrier/permits/capacity and the accepted
//! owner registry under ONE short mutex. All fallible reservations precede
//! consume; owner installation afterward must be synchronous and infallible.
use super::{
    Error, Role, Sender,
    verify::{
        self, CheckedContent, CombinedRequest, EligibleRequest, ReceiverIdentity, RequestHandoff,
    },
};
use crate::cluster::controller::OwnershipObservation;
use std::sync::Arc;

const WORKERS: usize = 2048;
const APIS: usize = 128;
/// Fixed per-sender storage permits legitimate concurrent HTTP reordering.
const SEQUENCE_WINDOW: usize = 4096;
const CLOSED_PERMITS: usize = 4096;
#[derive(Clone, PartialEq, Eq)]
struct Identity {
    role: Role,
    process: uuid::Uuid,
    node: Option<uuid::Uuid>,
    incarnation: Option<uuid::Uuid>,
    session: Option<uuid::Uuid>,
    key: [u8; 32],
}
impl Identity {
    fn from_sender(sender: &Sender) -> Result<Self, Error> {
        Ok(Self {
            role: sender.role(),
            process: sender.process_id(),
            node: sender.node_id(),
            incarnation: sender.incarnation(),
            session: sender.session_id(),
            key: sender.public_key()?,
        })
    }
}
struct Entry {
    identity: Identity,
    highest: u64,
    seen: [u64; SEQUENCE_WINDOW / 64],
}
impl Entry {
    fn floor(&self) -> u64 {
        self.highest
            .saturating_sub((SEQUENCE_WINDOW - 1) as u64)
            .max(1)
    }
    fn consume(&mut self, sequence: u64) -> Result<(), Error> {
        if sequence == 0 || sequence < self.floor() {
            return Err(Error::Replay);
        }
        if sequence > self.highest {
            let advance = sequence - self.highest;
            if advance >= SEQUENCE_WINDOW as u64 {
                self.seen.fill(0);
            } else {
                for step in 1..=advance {
                    let bit = ((self.highest + step) % SEQUENCE_WINDOW as u64) as usize;
                    self.seen[bit / 64] &= !(1u64 << (bit % 64));
                }
            }
            self.highest = sequence;
        }
        let bit = (sequence % SEQUENCE_WINDOW as u64) as usize;
        let mask = 1u64 << (bit % 64);
        if self.seen[bit / 64] & mask != 0 {
            return Err(Error::Replay);
        }
        self.seen[bit / 64] |= mask;
        Ok(())
    }
}
struct ClosedPermit {
    identity: Identity,
    permit: uuid::Uuid,
    sequence: u64,
}
/// Prepared from one validated authoritative control root outside the receiver
/// mutex, then compared to the exact observation accepted under that mutex.
pub(crate) struct ReplayMembership {
    observation: OwnershipObservation,
    workers: Vec<(uuid::Uuid, uuid::Uuid, uuid::Uuid)>,
}
impl ReplayMembership {
    pub(crate) fn prepare(root: &crate::cluster::state::Control) -> Result<Self, Error> {
        use crate::cluster::member::NodeState;
        let observation = OwnershipObservation::new(root).map_err(|_| Error::Invalid)?;
        let workers = root
            .nodes
            .iter()
            .filter(|node| !matches!(node.state, NodeState::Lost | NodeState::Stopped))
            .filter_map(|node| {
                let incarnation = node.incarnation?;
                let session = node.session.as_ref()?.id;
                if node.retired_incarnations.contains(&incarnation) {
                    None
                } else {
                    Some((node.node_id, incarnation, session))
                }
            })
            .collect();
        Ok(Self {
            observation,
            workers,
        })
    }
}
/// Capacity is checked before replay consumption; commit cannot allocate or fail.
pub(crate) struct PreparedClose {
    identity: Identity,
    permit: uuid::Uuid,
    sequence: u64,
}
#[allow(dead_code)] // Pending receiver runtime owner.
pub(crate) struct ReplayState {
    local: Arc<ReceiverIdentity>,
    observation: Option<OwnershipObservation>,
    failed: bool,
    workers: Vec<Entry>,
    apis: Vec<Entry>,
    closed_permits: Vec<ClosedPermit>,
}
impl ReplayState {
    #[allow(dead_code)] // Pending receiver runtime construction.
    pub(crate) fn new(local: ReceiverIdentity) -> Self {
        Self::for_receiver(Arc::new(local))
    }
    pub(crate) fn for_receiver(local: Arc<ReceiverIdentity>) -> Self {
        Self {
            local,
            observation: None,
            failed: false,
            workers: Vec::with_capacity(WORKERS),
            apis: Vec::with_capacity(APIS),
            closed_permits: Vec::with_capacity(CLOSED_PERMITS),
        }
    }
    /// No storage access or freshness assertion. Divergence permanently fails closed.
    #[allow(dead_code)] // Pending receiver runtime observations.
    pub(crate) fn observe(&mut self, root: &OwnershipObservation) -> Result<(), Error> {
        if let Some(old) = &self.observation {
            if root.revision() < old.revision() {
                return Err(Error::Invalid);
            }
            if root.revision() == old.revision() {
                if root != old {
                    self.failed = true;
                    return Err(Error::Invalid);
                }
                return Ok(());
            }
        }
        self.observation = Some(root.clone());
        Ok(())
    }
    pub(crate) fn observe_membership(&mut self, membership: ReplayMembership) -> Result<(), Error> {
        if self.failed || self.observation.as_ref() != Some(&membership.observation) {
            return Err(Error::Invalid);
        }
        self.closed_permits.retain(|closed| {
            closed.identity.role != Role::Worker
                || closed
                    .identity
                    .node
                    .zip(closed.identity.incarnation)
                    .zip(closed.identity.session)
                    .is_some_and(|((node, incarnation), session)| {
                        membership.workers.contains(&(node, incarnation, session))
                    })
        });
        Ok(())
    }
    fn collect_closed_permits(&mut self) {
        let workers = &self.workers;
        let apis = &self.apis;
        self.closed_permits.retain(|closed| {
            let entries = match closed.identity.role {
                Role::Worker => workers,
                Role::Api => apis,
            };
            // Retain on unknown identity. Only a proven replay floor permits GC.
            entries
                .iter()
                .find(|entry| entry.identity == closed.identity)
                .is_none_or(|entry| entry.floor() <= closed.sequence)
        });
    }
    pub(crate) fn check_dependency_open(
        &mut self,
        sender: &Sender,
        permit: uuid::Uuid,
    ) -> Result<(), Error> {
        self.collect_closed_permits();
        let identity = Identity::from_sender(sender)?;
        if self
            .closed_permits
            .iter()
            .any(|closed| closed.identity == identity && closed.permit == permit)
        {
            return Err(Error::Replay);
        }
        Ok(())
    }
    pub(crate) fn prepare_dependency_close(
        &mut self,
        sender: &Sender,
        permit: uuid::Uuid,
        sequence: u64,
    ) -> Result<PreparedClose, Error> {
        self.collect_closed_permits();
        let identity = Identity::from_sender(sender)?;
        if self.closed_permits.len() == CLOSED_PERMITS
            && !self
                .closed_permits
                .iter()
                .any(|closed| closed.identity == identity && closed.permit == permit)
        {
            return Err(Error::Exhausted);
        }
        Ok(PreparedClose {
            identity,
            permit,
            sequence,
        })
    }
    pub(crate) fn commit_dependency_close(&mut self, prepared: PreparedClose) {
        if let Some(closed) = self
            .closed_permits
            .iter_mut()
            .find(|closed| closed.identity == prepared.identity && closed.permit == prepared.permit)
        {
            closed.sequence = closed.sequence.max(prepared.sequence);
        } else {
            // Prepared under the same receiver mutex; successful consumption can
            // only free tombstones, never use a slot reserved by this close.
            debug_assert!(self.closed_permits.len() < CLOSED_PERMITS);
            self.closed_permits.push(ClosedPermit {
                identity: prepared.identity,
                permit: prepared.permit,
                sequence: prepared.sequence,
            });
        }
    }
    #[allow(dead_code)] // Pending final receiver transaction.
    pub(crate) fn consume(
        &mut self,
        eligible: EligibleRequest,
        content: CheckedContent,
    ) -> Result<ReplayedRequest, Error> {
        let combined = verify::combine_request(eligible, content)?;
        if self.failed
            || self.observation.as_ref() != Some(combined.observation())
            || !self.local.same_boot(combined.local())
            || !self.local.matches_auth(combined.auth())
        {
            return Err(Error::Invalid);
        }
        let auth = combined.auth();
        let sequence = auth.sequence();
        if sequence == 0 {
            return Err(Error::Invalid);
        }
        let identity = Identity::from_sender(auth.sender())?;
        let (entries, cap) = match auth.sender().role() {
            Role::Worker => (&mut self.workers, WORKERS),
            Role::Api => (&mut self.apis, APIS),
        };
        if let Some(entry) = entries.iter_mut().find(|e| e.identity == identity) {
            entry.consume(sequence)?;
        } else {
            if entries.len() == cap {
                return Err(Error::Exhausted);
            }
            let mut entry = Entry {
                identity,
                highest: 0,
                seen: [0; SEQUENCE_WINDOW / 64],
            };
            entry.consume(sequence)?;
            entries.push(entry);
        }
        self.collect_closed_permits();
        Ok(ReplayedRequest { combined })
    }
}
/// Spent replay evidence. Dropping this or its handoff never rolls back replay.
#[allow(dead_code)] // Pending semantic/runtime transfer.
pub(crate) struct ReplayedRequest {
    combined: CombinedRequest,
}
impl ReplayedRequest {
    #[allow(dead_code)] // Pending semantic/runtime transfer.
    pub(crate) fn into_handoff(self) -> RequestHandoff {
        self.combined.into_handoff()
    }
}
impl std::fmt::Debug for ReplayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplayState")
    }
}
impl std::fmt::Debug for ReplayedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplayedRequest")
    }
}
#[cfg(test)]
mod tests;
