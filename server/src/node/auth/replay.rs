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
#[derive(PartialEq, Eq)]
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
}
#[allow(dead_code)] // Pending receiver runtime owner.
pub(crate) struct ReplayState {
    local: Arc<ReceiverIdentity>,
    observation: Option<OwnershipObservation>,
    failed: bool,
    workers: Vec<Entry>,
    apis: Vec<Entry>,
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
            if sequence <= entry.highest {
                return Err(Error::Replay);
            }
            entry.highest = sequence;
        } else {
            if entries.len() == cap {
                return Err(Error::Exhausted);
            }
            entries.push(Entry {
                identity,
                highest: sequence,
            });
        }
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
