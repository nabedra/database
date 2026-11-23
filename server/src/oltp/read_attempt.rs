//! Data read tickets retain a worker memory charge until settlement.
use crate::{
    oltp::{mvcc::store::consensus::ConsensusFuture, pool::WorkerSharedResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::{authority::ReadGrant, types::WorkerIdentity};
use nabedra_resources::scheduling::MemoryCharge;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod fixture;

const MAX_ATTEMPTS: usize = 16;
pub(crate) const ATTEMPT_BYTES: u64 = 4096;
pub(crate) const UNUSED_LIFETIME: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(crate) struct ReadAttempts {
    entries: Arc<Mutex<BTreeMap<Uuid, Entry>>>,
}

struct Entry {
    grant: ReadGrant,
    state: State,
    _owner: MemoryCharge,
}

enum State {
    Unused(Instant),
    Running,
}

impl ReadAttempts {
    pub(crate) fn open(
        &self,
        shared: &WorkerSharedResources,
        mut grant: ReadGrant,
    ) -> Result<ReadGrant, ProtocolError> {
        let now = nabedra_rt::time::now();
        let mut entries = self.entries.lock().map_err(|_| unavailable())?;
        entries
            .retain(|_, entry| !matches!(entry.state, State::Unused(deadline) if deadline <= now));
        if entries.len() >= MAX_ATTEMPTS {
            return Err(ProtocolError::new(
                ErrorCode::Overloaded,
                "Data read attempt capacity is exhausted",
                Outcome::NotApplied,
            ));
        }
        let ticket = nabedra_rt::ids::uuid_v4();
        grant.ticket = ticket;
        if entries.contains_key(&ticket) {
            return Err(unavailable());
        }
        let owner = shared
            .scheduler()
            .charge_memory(shared.scheduler_node_id(), ATTEMPT_BYTES)?;
        entries.insert(
            ticket,
            Entry {
                grant,
                state: State::Unused(now + UNUSED_LIFETIME),
                _owner: owner,
            },
        );
        Ok(grant)
    }

    pub(crate) fn claim(
        &self,
        ticket: Uuid,
        peer: WorkerIdentity,
        group: Uuid,
        node: u64,
    ) -> Result<(ReadGrant, RunningAttempt), ProtocolError> {
        let mut entries = self.entries.lock().map_err(|_| unavailable())?;
        let entry = entries.get_mut(&ticket).ok_or_else(invalid)?;
        if entry.grant.owner != peer
            || entry.grant.group != group
            || entry.grant.node != node
            || !matches!(entry.state, State::Unused(deadline) if deadline > nabedra_rt::time::now())
        {
            return Err(invalid());
        }
        entry.state = State::Running;
        Ok((
            entry.grant,
            RunningAttempt {
                ticket,
                entries: self.entries.clone(),
            },
        ))
    }

    /// A running ticket returns a retryable answer immediately so remote
    /// settlement never occupies queue-control handlers while work is held.
    pub(crate) fn settle(&self, grant: ReadGrant) -> Result<(), ProtocolError> {
        let ticket = grant.ticket;
        let mut entries = self.entries.lock().map_err(|_| unavailable())?;
        let Some(entry) = entries.get(&ticket) else {
            return Ok(());
        };
        if entry.grant != grant {
            return Err(invalid());
        }
        if matches!(entry.state, State::Running) {
            #[cfg(any(test, feature = "test-hooks"))]
            fixture::record(
                grant.scope,
                grant.group,
                "settlement-waiting",
                ticket,
                grant.worker,
            );
            return Err(unavailable());
        }
        entries.remove(&ticket);
        Ok(())
    }
}

pub(crate) struct RunningAttempt {
    ticket: Uuid,
    entries: Arc<Mutex<BTreeMap<Uuid, Entry>>>,
}

impl Drop for RunningAttempt {
    fn drop(&mut self) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.ticket);
    }
}

pub(crate) enum ScheduledReadError {
    Settled(ProtocolError),
    Uncertain {
        cause: ProtocolError,
        read: UnsettledRead,
    },
}

impl From<ProtocolError> for ScheduledReadError {
    fn from(error: ProtocolError) -> Self {
        Self::Settled(error)
    }
}

pub(crate) struct UnsettledRead {
    settlement: Arc<dyn nabedra_oltp_types::queue::QueueReadSource>,
    pub(crate) grant: Box<ReadGrant>,
}
impl UnsettledRead {
    pub(crate) fn new(settlement: Arc<dyn nabedra_oltp_types::queue::QueueReadSource>, grant: ReadGrant) -> Self {
        Self {
            settlement,
            grant: Box::new(grant),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn record(&self, stage: &str) {
        fixture::record(
            self.grant.scope,
            self.grant.group,
            stage,
            self.grant.ticket,
            self.grant.worker,
        );
    }

    pub(crate) async fn settle(&self) -> bool {
        self.settlement.settle_read_grant(*self.grant).await.is_ok()
    }
}

fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "no registered group leader completed the strong service call",
        Outcome::Unknown,
    )
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "authenticated participant proof context differs",
        Outcome::NotApplied,
    )
}
