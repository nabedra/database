use super::*;
use crate::{oltp_runtime::coordinator::QueueCoordinator, scheduling::MemoryCharge};
use std::{collections::BTreeMap, sync::Mutex};
use tokio::time::Instant;
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod fixture;

const MAX_ATTEMPTS: usize = 16;
const ATTEMPT_BYTES: u64 = 4096;
const UNUSED_LIFETIME: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadScope {
    pub request: RequestBinding,
    pub authority: Authority,
    pub read_ts: ReadTs,
}

/// The original issuer and exact receiver of one Data scan. The queue retains
/// this identity in RAM; the worker keeps the executable input separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    pub(crate) scope: ReadScope,
    pub(crate) owner: WorkerIdentity,
    pub(crate) worker: WorkerIdentity,
    pub(crate) group: Uuid,
    pub(crate) node: u64,
    pub(crate) ticket: Uuid,
    pub(crate) input: Digest,
}

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
        let now = Instant::now();
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
        let ticket = Uuid::new_v4();
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
            || !matches!(entry.state, State::Unused(deadline) if deadline > Instant::now())
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
            #[cfg(test)]
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
    pub(super) queue: Arc<QueueCoordinator>,
    pub(super) grant: Box<ReadGrant>,
}
impl UnsettledRead {
    #[cfg(test)]
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
        self.queue.settle_read_grant(*self.grant).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkerResources;

    #[tokio::test(start_paused = true)]
    async fn o22_f01_f03_running_tickets_survive_timeout_and_unused_tickets_are_fenced() {
        let shared =
            WorkerSharedResources::new(1, WorkerResources::default(), Default::default()).unwrap();
        let book = &shared.read_attempts;
        let base = shared.scheduler().usage();
        let hash = Digest::from_bytes([7; 32]);
        let worker = WorkerIdentity {
            worker_id: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            certificate_sha256: hash,
            enrollment_revision: 1,
        };
        let run_id = Uuid::new_v4();
        let scope = ReadScope {
            request: RequestBinding {
                run_id,
                request_id: Uuid::new_v4(),
                digest: hash,
                accepted_context: hash,
            },
            authority: Authority {
                run_id,
                owner_term: 1,
            },
            read_ts: ReadTs::new(13).unwrap(),
        };
        let group = Uuid::new_v4();
        let grant = ReadGrant {
            scope,
            owner: worker,
            worker,
            group,
            node: 7,
            ticket: Uuid::nil(),
            input: hash,
        };
        let unused = book.open(&shared, grant).unwrap();
        book.settle(unused).unwrap();
        assert!(
            book.claim(unused.ticket, worker, group, grant.node)
                .is_err()
        );
        let live = book.open(&shared, grant).unwrap();
        let stale = WorkerIdentity {
            incarnation: Uuid::new_v4(),
            ..worker
        };
        assert!(book.claim(live.ticket, stale, group, grant.node).is_err());
        assert!(
            book.claim(live.ticket, worker, group, grant.node + 1)
                .is_err()
        );
        assert!(
            book.settle(ReadGrant {
                owner: stale,
                ..live
            })
            .is_err()
        );
        let (original, work) = book.claim(live.ticket, worker, group, grant.node).unwrap();
        assert_eq!(original, live);
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        let aged = book.open(&shared, grant).unwrap();
        tokio::time::advance(UNUSED_LIFETIME * 2).await;
        let another = book.open(&shared, grant).unwrap();
        assert!(book.claim(aged.ticket, worker, group, grant.node).is_err());
        book.settle(another).unwrap();
        assert!(book.settle(live).is_err());
        assert_eq!(shared.scheduler().usage()[0].1, 0);
        assert_eq!(shared.scheduler().usage()[0].2, base[0].2 + ATTEMPT_BYTES);
        drop(work);
        book.settle(live).unwrap();
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        assert_eq!(shared.scheduler().usage(), base);
    }
}
