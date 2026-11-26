//! GC2: per-range batching of direct commits (`DirectBatch`).
//!
//! This is ScyllaDB's batching of concurrent writes on a shard (commitlog
//! segment writes and memtable applies shared by every mutation that arrived
//! while the previous write was in flight), at the level of Raft entries:
//!
//! - A lone commit is proposed at once as a one-entry `Direct` commit, with
//!   zero linger, exactly as without batching.
//! - While a proposal for the range is in flight, further commits for that
//!   range queue here. When it returns, the queue (at most
//!   `MAX_DIRECT_BATCH_MEMBERS` members and `MAX_DIRECT_ENTRY_BYTES` encoded)
//!   is proposed as one `DirectBatch` by a driver task, and so on until the
//!   queue is empty. One proposal per range is in flight at a time.
//!
//! Every member has already done steps 1 to 4 of plan section 6 under its own
//! key latches and registered its `commit_ts` with the range clock, so the
//! members of one batch never share a key (apply refuses one that does).
//! Each member keeps its own reply: a proposal error reaches every member,
//! and each retries through its dedup record as it would alone.
//!
//! A member whose deadline passes while it is still queued is withdrawn: it
//! was never proposed, so its outcome is `not_applied`. One whose caller is
//! gone is dropped before its batch is proposed, and its pending timestamp
//! leaves the range clock. Once a member is in a proposal, a deadline makes
//! its outcome unknown, as for a lone commit.
use super::stats::DirectStats;
use crate::{
    oltp::{
        mvcc::store::{
            Fault, ParticipantCommand, ParticipantHandle, ParticipantReply,
            direct::{
                DirectBatch, DirectCommit, DirectMemberReply, DirectReply, DirectResult,
                MAX_DIRECT_BATCH_MEMBERS,
            },
        },
        pool::{ByteClass, WorkerSharedResources},
    },
    oltp_replication::owned::OwnedCommand,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::types::CommitTs;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::{sync::oneshot, time::Instant};
use uuid::Uuid;

/// One commit's outcome, from a lone `Direct` entry or a `DirectBatch`.
#[derive(Debug)]
pub(super) enum Committed {
    Applied,
    /// The request's dedup record already existed. A batch member carries
    /// no result: the caller reads the record while still holding its latches.
    Replayed(Option<DirectResult>),
    Forwarded,
    Rejected(Fault),
    /// The reply did not fit the command.
    Corrupt,
}
impl Committed {
    pub(super) fn of(reply: ParticipantReply) -> Self {
        match reply {
            ParticipantReply::Direct(DirectReply::Applied { .. }) => Self::Applied,
            ParticipantReply::Direct(DirectReply::Replayed { result }) => {
                Self::Replayed(Some(result))
            }
            ParticipantReply::Direct(DirectReply::Forwarded) => Self::Forwarded,
            ParticipantReply::Direct(DirectReply::Rejected(fault))
            | ParticipantReply::Rejected(fault) => Self::Rejected(fault),
            _ => Self::Corrupt,
        }
    }
    fn member(reply: DirectMemberReply) -> Self {
        match reply {
            DirectMemberReply::Applied { .. } => Self::Applied,
            DirectMemberReply::Replayed => Self::Replayed(None),
            DirectMemberReply::Forwarded => Self::Forwarded,
            DirectMemberReply::Rejected(fault) => Self::Rejected(fault),
        }
    }
}

type Reply = Result<Committed, ProtocolError>;

struct Member {
    id: u64,
    commit: DirectCommit,
    /// `encoded_len` of the commit.
    bytes: usize,
    deadline: Instant,
    handle: Arc<ParticipantHandle>,
    reply: oneshot::Sender<Reply>,
}

#[derive(Default)]
struct State {
    /// A proposal for the range is in flight (a lone commit or a driver).
    busy: bool,
    queue: VecDeque<Member>,
    next_id: u64,
}

#[derive(Default)]
struct Range {
    state: Mutex<State>,
}
impl Range {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// The next batch, or `None` (and the range idle) when nothing is
    /// queued. Members whose callers are gone are dropped unproposed.
    fn take(&self, max_bytes: usize) -> Option<Vec<Member>> {
        let mut state = self.lock();
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while let Some(member) = state.queue.front() {
            if batch.len() == MAX_DIRECT_BATCH_MEMBERS
                || (!batch.is_empty() && bytes.saturating_add(member.bytes) > max_bytes)
            {
                break;
            }
            let member = state.queue.pop_front().expect("front exists");
            if member.reply.is_closed() {
                if let Ok(clock) = member.handle.direct_clock() {
                    clock.settle(member.commit.commit_ts);
                }
                continue;
            }
            bytes = bytes.saturating_add(member.bytes);
            batch.push(member);
        }
        if batch.is_empty() {
            state.busy = false;
            None
        } else {
            Some(batch)
        }
    }
    /// Removes a still-queued member. False once a batch took it.
    fn withdraw(&self, id: u64) -> bool {
        let mut state = self.lock();
        let Some(index) = state.queue.iter().position(|member| member.id == id) else {
            return false;
        };
        state.queue.remove(index);
        true
    }
}

/// The batchers of every range this worker leads, keyed by group id.
pub(super) struct Batchers {
    ranges: Mutex<HashMap<Uuid, Arc<Range>>>,
    resources: Arc<WorkerSharedResources>,
    max_bytes: usize,
}

impl Batchers {
    pub(super) fn new(resources: Arc<WorkerSharedResources>, max_bytes: usize) -> Self {
        Self {
            ranges: Mutex::default(),
            resources,
            max_bytes,
        }
    }

    fn range(&self, group_id: Uuid) -> Arc<Range> {
        self.ranges
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(group_id)
            .or_default()
            .clone()
    }

    /// Proposes `commit` (already registered with the range clock) alone or
    /// with the commits queued behind the range's proposal in flight.
    pub(super) async fn propose(
        &self,
        stats: &Arc<DirectStats>,
        handle: &Arc<ParticipantHandle>,
        commit: DirectCommit,
        bytes: usize,
        deadline: Instant,
    ) -> Reply {
        let range = self.range(handle.identity().group_id);
        let queued = {
            let mut state = range.lock();
            if state.busy {
                let (reply, receiver) = oneshot::channel();
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.queue.push_back(Member {
                    id,
                    commit,
                    bytes,
                    deadline,
                    handle: handle.clone(),
                    reply,
                });
                Err((id, receiver))
            } else {
                state.busy = true;
                Ok(commit)
            }
        };
        match queued {
            Ok(commit) => {
                // Hands the queue to a driver (or idles the range) however
                // this proposal ends, cancellation included.
                let _handoff = Handoff {
                    range,
                    resources: self.resources.clone(),
                    stats: stats.clone(),
                    max_bytes: self.max_bytes,
                };
                lone(&self.resources, handle, commit, deadline).await
            }
            Err((id, receiver)) => match nabedra_rt::time::timeout_at(deadline, receiver).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(_)) => Err(unknown("the direct batch driver stopped")),
                Err(_) if range.withdraw(id) => Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "direct request deadline expired before its batch was proposed",
                    Outcome::NotApplied,
                )),
                Err(_) => Err(unknown("direct request deadline expired")),
            },
        }
    }
}

async fn lone(
    resources: &Arc<WorkerSharedResources>,
    handle: &ParticipantHandle,
    commit: DirectCommit,
    deadline: Instant,
) -> Reply {
    let command = OwnedCommand::new_owned(
        ParticipantCommand::Direct(commit),
        resources,
        ByteClass::Data,
    )?;
    let applied = by(deadline, handle.propose(command)).await?;
    Ok(Committed::of(applied.reply))
}

/// Ends the range's lone proposal: the commits queued behind it go to a
/// driver task, or the range becomes idle.
struct Handoff {
    range: Arc<Range>,
    resources: Arc<WorkerSharedResources>,
    stats: Arc<DirectStats>,
    max_bytes: usize,
}
impl Drop for Handoff {
    fn drop(&mut self) {
        let Some(batch) = self.range.take(self.max_bytes) else {
            return;
        };
        let driver = Driver {
            range: self.range.clone(),
            resources: self.resources.clone(),
            stats: self.stats.clone(),
            max_bytes: self.max_bytes,
            unproposed: batch,
            armed: true,
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                nabedra_rt::task::spawn_on(&runtime, driver.run());
            }
            // No runtime (shutdown): nothing was proposed.
            Err(_) => drop(driver),
        }
    }
}

struct Driver {
    range: Arc<Range>,
    resources: Arc<WorkerSharedResources>,
    stats: Arc<DirectStats>,
    max_bytes: usize,
    /// Members taken from the queue before the driver starts proposing them.
    unproposed: Vec<Member>,
    /// Until the queue is empty: on an unexpected stop (a panic or runtime
    /// shutdown) `Drop` refuses unproposed members and idles the range.
    armed: bool,
}
impl Driver {
    async fn run(mut self) {
        loop {
            let batch = std::mem::take(&mut self.unproposed);
            self.propose(batch).await;
            match self.range.take(self.max_bytes) {
                Some(next) => self.unproposed = next,
                None => {
                    self.armed = false;
                    return;
                }
            }
        }
    }

    async fn propose(&self, batch: Vec<Member>) {
        let handle = batch[0].handle.clone();
        let deadline = batch
            .iter()
            .map(|member| member.deadline)
            .max()
            .expect("a batch has a member");
        if batch.len() == 1 {
            let member = batch.into_iter().next().expect("one member");
            let commit_ts = member.commit.commit_ts;
            let reply = lone(&self.resources, &handle, member.commit, deadline).await;
            finish_member(&handle, commit_ts, member.reply, reply);
            return;
        }
        let (commits, replies): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .map(|member| {
                let commit_ts = member.commit.commit_ts;
                (member.commit, (member.handle, commit_ts, member.reply))
            })
            .unzip();
        let members = commits.len();
        self.stats.batch_entry(members);
        let result = match OwnedCommand::new_owned(
            ParticipantCommand::DirectBatch(DirectBatch { members: commits }),
            &self.resources,
            ByteClass::Data,
        ) {
            Ok(command) => by(deadline, handle.propose(command)).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(applied) => match applied.reply {
                ParticipantReply::Direct(DirectReply::Batch(outcomes))
                    if outcomes.len() == members =>
                {
                    for ((_, _, reply), outcome) in replies.into_iter().zip(outcomes) {
                        let _ = reply.send(Ok(Committed::member(outcome)));
                    }
                }
                // A whole-entry rejection wrote nothing for any member.
                ParticipantReply::Direct(DirectReply::Rejected(fault))
                | ParticipantReply::Rejected(fault) => {
                    for (_, _, reply) in replies {
                        let _ = reply.send(Ok(Committed::Rejected(fault)));
                    }
                }
                _ => {
                    for (_, _, reply) in replies {
                        let _ = reply.send(Ok(Committed::Corrupt));
                    }
                }
            },
            Err(error) => {
                for (handle, commit_ts, reply) in replies {
                    finish_member(&handle, commit_ts, reply, Err(error.clone()));
                }
            }
        }
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let queued = {
            let mut state = self.range.lock();
            state.busy = false;
            std::mem::take(&mut state.queue)
        };
        for member in self.unproposed.drain(..).chain(queued) {
            if let Ok(clock) = member.handle.direct_clock() {
                clock.settle(member.commit.commit_ts);
            }
            let _ = member.reply.send(Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "the direct batch driver stopped before proposing",
                Outcome::NotApplied,
            )));
        }
    }
}

fn finish_member(
    handle: &ParticipantHandle,
    commit_ts: CommitTs,
    sender: oneshot::Sender<Reply>,
    reply: Reply,
) {
    if matches!(&reply, Err(error) if error.outcome == Outcome::NotApplied)
        && let Ok(clock) = handle.direct_clock()
    {
        // Once a driver takes a member, the caller can no longer withdraw it.
        // A definite pre-append error settles it even if the caller timed out.
        clock.settle(commit_ts);
    }
    let _ = sender.send(reply);
}

fn unknown(message: &str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::Unknown)
}

/// A deadline can cut a submitted proposal before its response is known.
async fn by<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = Result<T, ProtocolError>>,
) -> Result<T, ProtocolError> {
    nabedra_rt::time::timeout_at(deadline, future)
        .await
        .unwrap_or_else(|_| Err(unknown("direct request deadline expired")))
}
