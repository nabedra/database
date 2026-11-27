//! Own the Data proposal's filesystem charge through completion of its apply.
use super::{HostRaft, client_write_error, proposal_forwarded, unavailable};
use crate::{
    oltp::{
        mvcc::store::{
            ParticipantCommand, ParticipantMachine,
            direct::{DirectCommit, RangeClock},
            encoded_length,
        },
        pool::{DiskReservation, GroupStore},
    },
    oltp_replication::{authority::config::DataConfig, owned::OwnedCommand},
    protocol::ProtocolError,
};
use openraft::{
    errors::{ClientWriteError, RaftError},
    raft::{ClientWriteResponse, ClientWriteResult, responder::ProgressResponder},
    type_config::async_runtime::watch::WatchReceiver,
};
use tokio::sync::oneshot;
use uuid::Uuid;

pub(super) async fn client_write(
    raft: &HostRaft<DataConfig, ParticipantMachine>,
    store: &GroupStore,
    command: OwnedCommand<ParticipantCommand>,
) -> Result<ClientWriteResponse<DataConfig>, ProtocolError> {
    let payload = command.payload()?;
    let Some((request, apply_items)) = reservation_request(payload)? else {
        return raft.client_write(command).await.map_err(client_write_error);
    };
    // The bounded Raft API send can be cancelled while waiting for capacity.
    // The timestamp remains ours until Raft owns the command.
    let mut unsubmitted = Unsubmitted {
        store,
        direct: matches!(
            payload,
            ParticipantCommand::Direct(_) | ParticipantCommand::DirectBatch(_)
        )
        .then(|| command.clone()),
    };
    let encoded_bytes = encoded_length(payload, usize::MAX)?;
    #[cfg(any(test, feature = "test-hooks"))]
    if is_leader(raft) {
        crate::oltp::test_hooks::transaction_checkpoint(
            "storage-admission",
            request,
            &store.identity().group_id,
        )
        .await;
    }
    #[cfg(not(any(test, feature = "test-hooks")))]
    let _ = request;
    let reservation = store
        .pool()
        .reserve_proposal_disk(encoded_bytes, apply_items)
        .map_err(|error| {
            if is_leader(raft) {
                error
            } else {
                proposal_forwarded()
            }
        })?;
    #[cfg(any(test, feature = "test-hooks"))]
    if is_leader(raft) {
        crate::oltp::test_hooks::transaction_checkpoint(
            "storage-reserved",
            request,
            &store.identity().group_id,
        )
        .await;
    }

    let (responder, receiver) = ProgressResponder::complete_only();
    raft.client_write_ff(command, Some(responder))
        .await
        .map_err(|err| client_write_error(RaftError::Fatal(err)))?;
    // A successful send transfers ownership even when the deadline cuts our wait.
    Pending {
        completion: Some((receiver, reservation)),
        store,
        direct: unsubmitted.direct.take(),
    }
    .receive()
    .await
}

fn is_leader(raft: &HostRaft<DataConfig, ParticipantMachine>) -> bool {
    let metrics = raft.metrics();
    let metrics = metrics.borrow_watched();
    metrics.current_leader == Some(metrics.id)
}

/// New data writes are admitted here. Votes and replication use the storage
/// adapters directly, while seal, resolve, fencing and cleanup must still run
/// when new writes are refused. Timestamp-point recovery also uses its
/// Prepare command to settle an already installed decision home.
fn reservation_request(
    command: &ParticipantCommand,
) -> Result<Option<(Uuid, usize)>, ProtocolError> {
    fn direct_items(commit: &DirectCommit) -> usize {
        // One item for the member's dedup and expiry metadata, then one per
        // changed row or secondary index key.
        1 + commit
            .writes
            .iter()
            .map(|write| 1 + write.index.len())
            .sum::<usize>()
    }
    Ok(match command {
        ParticipantCommand::Direct(commit) => Some((commit.request_id, direct_items(commit))),
        ParticipantCommand::DirectBatch(batch) => batch.members.first().map(|first| {
            (
                first.request_id,
                batch.members.iter().map(direct_items).sum(),
            )
        }),
        ParticipantCommand::PreparePage { txn, writes, .. } => Some((txn.request_id, writes.len())),
        ParticipantCommand::Scoped { command, .. } => reservation_request(command.payload()?)?,
        _ => None,
    })
}

type Completion = oneshot::Receiver<ClientWriteResult<DataConfig>>;

struct Unsubmitted<'a> {
    store: &'a GroupStore,
    direct: Option<OwnedCommand<ParticipantCommand>>,
}

impl Drop for Unsubmitted<'_> {
    fn drop(&mut self) {
        if let Some(command) = self.direct.take() {
            settle_direct(self.store, &command);
        }
    }
}

fn settle_direct(store: &GroupStore, command: &OwnedCommand<ParticipantCommand>) {
    let Ok(payload) = command.payload() else {
        return;
    };
    let clock = RangeClock::for_store(store);
    match payload {
        ParticipantCommand::Direct(commit) => clock.settle(commit.commit_ts),
        ParticipantCommand::DirectBatch(batch) => {
            for member in &batch.members {
                clock.settle(member.commit_ts);
            }
        }
        _ => {}
    }
}

struct Pending<'a> {
    completion: Option<(Completion, DiskReservation)>,
    store: &'a GroupStore,
    direct: Option<OwnedCommand<ParticipantCommand>>,
}

impl Pending<'_> {
    async fn receive(&mut self) -> Result<ClientWriteResponse<DataConfig>, ProtocolError> {
        let (reply, _) = self.completion.as_mut().expect("unconsumed proposal");
        let reply = reply.await;
        self.completion.take();
        reply
            .map_err(|_| unavailable())?
            .map_err(|err| client_write_error(RaftError::APIError(err)))
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Some((reply, reservation)) = self.completion.take() {
            // A cancelled caller can leave an entry in Raft. Keep its charge
            // until the responder completes, is discarded, or the core stops.
            let direct = self
                .direct
                .take()
                .map(|command| (self.store.clone(), command));
            nabedra_rt::task::spawn(async move {
                let response = reply.await;
                if matches!(response, Ok(Err(ClientWriteError::ForwardToLeader(_))))
                    && let Some((store, command)) = direct
                {
                    // ForwardToLeader is issued before append, even after enqueue.
                    settle_direct(&store, &command);
                }
                drop(reservation);
            });
        }
    }
}
