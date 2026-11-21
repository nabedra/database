//! Own the Data proposal's filesystem charge through completion of its apply.
use super::{HostRaft, client_write_error, proposal_forwarded, unavailable};
use crate::{
    oltp::{
        mvcc::store::{ParticipantCommand, ParticipantMachine},
        pool::{DiskReservation, GroupStore},
    },
    oltp_replication::{authority::config::DataConfig, owned::OwnedCommand},
    protocol::ProtocolError,
};
use openraft::{
    errors::RaftError,
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
    let Some(request) = reservation_request(command.payload()?)? else {
        return raft.client_write(command).await.map_err(client_write_error);
    };
    #[cfg(test)]
    if is_leader(raft) {
        crate::oltp_runtime::recovery_tests::transaction_checkpoint(
            "storage-admission",
            request,
            &store.identity().group_id,
        )
        .await;
    }
    #[cfg(not(test))]
    let _ = request;
    let reservation = store.pool().reserve_proposal_disk().map_err(|error| {
        if is_leader(raft) {
            error
        } else {
            proposal_forwarded()
        }
    })?;
    #[cfg(test)]
    if is_leader(raft) {
        crate::oltp_runtime::recovery_tests::transaction_checkpoint(
            "storage-reserved",
            request,
            &store.identity().group_id,
        )
        .await;
    }

    let (responder, receiver) = ProgressResponder::complete_only();
    let mut pending = Pending(Some((receiver, reservation)));
    raft.client_write_ff(command, Some(responder))
        .await
        .map_err(|err| client_write_error(RaftError::Fatal(err)))?;
    pending.receive().await
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
fn reservation_request(command: &ParticipantCommand) -> Result<Option<Uuid>, ProtocolError> {
    Ok(match command {
        ParticipantCommand::Direct(commit) => Some(commit.request_id),
        ParticipantCommand::DirectBatch(batch) => {
            batch.members.first().map(|commit| commit.request_id)
        }
        ParticipantCommand::PreparePage { txn, .. } => Some(txn.request_id),
        ParticipantCommand::Scoped { command, .. } => reservation_request(command.payload()?)?,
        _ => None,
    })
}

type Completion = oneshot::Receiver<ClientWriteResult<DataConfig>>;

struct Pending(Option<(Completion, DiskReservation)>);

impl Pending {
    async fn receive(&mut self) -> Result<ClientWriteResponse<DataConfig>, ProtocolError> {
        let (reply, _) = self.0.as_mut().expect("unconsumed proposal");
        let reply = reply.await;
        self.0.take();
        reply
            .map_err(|_| unavailable())?
            .map_err(|err| client_write_error(RaftError::APIError(err)))
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some((reply, reservation)) = self.0.take() {
            // A cancelled caller can leave an entry in Raft. Keep its charge
            // until the responder completes, is discarded, or the core stops.
            tokio::spawn(async move {
                let _ = reply.await;
                drop(reservation);
            });
        }
    }
}
