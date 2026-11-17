use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use openraft::Raft;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Instant, timeout, timeout_at};

use super::{
    GroupError, GroupResult, MAX_COMMAND_BYTES, RaftCommand, TypeConfig, WRITE_DEADLINE,
    command_size, submit,
};

const MAX_COMMANDS: usize = 16;
const MAX_DELAY: Duration = Duration::from_millis(1);

struct Pending {
    command: Value,
    bytes: usize,
    reply: oneshot::Sender<Result<Value, Arc<GroupError>>>,
    budget: OwnedSemaphorePermit,
}

pub(super) struct PublishBatcher {
    queue: mpsc::Sender<Pending>,
    budget: Arc<Semaphore>,
}

impl PublishBatcher {
    pub fn new(raft: Raft<TypeConfig>, budget: Arc<Semaphore>) -> Self {
        let (queue, receiver) = mpsc::channel(MAX_COMMANDS);
        tokio::spawn(send_batches(raft, receiver));
        Self { queue, budget }
    }

    pub async fn write(&self, command: Value) -> GroupResult<Value> {
        let bytes = command_size(&command)? + 1;
        let budget = self
            .budget
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| GroupError::Overloaded)?;
        let (send, receive) = oneshot::channel();
        self.queue
            .try_send(Pending {
                command,
                bytes,
                reply: send,
                budget,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => GroupError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => GroupError::Closed,
            })?;
        timeout(WRITE_DEADLINE, receive)
            .await
            .map_err(|_| GroupError::OutcomeUnknown)?
            .map_err(|_| GroupError::Closed)?
            .map_err(|error| match &*error {
                GroupError::NotLeader(id) => GroupError::NotLeader(*id),
                GroupError::Unavailable => GroupError::Unavailable,
                GroupError::OutcomeUnknown => GroupError::OutcomeUnknown,
                GroupError::Overloaded => GroupError::Overloaded,
                GroupError::Closed => GroupError::Closed,
                GroupError::Other(error) => GroupError::Other(anyhow!("{error}")),
            })
    }
}

async fn send_batches(raft: Raft<TypeConfig>, mut receiver: mpsc::Receiver<Pending>) {
    let mut deferred = None::<Pending>;
    loop {
        let first = match deferred.take() {
            Some(first) => first,
            None => match receiver.recv().await {
                Some(first) => first,
                None => return,
            },
        };
        if first.reply.is_closed() {
            continue;
        }
        let mut bytes = first.bytes;
        let mut pending = vec![first];
        let deadline = Instant::now() + MAX_DELAY;
        while pending.len() < MAX_COMMANDS {
            let Ok(Some(next)) = timeout_at(deadline, receiver.recv()).await else {
                break;
            };
            if next.reply.is_closed() {
                continue;
            }
            if bytes + next.bytes > MAX_COMMAND_BYTES - 64 {
                deferred = Some(next);
                break;
            }
            bytes += next.bytes;
            pending.push(next);
        }

        let (commands, replies): (Vec<_>, Vec<_>) = pending
            .into_iter()
            .map(|pending| (pending.command, (pending.reply, pending.budget)))
            .unzip();
        let result =
            submit(&raft, RaftCommand::Batch(commands))
                .await
                .and_then(|reply| match reply {
                    Value::Array(results) if replies.len() == results.len() => Ok(results),
                    _ => Err(anyhow!("publish batch reply has an incorrect count").into()),
                });
        match result {
            Ok(results) => {
                for ((reply, _budget), result) in replies.into_iter().zip(results) {
                    let _ = reply.send(Ok(result));
                }
            }
            Err(error) => {
                let error = Arc::new(error);
                for (reply, _budget) in replies {
                    let _ = reply.send(Err(error.clone()));
                }
            }
        }
    }
}
