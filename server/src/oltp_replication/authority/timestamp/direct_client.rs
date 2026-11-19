//! Per-worker `AllocateDirect` client.
//!
//! Concurrent callers share one in-flight call. A caller that arrives while a
//! call is in flight waits for the next one, which starts after the current
//! call returns. Values are never cached across calls: a caller that starts
//! after another caller finished is always served by a later call, so its
//! timestamps are higher (O09: no cached timestamp behind a completed
//! operation).
//!
//! A worker that hosts a Timestamp replica tries it in process first. Only the
//! leader can answer; on any local failure the client uses the remote route
//! and skips the local replica for `LOCAL_BACKOFF`.
// Consumers arrive with T4, T5 and T7; until then only tests call this API.
#![cfg_attr(not(test), allow(dead_code))]
use super::direct::{DirectTimestampRange, DirectTimestampSource, MAX_DIRECT_COUNT};
use crate::{
    oltp_replication::types::CommitTs,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const LOCAL_BACKOFF: Duration = Duration::from_secs(1);

type Reply = Result<DirectTimestampRange, ProtocolError>;

struct Waiter {
    count: u64,
    reply: oneshot::Sender<Reply>,
}
#[derive(Default)]
struct Queue {
    waiters: VecDeque<Waiter>,
    running: bool,
}
struct Inner {
    local: Option<Arc<dyn DirectTimestampSource>>,
    remote: Arc<dyn DirectTimestampSource>,
    local_skip_until: Mutex<Option<Instant>>,
    queue: Mutex<Queue>,
    calls: AtomicU64,
    local_calls: AtomicU64,
}

/// Coalescing `AllocateDirect` handle. Create one per worker and share it.
#[derive(Clone)]
pub(crate) struct DirectTimestampClient {
    inner: Arc<Inner>,
}
impl DirectTimestampClient {
    /// `local` is this worker's in-process Timestamp replica, if any.
    /// `remote` routes to the Timestamp leader.
    pub(crate) fn new(
        local: Option<Arc<dyn DirectTimestampSource>>,
        remote: Arc<dyn DirectTimestampSource>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                local,
                remote,
                local_skip_until: Mutex::new(None),
                queue: Mutex::new(Queue::default()),
                calls: AtomicU64::new(0),
                local_calls: AtomicU64::new(0),
            }),
        }
    }
    /// One commit timestamp above every timestamp returned before this call.
    pub(crate) async fn commit_ts(&self) -> Result<CommitTs, ProtocolError> {
        Ok(self.allocate(1).await?.first())
    }
    /// `count` contiguous timestamps.
    pub(crate) async fn allocate(&self, count: u64) -> Result<DirectTimestampRange, ProtocolError> {
        if count == 0 || count > MAX_DIRECT_COUNT {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                format!("AllocateDirect count must be 1 to {MAX_DIRECT_COUNT}"),
                Outcome::NotApplied,
            ));
        }
        let (reply, receive) = oneshot::channel();
        let start = {
            let mut queue = self.inner.lock_queue();
            queue.waiters.push_back(Waiter { count, reply });
            !std::mem::replace(&mut queue.running, true)
        };
        if start {
            tokio::spawn(self.inner.clone().drive());
        }
        receive.await.map_err(|_| {
            ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "direct timestamp allocation stopped",
                Outcome::Unknown,
            )
        })?
    }
    /// Calls issued to either route (for tests and stats).
    pub(crate) fn calls(&self) -> u64 {
        self.inner.calls.load(Ordering::Relaxed)
    }
    /// Calls answered by the in-process Timestamp leader.
    pub(crate) fn local_calls(&self) -> u64 {
        self.inner.local_calls.load(Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.inner.lock_queue().waiters.len()
    }
}
/// Clears `running` if the driver is cancelled or panics. Its in-flight batch
/// and queued waiters lose their senders, so they fail instead of hanging.
struct Running(Option<Arc<Inner>>);
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            let mut queue = inner.lock_queue();
            queue.running = false;
            queue.waiters.clear();
        }
    }
}
impl Inner {
    fn lock_queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    async fn drive(self: Arc<Self>) {
        let mut running = Running(Some(self.clone()));
        loop {
            let (batch, total) = {
                let mut queue = self.lock_queue();
                let mut batch = Vec::new();
                let mut total = 0u64;
                while let Some(waiter) = queue.waiters.front() {
                    if !batch.is_empty() && total + waiter.count > MAX_DIRECT_COUNT {
                        break;
                    }
                    total += waiter.count;
                    batch.extend(queue.waiters.pop_front());
                }
                if batch.is_empty() {
                    queue.running = false;
                    running.0 = None;
                    return;
                }
                (batch, total)
            };
            let result = self
                .call(total)
                .await
                .and_then(|range| range.expect_len(total));
            match result {
                Ok(mut range) => {
                    for waiter in batch {
                        let (head, rest) = match range.split_first(waiter.count) {
                            Ok(split) => split,
                            Err(error) => {
                                let _ = waiter.reply.send(Err(error));
                                continue;
                            }
                        };
                        let _ = waiter.reply.send(Ok(head));
                        if let Some(rest) = rest {
                            range = rest;
                        }
                    }
                }
                Err(error) => {
                    for waiter in batch {
                        let _ = waiter.reply.send(Err(error.clone()));
                    }
                }
            }
        }
    }
    async fn call(&self, count: u64) -> Result<DirectTimestampRange, ProtocolError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(local) = &self.local {
            let skip = self
                .local_skip_until
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some_and(|until| Instant::now() < until);
            if !skip {
                match local.allocate_direct(count).await {
                    Ok(range) => {
                        self.local_calls.fetch_add(1, Ordering::Relaxed);
                        return Ok(range);
                    }
                    Err(_) => {
                        *self
                            .local_skip_until
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                            Some(Instant::now() + LOCAL_BACKOFF);
                    }
                }
            }
        }
        self.remote.allocate_direct(count).await
    }
}
