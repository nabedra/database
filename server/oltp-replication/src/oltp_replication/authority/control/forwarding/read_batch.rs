use super::{
    ByteClass, ProtocolError, ReadBarrier, ScratchLease, WorkerSharedResources, unavailable,
};
use futures_util::future::{BoxFuture, FutureExt, Shared, WeakShared};
use std::{
    future::Future,
    mem::{size_of, size_of_val},
    sync::{Arc, Mutex},
};

const MAX_READERS: usize = 8;
type BarrierFuture = BoxFuture<'static, (Result<ReadBarrier, ProtocolError>, Arc<ScratchLease>)>;

struct Waiting {
    barrier: WeakShared<BarrierFuture>,
    readers: usize,
}

// The weak pointer may leave one empty Shared allocation until the next read.
pub(super) const RESIDENT_BYTES: usize = size_of::<Mutex<Option<Waiting>>>() + 256;

#[derive(Clone, Default)]
pub(super) struct ReadBatch {
    waiting: Arc<Mutex<Option<Waiting>>>,
}

impl ReadBatch {
    pub(super) fn join<F>(
        &self,
        resources: &WorkerSharedResources,
        issue: impl FnOnce() -> F,
    ) -> Option<Shared<BarrierFuture>>
    where
        F: Future<Output = Result<ReadBarrier, ProtocolError>> + Send + 'static,
    {
        let mut waiting = self.waiting.lock().ok()?;
        if let Some(batch) = waiting.as_mut()
            && let Some(barrier) = batch.barrier.upgrade()
        {
            if batch.readers == MAX_READERS {
                return None;
            }
            batch.readers += 1;
            return Some(barrier);
        }

        let read = issue();
        // Cover the boxed future, Shared state, and its bounded waker slab.
        let bytes = size_of_val(&read) + 512 + MAX_READERS * 128;
        // Sharing is optional. Keep its memory out of the progress arena
        // that the forwarded barrier needs to receive its reply.
        let owner = Arc::new(resources.reserve_scratch(ByteClass::Data, bytes).ok()?);
        let open = self.waiting.clone();
        let barrier = async move {
            let result = async move {
                tokio::task::yield_now().await;
                // An active Shared keeps its own weak entry alive until this point.
                // Later callers can only join a future that has not issued ReadIndex.
                open.lock().map_err(|_| unavailable())?.take();
                read.await
            }
            .await;
            (result, owner)
        }
        .boxed()
        .shared();
        *waiting = Some(Waiting {
            barrier: barrier.downgrade()?,
            readers: 1,
        });
        Some(barrier)
    }
}
