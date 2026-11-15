//! A timeout observes transport failure, never remote settlement. The owned
//! caller retains both the original allocation and payload owner through retries.
use std::{future::Future, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Evidence {
    Pending,
    Closed,
    Fenced,
}
/// Only authenticated closure or authoritative fencing releases `grant`.
/// Retry delays are bounded while total safety waiting may remain indefinite.
pub(super) async fn retain_until_settled<G, C, P, PF, W, WF>(
    grant: G,
    child: C,
    mut probe: P,
    mut wait: W,
) -> C
where
    P: FnMut() -> PF,
    PF: Future<Output = Evidence>,
    W: FnMut(Duration) -> WF,
    WF: Future<Output = ()>,
{
    let mut delay = Duration::from_millis(10);
    loop {
        match probe().await {
            Evidence::Closed | Evidence::Fenced => break,
            Evidence::Pending => wait(delay).await,
        }
        delay = delay.saturating_mul(2).min(Duration::from_secs(1));
    }
    drop(grant);
    child
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::WorkerResources, scheduling::ResourceScheduler};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::Poll,
    };
    use tokio::sync::Semaphore;
    struct Child(Arc<AtomicBool>);
    impl Drop for Child {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    async fn uncertain_until(proof: Evidence) {
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 4096,
            task_memory_limit_bytes: 2048,
            control_memory_reserve_bytes: 1024,
        };
        let scheduler = ResourceScheduler::new(vec![(1, resources), (2, resources)]).unwrap();
        scheduler.set_io_capacity(1, 1).unwrap();
        scheduler.set_io_capacity(2, 1).unwrap();
        let grant = scheduler.reserve_remote_memory(7, 1, 2048).await.unwrap();
        assert_eq!(grant.node_id(), 1);
        let disposed = Arc::new(AtomicBool::new(false));
        let child = Child(disposed.clone());
        let checks = Arc::new(AtomicUsize::new(0));
        let releases = Arc::new(Semaphore::new(0));
        let entered = Arc::new(Semaphore::new(0));
        let probe_checks = checks.clone();
        let wait_entered = entered.clone();
        let wait_releases = releases.clone();
        let mut settlement = Box::pin(retain_until_settled(
            grant,
            child,
            move || {
                let count = probe_checks.fetch_add(1, Ordering::SeqCst);
                async move { if count < 3 { Evidence::Pending } else { proof } }
            },
            move |delay| {
                let entered = wait_entered.clone();
                let releases = wait_releases.clone();
                async move {
                    assert!(delay <= Duration::from_secs(1));
                    entered.add_permits(1);
                    releases.acquire().await.unwrap().forget();
                }
            },
        ));
        let mut replacement = Box::pin(scheduler.reserve_remote_memory(7, 1, 1500));
        for _ in 0..3 {
            assert!(matches!(
                std::future::poll_fn(|cx| Poll::Ready(settlement.as_mut().poll(cx))).await,
                Poll::Pending
            ));
            entered.acquire().await.unwrap().forget();
            assert!(
                matches!(
                    std::future::poll_fn(|cx| Poll::Ready(replacement.as_mut().poll(cx))).await,
                    Poll::Pending
                ),
                "cap one cannot allocate another node while the original uncertain child remains live"
            );
            assert!(!disposed.load(Ordering::SeqCst));
            assert_eq!(scheduler.usage(), vec![(1, 0, 2048), (2, 0, 0)]);
            releases.add_permits(1);
        }
        let child = settlement.await;
        assert!(!disposed.load(Ordering::SeqCst));
        let replacement = replacement.await.unwrap();
        drop(replacement);
        drop(child);
        assert!(disposed.load(Ordering::SeqCst));
        assert_eq!(scheduler.usage(), vec![(1, 0, 0), (2, 0, 0)]);
    }
    #[tokio::test]
    async fn f03_c04_timeout_and_close_false_retain_original_cap_until_authenticated_close() {
        uncertain_until(Evidence::Closed).await;
    }
    #[tokio::test]
    async fn r05_c04_unreachable_close_releases_only_after_authoritative_fence() {
        uncertain_until(Evidence::Fenced).await;
    }
}
