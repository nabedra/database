//! Striped key latches for the range leader (plan section 6 step 2).
//!
//! A mutation latches every row key it writes before it reads them, and
//! holds the latches until its entry applied or failed, so two direct
//! mutations of one key on one leader never interleave their read and
//! commit. Keys map to a fixed number of stripes; a request takes its
//! stripes in ascending stripe order, which is a total order, so two
//! requests can never wait on each other in a cycle. Waiting is bounded by
//! the request's deadline.
//!
//! Latches are an optimisation, not the safety argument: `DirectCommit`
//! apply rechecks every expected version, so a commit computed from a stale
//! read is rejected with `Conflict` and retried.
use std::{
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    time::Instant,
};

pub(crate) const DEFAULT_STRIPES: usize = 1024;

pub(crate) struct KeyLatches {
    stripes: Box<[Arc<Mutex<()>>]>,
}

/// Holds a request's stripes. Dropping it releases them.
pub(crate) struct LatchGuard {
    _held: Vec<OwnedMutexGuard<()>>,
    /// How long the request waited for stripes other requests held.
    pub(crate) waited: Option<Duration>,
}

/// The deadline passed before every stripe was free.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LatchTimeout;

impl KeyLatches {
    pub(crate) fn new(stripes: usize) -> Self {
        Self {
            stripes: (0..stripes.max(1))
                .map(|_| Arc::new(Mutex::new(())))
                .collect(),
        }
    }

    fn stripe(&self, key: &[u8]) -> usize {
        // A fixed-key SipHash: stable within the process, no allocation.
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.stripes.len() as u64) as usize
    }

    /// The distinct stripes of `keys`, in acquisition order.
    pub(crate) fn stripes_of<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) -> Vec<usize> {
        let mut stripes: Vec<usize> = keys.into_iter().map(|key| self.stripe(key)).collect();
        stripes.sort_unstable();
        stripes.dedup();
        stripes
    }

    /// Latches `keys` (encoded), waiting at most until `deadline`.
    pub(crate) async fn lock<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a [u8]>,
        deadline: Instant,
    ) -> Result<LatchGuard, LatchTimeout> {
        let stripes = self.stripes_of(keys);
        let mut held = Vec::with_capacity(stripes.len());
        let mut waited_since = None;
        for stripe in stripes {
            let mutex = self.stripes[stripe].clone();
            match mutex.clone().try_lock_owned() {
                Ok(guard) => held.push(guard),
                Err(_) => {
                    waited_since.get_or_insert_with(nabedra_rt::time::now);
                    let guard = nabedra_rt::time::timeout_at(deadline, mutex.lock_owned())
                        .await
                        .map_err(|_| LatchTimeout)?;
                    held.push(guard);
                }
            }
        }
        Ok(LatchGuard {
            _held: held,
            waited: waited_since.map(|since| since.elapsed()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn o08_latches_serialize_one_key_and_time_out_at_the_deadline() {
        let latches = KeyLatches::new(64);
        let far = nabedra_rt::time::now() + Duration::from_secs(5);
        let first = latches.lock([&b"k1"[..]], far).await.unwrap();
        assert!(first.waited.is_none());
        let near = nabedra_rt::time::now() + Duration::from_millis(50);
        assert_eq!(
            latches.lock([&b"k1"[..]], near).await.err(),
            Some(LatchTimeout)
        );
        drop(first);
        let again = latches.lock([&b"k1"[..]], far).await.unwrap();
        drop(again);
    }

    /// O08: overlapping multi-key requests in opposite key orders never
    /// deadlock, and every request completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn o08_overlapping_requests_in_opposite_orders_do_not_deadlock() {
        let latches = Arc::new(KeyLatches::new(8));
        let keys: Vec<Vec<u8>> = (0..16u8).map(|k| vec![k]).collect();
        let mut tasks = Vec::new();
        for task in 0..16usize {
            let latches = latches.clone();
            let mut mine: Vec<Vec<u8>> = keys.iter().skip(task % 4).step_by(3).cloned().collect();
            if task % 2 == 1 {
                mine.reverse();
            }
            tasks.push(nabedra_rt::task::spawn(async move {
                for _ in 0..50 {
                    let deadline = nabedra_rt::time::now() + Duration::from_secs(10);
                    let guard = latches
                        .lock(mine.iter().map(Vec::as_slice), deadline)
                        .await
                        .expect("no deadlock");
                    tokio::task::yield_now().await;
                    drop(guard);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        // Every stripe is free afterwards.
        let all = latches
            .lock(
                keys.iter().map(Vec::as_slice),
                nabedra_rt::time::now() + Duration::from_millis(100),
            )
            .await
            .unwrap();
        assert!(all.waited.is_none());
    }

    #[test]
    fn stripes_are_sorted_and_distinct() {
        let latches = KeyLatches::new(4);
        let stripes = latches.stripes_of([&b"a"[..], b"b", b"c", b"a", b"zz", b"q"]);
        assert!(stripes.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
