//! openraft 0.9's `AsyncRuntime` for simulation.
//!
//! It delegates to `nabedra_rt`, whose `sim` feature reads the `nabedra_rt::sim` world:
//! `Instant` reads the current node's monotonic clock, so a clock jump moves that node's
//! election and lease timers; `ThreadLocalRng` draws from the node's seeded stream, which
//! seeds election timeouts; and `spawn` registers each task with its node, so a crash
//! aborts the node's Raft core, replication and state machine tasks. It stays in Events
//! because Events alone uses openraft 0.9; the OLTP simulator's openraft 0.10 runtime is
//! separate.

use std::future::Future;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

use openraft::async_runtime::TokioOneShotSender;
use openraft::{AsyncRuntime, OptionalSend, TokioInstant};

/// A tokio instant read on the current node's monotonic clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimInstant(TokioInstant);

impl openraft::Instant for SimInstant {
    fn now() -> Self {
        Self(nabedra_rt::time::now())
    }
}

impl Add<Duration> for SimInstant {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + rhs)
    }
}

impl AddAssign<Duration> for SimInstant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for SimInstant {
    type Output = Self;
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0 - rhs)
    }
}

impl Sub<SimInstant> for SimInstant {
    type Output = Duration;
    fn sub(self, rhs: SimInstant) -> Duration {
        self.0.saturating_duration_since(rhs.0)
    }
}

impl SubAssign<Duration> for SimInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}

/// The current node's seeded random stream.
pub struct SimRng;

impl rand::RngCore for SimRng {
    fn next_u32(&mut self) -> u32 {
        nabedra_rt::ids::random_u64() as u32
    }

    fn next_u64(&mut self) -> u64 {
        nabedra_rt::ids::random_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let bytes = nabedra_rt::ids::random_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SimRuntime;

impl AsyncRuntime for SimRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = SimInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = tokio::time::Timeout<T>;
    type ThreadLocalRng = SimRng;
    type OneshotSender<T: OptionalSend> = TokioOneShotSender<T>;
    type OneshotReceiver<T: OptionalSend> = tokio::sync::oneshot::Receiver<T>;
    type OneshotReceiverError = tokio::sync::oneshot::error::RecvError;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        nabedra_rt::task::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        nabedra_rt::time::sleep(duration)
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        nabedra_rt::time::sleep_until(deadline.0)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        nabedra_rt::time::timeout(duration, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: Self::Instant,
        future: F,
    ) -> Self::Timeout<R, F> {
        nabedra_rt::time::timeout_at(deadline.0, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        SimRng
    }

    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where
        T: OptionalSend,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (TokioOneShotSender(tx), rx)
    }
}
