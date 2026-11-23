//! openraft 0.10's [`AsyncRuntime`] for simulated nodes (feature `sim-openraft`).
//!
//! [`SimRuntime`] is the runtime the server's openraft groups use in a simulation build:
//! the five OLTP groups (`oltp_replication/authority/config.rs`) and, from phase 12, the
//! Cache controller. Product builds keep openraft's tokio runtime; this module never
//! reaches them.
//!
//! It delegates to tokio, like `openraft_rt_tokio::TokioRuntime`, but routes the parts
//! that carry time, tasks or randomness through the runtime seam, so they follow the
//! simulated node that calls them:
//!
//! - [`SimInstant::now`] is [`crate::time::now`], the node's monotonic clock with its
//!   offset, drift and forward stalls ([`super::World::advance_mono`]). openraft's
//!   `C::now()` (leader lease, election and heartbeat deadlines,
//!   `oltp_replication/manager/system_recovery.rs`) therefore reads the node clock. A
//!   wall-clock jump (scenario O09) moves [`crate::time::wall_now`] only and leaves
//!   `C::now()` where it is.
//! - `sleep`, `sleep_until`, `timeout` and `timeout_at` go through [`crate::time`], which
//!   maps node durations and deadlines back to the runtime's paused clock.
//! - `spawn` and `spawn_blocking` go through [`crate::task`], so Raft cores join the
//!   node's task registry (a simulated crash aborts them) and blocking work runs inline.
//! - `thread_rng` is [`SimRng`], which draws from the node's seeded stream
//!   ([`crate::ids::random_u64`]). openraft draws each election timeout from it
//!   (`Config::new_rand_election_timeout`), so the timeouts, for example the Cache
//!   controller's 500 to 950 ms, are seeded per node.
//! - The channels and the mutex are tokio's, exactly as in `TokioRuntime`.
//!
//! Outside an installed [`super::World`] every call behaves as in `TokioRuntime`, except
//! that `spawn_blocking` uses tokio's blocking pool instead of a new OS thread.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

use openraft_rt::{AsyncRuntime, OptionalSend};
use openraft_rt_tokio::TokioRuntime;

use crate::{ids, task, time};

/// openraft's instant on the simulated node clock: a tokio instant read through
/// [`crate::time::now`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(pub tokio::time::Instant);

impl Add<Duration> for SimInstant {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + rhs)
    }
}

impl AddAssign<Duration> for SimInstant {
    #[inline]
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for SimInstant {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0 - rhs)
    }
}

impl Sub<Self> for SimInstant {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: Self) -> Duration {
        self.0 - rhs.0
    }
}

impl SubAssign<Duration> for SimInstant {
    #[inline]
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}

impl openraft_rt::Instant for SimInstant {
    /// The current node's monotonic clock. `elapsed` keeps the trait's default, which
    /// reads this clock too.
    #[inline]
    fn now() -> Self {
        Self(time::now())
    }
}

/// openraft's thread-local random number generator: each word is a draw from the current
/// node's seeded stream ([`crate::ids::random_u64`]), or from the world's stream outside
/// a node. It holds no state, so a copy never repeats values.
///
/// Not for keys, tokens or nonces, like [`crate::ids::random_u64`].
#[derive(Clone, Debug, Default)]
pub struct SimRng;

impl rand::TryRng for SimRng {
    type Error = Infallible;

    #[inline]
    fn try_next_u32(&mut self) -> Result<u32, Infallible> {
        Ok((ids::random_u64() >> 32) as u32)
    }

    #[inline]
    fn try_next_u64(&mut self) -> Result<u64, Infallible> {
        Ok(ids::random_u64())
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
        for chunk in dst.chunks_mut(8) {
            let word = ids::random_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
        Ok(())
    }
}

/// openraft 0.10's [`AsyncRuntime`] on the simulated node context. See the module
/// documentation.
///
/// openraft itself never constructs the runtime: Raft cores run on whatever runtime
/// spawns them, which in a simulation is the paused runtime of [`super::run`].
/// [`AsyncRuntime::new`] and [`AsyncRuntime::block_on`] exist for openraft's runtime test
/// suite and run on [`super::runtime_seeded`].
pub struct SimRuntime {
    runtime: tokio::runtime::Runtime,
}

impl SimRuntime {
    /// A runtime on [`super::runtime_seeded`]`(seed)`.
    pub fn seeded(seed: u64) -> Self {
        Self {
            runtime: super::runtime_seeded(seed).expect("build the simulation runtime"),
        }
    }
}

impl fmt::Debug for SimRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimRuntime").finish_non_exhaustive()
    }
}

impl AsyncRuntime for SimRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = SimInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = tokio::time::Timeout<T>;
    type ThreadLocalRng = SimRng;

    #[inline]
    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        task::spawn(future)
    }

    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        time::sleep(duration)
    }

    #[inline]
    fn sleep_until(deadline: SimInstant) -> Self::Sleep {
        time::sleep_until(deadline.0)
    }

    #[inline]
    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        time::timeout(duration, future)
    }

    #[inline]
    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: SimInstant,
        future: F,
    ) -> Self::Timeout<R, F> {
        time::timeout_at(deadline.0, future)
    }

    #[inline]
    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    #[inline]
    fn thread_rng() -> SimRng {
        SimRng
    }

    type Mpsc = <TokioRuntime as AsyncRuntime>::Mpsc;
    type Watch = <TokioRuntime as AsyncRuntime>::Watch;
    type Oneshot = <TokioRuntime as AsyncRuntime>::Oneshot;
    type Mutex<T: OptionalSend + 'static> = <TokioRuntime as AsyncRuntime>::Mutex<T>;

    /// A paused current-thread runtime seeded with 0. `threads` is ignored: a simulation
    /// runs on one thread.
    fn new(threads: usize) -> Self {
        let _ = threads;
        Self::seeded(0)
    }

    fn block_on<F, T>(&mut self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        self.runtime.block_on(future)
    }

    /// [`crate::task::spawn_blocking`]: inline and in the node context inside a world,
    /// instead of the trait default's new OS thread. A panic in `f` becomes an error, as
    /// with the default.
    fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, io::Error>> + Send
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let handle = task::spawn_blocking(f);
        async move { handle.await.map_err(io::Error::other) }
    }
}

#[cfg(test)]
mod tests {
    //! Each test fails when the property it names is broken. Run with and without
    //! `--cfg tokio_unstable` (see [`super::super::runtime_seeded`]).

    use std::time::Duration;

    use openraft_rt::testing::Suite;
    use openraft_rt::{AsyncRuntime, Instant as _, Mpsc, MpscReceiver as _, MpscSender as _};
    use rand::RngExt;

    use super::{SimInstant, SimRuntime};
    use crate::sim::{NodeClock, World, current_node, in_node, run};
    use crate::time;

    type Rt = SimRuntime;

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    /// What `openraft::Config::new_rand_election_timeout` does (openraft 0.10.0,
    /// `config/config.rs:709`): `RT::thread_rng().random_range(min..max)`.
    fn election_timeout(min: u64, max: u64) -> u64 {
        Rt::thread_rng().random_range(min..max)
    }

    /// The Cache controller's election timeouts (500 to 950 ms), 16 draws per node for
    /// nodes 1 to 3, drawn in the given node order.
    fn election_timeouts(seed: u64, order: &[u64]) -> Vec<(u64, Vec<u64>)> {
        run(seed, |world| async move {
            for node in 1..=3 {
                world.add_node(node, NodeClock::default());
            }
            let mut drawn: Vec<(u64, Vec<u64>)> = (1..=3).map(|node| (node, Vec::new())).collect();
            for _ in 0..16 {
                for &node in order {
                    let timeout = in_node(node, async { election_timeout(500, 950) }).await;
                    drawn[(node - 1) as usize].1.push(timeout);
                }
            }
            drawn
        })
    }

    #[test]
    fn election_timeouts_are_seeded_per_node() {
        let first = election_timeouts(7, &[1, 2, 3]);
        assert_eq!(first, election_timeouts(7, &[1, 2, 3]), "same seed");
        // Each node has its own stream: the order in which nodes draw does not matter.
        assert_eq!(first, election_timeouts(7, &[3, 1, 2]), "other draw order");
        assert_ne!(first, election_timeouts(8, &[1, 2, 3]), "other seed");
        assert_ne!(first[0].1, first[1].1, "nodes draw different timeouts");
        for (_, timeouts) in &first {
            assert!(
                timeouts.iter().all(|t| (500..950).contains(t)),
                "{timeouts:?}"
            );
            // Not a constant stream.
            assert!(timeouts.iter().any(|t| *t != timeouts[0]), "{timeouts:?}");
        }
    }

    #[test]
    fn now_follows_node_drift_and_stalls_but_not_wall_jumps() {
        run(11, |world| async move {
            // 10% fast, and undrifted.
            world.add_node(
                1,
                NodeClock {
                    drift_ppm: 100_000,
                    ..NodeClock::default()
                },
            );
            world.add_node(2, NodeClock::default());
            let now = |node| in_node(node, async { <Rt as AsyncRuntime>::Instant::now() });
            let (one, two) = (now(1).await, now(2).await);

            tokio::time::advance(ms(10_000)).await;
            assert_eq!(now(1).await - one, ms(11_000), "drift");
            assert_eq!(now(2).await - two, ms(10_000));

            // O09: wall jumps in either direction move the wall clock, not C::now().
            let wall = in_node(1, async { time::unix_ms() }).await;
            let before = now(1).await;
            world.jump_wall_forward(1, Duration::from_secs(3_600));
            world.jump_wall_backward(2, Duration::from_secs(3_600));
            assert_eq!(now(1).await, before);
            assert_eq!(now(2).await - two, ms(10_000));
            assert_eq!(
                in_node(1, async { time::unix_ms() }).await,
                wall + 3_600_000
            );

            // A stall moves the node's monotonic clock forward at once.
            world.advance_mono(1, ms(5_000));
            assert_eq!(now(1).await - before, ms(5_000));

            // openraft's timers run on the node clock: 1,100 ms on the fast node is
            // 1,000 ms of virtual time, and `elapsed` reads the node clock.
            let virtual_start = tokio::time::Instant::now();
            in_node(1, async {
                let start = SimInstant::now();
                Rt::sleep(ms(1_100)).await;
                assert_eq!(start.elapsed(), ms(1_100));
                Rt::sleep_until(SimInstant::now() + ms(1_100)).await;
                let late = Rt::timeout(ms(1_100), Rt::sleep(ms(5_000))).await;
                assert!(late.is_err());
            })
            .await;
            assert_eq!(virtual_start.elapsed(), ms(3_000));
        });
    }

    #[test]
    fn spawned_raft_tasks_join_the_node() {
        run(12, |world| async move {
            world.add_node(4, NodeClock::default());
            let (spawned, blocking) = in_node(4, async {
                let spawned = Rt::spawn(async { current_node() }).await.unwrap();
                let blocking = Rt::spawn_blocking(current_node).await.unwrap();
                (spawned, blocking)
            })
            .await;
            assert_eq!((spawned, blocking), (Some(4), Some(4)));

            let failed = in_node(4, async {
                Rt::spawn_blocking(|| -> u8 { panic!("blocking work failed") }).await
            })
            .await;
            assert!(failed.is_err());

            // A crash aborts a Raft core spawned through the runtime.
            // The handle is the point: the task is awaited after the crash.
            #[allow(clippy::async_yields_async)]
            let core = in_node(4, async { Rt::spawn(std::future::pending::<()>()) }).await;
            tokio::task::yield_now().await;
            assert_eq!(world.live_tasks(4), 1);
            world.crash(4);
            let error = core.await.unwrap_err();
            assert!(error.is_cancelled() && !Rt::is_panic(&error));
        });
    }

    /// openraft's own conformance suite for runtimes, outside any world: the product
    /// fallbacks, `new` and `block_on`, and openraft's deterministic-rng wrapper.
    #[test]
    fn passes_the_openraft_runtime_suite() {
        Suite::<Rt>::test_all();
    }

    /// The parts of openraft's suite that read time, spawn or draw, on a drifted node with
    /// a monotonic offset.
    #[test]
    fn passes_the_openraft_runtime_suite_on_a_drifted_node() {
        run(13, |world| async move {
            world.add_node(
                9,
                NodeClock {
                    mono_offset: Duration::from_secs(30),
                    drift_ppm: -250_000,
                    wall_offset_nanos: -1_000_000_000,
                },
            );
            in_node(9, async {
                Suite::<Rt>::test_spawn_join_handle().await;
                Suite::<Rt>::test_thread_rng().await;
                Suite::<Rt>::test_sleep().await;
                Suite::<Rt>::test_instant().await;
                Suite::<Rt>::test_instant_arithmetic().await;
                Suite::<Rt>::test_instant_sub_instant().await;
                Suite::<Rt>::test_instant_saturating_duration_since().await;
                Suite::<Rt>::test_instant_ord().await;
                Suite::<Rt>::test_sleep_until().await;
                Suite::<Rt>::test_timeout().await;
                Suite::<Rt>::test_timeout_at().await;
                Suite::<Rt>::test_mpsc_send().await;
                Suite::<Rt>::test_mpsc_backpressure().await;
                Suite::<Rt>::test_oneshot_send_from_another_task().await;
                Suite::<Rt>::test_mutex_contention().await;
                Suite::<Rt>::test_watch_wait_loop_pattern().await;
            })
            .await;
        });
    }

    /// One trace entry: virtual nanoseconds since the world started when the node's timer
    /// fired, node, round, the election timeout the node drew, and an id the node drew
    /// next.
    type Trace = Vec<(u128, u64, u32, u64, uuid::Uuid)>;

    /// Three nodes with different drifts run an election-timer loop on the runtime: draw
    /// a timeout, sleep until it on the node clock, draw an id, report through an
    /// openraft channel. Node 3 crashes at 4 s of virtual time.
    fn election_trace(seed: u64) -> Trace {
        run(seed, |world| async move {
            let drifts = [(1, 0), (2, 50_000), (3, -30_000)];
            for (node, drift_ppm) in drifts {
                world.add_node(
                    node,
                    NodeClock {
                        drift_ppm,
                        ..NodeClock::default()
                    },
                );
            }
            let start = world.start();
            let (tx, mut rx) = <<Rt as AsyncRuntime>::Mpsc as Mpsc>::channel(64);
            for (node, _) in drifts {
                let tx = tx.clone();
                in_node(node, async move {
                    #[allow(clippy::let_underscore_future)]
                    let _ = Rt::spawn(async move {
                        for round in 0_u32.. {
                            let timeout = election_timeout(500, 950);
                            Rt::sleep_until(SimInstant::now() + ms(timeout)).await;
                            let fired = tokio::time::Instant::now().duration_since(start);
                            let id = crate::ids::uuid_v4();
                            if tx
                                .send((fired.as_nanos(), node, round, timeout, id))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    });
                })
                .await;
            }
            drop(tx);
            let crash_at = start + Duration::from_secs(4);
            let end = start + Duration::from_secs(10);
            let mut crashed = false;
            let mut trace = Trace::new();
            loop {
                tokio::select! {
                    biased;
                    () = tokio::time::sleep_until(crash_at), if !crashed => {
                        world.crash(3);
                        crashed = true;
                    }
                    entry = tokio::time::timeout_at(end, rx.recv()) => match entry {
                        Ok(Some(entry)) => trace.push(entry),
                        _ => break,
                    },
                }
            }
            assert!(crashed);
            assert_eq!(world.live_tasks(3), 0);
            let crash_nanos = Duration::from_secs(4).as_nanos();
            assert!(
                trace
                    .iter()
                    .all(|entry| entry.1 != 3 || entry.0 <= crash_nanos),
                "a crashed node's Raft task kept running: {trace:?}"
            );
            trace
        })
    }

    #[test]
    fn same_seed_gives_the_same_trace() {
        let first = election_trace(21);
        assert!(first.len() > 20, "{first:?}");
        for node in 1..=3 {
            assert!(
                first.iter().any(|entry| entry.1 == node),
                "node {node} never fired"
            );
        }
        assert_eq!(first, election_trace(21));
        assert_ne!(first, election_trace(22));
    }

    #[test]
    fn runtime_seeded_is_paused_and_current_thread() {
        let runtime = crate::sim::runtime_seeded(5).unwrap();
        let wall = std::time::Instant::now();
        let flavor = runtime.block_on(async {
            tokio::time::sleep(Duration::from_secs(3_600)).await;
            tokio::runtime::Handle::current().runtime_flavor()
        });
        assert_eq!(flavor, tokio::runtime::RuntimeFlavor::CurrentThread);
        assert!(
            wall.elapsed() < Duration::from_secs(60),
            "time is not paused"
        );
        assert_eq!(crate::sim::TOKIO_RNG_SEEDED, cfg!(tokio_unstable));
        // `World::new` accepts it.
        let world = runtime.block_on(async { World::new(5) });
        assert_eq!(world.seed(), 5);
    }

    /// With `--cfg tokio_unstable`, the branch tokio's `select!` polls first follows the
    /// seed. Without the flag this cannot hold, so the test is compiled out.
    #[cfg(tokio_unstable)]
    #[test]
    fn select_order_follows_the_seed() {
        fn choices(seed: u64) -> Vec<u8> {
            let runtime = crate::sim::runtime_seeded(seed).unwrap();
            runtime.block_on(async {
                let mut picked = Vec::new();
                for _ in 0..64 {
                    picked.push(tokio::select! {
                        () = std::future::ready(()) => 0,
                        () = std::future::ready(()) => 1,
                        () = std::future::ready(()) => 2,
                    });
                }
                picked
            })
        }
        let first = choices(31);
        assert_eq!(first, choices(31));
        assert_ne!(first, choices(32));
        assert!(first.iter().any(|&branch| branch != first[0]));
    }
}
