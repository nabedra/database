//! The seeded paused runtime of one simulation.
//!
//! Shim: `nabedra_rt::sim::runtime()` does not seed tokio's rng yet, and `World` does not
//! expose its virtual elapsed time. P10-RAFT adds `nabedra_rt::sim::runtime_seeded`; once it
//! lands, [`runtime_seeded`] and [`run`] forward to it and then go away.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use nabedra_rt::sim::{Installed, World};

/// True when this build seeds tokio's rng, so `select!` branch order follows the seed.
/// Needs `RUSTFLAGS="--cfg tokio_unstable"`; sweeps refuse to run without it.
pub const SEEDED: bool = cfg!(tokio_unstable);

/// A current-thread tokio runtime with paused time, for one simulation. Under
/// `--cfg tokio_unstable` tokio's rng is seeded from `seed` the way the Events simulator
/// seeds it.
pub fn runtime_seeded(seed: u64) -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_time().start_paused(true);
    #[cfg(tokio_unstable)]
    builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    #[cfg(not(tokio_unstable))]
    let _ = seed;
    builder.build()
}

/// Runs one simulation like `nabedra_rt::sim::run`, on [`runtime_seeded`]: a `World` seeded
/// with `seed` is installed on this thread while `body` runs.
pub fn run<F, Fut>(seed: u64, body: F) -> Fut::Output
where
    F: FnOnce(Arc<World>) -> Fut,
    Fut: Future,
{
    let runtime = runtime_seeded(seed).expect("build the simulation runtime");
    let world = runtime.block_on(async { World::new(seed) });
    let _installed = Installed::new(world.clone());
    runtime.block_on(body(world))
}

/// Virtual time elapsed since `world` started: the paused runtime clock, the same on every
/// node. Trace lines are stamped with it, never with a node's drifting clock.
pub fn virtual_elapsed(world: &World) -> Duration {
    // The runtime's own clock is what is meant here; node clocks drift.
    #[allow(clippy::disallowed_methods)]
    let now = tokio::time::Instant::now();
    now.saturating_duration_since(world.start())
}

/// [`virtual_elapsed`] in whole milliseconds.
pub fn virtual_ms(world: &World) -> u64 {
    virtual_elapsed(world).as_millis() as u64
}
