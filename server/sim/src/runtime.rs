//! The seeded paused runtime of one simulation.
use std::time::Duration;

use nabedra_rt::sim::World;

pub use nabedra_rt::sim::{TOKIO_RNG_SEEDED as SEEDED, run, runtime_seeded};

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
