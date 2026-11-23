//! ClockBound fault model. The bound ages in the world's elapsed time, independently
//! of the signed rate and wall-clock reading of any node.

use std::{collections::BTreeMap, io, time::Duration};

use nabedra_rt::{
    sim::{self, World},
    time,
};

const NANOS_PER_MILLISECOND: i128 = 1_000_000;
const PARTS_PER_MILLION: u128 = 1_000_000;

#[derive(Clone, Copy)]
pub(super) struct BoundSpec {
    pub base_error_ns: u64,
    pub declared_drift_ppm: u32,
}

#[derive(Default)]
pub(super) struct SimClockBound {
    nodes: BTreeMap<u64, NodeBound>,
}

struct NodeBound {
    spec: BoundSpec,
    synced_at: Option<Duration>,
    reanchors_since_sync: u64,
    synchronized: bool,
}

impl SimClockBound {
    pub(super) fn sample(&self, world: &World) -> io::Result<(u64, u64)> {
        let node = sim::current_node()
            .ok_or_else(|| unavailable("simulated ClockBound requires a node context"))?;
        let bound = self.nodes.get(&node).ok_or_else(unsynchronized)?;
        let synced_at = bound
            .synced_at
            .filter(|_| bound.synchronized && world.is_up(node))
            .ok_or_else(unsynchronized)?;
        let age = world.elapsed().saturating_sub(synced_at).as_nanos();
        // A slow local clock understates elapsed time. Using the world's clock
        // bounds either drift sign, and rounding up covers fractional nanoseconds.
        let growth = (age * u128::from(bound.spec.declared_drift_ppm)).div_ceil(PARTS_PER_MILLION);
        let error = u64::try_from(
            u128::from(bound.spec.base_error_ns) + growth + u128::from(bound.reanchors_since_sync),
        )
        .map_err(|_| unavailable("simulated ClockBound error exceeds range"))?;
        let reading =
            u64::try_from(reading_ns()).map_err(|_| unavailable("cache clock exceeds range"))?;
        reading
            .checked_sub(error)
            .zip(reading.checked_add(error))
            .ok_or_else(|| unavailable("cache clock exceeds range"))
    }
}

// The Cache harness will configure this extension before starting its actors.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the Cache simulation harness configures the clock"
    )
)]
impl SimClockBound {
    pub(super) fn add_node(&mut self, node: u64, spec: BoundSpec) {
        self.nodes.insert(
            node,
            NodeBound {
                spec,
                synced_at: None,
                reanchors_since_sync: 0,
                synchronized: false,
            },
        );
    }

    /// Changes actual drift without changing the declared maximum. Reanchoring the
    /// world's node clock can discard a fraction of a nanosecond, so budget a whole
    /// nanosecond each time. Harness drift changes use this method.
    pub(super) fn set_drift_ppm(&mut self, world: &World, node: u64, ppm: i64) -> io::Result<()> {
        let bound = self.nodes.get_mut(&node).ok_or_else(unsynchronized)?;
        if !world.is_up(node) || ppm <= -(PARTS_PER_MILLION as i64) {
            return Err(unavailable("invalid simulated ClockBound drift change"));
        }
        world.set_drift_ppm(node, ppm);
        bound.reanchors_since_sync = bound.reanchors_since_sync.saturating_add(1);
        Ok(())
    }

    pub(super) fn set_synchronized(&mut self, node: u64, synchronized: bool) -> io::Result<()> {
        self.nodes
            .get_mut(&node)
            .ok_or_else(unsynchronized)?
            .synchronized = synchronized;
        Ok(())
    }

    /// Calibrates the current node at true time plus a residual within its base error.
    pub(super) fn resync(&mut self, world: &World, node: u64, residual_ns: i64) -> io::Result<()> {
        if sim::current_node() != Some(node) || !world.is_up(node) {
            return Err(unavailable(
                "simulated ClockBound requires a live node context",
            ));
        }
        let bound = self.nodes.get_mut(&node).ok_or_else(unsynchronized)?;
        if residual_ns.unsigned_abs() > bound.spec.base_error_ns {
            return Err(unavailable(
                "simulated ClockBound residual exceeds base error",
            ));
        }
        let now = world.elapsed();
        let truth = true_ns(now);
        let delta = truth + i128::from(residual_ns) - reading_ns();
        let magnitude = u64::try_from(delta.unsigned_abs())
            .map_err(|_| unavailable("simulated ClockBound resync exceeds range"))?;
        if delta < 0 {
            world.jump_wall_backward(node, Duration::from_nanos(magnitude));
        } else {
            world.jump_wall_forward(node, Duration::from_nanos(magnitude));
        }
        bound.synced_at = Some(now);
        bound.reanchors_since_sync = 0;
        bound.synchronized = true;
        Ok(())
    }
}

fn true_ns(elapsed: Duration) -> i128 {
    i128::from(sim::EPOCH_MS) * NANOS_PER_MILLISECOND + elapsed.as_nanos() as i128
}

fn reading_ns() -> i128 {
    match time::wall_now().duration_since(time::UNIX_EPOCH) {
        Ok(time) => time.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    }
}

fn unsynchronized() -> io::Error {
    unavailable("ClockBound is not synchronized")
}

fn unavailable(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, message)
}

#[cfg(test)]
mod tests;
