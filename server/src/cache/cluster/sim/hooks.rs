use std::io;

use nabedra_rt::sim;

use super::clock_bound::SimClockBound;

pub(in crate::cache::cluster) fn clock_bound() -> Option<io::Result<(u64, u64)>> {
    let world = sim::world()?;
    world.with_ext::<SimClockBound, _>(|bound| bound.sample(&world))
}

#[cfg(not(target_os = "linux"))]
pub(in crate::cache::cluster) fn has_clock_bound() -> bool {
    sim::world()
        .and_then(|world| world.with_ext::<SimClockBound, _>(|_| ()))
        .is_some()
}
