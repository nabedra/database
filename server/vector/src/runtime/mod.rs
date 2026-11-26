//! The shared-nothing runtime of a search worker: one pinned thread per core that owns its state,
//! priority classes with admission before work starts, and budgets for queries and code memory
//! (plan sections 5 and 7).

mod budget;
mod cores;
mod sched;

#[cfg(test)]
mod tests;

pub use budget::{Budget, Memory, Meter, Tier, Usage};
pub use cores::{Cores, Ctx, Job};
pub use sched::{APPLY_SHARE, Class, Limits, Scheduler};
