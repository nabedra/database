//! Cluster request limits and per-node resource admission.

use nabedra_protocol::protocol;

pub mod config;
#[doc(hidden)]
pub mod memory;
pub mod partition_size;
pub mod scheduling;
pub mod stage_timing;
