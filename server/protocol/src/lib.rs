//! Wire data and validation shared by Nabedra engines.

pub mod migration_interchange;
pub mod protocol;
pub mod query_semantics;
mod queue_error;

pub use queue_error::QueueError;
#[cfg(test)]
mod golden_tests;
