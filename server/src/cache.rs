//! Native RESP cache. Accepted database receipts and transactional storage are
//! implemented by the other engine builds.

mod admission;
mod auth;
mod command;
mod config;
mod listener;
mod standalone;
mod store;
mod wire;

pub use standalone::run as run_standalone;
