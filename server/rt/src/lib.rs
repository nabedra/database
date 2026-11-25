//! Nabedra's runtime seam: the clock, task spawning, ids and the filesystem ([`fs`]).
//!
//! Server code reaches tokio, std and uuid through these functions instead of calling
//! them directly (`server/clippy.toml` bans the direct calls outside this crate). In a
//! product build every function is `#[inline]` and forwards to the direct call in one
//! line, so behaviour and generated code stay the same.
//!
//! With the `sim` feature the same functions read the simulated node context in
//! the `sim` module: a per-node monotonic offset and drift, a per-node wall clock with explicit
//! jumps (scenario O09), seeded per-node id streams, a per-node task registry so a
//! simulated crash aborts the node's tasks, and inline blocking work. The simulation
//! feature never reaches a product build; `server/src/main.rs` refuses to compile with it.

// This crate is the one place that makes the direct calls `server/clippy.toml` bans.
#![allow(clippy::disallowed_methods)]

pub mod fs;
pub mod ids;
#[cfg(feature = "net")]
pub mod net;
pub mod sync;
pub mod task;
pub mod time;

#[cfg(feature = "sim")]
pub mod sim;

/// True when the `sim` feature is enabled. `server/src/main.rs` asserts it is false, so
/// no simulation build produces a server binary however the feature was enabled.
pub const SIM: bool = cfg!(feature = "sim");

#[cfg(test)]
mod tests;

pub fn diag_on() -> bool { static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new(); *ON.get_or_init(|| std::env::var_os("NABEDRA_DIAG").is_some()) }
pub fn diag(msg: &str) {
    if !diag_on() { return; }
    #[cfg(feature = "sim")]
    let (node, ms) = (crate::sim::current_node(), crate::sim::world().map(|w| w.elapsed().as_millis()).unwrap_or(0));
    #[cfg(not(feature = "sim"))]
    let (node, ms) = (None::<u64>, 0u128);
    eprintln!("DIAG t={ms} n={node:?} {msg}");
}
pub fn diag_at(msg: &str, at: &std::panic::Location<'_>) { if diag_on() { diag(&format!("{msg} {}:{}", at.file(), at.line())); } }
