//! Nabedra's runtime seam: the clock, task spawning and ids.
//!
//! Server code reaches tokio, std and uuid through these functions instead of calling
//! them directly (`server/clippy.toml` bans the direct calls outside this crate). In a
//! product build every function is `#[inline]` and forwards to the direct call in one
//! line, so behaviour and generated code stay the same.
//!
//! With the `sim` feature the same functions read the simulated node context in
//! [`sim`]: a per-node monotonic offset and drift, a per-node wall clock with explicit
//! jumps (scenario O09), seeded per-node id streams, a per-node task registry so a
//! simulated crash aborts the node's tasks, and inline blocking work. The simulation
//! feature never reaches a product build; `server/src/main.rs` refuses to compile with it.

pub mod ids;
pub mod task;
pub mod time;

#[cfg(feature = "sim")]
pub mod sim;
