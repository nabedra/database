//! Pure, captured-snapshot job contracts. Execution and framing are separate.
mod admission;
pub(crate) mod auth;
pub mod frame;
pub mod job;
pub mod payload;
pub mod spool;
// Remove this module-only allowance when payload stream callers are integrated.
#[allow(dead_code)]
mod payload_owner;
// Remove when receive transport is wired into the runtime.
#[allow(dead_code)]
mod payload_stream;

pub(crate) use payload_owner::LocalOutput;

mod initialization;

mod drain_observation;

mod coordinator;
pub(crate) mod http;
pub mod runtime;
