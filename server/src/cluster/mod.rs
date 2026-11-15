//! Bounded durable control records and initial storage access. No runtime authority.
pub(crate) mod admin;
pub(crate) mod admin_history;
pub(crate) mod attachment;
mod callback;
pub mod configuration;
pub(crate) mod controller;
pub(crate) mod drain;
pub(crate) mod heartbeat;
pub mod member;
pub(crate) mod membership;
pub(crate) mod provision;
pub mod runtime;
pub mod state;
pub mod store;
// Remove the allowance when startup callers land.
#[allow(dead_code)]
pub(crate) mod startup_config;
pub(crate) mod startup_material;
