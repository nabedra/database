//! The search index of one worker. Protocols: `specs/vector/`, design: `plans/oltp-vector.md`.
#![forbid(unsafe_code)]

pub mod impact;
pub mod index;
pub mod multi;
pub mod quant;
pub mod text;
pub mod runtime;
pub mod store;
pub mod types;
pub mod worker;
