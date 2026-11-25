//! The search index of one worker. Protocols: `specs/vector/`, design: `plans/oltp-vector.md`.
#![forbid(unsafe_code)]

pub mod index;
pub mod quant;
pub mod types;
