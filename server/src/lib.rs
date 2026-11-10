//! MeshDB protocol, snapshot codec, storage, and production engine.
//! Runtime admission, HTTP envelopes, and transport are integrated separately.
pub mod api;
pub mod cli;
pub mod commit;
pub mod config;
pub mod engine;
pub mod executor;
pub mod protocol;
pub mod runtime;
pub mod snapshot;
pub mod storage;
