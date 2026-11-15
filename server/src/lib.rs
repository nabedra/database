//! MeshDB protocol, snapshot codec, storage, and production engine.
//! Runtime admission, HTTP envelopes, and transport are integrated separately.
#[cfg(test)]
extern crate self as meshdb_server;
pub mod api;
pub mod cli;
pub mod cluster;
pub mod commit;
pub mod config;
pub mod engine;
pub mod executor;
pub mod node;
pub mod partition_size;
pub mod protocol;
pub mod runtime;
pub mod snapshot;
pub mod storage;
#[cfg(test)]
#[path = "../tests/support/storage.rs"]
pub(crate) mod test_storage;
pub mod worker_pool;
