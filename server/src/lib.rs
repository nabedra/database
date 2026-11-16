//! Nabedra protocol and mutually exclusive execution engines.
#[cfg(all(feature = "olap", feature = "oltp"))]
compile_error!("select exactly one engine: olap or oltp");
#[cfg(not(any(feature = "olap", feature = "oltp")))]
compile_error!("select exactly one engine: olap or oltp");

#[cfg(test)]
extern crate self as nabedra_database_server;
#[cfg(feature = "olap")]
pub mod api;
pub mod cli;
#[cfg(feature = "olap")]
pub mod cluster;
#[cfg(feature = "olap")]
pub mod commit;
pub mod config;
#[cfg(feature = "olap")]
pub mod engine;
pub mod executor;
#[cfg(feature = "olap")]
pub mod maintenance;
pub(crate) mod network;
#[cfg(feature = "olap")]
pub mod node;
#[cfg(feature = "oltp")]
pub mod oltp;
#[cfg(feature = "oltp")]
pub mod oltp_replication;
#[cfg(feature = "oltp")]
pub mod oltp_runtime;
pub mod partition_size;
pub mod protocol;
pub(crate) mod query_semantics;
pub mod request_queue;
#[cfg(feature = "oltp")]
#[path = "api/response.rs"]
pub mod response;
#[cfg(feature = "olap")]
pub mod runtime;
pub mod scheduling;
#[cfg(feature = "olap")]
pub mod snapshot;
#[cfg(feature = "olap")]
pub mod storage;
#[cfg(not(feature = "olap"))]
#[path = "storage/shared.rs"]
pub mod storage;
#[cfg(all(test, feature = "olap"))]
#[path = "../tests/support/storage.rs"]
pub(crate) mod test_storage;
pub mod worker_pool;

/// The compile-time execution engine reported by deployment tooling.
pub const ENGINE: &str = if cfg!(feature = "oltp") {
    "oltp"
} else {
    "olap"
};
