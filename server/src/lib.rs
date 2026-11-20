//! Nabedra protocol and mutually exclusive execution engines.
#[cfg(any(
    all(feature = "olap", feature = "oltp"),
    all(feature = "olap", feature = "cache"),
    all(feature = "oltp", feature = "cache"),
    not(any(feature = "olap", feature = "oltp", feature = "cache")),
))]
compile_error!("select exactly one engine: olap, oltp, or cache");

#[cfg(all(feature = "cache", feature = "s3"))]
compile_error!("the cache engine does not support S3");

#[cfg(feature = "s3")]
pub(crate) mod aws_http;
#[cfg(feature = "cache")]
pub mod cache;

#[cfg(test)]
extern crate self as nabedra_database_server;
#[cfg(feature = "olap")]
pub mod api;
pub mod cli;
#[cfg(feature = "olap")]
pub mod cluster;
#[cfg(feature = "olap")]
pub mod commit;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod config;
#[cfg(feature = "olap")]
pub mod engine;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod executor;
#[cfg(feature = "olap")]
pub mod maintenance;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod migration_interchange;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub(crate) mod network;
#[cfg(feature = "olap")]
pub mod node;
#[cfg(feature = "olap")]
pub mod olap_export;
#[cfg(feature = "oltp")]
pub mod oltp;
#[cfg(all(test, feature = "oltp"))]
mod oltp_acceptance;
#[cfg(feature = "oltp")]
pub(crate) mod oltp_backup_repository;
#[cfg(feature = "oltp")]
pub mod oltp_deployment;
#[cfg(feature = "oltp")]
pub mod oltp_replication;
#[cfg(feature = "oltp")]
pub mod oltp_runtime;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod partition_size;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod protocol;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub(crate) mod query_semantics;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod request_queue;
#[cfg(feature = "oltp")]
#[path = "api/response.rs"]
pub mod response;
#[cfg(feature = "olap")]
pub mod runtime;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod scheduling;
#[cfg(feature = "olap")]
pub mod snapshot;
#[cfg(feature = "olap")]
pub mod storage;
#[cfg(all(feature = "oltp", not(feature = "olap")))]
#[path = "storage/shared.rs"]
pub mod storage;
#[cfg(all(unix, any(feature = "olap", feature = "oltp")))]
pub(crate) mod supervisor;
#[cfg(all(test, feature = "olap"))]
#[path = "../tests/support/storage.rs"]
pub(crate) mod test_storage;
#[cfg(any(feature = "olap", feature = "oltp"))]
pub mod worker_pool;

/// The compile-time execution engine reported by deployment tooling.
pub const ENGINE: &str = if cfg!(feature = "cache") {
    "cache"
} else if cfg!(feature = "oltp") {
    "oltp"
} else {
    "olap"
};
