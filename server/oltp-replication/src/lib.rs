//! Nabedra OLTP consensus, deployment and archive repositories.

#[cfg(feature = "s3")]
mod aws_http;
#[doc(hidden)]
pub mod oltp_backup_repository;
pub mod oltp_deployment;
pub mod oltp_replication;

use nabedra_oltp_storage::oltp;
use nabedra_protocol::protocol;

// Shared server-side TLS fixtures use this name when included by Cargo tests.
#[cfg(any(test, feature = "test-hooks"))]
extern crate self as nabedra_database_server;
#[cfg(test)]
use nabedra_resources::config;
