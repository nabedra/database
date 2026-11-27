//! Vector search processes and leader tasks (`plans/oltp-vector.md`, contract
//! `w3/contract-w2.md`). Owners: this file, `ports.rs` and `testkit.rs` are the
//! skeleton's (S); `worker/**` is W's, `embedder/**`, `embed_leader.rs` and
//! `query_embed.rs` are K's, `closer.rs` is F's, `admin.rs` is J's. Every
//! module is declared here already, so no owner edits this file.
//!
//! The `nabedra-db search ...` commands dispatch through [`run`].
pub mod admin;
mod closer;
mod embed_leader;
pub mod embedder;
pub(crate) mod ports;
mod query_embed;
#[cfg(test)]
pub(crate) mod testkit;
pub mod worker;

use super::{direct::DirectExecutor, worker_runtime::direct_capability::DirectLeaderServices};
use crate::cli::SearchCommand;
use std::sync::Arc;

/// The answer of a search command that its owner has not built yet.
pub(crate) const NOT_BUILT: &str = "this search command is not built yet";

/// Runs one `nabedra-db search` command.
pub async fn run(command: SearchCommand) -> Result<(), &'static str> {
    match command {
        SearchCommand::Worker(options) => worker::run(options).await,
        SearchCommand::Embedder(options) => embedder::run(options).await,
        SearchCommand::RetryEmbeddings(options) => embedder::retry(options).await,
        SearchCommand::Status(options) => admin::status(options).await,
        SearchCommand::Assign(options) => admin::assign(options).await,
        SearchCommand::Model(options) => admin::model(options).await,
    }
}

/// Called once a data worker's direct executor is installed (after
/// `data.direct.v1` is active): starts the search tasks of a range leader, the
/// closer (F) and the embedding lease state (K). Each task checks
/// `data.vector.v1` and leadership itself.
pub(crate) fn spawn_leader_tasks(services: &DirectLeaderServices, executor: &Arc<DirectExecutor>) {
    closer::spawn(services, executor);
    embed_leader::spawn(services, executor);
}
