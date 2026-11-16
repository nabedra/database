//! Command parsing only. Bootstrap validates options for the selected engine.
#[cfg(feature = "olap")]
use crate::config::ServeOptions;
#[cfg(feature = "oltp")]
use crate::oltp_runtime::ServeOptions;
use clap::{Parser, Subcommand};

/// Nabedra Database server command line.
#[derive(Debug, Parser)]
#[command(name = "nabedra-db", version = engine_version(), propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate local configuration and perform a read-only storage probe.
    Doctor {
        #[arg(long)]
        config: std::path::PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Explicit catalog maintenance after stopping incompatible writers.
    Database {
        #[command(subcommand)]
        command: DatabaseCommand,
    },
    /// Serve the Nabedra Database API.
    Serve(ServeOptions),
    /// Run the supervised cluster API.
    Api {
        #[arg(long)]
        config: std::path::PathBuf,
        /// Fence prior workers and begin a fresh in-memory request run.
        #[arg(long)]
        cold_start: bool,
        /// Confirm all old writers are stopped before upgrading a legacy catalog.
        #[arg(long)]
        migrate_catalog: bool,
    },
    /// Enroll and run a manually provisioned worker.
    Worker {
        #[arg(long)]
        bootstrap: std::path::PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum DatabaseCommand {
    Migrate {
        #[arg(long)]
        storage: String,
        #[arg(long)]
        region: String,
        #[arg(long)]
        cluster_id: uuid::Uuid,
        #[arg(long, required = true)]
        confirm_writers_stopped: bool,
    },
}

fn engine_version() -> &'static str {
    if cfg!(feature = "oltp") {
        concat!(env!("CARGO_PKG_VERSION"), " oltp")
    } else {
        concat!(env!("CARGO_PKG_VERSION"), " olap")
    }
}
