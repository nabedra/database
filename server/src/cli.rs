//! Command parsing only. Bootstrap must pass serve options to Config::from_env.
use crate::config::ServeOptions;
use clap::{Parser, Subcommand};

/// MeshDB server command line.
#[derive(Debug, Parser)]
#[command(name = "meshdb-server", version, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve the MeshDB API.
    Serve(ServeOptions),
}
