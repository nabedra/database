use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use nabedra_events::control::{
    Config, RECOVERY_EXIT_CODE, StoreRecoveryRequired, run_backup, run_restore, serve,
};

#[derive(Parser)]
#[command(version, about = "Run and operate Nabedra Events brokers")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    IssueGrant {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        grant: PathBuf,
    },
    Backup {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Restore {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        backup: PathBuf,
    },
    /// Authorize the first metadata voter, on its empty volume, to create a new cluster
    /// on its next start. Run once, before the first `serve`.
    Init {
        #[arg(long)]
        config: PathBuf,
    },
}

// Simulation swaps the Raft runtime, network and storage threading; it never ships.
#[cfg(feature = "sim")]
compile_error!("the `sim` feature is for simulation tests and must not build the broker binary");

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    match command {
        Command::Serve { config } => match serve(Config::load(&config)?).await {
            // A distinct code tells the supervisor to restart the broker: the next open
            // recovers the store or refuses it (RV-EV M2).
            Err(error) if error.is::<StoreRecoveryRequired>() => {
                tracing::error!(%error, code = RECOVERY_EXIT_CODE, "exiting for store recovery");
                std::process::exit(RECOVERY_EXIT_CODE)
            }
            result => result,
        },
        Command::IssueGrant { config, grant } => {
            println!("{}", Config::load(&config)?.issue_grant(&grant)?);
            Ok(())
        }
        Command::Backup { config, output } => run_backup(&Config::load(&config)?, &output).await,
        Command::Restore { config, backup } => run_restore(&Config::load(&config)?, &backup).await,
        Command::Init { config } => Config::load(&config)?.initialize(),
    }
}
