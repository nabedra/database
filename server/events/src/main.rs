use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use nabedra_events::control::{Config, run_backup, run_restore, serve};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = Cli::parse().command;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    match command {
        Command::Serve { config } => serve(Config::load(&config)?).await,
        Command::IssueGrant { config, grant } => {
            println!("{}", Config::load(&config)?.issue_grant(&grant)?);
            Ok(())
        }
        Command::Backup { config, output } => run_backup(&Config::load(&config)?, &output).await,
        Command::Restore { config, backup } => run_restore(&Config::load(&config)?, &backup).await,
    }
}
