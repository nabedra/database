//! Owner: W. The `nabedra-db search worker --config search.toml` process:
//! enrollment, assignment polling, one feed client per range, snapshot install,
//! the barrier client, `SearchQuery` serving and reports (contract-w2 1.1 W).
//!
//! Skeleton (S): the command exists and answers that it is not built yet.

/// `nabedra-db search worker`.
#[derive(Debug, clap::Args)]
pub struct WorkerOptions {
    /// The search worker's configuration (`search.toml`).
    #[arg(long)]
    pub config: std::path::PathBuf,
}

pub async fn run(_options: WorkerOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}
