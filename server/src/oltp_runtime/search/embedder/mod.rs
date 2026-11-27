//! Owner: K. The `nabedra-db search embedder --config embed.toml` process
//! (leases, batched provider calls, circuit breaker, egress rules) and the
//! operator command `retry-embeddings` (role `Worker`, contract-w2 Q13).
//!
//! Skeleton (S): the commands exist and answer that they are not built yet.

/// `nabedra-db search embedder`.
#[derive(Debug, clap::Args)]
pub struct EmbedderOptions {
    /// The embedder's configuration (`embed.toml`).
    #[arg(long)]
    pub config: std::path::PathBuf,
}

/// `nabedra-db search retry-embeddings`, run on a data worker host.
#[derive(Debug, clap::Args)]
pub struct RetryOptions {
    /// The data worker's bootstrap file (its worker TLS and Control).
    #[arg(long)]
    pub bootstrap: std::path::PathBuf,
    /// The table whose failed embeddings are requeued.
    #[arg(long)]
    pub table: String,
    /// Only this column; all embedded columns when absent.
    #[arg(long)]
    pub column: Option<String>,
}

pub async fn run(_options: EmbedderOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}

pub async fn retry(_options: RetryOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}
