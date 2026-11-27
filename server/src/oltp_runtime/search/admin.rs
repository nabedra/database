//! Owner: J. Operator commands `nabedra-db search status|assign|model`
//! (contract-w2 2.6): read a table's search status, move a search shard
//! replica, and manage the embedding model registry in Control.
//!
//! Skeleton (S): the commands exist and answer that they are not built yet.

/// The connection every admin command uses.
#[derive(Debug, clap::Args)]
pub struct AdminTarget {
    /// A data worker's bootstrap file (its worker TLS and Control).
    #[arg(long)]
    pub bootstrap: std::path::PathBuf,
}

/// `nabedra-db search status`.
#[derive(Debug, clap::Args)]
pub struct StatusOptions {
    #[command(flatten)]
    pub target: AdminTarget,
    #[arg(long)]
    pub table: String,
}

/// `nabedra-db search assign`.
#[derive(Debug, clap::Args)]
pub struct AssignOptions {
    #[command(flatten)]
    pub target: AdminTarget,
    #[arg(long)]
    pub table: String,
    /// The search shard to change.
    #[arg(long)]
    pub shard: u32,
    /// The search worker to add.
    #[arg(long)]
    pub add: uuid::Uuid,
    /// The search worker to remove once the new one is ready.
    #[arg(long)]
    pub remove: Option<uuid::Uuid>,
}

/// `nabedra-db search model`.
#[derive(Debug, clap::Args)]
pub struct ModelOptions {
    #[command(flatten)]
    pub target: AdminTarget,
    /// A TOML file with one `EmbedModel` to set; absent with `--drop`.
    #[arg(long, conflicts_with = "drop")]
    pub set: Option<std::path::PathBuf>,
    /// The name of a model to drop.
    #[arg(long)]
    pub drop: Option<String>,
}

pub async fn status(_options: StatusOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}

pub async fn assign(_options: AssignOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}

pub async fn model(_options: ModelOptions) -> Result<(), &'static str> {
    Err(super::NOT_BUILT)
}
