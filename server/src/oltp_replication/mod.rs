//! Durable per-group consensus components: typed Raft groups hosted in shared
//! pools, their multiplexed authenticated transport and the authority services.
pub(crate) mod authority;
pub mod entry;
pub(crate) mod hosted;
pub(crate) mod manager;
pub use nabedra_transport as multiplex;
pub mod owned;
mod peer_directory;
mod proof;
pub(crate) mod service_rpc;
pub mod typed_log;
pub(crate) mod typed_network;
pub mod types;
pub(crate) use peer_directory::AppliedPeerDirectory;
pub use proof::ReadySetHasher;
pub use nabedra_transport::tls::TlsMaterial;

pub(crate) fn raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(hosted::SNAPSHOT_LOGS_SINCE_LAST),
        max_in_snapshot_log_to_keep: hosted::SNAPSHOT_LOGS_TO_KEEP,
        ..Default::default()
    }
}
