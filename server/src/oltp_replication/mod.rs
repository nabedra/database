//! Durable per-group consensus components: typed Raft groups hosted in shared
//! pools, their multiplexed authenticated transport and the authority services.
pub(crate) mod authority;
pub mod entry;
pub(crate) mod hosted;
pub(crate) mod manager;
pub use nabedra_transport as multiplex;
pub mod owned;
mod proof;
pub(crate) mod service_rpc;
mod peer_directory;
pub mod typed_log;
pub(crate) mod typed_network;
pub mod types;
pub use proof::ReadySetHasher;
pub use nabedra_transport::tls::TlsMaterial;
