//! Compatibility imports for replication and runtime callers.
pub use nabedra_transport::*;

#[path = "peer_directory.rs"]
mod peer_directory;
pub(crate) use peer_directory::{AppliedPeerDirectory, MAX_VIEW_REVOCATIONS};
#[cfg(test)]
pub(crate) use peer_directory::{REVOCATION_REFRESH, REVOCATION_STALE_AFTER, RevocationSource};
