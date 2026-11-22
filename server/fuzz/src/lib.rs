//! Decoder properties shared by cargo-fuzz and stable corpus replay.

#[cfg(feature = "cache")]
mod cache;
#[cfg(feature = "events")]
mod events;
#[cfg(feature = "olap")]
mod olap;

#[cfg(feature = "cache")]
pub use cache::{fuzz_cache_command, fuzz_cache_resp};
#[cfg(feature = "events")]
pub use events::{fuzz_events_ack, fuzz_events_lease, fuzz_events_poll, fuzz_events_publish};
#[cfg(feature = "olap")]
pub use olap::{fuzz_olap_catalog, fuzz_olap_operation, fuzz_olap_predicate};
