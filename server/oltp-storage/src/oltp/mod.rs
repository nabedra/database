//! OLTP storage: typed group stores in shared native pools, MVCC, backups and
//! the shared helpers they use. The standalone RF1 engine has been removed;
//! every runtime opens pools through `WorkerRuntime`.
mod codec;
#[cfg(test)]
mod migration_acceptance;
mod native;
pub use native::NativeThreadBudget;
#[doc(hidden)]
pub mod backup;
pub mod mvcc;
pub mod pool;
mod query;
#[doc(hidden)]
pub mod read_attempt;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub mod test_hooks;

use crate::protocol::{ErrorCode, Outcome, ProtocolError};

#[doc(hidden)]
pub fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

#[doc(hidden)]
pub use nabedra_oltp_types::valid_database;

#[doc(hidden)]
pub fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}
