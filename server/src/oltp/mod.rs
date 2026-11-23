//! OLTP storage: typed group stores in shared native pools, MVCC, backups and
//! the shared helpers they use. The standalone RF1 engine has been removed;
//! every runtime opens pools through `WorkerRuntime`.
mod codec;
#[cfg(test)]
mod migration_acceptance;
mod native;
pub use native::NativeThreadBudget;
pub(crate) mod backup;
pub mod mvcc;
pub mod pool;
mod query;

use crate::protocol::{ErrorCode, Outcome, ProtocolError};

pub(crate) fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

pub(crate) use nabedra_oltp_types::valid_database;

fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}
