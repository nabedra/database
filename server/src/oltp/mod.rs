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

pub(crate) fn valid_database(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}
