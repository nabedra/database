use crate::protocol::{ErrorCode, Outcome, ProtocolError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    Invalid,
    Conflict,
    Overloaded,
    Unavailable,
    NotFound,
    Abandoned,
    Stale,
}
impl QueueError {
    pub fn protocol(self) -> ProtocolError {
        let (code, message, outcome) = match self {
            Self::Invalid => (
                ErrorCode::ValidationError,
                "invalid request queue record",
                Outcome::NotApplied,
            ),
            Self::Conflict => (
                ErrorCode::Conflict,
                "request identity has a different body",
                Outcome::NotApplied,
            ),
            Self::Overloaded => (
                ErrorCode::Overloaded,
                "request replica capacity exhausted",
                Outcome::NotApplied,
            ),
            Self::NotFound => (
                ErrorCode::NotFound,
                "receipt unavailable or expired",
                Outcome::NotApplied,
            ),
            Self::Abandoned => (
                ErrorCode::NotFound,
                "old-run request abandoned; check durable mutation decision",
                Outcome::Unknown,
            ),
            Self::Unavailable | Self::Stale => (
                ErrorCode::StorageUnavailable,
                "request replication or scheduler authority unavailable",
                Outcome::Unknown,
            ),
        };
        ProtocolError::new(code, message, outcome)
    }
}
