//! The gateway's search operations (contract-w2 4.2). `direct.rs` sends every
//! name in [`OPERATIONS`] here; each owner edits only its own arm below:
//! I `query.rank*` (`rank.rs`), J the column operations and
//! `table.search_status` (`search_ddl.rs`), K `table.embedding_status`
//! (`embedding_status.rs`, with its name in `OPERATIONS`).
//!
//! OD-3 rule for every search mutation (column operations, embedding requeue):
//! once a write may have been sent, the gateway never answers `not_applied`;
//! a refusal it gives itself before the definite answer (a deadline, an
//! unreachable route, a full lane) is `outcome_unknown` ([`unsettled`]). Only a
//! definite answer from the owner of the write passes through as it is.
use super::{DirectGateway, Routed};
use crate::protocol::{ErrorCode, Operation, Outcome, ProtocolError};

/// The operation names this module routes.
pub(super) const OPERATIONS: [&str; 7] = [
    "table.add_column",
    "table.drop_column",
    "table.add_full_text",
    "table.drop_full_text",
    "table.search_status",
    "query.rank",
    "query.rank_many",
];

pub(super) fn handles(name: &str) -> bool {
    OPERATIONS.contains(&name)
}

/// Refuses what this build does not serve (`check_served`), then hands the
/// request to its owner. An error is the request's final answer.
pub(super) async fn route(gateway: &DirectGateway, body: &[u8]) -> Result<Routed, ProtocolError> {
    let request = crate::protocol::parse_oltp_request(body)?;
    request.operation.check_served(gateway.vector())?;
    match &request.operation {
        // I
        Operation::Rank(_) | Operation::RankMany(_) => super::rank::route(gateway, &request).await,
        // J
        Operation::AddColumn(_)
        | Operation::DropColumn(_)
        | Operation::AddFullText(_)
        | Operation::DropFullText(_)
        | Operation::SearchStatus(_) => super::search_ddl::route(gateway, &request).await,
        // K: `table.embedding_status` (2.5) gets its arm with its operation.
        _ => Ok(Routed::Queued),
    }
}

/// OD-3: the answer for a gateway-side refusal of a search mutation that may
/// already have been sent. Mirrors `unsettled` of fx-oltp-direct's OD-3 commit
/// (4e1a47db), which this file cannot call before that merges.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn unsettled(error: ProtocolError) -> ProtocolError {
    if error.outcome == Outcome::NotApplied {
        ProtocolError::new(
            ErrorCode::OutcomeUnknown,
            "search request outcome unknown; retry the same request_id and body",
            Outcome::Unknown,
        )
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn od3_a_gateway_refusal_after_a_send_is_outcome_unknown() {
        let refused = ProtocolError::new(ErrorCode::Overloaded, "full", Outcome::NotApplied);
        let error = unsettled(refused);
        assert_eq!(error.code, ErrorCode::OutcomeUnknown);
        assert_eq!(error.outcome, Outcome::Unknown);
        let unknown = ProtocolError::new(ErrorCode::StorageUnavailable, "u", Outcome::Unknown);
        assert_eq!(unsettled(unknown.clone()), unknown);
    }

    /// TODO(J, K): for each search mutation (column operations, embedding
    /// requeue), send it, drop the reply (or let the deadline pass) and assert
    /// the gateway answers `outcome_unknown`, never `not_applied`.
    #[test]
    #[ignore = "TODO(J, K): OD-3 for search mutations, once they are served"]
    fn od3_search_mutation_without_a_definite_answer_is_outcome_unknown() {
        todo!("J and K write this with their mutations")
    }
}
