//! Owner: J. The gateway side of `table.add_column`, `drop_column`,
//! `add_full_text`, `drop_full_text` and `table.search_status`
//! (contract-w2 2.6). Mutations follow the OD-3 rule of `search_ops`.
//!
//! Skeleton (S): unreachable while their `served` lines are false; the queued
//! path answers, as before.
use super::{DirectGateway, Routed};
use crate::protocol::{ProtocolError, Request};

pub(super) async fn route(
    _gateway: &DirectGateway,
    _request: &Request,
) -> Result<Routed, ProtocolError> {
    Ok(Routed::Queued)
}
