//! Owner: I. `query.rank` and `query.rank_many` on the gateway: route cache,
//! pruning, one worker per shard with one hedge, coverage tiling, global merge,
//! fusion, rerank, `snapshot` cuts from floors (contract-w2 2.4). Submodules
//! `rank/{route,coverage,merge,hedge,stats}.rs` are I's.
//!
//! Skeleton (S): unreachable while `served::RANK*` are false; the queued path
//! answers, as before.
use super::{DirectGateway, Routed};
use crate::protocol::{ProtocolError, Request};

pub(super) async fn route(
    _gateway: &DirectGateway,
    _request: &Request,
) -> Result<Routed, ProtocolError> {
    Ok(Routed::Queued)
}
