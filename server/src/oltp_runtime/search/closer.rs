//! Owner: F. The leader task that proposes `Closed` once a second per range
//! while a search worker asked for floors (contract-w2 1.1 F, 2.3 `Close`).
//!
//! Skeleton (S): starts nothing.
use super::{DirectExecutor, DirectLeaderServices};
use std::sync::Arc;

pub(super) fn spawn(_services: &DirectLeaderServices, _executor: &Arc<DirectExecutor>) {}
