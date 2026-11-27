//! Owner: K. Leader side of the embedder: leases in leader RAM, `SetVector`
//! through the executor extension, `EmbedFailed` and `RequeueEmbedding`
//! (contract-w2 2.5).
//!
//! Skeleton (S): starts nothing.
use super::{DirectExecutor, DirectLeaderServices};
use std::sync::Arc;

pub(super) fn spawn(_services: &DirectLeaderServices, _executor: &Arc<DirectExecutor>) {}
