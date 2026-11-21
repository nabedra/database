//! Leader services depend on this typed consensus contract. Implementations
//! must await durable application and use a quorum read barrier, never a local
//! leader flag. Ordinary worker endpoints do not receive this capability.
use crate::oltp::pool::ScratchLease;
use crate::{
    oltp_replication::{
        owned::{OwnedCommand, OwnedPayload},
        types::AppliedPosition,
    },
    protocol::ProtocolError,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{future::Future, pin::Pin};

pub(crate) type ConsensusFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Applied<R> {
    pub position: AppliedPosition,
    pub reply: R,
    #[serde(skip)]
    decode_owner: Option<Arc<ScratchLease>>,
}
impl<R> Applied<R> {
    pub(crate) fn owned(position: AppliedPosition, reply: R, owner: Arc<ScratchLease>) -> Self {
        Self {
            position,
            reply,
            decode_owner: Some(owner),
        }
    }
    pub(crate) fn into_parts(
        self,
    ) -> Result<(AppliedPosition, R, Arc<ScratchLease>), ProtocolError> {
        let owner = self.decode_owner.ok_or_else(|| {
            crate::protocol::ProtocolError::new(
                crate::protocol::ErrorCode::StorageUnavailable,
                "unowned Raft response",
                crate::protocol::Outcome::Unknown,
            )
        })?;
        Ok((self.position, self.reply, owner))
    }
}
impl<R: std::fmt::Debug> std::fmt::Debug for Applied<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Applied")
            .field("position", &self.position)
            .field("reply", &self.reply)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadBarrier {
    pub applied: AppliedPosition,
}

pub(crate) trait Consensus<C: OwnedPayload, R>: Send + Sync {
    fn propose(&self, command: OwnedCommand<C>) -> ConsensusFuture<'_, Applied<R>>;
    /// Complete only after ReadIndex and local applied-position advancement.
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier>;
    /// The same barrier, but a leader may answer from a valid leader lease
    /// and fall back to ReadIndex when the lease is stale (plan section 6).
    fn ensure_linearizable_lease(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.ensure_linearizable()
    }
    /// The node this replica last observed as leader, if any. A routing
    /// hint only, never an authority.
    fn current_leader(&self) -> Option<u64> {
        None
    }
    /// This replica's permanent node id, when it has one.
    fn local_node(&self) -> Option<u64> {
        None
    }
    /// The log index this replica has applied, from memory. A change hint
    /// for polling loops only, never a read barrier.
    fn applied_index(&self) -> Option<u64> {
        None
    }
}
