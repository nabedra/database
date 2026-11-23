//! Replication supplies proof of an owned consensus reply.
pub(crate) use crate::oltp::mvcc::store::consensus::{
    Applied, Consensus, ConsensusFuture, ReadBarrier,
};
use crate::oltp::{mvcc::store::consensus::AppliedReplyProof, pool::ScratchLease};
use nabedra_oltp_types::types::AppliedPosition;
use std::sync::Arc;

struct OwnedAppliedReply<R> {
    position: AppliedPosition,
    reply: R,
    owner: Arc<ScratchLease>,
}

impl<R> AppliedReplyProof<R> for OwnedAppliedReply<R> {
    fn into_parts(self) -> (AppliedPosition, R, Arc<ScratchLease>) {
        (self.position, self.reply, self.owner)
    }
}

pub(crate) fn owned_applied<R>(
    position: AppliedPosition,
    reply: R,
    owner: Arc<ScratchLease>,
) -> Applied<R> {
    Applied::from_proof(OwnedAppliedReply {
        position,
        reply,
        owner,
    })
}
