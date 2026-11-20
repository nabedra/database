//! Feeds a Data group's leadership into its range clock (T4 rule 3, T5).
//!
//! Raft's blank and membership entries never reach `ParticipantMachine::apply`,
//! so the clock learns from the Raft metrics that this node leads a term
//! (`lead`), which entries applied (`observe_applied`), and that it stopped
//! leading (`step_down`). A clock that never saw `lead` refuses every
//! direct commit, so a missing watcher fails closed.
use super::{RaftConsensus, position};
use crate::oltp::mvcc::store::{ParticipantMachine, direct::RangeClock};
use crate::oltp_replication::authority::config::DataConfig;
use openraft::{ServerState, type_config::async_runtime::watch::WatchReceiver};
use std::sync::Arc;

/// Starts the watcher once per running Data replica. It ends when Raft
/// shuts down (the metrics channel closes) and steps the clock down then.
pub(crate) fn watch(consensus: &RaftConsensus<DataConfig, ParticipantMachine>) {
    consensus.direct_watch.call_once(|| {
        let node = consensus.state._resident.node_id();
        let clock = RangeClock::for_store(consensus.state.store());
        let metrics = consensus.raft.metrics();
        tokio::spawn(follow(metrics, node, clock));
    });
}

async fn follow(
    mut metrics: openraft::type_config::alias::WatchReceiverOf<
        DataConfig,
        openraft::RaftMetrics<DataConfig>,
    >,
    node: u64,
    clock: Arc<RangeClock>,
) {
    let mut leading = false;
    loop {
        {
            let current = metrics.borrow_watched();
            let leader =
                current.state == ServerState::Leader && current.current_leader == Some(node);
            if leader {
                // A no-op while the term is unchanged.
                clock.lead(current.current_term);
            } else if leading {
                clock.step_down();
            }
            leading = leader;
            // After `lead`, so the entry that ends rule 3's hold counts.
            if let Some(applied) = current.last_applied {
                clock.observe_applied(position(applied));
            }
        }
        if metrics.changed().await.is_err() {
            if leading {
                clock.step_down();
            }
            return;
        }
    }
}
