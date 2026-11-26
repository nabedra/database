//! Feeds a Data group's leadership into its range clock (T4 rule 3, T5).
//!
//! Raft's blank and membership entries never reach `ParticipantMachine::apply`,
//! so the clock learns from the Raft metrics that this node leads a term
//! (`lead`), which entries applied (`observe_applied`), and that it stopped
//! leading (`step_down`). A clock that never saw `lead` refuses every
//! direct commit, so a missing watcher fails closed.
use super::{RaftConsensus, RaftCores, position};
use crate::oltp::mvcc::store::{ParticipantMachine, direct::RangeClock};
use crate::oltp_replication::authority::config::DataConfig;
use openraft::{ServerState, type_config::async_runtime::watch::WatchReceiver};
use std::sync::{Arc, Weak};

/// Starts the watcher once per running Data replica. It follows each core
/// the supervisor runs on the host's log and state (RV-APPLY F4): when a
/// core stops, the clock steps down, and when a restarted core replaces it
/// the watcher subscribes to the new core's metrics (RV-FINAL H1). It ends
/// when the host drops. It holds the cores weakly, so it never keeps a
/// stopped host alive.
#[doc(hidden)]
pub fn watch(consensus: &RaftConsensus<DataConfig, ParticipantMachine>) {
    consensus.direct_watch.call_once(|| {
        let node = consensus.state._resident.node_id();
        let clock = RangeClock::for_store(consensus.state.store());
        let restarts = consensus.raft.subscribe_restarts();
        nabedra_rt::task::spawn(follow_cores(
            Arc::downgrade(&consensus.raft),
            restarts,
            node,
            clock,
        ));
    });
}

async fn follow_cores(
    cores: Weak<RaftCores<DataConfig, ParticipantMachine>>,
    mut restarts: tokio::sync::watch::Receiver<usize>,
    node: u64,
    clock: Arc<RangeClock>,
) {
    loop {
        // The upgraded handle drops at the end of this statement.
        let Some((core, metrics)) = cores.upgrade().map(|cores| cores.current_metrics()) else {
            return;
        };
        follow(metrics, node, &clock).await;
        // The core stopped. A later core of this host runs on the same log
        // and state; the closed channel means the host dropped.
        if restarts.wait_for(|current| *current > core).await.is_err() {
            return;
        }
    }
}

/// Follows one core until it stops (its metrics report a stopped core or
/// the channel closes); the clock has stepped down when it returns.
async fn follow(
    mut metrics: openraft::type_config::alias::WatchReceiverOf<
        DataConfig,
        openraft::RaftMetrics<DataConfig>,
    >,
    node: u64,
    clock: &RangeClock,
) {
    let mut leading = false;
    loop {
        let stopped = {
            let current = metrics.borrow_watched();
            let stopped = current.running_state.is_err();
            let leader = !stopped
                && current.state == ServerState::Leader
                && current.current_leader == Some(node);
            if leader {
                // A no-op while the term is unchanged.
                clock.lead(current.current_term);
                #[cfg(feature = "sim")]
                if !leading {
                    nabedra_oltp_types::sim_probe::emit(|| nabedra_oltp_types::sim_probe::Probe::LeaderSeen {
                        group: clock.group(),
                        replica: node,
                        term: current.current_term,
                        leader: node,
                    });
                }
            } else if leading {
                clock.step_down();
            }
            leading = leader;
            // After `lead`, so the entry that ends rule 3's hold counts.
            if let Some(applied) = current.last_applied {
                clock.observe_applied(position(applied));
            }
            stopped
        };
        if stopped || metrics.changed().await.is_err() {
            if leading {
                clock.step_down();
            }
            return;
        }
    }
}
