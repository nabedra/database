//! RV-APPLY F4: every hosted Raft core is supervised.
//!
//! OpenRaft stops a core (`Fatal`) on any storage error it is handed. The
//! hosts retry the transient ones themselves (`hosted/transient.rs`), so a
//! stopped core means something else went wrong. Without a supervisor such a
//! replica stays registered and routed but dead, and its group runs one voter
//! short until the process restarts (T7b, D07).
//!
//! The supervisor watches the current core's metrics. When it stops and the
//! host was not shut down on purpose, it counts the stop
//! (`/v1/runtime/stats` `raft.fatal*`) and classifies it:
//!
//! - **damaged** (kv quarantined the group): the NS14 damage watcher
//!   (`stop_on_damage`) stops and reports the replica; nothing else to do.
//! - **recovery required** (the pool latched after an ambiguous write, O29):
//!   the replica stops routing and its host shuts down. The O29 reopen flow is
//!   the pool's reopen when the worker restarts; nothing in the process can
//!   reopen a latched pool, and restarting the core would only fail again.
//! - **corrupt** (a corrupt Raft record that kv did not quarantine): given up.
//! - **transient** (anything else, such as a capacity reservation that
//!   outlasted its retries, or a panic): the core restarts on the same log and
//!   state after a backoff (1 s, 2 s, 4 s). More than [`MAX_RESTARTS`] stops
//!   within [`RESTART_WINDOW`], or a failed restart, gives the replica up.
//!
//! Giving up stops routing to the replica, shuts its host down and reports it
//! through the damage sink so Control replaces it (`DamageSink::gave_up`).
use super::*;
use std::{collections::VecDeque, time::Duration};

pub(crate) const MAX_RESTARTS: usize = 3;
pub(crate) const RESTART_WINDOW: Duration = Duration::from_secs(600);
const FIRST_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FatalClass {
    Transient,
    RecoveryRequired,
    Damaged,
    Corrupt,
}
pub(crate) fn classify(
    pool: &crate::oltp::pool::OltpPool,
    group: uuid::Uuid,
    fatal: &str,
) -> FatalClass {
    if pool.is_group_quarantined(group) || pool.group_damage(group).is_some() {
        FatalClass::Damaged
    } else if pool.requires_recovery() {
        FatalClass::RecoveryRequired
    } else if fatal.contains("corrupt") && !fatal.contains("unavailable or corrupt") {
        // A record the host itself found corrupt ("corrupt durable vote",
        // "corrupt typed durable response", ...). kv's generic read failure
        // ("OLTP storage unavailable or corrupt") may pass, so it restarts.
        FatalClass::Corrupt
    } else {
        FatalClass::Transient
    }
}

type Routes = std::sync::Weak<Mutex<BTreeMap<uuid::Uuid, GroupRoutes>>>;

impl HostedGroupManager {
    pub(crate) fn raft_health(
        &self,
    ) -> crate::oltp_replication::hosted::transient::RaftHealthSnapshot {
        self.health.snapshot()
    }
    pub(super) fn supervise<C, M>(&self, running: &Managed<C, M>, node: u64)
    where
        C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
        M: TypedMachine<C>,
    {
        use crate::oltp_replication::hosted::transient::RaftHealthStats;
        let host = Arc::downgrade(running);
        let routes = Arc::downgrade(&self.routes);
        let sink = Arc::downgrade(&self.damage_sink);
        let stats = self.health.clone();
        tokio::spawn(async move {
            let mut restarts: VecDeque<tokio::time::Instant> = VecDeque::new();
            loop {
                let Some(running) = host.upgrade() else {
                    return;
                };
                let mut metrics = running.raft.metrics();
                drop(running);
                let fatal = loop {
                    let stopped = metrics
                        .borrow_watched()
                        .running_state
                        .as_ref()
                        .err()
                        .map(ToString::to_string);
                    if let Some(fatal) = stopped {
                        break fatal;
                    }
                    if metrics.changed().await.is_err() {
                        break metrics
                            .borrow_watched()
                            .running_state
                            .as_ref()
                            .err()
                            .map_or_else(
                                || "Raft core task ended".to_owned(),
                                ToString::to_string,
                            );
                    }
                };
                let Some(running) = host.upgrade() else {
                    return;
                };
                if running.is_closed() {
                    return;
                }
                let store = running.state.store();
                let identity = store.identity().clone();
                let class = classify(store.pool(), identity.group_id, &fatal);
                RaftHealthStats::bump(&stats.fatal);
                RaftHealthStats::bump(match class {
                    FatalClass::Transient => &stats.fatal_transient,
                    FatalClass::RecoveryRequired => &stats.fatal_recovery_required,
                    FatalClass::Damaged | FatalClass::Corrupt => &stats.fatal_damaged,
                });
                tracing::error!(group=%identity.group_id, kind=?identity.kind, node, ?class, %fatal,
                    "typed Raft core stopped");
                match class {
                    FatalClass::Damaged => return,
                    FatalClass::RecoveryRequired => {
                        tracing::error!(group=%identity.group_id, node,
                            "the replica's pool requires recovery; its host stops until the worker reopens the pool (O29)");
                        retire(&routes, &identity, node, &running).await;
                        return;
                    }
                    FatalClass::Corrupt => {
                        give_up(&routes, &sink, &stats, &identity, node, &running).await;
                        return;
                    }
                    FatalClass::Transient => {}
                }
                let now = tokio::time::Instant::now();
                restarts.retain(|at| now.duration_since(*at) < RESTART_WINDOW);
                if restarts.len() >= MAX_RESTARTS {
                    give_up(&routes, &sink, &stats, &identity, node, &running).await;
                    return;
                }
                let delay = FIRST_BACKOFF * (1u32 << restarts.len());
                drop(running);
                tokio::time::sleep(delay).await;
                let Some(running) = host.upgrade() else {
                    return;
                };
                if running.is_closed() {
                    return;
                }
                match running.restart().await {
                    Ok(()) => {
                        restarts.push_back(now);
                        RaftHealthStats::bump(&stats.restarts);
                    }
                    Err(error) => {
                        if running.is_closed() {
                            return;
                        }
                        tracing::error!(group=%identity.group_id, node, %error,
                            "typed Raft core could not restart");
                        give_up(&routes, &sink, &stats, &identity, node, &running).await;
                        return;
                    }
                }
            }
        });
    }
}

/// Stops routing to this host's replica and shuts the host down. Returns the
/// route it removed, or `None` when another host took the route meanwhile.
async fn retire<C, M>(
    routes: &Routes,
    identity: &GroupIdentity,
    node: u64,
    running: &Managed<C, M>,
) -> Option<ReplicaRoute>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    let removed = routes.upgrade().and_then(|routes| {
        let mut routes = routes.lock().ok()?;
        let entry = routes.get_mut(&identity.group_id)?;
        let same = entry.replicas.get(&node).is_some_and(|route| {
            std::ptr::addr_eq(Arc::as_ptr(&route.typed), Arc::as_ptr(running))
        });
        if !same {
            return None;
        }
        let route = entry.replicas.remove(&node)?;
        Some((entry.dispatcher.clone(), entry.services.clone(), route))
    });
    let route = match removed {
        Some((dispatcher, services, route)) => {
            if let Some(services) = services {
                let _ = services.retire(node).await;
            }
            let _ = dispatcher.retire(node).await;
            Some(route)
        }
        None => None,
    };
    if let Err(error) = running.shutdown().await {
        tracing::warn!(group=%identity.group_id, node, %error, "stopped Raft host shut down with an error");
    }
    route
}

async fn give_up<C, M>(
    routes: &Routes,
    sink: &std::sync::Weak<Mutex<Option<Arc<dyn DamageSink>>>>,
    stats: &crate::oltp_replication::hosted::transient::RaftHealthStats,
    identity: &GroupIdentity,
    node: u64,
    running: &Managed<C, M>,
) where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    crate::oltp_replication::hosted::transient::RaftHealthStats::bump(&stats.given_up);
    tracing::error!(group=%identity.group_id, node,
        "giving up a replica whose Raft core keeps stopping; it stops routing and is reported for replacement");
    let Some(route) = retire(routes, identity, node, running).await else {
        return;
    };
    let sink = sink
        .upgrade()
        .and_then(|sink| sink.lock().ok().and_then(|sink| sink.clone()));
    if let Some(sink) = sink {
        sink.gave_up(identity.clone(), route.member);
    }
}
