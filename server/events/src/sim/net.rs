//! The in-memory peer network that replaces reqwest and the axum peer router.
//!
//! Each call is planned with `nabedra_sim::NetPlan` when it is sent: dropped (a black hole
//! until the sender's RPC deadline), delayed, or delivered twice. A partition blocks
//! directed links. A crashed target refuses at once, like a closed port. A paused node
//! holds what it sends and receives until it resumes. The request runs as a task of the
//! target node, so a crash of the target aborts it; the reply crosses the same filters.

use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use nabedra_sim::Plan;
use tokio::sync::oneshot;

use super::{current_node, harness};
use crate::raft::Broker;

/// Sends one RPC from the current node to `to`. `handler` runs on the target broker and
/// returns `None` when the target refuses (no such group, votes disabled). Returns `None`
/// when the call is unreachable: refused, dropped, or past `ttl`.
pub(crate) async fn call<R, F, Fut>(
    to: u64,
    rpc: &'static str,
    ttl: Duration,
    handler: F,
) -> Option<R>
where
    R: Send + 'static,
    F: Fn(Arc<Broker>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Option<R>> + Send + 'static,
{
    let sim = harness()?;
    let from = current_node()?;
    let handler = Arc::new(handler);
    let exchange = async {
        sim.wait_unpaused(from).await;
        let request = sim.lock().net.plan(from, to);
        sim.trace(format_args!("send n{from}->n{to} {rpc} {request:?}"));
        let Plan::Deliver {
            delay_ms,
            duplicate,
        } = request
        else {
            return pending::<Option<R>>().await;
        };
        nabedra_rt::time::sleep(Duration::from_millis(delay_ms)).await;
        let broker = sim.broker(to)?;
        if sim.lock().net.is_blocked(from, to) {
            return pending().await;
        }
        if duplicate {
            let (copy_sim, handler, broker) = (sim.clone(), handler.clone(), broker.clone());
            let copy = async move {
                nabedra_rt::time::sleep(Duration::from_millis(delay_ms + 1)).await;
                copy_sim.wait_unpaused(to).await;
                let _ = handler(broker).await;
            };
            sim.world().spawn_node(to, copy);
        }
        let (send, receive) = oneshot::channel();
        let serve = {
            let (sim, handler) = (sim.clone(), handler.clone());
            async move {
                sim.wait_unpaused(to).await;
                let _ = send.send(handler(broker).await);
            }
        };
        sim.world().spawn_node(to, serve);
        // A dropped receiver means the target crashed while serving: no reply ever comes.
        let Ok(reply) = receive.await else {
            return pending().await;
        };
        let response = sim.lock().net.plan(to, from);
        sim.trace(format_args!(
            "reply n{to}->n{from} {rpc} {response:?} {}",
            if reply.is_some() { "served" } else { "refused" }
        ));
        let Plan::Deliver { delay_ms, .. } = response else {
            return pending().await;
        };
        nabedra_rt::time::sleep(Duration::from_millis(delay_ms)).await;
        sim.wait_unpaused(from).await;
        if !sim.is_up(from) {
            return pending().await;
        }
        reply
    };
    nabedra_rt::time::timeout(ttl, exchange)
        .await
        .ok()
        .flatten()
}
