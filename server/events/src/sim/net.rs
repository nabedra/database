//! The in-memory peer network that replaces reqwest and the axum peer router.
//!
//! Each call is planned with the world's network rng when it is sent: dropped (a black
//! hole until the sender's RPC deadline), delayed, or delivered twice. A partition blocks
//! directed links. A crashed target refuses at once, like a closed port. A paused node
//! holds what it sends and receives until it resumes. The request runs as a task of the
//! target node, so a crash of the target aborts it; the reply crosses the same filters.

use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::{World, current_node, world};
use crate::raft::Broker;

const BASE_DELAY_MS: u64 = 1;

/// What happens to one message.
#[derive(Debug)]
enum Plan {
    Drop,
    Deliver { delay_ms: u64, duplicate: bool },
}

fn plan(world: &World, from: u64, to: u64) -> Plan {
    let mut state = world.lock();
    state.messages += 1;
    if state.net.blocked.contains(&(from, to)) {
        return Plan::Drop;
    }
    let drop = state.net.drop_per_mille;
    let dup = state.net.dup_per_mille;
    let delay_max = state.net.delay_max_ms;
    let rng = &mut state.net_rng;
    if drop > 0 && rng.chance(drop) {
        return Plan::Drop;
    }
    let delay_ms = BASE_DELAY_MS
        + if delay_max > 0 {
            rng.below(delay_max + 1)
        } else {
            0
        };
    let duplicate = dup > 0 && rng.chance(dup);
    Plan::Deliver {
        delay_ms,
        duplicate,
    }
}

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
    let world = world()?;
    let from = current_node()?;
    let handler = Arc::new(handler);
    let exchange = async {
        world.wait_unpaused(from).await;
        let request = plan(&world, from, to);
        world.trace(format_args!("send n{from}->n{to} {rpc} {request:?}"));
        let Plan::Deliver {
            delay_ms,
            duplicate,
        } = request
        else {
            return pending::<Option<R>>().await;
        };
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let broker = world.broker(to)?;
        if world.lock().net.blocked.contains(&(from, to)) {
            return pending().await;
        }
        if duplicate {
            let (world, handler, broker) = (world.clone(), handler.clone(), broker.clone());
            let copy = async move {
                tokio::time::sleep(Duration::from_millis(delay_ms + 1)).await;
                world.wait_unpaused(to).await;
                let _ = handler(broker).await;
            };
            if let Some(target) = super::world() {
                target.spawn_on(to, copy);
            }
        }
        let (send, receive) = oneshot::channel();
        let serve = {
            let (world, handler) = (world.clone(), handler.clone());
            async move {
                world.wait_unpaused(to).await;
                let _ = send.send(handler(broker).await);
            }
        };
        world.spawn_on(to, serve);
        // A dropped receiver means the target crashed while serving: no reply ever comes.
        let Ok(reply) = receive.await else {
            return pending().await;
        };
        let response = plan(&world, to, from);
        world.trace(format_args!(
            "reply n{to}->n{from} {rpc} {response:?} {}",
            if reply.is_some() { "served" } else { "refused" }
        ));
        let Plan::Deliver { delay_ms, .. } = response else {
            return pending().await;
        };
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        world.wait_unpaused(from).await;
        if !world.is_up(from) {
            return pending().await;
        }
        reply
    };
    tokio::time::timeout(ttl, exchange).await.ok().flatten()
}
