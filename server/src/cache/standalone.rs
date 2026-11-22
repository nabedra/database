use std::io;
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "test-hooks")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Semaphore};
use tokio::time::{self, MissedTickBehavior};

use super::command::DataCommand;
use super::config::{Config, Eviction, ListenerConfig};
use super::listener::{Backend, listen, shutdown};
use super::store::{
    EvictionPolicy, Operation, Reply, SlotStore, StoreError, TurnLimits, absolute_deadline,
};
use super::wire::Response;

pub(super) struct Standalone {
    pub(super) actor: Arc<Mutex<SlotStore>>,
    pending: Semaphore,
    pub(super) started: Instant,
    limits: TurnLimits,
    /// Cleared only by the test-hooks `DEBUG SET-ACTIVE-EXPIRE 0`.
    #[cfg(feature = "test-hooks")]
    pub(super) active_expire: Arc<AtomicBool>,
}

impl Standalone {
    pub(super) fn new(
        data_bytes: usize,
        eviction: EvictionPolicy,
        max_pending: usize,
        limits: TurnLimits,
    ) -> Self {
        Self {
            actor: Arc::new(Mutex::new(SlotStore::new(data_bytes, eviction))),
            pending: Semaphore::new(max_pending),
            started: Instant::now(),
            limits,
            #[cfg(feature = "test-hooks")]
            active_expire: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Backend for Standalone {
    async fn execute(&self, command: DataCommand) -> Response {
        let Ok(_admission) = self.pending.try_acquire() else {
            return Response::error(b"BUSY cache actor queue is full");
        };
        // A connection on the actor's current-thread runtime can execute the
        // turn directly when the actor is idle. The mutex preserves exclusive
        // ownership and the semaphore bounds waiting before any state changes.
        let mut store = self.actor.lock().await;
        response(apply(
            &mut store,
            &command.into_operation(),
            self.started,
            self.limits,
        ))
    }

    #[cfg(feature = "test-hooks")]
    async fn debug(&self, command: super::test_hooks::DebugCommand) -> Response {
        use super::test_hooks::{DebugCommand, digest_response};
        let store = self.actor.lock().await;
        match command {
            DebugCommand::Digest => digest_response(store.digest(elapsed_ms(self.started))),
            DebugCommand::SetActiveExpire(enabled) => {
                self.active_expire.store(enabled, Ordering::Relaxed);
                Response::ok()
            }
            DebugCommand::Sleep(duration) => {
                // Hold the actor as Valkey's blocking sleep holds the server.
                time::sleep(duration).await;
                Response::ok()
            }
        }
    }
}

pub fn run(path: &Path) -> io::Result<()> {
    let config = Config::read(path)?;
    let eviction = match config.eviction {
        Eviction::Noeviction => EvictionPolicy::NoEviction,
        Eviction::SampledLru => EvictionPolicy::SampledLru,
    };
    let root = path.parent().unwrap_or_else(|| Path::new("."));
    let listener = config.transport(root)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(serve(listener, eviction))
}

async fn serve(config: ListenerConfig, eviction: EvictionPolicy) -> io::Result<()> {
    let socket = tokio::net::TcpListener::bind(config.listen).await?;
    let stop = shutdown()?;
    let backend = Standalone::new(
        config.limits.data_bytes,
        eviction,
        config.limits.max_pending,
        TurnLimits {
            growth_bytes: config.turn_reserve_bytes,
            reply_bytes: config.limits.max_reply_bytes,
        },
    );
    let actor = Arc::clone(&backend.actor);
    let maintenance = tokio::spawn(expire(
        Arc::clone(&actor),
        backend.started,
        #[cfg(feature = "test-hooks")]
        Arc::clone(&backend.active_expire),
    ));
    eprintln!(
        "nabedra-db: standalone cache listening on {}",
        socket.local_addr()?
    );
    let result = listen(socket, config, backend, stop).await;
    maintenance.abort();
    let _ = maintenance.await;
    eprintln!(
        "nabedra-db: cache actor stopped with {} charged bytes",
        actor.lock().await.charged_bytes()
    );
    result
}

pub(super) async fn expire(
    actor: Arc<Mutex<SlotStore>>,
    started: Instant,
    #[cfg(feature = "test-hooks")] active: Arc<AtomicBool>,
) {
    let mut expiry = time::interval(Duration::from_millis(10));
    expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        expiry.tick().await;
        let mut store = actor.lock().await;
        #[cfg(feature = "test-hooks")]
        if !active.load(Ordering::Relaxed) {
            continue;
        }
        let prepared = store.prepare_expiry(elapsed_ms(started), 64, Duration::from_micros(200));
        store.commit(prepared);
    }
}

fn apply(
    store: &mut SlotStore,
    operation: &Operation,
    started: Instant,
    limits: TurnLimits,
) -> Reply {
    if let Err(error) = validate_wall_clock_expiry(operation, || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    }) {
        return Reply::Error(error);
    }
    for _ in 0..2 {
        let mut prepared = store.prepare(operation, elapsed_ms(started), limits);
        if let Err(error) = store.reserve(&mut prepared) {
            return Reply::Error(error);
        }
        match prepared.refresh_time(elapsed_ms(started)) {
            Ok(()) => return store.commit(prepared),
            Err(StoreError::StaleTurn) => {}
            Err(error) => return Reply::Error(error),
        }
    }
    Reply::Error(StoreError::StaleTurn)
}

/// The standalone store uses monotonic time. Enforce Valkey's signed Unix
/// millisecond limit separately when executing the command. The cluster
/// store already checks the same limit against its Unix clock.
fn validate_wall_clock_expiry(
    operation: &Operation,
    unix_ms: impl FnOnce() -> u64,
) -> Result<(), StoreError> {
    let ttl = match operation {
        Operation::Set { ttl_ms, .. } => *ttl_ms,
        Operation::Expire { ttl_ms, .. } if *ttl_ms > 0 => Some(*ttl_ms as u64),
        _ => None,
    };
    if let Some(ttl) = ttl {
        absolute_deadline(unix_ms(), ttl)?;
    }
    Ok(())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::cache::store::SetWhen;

    #[test]
    fn k01_signed_unix_millisecond_expiry_boundary() {
        let now = 1_800_000_000_123;
        let max_ttl = i64::MAX - now as i64;
        for ttl in [max_ttl - 1, max_ttl, max_ttl + 1] {
            let expected = if ttl <= max_ttl {
                Ok(())
            } else {
                Err(StoreError::InvalidTtl)
            };
            let key = Bytes::from_static(b"k");
            for op in [
                Operation::Set {
                    key: key.clone(),
                    value: Bytes::from_static(b"v"),
                    ttl_ms: Some(ttl as u64),
                    when: SetWhen::Always,
                },
                Operation::Expire { key, ttl_ms: ttl },
            ] {
                assert_eq!(validate_wall_clock_expiry(&op, || now), expected, "{op:?}");
            }
        }
    }
}

pub(super) fn response(reply: Reply) -> Response {
    match reply {
        Reply::Ok => Response::simple(b"OK"),
        Reply::Bulk(value) => Response::Bulk(value),
        Reply::Integer(value) => Response::Integer(value),
        Reply::Array(values) => Response::Array(values.into_iter().map(Response::Bulk).collect()),
        Reply::Error(error) => Response::error(error.resp().as_bytes()),
    }
}
