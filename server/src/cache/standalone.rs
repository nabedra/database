use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Semaphore};
use tokio::time::{self, MissedTickBehavior};

use super::command::DataCommand;
use super::config::{Config, Eviction, ListenerConfig};
use super::listener::{Backend, listen, shutdown};
use super::store::{EvictionPolicy, Operation, Reply, SlotStore, StoreError, TurnLimits};
use super::wire::Response;

struct Standalone {
    actor: Arc<Mutex<SlotStore>>,
    pending: Semaphore,
    started: Instant,
    limits: TurnLimits,
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
    let actor = Arc::new(Mutex::new(SlotStore::new(
        config.limits.data_bytes,
        eviction,
    )));
    let started = Instant::now();
    let maintenance = tokio::spawn(expire(Arc::clone(&actor), started));
    let backend = Standalone {
        actor: Arc::clone(&actor),
        pending: Semaphore::new(config.limits.max_pending),
        started,
        limits: TurnLimits {
            growth_bytes: config.turn_reserve_bytes,
            reply_bytes: config.limits.max_reply_bytes,
        },
    };
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

async fn expire(actor: Arc<Mutex<SlotStore>>, started: Instant) {
    let mut expiry = time::interval(Duration::from_millis(10));
    expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        expiry.tick().await;
        let mut store = actor.lock().await;
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

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
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
