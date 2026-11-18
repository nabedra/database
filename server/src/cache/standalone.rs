use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, MissedTickBehavior};

use super::command::DataCommand;
use super::config::{Config, Eviction, ListenerConfig};
use super::listener::{Backend, listen, shutdown};
use super::store::{EvictionPolicy, Operation, Reply, SlotStore, StoreError, TurnLimits};
use super::wire::Response;

struct Turn {
    operation: Operation,
    completed: oneshot::Sender<Reply>,
}

struct Standalone(mpsc::Sender<Turn>);

impl Backend for Standalone {
    async fn execute(&self, command: DataCommand) -> Response {
        let (completed, reply) = oneshot::channel();
        let turn = Turn {
            operation: command.into_operation(),
            completed,
        };
        if self.0.try_send(turn).is_err() {
            return Response::error(b"BUSY cache actor queue is full");
        }
        match reply.await {
            Ok(reply) => response(reply),
            Err(_) => Response::error(b"CLUSTERDOWN cache actor stopped"),
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
    let (sender, incoming) = mpsc::channel(config.limits.max_pending);
    let actor = tokio::spawn(run_actor(
        incoming,
        SlotStore::new(config.limits.data_bytes, eviction),
        config.turn_reserve_bytes,
        config.limits.max_reply_bytes,
    ));
    eprintln!(
        "nabedra-db: standalone cache listening on {}",
        socket.local_addr()?
    );
    let result = listen(socket, config, Standalone(sender), stop).await;
    actor.await.map_err(io::Error::other)?;
    result
}

async fn run_actor(
    mut incoming: mpsc::Receiver<Turn>,
    mut store: SlotStore,
    reserve: usize,
    max_reply: usize,
) {
    let started = Instant::now();
    let mut expiry = time::interval(Duration::from_millis(10));
    expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = expiry.tick() => {
                let prepared = store.prepare_expiry(elapsed_ms(started), 64, Duration::from_micros(200));
                store.commit(prepared);
            }
            request = incoming.recv() => {
                let Some(Turn { operation, completed }) = request else {
                    break;
                };
                if completed.is_closed() {
                    continue;
                }
                let limits = TurnLimits { growth_bytes: reserve, reply_bytes: max_reply };
                let _ = completed.send(apply(&mut store, &operation, started, limits));
            }
        }
    }
    eprintln!(
        "nabedra-db: cache actor stopped with {} charged bytes",
        store.charged_bytes()
    );
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
