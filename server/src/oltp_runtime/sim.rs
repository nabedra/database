//! Configuration and process control for an in-process OLTP simulation.
use std::{cell::RefCell, future::Future, time::Duration};
use tokio::{io::DuplexStream, sync::oneshot};

tokio::task_local! {
    static SUPERVISOR: RefCell<Option<DuplexStream>>;
    static WORKER_STOP: RefCell<Option<oneshot::Receiver<()>>>;
}

pub async fn with_supervisor<F: Future>(channel: DuplexStream, process: F) -> F::Output {
    SUPERVISOR.scope(RefCell::new(Some(channel)), process).await
}

pub async fn with_worker_stop<F: Future>(stop: oneshot::Receiver<()>, process: F) -> F::Output {
    WORKER_STOP.scope(RefCell::new(Some(stop)), process).await
}

pub(super) fn supervisor() -> Option<DuplexStream> {
    SUPERVISOR
        .try_with(|channel| channel.borrow_mut().take())
        .ok()
        .flatten()
}

pub(super) fn worker_stop() -> Option<oneshot::Receiver<()>> {
    WORKER_STOP
        .try_with(|stop| stop.borrow_mut().take())
        .ok()
        .flatten()
}

/// One immutable set of limits, installed as a World extension before startup.
#[derive(Clone, Copy)]
pub struct Limits {
    pub direct_deadline: Duration,
    pub direct_max_in_flight: usize,
    pub direct_batch: bool,
    pub data_apply_reply_bytes: usize,
}

pub(super) fn limits() -> Option<Limits> {
    nabedra_rt::sim::world()?.with_ext::<Limits, _>(|limits| *limits)
}
