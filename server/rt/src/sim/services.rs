//! Stepped services in simulation (skeleton: product behaviour until P9-RT (b) lands).

use std::future::Future;
use std::panic::Location;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::task::{ServiceHandle, Stepped};

/// A doorbell: a generation counter and a tokio `Notify` the driver awaits.
#[derive(Debug, Default)]
pub struct Bell {
    generation: AtomicU64,
    notify: tokio::sync::Notify,
}

impl Bell {
    pub(crate) fn ring(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
pub(crate) enum Handle {
    Thread(std::thread::JoinHandle<()>),
}

impl Handle {
    pub(crate) fn join(self) -> std::thread::Result<()> {
        match self {
            Handle::Thread(thread) => thread.join(),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        match self {
            Handle::Thread(thread) => thread.is_finished(),
        }
    }

    pub(crate) fn is_current(&self) -> bool {
        match self {
            Handle::Thread(thread) => thread.thread().id() == std::thread::current().id(),
        }
    }
}

pub(crate) fn spawn_stepped<S: Stepped>(
    name: String,
    stack_bytes: usize,
    service: S,
) -> std::io::Result<ServiceHandle> {
    std::thread::Builder::new()
        .name(name)
        .stack_size(stack_bytes)
        .spawn(move || service.run())
        .map(|thread| ServiceHandle {
            inner: Handle::Thread(thread),
        })
}

pub(crate) fn current_service() -> Option<String> {
    std::thread::current().name().map(str::to_owned)
}

pub(crate) fn spawn_oneshot<F>(name: String, stack_bytes: usize, f: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name(name)
        .stack_size(stack_bytes)
        .spawn(f)
        .map(drop)
}

pub(crate) fn drive_until<T>(_site: &'static str, _ready: impl FnMut() -> Option<T>) -> Option<T> {
    None
}

pub(crate) fn block_on_inline<F: Future>(
    _future: F,
    caller: &'static Location<'static>,
) -> F::Output {
    unimplemented!("block_on_inline lands in P9-RT (b); called at {caller}")
}

pub(crate) fn before_wait(_site: &'static str) {}

pub(crate) fn ring(bell: &Arc<Bell>) {
    bell.ring();
}
