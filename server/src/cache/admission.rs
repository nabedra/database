use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::config::Limits;

pub(super) struct Admission {
    connections: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
}

impl Admission {
    pub fn new(limits: Limits) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(limits.max_connections)),
            handshakes: Arc::new(Semaphore::new(limits.max_handshakes)),
        }
    }

    pub fn connect(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.connections).try_acquire_owned().ok()
    }

    pub fn handshake(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.handshakes).try_acquire_owned().ok()
    }
}
