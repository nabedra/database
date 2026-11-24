use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use nabedra_oltp_types::types::{GroupIdentity, WorkerEnrollment, WorkerIdentity};
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_resources::memory::ResourceOwner;
use nabedra_rt::sim::World;
use nabedra_transport::{
    EndpointRegistry, PeerDirectory as TransportDirectory, Purpose, TransportConfig,
    TransportTrust, WorkerTransport, tls::TlsMaterial,
};
use tokio::sync::watch;
use uuid::Uuid;

/// A local enrollment view for simulated peer fixtures. Real worker entry
/// points can still perform their production directory checks in the same world.
pub struct PeerDirectory {
    peers: RwLock<BTreeMap<Uuid, WorkerEnrollment>>,
    changes: watch::Sender<u64>,
}

impl Default for PeerDirectory {
    fn default() -> Self {
        Self {
            peers: RwLock::default(),
            changes: watch::channel(0).0,
        }
    }
}

impl PeerDirectory {
    /// Install the exact fingerprint that the simulated handshake should prove.
    /// Every later dial still has to pass the transport's pin and identity checks.
    pub fn register(&self, world: &Arc<World>, node: u64, enrollment: WorkerEnrollment) {
        world.set_certificate(node, *enrollment.certificate_sha256.as_bytes());
        self.publish(enrollment);
    }

    /// Publish an enrollment to this directory without changing any node's identity.
    pub fn publish(&self, enrollment: WorkerEnrollment) {
        self.peers
            .write()
            .unwrap()
            .insert(enrollment.worker_id, enrollment);
        self.changes.send_modify(|revision| *revision += 1);
    }

    pub fn revoke(&self, worker: Uuid) {
        if self.peers.write().unwrap().remove(&worker).is_some() {
            self.changes.send_modify(|revision| *revision += 1);
        }
    }
}

impl TransportDirectory for PeerDirectory {
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        self.peers
            .read()
            .unwrap()
            .get(&worker)
            .cloned()
            .ok_or_else(not_enrolled)
    }

    fn verify(
        &self,
        peer: WorkerIdentity,
        _group: &GroupIdentity,
        _purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if self.revoked(peer) {
            return Err(not_enrolled());
        }
        Ok(TransportTrust::AppliedEnrollment)
    }

    fn revoked(&self, peer: WorkerIdentity) -> bool {
        !self
            .peers
            .read()
            .unwrap()
            .get(&peer.worker_id)
            .is_some_and(|enrollment| WorkerIdentity::from(enrollment) == peer)
    }

    fn trust_changes(&self) -> Option<watch::Receiver<u64>> {
        Some(self.changes.subscribe())
    }

    fn applied_revision(&self) -> Option<u64> {
        Some(*self.changes.borrow())
    }
}

fn not_enrolled() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "simulated peer identity differs from current enrollment",
        Outcome::NotApplied,
    )
}

/// Which transport implementation a simulated worker offers to its peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PeerProtocol {
    #[default]
    Current,
    /// D10 peer offering only the released one-shot protocol.
    OneShot,
}

impl PeerProtocol {
    pub fn transport(
        self,
        local: &WorkerEnrollment,
        tls: &TlsMaterial,
        shared: Arc<impl ResourceOwner>,
        directory: Arc<dyn TransportDirectory>,
        registry: Arc<EndpointRegistry>,
        config: TransportConfig,
    ) -> Result<WorkerTransport, ProtocolError> {
        match self {
            Self::Current => WorkerTransport::new(local, tls, shared, directory, registry, config),
            Self::OneShot => {
                WorkerTransport::one_shot_for_test(local, tls, shared, directory, registry, config)
            }
        }
    }
}
