use std::sync::Arc;

/// A reservation retains the replica and its physical capacity until drop.
pub trait ReplicaMetadataOwner: Send + Sync {
    fn release_metadata(&self, bytes: usize, log: bool);
}

pub struct ReplicaMetadata {
    owner: Arc<dyn ReplicaMetadataOwner>,
    bytes: usize,
    log: bool,
}

impl ReplicaMetadata {
    pub fn new(owner: Arc<dyn ReplicaMetadataOwner>, bytes: usize, log: bool) -> Self {
        Self { owner, bytes, log }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn belongs_to(&self, owner: &Arc<impl ReplicaMetadataOwner>) -> bool {
        Arc::as_ptr(&self.owner).cast::<()>() == Arc::as_ptr(owner).cast::<()>()
    }
}

impl Drop for ReplicaMetadata {
    fn drop(&mut self) {
        self.owner.release_metadata(self.bytes, self.log);
    }
}
