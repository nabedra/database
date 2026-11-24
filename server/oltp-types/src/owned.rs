use crate::types::Digest;
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};

use nabedra_resources::{
    decode_budget::json_decode_budget,
    memory::{ByteClass, ChargedBytes, ReplicaMetadata, ScratchLease, WorkerMemory},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use std::{
    fmt,
    mem::size_of,
    sync::{Arc, OnceLock},
};

/// Implementations count all uncharged retained heap, including Vec capacity
/// and String capacity. Field attachment must retain the same whole-command
/// owner when a canonical JSON field is extracted or cloned.
pub trait OwnedPayload: Serialize + DeserializeOwned + Send + Sync + 'static {
    fn heap_bytes(&self) -> Result<usize, ProtocolError>;
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError>;
    fn fields_attached(&self) -> bool;
}
struct Inner<D> {
    payload: D,
    owner: OnceLock<Arc<ScratchLease>>,
    raft_metadata: OnceLock<Arc<ReplicaMetadata>>,
}
pub struct OwnedCommand<D: OwnedPayload> {
    inner: Arc<Inner<D>>,
}
impl<D: OwnedPayload> Clone for OwnedCommand<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<D: OwnedPayload> fmt::Debug for OwnedCommand<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedCommand")
            .field("charged", &self.is_attached())
            .finish_non_exhaustive()
    }
}
impl<D: OwnedPayload + fmt::Display> fmt::Display for OwnedCommand<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.payload.fmt(f)
    }
}
impl<D: OwnedPayload> OwnedCommand<D> {
    pub fn attach_raft_metadata(
        &self,
        reserve: impl FnOnce() -> Result<ReplicaMetadata, ProtocolError>,
        accepts: impl Fn(&ReplicaMetadata) -> bool,
    ) -> Result<(), ProtocolError> {
        if let Some(owner) = self.inner.raft_metadata.get() {
            return if accepts(owner) {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let owner = Arc::new(reserve()?);
        if self.inner.raft_metadata.set(owner).is_err() {
            let owner = self.inner.raft_metadata.get().ok_or_else(invalid)?;
            if !accepts(owner) {
                return Err(invalid());
            }
        }
        Ok(())
    }

    pub fn raft_metadata(&self) -> Option<Arc<ReplicaMetadata>> {
        self.inner.raft_metadata.get().cloned()
    }

    /// A native builder reserves its complete working footprint before growing
    /// this typed payload. Validate its retained heap and attach the same owner
    /// before any field or command can leave that native job.
    pub fn new_with_owner(payload: D, owner: Arc<ScratchLease>) -> Result<Self, ProtocolError> {
        let command = Self::unattached(payload);
        let required = command.minimum_bytes()?;
        command.attach(owner, required)?;
        Ok(command)
    }
    fn unattached(payload: D) -> Self {
        Self {
            inner: Arc::new(Inner {
                payload,
                owner: OnceLock::new(),
                raft_metadata: OnceLock::new(),
            }),
        }
    }
    fn minimum_bytes(&self) -> Result<usize, ProtocolError> {
        self.inner
            .payload
            .heap_bytes()?
            .checked_add(size_of::<Inner<D>>())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(capacity)
    }
    pub fn uncharged_heap_bytes(&self) -> Result<usize, ProtocolError> {
        if self.is_attached() {
            Ok(0)
        } else {
            self.minimum_bytes()
        }
    }
    pub fn attach_from_codec(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if self.is_attached() {
            return Ok(());
        }
        let required = owner.bytes();
        self.attach(owner, required)
    }
    pub fn decode_owner(&self) -> Result<Arc<ScratchLease>, ProtocolError> {
        self.ensure_charged()?;
        self.inner.owner.get().cloned().ok_or_else(capacity)
    }
    fn attach(
        &self,
        owner: Arc<ScratchLease>,
        required_decode: usize,
    ) -> Result<(), ProtocolError> {
        if owner.bytes() < required_decode || owner.bytes() < self.minimum_bytes()? {
            return Err(capacity());
        }
        if let Some(existing) = self.inner.owner.get() {
            return if Arc::ptr_eq(existing, &owner) {
                Ok(())
            } else {
                Err(capacity())
            };
        }
        self.inner.payload.attach_fields(owner.clone())?;
        if !self.inner.payload.fields_attached() {
            return Err(capacity());
        }
        match self.inner.owner.set(owner.clone()) {
            Ok(()) => Ok(()),
            Err(_) => {
                if self
                    .inner
                    .owner
                    .get()
                    .is_some_and(|current| Arc::ptr_eq(current, &owner))
                {
                    Ok(())
                } else {
                    Err(capacity())
                }
            }
        }
    }
    pub fn is_attached(&self) -> bool {
        self.inner.owner.get().is_some() && self.inner.payload.fields_attached()
    }
    pub fn ensure_charged(&self) -> Result<(), ProtocolError> {
        if self.is_attached() {
            Ok(())
        } else {
            Err(capacity())
        }
    }
    pub fn payload(&self) -> Result<&D, ProtocolError> {
        self.ensure_charged()?;
        Ok(&self.inner.payload)
    }

    /// A leader already owns fixed prepared bytes. Raw/unattached wire fields
    /// cannot enter this constructor. Metadata is charged before the Arc grows.
    pub fn new_owned(
        payload: D,
        shared: &WorkerMemory,
        class: ByteClass,
    ) -> Result<Self, ProtocolError> {
        if !payload.fields_attached() {
            return Err(capacity());
        }
        let bytes = payload
            .heap_bytes()?
            .checked_add(size_of::<Inner<D>>())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(capacity)?;
        let owner = Arc::new(shared.reserve_scratch(class, bytes)?);
        let command = Self::unattached(payload);
        command.attach(owner, bytes)?;
        Ok(command)
    }

    /// Reserve the allocation-free lexical upper bound before Deserialize. The
    /// bound covers canonical verification overlap as well as the retained DTO.
    pub fn decode_json(
        input: &ChargedBytes,
        shared: &WorkerMemory,
        class: ByteClass,
        max_decode_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        let required = json_decode_budget(input.as_slice())?
            .checked_add(size_of::<Inner<D>>())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(capacity)?;
        if required > max_decode_bytes {
            return Err(capacity());
        }
        let owner = Arc::new(shared.reserve_scratch(class, required)?);
        Self::decode_with_owner(input, owner, required)
    }
    /// A precharged frame codec may share its existing whole-command owner.
    /// This check precedes Deserialize, so a tiny reservation cannot bless a
    /// large allocation and then rely on an after-the-fact attachment check.
    pub fn decode_with_owner(
        input: &ChargedBytes,
        owner: Arc<ScratchLease>,
        required: usize,
    ) -> Result<Self, ProtocolError> {
        let lexical = json_decode_budget(input.as_slice())?
            .checked_add(size_of::<Inner<D>>())
            .and_then(|n| n.checked_add(64))
            .ok_or_else(capacity)?;
        if required < lexical || owner.bytes() < required {
            return Err(capacity());
        }
        let command: Self = serde_json::from_slice(input.as_slice()).map_err(|_| invalid())?;
        command.attach(owner, required)?;
        Ok(command)
    }
}
impl<D: OwnedPayload> Serialize for OwnedCommand<D> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.ensure_charged().map_err(serde::ser::Error::custom)?;
        self.inner.payload.serialize(serializer)
    }
}
impl<'de, D: OwnedPayload> Deserialize<'de> for OwnedCommand<D> {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        // Only the private codec can attach this intermediate value. Neither
        // serialization nor payload access accepts it before attachment.
        Ok(Self::unattached(D::deserialize(deserializer)?))
    }
}
fn capacity() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::LimitExceeded,
        "command has no adequate owned decode reservation",
        Outcome::NotApplied,
    )
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid typed command JSON",
        Outcome::NotApplied,
    )
}

/// Hash an already-owned bounded DTO without allocating a second encoded
/// buffer. Call this from the owned native codec/apply job.
#[inline]
pub fn serialized_digest(value: &impl Serialize) -> Result<Digest, ProtocolError> {
    use sha2::{Digest as _, Sha256};
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        ProtocolError::new(
            ErrorCode::ValidationError,
            "invalid typed command JSON",
            Outcome::NotApplied,
        )
    })?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}
