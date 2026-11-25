use crate::{
    oltp_replication::types::*,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct RestartIntent {
    pub id: Uuid,
    pub run_id: Uuid,
    pub previous: WorkerIdentity,
    pub next: WorkerEnrollment,
    pub pool_id: Uuid,
    pub mount_generation: u64,
}
impl RestartIntent {
    #[doc(hidden)]
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.id.is_nil()
            || self.run_id.is_nil()
            || self.pool_id.is_nil()
            || self.mount_generation == 0
            || self.previous.validate().is_err()
            || self.next.validate().is_err()
            || self.next.worker_id != self.previous.worker_id
            || self.next.incarnation == self.previous.incarnation
            || self.next.enrollment_revision <= self.previous.enrollment_revision
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn message(&self, control: &GroupIdentity) -> Result<Digest, ProtocolError> {
        crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/retained-mount-restart/v1".as_slice(),
            control,
            self,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct RestartPreparation {
    pub intent: RestartIntent,
    pub enrolled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestartMountProof([Digest; 2]);
impl RestartMountProof {
    pub(super) fn verify(
        &self,
        control: &GroupIdentity,
        intent: &RestartIntent,
        public_key: Digest,
    ) -> Result<(), ProtocolError> {
        self.verify_message(intent.message(control)?, public_key)
    }
    pub(super) fn verify_message(
        &self,
        message: Digest,
        public_key: Digest,
    ) -> Result<(), ProtocolError> {
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(self.0[0].as_bytes());
        signature[32..].copy_from_slice(self.0[1].as_bytes());
        UnparsedPublicKey::new(&ED25519, public_key.as_bytes())
            .verify(message.as_bytes(), &signature)
            .map_err(|_| invalid())
    }
}

#[doc(hidden)]
pub struct RestartMountKey(Ed25519KeyPair);
impl RestartMountKey {
    pub(crate) fn from_seed(seed: &[u8]) -> Result<Self, ProtocolError> {
        Ed25519KeyPair::from_seed_unchecked(seed)
            .map(Self)
            .map_err(|_| invalid())
    }
    pub(crate) fn public_key(&self) -> Digest {
        Digest::from_bytes(
            self.0
                .public_key()
                .as_ref()
                .try_into()
                .expect("Ed25519 public key size"),
        )
    }
    pub(crate) fn sign(
        &self,
        control: &GroupIdentity,
        intent: &RestartIntent,
    ) -> Result<RestartMountProof, ProtocolError> {
        Ok(self.sign_message(intent.message(control)?))
    }
    pub(super) fn sign_message(&self, message: Digest) -> RestartMountProof {
        let signature = self.0.sign(message.as_bytes());
        let bytes = signature.as_ref();
        RestartMountProof([
            Digest::from_bytes(bytes[..32].try_into().expect("Ed25519 signature half")),
            Digest::from_bytes(bytes[32..].try_into().expect("Ed25519 signature half")),
        ])
    }
}

impl crate::oltp::pool::mount::RestartMountKeyProof for RestartMountKey {
    fn from_mount_seed(seed: &[u8; 32]) -> Result<Self, ProtocolError> {
        Self::from_seed(seed)
    }

    fn public_key(&self) -> Digest {
        self.public_key()
    }
}

#[track_caller]
fn invalid() -> ProtocolError {
    nabedra_rt::diag_at("invalid", std::panic::Location::caller());
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "retained mount restart intent or signature differs",
        Outcome::NotApplied,
    )
}
