//! Ordered ready-set hashing for bounded worker membership pages.
//! The private authority component consumes the same ordered hash domain.
use super::types::{Digest, Invalid, WorkerIdentity};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub(super) fn hash(parts: &[&[u8]]) -> Digest {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    Digest::from_bytes(digest.finalize().into())
}

/// A resumable ordered ready-set hash. Control can persist this fixed-size
/// cursor between bounded membership-build pages.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadySetHasher {
    previous: Option<Uuid>,
    count: u64,
    chain: Digest,
}
impl Default for ReadySetHasher {
    fn default() -> Self {
        Self {
            previous: None,
            count: 0,
            chain: hash(&[b"nabedra/ready-set/v1"]),
        }
    }
}
impl ReadySetHasher {
    pub fn push(&mut self, member: WorkerIdentity) -> Result<(), Invalid> {
        member.validate()?;
        if self
            .previous
            .is_some_and(|previous| previous >= member.worker_id)
        {
            return Err(Invalid("ready workers must be unique and ordered"));
        }
        let count = self
            .count
            .checked_add(1)
            .ok_or(Invalid("ready count overflow"))?;
        self.chain = member_hash(b"nabedra/ready-worker/v1", self.chain, member);
        self.previous = Some(member.worker_id);
        self.count = count;
        Ok(())
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn finish(&self) -> Digest {
        hash(&[
            b"nabedra/ready-root/v1",
            &self.count.to_be_bytes(),
            self.chain.as_bytes(),
        ])
    }
}
pub(super) fn member_hash(domain: &[u8], prior: Digest, member: WorkerIdentity) -> Digest {
    hash(&[
        domain,
        prior.as_bytes(),
        member.worker_id.as_bytes(),
        member.incarnation.as_bytes(),
        member.certificate_sha256.as_bytes(),
        &member.enrollment_revision.to_be_bytes(),
    ])
}
