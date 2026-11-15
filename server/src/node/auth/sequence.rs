//! One boot sender's sequence allocation. Runtime owns the single shared instance.
use super::{ApiSigningKey, Auth, Error, Role, Sender, WorkerSigningKey};
use uuid::Uuid;

#[allow(dead_code)] // The boot sender owner is implemented in a later slice.
pub(crate) struct SenderSequence {
    cluster: Uuid,
    sender: Sender,
    last: u64,
}
impl std::fmt::Debug for SenderSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SenderSequence")
    }
}
impl SenderSequence {
    #[allow(dead_code)] // Pending boot sender construction.
    pub(crate) fn for_worker(
        cluster: Uuid,
        sender: &Sender,
        key: &WorkerSigningKey,
    ) -> Result<Self, Error> {
        if sender.role() != Role::Worker || sender.public_key()? != key.public_key() {
            return Err(Error::Invalid);
        }
        Ok(Self {
            cluster,
            sender: sender.clone(),
            last: 0,
        })
    }
    #[allow(dead_code)] // Pending API boot sender construction.
    pub(crate) fn for_api(
        cluster: Uuid,
        sender: &Sender,
        key: &ApiSigningKey,
    ) -> Result<Self, Error> {
        if cluster != key.cluster_id()
            || sender.role() != Role::Api
            || sender.public_key()? != key.public_key()
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            cluster,
            sender: sender.clone(),
            last: 0,
        })
    }
    /// An issued number stays spent if context construction, signing or sending fails.
    #[allow(dead_code)] // Pending shared boot sender owner.
    pub(crate) fn stamp(&mut self, auth: Auth) -> Result<Auth, Error> {
        if auth.cluster_id() != self.cluster || auth.sender() != &self.sender {
            return Err(Error::Invalid);
        }
        let next = self.last.checked_add(1).ok_or(Error::Exhausted)?;
        let stamped = auth.with_sequence(next)?;
        self.last = next;
        Ok(stamped)
    }
}
#[cfg(test)]
mod tests;
