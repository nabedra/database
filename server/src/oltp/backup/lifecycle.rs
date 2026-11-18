//! Only the settled exporter may cancel an unpublished attempt. A caller DTO
//! cannot turn a transport error or a timeout into permission to drop its pin.
use super::*;
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BackupCancelledStatement {
    pub owner: BackupOwnerRef,
    pub pin: StoragePin,
    pub archive_id: Uuid,
}
pub(crate) struct VerifiedBackupCancelled {
    statement: BackupCancelledStatement,
}
impl VerifiedBackupCancelled {
    /// The owner has awaited all writes and authoritatively observed no root.
    pub(crate) fn after_settlement(
        statement: BackupCancelledStatement,
    ) -> Result<Self, ProtocolError> {
        if statement.owner.backup_id.is_nil()
            || statement.owner.attempt == 0
            || statement.archive_id.is_nil()
        {
            return Err(invalid());
        }
        Ok(Self { statement })
    }
    pub(crate) fn statement(&self) -> BackupCancelledStatement {
        self.statement
    }
    pub(crate) fn seal(self) -> Result<SealedBackupCancelled, ProtocolError> {
        Ok(SealedBackupCancelled {
            digest: serialized_digest(&(b"nabedra/cancelled-backup/v1", self.statement))?,
            statement: self.statement,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedBackupCancelled {
    statement: BackupCancelledStatement,
    digest: Digest,
}
impl SealedBackupCancelled {
    pub(crate) fn validate(&self) -> Result<BackupCancelledStatement, ProtocolError> {
        if self.digest != serialized_digest(&(b"nabedra/cancelled-backup/v1", self.statement))? {
            return Err(invalid());
        }
        Ok(self.statement)
    }
}
