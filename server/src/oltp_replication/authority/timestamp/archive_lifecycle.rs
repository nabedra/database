//! Continuous archive retention shares the backup active index but has its own
//! pin UUID, so publishing a base cannot remove its retained floor.
use super::*;
use crate::oltp::backup::{
    SealedBackupPublished,
    lifecycle::{SealedBackupCancelled, VerifiedBackupCancelled},
};
const RETENTION: &[u8] = b"ts/continuous-archive/v1/";
const CANCELLED: &[u8] = b"ts/cancelled-backup/v1/";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ArchiveControl {
    Retain {
        authority: Authority,
        proof: SealedBackupPublished,
        previous_root: Option<Digest>,
    },
    Cancel {
        authority: Authority,
        proof: SealedBackupCancelled,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retention {
    root: Digest,
    pin: StoragePin,
    archive_id: uuid::Uuid,
}
pub(super) fn cancelled(
    tx: &mut GroupTransaction<'_>,
    owner: crate::oltp::backup::BackupOwnerRef,
) -> Result<bool, ProtocolError> {
    Ok(tx.pinned(&cancel_key(owner))?.is_some())
}
fn cancel_key(owner: crate::oltp::backup::BackupOwnerRef) -> Vec<u8> {
    [
        CANCELLED,
        owner.backup_id.as_bytes(),
        &owner.attempt.to_be_bytes(),
    ]
    .concat()
}
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    command: &ArchiveControl,
) -> Result<Transition, ProtocolError> {
    let Some(state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    match command {
        ArchiveControl::Cancel { authority, proof } => {
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let statement = proof.validate()?;
            let key = backup_key(statement.owner);
            let record: BackupRecord = load(tx, &key)?.ok_or_else(unknown)?;
            if record.owner != statement.owner
                || record.pin != statement.pin
                || record.published.is_some()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let cancelled_key = cancel_key(statement.owner);
            if let Some(prior) = load::<SealedBackupCancelled>(tx, &cancelled_key)? {
                if crate::oltp_replication::owned::serialized_digest(&prior)?
                    != crate::oltp_replication::owned::serialized_digest(proof)?
                {
                    return reject(tx, Fault::TransitionConflict);
                }
                return commit(tx, &TimestampReply::Released);
            }
            put(tx, &cancelled_key, proof)?;
            tx.delete(&backup_active_key(record.pin))?;
            commit(tx, &TimestampReply::Released)
        }
        ArchiveControl::Retain {
            authority,
            proof,
            previous_root,
        } => {
            if state.authority != *authority {
                return reject(tx, Fault::StaleAuthority);
            }
            let statement = proof.validate()?;
            let record: BackupRecord =
                load(tx, &backup_key(statement.owner))?.ok_or_else(unknown)?;
            if record.pin != statement.pin || cancelled(tx, statement.owner)? {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = [RETENTION, statement.owner.backup_id.as_bytes()].concat();
            let previous: Option<Retention> = load(tx, &key)?;
            if let Some(previous) = &previous {
                if previous.root == statement.catalog_root
                    && previous.archive_id == statement.archive_id
                {
                    let independent = retention_candidate(tx, state.high_watermark, None)?;
                    split::advance_retention(tx, independent)?;
                    return commit(tx, &TimestampReply::Released);
                }
                if *previous_root != Some(previous.root)
                    || previous.pin.read_ts >= statement.pin.read_ts
                {
                    return reject(tx, Fault::TransitionConflict);
                }
            } else if previous_root.is_some() {
                return reject(tx, Fault::InvalidBinding);
            }
            let mut pin = statement.pin;
            pin.pin_id = statement.owner.backup_id;
            let next = Retention {
                root: statement.catalog_root,
                pin,
                archive_id: statement.archive_id,
            };
            // The old and new index entries change in the same durable batch.
            let replaced = previous.map(|previous| backup_active_key(previous.pin));
            let independent = retention_candidate(tx, pin.read_ts.get(), replaced.as_deref())?;
            split::advance_retention(tx, independent)?;
            if let Some(replaced) = replaced {
                tx.delete(&replaced)?;
            }
            tx.put(&backup_active_key(pin), &key)?;
            put(tx, &key, &next)?;
            commit(tx, &TimestampReply::Released)
        }
    }
}
impl TimestampHandle {
    pub(crate) async fn retain_archive(
        &self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
        previous_root: Option<Digest>,
    ) -> Result<(), ProtocolError> {
        let proof = SealedBackupPublished::from_verified(proof)?;
        match self
            .submit(TimestampCommand::ArchiveControl {
                command: ArchiveControl::Retain {
                    authority,
                    proof,
                    previous_root,
                },
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            _ => Err(unknown()),
        }
    }
    pub(crate) async fn cancel_backup(
        &self,
        authority: Authority,
        proof: VerifiedBackupCancelled,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::ArchiveControl {
                command: ArchiveControl::Cancel {
                    authority,
                    proof: proof.seal()?,
                },
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            _ => Err(unknown()),
        }
    }
}
