use super::*;
use crate::oltp_replication::authority::{
    status::identity::transaction::{
        ArchivedIdentityDisposition,
        archive_images::{identity_receipt_metadata_step, receipt_step},
    },
    timestamp::archive::{
        ArchiveClosureStatement,
        point::{POINT_IMAGE, PointArchiveCut},
    },
};

pub(super) use crate::oltp_replication::authority::timestamp::archive::point::verify_identity;

#[derive(Default)]
pub(super) struct PointTranscript {
    closure: Option<ArchiveClosureStatement>,
    pending_image: Option<Digest>,
    inventory: Option<Inventory>,
}

struct Inventory {
    point: PointArchiveCut,
    receipts: Digest,
    native_receipts: Digest,
    decisions: Digest,
    native_decisions: Digest,
    previous_uuid: Option<Uuid>,
}

impl PointTranscript {
    pub(super) fn observe(
        &mut self,
        frame: &HistoryFrame,
        shared: &crate::oltp::pool::WorkerSharedResources,
    ) -> Result<(), ProtocolError> {
        if let Some(expected) = self.pending_image.take() {
            let HistoryFrame::Metadata(image) = frame else {
                return Err(unknown());
            };
            return if serialized_digest(image)? == expected {
                Ok(())
            } else {
                Err(unknown())
            };
        }
        if let HistoryFrame::StatusClosure(closure) = frame {
            self.finish()?;
            if closure
                .point_history
                .as_ref()
                .is_some_and(|history| !history.valid())
            {
                return Err(unknown());
            }
            self.inventory = closure.point_history.as_ref().map(|point| {
                let seed = Digest::from_bytes([0; 32]);
                Inventory {
                    point: PointArchiveCut::new(point.applied),
                    receipts: seed,
                    native_receipts: seed,
                    decisions: seed,
                    native_decisions: seed,
                    previous_uuid: None,
                }
            });
            self.closure = Some(closure.clone());
            return Ok(());
        }

        match frame {
            HistoryFrame::Identity { identity, .. } => {
                let closure = self.closure.as_ref().ok_or_else(unknown)?;
                if let Some(point) = &identity.point_projection {
                    let _owner = shared.reserve_scratch(ByteClass::Data, 32 << 10)?;
                    let image = verify_identity(closure, identity)?.ok_or_else(unknown)?;
                    self.pending_image = Some(serialized_digest(&image)?);
                    self.inventory
                        .as_mut()
                        .ok_or_else(unknown)?
                        .point
                        .push(point)?;
                }
                if let Some(inventory) = &mut self.inventory {
                    let uuid = identity.reservation.request_id;
                    if inventory.previous_uuid.is_some_and(|old| old >= uuid) {
                        return Err(unknown());
                    }
                    inventory.previous_uuid = Some(uuid);
                    inventory.receipts = receipt_step(inventory.receipts, identity)?;
                    if let ArchivedIdentityDisposition::PreCutCommitted { decision, outcome } =
                        &identity.disposition
                        && identity.origin.is_none()
                        && matches!(decision.decision, TxnDecision::Commit { commit_ts }
                            if closure.base_exclusive.is_none_or(|base| commit_ts > base))
                    {
                        inventory.decisions = serialized_digest(&(
                            inventory.decisions,
                            identity.reservation,
                            decision,
                            outcome,
                        ))?;
                    }
                }
            }
            HistoryFrame::Metadata(image) if image.key.starts_with(POINT_IMAGE) => {
                return Err(unknown());
            }
            HistoryFrame::Metadata(image)
                if matches!(image.group.kind, GroupKind::Status { .. }) =>
            {
                if let Some(inventory) = &mut self.inventory {
                    let closure = self.closure.as_ref().ok_or_else(unknown)?;
                    if let Some(root) = identity_receipt_metadata_step(
                        shared,
                        inventory.native_receipts,
                        image,
                        closure.pin.read_ts,
                    )? {
                        inventory.native_receipts = root;
                    }
                }
            }
            HistoryFrame::BeginTransaction(decision) => {
                if let Some(inventory) = &mut self.inventory {
                    inventory.native_decisions = serialized_digest(&(
                        inventory.native_decisions,
                        decision.reservation,
                        &decision.statement,
                        &decision.outcome,
                    ))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<(), ProtocolError> {
        if self.pending_image.is_some() {
            return Err(unknown());
        }
        if let Some(inventory) = &self.inventory {
            let closure = self.closure.as_ref().ok_or_else(unknown)?;
            if closure.point_history.as_ref() != Some(&inventory.point)
                || inventory.receipts != inventory.native_receipts
                || inventory.decisions != inventory.native_decisions
            {
                return Err(unknown());
            }
        }
        Ok(())
    }
}
