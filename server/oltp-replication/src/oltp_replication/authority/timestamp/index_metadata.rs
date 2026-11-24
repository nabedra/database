//! Locate one existing index metadata pin by its original Status nonce.
use super::super::status::RequestKind;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct IndexMetadataPin {
    pub pin: StoragePin,
    pub released: bool,
}

impl TimestampHandle {
    pub(crate) async fn index_metadata_pin(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
        request: RequestBinding,
    ) -> Result<IndexMetadataPin, ProtocolError> {
        if txn.request_id != reservation.request_id
            || reservation.request_id != request.request_id
            || reservation.reservation_id.is_nil()
            || reservation.kind != RequestKind::Mutation
            || reservation.original_run != authority.run_id
            || request.run_id != authority.run_id
            || request.digest != reservation.digest
        {
            return Err(unknown());
        }
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state: State = view
                    .get(STATE)?
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?
                    .ok_or_else(unknown)?;
                if state.authority != authority {
                    return Err(unknown());
                }
                let record: PinRecord = view
                    .get(&pin_key(reservation.reservation_id))?
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?
                    .ok_or_else(unknown)?;
                let identity = PinIdentity {
                    run_id: reservation.original_run,
                    request_id: reservation.request_id,
                    digest: reservation.digest,
                    nonce: reservation.reservation_id,
                };
                if record.identity != identity
                    || record.reservation != reservation
                    || record.request != Some(request)
                    || record.purpose != (PinPurpose::IndexMetadata { owner, txn })
                    || record.pin.pin_id.is_nil()
                    || record.pin.allocator_epoch == 0
                {
                    return Err(unknown());
                }
                let active = view.get(&active_key(record.pin))?;
                let released = match record.state {
                    PinState::Active
                        if active
                            .as_ref()
                            .is_some_and(|nonce| nonce.as_slice() == identity.nonce.as_bytes()) =>
                    {
                        false
                    }
                    PinState::MetadataReleased { .. } if active.is_none() => true,
                    _ => return Err(unknown()),
                };
                view.encode(&IndexMetadataPin {
                    pin: record.pin,
                    released,
                })
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }
}
