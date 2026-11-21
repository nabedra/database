//! Cold publication invalidates a run's executable copies. Durable decisions
//! and their participant settlement remain available through their homes.
use super::*;
use crate::oltp::pool::GroupRead;
use std::future::Future;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColdApplicationPin {
    pub identity: PinIdentity,
    pub reservation: IdentityReservation,
    pub request: Option<RequestBinding>,
    pub pin: StoragePin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColdPinCursor {
    read_ts: Timestamp,
    pin_id: Uuid,
}
impl ColdPinCursor {
    fn key(self) -> [u8; ACTIVE_PREFIX.len() + 24] {
        active_key(StoragePin {
            read_ts: self.read_ts,
            pin_id: self.pin_id,
            allocator_epoch: 0,
        })
    }
    fn parse(key: &[u8]) -> Result<Self, ProtocolError> {
        let suffix = key
            .strip_prefix(ACTIVE_PREFIX)
            .filter(|key| key.len() == 24)
            .ok_or_else(unknown)?;
        let ts = u64::from_be_bytes(suffix[..8].try_into().map_err(|_| unknown())?);
        Ok(Self {
            read_ts: Timestamp::new(ts).map_err(|_| unknown())?,
            pin_id: Uuid::from_slice(&suffix[8..]).map_err(|_| unknown())?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColdPinPage {
    after: Option<ColdPinCursor>,
    done: bool,
    candidate: Option<ColdApplicationPin>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColdPinStep {
    pub after: Option<ColdPinCursor>,
    pub done: bool,
    pub retry: bool,
}

impl TimestampHandle {
    pub(crate) async fn sweep_cold_application_pin<F>(
        &self,
        authority: Authority,
        transition_id: Uuid,
        after: Option<ColdPinCursor>,
        verify: impl FnOnce(ColdApplicationPin) -> F,
    ) -> Result<ColdPinStep, ProtocolError>
    where
        F: Future<Output = Result<bool, ProtocolError>>,
    {
        let page = self.cold_pin_page(authority, transition_id, after).await?;
        let retry = if let Some(pin) = page.candidate {
            let request_id = pin.reservation.request_id;
            let released = async {
                if !verify(pin.clone()).await? {
                    return Ok(false);
                }
                self.release_cold_application(authority, transition_id, pin)
                    .await?;
                Ok::<bool, ProtocolError>(true)
            }
            .await;
            if let Err(error) = &released {
                tracing::debug!(%request_id, code=?error.code,
                    message=error.message, "cold application pin proof awaits retry");
            }
            !matches!(released, Ok(true))
        } else {
            false
        };
        Ok(ColdPinStep {
            after: page.after,
            done: page.done,
            retry,
        })
    }

    async fn cold_pin_page(
        &self,
        authority: Authority,
        transition_id: Uuid,
        after: Option<ColdPinCursor>,
    ) -> Result<ColdPinPage, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state: State = read(view, STATE)?.ok_or_else(unknown)?;
                if state.authority != authority {
                    return Err(unknown());
                }
                let fence: Option<FenceReceipt> = read(view, &fence_key(transition_id))?;
                let (cutoff, _) =
                    cold_fence(fence.as_ref(), authority, transition_id).ok_or_else(invalid)?;
                let mut entries = [None; 32];
                let mut count = 0;
                let start = after.map(ColdPinCursor::key);
                let progress = view.scan_page(
                    ACTIVE_PREFIX,
                    start.as_ref().map(|key| &key[..]),
                    entries.len(),
                    |key, value| {
                        let nonce = Uuid::from_slice(value).map_err(|_| unknown())?;
                        entries[count] = Some((ColdPinCursor::parse(key)?, nonce));
                        count += 1;
                        Ok(())
                    },
                )?;
                let mut page = ColdPinPage {
                    after,
                    done: progress.exhausted,
                    candidate: None,
                };
                for (index, (cursor, nonce)) in entries.into_iter().flatten().enumerate() {
                    let record: PinRecord = read(view, &pin_key(nonce))?.ok_or_else(unknown)?;
                    if record.identity.nonce != nonce
                        || active_key(record.pin) != cursor.key()
                        || !matches!(record.state, PinState::Active)
                    {
                        return Err(unknown());
                    }
                    page.after = Some(cursor);
                    if record.pin.allocator_epoch >= cutoff {
                        // Timestamps and allocator epochs both advance monotonically.
                        page.done = true;
                        break;
                    }
                    if record.purpose == PinPurpose::Application
                        && record.identity.run_id != authority.run_id
                    {
                        page.candidate = Some(ColdApplicationPin::from(&record));
                        page.done &= index + 1 == count;
                        break;
                    }
                }
                view.encode(&page)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }

    /// Called only after the requester has checked the published Control run
    /// and the exact pin against the linearizable Status home.
    async fn release_cold_application(
        &self,
        authority: Authority,
        transition_id: Uuid,
        pin: ColdApplicationPin,
    ) -> Result<(), ProtocolError> {
        #[cfg(test)]
        let observed = pin.clone();
        match self
            .submit(TimestampCommand::ReleaseColdApplication {
                authority,
                transition_id,
                pin,
            })
            .await?
            .reply
        {
            TimestampReply::Released => {
                #[cfg(test)]
                self.observe_cold_application_release(authority, transition_id, &observed)
                    .await?;
                Ok(())
            }
            reply => Err(handle::rejected(reply)),
        }
    }

    #[cfg(test)]
    async fn observe_cold_application_release(
        &self,
        authority: Authority,
        transition_id: Uuid,
        pin: &ColdApplicationPin,
    ) -> Result<(), ProtocolError> {
        let Some(directory) = std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR") else {
            return Ok(());
        };
        let directory = std::path::PathBuf::from(directory);
        let selected = std::fs::read_to_string(directory.join("cold-retention-fixture"));
        if selected.ok().and_then(|value| value.trim().parse().ok()) != Some(transition_id) {
            return Ok(());
        }
        let reply = self
            .submit(TimestampCommand::AdvanceFloor { authority })
            .await?
            .reply;
        let TimestampReply::Retention { safe_ts, .. } = reply else {
            return Err(handle::rejected(reply));
        };
        let bytes = serde_json::to_vec(&serde_json::json!({
            "authority": authority,
            "transition_id": transition_id,
            "pin": pin,
            "safe_ts": safe_ts,
        }))
        .map_err(|_| unknown())?;
        let id = pin.reservation.request_id;
        let staging = directory.join(format!("cold-pin-released-{id}.next"));
        let path = directory.join(format!("cold-pin-released-{id}.json"));
        std::fs::write(&staging, bytes).map_err(|_| unknown())?;
        std::fs::rename(staging, path).map_err(|_| unknown())
    }
}

impl From<&PinRecord> for ColdApplicationPin {
    fn from(record: &PinRecord) -> Self {
        Self {
            identity: record.identity.clone(),
            reservation: record.reservation,
            request: record.request,
            pin: record.pin,
        }
    }
}

fn read<T: DeserializeOwned>(
    view: &mut GroupRead<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    view.get(key)?
        .map(|bytes| serde_json::from_slice(bytes.as_slice()).map_err(|_| unknown()))
        .transpose()
}

fn cold_fence(
    fence: Option<&FenceReceipt>,
    authority: Authority,
    transition_id: Uuid,
) -> Option<(u64, &GroupIdentity)> {
    match &fence?.reply {
        TimestampReply::Fenced {
            allocator_epoch,
            receipt,
            ..
        } if receipt.transition_id == transition_id
            && receipt.next.run_id == authority.run_id
            && receipt.next.owner_term <= authority.owner_term
            && receipt
                .expected
                .is_some_and(|prior| prior.run_id != authority.run_id) =>
        {
            Some((*allocator_epoch, &receipt.group))
        }
        _ => None,
    }
}

pub(super) fn release(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    authority: Authority,
    transition_id: Uuid,
    pin: &ColdApplicationPin,
) -> Result<Transition, ProtocolError> {
    let Some(state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.authority != authority {
        return reject(tx, Fault::StaleAuthority);
    }
    let fence = load::<FenceReceipt>(tx, &fence_key(transition_id))?;
    let Some((cutoff, scope)) = cold_fence(fence.as_ref(), authority, transition_id) else {
        return reject(tx, Fault::InvalidBinding);
    };
    if pin.identity.run_id == authority.run_id || pin.pin.allocator_epoch >= cutoff {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = pin_key(pin.identity.nonce);
    let Some(mut record) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if record.purpose != PinPurpose::Application || ColdApplicationPin::from(&record) != *pin {
        return reject(tx, Fault::InvalidBinding);
    }
    // The service verified exact point settlement in Status before proposing
    // release. Recheck the immutable Timestamp half in the native batch.
    if pin.reservation.scoped.is_some() {
        if let Some(decision) =
            point_decision::read_record_by_uuid(scope, &state, pin.reservation.request_id, |key| {
                tx.get(key)
            })?
        {
            let binding = &decision.locator().binding;
            if binding.reservation != pin.reservation
                || binding.pin_identity != pin.identity
                || binding.storage_pin != pin.pin
                || Some(binding.request) != pin.request
                || !pin.reservation.binds(binding.request)
                || binding.pin_identity.nonce != pin.reservation.reservation_id
            {
                return reject(tx, Fault::InvalidBinding);
            }
        }
    }
    if matches!(record.state, PinState::ColdReleased) {
        return commit(tx, &TimestampReply::Released);
    }
    let active = active_key(pin.pin);
    if !matches!(record.state, PinState::Active)
        || !tx
            .get(&active)?
            .is_some_and(|value| value.as_slice() == pin.identity.nonce.as_bytes())
    {
        return reject(tx, Fault::InvalidBinding);
    }
    record.state = PinState::ColdReleased;
    record.scoped_copy_proof = None;
    retry_snapshot::release(tx, &mut record)?;
    put(tx, &key, &record)?;
    tx.delete(&active)?;
    commit(tx, &TimestampReply::Released)
}
