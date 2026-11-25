//! Bounded exact-key reads share one native snapshot and one leader barrier.
//! Absent keys retain intent checks, and tombstone versions remain explicit.
use super::scan::{ScanPage, ScanRecord};
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointReadRequest {
    pub read_ts: ReadTs,
    pub keys: Page<MvccKey>,
    pub max_bytes: usize,
}
impl OwnedPayload for PointReadRequest {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.keys.iter().try_fold(
            self.keys.capacity() * std::mem::size_of::<MvccKey>(),
            |n, key| n.checked_add(key.heap_bytes()).ok_or_else(invalid),
        )
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
impl ParticipantMachine {
    pub async fn read_page(
        &self,
        request: OwnedCommand<PointReadRequest>,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        let input = request.payload()?;
        if input.keys.is_empty()
            || input.keys.len() > 128
            || input.heap_bytes()? > 1 << 20
            || input.max_bytes < 64 << 10
            || input.max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
        {
            return Err(invalid());
        }
        self.observe_snapshot(input.read_ts).await?;
        let owner = Arc::new(
            self.store
                .resources()
                .reserve_scratch_wait(
                    ByteClass::Data,
                    input
                        .max_bytes
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(128 << 10))
                        .ok_or_else(invalid)?,
                    0,
                )
                .await?,
        );
        let group = self.store.identity().clone();
        self.store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let request = request.payload()?;
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    // Validate the entire requested page before producing a prefix.
                    lineage::validate_read(&config, request.read_ts)?;
                    if request
                        .keys
                        .iter()
                        .any(|key| !owns(&group, &config.span, key))
                    {
                        return Err(invalid());
                    }
                    #[cfg(feature = "sim")]
                    super::snapshot_probe(&group, request.read_ts);
                    let mut records = Vec::with_capacity(request.keys.len());
                    let mut used = 4096 + records.capacity() * std::mem::size_of::<ScanRecord>();
                    for key in request.keys.iter() {
                        let length = key_length(key)?;
                        let outcome = view.with_scratch_buffer(length + 9, |view, buffer| {
                            encode_key(key, &mut buffer[1..1 + length]);
                            let end = length + 1;
                            buffer[0] = HEAD;
                            if let Some(raw) = view.pinned(&buffer[..end])? {
                                let head: Head =
                                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                                if let Some(txn) = head.intent {
                                    buffer[0] = INTENT;
                                    let raw = view
                                        .pinned(&buffer[..end])?
                                        .ok_or_else(recovery_unknown)?;
                                    let intent: Intent<'_> = serde_json::from_slice(&raw)
                                        .map_err(|_| recovery_unknown())?;
                                    if intent.txn != txn {
                                        return Err(recovery_unknown());
                                    }
                                    if intent.read_ts < request.read_ts {
                                        return Ok(Some(Err((txn, intent.manifest))));
                                    }
                                }
                            }
                            buffer[0] = VALUE;
                            buffer[end..].copy_from_slice(&(!request.read_ts.get()).to_be_bytes());
                            let version = view.first_from(&buffer[..end], buffer)?;
                            let (stamp, value) = if let Some(version) = &version {
                                if version.key().len() != buffer.len() {
                                    return Err(recovery_unknown());
                                }
                                let stamp = CommitTs::new(!u64::from_be_bytes(
                                    version.key()[end..]
                                        .try_into()
                                        .map_err(|_| recovery_unknown())?,
                                ))
                                .map_err(|_| recovery_unknown())?;
                                if stamp > request.read_ts {
                                    return Err(recovery_unknown());
                                }
                                let value = row::Version::decode(version.value())?;
                                (Some(stamp), value.value)
                            } else {
                                (None, None)
                            };
                            let cost = key
                                .heap_bytes()
                                .checked_add(length)
                                .and_then(|n| n.checked_add(value.map_or(0, |v| v.get().len())))
                                .and_then(|n| n.checked_add(1024))
                                .ok_or_else(invalid)?;
                            if used.checked_add(cost).is_none_or(|n| n > request.max_bytes) {
                                return Ok(None);
                            }
                            used += cost;
                            let value = value
                                .map(|v| {
                                    super::super::CanonicalJson::from_stored_json(
                                        v.get().as_bytes(),
                                        owner.clone(),
                                    )
                                })
                                .transpose()?;
                            Ok(Some(Ok(ScanRecord {
                                key: key.clone(),
                                version: stamp,
                                value,
                            })))
                        })?;
                        match outcome {
                            Some(Err((txn, manifest))) => {
                                drop(records);
                                owner.shrink_unused(4096)?;
                                return OwnedCommand::new_with_owner(
                                    ScanPage::Blocked { txn, manifest },
                                    owner,
                                );
                            }
                            Some(Ok(record)) => records.push(record),
                            None => {
                                if records.is_empty() {
                                    return Err(failure(
                                        ErrorCode::LimitExceeded,
                                        "point read record exceeds page byte budget",
                                    ));
                                }
                                break;
                            }
                        }
                    }
                    let exhausted = records.len() == request.keys.len();
                    owner.shrink_unused(used)?;
                    OwnedCommand::new_with_owner(
                        ScanPage::Records {
                            records: Page::new(records).map_err(|_| invalid())?,
                            exhausted,
                        },
                        owner,
                    )
                },
            )
            .await
    }
}
