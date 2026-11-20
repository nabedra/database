//! A retained cut exports committed images, never executable intents. Pages
//! cover the participant's whole logical namespace, including Catalog keys.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};

const IMAGE_METADATA_BYTES: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommittedImage {
    pub key: MvccKey,
    pub commit_ts: CommitTs,
    pub value: Option<super::super::CanonicalJson>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImageRequest {
    pub read_ts: ReadTs,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub span: KeySpan,
    pub after: Option<MvccKey>,
    pub max_items: u16,
    pub max_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ImagePage {
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
    Images {
        group: GroupIdentity,
        read_ts: ReadTs,
        images: Page<CommittedImage>,
        after: Option<MvccKey>,
        exhausted: bool,
    },
}
impl ImagePage {
    /// The last image already owns its complete key. Omit that duplicate on
    /// the wire and borrow it as the continuation. An explicit `after` covers
    /// traversed heads with no committed value visible at this cut.
    pub(crate) fn continuation(&self) -> Result<Option<&MvccKey>, ProtocolError> {
        match self {
            Self::Blocked { .. } => Err(invalid()),
            Self::Images {
                images,
                after,
                exhausted,
                ..
            } => {
                let cursor = after
                    .as_ref()
                    .or_else(|| images.last().map(|image| &image.key));
                if !*exhausted && cursor.is_none() {
                    return Err(invalid());
                }
                Ok(cursor)
            }
        }
    }
}
impl OwnedPayload for ImageRequest {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        fn key(value: &Key) -> usize {
            match value {
                Key::Utf8(value) => value.capacity(),
                _ => 0,
            }
        }
        self.after
            .as_ref()
            .map_or(0, MvccKey::heap_bytes)
            .checked_add(self.span.start.as_ref().map_or(0, key))
            .and_then(|n| n.checked_add(self.span.end.as_ref().map_or(0, key)))
            .ok_or_else(invalid)
    }
    fn attach_fields(&self, _: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        true
    }
}
impl OwnedPayload for ImagePage {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        match self {
            Self::Blocked { .. } => Ok(0),
            Self::Images {
                group,
                images,
                after,
                ..
            } => images.iter().try_fold(
                group.database.capacity_bytes()
                    + after.as_ref().map_or(0, MvccKey::heap_bytes)
                    + images.capacity() * std::mem::size_of::<CommittedImage>(),
                |n, image| {
                    n.checked_add(image.key.heap_bytes())
                        .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                        .ok_or_else(invalid)
                },
            ),
        }
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Self::Images { images, .. } = self {
            for image in images.iter() {
                if let Some(value) = &image.value {
                    value.attach_charge(owner.clone())?;
                }
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        match self {
            Self::Blocked { .. } => true,
            Self::Images { images, .. } => images
                .iter()
                .all(|image| image.value.as_ref().is_none_or(|value| value.is_charged())),
        }
    }
}
impl ParticipantMachine {
    pub(crate) async fn committed_image_page(
        &self,
        request: OwnedCommand<ImageRequest>,
    ) -> Result<OwnedCommand<ImagePage>, ProtocolError> {
        let input = request.payload()?;
        if input.max_items == 0
            || input.max_items > 128
            || input.max_bytes < 64 << 10
            || input.max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
            || !valid_span(&input.span)
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
                        .and_then(|n| n.checked_add(256 << 10))
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
                    if config.schema_epoch != request.schema_epoch
                        || config.routing_epoch != request.routing_epoch
                        || config.span != request.span
                    {
                        return Err(failure(
                            ErrorCode::StorageUnavailable,
                            "archive participant lineage or retained cut changed",
                        ));
                    }
                    lineage::validate_read(&config, request.read_ts)?;
                    // Heads first, then direct-commit dedup records, which
                    // sort after every head key (`MvccKey::DirectDedup`).
                    let mut prefix = match &request.after {
                        Some(MvccKey::DirectDedup { .. }) => [super::direct::DEDUP],
                        _ => [HEAD],
                    };
                    let mut cursor = match &request.after {
                        Some(after) => {
                            if !owns_image(&group, &config.span, after) {
                                return Err(invalid());
                            }
                            let mut key = vec![prefix[0]; key_length(after)? + 2];
                            let end = key.len() - 1;
                            encode_key(after, &mut key[1..end]);
                            key[end] = 0;
                            key
                        }
                        None => prefix.to_vec(),
                    };
                    let mut images = Vec::with_capacity(request.max_items as usize);
                    let mut used = 4096
                        + images.capacity() * std::mem::size_of::<CommittedImage>()
                        + cursor.capacity()
                        + request.after.as_ref().map_or(0, MvccKey::heap_bytes);
                    let mut wire_used = 4096usize;
                    let mut next = request.after.clone();
                    let mut visited = 0;
                    while visited < request.max_items {
                        let Some(head_row) = view.first_from(&prefix, &cursor)? else {
                            if prefix[0] == HEAD {
                                prefix = [super::direct::DEDUP];
                                cursor.clear();
                                cursor.push(super::direct::DEDUP);
                                continue;
                            }
                            break;
                        };
                        visited += 1;
                        let logical = &head_row.key()[1..];
                        let key = super::archive::decode_logical(logical)?;
                        if !owns_image(&group, &config.span, &key) {
                            return Err(recovery_unknown());
                        }
                        let dedup = prefix[0] == super::direct::DEDUP;
                        if dedup != matches!(key, MvccKey::DirectDedup { .. })
                            || (!dedup && head_row.value().len() > METADATA_BYTES)
                        {
                            return Err(recovery_unknown());
                        }
                        let head: Head = if dedup {
                            Head::default()
                        } else {
                            serde_json::from_slice(head_row.value())
                                .map_err(|_| recovery_unknown())?
                        };
                        let length = logical.len();
                        let outcome = view.with_scratch_buffer(length + 9, |view, buffer| {
                            if dedup {
                                let value = head_row.value();
                                let cost =
                                    encoded_length(&key, super::super::MAX_FIXED_COMMAND_BYTES)?
                                        .checked_add(value.len())
                                        .and_then(|n| n.checked_add(2048))
                                        .ok_or_else(invalid)?;
                                if wire_used
                                    .checked_add(cost)
                                    .is_none_or(|n| n > request.max_bytes)
                                {
                                    return Ok(None);
                                }
                                let Some(image) = super::direct::image_from_record(
                                    key.clone(),
                                    value,
                                    request.read_ts,
                                    owner.clone(),
                                )?
                                else {
                                    return Ok(Some(Ok(None)));
                                };
                                wire_used += cost;
                                used = used
                                    .checked_add(key.heap_bytes())
                                    .and_then(|n| n.checked_add(value.len()))
                                    .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                                    .ok_or_else(invalid)?;
                                return Ok(Some(Ok(Some(image))));
                            }
                            buffer[1..length + 1].copy_from_slice(logical);
                            let end = length + 1;
                            if let Some(txn) = head.intent {
                                buffer[0] = INTENT;
                                let raw =
                                    view.pinned(&buffer[..end])?.ok_or_else(recovery_unknown)?;
                                let intent: Intent<'_> =
                                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                                if intent.txn != txn {
                                    return Err(recovery_unknown());
                                }
                                if intent.read_ts < request.read_ts {
                                    return Ok(Some(Err((txn, intent.manifest))));
                                }
                            }
                            buffer[0] = VALUE;
                            buffer[end..].copy_from_slice(&(!request.read_ts.get()).to_be_bytes());
                            let Some(version) = view.first_from(&buffer[..end], buffer)? else {
                                return Ok(Some(Ok(None)));
                            };
                            if version.key().len() != buffer.len() {
                                return Err(recovery_unknown());
                            }
                            let commit_ts = CommitTs::new(!u64::from_be_bytes(
                                version.key()[end..]
                                    .try_into()
                                    .map_err(|_| recovery_unknown())?,
                            ))
                            .map_err(|_| recovery_unknown())?;
                            if commit_ts > request.read_ts {
                                return Err(recovery_unknown());
                            }
                            #[derive(Deserialize)]
                            #[serde(deny_unknown_fields)]
                            struct Version<'a> {
                                #[serde(borrow)]
                                value: Option<&'a RawValue>,
                            }
                            let value: Version<'_> = serde_json::from_slice(version.value())
                                .map_err(|_| recovery_unknown())?;
                            let cost = encoded_length(&key, super::super::MAX_FIXED_COMMAND_BYTES)?
                                .checked_add(value.value.map_or(0, |v| v.get().len()))
                                .and_then(|n| n.checked_add(2048))
                                .ok_or_else(invalid)?;
                            if wire_used
                                .checked_add(cost)
                                .is_none_or(|n| n > request.max_bytes)
                            {
                                return Ok(None);
                            }
                            wire_used += cost;
                            used = used
                                .checked_add(key.heap_bytes())
                                .and_then(|n| {
                                    n.checked_add(value.value.map_or(0, |v| v.get().len()))
                                })
                                .and_then(|n| n.checked_add(IMAGE_METADATA_BYTES))
                                .ok_or_else(invalid)?;
                            let value = value
                                .value
                                .map(|value| {
                                    super::super::CanonicalJson::from_stored_json(
                                        value.get().as_bytes(),
                                        owner.clone(),
                                    )
                                })
                                .transpose()?;
                            Ok(Some(Ok(Some(CommittedImage {
                                key: key.clone(),
                                commit_ts,
                                value,
                            }))))
                        })?;
                        let emitted = match outcome {
                            Some(Err((txn, manifest))) => {
                                drop(images);
                                owner.shrink_unused(4096)?;
                                return OwnedCommand::new_with_owner(
                                    ImagePage::Blocked { txn, manifest },
                                    owner,
                                );
                            }
                            Some(Ok(Some(image))) => {
                                images.push(image);
                                true
                            }
                            Some(Ok(None)) => false,
                            None => {
                                if images.is_empty() {
                                    return Err(failure(
                                        ErrorCode::LimitExceeded,
                                        "archive image exceeds bounded page budget",
                                    ));
                                }
                                break;
                            }
                        };
                        if !emitted
                            && wire_used
                                .checked_add(encoded_length(
                                    &key,
                                    super::super::MAX_FIXED_COMMAND_BYTES,
                                )?)
                                .is_none_or(|n| n > request.max_bytes)
                        {
                            if images.is_empty() {
                                return Err(failure(
                                    ErrorCode::LimitExceeded,
                                    "archive continuation exceeds bounded page budget",
                                ));
                            }
                            break;
                        }
                        let needed = head_row.key().len() + 1;
                        let additional = needed.saturating_sub(cursor.capacity());
                        let retained = used
                            .checked_add(additional)
                            .and_then(|n| {
                                n.checked_sub(next.as_ref().map_or(0, MvccKey::heap_bytes))
                            })
                            .and_then(|n| n.checked_add(key.heap_bytes()))
                            .ok_or_else(invalid)?;
                        if retained > owner.bytes() {
                            return Err(invalid());
                        }
                        cursor.clear();
                        cursor.try_reserve_exact(needed).map_err(|_| invalid())?;
                        cursor.extend_from_slice(head_row.key());
                        cursor.push(0);
                        used = retained;
                        next = Some(key);
                    }
                    let exhausted = view.first_from(&prefix, &cursor)?.is_none()
                        && (prefix[0] == super::direct::DEDUP
                            || view
                                .first_from(&[super::direct::DEDUP], &[super::direct::DEDUP])?
                                .is_none());
                    if next.as_ref() == images.last().map(|image| &image.key) {
                        next = None;
                    }
                    owner.shrink_unused(used)?;
                    OwnedCommand::new_with_owner(
                        ImagePage::Images {
                            group,
                            read_ts: request.read_ts,
                            images: Page::new(images).map_err(|_| invalid())?,
                            after: next,
                            exhausted,
                        },
                        owner,
                    )
                },
            )
            .await
    }
}
impl ParticipantHandle {
    pub(crate) async fn committed_image_page(
        &self,
        request: OwnedCommand<ImageRequest>,
    ) -> Result<OwnedCommand<ImagePage>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.committed_image_page(request).await;
        }
        self.local_consensus().ensure_linearizable().await?;
        self.local_machine().committed_image_page(request).await
    }
}
