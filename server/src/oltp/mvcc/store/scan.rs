//! Byte-adaptive head scans include absent-key intents and tombstones. Returned
//! records own a working-memory lease; no native apply buffer escapes its slot.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScanDomain {
    Rows { table_id: Uuid },
    CatalogNames,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanRequest {
    pub domain: ScanDomain,
    pub read_ts: ReadTs,
    pub after: Option<MvccKey>,
    #[serde(skip)]
    pub after_owner: Option<Arc<ScratchLease>>,
    pub lower: Option<(Key, bool)>,
    pub upper: Option<(Key, bool)>,
    pub max_items: u16,
    pub max_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanRecord {
    pub key: MvccKey,
    pub version: Option<CommitTs>,
    pub value: Option<super::super::CanonicalJson>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScanPage {
    Records {
        records: Page<ScanRecord>,
        exhausted: bool,
    },
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
}
impl ParticipantMachine {
    pub(crate) async fn scan_page(
        &self,
        request: ScanRequest,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        if request.max_items == 0
            || request.max_items > 128
            || request.max_bytes == 0
            || request.max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
        {
            return Err(invalid());
        }
        let bytes = request
            .max_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(128 << 10))
            .ok_or_else(invalid)?;
        let owner = Arc::new(
            self.store
                .resources()
                .reserve_scratch(ByteClass::Data, bytes)?,
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
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if request.read_ts.get() < config.gc_floor {
                        return Err(invalid());
                    }
                    if let (
                        GroupKind::Data { table_id, .. },
                        ScanDomain::Rows { table_id: target },
                    ) = (&group.kind, &request.domain)
                    {
                        if table_id != target {
                            return Err(invalid());
                        }
                    }
                    let prefix = domain_prefix(&request.domain);
                    if !matches!(
                        (&group.kind, &request.domain),
                        (GroupKind::Catalog, ScanDomain::CatalogNames)
                            | (GroupKind::Data { .. }, ScanDomain::Rows { .. })
                    ) {
                        return Err(invalid());
                    }
                    let mut start = match &request.after {
                        Some(after) => {
                            if !owns(&group, &config.span, after) {
                                return Err(invalid());
                            }
                            let length = key_length(after)?;
                            let mut bytes = Vec::new();
                            bytes.try_reserve_exact(length + 2).map_err(|_| invalid())?;
                            bytes.resize(length + 2, 0);
                            bytes[0] = HEAD;
                            encode_key(after, &mut bytes[1..1 + length]);
                            bytes
                        }
                        None => prefix.clone(),
                    };
                    let bound = |key: &Key,
                                 inclusive: bool,
                                 lower: bool|
                     -> Result<Vec<u8>, ProtocolError> {
                        let ScanDomain::Rows { table_id } = request.domain else {
                            return Err(invalid());
                        };
                        let logical = MvccKey::Row {
                            table_id,
                            primary_key: key.clone(),
                        };
                        let length = key_length(&logical)?;
                        let mut out = Vec::new();
                        out.try_reserve_exact(length + 2).map_err(|_| invalid())?;
                        out.resize(length + 1, 0);
                        out[0] = HEAD;
                        encode_key(&logical, &mut out[1..]);
                        if lower && !inclusive {
                            out.push(0)
                        }
                        Ok(out)
                    };
                    if let Some((key, inclusive)) = &request.lower {
                        let lower = bound(key, *inclusive, true)?;
                        if lower > start {
                            start = lower;
                        }
                    }
                    let upper = request
                        .upper
                        .as_ref()
                        .map(|(key, inclusive)| {
                            bound(key, *inclusive, false).map(|key| (key, *inclusive))
                        })
                        .transpose()?;
                    if !start.starts_with(&prefix) {
                        return Err(invalid());
                    }
                    let mut records = Vec::new();
                    records
                        .try_reserve_exact(request.max_items as usize)
                        .map_err(|_| invalid())?;
                    let mut used = records.capacity() * std::mem::size_of::<ScanRecord>() + 4096;
                    let mut exhausted = false;
                    while records.len() < request.max_items as usize {
                        let Some(head_record) = view.first_from(&prefix, &start)? else {
                            exhausted = true;
                            break;
                        };
                        if upper.as_ref().is_some_and(|(upper, inclusive)| {
                            head_record.key() > upper.as_slice()
                                || (!inclusive && head_record.key() == upper.as_slice())
                        }) {
                            exhausted = true;
                            break;
                        }
                        if head_record.value().len() > METADATA_BYTES {
                            return Err(recovery_unknown());
                        }
                        let head: Head = serde_json::from_slice(head_record.value())
                            .map_err(|_| recovery_unknown())?;
                        let logical = &head_record.key()[1..];
                        let length = logical.len();
                        let outcome = view.with_scratch_buffer(length + 9, |view, buffer| {
                            buffer[1..1 + length].copy_from_slice(logical);
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
                            let version = view.first_from(&buffer[..end], buffer)?;
                            #[derive(Deserialize)]
                            struct Version<'a> {
                                #[serde(borrow)]
                                value: Option<&'a RawValue>,
                            }
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
                                let decoded: Version<'_> = serde_json::from_slice(version.value())
                                    .map_err(|_| recovery_unknown())?;
                                (Some(stamp), decoded.value)
                            } else {
                                (None, None)
                            };
                            let cost = logical
                                .len()
                                .checked_add(value.map_or(0, |v| v.get().len()))
                                .and_then(|n| n.checked_add(512))
                                .ok_or_else(invalid)?;
                            if used.checked_add(cost).is_none_or(|n| n > request.max_bytes) {
                                return Ok(None);
                            }
                            used += cost;
                            let key = decode_key(logical)?;
                            let value = value
                                .map(|value| {
                                    super::super::CanonicalJson::from_stored_json(
                                        value.get().as_bytes(),
                                        owner.clone(),
                                    )
                                })
                                .transpose()?;
                            Ok(Some(Ok(ScanRecord {
                                key,
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
                                        "scan record exceeds page byte budget",
                                    ));
                                }
                                break;
                            }
                        }
                        // Capacity is reserved before growing a cursor. Only one
                        // mutable cursor coexists with the bounded output records.
                        let needed = head_record.key().len() + 1;
                        if needed > start.capacity() {
                            start
                                .try_reserve_exact(needed - start.len())
                                .map_err(|_| invalid())?;
                        }
                        start.clear();
                        start.extend_from_slice(head_record.key());
                        start.push(0);
                    }
                    drop(start);
                    drop(upper);
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
impl ParticipantHandle {
    pub(crate) async fn scan_page(
        &self,
        request: ScanRequest,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine.scan_page(request).await
    }
}
fn domain_prefix(domain: &ScanDomain) -> Vec<u8> {
    match domain {
        ScanDomain::Rows { table_id } => {
            let mut p = vec![HEAD, 1];
            p.extend_from_slice(table_id.as_bytes());
            p.extend_from_slice(Uuid::nil().as_bytes());
            p
        }
        ScanDomain::CatalogNames => {
            let mut p = vec![HEAD, 2];
            p.extend_from_slice(&[0; 32]);
            p
        }
    }
}
fn decode_key(bytes: &[u8]) -> Result<MvccKey, ProtocolError> {
    if bytes.len() < 33 {
        return Err(recovery_unknown());
    }
    let table = Uuid::from_slice(&bytes[1..17]).map_err(|_| recovery_unknown())?;
    match bytes[0] {
        1 => {
            let p = &bytes[33..];
            let primary_key = match p.first() {
                Some(1) if p.len() == 9 => Key::Int64(
                    (u64::from_be_bytes(p[1..].try_into().map_err(|_| recovery_unknown())?)
                        ^ (1 << 63)) as i64,
                ),
                Some(2) => Key::Utf8(decode_text(&p[1..])?),
                _ => return Err(recovery_unknown()),
            };
            Ok(MvccKey::Row {
                table_id: table,
                primary_key,
            })
        }
        2 => Ok(MvccKey::CatalogName {
            name: crate::oltp_replication::types::Text::new(decode_text(&bytes[33..])?)
                .map_err(|_| recovery_unknown())?,
        }),
        _ => Err(recovery_unknown()),
    }
}
fn decode_text(bytes: &[u8]) -> Result<String, ProtocolError> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len()).map_err(|_| invalid())?;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != 0 {
            out.push(bytes[i]);
            i += 1;
        } else {
            match bytes.get(i + 1) {
                Some(255) => {
                    out.push(0);
                    i += 2
                }
                Some(0) if i + 2 == bytes.len() => {
                    return String::from_utf8(out).map_err(|_| recovery_unknown());
                }
                _ => return Err(recovery_unknown()),
            }
        }
    }
    Err(recovery_unknown())
}
impl OwnedPayload for ScanPage {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        match self {
            Self::Blocked { .. } => Ok(0),
            Self::Records { records, .. } => records.iter().try_fold(
                records.capacity() * std::mem::size_of::<ScanRecord>(),
                |n, r| {
                    n.checked_add(r.key.heap_bytes())
                        .and_then(|n| n.checked_add(512))
                        .ok_or_else(invalid)
                },
            ),
        }
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Self::Records { records, .. } = self {
            for record in records.iter() {
                if let Some(v) = &record.value {
                    v.attach_charge(owner.clone())?
                }
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        match self {
            Self::Blocked { .. } => true,
            Self::Records { records, .. } => records
                .iter()
                .all(|r| r.value.as_ref().is_none_or(|v| v.is_charged())),
        }
    }
}
