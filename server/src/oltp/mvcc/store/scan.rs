//! Byte-adaptive head scans include absent-key intents and tombstones. Returned
//! records own a working-memory lease; no native apply buffer escapes its slot.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};
pub(crate) const SCAN_RECORD_TOO_WIDE: &str = "scan record exceeds page byte budget";
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ScanDomain {
    Rows {
        table_id: Uuid,
    },
    CatalogNames,
    CatalogSplits,
    Index {
        table_id: Uuid,
        index_id: Uuid,
        equality: Page<super::super::index::IndexValue>,
        complete: bool,
        lower: Option<(super::super::index::IndexValue, bool)>,
        upper: Option<(super::super::index::IndexValue, bool)>,
    },
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
        self.scan_page_with_budget(request, false).await
    }

    async fn scan_page_with_budget(
        &self,
        request: ScanRequest,
        scheduled: bool,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        #[cfg(test)]
        let _scan = matches!(
            &request.domain,
            ScanDomain::Rows { .. } | ScanDomain::Index { .. }
        )
        .then(|| crate::oltp_runtime::stage_timing::perf_stage!("test.data.scan").timer());
        if request.max_items == 0
            || request.max_items > 128
            || request.max_bytes == 0
            || request.max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
        {
            return Err(invalid());
        }
        self.observe_snapshot(request.read_ts).await?;
        let bytes = request
            .max_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(128 << 10))
            .ok_or_else(invalid)?;
        let resources = self.store.resources();
        let owner = Arc::new(if scheduled {
            // Await working memory before opening the native snapshot or
            // taking either physical or scheduler compute capacity.
            resources
                .reserve_scratch_wait(ByteClass::Data, bytes, resources.next_native_request_id()?)
                .await?
        } else {
            resources.reserve_scratch(ByteClass::Data, bytes)?
        });
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
                    lineage::validate_read(&config, request.read_ts)?;
                    if let (
                        GroupKind::Data { table_id, .. },
                        ScanDomain::Rows { table_id: target }
                        | ScanDomain::Index {
                            table_id: target, ..
                        },
                    ) = (&group.kind, &request.domain)
                    {
                        if table_id != target {
                            return Err(invalid());
                        }
                    }
                    let prefix = domain_prefix(&request.domain)?;
                    if !matches!(
                        (&group.kind, &request.domain),
                        (
                            GroupKind::Catalog,
                            ScanDomain::CatalogNames | ScanDomain::CatalogSplits
                        ) | (
                            GroupKind::Data { .. },
                            ScanDomain::Rows { .. } | ScanDomain::Index { .. }
                        )
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
                    let mut upper = request
                        .upper
                        .as_ref()
                        .map(|(key, inclusive)| {
                            bound(key, *inclusive, false).map(|key| (key, *inclusive))
                        })
                        .transpose()?;
                    if let ScanDomain::Index {
                        lower: index_lower,
                        upper: index_upper,
                        ..
                    } = &request.domain
                    {
                        let bound = |value: &super::super::index::IndexValue,
                                     inclusive: bool,
                                     is_lower: bool|
                         -> Result<Vec<u8>, ProtocolError> {
                            if !value.valid() {
                                return Err(invalid());
                            }
                            let mut bytes = prefix.clone();
                            let offset = bytes.len();
                            bytes
                                .try_reserve_exact(value.encoded_len())
                                .map_err(|_| invalid())?;
                            bytes.resize(offset + value.encoded_len(), 0);
                            value.encode(&mut bytes[offset..]);
                            if (is_lower && !inclusive) || (!is_lower && inclusive) {
                                bytes = crate::oltp::codec::end(&bytes)
                            }
                            Ok(bytes)
                        };
                        if let Some((value, inclusive)) = index_lower {
                            let lower = bound(value, *inclusive, true)?;
                            if lower > start {
                                start = lower
                            }
                        }
                        if let Some((value, inclusive)) = index_upper {
                            upper = Some((bound(value, *inclusive, false)?, false));
                        }
                    }
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
                                .and_then(|n| {
                                    n.checked_add(if logical.first() == Some(&10) {
                                        128 * std::mem::size_of::<super::super::index::IndexValue>()
                                    } else {
                                        0
                                    })
                                })
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
                                        SCAN_RECORD_TOO_WIDE,
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
    pub(crate) async fn scheduled_scan_page(
        &self,
        scope: crate::oltp_replication::service_rpc::ReadScope,
        request: ScanRequest,
    ) -> Result<OwnedCommand<ScanPage>, crate::oltp_replication::service_rpc::ScheduledReadError>
    {
        if let Some(remote) = &self.remote {
            return remote.scheduled_scan_page(scope, request).await;
        }
        Ok(self.scan_page_wait(request).await?)
    }
    pub(crate) async fn scan_page_wait(
        &self,
        request: ScanRequest,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        let _barrier = self.local_consensus().ensure_linearizable().await?;
        #[cfg(any(test, feature = "test-hooks"))]
        crate::oltp::test_hooks::continuation::applied(_barrier.applied);
        self.local_machine()
            .scan_page_with_budget(request, true)
            .await
    }
    pub(crate) async fn scan_page(
        &self,
        request: ScanRequest,
    ) -> Result<OwnedCommand<ScanPage>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.scan_page(request).await;
        }

        self.local_consensus().ensure_linearizable().await?;
        self.local_machine().scan_page(request).await
    }
}
fn domain_prefix(domain: &ScanDomain) -> Result<Vec<u8>, ProtocolError> {
    Ok(match domain {
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
        ScanDomain::CatalogSplits => vec![HEAD, 13],
        ScanDomain::Index {
            table_id,
            index_id,
            equality,
            complete,
            ..
        } => {
            if equality.iter().any(|v| !v.valid()) {
                return Err(invalid());
            }
            let length = equality
                .iter()
                .try_fold(34usize, |n, v| n.checked_add(v.encoded_len()))
                .and_then(|n| n.checked_add(usize::from(*complete)))
                .filter(|n| *n <= 2 * crate::protocol::MAX_BODY_BYTES + 128)
                .ok_or_else(invalid)?;
            let mut p = Vec::new();
            p.try_reserve_exact(length).map_err(|_| invalid())?;
            p.resize(length, 0);
            p[0] = HEAD;
            p[1] = 10;
            p[2..18].copy_from_slice(table_id.as_bytes());
            p[18..34].copy_from_slice(index_id.as_bytes());
            let mut cursor = 34;
            for v in equality.iter() {
                cursor += v.encode(&mut p[cursor..]);
            }
            if *complete {
                p[cursor] = 0;
            }
            p
        }
    })
}
pub(super) fn decode_key(bytes: &[u8]) -> Result<MvccKey, ProtocolError> {
    if bytes.first() == Some(&13) {
        return super::archive::decode_logical(bytes);
    }
    if bytes.first() == Some(&super::direct::DEDUP_TAG) {
        // Row-shaped anchor followed by the 16-byte request id.
        let split = bytes
            .len()
            .checked_sub(16)
            .filter(|n| *n > 33)
            .ok_or_else(recovery_unknown)?;
        if bytes[17..33] != [0; 16] {
            return Err(recovery_unknown());
        }
        let mut row = Vec::new();
        row.try_reserve_exact(split).map_err(|_| invalid())?;
        row.push(1);
        row.extend_from_slice(&bytes[1..split]);
        let MvccKey::Row {
            table_id,
            primary_key,
        } = decode_key(&row)?
        else {
            return Err(recovery_unknown());
        };
        let request_id = Uuid::from_slice(&bytes[split..]).map_err(|_| recovery_unknown())?;
        if request_id.is_nil() {
            return Err(recovery_unknown());
        }
        return Ok(MvccKey::DirectDedup {
            table_id,
            anchor: primary_key,
            request_id,
        });
    }
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
        10 => {
            let index_id = Uuid::from_slice(&bytes[17..33]).map_err(|_| recovery_unknown())?;
            let mut offset = 33;
            let mut values = Vec::new();
            while bytes.get(offset) != Some(&0) {
                if values.len() == 128 {
                    return Err(recovery_unknown());
                }
                let (v, n) = super::super::index::IndexValue::decode(
                    bytes.get(offset..).ok_or_else(recovery_unknown)?,
                )?;
                values.push(v);
                offset += n;
            }
            offset += 1;
            let p = bytes.get(offset..).ok_or_else(recovery_unknown)?;
            let primary_key = match p.first() {
                Some(1) if p.len() == 9 => Key::Int64(
                    (u64::from_be_bytes(p[1..].try_into().map_err(|_| recovery_unknown())?)
                        ^ (1 << 63)) as i64,
                ),
                Some(2) => Key::Utf8(decode_text(&p[1..])?),
                _ => return Err(recovery_unknown()),
            };
            Ok(MvccKey::IndexEntry {
                table_id: table,
                index_id,
                values: Page::new(values).map_err(|_| recovery_unknown())?,
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
pub(super) fn decode_text(bytes: &[u8]) -> Result<String, ProtocolError> {
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

/// A streaming consumer retains its continuation until a page succeeds, so a
/// blocked intent can retry that exact page. Charge the one temporary key clone
/// before it grows; the previous page owner remains attached through dispatch.
pub(crate) fn clone_scan_cursor(
    shared: &crate::oltp::pool::WorkerSharedResources,
    key: &Option<MvccKey>,
    owner: &Option<Arc<ScratchLease>>,
) -> Result<(Option<MvccKey>, Option<Arc<ScratchLease>>), ProtocolError> {
    match key {
        None => Ok((None, None)),
        Some(key) => {
            let held = owner.as_ref().ok_or_else(invalid)?;
            let bytes = held
                .bytes()
                .checked_add(key.heap_bytes())
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(invalid)?;
            let charge = shared.ensure_scratch_budget(held.clone(), bytes)?;
            Ok((Some(key.clone()), Some(charge)))
        }
    }
}
