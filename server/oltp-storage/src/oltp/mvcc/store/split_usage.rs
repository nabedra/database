//! Committed MVCC bytes in one Data replica's active native generation.
use super::*;
use crate::oltp::pool::{ByteClass, ScratchLease};

const ITEMS_PER_PAGE: usize = 128;
const BYTES_PER_PAGE: u64 = 2 << 20;
const PIVOT_KEY_BYTES: usize = 4 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardEpoch {
    pub schema: u64,
    pub routing: u64,
}

pub struct ShardUsageScan {
    generation: u64,
    epoch: ShardEpoch,
    after: Option<ChargedBytes>,
    bytes: u64,
    row_bytes: u64,
    largest_image: usize,
    pivot: Pivot,
    _owner: ScratchLease,
}

pub enum ShardUsage {
    Inactive,
    Frozen {
        epoch: ShardEpoch,
        split_id: Uuid,
    },
    Scanning(ShardUsageScan),
    Observed {
        epoch: ShardEpoch,
        bytes: u64,
        largest_image: usize,
        pivot: Option<Key>,
    },
}

#[derive(Default)]
struct Pivot {
    last_logical: Vec<u8>,
    first: bool,
    boundary: Option<Key>,
    preferred: bool,
}
impl Pivot {
    fn observe(
        &mut self,
        logical: &[u8],
        value: &[u8],
        span: &KeySpan,
        rows_passed_target: bool,
    ) -> Result<(), ProtocolError> {
        if self.preferred || logical.len() > PIVOT_KEY_BYTES || logical == self.last_logical {
            return Ok(());
        }
        self.last_logical.clear();
        self.last_logical.extend_from_slice(logical);
        // Native VALUE versions are ordered newest first within a logical key.
        if value == b"{\"value\":null}" {
            return Ok(());
        }
        let MvccKey::Row { primary_key, .. } = archive::decode_logical(logical)? else {
            return Err(recovery_unknown());
        };
        if self.first {
            if span
                .start
                .as_ref()
                .is_some_and(|start| compare_primary(start, &primary_key) != Ordering::Less)
                || span
                    .end
                    .as_ref()
                    .is_some_and(|end| compare_primary(&primary_key, end) != Ordering::Less)
            {
                return Err(recovery_unknown());
            }
            self.boundary = Some(primary_key);
            self.preferred = rows_passed_target;
        }
        self.first = true;
        Ok(())
    }
}

impl ParticipantMachine {
    pub async fn observe_split_usage(
        &self,
        authority: Authority,
        target_bytes: u64,
        scan: Option<ShardUsageScan>,
    ) -> Result<ShardUsage, ProtocolError> {
        let GroupKind::Data { table_id, .. } = self.store.identity().kind else {
            return Err(invalid());
        };
        let shared = self.store.resources().clone();
        self.store
            .read_shard_usage(move |view, generation| {
                let raw = view.pinned(CONFIG)?.ok_or_else(recovery_unknown)?;
                if raw.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let config: Config =
                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                if config.authority != authority {
                    return Err(failure(
                        ErrorCode::StorageUnavailable,
                        "split usage waits for the local authority fence",
                    ));
                }
                if config.split_child.is_some_and(|child| !child.complete) {
                    return Ok(ShardUsage::Inactive);
                }
                let epoch = ShardEpoch {
                    schema: config.schema_epoch,
                    routing: config.routing_epoch,
                };
                if let Some(source) = config.split_source {
                    return Ok(ShardUsage::Frozen {
                        epoch,
                        split_id: source.statement.split_id,
                    });
                }
                // RANGES-2: a source held by a split's direct barrier is held as
                // much as a frozen one; the owner's sweep resumes that split.
                if let Some(split_id) = config.direct_pause {
                    return Ok(ShardUsage::Frozen { epoch, split_id });
                }
                let mut scan = match scan {
                    Some(scan) if scan.generation == generation && scan.epoch == epoch => scan,
                    _ => ShardUsageScan {
                        generation,
                        epoch,
                        after: None,
                        bytes: 0,
                        row_bytes: 0,
                        largest_image: 0,
                        pivot: Pivot::default(),
                        _owner: shared.reserve_scratch(ByteClass::Data, 6 * PIVOT_KEY_BYTES)?,
                    },
                };
                let before = scan.bytes;
                let progress = view.scan_page_until(
                    &[VALUE],
                    scan.after.as_ref().map(ChargedBytes::as_slice),
                    ITEMS_PER_PAGE,
                    |key, value| {
                        let logical = key
                            .get(1..key.len().checked_sub(8).ok_or_else(recovery_unknown)?)
                            .ok_or_else(recovery_unknown)?;
                        if logical.get(1..17) != Some(table_id.as_bytes().as_slice())
                            || !matches!(logical[0], 1 | 10)
                        {
                            return Err(recovery_unknown());
                        }
                        // The group/state/generation prefix adds 26 native key bytes.
                        let bytes = key
                            .len()
                            .checked_add(value.len())
                            .and_then(|n| n.checked_add(26))
                            .and_then(|n| u64::try_from(n).ok())
                            .ok_or_else(recovery_unknown)?;
                        scan.bytes = scan.bytes.checked_add(bytes).ok_or_else(recovery_unknown)?;
                        let image_ceiling = key
                            .len()
                            .checked_mul(6)
                            .and_then(|key| key.checked_add(value.len()))
                            .and_then(|raw| raw.checked_add(64 << 10))
                            .ok_or_else(recovery_unknown)?;
                        scan.largest_image = scan.largest_image.max(image_ceiling);
                        if logical[0] == 1 {
                            scan.pivot.observe(
                                logical,
                                value,
                                &config.span,
                                scan.row_bytes >= target_bytes / 2,
                            )?;
                            scan.row_bytes = scan
                                .row_bytes
                                .checked_add(bytes)
                                .ok_or_else(recovery_unknown)?;
                        }
                        Ok(scan.bytes - before >= BYTES_PER_PAGE)
                    },
                )?;
                if progress.exhausted {
                    Ok(ShardUsage::Observed {
                        epoch,
                        bytes: scan.bytes,
                        largest_image: scan.largest_image,
                        pivot: scan.pivot.boundary,
                    })
                } else {
                    let cursor = progress.last_key.ok_or_else(recovery_unknown)?;
                    // Retain the continuation in the ordinary Data arena.
                    // An idle scanner must release native apply scratch.
                    scan.after = Some(shared.copy_bytes(cursor.as_slice())?);
                    Ok(ShardUsage::Scanning(scan))
                }
            })
            .await
    }
}

#[cfg(test)]
#[test]
fn o16_o21_m01_split_pivot_excludes_a_single_hot_key_and_tombstoned_versions() {
    let table = Uuid::from_u128(1);
    let key = |id| {
        let key = MvccKey::Row {
            table_id: table,
            primary_key: Key::Int64(id),
        };
        let mut encoded = vec![0; key_length(&key).unwrap()];
        encode_key(&key, &mut encoded);
        encoded
    };
    let span = KeySpan {
        start: Some(Key::Int64(1)),
        end: Some(Key::Int64(8)),
    };
    let mut pivot = Pivot::default();
    let present = b"{\"value\":{}}";
    pivot.observe(&key(1), present, &span, false).unwrap();
    pivot.observe(&key(1), present, &span, true).unwrap();
    pivot
        .observe(&key(2), b"{\"value\":null}", &span, true)
        .unwrap();
    pivot.observe(&key(2), present, &span, true).unwrap();
    assert!(pivot.boundary.is_none());
    pivot.observe(&key(4), present, &span, true).unwrap();
    pivot.observe(&key(6), present, &span, true).unwrap();
    assert_eq!(pivot.boundary, Some(Key::Int64(4)));
}
