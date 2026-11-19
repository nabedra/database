//! One-entry direct commits (plan section 4).
//!
//! A `DirectCommit` carries the exact versions, index changes and response of
//! one request. Apply validates it against the replicated state in a fixed
//! order and either writes everything in one batch with the applied position
//! or writes nothing. A dedup record in the `'D'` namespace makes a retry with
//! the same request id and body return the original result.
//!
//! Dedup records expire through a separate replicated command,
//! `DirectExpire { cutoff_unix_ms }`. The range leader computes the cutoff
//! from its own clock (`expiry_cutoff`) and proposes it; apply reads no clock,
//! so every replica deletes exactly the same records. An `'E'` index ordered
//! by `applied_unix_ms` makes each page of deletions a bounded prefix scan.
//!
//! Rejections map to the leader executor's actions (plan section 6):
//! `StaleEpoch` (epoch changed, or the group is frozen or not yet published
//! for a split) refreshes the route; `StaleAuthority` means the fence moved;
//! `Incomplete` means a queued intent holds a key, so fall back to the queued
//! path; `Conflict` means re-read (stale `expected_version`, `commit_ts` not
//! above the newest version or the range floor, or the request id was used
//! with a different body); `ResourceLimit` means the entry is too large for
//! one batch, so use the queued path; `InvalidBinding` is a malformed command.
use super::*;
use crate::oltp::{mvcc::CanonicalJson, pool::ScratchLease};

pub(crate) mod clock;
#[allow(unused_imports)] // Consumed by the leader executor (T5).
pub(crate) use clock::{ClockRejection, RANGE_CLOCK_WAIT, RangeClock};

/// Logical key tag of `MvccKey::DirectDedup`. It sorts after every head tag.
pub(crate) const DEDUP_TAG: u8 = 14;
/// Physical namespace of dedup records: `'D' || logical dedup key`.
pub(super) const DEDUP: u8 = b'D';
/// Expiry index: `'E' || applied_unix_ms (big endian) || logical dedup key`.
const EXPIRY: u8 = b'E';
/// The highest commit timestamp any direct commit applied on this group.
/// A split freeze below it is refused, because its images would miss it.
const DIRECT_HIGH: &[u8] = b"mvcc/direct-high/v1";

/// `direct_dedup_retention_ms` default: 24 hours.
#[allow(dead_code)] // Read by the leader's expiry task (T5).
pub(crate) const DEFAULT_DIRECT_DEDUP_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;
/// Dedup records deleted per `DirectExpire` entry at most.
pub(crate) const MAX_DIRECT_EXPIRE_ITEMS: u16 = 128;
/// Rows per command. `insert_many` above this uses the queued path.
pub(crate) const MAX_DIRECT_WRITES: usize = 4096;
/// Index changes per row.
pub(crate) const MAX_DIRECT_INDEX_CHANGES: usize = 256;
/// The exact result must fit a participant reply, so a replay can return it.
pub(crate) const MAX_DIRECT_RESULT_BYTES: usize = 48 << 10;
/// Native key prefix and write-batch framing per put or delete.
const OP_OVERHEAD: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectCommit {
    /// Run and group fence, checked at apply.
    pub authority: Authority,
    pub table_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub request_id: Uuid,
    /// SHA-256 of the exact request body.
    pub digest: Digest,
    pub commit_ts: CommitTs,
    /// Rows in strictly ascending key order. One for point operations.
    pub writes: Vec<DirectWrite>,
    /// Exact `data` value of the response.
    pub result: CanonicalJson,
    /// Leader wall clock when proposing. Stored in the dedup record and
    /// compared only with a `DirectExpire` cutoff, never with a local clock.
    pub applied_unix_ms: u64,
}
/// Delete dedup records with `applied_unix_ms < cutoff_unix_ms`, oldest
/// first, at most `max_items` per entry. The proposer repeats it until the
/// reply says `complete`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectExpire {
    pub authority: Authority,
    pub cutoff_unix_ms: u64,
    pub max_items: u16,
}
/// The cutoff a leader proposes at `now_unix_ms` for `retention_ms`.
/// `None` while nothing can be old enough yet.
#[allow(dead_code)] // Called by the leader's expiry task (T5).
pub(crate) fn expiry_cutoff(now_unix_ms: u64, retention_ms: u64) -> Option<u64> {
    now_unix_ms
        .checked_sub(retention_ms.max(1))
        .filter(|cutoff| *cutoff > 0)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectWrite {
    pub key: MvccKey,
    /// The exact head version: `None` only when the key has no head. A
    /// deleted row keeps its tombstone's commit timestamp as its head
    /// version, as for `PreparedWrite`, so the executor passes that.
    pub expected_version: Option<CommitTs>,
    /// `None` writes a tombstone.
    pub value: Option<CanonicalJson>,
    pub index: Vec<IndexChange>,
}
/// One secondary index entry written with its row, under the same rules.
/// `expected_version` is the exact head version, as for a row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexChange {
    pub key: MvccKey,
    pub expected_version: Option<CommitTs>,
    pub value: Option<CanonicalJson>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectReply {
    Applied { commit_ts: CommitTs },
    Replayed { result: DirectResult },
    Rejected(Fault),
}
/// The exact response `data` of a committed request, compared by its bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct DirectResult(Box<RawValue>);
impl DirectResult {
    pub(crate) fn get(&self) -> &str {
        self.0.get()
    }
}
impl PartialEq for DirectResult {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl Eq for DirectResult {}

/// The stored dedup value. Fields are in canonical (sorted) order, so the
/// stored bytes are canonical JSON and can travel as an image value.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DedupRecord<'a> {
    applied_unix_ms: u64,
    commit_ts: CommitTs,
    digest: Digest,
    #[serde(borrow)]
    result: &'a RawValue,
}

impl DirectCommit {
    /// The dedup key: the anchor is the smallest key of the request.
    pub(crate) fn dedup_key(&self) -> Option<MvccKey> {
        match &self.writes.first()?.key {
            MvccKey::Row { primary_key, .. } => Some(MvccKey::DirectDedup {
                table_id: self.table_id,
                anchor: primary_key.clone(),
                request_id: self.request_id,
            }),
            _ => None,
        }
    }
    /// Every row and index key this commit writes, for `RangeClock` pending.
    #[allow(dead_code)] // Called by the leader executor (T5).
    pub(crate) fn keys(&self) -> Vec<MvccKey> {
        self.writes
            .iter()
            .flat_map(|write| {
                std::iter::once(write.key.clone())
                    .chain(write.index.iter().map(|change| change.key.clone()))
            })
            .collect()
    }
    fn changes(
        &self,
    ) -> impl Iterator<Item = (&MvccKey, Option<CommitTs>, Option<&CanonicalJson>)> {
        self.writes.iter().flat_map(|write| {
            std::iter::once((&write.key, write.expected_version, write.value.as_ref())).chain(
                write
                    .index
                    .iter()
                    .map(|c| (&c.key, c.expected_version, c.value.as_ref())),
            )
        })
    }
}

fn reply(tx: &mut GroupTransaction<'_>, reply: DirectReply) -> Result<Transition, ProtocolError> {
    let applied = matches!(
        reply,
        DirectReply::Applied { .. } | DirectReply::Replayed { .. }
    );
    let encoded = tx.encode(&ParticipantReply::Direct(reply))?;
    Ok(if applied {
        Transition::Commit(encoded)
    } else {
        Transition::Reject(encoded)
    })
}
fn rejected(tx: &mut GroupTransaction<'_>, fault: Fault) -> Result<Transition, ProtocolError> {
    reply(tx, DirectReply::Rejected(fault))
}

/// Structural checks that need no state. They reject before the ordered
/// state checks, so a malformed command never reads a head.
fn well_formed(group: &GroupIdentity, command: &DirectCommit) -> Result<bool, ProtocolError> {
    let GroupKind::Data { table_id, .. } = &group.kind else {
        return Ok(false);
    };
    if command.table_id != *table_id
        || command.request_id.is_nil()
        || command.authority.validate().is_err()
        || command.schema_epoch == 0
        || command.routing_epoch == 0
        || command.writes.is_empty()
        || command.writes.len() > MAX_DIRECT_WRITES
        || command.result.as_str().len() > MAX_DIRECT_RESULT_BYTES
    {
        return Ok(false);
    }
    let mut keys = Vec::new();
    for (offset, write) in command.writes.iter().enumerate() {
        let MvccKey::Row {
            table_id: row_table,
            primary_key,
        } = &write.key
        else {
            return Ok(false);
        };
        if row_table != table_id
            || write.index.len() > MAX_DIRECT_INDEX_CHANGES
            || (offset != 0
                && compare_keys(&command.writes[offset - 1].key, &write.key) != Ordering::Less)
        {
            return Ok(false);
        }
        for change in &write.index {
            let MvccKey::IndexEntry {
                table_id: index_table,
                primary_key: index_primary,
                ..
            } = &change.key
            else {
                return Ok(false);
            };
            if index_table != table_id || index_primary != primary_key {
                return Ok(false);
            }
            keys.push(&change.key);
        }
    }
    keys.sort_by(|a, b| compare_keys(a, b));
    if keys
        .windows(2)
        .any(|pair| compare_keys(pair[0], pair[1]) == Ordering::Equal)
    {
        return Ok(false);
    }
    for (key, _, value) in command.changes() {
        key_length(key)?;
        if let Some(value) = value {
            serde_json::from_str::<&RawValue>(value.as_str()).map_err(|_| invalid())?;
        }
    }
    Ok(true)
}

/// The native batch bytes this commit writes, before expiry deletions.
fn batch_estimate(command: &DirectCommit, dedup: usize) -> Result<usize, ProtocolError> {
    let mut bytes = 16usize << 10;
    for (key, _, value) in command.changes() {
        let key = key_length(key)?;
        bytes = key
            .checked_add(9)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(value.map_or(0, |v| v.as_str().len())))
            .and_then(|n| n.checked_add(2 * OP_OVERHEAD + 64))
            .and_then(|n| bytes.checked_add(n))
            .ok_or_else(invalid)?;
    }
    dedup
        .checked_add(9)
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(command.result.as_str().len()))
        .and_then(|n| n.checked_add(2 * OP_OVERHEAD + 512))
        .and_then(|n| bytes.checked_add(n))
        .ok_or_else(invalid)
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectCommit,
) -> Result<Transition, ProtocolError> {
    let Some(config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if !well_formed(group, command)? {
        return rejected(tx, Fault::InvalidBinding);
    }
    // 1. The group is not frozen for a split, nor an unimported child.
    if config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete) {
        return rejected(tx, Fault::StaleEpoch);
    }
    // 2. The run and group fence.
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    // 3. Both epochs.
    if config.schema_epoch != command.schema_epoch || config.routing_epoch != command.routing_epoch
    {
        return rejected(tx, Fault::StaleEpoch);
    }
    // 4. Every key belongs to the group.
    if command
        .changes()
        .any(|(key, _, _)| !owns(group, &config.span, key))
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    // 5. A committed request with this id and anchor replays or conflicts.
    //    This precedes the head checks, which the first attempt changed.
    let dedup_key = command.dedup_key().ok_or_else(invalid)?;
    let dedup = physical(DEDUP, &dedup_key)?;
    if let Some(raw) = tx.pinned(&dedup)? {
        let record: DedupRecord<'_> =
            serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
        if record.digest != command.digest {
            return rejected(tx, Fault::Conflict);
        }
        let result = DirectResult(record.result.to_owned());
        drop(raw);
        return reply(tx, DirectReply::Replayed { result });
    }
    if batch_estimate(command, dedup.len())? > APPLY.batch_bytes - (64 << 10) {
        return rejected(tx, Fault::ResourceLimit);
    }
    // 6-8. No intent, the exact head version, then a timestamp above the
    //      newest version, the GC floor and a split child's import cut.
    let mut heads = Vec::with_capacity(command.writes.len());
    for (key, expected, _) in command.changes() {
        let length = key_length(key)?;
        let head = tx.with_scratch_buffer(length + 1, |tx, buffer| {
            buffer[0] = HEAD;
            encode_key(key, &mut buffer[1..]);
            load::<Head>(tx, buffer)
        })?;
        heads.push((head.unwrap_or_default(), expected));
    }
    if heads.iter().any(|(head, _)| head.intent.is_some()) {
        return rejected(tx, Fault::Incomplete);
    }
    if heads
        .iter()
        .any(|(head, expected)| head.version != *expected)
    {
        return rejected(tx, Fault::Conflict);
    }
    if heads.iter().any(|(head, _)| {
        head.version
            .is_some_and(|newest| command.commit_ts <= newest)
    }) {
        return rejected(tx, Fault::Conflict);
    }
    drop(heads);
    // A direct commit never lands at or below the group's GC floor (or a
    // split child's import cut): no reader below it is served here.
    let floor = config.split_child.map_or(config.gc_floor, |child| {
        config.gc_floor.max(child.read_ts.get())
    });
    if command.commit_ts.get() <= floor {
        return rejected(tx, Fault::Conflict);
    }

    #[derive(Serialize)]
    struct Version<'a> {
        value: Option<&'a RawValue>,
    }
    for (key, _, value) in command.changes() {
        let length = key_length(key)?;
        let value = value
            .map(|v| serde_json::from_str::<&RawValue>(v.as_str()).map_err(|_| invalid()))
            .transpose()?;
        tx.with_scratch_buffer(length + 9, |tx, buffer| {
            encode_key(key, &mut buffer[1..length + 1]);
            buffer[0] = VALUE;
            buffer[length + 1..].copy_from_slice(&(!command.commit_ts.get()).to_be_bytes());
            tx.put_serialized(buffer, &Version { value })?;
            buffer[0] = HEAD;
            tx.put_serialized(
                &buffer[..length + 1],
                &Head {
                    version: Some(command.commit_ts),
                    intent: None,
                },
            )
        })?;
    }
    let result =
        serde_json::from_str::<&RawValue>(command.result.as_str()).map_err(|_| invalid())?;
    put_record(
        tx,
        &dedup,
        &DedupRecord {
            applied_unix_ms: command.applied_unix_ms,
            commit_ts: command.commit_ts,
            digest: command.digest,
            result,
        },
    )?;
    let high = load::<CommitTs>(tx, DIRECT_HIGH)?;
    if high.is_none_or(|high| high < command.commit_ts) {
        tx.put_serialized(DIRECT_HIGH, &command.commit_ts)?;
    }
    reply(
        tx,
        DirectReply::Applied {
            commit_ts: command.commit_ts,
        },
    )
}

/// `tag || logical key`.
fn physical(tag: u8, key: &MvccKey) -> Result<Vec<u8>, ProtocolError> {
    let length = key_length(key)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length + 1).map_err(|_| invalid())?;
    bytes.resize(length + 1, 0);
    bytes[0] = tag;
    encode_key(key, &mut bytes[1..]);
    Ok(bytes)
}
fn expiry_key(applied_unix_ms: u64, dedup: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(dedup.len() + 8)
        .map_err(|_| invalid())?;
    bytes.push(EXPIRY);
    bytes.extend_from_slice(&applied_unix_ms.to_be_bytes());
    bytes.extend_from_slice(&dedup[1..]);
    Ok(bytes)
}
fn put_record(
    tx: &mut GroupTransaction<'_>,
    dedup: &[u8],
    record: &DedupRecord<'_>,
) -> Result<(), ProtocolError> {
    tx.put_serialized(dedup, record)?;
    tx.put(&expiry_key(record.applied_unix_ms, dedup)?, &[])
}

pub(super) fn expire(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectExpire,
) -> Result<Transition, ProtocolError> {
    let Some(config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if !matches!(group.kind, GroupKind::Data { .. })
        || command.max_items == 0
        || command.max_items > MAX_DIRECT_EXPIRE_ITEMS
        || command.cutoff_unix_ms == 0
    {
        return reject(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    // A split copies dedup records page by page; keep them stable meanwhile.
    if config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete) {
        return reject(tx, Fault::Incomplete);
    }
    let (deleted, complete) = sweep(
        tx,
        command.cutoff_unix_ms,
        command.max_items,
        APPLY.batch_bytes - (64 << 10),
    )?;
    commit(tx, &ParticipantReply::DirectExpired { deleted, complete })
}

/// Delete expired records, oldest first, within `max_items` and a byte
/// allowance so the batch stays inside its budget. Deterministic for a given
/// state. Returns the count and whether no expired record remains.
fn sweep(
    tx: &mut GroupTransaction<'_>,
    cutoff_unix_ms: u64,
    max_items: u16,
    mut allowance: usize,
) -> Result<(u64, bool), ProtocolError> {
    let mut cursor = vec![EXPIRY];
    let mut deleted = 0u64;
    loop {
        let Some(record) = tx.first_from(&[EXPIRY], &cursor)? else {
            return Ok((deleted, true));
        };
        let key = record.key();
        let stamp = key
            .get(1..9)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(recovery_unknown)?;
        if key.len() <= 9 {
            return Err(recovery_unknown());
        }
        if stamp >= cutoff_unix_ms {
            return Ok((deleted, true));
        }
        let cost = key
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(2 * OP_OVERHEAD))
            .ok_or_else(invalid)?;
        if deleted == u64::from(max_items) || cost > allowance {
            return Ok((deleted, false));
        }
        allowance -= cost;
        cursor.clear();
        cursor
            .try_reserve_exact(key.len() + 1)
            .map_err(|_| invalid())?;
        cursor.extend_from_slice(key);
        drop(record);
        let mut dedup = Vec::new();
        dedup
            .try_reserve_exact(cursor.len() - 8)
            .map_err(|_| invalid())?;
        dedup.push(DEDUP);
        dedup.extend_from_slice(&cursor[9..]);
        if tx.pinned(&dedup)?.is_none() {
            return Err(recovery_unknown());
        }
        tx.delete(&cursor)?;
        tx.delete(&dedup)?;
        deleted += 1;
        cursor.push(0);
    }
}

/// Install a dedup image carried by a split page or a restore. The record's
/// own commit timestamp must match the image.
pub(super) fn import_image(
    tx: &mut GroupTransaction<'_>,
    image: &super::image_archive::CommittedImage,
) -> Result<(), ProtocolError> {
    if !matches!(image.key, MvccKey::DirectDedup { .. }) {
        return Err(invalid());
    }
    let value = image.value.as_ref().ok_or_else(invalid)?;
    let record: DedupRecord<'_> = serde_json::from_str(value.as_str()).map_err(|_| invalid())?;
    if record.commit_ts != image.commit_ts {
        return Err(invalid());
    }
    let dedup = physical(DEDUP, &image.key)?;
    if let Some(existing) = tx.pinned(&dedup)? {
        return if existing.as_ref() == value.as_str().as_bytes() {
            Ok(())
        } else {
            Err(recovery_unknown())
        };
    }
    put_record(tx, &dedup, &record)
}

/// A dedup image for the committed-image traversal, or `None` when its
/// commit is above the cut.
pub(super) fn image_from_record(
    key: MvccKey,
    value: &[u8],
    read_ts: ReadTs,
    owner: Arc<ScratchLease>,
) -> Result<Option<super::image_archive::CommittedImage>, ProtocolError> {
    let record: DedupRecord<'_> = serde_json::from_slice(value).map_err(|_| recovery_unknown())?;
    if record.commit_ts > read_ts {
        return Ok(None);
    }
    Ok(Some(super::image_archive::CommittedImage {
        key,
        commit_ts: record.commit_ts,
        value: Some(CanonicalJson::from_stored_json(value, owner)?),
    }))
}

/// The highest direct commit timestamp applied on this group.
pub(super) fn high(tx: &mut GroupTransaction<'_>) -> Result<Option<CommitTs>, ProtocolError> {
    load::<CommitTs>(tx, DIRECT_HIGH)
}

impl crate::oltp_replication::owned::OwnedPayload for DirectCommit {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let mut bytes = self
            .writes
            .capacity()
            .checked_mul(std::mem::size_of::<DirectWrite>())
            .and_then(|n| n.checked_add(self.result.heap_bytes().ok()?))
            .ok_or_else(invalid)?;
        for write in &self.writes {
            bytes = write
                .index
                .capacity()
                .checked_mul(std::mem::size_of::<IndexChange>())
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(invalid)?;
        }
        for (key, _, value) in self.changes() {
            bytes = bytes
                .checked_add(key.heap_bytes())
                .and_then(|n| n.checked_add(value.map_or(Ok(0), |v| v.heap_bytes()).ok()?))
                .ok_or_else(invalid)?;
        }
        Ok(bytes)
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.result.attach_charge(owner.clone())?;
        for (_, _, value) in self.changes() {
            if let Some(value) = value {
                value.attach_charge(owner.clone())?;
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        self.result.is_charged()
            && self
                .changes()
                .all(|(_, _, value)| value.is_none_or(CanonicalJson::is_charged))
    }
}

#[cfg(test)]
mod tests;
