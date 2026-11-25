//! Background embedding at apply (`plans/oltp-vector.md`, section 6, spec
//! `specs/vector/embed/EmbedApply.tla`).
//!
//! `'e' ‖ row key ‖ column` holds the pending or the failure record of one
//! embedded column, never both. A record names the source version it waits for
//! and its own identity, the commit timestamp that created it, so a report that
//! was made for another text, another record or another attempt matches nothing.
//! (The plan calls the tag `'E'`, which the expiry index of dedup records has.)
use super::{
    feed::Changes,
    row::Version,
    search::{SearchReply, SearchSchema},
    *,
};
use crate::oltp::{
    mvcc::vector::{ColumnId, SearchColumn, VectorValue},
    pool::GroupRead,
};
use nabedra_oltp_types::types::Text;

pub(super) const EMBED: u8 = b'e';
/// Provider attempts before a record becomes a failure, and per requeue.
pub const EMBED_ATTEMPTS: u32 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedRecord {
    /// The commit timestamp that gave the row its current source text.
    pub source_version: CommitTs,
    pub id: CommitTs,
    pub attempts: u32,
    pub limit: u32,
    /// Set on a failure record: why the last attempt failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<Text<256>>,
}
impl EmbedRecord {
    pub(super) fn fresh(commit_ts: CommitTs) -> Self {
        Self::derived(commit_ts, commit_ts)
    }
    /// The record of a row that lacks its vector. A restore gives it an identity
    /// of its own.
    pub(super) fn derived(source_version: CommitTs, id: CommitTs) -> Self {
        Self {
            source_version,
            id,
            attempts: 0,
            limit: EMBED_ATTEMPTS,
            failed: None,
        }
    }
}
fn decode(raw: &[u8]) -> Result<EmbedRecord, ProtocolError> {
    if raw.len() > METADATA_BYTES {
        return Err(recovery_unknown());
    }
    serde_json::from_slice(raw).map_err(|_| recovery_unknown())
}
pub(super) fn load(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    column: ColumnId,
) -> Result<Option<EmbedRecord>, ProtocolError> {
    tx.pinned(&row::column_key(EMBED, logical, column, None)?)?
        .map(|raw| decode(&raw))
        .transpose()
}
pub(super) fn read(
    view: &mut GroupRead<'_>,
    logical: &[u8],
    column: ColumnId,
) -> Result<Option<EmbedRecord>, ProtocolError> {
    view.pinned(&row::column_key(EMBED, logical, column, None)?)?
        .map(|raw| decode(&raw))
        .transpose()
}
pub(super) fn put(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    column: ColumnId,
    record: &EmbedRecord,
) -> Result<(), ProtocolError> {
    tx.put_serialized(&row::column_key(EMBED, logical, column, None)?, record)
}
pub(super) fn delete(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    column: ColumnId,
) -> Result<(), ProtocolError> {
    tx.delete(&row::column_key(EMBED, logical, column, None)?)
}
/// Gives a restored row exactly the records its newest version needs.
pub(super) fn derive(
    tx: &mut GroupTransaction<'_>,
    logical: &[u8],
    image: Option<&row::RowImage>,
    id: CommitTs,
) -> Result<(), ProtocolError> {
    let pending: Vec<_> = image.into_iter().flat_map(|image| image.pending()).collect();
    let prefix = row::tagged(EMBED, logical, 0)?;
    let mut cursor = prefix.clone();
    while let Some(record) = tx.first_from(&prefix, &cursor)? {
        cursor = [record.key(), &[0]].concat();
        let [high, low] = record.key()[prefix.len()..] else {
            return Err(recovery_unknown());
        };
        let column = u16::from_be_bytes([high, low]);
        if !pending.iter().any(|(pending, _)| *pending == column) {
            delete(tx, logical, column)?;
        }
    }
    for (column, source_version) in pending {
        put(tx, logical, column, &EmbedRecord::derived(source_version, id))?;
    }
    Ok(())
}

/// Stores an embedding. An ordinary conditional direct commit of one row: it
/// applies only to the row version the leader read, and only if the row exists
/// and its source version and the column still match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetVector {
    pub authority: Authority,
    pub table_id: Uuid,
    /// The group's direct epoch, as for `DirectCommit`.
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub primary_key: Key,
    pub column: ColumnId,
    pub source_version: CommitTs,
    /// The row version the leader read.
    pub expected_version: CommitTs,
    pub commit_ts: CommitTs,
    pub value: VectorValue,
}
/// One provider attempt failed. It counts once, and only for the record, source
/// version and attempt it was made for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedFailed {
    pub authority: Authority,
    pub table_id: Uuid,
    pub primary_key: Key,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub id: CommitTs,
    /// Attempts the record counted when it was leased.
    pub attempt: u32,
    pub reason: Text<256>,
}
/// An operator lets a failed record try again. The attempt number goes on, so a
/// late report of an earlier attempt still matches nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequeueEmbedding {
    pub authority: Authority,
    pub table_id: Uuid,
    pub primary_key: Key,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub id: CommitTs,
}

pub(super) fn set_vector(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    position: AppliedPosition,
    command: &SetVector,
) -> Result<Transition, ProtocolError> {
    let Some(config) = config else {
        return search::rejected(tx, Fault::NotInitialized);
    };
    let (Some(direct_schema), Some(schema)) = (config.direct_schema, search::ready(config)) else {
        return search::rejected(tx, Fault::DirectInactive);
    };
    if direct::split_in_progress(config) {
        return search::rejected(tx, Fault::SplitInProgress);
    }
    if config.authority != command.authority {
        return search::rejected(tx, Fault::StaleAuthority);
    }
    if direct_schema.lifecycle.is_none()
        || direct_schema.epoch != command.schema_epoch
        || config.routing_epoch != command.routing_epoch
    {
        return search::rejected(tx, Fault::StaleEpoch);
    }
    let Some(logical) = owned_row(group, config, command.table_id, &command.primary_key)? else {
        return search::rejected(tx, Fault::InvalidBinding);
    };
    let Some(column) = embedded(schema, command.column) else {
        return search::reply(tx, SearchReply::Obsolete);
    };
    if !column.accepts(&command.value) {
        return search::rejected(tx, Fault::InvalidBinding);
    }
    let head = super::load::<Head>(tx, &row::tagged(HEAD, &logical, 0)?)?.unwrap_or_default();
    if head.intent.is_some() {
        return search::rejected(tx, Fault::Incomplete);
    }
    let Some(at) = head.version else {
        return search::reply(tx, SearchReply::Obsolete);
    };
    let raw = row::stored(tx, &logical, at)?;
    let old = Version::decode(&raw)?;
    if old.value.is_none() || old.source.get(column.id) != Some(command.source_version) {
        return search::reply(tx, SearchReply::Obsolete);
    }
    let waiting = load(tx, &logical, column.id)?.is_some();
    if old.blobs.get(column.id).is_some() && !waiting {
        return search::reply(tx, SearchReply::AlreadySet);
    }
    if at != command.expected_version
        || command.commit_ts <= at
        || command.commit_ts.get() <= direct::commit_floor(config)
    {
        return search::rejected(tx, Fault::Conflict);
    }
    let mut next = Version::plain(old.value);
    for column in &schema.columns {
        next.keep(&old, column.id);
    }
    next.blobs.0.insert(column.id, command.commit_ts);
    row::put_version(tx, &logical, command.commit_ts, &next)?;
    let vector = row::column_key(row::VECTOR, &logical, column.id, Some(command.commit_ts))?;
    row::put_vector(tx, &vector, &command.value)?;
    if waiting {
        delete(tx, &logical, column.id)?;
    }
    direct::raise_high(tx, command.commit_ts)?;
    let mut changes = Changes::new(config);
    changes.row(&logical, command.commit_ts, true)?;
    changes.flush(tx, position)?;
    search::reply(
        tx,
        SearchReply::Set {
            commit_ts: command.commit_ts,
        },
    )
}

pub(super) fn failed(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &EmbedFailed,
) -> Result<Transition, ProtocolError> {
    let logical = match admit(
        group,
        config,
        command.authority,
        command.table_id,
        &command.primary_key,
        command.column,
    )? {
        Ok(logical) => logical,
        Err(reply) => return search::reply(tx, reply),
    };
    let record = load(tx, &logical, command.column)?.filter(|record| {
        record.failed.is_none()
            && record.source_version == command.source_version
            && record.id == command.id
            && record.attempts == command.attempt
    });
    let Some(mut record) = record else {
        return search::reply(tx, SearchReply::Unmatched);
    };
    let Some(attempts) = record.attempts.checked_add(1) else {
        return search::rejected(tx, Fault::ResourceLimit);
    };
    record.attempts = attempts;
    let reply = if attempts >= record.limit {
        record.failed = Some(command.reason.clone());
        SearchReply::EmbedFailed { attempts }
    } else {
        SearchReply::Counted { attempts }
    };
    put(tx, &logical, command.column, &record)?;
    search::reply(tx, reply)
}

pub(super) fn requeue(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &RequeueEmbedding,
) -> Result<Transition, ProtocolError> {
    let logical = match admit(
        group,
        config,
        command.authority,
        command.table_id,
        &command.primary_key,
        command.column,
    )? {
        Ok(logical) => logical,
        Err(reply) => return search::reply(tx, reply),
    };
    let record = load(tx, &logical, command.column)?.filter(|record| {
        record.failed.is_some()
            && record.source_version == command.source_version
            && record.id == command.id
    });
    let Some(mut record) = record else {
        return search::reply(tx, SearchReply::Unmatched);
    };
    let Some(limit) = record.attempts.checked_add(EMBED_ATTEMPTS) else {
        return search::rejected(tx, Fault::ResourceLimit);
    };
    record.limit = limit;
    record.failed = None;
    put(tx, &logical, command.column, &record)?;
    let attempts = record.attempts;
    search::reply(tx, SearchReply::Requeued { attempts, limit })
}

/// The checks of an entry that touches a record but no row version. A frozen
/// range refuses it, since a split copies its records exactly.
fn admit(
    group: &GroupIdentity,
    config: Option<&Config>,
    authority: Authority,
    table_id: Uuid,
    primary_key: &Key,
    column: ColumnId,
) -> Result<Result<Vec<u8>, SearchReply>, ProtocolError> {
    let refuse = |fault| Ok(Err(SearchReply::Rejected(fault)));
    let Some(config) = config else {
        return refuse(Fault::NotInitialized);
    };
    if search::frozen(config) {
        return refuse(Fault::SplitInProgress);
    }
    if config.authority != authority {
        return refuse(Fault::StaleAuthority);
    }
    let Some(schema) = search::ready(config) else {
        return refuse(Fault::DirectInactive);
    };
    let Some(logical) = owned_row(group, config, table_id, primary_key)? else {
        return refuse(Fault::InvalidBinding);
    };
    Ok(embedded(schema, column)
        .map(|_| logical)
        .ok_or(SearchReply::Unmatched))
}
fn embedded(schema: &SearchSchema, id: ColumnId) -> Option<&SearchColumn> {
    schema.column(id).filter(|column| column.source.is_some())
}
/// The logical key of a row this group owns.
fn owned_row(
    group: &GroupIdentity,
    config: &Config,
    table_id: Uuid,
    primary_key: &Key,
) -> Result<Option<Vec<u8>>, ProtocolError> {
    let key = MvccKey::Row {
        table_id,
        primary_key: primary_key.clone(),
    };
    if key_length(&key).is_err() || !owns(group, &config.span, &key) {
        return Ok(None);
    }
    row::logical_key(&key).map(Some)
}
fn key_bytes(key: &Key) -> usize {
    match key {
        Key::Utf8(text) => text.capacity(),
        _ => 0,
    }
}
impl SetVector {
    pub(super) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.value
            .heap_bytes()?
            .checked_add(key_bytes(&self.primary_key))
            .ok_or_else(invalid)
    }
}
impl EmbedFailed {
    pub(super) fn heap_bytes(&self) -> usize {
        key_bytes(&self.primary_key) + self.reason.capacity_bytes()
    }
}
impl RequeueEmbedding {
    pub(super) fn heap_bytes(&self) -> usize {
        key_bytes(&self.primary_key)
    }
}

/// One waiting or failed embedding, for the leader's lease and for status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedItem {
    pub key: MvccKey,
    pub column: ColumnId,
    pub record: EmbedRecord,
}
impl ParticipantMachine {
    /// Up to `max_items` records of this replica's applied state, in key order
    /// after `after`. The caller took the read barrier.
    pub async fn embed_records(
        &self,
        after: Option<(MvccKey, ColumnId)>,
        max_items: u16,
    ) -> Result<Vec<EmbedItem>, ProtocolError> {
        const KEYS: usize = 512 << 10;
        if max_items == 0 || max_items > 128 {
            return Err(invalid());
        }
        let raw = self
            .store
            .read(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 8 << 20,
                },
                move |view| {
                    let mut cursor = match &after {
                        Some((key, column)) => {
                            let logical = row::logical_key(key)?;
                            let mut cursor = row::column_key(EMBED, &logical, *column, None)?;
                            cursor.push(0);
                            cursor
                        }
                        None => vec![EMBED],
                    };
                    let mut items = Vec::new();
                    let mut bytes = 0;
                    while items.len() < usize::from(max_items) && bytes < KEYS {
                        let Some(found) = view.first_from(&[EMBED], &cursor)? else {
                            break;
                        };
                        let key = found.key();
                        let (logical, column) = key[1..]
                            .split_last_chunk::<2>()
                            .ok_or_else(recovery_unknown)?;
                        bytes += key.len();
                        items.push(EmbedItem {
                            key: archive::decode_logical(logical)?,
                            column: u16::from_be_bytes(*column),
                            record: decode(found.value())?,
                        });
                        cursor = [key, &[0]].concat();
                    }
                    view.encode(&items)
                },
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
}
impl ParticipantHandle {
    /// `ParticipantMachine::embed_records` behind the leader's read barrier.
    pub async fn embed_records_local(
        &self,
        after: Option<(MvccKey, ColumnId)>,
        max_items: u16,
    ) -> Result<Vec<EmbedItem>, ProtocolError> {
        if self.remote.is_some() {
            return Err(invalid());
        }
        self.local_consensus().ensure_linearizable_lease().await?;
        self.local_machine().embed_records(after, max_items).await
    }
}
