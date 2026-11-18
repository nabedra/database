use std::collections::HashSet;

use anyhow::Context;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{
    Error, ErrorCode, Event, HEAD_KEY, Head, MAX_BATCH_BYTES, MAX_BATCH_EVENTS, MAX_EVENT_BYTES,
    MAX_PAGE_BYTES, MAX_PAGE_EVENTS, MAX_RETAIN_EVENTS, MAX_TIMESTAMP_MS, Poll, PublishEvent,
    Published, Reply, Status, Step, StorageResult, Transaction, Uuid, as_outcome, load, store,
    validate_time,
};

const EVENT_PREFIX: &[u8] = b"partition/v1/event/";
const DEDUP_PREFIX: &[u8] = b"partition/v1/dedup/";
const BATCH_WRAPPER_BYTES: usize = b"{\"events\":[]}".len();
// Reserve room for the checkpoint, signed delivery token, and response fields.
const PAGE_WRAPPER_BYTES: usize = 4096;
// Leave room for command and storage envelopes within the JSON parser's depth limit.
const MAX_DATA_DEPTH: usize = 64;
// Raft snapshots cap images at 64 GiB. Leave 1 GiB for framing, the head,
// and up to MAX_SUBSCRIPTIONS_PER_PARTITION subscription records.
const MAX_RETAINED_EVENT_BYTES: u64 = 63 * 1024 * 1024 * 1024;

#[cfg(test)]
mod tests;

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredEvent<T = Event> {
    published_ms: i64,
    event: T,
}

struct EventRecords {
    event_key: Vec<u8>,
    event_value: Vec<u8>,
    dedup_key: Vec<u8>,
    dedup_value: Vec<u8>,
}

pub(super) fn publish(
    tx: &mut Transaction<'_>,
    head: &mut Head,
    now_ms: i64,
    events: Vec<PublishEvent>,
) -> Step<Published> {
    validate_time(now_ms)?;
    if events.is_empty() || events.len() > MAX_BATCH_EVENTS {
        return Err(Error::new(ErrorCode::InvalidRequest, "invalid batch count").into());
    }

    let published_ms = now_ms
        .max(head.last_published_ms)
        .max(head.retention_cutoff_ms);
    let published_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(published_ms) * 1_000_000)
            .context("invalid publish time")?
            .format(&Rfc3339)
            .context("invalid RFC 3339 publish time")?;
    let mut publish_ids = HashSet::with_capacity(events.len());
    let mut request_bytes = BATCH_WRAPPER_BYTES;
    let mut response_bytes = BATCH_WRAPPER_BYTES;
    let mut next_offset = head.next_offset;
    let mut retained_event_bytes = head.retained_event_bytes;
    let mut inserted = Vec::with_capacity(events.len());
    let mut receipts = Vec::with_capacity(events.len());

    for (index, input) in events.into_iter().enumerate() {
        if !valid_data_depth(&input.data, MAX_DATA_DEPTH) {
            return Err(
                Error::new(ErrorCode::InvalidRequest, "event data is nested too deeply").into(),
            );
        }
        let event_size = json_size(&input)?;
        if event_size > MAX_EVENT_BYTES {
            return Err(
                Error::new(ErrorCode::PayloadTooLarge, "event exceeds the byte limit").into(),
            );
        }
        request_bytes += event_size + usize::from(index > 0);
        if request_bytes > MAX_BATCH_BYTES {
            return Err(
                Error::new(ErrorCode::PayloadTooLarge, "batch exceeds the byte limit").into(),
            );
        }
        if !publish_ids.insert(input.event_id) {
            return Err(
                Error::new(ErrorCode::Conflict, "event ID repeats within the batch").into(),
            );
        }

        let dedup_key = dedup_key(input.event_id);
        let event = if let Some(offset) = load::<u64>(tx, &dedup_key)? {
            let committed = load_event(tx, offset)?.event;
            // JSON objects are canonically serialized with sorted keys. Byte comparison also
            // preserves numeric distinctions that a semantic Value comparison could collapse.
            if serde_json::to_vec(&committed.data).context("encode committed event data")?
                != serde_json::to_vec(&input.data).context("encode incoming event data")?
            {
                return Err(Error::new(ErrorCode::Conflict, "event ID has different data").into());
            }
            committed
        } else {
            let offset = next_offset;
            next_offset = offset.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::Conflict,
                    "partition has exhausted its offset space",
                )
            })?;
            let event = Event {
                topic_id: head.topic_id,
                partition: head.partition,
                offset,
                event_id: input.event_id,
                published_at: published_at.clone(),
                data: input.data,
            };
            let records = EventRecords {
                event_key: event_key(offset),
                event_value: serde_json::to_vec(&StoredEvent {
                    published_ms,
                    event: &event,
                })
                .context("encode retained event")?,
                dedup_key,
                dedup_value: offset.to_string().into_bytes(),
            };
            let record_bytes = snapshot_record_bytes(&records.event_key, records.event_value.len())
                + snapshot_record_bytes(&records.dedup_key, records.dedup_value.len());
            retained_event_bytes = retained_event_bytes
                .checked_add(record_bytes)
                .context("retained event byte counter overflowed")?;
            inserted.push(records);
            event
        };
        response_bytes += json_size(&event)? + usize::from(index > 0);
        if response_bytes > MAX_BATCH_BYTES {
            return Err(Error::new(
                ErrorCode::PayloadTooLarge,
                "batch receipts exceed the byte limit",
            )
            .into());
        }
        receipts.push(event);
    }

    if retained_event_bytes > MAX_RETAINED_EVENT_BYTES {
        return Err(Error::new(
            ErrorCode::InsufficientStorage,
            "partition has reached its retained event byte limit",
        )
        .into());
    }

    // Validate the complete logical batch before mutating the transaction. A rejected
    // batch leaves prior batches in a coalesced command intact and consumes no offsets.
    for records in &inserted {
        tx.put(&records.event_key, &records.event_value)?;
        tx.put(&records.dedup_key, &records.dedup_value)?;
    }
    if !inserted.is_empty() {
        head.next_offset = next_offset;
        head.retained_event_bytes = retained_event_bytes;
        head.last_published_ms = published_ms;
        store(tx, HEAD_KEY, head)?;
    }

    Ok(Published {
        events: receipts,
        inserted: inserted.len(),
    })
}

pub(super) fn publish_many(
    tx: &mut Transaction<'_>,
    head: &mut Head,
    now_ms: i64,
    batches: Vec<Vec<PublishEvent>>,
) -> Step<Reply> {
    if batches.is_empty()
        || batches.len() > MAX_BATCH_EVENTS
        || batches.iter().map(Vec::len).sum::<usize>() > MAX_BATCH_EVENTS
    {
        return Err(Error::new(ErrorCode::InvalidRequest, "invalid coalesced batch count").into());
    }
    if json_size(&batches)? > MAX_BATCH_BYTES {
        return Err(Error::new(
            ErrorCode::PayloadTooLarge,
            "coalesced batches exceed the byte limit",
        )
        .into());
    }
    let results = batches
        .into_iter()
        .map(|events| as_outcome(publish(tx, head, now_ms, events)))
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(Reply::Coalesced { results })
}

pub(super) fn retain(
    tx: &mut Transaction<'_>,
    head: &mut Head,
    cutoff_ms: i64,
    limit: usize,
) -> Step<Reply> {
    if cutoff_ms > MAX_TIMESTAMP_MS || !(1..=MAX_RETAIN_EVENTS).contains(&limit) {
        return Err(Error::new(ErrorCode::InvalidRequest, "invalid retention bounds").into());
    }

    let cutoff_ms = cutoff_ms.max(head.retention_cutoff_ms);
    let mut removed = 0;
    while removed < limit && head.earliest_offset < head.next_offset {
        let key = event_key(head.earliest_offset);
        let (oldest, event_value_bytes) = read_event(tx, &key)?;
        if oldest.published_ms >= cutoff_ms {
            break;
        }

        let dedup_key = dedup_key(oldest.event.event_id);
        let deleted_bytes = snapshot_record_bytes(&key, event_value_bytes)
            + snapshot_record_bytes(&dedup_key, head.earliest_offset.to_string().len());
        head.retained_event_bytes = head
            .retained_event_bytes
            .checked_sub(deleted_bytes)
            .context("retained event byte counter is inconsistent")?;
        tx.delete(&key)?;
        tx.delete(&dedup_key)?;
        head.earliest_offset += 1;
        removed += 1;
    }
    if removed > 0 || cutoff_ms != head.retention_cutoff_ms {
        head.retention_cutoff_ms = cutoff_ms;
        store(tx, HEAD_KEY, head)?;
    }

    Ok(Reply::Retained {
        removed,
        earliest_offset: head.earliest_offset,
    })
}

pub(super) fn page(tx: &Transaction<'_>, head: &Head, after: u64, limit: usize) -> Step<Poll> {
    if !(1..=MAX_PAGE_EVENTS).contains(&limit) {
        return Err(Error::new(ErrorCode::InvalidRequest, "invalid poll limit").into());
    }
    if after < head.first_checkpoint() {
        return Err(head.expired().into());
    }
    if after > head.tail() {
        return Err(anyhow::anyhow!("subscription checkpoint exceeds the partition tail").into());
    }

    let mut events = Vec::with_capacity(limit);
    let mut response_bytes = PAGE_WRAPPER_BYTES;
    for offset in (after + 1..head.next_offset).take(limit) {
        let event = load_event(tx, offset)?.event;
        response_bytes += json_size(&event)? + usize::from(!events.is_empty());
        if response_bytes > MAX_PAGE_BYTES {
            break;
        }
        events.push(event);
    }
    Ok(Poll {
        after,
        last: events.last().map(|event| event.offset),
        events,
    })
}

pub(super) fn status(tx: &Transaction<'_>, head: &Head) -> StorageResult<Status> {
    let oldest_published_ms = (head.earliest_offset < head.next_offset)
        .then(|| load_event(tx, head.earliest_offset).map(|event| event.published_ms))
        .transpose()?;
    Ok(Status {
        topic_id: head.topic_id,
        partition: head.partition,
        earliest_offset: head.earliest_offset,
        tail: head.tail(),
        oldest_published_ms,
    })
}

fn load_event(tx: &Transaction<'_>, offset: u64) -> StorageResult<StoredEvent> {
    Ok(read_event(tx, &event_key(offset))?.0)
}

fn read_event(tx: &Transaction<'_>, key: &[u8]) -> StorageResult<(StoredEvent, usize)> {
    let bytes = tx.get(key)?.context("missing retained event")?;
    let event = serde_json::from_slice(&bytes).context("invalid retained event on disk")?;
    Ok((event, bytes.len()))
}

fn snapshot_record_bytes(key: &[u8], value_bytes: usize) -> u64 {
    // Snapshot records prefix the key and value with their u32 lengths.
    (2 * size_of::<u32>() + key.len() + value_bytes) as u64
}

fn valid_data_depth(data: &serde_json::Value, remaining: usize) -> bool {
    use serde_json::Value;
    match data {
        Value::Object(entries) => {
            remaining > 0
                && entries
                    .values()
                    .all(|entry| valid_data_depth(entry, remaining - 1))
        }
        Value::Array(entries) => {
            remaining > 0
                && entries
                    .iter()
                    .all(|entry| valid_data_depth(entry, remaining - 1))
        }
        _ => true,
    }
}

fn json_size(value: &impl serde::Serialize) -> StorageResult<usize> {
    Ok(serde_json::to_vec(value)?.len())
}

fn event_key(offset: u64) -> Vec<u8> {
    [EVENT_PREFIX, &offset.to_be_bytes()].concat()
}

fn dedup_key(event_id: Uuid) -> Vec<u8> {
    [DEDUP_PREFIX, event_id.as_bytes()].concat()
}
