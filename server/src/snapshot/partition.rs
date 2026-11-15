//! Pure CPU partition encoding. The caller assigns storage identities later.
use super::{check_envelope, encode_into, error, footer, limit, row_payload, validate_rows};
use crate::{
    partition_size::{MAX_PARTITION_BYTES, PartitionSize},
    protocol::{ErrorCode, Key, ProtocolError, Row, Scalar, TableDescriptor},
    storage::KeyRange,
};
use std::io::{self, Write};

/// A complete, closed Parquet file with logical metadata and no storage identity.
#[derive(Debug)]
pub struct EncodedPartition {
    pub range: KeyRange,
    pub row_count: u64,
    /// Uses the same scalar accounting as `snapshot::validate_rows`.
    pub payload_bytes: u64,
    pub bytes: Vec<u8>,
}

fn key(table: &TableDescriptor, row: &Row) -> Key {
    match &row[&table.key] {
        Scalar::Int64(n) => Key::Int64(*n),
        Scalar::Utf8(s) => Key::Utf8(s.clone()),
        _ => unreachable!("validated primary key"),
    }
}

/// Validate once, sort by typed key, then split oversized closed files at the
/// middle key. Bounds are inclusive below and exclusive above. A singleton may
/// exceed the soft target, but every file must fit the hard cap and footer guard.
/// This synchronous function does no I/O and can run wholly on a worker thread.
pub fn encode_partition_range(
    table: &TableDescriptor,
    rows: Vec<Row>,
    range: KeyRange,
    target: PartitionSize,
) -> Result<Vec<EncodedPartition>, ProtocolError> {
    let mut files = Vec::new();
    encode_partition_range_each(table, rows, range, target, |part| {
        files.push(part);
        Ok(())
    })?;
    Ok(files)
}

/// Emit one closed file at a time. A rejected emission stops all further encoding.
pub(crate) fn encode_partition_range_each(
    table: &TableDescriptor,
    mut rows: Vec<Row>,
    range: KeyRange,
    target: PartitionSize,
    mut emit: impl FnMut(EncodedPartition) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError> {
    validate_rows(table, &rows)?;
    let ty = table.schema[&table.key];
    let invalid_range = || error(ErrorCode::ValidationError, "invalid partition key range");
    range.validate(ty).map_err(|_| invalid_range())?;
    for row in &rows {
        if !range
            .contains(&key(table, row), ty)
            .map_err(|_| invalid_range())?
        {
            return Err(invalid_range());
        }
    }
    rows.sort_unstable_by(|a, b| match (&a[&table.key], &b[&table.key]) {
        (Scalar::Int64(a), Scalar::Int64(b)) => a.cmp(b),
        (Scalar::Utf8(a), Scalar::Utf8(b)) => a.cmp(b),
        _ => unreachable!("validated primary key"),
    });
    if rows.is_empty() {
        return Ok(());
    }
    let mut pending = vec![(0, rows.len(), range)];
    let working = super::memory_shape(table, rows.len(), rows.iter().map(row_payload).sum(), 0)?;
    let mut retained_encoded = 0u64;
    while let Some((start, end, range)) = pending.pop() {
        let slice = &rows[start..end];
        let simultaneous = retained_encoded
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(working))
            .ok_or_else(limit)?;
        let attempt = encode_bounded(table, slice, simultaneous);
        let split = match &attempt {
            Ok(bytes) => bytes.len() > target.bytes() && slice.len() > 1,
            Err(e) => e.code == ErrorCode::LimitExceeded && slice.len() > 1,
        };
        if split {
            // Drop a soft-target overflow before retrying the smaller children.
            drop(attempt);
            let middle = start + slice.len() / 2;
            let boundary = key(table, &rows[middle]);
            pending.push((
                middle,
                end,
                KeyRange {
                    lower: Some(boundary.clone()),
                    upper: range.upper,
                },
            ));
            pending.push((
                start,
                middle,
                KeyRange {
                    lower: range.lower,
                    upper: Some(boundary),
                },
            ));
        } else {
            let bytes = attempt?;
            retained_encoded = retained_encoded
                .checked_add(bytes.len() as u64)
                .ok_or_else(limit)?;
            emit(EncodedPartition {
                range,
                row_count: slice.len() as u64,
                payload_bytes: slice.iter().map(row_payload).sum::<usize>() as u64,
                bytes,
            })?;
        }
    }
    Ok(())
}

fn encode_bounded(
    table: &TableDescriptor,
    rows: &[Row],
    working_bytes: u64,
) -> Result<Vec<u8>, ProtocolError> {
    let mut sink = BoundedSink {
        working_bytes,
        ..Default::default()
    };
    let result = encode_into(table, rows, &mut sink);
    if sink.exceeded {
        return Err(limit());
    }
    result?;
    // Use exactly the decoder's envelope and raw metadata preflight. Never
    // publish a file that its footer guard would reject.
    footer::validate(check_envelope(&sink.bytes)?, table.schema.len())?;
    Ok(sink.bytes)
}

#[derive(Default)]
struct BoundedSink {
    bytes: Vec<u8>,
    exceeded: bool,
    working_bytes: u64,
}
impl Write for BoundedSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = MAX_PARTITION_BYTES - self.bytes.len();
        if bytes.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::other("partition byte limit exceeded"));
        }
        let charged = (self.bytes.len() as u64)
            .checked_add(bytes.len() as u64)
            .and_then(|encoded| encoded.checked_mul(4))
            .and_then(|encoded| encoded.checked_add(self.working_bytes));
        if charged.is_none_or(|charged| crate::scheduling::check_task_memory(charged).is_err()) {
            self.exceeded = true;
            return Err(io::Error::other("partition working memory limit exceeded"));
        }
        // Exact growth prevents Vec's geometric reservation from exceeding the
        // file cap. This bounds the output sink, not Arrow's working memory.
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
