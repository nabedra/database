//! Synchronous, bounded snapshot codec. Run on a blocking worker.
use std::{collections::BTreeSet, sync::Arc};

mod footer;
mod partition;
pub(crate) mod stream;

// Remove the unused-import allowance when R02a2 wires the captured handler.
#[allow(unused_imports)]
pub(crate) use partition::encode_partition_range_each;
pub use partition::{EncodedPartition, encode_partition_range};

use crate::protocol::{
    self, ColumnType, ErrorCode, MAX_DECODED_PAYLOAD_BYTES, MAX_TABLE_ROWS, Outcome, ProtocolError,
    Row, RowMode, Scalar, TableDescriptor,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    },
    basic::{Compression, Encoding, Type},
    column::page::Page,
    file::{
        properties::{EnabledStatistics, WriterProperties},
        reader::FileReader,
        serialized_reader::SerializedFileReader,
    },
};

/// R04 must pass this limit to Storage::read before collecting snapshot bytes.
/// Plain values <=64 MiB; 16 MiB headroom covers page headers and metadata.
pub const MAX_ENCODED_SNAPSHOT_BYTES: usize = 80 * 1024 * 1024;
const MAX_FOOTER_BYTES: usize = 1024 * 1024;

fn error(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
fn stored() -> ProtocolError {
    error(ErrorCode::StorageUnavailable, "invalid stored snapshot")
}
fn limit() -> ProtocolError {
    error(ErrorCode::LimitExceeded, "snapshot limit exceeded")
}
fn stored_validation(e: ProtocolError) -> ProtocolError {
    if e.code == ErrorCode::LimitExceeded {
        e
    } else {
        stored()
    }
}

/// Payload accounting: 8 bytes per row, plus 8 + UTF-8 column-name length
/// per cell, plus value bytes (UTF-8 length, 8 for int/float, 1 for bool).
/// This deterministic logical size is not a bound on Rust allocator overhead.
pub(crate) fn row_payload(row: &Row) -> usize {
    8 + row
        .iter()
        .map(|(name, value)| {
            8 + name.len()
                + match value {
                    Scalar::Utf8(s) => s.len(),
                    Scalar::Int64(_) | Scalar::Float64(_) => 8,
                    Scalar::Bool(_) => 1,
                    _ => unreachable!("OLAP columns are scalar"),
                }
        })
        .sum::<usize>()
}

/// Validate complete rows, exact types, finite floats, unique string/integer
/// keys, row count and payload. Returns accounted bytes for future mutations.
pub fn validate_rows(table: &TableDescriptor, rows: &[Row]) -> Result<usize, ProtocolError> {
    table.validate()?;
    if rows.len() > MAX_TABLE_ROWS {
        return Err(limit());
    }
    check_rows_memory(table, rows, 0)?;
    let mut keys = BTreeSet::new();
    let mut payload = 0usize;
    for row in rows {
        protocol::validate_row(row, table, RowMode::Complete)?;
        payload = payload.checked_add(row_payload(row)).ok_or_else(limit)?;
        if payload > MAX_DECODED_PAYLOAD_BYTES {
            return Err(limit());
        }
        let key = match &row[&table.key] {
            Scalar::Utf8(s) => (Some(s.as_str()), 0),
            Scalar::Int64(i) => (None, *i),
            _ => {
                return Err(error(
                    ErrorCode::ValidationError,
                    "invalid primary key type",
                ));
            }
        };
        if !keys.insert(key) {
            return Err(error(ErrorCode::Conflict, "duplicate primary key"));
        }
    }
    Ok(payload)
}
/// Conservative simultaneous charge: encoded input/output and codec scratch,
/// BTree nodes, cloned names/scalars, candidate rows, Arrow buffers and sorting.
/// This logical bound is separate from allocator RSS; allocation failure remains fallible where supported.
pub(crate) fn memory_shape(
    table: &TableDescriptor,
    count: usize,
    payload: usize,
    encoded: usize,
) -> Result<u64, ProtocolError> {
    let cells = count.checked_mul(table.schema.len()).ok_or_else(limit)?;
    let bytes = cells
        .checked_mul(768)
        .and_then(|n| count.checked_mul(256).and_then(|v| n.checked_add(v)))
        .and_then(|n| payload.checked_mul(4).and_then(|v| n.checked_add(v)))
        .and_then(|n| encoded.checked_mul(4).and_then(|v| n.checked_add(v)))
        .ok_or_else(limit)?;
    Ok(bytes as u64)
}
pub(crate) fn check_memory_shape(
    table: &TableDescriptor,
    count: usize,
    payload: usize,
    encoded: usize,
) -> Result<(), ProtocolError> {
    crate::scheduling::check_task_memory(memory_shape(table, count, payload, encoded)?)
}
pub(crate) fn check_rows_memory(
    table: &TableDescriptor,
    rows: &[Row],
    encoded: usize,
) -> Result<(), ProtocolError> {
    let payload = rows.iter().try_fold(0usize, |sum, row| {
        sum.checked_add(row_payload(row)).ok_or_else(limit)
    })?;
    check_memory_shape(table, rows.len(), payload, encoded)
}
fn schema(table: &TableDescriptor) -> Schema {
    Schema::new(
        table
            .schema
            .iter()
            .map(|(name, ty)| {
                Field::new(
                    name,
                    match ty {
                        ColumnType::Utf8 => DataType::Utf8,
                        ColumnType::Int64 => DataType::Int64,
                        ColumnType::Float64 => DataType::Float64,
                        ColumnType::Bool => DataType::Boolean,
                        _ => unreachable!("OLAP columns are scalar"),
                    },
                    false,
                )
            })
            .collect::<Vec<_>>(),
    )
}
fn array(name: &str, ty: ColumnType, rows: &[Row]) -> ArrayRef {
    match ty {
        ColumnType::Utf8 => Arc::new(StringArray::from_iter_values(rows.iter().map(|r| match &r
            [name]
        {
            Scalar::Utf8(v) => v.as_str(),
            _ => unreachable!(),
        }))),
        ColumnType::Int64 => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| {
            match r[name] {
                Scalar::Int64(v) => v,
                _ => unreachable!(),
            }
        }))),
        ColumnType::Float64 => Arc::new(Float64Array::from_iter_values(rows.iter().map(
            |r| match r[name] {
                Scalar::Float64(v) => v,
                _ => unreachable!(),
            },
        ))),
        ColumnType::Bool => Arc::new(BooleanArray::from(
            rows.iter()
                .map(|r| match r[name] {
                    Scalar::Bool(v) => v,
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        )),
        _ => unreachable!("OLAP columns are scalar"),
    }
}

/// Validate before constructing Arrow arrays. Return only a closed Parquet file.
pub fn encode(table: &TableDescriptor, rows: &[Row]) -> Result<Vec<u8>, ProtocolError> {
    validate_rows(table, rows)?;
    let mut sink = SnapshotSink {
        bytes: Vec::new(),
        exceeded: false,
        working_bytes: memory_shape(table, rows.len(), rows.iter().map(row_payload).sum(), 0)?,
    };
    let encoded = encode_into(table, rows, &mut sink);
    if sink.exceeded {
        return Err(limit());
    }
    encoded?;
    let bytes = sink.bytes;
    if bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES {
        return Err(limit());
    }
    Ok(bytes)
}

fn encode_into<W: std::io::Write + Send>(
    table: &TableDescriptor,
    rows: &[Row],
    sink: W,
) -> Result<(), ProtocolError> {
    check_rows_memory(
        table,
        rows,
        MAX_ENCODED_SNAPSHOT_BYTES.min(rows.iter().map(row_payload).sum::<usize>()),
    )?;
    let schema = Arc::new(schema(table));
    let arrays = table
        .schema
        .iter()
        .map(|(name, ty)| array(name, *ty, rows))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|_| stored())?;
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(MAX_TABLE_ROWS))
        .set_max_row_group_bytes(None)
        .build();
    let mut writer = ArrowWriter::try_new(sink, schema, Some(props)).map_err(|_| stored())?;
    writer.write(&batch).map_err(|_| stored())?;
    writer.close().map_err(|_| stored())?;
    Ok(())
}

/// Expected descriptor is mandatory. Supply catalog row_count to check it here;
/// callers passing None must check their catalog count themselves.
/// Only this codec's flat required, uncompressed PLAIN V1 profile is supported.
pub fn decode(
    table: &TableDescriptor,
    bytes: Bytes,
    expected_rows: Option<u64>,
) -> Result<Vec<Row>, ProtocolError> {
    table.validate().map_err(stored_validation)?;
    // Expected codec failures use Result. The worker's outer job boundary contains unwinding panics.
    decode_inner(table, bytes, expected_rows)
}
fn check_envelope(bytes: &[u8]) -> Result<&[u8], ProtocolError> {
    if bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES {
        return Err(limit());
    }
    if bytes.len() < 12 || &bytes[..4] != b"PAR1" || &bytes[bytes.len() - 4..] != b"PAR1" {
        return Err(stored());
    }
    let footer = u32::from_le_bytes(
        bytes[bytes.len() - 8..bytes.len() - 4]
            .try_into()
            .map_err(|_| stored())?,
    ) as usize;
    if footer > MAX_FOOTER_BYTES {
        return Err(limit());
    }
    if footer > bytes.len() - 12 {
        return Err(stored());
    }
    Ok(&bytes[bytes.len() - 8 - footer..bytes.len() - 8])
}
fn decode_inner(
    table: &TableDescriptor,
    bytes: Bytes,
    expected_rows: Option<u64>,
) -> Result<Vec<Row>, ProtocolError> {
    let (builder, count, _) = validated_reader(table, bytes, expected_rows, true)?;
    let reader = builder.with_batch_size(1).build().map_err(|_| stored())?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(count).map_err(|_| limit())?;
    let mut payload = 0usize;
    for batch in reader {
        let batch = batch.map_err(|_| stored())?;
        for i in 0..batch.num_rows() {
            if rows.len() >= MAX_TABLE_ROWS || rows.len() >= count {
                return Err(stored());
            }
            let mut row = Row::new();
            for ((name, ty), arr) in table.schema.iter().zip(batch.columns()) {
                if arr.is_null(i) {
                    return Err(stored());
                }
                let value = scalar(arr, *ty, i)?;
                row.insert(name.clone(), value);
            }
            payload = payload.checked_add(row_payload(&row)).ok_or_else(limit)?;
            if payload > MAX_DECODED_PAYLOAD_BYTES {
                return Err(limit());
            }
            rows.push(row);
        }
    }
    if rows.len() != count {
        return Err(stored());
    }
    validate_rows(table, &rows).map_err(stored_validation)?;
    Ok(rows)
}
fn validated_reader(
    table: &TableDescriptor,
    bytes: Bytes,
    expected_rows: Option<u64>,
    materialize: bool,
) -> Result<(ParquetRecordBatchReaderBuilder<Bytes>, usize, usize), ProtocolError> {
    // Check raw schema declarations before either reader constructs schema
    // types. A capped footer alone cannot bound num_children allocations.
    crate::scheduling::check_task_memory((bytes.len() as u64).checked_mul(4).ok_or_else(limit)?)?;
    footer::validate(check_envelope(&bytes)?, table.schema.len())?;
    let file = SerializedFileReader::new(bytes.clone()).map_err(|_| stored())?;
    let metadata = file.metadata();
    let count = usize::try_from(metadata.file_metadata().num_rows()).map_err(|_| stored())?;
    if count > MAX_TABLE_ROWS {
        return Err(limit());
    }
    if expected_rows.is_some_and(|n| n != count as u64) {
        return Err(stored());
    }
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let encoded_len = bytes.len() as u64;
    if materialize {
        check_memory_shape(table, count, 0, bytes.len())?;
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
        .map_err(|_| stored())?;
    if builder.schema().as_ref() != &schema(table) {
        return Err(stored());
    }
    // Check physical schema independently of embedded Arrow metadata.
    let descriptor = metadata.file_metadata().schema_descr();
    if descriptor.num_columns() != table.schema.len() {
        return Err(stored());
    }
    for (col, ty) in descriptor.columns().iter().zip(table.schema.values()) {
        let physical = match ty {
            ColumnType::Utf8 => Type::BYTE_ARRAY,
            ColumnType::Int64 => Type::INT64,
            ColumnType::Float64 => Type::DOUBLE,
            ColumnType::Bool => Type::BOOLEAN,
            _ => return Err(stored()),
        };
        if col.physical_type() != physical {
            return Err(stored());
        }
        if col.max_def_level() != 0 || col.max_rep_level() != 0 {
            return Err(stored());
        }
    }
    let payload = preflight(&file, table, count, encoded_len)?;
    if materialize {
        check_memory_shape(table, count, payload, encoded_len as usize)?;
    }
    Ok((builder, count, payload))
}

fn scalar(arr: &ArrayRef, ty: ColumnType, i: usize) -> Result<Scalar, ProtocolError> {
    Ok(match ty {
        ColumnType::Utf8 => {
            let value = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(stored)?
                .value(i);
            let mut owned = String::new();
            owned.try_reserve_exact(value.len()).map_err(|_| limit())?;
            owned.push_str(value);
            Scalar::Utf8(owned)
        }
        ColumnType::Int64 => Scalar::Int64(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(stored)?
                .value(i),
        ),
        ColumnType::Float64 => Scalar::Float64(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(stored)?
                .value(i),
        ),
        ColumnType::Bool => Scalar::Bool(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(stored)?
                .value(i),
        ),
        _ => return Err(stored()),
    })
}

// Scan uncompressed buffers before Arrow allocates value arrays. PLAIN and no
// dictionaries exclude length/dictionary expansion. Metadata alone is untrusted.
fn preflight(
    file: &SerializedFileReader<Bytes>,
    table: &TableDescriptor,
    count: usize,
    encoded_len: u64,
) -> Result<usize, ProtocolError> {
    let mut total_rows = 0usize;
    let overhead: usize = 8 + table.schema.keys().map(|s| 8 + s.len()).sum::<usize>();
    let mut payload = count.checked_mul(overhead).ok_or_else(limit)?;
    if payload > MAX_DECODED_PAYLOAD_BYTES {
        return Err(limit());
    }
    for g in 0..file.num_row_groups() {
        let group = file.get_row_group(g).map_err(|_| stored())?;
        let n = usize::try_from(group.metadata().num_rows()).map_err(|_| stored())?;
        total_rows = total_rows.checked_add(n).ok_or_else(limit)?;
        if total_rows > count {
            return Err(stored());
        }
        for (i, ty) in table.schema.values().enumerate() {
            let col = group.metadata().column(i);
            if col.compression() != Compression::UNCOMPRESSED || col.num_values() != n as i64 {
                return Err(stored());
            }
            if col.uncompressed_size() < 0 || col.compressed_size() < 0 {
                return Err(stored());
            }
            if col.uncompressed_size() as u64 > MAX_ENCODED_SNAPSHOT_BYTES as u64
                || col.compressed_size() as u64 > MAX_ENCODED_SNAPSHOT_BYTES as u64
            {
                return Err(limit());
            }
            let (start, len) = col.byte_range();
            if start.checked_add(len).is_none_or(|end| end > encoded_len) {
                return Err(stored());
            }
            let mut pages = group.get_column_page_reader(i).map_err(|_| stored())?;
            let mut values = 0usize;
            while let Some(meta) = pages.peek_next_page().map_err(|_| stored())? {
                if meta.is_dict {
                    return Err(stored());
                }
                let nv = meta.num_levels.ok_or_else(stored)?;
                if nv == 0 || nv > n.saturating_sub(values) {
                    return Err(stored());
                }
                let page = pages
                    .get_next_page()
                    .map_err(|_| stored())?
                    .ok_or_else(stored)?;
                let Page::DataPage {
                    ref buf,
                    num_values,
                    encoding: Encoding::PLAIN,
                    ..
                } = page
                else {
                    return Err(stored());
                };
                if num_values as usize != nv {
                    return Err(stored());
                }
                let size = plain_payload(buf, *ty, nv)?;
                payload = payload.checked_add(size).ok_or_else(limit)?;
                if payload > MAX_DECODED_PAYLOAD_BYTES {
                    return Err(limit());
                }
                values += nv;
            }
            if values != n {
                return Err(stored());
            }
        }
    }
    if total_rows != count {
        return Err(stored());
    }
    Ok(payload)
}
fn plain_payload(mut buf: &[u8], ty: ColumnType, n: usize) -> Result<usize, ProtocolError> {
    match ty {
        ColumnType::Utf8 => {
            let mut size = 0usize;
            for _ in 0..n {
                if buf.len() < 4 {
                    return Err(stored());
                }
                let len = u32::from_le_bytes(buf[..4].try_into().map_err(|_| stored())?) as usize;
                buf = &buf[4..];
                if len > MAX_DECODED_PAYLOAD_BYTES {
                    return Err(limit());
                }
                if len > buf.len() || std::str::from_utf8(&buf[..len]).is_err() {
                    return Err(stored());
                }
                size = size.checked_add(len).ok_or_else(limit)?;
                buf = &buf[len..];
            }
            if !buf.is_empty() {
                return Err(stored());
            }
            Ok(size)
        }
        ColumnType::Int64 | ColumnType::Float64 => {
            if buf.len() != n * 8 {
                return Err(stored());
            }
            if ty == ColumnType::Float64
                && buf
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .any(|b| !f64::from_le_bytes(*b).is_finite())
            {
                return Err(stored());
            }
            Ok(n * 8)
        }
        ColumnType::Bool => {
            if buf.len() != n.div_ceil(8) {
                return Err(stored());
            }
            Ok(n)
        }
        _ => Err(stored()),
    }
}

struct SnapshotSink {
    bytes: Vec<u8>,
    exceeded: bool,
    working_bytes: u64,
}
impl std::io::Write for SnapshotSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES - self.bytes.len() {
            self.exceeded = true;
            return Err(std::io::Error::other("snapshot byte limit"));
        }
        let charged = (self.bytes.len() as u64)
            .checked_add(bytes.len() as u64)
            .and_then(|encoded| encoded.checked_mul(4))
            .and_then(|encoded| encoded.checked_add(self.working_bytes));
        if charged.is_none_or(|charged| crate::scheduling::check_task_memory(charged).is_err()) {
            self.exceeded = true;
            return Err(std::io::Error::other("snapshot working memory limit"));
        }
        self.bytes
            .try_reserve_exact(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use crate::protocol::TableFormat;
    fn table() -> TableDescriptor {
        TableDescriptor {
            id: uuid::Uuid::nil(),
            name: "memory".into(),
            key: "id".into(),
            format: TableFormat::Parquet,
            schema: [
                ("id".into(), ColumnType::Int64),
                ("text".into(), ColumnType::Utf8),
            ]
            .into(),
            search: Default::default(),
        }
    }
    fn row(text: String) -> Row {
        [
            ("id".into(), Scalar::Int64(1)),
            ("text".into(), Scalar::Utf8(text)),
        ]
        .into()
    }
    #[test]
    fn m01_wide_strings_extreme_counts_and_encoded_growth_fail_before_allocation() {
        let table = table();
        let rows = vec![row("x".repeat(16 * 1024))];
        let bytes = encode(&table, &rows).unwrap();
        crate::scheduling::with_task_limit(1024, || {
            assert_eq!(
                encode(&table, &rows).unwrap_err().code,
                ErrorCode::LimitExceeded
            );
            assert_eq!(
                decode(&table, bytes.clone().into(), Some(1))
                    .unwrap_err()
                    .code,
                ErrorCode::LimitExceeded
            );
            assert!(check_memory_shape(&table, usize::MAX, usize::MAX, usize::MAX).is_err());
            let mut sink = SnapshotSink {
                bytes: Vec::new(),
                exceeded: false,
                working_bytes: 1000,
            };
            assert!(std::io::Write::write(&mut sink, &[0; 8]).is_err());
            assert!(sink.bytes.is_empty());
        });
        // Task-local limits leave subsequent codec work usable.
        assert_eq!(decode(&table, bytes.into(), Some(1)).unwrap(), rows);
        let mut wide = table.clone();
        for index in 0..62 {
            wide.schema
                .insert(format!("column{index}"), ColumnType::Int64);
        }
        crate::scheduling::with_task_limit(4096, || {
            assert!(check_memory_shape(&wide, 1, 0, 0).is_err());
        });
    }
}
