//! Synchronous, bounded snapshot codec. Run on a blocking worker.
use std::{collections::BTreeSet, sync::Arc};

mod footer;

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
fn row_payload(row: &Row) -> usize {
    8 + row
        .iter()
        .map(|(name, value)| {
            8 + name.len()
                + match value {
                    Scalar::Utf8(s) => s.len(),
                    Scalar::Int64(_) | Scalar::Float64(_) => 8,
                    Scalar::Bool(_) => 1,
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
    }
}

/// Validate before constructing Arrow arrays. Return only a closed Parquet file.
pub fn encode(table: &TableDescriptor, rows: &[Row]) -> Result<Vec<u8>, ProtocolError> {
    validate_rows(table, rows)?;
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
    let mut bytes = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema, Some(props)).map_err(|_| stored())?;
        writer.write(&batch).map_err(|_| stored())?;
        writer.close().map_err(|_| stored())?;
    }
    if bytes.len() > MAX_ENCODED_SNAPSHOT_BYTES {
        return Err(limit());
    }
    Ok(bytes)
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
    // Some malformed Parquet headers cause dependency panics. Keep them at the
    // stored-data boundary; no raw parser errors or row values reach clients.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        decode_inner(table, bytes, expected_rows)
    }))
    .map_err(|_| stored())?
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
    // Check raw schema declarations before either reader constructs schema
    // types. A capped footer alone cannot bound num_children allocations.
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
        };
        if col.physical_type() != physical {
            return Err(stored());
        }
        if col.max_def_level() != 0 || col.max_rep_level() != 0 {
            return Err(stored());
        }
    }
    preflight(&file, table, count, encoded_len)?;
    let reader = builder.with_batch_size(1).build().map_err(|_| stored())?;
    let mut rows = Vec::new();
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
fn scalar(arr: &ArrayRef, ty: ColumnType, i: usize) -> Result<Scalar, ProtocolError> {
    Ok(match ty {
        ColumnType::Utf8 => Scalar::Utf8(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(stored)?
                .value(i)
                .into(),
        ),
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
    })
}

// Scan uncompressed buffers before Arrow allocates value arrays. PLAIN and no
// dictionaries exclude length/dictionary expansion. Metadata alone is untrusted.
fn preflight(
    file: &SerializedFileReader<Bytes>,
    table: &TableDescriptor,
    count: usize,
    encoded_len: u64,
) -> Result<(), ProtocolError> {
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
    Ok(())
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
    }
}
