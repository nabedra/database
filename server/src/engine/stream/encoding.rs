//! The batch row count is independent of its encoding. Count past an over-cap
//! prefix so a short tail can still be discarded when drop_last is set.
use super::{BatchFormat, BatchOptions, PinnedScan, error, resource};
use crate::protocol::{ColumnType, ErrorCode, ProtocolError, Row, Scalar};
use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::{
    io::{self, Write},
    sync::Arc,
};

pub(super) struct BatchEncoder {
    options: BatchOptions,
    rows: usize,
    // None records a prefix that cannot fit the encoded byte cap.
    output: Option<Output>,
}

enum Output {
    Rows(BoundedSink),
    Arrow(ArrowBatch),
}

impl BatchEncoder {
    pub(super) fn budget(scan: &PinnedScan, options: BatchOptions) -> u64 {
        let multiplier = match options.format {
            BatchFormat::Rows => 2,
            // Arrow builders grow geometrically. Retain room for their buffers,
            // IPC message staging/alignment and the final encoded stream.
            BatchFormat::Arrow => 8,
        };
        (options.max_batch_bytes * multiplier + scan.output_columns().len() * 2048 + 65536) as u64
    }

    pub(super) fn new(scan: &PinnedScan, options: BatchOptions) -> Result<Self, ProtocolError> {
        let output = match options.format {
            BatchFormat::Rows => {
                let mut sink = BoundedSink::new(options.max_batch_bytes);
                match sink.write_all(b"{\"rows\":[") {
                    Ok(()) => Some(Output::Rows(sink)),
                    Err(_) if sink.exceeded => None,
                    Err(_) => return Err(sink.failure()),
                }
            }
            BatchFormat::Arrow => Some(Output::Arrow(ArrowBatch::new(scan))),
        };
        Ok(Self {
            options,
            rows: 0,
            output,
        })
    }

    pub(super) fn full(&self) -> bool {
        self.rows == self.options.batch_rows
    }

    pub(super) fn push(&mut self, scan: &PinnedScan, row: &Row) -> Result<(), ProtocolError> {
        let fits = match &mut self.output {
            Some(Output::Rows(sink)) => {
                let result = (|| {
                    if self.rows > 0 {
                        sink.write_all(b",")?;
                    }
                    serde_json::to_writer(&mut *sink, &ProjectedRow(scan.output_columns(), row))
                        .map_err(io::Error::from)
                })();
                if result.is_err() && !sink.exceeded {
                    return Err(sink.failure());
                }
                !sink.exceeded
            }
            Some(Output::Arrow(batch)) => {
                batch.push(scan, row, self.rows, self.options.max_batch_bytes)
            }
            None => false,
        };
        if !fits {
            self.output = None;
        }
        self.rows += 1;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<Vec<u8>>, ProtocolError> {
        if self.rows == 0 || (self.options.drop_last && !self.full()) {
            return Ok(None);
        }
        let bytes = match self.output.ok_or_else(too_large)? {
            Output::Rows(mut sink) => {
                sink.write_all(b"]}").map_err(|_| sink.failure())?;
                sink.bytes
            }
            Output::Arrow(batch) => batch.finish(self.options.max_batch_bytes)?,
        };
        Ok(Some(bytes))
    }
}

struct ProjectedRow<'a>(&'a [String], &'a Row);
impl Serialize for ProjectedRow<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for name in self.0 {
            map.serialize_entry(name, &self.1[name])?;
        }
        map.end()
    }
}

struct ArrowBatch {
    columns: Vec<ArrowColumn>,
    schema: Arc<Schema>,
    payload: usize,
}
impl ArrowBatch {
    fn new(scan: &PinnedScan) -> Self {
        let columns: Vec<_> = scan
            .output_columns()
            .iter()
            .map(|name| ArrowColumn::new(scan.table.schema[name]))
            .collect();
        let schema = Arc::new(Schema::new(
            scan.output_columns()
                .iter()
                .zip(&columns)
                .map(|(name, column)| Field::new(name, column.data_type(), false))
                .collect::<Vec<_>>(),
        ));
        Self {
            columns,
            schema,
            payload: 0,
        }
    }

    fn push(&mut self, scan: &PinnedScan, row: &Row, row_index: usize, limit: usize) -> bool {
        // Reject before builders grow when even the minimally sized IPC body
        // would exceed the cap. The bounded writer also counts headers/padding.
        let payload = scan
            .output_columns()
            .iter()
            .try_fold(self.payload, |sum, name| {
                let bytes = match &row[name] {
                    Scalar::Utf8(value) => value.len() + 4,
                    Scalar::Int64(_) | Scalar::Float64(_) => 8,
                    Scalar::Bool(_) => usize::from(row_index.is_multiple_of(8)),
                    _ => unreachable!("OLAP columns are scalar"),
                };
                sum.checked_add(bytes).filter(|sum| *sum <= limit)
            });
        let Some(payload) = payload else {
            return false;
        };
        self.payload = payload;
        for (column, name) in self.columns.iter_mut().zip(scan.output_columns()) {
            column.push(&row[name]);
        }
        true
    }

    fn finish(self, limit: usize) -> Result<Vec<u8>, ProtocolError> {
        let arrays = self.columns.into_iter().map(ArrowColumn::finish).collect();
        let batch =
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(|_| encode_error())?;
        let mut sink = BoundedSink::new(limit);
        let written = (|| {
            let mut writer = StreamWriter::try_new(&mut sink, &self.schema)?;
            writer.write(&batch)?;
            writer.finish()
        })();
        if written.is_err() {
            return Err(sink.failure());
        }
        Ok(sink.bytes)
    }
}

enum ArrowColumn {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
}
impl ArrowColumn {
    fn new(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Utf8 => Self::Utf8(StringBuilder::with_capacity(0, 0)),
            ColumnType::Int64 => Self::Int64(Int64Builder::with_capacity(0)),
            ColumnType::Float64 => Self::Float64(Float64Builder::with_capacity(0)),
            ColumnType::Bool => Self::Bool(BooleanBuilder::with_capacity(0)),
            _ => unreachable!("OLAP columns are scalar"),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Utf8(_) => DataType::Utf8,
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::Bool(_) => DataType::Boolean,
        }
    }

    fn push(&mut self, value: &Scalar) {
        match (self, value) {
            (Self::Utf8(builder), Scalar::Utf8(value)) => builder.append_value(value),
            (Self::Int64(builder), Scalar::Int64(value)) => builder.append_value(*value),
            (Self::Float64(builder), Scalar::Float64(value)) => builder.append_value(*value),
            (Self::Bool(builder), Scalar::Bool(value)) => builder.append_value(*value),
            _ => unreachable!("validated projection"),
        }
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
        }
    }
}

struct BoundedSink {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
    allocation_failed: bool,
}
impl BoundedSink {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
            allocation_failed: false,
        }
    }

    fn failure(&self) -> ProtocolError {
        if self.exceeded {
            too_large()
        } else if self.allocation_failed {
            resource()
        } else {
            encode_error()
        }
    }
}
impl Write for BoundedSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|end| *end <= self.limit);
        let Some(end) = end else {
            self.exceeded = true;
            return Err(io::Error::other("stream batch byte limit"));
        };
        if end > self.bytes.capacity() {
            let capacity = (self.bytes.capacity() * 2).max(end).max(64).min(self.limit);
            if let Err(err) = self.bytes.try_reserve_exact(capacity - self.bytes.len()) {
                self.allocation_failed = true;
                return Err(io::Error::other(err));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn too_large() -> ProtocolError {
    error(
        ErrorCode::LimitExceeded,
        "encoded batch exceeds the byte cap; choose fewer rows or columns",
    )
}

fn encode_error() -> ProtocolError {
    error(ErrorCode::InternalError, "stream batch encoding failed")
}
