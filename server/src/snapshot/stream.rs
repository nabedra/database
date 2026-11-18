//! Parquet preflight shared with ordinary reads, followed by a projected reader
//! at a physical row offset. The stream can move the reader across worker jobs.
use super::{scalar, stored, validated_reader};
use crate::{
    protocol::{Key, ProtocolError, Row, Scalar, TableDescriptor},
    storage::PartitionFile,
};
use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};
use std::cmp::Ordering;

const DECODE_ROWS: usize = 64;

pub(crate) struct ProjectedReader {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    offset: usize,
}

impl ProjectedReader {
    pub(crate) fn open(
        table: &TableDescriptor,
        bytes: Bytes,
        partition: &PartitionFile,
        columns: Vec<usize>,
        raw_offset: u64,
        raw_rows: u64,
    ) -> Result<Self, ProtocolError> {
        let (builder, _, payload) =
            validated_reader(table, bytes.clone(), Some(partition.row_count), false)?;
        if payload as u64 != partition.payload_bytes {
            return Err(stored());
        }
        validate_keys(table, partition, bytes)?;

        // Determine the first intersecting row group before applying the
        // relative offset. No Arrow value arrays are built for skipped groups.
        let raw_end = raw_offset.checked_add(raw_rows).ok_or_else(stored)?;
        let mut first_start = None;
        let mut group_end = 0_u64;
        let mut row_groups = Vec::new();
        for (index, group) in builder.metadata().row_groups().iter().enumerate() {
            let start = group_end;
            group_end = start
                .checked_add(u64::try_from(group.num_rows()).map_err(|_| stored())?)
                .ok_or_else(stored)?;
            if group_end > raw_offset && start < raw_end && group_end > start {
                first_start.get_or_insert(start);
                row_groups.push(index);
            }
        }
        let skip =
            usize::try_from(raw_offset - first_start.ok_or_else(stored)?).map_err(|_| stored())?;
        let read = usize::try_from(raw_rows).map_err(|_| stored())?;
        let mask = ProjectionMask::leaves(builder.parquet_schema(), columns);
        let reader = builder
            .with_row_groups(row_groups)
            .with_offset(skip)
            .with_limit(read)
            .with_projection(mask)
            .with_batch_size(DECODE_ROWS)
            .build()
            .map_err(|_| stored())?;
        Ok(Self {
            reader,
            batch: None,
            offset: 0,
        })
    }

    pub(crate) fn next_row(&mut self, table: &TableDescriptor) -> Result<Row, ProtocolError> {
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| self.offset == batch.num_rows())
        {
            self.batch = Some(
                self.reader
                    .next()
                    .ok_or_else(stored)?
                    .map_err(|_| stored())?,
            );
            self.offset = 0;
        }
        let batch = self.batch.as_ref().ok_or_else(stored)?;
        let mut row = Row::new();
        for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
            let ty = *table.schema.get(field.name()).ok_or_else(stored)?;
            if array.is_null(self.offset) {
                return Err(stored());
            }
            row.insert(field.name().clone(), scalar(array, ty, self.offset)?);
        }
        self.offset += 1;
        Ok(row)
    }
}

/// Match ordinary partition integrity checks, including every key in a file
/// whose shard intersects only a subset of its physical rows.
fn validate_keys(
    table: &TableDescriptor,
    partition: &PartitionFile,
    bytes: Bytes,
) -> Result<(), ProtocolError> {
    let key_index = table
        .schema
        .keys()
        .position(|column| column == &table.key)
        .ok_or_else(stored)?;
    let key_type = table.schema[&table.key];
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes,
        super::ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .map_err(|_| stored())?;
    let mask = ProjectionMask::leaves(builder.parquet_schema(), [key_index]);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(DECODE_ROWS)
        .build()
        .map_err(|_| stored())?;
    let mut previous: Option<Key> = None;
    let mut count = 0;
    for batch in reader {
        let batch = batch.map_err(|_| stored())?;
        let array = batch.column(0);
        for index in 0..batch.num_rows() {
            if array.is_null(index) {
                return Err(stored());
            }
            let key = match scalar(array, key_type, index)? {
                Scalar::Int64(value) => Key::Int64(value),
                Scalar::Utf8(value) => Key::Utf8(value),
                _ => return Err(stored()),
            };
            if !partition
                .range
                .contains(&key, key_type)
                .map_err(|_| stored())?
            {
                return Err(stored());
            }
            let ordered = match (&previous, &key) {
                (None, _) => true,
                (Some(Key::Int64(a)), Key::Int64(b)) => a.cmp(b) == Ordering::Less,
                (Some(Key::Utf8(a)), Key::Utf8(b)) => a.cmp(b) == Ordering::Less,
                _ => false,
            };
            if !ordered {
                return Err(stored());
            }
            previous = Some(key);
            count += 1;
        }
    }
    if count != partition.row_count {
        return Err(stored());
    }
    Ok(())
}
