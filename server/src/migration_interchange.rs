//! Engine-neutral offline interchange. The archive contains only one captured
//! logical snapshot; request bodies, receipts and source run histories are absent.
//! Each frame is bounded independently, so table/archive size has no fixed cap.
use crate::protocol::{self, ErrorCode, Key, Outcome, ProtocolError, Row, Scalar, TableDescriptor};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::io::{Read, Write};
use uuid::Uuid;
const MAGIC: &[u8; 8] = b"NABMIG1\n";
pub const MAX_RECORD_BYTES: usize = 32 << 20;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceEngine {
    Olap,
    Oltp,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveHeader {
    pub format_version: u32,
    pub archive_id: Uuid,
    pub source_engine: SourceEngine,
    pub source_snapshot: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSummary {
    pub table_id: Uuid,
    pub rows: u64,
    pub ordered_rows_hash: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSummary {
    pub tables: u64,
    pub rows: u64,
    pub prefix_hash: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDescriptor {
    pub index_id: Uuid,
    pub name: String,
    pub columns: Vec<String>,
    pub covering: Vec<String>,
}
#[derive(Debug)]
pub enum Record {
    Header(ArchiveHeader),
    Table(TableDescriptor),
    Row(Row),
    Index(IndexDescriptor),
    EndTable(TableSummary),
    EndArchive(ArchiveSummary),
}
struct TableState {
    descriptor: TableDescriptor,
    last: Option<Key>,
    rows: u64,
    hash: Sha256,
}
struct State {
    prefix: [u8; 32],
    table: Option<TableState>,
    tables: u64,
    rows: u64,
    started: bool,
    finished: bool,
}
impl State {
    fn new() -> Self {
        Self {
            prefix: [0; 32],
            table: None,
            tables: 0,
            rows: 0,
            started: false,
            finished: false,
        }
    }
    fn validate(&mut self, record: &Record, payload_hash: [u8; 32]) -> Result<(), ProtocolError> {
        if self.finished {
            return Err(invalid("data follows archive footer"));
        }
        match record {
            Record::Header(header) => {
                if self.started || header.format_version != 1 || header.archive_id.is_nil() {
                    return Err(invalid("invalid interchange header"));
                }
                self.started = true;
            }
            Record::Table(table) => {
                if !self.started || self.table.is_some() {
                    return Err(invalid("invalid table boundary"));
                }
                table.validate()?;
                self.table = Some(TableState {
                    descriptor: table.clone(),
                    last: None,
                    rows: 0,
                    hash: Sha256::new(),
                });
            }
            Record::Row(row) => {
                let table = self
                    .table
                    .as_mut()
                    .ok_or_else(|| invalid("row outside table"))?;
                protocol::validate_row(row, &table.descriptor, protocol::RowMode::Complete)?;
                let key = match &row[&table.descriptor.key] {
                    Scalar::Int64(v) => Key::Int64(*v),
                    Scalar::Utf8(v) => Key::Utf8(v.clone()),
                    _ => return Err(invalid("invalid migration primary key")),
                };
                if table
                    .last
                    .as_ref()
                    .is_some_and(|last| compare_key(last, &key) != std::cmp::Ordering::Less)
                {
                    return Err(invalid("rows are not in unique primary-key order"));
                }
                table.last = Some(key);
                table.rows = table.rows.checked_add(1).ok_or_else(limit)?;
                table.hash.update(payload_hash);
            }
            Record::Index(index) => {
                let table = self
                    .table
                    .as_ref()
                    .ok_or_else(|| invalid("index outside table"))?;
                if index.index_id.is_nil()
                    || index.columns.is_empty()
                    || index.columns.len() > 64
                    || index.covering.len() > 64
                {
                    return Err(invalid("invalid archived index"));
                }
                protocol::validate_identifier(&index.name)?;
                for columns in [&index.columns, &index.covering] {
                    let mut seen = std::collections::BTreeSet::new();
                    for name in columns {
                        if !table.descriptor.schema.contains_key(name) || !seen.insert(name) {
                            return Err(invalid("invalid archived index columns"));
                        }
                    }
                }
            }
            Record::EndTable(summary) => {
                let table = self
                    .table
                    .take()
                    .ok_or_else(|| invalid("table footer without table"))?;
                if summary.table_id != table.descriptor.id
                    || summary.rows != table.rows
                    || summary.ordered_rows_hash != <[u8; 32]>::from(table.hash.finalize())
                {
                    return Err(invalid("table count/hash mismatch"));
                }
                self.tables = self.tables.checked_add(1).ok_or_else(limit)?;
                self.rows = self.rows.checked_add(table.rows).ok_or_else(limit)?;
            }
            Record::EndArchive(summary) => {
                if !self.started
                    || self.table.is_some()
                    || summary.tables != self.tables
                    || summary.rows != self.rows
                    || summary.prefix_hash != self.prefix
                {
                    return Err(invalid("archive count/hash mismatch"));
                }
                self.finished = true;
            }
        }
        Ok(())
    }
    fn advance(&mut self, tag: u8, len: u64, payload: [u8; 32]) {
        let mut hash = Sha256::new();
        hash.update(b"nabedra/interchange-chain/v1\0");
        hash.update(self.prefix);
        hash.update([tag]);
        hash.update(len.to_be_bytes());
        hash.update(payload);
        self.prefix = hash.finalize().into();
    }
}
pub struct ArchiveWriter<W> {
    output: W,
    state: State,
}
impl<W: Write> ArchiveWriter<W> {
    pub fn new(mut output: W, header: ArchiveHeader) -> Result<Self, ProtocolError> {
        output.write_all(MAGIC).map_err(io_error)?;
        let mut writer = Self {
            output,
            state: State::new(),
        };
        writer.record(Record::Header(header))?;
        Ok(writer)
    }
    pub fn begin_table(&mut self, table: TableDescriptor) -> Result<(), ProtocolError> {
        self.record(Record::Table(table))
    }
    pub fn row(&mut self, row: Row) -> Result<(), ProtocolError> {
        self.record(Record::Row(row))
    }
    pub fn index(&mut self, index: IndexDescriptor) -> Result<(), ProtocolError> {
        self.record(Record::Index(index))
    }
    pub fn end_table(&mut self) -> Result<TableSummary, ProtocolError> {
        let table = self
            .state
            .table
            .as_ref()
            .ok_or_else(|| invalid("no open table"))?;
        let summary = TableSummary {
            table_id: table.descriptor.id,
            rows: table.rows,
            ordered_rows_hash: table.hash.clone().finalize().into(),
        };
        self.record(Record::EndTable(summary.clone()))?;
        Ok(summary)
    }
    pub fn finish(mut self) -> Result<(W, ArchiveSummary), ProtocolError> {
        let summary = ArchiveSummary {
            tables: self.state.tables,
            rows: self.state.rows,
            prefix_hash: self.state.prefix,
        };
        self.record(Record::EndArchive(summary.clone()))?;
        self.output.flush().map_err(io_error)?;
        Ok((self.output, summary))
    }
    fn record(&mut self, record: Record) -> Result<(), ProtocolError> {
        let (tag, len, digest) = match &record {
            Record::Header(v) => measure(1, v)?,
            Record::Table(v) => measure(2, v)?,
            Record::Row(v) => measure(3, v)?,
            Record::EndTable(v) => measure(4, v)?,
            Record::EndArchive(v) => measure(5, v)?,
            Record::Index(v) => measure(6, v)?,
        };
        self.state.validate(&record, digest)?;
        self.output
            .write_all(&[tag])
            .and_then(|_| self.output.write_all(&len.to_be_bytes()))
            .and_then(|_| self.output.write_all(&digest))
            .map_err(io_error)?;
        match &record {
            Record::Header(v) => serde_json::to_writer(&mut self.output, v),
            Record::Table(v) => serde_json::to_writer(&mut self.output, v),
            Record::Row(v) => serde_json::to_writer(&mut self.output, v),
            Record::EndTable(v) => serde_json::to_writer(&mut self.output, v),
            Record::EndArchive(v) => serde_json::to_writer(&mut self.output, v),
            Record::Index(v) => serde_json::to_writer(&mut self.output, v),
        }
        .map_err(|_| invalid("archive serialization failed"))?;
        self.state.advance(tag, len, digest);
        Ok(())
    }
}
/// Offline single-frame decoder. The caller supplies a hard working allowance.
/// Lexical counting bounds the encoded buffer plus decoded allocation before
/// serde can grow maps/strings. Returned records retain that caller allowance.
pub struct ArchiveReader<R> {
    input: R,
    state: State,
    working_bytes: usize,
}
impl<R: Read> ArchiveReader<R> {
    pub fn new(mut input: R, working_bytes: usize) -> Result<Self, ProtocolError> {
        let mut magic = [0; 8];
        input.read_exact(&mut magic).map_err(io_error)?;
        if &magic != MAGIC {
            return Err(invalid("foreign migration format"));
        }
        Ok(Self {
            input,
            state: State::new(),
            working_bytes,
        })
    }
    pub fn next_record(&mut self) -> Result<Option<Record>, ProtocolError> {
        if self.state.finished {
            let mut byte = [0];
            if self.input.read(&mut byte).map_err(io_error)? != 0 {
                return Err(invalid("trailing archive bytes"));
            }
            return Ok(None);
        }
        let mut header = [0; 41];
        self.input.read_exact(&mut header).map_err(io_error)?;
        let len = usize::try_from(u64::from_be_bytes(header[1..9].try_into().expect("fixed")))
            .map_err(|_| limit())?;
        if len == 0 || len > MAX_RECORD_BYTES || len > self.working_bytes {
            return Err(limit());
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| limit())?;
        bytes.resize(len, 0);
        self.input.read_exact(&mut bytes).map_err(io_error)?;
        protocol::json_input(&bytes, MAX_RECORD_BYTES)
            .map_err(|_| invalid("invalid archive JSON bounds"))?;
        let mut structure = 0usize;
        let (mut quoted, mut escaped) = (false, false);
        for byte in &bytes {
            if quoted {
                if escaped {
                    escaped = false;
                } else if *byte == b'\\' {
                    escaped = true;
                } else if *byte == b'"' {
                    quoted = false;
                }
            } else if *byte == b'"' {
                quoted = true;
            } else if matches!(*byte, b'{' | b'[' | b',' | b':') {
                structure += 1;
            }
        }
        let charge = len
            .checked_mul(4)
            .and_then(|n| structure.checked_mul(768).and_then(|m| n.checked_add(m)))
            .and_then(|n| n.checked_add(64 << 10))
            .ok_or_else(limit)?;
        if charge > self.working_bytes {
            return Err(limit());
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if digest.as_slice() != &header[9..] {
            return Err(invalid("archive frame checksum mismatch"));
        }
        let decode = || invalid("invalid migration record");
        let record = match header[0] {
            1 => Record::Header(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            2 => Record::Table(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            3 => Record::Row(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            4 => Record::EndTable(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            5 => Record::EndArchive(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            6 => Record::Index(serde_json::from_slice(&bytes).map_err(|_| decode())?),
            _ => return Err(invalid("unknown migration record")),
        };
        self.state.validate(&record, digest)?;
        self.state.advance(header[0], len as u64, digest);
        Ok(Some(record))
    }
}
fn measure(tag: u8, value: &impl Serialize) -> Result<(u8, u64, [u8; 32]), ProtocolError> {
    struct Counter {
        len: usize,
        hash: Sha256,
    }
    impl Write for Counter {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.len = self
                .len
                .checked_add(b.len())
                .filter(|n| *n <= MAX_RECORD_BYTES)
                .ok_or_else(|| std::io::Error::other("record too large"))?;
            self.hash.update(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut out = Counter {
        len: 0,
        hash: Sha256::new(),
    };
    serde_json::to_writer(&mut out, value).map_err(|_| limit())?;
    Ok((tag, out.len as u64, out.hash.finalize().into()))
}
fn compare_key(a: &Key, b: &Key) -> std::cmp::Ordering {
    match (a, b) {
        (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
        (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn limit() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::LimitExceeded,
        "migration frame exceeds working memory",
        Outcome::NotApplied,
    )
}
fn io_error(_: std::io::Error) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "migration archive I/O failed",
        Outcome::Unknown,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn migration_interchange_roundtrip_order_and_corruption() {
        let table = TableDescriptor {
            id: Uuid::new_v4(),
            name: "typed".into(),
            schema: std::collections::BTreeMap::from([
                ("id".into(), crate::protocol::ColumnType::Int64),
                ("text".into(), crate::protocol::ColumnType::Utf8),
            ]),
            key: "id".into(),
            format: crate::protocol::TableFormat::Parquet,
        };
        let header = ArchiveHeader {
            format_version: 1,
            archive_id: Uuid::new_v4(),
            source_engine: SourceEngine::Olap,
            source_snapshot: [11; 32],
        };
        let mut writer = ArchiveWriter::new(Vec::new(), header.clone()).unwrap();
        writer.begin_table(table.clone()).unwrap();
        for key in [i64::MIN, -1, 0, i64::MAX] {
            writer
                .row(Row::from([
                    ("id".into(), Scalar::Int64(key)),
                    (
                        "text".into(),
                        Scalar::Utf8("commas,,,,quotes\"escape\\\0".into()),
                    ),
                ]))
                .unwrap();
        }
        assert_eq!(writer.end_table().unwrap().rows, 4);
        let (archive, summary) = writer.finish().unwrap();
        assert_eq!((summary.tables, summary.rows), (1, 4));
        let mut reader = ArchiveReader::new(archive.as_slice(), 4 << 20).unwrap();
        let mut rows = 0;
        while let Some(record) = reader.next_record().unwrap() {
            if matches!(record, Record::Row(_)) {
                rows += 1;
            }
        }
        assert_eq!(rows, 4);
        for index in [8, 50, archive.len() - 1] {
            let mut broken = archive.clone();
            broken[index] ^= 1;
            let mut reader = ArchiveReader::new(broken.as_slice(), 4 << 20).unwrap();
            let mut rejected = false;
            loop {
                match reader.next_record() {
                    Err(_) => {
                        rejected = true;
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(_)) => {}
                }
            }
            assert!(rejected, "corruption at {index}");
        }
        let mut reader = ArchiveReader::new(&archive[..archive.len() - 1], 4 << 20).unwrap();
        loop {
            match reader.next_record() {
                Err(_) => break,
                Ok(None) => panic!("truncated archive accepted"),
                Ok(Some(_)) => {}
            }
        }
        let mut writer = ArchiveWriter::new(Vec::new(), header).unwrap();
        writer.begin_table(table).unwrap();
        let row = Row::from([
            ("id".into(), Scalar::Int64(1)),
            ("text".into(), Scalar::Utf8("x".into())),
        ]);
        writer.row(row.clone()).unwrap();
        assert!(writer.row(row).is_err());
    }
}
