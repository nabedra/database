use crate::{
    engine::{compare_rows, matches_filter},
    node::{
        job::{
            JobEnvelope, JobError, JobResult, MAX_INTERNAL_ROW_BYTES, ResultEnvelope, Task,
            validate_get_rows,
        },
        spool::{SpoolOperation, SpoolPayload, SpoolWriter},
    },
    protocol::{self, ColumnType, ErrorCode, Row, RowMode, Scalar, TableDescriptor},
    snapshot,
};
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use std::{
    fmt,
    io::{self, BufRead, BufReader, BufWriter, Write},
};

const NUMERIC_BYTES: usize = 128;
fn invalid() -> JobError {
    JobError {
        code: ErrorCode::ValidationError,
        message: "invalid row payload".into(),
    }
}
fn check(ok: bool) -> Result<(), JobError> {
    if ok { Ok(()) } else { Err(invalid()) }
}

// Validate constructed envelopes before any schema indexing or row comparison.
fn bounds(result: &ResultEnvelope, job: &JobEnvelope) -> Result<(usize, usize), JobError> {
    result.validate_for(job).map_err(|_| invalid())?;
    match (&job.task, &result.result) {
        (
            Task::Get { .. },
            JobResult::Get {
                found,
                source_payload_bytes,
                ..
            },
        ) => Ok((usize::from(*found), *source_payload_bytes as usize)),
        (
            Task::Collect { .. },
            JobResult::Collect {
                matched_rows,
                source_payload_bytes,
                ..
            },
        ) => Ok((*matched_rows as usize, *source_payload_bytes as usize)),
        _ => Err(invalid()),
    }
}
pub(crate) fn validate(
    rows: &[Row],
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<(), JobError> {
    let (count, bytes) = bounds(result, job)?;
    check(rows.len() == count)?;
    let payload = snapshot::validate_rows(&job.table, rows).map_err(|_| invalid())?;
    check(payload <= bytes)?;
    match &job.task {
        Task::Get { .. } => {
            validate_get_rows(result, job, rows).map_err(|_| invalid())?;
        }
        Task::Collect { query, .. } => {
            check(rows.iter().all(|r| matches_filter(query, r)))?;
            check(
                rows.windows(2)
                    .all(|pair| compare_rows(&job.table, query, &pair[0], &pair[1]).is_le()),
            )?;
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

/// Decode a borrowed immutable spool for a matching validated Get/Collect.
/// Returns full local rows, without projection, limit or public response caps.
pub fn decode_rows(
    payload: &SpoolPayload,
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<Vec<Row>, JobError> {
    let (count, bytes) = bounds(result, job)?;
    check(payload.len() <= MAX_INTERNAL_ROW_BYTES as u64)?;
    guard(
        BufReader::new(payload.open().map_err(|_| invalid())?),
        &job.table,
        count,
        bytes,
    )?;
    let mut decoder = serde_json::Deserializer::from_reader(BufReader::new(
        payload.open().map_err(|_| invalid())?,
    ));
    let rows = RowsSeed {
        table: &job.table,
        count,
    }
    .deserialize(&mut decoder)
    .map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    validate(&rows, result, job)?;
    Ok(rows)
}

/// Validate first, then serialize directly into a capped spool. The operation's
/// scratch quotas may reject capacity; they do not change the wire-size bound.
pub fn encode_rows(
    operation: &SpoolOperation,
    rows: &[Row],
    result: &ResultEnvelope,
    job: &JobEnvelope,
) -> Result<SpoolPayload, JobError> {
    validate(rows, result, job)?;
    let mut sink = BufWriter::with_capacity(
        64 * 1024,
        Sink(
            operation
                .writer(MAX_INTERNAL_ROW_BYTES as u64)
                .map_err(|_| invalid())?,
        ),
    );
    serde_json::to_writer(&mut sink, rows).map_err(|_| invalid())?;
    sink.into_inner()
        .map_err(|_| invalid())?
        .0
        .finish()
        .map_err(|_| invalid())
}
struct Sink(SpoolWriter);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for chunk in bytes.chunks(64 * 1024) {
            self.0
                .append(chunk)
                .map_err(|_| io::Error::other("row payload write failed"))?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct RowsSeed<'a> {
    table: &'a TableDescriptor,
    count: usize,
}
impl<'de> DeserializeSeed<'de> for RowsSeed<'_> {
    type Value = Vec<Row>;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for RowsSeed<'_> {
    type Value = Vec<Row>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("row array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut rows = Vec::new();
        // RowSeed checks admission before constructing the next map.
        while let Some(row) = seq.next_element_seed(RowSeed {
            table: self.table,
            admitted: rows.len() < self.count,
        })? {
            rows.push(row);
        }
        if rows.len() != self.count {
            return Err(de::Error::custom("row count"));
        }
        Ok(rows)
    }
}
struct RowSeed<'a> {
    table: &'a TableDescriptor,
    admitted: bool,
}
impl<'de> DeserializeSeed<'de> for RowSeed<'_> {
    type Value = Row;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Row, D::Error> {
        if !self.admitted {
            return Err(de::Error::custom("surplus row"));
        }
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for RowSeed<'_> {
    type Value = Row;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("complete row object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Row, A::Error> {
        let mut row = Row::new();
        while let Some(key) = map.next_key::<String>()? {
            let ty = self
                .table
                .schema
                .get(&key)
                .ok_or_else(|| de::Error::custom("unknown field"))?;
            if row.contains_key(&key) {
                return Err(de::Error::custom("duplicate field"));
            }
            let value = map.next_value_seed(ScalarSeed(*ty))?;
            row.insert(key, value);
        }
        protocol::validate_row(&row, self.table, RowMode::Complete)
            .map_err(|_| de::Error::custom("invalid row"))?;
        Ok(row)
    }
}
struct ScalarSeed(ColumnType);
impl<'de> DeserializeSeed<'de> for ScalarSeed {
    type Value = Scalar;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Scalar, D::Error> {
        if self.0 == ColumnType::Utf8 {
            return String::deserialize(d).map(Scalar::Utf8);
        }
        // Guard has already excluded strings/containers and bounded every number.
        let value = Scalar::deserialize(d)?;
        if matches!(
            (&value, self.0),
            (Scalar::Int64(_), ColumnType::Int64)
                | (Scalar::Float64(_), ColumnType::Float64)
                | (Scalar::Bool(_), ColumnType::Bool)
        ) {
            Ok(value)
        } else {
            Err(de::Error::custom("scalar type"))
        }
    }
}

// Fixed shallow grammar resource scan, not a general JSON parser. Serde remains
// syntax/Unicode authority. No value strings, raw scalar copies or tree survive.
struct Guard<R> {
    reader: R,
    used: usize,
    budget: usize,
}
impl<R: BufRead> Guard<R> {
    fn peek(&mut self) -> Result<Option<u8>, JobError> {
        Ok(self
            .reader
            .fill_buf()
            .map_err(|_| invalid())?
            .first()
            .copied())
    }
    fn byte(&mut self) -> Result<u8, JobError> {
        let b = self.peek()?.ok_or_else(invalid)?;
        self.reader.consume(1);
        Ok(b)
    }
    fn ws(&mut self) -> Result<(), JobError> {
        while self
            .peek()?
            .is_some_and(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.byte()?;
        }
        Ok(())
    }
    fn expect(&mut self, b: u8) -> Result<(), JobError> {
        self.ws()?;
        check(self.byte()? == b)
    }
    fn charge(&mut self, n: usize) -> Result<(), JobError> {
        self.used = self.used.checked_add(n).ok_or_else(invalid)?;
        check(self.used <= self.budget)
    }
    fn hex(&mut self) -> Result<u16, JobError> {
        let mut n = 0;
        for _ in 0..4 {
            let b = self.byte()?;
            let v = match b {
                b'0'..=b'9' => b - b'0',
                b'a'..=b'f' => b - b'a' + 10,
                b'A'..=b'F' => b - b'A' + 10,
                _ => return Err(invalid()),
            };
            n = n * 16 + u16::from(v);
        }
        Ok(n)
    }
    // Emits decoded bytes as they arrive. Key matching uses bounded schema
    // candidate flags rather than retaining any spelling of the key token.
    fn string(&mut self, mut emit: impl FnMut(u8) -> Result<(), JobError>) -> Result<(), JobError> {
        check(self.byte()? == b'"')?;
        loop {
            match self.byte()? {
                b'"' => return Ok(()),
                b'\\' => match self.byte()? {
                    b'u' => {
                        let first = self.hex()?;
                        let cp = if (0xd800..=0xdbff).contains(&first) {
                            check(self.byte()? == b'\\' && self.byte()? == b'u')?;
                            let low = self.hex()?;
                            check((0xdc00..=0xdfff).contains(&low))?;
                            0x10000
                                + ((u32::from(first) - 0xd800) << 10)
                                + (u32::from(low) - 0xdc00)
                        } else {
                            u32::from(first)
                        };
                        let c = char::from_u32(cp).ok_or_else(invalid)?;
                        let mut buf = [0; 4];
                        for b in c.encode_utf8(&mut buf).bytes() {
                            emit(b)?;
                        }
                    }
                    b'"' => emit(b'"')?,
                    b'\\' => emit(b'\\')?,
                    b'/' => emit(b'/')?,
                    b'b' => emit(8)?,
                    b'f' => emit(12)?,
                    b'n' => emit(10)?,
                    b'r' => emit(13)?,
                    b't' => emit(9)?,
                    _ => return Err(invalid()),
                },
                0..=31 => return Err(invalid()),
                b => emit(b)?,
            }
        }
    }
}
fn guard<R: BufRead>(
    reader: R,
    table: &TableDescriptor,
    count: usize,
    budget: usize,
) -> Result<(), JobError> {
    let mut g = Guard {
        reader,
        used: 0,
        budget,
    };
    g.expect(b'[')?;
    g.ws()?;
    let overhead = 8 + table.schema.keys().map(|k| 8 + k.len()).sum::<usize>();
    let mut rows = 0;
    if g.peek()? != Some(b']') {
        loop {
            check(rows < count)?;
            rows += 1;
            g.charge(overhead)?;
            g.expect(b'{')?;
            g.ws()?;
            let mut fields = 0;
            if g.peek()? != Some(b'}') {
                loop {
                    check(fields < table.schema.len())?;
                    fields += 1;
                    g.ws()?;
                    let mut candidates = vec![true; table.schema.len()];
                    let mut pos = 0;
                    g.string(|b| {
                        check(pos < 64)?;
                        for ((name, _), candidate) in table.schema.iter().zip(&mut candidates) {
                            *candidate &= name.as_bytes().get(pos) == Some(&b);
                        }
                        pos += 1;
                        Ok(())
                    })?;
                    let ty = table
                        .schema
                        .iter()
                        .zip(candidates)
                        .find(|((name, _), yes)| *yes && name.len() == pos)
                        .map(|((_, ty), _)| *ty)
                        .ok_or_else(invalid)?;
                    g.expect(b':')?;
                    g.ws()?;
                    match g.peek()?.ok_or_else(invalid)? {
                        b'"' => {
                            check(ty == ColumnType::Utf8)?;
                            let mut used = g.used;
                            let cap = g.budget;
                            g.string(|_| {
                                used = used.checked_add(1).ok_or_else(invalid)?;
                                check(used <= cap)
                            })?;
                            g.used = used;
                        }
                        b't' | b'f' => {
                            g.charge(1)?;
                            let word: &[u8] = if g.peek()? == Some(b't') {
                                b"true"
                            } else {
                                b"false"
                            };
                            for b in word {
                                check(g.byte()? == *b)?;
                            }
                        }
                        b'-' | b'0'..=b'9' => {
                            g.charge(8)?;
                            let mut n = 0;
                            while g.peek()?.is_some_and(|b| {
                                matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                            }) {
                                check(n < NUMERIC_BYTES)?;
                                g.byte()?;
                                n += 1;
                            }
                        }
                        _ => return Err(invalid()),
                    }
                    g.ws()?;
                    if g.peek()? != Some(b',') {
                        break;
                    }
                    g.byte()?;
                }
            }
            g.expect(b'}')?;
            g.ws()?;
            if g.peek()? != Some(b',') {
                break;
            }
            g.byte()?;
        }
    }
    g.expect(b']')?;
    g.ws()?;
    check(g.peek()?.is_none() && rows == count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn resource_scan_stops_before_large_values_and_counts_decoded_escapes() {
        let table: TableDescriptor = serde_json::from_str(r#"{"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"}"#).unwrap();
        let overhead = 8 + 8 + 2 + 8 + 4;
        let text = br#"[{"id":1,"text":"\u0000\ud83d\ude00"}]"#;
        assert!(guard(Cursor::new(text), &table, 1, overhead + 8 + 5).is_ok());
        assert!(guard(Cursor::new(text), &table, 1, overhead + 8 + 4).is_err());
        let mut input =
            Cursor::new(br#"[{"id":1,"text":"\u0000xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}]"#);
        assert!(guard(&mut input, &table, 1, overhead + 8).is_err());
        assert_eq!(input.position(), 23); // Stops after the first decoded NUL.
        let text = format!(r#"[{{"id":{},"text":"unread"}}]"#, "1".repeat(1024));
        let mut input = Cursor::new(text.as_bytes());
        assert!(guard(&mut input, &table, 1, 2048).is_err());
        assert_eq!(input.position(), 7 + NUMERIC_BYTES as u64);
        let mut input = Cursor::new(text.as_bytes());
        assert!(guard(&mut input, &table, 0, 2048).is_err());
        assert_eq!(input.position(), 1); // No row allocation or value scan.
        let text = format!(r#"[{{"{}":0}}]"#, "x".repeat(65));
        let mut input = Cursor::new(text.as_bytes());
        assert!(guard(&mut input, &table, 1, 2048).is_err());
        assert!(input.position() < 70);
    }
}
