//! Pure synchronous evaluation shared by worker execution and query tests.
//!
//! Each entry point validates the descriptor and query. The snapshot loader must
//! supply complete, schema-valid rows with finite floats and unique primary keys.
//! Table row/payload limits belong to that loader; evaluation does not revalidate
//! the table. `collect` bounds the serialized row array and `explain` bounds its
//! serialized string. HTTP must also bound its
//! final envelope, whose request ID and wrapper consume additional bytes.
use crate::protocol::{
    ErrorCode, MAX_BODY_BYTES, MAX_RESULT_ROWS, Outcome, ProtocolError, Query, Row, TableDescriptor,
};
pub(crate) use crate::query_semantics::{compare_rows, matches_filter};
use std::io::{self, Write};

/// Filter, order, limit, then project. Oversized results fail without truncation.
#[allow(dead_code)] // Retained pure evaluator API, exercised by query integration tests.
pub fn collect(
    table: &TableDescriptor,
    query: &Query,
    rows: &[Row],
) -> Result<Vec<Row>, ProtocolError> {
    validate(table, query)?;
    crate::snapshot::check_rows_memory(table, rows, 0)?;
    let matched: Vec<&Row> = rows
        .iter()
        .filter(|row| matches_filter(query, row))
        .collect();
    finish_collect(table, query, matched)
}

/// Filter and locally order full rows; retain key/sort columns until reduction.
pub(super) fn local_collect(table: &TableDescriptor, query: &Query, rows: Vec<Row>) -> Vec<Row> {
    let mut matched: Vec<Row> = rows
        .into_iter()
        .filter(|row| matches_filter(query, row))
        .collect();
    matched.sort_unstable_by(|a, b| compare_rows(table, query, a, b));
    matched
}

pub(super) fn local_count(query: &Query, rows: &[Row]) -> u64 {
    rows.iter().filter(|row| matches_filter(query, row)).count() as u64
}

/// Apply global order, one limit, projection, and result budgets.
pub(crate) fn reduce_collect(
    table: &TableDescriptor,
    query: &Query,
    rows: &[Row],
) -> Result<Vec<Row>, ProtocolError> {
    crate::snapshot::check_rows_memory(table, rows, 0)?;
    finish_collect(table, query, rows.iter().collect())
}

fn finish_collect(
    table: &TableDescriptor,
    query: &Query,
    mut matched: Vec<&Row>,
) -> Result<Vec<Row>, ProtocolError> {
    let result_len = limited_len(matched.len(), query.limit);
    if result_len > MAX_RESULT_ROWS {
        return Err(error(ErrorCode::LimitExceeded, "result exceeds 10000 rows"));
    }
    if result_len == 0 {
        return Ok(Vec::new());
    }
    matched.sort_unstable_by(|a, b| compare_rows(table, query, a, b));
    matched.truncate(result_len);
    let result: Vec<Row> = matched
        .into_iter()
        .map(|row| match &query.select {
            Some(columns) => columns
                .iter()
                .map(|c| (c.clone(), row[c].clone()))
                .collect(),
            None => row.clone(),
        })
        .collect();
    // Count escaped JSON bytes through a bounded writer, without allocating a
    // second result-sized byte buffer. Serialization stops at the first excess.
    check_body_budget(&result)?;
    Ok(result)
}

/// Count matches including the limit, without sorting or projecting rows.
/// The collect row/array limits do not apply to this scalar result.
#[allow(dead_code)] // Retained pure evaluator API, exercised by query integration tests.
pub fn count(table: &TableDescriptor, query: &Query, rows: &[Row]) -> Result<u64, ProtocolError> {
    validate(table, query)?;
    crate::snapshot::check_rows_memory(table, rows, 0)?;
    let cap = query.limit.unwrap_or(u64::MAX);
    let mut count = 0;
    for row in rows {
        if count == cap {
            break;
        }
        if matches_filter(query, row) {
            count += 1;
        }
    }
    Ok(count)
}

/// Describe execution using metadata only. This API cannot scan row data.
pub fn explain(table: &TableDescriptor, query: &Query) -> Result<String, ProtocolError> {
    validate(table, query)?;
    let filter = match &query.filter {
        Some(predicate) => serde_json::to_string(predicate)
            .map_err(|_| error(ErrorCode::InternalError, "filter serialization failed"))?,
        None => "none".into(),
    };
    let sort = match &query.order_by {
        Some(order) => format!(
            "{} {}; tie-breaker {} ASC",
            order.column,
            if order.desc { "DESC" } else { "ASC" },
            table.key
        ),
        None => format!("{} ASC (primary key)", table.key),
    };
    let explanation = format!(
        "scan {} ({})\nfilter {}\nsort {}\nlimit {}\nprojection {}",
        table.name,
        table.id,
        filter,
        sort,
        query.limit.map_or_else(|| "none".into(), |n| n.to_string()),
        query
            .select
            .as_ref()
            .map_or_else(|| "all".into(), |cols| cols.join(", "))
    );
    check_body_budget(&explanation)?;
    Ok(explanation)
}

fn validate(table: &TableDescriptor, query: &Query) -> Result<(), ProtocolError> {
    table.validate()?;
    query.validate(&table.schema)
}

fn limited_len(len: usize, limit: Option<u64>) -> usize {
    limit
        .and_then(|n| usize::try_from(n).ok())
        .map_or(len, |n| len.min(n))
}

fn error(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

pub(super) fn check_body_budget(value: &impl serde::Serialize) -> Result<(), ProtocolError> {
    let mut budget = BodyBudget {
        remaining: MAX_BODY_BYTES,
    };
    serde_json::to_writer(&mut budget, value).map_err(|e| {
        if e.is_io() {
            error(ErrorCode::LimitExceeded, "serialized result exceeds 8 MiB")
        } else {
            error(ErrorCode::InternalError, "result serialization failed")
        }
    })
}

struct BodyBudget {
    remaining: usize,
}
impl Write for BodyBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("result body budget exceeded"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
