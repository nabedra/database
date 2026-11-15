//! Pure synchronous row rules. The snapshot loader must supply complete rows
//! with the exact schema, finite floats, unique keys, and valid table limits.
//! This function consumes that private candidate, with no external side effects.
//!
//! Before publishing changed rows, the engine must call the R03 codec helper to
//! validate the 64 MiB decoded payload limit and encode Parquet, then use the R04
//! publication path. `changed` alone never establishes durable atomicity.
use crate::protocol::{
    ErrorCode, Key, MAX_TABLE_ROWS, Operation, Outcome, ProtocolError, Row, Scalar, TableDescriptor,
};
use std::collections::BTreeSet;

#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(untagged)]
/// Serializes directly as wire data: insert/upsert null, batch count, or
/// update/delete found status. Found status is independent of `changed`.
pub enum MutationResult {
    Null,
    Count(usize),
    Found(bool),
}

#[derive(Debug)]
pub struct Mutation {
    pub rows: Vec<Row>,
    pub result: MutationResult,
    pub changed: bool,
}

/// Validate public operation inputs before applying any changes to owned rows.
/// Errors are `not_applied` and return no candidate or successful batch prefix.
/// Existing row order is preserved; new rows append in request order.
pub fn apply(
    table: &TableDescriptor,
    operation: &Operation,
    mut rows: Vec<Row>,
) -> Result<Mutation, ProtocolError> {
    table.validate()?;
    operation.validate_for_table(table)?;
    crate::snapshot::check_rows_memory(table, &rows, 0)?;
    let added = match operation {
        Operation::InsertMany(p) => p.rows.as_slice(),
        Operation::Insert(p) => std::slice::from_ref(&p.row),
        Operation::Upsert(p) => std::slice::from_ref(&p.row),
        Operation::Update(p) => std::slice::from_ref(&p.values),
        _ => &[],
    };
    let extra = added.iter().try_fold(0usize, |sum, row| {
        sum.checked_add(crate::snapshot::row_payload(row))
            .ok_or_else(|| error(ErrorCode::LimitExceeded, "mutation memory limit"))
    })?;
    let existing = rows.iter().try_fold(0usize, |sum, row| {
        sum.checked_add(crate::snapshot::row_payload(row))
            .ok_or_else(|| error(ErrorCode::LimitExceeded, "mutation memory limit"))
    })?;
    crate::snapshot::check_memory_shape(
        table,
        rows.len()
            .checked_add(added.len())
            .ok_or_else(|| error(ErrorCode::LimitExceeded, "mutation memory limit"))?,
        existing
            .checked_add(extra)
            .ok_or_else(|| error(ErrorCode::LimitExceeded, "mutation memory limit"))?,
        0,
    )?;
    rows.try_reserve_exact(added.len())
        .map_err(|_| error(ErrorCode::LimitExceeded, "mutation allocation limit"))?;
    let (result, changed) = match operation {
        Operation::Insert(p) => {
            if rows.iter().any(|row| row[&table.key] == p.row[&table.key]) {
                return Err(error(ErrorCode::Conflict, "primary key already exists"));
            }
            check_growth(rows.len(), 1)?;
            rows.push(p.row.clone());
            (MutationResult::Null, true)
        }
        Operation::InsertMany(p) => {
            // Transient uniqueness check only; no persistent index. All checks
            // finish before extending the candidate, even for a late conflict.
            let mut keys: BTreeSet<_> = rows.iter().map(|row| row_key(table, row)).collect();
            for row in &p.rows {
                if !keys.insert(row_key(table, row)) {
                    return Err(error(ErrorCode::Conflict, "duplicate primary key"));
                }
            }
            check_growth(rows.len(), p.rows.len())?;
            rows.extend(p.rows.iter().cloned());
            (MutationResult::Count(p.rows.len()), !p.rows.is_empty())
        }
        Operation::Upsert(p) => {
            let changed = match rows
                .iter_mut()
                .find(|row| row[&table.key] == p.row[&table.key])
            {
                Some(row) => {
                    let changed = !same_row(row, &p.row);
                    *row = p.row.clone();
                    changed
                }
                None => {
                    check_growth(rows.len(), 1)?;
                    rows.push(p.row.clone());
                    true
                }
            };
            (MutationResult::Null, changed)
        }
        Operation::Update(p) => match find(table, &rows, &p.key) {
            Some(index) => {
                let row = &mut rows[index];
                let changed = p
                    .values
                    .iter()
                    .any(|(column, value)| !same_scalar(&row[column], value));
                row.extend(p.values.clone());
                (MutationResult::Found(true), changed)
            }
            None => (MutationResult::Found(false), false),
        },
        Operation::Delete(p) => match find(table, &rows, &p.key) {
            Some(index) => {
                rows.remove(index);
                (MutationResult::Found(true), true)
            }
            None => (MutationResult::Found(false), false),
        },
        _ => {
            return Err(error(
                ErrorCode::UnsupportedOperation,
                "expected a row mutation",
            ));
        }
    };
    check_growth(rows.len(), 0)?;
    Ok(Mutation {
        rows,
        result,
        changed,
    })
}

fn find(table: &TableDescriptor, rows: &[Row], key: &Key) -> Option<usize> {
    rows.iter().position(|row| match (&row[&table.key], key) {
        (Scalar::Utf8(a), Key::Utf8(b)) => a == b,
        (Scalar::Int64(a), Key::Int64(b)) => a == b,
        _ => false,
    })
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum RowKey<'a> {
    Utf8(&'a str),
    Int64(i64),
}
fn row_key<'a>(table: &TableDescriptor, row: &'a Row) -> RowKey<'a> {
    match &row[&table.key] {
        Scalar::Utf8(value) => RowKey::Utf8(value),
        Scalar::Int64(value) => RowKey::Int64(*value),
        _ => unreachable!("validated input and snapshot keys are utf8 or int64"),
    }
}

fn same_row(a: &Row, b: &Row) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(column, value)| b.get(column).is_some_and(|v| same_scalar(value, v)))
}
fn same_scalar(a: &Scalar, b: &Scalar) -> bool {
    match (a, b) {
        (Scalar::Float64(a), Scalar::Float64(b)) => a.to_bits() == b.to_bits(),
        _ => a == b,
    }
}

fn check_growth(current: usize, additional: usize) -> Result<(), ProtocolError> {
    if current
        .checked_add(additional)
        .is_none_or(|len| len > MAX_TABLE_ROWS)
    {
        Err(error(ErrorCode::LimitExceeded, "table exceeds 100000 rows"))
    } else {
        Ok(())
    }
}
fn error(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    use crate::protocol::{ColumnType, TableFormat, UpdatePayload};
    #[test]
    fn m01_update_values_are_checked_before_cloning_into_candidate() {
        let table = TableDescriptor {
            id: uuid::Uuid::nil(),
            name: "memory".into(),
            key: "id".into(),
            format: TableFormat::Parquet,
            schema: [
                ("id".into(), ColumnType::Int64),
                ("text".into(), ColumnType::Utf8),
            ]
            .into(),
        };
        let operation = Operation::Update(UpdatePayload {
            table_id: table.id,
            key: Key::Int64(1),
            values: [("text".into(), Scalar::Utf8("x".repeat(16 * 1024)))].into(),
        });
        let rows = vec![
            ([
                ("id".into(), Scalar::Int64(1)),
                ("text".into(), Scalar::Utf8("small".into())),
            ])
            .into(),
        ];
        crate::scheduling::with_task_limit(8192, || {
            assert_eq!(
                apply(&table, &operation, rows).unwrap_err().code,
                ErrorCode::LimitExceeded
            );
        });
    }
}

#[cfg(test)]
mod tests;
