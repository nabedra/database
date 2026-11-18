//! Snapshot iterators and bounded top-k. No table-wide row or byte limit.
use super::{codec, failure};
use crate::protocol::{
    ComparisonOp, ErrorCode, LogicalPredicate, MAX_BODY_BYTES, MAX_RESULT_ROWS, Predicate, Query,
    Row, Scalar, TableDescriptor,
};
use crate::query_semantics as semantics;
use rocksdb::{Direction, IteratorMode, ReadOptions, Snapshot};
use serde::Serialize;
use serde_json::Value;
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io::{self, Write},
};

/// Conservative charge for decoded BTreeMap nodes, strings, JSON and row data.
/// This bounds task-owned row state, not RocksDB's native allocations.
pub(crate) fn row_charge(
    table: &TableDescriptor,
    encoded: usize,
) -> Result<usize, crate::protocol::ProtocolError> {
    encoded
        .checked_mul(8)
        .and_then(|n| n.checked_add(table.schema.len().checked_mul(256)?))
        .ok_or_else(|| failure(ErrorCode::LimitExceeded, "row working memory overflow"))
}

pub(crate) fn decode_row(
    table: &TableDescriptor,
    bytes: &[u8],
    budget: usize,
) -> Result<Row, crate::protocol::ProtocolError> {
    if row_charge(table, bytes.len())? > budget {
        return Err(failure(
            ErrorCode::LimitExceeded,
            "row exceeds task working memory",
        ));
    }
    let row: Row = serde_json::from_slice(bytes)
        .map_err(|_| failure(ErrorCode::StorageUnavailable, "invalid stored row"))?;
    crate::protocol::validate_row(&row, table, crate::protocol::RowMode::Complete).map_err(
        |_| {
            failure(
                ErrorCode::StorageUnavailable,
                "stored row does not match schema",
            )
        },
    )?;
    Ok(row)
}

pub(crate) fn check_body(
    value: &impl Serialize,
    limit: usize,
) -> Result<usize, crate::protocol::ProtocolError> {
    let mut writer = BudgetWriter { written: 0, limit };
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        failure(
            if error.is_io() {
                ErrorCode::LimitExceeded
            } else {
                ErrorCode::InternalError
            },
            "serialized value exceeds its budget or cannot be encoded",
        )
    })?;
    Ok(writer.written)
}

struct BudgetWriter {
    written: usize,
    limit: usize,
}
impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.written {
            return Err(io::Error::other("serialization budget"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct Plan {
    pub lower: Vec<u8>,
    pub upper: Vec<u8>,
    pub point: Option<Vec<u8>>,
    pub indexed: bool,
}
impl Plan {
    pub fn for_query(namespace: codec::Namespace, table: &TableDescriptor, query: &Query) -> Self {
        let prefix = namespace.rows(table.id);
        let mut plan = Self {
            lower: prefix.clone(),
            upper: codec::end(&prefix),
            point: None,
            indexed: false,
        };
        if let Some(predicate) = &query.filter {
            plan.constrain(namespace, table, predicate);
        }
        plan
    }
    fn constrain(
        &mut self,
        namespace: codec::Namespace,
        table: &TableDescriptor,
        predicate: &Predicate,
    ) {
        match predicate {
            Predicate::Logical(LogicalPredicate::And { args }) => {
                for arg in args {
                    self.constrain(namespace, table, arg);
                }
            }
            Predicate::Comparison(p) if p.column == table.key => {
                let key = namespace.row(table.id, &codec::scalar_key(&p.value));
                match p.op {
                    ComparisonOp::Eq => {
                        self.lower = self.lower.clone().max(key.clone());
                        self.upper = self.upper.clone().min(codec::end(&key));
                        self.point = Some(key);
                    }
                    ComparisonOp::Ge => self.lower = self.lower.clone().max(key),
                    ComparisonOp::Gt => self.lower = self.lower.clone().max(codec::end(&key)),
                    ComparisonOp::Lt => self.upper = self.upper.clone().min(key),
                    ComparisonOp::Le => self.upper = self.upper.clone().min(codec::end(&key)),
                    ComparisonOp::Ne => return,
                }
                self.indexed = true;
            }
            _ => {}
        }
    }
}

/// Callback observes one schema-valid row at a time and can stop the iterator.
fn visit(
    snapshot: &Snapshot<'_>,
    table: &TableDescriptor,
    plan: &Plan,
    budget: usize,
    mut visitor: impl FnMut(Row, usize) -> Result<bool, crate::protocol::ProtocolError>,
) -> Result<(), crate::protocol::ProtocolError> {
    if plan.lower >= plan.upper {
        return Ok(());
    }
    if let Some(key) = &plan.point {
        if key >= &plan.lower
            && key < &plan.upper
            && let Some(bytes) = snapshot.get(key).map_err(super::storage_error)?
        {
            let charge = row_charge(table, bytes.len())?;
            visitor(decode_row(table, &bytes, budget)?, charge)?;
        }
        return Ok(());
    }
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    options.set_iterate_lower_bound(plan.lower.clone());
    options.set_iterate_upper_bound(plan.upper.clone());
    for entry in snapshot.iterator_opt(IteratorMode::From(&plan.lower, Direction::Forward), options)
    {
        let (_, bytes) = entry.map_err(super::storage_error)?;
        let charge = row_charge(table, bytes.len())?;
        if !visitor(decode_row(table, &bytes, budget)?, charge)? {
            break;
        }
    }
    Ok(())
}

pub(crate) fn count(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
    table: &TableDescriptor,
    query: &Query,
    budget: usize,
) -> Result<Value, crate::protocol::ProtocolError> {
    let limit = query.limit.unwrap_or(u64::MAX);
    let mut count = 0_u64;
    if limit > 0 {
        visit(
            snapshot,
            table,
            &Plan::for_query(namespace, table, query),
            budget,
            |row, _| {
                if semantics::matches_filter(query, &row) {
                    count = count.checked_add(1).ok_or_else(|| {
                        failure(ErrorCode::LimitExceeded, "count exceeds u64 representation")
                    })?;
                }
                Ok(count < limit)
            },
        )?;
    }
    Ok(Value::from(count))
}

pub(crate) fn collect(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
    table: &TableDescriptor,
    query: &Query,
    budget: usize,
) -> Result<Value, crate::protocol::ProtocolError> {
    let cap = query
        .limit
        .unwrap_or(u64::MAX)
        .min((MAX_RESULT_ROWS + 1) as u64) as usize;
    if cap == 0 {
        return Ok(Value::Array(Vec::new()));
    }
    let primary_order = query
        .order_by
        .as_ref()
        .is_none_or(|order| order.column == table.key && !order.desc);
    let mut heap = BinaryHeap::<OrderedRow>::new();
    let mut retained = 0_usize;
    let mut matched = 0_usize;
    visit(
        snapshot,
        table,
        &Plan::for_query(namespace, table, query),
        budget / 2,
        |row, charge| {
            if !semantics::matches_filter(query, &row) {
                return Ok(true);
            }
            matched = matched.saturating_add(1);
            if cap > MAX_RESULT_ROWS && matched > MAX_RESULT_ROWS {
                return Err(failure(
                    ErrorCode::LimitExceeded,
                    "result exceeds 10000 rows",
                ));
            }
            let order = query.order_by.as_ref();
            let item = OrderedRow {
                sort: row[order.map_or(table.key.as_str(), |order| order.column.as_str())].clone(),
                key: row[&table.key].clone(),
                desc: order.is_some_and(|order| order.desc),
                row,
                charge,
            };
            if heap.len() < cap {
                // The next decoded row coexists with retained rows. Reserve space
                // for that row and for converting the selected rows to JSON Values.
                retained = retained.checked_add(charge).ok_or_else(|| {
                    failure(ErrorCode::LimitExceeded, "query working memory overflow")
                })?;
                if retained > budget / 2 {
                    return Err(failure(
                        ErrorCode::LimitExceeded,
                        "query exceeds task working memory",
                    ));
                }
                heap.push(item);
            } else if heap.peek().is_some_and(|worst| item < *worst) {
                let removed = heap.pop().expect("full nonempty heap");
                retained -= removed.charge;
                retained = retained.checked_add(charge).ok_or_else(|| {
                    failure(ErrorCode::LimitExceeded, "query working memory overflow")
                })?;
                if retained > budget / 2 {
                    return Err(failure(
                        ErrorCode::LimitExceeded,
                        "query exceeds task working memory",
                    ));
                }
                heap.push(item);
            }
            Ok(!primary_order || heap.len() < cap)
        },
    )?;
    let mut rows: Vec<Row> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|entry| entry.row)
        .collect();
    // Keep this call as a semantic cross-check on the heap ordering and ties.
    debug_assert!(rows.windows(2).all(|pair| semantics::compare_rows(
        table, query, &pair[0], &pair[1]
    ) != Ordering::Greater));
    if let Some(columns) = &query.select {
        for row in &mut rows {
            row.retain(|column, _| columns.contains(column));
        }
    }
    check_body(&rows, MAX_BODY_BYTES.saturating_sub(128))?;
    serde_json::to_value(rows)
        .map_err(|_| failure(ErrorCode::InternalError, "row result encoding failed"))
}

struct OrderedRow {
    sort: Scalar,
    key: Scalar,
    desc: bool,
    row: Row,
    charge: usize,
}
impl PartialEq for OrderedRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OrderedRow {}
impl PartialOrd for OrderedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = semantics::compare(&self.sort, &other.sort);
        (if self.desc { cmp.reverse() } else { cmp })
            .then_with(|| semantics::compare(&self.key, &other.key))
    }
}

pub(crate) fn explain(table: &TableDescriptor, query: &Query) -> Value {
    let plan = Plan::for_query(codec::Namespace(0), table, query);
    Value::String(format!(
        "engine oltp; experimental standalone RF1\nprimary index {}; {}\nshards 1; merge {}\nconsistency local RocksDB snapshot; synchronous WAL writes\nsecondary indexes unavailable",
        table.key,
        if plan.point.is_some() {
            "point get"
        } else if plan.indexed {
            "bounded range seek"
        } else {
            "streaming scan"
        },
        if query
            .order_by
            .as_ref()
            .is_none_or(|order| order.column == table.key && !order.desc)
        {
            "primary-key ascending"
        } else {
            "bounded top-k; primary-key ascending ties"
        }
    ))
}
