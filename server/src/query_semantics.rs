//! Pure comparison and predicate semantics for schema-validated rows.
//! Both engines use the same ordering and float signed-zero behavior.
use crate::protocol::{
    ComparisonOp, LogicalPredicate, Predicate, Query, Row, Scalar, TableDescriptor,
};
use std::cmp::Ordering;

pub(crate) fn compare_rows(table: &TableDescriptor, query: &Query, a: &Row, b: &Row) -> Ordering {
    let key_order = || compare(&a[&table.key], &b[&table.key]);
    match &query.order_by {
        Some(order) => {
            let cmp = compare(&a[&order.column], &b[&order.column]);
            (if order.desc { cmp.reverse() } else { cmp }).then_with(key_order)
        }
        None => key_order(),
    }
}

pub(crate) fn matches_filter(query: &Query, row: &Row) -> bool {
    query
        .filter
        .as_ref()
        .is_none_or(|p| matches_predicate(p, row))
}

fn matches_predicate(predicate: &Predicate, row: &Row) -> bool {
    match predicate {
        Predicate::Comparison(p) => {
            let cmp = compare(&row[&p.column], &p.value);
            match p.op {
                ComparisonOp::Eq => cmp == Ordering::Equal,
                ComparisonOp::Ne => cmp != Ordering::Equal,
                ComparisonOp::Gt => cmp == Ordering::Greater,
                ComparisonOp::Ge => cmp != Ordering::Less,
                ComparisonOp::Lt => cmp == Ordering::Less,
                ComparisonOp::Le => cmp != Ordering::Greater,
            }
        }
        Predicate::Logical(LogicalPredicate::And { args }) => {
            args.iter().all(|p| matches_predicate(p, row))
        }
        Predicate::Logical(LogicalPredicate::Or { args }) => {
            args.iter().any(|p| matches_predicate(p, row))
        }
        Predicate::Logical(LogicalPredicate::Not { arg }) => !matches_predicate(arg, row),
    }
}

pub(crate) fn compare(a: &Scalar, b: &Scalar) -> Ordering {
    match (a, b) {
        (Scalar::Utf8(a), Scalar::Utf8(b)) => a.cmp(b),
        (Scalar::Int64(a), Scalar::Int64(b)) => a.cmp(b),
        (Scalar::Bool(a), Scalar::Bool(b)) => a.cmp(b),
        // Numeric comparison makes both zero signs equal. Snapshot validation
        // guarantees finite floats, so unordered comparison is impossible.
        (Scalar::Float64(a), Scalar::Float64(b)) => {
            a.partial_cmp(b).expect("schema-valid finite floats")
        }
        _ => unreachable!("validated query and schema-valid rows have matching types"),
    }
}
