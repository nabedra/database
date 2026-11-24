//! Bounded, worker-local observations of successful collect and count executions.
//! Retain a fingerprint of the query structure and at most one schema column
//! recommendation per entry. Scalar literals and row data never enter the hash.
use crate::{
    oltp::pool::WorkerSharedResources,
    protocol::{ComparisonOp, LogicalPredicate, Predicate, ProtocolError, Query, TableDescriptor},
    scheduling::MemoryCharge,
};
use nabedra_oltp_types::types::{Digest, Text};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use std::sync::Mutex;
use uuid::Uuid;

const CAPACITY: usize = 256;

#[derive(Clone, Copy)]
pub(super) enum AccessKind {
    Empty,
    Point,
    PrimaryRange,
    PrimaryScan,
    SecondaryIndex,
    CoveringIndex,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QueryKind {
    Collect,
    Count,
}

#[derive(Clone, Default, Serialize)]
struct PlanCounts {
    empty: u64,
    point: u64,
    primary_range: u64,
    primary_scan: u64,
    secondary_index: u64,
    covering_index: u64,
}

impl PlanCounts {
    fn record(&mut self, access: AccessKind) {
        let count = match access {
            AccessKind::Empty => &mut self.empty,
            AccessKind::Point => &mut self.point,
            AccessKind::PrimaryRange => &mut self.primary_range,
            AccessKind::PrimaryScan => &mut self.primary_scan,
            AccessKind::SecondaryIndex => &mut self.secondary_index,
            AccessKind::CoveringIndex => &mut self.covering_index,
        };
        *count = count.saturating_add(1);
    }
}

#[derive(Clone, Serialize)]
struct ShapeStats {
    table_id: Uuid,
    fingerprint: Digest,
    operation: QueryKind,
    executions: u64,
    shard_visits: u64,
    plans: PlanCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    consider_index_on: Option<Text<64>>,
}

#[derive(Clone, Default, Serialize)]
pub struct QueryStatistics {
    executions: u64,
    untracked_executions: u64,
    capacity: usize,
    shapes: Vec<ShapeStats>,
}

pub(super) struct QueryMetrics {
    stats: Mutex<QueryStatistics>,
    _charge: MemoryCharge,
}

impl QueryMetrics {
    pub(super) fn new(shared: &WorkerSharedResources) -> Result<Self, ProtocolError> {
        let retained_bytes = CAPACITY * (std::mem::size_of::<ShapeStats>() + 64) + 4096;
        let charge = shared
            .scheduler()
            .charge_memory(shared.scheduler_node_id(), retained_bytes as u64)?;
        Ok(Self {
            stats: Mutex::new(QueryStatistics {
                capacity: CAPACITY,
                shapes: Vec::with_capacity(CAPACITY),
                ..Default::default()
            }),
            _charge: charge,
        })
    }

    pub(super) fn record(
        &self,
        table: &TableDescriptor,
        query: &Query,
        count: bool,
        access: AccessKind,
        shards: u64,
    ) {
        let shape = QueryShape::new(query, &table.key, count);
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(table.id, shape, access, shards);
    }

    pub(super) fn snapshot(&self) -> QueryStatistics {
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl QueryStatistics {
    fn record(&mut self, table_id: Uuid, shape: QueryShape<'_>, access: AccessKind, shards: u64) {
        self.executions = self.executions.saturating_add(1);
        let entry = self.shapes.iter().position(|entry| {
            entry.table_id == table_id
                && entry.operation == shape.operation
                && entry.fingerprint == shape.fingerprint
        });
        let entry = match entry {
            Some(index) => &mut self.shapes[index],
            None if self.shapes.len() == self.capacity => {
                self.untracked_executions = self.untracked_executions.saturating_add(1);
                return;
            }
            None => {
                self.shapes.push(ShapeStats {
                    table_id,
                    operation: shape.operation,
                    fingerprint: shape.fingerprint,
                    executions: 0,
                    shard_visits: 0,
                    plans: PlanCounts::default(),
                    consider_index_on: None,
                });
                self.shapes.last_mut().expect("new query shape")
            }
        };
        entry.executions = entry.executions.saturating_add(1);
        entry.shard_visits = entry.shard_visits.saturating_add(shards);
        entry.plans.record(access);
        if matches!(access, AccessKind::PrimaryScan | AccessKind::PrimaryRange) {
            if entry.consider_index_on.is_none() {
                entry.consider_index_on = shape
                    .index_column
                    .and_then(|column| Text::try_from(column).ok());
            }
        } else if matches!(
            access,
            AccessKind::SecondaryIndex | AccessKind::CoveringIndex
        ) {
            entry.consider_index_on = None;
        }
    }
}

struct QueryShape<'a> {
    operation: QueryKind,
    fingerprint: Digest,
    index_column: Option<&'a str>,
}

impl<'a> QueryShape<'a> {
    fn new(query: &'a Query, primary: &str, count: bool) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"nabedra/query-shape/v1\0");
        digest.update([match query.limit {
            None => 0,
            Some(0) => 1,
            Some(_) => 2,
        }]);
        if !count {
            if let Some(select) = &query.select {
                digest.update(b"select\0");
                for column in select {
                    identifier(column, &mut digest);
                }
                digest.update([0]);
            }
            if let Some(order) = &query.order_by {
                digest.update(b"order\0");
                identifier(&order.column, &mut digest);
                digest.update([u8::from(order.desc)]);
            }
        }
        let mut candidate = None;
        if let Some(predicate) = &query.filter {
            digest.update(b"filter\0");
            hash_predicate(predicate, &mut digest, primary, true, &mut candidate);
        }
        Self {
            operation: if count {
                QueryKind::Count
            } else {
                QueryKind::Collect
            },
            fingerprint: Digest::from_bytes(digest.finalize().into()),
            index_column: candidate.map(|(_, column)| column),
        }
    }
}

fn identifier(column: &str, digest: &mut Sha256) {
    digest.update(column.as_bytes());
    digest.update([0]);
}

fn hash_predicate<'a>(
    predicate: &'a Predicate,
    digest: &mut Sha256,
    primary: &str,
    conjunctive: bool,
    candidate: &mut Option<(bool, &'a str)>,
) {
    match predicate {
        Predicate::Comparison(comparison) => {
            digest.update([b'p', comparison.op as u8]);
            identifier(&comparison.column, digest);
            if conjunctive && comparison.column != primary && comparison.op != ComparisonOp::Ne {
                let next = (
                    comparison.op != ComparisonOp::Eq,
                    comparison.column.as_str(),
                );
                if candidate.is_none_or(|current| next < current) {
                    *candidate = Some(next);
                }
            }
        }
        Predicate::Logical(LogicalPredicate::And { args }) => {
            digest.update(b"a");
            for predicate in args {
                hash_predicate(predicate, digest, primary, conjunctive, candidate);
            }
            digest.update([0]);
        }
        Predicate::Logical(LogicalPredicate::Or { args }) => {
            digest.update(b"o");
            for predicate in args {
                hash_predicate(predicate, digest, primary, false, candidate);
            }
            digest.update([0]);
        }
        Predicate::Logical(LogicalPredicate::Not { arg }) => {
            digest.update(b"n");
            hash_predicate(arg, digest, primary, false, candidate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query(column: &str, literal: &str, limit: u64) -> Query {
        serde_json::from_value(json!({
            "filter":{"column":column,"op":"eq","value":literal},
            "select":["id"],"order_by":null,"limit":limit,
        }))
        .unwrap()
    }

    #[test]
    fn o04_o24_m01_query_metrics_exclude_literals_and_bound_shape_retention() {
        let mut stats = QueryStatistics {
            capacity: CAPACITY,
            shapes: Vec::with_capacity(CAPACITY),
            ..Default::default()
        };
        let table = Uuid::from_u128(1);
        let a = query("label", "first-private-literal", 123456789);
        let b = query("label", "second-private-literal", 987654321);
        assert_eq!(
            QueryShape::new(&a, "id", false).fingerprint,
            QueryShape::new(&b, "id", false).fingerprint
        );
        stats.record(
            table,
            QueryShape::new(&a, "id", false),
            AccessKind::PrimaryScan,
            3,
        );
        stats.record(
            table,
            QueryShape::new(&b, "id", false),
            AccessKind::PrimaryScan,
            2,
        );
        stats.record(
            table,
            QueryShape::new(&b, "id", true),
            AccessKind::CoveringIndex,
            1,
        );
        let entry = &stats.shapes[0];
        assert_eq!(
            (
                entry.executions,
                entry.shard_visits,
                entry.plans.primary_scan
            ),
            (2, 5, 2)
        );
        assert_eq!(
            entry.consider_index_on.as_ref().map(Text::as_str),
            Some("label")
        );
        assert!(stats.shapes[1].consider_index_on.is_none());
        stats.record(
            table,
            QueryShape::new(&a, "id", false),
            AccessKind::SecondaryIndex,
            3,
        );
        assert!(stats.shapes[0].consider_index_on.is_none());

        for id in 2..=(CAPACITY + 10) as u128 {
            stats.record(
                Uuid::from_u128(id),
                QueryShape::new(&b, "id", false),
                AccessKind::PrimaryScan,
                1,
            );
        }
        assert_eq!(stats.shapes.len(), CAPACITY);
        assert_eq!(stats.untracked_executions, 11);
        stats.record(
            table,
            QueryShape::new(&b, "id", false),
            AccessKind::PrimaryScan,
            1,
        );
        assert_eq!(stats.shapes[0].executions, 4);
        assert_eq!(
            stats.executions,
            stats
                .shapes
                .iter()
                .map(|entry| entry.executions)
                .sum::<u64>()
                + stats.untracked_executions
        );
        let json = serde_json::to_string(&stats).unwrap();
        assert!(!json.contains("private-literal"));
        assert!(!json.contains("123456789"));
        assert!(!json.contains("987654321"));
    }
}
