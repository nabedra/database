use super::*;
use crate::oltp::{
    codec::scalar_key,
    mvcc::{catalog::primary, store::KeySpan},
};
use protocol::{ComparisonOp, LogicalPredicate, Predicate};

#[derive(Default)]
pub(super) struct PrimaryRange {
    pub(super) lower: Option<(Key, bool)>,
    pub(super) upper: Option<(Key, bool)>,
}

pub(super) enum PrimaryConstraint {
    Empty,
    Point(Key),
    Range(PrimaryRange),
}

impl PrimaryConstraint {
    pub(super) fn from_query(query: &Query, key: &str) -> Self {
        #[derive(Default)]
        struct Comparisons<'a> {
            equal: Option<&'a protocol::Scalar>,
            lower: Option<(&'a protocol::Scalar, bool)>,
            upper: Option<(&'a protocol::Scalar, bool)>,
        }

        fn collect<'a>(predicate: &'a Predicate, key: &str, out: &mut Comparisons<'a>) {
            match predicate {
                Predicate::Logical(LogicalPredicate::And { args }) => {
                    for p in args {
                        collect(p, key, out);
                    }
                }
                Predicate::Comparison(p) if p.column == key => match p.op {
                    ComparisonOp::Eq => {
                        out.equal.get_or_insert(&p.value);
                    }
                    ComparisonOp::Ge | ComparisonOp::Gt => {
                        let inclusive = p.op == ComparisonOp::Ge;
                        if out.lower.is_none_or(|(old, old_inclusive)| {
                            query_semantics::compare(&p.value, old) == Ordering::Greater
                                || (p.value == *old && old_inclusive && !inclusive)
                        }) {
                            out.lower = Some((&p.value, inclusive));
                        }
                    }
                    ComparisonOp::Le | ComparisonOp::Lt => {
                        let inclusive = p.op == ComparisonOp::Le;
                        if out.upper.is_none_or(|(old, old_inclusive)| {
                            query_semantics::compare(&p.value, old) == Ordering::Less
                                || (p.value == *old && old_inclusive && !inclusive)
                        }) {
                            out.upper = Some((&p.value, inclusive));
                        }
                    }
                    ComparisonOp::Ne => {}
                },
                _ => {}
            }
        }

        if query.limit == Some(0) {
            return Self::Empty;
        }
        let mut cmp = Comparisons::default();
        if let Some(predicate) = &query.filter {
            collect(predicate, key, &mut cmp);
        }
        if let Some(equal) = cmp.equal {
            return Self::Point(scalar_key(equal));
        }
        if let (Some((lower, include_lower)), Some((upper, include_upper))) = (cmp.lower, cmp.upper)
        {
            match query_semantics::compare(lower, upper) {
                Ordering::Greater => return Self::Empty,
                Ordering::Equal if include_lower && include_upper => {
                    return Self::Point(scalar_key(lower));
                }
                Ordering::Equal => return Self::Empty,
                Ordering::Less => {}
            }
        }
        let range = PrimaryRange {
            lower: cmp
                .lower
                .map(|(value, inclusive)| (scalar_key(value), inclusive)),
            upper: cmp
                .upper
                .map(|(value, inclusive)| (scalar_key(value), inclusive)),
        };
        if range.has_no_keys() {
            Self::Empty
        } else {
            Self::Range(range)
        }
    }
}

impl PrimaryRange {
    pub(super) fn bounded(&self) -> bool {
        self.lower.is_some() || self.upper.is_some()
    }

    fn has_no_keys(&self) -> bool {
        matches!(&self.lower, Some((Key::Int64(i64::MAX), false)))
            || matches!(&self.upper, Some((Key::Int64(i64::MIN), false)))
            || matches!(&self.upper, Some((Key::Utf8(value), false)) if value.is_empty())
            || matches!((&self.lower, &self.upper), (Some((lower, false)), Some((upper, false)))
                if immediately_precedes(lower, upper))
    }

    fn before(&self, span: &KeySpan) -> bool {
        match (&span.end, &self.lower) {
            // A route's end key belongs to the successor route, even when the
            // caller includes the query's lower bound.
            (Some(end), Some((lower, inclusive))) => {
                primary(end, lower) != Ordering::Greater
                    || (!*inclusive && immediately_precedes(lower, end))
            }
            _ => false,
        }
    }

    fn after(&self, start: Option<&Key>) -> bool {
        match (start, &self.upper) {
            (Some(start), Some((upper, inclusive))) => match primary(start, upper) {
                Ordering::Greater => true,
                Ordering::Equal => !inclusive,
                Ordering::Less => false,
            },
            _ => false,
        }
    }
}

fn immediately_precedes(lower: &Key, upper: &Key) -> bool {
    match (lower, upper) {
        (Key::Int64(lower), Key::Int64(upper)) => lower.checked_add(1) == Some(*upper),
        // Appending NUL gives the immediate bytewise successor of a UTF-8 key.
        (Key::Utf8(lower), Key::Utf8(upper)) => upper.strip_suffix('\0') == Some(lower.as_str()),
        _ => false,
    }
}

/// The Catalog stores a forward-linked list of ordered, half-open slots. Retain
/// one page and stop before the next page when its first start is past the seek.
pub(super) struct RoutingCursor<'a> {
    catalog: &'a CatalogHandle,
    table: &'a TableRecord,
    pin: &'a PinnedCatalog,
    bounds: &'a PrimaryRange,
    page: Option<OwnedRecord<RoutingPage>>,
    next_page: Option<Uuid>,
    index: usize,
    pub(super) pages_read: u64,
}

impl<'a> RoutingCursor<'a> {
    pub(super) async fn open(
        catalog: &'a CatalogHandle,
        table: &'a TableRecord,
        pin: &'a PinnedCatalog,
        bounds: &'a PrimaryRange,
    ) -> Result<Self, PlanError> {
        let (_, root) = value(catalog.routing_root(pin, table.descriptor.id).await?)?;
        let root = root.ok_or_else(not_found)?;
        if root.schema_epoch != table.schema_epoch
            || root.routing_epoch != table.routing_epoch
            || root.first_page.is_none()
        {
            return Err(invalid_routing().into());
        }
        Ok(Self {
            catalog,
            table,
            pin,
            bounds,
            page: None,
            next_page: root.first_page,
            index: 0,
            pages_read: 0,
        })
    }

    pub(super) async fn next(
        &mut self,
    ) -> Result<Option<(OwnedRecord<RoutingPage>, usize)>, PlanError> {
        loop {
            if let Some(page) = &self.page {
                while let Some(route) = page.shards.get(self.index) {
                    let index = self.index;
                    self.index += 1;
                    validate_route(self.table, route)?;
                    if self.bounds.before(&route.span) {
                        continue;
                    }
                    if self.bounds.after(route.span.start.as_ref()) {
                        return Ok(None);
                    }
                    return Ok(Some((page.clone(), index)));
                }
            }

            let Some(id) = self.next_page else {
                return Ok(None);
            };
            self.page = None;
            let (_, page) = value(
                self.catalog
                    .routing_page(self.pin, self.table.descriptor.id, id)
                    .await?,
            )?;
            let page = page.ok_or_else(not_found)?;
            let last = page.shards.last().ok_or_else(invalid_routing)?;
            if page.next_page == Some(id) || page.next_page.is_some() != last.span.end.is_some() {
                return Err(invalid_routing().into());
            }
            self.next_page = if self.bounds.after(last.span.end.as_ref()) {
                None
            } else {
                page.next_page
            };
            self.pages_read = checked_scope_increment(self.pages_read)?;
            self.index = 0;
            self.page = Some(page);
        }
    }
}

pub(super) fn validate_route(table: &TableRecord, route: &ShardRoute) -> Result<(), ProtocolError> {
    if route.schema_epoch != table.schema_epoch
        || route.routing_epoch == 0
        || route.routing_epoch > table.routing_epoch
    {
        return Err(invalid_routing());
    }
    Ok(())
}

pub(super) fn checked_scope_increment(count: u64) -> Result<u64, ProtocolError> {
    count.checked_add(1).ok_or_else(|| {
        failure(
            ErrorCode::LimitExceeded,
            "routing count exceeds u64 representation",
        )
    })
}

fn invalid_routing() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "invalid routing at the pinned Catalog epoch",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn o03_o28_half_open_split_fences_keep_inclusive_upper_keys() {
        for (below, split, above) in [
            (Key::Int64(i64::MIN), Key::Int64(-1), Key::Int64(i64::MAX)),
            (
                Key::Utf8("a".into()),
                Key::Utf8("a\0".into()),
                Key::Utf8("é".into()),
            ),
        ] {
            let left = KeySpan {
                start: None,
                end: Some(split.clone()),
            };
            let right = KeySpan {
                start: Some(split.clone()),
                end: Some(above.clone()),
            };
            let leading = PrimaryRange {
                lower: Some((below, true)),
                upper: Some((split.clone(), false)),
            };
            assert!(!leading.before(&left));
            assert!(leading.after(right.start.as_ref()));
            // A cursor can skip the successor page when the current page's
            // end proves its first range is beyond this exclusive cutoff.
            assert!(leading.after(left.end.as_ref()));
            let boundary = PrimaryRange {
                lower: None,
                upper: Some((split.clone(), true)),
            };
            assert!(!boundary.after(right.start.as_ref()));
            assert!(!boundary.after(left.end.as_ref()));
            assert!(boundary.after(Some(&above)));
            let trailing = PrimaryRange {
                lower: Some((split, true)),
                upper: None,
            };
            assert!(trailing.before(&left));
            assert!(!trailing.before(&right));
        }
    }

    #[test]
    fn o03_o04_nested_residuals_never_narrow_the_primary_seek() {
        let query: Query = serde_json::from_value(json!({
            "select": null, "order_by": null, "limit": null,
            "filter": {"op":"and", "args":[
                {"column":"id", "op":"ge", "value":i64::MIN},
                {"column":"id", "op":"gt", "value":i64::MIN},
                {"op":"or", "args":[
                    {"column":"id", "op":"eq", "value":0},
                    {"op":"not", "arg":{"column":"id", "op":"le", "value":1}}
                ]}
            ]}
        }))
        .unwrap();
        let PrimaryConstraint::Range(range) = PrimaryConstraint::from_query(&query, "id") else {
            panic!("the OR branch must remain a residual");
        };
        assert_eq!(range.lower, Some((Key::Int64(i64::MIN), false)));
        assert_eq!(range.upper, None);
    }

    #[test]
    fn o03_typed_empty_intervals_skip_data_shards() {
        for (operator, value) in [
            ("gt", json!(i64::MAX)),
            ("lt", json!(i64::MIN)),
            ("lt", json!("")),
        ] {
            let query: Query = serde_json::from_value(json!({
                "select":null,"order_by":null,"limit":null,
                "filter":{"column":"id","op":operator,"value":value},
            }))
            .unwrap();
            assert!(matches!(
                PrimaryConstraint::from_query(&query, "id"),
                PrimaryConstraint::Empty
            ));
        }
        for (lower, upper) in [
            (Key::Int64(-1), Key::Int64(0)),
            (Key::Utf8("a".into()), Key::Utf8("a\0".into())),
        ] {
            let range = PrimaryRange {
                lower: Some((lower, false)),
                upper: Some((upper.clone(), false)),
            };
            assert!(range.has_no_keys());
            let shard = KeySpan {
                start: None,
                end: Some(upper),
            };
            assert!(range.before(&shard));
            let inclusive = PrimaryRange {
                lower: range.lower.map(|(key, _)| (key, true)),
                upper: range.upper,
            };
            assert!(!inclusive.has_no_keys());
            assert!(!inclusive.before(&shard));
        }
    }
}
