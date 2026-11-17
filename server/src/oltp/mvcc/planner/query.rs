use super::*;
use crate::oltp::mvcc::{
    catalog::{RoutingPage, decode_canonical},
    store::scan::{ScanDomain, ScanPage, ScanRequest},
};
use crate::{
    oltp::pool::{ByteClass, ScratchLease},
    protocol::Query,
    query_semantics,
};
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};
use std::{cmp::Ordering, collections::BinaryHeap};
struct OrderingContext {
    table: OwnedRecord<TableRecord>,
    query: Query,
    _owner: Arc<ScratchLease>,
}
struct Item {
    row: OwnedRecord<Row>,
    order: Arc<OrderingContext>,
}
impl PartialEq for Item {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Item {
    fn cmp(&self, o: &Self) -> Ordering {
        query_semantics::compare_rows(
            &self.order.table.descriptor,
            &self.order.query,
            &self.row,
            &o.row,
        )
    }
}
struct State {
    heap: BinaryHeap<Item>,
    matched: u64,
    cap: usize,
    count: bool,
    order: Arc<OrderingContext>,
    _container: Arc<ScratchLease>,
}
struct Cursor {
    key: MvccKey,
    owner: Arc<ScratchLease>,
}
impl NativePlanner {
    pub(super) async fn query(
        &self,
        pin: &PinnedCatalog,
        table: OwnedRecord<TableRecord>,
        query: Query,
        count: bool,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let cap = query
            .limit
            .unwrap_or(u64::MAX)
            .min((protocol::MAX_RESULT_ROWS + 1) as u64) as usize;
        if cap == 0 {
            return Ok(self
                .catalog
                .resources()
                .copy_bytes(if count { b"0" } else { b"[]" })?);
        }
        let shared = self.catalog.resources();
        let container = Arc::new(shared.reserve_scratch(
            ByteClass::Data,
            if count {
                4096
            } else {
                cap.checked_mul(std::mem::size_of::<Item>() * 2)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(not_found)?
            },
        )?);
        let order = Arc::new(OrderingContext {
            table: table.clone(),
            query,
            _owner: owner.clone(),
        });
        let mut heap = BinaryHeap::new();
        if !count {
            heap.try_reserve_exact(cap)
                .map_err(|_| failure(ErrorCode::LimitExceeded, "top-k metadata exceeds memory"))?;
        }
        let mut state = State {
            heap,
            matched: 0,
            cap,
            count,
            order,
            _container: container,
        };
        // Equality on the primary key touches one owning Data group. Every
        // predicate remains a residual check with the shared exact semantics.
        if let Some(key) = point_key(&state.order.query, &table.descriptor.key) {
            let (_, row) = self.row(pin, &table, &key).await?;
            if let Some(row) = row {
                state = self.accept_row(state, row, owner.clone()).await?;
            }
            return self.finish_query(state, owner).await;
        }
        let mut next = match value(self.catalog.routing_root(pin, table.descriptor.id).await?)? {
            (_, Some(root)) => root.first_page,
            _ => return Err(not_found().into()),
        };
        while let Some(page_id) = next {
            let (_, page) = value(
                self.catalog
                    .routing_page(pin, table.descriptor.id, page_id)
                    .await?,
            )?;
            let page = page.ok_or_else(not_found)?;
            for route in page.shards.iter() {
                if route.schema_epoch != table.schema_epoch
                    || route.routing_epoch != table.routing_epoch
                {
                    return Err(not_found().into());
                }
                let participant = self.resolver.participant(&route.group).await?;
                let mut cursor: Option<Cursor> = None;
                loop {
                    let (after, after_owner) = cursor
                        .take()
                        .map_or((None, None), |c| (Some(c.key), Some(c.owner)));
                    let response = participant
                        .scan_page(ScanRequest {
                            domain: ScanDomain::Rows {
                                table_id: table.descriptor.id,
                            },
                            read_ts: pin.read_ts,
                            after,
                            after_owner,
                            lower: primary_bounds(&state.order.query, &table.descriptor.key).0,
                            upper: primary_bounds(&state.order.query, &table.descriptor.key).1,
                            max_items: 128,
                            max_bytes: super::super::MAX_FIXED_COMMAND_BYTES,
                        })
                        .await?;
                    match response.payload()? {
                        ScanPage::Blocked { txn, manifest } => {
                            return Err(PlanError::Blocked(IntentRef {
                                txn: *txn,
                                manifest: *manifest,
                            }));
                        }
                        ScanPage::Records { records, exhausted } => {
                            for record in records.iter() {
                                if let Some(value) = &record.value {
                                    let row =
                                        decode_canonical(self.catalog.resources(), value.clone())
                                            .await?;
                                    state = self.accept_row(state, row, owner.clone()).await?;
                                    if count
                                        && state.matched
                                            >= state.order.query.limit.unwrap_or(u64::MAX)
                                    {
                                        return self.finish_query(state, owner).await;
                                    }
                                }
                            }
                            if *exhausted {
                                break;
                            }
                            let last = records.last().ok_or_else(not_found)?;
                            let charge =
                                Arc::new(self.catalog.resources().reserve_scratch(
                                    ByteClass::Data,
                                    last.key.heap_bytes() + 512,
                                )?);
                            let copy = response.clone();
                            cursor = Some(
                                self.cpu(owner.clone(), move || {
                                    let ScanPage::Records { records, .. } = copy.payload()? else {
                                        return Err(not_found());
                                    };
                                    Ok(Cursor {
                                        key: records.last().ok_or_else(not_found)?.key.clone(),
                                        owner: charge,
                                    })
                                })
                                .await?,
                            );
                        }
                    }
                }
            }
            next = page.next_page;
        }
        self.finish_query(state, owner).await
    }
    async fn accept_row(
        &self,
        mut state: State,
        row: OwnedRecord<Row>,
        owner: Arc<ScratchLease>,
    ) -> Result<State, PlanError> {
        Ok(self
            .cpu(owner, move || {
                protocol::validate_row(
                    &row,
                    &state.order.table.descriptor,
                    protocol::RowMode::Complete,
                )?;
                if query_semantics::matches_filter(&state.order.query, &row) {
                    state.matched = state.matched.checked_add(1).ok_or_else(|| {
                        failure(ErrorCode::LimitExceeded, "count exceeds u64 representation")
                    })?;
                    if !state.count {
                        if state.cap > protocol::MAX_RESULT_ROWS
                            && state.matched > protocol::MAX_RESULT_ROWS as u64
                        {
                            return Err(failure(
                                ErrorCode::LimitExceeded,
                                "result exceeds 10000 rows",
                            ));
                        }
                        let item = Item {
                            row,
                            order: state.order.clone(),
                        };
                        if state.heap.len() < state.cap {
                            state.heap.push(item)
                        } else if state.heap.peek().is_some_and(|worst| item < *worst) {
                            state.heap.pop();
                            state.heap.push(item)
                        }
                    }
                }
                Ok(state)
            })
            .await?)
    }
    async fn finish_query(
        &self,
        state: State,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let shared = self.catalog.resources().clone();
        Ok(self
            .cpu(owner, move || {
                if state.count {
                    return shared.encode(
                        &state
                            .matched
                            .min(state.order.query.limit.unwrap_or(u64::MAX)),
                        protocol::MAX_BODY_BYTES - 128,
                    );
                }
                let _container = state._container;
                let mut rows = state.heap.into_vec();
                rows.sort_unstable();
                shared.encode(
                    &Rows {
                        rows: &rows,
                        select: state.order.query.select.as_deref(),
                    },
                    protocol::MAX_BODY_BYTES - 128,
                )
            })
            .await?)
    }
    pub(super) async fn list(
        &self,
        pin: &PinnedCatalog,
        owner: Arc<ScratchLease>,
    ) -> Result<ChargedBytes, PlanError> {
        let mut after = None;
        let mut after_owner = None;
        let mut tables: Vec<OwnedRecord<TableRecord>> = Vec::new();
        let mut bytes = 2usize;
        loop {
            let page = self
                .catalog
                .participant()
                .scan_page(ScanRequest {
                    domain: ScanDomain::CatalogNames,
                    read_ts: pin.read_ts,
                    after,
                    after_owner,
                    lower: None,
                    upper: None,
                    max_items: 128,
                    max_bytes: super::super::MAX_FIXED_COMMAND_BYTES,
                })
                .await?;
            let ScanPage::Records { records, exhausted } = page.payload()? else {
                let ScanPage::Blocked { txn, manifest } = page.payload()? else {
                    unreachable!()
                };
                return Err(PlanError::Blocked(IntentRef {
                    txn: *txn,
                    manifest: *manifest,
                }));
            };
            for record in records.iter() {
                if let Some(raw) = &record.value {
                    let name: OwnedRecord<NameRecord> =
                        decode_canonical(self.catalog.resources(), raw.clone()).await?;
                    let (_, table) = value(self.catalog.describe(pin, name.table_id).await?)?;
                    let table = table.ok_or_else(not_found)?;
                    let copy = table.clone();
                    bytes = bytes
                        .checked_add(
                            self.cpu(owner.clone(), move || {
                                crate::oltp::query::check_body(
                                    &copy.descriptor,
                                    protocol::MAX_BODY_BYTES,
                                )
                            })
                            .await?
                                + 1,
                        )
                        .ok_or_else(not_found)?;
                    if bytes > protocol::MAX_BODY_BYTES - 128 {
                        return Err(failure(
                            ErrorCode::LimitExceeded,
                            "table listing exceeds response budget",
                        )
                        .into());
                    }
                    tables.try_reserve(1).map_err(|_| {
                        failure(
                            ErrorCode::LimitExceeded,
                            "table listing metadata allocation",
                        )
                    })?;
                    tables.push(table);
                }
            }
            if *exhausted {
                break;
            }
            let last = records.last().ok_or_else(not_found)?;
            let charge = Arc::new(
                self.catalog
                    .resources()
                    .reserve_scratch(ByteClass::Data, last.key.heap_bytes() + 512)?,
            );
            after = Some(last.key.clone());
            after_owner = Some(charge);
        }
        let shared = self.catalog.resources().clone();
        Ok(self
            .cpu(owner, move || {
                shared.encode(&TableList(&tables), protocol::MAX_BODY_BYTES - 128)
            })
            .await?)
    }
}
struct Rows<'a> {
    rows: &'a [Item],
    select: Option<&'a [String]>,
}
impl Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.rows.len()))?;
        for item in self.rows {
            seq.serialize_element(&Projection {
                row: &item.row,
                select: self.select,
            })?;
        }
        seq.end()
    }
}
struct Projection<'a> {
    row: &'a Row,
    select: Option<&'a [String]>,
}
impl Serialize for Projection<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        for (k, v) in self.row {
            if self.select.is_none_or(|cols| cols.contains(k)) {
                map.serialize_entry(k, v)?;
            }
        }
        map.end()
    }
}
struct TableList<'a>(&'a [OwnedRecord<TableRecord>]);
impl Serialize for TableList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for t in self.0 {
            seq.serialize_element(&t.descriptor)?;
        }
        seq.end()
    }
}
fn point_key(query: &Query, key: &str) -> Option<Key> {
    fn find(p: &protocol::Predicate, key: &str) -> Option<Key> {
        match p {
            protocol::Predicate::Comparison(p)
                if p.column == key && p.op == protocol::ComparisonOp::Eq =>
            {
                Some(crate::oltp::codec::scalar_key(&p.value))
            }
            protocol::Predicate::Logical(protocol::LogicalPredicate::And { args }) => {
                args.iter().find_map(|p| find(p, key))
            }
            _ => None,
        }
    }
    query.filter.as_ref().and_then(|p| find(p, key))
}

fn primary_bounds(query: &Query, key: &str) -> (Option<(Key, bool)>, Option<(Key, bool)>) {
    fn merge(
        p: &protocol::Predicate,
        key: &str,
        lower: &mut Option<(Key, bool)>,
        upper: &mut Option<(Key, bool)>,
    ) {
        match p {
            protocol::Predicate::Logical(protocol::LogicalPredicate::And { args }) => {
                for p in args {
                    merge(p, key, lower, upper)
                }
            }
            protocol::Predicate::Comparison(p) if p.column == key => {
                use protocol::ComparisonOp::*;
                let value = crate::oltp::codec::scalar_key(&p.value);
                let order = |a: &Key, b: &Key| match (a, b) {
                    (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
                    (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
                    _ => Ordering::Equal,
                };
                match p.op {
                    Ge | Gt => {
                        let inclusive = p.op == Ge;
                        if lower.as_ref().is_none_or(|(old, inc)| {
                            order(&value, old) == Ordering::Greater
                                || (value == *old && *inc && !inclusive)
                        }) {
                            *lower = Some((value, inclusive));
                        }
                    }
                    Le | Lt => {
                        let inclusive = p.op == Le;
                        if upper.as_ref().is_none_or(|(old, inc)| {
                            order(&value, old) == Ordering::Less
                                || (value == *old && *inc && !inclusive)
                        }) {
                            *upper = Some((value, inclusive));
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    let (mut lower, mut upper) = (None, None);
    if let Some(filter) = &query.filter {
        merge(filter, key, &mut lower, &mut upper)
    };
    (lower, upper)
}
