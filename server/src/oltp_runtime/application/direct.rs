//! The direct-path gateway (plan `oltp-direct-path.md` section 3, the
//! gateway paragraphs of section 6, "Failure behavior" and the addendum of
//! 2026-11-19).
//!
//! An eligible `/v1/operations` request (`table.get`, `insert`, `upsert`,
//! `update`, `delete`, or an `insert_many` whose keys share one range) goes
//! straight to the leader of its key range's Data group over the worker's
//! persistent session. The gateway:
//!
//! - resolves the range from a bounded [`TopologyCache`]. A miss costs one
//!   Catalog point lookup and one Control placement read per range. An
//!   insert with no cached descriptor first resolves the primary key column.
//!   A stale route, leader change or unknown table can refresh the cache.
//!   The steady state makes no Control or
//!   Catalog call per request. A cached route never authorizes anything:
//!   the leader and the log entry check epochs and fences;
//! - retries `NotLeader` and `StaleRoute` against the refreshed route, and
//!   resends the same id and body after a lost reply (the leader's dedup
//!   record makes that exactly-once). At the deadline an attempt that may
//!   have executed is `outcome_unknown`;
//! - on `Fallback`, and before any eligible mutation takes the queued path
//!   while `data.direct.v1` is active, settles the request id at the anchor
//!   (its smallest key) with `DirectForward`. A replayed result is returned
//!   as is; a forward marker sends the request down the queued path, and
//!   no later direct attempt of that id can apply. When the anchor cannot be
//!   reached the answer is `outcome_unknown`, never the queued path.
//! - never settles a `table.get` (user decision 2026-11-21, RV-FINAL M3): a
//!   read has no exactly-once identity of its own, so a get that does not
//!   run directly takes the queued path as is and costs no Data entry. A
//!   retried get that runs directly is a new linearizable read.
//!
//! The off switch: while `data.direct.v1` is not active every request takes
//! the queued path exactly as before (O01).
use crate::{
    oltp::{
        mvcc::{
            catalog::{
                CatalogHandle, CatalogRead, OwnedRecord, PinnedCatalog, RouteRead, RoutingRoot,
                ShardRoute, TableRecord,
            },
            store::{compare_primary, encoded_length},
        },
        pool::{ByteClass, ScratchLease, WorkerSharedResources},
    },
    oltp_replication::{
        multiplex::EndpointFuture,
        service_rpc::direct::{
            DirectData, DirectRequest, DirectResponse, DirectSendError, FallbackReason,
        },
        types::{Authority, Digest, GroupIdentity, GroupKind, ReadTs, Text},
    },
    protocol::{ColumnType, ErrorCode, Key, Outcome, ProtocolError},
    response::{self, EncodedResponse},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

/// Time a direct request may take at the gateway, forward included.
pub(crate) const GATEWAY_DEADLINE: Duration = Duration::from_secs(10);
/// The largest `insert_many` body the gateway sends directly. The leader
/// checks the encoded entry against the same 1 MiB.
pub(crate) const MAX_DIRECT_BATCH_BODY: usize = 1 << 20;
/// The gateway's retained route budget, independent of table size.
const CACHE_RANGES: usize = 256;
const CACHE_BYTES: usize = 1 << 20;
/// Leave room for the observation RPC's reply header.
pub(super) const ROUTE_REPLY_BYTES: usize = 60 << 10;
/// The leader gets this much less time than the gateway, so its own
/// deadline answer arrives before the gateway gives up.
const LEADER_MARGIN: Duration = Duration::from_millis(100);
// Leave time for another voter to answer within the gateway deadline, even
// when TCP accepts frames but the peer never acknowledges or replies.
const PEER_ATTEMPT: Duration = Duration::from_millis(1500);
const FIRST_PAUSE: Duration = Duration::from_millis(10);
const MAX_PAUSE: Duration = Duration::from_millis(200);

/// What the gateway does with eligible operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectMode {
    /// `data.direct.v1` is not active: the queued path, unchanged.
    Off,
    /// Active, `direct_oltp: false`: eligible mutations are settled at
    /// their anchor, then take the queued path. Gets take it unsettled.
    ForwardOnly,
    /// Active, `direct_oltp: true`, inside the queued-history window (T7c):
    /// eligible mutations are settled and go queued, because the leader
    /// would answer `Fallback{History}`; gets go direct (RV-FINAL M3).
    Settling,
    /// Active, `direct_oltp: true`: eligible operations go direct.
    Direct,
}

/// Where the gateway reads `data.direct.v1` and `direct_oltp` from: T7's
/// `DirectSwitch` in production.
pub(crate) trait DirectRouting: Send + Sync {
    fn mode(&self) -> DirectMode;
}
impl crate::oltp_runtime::worker_runtime::direct_capability::DirectSwitch {
    /// The gateway's mode: `requires_forward()` is the capability,
    /// `routes_direct()` adds `direct_oltp`. Within the queued-history
    /// window (T7c) eligible mutations are settled and go queued: the leader
    /// would answer `Fallback{History}` anyway. Gets still go direct.
    pub(crate) fn mode(&self) -> DirectMode {
        match mode(self.requires_forward(), self.routes_direct()) {
            DirectMode::Direct if self.history_settling() => DirectMode::Settling,
            mode => mode,
        }
    }
}
impl DirectRouting for crate::oltp_runtime::worker_runtime::direct_capability::DirectSwitch {
    fn mode(&self) -> DirectMode {
        crate::oltp_runtime::worker_runtime::direct_capability::DirectSwitch::mode(self)
    }
}

/// Two flags, for tests: `active` is the capability, `direct_oltp` the
/// operator setting (on by default).
#[cfg(test)]
pub(crate) struct GatewaySwitch {
    active: std::sync::atomic::AtomicBool,
    direct_oltp: std::sync::atomic::AtomicBool,
    /// The queued-history window (T7c).
    history: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl Default for GatewaySwitch {
    fn default() -> Self {
        Self {
            active: std::sync::atomic::AtomicBool::new(false),
            direct_oltp: std::sync::atomic::AtomicBool::new(true),
            history: std::sync::atomic::AtomicBool::new(false),
        }
    }
}
#[cfg(test)]
impl GatewaySwitch {
    pub(crate) fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::Release);
    }
    pub(crate) fn set_direct_oltp(&self, enabled: bool) {
        self.direct_oltp.store(enabled, Ordering::Release);
    }
    pub(crate) fn set_history(&self, settling: bool) {
        self.history.store(settling, Ordering::Release);
    }
}
#[cfg(test)]
impl DirectRouting for GatewaySwitch {
    fn mode(&self) -> DirectMode {
        match mode(
            self.active.load(Ordering::Acquire),
            self.active.load(Ordering::Acquire) && self.direct_oltp.load(Ordering::Acquire),
        ) {
            DirectMode::Direct if self.history.load(Ordering::Acquire) => DirectMode::Settling,
            mode => mode,
        }
    }
}
/// `forward`: the capability is active (every queued eligible request is
/// settled at its anchor first); `direct`: eligible operations go direct.
pub(crate) fn mode(forward: bool, direct: bool) -> DirectMode {
    match (forward, direct) {
        (false, _) => DirectMode::Off,
        (true, false) => DirectMode::ForwardOnly,
        (true, true) => DirectMode::Direct,
    }
}

/// One replica of a range: its Raft node id and its worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Voter {
    pub node_id: u64,
    pub worker_id: Uuid,
}

/// What one Catalog read returns for a table.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableFill {
    pub table_id: Uuid,
    pub key_column: String,
    pub key_type: ColumnType,
    pub schema_epoch: u64,
    pub shards: Vec<ShardRoute>,
}

/// An authenticated Catalog lookup returns only the selected range.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PointFill {
    pub table_id: Uuid,
    pub key_column: String,
    pub key_type: ColumnType,
    pub schema_epoch: u64,
    pub shard: ShardRoute,
}

impl TableFill {
    pub(super) fn select(mut self, key: Option<&Key>) -> Option<PointFill> {
        let index = key
            .filter(|key| key.validate(self.key_type).is_ok())
            .map_or(Some(0), |key| {
                self.shards.iter().position(|shard| shard.contains_key(key))
            })?;
        if index >= self.shards.len() {
            return None;
        }
        Some(PointFill {
            table_id: self.table_id,
            key_column: self.key_column,
            key_type: self.key_type,
            schema_epoch: self.schema_epoch,
            shard: self.shards.swap_remove(index),
        })
    }
}

/// One key range of a table (plan section 3). The voters come from Control
/// on first use; the leader hint follows `NotLeader` replies.
pub(crate) struct RangeRoute {
    pub shard: ShardRoute,
    voters: Mutex<Option<Arc<[Voter]>>>,
    /// Node id of the last observed leader, 0 when unknown.
    leader_hint: AtomicU64,
}
impl RangeRoute {
    fn new(shard: ShardRoute) -> Self {
        Self {
            shard,
            voters: Mutex::new(None),
            leader_hint: AtomicU64::new(0),
        }
    }
    pub(crate) fn group(&self) -> &GroupIdentity {
        &self.shard.group
    }
    pub(crate) fn leader_hint(&self) -> Option<u64> {
        Some(self.leader_hint.load(Ordering::Relaxed)).filter(|node| *node != 0)
    }
    fn forget_leader(&self, node: u64) {
        let _ = self
            .leader_hint
            .compare_exchange(node, 0, Ordering::Relaxed, Ordering::Relaxed);
    }
    fn cached_voters(&self) -> Option<Arc<[Voter]>> {
        self.voters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// A cached range and the schema needed to extract keys for that table.
pub(crate) struct TableRoute {
    pub table_id: Uuid,
    pub key_column: String,
    pub key_type: ColumnType,
    pub schema_epoch: u64,
    pub range: Arc<RangeRoute>,
    // Keep key-column metadata on an unavailable range so retrying an
    // insert can look up its anchor without filling another range first.
    unavailable: AtomicBool,
    refresh_before_settle: AtomicBool,
    _charge: ScratchLease,
}
impl TableRoute {
    fn covers(&self, key: Option<&Key>) -> bool {
        key.is_none_or(|key| {
            !self.unavailable.load(Ordering::Relaxed)
                && (key.validate(self.key_type).is_err() || self.range.shard.contains_key(key))
        })
    }

    fn heap_bytes(fill: &PointFill) -> usize {
        let key = |key: &Option<Key>| match key {
            Some(Key::Utf8(text)) => text.capacity(),
            _ => 0,
        };
        // Include the maximum number of voters plus the LRU and Arc metadata.
        std::mem::size_of::<TableRoute>()
            + std::mem::size_of::<RangeRoute>()
            + 7 * std::mem::size_of::<Voter>()
            + 256
            + fill.key_column.capacity()
            + fill.shard.group.database.capacity_bytes()
            + key(&fill.shard.span.start)
            + key(&fill.shard.span.end)
    }
}

/// Byte- and entry-bounded LRU. Unrelated ranges keep their placement and
/// leader hints when a split replaces the overlapping parent.
pub(crate) struct TopologyCache {
    inner: Mutex<Lru>,
}
#[derive(Default)]
struct Lru {
    tick: u64,
    entries: HashMap<Uuid, Vec<(Arc<TableRoute>, u64)>>,
    count: usize,
    bytes: usize,
    peak_bytes: usize,
}
impl Lru {
    fn remove(&mut self, table_id: Uuid, mut matches: impl FnMut(&Arc<TableRoute>, u64) -> bool) {
        if let Some(routes) = self.entries.get_mut(&table_id) {
            routes.retain(|(route, tick)| {
                if matches(route, *tick) {
                    self.count -= 1;
                    self.bytes -= route._charge.bytes();
                    false
                } else {
                    true
                }
            });
            if routes.is_empty() {
                self.entries.remove(&table_id);
            }
        }
    }
}
impl TopologyCache {
    fn new() -> Self {
        Self {
            inner: Mutex::default(),
        }
    }
    pub(crate) fn get(&self, table_id: Uuid, key: Option<&Key>) -> Option<Arc<TableRoute>> {
        let mut lru = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        lru.tick += 1;
        let tick = lru.tick;
        let (route, used) = lru
            .entries
            .get_mut(&table_id)?
            .iter_mut()
            .find(|(route, _)| route.covers(key))?;
        *used = tick;
        Some(route.clone())
    }
    fn insert(&self, route: Arc<TableRoute>) {
        let bytes = route._charge.bytes();
        let mut lru = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let span = &route.range.shard.span;
        let before = |end: &Option<Key>, start: &Option<Key>| {
            end.as_ref()
                .zip(start.as_ref())
                .is_some_and(|(end, start)| !compare_primary(end, start).is_gt())
        };
        lru.remove(route.table_id, |existing, _| {
            let old = &existing.range.shard.span;
            existing.schema_epoch != route.schema_epoch
                || !(before(&old.end, &span.start) || before(&span.end, &old.start))
        });
        if bytes > CACHE_BYTES {
            return;
        }
        while lru.count == CACHE_RANGES || lru.bytes + bytes > CACHE_BYTES {
            let (table_id, oldest) = lru
                .entries
                .iter()
                .flat_map(|(&table_id, routes)| {
                    routes.iter().map(move |(_, tick)| (table_id, *tick))
                })
                .min_by_key(|&(_, tick)| tick)
                .expect("a full cache has an entry");
            lru.remove(table_id, |_, tick| tick == oldest);
        }
        lru.tick += 1;
        let tick = lru.tick;
        lru.count += 1;
        lru.bytes += bytes;
        lru.peak_bytes = lru.peak_bytes.max(lru.bytes);
        lru.entries
            .entry(route.table_id)
            .or_default()
            .push((route, tick));
    }
    fn remove(&self, table_id: Uuid) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(table_id, |_, _| true);
    }
    fn usage(&self) -> (usize, usize, usize) {
        let lru = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        (lru.count, lru.bytes, lru.peak_bytes)
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.usage().0
    }
}

pub(crate) trait TopologySource: Send + Sync {
    /// With no key, read the descriptor and first range so a row can be
    /// decoded using its primary key column.
    fn point<'a>(
        &'a self,
        table_id: Uuid,
        key: Option<&'a Key>,
    ) -> EndpointFuture<'a, Option<PointFill>>;
    /// The voters of `group`. `refresh` drops any placement the source
    /// itself cached before reading.
    fn placement<'a>(
        &'a self,
        group: &'a GroupIdentity,
        refresh: bool,
    ) -> EndpointFuture<'a, Vec<Voter>>;
}

pub(crate) type SendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DirectResponse, DirectSendError>> + Send + 'a>>;

/// Sends one request to one voter: `Method::Direct`, or
/// `Method::DirectForward` when `forward`.
pub(crate) trait DirectTransport: Send + Sync {
    fn send<'a>(
        &'a self,
        group: &'a GroupIdentity,
        voter: Voter,
        request: &'a DirectRequest,
        forward: bool,
    ) -> SendFuture<'a>;
}

/// Gateway counters. Tests read them; `/v1/runtime/stats` can show them.
#[derive(Default)]
pub(crate) struct GatewayStats {
    pub requests: AtomicU64,
    pub direct_done: AtomicU64,
    pub queued: AtomicU64,
    pub forwards: AtomicU64,
    pub forward_replayed: AtomicU64,
    pub fallbacks: AtomicU64,
    pub catalog_reads: AtomicU64,
    pub placement_reads: AtomicU64,
    pub stale_routes: AtomicU64,
    pub not_leader: AtomicU64,
    pub resends: AtomicU64,
    pub peer_timeouts: AtomicU64,
    pub unknown: AtomicU64,
    pub overloaded: AtomicU64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct GatewayStatsSnapshot {
    pub requests: u64,
    pub direct_done: u64,
    pub queued: u64,
    pub forwards: u64,
    pub forward_replayed: u64,
    pub fallbacks: u64,
    pub catalog_reads: u64,
    pub placement_reads: u64,
    pub stale_routes: u64,
    pub not_leader: u64,
    pub resends: u64,
    pub peer_timeouts: u64,
    pub unknown: u64,
    pub overloaded: u64,
    pub cached_ranges: usize,
    pub cached_route_bytes: usize,
    pub peak_cached_route_bytes: usize,
    pub cache_limit_bytes: usize,
}
impl GatewayStats {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(&self, cache: &TopologyCache) -> GatewayStatsSnapshot {
        let (cached_ranges, cached_route_bytes, peak_cached_route_bytes) = cache.usage();
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        GatewayStatsSnapshot {
            requests: get(&self.requests),
            direct_done: get(&self.direct_done),
            queued: get(&self.queued),
            forwards: get(&self.forwards),
            forward_replayed: get(&self.forward_replayed),
            fallbacks: get(&self.fallbacks),
            catalog_reads: get(&self.catalog_reads),
            placement_reads: get(&self.placement_reads),
            stale_routes: get(&self.stale_routes),
            not_leader: get(&self.not_leader),
            resends: get(&self.resends),
            peer_timeouts: get(&self.peer_timeouts),
            unknown: get(&self.unknown),
            overloaded: get(&self.overloaded),
            cached_ranges,
            cached_route_bytes,
            peak_cached_route_bytes,
            cache_limit_bytes: CACHE_BYTES,
        }
    }
}

/// What the gateway did with a request.
pub(crate) enum Routed {
    /// The complete response: committed, replayed or read directly.
    Done(EncodedResponse),
    /// Take the queued path. For an eligible mutation while the capability
    /// is active, the request id is settled at its anchor first; a get is
    /// never settled.
    Queued,
}

/// The fields routing needs, scanned from the exact body without decoding
/// rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Get,
    Insert,
    InsertMany,
    Update,
    Upsert,
    Delete,
}
impl Op {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "table.get" => Self::Get,
            "table.insert" => Self::Insert,
            "table.insert_many" => Self::InsertMany,
            "table.update" => Self::Update,
            "table.upsert" => Self::Upsert,
            "table.delete" => Self::Delete,
            _ => return None,
        })
    }
}
struct Scanned<'a> {
    request_id: Uuid,
    op: Op,
    table_id: Uuid,
    payload: &'a RawValue,
}

enum Terminal {
    Done(DirectData),
    Fallback(FallbackReason),
    Failed(ProtocolError),
}

struct RouteAttempt<'a> {
    table: Arc<TableRoute>,
    forward: bool,
    last: Option<&'a Key>,
}

pub(crate) struct DirectGateway {
    shared: Arc<WorkerSharedResources>,
    database: Text<128>,
    switch: Arc<dyn DirectRouting>,
    source: Arc<dyn TopologySource>,
    transport: Arc<dyn DirectTransport>,
    cache: TopologyCache,
    fill: tokio::sync::Mutex<()>,
    stats: GatewayStats,
    deadline: Duration,
}

impl DirectGateway {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        database: Text<128>,
        switch: Arc<dyn DirectRouting>,
        source: Arc<dyn TopologySource>,
        transport: Arc<dyn DirectTransport>,
    ) -> Arc<Self> {
        Arc::new(Self {
            shared,
            database,
            switch,
            source,
            transport,
            cache: TopologyCache::new(),
            fill: tokio::sync::Mutex::new(()),
            stats: GatewayStats::default(),
            #[cfg(not(feature = "sim"))]
            deadline: GATEWAY_DEADLINE,
            #[cfg(feature = "sim")]
            deadline: crate::oltp_runtime::sim::limits()
                .map_or(GATEWAY_DEADLINE, |limits| limits.direct_deadline),
        })
    }
    /// As `new` with another deadline (tests).
    #[cfg(test)]
    pub(crate) fn with_deadline(self: Arc<Self>, deadline: Duration) -> Arc<Self> {
        let mut gateway = Arc::try_unwrap(self).unwrap_or_else(|_| panic!("unshared gateway"));
        gateway.deadline =
            deadline.min(crate::oltp_replication::service_rpc::direct::MAX_DIRECT_DEADLINE);
        Arc::new(gateway)
    }
    pub(crate) fn mode(&self) -> DirectMode {
        self.switch.mode()
    }
    pub(crate) fn stats(&self) -> GatewayStatsSnapshot {
        self.stats.snapshot(&self.cache)
    }
    #[cfg(test)]
    pub(crate) fn cache(&self) -> &TopologyCache {
        &self.cache
    }

    /// Routes one request body. `asynchronous` is `POST /v1/requests` (and
    /// the queue owner's own check, T7c): never direct, but its eligible
    /// mutations are settled at their anchor while the capability is
    /// active, like any other mutation that goes queued. A get that does not
    /// go direct is queued unsettled: no forward, no Data entry (user
    /// decision 2026-11-21, RV-FINAL M3). An error is the request's final
    /// answer.
    pub(crate) async fn route(
        &self,
        body: &[u8],
        asynchronous: bool,
    ) -> Result<Routed, ProtocolError> {
        let mode = self.switch.mode();
        if mode == DirectMode::Off {
            return Ok(Routed::Queued);
        }
        // Anything the scan does not recognize is not eligible, or is
        // malformed: the queued path gives the exact answer, and a body that
        // does not parse can never have committed directly.
        let Some(scanned) = scan(body) else {
            return Ok(Routed::Queued);
        };
        let get = scanned.op == Op::Get;
        GatewayStats::bump(&self.stats.requests);
        if get && (asynchronous || mode == DirectMode::ForwardOnly) {
            return Ok(self.queued());
        }
        let deadline = nabedra_rt::time::now() + self.deadline;
        let point = match scanned.op {
            Op::Get | Op::Delete | Op::Update => match point_key(&scanned) {
                Some(key) => Some(key),
                None => return Ok(self.queued()),
            },
            _ => None,
        };
        let descriptor = match self
            .table(scanned.table_id, point.as_ref(), None, deadline)
            .await
        {
            Ok(Some(table)) => table,
            Ok(None) => return Ok(self.queued()),
            Err(_) if get => return Ok(self.queued()),
            Err(error) => return Err(unsettled(error)),
        };
        let keys = match point {
            Some(key) if key.validate(descriptor.key_type).is_ok() => RouteKeys::single(key),
            Some(_) => return Ok(self.queued()),
            None => match row_keys(&scanned, &descriptor) {
                Some(keys) => keys,
                None => return Ok(self.queued()),
            },
        };
        let RouteKeys { anchor, last } = keys;
        let table = if descriptor.covers(Some(&anchor)) {
            descriptor
        } else {
            match self
                .table(scanned.table_id, Some(&anchor), None, deadline)
                .await
            {
                Ok(Some(table)) if table.schema_epoch == descriptor.schema_epoch => table,
                Ok(_) if get => return Err(route_unavailable()),
                Ok(_) => return Err(unsettled(route_unavailable())),
                Err(error) if get => return Err(error),
                Err(error) => return Err(unsettled(error)),
            }
        };
        let mut request = DirectRequest {
            database: self.database.clone(),
            table_id: scanned.table_id,
            schema_epoch: table.schema_epoch,
            routing_epoch: 1,
            group: Uuid::nil(),
            request_id: scanned.request_id,
            digest: digest(body),
            body: self
                .shared
                .copy_bytes(body)
                .map_err(|error| if get { error } else { unsettled(error) })?,
            deadline: self.deadline,
        };
        let mut uncertain = false;
        if !get && (mode != DirectMode::Direct || asynchronous) {
            return self
                .settle(&table, &anchor, &mut request, deadline, &mut uncertain)
                .await;
        }
        if scanned.op == Op::InsertMany
            && (!table.range.shard.contains_key(&last) || body.len() > MAX_DIRECT_BATCH_BODY)
        {
            GatewayStats::bump(&self.stats.fallbacks);
            return self
                .settle(&table, &anchor, &mut request, deadline, &mut uncertain)
                .await;
        }
        let batch = (scanned.op == Op::InsertMany).then_some(&last);
        match self
            .call(
                &anchor,
                &mut request,
                deadline,
                &mut uncertain,
                RouteAttempt {
                    table: table.clone(),
                    forward: false,
                    last: batch,
                },
            )
            .await
        {
            Ok(Terminal::Done(data)) => {
                GatewayStats::bump(&self.stats.direct_done);
                Ok(Routed::Done(response::encode_success(
                    request.request_id,
                    &data.0,
                    !get,
                )))
            }
            // RV-FINAL L2: after an uncertain attempt, a definite failure
            // may come from checks the leader runs before its dedup lookup
            // (the table and its descriptor), for a request that committed.
            // Settle the id at the anchor instead: a dedup record replays the
            // result, otherwise a marker sends it queued, which answers it.
            // A conflict comes after the lookup and stands.
            // A get committed nothing: its failure stands.
            Ok(Terminal::Failed(error))
                if uncertain && !get && error.code != ErrorCode::Conflict =>
            {
                self.settle(&table, &anchor, &mut request, deadline, &mut uncertain)
                    .await
            }
            Ok(Terminal::Failed(error)) => Err(error),
            // The marker is already in place.
            Ok(Terminal::Fallback(FallbackReason::Forwarded)) => {
                GatewayStats::bump(&self.stats.fallbacks);
                Ok(self.queued())
            }
            // A get that falls back (an open intent, a split) is queued as
            // is: a read has no exactly-once identity to settle (RV-FINAL M3).
            Ok(Terminal::Fallback(_)) if get => {
                GatewayStats::bump(&self.stats.fallbacks);
                Ok(self.queued())
            }
            Ok(Terminal::Fallback(_)) => {
                GatewayStats::bump(&self.stats.fallbacks);
                self.settle(&table, &anchor, &mut request, deadline, &mut uncertain)
                    .await
            }
            Err(error) if get && error.outcome == Outcome::Unknown => Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "direct read result unavailable",
                Outcome::NotApplied,
            )),
            Err(error) if get => Err(self.final_error(error)),
            Err(error) => Err(self.final_error(unsettled(error))),
        }
    }

    fn queued(&self) -> Routed {
        GatewayStats::bump(&self.stats.queued);
        Routed::Queued
    }

    fn final_error(&self, error: ProtocolError) -> ProtocolError {
        if error.outcome == Outcome::Unknown {
            GatewayStats::bump(&self.stats.unknown);
        } else if error.code == ErrorCode::Overloaded {
            GatewayStats::bump(&self.stats.overloaded);
        }
        error
    }

    /// The addendum's rule: commit `DirectForward` at the anchor's range
    /// leader before the request goes queued. `Replayed` returns the direct
    /// result; an unreachable anchor is `outcome_unknown`.
    async fn settle(
        &self,
        route: &Arc<TableRoute>,
        anchor: &Key,
        request: &mut DirectRequest,
        deadline: Instant,
        uncertain: &mut bool,
    ) -> Result<Routed, ProtocolError> {
        GatewayStats::bump(&self.stats.forwards);
        let mut delay = Duration::ZERO;
        loop {
            let mut current = self
                .cache
                .get(route.table_id, Some(anchor))
                .unwrap_or_else(|| route.clone());
            if current.refresh_before_settle.swap(false, Ordering::Relaxed)
                && let Ok(Some(fresh)) = self.refresh(&current, anchor, deadline).await
            {
                current = fresh;
            }
            match self
                .call(
                    anchor,
                    request,
                    deadline,
                    uncertain,
                    RouteAttempt {
                        table: current.clone(),
                        forward: true,
                        last: None,
                    },
                )
                .await
            {
                Ok(Terminal::Done(data)) => {
                    GatewayStats::bump(&self.stats.forward_replayed);
                    return Ok(Routed::Done(response::encode_success(
                        request.request_id,
                        &data.0,
                        true,
                    )));
                }
                // A marker is in place, or the request can never commit
                // directly (an empty batch, too many rows): go queued.
                Ok(Terminal::Fallback(
                    FallbackReason::Forwarded
                    | FallbackReason::Ineligible
                    | FallbackReason::ResourceLimit,
                )) => return Ok(self.queued()),
                // No marker could be written (a leader whose capability flag
                // lags, or a transient state): the request id is not settled,
                // so wait for one within the deadline.
                Ok(Terminal::Fallback(_)) => {
                    if !pause(deadline, &mut delay).await {
                        return Err(self.final_error(unknown()));
                    }
                }
                // RV-FINAL L2: the id may have committed directly and the
                // anchor could not settle it, so the outcome is unknown.
                Ok(Terminal::Failed(error)) if *uncertain && error.code != ErrorCode::Conflict => {
                    return Err(self.final_error(unknown()));
                }
                Ok(Terminal::Failed(error)) => return Err(self.final_error(error)),
                // OD-3: a refusal before the anchor answered (a full leader,
                // the deadline) proves nothing about an earlier call of this
                // id, so the identity's outcome is unknown.
                Err(error)
                    if error.code == ErrorCode::Overloaded
                        && error.outcome == Outcome::NotApplied =>
                {
                    return Err(self.final_error(unsettled(error)));
                }
                // The anchor cannot be reached: the request may have run
                // directly before, so it must not go queued.
                Err(_) => {
                    // A retired parent cannot answer StaleRoute. Refresh
                    // for the next settlement while keeping warm routes
                    // usable by direct requests during a queue outage.
                    current.refresh_before_settle.store(true, Ordering::Relaxed);
                    return Err(self.final_error(unknown()));
                }
            }
        }
    }

    /// Sends `request` to the leader of the range holding `key` until it
    /// answers `Done`, `Fallback` or a definite failure, following
    /// `NotLeader` and `StaleRoute` and resending after a lost reply. An
    /// `Err` is the deadline (`outcome_unknown` once an attempt may have
    /// executed) or a typed refusal.
    async fn call(
        &self,
        key: &Key,
        request: &mut DirectRequest,
        deadline: Instant,
        uncertain: &mut bool,
        attempt: RouteAttempt<'_>,
    ) -> Result<Terminal, ProtocolError> {
        let RouteAttempt {
            mut table,
            forward,
            last,
        } = attempt;
        let mut delay = Duration::ZERO;
        let mut rotation = 0usize;
        let mut attempts = FuturesUnordered::new();
        let mut pending = [None; 7];
        let mut attempted_route: Option<(_, Arc<[Voter]>)> = None;
        let mut next_probe = nabedra_rt::time::now();
        let mut refreshed_unknown_table = false;
        loop {
            if nabedra_rt::time::now() >= deadline {
                return Err(deadline_error(&table, forward, *uncertain));
            }
            if last.is_some_and(|last| !table.range.shard.contains_key(last)) {
                return Ok(Terminal::Fallback(FallbackReason::Ineligible));
            }
            let range = &table.range;
            let voters = match self.voters(range, false, deadline).await {
                Ok(voters) if !voters.is_empty() => voters,
                _ => {
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(&table, forward, *uncertain));
                    }
                    continue;
                }
            };
            let route = (
                range.group().group_id,
                range.shard.routing_epoch,
                table.schema_epoch,
            );
            if attempted_route
                .as_ref()
                .is_none_or(|(prior, prior_voters)| {
                    *prior != route || !Arc::ptr_eq(prior_voters, &voters)
                })
            {
                attempts.clear();
                pending.fill(None);
                attempted_route = Some((route, voters.clone()));
            }
            let now = nabedra_rt::time::now();
            let waiting_for_probe = now < next_probe && !attempts.is_empty();
            let available = |voter: &Voter| !pending.contains(&Some(voter.node_id));
            let next = if waiting_for_probe {
                None
            } else {
                range
                    .leader_hint()
                    .and_then(|node| {
                        voters
                            .iter()
                            .enumerate()
                            .find(|(_, voter)| voter.node_id == node && available(voter))
                    })
                    .or_else(|| {
                        let first = rotation % voters.len();
                        voters
                            .iter()
                            .enumerate()
                            .cycle()
                            .skip(first)
                            .take(voters.len())
                            .find(|(_, voter)| available(voter))
                    })
            };
            request.schema_epoch = table.schema_epoch;
            request.routing_epoch = range.shard.routing_epoch;
            request.group = range.group().group_id;
            let attempt = if let Some((index, voter)) = next {
                let voter = *voter;
                rotation = index + 1;
                // A connected leader may still be waiting for Timestamp or
                // Raft. Keep its waiter while trying other voters, and allow
                // only one in-flight execution per voter for this request.
                let left = deadline.saturating_duration_since(now);
                request.deadline = left
                    .saturating_sub(LEADER_MARGIN)
                    .max(left / 2)
                    .max(Duration::from_millis(1));
                let sent_request = request.clone();
                let slot = pending
                    .iter()
                    .position(Option::is_none)
                    .expect("bounded voters");
                pending[slot] = Some(voter.node_id);
                #[cfg(feature = "sim")]
                nabedra_oltp_types::sim_probe::emit(|| nabedra_oltp_types::sim_probe::Probe::GatewayAttempt {
                    request_id: request.request_id,
                    group: request.group,
                    voter: voter.node_id,
                    remaining_us: u64::try_from(left.as_micros()).unwrap_or(u64::MAX),
                });
                let group = range.clone();
                attempts.push(async move {
                    let reply = self
                        .transport
                        .send(group.group(), voter, &sent_request, forward)
                        .await;
                    (slot, voter, reply)
                });
                if voters
                    .iter()
                    .any(|voter| !pending.contains(&Some(voter.node_id)))
                {
                    deadline.min(now + PEER_ATTEMPT)
                } else {
                    deadline
                }
            } else if waiting_for_probe {
                deadline.min(next_probe)
            } else {
                deadline
            };
            let (slot, voter, reply) = match nabedra_rt::time::timeout_at(attempt, attempts.next()).await
            {
                Ok(Some(settled)) => settled,
                Ok(None) => return Err(deadline_error(&table, forward, *uncertain)),
                Err(_) if waiting_for_probe && attempt < deadline => continue,
                Err(_) => {
                    *uncertain = true;
                    GatewayStats::bump(&self.stats.peer_timeouts);
                    if attempt == deadline {
                        return Err(deadline_error(&table, forward, true));
                    }
                    GatewayStats::bump(&self.stats.resends);
                    if let Some((_, voter)) = next {
                        range.forget_leader(voter.node_id);
                    }
                    continue;
                }
            };
            pending[slot] = None;
            #[cfg(test)]
            if std::env::var_os("T6_TRACE").is_some() {
                eprintln!(
                    "t6 {} forward={forward} node={} group={} epochs={}/{}: {reply:?}",
                    request.request_id,
                    voter.node_id,
                    range.group().group_id,
                    request.schema_epoch,
                    request.routing_epoch
                );
            }
            match reply {
                Ok(DirectResponse::Done(data)) => {
                    range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                    return Ok(Terminal::Done(data));
                }
                Ok(DirectResponse::Fallback { reason }) => {
                    range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                    // The Catalog may now name the children of this frozen source.
                    if forward
                        && reason == FallbackReason::SplitInProgress
                        && let Ok(Some(fresh)) = self.refresh(&table, key, deadline).await
                        && fresh.range.shard != range.shard
                    {
                        table = fresh;
                        continue;
                    }
                    return Ok(Terminal::Fallback(reason));
                }
                Ok(DirectResponse::Failed(error)) => {
                    if error.outcome == Outcome::Unknown {
                        // Not settled yet (a proposal failed in an election):
                        // resend the same id and body; the dedup record
                        // decides.
                        *uncertain = true;
                        GatewayStats::bump(&self.stats.resends);
                        range.forget_leader(voter.node_id);
                        if error.code == ErrorCode::OutcomeUnknown {
                            // The old leader spent its execution budget.
                            // Check the dedup result while time remains.
                            delay = Duration::ZERO;
                        } else if !pause(deadline, &mut delay).await {
                            return Err(deadline_error(&table, forward, true));
                        }
                        continue;
                    }
                    if error.code == ErrorCode::NotFound && !refreshed_unknown_table {
                        // `UnknownTable`: the table may have been recreated.
                        refreshed_unknown_table = true;
                        match self.refresh(&table, key, deadline).await {
                            Ok(Some(fresh)) => {
                                table = fresh;
                                continue;
                            }
                            Ok(None) => return Ok(Terminal::Failed(error)),
                            Err(_) => {}
                        }
                    }
                    if error.code == ErrorCode::Overloaded {
                        // Typed 429, refused before execution. Pass it
                        // through unless an earlier attempt may have run.
                        if !*uncertain {
                            return Err(error);
                        }
                    } else if error.code != ErrorCode::StorageUnavailable
                        || crate::oltp::pool::is_disk_reservation_refusal(&error)
                    {
                        range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                        return Ok(Terminal::Failed(error));
                    }
                    // Not applied and transient: retry within the deadline.
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(&table, forward, *uncertain));
                    }
                }
                Ok(DirectResponse::StaleRoute { .. }) => {
                    GatewayStats::bump(&self.stats.stale_routes);
                    let changed = match self.refresh(&table, key, deadline).await {
                        Ok(Some(fresh)) => {
                            let changed = fresh.range.shard != range.shard;
                            table = fresh;
                            changed
                        }
                        Ok(None) => {
                            return Ok(Terminal::Failed(ProtocolError::new(
                                ErrorCode::NotFound,
                                "table or shard not found",
                                Outcome::NotApplied,
                            )));
                        }
                        Err(_) => false,
                    };
                    if !changed && !pause(deadline, &mut delay).await {
                        return Err(deadline_error(&table, forward, *uncertain));
                    }
                }
                Ok(DirectResponse::NotLeader { hint }) => {
                    GatewayStats::bump(&self.stats.not_leader);
                    match hint
                        .filter(|node| *node != voter.node_id && !pending.contains(&Some(*node)))
                    {
                        Some(node) if voters.iter().any(|voter| voter.node_id == node) => {
                            range.leader_hint.store(node, Ordering::Relaxed);
                        }
                        Some(_) => {
                            // A leader outside the cached placement: the
                            // replicas moved.
                            range.leader_hint.store(0, Ordering::Relaxed);
                            let _ = self.voters(range, true, deadline).await;
                        }
                        None => {
                            range.forget_leader(voter.node_id);
                            // Back off the next probe, while still polling a
                            // leader that may be finishing the request.
                            if !attempts.is_empty() {
                                delay = (delay * 2).clamp(FIRST_PAUSE, MAX_PAUSE);
                                next_probe = nabedra_rt::time::now() + delay;
                            } else if !pause(deadline, &mut delay).await {
                                return Err(deadline_error(&table, forward, *uncertain));
                            }
                        }
                    }
                }
                Err(DirectSendError::NotApplied(error)) => {
                    if error.code == ErrorCode::Unauthorized {
                        return Err(if *uncertain { unknown() } else { error });
                    }
                    if error.code == ErrorCode::Overloaded && !*uncertain {
                        return Err(error);
                    }
                    range.forget_leader(voter.node_id);
                    if error.code == ErrorCode::ValidationError {
                        // The receiver refused this replica identity: read
                        // the placement again.
                        let _ = self.voters(range, true, deadline).await;
                    }
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(&table, forward, *uncertain));
                    }
                }
                Err(DirectSendError::Uncertain(_)) => {
                    // A lost reply: resend the same id and body. The dedup
                    // record decides whether the first attempt committed.
                    *uncertain = true;
                    GatewayStats::bump(&self.stats.resends);
                    range.forget_leader(voter.node_id);
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(&table, forward, true));
                    }
                }
            }
        }
    }

    /// A miss or refresh reads only the requested range. Another miss on
    /// the same span can reuse the result even if it names a different key.
    async fn table(
        &self,
        table_id: Uuid,
        key: Option<&Key>,
        seen: Option<&Arc<TableRoute>>,
        deadline: Instant,
    ) -> Result<Option<Arc<TableRoute>>, ProtocolError> {
        let fresh = |current: &Arc<_>| seen.is_none_or(|old| !Arc::ptr_eq(old, current));
        if let Some(current) = self.cache.get(table_id, key).filter(fresh) {
            return Ok(Some(current));
        }
        let _fill = nabedra_rt::time::timeout_at(deadline, self.fill.lock())
            .await
            .map_err(|_| route_unavailable())?;
        if let Some(current) = self.cache.get(table_id, key).filter(fresh) {
            return Ok(Some(current));
        }
        GatewayStats::bump(&self.stats.catalog_reads);
        let fill = nabedra_rt::time::timeout_at(deadline, self.source.point(table_id, key))
            .await
            .map_err(|_| route_unavailable())??;
        let Some(fill) = fill else {
            self.cache.remove(table_id);
            return Ok(None);
        };
        let route = Arc::new(self.build(table_id, key, fill, seen.map(Arc::as_ref))?);
        self.cache.insert(route.clone());
        Ok(Some(route))
    }

    async fn refresh(
        &self,
        table: &Arc<TableRoute>,
        key: &Key,
        deadline: Instant,
    ) -> Result<Option<Arc<TableRoute>>, ProtocolError> {
        self.table(table.table_id, Some(key), Some(table), deadline)
            .await
    }

    fn build(
        &self,
        table_id: Uuid,
        key: Option<&Key>,
        fill: PointFill,
        previous: Option<&TableRoute>,
    ) -> Result<TableRoute, ProtocolError> {
        let shard = &fill.shard;
        if fill.table_id != table_id
            || shard.group.database != self.database
            || shard.group.validate().is_err()
            || !matches!(shard.group.kind, GroupKind::Data { table_id: id, .. } if id == table_id)
            || shard.routing_epoch == 0
            || shard.schema_epoch != fill.schema_epoch
            || fill.schema_epoch == 0
            || !matches!(fill.key_type, ColumnType::Int64 | ColumnType::Utf8)
            || [&shard.span.start, &shard.span.end]
                .into_iter()
                .flatten()
                .any(|bound| bound.validate(fill.key_type).is_err())
            || shard
                .span
                .start
                .as_ref()
                .zip(shard.span.end.as_ref())
                .is_some_and(|(a, b)| !compare_primary(a, b).is_lt())
            || key
                .is_some_and(|key| key.validate(fill.key_type).is_ok() && !shard.contains_key(key))
        {
            return Err(route_unavailable());
        }
        let bytes = TableRoute::heap_bytes(&fill);
        let charge = self.shared.reserve_scratch(
            if bytes <= CACHE_BYTES {
                ByteClass::Progress
            } else {
                ByteClass::Data
            },
            bytes,
        )?;
        let range = previous
            .filter(|old| old.range.shard == fill.shard)
            .map(|old| old.range.clone())
            .unwrap_or_else(|| Arc::new(RangeRoute::new(fill.shard)));
        Ok(TableRoute {
            table_id,
            key_column: fill.key_column,
            key_type: fill.key_type,
            schema_epoch: fill.schema_epoch,
            range,
            unavailable: AtomicBool::new(false),
            refresh_before_settle: AtomicBool::new(false),
            _charge: charge,
        })
    }

    async fn voters(
        &self,
        range: &RangeRoute,
        refresh: bool,
        deadline: Instant,
    ) -> Result<Arc<[Voter]>, ProtocolError> {
        if !refresh && let Some(voters) = range.cached_voters() {
            return Ok(voters);
        }
        GatewayStats::bump(&self.stats.placement_reads);
        let mut voters =
            nabedra_rt::time::timeout_at(deadline, self.source.placement(range.group(), refresh))
                .await
                .map_err(|_| route_unavailable())??;
        voters.sort_unstable_by_key(|voter| voter.node_id);
        voters.dedup_by_key(|voter| voter.node_id);
        if voters.is_empty() || voters.len() > 7 {
            return Err(route_unavailable());
        }
        let voters: Arc<[Voter]> = voters.into();
        *range.voters.lock().unwrap_or_else(|p| p.into_inner()) = Some(voters.clone());
        Ok(voters)
    }
}

/// `Method::Direct` over the worker transport's persistent sessions.
pub(crate) struct WireTransport {
    shared: Arc<WorkerSharedResources>,
    transport: crate::oltp_replication::multiplex::WorkerClient,
    control: Arc<crate::oltp_replication::authority::control::ControlClient>,
    action: crate::oltp_replication::authority::control::ServiceAction,
}
impl WireTransport {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        transport: crate::oltp_replication::multiplex::WorkerClient,
        control: Arc<crate::oltp_replication::authority::control::ControlClient>,
        action: crate::oltp_replication::authority::control::ServiceAction,
    ) -> Self {
        Self {
            shared,
            transport,
            control,
            action,
        }
    }
}
impl DirectTransport for WireTransport {
    fn send<'a>(
        &'a self,
        group: &'a GroupIdentity,
        voter: Voter,
        request: &'a DirectRequest,
        forward: bool,
    ) -> SendFuture<'a> {
        Box::pin(async move {
            let link = crate::oltp_replication::service_rpc::direct::DirectLink::new(
                group.clone(),
                self.shared.clone(),
                self.transport.clone(),
                self.control.clone(),
                self.action,
            )
            .map_err(DirectSendError::NotApplied)?;
            link.send(voter.node_id, voter.worker_id, request, forward)
                .await
        })
    }
}

/// Control's replica placement for a group, through the caller's client.
pub(crate) async fn control_placement(
    control: &crate::oltp_replication::authority::control::ControlClient,
    group: &GroupIdentity,
    refresh: bool,
) -> Result<Vec<Voter>, ProtocolError> {
    if refresh && let Some(cached) = control.cached_replica_routes(group) {
        control.invalidate_replica_routes(&cached);
    }
    let routes = control.replica_routes(group).await?;
    Ok(routes
        .members
        .iter()
        .map(|member| Voter {
            node_id: member.node_id,
            worker_id: member.worker_id,
        })
        .collect())
}

/// The API process reads Catalog through the authenticated application
/// endpoint and placement through Control.
pub(crate) struct ApiTopology {
    pub client: Arc<super::ApplicationClient>,
    pub control: Arc<crate::oltp_replication::authority::control::ControlClient>,
}
impl TopologySource for ApiTopology {
    fn point<'a>(
        &'a self,
        table_id: Uuid,
        key: Option<&'a Key>,
    ) -> EndpointFuture<'a, Option<PointFill>> {
        Box::pin(self.client.direct_point(table_id, key))
    }
    fn placement<'a>(
        &'a self,
        group: &'a GroupIdentity,
        refresh: bool,
    ) -> EndpointFuture<'a, Vec<Voter>> {
        Box::pin(control_placement(&self.control, group, refresh))
    }
}

struct CatalogHeader {
    pin: PinnedCatalog,
    table: OwnedRecord<TableRecord>,
    root: OwnedRecord<RoutingRoot>,
    key_type: ColumnType,
}

async fn catalog_header(
    catalog: &CatalogHandle,
    authority: Authority,
    table_id: Uuid,
) -> Result<Option<CatalogHeader>, ProtocolError> {
    let latest = ReadTs::new(u64::MAX).map_err(|_| route_unavailable())?;
    let pin = catalog.snapshot(latest, authority)?;
    let table = match catalog.describe(&pin, table_id).await? {
        CatalogRead::Blocked(_) => return Err(route_unavailable()),
        CatalogRead::Value { record: None, .. } => return Ok(None),
        CatalogRead::Value {
            record: Some(record),
            ..
        } => record,
    };
    let key_type = *table
        .descriptor
        .schema
        .get(&table.descriptor.key)
        .ok_or_else(route_unavailable)?;
    let root = match catalog.routing_root(&pin, table_id).await? {
        CatalogRead::Value {
            record: Some(root), ..
        } if root.schema_epoch == table.schema_epoch
            && root.routing_epoch == table.routing_epoch =>
        {
            root
        }
        _ => return Err(route_unavailable()),
    };
    if table.descriptor.id != table_id || !matches!(key_type, ColumnType::Int64 | ColumnType::Utf8)
    {
        return Err(route_unavailable());
    }
    Ok(Some(CatalogHeader {
        pin,
        table,
        root,
        key_type,
    }))
}

fn check_catalog_shard(
    pin: &PinnedCatalog,
    table: &TableRecord,
    shard: &ShardRoute,
) -> Result<(), ProtocolError> {
    if shard.group.cluster_id != pin.catalog.cluster_id
        || shard.group.database != pin.catalog.database
        || shard.group.validate().is_err()
        || !matches!(shard.group.kind, GroupKind::Data { table_id, .. } if table_id == table.descriptor.id)
        || shard.schema_epoch != table.schema_epoch
        || shard.routing_epoch == 0
        || shard.routing_epoch > table.routing_epoch
    {
        return Err(route_unavailable());
    }
    Ok(())
}

/// Traverses authenticated, bounded Catalog records and retains one page.
/// The newest view can race publication. Check that the descriptor, routing
/// root and selected shard agree before returning a hint to the gateway.
pub(crate) async fn catalog_point(
    catalog: &CatalogHandle,
    authority: Authority,
    table_id: Uuid,
    key: Option<&Key>,
) -> Result<Option<PointFill>, ProtocolError> {
    let Some(CatalogHeader {
        pin,
        table,
        root,
        key_type,
    }) = catalog_header(catalog, authority, table_id).await?
    else {
        return Ok(None);
    };
    let (page, index) = match key.filter(|key| key.validate(key_type).is_ok()) {
        Some(key) => match catalog
            .route_pages(&pin, table_id, key, root.first_page)
            .await?
        {
            RouteRead::Route { page, index } => (page, index),
            _ => return Err(route_unavailable()),
        },
        None => {
            let first = root.first_page.ok_or_else(route_unavailable)?;
            match catalog.routing_page(&pin, table_id, first).await? {
                CatalogRead::Value {
                    record: Some(page), ..
                } => (page, 0),
                _ => return Err(route_unavailable()),
            }
        }
    };
    let shard = page.shards.get(index).ok_or_else(route_unavailable)?;
    check_catalog_shard(&pin, &table, shard)?;
    Ok(Some(PointFill {
        table_id,
        key_column: table.descriptor.key.clone(),
        key_type,
        schema_epoch: table.schema_epoch,
        shard: shard.clone(),
    }))
}

/// Legacy application read for older gateways. Bound the encoded reply
/// while collecting: the legacy lane cannot carry large tables.
pub(crate) async fn catalog_fill(
    catalog: &CatalogHandle,
    authority: Authority,
    table_id: Uuid,
) -> Result<Option<TableFill>, ProtocolError> {
    let Some(CatalogHeader {
        pin,
        table,
        root,
        key_type,
    }) = catalog_header(catalog, authority, table_id).await?
    else {
        return Ok(None);
    };
    let mut fill = TableFill {
        table_id,
        key_column: table.descriptor.key.clone(),
        key_type,
        schema_epoch: table.schema_epoch,
        shards: Vec::new(),
    };
    let mut remaining = ROUTE_REPLY_BYTES - encoded_length(&fill, ROUTE_REPLY_BYTES)?;
    let mut next = root.first_page;
    while let Some(page_id) = next {
        let page = match catalog.routing_page(&pin, table_id, page_id).await? {
            CatalogRead::Value {
                record: Some(page), ..
            } => page,
            _ => return Err(route_unavailable()),
        };
        for shard in page.shards.iter() {
            check_catalog_shard(&pin, &table, shard)?;
            let shard_bytes = encoded_length(shard, remaining).map_err(|_| route_unavailable())?;
            remaining = remaining
                .checked_sub(shard_bytes + 1)
                .ok_or_else(route_unavailable)?;
            fill.shards.push(shard.clone());
        }
        next = page.next_page;
    }
    Ok(Some(fill))
}

/// Waits before the next attempt, doubling from `FIRST_PAUSE` to
/// `MAX_PAUSE`. False when the deadline leaves no time for one.
async fn pause(deadline: Instant, delay: &mut Duration) -> bool {
    *delay = (*delay * 2).clamp(FIRST_PAUSE, MAX_PAUSE);
    if nabedra_rt::time::now() + *delay >= deadline {
        return false;
    }
    nabedra_rt::time::sleep(*delay).await;
    true
}

fn digest(body: &[u8]) -> Digest {
    use sha2::{Digest as _, Sha256};
    Digest::from_bytes(Sha256::digest(body).into())
}

fn deadline_error(route: &TableRoute, forward: bool, uncertain: bool) -> ProtocolError {
    // A retired range may never answer StaleRoute. Let an exact retry
    // refill the range without discarding its primary key descriptor.
    if forward {
        route.refresh_before_settle.store(true, Ordering::Relaxed);
    } else {
        route.unavailable.store(true, Ordering::Relaxed);
    }
    if uncertain {
        unknown()
    } else {
        ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "direct request deadline exceeded before execution",
            Outcome::NotApplied,
        )
    }
}

fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "direct request outcome unknown; retry the same request_id and body",
        Outcome::Unknown,
    )
}

/// OD-3 and OD-7: a mutation identity spans gateway calls, and only its
/// anchor can settle it. A refusal the gateway gives before the anchor
/// answered (the deadline, an unreachable route, a full leader or lane, a
/// latch that timed out) says nothing about an attempt of an earlier call,
/// which may still commit. It is `outcome_unknown`; the client retries the
/// same id and body. A definite answer from the leader, given after its dedup
/// lookup, is not passed through here. Gets keep their refusals.
fn unsettled(error: ProtocolError) -> ProtocolError {
    if error.outcome == Outcome::NotApplied {
        unknown()
    } else {
        error
    }
}

fn route_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "direct route unavailable",
        Outcome::NotApplied,
    )
}

/// Reads the envelope and the routing fields of an eligible operation.
/// Rows are skipped, not decoded.
/// An eligible mutation (`insert`, `insert_many`, `update`, `upsert`,
/// `delete`): the requests a queue owner settles at their anchor before it
/// reserves them while `data.direct.v1` is active (T7c). Gets are eligible
/// for the direct path but never settled (user decision 2026-11-21,
/// RV-FINAL M3). A body this scan does not recognize never runs directly.
pub(crate) fn settled_before_queue(body: &[u8]) -> bool {
    scan(body).is_some_and(|scanned| scanned.op != Op::Get)
}

fn scan(body: &[u8]) -> Option<Scanned<'_>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope<'a> {
        request_id: Uuid,
        #[serde(borrow)]
        operation: std::borrow::Cow<'a, str>,
        #[serde(borrow)]
        payload: &'a RawValue,
    }
    #[derive(Deserialize)]
    struct Table {
        table_id: Uuid,
    }
    if body.len() > crate::protocol::MAX_BODY_BYTES {
        return None;
    }
    let envelope: Envelope<'_> = serde_json::from_slice(body).ok()?;
    let op = Op::parse(&envelope.operation)?;
    // Only the table id: other fields are checked against the route.
    let Table { table_id } = serde_json::from_str(envelope.payload.get()).ok()?;
    Some(Scanned {
        request_id: envelope.request_id,
        op,
        table_id,
        payload: envelope.payload,
    })
}

/// A batch is eligible when both extrema lie in the anchor's range.
struct RouteKeys {
    anchor: Key,
    last: Key,
}
impl RouteKeys {
    fn single(key: Key) -> Self {
        Self {
            anchor: key.clone(),
            last: key,
        }
    }
}

fn point_key(scanned: &Scanned<'_>) -> Option<Key> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Point<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        key: Key,
        #[serde(borrow, default)]
        values: Option<&'a RawValue>,
    }
    let point: Point<'_> = serde_json::from_str(scanned.payload.get()).ok()?;
    ((scanned.op == Op::Update) == point.values.is_some()).then_some(point.key)
}

/// Visit one row at a time, retaining only the smallest and largest keys.
fn row_keys(scanned: &Scanned<'_>, table: &TableRoute) -> Option<RouteKeys> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct One<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        #[serde(borrow)]
        row: &'a RawValue,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Many<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        #[serde(borrow)]
        rows: &'a RawValue,
    }
    let raw = scanned.payload.get();
    if scanned.op == Op::InsertMany {
        let many: Many<'_> = serde_json::from_str(raw).ok()?;
        let mut decoder = serde_json::Deserializer::from_str(many.rows.get());
        let keys = serde::Deserializer::deserialize_seq(&mut decoder, Rows(table)).ok()??;
        decoder.end().ok()?;
        Some(keys)
    } else {
        let one: One<'_> = serde_json::from_str(raw).ok()?;
        route_row_key(one.row.get(), table).map(RouteKeys::single)
    }
}

fn route_row_key(raw: &str, table: &TableRoute) -> Option<Key> {
    row_key(raw, &table.key_column).filter(|key| key.validate(table.key_type).is_ok())
}

struct Rows<'t>(&'t TableRoute);
impl<'de> serde::de::Visitor<'de> for Rows<'_> {
    type Value = Option<RouteKeys>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an array of rows")
    }
    fn visit_seq<S: serde::de::SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
        let mut keys: Option<RouteKeys> = None;
        while let Some(raw) = seq.next_element::<&'de RawValue>()? {
            let Some(key) = route_row_key(raw.get(), self.0) else {
                return Ok(None);
            };
            match &mut keys {
                None => keys = Some(RouteKeys::single(key)),
                Some(keys) if compare_primary(&key, &keys.anchor).is_lt() => keys.anchor = key,
                Some(keys) if compare_primary(&key, &keys.last).is_gt() => keys.last = key,
                Some(_) => {}
            }
        }
        Ok(keys)
    }
}

/// The value of `column` in one row object, skipping the other columns.
fn row_key(row: &str, column: &str) -> Option<Key> {
    use serde::de::{self, IgnoredAny, MapAccess, Visitor};
    struct Find<'c>(&'c str);
    impl<'de> Visitor<'de> for Find<'_> {
        type Value = Option<Key>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a row object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut found = None;
            while let Some(name) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                if name == self.0 {
                    if found.is_some() {
                        return Err(de::Error::custom("duplicate key column"));
                    }
                    found = Some(map.next_value::<Key>()?);
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(found)
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(row);
    let key = serde::Deserializer::deserialize_map(&mut decoder, Find(column)).ok()?;
    decoder.end().ok()?;
    key
}

#[cfg(test)]
mod tests;
