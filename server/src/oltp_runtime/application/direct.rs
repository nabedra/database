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
//!   Catalog read (descriptor key, schema epoch, routing pages) and, per
//!   range, one Control placement read. Only `StaleRoute`, `NotLeader` and
//!   an unknown table refresh it, so the steady state makes no Control or
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
        mvcc::{MvccKey, catalog::ShardRoute, store::compare_keys},
        pool::{ByteClass, ScratchLease, WorkerSharedResources},
    },
    oltp_replication::{
        multiplex::EndpointFuture,
        service_rpc::direct::{
            DirectData, DirectRequest, DirectResponse, DirectSendError, FallbackReason,
        },
        types::{Digest, GroupIdentity, GroupKind, Text},
    },
    protocol::{ColumnType, ErrorCode, Key, Outcome, ProtocolError},
    response::{self, EncodedResponse},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
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
/// Tables kept in the cache.
const CACHE_TABLES: usize = 256;
/// Ranges one table route may hold. A larger table takes the queued path
/// for reads and refuses direct-era mutations it cannot settle.
const MAX_RANGES: usize = 1024;
/// The leader gets this much less time than the gateway, so its own
/// deadline answer arrives before the gateway gives up.
const LEADER_MARGIN: Duration = Duration::from_millis(100);
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
    fn cached_voters(&self) -> Option<Arc<[Voter]>> {
        self.voters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// A table's routing (plan section 3).
pub(crate) struct TableRoute {
    pub table_id: Uuid,
    pub key_column: String,
    pub key_type: ColumnType,
    pub schema_epoch: u64,
    pub ranges: Vec<Arc<RangeRoute>>,
    _charge: ScratchLease,
}
impl TableRoute {
    fn range_for(&self, key: &Key) -> Option<&Arc<RangeRoute>> {
        self.ranges
            .iter()
            .find(|range| range.shard.contains_key(key))
    }
    fn heap_bytes(fill: &TableFill) -> usize {
        let key = |key: &Option<Key>| match key {
            Some(Key::Utf8(text)) => text.capacity(),
            _ => 0,
        };
        fill.key_column.capacity()
            + fill
                .shards
                .iter()
                .map(|shard| {
                    std::mem::size_of::<RangeRoute>()
                        + 3 * std::mem::size_of::<Voter>()
                        + shard.group.database.capacity_bytes()
                        + key(&shard.span.start)
                        + key(&shard.span.end)
                })
                .sum::<usize>()
    }
}

/// Bounded LRU of table routes (plan section 3).
pub(crate) struct TopologyCache {
    capacity: usize,
    inner: Mutex<Lru>,
}
#[derive(Default)]
struct Lru {
    tick: u64,
    entries: HashMap<Uuid, (Arc<TableRoute>, u64)>,
}
impl TopologyCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::default(),
        }
    }
    pub(crate) fn get(&self, table_id: Uuid) -> Option<Arc<TableRoute>> {
        let mut lru = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        lru.tick += 1;
        let tick = lru.tick;
        let (route, used) = lru.entries.get_mut(&table_id)?;
        *used = tick;
        Some(route.clone())
    }
    fn insert(&self, route: Arc<TableRoute>) {
        let mut lru = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        lru.tick += 1;
        let tick = lru.tick;
        lru.entries.insert(route.table_id, (route, tick));
        while lru.entries.len() > self.capacity {
            let Some(oldest) = lru
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(id, _)| *id)
            else {
                break;
            };
            lru.entries.remove(&oldest);
        }
    }
    fn remove(&self, table_id: Uuid) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .remove(&table_id);
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .len()
    }
}

/// Where routes come from: one Catalog read per table, one Control
/// placement read per range.
pub(crate) trait TopologySource: Send + Sync {
    /// The table's key column, schema epoch and ranges, `None` when the
    /// table does not exist.
    fn table(&self, table_id: Uuid) -> EndpointFuture<'_, Option<TableFill>>;
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
    pub unknown: u64,
    pub overloaded: u64,
}
impl GatewayStats {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn snapshot(&self) -> GatewayStatsSnapshot {
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
            unknown: get(&self.unknown),
            overloaded: get(&self.overloaded),
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
            cache: TopologyCache::new(CACHE_TABLES),
            fill: tokio::sync::Mutex::new(()),
            stats: GatewayStats::default(),
            deadline: GATEWAY_DEADLINE,
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
        self.stats.snapshot()
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
        let deadline = Instant::now() + self.deadline;
        let table = match self.table(scanned.table_id, None, deadline).await {
            Ok(Some(table)) => table,
            // No such table: nothing can have committed directly into it.
            Ok(None) => return Ok(self.queued()),
            // A read needs no settlement.
            Err(_) if get => return Ok(self.queued()),
            Err(error) => return Err(error),
        };
        let Some(RouteKeys {
            first,
            anchor,
            one_range,
        }) = route_keys(&scanned, &table)
        else {
            // A payload whose keys do not match the schema fails validation
            // on either path, so it never committed directly.
            return Ok(self.queued());
        };
        let mut request = DirectRequest {
            database: self.database.clone(),
            table_id: scanned.table_id,
            schema_epoch: table.schema_epoch,
            routing_epoch: 1,
            group: Uuid::nil(),
            request_id: scanned.request_id,
            digest: digest(body),
            body: self.shared.copy_bytes(body)?,
            deadline: self.deadline,
        };
        let mut uncertain = false;
        if !get && (mode != DirectMode::Direct || asynchronous) {
            return self
                .settle(&anchor, &mut request, deadline, &mut uncertain)
                .await;
        }
        if scanned.op == Op::InsertMany && (!one_range || body.len() > MAX_DIRECT_BATCH_BODY) {
            GatewayStats::bump(&self.stats.fallbacks);
            return self
                .settle(&anchor, &mut request, deadline, &mut uncertain)
                .await;
        }
        match self
            .call(&first, &mut request, deadline, false, &mut uncertain)
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
                self.settle(&anchor, &mut request, deadline, &mut uncertain)
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
                self.settle(&anchor, &mut request, deadline, &mut uncertain)
                    .await
            }
            Err(error) if get && error.outcome == Outcome::Unknown => Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "direct read result unavailable",
                Outcome::NotApplied,
            )),
            Err(error) => Err(self.final_error(error)),
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
        anchor: &Key,
        request: &mut DirectRequest,
        deadline: Instant,
        uncertain: &mut bool,
    ) -> Result<Routed, ProtocolError> {
        GatewayStats::bump(&self.stats.forwards);
        let mut delay = Duration::ZERO;
        loop {
            match self.call(anchor, request, deadline, true, uncertain).await {
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
                Err(error)
                    if error.code == ErrorCode::Overloaded
                        && error.outcome == Outcome::NotApplied
                        && !*uncertain =>
                {
                    return Err(self.final_error(error));
                }
                // The anchor cannot be reached: the request may have run
                // directly before, so it must not go queued.
                Err(_) => return Err(self.final_error(unknown())),
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
        forward: bool,
        uncertain: &mut bool,
    ) -> Result<Terminal, ProtocolError> {
        let mut delay = Duration::ZERO;
        let mut rotation = 0usize;
        let mut refreshed_unknown_table = false;
        loop {
            if Instant::now() >= deadline {
                return Err(deadline_error(*uncertain));
            }
            let table = match self.table(request.table_id, None, deadline).await {
                Ok(Some(table)) => table,
                Ok(None) => {
                    return Ok(Terminal::Failed(ProtocolError::new(
                        ErrorCode::NotFound,
                        "table or shard not found",
                        Outcome::NotApplied,
                    )));
                }
                Err(_) => {
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(*uncertain));
                    }
                    continue;
                }
            };
            let Some(range) = table.range_for(key).cloned() else {
                // The cached spans do not cover the key: read them again.
                self.refresh(&table, deadline).await;
                if !pause(deadline, &mut delay).await {
                    return Err(deadline_error(*uncertain));
                }
                continue;
            };
            let voters = match self.voters(&range, false).await {
                Ok(voters) if !voters.is_empty() => voters,
                _ => {
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(*uncertain));
                    }
                    continue;
                }
            };
            let voter = range
                .leader_hint()
                .and_then(|node| voters.iter().find(|voter| voter.node_id == node))
                .copied()
                .unwrap_or_else(|| voters[rotation % voters.len()]);
            request.schema_epoch = table.schema_epoch;
            request.routing_epoch = range.shard.routing_epoch;
            request.group = range.group().group_id;
            let left = deadline.saturating_duration_since(Instant::now());
            request.deadline = left
                .saturating_sub(LEADER_MARGIN)
                .max(left / 2)
                .max(Duration::from_millis(1));
            let sent = tokio::time::timeout_at(
                deadline,
                self.transport.send(range.group(), voter, request, forward),
            )
            .await;
            #[cfg(test)]
            if std::env::var_os("T6_TRACE").is_some() {
                eprintln!(
                    "t6 {} forward={forward} node={} group={} epochs={}/{}: {sent:?}",
                    request.request_id,
                    voter.node_id,
                    range.group().group_id,
                    request.schema_epoch,
                    request.routing_epoch
                );
            }
            let reply = match sent {
                // The gateway's deadline cut the call: it may have executed.
                Err(_) => {
                    *uncertain = true;
                    return Err(deadline_error(true));
                }
                Ok(reply) => reply,
            };
            match reply {
                Ok(DirectResponse::Done(data)) => {
                    range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                    return Ok(Terminal::Done(data));
                }
                Ok(DirectResponse::Fallback { reason }) => {
                    range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                    return Ok(Terminal::Fallback(reason));
                }
                Ok(DirectResponse::Failed(error)) => {
                    if error.code == ErrorCode::OutcomeUnknown {
                        // The leader could not settle an attempt within the
                        // deadline: pass it through as unknown.
                        *uncertain = true;
                        return Err(error);
                    }
                    if error.outcome == Outcome::Unknown {
                        // Not settled yet (a proposal failed in an election):
                        // resend the same id and body; the dedup record
                        // decides.
                        *uncertain = true;
                        GatewayStats::bump(&self.stats.resends);
                        range.leader_hint.store(0, Ordering::Relaxed);
                        rotation += 1;
                        if !pause(deadline, &mut delay).await {
                            return Err(deadline_error(true));
                        }
                        continue;
                    }
                    if error.code == ErrorCode::NotFound && !refreshed_unknown_table {
                        // `UnknownTable`: the table may have been recreated.
                        refreshed_unknown_table = true;
                        self.refresh(&table, deadline).await;
                        continue;
                    }
                    if error.code == ErrorCode::Overloaded {
                        // Typed 429, refused before execution. Pass it
                        // through unless an earlier attempt may have run.
                        if !*uncertain {
                            return Err(error);
                        }
                    } else if error.code != ErrorCode::StorageUnavailable {
                        range.leader_hint.store(voter.node_id, Ordering::Relaxed);
                        return Ok(Terminal::Failed(error));
                    }
                    // Not applied and transient: retry within the deadline.
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(*uncertain));
                    }
                }
                Ok(DirectResponse::StaleRoute { .. }) => {
                    GatewayStats::bump(&self.stats.stale_routes);
                    let fresh = self.refresh(&table, deadline).await;
                    // A route the Catalog has not moved past yet: wait for
                    // the publication instead of spinning.
                    let unchanged = fresh.as_ref().is_none_or(|fresh| {
                        fresh.schema_epoch == request.schema_epoch
                            && fresh
                                .range_for(key)
                                .is_some_and(|r| r.shard.routing_epoch == request.routing_epoch)
                    });
                    if unchanged && !pause(deadline, &mut delay).await {
                        return Err(deadline_error(*uncertain));
                    }
                }
                Ok(DirectResponse::NotLeader { hint }) => {
                    GatewayStats::bump(&self.stats.not_leader);
                    match hint.filter(|node| *node != voter.node_id) {
                        Some(node) if voters.iter().any(|voter| voter.node_id == node) => {
                            range.leader_hint.store(node, Ordering::Relaxed);
                        }
                        Some(_) => {
                            // A leader outside the cached placement: the
                            // replicas moved.
                            range.leader_hint.store(0, Ordering::Relaxed);
                            let _ = self.voters(&range, true).await;
                        }
                        None => {
                            range.leader_hint.store(0, Ordering::Relaxed);
                            rotation += 1;
                            // No leader known (an election): back off.
                            if !pause(deadline, &mut delay).await {
                                return Err(deadline_error(*uncertain));
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
                    range.leader_hint.store(0, Ordering::Relaxed);
                    rotation += 1;
                    if error.code == ErrorCode::ValidationError {
                        // The receiver refused this replica identity: read
                        // the placement again.
                        let _ = self.voters(&range, true).await;
                    }
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(*uncertain));
                    }
                }
                Err(DirectSendError::Uncertain(_)) => {
                    // A lost reply: resend the same id and body. The dedup
                    // record decides whether the first attempt committed.
                    *uncertain = true;
                    GatewayStats::bump(&self.stats.resends);
                    range.leader_hint.store(0, Ordering::Relaxed);
                    rotation += 1;
                    if !pause(deadline, &mut delay).await {
                        return Err(deadline_error(true));
                    }
                }
            }
        }
    }

    /// The cached route, or one filled from the source. With `seen`, a
    /// refresh: fill again unless another request already replaced `seen`.
    async fn table(
        &self,
        table_id: Uuid,
        seen: Option<&Arc<TableRoute>>,
        deadline: Instant,
    ) -> Result<Option<Arc<TableRoute>>, ProtocolError> {
        let current = self.cache.get(table_id);
        match (&current, seen) {
            (Some(current), None) => return Ok(Some(current.clone())),
            (Some(current), Some(seen)) if !Arc::ptr_eq(current, seen) => {
                return Ok(Some(current.clone()));
            }
            _ => {}
        }
        let _fill = tokio::time::timeout_at(deadline, self.fill.lock())
            .await
            .map_err(|_| route_unavailable())?;
        // Another request may have filled it while this one waited.
        if let Some(now) = self.cache.get(table_id)
            && current
                .as_ref()
                .is_none_or(|before| !Arc::ptr_eq(before, &now))
        {
            return Ok(Some(now));
        }
        GatewayStats::bump(&self.stats.catalog_reads);
        let fill = tokio::time::timeout_at(deadline, self.source.table(table_id))
            .await
            .map_err(|_| route_unavailable())??;
        let Some(fill) = fill else {
            self.cache.remove(table_id);
            return Ok(None);
        };
        let route = Arc::new(self.build(fill, current.as_deref())?);
        self.cache.insert(route.clone());
        Ok(Some(route))
    }

    /// Fills `table` again after `StaleRoute` or an unknown table.
    async fn refresh(&self, table: &Arc<TableRoute>, deadline: Instant) -> Option<Arc<TableRoute>> {
        self.table(table.table_id, Some(table), deadline)
            .await
            .ok()
            .flatten()
    }

    fn build(
        &self,
        fill: TableFill,
        previous: Option<&TableRoute>,
    ) -> Result<TableRoute, ProtocolError> {
        if fill.shards.is_empty()
            || fill.shards.len() > MAX_RANGES
            || fill.shards.iter().any(|shard| {
                shard.group.database != self.database
                    || !matches!(shard.group.kind, GroupKind::Data { table_id, .. }
                        if table_id == fill.table_id)
                    || shard.routing_epoch == 0
            })
            || fill.schema_epoch == 0
            || !matches!(fill.key_type, ColumnType::Int64 | ColumnType::Utf8)
        {
            return Err(route_unavailable());
        }
        let charge = self
            .shared
            .reserve_scratch(ByteClass::Progress, TableRoute::heap_bytes(&fill).max(1))?;
        let ranges = fill
            .shards
            .into_iter()
            .map(|shard| {
                // A range whose group and epoch did not change keeps its
                // voters and leader hint: only the moved part is re-read.
                previous
                    .and_then(|previous| {
                        previous.ranges.iter().find(|old| {
                            old.shard.group == shard.group
                                && old.shard.routing_epoch == shard.routing_epoch
                                && old.shard.span == shard.span
                        })
                    })
                    .cloned()
                    .unwrap_or_else(|| Arc::new(RangeRoute::new(shard)))
            })
            .collect();
        Ok(TableRoute {
            table_id: fill.table_id,
            key_column: fill.key_column,
            key_type: fill.key_type,
            schema_epoch: fill.schema_epoch,
            ranges,
            _charge: charge,
        })
    }

    async fn voters(
        &self,
        range: &RangeRoute,
        refresh: bool,
    ) -> Result<Arc<[Voter]>, ProtocolError> {
        if !refresh && let Some(voters) = range.cached_voters() {
            return Ok(voters);
        }
        GatewayStats::bump(&self.stats.placement_reads);
        let mut voters = self.source.placement(range.group(), refresh).await?;
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

/// The API process: the Catalog read goes to a worker (`DirectRoute` on
/// the application endpoint), the placement read to Control.
pub(crate) struct ApiTopology {
    pub client: Arc<super::ApplicationClient>,
    pub control: Arc<crate::oltp_replication::authority::control::ControlClient>,
}
impl TopologySource for ApiTopology {
    fn table(&self, table_id: Uuid) -> EndpointFuture<'_, Option<TableFill>> {
        Box::pin(self.client.direct_table(table_id))
    }
    fn placement<'a>(
        &'a self,
        group: &'a GroupIdentity,
        refresh: bool,
    ) -> EndpointFuture<'a, Vec<Voter>> {
        Box::pin(control_placement(&self.control, group, refresh))
    }
}

/// Reads a table's fill from a Catalog replica at its newest version.
pub(crate) async fn catalog_fill(
    catalog: &crate::oltp::mvcc::catalog::CatalogHandle,
    authority: crate::oltp_replication::types::Authority,
    table_id: Uuid,
) -> Result<Option<TableFill>, ProtocolError> {
    use crate::oltp::mvcc::catalog::CatalogRead;
    let latest =
        crate::oltp_replication::types::ReadTs::new(u64::MAX).map_err(|_| route_unavailable())?;
    let pin = catalog.snapshot(latest, authority)?;
    let table = match catalog.describe(&pin, table_id).await? {
        // A DDL holds the table: no route until it settles.
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
    let mut next = match catalog.routing_root(&pin, table_id).await? {
        CatalogRead::Blocked(_) => return Err(route_unavailable()),
        CatalogRead::Value { record: None, .. } => return Ok(None),
        CatalogRead::Value {
            record: Some(root), ..
        } => {
            if root.schema_epoch != table.schema_epoch {
                return Err(route_unavailable());
            }
            root.first_page
        }
    };
    let mut shards = Vec::new();
    while let Some(page_id) = next {
        let page = match catalog.routing_page(&pin, table_id, page_id).await? {
            CatalogRead::Value {
                record: Some(page), ..
            } => page,
            _ => return Err(route_unavailable()),
        };
        for shard in page.shards.iter() {
            if shards.len() == MAX_RANGES || shard.schema_epoch != table.schema_epoch {
                return Err(route_unavailable());
            }
            shards.push(shard.clone());
        }
        next = page.next_page;
    }
    Ok(Some(TableFill {
        table_id,
        key_column: table.descriptor.key.clone(),
        key_type,
        schema_epoch: table.schema_epoch,
        shards,
    }))
}

/// Waits before the next attempt, doubling from `FIRST_PAUSE` to
/// `MAX_PAUSE`. False when the deadline leaves no time for one.
async fn pause(deadline: Instant, delay: &mut Duration) -> bool {
    *delay = (*delay * 2).clamp(FIRST_PAUSE, MAX_PAUSE);
    if Instant::now() + *delay >= deadline {
        return false;
    }
    tokio::time::sleep(*delay).await;
    true
}

fn digest(body: &[u8]) -> Digest {
    use sha2::{Digest as _, Sha256};
    Digest::from_bytes(Sha256::digest(body).into())
}

fn deadline_error(uncertain: bool) -> ProtocolError {
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

/// The keys routing needs: one key to find the range, the anchor (the
/// smallest key) and whether every key falls in the first key's range.
struct RouteKeys {
    first: Key,
    anchor: Key,
    one_range: bool,
}

/// Reads the request's primary keys with the route's key column. Rows are
/// visited one at a time; only the first and the smallest key are kept.
fn route_keys(scanned: &Scanned<'_>, table: &TableRoute) -> Option<RouteKeys> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Point<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        key: Key,
        #[serde(borrow, default)]
        values: Option<&'a RawValue>,
    }
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
    let valid = |key: Key| key.validate(table.key_type).is_ok().then_some(key);
    let single = |key: Key| {
        valid(key).map(|key| RouteKeys {
            first: key.clone(),
            anchor: key,
            one_range: true,
        })
    };
    match scanned.op {
        Op::Get | Op::Delete | Op::Update => {
            let point: Point<'_> = serde_json::from_str(raw).ok()?;
            if (scanned.op == Op::Update) != point.values.is_some() {
                return None;
            }
            single(point.key)
        }
        Op::Insert | Op::Upsert => {
            let one: One<'_> = serde_json::from_str(raw).ok()?;
            single(row_key(one.row.get(), &table.key_column)?)
        }
        Op::InsertMany => {
            let many: Many<'_> = serde_json::from_str(raw).ok()?;
            let mut decoder = serde_json::Deserializer::from_str(many.rows.get());
            let keys = serde::Deserializer::deserialize_seq(
                &mut decoder,
                Rows {
                    table,
                    table_id: scanned.table_id,
                },
            )
            .ok()??;
            decoder.end().ok()?;
            Some(keys)
        }
    }
}

struct Rows<'t> {
    table: &'t TableRoute,
    table_id: Uuid,
}
impl<'de> serde::de::Visitor<'de> for Rows<'_> {
    type Value = Option<RouteKeys>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an array of rows")
    }
    fn visit_seq<S: serde::de::SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
        let row = |key: &Key| MvccKey::Row {
            table_id: self.table_id,
            primary_key: key.clone(),
        };
        let mut keys: Option<(RouteKeys, Option<*const RangeRoute>)> = None;
        while let Some(raw) = seq.next_element::<&'de RawValue>()? {
            let Some(key) = row_key(raw.get(), &self.table.key_column)
                .filter(|key| key.validate(self.table.key_type).is_ok())
            else {
                return Ok(None);
            };
            let range = self.table.range_for(&key).map(Arc::as_ptr);
            match &mut keys {
                None => {
                    keys = Some((
                        RouteKeys {
                            first: key.clone(),
                            anchor: key,
                            one_range: range.is_some(),
                        },
                        range,
                    ));
                }
                Some((keys, first)) => {
                    if range.is_none() || range != *first {
                        keys.one_range = false;
                    }
                    if compare_keys(&row(&key), &row(&keys.anchor)).is_lt() {
                        keys.anchor = key;
                    }
                }
            }
        }
        Ok(keys.map(|(keys, _)| keys))
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
