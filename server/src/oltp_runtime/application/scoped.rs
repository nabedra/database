use super::*;
use crate::{
    oltp::{mvcc::catalog::ShardRoute, pool::ScratchLease},
    oltp_replication::authority::control::ScopedQueuePlacement,
    protocol::{Key, Operation},
};
use std::time::Duration;
use std::{collections::VecDeque, sync::Mutex};

const MAX_ROUTE_BYTES: usize = 4096;
const MAX_CACHED_RANGES: usize = 64;
const MAX_CACHED_RECEIPTS: usize = 128;
const ROUTE_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::oltp_runtime) struct PointRequest {
    pub request_id: Uuid,
    pub table_id: Uuid,
    pub key: Key,
}

impl PointRequest {
    pub(in crate::oltp_runtime) fn from_operation(
        request_id: Uuid,
        operation: &Operation,
    ) -> Option<Self> {
        let (table_id, key) = match operation {
            Operation::Get(payload) => (payload.table_id, &payload.key),
            Operation::Update(payload) => (payload.table_id, &payload.key),
            _ => return None,
        };
        if matches!(key, Key::Utf8(text) if text.len() > MAX_ROUTE_BYTES) {
            return None;
        }
        Some(Self {
            request_id,
            table_id,
            key: key.clone(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::oltp_runtime) struct ScopedTarget {
    pub group_id: Uuid,
    pub data_generation: Uuid,
    pub queue_generation: Uuid,
    pub view: AuthorityView,
    pub owner: WorkerIdentity,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::oltp_runtime) struct ScopedReceiptRoute {
    pub target: ScopedTarget,
    pub digest: Digest,
}

impl From<&ScopedQueuePlacement> for ScopedTarget {
    fn from(placement: &ScopedQueuePlacement) -> Self {
        Self {
            group_id: placement.group.group_id,
            data_generation: placement.data_generation,
            queue_generation: placement.queue_generation,
            view: placement.view,
            owner: placement.owner,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::oltp_runtime) struct ScopedRoute {
    pub shard: ShardRoute,
    pub target: ScopedTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::oltp_runtime) enum PointRouting {
    Legacy,
    Owner(ScopedTarget),
    Range(ScopedRoute),
}

impl PointRouting {
    pub(in crate::oltp_runtime) fn range(shard: ShardRoute, target: ScopedTarget) -> Self {
        let route = ScopedRoute { shard, target };
        if route.heap_bytes() <= 2048 {
            Self::Range(route)
        } else {
            Self::Owner(target)
        }
    }
}

impl ScopedRoute {
    fn matches(&self, point: &PointRequest, group: &GroupIdentity) -> bool {
        self.shard.group.cluster_id == group.cluster_id
            && self.shard.group.database == group.database
            && matches!(self.shard.group.kind, GroupKind::Data { table_id, .. }
                if table_id == point.table_id)
            && self.shard.group.group_id == self.target.group_id
            && self.shard.contains_key(&point.key)
    }

    fn heap_bytes(&self) -> usize {
        let bounds = [&self.shard.span.start, &self.shard.span.end];
        self.shard.group.database.capacity_bytes()
            + bounds
                .iter()
                .map(|bound| match bound {
                    Some(Key::Utf8(text)) => text.capacity(),
                    _ => 0,
                })
                .sum::<usize>()
    }
}

struct RangeHint {
    route: ScopedRoute,
    _charge: ScratchLease,
}

pub(super) struct ScopedRoutes {
    ranges: Mutex<VecDeque<RangeHint>>,
    receipts: Mutex<VecDeque<(Uuid, Uuid, ScopedReceiptRoute)>>,
    _resident: ScratchLease,
}

impl ScopedRoutes {
    pub(super) fn new(shared: &WorkerSharedResources) -> Result<Self, ProtocolError> {
        let bytes = MAX_CACHED_RANGES * std::mem::size_of::<RangeHint>()
            + MAX_CACHED_RECEIPTS * std::mem::size_of::<(Uuid, Uuid, ScopedReceiptRoute)>();
        let resident = shared.reserve_scratch(ByteClass::Progress, bytes)?;
        Ok(Self {
            ranges: Mutex::new(VecDeque::with_capacity(MAX_CACHED_RANGES)),
            receipts: Mutex::new(VecDeque::with_capacity(MAX_CACHED_RECEIPTS)),
            _resident: resident,
        })
    }

    fn target(&self, point: &PointRequest, group: &GroupIdentity) -> Option<ScopedTarget> {
        self.ranges
            .lock()
            .ok()?
            .iter()
            .rev()
            .find(|hint| hint.route.matches(point, group))
            .map(|hint| hint.route.target)
    }

    fn remember_route(
        &self,
        shared: &WorkerSharedResources,
        route: ScopedRoute,
        point: &PointRequest,
        group: &GroupIdentity,
    ) -> Result<ScopedTarget, ProtocolError> {
        if !route.matches(point, group)
            || route.target.view.validate().is_err()
            || route.target.owner.validate().is_err()
            || route.target.data_generation.is_nil()
            || route.target.queue_generation.is_nil()
        {
            return Err(invalid());
        }
        let target = route.target;
        if route.heap_bytes() <= MAX_ROUTE_BYTES
            && let Ok(charge) = shared.reserve_scratch(ByteClass::Data, route.heap_bytes().max(1))
        {
            let mut hints = self.ranges.lock().map_err(|_| unavailable())?;
            hints.retain(|hint| hint.route.target.group_id != target.group_id);
            if hints.len() == MAX_CACHED_RANGES {
                hints.pop_front();
            }
            hints.push_back(RangeHint {
                route,
                _charge: charge,
            });
        }
        Ok(target)
    }

    fn forget(&self, target: ScopedTarget) {
        if let Ok(mut ranges) = self.ranges.lock() {
            ranges.retain(|hint| hint.route.target != target);
        }
        if let Ok(mut receipts) = self.receipts.lock() {
            receipts.retain(|(_, _, old)| old.target != target);
        }
    }

    fn remember_receipt(&self, run: Uuid, request_id: Uuid, route: ScopedReceiptRoute) {
        if let Ok(mut receipts) = self.receipts.lock() {
            receipts.retain(|(r, id, _)| (*r, *id) != (run, request_id));
            if receipts.len() == MAX_CACHED_RECEIPTS {
                receipts.pop_front();
            }
            receipts.push_back((run, request_id, route));
        }
    }

    fn receipt(&self, run: Uuid, request_id: Uuid) -> Option<ScopedReceiptRoute> {
        self.receipts
            .lock()
            .ok()?
            .iter()
            .find(|(r, id, _)| (*r, *id) == (run, request_id))
            .map(|(_, _, target)| *target)
    }
}

pub(in crate::oltp_runtime) fn stale_route() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped OLTP route changed before execution",
        Outcome::NotApplied,
    )
}

impl ApplicationClient {
    pub(in crate::oltp_runtime) fn has_scoped_receipt(&self, run: Uuid, id: Uuid) -> bool {
        self.scoped.as_ref().is_some_and(|routes| routes.receipt(run, id).is_some())
    }

    pub(in crate::oltp_runtime) async fn submit_routed(
        &self,
        body: ChargedBytes,
        point: Option<PointRequest>,
    ) -> Result<RequestRecord, ProtocolError> {
        let (Some(routes), Some(point)) = (&self.scoped, point) else {
            return self.submit_legacy(body).await;
        };
        tokio::time::timeout(ROUTE_DEADLINE, async {
            let mut hint = routes.target(&point, &self.group);
            for attempt in 0..2 {
                let target = match hint.take() {
                    Some(target) => target,
                    None => {
                        use sha2::{Digest as _, Sha256};
                        let digest = Digest::from_bytes(Sha256::digest(body.as_ref()).into());
                        // Long keys still work on the existing wire. Bound the
                        // route-discovery header after JSON escaping.
                        if serde_json::to_vec(&point).map_err(|_| invalid())?.len()
                            > MAX_ROUTE_BYTES
                        {
                            return self.submit_legacy(body).await;
                        }
                        let reply = self
                            .call(
                                Method::RoutePoint {
                                    point: point.clone(),
                                    digest,
                                },
                                None,
                            )
                            .await?;
                        let route = reply.decode_section::<PointRouting>(0)?;
                        match &*route {
                            PointRouting::Legacy => return self.submit_legacy(body).await,
                            PointRouting::Owner(target) => *target,
                            PointRouting::Range(route) => routes.remember_route(
                                &self.shared,
                                route.clone(),
                                &point,
                                &self.group,
                            )?,
                        }
                    }
                };
                let submitted = self
                    .call_to(
                        target.owner,
                        Method::ScopedSubmit {
                            bytes: body.len() as u64,
                            target,
                        },
                        Some(body.clone()),
                    )
                    .await;
                match submitted {
                    Err(error) if error == stale_route() && attempt == 0 => {
                        routes.forget(target);
                    }
                    Err(error) => {
                        if error.code == ErrorCode::StorageUnavailable {
                            routes.forget(target);
                        }
                        return Err(error);
                    }
                    Ok(reply) => {
                        let record = Self::submitted_record(reply)?;
                        if record.request_id != point.request_id
                            || record.run_id != target.view.run_id
                        {
                            return Err(invalid());
                        }
                        routes.remember_receipt(
                            record.run_id,
                            record.request_id,
                            ScopedReceiptRoute {
                                target,
                                digest: Digest::parse(&record.digest).map_err(|_| invalid())?,
                            },
                        );
                        return Ok(record);
                    }
                }
            }
            Err(stale_route())
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) async fn call_for_receipt(
        &self,
        run: Uuid,
        id: Uuid,
        legacy: Method,
        scoped: impl Fn(ScopedReceiptRoute) -> Method,
    ) -> Result<ReceivedReply, ProtocolError> {
        let Some(routes) = &self.scoped else {
            return self.call(legacy, None).await;
        };
        let mut target = routes.receipt(run, id);
        for attempt in 0..2 {
            let current = match target.take() {
                Some(current) => current,
                None => {
                    let reply = self
                        .call(
                            Method::RouteReceipt {
                                run_id: run,
                                request_id: id,
                            },
                            None,
                        )
                        .await?;
                    let Some(current) = *reply.decode_section::<Option<ScopedReceiptRoute>>(0)?
                    else {
                        return self.call(legacy, None).await;
                    };
                    if current.target.view.run_id != run || current.target.owner.validate().is_err()
                    {
                        return Err(invalid());
                    }
                    current
                }
            };
            match self
                .call_to(current.target.owner, scoped(current), None)
                .await
            {
                Err(error) if error.code == ErrorCode::StorageUnavailable && attempt == 0 => {
                    routes.forget(current.target);
                }
                Ok(reply) => {
                    routes.remember_receipt(run, id, current);
                    return Ok(reply);
                }
                Err(error) => return Err(error),
            }
        }
        Err(stale_route())
    }
}
