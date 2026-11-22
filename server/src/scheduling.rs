//! Event-driven, dominant-share resource admission. No I/O runs under the state lock.
use crate::{
    config::WorkerResources,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

#[cfg(test)]
mod remote_placement_tests;
#[cfg(test)]
mod scheduler_properties;

#[derive(Clone)]
pub struct ResourceScheduler {
    shared: Arc<Shared>,
}
struct Shared {
    state: Mutex<State>,
}
struct State {
    closed: bool,
    nodes: BTreeMap<u64, Node>,
    pending: BTreeMap<u64, Pending>,
    allocated: BTreeMap<u64, Allocation>,
    next: u64,
    round: u64,
    last_grant: BTreeMap<u64, u64>,
}
struct Node {
    enabled: bool,
    limits: WorkerResources,
    threads: u64,
    bytes: u64,
    retained: u64,
    io_limit: u64,
    io_allocations: u64,
}
struct Pending {
    request: u64,
    cap: usize,
    bytes: u64,
    compute: bool,
    node_allocation: bool,
    fixed_node: Option<u64>,
    headroom: u64,
    bypass: u32,
    notify: oneshot::Sender<u64>,
}
/// Rebuilt from allocation owners for one dispatch pass, never retained as state.
#[derive(Default)]
struct RequestUsage {
    threads: u64,
    bytes: u64,
    nodes: std::collections::BTreeSet<u64>,
    retained_nodes: std::collections::BTreeSet<u64>,
}
struct Allocation {
    request: u64,
    node: u64,
    bytes: u64,
    compute: bool,
    node_allocation: bool,
    retained: bool,
}
/// A grant owns CPU and working bytes until the actual job settles, even if its receiver closes.
pub struct Reservation {
    scheduler: ResourceScheduler,
    token: Option<u64>,
    node: u64,
    bytes: u64,
    charged: Vec<MemoryCharge>,
}
impl Reservation {
    pub fn node_id(&self) -> u64 {
        self.node
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    /// Release CPU while retaining every returned buffer byte under one memory owner.
    pub fn into_memory(mut self) -> MemoryCharge {
        let token = self.token.take().unwrap();
        let mut state = self.scheduler.shared.state.lock().unwrap();
        for charge in &mut self.charged {
            if let Some(other) = charge.allocation.take() {
                state.release(other);
            } else {
                state.nodes.get_mut(&self.node).unwrap().retained -= charge.bytes;
            }
            let bytes = charge.bytes;
            charge.bytes = 0;
            state.allocated.get_mut(&token).unwrap().bytes += bytes;
            state.nodes.get_mut(&self.node).unwrap().bytes += bytes;
        }
        let allocation = state.allocated.get_mut(&token).unwrap();
        allocation.retained = true;
        if allocation.compute {
            allocation.compute = false;
            state.nodes.get_mut(&self.node).unwrap().threads -= 1;
        }
        state.schedule();
        drop(state);
        MemoryCharge {
            scheduler: self.scheduler.clone(),
            node: self.node,
            bytes: self.bytes,
            allocation: Some(token),
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.scheduler.cancel(token);
        }
    }
}
struct Waiting {
    scheduler: ResourceScheduler,
    token: u64,
    armed: bool,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if self.armed {
            self.scheduler.cancel(self.token);
        }
    }
}
/// Queue/result memory shares the same authoritative ceiling as compute.
pub struct MemoryCharge {
    scheduler: ResourceScheduler,
    node: u64,
    bytes: u64,
    allocation: Option<u64>,
}
impl MemoryCharge {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn node_id(&self) -> u64 {
        self.node
    }
    pub fn shrink_to(&mut self, bytes: u64) -> Result<(), ProtocolError> {
        if bytes > self.bytes {
            return Err(resource());
        }
        let freed = self.bytes - bytes;
        let mut state = self.scheduler.shared.state.lock().unwrap();
        if let Some(token) = self.allocation {
            state.allocated.get_mut(&token).ok_or_else(resource)?.bytes -= freed;
            state.nodes.get_mut(&self.node).unwrap().bytes -= freed;
        } else {
            state.nodes.get_mut(&self.node).unwrap().retained -= freed;
        }
        self.bytes = bytes;
        state.schedule();
        Ok(())
    }
}
impl Drop for MemoryCharge {
    fn drop(&mut self) {
        let mut state = self.scheduler.shared.state.lock().unwrap();
        if let Some(token) = self.allocation {
            state.release(token);
        } else if let Some(node) = state.nodes.get_mut(&self.node) {
            node.retained -= self.bytes;
        }
        state.schedule();
    }
}
impl ResourceScheduler {
    pub fn new(nodes: Vec<(u64, WorkerResources)>) -> Result<Self, ProtocolError> {
        let mut members = BTreeMap::new();
        for (id, limits) in nodes {
            limits.validate().map_err(|_| resource())?;
            if members
                .insert(
                    id,
                    Node {
                        enabled: true,
                        limits,
                        threads: 0,
                        bytes: 0,
                        retained: 0,
                        io_limit: u64::MAX,
                        io_allocations: 0,
                    },
                )
                .is_some()
            {
                return Err(resource());
            }
        }
        if members.is_empty() {
            return Err(resource());
        }
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    closed: false,
                    nodes: members,
                    pending: BTreeMap::new(),
                    allocated: BTreeMap::new(),
                    next: 0,
                    round: 0,
                    last_grant: BTreeMap::new(),
                }),
            }),
        })
    }
    pub async fn reserve(
        &self,
        request: u64,
        cap: usize,
        bytes: u64,
    ) -> Result<Reservation, ProtocolError> {
        self.reserve_kind(request, cap, bytes, true, None, false, false, 0)
            .await
    }
    /// Place a remote child using its memory and request-node allocation only.
    /// The receiving worker reserves CPU when its storage input is ready.
    pub async fn reserve_remote_memory(
        &self,
        request: u64,
        cap: usize,
        bytes: u64,
    ) -> Result<Reservation, ProtocolError> {
        self.reserve_kind(request, cap, bytes, false, None, false, true, 0)
            .await
    }
    pub async fn reserve_memory(
        &self,
        request: u64,
        node: u64,
        bytes: u64,
    ) -> Result<MemoryCharge, ProtocolError> {
        let mut reservation = self
            .reserve_kind(
                request,
                usize::MAX,
                bytes,
                false,
                Some(node),
                false,
                false,
                0,
            )
            .await?;
        Ok(MemoryCharge {
            scheduler: self.clone(),
            node,
            bytes,
            allocation: reservation.token.take(),
        })
    }
    /// Coordinator metadata/encoding stages use working RAM, outside the
    /// per-partition task limit. Leave headroom for ingress and child progress.
    #[cfg(any(feature = "olap", test))]
    pub(crate) async fn reserve_coordinator_memory(
        &self,
        request: u64,
        node: u64,
        bytes: u64,
        headroom: u64,
    ) -> Result<MemoryCharge, ProtocolError> {
        if headroom == 0 {
            return Err(resource());
        }
        let mut reservation = self
            .reserve_kind(
                request,
                usize::MAX,
                bytes,
                false,
                Some(node),
                false,
                false,
                headroom,
            )
            .await?;
        Ok(MemoryCharge {
            scheduler: self.clone(),
            node,
            bytes,
            allocation: reservation.token.take(),
        })
    }
    pub async fn reserve_precharged(
        &self,
        request: u64,
        cap: usize,
        charge: MemoryCharge,
    ) -> Result<Reservation, ProtocolError> {
        if !Arc::ptr_eq(&self.shared, &charge.scheduler.shared) {
            return Err(resource());
        }
        {
            let state = self.shared.state.lock().unwrap();
            if charge.bytes
                > state
                    .nodes
                    .get(&charge.node)
                    .ok_or_else(resource)?
                    .limits
                    .task_memory_limit_bytes
            {
                return Err(resource());
            }
        }
        let mut reservation = self
            .reserve_kind(request, cap, 0, true, Some(charge.node), false, false, 0)
            .await?;
        reservation.bytes = charge.bytes;
        reservation.charged = vec![charge];
        Ok(reservation)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "admission checks these independent resource and placement bounds together"
    )]
    async fn reserve_kind(
        &self,
        request: u64,
        cap: usize,
        bytes: u64,
        compute: bool,
        fixed_node: Option<u64>,
        buffered_growth: bool,
        node_allocation: bool,
        headroom: u64,
    ) -> Result<Reservation, ProtocolError> {
        let (tx, rx) = oneshot::channel();
        let token = {
            let mut state = self.shared.state.lock().unwrap();
            if state.closed
                || cap == 0
                || !state.nodes.iter().any(|(&id, node)| {
                    node.enabled
                        && fixed_node.is_none_or(|wanted| wanted == id)
                        && (headroom > 0 || bytes <= node.limits.task_memory_limit_bytes)
                        && bytes
                            .checked_add(headroom)
                            .is_some_and(|total| total <= node.limits.working_bytes())
                })
            {
                return Err(resource());
            }
            // A bounded operator may retain earlier output while requesting its next
            // input. Waiting for more memory then permits cycles between operators.
            // Reject that growth, while a fresh request may still wait and age normally.
            if !compute && let Some(node_id) = fixed_node {
                // An available buffer cannot form a retained-memory cycle.
                // Avoid scanning every queue/body owner on the common path.
                if state.check_available_memory(node_id, bytes).is_err() {
                    state.check_buffered_growth(request, node_id, bytes)?;
                }
            }
            if buffered_growth {
                state.check_available_memory(fixed_node.ok_or_else(resource)?, bytes)?;
            }
            let token = state.next;
            state.next = state.next.checked_add(1).ok_or_else(resource)?;
            state.pending.insert(
                token,
                Pending {
                    request,
                    cap,
                    bytes,
                    compute,
                    node_allocation,
                    fixed_node,
                    headroom,
                    bypass: 0,
                    notify: tx,
                },
            );
            state.schedule();
            if !state.allocated.contains_key(&token) {
                // Scheduling only adds unretained allocations, so this predicate
                // is unchanged by dispatch. A granted waiter needs no owner scan.
                let must_grant = buffered_growth
                    || (!compute
                        && fixed_node.is_some_and(|node| {
                            state.allocated.values().any(|a| {
                                a.request == request && a.node == node && a.bytes > 0 && a.retained
                            })
                        }));
                if must_grant {
                    state.pending.remove(&token);
                    state.forget_if_idle(request);
                    return Err(resource());
                }
            }
            token
        };
        let mut waiting = Waiting {
            scheduler: self.clone(),
            token,
            armed: true,
        };
        let node = rx.await.map_err(|_| resource())?;
        waiting.armed = false;
        Ok(Reservation {
            scheduler: self.clone(),
            token: Some(token),
            node,
            bytes,
            charged: Vec::new(),
        })
    }
    pub async fn reserve_with_memories(
        &self,
        request: u64,
        cap: usize,
        mut charges: Vec<MemoryCharge>,
        budget: u64,
    ) -> Result<Reservation, ProtocolError> {
        let node = charges.first().map(|c| c.node).ok_or_else(resource)?;
        let held = charges.iter().try_fold(0u64, |sum, c| {
            if c.node != node || !Arc::ptr_eq(&c.scheduler.shared, &self.shared) {
                return Err(resource());
            }
            sum.checked_add(c.bytes).ok_or_else(resource)
        })?;
        let extra = budget.checked_sub(held).ok_or_else(resource)?;
        {
            let state = self.shared.state.lock().unwrap();
            if budget
                > state
                    .nodes
                    .get(&node)
                    .ok_or_else(resource)?
                    .limits
                    .task_memory_limit_bytes
            {
                return Err(resource());
            }
        }
        if extra > 0 {
            // Obtain the complete bounded reducer buffer before waiting for CPU.
            // An operator retaining output must either grow immediately or fail;
            // it cannot wait for another operator to release its retained output.
            let topup = self
                .reserve_kind(
                    request,
                    usize::MAX,
                    extra,
                    false,
                    Some(node),
                    held > 0,
                    false,
                    0,
                )
                .await?;
            charges.push(topup.into_memory());
        }
        let mut reservation = self
            .reserve_kind(request, cap, 0, true, Some(node), false, false, 0)
            .await?;
        reservation.bytes = budget;
        reservation.charged = charges;
        Ok(reservation)
    }
    pub fn charge_memory(&self, node: u64, bytes: u64) -> Result<MemoryCharge, ProtocolError> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            return Err(resource());
        }
        let member = state.nodes.get_mut(&node).ok_or_else(resource)?;
        let retained = member.retained.checked_add(bytes).ok_or_else(resource)?;
        if retained
            .checked_add(member.bytes)
            .is_none_or(|n| n > member.limits.working_bytes())
        {
            return Err(resource());
        }
        member.retained = retained;
        Ok(MemoryCharge {
            scheduler: self.clone(),
            node,
            bytes,
            allocation: None,
        })
    }
    /// Resource changes never revoke running grants. Lower limits require a prior drain.
    pub fn update_node(&self, id: u64, limits: WorkerResources) -> Result<(), ProtocolError> {
        limits.validate().map_err(|_| resource())?;
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            return Err(resource());
        }
        if let Some(node) = state.nodes.get_mut(&id) {
            if node.threads > limits.max_threads
                || (limits.task_memory_limit_bytes < node.limits.task_memory_limit_bytes
                    && (node.bytes > 0 || node.retained > 0 || node.threads > 0))
                || node
                    .bytes
                    .checked_add(node.retained)
                    .is_none_or(|n| n > limits.working_bytes())
            {
                return Err(resource());
            }
            node.limits = limits;
            node.enabled = true;
        } else {
            state.nodes.insert(
                id,
                Node {
                    enabled: true,
                    limits,
                    threads: 0,
                    bytes: 0,
                    retained: 0,
                    io_limit: u64::MAX,
                    io_allocations: 0,
                },
            );
        }
        state.schedule();
        Ok(())
    }
    pub fn drain_node(&self, id: u64) -> Result<(), ProtocolError> {
        let mut state = self.shared.state.lock().unwrap();
        state.nodes.get_mut(&id).ok_or_else(resource)?.enabled = false;
        state.schedule();
        Ok(())
    }
    pub fn set_io_capacity(&self, id: u64, slots: u64) -> Result<(), ProtocolError> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed || slots == 0 {
            return Err(resource());
        }
        let node = state.nodes.get_mut(&id).ok_or_else(resource)?;
        if node.io_allocations > slots {
            return Err(resource());
        }
        node.io_limit = slots;
        state.schedule();
        Ok(())
    }
    pub async fn reserve_on(
        &self,
        request: u64,
        cap: usize,
        node: u64,
        bytes: u64,
    ) -> Result<Reservation, ProtocolError> {
        self.reserve_kind(request, cap, bytes, true, Some(node), false, false, 0)
            .await
    }
    pub fn usage(&self) -> Vec<(u64, u64, u64)> {
        self.shared
            .state
            .lock()
            .unwrap()
            .nodes
            .iter()
            .map(|(&id, n)| (id, n.threads, n.bytes + n.retained))
            .collect()
    }
    #[cfg(feature = "oltp")]
    pub(crate) fn oltp_capacity(&self, id: u64) -> Option<(u64, u64, WorkerResources)> {
        let state = self.shared.state.lock().ok()?;
        let node = state.nodes.get(&id)?;
        Some((node.threads, node.bytes.checked_add(node.retained)?, node.limits))
    }
    /// Stop new reservations and wake all waiters, including storage downloads
    /// waiting only for memory. Existing owners retain their charges until drop.
    pub fn close(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        state.pending.clear();
    }
    fn cancel(&self, token: u64) {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(pending) = state.pending.remove(&token) {
            state.forget_if_idle(pending.request);
        }
        state.release(token);
        state.schedule();
    }
}
impl State {
    fn check_available_memory(&self, node: u64, bytes: u64) -> Result<(), ProtocolError> {
        let node = self.nodes.get(&node).ok_or_else(resource)?;
        if node
            .bytes
            .checked_add(node.retained)
            .and_then(|used| used.checked_add(bytes))
            .is_none_or(|used| used > node.limits.working_bytes())
        {
            return Err(ProtocolError::new(
                ErrorCode::LimitExceeded,
                "bounded operator cannot retain output while awaiting more memory",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
    fn check_buffered_growth(
        &self,
        request: u64,
        node: u64,
        bytes: u64,
    ) -> Result<(), ProtocolError> {
        if self
            .allocated
            .values()
            .any(|a| a.request == request && a.node == node && a.bytes > 0 && a.retained)
        {
            self.check_available_memory(node, bytes)?;
        }
        Ok(())
    }
    fn release(&mut self, token: u64) {
        if let Some(grant) = self.allocated.remove(&token) {
            let node = self.nodes.get_mut(&grant.node).unwrap();
            if grant.compute {
                node.threads -= 1;
            }
            if grant.node_allocation && !grant.compute {
                node.io_allocations -= 1;
            }
            node.bytes -= grant.bytes;
            self.forget_if_idle(grant.request);
        }
    }
    /// Grant order (`last_grant`) only ranks requests that wait or hold work. A
    /// request with nothing pending or allocated drops its entry, so its next job
    /// competes as a new request. Without this the map kept one entry per request
    /// id for the life of the process; native executions take a fresh id each
    /// (FLAKE1: about 4.5 MiB on an RF3 leader after 7 minutes of point writes).
    fn forget_if_idle(&mut self, request: u64) {
        if !self.pending.values().any(|p| p.request == request)
            && !self.allocated.values().any(|a| a.request == request)
        {
            self.last_grant.remove(&request);
        }
    }
    fn request_usages(&self) -> BTreeMap<u64, RequestUsage> {
        let mut requests: BTreeMap<u64, RequestUsage> = BTreeMap::new();
        for allocation in self.allocated.values() {
            let usage = requests.entry(allocation.request).or_default();
            usage.threads = usage.threads.saturating_add(u64::from(allocation.compute));
            usage.bytes = usage.bytes.saturating_add(allocation.bytes);
            if allocation.compute || allocation.node_allocation {
                usage.nodes.insert(allocation.node);
            }
            if allocation.bytes > 0 && allocation.retained {
                usage.retained_nodes.insert(allocation.node);
            }
        }
        requests
    }
    fn schedule(&mut self) {
        if self.closed || self.pending.is_empty() {
            return;
        }
        while !self.pending.is_empty() {
            // One owner scan serves every waiter and aged-worker comparison.
            // Rebuild after each grant so fairness and caps see exactly its owners.
            let usages = self.request_usages();
            self.reject_impossible_waiters(&usages);
            if !self.schedule_one(&usages) {
                break;
            }
        }
    }
    fn reject_impossible_waiters(&mut self, usages: &BTreeMap<u64, RequestUsage>) {
        let impossible: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                let retained_cycle = !p.compute
                    && p.fixed_node.is_some_and(|node| {
                        usages
                            .get(&p.request)
                            .is_some_and(|usage| usage.retained_nodes.contains(&node))
                            && self.check_available_memory(node, p.bytes).is_err()
                    });
                retained_cycle
                    || !self.nodes.iter().any(|(&id, n)| {
                        n.enabled
                            && p.fixed_node.is_none_or(|wanted| wanted == id)
                            && (p.headroom > 0 || p.bytes <= n.limits.task_memory_limit_bytes)
                            && p.bytes
                                .checked_add(p.headroom)
                                .is_some_and(|total| total <= n.limits.working_bytes())
                    })
            })
            .map(|(&token, _)| token)
            .collect();
        for token in impossible {
            if let Some(pending) = self.pending.remove(&token) {
                self.forget_if_idle(pending.request);
            }
        }
    }
    fn schedule_one(&mut self, usages: &BTreeMap<u64, RequestUsage>) -> bool {
        let cluster_threads = self
            .nodes
            .values()
            .filter(|n| n.enabled)
            .map(|n| n.limits.max_threads as f64)
            .sum::<f64>();
        let cluster_bytes = self
            .nodes
            .values()
            .filter(|n| n.enabled)
            .map(|n| n.limits.working_bytes() as f64)
            .sum::<f64>();
        let aged = self
            .pending
            .iter()
            .find(|(_, p)| p.headroom == 0 && p.bypass >= 32)
            .map(|(&id, _)| id);
        let empty_usage = RequestUsage::default();
        // The oldest waiter's placement depends only on this pass's owners and
        // node state, not on the candidate being compared with it.
        let earmarked = aged
            .and_then(|token| self.pending.get(&token))
            .and_then(|old| {
                let old_nodes = &usages.get(&old.request).unwrap_or(&empty_usage).nodes;
                self.nodes
                    .iter()
                    .filter(|(id, node)| {
                        node.enabled
                            && old.fixed_node.is_none_or(|wanted| wanted == **id)
                            && old.bytes <= node.limits.task_memory_limit_bytes
                            && (old_nodes.contains(id) || old_nodes.len() < old.cap)
                    })
                    .min_by_key(|(id, node)| {
                        (
                            old.bytes.saturating_sub(
                                node.limits
                                    .working_bytes()
                                    .saturating_sub(node.bytes + node.retained),
                            ),
                            **id,
                        )
                    })
                    .map(|(&id, _)| (id, old.bytes))
            });
        let mut candidates = Vec::new();
        for (&token, pending) in &self.pending {
            let usage = usages.get(&pending.request).unwrap_or(&empty_usage);
            let nodes = &usage.nodes;
            let share =
                (usage.threads as f64 / cluster_threads).max(usage.bytes as f64 / cluster_bytes);
            let target = self
                .nodes
                .iter()
                .filter(|(id, node)| {
                    let fit = node.enabled
                        && (!pending.compute || node.threads < node.limits.max_threads)
                        && (!pending.node_allocation
                            || pending.compute
                            || node.io_allocations < node.io_limit)
                        && pending.fixed_node.is_none_or(|wanted| wanted == **id)
                        && (pending.headroom > 0
                            || pending.bytes <= node.limits.task_memory_limit_bytes)
                        && node
                            .bytes
                            .checked_add(node.retained)
                            .and_then(|n| n.checked_add(pending.bytes))
                            .and_then(|n| n.checked_add(pending.headroom))
                            .is_some_and(|n| n <= node.limits.working_bytes())
                        && (!(pending.compute || pending.node_allocation)
                            || nodes.contains(id)
                            || nodes.len() < pending.cap);
                    // Earmark only the bytes the oldest aged waiter needs on its best candidate node.
                    let protected = Some(token) != aged
                        && earmarked.is_some_and(|(earmarked_node, bytes)| {
                            earmarked_node == **id
                                && node
                                    .limits
                                    .working_bytes()
                                    .saturating_sub(node.bytes + node.retained)
                                    < bytes.saturating_add(pending.bytes)
                        });
                    fit && !protected
                })
                .min_by(|(aid, a), (bid, b)| {
                    let score = |n: &Node| {
                        let work = if pending.compute {
                            n.threads + 1
                        } else if pending.node_allocation {
                            n.io_allocations + 1
                        } else {
                            0
                        };
                        (work as f64 / n.limits.max_threads as f64).max(
                            (n.bytes + n.retained + pending.bytes) as f64
                                / n.limits.working_bytes() as f64,
                        )
                    };
                    score(a).total_cmp(&score(b)).then(aid.cmp(bid))
                })
                .map(|(&id, _)| id);
            if let Some(node) = target {
                candidates.push((
                    token,
                    node,
                    share,
                    Some(token) != aged,
                    self.last_grant.get(&pending.request).copied().unwrap_or(0),
                ));
            }
        }
        let Some((token, node, _, _, _)) = candidates.into_iter().min_by(|a, b| {
            a.3.cmp(&b.3)
                .then(a.2.total_cmp(&b.2))
                .then(a.4.cmp(&b.4))
                .then(a.0.cmp(&b.0))
        }) else {
            return false;
        };
        for (&other, p) in &mut self.pending {
            if other != token {
                p.bypass = p.bypass.saturating_add(1);
            }
        }
        let pending = self.pending.remove(&token).unwrap();
        self.round = self.round.saturating_add(1);
        self.last_grant.insert(pending.request, self.round);
        let member = self.nodes.get_mut(&node).unwrap();
        if pending.compute {
            member.threads += 1;
        }
        if pending.node_allocation && !pending.compute {
            member.io_allocations += 1;
        }
        member.bytes += pending.bytes;
        self.allocated.insert(
            token,
            Allocation {
                request: pending.request,
                node,
                bytes: pending.bytes,
                compute: pending.compute,
                node_allocation: pending.node_allocation,
                retained: false,
            },
        );
        if pending.notify.send(node).is_err() {
            self.release(token);
        }
        true
    }
}
fn resource() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::LimitExceeded,
        "worker resource limit exceeded",
        Outcome::NotApplied,
    )
}

// Budget checks at the active pool job boundary. Outside a compute job, existing codec limits apply.
thread_local! {static TASK_LIMIT:std::cell::Cell<Option<u64>>=const {std::cell::Cell::new(None)};}
pub(crate) fn with_task_limit<T>(bytes: u64, work: impl FnOnce() -> T) -> T {
    struct Reset(Option<u64>);
    impl Drop for Reset {
        fn drop(&mut self) {
            TASK_LIMIT.with(|v| v.set(self.0));
        }
    }
    let reset = Reset(TASK_LIMIT.with(|v| v.replace(Some(bytes))));
    let result = work();
    drop(reset);
    result
}
#[cfg(feature = "olap")]
pub(crate) fn check_task_memory(bytes: u64) -> Result<(), ProtocolError> {
    if TASK_LIMIT.with(|limit| limit.get().is_some_and(|limit| bytes > limit)) {
        Err(resource())
    } else {
        Ok(())
    }
}
#[cfg(feature = "olap")]
pub(crate) fn task_memory_limit() -> Option<u64> {
    TASK_LIMIT.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, future::Future, pin::Pin, task::Poll};
    pub(super) fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        // The fixture explicitly advances one scheduler event at a time. Tokio's
        // cooperative budget must not turn an already sent grant into Pending.
        std::pin::pin!(tokio::task::unconstrained(future))
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }
    fn limits(threads: u64, memory: u64) -> WorkerResources {
        WorkerResources {
            max_threads: threads,
            memory_limit_bytes: memory,
            task_memory_limit_bytes: memory - 100,
            control_memory_reserve_bytes: 100,
        }
    }

    #[tokio::test]
    async fn c01_c02_heterogeneous_limits_and_impossible_work() {
        let scheduler = ResourceScheduler::new(vec![
            (1, limits(1, 1000)),
            (2, limits(2, 2000)),
            (3, limits(4, 4000)),
        ])
        .unwrap();
        let mut grants = Vec::new();
        for request in 0..7 {
            grants.push(scheduler.reserve(request, 1, 100).await.unwrap());
        }
        assert_eq!(
            scheduler
                .usage()
                .iter()
                .map(|(id, threads, _)| (*id, *threads))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (3, 4)]
        );
        assert!(scheduler.reserve(9, 3, 4000).await.is_err());
        assert!(ResourceScheduler::new(vec![(0, limits(0, 1000))]).is_err());
        assert!(ResourceScheduler::new(vec![(0, limits(u64::MAX, 1000))]).is_err());
        drop(grants);
        assert!(scheduler.usage().iter().all(|(_, t, b)| *t == 0 && *b == 0));
    }
    #[tokio::test]
    async fn c04_caps_include_reducers_across_hundred_jobs() {
        for cap in 1..=3 {
            let scheduler = ResourceScheduler::new(vec![
                (1, limits(1, 1000)),
                (2, limits(2, 2000)),
                (3, limits(4, 4000)),
            ])
            .unwrap();
            let mut grants = Vec::new();
            let mut completed = 0;
            while completed < 100 {
                let mut waiting = Box::pin(scheduler.reserve(42, cap, 100));
                match poll_once(waiting.as_mut()) {
                    Poll::Ready(Ok(grant)) => grants.push(grant),
                    Poll::Pending => {
                        drop(waiting);
                        drop(grants.remove(0));
                        completed += 1;
                    }
                    _ => panic!("eligible job rejected"),
                }
                assert!(
                    grants
                        .iter()
                        .map(|g| g.node_id())
                        .collect::<BTreeSet<_>>()
                        .len()
                        <= cap
                );
            }
            drop(grants);
            let reducer = scheduler.reserve(42, cap, 200).await.unwrap();
            assert!(reducer.node_id() > 0);
            drop(reducer);
            assert!(scheduler.usage().iter().all(|(_, t, b)| *t == 0 && *b == 0));
        }
    }
    fn grant_order(scheduler: &ResourceScheduler) -> Vec<u64> {
        let state = scheduler.shared.state.lock().unwrap();
        state.last_grant.keys().copied().collect()
    }
    /// FLAKE1 regression: grant order is kept only for requests that wait or hold
    /// work, so many single-use request ids leave no state behind.
    #[tokio::test]
    async fn c05_grant_order_is_bounded_by_live_requests() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, 1000))]).unwrap();
        for request in 1..=1000 {
            drop(scheduler.reserve(request, 1, 100).await.unwrap());
        }
        assert!(grant_order(&scheduler).is_empty());
        // A request that still holds a grant keeps its order while a sibling waits.
        let held = scheduler.reserve(7, 1, 100).await.unwrap();
        let other = scheduler.reserve(8, 1, 100).await.unwrap();
        let mut sibling = Box::pin(scheduler.reserve(7, 1, 100));
        assert!(poll_once(sibling.as_mut()).is_pending());
        drop(held);
        assert_eq!(grant_order(&scheduler), vec![7, 8]);
        let sibling = match poll_once(sibling.as_mut()) {
            Poll::Ready(Ok(grant)) => grant,
            _ => panic!("the sibling was not granted the freed slot"),
        };
        drop((sibling, other));
        assert!(grant_order(&scheduler).is_empty());
        // Cancelled waiters and impossible work leave nothing either.
        let full = scheduler.reserve(9, 2, 100).await.unwrap();
        let full2 = scheduler.reserve(10, 1, 100).await.unwrap();
        let mut waiter = Box::pin(scheduler.reserve(11, 1, 100));
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(waiter);
        assert!(scheduler.reserve(12, 1, 5000).await.is_err());
        drop((full, full2));
        assert!(grant_order(&scheduler).is_empty());
    }
    #[tokio::test]
    async fn c05_equal_peers_progress_with_one_slot_per_fair_round() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1000))]).unwrap();
        let first = scheduler.reserve(1, 1, 100).await.unwrap();
        let mut a = Box::pin(scheduler.reserve(1, 1, 100));
        let mut b = Box::pin(scheduler.reserve(2, 1, 100));
        let mut c = Box::pin(scheduler.reserve(3, 1, 100));
        assert!(poll_once(a.as_mut()).is_pending());
        assert!(poll_once(b.as_mut()).is_pending());
        assert!(poll_once(c.as_mut()).is_pending());
        drop(first);
        assert!(poll_once(a.as_mut()).is_pending());
        let b = match poll_once(b.as_mut()) {
            Poll::Ready(Ok(g)) => g,
            _ => panic!("second peer starved"),
        };
        drop(b);
        let c = match poll_once(c.as_mut()) {
            Poll::Ready(Ok(g)) => g,
            _ => panic!("third peer starved"),
        };
        drop(c);
        assert!(poll_once(a.as_mut()).is_ready());
    }
    #[tokio::test]
    async fn m02_queue_results_cpu_and_memory_share_one_ceiling() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(4, 1000))]).unwrap();
        let queue = scheduler.charge_memory(0, 200).unwrap();
        let result = scheduler.charge_memory(0, 100).unwrap();
        let first = scheduler.reserve(1, 1, 500).await.unwrap();
        let mut second = Box::pin(scheduler.reserve(2, 1, 200));
        assert!(poll_once(second.as_mut()).is_pending());
        assert_eq!(scheduler.usage(), vec![(0, 1, 800)]);
        assert!(scheduler.charge_memory(0, 101).is_err());
        drop(first);
        let second = second.await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 1, 500)]);
        drop(second);
        drop(queue);
        drop(result);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn m01_precharged_compute_and_reconfiguration_preserve_task_ceiling() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, 1000))]).unwrap();
        let mut smaller = limits(2, 1000);
        smaller.task_memory_limit_bytes = 200;
        scheduler.update_node(0, smaller).unwrap();
        let oversized = scheduler.charge_memory(0, 300).unwrap();
        assert!(scheduler.reserve_precharged(1, 1, oversized).await.is_err());
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
        let active = scheduler.reserve(2, 1, 200).await.unwrap();
        smaller.task_memory_limit_bytes = 100;
        assert!(scheduler.update_node(0, smaller).is_err());
        drop(active);
        scheduler.update_node(0, smaller).unwrap();
        assert!(scheduler.reserve(3, 1, 200).await.is_err());
    }
    #[tokio::test]
    async fn m03_aging_earmarks_memory_after_thirty_two_grants() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(4, 1000))]).unwrap();
        let resident = scheduler.reserve(1, 1, 700).await.unwrap();
        let mut large = Box::pin(scheduler.reserve(2, 1, 500));
        assert!(poll_once(large.as_mut()).is_pending());
        for request in 3..35 {
            let small = scheduler.reserve(request, 1, 100).await.unwrap();
            drop(small);
            assert!(poll_once(large.as_mut()).is_pending());
        }
        let mut bypass = Box::pin(scheduler.reserve(100, 1, 100));
        assert!(poll_once(bypass.as_mut()).is_pending());
        drop(resident);
        let large = large.await.unwrap();
        assert_eq!(large.bytes(), 500);
        drop(large);
        drop(bypass.await.unwrap());
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn c07_f03_memory_only_io_and_cancelled_grants_settle_once() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1000))]).unwrap();
        let memory = scheduler.reserve_memory(1, 0, 500).await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 0, 500)]);
        let compute = scheduler.reserve_precharged(1, 1, memory).await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 1, 500)]);
        let mut pending = Box::pin(scheduler.reserve(2, 1, 200));
        assert!(poll_once(pending.as_mut()).is_pending());
        let mut retained = compute.into_memory();
        assert_eq!(scheduler.usage(), vec![(0, 1, 700)]);
        // Cancellation after the scheduler sends a grant, before the waiter receives it.
        drop(pending);
        assert_eq!(scheduler.usage(), vec![(0, 0, 500)]);
        retained.shrink_to(100).unwrap();
        let reducer = scheduler
            .reserve_with_memories(1, 1, vec![retained], 500)
            .await
            .unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 1, 500)]);
        drop(reducer.into_memory());
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn c04_c07_remote_io_keeps_node_cap_without_holding_cpu() {
        let scheduler =
            ResourceScheduler::new(vec![(0, limits(1, 1000)), (1, limits(2, 2000))]).unwrap();
        let first = scheduler.reserve_remote_memory(1, 1, 1500).await.unwrap();
        assert_eq!(first.node_id(), 1);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0), (1, 0, 1500)]);
        let mut next = Box::pin(scheduler.reserve_remote_memory(1, 1, 600));
        assert!(
            poll_once(next.as_mut()).is_pending(),
            "request cap forbids allocating another node during child IO"
        );
        let unrelated = scheduler.reserve_on(2, 1, 1, 100).await.unwrap();
        assert_eq!(
            unrelated.node_id(),
            1,
            "IO leaves compute capacity available for unrelated work"
        );
        drop(unrelated);
        drop(first);
        let next = next.await.unwrap();
        assert_eq!(
            scheduler
                .usage()
                .iter()
                .map(|(_, threads, _)| threads)
                .sum::<u64>(),
            0
        );
        drop(next);
        assert!(
            scheduler
                .usage()
                .iter()
                .all(|(_, threads, bytes)| *threads == 0 && *bytes == 0)
        );
    }
    #[tokio::test]
    async fn c07_remote_io_count_is_bounded_across_requests_and_refills() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1000))]).unwrap();
        scheduler.set_io_capacity(0, 2).unwrap();
        let a = scheduler.reserve_remote_memory(1, 1, 100).await.unwrap();
        let b = scheduler.reserve_remote_memory(2, 1, 100).await.unwrap();
        let mut c = Box::pin(scheduler.reserve_remote_memory(3, 1, 100));
        assert!(poll_once(c.as_mut()).is_pending());
        assert!(scheduler.set_io_capacity(0, 1).is_err());
        let cpu = scheduler.reserve(4, 1, 50).await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 1, 250)]);
        drop(cpu);
        drop(a);
        let c = c.await.unwrap();
        assert_eq!(
            scheduler.shared.state.lock().unwrap().nodes[&0].io_allocations,
            2
        );
        drop((b, c));
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
        assert_eq!(
            scheduler.shared.state.lock().unwrap().nodes[&0].io_allocations,
            0
        );
    }
    #[tokio::test]
    async fn c03_drain_preserves_owned_work_and_reconfigure_reuses_node() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1000))]).unwrap();
        let active = scheduler.reserve(1, 1, 200).await.unwrap();
        let mut pending = Box::pin(scheduler.reserve(2, 1, 200));
        assert!(poll_once(pending.as_mut()).is_pending());
        scheduler.drain_node(0).unwrap();
        assert!(pending.await.is_err());
        assert_eq!(scheduler.usage(), vec![(0, 1, 200)]);
        drop(active);
        scheduler.update_node(0, limits(2, 2000)).unwrap();
        let a = scheduler.reserve(1, 1, 200).await.unwrap();
        let b = scheduler.reserve(2, 1, 200).await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 2, 400)]);
        drop((a, b));
    }
    #[tokio::test]
    async fn m03_f03_retained_growth_rejects_cycles_but_fresh_waiters_progress() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, 1000))]).unwrap();
        let left = scheduler.reserve(1, 1, 400).await.unwrap().into_memory();
        let right = scheduler.reserve(2, 1, 400).await.unwrap().into_memory();
        assert!(scheduler.reserve_memory(1, 0, 500).await.is_err());
        // A reducer uses a new request ID and still must not wait on held output.
        assert!(
            scheduler
                .reserve_with_memories(3, 1, vec![left], 800)
                .await
                .is_err()
        );
        let mut fresh = Box::pin(scheduler.reserve_memory(4, 0, 800));
        assert!(poll_once(fresh.as_mut()).is_pending());
        drop(right);
        let fresh = fresh.await.unwrap();
        assert_eq!(scheduler.usage(), vec![(0, 0, 800)]);
        drop(fresh);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn m03_waiters_registered_before_first_grant_cannot_form_a_cycle() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, 1000))]).unwrap();
        let blocker = scheduler.reserve_memory(99, 0, 900).await.unwrap();
        let mut a = Box::pin(scheduler.reserve_memory(1, 0, 600));
        let mut b = Box::pin(scheduler.reserve_memory(1, 0, 600));
        assert!(poll_once(a.as_mut()).is_pending());
        assert!(poll_once(b.as_mut()).is_pending());
        drop(blocker);
        let a = a.await.unwrap();
        // In-flight inputs may await temporary sibling memory. Only retained
        // output creates a dependency that can require reduction to release it.
        assert!(poll_once(b.as_mut()).is_pending());
        let a = scheduler
            .reserve_precharged(1, 1, a)
            .await
            .unwrap()
            .into_memory();
        assert!(b.await.is_err());
        drop(a);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn m03_same_request_can_wait_for_in_flight_sibling_memory() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, 1000))]).unwrap();
        let active = scheduler.reserve(1, 1, 700).await.unwrap();
        let mut input = Box::pin(scheduler.reserve_memory(1, 0, 500));
        assert!(poll_once(input.as_mut()).is_pending());
        drop(active);
        drop(input.await.unwrap());
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn f03_close_wakes_memory_only_and_cpu_waiters() {
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1000))]).unwrap();
        let active = scheduler.reserve(1, 1, 900).await.unwrap();
        let mut memory = Box::pin(scheduler.reserve_memory(2, 0, 100));
        let mut cpu = Box::pin(scheduler.reserve(3, 1, 0));
        assert!(poll_once(memory.as_mut()).is_pending());
        assert!(poll_once(cpu.as_mut()).is_pending());
        scheduler.close();
        assert!(memory.await.is_err());
        assert!(cpu.await.is_err());
        assert!(scheduler.reserve_memory(4, 0, 1).await.is_err());
        assert!(scheduler.charge_memory(0, 1).is_err());
        drop(active);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn u03_seed_343687_conserves_resources_after_every_event() {
        let scheduler = ResourceScheduler::new(vec![
            (0, limits(1, 1000)),
            (1, limits(2, 2000)),
            (2, limits(4, 4000)),
        ])
        .unwrap();
        type Pending = Pin<Box<dyn Future<Output = Result<Reservation, ProtocolError>> + Send>>;
        let mut waiting: Vec<(u64, Pending)> = Vec::new();
        let mut active: Vec<(u64, Reservation)> = Vec::new();
        let mut seed = 343687u64;
        for _ in 0..2000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            match seed % 4 {
                0 | 1 => {
                    let request = seed % 8;
                    let copy = scheduler.clone();
                    waiting.push((
                        request,
                        Box::pin(async move {
                            copy.reserve(request, (request % 3 + 1) as usize, seed % 150 + 1)
                                .await
                        }),
                    ));
                }
                2 if !active.is_empty() => {
                    active.swap_remove(seed as usize % active.len());
                }
                3 if !waiting.is_empty() => {
                    drop(waiting.swap_remove(seed as usize % waiting.len()));
                }
                _ => {}
            }
            let mut i = 0;
            while i < waiting.len() {
                match poll_once(waiting[i].1.as_mut()) {
                    Poll::Pending => i += 1,
                    Poll::Ready(result) => {
                        let (request, _) = waiting.swap_remove(i);
                        active.push((request, result.unwrap()));
                    }
                }
            }
            for (id, threads, bytes) in scheduler.usage() {
                let node = limits([1, 2, 4][id as usize], [1000, 2000, 4000][id as usize]);
                assert!(threads <= node.max_threads && bytes <= node.working_bytes());
                assert_eq!(
                    threads,
                    active.iter().filter(|(_, g)| g.node_id() == id).count() as u64
                );
                assert_eq!(
                    bytes,
                    active
                        .iter()
                        .filter(|(_, g)| g.node_id() == id)
                        .map(|(_, g)| g.bytes())
                        .sum::<u64>()
                );
            }
            for request in 0..8 {
                assert!(
                    active
                        .iter()
                        .filter(|(r, _)| *r == request)
                        .map(|(_, g)| g.node_id())
                        .collect::<BTreeSet<_>>()
                        .len()
                        <= (request % 3 + 1) as usize
                );
            }
        }
        drop(waiting);
        drop(active);
        assert!(scheduler.usage().iter().all(|(_, t, b)| *t == 0 && *b == 0));
    }
    #[tokio::test]
    async fn u03_seed_343687_membership_memory_drain_and_retry_conservation() {
        let mut configured: BTreeMap<_, _> = [
            (0, limits(1, 1000)),
            (1, limits(2, 2000)),
            (2, limits(4, 4000)),
        ]
        .into();
        let scheduler =
            ResourceScheduler::new(configured.iter().map(|(id, node)| (*id, *node)).collect())
                .unwrap();
        type Pending = Pin<Box<dyn Future<Output = Result<Reservation, ProtocolError>> + Send>>;
        let mut waiting: Vec<(u64, Pending)> = Vec::new();
        let mut active: Vec<(u64, Reservation)> = Vec::new();
        let mut external: Vec<MemoryCharge> = Vec::new();
        let mut seed = 343687u64;
        let mut events = [0usize; 10];
        for step in 0..2000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let event = seed as usize % 10;
            events[event] += 1;
            match event {
                0..=2 => {
                    let request = seed % 8;
                    let copy = scheduler.clone();
                    waiting.push((
                        request,
                        Box::pin(async move {
                            copy.reserve(request, (request % 3 + 1) as usize, seed % 450 + 1)
                                .await
                        }),
                    ));
                }
                3 if !active.is_empty() => {
                    active.swap_remove(seed as usize % active.len());
                }
                4 if !waiting.is_empty() => {
                    drop(waiting.swap_remove(seed as usize % waiting.len()));
                }
                5 => {
                    if let Ok(charge) = scheduler.charge_memory(seed % 4, seed % 200 + 1) {
                        external.push(charge);
                    }
                }
                6 if !external.is_empty() => {
                    external.swap_remove(seed as usize % external.len());
                }
                7 => {
                    let _ = scheduler.drain_node(seed % 4);
                }
                8 | 9 => {
                    let id = seed % 4;
                    let node = limits(id % 3 + 1, (id + 1) * 1000);
                    if scheduler.update_node(id, node).is_ok() {
                        configured.insert(id, node);
                    }
                }
                _ => {}
            }
            let mut index = 0;
            while index < waiting.len() {
                match poll_once(waiting[index].1.as_mut()) {
                    Poll::Pending => index += 1,
                    Poll::Ready(result) => {
                        let (request, _) = waiting.swap_remove(index);
                        match result {
                            Ok(grant) => active.push((request, grant)),
                            Err(error) => assert_eq!(error.code, ErrorCode::LimitExceeded),
                        }
                    }
                }
            }
            for (node, threads, bytes) in scheduler.usage() {
                let limits = configured[&node];
                assert!(
                    threads <= limits.max_threads && bytes <= limits.working_bytes(),
                    "step {step} seed {seed}"
                );
                let expected_threads = active
                    .iter()
                    .filter(|(_, grant)| grant.node_id() == node)
                    .count() as u64;
                let expected_bytes = active
                    .iter()
                    .filter(|(_, grant)| grant.node_id() == node)
                    .map(|(_, grant)| grant.bytes())
                    .sum::<u64>()
                    + external
                        .iter()
                        .filter(|charge| charge.node_id() == node)
                        .map(MemoryCharge::bytes)
                        .sum::<u64>();
                assert_eq!(
                    (threads, bytes),
                    (expected_threads, expected_bytes),
                    "step {step} seed {seed}"
                );
            }
            for request in 0..8 {
                let nodes: BTreeSet<_> = active
                    .iter()
                    .filter(|(id, _)| *id == request)
                    .map(|(_, grant)| grant.node_id())
                    .collect();
                assert!(
                    nodes.len() <= (request % 3 + 1) as usize,
                    "step {step} seed {seed}"
                );
            }
        }
        assert!(events.iter().all(|count| *count > 0));
        drop(waiting);
        drop(active);
        drop(external);
        assert!(
            scheduler
                .usage()
                .iter()
                .all(|(_, threads, bytes)| *threads == 0 && *bytes == 0)
        );
    }
}

#[cfg(test)]
mod coordinator_stage_tests {
    use super::*;
    use std::{future::Future, task::Poll};
    #[tokio::test]
    async fn q01_thousand_body_owners_and_coordinator_waiters_preserve_order_and_bytes() {
        let scheduler = ResourceScheduler::new(vec![(
            0,
            WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 1 << 20,
                task_memory_limit_bytes: 900_000,
                control_memory_reserve_bytes: 100,
            },
        )])
        .unwrap();
        // Accepted bodies share their queue accounting identity. Each coordinator
        // stage has its own group and only one stage fits alongside those bodies.
        let mut bodies = Vec::new();
        for _ in 0..1000 {
            bodies.push(scheduler.reserve_memory(0, 0, 64).await.unwrap());
        }
        let mut current = Some(
            scheduler
                .reserve_coordinator_memory(1, 0, 600_000, 100_000)
                .await
                .unwrap(),
        );
        let mut waiters = Vec::new();
        for request in 2..=1000 {
            let mut waiter =
                Box::pin(scheduler.reserve_coordinator_memory(request, 0, 600_000, 100_000));
            assert!(super::tests::poll_once(waiter.as_mut()).is_pending());
            waiters.push(waiter);
        }
        for mut waiter in waiters {
            drop(current.take());
            current = Some(match super::tests::poll_once(waiter.as_mut()) {
                Poll::Ready(Ok(charge)) => charge,
                _ => panic!("oldest eligible coordinator did not receive the freed memory"),
            });
            assert_eq!(scheduler.usage(), vec![(0, 0, 664_000)]);
        }
        drop(current);
        assert_eq!(scheduler.usage(), vec![(0, 0, 64_000)]);
        drop(bodies);
        assert_eq!(scheduler.usage(), vec![(0, 0, 0)]);
    }
    #[tokio::test]
    async fn q01_m02_coordinator_stages_wait_without_cpu_and_preserve_child_ingress_headroom() {
        let scheduler = ResourceScheduler::new(vec![(
            0,
            WorkerResources {
                max_threads: 2,
                memory_limit_bytes: 4096,
                task_memory_limit_bytes: 256,
                control_memory_reserve_bytes: 1024,
            },
        )])
        .unwrap();
        let base = scheduler.charge_memory(0, 200).unwrap();
        let first = scheduler
            .reserve_coordinator_memory(1, 0, 2000, 700)
            .await
            .unwrap();
        let mut waiting = Box::pin(scheduler.reserve_coordinator_memory(2, 0, 2000, 700));
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        assert_eq!(scheduler.usage()[0], (0, 0, 2200));
        let child = scheduler.reserve_on(3, 1, 0, 256).await.unwrap();
        let ingress = scheduler.charge_memory(0, 512).unwrap();
        assert_eq!(scheduler.usage()[0], (0, 1, 2968));
        drop((child, ingress, first));
        let second = waiting.await.unwrap();
        assert_eq!(scheduler.usage()[0], (0, 0, 2200));
        drop((base, second));
        assert_eq!(scheduler.usage()[0], (0, 0, 0));
    }
}
