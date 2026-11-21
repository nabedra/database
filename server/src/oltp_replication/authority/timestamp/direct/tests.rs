//! O09 and D06 (unit level): direct allocation stays monotonic across queued
//! allocations, window discards, a Timestamp leader change and coalescing.
//!
//! `Cluster` applies every proposal to real Timestamp machines on separate
//! pools, in log order, as committed Raft entries would be. A leader change
//! appends a blank entry in the new term, as openraft does.
use super::super::tests::{Fixture, ready, worker};
use super::super::*;
use super::*;
use crate::oltp::pool::ByteClass;
use crate::oltp_replication::{
    authority::consensus::{Applied, Consensus, ReadBarrier},
    owned::{OwnedCommand, serialized_digest},
};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use uuid::Uuid;

struct Node {
    fx: Fixture,
    applied: Mutex<AppliedPosition>,
}
impl Node {
    async fn apply_at(
        &self,
        position: AppliedPosition,
        command: TimestampCommand,
    ) -> Result<TimestampReply, ProtocolError> {
        let command =
            OwnedCommand::new_owned(command, self.fx.pool.resources(), ByteClass::Progress)?;
        let meta = crate::oltp::pool::ApplyMeta {
            position,
            command_digest: serialized_digest(&command)?,
            membership: None,
        };
        let reply = self.fx.machine.apply(meta, command).await?;
        *self.applied.lock().unwrap() = position;
        Ok(serde_json::from_slice(reply.as_slice()).unwrap())
    }
    /// A new leader's blank entry, applied the way the typed Raft host does.
    async fn apply_blank(&self, position: AppliedPosition) {
        let meta = crate::oltp::pool::ApplyMeta {
            position,
            command_digest: Digest::from_bytes([0xb1; 32]),
            membership: None,
        };
        self.fx
            .machine
            .store()
            .transaction(meta, BUDGET, |tx| {
                Ok(crate::oltp::pool::Transition::Commit(
                    tx.copy_output(b"null")?,
                ))
            })
            .await
            .unwrap();
        *self.applied.lock().unwrap() = position;
    }
}
struct LogState {
    term: u64,
    leader: usize,
    index: u64,
    connected: Vec<bool>,
    missed: Vec<Vec<(AppliedPosition, Option<TimestampCommand>)>>,
    proposals: Vec<String>,
}
struct Cluster {
    nodes: Vec<Node>,
    log: tokio::sync::Mutex<LogState>,
    /// While set, proposals never complete, like a leader partitioned after
    /// passing its read barrier.
    stalled: AtomicBool,
    /// Barriers requested as ReadIndex and as lease reads (PERF1).
    read_index: AtomicUsize,
    lease_reads: AtomicUsize,
}
impl Cluster {
    async fn new(size: usize) -> Arc<Self> {
        let mut nodes = Vec::new();
        for _ in 0..size {
            nodes.push(Node {
                fx: Fixture::new().await,
                applied: Mutex::new(AppliedPosition {
                    term: 1,
                    node_id: 1,
                    index: 0,
                }),
            });
        }
        Arc::new(Self {
            nodes,
            log: tokio::sync::Mutex::new(LogState {
                term: 1,
                leader: 0,
                index: 0,
                connected: vec![true; size],
                missed: vec![Vec::new(); size],
                proposals: Vec::new(),
            }),
            stalled: AtomicBool::new(false),
            read_index: AtomicUsize::new(0),
            lease_reads: AtomicUsize::new(0),
        })
    }
    /// The handle gets its own `TimestampMachine` over the node's store, as
    /// `bootstrap.rs` builds the service handle beside the hosted machine.
    fn handle(self: &Arc<Self>, node: usize) -> Arc<TimestampHandle> {
        TimestampHandle::new(
            TimestampMachine::new(self.nodes[node].fx.machine.store().clone()).unwrap(),
            Arc::new(ModelConsensus {
                cluster: self.clone(),
                node,
            }),
        )
    }
    async fn elect(&self, leader: usize) {
        let mut log = self.log.lock().await;
        assert!(log.connected[leader]);
        log.term += 1;
        log.index += 1;
        log.leader = leader;
        let position = AppliedPosition {
            term: log.term,
            node_id: leader as u64 + 1,
            index: log.index,
        };
        for (index, node) in self.nodes.iter().enumerate() {
            if log.connected[index] {
                node.apply_blank(position).await;
            } else {
                log.missed[index].push((position, None));
            }
        }
    }
    async fn disconnect(&self, node: usize) {
        self.log.lock().await.connected[node] = false;
    }
    /// Catch up by applying the missed committed entries in order.
    async fn reconnect(&self, node: usize) {
        let mut log = self.log.lock().await;
        for (position, command) in std::mem::take(&mut log.missed[node]) {
            match command {
                Some(command) => {
                    self.nodes[node].apply_at(position, command).await.unwrap();
                }
                None => self.nodes[node].apply_blank(position).await,
            }
        }
        log.connected[node] = true;
    }
    /// Commit an entry from the current leader and apply it through each
    /// node's hosted machine only, never through a handle.
    async fn commit_external(&self, command: TimestampCommand) -> TimestampReply {
        let mut log = self.log.lock().await;
        log.index += 1;
        let position = AppliedPosition {
            term: log.term,
            node_id: log.leader as u64 + 1,
            index: log.index,
        };
        let mut reply = None;
        for (index, node) in self.nodes.iter().enumerate() {
            let applied = node.apply_at(position, command.clone()).await.unwrap();
            if index == log.leader {
                reply = Some(applied);
            }
        }
        reply.unwrap()
    }
    async fn reserve_window_proposals(&self) -> usize {
        self.log
            .lock()
            .await
            .proposals
            .iter()
            .filter(|name| *name == "timestamp.reserve_window")
            .count()
    }
}
struct ModelConsensus {
    cluster: Arc<Cluster>,
    node: usize,
}
fn not_leader() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "not leader",
        Outcome::Unknown,
    )
}
impl Consensus<TimestampCommand, TimestampReply> for ModelConsensus {
    fn propose(
        &self,
        command: OwnedCommand<TimestampCommand>,
    ) -> ConsensusFuture<'_, Applied<TimestampReply>> {
        Box::pin(async move {
            if self.cluster.stalled.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            let mut log = self.cluster.log.lock().await;
            if log.leader != self.node || !log.connected[self.node] {
                return Err(not_leader());
            }
            log.index += 1;
            let position = AppliedPosition {
                term: log.term,
                node_id: self.node as u64 + 1,
                index: log.index,
            };
            let payload = command.payload()?.clone();
            log.proposals.push(payload.to_string());
            let mut reply = None;
            for (index, node) in self.cluster.nodes.iter().enumerate() {
                if !log.connected[index] {
                    log.missed[index].push((position, Some(payload.clone())));
                    continue;
                }
                let applied = node.apply_at(position, payload.clone()).await?;
                if index == self.node {
                    reply = Some(applied);
                }
            }
            let owner = self.cluster.nodes[self.node]
                .fx
                .pool
                .resources()
                .reserve_scratch_wait(ByteClass::Progress, 64, 0)
                .await?;
            Ok(Applied::owned(position, reply.unwrap(), Arc::new(owner)))
        })
    }
    fn ensure_linearizable(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.cluster.read_index.fetch_add(1, Ordering::SeqCst);
        self.barrier()
    }
    fn ensure_linearizable_lease(&self) -> ConsensusFuture<'_, ReadBarrier> {
        self.cluster.lease_reads.fetch_add(1, Ordering::SeqCst);
        self.barrier()
    }
}
impl ModelConsensus {
    fn barrier(&self) -> ConsensusFuture<'_, ReadBarrier> {
        Box::pin(async move {
            let log = self.cluster.log.lock().await;
            if log.leader != self.node || !log.connected[self.node] {
                return Err(not_leader());
            }
            Ok(ReadBarrier {
                applied: *self.cluster.nodes[self.node].applied.lock().unwrap(),
            })
        })
    }
}

struct Gate(AtomicBool);
impl DirectTimestampGate for Gate {
    fn reserve_window_supported<'a>(&'a self, _: &'a GroupIdentity) -> ConsensusFuture<'a, bool> {
        Box::pin(async move { Ok(self.0.load(Ordering::SeqCst)) })
    }
}
fn open_gate() -> Arc<Gate> {
    Arc::new(Gate(AtomicBool::new(true)))
}

/// Initialize the Timestamp authority through the current leader.
async fn initialize(handle: &TimestampHandle) -> Authority {
    let ready = ready(worker());
    let authority = ready.view.authority();
    let applied = handle
        .fence(Uuid::new_v4(), None, authority, ready)
        .await
        .unwrap();
    assert!(matches!(applied.reply, TimestampReply::Fenced { .. }));
    authority
}
/// A queued allocation that moves `high_watermark`: a backup snapshot pin.
async fn queued(handle: &TimestampHandle, authority: Authority) -> u64 {
    handle
        .begin_backup(
            authority,
            crate::oltp::backup::BackupOwnerRef {
                backup_id: Uuid::new_v4(),
                attempt: 1,
            },
        )
        .await
        .unwrap()
        .read_ts
        .get()
}

#[tokio::test]
async fn o09_d06_direct_and_queued_allocations_interleave_monotonically() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    handle.set_direct_window_size(8).unwrap();
    let mut last = 0;
    let mut seed = 0x2026_1119_u64;
    for _ in 0..200 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let (low, high) = if seed >> 61 == 0 {
            let value = queued(&handle, authority).await;
            (value, value)
        } else {
            let count = (seed >> 33) % 3 + 1;
            let range = handle.allocate_direct(count).await.unwrap();
            assert_eq!(range.len(), count);
            (range.first().get(), range.last().get())
        };
        assert!(low > last, "allocation {low} is not above completed {last}");
        last = high;
    }
    let stats = handle.direct_window_stats();
    assert!(stats.reservations > 1);
    assert!(
        stats.discards > 0,
        "queued allocations must discard windows"
    );
}

#[tokio::test]
async fn o09_d06_watermark_move_discards_the_rest_of_the_window() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    let a = handle.allocate_direct(1).await.unwrap().first().get();
    let b = handle.allocate_direct(2).await.unwrap();
    assert_eq!(b.first().get(), a + 1, "values come from one window");
    assert_eq!(handle.direct_window_stats().reservations, 1);
    let q = queued(&handle, authority).await;
    assert_eq!(
        q,
        a + DEFAULT_DIRECT_WINDOW,
        "the window is durable watermark"
    );
    assert_eq!(handle.direct_window_stats().discards, 1);
    let c = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(c > q, "direct value {c} after queued {q} must be higher");
    assert_eq!(handle.direct_window_stats().reservations, 2);
    assert_eq!(cluster.reserve_window_proposals().await, 2);
}

#[tokio::test]
async fn o09_d06_raced_reservation_serves_its_head_and_installs_nothing() {
    let mut f = Fixture::new().await;
    let ready = ready(worker());
    let authority = ready.view.authority();
    f.initialize(ready).await;
    let reserve = TimestampCommand::ReserveWindow {
        authority,
        count: 16,
    };
    let TimestampReply::Window { first, last, .. } = f.apply(reserve.clone()).await else {
        panic!("expected a window")
    };
    assert_eq!(last.get() - first.get(), 15);
    f.apply(TimestampCommand::BeginBackup {
        authority,
        owner: crate::oltp::backup::BackupOwnerRef {
            backup_id: Uuid::new_v4(),
            attempt: 1,
        },
        pin_id: Uuid::new_v4(),
    })
    .await;
    let windows = f.machine.direct.clone();
    let leader = (1, 1);
    // The caller still gets the head of its reservation; the rest is skipped.
    let head = windows
        .install_and_take(leader, authority, first.get(), last.get(), 2)
        .unwrap();
    assert_eq!((head.first(), head.len()), (first, 2));
    assert_eq!(windows.stats().raced, 1);
    assert_eq!(windows.take(leader, 1), None, "nothing was installed");
    let TimestampReply::Window { first, last, .. } = f.apply(reserve).await else {
        panic!("expected a window")
    };
    let taken = windows
        .install_and_take(leader, authority, first.get(), last.get(), 1)
        .unwrap();
    assert_eq!(taken.first(), first);
    // A caller whose barrier saw an older leader leaves the newer window alone.
    assert_eq!(windows.take((1, 0), 1), None);
    assert_eq!(
        windows.take(leader, 1).unwrap().first().get(),
        first.get() + 1
    );
    // Another leader in the same term (advanced leader IDs) cannot use it.
    assert_eq!(windows.take((1, 2), 1), None);
    assert_eq!(
        windows.take(leader, 1),
        None,
        "the stale window was discarded"
    );
    assert_eq!(windows.stats().discards, 1);
}

#[tokio::test]
async fn o09_reserve_window_validates_authority_count_and_initialization() {
    let mut f = Fixture::new().await;
    let ready = ready(worker());
    let authority = ready.view.authority();
    assert_eq!(
        f.apply(TimestampCommand::ReserveWindow {
            authority,
            count: 1
        })
        .await,
        TimestampReply::Rejected(Fault::NotInitialized)
    );
    f.initialize(ready).await;
    let stale = Authority {
        run_id: Uuid::new_v4(),
        owner_term: authority.owner_term,
    };
    assert_eq!(
        f.apply(TimestampCommand::ReserveWindow {
            authority: stale,
            count: 1
        })
        .await,
        TimestampReply::Rejected(Fault::StaleAuthority)
    );
    for count in [0, MAX_DIRECT_WINDOW + 1] {
        assert_eq!(
            f.apply(TimestampCommand::ReserveWindow { authority, count })
                .await,
            TimestampReply::Rejected(Fault::InvalidBinding)
        );
    }
    let TimestampReply::Window { first, last, .. } = f
        .apply(TimestampCommand::ReserveWindow {
            authority,
            count: 100,
        })
        .await
    else {
        panic!("expected a window")
    };
    assert_eq!((first.get(), last.get()), (1, 100));
    // Reserved values are durable: the next queued allocation is above them.
    let TimestampReply::Snapshot(pin) = f
        .apply(TimestampCommand::BeginBackup {
            authority,
            owner: crate::oltp::backup::BackupOwnerRef {
                backup_id: Uuid::new_v4(),
                attempt: 1,
            },
            pin_id: Uuid::new_v4(),
        })
        .await
    else {
        panic!("expected a pin")
    };
    assert_eq!(pin.read_ts.get(), 101);
}

#[tokio::test]
async fn o09_d06_leader_change_never_reissues_old_window_values() {
    let cluster = Cluster::new(3).await;
    let handles: Vec<_> = (0..3).map(|node| cluster.handle(node)).collect();
    for handle in &handles {
        handle.install_direct_gate(open_gate());
    }
    let authority = initialize(&handles[0]).await;
    let a = handles[0].allocate_direct(1).await.unwrap().last().get();
    cluster.elect(1).await;
    assert!(
        handles[0].allocate_direct(1).await.is_err(),
        "a deposed leader must not hand out its window"
    );
    let b = handles[1].allocate_direct(1).await.unwrap().first().get();
    assert!(
        b > a + DEFAULT_DIRECT_WINDOW - 1,
        "new leader value {b} must be above the old window ending at {}",
        a + DEFAULT_DIRECT_WINDOW - 1
    );
    let q = queued(&handles[1], authority).await;
    assert!(q > b);
    cluster.elect(0).await;
    let c = handles[0].allocate_direct(1).await.unwrap().first().get();
    assert!(c > q, "resumed leader value {c} must be above {q}");
    assert_eq!(handles[0].direct_window_stats().reservations, 2);
}

#[tokio::test]
async fn o09_d06_regained_leadership_reserves_a_new_window_by_term() {
    // No watermark moves while node 0 is deposed, so only the term check can
    // retire its old window.
    let cluster = Cluster::new(2).await;
    let handles: Vec<_> = (0..2).map(|node| cluster.handle(node)).collect();
    for handle in &handles {
        handle.install_direct_gate(open_gate());
    }
    initialize(&handles[0]).await;
    let a = handles[0].allocate_direct(1).await.unwrap().first().get();
    cluster.elect(1).await;
    cluster.elect(0).await;
    let c = handles[0].allocate_direct(1).await.unwrap().first().get();
    assert_eq!(c, a + DEFAULT_DIRECT_WINDOW);
    assert_eq!(handles[0].direct_window_stats().reservations, 2);
}

#[tokio::test]
async fn o09_d06_partitioned_old_leader_catches_up_before_serving() {
    let cluster = Cluster::new(3).await;
    let handles: Vec<_> = (0..3).map(|node| cluster.handle(node)).collect();
    for handle in &handles {
        handle.install_direct_gate(open_gate());
    }
    let authority = initialize(&handles[0]).await;
    let a = handles[0].allocate_direct(1).await.unwrap().first().get();
    cluster.disconnect(0).await;
    cluster.elect(1).await;
    let b = handles[1].allocate_direct(1).await.unwrap().first().get();
    let q = queued(&handles[1], authority).await;
    assert!(a < b && b < q);
    assert!(handles[0].allocate_direct(1).await.is_err());
    cluster.reconnect(0).await;
    cluster.elect(0).await;
    let c = handles[0].allocate_direct(1).await.unwrap().first().get();
    assert!(c > q, "caught-up leader value {c} must be above {q}");
}

#[tokio::test]
async fn o02_o09_reserve_window_waits_for_the_mixed_version_gate() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    initialize(&handle).await;
    let error = handle.allocate_direct(1).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::UnsupportedOperation);
    assert_eq!(error.outcome, Outcome::NotApplied);
    let gate = Arc::new(Gate(AtomicBool::new(false)));
    handle.install_direct_gate(gate.clone());
    let error = handle.allocate_direct(1).await.unwrap_err();
    assert_eq!(error.outcome, Outcome::NotApplied);
    assert_eq!(cluster.reserve_window_proposals().await, 0);
    gate.0.store(true, Ordering::SeqCst);
    handle.set_direct_window_size(2).unwrap();
    let first = handle.allocate_direct(1).await.unwrap().first().get();
    assert_eq!(cluster.reserve_window_proposals().await, 1);
    // Closing the gate stops new reservations; the reserved window is served.
    gate.0.store(false, Ordering::SeqCst);
    assert_eq!(
        handle.allocate_direct(1).await.unwrap().first().get(),
        first + 1
    );
    assert!(handle.allocate_direct(1).await.is_err());
    assert_eq!(cluster.reserve_window_proposals().await, 1);
}

#[tokio::test]
async fn o09_direct_allocation_rejects_invalid_counts() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    for count in [0, MAX_DIRECT_COUNT + 1] {
        assert_eq!(
            handle.allocate_direct(count).await.unwrap_err().code,
            ErrorCode::ValidationError
        );
    }
    assert!(handle.set_direct_window_size(0).is_err());
    assert!(
        handle
            .set_direct_window_size(MAX_DIRECT_WINDOW + 1)
            .is_err()
    );
    // A request larger than the configured window reserves exactly enough.
    handle.set_direct_window_size(4).unwrap();
    let range = handle.allocate_direct(10).await.unwrap();
    assert_eq!(range.len(), 10);
}

/// Remote/local source model for the coalescing client.
struct Source {
    next: Mutex<u64>,
    calls: Mutex<Vec<u64>>,
    hold: tokio::sync::Semaphore,
    held: AtomicBool,
    fail: AtomicBool,
    panic: AtomicBool,
    entered: AtomicUsize,
}
impl Source {
    fn new(held: bool) -> Arc<Self> {
        Arc::new(Self {
            next: Mutex::new(1),
            calls: Mutex::new(Vec::new()),
            hold: tokio::sync::Semaphore::new(0),
            held: AtomicBool::new(held),
            fail: AtomicBool::new(false),
            panic: AtomicBool::new(false),
            entered: AtomicUsize::new(0),
        })
    }
    fn calls(&self) -> Vec<u64> {
        self.calls.lock().unwrap().clone()
    }
}
impl DirectTimestampSource for Source {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(count);
            self.entered.fetch_add(1, Ordering::SeqCst);
            if self.held.load(Ordering::SeqCst) {
                self.hold.acquire().await.unwrap().forget();
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(not_leader());
            }
            assert!(!self.panic.load(Ordering::SeqCst), "modelled driver panic");
            let mut next = self.next.lock().unwrap();
            let range = DirectTimestampRange::from_bounds(*next, *next + count - 1)?;
            *next += count;
            Ok(range)
        })
    }
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition not reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o09_coalescing_client_shares_one_call_among_concurrent_callers() {
    let remote = Source::new(true);
    let client = DirectTimestampClient::new(None, remote.clone());
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.allocate(1).await }
    });
    until(|| remote.entered.load(Ordering::SeqCst) == 1).await;
    let callers: Vec<_> = (0..16u64)
        .map(|index| {
            let client = client.clone();
            tokio::spawn(async move { client.allocate(index % 3 + 1).await })
        })
        .collect();
    until(|| client.waiting() == 16).await;
    remote.hold.add_permits(2);
    let first = first.await.unwrap().unwrap();
    let mut ranges = vec![first];
    for caller in callers {
        ranges.push(caller.await.unwrap().unwrap());
    }
    let total: u64 = (0..16u64).map(|index| index % 3 + 1).sum();
    assert_eq!(remote.calls(), vec![1, total], "16 waiters share one call");
    assert_eq!(client.calls(), 2);
    let mut values: Vec<_> = ranges.iter().flat_map(|range| range.iter()).collect();
    let count = values.len();
    values.sort();
    values.dedup();
    assert_eq!(values.len(), count, "coalesced ranges must not overlap");
    assert!(ranges[1..].iter().all(|range| range.first() > first.last()));
}

#[tokio::test]
async fn o09_coalescing_client_never_serves_a_later_caller_from_an_earlier_call() {
    let remote = Source::new(false);
    let client = DirectTimestampClient::new(None, remote.clone());
    let mut last = 0;
    for _ in 0..50 {
        let value = client.commit_ts().await.unwrap().get();
        assert!(value > last);
        last = value;
    }
    assert_eq!(client.calls(), 50, "no values are cached across calls");
}

#[tokio::test]
async fn o09_coalescing_client_calls_a_colocated_leader_in_process() {
    let local = Source::new(false);
    let remote = Source::new(false);
    let client = DirectTimestampClient::new(Some(local.clone()), remote.clone());
    for _ in 0..3 {
        client.commit_ts().await.unwrap();
    }
    assert_eq!(local.calls().len(), 3);
    assert!(remote.calls().is_empty());
    assert_eq!(client.local_calls(), 3);
    local.fail.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        client.commit_ts().await.unwrap();
    }
    assert_eq!(local.calls().len(), 4, "a failed local leader is skipped");
    assert_eq!(remote.calls().len(), 3);
}

#[tokio::test]
async fn o09_coalescing_client_fails_every_waiter_of_a_failed_call() {
    let remote = Source::new(false);
    remote.fail.store(true, Ordering::SeqCst);
    let client = DirectTimestampClient::new(None, remote.clone());
    assert!(client.commit_ts().await.is_err());
    remote.fail.store(false, Ordering::SeqCst);
    assert!(client.commit_ts().await.is_ok(), "the driver restarts");
    assert!(client.allocate(0).await.is_err());
    assert!(client.allocate(MAX_DIRECT_COUNT + 1).await.is_err());
}

#[tokio::test]
async fn o09_d06_coalescing_client_follows_a_timestamp_leader_change() {
    let cluster = Cluster::new(2).await;
    let handles: Vec<_> = (0..2).map(|node| cluster.handle(node)).collect();
    for handle in &handles {
        handle.install_direct_gate(open_gate());
    }
    let authority = initialize(&handles[0]).await;
    let local: Arc<dyn DirectTimestampSource> = handles[0].clone();
    let remote: Arc<dyn DirectTimestampSource> = handles[1].clone();
    let client = DirectTimestampClient::new(Some(local), remote);
    let a = client.commit_ts().await.unwrap().get();
    assert_eq!(client.local_calls(), 1);
    cluster.elect(1).await;
    let b = client.commit_ts().await.unwrap().get();
    assert!(b > a + DEFAULT_DIRECT_WINDOW - 1);
    let q = queued(&handles[1], authority).await;
    let c = client.commit_ts().await.unwrap().get();
    assert!(b < q && q < c);
    assert_eq!(client.local_calls(), 1);
}

#[tokio::test]
async fn o09_coalescing_client_falls_back_when_the_local_leader_never_answers() {
    // The local source never completes, like a leader partitioned mid-write.
    let local = Source::new(true);
    let remote = Source::new(false);
    let client = DirectTimestampClient::with_local_deadline(
        Some(local.clone()),
        remote.clone(),
        std::time::Duration::from_millis(50),
    );
    let value = tokio::time::timeout(std::time::Duration::from_secs(5), client.commit_ts())
        .await
        .expect("the driver must not wedge on the local route")
        .unwrap();
    assert_eq!(value.get(), 1);
    assert_eq!(local.entered.load(Ordering::SeqCst), 1);
    assert_eq!(remote.calls(), vec![1]);
    let next = client.commit_ts().await.unwrap();
    assert!(next > value);
    assert_eq!(
        local.entered.load(Ordering::SeqCst),
        1,
        "the stuck local replica is skipped during the backoff"
    );
    assert_eq!(client.local_calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o09_d06_local_deadline_releases_a_refill_stuck_in_reserve_window() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    cluster.stalled.store(true, Ordering::SeqCst);
    let remote = Source::new(false);
    let local: Arc<dyn DirectTimestampSource> = handle.clone();
    let client = DirectTimestampClient::with_local_deadline(
        Some(local),
        remote.clone(),
        std::time::Duration::from_millis(50),
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), client.commit_ts())
        .await
        .expect("the driver must not wedge on a stalled ReserveWindow")
        .unwrap();
    assert_eq!(remote.calls(), vec![1]);
    // The abandoned local attempt released the store's refill lock.
    cluster.stalled.store(false, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.allocate_direct(1))
        .await
        .expect("the refill lock must be free")
        .unwrap();
}

#[tokio::test]
async fn o09_d06_hosted_apply_discards_the_window_of_an_independent_handle_machine() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    let a = handle.allocate_direct(1).await.unwrap().first().get();
    assert_eq!(handle.direct_window_stats().reservations, 1);
    // Applied through the hosted machine only, never through the handle.
    let TimestampReply::Snapshot(pin) = cluster
        .commit_external(TimestampCommand::BeginBackup {
            authority,
            owner: crate::oltp::backup::BackupOwnerRef {
                backup_id: Uuid::new_v4(),
                attempt: 1,
            },
            pin_id: Uuid::new_v4(),
        })
        .await
    else {
        panic!("expected a pin")
    };
    let q = pin.read_ts.get();
    assert_eq!(q, a + DEFAULT_DIRECT_WINDOW);
    assert_eq!(handle.direct_window_stats().discards, 1);
    let c = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(
        c > q,
        "direct value {c} after hosted queued {q} must be higher"
    );
    assert_eq!(handle.direct_window_stats().reservations, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o09_d06_concurrent_queued_and_direct_allocations_keep_real_time_order() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    handle.set_direct_window_size(8).unwrap();
    let clock = Arc::new(AtomicU64::new(0));
    let tasks: Vec<_> = (0..8u64)
        .map(|task| {
            let handle = handle.clone();
            let clock = clock.clone();
            tokio::spawn(async move {
                let mut events = Vec::new();
                let mut seed = 0x2026_1119 ^ (task + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                for _ in 0..40 {
                    seed = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let start = clock.fetch_add(1, Ordering::SeqCst);
                    let (low, high) = if seed >> 62 == 0 {
                        let value = queued(&handle, authority).await;
                        (value, value)
                    } else {
                        let range = handle.allocate_direct((seed >> 33) % 3 + 1).await.unwrap();
                        (range.first().get(), range.last().get())
                    };
                    let end = clock.fetch_add(1, Ordering::SeqCst);
                    events.push((start, end, low, high));
                }
                events
            })
        })
        .collect();
    let mut events = Vec::new();
    for task in tasks {
        events.extend(task.await.unwrap());
    }
    let mut values: Vec<u64> = events.iter().flat_map(|e| e.2..=e.3).collect();
    let issued = values.len();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values.len(), issued, "no value is issued twice");
    for before in &events {
        for after in &events {
            if before.1 < after.0 {
                assert!(
                    after.2 > before.3,
                    "call {after:?} started after {before:?} finished but is not above it"
                );
            }
        }
    }
    let stats = handle.direct_window_stats();
    assert!(stats.reservations > 1 && stats.discards > 0, "{stats:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o09_coalescing_client_survives_callers_dropped_mid_batch() {
    let remote = Source::new(true);
    let client = DirectTimestampClient::new(None, remote.clone());
    let spawn = |client: &DirectTimestampClient| {
        let client = client.clone();
        tokio::spawn(async move { client.allocate(1).await })
    };
    let in_flight = spawn(&client);
    until(|| remote.entered.load(Ordering::SeqCst) == 1).await;
    // Enqueue one at a time so arrival order, hence slice order, is fixed.
    let kept = spawn(&client);
    until(|| client.waiting() == 1).await;
    let dropped = spawn(&client);
    until(|| client.waiting() == 2).await;
    let kept_too = spawn(&client);
    until(|| client.waiting() == 3).await;
    in_flight.abort();
    dropped.abort();
    assert!(in_flight.await.unwrap_err().is_cancelled());
    assert!(dropped.await.unwrap_err().is_cancelled());
    remote.hold.add_permits(3);
    let kept = kept.await.unwrap().unwrap();
    let kept_too = kept_too.await.unwrap().unwrap();
    let later = tokio::time::timeout(std::time::Duration::from_secs(5), client.allocate(1))
        .await
        .expect("no hang after dropped callers")
        .unwrap();
    // Dropped callers' values are skipped, never handed to anyone else.
    assert_eq!(remote.calls(), vec![1, 3, 1]);
    assert_eq!((kept.first().get(), kept_too.first().get()), (2, 4));
    assert_eq!(later.first().get(), 5);
}

#[tokio::test]
async fn o09_coalescing_client_recovers_after_the_driver_panics() {
    let remote = Source::new(false);
    let client = DirectTimestampClient::new(None, remote.clone());
    let before = client.commit_ts().await.unwrap();
    remote.panic.store(true, Ordering::SeqCst);
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), client.commit_ts())
        .await
        .expect("waiters of a panicked driver must not hang")
        .unwrap_err();
    assert_eq!(error.outcome, Outcome::Unknown);
    remote.panic.store(false, Ordering::SeqCst);
    let after = tokio::time::timeout(std::time::Duration::from_secs(5), client.commit_ts())
        .await
        .expect("a new driver must start")
        .unwrap();
    assert!(after > before);
}

#[test]
fn o09_coalescing_client_recovers_after_its_runtime_cancels_the_driver() {
    let remote = Source::new(true);
    let client = DirectTimestampClient::new(None, remote.clone());
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    first.block_on(async {
        let client = client.clone();
        tokio::spawn(async move { client.allocate(1).await });
        until(|| remote.entered.load(Ordering::SeqCst) == 1).await;
    });
    // Dropping the runtime cancels the driver mid-call.
    drop(first);
    remote.held.store(false, Ordering::SeqCst);
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let value = second.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), client.commit_ts())
            .await
            .expect("a cancelled driver must not leave the client stuck")
            .unwrap()
    });
    assert_eq!(value.get(), 1, "the cancelled call issued nothing");
    assert_eq!(remote.calls(), vec![1, 1]);
}

/// PERF1, plan section 5: `AllocateDirect` passes a leader-lease barrier
/// (ReadIndex only as the lease's own fallback), never a forced ReadIndex.
#[tokio::test]
async fn o09_d06_perf1_allocate_direct_uses_the_lease_barrier() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    let before = cluster.read_index.load(Ordering::SeqCst);
    let leases = cluster.lease_reads.load(Ordering::SeqCst);
    let a = handle.allocate_direct(1).await.unwrap().first().get();
    let b = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(b > a);
    assert_eq!(cluster.read_index.load(Ordering::SeqCst), before);
    assert_eq!(cluster.lease_reads.load(Ordering::SeqCst), leases + 2);
    // The window rule still holds: a queued allocation in between is below
    // the next direct value.
    let q = queued(&handle, authority).await;
    let c = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(c > q, "direct value {c} after queued {q} must be higher");
}

/// PERF1: after a queued allocation discards the window, the background task
/// reserves the next one, so the next direct allocation proposes nothing and
/// is still above the queued value (O09 real-time order).
#[tokio::test]
async fn o09_d06_perf1_discarded_window_is_reserved_again_in_the_background() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    let prefill = handle.spawn_direct_prefill();
    let a = handle.allocate_direct(1).await.unwrap().first().get();
    assert_eq!(handle.direct_window_stats().reservations, 1);
    let q = queued(&handle, authority).await;
    assert_eq!(q, a + DEFAULT_DIRECT_WINDOW);
    assert_eq!(handle.direct_window_stats().discards, 1);
    until(|| handle.direct_window_stats().reservations == 2).await;
    let proposals = cluster.reserve_window_proposals().await;
    assert_eq!(proposals, 2);
    let c = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(c > q, "direct value {c} after queued {q} must be higher");
    let d = handle.allocate_direct(3).await.unwrap();
    assert_eq!(d.first().get(), c + 1, "served from the prefilled window");
    assert_eq!(cluster.reserve_window_proposals().await, proposals);
    // A second discard is refilled too; values stay above every queued one.
    let q2 = queued(&handle, authority).await;
    until(|| handle.direct_window_stats().reservations == 3).await;
    let e = handle.allocate_direct(1).await.unwrap().first().get();
    assert!(e > q2, "direct value {e} after queued {q2} must be higher");
    assert_eq!(cluster.reserve_window_proposals().await, 3);
    drop(handle);
    tokio::time::timeout(std::time::Duration::from_secs(5), prefill)
        .await
        .expect("the prefill task ends with its handle")
        .unwrap();
}

/// PERF1: a queued-only node (no `AllocateDirect` served, no window) writes
/// no background reservations.
#[tokio::test]
async fn o09_perf1_no_prefill_without_direct_allocations() {
    let cluster = Cluster::new(1).await;
    let handle = cluster.handle(0);
    let authority = initialize(&handle).await;
    handle.install_direct_gate(open_gate());
    let _prefill = handle.spawn_direct_prefill();
    for _ in 0..3 {
        queued(&handle, authority).await;
    }
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(cluster.reserve_window_proposals().await, 0);
    assert_eq!(handle.direct_window_stats().reservations, 0);
}
