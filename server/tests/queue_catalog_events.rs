//! U03 seeded event histories across RAM replicas, resource ownership and the
//! real fenced local mutation engine. Network/process faults have separate tests.
#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use futures_util::FutureExt;
use nabedra_database_server::{
    config::WorkerResources,
    engine::Engine,
    partition_size::PartitionSize,
    request_queue::{QueueError, QueueLimits, RequestQueue, RequestRecord, RequestState},
    scheduling::{MemoryCharge, Reservation, ResourceScheduler},
    snapshot,
    storage::*,
    worker_pool::WorkerPool,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
};
use tokio::time::{Duration, Instant};
use uuid::Uuid;
#[path = "support/storage.rs"]
mod fake;
use fake::Fake;
const NAMESPACE: &str = "event-history";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn limits() -> QueueLimits {
    QueueLimits {
        max_pending_requests: 8,
        max_queue_bytes_per_replica: 4 << 20,
        max_retained_result_bytes_per_worker: 4 << 20,
        result_retention_ms: 1_000_000,
    }
}
fn resources(node: u64) -> WorkerResources {
    WorkerResources {
        max_threads: 1 + node % 2,
        memory_limit_bytes: 32 << 20,
        task_memory_limit_bytes: 8 << 20,
        control_memory_reserve_bytes: 1 << 20,
    }
}
fn budget() -> HistoryBudget {
    HistoryBudget::new(1024, 16 << 20, Instant::now() + Duration::from_secs(10)).unwrap()
}
#[derive(Clone, Debug)]
enum Spec {
    Insert(i64, String),
    Upsert(i64, String),
    Delete(i64),
    Batch(Vec<(i64, String)>),
    Count,
}
impl Spec {
    fn request(&self, request: Uuid, table: Uuid) -> Vec<u8> {
        let row = |key: i64, text: &str| json!({"id":key,"text":text});
        let (operation, payload) = match self {
            Self::Insert(k, v) => ("table.insert", json!({"table_id":table,"row":row(*k,v)})),
            Self::Upsert(k, v) => ("table.upsert", json!({"table_id":table,"row":row(*k,v)})),
            Self::Delete(k) => ("table.delete", json!({"table_id":table,"key":k})),
            Self::Batch(rows) => (
                "table.insert_many",
                json!({"table_id":table,"rows":rows.iter().map(|(k,v)|row(*k,v)).collect::<Vec<_>>()}),
            ),
            Self::Count => (
                "query.count",
                json!({"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}),
            ),
        };
        serde_json::to_vec(&json!({"request_id":request,"operation":operation,"payload":payload}))
            .unwrap()
    }
    fn mutation(&self) -> bool {
        !matches!(self, Self::Count)
    }
    fn apply(&self, rows: &mut BTreeMap<i64, String>, read_count: Option<usize>) -> (u16, Value) {
        match self {
            Self::Insert(k, v) => {
                if rows.contains_key(k) {
                    (409, Value::Null)
                } else {
                    rows.insert(*k, v.clone());
                    (200, Value::Null)
                }
            }
            Self::Upsert(k, v) => {
                rows.insert(*k, v.clone());
                (200, Value::Null)
            }
            Self::Delete(k) => (200, json!(rows.remove(k).is_some())),
            Self::Batch(batch) => {
                let mut keys = BTreeSet::new();
                if batch
                    .iter()
                    .any(|(key, _)| rows.contains_key(key) || !keys.insert(*key))
                {
                    (409, Value::Null)
                } else {
                    for (k, v) in batch {
                        rows.insert(*k, v.clone());
                    }
                    (200, json!(batch.len()))
                }
            }
            Self::Count => (200, json!(read_count.unwrap())),
        }
    }
}
struct Running {
    _grant: Reservation,
    pin: Option<LoadedClusterCatalog>,
    read_count: Option<usize>,
}
struct History {
    seed: u64,
    rng: u64,
    trace: Vec<String>,
    engine: Engine<Fake>,
    queue: RequestQueue,
    replicas: BTreeMap<Uuid, RequestQueue>,
    ledger: ResourceScheduler,
    node_limits: BTreeMap<u64, WorkerResources>,
    held_memory: Option<MemoryCharge>,
    active: BTreeMap<Uuid, Running>,
    specs: BTreeMap<Uuid, Spec>,
    pins: BTreeMap<Uuid, (LoadedClusterCatalog, usize)>,
    accepted: BTreeSet<Uuid>,
    decisions: BTreeMap<Uuid, (u16, Vec<u8>)>,
    terminal: BTreeMap<Uuid, (u16, Vec<u8>)>,
    oracle: BTreeMap<i64, String>,
    table: Uuid,
    fence: CatalogFence,
    next: u128,
    next_node: u64,
    now: u64,
    arrivals: usize,
    rejected: usize,
    joins: usize,
    losses: usize,
    drains: usize,
    cancelled: usize,
    retries: usize,
}
impl History {
    fn random(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.rng >> 32
    }
    fn identity(&mut self) -> Uuid {
        self.next += 1;
        id(((self.seed as u128) << 64) | self.next)
    }
    fn log(&mut self, event: impl Into<String>) {
        self.trace.push(format!("{}:{}", self.now, event.into()));
    }
    fn replicate(&mut self, mut record: RequestRecord) {
        let members = self.queue.members().clone();
        for member in members {
            self.replicas
                .get_mut(&member)
                .unwrap()
                .install(record.clone())
                .unwrap();
            record = self
                .queue
                .acknowledge_version(record.request_id, record.version, member)
                .unwrap();
        }
        let final_record = self.queue.record(record.request_id).unwrap().clone();
        for replica in self.replicas.values_mut() {
            replica.install(final_record.clone()).unwrap();
        }
    }
    fn arrive(&mut self) {
        let n = self.random();
        let key = (n % 12) as i64;
        let text = format!("seed{}-{}", self.seed, self.next);
        let spec = match (n / 12) % 5 {
            0 => Spec::Insert(key, text),
            1 => Spec::Upsert(key, text),
            2 => Spec::Delete(key),
            3 => Spec::Batch(vec![(key, text.clone()), (((n / 60) % 12) as i64, text)]),
            _ => Spec::Count,
        };
        let request = self.identity();
        let body = spec.request(request, self.table);
        self.arrivals += 1;
        self.log(format!("arrive:{request}:{spec:?}"));
        match self.queue.propose(body, self.now) {
            Ok(record) => {
                self.specs.insert(request, spec);
                self.replicate(record);
                assert!(self.accepted.insert(request));
            }
            Err(QueueError::Overloaded) => {
                self.rejected += 1;
            }
            other => panic!("unexpected admission {other:?}"),
        }
    }
    async fn start(&mut self) {
        let next = self
            .queue
            .records()
            .filter(|r| {
                matches!(r.state, RequestState::Queued | RequestState::Reconciling)
                    && !self.active.contains_key(&r.request_id)
            })
            .min_by_key(|r| r.sequence)
            .map(|r| (r.request_id, r.sequence));
        let Some((request, sequence)) = next else {
            self.log("start:none");
            return;
        };
        let cap = 1 + (self.random() % 2) as usize;
        let ledger = self.ledger.clone();
        let mut reservation = Box::pin(ledger.reserve_remote_memory(sequence, cap, 16 << 10));
        let grant = match reservation.as_mut().now_or_never() {
            Some(Ok(grant)) => grant,
            None => {
                self.log(format!("start:temporarily-unfit:{request}"));
                return;
            }
            Some(Err(error)) => panic!("unexpected grant error {error}"),
        };
        self.log(format!("start:{request}:node{}:cap{cap}", grant.node_id()));
        let (pin, read_count, pin_bytes) = if self.specs[&request].mutation() {
            (None, None, None)
        } else {
            if !self.pins.contains_key(&request) {
                let pin = self.engine.storage().load_cluster_catalog().await.unwrap();
                self.pins.insert(request, (pin, self.oracle.len()));
            }
            let (pin, count) = self.pins[&request].clone();
            let bytes = if self.queue.record(request).unwrap().pinned_catalog.is_some() {
                None
            } else {
                Some(pin.catalog().encode().unwrap())
            };
            (Some(pin), Some(count), bytes)
        };
        let record = self.queue.start(request, pin_bytes).unwrap();
        self.replicate(record);
        assert!(
            self.active
                .insert(
                    request,
                    Running {
                        _grant: grant,
                        pin,
                        read_count
                    }
                )
                .is_none()
        );
    }
    async fn complete(&mut self) {
        let candidates = self.active.keys().copied().collect::<Vec<_>>();
        if candidates.is_empty() {
            self.log("complete:none");
            return;
        }
        let request = candidates[(self.random() as usize) % candidates.len()];
        let running = self.active.remove(&request).unwrap();
        self.log(format!("complete:{request}"));
        let spec = self.specs[&request].clone();
        let body = self.queue.record(request).unwrap().body.clone();
        if spec.mutation() {
            let record = self
                .queue
                .transition(request, RequestState::Committing)
                .unwrap();
            self.replicate(record);
        }
        if spec.mutation() && self.random() % 4 == 0 {
            self.log("catalog-put:apply-then-lose-reply");
            self.engine
                .storage()
                .backend()
                .state
                .lock()
                .unwrap()
                .put_faults
                .insert(
                    format!("{NAMESPACE}/catalog.json"),
                    (StorageError::Ambiguous, true),
                );
        }
        let mut expected = self.oracle.clone();
        let (status, data) = spec.apply(&mut expected, running.read_count);
        let response = self
            .engine
            .execute_identified(body, self.fence, running.pin)
            .await
            .unwrap();
        assert_eq!(response.status, status);
        let value: Value = serde_json::from_slice(&response.body).unwrap();
        if status == 200 {
            assert_eq!(value["data"], data);
        } else {
            assert_eq!(value["error"]["code"], "conflict");
        }
        self.oracle = expected;
        if spec.mutation() {
            assert!(
                self.decisions
                    .insert(request, (response.status, response.body.clone()))
                    .is_none()
            );
        }
        assert!(
            self.terminal
                .insert(request, (response.status, response.body.clone()))
                .is_none()
        );
        let record = self
            .queue
            .finish(request, response.status, response.body, self.now)
            .unwrap();
        self.replicate(record);
        drop(running._grant);
    }
    async fn retry(&mut self) {
        let candidates = self.accepted.iter().copied().collect::<Vec<_>>();
        if candidates.is_empty() {
            return;
        }
        let request = candidates[self.random() as usize % candidates.len()];
        let old = self.queue.record(request).unwrap().clone();
        self.log(format!("retry:{request}"));
        self.retries += 1;
        let attached = self.queue.propose(old.body.clone(), self.now).unwrap();
        assert_eq!(attached.sequence, old.sequence);
        assert_eq!(attached.state, old.state);
        if let Some((status, body)) = self.decisions.get(&request) {
            let replay = self
                .engine
                .execute_identified(old.body, self.fence, None)
                .await
                .unwrap();
            assert_eq!((replay.status, replay.body), (*status, body.clone()));
        }
    }
    fn conflict(&mut self) {
        let Some(request) = self.accepted.iter().next().copied() else {
            return;
        };
        let mut body = self.queue.record(request).unwrap().body.clone();
        body.push(b' ');
        self.log(format!("different-body:{request}"));
        assert_eq!(
            self.queue.propose(body, self.now),
            Err(QueueError::Conflict)
        );
    }
    fn cancel_before_execution(&mut self) {
        let Some(request) = self.active.keys().next().copied() else {
            self.log("cancel:none");
            return;
        };
        self.log(format!("cancel-before-execution:{request}"));
        self.cancelled += 1;
        let running = self.active.remove(&request).unwrap();
        drop(running);
        let record = self
            .queue
            .transition(request, RequestState::Reconciling)
            .unwrap();
        self.replicate(record);
    }
    async fn change_members(&mut self, new_members: BTreeSet<Uuid>, event: &str) {
        self.log(format!("{event}:{new_members:?}"));
        let base = self.engine.storage().load_cluster_catalog().await.unwrap();
        let stale_id = self.identity();
        let stale_body = Spec::Batch(Vec::new()).request(stale_id, self.table);
        let stale = Decision {
            version: 1,
            id: self.identity(),
            parent: base.catalog().decision_head,
            base_revision: base.catalog().revision,
            revision: base.catalog().revision + 1,
            request_id: stale_id,
            request_body: String::from_utf8(stale_body).unwrap(),
            response_status: 200,
            response_body: serde_json::to_string(&json!({"request_id":stale_id,"data":0})).unwrap(),
        };
        let candidate = PreparedPublication::prepare_fenced(
            base.clone(),
            base.catalog().tables.clone(),
            stale,
            self.fence,
        )
        .unwrap();
        self.fence.scheduler_term += 1;
        self.engine
            .storage()
            .fence_cluster_catalog(&base, self.fence, FenceTransition::Takeover)
            .await
            .unwrap();
        // Only the authoritative root fence permits releasing these old attempts.
        self.active.clear();
        for old in self.queue.members().difference(&new_members) {
            self.ledger.drain_node(old.as_u128() as u64).unwrap();
        }
        for member in &new_members {
            let number = member.as_u128() as u64;
            let limits = resources(number);
            self.node_limits.insert(number, limits);
            self.ledger.update_node(number, limits).unwrap();
            self.ledger.set_io_capacity(number, 2).unwrap();
        }
        self.queue
            .takeover(
                self.fence.scheduler_term,
                self.queue.membership_revision() + 1,
                new_members.clone(),
            )
            .unwrap();
        let snapshot = self.queue.snapshot();
        self.replicas.clear();
        for member in new_members {
            let mut replica = RequestQueue::new(
                snapshot.run_id,
                snapshot.scheduler_term,
                snapshot.membership_revision,
                limits(),
                snapshot.members.clone(),
            )
            .unwrap();
            replica.install_snapshot(snapshot.clone()).unwrap();
            self.replicas.insert(member, replica);
        }
        let records = self.queue.records().cloned().collect::<Vec<_>>();
        for record in records {
            self.replicate(record);
        }
        assert_eq!(
            candidate
                .publish(self.engine.storage(), budget())
                .await
                .unwrap(),
            PublicationOutcome::NeedsRecompute
        );
    }
    async fn join(&mut self) {
        if self.queue.members().len() >= 3 {
            return;
        }
        self.next_node += 1;
        let member = id(self.next_node as u128);
        self.log(format!("copy-before-ready:{member}"));
        let old = self.queue.snapshot();
        let mut joining = RequestQueue::new(
            old.run_id,
            old.scheduler_term,
            old.membership_revision,
            limits(),
            old.members.clone(),
        )
        .unwrap();
        joining.install_snapshot(old).unwrap();
        assert_eq!(joining.records().count(), self.accepted.len());
        let mut members = self.queue.members().clone();
        members.insert(member);
        self.joins += 1;
        self.change_members(members, "join-ready").await;
    }
    async fn lose(&mut self, drain: bool) {
        if self.queue.members().len() == 1 {
            self.join().await;
            return;
        }
        let lost = *self.queue.members().iter().next().unwrap();
        let survivor = *self.queue.members().iter().find(|id| **id != lost).unwrap();
        if drain {
            self.drains += 1;
        } else {
            self.losses += 1;
            self.replicas.remove(&lost);
            let snapshot = self.replicas[&survivor].snapshot();
            let mut recovered = RequestQueue::new(
                snapshot.run_id,
                snapshot.scheduler_term,
                snapshot.membership_revision,
                limits(),
                snapshot.members.clone(),
            )
            .unwrap();
            recovered.install_snapshot(snapshot).unwrap();
            self.queue = recovered;
        }
        let mut members = self.queue.members().clone();
        members.remove(&lost);
        self.change_members(
            members,
            if drain {
                "transfer-then-drain"
            } else {
                "recover-surviving-replica"
            },
        )
        .await;
    }
    fn memory_event(&mut self) {
        if self.held_memory.take().is_some() {
            self.log("release-memory");
            return;
        }
        let node = self.queue.members().iter().next().unwrap().as_u128() as u64;
        let bytes = self.node_limits[&node].working_bytes() - 1024;
        self.log(format!("charge-memory:node{node}:{bytes}"));
        self.held_memory = self.ledger.charge_memory(node, bytes).ok();
    }
    async fn invariants(&self) {
        let ids = self
            .queue
            .records()
            .filter(|r| r.accepted)
            .map(|r| r.request_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, self.accepted);
        assert_eq!(self.arrivals, self.accepted.len() + self.rejected);
        assert!(self.queue.pending() <= limits().max_pending_requests);
        for record in self.queue.records() {
            assert!(record.accepted);
            assert_eq!(record.run_id, self.fence.run_id);
            assert_eq!(record.scheduler_term, self.fence.scheduler_term);
            assert!(self.queue.ready(record.request_id));
            for replica in self.replicas.values() {
                assert_eq!(replica.record(record.request_id), Some(record));
            }
            assert_eq!(
                record.state.terminal(),
                self.terminal.contains_key(&record.request_id)
            );
            let receipt = self
                .queue
                .lookup(self.fence.run_id, record.request_id, self.now)
                .unwrap();
            assert_eq!(receipt.state, record.state);
            if let Some((status, body)) = self.terminal.get(&record.request_id) {
                assert_eq!(record.response_status, Some(*status));
                assert_eq!(record.response_body.as_ref(), Some(body));
            }
        }
        for (node, threads, bytes) in self.ledger.usage() {
            let limits = self.node_limits[&node];
            assert!(threads <= limits.max_threads);
            assert!(bytes <= limits.working_bytes());
        }
        let loaded = self.engine.storage().load_cluster_catalog().await.unwrap();
        assert_eq!(loaded.catalog().fence, Some(self.fence));
        let entry = &loaded.catalog().tables["items"];
        let descriptor = entry.descriptor("items");
        let mut actual = BTreeMap::new();
        let TableData::Partitioned { files } = &entry.data else {
            panic!("partitioned fixture");
        };
        for file in files {
            let object = self
                .engine
                .storage()
                .read(
                    &ObjectKey::parse_partition(&file.object, self.table).unwrap(),
                    nabedra_database_server::partition_size::MAX_PARTITION_BYTES,
                )
                .await
                .unwrap();
            let rows = snapshot::decode(
                &descriptor,
                bytes::Bytes::from(object.bytes),
                Some(file.row_count),
            )
            .unwrap();
            for row in rows {
                let key = row["id"].clone();
                let text = row["text"].clone();
                let nabedra_database_server::protocol::Scalar::Int64(key) = key else {
                    panic!("key");
                };
                let nabedra_database_server::protocol::Scalar::Utf8(text) = text else {
                    panic!("text");
                };
                assert!(actual.insert(key, text).is_none());
            }
        }
        assert_eq!(actual, self.oracle);
        assert_eq!(entry.row_count, self.oracle.len() as u64);
        let mut next = loaded.catalog().decision_head;
        let mut unique = BTreeSet::new();
        while let Some(commit) = next {
            let bytes = self
                .engine
                .storage()
                .read(&ObjectKey::decision(commit), MAX_DECISION_BYTES)
                .await
                .unwrap()
                .bytes;
            let decision = Decision::decode(&bytes).unwrap();
            assert!(
                unique.insert(decision.request_id),
                "duplicate durable logical mutation"
            );
            let expected = &self.decisions[&decision.request_id];
            assert_eq!(
                (decision.response_status, decision.response_body.as_bytes()),
                (expected.0, expected.1.as_slice())
            );
            next = decision.parent;
        }
        assert_eq!(
            unique,
            self.decisions.keys().copied().collect::<BTreeSet<_>>()
        );
        assert_eq!(loaded.catalog().revision, self.decisions.len() as u64);
        assert!(
            self.engine
                .storage()
                .backend()
                .state
                .lock()
                .unwrap()
                .objects
                .keys()
                .all(|key| key.ends_with("/catalog.json")
                    || key.contains("/tables/")
                    || key.contains("/decisions/")
                    // OLAP-1: immutable decision index pages.
                    || key.contains("/decision-index/"))
        );
    }
    async fn run(&mut self) {
        // This prefix deterministically exercises independent count overflow.
        for _ in 0..10 {
            self.now += 1;
            self.arrive();
            self.invariants().await;
        }
        self.now += 1;
        self.start().await;
        self.cancel_before_execution();
        self.invariants().await;
        self.join().await;
        self.lose(false).await;
        self.join().await;
        self.lose(true).await;
        self.invariants().await;
        for step in 0..96 {
            self.now += 1;
            let event = if step < 12 { step } else { self.random() % 12 };
            match event {
                0 | 1 | 2 => self.arrive(),
                3 | 4 => self.start().await,
                5 | 6 => self.complete().await,
                7 => self.retry().await,
                8 => self.cancel_before_execution(),
                9 => self.join().await,
                10 => self.lose(step % 2 == 0).await,
                _ => {
                    self.memory_event();
                    self.conflict();
                }
            }
            self.invariants().await;
        }
        self.held_memory = None;
        for _ in 0..256 {
            if self.queue.pending() == 0 {
                break;
            }
            self.now += 1;
            self.start().await;
            self.complete().await;
            self.invariants().await;
        }
        assert_eq!(self.queue.pending(), 0);
        assert_eq!(self.terminal.len(), self.accepted.len());
        assert!(
            self.joins > 0
                && self.losses > 0
                && self.drains > 0
                && self.retries > 0
                && self.cancelled > 0
        );
        assert!(self.rejected >= 2);
        assert!(
            self.ledger
                .usage()
                .iter()
                .all(|(_, cpu, bytes)| *cpu == 0 && *bytes == 0)
        );
    }
}
#[tokio::test]
async fn u03_seeded_queue_resources_membership_and_fenced_catalog_match_serial_oracle() {
    for seed in [343687_u64, 99112, 20261115] {
        let storage = Storage::new(Fake::default(), Namespace::new(NAMESPACE).unwrap());
        let fence = CatalogFence {
            run_id: id(seed as u128 + 1),
            scheduler_term: 1,
        };
        storage
            .initialize_fenced_cluster_catalog(fence, false)
            .await
            .unwrap();
        let (pool, threads) = WorkerPool::start(
            NonZeroUsize::new(2).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(storage, pool, "4 KiB".parse::<PartitionSize>().unwrap());
        let setup = id((seed as u128) << 64);
        let body=serde_json::to_vec(&json!({"request_id":setup,"operation":"table.create","payload":{"name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"}})).unwrap();
        let created = engine.execute_identified(body, fence, None).await.unwrap();
        let value: Value = serde_json::from_slice(&created.body).unwrap();
        let table = Uuid::parse_str(value["data"]["id"].as_str().unwrap()).unwrap();
        let members = [id(10), id(11)].into_iter().collect::<BTreeSet<_>>();
        let queue = RequestQueue::new(fence.run_id, 1, 1, limits(), members.clone()).unwrap();
        let replicas = members
            .iter()
            .map(|member| {
                (
                    *member,
                    RequestQueue::new(fence.run_id, 1, 1, limits(), members.clone()).unwrap(),
                )
            })
            .collect();
        let node_limits = [(10, resources(10)), (11, resources(11))]
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let ledger =
            ResourceScheduler::new(node_limits.iter().map(|(n, r)| (*n, *r)).collect()).unwrap();
        for node in [10, 11] {
            ledger.set_io_capacity(node, 2).unwrap();
        }
        let mut history = History {
            seed,
            rng: seed,
            trace: Vec::new(),
            engine,
            queue,
            replicas,
            ledger,
            node_limits,
            held_memory: None,
            active: BTreeMap::new(),
            specs: BTreeMap::new(),
            pins: BTreeMap::new(),
            accepted: BTreeSet::new(),
            decisions: [(setup, (created.status, created.body))]
                .into_iter()
                .collect(),
            terminal: BTreeMap::new(),
            oracle: BTreeMap::new(),
            table,
            fence,
            next: 0,
            next_node: 11,
            now: 0,
            arrivals: 0,
            rejected: 0,
            joins: 0,
            losses: 0,
            drains: 0,
            cancelled: 0,
            retries: 0,
        };
        let result = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(45), history.run()))
            .catch_unwind()
            .await;
        let trace = history.trace.clone();
        drop(history);
        threads.shutdown().await.unwrap();
        match result {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("seed {seed} watchdog; trace {trace:#?}"),
            Err(error) => {
                let message = error
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied())
                    .unwrap_or("assertion unwind");
                panic!("seed {seed}: {message}; trace {trace:#?}");
            }
        }
    }
}

async fn router_exchange(
    app: axum::Router,
    path: &str,
    body: Option<Value>,
    authorized: bool,
) -> (u16, Value) {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let mut request = axum::http::Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(path)
        .header(
            "authorization",
            if authorized {
                "Bearer event-token"
            } else {
                "Bearer invalid"
            },
        );
    let body = match body {
        Some(body) => {
            request = request.header("content-type", "application/json");
            axum::body::Body::from(body.to_string())
        }
        None => axum::body::Body::empty(),
    };
    let response = app.oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
// U02 here covers the public in-process HTTP Body contract under real CPU
// saturation. Signed internal frame/replay grammar remains independently tested
// by payload_stream::tests::rows_require_payload_and_real_eof and
// auth::replay::tests::q05_c05_signed_reordered_concurrent_packets_are_accepted_once_and_window_bounded.
async fn malformed_ingress_while_cpu_saturated(
    app: &axum::Router,
    run: &str,
    table: &Value,
    pool: &WorkerPool,
) {
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let query = |request: Uuid| json!({"request_id":request,"operation":"query.count","payload":{"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}});
    let mut cases = Vec::<(&str, Uuid, Body, u16, &str)>::new();
    let request = id(102000);
    cases.push((
        "incomplete-json",
        request,
        Body::from(format!(
            r#"{{"request_id":"{request}","operation":"query.count","payload":"#
        )),
        400,
        "validation_error",
    ));
    let request = id(102001);
    let body = query(request).to_string().replacen(
        &format!(r#""request_id":"{request}""#),
        &format!(r#""request_id":"{request}","request_id":"{request}""#),
        1,
    );
    cases.push((
        "duplicate-envelope-field",
        request,
        Body::from(body),
        400,
        "validation_error",
    ));
    let request = id(102002);
    let body =
        query(request)
            .to_string()
            .replacen(r#""limit":null"#, r#""limit":null,"limit":null"#, 1);
    cases.push((
        "duplicate-query-field",
        request,
        Body::from(body),
        400,
        "validation_error",
    ));
    let request = id(102003);
    let mut predicate = json!({"op":"eq","column":"id","value":1});
    for _ in 0..33 {
        predicate = json!({"op":"not","arg":predicate});
    }
    let mut body = query(request);
    body["payload"]["query"]["filter"] = predicate;
    cases.push((
        "predicate-depth-34",
        request,
        Body::from(body.to_string()),
        400,
        "validation_error",
    ));
    let request = id(102004);
    let mut body = query(request);
    body["payload"]["table_id"] = json!(false);
    cases.push((
        "wrong-uuid-type",
        request,
        Body::from(body.to_string()),
        400,
        "validation_error",
    ));
    let request = id(102005);
    let mut body = query(request);
    body["payload"]["query"]["limit"] = json!("18446744073709551616");
    cases.push((
        "wrong-limit-type",
        request,
        Body::from(body.to_string()),
        400,
        "validation_error",
    ));
    let request = id(102006);
    let mut body = query(request);
    body["payload"]["query"]["order_by"] = json!({"column":"id","desc":"false"});
    cases.push((
        "wrong-order-direction-type",
        request,
        Body::from(body.to_string()),
        400,
        "validation_error",
    ));
    let request = id(102007);
    let body = query(request).to_string().replacen(
        r#""limit":null"#,
        &format!(r#""limit":{}"#, "9".repeat(1024)),
        1,
    );
    // Oversized numeric query limits retain the protocol's saturating semantics.
    // This valid envelope therefore meets finite queue overload, not validation.
    cases.push((
        "valid-1024-digit-limit-at-capacity",
        request,
        Body::from(body),
        429,
        "overloaded",
    ));
    let request = id(102008);
    let prefix = bytes::Bytes::from(format!(
        r#"{{"request_id":"{request}","operation":"query.count","payload":"#
    ));
    let unreadable = futures_util::stream::iter(vec![
        Ok(prefix),
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "injected body read failure",
        )),
    ]);
    cases.push((
        "unreadable-body-after-prefix",
        request,
        Body::from_stream(unreadable),
        400,
        "validation_error",
    ));
    let request = id(102009);
    let mut body = query(request).to_string().into_bytes();
    body.truncate(body.len() - 3);
    let middle = body.len() / 2;
    let truncated = futures_util::stream::iter(vec![
        Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(&body[..middle])),
        Ok(bytes::Bytes::copy_from_slice(&body[middle..])),
    ]);
    cases.push((
        "truncated-json-across-body-chunks",
        request,
        Body::from_stream(truncated),
        400,
        "validation_error",
    ));
    for (name, request_id, body, expected_status, expected_code) in cases {
        let request = axum::http::Request::post("/v1/requests")
            .header("authorization", "Bearer event-token")
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status().as_u16(),
            expected_status,
            "U02 case {name}"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["error"]["code"], expected_code, "U02 case {name}");
        assert_eq!(error["error"]["outcome"], "not_applied", "U02 case {name}");
        assert_eq!(
            pool.scheduler().usage()[0].1,
            2,
            "both compute gates remain occupied during {name}"
        );
        let (status, receipt) = router_exchange(
            app.clone(),
            &format!("/v1/requests/{run}/{request_id}"),
            None,
            true,
        )
        .await;
        assert_eq!(
            status, 404,
            "U02 case {name} must create no accepted request"
        );
        assert_eq!(receipt["error"]["code"], "not_found", "U02 case {name}");
    }
}
struct ReleaseCpu(Vec<std::sync::mpsc::Sender<()>>);
impl Drop for ReleaseCpu {
    fn drop(&mut self) {
        for release in self.0.drain(..) {
            let _ = release.send(());
        }
    }
}

/// U01/Q01/Q02/U02 complement remote process fixtures with actual occupied local
/// compute slots and 1,000 accepted mixed queries under the default 8 GiB
/// worker ceiling. The 1 MiB task budget covers the tiny three-row partitions;
/// pending request ownership must remain independent of execution-stage demand.
#[tokio::test]
async fn u01_q01_q02_cpu_saturated_local_router_retains_every_accepted_query() {
    use nabedra_database_server::{api, config::ExecutionConfig, executor::Executor};
    let storage = Storage::new(Fake::default(), Namespace::new("cpu-gated-router").unwrap());
    let (pool, threads) = WorkerPool::start_with_resources(
        WorkerResources {
            max_threads: 2,
            task_memory_limit_bytes: 1 << 20,
            ..WorkerResources::default()
        },
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let engine = Engine::new(storage, pool.clone(), PartitionSize::default());
    let ingress = Executor::new(4, 8).unwrap();
    let config = ExecutionConfig {
        max_pending_requests: 1000,
        max_queue_bytes_per_replica: 16 << 20,
        max_retained_result_bytes_per_worker: 16 << 20,
        ..ExecutionConfig::default()
    };
    let base = config.max_queue_bytes_per_replica + config.max_retained_result_bytes_per_worker;
    let (app, tasks) = api::router_configured(
        engine.clone(),
        ingress.clone(),
        "event-token".into(),
        config,
        false,
    )
    .unwrap();
    let created=router_exchange(app.clone(),"/v1/operations",Some(json!({"request_id":id(1),"operation":"table.create","payload":{"name":"items","schema":{"id":"int64","text":"utf8"},"key":"id","format":"parquet"}})),true).await;
    assert_eq!(created.0, 200);
    let table = created.1["data"]["id"].clone();
    let inserted=router_exchange(app.clone(),"/v1/operations",Some(json!({"request_id":id(2),"operation":"table.insert_many","payload":{"table_id":table,"rows":[{"id":1,"text":"a"},{"id":2,"text":"b"},{"id":3,"text":"c"}]}})),true).await;
    assert_eq!(inserted.0, 200);
    // Observe that all detached warm-up admission guards settled without stopping
    // the executor that will own the subsequently queued queries.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let guards = (0..1000).map(|_| tasks.try_admit()).collect::<Vec<_>>();
            let idle = guards.iter().all(Result::is_ok);
            drop(guards);
            if idle {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut blockers = Vec::new();
    let mut gates = ReleaseCpu(Vec::new());
    for worker in 0..2 {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        gates.0.push(release);
        blockers.push(
            pool.submit_request(9000 + worker, 1, 1024, move |_, _| {
                entered.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(120))
                    .unwrap();
                Ok(())
            })
            .await
            .unwrap(),
        );
        started.await.unwrap();
    }
    assert_eq!(pool.scheduler().usage()[0].1, 2);
    let mut accepted = Vec::new();
    let admission=AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(60),async{
        let mut first_body=None;
        for n in 0..1000u128{
            let request=id(100+n);let operation=if n%2==0{"query.count"}else{"query.collect"};let body=json!({"request_id":request,"operation":operation,"payload":{"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}});
            let(status,receipt)=router_exchange(app.clone(),"/v1/requests",Some(body.clone()),true).await;assert_eq!(status,202);assert_eq!(receipt["request_id"],request.to_string());assert_eq!(pool.scheduler().usage()[0].1,2,"actual compute gates remain occupied throughout admission");
            let run=receipt["run_id"].as_str().unwrap().to_owned();accepted.push((request,run,n%2==0));if n==0{first_body=Some(body);}
        }
        let(first,run,_)=&accepted[0];let path=format!("/v1/requests/{run}/{first}");let(status,pending)=router_exchange(app.clone(),&path,None,true).await;assert_eq!(status,200);assert!(matches!(pending["state"].as_str(),Some("queued"|"running")),"a real partition query cannot finish while every compute slot is gated");
        assert_eq!(router_exchange(app.clone(),"/v1/requests",first_body.clone(),true).await.0,202,"same identity attaches even at capacity");
        let mut conflict=first_body.unwrap();conflict["operation"]=json!("query.collect");assert_eq!(router_exchange(app.clone(),"/v1/requests",Some(conflict),true).await.0,409);
        let overflow=id(199999);let body=json!({"request_id":overflow,"operation":"query.count","payload":{"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}});let(status,error)=router_exchange(app.clone(),"/v1/requests",Some(body.clone()),true).await;assert_eq!(status,429);assert_eq!(error["error"]["code"],"overloaded");assert_eq!(router_exchange(app.clone(),&format!("/v1/requests/{run}/{overflow}"),None,true).await.0,404);
        assert_eq!(router_exchange(app.clone(),"/v1/requests",Some(body),false).await.0,401);
        assert_eq!(router_exchange(app.clone(),"/v1/requests",Some(json!({"request_id":id(199998),"operation":"invalid.operation","payload":{}})),true).await.0,400);
        malformed_ingress_while_cpu_saturated(&app,run,&table,&pool).await;
        for(request,run,_)in &accepted{let(status,receipt)=router_exchange(app.clone(),&format!("/v1/requests/{run}/{request}"),None,true).await;assert_eq!(status,200);assert!(matches!(receipt["state"].as_str(),Some("queued"|"running")));}
    })).catch_unwind().await;
    // Releasing through Drop also covers assertion or watchdog failure; retain
    // and join every actual compute job before stopping the worker threads.
    drop(gates);
    for job in blockers {
        tokio::time::timeout(Duration::from_secs(5), job.receive())
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(60), tasks.drain())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), ingress.drain())
        .await
        .unwrap();
    let verification = AssertUnwindSafe(async {
        assert_eq!(accepted.len(), 1000);
        for (request, run, count) in accepted {
            let (status, receipt) = router_exchange(
                app.clone(),
                &format!("/v1/requests/{run}/{request}"),
                None,
                true,
            )
            .await;
            assert_eq!(status, 200);
            assert_eq!(receipt["request_id"], request.to_string());
            assert_eq!(receipt["state"], "succeeded");
            if count {
                assert_eq!(receipt["result"], 3);
            } else {
                assert_eq!(
                    receipt["result"],
                    json!([{"id":1,"text":"a"},{"id":2,"text":"b"},{"id":3,"text":"c"}])
                );
            }
        }
        assert_eq!(
            engine
                .storage()
                .load_cluster_catalog()
                .await
                .unwrap()
                .catalog()
                .revision,
            2,
            "read receipts must create no logical mutations"
        );
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, base)]);
    })
    .catch_unwind()
    .await;
    threads.shutdown().await.unwrap();
    match admission {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("CPU-gated admission watchdog"),
        Err(error) => std::panic::resume_unwind(error),
    }
    if let Err(error) = verification {
        std::panic::resume_unwind(error);
    }
}
