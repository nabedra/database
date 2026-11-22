//! OLAP streaming simulation (OLAP-STR-G01 to G06) over `SimObjectStore`.
//!
//! Each case drives the real `Local` coordinator, its `demand`, `scan_one` and
//! prefetch paths, and the real engine scan reader against a seeded in-memory
//! object store. The harness plays the client, the replica holder and the
//! nemesis. See `docs/testing/olap-stream-sim.md` for what is simulated and
//! what is real.
//!
//! The oracle is a serial scan of the pinned snapshot, computed from the
//! harness's own row model: rows sorted by primary key, then filtered and
//! limited by an independent evaluator. Delivered batches, deduplicated by
//! sequence, must concatenate to exactly that scan.
use super::*;
use crate::{
    config::WorkerResources,
    partition_size::PartitionSize,
    protocol::parse_request,
    storage::{
        CatalogFence, Download, FenceTransition, Namespace, StorageError, TableData,
        WriteCondition,
        sim::{Fault, SimObjectStore},
    },
    worker_pool::WorkerThreads,
};
use nabedra_check::{Gen, prop};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Mutex as StdMutex};

const MIB: u64 = 1 << 20;
const MAX_STEPS: usize = 160;

/// Planted bugs. Each one is a test-only variant of the coordinator behaviour
/// that the harness models, and each must be caught by the checks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bug {
    None,
    /// A reconnect rebuilds the stream from its first cursor and silently
    /// replays every acknowledged batch to reach the checkpoint again.
    RescanOnReconnect,
    /// Recovery after a coordinator kill restores the checkpoint cursors but
    /// the previous sequence number, so the next demand skips one batch.
    GapAfterKill,
    /// An open retry with the same UUID and body creates a new handle, as if
    /// the identity table were local and lost.
    ReplayNewHandle,
}

/// The stream engine logs partition reads, including background prefetch.
/// The mutation engine shares the store and pool without logging its reads.
#[derive(Clone)]
struct Counted {
    sim: SimObjectStore,
    reads: Option<Arc<StdMutex<Vec<String>>>>,
}
impl ObjectStore for Counted {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let result = self.sim.get(key).await;
        if result.is_ok()
            && key.ends_with(".parquet")
            && let Some(reads) = &self.reads
        {
            reads.lock().unwrap().push(key.to_owned());
        }
        result
    }

    async fn put(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.sim.put(key, body, condition).await
    }
}

struct World {
    store: SimObjectStore,
    reads: Arc<StdMutex<Vec<String>>>,
    engine: Engine<Counted>,
    writer: Engine<Counted>,
    threads: Option<WorkerThreads>,
    run: Uuid,
    term: u64,
    table: Uuid,
    /// The committed table contents, by primary key.
    model: BTreeMap<i64, String>,
}

async fn operation(engine: &Engine<Counted>, name: &str, payload: Value) -> Result<Value, String> {
    let request = json!({"request_id": Uuid::new_v4(), "operation": name, "payload": payload});
    let bytes = serde_json::to_vec(&request).unwrap();
    let operation = parse_request(&bytes).unwrap().operation;
    let catalog = engine
        .storage()
        .load_cluster_catalog()
        .await
        .map_err(|error| format!("catalog load failed: {error:?}"))?;
    let fence = catalog.catalog().fence.unwrap();
    let pinned = (!operation.is_mutation()).then_some(catalog);
    let result = engine
        .execute_identified(bytes, fence, pinned)
        .await
        .map_err(|error| format!("{name} failed: {error:?}"))?;
    let value: Value = serde_json::from_slice(&result.body).unwrap();
    if result.status != 200 {
        return Err(format!("{name} answered {}: {value}", result.status));
    }
    Ok(value["data"].clone())
}

impl World {
    async fn new(g: &mut Gen) -> Self {
        let store = SimObjectStore::new();
        let reads = Arc::new(StdMutex::new(Vec::new()));
        let storage = |reads| {
            Storage::new(
                Counted {
                    sim: store.clone(),
                    reads,
                },
                Namespace::new("stream-sim/").unwrap(),
            )
        };
        let writer_storage = storage(None);
        let run = Uuid::new_v4();
        writer_storage
            .initialize_fenced_cluster_catalog(
                CatalogFence {
                    run_id: run,
                    scheduler_term: 1,
                },
                false,
                false,
            )
            .await
            .unwrap();
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 128 * MIB,
            task_memory_limit_bytes: 32 * MIB,
            control_memory_reserve_bytes: 16 * MIB,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, tokio::runtime::Handle::current()).unwrap();
        // Small partitions give the pinned manifest several files, so batches
        // cross physical partition boundaries and a rescan is visible.
        let partition: PartitionSize = ["200 B", "600 B", "1 KiB", "4 KiB"][g.below(4) as usize]
            .parse()
            .unwrap();
        let engine = Engine::new(storage(Some(reads.clone())), pool.clone(), partition);
        let writer = Engine::new(writer_storage, pool, partition);
        let table = operation(
            &writer,
            "table.create",
            json!({
                "name": "items", "schema": {"id": "int64", "text": "utf8"},
                "key": "id", "format": "parquet"
            }),
        )
        .await
        .unwrap();
        let table = serde_json::from_value(table["id"].clone()).unwrap();
        let mut world = Self {
            store,
            reads,
            engine,
            writer,
            threads: Some(threads),
            run,
            term: 1,
            table,
            model: BTreeMap::new(),
        };
        // Distinct sparse keys, inserted out of order in several commits. The
        // gaps are drawn, not rejected, so a shrunk tape always terminates.
        let count = g.below(49) as usize;
        let mut key = g.range_i64(-200, 0);
        let mut keys: Vec<i64> = (0..count)
            .map(|_| {
                key += 1 + g.below(12) as i64;
                key
            })
            .collect();
        for index in (1..keys.len()).rev() {
            keys.swap(index, g.below(index as u64 + 1) as usize);
        }
        let mut rest = keys.as_slice();
        while !rest.is_empty() {
            let take = (1 + g.below(16) as usize).min(rest.len());
            let (chunk, tail) = rest.split_at(take);
            rest = tail;
            let rows: Vec<Value> = chunk
                .iter()
                .map(|key| {
                    let text = format!("r{key}-{}", g.ascii(12));
                    json!({"id": key, "text": text})
                })
                .collect();
            operation(
                &world.writer,
                "table.insert_many",
                json!({"table_id": world.table, "rows": rows}),
            )
            .await
            .unwrap();
            for row in rows {
                world.model.insert(
                    row["id"].as_i64().unwrap(),
                    row["text"].as_str().unwrap().to_owned(),
                );
            }
        }
        world
    }

    /// The pinned manifest's partition objects, in scan order, with row counts.
    async fn files(&self) -> Vec<(String, u64)> {
        let catalog = self.engine.storage().load_cluster_catalog().await.unwrap();
        let entry = catalog
            .catalog()
            .tables
            .values()
            .find(|entry| entry.id == self.table)
            .unwrap();
        match &entry.data {
            TableData::Partitioned { files } => files
                .iter()
                .map(|file| (file.object.clone(), file.row_count))
                .collect(),
            TableData::Legacy { .. } => Vec::new(),
        }
    }

    /// A takeover fence: same run, next scheduler term.
    async fn takeover(&mut self) {
        let base = self.engine.storage().load_cluster_catalog().await.unwrap();
        self.term += 1;
        self.engine
            .storage()
            .fence_cluster_catalog(
                &base,
                CatalogFence {
                    run_id: self.run,
                    scheduler_term: self.term,
                },
                FenceTransition::Takeover,
            )
            .await
            .unwrap();
    }

    /// Every worker stopped. A new run starts on the same committed data.
    async fn cold_start(&mut self) {
        self.run = Uuid::new_v4();
        self.term = 1;
        self.engine
            .storage()
            .initialize_fenced_cluster_catalog(
                CatalogFence {
                    run_id: self.run,
                    scheduler_term: 1,
                },
                false,
                true,
            )
            .await
            .unwrap();
    }

    async fn shutdown(mut self) {
        let threads = self.threads.take().unwrap();
        drop(self);
        threads.shutdown().await.unwrap();
    }
}

/// The stream body the client opens. The oracle evaluates it independently.
#[derive(Clone, Debug)]
struct Spec {
    batch_rows: usize,
    prefetch: u8,
    drop_last: bool,
    range: Option<(i64, i64)>,
    limit: Option<u64>,
}
impl Spec {
    fn generate(g: &mut Gen) -> Self {
        Self {
            batch_rows: 1 + g.below(7) as usize,
            prefetch: g.below(3) as u8,
            drop_last: g.chance(1, 4),
            range: g
                .chance(1, 3)
                .then(|| (g.range_i64(-250, 200), g.range_i64(0, 450))),
            limit: g.chance(1, 5).then(|| g.below(30)),
        }
    }

    fn open(&self, request_id: Uuid, run: Uuid, table: Uuid) -> Open {
        let filter = self.range.map(|(lo, hi)| {
            json!({"op": "and", "args": [
                {"op": "ge", "column": "id", "value": lo},
                {"op": "lt", "column": "id", "value": hi},
            ]})
        });
        Open {
            request_id,
            expected_run_id: run,
            table_id: table,
            query: serde_json::from_value(json!({
                "select": null, "filter": filter,
                "order_by": if self.range.is_some() { json!({"column": "id", "desc": false}) } else { Value::Null },
                "limit": self.limit,
            }))
            .unwrap(),
            batch_rows: self.batch_rows,
            prefetch_batches: self.prefetch,
            format: wire::Format::Rows,
            drop_last: self.drop_last,
            snapshot_id: None,
            shard: 0,
        }
    }

    /// The serial scan of a snapshot, cut into the batches the stream must send.
    fn oracle(&self, model: &BTreeMap<i64, String>) -> Vec<Vec<Value>> {
        let mut rows: Vec<Value> = model
            .iter()
            .filter(|(key, _)| self.range.is_none_or(|(lo, hi)| **key >= lo && **key < hi))
            .map(|(key, text)| json!({"id": key, "text": text}))
            .collect();
        if let Some(limit) = self.limit {
            rows.truncate(limit as usize);
        }
        let mut batches: Vec<Vec<Value>> = rows
            .chunks(self.batch_rows)
            .map(<[Value]>::to_vec)
            .collect();
        if self.drop_last
            && batches
                .last()
                .is_some_and(|last| last.len() < self.batch_rows)
        {
            batches.pop();
        }
        batches
    }
}

fn decode(bytes: &[u8]) -> Result<Vec<Value>, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    match value.get("rows") {
        Some(Value::Array(rows)) => Ok(rows.clone()),
        _ => Err(format!("batch is not a rows frame: {value}")),
    }
}

fn retryable_error(error: &Error) -> bool {
    matches!(error, Error::Protocol(error) if retryable(error))
}

fn code_of(error: &Error) -> String {
    match error {
        Error::Protocol(error) => format!("{:?}", error.code),
        Error::Stream(error) => format!("{:?}", error.code),
    }
}

/// What the client observed for one demand.
#[derive(Debug, PartialEq)]
enum Seen {
    Batch(Vec<u8>),
    Eof,
}

/// Stop a demand after the local install, before it can return a reply.
#[derive(Clone)]
struct BeforeReplication(LocalDriver<Counted>);

impl Driver for BeforeReplication {
    type Store = Counted;

    fn registry(&self) -> &Arc<Registry> {
        self.0.registry()
    }

    fn pool(&self) -> &WorkerPool {
        self.0.pool()
    }

    async fn authority(&self) -> Result<Authority, ProtocolError> {
        self.0.authority().await
    }

    async fn source(
        &self,
        context: Option<RequestContext>,
    ) -> Result<Source<Counted>, ProtocolError> {
        self.0.source(context).await
    }

    async fn catalog(
        &self,
        source: &Source<Counted>,
    ) -> Result<LoadedClusterCatalog, ProtocolError> {
        self.0.catalog(source).await
    }

    async fn commit(
        &self,
        authority: Authority,
        entry: Entry,
        previous: Option<Version>,
        dependency: Option<Dependency>,
    ) -> Result<Arc<Stored>, Error> {
        self.0
            .commit(authority, entry, previous, dependency)
            .await?;
        Err(wire::unavailable().into())
    }
}

/// The coordinator under test, the replica holder, and the client's view.
struct Session<'w> {
    world: &'w mut World,
    bug: Bug,
    spec: Spec,
    local: Local,
    request_id: Uuid,
    opened: Opened,
    files: Vec<(String, u64)>,
    /// The next sequence the client demands.
    next: u64,
    /// True when the coordinator may have committed `next` but the reply was lost.
    pending: bool,
    /// Every frame the client received, by sequence.
    delivered: BTreeMap<u64, Seen>,
    /// Frames whose reply was lost. A later delivery must match them.
    lost: BTreeMap<u64, Seen>,
    /// Index of the first file in the last checkpoint the holder retained.
    frontier: usize,
    rows_scanned: u64,
    rows_behind_checkpoint: u64,
    disruptions: u64,
    log: Vec<String>,
}

impl<'w> Session<'w> {
    fn key(&self) -> Key {
        Key {
            scope: Scope::from_context(None).unwrap(),
            id: self.opened.stream_id,
        }
    }

    async fn call(&mut self, request: Request) -> Result<Reply, Error> {
        let reply = self
            .local
            .call(&self.world.engine, self.world.run, request)
            .await;
        self.account_reads();
        reply
    }

    fn account_reads(&mut self) {
        let reads = std::mem::take(&mut *self.world.reads.lock().unwrap());
        for key in reads {
            let Some(index) = self
                .files
                .iter()
                .position(|(object, _)| key.ends_with(object))
            else {
                continue;
            };
            let rows = self.files[index].1;
            self.rows_scanned += rows;
            if index < self.frontier {
                self.rows_behind_checkpoint += rows;
            }
        }
    }

    fn batch_request(&self, sequence: u64) -> Request {
        Request::Batch {
            run_id: self.opened.run_id,
            stream_id: self.opened.stream_id,
            sequence,
        }
    }

    /// Demand once. Retryable failures return `Ok(None)`.
    async fn demand_once(&mut self, sequence: u64) -> Result<Option<Seen>, String> {
        match self.call(self.batch_request(sequence)).await {
            Ok(Reply::Batch(batch)) => Ok(Some(Seen::Batch(batch.bytes.to_vec()))),
            Ok(Reply::Eof) => Ok(Some(Seen::Eof)),
            Ok(other) => Err(format!("demand {sequence} answered {other:?}")),
            Err(error) if retryable_error(&error) => Ok(None),
            Err(error) => Err(format!(
                "OLAP-STR-G03: demand {sequence} failed with {}: {error}",
                code_of(&error)
            )),
        }
    }

    async fn demand(&mut self, sequence: u64) -> Result<Seen, String> {
        for _ in 0..20 {
            if let Some(seen) = self.demand_once(sequence).await? {
                return Ok(seen);
            }
            self.disruptions += 1;
            tokio::task::yield_now().await;
        }
        Err(format!("demand {sequence} stayed unavailable"))
    }

    /// The checkpoint's first file index, read from the coordinator record.
    fn checkpoint_file(&self) -> Option<usize> {
        let record = self.local.registry.handle(&self.key()).ok()??;
        let Kind::Stream(stream) = &record.entry.kind else {
            return None;
        };
        let cursor = serde_json::to_value(stream.checkpoint.as_ref()?.first).ok()?;
        cursor["file"].as_u64().map(|file| file as usize)
    }

    fn receive(&mut self, sequence: u64, seen: Seen) -> Result<(), String> {
        if let Some(lost) = self.lost.remove(&sequence)
            && lost != seen
        {
            return Err(format!(
                "OLAP-STR-G03: sequence {sequence} changed after a lost reply"
            ));
        }
        match self.delivered.get(&sequence) {
            Some(previous) if *previous != seen => {
                return Err(format!(
                    "OLAP-STR-G03: replay of sequence {sequence} differs from the original"
                ));
            }
            Some(_) => {}
            None => {
                self.delivered.insert(sequence, seen);
            }
        }
        self.note_checkpoint();
        Ok(())
    }

    fn note_checkpoint(&mut self) {
        if let Some(file) = self.checkpoint_file() {
            self.frontier = self.frontier.max(file);
        }
    }

    fn eof(&self) -> bool {
        self.delivered.values().any(|seen| *seen == Seen::Eof)
    }

    /// The sequence of the coordinator's checkpoint, as the client knows it.
    fn checkpoint_sequence(&self) -> Option<u64> {
        if self.pending {
            Some(self.next)
        } else {
            self.next.checked_sub(1)
        }
    }

    /// Demand the next frame and receive it.
    async fn advance(&mut self) -> Result<(), String> {
        let sequence = self.next;
        let seen = self.demand(sequence).await?;
        self.receive(sequence, seen)?;
        self.pending = false;
        self.next += 1;
        Ok(())
    }

    /// The coordinator commits the demand, then its reply is lost.
    async fn lose_reply(&mut self) -> Result<(), String> {
        let sequence = self.next;
        let seen = self.demand(sequence).await?;
        self.note_lost(sequence, seen)
    }

    fn note_lost(&mut self, sequence: u64, seen: Seen) -> Result<(), String> {
        match self.lost.entry(sequence) {
            std::collections::btree_map::Entry::Occupied(entry) if *entry.get() != seen => {
                return Err(format!(
                    "OLAP-STR-G03: sequence {sequence} changed after a lost reply"
                ));
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(seen);
            }
            _ => {}
        }
        self.note_checkpoint();
        self.pending = true;
        Ok(())
    }

    /// OLAP-STR-G03: the current sequence replays, stale and future ones conflict.
    async fn probe_sequences(&mut self) -> Result<(), String> {
        let Some(current) = self.checkpoint_sequence() else {
            return Ok(());
        };
        let seen = self.demand(current).await?;
        if self.pending {
            self.note_lost(current, seen)?;
        } else {
            self.receive(current, seen)?;
        }
        let mut wrong = vec![current + 2];
        if current > 0 {
            wrong.push(current - 1);
        }
        if self.eof() {
            wrong.push(current + 1);
        }
        for sequence in wrong {
            self.expect_conflict(self.batch_request(sequence))
                .await
                .map_err(|error| {
                    format!("OLAP-STR-G03: demand {sequence} at checkpoint {current}: {error}")
                })?;
        }
        Ok(())
    }

    async fn expect_conflict(&mut self, request: Request) -> Result<(), String> {
        for _ in 0..20 {
            match self.call(request.clone()).await {
                Err(Error::Protocol(error)) if error.code == ErrorCode::Conflict => return Ok(()),
                Err(error) if retryable_error(&error) => self.disruptions += 1,
                Ok(reply) => return Err(format!("answered {reply:?}")),
                Err(error) => return Err(format!("answered {}", code_of(&error))),
            }
        }
        Err("stayed unavailable".into())
    }

    /// OLAP-STR-G02: an open retry with the same UUID and body replays the handle.
    async fn reopen(&mut self, g: &mut Gen) -> Result<(), String> {
        if self.bug == Bug::RescanOnReconnect {
            return self.reconnect_from_start().await;
        }
        if self.bug == Bug::ReplayNewHandle {
            // Planted: the identity table is lost, so the retry opens afresh.
            self.local = Local::new();
        }
        if g.chance(1, 4) {
            let mut conflicting = self.spec.clone();
            conflicting.batch_rows = conflicting.batch_rows % 7 + 1;
            let body = conflicting.open(self.request_id, self.world.run, self.world.table);
            self.expect_conflict(Request::Open(body))
                .await
                .map_err(|error| {
                    format!("OLAP-STR-G02: conflicting body for the same UUID: {error}")
                })?;
        }
        let body = self
            .spec
            .open(self.request_id, self.world.run, self.world.table);
        for _ in 0..20 {
            match self.call(Request::Open(body.clone())).await {
                Ok(Reply::Opened(opened)) => {
                    if opened != self.opened {
                        return Err(format!(
                            "OLAP-STR-G02: open replay returned handle {} instead of {}",
                            opened.stream_id, self.opened.stream_id
                        ));
                    }
                    return Ok(());
                }
                Err(error) if retryable_error(&error) => self.disruptions += 1,
                other => return Err(format!("OLAP-STR-G02: open replay answered {other:?}")),
            }
        }
        Err("open replay stayed unavailable".into())
    }

    /// The replicated record a surviving holder retains, as replicated bytes.
    fn replicate(&self) -> Option<Entry> {
        let record = self.local.registry.handle(&self.key()).ok()??;
        let bytes = serde_json::to_vec(&record.entry).unwrap();
        Some(serde_json::from_slice(&bytes).unwrap())
    }

    /// Kill the coordinator and recover its record on a new owner (OLAP-STR-G04).
    async fn kill(&mut self, g: &mut Gen) -> Result<(), String> {
        let mut cut = g.below(3);
        if cut == 1 && self.pending {
            // This demand already replicated, so there is no new install to interrupt.
            cut = 0;
        }
        let mut holder = self.replicate();
        let lost = match cut {
            // Between demand and ack, after the checkpoint replicated.
            0 => {
                let sequence = self.next;
                let seen = self.demand(sequence).await?;
                holder = self.replicate();
                Some((sequence, seen))
            }
            // Between demand and ack, before the checkpoint replicated.
            1 => {
                let driver = BeforeReplication(LocalDriver {
                    registry: self.local.registry.clone(),
                    changes: self.local.changes.clone(),
                    engine: self.world.engine.clone(),
                    run: self.world.run,
                });
                let reply = call(driver, self.batch_request(self.next), None).await;
                self.account_reads();
                let installed = self.replicate();
                let advanced = matches!((&holder, &installed), (Some(old), Some(new))
                    if old.version != new.version
                        && matches!(&new.kind, Kind::Stream(stream)
                            if stream.checkpoint.as_ref().is_some_and(|c| c.sequence == self.next)));
                if !advanced || !matches!(&reply, Err(error) if retryable_error(error)) {
                    return Err(format!(
                        "OLAP-STR-G04: demand {} did not stop after the local install: {reply:?}",
                        self.next
                    ));
                }
                None
            }
            // Idle, with the holder's most recent complete checkpoint.
            _ => None,
        };
        let Some(mut entry) = holder else {
            return Err("the replica holder lost the stream record".into());
        };
        self.log
            .push(format!("kill cut {cut} at sequence {}", self.next));
        // The old coordinator stops. A takeover fences it out.
        self.local = Local::new();
        self.world.takeover().await;
        let recovered = match &entry.kind {
            Kind::Stream(stream) => stream.checkpoint.as_ref().map(|c| c.sequence),
            Kind::Snapshot(_) => None,
        };
        if self.bug == Bug::GapAfterKill
            && let Kind::Stream(stream) = &mut entry.kind
            && let Some(checkpoint) = &mut stream.checkpoint
            && checkpoint.sequence > 0
        {
            checkpoint.sequence -= 1;
        }
        self.install(entry)?;
        self.disruptions += 1;
        if let Some((sequence, seen)) = lost {
            self.note_lost(sequence, seen)?;
        }
        // After replication the checkpoint holds the unacknowledged frame;
        // before it, the new owner resumes from the previous checkpoint.
        self.pending = recovered == Some(self.next);
        Ok(())
    }

    fn install(&mut self, entry: Entry) -> Result<(), String> {
        let registry = &self.local.registry;
        registry
            .set_run(self.world.run, false)
            .map_err(|e| e.to_string())?;
        let bytes = record_bytes(&entry).unwrap();
        registry
            .install_fenced(
                Authority {
                    run: self.world.run,
                    term: self.world.term,
                },
                entry,
                None,
                bytes,
                self.world.engine.worker_pool().scheduler(),
                now_ms(),
            )
            .map_err(|error| format!("recovery install failed: {error}"))?
            .ok_or("recovery install dropped the record")?;
        Ok(())
    }

    /// Planted: a reconnect rebuilds from the first cursor and silently replays.
    async fn reconnect_from_start(&mut self) -> Result<(), String> {
        let Some(mut entry) = self.replicate() else {
            return Ok(());
        };
        let target = self.checkpoint_sequence();
        if let Kind::Stream(stream) = &mut entry.kind {
            stream.checkpoint = None;
        }
        self.local = Local::new();
        self.install(entry)?;
        if let Some(target) = target {
            for sequence in 0..=target {
                self.demand(sequence).await?;
            }
        }
        Ok(())
    }

    /// Commits that land during the stream (Vanlightly's ConsistentRead).
    async fn commit(&mut self, g: &mut Gen) -> Result<(), String> {
        let table = self.world.table;
        let existing: Vec<i64> = self.world.model.keys().copied().collect();
        match g.below(3) {
            0 if !existing.is_empty() => {
                let key = *g.pick(&existing);
                operation(
                    &self.world.writer,
                    "table.delete",
                    json!({"table_id": table, "key": key}),
                )
                .await?;
                self.world.model.remove(&key);
            }
            1 if !existing.is_empty() => {
                let key = *g.pick(&existing);
                let text = format!("u{key}-{}", g.ascii(8));
                operation(
                    &self.world.writer,
                    "table.upsert",
                    json!({"table_id": table, "row": {"id": key, "text": text}}),
                )
                .await?;
                self.world.model.insert(key, text);
            }
            _ => {
                let key = g.range_i64(-200, 400);
                if self.world.model.contains_key(&key) {
                    return Ok(());
                }
                let text = format!("n{key}");
                operation(
                    &self.world.writer,
                    "table.insert",
                    json!({"table_id": table, "row": {"id": key, "text": text}}),
                )
                .await?;
                self.world.model.insert(key, text);
            }
        }
        self.log.push("commit".into());
        Ok(())
    }

    /// Seeded GET faults (503) for one demand.
    async fn faulted_advance(&mut self, g: &mut Gen) -> Result<(), String> {
        let seed = g.u64();
        let per_mille = 100 + g.below(500);
        self.world
            .store
            .random_faults(seed, per_mille, vec![Fault::Status(503)]);
        let sequence = self.next;
        let attempt = self.demand_once(sequence).await;
        self.world.store.clear_faults();
        match attempt? {
            Some(seen) => {
                self.receive(sequence, seen)?;
                self.pending = false;
                self.next += 1;
            }
            None => self.disruptions += 1,
        }
        Ok(())
    }

    /// OLAP-STR-G01 and G06 at the end of the stream.
    async fn finish(&mut self, oracle: &[Vec<Value>]) -> Result<(), String> {
        while !self.eof() && self.delivered.len() <= oracle.len() {
            self.advance().await?;
        }
        // EOF itself must replay, and every later sequence must conflict.
        if self.eof() {
            self.probe_sequences().await?;
        }
        self.verify(oracle)
    }

    fn verify(&mut self, oracle: &[Vec<Value>]) -> Result<(), String> {
        self.account_reads();
        let mut batches = Vec::new();
        for (expected, (sequence, seen)) in (0_u64..).zip(&self.delivered) {
            if *sequence != expected {
                return Err(format!(
                    "OLAP-STR-G01: sequence gap, expected {expected} but got {sequence}"
                ));
            }
            match seen {
                Seen::Batch(bytes) => batches.push(decode(bytes)?),
                Seen::Eof => {
                    if *sequence + 1 != self.delivered.len() as u64 {
                        return Err("OLAP-STR-G01: frames after EOF".into());
                    }
                }
            }
        }
        if !self.eof() {
            return Err("OLAP-STR-G01: the stream never reached EOF".into());
        }
        let actual: Vec<&Value> = batches.iter().flatten().collect();
        let wanted: Vec<&Value> = oracle.iter().flatten().collect();
        if actual != wanted {
            return Err(format!(
                "OLAP-STR-G01: delivered {} rows that differ from the {}-row serial scan; first difference at row {}",
                actual.len(),
                wanted.len(),
                actual
                    .iter()
                    .zip(&wanted)
                    .position(|(a, b)| a != b)
                    .unwrap_or(actual.len().min(wanted.len()))
            ));
        }
        let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
        let wanted_sizes: Vec<usize> = oracle.iter().map(Vec::len).collect();
        if sizes != wanted_sizes {
            return Err(format!(
                "OLAP-STR-G01: batch sizes {sizes:?}, expected {wanted_sizes:?}"
            ));
        }
        if self.rows_behind_checkpoint > 0 {
            return Err(format!(
                "OLAP-STR-G06: rescanned {} rows behind the checkpoint ({} scanned, {} delivered)",
                self.rows_behind_checkpoint,
                self.rows_scanned,
                actual.len()
            ));
        }
        Ok(())
    }
}

async fn open_session<'w>(
    world: &'w mut World,
    g: &mut Gen,
    bug: Bug,
) -> Result<Session<'w>, String> {
    let spec = Spec::generate(g);
    let request_id = Uuid::new_v4();
    let files = world.files().await;
    let local = Local::new();
    let body = spec.open(request_id, world.run, world.table);
    // OLAP-STR-G02: racing opens of one identity get one handle.
    let opened = if g.chance(1, 3) {
        let (a, b) = tokio::join!(
            local.call(&world.engine, world.run, Request::Open(body.clone())),
            local.call(&world.engine, world.run, Request::Open(body.clone())),
        );
        match (a, b) {
            (Ok(Reply::Opened(a)), Ok(Reply::Opened(b))) if a == b => a,
            (a, b) => {
                return Err(format!(
                    "OLAP-STR-G02: racing opens answered {a:?} and {b:?}"
                ));
            }
        }
    } else {
        match local
            .call(&world.engine, world.run, Request::Open(body))
            .await
        {
            Ok(Reply::Opened(opened)) => opened,
            other => return Err(format!("open answered {other:?}")),
        }
    };
    Ok(Session {
        world,
        bug,
        spec,
        local,
        request_id,
        opened,
        files,
        next: 0,
        pending: false,
        delivered: BTreeMap::new(),
        lost: BTreeMap::new(),
        frontier: 0,
        rows_scanned: 0,
        rows_behind_checkpoint: 0,
        disruptions: 0,
        log: Vec::new(),
    })
}

/// One simulated case: a stream under the nemesis, then an optional cold start.
async fn case(g: &mut Gen, bug: Bug) -> Result<(), String> {
    let mut world = World::new(g).await;
    let pinned = world.model.clone();
    let result = async {
        let mut session = open_session(&mut world, g, bug).await?;
        let oracle = session.spec.oracle(&pinned);
        let mut steps = 0;
        while !session.eof() && steps < MAX_STEPS {
            steps += 1;
            match g.below(100) {
                0..=49 => session.advance().await?,
                50..=59 => session.lose_reply().await?,
                60..=67 => session.probe_sequences().await?,
                68..=74 => session.reopen(g).await?,
                75..=82 => session.kill(g).await?,
                83..=92 => session.commit(g).await?,
                _ => session.faulted_advance(g).await?,
            }
        }
        session
            .finish(&oracle)
            .await
            .map_err(|error| format!("{error}; spec {:?}; {:?}", session.spec, session.log))?;
        if std::env::var_os("NABEDRA_SIM_TRACE").is_some() {
            eprintln!(
                "olap-stream-sim: {} pinned rows in {} files, {} frames, {} scanned rows, {} disruptions, {:?}",
                pinned.len(),
                session.files.len(),
                session.delivered.len(),
                session.rows_scanned,
                session.disruptions,
                session.log
            );
        }

        if g.chance(1, 3) {
            cold_start(session, g).await?;
        }
        Ok(())
    }
    .await;
    world.shutdown().await;
    result
}

/// OLAP-STR-G05: after every worker stops, old handles are abandoned and a
/// new open pins the latest committed data.
async fn cold_start(mut session: Session<'_>, g: &mut Gen) -> Result<(), String> {
    // Leave a fresh open outstanding when every holder stops.
    let request_id = Uuid::new_v4();
    let body = session
        .spec
        .open(request_id, session.world.run, session.world.table);
    let Ok(Reply::Opened(old)) = session.call(Request::Open(body)).await else {
        return Err("OLAP-STR-G05: could not create an outstanding open".into());
    };
    session.opened = old.clone();
    session.next = 0;
    session.local = Local::new();
    session.world.cold_start().await;
    let new_run = session.world.run;
    match session.call(session.batch_request(session.next)).await {
        Err(Error::Stream(error)) if error.code == wire::StreamCode::StreamAbandoned => {}
        other => {
            return Err(format!(
                "OLAP-STR-G05: an old-run demand answered {other:?}"
            ));
        }
    }
    match session
        .call(Request::Batch {
            run_id: new_run,
            stream_id: old.stream_id,
            sequence: session.next,
        })
        .await
    {
        Err(Error::Stream(error)) if error.code == wire::StreamCode::StreamExpired => {}
        other => {
            return Err(format!(
                "OLAP-STR-G05: the old handle under the new run answered {other:?}"
            ));
        }
    }
    let stale = session
        .spec
        .open(request_id, old.run_id, session.world.table);
    match session.call(Request::Open(stale)).await {
        Err(Error::Stream(error)) if error.code == wire::StreamCode::StreamAbandoned => {}
        other => {
            return Err(format!(
                "OLAP-STR-G05: an open retry for the old run answered {other:?}"
            ));
        }
    }
    // The same request ID in the new run is a new stream on the latest data.
    let body = session.spec.open(request_id, new_run, session.world.table);
    let Ok(Reply::Opened(opened)) = session.call(Request::Open(body)).await else {
        return Err("OLAP-STR-G05: a new-run open failed".into());
    };
    if opened.stream_id == old.stream_id || opened.run_id != new_run {
        return Err("OLAP-STR-G05: the new run reused the abandoned handle".into());
    }
    let latest = session.world.model.clone();
    session.opened = opened;
    session.next = 0;
    session.pending = false;
    session.delivered.clear();
    session.lost.clear();
    session.frontier = 0;
    session.rows_scanned = 0;
    session.rows_behind_checkpoint = 0;
    session.files = session.world.files().await;
    let oracle = session.spec.oracle(&latest);
    while !session.eof() && session.delivered.len() <= oracle.len() {
        if g.chance(1, 4) {
            session.lose_reply().await?;
        }
        session.advance().await?;
    }
    session.finish(&oracle).await
}

fn run_case(g: &mut Gen, bug: Bug) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(case(g, bug))
}

#[test]
fn st01_st06_olap_stream_simulation() {
    // A case takes about 0.2 s, so bound shrinking to a few minutes.
    let config = prop::Config {
        max_shrink_steps: 600,
        ..prop::Config::default()
    };
    prop::check_with(config, "olap-stream-sim", |g| run_case(g, Bug::None));
}

/// Each planted bug must make the same property fail.
fn planted(bug: Bug, expect: &str) {
    let cases = std::env::var("NABEDRA_PLANTED_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(120);
    let config = prop::Config {
        cases,
        seed: Some(0x5157_0000 + bug as u64),
        max_shrink_steps: 0,
    };
    let caught = std::panic::catch_unwind(|| {
        prop::check_with(config, "olap-stream-sim-planted", |g| run_case(g, bug));
    });
    let Err(panic) = caught else {
        panic!("planted bug {bug:?} was not caught in {cases} cases");
    };
    let message = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        message.contains(expect),
        "planted bug {bug:?} failed for another reason: {message}"
    );
    eprintln!("planted bug {bug:?} caught: {message}");
}

#[test]
fn st04_st06_planted_rescan_after_reconnect_is_caught() {
    planted(Bug::RescanOnReconnect, "OLAP-STR-G06");
}

#[test]
fn st04_planted_sequence_gap_after_kill_is_caught() {
    planted(Bug::GapAfterKill, "OLAP-STR-G03");
}

#[test]
fn st04_planted_replay_with_new_handle_is_caught() {
    planted(Bug::ReplayNewHandle, "OLAP-STR-G02");
}
