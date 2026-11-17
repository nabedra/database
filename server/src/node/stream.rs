//! OLAP stream leases and deterministic batch replay. Only workers own lookahead.
mod distributed;
mod state;
pub(crate) mod wire;

pub(super) use distributed::{PATH, handler};
pub(crate) use distributed::{release, remote, renew};
pub(super) use state::Registry;

use self::{
    state::{
        Activity, Ahead, Checkpoint, Delivery, Entry, Frame, Key, Kind, Output, Scope, Snapshot,
        Stored, Stream, Version, retryable,
    },
    wire::{
        Batch, Capabilities, Error, Open, OpenSnapshot, Opened, Reply, Request, SnapshotOpened,
    },
};
use super::{
    queue_runtime::now_ms,
    queue_transport::{encode_exact, encoded_len},
    runtime::DataGrant,
};
use crate::{
    engine::{
        Engine,
        stream::{Cursor, PinnedScan, ScanReader},
    },
    protocol::{ErrorCode, MAX_CATALOG_BYTES, ProtocolError, Query},
    storage::{LoadedClusterCatalog, ObjectStore, Storage, context::RequestContext},
    worker_pool::WorkerPool,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit};
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq)]
struct Authority {
    run: Uuid,
    term: u64,
}
enum Source<B: ObjectStore> {
    Local(Engine<B>),
    Worker {
        storage: Arc<Storage<B>>,
        database: Option<DataGrant>,
    },
}
impl<B: ObjectStore> Source<B> {
    fn storage(&self) -> &Storage<B> {
        match self {
            Self::Local(engine) => engine.storage(),
            Self::Worker { storage, .. } => storage,
        }
    }
}
struct Dependency {
    key: Key,
    version: Version,
}
trait Driver: Clone + Send + Sync + 'static {
    type Store: ObjectStore + 'static;
    fn registry(&self) -> &Arc<Registry>;
    fn pool(&self) -> &WorkerPool;
    fn authority(&self) -> impl Future<Output = Result<Authority, ProtocolError>> + Send;
    fn source(
        &self,
        context: Option<RequestContext>,
    ) -> impl Future<Output = Result<Source<Self::Store>, ProtocolError>> + Send;
    fn catalog(
        &self,
        source: &Source<Self::Store>,
    ) -> impl Future<Output = Result<LoadedClusterCatalog, ProtocolError>> + Send;
    fn commit(
        &self,
        authority: Authority,
        entry: Entry,
        previous: Option<Version>,
        dependency: Option<Dependency>,
    ) -> impl Future<Output = Result<Arc<Stored>, Error>> + Send;
    fn may_speculate(&self) -> bool {
        let (_, occupied, bytes) = self.pool().scheduler().usage()[0];
        occupied < self.pool().resources().max_threads
            && bytes < self.pool().resources().working_bytes() / 2
    }
}

pub(crate) struct Local {
    registry: Arc<Registry>,
    changes: Arc<Mutex<()>>,
}
impl Local {
    pub(crate) fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            changes: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) async fn call<B: ObjectStore + 'static>(
        &self,
        engine: &Engine<B>,
        run_id: Uuid,
        request: Request,
    ) -> Result<Reply, Error> {
        self.registry.set_run(run_id, false)?;
        if !self.registry.reaper_started.swap(true, Ordering::AcqRel) {
            let registry = Arc::downgrade(&self.registry);
            let pool = engine.worker_pool().clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let Some(registry) = registry.upgrade() else {
                        break;
                    };
                    if registry.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    registry.reap(now_ms());
                    if memory_pressure(&pool) {
                        registry.evict_speculation();
                    }
                }
            });
        }
        let driver = LocalDriver {
            registry: self.registry.clone(),
            changes: self.changes.clone(),
            engine: engine.clone(),
            run: run_id,
        };
        call(driver, request, None).await
    }
}
impl Drop for Local {
    fn drop(&mut self) {
        self.registry.stop();
    }
}
struct LocalDriver<B: ObjectStore> {
    registry: Arc<Registry>,
    changes: Arc<Mutex<()>>,
    engine: Engine<B>,
    run: Uuid,
}
impl<B: ObjectStore> Clone for LocalDriver<B> {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            changes: self.changes.clone(),
            engine: self.engine.clone(),
            run: self.run,
        }
    }
}
impl<B: ObjectStore + 'static> Driver for LocalDriver<B> {
    type Store = B;

    fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
    fn pool(&self) -> &WorkerPool {
        self.engine.worker_pool()
    }

    async fn authority(&self) -> Result<Authority, ProtocolError> {
        if self.registry.stopped.load(Ordering::Acquire) {
            return Err(wire::unavailable());
        }
        let catalog = self
            .engine
            .storage()
            .load_cluster_catalog()
            .await
            .map_err(|_| wire::unavailable())?;
        let fence = catalog.catalog().fence.ok_or_else(wire::unavailable)?;
        if fence.run_id != self.run {
            return Err(wire::unavailable());
        }
        Ok(Authority {
            run: fence.run_id,
            term: fence.scheduler_term,
        })
    }

    async fn source(&self, context: Option<RequestContext>) -> Result<Source<B>, ProtocolError> {
        if context.is_some() {
            return Err(wire::unauthorized());
        }
        Ok(Source::Local(self.engine.clone()))
    }

    async fn catalog(&self, source: &Source<B>) -> Result<LoadedClusterCatalog, ProtocolError> {
        source
            .storage()
            .load_cluster_catalog()
            .await
            .map_err(|_| wire::unavailable())
    }

    async fn commit(
        &self,
        authority: Authority,
        entry: Entry,
        previous: Option<Version>,
        dependency: Option<Dependency>,
    ) -> Result<Arc<Stored>, Error> {
        let _change = self.changes.lock().await;
        check_authority(self, authority).await?;
        check_dependency(&self.registry, dependency)?;
        install(self, entry, previous)
    }
}

async fn call<D: Driver>(
    driver: D,
    request: Request,
    context: Option<RequestContext>,
) -> Result<Reply, Error> {
    let authority = driver.authority().await?;
    let now = now_ms();
    let scope = Scope::from_context(context.as_ref())?;
    driver.registry().reap(now);
    match request {
        Request::Capabilities => Ok(Reply::Capabilities(Capabilities {
            version: 1,
            run_id: authority.run,
            max_batch_rows: wire::MAX_BATCH_ROWS,
            max_batch_bytes: wire::MAX_BATCH_BYTES,
            max_prefetch_batches: prefetch_ceiling(driver.pool()),
        })),
        Request::Open(open) => open_stream(driver, authority, scope, context, open, now).await,
        Request::OpenSnapshot(open) => {
            open_snapshot(driver, authority, scope, context, open, now).await
        }
        Request::Batch {
            run_id,
            stream_id,
            sequence,
        } => {
            check_run(authority, run_id)?;
            demand(
                driver,
                authority,
                Key {
                    scope,
                    id: stream_id,
                },
                context,
                sequence,
                now,
            )
            .await
        }
        Request::Close { run_id, stream_id } => {
            check_run(authority, run_id)?;
            close(
                &driver,
                authority,
                Key {
                    scope,
                    id: stream_id,
                },
                false,
                now,
            )
            .await
        }
        Request::CloseSnapshot {
            run_id,
            snapshot_id,
        } => {
            let (snapshot_run, id) = wire::parse_snapshot_id(&snapshot_id)?;
            check_run(authority, run_id)?;
            check_run(authority, snapshot_run)?;
            close(&driver, authority, Key { scope, id }, true, now).await
        }
    }
}

fn check_run(authority: Authority, requested: Uuid) -> Result<(), Error> {
    if requested.is_nil() {
        return Err(wire::invalid().into());
    }
    if requested != authority.run {
        return Err(Error::abandoned());
    }
    Ok(())
}

async fn check_authority<D: Driver>(driver: &D, expected: Authority) -> Result<(), Error> {
    let observed = driver.authority().await?;
    if observed.run != expected.run {
        return Err(Error::abandoned());
    }
    if observed.term != expected.term {
        return Err(wire::unavailable().into());
    }
    Ok(())
}

fn check_dependency(registry: &Registry, dependency: Option<Dependency>) -> Result<(), Error> {
    if let Some(dependency) = dependency
        && registry.get(&dependency.key).is_none_or(|record| {
            record.entry.version != dependency.version || !record.entry.active(now_ms())
        })
    {
        return Err(Error::expired());
    }
    Ok(())
}

fn install<D: Driver>(
    driver: &D,
    entry: Entry,
    previous: Option<Version>,
) -> Result<Arc<Stored>, Error> {
    let bytes = record_bytes(&entry)?;
    driver
        .registry()
        .install(
            entry,
            Some(previous),
            bytes,
            driver.pool().scheduler(),
            now_ms(),
        )?
        .ok_or_else(Error::expired)
}

fn record_bytes(entry: &Entry) -> Result<u64, ProtocolError> {
    let bytes = encoded_len(entry, state::MAX_RECORD_BYTES)?;
    // The validated manifest carries owned maps, vectors and scalar keys in RAM.
    Ok(bytes as u64 * 4 + 4096)
}

fn prefetch_ceiling(pool: &WorkerPool) -> u8 {
    let slots = pool.resources().working_bytes() / (16 * wire::MAX_BATCH_BYTES as u64);
    slots.min(u64::from(wire::MAX_PREFETCH)) as u8
}

pub(in crate::node) fn memory_pressure(pool: &WorkerPool) -> bool {
    let (_, _, bytes) = pool.scheduler().usage()[0];
    let resources = pool.resources();
    bytes.saturating_add(resources.task_memory_limit_bytes) > resources.working_bytes()
}

fn accepted_body(value: &impl serde::Serialize) -> Result<([u8; 32], Arc<str>), ProtocolError> {
    let length = encoded_len(value, wire::MAX_REQUEST_BYTES)?;
    let bytes = encode_exact(value, length)?;
    let identity = Sha256::digest(&bytes).into();
    let text = String::from_utf8(bytes).map_err(|_| wire::invalid())?;
    Ok((identity, Arc::from(text)))
}

async fn capture<D: Driver>(
    driver: &D,
    authority: Authority,
    context: Option<RequestContext>,
    table_id: Uuid,
    query: Query,
) -> Result<(Arc<PinnedScan>, crate::scheduling::MemoryCharge), Error> {
    let source = driver.source(context).await?;
    let charge = driver
        .pool()
        .scheduler()
        .charge_memory(0, 8 * MAX_CATALOG_BYTES as u64)?;
    let loaded = driver.catalog(&source).await?;
    let catalog = loaded.catalog();
    let fence = catalog.fence.ok_or_else(wire::unavailable)?;
    if fence.run_id != authority.run || fence.scheduler_term != authority.term {
        return Err(wire::unavailable().into());
    }
    let scan = PinnedScan::capture_cluster(catalog, table_id, query)?;
    Ok((Arc::new(scan), charge))
}

async fn sync<D: Driver>(
    driver: &D,
    authority: Authority,
    stored: Arc<Stored>,
) -> Result<Arc<Stored>, Error> {
    let mut entry = stored.entry.clone();
    if entry.version.term != authority.term {
        entry.version = Version::advance(Some(entry.version), authority.term)?;
    }
    driver
        .commit(authority, entry, Some(stored.entry.version), None)
        .await
}

async fn open_snapshot<D: Driver>(
    driver: D,
    authority: Authority,
    scope: Scope,
    context: Option<RequestContext>,
    open: OpenSnapshot,
    now: u64,
) -> Result<Reply, Error> {
    check_run(authority, open.expected_run_id)?;
    if open.request_id.is_nil() || !(1..=wire::MAX_SHARDS).contains(&open.shards) {
        return Err(wire::invalid().into());
    }
    let _body_charge = driver
        .pool()
        .scheduler()
        .charge_memory(0, 2 * wire::MAX_REQUEST_BYTES as u64)?;
    let (digest, body) = accepted_body(&open)?;
    let key = Key {
        scope,
        id: open.request_id,
    };
    if let Some(previous) = driver.registry().get(&key) {
        return reopen(&driver, authority, previous, digest, true).await;
    }
    let (scan, _working) = capture(
        &driver,
        authority,
        context.clone(),
        open.table_id,
        open.query,
    )
    .await?;
    scan.cursor(0, open.shards)?;
    let handle_id = Uuid::new_v4();
    let opened = SnapshotOpened {
        snapshot_id: wire::snapshot_id(authority.run, handle_id),
        run_id: authority.run,
        expires_at: now.saturating_add(state::LIFETIME_MS),
    };
    let entry = Entry {
        key,
        run: authority.run,
        version: Version::advance(None, authority.term)?,
        identity: digest,
        accepted_request: Some(body),
        context,
        scan: Some(scan),
        kind: Kind::Snapshot(Snapshot {
            opened: opened.clone(),
            handle_id,
            shards: open.shards,
        }),
        idle_at: opened.expires_at,
        closed_at: None,
    };
    commit_open(driver, authority, entry, None).await
}

async fn open_stream<D: Driver>(
    driver: D,
    authority: Authority,
    scope: Scope,
    context: Option<RequestContext>,
    open: Open,
    now: u64,
) -> Result<Reply, Error> {
    check_run(authority, open.expected_run_id)?;
    if open.request_id.is_nil()
        || !(1..=wire::MAX_BATCH_ROWS).contains(&open.batch_rows)
        || open.prefetch_batches > wire::MAX_PREFETCH
        || (open.snapshot_id.is_none() && open.shard != 0)
    {
        return Err(wire::invalid().into());
    }
    let _body_charge = driver
        .pool()
        .scheduler()
        .charge_memory(0, 2 * wire::MAX_REQUEST_BYTES as u64)?;
    let (digest, body) = accepted_body(&open)?;
    let key = Key {
        scope,
        id: open.request_id,
    };
    if let Some(previous) = driver.registry().get(&key) {
        return reopen(&driver, authority, previous, digest, false).await;
    }

    let (scan, first, expires_at, dependency, _working) = match &open.snapshot_id {
        Some(snapshot_id) => {
            let (run, id) = wire::parse_snapshot_id(snapshot_id)?;
            check_run(authority, run)?;
            let snapshot_key = Key {
                scope: key.scope.clone(),
                id,
            };
            let record = driver
                .registry()
                .handle(&snapshot_key)?
                .ok_or_else(Error::expired)?;
            let Kind::Snapshot(snapshot) = &record.entry.kind else {
                return Err(wire::conflict().into());
            };
            if !record.entry.active(now) {
                return Err(Error::expired());
            }
            let scan = record.entry.scan.clone().ok_or_else(Error::expired)?;
            scan.check_query(open.table_id, &open.query)?;
            let first = scan.cursor(open.shard, snapshot.shards)?;
            // Hold the shared reservation during construction, until the
            // new rank takes its own replication and pin reservation.
            (
                scan,
                first,
                snapshot.opened.expires_at,
                Some(Dependency {
                    key: record.entry.key.clone(),
                    version: record.entry.version,
                }),
                Some(record),
            )
        }
        None => {
            let (scan, working) = capture(
                &driver,
                authority,
                context.clone(),
                open.table_id,
                open.query.clone(),
            )
            .await?;
            let first = scan.cursor(0, 1)?;
            let opened = Opened {
                stream_id: Uuid::new_v4(),
                run_id: authority.run,
                prefetch_batches: open.prefetch_batches.min(prefetch_ceiling(driver.pool())),
                expires_at: now.saturating_add(state::LIFETIME_MS),
            };
            let entry = stream_entry(
                authority, context, digest, body, key, &open, opened, scan, first, now,
            )?;
            let reply = commit_open(driver, authority, entry, None).await;
            drop(working);
            return reply;
        }
    };
    let opened = Opened {
        stream_id: Uuid::new_v4(),
        run_id: authority.run,
        prefetch_batches: open.prefetch_batches.min(prefetch_ceiling(driver.pool())),
        expires_at,
    };
    let entry = stream_entry(
        authority, context, digest, body, key, &open, opened, scan, first, now,
    )?;
    commit_open(driver, authority, entry, dependency).await
}

async fn commit_open<D: Driver>(
    driver: D,
    authority: Authority,
    entry: Entry,
    dependency: Option<Dependency>,
) -> Result<Reply, Error> {
    let key = entry.key.clone();
    let digest = entry.identity;
    let snapshot = matches!(entry.kind, Kind::Snapshot(_));
    let record = match driver.commit(authority, entry, None, dependency).await {
        Ok(record) => record,
        Err(Error::Protocol(error)) if error.code == ErrorCode::Conflict => {
            let previous = driver.registry().get(&key).ok_or(error)?;
            return reopen(&driver, authority, previous, digest, snapshot).await;
        }
        Err(error) => return Err(error),
    };
    match &record.entry.kind {
        Kind::Snapshot(snapshot) => Ok(Reply::SnapshotOpened(snapshot.opened.clone())),
        Kind::Stream(stream) => {
            let opened = stream.opened.clone();
            start_prefetch(driver, authority, record);
            Ok(Reply::Opened(opened))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn stream_entry(
    authority: Authority,
    context: Option<RequestContext>,
    identity: [u8; 32],
    accepted_request: Arc<str>,
    key: Key,
    open: &Open,
    opened: Opened,
    scan: Arc<PinnedScan>,
    first: Cursor,
    now: u64,
) -> Result<Entry, ProtocolError> {
    Ok(Entry {
        key,
        run: authority.run,
        version: Version::advance(None, authority.term)?,
        identity,
        accepted_request: Some(accepted_request),
        context,
        scan: Some(scan),
        kind: Kind::Stream(Stream {
            opened,
            batch_rows: open.batch_rows,
            format: open.format,
            drop_last: open.drop_last,
            first,
            checkpoint: None,
        }),
        idle_at: now.saturating_add(state::IDLE_MS),
        closed_at: None,
    })
}

async fn reopen<D: Driver>(
    driver: &D,
    authority: Authority,
    stored: Arc<Stored>,
    digest: [u8; 32],
    snapshot: bool,
) -> Result<Reply, Error> {
    if stored.entry.identity != digest {
        return Err(wire::conflict().into());
    }
    let reply = match &stored.entry.kind {
        Kind::Stream(stream) if !snapshot => Reply::Opened(stream.opened.clone()),
        Kind::Snapshot(snapshot_data) if snapshot => {
            Reply::SnapshotOpened(snapshot_data.opened.clone())
        }
        _ => return Err(wire::conflict().into()),
    };
    let record = sync(driver, authority, stored).await?;
    if !snapshot {
        start_prefetch(driver.clone(), authority, record);
    }
    Ok(reply)
}

async fn close<D: Driver>(
    driver: &D,
    authority: Authority,
    key: Key,
    snapshot: bool,
    now: u64,
) -> Result<Reply, Error> {
    if key.id.is_nil() {
        return Err(wire::invalid().into());
    }
    let Some(previous) = driver.registry().handle(&key)? else {
        return Ok(Reply::Closed);
    };
    if snapshot != matches!(previous.entry.kind, Kind::Snapshot(_)) {
        return Err(wire::conflict().into());
    }
    if !previous.entry.active(now) {
        if previous.entry.retained(now) {
            sync(driver, authority, previous).await?;
        }
        return Ok(Reply::Closed);
    }
    let mut closed = previous.entry.clone();
    closed.version = Version::advance(Some(closed.version), authority.term)?;
    closed.closed_at = Some(now);
    closed.compact(now);
    driver
        .commit(authority, closed, Some(previous.entry.version), None)
        .await?;
    Ok(Reply::Closed)
}

struct BatchOwner {
    output: Arc<Output>,
    _delivery: Delivery,
}
impl AsRef<[u8]> for BatchOwner {
    fn as_ref(&self) -> &[u8] {
        self.output.bytes.as_deref().unwrap_or_default()
    }
}

async fn demand<D: Driver>(
    driver: D,
    authority: Authority,
    key: Key,
    context: Option<RequestContext>,
    sequence: u64,
    now: u64,
) -> Result<Reply, Error> {
    if key.id.is_nil() {
        return Err(wire::invalid().into());
    }
    let record = driver.registry().handle(&key)?.ok_or_else(Error::expired)?;
    let Kind::Stream(stream) = &record.entry.kind else {
        return Err(wire::conflict().into());
    };
    if !record.entry.active(now) {
        return Err(Error::expired());
    }
    let delivery = record.activity.delivery()?;
    let source = driver.source(context.clone()).await?;
    let (first, replay) = match &stream.checkpoint {
        Some(checkpoint) if sequence == checkpoint.sequence => (checkpoint.first, Some(checkpoint)),
        Some(checkpoint)
            if checkpoint.sequence.checked_add(1) == Some(sequence)
                && matches!(checkpoint.outcome, Frame::Bytes(_)) =>
        {
            (checkpoint.next, None)
        }
        None if sequence == 0 => (stream.first, None),
        _ => return Err(wire::conflict().into()),
    };

    let output = if let Some(checkpoint) = replay
        && !matches!(checkpoint.outcome, Frame::Bytes(_))
    {
        Arc::new(Output::new(checkpoint.clone(), None, None))
    } else if let Some(output) = take_cached(&record, authority.term, sequence, first) {
        output
    } else {
        let _scanning = record.activity.scanning.lock().await;
        let cached = take_cached(&record, authority.term, sequence, first);
        match cached {
            Some(output) => output,
            _ => {
                let slot = output_slot(driver.registry())?;
                let output =
                    scan_one(&driver, &source, &record, stream, sequence, first, slot).await;
                if matches!(&output, Err(Error::Protocol(error)) if retryable(error)) {
                    driver.registry().evict_speculation();
                }
                output?
            }
        }
    };
    let updated = if let Some(checkpoint) = replay {
        if output.checkpoint != *checkpoint {
            return Err(wire::fail(
                ErrorCode::InternalError,
                "stream replay failed verification",
            )
            .into());
        }
        sync(&driver, authority, record.clone()).await?
    } else {
        let mut entry = record.entry.clone();
        let Kind::Stream(stream) = &mut entry.kind else {
            return Err(wire::conflict().into());
        };
        stream.checkpoint = Some(output.checkpoint.clone());
        entry.version = Version::advance(Some(entry.version), authority.term)?;
        entry.idle_at = now_ms().saturating_add(state::IDLE_MS);
        entry.context = context;
        driver
            .commit(authority, entry, Some(record.entry.version), None)
            .await?
    };
    if !updated.entry.active(now_ms()) {
        return Err(Error::expired());
    }
    match &output.checkpoint.outcome {
        Frame::Bytes(_) => {
            {
                let mut cache = updated.activity.cache.lock().unwrap();
                cache.check_term(authority.term);
                cache.last = Some((sequence, output.clone()));
            }
            start_prefetch(driver, authority, updated);
            Ok(Reply::Batch(Batch {
                bytes: Bytes::from_owner(BatchOwner {
                    output,
                    _delivery: delivery,
                }),
                format: stream.format,
                release: None,
            }))
        }
        Frame::Eof => Ok(Reply::Eof),
        Frame::Error(error) => Err(error.clone().into()),
    }
}

fn take_cached(record: &Stored, term: u64, sequence: u64, first: Cursor) -> Option<Arc<Output>> {
    record
        .activity
        .cache
        .lock()
        .unwrap()
        .take(term, sequence, first)
}

fn output_slot(registry: &Registry) -> Result<OwnedSemaphorePermit, ProtocolError> {
    match registry.outputs.clone().try_acquire_owned() {
        Ok(slot) => Ok(slot),
        Err(_) => {
            registry.evict_speculation();
            registry
                .outputs
                .clone()
                .try_acquire_owned()
                .map_err(|_| wire::overloaded())
        }
    }
}

async fn scan_one<D: Driver>(
    driver: &D,
    source: &Source<D::Store>,
    record: &Arc<Stored>,
    stream: &Stream,
    sequence: u64,
    first: Cursor,
    slot: OwnedSemaphorePermit,
) -> Result<Arc<Output>, Error> {
    let Some(scan) = record.entry.scan.clone() else {
        return Err(Error::expired());
    };
    let mut reader: ScanReader = record
        .activity
        .reader
        .lock()
        .unwrap()
        .take()
        .unwrap_or_default();
    let eviction = driver.registry().eviction.load(Ordering::Acquire);
    let result = reader
        .next_batch(
            source.storage(),
            driver.pool(),
            scan,
            first,
            stream.options(),
        )
        .await;
    if result.as_ref().is_ok_and(|batch| batch.data.is_some())
        && driver.registry().eviction.load(Ordering::Acquire) == eviction
        && driver
            .registry()
            .get(&record.entry.key)
            .is_some_and(|record| record.entry.active(now_ms()))
    {
        *record.activity.reader.lock().unwrap() = Some(reader);
    }
    let (next, outcome, bytes) = match result {
        Ok(batch) => match batch.data {
            Some(data) => {
                let bytes = data.bytes().clone();
                (
                    batch.cursor,
                    Frame::Bytes(Sha256::digest(&bytes).into()),
                    Some(bytes),
                )
            }
            None => (batch.cursor, Frame::Eof, None),
        },
        Err(error) if retryable(&error) => return Err(error.into()),
        Err(error) => (first, Frame::Error(error), None),
    };
    let slot = bytes.is_some().then_some(slot);
    Ok(Arc::new(Output::new(
        Checkpoint {
            sequence,
            first,
            next,
            outcome,
        },
        bytes,
        slot,
    )))
}

struct Speculation {
    activity: Arc<Activity>,
    released: bool,
}
impl Speculation {
    fn settled(&mut self) -> bool {
        if self
            .activity
            .speculative
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.released = true;
            true
        } else {
            self.activity.speculative.store(1, Ordering::Release);
            false
        }
    }
}
impl Drop for Speculation {
    fn drop(&mut self) {
        if !self.released {
            self.activity.speculative.store(0, Ordering::Release);
        }
    }
}
fn start_prefetch<D: Driver>(driver: D, authority: Authority, record: Arc<Stored>) {
    let Kind::Stream(stream) = &record.entry.kind else {
        return;
    };
    if !record.entry.active(now_ms())
        || stream.opened.prefetch_batches == 0
        || stream
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !matches!(checkpoint.outcome, Frame::Bytes(_)))
    {
        return;
    }
    let state = &record.activity.speculative;
    let mut previous = state.load(Ordering::Acquire);
    loop {
        let next = if previous == 0 { 1 } else { 2 };
        match state.compare_exchange_weak(previous, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(0) => break,
            Ok(_) => return,
            Err(observed) => previous = observed,
        }
    }
    let mut running = Speculation {
        activity: record.activity.clone(),
        released: false,
    };
    tokio::spawn(async move {
        loop {
            let _ = prefetch(&driver, authority, &record).await;
            if running.settled() {
                break;
            }
        }
    });
}

async fn prefetch<D: Driver>(
    driver: &D,
    authority: Authority,
    record: &Arc<Stored>,
) -> Result<(), Error> {
    let registry = driver.registry();
    let Ok(Ok(_worker)) = tokio::time::timeout(
        Duration::from_millis(250),
        registry.speculation.clone().acquire_owned(),
    )
    .await
    else {
        return Ok(());
    };
    if !driver.may_speculate() {
        return Ok(());
    }
    // The active scanner retains its descriptor and source independently of
    // close/expiry. Late speculative output can be dropped without replication.
    for _ in 0..wire::MAX_PREFETCH {
        if registry.stopped.load(Ordering::Acquire) || !driver.may_speculate() {
            break;
        }
        check_authority(driver, authority).await?;
        let _scanning = record.activity.scanning.lock().await;
        let Some(current) = registry.get(&record.entry.key) else {
            break;
        };
        let Kind::Stream(stream) = &current.entry.kind else {
            break;
        };
        if !current.entry.active(now_ms()) || stream.opened.prefetch_batches == 0 {
            break;
        }
        let source = driver.source(current.entry.context.clone()).await?;
        let next = if let Some(checkpoint) = &stream.checkpoint {
            if !matches!(checkpoint.outcome, Frame::Bytes(_)) {
                break;
            }
            let Some(next) = checkpoint.sequence.checked_add(1) else {
                break;
            };
            (next, checkpoint.next)
        } else {
            (0, stream.first)
        };
        let (sequence, first) = {
            let mut cache = current.activity.cache.lock().unwrap();
            cache.check_term(authority.term);
            if cache.ahead.len() >= stream.opened.prefetch_batches as usize {
                break;
            }
            if let Some((sequence, ahead)) = cache.ahead.last_key_value() {
                if !matches!(ahead.output.checkpoint.outcome, Frame::Bytes(_)) {
                    break;
                }
                let Some(sequence) = sequence.checked_add(1) else {
                    break;
                };
                (sequence, ahead.output.checkpoint.next)
            } else {
                next
            }
        };
        let Ok(speculative_slot) = registry.speculative_outputs.clone().try_acquire_owned() else {
            break;
        };
        let Ok(output_slot) = registry.outputs.clone().try_acquire_owned() else {
            break;
        };
        let eviction = registry.eviction.load(Ordering::Acquire);
        let output = scan_one(
            driver,
            &source,
            &current,
            stream,
            sequence,
            first,
            output_slot,
        )
        .await?;
        if registry.eviction.load(Ordering::Acquire) != eviction {
            break;
        }
        let Some(present) = registry.get(&record.entry.key) else {
            break;
        };
        if present.entry.version != current.entry.version || !present.entry.active(now_ms()) {
            break;
        }
        let terminal = !matches!(output.checkpoint.outcome, Frame::Bytes(_));
        present.activity.cache.lock().unwrap().ahead.insert(
            sequence,
            Ahead {
                output,
                _slot: speculative_slot,
            },
        );
        if terminal {
            break;
        }
    }
    Ok(())
}
