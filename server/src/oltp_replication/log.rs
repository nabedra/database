//! Votes, logs, purge markers and commit positions share one RocksDB pool.
//! Every write uses the WAL with sync=true. Cancellation cannot release ordering
//! or I/O permits while the blocking operation still owns disk work.
use super::{Peer, TypeConfig};
use futures_util::{Stream, StreamExt};
use openraft::{
    RaftLogReader,
    entry::RaftEntry,
    errors::LeaderChanged,
    storage::{IOFlushed, LeaderBoundedStreamError, LogState, RaftLogStorage},
    type_config::alias::{EntryOf, LeaderIdOf, LogIdOf, VoteOf},
};
use rocksdb::{DB, Direction, IteratorMode, ReadOptions, WriteBatch, WriteOptions};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io,
    ops::{Bound, RangeBounds},
    pin::Pin,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore};

const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
const MAX_BATCH_ENTRIES: usize = 1024;

#[derive(Clone)]
pub struct LogStore {
    db: Arc<DB>,
    prefix: Vec<u8>,
    writes: Arc<Mutex<()>>,
    io: Arc<Semaphore>,
    recovery: Arc<AtomicBool>,
}
impl LogStore {
    /// The pool, cache, I/O limit and recovery latch are shared across groups.
    /// A different latch cannot reopen a handle on the same live database.
    pub fn new(
        db: Arc<DB>,
        local: &Peer,
        io: Arc<Semaphore>,
        recovery: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let recovery = pool_recovery(&db, recovery)?;
        healthy(&recovery)?;
        if io.is_closed()
            || local.node_id == 0
            || local.group_id.is_nil()
            || local.cluster_id.is_nil()
            || local.incarnation.is_nil()
        {
            return Err(invalid("invalid group identity or I/O capacity"));
        }
        let mut prefix = vec![0x30];
        prefix.extend_from_slice(local.group_id.as_bytes());
        type Registry = std::sync::Mutex<BTreeMap<(usize, uuid::Uuid), Weak<Mutex<()>>>>;
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        let mut registry = REGISTRY
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| invalid("group registry unavailable"))?;
        registry.retain(|_, writes| writes.strong_count() != 0);
        let registry_key = (Arc::as_ptr(&db) as usize, local.group_id);
        let writes = registry
            .get(&registry_key)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let writes = Arc::new(Mutex::new(()));
                registry.insert(registry_key, Arc::downgrade(&writes));
                writes
            });
        let store = Self {
            db,
            prefix,
            writes,
            io,
            recovery,
        };
        let key = store.meta(b"identity");
        let identity = (
            1_u32,
            local.cluster_id,
            local.group_id,
            local.node_id,
            local.incarnation,
        );
        match store.db.get(&key).map_err(disk)? {
            Some(bytes) => {
                let existing: (u32, uuid::Uuid, uuid::Uuid, u64, uuid::Uuid) = decode(&bytes)?;
                if existing != identity {
                    return Err(invalid("foreign Raft group storage"));
                }
            }
            None => mutate(&store.recovery, || {
                store
                    .db
                    .put_opt(key, encode(&identity)?, &sync_options())
                    .map_err(disk)
            })?,
        }
        drop(registry);
        Ok(store)
    }
    fn meta(&self, name: &[u8]) -> Vec<u8> {
        key(&self.prefix, b"/meta/", name)
    }
    async fn read<T: Send + 'static>(
        &self,
        work: impl FnOnce(&DB, &[u8]) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        healthy(&self.recovery)?;
        let permit = self
            .io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("pool I/O closed"))?;
        let db = self.db.clone();
        let prefix = self.prefix.clone();
        let recovery = self.recovery.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            healthy(&recovery)?;
            let result = work(&db, &prefix);
            healthy(&recovery)?;
            result
        })
        .await
        .map_err(|_| invalid("pool I/O task failed"))?
    }
    async fn write<T: Send + 'static>(
        &self,
        work: impl FnOnce(&DB, &[u8]) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        healthy(&self.recovery)?;
        let order = self.writes.clone().lock_owned().await;
        let permit = self
            .io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("pool I/O closed"))?;
        let db = self.db.clone();
        let prefix = self.prefix.clone();
        let recovery = self.recovery.clone();
        tokio::task::spawn_blocking(move || {
            let _order = order;
            let _permit = permit;
            mutate(&recovery, || work(&db, &prefix))
        })
        .await
        .map_err(|_| invalid("pool I/O task failed"))?
    }
    async fn flush_append(
        &self,
        order: OwnedMutexGuard<()>,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> io::Result<OwnedMutexGuard<()>> {
        healthy(&self.recovery)?;
        let permit = self
            .io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("pool I/O closed"))?;
        let db = self.db.clone();
        let prefix = self.prefix.clone();
        let recovery = self.recovery.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            healthy(&recovery)?;
            let mut batch = WriteBatch::default();
            for (index, bytes) in entries {
                batch.put(key(&prefix, b"/log/", &index.to_be_bytes()), bytes);
            }
            mutate(&recovery, || {
                db.write_opt(batch, &sync_options()).map_err(disk)
            })?;
            Ok(order)
        })
        .await
        .map_err(|_| invalid("log flush task failed"))?
    }
    async fn logs(
        &self,
        start: u64,
        end: Option<u64>,
        limited: bool,
    ) -> io::Result<Vec<EntryOf<TypeConfig>>> {
        self.read(move |db, prefix| {
            let prefix = key(prefix, b"/log/", b"");
            let mut options = ReadOptions::default();
            options.set_iterate_lower_bound(key(&prefix, b"", &start.to_be_bytes()));
            options.set_iterate_upper_bound(end.map_or_else(
                || successor(&prefix),
                |end| key(&prefix, b"", &end.to_be_bytes()),
            ));
            let snapshot = db.snapshot();
            let mut out = Vec::new();
            let mut total = 0usize;
            let mut previous: Option<u64> = None;
            for item in snapshot.iterator_opt(IteratorMode::Start, options) {
                let (stored_key, bytes) = item.map_err(disk)?;
                if bytes.len() > MAX_ENTRY_BYTES {
                    return Err(invalid("oversized Raft entry"));
                }
                if limited
                    && !out.is_empty()
                    && (out.len() >= 64 || total.saturating_add(bytes.len()) > MAX_BATCH_BYTES)
                {
                    break;
                }
                if !limited
                    && (out.len() >= MAX_BATCH_ENTRIES
                        || total.saturating_add(bytes.len()) > MAX_BATCH_BYTES)
                {
                    return Err(invalid("Raft log read exceeds bounded recovery window"));
                }
                let entry: EntryOf<TypeConfig> = decode(&bytes)?;
                let index = entry.index();
                if stored_key.as_ref() != key(&prefix, b"", &index.to_be_bytes())
                    || previous.is_some_and(|n| n.checked_add(1) != Some(index))
                {
                    return Err(invalid("corrupt or nonconsecutive Raft log"));
                }
                total = total
                    .checked_add(bytes.len())
                    .ok_or_else(|| invalid("log memory overflow"))?;
                previous = Some(index);
                out.push(entry);
            }
            Ok(out)
        })
        .await
    }
    fn stream(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Pin<Box<dyn Stream<Item = io::Result<EntryOf<TypeConfig>>> + Send>> {
        let store = self.clone();
        Box::pin(futures_util::stream::try_unfold(
            (store, Some(start), end, Vec::new().into_iter(), false),
            |(store, mut cursor, end, mut buffered, mut seen)| async move {
                loop {
                    if let Some(entry) = buffered.next() {
                        return Ok(Some((entry, (store, cursor, end, buffered, true))));
                    }
                    let Some(start) = cursor else { return Ok(None) };
                    if end.is_some_and(|end| start >= end) {
                        return Ok(None);
                    }
                    let entries = store.logs(start, end, true).await?;
                    if entries.is_empty() {
                        return Ok(None);
                    }
                    if seen && entries[0].index() != start {
                        return Err(invalid("log changed during stream"));
                    }
                    cursor = entries
                        .last()
                        .and_then(|entry| entry.index().checked_add(1));
                    buffered = entries.into_iter();
                    seen = true;
                }
            },
        ))
    }
}
impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<EntryOf<TypeConfig>>> {
        let start = match range.start_bound() {
            Bound::Included(n) => *n,
            Bound::Excluded(n) => match n.checked_add(1) {
                Some(n) => n,
                None => return Ok(vec![]),
            },
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(n) => n.checked_add(1),
            Bound::Excluded(n) => Some(*n),
            Bound::Unbounded => None,
        };
        if end.is_some_and(|end| start >= end) {
            return Ok(vec![]);
        }
        self.logs(start, end, false).await
    }
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> io::Result<Vec<EntryOf<TypeConfig>>> {
        if start >= end {
            return Ok(vec![]);
        }
        self.logs(start, Some(end), true).await
    }
    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = io::Result<EntryOf<TypeConfig>>> + Send
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        match bounds(&range) {
            Some((start, end)) => self.stream(start, end),
            None => Box::pin(futures_util::stream::empty())
                as Pin<Box<dyn Stream<Item = io::Result<EntryOf<TypeConfig>>> + Send>>,
        }
    }
    async fn leader_bounded_stream<RB>(
        &mut self,
        leader: LeaderIdOf<TypeConfig>,
        range: RB,
    ) -> impl Stream<Item = Result<EntryOf<TypeConfig>, LeaderBoundedStreamError<TypeConfig>>> + Send
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        let stream = match bounds(&range) {
            Some((start, end)) => self.stream(start, end),
            None => Box::pin(futures_util::stream::empty())
                as Pin<Box<dyn Stream<Item = io::Result<EntryOf<TypeConfig>>> + Send>>,
        };
        futures_util::stream::try_unfold(
            (stream, self.clone(), leader),
            |(mut stream, mut reader, leader)| async move {
                let vote = reader.read_vote().await?;
                if vote
                    .as_ref()
                    .is_none_or(|vote| !vote.is_committed() || vote.leader_id() != &leader)
                {
                    return Err(LeaderBoundedStreamError::LeaderChanged(LeaderChanged::new(
                        leader, vote,
                    )));
                }
                let Some(entry) = stream.next().await else {
                    return Ok(None);
                };
                let entry = entry?;
                let after = reader.read_vote().await?;
                if after != vote {
                    return Err(LeaderBoundedStreamError::LeaderChanged(LeaderChanged::new(
                        leader, after,
                    )));
                }
                Ok(Some((entry, (stream, reader, leader))))
            },
        )
    }
    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<TypeConfig>>> {
        self.read(|db, prefix| get(db, &key(prefix, b"/meta/", b"vote")))
            .await
    }
}
impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        self.read(|db, prefix| snapshot_state(&db.snapshot(), prefix))
            .await
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> io::Result<()> {
        let bytes = encode(vote)?;
        self.write(move |db, prefix| {
            db.put_opt(key(prefix, b"/meta/", b"vote"), bytes, &sync_options())
                .map_err(disk)
        })
        .await
    }
    async fn save_committed(&mut self, committed: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
        let bytes = encode(&committed)?;
        self.write(move |db, prefix| {
            db.put_opt(key(prefix, b"/meta/", b"committed"), bytes, &sync_options())
                .map_err(disk)
        })
        .await
    }
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
        self.read(|db, prefix| {
            Ok(
                get::<Option<LogIdOf<TypeConfig>>>(db, &key(prefix, b"/meta/", b"committed"))?
                    .flatten(),
            )
        })
        .await
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let result = async {
            let mut order = self.writes.clone().lock_owned().await;
            let mut encoded = Vec::new();
            let mut total = 0usize;
            let mut previous: Option<u64> = None;
            for entry in entries {
                let index = entry.index();
                if previous.is_some_and(|n| n.checked_add(1) != Some(index)) {
                    return Err(invalid("nonconsecutive log append"));
                }
                let bytes = encode(&entry)?;
                if !encoded.is_empty()
                    && (encoded.len() >= MAX_BATCH_ENTRIES || total + bytes.len() > MAX_BATCH_BYTES)
                {
                    order = self.flush_append(order, encoded).await?;
                    encoded = Vec::new();
                    total = 0;
                }
                previous = Some(index);
                total += bytes.len();
                encoded.push((index, bytes));
            }
            if !encoded.is_empty() {
                order = self.flush_append(order, encoded).await?;
            }
            drop(order);
            Ok(())
        }
        .await;
        callback.io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error: &io::Error| io::Error::new(error.kind(), error.to_string())),
        );
        result
    }

    async fn truncate_after(&mut self, last: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
        let start = match last {
            Some(id) => match id.index.checked_add(1) {
                Some(n) => n,
                None => return Ok(()),
            },
            None => 0,
        };
        self.write(move |db, prefix| {
            let lp = key(prefix, b"/log/", b"");
            let mut batch = WriteBatch::default();
            batch.delete_range(key(&lp, b"", &start.to_be_bytes()), successor(&lp));
            db.write_opt(batch, &sync_options()).map_err(disk)
        })
        .await
    }
    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> io::Result<()> {
        let bytes = encode(&log_id)?;
        self.write(move |db, prefix| {
            let lp = key(prefix, b"/log/", b"");
            let end = log_id
                .index
                .checked_add(1)
                .map_or_else(|| successor(&lp), |n| key(&lp, b"", &n.to_be_bytes()));
            let mut batch = WriteBatch::default();
            batch.delete_range(lp, end);
            batch.put(key(prefix, b"/meta/", b"purged"), bytes);
            db.write_opt(batch, &sync_options()).map_err(disk)
        })
        .await
    }
}
fn snapshot_state(
    snapshot: &rocksdb::Snapshot<'_>,
    prefix: &[u8],
) -> io::Result<LogState<TypeConfig>> {
    let purged: Option<LogIdOf<TypeConfig>> = snapshot
        .get(key(prefix, b"/meta/", b"purged"))
        .map_err(disk)?
        .map(|bytes| decode(&bytes))
        .transpose()?;
    let lp = key(prefix, b"/log/", b"");
    let end = successor(&lp);
    let mut options = ReadOptions::default();
    options.set_iterate_lower_bound(lp.clone());
    options.set_iterate_upper_bound(end.clone());
    let last = match snapshot
        .iterator_opt(IteratorMode::From(&end, Direction::Reverse), options)
        .next()
    {
        Some(item) => {
            let (_, bytes) = item.map_err(disk)?;
            let entry: EntryOf<TypeConfig> = decode(&bytes)?;
            Some(entry.log_id())
        }
        None => None,
    };
    Ok(LogState {
        last_purged_log_id: purged,
        last_log_id: last.or(purged),
    })
}

fn pool_recovery(db: &Arc<DB>, requested: Arc<AtomicBool>) -> io::Result<Arc<AtomicBool>> {
    type Pools = std::sync::Mutex<BTreeMap<usize, (Weak<DB>, Arc<AtomicBool>)>>;
    static POOLS: OnceLock<Pools> = OnceLock::new();
    let mut pools = POOLS
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| invalid("pool registry unavailable"))?;
    pools.retain(|_, (db, _)| db.strong_count() != 0);
    let identity = Arc::as_ptr(db) as usize;
    if let Some((_, existing)) = pools.get(&identity) {
        if requested.load(Ordering::SeqCst) {
            existing.store(true, Ordering::SeqCst);
        }
        if !Arc::ptr_eq(existing, &requested) {
            return Err(invalid("recovery latch differs from live pool"));
        }
        return Ok(existing.clone());
    }
    pools.insert(identity, (Arc::downgrade(db), requested.clone()));
    Ok(requested)
}
fn healthy(recovery: &AtomicBool) -> io::Result<()> {
    if recovery.load(Ordering::SeqCst) {
        Err(io::Error::other("Raft pool requires reopen and recovery"))
    } else {
        Ok(())
    }
}
fn mutate<T>(recovery: &AtomicBool, work: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    healthy(recovery)?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work))
        .unwrap_or_else(|_| Err(io::Error::other("Raft write task panicked")));
    if result.is_err() {
        recovery.store(true, Ordering::SeqCst);
    }
    result
}

fn bounds(range: &impl RangeBounds<u64>) -> Option<(u64, Option<u64>)> {
    let start = match range.start_bound() {
        Bound::Included(n) => *n,
        Bound::Excluded(n) => n.checked_add(1)?,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(n) => n.checked_add(1),
        Bound::Excluded(n) => Some(*n),
        Bound::Unbounded => None,
    };
    if end.is_some_and(|end| start >= end) {
        None
    } else {
        Some((start, end))
    }
}

fn key(prefix: &[u8], middle: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + middle.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(middle);
    key.extend_from_slice(suffix);
    key
}
fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    for index in (0..out.len()).rev() {
        if out[index] != 255 {
            out[index] += 1;
            out.truncate(index + 1);
            return out;
        }
    }
    unreachable!("fixed namespace has successor")
}
fn get<T: DeserializeOwned>(db: &DB, key: &[u8]) -> io::Result<Option<T>> {
    db.get(key)
        .map_err(disk)?
        .map(|bytes| decode(&bytes))
        .transpose()
}
fn encode(value: &impl Serialize) -> io::Result<Vec<u8>> {
    struct Writer(Vec<u8>);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > MAX_ENTRY_BYTES - self.0.len() {
                return Err(invalid("Raft record exceeds bound"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Vec::new());
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| invalid("Raft record exceeds bound or encoding failed"))?;
    Ok(writer.0)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    if bytes.len() > MAX_ENTRY_BYTES {
        return Err(invalid("Raft record exceeds bound"));
    }
    serde_json::from_slice(bytes).map_err(|_| invalid("corrupt Raft record"))
}
fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn disk(_: rocksdb::Error) -> io::Error {
    io::Error::other("Raft storage I/O failed")
}

#[cfg(test)]
mod tests;
