use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use openraft::{LogId, SnapshotMeta, StoredMembership};
use rocksdb::{
    BlockBasedOptions, Cache, DB, IteratorMode, Options, ReadOptions, Snapshot, WriteBatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{RwLock, Semaphore};
use uuid::Uuid;

use super::Node;

#[cfg(test)]
mod tests;

const DATABASE_IDENTITY: &[u8] = b"identity/v1";
const HOSTED: &[u8] = b"hosted/";
const RETIRED: &[u8] = b"retired/v1/";
const MAX_RECORD_KEY_BYTES: usize = 4 * 1024;
pub(super) const MAX_RECORD_VALUE_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCAN_ITEMS: usize = 4096;
const MAX_SCAN_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct Database {
    pub db: DB,
    pub root: PathBuf,
    io: Arc<Semaphore>,
    bulk: Arc<Semaphore>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    cluster: Uuid,
    broker: u64,
}

impl Database {
    pub fn open(root: &Path, cluster: Uuid, broker: u64) -> Result<Arc<Self>> {
        std::fs::create_dir_all(root.join("snapshots"))?;
        std::fs::File::open(root)?.sync_all()?;

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&Cache::new_lru_cache(64 * 1024 * 1024));
        table.set_cache_index_and_filter_blocks(true);

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&table);
        options.set_max_open_files(128);
        options.set_write_buffer_size(16 * 1024 * 1024);
        options.set_max_write_buffer_number(3);
        options.set_max_background_jobs(2);
        options.set_max_log_file_size(4 * 1024 * 1024);
        options.set_keep_log_file_num(2);
        let db = DB::open(&options, root.join("rocksdb"))?;
        let wanted = Identity { cluster, broker };
        let on_disk = db
            .get(DATABASE_IDENTITY)?
            .map(|bytes| serde_json::from_slice::<Identity>(&bytes))
            .transpose()?;
        match on_disk {
            Some(on_disk) => ensure!(
                on_disk == wanted,
                "data directory belongs to another Events cluster or broker"
            ),
            None => {
                let mut batch = WriteBatch::default();
                batch.put(DATABASE_IDENTITY, serde_json::to_vec(&wanted)?);
                sync_write(&db, batch)?;
            }
        }
        Ok(Arc::new(Self {
            db,
            root: root.to_path_buf(),
            io: Arc::new(Semaphore::new(8)),
            bulk: Arc::new(Semaphore::new(1)),
        }))
    }

    pub async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.io, work).await
    }

    pub async fn run_bulk<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.bulk, work).await
    }

    async fn run_bounded<T: Send + 'static>(
        self: &Arc<Self>,
        semaphore: &Arc<Semaphore>,
        work: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = semaphore.clone().acquire_owned().await?;
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&database)
        })
        .await?
    }

    pub fn stored_group_ids(&self) -> Result<Vec<String>> {
        let mut read = ReadOptions::default();
        read.set_iterate_upper_bound(prefix_end(HOSTED));
        self.db
            .iterator_opt(
                IteratorMode::From(HOSTED, rocksdb::Direction::Forward),
                read,
            )
            .map(|entry| {
                let (key, _) = entry?;
                Ok(std::str::from_utf8(&key[HOSTED.len()..])?.to_owned())
            })
            .collect()
    }

    pub fn hosted_key(id: &str) -> Vec<u8> {
        [HOSTED, id.as_bytes()].concat()
    }

    pub fn mark_hosted(&self, id: &str) -> Result<()> {
        let key = Self::hosted_key(id);
        if self.db.get(&key)?.is_none() {
            let mut write = WriteBatch::default();
            write.put(key, b"");
            sync_write(&self.db, write)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct GroupStore {
    pub db: Arc<Database>,
    pub id: Arc<str>,
    root: Arc<[u8]>,
    sealed: Arc<RwLock<bool>>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(super) struct StateHeader {
    pub generation: Uuid,
    pub applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, Node>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct StoredSnapshot {
    pub file_id: Uuid,
    pub meta: SnapshotMeta<u64, Node>,
}

impl GroupStore {
    pub fn new(db: Arc<Database>, id: &str) -> Self {
        Self {
            db,
            id: Arc::from(id),
            root: Arc::from(format!("group/{id}/").into_bytes()),
            sealed: Arc::default(),
        }
    }

    pub async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.io, work).await
    }

    pub async fn run_bulk<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        self.run_bounded(&self.db.bulk, work).await
    }

    async fn run_bounded<T: Send + 'static>(
        &self,
        pool: &Arc<Semaphore>,
        work: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let activity = self.sealed.clone().read_owned().await;
        ensure!(!*activity, "Raft group storage has closed");
        self.db
            .run_bounded(pool, move |db| {
                let _activity = activity;
                work(db)
            })
            .await
    }

    /// Drain this open generation before its identifier can be reused. A
    /// canceled async caller still holds its read permit in the blocking job.
    pub async fn seal(&self) {
        *self.sealed.write().await = true;
    }

    pub fn key(&self, suffix: &[u8]) -> Vec<u8> {
        [&*self.root, suffix].concat()
    }

    pub fn state_prefix(&self, generation: Uuid) -> Vec<u8> {
        [self.key(b"state/").as_slice(), generation.as_bytes(), b"/"].concat()
    }

    pub fn header(&self) -> Result<StateHeader> {
        Ok(self.read(b"state-header")?.unwrap_or_default())
    }

    pub fn read<T: DeserializeOwned>(&self, suffix: &[u8]) -> Result<Option<T>> {
        decode(self.db.db.get(self.key(suffix))?)
    }

    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let view = self.db.db.snapshot();
        let header: StateHeader = decode(view.get(self.key(b"state-header"))?)?.unwrap_or_default();
        Ok(Transaction {
            store: self,
            prefix: self.state_prefix(header.generation),
            view,
            header,
            pending: BTreeMap::new(),
            pending_bytes: 0,
        })
    }

    pub fn write_header(&self, write: &mut WriteBatch, header: &StateHeader) -> Result<()> {
        write.put(self.key(b"state-header"), serde_json::to_vec(header)?);
        Ok(())
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.db.root.join("snapshots").join(&*self.id)
    }

    pub fn remove_generation(&self, generation: Uuid, write: &mut WriteBatch) {
        let prefix = self.state_prefix(generation);
        let end = prefix_end(&prefix);
        write.delete_range(&prefix, &end);
    }

    pub fn cleanup_interrupted_stage(&self) -> Result<()> {
        if let Some(generation) = self.read(b"staged-generation")? {
            ensure!(
                self.header()?.generation != generation,
                "active snapshot staging marker"
            );
            let mut write = WriteBatch::default();
            self.remove_generation(generation, &mut write);
            write.delete(self.key(b"staged-generation"));
            sync_write(&self.db.db, write)?;
        }
        Ok(())
    }

    pub fn retired_at(&self) -> Result<Option<u64>> {
        decode(self.db.db.get([RETIRED, self.id.as_bytes()].concat())?)
    }

    pub fn forget(&self, removed_at: u64) -> Result<()> {
        let mut write = WriteBatch::default();
        write.delete(Database::hosted_key(&self.id));
        write.delete_range(&*self.root, prefix_end(&self.root).as_slice());
        write.put(
            [RETIRED, self.id.as_bytes()].concat(),
            serde_json::to_vec(&removed_at)?,
        );
        sync_write(&self.db.db, write)?;
        let dir = self.snapshot_dir();
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(())
    }

    pub fn clean_snapshot_dir(&self) -> Result<()> {
        let dir = self.snapshot_dir();
        if !dir.exists() {
            std::fs::create_dir(&dir)?;
            std::fs::File::open(self.db.root.join("snapshots"))?.sync_all()?;
        }
        let current: Option<StoredSnapshot> = self.read(b"snapshot")?;
        let keep = current.map(|snapshot| snapshot.file_id.to_string());
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() && keep.as_deref() != entry.file_name().to_str() {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

pub(super) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if let Some(incremented) = last.checked_add(1) {
            end.push(incremented);
            return end;
        }
    }
    // All internal prefixes start with a printable ASCII namespace.
    unreachable!("invalid internal RocksDB prefix");
}

pub(super) fn decode<T: DeserializeOwned>(data: Option<Vec<u8>>) -> Result<Option<T>> {
    data.map(|data| serde_json::from_slice(&data))
        .transpose()
        .context("invalid Events disk record")
}

pub(super) fn sync_write(db: &DB, batch: WriteBatch) -> Result<()> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    db.write_opt(batch, &options)?;
    Ok(())
}

pub(super) fn validate_record(key: &[u8], value: &[u8]) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= MAX_RECORD_KEY_BYTES,
        "invalid record key size"
    );
    ensure!(
        value.len() <= MAX_RECORD_VALUE_BYTES,
        "record exceeds the value size limit"
    );
    Ok(())
}

/// A consistent view of one application's keys. Writes are visible within the
/// transaction and are committed with the associated applied Raft index.
pub struct Transaction<'a> {
    store: &'a GroupStore,
    prefix: Vec<u8>,
    pub(super) view: Snapshot<'a>,
    pub(super) header: StateHeader,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pending_bytes: usize,
}

impl Transaction<'_> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.pending.get(key) {
            return Ok(value.clone());
        }
        Ok(self.view.get([self.prefix.as_slice(), key].concat())?)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_record(key, value)?;
        self.stage(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        validate_record(key, &[])?;
        self.stage(key, None)
    }

    fn stage(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let removed = self
            .pending
            .get(key)
            .map_or(0, |prev| key.len() + prev.as_ref().map_or(0, Vec::len));
        let next = self.pending_bytes - removed + key.len() + value.map_or(0, <[u8]>::len);
        ensure!(
            next <= MAX_PENDING_BYTES,
            "transaction exceeds the write size limit"
        );
        self.pending.insert(key.to_vec(), value.map(<[u8]>::to_vec));
        self.pending_bytes = next;
        Ok(())
    }

    /// Return up to the requested limit with this prefix, strictly after the cursor.
    /// Continue with the final record's key until an empty page. The page can end
    /// early at its independent byte limit.
    pub fn scan(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        ensure!(
            limit <= MAX_SCAN_ITEMS,
            "record page exceeds the count limit"
        );
        if limit == 0 {
            return Ok(Vec::new());
        }

        let start = after.map_or(prefix, |after| after.max(prefix));
        let exclusive = after == Some(start);
        let key_prefix = [self.prefix.as_slice(), prefix].concat();
        let full_start = [self.prefix.as_slice(), start].concat();
        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(prefix_end(&key_prefix));
        let mut disk = self
            .view
            .iterator_opt(
                IteratorMode::From(&full_start, rocksdb::Direction::Forward),
                opts,
            )
            .map(|entry| {
                let (key, value) = entry?;
                Ok::<_, rocksdb::Error>((key[self.prefix.len()..].to_vec(), value.into_vec()))
            })
            .peekable();
        if exclusive && matches!(disk.peek(), Some(Ok((key, _))) if key == start) {
            disk.next();
        }

        let bound = if exclusive {
            Excluded(start)
        } else {
            Included(start)
        };
        let mut staged = self
            .pending
            .range::<[u8], _>((bound, Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .peekable();
        let mut page = Vec::new();
        let mut bytes = 0;
        while page.len() < limit {
            let pair = match (disk.peek(), staged.peek()) {
                (None, None) => break,
                (Some(Err(_)), _) => disk.next().transpose()?,
                (Some(Ok((key, _))), Some((pending, _))) if key < *pending => {
                    disk.next().transpose()?
                }
                (Some(Ok((key, _))), Some((pending, _))) if key == *pending => {
                    disk.next();
                    staged
                        .next()
                        .and_then(|(key, value)| Some((key.clone(), value.clone()?)))
                }
                (_, Some(_)) => staged
                    .next()
                    .and_then(|(key, value)| Some((key.clone(), value.clone()?))),
                (Some(_), None) => disk.next().transpose()?,
            };
            if let Some((key, value)) = pair {
                bytes += key.len() + value.len();
                if bytes > MAX_SCAN_BYTES {
                    break;
                }
                page.push((key, value));
            }
        }
        Ok(page)
    }

    pub(super) fn commit(self) -> Result<()> {
        let mut write = WriteBatch::default();
        for (key, value) in self.pending {
            let key = [self.prefix.as_slice(), &key].concat();
            match value {
                Some(value) => write.put(key, value),
                None => write.delete(key),
            }
        }
        self.store.write_header(&mut write, &self.header)?;
        sync_write(&self.store.db.db, write)
    }
}
