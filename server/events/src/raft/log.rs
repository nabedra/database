use std::fmt::Debug;
use std::io;
use std::ops::{
    Bound::{Excluded, Included, Unbounded},
    RangeBounds,
};

use anyhow::Result;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader, StorageError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch};
use serde::Serialize;

use super::store::{GroupStore, prefix_end, sync_write};
use super::{TypeConfig, storage_error};

const REPLICATION_PAGE_BYTES: usize = 4 * 1024 * 1024;
pub(super) const VOTE_KEY: &[u8] = b"vote";

#[derive(Clone)]
pub(super) struct LogStore(pub GroupStore);

impl LogStore {
    fn prefix(&self) -> Vec<u8> {
        self.0.key(b"log/")
    }

    fn key(&self, index: u64) -> Vec<u8> {
        [self.prefix().as_slice(), &index.to_be_bytes()].concat()
    }

    fn entries(
        &self,
        (start, end): (std::ops::Bound<u64>, std::ops::Bound<u64>),
        byte_limit: usize,
    ) -> Result<Vec<Entry<TypeConfig>>> {
        let from = match start {
            Included(index) => index,
            Excluded(index) => match index.checked_add(1) {
                Some(index) => index,
                None => return Ok(Vec::new()),
            },
            Unbounded => 0,
        };
        let to = match end {
            Included(index) => index.checked_add(1),
            Excluded(index) => Some(index),
            Unbounded => None,
        };
        if to.is_some_and(|to| to <= from) {
            return Ok(Vec::new());
        }
        let from = self.key(from);
        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(
            to.map_or_else(|| prefix_end(&self.prefix()), |n| self.key(n)),
        );
        let mut bytes = 0_usize;
        let mut result = Vec::new();
        for entry in self
            .0
            .db
            .db
            .iterator_opt(IteratorMode::From(&from, Direction::Forward), opts)
        {
            let (_, value) = entry?;
            bytes = bytes.saturating_add(value.len());
            if bytes > byte_limit && !result.is_empty() {
                break;
            }
            result.push(serde_json::from_slice(&value)?);
        }
        Ok(result)
    }

    async fn read_entries(
        &self,
        range: (std::ops::Bound<u64>, std::ops::Bound<u64>),
        byte_limit: usize,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let store = self.clone();
        self.0
            .run(move |_| store.entries(range, byte_limit))
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn persist(
        &self,
        key: &'static [u8],
        value: impl Serialize + Send + 'static,
        subject: ErrorSubject<u64>,
    ) -> Result<(), StorageError<u64>> {
        let key = self.0.key(key);
        self.0
            .run(move |db| {
                let mut batch = WriteBatch::default();
                batch.put(key, serde_json::to_vec(&value)?);
                sync_write(&db.db, batch)
            })
            .await
            .map_err(|error| storage_error(subject, ErrorVerb::Write, error))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        self.read_entries(
            (range.start_bound().cloned(), range.end_bound().cloned()),
            usize::MAX,
        )
        .await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        self.read_entries((Included(start), Excluded(end)), REPLICATION_PAGE_BYTES)
            .await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let store = self.clone();
        self.0
            .run(move |db| {
                let prefix = store.prefix();
                let snapshot = db.db.snapshot();
                let last_purged_log_id =
                    super::store::decode(snapshot.get(store.0.key(b"purged"))?)?;
                let mut opts = ReadOptions::default();
                opts.set_iterate_lower_bound(prefix);
                let mut entries = snapshot.iterator_opt(
                    IteratorMode::From(&store.key(u64::MAX), Direction::Reverse),
                    opts,
                );
                let last_log_id = entries
                    .next()
                    .transpose()?
                    .map(|(_, value)| -> Result<_> {
                        Ok(serde_json::from_slice::<Entry<TypeConfig>>(&value)?.log_id)
                    })
                    .transpose()?
                    .or(last_purged_log_id);
                Ok(LogState {
                    last_purged_log_id,
                    last_log_id,
                })
            })
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.persist(VOTE_KEY, *vote, ErrorSubject::Vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let store = self.0.clone();
        self.0
            .run(move |_| store.read(VOTE_KEY))
            .await
            .map_err(|error| storage_error(ErrorSubject::Vote, ErrorVerb::Read, error))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.persist(b"committed", committed, ErrorSubject::Logs)
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let store = self.0.clone();
        self.0
            .run(move |_| Ok(store.read(b"committed")?.flatten()))
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let store = self.clone();
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .0
            .run(move |db| {
                let mut batch = WriteBatch::default();
                for entry in entries {
                    batch.put(store.key(entry.log_id.index), serde_json::to_vec(&entry)?);
                }
                sync_write(&db.db, batch)
            })
            .await;
        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::other(error.to_string())),
        );
        result.map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let from = self.key(log_id.index);
        let to = prefix_end(&self.prefix());
        self.0
            .run(move |db| {
                let mut batch = WriteBatch::default();
                batch.delete_range(from, to);
                sync_write(&db.db, batch)
            })
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let from = self.prefix();
        let to = log_id
            .index
            .checked_add(1)
            .map_or_else(|| prefix_end(&from), |next| self.key(next));
        let purged = self.0.key(b"purged");
        self.0
            .run(move |db| {
                let mut batch = WriteBatch::default();
                batch.put(purged, serde_json::to_vec(&log_id)?);
                batch.delete_range(from, to);
                sync_write(&db.db, batch)
            })
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }
}
