use std::fmt::Debug;
use std::io;
use std::ops::{
    Bound::{Excluded, Included, Unbounded},
    RangeBounds,
};

use anyhow::Result;
use nabedra_kv::WriteBatch;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader, StorageError, Vote,
    storage::{LogFlushed, RaftLogStorage},
};
use serde::Serialize;

use super::store::{GroupStore, decode, entries, prefix_end};
use super::{TypeConfig, storage_error};

const REPLICATION_PAGE_BYTES: usize = 4 * 1024 * 1024;
/// One append is split into synced batches of about this size, well below a journal
/// segment. A crash between them leaves a prefix of the entries, which Raft allows: the
/// append is acknowledged only after the last one.
const APPEND_BATCH_BYTES: usize = 16 * 1024 * 1024;
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

    pub(super) fn entries(
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
        let to = to.map_or_else(|| prefix_end(&self.prefix()), |n| self.key(n));
        let mut bytes = 0_usize;
        let mut result = Vec::new();
        for entry in entries(&self.0.db.snapshot(), &from, &to, true) {
            let (_, value) = entry?;
            bytes = bytes.saturating_add(value.len());
            if bytes > byte_limit && !result.is_empty() {
                break;
            }
            result.push(serde_json::from_slice(&value)?);
        }
        Ok(result)
    }

    /// The last log id: the highest entry, else the last purged id. Blocking.
    pub(super) fn log_state(&self) -> Result<LogState<TypeConfig>> {
        let prefix = self.prefix();
        let snapshot = self.0.db.snapshot();
        let last_purged_log_id: Option<LogId<u64>> = decode(snapshot.get(&self.0.key(b"purged"))?)?;
        let end = prefix_end(&prefix);
        let mut last = snapshot.iter(Included(prefix.as_slice()), Excluded(end.as_slice()), true);
        last.seek_for_prev(&self.key(u64::MAX));
        last.status()?;
        let last_log_id = if last.valid() && last.key().starts_with(&prefix) {
            Some(serde_json::from_slice::<Entry<TypeConfig>>(&last.value()?)?.log_id)
        } else {
            None
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id: last_log_id.or(last_purged_log_id),
        })
    }

    /// Durably store `entries`. Blocking.
    pub(super) fn append_entries(&self, entries: Vec<Entry<TypeConfig>>) -> Result<()> {
        let mut batch = WriteBatch::new();
        for entry in entries {
            let value = serde_json::to_vec(&entry)?;
            if !batch.is_empty() && batch.encoded_bytes() + value.len() > APPEND_BATCH_BYTES {
                self.0.db.write(std::mem::take(&mut batch))?;
            }
            batch.put_journal(&self.key(entry.log_id.index), &value);
        }
        if !batch.is_empty() {
            self.0.db.write(batch)?;
        }
        Ok(())
    }

    /// Durably delete the entries from `index` on. Blocking.
    pub(super) fn truncate_from(&self, index: u64) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_range(&self.key(index), &prefix_end(&self.prefix()));
        self.0.db.write(batch)
    }

    /// Durably delete the entries up to `log_id` and record it as purged. Blocking.
    pub(super) fn purge_through(&self, log_id: LogId<u64>) -> Result<()> {
        let from = self.prefix();
        let to = log_id
            .index
            .checked_add(1)
            .map_or_else(|| prefix_end(&from), |next| self.key(next));
        let mut batch = WriteBatch::new();
        batch.put(&self.0.key(b"purged"), &serde_json::to_vec(&log_id)?);
        batch.delete_range(&from, &to);
        self.0.db.write(batch)
    }

    /// Durably store one small record such as the vote. Blocking.
    pub(super) fn put_record(&self, key: &[u8], value: &impl Serialize) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(&self.0.key(key), &serde_json::to_vec(value)?);
        self.0.db.write(batch)
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
        label: &'static str,
        key: &'static [u8],
        value: impl Serialize + Send + 'static,
        subject: ErrorSubject<u64>,
    ) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        self.0
            .run_as(label, move |_| store.put_record(key, &value))
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
            .run(move |_| store.log_state())
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.persist("vote", VOTE_KEY, *vote, ErrorSubject::Vote)
            .await
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
        self.persist("committed", b"committed", committed, ErrorSubject::Logs)
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
        #[cfg(feature = "sim")]
        let appended: Vec<_> = entries.iter().map(|entry| entry.log_id).collect();
        // A full device is retried per sub-batch inside the job, so the callback reports
        // success only after the last one is durable (RV-EV M1).
        let result = self
            .0
            .run_as("append", move |_| store.append_entries(entries))
            .await;
        #[cfg(feature = "sim")]
        if result.is_ok() {
            crate::sim::hooks::appended(&self.0.id, &appended);
        }
        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::other(error.to_string())),
        );
        result.map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        self.0
            .run_as("truncate", move |_| store.truncate_from(log_id.index))
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        #[cfg(feature = "sim")]
        crate::sim::hooks::truncated(&self.0.id, log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let store = self.clone();
        self.0
            .run_as("purge", move |_| store.purge_through(log_id))
            .await
            .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }
}
