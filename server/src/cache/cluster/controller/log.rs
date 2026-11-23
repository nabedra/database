use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Seek};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::{
    RaftLogReader,
    entry::RaftEntry,
    storage::{IOFlushed, LogState, RaftLogStorage},
    type_config::alias::{EntryOf, LogIdOf, VoteOf},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::RaftConfig;
use super::disk::{self, ReadRecord, invalid};

type Entry = EntryOf<RaftConfig>;
type LogId = LogIdOf<RaftConfig>;
type Vote = VoteOf<RaftConfig>;
const MAX_WAL_BYTES: u64 = 512 << 20;

#[derive(Serialize, Deserialize)]
enum Record {
    Vote(Vote),
    Committed(Option<LogId>),
    Append(Entry),
    Truncate(Option<LogId>),
    Purged(LogId),
}

#[derive(Clone)]
pub(super) struct Log(Arc<Mutex<Wal>>);

impl Log {
    pub fn open(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("controller.lock"))?;
        lock.try_lock().map_err(io::Error::other)?;
        let path = dir.join("topology.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        File::open(dir)?.sync_all()?;
        if file.metadata()?.len() > MAX_WAL_BYTES {
            return Err(invalid("controller WAL exceeds its recovery bound"));
        }

        let mut log = Contents::default();
        let recovered_len = {
            let mut reader = BufReader::new(&mut file);
            loop {
                let offset = reader.stream_position()?;
                match disk::read_record(&mut reader)? {
                    ReadRecord::Complete(record) => log.apply(record)?,
                    ReadRecord::End | ReadRecord::Partial => break offset,
                }
            }
        };
        if recovered_len != file.metadata()?.len() {
            file.set_len(recovered_len)?;
            file.sync_all()?;
        }
        file.seek(std::io::SeekFrom::Start(recovered_len))?;
        Ok(Self(Arc::new(Mutex::new(Wal {
            path,
            file,
            _lock: lock,
            contents: log,
            healthy: true,
        }))))
    }

    async fn write(
        &self,
        work: impl FnOnce(&mut Wal) -> io::Result<()> + Send + 'static,
    ) -> io::Result<()> {
        let mut guard = Arc::clone(&self.0).lock_owned().await;
        nabedra_rt::task::spawn_blocking(move || {
            guard.require_healthy()?;
            let result = work(&mut guard);
            guard.healthy = result.is_ok();
            result
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[derive(Default)]
struct Contents {
    vote: Option<Vote>,
    committed: Option<LogId>,
    purged: Option<LogId>,
    entries: BTreeMap<u64, Entry>,
}

impl Contents {
    fn last_log_id(&self) -> Option<LogId> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id())
            .or(self.purged)
    }

    fn apply(&mut self, record: Record) -> io::Result<()> {
        match record {
            Record::Vote(vote) => self.vote = Some(vote),
            Record::Committed(id) => self.committed = id,
            Record::Append(entry) => {
                if self
                    .last_log_id()
                    .is_some_and(|id| id.index.checked_add(1) != Some(entry.index()))
                {
                    return Err(invalid(
                        "controller WAL contains a nonconsecutive log entry",
                    ));
                }
                self.entries.insert(entry.index(), entry);
            }
            Record::Truncate(last) => self
                .entries
                .retain(|index, _| last.is_some_and(|last| *index <= last.index)),
            Record::Purged(id) => {
                if self.purged.is_some_and(|old| old.index > id.index) {
                    return Err(invalid("controller WAL purge pointer regressed"));
                }
                self.entries.retain(|index, _| *index > id.index);
                self.purged = Some(id);
            }
        }
        Ok(())
    }
}

struct Wal {
    path: PathBuf,
    file: File,
    _lock: File,
    contents: Contents,
    healthy: bool,
}

impl Wal {
    fn require_healthy(&self) -> io::Result<()> {
        if self.healthy {
            Ok(())
        } else {
            Err(invalid("controller WAL requires recovery"))
        }
    }

    fn append(&mut self, records: impl IntoIterator<Item = Record>) -> io::Result<()> {
        for record in records {
            if self.file.stream_position()? >= MAX_WAL_BYTES - (8 << 20) {
                return Err(invalid("controller WAL reached its configured byte limit"));
            }
            disk::write_record(&mut self.file, &record)?;
            self.contents.apply(record)?;
        }
        self.file.sync_data()
    }

    fn compact(&mut self, purged: LogId) -> io::Result<()> {
        self.contents.apply(Record::Purged(purged))?;
        let contents = &self.contents;
        self.file = disk::replace(&self.path, |file| {
            disk::write_record(file, &Record::Purged(purged))?;
            if let Some(vote) = &contents.vote {
                disk::write_record(file, &Record::Vote(*vote))?;
            }
            disk::write_record(file, &Record::Committed(contents.committed))?;
            for entry in contents.entries.values() {
                disk::write_record(file, &Record::Append(entry.clone()))?;
            }
            Ok(())
        })?;
        Ok(())
    }
}

impl RaftLogReader<RaftConfig> for Log {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry>> {
        let log = self.0.lock().await;
        log.require_healthy()?;
        Ok(log
            .contents
            .entries
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        let log = self.0.lock().await;
        log.require_healthy()?;
        Ok(log.contents.vote)
    }
}

impl RaftLogStorage<RaftConfig> for Log {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<RaftConfig>> {
        let log = self.0.lock().await;
        log.require_healthy()?;
        Ok(LogState {
            last_purged_log_id: log.contents.purged,
            last_log_id: log.contents.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        let vote = *vote;
        self.write(move |wal| wal.append([Record::Vote(vote)]))
            .await
    }

    async fn save_committed(&mut self, id: Option<LogId>) -> io::Result<()> {
        self.write(move |wal| {
            if id == wal.contents.committed {
                return Ok(());
            }
            wal.append([Record::Committed(id)])
        })
        .await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        let log = self.0.lock().await;
        log.require_healthy()?;
        Ok(log.contents.committed)
    }

    async fn append<I: IntoIterator<Item = Entry> + Send>(
        &mut self,
        entries: I,
        callback: IOFlushed<RaftConfig>,
    ) -> io::Result<()>
    where
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let result = self
            .write(move |wal| wal.append(entries.into_iter().map(Record::Append)))
            .await;
        callback.io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::new(error.kind(), error.to_string())),
        );
        result
    }

    async fn truncate_after(&mut self, last: Option<LogId>) -> io::Result<()> {
        self.write(move |wal| wal.append([Record::Truncate(last)]))
            .await
    }

    async fn purge(&mut self, id: LogId) -> io::Result<()> {
        self.write(move |wal| wal.compact(id)).await
    }
}
