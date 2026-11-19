use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use openraft::{
    EntryPayload, RaftSnapshotBuilder,
    entry::RaftEntry,
    storage::{EntryResponder, RaftStateMachine},
    type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::RaftConfig;
use super::disk::{self, invalid};
use super::state::State;
use crate::cache::command::SLOT_COUNT;

type LogId = LogIdOf<RaftConfig>;
pub(super) type Meta = SnapshotMetaOf<RaftConfig>;
pub(super) type Snapshot = SnapshotOf<RaftConfig, State>;
type Membership = StoredMembershipOf<RaftConfig>;

#[derive(Clone)]
pub(super) struct Machine {
    current: Arc<RwLock<Applied>>,
    snapshots: Arc<Mutex<Snapshots>>,
}

#[derive(Default)]
struct Applied {
    state: State,
    last_applied: Option<LogId>,
    membership: Membership,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Saved {
    pub meta: Meta,
    pub state: State,
}

impl From<Saved> for Snapshot {
    fn from(saved: Saved) -> Self {
        Self {
            meta: saved.meta,
            snapshot: saved.state,
        }
    }
}

struct Snapshots {
    path: PathBuf,
    saved: Option<Saved>,
}

impl Machine {
    pub fn open(dir: &Path) -> io::Result<Self> {
        let path = dir.join("topology.snapshot");
        let saved: Option<Saved> = disk::read_atomic(&path)?;
        let current = if let Some(saved) = &saved {
            validate(saved)?;
            Applied {
                state: saved.state.clone(),
                last_applied: saved.meta.last_log_id,
                membership: saved.meta.last_membership.clone(),
            }
        } else {
            Applied::default()
        };
        Ok(Self {
            current: Arc::new(RwLock::new(current)),
            snapshots: Arc::new(Mutex::new(Snapshots { path, saved })),
        })
    }

    pub async fn read<T>(&self, f: impl FnOnce(&State) -> T) -> T {
        f(&self.current.read().await.state)
    }
}

fn index(id: Option<LogId>) -> Option<u64> {
    id.map(|id| id.index)
}

fn validate(saved: &Saved) -> io::Result<()> {
    if saved.state.topology.slots.len() != SLOT_COUNT
        || saved.state.topology.generation == 0
        || !saved.state.valid_snapshot()
        || index(*saved.meta.last_membership.log_id()) > index(saved.meta.last_log_id)
    {
        return Err(invalid("invalid cache topology snapshot"));
    }
    Ok(())
}

async fn persist(snapshots: Arc<Mutex<Snapshots>>, saved: Saved) -> io::Result<Saved> {
    let mut snapshots = snapshots.lock_owned().await;
    tokio::task::spawn_blocking(move || {
        if let Some(prior) = &snapshots.saved
            && index(prior.meta.last_log_id) >= index(saved.meta.last_log_id)
        {
            return Ok(prior.clone());
        }
        disk::replace(&snapshots.path, |file| disk::write_record(file, &saved))?;
        snapshots.saved = Some(saved.clone());
        Ok(saved)
    })
    .await
    .map_err(io::Error::other)?
}

pub(super) struct Builder {
    cut: Saved,
    snapshots: Arc<Mutex<Snapshots>>,
}

impl RaftSnapshotBuilder<RaftConfig> for Builder {
    type SnapshotData = State;

    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
        let saved = persist(Arc::clone(&self.snapshots), self.cut.clone()).await?;
        Ok(saved.into())
    }
}

impl RaftStateMachine<RaftConfig> for Machine {
    type SnapshotData = State;
    type SnapshotBuilder = Builder;

    async fn applied_state(&mut self) -> io::Result<(Option<LogId>, Membership)> {
        let current = self.current.read().await;
        Ok((current.last_applied, current.membership.clone()))
    }

    async fn apply<S>(&mut self, mut entries: S) -> io::Result<()>
    where
        S: Stream<Item = io::Result<EntryResponder<RaftConfig>>> + Unpin + Send,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            let mut current = self.current.write().await;
            let id = entry.log_id();
            current.last_applied = Some(id);
            let response = match entry.payload {
                EntryPayload::Blank => None,
                EntryPayload::Membership(membership) => {
                    current.membership = Membership::new(Some(id), membership);
                    None
                }
                EntryPayload::Normal(command) => current.state.apply(command, responder.is_some()),
            };
            drop(current);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Builder {
        let current = self.current.read().await;
        let meta = Meta {
            last_log_id: current.last_applied,
            last_membership: current.membership.clone(),
        };
        Builder {
            cut: Saved {
                meta,
                state: current.state.clone(),
            },
            snapshots: Arc::clone(&self.snapshots),
        }
    }

    async fn install_snapshot(&mut self, meta: &Meta, state: State) -> io::Result<()> {
        let saved = Saved {
            meta: meta.clone(),
            state,
        };
        validate(&saved)?;
        let mut current = self.current.write().await;
        if index(current.last_applied) > index(saved.meta.last_log_id) {
            return Err(invalid("cache topology snapshot regresses applied log"));
        }
        let saved = persist(Arc::clone(&self.snapshots), saved).await?;
        current.state = saved.state;
        current.last_applied = saved.meta.last_log_id;
        current.membership = saved.meta.last_membership;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        Ok(self
            .snapshots
            .lock()
            .await
            .saved
            .clone()
            .map(Snapshot::from))
    }
}
