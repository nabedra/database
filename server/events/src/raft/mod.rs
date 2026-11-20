//! Each Events partition and the metadata service have their own Raft group.
//! A broker shares its disk, cache, and I/O limits across its hosted groups.

mod batch;
mod log;
mod network;
mod snapshot;
mod state_machine;
mod store;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{fmt, io};

use anyhow::{Result, anyhow, ensure};
use nabedra_kv::WriteBatch;
use openraft::error::{Fatal, InitializeError, RaftError};
use openraft::{Config, ErrorSubject, ErrorVerb, Raft, SnapshotPolicy, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

pub use network::PeerTransport;
pub use snapshot::SnapshotCut;
pub use state_machine::StateMachine;
pub use store::{QuarantinedGroup, Transaction};

use batch::PublishBatcher;
use log::LogStore;
use network::NetworkFactory;
use snapshot::{CHUNK_BYTES, MAX_SNAPSHOT_BYTES, SnapshotFile, Snapshots};
use state_machine::AppliedState;
use store::{Database, GroupStore};

pub(super) type Node = openraft::BasicNode;

#[derive(Serialize, Deserialize, Clone)]
enum RaftCommand {
    Single(Value),
    Batch(Vec<Value>),
}

impl fmt::Debug for RaftCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single(_) => formatter.write_str("RaftCommand::Single"),
            Self::Batch(commands) => formatter
                .debug_struct("RaftCommand::Batch")
                .field("commands", &commands.len())
                .finish(),
        }
    }
}

openraft::declare_raft_types!(
    TypeConfig:
        D = RaftCommand,
        R = Value,
        Node = Node,
        SnapshotData = SnapshotFile,
);

const READ_DEADLINE: Duration = Duration::from_secs(2);
const WRITE_DEADLINE: Duration = Duration::from_secs(3);
const MEMBERSHIP_DEADLINE: Duration = Duration::from_secs(45);
const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(120);
const MAX_GROUP_MEMBERS: usize = 64;
pub(crate) const MAX_HOSTED_GROUPS: usize = 4096;
const MAX_COMMAND_BYTES: usize = 2 * 1024 * 1024;
const PUBLISH_BUDGET_BYTES: usize = 32 * 1024 * 1024;
const ELECTION_FLOOR: &[u8] = b"election-floor/v1";

pub struct BrokerConfig {
    pub id: u64,
    pub cluster_id: Uuid,
    pub data_dir: PathBuf,
    pub peer_token: String,
    pub peer_ca: Option<PathBuf>,
    pub peer_identity: Option<PathBuf>,
    pub allow_plaintext: bool,
}

pub struct Broker {
    id: u64,
    cluster_id: Uuid,
    database: Arc<Database>,
    peers: PeerTransport,
    groups: RwLock<BTreeMap<String, Arc<Group>>>,
    change_groups: Arc<Mutex<()>>,
    publish_budget: Arc<Semaphore>,
    control_slots: Arc<Semaphore>,
}

#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    #[error("local broker is not group leader, known leader {0:?}")]
    NotLeader(Option<u64>),
    #[error("group quorum or request deadline unavailable")]
    Unavailable,
    #[error("write outcome is unknown after the request deadline")]
    OutcomeUnknown,
    #[error("group admission capacity exhausted")]
    Overloaded,
    #[error("group is closed")]
    Closed,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type GroupResult<T> = Result<T, GroupError>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Membership {
    pub voters: BTreeSet<u64>,
    pub nodes: BTreeMap<u64, String>,
    pub joint: bool,
    pub voter_sets: Vec<BTreeSet<u64>>,
    pub applied_index: Option<u64>,
    pub membership_index: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupStatus {
    pub term: u64,
    pub leader: Option<u64>,
    pub running: bool,
    pub last_log_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub quorum_ack_age_ms: Option<u64>,
    pub matched_index: Option<BTreeMap<u64, Option<u64>>>,
    pub membership: Membership,
}

pub struct Group {
    local_id: u64,
    raft: Raft<TypeConfig>,
    snapshots: Snapshots,
    publish: PublishBatcher,
    change_membership: Mutex<()>,
    control_slots: Arc<Semaphore>,
    peers: PeerTransport,
    election_after: Option<u64>,
    accepts_votes: AtomicBool,
}

impl Broker {
    pub fn open(config: BrokerConfig) -> Result<Arc<Self>> {
        ensure!(config.id != 0, "broker ID must be positive");
        ensure!(!config.cluster_id.is_nil(), "cluster ID must be nonzero");
        let peers = PeerTransport::new(
            config.cluster_id,
            &config.peer_token,
            config.peer_ca.as_deref(),
            config.peer_identity.as_deref(),
            config.allow_plaintext,
        )?;
        let database = Database::open(&config.data_dir, config.cluster_id, config.id)?;
        Ok(Self::with_database(config, peers, database))
    }

    #[cfg(test)]
    pub(crate) fn open_with(
        config: BrokerConfig,
        options: nabedra_kv::Options,
    ) -> Result<Arc<Self>> {
        let peers = PeerTransport::new(
            config.cluster_id,
            &config.peer_token,
            None,
            None,
            config.allow_plaintext,
        )?;
        let database =
            Database::open_with(&config.data_dir, config.cluster_id, config.id, options)?;
        Ok(Self::with_database(config, peers, database))
    }

    fn with_database(
        config: BrokerConfig,
        peers: PeerTransport,
        database: Arc<Database>,
    ) -> Arc<Self> {
        let broker = Arc::new(Self {
            id: config.id,
            cluster_id: config.cluster_id,
            database,
            peers,
            groups: RwLock::default(),
            change_groups: Arc::default(),
            publish_budget: Arc::new(Semaphore::new(PUBLISH_BUDGET_BYTES)),
            control_slots: Arc::new(Semaphore::new(64)),
        });
        // Damage found while serving quarantines a group: stop its Raft node so it neither
        // votes nor acknowledges appends (section 3, rule 1).
        if let (Ok(runtime), Some(mut damaged)) = (
            tokio::runtime::Handle::try_current(),
            broker.database.take_damage_events(),
        ) {
            let weak = Arc::downgrade(&broker);
            runtime.spawn(async move {
                while let Some(id) = damaged.recv().await {
                    let Some(broker) = weak.upgrade() else {
                        return;
                    };
                    if let Err(error) = broker.close_group(&id).await {
                        tracing::error!(group = %id, %error, "could not stop a damaged group");
                    }
                }
            });
        }
        broker
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn peers(&self) -> &PeerTransport {
        &self.peers
    }

    pub fn router(self: &Arc<Self>) -> axum::Router {
        network::router(self.clone())
    }

    pub fn group(&self, id: &str) -> Option<Arc<Group>> {
        self.groups.read().ok()?.get(id).cloned()
    }

    pub fn group_ids(&self) -> Vec<String> {
        self.groups
            .read()
            .map(|groups| groups.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Hosted groups that can start. Groups whose storage is quarantined are left out:
    /// they stay stopped and are listed by [`Broker::quarantined_groups`].
    pub fn stored_group_ids(&self) -> Result<Vec<String>> {
        let quarantined = self.database.quarantined_groups();
        Ok(self
            .database
            .stored_group_ids()?
            .into_iter()
            .filter(|id| quarantined.iter().all(|group| &group.group != id))
            .collect())
    }

    /// Hosted groups whose storage is damaged. They are not started; an operator re-adds
    /// this replica through the membership and backup tools.
    pub fn quarantined_groups(&self) -> Vec<QuarantinedGroup> {
        self.database.quarantined_groups()
    }

    pub fn disk_available_bytes(&self) -> Result<u64> {
        Ok(fs2::available_space(&self.database.root)?)
    }

    pub async fn open_group(
        self: &Arc<Self>,
        id: &str,
        machine: Arc<dyn StateMachine>,
    ) -> GroupResult<Arc<Group>> {
        self.open_group_after(id, machine, None).await
    }

    pub(crate) async fn open_group_after(
        self: &Arc<Self>,
        id: &str,
        machine: Arc<dyn StateMachine>,
        election_after: Option<u64>,
    ) -> GroupResult<Arc<Group>> {
        validate_group_id(id)?;
        let broker = self.clone();
        let id = id.to_owned();
        self.change_group(
            async move { broker.open_group_locked(&id, machine, election_after).await },
        )
        .await
    }

    async fn open_group_locked(
        &self,
        id: &str,
        machine: Arc<dyn StateMachine>,
        election_after: Option<u64>,
    ) -> GroupResult<Arc<Group>> {
        if self.database.is_quarantined(id) {
            tracing::warn!(
                group = id,
                "not starting a group whose storage is quarantined"
            );
            return Err(GroupError::Other(anyhow!(
                "group {id} storage is quarantined; replace this replica"
            )));
        }
        if let Some(group) = self.group(id) {
            if election_after.is_none_or(|cut| group.election_after >= Some(cut)) {
                return if group.status().running {
                    Ok(group)
                } else {
                    Err(GroupError::Closed)
                };
            }
            self.close_group_locked(id).await?;
        }

        let store = GroupStore::new(self.database.clone(), id);
        let staged = store.clone();
        let local_id = self.id;
        let (election_after, accepts_votes) = self
            .database
            .run(move |db| {
                validate_hosting_capacity(db, &staged.id)?;
                let prior: Option<u64> = staged.read(ELECTION_FLOOR)?;
                let floor = prior.max(election_after);
                ensure!(
                    staged
                        .retired_at()?
                        .is_none_or(|retired| floor.is_some_and(|cut| cut >= retired)),
                    "retired group requires a current enrollment cut"
                );
                let accepts_votes = match floor {
                    Some(cut) => {
                        let voting = voter_after(&staged, local_id, cut)?;
                        let obsolete_leader = !voting
                            && staged
                                .read::<openraft::Vote<u64>>(log::VOTE_KEY)?
                                .is_some_and(|vote| {
                                    vote.is_committed() && vote.leader_id().node_id == local_id
                                });
                        if obsolete_leader || floor != prior {
                            let mut batch = WriteBatch::new();
                            if obsolete_leader {
                                // OpenRaft can resume a persisted self-vote before its
                                // election timer runs. The committed placement has
                                // excluded this obsolete replica, so prepare it empty.
                                let root = staged.key(b"");
                                batch.delete_range(&root, &store::prefix_end(&root));
                            }
                            batch.put(&staged.key(ELECTION_FLOOR), &serde_json::to_vec(&cut)?);
                            db.write(batch)?;
                        }
                        voting
                    }
                    None => true,
                };
                staged.cleanup_interrupted_stage()?;
                staged.clean_snapshot_dir()?;
                db.mark_hosted(&staged.id)?;
                Ok((floor, accepts_votes))
            })
            .await?;

        let config = Config {
            cluster_name: format!("{}/{}", self.cluster_id, id),
            heartbeat_interval: 120,
            election_timeout_min: 450,
            election_timeout_max: 850,
            install_snapshot_timeout: 17 * 60_000,
            max_payload_entries: 64,
            replication_lag_threshold: 4096,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1024),
            snapshot_max_chunk_size: CHUNK_BYTES as u64,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 64,
            enable_elect: accepts_votes,
            ..Config::default()
        }
        .validate()
        .map_err(anyhow::Error::from)?;
        let snapshots = Snapshots::new(store.clone(), self.cluster_id);
        let opened = Raft::new(
            self.id,
            Arc::new(config),
            NetworkFactory {
                peers: self.peers.clone(),
                group: store.id.clone(),
            },
            LogStore(store),
            AppliedState {
                machine,
                snapshots: snapshots.clone(),
            },
        )
        .await;
        let raft = match opened {
            Ok(raft) => raft,
            Err(error) => {
                snapshots.store.seal().await;
                return Err(raft_fatal(error));
            }
        };
        let group = Arc::new(Group {
            local_id: self.id,
            publish: PublishBatcher::new(raft.clone(), self.publish_budget.clone()),
            change_membership: Mutex::default(),
            control_slots: self.control_slots.clone(),
            raft,
            snapshots,
            peers: self.peers.clone(),
            election_after,
            accepts_votes: AtomicBool::new(accepts_votes),
        });
        if let Some(cut) = election_after.filter(|_| !accepts_votes) {
            tokio::spawn(group.clone().enable_votes_after(cut));
        }
        self.groups
            .write()
            .map_err(|_| anyhow!("group registry lock poisoned"))?
            .insert(id.to_owned(), group.clone());
        Ok(group)
    }

    pub async fn close_group(self: &Arc<Self>, id: &str) -> GroupResult<()> {
        let broker = self.clone();
        let id = id.to_owned();
        self.change_group(async move { broker.close_group_locked(&id).await })
            .await
    }

    async fn change_group<T: Send + 'static>(
        &self,
        operation: impl Future<Output = GroupResult<T>> + Send + 'static,
    ) -> GroupResult<T> {
        let change = self.change_groups.clone().lock_owned().await;
        // Once accepted, the disk and registry transition owns the lock through
        // completion even if a private RPC stops waiting for its response.
        tokio::spawn(async move {
            let _change = change;
            operation.await
        })
        .await
        .map_err(anyhow::Error::from)?
    }

    async fn close_group_locked(&self, id: &str) -> GroupResult<()> {
        let group = self
            .groups
            .write()
            .map_err(|_| anyhow!("group registry lock poisoned"))?
            .remove(id);
        if let Some(group) = group {
            let shutdown = group.raft.shutdown().await;
            group.snapshots.store.seal().await;
            shutdown.map_err(anyhow::Error::from)?;
        }
        Ok(())
    }

    pub(crate) fn was_retired(&self, id: &str) -> Result<bool> {
        validate_group_id(id)?;
        Ok(GroupStore::new(self.database.clone(), id)
            .retired_at()?
            .is_some())
    }

    /// Delete the incarnation excluded by an authoritative committed membership.
    pub async fn retire_group(self: &Arc<Self>, id: &str, removed_at: u64) -> GroupResult<bool> {
        validate_group_id(id)?;
        let store = GroupStore::new(self.database.clone(), id);
        let broker = self.clone();
        self.change_group(async move {
            let inspect = store.clone();
            let local_id = broker.id;
            if !broker
                .database
                .run(move |_| retirement_matches(&inspect, local_id, removed_at))
                .await?
            {
                return Ok(false);
            }
            broker.close_group_locked(&store.id).await?;
            Ok(broker
                .database
                .run_bulk(move |_| {
                    if !retirement_matches(&store, local_id, removed_at)? {
                        return Ok(false);
                    }
                    store.forget(removed_at)?;
                    Ok(true)
                })
                .await?)
        })
        .await
    }

    /// A durable receipt committed atomically with an offline restore seed.
    pub fn seeded_cut(&self, id: &str) -> Result<Option<SnapshotCut>> {
        validate_group_id(id)?;
        GroupStore::new(self.database.clone(), id).read(b"restore-cut")
    }

    /// Seed the logical image from a different cluster into a pristine group.
    /// The caller must seed all initial voters from the same image, or seed an
    /// RF1 group and snapshot it before adding any learner.
    pub async fn seed_group(&self, id: &str, path: &Path) -> Result<SnapshotCut> {
        validate_group_id(id)?;
        let _change = self.change_groups.lock().await;
        ensure!(self.group(id).is_none(), "cannot restore a running group");
        let snapshots = Snapshots::new(GroupStore::new(self.database.clone(), id), self.cluster_id);
        let path = path.to_path_buf();
        self.database
            .run_bulk(move |db| {
                validate_hosting_capacity(db, &snapshots.store.id)?;
                snapshots.seed(&path)
            })
            .await
    }
}

impl Group {
    fn accepts_votes(&self) -> bool {
        self.accepts_votes.load(Ordering::Acquire)
    }

    async fn enable_votes_after(self: Arc<Self>, cut: u64) {
        let mut metrics = self.raft.metrics();
        loop {
            let may_vote = {
                let status = metrics.borrow_and_update();
                if status.running_state.is_err() || status.state == openraft::ServerState::Shutdown
                {
                    return;
                }
                status.last_applied.is_some_and(|log| log.index > cut)
                    && status
                        .membership_config
                        .voter_ids()
                        .any(|id| id == self.local_id)
            };
            if may_vote {
                let store = self.snapshots.store.clone();
                let id = self.local_id;
                let ready = self
                    .snapshots
                    .store
                    .run(move |_| voter_after(&store, id, cut))
                    .await;
                if matches!(ready, Ok(true)) {
                    self.accepts_votes.store(true, Ordering::Release);
                    self.raft.runtime_config().elect(true);
                    return;
                }
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn id(&self) -> &str {
        &self.snapshots.store.id
    }

    pub fn leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    pub fn is_initialized(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        metrics.last_log_index.is_some() || metrics.vote != openraft::Vote::default()
    }

    /// Local routing hint. Use committed_membership on the leader for a
    /// topology operation that depends on replicated membership.
    pub fn membership(&self) -> Membership {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        describe_membership(
            &metrics.membership_config,
            metrics.last_applied.map(|id| id.index),
        )
    }

    pub async fn committed_membership(&self) -> GroupResult<Membership> {
        self.read(|tx| {
            Ok(describe_membership(
                &tx.header.membership,
                tx.header.applied.map(|id| id.index),
            ))
        })
        .await
    }

    pub fn status(&self) -> GroupStatus {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        GroupStatus {
            term: metrics.current_term,
            leader: metrics.current_leader,
            running: metrics.running_state.is_ok()
                && metrics.state != openraft::ServerState::Shutdown,
            last_log_index: metrics.last_log_index,
            snapshot_index: metrics.snapshot.map(|id| id.index),
            quorum_ack_age_ms: metrics.millis_since_quorum_ack,
            matched_index: metrics.replication.as_ref().map(|nodes| {
                nodes
                    .iter()
                    .map(|(id, matched)| (*id, matched.map(|log| log.index)))
                    .collect()
            }),
            membership: describe_membership(
                &metrics.membership_config,
                metrics.last_applied.map(|id| id.index),
            ),
        }
    }

    pub async fn initialize(&self, members: BTreeMap<u64, String>) -> GroupResult<()> {
        if !self.accepts_votes() {
            return Err(GroupError::Unavailable);
        }
        if members.len() > MAX_GROUP_MEMBERS {
            return Err(anyhow!("Raft group exceeds its member limit").into());
        }
        let mut nodes = BTreeMap::new();
        for (id, address) in members {
            ensure_broker(id, &address, &self.peers)?;
            nodes.insert(id, Node::new(address));
        }
        if !nodes.contains_key(&self.local_id) {
            return Err(anyhow!("initial voters must include this broker").into());
        }
        match tokio::time::timeout(WRITE_DEADLINE, self.raft.initialize(nodes)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(RaftError::APIError(InitializeError::NotAllowed(_)))) => Ok(()),
            Ok(Err(RaftError::Fatal(fatal))) => Err(raft_fatal(fatal)),
            Ok(Err(error)) => Err(anyhow!(error).into()),
            Err(_) => Err(GroupError::OutcomeUnknown),
        }
    }

    pub async fn add_learner(&self, id: u64, peer_url: String) -> GroupResult<()> {
        ensure_broker(id, &peer_url, &self.peers)?;
        let _change = self.change_membership.lock().await;
        let term = self.raft.metrics().borrow().current_term;
        let current = self.committed_membership().await?;
        let cut = if let Some(existing) = current.nodes.get(&id) {
            if existing != &peer_url {
                return Err(anyhow!("existing broker ID has a different peer origin").into());
            }
            if current.voters.contains(&id) {
                return Ok(());
            }
            current.applied_index.ok_or(GroupError::Unavailable)?
        } else {
            if current.nodes.len() >= MAX_GROUP_MEMBERS {
                return Err(anyhow!("Raft group exceeds its member limit").into());
            }
            self.prepare_learner_snapshot(id, term).await?;
            tokio::time::timeout(
                MEMBERSHIP_DEADLINE,
                self.raft.add_learner(id, Node::new(peer_url), false),
            )
            .await
            .map_err(|_| GroupError::OutcomeUnknown)?
            .map_err(write_error)?
            .log_id
            .index
        };
        self.wait_for_replication(&BTreeSet::from([id]), cut, term)
            .await
    }

    async fn prepare_learner_snapshot(&self, id: u64, term: u64) -> GroupResult<()> {
        let membership = self.read(|tx| Ok(tx.header.membership.clone())).await?;
        let Some(membership_log) = *membership.log_id() else {
            return Err(GroupError::Unavailable);
        };
        if membership.voter_ids().any(|voter| voter == id) {
            return Err(GroupError::Unavailable);
        }

        // A blank replica can reuse an ID retired from a historical membership.
        // Offer an image of this exact membership and remove the earlier log chain.
        let eligible = |snapshot: &openraft::Snapshot<TypeConfig>| {
            snapshot.meta.last_membership == membership
                && snapshot
                    .meta
                    .last_log_id
                    .is_some_and(|log| log.index >= membership_log.index)
        };
        if !self
            .snapshots
            .current()
            .await?
            .as_ref()
            .is_some_and(eligible)
        {
            self.raft.trigger().snapshot().await.map_err(raft_fatal)?;
            self.raft
                .wait(Some(SNAPSHOT_DEADLINE))
                .metrics(
                    |view| {
                        view.snapshot
                            .is_some_and(|log| log.index >= membership_log.index)
                    },
                    "wait for an Events snapshot covering the enrollment membership",
                )
                .await
                .map_err(|_| GroupError::Unavailable)?;
        }
        let snapshot = self
            .snapshots
            .current()
            .await?
            .filter(eligible)
            .ok_or(GroupError::Unavailable)?;
        let cut = snapshot.meta.last_log_id.ok_or(GroupError::Unavailable)?;
        self.purge_through(cut.index).await?;

        // A new leader must confirm its own durable image before enrolling a replica.
        let current = self.committed_membership().await?;
        let metrics = self.raft.metrics();
        let status = metrics.borrow();
        if status.current_term != term
            || status.current_leader != Some(self.local_id)
            || current.membership_index != Some(membership_log.index)
        {
            return Err(GroupError::Unavailable);
        }
        Ok(())
    }

    // The library's lag threshold may accept an empty follower. Use a fixed
    // applied cut so every acknowledged learner has a durable copy through it.
    async fn wait_for_replication(
        &self,
        learners: &BTreeSet<u64>,
        cut: u64,
        term: u64,
    ) -> GroupResult<()> {
        let leading = |metrics: &openraft::RaftMetrics<u64, Node>| {
            metrics.running_state.is_ok()
                && metrics.state == openraft::ServerState::Leader
                && metrics.current_leader == Some(self.local_id)
                && metrics.current_term == term
        };
        let metrics = self
            .raft
            .wait(Some(MEMBERSHIP_DEADLINE))
            .metrics(
                |metrics| {
                    !leading(metrics)
                        || metrics.replication.as_ref().is_some_and(|replication| {
                            learners.iter().all(|id| {
                                replication
                                    .get(id)
                                    .and_then(Option::as_ref)
                                    .is_some_and(|matched| matched.index >= cut)
                            })
                        })
                },
                "wait for Events learners to match the applied cut",
            )
            .await
            .map_err(|_| GroupError::Unavailable)?;
        if !leading(&metrics) {
            return Err(if metrics.current_leader == Some(self.local_id) {
                GroupError::Unavailable
            } else {
                GroupError::NotLeader(metrics.current_leader)
            });
        }
        Ok(())
    }

    pub async fn change_membership(&self, voters: BTreeSet<u64>) -> GroupResult<()> {
        let _change = self.change_membership.lock().await;
        let term = self.raft.metrics().borrow().current_term;
        let current = self.committed_membership().await?;
        self.apply_membership_change(current, voters, term).await
    }

    /// Fence a resumed controller intent against a different committed membership.
    pub async fn change_membership_from(
        &self,
        expected: &BTreeSet<u64>,
        voters: BTreeSet<u64>,
    ) -> GroupResult<()> {
        let _change = self.change_membership.lock().await;
        let term = self.raft.metrics().borrow().current_term;
        let current = self.committed_membership().await?;
        let matches = if current.joint {
            current.voter_sets.len() == 2
                && current.voter_sets.contains(expected)
                && current.voter_sets.contains(&voters)
        } else {
            current.voters == *expected || current.voters == voters
        };
        if !matches {
            return Err(GroupError::Unavailable);
        }
        self.apply_membership_change(current, voters, term).await
    }

    async fn apply_membership_change(
        &self,
        current: Membership,
        voters: BTreeSet<u64>,
        term: u64,
    ) -> GroupResult<()> {
        if !current.joint && current.voters == voters && current.nodes.len() == voters.len() {
            return Ok(());
        }
        let learners = voters
            .difference(&current.voters)
            .copied()
            .collect::<BTreeSet<_>>();
        if !learners.is_empty() {
            if !learners.iter().all(|id| current.nodes.contains_key(id)) {
                return Err(GroupError::Unavailable);
            }
            let cut = current.applied_index.ok_or(GroupError::Unavailable)?;
            // Recheck on the proposing leader, even if a previous AddLearner
            // call confirmed catch-up before a leadership change.
            self.wait_for_replication(&learners, cut, term).await?;
        }
        let result = tokio::time::timeout(
            MEMBERSHIP_DEADLINE,
            self.raft.change_membership(voters, false),
        )
        .await
        .map_err(|_| GroupError::OutcomeUnknown)?;
        result.map(|_| ()).map_err(write_error)
    }

    pub async fn barrier(&self) -> GroupResult<()> {
        use openraft::error::CheckIsLeaderError::{ForwardToLeader, QuorumNotEnough};
        if !self.accepts_votes() {
            return Err(GroupError::NotLeader(
                self.leader().filter(|id| *id != self.local_id),
            ));
        }
        match tokio::time::timeout(READ_DEADLINE, self.raft.ensure_linearizable()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(RaftError::APIError(ForwardToLeader(forward)))) => {
                Err(GroupError::NotLeader(forward.leader_id))
            }
            Ok(Err(RaftError::Fatal(fatal))) => Err(raft_fatal(fatal)),
            Ok(Err(RaftError::APIError(QuorumNotEnough(_)))) | Err(_) => {
                Err(GroupError::Unavailable)
            }
        }
    }

    pub async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> GroupResult<T> {
        self.barrier().await?;
        let store = self.snapshots.store.clone();
        Ok(tokio::time::timeout(
            READ_DEADLINE,
            self.snapshots.store.run(move |_| f(&store.transaction()?)),
        )
        .await
        .map_err(|_| GroupError::Unavailable)??)
    }

    pub async fn write(&self, command: Value) -> GroupResult<Value> {
        command_size(&command)?;
        let _permit = self
            .control_slots
            .try_acquire()
            .map_err(|_| GroupError::Overloaded)?;
        self.check_write_authority().await?;
        submit(&self.raft, RaftCommand::Single(command)).await
    }

    async fn check_write_authority(&self) -> GroupResult<()> {
        let status = self.raft.metrics().borrow().clone();
        if status.running_state.is_err() || status.state == openraft::ServerState::Shutdown {
            return Err(GroupError::Closed);
        }
        if !self.accepts_votes() || status.current_leader != Some(self.local_id) {
            return Err(GroupError::NotLeader(
                status.current_leader.filter(|id| *id != self.local_id),
            ));
        }
        // Once the last quorum contact is stale, confirm a quorum before
        // appending more work to an isolated leader's uncommitted log.
        if status.millis_since_quorum_ack.is_none_or(|age| age > 400) {
            self.barrier().await?;
        }
        Ok(())
    }

    /// Coalesce independent publish operations under the broker's shared byte
    /// budget and a short delay. Control writes use write() directly.
    pub async fn write_coalesced(&self, command: Value) -> GroupResult<Value> {
        self.check_write_authority().await?;
        self.publish.write(command).await
    }

    pub async fn export(&self, path: &Path) -> GroupResult<SnapshotCut> {
        self.export_bounded(path, MAX_SNAPSHOT_BYTES).await
    }

    pub async fn export_bounded(
        &self,
        path: &Path,
        maximum_bytes: u64,
    ) -> GroupResult<SnapshotCut> {
        self.barrier().await?;
        Ok(self.snapshots.export(path, maximum_bytes).await?)
    }

    /// Compact to a recoverable image before enrolling replicas in a group
    /// that was seeded with an offline restore.
    pub async fn snapshot_and_purge(&self) -> GroupResult<()> {
        self.barrier().await?;
        let Some(index) = self.membership().applied_index else {
            return Err(GroupError::Unavailable);
        };
        self.raft.trigger().snapshot().await.map_err(raft_fatal)?;
        self.raft
            .wait(Some(SNAPSHOT_DEADLINE))
            .metrics(
                |view| view.snapshot.is_some_and(|log| log.index >= index),
                "wait for durable Events snapshot",
            )
            .await
            .map_err(|_| GroupError::Unavailable)?;
        self.purge_through(index).await
    }

    async fn purge_through(&self, index: u64) -> GroupResult<()> {
        self.raft
            .trigger()
            .purge_log(index)
            .await
            .map_err(raft_fatal)?;
        self.raft
            .wait(Some(SNAPSHOT_DEADLINE))
            .metrics(
                |view| view.purged.is_some_and(|log| log.index >= index),
                "wait for Events snapshot log purge",
            )
            .await
            .map_err(|_| GroupError::Unavailable)?;
        Ok(())
    }
}

fn voter_after(store: &GroupStore, id: u64, cut: u64) -> Result<bool> {
    let membership = store.header()?.membership;
    Ok(membership.log_id().is_some_and(|log| log.index > cut)
        && membership.voter_ids().any(|voter| voter == id))
}

fn retirement_matches(store: &GroupStore, id: u64, removed_at: u64) -> Result<bool> {
    if store.db.is_quarantined(&store.id) {
        // The damaged group's records cannot be read, so only the store-wide retirement
        // marker fences a stale request. The replica never restarts on these bytes.
        return Ok(store.retired_at()?.is_none_or(|cut| cut < removed_at));
    }
    if store
        .read::<u64>(ELECTION_FLOOR)?
        .is_some_and(|floor| floor >= removed_at)
        || store.retired_at()?.is_some_and(|cut| cut >= removed_at)
    {
        return Ok(false);
    }
    let membership = store.header()?.membership;
    Ok(membership.log_id().is_none_or(|log| {
        log.index < removed_at
            || (log.index == removed_at && membership.voter_ids().all(|voter| voter != id))
    }))
}

fn validate_hosting_capacity(database: &Database, id: &str) -> Result<()> {
    if !database.is_hosted(id)? {
        ensure!(
            database.stored_group_ids()?.len() < MAX_HOSTED_GROUPS,
            "broker has reached its hosted group limit"
        );
    }
    Ok(())
}

fn validate_group_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "invalid Raft group ID"
    );
    Ok(())
}

fn ensure_broker(id: u64, address: &str, peers: &PeerTransport) -> Result<()> {
    ensure!(id != 0, "broker ID must be positive");
    peers.validate_address(address)
}

fn describe_membership(
    membership: &openraft::StoredMembership<u64, Node>,
    applied_index: Option<u64>,
) -> Membership {
    Membership {
        voters: membership.voter_ids().collect(),
        nodes: membership
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect(),
        joint: membership.membership().get_joint_config().len() > 1,
        voter_sets: membership.membership().get_joint_config().clone(),
        applied_index,
        membership_index: membership.log_id().map(|log| log.index),
    }
}

async fn submit(raft: &Raft<TypeConfig>, command: RaftCommand) -> GroupResult<Value> {
    let result = tokio::time::timeout(WRITE_DEADLINE, raft.client_write(command))
        .await
        .map_err(|_| GroupError::OutcomeUnknown)?;
    result.map(|response| response.data).map_err(write_error)
}

fn command_size(command: &Value) -> GroupResult<usize> {
    let bytes = serde_json::to_vec(command)
        .map_err(anyhow::Error::from)?
        .len();
    if bytes > MAX_COMMAND_BYTES - 64 {
        return Err(anyhow!("Raft command exceeds its byte limit").into());
    }
    Ok(bytes)
}

fn write_error(error: RaftError<u64, openraft::error::ClientWriteError<u64, Node>>) -> GroupError {
    if let Some(forward) = error.forward_to_leader::<Node>() {
        return GroupError::NotLeader(forward.leader_id);
    }
    match error {
        RaftError::Fatal(fatal) => raft_fatal(fatal),
        error => GroupError::Other(anyhow!(error)),
    }
}

fn raft_fatal(error: Fatal<u64>) -> GroupError {
    tracing::error!(%error, "Events Raft group stopped");
    GroupError::Closed
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: anyhow::Error,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, io::Error::other(error.to_string()))
}
