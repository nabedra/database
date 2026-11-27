mod backup;
mod catalog;
mod forward;
mod operate;
mod serve;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    http::{
        self, BackendError, BackendFuture, HttpLimits, Keyring, KeyringConfig, PartitionPage,
        PartitionRoute,
    },
    partition,
    raft::{Broker, BrokerConfig, ClockPolicy, Group, GroupError},
};

use catalog::{Bootstrap, BrokerSpec, Header, Topic};
use forward::GroupCall;
use operate::BackupLayout;

pub use backup::{run_backup, run_restore};
pub use serve::{RECOVERY_EXIT_CODE, StoreRecoveryRequired, serve};

const METADATA: &str = "metadata";
const RESTORE_REBIND: &str = "restore-rebind";
/// Written by `nabedra-events init` on the first metadata voter's empty volume: the one
/// authorization to create a new cluster. `Control::join_metadata` removes it once the
/// metadata group is initialized, and a volume that loses its data loses it too.
const INITIALIZE_CLUSTER: &str = "initialize-cluster";
const CACHE_TOPICS: usize = 32;
const CACHE_LEADERS: usize = 512;
const CACHE_FRESH_FOR: Duration = Duration::from_secs(3);
const CONTROL_WORK_TIMEOUT: Duration = Duration::from_secs(150);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub broker_id: u64,
    pub cluster_id: Uuid,
    pub data_dir: PathBuf,
    public_bind: SocketAddr,
    peer_bind: SocketAddr,
    peer_token: String,
    #[serde(default)]
    pub allow_plaintext: bool,
    pub peer_ca: Option<PathBuf>,
    pub peer_identity: Option<PathBuf>,
    public_tls: Option<TlsFiles>,
    peer_tls: Option<TlsFiles>,
    bootstrap: Bootstrap,
    keyring: Option<KeyringConfig>,
    #[serde(default)]
    http_limits: HttpLimits,
    #[serde(default = "default_disk_reserve")]
    min_free_disk_bytes: u64,
    /// The stated broker clock bound (EVENTS-2, EVT-G08). Older configs omit it and get
    /// the defaults.
    #[serde(default)]
    clock: ClockPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TlsFiles {
    cert: PathBuf,
    key: PathBuf,
}

fn default_disk_reserve() -> u64 {
    128 * 1024 * 1024
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        const MAX_CONFIG_BYTES: u64 = 1 << 20;
        use std::io::Read;
        let mut file = std::fs::File::open(path).context("open broker config")?;
        let mut data = Vec::new();
        (&mut file)
            .take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut data)?;
        ensure!(
            data.len() as u64 <= MAX_CONFIG_BYTES,
            "broker config exceeds size limit"
        );
        let config: Self = serde_json::from_slice(&data).context("parse broker config")?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        ensure!(!self.cluster_id.is_nil(), "cluster_id cannot be nil");
        ensure!(
            self.keyring
                .as_ref()
                .is_some_and(|keys| keys.cluster_id == self.cluster_id),
            "keyring belongs to another cluster"
        );
        ensure!(
            self.public_bind.port() != 0
                && self.peer_bind.port() != 0
                && self.public_bind.port() != self.peer_bind.port(),
            "configure distinct public and peer ports"
        );
        ensure!(
            self.allow_plaintext || self.public_tls.is_some(),
            "public TLS is required"
        );
        ensure!(
            (0..=60_000).contains(&self.clock.max_clock_error_ms)
                && self.clock.max_uncertainty_ms >= 2 * self.clock.max_clock_error_ms
                && (1..=60_000).contains(&self.clock.sample_max_age_ms),
            "the clock bound needs 0 to 60000 ms of error, an uncertainty budget of at least \
             twice the error and a sample age of 1 to 60000 ms"
        );
        ensure!(
            self.allow_plaintext
                || self.peer_tls.is_some()
                    && self.peer_ca.is_some()
                    && self.peer_identity.is_some(),
            "private peer TLS with client verification is required"
        );
        let roster = self
            .bootstrap
            .broker_map(self.allow_plaintext)
            .context("invalid bootstrap roster")?;
        ensure!(
            roster.contains_key(&self.broker_id),
            "bootstrap roster must include this broker"
        );
        Ok(())
    }

    pub fn issue_grant(mut self, path: &Path) -> Result<String> {
        use std::io::Read;
        let mut file = std::fs::File::open(path).context("open grant request")?;
        let mut data = Vec::new();
        (&mut file).take(8 * 1024 + 1).read_to_end(&mut data)?;
        ensure!(data.len() <= 8 * 1024, "grant request exceeds size limit");
        let grant = serde_json::from_slice(&data).context("parse grant request")?;
        self.take_keyring()?.issue_grant(grant, http::now_ms())
    }

    fn take_keyring(&mut self) -> Result<Keyring> {
        Keyring::new(
            self.keyring
                .take()
                .context("broker config needs a keyring")?,
        )
    }

    /// `nabedra-events init`: authorize this broker to create a new cluster on its next
    /// start. Only the first bootstrap metadata voter, only on a volume that never served.
    /// After a total loss of the bootstrap disks a new cluster also needs a new
    /// `cluster_id` and keyring, so old grants, tokens and stray brokers are refused.
    pub fn initialize(&self) -> Result<()> {
        ensure!(
            self.bootstrap.metadata_voters.first() == Some(&self.broker_id),
            "only the first bootstrap metadata voter initializes a new cluster"
        );
        ensure!(
            !self.data_dir.join("kv").try_exists()?,
            "this volume already holds an Events store; a new cluster starts on an empty volume"
        );
        std::fs::create_dir_all(&self.data_dir)?;
        let marker = self.data_dir.join(INITIALIZE_CLUSTER);
        let file = std::fs::File::create(&marker)?;
        file.sync_all()?;
        std::fs::File::open(&self.data_dir)?.sync_all()?;
        Ok(())
    }

    pub(super) fn open_broker(&self) -> Result<Arc<Broker>> {
        if !self.allow_plaintext {
            require_mount(&self.data_dir)?;
        }
        Broker::open(BrokerConfig {
            id: self.broker_id,
            cluster_id: self.cluster_id,
            data_dir: self.data_dir.clone(),
            peer_token: self.peer_token.clone(),
            peer_ca: self.peer_ca.clone(),
            peer_identity: self.peer_identity.clone(),
            allow_plaintext: self.allow_plaintext,
        })
    }
}

#[cfg(target_os = "linux")]
fn require_mount(dir: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let absolute = dir.canonicalize().context("resolve broker volume mount")?;
    let mut encoded = Vec::new();
    for &byte in absolute.as_os_str().as_bytes() {
        match byte {
            b' ' => encoded.extend_from_slice(b"\\040"),
            b'\t' => encoded.extend_from_slice(b"\\011"),
            b'\n' => encoded.extend_from_slice(b"\\012"),
            b'\\' => encoded.extend_from_slice(b"\\134"),
            _ => encoded.push(byte),
        }
    }
    let mounts = std::fs::read("/proc/self/mountinfo").context("read volume mounts")?;
    ensure!(
        mounts
            .split(|byte| *byte == b'\n')
            .any(|line| line.split(|byte| *byte == b' ').nth(4) == Some(encoded.as_slice())),
        "the broker data directory must be a mounted volume"
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn require_mount(_: &Path) -> Result<()> {
    anyhow::bail!("production broker volume validation requires Linux")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, thiserror::Error)]
pub(super) enum Error {
    #[error("invalid operator request")]
    Invalid,
    #[error("topic, partition, or operation does not exist")]
    NotFound,
    #[error("request conflicts with committed topology")]
    Conflict,
    #[error("a topology operation is already in progress")]
    Busy,
    #[error("no eligible placement or processing capacity")]
    Capacity,
    #[error("cluster supports at most 4095 data partitions")]
    PartitionLimit,
    #[error("broker disk is below its free-space reserve")]
    Disk,
    #[error("group has no reachable quorum")]
    Unavailable,
    #[error("operation may have been committed")]
    Unknown,
    #[error("unexpected broker failure")]
    Internal,
}

impl From<Error> for BackendError {
    fn from(error: Error) -> Self {
        match error {
            Error::NotFound => Self::NotFound,
            Error::Busy | Error::Capacity => Self::Overloaded,
            Error::Disk => Self::InsufficientStorage,
            Error::Unknown => Self::OutcomeUnknown,
            Error::Internal | Error::Invalid | Error::Conflict | Error::PartitionLimit => {
                Self::Internal
            }
            Error::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Default)]
struct TopicCache {
    entries: VecDeque<(Topic, Instant)>,
}

impl TopicCache {
    fn get(&mut self, name: &str) -> Option<Topic> {
        let idx = self
            .entries
            .iter()
            .position(|(topic, _)| topic.name == name)?;
        let (topic, read_at) = self.entries.remove(idx)?;
        let entry = topic.clone();
        self.entries.push_back((topic, read_at));
        Some(entry)
    }

    fn put(&mut self, topic: Topic) {
        self.entries.retain(|(entry, _)| entry.name != topic.name);
        self.entries.push_back((topic, Instant::now()));
        if self.entries.len() > CACHE_TOPICS {
            self.entries.pop_front();
        }
    }

    fn by_id(&mut self, id: Uuid) -> Option<Topic> {
        let name = self
            .entries
            .iter()
            .find_map(|(topic, _)| (topic.id == id).then(|| topic.name.clone()))?;
        self.get(&name)
    }

    fn stale_name(&self) -> Option<String> {
        self.entries
            .iter()
            .filter(|(_, at)| at.elapsed() > CACHE_FRESH_FOR)
            .min_by_key(|(_, at)| *at)
            .map(|(topic, _)| topic.name.clone())
    }

    fn probe_route(&self, cursor: usize) -> Option<(Uuid, u32, BTreeSet<u64>)> {
        let (topic, _) = self.entries.get(cursor.checked_rem(self.entries.len())?)?;
        let index = cursor.checked_rem(topic.partitions.len())?;
        Some((
            topic.id,
            index as u32,
            topic.partitions[index].voters.clone(),
        ))
    }
}

pub(super) struct Control {
    config: Config,
    broker: Arc<Broker>,
    keyring: Arc<Keyring>,
    bootstrap: BTreeMap<u64, BrokerSpec>,
    catalog: RwLock<Option<Header>>,
    topics: Mutex<TopicCache>,
    leaders: Mutex<BTreeMap<String, u64>>,
    ready_at: Mutex<Option<Instant>>,
    disk_available: AtomicU64,
    admin_slots: Semaphore,
    peer_data: Semaphore,
    peer_control: Semaphore,
    publish_metrics: Calls,
    poll_metrics: Calls,
    /// The metadata group still waits for the empty-volume rule (EVENTS-1). It holds
    /// `true` on the broker that initializes a new cluster.
    metadata_join: Mutex<Option<bool>>,
    /// The rule found this volume's metadata replica lost: an operator re-adds it.
    metadata_lost: AtomicBool,
}

const LATENCY_BOUNDS_MS: [u64; 11] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000];

#[derive(Default)]
struct Calls {
    failures: AtomicU64,
    latency: [AtomicU64; LATENCY_BOUNDS_MS.len() + 1],
}

impl Calls {
    fn observe(&self, started: Instant, failed: bool) {
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let bucket = LATENCY_BOUNDS_MS.partition_point(|bound| *bound < elapsed_ms);
        self.latency[bucket].fetch_add(1, Ordering::Relaxed);
        if failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn report(&self) -> serde_json::Value {
        serde_json::json!({
            "failures": self.failures.load(Ordering::Relaxed),
            "latency_upper_bounds_ms": LATENCY_BOUNDS_MS,
            "latency_counts": self.latency.each_ref().map(|count| count.load(Ordering::Relaxed)),
        })
    }
}

impl Control {
    fn new(mut config: Config, broker: Arc<Broker>) -> Result<Arc<Self>> {
        let bootstrap = config.bootstrap.broker_map(config.allow_plaintext)?;
        let keyring = Arc::new(config.take_keyring()?);
        broker.set_clock_policy(config.clock);
        let disk_available = AtomicU64::new(broker.disk_available_bytes()?);
        Ok(Arc::new(Self {
            config,
            broker,
            keyring,
            bootstrap,
            catalog: RwLock::new(None),
            topics: Mutex::new(TopicCache::default()),
            leaders: Mutex::new(BTreeMap::new()),
            ready_at: Mutex::new(None),
            disk_available,
            admin_slots: Semaphore::new(8),
            peer_data: Semaphore::new(24),
            peer_control: Semaphore::new(24),
            publish_metrics: Calls::default(),
            poll_metrics: Calls::default(),
            metadata_join: Mutex::new(None),
            metadata_lost: AtomicBool::new(false),
        }))
    }

    fn known_roster(&self) -> BTreeMap<u64, BrokerSpec> {
        self.catalog
            .read()
            .expect("catalog lock")
            .as_ref()
            .map_or_else(|| self.bootstrap.clone(), |header| header.roster.clone())
    }

    fn remember_header(&self, header: Header) {
        let mut catalog = self.catalog.write().expect("catalog lock");
        if header.cluster_id == self.config.cluster_id
            && catalog
                .as_ref()
                .is_none_or(|previous| previous.revision <= header.revision)
        {
            *catalog = Some(header);
        }
    }

    fn topology_loaded(&self) -> bool {
        self.catalog.read().expect("catalog lock").is_some()
    }

    fn metadata_candidates(&self) -> BTreeSet<u64> {
        let view = self.catalog.read().expect("catalog lock");
        view.as_ref().map_or_else(
            || self.config.bootstrap.metadata_voters.clone(),
            |header| header.metadata.clone(),
        )
    }

    async fn cache_topic(&self, name: &str, refresh: bool) -> Result<Topic, Error> {
        let cached = self.topics.lock().expect("topics lock").get(name);
        if !refresh && let Some(topic) = &cached {
            return Ok(topic.clone());
        }
        match self
            .meta_read(catalog::Query::Topic { name: name.into() })
            .await
        {
            Ok(catalog::Reply::Topic(topic)) => {
                self.topics.lock().expect("topics lock").put(topic.clone());
                Ok(topic)
            }
            Err(Error::Unavailable | Error::Capacity) if cached.is_some() => {
                Ok(cached.expect("cached topic"))
            }
            Err(error) => Err(error),
            Ok(_) => Err(Error::Internal),
        }
    }

    async fn open_group(&self, group: &str) -> Result<Arc<Group>, GroupError> {
        self.open_group_after(group, None).await
    }

    async fn open_group_after(
        &self,
        group: &str,
        election_after: Option<u64>,
    ) -> Result<Arc<Group>, GroupError> {
        let machine: Arc<dyn crate::raft::StateMachine> = if group == METADATA {
            Arc::new(catalog::Machine)
        } else if parse_partition_id(group).is_some() {
            Arc::new(partition::Machine)
        } else {
            return Err(anyhow::anyhow!("unknown group kind").into());
        };
        self.broker
            .open_group_after(group, machine, election_after)
            .await
    }

    fn mark_ready(&self) {
        *self.ready_at.lock().expect("readiness lock") = Some(Instant::now());
    }

    fn mark_group_result<T>(&self, result: Result<T, Error>) -> Result<T, BackendError> {
        let value = result?;
        self.mark_ready();
        Ok(value)
    }

    /// Ready while a group call succeeded recently and the store is usable: a store that
    /// requires recovery cannot do any work, whatever other brokers answer (RV-EV M2).
    fn ready(&self) -> bool {
        !self.broker.requires_recovery()
            && self
                .ready_at
                .lock()
                .expect("readiness lock")
                .is_some_and(|at| at.elapsed() < Duration::from_secs(6))
    }

    /// Free space below which publishes are refused: the configured reserve plus the room
    /// the store needs to keep committing acknowledgments, claims and replication (RV-EV
    /// M1).
    fn disk_reserve_bytes(&self) -> u64 {
        self.config
            .min_free_disk_bytes
            .saturating_add(self.broker.disk_headroom_bytes())
    }

    async fn partition_call<T: serde::de::DeserializeOwned>(
        &self,
        route: &PartitionRoute,
        call: GroupCall,
    ) -> Result<T, Error> {
        // Placement cache entries are keyed by stable topic ID, so a replay
        // can never be routed using a cached name from a different topic.
        let cached = self
            .topics
            .lock()
            .expect("topics lock")
            .by_id(route.topic_id);
        let mut topic = match cached {
            Some(topic) => topic,
            None => match self
                .meta_read(catalog::Query::TopicById { id: route.topic_id })
                .await?
            {
                catalog::Reply::Topic(topic) => topic,
                _ => return Err(Error::Internal),
            },
        };
        if let Some(name) = call.subscription()
            && !topic.subscriptions.contains_key(name)
        {
            topic = self.cache_topic(&topic.name, true).await?;
            if !topic.subscriptions.contains_key(name) {
                return Err(Error::NotFound);
            }
        }
        let placement = topic
            .partitions
            .get(route.partition as usize)
            .ok_or(Error::NotFound)?;
        let request = forward::Rpc::Group {
            group: route.group_id.clone(),
            call,
        };
        match self
            .call_group(&route.group_id, &placement.voters, &request)
            .await
        {
            Err(Error::Unavailable) => {
                if let Ok(current) = self.cache_topic(&topic.name, true).await
                    && let Some(current) = current.partitions.get(route.partition as usize)
                    && placement.voters != current.voters
                {
                    return self
                        .call_group(&route.group_id, &current.voters, &request)
                        .await;
                }
                Err(Error::Unavailable)
            }
            result => result,
        }
    }

    async fn meta_read(&self, query: catalog::Query) -> Result<catalog::Reply, Error> {
        let request = forward::Rpc::Group {
            group: METADATA.into(),
            call: GroupCall::MetaRead { query },
        };
        self.call_group::<Result<catalog::Reply, Error>>(
            METADATA,
            &self.metadata_candidates(),
            &request,
        )
        .await?
    }

    async fn meta_write(&self, command: catalog::Command) -> Result<catalog::Reply, Error> {
        let request = forward::Rpc::Group {
            group: METADATA.into(),
            call: GroupCall::MetaWrite { command },
        };
        self.call_group::<Result<catalog::Reply, Error>>(
            METADATA,
            &self.metadata_candidates(),
            &request,
        )
        .await?
    }
}

impl http::Backend for Control {
    fn partitions<'a>(
        &'a self,
        topic: &'a str,
        after: Option<u32>,
        limit: usize,
    ) -> BackendFuture<'a, PartitionPage> {
        Box::pin(async move {
            if !self.topology_loaded() {
                return Err(BackendError::Unavailable);
            }
            let topic = self.cache_topic(topic, true).await?;
            let end = topic.partitions.len() as u32;
            let start = after.map_or(0, |offset| offset.saturating_add(1));
            let page_end = end.min(start.saturating_add(limit.clamp(1, 100) as u32));
            Ok(PartitionPage {
                topic_id: topic.id,
                partitions: (start..page_end).collect(),
                next_after: (page_end < end).then(|| page_end - 1),
            })
        })
    }

    fn resolve<'a>(&'a self, topic: &'a str, partition: u32) -> BackendFuture<'a, PartitionRoute> {
        Box::pin(async move {
            if !self.topology_loaded() {
                return Err(BackendError::Unavailable);
            }
            let mut definition = self.cache_topic(topic, false).await?;
            if definition.partitions.get(partition as usize).is_none() {
                definition = self.cache_topic(topic, true).await?;
            }
            if definition.partitions.get(partition as usize).is_none() {
                return Err(BackendError::NotFound);
            }
            Ok(PartitionRoute {
                topic_id: definition.id,
                partition,
                group_id: partition_id(definition.id, partition),
            })
        })
    }

    fn write<'a>(
        &'a self,
        route: &'a PartitionRoute,
        command: partition::Command,
    ) -> BackendFuture<'a, partition::Mutation> {
        Box::pin(async move {
            let publish = matches!(
                &command,
                partition::Command::Publish { .. } | partition::Command::PublishMany { .. }
            );
            let started = Instant::now();
            let result = self
                .partition_call::<partition::Mutation>(route, GroupCall::PartitionWrite { command })
                .await;
            if publish {
                self.publish_metrics
                    .observe(started, !matches!(&result, Ok(Ok(_))));
            }
            self.mark_group_result(result)
        })
    }

    fn read<'a>(
        &'a self,
        route: &'a PartitionRoute,
        query: partition::Query,
    ) -> BackendFuture<'a, Result<partition::QueryReply, partition::Error>> {
        Box::pin(async move {
            let started = Instant::now();
            let result = self
                .partition_call::<Result<partition::QueryReply, partition::Error>>(
                    route,
                    GroupCall::PartitionRead { query },
                )
                .await;
            self.poll_metrics
                .observe(started, !matches!(&result, Ok(Ok(_))));
            self.mark_group_result(result)
        })
    }
}

fn partition_id(topic: Uuid, partition: u32) -> String {
    format!("p-{}-{partition}", topic.simple())
}

fn parse_partition_id(group: &str) -> Option<(Uuid, u32)> {
    let (topic, partition) = group.strip_prefix("p-")?.split_once('-')?;
    if topic.len() != 32 {
        return None;
    }
    let (topic, partition) = (Uuid::parse_str(topic).ok()?, partition.parse().ok()?);
    (partition_id(topic, partition) == group).then_some((topic, partition))
}
