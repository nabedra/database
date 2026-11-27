mod events;
mod subscriptions;

use crate::raft::{StateMachine, Transaction};
use anyhow::{Context, Result as StorageResult};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_EVENTS: usize = 100;
pub const MAX_PAGE_EVENTS: usize = 100;
pub const MAX_RETAIN_EVENTS: usize = 128;
pub const MAX_LEASE_MS: i64 = 300_000;
pub const MAX_SUBSCRIPTIONS_PER_PARTITION: u32 = 4096;
const MAX_TIMESTAMP_MS: i64 = 253_402_300_799_999;
const HEAD_KEY: &[u8] = b"partition/v1/head";

/// Stamps a command the leader is about to propose with guarded time (EVENTS-2; plan,
/// "Brokers and storage"). A publish gets the upper bound, so an event's age never runs
/// ahead of true time. Lease commands get the lower bound, against which the replicated
/// lease clock tests expiry, and a TTL widened by the interval's width, so a new lease's
/// deadline is the upper bound plus the requested TTL.
pub fn stamp(command: &mut Command, bounds: crate::raft::TimeBounds) {
    match command {
        Command::Publish { now_ms, .. } | Command::PublishMany { now_ms, .. } => {
            *now_ms = bounds.upper_ms;
        }
        Command::Claim { now_ms, ttl_ms, .. } | Command::Renew { now_ms, ttl_ms, .. } => {
            *now_ms = bounds.lower_ms;
            if (1..=MAX_LEASE_MS).contains(ttl_ms) {
                let width = bounds.upper_ms.saturating_sub(bounds.lower_ms).max(0);
                *ttl_ms = ttl_ms.saturating_add(width).min(MAX_LEASE_MS);
            }
        }
        Command::Release { now_ms, .. } | Command::Ack { now_ms, .. } => {
            *now_ms = bounds.lower_ms;
        }
        _ => {}
    }
}

/// Stamps a poll with the lower bound, the time its lease check uses (EVENTS-2).
pub fn stamp_query(query: &mut Query, bounds: crate::raft::TimeBounds) {
    if let Query::Poll { now_ms, .. } = query {
        *now_ms = bounds.lower_ms;
    }
}

pub fn valid_name(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishEvent {
    pub event_id: Uuid,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub topic_id: Uuid,
    pub partition: u32,
    #[serde(with = "offset")]
    pub offset: u64,
    pub event_id: Uuid,
    pub published_at: String,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Start {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Init {
        topic_id: Uuid,
        partition: u32,
    },
    Publish {
        now_ms: i64,
        events: Vec<PublishEvent>,
    },
    PublishMany {
        now_ms: i64,
        batches: Vec<Vec<PublishEvent>>,
    },
    OpenSubscription {
        name: String,
        start: Start,
    },
    Claim {
        name: String,
        owner: String,
        claim_id: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    },
    Renew {
        name: String,
        owner: String,
        generation: u64,
        now_ms: i64,
        ttl_ms: i64,
    },
    Release {
        name: String,
        owner: String,
        generation: u64,
        now_ms: i64,
    },
    Ack {
        name: String,
        owner: String,
        generation: u64,
        after: u64,
        last: u64,
        through: u64,
        now_ms: i64,
    },
    ResetExpired {
        name: String,
    },
    Retain {
        cutoff_ms: i64,
        limit: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Done,
    Published(Published),
    Coalesced {
        results: Vec<Result<Published, Error>>,
    },
    Lease(Lease),
    Checkpoint {
        #[serde(with = "offset")]
        offset: u64,
    },
    Retained {
        removed: usize,
        #[serde(with = "offset")]
        earliest_offset: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Published {
    pub events: Vec<Event>,
    pub inserted: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Lease {
    pub generation: u64,
    pub expires_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Poll {
        name: String,
        owner: String,
        generation: u64,
        now_ms: i64,
        limit: usize,
    },
    Status,
    SubscriptionStatus {
        name: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueryReply {
    Poll(Poll),
    Status(Status),
    SubscriptionStatus(SubscriptionStatus),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Poll {
    #[serde(with = "offset")]
    pub after: u64,
    // The last offset is carried in the authenticated delivery token on the public API.
    pub last: Option<u64>,
    pub events: Vec<Event>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub topic_id: Uuid,
    pub partition: u32,
    #[serde(with = "offset")]
    pub earliest_offset: u64,
    #[serde(with = "offset")]
    pub tail: u64,
    pub oldest_published_ms: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscriptionStatus {
    #[serde(with = "offset")]
    pub checkpoint: u64,
    #[serde(with = "offset")]
    pub earliest_offset: u64,
    #[serde(with = "offset")]
    pub tail: u64,
    pub lease_generation: Option<u64>,
    pub lease_expires_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    PayloadTooLarge,
    InsufficientStorage,
    Conflict,
    NotFound,
    LeaseBusy,
    LeaseLost,
    CheckpointExpired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_offset"
    )]
    pub earliest_offset: Option<u64>,
}

impl Error {
    pub fn new(code: ErrorCode, message: &str) -> Self {
        Self {
            code,
            message: message.into(),
            partition: None,
            earliest_offset: None,
        }
    }
}

pub type Mutation = Result<Reply, Error>;

#[derive(Default)]
pub struct Machine;

impl StateMachine for Machine {
    fn apply(&self, tx: &mut Transaction<'_>, command: Value) -> StorageResult<Value> {
        let Ok(command) = serde_json::from_value(command) else {
            let error = Error::new(ErrorCode::InvalidRequest, "invalid partition command");
            return Ok(serde_json::to_value(Err::<Reply, _>(error))?);
        };
        Ok(serde_json::to_value(as_outcome(apply(tx, command))?)?)
    }
}

/// Trims one partition group's expired events (EVENTS-3): one `Retain` entry per
/// [`MAX_RETAIN_EVENTS`] events older than `cutoff_ms`, repeated while a full page was
/// removed and `deadline` (on [`crate::rt::now`]) allows. Returns the events removed and
/// the earliest offset the last entry left.
pub async fn trim(
    group: &crate::raft::Group,
    cutoff_ms: i64,
    deadline: crate::rt::Instant,
) -> (usize, Option<u64>) {
    let status = group.read(|tx| query(tx, Query::Status)).await;
    let expired = matches!(
        status,
        Ok(Ok(QueryReply::Status(status)))
            if status.oldest_published_ms.is_some_and(|time| time < cutoff_ms)
    );
    let (mut total, mut earliest) = (0, None);
    while expired {
        let Ok(command) = serde_json::to_value(Command::Retain {
            cutoff_ms,
            limit: MAX_RETAIN_EVENTS,
        }) else {
            break;
        };
        let Ok(reply) = group.write(command).await else {
            break;
        };
        let Ok(Ok(Reply::Retained {
            removed,
            earliest_offset,
        })) = serde_json::from_value::<Mutation>(reply)
        else {
            break;
        };
        total += removed;
        earliest = Some(earliest_offset);
        if removed < MAX_RETAIN_EVENTS || crate::rt::now() >= deadline {
            break;
        }
    }
    (total, earliest)
}

/// Execute on a read snapshot acquired through the group's quorum-confirmed read.
pub fn query(tx: &Transaction<'_>, query: Query) -> StorageResult<Result<QueryReply, Error>> {
    as_outcome(read(tx, query))
}

#[cfg(feature = "sim")]
#[derive(PartialEq)]
pub(crate) struct SimState {
    pub tail: u64,
    pub earliest: u64,
    pub checkpoint: u64,
    pub events: Vec<Event>,
}

/// Read the retained events and subscription checkpoint in one local transaction.
#[cfg(feature = "sim")]
pub(crate) fn sim_state(
    tx: &Transaction<'_>,
    subscription: &str,
) -> StorageResult<Result<SimState, Error>> {
    as_outcome((|| {
        let head = load_head(tx)?;
        let query = Query::SubscriptionStatus {
            name: subscription.into(),
        };
        let QueryReply::SubscriptionStatus(status) = subscriptions::read(tx, &head, query)? else {
            unreachable!("subscription status query returns subscription status");
        };
        let mut out = Vec::new();
        let mut after = head.first_checkpoint();
        while after < head.tail() {
            let page = events::page(tx, &head, after, MAX_PAGE_EVENTS)?;
            let Some(last) = page.events.last() else {
                break;
            };
            after = last.offset;
            out.extend(page.events);
        }
        Ok(SimState {
            tail: head.tail(),
            earliest: head.earliest_offset,
            checkpoint: status.checkpoint,
            events: out,
        })
    })())
}

#[derive(Serialize, Deserialize)]
struct Head {
    topic_id: Uuid,
    partition: u32,
    next_offset: u64,
    earliest_offset: u64,
    retained_event_bytes: u64,
    last_published_ms: i64,
    retention_cutoff_ms: i64,
    lease_clock_ms: i64,
    subscription_count: u32,
}

impl Head {
    fn tail(&self) -> u64 {
        self.next_offset - 1
    }

    fn first_checkpoint(&self) -> u64 {
        self.earliest_offset - 1
    }

    fn expired(&self) -> Error {
        Error {
            code: ErrorCode::CheckpointExpired,
            message: "checkpoint is behind the retention floor".into(),
            partition: Some(self.partition),
            earliest_offset: Some(self.earliest_offset),
        }
    }
}

fn apply(tx: &mut Transaction<'_>, command: Command) -> Step<Reply> {
    if let Command::Init {
        topic_id,
        partition,
    } = command
    {
        return initialize(tx, topic_id, partition);
    }

    let mut head = load_head(tx)?;
    match command {
        Command::Init { .. } => unreachable!(),
        Command::Publish { now_ms, events } => Ok(Reply::Published(events::publish(
            tx, &mut head, now_ms, events,
        )?)),
        Command::PublishMany { now_ms, batches } => {
            events::publish_many(tx, &mut head, now_ms, batches)
        }
        Command::Retain { cutoff_ms, limit } => events::retain(tx, &mut head, cutoff_ms, limit),
        command => subscriptions::apply(tx, &mut head, command),
    }
}

fn read(tx: &Transaction<'_>, query: Query) -> Step<QueryReply> {
    let head = load_head(tx)?;
    match query {
        Query::Status => Ok(QueryReply::Status(events::status(tx, &head)?)),
        query => subscriptions::read(tx, &head, query),
    }
}

fn initialize(tx: &mut Transaction<'_>, topic_id: Uuid, partition: u32) -> Step<Reply> {
    if let Some(head) = load::<Head>(tx, HEAD_KEY)? {
        if head.topic_id != topic_id || head.partition != partition {
            return Err(Error::new(ErrorCode::Conflict, "partition identity differs").into());
        }
        return Ok(Reply::Done);
    }
    let head = Head {
        topic_id,
        partition,
        next_offset: 1,
        earliest_offset: 1,
        retained_event_bytes: 0,
        last_published_ms: 0,
        retention_cutoff_ms: 0,
        lease_clock_ms: 0,
        subscription_count: 0,
    };
    store(tx, HEAD_KEY, &head)?;
    Ok(Reply::Done)
}

fn load_head(tx: &Transaction<'_>) -> Step<Head> {
    let head: Head = load(tx, HEAD_KEY)?
        .ok_or_else(|| Error::new(ErrorCode::NotFound, "partition has not been initialized"))?;
    if head.earliest_offset == 0 || head.earliest_offset > head.next_offset {
        return Err(anyhow::anyhow!("invalid partition offsets on disk").into());
    }
    Ok(head)
}

fn load<T: DeserializeOwned>(tx: &Transaction<'_>, key: &[u8]) -> StorageResult<Option<T>> {
    tx.get(key)?
        .map(|value| serde_json::from_slice(&value).context("invalid partition state on disk"))
        .transpose()
}

fn store(tx: &mut Transaction<'_>, key: &[u8], value: &impl Serialize) -> StorageResult<()> {
    tx.put(key, &serde_json::to_vec(value)?)
}

fn validate_time(now_ms: i64) -> Result<(), Error> {
    if !(0..=MAX_TIMESTAMP_MS).contains(&now_ms) {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "timestamp is out of range",
        ));
    }
    Ok(())
}

enum Failure {
    Domain(Error),
    Storage(anyhow::Error),
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self::Domain(error)
    }
}

impl From<anyhow::Error> for Failure {
    fn from(error: anyhow::Error) -> Self {
        Self::Storage(error)
    }
}

type Step<T> = Result<T, Failure>;

fn as_outcome<T>(step: Step<T>) -> StorageResult<Result<T, Error>> {
    match step {
        Ok(value) => Ok(Ok(value)),
        Err(Failure::Domain(error)) => Ok(Err(error)),
        Err(Failure::Storage(error)) => Err(error),
    }
}

mod offset {
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub(super) fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

mod optional_offset {
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub(super) fn serialize<S: Serializer>(
        value: &Option<u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => super::offset::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| value.parse().map_err(de::Error::custom))
            .transpose()
    }
}
