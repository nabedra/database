use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use super::{Error, METADATA, partition_id};
use crate::{
    partition,
    raft::{MAX_HOSTED_GROUPS, StateMachine, Transaction},
};

// A placement can put every data group alongside metadata on the same broker.
const MAX_DATA_PARTITIONS: usize = MAX_HOSTED_GROUPS - 1;
const MAX_BROKERS: usize = 64;
const MAX_HISTORY: u64 = 1024;
const STATE: &[u8] = b"catalog/v1/state";
const PENDING: &[u8] = b"catalog/v1/pending";
const TOPIC_PREFIX: &[u8] = b"catalog/v1/topic/";
const TOPIC_ID_PREFIX: &[u8] = b"catalog/v1/topic-id/";
const OPERATION_PREFIX: &[u8] = b"catalog/v1/operation/";
const HISTORY_PREFIX: &[u8] = b"catalog/v1/history/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BrokerSpec {
    pub id: u64,
    pub zone: String,
    pub peer_url: String,
}

impl BrokerSpec {
    pub(super) fn validate(&self, allow_plaintext: bool) -> Result<(), Error> {
        let url = reqwest::Url::parse(&self.peer_url).map_err(|_| Error::Invalid)?;
        if self.id == 0
            || !partition::valid_name(&self.zone)
            || self.peer_url.len() > 512
            || !(url.scheme() == "https" || allow_plaintext && url.scheme() == "http")
            || !url.has_host()
            || url.path() != "/"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Bootstrap {
    pub metadata_voters: BTreeSet<u64>,
    pub brokers: Vec<BrokerSpec>,
}

impl Bootstrap {
    pub(super) fn broker_map(
        &self,
        allow_plaintext: bool,
    ) -> Result<BTreeMap<u64, BrokerSpec>, Error> {
        let mut roster = BTreeMap::new();
        if self.brokers.is_empty() || self.brokers.len() > MAX_BROKERS {
            return Err(Error::Invalid);
        }
        for broker in &self.brokers {
            broker.validate(allow_plaintext)?;
            if roster
                .values()
                .any(|entry: &BrokerSpec| entry.peer_url == broker.peer_url)
                || roster.insert(broker.id, broker.clone()).is_some()
            {
                return Err(Error::Invalid);
            }
        }
        validate_voters(&roster, &self.metadata_voters)?;
        Ok(roster)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Header {
    pub revision: u64,
    pub cluster_id: Uuid,
    max_replication_factor: usize,
    data_partitions: usize,
    pub roster: BTreeMap<u64, BrokerSpec>,
    pub metadata: BTreeSet<u64>,
    pub drained: BTreeSet<u64>,
    pending: Option<Uuid>,
    history: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Placement {
    pub voters: BTreeSet<u64>,
    pub preferred: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Topic {
    pub name: String,
    pub id: Uuid,
    pub retention_seconds: u64,
    pub replication_factor: usize,
    pub partitions: Vec<Placement>,
    pub subscriptions: BTreeMap<String, partition::Start>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Action {
    CreateTopic {
        topic: String,
        partitions: u32,
        replication_factor: usize,
        retention_seconds: u64,
    },
    AddPartitions {
        topic: String,
        partitions: u32,
    },
    CreateSubscription {
        topic: String,
        subscription: String,
        start: partition::Start,
    },
    AddBroker {
        broker: BrokerSpec,
    },
    MovePartition {
        topic: String,
        partition: u32,
        voters: BTreeSet<u64>,
    },
    MoveMetadata {
        voters: BTreeSet<u64>,
    },
    SetReplicationFactor {
        topic: String,
        replication_factor: usize,
    },
    Rebalance {
        topic: String,
    },
    Drain {
        broker_id: u64,
    },
    ResetSubscription {
        topic: String,
        partition: u32,
        subscription: String,
    },
}

impl Action {
    pub fn label(&self) -> &'static str {
        match self {
            Self::CreateTopic { .. } => "create_topic",
            Self::AddPartitions { .. } => "add_partitions",
            Self::CreateSubscription { .. } => "create_subscription",
            Self::AddBroker { .. } => "add_broker",
            Self::MovePartition { .. } => "move_partition",
            Self::MoveMetadata { .. } => "move_metadata",
            Self::SetReplicationFactor { .. } => "set_replication_factor",
            Self::Rebalance { .. } => "rebalance",
            Self::Drain { .. } => "drain",
            Self::ResetSubscription { .. } => "reset_subscription",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub operation_id: Uuid,
    pub action: Action,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Receipt {
    pub operation_id: Uuid,
    #[serde(flatten)]
    pub stage: Stage,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum Stage {
    Pending,
    Completed { revision: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Prior {
    action: Action,
    revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Pending {
    operation: Operation,
    change: Change,
    cursor: usize,
    sub_cursor: usize,
    drain_after: Option<String>,
    step: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Change {
    AddPartitions {
        desired: Topic,
    },
    AddSubscription {
        desired: Topic,
        name: String,
        start: partition::Start,
    },
    Place {
        desired: Topic,
    },
    Metadata {
        voters: BTreeSet<u64>,
    },
    Drain {
        broker_id: u64,
    },
    Reset {
        topic: String,
        partition: u32,
        subscription: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Effect {
    Initialize {
        topic: Uuid,
        partition: u32,
        target: Placement,
    },
    Subscribe {
        topic: Uuid,
        partition: u32,
        voters: BTreeSet<u64>,
        name: String,
        start: partition::Start,
    },
    Place {
        group: String,
        previous: BTreeSet<u64>,
        target: BTreeSet<u64>,
    },
    Reset {
        topic: Uuid,
        partition: u32,
        voters: BTreeSet<u64>,
        name: String,
    },
    Skip,
    Finish,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Work {
    pub id: Uuid,
    pub step: u64,
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Command {
    Bootstrap {
        cluster_id: Uuid,
        config: Bootstrap,
        allow_plaintext: bool,
        restore: bool,
    },
    Submit {
        operation: Operation,
        new_topic_id: Uuid,
        allow_plaintext: bool,
    },
    Advance {
        id: Uuid,
        step: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Query {
    Header,
    Topic { name: String },
    TopicById { id: Uuid },
    Topics { after: Option<String>, limit: usize },
    Operation { id: Uuid },
    Work,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Reply {
    Header(Header),
    Topic(Topic),
    Topics(Vec<Topic>),
    Receipt(Receipt),
    Work(Option<Work>),
    Applied,
}

pub(super) struct Machine;

impl StateMachine for Machine {
    fn apply(&self, tx: &mut Transaction<'_>, command: Value) -> Result<Value> {
        Ok(serde_json::to_value(apply(
            tx,
            serde_json::from_value(command)?,
        )?)?)
    }
}

fn apply(tx: &mut Transaction<'_>, command: Command) -> Result<Result<Reply, Error>> {
    let result = match command {
        Command::Bootstrap {
            cluster_id,
            config,
            allow_plaintext,
            restore,
        } => bootstrap(tx, cluster_id, config, allow_plaintext, restore),
        Command::Submit {
            operation,
            new_topic_id,
            allow_plaintext,
        } => submit(tx, operation, new_topic_id, allow_plaintext),
        Command::Advance { id, step } => advance(tx, id, step),
    };
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(StepError::Domain(error)) => Ok(Err(error)),
        Err(StepError::Storage(error)) => Err(error),
    }
}

pub(super) fn query(tx: &Transaction<'_>, query: Query) -> Result<Result<Reply, Error>> {
    let result = match query {
        Query::Header => load::<Header>(tx, STATE)?
            .map(Reply::Header)
            .ok_or(Error::Unavailable),
        Query::Topic { name } => load::<Topic>(tx, &topic_key(&name))?
            .map(Reply::Topic)
            .ok_or(Error::NotFound),
        Query::TopicById { id } => match load::<String>(tx, &topic_id_key(id))? {
            Some(name) => load::<Topic>(tx, &topic_key(&name))?
                .map(Reply::Topic)
                .ok_or(Error::Internal),
            None => Err(Error::NotFound),
        },
        Query::Topics { after, limit } => Ok(Reply::Topics(topics(tx, after.as_deref(), limit)?)),
        Query::Operation { id } => receipt(tx, id)?.map(Reply::Receipt).ok_or(Error::NotFound),
        Query::Work => match (load::<Header>(tx, STATE)?, load::<Pending>(tx, PENDING)?) {
            (Some(header), Some(pending)) => Ok(Reply::Work(Some(work(tx, &header, &pending)?))),
            (Some(_), None) => Ok(Reply::Work(None)),
            (None, _) => Err(Error::Unavailable),
        },
    };
    Ok(result)
}

fn bootstrap(
    tx: &mut Transaction<'_>,
    cluster_id: Uuid,
    config: Bootstrap,
    allow_plaintext: bool,
    restore: bool,
) -> Step<Reply> {
    let roster = config.broker_map(allow_plaintext)?;
    if let Some(mut header) = load::<Header>(tx, STATE)? {
        if header.cluster_id != cluster_id {
            if !restore
                || header.pending.is_some()
                || !header.roster.keys().eq(roster.keys())
                || config.metadata_voters != header.metadata
                || roster
                    .iter()
                    .any(|(id, broker)| broker.zone != header.roster[id].zone)
            {
                return Err(Error::Conflict.into());
            }
            header.roster = roster;
            header.cluster_id = cluster_id;
            bump(&mut header)?;
            save(tx, STATE, &header)?;
        }
        return Ok(Reply::Header(header));
    }
    let header = Header {
        revision: 1,
        cluster_id,
        max_replication_factor: config.metadata_voters.len(),
        data_partitions: 0,
        roster,
        metadata: config.metadata_voters,
        drained: BTreeSet::new(),
        pending: None,
        history: 0,
    };
    save(tx, STATE, &header)?;
    Ok(Reply::Header(header))
}

fn submit(
    tx: &mut Transaction<'_>,
    operation: Operation,
    new_topic_id: Uuid,
    allow_plaintext: bool,
) -> Step<Reply> {
    if operation.operation_id.is_nil() {
        return Err(Error::Invalid.into());
    }
    let mut header = load::<Header>(tx, STATE)?.ok_or(Error::Unavailable)?;
    if let Some(prior) = load::<Prior>(tx, &op_key(operation.operation_id))? {
        if prior.action != operation.action {
            return Err(Error::Conflict.into());
        }
        return Ok(Reply::Receipt(Receipt {
            operation_id: operation.operation_id,
            stage: Stage::Completed {
                revision: prior.revision,
            },
        }));
    }
    if let Some(pending) = load::<Pending>(tx, PENDING)? {
        if pending.operation.operation_id != operation.operation_id {
            return Err(Error::Busy.into());
        }
        if pending.operation.action != operation.action {
            return Err(Error::Conflict.into());
        }
        return Ok(Reply::Receipt(Receipt {
            operation_id: operation.operation_id,
            stage: Stage::Pending,
        }));
    }
    let Some((change, cursor)) = plan(tx, &mut header, &operation, new_topic_id, allow_plaintext)?
    else {
        return complete(tx, &mut header, operation);
    };
    if let Change::AddPartitions { desired } = &change {
        let additional = desired.partitions.len() - cursor;
        let reserved = header
            .data_partitions
            .checked_add(additional)
            .context("catalog partition count overflow")?;
        if reserved > MAX_DATA_PARTITIONS {
            return Err(Error::PartitionLimit.into());
        }
        header.data_partitions = reserved;
    }
    header.pending = Some(operation.operation_id);
    save(tx, STATE, &header)?;
    save(
        tx,
        PENDING,
        &Pending {
            operation: operation.clone(),
            change,
            cursor,
            sub_cursor: 0,
            drain_after: None,
            step: 0,
        },
    )?;
    Ok(Reply::Receipt(Receipt {
        operation_id: operation.operation_id,
        stage: Stage::Pending,
    }))
}

fn plan(
    tx: &Transaction<'_>,
    header: &mut Header,
    operation: &Operation,
    new_topic_id: Uuid,
    allow_plaintext: bool,
) -> Step<Option<(Change, usize)>> {
    let change = match &operation.action {
        Action::CreateTopic {
            topic,
            partitions,
            replication_factor,
            retention_seconds,
        } => {
            valid_name(topic)?;
            validate_partition_count(*partitions)?;
            validate_replication_factor(*replication_factor)?;
            if !(1..=315_576_000).contains(retention_seconds) {
                return Err(Error::Invalid.into());
            }
            if load::<Topic>(tx, &topic_key(topic))?.is_some() {
                return Err(Error::Conflict.into());
            }
            if new_topic_id.is_nil() || load::<String>(tx, &topic_id_key(new_topic_id))?.is_some() {
                return Err(Error::Conflict.into());
            }
            let desired = Topic {
                name: topic.clone(),
                id: new_topic_id,
                retention_seconds: *retention_seconds,
                replication_factor: *replication_factor,
                partitions: assign(header, 0, *partitions, *replication_factor)?,
                subscriptions: BTreeMap::new(),
            };
            Change::AddPartitions { desired }
        }
        Action::AddPartitions { topic, partitions } => {
            validate_partition_count(*partitions)?;
            let mut desired = get_topic(tx, topic)?;
            let current = desired.partitions.len();
            if (*partitions as usize) < current {
                return Err(Error::Conflict.into());
            }
            if *partitions as usize == current {
                return Ok(None);
            }
            desired.partitions.extend(assign(
                header,
                current as u32,
                *partitions,
                desired.replication_factor,
            )?);
            return Ok(Some((Change::AddPartitions { desired }, current)));
        }
        Action::CreateSubscription {
            topic,
            subscription,
            start,
        } => {
            valid_name(subscription)?;
            let mut desired = get_topic(tx, topic)?;
            if let Some(previous) = desired.subscriptions.get(subscription) {
                return if previous == start {
                    Ok(None)
                } else {
                    Err(Error::Conflict.into())
                };
            }
            if desired.subscriptions.len() >= partition::MAX_SUBSCRIPTIONS_PER_PARTITION as usize {
                return Err(Error::Capacity.into());
            }
            desired.subscriptions.insert(subscription.clone(), *start);
            Change::AddSubscription {
                desired,
                name: subscription.clone(),
                start: *start,
            }
        }
        Action::AddBroker { broker } => {
            broker.validate(allow_plaintext)?;
            if let Some(previous) = header.roster.get(&broker.id) {
                return if previous == broker && !header.drained.contains(&broker.id) {
                    Ok(None)
                } else {
                    Err(Error::Conflict.into())
                };
            }
            if header.roster.len() >= MAX_BROKERS {
                return Err(Error::Capacity.into());
            }
            if header
                .roster
                .values()
                .any(|entry| entry.peer_url == broker.peer_url)
            {
                return Err(Error::Conflict.into());
            }
            header.roster.insert(broker.id, broker.clone());
            return Ok(None);
        }
        Action::MovePartition {
            topic,
            partition,
            voters,
        } => {
            validate_active_voters(header, voters)?;
            let mut desired = get_topic(tx, topic)?;
            if voters.len() != desired.replication_factor {
                return Err(Error::Conflict.into());
            }
            let target = desired
                .partitions
                .get_mut(*partition as usize)
                .ok_or(Error::NotFound)?;
            if &target.voters == voters {
                return Ok(None);
            }
            target.preferred = *voters.first().context("validated voters")?;
            target.voters = voters.clone();
            return Ok(Some((Change::Place { desired }, *partition as usize)));
        }
        Action::MoveMetadata { voters } => {
            validate_active_voters(header, voters)?;
            if voters.len() < header.metadata.len() {
                return Err(Error::Conflict.into());
            }
            Change::Metadata {
                voters: voters.clone(),
            }
        }
        Action::SetReplicationFactor {
            topic,
            replication_factor,
        } => {
            validate_replication_factor(*replication_factor)?;
            let mut desired = get_topic(tx, topic)?;
            if *replication_factor < desired.replication_factor {
                return Err(Error::Conflict.into());
            }
            if *replication_factor == desired.replication_factor {
                return Ok(None);
            }
            desired.replication_factor = *replication_factor;
            desired.partitions = assign(
                header,
                0,
                desired.partitions.len() as u32,
                *replication_factor,
            )?;
            Change::Place { desired }
        }
        Action::Rebalance { topic } => {
            let mut desired = get_topic(tx, topic)?;
            desired.partitions = assign(
                header,
                0,
                desired.partitions.len() as u32,
                desired.replication_factor,
            )?;
            Change::Place { desired }
        }
        Action::Drain { broker_id } => {
            if !header.roster.contains_key(broker_id) {
                return Err(Error::NotFound.into());
            }
            if header.drained.contains(broker_id) {
                return Ok(None);
            }
            let eligible = header
                .roster
                .values()
                .filter(|broker| broker.id != *broker_id && !header.drained.contains(&broker.id))
                .map(|broker| &broker.zone)
                .collect::<BTreeSet<_>>();
            // Leave enough independent zones to replace even an RF3 placement.
            if eligible.len() < header.max_replication_factor {
                return Err(Error::Capacity.into());
            }
            Change::Drain {
                broker_id: *broker_id,
            }
        }
        Action::ResetSubscription {
            topic,
            partition,
            subscription,
        } => {
            let current = get_topic(tx, topic)?;
            if current.partitions.get(*partition as usize).is_none()
                || !current.subscriptions.contains_key(subscription)
            {
                return Err(Error::NotFound.into());
            }
            Change::Reset {
                topic: topic.clone(),
                partition: *partition,
                subscription: subscription.clone(),
            }
        }
    };
    Ok(Some((change, 0)))
}

fn work(tx: &Transaction<'_>, header: &Header, pending: &Pending) -> Result<Work> {
    let cursor = pending.cursor as u32;
    let effect = match &pending.change {
        Change::AddPartitions { desired } => match desired.partitions.get(pending.cursor) {
            Some(target) if pending.sub_cursor == 0 => Effect::Initialize {
                topic: desired.id,
                partition: cursor,
                target: target.clone(),
            },
            Some(target) => {
                let (name, start) = desired
                    .subscriptions
                    .iter()
                    .nth(pending.sub_cursor - 1)
                    .context("invalid add-partition subscription cursor")?;
                Effect::Subscribe {
                    topic: desired.id,
                    partition: cursor,
                    voters: target.voters.clone(),
                    name: name.clone(),
                    start: *start,
                }
            }
            None => Effect::Finish,
        },
        Change::AddSubscription {
            desired,
            name,
            start,
        } => match desired.partitions.get(pending.cursor) {
            Some(placement) => Effect::Subscribe {
                topic: desired.id,
                partition: cursor,
                voters: placement.voters.clone(),
                name: name.clone(),
                start: *start,
            },
            None => Effect::Finish,
        },
        Change::Place { desired } => match desired.partitions.get(pending.cursor) {
            Some(placement) => {
                let current: Topic = load(tx, &topic_key(&desired.name))?
                    .context("missing topic during placement")?;
                placement_effect(
                    partition_id(desired.id, cursor),
                    &current.partitions[pending.cursor].voters,
                    &placement.voters,
                )
            }
            None => Effect::Finish,
        },
        Change::Metadata { voters } if pending.cursor == 0 => {
            placement_effect(METADATA.to_string(), &header.metadata, voters)
        }
        Change::Metadata { .. } => Effect::Finish,
        Change::Reset {
            topic,
            partition,
            subscription,
        } if pending.cursor == 0 => {
            let current: Topic =
                load(tx, &topic_key(topic))?.context("missing topic during reset")?;
            Effect::Reset {
                topic: current.id,
                partition: *partition,
                voters: current.partitions[*partition as usize].voters.clone(),
                name: subscription.clone(),
            }
        }
        Change::Reset { .. } => Effect::Finish,
        Change::Drain { broker_id } => drain_effect(tx, header, pending, *broker_id)?,
    };
    Ok(Work {
        id: pending.operation.operation_id,
        step: pending.step,
        effect,
    })
}

fn drain_effect(
    tx: &Transaction<'_>,
    header: &Header,
    pending: &Pending,
    broker_id: u64,
) -> Result<Effect> {
    match next_drain_topic(tx, pending)? {
        Some(topic) => {
            let Some(placement) = topic.partitions.get(pending.cursor) else {
                return Ok(Effect::Skip);
            };
            let target = replacement(header, &placement.voters, broker_id, pending.cursor)
                .map_err(|error| anyhow::anyhow!("invalid drain target: {error:?}"))?;
            Ok(placement_effect(
                partition_id(topic.id, pending.cursor as u32),
                &placement.voters,
                &target,
            ))
        }
        None if header.metadata.contains(&broker_id) => {
            let target = replacement(header, &header.metadata, broker_id, 0)
                .map_err(|error| anyhow::anyhow!("invalid metadata drain target: {error:?}"))?;
            Ok(placement_effect(
                METADATA.to_string(),
                &header.metadata,
                &target,
            ))
        }
        None => Ok(Effect::Finish),
    }
}

fn placement_effect(group: String, previous: &BTreeSet<u64>, target: &BTreeSet<u64>) -> Effect {
    if previous == target {
        Effect::Skip
    } else {
        Effect::Place {
            group,
            previous: previous.clone(),
            target: target.clone(),
        }
    }
}

fn next_drain_topic(tx: &Transaction<'_>, pending: &Pending) -> Result<Option<Topic>> {
    Ok(topics(tx, pending.drain_after.as_deref(), 1)?
        .into_iter()
        .next())
}

fn advance(tx: &mut Transaction<'_>, id: Uuid, step: u64) -> Step<Reply> {
    let mut header = load::<Header>(tx, STATE)?.ok_or(Error::Unavailable)?;
    let Some(mut pending) = load::<Pending>(tx, PENDING)? else {
        return Ok(Reply::Applied);
    };
    if pending.operation.operation_id != id || pending.step != step {
        return Ok(Reply::Applied);
    }
    // Recompute the effect from committed catalog state. An advance for an old
    // controller incarnation can only complete its exact persisted step.
    let effect = work(tx, &header, &pending)?.effect;
    if matches!(effect, Effect::Finish) {
        match pending.change {
            Change::AddPartitions { desired }
            | Change::AddSubscription { desired, .. }
            | Change::Place { desired } => {
                save(tx, &topic_key(&desired.name), &desired)?;
                save(tx, &topic_id_key(desired.id), &desired.name)?;
                header.max_replication_factor = header
                    .max_replication_factor
                    .max(desired.replication_factor);
            }
            Change::Drain { broker_id } => {
                header.drained.insert(broker_id);
            }
            Change::Metadata { .. } | Change::Reset { .. } => {}
        }
        return complete(tx, &mut header, pending.operation);
    }

    match &pending.change {
        Change::AddPartitions { desired } => {
            if pending.sub_cursor < desired.subscriptions.len() {
                pending.sub_cursor += 1;
            } else {
                pending.sub_cursor = 0;
                pending.cursor += 1;
            }
        }
        Change::AddSubscription { .. } | Change::Reset { .. } => pending.cursor += 1,
        Change::Place { desired } => {
            let mut current = get_topic(tx, &desired.name)?;
            current.partitions[pending.cursor] = desired.partitions[pending.cursor].clone();
            save(tx, &topic_key(&current.name), &current)?;
            bump(&mut header)?;
            pending.cursor += 1;
        }
        Change::Metadata { voters } => {
            header.metadata = voters.clone();
            header.max_replication_factor = header.max_replication_factor.max(voters.len());
            bump(&mut header)?;
            pending.cursor += 1;
        }
        Change::Drain { .. } => {
            if let Some(mut topic) = next_drain_topic(tx, &pending)? {
                if pending.cursor < topic.partitions.len() {
                    if let Effect::Place { target, .. } = effect {
                        let placement = &mut topic.partitions[pending.cursor];
                        if !target.contains(&placement.preferred) {
                            placement.preferred = *target.first().context("validated drain set")?;
                        }
                        placement.voters = target;
                        save(tx, &topic_key(&topic.name), &topic)?;
                        bump(&mut header)?;
                    }
                    pending.cursor += 1;
                } else {
                    pending.drain_after = Some(topic.name);
                    pending.cursor = 0;
                }
            } else if let Effect::Place { target, .. } = effect {
                header.metadata = target;
                bump(&mut header)?;
            }
        }
    }
    pending.step = pending
        .step
        .checked_add(1)
        .context("operation step overflow")?;
    save(tx, STATE, &header)?;
    save(tx, PENDING, &pending)?;
    Ok(Reply::Applied)
}

fn complete(tx: &mut Transaction<'_>, header: &mut Header, operation: Operation) -> Step<Reply> {
    bump(header)?;
    header.pending = None;
    header.history = header
        .history
        .checked_add(1)
        .context("operation history overflow")?;
    if let Some(expired) = header.history.checked_sub(MAX_HISTORY).filter(|n| *n > 0) {
        let key = history_key(expired);
        if let Some(old) = load::<Uuid>(tx, &key)? {
            tx.delete(&op_key(old))?;
            tx.delete(&key)?;
        }
    }
    let id = operation.operation_id;
    save(tx, &history_key(header.history), &id)?;
    save(
        tx,
        &op_key(id),
        &Prior {
            action: operation.action,
            revision: header.revision,
        },
    )?;
    save(tx, STATE, header)?;
    tx.delete(PENDING)?;
    Ok(Reply::Receipt(Receipt {
        operation_id: id,
        stage: Stage::Completed {
            revision: header.revision,
        },
    }))
}

fn receipt(tx: &Transaction<'_>, id: Uuid) -> Result<Option<Receipt>> {
    if let Some(prior) = load::<Prior>(tx, &op_key(id))? {
        return Ok(Some(Receipt {
            operation_id: id,
            stage: Stage::Completed {
                revision: prior.revision,
            },
        }));
    }
    let pending = load::<Header>(tx, STATE)?.and_then(|header| header.pending);
    Ok((pending == Some(id)).then_some(Receipt {
        operation_id: id,
        stage: Stage::Pending,
    }))
}

pub(super) fn metadata_is_quiet(header: &Header) -> bool {
    header.pending.is_none()
}

fn valid_name(name: &str) -> Step<()> {
    if !partition::valid_name(name) {
        return Err(Error::Invalid.into());
    }
    Ok(())
}

fn validate_partition_count(count: u32) -> Step<()> {
    if count == 0 {
        return Err(Error::Invalid.into());
    }
    if count as usize > MAX_DATA_PARTITIONS {
        return Err(Error::PartitionLimit.into());
    }
    Ok(())
}

fn validate_replication_factor(factor: usize) -> Result<(), Error> {
    if !matches!(factor, 1 | 3) {
        return Err(Error::Invalid);
    }
    Ok(())
}

fn validate_active_voters(header: &Header, voters: &BTreeSet<u64>) -> Result<(), Error> {
    validate_voters(&header.roster, voters)?;
    if voters.iter().any(|id| header.drained.contains(id)) {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn validate_voters(
    roster: &BTreeMap<u64, BrokerSpec>,
    voters: &BTreeSet<u64>,
) -> Result<(), Error> {
    validate_replication_factor(voters.len())?;
    let mut zones = BTreeSet::new();
    for id in voters {
        let broker = roster.get(id).ok_or(Error::NotFound)?;
        if !zones.insert(&broker.zone) {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

fn assign(header: &Header, start: u32, end: u32, factor: usize) -> Result<Vec<Placement>, Error> {
    let candidates: Vec<_> = header
        .roster
        .values()
        .filter(|broker| !header.drained.contains(&broker.id))
        .collect();
    if candidates.is_empty() {
        return Err(Error::Capacity);
    }
    (start..end)
        .map(|index| {
            let rotated = candidates
                .iter()
                .cycle()
                .skip(index as usize % candidates.len())
                .take(candidates.len());
            let preferred = candidates[index as usize % candidates.len()].id;
            let mut zones = BTreeSet::new();
            let voters = rotated
                .filter(|broker| zones.insert(&broker.zone))
                .take(factor)
                .map(|broker| broker.id)
                .collect::<BTreeSet<_>>();
            if voters.len() != factor {
                return Err(Error::Capacity);
            }
            Ok(Placement { voters, preferred })
        })
        .collect()
}

fn replacement(
    header: &Header,
    previous: &BTreeSet<u64>,
    removed: u64,
    index: usize,
) -> Result<BTreeSet<u64>, Error> {
    if !previous.contains(&removed) {
        return Ok(previous.clone());
    }
    let mut target = previous.clone();
    target.remove(&removed);
    let candidates = header
        .roster
        .values()
        .filter(|broker| {
            broker.id != removed
                && !header.drained.contains(&broker.id)
                && !target
                    .iter()
                    .any(|id| header.roster[id].zone == broker.zone)
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(Error::Capacity);
    }
    target.insert(candidates[index % candidates.len()].id);
    Ok(target)
}

fn bump(header: &mut Header) -> Result<()> {
    header.revision = header
        .revision
        .checked_add(1)
        .context("catalog revision overflow")?;
    Ok(())
}

fn get_topic(tx: &Transaction<'_>, name: &str) -> Step<Topic> {
    valid_name(name)?;
    load::<Topic>(tx, &topic_key(name))?.ok_or_else(|| Error::NotFound.into())
}

fn topics(tx: &Transaction<'_>, after: Option<&str>, limit: usize) -> Result<Vec<Topic>> {
    let cursor = after.map(topic_key);
    tx.scan(TOPIC_PREFIX, cursor.as_deref(), limit.clamp(1, 8))?
        .into_iter()
        .map(|(_, value)| Ok(serde_json::from_slice(&value)?))
        .collect()
}

fn topic_key(name: &str) -> Vec<u8> {
    [TOPIC_PREFIX, name.as_bytes()].concat()
}
fn topic_id_key(id: Uuid) -> Vec<u8> {
    [TOPIC_ID_PREFIX, id.as_bytes()].concat()
}
fn op_key(id: Uuid) -> Vec<u8> {
    [OPERATION_PREFIX, id.as_bytes()].concat()
}
fn history_key(sequence: u64) -> Vec<u8> {
    [HISTORY_PREFIX, &sequence.to_be_bytes()].concat()
}

fn load<T: DeserializeOwned>(tx: &Transaction<'_>, key: &[u8]) -> Result<Option<T>> {
    tx.get(key)?
        .map(|value| serde_json::from_slice(&value).map_err(Into::into))
        .transpose()
}

fn save<T: Serialize>(tx: &mut Transaction<'_>, key: &[u8], value: &T) -> Result<()> {
    tx.put(key, &serde_json::to_vec(value)?)
}

type Step<T> = Result<T, StepError>;

enum StepError {
    Domain(Error),
    Storage(anyhow::Error),
}

impl From<Error> for StepError {
    fn from(error: Error) -> Self {
        Self::Domain(error)
    }
}
impl From<anyhow::Error> for StepError {
    fn from(error: anyhow::Error) -> Self {
        Self::Storage(error)
    }
}
