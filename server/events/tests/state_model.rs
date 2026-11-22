use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Result;
use nabedra_check::{r#gen::Gen, prop};
use nabedra_events::{
    partition::{self, Command, Mutation, PublishEvent, Query, QueryReply, Reply, Start},
    raft::{Broker, BrokerConfig, Group, StateMachine, Transaction},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_value};
use uuid::Uuid;

#[path = "state_model/reference.rs"]
mod reference;

use reference::Model;

const TOPIC: Uuid = Uuid::from_u128(42);
const NAME: &str = "replay";
const OWNER: &str = "test/consumer";
const PARTITION_PREFIX: &[u8] = b"partition/v1/";

#[derive(Clone, Debug, Deserialize, Serialize)]
enum Step {
    Write(Command),
    Read(Query),
}

impl From<Command> for Step {
    fn from(command: Command) -> Self {
        Self::Write(command)
    }
}

impl From<Query> for Step {
    fn from(query: Query) -> Self {
        Self::Read(query)
    }
}

fn init() -> Vec<Step> {
    vec![
        Command::Init {
            topic_id: TOPIC,
            partition: 0,
        }
        .into(),
        Command::OpenSubscription {
            name: NAME.into(),
            start: Start::Earliest,
        }
        .into(),
    ]
}

fn input(id: u64, data: impl Into<Value>) -> PublishEvent {
    PublishEvent {
        event_id: Uuid::from_u128(id.into()),
        data: data.into(),
    }
}

fn publish(time: i64, events: Vec<PublishEvent>) -> Step {
    Command::Publish {
        now_ms: time,
        events,
    }
    .into()
}

fn claim(time: i64, id: u64, ttl_ms: i64, owner: &str) -> Step {
    Command::Claim {
        name: NAME.into(),
        owner: owner.into(),
        claim_id: Uuid::from_u128(id.into()),
        now_ms: time,
        ttl_ms,
    }
    .into()
}

fn renew(time: i64, generation: u64, ttl_ms: i64) -> Step {
    Command::Renew {
        name: NAME.into(),
        owner: OWNER.into(),
        generation,
        now_ms: time,
        ttl_ms,
    }
    .into()
}

fn ack(time: i64, generation: u64, after: u64, last: u64, through: u64) -> Step {
    Command::Ack {
        name: NAME.into(),
        owner: OWNER.into(),
        generation,
        after,
        last,
        through,
        now_ms: time,
    }
    .into()
}

fn poll(time: i64, generation: u64, limit: usize) -> Step {
    Query::Poll {
        name: NAME.into(),
        owner: OWNER.into(),
        generation,
        now_ms: time,
        limit,
    }
    .into()
}

fn subscription_status() -> Step {
    Query::SubscriptionStatus { name: NAME.into() }.into()
}

fn retain(cutoff_ms: i64, limit: usize) -> Step {
    Command::Retain { cutoff_ms, limit }.into()
}

// The same witnesses run against the unmodified machine and the test-only faults.
fn stale_ack_trace(time: i64) -> Vec<Step> {
    let mut steps = init();
    steps.extend([
        publish(
            time,
            vec![
                input(1, 1),
                input(2, 2),
                input(3, json!({"published_at": "2100-01-01T00:00:00Z"})),
            ],
        ),
        claim(time, 1, 10, OWNER),
        claim(time + 1, 1, 10, OWNER),
        poll(time + 2, 1, 3),
        ack(time + 2, 1, 0, 3, 1),
        ack(time + 2, 1, 0, 3, 1),
        renew(time + 3, 1, 10),
        poll(time + 4, 1, 3),
        claim(time + 13, 2, 10, OWNER),
        ack(time + 12, 1, 1, 3, 2),
        subscription_status(),
        poll(time + 13, 2, 3),
    ]);
    steps
}

fn rejected_batch_trace(time: i64) -> Vec<Step> {
    let mut steps = init();
    steps.extend([
        publish(time, vec![input(1, 1), input(2, 2)]),
        publish(time - 1, vec![input(2, 2), input(3, 3), input(1, 1)]),
        publish(time, vec![input(4, 4), input(1, 99)]),
    ]);
    steps
}

fn generated_trace(generator: &mut Gen) -> Vec<Step> {
    let start = generator.range_i64(1_000, 60_000);
    let mut steps = stale_ack_trace(start);
    steps.extend([
        Command::PublishMany {
            now_ms: start + 1,
            batches: vec![
                vec![input(2, 2), input(4, 4)],
                vec![input(5, 5), input(1, 99)],
                vec![input(6, json!({"published_at": "1970-01-01T00:00:00Z"}))],
            ],
        }
        .into(),
        retain(start + 1, 1),
        retain(start, 100),
        poll(start + 14, 2, 2),
        Command::ResetExpired { name: NAME.into() }.into(),
        claim(start + 14, 3, 100, OWNER),
        poll(start + 14, 3, 2),
        // A renewal at the expiry observes it. The backward poll must not revive it.
        renew(start + 114, 3, 100),
        poll(start + 15, 3, 2),
        publish(start - 2, vec![input(1, 11)]),
    ]);

    let mut model = Model::default();
    for step in &steps {
        if let Step::Write(command) = step {
            let _ = model.apply(command);
        }
    }
    let mut time = start + 15;
    for _ in 0..(20 + generator.below(32)) {
        time += generator.range_i64(0, 8);
        let now = time + generator.range_i64(-10, 2);
        let generation = model.generation();
        let generation = if generator.chance(1, 4) {
            generation.saturating_sub(1)
        } else {
            generation
        };
        let step = match generator.below(11) {
            0..=2 => {
                let events = (0..generator.below(5))
                    .map(|_| {
                        let id = generator.below(25);
                        let data = if generator.chance(1, 5) {
                            json!({"changed": id})
                        } else {
                            json!(id)
                        };
                        input(id, data)
                    })
                    .collect();
                publish(now, events)
            }
            3 => claim(
                now,
                generator.below(12),
                generator.range_i64(1, 30),
                if generator.chance(1, 4) {
                    "test/standby"
                } else {
                    OWNER
                },
            ),
            4 => renew(
                now,
                generation,
                *generator.pick(&[0, 1, 15, 300_000, 300_001]),
            ),
            5 => {
                let after = model.checkpoint;
                let last = (after + generator.below(4) + 1).min(model.tail());
                let through = if generator.chance(1, 4) {
                    last + 1
                } else {
                    (after + 1).min(last)
                };
                ack(now, generation, after, last, through)
            }
            6 => poll(now, generation, generator.below(6) as usize + 1),
            7 => subscription_status(),
            8 => retain(
                time + generator.range_i64(-15, 8),
                generator.below(4) as usize + 1,
            ),
            9 => Command::ResetExpired { name: NAME.into() }.into(),
            _ => Query::Status.into(),
        };
        if let Step::Write(command) = &step {
            let _ = model.apply(command);
        }
        steps.push(step);
    }
    steps.extend([Query::Status.into(), subscription_status()]);
    steps
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    AcceptStaleAck,
    HalfApplyBatch,
}

struct TraceMachine(Fault);

impl StateMachine for TraceMachine {
    fn apply(&self, tx: &mut Transaction<'_>, trace: Value) -> Result<Value> {
        // Each generated sequence starts with an empty partition. All the steps
        // use the real machine and see earlier writes in the same transaction.
        for (key, _) in tx.scan(PARTITION_PREFIX, None, 4096)? {
            tx.delete(&key)?;
        }
        let machine: &dyn StateMachine = &partition::Machine;
        let steps: Vec<Step> = serde_json::from_value(trace)?;
        let mut replies = Vec::with_capacity(steps.len());
        for step in steps {
            let reply = match step {
                Step::Read(query) => to_value(partition::query(tx, query)?)?,
                Step::Write(mut command) => {
                    if matches!(self.0, Fault::AcceptStaleAck)
                        && let Command::Ack {
                            name, generation, ..
                        } = &mut command
                        && let Ok(QueryReply::SubscriptionStatus(status)) =
                            partition::query(tx, Query::SubscriptionStatus { name: name.clone() })?
                        && let Some(current) = status.lease_generation
                    {
                        *generation = current;
                    }
                    let reply = machine.apply(tx, to_value(&command)?)?;
                    if matches!(self.0, Fault::HalfApplyBatch)
                        && reply.get("Err").is_some()
                        && let Command::Publish { now_ms, mut events } = command
                        && events.len() > 1
                    {
                        events.truncate(1);
                        let partial =
                            machine.apply(tx, to_value(Command::Publish { now_ms, events })?)?;
                        anyhow::ensure!(partial.get("Ok").is_some(), "fault prefix must commit");
                    }
                    reply
                }
            };
            replies.push(reply);
        }
        Ok(to_value(replies)?)
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    broker: Arc<Broker>,
    group: Arc<Group>,
}

impl Fixture {
    async fn new(fault: Fault) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let broker = Broker::open(BrokerConfig {
            id: 1,
            cluster_id: Uuid::from_u128(15),
            data_dir: dir.path().into(),
            peer_token: "fixture-only-peer-token-with-32-bytes".into(),
            peer_ca: None,
            peer_identity: None,
            allow_plaintext: true,
        })?;
        let group = broker
            .open_group("model", Arc::new(TraceMachine(fault)))
            .await?;
        group
            .initialize(BTreeMap::from([(1, "http://127.0.0.1:9".into())]))
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while group.leader() != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(Self {
            _dir: dir,
            broker,
            group,
        })
    }

    async fn close(self) -> Result<()> {
        self.broker.close_group(self.group.id()).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum Divergence {
    Reply {
        step: String,
        expected: Value,
        observed: Value,
    },
    State {
        key: Vec<u8>,
        expected: Option<Value>,
        observed: Option<Value>,
    },
}

async fn compare(group: &Group, trace: &[Step]) -> Result<Option<Divergence>> {
    let observed: Vec<Value> = serde_json::from_value(group.write(to_value(trace)?).await?)?;
    anyhow::ensure!(observed.len() == trace.len(), "wrong number of replies");
    let mut model = Model::default();
    for (index, (step, observed)) in trace.iter().zip(observed).enumerate() {
        let expected = model.replay(step);
        if expected != observed {
            return Ok(Some(Divergence::Reply {
                step: format!("{index} {step:?}"),
                expected,
                observed,
            }));
        }
    }

    // Read through a separate committed snapshot to spot silent changes, including
    // a rejected batch that left an event/dedup record unreachable from the head.
    let observed = group
        .read(|tx| {
            tx.scan(PARTITION_PREFIX, None, 4096)?
                .into_iter()
                .map(|(key, value)| Ok((key, serde_json::from_slice(&value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
        })
        .await?;
    let mut expected = model.records();
    for (key, observed) in observed {
        let expected = expected.remove(&key);
        if expected.as_ref() != Some(&observed) {
            return Ok(Some(Divergence::State {
                key,
                expected,
                observed: Some(observed),
            }));
        }
    }
    if let Some((key, expected)) = expected.into_iter().next() {
        return Ok(Some(Divergence::State {
            key,
            expected: Some(expected),
            observed: None,
        }));
    }
    Ok(None)
}

#[test]
fn generated_state_machine_matches_reference() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let fixture = runtime.block_on(Fixture::new(Fault::None))?;
    let mut config = prop::Config::default();
    if std::env::var_os("NABEDRA_CASES").is_none() {
        // Keep the default T0 check short, including durable writes and reads.
        config.cases = 48;
    }
    prop::check_with(
        config,
        "EVT-G02–G05/EVT-G08 Events state machine",
        |generator| {
            let trace = generated_trace(generator);
            match runtime.block_on(compare(&fixture.group, &trace)) {
                Ok(None) => Ok(()),
                Ok(Some(divergence)) => Err(format!("{divergence:?}")),
                Err(error) => Err(format!("{error:#}")),
            }
        },
    );
    runtime.block_on(fixture.close())
}

#[tokio::test]
async fn catches_ack_accepted_from_a_stale_generation() -> Result<()> {
    let trace = stale_ack_trace(1_000);
    let healthy = Fixture::new(Fault::None).await?;
    assert_eq!(compare(&healthy.group, &trace).await?, None);
    healthy.close().await?;

    let faulty = Fixture::new(Fault::AcceptStaleAck).await?;
    let Some(Divergence::Reply {
        step,
        expected,
        observed,
    }) = compare(&faulty.group, &trace).await?
    else {
        panic!("the model accepted a stale-generation ack");
    };
    assert!(
        step.contains("Ack") && step.contains("generation: 1"),
        "{step}"
    );
    assert_eq!(expected["Err"]["code"], "lease_lost");
    assert_eq!(
        observed,
        json!({"Ok": {"type": "checkpoint", "offset": "2"}})
    );
    faulty.close().await
}

#[tokio::test]
async fn catches_rejected_batch_that_partially_applied() -> Result<()> {
    let trace = rejected_batch_trace(1_000);
    let healthy = Fixture::new(Fault::None).await?;
    assert_eq!(compare(&healthy.group, &trace).await?, None);
    healthy.close().await?;

    let faulty = Fixture::new(Fault::HalfApplyBatch).await?;
    let Some(Divergence::State {
        key,
        expected,
        observed,
    }) = compare(&faulty.group, &trace).await?
    else {
        panic!("the model accepted a partially applied batch");
    };
    assert_eq!(
        key,
        [
            b"partition/v1/dedup/".as_slice(),
            Uuid::from_u128(4).as_bytes()
        ]
        .concat()
    );
    assert_eq!(expected, None);
    assert_eq!(observed, Some(json!(4)));
    faulty.close().await
}

#[tokio::test]
async fn partition_rejects_the_4097th_subscription() -> Result<()> {
    let fixture = Fixture::new(Fault::None).await?;
    let open = |index| {
        Step::from(Command::OpenSubscription {
            name: format!("subscription-{index}"),
            start: Start::Earliest,
        })
    };
    let mut trace = vec![
        Command::Init {
            topic_id: TOPIC,
            partition: 0,
        }
        .into(),
    ];
    trace.extend((1..=4097).map(open));
    trace.push(open(4096));
    let replies: Vec<Mutation> =
        serde_json::from_value(fixture.group.write(to_value(trace)?).await?)?;
    assert_eq!(replies.len(), 4099);
    assert!(
        replies[..4097]
            .iter()
            .all(|reply| matches!(reply, Ok(Reply::Done)))
    );
    let exhausted = replies[4097].as_ref().unwrap_err();
    assert_eq!(
        to_value(exhausted)?,
        json!({
            "code": "conflict", "message": "partition has too many subscriptions"
        })
    );
    assert!(matches!(replies[4098], Ok(Reply::Done)));
    fixture.close().await
}
