use std::collections::BTreeMap;

use nabedra_events::partition::{
    Command, Error, ErrorCode, Event, Lease, Mutation, Poll, PublishEvent, Published, Query,
    QueryReply, Reply, Status, SubscriptionStatus,
};
use serde_json::{Value, json, to_value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use super::{Step, TOPIC};

const HEAD_KEY: &[u8] = b"partition/v1/head";
const SUB_KEY: &[u8] = b"partition/v1/subscription/replay";

#[derive(Default)]
pub(super) struct Model {
    // Keep the full history so an offset can never be reused after retention.
    log: Vec<(i64, Event)>,
    removed: usize,
    cutoff: i64,
    lease_clock: i64,
    pub(super) checkpoint: u64,
    lease: Option<ModelLease>,
}

struct ModelLease {
    owner: String,
    claim_id: Uuid,
    generation: u64,
    expires_ms: i64,
    released: bool,
}

impl ModelLease {
    fn receipt(&self) -> Reply {
        Reply::Lease(Lease {
            generation: self.generation,
            expires_ms: self.expires_ms,
        })
    }

    fn live_at(&self, now: i64) -> bool {
        !self.released && now < self.expires_ms
    }
}

impl Model {
    pub(super) fn tail(&self) -> u64 {
        self.log.len() as u64
    }

    pub(super) fn generation(&self) -> u64 {
        self.lease.as_ref().map_or(1, |lease| lease.generation)
    }

    fn earliest(&self) -> u64 {
        self.removed as u64 + 1
    }

    fn retained(&self) -> &[(i64, Event)] {
        &self.log[self.removed..]
    }

    fn publish(&mut self, now: i64, inputs: &[PublishEvent]) -> Result<Published, Error> {
        if inputs.is_empty() {
            return Err(Error::new(ErrorCode::InvalidRequest, "invalid batch count"));
        }
        let mut ids = std::collections::HashSet::new();
        let mut pending = Vec::new();
        let mut receipts = Vec::new();
        let published = now
            .max(self.cutoff)
            .max(self.log.last().map_or(0, |(time, _)| *time));
        for input in inputs {
            if !ids.insert(input.event_id) {
                return Err(Error::new(
                    ErrorCode::Conflict,
                    "event ID repeats within the batch",
                ));
            }
            let event = if let Some((_, event)) = self
                .retained()
                .iter()
                .find(|(_, event)| event.event_id == input.event_id)
            {
                if event.data != input.data {
                    return Err(Error::new(
                        ErrorCode::Conflict,
                        "event ID has different data",
                    ));
                }
                event.clone()
            } else {
                let event = Event {
                    topic_id: TOPIC,
                    partition: 0,
                    offset: self.tail() + pending.len() as u64 + 1,
                    event_id: input.event_id,
                    published_at: OffsetDateTime::from_unix_timestamp_nanos(
                        i128::from(published) * 1_000_000,
                    )
                    .unwrap()
                    .format(&Rfc3339)
                    .unwrap(),
                    data: input.data.clone(),
                };
                pending.push((published, event.clone()));
                event
            };
            receipts.push(event);
        }
        let inserted = pending.len();
        self.log.extend(pending);
        Ok(Published {
            events: receipts,
            inserted,
        })
    }

    fn live(&self, owner: &str, generation: u64, now: i64) -> Result<(), Error> {
        match &self.lease {
            Some(lease)
                if lease.owner == owner && lease.generation == generation && lease.live_at(now) =>
            {
                Ok(())
            }
            _ => Err(lease_lost()),
        }
    }

    fn claim(&mut self, owner: &str, claim_id: Uuid, received: i64, ttl: i64) -> Mutation {
        let now = self.observe_time(received);
        let expires_ms = expiry(now, ttl)?;
        let generation = if let Some(previous) = &self.lease {
            let live = previous.live_at(now);
            match (
                live,
                previous.owner == owner && previous.claim_id == claim_id,
            ) {
                (true, true) => return Ok(previous.receipt()),
                (true, false) => {
                    return Err(Error::new(
                        ErrorCode::LeaseBusy,
                        "another grant already holds this partition",
                    ));
                }
                (false, true) => return Err(lease_lost()),
                (false, false) => previous.generation + 1,
            }
        } else {
            1
        };
        let lease = ModelLease {
            owner: owner.into(),
            claim_id,
            generation,
            expires_ms,
            released: false,
        };
        let reply = lease.receipt();
        self.lease = Some(lease);
        Ok(reply)
    }

    fn observe_time(&mut self, now: i64) -> i64 {
        self.lease_clock = self.lease_clock.max(now);
        self.lease_clock
    }

    fn expired(&self) -> Error {
        Error {
            code: ErrorCode::CheckpointExpired,
            message: "checkpoint is behind the retention floor".into(),
            partition: Some(0),
            earliest_offset: Some(self.earliest()),
        }
    }

    pub(super) fn apply(&mut self, command: &Command) -> Mutation {
        match command {
            Command::Init { .. } | Command::OpenSubscription { .. } => Ok(Reply::Done),
            Command::Publish { now_ms, events } => {
                Ok(Reply::Published(self.publish(*now_ms, events)?))
            }
            Command::PublishMany { now_ms, batches } => Ok(Reply::Coalesced {
                results: batches
                    .iter()
                    .map(|events| self.publish(*now_ms, events))
                    .collect(),
            }),
            Command::Claim {
                owner,
                claim_id,
                now_ms,
                ttl_ms,
                ..
            } => self.claim(owner, *claim_id, *now_ms, *ttl_ms),
            Command::Renew {
                owner,
                generation,
                now_ms,
                ttl_ms,
                ..
            } => {
                let now = self.observe_time(*now_ms);
                let expires = expiry(now, *ttl_ms)?;
                self.live(owner, *generation, now)?;
                let lease = self.lease.as_mut().unwrap();
                lease.expires_ms = lease.expires_ms.max(expires);
                Ok(lease.receipt())
            }
            Command::Ack {
                owner,
                generation,
                after,
                last,
                through,
                now_ms,
                ..
            } => {
                let now = self.observe_time(*now_ms);
                self.live(owner, *generation, now)?;
                if !(*after <= self.checkpoint
                    && after < through
                    && through <= last
                    && *last <= self.tail())
                {
                    return Err(Error::new(
                        ErrorCode::InvalidRequest,
                        "acknowledgment is outside the delivered prefix",
                    ));
                }
                // EVENTS-8: a token that starts at the checkpoint covers only delivered
                // offsets, so it advances even after retention passed the checkpoint.
                if *through > self.checkpoint
                    && self.checkpoint < self.removed as u64
                    && *after != self.checkpoint
                {
                    return Err(self.expired());
                }
                self.checkpoint = self.checkpoint.max(*through);
                Ok(Reply::Checkpoint {
                    offset: self.checkpoint,
                })
            }
            Command::ResetExpired { .. } => {
                if self.checkpoint < self.removed as u64 {
                    self.checkpoint = self.removed as u64;
                    if let Some(lease) = &mut self.lease {
                        lease.released = true;
                    }
                }
                Ok(Reply::Checkpoint {
                    offset: self.checkpoint,
                })
            }
            Command::Retain { cutoff_ms, limit } => {
                self.cutoff = self.cutoff.max(*cutoff_ms);
                let removed = self
                    .retained()
                    .iter()
                    .take(*limit)
                    .take_while(|(ms, _)| *ms < self.cutoff)
                    .count();
                self.removed += removed;
                Ok(Reply::Retained {
                    removed,
                    earliest_offset: self.earliest(),
                })
            }
            unexpected => panic!("operation outside the model: {unexpected:?}"),
        }
    }

    fn read(&self, query: &Query) -> Result<QueryReply, Error> {
        match query {
            Query::Status => Ok(QueryReply::Status(Status {
                topic_id: TOPIC,
                partition: 0,
                earliest_offset: self.earliest(),
                tail: self.tail(),
                oldest_published_ms: self.retained().first().map(|(time, _)| *time),
            })),
            Query::SubscriptionStatus { .. } => {
                let lease = self.lease.as_ref().filter(|lease| !lease.released);
                Ok(QueryReply::SubscriptionStatus(SubscriptionStatus {
                    checkpoint: self.checkpoint,
                    earliest_offset: self.earliest(),
                    tail: self.tail(),
                    lease_generation: lease.map(|lease| lease.generation),
                    lease_expires_ms: lease.map(|lease| lease.expires_ms),
                }))
            }
            Query::Poll {
                owner,
                generation,
                now_ms,
                limit,
                ..
            } => {
                self.live(owner, *generation, (*now_ms).max(self.lease_clock))?;
                if self.checkpoint < self.removed as u64 {
                    return Err(self.expired());
                }
                let events: Vec<_> = self.log[self.checkpoint as usize..]
                    .iter()
                    .take(*limit)
                    .map(|(_, event)| event.clone())
                    .collect();
                Ok(QueryReply::Poll(Poll {
                    after: self.checkpoint,
                    last: events.last().map(|event| event.offset),
                    events,
                }))
            }
        }
    }

    pub(super) fn replay(&mut self, step: &Step) -> Value {
        match step {
            Step::Write(command) => to_value(self.apply(command)).unwrap(),
            Step::Read(query) => to_value(self.read(query)).unwrap(),
        }
    }

    pub(super) fn records(&self) -> BTreeMap<Vec<u8>, Value> {
        let mut head = json!({
            "topic_id": TOPIC, "partition": 0, "next_offset": self.tail() + 1,
            "earliest_offset": self.earliest(), "last_published_ms": self.log.last().map_or(0, |(ms, _)| *ms),
            "retention_cutoff_ms": self.cutoff, "lease_clock_ms": self.lease_clock, "subscription_count": 1,
        });
        let sub = json!({
            "start": "earliest", "checkpoint": self.checkpoint,
            "lease": self.lease.as_ref().map(|lease| json!({
                "owner": lease.owner, "claim_id": lease.claim_id, "generation": lease.generation,
                "expires_ms": lease.expires_ms, "released": lease.released,
            })),
        });
        let mut records = BTreeMap::new();
        for (published_ms, event) in self.retained() {
            let event_key = [
                b"partition/v1/event/".as_slice(),
                &event.offset.to_be_bytes(),
            ]
            .concat();
            let dedup_key = [b"partition/v1/dedup/".as_slice(), event.event_id.as_bytes()].concat();
            records.insert(
                event_key,
                json!({"published_ms": published_ms, "event": event}),
            );
            records.insert(dedup_key, json!(event.offset));
        }
        // A retained record includes a u32 length for both its key and value.
        let retained_bytes = records
            .iter()
            .map(|(key, value)| 8 + key.len() + serde_json::to_vec(value).unwrap().len())
            .sum::<usize>();
        head["retained_event_bytes"] = json!(retained_bytes);
        records.insert(HEAD_KEY.to_vec(), head);
        records.insert(SUB_KEY.to_vec(), sub);
        records
    }
}

fn expiry(now: i64, ttl: i64) -> Result<i64, Error> {
    if (1..=300_000).contains(&ttl) {
        Ok(now + ttl)
    } else {
        Err(Error::new(
            ErrorCode::InvalidRequest,
            "invalid lease lifetime",
        ))
    }
}

fn lease_lost() -> Error {
    Error::new(ErrorCode::LeaseLost, "lease is expired or no longer held")
}
