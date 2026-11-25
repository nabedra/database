//! Public HTTP observations in the shared JSONL history format.

use std::collections::BTreeMap;

use nabedra_check::{
    checker::{Verdict, direct},
    history::{Event, EventType, NEMESIS, write_jsonl},
};
use nabedra_oltp_types::types::Digest;
use nabedra_protocol::protocol::{Outcome, ProtocolError};
use nabedra_rt::sim::World;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    client::digest,
    invariants::{TraceObserver, elapsed_ms},
};

#[derive(Debug)]
pub struct Client {
    process: u64,
}

/// A history operation. Supply the public operation name and its input row or
/// value. Admin and setup operations have no table or key in the history.
pub struct Operation<'a> {
    pub table: Option<&'a str>,
    pub name: &'a str,
    pub key: Option<&'a str>,
    pub request_id: Uuid,
    pub value: Value,
}

/// A handle to the invocation that the HTTP reply completes.
pub struct Attempt(usize);

pub enum Completion {
    /// The public success envelope's `data` field.
    Ok(Value),
    /// The public error envelope, which declares whether the outcome is known.
    Refused(ProtocolError),
    /// The client could not observe a reply, for example after a deadline or cut.
    Unknown(String),
}

pub type Bodies = BTreeMap<usize, Digest>;

#[derive(Default)]
pub struct History {
    events: Vec<Event>,
    /// The digest of the body that each invocation sent, by event index.
    bodies: Bodies,
    next_process: u64,
    trace: Option<TraceObserver>,
}

impl History {
    pub fn set_trace(&mut self, trace: TraceObserver) {
        self.trace = Some(trace);
    }

    pub fn client(&mut self) -> Client {
        assert_ne!(self.next_process, NEMESIS, "history process IDs exhausted");
        let process = self.next_process;
        self.next_process += 1;
        Client { process }
    }

    pub fn invoke(
        &mut self,
        client: &Client,
        world: &World,
        gateway: u64,
        operation: Operation<'_>,
    ) -> Attempt {
        let name = if operation.table.is_some() {
            operation
                .name
                .strip_prefix("table.")
                .unwrap_or(operation.name)
        } else {
            operation.name
        };
        let qualify = |key: &str| match operation.table {
            Some(table) => format!("{table}/{key}"),
            None => key.to_owned(),
        };

        let mut value = operation.value;
        if name == "insert_many"
            && let Some(rows) = value.as_array_mut()
        {
            // The checker partitions both event keys and batch row keys by table.
            for row in rows {
                let Some(fields) = row.as_object_mut() else {
                    continue;
                };
                let key = fields.get("key").or_else(|| fields.get("id"));
                let key = match key {
                    Some(Value::String(key)) => key.clone(),
                    Some(Value::Number(key)) => key.to_string(),
                    _ => continue,
                };
                fields.insert("key".into(), Value::String(qualify(&key)));
            }
        }
        let mut event = Event::new(EventType::Invoke, client.process, name).with_value(value);
        event.key = operation.key.map(qualify);
        event.request_id = Some(operation.request_id.to_string());
        event.node = Some(format!("n{gateway}"));
        let attempt = Attempt(self.events.len());
        self.push(world, event);
        attempt
    }

    /// The body that an invocation sends.
    pub fn sends(&mut self, attempt: &Attempt, body: &Value) {
        self.bodies.insert(attempt.0, digest(body));
    }

    pub fn bodies(&self) -> &Bodies {
        &self.bodies
    }

    pub fn complete(
        &mut self,
        client: &mut Client,
        world: &World,
        attempt: Attempt,
        completion: Completion,
    ) {
        let mut event = self.events[attempt.0].clone();
        assert_eq!(
            event.process, client.process,
            "attempt belongs to another process"
        );
        let (kind, value) = match completion {
            Completion::Ok(value) => (EventType::Ok, value),
            Completion::Refused(error) => match error.outcome {
                Outcome::NotApplied => (
                    EventType::Fail,
                    json!({"not_applied": true, "code": error.code, "message": error.message}),
                ),
                Outcome::Unknown => (
                    EventType::Info,
                    json!({"code": error.code, "message": error.message}),
                ),
            },
            Completion::Unknown(reason) => (EventType::Info, json!({"reason": reason})),
        };
        event.kind = kind;
        event.value = value;
        self.push(world, event);
        if kind == EventType::Info {
            // An unknown attempt can still apply later. Its next operation starts
            // a different logical process even if it uses the same client node.
            *client = self.client();
        }
    }

    pub fn marker(&mut self, world: &World, action: &str, value: Value) {
        let event = Event::new(EventType::Ok, NEMESIS, action).with_value(value);
        self.push(world, event);
    }

    fn push(&mut self, world: &World, mut event: Event) {
        event.index = self.events.len();
        event.time_ns = Some(u64::try_from(world.elapsed().as_nanos()).unwrap_or(u64::MAX));
        if let Some(trace) = &self.trace {
            let event_json = serde_json::to_string(&event).expect("serializable history event");
            trace(elapsed_ms(world), format!("history {event_json}"));
        }
        self.events.push(event);
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn jsonl(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &self.events).expect("write history to memory");
        bytes
    }

    /// Runs per-key linearizability, failed-write observation and keyed
    /// deduplication together, so successful retries retain their original effect.
    pub fn check_direct(&self) -> Verdict {
        direct::check(&self.events)
    }
}
