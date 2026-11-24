//! Client operations and the relaxed replication history.

use bytes::Bytes;
use nabedra_check::history::EventType;
use serde_json::{Value, json};

use super::super::protocol::WorkerId;
use super::invariants::event;
use crate::cache::{
    command::{Command, DataCommand},
    wire::Response,
};

#[derive(Clone, Debug)]
pub(super) enum Op {
    Set(String, Option<u64>),
    Get,
    Del,
    Incr(u64),
    Expire(i64),
    Persist,
    Pttl,
}

impl Op {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Set(..) => "set",
            Self::Get => "get",
            Self::Del => "del",
            Self::Incr(_) => "incrby",
            Self::Expire(_) => "expire",
            Self::Persist => "persist",
            Self::Pttl => "pttl",
        }
    }

    pub fn command(&self, key: &str) -> DataCommand {
        let mut args = vec![
            Bytes::from_static(self.name().as_bytes()),
            Bytes::copy_from_slice(key.as_bytes()),
        ];
        match self {
            Self::Set(v, ttl) => {
                args.push(Bytes::copy_from_slice(v.as_bytes()));
                if let Some(ttl) = ttl {
                    args.extend([Bytes::from_static(b"PX"), Bytes::from(ttl.to_string())]);
                }
            }
            Self::Incr(n) => args.push(Bytes::from(n.to_string())),
            Self::Expire(ms) => {
                args[0] = Bytes::from_static(b"PEXPIRE");
                args.push(Bytes::from(ms.to_string()));
            }
            _ => {}
        }
        let Ok(Command::Data(command)) = Command::parse(args) else {
            panic!("failed to parse {}", self.name());
        };
        command
    }

    pub fn invoke(
        &self,
        process: u64,
        worker: WorkerId,
        key: &str,
        now: u64,
    ) -> nabedra_check::history::Event {
        let expiry = |ttl: u64| (nabedra_rt::sim::EPOCH_MS + now + ttl) * 1_000_000;
        let value = match self {
            Self::Set(v, Some(ttl)) => json!({"value": v, "at_ns": expiry(*ttl)}),
            Self::Set(v, None) => json!(v),
            Self::Incr(n) => json!(n),
            Self::Expire(n) => json!({"at_ns": expiry((*n).max(0) as u64)}),
            _ => Value::Null,
        };
        let mut event = event(EventType::Invoke, process, self.name(), key, value, now);
        event.node = Some(worker.node.to_string());
        event
    }

    pub fn complete(
        &self,
        process: u64,
        worker: WorkerId,
        key: &str,
        now: u64,
        reply: Option<&(Response, Option<u64>)>,
    ) -> nabedra_check::history::Event {
        let (kind, value) = match reply {
            None => (EventType::Info, Value::Null),
            Some((Response::Error(reason), _)) => {
                (EventType::Fail, json!(String::from_utf8_lossy(reason)))
            }
            Some((response, epoch)) => {
                let value = match response {
                    Response::Bulk(Some(v)) => json!(String::from_utf8_lossy(v)),
                    Response::Integer(v) => json!(v),
                    _ => Value::Null,
                };
                (EventType::Ok, json!({"value": value, "epoch": epoch}))
            }
        };
        let mut event = event(kind, process, self.name(), key, value, now);
        event.node = Some(worker.node.to_string());
        event
    }
}
