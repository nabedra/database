//! A05 member records. Use decode/encode/validate at trust boundaries.
use crate::protocol::deserialize_object;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    fmt,
    io::{self, Write},
};
use uuid::Uuid;

pub const MAX_NODE_BYTES: usize = 8 * 1024;
pub const MAX_INTENT_BYTES: usize = 4 * 1024;
pub const MAX_HEARTBEAT_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberError {
    InvalidRecord,
    LimitExceeded,
}
impl fmt::Display for MemberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRecord => "invalid member record",
            Self::LimitExceeded => "member record limit exceeded",
        })
    }
}
impl std::error::Error for MemberError {}
fn check(ok: bool) -> Result<(), MemberError> {
    if ok {
        Ok(())
    } else {
        Err(MemberError::InvalidRecord)
    }
}
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
pub(crate) fn uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
    let s = String::deserialize(d)?;
    let id = Uuid::parse_str(&s).map_err(|_| serde::de::Error::custom("invalid identity"))?;
    if id.hyphenated().to_string() != s {
        return Err(serde::de::Error::custom("invalid identity"));
    }
    Ok(id)
}
pub(crate) fn nullable_uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
    struct Identity(Uuid);
    impl<'de> Deserialize<'de> for Identity {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            uuid(d).map(Self)
        }
    }
    Option::<Identity>::deserialize(d).map(|v| v.map(|id| id.0))
}
fn identities<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Uuid>, D::Error> {
    let strings = Vec::<String>::deserialize(d)?;
    strings
        .into_iter()
        .map(|s| {
            let id =
                Uuid::parse_str(&s).map_err(|_| serde::de::Error::custom("invalid identity"))?;
            if id.hyphenated().to_string() != s {
                return Err(serde::de::Error::custom("invalid identity"));
            }
            Ok(id)
        })
        .collect()
}
macro_rules! string_enum {
    ($name:ident { $($variant:ident => $text:literal),* }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
        pub enum $name { $(#[serde(rename=$text)] $variant),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self,D::Error> {
                match String::deserialize(d)?.as_str() { $($text => Ok(Self::$variant),)* _ => Err(serde::de::Error::custom("invalid enum")) }
            }
        }
    }
}
string_enum!(Ownership { Managed => "managed", External => "external" });
string_enum!(NodeState { Starting => "starting", Ready => "ready", Draining => "draining", Lost => "lost", Stopped => "stopped" });
string_enum!(Registration { Unregistered => "unregistered", Registered => "registered", Verified => "verified" });
string_enum!(Action { Start => "start", Stop => "stop" });
string_enum!(IntentState { Pending => "pending", InProgress => "in_progress", Reconciling => "reconciling", Completed => "completed", Failed => "failed" });
string_enum!(IntentOutcome { ResourceKnown => "resource_known", Terminated => "terminated", NotInvoked => "not_invoked" });

// Reuse the protocol object visitor, with derived strict fields/duplicates.
macro_rules! record {
    ($name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Eq, Serialize)]
        pub struct $name { $(pub $field: $ty),* }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct(stringify!($name)).finish_non_exhaustive() }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d:D)->Result<Self,D::Error>{
                #[derive(Deserialize)] #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let w: Wire = deserialize_object(d)?;
                Ok(Self { $($field:w.$field),* })
            }
        }
    }
}
record!(Session {
    #[serde(deserialize_with = "uuid")]
    id: Uuid,
    verifier: String,
});
record!(Drain {
    #[serde(deserialize_with="uuid")]
    id: Uuid,
    requested_at_ms: u64,
    #[serde(deserialize_with="nullable")]
    ack_seq: Option<u64>,
    confirmed_idle: bool,
});
record!(Delivery {
    #[serde(deserialize_with = "uuid")]
    invocation_id: Uuid,
    #[serde(deserialize_with = "uuid")]
    owner_id: Uuid,
    epoch: u64,
    command_sha256: String,
});
record!(Node {
    #[serde(deserialize_with="uuid")]
    node_id: Uuid,
    #[serde(deserialize_with="nullable_uuid")]
    launch_id: Option<Uuid>,
    #[serde(deserialize_with="nullable")]
    resource_id: Option<String>,
    ownership: Ownership,
    state: NodeState,
    #[serde(deserialize_with="nullable_uuid")]
    incarnation: Option<Uuid>,
    #[serde(deserialize_with="identities")]
    retired_incarnations: Vec<Uuid>,
    #[serde(deserialize_with="nullable")]
    endpoint: Option<String>,
    operation_slots: u64,
    partition_slots: u64,
    queue_size: u64,
    registration: Registration,
    #[serde(deserialize_with="nullable")]
    session: Option<Session>,
    #[serde(deserialize_with="nullable")]
    drain: Option<Drain>,
});
record!(Intent {
    #[serde(deserialize_with="uuid")]
    request_id: Uuid,
    #[serde(deserialize_with="uuid")]
    node_id: Uuid,
    #[serde(deserialize_with="uuid")]
    launch_id: Uuid,
    #[serde(deserialize_with="nullable_uuid")]
    incarnation_id: Option<Uuid>,
    #[serde(deserialize_with="nullable")]
    resource_id: Option<String>,
    action: Action,
    state: IntentState,
    attempts: u64,
    created_at_ms: u64,
    deadline_ms: u64,
    retry_at_ms: u64,
    #[serde(deserialize_with="nullable")]
    error_code: Option<String>,
    #[serde(deserialize_with="nullable")]
    template_fingerprint: Option<String>,
    #[serde(deserialize_with="nullable")]
    delivery: Option<Delivery>,
    #[serde(deserialize_with="nullable")]
    outcome: Option<IntentOutcome>,
});
record!(Heartbeat {
    version: u64,
    #[serde(deserialize_with="uuid")]
    cluster_id: Uuid,
    #[serde(deserialize_with="uuid")]
    node_id: Uuid,
    #[serde(deserialize_with="uuid")]
    incarnation: Uuid,
    sequence: u64,
    operation_slots: u64,
    partition_slots: u64,
    queue_size: u64,
    running_operations: u64,
    running_partitions: u64,
    queued_operations: u64,
    queued_partitions: u64,
    #[serde(deserialize_with="nullable_uuid")]
    drain_id: Option<Uuid>,
    accepting: bool,
});

fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn resource(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)
}
fn code(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}
fn capacities(operation: u64, partition: u64, queue: u64) -> Result<(), MemberError> {
    check((1..=256).contains(&operation) && (1..=256).contains(&partition) && queue <= 4096)
}
pub(crate) fn endpoint(s: &str) -> bool {
    if s.len() > 2048
        || !s.is_ascii()
        || s.bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
        || s.contains(['?', '#', '\\'])
    {
        return false;
    }
    let Some((scheme, tail)) = s.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let authority = tail.split('/').next().unwrap_or("");
    if authority.contains('@') {
        return false;
    }
    let port = if let Some(bracketed) = authority.strip_prefix('[') {
        let Some((host, rest)) = bracketed.split_once(']') else {
            return false;
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        if rest.is_empty() {
            None
        } else if let Some(port) = rest.strip_prefix(':') {
            Some(port)
        } else {
            return false;
        }
    } else {
        let (host, port) = authority
            .split_once(':')
            .map_or((authority, None), |(h, p)| (h, Some(p)));
        if host.is_empty() || host.contains(['[', ']']) {
            return false;
        }
        port
    };
    if let Some(port) = port
        && (port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || !matches!(port.parse::<u16>(), Ok(1..=u16::MAX)))
    {
        return false;
    }
    let Ok(uri) = s.parse::<hyper::Uri>() else {
        return false;
    };
    uri.authority().is_some() && uri.host().is_some_and(|host| !host.is_empty())
}

impl Node {
    fn validate_fields(&self) -> Result<(), MemberError> {
        capacities(self.operation_slots, self.partition_slots, self.queue_size)?;
        check(
            self.resource_id.as_deref().is_none_or(resource)
                && self.endpoint.as_deref().is_none_or(endpoint),
        )?;
        match self.ownership {
            Ownership::Managed => check(self.launch_id.is_some())?,
            Ownership::External => check(
                self.launch_id.is_none()
                    && self.resource_id.as_deref()
                        == Some(format!("external:{}", self.node_id).as_str()),
            )?,
        }
        match self.registration {
            Registration::Unregistered => check(
                self.incarnation.is_none() && self.endpoint.is_none() && self.session.is_none(),
            )?,
            Registration::Registered | Registration::Verified => check(
                self.incarnation.is_some() && self.endpoint.is_some() && self.session.is_some(),
            )?,
        }
        check(self.registration != Registration::Verified || self.resource_id.is_some())?;
        check(self.state != NodeState::Ready || self.registration == Registration::Verified)?;
        check(
            self.retired_incarnations.len() <= 16
                && self.retired_incarnations.windows(2).all(|v| v[0] < v[1])
                && self
                    .incarnation
                    .is_none_or(|id| !self.retired_incarnations.contains(&id)),
        )?;
        if let Some(session) = &self.session {
            check(digest(&session.verifier))?;
        }
        if let Some(drain) = &self.drain {
            check(drain.ack_seq.is_none_or(|v| v > 0))?;
            check(
                !drain.confirmed_idle
                    || (drain.ack_seq.is_some()
                        && self.incarnation.is_some()
                        && self.registration != Registration::Unregistered),
            )?;
        }
        Ok(())
    }
}
impl Intent {
    fn validate_fields(&self) -> Result<(), MemberError> {
        check(
            self.resource_id.as_deref().is_none_or(resource)
                && self.error_code.as_deref().is_none_or(code)
                && self.template_fingerprint.as_deref().is_none_or(digest),
        )?;
        match self.action {
            Action::Start => check(
                self.request_id == self.launch_id
                    && self.incarnation_id.is_none()
                    && self.template_fingerprint.is_some(),
            )?,
            Action::Stop => check(
                self.request_id != self.launch_id
                    && self.resource_id.is_some()
                    && self.template_fingerprint.is_none(),
            )?,
        }
        if let Some(delivery) = &self.delivery {
            check(delivery.epoch > 0 && digest(&delivery.command_sha256))?;
        }
        match self.state {
            IntentState::InProgress => {
                check(self.delivery.is_some() && self.attempts > 0 && self.outcome.is_none())?
            }
            IntentState::Pending | IntentState::Reconciling => check(self.outcome.is_none())?,
            IntentState::Completed => match self.action {
                Action::Start => check(
                    self.outcome == Some(IntentOutcome::ResourceKnown)
                        && self.resource_id.is_some(),
                )?,
                Action::Stop => check(self.outcome == Some(IntentOutcome::Terminated))?,
            },
            IntentState::Failed => check(matches!(
                self.outcome,
                Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
            ))?,
        }
        Ok(())
    }
}
impl Heartbeat {
    fn validate_fields(&self) -> Result<(), MemberError> {
        capacities(self.operation_slots, self.partition_slots, self.queue_size)?;
        check(
            self.version == 1
                && self.sequence > 0
                && self.running_operations <= self.operation_slots
                && self.running_partitions <= self.partition_slots
                && self.queued_operations <= self.queue_size
                && self.queued_partitions <= self.queue_size
                && (self.drain_id.is_none() || !self.accepting),
        )
    }
}

// Same bounded-writer pattern as storage::decision. No unbounded to_vec first.
fn bounded(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, MemberError> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit - self.bytes.len() {
                return Err(io::Error::other("member record limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| MemberError::LimitExceeded)?;
    Ok(writer.bytes)
}
macro_rules! codec {
    ($name:ident, $limit:ident) => {
        impl $name {
            /// Bounds input before allocation and validates record-local facts.
            pub fn decode(bytes: &[u8]) -> Result<Self, MemberError> {
                if bytes.len() > $limit {
                    return Err(MemberError::LimitExceeded);
                }
                let value: Self =
                    serde_json::from_slice(bytes).map_err(|_| MemberError::InvalidRecord)?;
                value.validate_fields()?;
                Ok(value)
            }
            /// Validates constructed candidates and bounds output while writing.
            pub fn encode(&self) -> Result<Vec<u8>, MemberError> {
                self.validate_fields()?;
                bounded(self, $limit)
            }
            /// Includes the encoded bound; establishes no historical evidence.
            pub fn validate(&self) -> Result<(), MemberError> {
                self.encode().map(|_| ())
            }
        }
    };
}
codec!(Node, MAX_NODE_BYTES);
codec!(Intent, MAX_INTENT_BYTES);
codec!(Heartbeat, MAX_HEARTBEAT_BYTES);
