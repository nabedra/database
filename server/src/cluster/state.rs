//! A05 durable control roots and A08 derived capacity. No transition authority or I/O.
use super::{
    configuration::{Template, Timing},
    member::{Action, Intent, IntentOutcome, IntentState, Node, Ownership},
};
use crate::protocol::deserialize_object;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{self, Write},
};
use uuid::Uuid;

pub const MAX_CONTROL_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_NODES: usize = 2048;
pub const MAX_INTENTS: usize = 3072;
pub const MAX_RECORDS: usize = 4096;
const FIXED_BYTES: usize = 256 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlError {
    InvalidRecord,
    LimitExceeded,
}
impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRecord => "invalid control record",
            Self::LimitExceeded => "control record limit exceeded",
        })
    }
}
impl std::error::Error for ControlError {}
fn check(ok: bool) -> Result<(), ControlError> {
    if ok {
        Ok(())
    } else {
        Err(ControlError::InvalidRecord)
    }
}
fn limit(ok: bool) -> Result<(), ControlError> {
    if ok {
        Ok(())
    } else {
        Err(ControlError::LimitExceeded)
    }
}
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
fn uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
    let s = String::deserialize(d)?;
    let id = Uuid::parse_str(&s).map_err(|_| serde::de::Error::custom("invalid identity"))?;
    if id.hyphenated().to_string() != s {
        return Err(serde::de::Error::custom("invalid identity"));
    }
    Ok(id)
}
fn nullable_uuid<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
    struct Identity(Uuid);
    impl<'de> Deserialize<'de> for Identity {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            uuid(d).map(Self)
        }
    }
    Option::<Identity>::deserialize(d).map(|v| v.map(|id| id.0))
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
macro_rules! record {
    ($name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Clone, PartialEq, Eq, Serialize)]
        pub struct $name { $($(#[$attr])* pub $field: $ty),* }
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

string_enum!(Provisioning { Managed => "managed", External => "external" });
string_enum!(PolicyMode { Fixed => "fixed", Autoscale => "autoscale" });
string_enum!(LeaseState { Released => "released", Held => "held" });
record!(Config {
 generation:u64, storage_fingerprint:String, auth_fingerprint:String,
 template_fingerprint:String, lifecycle_fingerprint:String, worker_semantic_fingerprint:String,
 #[serde(deserialize_with="nullable")] adapter_contract:Option<String>,
});
record!(Policy {
    revision: u64,
    mode: PolicyMode,
    desired: u64,
    min_nodes: u64,
    max_nodes: u64
});
record!(Controller {
 #[serde(deserialize_with="nullable_uuid")] owner_id:Option<Uuid>,
 epoch:u64, lease_state:LeaseState,
 #[serde(deserialize_with="nullable")] lease_until_ms:Option<u64>,
});
fn zero(value: &u64) -> bool {
    *value == 0
}
record!(Scheduler {
 #[serde(default,skip_serializing_if="zero")] scheduler_term:u64,
 #[serde(default, deserialize_with="nullable_uuid", skip_serializing_if="Option::is_none")] coordinator_node:Option<Uuid>,
 cooldown_until_ms:u64, backoff_until_ms:u64, failures:u64,
 #[serde(deserialize_with="nullable")] low_since_ms:Option<u64>,
});
record!(Control {
 version:u64, #[serde(deserialize_with="uuid")] cluster_id:Uuid, name:String,
 config:Config, provisioning:Provisioning, template:Template, timing:Timing,
 revision:u64, #[serde(deserialize_with="uuid")] change_id:Uuid,
 policy:Policy, controller:Controller, scheduler:Scheduler,
 admin_seq:u64, #[serde(deserialize_with="nullable_uuid")] admin_head:Option<Uuid>,
 #[serde(deserialize_with="nodes")] nodes:Vec<Node>,
 #[serde(deserialize_with="intents")] intents:Vec<Intent>,
});
fn collection<'de, D: Deserializer<'de>, T: Deserialize<'de>, const MAX: usize>(
    d: D,
) -> Result<Vec<T>, D::Error> {
    struct Bounded<T, const MAX: usize>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>, const MAX: usize> Visitor<'de> for Bounded<T, MAX> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bounded member array")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            loop {
                if values.len() == MAX {
                    if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                        return Err(serde::de::Error::custom("member count limit"));
                    }
                    return Ok(values);
                }
                match seq.next_element()? {
                    Some(value) => values.push(value),
                    None => return Ok(values),
                }
            }
        }
    }
    d.deserialize_seq(Bounded::<T, MAX>(std::marker::PhantomData))
}
fn nodes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Node>, D::Error> {
    collection::<D, Node, MAX_NODES>(d)
}
fn intents<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Intent>, D::Error> {
    collection::<D, Intent, MAX_INTENTS>(d)
}
fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
// The callback adapter tag is a persisted format contract, retained across renames.
impl Config {
    fn validate_fields(&self) -> Result<(), ControlError> {
        check(
            self.generation == 2
                && [
                    &self.storage_fingerprint,
                    &self.auth_fingerprint,
                    &self.template_fingerprint,
                    &self.lifecycle_fingerprint,
                    &self.worker_semantic_fingerprint,
                ]
                .into_iter()
                .all(|s| digest(s))
                && self
                    .adapter_contract
                    .as_deref()
                    .is_none_or(|s| s == "meshdb.callbacks.v1"),
        )
    }
}
impl Policy {
    fn validate_fields(&self) -> Result<(), ControlError> {
        check(
            self.revision > 0
                && self.max_nodes <= 1024
                && self.min_nodes <= self.desired
                && self.desired <= self.max_nodes
                && match self.mode {
                    PolicyMode::Fixed => {
                        self.min_nodes == self.desired && self.max_nodes == self.desired
                    }
                    PolicyMode::Autoscale => self.min_nodes >= 1,
                },
        )
    }
}
impl Controller {
    fn validate_fields(&self) -> Result<(), ControlError> {
        check(match self.lease_state {
            LeaseState::Released => self.owner_id.is_none() && self.lease_until_ms.is_none(),
            LeaseState::Held => {
                self.owner_id.is_some() && self.lease_until_ms.is_some() && self.epoch > 0
            }
        })
    }
}
impl Scheduler {
    fn validate_fields(&self) -> Result<(), ControlError> {
        Ok(())
    }
}
/// Maximum-record accounting, independent of current encoded field lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    pub nodes: usize,
    pub intents: usize,
    pub credits: usize,
    pub retained_bytes: usize,
    pub admission_bytes: usize,
}
fn budget(n: usize, i: usize) -> Result<usize, ControlError> {
    n.checked_mul(8192)
        .and_then(|v| i.checked_mul(4096).and_then(|w| v.checked_add(w)))
        .and_then(|v| v.checked_add(FIXED_BYTES))
        .ok_or(ControlError::LimitExceeded)
}
impl Control {
    fn validate_fields(&self) -> Result<(), ControlError> {
        limit(
            self.nodes.len() <= MAX_NODES
                && self.intents.len() <= MAX_INTENTS
                && self
                    .nodes
                    .len()
                    .checked_add(self.intents.len())
                    .is_some_and(|n| n <= MAX_RECORDS),
        )?;
        limit(budget(self.nodes.len(), self.intents.len())? <= MAX_CONTROL_BYTES)?;
        check(
            self.version == 1
                && self.revision > 0
                && !self.name.is_empty()
                && self.name.len() <= 64
                && self.name.bytes().enumerate().all(|(i, b)| {
                    b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
                })
                && (self.admin_seq == 0) == self.admin_head.is_none(),
        )?;
        self.config.validate()?;
        self.policy.validate()?;
        self.controller.validate()?;
        self.scheduler.validate()?;
        self.template
            .validate()
            .map_err(|_| ControlError::InvalidRecord)?;
        self.timing
            .validate()
            .map_err(|_| ControlError::InvalidRecord)?;
        check(
            self.template
                .fingerprint()
                .map_err(|_| ControlError::InvalidRecord)?
                == self.config.template_fingerprint,
        )?;
        match self.provisioning {
            Provisioning::External => check(
                self.policy.mode == PolicyMode::Fixed && self.config.adapter_contract.is_none(),
            )?,
            Provisioning::Managed => {
                check(self.config.adapter_contract.as_deref() == Some("meshdb.callbacks.v1"))?
            }
        }
        check(
            self.nodes.windows(2).all(|p| p[0].node_id < p[1].node_id)
                && self
                    .intents
                    .windows(2)
                    .all(|p| p[0].request_id < p[1].request_id),
        )?;
        let mut resources = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        let mut launches = BTreeSet::new();
        for node in &self.nodes {
            node.validate().map_err(|_| ControlError::InvalidRecord)?;
            check(
                node.ownership
                    == match self.provisioning {
                        Provisioning::Managed => Ownership::Managed,
                        Provisioning::External => Ownership::External,
                    },
            )?;
            if let Some(r) = &node.resource_id {
                check(resources.insert(r, node.node_id).is_none())?;
            }
            if let Some(i) = node.incarnation {
                check(incarnations.insert(i))?;
            }
            if let Some(l) = node.launch_id {
                check(launches.insert(l))?;
                check(
                    node.resources
                        .is_none_or(|r| r.validate_execution(&self.template.execution).is_ok()),
                )?;
            }
        }
        for node in &self.nodes {
            check(
                node.retired_incarnations
                    .iter()
                    .all(|i| !incarnations.contains(i)),
            )?;
        }
        let nodes: BTreeMap<_, _> = self.nodes.iter().map(|n| (n.node_id, n)).collect();
        let mut starts = BTreeSet::new();
        for intent in &self.intents {
            intent.validate().map_err(|_| ControlError::InvalidRecord)?;
            let node = nodes
                .get(&intent.node_id)
                .ok_or(ControlError::InvalidRecord)?;
            check(
                node.ownership == Ownership::Managed && node.launch_id == Some(intent.launch_id),
            )?;
            if let Some(r) = &intent.resource_id {
                check(node.resource_id.as_ref().is_none_or(|known| known == r))?;
                if let Some(other) = resources.insert(r, node.node_id) {
                    check(other == node.node_id)?;
                }
            }
            match intent.action {
                Action::Start => {
                    check(
                        starts.insert(node.node_id)
                            && intent.template_fingerprint.as_ref()
                                == Some(&self.config.template_fingerprint),
                    )?;
                }
                Action::Stop => check(
                    node.resource_id.is_some()
                        && intent.resource_id == node.resource_id
                        && intent.incarnation_id == node.incarnation,
                )?,
            }
        }
        check(
            self.nodes
                .iter()
                .filter(|n| n.ownership == Ownership::Managed)
                .all(|n| starts.contains(&n.node_id)),
        )
    }
    fn derived_capacity(&self) -> Result<Capacity, ControlError> {
        let stopped: BTreeSet<_> = self
            .intents
            .iter()
            .filter(|i| i.action == Action::Stop)
            .map(|i| (i.node_id, i.launch_id))
            .collect();
        let credits = self
            .intents
            .iter()
            .filter(|i| {
                i.action == Action::Start
                    && !stopped.contains(&(i.node_id, i.launch_id))
                    && !(i.state == IntentState::Failed
                        && matches!(
                            i.outcome,
                            Some(IntentOutcome::NotInvoked | IntentOutcome::Terminated)
                        ))
            })
            .count();
        let nodes = self.nodes.len();
        let intents = self.intents.len();
        Ok(Capacity {
            nodes,
            intents,
            credits,
            retained_bytes: budget(nodes, intents)?,
            admission_bytes: budget(
                nodes,
                intents
                    .checked_add(credits)
                    .ok_or(ControlError::LimitExceeded)?,
            )?,
        })
    }
    /// Validates the whole retained candidate, then derives first-stop credits.
    /// Terminal outcomes are persisted witnesses; this cannot establish their history.
    pub fn capacity(&self) -> Result<Capacity, ControlError> {
        self.validate()?;
        self.derived_capacity()
    }
    /// Additional check for a whole candidate allocating unreserved identities.
    /// Retained updates and reserved first stops use encode/validate alone.
    pub fn check_new_admission(&self) -> Result<(), ControlError> {
        let c = self.capacity()?;
        let augmented = c
            .intents
            .checked_add(c.credits)
            .ok_or(ControlError::LimitExceeded)?;
        limit(
            augmented <= MAX_INTENTS
                && c.nodes
                    .checked_add(augmented)
                    .is_some_and(|v| v <= MAX_RECORDS)
                && c.admission_bytes <= MAX_CONTROL_BYTES,
        )
    }
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, ControlError> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit - self.bytes.len() {
                return Err(io::Error::other("control record limit"));
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
    serde_json::to_writer(&mut writer, value).map_err(|_| ControlError::LimitExceeded)?;
    Ok(writer.bytes)
}
macro_rules! codec {
    ($name:ident, $limit:ident) => {
        impl $name {
            /// Bounds input before allocation and validates record-local facts.
            pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
                if bytes.len() > $limit {
                    return Err(ControlError::LimitExceeded);
                }
                let value: Self =
                    serde_json::from_slice(bytes).map_err(|_| ControlError::InvalidRecord)?;
                value.validate()?;
                Ok(value)
            }
            /// Validates constructed candidates and bounds output while writing.
            pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
                self.validate_fields()?;
                bounded(self, $limit)
            }
            /// Includes the encoded bound; establishes no historical evidence.
            pub fn validate(&self) -> Result<(), ControlError> {
                self.encode().map(|_| ())
            }
        }
    };
}
codec!(Control, MAX_CONTROL_BYTES);
const MAX_GLOBAL_RECORD_BYTES: usize = 4096;
codec!(Config, MAX_GLOBAL_RECORD_BYTES);
codec!(Policy, MAX_GLOBAL_RECORD_BYTES);
codec!(Controller, MAX_GLOBAL_RECORD_BYTES);
codec!(Scheduler, MAX_GLOBAL_RECORD_BYTES);
