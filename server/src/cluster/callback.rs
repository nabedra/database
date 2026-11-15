//! Fixed callback wire records only. No dispatch, authority or provider truth.
use super::{
    attachment::AttachedConfig,
    configuration::{self, Storage, Tls},
    controller::{Owner, OwnershipObservation},
    member::{self, Action},
    state::{Control, LeaseState, Provisioning},
};
use crate::{protocol::deserialize_object, storage::StorageError};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt};
use uuid::Uuid;
const MAX_BODY: usize = 1024 * 1024;
const MAX_RECORDS: usize = 4096;
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
// Same fixed-object pattern as configuration/member; no generic JSON tree.
macro_rules! record {
    ($vis:vis $name:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        #[derive(Serialize, PartialEq, Eq)]
        $vis struct $name { $($field: $ty),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d:D)->Result<Self,D::Error> {
                #[derive(Deserialize)] #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let _w: Wire = deserialize_object(d)?;
                Ok(Self { $($field:_w.$field),* })
            }
        }
    }
}
macro_rules! redacted {
    ($($name:ty),*) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f:&mut fmt::Formatter<'_>)->fmt::Result {
            f.write_str(concat!(stringify!($name), " { .. }"))
        }
    })* }
}
record!(Target {
    #[serde(deserialize_with="member::uuid")] node_id: Uuid,
    #[serde(deserialize_with="member::uuid")] request_id: Uuid,
    #[serde(deserialize_with="member::nullable_uuid")] incarnation_id: Option<Uuid>,
});
record!(Bootstrap {
    version: u64,
    #[serde(deserialize_with="member::uuid")] cluster_id: Uuid,
    #[serde(deserialize_with="member::uuid")] node_id: Uuid,
    #[serde(deserialize_with="member::uuid")] request_id: Uuid,
    api_urls: Vec<String>, listen: String,
    #[serde(deserialize_with="nullable")] advertise_url: Option<String>,
    storage: Storage, enrollment_token: String,
    #[serde(deserialize_with="nullable")] tls: Option<Tls>,
    operation_slots: u64, partition_slots: u64, queue_size: u64, partition_size: String,
});
record!(StartPayload {
    version: u64,
    #[serde(deserialize_with = "member::uuid")]
    cluster_id: Uuid,
    cluster_name: String,
    #[serde(deserialize_with = "member::uuid")]
    request_id: Uuid,
    #[serde(deserialize_with = "member::uuid")]
    node_id: Uuid,
    bootstrap: Bootstrap,
});
record!(StopPayload {
    resource_id: String
});
record!(Empty {});
#[derive(Serialize)]
struct Command<P> {
    version: u64,
    invocation_id: Uuid,
    cluster_id: Uuid,
    owner_id: Uuid,
    epoch: u64,
    intent_id: Option<Uuid>,
    action: &'static str,
    target: Option<Target>,
    payload: P,
}
// RawValue preserves result/payload until its action selects a fixed decoder.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandWire {
    version: u64,
    #[serde(deserialize_with = "member::uuid")]
    invocation_id: Uuid,
    #[serde(deserialize_with = "member::uuid")]
    cluster_id: Uuid,
    #[serde(deserialize_with = "member::uuid")]
    owner_id: Uuid,
    epoch: u64,
    #[serde(deserialize_with = "member::nullable_uuid")]
    intent_id: Option<Uuid>,
    action: String,
    #[serde(deserialize_with = "nullable")]
    target: Option<Target>,
    payload: Box<RawValue>,
}
pub(super) struct StartCommand {
    bytes: Vec<u8>,
    digest: String,
    context: ReplyContext,
}
pub(super) struct StopCommand {
    bytes: Vec<u8>,
    digest: String,
    context: ReplyContext,
}
#[allow(dead_code)] // Inventory reconciliation caller follows C05a.
pub(super) struct InventoryCommand {
    bytes: Vec<u8>,
    digest: String,
    context: ReplyContext,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplyAction {
    Start,
    Stop,
    Inventory,
}
pub(super) struct ReplyContext {
    cluster_id: Uuid,
    owner: Owner,
    invocation_id: Uuid,
    intent_id: Option<Uuid>,
    action: ReplyAction,
    observation: OwnershipObservation,
    delivery: Option<DeliveryAssociation>,
}
pub(super) enum CallbackReply {
    Started(String),
    Stopped,
    #[allow(dead_code)] // C05d consumes complete inventory evidence.
    Inventory(InventoryEvidence),
    Failure {
        code: String,
        outcome: DeliveryOutcome,
    },
}
#[derive(PartialEq, Eq)]
pub(super) enum DeliveryOutcome {
    NotInvoked,
    Unknown,
}
record!(InventoryRecordWire {
    resource_id: String,
    #[serde(deserialize_with = "member::uuid")]
    cluster_id: Uuid,
    #[serde(deserialize_with = "member::uuid")]
    node_id: Uuid,
    #[serde(deserialize_with = "member::uuid")]
    request_id: Uuid,
    state: String,
});
pub(super) struct InventoryRecord {
    wire: InventoryRecordWire,
}
#[allow(dead_code)] // Only complete validated inventory constructs evidence.
pub(super) struct InventoryEvidence {
    cluster_id: Uuid,
    owner: Owner,
    invocation_id: Uuid,
    observation: OwnershipObservation,
    records: Vec<InventoryRecord>,
}
redacted!(
    StartCommand,
    StopCommand,
    InventoryCommand,
    ReplyContext,
    CallbackReply,
    InventoryRecord,
    InventoryEvidence,
    DeliveryOutcome
);
fn invalid() -> StorageError {
    StorageError::InvalidCatalog
}
fn config_error(e: configuration::ConfigurationError) -> StorageError {
    match e {
        configuration::ConfigurationError::LimitExceeded => StorageError::LimitExceeded,
        _ => invalid(),
    }
}
fn parse<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T, StorageError> {
    configuration::input(raw, MAX_BODY).map_err(config_error)?;
    serde_json::from_slice(raw).map_err(|_| invalid())
}
fn object<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T, StorageError> {
    configuration::input(raw, MAX_BODY).map_err(config_error)?;
    let mut d = serde_json::Deserializer::from_slice(raw);
    let result = deserialize_object(&mut d).map_err(|_| invalid())?;
    d.end().map_err(|_| invalid())?;
    Ok(result)
}
fn resource(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)
}
fn digest(raw: &[u8]) -> String {
    Sha256::digest(raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn context(
    root: &Control,
    owner: Owner,
    invocation_id: Uuid,
    intent_id: Option<Uuid>,
    action: ReplyAction,
) -> Result<ReplyContext, StorageError> {
    let observation = OwnershipObservation::new(root)?;
    if root.provisioning != Provisioning::Managed
        || owner.epoch == 0
        || root.controller.owner_id != Some(owner.process_id)
        || root.controller.epoch != owner.epoch
        || root.controller.lease_state != LeaseState::Held
    {
        return Err(StorageError::Conflict);
    }
    Ok(ReplyContext {
        cluster_id: root.cluster_id,
        owner,
        invocation_id,
        intent_id,
        action,
        observation,
        delivery: None,
    })
}
fn expected_start(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    invocation_id: Uuid,
    attached: &AttachedConfig,
) -> Result<Command<StartPayload>, StorageError> {
    // Validate the complete root and immutable attachment before allocating payload.
    let enrollment = attached.callback_enrollment(root).map_err(|_| invalid())?;
    let _ = context(
        root,
        owner,
        invocation_id,
        Some(intent_id),
        ReplyAction::Start,
    )?;
    let intent = root
        .intents
        .iter()
        .find(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    if intent.delivery.as_ref().is_some_and(|d| {
        d.invocation_id != invocation_id || d.owner_id != owner.process_id || d.epoch != owner.epoch
    }) {
        return Err(StorageError::Conflict);
    }
    if intent.action != Action::Start
        || intent.template_fingerprint.as_deref() != Some(&root.config.template_fingerprint)
    {
        return Err(StorageError::Conflict);
    }
    let t = &root.template;
    Ok(Command {
        version: 1,
        invocation_id,
        cluster_id: root.cluster_id,
        owner_id: owner.process_id,
        epoch: owner.epoch,
        intent_id: Some(intent_id),
        action: "start",
        target: Some(Target {
            node_id: intent.node_id,
            request_id: intent.launch_id,
            incarnation_id: None,
        }),
        payload: StartPayload {
            version: 1,
            cluster_id: root.cluster_id,
            cluster_name: root.name.clone(),
            request_id: intent.launch_id,
            node_id: intent.node_id,
            bootstrap: Bootstrap {
                version: 1,
                cluster_id: root.cluster_id,
                node_id: intent.node_id,
                request_id: intent.launch_id,
                api_urls: t.api_urls.clone(),
                listen: t.listen.clone(),
                advertise_url: None,
                storage: t.storage.clone(),
                enrollment_token: enrollment.into(),
                tls: t.tls.clone(),
                operation_slots: t.operation_slots,
                partition_slots: t.partition_slots,
                queue_size: t.queue_size,
                partition_size: t.partition_size.clone(),
            },
        },
    })
}
impl StartCommand {
    pub(super) fn build(
        root: &Control,
        owner: Owner,
        intent_id: Uuid,
        invocation_id: Uuid,
        attached: &AttachedConfig,
    ) -> Result<Self, StorageError> {
        let wire = expected_start(root, owner, intent_id, invocation_id, attached)?;
        let bytes = configuration::bounded(&wire, MAX_BODY).map_err(config_error)?;
        let mut context = context(
            root,
            owner,
            invocation_id,
            Some(intent_id),
            ReplyAction::Start,
        )?;
        let digest = digest(&bytes);
        context.delivery = Some(DeliveryAssociation {
            binding: DeliveryBinding::Start,
            target: wire.target.ok_or_else(invalid)?,
            digest: digest.clone(),
        });
        Ok(Self {
            digest,
            bytes,
            context,
        })
    }
    #[allow(dead_code)] // Retained for byte validation; delivery consumes into_parts.
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(super) fn context(&self) -> &ReplyContext {
        &self.context
    }
    pub(super) fn digest(&self) -> &str {
        &self.digest
    }
    pub(super) fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        (self.bytes, self.context)
    }
}
fn expected_stop(
    root: &Control,
    owner: Owner,
    intent_id: Uuid,
    invocation_id: Uuid,
) -> Result<Command<StopPayload>, StorageError> {
    let _ = context(
        root,
        owner,
        invocation_id,
        Some(intent_id),
        ReplyAction::Stop,
    )?;
    let intent = root
        .intents
        .iter()
        .find(|i| i.request_id == intent_id)
        .ok_or(StorageError::Conflict)?;
    let node = root
        .nodes
        .iter()
        .find(|n| n.node_id == intent.node_id && n.launch_id == Some(intent.launch_id))
        .ok_or(StorageError::Conflict)?;
    let start = root
        .intents
        .iter()
        .find(|i| {
            i.action == Action::Start
                && i.node_id == intent.node_id
                && i.request_id == intent.launch_id
        })
        .ok_or(StorageError::Conflict)?;
    if intent.action != Action::Stop
        || intent.template_fingerprint.is_some()
        || intent.resource_id != node.resource_id
        || intent.incarnation_id != node.incarnation
        || start
            .resource_id
            .as_ref()
            .is_some_and(|r| Some(r) != intent.resource_id.as_ref())
        || intent.delivery.as_ref().is_some_and(|d| {
            d.invocation_id != invocation_id
                || d.owner_id != owner.process_id
                || d.epoch != owner.epoch
        })
    {
        return Err(StorageError::Conflict);
    }
    Ok(Command {
        version: 1,
        invocation_id,
        cluster_id: root.cluster_id,
        owner_id: owner.process_id,
        epoch: owner.epoch,
        intent_id: Some(intent_id),
        action: "stop",
        target: Some(Target {
            node_id: intent.node_id,
            request_id: intent.launch_id,
            incarnation_id: intent.incarnation_id,
        }),
        payload: StopPayload {
            resource_id: intent.resource_id.clone().ok_or(StorageError::Conflict)?,
        },
    })
}
impl StopCommand {
    #[allow(dead_code)] // C06c3 constructs records after proof-bound eligibility.
    pub(super) fn build(
        root: &Control,
        owner: Owner,
        intent_id: Uuid,
        invocation_id: Uuid,
    ) -> Result<Self, StorageError> {
        let wire = expected_stop(root, owner, intent_id, invocation_id)?;
        let bytes = configuration::bounded(&wire, MAX_BODY).map_err(config_error)?;
        let mut context = context(
            root,
            owner,
            invocation_id,
            Some(intent_id),
            ReplyAction::Stop,
        )?;
        let digest = digest(&bytes);
        context.delivery = Some(DeliveryAssociation {
            target: wire.target.ok_or_else(invalid)?,
            binding: DeliveryBinding::Stop(wire.payload.resource_id),
            digest: digest.clone(),
        });
        Ok(Self {
            digest,
            bytes,
            context,
        })
    }
    #[allow(dead_code)] // Original immutable bytes for later authorization.
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(super) fn context(&self) -> &ReplyContext {
        &self.context
    }
    pub(super) fn digest(&self) -> &str {
        &self.digest
    }
    pub(super) fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        (self.bytes, self.context)
    }
}
#[allow(dead_code)] // Reconstruction establishes no callback permission.
pub(super) fn validate_stop_bytes(raw: &[u8], root: &Control) -> Result<String, StorageError> {
    let wire: CommandWire = object(raw)?;
    let expected = expected_stop(
        root,
        Owner {
            process_id: wire.owner_id,
            epoch: wire.epoch,
        },
        wire.intent_id.ok_or_else(invalid)?,
        wire.invocation_id,
    )?;
    let payload: StopPayload = object(wire.payload.get().as_bytes())?;
    if wire.version != 1
        || wire.action != "stop"
        || wire.cluster_id != expected.cluster_id
        || wire.target != expected.target
        || payload != expected.payload
    {
        return Err(invalid());
    }
    Ok(digest(raw))
}
#[allow(dead_code)] // Later inventory caller consumes the exact bytes/context.
impl InventoryCommand {
    pub(super) fn build(
        root: &Control,
        owner: Owner,
        invocation_id: Uuid,
    ) -> Result<Self, StorageError> {
        let context = context(root, owner, invocation_id, None, ReplyAction::Inventory)?;
        let bytes = configuration::bounded(
            &Command {
                version: 1,
                invocation_id,
                cluster_id: root.cluster_id,
                owner_id: owner.process_id,
                epoch: owner.epoch,
                intent_id: None,
                action: "inventory",
                target: None,
                payload: Empty {},
            },
            MAX_BODY,
        )
        .map_err(config_error)?;
        Ok(Self {
            digest: digest(&bytes),
            bytes,
            context,
        })
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(super) fn digest(&self) -> &str {
        &self.digest
    }
    pub(super) fn into_parts(self) -> (Vec<u8>, ReplyContext) {
        (self.bytes, self.context)
    }
}
#[allow(dead_code)] // Fresh authorization must separately check current authority.
pub(super) fn validate_start_bytes(
    raw: &[u8],
    root: &Control,
    attached: &AttachedConfig,
) -> Result<String, StorageError> {
    let wire: CommandWire = object(raw)?;
    let intent_id = wire.intent_id.ok_or_else(invalid)?;
    let expected = expected_start(
        root,
        Owner {
            process_id: wire.owner_id,
            epoch: wire.epoch,
        },
        intent_id,
        wire.invocation_id,
        attached,
    )?;
    let payload: StartPayload = object(wire.payload.get().as_bytes())?;
    if wire.version != 1
        || wire.action != "start"
        || wire.cluster_id != expected.cluster_id
        || wire.target != expected.target
        || payload != expected.payload
    {
        return Err(invalid());
    }
    Ok(digest(raw))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Success {
    version: u64,
    #[serde(deserialize_with = "member::uuid")]
    invocation_id: Uuid,
    result: Box<RawValue>,
}
record!(Failure {
    version: u64,
    #[serde(deserialize_with="member::nullable_uuid")] invocation_id: Option<Uuid>,
    error: FailureDetail,
});
record!(FailureDetail {
    code: String,
    outcome: String
});
#[allow(dead_code)] // Reply acceptance is local evidence, never a metadata commit.
pub(super) fn decode_reply(
    status: u16,
    raw: &[u8],
    ctx: &ReplyContext,
) -> Result<CallbackReply, StorageError> {
    if status != 200 {
        let f: Failure = object(raw)?;
        let code = f.error.code.as_str();
        let allowed = matches!(
            (status, code),
            (400, "invalid_command")
                | (401, "unauthorized")
                | (
                    409,
                    "callback_in_progress" | "identity_conflict" | "ownership_changed"
                )
                | (413, "limit_exceeded")
                | (429, "callback_capacity")
                | (503, "bridge_not_active" | "ownership_unavailable")
                | (502, "callback_failed" | "invalid_callback_result")
                | (504, "callback_timeout")
        );
        let outcome = f.error.outcome.as_str();
        let valid_outcome = match code {
            "callback_in_progress" => outcome == "unknown",
            "callback_failed" | "invalid_callback_result" => {
                outcome
                    == if ctx.action == ReplyAction::Inventory {
                        "not_invoked"
                    } else {
                        "unknown"
                    }
            }
            "callback_timeout" | "bridge_not_active" => {
                outcome == "not_invoked"
                    || (outcome == "unknown" && ctx.action != ReplyAction::Inventory)
            }
            _ => outcome == "not_invoked",
        };
        if f.version != 1 || !allowed || !valid_outcome {
            return Err(invalid());
        }
        let outcome = if f.invocation_id == Some(ctx.invocation_id) {
            if outcome == "not_invoked" {
                DeliveryOutcome::NotInvoked
            } else {
                DeliveryOutcome::Unknown
            }
        } else if f.invocation_id.is_none() && matches!(status, 400 | 401 | 413) {
            DeliveryOutcome::Unknown
        } else {
            return Err(invalid());
        };
        return Ok(CallbackReply::Failure {
            code: f.error.code,
            outcome,
        });
    }
    let s: Success = object(raw)?;
    if s.version != 1 || s.invocation_id != ctx.invocation_id {
        return Err(invalid());
    }
    let raw = s.result.get().as_bytes();
    match ctx.action {
        ReplyAction::Start => {
            let s: String = parse(raw)?;
            if !resource(&s) {
                return Err(invalid());
            }
            Ok(CallbackReply::Started(s))
        }
        ReplyAction::Stop => {
            parse::<()>(raw)?;
            Ok(CallbackReply::Stopped)
        }
        ReplyAction::Inventory => {
            let records: Vec<InventoryRecordWire> = parse(raw)?;
            if records.len() > MAX_RECORDS {
                return Err(StorageError::LimitExceeded);
            }
            let (mut resources, mut nodes, mut requests) =
                (HashSet::new(), HashSet::new(), HashSet::new());
            for r in &records {
                if r.cluster_id != ctx.cluster_id
                    || !resource(&r.resource_id)
                    || !matches!(
                        r.state.as_str(),
                        "starting" | "running" | "stopping" | "terminated"
                    )
                    || !resources.insert(&r.resource_id)
                    || (r.state != "terminated"
                        && (!nodes.insert(r.node_id) || !requests.insert(r.request_id)))
                {
                    return Err(invalid());
                }
            }
            Ok(CallbackReply::Inventory(InventoryEvidence {
                cluster_id: ctx.cluster_id,
                owner: ctx.owner,
                invocation_id: ctx.invocation_id,
                observation: ctx.observation.clone(),
                records: records
                    .into_iter()
                    .map(|wire| InventoryRecord { wire })
                    .collect(),
            }))
        }
    }
}
#[cfg(test)]
mod tests;

#[allow(dead_code)] // C05d reads only validated complete observations.
impl InventoryEvidence {
    pub(super) fn owner(&self) -> Owner {
        self.owner
    }
    pub(super) fn invocation_id(&self) -> Uuid {
        self.invocation_id
    }
    pub(super) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(super) fn records(&self) -> &[InventoryRecord] {
        &self.records
    }
}
#[allow(dead_code)] // C05d compares these records without granting authority.
impl InventoryRecord {
    pub(super) fn resource_id(&self) -> &str {
        &self.wire.resource_id
    }
    pub(super) fn cluster_id(&self) -> Uuid {
        self.wire.cluster_id
    }
    pub(super) fn node_id(&self) -> Uuid {
        self.wire.node_id
    }
    pub(super) fn request_id(&self) -> Uuid {
        self.wire.request_id
    }
    pub(super) fn state(&self) -> &str {
        &self.wire.state
    }
}

// Only validated serialized command construction populates this closed association.
enum DeliveryBinding {
    Start,
    Stop(String),
}
struct DeliveryAssociation {
    binding: DeliveryBinding,
    target: Target,
    digest: String,
}
impl ReplyContext {
    pub(super) fn intent_id(&self) -> Option<Uuid> {
        self.intent_id
    }
    pub(super) fn matches_delivery(
        &self,
        root: &Control,
        owner: Owner,
        intent_id: Uuid,
    ) -> Result<(), StorageError> {
        OwnershipObservation::new(root)?;
        let a = self.delivery.as_ref().ok_or(StorageError::Conflict)?;
        let i = root
            .intents
            .iter()
            .find(|i| i.request_id == intent_id)
            .ok_or(StorageError::Conflict)?;
        if self.cluster_id != root.cluster_id
            || self.owner != owner
            || self.intent_id != Some(intent_id)
            || root.controller.lease_state != LeaseState::Held
            || root.controller.owner_id != Some(owner.process_id)
            || root.controller.epoch != owner.epoch
            || i.state != member::IntentState::InProgress
            || i.node_id != a.target.node_id
            || i.launch_id != a.target.request_id
            || i.delivery.as_ref().is_none_or(|d| {
                d.invocation_id != self.invocation_id
                    || d.owner_id != owner.process_id
                    || d.epoch != owner.epoch
                    || d.command_sha256 != a.digest
            })
        {
            return Err(StorageError::Conflict);
        }
        match &a.binding {
            DeliveryBinding::Start => {
                if self.action != ReplyAction::Start
                    || i.action != Action::Start
                    || a.target.incarnation_id.is_some()
                {
                    return Err(StorageError::Conflict);
                }
            }
            DeliveryBinding::Stop(resource) => {
                let n = root
                    .nodes
                    .iter()
                    .find(|n| {
                        n.node_id == a.target.node_id && n.launch_id == Some(a.target.request_id)
                    })
                    .ok_or(StorageError::Conflict)?;
                if self.action != ReplyAction::Stop
                    || i.action != Action::Stop
                    || i.incarnation_id != a.target.incarnation_id
                    || n.incarnation != a.target.incarnation_id
                    || i.resource_id.as_ref() != Some(resource)
                    || n.resource_id.as_ref() != Some(resource)
                {
                    return Err(StorageError::Conflict);
                }
            }
        }
        Ok(())
    }
}
