//! Pure administration preparation. No lookup, publication or controller authority.
use super::{
    configuration::{self, ConfigurationError},
    member::uuid,
    state::{Control, ControlError, Policy, PolicyMode, Provisioning},
};
use crate::{
    protocol::deserialize_object,
    storage::{ControlDecision, MAX_CONTROL_BODY_BYTES, StorageError},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use uuid::Uuid;

/// Caller must authorize and strictly admit the original request, then prove
/// complete reachable-history absence before calling, even with exhausted counters.
/// Runtime supplies a globally fresh proposal UUID. This checks local freshness.
pub(crate) fn admin_candidate(
    root: &Control,
    request_id: Uuid,
    exact_request: &[u8],
    proposal_id: Uuid,
) -> Result<(Control, ControlDecision), StorageError> {
    // Check bytes/depth before serde or root encoding allocates output.
    configuration::input(exact_request, MAX_CONTROL_BODY_BYTES).map_err(|error| match error {
        ConfigurationError::LimitExceeded => StorageError::LimitExceeded,
        ConfigurationError::InvalidRecord | ConfigurationError::UnsupportedStorage => {
            StorageError::InvalidCatalog
        }
    })?;
    let request: Request = object(exact_request)?;
    if request.request_id != request_id
        || proposal_id == root.change_id
        || Some(proposal_id) == root.admin_head
    {
        return Err(StorageError::InvalidCatalog);
    }
    let target = match request.operation.as_str() {
        "cluster.scale" => {
            let payload: Scale = object(request.payload.get().as_bytes())?;
            (PolicyMode::Fixed, payload.nodes, payload.nodes)
        }
        "cluster.autoscale" => {
            let payload: Autoscale = object(request.payload.get().as_bytes())?;
            (PolicyMode::Autoscale, payload.min_nodes, payload.max_nodes)
        }
        _ => return Err(StorageError::InvalidCatalog),
    };
    root.encode().map_err(control_error)?;
    let mut policy = root.policy.clone();
    let error = if target.2 > 1024
        || target.1 > target.2
        || (target.0 == PolicyMode::Autoscale && target.1 == 0)
    {
        Some(TerminalError {
            code: "validation_error",
            message: "Invalid policy bounds",
            outcome: "not_applied",
        })
    } else if target.0 == PolicyMode::Autoscale && root.provisioning == Provisioning::External {
        Some(TerminalError {
            code: "unsupported_capability",
            message: "Autoscale requires managed provisioning",
            outcome: "not_applied",
        })
    } else {
        policy.mode = target.0;
        policy.min_nodes = target.1;
        policy.max_nodes = target.2;
        policy.desired = policy.desired.clamp(target.1, target.2);
        // Revision is unchanged during comparison, so this compares precisely
        // mode, desired and both bounds, independently of the root/admin counters.
        if policy != root.policy {
            policy.revision = increment(policy.revision)?;
        }
        None
    };
    let mut candidate = root.clone();
    candidate.policy = policy;
    candidate.revision = increment(root.revision)?;
    candidate.admin_seq = increment(root.admin_seq)?;
    candidate.change_id = proposal_id;
    candidate.admin_head = Some(proposal_id);
    let response_status = if error.is_some() { 400 } else { 200 };
    // These responses contain only fixed-width scalars and static strings.
    let response_body = match error {
        Some(error) => serde_json::to_string(&Failure { request_id, error }),
        None => serde_json::to_string(&Success {
            request_id,
            data: &candidate.policy,
        }),
    }
    .map_err(|_| StorageError::InvalidCatalog)?;
    if response_body.len() > MAX_CONTROL_BODY_BYTES {
        return Err(StorageError::LimitExceeded);
    }
    let decision = ControlDecision {
        version: 1,
        id: proposal_id,
        parent: root.admin_head,
        admin_seq: candidate.admin_seq,
        cluster_id: root.cluster_id,
        base_root_revision: root.revision,
        published_root_revision: candidate.revision,
        request_id,
        request_body: std::str::from_utf8(exact_request)
            .map_err(|_| StorageError::InvalidCatalog)?
            .to_owned(),
        response_status,
        response_body,
    };
    candidate.encode().map_err(control_error)?;
    decision.encode()?;
    Ok((candidate, decision))
}

fn increment(value: u64) -> Result<u64, StorageError> {
    value.checked_add(1).ok_or(StorageError::LimitExceeded)
}
fn control_error(error: ControlError) -> StorageError {
    match error {
        ControlError::InvalidRecord => StorageError::InvalidCatalog,
        ControlError::LimitExceeded => StorageError::LimitExceeded,
    }
}
fn object<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = deserialize_object(&mut deserializer).map_err(|_| StorageError::InvalidCatalog)?;
    deserializer
        .end()
        .map_err(|_| StorageError::InvalidCatalog)?;
    Ok(value)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(deserialize_with = "uuid")]
    request_id: Uuid,
    operation: String,
    payload: Box<RawValue>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scale {
    nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Autoscale {
    min_nodes: u64,
    max_nodes: u64,
}
#[derive(Serialize)]
struct Success<'a> {
    request_id: Uuid,
    data: &'a Policy,
}
#[derive(Serialize)]
struct Failure {
    request_id: Uuid,
    error: TerminalError,
}
#[derive(Serialize)]
struct TerminalError {
    code: &'static str,
    message: &'static str,
    outcome: &'static str,
}

#[cfg(test)]
mod tests;
