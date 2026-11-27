use crate::{
    oltp::{
        mvcc::store::ParticipantHandle,
        pool::ByteClass,
    },
    oltp_replication::{
        authority::{
            control::{
                ControlClient, ControlHandle, ControlReply, FencePending, GroupRecord, InventoryPage,
                LocalOperatorPermit, ServiceAction,
            },
            status::StatusReply,
            timestamp::TimestampReply,
        },
        manager::HostedGroupManager,
        service_rpc::{ParticipantClient, StatusClient, TimestampClient},
        types::GroupKind,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use std::sync::Arc;
use uuid::Uuid;

pub(super) fn transition_id<T: serde::Serialize>(
    stage: &str,
    input: &T,
) -> Result<Uuid, ProtocolError> {
    let digest = crate::oltp_replication::owned::serialized_digest(&(stage, input))?;
    let raw = digest.to_string();
    // A deterministic operation identity makes lost replies resumable.
    Uuid::parse_str(&raw[..32]).map_err(|_| unknown())
}

/// CONTROL-4: concurrent group fences per inventory page. Tests keep one at a
/// time, so their checkpoints between groups stay ordered.
#[cfg(not(test))]
const FENCE_FAN_OUT: usize = 16;
#[cfg(test)]
const FENCE_FAN_OUT: usize = 1;

pub(super) async fn fence_groups(
    control: &ControlHandle,
    manager: &HostedGroupManager,
    rpc: &Arc<ControlClient>,
    fence: &FencePending,
) -> Result<(), ProtocolError> {
    let shared = manager.resources();
    let mut after = None;
    loop {
        let bytes = control.groups(fence.group_inventory_revision, after).await?;
        let _decoded = shared.reserve_scratch(
            ByteClass::Progress,
            crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?,
        )?;
        let page: InventoryPage<GroupRecord> =
            serde_json::from_slice(bytes.as_slice()).map_err(|_| unknown())?;
        drop(bytes);
        if page.revision != fence.group_inventory_revision || (!page.done && page.after <= after) {
            return Err(unknown());
        }
        // CONTROL-4: fence the groups of a page concurrently, at most
        // `FENCE_FAN_OUT` at a time, instead of one round trip and one Control
        // commit after another. Each group's fence and record stay ordered.
        {
            use futures_util::StreamExt as _;
            let mut groups = page.items.iter();
            let mut running = futures_util::stream::FuturesUnordered::new();
            loop {
                while running.len() < FENCE_FAN_OUT {
                    let Some(group) = groups.next() else { break };
                    running.push(fence_group(control, manager, rpc, fence, group));
                }
                match running.next().await {
                    Some(result) => result?,
                    None => break,
                }
            }
        }
        if page.done {
            return Ok(());
        }
        after = page.after;
    }
}

async fn fence_group(
    control: &ControlHandle,
    manager: &HostedGroupManager,
    rpc: &Arc<ControlClient>,
    fence: &FencePending,
    group: &GroupRecord,
) -> Result<(), ProtocolError> {
    let action = ServiceAction::ControlFence {
        transition_id: fence.transition_id,
    };
    let old = fence.expected.view.authority();
    let next = fence.next.view.authority();
    let shared = &manager.resources();
            let identity = &group.identity;
            let receipt = match &identity.kind {
                GroupKind::Control => return Ok(()),
                GroupKind::Timestamps => {
                    let client = TimestampClient::new(
                        identity.clone(),
                        Arc::clone(shared),
                        manager.client(),
                        rpc.clone(),
                        action,
                    )?;
                    let reply = client
                        .fence(fence.transition_id, Some(old), next, fence.next)
                        .await?;
                    let TimestampReply::Fenced { receipt, .. } = reply.reply else {
                        return Err(unknown());
                    };
                    receipt
                }
                GroupKind::Status { .. } => {
                    let client = StatusClient::new(
                        identity.clone(),
                        Arc::clone(shared),
                        manager.client(),
                        rpc.clone(),
                        action,
                    )?;
                    let reply = client.fence(fence.transition_id, Some(old), next).await?;
                    let StatusReply::Fenced(receipt) = reply.reply else {
                        return Err(unknown());
                    };
                    receipt
                }
                GroupKind::Catalog | GroupKind::Data { .. } => {
                    let client = ParticipantClient::new(
                        identity.clone(),
                        Arc::clone(shared),
                        manager.client(),
                        rpc.clone(),
                        action,
                    )?;
                    let participant =
                        ParticipantHandle::remote(identity.clone(), Arc::clone(shared), client)?;
                    participant
                        .fence_current(fence.transition_id, old, next)
                        .await?
                }
            };
            #[cfg(test)]
            let observed_fence = receipt.clone();
            let op = transition_id("record-fence", &(fence.transition_id, identity))?;
            accepted(control.record_group_fence(op, receipt).await?.reply)?;
            tracing::debug!(transition = %fence.transition_id, group = %identity.group_id, "recorded registered group fence");
            #[cfg(test)]
            if fence.cold {
                super::recovery_tests::cold_fence_checkpoint(&observed_fence).await;
                super::recovery_tests::pause("cold_group_fenced", fence.transition_id).await;
            }
    Ok(())
}

pub(super) async fn publish_fence(
    control: &ControlHandle,
    operator: &LocalOperatorPermit,
    fence: &FencePending,
) -> Result<(), ProtocolError> {
    accepted(
        control
            .publish_fence_local(
                operator,
                transition_id("publish-fence", &fence.transition_id)?,
                fence.transition_id,
            )
            .await?
            .reply,
    )?;
    tracing::debug!(transition = %fence.transition_id, "published registered group fence");
    Ok(())
}

fn accepted(reply: ControlReply) -> Result<(), ProtocolError> {
    match reply {
        ControlReply::Rejected(_) => Err(unknown()),
        _ => Ok(()),
    }
}

fn unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "registered group fence requires reconciliation",
        Outcome::Unknown,
    )
}
