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

pub(super) async fn fence_groups(
    control: &ControlHandle,
    manager: &HostedGroupManager,
    rpc: &Arc<ControlClient>,
    fence: &FencePending,
) -> Result<(), ProtocolError> {
    let action = ServiceAction::ControlFence {
        transition_id: fence.transition_id,
    };
    let old = fence.expected.view.authority();
    let next = fence.next.view.authority();
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
        for group in page.items.iter() {
            let identity = &group.identity;
            let receipt = match &identity.kind {
                GroupKind::Control => continue,
                GroupKind::Timestamps => {
                    let client = TimestampClient::new(
                        identity.clone(),
                        shared.clone(),
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
                        shared.clone(),
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
                        shared.clone(),
                        manager.client(),
                        rpc.clone(),
                        action,
                    )?;
                    let participant =
                        ParticipantHandle::remote(identity.clone(), shared.clone(), client)?;
                    participant
                        .fence_current(fence.transition_id, old, next)
                        .await?
                }
            };
            let op = transition_id("record-fence", &(fence.transition_id, identity))?;
            accepted(control.record_group_fence(op, receipt).await?.reply)?;
            #[cfg(test)]
            if fence.cold {
                super::recovery_tests::pause("cold_group_fenced", fence.transition_id).await;
            }
        }
        if page.done {
            return Ok(());
        }
        after = page.after;
    }
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
    )
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
