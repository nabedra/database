//! The current system coordinator drives the existing ready-set and release
//! transition after Control accepts a data worker's scoped join request.
use super::*;
use crate::oltp_replication::authority::control::{InventoryPage, ReadyPending, ServiceAction};

pub(super) struct JoinOwner {
    pub operator: Arc<LocalOperatorPermit>,
    pub timestamps: GroupIdentity,
    pub manager: Arc<HostedGroupManager>,
    pub rpc: Arc<ControlClient>,
}

impl JoinOwner {
    pub(super) async fn advance(
        &self,
        control: &ControlHandle,
        pending: &ReadyPending,
    ) -> Result<(), ProtocolError> {
        let ticket = pending.transition_id;
        match pending.phase {
            ReadyPhase::Building => {
                let bytes = control
                    .ready_page_read(pending.expected, pending.source_after)
                    .await?;
                let page: InventoryPage<WorkerIdentity> =
                    serde_json::from_slice(bytes.as_ref()).map_err(|_| unavailable())?;
                let mut members = page.items.into_inner();
                if !pending.target_seen
                    && (page.done
                        || members
                            .last()
                            .is_some_and(|after| pending.target.worker_id < after.worker_id))
                {
                    let index = members
                        .partition_point(|member| member.worker_id < pending.target.worker_id);
                    members.insert(index, pending.target);
                    members.truncate(128);
                }
                if members.is_empty() {
                    accepted(
                        control
                            .seal_ready_local(
                                &self.operator,
                                id(ticket, b"data-worker-seal"),
                                ticket,
                            )
                            .await?
                            .reply,
                    )
                } else {
                    let operation = super::super::super::authority_fence::transition_id(
                        "data-worker-ready-page",
                        &(ticket, pending.root.count()),
                    )?;
                    let members = Page::new(members).map_err(|_| unavailable())?;
                    accepted(
                        control
                            .ready_page_local(&self.operator, operation, ticket, members)
                            .await?
                            .reply,
                    )
                }
            }
            ReadyPhase::AwaitingTimestampFence => {
                let next = pending.next.ok_or_else(unavailable)?;
                let clock = TimestampClient::new(
                    self.timestamps.clone(),
                    self.manager.resources().clone(),
                    self.manager.client(),
                    self.rpc.clone(),
                    ServiceAction::ControlFence {
                        transition_id: ticket,
                    },
                )?;
                let receipt = clock.fence_ready(ticket, pending.expected, next).await?;
                accepted(
                    control
                        .timestamp_fenced(id(ticket, b"data-worker-timestamp-fenced"), receipt)
                        .await?
                        .reply,
                )
            }
            ReadyPhase::AwaitingQueueProof => Ok(()),
        }
    }
}
