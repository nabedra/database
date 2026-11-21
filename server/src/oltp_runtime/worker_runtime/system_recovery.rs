use super::*;
use crate::oltp_replication::{
    authority::control::{
        GroupRecord, InventoryPage, MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup,
        ReplacementIntent, SYSTEM_REPLACEMENT_CAPABILITY, State,
    },
    manager::system_recovery::{QuorumRequest, reachable},
};
use crate::oltp_runtime::{
    bootstrap::registered::ready_members,
    worker_endpoint::{PeerReachability, QueuePeerClient},
};
use serde::Deserialize;
use std::time::Duration;

const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplacementCheck {
    run_id: Uuid,
    previous: WorkerIdentity,
}
impl ReplacementCheck {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.run_id.is_nil() || self.previous.validate().is_err() {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "invalid run or lost worker identity",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
}

struct CheckedGroups {
    state: State,
    groups: Vec<(GroupRecord, [ReplicaMember; 3])>,
    _scratch: ScratchLease,
}

impl WorkerRuntime {
    fn require_system_replacement_operator(&self) -> Result<(), ProtocolError> {
        if self.services.replication_factor != 3 {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "system replacement requires a registered RF3 worker",
                Outcome::NotApplied,
            ));
        }
        if !self.services.control.is_operator(&self.services.local) {
            return Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "system replacement requires a registered operator",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }

    pub(crate) async fn enable_system_replacement(
        &self,
        request_id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        self.require_system_replacement_operator()?;
        let position = tokio::time::timeout(
            CHECK_TIMEOUT,
            self.services.control.enable_system_replacement(
                &self.services.manager,
                &self.services.operator,
                request_id,
            ),
        )
        .await
        .map_err(|_| quorum_unavailable())??;
        Ok(response::encode_success(
            request_id,
            &serde_json::json!({
                "capability": SYSTEM_REPLACEMENT_CAPABILITY,
                "applied_at": position,
            }),
            true,
        ))
    }

    pub(crate) async fn prepare_system_replacement(
        &self,
        intent: ReplacementIntent,
    ) -> Result<EncodedResponse, ProtocolError> {
        self.require_system_replacement_operator()?;
        let control = &self.services.control;
        let request_id = intent.id;
        if let Some(record) = control
            .system_replacement_record(intent.previous.worker_id)
            .await?
        {
            if record.intent != intent {
                return Err(conflict(
                    "a different intent already names this lost worker",
                ));
            }
            return Ok(response::encode_success(request_id, &record, true));
        }
        let check = ReplacementCheck {
            run_id: intent.run_id,
            previous: intent.previous,
        };
        let checked = tokio::time::timeout(CHECK_TIMEOUT, self.check_surviving_groups(&check))
            .await
            .map_err(|_| quorum_unavailable())??;
        let groups = checked
            .groups
            .into_iter()
            .map(|(record, old)| ReplacementGroup::new(record, old, &intent))
            .collect::<Result<Vec<_>, _>>()?;
        let record = tokio::time::timeout(
            CHECK_TIMEOUT,
            control.prepare_system_replacement(
                &self.services.manager,
                &self.services.operator,
                intent,
                &checked.state,
                groups,
            ),
        )
        .await
        .map_err(|_| unknown())??;
        Ok(response::encode_success(request_id, &record, true))
    }

    pub(crate) async fn system_replacement_status(
        &self,
        request_id: Uuid,
        check: ReplacementCheck,
    ) -> Result<EncodedResponse, ProtocolError> {
        self.require_system_replacement_operator()?;
        let control = &self.services.control;
        let record = control
            .system_replacement_record(check.previous.worker_id)
            .await?
            .filter(|record| {
                record.intent.previous == check.previous && record.intent.run_id == check.run_id
            })
            .ok_or_else(|| conflict("system replacement intent or run differs from Control"))?;
        let (_, mounted, complete) = control
            .replacement_record_local(record.intent.id)
            .await?
            .ok_or_else(quorum_unavailable)?;
        let scratch = self
            .services
            .shared
            .reserve_scratch(ByteClass::Progress, record.groups.len() * 2048)?;
        let mut groups = Vec::new();
        for plan in &record.groups {
            let routes = self
                .services
                .control_client
                .replica_routes(&plan.expected.identity)
                .await?;
            groups.push(serde_json::json!({"record": routes.record, "members": routes.members}));
        }
        let response = response::encode_success(
            request_id,
            &serde_json::json!({"id": record.intent.id, "mounted": mounted, "complete": complete, "groups": groups}),
            false,
        );
        drop(scratch);
        Ok(response)
    }

    pub(crate) async fn check_system_replacement(
        &self,
        request_id: Uuid,
        check: ReplacementCheck,
    ) -> Result<EncodedResponse, ProtocolError> {
        self.require_system_replacement_operator()?;
        let checked = tokio::time::timeout(CHECK_TIMEOUT, self.check_surviving_groups(&check))
            .await
            .map_err(|_| quorum_unavailable())??;
        let groups = checked
            .groups
            .iter()
            .map(|(record, _)| record.identity.group_id)
            .collect::<Vec<_>>();
        Ok(response::encode_success(
            request_id,
            &serde_json::json!({
                "run_id": check.run_id,
                "previous": check.previous,
                "groups": groups,
            }),
            false,
        ))
    }

    async fn check_surviving_groups(
        &self,
        check: &ReplacementCheck,
    ) -> Result<CheckedGroups, ProtocolError> {
        let control = &self.services.control;
        let state = control.inspect().await?.ok_or_else(quorum_unavailable)?;
        if state.ready.view.run_id != check.run_id
            || !control
                .worker(check.previous.worker_id)
                .await?
                .is_some_and(|enrollment| WorkerIdentity::from(&enrollment) == check.previous)
        {
            return Err(conflict(
                "lost worker incarnation or run differs from Control",
            ));
        }

        self.probe_lost_worker(check.previous, &state).await?;
        if state.pending.is_some()
            || state.restart.is_some()
            || state.invalid_ready != 0
            || state
                .owner
                .is_none_or(|owner| owner.worker_id == check.previous.worker_id)
            || ready_members(control, state.ready)
                .await?
                .iter()
                .any(|member| member.worker_id == check.previous.worker_id)
        {
            return Err(conflict("lost worker has not left the ready set"));
        }

        let scratch = self
            .services
            .shared
            .reserve_scratch(ByteClass::Progress, MAX_SYSTEM_REPLACEMENT_GROUPS * 2048)?;
        let mut groups = Vec::new();
        let mut after = None;
        loop {
            let raw = control
                .groups(state.group_inventory_revision, after)
                .await?;
            let (page, _page_memory): (InventoryPage<GroupRecord>, _) = {
                let budget = crate::oltp::mvcc::json_decode_budget(raw.as_ref())?;
                let memory = self
                    .services
                    .shared
                    .reserve_scratch(ByteClass::Progress, budget)?;
                self.services
                    .shared
                    .execute(ExecutionClass::Control, 0, 0, move || {
                        let page = serde_json::from_slice(raw.as_ref())
                            .map_err(|_| quorum_unavailable())?;
                        Ok((page, memory))
                    })
                    .await?
            };
            for record in page.items.iter() {
                let route = self
                    .services
                    .control_client
                    .replica_routes(&record.identity)
                    .await?;
                if route.record != *record {
                    return Err(quorum_unavailable());
                }
                if route
                    .members
                    .iter()
                    .any(|member| member.worker_id == check.previous.worker_id)
                {
                    if groups.len() >= MAX_SYSTEM_REPLACEMENT_GROUPS {
                        return Err(ProtocolError::new(
                            ErrorCode::UnsupportedOperation,
                            "system replacement check exceeds the affected group limit",
                            Outcome::NotApplied,
                        ));
                    }
                    let voters: [ReplicaMember; 3] = route
                        .members
                        .as_slice()
                        .try_into()
                        .map(|slice: &[ReplicaMember; 3]| slice.clone())
                        .map_err(|_| quorum_unavailable())?;
                    self.check_group_quorum(record, &voters, check.previous)
                        .await?;
                    groups.push((record.clone(), voters));
                }
            }
            if page.done {
                break;
            }
            if page.after.is_none() || page.after <= after {
                return Err(quorum_unavailable());
            }
            after = page.after;
        }
        if !groups
            .iter()
            .any(|(record, _)| record.identity == *control.identity())
        {
            return Err(conflict(
                "lost worker is outside the current Control voters",
            ));
        }

        self.probe_lost_worker(check.previous, &state).await?;
        let end = control.inspect().await?.ok_or_else(quorum_unavailable)?;
        if end.ready != state.ready
            || end.owner != state.owner
            || end.enrollment_revision != state.enrollment_revision
            || end.group_inventory_revision != state.group_inventory_revision
            || end.pending.is_some()
        {
            return Err(quorum_unavailable());
        }
        Ok(CheckedGroups {
            state,
            groups,
            _scratch: scratch,
        })
    }

    async fn probe_lost_worker(
        &self,
        previous: WorkerIdentity,
        state: &State,
    ) -> Result<(), ProtocolError> {
        if previous == self.services.local {
            return Err(reachable());
        }
        let peer = QueuePeerClient::new(
            self.services.manager.client(),
            self.services.shared.clone(),
            self.services.control.identity().clone(),
        )?;
        match peer
            .probe(previous, self.services.scope.clone(), state.ready)
            .await
        {
            PeerReachability::Unreachable => Ok(()),
            PeerReachability::Reachable => Err(reachable()),
            PeerReachability::Indeterminate => Err(quorum_unavailable()),
        }
    }

    async fn check_group_quorum(
        &self,
        record: &GroupRecord,
        voters: &[ReplicaMember; 3],
        previous: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        let control = &self.services.control;
        let manager = &self.services.manager;
        for survivor in voters
            .iter()
            .filter(|voter| voter.worker_id != previous.worker_id)
        {
            let request = QuorumRequest {
                group: record.identity.clone(),
                voters: voters.clone(),
                lost: previous.worker_id,
                leader: survivor.node_id,
                minimum: record.membership_position,
            };
            let result = if survivor.worker_id == self.services.local.worker_id {
                manager.surviving_quorum(&request).await
            } else {
                let enrolled = control
                    .worker(survivor.worker_id)
                    .await?
                    .ok_or_else(quorum_unavailable)?;
                manager
                    .remote_surviving_quorum(
                        control.identity(),
                        WorkerIdentity::from(&enrolled),
                        &request,
                    )
                    .await
            };
            match result {
                Ok(_) => return Ok(()),
                Err(error) if error.code == ErrorCode::Conflict => return Err(error),
                Err(_) => {}
            }
        }
        tracing::debug!(group_id = %record.identity.group_id, "system recovery requires a surviving quorum");
        Err(quorum_unavailable())
    }
}

fn conflict(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}

fn quorum_unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "could not verify a surviving RF3 quorum for system replacement",
        Outcome::NotApplied,
    )
}
