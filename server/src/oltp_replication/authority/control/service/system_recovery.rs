use super::*;
use crate::oltp_replication::{
    authority::control::{
        ReplacementGroup, ReplacementIntent, ReplacementRecord, SystemRecoveryCommand,
        machine::system_recovery::{ACTIVATION, by_id, by_worker},
    },
    manager::{HostedGroupManager, ManagedControl},
    owned::serialized_digest,
};

impl ControlHandle {
    pub(crate) async fn system_replacement_activation(
        &self,
    ) -> Result<Option<AppliedPosition>, ProtocolError> {
        let raw = self
            .read(|view| {
                let activation = load::<AppliedPosition>(view, ACTIVATION)?;
                view.encode(&activation)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn enable_system_replacement(
        &self,
        manager: &HostedGroupManager,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
    ) -> Result<AppliedPosition, ProtocolError> {
        self.local_operator(
            permit,
            operation_id,
            machine::hash(b"enable-system-replacement"),
        )?;
        if let Some(position) = self.system_replacement_activation().await? {
            return Ok(position);
        }
        let running = manager.local_control(self.identity())?;
        let stable = running.observe_stable_membership().await?;
        if stable.replicas().len() != 3 {
            return Err(unavailable(
                "system replacement capability requires three Control voters",
            ));
        }
        let state = self.inspect().await?.ok_or_else(stale)?;
        let group = self
            .group(self.identity().group_id)
            .await?
            .ok_or_else(stale)?;
        let expected_root = machine::group_root(
            3,
            stable
                .replicas()
                .iter()
                .fold(machine::group_seed(), machine::group_step),
        );
        if group.replica_count != 3 || group.replica_root != expected_root {
            return Err(stale());
        }
        let mut voters = Vec::with_capacity(3);
        for member in stable.replicas() {
            let identity = self.worker(member.worker_id).await?.ok_or_else(stale)?;
            let identity = WorkerIdentity::from(&identity);
            if identity.worker_id != permit.local.worker_id {
                manager
                    .verify_recovery_advert(self.identity(), identity)
                    .await?;
            } else if identity != permit.local {
                return Err(stale());
            }
            voters.push(identity);
        }
        let voters: [WorkerIdentity; 3] = voters.try_into().map_err(|_| stale())?;
        let evidence = serialized_digest(&(&group, voters, state.enrollment_revision))?;
        let authorization = self.local_operator(permit, operation_id, evidence)?;
        // The same local leader writes the activation entry. Recheck the
        // committed configuration after gathering the authenticated adverts.
        let current = running.observe_stable_membership().await?;
        if current.replicas() != stable.replicas() || current.applied_at() != stable.applied_at() {
            return Err(stale());
        }
        let reply = self
            .propose_system_recovery(
                &running,
                operation_id,
                SystemRecoveryCommand::Enable {
                    group,
                    voters,
                    enrollment_revision: state.enrollment_revision,
                    authorization,
                },
            )
            .await?;
        match reply {
            ControlReply::SystemReplacementEnabled(position) => Ok(position),
            _ => Err(stale()),
        }
    }

    pub(crate) async fn system_replacement_record(
        &self,
        worker: Uuid,
    ) -> Result<Option<ReplacementRecord>, ProtocolError> {
        let raw = self
            .read(move |view| {
                let record = if let Some(id) = load::<Uuid>(view, &by_worker(worker))? {
                    Some(load::<ReplacementRecord>(view, &by_id(id))?.ok_or_else(stale)?)
                } else {
                    None
                };
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn prepare_system_replacement(
        &self,
        manager: &HostedGroupManager,
        permit: &LocalOperatorPermit,
        intent: ReplacementIntent,
        state: &State,
        groups: Vec<ReplacementGroup>,
    ) -> Result<ReplacementRecord, ProtocolError> {
        let running = manager.local_control(self.identity())?;
        // The intent owns its durable operator ID. A separate log operation
        // ID lets concurrent identical callers reach the same intent record.
        let operation_id = Uuid::new_v4();
        let authorization = self.local_operator(
            permit,
            operation_id,
            serialized_digest(&(
                &intent,
                state.ready,
                state.enrollment_revision,
                state.group_inventory_revision,
                &groups,
            ))?,
        )?;
        if self.system_replacement_activation().await?.is_none() {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "enable control.system-replacement.v1 before a voter is lost",
                Outcome::NotApplied,
            ));
        }
        let reply = self
            .propose_system_recovery(
                &running,
                operation_id,
                SystemRecoveryCommand::Prepare {
                    intent,
                    ready: state.ready,
                    enrollment_revision: state.enrollment_revision,
                    group_inventory_revision: state.group_inventory_revision,
                    groups,
                    authorization,
                },
            )
            .await?;
        match reply {
            ControlReply::SystemReplacementPrepared(record) => Ok(*record),
            ControlReply::Rejected(Fault::Conflict) => Err(ProtocolError::new(
                ErrorCode::Conflict,
                "another replacement intent owns this identity",
                Outcome::NotApplied,
            )),
            ControlReply::Rejected(Fault::Invalid) => Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "replacement worker, certificate, mount or topology is invalid",
                Outcome::NotApplied,
            )),
            ControlReply::Rejected(Fault::Unauthorized) => Err(unauthorized()),
            _ => Err(stale()),
        }
    }

    async fn propose_system_recovery(
        &self,
        running: &ManagedControl,
        operation_id: Uuid,
        command: SystemRecoveryCommand,
    ) -> Result<ControlReply, ProtocolError> {
        let command = OwnedCommand::new_owned(
            ControlCommand::SystemRecovery {
                operation_id,
                command: Box::new(command),
            },
            self.resources(),
            ByteClass::Progress,
        )?;
        Ok(running.propose(command).await?.reply)
    }
}
