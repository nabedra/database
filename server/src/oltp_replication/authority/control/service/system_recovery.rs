use super::*;
use crate::oltp_replication::{
    authority::control::{
        ReplacementGroup, ReplacementIntent, ReplacementMountProof, ReplacementRecord,
        SystemRecoveryCommand,
        machine::system_recovery::{
            ACTIVATION, ACTIVE, EXECUTION, by_id, by_worker, published, revoked_id,
        },
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
        if let Some(position) = self.system_replacement_execution().await? {
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
                    .verify_recovery_execution(self.identity(), identity)
                    .await?;
            } else if identity != permit.local {
                return Err(stale());
            }
            voters.push(identity);
        }
        let voters: [WorkerIdentity; 3] = voters.try_into().map_err(|_| stale())?;
        let evidence = serialized_digest(&(&group, voters, state.enrollment_revision, true))?;
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
                    group: Box::new(group),
                    execution: true,
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
        if self.system_replacement_activation().await?.is_none()
            || (intent.mount_public_key.is_some()
                && self.system_replacement_execution().await?.is_none())
        {
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
                    intent: Box::new(intent),
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

    pub(in crate::oltp_replication::authority::control) async fn check_replacement_enrollment_capability(
        &self,
        manager: &HostedGroupManager,
        next: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        if !self
            .registered_directory
            .get()
            .is_some_and(|directory| directory.replacement_capability_active())
        {
            return Ok(());
        }
        if self.system_replacement_execution().await?.is_some() {
            manager
                .verify_recovery_execution(self.identity(), next)
                .await
        } else {
            manager
                .verify_recovery_capability(self.identity(), next)
                .await
        }
    }

    async fn system_replacement_execution(&self) -> Result<Option<AppliedPosition>, ProtocolError> {
        let raw = self
            .read(|view| {
                let activation = load::<AppliedPosition>(view, EXECUTION)?;
                view.encode(&activation)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn replacement_record_local(
        &self,
        id: Uuid,
    ) -> Result<Option<(ReplacementRecord, bool, bool)>, ProtocolError> {
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let record = if let Some(record) = load::<ReplacementRecord>(view, &by_id(id))? {
                    let mounted = view
                        .get(&revoked_id(record.intent.previous.worker_id))?
                        .is_some();
                    let complete = mounted && load::<Uuid>(view, ACTIVE)? != Some(id);
                    Some((record, mounted, complete))
                } else {
                    None
                };
                view.encode(&record)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn active_system_replacement(
        &self,
    ) -> Result<Option<(ReplacementRecord, Vec<Uuid>)>, ProtocolError> {
        let raw = self
            .read(|view| {
                let active = if let Some(id) = load::<Uuid>(view, ACTIVE)? {
                    let record = load::<ReplacementRecord>(view, &by_id(id))?.ok_or_else(stale)?;
                    let mut remaining = Vec::new();
                    for plan in &record.groups {
                        let group = plan.expected.identity.group_id;
                        if view.get(&published(id, group))?.is_none() {
                            remaining.push(group);
                        }
                    }
                    Some((record, remaining))
                } else {
                    None
                };
                view.encode(&active)
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    pub(crate) async fn mount_system_replacement(
        &self,
        running: &ManagedControl,
        proof: ReplacementMountProof,
        state: &State,
    ) -> Result<(), ProtocolError> {
        match self
            .propose_system_recovery(
                running,
                Uuid::new_v4(),
                SystemRecoveryCommand::Mount {
                    proof,
                    ready: state.ready,
                    enrollment_revision: state.enrollment_revision,
                    group_inventory_revision: state.group_inventory_revision,
                },
            )
            .await?
        {
            ControlReply::SystemReplacementMounted => Ok(()),
            ControlReply::Rejected(Fault::Unauthorized) => Err(unauthorized()),
            _ => Err(unavailable("system replacement mount or inventory changed")),
        }
    }

    pub(crate) async fn publish_system_replacement_group(
        &self,
        running: &ManagedControl,
        permit: &LocalOperatorPermit,
        record: &ReplacementRecord,
        group: Uuid,
        applied: AppliedPosition,
    ) -> Result<(), ProtocolError> {
        let id = record.intent.id;
        let operation_id = Uuid::new_v4();
        let evidence = serialized_digest(&(id, group, applied))?;
        let authorization = if group == self.identity().group_id
            && permit.local == WorkerIdentity::from(&record.intent.next)
        {
            let membership = running.observe_stable_membership().await?;
            let plan = record
                .groups
                .iter()
                .find(|plan| plan.expected.identity == *self.identity())
                .ok_or_else(stale)?;
            if membership.replicas() != plan.next(record.intent.previous.worker_id)? {
                return Err(unauthorized());
            }
            AuthorizationStamp {
                authorization_id: operation_id,
                subject: permit.local.certificate_sha256,
                policy: self.policy.policy,
                evidence,
            }
        } else {
            self.local_operator(permit, operation_id, evidence)?
        };
        match self
            .propose_system_recovery(
                running,
                operation_id,
                SystemRecoveryCommand::Publish {
                    id,
                    group,
                    applied,
                    authorization,
                },
            )
            .await?
        {
            ControlReply::SystemReplacementPublished(_) => Ok(()),
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
