//! NS14: `GroupDamaged` and `GroupDamageReplaced` through the Control handle.
//! See `control/group_damage.rs`.
use super::super::group_damage::{
    GroupDamageGate, GroupDamageRecord, GroupDamageReplacement, GroupDamageReport, record_key,
};
use super::*;

impl ControlHandle {
    fn group_damage_gate(&self) -> Result<(), ProtocolError> {
        let open = self
            .group_damage_gate
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .is_some_and(|gate| gate.group_damage_supported());
        if open {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "GroupDamaged is not enabled on every Control replica",
                Outcome::NotApplied,
            ))
        }
    }
    /// Installs the predicate that allows the NS14 commands.
    pub(crate) fn install_group_damage_gate(&self, gate: Arc<dyn GroupDamageGate>) {
        *self
            .group_damage_gate
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(gate);
    }

    /// Rule 2. `worker` is the authenticated reporter (the transport identity
    /// for a remote call, the local worker in process).
    pub(crate) async fn report_group_damage(
        &self,
        worker: WorkerIdentity,
        report: GroupDamageReport,
    ) -> Result<GroupDamageRecord, ProtocolError> {
        report.validate()?;
        if report.replica.worker_id != worker.worker_id
            || report.group.cluster_id != self.identity().cluster_id
            || report.group.database != self.identity().database
        {
            return Err(damage_unauthorized());
        }
        self.group_damage_gate()?;
        let applied = self
            .submit(ControlCommand::GroupDamaged {
                operation_id: report.operation_id(worker)?,
                worker,
                report: Box::new(report),
            })
            .await?;
        damage_reply(applied.reply)
    }

    /// Rule 3, after the leader's `replace_voter` committed. A local operator
    /// call: the driver runs in a process that hosts Control.
    pub(crate) async fn group_damage_replaced_local(
        &self,
        permit: &LocalOperatorPermit,
        operation_id: Uuid,
        proof: GroupDamageReplacement,
    ) -> Result<GroupDamageRecord, ProtocolError> {
        self.group_damage_gate()?;
        let evidence = crate::oltp_replication::owned::serialized_digest(&proof)?;
        let authorization = self.local_operator(permit, operation_id, evidence)?;
        let applied = self
            .submit(ControlCommand::GroupDamageReplaced {
                operation_id,
                authorization,
                proof: Box::new(proof),
            })
            .await?;
        damage_reply(applied.reply)
    }

    /// The applied record of one replica's damage, after a quorum read.
    pub(crate) async fn group_damage(
        &self,
        group_id: Uuid,
        node_id: u64,
    ) -> Result<Option<GroupDamageRecord>, ProtocolError> {
        let bytes = self
            .read(move |view| {
                let record: Option<GroupDamageRecord> = view
                    .get(&record_key(group_id, node_id))?
                    .map(|raw| machine::decode(raw.as_ref()))
                    .transpose()?;
                view.encode(&record)
            })
            .await?;
        machine::decode(bytes.as_ref())
    }
}

fn damage_reply(reply: ControlReply) -> Result<GroupDamageRecord, ProtocolError> {
    match reply {
        ControlReply::GroupDamage(record) => Ok(*record),
        ControlReply::Rejected(Fault::Unauthorized) => Err(damage_unauthorized()),
        ControlReply::Rejected(Fault::NotFound) => Err(ProtocolError::new(
            ErrorCode::NotFound,
            "no damage report for that replica",
            Outcome::NotApplied,
        )),
        ControlReply::Rejected(Fault::Conflict) => Err(ProtocolError::new(
            ErrorCode::Conflict,
            "damage record already names another replacement",
            Outcome::NotApplied,
        )),
        ControlReply::Rejected(_) => Err(invalid()),
        _ => Err(ProtocolError::new(
            ErrorCode::InternalError,
            "unexpected Control reply to a damage command",
            Outcome::Unknown,
        )),
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid group damage command",
        Outcome::NotApplied,
    )
}
fn damage_unauthorized() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "group damage report does not match the reporting worker",
        Outcome::NotApplied,
    )
}
