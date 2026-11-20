//! `data.direct.v1` reads and the accepting leader's advert path.
use super::*;
use crate::oltp_replication::authority::control::{
    DIRECT_VERSION, DirectActivation, DirectCapability, DirectCapabilityOperation,
    machine::direct::{activation_key, reader_key},
};

impl ControlHandle {
    /// The durable activation and whether `remote`'s current identity
    /// advertised the capability. A strong (linearizable) read.
    pub(crate) async fn direct_capability(
        &self,
        remote: WorkerIdentity,
    ) -> Result<DirectCapability, ProtocolError> {
        let raw = self
            .read(move |view| {
                let activation: Option<DirectActivation> = load(view, activation_key())?;
                let advertised = load::<WorkerIdentity>(view, &reader_key(remote))? == Some(remote);
                view.encode(&DirectCapability {
                    version: DIRECT_VERSION,
                    activation,
                    advertised,
                })
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    /// The accepting Control leader binds the transport identity to the
    /// advert, like the scoped-queue capability.
    pub(in crate::oltp_replication::authority::control) async fn apply_direct_capability_rpc(
        &self,
        session: &AuthenticatedSession,
        operation_id: Uuid,
        action: DirectCapabilityOperation,
        leader: Option<&dyn Consensus<ControlCommand, ControlReply>>,
    ) -> Result<ControlReply, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.identity()
            || operation_id.is_nil()
        {
            return Err(unauthorized());
        }
        let command = ControlCommand::DirectCapability {
            operation_id,
            actor: session.remote(),
            action,
            unix_ms: crate::oltp_replication::authority::control::direct::wall_ms(),
        };
        let applied = if let Some(leader) = leader {
            let owned = OwnedCommand::new_owned(command, self.resources(), ByteClass::Progress)?;
            leader.propose(owned).await?
        } else {
            self.submit(command).await?
        };
        Ok(applied.reply)
    }
}
