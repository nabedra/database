//! `data.direct.v1` reads and the accepting leader's advert path.
use super::*;
use crate::oltp_replication::authority::control::{
    ClusterFeatureState, DIRECT_BATCH_FEATURE, DIRECT_VERSION, DirectActivation, DirectCapability,
    DirectCapabilityOperation, DirectFeatures, FeatureActivation, cluster_feature,
    feature_operation_id,
    machine::direct::{SNAPSHOT_RETRY_ENABLED, activation_key, batch_key, reader_key},
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

    /// GC2: the cluster features of the direct family and whether `remote`'s
    /// current identity advertised them. A strong (linearizable) read.
    pub(crate) async fn direct_features(
        &self,
        remote: WorkerIdentity,
    ) -> Result<DirectFeatures, ProtocolError> {
        let raw = self
            .read(move |view| {
                let batch: Option<FeatureActivation> = load(view, batch_key())?;
                let receipt = machine::key(
                    b"ct/op/",
                    feature_operation_id(DIRECT_BATCH_FEATURE, remote),
                );
                let batch_advertised = view.get(&receipt)?.is_some();
                view.encode(&DirectFeatures {
                    batch,
                    batch_advertised,
                })
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    /// CAP1: one registered cluster feature's enablement and whether
    /// `remote`'s current identity advertised it. A strong (linearizable)
    /// read. A name this build does not register is invalid.
    pub(crate) async fn cluster_feature(
        &self,
        remote: WorkerIdentity,
        name: &str,
    ) -> Result<ClusterFeatureState, ProtocolError> {
        let feature = cluster_feature(name).ok_or_else(|| {
            ProtocolError::new(
                crate::protocol::ErrorCode::ValidationError,
                "unknown Control cluster feature",
                crate::protocol::Outcome::NotApplied,
            )
        })?;
        let raw = self
            .read(move |view| {
                let enabled: Option<FeatureActivation> = load(view, feature.enabled_key)?;
                let receipt = machine::key(b"ct/op/", feature_operation_id(feature.name, remote));
                // As `machine::direct::supports_feature` reads it.
                let advertised = load::<machine::Receipt>(view, &receipt)?.is_some_and(|receipt| {
                    matches!(receipt.reply, ControlReply::DirectCapability(_))
                });
                view.encode(&ClusterFeatureState {
                    enabled,
                    advertised,
                })
            })
            .await?;
        machine::decode(raw.as_ref())
    }

    #[doc(hidden)]
    pub async fn queued_snapshot_retry_enabled(&self) -> Result<bool, ProtocolError> {
        let raw = self
            .read(|view| {
                let enabled = view.get(SNAPSHOT_RETRY_ENABLED)?.is_some();
                view.encode(&enabled)
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
