//! Named metadata calls. The endpoint reads dependent authorities itself;
//! caller-provided checksums and TLS membership cannot replace those reads.
use super::*;
use crate::oltp::mvcc::CanonicalJson;
use crate::oltp_replication::authority::{
    consensus::{Applied, ConsensusFuture},
    status::*,
    timestamp::{
        ReadyFenceReceipt, TimestampHandle, TimestampReply, TimestampService, VerifiedPinRelease,
    },
};
mod admission;
mod status_service;
mod timestamp_service;
pub(crate) use admission::{
    AdmissionTicketIssuer, OwnerAdmissionObservation, OwnerAdmissionReader, OwnerAdmissionTicket,
};
pub(crate) use status_service::{StatusClient, StatusEndpoint};
pub(crate) use timestamp_service::{BackupPublicationReader, TimestampClient, TimestampEndpoint};

const LIMIT: usize = 128 << 10;

/// Decode ownership crosses every proof/network await until the call settles.
async fn decode<T: serde::de::DeserializeOwned + Send + 'static>(
    shared: &Arc<WorkerSharedResources>,
    sections: ChargedSections,
) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
    let input = sections.section(0).ok_or_else(invalid)?.clone();
    let required = shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            crate::oltp::mvcc::json_decode_budget(input.as_slice())
        })
        .await?;
    let owner = Arc::new(
        shared
            .reserve_scratch_wait(ByteClass::Progress, required, 0)
            .await?,
    );
    shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            let value = serde_json::from_slice(sections.section(0).ok_or_else(invalid)?.as_slice())
                .map_err(|_| invalid())?;
            Ok((value, owner))
        })
        .await
}
async fn encode<T: Serialize + Send + 'static>(
    shared: &Arc<WorkerSharedResources>,
    value: T,
) -> Result<ChargedBytes, ProtocolError> {
    let resources = shared.clone();
    shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            resources.encode_in(ByteClass::Progress, &value, LIMIT)
        })
        .await
}
impl Client {
    async fn named<T: Serialize + Send + 'static>(
        &self,
        method: Method,
        request: T,
        converge: bool,
    ) -> Result<ServiceResponse, ProtocolError> {
        let payload = encode(&self.shared, request).await.map_err(|mut error| {
            error.message = format!("metadata request encode {method:?}: {}", error.message);
            error
        })?;
        if converge {
            self.converge(method, payload).await
        } else {
            self.call(method, Some(payload)).await
        }
    }
}
impl ServiceResponse {
    async fn decoded<T: serde::de::DeserializeOwned + Send + 'static>(
        self,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        shared
            .execute(ExecutionClass::Control, self.node, 0, move || {
                let value = serde_json::from_slice(
                    self.reply.sections[0]
                        .as_ref()
                        .ok_or_else(invalid)?
                        .as_slice(),
                )
                .map_err(|_| invalid())?;
                Ok((value, self.reply.scratch.clone()))
            })
            .await
    }
}

/// Exact authenticated reply provenance, retained through all extracted DTOs.
/// Only the registered-route client can construct it.
pub(crate) struct AuthenticatedMetadataReply {
    group: GroupIdentity,
    method: Method,
    digest: Digest,
    owner: Arc<ScratchLease>,
}
impl AuthenticatedMetadataReply {
    pub(crate) fn status(
        &self,
        operation: u8,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if !matches!(self.group.kind, GroupKind::Status { .. })
            || !matches!(self.method, Method::StatusCall { operation: actual, .. } if actual == operation)
            || serialized_digest(value)? != self.digest
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn timestamp(
        &self,
        operation: u8,
        value: &impl Serialize,
    ) -> Result<(), ProtocolError> {
        if self.group.kind != GroupKind::Timestamps
            || !matches!(self.method, Method::TimestampCall { operation: actual, .. } if actual == operation)
            || serialized_digest(value)? != self.digest
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn group(&self) -> &GroupIdentity {
        &self.group
    }
    pub(crate) fn owner(&self) -> Arc<ScratchLease> {
        self.owner.clone()
    }
}
impl ServiceResponse {
    fn metadata_context(&self) -> Result<AuthenticatedMetadataReply, ProtocolError> {
        if !matches!(
            self.method,
            Method::StatusCall { .. } | Method::TimestampCall { .. }
        ) || self.node == 0
            || self.reply.nonce().is_nil()
            || self.reply.remote_identity().validate().is_err()
        {
            return Err(invalid());
        }
        Ok(AuthenticatedMetadataReply {
            group: self.reply.group().clone(),
            method: self.method,
            digest: self.digest,
            owner: self.reply.scratch.clone(),
        })
    }
}
