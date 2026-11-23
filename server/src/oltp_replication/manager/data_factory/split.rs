//! Typed owner-only split transitions and worker-authenticated proof reads.
use super::*;
use crate::{
    oltp::mvcc::{
        MAX_FIXED_COMMAND_BYTES, MvccKey, json_decode_budget,
        store::{
            ParticipantCommand, ParticipantReply,
            image_archive::{ImagePage, ImageRequest},
            lineage::{
                SplitBinding, SplitImportedStatement, SplitSourceStatement, VerifiedSplitImported,
                VerifiedSplitSource, child_digest,
            },
        },
    },
    oltp_replication::{
        authority::{
            control::{ControlReply, LocalOperatorPermit, ServiceAction},
            status::SealedSettlement,
            timestamp::{
                TimestampHandle, TimestampReply,
                split::{
                    MAX_METADATA, SplitAttempt, SplitPlacement, SplitSourcePlacement,
                    SplitTimestampCommand, SplitVoter,
                },
            },
        },
        owned::{OwnedCommand, serialized_digest},
        service_rpc::{ParticipantProofClient, StatusProofClient},
    },
};
use serde::de::DeserializeOwned;
use std::{ops::Deref, time::Duration};

mod automatic;
mod endpoint;
pub(crate) use automatic::{
    AutomaticSplitPolicy, SplitCapacity, SplitPoolDeficit, Usage, UsageResume,
};
pub(crate) use endpoint::SplitEndpoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Capacity,
    Usage,
    Timestamp,
    Job,
    FailureSettlement,
    Freeze,
    Source,
    Host,
    Activate,
    Stage,
    Progress,
    Advance,
    Images,
    Imported,
    Publication,
    Finish,
}
impl Operation {
    fn routes_data(self) -> bool {
        matches!(
            self,
            Self::Freeze
                | Self::Source
                | Self::Stage
                | Self::Progress
                | Self::Advance
                | Self::Images
                | Self::Imported
                | Self::Finish
        )
    }
    fn purpose(self) -> Purpose {
        match self {
            Self::Job | Self::Source | Self::Publication | Self::FailureSettlement => {
                Purpose::ShardSplitProof
            }
            Self::Images => Purpose::ShardSplitImage,
            _ => Purpose::ShardSplit,
        }
    }
    fn traffic(self) -> TrafficClass {
        match self.purpose() {
            Purpose::ShardSplitProof => TrafficClass::ServiceAuthority,
            Purpose::ShardSplitImage => TrafficClass::Storage,
            _ => TrafficClass::Settlement,
        }
    }
    fn input_limit(self) -> usize {
        if self == Self::Images {
            MAX_FIXED_COMMAND_BYTES
        } else {
            MAX_METADATA
        }
    }
    fn owner_only(self) -> bool {
        !matches!(
            self,
            Self::Job | Self::Source | Self::Images | Self::Publication | Self::FailureSettlement
        )
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitHeader {
    worker_node: u64,
    operation: Operation,
    payload_bytes: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitReply {
    worker_node: u64,
    nonce: Uuid,
    payload_bytes: usize,
    digest: Digest,
    error: Option<ProtocolError>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRequest {
    split_id: Uuid,
    source: GroupIdentity,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetRequest {
    split_id: Uuid,
    target: GroupIdentity,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageRead {
    source: SourceRequest,
    target: GroupIdentity,
    after: Option<MvccKey>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitCursor {
    pub pages: u64,
    pub root: Digest,
    pub after: Option<Digest>,
    pub complete: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Advance {
    child: TargetRequest,
    expected: SplitCursor,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Finish {
    source: SourceRequest,
    manifest: ManifestRef,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    source: SourceRequest,
    manifest: ManifestRef,
    at: CommitTs,
}

/// This context can only originate from the exact authenticated proof RPC.
pub(crate) struct AuthenticatedSplitProof {
    operation: Operation,
    group: GroupIdentity,
    digest: Digest,
    owner: Arc<ScratchLease>,
}
impl AuthenticatedSplitProof {
    pub(crate) fn verify(
        self,
        source: bool,
        group: &GroupIdentity,
        value: &impl Serialize,
    ) -> Result<Arc<ScratchLease>, ProtocolError> {
        if self.group != *group
            || self.operation
                != if source {
                    Operation::Source
                } else {
                    Operation::Imported
                }
            || self.digest != serialized_digest(value)?
        {
            return Err(invalid());
        }
        Ok(self.owner)
    }
}

#[derive(Clone)]
pub(crate) struct SplitClient {
    data: Arc<DataGroupClient>,
    control: Arc<ControlClient>,
    timestamps: GroupIdentity,
    catalog: GroupIdentity,
}
pub(crate) struct OwnedSplitAttempt {
    job: SplitAttempt,
    _owner: Arc<ScratchLease>,
}
impl Deref for OwnedSplitAttempt {
    type Target = SplitAttempt;

    fn deref(&self) -> &SplitAttempt {
        &self.job
    }
}
impl SplitClient {
    pub(crate) fn new(
        data: Arc<DataGroupClient>,
        control: Arc<ControlClient>,
        timestamps: GroupIdentity,
        catalog: GroupIdentity,
    ) -> Self {
        Self {
            data,
            control,
            timestamps,
            catalog,
        }
    }
    fn shared(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.data.manager.resources()
    }
    async fn request(
        &self,
        member: &ReplicaMember,
        operation: Operation,
        payload: &ChargedBytes,
    ) -> Result<(ReceivedReply, Digest), ProtocolError> {
        let header = self.shared().encode_in(
            operation.traffic().byte_class(),
            &SplitHeader {
                worker_node: member.node_id,
                operation,
                payload_bytes: payload.len() as u64,
            },
            HEADER,
        )?;
        let reply = self
            .data
            .manager
            .client()
            .request(
                &self.data.control,
                member.worker_id,
                operation.purpose(),
                header,
                [Some(payload.clone()), None, None],
                64 << 10,
            )
            .await?;
        let observed = reply.remote_identity();
        if observed.worker_id != member.worker_id
            || reply.purpose() != operation.purpose()
            || reply.group() != &self.data.control
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(invalid());
        }
        let header = reply.decode_header::<SplitReply>()?;
        if header.worker_node != member.node_id || header.nonce != reply.nonce() {
            return Err(invalid());
        }
        if let Some(error) = &header.error {
            return Err(error.clone());
        }
        let raw = reply.sections[0].as_ref().ok_or_else(invalid)?;
        if raw.len() != header.payload_bytes {
            return Err(invalid());
        }
        let digest = header.digest;
        drop(header);
        Ok((reply, digest))
    }
    async fn route(
        &self,
        destination: &GroupIdentity,
        operation: Operation,
        input: &impl Serialize,
    ) -> Result<(ReceivedReply, Digest), ProtocolError> {
        let routes = self.control.replica_routes(destination).await?;
        let payload = self.shared().encode_in(
            operation.traffic().byte_class(),
            input,
            operation.input_limit(),
        )?;
        let mut last = unavailable();
        let data = operation.routes_data();
        let system_routes = if data || destination == &self.catalog {
            routes.clone()
        } else {
            self.control.replica_routes(&self.catalog).await?
        };
        let system = system_routes.members.iter().filter(|voter| {
            !data
                && routes.members.iter().any(|member| {
                    member.worker_id == voter.worker_id && member.pool_id == voter.pool_id
                })
        });
        let candidates = routes.members.iter().filter(|_| data).chain(system);
        for member in candidates {
            match self.request(member, operation, &payload).await {
                Ok(reply) => return Ok(reply),
                Err(error) if transient(&error) => last = error,
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }
    async fn decode<T: DeserializeOwned>(
        &self,
        reply: ReceivedReply,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let raw = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        let size = json_decode_budget(raw.as_slice())?;
        let reply = reply.reserve_reply_decode(self.shared(), size).await?;
        let decoded = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
        Ok((decoded, reply.scratch.clone()))
    }
    async fn typed<T: DeserializeOwned>(
        &self,
        destination: &GroupIdentity,
        operation: Operation,
        input: &impl Serialize,
    ) -> Result<T, ProtocolError> {
        let (reply, _) = self.route(destination, operation, input).await?;
        self.decode(reply).await.map(|(value, _owner)| value)
    }
    pub(crate) async fn job(
        &self,
        split_id: Uuid,
    ) -> Result<Option<OwnedSplitAttempt>, ProtocolError> {
        let (reply, _) = self
            .route(&self.timestamps, Operation::Job, &split_id)
            .await?;
        let (job, owner): (Option<SplitAttempt>, _) = self.decode(reply).await?;
        Ok(job.map(|job| OwnedSplitAttempt { job, _owner: owner }))
    }
    pub(crate) async fn timestamp(
        &self,
        command: SplitTimestampCommand,
    ) -> Result<TimestampReply, ProtocolError> {
        let reply = self
            .typed(&self.timestamps, Operation::Timestamp, &command)
            .await?;
        match reply {
            TimestampReply::Rejected(fault) => Err(conflict(format!(
                "split timestamp transition rejected: {fault:?}"
            ))),
            reply => Ok(reply),
        }
    }
    pub(crate) async fn freeze(&self, attempt: &SplitAttempt) -> Result<(), ProtocolError> {
        self.typed(&attempt.intent.source, Operation::Freeze, &source(attempt))
            .await
    }
    pub(crate) async fn source(
        &self,
        attempt: &SplitAttempt,
    ) -> Result<VerifiedSplitSource, ProtocolError> {
        let group = &attempt.intent.source;
        let (reply, digest) = self
            .route(group, Operation::Source, &source(attempt))
            .await?;
        let (value, owner): ((SplitSourceStatement, Option<CommitTs>), _) =
            self.decode(reply).await?;
        VerifiedSplitSource::from_authenticated(
            AuthenticatedSplitProof {
                operation: Operation::Source,
                group: group.clone(),
                digest,
                owner,
            },
            value,
            attempt.children()?,
        )
    }
    pub(crate) async fn place(
        &self,
        control: &ControlHandle,
        operator: &LocalOperatorPermit,
        attempt: &SplitAttempt,
        child: &SplitBinding,
    ) -> Result<(), ProtocolError> {
        let input = child_request(child);
        let payload = self
            .shared()
            .encode_in(ByteClass::Progress, &input, MAX_METADATA)?;
        let generation = split_id(child.target.group_id, b"split-control-membership");
        let placement = attempt.placement();
        let voters = if let Some(placement) = placement {
            self.verify_source_placement(&placement.source, attempt.authority)
                .await?;
            placement
                .source
                .voters
                .iter()
                .map(|voter| voter.member.clone())
                .collect::<Vec<_>>()
        } else {
            self.data.initial_voters(control, &self.control).await?.into_inner()
        };
        let exact_worker = |reply: &ReceivedReply, index: usize| {
            placement.is_none_or(|placement| {
                reply.remote_identity() == placement.source.voters[index].worker
            })
        };
        let registered = control.group(child.target.group_id).await?;
        if let Some(record) = &registered {
            if record.generation != generation
                || record.identity != child.target
                || self
                    .control
                    .replica_routes(&child.target)
                    .await?
                    .members
                    .as_slice()
                    != voters.as_slice()
            {
                return Err(invalid());
            }
        } else {
            for (index, member) in voters.iter().enumerate() {
                let (reply, _) = self.request(member, Operation::Host, &payload).await?;
                if !exact_worker(&reply, index) {
                    return Err(unauthorized());
                }
                let ((observed, hosted), _): ((GroupIdentity, ReplicaMember), _) =
                    self.decode(reply).await?;
                if observed != child.target || hosted != *member {
                    return Err(invalid());
                }
            }
        }
        let mut observation = None;
        let deadline = nabedra_rt::time::now() + Duration::from_secs(12);
        while observation.is_none() {
            for (index, member) in voters.iter().enumerate() {
                match nabedra_rt::time::timeout_at(
                    deadline,
                    self.request(member, Operation::Activate, &payload),
                )
                .await
                {
                    Ok(Ok((reply, _))) => {
                        if !exact_worker(&reply, index) {
                            return Err(unauthorized());
                        }
                        let (actual, owner): (Membership, _) = self.decode(reply).await?;
                        if actual.group != child.target
                            || actual.replicas.as_ref() != voters.as_slice()
                            || actual.applied_at.index == 0
                        {
                            return Err(invalid());
                        }
                        observation = Some(GroupMembershipObservation {
                            group: actual.group,
                            replicas: actual.replicas,
                            applied_at: actual.applied_at,
                            _owner: owner,
                        });
                        break;
                    }
                    Ok(Err(error)) if transient(&error) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => return Err(unavailable()),
                }
            }
            if observation.is_none() {
                if nabedra_rt::time::now() >= deadline {
                    return Err(unavailable());
                }
                nabedra_rt::time::sleep(Duration::from_millis(50)).await;
            }
        }
        if registered.is_some() {
            return Ok(());
        }
        let observed = observation.ok_or_else(unavailable)?;
        if let Some(placement) = placement {
            self.verify_source_placement(&placement.source, attempt.authority)
                .await?;
        }
        let applied = control
            .register_initial_data_local(operator, generation, attempt.authority, observed)
            .await?;
        if !matches!(applied.reply, ControlReply::GroupRegistered { group_id, count, .. } if group_id == child.target.group_id && count == voters.len() as u64)
        {
            return Err(unavailable());
        }
        Ok(())
    }
    pub(crate) async fn stage(&self, child: &SplitBinding) -> Result<(), ProtocolError> {
        self.typed(&child.target, Operation::Stage, &child_request(child))
            .await
    }
    pub(crate) async fn progress(
        &self,
        child: &SplitBinding,
    ) -> Result<SplitCursor, ProtocolError> {
        self.typed(&child.target, Operation::Progress, &child_request(child))
            .await
    }
    pub(crate) async fn advance(
        &self,
        child: &SplitBinding,
        expected: SplitCursor,
    ) -> Result<(), ProtocolError> {
        self.typed(
            &child.target,
            Operation::Advance,
            &Advance {
                child: child_request(child),
                expected,
            },
        )
        .await
    }
    pub(crate) async fn imported(
        &self,
        child: &SplitBinding,
    ) -> Result<VerifiedSplitImported, ProtocolError> {
        let target = &child.target;
        let (reply, digest) = self
            .route(target, Operation::Imported, &child_request(child))
            .await?;
        let (value, owner): (SplitImportedStatement, _) = self.decode(reply).await?;
        VerifiedSplitImported::from_authenticated(
            AuthenticatedSplitProof {
                operation: Operation::Imported,
                group: target.clone(),
                digest,
                owner,
            },
            value,
            child,
        )
    }
    async fn image(&self, request: ImageRead) -> Result<OwnedCommand<ImagePage>, ProtocolError> {
        let (reply, expected) = self
            .route(&request.source.source, Operation::Images, &request)
            .await?;
        let raw = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        let shared = self.shared().clone();
        let resources = shared.clone();
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                use sha2::{Digest as _, Sha256};
                if expected != Digest::from_bytes(Sha256::digest(raw.as_slice()).into()) {
                    return Err(invalid());
                }
                OwnedCommand::decode_json(&raw, &resources, ByteClass::Data, 128 << 20)
            })
            .await
    }
    pub(crate) async fn finish(
        &self,
        attempt: &SplitAttempt,
        manifest: ManifestRef,
    ) -> Result<(), ProtocolError> {
        self.typed(
            &attempt.intent.source,
            Operation::Finish,
            &Finish {
                source: source(attempt),
                manifest,
            },
        )
        .await
    }
}
fn child_request(child: &SplitBinding) -> TargetRequest {
    TargetRequest {
        split_id: child.split_id,
        target: child.target.clone(),
    }
}
fn source(attempt: &SplitAttempt) -> SourceRequest {
    SourceRequest {
        split_id: attempt.intent.split_id,
        source: attempt.intent.source.clone(),
    }
}
fn transient(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}
fn conflict(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}

fn split_id(namespace: Uuid, phase: &[u8]) -> Uuid {
    use sha2::{Digest as _, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"nabedra/split-placement/v1\0");
    hash.update(namespace.as_bytes());
    hash.update(phase);
    let digest = hash.finalize();
    Uuid::from_bytes(digest[..16].try_into().expect("fixed hash prefix"))
}
