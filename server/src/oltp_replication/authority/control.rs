//! Metadata-only Control state and private leader service. Application requests,
//! rows, query text, and pending queue bodies never enter these commands.
mod data_worker;
mod direct;
mod endpoint;
mod features;
mod forwarding;
mod group_damage;
mod growth;
mod machine;
mod model;
mod placement;
mod pool_drain;
mod restart;
mod scoped_queue;
mod service;
mod system_recovery;
mod worker_drain;

pub(crate) use data_worker::{DataWorkerGrowthProof, DataWorkerIntent};
pub(crate) use direct::{
    CLUSTER_FEATURES, ClusterFeatureState, DIRECT_BATCH_FEATURE, DIRECT_VERSION, DirectActivation,
    DirectCapability, DirectCapabilityOperation, DirectFeatures, FeatureActivation,
    INDEX_RECLAIM_FEATURE, QUEUED_SNAPSHOT_RETRY_FEATURE, cluster_feature, feature_operation_id,
};
pub(crate) use endpoint::{
    ControlClient, ControlEndpoint, ControlRoutes, PublishedControlRoutes, ReplicaRoutes,
};
pub(crate) use features::{EnabledFeatures, spawn_watch as watch_cluster_features};
pub(crate) use forwarding::{DataWorkerClient, ForwardedControl, HostedControlEndpoint};
pub(crate) use group_damage::{
    GroupDamageReason, GroupDamageRecord, GroupDamageReplacement, GroupDamageReplacementPlan,
    GroupDamageReport, PoolRefusal, DirectCapabilityDamageGate,
    MAX_ACTIVE_REPLACEMENTS as MAX_ACTIVE_DAMAGE_REPLACEMENTS,
};
#[allow(unused_imports)]
pub(crate) use group_damage::{GroupDamageGate, GroupDamagePhase};
pub(crate) use machine::ControlMachine;
pub(crate) use machine::{group_root, group_seed, group_step};
pub(crate) use model::*;
pub(crate) use placement::{
    RemoteActivationAttestation, RemoteMoveView, RemotePlacementReply, RemoteRetirementAttestation,
    RemoteSourceBinding, ReplicaActivationEvidence, ReplicaMove, ReplicaMoveLineage, ReplicaMovePhase,
    ReplicaMoveRequest, VerifiedRemoteActivation, VerifiedRemoteRetirement,
    decode_reply as decode_remote_placement_reply,
};
pub(crate) use pool_drain::{DrainMount, PoolDrainPhase, PoolDrainRecord, RetainedPoolDetachPermit};
pub(crate) use worker_drain::{WorkerCloseAttestation, WorkerDrainIntent, WorkerDrainPhase, WorkerDrainRecord};
pub(crate) use worker_drain::{EksReplacementAction, EksReplacementPermit, SignedEksReplacement};
pub(crate) use restart::{RestartIntent, RestartMountKey, RestartPreparation};
pub(crate) use scoped_queue::{
    ScopedQueueCapability, ScopedQueuePlacement, ScopedQueueTransferReport, ScopedQueueTransition,
    SCOPED_QUEUE_VERSION,
};
#[allow(unused_imports)]
pub(crate) use scoped_queue::ScopedQueueInventory;
pub(crate) use service::{
    ControlHandle, InventoryPage, LocalOperatorPermit, OperatorPolicy, ReplicaInventoryPage,
    VerifiedExclusion, VerifiedMembership, VerifiedQueueCopy,
};
pub(crate) use service::IdlePasses;
pub(crate) use system_recovery::{
    CAPABILITY as SYSTEM_REPLACEMENT_CAPABILITY,
    EXECUTION_CAPABILITY as SYSTEM_REPLACEMENT_EXECUTION,
    HARDENING_CAPABILITY as SYSTEM_REPLACEMENT_HARDENING,
    MAX_GROUPS as MAX_SYSTEM_REPLACEMENT_GROUPS, ReplacementGroup, ReplacementIntent,
    ReplacementMountProof, ReplacementRecord, SystemRecoveryCommand,
};
#[cfg(test)]
pub(crate) mod tests;
