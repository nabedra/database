//! Wire types of the `data.direct.v1` Control capability (plan section 4,
//! "Mixed versions"). Control records adverts and the activation in its own
//! log; see `machine::direct`.
//!
//! Upgrade order: every Control voter must run a build with this module
//! before any worker advertises, as with the scoped-queue capability: an
//! older Control replica cannot decode `ControlCommand::DirectCapability`.
//! Downgrade is unsupported once the capability is active: Data groups then
//! hold direct entries, dedup records and forward markers an older build
//! cannot decode.
//!
//! **Cluster features (GC2).** Later direct-family entries are gated by named
//! cluster features, as in ScyllaDB: every worker advertises the features its
//! build supports, Control enables a feature once every enrolled worker (and
//! so every Control voter and learner, each an enrolled worker) advertised
//! it, the enablement is sticky, and afterwards a worker identity that did
//! not advertise it cannot join a group or the ready set.
//!
//! The advert must apply identically on Control voters that predate the
//! feature. It is therefore an ordinary `Advertise { version: 1 }` whose
//! operation id is derived from the feature and the advertiser's exact
//! identity (`feature_operation_id`). Every build records it the same way
//! (the v1 reader key and the `ct/op/` receipt), and a feature-aware build
//! reads "W advertised F" as "the receipt of F's operation id for W's current
//! identity exists". The enablement itself is written only by a
//! feature-aware build, in an apply that sees every enrolled worker's advert,
//! that is when no older Control replica is left. A feature's read uses its
//! own `ReadRequest`, which an older Control leader rejects in its parser;
//! callers treat that as "not enabled".
use crate::oltp_replication::types::{AppliedPosition, WorkerIdentity};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The capability name the version stands for.
#[allow(dead_code)] // Documentation and diagnostics.
pub(crate) const DIRECT_CAPABILITY: &str = "data.direct.v1";
#[doc(hidden)]
pub const DIRECT_VERSION: u32 = 1;

/// GC2: Data leaders may pack several direct commits to one range into one
/// `DirectBatch` entry.
#[doc(hidden)]
pub const DIRECT_BATCH_FEATURE: &str = "data.direct.batch";
#[doc(hidden)]
pub const QUEUED_SNAPSHOT_RETRY_FEATURE: &str = "status.queued-snapshot-retry";
/// CAP1: Data leaders may propose `ReclaimIndex` entries (IDXGC1), and
/// workers may send the reclamation service calls (`ParticipantStoredIndex`,
/// `ParticipantIndexReclaim`, the Timestamp `AdvanceFloor` call). An older
/// build cannot decode them.
#[doc(hidden)]
pub const INDEX_RECLAIM_FEATURE: &str = "data.index.reclaim";

/// One registered cluster feature: its advertised name and the Control key
/// of its sticky enablement (`ct/feature/<name>`, a `FeatureActivation`).
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub struct ClusterFeature {
    pub name: &'static str,
    pub enabled_key: &'static [u8],
}

/// The cluster feature registry (CAP1), as ScyllaDB's feature service keeps
/// one list of known features. Every worker of this build advertises each
/// entry on its join paths; Control's feature-aware apply evaluates each
/// entry's enablement and refuses joiners that did not advertise an enabled
/// one (`machine::direct`). Entries are only ever appended: a name, once
/// shipped, keeps its meaning and its key.
#[doc(hidden)]
pub const CLUSTER_FEATURES: [ClusterFeature; 3] = [
    ClusterFeature {
        name: DIRECT_BATCH_FEATURE,
        enabled_key: b"ct/feature/data.direct.batch",
    },
    ClusterFeature {
        name: QUEUED_SNAPSHOT_RETRY_FEATURE,
        enabled_key: b"ct/feature/status.queued-snapshot-retry",
    },
    ClusterFeature {
        name: INDEX_RECLAIM_FEATURE,
        enabled_key: b"ct/feature/data.index.reclaim",
    },
];

/// The registry entry named `name`.
pub(crate) fn cluster_feature(name: &str) -> Option<ClusterFeature> {
    CLUSTER_FEATURES
        .into_iter()
        .find(|feature| feature.name == name)
}

/// Control's answer to `ReadRequest::ClusterFeature`: one registered
/// feature's enablement and whether the caller's current enrolled identity
/// advertised it. An older Control rejects the request in its parser, and a
/// Control that does not know the name answers an error: both read as "not
/// enabled".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClusterFeatureState {
    pub enabled: Option<FeatureActivation>,
    pub advertised: bool,
}

/// The operation id of `actor`'s advert of `feature` (see the module doc).
/// It covers the exact enrolled identity, so a new incarnation (possibly an
/// older build) must advertise again.
pub(crate) fn feature_operation_id(feature: &str, actor: WorkerIdentity) -> Uuid {
    use sha2::{Digest as _, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"nabedra/control/feature-advert/v1\0");
    hash.update(feature.as_bytes());
    hash.update([0]);
    hash.update(actor.worker_id.as_bytes());
    hash.update(actor.incarnation.as_bytes());
    hash.update(actor.certificate_sha256.as_bytes());
    hash.update(actor.enrollment_revision.to_be_bytes());
    let digest = hash.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}

/// The durable enablement of a cluster feature. It never changes once
/// written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct FeatureActivation {
    pub activated_at: AppliedPosition,
    pub enrollment_revision: u64,
}

/// Control's answer to `ReadRequest::DirectFeatures`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct DirectFeatures {
    /// `data.direct.batch` is enabled cluster-wide.
    pub batch: Option<FeatureActivation>,
    /// The caller's current enrolled identity advertised `data.direct.batch`.
    pub batch_advertised: bool,
}

/// The proposer's wall clock for `ControlCommand::DirectCapability`.
pub(crate) fn wall_ms() -> u64 {
    nabedra_rt::time::wall_now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// The durable activation. It never changes once written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct DirectActivation {
    pub activated_at: AppliedPosition,
    pub run_id: Uuid,
    pub enrollment_revision: u64,
    /// A queue owner reserved an eligible mutation without a forward marker
    /// before this activation (`QueuedHistory`, T7c). Such a request id may
    /// still be retried, so for one dedup retention after `unix_ms` no
    /// leader executes a mutation directly unless its anchor already holds
    /// a dedup record; the queued path answers instead.
    #[serde(default)]
    pub queued_history: bool,
    /// Wall clock (Unix ms) of the command that activated, from its proposer.
    #[serde(default)]
    pub unix_ms: u64,
}
impl DirectActivation {
    /// The end (Unix ms) of the queued-history window, `None` without one.
    #[doc(hidden)]
    pub fn history_until(&self, retention_ms: u64) -> Option<u64> {
        self.queued_history
            .then(|| self.unix_ms.saturating_add(retention_ms))
    }
}

/// Control's answer to an advert, an activation attempt or a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct DirectCapability {
    pub version: u32,
    pub activation: Option<DirectActivation>,
    /// The caller's current enrolled identity advertised the capability.
    pub advertised: bool,
}
impl DirectCapability {
    #[doc(hidden)]
    pub fn active(&self) -> bool {
        self.activation.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[doc(hidden)]
pub enum DirectCapabilityOperation {
    /// The caller's build decodes every direct-family entry.
    Advertise { version: u32 },
    /// Re-evaluates activation, after a member that never advertised left.
    Activate,
    /// Sent by a queue owner (which runs this build, so it also advertises)
    /// before it reserves an eligible mutation without settling it at its
    /// anchor (T7c). Control activates now if it can; otherwise it records
    /// that pre-activation queued history exists. The reply says which: an
    /// active capability means the owner must settle at the anchor first.
    QueuedHistory,
}
