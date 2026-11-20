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
use crate::oltp_replication::types::AppliedPosition;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The capability name the version stands for.
#[allow(dead_code)] // Documentation and diagnostics.
pub(crate) const DIRECT_CAPABILITY: &str = "data.direct.v1";
pub(crate) const DIRECT_VERSION: u32 = 1;

/// The proposer's wall clock for `ControlCommand::DirectCapability`.
pub(crate) fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// The durable activation. It never changes once written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectActivation {
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
    pub(crate) fn history_until(&self, retention_ms: u64) -> Option<u64> {
        self.queued_history
            .then(|| self.unix_ms.saturating_add(retention_ms))
    }
}

/// Control's answer to an advert, an activation attempt or a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectCapability {
    pub version: u32,
    pub activation: Option<DirectActivation>,
    /// The caller's current enrolled identity advertised the capability.
    pub advertised: bool,
}
impl DirectCapability {
    pub(crate) fn active(&self) -> bool {
        self.activation.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectCapabilityOperation {
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
