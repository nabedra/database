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

/// The durable activation. It never changes once written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectActivation {
    pub activated_at: AppliedPosition,
    pub run_id: Uuid,
    pub enrollment_revision: u64,
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
}
