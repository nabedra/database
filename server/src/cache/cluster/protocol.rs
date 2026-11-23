//! Durable topology records and the worker/controller protocol. These records
//! contain authority, progress and placement only.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::transport::{Endpoint, Fingerprint};
use crate::cache::command::SLOT_COUNT;

pub(super) const VERSION: u16 = 1;
pub(super) const LEASE_MS: u64 = 3_000;
pub(super) const RENEW_MS: u64 = 1_000;
pub(super) const CONTROL_FRAME: usize = 12 << 20;
pub(super) const MAX_WORKERS: usize = 48;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerId {
    pub node: u64,
    pub incarnation: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Member {
    pub id: WorkerId,
    pub zone: String,
    pub client_host: String,
    pub client_port: u16,
    pub peer: Endpoint,
}

impl Member {
    pub fn valid(&self) -> bool {
        self.id.node != 0
            && !self.id.incarnation.is_nil()
            && !self.zone.is_empty()
            && self.zone.len() <= 64
            && self
                .zone
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && self.client_port > 0
            && self.client_host.len() <= 253
            && rustls::pki_types::ServerName::try_from(self.client_host.as_str()).is_ok()
            && self.peer.server_name().is_ok()
    }

    pub fn authenticates(&self, fingerprint: Fingerprint) -> bool {
        self.peer.fingerprints.contains(&fingerprint)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SlotRange {
    pub first: u16,
    pub last: u16,
    pub epoch: u64,
}

impl SlotRange {
    pub fn valid(self) -> bool {
        self.first <= self.last && usize::from(self.last) < SLOT_COUNT && self.epoch > 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Lease {
    pub expires_ms: u64,
    pub not_before_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Move {
    pub operation: Uuid,
    pub target: WorkerId,
    #[serde(default)]
    pub replica_only: bool,
    pub freeze_requested: bool,
    pub frozen: Option<Frozen>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Frozen {
    pub lease_expires_ms: u64,
    pub sequence: u64,
    pub upper_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct ReplicaSeed {
    pub previous_primary: WorkerId,
    pub previous_epoch: u64,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Slot {
    pub epoch: u64,
    pub primary: Option<WorkerId>,
    pub replica: Option<WorkerId>,
    pub lease: Option<Lease>,
    pub moving: Option<Move>,
    #[serde(default)]
    pub replica_seed: Option<ReplicaSeed>,
    /// Only a newly allocated or explicitly cold-reset primary has permission
    /// to initialize an empty slot. Promotions and moves require a baseline.
    pub cold: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(test, feature = "sim"), derive(Hash))]
pub(super) struct Topology {
    pub generation: u64,
    pub revision: u64,
    pub workers: BTreeMap<u64, Member>,
    pub slots: Vec<Slot>,
    #[serde(default)]
    pub draining: BTreeSet<u64>,
    #[serde(default)]
    pub drained: BTreeSet<u64>,
    #[serde(default)]
    pub ready_primaries: usize,
    #[serde(default)]
    pub ready_replicas: usize,
}

impl Default for Topology {
    fn default() -> Self {
        Self {
            generation: 1,
            revision: 0,
            workers: BTreeMap::new(),
            slots: vec![Slot::default(); SLOT_COUNT],
            draining: BTreeSet::new(),
            drained: BTreeSet::new(),
            ready_primaries: 0,
            ready_replicas: 0,
        }
    }
}

impl Topology {
    pub fn member(&self, identity: WorkerId) -> Option<&Member> {
        self.workers
            .get(&identity.node)
            .filter(|worker| worker.id == identity)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Progress {
    pub slot: u16,
    pub epoch: u64,
    pub sequence: u64,
    pub baseline: bool,
    #[serde(default)]
    pub serving_primary: bool,
    pub frozen: Option<Frozen>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Heartbeat {
    pub version: u16,
    pub generation: u64,
    pub revision: u64,
    pub worker: WorkerId,
    pub progress: Vec<Progress>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Update {
    pub version: u16,
    pub generation: u64,
    pub revision: u64,
    pub worker: WorkerId,
    pub topology: Option<Topology>,
    pub grants: Vec<Grant>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Grant {
    pub range: SlotRange,
    pub lease: Lease,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Admin {
    Init { groups: usize },
    Status,
    Enroll { member: Member },
    Rebalance { first: u16, last: u16, target: u64 },
    Drain { worker: u64 },
    Replace { member: Member },
    ColdReset { slot: Option<u16> },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Request {
    Enroll(Member),
    Heartbeat(Heartbeat),
    Admin {
        version: u16,
        operation: Uuid,
        command: Admin,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Reply {
    Update(Update),
    Status(Topology),
    Applied { operation: Uuid, revision: u64 },
    Retry,
    Refused(String),
}
