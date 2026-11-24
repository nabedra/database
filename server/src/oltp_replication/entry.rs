//! Owned OpenRaft entries for the typed group adapters. Entry clones share the
//! payload and decode owner. Membership is bounded per replica group, not by
//! the number of workers or shards in the database.
use super::{
    owned::{OwnedCommand, OwnedPayload},
    types::{MAX_PAGE_ITEMS, Page, ReplicaMember},
};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ScratchLease, WorkerSharedResources},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use openraft::{entry::RaftEntry, vote::RaftLeaderId};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    mem::size_of,
    sync::{Arc, OnceLock},
};

pub type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
pub type CommittedLeaderId = <LeaderId as RaftLeaderId>::Committed;
pub type LogId = openraft::LogId<CommittedLeaderId>;
pub type Vote = openraft::impls::Vote<LeaderId>;
pub type Membership = openraft::impls::Membership<u64, ReplicaMember>;
pub(super) fn reserve_raft_metadata(
    command: &OwnedCommand<impl OwnedPayload>,
    resident: &super::multiplex::ReplicaLease,
    bytes: usize,
) -> Result<(), ProtocolError> {
    command.attach_raft_metadata(
        || resident.proposal_metadata(bytes),
        |owner| resident.owns_metadata(owner) && owner.bytes() >= bytes,
    )
}

pub type Payload<D> = openraft::impls::EntryPayload<OwnedCommand<D>, Membership>;
pub trait CommandData: OwnedPayload + fmt::Display {}
impl<T: OwnedPayload + fmt::Display> CommandData for T {}
pub const MAX_GROUP_REPLICA_NODES: usize = MAX_PAGE_ITEMS;

pub struct OwnedRaftEntry<D: CommandData> {
    pub log_id: LogId,
    payload: Arc<Payload<D>>,
    owner: Arc<OnceLock<EntryOwner>>,
}
enum EntryOwner {
    Decode(Arc<ScratchLease>),
    Resident(Arc<super::multiplex::ReplicaMetadata>),
}
impl<D: CommandData> Clone for OwnedRaftEntry<D> {
    fn clone(&self) -> Self {
        Self {
            log_id: self.log_id,
            payload: self.payload.clone(),
            owner: self.owner.clone(),
        }
    }
}
impl<D: CommandData> fmt::Debug for OwnedRaftEntry<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedRaftEntry")
            .field("log_id", &self.log_id)
            .field("owned", &self.owner.get().is_some())
            .finish_non_exhaustive()
    }
}
impl<D: CommandData> fmt::Display for OwnedRaftEntry<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.log_id, self.payload)
    }
}
impl<D: CommandData> OwnedRaftEntry<D> {
    pub(crate) fn normal_metadata_bytes() -> usize {
        size_of::<Payload<D>>()
            + size_of::<OnceLock<EntryOwner>>()
            + 256
            + size_of::<Self>() * 2
            + 64
    }
    pub(crate) fn charge_resident(
        self,
        resident: &super::multiplex::ReplicaLease,
    ) -> Result<Self, ProtocolError> {
        if self.ensure_charged().is_ok() {
            return Ok(self);
        }
        let bytes = self.resident_bytes()?;
        let owner = match self.payload() {
            Payload::Normal(command) => match command.raft_metadata() {
                Some(owner) if resident.owns_metadata(&owner) && owner.bytes() >= bytes => owner,
                Some(_) => return Err(invalid("entry proposal residency differs")),
                None => Arc::new(resident.log_metadata(bytes)?),
            },
            _ => Arc::new(resident.log_metadata(bytes)?),
        };
        if self.owner.set(EntryOwner::Resident(owner)).is_err() {
            self.ensure_charged()?
        }
        self.ensure_charged()?;
        Ok(self)
    }
    fn resident_bytes(&self) -> Result<usize, ProtocolError> {
        if let Payload::Normal(command) = self.payload() {
            command.ensure_charged()?
        }
        check_membership(self.payload())?;
        // Include the bounded append vector cell and allocation framing before
        // the vector grows. Payload clones share this same resident owner.
        self.heap_bytes()?
            .checked_add(size_of::<Self>() * 2 + 64)
            .ok_or_else(|| invalid("entry metadata overflow"))
    }
    /// [`Self::charge_resident`] for the log append, where a refusal would
    /// stop the Raft core: an entry without a proposal owner (the leader's own
    /// Blank and Membership entries) waits for log metadata room instead of
    /// failing (RV-APPLY F5).
    pub(crate) async fn charge_resident_wait(
        self,
        resident: &super::multiplex::ReplicaLease,
    ) -> Result<Self, ProtocolError> {
        if self.ensure_charged().is_ok() {
            return Ok(self);
        }
        if let Payload::Normal(command) = self.payload()
            && command.raft_metadata().is_some()
        {
            return self.charge_resident(resident);
        }
        let bytes = self.resident_bytes()?;
        let owner = Arc::new(resident.log_metadata_wait(bytes).await?);
        if self.owner.set(EntryOwner::Resident(owner)).is_err() {
            self.ensure_charged()?
        }
        self.ensure_charged()?;
        Ok(self)
    }
    pub fn payload(&self) -> &Payload<D> {
        &self.payload
    }
    pub fn ensure_charged(&self) -> Result<(), ProtocolError> {
        match self.owner.get() {
            None => return Err(invalid("entry has no owned decode reservation")),
            Some(EntryOwner::Resident(owner)) if owner.bytes() == 0 => {
                return Err(invalid("empty entry residency"));
            }
            _ => {}
        }
        if let Payload::Normal(command) = self.payload() {
            command.ensure_charged()?;
        }
        check_membership(self.payload())
    }
    /// New leader entries already own their application command. This adds the
    /// entry/membership metadata charge before logging or replication.
    pub fn charge_leader(
        self,
        shared: &WorkerSharedResources,
        class: ByteClass,
    ) -> Result<Self, ProtocolError> {
        if self.ensure_charged().is_ok() {
            return Ok(self);
        }
        if let Payload::Normal(command) = self.payload() {
            command.ensure_charged()?;
        }
        check_membership(self.payload())?;
        let retained = self.clone();
        match OwnedCommand::new_owned(self, shared, class).and_then(Self::from_owned) {
            Ok(entry) => Ok(entry),
            // Concurrent replication/logging of the same leader entry may
            // install the shared metadata owner first. Its valid owner wins;
            // the losing temporary reservation is released by the codec.
            Err(_) if retained.ensure_charged().is_ok() => Ok(retained),
            Err(error) => Err(error),
        }
    }
    pub fn decode(
        input: &ChargedBytes,
        shared: &WorkerSharedResources,
        class: ByteClass,
        max_decode_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        Self::from_owned(OwnedCommand::decode_json(
            input,
            shared,
            class,
            max_decode_bytes,
        )?)
    }
    /// Native log_read_owned returns this envelope directly from the pinned
    /// value's decode task. No apply or small-reply buffer leaves that task.
    pub fn from_owned(owned: OwnedCommand<Self>) -> Result<Self, ProtocolError> {
        let entry = owned.payload()?.clone();
        let owner = owned.decode_owner()?;
        if let Some(existing) = entry.owner.get() {
            if !matches!(existing,EntryOwner::Decode(existing) if Arc::ptr_eq(existing,&owner)) {
                return Err(invalid("entry owner changed"));
            }
        } else {
            entry
                .owner
                .set(EntryOwner::Decode(owner))
                .map_err(|_| invalid("entry owner raced"))?;
        }
        entry.ensure_charged()?;
        Ok(entry)
    }
}
impl<D: CommandData> RaftEntry for OwnedRaftEntry<D> {
    type CommittedLeaderId = CommittedLeaderId;
    type Payload = Payload<D>;
    fn new(log_id: LogId, payload: Self::Payload) -> Self {
        Self {
            log_id,
            payload: Arc::new(payload),
            owner: Arc::new(OnceLock::new()),
        }
    }
    fn log_id_parts(&self) -> (&CommittedLeaderId, u64) {
        (&self.log_id.leader_id, self.log_id.index)
    }
    fn set_log_id(&mut self, new: LogId) {
        self.log_id = new;
    }
    fn get_membership(&self) -> Option<Membership> {
        match self.payload() {
            Payload::Membership(membership) => Some(membership.clone()),
            _ => None,
        }
    }
}
impl<D: CommandData> OwnedPayload for OwnedRaftEntry<D> {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let inner = match self.payload() {
            Payload::Normal(command) => command.uncharged_heap_bytes()?,
            Payload::Membership(membership) => {
                check_membership(self.payload())?;
                let nodes = membership
                    .nodes()
                    .count()
                    .checked_mul(size_of::<(u64, ReplicaMember)>() + 128)
                    .ok_or_else(|| invalid("membership heap overflow"))?;
                membership
                    .get_joint_config()
                    .iter()
                    .try_fold(nodes, |sum, set| {
                        sum.checked_add(set.len().checked_mul(96)?)
                    })
                    .ok_or_else(|| invalid("membership heap overflow"))?
            }
            Payload::Blank => 0,
        };
        inner
            .checked_add(size_of::<Payload<D>>() + size_of::<OnceLock<EntryOwner>>() + 256)
            .ok_or_else(|| invalid("entry heap overflow"))
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        if let Payload::Normal(command) = self.payload() {
            command.attach_from_codec(owner.clone())?;
        }
        if let Some(existing) = self.owner.get() {
            if !matches!(existing,EntryOwner::Decode(existing) if Arc::ptr_eq(existing,&owner)) {
                return Err(invalid("entry decode owner changed"));
            }
        } else {
            self.owner
                .set(EntryOwner::Decode(owner))
                .map_err(|_| invalid("entry decode owner raced"))?;
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        match self.payload() {
            Payload::Normal(command) => command.is_attached(),
            _ => true,
        }
    }
}
impl<D: CommandData> Serialize for OwnedRaftEntry<D> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        check_membership(self.payload()).map_err(serde::ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(bound = "")]
        struct Borrowed<'a, D: CommandData> {
            log_id: &'a LogId,
            payload: &'a Payload<D>,
        }
        Borrowed {
            log_id: &self.log_id,
            payload: self.payload(),
        }
        .serialize(serializer)
    }
}
impl<'de, D: CommandData> Deserialize<'de> for OwnedRaftEntry<D> {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        #[derive(Deserialize)]
        #[serde(bound = "", deny_unknown_fields)]
        struct Input<D: CommandData> {
            log_id: LogId,
            payload: BoundedPayload<D>,
        }
        let input = Input::<D>::deserialize(deserializer)?;
        let payload = match input.payload {
            BoundedPayload::Blank => Payload::Blank,
            BoundedPayload::Normal(command) => Payload::Normal(command),
            BoundedPayload::Membership(membership) => Payload::Membership(membership.0),
        };
        Ok(Self::new(input.log_id, payload))
    }
}
#[derive(Deserialize)]
#[serde(bound = "", deny_unknown_fields)]
enum BoundedPayload<D: CommandData> {
    Blank,
    Normal(OwnedCommand<D>),
    Membership(BoundedMembership),
}
struct BoundedMembership(Membership);
#[derive(Clone, Debug)]
pub(crate) struct BoundedStoredMembership(
    pub openraft::StoredMembership<CommittedLeaderId, Membership>,
);
impl Serialize for BoundedStoredMembership {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for BoundedStoredMembership {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Stored {
            log_id: Option<LogId>,
            membership: BoundedMembership,
        }
        let stored = Stored::deserialize(deserializer)?;
        Ok(Self(openraft::StoredMembership::new(
            stored.log_id,
            stored.membership.0,
        )))
    }
}
pub(crate) fn stored_membership(
    input: &[u8],
) -> Result<openraft::StoredMembership<CommittedLeaderId, Membership>, ProtocolError> {
    let stored: BoundedStoredMembership =
        serde_json::from_slice(input).map_err(|_| invalid("invalid bounded stored membership"))?;
    Ok(stored.0)
}
pub(super) fn activation_membership_matches(
    bytes: &[u8],
    voters: &[ReplicaMember],
    minimum: super::types::AppliedPosition,
    exact: bool,
) -> Result<bool, ProtocolError> {
    let stored = stored_membership(bytes)?;
    let membership = stored.membership();
    let joint = membership.get_joint_config();
    if joint.len() != 1 || (exact && stored.log_id().map(super::hosted::position) != Some(minimum)) {
        return Ok(false);
    }
    let voting = &joint[0];
    let actual = membership
        .nodes()
        .filter(|(id, _)| voting.contains(id))
        .map(|(_, replica)| replica.clone())
        .collect::<Vec<ReplicaMember>>();
    Ok(actual.as_slice() == voters)
}
impl<'de> Deserialize<'de> for BoundedMembership {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Input {
            configs: JointConfigs,
            nodes: NodeMap,
        }
        let input = Input::deserialize(deserializer)?;
        Membership::new(input.configs.0, input.nodes.0)
            .map(Self)
            .map_err(de::Error::custom)
    }
}
struct JointConfigs(Vec<BTreeSet<u64>>);
impl<'de> Deserialize<'de> for JointConfigs {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = JointConfigs;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("at most two bounded joint voter configurations")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut sets = Vec::with_capacity(2);
                while sets.len() < 2 {
                    let Some(ids) = seq.next_element::<Page<u64>>()? else {
                        return Ok(JointConfigs(sets));
                    };
                    let count = ids.len();
                    let set: BTreeSet<_> = ids.iter().copied().collect();
                    if count != set.len() {
                        return Err(de::Error::custom("duplicate voter ID"));
                    }
                    sets.push(set);
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("too many joint voter configurations"));
                }
                Ok(JointConfigs(sets))
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}
struct NodeMap(BTreeMap<u64, ReplicaMember>);
impl<'de> Deserialize<'de> for NodeMap {
    fn deserialize<T: Deserializer<'de>>(deserializer: T) -> Result<Self, T::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = NodeMap;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a bounded replica map")
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut nodes = BTreeMap::new();
                while nodes.len() < MAX_GROUP_REPLICA_NODES {
                    let Some(id) = map.next_key::<u64>()? else {
                        return Ok(NodeMap(nodes));
                    };
                    if nodes.contains_key(&id) {
                        return Err(de::Error::custom("duplicate replica ID"));
                    }
                    nodes.insert(id, map.next_value()?);
                }
                if map.next_key::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("too many replicas in one group"));
                }
                Ok(NodeMap(nodes))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}
fn check_membership<D: CommandData>(payload: &Payload<D>) -> Result<(), ProtocolError> {
    if let Payload::Membership(membership) = payload {
        if membership.nodes().count() > MAX_GROUP_REPLICA_NODES
            || membership.get_joint_config().len() > 2
            || membership
                .get_joint_config()
                .iter()
                .any(|set| set.len() > MAX_GROUP_REPLICA_NODES)
        {
            return Err(invalid(
                "replica group membership exceeds its bounded metadata budget",
            ));
        }
    }
    Ok(())
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_resources::config::WorkerResources;
    use crate::{
        oltp::{mvcc::CanonicalJson, pool::SharedResourceConfig},
    };
    #[derive(Serialize, Deserialize)]
    struct Command {
        value: CanonicalJson,
    }
    impl fmt::Display for Command {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("fixed")
        }
    }
    impl OwnedPayload for Command {
        fn heap_bytes(&self) -> Result<usize, ProtocolError> {
            self.value.heap_bytes()
        }
        fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
            self.value.attach_charge(owner)
        }
        fn fields_attached(&self) -> bool {
            self.value.is_charged()
        }
    }
    fn shared() -> Arc<WorkerSharedResources> {
        WorkerSharedResources::new(
            1,
            WorkerResources {
                max_threads: 8,
                memory_limit_bytes: 512 << 20,
                task_memory_limit_bytes: 128 << 20,
                control_memory_reserve_bytes: 64 << 20,
            },
            SharedResourceConfig::default(),
        )
        .unwrap()
    }
    #[test]
    fn f03_m02_entry_and_extracted_field_keep_decode_owner_after_frame_drops() {
        let shared = shared();
        let baseline = shared.scheduler().usage()[0].2;
        let json = shared.copy_bytes(br#"{"log_id":{"leader_id":{"term":3,"node_id":7},"index":4},"payload":{"Normal":{"value":{"key":"value"}}}}"#).unwrap();
        let entry =
            OwnedRaftEntry::<Command>::decode(&json, &shared, ByteClass::Data, 1 << 20).unwrap();
        let clone = entry.clone();
        let Payload::Normal(command) = entry.payload() else {
            panic!("normal payload")
        };
        let field = command.payload().unwrap().value.clone();
        let used = shared.scheduler().usage()[0].2;
        assert!(used > baseline);
        drop(entry);
        drop(json);
        assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
        assert_eq!(shared.scheduler().usage()[0].2, used);
        drop(clone);
        assert_eq!(shared.scheduler().usage()[0].2, used);
        assert_eq!(field.as_str(), r#"{"key":"value"}"#);
        drop(field);
        assert_eq!(shared.scheduler().usage()[0].2, baseline);
    }
    #[test]
    fn o05_raw_entries_reject_and_leader_metadata_is_idempotently_charged() {
        let shared = shared();
        let raw: OwnedRaftEntry<Command> = serde_json::from_str(r#"{"log_id":{"leader_id":{"term":3,"node_id":7},"index":4},"payload":{"Normal":{"value":1}}}"#).unwrap();
        assert!(raw.ensure_charged().is_err());
        assert!(raw.charge_leader(&shared, ByteClass::Data).is_err());
        let entry = OwnedRaftEntry::<Command>::new(
            LogId::new(CommittedLeaderId::new(3, 7), 4),
            Payload::Blank,
        );
        let clone = entry.clone();
        let charged = entry.charge_leader(&shared, ByteClass::Data).unwrap();
        clone.ensure_charged().unwrap();
        let used = shared.scheduler().usage()[0].2;
        charged.charge_leader(&shared, ByteClass::Data).unwrap();
        assert_eq!(shared.scheduler().usage()[0].2, used);
    }
    #[test]
    fn u02_membership_rejects_oversized_and_duplicate_metadata_before_handoff() {
        let mut ids = (0..=MAX_GROUP_REPLICA_NODES)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let wire = |ids: &str| {
            format!(
                r#"{{"log_id":{{"leader_id":{{"term":1,"node_id":1}},"index":0}},"payload":{{"Membership":{{"configs":[[{ids}]],"nodes":{{}}}}}}}}"#
            )
        };
        assert!(serde_json::from_str::<OwnedRaftEntry<Command>>(&wire(&ids)).is_err());
        ids = "1,1".into();
        assert!(serde_json::from_str::<OwnedRaftEntry<Command>>(&wire(&ids)).is_err());
        let third = r#"{"log_id":{"leader_id":{"term":1,"node_id":1},"index":0},"payload":{"Membership":{"configs":[[1],[1],[1]],"nodes":{}}}}"#;
        assert!(serde_json::from_str::<OwnedRaftEntry<Command>>(third).is_err());
    }
}
