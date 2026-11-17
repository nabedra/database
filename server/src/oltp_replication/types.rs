//! Shared identities for typed multi-group storage and transport.
//!
//! Control, timestamp and status protocols use these metadata types. Application
//! requests and pending queue records do not belong in their durable commands.
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt, ops::Deref};
use uuid::Uuid;

/// The typed group namespace version, independent of the physical pool format
/// and of the earlier experimental single-group engine format.
pub const GROUP_FORMAT_VERSION: u32 = 1;
pub const MAX_PAGE_ITEMS: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invalid(pub &'static str);
impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Invalid {}

/// A byte-limited string. Deserialize checks the borrowed value before owning
/// it. The enclosing transport also reserves its bounded decode scratch space.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Text<const N: usize>(String);
impl<const N: usize> Text<N> {
    pub fn new(value: String) -> Result<Self, Invalid> {
        if value.len() > N {
            return Err(Invalid("text exceeds its byte limit"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn capacity_bytes(&self) -> usize {
        self.0.capacity()
    }
}
impl<const N: usize> TryFrom<&str> for Text<N> {
    type Error = Invalid;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() > N {
            return Err(Invalid("text exceeds its byte limit"));
        }
        Ok(Self(value.to_owned()))
    }
}
impl<const N: usize> Deref for Text<N> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}
impl<const N: usize> fmt::Display for Text<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl<const N: usize> Serialize for Text<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}
impl<'de, const N: usize> Deserialize<'de> for Text<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<const N: usize>;
        impl<const N: usize> de::Visitor<'_> for Visitor<N> {
            type Value = Text<N>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a string of at most {N} bytes")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Text::try_from(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor::<N>)
    }
}

/// A per-message list limit. This bounds one page, never the database's total
/// number of workers, groups, shards, rows, or transaction records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T>(Vec<T>);
impl<T> Page<T> {
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }
    pub fn new(items: Vec<T>) -> Result<Self, Invalid> {
        if items.len() > MAX_PAGE_ITEMS {
            return Err(Invalid("page exceeds its item limit"));
        }
        Ok(Self(items))
    }
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}
impl<T> Deref for Page<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Serialize> Serialize for Page<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Page<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Visitor<T> {
            type Value = Page<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a page of at most {MAX_PAGE_ITEMS} items")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut items = Vec::new();
                items
                    .try_reserve_exact(seq.size_hint().unwrap_or(0).min(MAX_PAGE_ITEMS))
                    .map_err(de::Error::custom)?;
                loop {
                    if items.len() == MAX_PAGE_ITEMS {
                        if seq.next_element::<de::IgnoredAny>()?.is_some() {
                            return Err(de::Error::custom("page exceeds its item limit"));
                        }
                        break;
                    }
                    let Some(value) = seq.next_element()? else {
                        break;
                    };
                    if items.len() == items.capacity() {
                        let extra = items.capacity().max(1).min(MAX_PAGE_ITEMS - items.len());
                        items.try_reserve_exact(extra).map_err(de::Error::custom)?;
                    }
                    items.push(value);
                }
                Ok(Page(items))
            }
        }
        deserializer.deserialize_seq(Visitor(std::marker::PhantomData))
    }
}

/// Canonical lowercase SHA-256. The wire decoder never allocates an owned
/// digest string, and the in-memory representation is fixed size.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);
impl Digest {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn parse(value: &str) -> Result<Self, Invalid> {
        if value.len() != 64 {
            return Err(Invalid("expected a canonical SHA-256 digest"));
        }
        let mut bytes = [0; 32];
        for (out, pair) in bytes.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
            let nibble = |v| match v {
                b'0'..=b'9' => Ok(v - b'0'),
                b'a'..=b'f' => Ok(v - b'a' + 10),
                _ => Err(Invalid("expected a canonical SHA-256 digest")),
            };
            *out = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        Ok(Self(bytes))
    }
}
impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut text = [0; 64];
        const HEX: &[u8] = b"0123456789abcdef";
        for (pair, byte) in text.as_chunks_mut::<2>().0.iter_mut().zip(self.0) {
            pair[0] = HEX[(byte >> 4) as usize];
            pair[1] = HEX[(byte & 15) as usize];
        }
        // All bytes were selected from the ASCII alphabet above.
        serializer.serialize_str(std::str::from_utf8(&text).map_err(serde::ser::Error::custom)?)
    }
}
impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Digest;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a canonical SHA-256 digest")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Digest, E> {
                Digest::parse(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum GroupKind {
    Control,
    Timestamps,
    Status { bucket: u32 },
    Catalog,
    Data { table_id: Uuid, shard_id: Uuid },
}
/// Neutral durable consensus position; zero fields remain representable for
/// opaque storage-suite/initial membership records. Admission validates actors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedPosition {
    pub term: u64,
    pub node_id: u64,
    pub index: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupIdentity {
    pub cluster_id: Uuid,
    pub database: Text<128>,
    pub group_id: Uuid,
    pub kind: GroupKind,
    pub group_format_version: u32,
}
impl GroupIdentity {
    pub fn validate(&self) -> Result<(), Invalid> {
        if self.cluster_id.is_nil()
            || self.group_id.is_nil()
            || self.group_format_version != GROUP_FORMAT_VERSION
            || !crate::oltp::valid_database(self.database.as_str())
            || matches!(self.kind, GroupKind::Data { table_id, shard_id } if table_id.is_nil() || shard_id.is_nil())
        {
            return Err(Invalid("invalid typed group identity"));
        }
        Ok(())
    }
}

/// Permanent replica identity. Incarnation and certificate rotation do not
/// rewrite every hosted group's persisted Raft membership.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaMember {
    pub node_id: u64,
    pub worker_id: Uuid,
    pub pool_id: Uuid,
}
impl ReplicaMember {
    pub fn validate(&self) -> Result<(), Invalid> {
        if self.node_id == 0 || self.worker_id.is_nil() || self.pool_id.is_nil() {
            return Err(Invalid("invalid permanent replica identity"));
        }
        Ok(())
    }
}
pub fn validate_voters(voters: &[ReplicaMember]) -> Result<(), Invalid> {
    let mut nodes = std::collections::BTreeSet::new();
    let mut workers = std::collections::BTreeSet::new();
    if voters.is_empty() {
        return Err(Invalid("a voter set cannot be empty"));
    }
    for voter in voters {
        voter.validate()?;
        if !nodes.insert(voter.node_id) || !workers.insert(voter.worker_id) {
            return Err(Invalid("duplicate replica or physical voter"));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerEnrollment {
    pub worker_id: Uuid,
    pub incarnation: Uuid,
    pub certificate_sha256: Digest,
    pub endpoint: Text<255>,
    pub server_name: Text<253>,
    pub enrollment_revision: u64,
}
impl WorkerEnrollment {
    pub fn validate(&self) -> Result<(), Invalid> {
        if self.worker_id.is_nil()
            || self.incarnation.is_nil()
            || self.enrollment_revision == 0
            || self.endpoint.is_empty()
            || self.server_name.is_empty()
            || self.endpoint.chars().any(char::is_whitespace)
            || self.server_name.chars().any(char::is_whitespace)
        {
            return Err(Invalid("invalid current worker enrollment"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub run_id: Uuid,
    pub owner_term: u64,
}
impl Authority {
    pub fn validate(self) -> Result<(), Invalid> {
        if self.run_id.is_nil() || self.owner_term == 0 {
            return Err(Invalid("invalid run or owner term"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityView {
    pub run_id: Uuid,
    pub owner_term: u64,
    pub membership_revision: u64,
}
impl AuthorityView {
    pub fn authority(self) -> Authority {
        Authority {
            run_id: self.run_id,
            owner_term: self.owner_term,
        }
    }
    pub fn validate(self) -> Result<(), Invalid> {
        self.authority().validate()?;
        if self.membership_revision == 0 {
            return Err(Invalid("invalid ready membership revision"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TxnId {
    pub request_id: Uuid,
    pub attempt: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(std::num::NonZeroU64);
impl Timestamp {
    pub fn new(value: u64) -> Result<Self, Invalid> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(Invalid("timestamp must be positive"))
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}
pub type CommitTs = Timestamp;
pub type ReadTs = Timestamp;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinIdentity {
    pub run_id: Uuid,
    pub request_id: Uuid,
    pub digest: Digest,
    pub nonce: Uuid,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoragePin {
    pub pin_id: Uuid,
    pub read_ts: ReadTs,
    pub allocator_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentity {
    pub worker_id: Uuid,
    pub incarnation: Uuid,
    pub certificate_sha256: Digest,
    pub enrollment_revision: u64,
}
impl From<&WorkerEnrollment> for WorkerIdentity {
    fn from(worker: &WorkerEnrollment) -> Self {
        Self {
            worker_id: worker.worker_id,
            incarnation: worker.incarnation,
            certificate_sha256: worker.certificate_sha256,
            enrollment_revision: worker.enrollment_revision,
        }
    }
}
impl WorkerIdentity {
    pub fn validate(self) -> Result<(), Invalid> {
        if self.worker_id.is_nil() || self.incarnation.is_nil() || self.enrollment_revision == 0 {
            return Err(Invalid("invalid enrolled worker identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyMembership {
    pub view: AuthorityView,
    pub ready_root: Digest,
    pub ready_count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBinding {
    pub run_id: Uuid,
    pub request_id: Uuid,
    pub digest: Digest,
    pub accepted_context: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalCopyStatement {
    pub ready: ReadyMembership,
    pub request: RequestBinding,
    pub terminal_version: u64,
    pub response_hash: Digest,
    pub pin_identity: PinIdentity,
    pub storage_pin: StoragePin,
}
impl TerminalCopyStatement {
    pub fn validate(&self) -> Result<(), Invalid> {
        self.ready.view.validate()?;
        if self.ready.ready_count == 0
            || self.request.request_id.is_nil()
            || self.request.run_id != self.ready.view.run_id
            || self.terminal_version == 0
            || self.pin_identity.run_id != self.request.run_id
            || self.pin_identity.request_id != self.request.request_id
            || self.pin_identity.digest != self.request.digest
            || self.pin_identity.nonce.is_nil()
            || self.storage_pin.pin_id.is_nil()
            || self.storage_pin.allocator_epoch == 0
        {
            return Err(Invalid("invalid terminal-copy statement"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinTicket {
    pub ticket_id: Uuid,
    pub target: WorkerIdentity,
    pub source_ready: ReadyMembership,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCopyStatement {
    pub ticket: JoinTicket,
    pub source: WorkerIdentity,
    pub snapshot_id: Uuid,
    pub snapshot_cut: u64,
    pub final_cut: u64,
    pub record_version_hash: Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRef {
    pub status_group_id: Uuid,
    pub digest: Digest,
    pub participant_count: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantPlan {
    pub group: GroupIdentity,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub read_ts: ReadTs,
    pub write_root: Digest,
    pub write_count: u64,
    pub page_count: u64,
}
impl ParticipantPlan {
    pub fn validate(&self) -> Result<(), Invalid> {
        self.group.validate()?;
        if !matches!(self.group.kind, GroupKind::Catalog | GroupKind::Data { .. })
            || self.schema_epoch == 0
            || self.routing_epoch == 0
            || (self.write_count != 0 && self.page_count == 0)
        {
            return Err(Invalid("invalid fixed participant plan"));
        }
        Ok(())
    }
}
/// The write root is fixed BEFORE the manifest digest is computed. Its hash
/// domain cannot include that final manifest digest, which would create a cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedParticipant {
    pub txn: TxnId,
    pub manifest: ManifestRef,
    pub participant: ParticipantPlan,
    pub authority: Authority,
    pub sealed_at: AppliedPosition,
    pub result_digest: Digest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortReason {
    Conflict,
    Validation,
    ResourceLimit,
    AbandonedByFence,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TxnDecision {
    Commit { commit_ts: CommitTs },
    Abort { reason: AbortReason },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionStatement {
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub decided_at: AppliedPosition,
    pub decision: TxnDecision,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u02_fields_and_pages_reject_excess_before_decoding_more_items() {
        assert!(serde_json::from_str::<Text<3>>(r#""four""#).is_err());
        assert!(serde_json::from_str::<Text<3>>(r#""ññ""#).is_err());
        let mut page = vec!["1"; MAX_PAGE_ITEMS];
        page.push(r#"{"not":"an integer"}"#);
        let error =
            serde_json::from_str::<Page<u64>>(&format!("[{}]", page.join(","))).unwrap_err();
        assert!(error.to_string().contains("page exceeds"));
        assert!(serde_json::from_str::<Timestamp>("0").is_err());
        assert!(serde_json::from_str::<Timestamp>("18446744073709551615").is_ok());
    }

    #[test]
    fn o06_permanent_membership_excludes_duplicate_physical_voters() {
        let member = ReplicaMember {
            node_id: 1,
            worker_id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
        };
        let mut duplicate = member.clone();
        duplicate.node_id = 2;
        duplicate.pool_id = Uuid::new_v4();
        assert!(validate_voters(&[member.clone(), duplicate]).is_err());
        assert!(validate_voters(&[member]).is_ok());
    }

    #[test]
    fn u02_digest_has_one_canonical_fixed_size_representation() {
        let digest = Digest::from_bytes([0xab; 32]);
        let wire = serde_json::to_string(&digest).unwrap();
        assert_eq!(wire.len(), 66);
        assert_eq!(serde_json::from_str::<Digest>(&wire).unwrap(), digest);
        assert!(Digest::parse(&"AB".repeat(32)).is_err());
        assert!(Digest::parse(&"0".repeat(65)).is_err());
    }
}
