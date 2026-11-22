//! One-entry direct commits (plan section 4).
//!
//! A `DirectCommit` carries the exact versions, index changes and response of
//! one request. Apply validates it against the replicated state in a fixed
//! order and either writes everything in one batch with the applied position
//! or writes nothing. A dedup record in the `'D'` namespace makes a retry with
//! the same request id and body return the original result.
//!
//! Dedup records expire through a separate replicated command,
//! `DirectExpire { cutoff_unix_ms }`. The range leader computes the cutoff
//! from its own clock (`expiry_cutoff`) and proposes it; apply reads no clock,
//! so every replica deletes exactly the same records. An `'E'` index ordered
//! by `applied_unix_ms` makes each page of deletions a bounded prefix scan.
//!
//! Exactly-once across paths (plan addendum 2026-11-19): before a request
//! that may have run directly takes the queued path, the anchor's leader
//! commits `DirectForward`. It returns the committed result when a dedup
//! record exists, and otherwise writes a *forward marker* under the same
//! `'D'` key. A direct attempt that finds the marker answers `Forwarded`,
//! so it can never apply after the queued path accepted the request.
//! Markers share the dedup records' digest rules, expiry and carriage.
//! `ParticipantMachine::direct_dedup` reads either behind the leader's
//! linearizable barrier.
//!
//! Replies map to the leader executor's actions (plan section 6):
//! `Forwarded` means the request belongs to the queued path (Fallback);
//! `SplitInProgress` (a split barrier, a frozen source or an unimported
//! child) means Fallback or a bounded wait, never a route refresh;
//! `StaleEpoch` (the schema or routing epoch changed) refreshes the route;
//! `StaleAuthority` means the fence moved; `Incomplete` means a queued intent
//! holds a key, so fall back to the queued path; `Conflict` means re-read
//! (stale `expected_version`, `commit_ts` not above the newest version or the
//! range floor, or the request id was used with a different body);
//! `ResourceLimit` means the entry is too large for one direct commit, so use
//! the queued path; `InvalidBinding` is a malformed command.
//!
//! Clock skew (RV-T4 F7): every record is stamped with
//! `max(applied_unix_ms, group high-water)`, and a `DirectExpire` cutoff is
//! clamped to `high-water - retention_ms`, so a leader whose clock runs ahead
//! cannot delete records younger than the retention window unless it also
//! commits with that clock. Apply reads no clock.
use super::*;
use crate::oltp::{mvcc::CanonicalJson, pool::ScratchLease};

pub(crate) mod clock;
#[allow(unused_imports)] // Consumed by the leader executor (T5).
pub(crate) use clock::{ClockRejection, LeaderToken, RANGE_CLOCK_WAIT, RangeClock};

/// Logical key tag of `MvccKey::DirectDedup`. It sorts after every head tag.
pub(crate) const DEDUP_TAG: u8 = 14;
/// Physical namespace of dedup records: `'D' || logical dedup key`.
pub(super) const DEDUP: u8 = b'D';
/// Expiry index: `'E' || applied_unix_ms (big endian) || logical dedup key`.
pub(super) const EXPIRY: u8 = b'E';
/// The highest commit timestamp any direct commit applied on this group.
/// A split freeze below it is refused, because its images would miss it.
const DIRECT_HIGH: &[u8] = b"mvcc/direct-high/v1";
/// The highest `applied_unix_ms` stamped on this group (RV-T4 F7).
const UNIX_HIGH: &[u8] = b"mvcc/direct-unix-high/v1";

/// `direct_dedup_retention_ms` default: 24 hours. `result_retention_ms` is
/// bounded against it at startup (RV-FINAL M2).
pub(crate) const DEFAULT_DIRECT_DEDUP_RETENTION_MS: u64 = crate::config::DIRECT_MARKER_RETENTION_MS;
/// Dedup records deleted per `DirectExpire` entry at most.
pub(crate) const MAX_DIRECT_EXPIRE_ITEMS: u16 = 128;
/// Rows per command. `insert_many` above this uses the queued path.
pub(crate) const MAX_DIRECT_WRITES: usize = 4096;
/// Index changes per row.
pub(crate) const MAX_DIRECT_INDEX_CHANGES: usize = 256;
/// The exact result must fit a participant reply, so a replay can return it.
pub(crate) const MAX_DIRECT_RESULT_BYTES: usize = 48 << 10;
/// Native key prefix and write-batch framing per put or delete.
const OP_OVERHEAD: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectCommit {
    /// Run and group fence, checked at apply.
    pub authority: Authority,
    pub table_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub request_id: Uuid,
    /// SHA-256 of the exact request body.
    pub digest: Digest,
    pub commit_ts: CommitTs,
    /// Rows in strictly ascending key order. One for point operations.
    pub writes: Vec<DirectWrite>,
    /// Exact `data` value of the response.
    pub result: CanonicalJson,
    /// Leader wall clock when proposing. Stored in the dedup record and
    /// compared only with a `DirectExpire` cutoff, never with a local clock.
    pub applied_unix_ms: u64,
}
/// GC2: several requests' commits to one range in one Data entry. Proposed
/// only while Control's `data.direct.batch` cluster feature is enabled: an
/// older replica cannot decode the variant. Apply validates each member in
/// the section 4 order against the state the members before it left, and a
/// rejected member writes nothing while the others apply. Members never
/// share a key: the leader holds their key latches from read to apply, and
/// apply refuses a member that shares a key with an earlier one. Each member
/// keeps its own request id, digest, `commit_ts`, result and dedup record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectBatch {
    pub members: Vec<DirectCommit>,
}
/// Members per `DirectBatch` at most (and `MAX_DIRECT_ENTRY_BYTES` encoded).
pub(crate) const MAX_DIRECT_BATCH_MEMBERS: usize = 64;
/// One member's outcome in `DirectReply::Batch`. A replayed member carries no
/// result, so the reply stays within the participant reply bound whatever
/// the stored results weigh: the leader reads the record it just applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectMemberReply {
    Applied { commit_ts: CommitTs },
    Replayed,
    Forwarded,
    Rejected(Fault),
}
/// Delete dedup records and forward markers with `applied_unix_ms` below
/// `min(cutoff_unix_ms, high-water - retention_ms)`, oldest first, at most
/// `max_items` per entry. The proposer repeats it until the reply says
/// `complete`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectExpire {
    pub authority: Authority,
    pub cutoff_unix_ms: u64,
    /// The retention the cutoff was computed with; bounds the cutoff by the
    /// group's own high-water, so a clock running ahead cannot shorten it.
    pub retention_ms: u64,
    pub max_items: u16,
}
/// Settle `request_id` at its anchor before the request takes the queued
/// path. Returns the committed result, or writes a forward marker so a later
/// direct attempt of the request answers `Forwarded` instead of applying.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectForward {
    pub authority: Authority,
    pub table_id: Uuid,
    /// The smallest primary key the request writes.
    pub anchor: Key,
    pub request_id: Uuid,
    /// SHA-256 of the exact request body.
    pub digest: Digest,
    /// Leader wall clock when proposing; stamps the marker for expiry.
    pub applied_unix_ms: u64,
}
/// The replicated split barrier (RV-T4 F5). While a split holds it, direct
/// commits on the range answer `SplitInProgress`. The coordinator proposes
/// it before allocating the cut, so every direct commit that applied first
/// has a timestamp below the cut. A freeze for the same split takes it over;
/// an abort or `paused: false` releases it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectPause {
    pub authority: Authority,
    pub split_id: Uuid,
    pub paused: bool,
}
/// The coordinator tells a complete split child the source's publication
/// timestamp (RV-T4 F8). The child refuses direct commits at or below it,
/// because the source served snapshots up to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ChildPublished {
    pub authority: Authority,
    pub published_at: CommitTs,
}
/// The direct-commit schema state of one Data group (`data.direct.v1`,
/// plan section 7). A group accepts direct-family commands only once it has
/// one. `epoch` is what `DirectCommit::schema_epoch` must carry; `lifecycle`
/// is the Catalog table lifecycle id (`TableRecord::lifecycle_id`) whose
/// descriptor and index set direct commits must be computed from. `None`
/// means the table was dropped: every direct commit answers `StaleEpoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectSchema {
    pub epoch: u64,
    pub lifecycle: Option<Uuid>,
}
impl DirectSchema {
    /// Whether a descriptor read from the Catalog table record with
    /// `lifecycle_id` may compute direct commits at `self.epoch`. A leader
    /// whose cached descriptor fails this check refreshes it or falls back.
    pub(crate) fn admits(&self, lifecycle_id: Uuid) -> bool {
        self.lifecycle == Some(lifecycle_id)
    }
}
/// Activates direct commits on a group, or fences them before a DDL
/// publishes. Proposed only after Control activated `data.direct.v1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectSchemaFence {
    pub authority: Authority,
    pub table_id: Uuid,
    pub change: DirectSchemaChange,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectSchemaChange {
    /// First activation, bound to the lifecycle the proposer read from the
    /// Catalog while no exclusive lifecycle guard (DDL) was held or pending
    /// on the table. A no-op when the group already has a direct schema, so
    /// a DDL fence that applied first is never undone.
    Activate { lifecycle: Uuid },
    /// The DDL coordinator commits this before the Catalog publishes the
    /// table's next lifecycle (`Some`, index publication) or its drop
    /// (`None`). It bumps the epoch, so a `DirectCommit` computed from the
    /// previous descriptor fails at apply. Repeating the same lifecycle is
    /// a no-op.
    Fence { lifecycle: Option<Uuid> },
}

/// The cutoff a leader proposes at `now_unix_ms` for `retention_ms`.
/// `None` while nothing can be old enough yet.
pub(crate) fn expiry_cutoff(now_unix_ms: u64, retention_ms: u64) -> Option<u64> {
    now_unix_ms
        .checked_sub(retention_ms.max(1))
        .filter(|cutoff| *cutoff > 0)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DirectWrite {
    pub key: MvccKey,
    /// The exact head version: `None` only when the key has no head. A
    /// deleted row keeps its tombstone's commit timestamp as its head
    /// version, as for `PreparedWrite`, so the executor passes that.
    pub expected_version: Option<CommitTs>,
    /// `None` writes a tombstone.
    pub value: Option<CanonicalJson>,
    pub index: Vec<IndexChange>,
}
/// One secondary index entry written with its row, under the same rules.
/// `expected_version` is the exact head version, as for a row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexChange {
    pub key: MvccKey,
    pub expected_version: Option<CommitTs>,
    pub value: Option<CanonicalJson>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DirectReply {
    Applied {
        commit_ts: CommitTs,
    },
    Replayed {
        result: DirectResult,
    },
    /// The request id holds a forward marker at its anchor: it belongs to
    /// the queued path. Also the reply of a `DirectForward` that wrote one.
    Forwarded,
    Rejected(Fault),
    /// GC2: one reply per `DirectBatch` member, in member order.
    Batch(Vec<DirectMemberReply>),
}
/// What `direct_dedup` finds at an anchor for a request id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DedupLookup {
    Committed {
        digest: Digest,
        commit_ts: CommitTs,
        result: DirectResult,
    },
    Forwarded {
        digest: Digest,
    },
}
/// The exact response `data` of a committed request, compared by its bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct DirectResult(Box<RawValue>);
impl DirectResult {
    pub(crate) fn get(&self) -> &str {
        self.0.get()
    }
}
impl PartialEq for DirectResult {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl Eq for DirectResult {}

/// The stored dedup value. Fields are in canonical (sorted) order, so the
/// stored bytes are canonical JSON and can travel as an image value.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DedupRecord<'a> {
    applied_unix_ms: u64,
    commit_ts: CommitTs,
    digest: Digest,
    #[serde(borrow)]
    result: &'a RawValue,
}
/// A forward marker under a dedup key, also canonical JSON. The required
/// `forwarded` field keeps the two shapes disjoint under
/// `deny_unknown_fields`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForwardMarker {
    applied_unix_ms: u64,
    digest: Digest,
    forwarded: bool,
}
enum Stored<'a> {
    Committed(DedupRecord<'a>),
    Forwarded(ForwardMarker),
}
impl<'a> Stored<'a> {
    fn decode(raw: &'a [u8]) -> Option<Self> {
        if let Ok(record) = serde_json::from_slice::<DedupRecord<'_>>(raw) {
            return Some(Self::Committed(record));
        }
        serde_json::from_slice::<ForwardMarker>(raw)
            .ok()
            .filter(|marker| marker.forwarded)
            .map(Self::Forwarded)
    }
    fn digest(&self) -> Digest {
        match self {
            Self::Committed(record) => record.digest,
            Self::Forwarded(marker) => marker.digest,
        }
    }
    fn applied_unix_ms(&self) -> u64 {
        match self {
            Self::Committed(record) => record.applied_unix_ms,
            Self::Forwarded(marker) => marker.applied_unix_ms,
        }
    }
}

impl DirectCommit {
    /// The dedup key: the anchor is the smallest key of the request.
    pub(crate) fn dedup_key(&self) -> Option<MvccKey> {
        match &self.writes.first()?.key {
            MvccKey::Row { primary_key, .. } => Some(MvccKey::DirectDedup {
                table_id: self.table_id,
                anchor: primary_key.clone(),
                request_id: self.request_id,
            }),
            _ => None,
        }
    }
    /// Every row and index key this commit writes, for `RangeClock` pending.
    pub(crate) fn keys(&self) -> Vec<MvccKey> {
        self.writes
            .iter()
            .flat_map(|write| {
                std::iter::once(write.key.clone())
                    .chain(write.index.iter().map(|change| change.key.clone()))
            })
            .collect()
    }
    fn changes(
        &self,
    ) -> impl Iterator<Item = (&MvccKey, Option<CommitTs>, Option<&CanonicalJson>)> {
        self.writes.iter().flat_map(|write| {
            std::iter::once((&write.key, write.expected_version, write.value.as_ref())).chain(
                write
                    .index
                    .iter()
                    .map(|c| (&c.key, c.expected_version, c.value.as_ref())),
            )
        })
    }
}

fn reply(tx: &mut GroupTransaction<'_>, reply: DirectReply) -> Result<Transition, ProtocolError> {
    let applied = match &reply {
        DirectReply::Applied { .. } | DirectReply::Replayed { .. } | DirectReply::Forwarded => true,
        DirectReply::Rejected(_) => false,
        DirectReply::Batch(members) => members
            .iter()
            .any(|member| !matches!(member, DirectMemberReply::Rejected(_))),
    };
    let encoded = tx.encode(&ParticipantReply::Direct(reply))?;
    Ok(if applied {
        Transition::Commit(encoded)
    } else {
        Transition::Reject(encoded)
    })
}
fn rejected(tx: &mut GroupTransaction<'_>, fault: Fault) -> Result<Transition, ProtocolError> {
    reply(tx, DirectReply::Rejected(fault))
}

/// Structural checks that need no state. They reject before the ordered
/// state checks, so a malformed command never reads a head. A command that is
/// well formed but over a size limit answers `ResourceLimit`, so the
/// executor falls back to the queued path instead of failing (RV-T4 F9).
fn well_formed(
    group: &GroupIdentity,
    command: &DirectCommit,
) -> Result<Option<Fault>, ProtocolError> {
    let malformed = Ok(Some(Fault::InvalidBinding));
    let GroupKind::Data { table_id, .. } = &group.kind else {
        return malformed;
    };
    if command.table_id != *table_id
        || command.request_id.is_nil()
        || command.authority.validate().is_err()
        || command.schema_epoch == 0
        || command.routing_epoch == 0
        || command.applied_unix_ms == 0
        || command.writes.is_empty()
    {
        return malformed;
    }
    let mut keys = Vec::new();
    for (offset, write) in command.writes.iter().enumerate() {
        let MvccKey::Row {
            table_id: row_table,
            primary_key,
        } = &write.key
        else {
            return malformed;
        };
        if row_table != table_id
            || (offset != 0
                && compare_keys(&command.writes[offset - 1].key, &write.key) != Ordering::Less)
        {
            return malformed;
        }
        for change in &write.index {
            let MvccKey::IndexEntry {
                table_id: index_table,
                primary_key: index_primary,
                ..
            } = &change.key
            else {
                return malformed;
            };
            if index_table != table_id || index_primary != primary_key {
                return malformed;
            }
            keys.push(&change.key);
        }
    }
    keys.sort_by(|a, b| compare_keys(a, b));
    if keys
        .windows(2)
        .any(|pair| compare_keys(pair[0], pair[1]) == Ordering::Equal)
    {
        return malformed;
    }
    for (key, _, value) in command.changes() {
        key_length(key)?;
        if let Some(value) = value {
            serde_json::from_str::<&RawValue>(value.as_str()).map_err(|_| invalid())?;
        }
    }
    if command.writes.len() > MAX_DIRECT_WRITES
        || command
            .writes
            .iter()
            .any(|write| write.index.len() > MAX_DIRECT_INDEX_CHANGES)
        || command.result.as_str().len() > MAX_DIRECT_RESULT_BYTES
    {
        return Ok(Some(Fault::ResourceLimit));
    }
    Ok(None)
}

/// A split holds the range: the barrier, a frozen source, or a child that
/// has not imported everything.
fn split_in_progress(config: &Config) -> bool {
    config.direct_pause.is_some()
        || config.split_source.is_some()
        || config.split_child.is_some_and(|child| !child.complete)
}

/// The reply an existing dedup value gives a request with `digest`.
fn settled(stored: &Stored<'_>, digest: Digest) -> DirectReply {
    if stored.digest() != digest {
        return DirectReply::Rejected(Fault::Conflict);
    }
    match stored {
        Stored::Committed(record) => DirectReply::Replayed {
            result: DirectResult(record.result.to_owned()),
        },
        Stored::Forwarded(_) => DirectReply::Forwarded,
    }
}
fn lookup(
    tx: &mut GroupTransaction<'_>,
    dedup: &[u8],
    digest: Digest,
) -> Result<Option<DirectReply>, ProtocolError> {
    let Some(raw) = tx.pinned(dedup)? else {
        return Ok(None);
    };
    let stored = Stored::decode(&raw).ok_or_else(recovery_unknown)?;
    Ok(Some(settled(&stored, digest)))
}
/// The stamp for a new record or marker: never below the group high-water.
/// Raises the high-water when the leader's clock is ahead of it.
fn stamp(tx: &mut GroupTransaction<'_>, applied_unix_ms: u64) -> Result<u64, ProtocolError> {
    let high = load::<u64>(tx, UNIX_HIGH)?.unwrap_or(0);
    if applied_unix_ms > high {
        tx.put_serialized(UNIX_HIGH, &applied_unix_ms)?;
    }
    Ok(applied_unix_ms.max(high))
}

/// The native batch bytes this commit writes, before expiry deletions.
fn batch_estimate(command: &DirectCommit, dedup: usize) -> Result<usize, ProtocolError> {
    let mut bytes = 16usize << 10;
    for (key, _, value) in command.changes() {
        let key = key_length(key)?;
        bytes = key
            .checked_add(9)
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(value.map_or(0, |v| v.as_str().len())))
            .and_then(|n| n.checked_add(2 * OP_OVERHEAD + 64))
            .and_then(|n| bytes.checked_add(n))
            .ok_or_else(invalid)?;
    }
    dedup
        .checked_add(9)
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| n.checked_add(command.result.as_str().len()))
        .and_then(|n| n.checked_add(2 * OP_OVERHEAD + 512))
        .and_then(|n| bytes.checked_add(n))
        .ok_or_else(invalid)
}

/// What the ordered checks decided for one commit.
enum Checked {
    /// Every check passed: write it under this dedup key.
    Write { dedup: Vec<u8>, estimate: usize },
    /// The commit ends here and writes nothing.
    Done(DirectReply),
}

/// The group high-waters one entry raises (RV-T4 F7, the split freeze
/// bound). A group transaction does not read its own writes, so the members
/// of a batch carry them in memory and the entry writes each once.
struct Highs {
    unix: u64,
    unix_raised: bool,
    direct: Option<CommitTs>,
    direct_raised: bool,
}
impl Highs {
    fn load(tx: &mut GroupTransaction<'_>) -> Result<Self, ProtocolError> {
        Ok(Self {
            unix: load::<u64>(tx, UNIX_HIGH)?.unwrap_or(0),
            unix_raised: false,
            direct: load::<CommitTs>(tx, DIRECT_HIGH)?,
            direct_raised: false,
        })
    }
    /// `stamp` for a member: never below the high-water, raising it.
    fn stamp(&mut self, applied_unix_ms: u64) -> u64 {
        if applied_unix_ms > self.unix {
            self.unix = applied_unix_ms;
            self.unix_raised = true;
        }
        self.unix
    }
    fn commit(&mut self, commit_ts: CommitTs) {
        if self.direct.is_none_or(|high| high < commit_ts) {
            self.direct = Some(commit_ts);
            self.direct_raised = true;
        }
    }
    fn flush(self, tx: &mut GroupTransaction<'_>) -> Result<(), ProtocolError> {
        if self.unix_raised {
            tx.put_serialized(UNIX_HIGH, &self.unix)?;
        }
        if let Some(direct) = self.direct.filter(|_| self.direct_raised) {
            tx.put_serialized(DIRECT_HIGH, &direct)?;
        }
        Ok(())
    }
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectCommit,
) -> Result<Transition, ProtocolError> {
    match check(tx, group, config, command, 0)? {
        Checked::Done(reply) => self::reply(tx, reply),
        Checked::Write { dedup, .. } => {
            let mut highs = Highs::load(tx)?;
            write(tx, command, &dedup, &mut highs)?;
            highs.flush(tx)?;
            reply(
                tx,
                DirectReply::Applied {
                    commit_ts: command.commit_ts,
                },
            )
        }
    }
}

/// GC2: a `DirectBatch`. Each member runs the same ordered checks as a
/// one-entry commit; the only state a member leaves for the next is the
/// carried high-waters (members never share a key), and the native batch
/// budget is shared.
pub(super) fn apply_batch(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    batch: &DirectBatch,
) -> Result<Transition, ProtocolError> {
    if batch.members.is_empty() || batch.members.len() > MAX_DIRECT_BATCH_MEMBERS {
        return rejected(tx, Fault::InvalidBinding);
    }
    let mut replies = Vec::with_capacity(batch.members.len());
    let mut seen = std::collections::BTreeSet::new();
    let mut highs = None;
    let mut used = 0usize;
    for member in &batch.members {
        // Defensive: the leader's latches keep members off each other's
        // keys. A member that shares one with an earlier member would be
        // validated against a head that member is about to change.
        let mut shared = false;
        let mut keys = Vec::with_capacity(member.writes.len());
        for (key, _, _) in member.changes() {
            let key = physical(0, key)?;
            shared |= seen.contains(&key);
            keys.push(key);
        }
        if shared {
            replies.push(DirectMemberReply::Rejected(Fault::Conflict));
            continue;
        }
        seen.extend(keys);
        let reply = match check(tx, group, config, member, used)? {
            Checked::Done(reply) => member_reply(reply),
            Checked::Write { dedup, estimate } => {
                if highs.is_none() {
                    highs = Some(Highs::load(tx)?);
                }
                let Some(highs) = highs.as_mut() else {
                    unreachable!("loaded above");
                };
                write(tx, member, &dedup, highs)?;
                used = used.checked_add(estimate).ok_or_else(invalid)?;
                DirectMemberReply::Applied {
                    commit_ts: member.commit_ts,
                }
            }
        };
        replies.push(reply);
    }
    if let Some(highs) = highs {
        highs.flush(tx)?;
    }
    reply(tx, DirectReply::Batch(replies))
}

fn member_reply(reply: DirectReply) -> DirectMemberReply {
    match reply {
        DirectReply::Applied { commit_ts } => DirectMemberReply::Applied { commit_ts },
        DirectReply::Replayed { .. } => DirectMemberReply::Replayed,
        DirectReply::Forwarded => DirectMemberReply::Forwarded,
        DirectReply::Rejected(fault) => DirectMemberReply::Rejected(fault),
        DirectReply::Batch(_) => DirectMemberReply::Rejected(Fault::InvalidBinding),
    }
}

/// Steps 0 to 8 of plan section 4 for one commit. Nothing here writes.
/// `used` is the native batch budget earlier members of the entry took.
fn check(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectCommit,
    used: usize,
) -> Result<Checked, ProtocolError> {
    let done = |fault| Ok(Checked::Done(DirectReply::Rejected(fault)));
    let Some(config) = config else {
        return done(Fault::NotInitialized);
    };
    // 0. `data.direct.v1` reached this group (the store-side gate).
    let Some(schema) = config.direct_schema else {
        return done(Fault::DirectInactive);
    };
    if let Some(fault) = well_formed(group, command)? {
        return done(fault);
    }
    let dedup_key = command.dedup_key().ok_or_else(invalid)?;
    let dedup = physical(DEDUP, &dedup_key)?;
    // 1. No split holds the range. An exact retry still replays: the
    //    record is immutable and the reply writes nothing.
    if split_in_progress(config) {
        let reply = lookup(tx, &dedup, command.digest)?
            .unwrap_or(DirectReply::Rejected(Fault::SplitInProgress));
        return Ok(Checked::Done(reply));
    }
    // 2. The run and group fence.
    if config.authority != command.authority {
        return done(Fault::StaleAuthority);
    }
    // 3. Both epochs. The schema epoch is the group's direct epoch, which
    //    a DDL fence bumps before the Catalog publishes (plan section 7); a
    //    dropped table takes no direct commit.
    if schema.lifecycle.is_none()
        || schema.epoch != command.schema_epoch
        || config.routing_epoch != command.routing_epoch
    {
        return done(Fault::StaleEpoch);
    }
    // 4. Every key belongs to the group.
    if command
        .changes()
        .any(|(key, _, _)| !owns(group, &config.span, key))
    {
        return done(Fault::InvalidBinding);
    }
    // 5. A committed request with this id and anchor replays or conflicts,
    //    and a forwarded one belongs to the queued path. This precedes the
    //    head checks, which the first attempt changed.
    if let Some(reply) = lookup(tx, &dedup, command.digest)? {
        return Ok(Checked::Done(reply));
    }
    let estimate = batch_estimate(command, dedup.len())?;
    if used.saturating_add(estimate) > APPLY.batch_bytes - (64 << 10) {
        return done(Fault::ResourceLimit);
    }
    // 6-8. No intent, the exact head version, then a timestamp above the
    //      newest version, the GC floor and a split child's import cut.
    let mut heads = Vec::with_capacity(command.writes.len());
    for (key, expected, _) in command.changes() {
        let length = key_length(key)?;
        let head = tx.with_scratch_buffer(length + 1, |tx, buffer| {
            buffer[0] = HEAD;
            encode_key(key, &mut buffer[1..]);
            load::<Head>(tx, buffer)
        })?;
        heads.push((head.unwrap_or_default(), expected));
    }
    if heads.iter().any(|(head, _)| head.intent.is_some()) {
        return done(Fault::Incomplete);
    }
    if heads
        .iter()
        .any(|(head, expected)| head.version != *expected)
    {
        return done(Fault::Conflict);
    }
    if heads.iter().any(|(head, _)| {
        head.version
            .is_some_and(|newest| command.commit_ts <= newest)
    }) {
        return done(Fault::Conflict);
    }
    drop(heads);
    // A direct commit never lands at or below the group's GC floor (or a
    // split child's import cut): no reader below it is served here.
    let floor = config.split_child.map_or(config.gc_floor, |child| {
        config
            .gc_floor
            .max(child.read_ts.get())
            .max(child.published_at.map_or(0, CommitTs::get))
    });
    if command.commit_ts.get() <= floor {
        return done(Fault::Conflict);
    }
    Ok(Checked::Write { dedup, estimate })
}

/// The writes of a checked commit: its versions, heads, index entries and
/// dedup record. The high-waters are flushed by the caller.
fn write(
    tx: &mut GroupTransaction<'_>,
    command: &DirectCommit,
    dedup: &[u8],
    highs: &mut Highs,
) -> Result<(), ProtocolError> {
    #[derive(Serialize)]
    struct Version<'a> {
        value: Option<&'a RawValue>,
    }
    for (key, _, value) in command.changes() {
        let length = key_length(key)?;
        let value = value
            .map(|v| serde_json::from_str::<&RawValue>(v.as_str()).map_err(|_| invalid()))
            .transpose()?;
        tx.with_scratch_buffer(length + 9, |tx, buffer| {
            encode_key(key, &mut buffer[1..length + 1]);
            buffer[0] = VALUE;
            buffer[length + 1..].copy_from_slice(&(!command.commit_ts.get()).to_be_bytes());
            tx.put_serialized(buffer, &Version { value })?;
            buffer[0] = HEAD;
            tx.put_serialized(
                &buffer[..length + 1],
                &Head {
                    version: Some(command.commit_ts),
                    intent: None,
                },
            )
        })?;
    }
    let result =
        serde_json::from_str::<&RawValue>(command.result.as_str()).map_err(|_| invalid())?;
    let applied_unix_ms = highs.stamp(command.applied_unix_ms);
    put_entry(
        tx,
        dedup,
        applied_unix_ms,
        &DedupRecord {
            applied_unix_ms,
            commit_ts: command.commit_ts,
            digest: command.digest,
            result,
        },
    )?;
    highs.commit(command.commit_ts);
    Ok(())
}

/// `tag || logical key`.
fn physical(tag: u8, key: &MvccKey) -> Result<Vec<u8>, ProtocolError> {
    let length = key_length(key)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length + 1).map_err(|_| invalid())?;
    bytes.resize(length + 1, 0);
    bytes[0] = tag;
    encode_key(key, &mut bytes[1..]);
    Ok(bytes)
}
fn expiry_key(applied_unix_ms: u64, dedup: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(dedup.len() + 8)
        .map_err(|_| invalid())?;
    bytes.push(EXPIRY);
    bytes.extend_from_slice(&applied_unix_ms.to_be_bytes());
    bytes.extend_from_slice(&dedup[1..]);
    Ok(bytes)
}
fn put_entry(
    tx: &mut GroupTransaction<'_>,
    dedup: &[u8],
    applied_unix_ms: u64,
    value: &impl Serialize,
) -> Result<(), ProtocolError> {
    tx.put_serialized(dedup, value)?;
    tx.put(&expiry_key(applied_unix_ms, dedup)?, &[])
}

pub(super) fn forward(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectForward,
) -> Result<Transition, ProtocolError> {
    let Some(config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if config.direct_schema.is_none() {
        return rejected(tx, Fault::DirectInactive);
    }
    let anchor = MvccKey::Row {
        table_id: command.table_id,
        primary_key: command.anchor.clone(),
    };
    if !matches!(&group.kind, GroupKind::Data { table_id, .. } if *table_id == command.table_id)
        || command.request_id.is_nil()
        || command.authority.validate().is_err()
        || command.applied_unix_ms == 0
        || key_length(&anchor).is_err()
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    let dedup = physical(
        DEDUP,
        &MvccKey::DirectDedup {
            table_id: command.table_id,
            anchor: command.anchor.clone(),
            request_id: command.request_id,
        },
    )?;
    // A frozen source or an unimported child cannot take a marker: a split
    // copy would miss it. The split barrier alone does not block markers,
    // because the freeze that follows copies the state after them.
    if config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete) {
        let reply = lookup(tx, &dedup, command.digest)?
            .unwrap_or(DirectReply::Rejected(Fault::SplitInProgress));
        return self::reply(tx, reply);
    }
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    if !owns(group, &config.span, &anchor) {
        return rejected(tx, Fault::InvalidBinding);
    }
    if let Some(reply) = lookup(tx, &dedup, command.digest)? {
        return self::reply(tx, reply);
    }
    let applied_unix_ms = stamp(tx, command.applied_unix_ms)?;
    put_entry(
        tx,
        &dedup,
        applied_unix_ms,
        &ForwardMarker {
            applied_unix_ms,
            digest: command.digest,
            forwarded: true,
        },
    )?;
    reply(tx, DirectReply::Forwarded)
}

pub(super) fn pause(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    command: &DirectPause,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if config.direct_schema.is_none() {
        return reject(tx, Fault::DirectInactive);
    }
    if !matches!(group.kind, GroupKind::Data { .. }) || command.split_id.is_nil() {
        return reject(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    let done = ParticipantReply::DirectPaused {
        split_id: command.split_id,
        paused: command.paused,
    };
    let frozen_by = config
        .split_source
        .as_ref()
        .map(|source| source.statement.split_id);
    if command.paused {
        // The freeze of the same split already stops direct commits.
        if frozen_by == Some(command.split_id) || config.direct_pause == Some(command.split_id) {
            return commit(tx, &done);
        }
        if frozen_by.is_some() || config.direct_pause.is_some() {
            return reject(tx, Fault::Conflict);
        }
        if config.split_child.is_some_and(|child| !child.complete) {
            return reject(tx, Fault::Incomplete);
        }
        config.direct_pause = Some(command.split_id);
    } else {
        match config.direct_pause {
            None => return commit(tx, &done),
            Some(holder) if holder != command.split_id => return reject(tx, Fault::Conflict),
            Some(_) => config.direct_pause = None,
        }
    }
    tx.put_serialized(CONFIG, &config)?;
    commit(tx, &done)
}

pub(super) fn child_published(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    command: &ChildPublished,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if !matches!(group.kind, GroupKind::Data { .. }) {
        return reject(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    let Some(child) = config.split_child.as_mut() else {
        return reject(tx, Fault::InvalidBinding);
    };
    if !child.complete {
        return reject(tx, Fault::Incomplete);
    }
    if command.published_at <= child.read_ts {
        return reject(tx, Fault::InvalidBinding);
    }
    let done = ParticipantReply::ChildPublished {
        published_at: command.published_at,
    };
    match child.published_at {
        Some(existing) if existing == command.published_at => return commit(tx, &done),
        Some(_) => return reject(tx, Fault::Conflict),
        None => child.published_at = Some(command.published_at),
    }
    tx.put_serialized(CONFIG, &config)?;
    commit(tx, &done)
}

/// `direct_dedup` behind the caller's linearizable barrier: the committed
/// result or forward marker for `request_id` at `anchor`, if any.
pub(super) fn read_dedup(
    view: &mut crate::oltp::pool::GroupRead<'_>,
    group: &GroupIdentity,
    table_id: Uuid,
    anchor: Key,
    request_id: Uuid,
) -> Result<ChargedBytes, ProtocolError> {
    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
    if raw.len() > METADATA_BYTES {
        return Err(recovery_unknown());
    }
    let config: Config = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
    drop(raw);
    let found = dedup_in(view, group, &config, table_id, anchor, request_id)?;
    view.encode(&found)
}

/// `read_dedup` against an already-read `config`, without encoding: the
/// leader's combined snapshot read (PERF2b) shares one config read.
pub(super) fn dedup_in(
    view: &mut crate::oltp::pool::GroupRead<'_>,
    group: &GroupIdentity,
    config: &Config,
    table_id: Uuid,
    anchor: Key,
    request_id: Uuid,
) -> Result<Option<DedupLookup>, ProtocolError> {
    let row = MvccKey::Row {
        table_id,
        primary_key: anchor.clone(),
    };
    if request_id.is_nil()
        || !matches!(&group.kind, GroupKind::Data { table_id: own, .. } if *own == table_id)
        || !owns(group, &config.span, &row)
    {
        return Err(invalid());
    }
    if config.split_child.is_some_and(|child| !child.complete) {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "split child has not imported its dedup records yet",
        ));
    }
    if config
        .split_source
        .as_ref()
        .is_some_and(|source| source.published_at.is_some())
    {
        return Err(failure(
            ErrorCode::StorageUnavailable,
            "split routing moved this anchor to a child; refresh the route",
        ));
    }
    let dedup = physical(
        DEDUP,
        &MvccKey::DirectDedup {
            table_id,
            anchor,
            request_id,
        },
    )?;
    let found = match view.pinned(&dedup)? {
        None => None,
        Some(raw) => Some(match Stored::decode(&raw).ok_or_else(recovery_unknown)? {
            Stored::Committed(record) => DedupLookup::Committed {
                digest: record.digest,
                commit_ts: record.commit_ts,
                result: DirectResult(record.result.to_owned()),
            },
            Stored::Forwarded(marker) => DedupLookup::Forwarded {
                digest: marker.digest,
            },
        }),
    };
    Ok(found)
}

pub(super) fn expire(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<&Config>,
    command: &DirectExpire,
) -> Result<Transition, ProtocolError> {
    let Some(config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if config.direct_schema.is_none() {
        return reject(tx, Fault::DirectInactive);
    }
    if !matches!(group.kind, GroupKind::Data { .. })
        || command.max_items == 0
        || command.max_items > MAX_DIRECT_EXPIRE_ITEMS
        || command.cutoff_unix_ms == 0
        || command.retention_ms == 0
    {
        return reject(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    // A split copies dedup records page by page; keep them stable meanwhile.
    if config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete) {
        return reject(tx, Fault::Incomplete);
    }
    // A leader clock ahead of the group's own stamps cannot cut into the
    // retention window of records this group already holds.
    let high = load::<u64>(tx, UNIX_HIGH)?.unwrap_or(0);
    let cutoff = command
        .cutoff_unix_ms
        .min(high.saturating_sub(command.retention_ms));
    let (deleted, complete) = sweep(
        tx,
        cutoff,
        command.max_items,
        APPLY.batch_bytes - (64 << 10),
    )?;
    commit(tx, &ParticipantReply::DirectExpired { deleted, complete })
}

/// Delete expired records, oldest first, within `max_items` and a byte
/// allowance so the batch stays inside its budget. Deterministic for a given
/// state. Returns the count and whether no expired record remains.
fn sweep(
    tx: &mut GroupTransaction<'_>,
    cutoff_unix_ms: u64,
    max_items: u16,
    mut allowance: usize,
) -> Result<(u64, bool), ProtocolError> {
    let mut cursor = vec![EXPIRY];
    let mut deleted = 0u64;
    loop {
        let Some(record) = tx.first_from(&[EXPIRY], &cursor)? else {
            return Ok((deleted, true));
        };
        let key = record.key();
        let stamp = key
            .get(1..9)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(recovery_unknown)?;
        if key.len() <= 9 {
            return Err(recovery_unknown());
        }
        if stamp >= cutoff_unix_ms {
            return Ok((deleted, true));
        }
        let cost = key
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(2 * OP_OVERHEAD))
            .ok_or_else(invalid)?;
        if deleted == u64::from(max_items) || cost > allowance {
            return Ok((deleted, false));
        }
        allowance -= cost;
        cursor.clear();
        cursor
            .try_reserve_exact(key.len() + 1)
            .map_err(|_| invalid())?;
        cursor.extend_from_slice(key);
        drop(record);
        let mut dedup = Vec::new();
        dedup
            .try_reserve_exact(cursor.len() - 8)
            .map_err(|_| invalid())?;
        dedup.push(DEDUP);
        dedup.extend_from_slice(&cursor[9..]);
        if tx.pinned(&dedup)?.is_none() {
            return Err(recovery_unknown());
        }
        tx.delete(&cursor)?;
        tx.delete(&dedup)?;
        deleted += 1;
        cursor.push(0);
    }
}

/// Install a dedup image carried by a split page or a restore. The record's
/// own commit timestamp must match the image.
pub(super) fn import_image(
    tx: &mut GroupTransaction<'_>,
    image: &super::image_archive::CommittedImage,
) -> Result<(), ProtocolError> {
    if !matches!(image.key, MvccKey::DirectDedup { .. }) {
        return Err(invalid());
    }
    let value = image.value.as_ref().ok_or_else(invalid)?;
    let stored = Stored::decode(value.as_str().as_bytes()).ok_or_else(invalid)?;
    // A marker has no commit; its image carries the traversal's cut.
    if let Stored::Committed(record) = &stored
        && record.commit_ts != image.commit_ts
    {
        return Err(invalid());
    }
    let dedup = physical(DEDUP, &image.key)?;
    if let Some(existing) = tx.pinned(&dedup)? {
        return if existing.as_ref() == value.as_str().as_bytes() {
            Ok(())
        } else {
            Err(recovery_unknown())
        };
    }
    // Imported stamps raise the high-water, so expiry on the child or the
    // restored group is clamped as it was on the source.
    let applied_unix_ms = stored.applied_unix_ms();
    if load::<u64>(tx, UNIX_HIGH)?.is_none_or(|high| high < applied_unix_ms) {
        tx.put_serialized(UNIX_HIGH, &applied_unix_ms)?;
    }
    match &stored {
        Stored::Committed(record) => put_entry(tx, &dedup, applied_unix_ms, record),
        Stored::Forwarded(marker) => put_entry(tx, &dedup, applied_unix_ms, marker),
    }
}

/// A dedup image for the committed-image traversal, or `None` when its
/// commit is above the cut. A forward marker is not versioned: it travels
/// whenever it exists, stamped with the cut, so a request the queued path
/// may have accepted can never run directly on a split child or a restored
/// group.
pub(super) fn image_from_record(
    key: MvccKey,
    value: &[u8],
    read_ts: ReadTs,
    owner: Arc<ScratchLease>,
) -> Result<Option<super::image_archive::CommittedImage>, ProtocolError> {
    let commit_ts = match Stored::decode(value).ok_or_else(recovery_unknown)? {
        Stored::Committed(record) if record.commit_ts > read_ts => return Ok(None),
        Stored::Committed(record) => record.commit_ts,
        Stored::Forwarded(_) => read_ts,
    };
    Ok(Some(super::image_archive::CommittedImage {
        key,
        commit_ts,
        value: Some(CanonicalJson::from_stored_json(value, owner)?),
    }))
}

/// Activates direct commits on the group or fences them for a DDL (plan
/// section 7). Not gated by the group's own state: it is the activation.
/// Control gates it, like every direct-family command.
pub(super) fn schema(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    command: &DirectSchemaFence,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if !matches!(&group.kind, GroupKind::Data { table_id, .. } if *table_id == command.table_id)
        || command.authority.validate().is_err()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    let next = match (config.direct_schema, command.change) {
        (Some(current), DirectSchemaChange::Activate { .. }) => current,
        (None, DirectSchemaChange::Activate { lifecycle }) => DirectSchema {
            epoch: config.schema_epoch,
            lifecycle: Some(lifecycle),
        },
        (Some(current), DirectSchemaChange::Fence { lifecycle })
            if current.lifecycle == lifecycle =>
        {
            current
        }
        (current, DirectSchemaChange::Fence { lifecycle }) => {
            let base = current.map_or(config.schema_epoch, |current| current.epoch);
            let Some(epoch) = base.checked_add(1) else {
                return reject(tx, Fault::ResourceLimit);
            };
            DirectSchema { epoch, lifecycle }
        }
    };
    if config.direct_schema != Some(next) {
        config.direct_schema = Some(next);
        tx.put_serialized(CONFIG, &config)?;
    }
    commit(tx, &ParticipantReply::DirectSchema(next))
}

/// The highest direct commit timestamp applied on this group.
pub(super) fn high(tx: &mut GroupTransaction<'_>) -> Result<Option<CommitTs>, ProtocolError> {
    load::<CommitTs>(tx, DIRECT_HIGH)
}

impl DirectForward {
    pub(crate) fn heap_bytes(&self) -> usize {
        match &self.anchor {
            Key::Utf8(text) => text.capacity(),
            _ => 0,
        }
    }
}

impl crate::oltp_replication::owned::OwnedPayload for DirectCommit {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let mut bytes = self
            .writes
            .capacity()
            .checked_mul(std::mem::size_of::<DirectWrite>())
            .and_then(|n| n.checked_add(self.result.heap_bytes().ok()?))
            .ok_or_else(invalid)?;
        for write in &self.writes {
            bytes = write
                .index
                .capacity()
                .checked_mul(std::mem::size_of::<IndexChange>())
                .and_then(|n| bytes.checked_add(n))
                .ok_or_else(invalid)?;
        }
        for (key, _, value) in self.changes() {
            bytes = bytes
                .checked_add(key.heap_bytes())
                .and_then(|n| n.checked_add(value.map_or(Ok(0), |v| v.heap_bytes()).ok()?))
                .ok_or_else(invalid)?;
        }
        Ok(bytes)
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.result.attach_charge(owner.clone())?;
        for (_, _, value) in self.changes() {
            if let Some(value) = value {
                value.attach_charge(owner.clone())?;
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        self.result.is_charged()
            && self
                .changes()
                .all(|(_, _, value)| value.is_none_or(CanonicalJson::is_charged))
    }
}

impl DirectBatch {
    pub(crate) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        use crate::oltp_replication::owned::OwnedPayload as _;
        let mut bytes = self
            .members
            .capacity()
            .checked_mul(std::mem::size_of::<DirectCommit>())
            .ok_or_else(invalid)?;
        for member in &self.members {
            bytes = bytes
                .checked_add(member.heap_bytes()?)
                .ok_or_else(invalid)?;
        }
        Ok(bytes)
    }
    pub(crate) fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        use crate::oltp_replication::owned::OwnedPayload as _;
        for member in &self.members {
            member.attach_fields(owner.clone())?;
        }
        Ok(())
    }
    pub(crate) fn fields_attached(&self) -> bool {
        use crate::oltp_replication::owned::OwnedPayload as _;
        self.members.iter().all(DirectCommit::fields_attached)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) fn acceptance_indexed_direct_replay() {
    tests::o11_crash_replay_of_a_direct_commit_entry_is_idempotent();
}
