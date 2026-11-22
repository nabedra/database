//! A single-owner cache slot. Preparation never publishes data or evicts a key.
use std::{
    collections::{HashSet, hash_map::RandomState},
    mem::size_of,
    time::{Duration, Instant},
};

use bytes::Bytes;
use hashbrown::HashMap;
use smallvec::SmallVec;

mod records;

use records::{Layout, Records};

pub const MAX_KEY_BYTES: usize = 512;
pub const MAX_VALUE_BYTES: usize = 1 << 20;
const EVICTION_SAMPLES: usize = 64;
const EVICTION_VICTIMS: usize = 32;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvictionPolicy {
    #[default]
    SampledLru,
    NoEviction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetWhen {
    Always,
    Absent,
    Present,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operation {
    Get(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        when: SetWhen,
    },
    Delete(Vec<Bytes>),
    Exists(Vec<Bytes>),
    GetMany(Vec<Bytes>),
    Ttl {
        key: Bytes,
        milliseconds: bool,
    },
    Expire {
        key: Bytes,
        ttl_ms: i64,
    },
    Persist(Bytes),
    Increment {
        key: Bytes,
        delta: i64,
    },
}

impl Operation {
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &Bytes> {
        match self {
            Self::Get(key)
            | Self::Set { key, .. }
            | Self::Ttl { key, .. }
            | Self::Expire { key, .. }
            | Self::Persist(key)
            | Self::Increment { key, .. } => std::slice::from_ref(key),
            Self::Delete(keys) | Self::Exists(keys) | Self::GetMany(keys) => keys.as_slice(),
        }
        .iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Ok,
    Bulk(Option<Bytes>),
    Integer(i64),
    Array(Vec<Option<Bytes>>),
    Error(StoreError),
}

impl Reply {
    pub fn wire_bytes(&self) -> usize {
        match self {
            Self::Ok => 5,
            Self::Bulk(value) => bulk_bytes(value.as_ref()),
            Self::Integer(value) => {
                3 + decimal_digits(value.unsigned_abs()) + usize::from(*value < 0)
            }
            Self::Array(values) => values
                .iter()
                .fold(3 + decimal_digits(values.len() as u64), |size, value| {
                    size.saturating_add(bulk_bytes(value.as_ref()))
                }),
            Self::Error(error) => 3 + error.resp().len(),
        }
    }
}

fn decimal_digits(value: u64) -> usize {
    value.checked_ilog10().unwrap_or(0) as usize + 1
}

fn bulk_bytes(value: Option<&Bytes>) -> usize {
    value.map_or(5, |value| {
        value.len() + 5 + decimal_digits(value.len() as u64)
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreError {
    OutOfMemory,
    ReplyLimit,
    InvalidInteger,
    IntegerOverflow,
    InvalidTtl,
    InvalidKey,
    InvalidValue,
    InvalidEffects,
    SnapshotLimit,
    StaleTurn,
}

impl StoreError {
    pub const fn resp(self) -> &'static str {
        match self {
            Self::OutOfMemory => "OOM command not allowed under the cache memory limit",
            Self::ReplyLimit => "ERR response exceeds cache output limit",
            Self::InvalidInteger => "ERR value is not an integer or out of range",
            Self::IntegerOverflow => "ERR increment or decrement would overflow",
            Self::InvalidTtl => "ERR invalid expire time",
            Self::InvalidKey => "ERR cache key exceeds 512 bytes",
            Self::InvalidValue => "ERR cache value exceeds 1 MiB",
            Self::InvalidEffects => "ERR invalid cache replication effects",
            Self::SnapshotLimit => "TRYAGAIN snapshot pin exceeds its resource budget",
            Self::StaleTurn => "TRYAGAIN cache turn expired before publication",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    Upsert {
        key: Bytes,
        value: Bytes,
        deadline_ms: Option<u64>,
    },
    Delete(Bytes),
}

impl Effect {
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Upsert { key, .. } | Self::Delete(key) => key,
        }
    }

    /// Charge the canonical body and worst-case backing allocations, including
    /// bytes that may remain pinned after the live slot drops its reference.
    pub fn charged_bytes(&self) -> usize {
        const ALLOCATION_OVERHEAD: usize = 64;
        size_of::<Self>()
            .saturating_add(ALLOCATION_OVERHEAD)
            .saturating_add(self.key().len())
            .saturating_add(match self {
                Self::Upsert { value, .. } => value.len(),
                Self::Delete(_) => 0,
            })
    }
}

pub type Effects = SmallVec<[Effect; 1]>;

#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    pub growth_bytes: usize,
    pub reply_bytes: usize,
}

impl Default for TurnLimits {
    fn default() -> Self {
        Self {
            growth_bytes: usize::MAX,
            reply_bytes: usize::MAX,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct RemovalCounts {
    pub expired: u64,
    pub evicted: u64,
}

/// Owned result and effects of a command under one exclusive actor turn.
/// Dropping a preparation is cancellation.
pub struct Prepared {
    reply: Reply,
    effects: Effects,
    touches: SmallVec<[usize; 1]>,
    revision: u64,
    now_ms: u64,
    valid_until_ms: Option<u64>,
    relative_ttl_ms: Option<u64>,
    ttl_reply: Option<(u64, bool)>,
    layout: Layout,
    removals: RemovalCounts,
}

impl Prepared {
    pub fn removals(&self) -> RemovalCounts {
        self.removals
    }

    pub fn reply(&self) -> &Reply {
        &self.reply
    }

    pub fn reply_bytes(&self) -> usize {
        self.reply.wire_bytes()
    }

    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    pub fn effect_bytes(&self) -> usize {
        self.effects.iter().map(Effect::charged_bytes).sum()
    }

    pub fn charge_after(&self) -> usize {
        self.layout.charge_after()
    }

    pub fn reservation_bytes(&self) -> usize {
        self.layout.reservation_bytes()
    }

    /// Recheck the staged observations against the final clock sample. A live
    /// key that expires during the turn requires a new preparation. Relative
    /// TTLs are based on this same sample, taken at the publication boundary.
    ///
    /// The caller must also check the authority grant against this sample
    /// before publishing. A successful refresh never increases a reply's size.
    pub fn refresh_time(&mut self, now_ms: u64) -> Result<(), StoreError> {
        if now_ms < self.now_ms || self.valid_until_ms.is_some_and(|until| now_ms >= until) {
            return Err(StoreError::StaleTurn);
        }
        if let Some(ttl) = self.relative_ttl_ms {
            let deadline = absolute_deadline(now_ms, ttl)?;
            if let Some(Effect::Upsert { deadline_ms, .. }) = self.effects.last_mut() {
                *deadline_ms = Some(deadline);
            }
        }
        if let Some((deadline, milliseconds)) = self.ttl_reply {
            self.reply = Reply::Integer(ttl_reply(deadline, now_ms, milliseconds));
        }
        self.now_ms = now_ms;
        Ok(())
    }
}

pub struct SlotStore {
    records: Records,
    byte_limit: usize,
    eviction: EvictionPolicy,
    revision: u64,
    access: u64,
}

impl SlotStore {
    pub fn new(byte_limit: usize, eviction: EvictionPolicy) -> Self {
        Self {
            records: Records::default(),
            byte_limit,
            eviction,
            revision: 0,
            access: 0,
        }
    }

    pub fn charged_bytes(&self) -> usize {
        self.records.charged_bytes()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An order-independent digest of the live keyspace at `now_ms`, in the
    /// style of Valkey's `DEBUG DIGEST`: the XOR of one SHA-256 per record over
    /// its length-prefixed key and value and its absolute deadline, truncated
    /// to 20 bytes. An empty keyspace digests to zero. Keys that are logically
    /// expired but not yet removed are left out, so two copies compare equal
    /// whatever their sweep progress.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn digest(&self, now_ms: u64) -> [u8; 20] {
        use sha2::{Digest as _, Sha256};
        let mut digest = [0u8; 20];
        for record in self
            .records
            .iter()
            .filter(|record| !record.is_expired(now_ms))
        {
            let mut hash = Sha256::new();
            hash.update((record.key().len() as u64).to_le_bytes());
            hash.update(record.key());
            hash.update((record.value.len() as u64).to_le_bytes());
            hash.update(&record.value);
            match record.deadline_ms {
                None => hash.update([0]),
                Some(deadline) => {
                    hash.update([1]);
                    hash.update(deadline.to_le_bytes());
                }
            }
            for (out, byte) in digest.iter_mut().zip(hash.finalize().iter()) {
                *out ^= byte;
            }
        }
        digest
    }

    pub fn prepare(&self, op: &Operation, now_ms: u64, limits: TurnLimits) -> Prepared {
        let mut turn = self.turn(now_ms);
        let result = self
            .prepare_operation(op, limits, &mut turn)
            .and_then(|()| {
                if turn.reply_bytes() > limits.reply_bytes {
                    Err(StoreError::ReplyLimit)
                } else {
                    self.fit_layout(&mut turn, limits.growth_bytes)
                }
            });
        if let Err(error) = result {
            let mut rejected = self.failure(now_ms, error);
            rejected.valid_until_ms = turn.valid_until_ms;
            rejected
        } else {
            turn
        }
    }

    /// Reserve any backing-array growth before the final authority check.
    pub fn reserve(&self, turn: &mut Prepared) -> Result<(), StoreError> {
        if self.revision != turn.revision {
            return Err(StoreError::StaleTurn);
        }
        turn.layout.reserve(&self.records)
    }

    pub fn commit(&mut self, turn: Prepared) -> Reply {
        self.commit_with_effects(turn).0
    }

    /// Publish one turn and return the already-allocated canonical effects
    /// for the actor's journal. Standalone callers can use `commit`.
    pub fn commit_with_effects(&mut self, mut turn: Prepared) -> (Reply, Effects) {
        if let Err(error) = self.reserve(&mut turn) {
            return (Reply::Error(error), Effects::new());
        }
        if !turn.touches.is_empty() || !turn.effects.is_empty() {
            self.access = self.access.saturating_add(1);
            self.revision = self.revision.wrapping_add(1);
            for position in turn.touches {
                self.records.touch(position, self.access);
            }
            self.records.apply(&turn.effects, turn.layout, self.access);
        }
        (turn.reply, turn.effects)
    }

    /// Active expiry does at most `item_limit` starting at the heap root,
    /// with an elapsed time check at each node. A cluster journals these
    /// deletes through the same authority and commit path as user commands.
    pub fn prepare_expiry(&self, now_ms: u64, item_limit: usize, time: Duration) -> Prepared {
        let mut turn = self.turn(now_ms);
        self.records.expired(now_ms, item_limit, time, |key| {
            turn.effects.push(Effect::Delete(key.clone()));
            turn.removals.expired += 1;
        });
        let Some(layout) = self.records.layout(&turn.effects, self.byte_limit, 0) else {
            return self.failure(now_ms, StoreError::OutOfMemory);
        };
        turn.layout = layout;
        turn
    }

    pub fn snapshot_requirements(slot_bytes: usize) -> (usize, usize) {
        records::snapshot_requirements(slot_bytes)
    }

    /// Retain an immutable snapshot with a conservative charge for all keys
    /// and values that might be replaced while it is streamed. The caller
    /// defines the sequence cut and must keep this charge until transfer ends.
    pub fn pin_snapshot(
        &self,
        byte_limit: usize,
        item_limit: usize,
        time: Duration,
    ) -> Result<PinnedSnapshot, StoreError> {
        self.records.pin_snapshot(byte_limit, item_limit, time)
    }

    /// Stage a canonical replication batch. The caller checks the message
    /// envelope and sequence; the store checks each effect and never evicts
    /// data on its own to make room for the batch.
    pub fn prepare_replica(
        &self,
        effects: Effects,
        growth_bytes: usize,
    ) -> Result<Prepared, StoreError> {
        let mut seen = HashSet::with_capacity(effects.len());
        for effect in &effects {
            if !seen.insert(effect.key()) || effect.key().len() > MAX_KEY_BYTES {
                return Err(StoreError::InvalidEffects);
            }
            if let Effect::Upsert {
                value, deadline_ms, ..
            } = effect
                && (value.len() > MAX_VALUE_BYTES
                    || deadline_ms.is_some_and(|deadline| deadline > i64::MAX as u64))
            {
                return Err(StoreError::InvalidEffects);
            }
        }
        let mut turn = self.turn(0);
        turn.layout = self
            .records
            .layout(&effects, self.byte_limit, growth_bytes)
            .ok_or(StoreError::OutOfMemory)?;
        // As with client commands, the peer reader must hand over owned
        // key/value bodies to avoid retaining a larger input frame.
        turn.effects = effects;
        Ok(turn)
    }

    fn turn(&self, now_ms: u64) -> Prepared {
        Prepared {
            reply: Reply::Ok,
            effects: Effects::new(),
            touches: SmallVec::new(),
            revision: self.revision,
            now_ms,
            valid_until_ms: None,
            relative_ttl_ms: None,
            ttl_reply: None,
            layout: Layout::unchanged(self.charged_bytes()),
            removals: RemovalCounts::default(),
        }
    }

    fn failure(&self, now_ms: u64, error: StoreError) -> Prepared {
        let mut turn = self.turn(now_ms);
        turn.reply = Reply::Error(error);
        turn
    }

    fn prepare_operation(
        &self,
        op: &Operation,
        limits: TurnLimits,
        turn: &mut Prepared,
    ) -> Result<(), StoreError> {
        if op.keys().any(|key| key.len() > MAX_KEY_BYTES) {
            return Err(StoreError::InvalidKey);
        }
        match op {
            Operation::Get(key) => {
                let entry = self.observe(key, turn);
                if let Some((position, entry)) = entry {
                    turn.touches.push(position);
                    turn.reply = Reply::Bulk(Some(entry.value.clone()));
                } else {
                    turn.reply = Reply::Bulk(None);
                }
            }
            Operation::GetMany(keys) => self.prepare_get_many(keys, limits, turn)?,
            Operation::Set {
                key,
                value,
                ttl_ms,
                when,
            } => {
                if value.len() > MAX_VALUE_BYTES {
                    return Err(StoreError::InvalidValue);
                }
                let deadline = ttl_ms
                    .map(|ttl| absolute_deadline(turn.now_ms, ttl))
                    .transpose()?;
                let exists = self.observe(key, turn).is_some();
                if (*when == SetWhen::Absent && exists) || (*when == SetWhen::Present && !exists) {
                    turn.reply = Reply::Bulk(None);
                } else {
                    turn.relative_ttl_ms = *ttl_ms;
                    Self::stage_upsert(turn, key, value.clone(), deadline);
                }
            }
            Operation::Delete(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.observe(key, turn).is_some() && Self::stage_delete(turn, key) {
                        count += 1;
                    }
                }
                turn.reply = Reply::Integer(count);
            }
            Operation::Exists(keys) => {
                let count = keys
                    .iter()
                    .filter(|key| self.observe(key, turn).is_some())
                    .count();
                turn.reply = Reply::Integer(count as i64);
            }
            Operation::Ttl { key, milliseconds } => {
                turn.reply = Reply::Integer(match self.observe(key, turn) {
                    None => -2,
                    Some((_, entry)) => match entry.deadline_ms {
                        None => -1,
                        Some(deadline) => {
                            turn.ttl_reply = Some((deadline, *milliseconds));
                            ttl_reply(deadline, turn.now_ms, *milliseconds)
                        }
                    },
                });
            }
            Operation::Expire { key, ttl_ms } => {
                let deadline = u64::try_from(*ttl_ms)
                    .ok()
                    .filter(|ttl| *ttl != 0)
                    .map(|ttl| absolute_deadline(turn.now_ms, ttl))
                    .transpose()?;
                if let Some((_, entry)) = self.observe(key, turn) {
                    if deadline.is_some() {
                        turn.relative_ttl_ms = Some(*ttl_ms as u64);
                        Self::stage_upsert(turn, key, entry.value.clone(), deadline);
                    } else {
                        Self::stage_delete(turn, key);
                    }
                    turn.reply = Reply::Integer(1);
                } else {
                    turn.reply = Reply::Integer(0);
                }
            }
            Operation::Persist(key) => {
                let changed = self
                    .observe(key, turn)
                    .filter(|(_, entry)| entry.deadline_ms.is_some());
                if let Some((_, entry)) = changed {
                    Self::stage_upsert(turn, key, entry.value.clone(), None);
                }
                turn.reply = Reply::Integer(i64::from(changed.is_some()));
            }
            Operation::Increment { key, delta } => {
                let entry = self.observe(key, turn).map(|(_, entry)| entry);
                let current = entry
                    .map(|entry| parse_integer(&entry.value).ok_or(StoreError::InvalidInteger))
                    .transpose()?
                    .unwrap_or_default();
                let value = current
                    .checked_add(*delta)
                    .ok_or(StoreError::IntegerOverflow)?;
                Self::stage_upsert(
                    turn,
                    key,
                    Bytes::from(value.to_string()),
                    entry.and_then(|entry| entry.deadline_ms),
                );
                turn.reply = Reply::Integer(value);
            }
        }
        Ok(())
    }

    fn prepare_get_many(
        &self,
        keys: &[Bytes],
        limits: TurnLimits,
        turn: &mut Prepared,
    ) -> Result<(), StoreError> {
        let mut reply_bytes = 3 + decimal_digits(keys.len() as u64);
        for key in keys {
            let live = self
                .records
                .get(key)
                .filter(|(_, entry)| !entry.is_expired(turn.now_ms));
            if let Some((_, entry)) = live {
                Self::observe_deadline(entry, turn);
            }
            reply_bytes =
                reply_bytes.saturating_add(bulk_bytes(live.map(|(_, entry)| &entry.value)));
            if reply_bytes > limits.reply_bytes {
                return Err(StoreError::ReplyLimit);
            }
        }

        let mut values = Vec::new();
        values
            .try_reserve_exact(keys.len())
            .map_err(|_| StoreError::OutOfMemory)?;
        for key in keys {
            values.push(self.observe(key, turn).map(|(position, entry)| {
                turn.touches.push(position);
                entry.value.clone()
            }));
        }
        turn.reply = Reply::Array(values);
        Ok(())
    }

    fn observe(&self, key: &Bytes, turn: &mut Prepared) -> Option<(usize, &records::Record)> {
        let (position, entry) = self.records.get(key)?;
        if entry.is_expired(turn.now_ms) {
            if Self::stage_delete(turn, key) {
                turn.removals.expired += 1;
            }
            return None;
        }
        Self::observe_deadline(entry, turn);
        Some((position, entry))
    }

    fn observe_deadline(entry: &records::Record, turn: &mut Prepared) {
        if let Some(deadline) = entry.deadline_ms {
            turn.valid_until_ms = Some(
                turn.valid_until_ms
                    .map_or(deadline, |old| old.min(deadline)),
            );
        }
    }

    fn stage_delete(turn: &mut Prepared, key: &Bytes) -> bool {
        if turn.effects.iter().any(|effect| effect.key() == key) {
            return false;
        }
        turn.effects.push(Effect::Delete(key.clone()));
        true
    }

    fn stage_upsert(turn: &mut Prepared, key: &Bytes, value: Bytes, deadline_ms: Option<u64>) {
        // All commands with an upsert address exactly one key. It may have
        // already been marked for deletion by lazy expiry.
        turn.effects.clear();
        turn.effects.push(Effect::Upsert {
            key: key.clone(),
            value,
            deadline_ms,
        });
    }

    fn fit_layout(&self, turn: &mut Prepared, growth_bytes: usize) -> Result<(), StoreError> {
        if let Some(layout) = self.fitting_layout(&turn.effects, growth_bytes) {
            turn.layout = layout;
            return Ok(());
        }
        if self.eviction == EvictionPolicy::NoEviction {
            return Err(StoreError::OutOfMemory);
        }
        let Some(upsert @ Effect::Upsert { .. }) = turn.effects.pop() else {
            return Err(StoreError::OutOfMemory);
        };
        let victims = self
            .records
            .sample_oldest(self.revision, upsert.key(), turn.now_ms);
        for victim in victims.into_iter().take(EVICTION_VICTIMS) {
            if self
                .records
                .get(&victim)
                .is_some_and(|(_, record)| record.is_expired(turn.now_ms))
            {
                turn.removals.expired += 1;
            } else {
                turn.removals.evicted += 1;
            }
            turn.effects.push(Effect::Delete(victim));
            turn.effects.push(upsert.clone());
            if let Some(layout) = self.fitting_layout(&turn.effects, growth_bytes) {
                turn.layout = layout;
                return Ok(());
            }
            turn.effects.pop();
        }
        Err(StoreError::OutOfMemory)
    }

    fn fitting_layout(&self, effects: &[Effect], growth_bytes: usize) -> Option<Layout> {
        self.records.layout(effects, self.byte_limit, growth_bytes)
    }
}

pub struct PinnedSnapshot {
    effects: Vec<Effect>,
    charge: usize,
}

impl PinnedSnapshot {
    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    pub fn charged_bytes(&self) -> usize {
        self.charge
    }

    /// Return a bounded batch. The caller advances by its length.
    pub fn chunk(
        &self,
        start: usize,
        item_limit: usize,
        byte_limit: usize,
    ) -> Result<&[Effect], StoreError> {
        let rest = self
            .effects
            .get(start..)
            .ok_or(StoreError::InvalidEffects)?;
        let mut used_bytes = 0usize;
        let mut count = 0;
        for effect in rest.iter().take(item_limit) {
            used_bytes = used_bytes.saturating_add(effect.charged_bytes());
            if used_bytes > byte_limit {
                break;
            }
            count += 1;
        }
        if count == 0 && !rest.is_empty() {
            return Err(StoreError::SnapshotLimit);
        }
        Ok(&rest[..count])
    }
}

pub(super) fn absolute_deadline(now: u64, ttl: u64) -> Result<u64, StoreError> {
    now.checked_add(ttl)
        .filter(|deadline| ttl != 0 && *deadline <= i64::MAX as u64)
        .ok_or(StoreError::InvalidTtl)
}

fn ttl_reply(deadline: u64, now: u64, milliseconds: bool) -> i64 {
    let remaining = deadline.saturating_sub(now);
    if milliseconds {
        remaining as i64
    } else {
        ((remaining + 500) / 1000) as i64
    }
}

/// Match Valkey's canonical signed decimal syntax, including rejection of
/// a leading plus sign, leading zeroes, negative zero and whitespace.
pub fn parse_integer(bytes: &[u8]) -> Option<i64> {
    if bytes == b"0" {
        return Some(0);
    }
    if bytes.len() > 20 {
        return None;
    }
    let digits = bytes.strip_prefix(b"-").unwrap_or(bytes);
    if !matches!(digits.first(), Some(b'1'..=b'9')) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod tests;
