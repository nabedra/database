//! Live-run leases, replay checkpoints and worker-local caches.
use super::wire::{self, Error, Format, Opened, SnapshotOpened};
use crate::{
    engine::stream::{Cursor, PinnedScan, ScanReader},
    protocol::{ErrorCode, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
    storage::context::{RequestContext, StorageKey},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(super) const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_PIN_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PIN_BYTES_PER_DATABASE: u64 = 16 * 1024 * 1024;
const MAX_IDENTITIES: usize = 384;
const MAX_PER_SCOPE: usize = 96;
const MAX_ACTIVE: usize = 32;
const MAX_ACTIVE_PER_SCOPE: usize = 16;
const MAX_SNAPSHOTS: usize = 16;
const MAX_SNAPSHOTS_PER_SCOPE: usize = 8;
const MAX_OUTPUT_SLOTS: usize = 48;
const MAX_SPECULATIVE_SLOTS: usize = 8;
pub(super) const IDLE_MS: u64 = 120_000;
pub(super) const LIFETIME_MS: u64 = 30 * 60_000;
const RETRY_MS: u64 = 120_000;
const DELIVERY_MS: u64 = 1_500;
const DELIVERY_LIFETIME_MS: u64 = 30_000;
const IDENTITY_BYTES: u64 = 4096;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Scope {
    storage: Option<StorageKey>,
    principal: [u8; 32],
}
impl Scope {
    pub(super) fn from_context(context: Option<&RequestContext>) -> Result<Self, ProtocolError> {
        let (storage, principal) = match context {
            Some(context) => {
                let credentials = context.credentials();
                let mut principal = Sha256::new();
                principal.update(b"nabedra.olap.stream.caller.v1");
                for part in [
                    credentials.access_key_id.as_str(),
                    credentials.secret_access_key.as_str(),
                    credentials.session_token.as_deref().unwrap_or_default(),
                ] {
                    principal.update((part.len() as u64).to_be_bytes());
                    principal.update(part.as_bytes());
                }
                (
                    Some(context.storage_key().map_err(|_| wire::unauthorized())?),
                    principal.finalize().into(),
                )
            }
            None => (None, [0; 32]),
        };
        Ok(Self { storage, principal })
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Key {
    pub scope: Scope,
    pub id: Uuid,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Version {
    pub term: u64,
    pub index: u64,
}
impl Version {
    pub(super) fn advance(previous: Option<Self>, term: u64) -> Result<Self, ProtocolError> {
        match previous {
            Some(previous) if previous.term > term => Err(wire::unavailable()),
            Some(previous) if previous.term == term => Ok(Self {
                term,
                index: previous
                    .index
                    .checked_add(1)
                    .ok_or_else(wire::unavailable)?,
            }),
            _ => Ok(Self { term, index: 1 }),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub key: Key,
    pub run: Uuid,
    pub version: Version,
    pub identity: [u8; 32],
    #[serde(
        serialize_with = "serialize_request",
        deserialize_with = "deserialize_request"
    )]
    pub accepted_request: Option<Arc<str>>,
    pub idle_at: u64,
    pub closed_at: Option<u64>,
    pub context: Option<RequestContext>,
    #[serde(
        serialize_with = "serialize_scan",
        deserialize_with = "deserialize_scan"
    )]
    pub scan: Option<Arc<PinnedScan>>,
    pub kind: Kind,
}
impl Entry {
    fn handle_id(&self) -> Uuid {
        match &self.kind {
            Kind::Stream(stream) => stream.opened.stream_id,
            Kind::Snapshot(snapshot) => snapshot.handle_id,
        }
    }

    pub(super) fn active_until(&self) -> u64 {
        let cutoff = match &self.kind {
            Kind::Stream(stream) => stream.opened.expires_at.min(self.idle_at),
            Kind::Snapshot(snapshot) => snapshot.opened.expires_at,
        };
        cutoff.min(self.closed_at.unwrap_or(u64::MAX))
    }

    pub(super) fn active(&self, now: u64) -> bool {
        self.closed_at.is_none() && now < self.active_until() && self.scan.is_some()
    }

    pub(super) fn retained(&self, now: u64) -> bool {
        now < self.active_until().saturating_add(RETRY_MS)
    }

    pub(super) fn compact(&mut self, now: u64) {
        if !self.active(now) {
            self.scan = None;
            self.context = None;
            self.accepted_request = None;
        }
    }
}
fn serialize_request<S: serde::Serializer>(
    request: &Option<Arc<str>>,
    encoder: S,
) -> Result<S::Ok, S::Error> {
    request.as_deref().serialize(encoder)
}
fn deserialize_request<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Arc<str>>, D::Error> {
    Option::<String>::deserialize(decoder).map(|request| request.map(Arc::from))
}
fn serialize_scan<S: serde::Serializer>(
    scan: &Option<Arc<PinnedScan>>,
    encoder: S,
) -> Result<S::Ok, S::Error> {
    scan.as_deref().serialize(encoder)
}
fn deserialize_scan<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Arc<PinnedScan>>, D::Error> {
    Option::<PinnedScan>::deserialize(decoder).map(|scan| scan.map(Arc::new))
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Kind {
    Stream(Stream),
    Snapshot(Snapshot),
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Snapshot {
    pub opened: SnapshotOpened,
    pub handle_id: Uuid,
    pub shards: u32,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stream {
    pub opened: Opened,
    pub batch_rows: usize,
    pub format: Format,
    pub drop_last: bool,
    pub first: Cursor,
    pub checkpoint: Option<Checkpoint>,
}
impl Stream {
    pub(super) fn options(&self) -> crate::engine::stream::BatchOptions {
        crate::engine::stream::BatchOptions {
            batch_rows: self.batch_rows,
            max_batch_bytes: wire::MAX_BATCH_BYTES,
            format: match self.format {
                Format::Rows => crate::engine::stream::BatchFormat::Rows,
                Format::Arrow => crate::engine::stream::BatchFormat::Arrow,
            },
            drop_last: self.drop_last,
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    pub sequence: u64,
    pub first: Cursor,
    pub next: Cursor,
    pub outcome: Frame,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Frame {
    Bytes([u8; 32]),
    Eof,
    Error(ProtocolError),
}

pub(in crate::node) struct Registry {
    state: Mutex<State>,
    pub speculation: Arc<Semaphore>,
    pub outputs: Arc<Semaphore>,
    pub speculative_outputs: Arc<Semaphore>,
    pub eviction: AtomicU64,
    owns_scheduling: AtomicBool,
    pub stopped: AtomicBool,
    pub reaper_started: AtomicBool,
}
struct State {
    run: Option<Uuid>,
    entries: BTreeMap<Key, Arc<Stored>>,
    deliveries: BTreeMap<Uuid, DeliveryHold>,
}
struct DeliveryHold {
    idle_at: u64,
    expires_at: u64,
    _bytes: Bytes,
}
pub(super) struct Stored {
    pub entry: Entry,
    pub activity: Arc<Activity>,
    _identity: Arc<MemoryCharge>,
    retained: Option<Arc<MemoryCharge>>,
}
#[derive(Default)]
pub(super) struct Activity {
    delivering: AtomicBool,
    pub speculative: AtomicU8,
    pub cache: Mutex<Cache>,
    pub scanning: tokio::sync::Mutex<()>,
    pub reader: Mutex<Option<ScanReader>>,
}
pub(super) struct Delivery(pub Arc<Activity>);
impl Drop for Delivery {
    fn drop(&mut self) {
        self.0.delivering.store(false, Ordering::Release);
    }
}
impl Activity {
    pub(super) fn delivery(self: &Arc<Self>) -> Result<Delivery, ProtocolError> {
        self.delivering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| wire::overloaded())?;
        Ok(Delivery(self.clone()))
    }

    fn clear(&self) {
        self.cache.lock().unwrap().clear();
        self.reader.lock().unwrap().take();
    }
}

#[derive(Default)]
pub(super) struct Cache {
    pub term: u64,
    pub last: Option<(u64, Arc<Output>)>,
    pub ahead: BTreeMap<u64, Ahead>,
}
pub(super) struct Ahead {
    pub output: Arc<Output>,
    pub _slot: OwnedSemaphorePermit,
}
pub(super) struct Output {
    pub checkpoint: Checkpoint,
    pub bytes: Option<Bytes>,
    _slot: Option<OwnedSemaphorePermit>,
}
impl Output {
    pub(super) fn new(
        checkpoint: Checkpoint,
        bytes: Option<Bytes>,
        slot: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            checkpoint,
            bytes,
            _slot: slot,
        }
    }
}
impl Cache {
    pub(super) fn check_term(&mut self, term: u64) {
        if self.term != term {
            self.clear();
            self.term = term;
        }
    }
    pub(super) fn clear(&mut self) {
        self.last = None;
        self.ahead.clear();
    }

    pub(super) fn take(&mut self, term: u64, sequence: u64, first: Cursor) -> Option<Arc<Output>> {
        self.check_term(term);
        let output = if let Some((_, output)) =
            self.last.as_ref().filter(|(cached, _)| *cached == sequence)
        {
            Some(output.clone())
        } else {
            // The prior frame remains reproducible from its checkpoint.
            self.last.take();
            self.ahead.remove(&sequence).map(|ahead| ahead.output)
        };
        output.filter(|output| output.checkpoint.first == first)
    }
}

impl Registry {
    pub(in crate::node) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                run: None,
                entries: BTreeMap::new(),
                deliveries: BTreeMap::new(),
            }),
            speculation: Arc::new(Semaphore::new(1)),
            outputs: Arc::new(Semaphore::new(MAX_OUTPUT_SLOTS)),
            speculative_outputs: Arc::new(Semaphore::new(MAX_SPECULATIVE_SLOTS)),
            eviction: AtomicU64::new(0),
            owns_scheduling: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            reaper_started: AtomicBool::new(false),
        }
    }

    pub(in crate::node) fn set_run(&self, run: Uuid, clear: bool) -> Result<(), ProtocolError> {
        let mut state = self.state.lock().unwrap();
        match state.run {
            None => state.run = Some(run),
            Some(existing) if existing == run => {}
            Some(_) => return Err(wire::unavailable()),
        }
        if clear {
            state.entries.clear();
            state.deliveries.clear();
        }
        Ok(())
    }

    pub(in crate::node) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        state.entries.clear();
        state.deliveries.clear();
    }

    pub(in crate::node) fn reap(&self, now: u64) {
        self.state.lock().unwrap().reap(now);
    }

    pub(super) fn hold_delivery(
        &self,
        token: Uuid,
        bytes: Bytes,
        now: u64,
    ) -> Result<(), ProtocolError> {
        let mut state = self.state.lock().unwrap();
        state.deliveries.retain(|_, hold| hold.idle_at > now);
        if state.deliveries.len() >= MAX_ACTIVE
            || token.is_nil()
            || state.deliveries.contains_key(&token)
        {
            return Err(wire::overloaded());
        }
        state.deliveries.insert(
            token,
            DeliveryHold {
                idle_at: now.saturating_add(DELIVERY_MS),
                expires_at: now.saturating_add(DELIVERY_LIFETIME_MS),
                _bytes: bytes,
            },
        );
        Ok(())
    }

    pub(super) fn renew_delivery(&self, token: Uuid, now: u64) -> Result<(), ProtocolError> {
        let mut state = self.state.lock().unwrap();
        let hold = state
            .deliveries
            .get_mut(&token)
            .filter(|hold| hold.idle_at > now)
            .ok_or_else(wire::unavailable)?;
        hold.idle_at = hold.expires_at.min(now.saturating_add(DELIVERY_MS));
        Ok(())
    }

    pub(super) fn release_delivery(&self, token: Uuid) {
        self.state.lock().unwrap().deliveries.remove(&token);
    }

    pub(in crate::node) fn evict_speculation(&self) {
        self.eviction.fetch_add(1, Ordering::AcqRel);
        let state = self.state.lock().unwrap();
        for record in state.entries.values() {
            record.activity.cache.lock().unwrap().ahead.clear();
            record.activity.reader.lock().unwrap().take();
        }
    }

    pub(in crate::node) fn scheduler_owner(&self, owned: bool) {
        if self.owns_scheduling.swap(owned, Ordering::AcqRel) && !owned {
            self.eviction.fetch_add(1, Ordering::AcqRel);
            for record in self.state.lock().unwrap().entries.values() {
                record.activity.clear();
            }
        }
    }

    pub(super) fn get(&self, key: &Key) -> Option<Arc<Stored>> {
        self.state.lock().unwrap().entries.get(key).cloned()
    }

    pub(super) fn handle(&self, key: &Key) -> Result<Option<Arc<Stored>>, Error> {
        let state = self.state.lock().unwrap();
        for stored in state
            .entries
            .values()
            .filter(|stored| stored.entry.handle_id() == key.id)
        {
            if stored.entry.key.scope == key.scope {
                return Ok(Some(stored.clone()));
            }
            return Err(wire::unauthorized().into());
        }
        Ok(None)
    }

    pub(super) fn page(
        &self,
        run: Uuid,
        after: Option<&Key>,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        let mut state = self.state.lock().unwrap();
        if state.run != Some(run) {
            return Err(wire::unavailable());
        }
        state.reap(super::now_ms());
        use std::ops::Bound::{Excluded, Unbounded};
        Ok(match after {
            Some(key) => state.entries.range((Excluded(key), Unbounded)).next(),
            None => state.entries.first_key_value(),
        }
        .map(|(_, entry)| entry.clone()))
    }

    pub(super) fn install(
        &self,
        mut entry: Entry,
        expect: Option<Option<Version>>,
        owned_bytes: u64,
        budget: &ResourceScheduler,
        now: u64,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        let mut state = self.state.lock().unwrap();
        if self.stopped.load(Ordering::Acquire) || state.run != Some(entry.run) {
            return Err(wire::unavailable());
        }
        state.reap(now);
        entry.compact(now);
        let previous = state.entries.get(&entry.key);
        if expect.is_some_and(|expect| expect != previous.map(|record| record.entry.version)) {
            return Err(wire::conflict());
        }
        if let Some(previous) = previous {
            if entry.version < previous.entry.version {
                return Err(wire::conflict());
            }
            if entry.version == previous.entry.version {
                if entry != previous.entry {
                    return Err(wire::conflict());
                }
                return Ok(Some(previous.clone()));
            }
            if entry.identity != previous.entry.identity {
                // An unacknowledged partial open from an earlier term cannot
                // override the identity installed by the authoritative owner.
                if entry.version.term <= previous.entry.version.term {
                    return Err(wire::conflict());
                }
            }
        }
        if !entry.retained(now) {
            return Ok(None);
        }
        if previous.is_none() {
            state.admit(&entry, now)?;
        }
        let reserved = previous.filter(|old| {
            entry.scan.is_some()
                && entry.scan == old.entry.scan
                && entry.context == old.entry.context
        });
        let retained = if entry.scan.is_none() {
            None
        } else if let Some(old) = reserved {
            entry.scan = old.entry.scan.clone();
            old.retained.clone()
        } else {
            let (pinned, database_pinned) =
                state
                    .entries
                    .values()
                    .fold((0, 0), |(all, database), stored| {
                        let bytes = stored.retained.as_ref().map_or(0, |charge| charge.bytes());
                        let in_database = stored.entry.key.scope.storage == entry.key.scope.storage;
                        (all + bytes, database + if in_database { bytes } else { 0 })
                    });
            if owned_bytes > MAX_PIN_BYTES.saturating_sub(pinned)
                || owned_bytes > MAX_PIN_BYTES_PER_DATABASE.saturating_sub(database_pinned)
            {
                return Err(wire::overloaded());
            }
            Some(Arc::new(budget.charge_memory(0, owned_bytes)?))
        };
        let activity = previous.map_or_else(
            || Arc::new(Activity::default()),
            |previous| previous.activity.clone(),
        );
        if !entry.active(now) {
            activity.clear();
        }
        let identity = if let Some(previous) = previous {
            previous._identity.clone()
        } else {
            Arc::new(budget.charge_memory(0, IDENTITY_BYTES)?)
        };
        let stored = Arc::new(Stored {
            entry,
            activity,
            _identity: identity,
            retained,
        });
        state
            .entries
            .insert(stored.entry.key.clone(), stored.clone());
        Ok(Some(stored))
    }
}
impl State {
    fn reap(&mut self, now: u64) {
        self.deliveries.retain(|_, hold| hold.idle_at > now);
        self.entries.retain(|_, record| {
            if !record.entry.retained(now) {
                record.activity.clear();
                return false;
            }
            if !record.entry.active(now) && record.entry.scan.is_some() {
                let mut entry = record.entry.clone();
                entry.compact(now);
                record.activity.clear();
                *record = Arc::new(Stored {
                    entry,
                    activity: record.activity.clone(),
                    _identity: record._identity.clone(),
                    retained: None,
                });
            }
            true
        });
    }

    fn admit(&self, entry: &Entry, now: u64) -> Result<(), ProtocolError> {
        let mut scope_count = 0;
        let mut active = 0;
        let mut scope_active = 0;
        let mut snapshots = 0;
        let mut scope_snapshots = 0;
        for record in self.entries.values() {
            let same_scope = record.entry.key.scope.storage == entry.key.scope.storage;
            scope_count += usize::from(same_scope);
            if record.entry.active(now) {
                match record.entry.kind {
                    Kind::Stream(_) => {
                        active += 1;
                        scope_active += usize::from(same_scope);
                    }
                    Kind::Snapshot(_) => {
                        snapshots += 1;
                        scope_snapshots += usize::from(same_scope);
                    }
                }
            }
        }
        let active_full = match entry.kind {
            Kind::Stream(_) => active >= MAX_ACTIVE || scope_active >= MAX_ACTIVE_PER_SCOPE,
            Kind::Snapshot(_) => {
                snapshots >= MAX_SNAPSHOTS || scope_snapshots >= MAX_SNAPSHOTS_PER_SCOPE
            }
        };
        if self.entries.len() >= MAX_IDENTITIES
            || scope_count >= MAX_PER_SCOPE
            || (entry.active(now) && active_full)
        {
            Err(wire::overloaded())
        } else {
            Ok(())
        }
    }
}

pub(super) fn retryable(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::Overloaded | ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown
    )
}
