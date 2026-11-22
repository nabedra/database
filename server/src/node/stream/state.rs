//! Live-run leases, replay checkpoints and worker-local caches.
use super::{
    Authority,
    wire::{self, Error, Format, Opened, SnapshotOpened},
};
use crate::{
    cluster::state::MAX_NODES,
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
pub(super) const MAX_IDENTITIES: usize = 384;
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
            self.clear_payload();
        }
    }

    fn clear_payload(&mut self) {
        self.scan = None;
        self.context = None;
        self.accepted_request = None;
    }

    fn matches_compacted(&self, other: &Self) -> bool {
        let mut entry = self.clone();
        let mut other = other.clone();
        entry.clear_payload();
        other.clear_payload();
        entry == other
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::node) struct RecoveryView {
    pub(super) run: Uuid,
    pub(super) term: u64,
    pub(super) ready: [u8; 32],
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryStatus {
    AllPeers,
    LegacyPeers,
}

#[derive(Default)]
pub(super) struct RecoveryCursor {
    pub(super) peer: usize,
    pub(super) after: Option<Key>,
    pages: usize,
}
impl RecoveryCursor {
    pub(super) fn check_page(&self) -> Result<(), ProtocolError> {
        if self.pages >= MAX_IDENTITIES {
            return Err(wire::invalid());
        }
        Ok(())
    }

    pub(super) fn finish_page(&mut self, key: Key) {
        self.after = Some(key);
        self.pages += 1;
    }

    pub(super) fn finish_peer(&mut self) {
        self.peer += 1;
        self.after = None;
        self.pages = 0;
    }
}

pub(super) struct Recovery {
    pub(super) view: RecoveryView,
    pub(super) merge: Option<RecoveryCursor>,
    pub(super) repair: RecoveryCursor,
    pub(super) legacy_peers: [bool; MAX_NODES],
    _charge: MemoryCharge,
}
impl Recovery {
    pub(super) fn new(view: RecoveryView, charge: MemoryCharge) -> Self {
        Self {
            view,
            merge: Some(RecoveryCursor::default()),
            repair: RecoveryCursor::default(),
            legacy_peers: [false; MAX_NODES],
            _charge: charge,
        }
    }
}

pub(in crate::node) struct Registry {
    state: Mutex<State>,
    pub(super) recovery: tokio::sync::Mutex<Option<Recovery>>,
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
    observed_term: u64,
    recovered_view: Option<(RecoveryView, RecoveryStatus)>,
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
                observed_term: 0,
                recovered_view: None,
                entries: BTreeMap::new(),
                deliveries: BTreeMap::new(),
            }),
            recovery: tokio::sync::Mutex::new(None),
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
        state.bind_run(run)?;
        if clear {
            state.clear();
        }
        Ok(())
    }

    pub(super) fn reset_replica(&self, authority: Authority) -> Result<(), ProtocolError> {
        let mut state = self.live_state()?;
        state.bind_run(authority.run)?;
        state.observe(authority.run, authority.term)?;
        state.clear();
        Ok(())
    }

    pub(super) fn observe(&self, authority: Authority) -> Result<(), ProtocolError> {
        self.live_state()?.observe(authority.run, authority.term)
    }

    fn live_state(&self) -> Result<std::sync::MutexGuard<'_, State>, ProtocolError> {
        let state = self.state.lock().unwrap();
        if self.stopped.load(Ordering::Acquire) {
            return Err(wire::unavailable());
        }
        Ok(state)
    }

    pub(super) fn recovery_status(&self, view: RecoveryView) -> Option<RecoveryStatus> {
        let state = self.state.lock().unwrap();
        let (recovered, status) = state.recovered_view?;
        (state.run == Some(view.run) && recovered == view).then_some(status)
    }

    pub(super) fn handoff_ready(&self, view: RecoveryView, now: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some((recovered, status)) = state.recovered_view else {
            return false;
        };
        if state.run != Some(view.run) || recovered != view {
            return false;
        }
        if status == RecoveryStatus::AllPeers {
            return true;
        }
        state.reap(now);
        state.entries.is_empty()
    }

    pub(super) fn begin_recovery(&self, view: RecoveryView) -> Result<(), ProtocolError> {
        let mut state = self.live_state()?;
        state.bind_run(view.run)?;
        state.observe(view.run, view.term)?;
        state.recovered_view = None;
        Ok(())
    }

    pub(super) fn mark_recovered(
        &self,
        view: RecoveryView,
        status: RecoveryStatus,
    ) -> Result<(), ProtocolError> {
        let mut state = self.live_state()?;
        state.observe(view.run, view.term)?;
        state.recovered_view = Some((view, status));
        Ok(())
    }

    pub(in crate::node) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.state.lock().unwrap().clear();
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
        if let Some(stored) = state
            .entries
            .values()
            .find(|stored| stored.entry.handle_id() == key.id)
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
        authority: Authority,
        after: Option<&Key>,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        let mut state = self.live_state()?;
        state.observe(authority.run, authority.term)?;
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
        entry: Entry,
        expect: Option<Option<Version>>,
        owned_bytes: u64,
        budget: &ResourceScheduler,
        now: u64,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        self.live_state()?
            .install(entry, expect, owned_bytes, budget, now)
    }

    pub(super) fn install_fenced(
        &self,
        authority: Authority,
        entry: Entry,
        expect: Option<Option<Version>>,
        owned_bytes: u64,
        budget: &ResourceScheduler,
        now: u64,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        let mut state = self.live_state()?;
        state.observe(authority.run, authority.term)?;
        if entry.run != authority.run || entry.version.term > authority.term {
            return Err(wire::unavailable());
        }
        state.install(entry, expect, owned_bytes, budget, now)
    }
}
impl State {
    fn bind_run(&mut self, run: Uuid) -> Result<(), ProtocolError> {
        match self.run {
            None => self.run = Some(run),
            Some(existing) if existing == run => {}
            Some(_) => return Err(wire::unavailable()),
        }
        Ok(())
    }

    fn observe(&mut self, run: Uuid, term: u64) -> Result<(), ProtocolError> {
        if self.run != Some(run) || term < self.observed_term {
            return Err(wire::unavailable());
        }
        if term > self.observed_term {
            self.observed_term = term;
            self.recovered_view = None;
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.recovered_view = None;
        self.entries.clear();
        self.deliveries.clear();
    }

    fn install(
        &mut self,
        mut entry: Entry,
        expect: Option<Option<Version>>,
        owned_bytes: u64,
        budget: &ResourceScheduler,
        now: u64,
    ) -> Result<Option<Arc<Stored>>, ProtocolError> {
        if self.run != Some(entry.run) {
            return Err(wire::unavailable());
        }
        self.reap(now);
        entry.compact(now);
        let previous = self.entries.get(&entry.key);
        if expect.is_some_and(|expect| expect != previous.map(|record| record.entry.version)) {
            return Err(wire::conflict());
        }
        if let Some(previous) = previous {
            if entry.identity != previous.entry.identity
                || entry.handle_id() != previous.entry.handle_id()
            {
                return Err(wire::conflict());
            }
            if entry.version < previous.entry.version {
                // A recovery page can arrive after a newer copy was merged.
                return if expect.is_none() {
                    Ok(Some(previous.clone()))
                } else {
                    Err(wire::conflict())
                };
            }
            if entry.version == previous.entry.version {
                if entry == previous.entry {
                    return Ok(Some(previous.clone()));
                }
                if (entry.scan.is_some() && previous.entry.scan.is_some())
                    || !entry.matches_compacted(&previous.entry)
                {
                    return Err(wire::conflict());
                }
                // Replicas can reap independently. Preserve a complete copy
                // while its deadline is still in the future at this worker.
                if entry.scan.is_none() {
                    return Ok(Some(previous.clone()));
                }
            }
        }
        if !entry.retained(now) {
            return Ok(None);
        }
        if previous.is_none() {
            self.admit(&entry, now)?;
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
                self.entries
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
        self.entries
            .insert(stored.entry.key.clone(), stored.clone());
        Ok(Some(stored))
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::WorkerResources,
        protocol::{ColumnType, Query, TableFormat},
        storage::{Catalog, CatalogEntry, TableData},
    };

    #[test]
    fn r05_q03_stream_page_fences_delayed_replica_writes() {
        let former = Authority {
            run: Uuid::new_v4(),
            term: 7,
        };
        let current = Authority { term: 8, ..former };
        let registry = Registry::new();
        registry.reset_replica(former).unwrap();
        let now = super::super::now_ms();
        let key = Key {
            scope: Scope::from_context(None).unwrap(),
            id: Uuid::new_v4(),
        };
        let late = Entry {
            key: key.clone(),
            run: former.run,
            version: Version { term: 7, index: 1 },
            identity: [1; 32],
            accepted_request: None,
            idle_at: now,
            closed_at: Some(now),
            context: None,
            scan: None,
            kind: Kind::Snapshot(Snapshot {
                opened: SnapshotOpened {
                    snapshot_id: "old".into(),
                    run_id: former.run,
                    expires_at: now + IDLE_MS,
                },
                handle_id: Uuid::new_v4(),
                shards: 1,
            }),
        };
        let budget = ResourceScheduler::new(vec![(0, WorkerResources::default())]).unwrap();
        let install =
            |authority, entry| registry.install_fenced(authority, entry, None, 0, &budget, now);

        // An old install passed its async fence check before the new page.
        assert!(registry.page(current, None).unwrap().is_none());
        let failure = install(former, late.clone()).err().unwrap();
        assert_eq!(failure.code, ErrorCode::StorageUnavailable);
        assert!(registry.get(&key).is_none());

        let mut reopened = late.clone();
        reopened.version = Version { term: 8, index: 1 };
        let Kind::Snapshot(snapshot) = &mut reopened.kind else {
            unreachable!()
        };
        snapshot.handle_id = Uuid::new_v4();
        assert_eq!(
            install(current, reopened.clone())
                .unwrap()
                .unwrap()
                .entry
                .handle_id(),
            reopened.handle_id()
        );
        assert_eq!(
            registry.reset_replica(former).unwrap_err().code,
            ErrorCode::StorageUnavailable
        );
        assert_eq!(
            registry.get(&key).unwrap().entry.handle_id(),
            reopened.handle_id()
        );

        // A current repair can restore an older record from another holder.
        let mut surviving = late;
        surviving.key.id = Uuid::new_v4();
        let restored = install(current, surviving.clone()).unwrap().unwrap();
        assert!(restored.entry == surviving);
    }

    #[test]
    fn st04_recovery_restores_a_complete_record_missing_from_the_new_owner() {
        let run = Uuid::new_v4();
        let table = Uuid::new_v4();
        let mut catalog = Catalog::empty();
        catalog.tables.insert(
            "examples".into(),
            CatalogEntry {
                id: table,
                schema: BTreeMap::from([("id".into(), ColumnType::Int64)]),
                key: "id".into(),
                format: TableFormat::Parquet,
                data: TableData::Legacy {
                    snapshot: "fixture".into(),
                },
                row_count: 8,
            },
        );
        let scan = Arc::new(
            PinnedScan::capture(
                &catalog,
                table,
                Query {
                    select: None,
                    filter: None,
                    order_by: None,
                    limit: None,
                },
            )
            .unwrap(),
        );
        let first = scan.cursor(0, 1).unwrap();
        let key = Key {
            scope: Scope::from_context(None).unwrap(),
            id: Uuid::new_v4(),
        };
        let opened = Opened {
            stream_id: Uuid::new_v4(),
            run_id: run,
            prefetch_batches: 0,
            expires_at: 1_000,
        };
        let initial = Entry {
            key: key.clone(),
            run,
            version: Version { term: 1, index: 1 },
            identity: [7; 32],
            accepted_request: Some(Arc::from("{}")),
            idle_at: 500,
            closed_at: None,
            context: None,
            scan: Some(scan.clone()),
            kind: Kind::Stream(Stream {
                opened: opened.clone(),
                batch_rows: 4,
                format: Format::Rows,
                drop_last: false,
                first,
                checkpoint: None,
            }),
        };
        let mut latest = initial.clone();
        latest.version = Version { term: 1, index: 2 };
        latest.idle_at = 600;
        let Kind::Stream(stream) = &mut latest.kind else {
            unreachable!()
        };
        let checkpoint = Checkpoint {
            sequence: 0,
            first,
            next: scan.cursor(1, 2).unwrap(),
            outcome: Frame::Bytes([8; 32]),
        };
        stream.checkpoint = Some(checkpoint.clone());

        let budget = ResourceScheduler::new(vec![(0, WorkerResources::default())]).unwrap();
        let old_owner = Registry::new();
        let holder = Registry::new();
        let new_owner = Registry::new();
        for registry in [&old_owner, &holder, &new_owner] {
            registry.set_run(run, false).unwrap();
        }
        let install = |registry: &Registry, entry: Entry| {
            let bytes = super::super::record_bytes(&entry).unwrap();
            registry.install(entry, None, bytes, &budget, 100)
        };
        install(&old_owner, latest.clone()).unwrap();
        install(&holder, latest.clone()).unwrap();
        old_owner.stop();
        assert!(new_owner.get(&key).is_none());
        let view = RecoveryView {
            run,
            term: 2,
            ready: [7; 32],
        };
        assert!(new_owner.recovery_status(view).is_none());

        let page = holder.get(&key).unwrap().entry.clone();
        let encoded = serde_json::to_vec(&page).unwrap();
        let page = serde_json::from_slice(&encoded).unwrap();
        install(&new_owner, page).unwrap();
        install(&new_owner, initial.clone()).unwrap();
        let mut compacted = latest.clone();
        compacted.compact(latest.idle_at);
        install(&new_owner, compacted).unwrap();
        let merged = new_owner.get(&key).unwrap();
        install(&holder, merged.entry.clone()).unwrap();
        new_owner
            .mark_recovered(view, RecoveryStatus::AllPeers)
            .unwrap();
        assert!(new_owner.recovery_status(view) == Some(RecoveryStatus::AllPeers));
        assert!(new_owner.handoff_ready(view, 100));
        assert!(
            new_owner
                .recovery_status(RecoveryView {
                    ready: [8; 32],
                    ..view
                })
                .is_none()
        );
        assert!(merged.entry == latest);
        assert!(holder.get(&key).unwrap().entry == latest);
        let Kind::Stream(stream) = &merged.entry.kind else {
            unreachable!()
        };
        assert_eq!(stream.opened, opened);
        assert!(stream.checkpoint.as_ref() == Some(&checkpoint));

        let mut conflicting_checkpoint = latest.clone();
        conflicting_checkpoint.idle_at += 1;
        let error = install(&new_owner, conflicting_checkpoint).err().unwrap();
        assert_eq!(error.code, ErrorCode::Conflict);
        let mut conflicting = initial;
        conflicting.version = Version { term: 2, index: 1 };
        conflicting.identity = [9; 32];
        let error = install(&new_owner, conflicting).err().unwrap();
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(new_owner.get(&key).unwrap().entry == latest);

        // R03: a legacy cohort may hand off ordinary work once the last
        // stream identity expires, including its retry retention window.
        let legacy_view = RecoveryView {
            ready: [9; 32],
            ..view
        };
        new_owner
            .mark_recovered(legacy_view, RecoveryStatus::LegacyPeers)
            .unwrap();
        assert!(!new_owner.handoff_ready(legacy_view, 100));
        assert!(!new_owner.handoff_ready(legacy_view, latest.idle_at));
        assert!(new_owner.get(&key).unwrap().entry.scan.is_none());
        assert!(new_owner.handoff_ready(legacy_view, latest.idle_at + RETRY_MS));
        assert!(new_owner.get(&key).is_none());
        assert!(new_owner.recovery_status(legacy_view) == Some(RecoveryStatus::LegacyPeers));
    }
}
