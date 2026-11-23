mod copies;
mod journal;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nabedra_rt::task;
use nabedra_rt::time::{self, MissedTickBehavior, interval};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use crate::cache::standalone::response;
use crate::cache::store::{
    EvictionPolicy, Operation, Prepared, Reply, SlotStore, StoreError, TurnLimits,
};
use crate::cache::wire::Response;

use super::super::clock::{Clock, ClockBounds, ClockMode};
use super::super::protocol::{Frozen, Lease, Progress, Topology, WorkerId};
#[cfg(feature = "sim")]
use super::super::sim::hooks::{self, Observed};
use super::peer::{Packet, Probe};
use super::{MIB, frozen, invalid, unavailable};
use copies::Copies;
use journal::Journal;

const CONTROL_PENDING: usize = 4;
const TRANSFER_PENDING: usize = 4;
const EXPIRY_PER_TICK: usize = 8;

#[derive(Clone, Copy)]
pub(super) struct ActorConfig {
    pub clock: ClockMode,
    pub id: WorkerId,
    pub count: usize,
    pub slot_bytes: usize,
    pub primary_bytes: usize,
    pub replica_bytes: usize,
    pub journal_bytes: usize,
    pub snapshot_bytes: usize,
    pub turns: TurnLimits,
    pub max_pending: usize,
    pub eviction: EvictionPolicy,
}

impl ActorConfig {
    pub fn validate(self) -> io::Result<()> {
        if self.turns.growth_bytes < MIB
            || self.primary_bytes < MIB
            || self.replica_bytes < MIB
            || self.journal_bytes < MIB + (64 << 10)
            || !Copies::supports(self.slot_bytes, self.snapshot_bytes)
        {
            return Err(invalid("worker capacity is too small for the actor count"));
        }
        Ok(())
    }
}

pub(super) struct ClientTurn {
    pub generation: u64,
    pub epoch: u64,
    pub slot: u16,
    pub operation: Operation,
    pub queued: Instant,
    pub reply: oneshot::Sender<Response>,
}

pub(super) enum Control {
    Install {
        topology: Arc<Topology>,
        grants: Arc<Vec<Option<Lease>>>,
        done: oneshot::Sender<()>,
    },
    Progress {
        telemetry: bool,
        reply: oneshot::Sender<ActorReport>,
    },
    #[cfg(feature = "sim")]
    Sample {
        slot: u16,
        reply: oneshot::Sender<Option<SlotSample>>,
    },
}

#[cfg(feature = "sim")]
#[derive(Clone, Copy, Debug)]
pub(in crate::cache::cluster) struct SlotSample {
    pub generation: u64,
    pub epoch: u64,
    pub sequence: u64,
    pub digest: [u8; 20],
    pub authorized: bool,
    pub baseline: bool,
    pub lease: Option<Lease>,
}

#[derive(Serialize)]
pub(super) struct ActorReport {
    #[serde(skip)]
    pub slots: Vec<Progress>,
    pub generation: u64,
    pub revision: u64,
    primary_bytes: usize,
    replica_bytes: usize,
    snapshot_bytes: usize,
    journal_bytes: usize,
    missing_replica_baselines: usize,
    queued_turns: usize,
    oldest_queued_us: Option<u64>,
    oldest_queued_slot: Option<u16>,
    abandoned_actor_replies: u64,
    slot_stats: Vec<SlotMetrics>,
}

#[derive(Serialize)]
struct SlotMetrics {
    slot: u16,
    epoch: u64,
    operations: u64,
    write_operations: u64,
    expired: u64,
    evicted: u64,
    max_queue_us: u64,
    lease_headroom_ms: Option<u64>,
    replica_acknowledged: Option<u64>,
    replica_lag: Option<u64>,
}

pub(super) enum Transfer {
    Sample {
        source: WorkerId,
        generation: u64,
        slots: Vec<(u16, u64)>,
        reply: oneshot::Sender<Vec<Probe>>,
    },
    Fetch {
        source: WorkerId,
        generation: u64,
        probes: Vec<Probe>,
        limit: usize,
        reply: oneshot::Sender<Vec<u8>>,
    },
    Apply {
        permit: OwnedSemaphorePermit,
        source: WorkerId,
        generation: u64,
        packet: Packet,
        reply: oneshot::Sender<()>,
    },
}

pub(super) struct ActorHandle {
    pub clients: mpsc::Sender<ClientTurn>,
    pub control: mpsc::Sender<Control>,
    pub transfer: mpsc::Sender<Transfer>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Primary,
    Copy(WorkerId),
}

struct ActorReceivers {
    clients: mpsc::Receiver<ClientTurn>,
    control: mpsc::Receiver<Control>,
    transfer: mpsc::Receiver<Transfer>,
}

impl ActorHandle {
    fn channels(max_pending: usize) -> (Self, ActorReceivers) {
        let (clients, client_rx) = mpsc::channel(max_pending);
        let (control, control_rx) = mpsc::channel(CONTROL_PENDING);
        let (transfer, transfer_rx) = mpsc::channel(TRANSFER_PENDING);
        (
            Self {
                clients,
                control,
                transfer,
            },
            ActorReceivers {
                clients: client_rx,
                control: control_rx,
                transfer: transfer_rx,
            },
        )
    }
}

struct Shard {
    epoch: u64,
    role: Role,
    store: SlotStore,
    baseline: bool,
    sequence: u64,
    digest: Option<[u8; 32]>,
    lease: Option<Lease>,
    freeze_requested: bool,
    frozen: Option<Frozen>,
    operations: u64,
    write_operations: u64,
    expired: u64,
    evicted: u64,
    replica_prefix: Option<(WorkerId, u64)>,
    max_queue_us: u64,
}

impl Shard {
    fn new(epoch: u64, role: Role, baseline: bool, config: ActorConfig) -> Self {
        Self {
            epoch,
            role,
            store: SlotStore::new(config.slot_bytes, config.eviction),
            baseline,
            sequence: 0,
            digest: None,
            lease: None,
            freeze_requested: false,
            frozen: None,
            operations: 0,
            write_operations: 0,
            expired: 0,
            evicted: 0,
            replica_prefix: None,
            max_queue_us: 0,
        }
    }

    fn authorized(&self, now: ClockBounds) -> bool {
        self.write_eligible() && self.lease.is_some_and(|lease| lease_authorizes(lease, now))
    }

    #[inline]
    fn write_eligible(&self) -> bool {
        self.role == Role::Primary && self.baseline && !self.freeze_requested
    }

    fn freeze(&mut self, clock: &mut Clock) {
        if self.frozen.is_none()
            && self.baseline
            && self.freeze_requested
            && let Ok(now) = clock.now()
        {
            self.frozen = Some(Frozen {
                sequence: self.sequence,
                upper_ms: now.upper_ms,
                lease_expires_ms: self.lease.map_or(0, |lease| lease.expires_ms),
            });
        }
    }
}

#[inline]
pub(in crate::cache::cluster) fn lease_authorizes(lease: Lease, now: ClockBounds) -> bool {
    #[cfg(feature = "sim")]
    if hooks::knobs().lease_ignores_error_bound {
        return now.upper_ms >= lease.not_before_ms && now.lower_ms < lease.expires_ms;
    }
    now.fenced(lease.not_before_ms) && now.allows(lease.expires_ms)
}

struct State {
    config: ActorConfig,
    index: usize,
    clock: Clock,
    topology: Arc<Topology>,
    slots: BTreeMap<u16, Shard>,
    expiry_keys: BTreeSet<u16>,
    expiry_cursor: u16,
    journal: Journal,
    copies: Copies,
    storage: StoreBytes,
    abandoned_actor_replies: u64,
}

#[derive(Default)]
struct StoreBytes {
    primary: usize,
    replica: usize,
}

impl StoreBytes {
    fn ceiling(&self, config: &ActorConfig, primary: bool, current: usize) -> usize {
        let (class_limit, own, other) = if primary {
            (config.primary_bytes, self.primary, self.replica)
        } else {
            (config.replica_bytes, self.replica, self.primary)
        };
        // Retained bytes above a role's steady-state share reduce capacity
        // for the other role until expiry, deletion, or another relocation.
        let total_limit = config.primary_bytes + config.replica_bytes;
        class_limit
            .min(total_limit.saturating_sub(other))
            .saturating_sub(own - current)
            .max(current)
    }
}

pub(super) fn spawn(
    index: usize,
    config: ActorConfig,
) -> io::Result<(ActorHandle, thread::JoinHandle<()>)> {
    let (handle, receivers) = ActorHandle::channels(config.max_pending);
    let (startup, started) = std::sync::mpsc::sync_channel(1);
    let thread = task::spawn_service(format!("cache-actor-{index}"), move || {
        let state = State::new(index, config).and_then(|state| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            Ok((state, runtime))
        });
        match state {
            Ok((state, runtime)) => {
                if startup.send(Ok(())).is_ok() {
                    runtime.block_on(state.run(receivers));
                }
            }
            Err(error) => {
                let _ = startup.send(Err(error));
            }
        }
    })?;
    match started.recv() {
        Ok(Ok(())) => Ok((handle, thread)),
        result => {
            let _ = thread.join();
            match result {
                Ok(Err(error)) => Err(error),
                _ => Err(io::Error::other("cache actor initialization failed")),
            }
        }
    }
}

#[cfg(feature = "sim")]
pub(super) fn spawn_sim(index: usize, config: ActorConfig) -> io::Result<ActorHandle> {
    let state = State::new(index, config)?;
    let (handle, receivers) = ActorHandle::channels(config.max_pending);
    drop(task::spawn(state.run(receivers)));
    Ok(handle)
}

impl State {
    fn new(index: usize, config: ActorConfig) -> io::Result<Self> {
        Ok(Self {
            config,
            index,
            clock: Clock::new(config.clock)?,
            topology: Arc::new(Topology::default()),
            slots: BTreeMap::new(),
            expiry_keys: BTreeSet::new(),
            expiry_cursor: 0,
            journal: Journal::new(config.journal_bytes)?,
            copies: Copies::new(config.snapshot_bytes, config.slot_bytes),
            storage: StoreBytes::default(),
            abandoned_actor_replies: 0,
        })
    }

    async fn run(mut self, receivers: ActorReceivers) {
        let ActorReceivers {
            mut clients,
            mut control,
            mut transfer,
        } = receivers;
        let mut expiry = interval(Duration::from_millis(10));
        expiry.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut front = None;
        loop {
            if let Ok(message) = control.try_recv() {
                self.control(message, &mut clients, &mut front);
            }
            if let Some(turn) = front.take() {
                self.client(turn);
                continue;
            }
            tokio::select! {
                message = clients.recv() => {
                    let Some(message) = message else { break; };
                    self.client(message);
                }
                message = control.recv() => {
                    let Some(message) = message else { break; };
                    self.control(message, &mut clients, &mut front);
                }
                message = transfer.recv() => {
                    let Some(message) = message else { break; };
                    self.transfer(message);
                }
                _ = expiry.tick() => self.maintenance(),
            }
        }
    }

    fn client(&mut self, turn: ClientTurn) {
        if turn.reply.is_closed() {
            return;
        }
        let result = self.execute(
            turn.slot,
            turn.generation,
            turn.epoch,
            turn.queued,
            &turn.operation,
        );
        if turn.reply.send(result).is_err() {
            self.abandoned_actor_replies = self.abandoned_actor_replies.saturating_add(1);
        }
    }

    fn control(
        &mut self,
        message: Control,
        clients: &mut mpsc::Receiver<ClientTurn>,
        front: &mut Option<ClientTurn>,
    ) {
        match message {
            Control::Install {
                topology,
                grants,
                done,
            } => {
                self.install(topology, &grants);
                let _ = done.send(());
            }
            Control::Progress { telemetry, reply } => {
                if telemetry && front.is_none() {
                    // Peek exactly one queued request and execute it next.
                    // Admission remains held by the connection throughout.
                    *front = clients.try_recv().ok();
                }
                let _ = reply.send(self.progress(telemetry, front.as_ref(), clients.len()));
            }
            #[cfg(feature = "sim")]
            Control::Sample { slot, reply } => {
                let _ = reply.send(self.sample(slot));
            }
        }
    }

    #[cfg(feature = "sim")]
    fn sample(&self, slot: u16) -> Option<SlotSample> {
        let shard = self.slots.get(&slot)?;
        // Apply rollback and uncertainty checks without advancing the actor's
        // clock endpoints. Observations must not change later lease decisions.
        let now = self.clock.clone().now().ok();
        Some(SlotSample {
            generation: self.topology.generation,
            epoch: shard.epoch,
            sequence: shard.sequence,
            digest: shard.store.digest(0),
            authorized: now.is_some_and(|now| shard.authorized(now)),
            baseline: shard.baseline,
            lease: shard.lease,
        })
    }

    fn progress(
        &mut self,
        telemetry: bool,
        oldest: Option<&ClientTurn>,
        queued: usize,
    ) -> ActorReport {
        let mut slots = Vec::with_capacity(self.slots.len());
        let mut slot_stats = if telemetry {
            Vec::with_capacity(self.slots.len())
        } else {
            Vec::new()
        };
        let now = self.clock.now().ok();
        for (&slot, state) in &mut self.slots {
            state.freeze(&mut self.clock);
            slots.push(Progress {
                slot,
                epoch: state.epoch,
                sequence: state.sequence,
                baseline: state.baseline,
                serving_primary: now.is_some_and(|bounds| state.authorized(bounds)),
                frozen: state.frozen,
            });
            if telemetry {
                let placement = self.topology.slots[usize::from(slot)];
                let ack = state
                    .replica_prefix
                    .filter(|(worker, _)| placement.replica == Some(*worker))
                    .map(|(_, prefix)| prefix);
                slot_stats.push(SlotMetrics {
                    slot,
                    epoch: state.epoch,
                    operations: state.operations,
                    write_operations: state.write_operations,
                    expired: state.expired,
                    evicted: state.evicted,
                    max_queue_us: state.max_queue_us,
                    lease_headroom_ms: now.and_then(|now| {
                        state
                            .lease
                            .map(|lease| lease.expires_ms.saturating_sub(now.upper_ms))
                    }),
                    replica_acknowledged: ack,
                    replica_lag: ack.map(|prefix| state.sequence.saturating_sub(prefix)),
                });
            }
        }
        ActorReport {
            slots,
            generation: self.topology.generation,
            revision: self.topology.revision,
            primary_bytes: self.storage.primary,
            replica_bytes: self.storage.replica,
            snapshot_bytes: self.copies.charged_bytes(),
            journal_bytes: self.journal.charged_bytes(),
            missing_replica_baselines: self
                .slots
                .values()
                .filter(|slot| matches!(slot.role, Role::Copy(_)) && !slot.baseline)
                .count(),
            queued_turns: queued + usize::from(oldest.is_some()),
            oldest_queued_us: oldest.map(|turn| elapsed_us(turn.queued)),
            oldest_queued_slot: oldest.map(|turn| turn.slot),
            abandoned_actor_replies: self.abandoned_actor_replies,
            slot_stats,
        }
    }

    fn install(&mut self, topology: Arc<Topology>, grants: &[Option<Lease>]) {
        if topology.generation < self.topology.generation
            || topology.revision < self.topology.revision
        {
            return;
        }
        if topology.generation != self.topology.generation {
            self.slots.clear();
            self.expiry_keys.clear();
            self.journal.clear();
            self.copies.clear();
            self.storage.primary = 0;
            self.storage.replica = 0;
        }
        if !Arc::ptr_eq(&topology, &self.topology) {
            self.reconcile(&topology);
            self.topology = topology;
        }
        for (&slot, state) in &mut self.slots {
            if state.role == Role::Primary
                && !state.freeze_requested
                && let Some(grant) = grants[usize::from(slot)]
                && state
                    .lease
                    .is_none_or(|lease| grant.expires_ms >= lease.expires_ms)
            {
                state.lease = Some(grant);
            }
        }
        #[cfg(feature = "sim")]
        for slot in hooks::watched_slots() {
            if usize::from(slot) % self.config.count == self.index {
                let shard = self.slots.get(&slot);
                hooks::observe(Observed::Lease {
                    worker: self.config.id,
                    slot,
                    writable: shard.is_some_and(Shard::write_eligible),
                    epoch: shard.map_or(0, |state| state.epoch),
                    lease: shard.and_then(|state| state.lease),
                });
            }
        }
    }

    fn reconcile(&mut self, topology: &Topology) {
        self.storage.primary = 0;
        self.storage.replica = 0;
        let id = self.config.id;
        for (slot, placement) in topology
            .slots
            .iter()
            .enumerate()
            .skip(self.index)
            .step_by(self.config.count)
        {
            let slot_number = slot as u16;
            let role = if placement.primary == Some(id) {
                Some(Role::Primary)
            } else if placement.replica == Some(id)
                || placement.moving.is_some_and(|intent| intent.target == id)
            {
                placement.primary.map(Role::Copy)
            } else {
                None
            };
            let Some(role) = role else {
                self.slots.remove(&slot_number);
                self.expiry_keys.remove(&slot_number);
                self.copies.forget(slot_number);
                continue;
            };
            #[cfg(feature = "sim")]
            let old_primary = self
                .slots
                .get(&slot_number)
                .and_then(|state| (state.role == Role::Primary).then_some(state.epoch));
            let state = self.slots.entry(slot_number).or_insert_with(|| {
                Shard::new(
                    placement.epoch,
                    role,
                    placement.cold && role == Role::Primary,
                    self.config,
                )
            });
            if state.epoch != placement.epoch || state.role != role {
                let old_placement = self.topology.slots[slot];
                let replica_seed = matches!(role, Role::Copy(_))
                    && placement.replica == Some(id)
                    && placement.replica_seed.is_some_and(|seed| {
                        seed.previous_epoch == state.epoch
                            && seed.sequence == state.sequence
                            && match state.role {
                                Role::Copy(source) => source == seed.previous_primary,
                                Role::Primary => {
                                    id == seed.previous_primary && state.frozen.is_some()
                                }
                            }
                    });
                let retain = !placement.cold
                    && state.baseline
                    && (matches!((state.role, role), (Role::Copy(source), Role::Primary) if old_placement.primary == Some(source))
                        || state.role == Role::Primary && role == Role::Primary
                        || replica_seed);
                let mut replacement = Shard::new(
                    placement.epoch,
                    role,
                    placement.cold && role == Role::Primary,
                    self.config,
                );
                if retain {
                    std::mem::swap(&mut replacement.store, &mut state.store);
                    replacement.baseline = true;
                }
                *state = replacement;
                self.copies.forget(slot_number);
            }
            if role == Role::Primary {
                state.freeze_requested |= placement
                    .moving
                    .is_some_and(|intent| intent.freeze_requested);
                state.freeze(&mut self.clock);
                self.storage.primary += state.store.charged_bytes();
                #[cfg(feature = "sim")]
                if old_primary != Some(state.epoch) && state.baseline && hooks::watched(slot_number)
                {
                    observe_publication(self.config.id, topology.generation, slot_number, state);
                }
                if !state.store.is_empty() {
                    self.expiry_keys.insert(slot_number);
                }
            } else {
                self.storage.replica += state.store.charged_bytes();
            }
        }
    }

    fn execute(
        &mut self,
        slot: u16,
        generation: u64,
        epoch: u64,
        queued: Instant,
        operation: &Operation,
    ) -> Response {
        let Some(state) = self.slots.get_mut(&slot) else {
            return unavailable();
        };
        if generation != self.topology.generation
            || state.epoch != epoch
            || state.role != Role::Primary
        {
            return unavailable();
        }
        if state.freeze_requested {
            return frozen();
        }
        for _ in 0..2 {
            let Ok(now) = self.clock.now() else {
                return unavailable();
            };
            if !state.authorized(now) {
                return unavailable();
            }
            let turn = state
                .store
                .prepare(operation, now.upper_ms, self.config.turns);
            #[cfg(feature = "sim")]
            let sequence = state.sequence;
            let published = publish(
                &self.config,
                || self.clock.now(),
                &mut self.journal,
                &mut self.storage,
                slot,
                state,
                turn,
            );
            #[cfg(feature = "sim")]
            if sequence != state.sequence {
                observe_mutation(
                    &self.config,
                    self.topology.generation,
                    &self.storage,
                    slot,
                    state,
                );
            }
            match published {
                Published::Reply(reply) => {
                    #[cfg(feature = "sim")]
                    if !matches!(reply, Reply::Error(_)) {
                        hooks::observe(Observed::Served {
                            worker: self.config.id,
                            slot,
                            epoch: state.epoch,
                        });
                    }
                    state.operations = state.operations.saturating_add(1);
                    if matches!(
                        operation,
                        Operation::Set { .. }
                            | Operation::Delete(_)
                            | Operation::Expire { .. }
                            | Operation::Persist(_)
                            | Operation::Increment { .. }
                    ) {
                        state.write_operations = state.write_operations.saturating_add(1);
                    }
                    state.max_queue_us = state.max_queue_us.max(elapsed_us(queued));
                    if !state.store.is_empty() {
                        self.expiry_keys.insert(slot);
                    } else {
                        self.expiry_keys.remove(&slot);
                    }
                    return response(reply);
                }
                Published::Unavailable => return unavailable(),
                Published::Stale => {}
            }
        }
        response(Reply::Error(StoreError::StaleTurn))
    }

    fn maintenance(&mut self) {
        self.copies.expire();
        let expires = time::now_std() + Duration::from_micros(400);
        for _ in 0..EXPIRY_PER_TICK {
            let Some(slot) = self
                .expiry_keys
                .range(self.expiry_cursor..)
                .next()
                .or_else(|| self.expiry_keys.first())
                .copied()
            else {
                break;
            };
            self.expiry_cursor = slot + 1;
            let Some(state) = self.slots.get_mut(&slot) else {
                self.expiry_keys.remove(&slot);
                continue;
            };
            let Ok(now) = self.clock.now() else {
                break;
            };
            if state.authorized(now) {
                let turn = state
                    .store
                    .prepare_expiry(now.upper_ms, 32, Duration::from_micros(100));
                #[cfg(feature = "sim")]
                let sequence = state.sequence;
                let _ = publish(
                    &self.config,
                    || self.clock.now(),
                    &mut self.journal,
                    &mut self.storage,
                    slot,
                    state,
                    turn,
                );
                #[cfg(feature = "sim")]
                if sequence != state.sequence {
                    observe_mutation(
                        &self.config,
                        self.topology.generation,
                        &self.storage,
                        slot,
                        state,
                    );
                }
            }
            if state.store.is_empty() || state.role != Role::Primary {
                self.expiry_keys.remove(&slot);
            }
            if time::now_std() >= expires {
                break;
            }
        }
    }

    fn transfer(&mut self, message: Transfer) {
        match message {
            Transfer::Sample {
                source,
                generation,
                slots,
                reply,
            } => {
                let mut values = Vec::new();
                if generation == self.topology.generation {
                    for slot in self.copies.active() {
                        if let Some(state) = self
                            .slots
                            .get(&slot)
                            .filter(|state| state.role == Role::Copy(source))
                        {
                            values.push(self.copies.probe(slot, state));
                        }
                    }
                    for (slot, epoch) in slots {
                        if values.iter().any(|probe| probe.slot == slot) {
                            continue;
                        }
                        if let Some(state) = self.slots.get(&slot).filter(|state| {
                            state.role == Role::Copy(source) && state.epoch == epoch
                        }) {
                            values.push(self.copies.probe(slot, state));
                        }
                    }
                }
                let _ = reply.send(values);
            }
            Transfer::Fetch {
                source,
                generation,
                probes,
                limit,
                reply,
            } => {
                let value = if generation == self.topology.generation
                    && self.topology.member(source).is_some()
                {
                    self.fetch(source, &probes, limit)
                } else {
                    Vec::new()
                };
                let _ = reply.send(value);
            }
            Transfer::Apply {
                permit: _permit,
                source,
                generation,
                packet,
                reply,
            } => {
                if generation == self.topology.generation && self.topology.member(source).is_some()
                {
                    self.apply(source, packet);
                }
                let _ = reply.send(());
            }
        }
    }

    fn fetch(&mut self, destination: WorkerId, probes: &[Probe], limit: usize) -> Vec<u8> {
        let mut payload = Vec::new();
        for probe in probes {
            let Some(state) = self.slots.get_mut(&probe.slot) else {
                continue;
            };
            let placement = self.topology.slots[usize::from(probe.slot)];
            if state.role != Role::Primary
                || !state.baseline
                || state.epoch != probe.epoch
                || !(placement.replica == Some(destination)
                    || placement
                        .moving
                        .is_some_and(|movement| movement.target == destination))
            {
                continue;
            }
            if placement.replica == Some(destination)
                && probe.baseline
                && probe.sequence <= state.sequence
            {
                state.replica_prefix = Some((destination, probe.sequence));
            }
            let _ = self.copies.fetch(
                destination,
                probe,
                state,
                &self.journal,
                &mut payload,
                limit,
            );
            if limit - payload.len() < 128 {
                break;
            }
        }
        payload
    }

    fn apply(&mut self, source: WorkerId, packet: Packet) {
        let Some(state) = self.slots.get_mut(&packet.slot) else {
            return;
        };
        if state.role != Role::Copy(source) || state.epoch != packet.epoch {
            return;
        }
        #[cfg(feature = "sim")]
        let before = (state.baseline, state.sequence, packet.slot);
        let _ = self
            .copies
            .apply(&self.config, state, packet, &mut self.storage);
        #[cfg(feature = "sim")]
        if state.baseline && (!before.0 || state.sequence != before.1) {
            let slot = before.2;
            observe_charge(&self.config, &self.storage, slot, state);
            if hooks::watched(slot) {
                hooks::observe(Observed::ReplicaApplied {
                    worker: self.config.id,
                    source,
                    generation: self.topology.generation,
                    slot,
                    epoch: state.epoch,
                    sequence: state.sequence,
                    digest: state.store.digest(0),
                });
            }
        }
    }
}

#[cfg(feature = "sim")]
fn observe_mutation(
    config: &ActorConfig,
    generation: u64,
    storage: &StoreBytes,
    slot: u16,
    shard: &Shard,
) {
    observe_charge(config, storage, slot, shard);
    if hooks::watched(slot) {
        observe_publication(config.id, generation, slot, shard);
    }
}

#[cfg(feature = "sim")]
fn observe_publication(worker: WorkerId, generation: u64, slot: u16, shard: &Shard) {
    hooks::observe(Observed::Published {
        worker,
        generation,
        slot,
        epoch: shard.epoch,
        sequence: shard.sequence,
        digest: shard.store.digest(0),
    });
}

#[cfg(feature = "sim")]
fn observe_charge(config: &ActorConfig, storage: &StoreBytes, slot: u16, shard: &Shard) {
    hooks::observe(Observed::Charged {
        worker: config.id,
        slot,
        slot_bytes: shard.store.charged_bytes(),
        slot_limit: config.slot_bytes,
        actor_bytes: storage.primary + storage.replica,
        actor_limit: config.primary_bytes + config.replica_bytes,
    });
}

enum Published {
    Reply(Reply),
    Stale,
    Unavailable,
}

fn publish(
    config: &ActorConfig,
    sample_clock: impl FnOnce() -> io::Result<ClockBounds>,
    journal: &mut Journal,
    storage: &mut StoreBytes,
    slot: u16,
    state: &mut Shard,
    mut turn: Prepared,
) -> Published {
    let current = state.store.charged_bytes();
    let max = storage.ceiling(config, true, current);
    let effect_bytes = turn.effect_bytes();
    let reserved = if turn.charge_after() > max
        || !turn.effects().is_empty()
            && (state.sequence == u64::MAX || journal.reserve_append(effect_bytes).is_err())
    {
        Err(StoreError::OutOfMemory)
    } else {
        state.store.reserve(&mut turn)
    };
    let Ok(now) = sample_clock() else {
        return Published::Unavailable;
    };
    if !state.authorized(now) {
        return Published::Unavailable;
    }
    if let Err(error) = reserved {
        return Published::Reply(Reply::Error(error));
    }
    match turn.refresh_time(now.upper_ms) {
        Ok(()) => {}
        Err(StoreError::StaleTurn) => return Published::Stale,
        Err(error) => return Published::Reply(Reply::Error(error)),
    }
    let removals = turn.removals();
    let (reply, effects) = state.store.commit_with_effects(turn);
    if !matches!(reply, Reply::Error(_)) {
        state.expired = state.expired.saturating_add(removals.expired);
        state.evicted = state.evicted.saturating_add(removals.evicted);
    }
    storage.primary = storage.primary - current + state.store.charged_bytes();
    if !effects.is_empty() {
        state.sequence += 1;
        journal.append(slot, state.epoch, state.sequence, effects, effect_bytes);
    }
    Published::Reply(reply)
}

fn elapsed_us(queued: Instant) -> u64 {
    time::now_std()
        .duration_since(queued)
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "sim"))]
pub(in crate::cache::cluster) use tests::LeaseActorFixture;
