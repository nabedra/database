use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use nabedra_rt::time;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cache::store::{Effects, PinnedSnapshot, Reply, SlotStore, StoreError};

use super::super::peer::{self, CHUNK_BYTES, Chunk, Data, MAX_ITEMS, Packet, Probe, Resume};
use super::{ActorConfig, Journal, Shard, StoreBytes, WorkerId};

const MAX_TRANSFERS: usize = 4;
const PIN_TIME: Duration = Duration::from_millis(6);
const TRANSFER_TTL: Duration = Duration::from_secs(4);
const TRANSFER_STATE_BYTES: usize = 256;

type Destination = (u16, WorkerId);

pub(super) struct Copies {
    per_direction: usize,
    max_items: usize,
    outgoing: BTreeMap<Destination, Outgoing>,
    incoming: BTreeMap<u16, Incoming>,
}

struct Outgoing {
    id: Uuid,
    cut: u64,
    next: u32,
    until: Instant,
    image: PinnedSnapshot,
    digest: Sha256,
}

struct Incoming {
    id: Uuid,
    cut: u64,
    next: u32,
    until: Instant,
    store: SlotStore,
    digest: Sha256,
}

impl Copies {
    pub fn supports(slot_bytes: usize, limit: usize) -> bool {
        let (pin, items) = SlotStore::snapshot_requirements(slot_bytes);
        items <= u32::MAX as usize
            && pin.max(slot_bytes).saturating_add(TRANSFER_STATE_BYTES) <= limit / 2
    }

    pub fn new(limit: usize, slot_bytes: usize) -> Self {
        let (_, max_items) = SlotStore::snapshot_requirements(slot_bytes);
        Self {
            per_direction: limit / 2,
            max_items,
            outgoing: BTreeMap::new(),
            incoming: BTreeMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.outgoing.clear();
        self.incoming.clear();
    }

    pub fn forget(&mut self, slot: u16) {
        self.outgoing.retain(|(id, _), _| *id != slot);
        self.incoming.remove(&slot);
    }

    pub fn expire(&mut self) {
        let now = time::now_std();
        self.outgoing.retain(|_, copy| copy.until > now);
        self.incoming.retain(|_, copy| copy.until > now);
    }

    pub fn charged_bytes(&self) -> usize {
        self.outgoing_bytes() + self.incoming_bytes()
    }

    fn outgoing_bytes(&self) -> usize {
        self.outgoing
            .values()
            .map(|copy| copy.image.charged_bytes() + TRANSFER_STATE_BYTES)
            .sum()
    }

    fn incoming_bytes(&self) -> usize {
        self.incoming
            .values()
            .map(|copy| copy.store.charged_bytes() + TRANSFER_STATE_BYTES)
            .sum()
    }

    pub fn active(&self) -> impl Iterator<Item = u16> + '_ {
        self.incoming.keys().copied()
    }

    pub fn probe(&self, slot: u16, state: &Shard) -> Probe {
        Probe {
            slot,
            epoch: state.epoch,
            sequence: state.sequence,
            baseline: state.baseline,
            resume: self.incoming.get(&slot).map(|copy| Resume {
                id: copy.id,
                offset: copy.next,
            }),
        }
    }

    pub fn fetch(
        &mut self,
        destination: WorkerId,
        probe: &Probe,
        source: &Shard,
        journal: &Journal,
        output: &mut Vec<u8>,
        limit: usize,
    ) -> Result<(), StoreError> {
        let transfer = (probe.slot, destination);
        if probe.sequence > source.sequence {
            return Err(StoreError::InvalidEffects);
        }
        if probe.resume.is_none() && probe.baseline {
            if probe.sequence == source.sequence {
                self.outgoing.remove(&transfer);
                return Ok(());
            }
            let first = probe.sequence + 1;
            if journal.get(probe.slot, probe.epoch, first).is_some() {
                self.outgoing.remove(&transfer);
                for step in 0..peer::DELTAS_PER_PROBE as u64 {
                    let Some(sequence) = first.checked_add(step) else {
                        break;
                    };
                    let Some(effects) = journal.get(probe.slot, probe.epoch, sequence) else {
                        break;
                    };
                    if let Err(error) = peer::encode_delta(probe, sequence, effects, output, limit)
                    {
                        return if step == 0 { Err(error) } else { Ok(()) };
                    }
                }
                return Ok(());
            }
        }
        if source.store.is_empty()
            && (probe.resume.is_none() || !self.outgoing.contains_key(&transfer))
        {
            self.outgoing.remove(&transfer);
            return peer::encode_empty(probe, source.sequence, output, limit);
        }

        let resume_valid = self.outgoing.get(&transfer).is_some_and(|pin| {
            pin.until > time::now_std()
                && probe
                    .resume
                    .is_some_and(|resume| resume.id == pin.id && resume.offset == pin.next)
                && (pin.cut == source.sequence
                    || journal.get(probe.slot, probe.epoch, pin.cut + 1).is_some())
        });
        if !resume_valid {
            self.outgoing.remove(&transfer);
            if self.outgoing.len() >= MAX_TRANSFERS {
                return Err(StoreError::SnapshotLimit);
            }
            let remaining = self
                .per_direction
                .saturating_sub(self.outgoing_bytes())
                .saturating_sub(TRANSFER_STATE_BYTES);
            let image = source
                .store
                .pin_snapshot(remaining, self.max_items, PIN_TIME)?;
            self.outgoing.insert(
                transfer,
                Outgoing {
                    id: nabedra_rt::ids::uuid_v4(),
                    cut: source.sequence,
                    next: 0,
                    until: time::now_std() + TRANSFER_TTL,
                    image,
                    digest: Sha256::new(),
                },
            );
        }
        let pin = self.outgoing.get_mut(&transfer).expect("pinned transfer");
        let remaining = limit
            .saturating_sub(output.len())
            .saturating_sub(128)
            .min(CHUNK_BYTES);
        let chunk = pin.image.chunk(pin.next as usize, MAX_ITEMS, remaining)?;
        let next = pin
            .next
            .checked_add(chunk.len() as u32)
            .ok_or(StoreError::SnapshotLimit)?;
        let complete = next as usize == pin.image.effects().len();
        let mut digest = pin.digest.clone();
        peer::hash_effects(chunk, &mut digest);
        let checksum = complete.then(|| digest.clone().finalize().into());
        peer::encode_snapshot(
            probe, pin.id, pin.cut, pin.next, chunk, checksum, output, limit,
        )?;
        pin.next = next;
        pin.digest = digest;
        if complete {
            self.outgoing.remove(&transfer);
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        config: &ActorConfig,
        state: &mut Shard,
        packet: Packet,
        storage: &mut StoreBytes,
    ) -> Result<(), StoreError> {
        let slot = packet.slot;
        let old_size = state.store.charged_bytes();
        let available = storage.ceiling(config, false, old_size);
        match packet.data {
            Data::Empty(cut)
                if cut >= state.sequence && (!state.baseline || cut > state.sequence) =>
            {
                self.incoming.remove(&slot);
                state.store = SlotStore::new(config.slot_bytes, config.eviction);
                state.sequence = cut;
                state.baseline = true;
                state.digest = None;
            }
            Data::Delta {
                sequence,
                effects: _,
                digest,
            } if state.baseline && sequence == state.sequence && state.digest == Some(digest) => {}
            Data::Delta {
                sequence,
                effects,
                digest,
            } if state.baseline && state.sequence.checked_add(1) == Some(sequence) => {
                #[cfg(feature = "sim")]
                let effects = {
                    let mut effects = effects;
                    if super::hooks::knobs().replica_keeps_deleted_keys {
                        effects.retain(|effect| {
                            !matches!(effect, crate::cache::store::Effect::Delete(_))
                        });
                    }
                    effects
                };
                merge(
                    &mut state.store,
                    effects,
                    available,
                    config.turns.growth_bytes,
                )?;
                self.incoming.remove(&slot);
                state.sequence = sequence;
                state.digest = Some(digest);
            }
            Data::Snapshot(chunk) if !state.baseline || chunk.cut >= state.sequence => {
                if let Some((store, cut)) = self.receive(config, slot, chunk, available)? {
                    state.store = store;
                    state.sequence = cut;
                    state.baseline = true;
                    state.digest = None;
                }
            }
            _ => return Err(StoreError::InvalidEffects),
        }
        storage.replica = storage.replica - old_size + state.store.charged_bytes();
        Ok(())
    }

    fn receive(
        &mut self,
        config: &ActorConfig,
        slot: u16,
        chunk: Chunk,
        available: usize,
    ) -> Result<Option<(SlotStore, u64)>, StoreError> {
        if chunk.offset == 0
            && self
                .incoming
                .get(&slot)
                .is_none_or(|old| old.id != chunk.id)
        {
            self.incoming.remove(&slot);
            if self.incoming.len() == MAX_TRANSFERS
                || self.incoming_bytes() + TRANSFER_STATE_BYTES > self.per_direction
            {
                return Err(StoreError::SnapshotLimit);
            }
            self.incoming.insert(
                slot,
                Incoming {
                    id: chunk.id,
                    cut: chunk.cut,
                    next: 0,
                    until: time::now_std() + TRANSFER_TTL,
                    store: SlotStore::new(config.slot_bytes, config.eviction),
                    digest: Sha256::new(),
                },
            );
        }

        let total = self.incoming_bytes();
        let Some(transfer) = self.incoming.get_mut(&slot) else {
            return Err(StoreError::InvalidEffects);
        };
        if transfer.id != chunk.id
            || transfer.cut != chunk.cut
            || transfer.next != chunk.offset
            || time::now_std() >= transfer.until
        {
            return Err(StoreError::InvalidEffects);
        }
        let staged = transfer.store.charged_bytes();
        let cap = self
            .per_direction
            .saturating_sub(total - staged)
            .min(available);
        let items = chunk.effects.len() as u32;
        let next = transfer
            .next
            .checked_add(items)
            .filter(|count| *count as usize <= self.max_items)
            .ok_or(StoreError::SnapshotLimit)?;
        peer::hash_effects(&chunk.effects, &mut transfer.digest);
        let expected_len = transfer.store.len() + items as usize;
        if let Err(error) = merge(
            &mut transfer.store,
            chunk.effects,
            cap,
            config.turns.growth_bytes,
        ) {
            self.incoming.remove(&slot);
            return Err(error);
        }
        if transfer.store.len() != expected_len {
            self.incoming.remove(&slot);
            return Err(StoreError::InvalidEffects);
        }
        transfer.next = next;
        if let Some(checksum) = chunk.checksum {
            let transfer = self.incoming.remove(&slot).expect("completed transfer");
            let digest: [u8; 32] = transfer.digest.finalize().into();
            if digest != checksum {
                return Err(StoreError::InvalidEffects);
            }
            return Ok(Some((transfer.store, transfer.cut)));
        }
        Ok(None)
    }
}

fn merge(
    store: &mut SlotStore,
    effects: Effects,
    limit: usize,
    growth: usize,
) -> Result<(), StoreError> {
    let turn = store.prepare_replica(effects, growth)?;
    if turn.charge_after() > limit {
        return Err(StoreError::OutOfMemory);
    }
    match store.commit(turn) {
        Reply::Error(error) => Err(error),
        _ => Ok(()),
    }
}
