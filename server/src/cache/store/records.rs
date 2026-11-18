use super::*;

type Index = HashMap<Bytes, usize, RandomState>;

// Each owning key/value may have a shared Bytes control block and padding.
// Bodies are exact-sized allocations supplied by the wire reader.
const BODY_OVERHEAD: usize = 64;

#[derive(Default)]
pub(super) struct Records {
    index: Index,
    entries: Vec<Record>,
    expiry: Vec<usize>,
    body_bytes: usize,
}

pub(super) struct Record {
    key: Bytes,
    pub value: Bytes,
    pub deadline_ms: Option<u64>,
    expiry_index: Option<usize>,
    last_touch: u64,
}

impl Record {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.deadline_ms.is_some_and(|deadline| now_ms >= deadline)
    }
}

#[derive(Default)]
pub(super) struct Layout {
    charge_after: usize,
    reservation: usize,
    body_bytes: usize,
    index_target: usize,
    index: Option<Index>,
    entry_target: usize,
    entries: Option<Vec<Record>>,
    expiry_target: usize,
    expiry: Option<Vec<usize>>,
}

impl Layout {
    pub fn unchanged(charge_after: usize) -> Self {
        Self {
            charge_after,
            ..Self::default()
        }
    }

    pub fn charge_after(&self) -> usize {
        self.charge_after
    }

    pub fn reservation_bytes(&self) -> usize {
        self.reservation
    }

    pub fn reserve(&mut self, records: &Records) -> Result<(), StoreError> {
        if self.index_target > 0 && self.index.is_none() {
            let mut index = HashMap::with_hasher(records.index.hasher().clone());
            index
                .try_reserve(self.index_target)
                .map_err(|_| StoreError::OutOfMemory)?;
            debug_assert!(index.allocation_size() <= index_bytes(self.index_target).unwrap());
            self.index = Some(index);
        }
        reserve_array(self.entry_target, &mut self.entries)?;
        reserve_array(self.expiry_target, &mut self.expiry)
    }
}

fn reserve_array<T>(target: usize, buffer: &mut Option<Vec<T>>) -> Result<(), StoreError> {
    if target != 0 && buffer.is_none() {
        let mut vector = Vec::new();
        vector
            .try_reserve_exact(target)
            .map_err(|_| StoreError::OutOfMemory)?;
        *buffer = Some(vector);
    }
    Ok(())
}

fn capacity_target(current: usize, needed: usize, geometric: bool) -> usize {
    if needed <= current {
        return 0;
    }
    if geometric {
        needed.max(current.saturating_mul(2)).max(4)
    } else {
        needed
    }
}

/// Bound hashbrown 0.17 bucket and control storage before allocating. Charged
/// live bytes use `allocation_size()`; this bound includes alignment padding.
fn index_bytes(entries: usize) -> Option<usize> {
    if entries == 0 {
        return Some(0);
    }
    let buckets = match entries {
        1..=3 => 4,
        4..=7 => 8,
        8..=14 => 16,
        _ => (entries.checked_mul(8)? / 7).checked_next_power_of_two()?,
    };
    buckets
        .checked_mul(size_of::<(Bytes, usize)>() + 1)?
        .checked_add(64)
}

fn body_bytes(key: &Bytes, value: &Bytes) -> usize {
    BODY_OVERHEAD + key.len() + value.len()
}

impl Records {
    pub fn charged_bytes(&self) -> usize {
        self.body_bytes
            + self.index.allocation_size()
            + self.entries.capacity() * size_of::<Record>()
            + self.expiry.capacity() * size_of::<usize>()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, key: &Bytes) -> Option<(usize, &Record)> {
        let position = *self.index.get(key)?;
        Some((position, &self.entries[position]))
    }

    pub fn touch(&mut self, position: usize, access: u64) {
        self.entries[position].last_touch = access;
    }

    pub fn layout(
        &self,
        effects: &[Effect],
        byte_limit: usize,
        growth_bytes: usize,
    ) -> Option<Layout> {
        if effects.is_empty() {
            return Some(Layout::unchanged(self.charged_bytes()));
        }

        let mut body_after = self.body_bytes;
        let mut entry_after = self.entries.len();
        let mut ttl_after = self.expiry.len();
        let mut added_keys = 0;
        let mut added_ttls = 0;

        for effect in effects {
            let old = self.get(effect.key()).map(|(_, entry)| entry);
            let had_ttl = old.is_some_and(|entry| entry.deadline_ms.is_some());
            if let Some(entry) = old {
                body_after -= body_bytes(&entry.key, &entry.value);
            }
            match effect {
                Effect::Upsert {
                    key,
                    value,
                    deadline_ms,
                } => {
                    body_after = body_after.checked_add(body_bytes(key, value))?;
                    if old.is_none() {
                        entry_after += 1;
                        added_keys += 1;
                    }
                    added_ttls += usize::from(deadline_ms.is_some() && !had_ttl);
                }
                Effect::Delete(_) => {
                    entry_after -= usize::from(old.is_some());
                    ttl_after -= usize::from(had_ttl);
                }
            }
        }

        if entry_after == 0 {
            return Some(Layout::default());
        }

        let needs_index = added_keys > self.index.capacity() - self.index.len();
        // A turn applies deletes before upserts, but upserts can add a
        // deadline before removing another one. Reserve the maximum prefix.
        let ttl_peak = ttl_after.checked_add(added_ttls)?;

        [true, false].into_iter().find_map(|geometric| {
            let index_target = usize::from(needs_index) * entry_after;
            let entry_target = capacity_target(self.entries.capacity(), entry_after, geometric);
            let expiry_target = capacity_target(self.expiry.capacity(), ttl_peak, geometric);
            let new_index_bytes = index_bytes(index_target)?;
            let new_entry_bytes = entry_target.checked_mul(size_of::<Record>())?;
            let new_expiry_bytes = expiry_target.checked_mul(size_of::<usize>())?;

            let charge_after = [
                body_after,
                if needs_index {
                    new_index_bytes
                } else {
                    self.index.allocation_size()
                },
                self.entries
                    .capacity()
                    .max(entry_target)
                    .checked_mul(size_of::<Record>())?,
                self.expiry
                    .capacity()
                    .max(expiry_target)
                    .checked_mul(size_of::<usize>())?,
            ]
            .into_iter()
            .try_fold(0usize, usize::checked_add)?;
            let reservation = [
                body_after.saturating_sub(self.body_bytes),
                new_index_bytes,
                new_entry_bytes,
                new_expiry_bytes,
            ]
            .into_iter()
            .try_fold(0usize, usize::checked_add)?;
            (charge_after <= byte_limit && reservation <= growth_bytes).then_some(Layout {
                charge_after,
                reservation,
                body_bytes: body_after,
                index_target,
                entry_target,
                expiry_target,
                ..Layout::default()
            })
        })
    }

    pub fn apply(&mut self, effects: &[Effect], mut layout: Layout, access: u64) {
        if effects.is_empty() {
            return;
        }
        for effect in effects {
            if let Effect::Delete(key) = effect {
                self.remove(key);
            }
        }

        if let Some(mut replacement) = layout.index.take() {
            replacement.extend(self.index.drain());
            self.index = replacement;
        }
        if let Some(mut replacement) = layout.entries.take() {
            replacement.append(&mut self.entries);
            self.entries = replacement;
        }
        if let Some(mut replacement) = layout.expiry.take() {
            replacement.append(&mut self.expiry);
            self.expiry = replacement;
        }
        for effect in effects {
            if let Effect::Upsert {
                key,
                value,
                deadline_ms,
            } = effect
            {
                self.upsert(key, value, *deadline_ms, access);
            }
        }
        self.body_bytes = layout.body_bytes;
        if self.entries.is_empty() {
            // Retire all three allocations when a slot becomes empty.
            *self = Self::default();
        }
        debug_assert!(self.charged_bytes() <= layout.charge_after);
    }

    fn upsert(&mut self, key: &Bytes, value: &Bytes, deadline_ms: Option<u64>, access: u64) {
        let position = if let Some(&position) = self.index.get(key) {
            let record = &mut self.entries[position];
            record.value = value.clone();
            record.last_touch = access;
            position
        } else {
            let position = self.entries.len();
            debug_assert!(position < self.entries.capacity());
            debug_assert!(self.index.len() < self.index.capacity());
            self.index.insert(key.clone(), position);
            self.entries.push(Record {
                key: key.clone(),
                value: value.clone(),
                deadline_ms: None,
                expiry_index: None,
                last_touch: access,
            });
            position
        };
        self.set_deadline(position, deadline_ms);
    }

    fn remove(&mut self, key: &Bytes) {
        let Some(position) = self.index.remove(key) else {
            return;
        };
        self.set_deadline(position, None);
        self.entries.swap_remove(position);
        if let Some(moved) = self.entries.get(position) {
            *self
                .index
                .get_mut(&moved.key)
                .expect("record has index entry") = position;
            if let Some(expiry_index) = moved.expiry_index {
                self.expiry[expiry_index] = position;
            }
        }
    }

    fn set_deadline(&mut self, position: usize, deadline_ms: Option<u64>) {
        let old = self.entries[position].deadline_ms;
        if deadline_ms == old {
            return;
        }
        let existing = self.entries[position].expiry_index;
        if let Some(deadline) = deadline_ms {
            self.entries[position].deadline_ms = Some(deadline);
            match existing {
                Some(index) => self.restore_heap(index),
                None => {
                    let index = self.expiry.len();
                    debug_assert!(index < self.expiry.capacity());
                    self.entries[position].expiry_index = Some(index);
                    self.expiry.push(position);
                    self.sift_up(index);
                }
            }
        } else {
            if let Some(index) = existing {
                self.expiry.swap_remove(index);
                if let Some(&moved) = self.expiry.get(index) {
                    self.entries[moved].expiry_index = Some(index);
                    self.restore_heap(index);
                }
            }
            self.entries[position].deadline_ms = None;
            self.entries[position].expiry_index = None;
        }
    }

    fn deadline(&self, expiry_index: usize) -> u64 {
        self.entries[self.expiry[expiry_index]]
            .deadline_ms
            .expect("indexed record has deadline")
    }

    fn swap_expiry(&mut self, a: usize, b: usize) {
        self.expiry.swap(a, b);
        self.entries[self.expiry[a]].expiry_index = Some(a);
        self.entries[self.expiry[b]].expiry_index = Some(b);
    }

    fn restore_heap(&mut self, index: usize) {
        if self.sift_up(index) == index {
            self.sift_down(index);
        }
    }

    fn sift_up(&mut self, mut index: usize) -> usize {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.deadline(parent) <= self.deadline(index) {
                break;
            }
            self.swap_expiry(index, parent);
            index = parent;
        }
        index
    }

    fn sift_down(&mut self, mut index: usize) {
        while index < self.expiry.len() / 2 {
            let left = 2 * index + 1;
            let right = left + 1;
            let child = if right < self.expiry.len() && self.deadline(right) < self.deadline(left) {
                right
            } else {
                left
            };
            if self.deadline(index) <= self.deadline(child) {
                break;
            }
            self.swap_expiry(index, child);
            index = child;
        }
    }

    pub fn expired(
        &self,
        now_ms: u64,
        max_items: usize,
        time: Duration,
        mut visit: impl FnMut(&Bytes),
    ) {
        let start = Instant::now();
        let mut stack = SmallVec::<[usize; 32]>::new();
        if !self.expiry.is_empty() {
            stack.push(0);
        }
        let mut found = 0;
        while let Some(index) = stack.pop() {
            if found >= max_items || start.elapsed() >= time {
                break;
            }
            if self.deadline(index) > now_ms {
                continue;
            }
            visit(&self.entries[self.expiry[index]].key);
            found += 1;
            let left = 2 * index + 1;
            let right = left + 1;
            if right < self.expiry.len() {
                stack.push(right);
            }
            if left < self.expiry.len() {
                stack.push(left);
            }
        }
    }

    pub fn sample_oldest(
        &self,
        seed: u64,
        except: &Bytes,
        now_ms: u64,
    ) -> SmallVec<[Bytes; EVICTION_SAMPLES]> {
        let mut candidates = SmallVec::<[usize; EVICTION_SAMPLES]>::new();
        let len = self.entries.len();
        if len <= EVICTION_SAMPLES {
            candidates.extend(0..len);
        } else {
            let mut randomness = seed ^ now_ms;
            for _ in 0..EVICTION_SAMPLES {
                let candidate = (mix_next(&mut randomness) as usize) % len;
                if !candidates.contains(&candidate) {
                    candidates.push(candidate);
                }
            }
        }
        candidates.retain(|position| self.entries[*position].key != *except);
        candidates.sort_unstable_by_key(|&position| {
            let entry = &self.entries[position];
            (!entry.is_expired(now_ms), entry.last_touch)
        });
        candidates
            .into_iter()
            .map(|position| self.entries[position].key.clone())
            .collect()
    }

    pub fn pin_snapshot(
        &self,
        byte_limit: usize,
        item_limit: usize,
        time: Duration,
    ) -> Result<PinnedSnapshot, StoreError> {
        let start = Instant::now();
        let len = self.entries.len();
        let charge = self
            .body_bytes
            .checked_add(
                len.checked_mul(size_of::<Effect>())
                    .ok_or(StoreError::SnapshotLimit)?,
            )
            .filter(|charge| *charge <= byte_limit && len <= item_limit)
            .ok_or(StoreError::SnapshotLimit)?;
        let mut effects = Vec::new();
        effects
            .try_reserve_exact(len)
            .map_err(|_| StoreError::OutOfMemory)?;
        for entry in &self.entries {
            if start.elapsed() >= time {
                return Err(StoreError::SnapshotLimit);
            }
            effects.push(Effect::Upsert {
                key: entry.key.clone(),
                value: entry.value.clone(),
                deadline_ms: entry.deadline_ms,
            });
        }
        Ok(PinnedSnapshot { effects, charge })
    }
}

fn mix_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
