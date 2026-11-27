//! The row map: slot → key, generation and versions. A version knows when it was applied, and its
//! successor says when it was superseded. A version counts the entries, copies in flight and queued
//! appends that name it, and a slot is reused only when nothing does.
//!
//! With a vector store, a version's vector moves there when the version is added, and is released
//! when the version goes, so the store reuses its place under the same rule as the slot.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use super::mutant::mutant;
use crate::store::{VecKey, Vectors};
use crate::types::{CommitTs, Key, Pos, Result, Row};

pub type Slot = u32;
/// A generation of one range's index. A resync makes the older one, and every copy of it, void.
pub type Gen = u64;

/// What one scan sees of one range: the entries at or below an applied position and, for a
/// `snapshot` query, at or below a commit timestamp.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Point {
    pub pos: Pos,
    pub cut: CommitTs,
}

#[derive(Clone)]
pub struct Version {
    pub at: Pos,
    pub ts: CommitTs,
    /// `None` is a delete or a row without a vector: it only supersedes.
    pub row: Option<Arc<Row>>,
    refs: u32,
}

impl Version {
    fn within(&self, p: Point) -> bool {
        self.at <= p.pos && self.ts <= p.cut
    }
}

#[derive(Clone)]
struct SlotData {
    generation: Gen,
    key: Key,
    /// In log order.
    versions: Vec<Version>,
    /// The generation is void.
    dead: bool,
}

#[derive(Clone, Default)]
pub struct Rows {
    slots: Vec<Option<SlotData>>,
    free: Vec<Slot>,
    /// Where vectors live when not in the rows. A row there has `vec: None`.
    vectors: Option<Arc<dyn Vectors>>,
    /// Generations with a vector the store could not read.
    damage: Arc<Mutex<BTreeSet<Gen>>>,
    /// Some slot ever held two versions. Until then no version has a successor.
    superseded: bool,
}

impl Rows {
    pub fn new(vectors: Option<Arc<dyn Vectors>>) -> Rows {
        Rows {
            vectors,
            ..Rows::default()
        }
    }

    /// Moves the vector of a version into the store, if there is one. An error leaves it here.
    pub fn store_vector(&mut self, slot: Slot, at: Pos) -> Result<()> {
        let Some(store) = self.vectors.clone() else {
            return Ok(());
        };
        let Some(v) = self.version_mut(slot, at) else {
            return Ok(());
        };
        let Some((row, vec)) = v.row.as_ref().and_then(|r| Some((r, r.vec.clone()?))) else {
            return Ok(());
        };
        store.put(VecKey { slot, at }, &vec)?;
        let (cols, image) = (row.cols.clone(), row.image.clone());
        v.row = Some(Arc::new(Row {
            vec: None,
            cols,
            image,
        }));
        Ok(())
    }

    /// The vector of a version with a row. A store that cannot read it logs the damage.
    pub fn vector(&self, slot: Slot, at: Pos) -> Result<Option<Arc<[f32]>>> {
        let Some(row) = self.version(slot, at).and_then(|v| v.row.as_ref()) else {
            return Ok(None);
        };
        let (Some(store), None) = (&self.vectors, &row.vec) else {
            return Ok(row.vec.clone());
        };
        let got = store.get(VecKey { slot, at });
        if got.is_err()
            && let Some(generation) = self.generation(slot)
        {
            let mut damage = self.damage.lock().unwrap_or_else(PoisonError::into_inner);
            damage.insert(generation);
        }
        got.map(Some)
    }

    /// [`Rows::vector`] of several versions, with one batch of store reads.
    pub fn vectors(&self, keys: &[(Slot, Pos)]) -> Vec<Result<Option<Arc<[f32]>>>> {
        let Some(store) = &self.vectors else {
            return keys.iter().map(|&(s, at)| self.vector(s, at)).collect();
        };
        let (mut out, mut ask, mut from) = (Vec::with_capacity(keys.len()), Vec::new(), Vec::new());
        for (i, &(slot, at)) in keys.iter().enumerate() {
            let row = self.version(slot, at).and_then(|v| v.row.as_ref());
            out.push(Ok(row.and_then(|r| r.vec.clone())));
            if row.is_some_and(|r| r.vec.is_none()) {
                ask.push(VecKey { slot, at });
                from.push(i);
            }
        }
        for ((i, key), got) in from.into_iter().zip(&ask).zip(store.get_many(&ask)) {
            if got.is_err()
                && let Some(generation) = self.generation(key.slot)
            {
                self.log_damage(generation);
            }
            out[i] = got.map(Some);
        }
        out
    }

    /// The row of a version with the vector already read during scoring.
    pub fn full_row(&self, slot: Slot, at: Pos, vector: Arc<[f32]>) -> Option<Arc<Row>> {
        let row = self.version(slot, at)?.row.as_ref()?;
        if row.vec.is_some() {
            return Some(row.clone());
        }
        Some(Arc::new(Row {
            vec: Some(vector),
            cols: row.cols.clone(),
            image: row.image.clone(),
        }))
    }

    /// Whether reading a vector costs I/O.
    pub fn on_disk(&self) -> bool {
        self.vectors.as_ref().is_some_and(|v| v.on_disk())
    }

    /// Logs that the store failed a version of a generation, so that its range fails.
    pub fn log_damage(&self, generation: Gen) {
        let mut damage = self.damage.lock().unwrap_or_else(PoisonError::into_inner);
        damage.insert(generation);
    }

    /// The generations logged as damaged since the last call.
    pub fn take_damage(&self) -> Vec<Gen> {
        let mut damage = self.damage.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *damage).into_iter().collect()
    }

    fn data(&self, slot: Slot) -> Option<&SlotData> {
        self.slots.get(slot as usize)?.as_ref()
    }

    fn data_mut(&mut self, slot: Slot) -> Option<&mut SlotData> {
        self.slots.get_mut(slot as usize)?.as_mut()
    }

    pub fn version(&self, slot: Slot, at: Pos) -> Option<&Version> {
        let v = &self.data(slot)?.versions;
        v.get(v.binary_search_by_key(&at, |v| v.at).ok()?)
    }

    fn version_mut(&mut self, slot: Slot, at: Pos) -> Option<&mut Version> {
        let v = &mut self.data_mut(slot)?.versions;
        let i = v.binary_search_by_key(&at, |v| v.at).ok()?;
        v.get_mut(i)
    }

    pub fn key(&self, slot: Slot) -> Option<&Key> {
        Some(&self.data(slot)?.key)
    }

    /// Live slots, their versions, and the raw vectors kept in RAM, as (slots, versions, values).
    pub fn census_counts(&self) -> (usize, usize, usize) {
        let live = self.slots.iter().flatten();
        live.fold((0, 0, 0), |(s, v, x), d| {
            let held = d.versions.iter().filter_map(|v| v.row.as_ref()?.vec.as_ref());
            (s + 1, v + d.versions.len(), x + held.map(|vec| vec.len()).sum::<usize>())
        })
    }

    pub fn store(&self) -> Option<&Arc<dyn Vectors>> {
        self.vectors.as_ref()
    }

    /// Whether any version may have a successor.
    pub fn any_superseded(&self) -> bool {
        self.superseded
    }

    /// The version that superseded `(slot, at)`.
    pub fn successor(&self, slot: Slot, at: Pos) -> Option<&Version> {
        let v = &self.data(slot)?.versions;
        v.get(v.partition_point(|v| v.at <= at))
    }

    /// `VisibleAt`: the version is the one to show at this point.
    pub fn visible(&self, slot: Slot, at: Pos, p: Point) -> bool {
        self.version(slot, at)
            .is_some_and(|v| v.row.is_some() && v.within(p))
            && !self.successor(slot, at).is_some_and(|s| s.within(p))
    }

    /// `Shows`, with the mutants of the spec. `applied` is the range's applied position.
    pub fn shows(&self, slot: Slot, at: Pos, p: Point, applied: Pos) -> bool {
        if mutant("prefix_current_liveness") {
            let now = Point {
                pos: applied,
                cut: CommitTs::MAX,
            };
            return at <= p.pos && self.visible(slot, at, now);
        }
        if mutant("prefix_short_version") {
            let versions = self.data(slot).map_or(&[][..], |d| &d.versions);
            let mut like = versions.iter().filter(|v| v.at % 2 == at % 2);
            return at <= p.pos && like.any(|v| self.visible(slot, v.at, p));
        }
        if mutant("show_superseded") {
            return at <= p.pos;
        }
        self.visible(slot, at, p)
    }

    /// Adds a version of a key to its slot, or to a new one. Returns the slot and whether an older
    /// version was superseded, or `None` when the version breaks the log or timestamp order.
    pub fn insert(
        &mut self,
        keys: &mut BTreeMap<Key, Slot>,
        generation: Gen,
        key: &Key,
        version: (Pos, CommitTs, Option<Arc<Row>>),
    ) -> Option<(Slot, bool)> {
        let (at, ts, row) = version;
        let new = Version {
            at,
            ts,
            row,
            refs: 0,
        };
        if let Some(&slot) = keys.get(key) {
            let versions = &mut self.data_mut(slot)?.versions;
            if versions.last().is_some_and(|v| v.at >= at || v.ts >= ts) {
                return None;
            }
            versions.push(new);
            self.superseded = true;
            return Some((slot, true));
        }
        let data = SlotData {
            generation,
            key: key.clone(),
            versions: vec![new],
            dead: false,
        };
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.slots.push(None);
                Slot::try_from(self.slots.len() - 1).ok()?
            }
        };
        self.slots[slot as usize] = Some(data);
        keys.insert(key.clone(), slot);
        Some((slot, false))
    }

    /// Recovery: puts a version of a checkpoint where it belongs. A version that was known only as
    /// a successor gets its row.
    pub fn restore(
        &mut self,
        keys: &mut BTreeMap<Key, Slot>,
        generation: Gen,
        key: &Key,
        version: (Pos, CommitTs, Option<Arc<Row>>),
    ) -> Option<Slot> {
        let Some(&slot) = keys.get(key) else {
            return Some(self.insert(keys, generation, key, version)?.0);
        };
        let (at, ts, row) = version;
        let versions = &mut self.data_mut(slot)?.versions;
        match versions.binary_search_by_key(&at, |v| v.at) {
            Ok(i) if versions[i].row.is_none() => versions[i].row = row,
            Ok(_) => {}
            Err(i) => versions.insert(
                i,
                Version {
                    at,
                    ts,
                    row,
                    refs: 0,
                },
            ),
        }
        self.superseded |= versions.len() > 1;
        Some(slot)
    }

    /// Something names the version.
    pub fn name(&mut self, slot: Slot, at: Pos) {
        if let Some(v) = self.version_mut(slot, at) {
            v.refs = v.refs.saturating_add(1);
        }
    }

    pub fn refs(&self, slot: Slot, at: Pos) -> u32 {
        self.version(slot, at).map_or(0, |v| v.refs)
    }

    /// Something stopped naming the version. `low` is the oldest servable cut of its range.
    pub fn forget(
        &mut self,
        keys: Option<&mut BTreeMap<Key, Slot>>,
        slot: Slot,
        at: Pos,
        low: Pos,
    ) {
        if let Some(v) = self.version_mut(slot, at) {
            v.refs = v.refs.saturating_sub(1);
        }
        self.trim(keys, slot, low);
    }

    /// Drops the oldest versions that nothing names and no cut can see, and the slot with the last.
    pub fn trim(&mut self, keys: Option<&mut BTreeMap<Key, Slot>>, slot: Slot, low: Pos) {
        let Some(data) = self.data_mut(slot) else {
            return;
        };
        let dropped: Vec<Version> = if data.dead {
            let all = std::mem::take(&mut data.versions);
            let (keep, dropped) = all.into_iter().partition(|v| v.refs > 0);
            data.versions = keep;
            dropped
        } else {
            let versions = &data.versions;
            let gone = |i: usize| {
                let v = &versions[i];
                let superseded = versions.get(i + 1).is_some_and(|s| s.at <= low);
                v.refs == 0
                    && (superseded || (v.row.is_none() && i + 1 == versions.len() && v.at <= low))
            };
            let n = (0..versions.len()).take_while(|&i| gone(i)).count();
            data.versions.drain(..n).collect()
        };
        let empty = data.versions.is_empty();
        if empty && let (Some(keys), false) = (keys, data.dead) {
            keys.remove(&data.key);
        }
        if let Some(store) = &self.vectors {
            for v in dropped.iter().filter(|v| v.row.is_some()) {
                store.release(VecKey { slot, at: v.at });
            }
        }
        if empty {
            self.slots[slot as usize] = None;
            self.free.push(slot);
        }
    }

    /// The slot's generation is void.
    pub fn kill(&mut self, slot: Slot) {
        if let Some(data) = self.data_mut(slot) {
            data.dead = true;
            self.trim(None, slot, 0);
        }
    }

    pub fn generation(&self, slot: Slot) -> Option<Gen> {
        Some(self.data(slot)?.generation)
    }

    /// Every version of a slot that superseded another one above `low`.
    pub fn superseding(&self, slot: Slot, low: Pos) -> impl Iterator<Item = (Pos, CommitTs)> + '_ {
        let versions = self.data(slot).map_or(&[][..], |d| &d.versions);
        let after_first = versions.iter().skip(1).filter(move |v| v.at > low);
        after_first.map(|v| (v.at, v.ts))
    }

    #[cfg(test)]
    pub fn census(&self) -> Vec<Census> {
        let live = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(i, d)| Some((i, d.as_ref()?)));
        live.map(|(i, d)| {
            let versions = d.versions.iter().map(|v| (v.at, v.refs)).collect();
            Census {
                slot: i as Slot,
                versions,
            }
        })
        .collect()
    }
}

/// A live slot, and how often each of its versions is named.
#[cfg(test)]
pub struct Census {
    pub slot: Slot,
    pub versions: Vec<(Pos, u32)>,
}
