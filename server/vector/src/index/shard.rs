//! One core: the partitions it owns and what it does with them. Every function is one step of
//! `LocalIndex.tla` on one partition. A step ends in the shared state under its lock, which an
//! executor with pinned threads would hold only for that.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::codec::{Codec, direction};
use super::filter::Column;
use super::mutant::mutant;
use super::rows::{Gen, Slot};
use super::{Append, Copy, Msg, Shared, State};
use crate::types::{PartId, Pos, Row};

/// An entry instance. A removal names `id`, never the row.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Entry {
    pub slot: Slot,
    /// The version: the full apply position.
    pub at: Pos,
    pub generation: Gen,
    pub id: u64,
    /// A drain sent a copy to a target.
    pub(super) sent: bool,
}

/// Entries of similar length, a structure of arrays.
#[derive(Clone)]
pub struct Band<B> {
    pub exp: i32,
    pub entries: Vec<Entry>,
    pub lens: Vec<f64>,
    pub cols: Vec<Column>,
    pub codes: B,
}

#[derive(Clone)]
pub struct Partition<B> {
    /// Rotated, or `None` in the list of vectors without a direction.
    pub centroid: Option<Vec<f32>>,
    pub bands: Vec<Band<B>>,
    /// Entries that moved away: instance, and the pins to wait for once the copy landed.
    removals: BTreeMap<u64, Option<u64>>,
}

/// The band of a length: lengths within a factor √2 of a power of two. Unit vectors share one.
pub fn band_of(len: f64) -> i32 {
    if len > 0.0 {
        len.log2().round() as i32
    } else {
        i32::MIN
    }
}

/// The lengths a band may hold.
pub fn band_span(exp: i32) -> (f64, f64) {
    if exp == i32::MIN {
        return (0.0, 0.0);
    }
    let e = f64::from(exp);
    ((e - 0.500001).exp2(), (e + 0.500001).exp2())
}

#[derive(Clone)]
pub struct Shard<C: Codec> {
    pub parts: BTreeMap<PartId, Partition<C::Block>>,
}

impl<B> Partition<B> {
    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.bands.iter().flat_map(|b| &b.entries)
    }

    fn find(&self, id: u64) -> Option<Entry> {
        self.entries().find(|e| e.id == id).copied()
    }

    fn removing(&self, e: &Entry) -> bool {
        self.removals.contains_key(&e.id)
    }
}

/// What a step needs of a live version.
fn row_of(sh: &Shared, slot: Slot, at: Pos) -> Option<(Arc<Row>, Vec<f32>, f64)> {
    let row = sh.rows.version(slot, at)?.row.clone()?;
    let vec = row.vec.clone()?;
    let (dir, len) = direction(&vec).unwrap_or_else(|| (vec.to_vec(), 0.0));
    Some((row, dir, len))
}

impl<C: Codec> Shard<C> {
    pub fn new() -> Self {
        Shard {
            parts: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, sh: &mut Shared, codec: &C, part: PartId, e: Entry) {
        let Some((row, dir, len)) = row_of(sh, e.slot, e.at) else {
            return;
        };
        let Some(meta) = sh.dir.get_mut(&part) else {
            return;
        };
        let rotated = codec.rotate(&dir);
        let p = self.parts.entry(part).or_insert_with(|| Partition {
            centroid: (!meta.undirected).then(|| codec.rotate(&meta.centroid)),
            bands: Vec::new(),
            removals: BTreeMap::new(),
        });
        let exp = band_of(len);
        let i = p
            .bands
            .iter()
            .position(|b| b.exp == exp)
            .unwrap_or_else(|| {
                p.bands.push(Band {
                    exp,
                    entries: Vec::new(),
                    lens: Vec::new(),
                    cols: vec![Vec::new(); row.cols.len()],
                    codes: codec.block(),
                });
                p.bands.len() - 1
            });
        let band = &mut p.bands[i];
        if let Some(centroid) = &p.centroid {
            codec.push(&mut band.codes, &rotated, centroid, len as f32);
            meta.radius = meta.radius.min(super::codec::dot(&dir, &meta.centroid));
        }
        band.entries.push(e);
        band.lens.push(len);
        for (col, v) in band.cols.iter_mut().zip(&row.cols) {
            col.push(v.clone());
        }
        meta.entries += 1;
        sh.summary.add(part, &row.cols);
        if let Some(root) = sh.roots.get_mut(&meta.group) {
            root.entries += 1;
            *root.bands.entry(exp).or_default() += 1;
        }
    }

    /// Takes an entry out and stops naming its version.
    fn take(&mut self, sh: &mut Shared, codec: &C, part: PartId, id: u64) {
        let Some(p) = self.parts.get_mut(&part) else {
            return;
        };
        p.removals.remove(&id);
        let find = |b: &Band<C::Block>| b.entries.iter().position(|e| e.id == id);
        let Some((b, i)) = p
            .bands
            .iter()
            .enumerate()
            .find_map(|(b, band)| Some((b, find(band)?)))
        else {
            return;
        };
        let band = &mut p.bands[b];
        let e = band.entries.swap_remove(i);
        band.lens.swap_remove(i);
        let cols: Vec<_> = band.cols.iter_mut().map(|c| c.swap_remove(i)).collect();
        if p.centroid.is_some() {
            codec.swap_remove(&mut band.codes, i);
        }
        let exp = band.exp;
        if band.entries.is_empty() {
            p.bands.remove(b);
        }
        sh.summary.remove(part, &cols);
        if let Some(meta) = sh.dir.get_mut(&part) {
            meta.entries = meta.entries.saturating_sub(1);
            if let Some(root) = sh.roots.get_mut(&meta.group) {
                root.entries = root.entries.saturating_sub(1);
                if let Some(n) = root.bands.get_mut(&exp) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        root.bands.remove(&exp);
                    }
                }
            }
        }
        sh.forget(e.generation, e.slot, e.at);
    }

    /// Handles one message. Returns it when it has to wait.
    pub fn handle(&mut self, sh: &mut Shared, codec: &C, msg: Msg) -> Option<Msg> {
        match msg {
            Msg::Append(a, part) => {
                self.append_entry(sh, codec, a, part);
                None
            }
            Msg::Copy(c) => self.land(sh, codec, c),
        }
    }

    /// `AppendEntry`. A partition that drains by now gets nothing: the entry is assigned again.
    fn append_entry(&mut self, sh: &mut Shared, codec: &C, a: Append, part: PartId) {
        if !sh.gens.contains_key(&a.generation) {
            sh.forget(a.generation, a.slot, a.at);
        } else if sh.dir.get(&part).is_some_and(|m| m.state == State::Ready) {
            let id = sh.new_id();
            let Append {
                slot,
                at,
                generation,
            } = a;
            self.put(
                sh,
                codec,
                part,
                Entry {
                    slot,
                    at,
                    generation,
                    id,
                    sent: false,
                },
            );
            sh.placed(a.generation, a.at);
        } else {
            sh.inbox.push_front(a);
        }
    }

    /// `Land`: also in a partition that drains meanwhile, which then sends the copy on. A copy of an
    /// older generation is dropped.
    fn land(&mut self, sh: &mut Shared, codec: &C, c: Copy) -> Option<Msg> {
        let state = sh.dir.get(&c.to).map(|m| m.state);
        let current = sh.gens.contains_key(&c.entry.generation);
        let lands = current || (mutant("late_copy_after_resync") && state == Some(State::Ready));
        match state {
            Some(State::Ready) => {}
            Some(State::Draining) if !mutant("prefix_land_needs_ready") => {}
            _ if !current => {}
            _ => return Some(Msg::Copy(c)),
        }
        if lands {
            let id = c.entry.id;
            self.put(
                sh,
                codec,
                c.to,
                Entry {
                    id,
                    sent: false,
                    ..c.entry
                },
            );
        } else {
            sh.forget(c.entry.generation, c.entry.slot, c.entry.at);
        }
        let epoch = sh.readers.epoch();
        if let Some(meta) = sh.dir.get_mut(&c.to) {
            meta.arriving = meta.arriving.saturating_sub(1);
        }
        if let Some(meta) = sh.dir.get_mut(&c.from) {
            meta.leaving = meta.leaving.saturating_sub(1);
            if lands && c.moved {
                meta.landed.push((c.source, epoch));
            }
        }
        None
    }

    /// Puts a copy on its way if the target's queue has room.
    fn send(&self, sh: &mut Shared, e: Entry, from: PartId, to: PartId, moved: bool) -> bool {
        let Some(owner) = sh.dir.get(&to).map(|m| m.owner) else {
            return false;
        };
        if sh.queues[owner].len() >= sh.queue_len {
            return false;
        }
        let id = sh.new_id();
        sh.rows.name(e.slot, e.at);
        let entry = Entry {
            id,
            sent: false,
            ..e
        };
        sh.queues[owner].push_back(Msg::Copy(Copy {
            entry,
            from,
            source: e.id,
            to,
            moved,
        }));
        for (part, leaves) in [(from, true), (to, false)] {
            if let Some(meta) = sh.dir.get_mut(&part) {
                *(if leaves {
                    &mut meta.leaving
                } else {
                    &mut meta.arriving
                }) += 1;
            }
        }
        true
    }

    /// `SendCopy`, for up to `n` entries. Each goes to the target with the nearest centroid.
    pub fn send_copies(&mut self, sh: &mut Shared, part: PartId, n: usize) -> usize {
        let Some(meta) = sh.dir.get(&part).filter(|m| m.state == State::Draining) else {
            return 0;
        };
        let targets: Vec<(PartId, Arc<[f32]>)> = meta
            .targets
            .iter()
            .filter_map(|t| Some((*t, sh.dir.get(t)?.centroid.clone())))
            .collect();
        let Some(p) = self.parts.get(&part) else {
            return 0;
        };
        let unsent = p
            .entries()
            .filter(|e| !e.sent && sh.wanted(e.generation, e.slot, e.at));
        let todo: Vec<Entry> = unsent.take(n).copied().collect();
        let mut sent = 0;
        for e in todo {
            let Some((_, dir, _)) = row_of(sh, e.slot, e.at) else {
                continue;
            };
            let cos = |t: &(PartId, Arc<[f32]>)| super::codec::dot(&t.1, &dir);
            let Some(to) = targets.iter().max_by(|a, b| cos(a).total_cmp(&cos(b))) else {
                continue;
            };
            if !self.send(sh, e, part, to.0, false) {
                break;
            }
            sent += 1;
            if let Some(p) = self.parts.get_mut(&part) {
                let mut all = p.bands.iter_mut().flat_map(|b| &mut b.entries);
                if let Some(e) = all.find(|x| x.id == e.id) {
                    e.sent = true;
                }
            }
        }
        sent
    }

    /// `Reassign`: append to the target, remove from the source later.
    pub fn reassign(
        &mut self,
        sh: &mut Shared,
        codec: &C,
        part: PartId,
        id: u64,
        to: PartId,
    ) -> bool {
        let ready = |p| {
            sh.dir
                .get(&p)
                .is_some_and(|m| m.state == State::Ready && !m.undirected)
        };
        if part == to || !ready(part) || !ready(to) {
            return false;
        }
        let Some(p) = self.parts.get(&part) else {
            return false;
        };
        let Some(e) = p.find(id).filter(|e| !p.removing(e)) else {
            return false;
        };
        if !sh.wanted(e.generation, e.slot, e.at) || !self.send(sh, e, part, to, true) {
            return false;
        }
        if mutant("remove_before_append") {
            self.take(sh, codec, part, id);
        } else if let Some(p) = self.parts.get_mut(&part) {
            p.removals.insert(id, None);
        }
        true
    }

    /// `FinishDrain`: everything wanted was sent and has landed, and no copy is on its way here.
    pub fn finish_drain(&mut self, sh: &mut Shared, part: PartId) -> bool {
        let Some(meta) = sh.dir.get(&part) else {
            return false;
        };
        let copied = |e: &Entry| e.sent || !sh.wanted(e.generation, e.slot, e.at);
        let done = self
            .parts
            .get(&part)
            .is_none_or(|p| p.entries().all(copied));
        if meta.state != State::Draining || !done || meta.leaving > 0 || meta.arriving > 0 {
            return false;
        }
        sh.retire(part);
        true
    }

    /// `FreePart`: no pin that listed the retired partition is left.
    pub fn free_part(&mut self, sh: &mut Shared, codec: &C, part: PartId) -> bool {
        let Some(meta) = sh.dir.get(&part).filter(|m| m.state == State::Retired) else {
            return false;
        };
        if !sh.readers.past(meta.waits) && !mutant("free_without_wait") {
            return false;
        }
        for id in self.ids(part, |_, _| true) {
            self.take(sh, codec, part, id);
        }
        self.parts.remove(&part);
        sh.dir.remove(&part);
        true
    }

    fn ids(&self, part: PartId, f: impl Fn(&Partition<C::Block>, &Entry) -> bool) -> Vec<u64> {
        let p = self.parts.get(&part).into_iter();
        p.flat_map(|p| p.entries().filter(|e| f(p, e)).map(|e| e.id))
            .collect()
    }

    /// `RemoveMoved`: the pins that began before the copies landed are gone.
    pub fn remove_moved(&mut self, sh: &mut Shared, codec: &C, part: PartId) -> usize {
        let (Some(p), Some(meta)) = (self.parts.get_mut(&part), sh.dir.get_mut(&part)) else {
            return 0;
        };
        for (id, epoch) in meta.landed.drain(..) {
            if let Some(wait) = p.removals.get_mut(&id) {
                *wait = Some(epoch);
            }
        }
        let due = |wait: u64| sh.readers.past(wait) || mutant("remove_without_wait");
        let due = p
            .removals
            .iter()
            .filter(|(_, w)| w.is_some_and(due))
            .map(|(id, _)| *id);
        let due: Vec<u64> = due.collect();
        for id in &due {
            let e = self.parts.get(&part).and_then(|p| p.find(*id));
            self.take(sh, codec, part, *id);
            if let Some(e) = e.filter(|_| mutant("prefix_remove_by_slot")) {
                for id in self.ids(part, |_, x| (x.slot, x.at) == (e.slot, e.at)) {
                    self.take(sh, codec, part, id);
                }
            }
        }
        due.len()
    }

    /// `Collect`: a superseded entry goes once no pin and no retained cut can see it.
    pub fn collect(&mut self, sh: &mut Shared, codec: &C, part: PartId) -> usize {
        let dead = self.ids(part, |p, e| {
            !p.removing(e) && sh.collectable(e.generation, e.slot, e.at)
        });
        for id in &dead {
            self.take(sh, codec, part, *id);
        }
        dead.len()
    }

    /// A resync or a drop takes every entry of a generation.
    pub fn purge(&mut self, sh: &mut Shared, codec: &C, generation: Gen) {
        let parts: Vec<PartId> = self.parts.keys().copied().collect();
        for part in parts {
            for id in self.ids(part, |_, e| e.generation == generation) {
                self.take(sh, codec, part, id);
            }
        }
    }

    /// The directions of the wanted entries of a partition, for a split.
    pub fn directions(&self, sh: &Shared, part: PartId) -> Vec<Vec<f32>> {
        let all = self.parts.get(&part).into_iter().flat_map(|p| p.entries());
        let wanted = all.filter(|e| sh.wanted(e.generation, e.slot, e.at));
        wanted
            .filter_map(|e| Some(row_of(sh, e.slot, e.at)?.1))
            .collect()
    }

    /// Entries of a ready partition that are clearly nearer to another one, and to which.
    pub fn strays(&self, sh: &Shared, part: PartId, n: usize) -> Vec<(u64, PartId)> {
        const MARGIN: f32 = 1e-3;
        let ready = sh.dir.get(&part).filter(|m| m.state == State::Ready);
        let Some((meta, p)) = ready.zip(self.parts.get(&part)) else {
            return Vec::new();
        };
        let Some(root) = sh.roots.get(&meta.group) else {
            return Vec::new();
        };
        let stray = |e: &Entry| {
            let (_, dir, _) = row_of(sh, e.slot, e.at)?;
            let (to, cos) = root.tree.nearest(&dir, Some(part))?;
            (cos > super::codec::dot(&dir, &meta.centroid) + MARGIN).then_some((e.id, to))
        };
        let here = p
            .entries()
            .filter(|e| !p.removing(e) && sh.wanted(e.generation, e.slot, e.at));
        here.filter_map(stray).take(n).collect()
    }
}
