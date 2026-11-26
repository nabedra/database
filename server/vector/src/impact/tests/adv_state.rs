//! Adversarial tests of task G, state: visibility, pins, generations, retention and dedup by slot
//! under churn, merges, drops and resyncs, with pins held across merges. The model is written here:
//! every version of a key has a distinct integer value `v`, stored as a one-index sparse vector
//! `[(0, v)]` and as the image, so that a dot product with `[(0, 1)]` returns `v` and any stale or
//! mixed-up row shows.

use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};

use super::*;

pub(super) fn vrow(v: u64) -> Row {
    sparse(&[(0, v as f32)], vec![tags(&[])], v)
}

/// The feed of one range and what a reader at a point must see.
#[derive(Clone)]
pub(super) struct RM {
    /// (position, commit timestamp, key, version value or a delete).
    pub ops: Vec<(Pos, CommitTs, u64, Option<u64>)>,
    pub next: Pos,
    pub closed: CommitTs,
}

impl RM {
    pub fn new() -> RM {
        RM {
            ops: Vec::new(),
            next: 0,
            closed: 0,
        }
    }

    pub fn page(&mut self, changes: Vec<(u64, Option<u64>)>, same: bool) -> Page {
        let after = self.next;
        let mut records = Vec::new();
        for (i, (k, v)) in changes.into_iter().enumerate() {
            if i == 0 || !same {
                self.next += 1;
            }
            let (at, ts) = (self.next, self.next * 10 + i as u64);
            self.ops.push((at, ts, k, v));
            records.push(Record {
                at,
                change: Change::Row {
                    key: key(k),
                    commit_ts: ts,
                    row: v.map(vrow),
                },
            });
        }
        Page {
            after,
            through: self.next,
            records,
        }
    }

    pub fn close(&mut self) -> Page {
        let after = self.next;
        self.next += 1;
        self.closed = self
            .ops
            .iter()
            .map(|o| o.1)
            .max()
            .unwrap_or(0)
            .max(self.closed);
        Page {
            after,
            through: self.next,
            records: vec![Record {
                at: self.next,
                change: Change::Closed(self.closed),
            }],
        }
    }

    /// The newest version of each key at or below a position and a commit timestamp, if it is a
    /// value.
    pub fn visible(&self, pos: Pos, cut: CommitTs) -> BTreeMap<u64, u64> {
        let mut newest: BTreeMap<u64, Option<u64>> = BTreeMap::new();
        for (at, ts, k, v) in &self.ops {
            if *at <= pos && *ts <= cut {
                newest.insert(*k, *v);
            }
        }
        newest
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect()
    }
}

pub(super) fn bring_up(ix: &mut ImpactIndex, id: RangeId, at: Pos) {
    ix.begin_load(id, at, 0).unwrap();
    ix.finish_load(id).unwrap();
    ix.caught_up(id, at).unwrap();
}

fn kk(h: &Hit) -> u64 {
    u64::from_be_bytes((&*h.key).try_into().unwrap())
}

/// What a pin shows of every range: key to (score, image, range). A key twice is an error.
pub(super) fn shown(
    ix: &ImpactIndex,
    pin: &Pin,
    exact: bool,
) -> std::result::Result<BTreeMap<u64, (f32, u64, RangeId)>, Error> {
    let q = Query {
        exact,
        ..sparse_query(Op::Dot, &[(0, 1.0)], MAX_LIMIT)
    };
    let mut out = BTreeMap::new();
    for h in ix.search(pin, &q)? {
        let image = u64::from_be_bytes((&*h.image).try_into().unwrap());
        if out.insert(kk(&h), (h.score, image, h.range)).is_some() {
            return Err(Error::Internal("a key twice in a result"));
        }
    }
    Ok(out)
}

pub(super) fn same(
    want: &BTreeMap<u64, u64>,
    got: &BTreeMap<u64, (f32, u64, RangeId)>,
    ranges: &[(RangeId, u64)],
) -> std::result::Result<(), String> {
    let g: BTreeMap<u64, u64> = got.iter().map(|(k, (s, _, _))| (*k, *s as u64)).collect();
    if want != &g {
        return Err(format!("want {want:?}, got {g:?}"));
    }
    for (k, (s, image, range)) in got {
        let owner = ranges
            .iter()
            .find(|(_, base)| k / 100 == *base / 100)
            .map(|r| r.0);
        if *image != *s as u64 || Some(*range) != owner {
            return Err(format!(
                "key {k}: score {s}, image {image}, range {range:?}"
            ));
        }
    }
    Ok(())
}

const A: RangeId = RangeId(7);
const B: RangeId = RangeId(8);

/// One held read: the pin and, per range, the model point it must show.
struct Held {
    pin: Pin,
    points: Vec<(usize, Pos, CommitTs)>,
    exact: bool,
}

fn run(g: &mut Gen) -> std::result::Result<(), String> {
    let cfg = Config {
        segment_rows: 1 + g.below(5) as usize,
        merge_factor: 2 + g.below(3) as usize,
        max_segment_rows: 32,
        ..sparse_cfg()
    };
    let mut ix = ImpactIndex::new(cfg, RamStore::default(), None).unwrap();
    let ids = [A, B];
    let bases = [0u64, 100];
    for id in ids {
        bring_up(&mut ix, id, 0);
    }
    let mut ms = [RM::new(), RM::new()];
    let mut held: Vec<Held> = Vec::new();
    // Pins that a drop or a resync must have failed.
    let mut dead: Vec<Pin> = Vec::new();
    let mut v = 0u64;
    let expect = |ms: &[RM; 2], pts: &[(usize, Pos, CommitTs)]| -> BTreeMap<u64, u64> {
        pts.iter()
            .flat_map(|(r, p, c)| ms[*r].visible(*p, *c))
            .collect()
    };
    let ranges: Vec<(RangeId, u64)> = ids.iter().copied().zip(bases).collect();
    for _ in 0..g.below(70) {
        match g.below(15) {
            0..=4 => {
                let r = g.below(2) as usize;
                let mut changes: BTreeMap<u64, Option<u64>> = BTreeMap::new();
                for _ in 0..1 + g.below(3) {
                    v += 1;
                    let val = if g.chance(1, 4) { None } else { Some(v) };
                    changes.insert(bases[r] + g.below(8), val);
                }
                let page = ms[r].page(changes.into_iter().collect(), g.bool());
                ix.apply(ids[r], &page).map_err(|e| format!("apply: {e}"))?;
            }
            5 => {
                let r = g.below(2) as usize;
                let page = ms[r].close();
                ix.apply(ids[r], &page).map_err(|e| format!("close: {e}"))?;
            }
            6 | 7 => {
                for _ in 0..1 + g.below(4) {
                    ix.maintain().map_err(|e| e.to_string())?;
                }
            }
            8 => {
                let r = g.below(2) as usize;
                let s = ix.status(ids[r]).unwrap();
                let low = s.low + g.below(s.applied - s.low + 1);
                ix.advance_low(ids[r], low).map_err(|e| e.to_string())?;
            }
            9 if held.len() < 4 => {
                let mut reads = Vec::new();
                let mut points = Vec::new();
                for (r, &id) in ids.iter().enumerate() {
                    let s = ix.status(id).unwrap();
                    let (read, p, c) = match g.below(3) {
                        0 => (
                            ReadAt::Latest { barrier: s.applied },
                            s.applied,
                            CommitTs::MAX,
                        ),
                        1 => {
                            let pos = s.low + g.below(s.applied - s.low + 1);
                            (ReadAt::Position(pos), pos, CommitTs::MAX)
                        }
                        _ if s.closed >= s.bound && s.closed > 0 => {
                            let ts = s.bound + g.below(s.closed - s.bound + 1);
                            (ReadAt::Cut(ts), s.applied, ts)
                        }
                        _ => (
                            ReadAt::Latest { barrier: s.applied },
                            s.applied,
                            CommitTs::MAX,
                        ),
                    };
                    reads.push((id, read));
                    points.push((r, p, c));
                }
                let pin = ix.pin(&reads).map_err(|e| format!("pin {reads:?}: {e}"))?;
                held.push(Held {
                    pin,
                    points,
                    exact: g.bool(),
                });
            }
            10 | 11 => {
                for h in &held {
                    let got = shown(&ix, &h.pin, h.exact).map_err(|e| format!("held: {e}"))?;
                    same(&expect(&ms, &h.points), &got, &ranges)
                        .map_err(|e| format!("held pin {:?}: {e}", h.points))?;
                }
                if g.chance(1, 3) {
                    held.truncate(g.below(held.len() as u64 + 1) as usize);
                }
            }
            12 => {
                ix.compact().map_err(|e| e.to_string())?;
            }
            13 => {
                // A fresh latest pin on both ranges.
                let reads: Vec<_> = (0..2)
                    .map(|r| {
                        (
                            ids[r],
                            ReadAt::Latest {
                                barrier: ms[r].next,
                            },
                        )
                    })
                    .collect();
                let pin = ix.pin(&reads).map_err(|e| e.to_string())?;
                let pts: Vec<_> = (0..2).map(|r| (r, ms[r].next, CommitTs::MAX)).collect();
                let got = shown(&ix, &pin, g.bool()).map_err(|e| e.to_string())?;
                same(&expect(&ms, &pts), &got, &ranges).map_err(|e| format!("fresh: {e}"))?;
            }
            _ if g.chance(1, 2) => {
                // A drop, then a resync from a snapshot of the newest rows, at a later position.
                let r = g.below(2) as usize;
                let before: Vec<Pin> = held.iter().map(|h| h.pin.clone()).collect();
                ix.drop_range(ids[r]).map_err(|e| e.to_string())?;
                if !matches!(
                    ix.pin(&[(ids[r], ReadAt::Latest { barrier: 0 })]),
                    Err(Error::NotHeld(_))
                ) {
                    return Err("a dropped range is still pinned".into());
                }
                // The other range is not touched.
                let other = 1 - r;
                let pin = ix
                    .pin(&[(ids[other], ReadAt::Latest { barrier: 0 })])
                    .map_err(|e| e.to_string())?;
                let got = shown(&ix, &pin, false).map_err(|e| e.to_string())?;
                same(
                    &ms[other].visible(ms[other].next, CommitTs::MAX),
                    &got,
                    &ranges,
                )
                .map_err(|e| format!("after a drop: {e}"))?;
                if g.bool() {
                    ix.compact().map_err(|e| e.to_string())?;
                }
                let at = ms[r].next + 1 + g.below(5);
                let snapshot = ms[r].visible(ms[r].next, CommitTs::MAX);
                let bound = ms[r].ops.iter().map(|o| o.1).max().unwrap_or(0);
                ix.begin_load(ids[r], at, bound)
                    .map_err(|e| e.to_string())?;
                let rows: Vec<(Key, CommitTs, Row)> = snapshot
                    .iter()
                    .map(|(k, val)| {
                        (
                            key(*k),
                            ms[r]
                                .ops
                                .iter()
                                .filter(|o| o.2 == *k)
                                .map(|o| o.1)
                                .max()
                                .unwrap(),
                            vrow(*val),
                        )
                    })
                    .collect();
                ix.load_rows(ids[r], &rows).map_err(|e| e.to_string())?;
                ix.finish_load(ids[r]).map_err(|e| e.to_string())?;
                if ix.pin(&[(ids[r], ReadAt::Latest { barrier: 0 })]).is_ok() {
                    return Err("a range serves before a fresh barrier".into());
                }
                ix.caught_up(ids[r], at).map_err(|e| e.to_string())?;
                // The model: the snapshot, as of position `at`.
                let mut m = RM::new();
                m.next = at;
                m.closed = 0;
                for (k, val) in &snapshot {
                    let ts = ms[r]
                        .ops
                        .iter()
                        .filter(|o| o.2 == *k)
                        .map(|o| o.1)
                        .max()
                        .unwrap();
                    m.ops.push((at, ts, *k, Some(*val)));
                }
                ms[r] = m;
                for p in before.into_iter() {
                    dead.push(p);
                }
                held.retain(|_| false);
            }
            _ => {}
        }
        for p in &dead {
            if !matches!(shown_err(&ix, p), Some(Error::PinFailed)) {
                return Err("a pin of a dropped generation still reads".into());
            }
        }
    }
    // Everything is deleted and every pin is gone: memory returns.
    drop(held);
    drop(dead);
    for r in 0..2 {
        let keys: Vec<(u64, Option<u64>)> = ms[r]
            .visible(ms[r].next, CommitTs::MAX)
            .keys()
            .map(|k| (*k, None))
            .collect();
        if !keys.is_empty() {
            let page = ms[r].page(keys, false);
            ix.apply(ids[r], &page).map_err(|e| e.to_string())?;
        }
        let applied = ix.status(ids[r]).unwrap().applied;
        ix.advance_low(ids[r], applied).map_err(|e| e.to_string())?;
    }
    for _ in 0..4 {
        ix.compact().map_err(|e| e.to_string())?;
    }
    let (ready, retired) = ix.segments();
    if ix.rows.live() != 0 || ix.store.blocks() != 0 || retired != 0 || ready > 1 {
        return Err(format!(
            "memory stays: {} slots, {} blocks, {ready} ready, {retired} retired segments",
            ix.rows.live(),
            ix.store.blocks()
        ));
    }
    Ok(())
}

fn shown_err(ix: &ImpactIndex, pin: &Pin) -> Option<Error> {
    shown(ix, pin, false).err()
}

#[test]
fn vs05_adv_pins_generations_and_retention_equal_the_model_under_churn() {
    check_with(
        Runs {
            cases: 300,
            ..Runs::default()
        },
        "vs05_adv_pins_generations_and_retention_equal_the_model_under_churn",
        run,
    );
}

#[test]
fn vs14_adv_a_dropped_range_leaves_memory_at_its_segments_next_merge() {
    let mut ix = ImpactIndex::new(
        Config {
            segment_rows: 64,
            merge_factor: 2,
            ..sparse_cfg()
        },
        RamStore::default(),
        None,
    )
    .unwrap();
    for id in [A, B] {
        bring_up(&mut ix, id, 0);
    }
    let mut v = 0;
    let mut ms = [RM::new(), RM::new()];
    for (r, id) in [A, B].into_iter().enumerate() {
        for chunk in 0..10 {
            let changes = (0..60).map(|k| {
                v += 1;
                (r as u64 * 1000 + chunk * 60 + k, Some(v))
            });
            let page = ms[r].page(changes.collect(), false);
            ix.apply(id, &page).unwrap();
        }
    }
    let live_entries =
        |ix: &ImpactIndex| ix.segments.values().map(|s| s.entries.len()).sum::<usize>();
    assert_eq!(live_entries(&ix), 1200);
    let pin_a = ix.pin(&[(A, ReadAt::Latest { barrier: 0 })]).unwrap();
    let pin_b = ix.pin(&[(B, ReadAt::Latest { barrier: 0 })]).unwrap();
    ix.drop_range(A).unwrap();
    // The failed pin does not hold segments, the other one does until it ends.
    assert_eq!(shown(&ix, &pin_a, false).err(), Some(Error::PinFailed));
    while ix.maintain().unwrap() {}
    assert!(live_entries(&ix) >= 600);
    let want = ms[1].visible(ms[1].next, CommitTs::MAX);
    let got = shown(&ix, &pin_b, true).unwrap();
    assert_eq!(want.len(), got.len());
    drop(pin_b);
    drop(pin_a);
    for _ in 0..4 {
        ix.compact().unwrap();
    }
    assert_eq!(
        live_entries(&ix),
        600,
        "the entries of the dropped range are gone"
    );
    assert_eq!(ix.rows.live(), 600);
    assert_eq!(ix.segments().1, 0);
}

#[test]
fn vs14_adv_a_resync_replaces_the_range_and_old_rows_never_come_back() {
    let mut ix = ImpactIndex::new(
        Config {
            segment_rows: 2,
            ..sparse_cfg()
        },
        RamStore::default(),
        None,
    )
    .unwrap();
    bring_up(&mut ix, A, 0);
    let mut m = RM::new();
    let page = m.page((0..6).map(|k| (k, Some(k + 1))).collect(), false);
    ix.apply(A, &page).unwrap();
    let old = ix.pin(&[(A, ReadAt::Latest { barrier: 0 })]).unwrap();
    // The resync's snapshot holds only keys 2 and 9.
    ix.begin_load(A, 50, 500).unwrap();
    ix.load_rows(A, &[(key(2), 400, vrow(70)), (key(9), 500, vrow(71))])
        .unwrap();
    ix.finish_load(A).unwrap();
    ix.caught_up(A, 50).unwrap();
    assert_eq!(shown(&ix, &old, false).err(), Some(Error::PinFailed));
    for _ in 0..3 {
        ix.compact().unwrap();
        let pin = ix.pin(&[(A, ReadAt::Latest { barrier: 50 })]).unwrap();
        let got: Vec<u64> = shown(&ix, &pin, false).unwrap().into_keys().collect();
        assert_eq!(got, [2, 9]);
    }
    // Cuts below what the snapshot knows are refused.
    assert_eq!(
        ix.pin(&[(A, ReadAt::Position(49))]).err(),
        Some(Error::CutTooOld { oldest: 50 })
    );
    assert_eq!(
        ix.pin(&[(A, ReadAt::Cut(499))]).err(),
        Some(Error::CutTooOld { oldest: 500 })
    );
    // A snapshot that repeats a key or holds an invalid row fails the range.
    ix.begin_load(A, 60, 5).unwrap();
    assert!(
        ix.load_rows(A, &[(key(1), 1, vrow(1)), (key(1), 2, vrow(2))])
            .is_err()
    );
    assert!(matches!(
        ix.pin(&[(A, ReadAt::Latest { barrier: 0 })]),
        Err(Error::OutOfService { .. })
    ));
    // Only another install brings it back.
    ix.begin_load(A, 70, 5).unwrap();
    ix.finish_load(A).unwrap();
    ix.caught_up(A, 70).unwrap();
    let pin = ix.pin(&[(A, ReadAt::Latest { barrier: 70 })]).unwrap();
    assert!(shown(&ix, &pin, false).unwrap().is_empty());
}

#[test]
fn vs05_adv_a_pin_held_across_every_merge_and_retention_step_keeps_its_cut() {
    for settle_each in [false, true] {
        let mut ix = ImpactIndex::new(
            Config {
                segment_rows: 3,
                merge_factor: 2,
                max_segment_rows: 16,
                ..sparse_cfg()
            },
            RamStore::default(),
            None,
        )
        .unwrap();
        bring_up(&mut ix, A, 0);
        let mut m = RM::new();
        let mut v = 0;
        let mut pins: Vec<(Pin, Pos)> = Vec::new();
        for round in 0..30u64 {
            let changes: Vec<(u64, Option<u64>)> = (0..4)
                .map(|i| {
                    v += 1;
                    ((round * 3 + i) % 7, if v % 5 == 0 { None } else { Some(v) })
                })
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect();
            let page = m.page(changes, false);
            ix.apply(A, &page).unwrap();
            if round % 4 == 0 {
                let p = ix.pin(&[(A, ReadAt::Latest { barrier: 0 })]).unwrap();
                pins.push((p, m.next));
            }
            let applied = ix.status(A).unwrap().applied;
            ix.advance_low(A, applied.saturating_sub(1)).unwrap();
            if settle_each {
                settle(&mut ix);
            } else {
                ix.compact().unwrap();
            }
            for (pin, pos) in &pins {
                let got = shown(&ix, pin, round % 2 == 0).unwrap();
                same(&m.visible(*pos, CommitTs::MAX), &got, &[(A, 0)])
                    .unwrap_or_else(|e| panic!("round {round} pin at {pos}: {e}"));
            }
        }
    }
}
