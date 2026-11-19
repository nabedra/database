//! An [`InternalIterator`] over one memtable shard.
//!
//! A shard is a `RwLock<BTreeMap>` that the journal thread keeps inserting into, so the
//! cursor cannot hold a borrow across calls. It copies a small window of entries (two
//! reference-counted `Bytes` each) under one read lock, walks the window, and refills it
//! from the last key it saw. Windows grow from 8 to 256 entries, so a point-like seek
//! copies little and a long scan takes the lock once per 256 entries.
//!
//! Entries that appear after the window was copied have sequences above every snapshot
//! taken before them, so a snapshot read never misses an entry it can see.

use std::ops::Bound;
use std::sync::{Arc, PoisonError};

use bytes::Bytes;

use super::Memtable;
use super::key::{KeyBound, KeyView, MemKey, Probe, is_empty_range, max_lower, min_upper};
use crate::error::Result;
use crate::source::InternalIterator;

const FIRST_WINDOW: usize = 8;
const MAX_WINDOW: usize = 256;

pub struct ShardCursor {
    mem: Arc<Memtable>,
    shard: usize,
    lower: Bound<MemKey>,
    upper: Bound<MemKey>,
    /// Ascending window of entries; `pos` indexes the current one.
    window: Vec<(MemKey, Bytes)>,
    pos: usize,
    valid: bool,
    next_window: usize,
    key: Vec<u8>,
}

fn as_view(bound: &Bound<MemKey>) -> KeyBound<'_> {
    match bound {
        Bound::Included(k) => Bound::Included(k as &dyn KeyView),
        Bound::Excluded(k) => Bound::Excluded(k as &dyn KeyView),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Replaces `window` with up to `limit` entries of `[start, end]`, ascending: the first
/// ones of the range when `forward`, else the last ones.
fn fill_window(
    mem: &Memtable,
    shard: usize,
    window: &mut Vec<(MemKey, Bytes)>,
    start: KeyBound<'_>,
    end: KeyBound<'_>,
    forward: bool,
    limit: usize,
) {
    window.clear();
    if is_empty_range(start, end) {
        return;
    }
    let map = mem.shards[shard]
        .map
        .read()
        .unwrap_or_else(PoisonError::into_inner);
    let range = map.range::<dyn KeyView, _>((start, end));
    let clone = |(k, v): (&MemKey, &Bytes)| (k.clone(), v.clone());
    if forward {
        window.extend(range.take(limit).map(clone));
    } else {
        window.extend(range.rev().take(limit).map(clone));
        window.reverse();
    }
}

impl ShardCursor {
    /// `lower` and `upper` are internal-key bounds; entries outside them are never
    /// returned.
    pub(crate) fn new(
        mem: Arc<Memtable>,
        shard: usize,
        lower: Bound<MemKey>,
        upper: Bound<MemKey>,
    ) -> ShardCursor {
        ShardCursor {
            mem,
            shard,
            lower,
            upper,
            window: Vec::new(),
            pos: 0,
            valid: false,
            next_window: FIRST_WINDOW,
            key: Vec::new(),
        }
    }

    fn sync_key(&mut self) {
        if self.valid {
            self.window[self.pos].0.encode_into(&mut self.key);
        }
    }

    fn after_fill(&mut self, forward: bool) {
        self.valid = !self.window.is_empty();
        self.pos = if forward {
            0
        } else {
            self.window.len().saturating_sub(1)
        };
        self.sync_key();
    }

    fn fill_from_start(&mut self, start: KeyBound<'_>, limit: usize) {
        let start = max_lower(start, as_view(&self.lower));
        let end = as_view(&self.upper);
        fill_window(
            &self.mem,
            self.shard,
            &mut self.window,
            start,
            end,
            true,
            limit,
        );
        self.after_fill(true);
    }

    fn fill_to_end(&mut self, end: KeyBound<'_>, limit: usize) {
        let end = min_upper(end, as_view(&self.upper));
        let start = as_view(&self.lower);
        fill_window(
            &self.mem,
            self.shard,
            &mut self.window,
            start,
            end,
            false,
            limit,
        );
        self.after_fill(false);
    }

    fn grow(&mut self) -> usize {
        let limit = self.next_window;
        self.next_window = (limit * 2).min(MAX_WINDOW);
        limit
    }
}

impl InternalIterator for ShardCursor {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.next_window = FIRST_WINDOW;
        let limit = self.grow();
        self.fill_from_start(Bound::Unbounded, limit);
    }

    fn seek_to_last(&mut self) {
        self.next_window = FIRST_WINDOW;
        let limit = self.grow();
        self.fill_to_end(Bound::Unbounded, limit);
    }

    fn seek(&mut self, target: &[u8]) {
        self.next_window = FIRST_WINDOW;
        let limit = self.grow();
        let probe = Probe::from_internal(target);
        self.fill_from_start(Bound::Included(&probe), limit);
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.next_window = FIRST_WINDOW;
        let limit = self.grow();
        let probe = Probe::from_internal(target);
        self.fill_to_end(Bound::Included(&probe), limit);
    }

    fn next(&mut self) {
        debug_assert!(self.valid);
        if self.pos + 1 < self.window.len() {
            self.pos += 1;
            self.sync_key();
            return;
        }
        let Some((last, _)) = self.window.pop() else {
            self.valid = false;
            return;
        };
        let limit = self.grow();
        self.fill_from_start(Bound::Excluded(&last), limit);
    }

    fn prev(&mut self) {
        debug_assert!(self.valid);
        if self.pos > 0 {
            self.pos -= 1;
            self.sync_key();
            return;
        }
        if self.window.is_empty() {
            self.valid = false;
            return;
        }
        let (first, _) = self.window.swap_remove(0);
        let limit = self.grow();
        self.fill_to_end(Bound::Excluded(&first), limit);
    }

    fn key(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.key
    }

    fn value(&self) -> Bytes {
        debug_assert!(self.valid);
        self.window[self.pos].1.clone()
    }

    fn status(&self) -> Result<()> {
        Ok(())
    }
}
