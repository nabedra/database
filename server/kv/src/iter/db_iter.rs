//! The user-facing iterator: a merged [`InternalIterator`] filtered to one snapshot.
//!
//! It yields each user key once, with its newest version visible at the snapshot, and
//! hides keys whose newest visible version is a point tombstone or is covered by a newer
//! visible range tombstone. Bounds are applied to user keys. The algorithm follows
//! LevelDB's `DBIter`: going forward the inner iterator sits on the yielded entry; going
//! backward it sits just before the yielded key's versions. Direction changes re-seek.

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, FileId, Result};
use crate::iter::ValueResolver;
use crate::iter::range_del::RangeDelAggregator;
use crate::source::InternalIterator;
use crate::types::{Kind, Locator, MAX_SEQ, SEEK_KIND_BYTE, Seq, parse_internal_key};

/// After this many consecutive hidden entries (either direction), or visible versions of
/// one key going backward, re-seek instead of stepping.
const MAX_SEQUENTIAL_SKIPS: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Forward,
    Reverse,
}

fn owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    bound.map(<[u8]>::to_vec)
}

/// `out = user_key ‖ trailer`.
fn put_key(out: &mut Vec<u8>, user_key: &[u8], trailer: u64) {
    out.clear();
    out.extend_from_slice(user_key);
    out.extend_from_slice(&trailer.to_be_bytes());
}

/// The trailer of the first entry visible at `seq` (sorts before every stored kind).
fn visible_trailer(seq: Seq) -> u64 {
    (seq << 8) | u64::from(SEEK_KIND_BYTE)
}

/// The trailer of the last possible entry of a user key: sequence 0, kind 0.
const LAST_TRAILER: u64 = 0;

pub struct DbIter<I: InternalIterator = Box<dyn InternalIterator>> {
    inner: I,
    seq: Seq,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    tombstones: RangeDelAggregator,
    resolver: Option<Arc<dyn ValueResolver>>,
    fill_cache: bool,
    direction: Direction,
    valid: bool,
    /// Current user key (also the key being skipped while searching forward).
    key: Vec<u8>,
    /// Stored value of the current entry.
    value: Bytes,
    kind: Kind,
    target: Vec<u8>,
    error: Option<Error>,
}

impl<I: InternalIterator> DbIter<I> {
    /// An iterator over `inner` at `snapshot_seq`, limited to user keys within the
    /// bounds. It starts unpositioned.
    pub fn new(inner: I, snapshot_seq: Seq, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        DbIter {
            inner,
            seq: snapshot_seq.min(MAX_SEQ),
            lower: owned(lower),
            upper: owned(upper),
            tombstones: RangeDelAggregator::new(snapshot_seq.min(MAX_SEQ)),
            resolver: None,
            fill_cache: true,
            direction: Direction::Forward,
            valid: false,
            key: Vec::new(),
            value: Bytes::new(),
            kind: Kind::Put,
            target: Vec::new(),
            error: None,
        }
    }

    /// The range tombstones of every source of `inner`. Its snapshot should be this
    /// iterator's; tombstones above `snapshot_seq` are never applied either way.
    pub fn with_range_tombstones(mut self, tombstones: RangeDelAggregator) -> Self {
        debug_assert_eq!(tombstones.snapshot(), self.seq);
        self.tombstones = tombstones;
        self
    }

    /// Resolves `JournalPut` values in [`DbIter::value`].
    pub fn with_resolver(mut self, resolver: Arc<dyn ValueResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Recorded for the sources that read blocks (passed through, unused here).
    pub fn with_fill_cache(mut self, fill_cache: bool) -> Self {
        self.fill_cache = fill_cache;
        self
    }

    pub fn fill_cache(&self) -> bool {
        self.fill_cache
    }

    pub fn snapshot_seq(&self) -> Seq {
        self.seq
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// The current user key. Requires `valid()`.
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid);
        &self.key
    }

    /// The current value, read through the resolver for journal values.
    /// Requires `valid()`.
    pub fn value(&self) -> Result<Bytes> {
        debug_assert!(self.valid);
        if self.kind != Kind::JournalPut {
            return Ok(self.value.clone());
        }
        let locator = Locator::decode(&self.value).ok_or(Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "malformed journal value locator",
        })?;
        match &self.resolver {
            Some(resolver) => resolver.resolve(&locator),
            None => Err(Error::InvalidArgument(
                "journal value read without a resolver",
            )),
        }
    }

    /// The stored value without resolving: an encoded locator for a journal value.
    pub fn raw_value(&self) -> &Bytes {
        &self.value
    }

    /// `Put` or `JournalPut`.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn status(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => self.inner.status(),
        }
    }

    fn above_upper(&self, user_key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(u) => user_key > u.as_slice(),
            Bound::Excluded(u) => user_key >= u.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn below_lower(&self, user_key: &[u8]) -> bool {
        match &self.lower {
            Bound::Included(l) => user_key < l.as_slice(),
            Bound::Excluded(l) => user_key <= l.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn fail(&mut self, error: Error) {
        self.error = Some(error);
        self.valid = false;
    }

    fn corrupt_key(&mut self) {
        self.fail(Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "malformed internal key in iterator",
        });
    }

    /// Ends a search because the inner iterator ran out.
    fn exhausted(&mut self) {
        self.valid = false;
        if let Err(error) = self.inner.status() {
            self.error = Some(error);
        }
    }

    pub fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.valid = false;
        match &self.lower {
            Bound::Unbounded => {
                self.inner.seek_to_first();
                self.find_next(false);
            }
            Bound::Included(l) => {
                put_key(&mut self.target, l, visible_trailer(self.seq));
                self.inner.seek(&self.target);
                self.find_next(false);
            }
            Bound::Excluded(l) => {
                put_key(&mut self.target, l, LAST_TRAILER);
                self.key.clear();
                self.key.extend_from_slice(l);
                self.inner.seek(&self.target);
                self.find_next(true);
            }
        }
    }

    pub fn seek_to_last(&mut self) {
        self.direction = Direction::Reverse;
        self.valid = false;
        match &self.upper {
            Bound::Unbounded => self.inner.seek_to_last(),
            Bound::Included(u) => {
                put_key(&mut self.target, u, LAST_TRAILER);
                self.inner.seek_for_prev(&self.target);
            }
            Bound::Excluded(u) => {
                put_key(&mut self.target, u, u64::MAX);
                self.inner.seek_for_prev(&self.target);
            }
        }
        self.find_prev();
    }

    /// Positions at the first user key `>= target` within the bounds.
    pub fn seek(&mut self, target: &[u8]) {
        if self.below_lower(target) {
            return self.seek_to_first();
        }
        self.direction = Direction::Forward;
        self.valid = false;
        if self.above_upper(target) {
            return;
        }
        put_key(&mut self.target, target, visible_trailer(self.seq));
        self.inner.seek(&self.target);
        self.find_next(false);
    }

    /// Positions at the last user key `<= target` within the bounds.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        if self.above_upper(target) {
            return self.seek_to_last();
        }
        self.direction = Direction::Reverse;
        self.valid = false;
        if self.below_lower(target) {
            return;
        }
        put_key(&mut self.target, target, LAST_TRAILER);
        self.inner.seek_for_prev(&self.target);
        self.find_prev();
    }

    /// Requires `valid()`.
    pub fn next(&mut self) {
        debug_assert!(self.valid);
        if self.direction == Direction::Reverse {
            // The inner iterator is before this key's versions: jump past all of them.
            self.direction = Direction::Forward;
            put_key(&mut self.target, &self.key, LAST_TRAILER);
            self.inner.seek(&self.target);
        } else {
            self.inner.next();
        }
        self.find_next(true);
    }

    /// Requires `valid()`.
    pub fn prev(&mut self) {
        debug_assert!(self.valid);
        if self.direction == Direction::Forward {
            // The inner iterator is on this key's newest visible version: go before
            // every version of it.
            self.direction = Direction::Reverse;
            put_key(&mut self.target, &self.key, u64::MAX);
            self.inner.seek_for_prev(&self.target);
        }
        self.find_prev();
    }

    /// Moves forward to the newest visible version of the next live user key. With
    /// `skipping`, versions of user keys `<= self.key` are passed over.
    fn find_next(&mut self, mut skipping: bool) {
        self.valid = false;
        let mut skipped = 0;
        while self.inner.valid() {
            let Some(parsed) = parse_internal_key(self.inner.key()) else {
                return self.corrupt_key();
            };
            if self.above_upper(parsed.user_key) {
                return;
            }
            let hidden_by_snapshot = parsed.seq > self.seq;
            let already_seen = skipping && parsed.user_key <= self.key.as_slice();
            if !hidden_by_snapshot && !already_seen {
                match parsed.kind {
                    Kind::Put | Kind::JournalPut => {
                        let covered = self.tombstones.covers(parsed.user_key, parsed.seq);
                        self.key.clear();
                        self.key.extend_from_slice(parsed.user_key);
                        if !covered {
                            self.kind = parsed.kind;
                            self.value = self.inner.value();
                            self.valid = true;
                            return;
                        }
                        skipping = true;
                    }
                    Kind::Delete => {
                        self.key.clear();
                        self.key.extend_from_slice(parsed.user_key);
                        skipping = true;
                    }
                    Kind::RangeDelete => return self.corrupt_key(),
                }
                skipped = 0;
                self.inner.next();
                continue;
            }
            skipped += 1;
            if skipped > MAX_SEQUENTIAL_SKIPS {
                // Many hidden versions in a row: seek past them.
                skipped = 0;
                if hidden_by_snapshot && !already_seen {
                    put_key(&mut self.target, parsed.user_key, visible_trailer(self.seq));
                } else {
                    put_key(&mut self.target, &self.key, LAST_TRAILER);
                }
                self.inner.seek(&self.target);
                // A seek to the last trailer lands on that entry if it exists; the
                // next pass skips it.
                continue;
            }
            self.inner.next();
        }
        self.exhausted();
    }

    /// Moves backward to the newest visible version of the previous live user key.
    /// Leaves the inner iterator just before that key's versions.
    ///
    /// Going backward a key's versions arrive oldest first, so the newest visible one is
    /// the last seen. After more than [`MAX_SEQUENTIAL_SKIPS`] visible versions of one key
    /// it seeks straight to the newest instead of stepping through the rest, and after as
    /// many versions above the snapshot in a row it seeks before their key: every version
    /// of that key still ahead is newer, so hidden too.
    fn find_prev(&mut self) {
        self.valid = false;
        let mut found = false;
        // Whether `self.key` holds a key met in this search.
        let mut have_key = false;
        // Visible versions of `self.key`, and versions above the snapshot, met in a row.
        let mut versions = 0;
        let mut hidden = 0;
        while self.inner.valid() {
            let Some(parsed) = parse_internal_key(self.inner.key()) else {
                return self.corrupt_key();
            };
            if self.below_lower(parsed.user_key) {
                break;
            }
            if parsed.seq > self.seq {
                hidden += 1;
                if hidden > MAX_SEQUENTIAL_SKIPS {
                    hidden = 0;
                    put_key(&mut self.target, parsed.user_key, u64::MAX);
                    self.inner.seek_for_prev(&self.target);
                } else {
                    self.inner.prev();
                }
                continue;
            }
            hidden = 0;
            if found && parsed.user_key.cmp(&self.key) == Ordering::Less {
                // The versions of the found key are all behind us.
                break;
            }
            let same_key = have_key && parsed.user_key == self.key.as_slice();
            versions = if same_key { versions + 1 } else { 1 };
            if versions > MAX_SEQUENTIAL_SKIPS {
                // Many versions of `self.key`: read its newest visible one directly, then
                // go before all of its versions.
                versions = 0;
                put_key(&mut self.target, &self.key, visible_trailer(self.seq));
                self.inner.seek(&self.target);
                if !self.inner.valid() {
                    return self.lost_visible_version();
                }
                let Some(newest) = parse_internal_key(self.inner.key()) else {
                    return self.corrupt_key();
                };
                if newest.user_key != self.key.as_slice() || newest.seq > self.seq {
                    return self.lost_visible_version();
                }
                found = match newest.kind {
                    Kind::Put | Kind::JournalPut => {
                        !self.tombstones.covers(newest.user_key, newest.seq)
                    }
                    Kind::Delete => false,
                    Kind::RangeDelete => return self.corrupt_key(),
                };
                if found {
                    self.kind = newest.kind;
                    self.value = self.inner.value();
                }
                put_key(&mut self.target, &self.key, u64::MAX);
                self.inner.seek_for_prev(&self.target);
                continue;
            }
            let live = match parsed.kind {
                Kind::Put | Kind::JournalPut => {
                    !self.tombstones.covers(parsed.user_key, parsed.seq)
                }
                Kind::Delete => false,
                Kind::RangeDelete => return self.corrupt_key(),
            };
            // Versions of one key arrive oldest first, so the last one seen wins.
            self.key.clear();
            self.key.extend_from_slice(parsed.user_key);
            have_key = true;
            if live {
                self.kind = parsed.kind;
                self.value = self.inner.value();
            }
            found = live;
            self.inner.prev();
        }
        if let Err(error) = self.inner.status() {
            return self.fail(error);
        }
        self.valid = found;
    }

    /// A re-seek to a visible version just stepped over did not find it: an inner
    /// error, or sources that changed under the iterator.
    fn lost_visible_version(&mut self) {
        match self.inner.status() {
            Err(error) => self.fail(error),
            Ok(()) => self.fail(Error::Corruption {
                file: FileId::Other,
                offset: 0,
                what: "iterator lost a visible version on a re-seek",
            }),
        }
    }
}
