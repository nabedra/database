//! Test helpers: a seeded generator, a sorted-vector source and a fake journal resolver.

use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::crc::crc32c;
use crate::error::{Error, FileId, Result};
use crate::iter::ValueResolver;
use crate::source::InternalIterator;
use crate::types::{Locator, compare_internal_keys};

/// SplitMix64.
#[derive(Clone, Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub(crate) fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// An [`InternalIterator`] over a sorted vector, standing in for a sorted run.
pub(crate) struct VecIter {
    entries: Arc<Vec<(Vec<u8>, Bytes)>>,
    pos: Option<usize>,
    error: Option<Error>,
}

impl VecIter {
    pub(crate) fn new(entries: Vec<(Vec<u8>, Bytes)>) -> VecIter {
        VecIter::shared(Arc::new(entries))
    }

    pub(crate) fn shared(entries: Arc<Vec<(Vec<u8>, Bytes)>>) -> VecIter {
        debug_assert!(
            entries
                .windows(2)
                .all(|w| compare_internal_keys(&w[0].0, &w[1].0) == Ordering::Less)
        );
        VecIter {
            entries,
            pos: None,
            error: None,
        }
    }

    /// A source whose every positioning fails with `error`.
    pub(crate) fn failing(error: Error) -> VecIter {
        VecIter {
            entries: Arc::new(Vec::new()),
            pos: None,
            error: Some(error),
        }
    }

    fn at(&mut self, i: Option<usize>) {
        self.pos = if self.error.is_some() { None } else { i };
    }
}

impl InternalIterator for VecIter {
    fn valid(&self) -> bool {
        self.pos.is_some()
    }
    fn seek_to_first(&mut self) {
        self.at((!self.entries.is_empty()).then_some(0));
    }
    fn seek_to_last(&mut self) {
        self.at(self.entries.len().checked_sub(1));
    }
    fn seek(&mut self, target: &[u8]) {
        let i = self
            .entries
            .partition_point(|e| compare_internal_keys(&e.0, target) == Ordering::Less);
        self.at((i < self.entries.len()).then_some(i));
    }
    fn seek_for_prev(&mut self, target: &[u8]) {
        let i = self
            .entries
            .partition_point(|e| compare_internal_keys(&e.0, target) != Ordering::Greater);
        self.at(i.checked_sub(1));
    }
    fn next(&mut self) {
        let next = self.pos.map(|p| p + 1).filter(|&p| p < self.entries.len());
        self.at(next);
    }
    fn prev(&mut self) {
        let prev = self.pos.and_then(|p| p.checked_sub(1));
        self.at(prev);
    }
    fn key(&self) -> &[u8] {
        &self.entries[self.pos.expect("valid")].0
    }
    fn value(&self) -> Bytes {
        self.entries[self.pos.expect("valid")].1.clone()
    }
    fn status(&self) -> Result<()> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

/// Stores journal values in memory; a locator's `offset` is the value's index.
#[derive(Default)]
pub(crate) struct FakeResolver {
    values: Mutex<Vec<Bytes>>,
}

impl FakeResolver {
    pub(crate) fn store(&self, value: &[u8]) -> Locator {
        let mut values = self.values.lock().unwrap();
        values.push(Bytes::copy_from_slice(value));
        Locator {
            segment: 1,
            epoch: 1,
            offset: values.len() as u64 - 1,
            len: value.len() as u32,
            crc32c: crc32c(value),
        }
    }
}

impl ValueResolver for FakeResolver {
    fn resolve(&self, locator: &Locator) -> Result<Bytes> {
        let values = self.values.lock().unwrap();
        let value = values
            .get(locator.offset as usize)
            .filter(|v| v.len() == locator.len as usize && crc32c(v) == locator.crc32c)
            .ok_or(Error::Corruption {
                file: FileId::JournalValue {
                    segment: locator.segment,
                },
                offset: locator.offset,
                what: "fake journal value mismatch",
            })?;
        Ok(value.clone())
    }
}
