//! A lazy iterator over one level ≥ 1 (RV-KV2 finding 8): the level's runs are sorted
//! and disjoint, so only the run under the cursor is open. A store-wide scan therefore
//! holds one run per level, not every run it will pass, and the table cache's limit on
//! open runs bounds live iterators too. The merge above it is O(log levels) per step.

use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::run::RunIter;
use crate::source::InternalIterator;
use crate::types::user_key;
use crate::version::RunFile;

use super::Inner;

pub(super) struct LevelIter {
    inner: Arc<Inner>,
    /// Sorted by key, disjoint.
    runs: Vec<Arc<RunFile>>,
    fill_cache: bool,
    at: usize,
    cur: Option<RunIter>,
    error: Option<Error>,
}

impl LevelIter {
    pub fn new(inner: Arc<Inner>, runs: Vec<Arc<RunFile>>, fill_cache: bool) -> LevelIter {
        LevelIter {
            inner,
            runs,
            fill_cache,
            at: 0,
            cur: None,
            error: None,
        }
    }

    /// Positions on run `i` (unpositioned). False when it could not be opened.
    fn open(&mut self, i: usize) -> bool {
        self.at = i;
        self.cur = None;
        match self.inner.table_cache.get(self.runs[i].id()) {
            Ok(run) => {
                self.cur = Some(run.iter(self.fill_cache));
                true
            }
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }

    fn cur_ok(&self) -> bool {
        self.cur
            .as_ref()
            .is_some_and(|c| !c.valid() && c.status().is_ok())
    }

    fn skip_forward(&mut self) {
        while self.cur_ok() && self.at + 1 < self.runs.len() {
            if !self.open(self.at + 1) {
                return;
            }
            self.cur.as_mut().expect("opened").seek_to_first();
        }
    }

    fn skip_backward(&mut self) {
        while self.cur_ok() && self.at > 0 {
            if !self.open(self.at - 1) {
                return;
            }
            self.cur.as_mut().expect("opened").seek_to_last();
        }
    }

    fn reset(&mut self) {
        self.cur = None;
        self.error = None;
    }
}

impl InternalIterator for LevelIter {
    fn valid(&self) -> bool {
        self.error.is_none() && self.cur.as_ref().is_some_and(|c| c.valid())
    }

    fn seek_to_first(&mut self) {
        self.reset();
        if !self.runs.is_empty() && self.open(0) {
            self.cur.as_mut().expect("opened").seek_to_first();
            self.skip_forward();
        }
    }

    fn seek_to_last(&mut self) {
        self.reset();
        if !self.runs.is_empty() && self.open(self.runs.len() - 1) {
            self.cur.as_mut().expect("opened").seek_to_last();
            self.skip_backward();
        }
    }

    fn seek(&mut self, target: &[u8]) {
        self.reset();
        let key = user_key(target);
        let i = self.runs.partition_point(|r| {
            if r.hi_exclusive {
                r.hi.as_ref() <= key
            } else {
                r.hi.as_ref() < key
            }
        });
        if i < self.runs.len() && self.open(i) {
            self.cur.as_mut().expect("opened").seek(target);
            self.skip_forward();
        }
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.reset();
        let key = user_key(target);
        let i = self.runs.partition_point(|r| r.lo.as_ref() <= key);
        if i > 0 && self.open(i - 1) {
            self.cur.as_mut().expect("opened").seek_for_prev(target);
            self.skip_backward();
        }
    }

    fn next(&mut self) {
        if let Some(cur) = self.cur.as_mut() {
            cur.next();
            self.skip_forward();
        }
    }

    fn prev(&mut self) {
        if let Some(cur) = self.cur.as_mut() {
            cur.prev();
            self.skip_backward();
        }
    }

    fn key(&self) -> &[u8] {
        self.cur.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.cur.as_ref().expect("valid").value()
    }

    fn status(&self) -> Result<()> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        match &self.cur {
            Some(cur) => cur.status(),
            None => Ok(()),
        }
    }
}
