//! Block-max inverted lists. One format serves BM25 and sparse vectors: a posting is a document of
//! a segment and a value, the count of a term in a text or the weight of an index, and the
//! segment's entry table holds each document's norm, its number of words or its vector length.
//!
//! A list is blocks of [`BLOCK`] postings. The summary of every block (its last document, the
//! largest and smallest value and value per norm, and the smallest norm) stays in RAM, so that a
//! scan can skip a block without reading it. The postings of full blocks go to a [`PostingStore`],
//! RAM today and NVMe later. The open block at the end of a list is in RAM.

use std::collections::HashMap;
use std::sync::Arc;

use super::rows::{Gen, Slot};
use crate::types::{Error, Pos, Result};

/// A dictionary id of a text term, or the index of a sparse vector.
pub type Term = u32;
/// The place of an entry in its segment.
pub type Doc = u32;

pub const BLOCK: usize = 128;
const POSTING_BYTES: usize = 8;

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Posting {
    pub doc: Doc,
    pub value: f32,
}

/// Where a store keeps one block.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct BlockRef(pub u64);

/// Keeps the postings of full blocks. A store detects damage of what it returns, and `get` fails
/// then: the index never reads a damaged block as postings.
pub trait PostingStore: Send + Sync {
    fn put(&mut self, bytes: &[u8]) -> Result<BlockRef>;
    fn get(&self, block: BlockRef, out: &mut Vec<u8>) -> Result<()>;
    fn free(&mut self, block: BlockRef);
}

/// Blocks in RAM.
#[derive(Clone, Default)]
pub struct RamStore {
    blocks: Vec<Option<Box<[u8]>>>,
    free: Vec<u64>,
}

impl RamStore {
    pub fn blocks(&self) -> usize {
        self.blocks.len() - self.free.len()
    }
}

impl PostingStore for RamStore {
    fn put(&mut self, bytes: &[u8]) -> Result<BlockRef> {
        let bytes = Some(bytes.into());
        Ok(BlockRef(match self.free.pop() {
            Some(i) => {
                self.blocks[i as usize] = bytes;
                i
            }
            None => {
                self.blocks.push(bytes);
                self.blocks.len() as u64 - 1
            }
        }))
    }

    fn get(&self, block: BlockRef, out: &mut Vec<u8>) -> Result<()> {
        let bytes = usize::try_from(block.0)
            .ok()
            .and_then(|i| self.blocks.get(i)?.as_deref())
            .ok_or(Error::Internal("a posting block is gone"))?;
        out.clear();
        out.extend_from_slice(bytes);
        Ok(())
    }

    fn free(&mut self, block: BlockRef) {
        if let Some(b) = self.blocks.get_mut(block.0 as usize)
            && b.take().is_some()
        {
            self.free.push(block.0);
        }
    }
}

fn encode(postings: &[Posting]) -> Vec<u8> {
    let mut out = Vec::with_capacity(postings.len() * POSTING_BYTES);
    for p in postings {
        out.extend(p.doc.to_le_bytes());
        out.extend(p.value.to_le_bytes());
    }
    out
}

fn decode(bytes: &[u8], n: usize, out: &mut Vec<Posting>) -> Result<()> {
    if bytes.len() != n * POSTING_BYTES {
        return Err(damaged());
    }
    out.clear();
    for c in bytes.chunks_exact(POSTING_BYTES) {
        let doc = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let value = f32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        if out.last().is_some_and(|p: &Posting| p.doc >= doc) || !value.is_finite() {
            return Err(damaged());
        }
        out.push(Posting { doc, value });
    }
    Ok(())
}

fn damaged() -> Error {
    Error::Io("a posting block is damaged".to_owned())
}

/// What any posting of a block or a list can contribute.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Bounds {
    pub max: f32,
    pub min: f32,
    pub max_per_norm: f64,
    pub min_per_norm: f64,
    pub min_norm: f64,
}

impl Bounds {
    const EMPTY: Bounds = Bounds {
        max: f32::NEG_INFINITY,
        min: f32::INFINITY,
        max_per_norm: f64::NEG_INFINITY,
        min_per_norm: f64::INFINITY,
        min_norm: f64::INFINITY,
    };

    fn add(&mut self, value: f32, norm: f64) {
        let per = if norm > 0.0 {
            f64::from(value) / norm
        } else {
            0.0
        };
        self.max = self.max.max(value);
        self.min = self.min.min(value);
        self.max_per_norm = self.max_per_norm.max(per);
        self.min_per_norm = self.min_per_norm.min(per);
        self.min_norm = self.min_norm.min(norm);
    }
}

#[derive(Clone)]
struct Block {
    at: BlockRef,
    last: Doc,
    len: u16,
    bounds: Bounds,
}

#[derive(Clone)]
pub struct List {
    blocks: Vec<Block>,
    tail: Vec<Posting>,
    tail_bounds: Bounds,
    pub all: Bounds,
    pub len: usize,
}

impl List {
    fn new() -> List {
        List {
            blocks: Vec::new(),
            tail: Vec::new(),
            tail_bounds: Bounds::EMPTY,
            all: Bounds::EMPTY,
            len: 0,
        }
    }

    fn push<S: PostingStore>(&mut self, store: &mut S, p: Posting, norm: f64) -> Result<()> {
        self.tail.push(p);
        self.tail_bounds.add(p.value, norm);
        self.all.add(p.value, norm);
        self.len += 1;
        if self.tail.len() == BLOCK {
            let at = store.put(&encode(&self.tail))?;
            self.blocks.push(Block {
                at,
                last: p.doc,
                len: BLOCK as u16,
                bounds: self.tail_bounds,
            });
            self.tail.clear();
            self.tail_bounds = Bounds::EMPTY;
        }
        Ok(())
    }

    fn free<S: PostingStore>(&self, store: &mut S) {
        for b in &self.blocks {
            store.free(b.at);
        }
    }

    /// The summary of block `i`, the open one at `blocks.len()`.
    fn summary(&self, i: usize) -> Option<(Doc, &Bounds)> {
        match self.blocks.get(i) {
            Some(b) => Some((b.last, &b.bounds)),
            None if i == self.blocks.len() => Some((self.tail.last()?.doc, &self.tail_bounds)),
            None => None,
        }
    }
}

/// Reads one list forwards.
pub struct Cursor<'a> {
    list: &'a List,
    block: usize,
    /// The postings of `block` once read, and the place in them.
    buf: Vec<Posting>,
    loaded: bool,
    i: usize,
    bytes: Vec<u8>,
}

impl<'a> Cursor<'a> {
    pub fn new(list: &'a List) -> Cursor<'a> {
        Cursor {
            list,
            block: 0,
            buf: Vec::new(),
            loaded: false,
            i: 0,
            bytes: Vec::new(),
        }
    }

    fn load<S: PostingStore>(&mut self, store: &S) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        match self.list.blocks.get(self.block) {
            Some(b) => {
                store.get(b.at, &mut self.bytes)?;
                decode(&self.bytes, b.len as usize, &mut self.buf)?;
                if self.buf.last().map(|p| p.doc) != Some(b.last) {
                    return Err(damaged());
                }
            }
            None => {
                self.buf.clear();
                self.buf.extend_from_slice(&self.list.tail);
            }
        }
        self.loaded = true;
        Ok(())
    }

    fn next_block(&mut self) {
        self.block += 1;
        self.loaded = false;
        self.i = 0;
    }

    /// The current posting, `None` at the end.
    pub fn current<S: PostingStore>(&mut self, store: &S) -> Result<Option<Posting>> {
        loop {
            if self.block > self.list.blocks.len() {
                return Ok(None);
            }
            self.load(store)?;
            if let Some(p) = self.buf.get(self.i) {
                return Ok(Some(*p));
            }
            if self.block == self.list.blocks.len() {
                self.block += 1;
                return Ok(None);
            }
            self.next_block();
        }
    }

    pub fn advance(&mut self) {
        self.i += 1;
    }

    /// Moves to the block that may hold `doc` without reading any, and returns its bounds. `None`:
    /// no posting at or after `doc`.
    pub fn shallow(&mut self, doc: Doc) -> Option<&'a Bounds> {
        loop {
            let (last, _) = self.list.summary(self.block)?;
            if last >= doc {
                return self.list.summary(self.block).map(|(_, b)| b);
            }
            self.next_block();
        }
    }

    /// The bounds and the last document of the current block.
    pub fn block(&self) -> Option<(Doc, &'a Bounds)> {
        self.list.summary(self.block)
    }

    /// The first posting at or after `doc`.
    pub fn seek<S: PostingStore>(&mut self, doc: Doc, store: &S) -> Result<Option<Posting>> {
        if self.shallow(doc).is_none() {
            self.block = self.list.blocks.len() + 1;
            return Ok(None);
        }
        self.load(store)?;
        let rest = &self.buf[self.i.min(self.buf.len())..];
        self.i += rest.partition_point(|p| p.doc < doc);
        self.current(store)
    }
}

/// An entry instance: a version of a row in one segment.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Entry {
    pub slot: Slot,
    pub at: Pos,
    pub generation: Gen,
    pub norm: f64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SegState {
    Ready,
    /// Merged into another one. It waits for the readers that began before.
    Retired {
        waits: u64,
    },
}

#[derive(Clone)]
pub struct Segment {
    pub state: SegState,
    pub entries: Vec<Entry>,
    pub lists: HashMap<Term, List>,
}

impl Segment {
    pub fn new() -> Segment {
        Segment {
            state: SegState::Ready,
            entries: Vec::new(),
            lists: HashMap::new(),
        }
    }

    /// Appends an entry and its postings. Returns the terms that got a list here.
    pub fn push<S: PostingStore>(
        &mut self,
        store: &mut S,
        entry: Entry,
        terms: &[(Term, f32)],
        new_terms: &mut Vec<Term>,
    ) -> Result<()> {
        let doc =
            Doc::try_from(self.entries.len()).map_err(|_| Error::Internal("a segment is full"))?;
        for &(term, value) in terms {
            let list = self.lists.entry(term).or_insert_with(|| {
                new_terms.push(term);
                List::new()
            });
            list.push(store, Posting { doc, value }, entry.norm)?;
        }
        self.entries.push(entry);
        Ok(())
    }

    /// Gives the blocks back. Returns the terms it had.
    pub fn free<S: PostingStore>(&self, store: &mut S) -> impl Iterator<Item = Term> + '_ {
        for list in self.lists.values() {
            list.free(store);
        }
        self.lists.keys().copied()
    }
}

/// The text terms that some segment has a list for, by name and id. An id is used again only
/// once no segment holds it.
#[derive(Clone, Default)]
pub struct Dict {
    ids: HashMap<Arc<str>, Term>,
    names: Vec<Option<(Arc<str>, u64)>>,
    free: Vec<Term>,
}

impl Dict {
    pub fn get(&self, name: &str) -> Option<Term> {
        self.ids.get(name).copied()
    }

    pub fn name(&self, term: Term) -> Option<&Arc<str>> {
        Some(&self.names.get(term as usize)?.as_ref()?.0)
    }

    /// The id of a name, new or not. A new one is held by nothing yet.
    pub fn intern(&mut self, name: &str) -> Result<Term> {
        if let Some(t) = self.get(name) {
            return Ok(t);
        }
        let name: Arc<str> = name.into();
        let term = match self.free.pop() {
            Some(t) => {
                self.names[t as usize] = Some((name.clone(), 0));
                t
            }
            None => {
                let t = Term::try_from(self.names.len())
                    .map_err(|_| Error::Internal("the dictionary is full"))?;
                self.names.push(Some((name.clone(), 0)));
                t
            }
        };
        self.ids.insert(name, term);
        Ok(term)
    }

    pub fn hold(&mut self, term: Term) {
        if let Some(Some((_, n))) = self.names.get_mut(term as usize) {
            *n += 1;
        }
    }

    /// One holder fewer. An id that nothing holds is removed.
    pub fn release(&mut self, term: Term) {
        let Some(slot) = self.names.get_mut(term as usize) else {
            return;
        };
        if let Some((name, n)) = slot {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.ids.remove(name);
                *slot = None;
                self.free.push(term);
            }
        }
    }

    /// Removes the interned ids that nothing came to hold.
    pub fn sweep(&mut self, terms: &[Term]) {
        for &t in terms {
            if matches!(self.names.get(t as usize), Some(Some((_, 0)))) {
                self.hold(t);
                self.release(t);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }
}
