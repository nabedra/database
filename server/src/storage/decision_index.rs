//! OLAP-1: an immutable, content-addressed UUID index over committed decisions.
//!
//! The index is the canonical radix tree over the 16 bytes of a request UUID.
//! A page at depth `d` has up to 256 slots keyed by byte `d` of the UUID. A
//! slot holds an entry (the full request UUID and its decision UUID) when
//! exactly one indexed UUID has that prefix, and otherwise a child page at
//! depth `d + 1`. The tree is a function of its entry set alone, so two writers
//! that index the same decisions produce the same bytes. Each page is named by
//! the SHA-256 of its bytes and is written once with `Create`; a reader checks
//! the name, the depth and every entry's route prefix before using a page.
//!
//! A format 6 catalog root names the root page next to the decision head, and
//! both change in the one conditional root PUT of a commit. A lookup reads at
//! most 16 pages and one decision; a commit writes the copied path (at most 17
//! pages). Absence is proved only by a validated path with no matching UUID.
use super::{Object, ObjectKey, ObjectStore, Storage, StorageError, WriteCondition};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use nabedra_rt::time::{Instant, timeout_at};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const PAGE_MAGIC: &[u8; 4] = b"NBDX";
const CHUNK_MAGIC: &[u8; 4] = b"NBDC";
const VERSION: u8 = 1;
const PAGE_HEADER: usize = 8;
const SLOT_BYTES: usize = 34;
/// The largest valid page: 256 slots.
pub const MAX_INDEX_PAGE_BYTES: usize = PAGE_HEADER + 256 * SLOT_BYTES;
/// Byte 15 is the last UUID byte, so a page there holds only entries.
const LAST_DEPTH: usize = 15;
/// Pages a commit or migration uploads at once.
const PUT_CONCURRENCY: usize = 16;
/// Decisions per migration chunk. Every chunk but the last is full.
pub(crate) const CHUNK_ENTRIES: usize = 1024;
const CHUNK_HEADER: usize = 4 + 1 + 16 + 4 + 16 + 8 + 2 + 1 + 16 + 8;
const MAX_CHUNK_BYTES: usize = CHUNK_HEADER + CHUNK_ENTRIES * 32;
/// A migration holds every entry in memory once, 48 bytes each.
const MAX_MIGRATION_ENTRIES: usize = 1 << 22;
/// Decoded pages kept per Storage, by encoded size.
const CACHE_BYTES: usize = 1 << 20;

pub type PageHash = [u8; 32];

pub(crate) fn page_hash(bytes: &[u8]) -> PageHash {
    let digest = Sha256::digest(bytes);
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(&digest);
    hash
}
pub(crate) fn hex(hash: &PageHash) -> String {
    hash.iter().map(|b| format!("{b:02x}")).collect()
}
/// Strict lowercase 64-digit hex, the only spelling a root or key may carry.
pub(crate) fn parse_hex(text: &str) -> Option<PageHash> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut hash = [0_u8; 32];
    for (i, out) in hash.iter_mut().enumerate() {
        *out = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(hash)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Slot {
    Entry { request: Uuid, decision: Uuid },
    Child(PageHash),
}

/// One validated page. Slots are sorted by their key byte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Page {
    depth: usize,
    slots: Vec<(u8, Slot)>,
}
impl Page {
    fn get(&self, key: u8) -> Option<Slot> {
        self.slots
            .binary_search_by_key(&key, |(k, _)| *k)
            .ok()
            .map(|i| self.slots[i].1)
    }
    fn set(&mut self, key: u8, slot: Slot) {
        match self.slots.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.slots[i].1 = slot,
            Err(i) => self.slots.insert(i, (key, slot)),
        }
    }
    /// A decoded page is valid under one route prefix only.
    fn check_route(&self, prefix: &[u8]) -> Result<(), StorageError> {
        let routed = self.slots.iter().all(|(_, slot)| match slot {
            Slot::Entry { request, .. } => &request.as_bytes()[..self.depth] == prefix,
            Slot::Child(_) => true,
        });
        if self.depth == prefix.len() && routed {
            Ok(())
        } else {
            Err(StorageError::InvalidCatalog)
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(PAGE_HEADER + self.slots.len() * SLOT_BYTES);
        bytes.extend_from_slice(PAGE_MAGIC);
        bytes.push(VERSION);
        bytes.push(self.depth as u8);
        bytes.extend_from_slice(&(self.slots.len() as u16).to_be_bytes());
        for (key, slot) in &self.slots {
            bytes.push(*key);
            match slot {
                Slot::Entry { request, decision } => {
                    bytes.push(1);
                    bytes.extend_from_slice(request.as_bytes());
                    bytes.extend_from_slice(decision.as_bytes());
                }
                Slot::Child(hash) => {
                    bytes.push(2);
                    bytes.extend_from_slice(hash);
                }
            }
        }
        bytes
    }
    /// Decode and check one page against the depth and the UUID prefix of the
    /// path that reached it. Also refuses non-canonical shapes.
    fn decode(bytes: &[u8], depth: usize, prefix: &[u8]) -> Result<Self, StorageError> {
        let invalid = StorageError::InvalidCatalog;
        if bytes.len() < PAGE_HEADER
            || bytes.len() > MAX_INDEX_PAGE_BYTES
            || &bytes[..4] != PAGE_MAGIC
            || bytes[4] != VERSION
            || usize::from(bytes[5]) != depth
            || depth > LAST_DEPTH
            || prefix.len() != depth
        {
            return Err(invalid);
        }
        let count = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
        if count == 0 || count > 256 || bytes.len() != PAGE_HEADER + count * SLOT_BYTES {
            return Err(invalid);
        }
        let mut slots = Vec::with_capacity(count);
        for raw in bytes[PAGE_HEADER..].chunks_exact(SLOT_BYTES) {
            let key = raw[0];
            if slots.last().is_some_and(|(last, _)| *last >= key) {
                return Err(invalid);
            }
            let slot = match raw[1] {
                1 => {
                    let request = Uuid::from_slice(&raw[2..18]).map_err(|_| invalid)?;
                    let decision = Uuid::from_slice(&raw[18..34]).map_err(|_| invalid)?;
                    let route = request.as_bytes();
                    if &route[..depth] != prefix || route[depth] != key || decision.is_nil() {
                        return Err(invalid);
                    }
                    Slot::Entry { request, decision }
                }
                2 if depth < LAST_DEPTH => {
                    let mut hash = [0_u8; 32];
                    hash.copy_from_slice(&raw[2..34]);
                    Slot::Child(hash)
                }
                _ => return Err(invalid),
            };
            slots.push((key, slot));
        }
        // A lone entry below the root belongs in its parent's slot.
        if depth > 0 && count == 1 && matches!(slots[0].1, Slot::Entry { .. }) {
            return Err(invalid);
        }
        Ok(Self { depth, slots })
    }
}

/// Pages made by a commit or a migration, not yet uploaded.
#[derive(Default)]
pub(crate) struct NewPages {
    pages: Vec<(PageHash, Vec<u8>, Arc<Page>)>,
}
impl NewPages {
    fn push(&mut self, page: Page) -> PageHash {
        let bytes = page.encode();
        let hash = page_hash(&bytes);
        self.pages.push((hash, bytes, Arc::new(page)));
        hash
    }
}

/// The canonical subtree of `entries` at `depth`. Entries are sorted by
/// request UUID, unique, nonempty and share the first `depth` bytes.
fn build(
    entries: &[(Uuid, Uuid)],
    depth: usize,
    out: &mut NewPages,
) -> Result<PageHash, StorageError> {
    let mut page = Page {
        depth,
        slots: Vec::new(),
    };
    let mut start = 0;
    while start < entries.len() {
        let key = entries[start].0.as_bytes()[depth];
        let end = start
            + entries[start..]
                .iter()
                .take_while(|(request, _)| request.as_bytes()[depth] == key)
                .count();
        let group = &entries[start..end];
        let slot = if let [(request, decision)] = group {
            Slot::Entry {
                request: *request,
                decision: *decision,
            }
        } else if depth < LAST_DEPTH {
            Slot::Child(build(group, depth + 1, out)?)
        } else {
            // Two entries with one full UUID: the caller did not deduplicate.
            return Err(StorageError::InvalidCatalog);
        };
        page.slots.push((key, slot));
        start = end;
    }
    if page.slots.is_empty() {
        return Err(StorageError::InvalidCatalog);
    }
    Ok(out.push(page))
}

/// The canonical index of a set of `(request, decision)` pairs. When a request
/// UUID repeats, the first pair wins: callers list the newest decision first.
pub(crate) fn build_index(
    entries: impl IntoIterator<Item = (Uuid, Uuid)>,
) -> Result<Option<(PageHash, NewPages)>, StorageError> {
    let mut seen = HashSet::new();
    let mut unique: Vec<_> = entries
        .into_iter()
        .filter(|(request, _)| seen.insert(*request))
        .collect();
    if unique.is_empty() {
        return Ok(None);
    }
    unique.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut out = NewPages::default();
    let root = build(&unique, 0, &mut out)?;
    Ok(Some((root, out)))
}

#[derive(Default)]
struct CacheInner {
    pages: HashMap<PageHash, Arc<Page>>,
    order: VecDeque<(PageHash, usize)>,
    bytes: usize,
}
/// Validated pages by content hash. A hit still checks its route prefix.
#[derive(Default)]
pub(crate) struct PageCache {
    inner: Mutex<CacheInner>,
}
impl PageCache {
    fn get(&self, hash: &PageHash) -> Option<Arc<Page>> {
        self.inner.lock().ok()?.pages.get(hash).cloned()
    }
    fn insert(&self, hash: PageHash, size: usize, page: Arc<Page>) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if inner.pages.contains_key(&hash) {
            return;
        }
        inner.pages.insert(hash, page);
        inner.order.push_back((hash, size));
        inner.bytes += size;
        while inner.bytes > CACHE_BYTES {
            let Some((old, size)) = inner.order.pop_front() else {
                break;
            };
            inner.pages.remove(&old);
            inner.bytes -= size;
        }
    }
}

/// The work a lookup did, reused by the commit that follows it.
pub(crate) enum IndexBase {
    /// A format 6 root and its index (None: no decisions yet).
    Index(Option<PageHash>),
    /// A legacy fenced root whose whole chain was read: newest first.
    Chain(Vec<(Uuid, Uuid)>),
    /// A format 3 root: no index is written.
    Unindexed,
}

/// Counted page and decision reads of one lookup, inside the caller's caps.
pub(crate) struct Reads {
    pub(crate) records: usize,
    pub(crate) bytes: usize,
}

struct Chunk {
    start: Uuid,
    start_revision: u64,
    next: Option<(Uuid, u64)>,
    entries: Vec<(Uuid, Uuid)>,
}
impl Chunk {
    fn encode(&self, head: Uuid, ordinal: u32) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CHUNK_HEADER + self.entries.len() * 32);
        bytes.extend_from_slice(CHUNK_MAGIC);
        bytes.push(VERSION);
        bytes.extend_from_slice(head.as_bytes());
        bytes.extend_from_slice(&ordinal.to_be_bytes());
        bytes.extend_from_slice(self.start.as_bytes());
        bytes.extend_from_slice(&self.start_revision.to_be_bytes());
        bytes.extend_from_slice(&(self.entries.len() as u16).to_be_bytes());
        let (flag, next, revision) = match self.next {
            Some((id, revision)) => (1, id, revision),
            None => (0, Uuid::nil(), 0),
        };
        bytes.push(flag);
        bytes.extend_from_slice(next.as_bytes());
        bytes.extend_from_slice(&revision.to_be_bytes());
        for (request, decision) in &self.entries {
            bytes.extend_from_slice(request.as_bytes());
            bytes.extend_from_slice(decision.as_bytes());
        }
        bytes
    }
    fn decode(bytes: &[u8], head: Uuid, ordinal: u32) -> Result<Self, StorageError> {
        let invalid = StorageError::InvalidCatalog;
        if bytes.len() < CHUNK_HEADER
            || bytes.len() > MAX_CHUNK_BYTES
            || &bytes[..4] != CHUNK_MAGIC
            || bytes[4] != VERSION
            || bytes[5..21] != *head.as_bytes()
            || bytes[21..25] != ordinal.to_be_bytes()
        {
            return Err(invalid);
        }
        let uuid = |at: usize| Uuid::from_slice(&bytes[at..at + 16]).map_err(|_| invalid);
        let u64_at = |at: usize| {
            let mut raw = [0_u8; 8];
            raw.copy_from_slice(&bytes[at..at + 8]);
            u64::from_be_bytes(raw)
        };
        let start = uuid(25)?;
        let start_revision = u64_at(41);
        let count = usize::from(u16::from_be_bytes([bytes[49], bytes[50]]));
        let next = match bytes[51] {
            0 if uuid(52)?.is_nil() && u64_at(68) == 0 => None,
            1 => Some((uuid(52)?, u64_at(68))),
            _ => return Err(invalid),
        };
        // Only the terminal chunk may be short, so chunking is deterministic.
        if count == 0
            || count > CHUNK_ENTRIES
            || (count < CHUNK_ENTRIES && next.is_some())
            || bytes.len() != CHUNK_HEADER + count * 32
            || start.is_nil()
        {
            return Err(invalid);
        }
        let entries = bytes[CHUNK_HEADER..]
            .chunks_exact(32)
            .map(|raw| {
                Ok((
                    Uuid::from_slice(&raw[..16]).map_err(|_| invalid)?,
                    Uuid::from_slice(&raw[16..]).map_err(|_| invalid)?,
                ))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        if entries[0].1 != start {
            return Err(invalid);
        }
        Ok(Self {
            start,
            start_revision,
            next,
            entries,
        })
    }
}

fn unavailable<T>(_: T) -> StorageError {
    StorageError::Unavailable
}

impl<B: ObjectStore> Storage<B> {
    /// Read and validate one page, from the cache when its hash is known.
    pub(crate) async fn index_page(
        &self,
        hash: &PageHash,
        prefix: &[u8],
        deadline: Instant,
        reads: &mut Reads,
        caps: (usize, usize),
    ) -> Result<Arc<Page>, StorageError> {
        let depth = prefix.len();
        if let Some(page) = self.index_cache.get(hash) {
            // The cached page was validated once. Recheck it against this path.
            page.check_route(prefix)?;
            return Ok(page);
        }
        reads.records += 1;
        if reads.records > caps.0 {
            return Err(StorageError::LimitExceeded);
        }
        let remaining = caps
            .1
            .checked_sub(reads.bytes)
            .ok_or(StorageError::LimitExceeded)?;
        let object = timeout_at(
            deadline,
            self.read(
                &ObjectKey::decision_index_page(hash),
                MAX_INDEX_PAGE_BYTES.min(remaining),
            ),
        )
        .await
        .map_err(unavailable)?
        .map_err(|error| match error {
            // A referenced page that is gone is damage, never absence.
            StorageError::NotFound => StorageError::InvalidCatalog,
            error => error,
        })?;
        reads.bytes += object.bytes.len();
        if page_hash(&object.bytes) != *hash {
            return Err(StorageError::InvalidCatalog);
        }
        let page = Arc::new(Page::decode(&object.bytes, depth, prefix)?);
        self.index_cache
            .insert(*hash, object.bytes.len(), page.clone());
        Ok(page)
    }

    /// The decision UUID indexed for `request`, or None from a validated path.
    pub(crate) async fn index_find(
        &self,
        root: &PageHash,
        request: Uuid,
        deadline: Instant,
        reads: &mut Reads,
        caps: (usize, usize),
    ) -> Result<Option<Uuid>, StorageError> {
        let route = *request.as_bytes();
        let mut hash = *root;
        for depth in 0..=LAST_DEPTH {
            let page = self
                .index_page(&hash, &route[..depth], deadline, reads, caps)
                .await?;
            match page.get(route[depth]) {
                None => return Ok(None),
                Some(Slot::Entry {
                    request: indexed,
                    decision,
                }) => return Ok((indexed == request).then_some(decision)),
                Some(Slot::Child(child)) => hash = child,
            }
        }
        Err(StorageError::InvalidCatalog)
    }

    /// Copy-on-write insertion of one fresh pair. The caller has proved that
    /// `request` is absent from this exact index.
    pub(crate) async fn index_insert(
        &self,
        root: Option<PageHash>,
        request: Uuid,
        decision: Uuid,
        deadline: Instant,
    ) -> Result<(PageHash, NewPages), StorageError> {
        let mut out = NewPages::default();
        let Some(root) = root else {
            return Ok((build(&[(request, decision)], 0, &mut out)?, out));
        };
        let route = *request.as_bytes();
        let mut reads = Reads {
            records: 0,
            bytes: 0,
        };
        let caps = (LAST_DEPTH + 1, (LAST_DEPTH + 1) * MAX_INDEX_PAGE_BYTES);
        let mut path = Vec::new();
        let mut hash = root;
        loop {
            let depth = path.len();
            let page = self
                .index_page(&hash, &route[..depth], deadline, &mut reads, caps)
                .await?;
            let slot = page.get(route[depth]);
            path.push(page);
            match slot {
                Some(Slot::Child(child)) => hash = child,
                _ => break,
            }
        }
        let mut page = (*path.pop().ok_or(StorageError::InvalidCatalog)?).clone();
        let depth = page.depth;
        let slot = match page.get(route[depth]) {
            None => Slot::Entry { request, decision },
            Some(Slot::Entry {
                request: other,
                decision: other_decision,
            }) if other != request => {
                let mut pair = [(other, other_decision), (request, decision)];
                pair.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                Slot::Child(build(&pair, depth + 1, &mut out)?)
            }
            _ => return Err(StorageError::InvalidCatalog),
        };
        page.set(route[depth], slot);
        let mut child = out.push(page);
        while let Some(parent) = path.pop() {
            let mut parent = (*parent).clone();
            parent.set(route[parent.depth], Slot::Child(child));
            child = out.push(parent);
        }
        Ok((child, out))
    }

    /// Upload pages with bounded concurrency. A page that already exists has
    /// the same name, so it is accepted once its bytes hash to that name.
    pub(crate) async fn put_index_pages(
        &self,
        pages: NewPages,
        deadline: Instant,
    ) -> Result<(), StorageError> {
        stream::iter(pages.pages)
            .map(|(hash, bytes, page)| async move {
                let key = ObjectKey::decision_index_page(&hash);
                let size = bytes.len();
                let put = timeout_at(deadline, self.put(&key, bytes, WriteCondition::Create))
                    .await
                    .map_err(unavailable)?;
                match put {
                    Ok(token) if !token.is_empty() => {}
                    Ok(_) | Err(StorageError::Conflict | StorageError::Ambiguous) => {
                        let object = timeout_at(deadline, self.read(&key, MAX_INDEX_PAGE_BYTES))
                            .await
                            .map_err(unavailable)??;
                        if page_hash(&object.bytes) != hash {
                            return Err(StorageError::InvalidCatalog);
                        }
                    }
                    Err(error) => return Err(error),
                }
                self.index_cache.insert(hash, size, page);
                Ok(())
            })
            .buffer_unordered(PUT_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
            .map(drop)
    }

    async fn read_chunk(
        &self,
        head: Uuid,
        ordinal: u32,
        deadline: Instant,
    ) -> Result<Option<Chunk>, StorageError> {
        match timeout_at(
            deadline,
            self.read(
                &ObjectKey::decision_index_chunk(head, ordinal),
                MAX_CHUNK_BYTES,
            ),
        )
        .await
        .map_err(unavailable)?
        {
            Ok(Object { bytes, .. }) => Chunk::decode(&bytes, head, ordinal).map(Some),
            Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn write_chunk(
        &self,
        head: Uuid,
        ordinal: u32,
        chunk: &Chunk,
        deadline: Instant,
    ) -> Result<(), StorageError> {
        let key = ObjectKey::decision_index_chunk(head, ordinal);
        let bytes = chunk.encode(head, ordinal);
        match timeout_at(
            deadline,
            self.put(&key, bytes.clone(), WriteCondition::Create),
        )
        .await
        .map_err(unavailable)?
        {
            Ok(token) if !token.is_empty() => Ok(()),
            // Chunks are a function of the immutable chain: a concurrent
            // migrator writes the same bytes.
            Ok(_) | Err(StorageError::Conflict | StorageError::Ambiguous) => {
                let object = timeout_at(deadline, self.read(&key, MAX_CHUNK_BYTES))
                    .await
                    .map_err(unavailable)??;
                if object.bytes == bytes {
                    Ok(())
                } else {
                    Err(StorageError::InvalidCatalog)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// The number of chunks already written for `head`: chunks are written in
    /// order, so their presence is monotone and a galloping search finds it.
    async fn written_chunks(&self, head: Uuid, deadline: Instant) -> Result<u32, StorageError> {
        let exists = async |ordinal| {
            self.read_chunk(head, ordinal, deadline)
                .await
                .map(|c| c.is_some())
        };
        if !exists(0).await? {
            return Ok(0);
        }
        let (mut low, mut high) = (0_u32, 1_u32);
        while exists(high).await? {
            low = high;
            high = high.checked_mul(2).ok_or(StorageError::LimitExceeded)?;
        }
        // Invariant: chunk `low` exists and chunk `high` does not.
        while high - low > 1 {
            let mid = low + (high - low) / 2;
            if exists(mid).await? {
                low = mid;
            } else {
                high = mid;
            }
        }
        Ok(high)
    }

    /// Build the exact index of the legacy chain under `head` (at
    /// `head_revision`) with resumable work. Progress is saved as immutable
    /// chunks of `(request, decision)` pairs, so attempts that run out of time
    /// continue where the last one stopped. `prefix` is the newest part of the
    /// chain that the caller already read, as `(request, decision, revision)`,
    /// followed by the cursor where its walk stopped. Returns the index root;
    /// publication of the migrated root is the caller's conditional write.
    pub(crate) async fn build_chain_index(
        &self,
        head: Uuid,
        head_revision: u64,
        prefix: (Vec<(Uuid, Uuid, u64)>, Option<(Uuid, u64)>),
        deadline: Instant,
    ) -> Result<Option<(PageHash, NewPages)>, StorageError> {
        let mut ordinal = self.written_chunks(head, deadline).await?;
        let mut pending: Vec<(Uuid, Uuid, u64)> = Vec::new();
        let mut cursor = if ordinal == 0 {
            let (entries, cursor) = prefix;
            if entries
                .first()
                .is_some_and(|e| e.1 != head || e.2 != head_revision)
            {
                return Err(StorageError::InvalidCatalog);
            }
            pending = entries;
            if pending.is_empty() {
                Some((head, head_revision))
            } else {
                cursor
            }
        } else {
            let last = self
                .read_chunk(head, ordinal - 1, deadline)
                .await?
                .ok_or(StorageError::Unavailable)?;
            last.next
        };
        // Walk to the terminus, writing every full chunk as it fills.
        loop {
            while pending.len() >= CHUNK_ENTRIES || (cursor.is_none() && !pending.is_empty()) {
                let take = pending.len().min(CHUNK_ENTRIES);
                let rest = pending.split_off(take);
                let next = rest.first().map(|e| (e.1, e.2)).or(cursor);
                let chunk = Chunk {
                    start: pending[0].1,
                    start_revision: pending[0].2,
                    next,
                    entries: pending.iter().map(|e| (e.0, e.1)).collect(),
                };
                self.write_chunk(head, ordinal, &chunk, deadline).await?;
                ordinal = ordinal.checked_add(1).ok_or(StorageError::LimitExceeded)?;
                pending = rest;
            }
            let Some((id, revision)) = cursor else {
                break;
            };
            if nabedra_rt::time::now() >= deadline {
                return Err(StorageError::Unavailable);
            }
            let decision = self.read_chain_decision(id, revision, deadline).await?;
            pending.push((decision.request_id, decision.id, decision.revision));
            cursor = decision
                .parent
                .map(|parent| (parent, decision.base_revision));
        }
        // Assemble: every chunk, checked for continuity from the head.
        let mut entries = Vec::new();
        let chunks = stream::iter(0..ordinal)
            .map(|k| async move {
                self.read_chunk(head, k, deadline)
                    .await?
                    .ok_or(StorageError::InvalidCatalog)
            })
            .buffered(PUT_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut expect = Some((head, head_revision));
        for chunk in chunks {
            if expect != Some((chunk.start, chunk.start_revision)) {
                return Err(StorageError::InvalidCatalog);
            }
            expect = chunk.next;
            entries.extend(chunk.entries);
            if entries.len() > MAX_MIGRATION_ENTRIES {
                return Err(StorageError::LimitExceeded);
            }
        }
        if expect.is_some() {
            return Err(StorageError::InvalidCatalog);
        }
        build_index(entries)
    }

    /// One decision of a legacy chain, checked against its expected position.
    pub(crate) async fn read_chain_decision(
        &self,
        id: Uuid,
        revision: u64,
        deadline: Instant,
    ) -> Result<super::Decision, StorageError> {
        let object = timeout_at(
            deadline,
            self.read(&ObjectKey::decision(id), super::MAX_DECISION_BYTES),
        )
        .await
        .map_err(unavailable)?
        .map_err(|error| match error {
            StorageError::NotFound => StorageError::InvalidCatalog,
            error => error,
        })?;
        let decision = super::Decision::decode(&object.bytes)?;
        if decision.id != id
            || decision.revision != revision
            || revision.checked_sub(1) != Some(decision.base_revision)
        {
            return Err(StorageError::InvalidCatalog);
        }
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }
    fn tree(entries: &[(Uuid, Uuid)]) -> (PageHash, HashMap<PageHash, Vec<u8>>) {
        let (root, pages) = build_index(entries.iter().copied()).unwrap().unwrap();
        (
            root,
            pages.pages.into_iter().map(|(h, b, _)| (h, b)).collect(),
        )
    }
    fn find(pages: &HashMap<PageHash, Vec<u8>>, root: PageHash, request: Uuid) -> Option<Uuid> {
        let route = *request.as_bytes();
        let mut hash = root;
        for depth in 0..=LAST_DEPTH {
            let page = Page::decode(&pages[&hash], depth, &route[..depth]).unwrap();
            match page.get(route[depth]) {
                None => return None,
                Some(Slot::Entry {
                    request: r,
                    decision,
                }) => {
                    return (r == request).then_some(decision);
                }
                Some(Slot::Child(c)) => hash = c,
            }
        }
        panic!("too deep")
    }

    #[test]
    fn index_is_canonical_exact_and_handles_shared_prefixes() {
        // Sequential UUIDs share 15 bytes: the deepest possible path.
        let entries: Vec<_> = (1..=300).map(|n| (id(n), id(10_000 + n))).collect();
        let (root, pages) = tree(&entries);
        let mut reversed = entries.clone();
        reversed.reverse();
        assert_eq!(tree(&reversed).0, root, "the tree depends only on the set");
        for (request, decision) in &entries {
            assert_eq!(find(&pages, root, *request), Some(*decision));
        }
        assert_eq!(find(&pages, root, id(301)), None);
        assert_eq!(find(&pages, root, id(1 << 120)), None);
        // An unknown UUID that shares a full 15-byte prefix with an entry.
        assert_eq!(find(&pages, root, id(256 + 255)), None);
    }

    #[test]
    fn first_pair_wins_for_a_repeated_request() {
        let (root, pages) = tree(&[(id(1), id(20)), (id(1), id(10))]);
        assert_eq!(find(&pages, root, id(1)), Some(id(20)));
    }

    #[test]
    fn pages_reject_wrong_route_depth_shape_or_bytes() {
        let (root, pages) = tree(&[(id(1), id(2)), (id(3 << 120), id(4))]);
        let bytes = &pages[&root];
        assert!(Page::decode(bytes, 0, &[]).is_ok());
        assert!(Page::decode(bytes, 1, &[0]).is_err());
        let mut bad = bytes.clone();
        bad[0] = b'X';
        assert!(Page::decode(&bad, 0, &[]).is_err());
        let mut short = bytes.clone();
        short.pop();
        assert!(Page::decode(&short, 0, &[]).is_err());
        // An entry under a slot byte that is not its own route byte.
        let mut moved = bytes.clone();
        moved[PAGE_HEADER] ^= 1;
        assert!(Page::decode(&moved, 0, &[]).is_err());
        // A single entry below the root is not canonical.
        let lone = Page {
            depth: 1,
            slots: vec![(
                id(1).as_bytes()[1],
                Slot::Entry {
                    request: id(1),
                    decision: id(2),
                },
            )],
        };
        assert!(Page::decode(&lone.encode(), 1, &[0]).is_err());
    }

    #[test]
    fn hex_round_trip_is_strict() {
        let hash = page_hash(b"x");
        assert_eq!(parse_hex(&hex(&hash)), Some(hash));
        assert_eq!(parse_hex(&hex(&hash).to_uppercase()), None);
        assert_eq!(parse_hex("00"), None);
    }

    #[test]
    fn chunks_round_trip_and_reject_short_non_terminal_chunks() {
        let head = id(9);
        let chunk = Chunk {
            start: id(2),
            start_revision: 5,
            next: None,
            entries: vec![(id(1), id(2))],
        };
        let bytes = chunk.encode(head, 3);
        let back = Chunk::decode(&bytes, head, 3).unwrap();
        assert_eq!(back.entries, chunk.entries);
        assert!(Chunk::decode(&bytes, head, 4).is_err());
        assert!(Chunk::decode(&bytes, id(8), 3).is_err());
        let short = Chunk {
            next: Some((id(7), 4)),
            ..chunk
        };
        assert!(Chunk::decode(&short.encode(head, 3), head, 3).is_err());
    }
}
