//! The worker-shared block cache.
//!
//! A 16-shard LRU of [`Bytes`] with a byte capacity (`cache_bytes`). It holds run data,
//! index and filter blocks, and journal values read through a locator. Every entry is
//! charged its length plus [`ENTRY_OVERHEAD`].
//!
//! **Pinning.** A cached `Bytes` that a caller still holds (a value returned by a get, an
//! iterator's current block) is *pinned*: its memory stays alive whatever the cache does.
//! Eviction therefore skips pinned entries, so `usage_bytes` keeps counting memory that
//! is really resident, and may exceed the capacity while callers hold more than it.
//! [`BlockCache::pinned_bytes`] reports how much of the usage is pinned.
//!
//! **Reservations.** Memory that callers keep resident outside the LRU on the cache's
//! budget (each open run's top-level index) is charged with [`BlockCache::reserve`]: it
//! counts in `usage_bytes` and `pinned_bytes`, and the LRU gets only the capacity left
//! over, so index and filter partitions compete for what remains.
//!
//! Keys carry a per-store namespace from [`BlockCache::new_namespace`], because run ids
//! and journal segment numbers are only unique inside one store while the cache is
//! shared by every pool on a worker.

mod lru;

use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

pub(crate) use lru::Lru;

/// Number of independently locked shards.
pub const SHARDS: usize = 16;

/// Bytes charged per entry on top of its length: the slab node, the hash slot and the
/// `Bytes` header.
pub const ENTRY_OVERHEAD: usize = 96;

/// Entries an insert inspects at most, from the cold end, looking for unpinned ones.
const MAX_EVICTION_SCAN: usize = 64;

/// What a cached block is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheKey {
    /// A block of a sorted run, by the block's file offset.
    Run { store: u64, run: u64, offset: u64 },
    /// A journal value, by the locator that names it.
    Journal {
        store: u64,
        segment: u64,
        epoch: u64,
        offset: u64,
    },
}

impl CacheKey {
    /// The store namespace the key belongs to.
    pub fn namespace(&self) -> u64 {
        match *self {
            CacheKey::Run { store, .. } | CacheKey::Journal { store, .. } => store,
        }
    }

    /// A well mixed 64-bit hash. Cache keys come from the engine, never from clients, so
    /// a fast non-keyed hash is enough.
    pub fn hash64(&self) -> u64 {
        match *self {
            CacheKey::Run { store, run, offset } => {
                fmix64(store ^ fmix64(run ^ fmix64(offset ^ 0x5255_4e00)))
            }
            CacheKey::Journal {
                store,
                segment,
                epoch,
                offset,
            } => fmix64(store ^ fmix64(segment ^ fmix64(epoch ^ fmix64(offset ^ 0x4a4e_4c00)))),
        }
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash64());
    }
}

/// Bits 32..36 pick the shard: the hash map inside a shard indexes buckets with the low
/// bits and tags them with the top seven, so both stay fully varied.
fn shard_index(key: &CacheKey) -> usize {
    (key.hash64() >> 32) as usize % SHARDS
}

/// MurmurHash3's 64-bit finalizer.
pub(crate) fn fmix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// A hasher for keys that hash themselves into one well mixed `u64`.
#[derive(Default)]
pub(crate) struct PremixedHasher(u64);

impl Hasher for PremixedHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = fmix64(self.0 ^ u64::from(b));
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 ^= value;
    }
}

pub(crate) type PremixedBuild = BuildHasherDefault<PremixedHasher>;

struct Shard {
    lru: Lru<CacheKey, Bytes, PremixedBuild>,
}

/// Point-in-time counters of a [`BlockCache`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub capacity_bytes: usize,
    pub usage_bytes: usize,
    pub pinned_bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
}

pub struct BlockCache {
    shards: Box<[Mutex<Shard>]>,
    capacity: usize,
    /// Bytes held by live [`CacheReservation`]s.
    reserved: AtomicUsize,
    usage: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    next_namespace: AtomicU64,
}

impl BlockCache {
    /// A cache holding about `capacity_bytes`. Zero disables caching: every insert is
    /// handed straight back.
    pub fn new(capacity_bytes: usize) -> BlockCache {
        let shards = (0..SHARDS)
            .map(|_| {
                Mutex::new(Shard {
                    lru: Lru::with_hasher(PremixedBuild::default()),
                })
            })
            .collect();
        BlockCache {
            shards,
            capacity: capacity_bytes,
            reserved: AtomicUsize::new(0),
            usage: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            next_namespace: AtomicU64::new(1),
        }
    }

    /// A namespace unique within this cache. Each open store takes one and puts it in
    /// every key it uses.
    pub fn new_namespace(&self) -> u64 {
        self.next_namespace.fetch_add(1, Ordering::Relaxed)
    }

    fn shard(&self, key: &CacheKey) -> &Mutex<Shard> {
        &self.shards[shard_index(key)]
    }

    /// What one shard's LRU may hold: the capacity left after reservations, split evenly.
    fn shard_capacity(&self) -> usize {
        self.capacity
            .saturating_sub(self.reserved.load(Ordering::Relaxed))
            / SHARDS
    }

    /// Charges `bytes` that the caller keeps resident outside the LRU (for example an
    /// open run's top-level index) to this cache until the reservation is dropped. They
    /// count as used and pinned, and the LRU shrinks by as much.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> CacheReservation {
        self.reserved.fetch_add(bytes, Ordering::Relaxed);
        self.usage.fetch_add(bytes, Ordering::Relaxed);
        CacheReservation {
            cache: Arc::clone(self),
            bytes,
        }
    }

    /// Bytes held by live reservations.
    pub fn reserved_bytes(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    /// The cached block, marked most recently used.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let found = lock(self.shard(key)).lru.get(key).cloned();
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    /// Caches `value` unless `key` is already present, and returns the resident copy, so
    /// two readers that missed at the same time end up sharing one buffer. Evicts
    /// unpinned entries, least recently used first, until the shard fits again.
    pub fn insert(&self, key: CacheKey, value: Bytes) -> Bytes {
        let charge = value.len() + ENTRY_OVERHEAD;
        let shard_capacity = self.shard_capacity();
        if charge > shard_capacity {
            return value;
        }
        let mut shard = lock(self.shard(&key));
        if let Some(existing) = shard.lru.get(&key) {
            return existing.clone();
        }
        shard.lru.insert(key, value.clone(), charge);
        self.usage.fetch_add(charge, Ordering::Relaxed);
        self.inserts.fetch_add(1, Ordering::Relaxed);
        while shard.lru.charge() > shard_capacity {
            // The entry just inserted is pinned by `value`, so it is never chosen here.
            match shard.lru.evict(MAX_EVICTION_SCAN, Bytes::is_unique) {
                Some((_, evicted, charge)) => {
                    self.usage.fetch_sub(charge, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    drop(evicted);
                }
                None => break,
            }
        }
        value
    }

    /// Drops `key` if present.
    pub fn erase(&self, key: &CacheKey) {
        let mut shard = lock(self.shard(key));
        if let Some(value) = shard.lru.remove(key) {
            self.usage
                .fetch_sub(value.len() + ENTRY_OVERHEAD, Ordering::Relaxed);
        }
    }

    /// Drops every entry of `namespace` (one store's run blocks and journal values).
    /// Entries a reader still holds stay alive in that reader only. Returns how many
    /// entries were dropped.
    pub fn erase_namespace(&self, namespace: u64) -> usize {
        let mut erased = 0;
        for shard in self.shards.iter() {
            let mut shard = lock(shard);
            let keys: Vec<CacheKey> = shard
                .lru
                .entries()
                .map(|(key, _, _)| *key)
                .filter(|key| key.namespace() == namespace)
                .collect();
            for key in keys {
                if let Some(value) = shard.lru.remove(&key) {
                    self.usage
                        .fetch_sub(value.len() + ENTRY_OVERHEAD, Ordering::Relaxed);
                    erased += 1;
                }
            }
        }
        erased
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity
    }

    /// Bytes charged for resident entries, pinned ones included.
    pub fn usage_bytes(&self) -> usize {
        self.usage.load(Ordering::Relaxed)
    }

    /// Bytes charged for resident entries that a caller still references, plus the
    /// reservations. Walks every entry, so it is meant for stats, not for hot paths.
    pub fn pinned_bytes(&self) -> usize {
        self.reserved_bytes()
            + self
                .shards
                .iter()
                .map(|shard| {
                    lock(shard)
                        .lru
                        .entries()
                        .filter(|(_, value, _)| !value.is_unique())
                        .map(|(_, _, charge)| charge)
                        .sum::<usize>()
                })
                .sum::<usize>()
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            capacity_bytes: self.capacity,
            usage_bytes: self.usage_bytes(),
            pinned_bytes: self.pinned_bytes(),
            entries: self.shards.iter().map(|s| lock(s).lru.len()).sum(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

/// Bytes charged to a [`BlockCache`] until dropped (see [`BlockCache::reserve`]).
pub struct CacheReservation {
    cache: Arc<BlockCache>,
    bytes: usize,
}

impl CacheReservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for CacheReservation {
    fn drop(&mut self) {
        self.cache.reserved.fetch_sub(self.bytes, Ordering::Relaxed);
        self.cache.usage.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for CacheReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("capacity", &self.capacity)
            .field("usage", &self.usage_bytes())
            .finish_non_exhaustive()
    }
}

/// The shards hold no invariant a panicking holder could break halfway, so a poisoned
/// lock is still usable.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_key(run: u64, offset: u64) -> CacheKey {
        CacheKey::Run {
            store: 1,
            run,
            offset,
        }
    }

    #[test]
    fn erase_namespace_drops_only_that_stores_entries() {
        let cache = BlockCache::new(1 << 20);
        let journal = |store| CacheKey::Journal {
            store,
            segment: 1,
            epoch: 1,
            offset: 0,
        };
        let other = CacheKey::Run {
            store: 2,
            run: 1,
            offset: 0,
        };
        drop(cache.insert(run_key(1, 0), Bytes::from_static(b"a")));
        let held = cache.insert(run_key(1, 4096), Bytes::from_static(b"b"));
        drop(cache.insert(journal(1), Bytes::from_static(b"c")));
        drop(cache.insert(other, Bytes::from_static(b"d")));
        drop(cache.insert(journal(2), Bytes::from_static(b"e")));
        assert_eq!(cache.erase_namespace(1), 3);
        assert!(cache.get(&run_key(1, 0)).is_none());
        assert!(cache.get(&run_key(1, 4096)).is_none());
        assert!(cache.get(&journal(1)).is_none());
        assert!(cache.get(&other).is_some());
        assert!(cache.get(&journal(2)).is_some());
        assert_eq!(cache.usage_bytes(), 2 * (1 + ENTRY_OVERHEAD));
        assert_eq!(held.as_ref(), b"b", "a reader's copy outlives the eviction");
        assert_eq!(cache.erase_namespace(1), 0);
    }

    #[test]
    fn cache_respects_capacity_and_evicts_least_recently_used() {
        // 16 shards of 4 KiB, each holding three entries; every shard overflows.
        let cache = BlockCache::new(16 * 4096);
        for i in 0..1000u64 {
            cache.insert(run_key(1, i * 4096), Bytes::from(vec![i as u8; 1000]));
        }
        let stats = cache.stats();
        assert!(stats.usage_bytes <= cache.capacity_bytes(), "{stats:?}");
        assert!(stats.evictions > 0);
        assert_eq!(stats.pinned_bytes, 0);
        assert_eq!(stats.usage_bytes, stats.entries * (1000 + ENTRY_OVERHEAD));
        // The most recent insert is still there and round trips.
        assert_eq!(
            cache.get(&run_key(1, 999 * 4096)).unwrap(),
            Bytes::from(vec![231u8; 1000])
        );
        assert!(cache.get(&run_key(1, 0)).is_none());
    }

    #[test]
    fn cache_insert_returns_the_resident_copy_and_counts_hits() {
        let cache = BlockCache::new(1 << 20);
        let first = cache.insert(run_key(7, 0), Bytes::from_static(b"one"));
        let second = cache.insert(run_key(7, 0), Bytes::from_static(b"two"));
        assert_eq!(first, second);
        assert_eq!(cache.get(&run_key(7, 0)).unwrap(), "one");
        assert!(cache.get(&run_key(7, 1)).is_none());
        // Namespaces and key kinds never collide.
        let other_store = CacheKey::Run {
            store: 2,
            run: 7,
            offset: 0,
        };
        assert!(cache.get(&other_store).is_none());
        let journal = CacheKey::Journal {
            store: 1,
            segment: 7,
            epoch: 0,
            offset: 0,
        };
        assert!(cache.get(&journal).is_none());
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses, stats.inserts), (1, 3, 1));
        cache.erase(&run_key(7, 0));
        assert_eq!(cache.usage_bytes(), 0);
        assert_ne!(cache.new_namespace(), cache.new_namespace());
    }

    #[test]
    fn cache_counts_pinned_bytes_and_never_evicts_pinned_entries() {
        // One shard holds 4 KiB.
        let cache = BlockCache::new(SHARDS * 4096);
        let pinned: Vec<Bytes> = (0..200u64)
            .map(|i| cache.insert(run_key(1, i), Bytes::from(vec![1u8; 1000])))
            .collect();
        let charge = 1000 + ENTRY_OVERHEAD;
        // Every entry is held by a caller: nothing may be evicted, usage exceeds capacity
        // and all of it is pinned.
        let stats = cache.stats();
        assert_eq!(stats.evictions, 0);
        assert_eq!(stats.usage_bytes, 200 * charge);
        assert_eq!(stats.pinned_bytes, 200 * charge);
        // A slice of a cached block pins it too.
        let slice = pinned[5].slice(10..20);
        drop(pinned);
        assert_eq!(cache.pinned_bytes(), charge);
        // New inserts now evict the unpinned entries but keep the pinned one.
        for i in 1000..1400u64 {
            drop(cache.insert(run_key(1, i), Bytes::from(vec![2u8; 1000])));
        }
        let stats = cache.stats();
        assert!(
            stats.usage_bytes <= cache.capacity_bytes() + charge,
            "{stats:?}"
        );
        assert_eq!(stats.pinned_bytes, charge);
        assert!(cache.get(&run_key(1, 5)).is_some());
        drop(slice);
        assert_eq!(cache.pinned_bytes(), 0);
    }

    #[test]
    fn reservations_count_as_pinned_and_shrink_the_lru() {
        let cache = Arc::new(BlockCache::new(SHARDS * 8192));
        for i in 0..400u64 {
            drop(cache.insert(run_key(1, i), Bytes::from(vec![1u8; 1000])));
        }
        let before = cache.stats();
        assert!(before.usage_bytes <= cache.capacity_bytes());
        let reservation = cache.reserve(SHARDS * 4096);
        assert_eq!(cache.reserved_bytes(), SHARDS * 4096);
        assert_eq!(cache.pinned_bytes(), SHARDS * 4096);
        // New inserts now fit the LRU into what the reservation left.
        for i in 1000..1400u64 {
            drop(cache.insert(run_key(1, i), Bytes::from(vec![2u8; 1000])));
        }
        let stats = cache.stats();
        assert!(
            stats.usage_bytes <= cache.capacity_bytes() + SHARDS * (1000 + ENTRY_OVERHEAD),
            "{stats:?}"
        );
        assert!(stats.usage_bytes - SHARDS * 4096 <= SHARDS * 4096 + SHARDS * 1100);
        drop(reservation);
        assert_eq!(cache.reserved_bytes(), 0);
        assert_eq!(cache.pinned_bytes(), 0);
        assert!(cache.usage_bytes() <= cache.capacity_bytes());
    }

    #[test]
    fn zero_capacity_cache_stores_nothing() {
        let cache = BlockCache::new(0);
        let value = cache.insert(run_key(1, 0), Bytes::from_static(b"x"));
        assert_eq!(value, "x");
        assert!(cache.get(&run_key(1, 0)).is_none());
        assert_eq!(cache.usage_bytes(), 0);
    }

    #[test]
    fn cache_keys_spread_over_every_shard() {
        let mut counts = [0usize; SHARDS];
        for offset in 0..16_000u64 {
            let key = run_key(3, offset * 4096);
            counts[shard_index(&key)] += 1;
        }
        for count in counts {
            assert!((800..1200).contains(&count), "{counts:?}");
        }
    }
}
