//! Quarantined scopes (task NS5, plan section 3).
//!
//! A scope is quarantined when damage confined to it is found: a journal record at open
//! (G1), a run file or block (G2), or a journal value (G3). Reads of a key under a
//! quarantined scope return `Corruption` until [`crate::Store::release_scope`] deletes
//! the scope. The set is small (R6 refuses a store above a few scopes), so lookups scan
//! it; the common case, an empty set, costs one atomic load.

use std::ops::Bound;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::batch::prefix_successor;
use crate::error::{Damage, Error, FileId};

/// R6: the most scopes one open may quarantine before the store is refused instead.
/// The plan's second limit, 10% of a pool's groups, needs the group count, which only
/// the pool knows; the pool applies it to [`crate::Opened::quarantined`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuarantineLimit {
    pub max_scopes: usize,
}

impl Default for QuarantineLimit {
    fn default() -> Self {
        QuarantineLimit { max_scopes: 8 }
    }
}

#[derive(Default)]
pub(crate) struct Quarantine {
    scopes: RwLock<Vec<Damage>>,
    count: AtomicUsize,
}

impl Quarantine {
    pub fn new(initial: Vec<Damage>) -> Quarantine {
        Quarantine {
            count: AtomicUsize::new(initial.len()),
            scopes: RwLock::new(initial),
        }
    }

    /// Adds `damage` unless its scope is already quarantined. True when added.
    pub fn add(&self, damage: Damage) -> bool {
        let mut scopes = self.scopes.write().unwrap_or_else(|e| e.into_inner());
        if scopes.iter().any(|d| d.scope == damage.scope) {
            return false;
        }
        scopes.push(damage);
        self.count.store(scopes.len(), Ordering::Release);
        true
    }

    pub fn remove(&self, scope: &[u8]) {
        let mut scopes = self.scopes.write().unwrap_or_else(|e| e.into_inner());
        scopes.retain(|d| d.scope != scope);
        self.count.store(scopes.len(), Ordering::Release);
    }

    pub fn list(&self) -> Vec<Damage> {
        self.scopes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// The quarantined scope `key` lies under, if any.
    pub fn scope_of(&self, key: &[u8]) -> Option<Bytes> {
        if self.len() == 0 {
            return None;
        }
        self.scopes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .find(|d| key.starts_with(&d.scope))
            .map(|d| d.scope.clone())
    }

    /// True when some key in the range from `lower` to `upper` lies under a quarantined
    /// scope. An iterator that moves across such a range without landing on a key there
    /// (the scope's records were skipped by recovery, or are all deleted) must fail,
    /// never return the rest of the range as if the scope were empty.
    pub fn overlaps(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        if self.len() == 0 {
            return false;
        }
        self.scopes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|d| range_overlaps_scope(lower, upper, &d.scope))
    }

    /// `Err(Corruption)` when `key` is under a quarantined scope.
    pub fn check(&self, key: &[u8]) -> Result<(), Error> {
        match self.scope_of(key) {
            None => Ok(()),
            Some(_) => Err(quarantined_error()),
        }
    }
}

pub(crate) fn quarantined_error() -> Error {
    Error::Corruption {
        file: FileId::Other,
        offset: 0,
        what: "key belongs to a quarantined scope",
    }
}

/// True when some key `k` with `lower <= k <= upper` (each end as its bound says) starts
/// with `scope`. Conservative only for a range with no key at all between two adjacent
/// byte strings, which a caller never has. An empty scope never matches.
pub(crate) fn range_overlaps_scope(lower: Bound<&[u8]>, upper: Bound<&[u8]>, scope: &[u8]) -> bool {
    if scope.is_empty() {
        return false;
    }
    let end = prefix_successor(scope);
    // The intersection's lower end: the larger of `lower` and `scope` (inclusive).
    let (lo, lo_incl): (&[u8], bool) = match lower {
        Bound::Included(k) if k > scope => (k, true),
        Bound::Excluded(k) if k >= scope => (k, false),
        _ => (scope, true),
    };
    // Its upper end: the smaller of `upper` and the scope's end (exclusive).
    let (hi, hi_incl): (&[u8], bool) = match (upper, end.as_deref()) {
        (Bound::Unbounded, None) => return true,
        (Bound::Unbounded, Some(e)) => (e, false),
        (Bound::Included(h), Some(e)) if e <= h => (e, false),
        (Bound::Included(h), _) => (h, true),
        (Bound::Excluded(h), Some(e)) if e <= h => (e, false),
        (Bound::Excluded(h), _) => (h, false),
    };
    lo < hi || (lo == hi && lo_incl && hi_incl)
}

/// The scope prefix both ends of `[lo, hi]` share under `scope_of`, if the whole range
/// lies inside one non-empty scope. `hi_exclusive` allows `hi` to be the scope's end.
pub(crate) fn single_scope(
    scope_of: fn(&[u8]) -> usize,
    lo: &[u8],
    hi: &[u8],
    hi_exclusive: bool,
) -> Option<Bytes> {
    let len = scope_of(lo).min(lo.len());
    if len == 0 {
        return None;
    }
    let scope = &lo[..len];
    let inside = hi.starts_with(scope)
        || (hi_exclusive && prefix_successor(scope).is_some_and(|end| hi <= end.as_slice()));
    inside.then(|| Bytes::copy_from_slice(scope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DamageReason;

    fn two(_: &[u8]) -> usize {
        2
    }

    #[test]
    fn ns_unit_quarantine_matches_prefixes_and_ranges() {
        let q = Quarantine::default();
        assert!(q.check(b"g1x").is_ok());
        assert!(q.add(Damage {
            scope: Bytes::from_static(b"g1"),
            reason: DamageReason::RunBlock,
            file: FileId::Run { id: 1 },
            first_bad_seq: None,
        }));
        assert!(q.check(b"g1x").is_err());
        assert!(q.check(b"g2x").is_ok());
        q.remove(b"g1");
        assert_eq!(q.len(), 0);
        assert_eq!(
            single_scope(two, b"g1a", b"g1z", false).as_deref(),
            Some(&b"g1"[..])
        );
        assert_eq!(
            single_scope(two, b"g1a", b"g2", true).as_deref(),
            Some(&b"g1"[..])
        );
        assert_eq!(single_scope(two, b"g1a", b"g2", false), None);
    }
}
