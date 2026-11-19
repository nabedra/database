//! Memtable map keys ordered like [`crate::types::compare_internal_keys`].
//!
//! A map entry stores its user key as a [`Bytes`] slice of the batch arena plus the
//! packed trailer, so inserting allocates nothing per entry. Lookups borrow the map key
//! as `dyn KeyView`, which a [`Probe`] (a borrowed user key plus a trailer) also
//! implements, so seeks and point gets never build an encoded internal key.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::Bound;

use bytes::Bytes;

use crate::types::TRAILER_LEN;

/// A view of an internal key split into its user key and trailer.
pub(crate) trait KeyView {
    fn user_key(&self) -> &[u8];
    fn trailer(&self) -> u64;
}

/// User key ascending, then trailer descending: the internal key order.
#[inline]
pub(crate) fn compare_parts(
    a_key: &[u8],
    a_trailer: u64,
    b_key: &[u8],
    b_trailer: u64,
) -> Ordering {
    a_key.cmp(b_key).then_with(|| b_trailer.cmp(&a_trailer))
}

impl PartialEq for dyn KeyView + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for dyn KeyView + '_ {}

impl PartialOrd for dyn KeyView + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn KeyView + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_parts(
            self.user_key(),
            self.trailer(),
            other.user_key(),
            other.trailer(),
        )
    }
}

/// An owned internal key: the user key shares the batch arena.
#[derive(Clone, Debug)]
pub struct MemKey {
    pub(crate) user_key: Bytes,
    pub(crate) trailer: u64,
}

impl MemKey {
    /// Appends the encoded internal key to `out`.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        out.clear();
        out.reserve(self.user_key.len() + TRAILER_LEN);
        out.extend_from_slice(&self.user_key);
        out.extend_from_slice(&self.trailer.to_be_bytes());
    }
}

impl KeyView for MemKey {
    #[inline]
    fn user_key(&self) -> &[u8] {
        &self.user_key
    }
    #[inline]
    fn trailer(&self) -> u64 {
        self.trailer
    }
}

impl PartialEq for MemKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MemKey {}

impl PartialOrd for MemKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_parts(&self.user_key, self.trailer, &other.user_key, other.trailer)
    }
}

impl<'a> Borrow<dyn KeyView + 'a> for MemKey {
    fn borrow(&self) -> &(dyn KeyView + 'a) {
        self
    }
}

/// A borrowed lookup key.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Probe<'a> {
    pub(crate) user_key: &'a [u8],
    pub(crate) trailer: u64,
}

impl<'a> Probe<'a> {
    /// Splits an encoded internal key the way [`crate::types::compare_internal_keys`]
    /// does, so probes built from arbitrary seek targets order consistently.
    pub(crate) fn from_internal(ikey: &'a [u8]) -> Probe<'a> {
        let split = ikey.len().saturating_sub(TRAILER_LEN);
        let (user_key, tail) = ikey.split_at(split);
        let mut bytes = [0u8; TRAILER_LEN];
        bytes[TRAILER_LEN - tail.len()..].copy_from_slice(tail);
        Probe {
            user_key,
            trailer: u64::from_be_bytes(bytes),
        }
    }
}

impl KeyView for Probe<'_> {
    #[inline]
    fn user_key(&self) -> &[u8] {
        self.user_key
    }
    #[inline]
    fn trailer(&self) -> u64 {
        self.trailer
    }
}

pub(crate) type KeyBound<'a> = Bound<&'a (dyn KeyView + 'a)>;

/// The tighter of two lower bounds.
pub(crate) fn max_lower<'a>(a: KeyBound<'a>, b: KeyBound<'a>) -> KeyBound<'a> {
    match (a, b) {
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => other,
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(if x >= y { x } else { y }),
        (Bound::Excluded(x), Bound::Excluded(y)) => Bound::Excluded(if x >= y { x } else { y }),
        (Bound::Included(i), Bound::Excluded(e)) | (Bound::Excluded(e), Bound::Included(i)) => {
            if i > e {
                Bound::Included(i)
            } else {
                Bound::Excluded(e)
            }
        }
    }
}

/// The tighter of two upper bounds.
pub(crate) fn min_upper<'a>(a: KeyBound<'a>, b: KeyBound<'a>) -> KeyBound<'a> {
    match (a, b) {
        (Bound::Unbounded, other) | (other, Bound::Unbounded) => other,
        (Bound::Included(x), Bound::Included(y)) => Bound::Included(if x <= y { x } else { y }),
        (Bound::Excluded(x), Bound::Excluded(y)) => Bound::Excluded(if x <= y { x } else { y }),
        (Bound::Included(i), Bound::Excluded(e)) | (Bound::Excluded(e), Bound::Included(i)) => {
            if i < e {
                Bound::Included(i)
            } else {
                Bound::Excluded(e)
            }
        }
    }
}

/// True when no key lies between the bounds. `BTreeMap::range` panics on inverted
/// ranges, so every range call checks this first.
pub(crate) fn is_empty_range(start: KeyBound<'_>, end: KeyBound<'_>) -> bool {
    match (start, end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a), Bound::Excluded(b))
        | (Bound::Excluded(a), Bound::Included(b))
        | (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Kind, compare_internal_keys, internal_key, pack_trailer, seek_key};

    #[test]
    fn ns01_probe_order_matches_encoded_internal_key_order() {
        let keys = [
            internal_key(b"", 3, Kind::Put),
            internal_key(b"a", 9, Kind::Delete),
            internal_key(b"a", 1, Kind::Put),
            seek_key(b"a", 5),
            internal_key(b"ab", 5, Kind::JournalPut),
            internal_key(b"b", 0, Kind::Delete),
            b"exactly8".to_vec(),
        ];
        for a in &keys {
            for b in &keys {
                let pa = Probe::from_internal(a);
                let pb = Probe::from_internal(b);
                let (va, vb): (&dyn KeyView, &dyn KeyView) = (&pa, &pb);
                assert_eq!(va.cmp(vb), compare_internal_keys(a, b), "{a:?} vs {b:?}");
            }
        }
        let mk = MemKey {
            user_key: Bytes::from_static(b"a"),
            trailer: pack_trailer(1, Kind::Put),
        };
        let mut buf = Vec::new();
        mk.encode_into(&mut buf);
        assert_eq!(buf, internal_key(b"a", 1, Kind::Put));
    }

    #[test]
    fn ns21_bound_helpers_handle_ties_and_inversions() {
        let a = Probe {
            user_key: b"a",
            trailer: 0,
        };
        let b = Probe {
            user_key: b"b",
            trailer: 0,
        };
        let (a, b): (&dyn KeyView, &dyn KeyView) = (&a, &b);
        assert!(matches!(
            max_lower(Bound::Included(a), Bound::Excluded(a)),
            Bound::Excluded(_)
        ));
        assert!(matches!(
            min_upper(Bound::Included(b), Bound::Excluded(b)),
            Bound::Excluded(_)
        ));
        assert!(is_empty_range(Bound::Included(b), Bound::Included(a)));
        assert!(is_empty_range(Bound::Excluded(a), Bound::Excluded(a)));
        assert!(is_empty_range(Bound::Included(a), Bound::Excluded(a)));
        assert!(!is_empty_range(Bound::Included(a), Bound::Included(a)));
        assert!(!is_empty_range(Bound::Unbounded, Bound::Excluded(a)));
    }
}
