use std::cmp::Ordering;

use super::*;
use crate::iter::testutil::{FakeResolver, Rng};
use crate::source::InternalIterator;
use crate::types::{compare_internal_keys, internal_key, parse_internal_key};

/// `0x40 ‖ 16-byte group id ‖ suffix`.
fn group_key(group: u8, suffix: &[u8]) -> Vec<u8> {
    let mut key = vec![0x40];
    key.extend_from_slice(&[0u8; 15]);
    key.push(group);
    key.extend_from_slice(suffix);
    key
}

fn put(mem: &Memtable, seq: Seq, ops: &[(&[u8], Option<&[u8]>)]) {
    let mut batch = WriteBatch::new();
    for (key, value) in ops {
        match value {
            Some(value) => batch.put(key, value),
            None => batch.delete(key),
        }
    }
    mem.insert(&batch, seq, &[]).unwrap();
}

fn found(value: &[u8], seq: Seq) -> Lookup {
    Lookup::Found(Kind::Put, Bytes::copy_from_slice(value), seq)
}

#[test]
fn ns01_memtable_get_returns_the_newest_version_at_each_snapshot() {
    let mem = Memtable::new(MemtableOptions::default(), None);
    put(&mem, 1, &[(b"a", Some(b"1")), (b"b", Some(b"1"))]);
    put(&mem, 3, &[(b"a", None), (b"b", Some(b"2"))]);
    let mut batch = WriteBatch::new();
    batch.delete_range(b"a", b"c");
    batch.put(b"a", b"3");
    mem.insert(&batch, 5, &[]).unwrap();

    assert_eq!(mem.get(b"a", 0), Lookup::NotFound);
    assert_eq!(mem.get(b"a", 1), found(b"1", 1));
    assert_eq!(mem.get(b"a", 2), found(b"1", 1));
    assert_eq!(mem.get(b"a", 3), Lookup::Deleted(3));
    assert_eq!(mem.get(b"a", 5), Lookup::Deleted(5));
    assert_eq!(
        mem.get(b"a", 6),
        found(b"3", 6),
        "a put after a covering range delete in the same batch wins"
    );
    assert_eq!(mem.get(b"b", 4), found(b"2", 4));
    assert_eq!(mem.get(b"b", 5), Lookup::Deleted(5));
    assert_eq!(
        mem.get(b"bb", 9),
        Lookup::Deleted(5),
        "range tombstone only"
    );
    assert_eq!(mem.get(b"c", 9), Lookup::NotFound, "end is exclusive");
    assert_eq!(mem.len(), 5);
    assert_eq!(mem.tombstone_count(), 1);
    assert_eq!((mem.smallest_seq(), mem.largest_seq()), (Some(1), Some(6)));
}

#[test]
fn ns01_memtable_keeps_locators_instead_of_journal_values() {
    let resolver = FakeResolver::default();
    let mem = Memtable::new(MemtableOptions::default(), None);
    let big = vec![7u8; 64 * 1024];
    let mut batch = WriteBatch::new();
    batch.put(b"state", b"s");
    batch.put_journal(b"log/1", &big);
    let locator = resolver.store(&big);
    assert!(matches!(
        mem.insert(&batch, 1, &[]),
        Err(Error::InvalidArgument(_))
    ));
    mem.insert(&batch, 1, &[locator]).unwrap();
    match mem.get(b"log/1", 2) {
        Lookup::Found(Kind::JournalPut, value, 2) => {
            assert_eq!(Locator::decode(&value), Some(locator));
            use crate::iter::ValueResolver;
            assert_eq!(resolver.resolve(&locator).unwrap(), big);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(
        mem.approximate_memory() < 1024,
        "the memtable must not hold journal values: {}",
        mem.approximate_memory()
    );
}

#[test]
fn ns_unit_memtable_shards_by_group_prefix_and_releases_its_budget() {
    let budget = Arc::new(MemtableBudget::new(1 << 20));
    let mem = Memtable::new(MemtableOptions::default(), Some(budget.clone()));
    assert_eq!(mem.shard_of(b"short"), 0);
    let shard = mem.shard_of(&group_key(9, b""));
    assert_eq!(mem.shard_of(&group_key(9, b"row/1")), shard);
    let groups: std::collections::HashSet<_> =
        (0..64).map(|g| mem.shard_of(&group_key(g, b"x"))).collect();
    assert!(groups.len() > 8, "groups spread over shards: {groups:?}");

    let (lo, hi) = (group_key(9, b""), group_key(10, b""));
    assert_eq!(
        mem.single_shard_for(Bound::Included(&lo), Bound::Excluded(&hi)),
        Some(shard)
    );
    assert_eq!(
        mem.single_shard_for(Bound::Included(&lo), Bound::Included(&hi)),
        None
    );
    assert_eq!(
        mem.single_shard_for(
            Bound::Excluded(&group_key(9, b"a")),
            Bound::Included(&group_key(9, b"z"))
        ),
        Some(shard)
    );
    assert_eq!(
        mem.single_shard_for(Bound::Included(&lo), Bound::Unbounded),
        None
    );
    assert_eq!(
        mem.single_shard_for(Bound::Unbounded, Bound::Excluded(&hi)),
        None
    );
    assert_eq!(
        mem.single_shard_for(Bound::Included(b"short"), Bound::Excluded(b"shorz")),
        None
    );
    let all_ff = [0xFFu8; 17];
    assert_eq!(
        mem.single_shard_for(Bound::Included(&all_ff), Bound::Excluded(&[0xFF; 18])),
        None
    );

    put(&mem, 1, &[(&group_key(1, b"a"), Some(b"v")), (b"k", None)]);
    let mut batch = WriteBatch::new();
    batch.delete_range(b"a", b"b");
    mem.insert(&batch, 3, &[]).unwrap();
    assert!(mem.approximate_memory() > 2 * ENTRY_OVERHEAD + TOMBSTONE_OVERHEAD);
    assert_eq!(budget.used(), mem.approximate_memory());
    drop(mem);
    assert_eq!(budget.used(), 0);
}

fn collect_forward<I: InternalIterator>(it: &mut I) -> Vec<(Vec<u8>, Bytes)> {
    let mut out = Vec::new();
    it.seek_to_first();
    while it.valid() {
        out.push((it.key().to_vec(), it.value()));
        it.next();
    }
    out
}

fn collect_reverse<I: InternalIterator>(it: &mut I) -> Vec<(Vec<u8>, Bytes)> {
    let mut out = Vec::new();
    it.seek_to_last();
    while it.valid() {
        out.push((it.key().to_vec(), it.value()));
        it.prev();
    }
    out.reverse();
    out
}

#[test]
fn ns21_memtable_iterators_merge_shards_in_internal_key_order() {
    let mut rng = Rng::new(0x3E3_0021);
    let mem = Arc::new(Memtable::new(MemtableOptions::default(), None));
    let mut expected = Vec::new();
    let mut seq = 1;
    for _ in 0..600 {
        let mut batch = WriteBatch::new();
        let n = 1 + rng.below(4);
        for _ in 0..n {
            let key = if rng.below(10) == 0 {
                vec![b'm', rng.below(20) as u8]
            } else {
                group_key(rng.below(8) as u8, &[rng.below(40) as u8])
            };
            let value = rng.next_u64().to_le_bytes();
            if rng.below(5) == 0 {
                batch.delete(&key);
                expected.push((internal_key(&key, seq, Kind::Delete), Bytes::new()));
            } else {
                batch.put(&key, &value);
                expected.push((
                    internal_key(&key, seq, Kind::Put),
                    Bytes::copy_from_slice(&value),
                ));
            }
            seq += 1;
        }
        mem.insert(&batch, seq - n, &[]).unwrap();
    }
    expected.sort_by(|a, b| compare_internal_keys(&a.0, &b.0));
    assert_eq!(collect_forward(&mut mem.iter()), expected);
    assert_eq!(collect_reverse(&mut mem.iter()), expected);

    // A group's bounds touch one shard and only that group's entries.
    let (lo, hi) = (group_key(3, b""), group_key(4, b""));
    let mut ranged = mem.iter_range(Bound::Included(&lo), Bound::Excluded(&hi));
    assert_eq!(ranged.children().len(), 1);
    let in_group: Vec<_> = expected
        .iter()
        .filter(|(k, _)| {
            let uk = parse_internal_key(k).unwrap().user_key;
            uk >= lo.as_slice() && uk < hi.as_slice()
        })
        .cloned()
        .collect();
    assert!(!in_group.is_empty());
    assert_eq!(collect_forward(&mut ranged), in_group);
    assert_eq!(collect_reverse(&mut ranged), in_group);

    // Seeks below and above the bounds clamp to them.
    ranged.seek(&internal_key(b"", 0, Kind::Put));
    assert_eq!(ranged.key(), in_group[0].0.as_slice());
    ranged.seek_for_prev(&internal_key(&[0xFF; 20], 0, Kind::Put));
    assert_eq!(ranged.key(), in_group.last().unwrap().0.as_slice());
    ranged.seek(&internal_key(&[0xFF; 20], 0, Kind::Put));
    assert!(!ranged.valid());
    ranged.seek_for_prev(&internal_key(b"", 0, Kind::Put));
    assert!(!ranged.valid());

    // Seek and seek_for_prev at every entry, then step both ways across windows.
    let mut it = mem.iter();
    for (i, (key, _)) in expected.iter().enumerate().step_by(7) {
        it.seek(key);
        assert_eq!(it.key(), key.as_slice());
        it.seek_for_prev(key);
        assert_eq!(it.key(), key.as_slice());
        for _ in 0..3 {
            if it.valid() {
                it.prev();
            }
        }
        let back = i.checked_sub(3);
        assert_eq!(it.valid(), back.is_some());
        if let Some(back) = back {
            assert_eq!(it.key(), expected[back].0.as_slice());
            it.next();
            assert_eq!(it.key(), expected[back + 1].0.as_slice());
        }
    }
    // Probing between versions: a seek key sorts before every version it can see.
    let probe = crate::types::seek_key(parse_internal_key(&expected[10].0).unwrap().user_key, 0);
    it.seek(&probe);
    assert!(it.valid());
    assert_ne!(compare_internal_keys(it.key(), &probe), Ordering::Less);
}

#[test]
fn ns01_memtable_point_reads_match_brute_force_over_many_range_tombstones() {
    let mut rng = Rng::new(0x3E3_7037);
    let mem = Memtable::new(MemtableOptions::default(), None);
    // (put key, or a range delete's (start, end), and its sequence).
    type Op = (Vec<u8>, Option<(Vec<u8>, Vec<u8>)>, Seq);
    let mut model: Vec<Op> = Vec::new();
    for seq in 1..400u64 {
        let mut batch = WriteBatch::new();
        let a = [rng.below(30) as u8];
        if rng.below(3) == 0 {
            let b = [rng.below(30) as u8];
            batch.delete_range(&a, &b);
            model.push((Vec::new(), Some((a.to_vec(), b.to_vec())), seq));
        } else {
            batch.put(&a, &seq.to_le_bytes());
            model.push((a.to_vec(), None, seq));
        }
        mem.insert(&batch, seq, &[]).unwrap();
        if seq % 37 == 0 {
            for key in 0..31u8 {
                let snapshot = rng.below(seq + 1);
                let point = model
                    .iter()
                    .filter(|(k, r, s)| r.is_none() && k.as_slice() == [key] && *s <= snapshot)
                    .map(|e| e.2)
                    .max();
                let tomb = model
                    .iter()
                    .filter_map(|(_, r, s)| {
                        let (a, b) = r.as_ref()?;
                        (*s <= snapshot
                            && a.as_slice() <= [key].as_slice()
                            && [key].as_slice() < b.as_slice())
                        .then_some(*s)
                    })
                    .max();
                let want = match (point, tomb) {
                    (Some(p), t) if t.is_none_or(|t| t < p) => found(&p.to_le_bytes(), p),
                    (_, Some(t)) => Lookup::Deleted(t),
                    (None, None) => Lookup::NotFound,
                    (Some(_), None) => unreachable!(),
                };
                assert_eq!(mem.get(&[key], snapshot), want, "key {key} at {snapshot}");
            }
        }
    }
    assert!(mem.tombstone_count() > LINEAR_TOMBSTONES);
    let fragmented: Vec<_> = mem.fragmented_tombstones().tombstones().collect();
    assert!(!fragmented.is_empty());
}
