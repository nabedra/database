use super::*;

const ROOM: usize = 1 << 22;

fn key(bytes: &'static [u8]) -> Bytes {
    Bytes::from_static(bytes)
}

fn set(key: Bytes, value: Bytes, ttl_ms: Option<u64>, when: SetWhen) -> Operation {
    Operation::Set {
        key,
        value,
        ttl_ms,
        when,
    }
}

fn run(slot: &mut SlotStore, command: Operation, now_ms: u64) -> Reply {
    let prepared = slot.prepare(&command, now_ms, TurnLimits::default());
    slot.commit(prepared)
}

fn ttl(key: Bytes) -> Operation {
    Operation::Ttl {
        key,
        milliseconds: true,
    }
}

#[test]
fn k01_binary_conditional_ttl_and_multikey_semantics() {
    let mut slot = SlotStore::new(ROOM, EvictionPolicy::default());
    let a = key(b"a\0\r\n");
    let b = key(b"\xff\x00");
    let empty = Bytes::new();
    assert_eq!(
        run(
            &mut slot,
            set(a.clone(), empty.clone(), Some(100), SetWhen::Absent),
            10
        ),
        Reply::Ok
    );
    assert_eq!(
        run(&mut slot, Operation::Get(a.clone()), 11),
        Reply::Bulk(Some(empty.clone()))
    );
    let value = key(b"\xff\r\n\0");
    assert_eq!(
        run(
            &mut slot,
            set(a.clone(), value.clone(), Some(1000), SetWhen::Absent),
            12
        ),
        Reply::Bulk(None)
    );
    assert_eq!(run(&mut slot, ttl(a.clone()), 12), Reply::Integer(98));
    assert_eq!(
        run(
            &mut slot,
            set(b.clone(), value.clone(), None, SetWhen::Present),
            12
        ),
        Reply::Bulk(None)
    );
    assert_eq!(
        run(
            &mut slot,
            set(b.clone(), value.clone(), Some(100), SetWhen::Always),
            12
        ),
        Reply::Ok
    );
    let keys = vec![a.clone(), b.clone(), a.clone(), key(b"missing")];
    assert_eq!(
        run(&mut slot, Operation::GetMany(keys.clone()), 13),
        Reply::Array(vec![
            Some(empty.clone()),
            Some(value.clone()),
            Some(empty),
            None
        ])
    );
    assert_eq!(
        run(&mut slot, Operation::Exists(keys.clone()), 13),
        Reply::Integer(3)
    );
    assert_eq!(
        run(
            &mut slot,
            set(b.clone(), value.clone(), None, SetWhen::Present),
            20
        ),
        Reply::Ok
    );
    assert_eq!(run(&mut slot, ttl(b.clone()), 20), Reply::Integer(-1));
    assert_eq!(
        run(
            &mut slot,
            Operation::Expire {
                key: b.clone(),
                ttl_ms: 501
            },
            20
        ),
        Reply::Integer(1)
    );
    let seconds = Operation::Ttl {
        key: b.clone(),
        milliseconds: false,
    };
    assert_eq!(run(&mut slot, seconds, 20), Reply::Integer(1));
    assert_eq!(
        run(&mut slot, Operation::Persist(b.clone()), 30),
        Reply::Integer(1)
    );
    assert_eq!(
        run(&mut slot, Operation::Persist(b.clone()), 30),
        Reply::Integer(0)
    );
    assert_eq!(
        run(
            &mut slot,
            Operation::Expire {
                key: b.clone(),
                ttl_ms: 0
            },
            40
        ),
        Reply::Integer(1)
    );
    assert_eq!(
        run(
            &mut slot,
            Operation::Expire {
                key: b.clone(),
                ttl_ms: -1
            },
            41
        ),
        Reply::Integer(0)
    );
    assert_eq!(run(&mut slot, ttl(b.clone()), 40), Reply::Integer(-2));
    assert_eq!(
        run(&mut slot, set(b.clone(), value, None, SetWhen::Always), 41),
        Reply::Ok
    );
    assert_eq!(
        run(&mut slot, Operation::Delete(keys), 41),
        Reply::Integer(2)
    );
    assert_eq!(slot.charged_bytes(), 0);
    assert!(slot.is_empty());
}

#[test]
fn k01_checked_integer_matches_canonical_valkey_syntax_and_preserves_expiry() {
    let mut slot = SlotStore::new(ROOM, EvictionPolicy::default());
    let counter = key(b"counter");
    let incr = |delta| Operation::Increment {
        key: counter.clone(),
        delta,
    };
    let set_integer = |value| set(counter.clone(), key(value), Some(400), SetWhen::Always);

    assert_eq!(run(&mut slot, incr(i64::MIN), 10), Reply::Integer(i64::MIN));
    assert_eq!(
        run(&mut slot, incr(-1), 10),
        Reply::Error(StoreError::IntegerOverflow)
    );
    assert_eq!(
        run(&mut slot, Operation::Get(counter.clone()), 10),
        Reply::Bulk(Some(key(b"-9223372036854775808")))
    );
    assert_eq!(
        run(&mut slot, set_integer(b"9223372036854775806"), 11),
        Reply::Ok
    );
    assert_eq!(run(&mut slot, incr(1), 12), Reply::Integer(i64::MAX));
    assert_eq!(
        run(&mut slot, incr(1), 12),
        Reply::Error(StoreError::IntegerOverflow)
    );
    assert_eq!(
        run(&mut slot, ttl(counter.clone()), 12),
        Reply::Integer(399)
    );
    assert_eq!(
        run(&mut slot, Operation::Get(counter.clone()), 13),
        Reply::Bulk(Some(key(b"9223372036854775807")))
    );
    for invalid in [
        b"01".as_slice(),
        b"-0",
        b"+1",
        b"1 ",
        b"1\0",
        b"9223372036854775808",
        b"",
    ] {
        assert_eq!(run(&mut slot, set_integer(invalid), 14), Reply::Ok);
        assert_eq!(
            run(&mut slot, incr(0), 15),
            Reply::Error(StoreError::InvalidInteger),
            "{invalid:?}"
        );
        assert_eq!(
            run(&mut slot, Operation::Get(counter.clone()), 15),
            Reply::Bulk(Some(key(invalid)))
        );
        assert_eq!(
            run(&mut slot, ttl(counter.clone()), 15),
            Reply::Integer(399)
        );
    }
    assert_eq!(run(&mut slot, incr(42), 414), Reply::Integer(42));
    assert_eq!(run(&mut slot, ttl(counter), 414), Reply::Integer(-1));
}

#[test]
fn k02_no_eviction_output_and_growth_rejection_leave_values_intact() {
    let mut slot = SlotStore::new(1024, EvictionPolicy::NoEviction);
    let a = key(b"a");
    let value = Bytes::from(vec![8; 100]);
    assert_eq!(
        run(
            &mut slot,
            set(a.clone(), value.clone(), Some(50), SetWhen::Always),
            10
        ),
        Reply::Ok
    );
    let before = slot.charged_bytes();
    let too_big = Bytes::from(vec![9; 900]);
    assert_eq!(
        run(
            &mut slot,
            set(a.clone(), too_big, None, SetWhen::Always),
            11
        ),
        Reply::Error(StoreError::OutOfMemory)
    );
    assert_eq!(slot.charged_bytes(), before);
    assert_eq!(run(&mut slot, ttl(a.clone()), 11), Reply::Integer(49));

    let limits = TurnLimits {
        growth_bytes: 0,
        reply_bytes: 256,
    };
    let ordinary = set(key(b"new"), value.clone(), None, SetWhen::Always);
    let prepared = slot.prepare(&ordinary, 12, limits);
    assert_eq!(prepared.reply(), &Reply::Error(StoreError::OutOfMemory));
    assert!(prepared.effects().is_empty());
    assert_eq!(prepared.charge_after(), before);
    assert_eq!(slot.commit(prepared), Reply::Error(StoreError::OutOfMemory));

    let limits = TurnLimits {
        growth_bytes: ROOM,
        reply_bytes: 200,
    };
    let repeated = Operation::GetMany(vec![a.clone(), a.clone()]);
    let prepared = slot.prepare(&repeated, 13, limits);
    assert!(prepared.effects().is_empty());
    assert_eq!(slot.commit(prepared), Reply::Error(StoreError::ReplyLimit));
    assert_eq!(
        run(&mut slot, Operation::Get(a), 14),
        Reply::Bulk(Some(value))
    );
    assert_eq!(slot.charged_bytes(), before);
}

#[test]
fn k02_sampled_lru_applies_exact_victims_only_on_successful_commit() {
    let mut slot = SlotStore::new(2200, EvictionPolicy::SampledLru);
    let value = Bytes::from(vec![1; 400]);
    let a = key(b"a");
    let b = key(b"b");
    let c = key(b"c");
    for key in [&a, &b, &c] {
        assert_eq!(
            run(
                &mut slot,
                set(key.clone(), value.clone(), None, SetWhen::Always),
                1
            ),
            Reply::Ok
        );
    }
    assert_eq!(
        run(&mut slot, Operation::Get(a.clone()), 2),
        Reply::Bulk(Some(value.clone()))
    );
    let before = slot.charged_bytes();
    let cannot_fit = set(key(b"d"), Bytes::from(vec![9; 2100]), None, SetWhen::Always);
    assert_eq!(
        run(&mut slot, cannot_fit, 2),
        Reply::Error(StoreError::OutOfMemory)
    );
    assert_eq!(slot.charged_bytes(), before);
    assert_eq!(slot.len(), 3);

    let can_fit = set(key(b"d"), value.clone(), None, SetWhen::Always);
    let prepared = slot.prepare(&can_fit, 3, TurnLimits::default());
    assert_eq!(prepared.reply(), &Reply::Ok);
    assert_eq!(prepared.effects().first(), Some(&Effect::Delete(b.clone())));
    assert_eq!(slot.charged_bytes(), before);
    drop(prepared);
    assert_eq!(slot.len(), 3);
    let prepared = slot.prepare(&can_fit, 3, TurnLimits::default());
    let (reply, effects) = slot.commit_with_effects(prepared);
    assert_eq!(reply, Reply::Ok);
    assert_eq!(effects.len(), 2);
    assert_eq!(run(&mut slot, Operation::Get(b), 3), Reply::Bulk(None));
    assert_eq!(
        run(&mut slot, Operation::Get(a), 3),
        Reply::Bulk(Some(value.clone()))
    );
    assert_eq!(
        run(&mut slot, Operation::Get(c), 3),
        Reply::Bulk(Some(value))
    );
    assert!(slot.charged_bytes() <= 2200);
}

#[test]
fn k02_expiry_heap_churn_and_independent_slot_progress() {
    let mut slot = SlotStore::new(ROOM, EvictionPolicy::NoEviction);
    let mut independent = SlotStore::new(ROOM, EvictionPolicy::NoEviction);
    let keys: Vec<_> = (0..97)
        .map(|number| Bytes::from(number.to_string()))
        .collect();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            run(
                &mut slot,
                set(
                    key.clone(),
                    key.clone(),
                    Some(index as u64 + 1),
                    SetWhen::Always
                ),
                0
            ),
            Reply::Ok
        );
    }
    let before = slot.charged_bytes();
    for round in 0..5000u64 {
        let position = (round * 23) as usize % keys.len();
        let key = keys[position].clone();
        let ttl_ms = ((round * 19) % 97) + 1;
        assert_eq!(
            run(
                &mut slot,
                set(key.clone(), key.clone(), Some(ttl_ms), SetWhen::Always),
                0
            ),
            Reply::Ok
        );
        assert_eq!(run(&mut slot, ttl(key), 0), Reply::Integer(ttl_ms as i64));
        assert_eq!(slot.charged_bytes(), before);
    }
    let due = slot.prepare_expiry(97, 4, Duration::ZERO);
    assert!(due.effects().is_empty());
    for processed in (0..97).step_by(4) {
        let due = slot.prepare_expiry(97, 4, Duration::from_secs(1));
        assert_eq!(due.effects().len(), (97 - processed).min(4));
        assert!(slot.charged_bytes() <= before);
        assert_eq!(slot.commit(due), Reply::Ok);
        let reply = run(
            &mut independent,
            Operation::Increment {
                key: key(b"live"),
                delta: 1,
            },
            97,
        );
        assert_eq!(reply, Reply::Integer((processed / 4 + 1) as i64));
    }
    assert!(slot.is_empty());
    assert_eq!(slot.charged_bytes(), 0);
}

#[test]
fn k03_final_clock_validation_and_bounded_pinned_snapshot_preserve_deadlines() {
    let mut source = SlotStore::new(ROOM, EvictionPolicy::NoEviction);
    let mut replica = SlotStore::new(ROOM, EvictionPolicy::NoEviction);
    let a = key(b"k");
    let value = key(b"original");
    let command = set(a.clone(), value.clone(), Some(10), SetWhen::Always);
    let mut prepared = source.prepare(&command, 100, TurnLimits::default());
    assert!(source.is_empty());
    source.reserve(&mut prepared).unwrap();
    prepared.refresh_time(105).unwrap();
    let (reply, delta) = source.commit_with_effects(prepared);
    assert_eq!(reply, Reply::Ok);
    assert_eq!(
        delta.as_slice(),
        &[Effect::Upsert {
            key: a.clone(),
            value: value.clone(),
            deadline_ms: Some(115)
        }]
    );

    let mut stale_read = source.prepare(&Operation::Get(a.clone()), 114, TurnLimits::default());
    assert_eq!(stale_read.refresh_time(115), Err(StoreError::StaleTurn));
    let mut stale_error = source.prepare(
        &Operation::Increment {
            key: a.clone(),
            delta: 1,
        },
        114,
        TurnLimits::default(),
    );
    assert_eq!(
        stale_error.reply(),
        &Reply::Error(StoreError::InvalidInteger)
    );
    assert_eq!(stale_error.refresh_time(115), Err(StoreError::StaleTurn));
    assert_eq!(
        source.pin_snapshot(0, 1, Duration::from_secs(1)).err(),
        Some(StoreError::SnapshotLimit)
    );
    assert_eq!(
        source.pin_snapshot(ROOM, 0, Duration::from_secs(1)).err(),
        Some(StoreError::SnapshotLimit)
    );
    assert_eq!(
        source.pin_snapshot(ROOM, 1, Duration::ZERO).err(),
        Some(StoreError::SnapshotLimit)
    );
    let snapshot = source
        .pin_snapshot(ROOM, 1, Duration::from_secs(1))
        .unwrap();
    assert_eq!(snapshot.chunk(0, 1, 1), Err(StoreError::SnapshotLimit));
    assert_eq!(
        snapshot.chunk(0, 1, snapshot.charged_bytes()).unwrap(),
        delta.as_slice()
    );
    assert_eq!(
        run(
            &mut source,
            set(a.clone(), key(b"new"), None, SetWhen::Always),
            112
        ),
        Reply::Ok
    );

    let snapshot_chunk: Effects = snapshot.effects().iter().cloned().collect();
    let prepared = replica
        .prepare_replica(snapshot_chunk.clone(), ROOM)
        .unwrap();
    assert_eq!(replica.commit(prepared), Reply::Ok);
    assert_eq!(
        run(&mut replica, Operation::Get(a.clone()), 114),
        Reply::Bulk(Some(value))
    );
    assert_eq!(run(&mut replica, ttl(a.clone()), 114), Reply::Integer(1));
    assert_eq!(
        run(&mut replica, Operation::Get(a.clone()), 115),
        Reply::Bulk(None)
    );
    let duplicate = snapshot_chunk
        .iter()
        .chain(&snapshot_chunk)
        .cloned()
        .collect();
    assert_eq!(
        replica.prepare_replica(duplicate, ROOM).err(),
        Some(StoreError::InvalidEffects)
    );

    let old_effects = source.prepare(
        &Operation::Delete(vec![a.clone()]),
        116,
        TurnLimits::default(),
    );
    assert_eq!(
        run(
            &mut source,
            set(a.clone(), key(b"latest"), None, SetWhen::Always),
            116
        ),
        Reply::Ok
    );
    assert_eq!(
        source.commit(old_effects),
        Reply::Error(StoreError::StaleTurn)
    );
    assert_eq!(
        run(&mut source, Operation::Get(a), 116),
        Reply::Bulk(Some(key(b"latest")))
    );
}
