use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::cache::cluster::protocol::{Move, Slot};
use crate::cache::command::key_slot;
use crate::cache::store::{Effect, Effects, SetWhen};

use super::super::peer::{Chunk, Data, hash_effects};
use super::*;

fn node(node: u64) -> WorkerId {
    WorkerId {
        node,
        incarnation: nabedra_rt::ids::uuid_v4(),
    }
}

fn config(id: WorkerId) -> ActorConfig {
    ActorConfig {
        clock: ClockMode::LocalDocker,
        id,
        count: 1,
        slot_bytes: 4 * MIB,
        primary_bytes: 8 * MIB,
        replica_bytes: 8 * MIB,
        journal_bytes: 4 * MIB,
        snapshot_bytes: 4 * MIB,
        turns: TurnLimits {
            growth_bytes: 4 * MIB,
            reply_bytes: 2 * MIB,
        },
        max_pending: 2,
        eviction: EvictionPolicy::NoEviction,
    }
}

fn set(key: Bytes, value: Bytes) -> Operation {
    Operation::Set {
        key,
        value,
        ttl_ms: Some(5_000),
        when: SetWhen::Always,
    }
}

fn only(effect: Effect) -> Effects {
    let mut effects = Effects::new();
    effects.push(effect);
    effects
}

#[test]
fn k03_final_lease_check_never_publishes_staged_mutation() {
    let config = config(node(1));
    let key = Bytes::from_static(b"{fence}");
    let slot = key_slot(&key);
    let mut state = Shard::new(3, Role::Primary, true, config);
    state.lease = Some(Lease {
        not_before_ms: 10,
        expires_ms: 110,
    });
    let mut journal = Journal::new(config.journal_bytes).unwrap();
    let mut charge = StoreBytes::default();
    let staged = state
        .store
        .prepare(&set(key, Bytes::from_static(b"payload")), 100, config.turns);
    let outcome = publish(
        &config,
        || {
            Ok(ClockBounds {
                lower_ms: 109,
                upper_ms: 110,
            })
        },
        &mut journal,
        &mut charge,
        slot,
        &mut state,
        staged,
    );
    assert!(matches!(outcome, Published::Unavailable));
    assert!(state.store.is_empty());
    assert_eq!(state.sequence, 0);
    assert_eq!(charge.primary, 0);
    assert!(journal.get(slot, 3, 1).is_none());
}

#[test]
fn k03_freeze_survives_delayed_grants_and_retired_state_is_fenced() {
    let id = node(1);
    let config = config(id);
    let mut actor = State::new(0, config).unwrap();
    let key = Bytes::from_static(b"{move}");
    let index = usize::from(key_slot(&key));
    let slot = index as u16;
    let mut topology = Topology {
        revision: 1,
        ..Topology::default()
    };
    topology.slots[index] = Slot {
        epoch: 1,
        primary: Some(id),
        cold: true,
        ..Default::default()
    };
    let mut grants = vec![None; topology.slots.len()];
    grants[index] = Some(Lease {
        expires_ms: u64::MAX,
        not_before_ms: 0,
    });
    let old = Arc::new(topology.clone());
    actor.install(Arc::clone(&old), &grants);
    let operation = set(key.clone(), Bytes::from_static(b"before"));
    assert!(matches!(
        actor.execute(slot, 1, 1, nabedra_rt::time::now_std(), &operation),
        Response::Simple(_)
    ));
    topology.revision += 1;
    let target = node(2);
    topology.slots[index].moving = Some(Move {
        operation: nabedra_rt::ids::uuid_v4(),
        target,
        freeze_requested: true,
        frozen: None,
        replica_only: false,
    });
    actor.install(Arc::new(topology.clone()), &grants);
    actor.install(old, &grants);
    let current = actor.slots.get(&slot).unwrap();
    assert_eq!(current.frozen.unwrap().sequence, 1);
    assert_eq!(current.frozen.unwrap().lease_expires_ms, u64::MAX);
    assert!(
        matches!(actor.execute(slot, 1, 1, nabedra_rt::time::now_std(), &operation), Response::Error(error) if error.starts_with(b"TRYAGAIN"))
    );

    topology.generation += 1;
    topology.revision += 1;
    topology.slots[index].epoch += 1;
    topology.slots[index].moving = None;
    actor.install(Arc::new(topology), &grants);
    assert!(
        matches!(actor.execute(slot, 1, 1, nabedra_rt::time::now_std(), &Operation::Get(key.clone())), Response::Error(error) if error.starts_with(b"CLUSTERDOWN"))
    );
    assert!(matches!(
        actor.execute(
            slot,
            2,
            2,
            nabedra_rt::time::now_std(),
            &Operation::Get(key)
        ),
        Response::Bulk(None)
    ));
}

#[test]
fn k03_replica_rejects_gap_conflicting_duplicate_and_old_incarnation() {
    let owner = node(1);
    let config = config(node(2));
    let mut actor = State::new(0, config).unwrap();
    let key = Bytes::from_static(b"{copy}");
    let slot = key_slot(&key);
    actor
        .slots
        .insert(slot, Shard::new(2, Role::Copy(owner), true, config));
    let packet = |sequence, value, digest| Packet {
        slot,
        epoch: 2,
        data: Data::Delta {
            sequence,
            effects: only(Effect::Upsert {
                key: key.clone(),
                value: Bytes::from_static(value),
                deadline_ms: Some(600),
            }),
            digest: [digest; 32],
        },
    };
    actor.apply(owner, packet(2, b"gap", 2));
    let wrong = WorkerId {
        node: owner.node,
        incarnation: nabedra_rt::ids::uuid_v4(),
    };
    actor.apply(wrong, packet(1, b"forged", 1));
    assert_eq!(actor.slots[&slot].sequence, 0);
    actor.apply(owner, packet(1, b"expected", 1));
    let current = actor.storage.replica;
    actor.apply(owner, packet(1, b"expected", 1));
    actor.apply(owner, packet(1, b"conflict", 9));
    assert_eq!(actor.slots[&slot].sequence, 1);
    assert_eq!(actor.storage.replica, current);
    assert!(
        matches!(actor.slots[&slot].store.prepare(&Operation::Get(key.clone()), 100, config.turns).reply(), Reply::Bulk(Some(value)) if value == b"expected".as_slice())
    );
    assert!(matches!(
        actor.slots[&slot]
            .store
            .prepare(&Operation::Get(key), 600, config.turns)
            .reply(),
        Reply::Bulk(None)
    ));
}

#[test]
fn k03_snapshot_installs_after_verified_final_chunk_only() {
    let owner = node(1);
    let config = config(node(2));
    let mut state = Shard::new(7, Role::Copy(owner), false, config);
    let mut copies = Copies::new(config.snapshot_bytes, config.slot_bytes);
    let one = Bytes::from_static(b"{snapshot}a");
    let two = Bytes::from_static(b"{snapshot}b");
    let slot = key_slot(&one);
    let stream = nabedra_rt::ids::uuid_v4();
    let effect = |key| {
        only(Effect::Upsert {
            key,
            value: Bytes::from(vec![31; 700_000]),
            deadline_ms: Some(8_000),
        })
    };
    let a = effect(one.clone());
    let b = effect(two.clone());
    let mut digest = Sha256::new();
    hash_effects(&a, &mut digest);
    hash_effects(&b, &mut digest);
    let expected: [u8; 32] = digest.finalize().into();
    let mut charge = StoreBytes::default();
    let chunk = |offset, effects, checksum| Packet {
        slot,
        epoch: 7,
        data: Data::Snapshot(Chunk {
            id: stream,
            cut: 11,
            offset,
            effects,
            checksum,
        }),
    };
    copies
        .apply(&config, &mut state, chunk(0, a.clone(), None), &mut charge)
        .unwrap();
    assert!(state.store.is_empty());
    assert!(!state.baseline && copies.charged_bytes() > 700_000);
    assert!(
        copies
            .apply(
                &config,
                &mut state,
                chunk(1, b.clone(), Some([1; 32])),
                &mut charge
            )
            .is_err()
    );
    assert!(state.store.is_empty());
    assert_eq!(copies.charged_bytes(), 0);
    copies
        .apply(&config, &mut state, chunk(0, a, None), &mut charge)
        .unwrap();
    copies
        .apply(
            &config,
            &mut state,
            chunk(1, b, Some(expected)),
            &mut charge,
        )
        .unwrap();
    assert!(state.baseline);
    assert_eq!(state.sequence, 11);
    assert_eq!(state.store.len(), 2);
    assert_eq!(copies.charged_bytes(), 0);
    assert!(matches!(
        state
            .store
            .prepare(&Operation::Get(two), 8_000, config.turns)
            .reply(),
        Reply::Bulk(None)
    ));
}

#[test]
fn k02_replay_journal_retains_only_byte_bounded_prefix_tail() {
    let mut journal = Journal::new(2 * MIB).unwrap();
    let key = Bytes::from_static(b"{journal}");
    let slot = key_slot(&key);
    let effects = || {
        only(Effect::Upsert {
            key: key.clone(),
            value: Bytes::from(vec![1; 900_000]),
            deadline_ms: None,
        })
    };
    for sequence in 1..=3 {
        let values = effects();
        let bytes = values[0].charged_bytes();
        journal.reserve_append(bytes).unwrap();
        journal.append(slot, 1, sequence, values, bytes);
    }
    assert!(journal.get(slot, 1, 1).is_none());
    assert!(journal.get(slot, 1, 3).is_some());
    assert!(journal.reserve_append(2 * MIB).is_err());
}

#[test]
fn k03_epoch_handoff_keeps_only_exact_seeded_replica() {
    use crate::cache::cluster::protocol::ReplicaSeed;

    let prior = node(1);
    let next = node(2);
    let id = node(3);
    let config = config(id);
    let mut actor = State::new(0, config).unwrap();
    let key = Bytes::from_static(b"{replica-seed}");
    let slot = key_slot(&key);
    let index = usize::from(slot);
    let mut topology = Topology {
        revision: 1,
        ..Topology::default()
    };
    topology.slots[index] = Slot {
        epoch: 1,
        primary: Some(prior),
        replica: Some(id),
        ..Default::default()
    };
    let grants = vec![None; topology.slots.len()];
    actor.install(Arc::new(topology.clone()), &grants);
    actor.apply(
        prior,
        Packet {
            slot,
            epoch: 1,
            data: Data::Empty(0),
        },
    );
    actor.apply(
        prior,
        Packet {
            slot,
            epoch: 1,
            data: Data::Delta {
                sequence: 1,
                effects: only(Effect::Upsert {
                    key: key.clone(),
                    value: Bytes::from_static(b"value"),
                    deadline_ms: Some(1_000),
                }),
                digest: [1; 32],
            },
        },
    );
    assert_eq!(actor.slots[&slot].sequence, 1);

    topology.revision += 1;
    topology.slots[index].epoch += 1;
    topology.slots[index].primary = Some(next);
    topology.slots[index].replica_seed = Some(ReplicaSeed {
        previous_primary: prior,
        previous_epoch: 1,
        sequence: 1,
    });
    actor.install(Arc::new(topology.clone()), &grants);
    assert_eq!(actor.slots[&slot].role, Role::Copy(next));
    assert_eq!(actor.slots[&slot].sequence, 0);
    assert!(actor.slots[&slot].baseline);
    assert!(
        matches!(actor.slots[&slot].store.prepare(&Operation::Get(key), 900, config.turns).reply(), Reply::Bulk(Some(value)) if value == b"value".as_slice())
    );

    topology.revision += 1;
    topology.slots[index].epoch += 1;
    actor.install(Arc::new(topology), &grants);
    assert!(!actor.slots[&slot].baseline);
    assert!(actor.slots[&slot].store.is_empty());
}

#[test]
fn k02_promoted_then_refilled_roles_stay_within_total_data_capacity() {
    let id = node(1);
    let origin = node(2);
    let mut config = config(id);
    config.primary_bytes = MIB;
    config.replica_bytes = MIB;
    let mut actor = State::new(0, config).unwrap();
    let original = Bytes::from_static(b"{local-primary}");
    let promoted = Bytes::from_static(b"{promoted-copy}");
    let refill = Bytes::from_static(b"{new-copy}");
    let slot_a = key_slot(&original);
    let slot_b = key_slot(&promoted);
    let slot_c = key_slot(&refill);
    assert_ne!(slot_a, slot_b);
    assert_ne!(slot_b, slot_c);
    let mut topology = Topology {
        revision: 1,
        ..Topology::default()
    };
    let primary = Slot {
        epoch: 1,
        primary: Some(id),
        cold: true,
        ..Default::default()
    };
    let replica = Slot {
        epoch: 1,
        primary: Some(origin),
        replica: Some(id),
        ..Default::default()
    };
    topology.slots[usize::from(slot_a)] = primary;
    topology.slots[usize::from(slot_b)] = replica;
    let mut grants = vec![None; topology.slots.len()];
    grants[usize::from(slot_a)] = Some(Lease {
        expires_ms: u64::MAX,
        not_before_ms: 0,
    });
    actor.install(Arc::new(topology.clone()), &grants);
    let value = Bytes::from(vec![4; 900_000]);
    let op = set(original.clone(), value.clone());
    assert!(matches!(
        actor.execute(slot_a, 1, 1, nabedra_rt::time::now_std(), &op),
        Response::Simple(_)
    ));
    actor.apply(
        origin,
        Packet {
            slot: slot_b,
            epoch: 1,
            data: Data::Empty(0),
        },
    );
    actor.apply(
        origin,
        Packet {
            slot: slot_b,
            epoch: 1,
            data: Data::Delta {
                sequence: 1,
                effects: only(Effect::Upsert {
                    key: promoted.clone(),
                    value: value.clone(),
                    deadline_ms: None,
                }),
                digest: [4; 32],
            },
        },
    );
    let data_before = actor.storage.primary + actor.storage.replica;
    assert!(data_before > MIB && data_before <= 2 * MIB);

    topology.revision += 1;
    topology.slots[usize::from(slot_b)] = Slot {
        primary: Some(id),
        epoch: 2,
        ..Default::default()
    };
    topology.slots[usize::from(slot_c)] = replica;
    grants[usize::from(slot_b)] = grants[usize::from(slot_a)];
    actor.install(Arc::new(topology), &grants);
    assert_eq!(actor.storage.primary, data_before);
    assert!(
        matches!(actor.execute(slot_b, 1, 2, nabedra_rt::time::now_std(), &Operation::Get(promoted.clone())), Response::Bulk(Some(v)) if v == value)
    );
    let stream = || {
        let effects = only(Effect::Upsert {
            key: refill.clone(),
            value: value.clone(),
            deadline_ms: None,
        });
        let mut checksum = Sha256::new();
        hash_effects(&effects, &mut checksum);
        Packet {
            slot: slot_c,
            epoch: 1,
            data: Data::Snapshot(Chunk {
                id: nabedra_rt::ids::uuid_v4(),
                cut: 1,
                offset: 0,
                effects,
                checksum: Some(checksum.finalize().into()),
            }),
        }
    };
    actor.apply(origin, stream());
    assert!(!actor.slots[&slot_c].baseline);
    assert_eq!(actor.progress(false, None, 0).missing_replica_baselines, 1);
    assert_eq!(actor.storage.replica, 0);
    assert_eq!(actor.copies.charged_bytes(), 0);

    assert!(matches!(
        actor.execute(
            slot_a,
            1,
            1,
            nabedra_rt::time::now_std(),
            &Operation::Delete(vec![original])
        ),
        Response::Integer(1)
    ));
    actor.apply(origin, stream());
    assert!(actor.slots[&slot_c].baseline);
    assert_eq!(actor.progress(false, None, 0).missing_replica_baselines, 0);
    assert!(actor.storage.primary + actor.storage.replica <= 2 * MIB);
    assert!(
        matches!(actor.execute(slot_b, 1, 2, nabedra_rt::time::now_std(), &Operation::Get(promoted)), Response::Bulk(Some(v)) if v == value)
    );
}

#[test]
fn k02_k03_full_slot_can_pin_and_install_bounded_snapshot() {
    let origin = node(1);
    let recipient = node(2);
    let mut config = config(origin);
    config.slot_bytes = 2 * MIB;
    let (pin, _) = SlotStore::snapshot_requirements(config.slot_bytes);
    config.snapshot_bytes = 2 * (pin.max(config.slot_bytes) + 256);
    assert!(Copies::supports(config.slot_bytes, config.snapshot_bytes));
    assert!(!Copies::supports(
        config.slot_bytes,
        config.snapshot_bytes - 1
    ));

    let keys = [b"{full}one".as_slice(), b"{full}two".as_slice()].map(Bytes::from_static);
    let slot = key_slot(&keys[0]);
    assert_eq!(slot, key_slot(&keys[1]));
    let body = Bytes::from(vec![82; 900_000]);
    let mut source = Shard::new(4, Role::Primary, true, config);
    for key in &keys {
        let staged = source
            .store
            .prepare(&set(key.clone(), body.clone()), 100, config.turns);
        assert!(matches!(source.store.commit(staged), Reply::Ok));
    }
    let extra = source.store.prepare(
        &set(Bytes::from_static(b"{full}over"), body.clone()),
        100,
        config.turns,
    );
    assert!(matches!(
        extra.reply(),
        Reply::Error(StoreError::OutOfMemory)
    ));
    source.sequence = 7;
    let mut destination = Shard::new(4, Role::Copy(origin), false, config);
    let journal = Journal::new(config.journal_bytes).unwrap();
    let mut sender = Copies::new(config.snapshot_bytes, config.slot_bytes);
    let mut receiver = Copies::new(config.snapshot_bytes, config.slot_bytes);
    let mut storage = StoreBytes::default();
    for expected_complete in [false, true] {
        let mut wire = Vec::new();
        let probe = receiver.probe(slot, &destination);
        assert_eq!(probe.resume.is_some(), expected_complete);
        sender
            .fetch(recipient, &probe, &source, &journal, &mut wire, 2 * MIB)
            .unwrap();
        let packet = super::super::peer::single_record_for_test(&wire).unwrap();
        receiver
            .apply(&config, &mut destination, packet, &mut storage)
            .unwrap();
        assert_eq!(destination.baseline, expected_complete);
    }
    assert_eq!(destination.sequence, 7);
    assert_eq!(destination.store.len(), 2);
    assert_eq!(sender.charged_bytes() + receiver.charged_bytes(), 0);
    for key in keys {
        let get = Operation::Get(key);
        assert!(
            matches!(destination.store.prepare(&get, 5_099, config.turns).reply(), Reply::Bulk(Some(v)) if v == &body)
        );
        assert!(matches!(
            destination.store.prepare(&get, 5_100, config.turns).reply(),
            Reply::Bulk(None)
        ));
    }
}

/// Drives the real actor one turn at a time under its node clock.
#[cfg(feature = "sim")]
pub(in crate::cache::cluster) struct LeaseActorFixture(State);

#[cfg(feature = "sim")]
impl LeaseActorFixture {
    pub fn new(id: WorkerId) -> Self {
        Self(
            State::new(
                0,
                ActorConfig {
                    clock: ClockMode::ClockBound,
                    ..config(id)
                },
            )
            .unwrap(),
        )
    }

    pub fn install_topology(&mut self, topology: Topology) {
        let grants = vec![None; topology.slots.len()];
        self.0.install(Arc::new(topology), &grants);
    }

    pub fn install_update(&mut self, update: crate::cache::cluster::protocol::Update) {
        assert_eq!(update.worker, self.0.config.id);
        let topology = update.topology.expect("the controller sends its topology");
        let mut grants = vec![None; topology.slots.len()];
        for grant in update.grants {
            for slot in grant.range.first..=grant.range.last {
                let slot = usize::from(slot);
                assert_eq!(topology.slots[slot].primary, Some(update.worker));
                assert_eq!(topology.slots[slot].epoch, grant.range.epoch);
                assert!(grants[slot].replace(grant.lease).is_none());
            }
        }
        self.0.install(Arc::new(topology), &grants);
    }

    pub fn copy_first_write_from(&mut self, primary: &Self, slot: u16, epoch: u64) {
        let source = primary.0.config.id;
        self.0.apply(
            source,
            Packet {
                slot,
                epoch,
                data: Data::Empty(0),
            },
        );
        let effects = primary
            .0
            .journal
            .get(slot, epoch, 1)
            .expect("the primary published its first write")
            .clone();
        let mut digest = Sha256::new();
        hash_effects(&effects, &mut digest);
        self.0.apply(
            source,
            Packet {
                slot,
                epoch,
                data: Data::Delta {
                    sequence: 1,
                    effects,
                    digest: digest.finalize().into(),
                },
            },
        );
        assert_eq!(self.0.slots[&slot].sequence, 1);
        assert!(self.0.slots[&slot].baseline);
    }

    pub fn progress(&mut self, slot: u16) -> Progress {
        self.0
            .progress(false, None, 0)
            .slots
            .into_iter()
            .find(|progress| progress.slot == slot)
            .expect("the actor owns the slot")
    }

    pub fn write(&mut self, key: &'static [u8], value: &'static [u8], epoch: u64) -> Response {
        self.0.execute(
            key_slot(key),
            self.0.topology.generation,
            epoch,
            time::now_std(),
            &set(Bytes::from_static(key), Bytes::from_static(value)),
        )
    }
}
