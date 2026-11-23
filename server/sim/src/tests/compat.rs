//! Compatibility with the Events phase 6 simulator, and pinned values.
//!
//! `events` below is copied from `server/events/src/sim/mod.rs` and `net.rs` at 53a5716,
//! changed only to stand alone. The tests run both implementations on the same inputs.
//! The pinned constants catch a change to either side: if one fails, trace hashes of every
//! harness on this core change, and the planted-bug seed windows need a rerun.

use std::collections::BTreeSet;

use crate::net::{Filters, NetPlan, Plan};
use crate::rng::{Draw, SplitMix64, stream_seed, uuid_from};
use crate::trace::{Fnv, Trace, fnv};

/// The Events simulator's code, as it is today.
mod events {
    use std::collections::BTreeSet;

    #[derive(Clone, Debug)]
    pub struct SplitMix64(u64);

    impl SplitMix64 {
        pub fn new(seed: u64) -> Self {
            Self(seed)
        }

        pub fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        pub fn below(&mut self, n: u64) -> u64 {
            assert!(n > 0, "below(0)");
            ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
        }

        pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
            lo + self.below(hi - lo + 1)
        }

        pub fn chance(&mut self, per_mille: u64) -> bool {
            self.below(1000) < per_mille
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct Fnv(u64);

    impl Default for Fnv {
        fn default() -> Self {
            Self(0xcbf2_9ce4_8422_2325)
        }
    }

    impl Fnv {
        pub fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 ^= u64::from(*byte);
                self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }

        pub fn finish(&self) -> u64 {
            self.0
        }
    }

    /// `World::trace`'s hashing, without the lock.
    pub fn trace(hash: &mut Fnv, events: &mut u64, elapsed_ms: u64, line: std::fmt::Arguments<'_>) {
        let line = format!("{} {line}", elapsed_ms);
        hash.write(line.as_bytes());
        hash.write(b"\n");
        *events += 1;
    }

    #[derive(Default)]
    pub struct NetState {
        pub blocked: BTreeSet<(u64, u64)>,
        pub drop_per_mille: u64,
        pub dup_per_mille: u64,
        pub delay_max_ms: u64,
    }

    pub struct State {
        pub net_rng: SplitMix64,
        pub net: NetState,
        pub messages: u64,
    }

    const BASE_DELAY_MS: u64 = 1;

    // The fields are read through Debug, as the Events trace reads them.
    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum Plan {
        Drop,
        Deliver { delay_ms: u64, duplicate: bool },
    }

    pub fn plan(state: &mut State, from: u64, to: u64) -> Plan {
        state.messages += 1;
        if state.net.blocked.contains(&(from, to)) {
            return Plan::Drop;
        }
        let drop = state.net.drop_per_mille;
        let dup = state.net.dup_per_mille;
        let delay_max = state.net.delay_max_ms;
        let rng = &mut state.net_rng;
        if drop > 0 && rng.chance(drop) {
            return Plan::Drop;
        }
        let delay_ms = BASE_DELAY_MS
            + if delay_max > 0 {
                rng.below(delay_max + 1)
            } else {
                0
            };
        let duplicate = dup > 0 && rng.chance(dup);
        Plan::Deliver {
            delay_ms,
            duplicate,
        }
    }

    /// `Cluster::new`'s and `Client::uuid`'s id construction.
    pub fn uuid(rng: &mut SplitMix64) -> uuid::Uuid {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&rng.next_u64().to_le_bytes());
        bytes[8..].copy_from_slice(&rng.next_u64().to_le_bytes());
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }
}

#[test]
fn fnv_matches_the_published_vectors_and_events() {
    // FNV-1a 64 test vectors (Fowler, Noll and Vo).
    assert_eq!(fnv(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv(b"foobar"), 0x8594_4171_f739_67e8);
    for input in [&b""[..], b"x", b"send n1->n2 append Drop", &[0xff; 300]] {
        let mut reference = events::Fnv::default();
        reference.write(input);
        let mut ours = Fnv::default();
        ours.write(input);
        assert_eq!(ours.finish(), reference.finish());
    }
}

#[test]
fn splitmix64_matches_events_check_and_the_published_sequence() {
    // SplitMix64 from seed 0 (Vigna's reference output).
    let mut zero = SplitMix64::new(0);
    assert_eq!(
        [zero.next_u64(), zero.next_u64(), zero.next_u64()],
        [
            0xe220_a839_7b1d_cdaf,
            0x6e78_9e6a_a1b9_65f4,
            0x06c4_5d18_8009_454f
        ]
    );
    for seed in [0, 1, 7, 0x5eed, u64::MAX] {
        let mut ours = SplitMix64::new(seed);
        let mut reference = events::SplitMix64::new(seed);
        let mut check = nabedra_check::rng::SplitMix64::new(seed);
        for _ in 0..64 {
            let value = ours.next_u64();
            assert_eq!(value, reference.next_u64());
            assert_eq!(value, check.next_u64());
        }
        for n in [1, 2, 3, 5, 1000, u64::MAX] {
            assert_eq!(ours.below(n), reference.below(n));
        }
        for (lo, hi) in [(0, 0), (1, 3), (200, 800), (150, 1_200), (0, u64::MAX - 1)] {
            assert_eq!(ours.range(lo, hi), reference.range(lo, hi));
        }
        for per_mille in [0, 1, 100, 500, 999, 1000] {
            assert_eq!(ours.chance(per_mille), reference.chance(per_mille));
        }
    }
}

#[test]
fn uuids_match_events() {
    for seed in [0, 3, 99] {
        let (mut ours, mut reference) = (SplitMix64::new(seed), events::SplitMix64::new(seed));
        for _ in 0..8 {
            assert_eq!(uuid_from(&mut ours), events::uuid(&mut reference));
        }
    }
    let id = uuid_from(&mut SplitMix64::new(7));
    assert_eq!(id.get_version_num(), 4);
    assert_eq!(id.to_string(), PINNED_UUID_SEED_7);
}

const LINES: &[(u64, &str)] = &[
    (0, "start n1"),
    (
        10,
        "send n1->n2 append Deliver { delay_ms: 3, duplicate: false }",
    ),
    (11, "reply n2->n1 append Drop served"),
    (25, "crash n2"),
    (25, "apply n1 p0 T1-N1-4 9f2a"),
];

#[test]
fn trace_hash_matches_events_world_trace() {
    let mut ours = Trace::with_tail(2);
    let (mut reference, mut events) = (events::Fnv::default(), 0);
    for (ms, line) in LINES {
        ours.record(*ms, format_args!("{line}"));
        events::trace(&mut reference, &mut events, *ms, format_args!("{line}"));
    }
    assert_eq!((ours.hash(), ours.events()), (reference.finish(), events));
    assert_eq!(ours.hash(), PINNED_TRACE_HASH);
    assert_eq!(
        ours.tail(),
        vec![
            "25 crash n2".to_owned(),
            "25 apply n1 p0 T1-N1-4 9f2a".to_owned()
        ]
    );
    // `push` with the prefix already applied hashes the same bytes.
    let mut pushed = Trace::with_tail(0);
    for (ms, line) in LINES {
        pushed.push(format!("{ms} {line}"));
    }
    assert_eq!(pushed.hash(), ours.hash());
    assert!(pushed.tail().is_empty());
}

#[test]
fn trace_keeps_a_bounded_tail_and_optionally_everything() {
    let mut trace = Trace::with_tail(3).keep_full(true);
    for i in 0..10 {
        trace.record(i, format_args!("line {i}"));
    }
    assert_eq!(trace.events(), 10);
    assert_eq!(trace.tail(), ["7 line 7", "8 line 8", "9 line 9"]);
    assert_eq!(trace.full().map(<[String]>::len), Some(10));
    assert_eq!(Trace::with_tail(3).full(), None);
    // The tail and the full trace never change the hash.
    let mut bare = Trace::with_tail(0);
    for i in 0..10 {
        bare.record(i, format_args!("line {i}"));
    }
    assert_eq!(bare.hash(), trace.hash());
    // Order matters.
    let mut swapped = Trace::with_tail(0);
    swapped.record(1, format_args!("b"));
    swapped.record(0, format_args!("a"));
    let mut ordered = Trace::with_tail(0);
    ordered.record(0, format_args!("a"));
    ordered.record(1, format_args!("b"));
    assert_ne!(swapped.hash(), ordered.hash());
}

/// Drives both planners through the same blocks, filters and messages.
#[test]
fn net_plan_matches_events_plan() {
    for seed in 0..40u64 {
        let mut driver = SplitMix64::new(seed ^ 0xabcd);
        let mut ours = NetPlan::with_rng(SplitMix64::new(seed));
        let mut reference = events::State {
            net_rng: events::SplitMix64::new(seed),
            net: events::NetState::default(),
            messages: 0,
        };
        for step in 0..400 {
            if step % 50 == 0 {
                let filters = Filters {
                    drop_per_mille: crate::net::DROP_PER_MILLE[driver.below(5) as usize],
                    dup_per_mille: crate::net::DUP_PER_MILLE[driver.below(3) as usize],
                    delay_max_ms: crate::net::DELAY_MAX_MS[driver.below(4) as usize],
                };
                ours.set_filters(filters);
                reference.net.drop_per_mille = filters.drop_per_mille;
                reference.net.dup_per_mille = filters.dup_per_mille;
                reference.net.delay_max_ms = filters.delay_max_ms;
            }
            if step % 70 == 0 {
                let (from, to) = (driver.range(1, 3), driver.range(1, 3));
                ours.block(from, to);
                reference.net.blocked.insert((from, to));
            }
            if step % 130 == 0 {
                ours.heal_links();
                reference.net.blocked.clear();
            }
            let (from, to) = (driver.range(1, 3), driver.range(1, 3));
            let a = ours.plan(from, to);
            let b = events::plan(&mut reference, from, to);
            assert_eq!(
                format!("{a:?}"),
                format!("{b:?}"),
                "seed {seed} step {step}"
            );
        }
        assert_eq!(ours.messages(), reference.messages);
    }
}

#[test]
fn net_plan_values_are_pinned() {
    let mut plan = NetPlan::with_rng(SplitMix64::new(42));
    plan.set_filters(Filters {
        drop_per_mille: 150,
        dup_per_mille: 50,
        delay_max_ms: 30,
    });
    let plans: Vec<String> = (0..8)
        .map(|i| format!("{:?}", plan.plan(1, 2 + i % 2)))
        .collect();
    assert_eq!(plans, PINNED_PLANS_SEED_42);
    // The derived net stream of seed 1 (what a new harness gets from `NetPlan::new`).
    let mut derived = NetPlan::new(1);
    derived.set_filters(Filters {
        drop_per_mille: 0,
        dup_per_mille: 0,
        delay_max_ms: 120,
    });
    let delays: Vec<Plan> = (0..4).map(|_| derived.plan(1, 2)).collect();
    assert_eq!(
        delays
            .iter()
            .map(|plan| match plan {
                Plan::Deliver { delay_ms, .. } => *delay_ms,
                Plan::Drop => 0,
            })
            .collect::<Vec<_>>(),
        PINNED_DELAYS_NET_SEED_1
    );
}

#[test]
fn stream_seeds_are_pinned_and_independent() {
    assert_eq!(stream_seed(1, "net"), PINNED_STREAM_NET_1);
    assert_eq!(stream_seed(1, "workload"), PINNED_STREAM_WORKLOAD_1);
    let seeds: BTreeSet<u64> = ["net", "workload", "nemesis", "client"]
        .iter()
        .flat_map(|name| (0..50).map(move |seed| stream_seed(seed, name)))
        .collect();
    assert_eq!(seeds.len(), 200, "stream seeds collide");
}

// Pinned on 2026-11-23 from this crate, after the reference comparisons above passed.
const PINNED_UUID_SEED_7: &str = "d70d3259-e4e1-4b63-9c66-3cf4d73c4c04";
const PINNED_TRACE_HASH: u64 = 0xa1c4_e806_e814_dcf5;
const PINNED_PLANS_SEED_42: [&str; 8] = [
    "Deliver { delay_ms: 5, duplicate: false }",
    "Deliver { delay_ms: 2, duplicate: false }",
    "Deliver { delay_ms: 25, duplicate: false }",
    "Deliver { delay_ms: 7, duplicate: false }",
    "Deliver { delay_ms: 17, duplicate: false }",
    "Deliver { delay_ms: 4, duplicate: false }",
    "Drop",
    "Deliver { delay_ms: 30, duplicate: false }",
];
const PINNED_DELAYS_NET_SEED_1: [u64; 4] = [34, 80, 2, 79];
const PINNED_STREAM_NET_1: u64 = 0xf3aa_bfdb_4a01_018e;
const PINNED_STREAM_WORKLOAD_1: u64 = 0x923c_a1ea_d4a2_d00e;
