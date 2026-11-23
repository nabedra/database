//! A toy cluster on the real `nabedra_rt::sim::World`: three nodes ping each other through
//! a [`NetPlan`], under a nemesis drawn from the tape, with every send, delivery and
//! nemesis action traced. It stands in for a harness in the sweep, shrink and planted
//! search tests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nabedra_check::Gen;
use nabedra_rt::sim::{NodeClock, World};
use serde_json::json;

use crate::nemesis::{Nemesis, Profile};
use crate::net::{Filters, NetPlan, Plan, draw_partition};
use crate::raft::RaftObserver;
use crate::runtime::{self, virtual_ms};
use crate::sweep::SeedRun;
use crate::trace::Trace;

pub const NODES: [u64; 3] = [1, 2, 3];
const CHAOS_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    Calm,
    Crash,
    Restart(u64),
    Isolate,
    Heal,
    Pause,
    Resume(u64),
    Filters,
    Clock,
}

/// Knobs that make a run misbehave.
#[derive(Clone, Copy, Debug, Default)]
pub struct Knobs {
    /// Report a violation when a node receives a duplicate (a planted bug).
    pub flag_duplicates: bool,
    /// Trace a process-wide counter, so two runs of one seed differ.
    pub leak_global_state: bool,
    /// Panic in the harness.
    pub panic: bool,
}

static LEAK: AtomicU64 = AtomicU64::new(0);

struct Shared {
    world: Arc<World>,
    trace: Trace,
    net: NetPlan,
    observer: RaftObserver,
    received: BTreeMap<u64, u64>,
    knobs: Knobs,
}

type State = Arc<Mutex<Shared>>;

fn lock(state: &State) -> std::sync::MutexGuard<'_, Shared> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn trace(state: &State, line: std::fmt::Arguments<'_>) {
    let mut shared = lock(state);
    let now = virtual_ms(&shared.world);
    shared.trace.record(now, line);
}

/// One node's loop: every 5 to 40 ms of its own clock it pings a random peer.
fn start(state: &State, node: u64) {
    let world = lock(state).world.clone();
    let task_state = state.clone();
    world.spawn_node(node, async move {
        let state = task_state;
        let mut sequence = 0u64;
        loop {
            let wait = 5 + nabedra_rt::ids::random_u64() % 36;
            nabedra_rt::time::sleep(Duration::from_millis(wait)).await;
            while lock(&state).net.is_paused(node) {
                nabedra_rt::time::sleep(Duration::from_millis(5)).await;
            }
            let to = NODES[(nabedra_rt::ids::random_u64() % 3) as usize];
            if to == node {
                continue;
            }
            sequence += 1;
            send(&state, node, to, sequence);
        }
    });
}

fn send(state: &State, from: u64, to: u64, sequence: u64) {
    let (plan, world) = {
        let mut shared = lock(state);
        (shared.net.plan(from, to), shared.world.clone())
    };
    trace(
        state,
        format_args!("send n{from}->n{to} #{sequence} {plan:?}"),
    );
    let Plan::Deliver {
        delay_ms,
        duplicate,
    } = plan
    else {
        return;
    };
    for copy in 0..=u64::from(duplicate) {
        let state = state.clone();
        world.spawn_node(to, async move {
            nabedra_rt::time::sleep(Duration::from_millis(delay_ms + copy)).await;
            while lock(&state).net.is_paused(to) {
                nabedra_rt::time::sleep(Duration::from_millis(5)).await;
            }
            receive(&state, from, to, sequence, copy);
        });
    }
}

fn receive(state: &State, from: u64, to: u64, sequence: u64, copy: u64) {
    trace(
        state,
        format_args!("recv n{from}->n{to} #{sequence} copy {copy}"),
    );
    let mut shared = lock(state);
    *shared.received.entry(to).or_default() += 1;
    if copy > 0 && shared.knobs.flag_duplicates {
        let now = virtual_ms(&shared.world);
        shared.observer.violate(
            now,
            format!("DuplicateDelivered: n{to} received #{sequence} from n{from} twice"),
        );
    }
}

fn apply(state: &State, nemesis: &mut Nemesis<Act>, g: &mut Gen, action: Act) {
    let world = lock(state).world.clone();
    let now = virtual_ms(&world);
    let pick = NODES[g.below(3) as usize];
    match action {
        Act::Calm => {}
        Act::Crash => {
            if world.is_up(pick) && NODES.iter().all(|node| world.is_up(*node)) {
                world.crash(pick);
                lock(state).net.bump_epoch();
                nemesis.after(now + 100 + g.below(400), Act::Restart(pick));
                trace(state, format_args!("crash n{pick}"));
            }
        }
        Act::Restart(node) => {
            if !world.is_up(node) {
                world.restart(node);
                lock(state).net.bump_epoch();
                start(state, node);
                trace(state, format_args!("restart n{node}"));
            }
        }
        Act::Isolate => {
            let sides = draw_partition(g, &NODES);
            lock(state).net.partition(&sides);
            trace(state, format_args!("partition {sides:?}"));
        }
        Act::Heal => {
            lock(state).net.heal_links();
            trace(state, format_args!("heal"));
        }
        Act::Pause => {
            let mut shared = lock(state);
            if !shared.net.is_paused(pick) {
                shared.net.pause(pick);
                drop(shared);
                nemesis.after(now + 50 + g.below(300), Act::Resume(pick));
                trace(state, format_args!("pause n{pick}"));
            }
        }
        Act::Resume(node) => {
            lock(state).net.resume(node);
            trace(state, format_args!("resume n{node}"));
        }
        Act::Filters => {
            let filters = Filters::draw(g);
            lock(state).net.set_filters(filters);
            trace(state, format_args!("filters {filters:?}"));
        }
        Act::Clock => {
            let by = Duration::from_millis(g.below(500));
            world.advance_mono(pick, by);
            world.set_drift_ppm(pick, g.below(2_001) as i64 - 1_000);
            trace(state, format_args!("clock n{pick} +{}ms", by.as_millis()));
        }
    }
}

fn profile() -> Profile<Act> {
    Profile::new("toy", (50, 300))
        .with(Act::Calm, 2)
        .with(Act::Crash, 2)
        .with(Act::Isolate, 2)
        .with(Act::Heal, 2)
        .with(Act::Pause, 1)
        .with(Act::Filters, 3)
        .with(Act::Clock, 1)
}

/// Runs the toy cluster for one seed.
pub fn run(seed: u64, g: &mut Gen, knobs: Knobs) -> SeedRun {
    assert!(!knobs.panic, "the toy harness was told to panic");
    runtime::run(seed, |world| async move {
        for node in NODES {
            world.add_node(node, NodeClock::default());
        }
        let state: State = Arc::new(Mutex::new(Shared {
            world: world.clone(),
            trace: Trace::with_tail(16),
            net: NetPlan::new(seed),
            observer: RaftObserver::new(3),
            received: BTreeMap::new(),
            knobs,
        }));
        if knobs.leak_global_state {
            let leaked = LEAK.fetch_add(1, Ordering::Relaxed);
            trace(&state, format_args!("leak {leaked}"));
        }
        for node in NODES {
            start(&state, node);
        }
        let mut nemesis = Nemesis::new(profile(), 0, g);
        while virtual_ms(&world) < CHAOS_MS {
            nabedra_rt::time::sleep(Duration::from_millis(10)).await;
            let now = virtual_ms(&world);
            for action in nemesis.poll(now, g) {
                trace(&state, format_args!("nemesis {action:?}"));
                apply(&state, &mut nemesis, g, action);
            }
        }
        for action in nemesis.stop() {
            apply(&state, &mut nemesis, g, action);
        }
        lock(&state).net.heal_all();
        trace(&state, format_args!("heal all"));
        nabedra_rt::time::sleep(Duration::from_millis(200)).await;
        for node in NODES {
            world.crash(node);
        }
        let mut shared = lock(&state);
        let history: Vec<u8> = shared
            .received
            .iter()
            .flat_map(|(node, count)| {
                format!("{}\n", json!({"node": node, "received": count})).into_bytes()
            })
            .collect();
        SeedRun {
            seed,
            trace_hash: shared.trace.hash(),
            trace_events: shared.trace.events(),
            violations: shared.observer.take_violations(),
            trace_tail: shared.trace.tail(),
            full_trace: shared.trace.full().map(<[String]>::to_vec),
            history,
            config: json!({"nodes": NODES.len(), "profile": "toy"}),
            virtual_ms: virtual_ms(&world),
            tape: Vec::new(),
        }
    })
}

#[test]
fn same_seed_same_trace_and_different_seeds_differ() {
    let a = run(7, &mut crate::sweep::workload_gen(7), Knobs::default());
    let b = run(7, &mut crate::sweep::workload_gen(7), Knobs::default());
    assert!(
        a.trace_events > 200,
        "the toy did too little: {}",
        a.trace_events
    );
    assert!(a.passed(), "{:?}", a.violations);
    assert_eq!(
        (a.trace_hash, a.trace_events, &a.history),
        (b.trace_hash, b.trace_events, &b.history)
    );
    let c = run(8, &mut crate::sweep::workload_gen(8), Knobs::default());
    assert_ne!(a.trace_hash, c.trace_hash);
    assert!(a.virtual_ms >= CHAOS_MS);
}

#[test]
fn replaying_the_recorded_tape_reproduces_the_run() {
    let mut g = crate::sweep::workload_gen(21);
    let first = run(21, &mut g, Knobs::default());
    let tape = g.into_tape();
    assert!(!tape.is_empty());
    let replay = run(21, &mut Gen::from_tape(tape), Knobs::default());
    assert_eq!(first.trace_hash, replay.trace_hash);
}
