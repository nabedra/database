//! Model of Events subscription leases: generations, the checkpoint and prefix acks (EVT-G05).
//!
//! What the product does (server/events/src/partition/subscriptions.rs):
//! - A subscription holds `checkpoint` and a `StoredLease{owner, claim_id, generation,
//!   expires_ms, released}` (:11-25).
//! - A claim by another claim ID while a lease is live is `LeaseBusy`; otherwise the
//!   generation is the old one plus one, or 1 (:139-182).
//! - `live_lease` requires `!released`, the same owner, the same generation and
//!   `now_ms < expires_ms`, else `lease_lost` (:28-30, :211-216), which HTTP maps to
//!   `409 lease_lost` (server/events/src/http/error.rs:141).
//! - Ack checks the live lease first (:82), then that the ack lies inside the delivered
//!   prefix (:83-88), and moves the checkpoint forward only (:89-94).
//! - Poll checks the live lease and returns events after the checkpoint without
//!   changing state (:105-121), so unacked events are redelivered.
//! - Each command carries the API node's wall clock; apply uses the maximum clock ever
//!   applied, so a later leader with a slower clock cannot revive an expired grant
//!   (:265-286).
//!
//! The model has one partition with two events and two processes of the same owner: an
//! old instance and its restart, which claims with a new claim ID. When the owner names
//! match, the generation is the only thing that tells the two apart. Two API nodes have
//! independent clocks. A process that processes events while its lease is not the live
//! one has its side effects fenced off downstream, so only processing under the live
//! lease counts.
//!
//! Invariants:
//! - acked-means-processed: every event at or below the checkpoint was processed by the
//!   live lease holder (an event is never skipped for the current generation);
//! - lease-clock-monotone: the applied lease clock never moves backward.
//!
//! Guards: `generation_check` (the generation comparison in `has_scope`), and
//! `max_clock` (the applied clock is the maximum of every command's clock).

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

const EVENTS: u8 = 2;
const TTL: u8 = 2;
const MAX_CLOCK: u8 = 4;
const MAX_GEN: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Lease {
    holder: u8,
    generation: u8,
    expires: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
struct Proc {
    /// The generation of this process's receipt.
    generation: Option<u8>,
    /// Delivered prefix (after, last].
    delivery: Option<(u8, u8)>,
    /// Whether it processed its delivery while holding the live lease.
    processed_live: bool,
    processed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// Wall clocks of the two API nodes.
    nodes: [u8; 2],
    /// The applied lease clock.
    clock: u8,
    lease: Option<Lease>,
    checkpoint: u8,
    procs: [Proc; 2],
    /// Ghost: event e (1-based) was processed under the live lease.
    handled: [bool; EVENTS as usize + 1],
    /// Ghost: the clock moved backward at some apply.
    clock_regressed: bool,
}

#[derive(Clone, Debug)]
enum Action {
    Tick(u8),
    Claim { p: u8, node: u8 },
    Poll { p: u8, node: u8 },
    Process(u8),
    Ack { p: u8, node: u8, through: u8 },
}

struct EventsLease {
    generation_check: bool,
    max_clock: bool,
}

impl EventsLease {
    /// subscriptions.rs:265-286: the clock an applied command uses.
    fn apply_clock(&self, t: &mut State, node: u8) -> u8 {
        let now = t.nodes[node as usize];
        if self.max_clock {
            t.clock = t.clock.max(now);
        } else {
            // Planted bug: use the command's own clock.
            t.clock_regressed |= now < t.clock;
            t.clock = now;
        }
        t.clock
    }

    /// `live_lease` (subscriptions.rs:211-216). Both processes share the owner name.
    fn live(&self, s: &State, generation: Option<u8>, now: u8) -> bool {
        let Some(l) = s.lease else { return false };
        let Some(g) = generation else { return false };
        (!self.generation_check || l.generation == g) && now < l.expires
    }

    /// Live in the ground truth: this process holds the current generation, unexpired.
    fn truly_live(s: &State, p: usize) -> bool {
        matches!(s.lease, Some(l) if Some(l.generation) == s.procs[p].generation
            && l.holder as usize == p && s.clock < l.expires)
    }
}

impl Model for EventsLease {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            nodes: [0, 0],
            clock: 0,
            lease: None,
            checkpoint: 0,
            procs: [Proc::default(); 2],
            handled: [false; EVENTS as usize + 1],
            clock_regressed: false,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for node in 0..2 {
            out.push(Action::Tick(node));
        }
        for p in 0..2 {
            out.push(Action::Process(p));
            for node in 0..2 {
                out.push(Action::Claim { p, node });
                out.push(Action::Poll { p, node });
                if let Some((after, last)) = s.procs[p as usize].delivery {
                    for through in after + 1..=last {
                        out.push(Action::Ack { p, node, through });
                    }
                }
            }
        }
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut t = s.clone();
        match *a {
            Action::Tick(node) => {
                if s.nodes[node as usize] >= MAX_CLOCK {
                    return None;
                }
                t.nodes[node as usize] += 1;
            }
            Action::Claim { p, node } => {
                if s.procs[p as usize].generation.is_some() {
                    return None;
                }
                let now = self.apply_clock(&mut t, node);
                // Busy while any grant with another claim ID is live (:156-161).
                if matches!(s.lease, Some(l) if now < l.expires) {
                    return None;
                }
                let generation = s.lease.map_or(1, |l| l.generation + 1);
                if generation > MAX_GEN {
                    return None;
                }
                t.lease = Some(Lease { holder: p, generation, expires: now + TTL });
                t.procs[p as usize] = Proc { generation: Some(generation), ..Proc::default() };
            }
            Action::Poll { p, node } => {
                let pr = s.procs[p as usize];
                if pr.delivery.is_some() || s.checkpoint >= EVENTS {
                    return None;
                }
                let now = self.apply_clock(&mut t, node);
                if !self.live(&t, pr.generation, now) {
                    return None;
                }
                t.procs[p as usize].delivery = Some((s.checkpoint, EVENTS));
            }
            Action::Process(p) => {
                let pr = s.procs[p as usize];
                if pr.delivery.is_none() || pr.processed {
                    return None;
                }
                let live = Self::truly_live(s, p as usize);
                t.procs[p as usize].processed = true;
                t.procs[p as usize].processed_live = live;
                if live {
                    let (after, last) = pr.delivery.unwrap();
                    for e in after + 1..=last {
                        t.handled[e as usize] = true;
                    }
                }
            }
            Action::Ack { p, node, through } => {
                let pr = s.procs[p as usize];
                let (after, _last) = pr.delivery?;
                if !pr.processed {
                    return None;
                }
                let now = self.apply_clock(&mut t, node);
                if !self.live(&t, pr.generation, now) {
                    // 409 lease_lost: the process drops its receipt and delivery.
                    t.procs[p as usize] = Proc::default();
                    return Some(t);
                }
                if after > t.checkpoint {
                    return None; // outside the delivered prefix (:83-88)
                }
                t.checkpoint = t.checkpoint.max(through);
                t.procs[p as usize].delivery = None;
                t.procs[p as usize].processed = false;
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.clock_regressed {
            return Err("lease-clock-monotone: an applied command moved the lease clock backward".into());
        }
        for e in 1..=s.checkpoint {
            if !s.handled[e as usize] {
                return Err(format!(
                    "acked-means-processed: checkpoint {} covers event {e}, which no live lease holder processed",
                    s.checkpoint
                ));
            }
        }
        Ok(())
    }
}

const LIMIT: usize = 500_000;

#[test]
fn events_lease_guarded_passes() {
    common::expect_passed(
        "events-lease guarded",
        &EventsLease { generation_check: true, max_clock: true },
        LIMIT,
    );
}

#[test]
fn without_generation_check_a_stale_ack_skips_events() {
    common::expect_violated(
        "events-lease without generation check",
        &EventsLease { generation_check: false, max_clock: true },
        LIMIT,
        "acked-means-processed",
    );
}

#[test]
fn without_max_clock_the_lease_clock_moves_backward() {
    common::expect_violated(
        "events-lease without max clock",
        &EventsLease { generation_check: true, max_clock: false },
        LIMIT,
        "lease-clock-monotone",
    );
}
