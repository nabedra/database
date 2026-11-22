//! Model of the KV cluster slot lease under clock error (KV-G06).
//!
//! What the product does (server/src/cache/cluster/):
//! - Leases last `LEASE_MS = 3_000` (protocol.rs:12). A clock sample is an interval
//!   [lower, upper] no wider than `MAX_UNCERTAINTY_NS = 10 ms` (clock.rs:8, :74-79).
//! - `allows(d)` is `upper < d` and `fenced(d)` is `lower >= d` (clock.rs:24-30).
//! - The controller mints a grant with `expires = now.lower + LEASE_MS`
//!   (controller.rs:388), and a renewal never moves `expires` backward
//!   (controller/state.rs:184-187).
//! - A primary serves only while `now.fenced(not_before) && now.allows(expires)`
//!   (worker/actor.rs:187-194).
//! - Failover requires `now.fenced(old.expires)` at the controller
//!   (controller/placement.rs:397-398), then installs the new primary with
//!   `Lease{expires: cutoff, not_before: cutoff}` where cutoff is the old deadline
//!   (placement.rs:611-616); the new primary serves once a renewal moves `expires`
//!   past `not_before`.
//!
//! Time is in units of 10 ms, so the bound is 1 unit and a lease is 300 units. Each
//! clock read by a node returns `true time + offset` for any offset within the actual
//! error, chosen freshly per read: per-node error and drift as bounded nondeterminism.
//! The declared interval is that reading plus or minus the declared bound.
//!
//! Invariant: one-live-primary, at most one node serves the slot at any true instant.
//!
//! Guard: `use_bound`, where lease checks use the interval endpoints (the reading with
//! the error bound subtracted or added) instead of the raw reading. A run whose actual
//! clock error exceeds the declared bound must violate the invariant even with the
//! guard: the lease is only as safe as the clock assumption.

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

/// 3 s in 10 ms units.
const LEASE: i32 = 300;
/// 10 ms.
const BOUND: i32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Lease {
    not_before: i32,
    expires: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// True time.
    t: i32,
    /// Node 0 is the old primary, node 1 the replica promoted by failover.
    leases: [Option<Lease>; 2],
    /// Renewals granted to the new primary (bounded).
    renewals: u8,
    /// Who served at the current true instant (reset on each tick).
    serving: [bool; 2],
}

#[derive(Clone, Debug)]
enum Action {
    Tick,
    Serve { node: u8, offset: i32 },
    Failover { offset: i32 },
    Renew { offset: i32 },
}

struct KvLease {
    use_bound: bool,
    /// The largest clock offset any read can actually have.
    actual_error: i32,
    /// Bound on true time explored.
    horizon: i32,
}

impl KvLease {
    fn offsets(&self) -> std::ops::RangeInclusive<i32> {
        -self.actual_error..=self.actual_error
    }

    /// (lower, upper) the node computes from one reading.
    fn interval(&self, t: i32, offset: i32) -> (i32, i32) {
        let r = t + offset;
        if self.use_bound {
            (r - BOUND, r + BOUND)
        } else {
            // Planted bug: treat the reading as exact.
            (r, r)
        }
    }
}

impl Model for KvLease {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        // The controller granted node 0 at true time 0, reading with any offset.
        self.offsets()
            .map(|offset| {
                let (lower, _) = self.interval(0, offset);
                State {
                    t: 0,
                    leases: [Some(Lease { not_before: 0, expires: lower + LEASE }), None],
                    renewals: 0,
                    serving: [false; 2],
                }
            })
            .collect()
    }

    fn actions(&self, _: &State, out: &mut Vec<Action>) {
        out.push(Action::Tick);
        for offset in self.offsets() {
            out.push(Action::Serve { node: 0, offset });
            out.push(Action::Serve { node: 1, offset });
            out.push(Action::Failover { offset });
            out.push(Action::Renew { offset });
        }
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut n = s.clone();
        match *a {
            Action::Tick => {
                if s.t >= self.horizon {
                    return None;
                }
                n.t += 1;
                n.serving = [false; 2];
            }
            Action::Serve { node, offset } => {
                let lease = s.leases[node as usize]?;
                let (lower, upper) = self.interval(s.t, offset);
                // worker/actor.rs:187-194.
                if !(lower >= lease.not_before && upper < lease.expires) || s.serving[node as usize] {
                    return None;
                }
                n.serving[node as usize] = true;
            }
            Action::Failover { offset } => {
                if s.leases[1].is_some() {
                    return None;
                }
                let old = s.leases[0]?;
                let (lower, _) = self.interval(s.t, offset);
                // placement.rs:397-398, then switch_owner (placement.rs:611-616).
                if lower < old.expires {
                    return None;
                }
                n.leases[0] = None;
                n.leases[1] = Some(Lease { not_before: old.expires, expires: old.expires });
            }
            Action::Renew { offset } => {
                let prior = s.leases[1]?;
                if s.renewals >= 1 {
                    return None;
                }
                let (lower, _) = self.interval(s.t, offset);
                let expires = lower + LEASE;
                // controller/state.rs:184-187.
                if expires < prior.expires || expires <= prior.not_before {
                    return None;
                }
                n.leases[1] = Some(Lease { expires, ..prior });
                n.renewals += 1;
            }
        }
        Some(n)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.serving[0] && s.serving[1] {
            return Err(format!(
                "one-live-primary: the old and the new primary both serve at true time {} ms",
                s.t * 10
            ));
        }
        Ok(())
    }
}

/// Explore the window around the first lease's expiry.
const HORIZON: i32 = LEASE + 8;
const LIMIT: usize = 200_000;

#[test]
fn lease_with_error_bound_passes() {
    for actual_error in 0..=BOUND {
        let e = common::expect_passed(
            &format!("kv-lease with bound, actual error {} ms", actual_error * 10),
            &KvLease { use_bound: true, actual_error, horizon: HORIZON },
            LIMIT,
        );
        assert!(e.max_depth as i32 >= HORIZON);
    }
}

#[test]
fn lease_without_error_bound_has_two_primaries() {
    common::expect_violated(
        "kv-lease without bound, actual error 10 ms",
        &KvLease { use_bound: false, actual_error: BOUND, horizon: HORIZON },
        LIMIT,
        "one-live-primary",
    );
}

/// Expected failure: clock error beyond the declared 10 ms breaks the lease even with
/// the bound applied. This documents the assumption KV-G06 depends on.
#[test]
fn drift_beyond_the_bound_breaks_the_lease() {
    common::expect_violated(
        "kv-lease with bound, actual error 20 ms (beyond the bound)",
        &KvLease { use_bound: true, actual_error: 2 * BOUND, horizon: HORIZON },
        LIMIT,
        "one-live-primary",
    );
}
