//! Model of the direct-writer fence in an index publication (bug class 3, ACC2).
//!
//! What the product does:
//! - A range group holds `DirectSchema{epoch, lifecycle}` (server/src/oltp/mvcc/store/direct.rs:172-191).
//!   Its transition function `schema()` (direct.rs:1245-1286) is: Activate on an
//!   unset schema binds the lifecycle; Fence for the lifecycle already held is a no-op;
//!   any other Fence(L) moves to {epoch + 1, L}.
//! - `plan_finish_index` validates the build: every shard's progress is done with the
//!   matching build, attempt and span, and routing matches
//!   (server/src/oltp/mvcc/planner/index_build.rs:94-140). Only then does it fence Data
//!   with the next table lifecycle, `txn.request_id` (index_build.rs:233, :254-258), and
//!   the Catalog commit publishes that lifecycle. "Fence before publishing the new
//!   lifecycle. A rejected validation must leave Data on the current Catalog lifecycle"
//!   (index_build.rs:254-256).
//! - ACC2 (fixed in merge a1973580): the fence ran at the top of `plan_finish_index`,
//!   before validation, so a rejected validation left Data fenced from epoch 2 to 3
//!   under a lifecycle the Catalog never published (scenario O12).
//! - A fence followed by a failed Catalog commit is the documented residual case:
//!   leaders see `!group.admits(table.lifecycle_id)` and fall back
//!   (server/src/oltp_runtime/direct/schema.rs:249-252). The model allows it.
//!
//! The DDL step moves along declared edges only (`EDGES`), and the planner may crash
//! and restart the step once. Whether the build is complete is fixed per run (two
//! initial states), so validation is deterministic within a run.
//!
//! Guard: `validate_before_fence`. Without it the fence may run straight after the
//! exclusive guard is acquired, as before the ACC2 fix.

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

/// Lifecycle IDs: L1 is published at the start, L2 is the index publication's next one.
const L1: u8 = 1;
const L2: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    Idle,
    Acquired,
    Validated,
    Rejected,
    Fenced,
    Published,
    Aborted,
}

/// Declared lifecycle edges of the publication step. A crash restarts any
/// non-terminal step from `Acquired` (the exclusive guard is durable).
const EDGES: &[(Step, Step)] = &[
    (Step::Idle, Step::Acquired),
    (Step::Acquired, Step::Validated),
    (Step::Acquired, Step::Rejected),
    (Step::Validated, Step::Fenced),
    (Step::Fenced, Step::Published),
    (Step::Fenced, Step::Aborted),
    (Step::Acquired, Step::Acquired),
    (Step::Validated, Step::Acquired),
    (Step::Fenced, Step::Acquired),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct DirectSchema {
    epoch: u8,
    lifecycle: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// Fixed per run: whether every shard finished the build.
    build_complete: bool,
    step: Step,
    /// The Catalog's published table lifecycle.
    published: u8,
    data: DirectSchema,
    crashed: bool,
    /// Ghost: the last step transition taken.
    last_edge: Option<(Step, Step)>,
}

#[derive(Clone, Debug)]
enum Action {
    Acquire,
    Validate,
    Fence,
    CatalogCommit { ok: bool },
    Crash,
}

struct Fence {
    validate_before_fence: bool,
    build_states: &'static [bool],
}

/// `schema()` for a Fence command (direct.rs:1245-1286).
fn fence(cur: DirectSchema, lifecycle: u8) -> DirectSchema {
    if cur.lifecycle == lifecycle {
        cur
    } else {
        DirectSchema {
            epoch: cur.epoch + 1,
            lifecycle,
        }
    }
}

fn go(t: &mut State, to: Step) {
    t.last_edge = Some((t.step, to));
    t.step = to;
}

impl Model for Fence {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        self.build_states
            .iter()
            .map(|&build_complete| State {
                build_complete,
                step: Step::Idle,
                published: L1,
                data: DirectSchema {
                    epoch: 2,
                    lifecycle: L1,
                },
                crashed: false,
                last_edge: None,
            })
            .collect()
    }

    fn actions(&self, _: &State, out: &mut Vec<Action>) {
        out.extend([
            Action::Acquire,
            Action::Validate,
            Action::Fence,
            Action::CatalogCommit { ok: true },
            Action::CatalogCommit { ok: false },
            Action::Crash,
        ]);
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut t = s.clone();
        match *a {
            Action::Acquire if s.step == Step::Idle => go(&mut t, Step::Acquired),
            Action::Validate if s.step == Step::Acquired => {
                let to = if s.build_complete {
                    Step::Validated
                } else {
                    Step::Rejected
                };
                go(&mut t, to);
            }
            Action::Fence => {
                let allowed = if self.validate_before_fence {
                    s.step == Step::Validated
                } else {
                    // Planted bug (pre-ACC2): fence as soon as the guard is held.
                    matches!(s.step, Step::Acquired | Step::Validated)
                };
                if !allowed {
                    return None;
                }
                t.data = fence(s.data, L2);
                go(&mut t, Step::Fenced);
            }
            Action::CatalogCommit { ok } if s.step == Step::Fenced => {
                if ok {
                    t.published = L2;
                    go(&mut t, Step::Published);
                } else {
                    go(&mut t, Step::Aborted);
                }
            }
            Action::Crash
                if !s.crashed
                    && matches!(s.step, Step::Acquired | Step::Validated | Step::Fenced) =>
            {
                t.crashed = true;
                go(&mut t, Step::Acquired);
            }
            _ => return None,
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.data.lifecycle != s.published && !s.build_complete {
            return Err(format!(
                "fence-after-validation: Data fenced to {:?} under unpublished lifecycle L{} although validation rejects the build",
                s.data, s.data.lifecycle
            ));
        }
        if let Some(edge) = s.last_edge
            && !EDGES.contains(&edge)
        {
            return Err(format!(
                "lifecycle-edges: undeclared transition {:?} -> {:?}",
                edge.0, edge.1
            ));
        }
        if s.data.lifecycle == L1 && s.published == L2 {
            return Err(
                "publish-after-fence: Catalog published L2 while Data still admits L1".into(),
            );
        }
        Ok(())
    }
}

#[test]
fn validated_fence_passes() {
    let e = common::expect_passed(
        "oltp-fence guarded",
        &Fence {
            validate_before_fence: true,
            build_states: &[false, true],
        },
        10_000,
    );
    assert!(e.states >= 10, "{e:?}");
}

#[test]
fn fence_before_validation_strands_data_under_an_unpublished_lifecycle() {
    let steps = common::expect_violated(
        "oltp-fence without validate-before-fence (ACC2)",
        &Fence {
            validate_before_fence: false,
            build_states: &[false, true],
        },
        10_000,
        "fence-after-validation",
    );
    assert_eq!(steps, 2, "Acquire, Fence");
}

#[test]
fn early_fence_is_an_undeclared_edge_even_when_the_build_is_complete() {
    common::expect_violated(
        "oltp-fence without validate-before-fence, complete build",
        &Fence {
            validate_before_fence: false,
            build_states: &[true],
        },
        10_000,
        "lifecycle-edges",
    );
}
