//! Model of the OLAP commit protocol against an object store (OLAP-G01 to OLAP-G04,
//! scenarios T03 through T05).
//!
//! What the product does (`PreparedPublication::publish`, server/src/storage/publication.rs:146-219):
//! 1. Look up the request ID in the history chain reachable from the base root;
//!    `Found` is terminal (publication.rs:151-163).
//! 2. PUT the decision object `decisions/{id}.json` with `WriteCondition::Create`, sent
//!    as `If-None-Match: *` (publication.rs:164-168, server/src/storage/http.rs:269-275).
//!    On `Conflict` or `Ambiguous` it reads the object back and requires its own bytes,
//!    otherwise `InvalidCatalog` (publication.rs:170-177).
//! 3. PUT the root `catalog.json` with `WriteCondition::Match(base etag)`, sent as
//!    `If-Match` (publication.rs:183-191). Success is terminal.
//! 4. Otherwise reload the root and search its whole chain: `Found` is terminal; `Absent`
//!    after a definite 412 is `NeedsRecompute`; anything else is `Unknown`
//!    (publication.rs:199-218).
//! - The coordinator retries a recompute with a fresh commit (server/src/node/coordinator.rs:452,
//!   :502). A fence mismatch or pre-root storage error starts as `unavailable()`
//!   (coordinator.rs:484-486, :603-607, server/src/node/http.rs:34-39). The coordinator
//!   changes failed mutation attempts to `outcome_unknown` (coordinator.rs:429-441).
//!   The queue also reconciles errors for accepted mutations and checks the fence
//!   before recording a successful reply (server/src/node/queue_runtime.rs:103-115,
//!   :1713-1757, :1874-1905).
//! - A takeover fences the root with a conditional root write (server/src/storage/history.rs:372-416),
//!   which changes the ETag and preserves the revision and history. A delayed root PUT
//!   of the old coordinator fails (T05,
//!   docs/testing/scheduler-contract.md:42).
//! - Timeouts and unknown statuses are `Ambiguous`, and adapters never retry a PUT
//!   (server/src/storage.rs:187-189). An ambiguous PUT may still apply later.
//!
//! The takeover scenario has one client request handled by the original coordinator
//! (writer 0) and then the new one (writer 1). The contention scenario has two distinct
//! requests under the same coordinator fence. Each writer makes at most two passes.
//! A PUT can time out either before or after it applies. A PUT still in flight at
//! timeout can apply late. Decision and root reads see the current store state.
//!
//! In the product, commit IDs are fresh UUIDs, so two passes never share a decision key,
//! and `If-None-Match` is defense in depth. `shared_key` makes every pass use the same
//! decision key, the case a deterministic commit ID or a replayed PUT would create, so
//! the guard has something to defend.
//!
//! Guards:
//! - `if_none_match`: the decision PUT is a create-only write.
//! - `decision_read_back`: after an ambiguous or conflicting decision PUT, read back
//!   and require the writer's own bytes before the root PUT.
//! - `root_match`: a root PUT must match the captured ETag.
//! - `root_read_back`: after a failed root PUT, reload the root and recover a decision
//!   already visible for this request.
//! - `reconcile_failures`: an attempt's failure is not an authoritative error for the ID.

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

const PASSES: u8 = 2;
const KEYS: usize = 4;

/// Bytes of a decision: who wrote it and the base revision it was prepared on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Bytes {
    writer: u8,
    pass: u8,
    base: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Resp {
    Ok,
    Conflict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum OpKind {
    Decision { key: u8, bytes: Bytes },
    Root { etag: u8, key: u8, bytes: Bytes },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Op {
    kind: OpKind,
    /// Set once the store applied (or refused) it.
    resp: Option<Resp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Answer {
    Committed,
    NotApplied,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    NotStarted,
    Ready {
        pass: u8,
    },
    Waiting {
        etag: u8,
        key: u8,
        bytes: Bytes,
        op: Op,
    },
    CommitFound,
    Done(Answer),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// The version changes on every successful root PUT, including a fence.
    etag: u8,
    /// Root history, with the expected decision bytes for each commit.
    chain: Vec<(u8, Bytes)>,
    fence: u8,
    decisions: [Option<Bytes>; KEYS],
    /// Ghost: a stored decision was replaced by different bytes.
    overwritten: bool,
    /// Ghost: a root PUT applied after its coordinator lost the fence.
    stale_root: bool,
    /// Ghost: publishing a stale root removed a previously visible decision.
    lost_history: bool,
    /// Ghost: recovery skipped a decision already present at read-back time.
    missed_decision: bool,
    writers: [Phase; 2],
    /// PUTs whose writer timed out; each may still apply.
    late: Vec<OpKind>,
}

#[derive(Clone, Debug)]
enum Action {
    Begin(u8),
    Apply(u8),
    Respond(u8),
    Timeout(u8),
    Settle(u8),
    ApplyLate(u8),
    Takeover,
}

struct OlapCommit {
    if_none_match: bool,
    decision_read_back: bool,
    root_match: bool,
    root_read_back: bool,
    reconcile_failures: bool,
    shared_key: bool,
    competing_requests: bool,
}

impl OlapCommit {
    fn writer_fence(&self, writer: u8) -> u8 {
        if self.competing_requests {
            1
        } else {
            writer + 1
        }
    }

    fn key(&self, writer: u8, pass: u8) -> u8 {
        if self.shared_key {
            0
        } else {
            writer * PASSES + pass
        }
    }

    /// The object store applying a PUT (S3 conditional-write semantics).
    fn store_apply(&self, s: &mut State, kind: OpKind) -> Resp {
        match kind {
            OpKind::Decision { key, bytes } => {
                let slot = &mut s.decisions[key as usize];
                match *slot {
                    Some(_) if self.if_none_match => Resp::Conflict,
                    Some(old) => {
                        // Planted bug: an unconditional PUT replaces the object.
                        s.overwritten |= old != bytes;
                        *slot = Some(bytes);
                        Resp::Ok
                    }
                    None => {
                        *slot = Some(bytes);
                        Resp::Ok
                    }
                }
            }
            OpKind::Root { etag, key, bytes } => {
                if self.root_match && s.etag != etag {
                    return Resp::Conflict;
                }
                s.stale_root |= s.fence != self.writer_fence(bytes.writer);
                // The root refers to the history captured when the PUT was built.
                // An unconditional stale PUT would replace any later suffix.
                s.lost_history |= s.chain.len() > bytes.base as usize;
                s.chain.truncate(bytes.base as usize);
                s.etag += 1;
                s.chain.push((key, bytes));
                Resp::Ok
            }
        }
    }

    fn found(&self, s: &State, writer: u8) -> bool {
        s.chain
            .iter()
            .any(|(_, bytes)| !self.competing_requests || bytes.writer == writer)
    }

    fn waiting(etag: u8, key: u8, bytes: Bytes, kind: OpKind) -> Phase {
        Phase::Waiting {
            etag,
            key,
            bytes,
            op: Op { kind, resp: None },
        }
    }

    fn failed_attempt(&self) -> Phase {
        // An accepted mutation still needs reconciliation. The mutant settles
        // the internal attempt error as a definitive failure for the request ID.
        Phase::Done(if self.reconcile_failures {
            Answer::Unknown
        } else {
            Answer::NotApplied
        })
    }

    /// The writer handles the outcome of its PUT; `resp` is `None` for a timeout (Ambiguous).
    fn handle(&self, t: &mut State, w: usize, resp: Option<Resp>) {
        let Phase::Waiting {
            etag,
            key,
            bytes,
            op,
        } = t.writers[w]
        else {
            unreachable!()
        };
        match op.kind {
            OpKind::Decision { .. } => {
                let ours = match resp {
                    Some(Resp::Ok) => true,
                    _ if self.decision_read_back => t.decisions[key as usize] == Some(bytes),
                    // Planted bug: assume the ambiguous or refused PUT stored our bytes.
                    _ => true,
                };
                if ours {
                    let kind = OpKind::Root { etag, key, bytes };
                    t.writers[w] = Self::waiting(etag, key, bytes, kind);
                } else {
                    t.writers[w] = self.failed_attempt();
                }
            }
            OpKind::Root { .. } => {
                let found = self.found(t, bytes.writer);
                t.writers[w] = match resp {
                    Some(Resp::Ok) => Phase::CommitFound,
                    _ if found && self.root_read_back => Phase::CommitFound,
                    _ if found => {
                        t.missed_decision = true;
                        Phase::Done(Answer::Unknown)
                    }
                    Some(Resp::Conflict) if bytes.pass + 1 < PASSES => Phase::Ready {
                        pass: bytes.pass + 1,
                    },
                    _ => Phase::Done(Answer::Unknown),
                };
            }
        }
    }
}

impl Model for OlapCommit {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            etag: 0,
            chain: Vec::new(),
            fence: 1,
            decisions: [None; KEYS],
            overwritten: false,
            stale_root: false,
            lost_history: false,
            missed_decision: false,
            writers: [
                Phase::Ready { pass: 0 },
                if self.competing_requests {
                    Phase::Ready { pass: 0 }
                } else {
                    Phase::NotStarted
                },
            ],
            late: Vec::new(),
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for w in 0..2 {
            out.extend([
                Action::Begin(w),
                Action::Apply(w),
                Action::Respond(w),
                Action::Timeout(w),
                Action::Settle(w),
            ]);
        }
        for i in 0..s.late.len() as u8 {
            out.push(Action::ApplyLate(i));
        }
        out.push(Action::Takeover);
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut t = s.clone();
        match *a {
            Action::Begin(w) => {
                let Phase::Ready { pass } = s.writers[w as usize] else {
                    return None;
                };
                let my_fence = self.writer_fence(w);
                t.writers[w as usize] = if s.fence != my_fence {
                    // coordinator.rs:484-486.
                    self.failed_attempt()
                } else if self.found(s, w) {
                    Phase::CommitFound
                } else {
                    let key = self.key(w, pass);
                    let bytes = Bytes {
                        writer: w,
                        pass,
                        base: s.chain.len() as u8,
                    };
                    Self::waiting(s.etag, key, bytes, OpKind::Decision { key, bytes })
                };
            }
            Action::Apply(w) => {
                let Phase::Waiting { op, .. } = s.writers[w as usize] else {
                    return None;
                };
                if op.resp.is_some() {
                    return None;
                }
                let r = self.store_apply(&mut t, op.kind);
                if let Phase::Waiting { op, .. } = &mut t.writers[w as usize] {
                    op.resp = Some(r);
                }
            }
            Action::Respond(w) => {
                let Phase::Waiting { op, .. } = s.writers[w as usize] else {
                    return None;
                };
                let r = op.resp?;
                self.handle(&mut t, w as usize, Some(r));
            }
            Action::Timeout(w) => {
                // The response is lost, or the PUT is still in flight.
                let Phase::Waiting { op, .. } = s.writers[w as usize] else {
                    return None;
                };
                if op.resp.is_none() {
                    t.late.push(op.kind);
                }
                self.handle(&mut t, w as usize, None);
            }
            Action::ApplyLate(i) => {
                let kind = t.late.remove(i as usize);
                self.store_apply(&mut t, kind);
            }
            Action::Settle(w) => {
                if s.writers[w as usize] != Phase::CommitFound {
                    return None;
                }
                t.writers[w as usize] = Phase::Done(if s.fence == self.writer_fence(w) {
                    Answer::Committed
                } else {
                    Answer::Unknown
                });
            }
            Action::Takeover => {
                if self.competing_requests || s.fence == 2 {
                    return None;
                }
                // One successful fence PUT against the current root.
                t.fence = 2;
                t.etag += 1;
                t.writers[1] = Phase::Ready { pass: 0 };
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.overwritten {
            return Err("decision-immutable: a stored decision object was overwritten".into());
        }
        if s.stale_root {
            return Err("fence-orders-root-put: a fenced coordinator changed the root".into());
        }
        if s.missed_decision {
            return Err(
                "recover-visible-decision: a failed root PUT hid an already visible decision"
                    .into(),
            );
        }
        if s.lost_history {
            return Err("serial-catalog: a root PUT removed a previously visible decision".into());
        }
        for (writer, phase) in s.writers.iter().enumerate() {
            if *phase == Phase::Done(Answer::NotApplied) && self.found(s, writer as u8) {
                return Err(
                    "no-false-not-applied: an attempt reported not_applied for an ID that committed"
                        .into(),
                );
            }
        }
        let distinct_requests = s.chain.len() == 2
            && self.competing_requests
            && s.chain[0].1.writer != s.chain[1].1.writer;
        if s.chain.len() > 1 && !distinct_requests {
            return Err(format!(
                "single-application: the request was applied {} times",
                s.chain.len()
            ));
        }
        for (base, &(key, bytes)) in s.chain.iter().enumerate() {
            if bytes.base as usize != base {
                return Err(format!(
                    "serial-catalog: decision {key} uses revision {}, expected {base}",
                    bytes.base
                ));
            }
            let stored = s.decisions[key as usize];
            if stored != Some(bytes) {
                return Err(format!(
                    "serial-catalog: root expects decision {key} to contain {bytes:?}, found {stored:?}"
                ));
            }
        }
        Ok(())
    }
}

const LIMIT: usize = 200_000;

fn guarded(shared_key: bool) -> OlapCommit {
    OlapCommit {
        if_none_match: true,
        decision_read_back: true,
        root_match: true,
        root_read_back: true,
        reconcile_failures: true,
        shared_key,
        competing_requests: false,
    }
}

fn run_schedule(model: &OlapCommit, actions: &[Action]) -> State {
    let mut state = model.init_states().pop().unwrap();
    for action in actions {
        state = model
            .next_state(&state, action)
            .unwrap_or_else(|| panic!("{action:?} disabled in {state:?}"));
        model.check(&state).unwrap();
    }
    state
}

#[test]
fn olap_commit_guarded_passes() {
    common::expect_passed("olap-commit guarded, unique keys", &guarded(false), LIMIT);
    common::expect_passed("olap-commit guarded, shared key", &guarded(true), LIMIT);
    common::expect_passed(
        "olap-commit guarded, contending requests",
        &OlapCommit {
            competing_requests: true,
            ..guarded(false)
        },
        LIMIT,
    );
}

#[test]
fn fenced_old_attempt_cannot_report_not_applied_after_replacement_commits() {
    let state = run_schedule(
        &guarded(false),
        &[
            Action::Begin(0),
            Action::Apply(0),
            Action::Respond(0), // The old coordinator sends its root PUT.
            Action::Takeover,
            Action::Apply(0),
            Action::Respond(0), // The root PUT conflicts. History has no decision yet.
            Action::Begin(1),
            Action::Apply(1),
            Action::Respond(1),
            Action::Apply(1), // The replacement commits the same request ID and body.
            Action::Begin(0), // The old coordinator's retry discovers the fence mismatch.
            Action::Respond(1),
            Action::Settle(1),
        ],
    );
    assert_eq!(
        state.writers,
        [Phase::Done(Answer::Unknown), Phase::Done(Answer::Committed)]
    );
    assert_eq!(state.chain.len(), 1);
    assert_eq!(state.chain[0].1.writer, 1);
}

#[test]
fn takeover_preserves_a_commit_that_applied_before_the_fence() {
    let state = run_schedule(
        &guarded(false),
        &[
            Action::Begin(0),
            Action::Apply(0),
            Action::Respond(0),
            Action::Apply(0),
            Action::Takeover,
            Action::Timeout(0), // The decision is visible but the old writer is fenced.
            Action::Settle(0),
            Action::Begin(1), // Reads the original decision without sending a PUT.
            Action::Settle(1),
        ],
    );
    assert_eq!(
        state.writers,
        [Phase::Done(Answer::Unknown), Phase::Done(Answer::Committed)]
    );
    assert_eq!(state.chain.len(), 1);
    assert_eq!(state.chain[0].1.writer, 0);
}

#[test]
fn without_if_none_match_a_decision_is_overwritten() {
    common::expect_violated(
        "olap-commit without If-None-Match",
        &OlapCommit {
            if_none_match: false,
            ..guarded(true)
        },
        LIMIT,
        "decision-immutable",
    );
}

#[test]
fn without_decision_read_back_the_root_points_at_a_missing_decision() {
    common::expect_violated(
        "olap-commit without decision read-back",
        &OlapCommit {
            decision_read_back: false,
            ..guarded(false)
        },
        LIMIT,
        "serial-catalog",
    );
}

#[test]
fn without_root_match_a_fenced_root_put_can_apply() {
    common::expect_violated(
        "olap-commit without root If-Match (T05)",
        &OlapCommit {
            root_match: false,
            ..guarded(false)
        },
        LIMIT,
        "fence-orders-root-put",
    );
}

#[test]
fn without_root_match_a_contender_can_publish_from_a_stale_base() {
    common::expect_violated(
        "olap-commit without root If-Match (T03)",
        &OlapCommit {
            root_match: false,
            competing_requests: true,
            ..guarded(false)
        },
        LIMIT,
        "serial-catalog",
    );
}

#[test]
fn without_root_read_back_a_visible_commit_is_missed() {
    common::expect_violated(
        "olap-commit without root read-back (T04)",
        &OlapCommit {
            root_read_back: false,
            ..guarded(false)
        },
        LIMIT,
        "recover-visible-decision",
    );
}

#[test]
fn reporting_attempt_failure_as_definitive_can_contradict_another_commit() {
    common::expect_violated(
        "olap-commit without attempt error reconciliation",
        &OlapCommit {
            reconcile_failures: false,
            ..guarded(false)
        },
        LIMIT,
        "no-false-not-applied",
    );
}
