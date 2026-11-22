//! Model of the OLAP commit protocol against an object store (OLAP-G01 to OLAP-G04,
//! scenarios T04 and T05).
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
//!   :502) and maps a pre-root `Err` to `not_applied` (coordinator.rs:607,
//!   server/src/node/http.rs:34-39). A fence mismatch at the start of a pass is
//!   `not_applied` too (coordinator.rs:484-486).
//! - A takeover fences the root with a conditional root write (server/src/storage/history.rs:372-416),
//!   which changes the same ETag, so a delayed root PUT of the old coordinator fails (T05,
//!   docs/testing/scheduler-contract.md:42).
//! - Timeouts and unknown statuses are `Ambiguous`, and adapters never retry a PUT
//!   (server/src/storage.rs:187-189). An ambiguous PUT may still apply later.
//!
//! The model has one client request handled by the original coordinator (writer 0) and,
//! after a takeover, by the new one (writer 1). Each writer makes at most two passes.
//! A PUT is applied, then its response is delivered or lost; a writer that times out
//! leaves its PUT in flight, and it may apply late.
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
//! - `root_read_back`: after an ambiguous root PUT, reload the root and search for the
//!   request instead of answering `not_applied`.

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
    Root { base: u8, key: u8, writer: u8 },
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
    Ready { pass: u8 },
    Waiting { pass: u8, base: u8, key: u8, bytes: Bytes, op: Op },
    Done(Answer),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// Root revision; the ETag is a function of it.
    rev: u8,
    /// Root history: (decision key, base revision, writer) per applied commit, in order.
    chain: Vec<(u8, u8, u8)>,
    fence: u8,
    decisions: [Option<Bytes>; KEYS],
    /// Ghost: a stored decision was replaced by different bytes.
    overwritten: bool,
    writers: [Phase; 2],
    /// PUTs whose writer timed out; each may still apply.
    late: Vec<OpKind>,
    took_over: bool,
}

#[derive(Clone, Debug)]
enum Action {
    Begin(u8),
    Apply(u8),
    Respond(u8),
    Timeout(u8),
    ApplyLate(u8),
    Takeover,
}

struct OlapCommit {
    if_none_match: bool,
    decision_read_back: bool,
    root_read_back: bool,
    shared_key: bool,
}

impl OlapCommit {
    fn key(&self, writer: u8, pass: u8) -> u8 {
        if self.shared_key { 0 } else { writer * PASSES + pass }
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
            OpKind::Root { base, key, writer } => {
                if s.rev == base {
                    s.rev += 1;
                    s.chain.push((key, base, writer));
                    Resp::Ok
                } else {
                    Resp::Conflict
                }
            }
        }
    }

    fn found(s: &State) -> bool {
        // Every decision in the model belongs to the one client request.
        !s.chain.is_empty()
    }

    fn send(phase: &mut Phase, pass: u8, base: u8, key: u8, bytes: Bytes, kind: OpKind) {
        *phase = Phase::Waiting { pass, base, key, bytes, op: Op { kind, resp: None } };
    }

    /// The writer handles the outcome of its PUT; `resp` is `None` for a timeout (Ambiguous).
    fn handle(&self, t: &mut State, w: usize, resp: Option<Resp>) {
        let Phase::Waiting { pass, base, key, bytes, op } = t.writers[w] else {
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
                    let kind = OpKind::Root { base, key, writer: w as u8 };
                    Self::send(&mut t.writers[w], pass, base, key, bytes, kind);
                } else {
                    // InvalidCatalog, a pre-root Err: not_applied (coordinator.rs:607).
                    t.writers[w] = Phase::Done(Answer::NotApplied);
                }
            }
            OpKind::Root { .. } => {
                t.writers[w] = match resp {
                    Some(Resp::Ok) => Phase::Done(Answer::Committed),
                    None if !self.root_read_back => {
                        // Planted bug: a timed-out root PUT is reported as not applied.
                        Phase::Done(Answer::NotApplied)
                    }
                    _ if Self::found(t) => Phase::Done(Answer::Committed),
                    Some(Resp::Conflict) if pass + 1 < PASSES => Phase::Ready { pass: pass + 1 },
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
            rev: 0,
            chain: Vec::new(),
            fence: 1,
            decisions: [None; KEYS],
            overwritten: false,
            writers: [Phase::Ready { pass: 0 }, Phase::NotStarted],
            late: Vec::new(),
            took_over: false,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for w in 0..2 {
            out.extend([Action::Begin(w), Action::Apply(w), Action::Respond(w), Action::Timeout(w)]);
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
                let Phase::Ready { pass } = s.writers[w as usize] else { return None };
                let my_fence = w + 1;
                t.writers[w as usize] = if s.fence != my_fence {
                    // coordinator.rs:484-486.
                    Phase::Done(Answer::NotApplied)
                } else if Self::found(s) {
                    Phase::Done(Answer::Committed)
                } else {
                    let key = self.key(w, pass);
                    let bytes = Bytes { writer: w, pass, base: s.rev };
                    let mut p = Phase::NotStarted;
                    Self::send(&mut p, pass, s.rev, key, bytes, OpKind::Decision { key, bytes });
                    p
                };
            }
            Action::Apply(w) => {
                let Phase::Waiting { op, .. } = s.writers[w as usize] else { return None };
                if op.resp.is_some() {
                    return None;
                }
                let r = self.store_apply(&mut t, op.kind);
                if let Phase::Waiting { op, .. } = &mut t.writers[w as usize] {
                    op.resp = Some(r);
                }
            }
            Action::Respond(w) => {
                let Phase::Waiting { op, .. } = s.writers[w as usize] else { return None };
                let r = op.resp?;
                self.handle(&mut t, w as usize, Some(r));
            }
            Action::Timeout(w) => {
                // The response is lost, or the PUT is still in flight.
                let Phase::Waiting { op, .. } = s.writers[w as usize] else { return None };
                if op.resp.is_none() {
                    t.late.push(op.kind);
                }
                self.handle(&mut t, w as usize, None);
            }
            Action::ApplyLate(i) => {
                let kind = t.late.remove(i as usize);
                self.store_apply(&mut t, kind);
            }
            Action::Takeover => {
                if s.took_over {
                    return None;
                }
                // The fence is itself a conditional root write that bumps the ETag.
                t.took_over = true;
                t.fence = 2;
                t.rev += 1;
                t.writers[1] = Phase::Ready { pass: 0 };
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.overwritten {
            return Err("decision-immutable: a stored decision object was overwritten".into());
        }
        // `not_applied` is a claim about this attempt only: "Err is a pre-root failure of
        // THIS attempt, never proof about other same-ID work" (publication.rs:144-145).
        for (w, phase) in s.writers.iter().enumerate() {
            if *phase == Phase::Done(Answer::NotApplied) && s.chain.iter().any(|c| c.2 as usize == w) {
                return Err(format!(
                    "no-false-not-applied: writer {w} answered not_applied but its commit is in the catalog"
                ));
            }
        }
        if s.chain.len() > 1 {
            return Err(format!("single-application: the request was applied {} times", s.chain.len()));
        }
        for &(key, base, _) in &s.chain {
            match s.decisions[key as usize] {
                Some(b) if b.base == base => {}
                other => {
                    return Err(format!(
                        "serial-catalog: root commit on base {base} points at decision {key} holding {other:?}"
                    ));
                }
            }
        }
        Ok(())
    }
}

const LIMIT: usize = 200_000;

fn guarded(shared_key: bool) -> OlapCommit {
    OlapCommit { if_none_match: true, decision_read_back: true, root_read_back: true, shared_key }
}

#[test]
fn olap_commit_guarded_passes() {
    common::expect_passed("olap-commit guarded, unique keys", &guarded(false), LIMIT);
    common::expect_passed("olap-commit guarded, shared key", &guarded(true), LIMIT);
}

#[test]
fn without_if_none_match_a_decision_is_overwritten() {
    common::expect_violated(
        "olap-commit without If-None-Match",
        &OlapCommit { if_none_match: false, ..guarded(true) },
        LIMIT,
        "decision-immutable",
    );
}

#[test]
fn without_decision_read_back_the_root_points_at_a_missing_decision() {
    common::expect_violated(
        "olap-commit without decision read-back",
        &OlapCommit { decision_read_back: false, ..guarded(false) },
        LIMIT,
        "serial-catalog",
    );
}

#[test]
fn without_root_read_back_an_applied_commit_is_reported_not_applied() {
    common::expect_violated(
        "olap-commit without root read-back (T04)",
        &OlapCommit { root_read_back: false, ..guarded(false) },
        LIMIT,
        "no-false-not-applied",
    );
}
