//! Model of the OLTP direct path for one key: commit, lost replies, retries with
//! the same request ID, and a leader change (OLTP-G03, OLTP-G04; bug class 1).
//!
//! What the product does (plans/oltp-direct-path.md):
//! - A single-key write commits as one Data entry `ParticipantCommand::Direct`
//!   (server/src/oltp/mvcc/store.rs:165). Its apply writes the row versions and the
//!   dedup record `DirectDedup{table_id, anchor, request_id}` in the same batch
//!   (server/src/oltp/mvcc/store/direct.rs:341-350, :777-823), so every replica,
//!   and therefore every later leader, has the record.
//! - The leader reads the dedup record before proposing: "The dedup record decides
//!   before any fallback" (server/src/oltp_runtime/direct/executor.rs:1249-1271). Apply
//!   checks it again and writes nothing for a replay (direct.rs:726, :480-488).
//! - Before that read the leader waits for an earlier pending entry on the key
//!   (`wait_latest`, executor.rs:1201-1208), and a new leader serves nothing until its
//!   first entry of the new term applies (server/src/oltp/mvcc/store/direct/clock.rs:10-17).
//! - The gateway resends the same request ID and body after `Failed{Unknown}`,
//!   `NotLeader` or a transport timeout (server/src/oltp_runtime/application/direct.rs:1036-1052,
//!   :1103-1129, :1147-1156).
//! - `not_applied` needs proof. The gateway's `deadline_error` answers `outcome_unknown`
//!   once any attempt was uncertain (application/direct.rs:1547-1564). The leader's
//!   `proposal_failed` answers `OutcomeUnknown` when the deadline expires after a
//!   proposal of unknown outcome (executor.rs:626-668). FIXA (merge 8af490f1) fixed a
//!   committed batch member that was answered `not_applied` near its deadline.
//!
//! Abstractions: one key and one request body. Attempts are numbered; an attempt's
//! reply may be lost. A leader change keeps or drops each pending entry. Replies are
//! atomic with the step that produces them.
//!
//! Guards:
//! - `dedup`: the leader's pre-read and the apply-time check of the dedup record.
//!   Without it, a retry after a lost reply executes the write twice.
//! - `proof`: `not_applied` only with a proof of non-application. Without it, a
//!   deadline after a proposal of unknown outcome (leader) or after an uncertain
//!   attempt (gateway) answers `not_applied`, as BUG1 and FIXA did.

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

const ATTEMPTS: usize = 3;
const MAX_TERM: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Reply {
    Ok,
    NotApplied,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Att {
    Unsent,
    /// Sent to the leader of `term`.
    Sent { term: u8 },
    /// Its entry is pending in the log, proposed in `term`.
    Proposed { term: u8 },
    /// The replica answered, and the gateway received it.
    Replied(Reply),
    /// The replica answered, and the reply was lost: the gateway times out (uncertain).
    Lost,
    /// The replica was no longer the leader and proposed nothing (`NotLeader`).
    Refused,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Answer {
    Ok,
    NotApplied,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    term: u8,
    attempts: [Att; ATTEMPTS],
    /// Pending Data entries in log order: (attempt, term proposed).
    pending: Vec<(u8, u8)>,
    /// Ghost: how many times the write was applied.
    executions: u8,
    /// The replicated dedup record for (table, key, request_id).
    dedup: bool,
    answer: Option<Answer>,
}

#[derive(Clone, Debug)]
enum Action {
    Send(u8),
    Prepare(u8),
    Commit,
    LoseReply(u8),
    LeaderDeadline(u8),
    LeaderChange { keep: u8 },
    ClientDeadline,
}

struct OltpDirect {
    dedup: bool,
    proof: bool,
}

impl Model for OltpDirect {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            term: 1,
            attempts: [Att::Unsent; ATTEMPTS],
            pending: Vec::new(),
            executions: 0,
            dedup: false,
            answer: None,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for k in 0..ATTEMPTS as u8 {
            out.push(Action::Send(k));
            out.push(Action::Prepare(k));
            out.push(Action::LoseReply(k));
            out.push(Action::LeaderDeadline(k));
        }
        out.push(Action::Commit);
        for keep in 0..(1u8 << s.pending.len()) {
            out.push(Action::LeaderChange { keep });
        }
        out.push(Action::ClientDeadline);
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut t = s.clone();
        match *a {
            Action::Send(k) => {
                let k = k as usize;
                if s.answer.is_some() || s.attempts[k] != Att::Unsent {
                    return None;
                }
                // Attempt 0 first; a resend only after the previous attempt was
                // uncertain or refused (application/direct.rs:1036-1052).
                if k > 0
                    && !matches!(
                        s.attempts[k - 1],
                        Att::Lost | Att::Refused | Att::Replied(Reply::Unknown)
                    )
                {
                    return None;
                }
                t.attempts[k] = Att::Sent { term: s.term };
            }
            Action::Prepare(k) => {
                let k = k as usize;
                if s.attempts[k] != (Att::Sent { term: s.term }) {
                    return None;
                }
                // wait_latest (executor.rs:1201-1208) and RangeClock rule 3
                // (clock.rs:10-17): no earlier entry on this key is still pending.
                if !s.pending.is_empty() {
                    return None;
                }
                if self.dedup && s.dedup {
                    // Replay of the stored result, no proposal.
                    t.attempts[k] = Att::Replied(Reply::Ok);
                } else {
                    t.pending.push((k as u8, s.term));
                    t.attempts[k] = Att::Proposed { term: s.term };
                }
            }
            Action::Commit => {
                let (k, term) = *s.pending.first()?;
                t.pending.remove(0);
                if self.dedup && s.dedup {
                    // Apply-time check: a replay writes nothing (direct.rs:480-488).
                } else {
                    t.executions += 1;
                    t.dedup = true;
                }
                // The leader that proposed it is still leading and waiting: it replies.
                // A replay's result is reread locally (executor.rs:1110-1148).
                let k = k as usize;
                if s.attempts[k] == (Att::Proposed { term }) && term == s.term {
                    t.attempts[k] = Att::Replied(Reply::Ok);
                }
            }
            Action::LoseReply(k) => {
                let k = k as usize;
                if !matches!(s.attempts[k], Att::Replied(_)) || s.answer.is_some() {
                    return None;
                }
                t.attempts[k] = Att::Lost;
            }
            Action::LeaderDeadline(k) => {
                // The request deadline expires while the leader waits for its proposal.
                let k = k as usize;
                if !matches!(s.attempts[k], Att::Proposed { term } if term == s.term) {
                    return None;
                }
                t.attempts[k] = if self.proof {
                    Att::Replied(Reply::Unknown)
                } else {
                    // Planted bug (BUG1/FIXA style): the unresolved proposal is
                    // treated as a retryable miss and answered not_applied.
                    Att::Replied(Reply::NotApplied)
                };
            }
            Action::LeaderChange { keep } => {
                if s.term >= MAX_TERM {
                    return None;
                }
                t.term += 1;
                t.pending = s
                    .pending
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| keep & (1 << i) != 0)
                    .map(|(_, e)| *e)
                    .collect();
                for att in t.attempts.iter_mut() {
                    *att = match *att {
                        // The old leader cannot tell whether its entry survives.
                        Att::Proposed { .. } => Att::Replied(Reply::Unknown),
                        Att::Sent { term } if term < t.term => Att::Refused,
                        other => other,
                    };
                }
            }
            Action::ClientDeadline => {
                if s.answer.is_some() {
                    return None;
                }
                let any_ok = s.attempts.contains(&Att::Replied(Reply::Ok));
                let typed_not_applied = s.attempts.contains(&Att::Replied(Reply::NotApplied));
                let uncertain = s.attempts.iter().any(|a| {
                    matches!(
                        a,
                        Att::Sent { .. } | Att::Proposed { .. } | Att::Lost | Att::Replied(Reply::Unknown)
                    )
                });
                t.answer = Some(if any_ok {
                    Answer::Ok
                } else if typed_not_applied {
                    Answer::NotApplied
                } else if uncertain && self.proof {
                    // deadline_error with `uncertain` set (application/direct.rs:1547-1564).
                    Answer::Unknown
                } else {
                    // Planted bug when `proof` is off: no proof is required.
                    Answer::NotApplied
                });
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        if s.executions > 1 {
            return Err(format!(
                "one-execution: request applied {} times for one (table, key, request_id)",
                s.executions
            ));
        }
        if s.executions > 0
            && (s.answer == Some(Answer::NotApplied)
                || s.attempts.contains(&Att::Replied(Reply::NotApplied)))
        {
            return Err("no-false-not-applied: not_applied answered for a request that applied".into());
        }
        if s.answer == Some(Answer::Ok) && s.executions != 1 {
            return Err("ok-means-applied: success answered for a request that never applied".into());
        }
        Ok(())
    }
}

const LIMIT: usize = 200_000;

#[test]
fn direct_path_guarded_passes() {
    common::expect_passed("oltp-direct guarded", &OltpDirect { dedup: true, proof: true }, LIMIT);
}

#[test]
fn without_dedup_a_retry_executes_twice() {
    common::expect_violated(
        "oltp-direct without dedup",
        &OltpDirect { dedup: false, proof: true },
        LIMIT,
        "one-execution",
    );
}

#[test]
fn without_proof_an_applied_write_is_reported_not_applied() {
    common::expect_violated(
        "oltp-direct without not_applied proof",
        &OltpDirect { dedup: true, proof: false },
        LIMIT,
        "no-false-not-applied",
    );
}
