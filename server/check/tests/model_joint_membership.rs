//! Model of a Raft joint-consensus membership change (OLTP-G08).
//!
//! The OLTP replication groups run openraft 0.10, and their membership changes go
//! through `Raft::change_membership_if` (server/src/oltp_replication/manager/lifecycle.rs:368),
//! guarded by `Precondition::LastMembershipLogId` so a stale retry cannot start a
//! second change (lifecycle.rs:371). openraft moves through a joint configuration
//! C_old,new and decides a quorum of a joint membership as "a quorum of every config"
//! (openraft-0.10.0/src/membership/membership_impl_quorum_set.rs:16-23). Both votes
//! and commits use the membership of the latest membership entry in the node's own
//! log, committed or not (Raft thesis section 4.3, which openraft follows as the
//! "effective membership").
//!
//! The model has four nodes. C_old = {0, 1, 2} and C_new = {1, 2, 3}: node 3 is added
//! and node 0 removed. Terms go up to `MAX_TERM` and logs to `MAX_LOG` entries.
//! Abstractions, each of which only adds behaviours:
//! - RequestVote and AppendEntries are atomic request-and-reply steps, and replies
//!   are never lost (a lost reply equals the step not happening).
//! - AppendEntries replaces the follower's log with the leader's, which is the state
//!   the consistency check and truncation converge to.
//! - A commit counts the followers whose current log matches the leader's prefix.
//!
//! Invariants: election safety (at most one leader per term, across configurations),
//! and leader completeness (an entry committed in term t is in the log of every leader
//! of a later term, so no committed entry is lost).
//!
//! Guard: `joint_quorum`. With it, a node whose latest configuration is C_old,new needs
//! a majority of C_old and a majority of C_new. Without it, a majority of C_new alone
//! counts, and the model finds a shortest trace where a C_old leader and a joint
//! leader lose a committed entry.

#[path = "../models/common.rs"]
mod common;

use nabedra_check::model::Model;

const NODES: usize = 4;
const OLD: u8 = 0b0111; // {0, 1, 2}
const NEW: u8 = 0b1110; // {1, 2, 3}
const MAX_TERM: u8 = 2;
const MAX_LOG: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Data,
    Joint,
    New,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Entry {
    term: u8,
    kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// A log of at most `MAX_LOG` entries, fixed size so states are `Copy` and cheap to hash.
/// Unused slots always hold `BLANK`, so equal logs compare equal.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Log {
    len: u8,
    e: [Entry; MAX_LOG],
}

const BLANK: Entry = Entry { term: 0, kind: Kind::Data };

impl Log {
    fn as_slice(&self) -> &[Entry] {
        &self.e[..self.len as usize]
    }
    fn len(&self) -> usize {
        self.len as usize
    }
    fn push(&mut self, e: Entry) {
        self.e[self.len as usize] = e;
        self.len += 1;
    }
}

impl std::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Node {
    term: u8,
    voted: Option<u8>,
    role: Role,
    /// Votes received while a candidate; cleared in every other role.
    votes: u8,
    log: Log,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Config {
    Old,
    Joint,
    New,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    nodes: [Node; NODES],
    /// Ghost: the committed prefix, each entry with the term of the leader that committed it.
    committed: Vec<(Entry, u8)>,
    /// Ghost: every (term, node) that became leader.
    elected: Vec<(u8, u8)>,
}

#[derive(Clone, Debug)]
enum Action {
    Timeout(u8),
    RequestVote { candidate: u8, voter: u8 },
    BecomeLeader(u8),
    ClientWrite(u8),
    ProposeJoint(u8),
    ProposeNew(u8),
    Replicate { leader: u8, follower: u8 },
    Commit { leader: u8, index: u8 },
}

struct JointMembership {
    /// The guard: in a joint configuration require a majority of both configs.
    joint_quorum: bool,
    /// Also append data entries. Config entries alone already exercise every commit
    /// rule, so the fast default leaves data entries out; the larger space is an
    /// ignored test.
    client_writes: bool,
}

fn majority(cfg: u8, set: u8) -> bool {
    2 * (cfg & set).count_ones() > cfg.count_ones()
}

fn config_of(log: &[Entry]) -> Config {
    log.iter()
        .rev()
        .find_map(|e| match e.kind {
            Kind::Joint => Some(Config::Joint),
            Kind::New => Some(Config::New),
            Kind::Data => None,
        })
        .unwrap_or(Config::Old)
}

fn last_term(log: &[Entry]) -> u8 {
    log.last().map_or(0, |e| e.term)
}

impl JointMembership {
    fn is_quorum(&self, cfg: Config, set: u8) -> bool {
        match cfg {
            Config::Old => majority(OLD, set),
            Config::New => majority(NEW, set),
            // membership_impl_quorum_set.rs:18-22: every config must reach its quorum.
            Config::Joint if self.joint_quorum => majority(OLD, set) && majority(NEW, set),
            // Planted bug: only the incoming configuration is counted.
            Config::Joint => majority(NEW, set),
        }
    }
}

impl Model for JointMembership {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let node = Node {
            term: 0,
            voted: None,
            role: Role::Follower,
            votes: 0,
            log: Log { len: 0, e: [BLANK; MAX_LOG] },
        };
        vec![State {
            nodes: [node; NODES],
            committed: Vec::new(),
            elected: Vec::new(),
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for n in 0..NODES as u8 {
            let node = &s.nodes[n as usize];
            match node.role {
                Role::Follower | Role::Candidate => {
                    out.push(Action::Timeout(n));
                    if node.role == Role::Candidate {
                        for v in 0..NODES as u8 {
                            if v != n {
                                out.push(Action::RequestVote { candidate: n, voter: v });
                            }
                        }
                        out.push(Action::BecomeLeader(n));
                    }
                }
                Role::Leader => {
                    if self.client_writes {
                        out.push(Action::ClientWrite(n));
                    }
                    out.push(Action::ProposeJoint(n));
                    out.push(Action::ProposeNew(n));
                    for f in 0..NODES as u8 {
                        if f != n {
                            out.push(Action::Replicate { leader: n, follower: f });
                        }
                    }
                    for i in 0..node.log.len() as u8 {
                        out.push(Action::Commit { leader: n, index: i });
                    }
                }
            }
        }
    }

    fn next_state(&self, s: &State, a: &Action) -> Option<State> {
        let mut t = s.clone();
        match *a {
            Action::Timeout(n) => {
                let node = &mut t.nodes[n as usize];
                if node.term >= MAX_TERM {
                    return None;
                }
                node.term += 1;
                node.role = Role::Candidate;
                node.voted = Some(n);
                node.votes = 1 << n;
            }
            Action::RequestVote { candidate, voter } => {
                let c = t.nodes[candidate as usize];
                let v = &mut t.nodes[voter as usize];
                if c.term < v.term || (c.term == v.term && v.voted.is_some()) {
                    return None;
                }
                if c.term > v.term {
                    v.term = c.term;
                    v.voted = None;
                    v.role = Role::Follower;
                    v.votes = 0;
                }
                // Raft section 5.4.1: grant only to a log at least as up to date.
                let up_to_date = (last_term(c.log.as_slice()), c.log.len()) >= (last_term(v.log.as_slice()), v.log.len());
                if !up_to_date {
                    return Some(t);
                }
                v.voted = Some(candidate);
                t.nodes[candidate as usize].votes |= 1 << voter;
            }
            Action::BecomeLeader(n) => {
                let node = &mut t.nodes[n as usize];
                if !self.is_quorum(config_of(node.log.as_slice()), node.votes) {
                    return None;
                }
                node.role = Role::Leader;
                node.votes = 0;
                let key = (node.term, n);
                if !t.elected.contains(&key) {
                    t.elected.push(key);
                    t.elected.sort();
                }
            }
            Action::ClientWrite(n) | Action::ProposeJoint(n) | Action::ProposeNew(n) => {
                let committed_joint = s.committed.iter().any(|(e, _)| e.kind == Kind::Joint);
                let node = &mut t.nodes[n as usize];
                if node.log.len() >= MAX_LOG {
                    return None;
                }
                let kind = match a {
                    Action::ClientWrite(_) => Kind::Data,
                    // One change at a time: start only from a uniform C_old.
                    Action::ProposeJoint(_) if config_of(node.log.as_slice()) == Config::Old => Kind::Joint,
                    // The uniform C_new follows once C_old,new is committed.
                    Action::ProposeNew(_)
                        if config_of(node.log.as_slice()) == Config::Joint && committed_joint =>
                    {
                        Kind::New
                    }
                    _ => return None,
                };
                node.log.push(Entry { term: node.term, kind });
            }
            Action::Replicate { leader, follower } => {
                let l = t.nodes[leader as usize];
                let f = &mut t.nodes[follower as usize];
                if f.term > l.term || f.log == l.log && f.term == l.term && f.role == Role::Follower {
                    return None;
                }
                if f.term < l.term {
                    f.voted = None;
                }
                f.term = l.term;
                f.role = Role::Follower;
                f.votes = 0;
                f.log = l.log;
            }
            Action::Commit { leader, index } => {
                let i = index as usize;
                let l = s.nodes[leader as usize];
                let l_log = l.log.as_slice();
                // Raft section 5.4.2: count replicas only for an entry of the leader's term.
                if l_log[i].term != l.term || s.committed.len() > i {
                    return None;
                }
                let mut acks = 0u8;
                for (k, node) in s.nodes.iter().enumerate() {
                    if node.log.len() > i && node.log.as_slice()[..=i] == l_log[..=i] {
                        acks |= 1 << k;
                    }
                }
                if !self.is_quorum(config_of(l.log.as_slice()), acks) {
                    return None;
                }
                for k in s.committed.len()..=i {
                    t.committed.push((l_log[k], l.term));
                }
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        for w in s.elected.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(format!(
                    "election-safety: nodes {} and {} both lead term {}",
                    w[0].1, w[1].1, w[0].0
                ));
            }
        }
        for (n, node) in s.nodes.iter().enumerate() {
            if node.role != Role::Leader {
                continue;
            }
            for (i, (e, ct)) in s.committed.iter().enumerate() {
                if *ct < node.term && node.log.as_slice().get(i) != Some(e) {
                    return Err(format!(
                        "leader-completeness: leader {n} of term {} lacks entry {i} {e:?} committed in term {ct}",
                        node.term
                    ));
                }
            }
        }
        Ok(())
    }
}

const LIMIT: usize = 3_000_000;

#[test]
fn joint_quorum_keeps_one_leader_per_term_and_committed_entries() {
    let e = common::expect_passed("joint-membership guarded", &JointMembership { joint_quorum: true, client_writes: false }, LIMIT);
    assert!(e.states > 1_000, "the bounded space should be non-trivial: {e:?}");
}

#[test]
fn incoming_majority_alone_loses_a_committed_entry() {
    let steps = common::expect_violated(
        "joint-membership without joint quorum",
        &JointMembership { joint_quorum: false, client_writes: false },
        LIMIT,
        "leader-completeness",
    );
    assert!(steps >= 4);
}

/// The same space with data entries as well (about 620,000 states, over a minute in debug).
#[test]
#[ignore]
fn joint_quorum_with_client_writes() {
    common::expect_passed(
        "joint-membership guarded with client writes",
        &JointMembership { joint_quorum: true, client_writes: true },
        LIMIT,
    );
}
