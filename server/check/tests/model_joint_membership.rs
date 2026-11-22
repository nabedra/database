//! Model of a Raft joint-consensus membership change (OLTP-G08).
//!
//! The OLTP replication groups run openraft 0.10, and their membership changes go
//! through `Raft::change_membership_if` (server/src/oltp_replication/manager/lifecycle.rs:368),
//! guarded by `Precondition::LastMembershipLogId` so a stale retry cannot start a
//! second change (lifecycle.rs:371). openraft moves through a joint configuration
//! C_old,new and decides a quorum of a joint membership as "a quorum of every config"
//! (openraft-0.10.0/src/membership/membership_impl_quorum_set.rs:16-23). Both votes
//! and commits use the membership of the latest membership entry in the node's own
//! log, committed or not (openraft's "effective membership", engine/engine_impl.rs:136
//! and engine/handler/replication_handler/mod.rs:118).
//! Nabedra uses advanced leader IDs (server/src/oltp_replication/entry.rs:21).
//! Votes and log freshness order leaders by (term, node ID), so a higher ID
//! can become leader within the same numeric term.
//!
//! The model has four nodes. C_old = {0, 1, 2} and C_new = {1, 2, 3}: node 3 is added
//! and node 0 removed. Terms go up to `MAX_TERM` and logs to `MAX_LOG` entries.
//! This is a bounded Raft model, with no pre-vote or leadership leases. RequestVote
//! and replication combine delivery and acknowledgment into one step, so delayed
//! and lost replies after a follower change are outside the model. Replication
//! installs the full leader log when the follower lacks that prefix. A commit
//! counts only matches acknowledged to that leader. Blank entries are omitted.
//!
//! Invariants: every higher leader must retain the committed prefix, and a
//! quorum cannot commit different entries at the same index.
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
type LeaderId = (u8, u8);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Data,
    Joint,
    New,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Entry {
    leader: LeaderId,
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

const BLANK: Entry = Entry {
    leader: (0, 0),
    kind: Kind::Data,
};

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
    vote: LeaderId,
    role: Role,
    /// Votes received while a candidate; cleared in every other role.
    votes: u8,
    /// Matched log lengths acknowledged to this leader by each follower.
    matched: [u8; NODES],
    log: Log,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Config {
    Old,
    Joint,
    New,
}

impl Config {
    fn voters(self) -> u8 {
        match self {
            Self::Old => OLD,
            Self::Joint => OLD | NEW,
            Self::New => NEW,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    nodes: [Node; NODES],
    /// Ghost: the committed prefix and the ID of the leader that committed each entry.
    committed: Vec<(Entry, LeaderId)>,
    conflicting_commit: bool,
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

fn last_log_id(log: &[Entry]) -> (LeaderId, usize) {
    (log.last().map_or((0, 0), |e| e.leader), log.len())
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
            vote: (0, 0),
            role: Role::Follower,
            votes: 0,
            matched: [0; NODES],
            log: Log {
                len: 0,
                e: [BLANK; MAX_LOG],
            },
        };
        vec![State {
            nodes: [node; NODES],
            committed: Vec::new(),
            conflicting_commit: false,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        for n in 0..NODES as u8 {
            let node = &s.nodes[n as usize];
            match node.role {
                Role::Follower | Role::Candidate => {
                    out.push(Action::Timeout(n));
                    if node.role == Role::Candidate {
                        let voters = config_of(node.log.as_slice()).voters();
                        for v in 0..NODES as u8 {
                            if v != n && voters & (1 << v) != 0 {
                                out.push(Action::RequestVote {
                                    candidate: n,
                                    voter: v,
                                });
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
                            out.push(Action::Replicate {
                                leader: n,
                                follower: f,
                            });
                        }
                    }
                    for i in 0..node.log.len() as u8 {
                        out.push(Action::Commit {
                            leader: n,
                            index: i,
                        });
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
                if node.vote.0 >= MAX_TERM
                    || config_of(node.log.as_slice()).voters() & (1 << n) == 0
                {
                    return None;
                }
                node.vote = (node.vote.0 + 1, n);
                node.role = Role::Candidate;
                node.votes = 1 << n;
            }
            Action::RequestVote { candidate, voter } => {
                let c = t.nodes[candidate as usize];
                let v = &mut t.nodes[voter as usize];
                if c.vote <= v.vote || last_log_id(c.log.as_slice()) < last_log_id(v.log.as_slice())
                {
                    return None;
                }
                v.vote = c.vote;
                v.role = Role::Follower;
                v.votes = 0;
                v.matched = [0; NODES];
                t.nodes[candidate as usize].votes |= 1 << voter;
            }
            Action::BecomeLeader(n) => {
                let node = &mut t.nodes[n as usize];
                if !self.is_quorum(config_of(node.log.as_slice()), node.votes) {
                    return None;
                }
                node.role = Role::Leader;
                node.votes = 0;
            }
            Action::ClientWrite(n) | Action::ProposeJoint(n) | Action::ProposeNew(n) => {
                let node = &mut t.nodes[n as usize];
                if node.log.len() >= MAX_LOG {
                    return None;
                }
                let kind = match a {
                    Action::ClientWrite(_) => Kind::Data,
                    // One change at a time: start only from a uniform C_old.
                    Action::ProposeJoint(_) if config_of(node.log.as_slice()) == Config::Old => {
                        Kind::Joint
                    }
                    // The uniform C_new follows once C_old,new is committed.
                    Action::ProposeNew(_)
                        if config_of(node.log.as_slice()) == Config::Joint
                            && node.log.as_slice().iter().zip(&s.committed).any(
                                |(local, (committed, _))| {
                                    local == committed && local.kind == Kind::Joint
                                },
                            ) =>
                    {
                        Kind::New
                    }
                    _ => return None,
                };
                node.log.push(Entry {
                    leader: node.vote,
                    kind,
                });
            }
            Action::Replicate { leader, follower } => {
                let l = t.nodes[leader as usize];
                let f = &mut t.nodes[follower as usize];
                if f.vote > l.vote {
                    return None;
                }
                f.vote = l.vote;
                f.role = Role::Follower;
                f.votes = 0;
                f.matched = [0; NODES];
                if !f.log.as_slice().starts_with(l.log.as_slice()) {
                    f.log = l.log;
                }
                t.nodes[leader as usize].matched[follower as usize] = l.log.len;
            }
            Action::Commit { leader, index } => {
                let i = index as usize;
                let l = s.nodes[leader as usize];
                let l_log = l.log.as_slice();
                // Only an entry of the current leader ID can advance the commit index.
                if l_log[i].leader != l.vote {
                    return None;
                }
                let mut acks = 1 << leader;
                for (k, &matched) in l.matched.iter().enumerate() {
                    if matched as usize > i {
                        acks |= 1 << k;
                    }
                }
                if !self.is_quorum(config_of(l.log.as_slice()), acks) {
                    return None;
                }
                t.conflicting_commit |= l_log[..=i]
                    .iter()
                    .zip(&s.committed)
                    .any(|(local, (committed, _))| local != committed);
                for &entry in l_log.iter().take(i + 1).skip(s.committed.len()) {
                    t.committed.push((entry, l.vote));
                }
            }
        }
        Some(t)
    }

    fn check(&self, s: &State) -> Result<(), String> {
        for (n, node) in s.nodes.iter().enumerate() {
            if node.role != Role::Leader {
                continue;
            }
            for (i, (e, committed_by)) in s.committed.iter().enumerate() {
                if *committed_by < node.vote && node.log.as_slice().get(i) != Some(e) {
                    return Err(format!(
                        "leader-completeness: leader {n} at {:?} lacks entry {i} {e:?} committed by {committed_by:?}",
                        node.vote
                    ));
                }
            }
        }
        if s.conflicting_commit {
            return Err(
                "commit-agreement: two quorums committed different entries at one index".into(),
            );
        }
        Ok(())
    }
}

const LIMIT: usize = 3_000_000;

#[test]
fn joint_quorum_preserves_committed_entries_across_leaders() {
    let e = common::expect_passed(
        "joint-membership guarded",
        &JointMembership {
            joint_quorum: true,
            client_writes: false,
        },
        LIMIT,
    );
    assert!(
        e.states > 1_000,
        "the bounded space should be non-trivial: {e:?}"
    );
}

#[test]
fn incoming_majority_alone_loses_a_committed_entry() {
    let steps = common::expect_violated(
        "joint-membership without joint quorum",
        &JointMembership {
            joint_quorum: false,
            client_writes: false,
        },
        LIMIT,
        "leader-completeness",
    );
    assert!(steps >= 4);
}

/// Optional larger space with both data and membership entries.
#[test]
#[ignore]
fn joint_quorum_with_client_writes() {
    common::expect_passed(
        "joint-membership guarded with client writes",
        &JointMembership {
            joint_quorum: true,
            client_writes: true,
        },
        LIMIT,
    );
}
