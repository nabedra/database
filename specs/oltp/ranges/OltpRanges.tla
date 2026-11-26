----------------------------- MODULE OltpRanges -----------------------------
(***************************************************************************)
(* One OLTP Raft group (a Data range, or a system group such as Control or *)
(* Timestamps): elections and commits, leader leases under bounded clock   *)
(* drift, replica identity across restarts without the disk or with an     *)
(* older copy of it, and a voter replacement through a learner and a joint *)
(* configuration.                                                          *)
(*                                                                         *)
(* Plans: plans/oltp-engine.md "Shards, disk layout, and indexes" (verify  *)
(* the volume before opening a store; never adopt a disk by its path),    *)
(* "Read execution" (strong leader reads; lease reads need clock tests),   *)
(* "ACID" (local durability), "AWS deployment" (stable worker identity     *)
(* across replacement Pods), "Scale-up" (learner, catch-up, promote, then  *)
(* remove); plans/oltp-direct-path.md sections 5 and 6 (AllocateDirect and *)
(* table.get use LeaseRead). Register OLTP-G01, G02, G08, G10. Independent *)
(* read R-06, R-07, R-09, R-10, R-14, R-20.                                *)
(*                                                                         *)
(* Raft abstraction. The group has one committed history `hist`, the state *)
(* after each committed entry. Each node durably holds a prefix of it      *)
(* (`mlen`) and a term, which is also its vote: granting a vote raises the *)
(* durable term, so a node grants at most one vote per term while its disk *)
(* is intact. A leader's append reaches every node it can talk to that     *)
(* accepts it, so a node lags only behind a crash, a partition or a        *)
(* restore, and `CatchUp` brings it back. A leader whose prefix is shorter *)
(* than the committed history overwrites committed entries when it         *)
(* commits; the spec records that as `lost` instead of assuming it away.   *)
(* Message loss, duplication and reordering inside Raft are covered by     *)
(* Raft's term and log-matching checks and are out of scope: a step either *)
(* reaches a node or does not.                                             *)
(*                                                                         *)
(* Time. True time `now` and a clock offset per node within DriftBound, so *)
(* a local duration can be off by twice the bound. openraft measures with  *)
(* tokio Instant (monotonic), so a clock error is a drift step, not a jump. *)
(* A heartbeat is sent and received at the same true time, which leaves no *)
(* network delay as slack: the worst case for the lease.                   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    Node,          \* worker ids; the initial voters and one spare
    Spare,         \* the node outside the group at the start (move target)
    Zone,          \* [Node -> zone]
    Client,        \* writers; each client commits one write
    Ops,           \* subset of {"lease", "move"}: the parts of the model a run enables
    Mutant,        \* "none" or a mutant name, see mutants/*.cfg
    MaxTerm,       \* highest Raft term
    MaxCrash,      \* crash and restart with the disk
    MaxWipe,       \* crash with the pool lost, then restart
    MaxStale,      \* restart from an older copy of the pool (R-07)
    MaxPart,       \* isolate one node, then heal
    MaxDrift,      \* clock offset bound in ticks, |clock - true time|
    MaxDriftSteps, \* clock drift budget
    MaxTime,       \* true time horizon in ticks
    Lease,         \* leader lease and follower vote-refusal window, in ticks
    LeaseMargin    \* leader-side margin subtracted from the lease

None == "none"
\* No replacement in progress, and no operator copy: records, so that TLC
\* compares like with like.
NoPlan == [on |-> FALSE, from |-> None, old |-> {}, new |-> {}, retired |-> FALSE]
NoCopy == [term |-> -1, mlen |-> -1]
InitVoters == Node \ {Spare}

ASSUME Spare \in Node /\ Cardinality(InitVoters) = 3
ASSUME Zone \in [Node -> STRING]
ASSUME Ops \subseteq {"lease", "move"}

LeaseOn == "lease" \in Ops
MoveOn == "move" \in Ops

(***************************************************************************)
(* Design choices under test. Each mutant names one; see mutants/*.cfg.    *)
(***************************************************************************)
\* SHIPPED: openraft 0.10.0 checks now < quorum-acked send time + lease on
\* the leader and refuses votes for the same lease from receipt on the
\* follower, with no allowance for clock rate error. The fixed design
\* shortens the leader's lease by LeaseMargin.
EffMargin == IF Mutant = "prefix_no_lease_margin" THEN 0 ELSE LeaseMargin
\* A clock that drifts past the bound the margin was sized for.
DriftBound == IF Mutant = "drift_beyond_bound" THEN MaxDrift + 1 ELSE MaxDrift
\* Replica identity. Fixed design: a replica whose pool was lost, or
\* replaced by an older copy, never votes or acknowledges under its old
\* identity; it can come back only as a new learner.
\* SHIPPED, empty pool: Data replicas refuse to start (retained_group,
\* pool.rs:664-669), but the system groups (Control, Timestamps, Status,
\* Catalog) open with create and rejoin empty (registered.rs:244-252);
\* open record 2026-11-25-oltp-sim-emptied-volume-votes. The prefix mutant
\* restores that.
\* SHIPPED, older copy: nothing compares the disk with Control's record
\* within one incarnation (registered/restart.rs:82,119,143), so a copy
\* rejoins with its older vote and log, in every group kind.
WipedRejoins == Mutant = "prefix_wiped_rejoins"
StaleRejoins == Mutant = "prefix_stale_rejoins"

VARIABLES
    hist,      \* committed history: Seq of group states
    term,      \* [Node -> 0..MaxTerm], durable term and vote
    mlen,      \* [Node -> Nat], durable committed prefix
    gone,      \* [Node -> BOOLEAN], identity retired: never votes or acks again
    up,        \* [Node -> BOOLEAN], process running
    role,      \* [Node -> {"F", "C", "L"}], volatile
    lease,     \* [Node -> Int], volatile: leader lease end, local clock
    vblock,    \* [Node -> Int], volatile: refuse votes until, local clock
    off,       \* [Node -> Int], clock offset from true time
    now,       \* true time
    bkp,       \* [Node -> record], an operator copy of the pool, or NoCopy
    iso,       \* the isolated node, or None
    plan,      \* the replacement in progress, or NoPlan
    acked,     \* ghost: clients that got success
    voteMax,   \* ghost: [Node -> Nat], highest term each identity ever voted in
    revote,    \* ghost: a node granted a vote in a term it had voted in
    lost,      \* ghost: a committed entry was overwritten
    badRead,   \* ghost: a lease read missed an acknowledged write
    bud        \* fault counters

nodeVars == <<term, mlen, gone, up, role, lease, vblock, bkp>>
timeVars == <<off, now>>
ghostVars == <<acked, voteMax, revote, lost, badRead>>

\* Faults hit the spare only in a run where it takes part.
FaultNode == IF MoveOn THEN Node ELSE InitVoters
vars == <<hist, nodeVars, timeVars, iso, plan, ghostVars, bud>>

(***************************************************************************)
(* Group state machine. Apply is deterministic.                            *)
(***************************************************************************)
S0 == [vo |-> {}, vn |-> {}, lrn |-> {}, w |-> {}]
SInit == [S0 EXCEPT !.vo = InitVoters, !.vn = InitVoters]

Apply(st, e) ==
    CASE e.k = "w" -> [st EXCEPT !.w = @ \cup {e.c}]
      [] e.k = "learner" -> [st EXCEPT !.lrn = @ \cup {e.n}]
      \* change_membership_if: the joint configuration old + new.
      [] e.k = "joint" -> [st EXCEPT !.vn = e.vn, !.lrn = @ \ e.vn]
      \* The second step. retain_removed keeps the removed voter a learner.
      [] e.k = "final" -> [st EXCEPT !.lrn = @ \cup (st.vo \ st.vn), !.vo = st.vn]
      [] OTHER -> st

\* A node beyond the committed history exists only after `lost`, where
\* every run already stops; it reads the last state.
StateAt(len) == IF len = 0 THEN S0 ELSE hist[IF len > Len(hist) THEN Len(hist) ELSE len]
Cur == StateAt(Len(hist))

IsMaj(Q, V) == V # {} /\ 2 * Cardinality(Q \cap V) > Cardinality(V)
Quorum(st, Q) == IsMaj(Q, st.vo) /\ IsMaj(Q, st.vn)
Voters(st) == st.vo \cup st.vn

\* No zone holds a majority of a voter set: one AZ loss keeps a quorum.
ZoneSafe(V) == \A z \in {Zone[n] : n \in V} :
                   2 * Cardinality({n \in V : Zone[n] = z}) < Cardinality(V)

Live(n) == up[n] /\ ~gone[n]
CanTalk(a, b) == a = b \/ iso \notin {a, b}
Clock(n) == now + off[n]
MinClock == -(MaxDrift + 2)

\* openraft is_lease_valid, minus the margin of the fixed design.
LeaseValid(n) == LeaseOn /\ Clock(n) + EffMargin < lease[n]
\* A follower refuses a vote until its lease from the last leader expired
\* (engine_impl.rs:421-424, is_expired(now, 0)).
VoteFree(q) == ~LeaseOn \/ Clock(q) > vblock[q]

Init ==
    /\ hist = <<SInit>>
    /\ term = [n \in Node |-> 0]
    /\ mlen = [n \in Node |-> IF n \in InitVoters THEN 1 ELSE 0]
    /\ gone = [n \in Node |-> FALSE]
    /\ up = [n \in Node |-> TRUE]
    /\ role = [n \in Node |-> "F"]
    /\ lease = [n \in Node |-> MinClock]
    /\ vblock = [n \in Node |-> MinClock]
    /\ off = [n \in Node |-> 0]
    /\ now = 0
    /\ bkp = [n \in Node |-> NoCopy]
    /\ iso = None
    /\ plan = NoPlan
    /\ acked = {}
    /\ voteMax = [n \in Node |-> 0]
    /\ revote = FALSE
    /\ lost = FALSE
    /\ badRead = FALSE
    /\ bud = [crash |-> 0, wipe |-> 0, copy |-> 0, stale |-> 0, part |-> 0, drift |-> 0]

(***************************************************************************)
(* Raft. Engine plan "ACID" (local durability; only authoritative quorums  *)
(* decide writes and strong reads). OLTP-G01, G08, G10.                    *)
(***************************************************************************)
\* Voters that grant n's request for term t: live, reachable, of a lower
\* term, not more up to date, outside a lease they gave a leader. Under the
\* mutant `vote_no_log_check` the log comparison is skipped.
Granting(n, t) ==
    LET st == StateAt(mlen[n]) IN
    {q \in Voters(st) :
        /\ Live(q) /\ CanTalk(n, q)
        /\ (term[q] < t \/ (Mutant = "double_vote" /\ term[q] = t))
        /\ (mlen[q] <= mlen[n] \/ Mutant = "vote_no_log_check")
        /\ (q = n \/ VoteFree(q))}

\* RequestVote round of n's election timer (openraft engine_impl).
HandleElectionTimeout(n) ==
    LET st == StateAt(mlen[n])
        t == term[n] + 1
        Q == Granting(n, t)
    IN
    /\ Live(n) /\ n \in Voters(st) /\ t <= MaxTerm
    /\ n \in Q /\ Quorum(st, Q)
    /\ term' = [m \in Node |-> IF m \in Q THEN t ELSE term[m]]
    /\ role' = [m \in Node |-> IF m = n THEN "C" ELSE IF m \in Q THEN "F" ELSE role[m]]
    /\ revote' = (revote \/ \E q \in Q : voteMax[q] >= t)
    /\ voteMax' = [m \in Node |-> IF m \in Q /\ voteMax[m] < t THEN t ELSE voteMax[m]]
    /\ UNCHANGED <<hist, mlen, gone, up, lease, vblock, bkp, timeVars, iso, plan,
                   acked, lost, badRead, bud>>

\* Nodes that accept an append from leader n: voters and learners of its
\* configuration that are live, reachable and not of a higher term.
Accepting(n) ==
    LET st == StateAt(mlen[n]) IN
    {q \in Voters(st) \cup (IF Mutant = "learner_not_replicated" THEN {} ELSE st.lrn) \cup {n} :
        Live(q) /\ CanTalk(n, q) /\ term[q] <= term[n]}

QuorumOK(n) == Quorum(StateAt(mlen[n]), Accepting(n))

\* The accepting nodes adopt n's term and the new prefix; a lower-term
\* leader among them steps down. Followers renew their vote-refusal window
\* from their own clock (vote_handler/mod.rs:128-142).
Adopt(n, newLen, newRole) ==
    LET Q == Accepting(n) IN
    /\ mlen' = [m \in Node |-> IF m \in Q THEN newLen ELSE mlen[m]]
    /\ term' = [m \in Node |-> IF m \in Q THEN term[n] ELSE term[m]]
    /\ role' = [m \in Node |-> IF m = n THEN newRole
                               ELSE IF m \in Q THEN "F" ELSE role[m]]
    /\ vblock' = [m \in Node |-> IF m \in Q \ {n} /\ LeaseOn THEN Clock(m) + Lease
                                 ELSE vblock[m]]

\* Commit entry e at leader n: the committed history becomes n's prefix
\* plus e. Missing committed entries are overwritten.
DoCommit(n, e) ==
    LET newHist == Append(SubSeq(hist, 1, mlen[n]), Apply(StateAt(mlen[n]), e)) IN
    /\ hist' = newHist
    /\ lost' = (lost \/ mlen[n] < Len(hist))
    /\ Adopt(n, Len(newHist), "L")

\* The first entry of a new term. Until it commits the leader serves
\* nothing (direct path plan section 5 rule 3).
CommitBlank(n) ==
    /\ Live(n) /\ role[n] = "C" /\ QuorumOK(n)
    /\ hist' = SubSeq(hist, 1, mlen[n])
    /\ lost' = (lost \/ mlen[n] < Len(hist))
    /\ Adopt(n, mlen[n], "L")
    /\ UNCHANGED <<gone, up, lease, bkp, timeVars, iso, plan, acked, voteMax, revote,
                   badRead, bud>>

\* The leader commits client c's write and replies success.
CommitWrite(n, c) ==
    /\ Live(n) /\ role[n] = "L" /\ QuorumOK(n)
    /\ Mutant = "commit_needs_all" => Voters(StateAt(mlen[n])) \subseteq Accepting(n)
    /\ c \notin acked /\ c \notin StateAt(mlen[n]).w
    /\ DoCommit(n, [k |-> "w", c |-> c])
    /\ acked' = acked \cup {c}
    /\ UNCHANGED <<gone, up, lease, bkp, timeVars, iso, plan, voteMax, revote,
                   badRead, bud>>

\* AppendEntries to a node that lags after a crash, partition or restore.
CatchUp(n, q) ==
    /\ Live(n) /\ role[n] = "L" /\ q # n /\ q \in Accepting(n)
    /\ mlen[q] < mlen[n]
    /\ mlen' = [mlen EXCEPT ![q] = mlen[n]]
    /\ term' = [term EXCEPT ![q] = term[n]]
    /\ role' = [role EXCEPT ![q] = "F"]
    /\ vblock' = IF LeaseOn THEN [vblock EXCEPT ![q] = Clock(q) + Lease] ELSE vblock
    /\ UNCHANGED <<hist, gone, up, lease, bkp, timeVars, iso, plan, ghostVars, bud>>

\* A node that hears from a node of a higher term (a request or a response
\* carrying a higher vote) adopts that term without voting, and a leader or
\* candidate steps down.
StepDown(n, q) ==
    /\ Live(n) /\ Live(q) /\ CanTalk(n, q) /\ term[q] > term[n]
    /\ term' = [term EXCEPT ![n] = term[q]]
    /\ role' = [role EXCEPT ![n] = "F"]
    /\ lease' = [lease EXCEPT ![n] = MinClock]
    /\ UNCHANGED <<hist, mlen, gone, up, vblock, bkp, timeVars, iso, plan, ghostVars, bud>>

\* A heartbeat round acknowledged by a quorum at send time Clock(n)
\* (openraft last_quorum_acked_time, proposer/leader.rs:312-319).
Heartbeat(n) ==
    LET Q == Accepting(n) IN
    /\ LeaseOn /\ Live(n) /\ role[n] = "L" /\ QuorumOK(n)
    /\ term' = [m \in Node |-> IF m \in Q THEN term[n] ELSE term[m]]
    /\ role' = [m \in Node |-> IF m \in Q \ {n} THEN "F" ELSE role[m]]
    /\ vblock' = [m \in Node |-> IF m \in Q \ {n} THEN Clock(m) + Lease ELSE vblock[m]]
    /\ lease' = [lease EXCEPT ![n] = Clock(n) + Lease]
    /\ UNCHANGED <<hist, mlen, gone, up, bkp, timeVars, iso, plan, ghostVars, bud>>

\* ensure_linearizable(LeaseRead), then a read of the leader's applied
\* state (table.get, AllocateDirect; hosted.rs:1042-1066). The ghost: the
\* read sees every write acknowledged before it.
LeaseRead(n) ==
    /\ LeaseOn /\ Live(n) /\ role[n] = "L" /\ LeaseValid(n)
    /\ badRead' = (badRead \/ \E c \in acked : c \notin StateAt(mlen[n]).w)
    /\ UNCHANGED <<hist, nodeVars, timeVars, iso, plan, acked, voteMax, revote,
                   lost, bud>>

(***************************************************************************)
(* Voter replacement. Engine plan "Scale-up": add a learner, catch up,     *)
(* promote through safe membership changes, then remove the old voter;     *)
(* O17 "drain before terminate", "Keep minimum AZ placement". SHIPPED:     *)
(* manager/lifecycle.rs:158-182, 358-378 (add_learner, change_membership_if *)
(* with LastMembershipLogId, retain_removed), placement/remote.rs:97-108   *)
(* (zones), machine/placement.rs:367-386 (retire after publish).           *)
(***************************************************************************)
\* The controller starts a replacement of voter `from` by the spare.
MoveBegin(from) ==
    LET st == Cur
        next == (st.vn \ {from}) \cup {Spare}
    IN
    /\ MoveOn /\ ~plan.on /\ st.vo = st.vn /\ from \in st.vn
    /\ Spare \notin Voters(st) \cup st.lrn /\ Live(Spare)
    /\ ZoneSafe(next) \/ Mutant = "no_zone_check"
    /\ plan' = [on |-> TRUE, from |-> from, old |-> st.vn, new |-> next, retired |-> FALSE]
    /\ UNCHANGED <<hist, nodeVars, timeVars, iso, ghostVars, bud>>

PlanEntry(n) ==
    LET st == StateAt(mlen[n]) IN
    IF Spare \notin st.lrn \cup Voters(st) THEN [k |-> "learner", n |-> Spare]
    ELSE IF st.vo = plan.old /\ st.vn = plan.old THEN [k |-> "joint", vn |-> plan.new]
    ELSE IF st.vo = plan.old /\ st.vn = plan.new THEN [k |-> "final"]
    ELSE [k |-> "none"]

\* The leader commits the next membership step of the plan. A learner is
\* promoted only once it holds the leader's prefix (lifecycle.rs:158-182).
CommitMembership(n) ==
    LET e == PlanEntry(n) IN
    /\ plan.on /\ Live(n) /\ role[n] = "L" /\ QuorumOK(n) /\ e.k # "none"
    /\ e.k = "joint" => mlen[Spare] = mlen[n]
    /\ DoCommit(n, e)
    /\ UNCHANGED <<gone, up, lease, bkp, timeVars, iso, plan, acked, voteMax, revote,
                   badRead, bud>>

\* Retire the replaced replica: stop it and release its pool. The design
\* waits until the configuration without it is committed.
RetireOld ==
    LET ready == IF Mutant = "retire_on_propose" THEN TRUE
                 ELSE Cur.vo = plan.new /\ Cur.vn = plan.new
    IN
    /\ plan.on /\ ~plan.retired /\ ready
    /\ up' = [up EXCEPT ![plan.from] = FALSE]
    /\ gone' = [gone EXCEPT ![plan.from] = TRUE]
    /\ mlen' = [mlen EXCEPT ![plan.from] = 0]
    /\ role' = [role EXCEPT ![plan.from] = "F"]
    /\ plan' = [plan EXCEPT !.retired = TRUE]
    /\ UNCHANGED <<hist, term, lease, vblock, bkp, timeVars, iso, ghostVars, bud>>

(***************************************************************************)
(* Faults. Each is budgeted. A crash keeps the disk, a wipe loses the pool, *)
(* a stale restore replaces it by an older operator copy of the same pool. *)
(***************************************************************************)
Crash(n) ==
    /\ up[n] /\ bud.crash < MaxCrash
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ role' = [role EXCEPT ![n] = "F"]
    /\ lease' = [lease EXCEPT ![n] = MinClock]
    /\ bud' = [bud EXCEPT !.crash = @ + 1]
    /\ UNCHANGED <<hist, term, mlen, gone, vblock, bkp, timeVars, iso, plan, ghostVars>>

\* Restart with the disk. openraft renews the follower lease at startup for
\* a committed vote (engine_impl.rs:196-200); the mutant forgets it.
Restart(n) ==
    /\ ~up[n] /\ ~gone[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ vblock' = IF LeaseOn /\ Mutant # "restart_forgets_lease"
                 THEN [vblock EXCEPT ![n] = Clock(n) + Lease] ELSE vblock
    /\ UNCHANGED <<hist, term, mlen, gone, role, lease, bkp, timeVars, iso, plan, ghostVars, bud>>

\* The pool is lost (volume replaced or emptied) and the worker restarts.
\* Under the prefix mutant the replica rejoins under the same identity
\* with an empty log and no vote (R-06, R-14); in the fixed design the
\* identity is retired and the replica can return only as a learner.
Wipe(n) ==
    /\ bud.wipe < MaxWipe /\ ~gone[n] /\ n # Spare
    /\ up' = [up EXCEPT ![n] = WipedRejoins]
    /\ gone' = [gone EXCEPT ![n] = ~WipedRejoins]
    /\ term' = [term EXCEPT ![n] = 0]
    /\ mlen' = [mlen EXCEPT ![n] = 0]
    /\ role' = [role EXCEPT ![n] = "F"]
    /\ lease' = [lease EXCEPT ![n] = MinClock]
    /\ vblock' = [vblock EXCEPT ![n] = MinClock]
    /\ bkp' = [bkp EXCEPT ![n] = NoCopy]
    /\ bud' = [bud EXCEPT !.wipe = @ + 1]
    /\ UNCHANGED <<hist, timeVars, iso, plan, ghostVars>>

\* An operator copies node n's pool while it runs (a volume snapshot).
Backup(n) ==
    /\ bud.copy < MaxStale /\ bkp[n] = NoCopy /\ ~gone[n] /\ n # Spare
    /\ bkp' = [bkp EXCEPT ![n] = [term |-> term[n], mlen |-> mlen[n]]]
    /\ bud' = [bud EXCEPT !.copy = @ + 1]
    /\ UNCHANGED <<hist, term, mlen, gone, up, role, lease, vblock, timeVars, iso, plan,
                   ghostVars>>

\* The crashed worker is rebuilt from that older copy, identity files
\* intact (R-07). Under the prefix mutant it rejoins with the older term
\* and log; in the fixed design the copy is not the current pool and the
\* identity is retired.
StaleRestore(n) ==
    /\ bkp[n] # NoCopy /\ bud.stale < MaxStale /\ ~up[n] /\ ~gone[n]
    /\ up' = [up EXCEPT ![n] = StaleRejoins]
    /\ gone' = [gone EXCEPT ![n] = ~StaleRejoins]
    /\ term' = [term EXCEPT ![n] = bkp[n].term]
    /\ mlen' = [mlen EXCEPT ![n] = bkp[n].mlen]
    /\ role' = [role EXCEPT ![n] = "F"]
    /\ vblock' = [vblock EXCEPT ![n] = MinClock]
    /\ bkp' = [bkp EXCEPT ![n] = NoCopy]
    /\ bud' = [bud EXCEPT !.stale = @ + 1]
    /\ UNCHANGED <<hist, lease, timeVars, iso, plan, ghostVars>>

Partition(n) ==
    /\ bud.part < MaxPart /\ iso = None
    /\ iso' = n
    /\ bud' = [bud EXCEPT !.part = @ + 1]
    /\ UNCHANGED <<hist, nodeVars, timeVars, plan, ghostVars>>

Heal ==
    /\ iso # None
    /\ iso' = None
    /\ UNCHANGED <<hist, nodeVars, timeVars, plan, ghostVars, bud>>

\* True time advances; every clock with it.
Tick ==
    /\ LeaseOn /\ now < MaxTime
    /\ LET nextTime == now + 1
           earliestClock == nextTime - DriftBound
           expiredLease == -DriftBound + EffMargin - 1
       IN /\ now' = nextTime
          \* Even future drift cannot revive these stamps. Keep their distinct
          \* boundaries: leader validity is strict and voter blocking includes equality.
          /\ lease' = [n \in Node |-> IF lease[n] <= earliestClock + EffMargin
                                      THEN expiredLease ELSE lease[n]]
          /\ vblock' = [n \in Node |-> IF vblock[n] < earliestClock
                                       THEN MinClock ELSE vblock[n]]
    /\ UNCHANGED <<hist, term, mlen, gone, up, role, bkp, off, iso, plan, ghostVars, bud>>

\* A clock runs a tick fast or slow, within the bound.
Drift(n, d) ==
    /\ LeaseOn /\ bud.drift < MaxDriftSteps
    /\ off[n] + d \in -DriftBound..DriftBound
    /\ off' = [off EXCEPT ![n] = off[n] + d]
    /\ bud' = [bud EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<hist, nodeVars, now, iso, plan, ghostVars>>

(***************************************************************************)
(* Next state                                                              *)
(***************************************************************************)
ElectAny == \E n \in Node : HandleElectionTimeout(n)
BlankAny == \E n \in Node : CommitBlank(n)
WriteAny == \E n \in Node, c \in Client : CommitWrite(n, c)
CatchUpAny == \E n, q \in Node : CatchUp(n, q)
StepDownAny == \E n, q \in Node : StepDown(n, q)
HeartbeatAny == \E n \in Node : Heartbeat(n)
LeaseReadAny == \E n \in Node : LeaseRead(n)
MoveBeginAny == \E n \in Node : MoveBegin(n)
MembershipAny == \E n \in Node : CommitMembership(n)
CrashAny == \E n \in FaultNode : Crash(n)
RestartAny == \E n \in Node : Restart(n)
WipeAny == \E n \in FaultNode : Wipe(n)
BackupAny == \E n \in FaultNode : Backup(n)
StaleRestoreAny == \E n \in FaultNode : StaleRestore(n)
PartitionAny == \E n \in FaultNode : Partition(n)
DriftAny == \E n \in FaultNode, d \in {-1, 1} : Drift(n, d)

\* Every client has its answer and no replacement is in flight. Two other
\* ends are allowed and named: no quorum of replicas that keep their
\* identity is left (two permanent losses exceed RF3), or no leader is left
\* and the term bound of the model allows no election.
Finished == acked = Client /\ (~plan.on \/ plan.retired)
NoQuorumLeft == ~Quorum(Cur, {n \in Node : ~gone[n]})
TermBound ==
    /\ ~\E n \in Node : Live(n) /\ role[n] \in {"C", "L"}
    /\ ~\E n \in Node : Live(n) /\ term[n] < MaxTerm /\ ENABLED HandleElectionTimeout(n)
Terminal == (Finished \/ NoQuorumLeft \/ TermBound) /\ UNCHANGED vars

Next ==
    \/ ElectAny \/ BlankAny \/ WriteAny \/ CatchUpAny \/ StepDownAny \/ HeartbeatAny
    \/ LeaseReadAny
    \/ MoveBeginAny \/ MembershipAny \/ RetireOld
    \/ CrashAny \/ RestartAny \/ WipeAny \/ BackupAny \/ StaleRestoreAny
    \/ PartitionAny \/ Heal \/ Tick \/ DriftAny
    \/ Terminal

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Liveness, checked after the fault budgets are spent. The mechanisms:    *)
(* the election timer, which fires only while no leader reaches the node   *)
(* (NeedElection); the leader's replication loop (CommitBlank, CatchUp,    *)
(* StepDown on a higher term);                                             *)
(* the client's retry of its pending write against the refreshed leader    *)
(* (CommitWrite); the replacement controller's reconcile loop              *)
(* (CommitMembership, RetireOld); the worker supervisor (Restart) and the  *)
(* end of the partition (Heal). Faults and MoveBegin get no fairness.      *)
(***************************************************************************)
LeaderReaches(n) ==
    \E l \in Node : Live(l) /\ role[l] \in {"C", "L"} /\ CanTalk(l, n) /\ term[l] >= term[n]
NeedElection == \E n \in Node : ~LeaderReaches(n) /\ HandleElectionTimeout(n)
Fairness ==
    /\ WF_vars(NeedElection) /\ WF_vars(BlankAny) /\ WF_vars(WriteAny)
    /\ WF_vars(CatchUpAny) /\ WF_vars(StepDownAny)
    /\ WF_vars(MembershipAny) /\ WF_vars(RetireOld)
    /\ WF_vars(RestartAny) /\ WF_vars(Heal)
\* The liveness run takes an election only when no leader reaches the node.
\* The model bounds terms, so spurious elections (which randomized election
\* timeouts make rare) could otherwise use up every term and leave no
\* leader: an artifact of the bound, not of the design. Safety runs keep
\* every election.
LiveNext ==
    \/ NeedElection \/ BlankAny \/ WriteAny \/ CatchUpAny \/ StepDownAny \/ HeartbeatAny
    \/ LeaseReadAny
    \/ MoveBeginAny \/ MembershipAny \/ RetireOld
    \/ CrashAny \/ RestartAny \/ WipeAny \/ BackupAny \/ StaleRestoreAny
    \/ PartitionAny \/ Heal \/ Tick \/ DriftAny
    \/ Terminal
LiveSpec == Init /\ [][LiveNext]_vars /\ Fairness

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ \A i \in 1..Len(hist) : hist[i].w \subseteq Client
    /\ term \in [Node -> 0..MaxTerm]
    /\ \A n \in Node : mlen[n] \in 0..Len(hist) \/ lost
    /\ gone \in [Node -> BOOLEAN] /\ up \in [Node -> BOOLEAN]
    /\ role \in [Node -> {"F", "C", "L"}]
    /\ lease \in [Node -> Int] /\ vblock \in [Node -> Int]
    /\ off \in [Node -> -DriftBound..DriftBound] /\ now \in 0..MaxTime
    /\ iso \in Node \cup {None}
    /\ acked \subseteq Client
    /\ voteMax \in [Node -> 0..MaxTerm] /\ revote \in BOOLEAN
    /\ lost \in BOOLEAN /\ badRead \in BOOLEAN

\* OLTP-G08, OLTP-G10: at most one leader per term.
G08_OneLeaderPerTerm ==
    \A a, b \in Node :
        /\ Live(a) /\ Live(b) /\ role[a] \in {"C", "L"} /\ role[b] \in {"C", "L"}
        /\ term[a] = term[b]
        => a = b

\* OLTP-G08 (the simulator's WipedNeverVotes and MonotonicVote): an
\* identity never votes again in a term it voted in, across restarts
\* without the disk and with an older disk.
G08_NoRevote == ~revote

\* OLTP-G01, OLTP-G08: no committed entry is ever overwritten.
G01_CommittedNeverLost == ~lost

\* OLTP-G01: while at most one replica was lost to a fault (the single-AZ
\* failure model), every acknowledged write has a holder that keeps its
\* identity, and every quorum of the current configuration that could
\* still elect a leader holds it, so the next leader has it. A replica the
\* replacement retired on purpose does not count as a fault.
Holders(c) == {n \in Node : ~gone[n] /\ mlen[n] >= 1 /\ c \in StateAt(mlen[n]).w}
Kept == {n \in Node : ~gone[n]}
FaultGone == {n \in Node : gone[n]} \ (IF plan.retired THEN {plan.from} ELSE {})
G01_AckedDurable ==
    Cardinality(FaultGone) <= 1 =>
        \A c \in acked :
            /\ Holders(c) # {}
            /\ \A Q \in SUBSET Kept : Quorum(Cur, Q) => Q \cap Holders(c) # {}

\* OLTP-G02 (direct path sections 5 and 6): a lease read sees every write
\* acknowledged before it.
G02_LeaseReadFresh == ~badRead

\* OLTP-G08 (O17): no zone holds a majority of a committed voter set,
\* including the joint configuration.
G08_ZonePlacement == ZoneSafe(Cur.vo) /\ ZoneSafe(Cur.vn)

\* OLTP-G10 liveness: once faults stop, every client's write is acknowledged
\* and a started replacement retires the old voter.
G10_WritesComplete == <>(acked = Client)
G08_MoveTerminates == plan.on ~> plan.retired
=============================================================================
