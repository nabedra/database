----------------------------- MODULE OltpRanges -----------------------------
(***************************************************************************)
(* OLTP range ownership, leadership, leases, routing, splits, replica      *)
(* moves, membership, worker trust and the Control plane that publishes    *)
(* placement.                                                              *)
(*                                                                         *)
(* Plan: plans/oltp-shard-refactor.md (steps 2 and 6, "Failure and         *)
(* movement"), with the mechanisms it relies on from plans/oltp-engine.md  *)
(* ("Shards, disk layout, and indexes", "Scale-up, scale-down", "Queue,    *)
(* scheduler, and failure semantics") and plans/oltp-direct-path.md        *)
(* (sections 3 to 7 and the 2026-11-19 addendum on splits). Where those    *)
(* plans are silent the spec follows the shipped code at d4963667; each    *)
(* such choice is marked SHIPPED with its source.                          *)
(*                                                                         *)
(* Register: docs/testing/guarantees/oltp.md, OLTP-G01, G02, G06, G08, G10. *)
(*                                                                         *)
(* Abstraction. One Raft group per range. OpenRaft's log replication is    *)
(* abstracted: each group has one committed history `hst[g]` (the applied *)
(* state after each committed entry); each node holds a durable prefix     *)
(* `mlen[n][g]` of it and a durable term (its vote). Elections, commits    *)
(* and heartbeats are atomic quorum steps that reach every eligible voter  *)
(* the leader can talk to, so followers lag only through the modelled      *)
(* crash, wipe, restore and partition faults, and catch up in the next     *)
(* quorum step that reaches them. A membership change takes    *)
(* effect when committed. A leader that commits while missing a committed  *)
(* entry overwrites the history, which the spec records as `lost` instead *)
(* of assuming it away. Raft-internal message loss, duplication and        *)
(* reordering are covered by Raft's term checks and are out of scope.      *)
(* Proposals of an older term are dropped by a new leader (the OltpDirect  *)
(* spec owns an entry that commits after its proposer was deposed); a     *)
(* leader commits its own proposals in any order, so the apply-time        *)
(* checks race every topology entry. The gateway's route refresh is one    *)
(* atomic read (one refresh per key is in flight, so a delayed reply is    *)
(* that read followed by a topology change). A request reaches the leader  *)
(* in one step; its reply can be lost, and a late copy that keeps the      *)
(* group and epochs it was sent with can arrive after the gateway retried, *)
(* refreshed its route or got its answer (delay, duplication, reordering). *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Node,          \* worker node ids
    Spare,         \* the node that starts outside every group (move target)
    Zone,          \* [Node -> zone], availability zones
    Key,           \* primary keys of the one table
    Client,        \* concurrent writers; client c writes key WKey[c] once
    WKey,          \* [Client -> Key]
    G0,            \* the initial range, spanning every key
    Children,      \* the two split children
    ChildSpan,     \* [Children -> SUBSET Key]
    InitSplit,     \* TRUE: the table starts as the two child ranges
    Ops,           \* subset of {"split", "ddl", "fence", "move", "enroll", "lease"}
    Mutant,        \* "none" or a mutant name, see mutants/*.cfg
    Fixes,         \* subset of the candidate fixes applied to the shipped design
    MaxTerm,       \* highest Raft term per group
    MaxCrash,      \* crash-and-restart budget (disk retained)
    MaxWipe,       \* crash-with-disk-loss budget
    MaxStale,      \* restart-from-an-older-disk-copy budget (independent read R-07)
    MaxPart,       \* partition budget (one node isolated, then healed)
    MaxLoss,       \* lost-reply and lost Control message budget
    MaxDup,        \* duplicated-request and duplicated Control message budget
    MaxRefuse,     \* Control pre-append refusals (a Control election), O16
    MaxDrift,      \* bound on |local clock - true time|, in ticks
    MaxDriftSteps, \* clock-drift budget
    MaxTime,       \* true-time horizon, in ticks
    Lease          \* leader lease in ticks (openraft: election_timeout_max)

\* Faults taken so far in the run, over every kind: the pair (or more) that
\* may overlap. The per-kind budgets above still apply.
FaultCount(b) == b.crash + b.wipe + b.stale + b.part + b.loss + b.dup + b.refuse + b.drift

Groups == {G0} \cup Children
NoNode == "none"
InitVoters == Node \ {Spare}

ASSUME Spare \in Node /\ Cardinality(InitVoters) = 3
ASSUME WKey \in [Client -> Key]
ASSUME ChildSpan \in [Children -> SUBSET Key]
ASSUME Zone \in [Node -> STRING]

\* Leader-side lease margin in ticks. SHIPPED: 0 (openraft checks
\* now < quorum_acked + leader_lease, with no allowance for clock rate
\* error). The candidate fix `lease_margin` leaves the true-time span that
\* two drifting clocks can disagree by.
Margin == IF "lease_margin" \in Fixes THEN 2 * MaxDrift ELSE 0

(***************************************************************************)
(* Variables. Durable per node: term, mlen (Raft vote, log and the Data    *)
(* store share one nabedra-kv pool), bkp (an operator copy of the pool).   *)
(* Volatile per node: up, role, lease, vblock. Control and the Catalog are *)
(* linearizable replicated services, abstracted to one state each.         *)
(***************************************************************************)
VARIABLES
    \* Catalog: routing pages and the table's schema epoch
    route,      \* [Key -> Groups], the published owner of each key
    repoch,     \* [Groups -> Nat], published routing epoch (0 = unpublished)
    sepoch,     \* published schema epoch
    \* Control
    placement,  \* [Groups -> SUBSET Node], published GroupRecord voters
    enrolled,   \* [Node -> Nat], committed incarnation (0 = none)
    revoked,    \* SUBSET Node, identities revoked by a replacement Mount
    owner,      \* run-owner term, Authority.owner_term
    ctlOps,     \* operation IDs Control applied (dedup by operation ID)
    guard,      \* Catalog lifecycle guard: "none", "split", "ddl", "move"
    plan,       \* replica move plan, or "none"
    cut,        \* split cut timestamp (0 = no split in progress)
    splitAb,    \* the split's durable decision is "abort"
    \* Data groups
    hst,        \* [Groups -> Seq(GroupState)], state after each committed entry
    pend,       \* [Groups -> SUBSET Entry], proposed, not yet committed
    tso,        \* Timestamp group: last allocated timestamp
    \* Nodes
    up, disk, inc, clk, now, bkp,
    term, mlen, role, lease, vblock,
    \* Gateway and clients
    gw,         \* [Key -> route record], the gateway topology cache
    gwSep,      \* schema epoch the gateway last read from the Catalog
    cst,        \* [Client -> {"pend", "acked"}]
    net,        \* Control messages in flight
    enr,        \* enrollment of Spare: [st, op]
    \* Ghosts
    acked,      \* clients that received success
    elected,    \* <<g, term, node>> for every leader ever elected
    votes,      \* <<g, term, voter, candidate>> for every vote granted
    lost,       \* a committed entry was overwritten
    badRead,    \* a lease read missed an acknowledged write
    staleApply, \* a write applied at a retired schema epoch or fenced authority
    budget      \* fault counters

catVars == <<route, repoch, sepoch>>
ctlVars == <<placement, enrolled, revoked, owner, ctlOps, guard, plan, cut, splitAb>>
dataVars == <<hst, pend, tso>>
hostVars == <<up, disk, inc, clk, now, bkp>>
raftVars == <<term, mlen, role, lease, vblock>>
gwVars == <<gw, gwSep, cst, net, enr>>
ghostVars == <<acked, elected, votes, lost, badRead, staleApply, budget>>
vars == <<catVars, ctlVars, dataVars, hostVars, raftVars, gwVars, ghostVars>>

(***************************************************************************)
(* Log entries, one record shape for every kind.                           *)
(***************************************************************************)
E(kind) == [k |-> kind, c |-> "none", t |-> 0, by |-> NoNode, a |-> 0, r |-> 0,
            s |-> 0, ts |-> 0, vo |-> {}, vn |-> {}, n |-> NoNode, sp |-> {}, img |-> {}]

InitEntry == [E("init") EXCEPT !.vo = InitVoters, !.vn = InitVoters, !.a = 1,
                               !.r = 1, !.s = 1, !.sp = Key]

(***************************************************************************)
(* The Data group state machine. Apply is deterministic on every replica   *)
(* (SHIPPED: store.rs:631-651, direct.rs:677-774). A write applies only    *)
(* if the group is not frozen, its authority and both epochs equal the     *)
(* group config and its key is in the span (direct path plan section 4).   *)
(***************************************************************************)
S0 == [vo |-> {}, vn |-> {}, lrn |-> {}, a |-> 0, r |-> 0, s |-> 0, sp |-> {},
       frozen |-> FALSE, frej |-> FALSE, pub |-> FALSE, w |-> {}, maxts |-> 0]

Max(a, b) == IF a > b THEN a ELSE b
Applied(st, c) == \E p \in st.w : p[1] = c

\* Outcome of a write entry against the state before it, in the apply order
\* of direct.rs:699-723: an exact dedup replay first, then frozen,
\* authority, epochs and key ownership.
WOutcome(st, e) ==
    IF Applied(st, e.c) THEN "done"
    ELSE IF st.frozen /\ Mutant # "no_frozen_check" THEN "fallback"
    ELSE IF st.a # e.a /\ Mutant # "no_authority_check" THEN "stale"
    ELSE IF (st.s # e.s \/ st.r # e.r) /\ Mutant # "no_epoch_check" THEN "stale"
    ELSE IF WKey[e.c] \notin st.sp THEN "stale"
    ELSE "applied"

Apply(st, e) ==
    CASE e.k = "init" ->
            [st EXCEPT !.vo = e.vo, !.vn = e.vn, !.a = e.a, !.r = e.r, !.s = e.s,
                       !.sp = e.sp, !.w = e.img,
                       !.maxts = IF e.img = {} THEN 0 ELSE e.ts]
      [] e.k = "w" ->
            IF WOutcome(st, e) = "applied"
            THEN [st EXCEPT !.w = @ \cup {<<e.c, e.ts>>}, !.maxts = Max(@, e.ts)]
            ELSE st
      [] e.k = "fence" ->       \* ParticipantCommand::Fence, store.rs:539-629
            IF e.a = st.a + 1 THEN [st EXCEPT !.a = e.a] ELSE st
      [] e.k = "sfence" ->      \* DirectSchema Fence, direct.rs:1246-1287
            IF e.s = st.s + 1 THEN [st EXCEPT !.s = e.s] ELSE st
      [] e.k = "freeze" ->      \* SplitFreeze, lineage.rs:442-530
            IF st.maxts > e.ts /\ Mutant # "no_cut_check"
            THEN [st EXCEPT !.frej = TRUE]          \* DirectCommitAboveCut
            ELSE [st EXCEPT !.frozen = TRUE, !.frej = FALSE]
      [] e.k = "unfreeze" ->    \* SplitFinishSource abort, lineage.rs:587
            [st EXCEPT !.frozen = FALSE]
      [] e.k = "finish" -> [st EXCEPT !.pub = TRUE] \* SplitFinishSource, published_at
      [] e.k = "learner" -> [st EXCEPT !.lrn = @ \cup {e.n}]
      [] e.k = "joint" ->       \* change_membership_if with LastMembershipLogId
            IF st.vo = st.vn /\ (st.vo = e.vo \/ Mutant = "no_config_cas")
            THEN [st EXCEPT !.vn = e.vn] ELSE st
      [] e.k = "final" ->       \* retain_removed = true (placement.rs:722-726)
            IF st.vn = e.vn /\ st.vo # st.vn
            THEN [st EXCEPT !.lrn = (@ \cup (st.vo \ e.vn)) \ e.vn, !.vo = e.vn]
            ELSE st
      [] e.k = "replace" ->     \* mutant no_joint: a one-step voter replacement
            IF st.vo = st.vn /\ st.vo = e.vo
            THEN [st EXCEPT !.lrn = (@ \cup (e.vo \ e.vn)) \ e.vn, !.vo = e.vn, !.vn = e.vn]
            ELSE st
      [] OTHER -> st

\* The state a node holding the first len committed entries of g applied.
StateAt(g, len) == IF len = 0 THEN S0 ELSE hst[g][len]
Cur(g) == StateAt(g, Len(hst[g]))

Exists(g) == Len(hst[g]) > 0
IsMaj(Q, V) == V # {} /\ 2 * Cardinality(Q \cap V) > Cardinality(V)
\* Joint consensus: a majority of both configurations.
Quorum(st, Q) == IsMaj(Q, st.vo) /\ IsMaj(Q, st.vn)
Voters(st) == st.vo \cup st.vn

\* No zone holds a majority of any voter set (failure-domain placement).
ZoneSafe(V) == \A z \in {Zone[n] : n \in V} :
                   2 * Cardinality({n \in V : Zone[n] = z}) < Cardinality(V)

(***************************************************************************)
(* Trust. A worker takes part only under its committed identity. A node    *)
(* whose pool was wiped cannot resume its identity: inspect_identity       *)
(* refuses a missing identity file (registered.rs:772-839), and RV-REP-3's *)
(* CheckMount refuses a mounted replacement. Peers refuse revoked or       *)
(* superseded identities (peer_directory.rs:782-876).                      *)
(***************************************************************************)
Trusted(n) ==
    /\ inc[n] # 0
    /\ (inc[n] = enrolled[n] \/ Mutant = "no_identity_check")
    /\ (n \notin revoked \/ Mutant = "no_revocation_check")

CanTalk(a, b) == a = b \/ budget.iso \notin {a, b}

\* Nodes that faults may hit: the spare only in profiles that use it.
FaultNodes == IF "move" \in Ops THEN Node ELSE Node \ {Spare}
Live(n) == up[n] /\ Trusted(n)

\* Lease time. Each node reads a monotonic clock that drifts from true time
\* by at most MaxDrift ticks.
LeaseOn == "lease" \in Ops
\* openraft is_lease_valid: now < quorum_acked_send_time + leader_lease
\* (proposer/leader.rs:312-319), minus the candidate margin.
LeaseValid(n, g) == LeaseOn /\ clk[n] + Margin < lease[n][g]
\* A follower refuses votes while now <= last heartbeat + lease
\* (utime.rs:147, engine_impl.rs:419-445, is_expired(now, 0)).
VoteFree(q, g) == ~LeaseOn \/ clk[q] > vblock[q][g]

\* The leader of the highest term among live leaders: a NotLeader hint.
LeaderHint(g) ==
    LET ls == {n \in Node : up[n] /\ role[n][g] = "L"}
    IN IF ls = {} THEN NoNode
       ELSE CHOOSE n \in ls : \A m \in ls : term[m][g] <= term[n][g]

\* A route the gateway caches: from one Catalog read and one Control read.
Fresh(k) == [g |-> route[k], r |-> repoch[route[k]], vs |-> placement[route[k]], hint |-> NoNode]

(***************************************************************************)
(* Initial state: three enrolled voters host the initial range(s); the     *)
(* spare is outside every group.                                           *)
(***************************************************************************)
InitGroupsSet == IF InitSplit THEN Children ELSE {G0}
InitRoute == [k \in Key |-> IF InitSplit THEN CHOOSE h \in Children : k \in ChildSpan[h] ELSE G0]
InitHst(g) ==
    IF g \notin InitGroupsSet THEN <<>>
    ELSE <<Apply(S0, IF g = G0 THEN InitEntry
                     ELSE [InitEntry EXCEPT !.sp = ChildSpan[g]])>>

Init ==
    /\ route = InitRoute
    /\ repoch = [g \in Groups |-> IF g \in InitGroupsSet THEN 1 ELSE 0]
    /\ sepoch = 1
    /\ placement = [g \in Groups |-> IF g \in InitGroupsSet THEN InitVoters ELSE {}]
    /\ enrolled = [n \in Node |-> IF n = Spare /\ "enroll" \in Ops THEN 0 ELSE 1]
    /\ revoked = {}
    /\ owner = 1
    /\ ctlOps = {}
    /\ guard = "none"
    /\ plan = "none"
    /\ cut = 0
    /\ splitAb = FALSE
    /\ hst = [g \in Groups |-> InitHst(g)]
    /\ pend = [g \in Groups |-> {}]
    /\ tso = 0
    /\ up = [n \in Node |-> n # Spare \/ "enroll" \notin Ops]
    /\ disk = [n \in Node |-> TRUE]
    /\ inc = [n \in Node |-> IF n = Spare /\ "enroll" \in Ops THEN 0 ELSE 1]
    /\ clk = [n \in Node |-> 0]
    /\ now = 0
    /\ bkp = [n \in Node |-> NoNode]
    /\ term = [n \in Node |-> [g \in Groups |-> 0]]
    /\ mlen = [n \in Node |-> [g \in Groups |->
                   IF g \in InitGroupsSet /\ n \in InitVoters THEN 1 ELSE 0]]
    /\ role = [n \in Node |-> [g \in Groups |-> "F"]]
    /\ lease = [n \in Node |-> [g \in Groups |-> 0]]
    /\ vblock = [n \in Node |-> [g \in Groups |-> 0]]
    /\ gw = [k \in Key |-> Fresh(k)]
    /\ gwSep = 1
    /\ cst = [c \in Client |-> "pend"]
    /\ net = {}
    /\ enr = [st |-> IF "enroll" \in Ops THEN "start" ELSE "done", op |-> 1]
    /\ acked = {}
    /\ elected = {}
    /\ votes = {}
    /\ lost = FALSE
    /\ badRead = FALSE
    /\ staleApply = FALSE
    /\ budget = [crash |-> 0, wipe |-> 0, stale |-> 0, part |-> 0, iso |-> NoNode,
                 loss |-> 0, dup |-> 0, refuse |-> 0, drift |-> 0, splitTry |-> 0]

(***************************************************************************)
(* Raft: elections, the first entry of a term, commits, catch-up and       *)
(* heartbeats. Engine plan "ACID" (local durability), "Queue, scheduler,   *)
(* and failure semantics" (only authoritative quorums decide writes),      *)
(* direct path section 5 rule 3. OLTP-G01, G08, G10.                       *)
(***************************************************************************)

\* Voters that grant n's vote request for term t (engine_impl.rs:400-445):
\* reachable, live, of a lower term, not more up to date, and outside a
\* lease they gave another leader.
Granting(n, g, t) ==
    LET st == StateAt(g, mlen[n][g]) IN
    {q \in Voters(st) \cup {n} :
        /\ Live(q) /\ CanTalk(n, q)
        /\ term[q][g] < t
        /\ mlen[q][g] <= mlen[n][g]
        /\ (q = n \/ VoteFree(q, g))}

Elect(n, g) ==
    LET st == StateAt(g, mlen[n][g])
        t == term[n][g] + 1
        Q == Granting(n, g, t)
    IN
    /\ Exists(g) /\ Live(n) /\ n \in Voters(st) /\ t <= MaxTerm
    /\ ~LeaseValid(n, g)
    /\ n \in Q /\ Quorum(st, Q)
    /\ term' = [m \in Node |-> [h \in Groups |->
                    IF m \in Q /\ h = g THEN t ELSE term[m][h]]]
    /\ role' = [m \in Node |-> [h \in Groups |->
                    IF h = g /\ m = n THEN "C"
                    ELSE IF h = g /\ m \in Q THEN "F" ELSE role[m][h]]]
    /\ votes' = votes \cup {<<g, t, q, n>> : q \in Q}
    /\ elected' = elected \cup {<<g, t, n>>}
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, mlen, lease, vblock, gwVars,
                   acked, lost, badRead, staleApply, budget>>

\* Nodes that accept an append from leader n of g in its term: its voters
\* and learners that are live, reachable and not of a higher term.
Accepting(n, g) ==
    LET st == StateAt(g, mlen[n][g]) IN
    {q \in Voters(st) \cup st.lrn \cup {n} :
        Live(q) /\ CanTalk(n, q) /\ term[q][g] <= term[n][g]}

QuorumOK(n, g) == Quorum(StateAt(g, mlen[n][g]), Accepting(n, g))

\* Every accepting node adopts the term and the new committed prefix;
\* leaders of a lower term among them step down.
Adopt(n, g, newLen, newRole) ==
    LET Q == Accepting(n, g) IN
    /\ mlen' = [m \in Node |-> [h \in Groups |->
                  IF h = g /\ m \in Q THEN newLen ELSE mlen[m][h]]]
    /\ term' = [m \in Node |-> [h \in Groups |->
                  IF m \in Q /\ h = g THEN term[n][g] ELSE term[m][h]]]
    /\ role' = [m \in Node |-> [h \in Groups |->
                  IF h = g /\ m = n THEN newRole
                  ELSE IF h = g /\ m \in Q THEN "F" ELSE role[m][h]]]

\* Commit e at leader n: the committed history becomes n's prefix plus e.
\* A leader missing a committed entry overwrites it: `lost`.
DoCommit(n, g, e, newRole) ==
    LET st == StateAt(g, mlen[n][g])
        newHst == Append(SubSeq(hst[g], 1, mlen[n][g]), Apply(st, e))
    IN /\ hst' = [hst EXCEPT ![g] = newHst]
       /\ lost' = (lost \/ mlen[n][g] < Len(hst[g]))
       /\ Adopt(n, g, Len(newHst), newRole)

\* A write is stale when it applies below the Catalog's schema epoch or
\* under an authority its group already fenced.
StaleWrite(st, e) ==
    e.k = "w" /\ WOutcome(st, e) = "applied" /\ (e.s < sepoch \/ e.a < st.a)

\* The blank entry of a new term. Until it commits the leader serves
\* nothing (RangeClock rule 3, clock.rs:10-17, 254-269).
CommitBlank(n, g) ==
    /\ up[n] /\ role[n][g] = "C" /\ QuorumOK(n, g)
    /\ hst' = [hst EXCEPT ![g] = SubSeq(@, 1, mlen[n][g])]
    /\ lost' = (lost \/ mlen[n][g] < Len(hst[g]))
    /\ Adopt(n, g, mlen[n][g], "L")
    /\ pend' = [pend EXCEPT ![g] = {e \in @ : e.t >= term[n][g]}]
    /\ UNCHANGED <<catVars, ctlVars, tso, hostVars, lease, vblock, gwVars,
                   acked, elected, votes, badRead, staleApply, budget>>

\* Leader n commits its own proposal and, for a write, replies with the
\* apply outcome (hosted.rs:1022-1028, executor.rs:1098-1116). The reply can
\* be lost (the gateway retries the same request ID).
Commit(n, g, e, delivered) ==
    LET st == StateAt(g, mlen[n][g])
        out == WOutcome(st, e)
    IN
    /\ up[n] /\ role[n][g] = "L" /\ e \in pend[g] /\ e.t = term[n][g] /\ e.by = n
    /\ QuorumOK(n, g)
    /\ DoCommit(n, g, e, "L")
    /\ pend' = [pend EXCEPT ![g] = @ \ {e}]
    /\ staleApply' = (staleApply \/ StaleWrite(st, e))
    /\ IF e.k # "w" \/ out \notin {"done", "applied"} \/ ~delivered
       THEN UNCHANGED <<acked, cst>>
       ELSE /\ acked' = acked \cup {e.c}
            /\ cst' = [cst EXCEPT ![e.c] = "acked"]
    \* A stale outcome makes the gateway refresh; frozen means retry later.
    /\ gw' = IF e.k = "w" /\ out = "stale"
             THEN [gw EXCEPT ![WKey[e.c]] = Fresh(WKey[e.c])] ELSE gw
    /\ gwSep' = IF e.k = "w" /\ out = "stale" THEN sepoch ELSE gwSep
    /\ budget' = IF ~delivered THEN [budget EXCEPT !.loss = @ + 1] ELSE budget
    /\ UNCHANGED <<catVars, ctlVars, tso, hostVars, lease, vblock, net, enr, elected,
                   votes, badRead>>

\* A heartbeat round acknowledged by a quorum at send time clk[n]
\* (leader_handler: last_quorum_acked_time). Followers renew their lease
\* (vote_handler::update_vote with a committed vote).
Heartbeat(n, g) ==
    LET Q == Accepting(n, g) IN
    /\ LeaseOn /\ up[n] /\ role[n][g] = "L" /\ QuorumOK(n, g)
    /\ \A q \in Q : mlen[q][g] = mlen[n][g] /\ term[q][g] = term[n][g]
    /\ vblock' = [m \in Node |-> [h \in Groups |->
                    IF h = g /\ m \in Q \ {n} THEN clk[m] + Lease ELSE vblock[m][h]]]
    /\ lease' = [lease EXCEPT ![n][g] = clk[n] + Lease]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, term, mlen, role, gwVars, ghostVars>>

(***************************************************************************)
(* Gateway, direct writes and lease reads. Direct path plan sections 3, 4  *)
(* and 6. SHIPPED: application/direct.rs:234-406, 1000-1130,               *)
(* executor.rs:383-431, 1200-1294, handle/direct.rs:312-324. OLTP-G06, G02. *)
(***************************************************************************)

\* An open call: the proposal waits at a live leader that still leads in its
\* term. A duplicate attempt queues behind the key latch (executor step 2).
Waiting(c) ==
    \E g \in Groups : \E e \in pend[g] :
        e.k = "w" /\ e.c = c /\ up[e.by] /\ role[e.by][g] = "L" /\ term[e.by][g] = e.t

\* The leader n proposes client c's write with the authority and epochs of
\* its own local config (executor.rs:1016-1023).
ProposeWrite(n, g, c, st) ==
    /\ tso' = tso + 1
    /\ pend' = [pend EXCEPT ![g] = @ \cup
                  {[E("w") EXCEPT !.c = c, !.t = term[n][g], !.by = n,
                                  !.a = st.a, !.r = st.r, !.s = st.s, !.ts = tso + 1]}]

\* The gateway sends c's request with its cached route to node n. The node
\* answers NotLeader (with a hint), StaleRoute, Fallback, Done from its dedup
\* record, or proposes. A stale reply refreshes the route; a hint outside the
\* cached voters refreshes it too. `late` leaves a second copy in flight.
HandleDirect(n, c, late) ==
    LET r == gw[WKey[c]]
        g == r.g
        st == StateAt(g, mlen[n][g])
        k == WKey[c]
        hint == LeaderHint(g)
        Check == Mutant # "no_leader_epoch_check"
        wrongEpoch == r.r # st.r \/ gwSep # st.s \/ k \notin st.sp \/ st.pub
    IN
    /\ cst[c] = "pend" /\ ~Waiting(c)
    /\ n \in (IF r.hint # NoNode THEN {r.hint} ELSE r.vs) /\ up[n] /\ budget.iso # n
    /\ late => budget.dup < MaxDup
    /\ IF role[n][g] # "L" \/ ~Trusted(n)
       THEN /\ gw' = IF hint \in r.vs THEN [gw EXCEPT ![k].hint = hint]
                     ELSE IF hint = NoNode THEN [gw EXCEPT ![k].hint = NoNode]
                     ELSE [gw EXCEPT ![k] = Fresh(k)]
            /\ gwSep' = IF hint \in r.vs \/ hint = NoNode THEN gwSep ELSE sepoch
            /\ UNCHANGED <<acked, cst, pend, tso>>
       ELSE IF Check /\ wrongEpoch
       THEN /\ gw' = [gw EXCEPT ![k] = Fresh(k)]
            /\ gwSep' = sepoch
            /\ UNCHANGED <<acked, cst, pend, tso>>
       ELSE IF Check /\ st.frozen
       THEN UNCHANGED <<gw, gwSep, acked, cst, pend, tso>>
       ELSE IF Applied(st, c)
       THEN /\ acked' = acked \cup {c}
            /\ cst' = [cst EXCEPT ![c] = "acked"]
            /\ UNCHANGED <<gw, gwSep, pend, tso>>
       ELSE /\ ProposeWrite(n, g, c, st)
            /\ UNCHANGED <<gw, gwSep, acked, cst>>
    /\ budget' = IF late THEN [budget EXCEPT !.dup = @ + 1] ELSE budget
    /\ net' = IF late
              THEN net \cup {[m |-> "late", d |-> 1, c |-> c, g |-> g, r |-> r.r, s |-> gwSep]}
              ELSE net
    /\ UNCHANGED <<catVars, ctlVars, hst, hostVars, raftVars, enr, elected, votes,
                   lost, badRead, staleApply>>

\* The late copy reaches some node that still thinks it leads the group the
\* request was sent to. The gateway no longer waits for its reply, so only a
\* proposal has an effect; a refusal changes nothing.
HandleLate(n, msg) ==
    LET g == msg.g
        st == StateAt(g, mlen[n][g])
        k == WKey[msg.c]
        Check == Mutant # "no_leader_epoch_check"
        ok == /\ role[n][g] = "L" /\ Trusted(n)
              /\ Check => /\ msg.r = st.r /\ msg.s = st.s /\ k \in st.sp
                          /\ ~st.pub /\ ~st.frozen
              /\ ~Applied(st, msg.c)
    IN
    /\ msg \in net /\ msg.m = "late" /\ up[n] /\ budget.iso # n
    /\ net' = net \ {msg}
    /\ IF ok THEN ProposeWrite(n, g, msg.c, st) ELSE UNCHANGED <<pend, tso>>
    /\ UNCHANGED <<catVars, ctlVars, hst, hostVars, raftVars, gw, gwSep, cst, enr, ghostVars>>

\* A direct table.get: ensure_linearizable(LeaseRead), then a read of the
\* leader's applied state (hosted.rs:1034-1058). The ghost check: the read
\* sees every write acknowledged before it.
LeaseRead(n, g, k) ==
    LET st == StateAt(g, mlen[n][g]) IN
    /\ LeaseOn /\ up[n] /\ role[n][g] = "L" /\ LeaseValid(n, g)
    \* validate_read refuses a frozen source (SplitPublicationPending) and a
    \* published one (lineage.rs:413-440).
    /\ k \in st.sp /\ ~st.pub /\ (~st.frozen \/ Mutant = "read_frozen_source")
    /\ badRead' = (badRead \/ \E c \in acked : WKey[c] = k /\ ~Applied(st, c))
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gwVars, acked, elected,
                   votes, lost, staleApply, budget>>

(***************************************************************************)
(* Split: engine plan "Splits and moves preserve table IDs ... Fence the   *)
(* old epoch and publish routing only when the new ownership is            *)
(* recoverable"; direct path addendum "Splits under write load". SHIPPED   *)
(* sequence (worker_runtime/split.rs:90-366): the Catalog lifecycle guard, *)
(* Cut in Timestamps, SplitFreeze in the source (rejected when a direct    *)
(* commit is above the cut), both children created on the source voters   *)
(* with routing epoch + 1 and the image at the cut, one Catalog            *)
(* publication, SplitFinishSource. Publish and abort are the two outcomes  *)
(* of one durable decision. The plan's DirectPause barrier is not proposed *)
(* by shipped code (drift).                                                *)
(***************************************************************************)
SplitActive == cut # 0
ChildrenExist == \A h \in Children : Exists(h)
Published == route # [k \in Key |-> G0]

SplitBegin ==
    /\ "split" \in Ops /\ guard = "none" /\ ~SplitActive /\ ~ChildrenExist
    /\ budget.splitTry < 2
    /\ guard' = IF Mutant = "split_without_guard" THEN guard ELSE "split"
    /\ cut' = tso + 1
    /\ tso' = tso + 1
    /\ budget' = [budget EXCEPT !.splitTry = @ + 1]
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, plan, splitAb, hst,
                   pend, hostVars, raftVars, gwVars, acked, elected, votes, lost, badRead,
                   staleApply>>

\* The source leader proposes SplitFreeze at the cut.
SplitFreeze(n) ==
    /\ SplitActive /\ ~splitAb /\ up[n] /\ role[n][G0] = "L"
    /\ ~\E e \in pend[G0] : e.k = "freeze"
    /\ ~Cur(G0).frozen /\ ~Cur(G0).frej
    /\ pend' = [pend EXCEPT ![G0] = @ \cup
                 {[E("freeze") EXCEPT !.t = term[n][G0], !.by = n, !.ts = cut]}]
    /\ UNCHANGED <<catVars, ctlVars, hst, tso, hostVars, raftVars, gwVars, ghostVars>>

\* A rejected freeze (a direct commit above the cut) ends the attempt; the
\* coordinator starts over with a new cut.
SplitRetry ==
    /\ SplitActive /\ ~splitAb /\ Cur(G0).frej /\ ~Cur(G0).frozen
    /\ cut' = 0
    /\ guard' = IF guard = "split" THEN "none" ELSE guard
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, plan, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

\* The source leader hosts both children on the committed source voters it
\* reaches, applies each child's initial Fence (routing epoch + 1, its span)
\* and imports the image at the cut (endpoint.rs:756-985). Register the
\* children with Control. The child's first leader is the source leader.
SplitStage(n) ==
    LET src == Cur(G0)
        hosts == {v \in src.vn : Live(v) /\ CanTalk(n, v)}
        Img(h) == {p \in src.w : WKey[p[1]] \in ChildSpan[h] /\
                                 (p[2] <= cut \/ Mutant = "image_after_cut")}
        Child(h) == [InitEntry EXCEPT !.vo = src.vn, !.vn = src.vn, !.a = src.a,
                       !.r = src.r + 1,
                       !.s = IF Mutant = "child_stale_schema" THEN 1 ELSE src.s,
                       !.sp = ChildSpan[h], !.img = Img(h), !.ts = cut]
    IN
    /\ SplitActive /\ ~splitAb /\ (src.frozen \/ Mutant = "stage_before_freeze")
    /\ ~src.pub /\ src.vo = src.vn /\ ~ChildrenExist
    /\ up[n] /\ role[n][G0] = "L" /\ n \in hosts
    /\ IsMaj(hosts, src.vn)
    /\ hst' = [h \in Groups |-> IF h \in Children THEN <<Apply(S0, Child(h))>> ELSE hst[h]]
    /\ mlen' = [m \in Node |-> [h \in Groups |->
                  IF h \in Children /\ m \in hosts THEN 1 ELSE mlen[m][h]]]
    /\ term' = [m \in Node |-> [h \in Groups |->
                  IF h \in Children /\ m \in hosts THEN 1 ELSE term[m][h]]]
    /\ role' = [m \in Node |-> [h \in Groups |->
                  IF h \in Children /\ m = n THEN "L" ELSE role[m][h]]]
    /\ placement' = [h \in Groups |-> IF h \in Children THEN src.vn ELSE placement[h]]
    /\ UNCHANGED <<catVars, enrolled, revoked, owner, ctlOps, guard, plan, cut, splitAb, pend,
                   tso, hostVars, lease, vblock, gwVars, ghostVars>>

\* One Catalog transaction publishes both children (planner/split.rs:300-387).
SplitPublish ==
    /\ SplitActive /\ ~splitAb /\ ~Published
    /\ ChildrenExist \/ Mutant = "publish_before_children"
    /\ route' = [k \in Key |-> CHOOSE h \in Children : k \in ChildSpan[h]]
    /\ repoch' = [h \in Groups |-> IF h \in Children THEN repoch[G0] + 1 ELSE repoch[h]]
    /\ UNCHANGED <<sepoch, ctlVars, dataVars, hostVars, raftVars, gwVars, ghostVars>>

\* The coordinator's durable decision is "abort" (before publication).
SplitAbort ==
    /\ SplitActive /\ ~splitAb /\ (~Published \/ Mutant = "abort_after_publish")
    /\ splitAb' = TRUE
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, guard, plan, cut,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

\* SplitFinishSource: published_at on commit, unfreeze on abort.
SplitFinish(n) ==
    /\ SplitActive /\ up[n] /\ role[n][G0] = "L"
    /\ ~\E e \in pend[G0] : e.k \in {"finish", "unfreeze"}
    /\ \/ /\ Published /\ ~splitAb /\ ~Cur(G0).pub
          /\ pend' = [pend EXCEPT ![G0] = @ \cup {[E("finish") EXCEPT !.t = term[n][G0], !.by = n]}]
       \/ /\ splitAb /\ Cur(G0).frozen
          /\ pend' = [pend EXCEPT ![G0] = @ \cup {[E("unfreeze") EXCEPT !.t = term[n][G0], !.by = n]}]
    /\ UNCHANGED <<catVars, ctlVars, hst, tso, hostVars, raftVars, gwVars, ghostVars>>

\* Release the lifecycle guard and Close in Timestamps.
SplitDone ==
    /\ SplitActive
    /\ IF splitAb THEN ~Cur(G0).frozen /\ ~\E e \in pend[G0] : e.k = "freeze"
                  ELSE Cur(G0).pub
    /\ cut' = 0
    /\ splitAb' = FALSE
    /\ guard' = IF guard = "split" THEN "none" ELSE guard
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, plan, dataVars,
                   hostVars, raftVars, gwVars, ghostVars>>

(***************************************************************************)
(* Schema fence: direct path plan section 7. The DDL coordinator commits   *)
(* the new schema epoch to every Data group of the table, then the        *)
(* Catalog publishes it. DDL and splits both hold the lifecycle guard.     *)
(***************************************************************************)
RoutedGroups == {route[k] : k \in Key}

DdlBegin ==
    /\ "ddl" \in Ops /\ guard = "none" /\ sepoch = 1
    /\ guard' = "ddl"
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, plan, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

DdlFenceGroup(n, g) ==
    /\ guard = "ddl" /\ g \in RoutedGroups /\ up[n] /\ role[n][g] = "L"
    /\ Cur(g).s = sepoch
    /\ ~\E e \in pend[g] : e.k = "sfence"
    /\ pend' = [pend EXCEPT ![g] = @ \cup
                 {[E("sfence") EXCEPT !.t = term[n][g], !.by = n, !.s = sepoch + 1]}]
    /\ UNCHANGED <<catVars, ctlVars, hst, tso, hostVars, raftVars, gwVars, ghostVars>>

DdlPublish ==
    /\ guard = "ddl" /\ sepoch = 1
    /\ (\A g \in RoutedGroups : Cur(g).s = sepoch + 1) \/ Mutant = "ddl_publish_first"
    /\ sepoch' = sepoch + 1
    /\ guard' = "none"
    /\ UNCHANGED <<route, repoch, placement, enrolled, revoked, owner, ctlOps, plan, cut,
                   splitAb, dataVars, hostVars, raftVars, gwVars, ghostVars>>

(***************************************************************************)
(* Run-owner fence: engine plan "Every decision transition checks its      *)
(* group's fence". Control bumps the owner term, then each group commits   *)
(* a Fence (authority_fence.rs:34-124).                                    *)
(***************************************************************************)
OwnerFence ==
    /\ "fence" \in Ops /\ owner = 1
    /\ owner' = owner + 1
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, ctlOps, guard, plan, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

FenceGroup(n, g) ==
    /\ Exists(g) /\ Cur(g).a < owner /\ up[n] /\ role[n][g] = "L"
    /\ ~\E e \in pend[g] : e.k = "fence"
    /\ pend' = [pend EXCEPT ![g] = @ \cup
                 {[E("fence") EXCEPT !.t = term[n][g], !.by = n, !.a = Cur(g).a + 1]}]
    /\ UNCHANGED <<catVars, ctlVars, hst, tso, hostVars, raftVars, gwVars, ghostVars>>

(***************************************************************************)
(* Enrollment of a new worker by a Control proposal with an operation ID   *)
(* (O16, PR #104: bootstrap/registered/initial.rs). A pre-append refusal   *)
(* or a lost reply is retried with the same ID; Control deduplicates by    *)
(* ID and replays the stored reply.                                        *)
(***************************************************************************)
EnrollSend ==
    /\ enr.st = "start"
    /\ net' = net \cup {[m |-> "creq", d |-> 0, op |-> enr.op]}
    /\ enr' = [enr EXCEPT !.st = "wait"]
    /\ up' = [up EXCEPT ![Spare] = TRUE]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, disk, inc, clk, now, bkp, raftVars,
                   gw, gwSep, cst, ghostVars>>

CtlEnroll(msg) ==
    LET dup == msg.op \in ctlOps /\ Mutant # "control_no_dedup" IN
    /\ msg \in net /\ msg.m = "creq"
    /\ IF dup
       THEN UNCHANGED <<enrolled, ctlOps>>
       ELSE /\ enrolled' = [enrolled EXCEPT ![Spare] = @ + 1]
            /\ ctlOps' = ctlOps \cup {msg.op}
    /\ net' = (net \ {msg}) \cup
              {[m |-> "crsp", d |-> 0, op |-> msg.op, ok |-> TRUE,
                inc |-> IF dup THEN enrolled[Spare] ELSE enrolled[Spare] + 1]}
    /\ UNCHANGED <<catVars, placement, revoked, owner, guard, plan, cut, splitAb, dataVars,
                   hostVars, raftVars, gw, gwSep, cst, enr, ghostVars>>

\* A Control election refuses the proposal before append: not applied.
CtlRefuse(msg) ==
    /\ msg \in net /\ msg.m = "creq" /\ budget.refuse < MaxRefuse
    /\ net' = (net \ {msg}) \cup {[m |-> "crsp", d |-> 0, op |-> msg.op, ok |-> FALSE, inc |-> 0]}
    /\ budget' = [budget EXCEPT !.refuse = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gw, gwSep, cst, enr,
                   acked, elected, votes, lost, badRead, staleApply>>

EnrollReply(msg) ==
    /\ msg \in net /\ msg.m = "crsp"
    /\ net' = net \ {msg}
    /\ IF enr.st # "wait"
       THEN UNCHANGED <<enr, inc>>
       ELSE IF msg.ok
       THEN /\ enr' = [enr EXCEPT !.st = "done"]
            /\ inc' = [inc EXCEPT ![Spare] = msg.inc]
       ELSE /\ enr' = IF Mutant = "enroll_abort_on_refusal"
                      THEN [enr EXCEPT !.st = "failed"] ELSE [enr EXCEPT !.st = "start"]
            /\ UNCHANGED inc
    /\ UNCHANGED <<catVars, ctlVars, dataVars, up, disk, clk, now, bkp, raftVars,
                   gw, gwSep, cst, ghostVars>>

\* The startup retry timer: an unanswered proposal is resent with its ID.
EnrollTimeout ==
    /\ enr.st = "wait"
    /\ ~\E msg \in net : msg.m \in {"creq", "crsp"}
    /\ enr' = [enr EXCEPT !.st = "start"]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gw, gwSep, cst, net,
                   ghostVars>>

(***************************************************************************)
(* Replica move and damage replacement. Engine plan "Scale-up": add a      *)
(* learner, catch up, promote through safe membership changes, then        *)
(* remove the old voter; shard refactor "Failure and movement". SHIPPED    *)
(* (machine/placement.rs, manager/lifecycle.rs): Control begin and arm,   *)
(* Data add_learner, joint change_membership_if, the final configuration   *)
(* keeping the removed voter as a learner, Control publish only for the   *)
(* exact plan (REP2 fix, lifecycle.rs:239-280), then retire. A target in   *)
(* another zone must keep the zone rule (placement.rs:415-480).            *)
(***************************************************************************)
MoveBegin(g, from) ==
    LET st == Cur(g)
        next == (st.vn \ {from}) \cup {Spare}
    IN
    /\ "move" \in Ops /\ guard = "none" /\ plan = "none" /\ Exists(g) /\ ~SplitActive
    /\ st.vo = st.vn /\ from \in st.vn /\ Spare \notin Voters(st)
    /\ Trusted(Spare) /\ up[Spare]
    /\ placement[g] = st.vn
    /\ ZoneSafe(next) \/ Mutant = "no_zone_check"
    /\ guard' = IF Mutant = "split_during_move" THEN guard ELSE "move"
    /\ plan' = [g |-> g, from |-> from, to |-> Spare, old |-> st.vn, new |-> next]
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

\* The Data group leader proposes the next membership step of the plan.
MovePropose(n, kind) ==
    LET g == plan.g
        cur == Cur(g)
        other == CHOOSE v \in plan.old \ {plan.from} : TRUE
        e == CASE kind = "learner" -> [E("learner") EXCEPT !.n = plan.to]
               [] kind = "joint" ->
                    [E("joint") EXCEPT !.vo = plan.old,
                       !.vn = IF Mutant = "rep2_any_removal"
                              THEN (plan.old \ {other}) \cup {plan.to}
                              ELSE plan.new]
               [] kind = "final" -> [E("final") EXCEPT !.vn = cur.vn]
               [] kind = "nojoint" -> [E("replace") EXCEPT !.vo = plan.old, !.vn = plan.new]
    IN
    /\ plan # "none" /\ up[n] /\ role[n][g] = "L"
    /\ ~\E x \in pend[g] : x.k \in {"learner", "joint", "final", "replace"}
    /\ CASE kind = "learner" -> plan.to \notin cur.lrn /\ cur.vn = plan.old /\ cur.vo = plan.old
         [] kind = "joint" -> plan.to \in cur.lrn /\ cur.vo = plan.old /\ cur.vn = plan.old
                               /\ Mutant # "no_joint"
         [] kind = "final" -> cur.vo # cur.vn
         [] kind = "nojoint" -> Mutant = "no_joint" /\ plan.to \in cur.lrn
                                 /\ cur.vo = plan.old /\ cur.vn = plan.old
    /\ pend' = [pend EXCEPT ![g] = @ \cup {[e EXCEPT !.t = term[n][g], !.by = n]}]
    /\ UNCHANGED <<catVars, ctlVars, hst, tso, hostVars, raftVars, gwVars, ghostVars>>

\* Control publish requires the Data group to have applied the exact
\* planned configuration (placement.rs:222-332).
MovePublish ==
    LET st == Cur(plan.g) IN
    /\ plan # "none" /\ st.vo = st.vn /\ placement[plan.g] # plan.new
    /\ IF Mutant = "rep2_any_removal"
       THEN plan.to \in st.vn /\ Cardinality(st.vn) = 3 /\ st.vn # plan.old
       ELSE st.vn = plan.new
    /\ placement' = [placement EXCEPT ![plan.g] = plan.new]
    /\ UNCHANGED <<catVars, enrolled, revoked, owner, ctlOps, guard, plan, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

\* Retire the removed replica and release the claim (retirement.rs:143-232).
\* Its pool is released: the node leaves the group for good.
MoveRetire ==
    /\ plan # "none" /\ placement[plan.g] = plan.new
    /\ plan' = "none"
    /\ guard' = IF guard = "move" THEN "none" ELSE guard
    /\ UNCHANGED <<catVars, placement, enrolled, revoked, owner, ctlOps, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

(***************************************************************************)
(* Faults. Each is budgeted. A crash keeps the disk; a wipe loses it; a    *)
(* restore replaces the disk by an older operator copy of the same pool.   *)
(***************************************************************************)
Crash(n) ==
    /\ up[n] /\ budget.crash < MaxCrash
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ role' = [role EXCEPT ![n] = [h \in Groups |-> "F"]]
    /\ lease' = [lease EXCEPT ![n] = [h \in Groups |-> 0]]
    /\ budget' = [budget EXCEPT !.crash = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, disk, inc, clk, now, bkp, term, mlen, vblock,
                   gwVars, acked, elected, votes, lost, badRead, staleApply>>

\* Restart with the disk: the persisted vote and log reopen
\* (data_factory/restart.rs). openraft renews the follower lease at startup
\* for a committed vote (engine_impl.rs:180-205).
Restart(n) ==
    /\ ~up[n] /\ disk[n] /\ inc[n] # 0
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ vblock' = IF LeaseOn /\ Mutant # "restart_forgets_lease"
                 THEN [vblock EXCEPT ![n] = [h \in Groups |-> clk[n] + Lease]]
                 ELSE vblock
    /\ UNCHANGED <<catVars, ctlVars, dataVars, disk, inc, clk, now, bkp, term, mlen, role,
                   lease, gwVars, ghostVars>>

\* Crash with the pool wiped. The node cannot resume its identity
\* (inspect_identity, CheckMount); under the mutant it resumes empty.
Wipe(n) ==
    /\ budget.wipe < MaxWipe /\ n # Spare /\ disk[n]
    /\ up' = [up EXCEPT ![n] = Mutant = "wiped_resumes"]
    /\ disk' = [disk EXCEPT ![n] = Mutant = "wiped_resumes"]
    /\ term' = [term EXCEPT ![n] = [h \in Groups |-> 0]]
    /\ mlen' = [mlen EXCEPT ![n] = [h \in Groups |-> 0]]
    /\ role' = [role EXCEPT ![n] = [h \in Groups |-> "F"]]
    /\ lease' = [lease EXCEPT ![n] = [h \in Groups |-> 0]]
    /\ vblock' = [vblock EXCEPT ![n] = [h \in Groups |-> 0]]
    /\ budget' = [budget EXCEPT !.wipe = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, inc, clk, now, bkp, gwVars, acked, elected,
                   votes, lost, badRead, staleApply>>

\* An operator copies node n's pool while it runs (an EBS snapshot).
Backup(n) ==
    /\ budget.stale < MaxStale /\ disk[n] /\ bkp[n] = NoNode
    /\ bkp' = [bkp EXCEPT ![n] = [term |-> term[n], mlen |-> mlen[n]]]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, up, disk, inc, clk, now, raftVars, gwVars,
                   ghostVars>>

\* The operator restores that older copy and restarts the worker with its
\* keys and identity file intact (inspect_identity passes). The candidate
\* fix `stale_disk_detected` refuses the older copy, as a wiped pool.
StaleRestore(n) ==
    /\ bkp[n] # NoNode /\ budget.stale < MaxStale /\ n # Spare /\ ~up[n]
    /\ IF "stale_disk_detected" \in Fixes
       THEN /\ up' = [up EXCEPT ![n] = FALSE]
            /\ disk' = [disk EXCEPT ![n] = FALSE]
            /\ UNCHANGED <<term, mlen>>
       ELSE /\ up' = [up EXCEPT ![n] = TRUE]
            /\ disk' = disk
            /\ term' = [term EXCEPT ![n] = bkp[n].term]
            /\ mlen' = [mlen EXCEPT ![n] = bkp[n].mlen]
    /\ budget' = [budget EXCEPT !.stale = @ + 1]
    /\ role' = [role EXCEPT ![n] = [h \in Groups |-> "F"]]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, inc, clk, now, bkp, lease, vblock, gwVars,
                   acked, elected, votes, lost, badRead, staleApply>>

\* A replacement Mount revokes the wiped worker's identity
\* (machine/system_recovery.rs:539-550).
Revoke(n) ==
    /\ budget.wipe > 0 /\ n # Spare /\ n \notin revoked
    /\ ~up[n] \/ Mutant = "wiped_resumes"
    /\ revoked' = revoked \cup {n}
    /\ UNCHANGED <<catVars, placement, enrolled, owner, ctlOps, guard, plan, cut, splitAb,
                   dataVars, hostVars, raftVars, gwVars, ghostVars>>

Partition(n) ==
    /\ budget.part < MaxPart /\ budget.iso = NoNode
    /\ budget' = [budget EXCEPT !.part = @ + 1, !.iso = n]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gwVars,
                   acked, elected, votes, lost, badRead, staleApply>>

Heal ==
    /\ budget.iso # NoNode
    /\ budget' = [budget EXCEPT !.iso = NoNode]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gwVars,
                   acked, elected, votes, lost, badRead, staleApply>>

\* Loss and duplication of the Control messages (enrollment). Lost replies
\* and duplicated requests of the direct path are modelled in Commit and
\* HandleDirect.
LoseMsg(msg) ==
    /\ msg \in net /\ budget.loss < MaxLoss
    /\ net' = net \ {msg}
    /\ budget' = [budget EXCEPT !.loss = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gw, gwSep, cst, enr,
                   acked, elected, votes, lost, badRead, staleApply>>

DupMsg(msg) ==
    /\ msg \in net /\ msg.d = 0 /\ budget.dup < MaxDup
    /\ net' = net \cup {[msg EXCEPT !.d = 1]}
    /\ budget' = [budget EXCEPT !.dup = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, hostVars, raftVars, gw, gwSep, cst, enr,
                   acked, elected, votes, lost, badRead, staleApply>>

\* True time advances; local clocks advance with it.
Tick ==
    /\ LeaseOn /\ now < MaxTime
    /\ now' = now + 1
    /\ clk' = [n \in Node |-> clk[n] + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, up, disk, inc, bkp, raftVars, gwVars, ghostVars>>

\* Clock drift within the bound: a local clock runs a tick slow or fast.
Drift(n, d) ==
    /\ LeaseOn /\ budget.drift < MaxDriftSteps
    /\ clk[n] + d - now \in -MaxDrift..MaxDrift
    /\ clk' = [clk EXCEPT ![n] = clk[n] + d]
    /\ budget' = [budget EXCEPT !.drift = @ + 1]
    /\ UNCHANGED <<catVars, ctlVars, dataVars, up, disk, inc, now, bkp, raftVars, gwVars,
                   acked, elected, votes, lost, badRead, staleApply>>

(***************************************************************************)
(* Next state                                                              *)
(***************************************************************************)
Faults ==
    \/ \E n \in FaultNodes : Crash(n) \/ Restart(n) \/ Wipe(n) \/ Revoke(n) \/ Partition(n)
                          \/ Backup(n) \/ StaleRestore(n)
    \/ Heal
    \/ \E msg \in net : LoseMsg(msg) \/ DupMsg(msg)
    \/ \E n \in FaultNodes, d \in {-1, 1} : Drift(n, d)
    \/ Tick

RaftSteps ==
    \E n \in Node, g \in Groups :
        \/ Elect(n, g)
        \/ CommitBlank(n, g)
        \/ \E e \in pend[g] : Commit(n, g, e, TRUE)
                          \/ (e.k = "w" /\ budget.loss < MaxLoss /\ Commit(n, g, e, FALSE))
        \/ Heartbeat(n, g)
        \/ \E k \in Key : LeaseRead(n, g, k)

GatewaySteps ==
    \/ \E n \in Node, c \in Client : HandleDirect(n, c, FALSE) \/ HandleDirect(n, c, TRUE)
    \/ \E n \in Node, msg \in net : HandleLate(n, msg)

TopologySteps ==
    \/ SplitBegin \/ SplitRetry \/ SplitPublish \/ SplitAbort \/ SplitDone
    \/ \E n \in Node : SplitFreeze(n) \/ SplitStage(n) \/ SplitFinish(n)
    \/ DdlBegin \/ DdlPublish \/ \E n \in Node, g \in Groups : DdlFenceGroup(n, g)
    \/ OwnerFence \/ \E n \in Node, g \in Groups : FenceGroup(n, g)
    \/ \E g \in Groups, v \in Node : MoveBegin(g, v)
    \/ \E n \in Node, kind \in {"learner", "joint", "final", "nojoint"} : MovePropose(n, kind)
    \/ MovePublish \/ MoveRetire
    \/ EnrollSend \/ EnrollTimeout
    \/ \E msg \in net : CtlEnroll(msg) \/ CtlRefuse(msg) \/ EnrollReply(msg)

\* The run ends when every client has its answer and no topology change or
\* enrollment is in flight.
Finished ==
    /\ \A c \in Client : cst[c] = "acked"
    /\ guard = "none" /\ ~SplitActive /\ plan = "none"
    /\ enr.st \in {"done", "failed"}

Terminal == Finished /\ UNCHANGED vars

Next == Faults \/ RaftSteps \/ GatewaySteps \/ TopologySteps \/ Terminal

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Type invariant                                                          *)
(***************************************************************************)
TypeOK ==
    /\ route \in [Key -> Groups]
    /\ repoch \in [Groups -> Nat]
    /\ sepoch \in Nat
    /\ placement \in [Groups -> SUBSET Node]
    /\ enrolled \in [Node -> Nat]
    /\ revoked \subseteq Node
    /\ owner \in Nat
    /\ guard \in {"none", "split", "ddl", "move"}
    /\ cut \in Nat /\ tso \in Nat /\ splitAb \in BOOLEAN
    /\ \A g \in Groups : \A e \in pend[g] : e.by \in Node /\ e.t \in 1..MaxTerm
    /\ up \in [Node -> BOOLEAN] /\ disk \in [Node -> BOOLEAN]
    /\ inc \in [Node -> Nat]
    /\ term \in [Node -> [Groups -> 0..MaxTerm]]
    /\ \A n \in Node, g \in Groups : mlen[n][g] \in 0..Len(hst[g]) \/ lost
    /\ role \in [Node -> [Groups -> {"F", "C", "L"}]]
    /\ lease \in [Node -> [Groups -> Int]] /\ vblock \in [Node -> [Groups -> Int]]
    /\ clk \in [Node -> Int] /\ now \in 0..MaxTime
    /\ \A n \in Node : clk[n] - now \in -MaxDrift..MaxDrift
    /\ cst \in [Client -> {"pend", "acked"}]
    /\ acked \subseteq Client
    /\ enr.st \in {"start", "wait", "done", "failed"}
    /\ lost \in BOOLEAN /\ badRead \in BOOLEAN /\ staleApply \in BOOLEAN

(***************************************************************************)
(* Safety properties. Each names the guarantee it checks.                  *)
(***************************************************************************)
\* OLTP-G08, OLTP-G10: one leader per range and term.
G08_OneLeaderPerTerm ==
    \A x, y \in elected : x[1] = y[1] /\ x[2] = y[2] => x[3] = y[3]

\* OLTP-G08 (RV-REP-3, the simulator's WipedNeverVotes): a voter grants one
\* vote per range and term, so a lost or rolled-back pool never votes again.
G08_OneVotePerTerm ==
    \A x, y \in votes : x[1] = y[1] /\ x[2] = y[2] /\ x[3] = y[3] => x[4] = y[4]

\* OLTP-G01, OLTP-G08: no committed entry is overwritten, across elections,
\* moves, replacements, restarts and restores.
G01_CommittedNeverLost == ~lost

\* OLTP-G01, G06, G08: every acknowledged write is in the committed state of
\* the group the Catalog routes its key to, across splits and moves.
G06_AckedAtOwner == \A c \in acked : Applied(Cur(route[WKey[c]]), c)

\* OLTP-G06: no write applies at a schema epoch the Catalog retired, or
\* under an authority its group already fenced.
G06_NoStaleApply == ~staleApply

\* OLTP-G02 (lease reads, direct path section 6): a lease read sees every
\* write acknowledged before it.
G02_LeaseReadFresh == ~badRead

\* OLTP-G08: Control's published placement of a group is the Data group's
\* configuration, or the old one while that group's move is in flight.
G08_PlacementMatchesData ==
    \A g \in Groups : Exists(g) =>
        placement[g] \in {Cur(g).vo, Cur(g).vn} \cup
                         (IF plan # "none" /\ plan.g = g THEN {plan.old} ELSE {})

\* OLTP-G08, OLTP-G01: no availability zone holds a majority of the voters
\* of any committed configuration (single-AZ loss keeps every range).
G08_ZonePlacement == \A g \in Groups : Exists(g) => ZoneSafe(Cur(g).vo) /\ ZoneSafe(Cur(g).vn)

\* OLTP-G08: at most one topology operation at a time.
G08_SerialTopologyOps == ~(SplitActive /\ plan # "none")

\* OLTP-G08 (O16, PR #104): an enrolled worker runs under exactly the
\* identity Control committed for it.
G08_EnrolledIdentity == enr.st = "done" => inc[Spare] = enrolled[Spare]

=============================================================================
