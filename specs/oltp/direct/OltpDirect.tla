----------------------------- MODULE OltpDirect -----------------------------
(***************************************************************************)
(* Design spec of the OLTP direct path: plans/oltp-direct-path.md, module  *)
(* oltp, level L3. Rewritten in the 2026-11-26 review (rv-oltp-direct)     *)
(* from the stopped draft of dr-oltp-direct, which needed VIEW to fit.     *)
(*                                                                         *)
(* It models, for synchronous single-key mutations:                        *)
(*  - gateway calls, attempts, resends and the deadline (section 6,        *)
(*    application/direct.rs `call`, `deadline_error`);                     *)
(*  - the range leader's executor: admission, latch, dedup lookup,         *)
(*    compute, timestamp, RangeClock registration, propose, and a          *)
(*    proposal that ends without a reply (direct/executor.rs `execute`,    *)
(*    `mutate`, `attempt`, `prepare`, `proposal_failed`);                  *)
(*  - one Data log entry per commit, decided at apply (section 4,          *)
(*    mvcc/store/direct.rs `check`, `write`): dedup record, expected       *)
(*    version, timestamp above the newest version;                         *)
(*  - the retry-results rule (section 4, lead decision 2026-11-24): a      *)
(*    no-op or row conflict is a definite result and is recorded;          *)
(*  - RangeClock rules 1 to 3 (section 5, store/direct/clock.rs) against   *)
(*    snapshot reads at a timestamp;                                       *)
(*  - a queued writer that takes no direct latch (section 5, last          *)
(*    sentence; its intent and commit as one entry).                       *)
(*                                                                         *)
(* Abstractions, recorded in the plan's Design review section:             *)
(*  - Raft is a committed state per range plus the leader's proposals that *)
(*    may still commit. A new leader holds a prefix of them (a follower's  *)
(*    log is a prefix of the leader's) and commits it before it serves     *)
(*    (rule 3). Elections, membership and disk loss are OltpRanges'.       *)
(*  - A deposed leader stops at once; lease safety of reads is assumed.    *)
(*  - A gateway request reaches its node at once and waits there before    *)
(*    its latch ("pre"), so a delayed request is a delayed executor. A     *)
(*    reply reaches the gateway when the executor ends, unless it is lost  *)
(*    (fault "loss"); a late reply is one the gateway no longer waits for. *)
(*  - Commit timestamps come from one counter that grows in real time.     *)
(*    The Timestamp group's window, coalescing and failover are out of     *)
(*    scope (independent read R-08, R-09).                                 *)
(*  - The activation boundary, forward markers, the queue owner and the    *)
(*    direct_oltp flip are not in this spec (plan Design review, gaps).    *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Nodes,          \* workers; each is a voter of every range (RF3)
    Ranges, Keys, RangeOf,
    Clients,        \* each client sends one request through its own gateway
    Scenarios,      \* set of [req: [Clients -> request], rows: [Keys -> value], qkey: key or "none"]
    InitLeader,     \* [Ranges -> Nodes]
    MaxFaults,      \* faults per behavior over all kinds (two may overlap)
    FaultKinds,     \* enabled fault kinds, each at most once
    MaxAtt,         \* [Clients -> Nat]: attempts per gateway call
    MaxRetry,       \* [Clients -> Nat]: new calls after outcome_unknown
    MaxRepeat,      \* [Clients -> Nat]: new calls after a definite answer
    MaxExecRetry,   \* leader re-reads after Rejected(Conflict)
    MaxReads,       \* snapshot reads (queued reads or scans at a timestamp)
    Mutant          \* "none" or a mutant name; see mutants/*.cfg

None == "none"
NoNode == "nonode"

VARIABLES
    scn,        \* the scenario of this behavior (constant after Init)
    \* Replicated, durable: committed state and proposals that may commit
    st,         \* [Ranges -> range state]
    prop,       \* [Ranges -> Seq(entry)]: in the leader's log, not committed
    \* Leadership and nodes (volatile)
    leader,     \* [Ranges -> Nodes]
    lst,        \* [Ranges -> {"down", "elected", "ready"}]
    up,         \* [Nodes -> BOOLEAN]
    iso,        \* a node cut off from the other workers, or NoNode
    \* The range leader's RangeClock (volatile, on the leader)
    pend,       \* [Ranges -> set of [ts, key]]
    tso,        \* the Timestamp authority's last issued value
    \* Executors on range leaders (volatile)
    exec,
    \* Gateways and clients (volatile)
    gw,
    \* The queued writer (scenario qkey)
    qw,
    \* Ghost and budgets
    answers, reads, used

vars == <<scn, st, prop, leader, lst, up, iso, pend, tso, exec, gw, qw, answers, reads, used>>

-----------------------------------------------------------------------------
(* Requests are [id, op, key, val]. The digest is the whole body.          *)

Req(c) == scn.req[c]
Dg(q) == <<q.op, q.key, q.val>>
KeyOf(c) == Req(c).key
RngOf(c) == RangeOf[KeyOf(c)]

-----------------------------------------------------------------------------
(* The range state machine (section 4, store/direct.rs `check`, `write`). *)
(* row[k] = [v, ver]: the newest value and its commit timestamp (0 for the *)
(* initial row). hist: every committed version [k, ts]. recs: dedup        *)
(* records. w, dup, bad: ghosts of the writes and records.                 *)

InitRS == [row |-> [k \in Keys |-> [v |-> scn.rows[k], ver |-> 0]],
           hist |-> {[k |-> k, ts |-> 0] : k \in {x \in Keys : scn.rows[x] # None}},
           recs |-> {}, w |-> {}, dup |-> FALSE, bad |-> FALSE]

NoRec == [key |-> None, id |-> None, dg |-> <<>>, res |-> None, rk |-> None]
\* The 'D' record of (key, id). Mutant dedup_ignored: none is ever found.
RecOf(s, key, id) ==
    LET R == {x \in s.recs : x.key = key /\ x.id = id}
    IN IF R = {} \/ Mutant = "dedup_ignored" THEN NoRec ELSE CHOOSE x \in R : TRUE
Has(rec) == rec.id # None

Present(s, k) == s.row[k].v # None

\* The exact result on state s (planner row functions): wr says whether the
\* row changes; rk is the answer kind ("ok" success, "na" a refusal).
Compute(q, s) ==
    CASE q.op = "ins" ->
           IF Present(s, q.key)
           THEN [wr |-> FALSE, nv |-> None, res |-> "exists", rk |-> "na"]
           ELSE [wr |-> TRUE, nv |-> q.val, res |-> "ok", rk |-> "ok"]
      [] q.op = "upd" ->
           IF Present(s, q.key)
           THEN [wr |-> TRUE, nv |-> q.val, res |-> "upd", rk |-> "ok"]
           ELSE [wr |-> FALSE, nv |-> None, res |-> "miss", rk |-> "ok"]
      [] q.op = "del" ->
           IF Present(s, q.key)
           THEN [wr |-> TRUE, nv |-> None, res |-> "del", rk |-> "ok"]
           ELSE [wr |-> FALSE, nv |-> None, res |-> "miss", rk |-> "ok"]

\* Apply one direct member m in the order of `check`: dedup record, head
\* version, timestamp above the newest version, then one batch with the row,
\* the record and the applied position. o is "applied", "replayed" (with the
\* record) or "conflict" (Rejected(Conflict): nothing written).
ApplyDirect(s, m) ==
    LET rec == RecOf(s, m.key, m.id)
        head == s.row[m.key]
        q == [id |-> m.id, op |-> m.dg[1], key |-> m.key, val |-> m.dg[3]]
    IN
    IF Has(rec) THEN
        IF rec.dg = m.dg THEN [s |-> s, o |-> "replayed", rec |-> rec]
        ELSE [s |-> s, o |-> "conflict", rec |-> NoRec]
    ELSE IF (head.ver # m.exp /\ ~(Mutant = "record_no_precondition" /\ ~m.wr))
            \/ (m.wr /\ m.ts <= head.ver)
         THEN [s |-> s, o |-> "conflict", rec |-> NoRec]
    ELSE
        LET r1 == [key |-> m.key, id |-> m.id, dg |-> m.dg, res |-> m.res, rk |-> m.rk]
            s1 == IF m.wr
                  THEN [s EXCEPT !.row[m.key] = [v |-> m.nv, ver |-> m.ts],
                                 !.hist = @ \cup {[k |-> m.key, ts |-> m.ts]},
                                 !.dup = @ \/ (\E x \in s.w : x.key = m.key /\ x.id = m.id),
                                 !.w = @ \cup {[key |-> m.key, id |-> m.id, dg |-> m.dg]}]
                  ELSE s
        \* bad: the recorded result is not the operation's result on the state
        \* at this log position (ghost for G03_RecordedPrecondition).
        IN [s |-> [s1 EXCEPT !.recs = @ \cup {r1},
                             !.bad = @ \/ (Compute(q, s).res # m.res)],
            o |-> "applied", rec |-> r1]

\* The queued writer's transaction: its intent and commit as one entry, with
\* the commit timestamp taken after its prepare (here: at apply).
ApplyQueued(s, e, ts) ==
    [s EXCEPT !.row[e.key] = [v |-> "q", ver |-> ts],
              !.hist = @ \cup {[k |-> e.key, ts |-> ts]}]

-----------------------------------------------------------------------------
(* Nodes and quorums.                                                      *)

Linked(n) == up[n] /\ iso # n
Quorum(n) == Linked(n) /\ \E o \in Nodes \ {n} : Linked(o)
Serving(n, r) == leader[r] = n /\ lst[r] = "ready" /\ up[n]

\* Something can still change a range or an answer.
Busy == \/ \E c \in Clients : gw[c].gs = "call"
        \/ exec # {} \/ \E r \in Ranges : prop[r] # <<>>
Fault(kind) == /\ Busy /\ kind \in FaultKinds /\ kind \notin used
               /\ Cardinality(used) < MaxFaults
               /\ used' = used \cup {kind}

-----------------------------------------------------------------------------
(* Gateway and client (application/direct.rs `route`, `call`).             *)
(* gw[c]: st "idle" (may send), "call" (a call waits) or "done"; k the call *)
(* number; n the attempts sent; unc an attempt of this call may have run;  *)
(* open the attempts without an answer; rt, rp the calls already made after *)
(* an unknown and after a definite answer.                                 *)

GwInit == [gs |-> "idle", k |-> 0, n |-> 0, unc |-> FALSE, open |-> {}, rt |-> 0, rp |-> 0]

\* A definite outcome of the identity: a result, or a refusal tied to the
\* row or the id (insert of a present row, reused id).
DefOutcome(kind, res) == kind = "ok" \/ (kind = "na" /\ res \in {"exists", "reused"})

\* not_applied without proof (DR-2). Section 6 says the deadline returns
\* outcome_unknown. The shipped gateway answers not_applied when this call
\* saw no uncertain attempt (`deadline_error`, and a typed 429 passed
\* through): mutant prefix_dr2_not_applied_per_call. The fixed design
\* answers outcome_unknown for an unsettled mutation.
NotApplied(g, a) ==
    IF Mutant = "prefix_dr2_not_applied_per_call" /\ ~g.unc /\ g.open \subseteq {a}
    THEN "na" ELSE "unk"

\* The call of c ends with an answer (kind "unk" is outcome_unknown): the
\* new gateways and answers, as a record [g, a].
End(G, A, c, kind, res) ==
    LET g == G[c]
        again == IF kind = "unk" THEN g.rt < MaxRetry[c]
                 ELSE DefOutcome(kind, res) /\ g.rp < MaxRepeat[c]
    IN [g |-> [G EXCEPT ![c] = [g EXCEPT !.gs = IF again THEN "idle" ELSE "done",
                                         !.n = 0, !.unc = FALSE, !.open = {},
                                         !.rt = IF kind = "unk" THEN @ + 1 ELSE @,
                                         !.rp = IF kind # "unk" THEN @ + 1 ELSE @]],
        a |-> IF kind = "unk" THEN A ELSE A \cup {[c |-> c, kind |-> kind, res |-> res]}]

Waiting(G, c, k, a) == G[c].gs = "call" /\ G[c].k = k /\ a \in G[c].open

\* Attempt a of call k of c ends with a reply that reaches the gateway.
\* kind: "ok" or "na" (definite), "unk" (unknown), "ovl" (a typed 429
\* before execution), "busy" (NotLeader or a not-applied refusal of this
\* attempt): the gateway keeps calling on the last three.
Deliver(G, A, c, k, a, kind, res) ==
    IF ~Waiting(G, c, k, a) THEN [g |-> G, a |-> A]
    ELSE IF kind \in {"ok", "na"} THEN End(G, A, c, kind, res)
    ELSE IF kind = "ovl" /\ ~G[c].unc THEN End(G, A, c, NotApplied(G[c], a), "overloaded")
    ELSE [g |-> [G EXCEPT ![c].open = @ \ {a}, ![c].unc = @ \/ kind \in {"unk", "ovl"}],
          a |-> A]

\* One executor's reply: delivered, or lost on the way (fault "loss"; the
\* gateway's attempt timer then resends or its deadline answers).
Out(x, kind, res) ==
    LET d == Deliver(gw, answers, x.c, x.k, x.a, kind, res) IN
    \/ /\ gw' = d.g /\ answers' = d.a /\ UNCHANGED used
    \/ /\ Waiting(gw, x.c, x.k, x.a) /\ Fault("loss")
       /\ UNCHANGED <<gw, answers>>

RECURSIVE DeliverAll(_, _, _)
\* The executors in X end: unknown after a crash; after an election,
\* unknown once proposed, else not applied (NotLeader).
DeliverAll(acc, X, crash) ==
    IF X = {} THEN acc
    ELSE LET x == CHOOSE x \in X : TRUE
             kind == IF crash \/ x.ph = "prop" THEN "unk" ELSE "busy"
         IN DeliverAll(Deliver(acc.g, acc.a, x.c, x.k, x.a, kind, None), X \ {x}, crash)

\* The attempt reaches node n: on the range's leader it becomes an
\* executor before its latch ("pre"); elsewhere NotLeader, or the node is
\* down and the connection fails before sending: not applied.
Arrive(c, k, a) ==
    LET r == RngOf(c)  n == leader[r] IN
    IF up[n] /\ lst[r] # "down"
    THEN [x |-> {[c |-> c, k |-> k, a |-> a, n |-> n, r |-> r, ph |-> "pre", rd |-> 0, ts |-> 0]},
          busy |-> FALSE]
    ELSE [x |-> {}, busy |-> TRUE]

\* A client sends its request, or a new call with the same id and body.
ClientSend(c) ==
    LET g == gw[c]  k == g.k + 1  v == Arrive(c, k, 1) IN
    /\ g.gs = "idle"
    /\ exec' = exec \cup v.x
    /\ gw' = [gw EXCEPT ![c] = [g EXCEPT !.gs = "call", !.k = k, !.n = 1, !.unc = FALSE,
                                         !.open = IF v.busy THEN {} ELSE {1}]]
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, qw, answers, reads, used>>

\* A retry call that reaches no leader before its deadline: the route cannot
\* be resolved, a Catalog or Control read fails, or every voter refuses the
\* connection (`route_unavailable`, `deadline_error`). A first call that
\* reaches nothing changes nothing an identity depends on.
GwUnreachable(c) ==
    /\ gw[c].gs = "idle" /\ gw[c].k >= 1
    /\ gw' = [gw EXCEPT ![c].k = @ + 1, ![c].gs = "call", ![c].n = 0, ![c].open = {}]
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, exec, qw, answers, reads,
                   used>>

\* PEER_ATTEMPT or a lost reply: another attempt of the same call. An
\* earlier attempt may still run, so the outcome is uncertain from now on.
GwResend(c) ==
    LET g == gw[c]  v == Arrive(c, g.k, g.n + 1) IN
    /\ g.gs = "call" /\ g.n >= 1 /\ g.n < MaxAtt[c]
    /\ exec' = exec \cup v.x
    /\ gw' = [gw EXCEPT ![c] = [g EXCEPT !.n = @ + 1, !.unc = @ \/ g.open # {},
                                         !.open = IF v.busy THEN @ ELSE @ \cup {g.n + 1}]]
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, qw, answers, reads, used>>

\* The gateway deadline. Each attempt carries a leader deadline below the
\* gateway's (`request.deadline = left - LEADER_MARGIN`, at least `left / 2`),
\* so every executor of the client has ended by now. Entries already
\* proposed stay in the log. The shipped executor removes a proposed one
\* from `pending` when its `by` deadline cuts the proposal
\* (`clock.settle(commit_ts)` after any proposal error): prefix_settle_unknown.
GwDeadline(c) ==
    LET X == {x \in exec : x.c = c}
        P == {x.ts : x \in {y \in X : y.ph = "prop"}}
        d == End(gw, answers, c, NotApplied(gw[c], 0), "deadline")
    IN
    /\ gw[c].gs = "call"
    /\ gw' = d.g /\ answers' = d.a
    /\ exec' = exec \ X
    /\ pend' = IF Mutant = "prefix_settle_unknown"
               THEN [r \in Ranges |-> {p \in pend[r] : p.ts \notin P}] ELSE pend
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, tso, qw, reads, used>>

\* The gateway process dies mid-call (D02): the client sees a broken
\* connection, outcome_unknown, and retries through another gateway. The
\* leader's executors run on to their own deadline.
GwCrash(c) ==
    LET d == End(gw, answers, c, "unk", "crash") IN
    /\ gw[c].gs = "call" /\ gw[c].n >= 1 /\ Fault("gw")
    /\ gw' = d.g /\ answers' = d.a
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, exec, qw, reads>>

-----------------------------------------------------------------------------
(* Range leader (direct/executor.rs).                                      *)

\* A saturated leader refuses before execution: 429 overloaded, not_applied
\* (`admit`, before the dedup lookup).
RefuseRequest(x) ==
    LET d == Deliver(gw, answers, x.c, x.k, x.a, "ovl", None) IN
    /\ x \in exec /\ x.ph = "pre" /\ Fault("refuse")
    /\ exec' = exec \ {x}
    /\ gw' = d.g /\ answers' = d.a
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, qw, reads>>

\* The key latch: no other executor of the node that proposed or retries
\* on the key (latches are held from the read to the apply).
LatchFree(x) ==
    ~\E y \in exec : y # x /\ y.n = x.n /\ KeyOf(y.c) = KeyOf(x.c) /\ y.ph \in {"prop", "retry"}
\* `wait_latest`: no pending commit on the key in the leader's clock.
NoPending(x) == ~\E p \in pend[x.r] : p.key = KeyOf(x.c)
CanExecute(x) == Serving(x.n, x.r) /\ LatchFree(x) /\ NoPending(x)

\* Steps 2 to 5 under the latch: dedup lookup, compute, timestamp, register
\* in `pending` (rule 1 holds: a new timestamp is above every snapshot
\* already served), propose. With a dedup record it answers from it. A
\* read-only outcome (a no-op or a row conflict) is a definite result: the
\* fixed design commits a record for it in the same kind of entry, with the
\* expected version of the key it read (lead decision 2026-11-24). The
\* shipped executor returns it without a record (`prepare`: "A read-only
\* outcome commits nothing"; `compute`: `already_exists()`): prefix_dr1_*.
Execute(x) ==
    LET q == Req(x.c)  r == x.r  s == st[r]
        rec == RecOf(s, q.key, q.id)
        cm == Compute(q, s)
        unrecorded == \/ (Mutant = "prefix_dr1_readonly_unrecorded" /\ cm.rk = "ok")
                      \/ (Mutant = "prefix_dr1_refusal_unrecorded" /\ cm.rk = "na")
        ts == tso + 1
        mem == [c |-> x.c, k |-> x.k, a |-> x.a, id |-> q.id, dg |-> Dg(q), key |-> q.key,
                exp |-> s.row[q.key].ver, wr |-> cm.wr, nv |-> cm.nv, res |-> cm.res,
                rk |-> cm.rk, ts |-> ts, q |-> FALSE]
    IN
    /\ x \in exec /\ x.ph \in {"pre", "retry"} /\ CanExecute(x)
    /\ IF Has(rec) \/ (~cm.wr /\ unrecorded) THEN
          /\ exec' = exec \ {x}
          /\ IF Has(rec)
             THEN Out(x, IF rec.dg # Dg(q) THEN "na" ELSE rec.rk,
                      IF rec.dg # Dg(q) THEN "reused"
                      ELSE IF Mutant = "reexecute_on_retry" THEN cm.res ELSE rec.res)
             ELSE Out(x, cm.rk, cm.res)
          /\ UNCHANGED <<prop, pend, tso>>
       ELSE
          /\ tso' = ts
          /\ pend' = [pend EXCEPT ![r] = @ \cup {[ts |-> ts, key |-> q.key]}]
          /\ prop' = [prop EXCEPT ![r] = Append(@, mem)]
          /\ exec' = (exec \ {x}) \cup {[x EXCEPT !.ph = "prop", !.ts = ts]}
          /\ IF Mutant = "ack_before_commit" /\ cm.wr THEN Out(x, cm.rk, cm.res)
             ELSE UNCHANGED <<gw, answers, used>>
    /\ UNCHANGED <<scn, st, leader, lst, up, iso, qw, reads>>

\* Before its latch or its proposal the attempt cannot go on: the leader is
\* not ready, or the latch or `wait_latest` waits past its deadline. It
\* ends with a not-applied refusal of this attempt (`unavailable_not_applied`).
ExecGiveUp(x) ==
    /\ x \in exec /\ x.ph \in {"pre", "retry"} /\ ~CanExecute(x)
    /\ exec' = exec \ {x}
    /\ Out(x, "busy", None)
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, tso, qw, reads>>

\* The proposal produced no reply before the leader's deadline, because
\* the leader lost its quorum. The entry may still commit: the outcome is
\* unknown (`proposal_failed`). The shipped executor also removes the
\* commit from `pending` (`clock.settle(commit_ts)` after the proposal
\* error): mutant prefix_settle_unknown. The fixed design keeps it there
\* until it applies or leadership changes.
ExecTimeout(x) ==
    /\ x \in exec /\ x.ph = "prop" /\ ~Quorum(x.n)
    /\ exec' = exec \ {x}
    /\ Out(x, "unk", None)
    /\ pend' = IF Mutant = "prefix_settle_unknown"
               THEN [pend EXCEPT ![x.r] = {p \in @ : p.ts # x.ts}] ELSE pend
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, tso, qw, reads>>

-----------------------------------------------------------------------------
(* Raft (hosted.rs) and the apply path (ParticipantMachine::apply).        *)

RECURSIVE ApplyAll(_, _, _, _)
\* Applies the entries of ps in order: [s, t] (state and timestamp counter).
ApplyAll(s, t, ps, i) ==
    IF i > Len(ps) THEN [s |-> s, t |-> t]
    ELSE IF ps[i].q THEN ApplyAll(ApplyQueued(s, ps[i], t + 1), t + 1, ps, i + 1)
    ELSE ApplyAll(ApplyDirect(s, ps[i]).s, t, ps, i + 1)

\* AppendEntries reached a majority: the head entry commits and applies on
\* the leader. Its member leaves `pending`; its executor replies or, on
\* Rejected(Conflict), goes back to step 3 holding its latch.
RaftCommit(r) ==
    LET L == leader[r]  e == Head(prop[r]) IN
    /\ lst[r] = "ready" /\ Quorum(L) /\ prop[r] # <<>>
    /\ prop' = [prop EXCEPT ![r] = Tail(@)]
    /\ IF e.q THEN
          /\ st' = [st EXCEPT ![r] = ApplyQueued(@, e, tso + 1)]
          /\ tso' = tso + 1
          /\ UNCHANGED <<exec, pend, gw, answers, used>>
       ELSE
          LET a == ApplyDirect(st[r], e)
              X == {x \in exec : x.n = L /\ x.c = e.c /\ x.k = e.k /\ x.a = e.a /\ x.ph = "prop"}
              x == CHOOSE x \in X : TRUE
          IN
          /\ st' = [st EXCEPT ![r] = a.s]
          /\ pend' = [pend EXCEPT ![r] = {p \in @ : p.ts # e.ts}]
          /\ UNCHANGED tso
          /\ IF X = {} THEN UNCHANGED <<exec, gw, answers, used>>
             ELSE IF a.o = "conflict" /\ x.rd < MaxExecRetry
             THEN /\ exec' = (exec \ {x}) \cup {[x EXCEPT !.ph = "retry", !.rd = @ + 1]}
                  /\ UNCHANGED <<gw, answers, used>>
             ELSE /\ exec' = exec \ {x}
                  /\ CASE a.o = "applied" -> Out(x, e.rk, e.res)
                       [] a.o = "replayed" -> Out(x, a.rec.rk, a.rec.res)
                       \* the retries ran out before the deadline: not applied
                       [] OTHER -> Out(x, "busy", None)
    /\ UNCHANGED <<scn, leader, lst, up, iso, qw, reads>>

\* n wins an election of r. It holds a prefix of the old leader's
\* proposals. The old leader's executors end and its clock is gone; the
\* new leader's clock starts empty (`lead`).
DoElect(n, r) ==
    LET d == DeliverAll([g |-> gw, a |-> answers], {x \in exec : x.r = r}, FALSE) IN
    /\ leader' = [leader EXCEPT ![r] = n]
    /\ lst' = [lst EXCEPT ![r] = "elected"]
    /\ \E j \in 0..Len(prop[r]) : prop' = [prop EXCEPT ![r] = SubSeq(@, 1, j)]
    /\ pend' = [pend EXCEPT ![r] = {}]
    /\ exec' = {x \in exec : x.r # r}
    /\ gw' = d.g /\ answers' = d.a

\* The election timer fires on a majority that lost its leader.
RaftElect(n, r) ==
    /\ Busy /\ Quorum(n) /\ n # leader[r]
    /\ lst[r] = "down" \/ ~Linked(leader[r])
    /\ DoElect(n, r)
    /\ UNCHANGED <<scn, st, up, iso, tso, qw, reads, used>>

\* Leader balancing or an operator transfer: a leader change, no failure.
MoveLeader(n, r) ==
    /\ Quorum(n) /\ n # leader[r] /\ lst[r] = "ready"
    /\ Fault("move")
    /\ DoElect(n, r)
    /\ UNCHANGED <<scn, st, up, iso, tso, qw, reads>>

\* Rule 3: the new leader's first entry commits after the older entries its
\* log holds. Mutant first_entry_needs_traffic: only a new proposal does it
\* (no leader no-op).
RaftFirstEntry(r) ==
    LET a == ApplyAll(st[r], tso, prop[r], 1) IN
    /\ lst[r] = "elected" /\ Quorum(leader[r])
    /\ Mutant = "first_entry_needs_traffic" => \E x \in exec : x.r = r
    /\ st' = [st EXCEPT ![r] = a.s]
    /\ tso' = a.t
    /\ prop' = [prop EXCEPT ![r] = <<>>]
    /\ lst' = [lst EXCEPT ![r] = "ready"]
    /\ UNCHANGED <<scn, leader, up, iso, pend, exec, gw, qw, answers, reads, used>>

\* Mutant no_rule3: a new leader serves before its first entry applied.
SkipRule3(r) ==
    /\ Mutant = "no_rule3" /\ lst[r] = "elected" /\ Quorum(leader[r])
    /\ lst' = [lst EXCEPT ![r] = "ready"]
    /\ UNCHANGED <<scn, st, prop, leader, up, iso, pend, tso, exec, gw, qw, answers, reads, used>>

-----------------------------------------------------------------------------
(* A snapshot read at s on the range leader (rule 2, `observe_snapshot`):  *)
(* a queued read or scan takes s from the Timestamp authority, raises      *)
(* `max_read_ts`, waits until no pending commit is at or below s, and      *)
(* reads the versions at or below s, behind a lease or ReadIndex.          *)
(* Commits register later with a larger timestamp, so rule 1 holds by      *)
(* construction and `max_read_ts` is not kept.                             *)

SnapshotRead(r) ==
    LET s == tso + 1 IN
    /\ Cardinality(reads) < MaxReads /\ Busy
    /\ Serving(leader[r], r) /\ Quorum(leader[r])
    /\ ~\E p \in pend[r] : p.ts <= s
    /\ tso' = s
    /\ reads' = reads \cup {[r |-> r, s |-> s, seen |-> {h \in st[r].hist : h.ts <= s}]}
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, iso, pend, exec, gw, qw, answers, used>>

\* The queued writer proposes its transaction on the scenario's key. It
\* takes no direct latch and is not in the RangeClock.
QueuedPropose ==
    LET qk == scn.qkey IN
    /\ qw = "idle" /\ qk # None /\ Serving(leader[RangeOf[qk]], RangeOf[qk])
    /\ prop' = [prop EXCEPT ![RangeOf[qk]] =
                   Append(@, [q |-> TRUE, key |-> qk, ts |-> 0, c |-> None, k |-> 0, a |-> 0])]
    /\ qw' = "sent"
    /\ UNCHANGED <<scn, st, leader, lst, up, iso, pend, tso, exec, gw, answers, reads, used>>

-----------------------------------------------------------------------------
(* Faults. Each enabled kind once; at most MaxFaults per behavior.         *)

\* A crash: the gateways see a broken connection (unknown) for every attempt
\* the node held; its clock is gone with it.
CrashNode(n) ==
    LET d == DeliverAll([g |-> gw, a |-> answers], {x \in exec : x.n = n}, TRUE) IN
    /\ up[n] /\ Fault("crash")
    /\ up' = [up EXCEPT ![n] = FALSE]
    /\ lst' = [r \in Ranges |-> IF leader[r] = n THEN "down" ELSE lst[r]]
    /\ exec' = {x \in exec : x.n # n}
    /\ pend' = [r \in Ranges |-> IF leader[r] = n THEN {} ELSE pend[r]]
    /\ gw' = d.g /\ answers' = d.a
    /\ UNCHANGED <<scn, st, prop, leader, iso, tso, qw, reads>>

\* The process supervisor restarts it with its disk.
Restart(n) ==
    /\ ~up[n] /\ Busy
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<scn, st, prop, leader, lst, iso, pend, tso, exec, gw, qw, answers, reads, used>>

\* n loses the other workers; gateways still reach it.
Partition(n) ==
    /\ iso = NoNode /\ Fault("part")
    /\ iso' = n
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, pend, tso, exec, gw, qw, answers, reads>>

Heal ==
    /\ iso # NoNode /\ Busy
    /\ iso' = NoNode
    /\ UNCHANGED <<scn, st, prop, leader, lst, up, pend, tso, exec, gw, qw, answers, reads, used>>

-----------------------------------------------------------------------------

Init ==
    /\ scn \in Scenarios
    /\ st = [r \in Ranges |-> InitRS]
    /\ prop = [r \in Ranges |-> <<>>]
    /\ leader = InitLeader
    /\ lst = [r \in Ranges |-> "ready"]
    /\ up = [n \in Nodes |-> TRUE]
    /\ iso = NoNode
    /\ pend = [r \in Ranges |-> {}]
    /\ tso = 0
    /\ exec = {}
    /\ gw = [c \in Clients |-> GwInit]
    /\ qw = "idle"
    /\ answers = {}
    /\ reads = {}
    /\ used = {}

\* Every client is done: an explicit terminal stuttering step.
Done ==
    /\ \A c \in Clients : gw[c].gs = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E c \in Clients : \/ ClientSend(c) \/ GwUnreachable(c) \/ GwResend(c)
                          \/ GwDeadline(c) \/ GwCrash(c)
    \/ \E x \in exec : RefuseRequest(x) \/ Execute(x) \/ ExecGiveUp(x) \/ ExecTimeout(x)
    \/ \E r \in Ranges : RaftCommit(r) \/ RaftFirstEntry(r) \/ SkipRule3(r) \/ SnapshotRead(r)
    \/ \E n \in Nodes, r \in Ranges : RaftElect(n, r) \/ MoveLeader(n, r)
    \/ \E n \in Nodes : CrashNode(n) \/ Restart(n) \/ Partition(n)
    \/ Heal \/ QueuedPropose
    \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety. Each property names the guarantee it checks.                    *)

TypeOK ==
    /\ leader \in [Ranges -> Nodes]
    /\ lst \in [Ranges -> {"down", "elected", "ready"}]
    /\ up \in [Nodes -> BOOLEAN]
    /\ iso \in Nodes \cup {NoNode}
    /\ tso \in Nat
    /\ \A x \in exec : x.ph \in {"pre", "prop", "retry"}
    /\ \A c \in Clients : gw[c].gs \in {"idle", "call", "done"}
    /\ \A a \in answers : a.kind \in {"ok", "na"}
    /\ used \subseteq FaultKinds
    /\ qw \in {"idle", "sent"}

StateFor(c) == st[RngOf(c)]
\* The identity of client c's request wrote its row.
Wrote(c) == [key |-> KeyOf(c), id |-> Req(c).id, dg |-> Dg(Req(c))] \in StateFor(c).w
Def(a) == DefOutcome(a.kind, a.res)

\* OLTP-G01 (D04, O06): an acknowledged write is committed.
G01_AckedDurable ==
    \A a \in answers : (a.kind = "ok" /\ a.res \in {"ok", "upd", "del"}) => Wrote(a.c)

\* OLTP-G02 (O07, O09, D06; section 5 rule 2): a snapshot read at s never
\* misses a commit at or below s, so a repeated read at s sees the same
\* versions. Checked in every later state.
G02_SnapshotStable ==
    \A rd \in reads : {h \in st[rd.r].hist : h.ts <= rd.s} = rd.seen

\* OLTP-G03 (D02, D04, O10): per table, key and request ID at most one
\* execution writes.
G03_OncePerKey == \A r \in Ranges : ~st[r].dup

\* OLTP-G03 (O08, D02; lead decision 2026-11-24): every definite answer of
\* an identity with the same body is the same, and equals its record.
G03_SameResult ==
    /\ \A a, b \in answers :
          (Def(a) /\ Def(b) /\ Req(a.c).id = Req(b.c).id /\ KeyOf(a.c) = KeyOf(b.c)
           /\ Dg(Req(a.c)) = Dg(Req(b.c)))
          => a.kind = b.kind /\ a.res = b.res
    /\ \A a \in answers :
          LET q == Req(a.c)  rec == RecOf(StateFor(a.c), q.key, q.id) IN
          (Def(a) /\ Has(rec) /\ rec.dg = Dg(q)) => a.kind = rec.rk /\ a.res = rec.res

\* OLTP-G02 and OLTP-G03 (O08, independent read R-01): a recorded result is
\* the operation's result on the state at its log position.
G03_RecordedPrecondition == \A r \in Ranges : ~st[r].bad

\* OLTP-G03 (D03): after one body wrote, another body of the same key and
\* ID gets a conflict, never a success.
G03_DigestConflict ==
    \A a \in answers :
        (a.kind = "ok" /\ a.res \in {"ok", "upd", "del"})
        => ~\E w \in StateFor(a.c).w :
              w.key = KeyOf(a.c) /\ w.id = Req(a.c).id /\ w.dg # Dg(Req(a.c))

\* OLTP-G04 (O10, O29, D05, D10): not_applied only with proof that no
\* attempt of the identity ever writes. Checked in every later state.
G04_NotAppliedHasProof == \A a \in answers : a.kind = "na" => ~Wrote(a.c)

-----------------------------------------------------------------------------
(* Liveness after the faults stop: the budget is finite and Busy turns     *)
(* false at the end. Fairness only on actions a timer, a retry loop or a   *)
(* background task drives; none on faults or client sends.                 *)

Fairness ==
    /\ \A c \in Clients :
          /\ WF_vars(GwResend(c))           \* PEER_ATTEMPT timer
          /\ WF_vars(GwDeadline(c))         \* the gateway deadline
    /\ WF_vars(\E x \in exec : Execute(x))           \* the executor task
    /\ WF_vars(\E x \in exec : ExecGiveUp(x))        \* the executor deadline (`by`)
    /\ WF_vars(\E x \in exec : ExecTimeout(x))       \* the executor deadline (`by`)
    /\ \A r \in Ranges :
          /\ WF_vars(RaftCommit(r))         \* AppendEntries and heartbeats
          /\ WF_vars(RaftFirstEntry(r))     \* OpenRaft's leader no-op
          /\ WF_vars(\E n \in Nodes : RaftElect(n, r))   \* the election timer
    /\ \A n \in Nodes : WF_vars(Restart(n))              \* the process supervisor
    /\ WF_vars(Heal)                                     \* the partition ends

LiveSpec == Spec /\ Fairness

\* OLTP-G10 (D05, O06): every proposal resolves and every call answers.
G10_ProposalsResolve == \A r \in Ranges : (prop[r] # <<>>) ~> (prop[r] = <<>>)
G10_CallsAnswered == \A c \in Clients : (gw[c].gs = "call") ~> (gw[c].gs # "call")

=============================================================================
