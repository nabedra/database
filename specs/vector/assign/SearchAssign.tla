-------------------------- MODULE SearchAssign --------------------------
(***************************************************************************)
(* Moves of a search replica between workers (plans/oltp-vector.md,        *)
(* section 7). Control assigns a range to workers under an epoch. A new    *)
(* replica syncs and catches up before Control publishes it. The replaced  *)
(* one drops its data after a grace period, by a message that can be late. *)
(* A range may move away and back. Gateways use stale routes.              *)
(*                                                                         *)
(* One range, its log a count of writes. The barrier is one step here: it  *)
(* and the leader are in SearchLatest.                                     *)
(*                                                                         *)
(* Properties: VEC-G01 (LatestRealTime), VEC-G08 (NeverServeEmpty,         *)
(* PublishedCaughtUp), VEC-G04 (Servable).                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Workers, Queries, MaxWrites, MaxMoves, MaxCrashes, MaxLost, None, Mutant

VARIABLES
    n,          \* committed and acknowledged writes
    epoch, owner,       \* Control: the published assignment
    next,       \* Control: [e, w] of a move in progress, or None
    ready,      \* Control: the new replica reported that it caught up, for this move
    ws,         \* ws[w]: [e, pos, whole, ok]: epoch of its assignment, 0 for none; position; it believes it
                \* holds the whole range; it does
    msgs,       \* assign, ready and drop messages
    route,      \* the gateway's cached owner
    q,          \* q[x]: [st, w, need, bar, got]
    caught,     \* epochs whose replica had caught up when they were published
    moves, crashes, lost

vars == <<n, epoch, owner, next, ready, ws, msgs, route, q, caught, moves, crashes, lost>>

First == CHOOSE w \in Workers : TRUE
NoReplica == [e |-> 0, pos |-> 0, whole |-> FALSE, ok |-> FALSE]

TypeOK ==
    /\ n \in 0..MaxWrites /\ epoch \in 1..(MaxMoves + 1) /\ owner \in Workers /\ route \in Workers
    /\ \A w \in Workers : ws[w].pos \in 0..n /\ ws[w].whole \in BOOLEAN
    /\ moves \in 0..MaxMoves /\ crashes \in 0..MaxCrashes /\ lost \in 0..MaxLost

Init ==
    /\ n = 0 /\ epoch = 1 /\ owner = First /\ next = None /\ ready = FALSE
    /\ ws = [w \in Workers |-> IF w = First THEN [e |-> 1, pos |-> 0, whole |-> TRUE, ok |-> TRUE] ELSE NoReplica]
    /\ msgs = {} /\ route = First /\ caught = {1}
    /\ q = [x \in Queries |-> [st |-> "idle", w |-> First, need |-> 0, bar |-> None, got |-> None]]
    /\ moves = 0 /\ crashes = 0 /\ lost = 0

Write ==
    /\ n < MaxWrites /\ n' = n + 1
    /\ UNCHANGED <<epoch, owner, next, ready, ws, msgs, route, q, caught, moves, crashes, lost>>

-----------------------------------------------------------------------------
(* Control.                                                                *)

StartMove(w) ==
    /\ next = None /\ w # owner /\ moves < MaxMoves /\ moves' = moves + 1
    /\ next' = [e |-> epoch + 1, w |-> w] /\ ready' = FALSE
    /\ msgs' = msgs \cup {[k |-> "assign", w |-> w, e |-> epoch + 1]}
    /\ UNCHANGED <<n, epoch, owner, ws, route, q, caught, crashes, lost>>

\* The assignment is repeated until the new replica reported.
Remind ==
    /\ next # None /\ ~ready
    /\ msgs' = msgs \cup {[k |-> "assign", w |-> next.w, e |-> next.e]}
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, route, q, caught, moves, crashes, lost>>

RecvReady ==
    /\ \E m \in msgs :
        /\ m.k = "ready" /\ msgs' = msgs \ {m}
        /\ ready' = (ready \/ (next # None /\ m.e = next.e /\ m.w = next.w))
    /\ UNCHANGED <<n, epoch, owner, next, ws, route, q, caught, moves, crashes, lost>>

\* Mutant publish_before_caught_up does not wait for the report.
Publish ==
    /\ next # None /\ (ready \/ Mutant = "publish_before_caught_up")
    /\ epoch' = next.e /\ owner' = next.w /\ next' = None /\ ready' = FALSE
    /\ caught' = IF ready THEN caught \cup {next.e} ELSE caught
    /\ msgs' = msgs \cup {[k |-> "drop", w |-> owner, e |-> epoch]}
    /\ UNCHANGED <<n, ws, route, q, moves, crashes, lost>>

-----------------------------------------------------------------------------
(* Workers.                                                                *)

RecvAssign(w) ==
    /\ \E m \in msgs :
        /\ m.k = "assign" /\ m.w = w /\ msgs' = msgs \ {m}
        /\ ws' = IF m.e > ws[w].e THEN [ws EXCEPT ![w] = [@ EXCEPT !.e = m.e]] ELSE ws
    /\ UNCHANGED <<n, epoch, owner, next, ready, route, q, caught, moves, crashes, lost>>

\* A range snapshot, or what it still has from an earlier assignment.
Sync(w) ==
    /\ ws[w].e # 0 /\ ~ws[w].whole
    /\ \E p \in 0..n : ws' = [ws EXCEPT ![w].pos = p, ![w].whole = TRUE, ![w].ok = TRUE]
    /\ UNCHANGED <<n, epoch, owner, next, ready, msgs, route, q, caught, moves, crashes, lost>>

Pull(w) ==
    /\ ws[w].whole /\ ws[w].pos < n
    /\ ws' = [ws EXCEPT ![w].pos = @ + 1]
    /\ UNCHANGED <<n, epoch, owner, next, ready, msgs, route, q, caught, moves, crashes, lost>>

\* It reached a fresh barrier.
Report(w) ==
    /\ ws[w].whole /\ ws[w].pos = n /\ next # None /\ next.w = w /\ ws[w].e = next.e /\ ~ready
    /\ msgs' = msgs \cup {[k |-> "ready", w |-> w, e |-> ws[w].e]}
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, route, q, caught, moves, crashes, lost>>

\* The grace period passed. The drop names the epoch it ends, so a late one cannot hit a later
\* assignment. Mutant drop_without_epoch deletes the data of a later one, which still counts it as held.
RecvDrop(w) ==
    /\ \E m \in msgs :
        /\ m.k = "drop" /\ m.w = w /\ msgs' = msgs \ {m}
        /\ ws' = CASE m.e = ws[w].e -> [ws EXCEPT ![w] = NoReplica]
                   [] Mutant = "drop_without_epoch" -> [ws EXCEPT ![w].ok = FALSE]
                   [] OTHER -> ws
    /\ UNCHANGED <<n, epoch, owner, next, ready, route, q, caught, moves, crashes, lost>>

\* The disk is lost. The worker keeps its assignment and syncs again.
CrashWorker(w) ==
    /\ ws[w].whole /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ ws' = [ws EXCEPT ![w].whole = FALSE, ![w].pos = 0, ![w].ok = FALSE]
    /\ UNCHANGED <<n, epoch, owner, next, ready, msgs, route, q, caught, moves, lost>>

LoseMsg ==
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ \E m \in msgs : msgs' = msgs \ {m}
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, route, q, caught, moves, crashes>>

-----------------------------------------------------------------------------
(* Queries.                                                                *)

GatewayRefreshes ==
    /\ route # owner /\ route' = owner
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, msgs, q, caught, moves, crashes, lost>>

StartQuery(x) ==
    /\ q[x].st = "idle"
    /\ q' = [q EXCEPT ![x] = [st |-> "run", w |-> route, need |-> n, bar |-> None, got |-> None]]
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, msgs, route, caught, moves, crashes, lost>>

Barrier(x) ==
    /\ q[x].st = "run" /\ q[x].bar = None
    /\ q' = [q EXCEPT ![x].bar = n]
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, msgs, route, caught, moves, crashes, lost>>

\* A worker with the whole range at the barrier serves, replaced or not.
\* Mutants: old_replica_serves_empty serves with whatever is there, serve_before_whole serves during
\* a sync.
Serve(x) ==
    LET w == q[x].w IN
    /\ q[x].st = "run" /\ q[x].bar # None
    /\ \/ ws[w].whole /\ ws[w].pos >= q[x].bar
       \/ Mutant = "old_replica_serves_empty" /\ ws[w].e = 0
       \/ Mutant = "serve_before_whole" /\ ws[w].e # 0
    /\ q' = [q EXCEPT ![x].st = "done", ![x].got = [pos |-> ws[w].pos, whole |-> ws[w].ok]]
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, msgs, route, caught, moves, crashes, lost>>

\* NotHosted or a deadline: the gateway refreshes its route and asks again. Mutant no_refresh keeps
\* the stale route for good.
Retry(x) ==
    /\ q[x].st = "run" /\ ws[q[x].w].e = 0
    /\ q[x].w # owner
    /\ q' = [q EXCEPT ![x].w = IF Mutant = "no_refresh" THEN q[x].w ELSE owner, ![x].bar = None]
    /\ UNCHANGED <<n, epoch, owner, next, ready, ws, msgs, route, caught, moves, crashes, lost>>

Terminated == (\A x \in Queries : q[x].st = "done") /\ UNCHANGED vars

Next ==
    \/ Write \/ Remind \/ RecvReady \/ Publish \/ LoseMsg \/ GatewayRefreshes
    \/ \E w \in Workers : StartMove(w) \/ RecvAssign(w) \/ Sync(w) \/ Pull(w) \/ Report(w) \/ RecvDrop(w) \/ CrashWorker(w)
    \/ \E x \in Queries : StartQuery(x) \/ Barrier(x) \/ Serve(x) \/ Retry(x)
    \/ Terminated

\* Control's reconcile loop, the worker's sync and tail loops, the gateway's retry.
Fairness ==
    /\ SF_vars(RecvReady) /\ WF_vars(Publish) /\ WF_vars(Remind)
    /\ \A w \in Workers : SF_vars(RecvAssign(w)) /\ WF_vars(Sync(w)) /\ WF_vars(Pull(w)) /\ WF_vars(Report(w))
    /\ \A x \in Queries : WF_vars(Barrier(x)) /\ SF_vars(Serve(x)) /\ SF_vars(Retry(x))

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------

\* VEC-G01: a query sees every write acknowledged before it began, whoever serves it.
LatestRealTime == \A x \in Queries : q[x].st = "done" => q[x].got.pos >= q[x].need

\* VEC-G08: nobody serves a range it does not hold whole.
NeverServeEmpty == \A x \in Queries : q[x].st = "done" => q[x].got.whole

\* VEC-G08: a published replica had caught up.
PublishedCaughtUp == epoch \in caught

\* VEC-G04: once faults stop, every query is answered.
Servable == \A x \in Queries : (q[x].st = "run") ~> (q[x].st = "done")

=============================================================================
