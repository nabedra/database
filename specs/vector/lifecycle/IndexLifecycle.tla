------------------------- MODULE IndexLifecycle -------------------------
(***************************************************************************)
(* Build, publish, drop and re-add of a search column                      *)
(* (plans/oltp-vector.md, section 7). A column is known to users by name.  *)
(* Every add makes a new incarnation, which is never reused. Control sends *)
(* each range a fence with a growing schema epoch and the incarnation it   *)
(* serves. Fences can be late and repeated. The barrier carries the        *)
(* incarnation. A split and a fence exclude each other through the         *)
(* existing lifecycle guard, so a child inherits its parent's epoch.       *)
(*                                                                         *)
(* Properties: VEC-G08 (NeverServeWrongColumn, NeverServeUnready,          *)
(* PublishedComplete, BuildTerminates).                                    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Ranges, Workers, Queries, MaxCols, MaxFailovers, MaxLosses, MaxLostReports, MaxDrops, None, Mutant

Cols == 1..MaxCols

VARIABLES
    col,        \* the current incarnation in the Catalog, 0 before the first
    status,     \* "none", "building", "ready", "dropping" or "dropped"
    se,         \* Control: the last schema epoch it assigned
    fences,     \* fences sent: [r, se, c]; they stay, so they can arrive late and again
    epoch,      \* epoch[r]: [se, c]: schema epoch of the Data group, and the incarnation it serves or 0
    ws,         \* ws[s][r]: [c, st] with st in {"none", "syncing", "ready"}
    msgs,       \* ready reports to Control
    seen,       \* Control memory: ready reports of the current build
    ever,       \* every (c, r) that was ever ready on some worker
    complete,   \* incarnations that were complete when published
    gw,         \* the gateway's cached incarnation, 0 for none
    q,          \* q[x]: [st, c, live], live: incarnations a user may see in this query
    served,     \* [c, live, st]: what a worker served, for the properties
    failovers, losses, lostReports, drops

vars == <<col, status, se, fences, epoch, ws, msgs, seen, ever, complete, gw, q, served, failovers, losses, lostReports, drops>>

NoIdx == [c |-> 0, st |-> "none"]

TypeOK ==
    /\ col \in 0..MaxCols
    /\ status \in {"none", "building", "publishing", "ready", "dropping", "dropped"}
    /\ \A r \in Ranges : epoch[r].c \in 0..MaxCols /\ epoch[r].se \in 0..se
    /\ gw \in 0..MaxCols
    /\ failovers \in 0..MaxFailovers /\ losses \in 0..MaxLosses /\ drops \in 0..MaxDrops

Init ==
    /\ col = 0 /\ status = "none" /\ se = 0 /\ fences = {}
    /\ epoch = [r \in Ranges |-> [se |-> 0, c |-> 0]]
    /\ ws = [s \in Workers |-> [r \in Ranges |-> NoIdx]]
    /\ msgs = {} /\ seen = {} /\ ever = {} /\ complete = {} /\ gw = 0
    /\ q = [x \in Queries |-> [st |-> "idle", c |-> 0, live |-> {}]]
    /\ served = {}
    /\ failovers = 0 /\ losses = 0 /\ lostReports = 0 /\ drops = 0

-----------------------------------------------------------------------------
(* Control and the Catalog.                                                *)

\* Every Data group acknowledged the current schema epoch.
Fenced == \A r \in Ranges : epoch[r].se >= se
SendFences(c) ==
    /\ se' = se + 1
    /\ fences' = fences \cup {[r |-> r, se |-> se + 1, c |-> c] : r \in Ranges}

AddColumn ==
    /\ status \in {"none", "dropped"} /\ col < MaxCols
    /\ col' = col + 1 /\ status' = "building" /\ seen' = {}
    /\ UNCHANGED <<se, fences, epoch, ws, msgs, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

\* A report counts for the incarnation it names. Mutant report_without_column counts any.
RecvReady ==
    /\ \E m \in msgs :
        /\ msgs' = msgs \ {m}
        /\ seen' = IF status = "building" /\ (m.c = col \/ Mutant = "report_without_column")
                   THEN seen \cup {[s |-> m.s, r |-> m.r]} ELSE seen
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

AllReady ==
    IF Mutant = "publish_before_all_ready" THEN seen # {}
    ELSE \A r \in Ranges : \E s \in Workers : [s |-> s, r |-> r] \in seen

\* Every range has a ready replica: first every Data group gets the epoch, then the Catalog publishes.
StartPublish ==
    /\ status = "building" /\ AllReady
    /\ status' = "publishing" /\ SendFences(col)
    /\ UNCHANGED <<col, epoch, ws, msgs, seen, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

\* The Data group takes a fence of a larger schema epoch only.
\* Mutant fence_not_monotone takes any.
FenceRange(r) ==
    /\ \E f \in fences :
        /\ f.r = r
        /\ f.se > epoch[r].se \/ (Mutant = "fence_not_monotone" /\ f.se # epoch[r].se)
        /\ epoch' = [epoch EXCEPT ![r] = [se |-> f.se, c |-> f.c]]
    /\ UNCHANGED <<col, status, se, fences, ws, msgs, seen, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

Publish ==
    /\ status = "publishing" /\ Fenced
    /\ status' = "ready"
    /\ complete' = IF \A r \in Ranges : [c |-> col, r |-> r] \in ever THEN complete \cup {col} ELSE complete
    /\ UNCHANGED <<col, se, fences, epoch, ws, msgs, seen, ever, gw, q, served, failovers, losses, drops, lostReports>>

\* A drop fences every Data group before the Catalog drops the name.
StartDrop ==
    /\ status \in {"building", "ready"} /\ drops < MaxDrops /\ drops' = drops + 1
    /\ status' = "dropping" /\ SendFences(0)
    /\ UNCHANGED <<col, epoch, ws, msgs, seen, ever, complete, gw, q, served, failovers, losses, lostReports>>

\* Mutant drop_without_fence publishes the drop first.
FinishDrop ==
    /\ status = "dropping" /\ (Fenced \/ Mutant = "drop_without_fence")
    /\ status' = "dropped"
    /\ UNCHANGED <<col, se, fences, epoch, ws, msgs, seen, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

\* A new Control leader has no reports and keeps the durable state.
Failover ==
    /\ failovers < MaxFailovers /\ failovers' = failovers + 1
    /\ seen' = {}
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, msgs, ever, complete, gw, q, served, losses, drops, lostReports>>

-----------------------------------------------------------------------------
(* Search workers learn the Catalog late.                                  *)

StartSync(s, r, c) ==
    /\ c \in 1..col /\ c >= ws[s][r].c
    /\ c # ws[s][r].c \/ ws[s][r].st = "none"
    /\ c = col => status \notin {"dropped"}
    /\ ws' = [ws EXCEPT ![s][r] = [c |-> c, st |-> "syncing"]]
    /\ UNCHANGED <<col, status, se, fences, epoch, msgs, seen, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

FinishSync(s, r) ==
    /\ ws[s][r].st = "syncing"
    /\ ws' = [ws EXCEPT ![s][r].st = "ready"]
    /\ ever' = ever \cup {[c |-> ws[s][r].c, r |-> r]}
    /\ msgs' = msgs \cup {[s |-> s, r |-> r, c |-> ws[s][r].c]}
    /\ UNCHANGED <<col, status, se, fences, epoch, seen, complete, gw, q, served, failovers, losses, drops, lostReports>>

\* The heartbeat repeats the report until Control has it. Mutant no_build_retry reports once.
ReportAgain(s, r) ==
    /\ Mutant # "no_build_retry"
    /\ ws[s][r].st = "ready" /\ ws[s][r].c = col /\ status = "building"
    /\ [s |-> s, r |-> r] \notin seen
    /\ [s |-> s, r |-> r, c |-> col] \notin msgs
    /\ msgs' = msgs \cup {[s |-> s, r |-> r, c |-> col]}
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, seen, ever, complete, gw, q, served, failovers, losses, drops, lostReports>>

LoseDisk(s, r) ==
    /\ losses < MaxLosses /\ losses' = losses + 1 /\ ws[s][r].st # "none"
    /\ ws' = [ws EXCEPT ![s][r].st = "none"]
    /\ UNCHANGED <<col, status, se, fences, epoch, msgs, seen, ever, complete, gw, q, served, failovers, drops, lostReports>>

LoseReport ==
    /\ lostReports < MaxLostReports /\ lostReports' = lostReports + 1
    /\ \E m \in msgs : msgs' = msgs \ {m}
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, seen, ever, complete, gw, q, served, failovers, losses, drops>>

-----------------------------------------------------------------------------
(* Queries.                                                                *)

GatewayRefreshes ==
    /\ status = "ready" /\ gw # col
    /\ gw' = col
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, msgs, seen, ever, complete, q, served, failovers, losses, drops, lostReports>>

\* Incarnations a query that runs now may see: the current one unless the drop finished.
LiveNow == IF status \in {"ready", "dropping"} /\ col \in complete THEN {col} ELSE {}

\* The gateway sends the incarnation it has cached, which may be gone.
StartQuery(x) ==
    /\ q[x].st = "idle" /\ gw # 0
    /\ q' = [q EXCEPT ![x] = [st |-> "run", c |-> gw, live |-> LiveNow]]
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, msgs, seen, ever, complete, gw, served, failovers, losses, drops, lostReports>>

\* A column that is published while the query runs may be seen too.
Widen == [x \in Queries |-> IF q[x].st = "run" THEN [q[x] EXCEPT !.live = @ \cup LiveNow] ELSE q[x]]

\* The worker serves the incarnation asked for, if it is ready, after a barrier in which the Data
\* leader confirms that incarnation. Mutants: serve_by_name serves whatever the worker has under the
\* name, serve_while_syncing does not wait, prefix_barrier_without_schema skips the leader's check.
ServeRange(x, s, r) ==
    /\ q[x].st = "run"
    /\ ws[s][r].c = q[x].c \/ Mutant = "serve_by_name"
    /\ ws[s][r].st = "ready" \/ (Mutant = "serve_while_syncing" /\ ws[s][r].st = "syncing")
    /\ epoch[r].c = q[x].c \/ Mutant = "prefix_barrier_without_schema"
    /\ served' = served \cup {[c |-> ws[s][r].c, live |-> Widen[x].live, st |-> ws[s][r].st, want |-> q[x].c]}
    /\ q' = [q EXCEPT ![x].st = "done"]
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, msgs, seen, ever, complete, gw, failovers, losses, drops, lostReports>>

\* Any refusal is an explicit error.
FailQuery(x) ==
    /\ q[x].st = "run"
    /\ q' = [q EXCEPT ![x].st = "done"]
    /\ UNCHANGED <<col, status, se, fences, epoch, ws, msgs, seen, ever, complete, gw, served, failovers, losses, drops, lostReports>>

Terminated == col = MaxCols /\ status \in {"ready", "dropped"} /\ UNCHANGED vars

Next ==
    \/ AddColumn \/ StartPublish \/ Publish \/ StartDrop \/ FinishDrop \/ Failover \/ GatewayRefreshes
    \/ \E r \in Ranges : FenceRange(r)
    \/ RecvReady \/ LoseReport
    \/ \E s \in Workers, r \in Ranges :
        \/ \E c \in Cols : StartSync(s, r, c)
        \/ FinishSync(s, r) \/ ReportAgain(s, r) \/ LoseDisk(s, r)
    \/ Terminated
    \/ \E x \in Queries :
        \/ StartQuery(x) \/ FailQuery(x)
        \/ \E s \in Workers, r \in Ranges : ServeRange(x, s, r)

\* Worker sync loops and heartbeats, Control's reconcile loop.
Fairness ==
    /\ \A s \in Workers, r \in Ranges :
        /\ \A c \in Cols : WF_vars(c = col /\ StartSync(s, r, c))
        /\ WF_vars(FinishSync(s, r)) /\ WF_vars(ReportAgain(s, r))
    /\ WF_vars(RecvReady)
    /\ WF_vars(StartPublish) /\ WF_vars(Publish) /\ WF_vars(FinishDrop)
    /\ \A r \in Ranges : WF_vars(FenceRange(r))

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------

\* VEC-G08: what was served is an incarnation that the query's user could see while it ran.
NeverServeWrongColumn == \A v \in served : v.c \in v.live

\* VEC-G08: from a complete index.
NeverServeUnready == \A v \in served : v.st = "ready"

\* VEC-G08: a published column had every range ready.
PublishedComplete == status = "ready" => col \in complete

\* VEC-G08: a build ends, published or dropped.
BuildTerminates == (status = "building") ~> (status \in {"ready", "dropped"})

=============================================================================
