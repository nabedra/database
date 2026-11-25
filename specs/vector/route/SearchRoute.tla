--------------------------- MODULE SearchRoute ---------------------------
(***************************************************************************)
(* Coverage and splits (plans/oltp-vector.md, sections 3, 4 and 7).        *)
(*                                                                         *)
(* One range G0 holds keys k1 and k2 and splits into G1 (k1) and G2 (k2).  *)
(* Freeze, copy at the cut, publish in the Catalog, finish the source. A   *)
(* search worker holds G0 and re-tags its rows to the children. Gateways   *)
(* may use any route, current, old or half refreshed. Leader changes and   *)
(* the feed are in SearchLatest and SearchSnapshot.                        *)
(*                                                                         *)
(* Properties: VEC-G04 (ExactCoverage), VEC-G01 (LatestRealTime),          *)
(* VEC-G02 (SnapshotExact).                                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Queries, MaxWrites, MaxCloses, MaxCrashes, None, Mutant

Keys == {"k1", "k2"}
Groups == {"G0", "G1", "G2"}
KeysOf(g) == CASE g = "G0" -> Keys [] g = "G1" -> {"k1"} [] g = "G2" -> {"k2"}
Child(k) == IF k = "k1" THEN "G1" ELSE "G2"

VARIABLES
    tso,
    log,        \* log[g]: entries [k, ts] of rows and [k |-> "closed", ts] of Closed entries
    pend,       \* pend[g]: a write that took its timestamp, not yet proposed, or None
    frozen, cut,\* the source is frozen, at this log length
    copied,     \* children imported the source at the cut
    published,  \* Catalog: the route is the children
    finished,   \* the source learned that the split is published
    acked,      \* acknowledged [k, ts]
    writes, closes,
    wpos,       \* wpos[g]: entries of g the worker applied
    tagged,     \* the worker moved the source's rows to the children
    tagBase,    \* source entries it held when it re-tagged
    knows,      \* the worker's catalog cache says the split is published
    qst, qmode, qroute, qneed, qs, qans,
    crashes

vars == <<tso, log, pend, frozen, cut, copied, published, finished, acked, writes, closes, wpos, tagged,
          tagBase, knows, qst, qmode, qroute, qneed, qs, qans, crashes>>
wr == <<tso, log, pend, frozen, cut, copied, published, finished, acked, writes, closes>>
wk == <<wpos, tagged, tagBase, knows>>
qr == <<qst, qmode, qroute, qneed, qs, qans>>

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Rows(seq, k) == {seq[i].ts : i \in {j \in 1..Len(seq) : seq[j].k = k}}
FloorOf(seq) == Max(Rows(seq, "closed"))
Prefix(seq, n) == SubSeq(seq, 1, n)
Routes == {{"G0"}, {"G1", "G2"}, {"G1"}, {"G2"}, {"G0", "G1"}, {"G0", "G2"}}

\* Every version of a key that will ever be committed so far.
History(k) == Rows(log["G0"], k) \cup Rows(log[Child(k)], k)

TypeOK ==
    /\ tso \in Nat /\ writes \in 0..MaxWrites /\ closes \in 0..MaxCloses /\ crashes \in 0..MaxCrashes
    /\ \A g \in Groups : wpos[g] \in 0..Len(log[g])
    /\ frozen \in BOOLEAN /\ copied \in BOOLEAN /\ published \in BOOLEAN /\ finished \in BOOLEAN
    /\ tagged \in BOOLEAN /\ knows \in BOOLEAN
    /\ qst \in [Queries -> {"idle", "run", "done", "failed"}]

Init ==
    /\ tso = 0
    /\ log = [g \in Groups |-> <<>>]
    /\ pend = [g \in Groups |-> None]
    /\ frozen = FALSE /\ cut = 0 /\ copied = FALSE /\ published = FALSE /\ finished = FALSE
    /\ acked = {} /\ writes = 0 /\ closes = 0
    /\ wpos = [g \in Groups |-> 0]
    /\ tagged = FALSE /\ tagBase = 0 /\ knows = FALSE
    /\ qst = [q \in Queries |-> "idle"]
    /\ qmode = [q \in Queries |-> None]
    /\ qroute = [q \in Queries |-> {}]
    /\ qneed = [q \in Queries |-> {}]
    /\ qs = [q \in Queries |-> None]
    /\ qans = [q \in Queries |-> [g \in Groups |-> None]]
    /\ crashes = 0

Open(g) == IF g = "G0" THEN ~frozen ELSE published

-----------------------------------------------------------------------------
(* Writes and the split.                                                   *)

TakeTs(g, k) ==
    /\ Open(g) /\ k \in KeysOf(g) /\ pend[g] = None /\ writes < MaxWrites
    /\ writes' = writes + 1 /\ tso' = tso + 1
    /\ pend' = [pend EXCEPT ![g] = [k |-> k, ts |-> tso + 1]]
    /\ UNCHANGED <<log, frozen, cut, copied, published, finished, acked, closes, wk, qr, crashes>>

\* Apply refuses a write to a frozen range and one at or below the floor; the client retries.
CommitWrite(g) ==
    /\ pend[g] # None
    /\ pend' = [pend EXCEPT ![g] = None]
    /\ IF Open(g) /\ pend[g].ts > FloorOf(log[g])
       THEN /\ log' = [log EXCEPT ![g] = Append(@, pend[g])]
            /\ acked' = acked \cup {pend[g]}
       ELSE UNCHANGED <<log, acked>>
    /\ UNCHANGED <<tso, frozen, cut, copied, published, finished, writes, closes, wk, qr, crashes>>

\* A child closes only after publication, an unfrozen source any allocated timestamp. Apply refuses
\* Closed on a frozen source. Mutant prefix_frozen_source_closes: the source keeps closing.
MayClose(g) == Open(g) \/ (Mutant = "prefix_frozen_source_closes" /\ g = "G0")

Close(g) ==
    /\ MayClose(g) /\ closes < MaxCloses /\ closes' = closes + 1
    /\ \E c \in (FloorOf(log[g]) + 1)..tso : log' = [log EXCEPT ![g] = Append(@, [k |-> "closed", ts |-> c])]
    /\ UNCHANGED <<tso, pend, frozen, cut, copied, published, finished, acked, writes, wk, qr, crashes>>

Freeze ==
    /\ ~frozen /\ frozen' = TRUE /\ cut' = Len(log["G0"])
    /\ UNCHANGED <<tso, log, pend, copied, published, finished, acked, writes, closes, wk, qr, crashes>>

Copy ==
    /\ frozen /\ ~copied /\ copied' = TRUE
    /\ UNCHANGED <<tso, log, pend, frozen, cut, published, finished, acked, writes, closes, wk, qr, crashes>>

\* Publication takes a timestamp, so every write of a child is above it.
Publish ==
    /\ copied /\ ~published /\ published' = TRUE /\ tso' = tso + 1
    /\ UNCHANGED <<log, pend, frozen, cut, copied, finished, acked, writes, closes, wk, qr, crashes>>

FinishSource ==
    /\ published /\ ~finished /\ finished' = TRUE
    /\ UNCHANGED <<tso, log, pend, frozen, cut, copied, published, acked, writes, closes, wk, qr, crashes>>

-----------------------------------------------------------------------------
(* The search worker.                                                      *)

Pull(g) ==
    /\ wpos[g] < Len(log[g])
    /\ g # "G0" => tagged
    /\ wpos' = [wpos EXCEPT ![g] = @ + 1]
    /\ UNCHANGED <<wr, tagged, tagBase, knows, qr, crashes>>

LearnSplit ==
    /\ published /\ ~knows /\ knows' = TRUE
    /\ UNCHANGED <<wr, wpos, tagged, tagBase, qr, crashes>>

\* Re-tag only when every source entry up to the cut is here; otherwise install the children.
\* Mutant retag_before_cut does it at once.
Retag ==
    /\ knows /\ ~tagged
    /\ wpos["G0"] >= cut \/ Mutant = "retag_before_cut"
    /\ tagged' = TRUE /\ tagBase' = wpos["G0"]
    /\ UNCHANGED <<wr, wpos, knows, qr, crashes>>

InstallChildren ==
    /\ knows /\ ~tagged
    /\ tagged' = TRUE /\ tagBase' = cut
    /\ wpos' = [wpos EXCEPT !["G0"] = cut]
    /\ UNCHANGED <<wr, knows, qr, crashes>>

\* A crash without the disk. The worker forgets what it knew and starts over.
CrashWorker ==
    /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ wpos' = [g \in Groups |-> 0] /\ tagged' = FALSE /\ tagBase' = 0 /\ knows' = FALSE
    /\ UNCHANGED <<wr, qr>>

Holds(g) == IF g = "G0" THEN ~tagged ELSE tagged

\* Rows of key k that the worker holds under group g.
Local(g, k) ==
    IF g = "G0" THEN Rows(Prefix(log["G0"], wpos["G0"]), k)
    ELSE Rows(Prefix(log["G0"], tagBase), k) \cup Rows(Prefix(log[g], wpos[g]), k)

WorkerFloor(g) == FloorOf(Prefix(log[g], wpos[g]))

-----------------------------------------------------------------------------
(* Queries. The route may be any set of ranges a gateway can have cached.  *)

StartQuery(q, m, R) ==
    /\ qst[q] = "idle"
    /\ qst' = [qst EXCEPT ![q] = "run"]
    /\ qmode' = [qmode EXCEPT ![q] = m]
    /\ qroute' = [qroute EXCEPT ![q] = R]
    /\ qneed' = [qneed EXCEPT ![q] = acked]
    /\ IF m = "snapshot"
       THEN LET floors == {WorkerFloor(g) : g \in R}
                low == CHOOSE f \in floors : \A h \in floors : f <= h IN
            \E s \in 1..low : qs' = [qs EXCEPT ![q] = s]
       ELSE UNCHANGED qs
    /\ UNCHANGED <<wr, wk, qans, crashes>>

\* Barrier and scan of one range. The leader of a frozen source first confirms through the Catalog
\* that the split is unpublished. Mutant frozen_source_serves skips that. Children answer after
\* publication only.
BarrierOk(g) ==
    IF g = "G0" THEN ~finished /\ (frozen => ~published \/ Mutant = "frozen_source_serves")
    ELSE published

\* The barrier position is taken now; the scan needs the worker to have reached it. A row version is
\* what the worker holds, the newest wins.
AnswerLatest(q, g) ==
    /\ qst[q] = "run" /\ qmode[q] = "latest" /\ g \in qroute[q] /\ qans[q][g] = None
    /\ BarrierOk(g) /\ Holds(g) /\ wpos[g] = Len(log[g])
    /\ qans' = [qans EXCEPT ![q][g] = [k \in KeysOf(g) |-> Max(Local(g, k))]]
    /\ UNCHANGED <<wr, wk, qst, qmode, qroute, qneed, qs, crashes>>

AnswerSnapshot(q, g) ==
    /\ qst[q] = "run" /\ qmode[q] = "snapshot" /\ g \in qroute[q] /\ qans[q][g] = None
    /\ Holds(g) /\ qs[q] <= WorkerFloor(g)
    /\ qans' = [qans EXCEPT ![q][g] = [k \in KeysOf(g) |-> Max({v \in Local(g, k) : v <= qs[q]})]]
    /\ UNCHANGED <<wr, wk, qst, qmode, qroute, qneed, qs, crashes>>

\* The spans tile the requested keys exactly. Mutants accept a gap or an overlap.
Covers(R, k) == Cardinality({g \in R : k \in KeysOf(g)})
Tiles(R) ==
    \A k \in Keys : /\ Covers(R, k) >= 1 \/ Mutant = "accept_span_gap"
                    /\ Covers(R, k) <= 1 \/ Mutant = "accept_overlap"

FinishQuery(q) ==
    /\ qst[q] = "run" /\ \A g \in qroute[q] : qans[q][g] # None
    /\ qst' = [qst EXCEPT ![q] = IF Tiles(qroute[q]) THEN "done" ELSE "failed"]
    /\ UNCHANGED <<wr, wk, qmode, qroute, qneed, qs, qans, crashes>>

\* A stale route, a missing replica or a deadline: an explicit error.
FailQuery(q) ==
    /\ qst[q] = "run"
    /\ qst' = [qst EXCEPT ![q] = "failed"]
    /\ UNCHANGED <<wr, wk, qmode, qroute, qneed, qs, qans, crashes>>

Next ==
    \/ \E g \in Groups, k \in Keys : TakeTs(g, k)
    \/ \E g \in Groups : CommitWrite(g) \/ Close(g) \/ Pull(g)
    \/ Freeze \/ Copy \/ Publish \/ FinishSource
    \/ LearnSplit \/ Retag \/ InstallChildren \/ CrashWorker
    \/ \E q \in Queries :
        \/ \E m \in {"latest", "snapshot"}, R \in Routes : StartQuery(q, m, R)
        \/ \E g \in Groups : AnswerLatest(q, g) \/ AnswerSnapshot(q, g)
        \/ FinishQuery(q) \/ FailQuery(q)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* VEC-G04: a successful result covers every key exactly once.
ExactCoverage == \A q \in Queries : qst[q] = "done" => \A k \in Keys : Covers(qroute[q], k) = 1

Answer(q, k) == LET g == CHOOSE x \in qroute[q] : k \in KeysOf(x) IN qans[q][g][k]

\* VEC-G01 across a split.
LatestRealTime ==
    \A q \in Queries : qst[q] = "done" /\ qmode[q] = "latest" =>
        \A w \in qneed[q] : Covers(qroute[q], w.k) >= 1 => Answer(q, w.k) >= w.ts

\* VEC-G02 across a split: the cut never changes after it was served.
SnapshotExact ==
    \A q \in Queries : qst[q] = "done" /\ qmode[q] = "snapshot" =>
        \A k \in Keys : Covers(qroute[q], k) >= 1 => Answer(q, k) = Max({v \in History(k) : v <= qs[q]})

=============================================================================
