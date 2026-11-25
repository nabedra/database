--------------------------- MODULE SearchRoute ---------------------------
(***************************************************************************)
(* Coverage and splits (plans/oltp-vector.md, sections 3, 4 and 7).        *)
(*                                                                         *)
(* One range G0 holds keys k1 and k2 and splits into G1 (k1) and G2 (k2):  *)
(* freeze, copy at the cut, publish in the Catalog, finish the source. A   *)
(* search worker holds G0, may have dropped superseded versions, and       *)
(* re-tags its rows to the children. Gateways may use any route, current,  *)
(* old or half refreshed, and a worker may answer for fewer ranges than it *)
(* was asked. Every property is per key, and a key is answered by one      *)
(* worker, so one worker stands for all. Leader changes and the feed are   *)
(* in SearchLatest and SearchSnapshot, moves between workers in            *)
(* SearchAssign. A freeze refuses while an intent is open                  *)
(* (lineage.rs, existing), so no transaction crosses the cut.              *)
(*                                                                         *)
(* Properties: VEC-G04 (ExactCoverage), VEC-G01 (LatestRealTime),          *)
(* VEC-G02 (SnapshotExact).                                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Queries, MaxWrites, MaxCloses, MaxCrashes, MaxForgets, MaxTs, None, Mutant

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
    base,       \* entries of G0 up to here kept only their newest version
    bound,      \* the smallest cut the worker serves
    tagged,     \* the worker moved the source's rows to the children
    tagBase,    \* source entries it held when it re-tagged
    knows,      \* the worker's catalog cache says the split is published
    qst, qmode, qroute, qneed, qs, qbar, qans,
    crashes, forgets

wr == <<tso, log, pend, frozen, cut, copied, published, finished, acked, writes, closes>>
wk == <<wpos, base, bound, tagged, tagBase, knows>>
qr == <<qst, qmode, qroute, qneed, qs, qbar, qans>>
fl == <<crashes, forgets>>
vars == <<wr, wk, qr, fl>>

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Rows(seq, k) == {seq[i].ts : i \in {j \in 1..Len(seq) : seq[j].k = k}}
AllTs(seq) == Rows(seq, "k1") \cup Rows(seq, "k2")
FloorOf(seq) == Max(Rows(seq, "closed"))
Prefix(seq, n) == SubSeq(seq, 1, n)
Routes == {{"G0"}, {"G1", "G2"}, {"G1"}, {"G2"}, {"G0", "G1"}, {"G0", "G2"}}

\* Every version of a key that was ever committed.
History(k) == Rows(log["G0"], k) \cup Rows(log[Child(k)], k)

TypeOK ==
    /\ tso \in 0..MaxTs /\ writes \in 0..MaxWrites /\ closes \in 0..MaxCloses
    /\ crashes \in 0..MaxCrashes /\ forgets \in 0..MaxForgets
    /\ \A g \in Groups : wpos[g] \in 0..Len(log[g])
    /\ frozen \in BOOLEAN /\ copied \in BOOLEAN /\ published \in BOOLEAN /\ finished \in BOOLEAN
    /\ tagged \in BOOLEAN /\ knows \in BOOLEAN /\ base \in 0..Len(log["G0"])
    /\ qst \in [Queries -> {"idle", "run", "done", "failed"}]

Init ==
    /\ tso = 0
    /\ log = [g \in Groups |-> <<>>]
    /\ pend = [g \in Groups |-> None]
    /\ frozen = FALSE /\ cut = 0 /\ copied = FALSE /\ published = FALSE /\ finished = FALSE
    /\ acked = {} /\ writes = 0 /\ closes = 0
    /\ wpos = [g \in Groups |-> 0] /\ base = 0 /\ bound = 0
    /\ tagged = FALSE /\ tagBase = 0 /\ knows = FALSE
    /\ qst = [q \in Queries |-> "idle"]
    /\ qmode = [q \in Queries |-> None]
    /\ qroute = [q \in Queries |-> {}]
    /\ qneed = [q \in Queries |-> {}]
    /\ qs = [q \in Queries |-> None]
    /\ qbar = [q \in Queries |-> [g \in Groups |-> None]]
    /\ qans = [q \in Queries |-> [g \in Groups |-> None]]
    /\ crashes = 0 /\ forgets = 0

Open(g) == IF g = "G0" THEN ~frozen ELSE published

-----------------------------------------------------------------------------
(* Writes and the split.                                                   *)

TakeTs(g, k) ==
    /\ Open(g) /\ k \in KeysOf(g) /\ pend[g] = None /\ writes < MaxWrites
    /\ writes' = writes + 1 /\ tso' = tso + 1
    /\ pend' = [pend EXCEPT ![g] = [k |-> k, ts |-> tso + 1]]
    /\ UNCHANGED <<log, frozen, cut, copied, published, finished, acked, closes, wk, qr, fl>>

\* Apply refuses a write to a frozen range and one at or below the floor; the client retries.
CommitWrite(g) ==
    /\ pend[g] # None
    /\ pend' = [pend EXCEPT ![g] = None]
    /\ IF Open(g) /\ pend[g].ts > FloorOf(log[g])
       THEN /\ log' = [log EXCEPT ![g] = Append(@, pend[g])]
            /\ acked' = acked \cup {pend[g]}
       ELSE UNCHANGED <<log, acked>>
    /\ UNCHANGED <<tso, frozen, cut, copied, published, finished, writes, closes, wk, qr, fl>>

\* A child closes only after publication, an unfrozen source any allocated timestamp. Apply refuses
\* Closed on a frozen source. Mutant prefix_frozen_source_closes: the source keeps closing.
MayClose(g) == Open(g) \/ (Mutant = "prefix_frozen_source_closes" /\ g = "G0")

Close(g, c) ==
    /\ MayClose(g) /\ closes < MaxCloses /\ closes' = closes + 1
    /\ c > FloorOf(log[g]) /\ c <= tso
    /\ log' = [log EXCEPT ![g] = Append(@, [k |-> "closed", ts |-> c])]
    /\ UNCHANGED <<tso, pend, frozen, cut, copied, published, finished, acked, writes, wk, qr, fl>>

Freeze ==
    /\ ~frozen /\ frozen' = TRUE /\ cut' = Len(log["G0"])
    /\ UNCHANGED <<tso, log, pend, copied, published, finished, acked, writes, closes, wk, qr, fl>>

Copy ==
    /\ frozen /\ ~copied /\ copied' = TRUE
    /\ UNCHANGED <<tso, log, pend, frozen, cut, published, finished, acked, writes, closes, wk, qr, fl>>

\* Publication takes a timestamp, so every write of a child is above it.
Publish ==
    /\ copied /\ ~published /\ published' = TRUE /\ tso' = tso + 1
    /\ UNCHANGED <<log, pend, frozen, cut, copied, finished, acked, writes, closes, wk, qr, fl>>

FinishSource ==
    /\ published /\ ~finished /\ finished' = TRUE
    /\ UNCHANGED <<tso, log, pend, frozen, cut, copied, published, acked, writes, closes, wk, qr, fl>>

-----------------------------------------------------------------------------
(* The search worker.                                                      *)

Pull(g) ==
    /\ wpos[g] < Len(log[g])
    /\ g # "G0" => tagged
    /\ wpos' = [wpos EXCEPT ![g] = @ + 1]
    /\ UNCHANGED <<wr, base, bound, tagged, tagBase, knows, qr, fl>>

\* It installs a range snapshot of G0, or drops superseded versions of what it has: it keeps the
\* newest versions only, and refuses cuts below the largest timestamp among them.
Forget(n) ==
    /\ ~tagged /\ forgets < MaxForgets /\ forgets' = forgets + 1
    /\ n \in wpos["G0"]..Len(log["G0"]) /\ n > base
    /\ wpos' = [wpos EXCEPT !["G0"] = n] /\ base' = n
    /\ bound' = Max(AllTs(Prefix(log["G0"], n)))
    /\ UNCHANGED <<wr, tagged, tagBase, knows, qr, crashes>>

LearnSplit ==
    /\ published /\ ~knows /\ knows' = TRUE
    /\ UNCHANGED <<wr, wpos, base, bound, tagged, tagBase, qr, fl>>

\* Re-tag only when every source entry up to the cut is here. The children inherit the bound.
\* Mutants: prefix_retag_before_cut does it at once, prefix_retag_drops_retained forgets the bound.
Retag ==
    /\ knows /\ ~tagged
    /\ wpos["G0"] >= cut \/ Mutant = "prefix_retag_before_cut"
    /\ tagged' = TRUE /\ tagBase' = wpos["G0"]
    /\ bound' = IF Mutant = "prefix_retag_drops_retained" THEN 0 ELSE bound
    /\ UNCHANGED <<wr, wpos, base, knows, qr, fl>>

\* Otherwise it installs the children's snapshots, which hold the newest versions of the cut.
InstallChildren ==
    /\ knows /\ ~tagged
    /\ tagged' = TRUE /\ tagBase' = cut /\ base' = cut
    /\ wpos' = [wpos EXCEPT !["G0"] = cut]
    /\ bound' = Max(AllTs(Prefix(log["G0"], cut)))
    /\ UNCHANGED <<wr, knows, qr, fl>>

\* A crash without the disk. The worker forgets what it knew and starts over from the log.
CrashWorker ==
    /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ wpos' = [g \in Groups |-> 0] /\ base' = 0 /\ bound' = 0
    /\ tagged' = FALSE /\ tagBase' = 0 /\ knows' = FALSE
    /\ UNCHANGED <<wr, qr, forgets>>

Holds(g) == IF g = "G0" THEN ~tagged ELSE tagged

\* Versions of key k from the first n entries of G0 that the worker still has.
Source(k, n) ==
    LET old == Rows(Prefix(log["G0"], IF base < n THEN base ELSE n), k) IN
    (IF old = {} THEN {} ELSE {Max(old)}) \cup {log["G0"][i].ts : i \in {j \in (base + 1)..n : log["G0"][j].k = k}}

Local(g, k) ==
    IF g = "G0" THEN Source(k, wpos["G0"]) ELSE Source(k, tagBase) \cup Rows(Prefix(log[g], wpos[g]), k)

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
       THEN \E s \in 1..MaxTs : s <= tso /\ qs' = [qs EXCEPT ![q] = s]
       ELSE UNCHANGED qs
    /\ UNCHANGED <<wr, wk, qbar, qans, fl>>

\* The barrier of one range. The leader of a frozen source first confirms through the Catalog that
\* the split is unpublished. Mutant frozen_source_serves skips that. Children answer after
\* publication only.
Barrier(q, g) ==
    /\ qst[q] = "run" /\ qmode[q] = "latest" /\ g \in qroute[q] /\ qbar[q][g] = None
    /\ IF g = "G0" THEN ~finished /\ (frozen => ~published \/ Mutant = "frozen_source_serves")
       ELSE published
    /\ qbar' = [qbar EXCEPT ![q][g] = Len(log[g])]
    /\ UNCHANGED <<wr, wk, qst, qmode, qroute, qneed, qs, qans, fl>>

\* The scan, once the worker reached the barrier. The newest version wins.
AnswerLatest(q, g) ==
    /\ qst[q] = "run" /\ qbar[q][g] # None /\ qans[q][g] = None
    /\ Holds(g) /\ wpos[g] >= qbar[q][g]
    /\ qans' = [qans EXCEPT ![q][g] = [k \in KeysOf(g) |-> Max(Local(g, k))]]
    /\ UNCHANGED <<wr, wk, qst, qmode, qroute, qneed, qs, qbar, fl>>

\* The worker serves a cut that it closed and still retains.
AnswerSnapshot(q, g) ==
    /\ qst[q] = "run" /\ qmode[q] = "snapshot" /\ g \in qroute[q] /\ qans[q][g] = None
    /\ Holds(g) /\ qs[q] <= WorkerFloor(g) /\ qs[q] >= bound
    /\ qans' = [qans EXCEPT ![q][g] = [k \in KeysOf(g) |-> Max({v \in Local(g, k) : v <= qs[q]})]]
    /\ UNCHANGED <<wr, wk, qst, qmode, qroute, qneed, qs, qbar, fl>>

\* The gateway compares the spans that came back with the keys it was asked for. Mutants:
\* accept_span_gap, accept_overlap, and trust_route, which looks at the route it sent.
Back(q) == {g \in Groups : qans[q][g] # None}
Covers(R, k) == Cardinality({g \in R : k \in KeysOf(g)})
Tiles(R) ==
    \A k \in Keys : /\ Covers(R, k) >= 1 \/ Mutant = "accept_span_gap"
                    /\ Covers(R, k) <= 1 \/ Mutant = "accept_overlap"

\* Every worker replied, for all ranges it was asked or for fewer.
FinishQuery(q) ==
    /\ qst[q] = "run" /\ Back(q) # {}
    /\ qst' = [qst EXCEPT ![q] = IF Tiles(IF Mutant = "trust_route" THEN qroute[q] ELSE Back(q)) THEN "done" ELSE "failed"]
    /\ UNCHANGED <<wr, wk, qmode, qroute, qneed, qs, qbar, qans, fl>>

\* A stale route, a missing replica or a deadline: an explicit error.
FailQuery(q) ==
    /\ qst[q] = "run" /\ Back(q) = {}
    /\ qst' = [qst EXCEPT ![q] = "failed"]
    /\ UNCHANGED <<wr, wk, qmode, qroute, qneed, qs, qbar, qans, fl>>

Terminated == (\A q \in Queries : qst[q] \in {"done", "failed"}) /\ UNCHANGED vars

Next ==
    \/ \E g \in Groups, k \in Keys : TakeTs(g, k)
    \/ \E g \in Groups : CommitWrite(g) \/ Pull(g) \/ \E c \in 1..MaxTs : Close(g, c)
    \/ Freeze \/ Copy \/ Publish \/ FinishSource
    \/ LearnSplit \/ Retag \/ InstallChildren \/ CrashWorker \/ \E n \in 1..MaxWrites + MaxCloses : Forget(n)
    \/ \E q \in Queries :
        \/ \E m \in {"latest", "snapshot"}, R \in Routes : StartQuery(q, m, R)
        \/ \E g \in Groups : Barrier(q, g) \/ AnswerLatest(q, g) \/ AnswerSnapshot(q, g)
        \/ FinishQuery(q) \/ FailQuery(q)
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* VEC-G04: a successful result was answered for every key by exactly one range.
ExactCoverage == \A q \in Queries : qst[q] = "done" => \A k \in Keys : Covers(Back(q), k) = 1

Answers(q, k) == {qans[q][g][k] : g \in {x \in Back(q) : k \in KeysOf(x)}}

\* VEC-G01 across a split.
LatestRealTime ==
    \A q \in Queries : qst[q] = "done" /\ qmode[q] = "latest" =>
        \A w \in qneed[q] : \A v \in Answers(q, w.k) : v >= w.ts

\* VEC-G02 across a split: the cut never changes after it was served.
SnapshotExact ==
    \A q \in Queries : qst[q] = "done" /\ qmode[q] = "snapshot" =>
        \A k \in Keys : \A v \in Answers(q, k) : v = Max({x \in History(k) : x <= qs[q]})

=============================================================================
