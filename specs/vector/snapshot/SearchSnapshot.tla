------------------------- MODULE SearchSnapshot -------------------------
(***************************************************************************)
(* `snapshot` search reads (plans/oltp-vector.md, sections 3 and 4).       *)
(*                                                                         *)
(* Every range leader closes a timestamp with a Data log entry. Apply      *)
(* records the floor, refuses a later commit at or below it, and refuses   *)
(* to close while an intent is open. A worker reads the floor from the     *)
(* feed, in log order. A range is a Raft group: its committed log and at   *)
(* most one uncommitted entry. Whether an entry took effect is a function  *)
(* of the log before it, as apply is deterministic.                        *)
(*                                                                         *)
(* Each range holds one key, so a newer version supersedes an older one.   *)
(* Property: VEC-G02 (SnapshotExact, ClosedIsFinal).                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Ranges,
    Directs,        \* single-row commits
    Home,           \* [Directs -> Ranges]
    Batch,          \* a cross-range transaction, or {} for none
    Queries,
    MaxAttempts, MaxElections, MaxCloses, MaxForgets,
    MaxTs,          \* at least the number of timestamps a run can hand out, plus one
    None, Mutant

VARIABLES
    tso,        \* the last timestamp the Timestamp group handed out
    log, tail,
    old,        \* old[r]: log length a deposed, unaware leader knows, or None
    dts, tries, \* timestamp of the direct commit's attempt, or None; attempts used
    prepared, bts, dec,
    closes,     \* Closed entries proposed per range
    cmsg,       \* Closed messages outside the log; only the prefix_* mutants send them
    wpos,       \* wpos[r]: entries the worker applied
    base,       \* base[r]: entries up to here kept only their newest version
    qs, qres,   \* the query's timestamp, and what each range returned
    elections, forgets

vars == <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x
Entry(k, w, ts) == [k |-> k, w |-> w, ts |-> ts]
OldDesign == Mutant \in {"prefix_deposed_leader_closes", "prefix_unknown_outcome_closes"}

-----------------------------------------------------------------------------
(* What apply does with entry i of a log.                                  *)

OpenIntent(l, i) ==
    \E j \in 1..(i - 1) : /\ l[j].k = "prep"
                          /\ ~\E m \in (j + 1)..(i - 1) : l[m].k \in {"res", "abt"}

\* Mutant closed_ignores_intents: Closed applies with an intent open.
ClosedTakes(l, i) == l[i].k = "closed" /\ (~OpenIntent(l, i) \/ Mutant = "closed_ignores_intents")

FloorAt(l, i) == Max({l[j].ts : j \in {m \in 1..(i - 1) : ClosedTakes(l, m)}})

\* Mutant direct_ignores_floor: apply does not compare a commit with the floor. The old design had
\* no floor in the log either.
RECURSIVE Takes(_, _)
NewestAt(l, i) == Max({l[j].ts : j \in {m \in 1..(i - 1) : Takes(l, m)}})
Takes(l, i) ==
    \/ l[i].k = "res"
    \/ /\ l[i].k = "dir"
       /\ ~OpenIntent(l, i)
       /\ l[i].ts > NewestAt(l, i)
       /\ OldDesign \/ Mutant = "direct_ignores_floor" \/ l[i].ts > FloorAt(l, i)

Committed(l) == {i \in 1..Len(l) : Takes(l, i)}

\* The newest version at or below s, 0 for none. Timestamps are unique, so a timestamp names a version.
NewestBelow(l, s) == Max({l[i].ts : i \in {j \in Committed(l) : l[j].ts <= s}})

-----------------------------------------------------------------------------

TypeOK ==
    /\ tso \in Nat
    /\ \A r \in Ranges : /\ wpos[r] \in 0..Len(log[r]) /\ base[r] \in 0..wpos[r]
                         /\ old[r] = None \/ old[r] \in 0..Len(log[r])
                         /\ closes[r] \in 0..MaxCloses
    /\ \A d \in Directs : tries[d] \in 0..MaxAttempts
    /\ dec \in {"none", "commit", "abort"}
    /\ prepared \subseteq Ranges
    /\ elections \in 0..MaxElections /\ forgets \in 0..MaxForgets

Init ==
    /\ tso = 0
    /\ log = [r \in Ranges |-> <<>>]
    /\ tail = [r \in Ranges |-> None]
    /\ old = [r \in Ranges |-> None]
    /\ dts = [d \in Directs |-> None]
    /\ tries = [d \in Directs |-> 0]
    /\ prepared = {} /\ bts = None /\ dec = "none"
    /\ closes = [r \in Ranges |-> 0]
    /\ cmsg = {}
    /\ wpos = [r \in Ranges |-> 0]
    /\ base = [r \in Ranges |-> 0]
    /\ qs = [q \in Queries |-> None]
    /\ qres = [q \in Queries |-> [r \in Ranges |-> None]]
    /\ elections = 0 /\ forgets = 0

InLog(r, w) == \E i \in 1..Len(log[r]) : log[r][i].w = w /\ log[r][i].k \in {"dir", "prep"}

-----------------------------------------------------------------------------
(* Commits.                                                                *)

\* The leader takes a timestamp for a commit. A refused or lost attempt takes a new one.
FetchTs(d) ==
    /\ dts[d] = None /\ tries[d] < MaxAttempts
    /\ tso' = tso + 1
    /\ dts' = [dts EXCEPT ![d] = tso + 1]
    /\ tries' = [tries EXCEPT ![d] = @ + 1]
    /\ UNCHANGED <<log, tail, old, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

ProposeDirect(d) ==
    /\ dts[d] # None /\ tail[Home[d]] = None
    /\ ~\E i \in 1..Len(log[Home[d]]) : log[Home[d]][i] = Entry("dir", d, dts[d])
    /\ tail' = [tail EXCEPT ![Home[d]] = Entry("dir", d, dts[d])]
    /\ UNCHANGED <<tso, log, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

\* The proposal was refused at apply, or its leader is gone: the caller gives the timestamp up. An
\* entry that is still uncommitted may commit later.
GiveUp(d) ==
    /\ dts[d] # None
    /\ dts' = [dts EXCEPT ![d] = None]
    /\ UNCHANGED <<tso, log, tail, old, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

ProposePrepare(r) ==
    /\ Batch # {} /\ dec = "none" /\ tail[r] = None /\ ~InLog(r, "b")
    /\ bts = None \/ Mutant = "alloc_before_prepare"
    /\ tail' = [tail EXCEPT ![r] = Entry("prep", "b", 0)]
    /\ UNCHANGED <<tso, log, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

\* The coordinator takes the commit timestamp after every participant prepared.
\* Mutant alloc_before_prepare takes it first, and prepares may still follow.
AllocCommitTs ==
    /\ Batch # {} /\ bts = None /\ dec = "none"
    /\ prepared = Ranges \/ Mutant = "alloc_before_prepare"
    /\ tso' = tso + 1 /\ bts' = tso + 1
    /\ UNCHANGED <<log, tail, old, dts, tries, prepared, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

Decide ==
    /\ dec = "none" /\ Batch # {}
    /\ \/ bts # None /\ prepared = Ranges /\ dec' = "commit"
       \/ dec' = "abort"
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

ProposeResolve(r) ==
    /\ dec # "none" /\ tail[r] = None /\ InLog(r, "b")
    /\ ~\E i \in 1..Len(log[r]) : log[r][i].k \in {"res", "abt"}
    /\ tail' = [tail EXCEPT ![r] = IF dec = "commit" THEN Entry("res", "b", bts) ELSE Entry("abt", "b", 0)]
    /\ UNCHANGED <<tso, log, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

\* The design before this review: a Closed message outside the log. A deposed leader sends it, or a
\* leader that saw no pending commit because an attempt of unknown outcome was settled (dts = None)
\* although its entry may still commit. It raised max_read_ts, so later attempts take larger ones.
OldClosed(r, c) ==
    IF Mutant = "prefix_deposed_leader_closes"
    THEN /\ old[r] # None /\ ~OpenIntent(log[r], old[r] + 1)
         /\ cmsg' = cmsg \cup {[r |-> r, at |-> old[r], ts |-> c]}
    ELSE /\ ~OpenIntent(log[r], Len(log[r]) + 1)
         /\ \A d \in Directs : Home[d] = r /\ dts[d] # None => dts[d] > c
         /\ cmsg' = cmsg \cup {[r |-> r, at |-> Len(log[r]), ts |-> c]}

\* The leader closes any timestamp that was already handed out. Mutant close_unallocated closes one
\* that was not.
ProposeClosed(r, c) ==
    /\ closes[r] < MaxCloses
    /\ c <= (IF Mutant = "close_unallocated" THEN tso + 1 ELSE tso)
    /\ c > FloorAt(log[r], Len(log[r]) + 1)
    /\ IF OldDesign THEN OldClosed(r, c) /\ UNCHANGED tail
       ELSE /\ tail[r] = None
            /\ tail' = [tail EXCEPT ![r] = Entry("closed", "c", c)]
            /\ UNCHANGED cmsg
    /\ closes' = [closes EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<tso, log, old, dts, tries, prepared, bts, dec, wpos, base, qs, qres, elections, forgets>>

Commit(r) ==
    /\ tail[r] # None
    /\ log' = [log EXCEPT ![r] = Append(@, tail[r])]
    /\ tail' = [tail EXCEPT ![r] = None]
    /\ prepared' = IF tail[r].k = "prep" THEN prepared \cup {r} ELSE prepared
    /\ UNCHANGED <<tso, old, dts, tries, bts, dec, closes, cmsg, wpos, base, qs, qres, elections, forgets>>

\* A leader change keeps the uncommitted entry or loses it. The old leader may not notice. An
\* attempt of the old leader cannot propose any more.
Elect(r) ==
    /\ elections < MaxElections /\ elections' = elections + 1
    /\ old' = [old EXCEPT ![r] = IF OldDesign THEN Len(log[r]) ELSE None]
    /\ \/ tail[r] # None /\ log' = [log EXCEPT ![r] = Append(@, tail[r])]
          /\ prepared' = IF tail[r].k = "prep" THEN prepared \cup {r} ELSE prepared
       \/ UNCHANGED <<log, prepared>>
    /\ tail' = [tail EXCEPT ![r] = None]
    /\ dts' = [d \in Directs |-> IF Home[d] = r THEN None ELSE dts[d]]
    /\ UNCHANGED <<tso, tries, bts, dec, closes, cmsg, wpos, base, qs, qres, forgets>>

-----------------------------------------------------------------------------
(* The search worker and its queries.                                      *)

Floor(r) ==
    Max({FloorAt(log[r], wpos[r] + 1)} \cup {m.ts : m \in {x \in cmsg : x.r = r /\ x.at <= wpos[r]}})

\* The smallest timestamp whose versions are all here.
Retained(r) == Max({log[r][i].ts : i \in {j \in Committed(log[r]) : j <= base[r]}})

PullFeed(r) ==
    /\ wpos[r] < Len(log[r])
    /\ wpos' = [wpos EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, base, qs, qres, elections, forgets>>

\* The worker installs a range snapshot, or drops versions that a newer one superseded. Either way it
\* keeps only the newest version of everything up to here.
Forget(r) ==
    /\ forgets < MaxForgets /\ forgets' = forgets + 1
    /\ \E n \in wpos[r]..Len(log[r]) :
        /\ wpos' = [wpos EXCEPT ![r] = n]
        /\ base' = [base EXCEPT ![r] = n]
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, qs, qres, elections>>

\* The worker loses its disk and starts the range again from the log.
RestartEmpty(r) ==
    /\ forgets < MaxForgets /\ forgets' = forgets + 1
    /\ wpos[r] > 0
    /\ wpos' = [wpos EXCEPT ![r] = 0]
    /\ base' = [base EXCEPT ![r] = 0]
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, qs, qres, elections>>

\* The gateway picks a timestamp that every range reported closed. Reports may be old.
\* Mutant gateway_picks_highest takes the highest floor.
StartQuery(q, s) ==
    /\ qs[q] = None
    /\ LET floors == {Floor(r) : r \in Ranges}
           top == IF Mutant = "gateway_picks_highest" THEN Max(floors)
                  ELSE CHOOSE f \in floors : \A g \in floors : f <= g IN
       s <= top /\ qs' = [qs EXCEPT ![q] = s]
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qres, elections, forgets>>

\* What the worker still holds at s.
Held(r, s) ==
    LET here == {i \in Committed(log[r]) : i <= wpos[r]}
        kept == {i \in here : i > base[r] \/ ~\E j \in here : j <= base[r] /\ log[r][j].ts > log[r][i].ts} IN
    Max({log[r][i].ts : i \in {j \in kept : log[r][j].ts <= s}})

\* Mutants: serve_above_floor trusts the gateway, serve_below_retained ignores what was dropped.
ServeRange(q, r) ==
    /\ qs[q] # None /\ qres[q][r] = None
    /\ qs[q] <= Floor(r) \/ Mutant \in {"serve_above_floor", "gateway_picks_highest"}
    /\ qs[q] >= Retained(r) \/ Mutant = "serve_below_retained"
    /\ qres' = [qres EXCEPT ![q][r] = Held(r, qs[q])]
    /\ UNCHANGED <<tso, log, tail, old, dts, tries, prepared, bts, dec, closes, cmsg, wpos, base, qs, elections, forgets>>

-----------------------------------------------------------------------------

Next ==
    \/ \E d \in Directs : FetchTs(d) \/ ProposeDirect(d) \/ GiveUp(d)
    \/ AllocCommitTs \/ Decide
    \/ \E r \in Ranges :
        \/ ProposePrepare(r) \/ ProposeResolve(r) \/ Commit(r) \/ Elect(r)
        \/ \E c \in 1..MaxTs : ProposeClosed(r, c)
        \/ PullFeed(r) \/ Forget(r) \/ RestartEmpty(r)
    \/ \E q \in Queries :
        \/ \E s \in 1..MaxTs : StartQuery(q, s)
        \/ \E r \in Ranges : ServeRange(q, r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties, VEC-G02.                                                    *)

\* Once a range served a query at s, its committed history at or below s never differs from what it
\* served. Across ranges this makes every batch whole, because its versions share one timestamp.
SnapshotExact ==
    \A q \in Queries, r \in Ranges :
        qres[q][r] # None => qres[q][r] = NewestBelow(log[r], qs[q])

\* No entry after an effective Closed commits at or below it.
ClosedIsFinal ==
    \A r \in Ranges : \A i, j \in 1..Len(log[r]) :
        i < j /\ ClosedTakes(log[r], i) /\ Takes(log[r], j) => log[r][j].ts > log[r][i].ts

=============================================================================
