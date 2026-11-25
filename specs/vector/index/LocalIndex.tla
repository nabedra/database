--------------------------- MODULE LocalIndex ---------------------------
(***************************************************************************)
(* The partitioned index inside one search worker (plans/oltp-vector.md,   *)
(* section 5): inserts, updates, deletes, partition split, merge and       *)
(* reassignment, exact scans, checkpoints and crashes.                     *)
(*                                                                         *)
(* `ops` is what the feed delivered, so it survives the worker. A partition *)
(* belongs to one core, so a step on one partition is atomic. A scan visits *)
(* partitions one at a time, with any steps between. An entry instance has *)
(* its own identity `i`, so a removal never hits a later copy.             *)
(*                                                                         *)
(* Properties: VEC-G05 (ExactScan, NoDuplicate), VEC-G03 (NoStaleEntry),   *)
(* VEC-G05 and VEC-G10 (NoLostRow).                                        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Slots, Parts, First, Queries,
          MaxOps, MaxDrains, MaxMoves, MaxCheckpoints, MaxCrashes, None, Mutant

VARIABLES
    ops,        \* the feed: [s, k] with k in {"up", "del"}; an entry's version is its index here
    app,        \* operations applied in memory
    ents,       \* ents[p]: entry instances [s, a, i]
    st,         \* st[p]: "free", "ready", "draining" or "retired"
    tgts,       \* tgts[p]: where a draining partition goes
    gone,       \* gone[p]: instances of p already copied to a target
    waits,      \* waits[p]: readers a retired partition waits for
    rems,       \* deferred removals [p, e, w]: w is the readers to wait for
    limbo,      \* entries between two partitions; only remove_before_append uses it
    q,          \* q[x]: [st, pt, todo, seen, cnt]
    ck,         \* the running checkpoint: [pos, todo, got, st], or None
    disk,       \* the installed manifest: [pos, got]
    unsynced,   \* files of the installed manifest are not on disk yet; only a mutant sets it
    drains, moves, checkpoints, crashes

vars == <<ops, app, ents, st, tgts, gone, waits, rems, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

Tree == {p \in Parts : st[p] \in {"ready", "draining"}}
Touched(s, lo, hi) == \E j \in (lo + 1)..hi : j <= Len(ops) /\ ops[j].s = s

\* The version (s, a) is the visible one at read point n.
VisibleAt(s, a, n) == a <= n /\ ops[a].s = s /\ ops[a].k = "up" /\ ~Touched(s, a, n)
LiveAt(s, n) == \E a \in 1..n : VisibleAt(s, a, n)

\* The smallest identity that no copy and no pending removal of this version has.
Used(s, a) ==
    {e.i : e \in {f \in (UNION {ents[p] : p \in Parts}) \cup {r.e : r \in rems} \cup (UNION {gone[p] : p \in Parts})
                       : f.s = s /\ f.a = a}}
NewId(s, a) == CHOOSE n \in 0..Cardinality(Used(s, a)) :
                 n \notin Used(s, a) /\ \A m \in 0..n : m < n => m \in Used(s, a)

Running == {x \in Queries : q[x].st = "run"}
\* Mutant prefix_checkpoint_without_epoch: a checkpoint does not hold removals back.
Readers == Running \cup (IF ck # None /\ Mutant # "prefix_checkpoint_without_epoch" THEN {"ck"} ELSE {})
PointOf(x) == IF x = "ck" THEN ck.pos ELSE q[x].pt
Idle == [st |-> "idle", pt |-> 0, todo |-> {}, seen |-> {}, cnt |-> [s \in Slots |-> 0]]

TypeOK ==
    /\ app \in 0..Len(ops) /\ Len(ops) <= MaxOps
    /\ \A p \in Parts : st[p] \in {"free", "ready", "draining", "retired"}
    /\ \A x \in Queries : q[x].st \in {"idle", "probed", "run", "done", "failed"}
    /\ drains \in 0..MaxDrains /\ moves \in 0..MaxMoves
    /\ checkpoints \in 0..MaxCheckpoints /\ crashes \in 0..MaxCrashes

Init ==
    /\ ops = <<>> /\ app = 0
    /\ ents = [p \in Parts |-> {}]
    /\ st = [p \in Parts |-> IF p \in First THEN "ready" ELSE "free"]
    /\ tgts = [p \in Parts |-> {}]
    /\ gone = [p \in Parts |-> {}]
    /\ waits = [p \in Parts |-> {}]
    /\ rems = {} /\ limbo = {}
    /\ q = [x \in Queries |-> Idle]
    /\ ck = None /\ disk = [pos |-> 0, got |-> {}] /\ unsynced = FALSE
    /\ drains = 0 /\ moves = 0 /\ checkpoints = 0 /\ crashes = 0

-----------------------------------------------------------------------------
(* Applying the feed.                                                      *)

Place(o, n, p) ==
    /\ IF o.k = "up" THEN ents' = [ents EXCEPT ![p] = @ \cup {[s |-> o.s, a |-> n, i |-> NewId(o.s, n)]}]
       ELSE UNCHANGED ents

NewOp(s, k) ==
    /\ app = Len(ops) /\ Len(ops) < MaxOps
    /\ k = "del" => LiveAt(s, app)
    /\ ops' = Append(ops, [s |-> s, k |-> k])
    /\ app' = app + 1
    /\ \E p \in {x \in Parts : st[x] = "ready"} : Place([s |-> s, k |-> k], app + 1, p)
    /\ UNCHANGED <<st, tgts, gone, waits, rems, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

\* After a crash the worker replays the feed from the checkpoint's position.
Replay ==
    /\ app < Len(ops)
    /\ app' = app + 1
    /\ \E p \in {x \in Parts : st[x] = "ready"} : Place(ops[app + 1], app + 1, p)
    /\ UNCHANGED <<ops, st, tgts, gone, waits, rems, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

-----------------------------------------------------------------------------
(* Split, merge and reassignment.                                          *)

\* A split drains into two new partitions, a merge into an existing one.
StartDrain(p, T) ==
    /\ drains < MaxDrains /\ drains' = drains + 1
    /\ st[p] = "ready" /\ p \notin T
    /\ \/ Cardinality(T) = 2 /\ \A t \in T : st[t] = "free"
       \/ Cardinality(T) = 1 /\ \A t \in T : st[t] = "ready"
    /\ st' = [x \in Parts |-> IF x = p THEN "draining" ELSE IF x \in T THEN "ready" ELSE st[x]]
    /\ tgts' = [tgts EXCEPT ![p] = T]
    /\ gone' = [gone EXCEPT ![p] = {}]
    /\ UNCHANGED <<ops, app, ents, waits, rems, limbo, q, ck, disk, unsynced, moves, checkpoints, crashes>>

CopyEntry(p, t) ==
    /\ st[p] = "draining" /\ t \in tgts[p] /\ st[t] = "ready"
    /\ \E e \in ents[p] \ gone[p] :
        /\ VisibleAt(e.s, e.a, app)
        /\ ents' = [ents EXCEPT ![t] = @ \cup {[e EXCEPT !.i = NewId(e.s, e.a)]}]
        /\ gone' = [gone EXCEPT ![p] = @ \cup {e}]
    /\ UNCHANGED <<ops, app, st, tgts, waits, rems, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

FinishDrain(p) ==
    /\ st[p] = "draining"
    /\ \A e \in ents[p] : VisibleAt(e.s, e.a, app) => e \in gone[p]
    /\ st' = [st EXCEPT ![p] = "retired"]
    /\ waits' = [waits EXCEPT ![p] = Readers]
    /\ tgts' = [tgts EXCEPT ![p] = {}] /\ gone' = [gone EXCEPT ![p] = {}]
    /\ UNCHANGED <<ops, app, ents, rems, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

\* Mutant free_without_wait reuses a retired partition while a scan still needs it.
FreePart(p) ==
    /\ st[p] = "retired" /\ (waits[p] \cap Readers = {} \/ Mutant = "free_without_wait")
    /\ st' = [st EXCEPT ![p] = "free"]
    /\ ents' = [ents EXCEPT ![p] = {}]
    /\ rems' = {r \in rems : r.p # p}
    /\ waits' = [waits EXCEPT ![p] = {}]
    /\ UNCHANGED <<ops, app, tgts, gone, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

\* Append to the target, remove from the source later. Mutant remove_before_append does it the
\* other way round, in two steps.
Reassign(p, t) ==
    /\ moves < MaxMoves /\ moves' = moves + 1
    /\ st[p] = "ready" /\ st[t] = "ready" /\ p # t
    /\ \E e \in ents[p] :
        /\ VisibleAt(e.s, e.a, app)
        /\ ~\E r \in rems : r.p = p /\ r.e = e
        /\ IF Mutant = "remove_before_append"
           THEN /\ ents' = [ents EXCEPT ![p] = @ \ {e}]
                /\ limbo' = limbo \cup {[e |-> e, t |-> t]}
                /\ UNCHANGED rems
           ELSE /\ ents' = [ents EXCEPT ![t] = @ \cup {[e EXCEPT !.i = NewId(e.s, e.a)]}]
                /\ rems' = rems \cup {[p |-> p, e |-> e, w |-> Readers]}
                /\ UNCHANGED limbo
    /\ UNCHANGED <<ops, app, st, tgts, gone, waits, q, ck, disk, unsynced, drains, checkpoints, crashes>>

\* Mutants: remove_without_wait ignores running scans, prefix_remove_by_slot removes every copy of
\* the row in the partition.
RemoveMoved(p) ==
    IF Mutant = "remove_before_append"
    THEN \E x \in limbo :
        /\ x.t = p /\ st[p] = "ready"
        /\ ents' = [ents EXCEPT ![p] = @ \cup {x.e}]
        /\ limbo' = limbo \ {x}
        /\ UNCHANGED <<ops, app, st, tgts, gone, waits, rems, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>
    ELSE \E r \in rems :
        /\ r.p = p /\ (r.w \cap Readers = {} \/ Mutant = "remove_without_wait")
        /\ rems' = rems \ {r}
        /\ ents' = [ents EXCEPT ![p] = IF Mutant = "prefix_remove_by_slot"
                                       THEN {e \in @ : ~(e.s = r.e.s /\ e.a = r.e.a)} ELSE @ \ {r.e}]
        /\ UNCHANGED <<ops, app, st, tgts, gone, waits, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

\* A superseded entry goes once no reader can see it. Mutant gc_without_epoch: at once.
Collect(p) ==
    /\ \E e \in ents[p] :
        /\ Touched(e.s, e.a, app)
        /\ \A x \in Readers : Touched(e.s, e.a, PointOf(x)) \/ Mutant = "gc_without_epoch"
        /\ ents' = [ents EXCEPT ![p] = @ \ {e}]
        /\ rems' = {r \in rems : ~(r.p = p /\ r.e = e)}
    /\ UNCHANGED <<ops, app, st, tgts, gone, waits, limbo, q, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

-----------------------------------------------------------------------------
(* Exact scans.                                                            *)

\* The barrier was reached. The scan fixes its read point and its list of partitions in one step.
\* Mutant prefix_probe_before_pin, the design before this review: the partitions are ranked while
\* the barrier is in flight, and the point is fixed in a later step.
StartScan(x) ==
    /\ app = Len(ops)
    /\ IF Mutant = "prefix_probe_before_pin"
       THEN \/ q[x].st = "idle" /\ q' = [q EXCEPT ![x] = [Idle EXCEPT !.st = "probed", !.todo = Tree]]
            \/ q[x].st = "probed" /\ q' = [q EXCEPT ![x].st = "run", ![x].pt = app]
       ELSE q[x].st = "idle" /\ q' = [q EXCEPT ![x] = [Idle EXCEPT !.st = "run", !.pt = app, !.todo = Tree]]
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

\* Mutant prefix_current_liveness judges an entry by the row map as it is now.
Shows(x, e) ==
    IF Mutant = "prefix_current_liveness" THEN e.a <= q[x].pt /\ VisibleAt(e.s, e.a, app)
    ELSE VisibleAt(e.s, e.a, q[x].pt)

\* Mutant no_slot_dedup counts every copy. Mutant show_superseded shows entries of old versions.
Visit(x, p) ==
    /\ q[x].st = "run" /\ p \in q[x].todo
    /\ LET hit == {e \in ents[p] : Shows(x, e) \/ (Mutant = "show_superseded" /\ e.a <= q[x].pt)}
           n(s) == Cardinality({e \in hit : e.s = s})
           c(s) == IF Mutant = "no_slot_dedup" THEN q[x].cnt[s] + n(s)
                   ELSE IF n(s) > 0 THEN 1 ELSE q[x].cnt[s] IN
       q' = [q EXCEPT ![x].todo = @ \ {p},
                      ![x].seen = @ \cup {[s |-> e.s, a |-> e.a] : e \in hit},
                      ![x].cnt = [s \in Slots |-> c(s)]]
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

FinishScan(x) ==
    /\ q[x].st = "run" /\ q[x].todo = {}
    /\ q' = [q EXCEPT ![x].st = "done"]
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, ck, disk, unsynced, drains, moves, checkpoints, crashes>>

-----------------------------------------------------------------------------
(* Checkpoints and crashes. A checkpoint reads like a scan at its position. *)

BeginCheckpoint ==
    /\ ck = None /\ checkpoints < MaxCheckpoints /\ app = Len(ops)
    /\ checkpoints' = checkpoints + 1
    /\ ck' = [pos |-> app, todo |-> Tree, got |-> {}]
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, q, disk, unsynced, drains, moves, crashes>>

WriteFile(p) ==
    /\ ck # None /\ p \in ck.todo
    /\ ck' = [ck EXCEPT !.todo = @ \ {p}, !.got = @ \cup {[s |-> e.s, a |-> e.a] : e \in ents[p]}]
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, q, disk, unsynced, drains, moves, checkpoints, crashes>>

\* Every file is synced, then the manifest is. Mutants: manifest_before_sync, and
\* checkpoint_ahead_of_files, whose manifest claims the position reached meanwhile.
InstallManifest ==
    /\ ck # None /\ ck.todo = {}
    /\ disk' = [pos |-> IF Mutant = "checkpoint_ahead_of_files" THEN app ELSE ck.pos, got |-> ck.got]
    /\ unsynced' = (Mutant = "manifest_before_sync")
    /\ ck' = None
    /\ UNCHANGED <<ops, app, ents, st, tgts, gone, waits, rems, limbo, q, drains, moves, checkpoints, crashes>>

\* Memory is lost. With the disk the worker loads the manifest, without it an empty one.
Crash(keep) ==
    /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ LET d == IF ~keep THEN [pos |-> 0, got |-> {}]
                ELSE IF unsynced THEN [disk EXCEPT !.got = {}] ELSE disk
           p0 == CHOOSE p \in First : TRUE IN
       /\ disk' = d
       /\ app' = d.pos
       /\ ents' = [p \in Parts |-> IF p = p0 THEN {[s |-> e.s, a |-> e.a, i |-> 0] : e \in d.got} ELSE {}]
    /\ st' = [p \in Parts |-> IF p \in First THEN "ready" ELSE "free"]
    /\ tgts' = [p \in Parts |-> {}] /\ gone' = [p \in Parts |-> {}] /\ waits' = [p \in Parts |-> {}]
    /\ rems' = {} /\ limbo' = {} /\ ck' = None /\ unsynced' = FALSE
    /\ q' = [x \in Queries |-> IF q[x].st \in {"run", "probed"} THEN [Idle EXCEPT !.st = "failed"] ELSE q[x]]
    /\ UNCHANGED <<ops, drains, moves, checkpoints>>

Next ==
    \/ \E s \in Slots, k \in {"up", "del"} : NewOp(s, k)
    \/ Replay
    \/ \E p \in Parts :
        \/ \E T \in SUBSET Parts : StartDrain(p, T)
        \/ \E t \in Parts : CopyEntry(p, t) \/ Reassign(p, t)
        \/ Collect(p)
        \/ FinishDrain(p) \/ FreePart(p) \/ WriteFile(p) \/ RemoveMoved(p)
        \/ \E x \in Queries : Visit(x, p)
    \/ \E x \in Queries : StartScan(x) \/ FinishScan(x)
    \/ BeginCheckpoint \/ InstallManifest
    \/ \E keep \in BOOLEAN : Crash(keep)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* VEC-G05: a finished exact scan saw every row visible at its read point.
ExactScan ==
    \A x \in Queries : q[x].st = "done" =>
        \A s \in Slots : LiveAt(s, q[x].pt) => \E e \in q[x].seen : e.s = s /\ VisibleAt(s, e.a, q[x].pt)

\* VEC-G03: and nothing else.
NoStaleEntry ==
    \A x \in Queries : q[x].st = "done" => \A e \in q[x].seen : VisibleAt(e.s, e.a, q[x].pt)

\* VEC-G05: and each row once.
NoDuplicate == \A x \in Queries : q[x].st = "done" => \A s \in Slots : q[x].cnt[s] <= 1

\* Every row visible at the applied position has an entry in a partition that scans visit, also
\* after a crash and while it replays.
NoLostRow ==
    \A s \in Slots : LiveAt(s, app) =>
        \E p \in Tree : \E e \in ents[p] : e.s = s /\ VisibleAt(s, e.a, app)

=============================================================================
