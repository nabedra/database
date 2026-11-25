--------------------------- MODULE LocalIndex ---------------------------
(***************************************************************************)
(* The partitioned index inside one search worker (plans/oltp-vector.md,   *)
(* section 5): inserts, updates, deletes, partition split, merge and       *)
(* reassignment, scans at the applied position and at older cuts,          *)
(* checkpoints, crashes, resync from a range snapshot and range drop.      *)
(*                                                                         *)
(* `ops` is what the feed delivered, so it survives the worker. Operations *)
(* are applied in a pipeline: cores append the entries of up to MaxFlight  *)
(* operations beyond the applied position, in any order, and the position  *)
(* advances over those that every core finished. A partition belongs to    *)
(* one core, so a step on one partition is atomic. A copy to another       *)
(* partition is a message between cores. A scan visits partitions one at a *)
(* time. An entry instance has its own identity `i` and a generation `g`.  *)
(*                                                                         *)
(* Properties: VEC-G05 (ExactScan, NoDuplicate, NoLostRow, DrainEnds),     *)
(* VEC-G03 (NoStaleEntry, SingleGeneration).                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Slots, Parts, First, Queries,
          MaxOps, MaxFlight, MaxDrains, MaxMoves, MaxCheckpoints, MaxCrashes, MaxResyncs, None, Mutant

VARIABLES
    ops,        \* the feed: [s, k] with k in {"up", "del"}; an entry's version is its index here
    app,        \* operations applied on every core
    placed,     \* operations beyond app that their core finished
    low,        \* the oldest cut a scan may ask for
    held,       \* the worker holds the range
    gen,        \* the generation of the index
    ents,       \* ents[p]: entry instances [s, a, i, g]
    st,         \* st[p]: "free", "ready", "draining" or "retired"
    tgts,       \* tgts[p]: where a draining partition goes
    sent,       \* sent[p]: instances of p already sent to a target
    flight,     \* copies on their way to another core: [p, t, e, src]
    waits,      \* waits[p]: readers a retired partition waits for
    rems,       \* deferred removals [p, e, w]: w is the readers to wait for, None until the copy landed
    q,          \* q[x]: [st, pt, todo, seen, cnt]
    ck,         \* the running checkpoint: [pos, low, todo, got], or None
    disk,       \* the installed manifest: [pos, low, got]
    unsynced,   \* files of the installed manifest are not on disk yet; only a mutant sets it
    drains, moves, checkpoints, crashes, resyncs

feed == <<ops, app, placed, low, held, gen>>
idx == <<ents, st, tgts, sent, flight, waits, rems>>
bud == <<drains, moves, checkpoints, crashes, resyncs>>
vars == <<feed, idx, q, ck, disk, unsynced, bud>>

Tree == {p \in Parts : st[p] \in {"ready", "draining"}}
Touched(s, lo, hi) == \E j \in (lo + 1)..hi : j <= Len(ops) /\ ops[j].s = s

\* The version (s, a) is the visible one at read point n.
VisibleAt(s, a, n) == a <= n /\ ops[a].s = s /\ ops[a].k = "up" /\ ~Touched(s, a, n)
LiveAt(s, n) == \E a \in 1..n : VisibleAt(s, a, n)

\* Some cut that is or may still be asked for sees this version, or it is not applied yet.
\* Mutant prefix_drain_copies_current_only keeps the newest applied versions only, and mutant
\* prefix_drain_ignores_unapplied forgets entries beyond the applied position.
Wanted(e) ==
    \/ e.a > app /\ Mutant # "prefix_drain_ignores_unapplied"
    \/ IF Mutant = "prefix_drain_copies_current_only" THEN VisibleAt(e.s, e.a, app)
       ELSE \E n \in low..app : VisibleAt(e.s, e.a, n)

Same(e, f) == e.s = f.s /\ e.a = f.a
Used(s, a) ==
    {e.i : e \in {f \in (UNION {ents[p] : p \in Parts}) \cup {r.e : r \in rems}
                        \cup (UNION {sent[p] : p \in Parts}) \cup {m.e : m \in flight} \cup {m.src : m \in flight}
                       : f.s = s /\ f.a = a}}
NewId(s, a) == CHOOSE n \in 0..Cardinality(Used(s, a)) :
                 n \notin Used(s, a) /\ \A m \in 0..n : m < n => m \in Used(s, a)

Running == {x \in Queries : q[x].st = "run"}
\* Mutant prefix_checkpoint_without_epoch: a checkpoint does not hold removals back.
Readers == Running \cup (IF ck # None /\ Mutant # "prefix_checkpoint_without_epoch" THEN {"ck"} ELSE {})
PointOf(x) == IF x = "ck" THEN ck.low ELSE q[x].pt
Idle == [st |-> "idle", pt |-> 0, todo |-> {}, seen |-> {}, cnt |-> [s \in Slots |-> 0]]

TypeOK ==
    /\ app \in 0..Len(ops) /\ low \in 0..app /\ Len(ops) <= MaxOps /\ held \in BOOLEAN
    /\ placed \subseteq (app + 1)..Len(ops)
    /\ \A p \in Parts : st[p] \in {"free", "ready", "draining", "retired"}
    /\ \A x \in Queries : q[x].st \in {"idle", "probed", "run", "done", "failed"}
    /\ drains \in 0..MaxDrains /\ moves \in 0..MaxMoves /\ resyncs \in 0..MaxResyncs
    /\ checkpoints \in 0..MaxCheckpoints /\ crashes \in 0..MaxCrashes

Init ==
    /\ ops = <<>> /\ app = 0 /\ placed = {} /\ low = 0 /\ held = TRUE /\ gen = 0
    /\ ents = [p \in Parts |-> {}]
    /\ st = [p \in Parts |-> IF p \in First THEN "ready" ELSE "free"]
    /\ tgts = [p \in Parts |-> {}] /\ sent = [p \in Parts |-> {}] /\ waits = [p \in Parts |-> {}]
    /\ flight = {} /\ rems = {}
    /\ q = [x \in Queries |-> Idle]
    /\ ck = None /\ disk = [pos |-> 0, low |-> 0, got |-> {}] /\ unsynced = FALSE
    /\ drains = 0 /\ moves = 0 /\ checkpoints = 0 /\ crashes = 0 /\ resyncs = 0

-----------------------------------------------------------------------------
(* Applying the feed.                                                      *)

NewOp(s, k) ==
    /\ Len(ops) < MaxOps /\ Len(ops) - app < MaxFlight
    /\ k = "del" => LiveAt(s, Len(ops))
    /\ ops' = Append(ops, [s |-> s, k |-> k])
    /\ UNCHANGED <<app, placed, low, held, gen, idx, q, ck, disk, unsynced, bud>>

\* The owner core of p appends the entry of operation n. After a crash these are replays.
AppendEntry(p, n) ==
    /\ held /\ n \in (app + 1)..(app + MaxFlight) /\ n <= Len(ops) /\ n \notin placed /\ st[p] = "ready"
    /\ placed' = placed \cup {n}
    /\ ents' = IF ops[n].k = "up"
               THEN [ents EXCEPT ![p] = @ \cup {[s |-> ops[n].s, a |-> n, i |-> NewId(ops[n].s, n), g |-> gen]}]
               ELSE ents
    /\ UNCHANGED <<ops, app, low, held, gen, st, tgts, sent, flight, waits, rems, q, ck, disk, unsynced, bud>>

\* The position counts as applied once every core finished it.
Advance ==
    /\ app + 1 \in placed
    /\ app' = app + 1 /\ placed' = placed \ {app + 1}
    /\ UNCHANGED <<ops, low, held, gen, idx, q, ck, disk, unsynced, bud>>

\* Retention moves on: older cuts are refused from now on.
AdvanceLow ==
    /\ low < app /\ low' = low + 1
    /\ UNCHANGED <<ops, app, placed, held, gen, idx, q, ck, disk, unsynced, bud>>

-----------------------------------------------------------------------------
(* Split, merge and reassignment.                                          *)

\* A split drains into two new partitions, a merge into an existing one. A partition that another one
\* drains into does not start to drain. Mutant prefix_drain_a_target lets it.
StartDrain(p, T) ==
    /\ drains < MaxDrains /\ drains' = drains + 1
    /\ st[p] = "ready" /\ p \notin T
    /\ (\A x \in Parts : p \notin tgts[x]) \/ Mutant = "prefix_drain_a_target"
    /\ \/ Cardinality(T) = 2 /\ \A t \in T : st[t] = "free"
       \/ Cardinality(T) = 1 /\ \A t \in T : st[t] = "ready"
    /\ st' = [x \in Parts |-> IF x = p THEN "draining" ELSE IF x \in T THEN "ready" ELSE st[x]]
    /\ tgts' = [tgts EXCEPT ![p] = T]
    /\ UNCHANGED <<feed, ents, sent, flight, waits, rems, q, ck, disk, unsynced, moves, checkpoints, crashes, resyncs>>

SendCopy(p, t) ==
    /\ st[p] = "draining" /\ t \in tgts[p]
    /\ \E e \in ents[p] \ sent[p] :
        /\ Wanted(e)
        /\ flight' = flight \cup {[p |-> p, t |-> t, src |-> e, e |-> [e EXCEPT !.i = NewId(e.s, e.a)]]}
        /\ sent' = [sent EXCEPT ![p] = @ \cup {e}]
    /\ UNCHANGED <<feed, ents, st, tgts, waits, rems, q, ck, disk, unsynced, bud>>

\* Append to the target, remove from the source later. Mutant remove_before_append removes first.
Reassign(p, t) ==
    /\ moves < MaxMoves /\ moves' = moves + 1
    /\ st[p] = "ready" /\ st[t] = "ready" /\ p # t
    /\ \E e \in ents[p] :
        /\ Wanted(e) /\ ~\E r \in rems : r.p = p /\ r.e = e
        /\ flight' = flight \cup {[p |-> p, t |-> t, src |-> e, e |-> [e EXCEPT !.i = NewId(e.s, e.a)]]}
        /\ IF Mutant = "remove_before_append"
           THEN ents' = [ents EXCEPT ![p] = @ \ {e}] /\ UNCHANGED rems
           ELSE rems' = rems \cup {[p |-> p, e |-> e, w |-> None]} /\ UNCHANGED ents
    /\ UNCHANGED <<feed, st, tgts, sent, waits, q, ck, disk, unsynced, drains, checkpoints, crashes, resyncs>>

\* The copy lands, also in a partition that drains meanwhile, which then sends it on. A copy of an
\* older generation is dropped. Mutants: prefix_land_needs_ready leaves it in flight while its target drains,
\* late_copy_after_resync lands an old one.
Land(t) ==
    /\ \E m \in flight :
        /\ m.t = t
        /\ st[t] = "ready" \/ (st[t] = "draining" /\ Mutant # "prefix_land_needs_ready") \/ m.e.g # gen
        /\ flight' = flight \ {m}
        /\ IF m.e.g = gen \/ (Mutant = "late_copy_after_resync" /\ st[t] = "ready")
           THEN /\ ents' = [ents EXCEPT ![t] = @ \cup {m.e}]
                /\ rems' = {IF r.p = m.p /\ r.e = m.src THEN [r EXCEPT !.w = Readers] ELSE r : r \in rems}
           ELSE UNCHANGED <<ents, rems>>
    /\ UNCHANGED <<feed, st, tgts, sent, waits, q, ck, disk, unsynced, bud>>

\* Everything wanted was sent and has landed, and no copy is on its way here.
FinishDrain(p) ==
    /\ st[p] = "draining"
    /\ \A e \in ents[p] : Wanted(e) => e \in sent[p]
    /\ ~\E m \in flight : m.p = p \/ m.t = p
    /\ st' = [st EXCEPT ![p] = "retired"]
    /\ waits' = [waits EXCEPT ![p] = Readers]
    /\ tgts' = [tgts EXCEPT ![p] = {}] /\ sent' = [sent EXCEPT ![p] = {}]
    /\ UNCHANGED <<feed, ents, flight, rems, q, ck, disk, unsynced, bud>>

\* Mutant free_without_wait reuses a retired partition while a scan still needs it.
FreePart(p) ==
    /\ st[p] = "retired" /\ (waits[p] \cap Readers = {} \/ Mutant = "free_without_wait")
    /\ st' = [st EXCEPT ![p] = "free"]
    /\ ents' = [ents EXCEPT ![p] = {}]
    /\ rems' = {r \in rems : r.p # p}
    /\ waits' = [waits EXCEPT ![p] = {}]
    /\ UNCHANGED <<feed, tgts, sent, flight, q, ck, disk, unsynced, bud>>

\* Mutants: remove_without_wait ignores running scans, prefix_remove_by_slot removes every copy of
\* the row in the partition.
RemoveMoved(p) ==
    /\ \E r \in rems :
        /\ r.p = p /\ r.w # None /\ (r.w \cap Readers = {} \/ Mutant = "remove_without_wait")
        /\ rems' = rems \ {r}
        /\ ents' = [ents EXCEPT ![p] = IF Mutant = "prefix_remove_by_slot"
                                       THEN {e \in @ : ~Same(e, r.e)} ELSE @ \ {r.e}]
    /\ UNCHANGED <<feed, st, tgts, sent, flight, waits, q, ck, disk, unsynced, bud>>

\* A superseded entry goes once no reader and no retained cut can see it. Mutant gc_without_epoch
\* looks at the newest version only.
Collect(p) ==
    /\ \E e \in ents[p] :
        /\ Touched(e.s, e.a, app) /\ ~\E r \in rems : r.p = p /\ r.e = e
        /\ \/ Mutant = "gc_without_epoch"
           \/ Touched(e.s, e.a, low) /\ \A x \in Readers : Touched(e.s, e.a, PointOf(x))
        /\ ents' = [ents EXCEPT ![p] = @ \ {e}]
        /\ sent' = [sent EXCEPT ![p] = @ \ {e}]
    /\ UNCHANGED <<feed, st, tgts, flight, waits, rems, q, ck, disk, unsynced, bud>>

-----------------------------------------------------------------------------
(* Exact scans, at the applied position (`latest`, after a barrier) or an   *)
(* older cut (`snapshot`).                                                 *)

\* The scan fixes its read point and its list of partitions in one step.
\* Mutant prefix_probe_before_pin, the design before this review: the partitions are ranked while
\* the barrier is in flight, and the point is fixed in a later step.
StartScan(x, n) ==
    /\ held /\ n \in low..app
    /\ IF Mutant = "prefix_probe_before_pin"
       THEN \/ q[x].st = "idle" /\ q' = [q EXCEPT ![x] = [Idle EXCEPT !.st = "probed", !.todo = Tree]]
            \/ q[x].st = "probed" /\ q' = [q EXCEPT ![x].st = "run", ![x].pt = n]
       ELSE q[x].st = "idle" /\ q' = [q EXCEPT ![x] = [Idle EXCEPT !.st = "run", !.pt = n, !.todo = Tree]]
    /\ UNCHANGED <<feed, idx, ck, disk, unsynced, bud>>

\* Mutants: prefix_current_liveness judges an entry by the newest applied version, prefix_short_version
\* compares only the last bit of a version, show_superseded shows entries of old versions.
Shows(x, e) ==
    CASE Mutant = "prefix_current_liveness" -> e.a <= q[x].pt /\ VisibleAt(e.s, e.a, app)
      [] Mutant = "prefix_short_version" ->
            e.a <= q[x].pt /\ \E b \in 1..q[x].pt : b % 2 = e.a % 2 /\ VisibleAt(e.s, b, q[x].pt)
      [] Mutant = "show_superseded" -> e.a <= q[x].pt
      [] OTHER -> VisibleAt(e.s, e.a, q[x].pt)

\* Mutant no_slot_dedup counts every copy.
Visit(x, p) ==
    /\ q[x].st = "run" /\ p \in q[x].todo
    /\ LET hit == {e \in ents[p] : Shows(x, e)}
           n(s) == Cardinality({e \in hit : e.s = s})
           c(s) == IF Mutant = "no_slot_dedup" THEN q[x].cnt[s] + n(s)
                   ELSE IF n(s) > 0 THEN 1 ELSE q[x].cnt[s] IN
       q' = [q EXCEPT ![x].todo = @ \ {p},
                      ![x].seen = @ \cup {[s |-> e.s, a |-> e.a] : e \in hit},
                      ![x].cnt = [s \in Slots |-> c(s)]]
    /\ UNCHANGED <<feed, idx, ck, disk, unsynced, bud>>

FinishScan(x) ==
    /\ q[x].st = "run" /\ q[x].todo = {}
    /\ q' = [q EXCEPT ![x].st = "done"]
    /\ UNCHANGED <<feed, idx, ck, disk, unsynced, bud>>

-----------------------------------------------------------------------------
(* Checkpoints, crashes, resync and drop. A checkpoint reads like a scan.  *)

BeginCheckpoint ==
    /\ held /\ ck = None /\ checkpoints < MaxCheckpoints
    /\ checkpoints' = checkpoints + 1
    /\ ck' = [pos |-> app, low |-> low, todo |-> Tree, got |-> {}]
    /\ UNCHANGED <<feed, idx, q, disk, unsynced, drains, moves, crashes, resyncs>>

WriteFile(p) ==
    /\ ck # None /\ p \in ck.todo
    /\ ck' = [ck EXCEPT !.todo = @ \ {p}, !.got = @ \cup {[s |-> e.s, a |-> e.a] : e \in ents[p]}]
    /\ UNCHANGED <<feed, idx, q, disk, unsynced, bud>>

\* Every file is synced, then the manifest is. Mutants: manifest_before_sync, checkpoint_ahead_of_files,
\* whose manifest claims the position reached meanwhile, and checkpoint_forgets_low, which claims
\* that it can serve any older cut.
InstallManifest ==
    /\ ck # None /\ ck.todo = {}
    /\ disk' = [pos |-> IF Mutant = "checkpoint_ahead_of_files" THEN app ELSE ck.pos,
                low |-> IF Mutant = "checkpoint_forgets_low" THEN 0 ELSE ck.low, got |-> ck.got]
    /\ unsynced' = (Mutant = "manifest_before_sync")
    /\ ck' = None
    /\ UNCHANGED <<feed, idx, q, bud>>

\* Put a set of versions into the first partition, as a generation. Every running scan fails.
\* Mutant prefix_scan_survives_resync lets it go on in the new index.
Load(got, n, l, g) ==
    /\ gen' = g /\ app' = n /\ placed' = {} /\ low' = l
    /\ LET p0 == CHOOSE p \in First : TRUE IN
       ents' = [p \in Parts |-> IF p = p0 THEN {[s |-> e.s, a |-> e.a, i |-> 0, g |-> g] : e \in got} ELSE {}]
    /\ st' = [p \in Parts |-> IF p \in First THEN "ready" ELSE "free"]
    /\ tgts' = [p \in Parts |-> {}] /\ sent' = [p \in Parts |-> {}] /\ waits' = [p \in Parts |-> {}]
    /\ rems' = {} /\ ck' = None
    /\ q' = IF Mutant = "prefix_scan_survives_resync" THEN q
            ELSE [x \in Queries |-> IF q[x].st \in {"run", "probed"} THEN [Idle EXCEPT !.st = "failed"] ELSE q[x]]

SnapshotAt(n) == {[s |-> ops[a].s, a |-> a] : a \in {b \in 1..n : VisibleAt(ops[b].s, b, n)}}
NoDisk == [pos |-> 0, low |-> 0, got |-> {}]

\* Memory is lost. The worker loads its own manifest, nothing, or a checkpoint of the other replica.
Crash(how) ==
    /\ held /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ flight' = {} /\ unsynced' = FALSE
    /\ CASE how = "disk" -> /\ Load(IF unsynced THEN {} ELSE disk.got, disk.pos, disk.low, gen) /\ UNCHANGED disk
         [] how = "empty" -> /\ Load({}, 0, 0, gen) /\ disk' = NoDisk
         [] how = "peer" -> \E n \in 0..Len(ops) : /\ Load(SnapshotAt(n), n, n, gen) /\ disk' = NoDisk
    /\ UNCHANGED <<ops, held, drains, moves, checkpoints, resyncs>>

\* The log was purged, or the range was assigned again: a range snapshot replaces the index at once, as
\* a new generation. It holds only the newest versions. Copies between cores may still be on their way.
Resync ==
    /\ resyncs < MaxResyncs /\ resyncs' = resyncs + 1
    /\ \E n \in app..Len(ops) : Load(SnapshotAt(n), n, n, gen + 1)
    /\ held' = TRUE /\ disk' = NoDisk /\ unsynced' = FALSE
    /\ UNCHANGED <<ops, flight, drains, moves, checkpoints, crashes>>

\* The range moved to another worker: this one drops it.
DropRange ==
    /\ held /\ resyncs < MaxResyncs /\ resyncs' = resyncs + 1
    /\ Load({}, 0, 0, gen + 1)
    /\ held' = FALSE /\ disk' = NoDisk /\ unsynced' = FALSE
    /\ UNCHANGED <<ops, flight, drains, moves, checkpoints, crashes>>

Terminated ==
    /\ \A x \in Queries : q[x].st \in {"done", "failed"} \/ ~held
    /\ app = Len(ops) \/ ~held
    /\ ck = None
    /\ ~\E p \in Parts : st[p] = "draining"
    /\ UNCHANGED vars

Next ==
    \/ \E s \in Slots, k \in {"up", "del"} : NewOp(s, k)
    \/ Advance \/ AdvanceLow
    \/ \E p \in Parts :
        \/ \E T \in SUBSET Parts : StartDrain(p, T)
        \/ \E t \in Parts : SendCopy(p, t) \/ Reassign(p, t)
        \/ \E n \in 1..MaxOps : AppendEntry(p, n)
        \/ Land(p) \/ Collect(p) \/ FinishDrain(p) \/ FreePart(p) \/ WriteFile(p) \/ RemoveMoved(p)
        \/ \E x \in Queries : Visit(x, p)
    \/ \E x \in Queries : FinishScan(x) \/ \E n \in 0..MaxOps : StartScan(x, n)
    \/ BeginCheckpoint \/ InstallManifest \/ Resync \/ DropRange
    \/ \E how \in {"disk", "empty", "peer"} : Crash(how)
    \/ Terminated

\* The cores' message loops and the maintenance loop.
Fairness ==
    \A p \in Parts : /\ WF_vars(Land(p)) /\ WF_vars(FinishDrain(p))
                     /\ \A t \in Parts : WF_vars(SendCopy(p, t))

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

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

\* VEC-G05: for every cut that may be asked for, every visible row has an entry in a partition that a
\* new scan would list. Also after a crash, while it replays and after a resync.
NoLostRow ==
    held => \A n \in low..app : \A s \in Slots : LiveAt(s, n) =>
        \E p \in Tree : \E e \in ents[p] : e.s = s /\ VisibleAt(s, e.a, n)

\* VEC-G03: a resync or a drop leaves no entry of the old index.
SingleGeneration == \A p \in Parts : \A e \in ents[p] : e.g = gen

\* VEC-G05: a split or merge ends.
DrainEnds == \A p \in Parts : (st[p] = "draining") ~> (st[p] # "draining")

=============================================================================
