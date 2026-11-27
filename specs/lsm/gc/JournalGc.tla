----------------------------- MODULE JournalGc -----------------------------
(***************************************************************************)
(* Design spec of journal values and journal GC in nabedra-kv (module lsm, *)
(* level L3). Plan: plans/native-storage-engine.md, section 2, "Journal    *)
(* values (Raft log bytes)" and "Flush and compaction". Code: gc.rs,       *)
(* store/journal_gc.rs, store/mod.rs (flush_one, retirable_segments,       *)
(* retire_unreachable). Register: docs/testing/guarantees/lsm.md, LSM-G01  *)
(* and LSM-G02, scenarios NS03, NS04, NS05 and NS13.                       *)
(*                                                                         *)
(* One store. Actors: two writers, the journal thread, the flusher, the    *)
(* compactor, journal GC and one snapshot reader. Each key is its own      *)
(* group scope, so the two keys are the two ranges of the L3 bound.        *)
(*                                                                         *)
(* Abstractions (the byte layout, torn records and directory entries are   *)
(* modelled in Lsm.tla, whose G02_ExactPrefix this spec relies on):        *)
(*  - a journal record is one batch of one operation on one key;           *)
(*  - power loss keeps an exact prefix of the journal between the last     *)
(*    completed sync and the last write (Lsm.tla checks that recovery      *)
(*    yields such a prefix); a kill keeps every written record;            *)
(*  - segment numbers are never reused (the code gives a recycled file a   *)
(*    new number and epoch), so a retired number is a recycled segment and *)
(*    a locator into it reads as VALUE_RECYCLED;                           *)
(*  - every value is a journal value (put_journal), as Raft log entries    *)
(*    are; a value is named by the sequence of the put that wrote it;      *)
(*  - all runs form one set; compaction drops shadowed versions under the  *)
(*    snapshot rule; an open is one step (its crash points are in Lsm.tla).*)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS
    Keys,        \* user keys; each key is its own group scope
    Writers,     \* concurrent writer clients
    MaxSeq,      \* sequence numbers 1..MaxSeq
    SegCap,      \* records per journal segment
    MaxSegs,     \* segment numbers 1..MaxSegs, never reused
    MaxCrashes,  \* Kill plus PowerLoss budget
    Mutant       \* "none" in the baselines; a mutant name in mutants/*.cfg

Segs == 1..MaxSegs
Seqs == 1..MaxSeq
Corrupt == -1          \* a read of a locator whose segment was recycled

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Max2(a, b) == IF a >= b THEN a ELSE b
Range(f) == {f[i] : i \in DOMAIN f}

\* A journal record. op: "jput" (put_journal), "del", or "reloc" (a Relocate
\* pair: the key, the old locator's segment and the value it moves).
Rec(s, k, op, v, old) == [s |-> s, k |-> k, op |-> op, v |-> v, old |-> old]
\* An LSM entry: the key, its sequence, its kind, the value it names and, for
\* a journal value, the segment its locator points into.
Ent(k, s, op, v, sg) == [k |-> k, s |-> s, op |-> op, v |-> v, sg |-> sg]
NoEnt == Ent(CHOOSE k \in Keys : TRUE, 0, "none", 0, 0)

RdIdle == [ph |-> "idle", seq |-> 0, E |-> {}]

VARIABLES
    \* ---- disk (durable; written records survive a kill) ----
    seg,        \* segment number -> "none" | "live" | "free"
    jos,        \* segment -> records written to it, in order
    runs,       \* entries of the runs the durable manifest references
    fl,         \* flushed_seq in the durable manifest
    \* ---- process (volatile) ----
    status,     \* "open" | "down"
    wst,        \* writer -> "idle" | "busy"
    queue,      \* submitted items not yet written: [k, op, v, old, s, w]
    nextSeq,    \* the sequence the next submission takes
    lastW,      \* last sequence written (and applied: visible)
    synced,     \* last sequence a completed sync covers
    act,        \* active segment
    segLast,    \* segment -> last sequence it holds once it is not active
    mem,        \* active memtable
    imm,        \* immutable memtable: [e, last], or [e |-> {}, last |-> 0]
    cleared,    \* segment -> sequence of its last relocation (0: not cleared)
    rd,         \* the snapshot reader
    \* ---- ghost ----
    hist,       \* the oracle: seq -> [k, op, v] of each client write
    crashes

vars == <<seg, jos, runs, fl, status, wst, queue, nextSeq, lastW, synced, act,
          segLast, mem, imm, cleared, rd, hist, crashes>>

ImmNone == [e |-> {}, last |-> 0]
HNone == [k |-> CHOOSE k \in Keys : TRUE, op |-> "none", v |-> 0]

-----------------------------------------------------------------------------
(* Reads (plan "Read path"; store/read.rs). A locator resolves only while  *)
(* its segment is live and holds the record that wrote the value.          *)

AllE == mem \cup imm.e \cup runs

Newest(E, k, bound) ==
    LET C == {e \in E : e.k = k /\ e.s <= bound}
    IN IF C = {} THEN NoEnt ELSE CHOOSE e \in C : \A f \in C : f.s <= e.s

Holds(sg, v) ==
    /\ seg[sg] = "live"
    /\ \E i \in DOMAIN jos[sg] : jos[sg][i].op \in {"jput", "reloc"} /\ jos[sg][i].v = v

ValueOf(e) ==
    IF e.op \in {"none", "del"} THEN 0
    ELSE IF Holds(e.sg, e.v) THEN e.v ELSE Corrupt

View(k) == ValueOf(Newest(AllE, k, lastW))
SnapView(k) == ValueOf(Newest(rd.E, k, rd.seq))

\* The oracle: the value of k after the client writes 1..p (relocations are
\* not client writes and change no value).
StateAt(p, k) ==
    LET W == {s \in 1..p : hist[s].k = k /\ hist[s].op \in {"jput", "del"}}
    IN IF W = {} THEN 0 ELSE IF hist[Max(W)].op = "del" THEN 0 ELSE hist[Max(W)].v

-----------------------------------------------------------------------------
(* The rule in gc.rs, `retirable`: not active, flushed, durable, and       *)
(* unreachable: (1) no live run, memtable or pinned snapshot points into   *)
(* it, or (2) cleared by relocation, the last relocation visible and       *)
(* durable and no snapshot older than it.                                  *)

Pinned == IF rd.ph = "snap" /\ Mutant /= "unpinned_snapshot" THEN rd.E ELSE {}
Referenced(s) == \E e \in AllE \cup Pinned : e.op = "jput" /\ e.sg = s
\* FileTracker::min_referenced_segment: rule 1 retires only segments below
\* the lowest segment any live run, memtable or pinned snapshot points into.
BelowReferenced(s) == \A e \in AllE \cup Pinned : e.op = "jput" => s < e.sg

RelocUnreachable(s) ==
    /\ cleared[s] > 0
    /\ cleared[s] <= lastW
    /\ cleared[s] <= synced \/ Mutant = "gc_ignores_durable"
    /\ rd.ph = "idle" \/ rd.seq >= cleared[s] \/ Mutant = "gc_ignores_snapshot"

Retirable(s) ==
    /\ seg[s] = "live"
    /\ s /= act
    /\ segLast[s] <= fl \/ Mutant = "retire_ignores_flushed"
    /\ segLast[s] <= synced
    /\ BelowReferenced(s) \/ RelocUnreachable(s)

-----------------------------------------------------------------------------

Init ==
    /\ seg = [s \in Segs |-> IF s = 1 THEN "live" ELSE "none"]
    /\ jos = [s \in Segs |-> <<>>]
    /\ runs = {}
    /\ fl = 0
    /\ status = "open"
    /\ wst = [w \in Writers |-> "idle"]
    /\ queue = <<>>
    /\ nextSeq = 1
    /\ lastW = 0
    /\ synced = 0
    /\ act = 1
    /\ segLast = [s \in Segs |-> MaxSeq + 1]
    /\ mem = {}
    /\ imm = ImmNone
    /\ cleared = [s \in Segs |-> 0]
    /\ rd = RdIdle
    /\ hist = [s \in Seqs |-> HNone]
    /\ crashes = 0

-----------------------------------------------------------------------------
(* Writers: Store::submit of a put_journal or a delete (a Raft append, or  *)
(* a purge or truncation range delete).                                    *)

Submit(w, k, op) ==
    /\ status = "open"
    /\ wst[w] = "idle"
    /\ nextSeq <= MaxSeq
    /\ queue' = Append(queue, [k |-> k, op |-> op, v |-> IF op = "jput" THEN nextSeq ELSE 0,
                               old |-> 0, s |-> nextSeq, w |-> w])
    /\ hist' = [hist EXCEPT ![nextSeq] = [k |-> k, op |-> op,
                                          v |-> IF op = "jput" THEN nextSeq ELSE 0]]
    /\ nextSeq' = nextSeq + 1
    /\ wst' = [wst EXCEPT ![w] = "busy"]
    /\ UNCHANGED <<seg, jos, runs, fl, status, lastW, synced, act, segLast, mem, imm,
                   cleared, rd, crashes>>

SubmitAny == \E w \in Writers, k \in Keys, op \in {"jput", "del"} : Submit(w, k, op)

-----------------------------------------------------------------------------
(* The journal thread (journal/writer.rs). One record per step: pwrite it, *)
(* then apply it to the memtable in sequence order (an unsynced write is   *)
(* visible once its bytes reach the OS). A Relocate pair applies only if   *)
(* the key's newest version at first_seq - 1 is still the old locator.     *)

ApplyRec(r, sg) ==
    IF r.op = "jput" THEN {Ent(r.k, r.s, "jput", r.v, sg)}
    ELSE IF r.op = "del" THEN {Ent(r.k, r.s, "del", 0, 0)}
    ELSE LET cur == Newest(AllE, r.k, r.s - 1)
         IN IF Mutant = "reloc_unconditional"
               \/ (cur.op = "jput" /\ cur.sg = r.old /\ cur.v = r.v)
            THEN {Ent(r.k, r.s, "jput", r.v, sg)}
            ELSE {}

JournalWrite ==
    /\ status = "open"
    /\ queue /= <<>>
    /\ Len(jos[act]) < SegCap
    /\ LET it == Head(queue)
           r == Rec(it.s, it.k, it.op, it.v, it.old)
       IN /\ jos' = [jos EXCEPT ![act] = Append(@, r)]
          /\ mem' = mem \cup ApplyRec(r, act)
          /\ lastW' = it.s
          /\ queue' = Tail(queue)
          \* a NoSync waiter completes here; GC never waits (journal_gc)
          /\ wst' = IF it.w \in Writers THEN [wst EXCEPT ![it.w] = "idle"] ELSE wst
    /\ UNCHANGED <<seg, runs, fl, status, nextSeq, synced, act, segLast, imm, cleared, rd,
                   hist, crashes>>

\* ensure_room: roll to a new segment (segment::activate) when the active one
\* is full.
JournalRoll ==
    /\ status = "open"
    /\ queue /= <<>>
    /\ Len(jos[act]) = SegCap
    /\ \E ns \in Segs :
        /\ seg[ns] = "none"
        /\ \A t \in Segs : seg[t] = "none" => ns <= t
        /\ seg' = [seg EXCEPT ![ns] = "live"]
        /\ act' = ns
        /\ segLast' = [segLast EXCEPT ![act] = lastW]
    /\ UNCHANGED <<jos, runs, fl, status, wst, queue, nextSeq, lastW, synced, mem, imm,
                   cleared, rd, hist, crashes>>

\* sync_written: one fdatasync covers every written record. The journal
\* thread syncs only on request: a Sync waiter (ClientSync, a client choice)
\* or journal_gc, which calls sync_journal once a relocation is visible but
\* not durable (JournalSync, the request GC liveness relies on).
SyncStep ==
    /\ status = "open"
    /\ synced < lastW
    /\ synced' = lastW
    /\ UNCHANGED <<seg, jos, runs, fl, status, wst, queue, nextSeq, lastW, act, segLast,
                   mem, imm, cleared, rd, hist, crashes>>

GcWantsSync ==
    /\ Mutant /= "gc_no_sync"
    /\ \E s \in Segs : cleared[s] > 0 /\ cleared[s] <= lastW /\ cleared[s] > synced

JournalSync == GcWantsSync /\ SyncStep
ClientSync == SyncStep

-----------------------------------------------------------------------------
(* Flush and compaction (plan "Flush and compaction"; store/mod.rs).       *)

FreezeMemtable ==
    /\ status = "open"
    /\ mem /= {}
    /\ imm = ImmNone
    /\ imm' = [e |-> mem, last |-> lastW]
    /\ mem' = {}
    /\ UNCHANGED <<seg, jos, runs, fl, status, wst, queue, nextSeq, lastW, synced, act,
                   segLast, cleared, rd, hist, crashes>>

\* flush_one: sync_journal (no run or edit may reference journal bytes that
\* are not durable), write and sync the run, then the manifest edit
\* {add run, flushed_seq}. The run file and the edit are atomic here (their
\* crash points are in Lsm.tla).
FlushMemtable ==
    /\ status = "open"
    /\ imm /= ImmNone
    /\ synced' = IF Mutant = "flush_no_journal_sync" THEN synced ELSE lastW
    /\ runs' = runs \cup imm.e
    /\ fl' = Max2(fl, imm.last)
    /\ imm' = ImmNone
    /\ UNCHANGED <<seg, jos, status, wst, queue, nextSeq, lastW, act, segLast, mem,
                   cleared, rd, hist, crashes>>

\* Compaction keeps the newest version of each key per snapshot stripe
\* (compaction.rs), so a shadowed locator leaves the runs.
Stripe(s) == IF rd.ph = "snap" /\ s > rd.seq THEN 1 ELSE 0
Compacted(E) ==
    {e \in E : ~\E f \in E : f.k = e.k /\ f.s > e.s /\ Stripe(f.s) = Stripe(e.s)}

Compact ==
    /\ status = "open"
    /\ Compacted(runs) /= runs
    /\ runs' = Compacted(runs)
    /\ UNCHANGED <<seg, jos, fl, status, wst, queue, nextSeq, lastW, synced, act, segLast,
                   mem, imm, cleared, rd, hist, crashes>>

-----------------------------------------------------------------------------
(* Journal GC (store/journal_gc.rs, relocation_pass). For a flushed,       *)
(* durable, referenced segment it scans the locators visible at its scan   *)
(* snapshot, submits NoSync Relocate records for them, and marks the       *)
(* segment cleared at the sequence of the last relocation, at least the   *)
(* scan sequence.                                                          *)

LiveIn(s) == {k \in Keys : LET e == Newest(AllE, k, lastW) IN e.op = "jput" /\ e.sg = s}

RelocItems(s, L, first) ==
    LET ks == CHOOSE f \in [1..Cardinality(L) -> L] : Range(f) = L
    IN [i \in 1..Cardinality(L) |->
          LET e == Newest(AllE, ks[i], lastW)
          IN [k |-> ks[i], op |-> "reloc", v |-> e.v, old |-> s, s |-> first + i - 1,
              w |-> "gc"]]

GcRelocate(s) ==
    /\ status = "open"
    /\ seg[s] = "live"
    /\ s /= act
    /\ cleared[s] = 0
    /\ segLast[s] <= fl
    /\ segLast[s] <= synced
    /\ Referenced(s)
    /\ LET L == LiveIn(s)
           n == Cardinality(L)
       IN /\ nextSeq + n - 1 <= MaxSeq
          /\ queue' = queue \o RelocItems(s, L, nextSeq)
          /\ nextSeq' = nextSeq + n
          \* relocated = max(commit seqs, scan_seq)
          /\ cleared' = [cleared EXCEPT ![s] =
                IF Mutant = "gc_no_scan_floor"
                THEN (IF n = 0 THEN segLast[s] ELSE nextSeq + n - 1)
                ELSE Max2(nextSeq + n - 1, lastW)]
    /\ UNCHANGED <<seg, jos, runs, fl, status, wst, lastW, synced, act, segLast, mem, imm,
                   rd, hist, crashes>>

\* retire_unreachable: the manifest edit drops the segment from the segment
\* set (synced), then Journal::retire returns it to the free pool, where a
\* later roll recycles it. Atomic here.
RetireSegment(s) ==
    /\ status = "open"
    /\ Retirable(s)
    /\ seg' = [seg EXCEPT ![s] = "free"]
    /\ jos' = [jos EXCEPT ![s] = <<>>]
    /\ cleared' = [cleared EXCEPT ![s] = 0]
    /\ UNCHANGED <<runs, fl, status, wst, queue, nextSeq, lastW, synced, act, segLast, mem,
                   imm, rd, hist, crashes>>

GcRelocateAny == \E s \in Segs : GcRelocate(s)
RetireAny == \E s \in Segs : RetireSegment(s)

-----------------------------------------------------------------------------
(* Snapshots (plan "Snapshots"): pin the visible sequence, the memtables   *)
(* and the version.                                                        *)

TakeSnapshot ==
    /\ status = "open"
    /\ rd.ph = "idle"
    /\ rd' = [ph |-> "snap", seq |-> lastW, E |-> {e \in AllE : e.s <= lastW}]
    /\ UNCHANGED <<seg, jos, runs, fl, status, wst, queue, nextSeq, lastW, synced, act,
                   segLast, mem, imm, cleared, hist, crashes>>

ReleaseSnapshot ==
    /\ rd.ph = "snap"
    /\ rd' = RdIdle
    /\ UNCHANGED <<seg, jos, runs, fl, status, wst, queue, nextSeq, lastW, synced, act,
                   segLast, mem, imm, cleared, hist, crashes>>

-----------------------------------------------------------------------------
(* Faults and recovery. A crash keeps the journal records up to a cut c:   *)
(* every written record for a kill, and any c between the last completed  *)
(* sync and the last write for a power loss. Open replays the records     *)
(* above flushed_seq in sequence order with the same Relocate check, and  *)
(* writing resumes after max(c, flushed_seq) (the void of finish_open).    *)

RECURSIVE Replay(_, _, _)
Replay(R, E, B) ==
    IF R = {} THEN E
    ELSE LET r == CHOOSE x \in R : \A y \in R : x[1].s <= y[1].s
             rec == r[1]
             add == IF rec.op = "jput" THEN {Ent(rec.k, rec.s, "jput", rec.v, r[2])}
                    ELSE IF rec.op = "del" THEN {Ent(rec.k, rec.s, "del", 0, 0)}
                    ELSE LET cur == Newest(E \cup B, rec.k, rec.s - 1)
                         IN IF Mutant = "reloc_unconditional"
                               \/ (cur.op = "jput" /\ cur.sg = rec.old /\ cur.v = rec.v)
                            THEN {Ent(rec.k, rec.s, "jput", rec.v, r[2])} ELSE {}
         IN Replay(R \ {r}, E \cup add, B)

Crash(c) ==
    /\ status = "open"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ jos' = [s \in Segs |-> SelectSeq(jos[s], LAMBDA r : r.s <= c)]
    /\ status' = "down"
    /\ wst' = [w \in Writers |-> "idle"]
    /\ queue' = <<>>
    /\ mem' = {}
    /\ imm' = ImmNone
    /\ cleared' = [s \in Segs |-> 0]
    /\ rd' = RdIdle
    /\ lastW' = c
    /\ synced' = c
    \* writes above the cut never happened in the recovered timeline
    /\ hist' = [s \in Seqs |-> IF s <= Max2(c, fl) THEN hist[s] ELSE HNone]
    /\ nextSeq' = Max2(c, fl) + 1
    /\ UNCHANGED <<seg, runs, fl, act, segLast>>

Kill == Crash(lastW)
PowerLoss == \E c \in synced..lastW : c < lastW /\ Crash(c)

Open ==
    /\ status = "down"
    /\ LET R == UNION {{<<jos[s][i], s>> : i \in DOMAIN jos[s]} :
                          s \in {t \in Segs : seg[t] = "live"}}
           above == {r \in R : r[1].s > fl}
           end == Max2(lastW, fl)
       IN /\ mem' = Replay(above, {}, runs)
          /\ lastW' = end
          /\ synced' = end
    /\ status' = "open"
    /\ UNCHANGED <<seg, jos, runs, fl, wst, queue, nextSeq, act, segLast, imm, cleared, rd,
                   hist, crashes>>

-----------------------------------------------------------------------------

Next ==
    \/ SubmitAny
    \/ JournalWrite
    \/ JournalRoll
    \/ JournalSync
    \/ ClientSync
    \/ FreezeMemtable
    \/ FlushMemtable
    \/ Compact
    \/ GcRelocateAny
    \/ RetireAny
    \/ TakeSnapshot
    \/ ReleaseSnapshot
    \/ Kill
    \/ PowerLoss
    \/ Open

Spec == Init /\ [][Next]_vars

\* Fairness for the liveness baseline, one mechanism per assumption:
\*  - JournalWrite, JournalRoll: the kv-journal thread loop, woken by the
\*    `work` condvar on every submit;
\*  - JournalSync: journal_gc's sync_journal while a relocation is visible
\*    but not durable (store/journal_gc.rs, journal_gc);
\*  - FlushMemtable: the kv-flush thread (schedule_flush) flushes every
\*    frozen memtable;
\*  - RetireSegment: journal_gc on a compaction thread, rescheduled every
\*    GC_WAIT (20 ms) while a segment is cleared (journal_gc returns
\*    Some(GC_WAIT));
\*  - Open: the pool mount retries the open after a crash.
\* No fairness on Submit, ClientSync, FreezeMemtable (a size trigger: the
\* memtable budget or max_replay_bytes), GcRelocate (it runs only while the journal is over
\* budget), Compact, snapshots or faults.
Fairness ==
    /\ WF_vars(JournalWrite) /\ WF_vars(JournalRoll) /\ WF_vars(JournalSync)
    /\ WF_vars(FlushMemtable)
    /\ WF_vars(RetireAny) /\ WF_vars(Open)

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ seg \in [Segs -> {"none", "live", "free"}]
    /\ \A s \in Segs : Len(jos[s]) <= SegCap
    /\ status \in {"open", "down"}
    /\ wst \in [Writers -> {"idle", "busy"}]
    /\ nextSeq \in 1..(MaxSeq + 1)
    /\ {lastW, synced, fl} \subseteq 0..MaxSeq
    /\ act \in Segs
    /\ cleared \in [Segs -> 0..MaxSeq]
    /\ crashes <= MaxCrashes

\* LSM-G01, LSM-G02, NS13: the open store serves the oracle at its visible
\* sequence; in particular no live locator points into a recycled segment,
\* before or after any crash.
G02_ExactPrefix ==
    status = "open" => \A k \in Keys : View(k) = StateAt(lastW, k)

\* NS03: a snapshot reads the oracle at its sequence through flush,
\* compaction, relocation and segment retirement.
NS03_SnapshotStable ==
    rd.ph = "snap" => \A k \in Keys : SnapView(k) = StateAt(rd.seq, k)

\* LSM-G02, NS13: a retired segment is never needed by replay: flushed_seq
\* covers it.
NS13_RetiredFlushed ==
    \A s \in Segs : seg[s] = "free" => segLast[s] <= fl

\* Every segment number is in use: a bound of the model, not of the code
\* (create_free takes the next number), after which the journal cannot roll.
SegmentsExhausted == \A t \in Segs : seg[t] /= "none"

\* Liveness: a segment cleared by relocation is retired (or un-cleared by a
\* crash) once no snapshot holds it back.
L_ClearedRetires ==
    \A s \in Segs : (cleared[s] > 0 /\ rd.ph = "idle") ~>
                    (cleared[s] = 0 \/ seg[s] = "free" \/ rd.ph = "snap" \/ SegmentsExhausted)

=============================================================================
