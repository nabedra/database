-------------------------------- MODULE Lsm --------------------------------
(***************************************************************************)
(* Design spec of the nabedra-kv storage engine (module lsm, level L3).    *)
(* Plan: plans/native-storage-engine.md, sections 2 (engine) and 3        *)
(* (recovery and damage). Register: docs/testing/guarantees/lsm.md.        *)
(*                                                                         *)
(* One store on one node. The concurrent actors are the writers, the       *)
(* journal thread, the flusher, the compactor and one snapshot reader.     *)
(* Each key is its own group scope, so the two keys are the two "ranges"   *)
(* of the L3 bound.                                                        *)
(*                                                                         *)
(* The disk follows the phase 9 FaultFs model (server/rt/src/fs/fault.rs): *)
(* every file has an OS layer and a durable layer, a file sync makes its   *)
(* contents durable, and a directory entry becomes durable only through a  *)
(* sync of its directory. Kill keeps the OS layer. PowerLoss keeps the     *)
(* durable layer and, for each unsynced journal slot and each pending      *)
(* directory entry, independently the new state, the old state or (for a   *)
(* slot) a torn record; a whole 4 KiB page that mixes synced and unsynced  *)
(* bytes may also be destroyed (FaultFs PowerLoss::destroy_4k).            *)
(*                                                                         *)
(* Abstractions (each is recorded in the plan's design review section):    *)
(*  - a journal slot is one record; a record is one write batch on one key *)
(*  - a run file is written and fsynced in one step, and so is a manifest  *)
(*    edit; their crash points are covered by the directory-entry choice   *)
(*    and by the manifest's all-or-nothing final record (plan case L3)     *)
(*  - segment numbers are never reused, so a segment has no epoch          *)
(*  - two levels, L0 and L1 (the bottom); no trivial moves, no range       *)
(*    deletes, no journal GC relocation, no ENOSPC or fsync errors         *)
(* Values are integers: 0 is "no value", a positive value is the sequence  *)
(* of the put that wrote it, Gone (-1) is a read of a deleted file and     *)
(* Corrupt (-2) a read of a damaged record.                                *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,          \* user keys; each key is its own group scope
    Writers,       \* concurrent writer clients
    NoKey,         \* a model value: the key field of a slot without a key
    MaxSeq,        \* sequence numbers 1..MaxSeq (bounds the writes per incarnation)
    SegCap,        \* records per journal segment before a roll
    MaxSegs,       \* journal segment numbers 1..MaxSegs
    MaxFile,       \* file numbers 1..MaxFile, shared by runs and manifests
    MaxImm,        \* immutable memtables waiting for flush
    MaxCrashes,    \* Kill plus PowerLoss budget
    MaxMedia,      \* media damage of a synced journal record (bit flip or EIO)
    MaxDiskLoss,   \* whole-volume loss (FaultFs Wipe)
    JournalValues, \* TRUE: writers may use put_journal (locator values)
    EarlyRoll,     \* TRUE: a roll may leave the rest of a segment unused
    Reader,        \* TRUE: the snapshot reader runs
    Mutant         \* "none" in Lsm.cfg; a mutant name in mutants/*.cfg

Segs == 1..MaxSegs
FileIds == 1..MaxFile
Seqs == 1..MaxSeq
Ops == IF JournalValues THEN {"put", "del", "jput"} ELSE {"put", "del"}
Gone == -1
Corrupt == -2

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(a, b) == IF a >= b THEN a ELSE b
Min2(a, b) == IF a <= b THEN a ELSE b
Range(f) == {f[i] : i \in DOMAIN f}

-----------------------------------------------------------------------------
(* Journal slots (plan section 2, "Journal format").                       *)
(* st: "ok" valid record, "torn" header damaged (scope unknown), "dmg"    *)
(* payload damaged (scope known), "eio" unreadable, "zero" end marker.    *)
(* lo..hi: the sequences the record takes (hi > lo only for a void pad).   *)
(* dm: durable_seq, the highest sequence a completed sync covered when the *)
(* record was written. pg: the 4 KiB page the record lies in.              *)

Slot(st, lo, hi, k, op, dm, pg) ==
    [st |-> st, lo |-> lo, hi |-> hi, key |-> k, op |-> op, dm |-> dm, pg |-> pg]
ZeroSlot == Slot("zero", 0, 0, NoKey, "none", 0, 0)
TornSlot == Slot("torn", 0, 0, NoKey, "none", 0, 0)

\* An LSM entry: user key, sequence, kind, and for a journal value the
\* segment its locator points into (0 otherwise).
Ent(k, s, op, sg) == [k |-> k, s |-> s, op |-> op, sg |-> IF op = "jput" THEN sg ELSE 0]
NoEnt == [k |-> NoKey, s |-> 0, op |-> "none", sg |-> 0]

\* A run as versions and manifests know it: file id and contents. (The
\* manifest records key and sequence ranges; the contents stand for them.)
NoRun == [id |-> 0, c |-> {}]
EmptyVer == [l0 |-> <<>>, l1 |-> {}]

\* The logical contents of a manifest (its snapshot record plus edits).
ManState(fl, l0, l1, segs, nf, q) ==
    [fl |-> fl, l0 |-> l0, l1 |-> l1, segs |-> segs, nf |-> nf, q |-> q]
NoMan == ManState(0, <<>>, {}, {}, 0, {})   \* no file (real manifests have nf >= 2)
NoFile == {}                               \* no run file (real runs are non-empty)

HNone == [k |-> NoKey, op |-> "none"]
RdIdle == [ph |-> "idle", seq |-> 0, mem |-> {}, l0 |-> <<>>, l1 |-> {}]
FlIdle == [ph |-> "idle", run |-> NoRun, last |-> 0, R |-> {}]
CpIdle == [ph |-> "idle", ins |-> {}, out |-> NoRun]
RecNone == [ms |-> NoMan, m |-> {}, next |-> 0, q |-> {}, act |-> 0, pg |-> FALSE, wp |-> 0]
PendNone == [ack |-> 0, end |-> 0, sub |-> 0, cert |-> FALSE, media |-> FALSE, eio |-> FALSE]
LastOpenNone == [ack |-> 0, end |-> 0, sub |-> 0, cert |-> FALSE, media |-> FALSE,
                 eio |-> FALSE, quiet |-> FALSE, prevEnd |-> 0, view |-> <<>>,
                 prevView |-> <<>>]

VARIABLES
    \* ---- disk: journal directory (kept by Kill; PowerLoss chooses) ----
    segName,    \* OS namespace: "none" | "free" | "live"
    segNameD,   \* durable namespace
    segFirst,   \* first_seq in the segment header (header writes are synced)
    jos,        \* OS layer: sequence of slots per segment
    jdur,       \* durable layer
    \* ---- disk: runs/ and the store directory ----
    rfile,      \* [o, d]: contents the OS / durable name runs/<id>.run resolves to
    mfile,      \* [o, d]: manifest contents behind MANIFEST-<id>
    cur,        \* manifest number CURRENT names in the OS namespace (0 = none)
    curD,       \* durable namespace
    \* ---- process (volatile; reset by every crash) ----
    status,     \* "open" | "down" | "recM" | "recC" | "recF" | "refused"
    wst,        \* writer -> "idle" | "busy"
    queue,      \* submitted, not yet written: [w, k, op, sync, s]
    grp,        \* written, not yet published: [w, k, op, sync, s, sg]
    nextSeq,    \* sequence the next submit takes
    lastW,      \* last sequence written to the journal (OS)
    synced,     \* last sequence a completed journal sync covers
    vis,        \* visible_seq
    durPub,     \* durable_seq as published to waiters
    act,        \* active segment
    padNext,    \* the next record starts a new 4 KiB page (after a sync)
    wpos,       \* slot the next record takes in the active segment
    rollPending,\* a roll renamed a segment and has not synced the directory yet
    mem,        \* active memtable (entries)
    imm,        \* immutable memtables, oldest first: [e, last]
    ver,        \* current version: [l0 (oldest first), l1]
    flushed,    \* flushed_seq of the recorded state
    recSegs,    \* segment set the manifest records
    nextFile,   \* next file number
    manNo,      \* the manifest file edits are appended to
    quar,       \* quarantined scopes (keys)
    fl,         \* flusher
    cp,         \* compactor
    obsolete,   \* runs removed from the version whose file is not deleted yet
    rd,         \* the snapshot reader
    rec,        \* recovery scratch between open steps
    \* ---- ghost state and fault budgets ----
    hist,       \* the oracle: seq -> [k, op], for the current incarnation
    ack,        \* highest durable_seq ever published in this incarnation
    lastSub,    \* highest sequence submitted in this incarnation
    crashes, media, wipes,
    pend,       \* what the open in progress found, for the checks
    justOpened, \* the previous step completed an open
    lastOpen,   \* facts about the last completed open
    prev,       \* end and view of the last completed open, for idempotence
    quiet       \* no submit, media damage or wipe since the last completed open

disk == <<segName, segNameD, segFirst, jos, jdur, rfile, mfile, cur, curD>>
proc == <<status, wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act,
          padNext, wpos, rollPending, mem, imm, ver, flushed, recSegs, nextFile, manNo,
          quar, fl, cp, obsolete, rd, rec>>
ghost == <<hist, ack, lastSub, crashes, media, wipes, pend, justOpened,
           lastOpen, prev, quiet>>
\* The ghost variables an ordinary step leaves alone (it clears justOpened).
ghostNJ == <<hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>
vars == <<disk, proc, ghost>>

-----------------------------------------------------------------------------
(* Reads (plan section 2, "Read path" and "Snapshots").                    *)

ImmE == UNION {imm[i].e : i \in DOMAIN imm}
RunsOf(v) == Range(v.l0) \cup v.l1

\* Newest entry for k at or below bound in a set of entries.
Newest(E, k, bound) ==
    LET C == {e \in E : e.k = k /\ e.s <= bound}
    IN IF C = {} THEN NoEnt ELSE CHOOSE e \in C : \A f \in C : f.s <= e.s

\* A journal value resolves through its locator: the segment must still be
\* live (a retired segment is a deleted file) and the record intact.
JVal(e) ==
    IF segName[e.sg] /= "live" THEN Gone
    ELSE IF \E i \in DOMAIN jos[e.sg] :
                jos[e.sg][i].st = "ok" /\ jos[e.sg][i].lo = e.s /\ jos[e.sg][i].op = "jput"
         THEN e.s ELSE Corrupt

ValueOf(e) == IF e.op = "del" THEN 0 ELSE IF e.op = "put" THEN e.s ELSE JVal(e)

\* Memtables, then L0 newest first, then L1. The first source holding a
\* version at or below bound decides, as the read path searches in order.
ReadSrc(memE, l0, l1, k, bound) ==
    LET m == Newest(memE, k, bound)
        L0hit == {i \in DOMAIN l0 : Newest(l0[i].c, k, bound).s /= 0}
        L1hit == {d \in l1 : Newest(d.c, k, bound).s /= 0}
    IN IF m.s /= 0 THEN ValueOf(m)
       ELSE IF L0hit /= {} THEN ValueOf(Newest(l0[Max(L0hit)].c, k, bound))
       ELSE IF L1hit /= {} THEN ValueOf(Newest((CHOOSE d \in L1hit : TRUE).c, k, bound))
       ELSE 0

View(k) == ReadSrc(mem \cup ImmE, ver.l0, ver.l1, k, vis)
SnapView(k) == ReadSrc(rd.mem, rd.l0, rd.l1, k, rd.seq)

\* The oracle: the value of k after the writes with sequences 1..p.
StateAt(h, p, k) ==
    LET W == {s \in 1..p : h[s].k = k}
    IN IF W = {} THEN 0 ELSE IF h[Max(W)].op = "del" THEN 0 ELSE Max(W)

-----------------------------------------------------------------------------
(* Flush and compaction output (plan "Snapshots"; compaction.rs Filter).   *)

Snaps == IF rd.ph = "snap" THEN {rd.seq} ELSE {}
Stripe(s, sn) == Cardinality({x \in sn : x < s})
Earliest(sn) == IF sn = {} THEN MaxSeq + 1 ELSE Min(sn)

\* Keep the newest version of each key per snapshot stripe; at the bottom,
\* drop a tombstone that every reader already sees.
Filter(E, sn, bottom) ==
    {e \in E :
        \* A snapshot pins its version, so a live snapshot never reads this
        \* output; the stripes protect the journal values a snapshot needs
        \* (with the retire rule) and the tombstone rule below. Mutant
        \* snapshot_values_unprotected drops the stripe rule and the
        \* snapshot's locators from the retire rule together; either alone is
        \* an equivalent mutant.
        /\ ~\E f \in E : f.k = e.k /\ f.s > e.s
              /\ (Mutant = "snapshot_values_unprotected" \/ Stripe(f.s, sn) = Stripe(e.s, sn))
        /\ ~(e.op = "del" /\ bottom /\
             (e.s <= Earliest(sn) \/ Mutant = "tombstone_ignores_snapshots"))}

KeysOf(E) == {e.k : e \in E}

\* Everything that can still read a journal value: memtables, the current
\* version, the snapshot's pinned memtables and version, and work in flight.
LiveEntries ==
    mem \cup ImmE \cup (UNION {d.c : d \in RunsOf(ver)})
    \cup (IF Mutant = "snapshot_values_unprotected" THEN {}
          ELSE rd.mem \cup (UNION {d.c : d \in RunsOf(rd)}))
    \cup fl.run.c \cup cp.out.c
RefSegs == {e.sg : e \in {x \in LiveEntries : x.op = "jput"}}

LiveSet == {s \in Segs : segName[s] = "live"}
NextLive(s) == LET L == {t \in LiveSet : t > s} IN IF L = {} THEN 0 ELSE Min(L)
SegLast(s) == IF NextLive(s) = 0 THEN lastW ELSE segFirst[NextLive(s)] - 1
UsedSegs == {s \in Segs : segName[s] /= "none" \/ segNameD[s] /= "none"}
NextSegNo == IF UsedSegs = {} THEN 1 ELSE Max(UsedSegs) + 1

\* gc.rs retirable: not active, flushed, durable, and below every segment a
\* live locator points into (rule 1; rule 2, relocation, is not modelled).
Retirable(F) ==
    {s \in LiveSet :
        /\ s /= act
        /\ SegLast(s) <= F
        /\ SegLast(s) <= synced
        /\ Mutant = "retire_ignores_locators" \/ \A r \in RefSegs : s < r}

\* segments_to_record: live segments whose first record is durable.
Recordable(R) == {s \in LiveSet : segFirst[s] <= synced} \ R

-----------------------------------------------------------------------------
(* Recovery (plan section 3, "Our rule"; journal/recovery.rs).             *)

SlotAt(s, i) == IF i <= Len(jos[s]) THEN jos[s][i] ELSE ZeroSlot

\* Valid records after position (s, i) in the live journal.
LaterValid(s, i) ==
    {p \in LiveSet \X (1..(SegCap + 1)) :
        /\ (p[1] = s /\ p[2] > i) \/ p[1] > s
        /\ p[2] <= Len(jos[p[1]])
        /\ jos[p[1]][p[2]].st = "ok"}
Evidence(s, i) ==
    LET V == LaterValid(s, i)
    IN IF V = {} THEN 0 ELSE Max({jos[p[1]][p[2]].dm : p \in V})
FirstValid(s, i) ==
    CHOOSE p \in LaterValid(s, i) :
        \A q \in LaterValid(s, i) : p[1] < q[1] \/ (p[1] = q[1] /\ p[2] <= q[2])

Done(s, i, nx, q, m) ==
    [ok |-> TRUE, why |-> "none", s |-> s, i |-> i, next |-> nx, q |-> q, m |-> m]
Refuse(why) ==
    [ok |-> FALSE, why |-> why, s |-> 0, i |-> 0, next |-> 0, q |-> {}, m |-> {}]

RECURSIVE Walk(_, _, _, _, _, _)
Walk(s, i, nx, q, m, F) ==
    IF i > SegCap /\ i > Len(jos[s]) THEN
        \* At::End: the segment is full
        LET ns == NextLive(s) IN
        IF ns = 0 THEN Done(s, i, nx, q, m)
        ELSE IF segFirst[ns] /= nx THEN Refuse("chain")
        ELSE Walk(ns, 1, nx, q, m, F)
    ELSE
    LET sl == SlotAt(s, i) IN
    IF sl.st = "ok" THEN
        IF sl.lo /= nx THEN Refuse("discontinuity")
        ELSE IF sl.op \in {"void", "pad"} \/ sl.key \in q THEN Walk(s, i + 1, sl.hi + 1, q, m, F)
        ELSE Walk(s, i + 1, sl.hi + 1, q,
                  IF sl.hi > F THEN m \cup {Ent(sl.key, sl.lo, sl.op, s)} ELSE m, F)
    ELSE IF sl.st = "eio" /\ Mutant /= "eio_as_torn" THEN Refuse("R2")
    ELSE IF sl.st /= "dmg" /\ NextLive(s) /= 0 /\ segFirst[NextLive(s)] = nx
            /\ Mutant /= "roll_gap_is_end" THEN
        \* the roll point of this segment: no sequence is lost
        Walk(NextLive(s), 1, nx, q, m, F)
    ELSE
    LET ev == Evidence(s, i) IN
    IF ev < nx \/ Mutant = "no_evidence_check" THEN Done(s, i, nx, q, m)   \* L1, L2
    ELSE IF sl.st = "dmg" THEN
        IF sl.hi <= F THEN Walk(s, i + 1, sl.hi + 1, q, m, F)
        ELSE Walk(s, i + 1, sl.hi + 1, q \cup {sl.key}, m, F)      \* G1
    ELSE
    LET fv == FirstValid(s, i)
        lo == jos[fv[1]][fv[2]].lo
    IN IF lo < nx THEN Refuse("discontinuity")
       ELSE IF lo /= nx /\ lo - 1 > F THEN Refuse("R1")
       ELSE Walk(fv[1], fv[2], lo, q, m, F)

\* Replay from the newest live segment whose first sequence is at or below
\* flushed_seq + 1.
RecJournal(ms) ==
    IF LiveSet = {} THEN Done(0, 1, ms.fl + 1, ms.q, {})
    ELSE LET C == {s \in LiveSet : segFirst[s] <= ms.fl + 1} IN
         IF C = {} THEN Refuse("starts after replay point")
         ELSE Walk(Max(C), 1, segFirst[Max(C)], ms.q, {}, ms.fl)

EmptyMan == ManState(0, <<>>, {}, {}, 1, {})

-----------------------------------------------------------------------------
(* Power loss images (FaultFs::power_loss and power_loss_with destroy_4k). *)

Synced(s, i) == i <= Len(jos[s]) /\ i <= Len(jdur[s]) /\ jos[s][i] = jdur[s][i]
OldSlot(s, i) == IF i <= Len(jdur[s]) THEN jdur[s][i] ELSE ZeroSlot
Width(s) == Max2(Len(jos[s]), Len(jdur[s]))

\* Pages holding both a synced record and an unsynced write. A destroyed
\* page loses both; the sync-alignment rule keeps this set empty.
DestroyablePages(s) ==
    {jos[s][i].pg : i \in {j \in 1..Len(jos[s]) : Synced(s, j) /\ jos[s][j].st = "ok"}}
    \cap {jos[s][i].pg : i \in {j \in 1..Len(jos[s]) : ~Synced(s, j)}}

SlotOptions(s, i, D) ==
    IF SlotAt(s, i).st = "ok" /\ SlotAt(s, i).pg \in D THEN {TornSlot}
    ELSE IF Synced(s, i) \/ SlotAt(s, i) = OldSlot(s, i) THEN {SlotAt(s, i)}
    ELSE {SlotAt(s, i), OldSlot(s, i), TornSlot}

RECURSIVE Trim(_)
Trim(q) == IF q /= <<>> /\ q[Len(q)] = ZeroSlot THEN Trim(SubSeq(q, 1, Len(q) - 1)) ELSE q

RECURSIVE SeqChoices(_, _, _)
SeqChoices(s, i, D) ==
    IF i > Width(s) THEN {<<>>}
    ELSE {<<x>> \o t : x \in SlotOptions(s, i, D), t \in SeqChoices(s, i + 1, D)}

SegImages(s) ==
    UNION {{Trim(q) : q \in SeqChoices(s, 1, D)} : D \in SUBSET DestroyablePages(s)}

\* All functions f on D with f[x] \in C[x], built one point at a time.
RECURSIVE Prod(_, _)
Prod(D, C) ==
    IF D = {} THEN {[x \in {} |-> 0]}
    ELSE LET x == CHOOSE y \in D : TRUE
         IN {[y \in (D \ {x}) \cup {x} |-> IF y = x THEN v ELSE g[y]] :
                g \in Prod(D \ {x}, C), v \in C[x]}

-----------------------------------------------------------------------------
(* Initial state: a store created and opened (FORMAT, journal/, runs/,     *)
(* MANIFEST and CURRENT durable; segment 1 live and empty).                *)

InitMan == ManState(0, <<>>, {}, {}, 2, {})

\* Every step but the last step of an open clears the facts about the open.
ClearOpen ==
    /\ justOpened' = FALSE
    /\ lastOpen' = LastOpenNone

ResetProc ==
    /\ wst' = [w \in Writers |-> "idle"]
    /\ queue' = <<>>
    /\ grp' = <<>>
    /\ nextSeq' = 0
    /\ lastW' = 0
    /\ synced' = 0
    /\ vis' = 0
    /\ durPub' = 0
    /\ act' = 0
    /\ padNext' = FALSE
    /\ wpos' = 0
    /\ rollPending' = FALSE
    /\ mem' = {}
    /\ imm' = <<>>
    /\ ver' = EmptyVer
    /\ flushed' = 0
    /\ recSegs' = {}
    /\ nextFile' = 0
    /\ manNo' = 0
    /\ quar' = {}
    /\ fl' = FlIdle
    /\ cp' = CpIdle
    /\ obsolete' = {}
    /\ rd' = RdIdle
    /\ rec' = RecNone

NoView == [k \in Keys |-> 0]

Init ==
    /\ segName = [s \in Segs |-> IF s = 1 THEN "live" ELSE "none"]
    /\ segNameD = segName
    /\ segFirst = [s \in Segs |-> IF s = 1 THEN 1 ELSE 0]
    /\ jos = [s \in Segs |-> <<>>]
    /\ jdur = jos
    /\ rfile = [i \in FileIds |-> [o |-> NoFile, d |-> NoFile]]
    /\ mfile = [i \in FileIds |-> IF i = 1 THEN [o |-> InitMan, d |-> InitMan]
                                  ELSE [o |-> NoMan, d |-> NoMan]]
    /\ cur = 1
    /\ curD = 1
    /\ status = "open"
    /\ wst = [w \in Writers |-> "idle"]
    /\ queue = <<>>
    /\ grp = <<>>
    /\ nextSeq = 1
    /\ lastW = 0
    /\ synced = 0
    /\ vis = 0
    /\ durPub = 0
    /\ act = 1
    /\ padNext = FALSE
    /\ wpos = 1
    /\ rollPending = FALSE
    /\ mem = {}
    /\ imm = <<>>
    /\ ver = EmptyVer
    /\ flushed = 0
    /\ recSegs = {}
    /\ nextFile = 2
    /\ manNo = 1
    /\ quar = {}
    /\ fl = FlIdle
    /\ cp = CpIdle
    /\ obsolete = {}
    /\ rd = RdIdle
    /\ rec = RecNone
    /\ hist = [s \in Seqs |-> HNone]
    /\ ack = 0
    /\ lastSub = 0
    /\ crashes = 0
    /\ media = 0
    /\ wipes = 0
    /\ pend = PendNone
    /\ justOpened = FALSE
    /\ lastOpen = LastOpenNone
    /\ prev = [end |-> 0, view |-> NoView]
    /\ quiet = TRUE

-----------------------------------------------------------------------------
(* Writers: Store::submit (plan section 2, "Group commit").                *)

Submit(w, k, op, sy) ==
    /\ status = "open"
    /\ wst[w] = "idle"
    /\ nextSeq <= MaxSeq
    /\ k \notin quar
    /\ queue' = Append(queue, [w |-> w, k |-> k, op |-> op, sync |-> sy, s |-> nextSeq])
    /\ nextSeq' = nextSeq + 1
    /\ wst' = [wst EXCEPT ![w] = "busy"]
    /\ hist' = [hist EXCEPT ![nextSeq] = [k |-> k, op |-> op]]
    /\ lastSub' = nextSeq
    /\ quiet' = FALSE
    /\ ClearOpen
    /\ UNCHANGED <<disk, status, grp, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, ver, flushed, recSegs, nextFile, manNo, quar,
                   fl, cp, obsolete, rd, rec, ack, crashes, media, wipes, pend, prev>>

SubmitAny == \E w \in Writers, k \in Keys, op \in Ops, sy \in BOOLEAN : Submit(w, k, op, sy)

-----------------------------------------------------------------------------
(* The journal thread (journal/writer.rs).                                 *)

LastPg(s) == IF jos[s] = <<>> THEN 0 ELSE jos[s][Len(jos[s])].pg

\* Writer::process: one pwrite of the queued records that fit in the active
\* segment. A group that follows a sync starts on a new 4 KiB page.
JournalWriteGroup ==
    /\ status = "open"
    /\ queue /= <<>>
    /\ wpos <= SegCap
    /\ ~rollPending \/ Mutant = "roll_no_dir_sync"
    /\ LET n == Min2(SegCap + 1 - wpos, Len(queue))
           b == SubSeq(queue, 1, n)
           before == SubSeq(jos[act], 1, wpos - 1)
           lastPg == IF before = <<>> THEN 0 ELSE before[Len(before)].pg
           pg == IF before = <<>> THEN 1 ELSE IF padNext THEN lastPg + 1 ELSE lastPg
       IN /\ jos' = [jos EXCEPT ![act] = before \o
                       [i \in 1..n |-> Slot("ok", b[i].s, b[i].s, b[i].k, b[i].op, synced, pg)]
                       \o SubSeq(jos[act], wpos + n, Len(jos[act]))]
          /\ wpos' = wpos + n
          /\ queue' = SubSeq(queue, n + 1, Len(queue))
          /\ grp' = grp \o [i \in 1..n |-> [w |-> b[i].w, k |-> b[i].k, op |-> b[i].op,
                                            sync |-> b[i].sync, s |-> b[i].s, sg |-> act]]
          /\ lastW' = b[n].s
          /\ padNext' = FALSE
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jdur, rfile, mfile, cur, curD,
                   status, wst, nextSeq, synced, vis, durPub, act, rollPending, mem,
                   imm, ver, flushed, recSegs, nextFile, manNo, quar, fl, cp,
                   obsolete, rd, rec, hist, ack, lastSub, crashes, media, wipes, pend,
                   prev, quiet>>

\* segment::activate: rename a prepared free file to the next live segment
\* number and write its header (synced). Its directory sync is the next step.
\* A roll happens when the segment is full, or early, when the next group
\* does not fit in what is left (independent read R-lsm-03): the rest of the
\* old segment then reads as an end marker whose sequence is the new
\* segment's first, the roll point recovery must step over.
JournalRoll ==
    /\ status = "open"
    /\ queue /= <<>>
    /\ wpos - 1 >= IF EarlyRoll THEN 1 ELSE SegCap
    /\ ~rollPending
    /\ NextSegNo <= MaxSegs
    /\ LET ns == NextSegNo IN
       /\ segName' = [segName EXCEPT ![ns] = "live"]
       /\ segNameD' = [segNameD EXCEPT ![ns] = "free"]   \* create_free made "free" durable
       /\ segFirst' = [segFirst EXCEPT ![ns] = lastW + 1]
       /\ act' = ns
    /\ wpos' = 1
    /\ rollPending' = TRUE
    /\ ClearOpen
    /\ UNCHANGED <<jos, jdur, rfile, mfile, cur, curD, status, wst, queue, grp,
                   nextSeq, lastW, synced, vis, durPub, padNext, mem, imm, ver,
                   flushed, recSegs, nextFile, manNo, quar, fl, cp, obsolete, rd, rec,
                   hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>

\* The directory sync at the end of segment::activate.
JournalRollSyncDir ==
    /\ status = "open"
    /\ rollPending
    /\ segNameD' = segName
    /\ rollPending' = FALSE
    /\ ClearOpen
    /\ UNCHANGED <<segName, segFirst, jos, jdur, rfile, mfile, cur, curD, status, wst,
                   queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos, mem,
                   imm, ver, flushed, recSegs, nextFile, manNo, quar, fl, cp, obsolete,
                   rd, rec, hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>

\* A waiter asked for Sync, or the flusher waits in Store::sync_journal.
NeedsSync ==
    \/ Mutant /= "sync_only_for_flush" /\ \E i \in DOMAIN grp : grp[i].sync /\ grp[i].s > synced
    \/ Mutant /= "flush_waits_without_sync_request"
       /\ fl.ph = "idle" /\ imm /= <<>> /\ synced < imm[1].last

\* Writer::sync_aligned: pad the write position to the next 4 KiB page, then
\* one fdatasync of every segment with unsynced bytes. The code writes the pad
\* before the sync; the plan's text says "fdatasync, then pad" (mutant
\* pad_after_sync), and "also syncs the previous segment" (mutant
\* sync_prev_segment_only).
SyncedSegs ==
    IF Mutant = "sync_prev_segment_only"
    THEN {s \in LiveSet : s = act \/ NextLive(s) = act}
    ELSE LiveSet

\* The plan-literal pad: a pad record written after the sync, into the page
\* that holds the synced records, so the page is dirty again.
PadSlot == Slot("ok", lastW + 1, lastW, NoKey, "pad", lastW, LastPg(act))

JournalSync ==
    /\ status = "open"
    /\ NeedsSync
    /\ synced < lastW
    /\ LET d1 == [s \in Segs |-> IF s \in SyncedSegs THEN jos[s] ELSE jdur[s]]
       IN /\ jdur' = d1
          /\ jos' = IF Mutant = "pad_after_sync" /\ Len(jos[act]) <= SegCap
                    THEN [jos EXCEPT ![act] = Append(jos[act], PadSlot)]
                    ELSE jos
    /\ wpos' = IF Mutant = "pad_after_sync" /\ Len(jos[act]) <= SegCap THEN wpos + 1 ELSE wpos
    /\ synced' = lastW
    /\ padNext' = (Mutant /= "no_sync_pad")
    \* Then the group is published (Writer: sink.apply, visible_seq, durable_seq,
    \* complete). A crash between the sync and the publication leaves the same
    \* disk as a crash just after it, with fewer acknowledgments, so the two are
    \* one step here.
    /\ LET n == Len(grp)
           v == IF n = 0 THEN vis ELSE grp[n].s
           d == Max2(durPub, Min2(lastW, v))
           told == {grp[i].s : i \in {j \in 1..n : grp[j].sync}}
       IN /\ mem' = mem \cup {Ent(grp[i].k, grp[i].s, grp[i].op, grp[i].sg) : i \in 1..n}
          /\ vis' = v
          /\ durPub' = d
          /\ ack' = Max2(ack, IF told = {} THEN d ELSE Max2(d, Max(told)))
          /\ wst' = [w \in Writers |-> IF \E i \in 1..n : grp[i].w = w THEN "idle" ELSE wst[w]]
          /\ grp' = <<>>
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, rfile, mfile, cur, curD, status,
                   queue, nextSeq, lastW, act, rollPending, imm,
                   ver, flushed, recSegs, nextFile, manNo, quar, fl, cp, obsolete, rd,
                   rec, hist, lastSub, crashes, media, wipes, pend, prev, quiet>>

\* Publish an unsynced head of the group (RD3): insert in sequence order,
\* publish visible_seq, complete the NoSync waiters. A Sync waiter completes
\* only with its sync, in JournalSync.
Publishable(i) == ~grp[i].sync \/ grp[i].s <= synced \/ Mutant = "ack_before_sync"
PubLen == LET L == {n \in 1..Len(grp) : \A i \in 1..n : Publishable(i)}
          IN IF L = {} THEN 0 ELSE Max(L)

JournalPublish ==
    /\ status = "open"
    /\ grp /= <<>>
    /\ PubLen >= 1
    /\ LET n == PubLen
           P == SubSeq(grp, 1, n)
           d == Max2(durPub, Min2(synced, P[n].s))
           \* a Sync waiter is told its write and every earlier one are durable
           told == {P[i].s : i \in {j \in 1..n : P[j].sync}}
       IN /\ mem' = mem \cup {Ent(P[i].k, P[i].s, P[i].op, P[i].sg) : i \in 1..n}
          /\ vis' = P[n].s
          /\ durPub' = d
          /\ ack' = Max2(ack, IF told = {} THEN d ELSE Max2(d, Max(told)))
          /\ wst' = [w \in Writers |-> IF \E i \in 1..n : P[i].w = w THEN "idle" ELSE wst[w]]
          /\ grp' = SubSeq(grp, n + 1, Len(grp))
    /\ ClearOpen
    /\ UNCHANGED <<disk, status, queue, nextSeq, lastW, synced, act, padNext, wpos,
                   rollPending, imm, ver, flushed, recSegs, nextFile, manNo, quar, fl,
                   cp, obsolete, rd, rec, hist, lastSub, crashes, media, wipes, pend,
                   prev, quiet>>

-----------------------------------------------------------------------------
(* Memtables and flush (plan "Flush and compaction"; store/mod.rs).        *)

\* Store::freeze_active (budget used, or replay window over max_replay_bytes).
FreezeMemtable ==
    /\ status = "open"
    /\ mem /= {}
    /\ Len(imm) < MaxImm
    /\ imm' = Append(imm, [e |-> mem, last |-> Max({e.s : e \in mem})])
    /\ mem' = {}
    /\ ClearOpen
    /\ UNCHANGED <<disk, status, wst, queue, grp, nextSeq, lastW, synced, vis, durPub,
                   act, padNext, wpos, rollPending, ver, flushed, recSegs, nextFile, manNo,
                   quar, fl, cp, obsolete, rd, rec, hist, ack, lastSub, crashes, media,
                   wipes, pend, prev, quiet>>

\* A sync of runs/: every pending entry under runs/ becomes durable.
SyncRunsDir(rf) == [i \in FileIds |-> [o |-> rf[i].o, d |-> rf[i].o]]

\* Create runs/<id>.run (written and fsynced), then sync runs/.
WrittenRun(rf0, id, c) ==
    LET rf == [rf0 EXCEPT ![id] = [o |-> c, d |-> rf0[id].d]]
    IN IF Mutant = "no_run_dir_sync" THEN rf ELSE SyncRunsDir(rf)
WriteRun(id, c) == rfile' = WrittenRun(rfile, id, c)

\* Remove runs/<id>.run for each run in P (no directory sync).
RemovedRuns(rf0, P) ==
    [i \in FileIds |-> IF \E d \in P : d.id = i THEN [rf0[i] EXCEPT !.o = NoFile] ELSE rf0[i]]

\* Remove runs/<id>.run for each run in P (no directory sync).
RemoveFiles(P) == rfile' = RemovedRuns(rfile, P)

\* retire_journal_segments: rename each to <n>.free, then sync the directory.
RetireNames(R) ==
    /\ segName' = [s \in Segs |-> IF s \in R THEN "free" ELSE segName[s]]
    /\ segNameD' = [s \in Segs |-> IF s \in R THEN "free" ELSE segName[s]]

\* The segments mutant retire_before_edit retires before the flush's edit.
EarlyR == IF Mutant = "retire_before_edit" THEN Retirable(Max2(imm[1].last, flushed)) ELSE {}

\* flush_one up to the manifest edit: sync_journal, write_outputs, sync_runs_dir.
FlushWriteRun ==
    /\ status = "open"
    /\ fl.ph = "idle"
    /\ imm /= <<>>
    /\ synced >= imm[1].last \/ Mutant = "flush_no_journal_sync"
    /\ LET out == Filter(imm[1].e, Snaps, Mutant = "flush_drops_tombstones")
       IN IF out = {} THEN
             /\ fl' = [ph |-> "written", run |-> NoRun, last |-> imm[1].last, R |-> EarlyR]
             /\ UNCHANGED <<rfile, nextFile>>
          ELSE
             /\ nextFile <= MaxFile
             /\ WriteRun(nextFile, out)
             /\ nextFile' = nextFile + 1
             /\ fl' = [ph |-> "written", run |-> [id |-> nextFile, c |-> out],
                       last |-> imm[1].last, R |-> EarlyR]
    \* mutant retire_before_edit: the segments the edit will drop retire now
    /\ IF Mutant = "retire_before_edit"
       THEN RetireNames(Retirable(Max2(imm[1].last, flushed)))
       ELSE UNCHANGED <<segName, segNameD>>
    /\ ClearOpen
    /\ UNCHANGED <<segFirst, jos, jdur, mfile, cur, curD, status,
                   wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, ver, flushed, recSegs, manNo, quar, cp,
                   obsolete, rd, rec, ghostNJ>>

\* The quarantined scopes every manifest edit records (mutant
\* quarantine_volatile: never written, so a crash forgets them).
PersistQ == IF Mutant = "quarantine_volatile" THEN {} ELSE quar

\* Store::log_edit: append the edit to the manifest and sync it.
LogEdit(ms) == [mfile EXCEPT ![manNo] = [o |-> ms, d |-> IF @.d.nf = 0 THEN NoMan ELSE ms]]

\* flush_one: the manifest edit {add run, flushed_seq, segments}, then
\* install the version. Segments the edit drops retire in the next step.
FlushLogEdit ==
    /\ status = "open"
    /\ fl.ph = "written"
    /\ LET l0 == IF fl.run = NoRun THEN ver.l0 ELSE Append(ver.l0, fl.run)
           F == Max2(fl.last, flushed)
           R == IF Mutant = "retire_before_edit" THEN fl.R ELSE Retirable(F)
           segs == Recordable(R)
       IN /\ mfile' = LogEdit(ManState(F, l0, ver.l1, segs, nextFile, PersistQ))
          /\ ver' = [ver EXCEPT !.l0 = l0]
          /\ imm' = Tail(imm)
          /\ flushed' = F
          /\ recSegs' = segs
          /\ fl' = IF R = {} \/ Mutant = "retire_before_edit" THEN FlIdle
                   ELSE [FlIdle EXCEPT !.ph = "retire", !.R = R]
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, rfile, cur, curD, status,
                   wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, nextFile, manNo, quar, cp, obsolete, rd, rec, ghostNJ>>


RetireJournalSegments ==
    /\ status = "open"
    /\ fl.ph = "retire"
    /\ RetireNames(fl.R)
    /\ fl' = FlIdle
    /\ ClearOpen
    /\ UNCHANGED <<segFirst, jos, jdur, rfile, mfile, cur, curD, status, wst, queue,
                   grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos, rollPending,
                   mem, imm, ver, flushed, recSegs, nextFile, manNo, quar, cp, obsolete,
                   rd, rec, hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>

-----------------------------------------------------------------------------
(* Compaction and deletion of obsolete files (compact_once).               *)

\* run_compaction up to the edit: every L0 run and the overlapping L1 runs,
\* merged through the filter into one L1 run (the bottom), synced.
CompactWriteRun ==
    /\ status = "open"
    /\ cp.ph = "idle"
    /\ ver.l0 /= <<>>
    /\ LET L0 == Range(ver.l0)
           L0k == UNION {KeysOf(d.c) : d \in L0}
           ins == L0 \cup {d \in ver.l1 : KeysOf(d.c) \cap L0k /= {}}
           out == Filter(UNION {d.c : d \in ins}, Snaps, TRUE)
       IN IF out = {} THEN
             /\ cp' = [ph |-> "written", ins |-> ins, out |-> NoRun]
             /\ UNCHANGED <<rfile, nextFile>>
          ELSE
             /\ nextFile <= MaxFile
             \* mutant purge_before_edit: the inputs are deleted before the edit
             /\ rfile' = LET rf == WrittenRun(rfile, nextFile, out)
                         IN IF Mutant = "purge_before_edit" THEN RemovedRuns(rf, ins) ELSE rf
             /\ nextFile' = nextFile + 1
             /\ cp' = [ph |-> "written", ins |-> ins, out |-> [id |-> nextFile, c |-> out]]
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, mfile, cur, curD, status,
                   wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, ver, flushed, recSegs, manNo, quar, fl,
                   obsolete, rd, rec, ghostNJ>>

\* run_compaction: the edit {remove inputs, add output}, install the
\* version, and mark the inputs obsolete.
CompactLogEdit ==
    /\ status = "open"
    /\ cp.ph = "written"
    /\ LET l0 == SelectSeq(ver.l0, LAMBDA d : d \notin cp.ins)
           l1 == (ver.l1 \ cp.ins) \cup (IF cp.out = NoRun THEN {} ELSE {cp.out})
       IN /\ mfile' = LogEdit(ManState(flushed, l0, l1, recSegs, nextFile, PersistQ))
          /\ ver' = [l0 |-> l0, l1 |-> l1]
    \* compact_once then deletes the inputs no snapshot pins (purge_obsolete
    \* after the pick is dropped); pinned inputs wait for PurgeObsolete.
    /\ LET keep == IF Mutant = "purge_ignores_snapshots" THEN {} ELSE RunsOf(rd)
           P == {d \in cp.ins : d \notin keep}
       IN /\ obsolete' = obsolete \cup (cp.ins \ P)
          /\ RemoveFiles(P)
    /\ cp' = CpIdle
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, cur, curD, status,
                   wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, flushed, recSegs, nextFile, manNo, quar, fl,
                   rd, rec, ghostNJ>>

\* A run file is deleted when it is obsolete and no version holds it: the
\* current one, or one a snapshot pins (RunHandle drop; purge_obsolete).
Pinned ==
    RunsOf(ver) \cup cp.ins \cup
    (IF Mutant = "purge_ignores_snapshots" THEN {} ELSE RunsOf(rd))


PurgeObsolete ==
    /\ status = "open"
    /\ Mutant = "purge_only_in_compaction" => cp.ph = "written"
    /\ LET P == {d \in obsolete : d \notin Pinned} IN
       /\ P /= {}
       /\ RemoveFiles(P)
       /\ obsolete' = obsolete \ P
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, mfile, cur, curD, status,
                   wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, ver, flushed, recSegs, nextFile, manNo, quar,
                   fl, cp, rd, rec, ghostNJ>>

-----------------------------------------------------------------------------
(* Snapshots and iterators (plan "Snapshots"). A snapshot pins the         *)
(* visible sequence, the memtables and the version; an iterator reads      *)
(* through it at any later point, so its reads are checked in every state. *)

TakeSnapshot ==
    /\ Reader
    /\ status = "open"
    /\ rd.ph = "idle"
    \* mutant snapshot_skips_immutable: the snapshot pins the active memtable only
    /\ rd' = [ph |-> "snap", seq |-> vis,
              mem |-> {e \in IF Mutant = "snapshot_skips_immutable" THEN mem ELSE mem \cup ImmE :
                         e.s <= vis},
              l0 |-> ver.l0, l1 |-> ver.l1]
    /\ ClearOpen
    /\ UNCHANGED <<disk, status, wst, queue, grp, nextSeq, lastW, synced, vis, durPub,
                   act, padNext, wpos, rollPending, mem, imm, ver, flushed, recSegs,
                   nextFile, manNo, quar, fl, cp, obsolete, rec, hist, ack, lastSub,
                   crashes, media, wipes, pend, prev, quiet>>

ReleaseSnapshot ==
    /\ status = "open"
    /\ rd.ph = "snap"
    /\ rd' = RdIdle
    /\ ClearOpen
    /\ UNCHANGED <<disk, status, wst, queue, grp, nextSeq, lastW, synced, vis, durPub,
                   act, padNext, wpos, rollPending, mem, imm, ver, flushed, recSegs,
                   nextFile, manNo, quar, fl, cp, obsolete, rec, hist, ack, lastSub,
                   crashes, media, wipes, pend, prev, quiet>>

-----------------------------------------------------------------------------
(* Faults. Each has a budget. A crash may hit any step, including a step   *)
(* of recovery (two overlapping faults).                                   *)

CrashProc ==
    /\ ResetProc
    /\ status' = "down"
    /\ ClearOpen

\* Process kill: every completed write survives in the OS layer.
Kill ==
    /\ status /= "refused"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ CrashProc
    /\ UNCHANGED <<disk, hist, ack, lastSub, media, wipes, pend, prev, quiet>>

\* Power loss: the durable layer survives; each unsynced slot and each
\* pending directory entry independently keeps its new or its old state, a
\* slot may tear, and a page mixing synced and unsynced bytes may be lost.
PowerLoss ==
    /\ status /= "refused"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ LET U == {s \in Segs : jos[s] /= jdur[s]}       \* segments with unsynced slots
           PN == {s \in Segs : segName[s] /= segNameD[s]} \* pending segment renames
           PR == {i \in FileIds : rfile[i].o /= rfile[i].d}
           PM == {i \in FileIds : mfile[i].o /= mfile[i].d}
       IN \E img \in Prod(U, [s \in U |-> SegImages(s)]),
             keepN \in SUBSET PN, keepR \in SUBSET PR, keepM \in SUBSET PM,
             c \in {cur, curD} :
          /\ jos' = [s \in Segs |-> IF s \in U THEN img[s] ELSE jos[s]]
          /\ jdur' = jos'
          /\ segName' = [s \in Segs |-> IF s \in PN \ keepN THEN segNameD[s] ELSE segName[s]]
          /\ segNameD' = segName'
          /\ rfile' = [i \in FileIds |->
                         LET f == IF i \in PR \ keepR THEN rfile[i].d ELSE rfile[i].o
                         IN [o |-> f, d |-> f]]
          /\ mfile' = [i \in FileIds |->
                         LET f == IF i \in PM \ keepM THEN mfile[i].d ELSE mfile[i].o
                         IN [o |-> f, d |-> f]]
          /\ cur' = c
          /\ curD' = c
    /\ CrashProc
    /\ UNCHANGED <<segFirst, hist, ack, lastSub, media, wipes, pend, prev, quiet>>

\* Media damage of a synced journal record while the store is down: a bit
\* flip in the payload ("dmg", scope known), in the header ("torn"), or an
\* unreadable region ("eio"). FaultFs flip_bit and Fault::range.
MediaDamage(s, i, kind) ==
    /\ status = "down"
    /\ media < MaxMedia
    /\ segName[s] = "live"
    /\ i \in 1..Len(jos[s])
    /\ Synced(s, i)
    /\ jos[s][i].st = "ok"
    /\ jos[s][i].op \notin {"void", "pad"}
    /\ LET old == jos[s][i]
           new == CASE kind = "dmg" -> [old EXCEPT !.st = "dmg"]
                    [] kind = "torn" -> TornSlot
                    [] kind = "zero" -> ZeroSlot
                    [] kind = "eio" -> [old EXCEPT !.st = "eio"]
       IN /\ jos' = [jos EXCEPT ![s][i] = new]
          /\ jdur' = [jdur EXCEPT ![s][i] = new]
    /\ media' = media + 1
    /\ quiet' = FALSE
    /\ UNCHANGED <<segName, segNameD, segFirst, rfile, mfile, cur, curD, proc, hist,
                   ack, lastSub, crashes, wipes, pend, justOpened, lastOpen, prev>>

MediaDamageAny ==
    \E s \in Segs, i \in 1..(SegCap + 1), kind \in {"dmg", "torn", "zero", "eio"} :
        MediaDamage(s, i, kind)

\* Whole-volume loss (FaultFs Wipe): the store reopens empty. The pool's
\* mount identity, not the engine, detects it (OLTP-G08, O17).
DiskLoss ==
    /\ status /= "refused"
    /\ wipes < MaxDiskLoss
    /\ wipes' = wipes + 1
    /\ segName' = [s \in Segs |-> "none"]
    /\ segNameD' = [s \in Segs |-> "none"]
    /\ segFirst' = [s \in Segs |-> 0]
    /\ jos' = [s \in Segs |-> <<>>]
    /\ jdur' = [s \in Segs |-> <<>>]
    /\ rfile' = [i \in FileIds |-> [o |-> NoFile, d |-> NoFile]]
    /\ mfile' = [i \in FileIds |-> [o |-> NoMan, d |-> NoMan]]
    /\ cur' = 0
    /\ curD' = 0
    /\ CrashProc
    /\ hist' = [s \in Seqs |-> HNone]
    /\ ack' = 0
    /\ lastSub' = 0
    /\ quiet' = FALSE
    /\ prev' = [end |-> 0, view |-> NoView]
    /\ UNCHANGED <<crashes, media, pend>>

-----------------------------------------------------------------------------
(* Opening the store (Store::open, recovery::prepare, Journal::open).      *)

\* Read CURRENT and the manifest, check the referenced runs, replay the
\* journal from flushed_seq, and repair it: zero the tail after the end and
\* sync the zeros (L2), free the segments after the end, void the sequences
\* the manifest holds and the journal lost, sync every live segment and the
\* journal directory. Writing resumes on a fresh 4 KiB page.
\* Mutant open_retries_on_torn_tail: a torn tail is taken for a transient
\* read and the open retried, without progress.
TornTailAtOpen ==
    /\ Mutant = "open_retries_on_torn_tail"
    /\ status = "down"
    /\ cur /= 0
    /\ mfile[cur].o.nf /= 0
    /\ LET r == RecJournal(mfile[cur].o)
       IN r.ok /\ r.s /= 0 /\ SlotAt(r.s, r.i).st \in {"torn", "dmg"}

OpenReplayStep ==
    /\ status = "down"
    /\ ~TornTailAtOpen
    /\ LET fresh == cur = 0
           ms == IF fresh THEN EmptyMan ELSE mfile[cur].o
           r == RecJournal(ms)
       IN IF (~fresh /\ ms.nf = 0)                            \* R3: CURRENT names a missing manifest
             \/ ~(\A d \in RunsOf(ms) : rfile[d.id].o /= NoFile) \* G2: a referenced run is missing
             \/ ~r.ok                                         \* R1, R2, R3 in the journal
             \/ ~(ms.segs \subseteq                            \* R3: a recorded segment is missing
                   {t \in LiveSet : ~(r.s /= 0 /\ t > r.s)})
          THEN
             /\ status' = "refused"
             /\ UNCHANGED <<disk, rec, pend>>
          ELSE
          LET F == ms.fl
              next2 == Max2(r.next, F + 1)
              needVoid == F >= r.next
              freeS == {t \in LiveSet : r.s /= 0 /\ t > r.s /\ Mutant /= "keep_later_segments"}
              names1 == [t \in Segs |-> IF t \in freeS THEN "free" ELSE segName[t]]
              cut == IF r.s = 0 \/ Mutant = "no_zero_tail" THEN jos
                     ELSE [jos EXCEPT ![r.s] = SubSeq(jos[r.s], 1, Min2(r.i - 1, Len(jos[r.s])))]
              vpg == IF r.s = 0 THEN 1 ELSE LastPg(r.s) + 1
              jos1 == IF needVoid /\ r.s /= 0
                      THEN [cut EXCEPT ![r.s] = Append(cut[r.s],
                                Slot("ok", r.next, F, NoKey, "void", r.next - 1, vpg))]
                      ELSE cut
              newSeg == r.s = 0
              ns == NextSegNo
              names2 == IF newSeg THEN [names1 EXCEPT ![ns] = "live"] ELSE names1
              cert == \E t \in LiveSet : \E j \in DOMAIN jos[t] :
                          jos[t][j].st = "ok" /\ jos[t][j].dm >= next2
              eio == \E t \in LiveSet : \E j \in DOMAIN jos[t] : jos[t][j].st = "eio"
          IN /\ ~newSeg \/ ns <= MaxSegs
             /\ jos' = jos1
             /\ jdur' = [t \in Segs |-> IF names2[t] = "live" /\ Mutant /= "no_open_sync"
                                        THEN jos1[t] ELSE jdur[t]]
             /\ segName' = names2
             /\ segNameD' = IF freeS /= {} \/ newSeg \/ Mutant /= "no_open_dir_sync"
                            THEN names2 ELSE segNameD
             /\ segFirst' = IF newSeg THEN [segFirst EXCEPT ![ns] = next2] ELSE segFirst
             /\ rec' = [ms |-> ms, m |-> r.m, next |-> next2, q |-> r.q,
                        act |-> IF newSeg THEN ns ELSE r.s,
                        pg |-> Mutant /= "resume_in_page",
                        wp |-> IF newSeg THEN 1
                               ELSE IF Mutant = "no_zero_tail" THEN Min2(r.i, Len(jos1[r.s]) + 1)
                               ELSE Len(jos1[r.s]) + 1]
             /\ pend' = [ack |-> ack, end |-> next2 - 1, sub |-> lastSub,
                         cert |-> cert, media |-> media > 0, eio |-> eio]
             /\ status' = "recM"
             /\ UNCHANGED <<rfile, mfile, cur, curD>>
    /\ ClearOpen
    /\ UNCHANGED <<wst, queue, grp, nextSeq, lastW, synced, vis, durPub, act, padNext, wpos,
                   rollPending, mem, imm, ver, flushed, recSegs, nextFile, manNo, quar,
                   fl, cp, obsolete, rd, hist, ack, lastSub, crashes, media, wipes,
                   prev, quiet>>

OpenReplayJournal == (TornTailAtOpen /\ UNCHANGED vars) \/ OpenReplayStep

\* ManifestWriter::create (write the snapshot record, sync, sync the
\* directory), then switch_current up to its rename: CURRENT.tmp written
\* and synced, renamed over CURRENT. The directory sync is the next step.
RecoveredMan ==
    ManState(rec.ms.fl, rec.ms.l0, rec.ms.l1,
             {s \in LiveSet : segFirst[s] <= rec.next - 1}, rec.ms.nf + 1,
             IF Mutant = "quarantine_volatile" THEN {} ELSE rec.q)

OpenCreateManifest ==
    /\ status = "recM"
    /\ rec.ms.nf <= MaxFile
    /\ LET n == rec.ms.nf
           mf == [mfile EXCEPT ![n] = [o |-> RecoveredMan, d |-> RecoveredMan]]
       IN mfile' = IF Mutant = "cleanup_before_current_sync"
                   THEN [i \in FileIds |-> IF i /= n THEN [mf[i] EXCEPT !.o = NoMan] ELSE mf[i]]
                   ELSE mf
    /\ cur' = rec.ms.nf
    /\ status' = "recC"
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, rfile, curD, wst, queue, grp,
                   nextSeq, lastW, synced, vis, durPub, act, padNext, wpos, rollPending, mem,
                   imm, ver, flushed, recSegs, nextFile, manNo, quar, fl, cp, obsolete,
                   rd, rec, hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>

\* switch_current: sync the store directory, so the rename is durable.
OpenSyncCurrent ==
    /\ status = "recC"
    /\ curD' = cur
    /\ mfile' = [i \in FileIds |-> [o |-> mfile[i].o, d |-> mfile[i].o]]
    /\ status' = "recF"
    /\ ClearOpen
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, rfile, cur, wst, queue, grp,
                   nextSeq, lastW, synced, vis, durPub, act, padNext, wpos, rollPending, mem,
                   imm, ver, flushed, recSegs, nextFile, manNo, quar, fl, cp, obsolete,
                   rd, rec, hist, ack, lastSub, crashes, media, wipes, pend, prev, quiet>>

\* recovery::cleanup (stale manifests and unreferenced runs, removed without
\* a directory sync), then publish the recovered sequences and serve.
OpenCleanup ==
    /\ status = "recF"
    /\ LET ms == mfile[cur].o
           keep == {d.id : d \in RunsOf(ms)}
           end == rec.next - 1
           v == [k \in Keys |-> ReadSrc(rec.m, ms.l0, ms.l1, k, end)]
       IN /\ mfile' = [i \in FileIds |-> IF i /= cur THEN [mfile[i] EXCEPT !.o = NoMan]
                                         ELSE mfile[i]]
          /\ rfile' = [i \in FileIds |-> IF i \notin keep THEN [rfile[i] EXCEPT !.o = NoFile]
                                         ELSE rfile[i]]
          /\ ver' = [l0 |-> ms.l0, l1 |-> ms.l1]
          /\ flushed' = ms.fl
          /\ recSegs' = ms.segs
          /\ nextFile' = ms.nf
          /\ quar' = rec.q
          /\ lastW' = end
          /\ synced' = end
          /\ vis' = end
          /\ durPub' = end
          /\ hist' = [s \in Seqs |-> IF s <= end THEN hist[s] ELSE HNone]
          /\ ack' = end
          /\ lastSub' = end
          /\ lastOpen' = [ack |-> pend.ack, end |-> pend.end, sub |-> pend.sub,
                          cert |-> pend.cert, media |-> pend.media, eio |-> pend.eio,
                          quiet |-> quiet,
                          prevEnd |-> prev.end, view |-> v, prevView |-> prev.view]
          /\ prev' = [end |-> end, view |-> v]
    /\ status' = "open"
    /\ mem' = rec.m
    /\ manNo' = cur
    /\ nextSeq' = rec.next
    /\ act' = rec.act
    /\ padNext' = rec.pg
    /\ wpos' = rec.wp
    /\ rec' = RecNone
    /\ justOpened' = TRUE
    /\ quiet' = TRUE
    /\ pend' = PendNone
    /\ UNCHANGED <<segName, segNameD, segFirst, jos, jdur, cur, curD, wst, queue, grp,
                   rollPending, imm, fl, cp, obsolete, rd, crashes, media, wipes>>

\* A refused open is final: the pool takes the failed-mount path (O26).
Refused ==
    /\ status = "refused"
    /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
    \/ SubmitAny
    \/ JournalWriteGroup
    \/ JournalRoll
    \/ JournalRollSyncDir
    \/ JournalSync
    \/ JournalPublish
    \/ FreezeMemtable
    \/ FlushWriteRun
    \/ FlushLogEdit
    \/ RetireJournalSegments
    \/ CompactWriteRun
    \/ CompactLogEdit
    \/ PurgeObsolete
    \/ TakeSnapshot
    \/ ReleaseSnapshot
    \/ Kill
    \/ PowerLoss
    \/ MediaDamageAny
    \/ DiskLoss
    \/ OpenReplayJournal
    \/ OpenCreateManifest
    \/ OpenSyncCurrent
    \/ OpenCleanup
    \/ Refused

Spec == Init /\ [][Next]_vars

\* Fairness for the liveness runs, one mechanism per assumption:
\*  - journal steps: the kv-journal thread loop, woken by the `work` condvar
\*    on every submit and on every sync request;
\*  - flush steps: the kv-flush thread, woken by schedule_flush when an
\*    immutable memtable exists (flush_ready);
\*  - compaction edit and purge: the kv-compact thread, which finishes a
\*    started compaction and purges obsolete runs in compact_once; the file
\*    of an obsolete run is also deleted when its last RunHandle drops;
\*  - open steps: the pool mount retries the open after a crash (worker
\*    restart or remount, O26).
\* No fairness on Submit, Freeze, CompactWriteRun, snapshots or faults: those
\* are client choices, budget or size triggers, or faults.
Fairness ==
    /\ WF_vars(JournalWriteGroup) /\ WF_vars(JournalRoll) /\ WF_vars(JournalRollSyncDir)
    /\ WF_vars(JournalSync) /\ WF_vars(JournalPublish)
    /\ WF_vars(FlushWriteRun) /\ WF_vars(FlushLogEdit) /\ WF_vars(RetireJournalSegments)
    /\ WF_vars(CompactLogEdit) /\ SF_vars(PurgeObsolete)
    /\ WF_vars(OpenReplayJournal) /\ WF_vars(OpenCreateManifest)
    /\ WF_vars(OpenSyncCurrent) /\ WF_vars(OpenCleanup)

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties. Each names the register guarantee it checks.                *)

SlotT == [st : {"ok", "torn", "dmg", "eio", "zero"}, lo : 0..MaxSeq, hi : 0..MaxSeq,
          key : Keys \cup {NoKey}, op : Ops \cup {"void", "pad", "none"}, dm : 0..MaxSeq,
          pg : 0..(SegCap + 2)]

TypeOK ==
    /\ segName \in [Segs -> {"none", "free", "live"}]
    /\ segNameD \in [Segs -> {"none", "free", "live"}]
    /\ segFirst \in [Segs -> 0..(MaxSeq + 1)]
    /\ \A s \in Segs : /\ Len(jos[s]) <= SegCap + 1 /\ Range(jos[s]) \subseteq SlotT
                       /\ Len(jdur[s]) <= SegCap + 1 /\ Range(jdur[s]) \subseteq SlotT
    /\ cur \in 0..MaxFile /\ curD \in 0..MaxFile
    /\ status \in {"open", "down", "recM", "recC", "recF", "refused"}
    /\ wst \in [Writers -> {"idle", "busy"}]
    /\ nextSeq \in 0..(MaxSeq + 1)
    /\ {lastW, synced, vis, durPub, flushed, ack, lastSub} \subseteq 0..MaxSeq
    /\ Len(imm) <= MaxImm
    /\ nextFile \in 0..(MaxFile + 1)
    /\ wpos \in 0..(SegCap + 2)
    /\ quar \subseteq Keys
    /\ crashes <= MaxCrashes /\ media <= MaxMedia /\ wipes <= MaxDiskLoss

\* LSM-G01: every write acknowledged as synced, and every write before it,
\* survives. The one accepted exception is the plan's risk R6 and question
\* U1: silent media damage (no read error) of the last synced group that no
\* later record certifies. An unreadable region is never that exception.
G01_AckedSurvive ==
    justOpened =>
        \/ lastOpen.end >= lastOpen.ack
        \/ lastOpen.media /\ ~lastOpen.cert /\ ~lastOpen.eio

\* LSM-G02: the recovered state is the oracle at an exact prefix p with
\* last synced ack <= p <= last submission, and the open store serves the
\* oracle at its visible sequence afterwards.
\* A read that meets a damaged journal value returns Corruption and
\* quarantines the key's scope (plan case G3); G03_NoFalseDamage checks that
\* this only follows media damage.
G02_ExactPrefix ==
    /\ justOpened => lastOpen.end <= lastOpen.sub
    /\ status = "open" =>
         \A k \in Keys \ quar : View(k) \in {StateAt(hist, vis, k), Corrupt}

\* LSM-G02: reopening again preserves the prefix (recovery is idempotent).
\* An open with no submission, media damage or wipe since the previous open
\* recovers the same end and the same state.
G02_RecoveryIdempotent ==
    (justOpened /\ lastOpen.quiet) =>
        /\ lastOpen.end = lastOpen.prevEnd
        /\ lastOpen.view = lastOpen.prevView

\* LSM-G03: loss of a record that a later record certifies durable never
\* passes as a torn tail: it quarantines the scope or refuses the pool.
G03_CertifiedLossDetected ==
    justOpened => (lastOpen.end >= lastOpen.ack \/ ~lastOpen.cert)

\* LSM-G01, LSM-G03: without media damage the store never refuses and never
\* quarantines (plan risk R8: false damage would start replacement storms).
G03_NoFalseDamage ==
    \/ media > 0
    \/ /\ status /= "refused"
       /\ quar = {}
       /\ status = "open" => \A k \in Keys : View(k) /= Corrupt
       /\ rd.ph = "snap" => \A k \in Keys : SnapView(k) /= Corrupt

\* A deleted key is never resurrected, in the current state or a snapshot.
NoResurrection ==
    /\ status = "open" =>
         \A k \in Keys \ quar : StateAt(hist, vis, k) = 0 => View(k) \in {0, Corrupt}
    /\ rd.ph = "snap" =>
         \A k \in Keys \ quar : StateAt(hist, rd.seq, k) = 0 => SnapView(k) \in {0, Corrupt}

\* A deleted file is never read: every run a live version can read exists
\* with its contents, and no reachable journal value points into a retired
\* segment.
NoDeletedFileRead ==
    /\ status = "open" =>
         /\ \A d \in RunsOf(ver) : rfile[d.id].o = d.c
         /\ \A k \in Keys : View(k) /= Gone
    /\ rd.ph = "snap" =>
         /\ \A d \in RunsOf(rd) : rfile[d.id].o = d.c
         /\ \A k \in Keys : SnapView(k) /= Gone

\* NS03: a snapshot's reads stay identical through flush, compaction,
\* deletion and journal retirement.
NS03_SnapshotStable ==
    rd.ph = "snap" =>
        \A k \in Keys \ quar : SnapView(k) \in {StateAt(hist, rd.seq, k), Corrupt}

-----------------------------------------------------------------------------
(* Liveness, checked under Fairness in Lsm.live.cfg (no symmetry). The     *)
(* fault budgets are finite, so every behavior has a last fault; the       *)
(* properties hold after it.                                               *)

\* LSM-G01: a waiting writer is answered: acknowledged, or failed by a crash.
L_CommitCompletes == \A w \in Writers : wst[w] = "busy" ~> wst[w] = "idle"

\* LSM-G01, LSM-G02: an open after a crash finishes: the store serves or refuses.
L_OpenTerminates ==
    status \in {"down", "recM", "recC", "recF"} ~> status \in {"open", "refused"}

\* LSM-G01: an immutable memtable is flushed (the flusher's journal sync request
\* is served).
L_FlushCompletes == imm /= <<>> ~> imm = <<>>

\* An obsolete run file is deleted, unless a snapshot keeps it.
L_ObsoletePurged == obsolete /= {} ~> (obsolete = {} \/ rd.ph = "snap")

=============================================================================
