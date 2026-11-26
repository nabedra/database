----------------------------- MODULE Quarantine -----------------------------
(***************************************************************************)
(* Design spec of damage detection, quarantine, release and backup in      *)
(* nabedra-kv and its OLTP pool (module lsm, level L3). Plan:              *)
(* plans/native-storage-engine.md, section 3, "Our rule" (G1, G2, G3, R6)  *)
(* and "Raft safety of a quarantined group" (rules 1 to 5). Code:          *)
(* journal/recovery.rs (G1), store/mod.rs (open, report_damage,            *)
(* persist_pending, finish_release), quarantine.rs,                        *)
(* oltp-storage/src/oltp/pool/damage.rs, manager/lifecycle.rs              *)
(* (settle_damage). Register: docs/testing/guarantees/lsm.md, LSM-G03,     *)
(* scenarios NS07, NS20, NS23.                                             *)
(*                                                                         *)
(* One pool (one store) hosting two groups, each its own scope: the two    *)
(* ranges of the L3 bound. Actors: each group's Raft node (a writer), the  *)
(* journal thread, the flusher, a reader that finds damage, a backup       *)
(* reader, the pool's release path and Control's replacement.              *)
(*                                                                         *)
(* Abstractions: every write is a synced journal record (a Raft append),   *)
(* so a crash loses nothing synced (torn tails are Lsm.tla's subject); a   *)
(* group's state is one key whose value names the write that set it;      *)
(* journal damage at open always has evidence (a later record exists), as  *)
(* G1 needs; damage of a stored version stands for a damaged run block     *)
(* (G2) or journal value (G3), found when read. The open is one step.      *)
(***************************************************************************)
EXTENDS Naturals, Integers, Sequences, FiniteSets

CONSTANTS
    Groups,       \* groups hosted on the pool; each is its own scope
    MaxSeq,       \* sequence numbers 1..MaxSeq
    MaxCrashes,   \* process crashes (kill or power loss; synced data survives)
    MaxMedia,     \* media damage events (a journal record or a stored version)
    MaxEditFail,  \* failed manifest edits (ENOSPC or EIO on the edit's sync)
    R6Max,        \* R6: most groups one open may newly quarantine (the plan's 8, scaled)
    Mutant        \* "none" in the baselines; a mutant name in mutants/*.cfg

Seqs == 1..MaxSeq
Corrupt == -1
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Range(f) == {f[i] : i \in DOMAIN f}

\* A journal record or an LSM entry: sequence, group, op ("put", "del" for
\* the release's range delete), bad: its bytes are damaged.
E(s, g, op) == [s |-> s, g |-> g, op |-> op, bad |-> FALSE]
NoE == [s |-> 0, g |-> CHOOSE g \in Groups : TRUE, op |-> "none", bad |-> FALSE]
NoBackup == [g |-> CHOOSE g \in Groups : TRUE, got |-> 0, want |-> 0, ok |-> FALSE]

VARIABLES
    \* ---- durable: the store ----
    jrn,        \* journal records, in sequence order (all synced)
    runs,       \* entries of the referenced runs
    fl,         \* flushed_seq
    qdur,       \* quarantined scopes in the manifest
    rel,        \* group -> sequence of its recorded release (0: none)
    \* ---- durable elsewhere ----
    removed,    \* group -> Control committed the change removing this replica
    hosted,     \* group -> the pool still holds the group (its identity record)
    \* ---- volatile ----
    status,     \* "open" | "down" | "refused"
    mem,        \* memtable entries
    qmem,       \* Store::quarantine (in memory)
    pend,       \* pending_damage: run-time quarantines whose edit failed
    running,    \* group -> its Raft node runs (votes, acknowledges)
    relPh,      \* group -> "none" | "deleted" | "flushed": the release in progress
    relSeq,     \* group -> sequence of the release's range delete
    bk,         \* the last completed backup read: [g, got, want, ok]
    \* ---- ghost ----
    oracle,     \* group -> the value the last acknowledged write set (0: none)
    everQ,      \* group -> it was quarantined (in memory) at some point
    lostQ,      \* group -> a crash forgot its run-time quarantine before the edit
    newQ,       \* groups the last open newly quarantined
    crashes, media, editFails

vars == <<jrn, runs, fl, qdur, rel, removed, hosted, status, mem, qmem, pend, running,
          relPh, relSeq, bk, oracle, everQ, lostQ, newQ, crashes, media, editFails>>

NextSeq == IF jrn = <<>> THEN 1 ELSE jrn[Len(jrn)].s + 1

-----------------------------------------------------------------------------
(* Reads. A damaged newest version reads as Corrupt (CRC), never as a      *)
(* wrong value.                                                            *)

Newest(S, g) ==
    LET C == {e \in S : e.g = g}
    IN IF C = {} THEN NoE ELSE CHOOSE e \in C : \A f \in C : f.s <= e.s

ValueOf(e) == IF e.bad THEN Corrupt ELSE IF e.op = "put" THEN e.s ELSE 0
View(g) == ValueOf(Newest(mem \cup runs, g))

-----------------------------------------------------------------------------

Init ==
    /\ jrn = <<>>
    /\ runs = {}
    /\ fl = 0
    /\ qdur = {}
    /\ rel = [g \in Groups |-> 0]
    /\ removed = [g \in Groups |-> FALSE]
    /\ hosted = [g \in Groups |-> TRUE]
    /\ status = "open"
    /\ mem = {}
    /\ qmem = {}
    /\ pend = {}
    /\ running = [g \in Groups |-> TRUE]
    /\ relPh = [g \in Groups |-> "none"]
    /\ relSeq = [g \in Groups |-> 0]
    /\ bk = NoBackup
    /\ oracle = [g \in Groups |-> 0]
    /\ everQ = [g \in Groups |-> FALSE]
    /\ lostQ = [g \in Groups |-> FALSE]
    /\ newQ = {}
    /\ crashes = 0
    /\ media = 0
    /\ editFails = 0

-----------------------------------------------------------------------------
(* A group's Raft node appends (GroupStore::mutate, synced). Writes of a   *)
(* quarantined scope are refused (damage.rs, rule 1).                      *)

Write(g) ==
    /\ status = "open"
    /\ running[g]
    /\ g \notin qmem
    /\ NextSeq <= MaxSeq
    /\ jrn' = Append(jrn, E(NextSeq, g, "put"))
    /\ mem' = mem \cup {E(NextSeq, g, "put")}
    /\ oracle' = [oracle EXCEPT ![g] = NextSeq]
    /\ UNCHANGED <<runs, fl, qdur, rel, removed, hosted, status, qmem, pend, running, relPh,
                   relSeq, bk, everQ, lostQ, newQ, crashes, media, editFails>>

\* flush_one (the journal is synced; the run and the edit are atomic here).
Flush ==
    /\ status = "open"
    /\ mem /= {}
    /\ runs' = runs \cup mem
    /\ fl' = jrn[Len(jrn)].s
    /\ mem' = {}
    /\ relPh' = [g \in Groups |-> IF relPh[g] = "deleted" THEN "flushed" ELSE relPh[g]]
    /\ UNCHANGED <<jrn, qdur, rel, removed, hosted, status, qmem, pend, running, relSeq, bk,
                   oracle, everQ, lostQ, newQ, crashes, media, editFails>>

-----------------------------------------------------------------------------
(* Damage found at run time (G2 block, G3 journal value): report_damage    *)
(* quarantines the scope in memory, stops the group's node (the damage     *)
(* watch, rule 1) and writes a manifest edit. If the edit fails it waits   *)
(* in pending_damage for compact_once's persist_pending.                   *)

Found(g) ==
    /\ status = "open"
    /\ g \notin qmem
    /\ Newest(mem \cup runs, g).bad
    /\ qmem' = qmem \cup {g}
    /\ running' = IF Mutant = "detect_no_stop" THEN running ELSE [running EXCEPT ![g] = FALSE]
    /\ everQ' = [everQ EXCEPT ![g] = TRUE]

ReadFindsDamage(g) ==
    /\ Found(g)
    /\ qdur' = qdur \cup {g}
    /\ UNCHANGED <<jrn, runs, fl, rel, removed, hosted, status, mem, pend, relPh, relSeq, bk,
                   oracle, lostQ, newQ, crashes, media, editFails>>

ReadFindsDamageEditFails(g) ==
    /\ editFails < MaxEditFail
    /\ Found(g)
    /\ pend' = pend \cup {g}
    /\ editFails' = editFails + 1
    /\ UNCHANGED <<jrn, runs, fl, qdur, rel, removed, hosted, status, mem, relPh, relSeq, bk,
                   oracle, lostQ, newQ, crashes, media>>

PersistPending ==
    /\ status = "open"
    /\ pend /= {}
    /\ qdur' = qdur \cup pend
    /\ pend' = {}
    /\ UNCHANGED <<jrn, runs, fl, rel, removed, hosted, status, mem, qmem, running, relPh,
                   relSeq, bk, oracle, everQ, lostQ, newQ, crashes, media, editFails>>

-----------------------------------------------------------------------------
(* Backup (plan section 1, backups are logical cuts read through          *)
(* GroupStore::read_archive): a group's state is read from a snapshot. A  *)
(* quarantined scope fails the read (Quarantine::check and ::overlaps); a *)
(* damaged version fails its CRC, which reports the damage (G2, G3).      *)

BackupRead(g) ==
    /\ status = "open"
    /\ hosted[g]
    /\ LET e == Newest(mem \cup runs, g)
           older == Newest({f \in mem \cup runs : f.g = g /\ f.s < e.s /\ ~f.bad}, g)
       IN IF g \in qmem THEN
             /\ bk' = [NoBackup EXCEPT !.g = g]
             /\ UNCHANGED <<qmem, qdur, running, everQ>>
          ELSE IF e.bad /\ Mutant = "backup_skips_damaged" THEN
             \* the export skips the unreadable version and goes on
             /\ bk' = [g |-> g, got |-> ValueOf(older), want |-> oracle[g], ok |-> TRUE]
             /\ UNCHANGED <<qmem, qdur, running, everQ>>
          ELSE IF e.bad THEN
             /\ bk' = [NoBackup EXCEPT !.g = g]
             /\ qmem' = qmem \cup {g}
             /\ qdur' = qdur \cup {g}
             /\ running' = [running EXCEPT ![g] = FALSE]
             /\ everQ' = [everQ EXCEPT ![g] = TRUE]
          ELSE
             /\ bk' = [g |-> g, got |-> ValueOf(e), want |-> oracle[g], ok |-> TRUE]
             /\ UNCHANGED <<qmem, qdur, running, everQ>>
    /\ UNCHANGED <<jrn, runs, fl, rel, removed, hosted, status, mem, pend, relPh, relSeq,
                   oracle, lostQ, newQ, crashes, media, editFails>>

-----------------------------------------------------------------------------
(* Control replaces the damaged replica with a fresh identity on another   *)
(* pool (group_damage.rs forbids this pool), after the worker reports      *)
(* GroupDamaged, retried every second until settled (settle_damage).       *)
(* Then the pool releases the scope: the range delete, a flush past it,    *)
(* and the release edit (finish_release), and drops the group.             *)

ControlReplace(g) ==
    /\ status = "open"
    /\ Mutant /= "no_damage_report"
    /\ g \in qmem
    /\ ~removed[g]
    /\ removed' = [removed EXCEPT ![g] = TRUE]
    /\ UNCHANGED <<jrn, runs, fl, qdur, rel, hosted, status, mem, qmem, pend, running, relPh,
                   relSeq, bk, oracle, everQ, lostQ, newQ, crashes, media, editFails>>

ReleaseDelete(g) ==
    /\ status = "open"
    /\ removed[g]
    /\ g \in qmem
    /\ relPh[g] = "none"
    \* no sequence bound: MaxSeq bounds client writes; a release writes at
    \* most once per group and open
    /\ jrn' = Append(jrn, E(NextSeq, g, "del"))
    /\ mem' = mem \cup {E(NextSeq, g, "del")}
    /\ relPh' = [relPh EXCEPT ![g] = "deleted"]
    /\ relSeq' = [relSeq EXCEPT ![g] = NextSeq]
    \* the group's data is dropped by design
    /\ oracle' = [oracle EXCEPT ![g] = 0]
    /\ UNCHANGED <<runs, fl, qdur, rel, removed, hosted, status, qmem, pend, running, bk,
                   everQ, lostQ, newQ, crashes, media, editFails>>

\* finish_release: flushed_seq must pass the range delete before the edit,
\* so a later replay never meets the scope's damaged records again.
ReleaseEdit(g) ==
    /\ status = "open"
    /\ relPh[g] = "flushed" \/ (Mutant = "release_without_flush" /\ relPh[g] = "deleted")
    /\ rel' = [rel EXCEPT ![g] = relSeq[g]]
    /\ qdur' = qdur \ {g}
    /\ qmem' = qmem \ {g}
    /\ hosted' = [hosted EXCEPT ![g] = FALSE]
    /\ relPh' = [relPh EXCEPT ![g] = "none"]
    /\ UNCHANGED <<jrn, runs, fl, removed, status, mem, pend, running, relSeq, bk, oracle,
                   everQ, lostQ, newQ, crashes, media, editFails>>

-----------------------------------------------------------------------------
(* Faults.                                                                 *)

Crash ==
    /\ status = "open"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ status' = "down"
    /\ mem' = {}
    /\ qmem' = {}
    /\ pend' = {}
    /\ running' = [g \in Groups |-> FALSE]
    /\ relPh' = [g \in Groups |-> "none"]
    /\ lostQ' = [g \in Groups |-> lostQ[g] \/ g \in pend]
    /\ UNCHANGED <<jrn, runs, fl, qdur, rel, removed, hosted, relSeq, bk, oracle, everQ,
                   newQ, media, editFails>>

\* Media damage of a synced journal record above flushed_seq while the store
\* is down, with a later record as evidence (NS07; G1 at the next open).
DamageRecord(i) ==
    /\ status = "down"
    /\ media < MaxMedia
    /\ i < Len(jrn)
    /\ jrn[i].s > fl
    /\ ~jrn[i].bad
    /\ jrn' = [jrn EXCEPT ![i].bad = TRUE]
    /\ media' = media + 1
    /\ UNCHANGED <<runs, fl, qdur, rel, removed, hosted, status, mem, qmem, pend, running,
                   relPh, relSeq, bk, oracle, everQ, lostQ, newQ, crashes, editFails>>

\* Media damage of a stored version at any time, found only when read: a
\* run block (G2), or the journal value a memtable locator points to (G3),
\* whose bytes are the journal record's, so replay finds it too (G1). A
\* damaged last record with no later one is the U1 residual, modelled in
\* Lsm.tla, so a damaged journal value here has a later record.
DamageStored(e) ==
    /\ media < MaxMedia
    /\ ~e.bad
    /\ e \in runs \/ (e \in mem /\ \E i \in 1..(Len(jrn) - 1) : jrn[i].s = e.s)
    /\ runs' = IF e \in runs THEN (runs \ {e}) \cup {[e EXCEPT !.bad = TRUE]} ELSE runs
    /\ mem' = IF e \in mem THEN (mem \ {e}) \cup {[e EXCEPT !.bad = TRUE]} ELSE mem
    /\ jrn' = IF e \in mem
              THEN [i \in DOMAIN jrn |-> IF jrn[i].s = e.s THEN [jrn[i] EXCEPT !.bad = TRUE]
                                         ELSE jrn[i]]
              ELSE jrn
    /\ media' = media + 1
    /\ UNCHANGED <<fl, qdur, rel, removed, hosted, status, qmem, pend, running, relPh,
                   relSeq, bk, oracle, everQ, lostQ, newQ, crashes, editFails>>

DamageRecordAny == \E i \in 1..Len(jrn) : DamageRecord(i)
DamageStoredAny == \E e \in runs \cup mem : DamageStored(e)

-----------------------------------------------------------------------------
(* Open (Store::open, then the pool's admit and node start). Replay walks  *)
(* the records above flushed_seq: a damaged record of a group scope is G1, *)
(* and replay skips it and every later record of that scope. Damage older  *)
(* than the scope's recorded release is history. The new quarantines go    *)
(* into the fresh manifest before anything is served. R6 refuses an open   *)
(* that newly quarantines more than R6Max groups.                          *)

RECURSIVE Scan(_, _, _, _)
Scan(i, m, q, skip) ==
    IF i > Len(jrn) THEN [m |-> m, q |-> q]
    ELSE LET r == jrn[i] IN
         IF r.s <= fl THEN Scan(i + 1, m, q, skip)
         ELSE IF r.g \in skip \/ (Mutant = "g1_skip_all_scopes" /\ skip /= {})
         THEN Scan(i + 1, m, q, skip)
         ELSE IF r.bad
         THEN Scan(i + 1, m, IF rel[r.g] > r.s THEN q ELSE q \cup {r.g}, skip \cup {r.g})
         ELSE Scan(i + 1, m \cup {[r EXCEPT !.bad = FALSE]}, q, skip)

Open ==
    /\ status = "down"
    /\ LET sc == Scan(1, {}, {}, {})
           new == {g \in sc.q : g \notin qdur /\ hosted[g]}
           q == qdur \cup new
       IN IF Cardinality(new) > R6Max /\ Mutant /= "no_r6"
          THEN /\ status' = "refused"
               /\ UNCHANGED <<mem, qmem, qdur, running, everQ, newQ>>
          ELSE /\ status' = "open"
               /\ mem' = sc.m
               /\ qmem' = q
               /\ qdur' = IF Mutant = "g1_unpersisted" THEN qdur ELSE q
               /\ running' = [g \in Groups |-> hosted[g] /\ g \notin q]
               /\ everQ' = [g \in Groups |-> everQ[g] \/ g \in q]
               /\ newQ' = new
    /\ UNCHANGED <<jrn, runs, fl, rel, removed, hosted, pend, relPh, relSeq, bk, oracle,
                   lostQ, crashes, media, editFails>>

\* A refused pool takes the failed-mount path (O26); Control replaces its
\* replicas. Final here.
Refused ==
    /\ status = "refused"
    /\ UNCHANGED vars

\* The run can finish: every sequence is used, or the pool hosts no group.
\* An explicit terminal step, so deadlock checking stays on.
Finished ==
    /\ status = "open"
    /\ NextSeq > MaxSeq \/ \A g \in Groups : ~hosted[g]
    /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
    \/ \E g \in Groups : Write(g)
    \/ Flush
    \/ \E g \in Groups : ReadFindsDamage(g)
    \/ \E g \in Groups : ReadFindsDamageEditFails(g)
    \/ PersistPending
    \/ \E g \in Groups : BackupRead(g)
    \/ \E g \in Groups : ControlReplace(g)
    \/ \E g \in Groups : ReleaseDelete(g)
    \/ \E g \in Groups : ReleaseEdit(g)
    \/ Crash
    \/ DamageRecordAny
    \/ DamageStoredAny
    \/ Open
    \/ Refused
    \/ Finished

Spec == Init /\ [][Next]_vars

\* Fairness for the liveness baseline, one mechanism per assumption:
\*  - ControlReplace: settle_damage reports GroupDamaged every second until
\*    Control records it, and Control's damage scheduler begins and finishes
\*    one replacement at a time (MAX_ACTIVE_REPLACEMENTS);
\*  - ReleaseDelete, ReleaseEdit, Flush: settle_damage calls
\*    release_damaged_group once the removal committed, which runs
\*    release_scope and finish_release's flush (flush_imms);
\*  - PersistPending: compact_once on a compaction thread
\*    (schedule_compaction after a failed edit);
\*  - Open: the pool mount retries after a crash.
\* No fairness on writes, reads, backups or faults.
Fairness ==
    /\ \A g \in Groups : WF_vars(ControlReplace(g)) /\ WF_vars(ReleaseDelete(g))
                        /\ WF_vars(ReleaseEdit(g))
    /\ WF_vars(Flush) /\ WF_vars(PersistPending) /\ WF_vars(Open)

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ Len(jrn) <= MaxSeq + (MaxCrashes + 1) * Cardinality(Groups)
    /\ fl \in 0..(MaxSeq + (MaxCrashes + 1) * Cardinality(Groups))
    /\ qdur \subseteq Groups /\ qmem \subseteq Groups /\ pend \subseteq Groups
    /\ status \in {"open", "down", "refused"}
    /\ running \in [Groups -> BOOLEAN]
    /\ relPh \in [Groups -> {"none", "deleted", "flushed"}]
    /\ crashes <= MaxCrashes /\ media <= MaxMedia /\ editFails <= MaxEditFail

\* LSM-G03, plan rule 1 (and Q5): a quarantined replica never votes or
\* acknowledges again under its old identity. The one exception is a run-time
\* quarantine whose manifest edit failed and that a crash then forgot
\* (lostQ), until the damage is read again: finding LSM-5.
G03_QuarantineSticky ==
    \A g \in Groups : running[g] => (~everQ[g] \/ lostQ[g])

\* LSM-G01, LSM-G03: an open store serves every unquarantined group the value
\* of its last acknowledged write, or a Corrupt error, never a wrong value or
\* a silently older one.
G03_ServeOracle ==
    status = "open" =>
        \A g \in Groups : (hosted[g] /\ g \notin qmem) =>
            View(g) \in {oracle[g], Corrupt}

\* Plan rule 4: once a release is recorded, the group's damaged data never
\* comes back unquarantined (independent read R-lsm-19).
G03_ReleaseClean ==
    status = "open" =>
        \A g \in Groups : (rel[g] > 0 /\ g \notin qmem) => View(g) \in {0, Corrupt}

\* NS20, O19: a backup never exports a partial or stale state: a completed
\* backup read of a group returns its last acknowledged value.
NS20_BackupExact == bk.ok => bk.got = bk.want

\* R6 (plan section 3): an open that serves quarantined at most R6Max new
\* groups.
R6_Bounded == status = "open" => Cardinality(newQ) <= R6Max

\* Liveness: a quarantined group is replaced and released (rule 4).
L_QuarantineReleased ==
    \A g \in Groups : (g \in qmem /\ status = "open") ~> (g \notin qmem \/ status /= "open")

=============================================================================
