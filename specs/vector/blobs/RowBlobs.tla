---------------------------- MODULE RowBlobs ----------------------------
(***************************************************************************)
(* Vector values next to their rows (plans/oltp-vector.md, section 2).     *)
(*                                                                         *)
(* One key of one range. A row version names the vector version it uses,   *)
(* so a metadata update does not rewrite the vector. MVCC collection runs  *)
(* in two steps, find and remove. A copy to another group (split child,    *)
(* replica snapshot, backup or restore) reads rows and vectors in          *)
(* different steps while collection runs. The pending embedding record     *)
(* travels with the row.                                                   *)
(*                                                                         *)
(* Property: VEC-G10 (NoDanglingBlob, CopyComplete, CopyKeepsPending).     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS MaxWrites, MaxCopies, MaxCrashes, None, Mutant

VARIABLES
    ts,         \* the last commit timestamp
    rows,       \* row versions [ts, del, ref]; ref is a vector version or 0
    blobs,      \* vector versions present
    pend,       \* a pending embedding record exists for the newest version
    upd,        \* a metadata update that read the newest version [ts, ref], or None
    safe,       \* the collection safe point
    cand,       \* vector versions that a collection scan found unreferenced
    copy,       \* [st, pin, rows, blobs, pend]; st: "idle", "rows", "blobs", "active"
    copies, crashes

vars == <<ts, rows, blobs, pend, upd, safe, cand, copy, copies, crashes>>

Newest(R) == CHOOSE r \in R : \A x \in R : x.ts <= r.ts
Idle == [st |-> "idle", pin |-> 0, rows |-> {}, blobs |-> {}, pend |-> FALSE, want |-> FALSE]
Copying == copy.st \in {"rows", "blobs"}

TypeOK ==
    /\ ts \in 0..MaxWrites /\ safe \in 0..ts
    /\ blobs \subseteq 1..MaxWrites
    /\ \A r \in rows : r.ts \in 1..MaxWrites /\ r.ref \in 0..MaxWrites
    /\ copies \in 0..MaxCopies /\ crashes \in 0..MaxCrashes

Init ==
    /\ ts = 0 /\ rows = {} /\ blobs = {} /\ pend = FALSE /\ upd = None /\ safe = 0 /\ cand = {}
    /\ copy = Idle /\ copies = 0 /\ crashes = 0

\* Writes stop while a split copies the range. Replica snapshots and backups copy under a pin, which
\* is the same for collection.
CanWrite == ts < MaxWrites /\ ~Copying

PutVector ==
    /\ CanWrite /\ ts' = ts + 1
    /\ rows' = rows \cup {[ts |-> ts + 1, del |-> FALSE, ref |-> ts + 1]}
    /\ blobs' = blobs \cup {ts + 1} /\ pend' = FALSE
    /\ UNCHANGED <<upd, safe, cand, copy, copies, crashes>>

\* A row with text only: its vector is pending.
PutText ==
    /\ CanWrite /\ ts' = ts + 1
    /\ rows' = rows \cup {[ts |-> ts + 1, del |-> FALSE, ref |-> 0]}
    /\ pend' = TRUE
    /\ UNCHANGED <<blobs, upd, safe, cand, copy, copies, crashes>>

Delete ==
    /\ CanWrite /\ rows # {} /\ ~Newest(rows).del /\ ts' = ts + 1
    /\ rows' = rows \cup {[ts |-> ts + 1, del |-> TRUE, ref |-> 0]}
    /\ pend' = FALSE
    /\ UNCHANGED <<blobs, upd, safe, cand, copy, copies, crashes>>

ReadForUpdate ==
    /\ upd = None /\ rows # {} /\ ~Newest(rows).del
    /\ upd' = [ts |-> Newest(rows).ts, ref |-> Newest(rows).ref]
    /\ UNCHANGED <<ts, rows, blobs, pend, safe, cand, copy, copies, crashes>>

\* The commit carries the version it read. Mutant no_expected_version applies it anyway.
ApplyUpdate ==
    /\ upd # None /\ upd' = None
    /\ IF CanWrite /\ rows # {} /\ (Newest(rows).ts = upd.ts \/ Mutant = "no_expected_version")
       THEN /\ ts' = ts + 1
            /\ rows' = rows \cup {[ts |-> ts + 1, del |-> FALSE, ref |-> upd.ref]}
       ELSE UNCHANGED <<ts, rows>>
    /\ UNCHANGED <<blobs, pend, safe, cand, copy, copies, crashes>>

-----------------------------------------------------------------------------
(* Collection.                                                             *)

\* A copy pins the safe point. Mutant copy_unpinned does not.
AdvanceSafe ==
    /\ safe < ts /\ (~Copying \/ Mutant = "copy_unpinned")
    /\ safe' = safe + 1
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, cand, copy, copies, crashes>>

\* The newest version at or below the safe point stays, unless it is a delete.
CollectRow ==
    /\ \E r \in rows :
        /\ r.ts <= safe
        /\ \E x \in rows : x.ts > r.ts /\ x.ts <= safe
        /\ rows' = rows \ {r}
    /\ UNCHANGED <<ts, blobs, pend, upd, safe, cand, copy, copies, crashes>>

\* Mutant gc_ignores_reference takes any vector older than the safe point and the newest row.
Referenced(b) == \E r \in rows : r.ref = b
FindBlob(b) ==
    /\ b \in blobs /\ b \notin cand
    /\ IF Mutant = "gc_ignores_reference" THEN b < safe /\ b # Newest(rows).ts ELSE ~Referenced(b)
    /\ cand' = cand \cup {b}
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, safe, copy, copies, crashes>>

\* Found earlier, removed now, with no second look: an unreferenced vector can gain no reference.
RemoveBlob(b) ==
    /\ b \in cand
    /\ cand' = cand \ {b} /\ blobs' = blobs \ {b}
    /\ UNCHANGED <<ts, rows, pend, upd, safe, copy, copies, crashes>>

-----------------------------------------------------------------------------
(* Copies.                                                                 *)

StartCopy ==
    /\ copy.st = "idle" /\ copies < MaxCopies /\ copies' = copies + 1
    /\ copy' = [Idle EXCEPT !.st = "rows", !.want = pend, !.pin = safe]
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, safe, cand, crashes>>

\* The copy takes the versions that a reader at or above its pin can need. Mutant
\* prefix_copy_takes_collectable takes every version that is still there, also one that was already
\* collectable when the pin was taken. Mutant prefix_copy_without_pending knows rows and vectors only.
Needed(r) == ~\E x \in rows : x.ts > r.ts /\ x.ts <= copy.pin
CopyRows ==
    /\ copy.st = "rows"
    /\ copy' = [copy EXCEPT !.st = "blobs",
                            !.rows = {r \in rows : Needed(r) \/ Mutant = "prefix_copy_takes_collectable"},
                            !.pend = pend /\ Mutant # "prefix_copy_without_pending"]
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, safe, cand, copies, crashes>>

\* Mutant split_drops_blobs copies no vectors.
CopyBlobs ==
    /\ copy.st = "blobs"
    /\ copy' = [copy EXCEPT !.st = "active",
                  !.blobs = IF Mutant = "split_drops_blobs" THEN {}
                            ELSE {b \in blobs : \E r \in copy.rows : r.ref = b}]
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, safe, cand, copies, crashes>>

\* A crash drops an unfinished copy and its staged data.
CrashCopy ==
    /\ Copying /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ copy' = Idle
    /\ UNCHANGED <<ts, rows, blobs, pend, upd, safe, cand, copies>>

Next ==
    \/ PutVector \/ PutText \/ Delete \/ ReadForUpdate \/ ApplyUpdate \/ AdvanceSafe
    \/ CollectRow
    \/ \E b \in 1..MaxWrites : FindBlob(b) \/ RemoveBlob(b)
    \/ StartCopy \/ CopyRows \/ CopyBlobs \/ CrashCopy

Spec == Init /\ [][Next]_vars

\* VEC-G10: every retained row version has its vector.
NoDanglingBlob == \A r \in rows : r.ref # 0 => r.ref \in blobs

\* VEC-G10: and so does every copy that is in use.
CopyComplete == copy.st = "active" => \A r \in copy.rows : r.ref # 0 => r.ref \in copy.blobs

\* VEC-G10, VEC-G07: a copy keeps the pending embedding record.
CopyKeepsPending == copy.st = "active" => copy.pend = copy.want

=============================================================================
