---------------------------- MODULE RowBlobs ----------------------------
(***************************************************************************)
(* Vector values next to their rows (plans/oltp-vector.md, section 2).     *)
(*                                                                         *)
(* One key of one range. A row version names the vector version it uses,   *)
(* so a metadata update does not rewrite the vector. MVCC collection runs  *)
(* in two steps, find and remove. A cross-range transaction stages its     *)
(* vector under the transaction before it has a commit timestamp. A copy   *)
(* at a cut (split child, backup, restore) reads rows and vectors in       *)
(* different steps while collection runs, and while writes go on unless it *)
(* is a split. Activation derives the pending embedding record. A replica  *)
(* snapshot is one engine snapshot, so it is one step and not modelled.    *)
(*                                                                         *)
(* Property: VEC-G10 (NoDanglingBlob, CopyExact, CopyComplete,             *)
(* CopyKeepsPending).                                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS MaxWrites, MaxCopies, MaxCrashes, None, Mutant

VARIABLES
    ts,         \* the last commit timestamp
    rows,       \* row versions [ts, del, txt, ref]; txt: it has a source text, ref: vector version or 0
    hist,       \* every row version ever written, for the properties
    blobs,      \* vector versions present
    staged,     \* "none", "staged" or "lost": the vector of an open transaction
    upd,        \* a metadata update that read the newest version [ts, ref, txt], or None
    safe,       \* the collection safe point
    cand,       \* vector versions that a collection scan found unreferenced
    copy,       \* [st, split, cut, rows, blobs, pend]; st: "idle", "rows", "blobs", "active"
    copies, crashes

vars == <<ts, rows, hist, blobs, staged, upd, safe, cand, copy, copies, crashes>>

Newest(R) == CHOOSE r \in R : \A x \in R : x.ts <= r.ts
\* What a copy at the cut must hold: the newest version at or below it, unless that is a delete.
At(R, c) == LET S == {r \in R : r.ts <= c} IN IF S = {} \/ Newest(S).del THEN {} ELSE {Newest(S)}
NeedsVector(R) == \E r \in R : r.txt /\ r.ref = 0
Idle == [st |-> "idle", split |-> FALSE, cut |-> 0, rows |-> {}, blobs |-> {}, pend |-> FALSE]
Copying == copy.st \in {"rows", "blobs"}

TypeOK ==
    /\ ts \in 0..MaxWrites /\ safe \in 0..ts /\ staged \in {"none", "staged", "lost"}
    /\ blobs \subseteq 1..MaxWrites /\ rows \subseteq hist
    /\ copies \in 0..MaxCopies /\ crashes \in 0..MaxCrashes

Init ==
    /\ ts = 0 /\ rows = {} /\ hist = {} /\ blobs = {} /\ staged = "none" /\ upd = None /\ safe = 0 /\ cand = {}
    /\ copy = Idle /\ copies = 0 /\ crashes = 0

\* Only a split freezes the range.
CanWrite == ts < MaxWrites /\ ~(Copying /\ copy.split)

Put(r) == rows' = rows \cup {r} /\ hist' = hist \cup {r} /\ ts' = r.ts

\* A row with its vector, a background SetVector included.
PutVector ==
    /\ CanWrite /\ Put([ts |-> ts + 1, del |-> FALSE, txt |-> TRUE, ref |-> ts + 1])
    /\ blobs' = blobs \cup {ts + 1}
    /\ UNCHANGED <<staged, upd, safe, cand, copy, copies, crashes>>

\* A row with text only: its vector is pending.
PutText ==
    /\ CanWrite /\ Put([ts |-> ts + 1, del |-> FALSE, txt |-> TRUE, ref |-> 0])
    /\ UNCHANGED <<blobs, staged, upd, safe, cand, copy, copies, crashes>>

Delete ==
    /\ CanWrite /\ rows # {} /\ ~Newest(rows).del
    /\ Put([ts |-> ts + 1, del |-> TRUE, txt |-> FALSE, ref |-> 0])
    /\ UNCHANGED <<blobs, staged, upd, safe, cand, copy, copies, crashes>>

ReadForUpdate ==
    /\ upd = None /\ rows # {} /\ ~Newest(rows).del
    /\ upd' = [ts |-> Newest(rows).ts, ref |-> Newest(rows).ref, txt |-> Newest(rows).txt]
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, safe, cand, copy, copies, crashes>>

\* The commit carries the version it read. Mutant no_expected_version applies it anyway.
ApplyUpdate ==
    /\ upd # None /\ upd' = None
    /\ IF CanWrite /\ rows # {} /\ (Newest(rows).ts = upd.ts \/ Mutant = "no_expected_version")
       THEN Put([ts |-> ts + 1, del |-> FALSE, txt |-> upd.txt, ref |-> upd.ref])
       ELSE UNCHANGED <<ts, rows, hist>>
    /\ UNCHANGED <<blobs, staged, safe, cand, copy, copies, crashes>>

\* Prepare stages the vector under the transaction, Resolve gives it the commit timestamp.
Prepare ==
    /\ staged = "none" /\ CanWrite /\ staged' = "staged"
    /\ UNCHANGED <<ts, rows, hist, blobs, upd, safe, cand, copy, copies, crashes>>

Resolve ==
    /\ staged # "none" /\ CanWrite
    /\ Put([ts |-> ts + 1, del |-> FALSE, txt |-> TRUE, ref |-> ts + 1])
    /\ blobs' = IF staged = "staged" THEN blobs \cup {ts + 1} ELSE blobs
    /\ staged' = "none"
    /\ UNCHANGED <<upd, safe, cand, copy, copies, crashes>>

-----------------------------------------------------------------------------
(* Collection.                                                             *)

\* A copy pins the safe point at its cut. Mutant copy_unpinned does not.
AdvanceSafe ==
    /\ safe < ts /\ (~Copying \/ safe < copy.cut \/ Mutant = "copy_unpinned")
    /\ safe' = safe + 1
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, cand, copy, copies, crashes>>

\* A version goes when a newer one is at or below the safe point.
CollectRow ==
    /\ \E r \in rows :
        /\ r.ts <= safe
        /\ \E x \in rows : x.ts > r.ts /\ x.ts <= safe
        /\ rows' = rows \ {r}
    /\ UNCHANGED <<ts, hist, blobs, staged, upd, safe, cand, copy, copies, crashes>>

\* Mutant gc_ignores_reference takes any vector older than the safe point and the newest row.
Referenced(b) == \E r \in rows : r.ref = b
FindBlob(b) ==
    /\ b \in blobs /\ b \notin cand
    /\ IF Mutant = "gc_ignores_reference" THEN b < safe /\ b # Newest(rows).ts ELSE ~Referenced(b)
    /\ cand' = cand \cup {b}
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, safe, copy, copies, crashes>>

\* Found earlier, removed now, with no second look: an unreferenced vector can gain no reference.
\* Mutant gc_takes_staged also takes the vector of an open transaction.
RemoveBlob(b) ==
    /\ b \in cand
    /\ cand' = cand \ {b} /\ blobs' = blobs \ {b}
    /\ staged' = IF Mutant = "gc_takes_staged" /\ staged = "staged" THEN "lost" ELSE staged
    /\ UNCHANGED <<ts, rows, hist, upd, safe, copy, copies, crashes>>

-----------------------------------------------------------------------------
(* Copies at a cut.                                                        *)

\* A split freezes the range, and its freeze refuses while a transaction is open in it.
StartCopy(split) ==
    /\ copy.st \in {"idle", "active"} /\ copies < MaxCopies /\ copies' = copies + 1
    /\ split => staged = "none"
    /\ copy' = [Idle EXCEPT !.st = "rows", !.split = split, !.cut = ts]
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, safe, cand, crashes>>

\* The newest version at or below the cut. Mutants: prefix_copy_takes_collectable takes every version
\* that is still there, copy_drops_row takes none.
CopyRows ==
    /\ copy.st = "rows"
    /\ copy' = [copy EXCEPT !.st = "blobs",
                  !.rows = CASE Mutant = "prefix_copy_takes_collectable" -> {r \in rows : r.ts <= copy.cut}
                             [] Mutant = "copy_drops_row" -> {}
                             [] OTHER -> At(rows, copy.cut)]
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, safe, cand, copies, crashes>>

\* The vectors, then activation, which derives the pending record from the copied row.
\* Mutants: split_drops_blobs copies no vectors, prefix_copy_without_pending derives nothing.
CopyBlobs ==
    /\ copy.st = "blobs"
    /\ copy' = [copy EXCEPT !.st = "active",
                  !.blobs = IF Mutant = "split_drops_blobs" THEN {}
                            ELSE {b \in blobs : \E r \in copy.rows : r.ref = b},
                  !.pend = NeedsVector(copy.rows) /\ Mutant # "prefix_copy_without_pending"]
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, safe, cand, copies, crashes>>

\* A crash drops an unfinished copy and its staged data.
CrashCopy ==
    /\ Copying /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ copy' = Idle
    /\ UNCHANGED <<ts, rows, hist, blobs, staged, upd, safe, cand, copies>>

Terminated == ts = MaxWrites /\ ~Copying /\ UNCHANGED vars

Next ==
    \/ PutVector \/ PutText \/ Delete \/ ReadForUpdate \/ ApplyUpdate \/ Prepare \/ Resolve
    \/ AdvanceSafe \/ CollectRow
    \/ \E b \in 1..MaxWrites : FindBlob(b) \/ RemoveBlob(b)
    \/ \E split \in BOOLEAN : StartCopy(split)
    \/ CopyRows \/ CopyBlobs \/ CrashCopy
    \/ Terminated

Spec == Init /\ [][Next]_vars

\* VEC-G10: every retained row version has its vector.
NoDanglingBlob == \A r \in rows : r.ref # 0 => r.ref \in blobs

\* VEC-G10: an active copy holds exactly the rows of its cut.
CopyExact == copy.st = "active" => copy.rows = At(hist, copy.cut)

\* VEC-G10: with their vectors.
CopyComplete == copy.st = "active" => \A r \in copy.rows : r.ref # 0 => r.ref \in copy.blobs

\* VEC-G10, VEC-G07: and a pending record if the row lacks its vector.
CopyKeepsPending == copy.st = "active" => (copy.pend <=> NeedsVector(copy.rows))

=============================================================================
