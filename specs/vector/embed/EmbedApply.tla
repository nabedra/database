--------------------------- MODULE EmbedApply ---------------------------
(***************************************************************************)
(* Background embedding (plans/oltp-vector.md, section 6) of one row and   *)
(* one embedded column, which can be dropped and added again.              *)
(*                                                                         *)
(* The row, its vector, the pending record and the failure record are      *)
(* replicated state: each step on them is one Data log entry. Leases live  *)
(* in leader memory. Each write that changes the source text gets a new    *)
(* source version, which every later entry must match.                     *)
(*                                                                         *)
(* Properties: VEC-G07 (VectorMatchesSource, NoSpuriousFailure,            *)
(* PendingWhenNeeded, EventuallyEmbedded), VEC-G03 (DeletedStaysDeleted).  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Texts, Embedders, MaxWrites, MaxReAdds, MaxLeaders, MaxCrashes, MaxLost, MaxDups, MaxErrors, MaxAttempts,
          None, Mutant

VARIABLES
    row,        \* [t, v] or None: the text and its source version
    col,        \* the incarnation of the column, each bound to one model for life
    vec,        \* [t, c] or None: the text it was made from and the column incarnation
    pend,       \* [v, n] or None: the pending record, with its durable attempt count
    failed,     \* [v] or None
    ver,        \* the last source version
    leased,     \* leader memory: the pending record is leased
    work,       \* work[e]: the task an embedder holds, or None
    msgs,       \* SetVector and error reports in flight
    upd,        \* a metadata update that read the row and has not been applied, or None
    errs,       \* errs[v]: provider errors seen for source version v
    writes, readds, leaders, crashes, lost, dups, errors

vars == <<row, col, vec, pend, failed, ver, leased, work, msgs, upd, errs,
          writes, readds, leaders, crashes, lost, dups, errors>>
rowv == <<row, col, vec, pend, failed, ver>>
budget == <<writes, readds, leaders, crashes, lost, dups, errors>>

Versions == 1..(MaxWrites + MaxReAdds + 1)

TypeOK ==
    /\ row = None \/ row.t \in Texts
    /\ ver \in 0..(MaxWrites + MaxReAdds) /\ col \in 1..(MaxReAdds + 1)
    /\ vec = None \/ (vec.t \in Texts /\ vec.c \in 1..(MaxReAdds + 1))
    /\ pend = None \/ (pend.v \in Versions /\ pend.n \in 0..MaxAttempts)
    /\ failed = None \/ failed.v \in Versions
    /\ leased \in BOOLEAN

Init ==
    /\ row = None /\ col = 1 /\ vec = None /\ pend = None /\ failed = None /\ ver = 0
    /\ leased = FALSE /\ work = [e \in Embedders |-> None] /\ msgs = {}
    /\ upd = None
    /\ errs = [v \in Versions |-> 0]
    /\ writes = 0 /\ readds = 0 /\ leaders = 0 /\ crashes = 0 /\ lost = 0 /\ dups = 0 /\ errors = 0

-----------------------------------------------------------------------------
(* Client writes.                                                          *)

\* A write that changes the text clears the vector and any failure, and writes a pending record of a
\* new source version, in one entry. Mutant prefix_stale_vector keeps the old vector.
Write(t) ==
    /\ writes < MaxWrites /\ writes' = writes + 1
    /\ IF row # None /\ row.t = t
       THEN UNCHANGED <<row, vec, pend, failed, ver>>
       ELSE /\ ver' = ver + 1
            /\ row' = [t |-> t, v |-> ver + 1]
            /\ vec' = IF Mutant = "prefix_stale_vector" THEN vec ELSE None
            /\ pend' = [v |-> ver + 1, n |-> 0]
            /\ failed' = None
    /\ leased' = FALSE
    /\ UNCHANGED <<col, work, msgs, upd, errs, readds, leaders, crashes, lost, dups, errors>>

Delete ==
    /\ row # None /\ writes < MaxWrites /\ writes' = writes + 1
    /\ row' = None /\ vec' = None /\ pend' = None /\ failed' = None /\ leased' = FALSE
    /\ UNCHANGED <<col, ver, work, msgs, upd, errs, readds, leaders, crashes, lost, dups, errors>>

\* The column is dropped and added again with another model. Backfill creates a pending record for
\* the existing row.
ReAddColumn ==
    /\ readds < MaxReAdds /\ readds' = readds + 1
    /\ col' = col + 1 /\ vec' = None /\ failed' = None /\ leased' = FALSE
    /\ IF row # None
       THEN /\ ver' = ver + 1 /\ row' = [row EXCEPT !.v = ver + 1]
            /\ pend' = [v |-> ver + 1, n |-> 0]
       ELSE UNCHANGED <<ver, row, pend>>
    /\ UNCHANGED <<work, msgs, upd, errs, writes, leaders, crashes, lost, dups, errors>>

\* A metadata update reads the row, then commits against the version it read: its source version and
\* whether it carries a vector. Mutant prefix_setvector_unversioned: SetVector does not change the
\* version, so an update that read before it writes the row back without the vector.
ReadForUpdate ==
    /\ row # None /\ upd = None /\ writes < MaxWrites
    /\ upd' = [v |-> row.v, c |-> col, vec |-> vec, pend |-> pend]
    /\ UNCHANGED <<rowv, leased, work, msgs, errs, budget>>

ApplyUpdate ==
    /\ upd # None /\ writes' = writes + 1
    /\ LET same == row # None /\ row.v = upd.v /\ col = upd.c IN
       /\ writes < MaxWrites
       /\ IF Mutant = "prefix_setvector_unversioned"
          THEN vec' = IF same THEN upd.vec ELSE vec
          ELSE UNCHANGED vec                  \* it keeps the vector, or conflicts and retries
    /\ upd' = None
    /\ UNCHANGED <<row, col, pend, failed, ver, leased, work, msgs, errs, readds, leaders, crashes, lost, dups, errors>>

-----------------------------------------------------------------------------
(* Embedders.                                                              *)

\* Mutant delete_pending_on_lease removes the pending record when it is handed out.
Lease(e) ==
    /\ pend # None /\ row # None /\ ~leased /\ work[e] = None
    /\ leased' = TRUE
    /\ work' = [work EXCEPT ![e] = [t |-> row.t, v |-> pend.v, c |-> col]]
    /\ pend' = IF Mutant = "delete_pending_on_lease" THEN None ELSE pend
    /\ UNCHANGED <<row, col, vec, failed, ver, msgs, upd, errs, budget>>

\* Mutant embedded_never_retried: a lease that a crash or a lost message left behind stays.
LeaseExpires ==
    /\ leased /\ Mutant # "embedded_never_retried"
    /\ leased' = FALSE
    /\ UNCHANGED <<rowv, work, msgs, upd, errs, budget>>

ProviderAnswers(e) ==
    /\ work[e] # None
    /\ msgs' = msgs \cup {[k |-> "set", t |-> work[e].t, v |-> work[e].v, c |-> work[e].c]}
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<rowv, leased, upd, errs, budget>>

ProviderFails(e) ==
    /\ work[e] # None /\ errors < MaxErrors /\ errors' = errors + 1
    /\ msgs' = msgs \cup {[k |-> "err", t |-> work[e].t, v |-> work[e].v, c |-> work[e].c]}
    /\ errs' = [errs EXCEPT ![work[e].v] = @ + 1]
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<rowv, leased, upd, writes, readds, leaders, crashes, lost, dups>>

\* SetVector takes effect only if the row exists and the source version and the column incarnation
\* match. Mutants drop one of these checks.
SetTakes(m) ==
    /\ row # None \/ Mutant = "apply_on_deleted_row"
    /\ \/ Mutant \in {"fence_by_key_only", "apply_on_deleted_row"}
       \/ row # None /\ (row.v = m.v \/ (Mutant = "fence_without_column" /\ row.t = m.t))
    /\ m.c = col \/ Mutant \in {"fence_without_column", "fence_by_key_only"}

ApplySet(m) ==
    IF SetTakes(m)
    THEN /\ vec' = [t |-> m.t, c |-> m.c]
         /\ pend' = None /\ failed' = None
    ELSE UNCHANGED <<vec, pend, failed>>

HandleSet ==
    /\ \E m \in msgs : m.k = "set" /\ ApplySet(m) /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<row, col, ver, leased, work, upd, errs, budget>>

DupSet ==
    /\ dups < MaxDups /\ dups' = dups + 1
    /\ \E m \in msgs : m.k = "set" /\ ApplySet(m)
    /\ UNCHANGED <<row, col, ver, leased, work, msgs, upd, errs, writes, readds, leaders, crashes, lost, errors>>

\* An error report counts an attempt in the pending record, durably, and after MaxAttempts turns it
\* into a failure record. It must match the pending source version.
\* Mutant prefix_unfenced_failure counts any report. Mutant prefix_volatile_attempts: see LeaderChange.
HandleErr ==
    /\ \E m \in msgs :
       /\ m.k = "err" /\ msgs' = msgs \ {m}
       /\ IF pend # None /\ (pend.v = m.v \/ Mutant = "prefix_unfenced_failure")
          THEN IF pend.n + 1 >= MaxAttempts
               THEN failed' = [v |-> pend.v] /\ pend' = None
               ELSE pend' = [pend EXCEPT !.n = @ + 1] /\ UNCHANGED failed
          ELSE UNCHANGED <<pend, failed>>
    /\ leased' = FALSE
    /\ UNCHANGED <<row, col, vec, ver, work, upd, errs, budget>>

-----------------------------------------------------------------------------
(* Faults.                                                                 *)

LeaderChange ==
    /\ leaders < MaxLeaders /\ leaders' = leaders + 1 /\ leased' = FALSE
    /\ UNCHANGED <<rowv, work, msgs, upd, errs, writes, readds, crashes, lost, dups, errors>>

EmbedderCrash(e) ==
    /\ work[e] # None /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<rowv, leased, msgs, upd, errs, writes, readds, leaders, lost, dups, errors>>

LoseMsg ==
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ \E m \in msgs : msgs' = msgs \ {m}
    /\ UNCHANGED <<rowv, leased, work, upd, errs, writes, readds, leaders, crashes, dups, errors>>

Next ==
    \/ \E t \in Texts : Write(t)
    \/ Delete \/ ReAddColumn \/ ReadForUpdate \/ ApplyUpdate \/ LeaseExpires \/ LeaderChange
    \/ \E e \in Embedders : Lease(e) \/ ProviderAnswers(e) \/ ProviderFails(e) \/ EmbedderCrash(e)
    \/ HandleSet \/ DupSet \/ HandleErr \/ LoseMsg

\* The embedder's poll loop, the provider client with its deadline, the proposal path, the lease timer.
Fairness ==
    /\ WF_vars(LeaseExpires)
    /\ \A e \in Embedders : SF_vars(Lease(e)) /\ WF_vars(ProviderAnswers(e))
    /\ WF_vars(HandleSet) /\ WF_vars(HandleErr)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------

\* VEC-G07: a stored vector was made from the row's current text, for the current column.
VectorMatchesSource == vec # None /\ row # None => vec.t = row.t /\ vec.c = col

\* VEC-G03: a deleted row has no vector.
DeletedStaysDeleted == row = None => vec = None

\* VEC-G07: every row without a vector is waiting for one or is marked failed.
PendingWhenNeeded ==
    row # None /\ vec = None =>
        \/ pend # None /\ pend.v = row.v
        \/ failed # None /\ failed.v = row.v

\* VEC-G07: a row is marked failed only after the provider failed MaxAttempts times on that version.
NoSpuriousFailure == failed # None => errs[failed.v] >= MaxAttempts

\* VEC-G07: once writes and faults stop, the row has its vector or is marked failed.
EventuallyEmbedded == <>[](row # None => vec # None \/ failed # None)

=============================================================================
