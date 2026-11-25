--------------------------- MODULE EmbedApply ---------------------------
(***************************************************************************)
(* Background embedding (plans/oltp-vector.md, section 6) of one row and   *)
(* one embedded column, which can be dropped and added again.              *)
(*                                                                         *)
(* The row, its vector, the pending record and the failure record are      *)
(* replicated state: each step on them is one Data log entry. Leases, and  *)
(* a commit that read the row and has not applied, live in leader memory.  *)
(* Every commit bumps the row version. A commit that changes the source    *)
(* text gets a new source version, which every later entry must match.     *)
(*                                                                         *)
(* Properties: VEC-G07 (VectorMatchesSource, CountedOnce,                  *)
(* PendingWhenNeeded, EventuallyEmbedded), VEC-G03 (DeletedStaysDeleted).  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Texts, Embedders, MaxWrites, MaxUpdates, MaxReAdds, MaxLeaders, MaxCrashes, MaxLost, MaxDups, MaxErrors,
          MaxRequeues, MaxAttempts, None, Mutant

VARIABLES
    row,        \* [t, v, n] or None: text, source version, row version
    ver,        \* the last commit timestamp, source of both versions
    col,        \* the incarnation of the column, each bound to one model for life
    filled,     \* backfill of this incarnation reached the row
    vec,        \* [t, c] or None: the text it was made from and the column incarnation
    pend,       \* [v, n, max] or None: pending record, attempts so far, attempts allowed
    failed,     \* [v, n] or None
    leased,     \* leader memory: the pending record is leased
    work,       \* work[e]: the task an embedder holds, or None
    msgs,       \* SetVector and error reports in flight
    setp,       \* a SetVector that read the row and has not applied: [m, n], or None
    upd,        \* a metadata update that read the row and has not applied: [n, vec], or None
    errs,       \* errs[v]: provider errors for source version v
    counted,    \* counted[v]: attempts that apply counted for it, requeues included
    writes, updates, readds, leaders, crashes, lost, dups, errors, requeues

vars == <<row, ver, col, filled, vec, pend, failed, leased, work, msgs, setp, upd, errs, counted,
          writes, updates, readds, leaders, crashes, lost, dups, errors, requeues>>
budget == <<writes, updates, readds, leaders, crashes, lost, dups, errors, requeues>>

MaxVer == 4 * (MaxWrites + MaxUpdates + MaxReAdds + MaxDups + MaxRequeues + 1)
Versions == 1..MaxVer

TypeOK ==
    /\ row = None \/ (row.t \in Texts /\ row.v \in Versions /\ row.n \in Versions)
    /\ col \in 1..(MaxReAdds + 1) /\ filled \in BOOLEAN /\ leased \in BOOLEAN
    /\ vec = None \/ (vec.t \in Texts /\ vec.c \in 1..(MaxReAdds + 1))

Init ==
    /\ row = None /\ ver = 0 /\ col = 1 /\ filled = TRUE /\ vec = None /\ pend = None /\ failed = None
    /\ leased = FALSE /\ work = [e \in Embedders |-> None] /\ msgs = {} /\ setp = None /\ upd = None
    /\ errs = [v \in Versions |-> 0] /\ counted = [v \in Versions |-> 0]
    /\ writes = 0 /\ updates = 0 /\ readds = 0 /\ leaders = 0 /\ crashes = 0 /\ lost = 0 /\ dups = 0
    /\ errors = 0 /\ requeues = 0

Fresh(v) == [v |-> v, n |-> 0, max |-> MaxAttempts]

-----------------------------------------------------------------------------
(* Client writes.                                                          *)

\* A write that changes the text clears the vector and any failure, and writes a pending record of a
\* new source version, in one entry. One that keeps the text keeps all three.
\* Mutant prefix_stale_vector keeps the old vector.
Write(t) ==
    /\ writes < MaxWrites /\ writes' = writes + 1 /\ ver' = ver + 1
    /\ IF row # None /\ row.t = t
       THEN row' = [row EXCEPT !.n = ver + 1] /\ UNCHANGED <<vec, pend, failed, leased>>
       ELSE /\ row' = [t |-> t, v |-> ver + 1, n |-> ver + 1]
            /\ vec' = IF Mutant = "prefix_stale_vector" THEN vec ELSE None
            /\ pend' = Fresh(ver + 1) /\ failed' = None /\ leased' = FALSE
    /\ UNCHANGED <<counted, col, filled, work, msgs, setp, upd, errs, updates, readds, leaders, crashes, lost, dups, errors, requeues>>

Delete ==
    /\ row # None /\ writes < MaxWrites /\ writes' = writes + 1 /\ ver' = ver + 1
    /\ row' = None /\ vec' = None /\ pend' = None /\ failed' = None /\ leased' = FALSE
    /\ UNCHANGED <<counted, col, filled, work, msgs, setp, upd, errs, updates, readds, leaders, crashes, lost, dups, errors, requeues>>

\* A metadata update reads the row image, vector reference included, and commits against the row
\* version it read. A conflict is retried by its client.
ReadForUpdate ==
    /\ row # None /\ upd = None /\ updates < MaxUpdates
    /\ updates' = updates + 1
    /\ upd' = [n |-> row.n, vec |-> vec]
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, work, msgs, setp, errs,
                   writes, readds, leaders, crashes, lost, dups, errors, requeues>>

ApplyUpdate ==
    /\ upd # None /\ upd' = None
    /\ IF row # None /\ row.n = upd.n
       THEN ver' = ver + 1 /\ row' = [row EXCEPT !.n = ver + 1] /\ vec' = upd.vec
       ELSE UNCHANGED <<ver, row, vec>>
    /\ UNCHANGED <<counted, col, filled, pend, failed, leased, work, msgs, setp, errs, budget>>

\* The column is dropped and added again, with another model: a new incarnation with no data.
ReAddColumn ==
    /\ readds < MaxReAdds /\ readds' = readds + 1
    /\ col' = col + 1 /\ filled' = FALSE
    /\ vec' = None /\ pend' = None /\ failed' = None /\ leased' = FALSE
    /\ upd' = None                          \* the schema epoch refuses a write that read the old schema
    /\ UNCHANGED <<counted, row, ver, work, msgs, setp, errs, writes, updates, leaders, crashes, lost, dups, errors, requeues>>

\* Backfill reaches the row: a pending record of its current source version, unless it has a vector,
\* a pending record or a failure of this incarnation.
Backfill ==
    /\ ~filled /\ filled' = TRUE
    /\ IF row # None /\ vec = None /\ pend = None /\ failed = None
       THEN pend' = Fresh(row.v) ELSE UNCHANGED pend
    /\ UNCHANGED <<counted, row, ver, col, vec, failed, leased, work, msgs, setp, upd, errs, budget>>

-----------------------------------------------------------------------------
(* Embedders.                                                              *)

\* Mutant delete_pending_on_lease removes the pending record when it is handed out.
Lease(e) ==
    /\ pend # None /\ row # None /\ ~leased /\ work[e] = None
    /\ leased' = TRUE
    /\ work' = [work EXCEPT ![e] = [t |-> row.t, v |-> pend.v, c |-> col, n |-> pend.n]]
    /\ pend' = IF Mutant = "delete_pending_on_lease" THEN None ELSE pend
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, failed, msgs, setp, upd, errs, budget>>

\* Mutant embedded_never_retried: a lease that a crash or a lost message left behind stays.
LeaseExpires ==
    /\ leased /\ Mutant # "embedded_never_retried"
    /\ leased' = FALSE
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, work, msgs, setp, upd, errs, budget>>

Report(e, k) == [k |-> k, t |-> work[e].t, v |-> work[e].v, c |-> work[e].c, n |-> work[e].n]

ProviderAnswers(e) ==
    /\ work[e] # None
    /\ msgs' = msgs \cup {Report(e, "set")}
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, setp, upd, errs, budget>>

ProviderFails(e) ==
    /\ work[e] # None /\ errors < MaxErrors /\ errors' = errors + 1
    /\ msgs' = msgs \cup {Report(e, "err")}
    /\ errs' = [errs EXCEPT ![work[e].v] = @ + 1]
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, setp, upd, writes, updates, readds, leaders, crashes, lost, dups, requeues>>

\* The leader reads the row version for a SetVector. The message stays until it settles, so it may
\* arrive again: a lost reply, a resend, a duplicate.
ReadForSet ==
    /\ setp = None
    /\ \E m \in msgs :
        /\ m.k = "set"
        /\ setp' = [m |-> m, n |-> IF row = None THEN 0 ELSE row.n]
        /\ IF dups < MaxDups THEN \/ msgs' = msgs \ {m} /\ UNCHANGED dups
                                  \/ UNCHANGED msgs /\ dups' = dups + 1
           ELSE msgs' = msgs \ {m} /\ UNCHANGED dups
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, work, upd, errs, writes, updates, readds, leaders, crashes, lost, errors, requeues>>

\* SetVector takes effect only if the row exists and the source version and the column incarnation
\* match. Mutants drop one of these checks.
SetTakes(m) ==
    /\ ~(vec # None /\ vec = [t |-> m.t, c |-> m.c] /\ pend = None)     \* it took effect already
    /\ row # None \/ Mutant = "apply_on_deleted_row"
    /\ Mutant \in {"fence_by_key_only", "apply_on_deleted_row"} \/ (row # None /\ row.v = m.v)
    /\ m.c = col \/ Mutant = "fence_without_column"

\* It applies against the row version the leader read, and bumps it. On a conflict the leader reads
\* again. Mutant prefix_setvector_unversioned neither compares nor bumps the row version.
ApplySet ==
    /\ setp # None
    /\ LET m == setp.m
           free == Mutant = "prefix_setvector_unversioned"
           same == (row = None /\ setp.n = 0) \/ (row # None /\ row.n = setp.n) IN
       IF SetTakes(m) /\ ~same /\ ~free
       THEN /\ setp' = [setp EXCEPT !.n = IF row = None THEN 0 ELSE row.n]
            /\ UNCHANGED <<ver, row, vec, pend, failed>>
       ELSE /\ setp' = None
            /\ IF SetTakes(m)
               THEN /\ vec' = [t |-> m.t, c |-> m.c] /\ pend' = None /\ failed' = None
                    /\ IF free \/ row = None THEN UNCHANGED <<ver, row>>
                       ELSE ver' = ver + 1 /\ row' = [row EXCEPT !.n = ver + 1]
               ELSE UNCHANGED <<ver, row, vec, pend, failed>>
    /\ UNCHANGED <<counted, col, filled, leased, work, msgs, upd, errs, budget>>

\* An error report carries the attempt it saw. It counts that attempt once, durably, only for a
\* matching source version, incarnation and attempt. Mutants: prefix_unfenced_failure counts a report
\* of any version, count_every_report counts a repeated one again.
HandleErr ==
    /\ \E m \in msgs :
        /\ m.k = "err"
        /\ IF dups < MaxDups THEN \/ msgs' = msgs \ {m} /\ UNCHANGED dups
                                  \/ UNCHANGED msgs /\ dups' = dups + 1
           ELSE msgs' = msgs \ {m} /\ UNCHANGED dups
        /\ IF /\ pend # None
              /\ pend.v = m.v \/ Mutant = "prefix_unfenced_failure"
              /\ m.c = col
              /\ pend.n = m.n \/ Mutant \in {"count_every_report", "prefix_unfenced_failure"}
           THEN /\ counted' = [counted EXCEPT ![pend.v] = @ + 1]
                /\ IF pend.n + 1 >= pend.max
                   THEN failed' = [v |-> pend.v, n |-> pend.n + 1] /\ pend' = None
                   ELSE pend' = [pend EXCEPT !.n = @ + 1] /\ UNCHANGED failed
           ELSE UNCHANGED <<pend, failed, counted>>
    /\ leased' = FALSE
    /\ UNCHANGED <<row, ver, col, filled, vec, work, setp, upd, errs, writes, updates, readds, leaders, crashes, lost, errors, requeues>>

\* An operator requeues a failed row. The attempt number goes on, so a late report of an earlier
\* attempt does not match.
\* Mutant requeue_resets_attempts starts again from zero.
Requeue ==
    /\ failed # None /\ row # None /\ requeues < MaxRequeues /\ requeues' = requeues + 1
    /\ LET n0 == IF Mutant = "requeue_resets_attempts" THEN 0 ELSE failed.n IN
       pend' = [v |-> failed.v, n |-> n0, max |-> n0 + MaxAttempts]
    /\ failed' = None
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, leased, work, msgs, setp, upd, errs, writes, updates, readds, leaders, crashes, lost, dups, errors>>

-----------------------------------------------------------------------------
(* Faults.                                                                 *)

LeaderChange ==
    /\ leaders < MaxLeaders /\ leaders' = leaders + 1 /\ leased' = FALSE
    /\ setp' = None /\ upd' = None
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, work, msgs, errs, writes, updates, readds, crashes, lost, dups, errors, requeues>>

EmbedderCrash(e) ==
    /\ work[e] # None /\ crashes < MaxCrashes /\ crashes' = crashes + 1
    /\ work' = [work EXCEPT ![e] = None]
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, msgs, setp, upd, errs, writes, updates, readds, leaders, lost, dups, errors, requeues>>

LoseMsg ==
    /\ lost < MaxLost /\ lost' = lost + 1
    /\ \E m \in msgs : msgs' = msgs \ {m}
    /\ UNCHANGED <<counted, row, ver, col, filled, vec, pend, failed, leased, work, setp, upd, errs, writes, updates, readds, leaders, crashes, dups, errors, requeues>>

\* Nothing left to do: no pending record, no work, no message.
Terminated ==
    /\ pend = None /\ msgs = {} /\ setp = None /\ upd = None /\ filled
    /\ \A e \in Embedders : work[e] = None
    /\ UNCHANGED vars

Next ==
    \/ \E t \in Texts : Write(t)
    \/ Delete \/ ReAddColumn \/ Backfill \/ ReadForUpdate \/ ApplyUpdate \/ LeaseExpires \/ LeaderChange \/ Requeue
    \/ \E e \in Embedders : Lease(e) \/ ProviderAnswers(e) \/ ProviderFails(e) \/ EmbedderCrash(e)
    \/ ReadForSet \/ ApplySet \/ HandleErr \/ LoseMsg
    \/ Terminated

\* The embedder's poll loop, the provider client with its deadline, the proposal path with its retry,
\* the backfill job and the lease timer.
Fairness ==
    /\ WF_vars(LeaseExpires) /\ WF_vars(Backfill)
    /\ \A e \in Embedders : SF_vars(Lease(e)) /\ SF_vars(ProviderAnswers(e))
    /\ SF_vars(ReadForSet) /\ WF_vars(ApplySet) /\ SF_vars(HandleErr)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------

\* VEC-G07: a stored vector was made from the row's current text, for the current column.
VectorMatchesSource == vec # None /\ row # None => vec.t = row.t /\ vec.c = col

\* VEC-G03: a deleted row has no vector.
DeletedStaysDeleted == row = None => vec = None

\* VEC-G07: every row that backfill reached and that has no vector is waiting for one or is failed.
PendingWhenNeeded ==
    row # None /\ vec = None /\ filled =>
        \/ pend # None /\ pend.v = row.v
        \/ failed # None /\ failed.v = row.v

\* VEC-G07: an attempt is counted at most once per provider error of that source version.
CountedOnce == \A v \in Versions : counted[v] <= errs[v]

\* VEC-G07: once writes and faults stop, the row has its vector or is marked failed.
EventuallyEmbedded == <>[](row # None => vec # None \/ failed # None)

=============================================================================
