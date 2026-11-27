-------------------------- MODULE OlapDecisionIndex ---------------------------
(* OLAP-1. Refines catalog publication with an immutable UUID index. A      *)
(* bounded page path is abstracted as an immutable finite map. The map and *)
(* decision reachability change together at the catalog Match. The map     *)
(* key is the complete UUID, including keys that share a page prefix.      *)
(*                                                                         *)
(* Ledger is the oracle, independent of the index. An attempt slot models  *)
(* a prepared private decision and its copied index path. It can survive  *)
(* the writer's deadline.                                                  *)
(*                                                                         *)
(* Catalog format 6 is the only format (user decision 2026-11-27): every   *)
(* root carries the index, so there is no migration, no legacy writer and  *)
(* no mixed-version model. The baseline checks three writer attempts, one  *)
(* prior UUID and two fresh UUIDs, CAS races, an ambiguous deadline and a  *)
(* lost reply. Row and reader behavior remains in OlapCommit. Bounds do    *)
(* not model byte-level encoding or S3 I/O.                                *)
EXTENDS Naturals, FiniteSets

CONSTANTS Writers, Old, FreshA, FreshB, Nil, Mutant

ASSUME /\ Cardinality(Writers) = 3
       /\ Cardinality({Old, FreshA, FreshB}) = 3
       /\ Mutant \in {"none", "ignore_match", "drop_index_update", "match_prefix",
                      "missing_page_absent", "timeout_final", "lost_reply_old_root"}

Requests == {Old, FreshA, FreshB}
Bodies == {"a", "b"}
Empty == [r \in Requests |-> Nil]
OldDecision == [req |-> Old, body |-> "a", result |-> <<"initial", 0>>]
Initial == [Empty EXCEPT ![Old] = OldDecision]

VARIABLES revision, ledger, committed, index, attempts, timedOut, lostReply,
          lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate

vars == <<revision, ledger, committed, index, attempts, timedOut, lostReply,
          lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate>>

Init ==
    /\ revision = 1
    /\ ledger = Initial /\ committed = {<<Old, OldDecision.result>>}
    /\ index = Initial
    /\ attempts = [w \in Writers |-> Nil]
    /\ timedOut = FALSE /\ lostReply = FALSE
    /\ lookupVerified = FALSE /\ lookupAccurate = TRUE
    /\ timeoutAccurate = TRUE /\ replayAccurate = TRUE

\* The actual decision stores both request body and terminal response. The
\* result tag represents immutable response status and bytes.
NoAnswer(kind) == [kind |-> kind, decision |-> Nil]
Found(decision) == [kind |-> "found", decision |-> decision]

Answer(decision, body) ==
    IF decision = Nil THEN NoAnswer("absent")
    ELSE IF decision.body = body THEN Found(decision) ELSE NoAnswer("conflict")

\* A lookup on the current root. An unreadable page is never absence.
Resolve(r, body, readable) ==
    IF ~readable THEN
        IF Mutant = "missing_page_absent" THEN NoAnswer("absent") ELSE NoAnswer("unknown")
    ELSE LET actual == IF Mutant = "match_prefix" /\ r = FreshB THEN FreshA ELSE r
         IN Answer(index[actual], body)

\* The index a commit publishes: the base's exact index plus its decision.
NextIndex(r, decision) ==
    IF Mutant = "drop_index_update" THEN index ELSE [index EXCEPT ![r] = decision]

Prepare(w, r, body) ==
    /\ attempts[w] = Nil
    /\ Resolve(r, body, TRUE) = NoAnswer("absent")
    /\ LET decision == [req |-> r, body |-> body, result |-> <<w, revision>>]
       IN attempts' = [attempts EXCEPT ![w] =
            [base |-> revision, decision |-> decision, timedOut |-> FALSE,
             slot |-> NextIndex(r, decision), phase |-> "prepared"]]
    /\ UNCHANGED <<revision, ledger, committed, index, timedOut, lostReply,
                   lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate>>

Submit(w) ==
    /\ attempts[w] # Nil /\ attempts[w].phase = "prepared"
    /\ attempts' = [attempts EXCEPT ![w].phase = "pending"]
    /\ UNCHANGED <<revision, ledger, committed, index, timedOut, lostReply,
                   lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate>>

Apply(w, afterTimeout) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "pending" /\ a.timedOut = afterTimeout
    /\ a.base = revision \/ Mutant = "ignore_match"
    /\ revision' = revision + 1
    /\ index' = a.slot
    /\ ledger' = [ledger EXCEPT ![a.decision.req] = a.decision]
    /\ committed' = committed \cup {<<a.decision.req, a.decision.result>>}
    /\ attempts' = [attempts EXCEPT ![w].phase = "applied"]
    /\ UNCHANGED <<timedOut, lostReply, lookupVerified, lookupAccurate,
                   timeoutAccurate, replayAccurate>>

Conflict(w) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "pending" /\ a.base # revision
    \* A definite conflict changes no authoritative data. Recovery consults
    \* the latest root, including an exact outcome committed by the winner.
    /\ replayAccurate' = (replayAccurate /\ Resolve(a.decision.req, a.decision.body, TRUE) =
                          Answer(ledger[a.decision.req], a.decision.body))
    /\ attempts' = [attempts EXCEPT ![w].phase = "conflict"]
    /\ UNCHANGED <<revision, ledger, committed, index, timedOut, lostReply,
                   lookupVerified, lookupAccurate, timeoutAccurate>>

Deadline(w) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "pending" /\ ~timedOut
    /\ attempts' = [attempts EXCEPT ![w].timedOut = TRUE]
    /\ timedOut' = TRUE
    \* With no decision in a successful read, this attempt's outcome is
    \* unknown; its already-submitted PUT can still win at the store.
    /\ LET known == Resolve(a.decision.req, a.decision.body, TRUE)
           reply == IF known.kind # "absent" THEN known
                    ELSE IF Mutant = "timeout_final" THEN NoAnswer("not_applied")
                         ELSE NoAnswer("unknown")
       IN timeoutAccurate' = (timeoutAccurate /\ reply.kind # "not_applied")
    /\ UNCHANGED <<revision, ledger, committed, index, lostReply, lookupVerified,
                   lookupAccurate, replayAccurate>>

LoseReplyAndRecover(w) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "applied" /\ ~lostReply
    /\ lostReply' = TRUE
    /\ replayAccurate' =
        (replayAccurate /\ (IF Mutant = "lost_reply_old_root" THEN NoAnswer("absent")
                            ELSE Resolve(a.decision.req, a.decision.body, TRUE)) = Found(a.decision))
    /\ UNCHANGED <<revision, ledger, committed, index, attempts, timedOut,
                   lookupVerified, lookupAccurate, timeoutAccurate>>

ProbeLookup ==
    /\ ~lookupVerified
    /\ lookupVerified' = TRUE
    /\ lookupAccurate' =
        \A r \in Requests, body \in Bodies :
            /\ Resolve(r, body, TRUE) = Answer(ledger[r], body)
            /\ Resolve(r, body, FALSE) = NoAnswer("unknown")
    /\ UNCHANGED <<revision, ledger, committed, index, attempts, timedOut, lostReply,
                   timeoutAccurate, replayAccurate>>

PrepareAny == \E w \in Writers, r \in Requests, body \in Bodies : Prepare(w, r, body)
SubmitAny == \E w \in Writers : Submit(w)
ApplyOnTime == \E w \in Writers : Apply(w, FALSE)
ApplyLate == \E w \in Writers : Apply(w, TRUE)
ConflictAny == \E w \in Writers : Conflict(w)
DeadlineAny == \E w \in Writers : Deadline(w)
LostReplyAny == \E w \in Writers : LoseReplyAndRecover(w)

Done ==
    /\ \A w \in Writers : IF attempts[w] = Nil THEN TRUE ELSE attempts[w].phase \in {"applied", "conflict"}
    /\ UNCHANGED vars

Next ==
    \/ PrepareAny \/ SubmitAny \/ ApplyOnTime \/ ApplyLate \/ ConflictAny
    \/ DeadlineAny \/ LostReplyAny \/ ProbeLookup \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ revision \in Nat
    /\ DOMAIN ledger = Requests /\ DOMAIN index = Requests
    /\ DOMAIN attempts = Writers
    /\ \A w \in Writers : IF attempts[w] = Nil THEN TRUE ELSE attempts[w].phase \in
                          {"prepared", "pending", "applied", "conflict"}
    /\ timedOut \in BOOLEAN /\ lostReply \in BOOLEAN
    /\ lookupVerified \in BOOLEAN /\ lookupAccurate \in BOOLEAN
    /\ timeoutAccurate \in BOOLEAN /\ replayAccurate \in BOOLEAN

PublishedIndexIsExact == index = ledger
OneDecisionPerUuid == \A r \in Requests : Cardinality({x \in committed : x[1] = r}) <= 1
ExactLookup == lookupAccurate
TimeoutRemainsUnknown == timeoutAccurate
ReplayIsExact == replayAccurate

=============================================================================
