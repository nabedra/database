-------------------------- MODULE OlapDecisionIndex ---------------------------
(* OLAP-1. Refines catalog publication with an immutable UUID index. A      *)
(* bounded page path is abstracted as an immutable finite map. The map and *)
(* decision reachability change together at the catalog Match. The map     *)
(* key is the complete UUID, including keys that share a page prefix.      *)
(*                                                                         *)
(* Ledger is the oracle, independent of the index. The legacy chain's      *)
(* logical contents are its latest published entries. An attempt slot      *)
(* models a prepared private decision and copied index path. It can survive *)
(* client timeout and the old writer's loss of admission.                   *)
(*                                                                         *)
(* A root transitions from legacy to fenced migration to indexed. A bump    *)
(* to the migration format makes even a legacy writer that ignores the     *)
(* maintenance switch reject the new root. Publication of partial private  *)
(* migration pages and a legacy writer accepting the new format are mutants. *)
(* This refinement checks one legacy and two indexed writer attempts, one   *)
(* prior UUID and two fresh UUIDs, a migration interruption, CAS races, an *)
(* ambiguous deadline and a lost reply. Row and reader behavior remains   *)
(* in OlapCommit. Bounds do not model byte-level encoding or actual S3 I/O. *)
EXTENDS Naturals, FiniteSets

CONSTANTS LegacyWriter, Writers, Old, FreshA, FreshB, Nil, Mutant

ASSUME /\ LegacyWriter \in Writers /\ Cardinality(Writers) = 3
       /\ Cardinality({Old, FreshA, FreshB}) = 3
       /\ Mutant \in {"none", "incomplete_migration", "ignore_match", "drop_index_update",
                      "old_accepts_new_root", "match_prefix", "missing_page_absent",
                      "timeout_final", "lost_reply_old_root", "no_migration"}

Requests == {Old, FreshA, FreshB}
Bodies == {"a", "b"}
Empty == [r \in Requests |-> Nil]
OldDecision == [req |-> Old, body |-> "a", result |-> <<LegacyWriter, 0>>]
Initial == [Empty EXCEPT ![Old] = OldDecision]

VARIABLES mode, revision, migrationStarted, ledger, committed,
          index, staging, interrupted, attempts, timedOut, lostReply,
          lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate

vars == <<mode, revision, migrationStarted, ledger, committed,
          index, staging, interrupted, attempts, timedOut, lostReply,
          lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate>>

Init ==
    /\ mode = "legacy" /\ revision = 1 /\ migrationStarted = FALSE
    /\ ledger = Initial /\ committed = {<<Old, OldDecision.result>>}
    /\ index = Empty /\ staging = Empty /\ interrupted = FALSE
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

Resolve(r, body, readable) ==
    IF mode # "indexed" THEN Answer(ledger[r], body)
    ELSE IF ~readable THEN
        IF Mutant = "missing_page_absent" THEN NoAnswer("absent") ELSE NoAnswer("unknown")
    ELSE LET actual == IF Mutant = "match_prefix" /\ r = FreshB THEN FreshA ELSE r
         IN Answer(index[actual], body)

\* The migration fence is a CAS on the captured legacy root: whichever of a
\* legacy attempt and the freeze wins first determines the frozen history.
Freeze ==
    /\ mode = "legacy"
    /\ mode' = "building" /\ revision' = revision + 1
    /\ migrationStarted' = TRUE
    /\ UNCHANGED <<ledger, committed, index, staging, interrupted, attempts,
                   timedOut, lostReply, lookupVerified, lookupAccurate,
                   timeoutAccurate, replayAccurate>>

BuildPage(r) ==
    /\ mode = "building" /\ ledger[r] # Nil /\ staging[r] = Nil
    /\ staging' = [staging EXCEPT ![r] = ledger[r]]
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   interrupted, attempts, timedOut, lostReply, lookupVerified,
                   lookupAccurate, timeoutAccurate, replayAccurate>>

InterruptBuild ==
    /\ mode = "building" /\ ~interrupted /\ staging # Empty
    /\ interrupted' = TRUE /\ staging' = Empty
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   attempts, timedOut, lostReply, lookupVerified, lookupAccurate,
                   timeoutAccurate, replayAccurate>>

PublishMigration ==
    /\ mode = "building" /\ Mutant # "no_migration"
    /\ staging = ledger \/ Mutant = "incomplete_migration"
    /\ mode' = "indexed" /\ revision' = revision + 1 /\ index' = staging
    /\ UNCHANGED <<migrationStarted, ledger, committed, staging, interrupted,
                   attempts, timedOut, lostReply, lookupVerified, lookupAccurate,
                   timeoutAccurate, replayAccurate>>

Prepare(w, r, body) ==
    /\ attempts[w] = Nil
    /\ IF w = LegacyWriter
       THEN mode = "legacy" \/ (mode = "indexed" /\ Mutant = "old_accepts_new_root")
       ELSE mode = "indexed"
    /\ Resolve(r, body, TRUE) = NoAnswer("absent")
    /\ LET decision == [req |-> r, body |-> body, result |-> <<w, revision>>]
       IN attempts' = [attempts EXCEPT ![w] =
            [base |-> revision, decision |-> decision, timedOut |-> FALSE,
             slot |-> [index EXCEPT ![r] = decision], phase |-> "prepared"]]
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, timedOut, lostReply, lookupVerified,
                   lookupAccurate, timeoutAccurate, replayAccurate>>

Submit(w) ==
    /\ attempts[w] # Nil /\ attempts[w].phase = "prepared"
    /\ attempts' = [attempts EXCEPT ![w].phase = "pending"]
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, timedOut, lostReply, lookupVerified,
                   lookupAccurate, timeoutAccurate, replayAccurate>>

Apply(w, afterTimeout) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "pending" /\ a.timedOut = afterTimeout
    /\ a.base = revision \/ Mutant = "ignore_match"
    \* A legacy writer serializes the old root shape. With format validation
    \* and the exact Match predicate, that shape cannot become current again.
    /\ mode' = IF w = LegacyWriter THEN "legacy" ELSE "indexed"
    /\ revision' = revision + 1
    /\ index' = CASE w = LegacyWriter -> Empty
                   [] Mutant = "drop_index_update" -> index
                   [] OTHER -> a.slot
    /\ ledger' = [ledger EXCEPT ![a.decision.req] = a.decision]
    /\ committed' = committed \cup {<<a.decision.req, a.decision.result>>}
    /\ attempts' = [attempts EXCEPT ![w].phase = "applied"]
    /\ UNCHANGED <<migrationStarted, staging, interrupted, timedOut, lostReply,
                   lookupVerified, lookupAccurate, timeoutAccurate, replayAccurate>>

Conflict(w) ==
    LET a == attempts[w] IN
    /\ a # Nil /\ a.phase = "pending" /\ a.base # revision
    \* A definite conflict changes no authoritative data. Recovery consults
    \* the latest root, including an exact outcome committed by the winner.
    /\ replayAccurate' = (replayAccurate /\ Resolve(a.decision.req, a.decision.body, TRUE) =
                          Answer(ledger[a.decision.req], a.decision.body))
    /\ attempts' = [attempts EXCEPT ![w].phase = "conflict"]
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, timedOut, lostReply, lookupVerified,
                   lookupAccurate, timeoutAccurate>>

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
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, lostReply, lookupVerified, lookupAccurate,
                   replayAccurate>>

LoseReplyAndRecover(w) ==
    LET a == attempts[w] IN
    /\ mode = "indexed" /\ a # Nil /\ a.phase = "applied" /\ ~lostReply
    /\ lostReply' = TRUE
    /\ replayAccurate' =
        (replayAccurate /\ (IF Mutant = "lost_reply_old_root" THEN NoAnswer("absent")
                            ELSE Resolve(a.decision.req, a.decision.body, TRUE)) = Found(a.decision))
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, attempts, timedOut, lookupVerified,
                   lookupAccurate, timeoutAccurate>>

ProbeLookup ==
    /\ mode = "indexed" /\ ~lookupVerified
    /\ lookupVerified' = TRUE
    /\ lookupAccurate' =
        \A r \in Requests, body \in Bodies :
            /\ Resolve(r, body, TRUE) = Answer(ledger[r], body)
            /\ Resolve(r, body, FALSE) = NoAnswer("unknown")
    /\ UNCHANGED <<mode, revision, migrationStarted, ledger, committed, index,
                   staging, interrupted, attempts, timedOut, lostReply,
                   timeoutAccurate, replayAccurate>>

PrepareAny == \E w \in Writers, r \in Requests, body \in Bodies : Prepare(w, r, body)
BuildPageAny == \E r \in Requests : BuildPage(r)
SubmitAny == \E w \in Writers : Submit(w)
ApplyOnTime == \E w \in Writers : Apply(w, FALSE)
ApplyLate == \E w \in Writers : Apply(w, TRUE)
ConflictAny == \E w \in Writers : Conflict(w)
DeadlineAny == \E w \in Writers : Deadline(w)
LostReplyAny == \E w \in Writers : LoseReplyAndRecover(w)

Done ==
    /\ mode = "indexed"
    /\ \A w \in Writers : IF attempts[w] = Nil THEN TRUE ELSE attempts[w].phase \in {"applied", "conflict"}
    /\ UNCHANGED vars

Next ==
    \/ Freeze \/ BuildPageAny \/ InterruptBuild \/ PublishMigration
    \/ PrepareAny \/ SubmitAny \/ ApplyOnTime \/ ApplyLate \/ ConflictAny
    \/ DeadlineAny \/ LostReplyAny \/ ProbeLookup \/ Done

Spec == Init /\ [][Next]_vars

\* The maintenance migrator resumes scanning, then retries the publication.
\* Object storage eventually evaluates each delivered conditional PUT.
LiveSpec ==
    /\ Spec
    /\ WF_vars(Freeze) /\ WF_vars(BuildPageAny) /\ WF_vars(PublishMigration)
    /\ \A w \in Writers : WF_vars(Apply(w, FALSE) \/ Apply(w, TRUE) \/ Conflict(w))

TypeOK ==
    /\ mode \in {"legacy", "building", "indexed"} /\ revision \in Nat
    /\ DOMAIN ledger = Requests /\ DOMAIN index = Requests /\ DOMAIN staging = Requests
    /\ DOMAIN attempts = Writers
    /\ \A w \in Writers : IF attempts[w] = Nil THEN TRUE ELSE attempts[w].phase \in
                          {"prepared", "pending", "applied", "conflict"}
    /\ migrationStarted \in BOOLEAN /\ interrupted \in BOOLEAN
    /\ timedOut \in BOOLEAN /\ lostReply \in BOOLEAN
    /\ lookupVerified \in BOOLEAN /\ lookupAccurate \in BOOLEAN
    /\ timeoutAccurate \in BOOLEAN /\ replayAccurate \in BOOLEAN

PublishedIndexIsExact == mode = "indexed" => index = ledger
LegacyWritersFenced == migrationStarted => mode # "legacy"
OneDecisionPerUuid == \A r \in Requests : Cardinality({x \in committed : x[1] = r}) <= 1
ExactLookup == lookupAccurate
TimeoutRemainsUnknown == timeoutAccurate
ReplayIsExact == replayAccurate
MigrationCompletes == <>[](mode = "indexed")

=============================================================================
