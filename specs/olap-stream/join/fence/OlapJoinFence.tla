---------------------------- MODULE OlapJoinFence ----------------------------
(* Refines the readiness publication of OlapStreamJoin. A delayed storage   *)
(* write can apply after its controller or transport deadline has expired.  *)
(* The join seal is bound to the control-root revision and held until the   *)
(* scheduler observes a successor. A competing controller CAS fences any   *)
(* pending write based on the old root. The scheduler refreshes the new     *)
(* ready set before it resumes work.                                       *)
(*                                                                         *)
(* The revision represents a nonrepeating object-store Match token. One     *)
(* join is active at a time. Commit models an all-ready acknowledgement,    *)
(* for both stream replies and accepted requests. The other join and data   *)
(* models cover message faults, partial copies and full-cluster cold start. *)
(* This model checks one worker loss, one controller loss, one deadline,    *)
(* and overlaps between them. A stopped worker is a dead incarnation and   *)
(* cannot restart in the same control view. A normal publication need not   *)
(* give the controller a reply: the scheduler always resolves from the     *)
(* next root. Worker loss and controller takeover are modeled as subsequent *)
(* writes to the same root.                                                *)
EXTENDS Naturals, FiniteSets

CONSTANTS Workers, Joiner, FirstOwner, Stream, Request, MaxVersion, Mutant

ASSUME /\ Joiner \in Workers /\ FirstOwner \in Workers \ {Joiner}
       /\ Cardinality(Workers) = 3
       /\ Stream # Request
       /\ MaxVersion \in Nat /\ MaxVersion > 0
       /\ Mutant \in {"none", "release_on_deadline", "no_abort"}

VARIABLES revision, ready, alive, owner, controllerUp, copy, confirmed,
          sealBase, pendingBase, expired, usedDeadline, usedWorkerCrash,
          usedControllerCrash

vars == <<revision, ready, alive, owner, controllerUp, copy, confirmed,
          sealBase, pendingBase, expired, usedDeadline, usedWorkerCrash,
          usedControllerCrash>>

Kinds == {Stream, Request}
Zero == [key \in Kinds |-> 0]
NoBase == 0

Init ==
    /\ revision = 1 /\ ready = Workers \ {Joiner}
    /\ alive = Workers /\ owner = FirstOwner /\ controllerUp = TRUE
    /\ copy = [n \in Workers |-> Zero] /\ confirmed = Zero
    /\ sealBase = NoBase /\ pendingBase = NoBase /\ expired = FALSE
    /\ usedDeadline = FALSE /\ usedWorkerCrash = FALSE /\ usedControllerCrash = FALSE

JoinCopy ==
    /\ controllerUp /\ Joiner \in alive \ ready /\ ready \subseteq alive
    /\ sealBase = NoBase /\ pendingBase = NoBase
    /\ copy' = [copy EXCEPT ![Joiner] = copy[owner]]
    /\ sealBase' = revision
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, confirmed,
                   pendingBase, expired, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

SubmitReady ==
    /\ controllerUp /\ sealBase = revision /\ pendingBase = NoBase
    \* A timed-out controller stops issuing new writes, but cannot revoke one
    \* already submitted. The mutant models the transport still applying it.
    /\ ~expired /\ Joiner \in alive
    /\ pendingBase' = sealBase
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, copy, confirmed,
                   sealBase, expired, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

ApplyReady ==
    /\ pendingBase = revision
    \* Storage checks its Match token. Neither the current controller nor the
    \* scheduler's seal map is consulted when a delayed request reaches storage.
    /\ revision' = revision + 1 /\ ready' = ready \cup {Joiner}
    /\ pendingBase' = NoBase
    /\ UNCHANGED <<alive, owner, controllerUp, copy, confirmed, sealBase,
                   expired, usedDeadline, usedWorkerCrash, usedControllerCrash>>

RejectStale ==
    /\ pendingBase # NoBase /\ pendingBase < revision
    /\ pendingBase' = NoBase
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, copy, confirmed,
                   sealBase, expired, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

Deadline ==
    /\ sealBase = revision /\ ~usedDeadline
    /\ usedDeadline' = TRUE /\ expired' = TRUE
    /\ sealBase' = IF Mutant = "release_on_deadline" /\ pendingBase = revision
                   THEN NoBase ELSE sealBase
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, copy, confirmed,
                   pendingBase, usedWorkerCrash, usedControllerCrash>>

FenceAbort ==
    /\ controllerUp /\ expired /\ sealBase = revision
    /\ Mutant # "no_abort"
    \* The same Match token guards this renewal/abort. If Ready won first,
    \* this action is disabled and resolution uses the published ready set.
    /\ revision' = revision + 1
    /\ UNCHANGED <<ready, alive, owner, controllerUp, copy, confirmed, sealBase,
                   pendingBase, expired, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

ResolveSeal ==
    /\ sealBase # NoBase /\ sealBase < revision
    \* A fresh control read proved this seal's captured CAS cannot still win.
    \* The refreshed ready set is already represented by ready.
    /\ sealBase' = NoBase /\ expired' = FALSE
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, copy, confirmed,
                   pendingBase, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

Commit(key) ==
    /\ sealBase = NoBase /\ ready \subseteq alive /\ owner \in ready
    /\ confirmed[key] < MaxVersion
    /\ confirmed' = [confirmed EXCEPT ![key] = @ + 1]
    /\ copy' = [n \in Workers |-> IF n \in ready
                                 THEN [copy[n] EXCEPT ![key] = confirmed'[key]]
                                 ELSE copy[n]]
    /\ UNCHANGED <<revision, ready, alive, owner, controllerUp, sealBase,
                   pendingBase, expired, usedDeadline, usedWorkerCrash,
                   usedControllerCrash>>

CrashWorker(n) ==
    /\ ~usedWorkerCrash /\ n \in alive
    /\ alive' = alive \ {n} /\ usedWorkerCrash' = TRUE
    /\ copy' = [copy EXCEPT ![n] = Zero]
    \* No surviving process can serve as this stopped incarnation. Any owner
    \* reassignment first changes the root, invalidating old Match tokens.
    /\ UNCHANGED <<revision, ready, owner, controllerUp, confirmed, sealBase,
                   pendingBase, expired, usedDeadline, usedControllerCrash>>

ObserveLoss ==
    /\ controllerUp
    /\ \/ ready \ alive # {}
       \/ sealBase = revision /\ Joiner \notin alive
    /\ revision' = revision + 1 /\ ready' = ready \cap alive
    /\ owner' \in (IF owner \in ready' THEN {owner} ELSE ready')
    /\ UNCHANGED <<alive, controllerUp, copy, confirmed, sealBase, pendingBase,
                   expired, usedDeadline, usedWorkerCrash, usedControllerCrash>>

CrashController ==
    /\ controllerUp /\ ~usedControllerCrash
    /\ controllerUp' = FALSE /\ usedControllerCrash' = TRUE
    /\ UNCHANGED <<revision, ready, alive, owner, copy, confirmed, sealBase,
                   pendingBase, expired, usedDeadline, usedWorkerCrash>>

TakeoverController ==
    /\ ~controllerUp
    /\ controllerUp' = TRUE /\ revision' = revision + 1
    /\ UNCHANGED <<ready, alive, owner, copy, confirmed, sealBase, pendingBase,
                   expired, usedDeadline, usedWorkerCrash, usedControllerCrash>>

CommitAny == \E key \in Kinds : Commit(key)
CrashWorkerAny == \E n \in Workers : CrashWorker(n)

Done ==
    /\ Joiner \in ready \/ Joiner \notin alive
    /\ sealBase = NoBase /\ pendingBase = NoBase
    /\ UNCHANGED vars

Next ==
    \/ JoinCopy \/ SubmitReady \/ ApplyReady \/ RejectStale
    \/ Deadline \/ FenceAbort \/ ResolveSeal \/ CommitAny
    \/ CrashWorkerAny \/ ObserveLoss \/ CrashController \/ TakeoverController
    \/ Done

Spec == Init /\ [][Next]_vars

(* Provider retries the join; S3 resolves a delivered CAS; the controller
   drives expiry with a renewal/abort, observes worker loss, and is replaced
   after a lost lease. The scheduler re-reads the root before unsealing.
   The fault actions themselves carry no fairness. *)
LiveSpec ==
    /\ Spec
    /\ WF_vars(JoinCopy) /\ WF_vars(SubmitReady) /\ WF_vars(ApplyReady)
    /\ WF_vars(RejectStale) /\ WF_vars(FenceAbort) /\ WF_vars(ResolveSeal)
    /\ WF_vars(ObserveLoss) /\ WF_vars(TakeoverController)

TypeOK ==
    /\ revision \in Nat /\ revision > 0
    /\ sealBase \in 0..revision /\ pendingBase \in 0..revision
    /\ ready \subseteq Workers /\ alive \subseteq Workers /\ owner \in ready
    /\ copy \in [Workers -> [Kinds -> 0..MaxVersion]]
    /\ confirmed \in [Kinds -> 0..MaxVersion]
    /\ controllerUp \in BOOLEAN /\ expired \in BOOLEAN
    /\ usedDeadline \in BOOLEAN /\ usedWorkerCrash \in BOOLEAN
    /\ usedControllerCrash \in BOOLEAN

G04_ReadyHoldsReplied == \A n \in ready \cap alive : copy[n][Stream] >= confirmed[Stream]
G07_ReadyHoldsAccepted == \A n \in ready \cap alive : copy[n][Request] >= confirmed[Request]
ReadyMatchStaysSealed == pendingBase = revision => sealBase = revision

JoinResolves ==
    <>[](sealBase = NoBase /\ pendingBase = NoBase /\ (Joiner \in alive => Joiner \in ready))

=============================================================================
