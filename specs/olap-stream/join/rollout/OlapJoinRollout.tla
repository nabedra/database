---------------------------- MODULE OlapJoinRollout ----------------------------
(* One controller owner, one coordinator, one ready peer, and one joiner.
   The seal and delayed Match behavior after activation refines OlapJoinFence.
   The maintenance window stops pre-upgrade admissions and fences legacy
   controller processes. The coordinator's current copies remain recoverable. *)
EXTENDS Naturals

CONSTANT Mutant

ASSUME Mutant \in {"none", "legacy_fallback", "reuse_activation_base", "old_owner_returns", "no_upgrade"}

VARIABLES revision, active, oldController, visitedOld, capable,
          ready, copied, acknowledged, seal, guarded, pending, expired, usedDeadline

vars == <<revision, active, oldController, visitedOld, capable,
          ready, copied, acknowledged, seal, guarded, pending, expired, usedDeadline>>

Init ==
    /\ revision = 1 /\ active = FALSE /\ oldController = TRUE
    /\ visitedOld = FALSE /\ capable = FALSE /\ ready = FALSE
    /\ copied = 0 /\ acknowledged = 0 /\ seal = 0 /\ guarded = FALSE
    /\ pending = 0 /\ expired = FALSE /\ usedDeadline = FALSE

\* A rolling upgrade hands coordination to an updated ready incarnation
\* using the existing stream and queue recovery protocol.
UpgradeCoordinator ==
    /\ ~capable /\ Mutant # "no_upgrade" /\ capable' = TRUE
    /\ UNCHANGED <<revision, active, oldController, visitedOld, ready, copied,
                   acknowledged, seal, guarded, pending, expired, usedDeadline>>

StopOldController ==
    /\ oldController /\ oldController' = FALSE
    /\ UNCHANGED <<revision, active, visitedOld, capable, ready, copied,
                   acknowledged, seal, guarded, pending, expired, usedDeadline>>

ReturnOldController ==
    /\ ~oldController /\ ~visitedOld /\ (~active \/ Mutant = "old_owner_returns")
    /\ oldController' = TRUE /\ visitedOld' = TRUE
    /\ UNCHANGED <<revision, active, capable, ready, copied, acknowledged,
                   seal, guarded, pending, expired, usedDeadline>>

RepairLegacyCopies ==
    /\ ~active /\ ~oldController /\ ready /\ copied < acknowledged
    /\ copied' = acknowledged
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   acknowledged, seal, guarded, pending, expired, usedDeadline>>

\* The new controller ownership CAS is the activation fence. A late legacy
\* CAS can win before this boundary. If so, verify and repair its copies
\* under quiescence before serving. A CAS based on the old token loses after it.
Activate ==
    /\ ~active /\ ~oldController /\ (ready => copied >= acknowledged)
    /\ active' = TRUE
    /\ revision' = IF Mutant = "reuse_activation_base" THEN revision ELSE revision + 1
    /\ seal' = 0 /\ guarded' = FALSE /\ expired' = FALSE
    /\ UNCHANGED <<oldController, visitedOld, capable, ready, copied,
                   acknowledged, pending, usedDeadline>>

Join ==
    /\ ~ready /\ seal = 0 /\ pending = 0
    /\ oldController \/ (active /\ (capable \/ Mutant = "legacy_fallback"))
    /\ seal' = revision /\ copied' = acknowledged
    /\ guarded' = (active /\ capable /\ ~oldController)
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   acknowledged, pending, expired, usedDeadline>>

SubmitReady ==
    /\ (oldController \/ active) /\ seal = revision /\ pending = 0 /\ ~expired
    /\ pending' = revision
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   copied, acknowledged, seal, guarded, expired, usedDeadline>>

ApplyReady ==
    /\ pending = revision
    /\ ready' = TRUE /\ pending' = 0 /\ revision' = revision + 1
    /\ UNCHANGED <<active, oldController, visitedOld, capable, copied,
                   acknowledged, seal, guarded, expired, usedDeadline>>

RejectStale ==
    /\ pending # 0 /\ pending < revision
    /\ pending' = 0
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   copied, acknowledged, seal, guarded, expired, usedDeadline>>

Deadline ==
    /\ seal = revision /\ ~usedDeadline
    /\ usedDeadline' = TRUE
    /\ IF guarded THEN /\ expired' = TRUE /\ UNCHANGED <<seal, guarded>>
                  ELSE /\ seal' = 0 /\ expired' = FALSE /\ guarded' = FALSE
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   copied, acknowledged, pending>>

FenceAbort ==
    /\ active /\ expired /\ seal = revision
    /\ revision' = revision + 1
    /\ UNCHANGED <<active, oldController, visitedOld, capable, ready, copied,
                   acknowledged, seal, guarded, pending, expired, usedDeadline>>

ResolveSeal ==
    /\ seal # 0 /\ seal < revision
    /\ seal' = 0 /\ guarded' = FALSE /\ expired' = FALSE
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   copied, acknowledged, pending, usedDeadline>>

Acknowledge ==
    /\ (oldController \/ active) /\ acknowledged < 2 /\ seal = 0
    /\ acknowledged' = acknowledged + 1
    \* Ordinary traffic cannot recopy the previously acknowledged prefix.
    /\ copied' = IF ready /\ copied = acknowledged THEN acknowledged' ELSE copied
    /\ UNCHANGED <<revision, active, oldController, visitedOld, capable, ready,
                   seal, guarded, pending, expired, usedDeadline>>

Done ==
    /\ active /\ ready /\ seal = 0 /\ pending = 0
    /\ UNCHANGED vars

Next ==
    \/ UpgradeCoordinator \/ StopOldController \/ ReturnOldController
    \/ RepairLegacyCopies \/ Activate \/ Join \/ SubmitReady \/ ApplyReady
    \/ RejectStale \/ Deadline \/ FenceAbort \/ ResolveSeal \/ Acknowledge \/ Done

Spec == Init /\ [][Next]_vars

(* The operator retires the old controller and upgrades coordination. The
   reconciler finishes joins and conditionals after the last modeled change. *)
LiveSpec ==
    /\ Spec
    /\ WF_vars(StopOldController) /\ WF_vars(UpgradeCoordinator)
    /\ WF_vars(RepairLegacyCopies) /\ WF_vars(Activate)
    /\ WF_vars(Join) /\ WF_vars(SubmitReady) /\ WF_vars(ApplyReady)
    /\ WF_vars(RejectStale) /\ WF_vars(FenceAbort) /\ WF_vars(ResolveSeal)

TypeOK ==
    /\ revision \in Nat /\ revision > 0
    /\ active \in BOOLEAN /\ oldController \in BOOLEAN /\ visitedOld \in BOOLEAN
    /\ capable \in BOOLEAN /\ ready \in BOOLEAN /\ guarded \in BOOLEAN
    /\ copied \in 0..2 /\ acknowledged \in 0..2 /\ copied <= acknowledged
    /\ seal \in 0..revision /\ pending \in 0..revision
    /\ expired \in BOOLEAN /\ usedDeadline \in BOOLEAN

ActiveCopiesSafe == active /\ ready => copied >= acknowledged
ActiveOwnerIsCurrent == active => ~oldController
RolloutProgress == <>[](active /\ ready /\ pending = 0 /\ seal = 0)

=============================================================================
