------------------------- MODULE EventsTimeRollout -------------------------
(* An already replicated partition changes from legacy to guarded time.  *)
(* EventsTime checks the RF3 sample arithmetic. Here a quorum round      *)
(* supplies the correct interval, and the log orders transition and     *)
(* commands. Member incarnation represents the voter identity set.     *)
EXTENDS Integers, FiniteSets

CONSTANTS MaxTime, Freshness, ClockError, Retention, MaxTtl, LegacyFast, Mutant

NoRound == [at |-> -1, term |-> 0, incarnation |-> 0, mode |-> "none"]
NoCommand == [kind |-> "none", lower |-> 0, upper |-> 0]
NoEvent == [present |-> FALSE, stamp |-> 0]
LeaseKinds == {"legacy", "guarded"}
CommandKinds == {"publish", "claim", "retain"}

VARIABLES
    now, term, incarnation, mode, round,
    oldPending, oldSubmitted,
    activationUpper, leaseClock, cutoff,
    pending, legacyEvent, guardedEvent, lease,
    unguardedSubmission, legacyAfterFence, earlyExpiry, earlyRemoval

authority == <<term, incarnation>>
transition == <<mode, activationUpper>>
legacyQueue == <<oldPending, oldSubmitted>>
watermarks == <<leaseClock, cutoff>>
records == <<legacyEvent, guardedEvent, lease>>
violations == <<unguardedSubmission, legacyAfterFence, earlyExpiry, earlyRemoval>>
vars == <<now, authority, transition, round, legacyQueue, watermarks,
          pending, records, violations>>

Max(S) == CHOOSE x \in S : \A y \in S : x >= y

Init ==
    /\ now = 0 /\ term = 1 /\ incarnation = 1
    /\ mode = "legacy" /\ round = NoRound
    /\ oldPending = "none" /\ oldSubmitted = FALSE
    /\ activationUpper = 0 /\ leaseClock = -1 /\ cutoff = -1 - Retention
    /\ pending = NoCommand
    /\ legacyEvent = [present |-> TRUE, stamp |-> -Retention]
    /\ guardedEvent = NoEvent
    /\ lease = [kind |-> "legacy", generation |-> 1, deadline |-> LegacyFast]
    /\ unguardedSubmission = FALSE /\ legacyAfterFence = FALSE
    /\ earlyExpiry = FALSE /\ earlyRemoval = FALSE

Tick ==
    /\ now < MaxTime /\ now' = now + 1
    /\ UNCHANGED <<authority, transition, round, legacyQueue, watermarks,
                   pending, records, violations>>

ChangeTerm ==
    /\ term = 1 /\ term' = 2
    /\ UNCHANGED <<now, incarnation, transition, round, legacyQueue,
                   watermarks, pending, records, violations>>

ReplaceVoter ==
    /\ incarnation = 1 /\ incarnation' = 2
    /\ UNCHANGED <<now, term, transition, round, legacyQueue, watermarks,
                   pending, records, violations>>

\* A queued legacy publish, lease edit or retain can outlive the client
\* and the transition. One such command is enough to exercise the fence.
QueueLegacy(kind) ==
    /\ mode = "legacy" /\ ~oldSubmitted
    /\ oldPending' = kind /\ oldSubmitted' = TRUE
    /\ UNCHANGED <<now, authority, transition, round, watermarks,
                   pending, records, violations>>

ApplyLegacy ==
    LET allowed == mode = "legacy" \/ Mutant = "allow_legacy_after_fence"
    IN
    /\ oldPending # "none"
    /\ oldPending' = "none"
    /\ legacyEvent' = IF allowed /\ oldPending = "publish"
                      THEN [present |-> TRUE, stamp |-> -Retention]
                      ELSE legacyEvent
    /\ lease' = IF allowed /\ oldPending = "claim"
                THEN [lease EXCEPT !.generation = @ + 1]
                ELSE lease
    /\ cutoff' = IF allowed /\ oldPending = "retain" THEN LegacyFast ELSE cutoff
    /\ legacyAfterFence' = (legacyAfterFence \/ (allowed /\ mode # "legacy"))
    /\ UNCHANGED <<now, authority, transition, round, oldSubmitted, leaseClock,
                   pending, guardedEvent, unguardedSubmission, earlyExpiry, earlyRemoval>>

\* The applied fence stops the raw clock path. The activation round is
\* taken only after observing this fence, so a lagging legacy apply
\* immediately before it is covered by the upper time used for the holds.
FenceLegacy ==
    /\ mode = "legacy" /\ mode' = "fenced"
    /\ UNCHANGED <<now, authority, activationUpper, round, legacyQueue,
                   watermarks, pending, records, violations>>

Sample ==
    LET sample == [at |-> now, term |-> term, incarnation |-> incarnation, mode |-> mode]
    IN
    /\ round # sample /\ round' = sample
    /\ UNCHANGED <<now, authority, transition, legacyQueue, watermarks,
                   pending, records, violations>>

Fresh == now - round.at <= Freshness
CurrentScope(expected) ==
    /\ round.at >= 0 /\ Fresh
    /\ round.term = term /\ round.incarnation = incarnation
    /\ round.mode = expected

Usable(expected) ==
    /\ round.at >= 0
    /\ Fresh \/ Mutant = "use_stale_round"
    /\ round.term = term \/ Mutant = "use_old_term"
    /\ round.incarnation = incarnation \/ Mutant = "use_old_incarnation"
    /\ round.mode = expected \/ Mutant = "use_prefence_round"

\* Let elapsed be an upper bound measured at submission, including
\* suspension. The honest lower endpoint is safe even when it ages.
Lower == round.at - ClockError
Upper == round.at + ClockError + (now - round.at)

Activate ==
    /\ mode = "fenced" /\ Usable("fenced")
    /\ mode' = "guarded" /\ activationUpper' = Upper
    /\ leaseClock' = IF Mutant = "import_legacy_lease_clock" THEN LegacyFast ELSE Lower
    /\ cutoff' = IF Mutant = "import_legacy_cutoff" THEN LegacyFast ELSE Lower - Retention
    /\ unguardedSubmission' = (unguardedSubmission \/ ~CurrentScope("fenced"))
    /\ UNCHANGED <<now, authority, round, legacyQueue, pending, records,
                   legacyAfterFence, earlyExpiry, earlyRemoval>>

Submit(kind) ==
    /\ mode = "guarded" /\ pending = NoCommand /\ Usable("guarded")
    /\ kind = "publish" => ~guardedEvent.present
    /\ pending' = [kind |-> kind, lower |-> Lower, upper |-> Upper]
    /\ unguardedSubmission' = (unguardedSubmission \/ ~CurrentScope("guarded"))
    /\ UNCHANGED <<now, authority, transition, round, legacyQueue, watermarks,
                   records, legacyAfterFence, earlyExpiry, earlyRemoval>>

LegacyLeaseHold == activationUpper + MaxTtl
LegacyEventHold == activationUpper + Retention

\* Each command receives its proposal-time interval once. Apply can lag
\* or occur after a leader change. Replicas use the encoded times and
\* never resample in apply. Holds use the post-fence activation upper.
\* The first guarded claim retires the legacy generation atomically,
\* after this drain, even when the legacy deadline was far in the future.
Apply ==
    LET leaseNow == Max({leaseClock, pending.lower})
        retentionCutoff == Max({cutoff, pending.lower - Retention})
        currentDeadline == IF lease.kind # "legacy" THEN lease.deadline
                           ELSE LegacyLeaseHold
        newLeaseAllowed ==
            \/ leaseNow >= currentDeadline
            \/ lease.kind = "legacy" /\ Mutant = "skip_legacy_lease_hold"
        removeLegacy ==
            /\ legacyEvent.present /\ legacyEvent.stamp < retentionCutoff
            /\ Mutant = "skip_legacy_event_hold" \/ pending.lower >= LegacyEventHold
        removeGuarded ==
            /\ ~legacyEvent.present \/ removeLegacy
            /\ guardedEvent.present /\ guardedEvent.stamp < retentionCutoff
    IN
    /\ pending # NoCommand /\ pending' = NoCommand
    /\ CASE pending.kind = "publish" ->
         /\ guardedEvent' = [present |-> TRUE, stamp |-> Max({pending.upper, legacyEvent.stamp})]
         /\ UNCHANGED <<legacyEvent, lease, watermarks, earlyExpiry, earlyRemoval>>
       [] pending.kind = "claim" ->
         /\ leaseClock' = leaseNow
         /\ lease' =
             IF newLeaseAllowed
             THEN [kind |-> "guarded", generation |-> lease.generation + 1,
                   deadline |-> pending.upper + MaxTtl]
             ELSE lease
         /\ earlyExpiry' = (earlyExpiry \/ (newLeaseAllowed /\ now < currentDeadline))
         /\ UNCHANGED <<cutoff, legacyEvent, guardedEvent, earlyRemoval>>
       [] OTHER ->
         /\ cutoff' = retentionCutoff
         /\ legacyEvent' = [legacyEvent EXCEPT !.present = @ /\ ~removeLegacy]
         /\ guardedEvent' = [guardedEvent EXCEPT !.present = @ /\ ~removeGuarded]
         /\ earlyRemoval' =
             (earlyRemoval \/ (removeLegacy /\ now < LegacyEventHold)
                           \/ (removeGuarded /\ now < guardedEvent.stamp + Retention))
         /\ UNCHANGED <<leaseClock, lease, earlyExpiry>>
    /\ UNCHANGED <<now, authority, transition, round, legacyQueue,
                   unguardedSubmission, legacyAfterFence>>

Done == now = MaxTime /\ UNCHANGED vars

Next ==
    \/ Tick \/ ChangeTerm \/ ReplaceVoter
    \/ \E kind \in CommandKinds : QueueLegacy(kind)
    \/ ApplyLegacy \/ FenceLegacy \/ Sample \/ Activate
    \/ \E kind \in CommandKinds : Submit(kind)
    \/ Apply \/ Done

TypeOK ==
    /\ now \in 0..MaxTime /\ term \in 1..2 /\ incarnation \in 1..2
    /\ mode \in {"legacy", "fenced", "guarded"}
    /\ oldPending \in CommandKinds \cup {"none"}
    /\ pending.kind \in CommandKinds \cup {"none"}
    /\ lease.kind \in LeaseKinds /\ lease.generation \in Nat
    /\ legacyEvent.present \in BOOLEAN /\ guardedEvent.present \in BOOLEAN

G06_FreshSubmission == ~unguardedSubmission
G08_NoLegacyAfterFence == ~legacyAfterFence
G05_LegacyLeaseDrains == ~earlyExpiry
G08_LegacyRetentionHolds == ~earlyRemoval
=========================================================================
