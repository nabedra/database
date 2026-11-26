-------------------------- MODULE CacheRecoveryPolicy --------------------------
EXTENDS CacheCluster

CONSTANTS w1, w2, w3, c1, c2, Scenario
VARIABLE step

OnePlace == {<<w1, w2>>}
Slot == CHOOSE s \in Slots : TRUE
Key == CHOOSE k \in Keys : TRUE
PolicyVars == <<vars, step>>

ASSUME Scenario \in {"last-copy", "transfer-at-limit", "transfer-over-limit",
                     "replace-after-window", "failover-after-window",
                     "staged-reset", "operator-reset"}

PolicyInit == Init /\ step = 0
Advance == step' = step + 1
TryStageStart == IF ENABLED StageStart(Slot) THEN StageStart(Slot) ELSE UNCHANGED vars
TryStagedPromote == IF ENABLED StagedPromote(Slot) THEN StagedPromote(Slot) ELSE UNCHANGED vars
TryStageComplete == IF ENABLED StageComplete(Slot) THEN StageComplete(Slot) ELSE UNCHANGED vars
TryFailover == IF ENABLED Failover(Slot) THEN Failover(Slot) ELSE UNCHANGED vars
EndAt(last) == step = last /\ UNCHANGED PolicyVars
SameClockNewTerm == LeaderChange /\ coff' = 0

\* Install a replica-only staging target with a verified empty snapshot,
\* after the assigned replica crashes. Then let the target fall behind the
\* source by 32 or 33 real mutation sequence numbers.
WritesAtCutover == IF Scenario = "transfer-at-limit" THEN MaxLag ELSE MaxLag + 1
StagedPrefix ==
    CASE step = 0 -> Crash(w2)
      [] step = 1 -> Heartbeat(w1, TRUE)
      [] step = 2 -> Heartbeat(w3, TRUE)
      [] step \in 3..5 -> Tick
      [] step = 6 -> StageStart(Slot)
      [] step = 7 -> Heartbeat(w1, TRUE)
      [] step = 8 -> Heartbeat(w3, TRUE)
      [] step = 9 -> Replicate(w3, Slot)
      [] step = 10 -> Heartbeat(w3, TRUE)
      [] step \in 11..(10 + WritesAtCutover) ->
             LET client == IF step % 2 = 0 THEN c1 ELSE c2
             IN Write(client, w1, Slot, Key, "set")
      [] step = 11 + WritesAtCutover -> Heartbeat(w1, TRUE)

\* Ordinary transfers can complete at the limit. A transition at 33 would
\* violate D6_OrdinaryTransferLag. Post-check stuttering is intentional.
TransferCheck ==
    LET endSetup == 12 + WritesAtCutover IN
    \/ /\ step < endSetup
       /\ StagedPrefix /\ Advance
    \/ /\ step = endSetup
       /\ IF Scenario = "transfer-at-limit"
          THEN StageComplete(Slot)
          ELSE TryStageComplete
       /\ Advance
    \/ EndAt(endSetup + 1)

\* After retirement, run to the reset deadline. An over-limit but verified
\* staged copy must be kept. Restoring the old lag gate would cold-reset it.
LastCopyCheck ==
    LET endSetup == 12 + WritesAtCutover IN
    \/ /\ step < endSetup
       /\ StagedPrefix /\ Advance
    \/ /\ step = endSetup
       /\ Replace(w1) /\ Advance
    \/ /\ step = endSetup + 1
       /\ TryStagedPromote /\ Advance
    \/ /\ step \in (endSetup + 2)..(endSetup + 7)
       /\ Tick /\ Advance
    \/ /\ step = endSetup + 8
       /\ (StagedPromote(Slot) \/ StagedReset(Slot)) /\ Advance
    \/ EndAt(endSetup + 9)

\* A replica is disconnected as a term starts, then reports late after heal.
\* The controller must wait during the window and keep the observed replica
\* once it reports. Requiring guards here runs the actual StageStart action.
ReplacementCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> CutCtrl(w2)
                  [] step = 1 -> SameClockNewTerm
                  [] step = 2 -> Heartbeat(w1, TRUE)
                  [] step = 3 -> Heartbeat(w3, TRUE)
                  [] step = 4 -> TryStageStart
                  [] step \in 5..6 -> Tick
                  [] step = 7 -> HealCtrl(w2)
                  [] step = 8 -> Heartbeat(w2, TRUE) \/ UNCHANGED vars
                  [] step = 9 -> Tick
                  [] step = 10 -> IF Live(Me(w2))
                                THEN ~ENABLED StageStart(Slot) /\ UNCHANGED vars
                                ELSE StageStart(Slot)
                  [] OTHER -> FALSE
    \/ EndAt(11)

\* Set up a verified assigned replica and let the primary grant expire.
\* After a term change, stale reports may not stand in for new observations.
FailoverCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> Heartbeat(w1, TRUE)
                  [] step = 1 -> Replicate(w2, Slot)
                  [] step = 2 -> Heartbeat(w2, TRUE)
                  [] step = 3 -> Heartbeat(w3, TRUE)
                  [] step \in 4..6 -> Tick
                  [] step = 7 -> SameClockNewTerm
                  [] step = 8 -> Heartbeat(w2, TRUE)
                  [] step = 9 -> TryFailover
                  [] step = 10 -> Heartbeat(w1, TRUE)
                  [] step = 11 -> ~ENABLED Failover(Slot) /\ UNCHANGED vars
                  [] OTHER -> FALSE
    \/ EndAt(12)

\* The staged target has not installed a baseline. Recovery waits for the
\* grace interval and can then reset exactly this slot.
StagedResetCheck ==
    \/ /\ Advance
       /\ CASE step \in 0..6 -> StagedPrefix
            [] step = 7 -> Heartbeat(w3, TRUE)
            [] step = 8 -> Replace(w1)
            [] step \in 9..10 -> Tick
            [] step = 11 -> ~ENABLED StagedReset(Slot) /\ UNCHANGED vars
            [] step = 12 -> Tick
            [] step = 13 -> StagedReset(Slot)
            [] OTHER -> FALSE
    \/ EndAt(14)

\* Losing both copies of an unstaged slot leaves automatic recovery
\* disabled. Exercise the separate, scoped operator action.
OperatorResetCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> Crash(w2)
            [] step = 1 -> Replace(w1)
            [] step = 2 -> Heartbeat(w3, TRUE)
            [] step \in 3..5 -> Tick
            [] step = 6 -> ~ENABLED StagedReset(Slot) /\ UNCHANGED vars
            [] step = 7 -> AdminColdReset({Slot}, FALSE)
            [] OTHER -> FALSE
    \/ EndAt(8)

PolicyNext ==
    CASE Scenario = "last-copy" -> LastCopyCheck
      [] Scenario \in {"transfer-at-limit", "transfer-over-limit"} -> TransferCheck
      [] Scenario = "replace-after-window" -> ReplacementCheck
      [] Scenario = "failover-after-window" -> FailoverCheck
      [] Scenario = "staged-reset" -> StagedResetCheck
      [] Scenario = "operator-reset" -> OperatorResetCheck

=============================================================================
