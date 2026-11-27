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
                     "staged-reset", "operator-reset", "skewed-failover", "frozen-move", "retired-replication"}

PolicyInit ==
    /\ Init
    /\ step = 0
    /\ (Scenario \in {"skewed-failover", "frozen-move"}) =>
        (coff = MaxDrift /\ off[w1] = -MaxDrift /\ off[w2] = MaxDrift
                         /\ off[w3] = MaxDrift)
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
    \/ /\ step = endSetup + 2
       /\ IF cs[Slot].ep = 2 THEN Heartbeat(w3, TRUE) ELSE UNCHANGED vars
       /\ Advance
    \/ /\ step = endSetup + 3
       /\ IF cs[Slot].ep = 2 THEN Write(c1, w1, Slot, Key, "set") ELSE UNCHANGED vars
       /\ Advance
    \/ /\ step \in (endSetup + 4)..(endSetup + 9)
       /\ Tick /\ Advance
    \/ /\ step = endSetup + 10
       /\ (StagedPromote(Slot) \/ StagedReset(Slot)) /\ Advance
    \/ EndAt(endSetup + 11)

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

\* A fast controller fences the old grant by its lower bound. The slow
\* predecessor and fast successor can both serve if authority ignores
\* uncertainty. The correct successor serves and the predecessor refuses.
SkewedFailoverCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> Heartbeat(w1, TRUE)
            [] step = 1 -> Replicate(w2, Slot)
            [] step = 2 -> Heartbeat(w2, TRUE)
            [] step = 3 -> Heartbeat(w3, TRUE)
            [] step = 4 -> CutCtrl(w1)
            [] step = 5 -> ReportAges(w1)
            [] step \in 6..8 -> Tick
            [] step = 9 -> Failover(Slot)
            [] step = 10 -> Heartbeat(w2, TRUE)
            [] step = 11 -> Write(c1, w2, Slot, Key, "set")
            [] step = 12 -> IF ENABLED Write(c2, w1, Slot, Key, "set")
                            THEN Write(c2, w1, Slot, Key, "set")
                            ELSE UNCHANGED vars
            [] OTHER -> FALSE
    \/ EndAt(13)

\* A planned move takes the old primary's frozen prefix while its grant
\* remains live in real time. The frozen actor must stop authorizing as
\* soon as it installs the freeze and stay frozen after target activation.
FrozenMoveCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> Heartbeat(w1, TRUE)
            [] step = 1 -> Heartbeat(w2, TRUE)
            [] step = 2 -> Heartbeat(w3, TRUE)
            [] step = 3 -> MoveStart(Slot) /\ cs'[Slot].mtgt = Me(w3)
            [] step = 4 -> Heartbeat(w1, TRUE)
            [] step = 5 -> Heartbeat(w3, TRUE)
            [] step = 6 -> Replicate(w3, Slot)
            [] step = 7 -> Heartbeat(w3, TRUE)
            [] step \in 8..9 -> Tick
            [] step = 10 -> MoveFreeze(Slot)
            [] step \in 11..12 -> Heartbeat(w1, TRUE)
            [] step = 13 -> MoveFrozen(Slot)
            [] step = 14 -> MoveCutover(Slot)
            [] step = 15 -> Heartbeat(w3, TRUE)
            [] OTHER -> FALSE
    \/ EndAt(16)

\* An assigned replica installs the retirement report, then pulls a write
\* acknowledged by the source under its still-active committed grant.
RetiredReplicationCheck ==
    \/ /\ Advance
       /\ CASE step = 0 -> Heartbeat(w1, TRUE)
            [] step = 1 -> Heartbeat(w2, TRUE)
            [] step = 2 -> Heartbeat(w3, TRUE)
            [] step = 3 -> Replicate(w2, Slot)
            [] step = 4 -> Tick
            [] step = 5 -> Heartbeat(w1, TRUE)
            [] step = 6 -> Replace(w1)
            [] step = 7 -> Heartbeat(w2, TRUE)
            [] step = 8 -> Write(c1, w1, Slot, Key, "set")
            [] step = 9 -> Replicate(w2, Slot)
            [] OTHER -> FALSE
    \/ EndAt(10)

PolicyNext ==
    CASE Scenario = "last-copy" -> LastCopyCheck
      [] Scenario \in {"transfer-at-limit", "transfer-over-limit"} -> TransferCheck
      [] Scenario = "replace-after-window" -> ReplacementCheck
      [] Scenario = "failover-after-window" -> FailoverCheck
      [] Scenario = "staged-reset" -> StagedResetCheck
      [] Scenario = "operator-reset" -> OperatorResetCheck
      [] Scenario = "skewed-failover" -> SkewedFailoverCheck
      [] Scenario = "frozen-move" -> FrozenMoveCheck
      [] Scenario = "retired-replication" -> RetiredReplicationCheck

=============================================================================
