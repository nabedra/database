------------------------- MODULE TwoPhaseTrace -------------------------
(***************************************************************************)
(* Trace validation for TwoPhase. Each step of a nabedra.trace/1 trace    *)
(* must be a TwoPhase action, with the logged values as constraints on    *)
(* the next state. RM IDs are strings, and the TM logs as node "tm".      *)
(*                                                                         *)
(* Variables a trace does not constrain:                                  *)
(* - msgs: the channel. Lines describe the message sent or received in   *)
(*   "msg", and the action takes it from there;                          *)
(* - crashes, drops: fault counters, which the harness does not log.     *)
(***************************************************************************)
EXTENDS TwoPhase, NabedraTrace

ASSUME TraceHeaderOK("TwoPhase")

TraceRM == ToSet(TraceParams.RM)
TraceMaxCrashes == TraceParams.MaxCrashes
TraceMaxDrops == TraceParams.MaxDrops

RMEvent(name, A(_)) ==
    /\ IsEvent(name)
    /\ LineNode \in RM
    /\ A(LineNode)
    /\ MatchVar("rmState", rmState'[LineNode])

TMEvent(name, A) ==
    /\ IsEvent(name)
    /\ LineNode = "tm"
    /\ A
    /\ MatchVar("tmDecision", tmDecision')
    /\ MatchSet("tmPrepared", tmPrepared')
    /\ MatchVar("tmUp", tmUp')

TraceNext ==
    \/ RMEvent("RMPrepare", RMPrepare)
    \/ RMEvent("RMChooseToAbort", RMChooseToAbort)
    \/ RMEvent("RMRcvCommit", RMRcvCommit)
    \/ RMEvent("RMRcvAbort", RMRcvAbort)
    \/ TMEvent("TMRcvPrepared", TMRcvPrepared(LineMsg.rm))
    \/ TMEvent("TMCommit", TMCommit)
    \/ TMEvent("TMTimeout", TMTimeout)
    \/ TMEvent("TMResend", TMResend)
    \/ TMEvent("TMCrash", TMCrash)
    \/ TMEvent("TMRestart", TMRestart)
    \/ TMEvent("DropMessage", DropMessage(LineMsg))

TraceInitState == Init /\ TraceInit

=============================================================================
