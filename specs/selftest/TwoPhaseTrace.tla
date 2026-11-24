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
(* TMResend and DropMessage change only these, so their lines carry no   *)
(* variables.                                                             *)
(***************************************************************************)
EXTENDS TwoPhase, NabedraTrace

ASSUME TraceHeaderOK("TwoPhase")

TraceRM == ToSet(TraceParams.RM)
TraceMaxCrashes == TraceParams.MaxCrashes
TraceMaxDrops == TraceParams.MaxDrops

RMStep(name, A(_)) ==
    TraceStep(name, LineNode \in RM /\ A(LineNode),
              [rmState |-> RequireVar(rmState'[LineNode])])

TMStep(name, A, checks) == TraceStep(name, LineNode = "tm" /\ A, checks)

TraceNext ==
    \/ RMStep("RMPrepare", RMPrepare)
    \/ RMStep("RMChooseToAbort", RMChooseToAbort)
    \/ RMStep("RMRcvCommit", RMRcvCommit)
    \/ RMStep("RMRcvAbort", RMRcvAbort)
    \/ TMStep("TMRcvPrepared", TMRcvPrepared(LineMsg.rm),
              [tmPrepared |-> RequireSet(tmPrepared')])
    \/ TMStep("TMCommit", TMCommit, [tmDecision |-> RequireVar(tmDecision')])
    \/ TMStep("TMTimeout", TMTimeout, [tmDecision |-> RequireVar(tmDecision')])
    \/ TMStep("TMResend", TMResend, NoVars)
    \* A crash changes tmPrepared only when the TM had read a message.
    \/ TMStep("TMCrash", TMCrash,
              [tmUp |-> RequireVar(tmUp'),
               tmPrepared |-> ChangedSet(tmPrepared, tmPrepared')])
    \/ TMStep("TMRestart", TMRestart, [tmUp |-> RequireVar(tmUp')])
    \/ TMStep("DropMessage", DropMessage(LineMsg), NoVars)

TraceInitState == Init /\ TraceInit

=============================================================================
