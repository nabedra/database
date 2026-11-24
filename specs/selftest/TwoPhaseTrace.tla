------------------------- MODULE TwoPhaseTrace -------------------------
(***************************************************************************)
(* Trace validation for TwoPhase. Each step of a nabedra.trace/1 trace    *)
(* must be a TwoPhase action, with the logged values as constraints on    *)
(* the next state. RM IDs are strings, and the TM logs as node "tm".      *)
(*                                                                         *)
(* Variables a trace does not constrain:                                  *)
(* - msgs: the channel. A line carries the message its step sent,         *)
(*   received or lost in "msg", and the step checks it: RequireMsg, or    *)
(*   ~HasMsg when there is none;                                          *)
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

\* The RM steps with the message each one sends or receives.
SendsPrepared(r) == RMPrepare(r) /\ RequireMsg([type |-> "Prepared", rm |-> r])
ChoosesToAbort(r) == RMChooseToAbort(r) /\ ~HasMsg
RcvsCommit(r) == RMRcvCommit(r) /\ RequireMsg([type |-> "Commit"])
RcvsAbort(r) == RMRcvAbort(r) /\ RequireMsg([type |-> "Abort"])

\* The message names the RM, so it is checked first.
TMRcvsPrepared ==
    /\ RequireMsg([type |-> "Prepared"])
    /\ "rm" \in DOMAIN LineMsg
    /\ TMRcvPrepared(LineMsg.rm)

TMStep(name, A, checks) == TraceStep(name, LineNode = "tm" /\ A, checks)

TraceNext ==
    \/ RMStep("RMPrepare", SendsPrepared)
    \/ RMStep("RMChooseToAbort", ChoosesToAbort)
    \/ RMStep("RMRcvCommit", RcvsCommit)
    \/ RMStep("RMRcvAbort", RcvsAbort)
    \/ TMStep("TMRcvPrepared", TMRcvsPrepared, [tmPrepared |-> RequireSet(tmPrepared')])
    \/ TMStep("TMCommit", TMCommit /\ RequireMsg([type |-> "Commit"]),
              [tmDecision |-> RequireVar(tmDecision')])
    \/ TMStep("TMTimeout", TMTimeout /\ RequireMsg([type |-> "Abort"]),
              [tmDecision |-> RequireVar(tmDecision')])
    \/ TMStep("TMResend",
              TMResend /\ RequireMsg([type |-> IF tmDecision = "committed" THEN "Commit" ELSE "Abort"]),
              NoVars)
    \* A crash changes tmPrepared only when the TM had read a message.
    \/ TMStep("TMCrash", TMCrash /\ ~HasMsg,
              [tmUp |-> RequireVar(tmUp'),
               tmPrepared |-> ChangedSet(tmPrepared, tmPrepared')])
    \/ TMStep("TMRestart", TMRestart /\ ~HasMsg, [tmUp |-> RequireVar(tmUp')])
    \/ TMStep("DropMessage", \E m \in msgs : DropMessage(m) /\ RequireMsg(m), NoVars)

TraceInitState == Init /\ TraceInit

=============================================================================
