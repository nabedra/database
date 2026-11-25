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
(* DropMessage changes only these, so its lines carry no variables.      *)
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
SendsPrepared(r) == RMPrepare(r) /\ RequireMsg([type |-> "Prepared", rm |-> r], {})
ChoosesToAbort(r) == RMChooseToAbort(r) /\ ~HasMsg
RcvsCommit(r) == RMRcvCommit(r) /\ RequireMsg([type |-> "Commit"], {})
RcvsAbort(r) == RMRcvAbort(r) /\ RequireMsg([type |-> "Abort"], {})

\* The message names the RM, so its other field is checked first.
TMRcvsPrepared ==
    /\ RequireMsg([type |-> "Prepared"], {"rm"})
    /\ "rm" \in DOMAIN LineMsg
    /\ TMRcvPrepared(LineMsg.rm)

TMStep(name, A, checks) == TraceStep(name, LineNode = "tm" /\ A, checks)

\* A TM line may carry every TM variable, also one the step left as it was.
\* Each is checked when it is there. A step requires only what it changes:
\* the faults do not require tmDecision, which the world that crashes the
\* process does not know.
TMVars ==
    [tmDecision |-> ChangedVar(tmDecision, tmDecision'),
     tmPrepared |-> ChangedSet(tmPrepared, tmPrepared'),
     tmUp |-> ChangedVar(tmUp, tmUp')]

TraceNext ==
    \/ RMStep("RMPrepare", SendsPrepared)
    \/ RMStep("RMChooseToAbort", ChoosesToAbort)
    \/ RMStep("RMRcvCommit", RcvsCommit)
    \/ RMStep("RMRcvAbort", RcvsAbort)
    \/ TMStep("TMRcvPrepared", TMRcvsPrepared, [tmPrepared |-> RequireSet(tmPrepared')] @@ TMVars)
    \/ TMStep("TMCommit", TMCommit /\ RequireMsg([type |-> "Commit"], {}),
              [tmDecision |-> RequireVar(tmDecision')] @@ TMVars)
    \/ TMStep("TMTimeout", TMTimeout /\ RequireMsg([type |-> "Abort"], {}),
              [tmDecision |-> RequireVar(tmDecision')] @@ TMVars)
    \* A resend may say how long it waited. The spec has no such field.
    \/ TMStep("TMResend",
              TMResend /\ RequireMsg([type |-> IF tmDecision = "committed" THEN "Commit" ELSE "Abort"],
                                     {"delay_ns"}),
              TMVars)
    \* A crash changes tmPrepared only when the TM had read a message.
    \/ TMStep("TMCrash", TMCrash /\ ~HasMsg, [tmUp |-> RequireVar(tmUp')] @@ TMVars)
    \/ TMStep("TMRestart", TMRestart /\ ~HasMsg, [tmUp |-> RequireVar(tmUp')] @@ TMVars)
    \* The line names the lost message, and may say where it was lost.
    \/ TMStep("DropMessage", \E m \in msgs : DropMessage(m) /\ RequireMsg(m, {"to", "cause"}), NoVars)

TraceInitState == Init /\ TraceInit

=============================================================================
