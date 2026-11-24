------------------------ MODULE TwoPhaseTraceMutants ------------------------
(***************************************************************************)
(* Broken trace specs. tla_trace.py runs each trace-mutants/*.cfg and      *)
(* checks that specs/lib/NabedraTrace.tla and the runner notice. The       *)
(* checks of trace_spec_errors do not apply here, so these show that the   *)
(* library holds on its own.                                               *)
(***************************************************************************)
EXTENDS TwoPhaseTrace

\* A step that consumes no line.
SilentNext ==
    \/ TraceNext
    \/ \E r \in RM : RMChooseToAbort(r) /\ UNCHANGED <<traceIndex, traceExplained>>

\* A step that consumes a line without TraceStep, such as a fault that the
\* spec does not model.
SkipNext ==
    \/ TraceNext
    \/ /\ traceIndex <= Len(Trace)
       /\ traceIndex' = traceIndex + 1
       /\ UNCHANGED <<vars, traceExplained>>

\* The same, without the variable that only TraceStep sets.
ForgetNext ==
    \/ TraceNext
    \/ /\ traceIndex <= Len(Trace)
       /\ traceIndex' = traceIndex + 1
       /\ UNCHANGED vars

\* A spec that reads the time or the seed as a number.
TimeNext ==
    \/ TraceNext
    \/ TraceStep("TMRestart", Line.t > 0 /\ TMRestart, [tmUp |-> RequireVar(tmUp')])
SeedInit == TraceInitState /\ TraceHeader.seed > 0

\* A spec that compares a msg field that does not fit 32 bits, alone and as
\* part of the whole record, and one that keeps it in a state variable.
Late(delay) == [type |-> "Commit", delay_ns |-> delay]
FieldNext ==
    \/ TraceNext
    \/ TraceStep("TMResend", LineMsg.delay_ns = 5 /\ TMResend, NoVars)
RecordNext ==
    \/ TraceNext
    \/ TraceStep("TMResend", LineMsg \in {Late(5)} /\ TMResend, NoVars)
OrderNext ==
    \/ TraceNext
    \/ TraceStep("TMResend", LineMsg.delay_ns < 5 /\ TMResend, NoVars)

\* An action with several next states: each is checked with its own values.
ChooseNext ==
    \/ /\ TraceNext
       /\ Line.action # "TMTimeout"
    \/ TraceStep("TMTimeout",
                 /\ \E d \in {"init", "aborted", "committed"} : tmDecision' = d
                 /\ msgs' = msgs \cup {[type |-> "Abort"]}
                 /\ UNCHANGED <<rmState, tmPrepared, tmUp, crashes, drops>>,
                 [tmDecision |-> RequireVar(tmDecision')])
ChosenAborted == traceIndex > 3 => tmDecision = "aborted"

\* A condition after TraceStep: the line is not consumed by any behavior.
TrailingNext == TraceNext /\ Line.action # "RMRcvCommit"

\* ChangedVar: a line without the variable says that the step kept it.
KeepNext ==
    \/ /\ TraceNext
       /\ Line.action # "TMCommit"
    \/ TraceStep("TMCommit", TMCommit, [tmDecision |-> ChangedVar(tmDecision, tmDecision')])

\* OptionalVar and OptionalSet: a line may leave the variable out, and then
\* any value is accepted, but a logged value must agree.
OptionalNext ==
    \/ /\ TraceNext
       /\ Line.action \notin {"TMCommit", "TMRcvPrepared"}
    \/ TraceStep("TMCommit", TMCommit, [tmDecision |-> OptionalVar(tmDecision')])
    \/ TraceStep("TMRcvPrepared", TMRcvPrepared(LineMsg.rm), [tmPrepared |-> OptionalSet(tmPrepared')])

=============================================================================
