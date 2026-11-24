---------------------------- MODULE NabedraTrace ----------------------------
(***************************************************************************)
(* Reusable trace-validation helpers for nabedra.trace/1 traces.          *)
(* See .agents/skills/design-review/references/trace-validation.md.       *)
(*                                                                         *)
(* A trace spec <Name>Trace.tla EXTENDS <Name> and NabedraTrace. It reads *)
(* the trace named by the TRACE_PATH environment variable, which          *)
(* scripts/testing/tla_trace.py sets. Line 1 of the file is the header,  *)
(* every later line is one protocol step. Each TraceNext disjunct names   *)
(* one spec action and consumes one line:                                 *)
(*                                                                         *)
(*   \/ /\ IsEvent("TMCommit")                                            *)
(*      /\ TMCommit                                                        *)
(*      /\ MatchVar("tmDecision", tmDecision')                            *)
(*                                                                         *)
(* The configuration sets CHECK_DEADLOCK FALSE and                        *)
(* POSTCONDITION TraceAccepted. A trace is accepted when some behavior   *)
(* of the spec consumes every line.                                       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, Json, IOUtils, TLC

\* The whole file, header and steps, as TLA+ values. JSON objects become
\* records, arrays become sequences, strings and integers stay as they are.
TraceFileLines == ndJsonDeserialize(IOEnv.TRACE_PATH)

\* Line 1: {"trace":"nabedra.trace/1","spec":..,"seed":..,"sha":..,
\*          "workload":..,"params":{..}}
TraceHeader == TraceFileLines[1]

\* The constants the trace spec needs, under their spec names.
TraceParams == TraceHeader.params

\* The protocol steps. Trace[i] is the step with seq i - 1.
Trace == SubSeq(TraceFileLines, 2, Len(TraceFileLines))

\* Holds when the header names this format and the given spec. Use it in
\* an ASSUME, so that a trace of another spec fails before any step.
TraceHeaderOK(spec) ==
    /\ TraceHeader.trace = "nabedra.trace/1"
    /\ TraceHeader.spec = spec

\* Spec sets are logged as sorted JSON arrays. ToSet turns one back.
ToSet(s) == {s[i] : i \in DOMAIN s}

-----------------------------------------------------------------------------

\* The index into Trace of the next line to consume.
VARIABLE traceIndex

TraceInit == traceIndex = 1

\* The next unconsumed line and its parts.
Line == Trace[traceIndex]
LineNode == Line.node
LineMsg == Line.msg
HasMsg == "msg" \in DOMAIN Line

\* Consumes the next line when it names action a.
IsEvent(a) ==
    /\ traceIndex <= Len(Trace)
    /\ Line.action = a
    /\ traceIndex' = traceIndex + 1

\* A line's vars hold only the variables the step changed, with the node's
\* entry already selected. A variable the code cannot log stays
\* unconstrained. List each such variable in a comment in the trace spec.
Logged(v) == v \in DOMAIN Line.vars
LoggedValue(v) == Line.vars[v]

\* The logged value, if there is one, constrains the next state. Pass the
\* primed expression, such as rmState'[LineNode].
MatchVar(v, value) == Logged(v) => LoggedValue(v) = value

\* The same for a spec set, which is logged as a sorted array.
MatchSet(v, value) == Logged(v) => ToSet(LoggedValue(v)) = value

-----------------------------------------------------------------------------

\* INVARIANT. Every step consumes exactly one line, so a state's depth in
\* the search equals traceIndex. TraceAccepted relies on it: a step that
\* consumes no line would make a behavior longer than the lines it
\* explains. Every <Name>Trace.cfg lists it, and tla_trace.py checks that.
TraceIndexIsLevel == TLCGet("level") = traceIndex

\* POSTCONDITION. It holds only when some behavior consumed every line.
\* The diameter is the length in states of the longest behavior TLC found,
\* so, with TraceIndexIsLevel, diameter - 1 lines were consumed. On
\* rejection it prints the 0-based index of the first line no behavior
\* explains, which tla_trace.py reads.
TraceAccepted ==
    LET consumed == TLCGet("stats").diameter - 1 IN
    IF consumed = Len(Trace)
    THEN TRUE
    ELSE PrintT(<<"nabedra-trace-rejected", consumed, Len(Trace)>>) /\ FALSE

=============================================================================
