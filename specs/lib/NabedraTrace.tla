---------------------------- MODULE NabedraTrace ----------------------------
(***************************************************************************)
(* Reusable trace-validation helpers for nabedra.trace/1 traces.          *)
(* See .agents/skills/design-review/references/trace-validation.md.       *)
(*                                                                         *)
(* A trace spec <Name>Trace.tla EXTENDS <Name> and NabedraTrace. It reads *)
(* the trace named by the TRACE_PATH environment variable, which          *)
(* scripts/testing/tla_trace.py sets. Line 1 of the file is the header,  *)
(* every later line is one protocol step. Each TraceNext disjunct is a    *)
(* TraceStep. It names one spec action, consumes one line, and says how   *)
(* to check every variable the line may carry:                            *)
(*                                                                         *)
(*   \/ TraceStep("TMCommit", TMCommit,                                   *)
(*                [tmDecision |-> RequireVar(tmDecision')])               *)
(*                                                                         *)
(* A line that misses a required variable, or carries one the step does   *)
(* not check, is rejected like a wrong value. The third argument is not   *)
(* optional, so a step cannot leave a variable unchecked silently:        *)
(* OptionalVar and NoVars say what they give up.                          *)
(*                                                                         *)
(* The step checks "node" and "msg" itself, in its second argument. See  *)
(* RequireMsg. A field that no step compares is not checked.             *)
(*                                                                         *)
(* The copy of the trace that TLC reads has no "t" and no "seed", because  *)
(* TLC integers have 32 bits. A trace spec that reads Line.t or           *)
(* TraceHeader.seed gets a TLC error. An integer outside 32 bits in "msg" *)
(* reaches TLC as a decimal string, so a spec must not compare such a     *)
(* field, alone or as part of the whole record.                           *)
(*                                                                         *)
(* The configuration sets CHECK_DEADLOCK FALSE, lists the invariant       *)
(* TraceIndexIsLevel and sets POSTCONDITION TraceAccepted. A trace is    *)
(* accepted when some behavior of the spec consumes every line.           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, Json, IOUtils, TLC

\* The whole file, header and steps, as TLA+ values. JSON objects become
\* records, arrays become sequences, strings and integers stay as they are.
TraceFileLines == ndJsonDeserialize(IOEnv.TRACE_PATH)

\* Line 1: {"trace":"nabedra.trace/1","spec":..,"seed":..,"sha":..,
\*          "workload":..,"params":{..}}, without the seed.
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

VARIABLES
    traceIndex,     \* the index into Trace of the next line to consume
    traceExplained  \* the lines this behavior consumed through TraceStep

\* TLC register: the most lines one behavior explained, and the variable
\* problems found at the furthest line.
LOCAL TraceRegister == 73001

TraceInit ==
    /\ traceIndex = 1
    /\ traceExplained = 0
    /\ TLCSet(TraceRegister, [explained |-> 0, at |-> 0, notes |-> {}])

\* The next unconsumed line and its parts.
Line == Trace[traceIndex]
LineNode == Line.node
LineMsg == Line.msg
HasMsg == "msg" \in DOMAIN Line

-----------------------------------------------------------------------------

(* How a step checks one variable of its line. Pass the primed expression, *)
(* with the node's entry already selected, such as rmState'[LineNode].     *)
(* The Set forms are for a spec set, which is logged as a sorted array.    *)

\* Every line of the action carries the variable, with the next state's
\* value. Use it unless one of the others is needed.
RequireVar(new) == [must |-> TRUE, set |-> FALSE, new |-> new]
RequireSet(new) == [must |-> TRUE, set |-> TRUE, new |-> new]

\* The line carries the variable when the step changes it. A line without
\* it says that the step left it as it was. Use it for a variable that only
\* some of the action's steps change. old is unprimed. TLC compares old and
\* new, so a change between a string and a record is a TLC error: use
\* RequireVar for such a variable.
ChangedVar(old, new) == [must |-> old # new, set |-> FALSE, new |-> new]
ChangedSet(old, new) == [must |-> old # new, set |-> TRUE, new |-> new]

\* The line may carry the variable. Without it, any value is accepted. Use
\* it only for a variable the code cannot always log, and list it with the
\* reason in the comment at the top of the trace spec.
OptionalVar(new) == [must |-> FALSE, set |-> FALSE, new |-> new]
OptionalSet(new) == [must |-> FALSE, set |-> TRUE, new |-> new]

\* The action changes no variable the code can log, so its lines carry none.
NoVars == [v \in {} |-> v]

LOCAL Logged == DOMAIN Line.vars

\* A set is logged as an array without duplicates. Its order is not
\* checked, because TLC does not order strings.
LOCAL Agrees(v, check) ==
    LET value == Line.vars[v] IN
    IF check.set THEN /\ Len(value) = Cardinality(ToSet(value))
                      /\ ToSet(value) = check.new
                 ELSE value = check.new

\* Keeps the problems of the furthest line, for TraceAccepted to print.
LOCAL Note(notes) ==
    LET old == TLCGet(TraceRegister) IN
    IF old.at > traceIndex THEN TRUE
    ELSE TLCSet(TraceRegister,
                [old EXCEPT !.at = traceIndex,
                            !.notes = notes \cup (IF old.at = traceIndex THEN @ ELSE {})])

\* The line's msg carries every field of m, with its value, and no other
\* field except those in extras, which are not compared. Use it in the second
\* argument of a TraceStep, such as
\* TMCommit /\ RequireMsg([type |-> "Commit"], {}). When the spec's message
\* is a record and the line also says where a fault hit it, project it:
\* \E m \in msgs : DropMessage(m) /\ RequireMsg(m, {"to", "cause"}).
RequireMsg(m, extras) ==
    LET have == IF HasMsg THEN DOMAIN LineMsg ELSE {}
        notes ==
            {<<"missing", "msg." \o f>> : f \in DOMAIN m \ have}
            \cup {<<"unchecked", "msg." \o f>> : f \in have \ (DOMAIN m \cup extras)}
            \cup {<<"different", "msg." \o f>> :
                     f \in {g \in DOMAIN m \cap have : LineMsg[g] # m[g]}}
    IN IF notes = {} THEN TRUE ELSE Note(notes) /\ FALSE

\* Counts the line as explained. A line consumed in any other way is not.
LOCAL Explained ==
    /\ traceExplained' = traceExplained + 1
    /\ LET old == TLCGet(TraceRegister) IN
       IF old.explained < traceExplained'
       THEN TLCSet(TraceRegister, [old EXCEPT !.explained = traceExplained'])
       ELSE TRUE

\* The line's vars are what checks allows: no required variable is missing,
\* none is left unchecked, and every value agrees.
LOCAL LineVarsOK(checks) ==
    LET notes ==
            {<<"missing", v>> : v \in {u \in DOMAIN checks \ Logged : checks[u].must}}
            \cup {<<"unchecked", v>> : v \in Logged \ DOMAIN checks}
            \cup {<<"different", v>> :
                     v \in {u \in DOMAIN checks \cap Logged : ~Agrees(u, checks[u])}}
    IN IF notes = {} THEN TRUE ELSE Note(notes) /\ FALSE

\* Consumes the next line when it names action a, takes the spec action A
\* and checks the line's variables. checks is a record from variable names
\* to the checks above, or NoVars. Join two records with @@. A guards its
\* own use of the line, such as LineNode \in RM /\ RMPrepare(LineNode).
TraceStep(a, A, checks) ==
    /\ traceIndex <= Len(Trace)
    /\ Line.action = a
    /\ traceIndex' = traceIndex + 1
    /\ A
    /\ LineVarsOK(checks)
    /\ Explained

-----------------------------------------------------------------------------

\* INVARIANT. Every step consumes exactly one line through TraceStep, so a
\* state's depth in the search equals traceIndex. A step that consumes no
\* line would make a behavior longer than the lines it explains, and a step
\* that skips a line would leave it unexplained. Every <Name>Trace.cfg lists
\* it, and tla_trace.py checks that. So a trace spec has no hidden steps: a
\* step the code does not log is folded into the action of the next logged
\* line.
TraceIndexIsLevel ==
    /\ TLCGet("level") = traceIndex
    /\ traceExplained = traceIndex - 1

\* POSTCONDITION. It holds only when some behavior explained every line. The
\* diameter is the length in states of the longest behavior TLC found, so,
\* with TraceIndexIsLevel, diameter - 1 lines were consumed. The lines that
\* TraceStep explained bound it, so it does not rest on the invariant alone.
\* traceExplained counts them, so nothing else may set it.
\* On rejection it prints the 0-based index of the first line no behavior
\* explains, and the variable problems found at that line, which
\* tla_trace.py reads.
TraceAccepted ==
    LET depth == TLCGet("stats").diameter - 1
        last == TLCGet(TraceRegister)
        consumed == IF depth < last.explained THEN depth ELSE last.explained
    IN
    IF consumed = Len(Trace)
    THEN TRUE
    ELSE /\ PrintT(<<"nabedra-trace-rejected", consumed, Len(Trace)>>)
         /\ \A n \in (IF last.at = consumed + 1 THEN last.notes ELSE {}) :
                PrintT(<<"nabedra-trace-line", consumed, n[1], n[2]>>)
         /\ FALSE

=============================================================================
