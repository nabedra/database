# Trace validation

Trace validation keeps the code and the checked design from drifting apart.
A simulation workload records the protocol steps the code takes. TLC then
checks that the recorded trace is a behavior of the spec. The method
follows Cirstea, Kuppe, Loillier and Merz,
[Validating Traces of Distributed Programs Against TLA+ Specifications](https://arxiv.org/abs/2404.16075)
(2024).

## Record the trace

- The module's simulation workload writes one JSON line per protocol step:
  `seq`, `node`, the spec action name, and the variables the action
  changes, under their spec names. A line must carry every variable that
  its trace spec step requires. A line that does not is rejected, with the
  line index and the variable name.
- Write a line at the point where the step takes effect, such as after the
  fsync or after the reply is sent, not where it starts.
- Times come from the simulated clock, and the trace is deterministic for a
  seed. See [determinism](../../code-review/references/simulation.md#determinism).
- TLC integers have 32 bits, and TLC wraps a larger JSON integer without an
  error. The runner checks `t` and `seed` itself, with exact integers, and
  TLC reads a copy of the trace without them. An integer in `vars` or
  `params` must fit 32 bits, or the lint fails. Log a 64-bit ID, hash or
  timestamp there as a string. An integer in `msg` that does not fit
  reaches TLC as a decimal string, so a trace spec must not compare that
  field, alone or as part of the whole record. The comparison is a TLC
  error on the lines where the value is large. A field that can exceed 32
  bits is safer as a string on every line.
- Only simulation builds record traces. Product builds never carry the
  instrumentation. The O02 and NS26 feature checks cover this.

## Check the trace

`<Name>Trace.tla` extends the design spec and `NabedraTrace`. Each
`TraceNext` disjunct is a `TraceStep`. It consumes the next line, takes the
spec action that line names, and checks the line's variables:

```tla
TraceStep("TMCrash", TMCrash,
          [tmUp |-> RequireVar(tmUp'),
           tmPrepared |-> ChangedSet(tmPrepared, tmPrepared')])
```

The third argument says how to check each variable that the line may carry.
Pass the primed expression with the node's entry already selected, such as
`rmState'[LineNode]`. The `Set` forms are for a spec set, which is logged as
a sorted array. Join two records with `@@`.

| Check | The line |
| --- | --- |
| `RequireVar`, `RequireSet` | Carries the variable, with the next state's value. Use it unless one of the others is needed |
| `ChangedVar`, `ChangedSet` | Carries the variable when the step changes it. Without it, the step must leave the variable as it was. For a variable that only some of the action's steps change |
| `OptionalVar`, `OptionalSet` | May carry the variable. Without it, any value is accepted |
| `NoVars` | Carries no variable. For an action that changes none that the code can log, such as most faults |

A line that carries a variable the step does not name is rejected too, so a
misspelt name cannot go unnoticed. A variable the code cannot log is left
out of every check, or is an `OptionalVar` when the code can log it on some
lines only. The comment at the top of the trace spec lists each one with
the reason. `OptionalVar` and `NoVars` are not a way to make a trace pass:
the line then proves less.

Every step consumes one line, so a trace spec has no hidden steps. When the
code takes a step it does not log, either log it, or fold it into the action
of the next logged line. The module's mapping file lists each folded step
with the reason nothing can interleave between the two.

The configuration lists the invariant `TraceIndexIsLevel` and sets
`POSTCONDITION TraceAccepted`, which holds only when some behavior explained
every line. On rejection, the check reports the index and content of the
first line no behavior explains, and the variables that are missing, logged
but not checked, or different there. A TLC evaluation error, such as a
comparison of a string with a number, is an `error` that names the line. It
is never a rejection.

## Run

`specs/lib/NabedraTrace.tla` holds the shared helpers. `python3
scripts/testing/tla_trace.py lint` checks the format, and `tla_trace.py
check` runs the trace spec once per trace. See
`specs/selftest/TwoPhaseTrace.tla` for a complete example and
[specs](../../../../specs/README.md) for the commands, and for what the
runner does and does not detect in a weak trace spec.

Earlier trace specs used `IsEvent`, `MatchVar` and `MatchSet`, which
accepted a line that did not log the variable. They were removed on
2026-11-24, and a trace spec that uses them breaks on purpose, with an
error that names the replacement.

A trace-validation check is kind T, with an ID ending in `-trace`. It lists
the same guarantees as its design spec.

| Tier | Traces |
| --- | --- |
| T0 | The traces of the workload's local seeds, about 20 |
| T1 | The workload's T1 seeds, plus every seed in the bug-replay corpus for the module |
| T2 | The nightly seed range |

## Read a rejection

A rejected trace is never a flake. The cause is one of three:

- the code took a step the design does not allow. That is a code bug, so
  follow the [bug loop](../../code-review/references/bug-loop.md);
- the instrumentation logged the wrong value or the wrong point. Fix it
  and record the fix;
- the design changed in the code. Return to the
  [design review](../SKILL.md) and update the plan and the spec first.

Replay the seed to reproduce the trace, as in
[replay](../../code-review/references/simulation.md#replay).
