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
  changes, under their spec names.
- Write a line at the point where the step takes effect, such as after the
  fsync or after the reply is sent, not where it starts.
- Times come from the simulated clock, and the trace is deterministic for a
  seed. See [determinism](../../code-review/references/simulation.md#determinism).
- Only simulation builds record traces. Product builds never carry the
  instrumentation. The O02 and NS26 feature checks cover this.

## Check the trace

`<Name>Trace.tla` extends the design spec. It reads the trace with
`ndJsonDeserialize` from the Community Modules `Json` module, taking the
path from the environment through `IOEnv`. Each step consumes the next
line and takes the spec action that line names, with the logged values as
constraints on the next state. A variable the code cannot log stays
unconstrained, and the trace spec lists each such variable in a comment.

The configuration uses a `POSTCONDITION` that holds only when the last
line was consumed. A trace is accepted when some behavior of the spec
consumes every line. On rejection, the check reports the index and content
of the first line no behavior explains.

## Run

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
