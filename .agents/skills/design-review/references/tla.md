# Writing specs

A spec models the design in the plan. It does not model the code. It holds
the processes, messages, stores and faults the plan names, at the level of
detail the plan's guarantees need.

## Layout

One folder per module, named as in `docs/testing/modules.toml`.

| Path | Content |
| --- | --- |
| `specs/<module>/<Name>.tla` | The design spec |
| `specs/<module>/MC<Name>.tla` | Model values, symmetry sets and state constraints for TLC |
| `specs/<module>/<Name>.cfg` | The exhaustive run at the level's minimum bounds, tier T1 |
| `specs/<module>/<Name>.large.cfg` | Larger bounds for exhaustive or simulation runs, tier T2 |
| `specs/<module>/mutants/<mutant>.cfg` | One configuration per mutant, with the property it must break |
| `specs/<module>/<Name>Trace.tla` | The trace-validation spec. See [trace validation](trace-validation.md) |

`specs/README.md` lists every spec, its guarantees and its T checks, and
pins the tool versions.

## Conventions

- Write raw TLA+. Each action is one protocol step or message handler.
  Name it after the handler that will implement it, such as
  `HandleAppendEntries`, so trace validation maps events to actions one to
  one.
- Model each channel as a variable: a set for unordered delivery that may
  duplicate, a sequence for an ordered stream. Loss, duplication and
  reordering are separate actions, so each fault can be switched off.
- Model each fault as an action with a budget constant, such as
  `MaxCrashes` or `MaxPartitions`. The budgets in the configuration are the
  fault part of the bounds.
- Model time as an integer. Clock drift is a constant bound on the
  difference between a node's clock and true time. Never model real time.
- Keep the durable and volatile state of each node in separate variables.
  A crash resets the volatile state. A restart without the disk resets
  both.
- Always define `TypeOK` and check it as an invariant.
- Use a symmetry set for interchangeable node or client IDs only in safety
  runs. TLC can miss liveness violations under symmetry, so liveness runs
  use a configuration without it.
- A state constraint may bound counters such as terms or sequence numbers.
  Record each constraint in the plan, since it limits what the run covers.
- Leave deadlock checking on. A protocol that can finish adds an explicit
  terminal stuttering step.
- Each section of the spec names, in a comment, the plan section and the
  guarantee it models.

## Properties

Name each property after the guarantee it checks, such as
`G03_OncePerKey`, and put the guarantee ID in its comment.

- **Safety.** State invariants, and action properties of the form
  `[][A]_vars` for rules about steps, such as "a term never decreases".
  Use ghost variables for history, such as the set of replies sent, when a
  guarantee is about what clients observe.
- **Liveness.** Use `<>P` or `P ~> Q` under weak or strong fairness on the
  actions the design drives by a timer or a retry. Fairness applies only to
  those actions. Fairness on a fault action, or on a client choosing to
  send, hides bugs.
- Liveness holds after the faults stop. Check it with the fault budgets
  exhausted, or with a `GST` constant after which no fault action is
  enabled.

Refinement mappings to an abstract spec are welcome but not required.

## Bounds

The bounds below are minimums. A plan may raise them. Lowering one needs a
reason in the plan and approval from a reviewer that did not author it.

| Level | Nodes | Concurrent clients or requests | Faults | T1 run |
| --- | --- | --- | --- | --- |
| L1, when required | Declared in the plan, with the reason they are enough | Declared | Declared | Exhaustive, 15 min |
| L2 | At least 3 | At least 2 | At least 1 of each modelled kind | Exhaustive, 15 min |
| L3 | At least 4, or 3 with 2 ranges or shards | At least 2 | At least 1 of each modelled kind, and 2 overlapping faults | Exhaustive with no state cap, 60 min |

The times are wall-clock limits on a standard Actions runner. When the
minimum bounds do not fit, abstract the spec or use Apalache with a bounded
length. Never cut the bounds instead. Larger bounds and TLC simulation runs
across seeds go to the T2 nightly, labelled as simulation.

## Mutants

A mutant is a one-line change to the design that a property must reject,
such as dropping a fence check or a quorum of one. Write mutants as cases
of one constant, so the baseline and every mutant share one spec.

```tla
CONSTANT Mutant   \* "none" in <Name>.cfg, a mutant name in mutants/*.cfg

LeaseValid(n, t) ==
    IF Mutant = "no_uncertainty"
    THEN t < leaseEnd[n]
    ELSE t + MaxDrift < leaseEnd[n]
```

Each `mutants/<mutant>.cfg` sets `Mutant` and names the property it must
break in a first-line comment, `\* expect: G06_SinglePrimary`. The T check
passes when the baseline passes every property and each mutant fails on
its expected property. A mutant that fails with an error, or on a
different property, does not count.

When a design bug is fixed, the reverted fix becomes a mutant named
`prefix_<slug>`. See the [design bug loop](design-bug-loop.md).

## Run

TLC and Apalache run in the pinned image `specs/tools/Dockerfile`, with the
versions in `specs/tools/versions.toml`. The wrapper `specs/tools/tlc`
builds the image on first use and reuses it after, locally and on Actions.
Docker is the only host requirement. Set `SPEC` to the spec name and
`MODULE` to its folder.

```sh
python3 scripts/testing/tla_check.py "specs/$MODULE" "$SPEC"            # T1: baselines, coverage, mutants
python3 scripts/testing/tla_check.py "specs/$MODULE" "$SPEC" --tier T2 \
  --simulate --seed 1 --num 100000                                     # T2: <Name>.large.cfg as a simulation
```

`tla_check.py` runs every baseline (`<Name>.cfg` and any other
`<Name>.<variant>.cfg` except `.large.cfg`) exhaustively with coverage,
checks that every action was taken and that every invariant and property
has a mutant, then runs every mutant and checks that it fails on the
property its `\* expect:` line names. Each run gets the level's wall-clock
budget. It reports a violation, a deadlock, a liveness violation and a TLC
error separately, and a stop at the budget or a state cap as `not-run`.
Logs, `-dumpTrace json` counterexamples and a summary go to
`target/tla/specs/<module>/<Name>/`. [Specs](../../../../specs/README.md)
describes the layout rules, the options and how results map to evidence.

To run TLC by hand, call the wrapper from the spec folder. It mounts
`specs/` read-only, so write outputs under `--tla-out`:

```sh
cd "specs/$MODULE"
../tools/tlc --tla-out /tmp/tlc -workers 2 -coverage 1 \
  -dumpTrace json "/out/$SPEC.cex.json" -config "$SPEC.cfg" "MC$SPEC.tla"
```

- `-coverage 1` prints action counts every minute and at the end. Every
  action needs a nonzero count.
- `-dumpTrace json` saves the counterexample for the plan and the bug
  record.
- `-simulate num=<n>` with `-seed <seed>` runs random behaviors. Use it
  only on `.large.cfg` runs, and label the result as simulation.
- `-fpmem` and `-workers` change memory and CPU use, not coverage.

TLC exits nonzero on a violation, a deadlock or an error. It prints "No
error has been found" even when it stopped early, so a run is exhaustive
only when the queue is empty: `0 states left on queue`. Record the distinct
states, the depth and the verdict for each run.

Apalache checks bounded-length behaviors symbolically and needs type
annotations.

```sh
cd "specs/$MODULE"
../tools/apalache-mc --tla-out /tmp/apalache check --length="$LENGTH" \
  --inv="$INVARIANT" --config="$SPEC.cfg" "MC$SPEC.tla"
```

An Apalache run covers behaviors up to `--length` steps only. State the
length in the plan, next to the TLC bounds.

## Read a counterexample

TLC searches breadth first, so a safety counterexample is a shortest one.
A liveness counterexample is a lasso: a prefix and a loop that repeats
forever. Write the trace in the plan as numbered steps, naming the node,
the action and the messages in flight. Then follow the
[design bug loop](design-bug-loop.md).
