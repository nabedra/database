---
name: design-review
description: Use before implementing a plan that changes a topology or a protocol, or touches an L2 or L3 module, and when asked to review or model-check a design
---

# Design review protocol

A design review checks a plan before any code is written. It models the
plan in TLA+, checks the model with TLC, fixes the plan where the checker
finds a counterexample, and hands the checked plan to implementation.
[Code review](../code-review/SKILL.md) then checks the code against the
checked design.

```text
plan → spec → TLC → fix the plan → user approval → implementation → code review
```

## When it applies

A design review is required when a plan:

- changes the topology: the processes, who talks to whom, placement,
  membership or routing;
- changes a protocol: a message, a replication or commit step, a lease, a
  fence, failover or recovery;
- touches a module at level L2 or L3. See
  [assurance levels](../code-review/references/guarantees.md#assurance-levels).

For other plans it is optional. A plan that skips it says so in one line,
with the reason.

Existing protocols get a spec on next touch. The first plan or diff that
changes an L2 or L3 protocol writes its spec. Until then its T cells stay
`missing`.

## Find the affected guarantees

Start with the plan. List every guarantee it adds, changes or can break,
from the registers in `docs/testing/guarantees/`. Add guarantees for shared
types, formats and defaults the plan changes, as code review does. For a
new guarantee, draft its register row with its scenario IDs. See
[guarantees](../code-review/references/guarantees.md).

Map each affected guarantee to a named TLA+ property, or give the reason it
is not modelled. Valid reasons are that the guarantee concerns timing,
byte-level compatibility or client encoding. A guarantee whose failure model
names a race, an interleaving or a fault is always modelled.

## Build the design rubric

The rubric has four parts. [The sample rubrics](references/example.md) show
each part for a real plan.

- **Topology and failure model.** Name the processes, channels and stores,
  and draw the topology in the plan. For each channel and store, state the
  faults the spec models: crash, restart with or without its disk,
  partition, message loss, duplication, reordering and delay, clock drift
  within a bound, and torn unsynced writes. List the faults left out of
  scope and why.
- **Properties.** Safety invariants for every level. For L2 and L3, liveness
  under stated fairness. Each property names the guarantee it checks. See
  [properties](references/tla.md#properties).
- **Capacity and cost.** Count the RPCs, log entries, fsyncs, round trips
  and bytes per operation from the spec's actions. Compare them with the
  plan's targets in a table. Code review measures them later with
  `scripts/testing/ab.py`.
- **Upgrade and rollout.** When a plan changes a protocol, the spec models
  old and new nodes together and the flag flip, such as `direct_oltp`, as an
  action. Safety holds in every mixed state. When the plan supports a
  downgrade, the downgrade is an action too.

## Write and check the spec

Write the spec in raw TLA+ in `specs/<module>/`. No PlusCal.
[Writing specs](references/tla.md) covers the layout, conventions, bounds,
mutants and the TLC and Apalache commands.

- Check with TLC. Use Apalache only when TLC cannot reach the required
  bounds within the budget.
- Run at least the [bounds for the module's level](references/tla.md#bounds).
  L3 runs finish exhaustively, with no state cap.
- Every action is reachable. TLC's coverage report shows a nonzero count
  for each one.
- An invariant counts only after it fails on a named spec mutant. Record
  the mutant and the length of its counterexample.
- Iterate locally. Evidence comes from T checks dispatched on Actions,
  pinned to the plan's commit. Follow the [evidence rules](../code-review/references/evidence.md)
  and [GitHub Actions for checks](../code-review/references/gh-actions.md).

## Get an independent read

The agent that writes the plan usually writes the spec too. Before the spec
is final, dispatch a fresh subagent with only the plan, the affected
register rows and their scenario IDs. It does not see the spec or the
author's notes. It returns the failure scenarios and properties the design
must survive. The author encodes each one as an action, a fault or a
property, or records why it does not apply. Use the
[subagent workflow](../nabedra-change/references/subagent-driven-development.md).

## Fix the design

A counterexample is a design bug until shown otherwise. Follow the
[design bug loop](references/design-bug-loop.md). Keep the shortest trace,
fix the plan and the spec together, rerun, and record the bug. A fix that
changes a public contract, such as a wire protocol, HTTP API, ORM
signature, configuration, CLI or on-disk format, goes to the user before
the plan changes.

## Gate implementation

Implementation starts when both hold:

- the T checks pass on the plan's commit, with attested evidence;
- the user approves the reviewed plan. Show the user each counterexample
  that changed the plan before asking.

Plan, spec and check files land first on the task branch. Implementation
follows on the same branch, and one pull request carries both.

## Hand off to code review

The checked spec is part of the contract that code review verifies.
Simulation workloads for the module emit event traces, and a T check
confirms that each trace is a behavior of the spec. See
[trace validation](references/trace-validation.md). A rejected trace is a
blocking finding. When the code is right and the design is wrong, the
change returns to this protocol.

## Design review section

Every reviewed plan carries a `## Design review` section.

| Part | Content |
| --- | --- |
| Guarantees | Each affected ID, its property names, or the reason it is not modelled |
| Topology | The diagram, and the modelled and out-of-scope faults for each channel and store |
| Spec | Paths, bounds, fairness assumptions and T check IDs |
| Results | Run URLs, tested SHA, distinct states, depth, coverage and verdict |
| Mutants | Each invariant's mutant and its counterexample length |
| Counterexamples | Each design bug, its record, and how the plan changed |
| Cost | The per-operation count table against the targets |
| Independent read | The scenarios the subagent returned, and how each was encoded |

## Pull request body

A pull request that carries a reviewed plan adds a fifth line to the four
in [code review](../code-review/SKILL.md#pull-request-body).

```text
Design: <spec paths and T check run URLs with their verdicts, or none: <reason>>
```

## Verification rules

- A spec passes only on attested T evidence on the reviewed SHA. Local TLC
  output helps the author iterate. It is not evidence.
- An exhaustive run that stops at a state cap, a depth limit or the time
  budget is `not-run`, never `pass`.
- A vacuous spec never passes. Every action is reachable, and every
  invariant has a mutant that breaks it.
- A run of TLC in simulation mode is labelled as one. It never replaces the
  exhaustive run at the required bounds. The coverage matrix applies this:
  a T cell is green only on a passing exhaustive T0 or T1 run on its SHA.
- Nothing limits the search except a [declared limit](references/tla.md#limits-on-the-search).
- Every fairness assumption names the mechanism that provides it, such as
  a retry timer or a heartbeat. An assumption that no mechanism provides is
  a design gap.
- Never weaken a property, drop a fault or shrink a bound to turn a check
  green. Such a change needs a reason in the plan and approval from a
  reviewer that did not author it.
- For each design bug, identify the property, fault or bound that should
  have caught it and add it. Follow the
  [design bug loop](references/design-bug-loop.md).
- Implementation starts only after the [gate](#gate-implementation) holds.

## Rubric-style reviewing

Record the review tasks in a Markdown checklist or table, one row per
guarantee, fault, property and mutant. When coordinating parallel work,
group independent specs and assign them using the
[subagent workflow](../nabedra-change/references/subagent-driven-development.md).
Each agent uses its own request ID for a dispatch and reruns or cancels
only runs it started.

## Tooling status

Kind T was defined ahead of its tooling. Each piece below follows its
interim rule until it lands.

| Piece | Status | Interim rule |
| --- | --- | --- |
| `T` in `KINDS` in `scripts/testing/evidence.py`, read by `matrix.py` and `run_check.py` | Landed | None. A T check needs `needs = ["tlc"]` and `result_parser = "tlc"`. |
| A pinned TLC and Apalache image (`specs/tools/`), and a `tlc` need for the `checks` job | Landed | None. Run `specs/tools/tlc` locally; results stay `local, unattested`. |
| A `tlc` result parser in `run_check.py`, fed by `scripts/testing/tla_check.py` and `tla_trace.py` | Landed | None. |
| A `Design:` line check in `scripts/testing/pr_lint.py` | Landed | None. The pull-request gate requires the line when the diff changes `specs/**`, a plan's `## Design review` section, or a path of an L2 or L3 module that is not Markdown or is a design document such as `DESIGN.md`. Write `none: <reason>` when no spec applies. |
| Trace output from simulation workloads | Missing | The first plan that specs a module adds trace output to its workload, in the format of `nabedra.trace/1`. The simulator wave owns the workloads; see `wave-p8/design-review.md` in the wave files. |

[Specs](../../../specs/README.md) lists the tool versions, the commands and
the self-test that proves the pipeline end to end.
