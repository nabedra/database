# Design specs

TLA+ specs of Nabedra's designs, one folder per module. The
[design review skill](../.agents/skills/design-review/SKILL.md) says when a
plan needs a spec and how to check it. [Writing specs](../.agents/skills/design-review/references/tla.md)
covers the layout and conventions.

## Tools

| Tool | Version | SHA-256 | Use |
| --- | --- | --- | --- |
| TLC (`tla2tools.jar`) | v1.8.0, "The Clarke release" (pre-release, 2026-11-19) | `fdffef247f563ef6edbfd44958d2a82db79e7564e07cb673262d896353fa6713` | Required checker |
| TLA+ Community Modules (`CommunityModules-deps.jar`) | 202611060952 | `a3b27c40f1fb5085f30c6ead37e918457f8cfc62c1e50a1dc668ef592ad2b5fb` | `Json` and `IOUtils` for trace validation |
| Apalache | v0.62.4 (2026-10-12) | `b781b53bba32fac14ac2e02b0332e1dc2edc64cb7b850c0608d8142cc7aa2ef4` | Symbolic checks of larger bounds, when needed |
| Java runtime | `eclipse-temurin:21-jre`, pinned by digest | `d77e090ae1f6970c047b443c6f0a6c1358b0426d44432bb5a7753408cd43de0d` | Runs both checkers |

[`tools/versions.toml`](tools/versions.toml) is the single source of these
versions. [`tools/Dockerfile`](tools/Dockerfile) builds the image from them
and checks every download against its SHA-256. The wrapper
[`tools/tlc`](tools/tlc) (and `tools/apalache-mc`, a link to it) builds the
image on first use, tags it with a hash of both files, and reuses it after.
The same image runs locally and in the `checks` job on GitHub Actions, which
caches it between runs. Docker is the only host requirement.

Why TLC v1.8.0 and not the stable v1.7.4: v1.7.4 (2024-08-05) rejects
`-dumpTrace` as an unknown option (tested with the v1.7.4 release jar), so
it cannot save the JSON counterexamples that the runner keeps for every
failing run and the bug records cite. v1.8.0 supports `-dumpTrace json`, `POSTCONDITION`,
and the Community Modules `ndJsonDeserialize` and `IOEnv`, which the
self-test exercises. Apalache v0.63.0 was exactly two weeks old when these
versions were pinned, so the newest release clearly older than two weeks,
v0.62.4, was taken.

## Layout

| Path | Content |
| --- | --- |
| `specs/<module>/<Name>.tla` | The design spec |
| `specs/<module>/MC<Name>.tla` | Optional model module; TLC runs it instead of `<Name>.tla` when it exists |
| `specs/<module>/<Name>.cfg` | The T1 baseline: exhaustive, at the level's minimum bounds |
| `specs/<module>/<Name>.<variant>.cfg` | Optional extra T1 baselines, such as a liveness run without symmetry |
| `specs/<module>/<Name>.large.cfg` | T2: larger bounds, exhaustive or as a simulation |
| `specs/<module>/mutants/<mutant>.cfg` | One per mutant. The first line is `\* expect: <Property>` (or `Deadlock`) |
| `specs/<module>/<Name>Trace.tla` and `.cfg` | Trace validation, extending `<Name>` and `NabedraTrace` |
| `specs/<module>/traces/` | Optional committed traces, such as a bug-replay corpus |
| `specs/<module>/trace-mutants/*.cfg` | Optional broken trace specs, run with `<Name>TraceMutants.tla` |
| `specs/lib/NabedraTrace.tla` | Shared trace helpers, on TLC's library path |
| `specs/tools/` | The pinned tools |
| `specs/selftest/` | The self-test spec |

The folder name is a module in `docs/testing/modules.toml`. Its level sets
the wall-clock budget of each run: 15 minutes for L1 and L2, 60 for L3.
`matrix.py select` picks a module's checks when `specs/<module>/**` changes,
and every kind T check when `specs/lib/**` or `specs/tools/**` changes.

Every `INVARIANT` and `PROPERTY` of a baseline needs a mutant whose
`\* expect:` line names it, and the baselines check at least one. Only a
type invariant, with `Type` in its name, may be exempted, with a line in the
baseline cfg such as
`\* no-mutant: TypeOK -- the type invariant; every run checks it.`

A `CONSTRAINT` may only be a conjunction of upper bounds on counters, and it
and an override of a defined operator need a line
`\* limit: <Name> -- <reason>` in the cfg. The runner refuses other limits on
the search, such as an `ACTION_CONSTRAINT`, a depth limit or a `VIEW`, in
baselines and mutants. A mutant changes nothing but the constant `Mutant`. See
[limits on the search](../.agents/skills/design-review/references/tla.md#limits-on-the-search).

## Run

```sh
python3 scripts/testing/tla_check.py specs/<module> <Name>                # T1
python3 scripts/testing/tla_check.py specs/<module> <Name> --tier T2 --simulate --seed 1 --num 100000
python3 scripts/testing/tla_trace.py lint <trace.ndjson>...
python3 scripts/testing/tla_trace.py check specs/<module> <Name> <traces or dirs> [--expect-rejected DIR]
```

`tla_check.py` options: `--module` when the folder is not a module name,
`--budget-s` to set the per-run budget, `--workers` (2 locally, `auto` on
Actions), and `--out` (default `target/tla/specs/<module>/<Name>/<tier>`).
`NABEDRA_TLC_HEAP` sets the JVM heap (2 GB locally, 8 GB on Actions).
Coverage counts the states each action generated, over the completed
baselines, so a terminal stuttering step counts and a liveness baseline with
exhausted fault budgets causes no gap.

`tla_trace.py check` lints each trace, then runs `<Name>Trace.tla` once per
trace with `TRACE_PATH` set. TLC integers have 32 bits, so TLC reads a copy
of the trace without `t` and `seed`, where an integer in `msg` that does not
fit is a decimal string. The lint refuses such an integer in `vars` and
`params`. A rejected trace is reported with the 0-based index (its `seq`)
and the content of the first line no behavior explains, and with the
variables that are missing, logged but not checked, or different there.
Every trace under `--expect-rejected` must be rejected, at index `n` when
the file is named `*.at-<n>.ndjson`, or refused by the lint when it is named
`*.at-lint.ndjson`. Otherwise a lint failure is a failure there too. Without a trace to accept, the result
is `not-run`, never `pass`: rejected samples, trace mutants and the control
only show that the check works. A path that does not exist, a directory
without traces next to other traces, and traces in a subfolder that no
argument covers are errors.

A trace spec takes its steps through `TraceStep`, which requires the line's
variables. See [trace validation](../.agents/skills/design-review/references/trace-validation.md).
Before any trace, the check reads the text of `<Name>Trace.cfg`,
`<Name>Trace.tla` and the modules of the spec folder that it extends or
instantiates, and refuses:

- a cfg without the invariant `TraceIndexIsLevel` or `POSTCONDITION
  TraceAccepted` (comments, also nested, do not count), or one that overrides
  an operator the check relies on;
- a module in the spec folder with the name of a shared or standard module,
  one that defines an operator of `NabedraTrace` or instantiates it, and a
  trace spec that does not extend it;
- a module that uses `traceIndex` or `traceExplained`, except to look ahead
  with `Trace[traceIndex + 1]`;
- a `TraceStep` whose action name is not a fixed string, so that one step
  could take any line;
- `ChangedVar` or `ChangedSet` with a primed first argument;
- a removed operator (`IsEvent`, `MatchVar`, `MatchSet`), no `TraceStep`, or
  no step that requires a variable.

TLC then counts a line as consumed only when a `TraceStep` took it. A step
that consumes no line is an error. A step that skips a line is an error
unless it also counts `traceExplained` itself, which the text check refuses.
After an accepted trace, one control run checks that a line with an unknown
action, no variables and no message is rejected.

Not detected, and left to the review of the trace spec:

- a step that takes a weaker action than its name says, such as one that
  stutters under the name of a fault;
- a `msg` field or a `node` that the step does not compare, and the extra
  fields that `RequireMsg` allows;
- that the variables and `msg` fields a trace writer declares for an action
  are the ones the trace spec's step checks, until a trace shows it;
- `OptionalVar` or `NoVars` without a reason, and a check that does not
  depend on the next state;
- an initial state weaker than `Init`, and a constant from `params` that
  hides a difference;
- the order of the array that logs a set, and an empty object in place of an
  empty one;
- that `RequireVar` is reached on every action. The text checks find
  mistakes. They do not stop a spec that is written to evade them.

Known false alarms: `ChangedVar` on a variable that changes between a string
and a record is a TLC error, because TLC compares the two, so use
`RequireVar` there. So is any comparison of a logged value with a value of
another type.

When `trace-mutants/` exists, each `*.cfg` there runs
`<Name>TraceMutants.tla`, a broken version of the trace spec, on one trace.
Its first two lines are `\* expect: rejected at 6` (or `accepted` or
`error`, with an optional index and an optional `: <text in the reason>`)
and `\* trace: <path in the spec folder>`. Each is one assertion.

Both print `nabedra-tla-summary: {json}` last. As the check's
`result_parser = "tlc"`, it becomes the evidence record, with a `tla` object
holding the distinct states, depth, coverage, each mutant's counterexample
length, or the trace counts. Logs and counterexamples are artifacts.

| Outcome | Result |
| --- | --- |
| Every baseline exhaustive and clean, every action covered, every property mutated, every mutant caught on its property | `pass` |
| A baseline violation, a coverage gap, no property or an unmutated one, an undeclared or refused limit on the search, or a mutant that passes, errs or fails on another property | `fail` |
| A baseline stopped at the time budget or a state cap, with no failure | `not-run` |
| A baseline TLC error, such as a parse or evaluation error, or a success message with a nonzero exit | `error` |
| A trace no behavior explains, a lint failure, or an expected rejection or trace mutant that does not happen as expected | `fail` |
| A trace spec the check refuses, a control that is accepted, or a TLC error on a trace | `error` |

## Check files

A T check is a check file with `kinds = ["T"]`, `needs = ["tlc"]` and
`result_parser = "tlc"`. The design check is `<module>-<spec>`, its T2 form
`<module>-<spec>-large`, and its trace check `<module>-<spec>-trace`, with
the same guarantees. See `docs/testing/checks/specs-selftest*.toml`.

## Self-test

`specs/selftest/` holds `TwoPhase`, a two-phase commit with a transaction
manager that crashes and restarts and a channel that loses messages. It has
`TypeOK`, the safety invariant `Consistent`, the liveness property
`Termination` under fairness named for its timers, its receive loop and its
supervisor, two mutants (`commit_one_prepared` breaks `Consistent`,
`no_resend` breaks `Termination`), and a trace spec. The accepted sample
traces include one with 64-bit times and a 64-bit seed. The rejected ones
cover a step the spec does not allow, missing variables (one, a set, one of
two, all), a variable that nothing checks, a wrong value and set, an element
logged twice, messages that are wrong, missing, out of place or have a field
too many, and traces the lint refuses. One accepted trace logs every variable of the transaction
manager on its own steps and what the fault defines on the others. The
trace mutants cover steps that skip or consume no line, trace specs that
read `t`, `seed` or a wide `msg` field, and what each check in
`NabedraTrace` requires. The checks `specs-selftest` (T0, T1),
`specs-selftest-trace` (T0, T1) and `specs-selftest-large` (T2, simulation)
prove the pipeline end to end.

## Specs

| Module | Spec | Guarantees | T checks | Plan |
| --- | --- | --- | --- | --- |
| testing | `selftest/TwoPhase.tla` | none: the tooling self-test | `specs-selftest`, `specs-selftest-trace`, `specs-selftest-large` | none |

No product spec exists yet. Existing protocols get one on next touch.
