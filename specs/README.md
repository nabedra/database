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
| `specs/lib/NabedraTrace.tla` | Shared trace helpers, on TLC's library path |
| `specs/tools/` | The pinned tools |
| `specs/selftest/` | The self-test spec |

The folder name is a module in `docs/testing/modules.toml`. Its level sets
the wall-clock budget of each run: 15 minutes for L1 and L2, 60 for L3.
`matrix.py select` picks a module's checks when `specs/<module>/**` changes,
and every kind T check when `specs/lib/**` or `specs/tools/**` changes.

Every `INVARIANT` and `PROPERTY` of a baseline needs a mutant whose
`\* expect:` line names it. Only a type invariant may be exempted, with a
line in the baseline cfg such as
`\* no-mutant: TypeOK -- the type invariant; every run checks it.`

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
trace with `TRACE_PATH` set. A rejected trace is reported with the 0-based
index (its `seq`) and the content of the first line no behavior explains.
Every trace under `--expect-rejected` must be rejected, at index `n` when
the file is named `*.at-<n>.ndjson`.

Both print `nabedra-tla-summary: {json}` last. As the check's
`result_parser = "tlc"`, it becomes the evidence record, with a `tla` object
holding the distinct states, depth, coverage, each mutant's counterexample
length, or the trace counts. Logs and counterexamples are artifacts.

| Outcome | Result |
| --- | --- |
| Every baseline exhaustive and clean, every action covered, every property mutated, every mutant caught on its property | `pass` |
| A baseline violation, a coverage gap, an unmutated property, or a mutant that passes, errs or fails on another property | `fail` |
| A baseline stopped at the time budget or a state cap, with no failure | `not-run` |
| A baseline TLC error, such as a parse or evaluation error | `error` |
| A trace no behavior explains, a lint failure, or an expected rejection that does not happen | `fail` |

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
`no_resend` breaks `Termination`), a trace spec, and one accepted and one
rejected sample trace. The checks `specs-selftest` (T0, T1),
`specs-selftest-trace` (T0, T1) and `specs-selftest-large` (T2, simulation)
prove the pipeline end to end.

## Specs

| Module | Spec | Guarantees | T checks | Plan |
| --- | --- | --- | --- | --- |
| testing | `selftest/TwoPhase.tla` | none: the tooling self-test | `specs-selftest`, `specs-selftest-trace`, `specs-selftest-large` | none |

No product spec exists yet. Existing protocols get one on next touch.
