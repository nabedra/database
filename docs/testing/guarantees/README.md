# Guarantee registers

Each module register records the behavior the test plan requires. A row
defines a guarantee, the failures and operating conditions it covers, its
scenario IDs, and the check kinds that need evidence.

## Register format

Use the module's name in the `Module: <module>` line and its highest
assurance level in the `Level: L1`, `Level: L2`, or `Level: L3` line.
Describe any split in levels in the introduction.

Each register has one Markdown table with this exact header.

```text
| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
```

Write scenario IDs as a comma-separated list. An empty list means the
guarantee has no numbered scenario. Write check letters separated by spaces.
For each omitted kind, give a `<kind>: <reason>` entry in `n/a`. Separate
entries with semicolons. Keep the kind in `Checks` when a required check has
a deferred portion. Record that portion as `R: cloud-deferred (<what>)` or
`P: hardware-deferred (<what>)` in `n/a`.

## Assurance levels and budgets

| Level | Applies to | Required checks | T1 budget | Release budget |
| --- | --- | --- | --- | --- |
| L1 | Standalone Cache, OLAP query semantics, scheduler internals, ORM clients, and streaming outside recovery | Generated unit or property checks, differential checks against a reference, fuzzing on every decoder, golden logic tests, Docker smoke checks, and paired A/B performance checks | 1,000 generated cases per property | 100,000 cases per property |
| L2 | Cache cluster, Events, OLAP commit protocol, and OLAP streaming recovery | L1 plus a model of the core protocol, simulation with fault injection, history checking, Docker and kind fault checks, and an upgrade and downgrade matrix | 500 seeds per touched workload | 200,000 clean seeds on the release SHA |
| L3 | OLTP and its LSM storage engine | L2 plus strict-serializability checking, 80% coverage in the bug-replay corpus, and mutation runs on all touched code | 2,000 seeds | 1,000,000 seeds |

The nightly budget is 160 core-hours. OLTP gets 60%, Events gets 15%,
Cache gets 15%, and OLAP and streaming share 10%. Release soaks pair 72 hours
of virtual time with parallel 5.5-hour real soaks on Actions runners.

## Check kinds

| Letter | Check |
| --- | --- |
| M | Model checking explores protocol states and invariants. |
| U | Unit or property checks exercise a component or its generated inputs. |
| S | Simulation runs seeded workloads and injects faults. |
| H | History checking judges recorded operations against a consistency or delivery rule. |
| F | Fuzzing exercises decoders and other input boundaries. |
| D | Differential checks compare results with a reference, another implementation, or another execution path. |
| R | Real-environment checks exercise running processes and integrations such as Docker, kind, or real S3. |
| P | Performance checks measure load targets or paired A/B regressions. |
| K | Soaks exercise sustained workloads and faults over virtual or real time. |

## Matrix states

The matrix has a cell for each guarantee and check kind. It combines the
register with attested evidence for the selected revision and reports the
tested SHA, date, run link, and assertion counts when available.

| State | Meaning |
| --- | --- |
| `pass` | The required assertions ran and passed on the selected SHA. |
| `fail` | An executed assertion failed. |
| `not-run` | A required check or assertion did not execute. The cell records why. |
| `quarantined n/5` | Quarantine ran the check five times and records the number of passes. The cell stays outside passing coverage. |
| `stale` | The available evidence comes from an older revision. The cell records how many commits behind it is. |
| `n/a` | The guarantee omits this check kind and records the reason. |
| `missing` | The check kind is required and has no evidence or recorded deferral. |

A required cloud- or hardware-deferred portion appears as `not-run` with
its deferral reason. Passing local cases or relative benchmarks can still
supply evidence for the portions that ran. Real S3 remains in scope and
reports `not-run: s3-unconfigured` when the test bucket is unconfigured.
The release gate requires every in-scope cell to pass on the release SHA
and lists deferrals separately.
