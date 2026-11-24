# Evidence

A guarantee is verified only by attested evidence on the SHA being shipped.

## Checks

Each runnable check is one file, `docs/testing/checks/<id>.toml`. The `id`
equals the filename stem.

```toml
id = "cache-store-model"
module = "cache"
kinds = ["U"]                    # M U S H F D R P K T
guarantees = ["CACHE-G02", "CACHE-G03"]
scenarios = ["K01", "K02"]
tiers = ["T0", "T1"]             # T0 T1 T2 T3 weekly
command = ["cargo", "test", "--no-default-features", "--features", "cache", "--lib", "cache::store::model_tests"]
cwd = "server"                   # relative to the repository root
timeout_s = 900
needs = []                       # docker, kind, s3, rust-nightly, node, python-orm, valkey, tlc
result_parser = "cargo-test"     # cargo-test, unittest, node-test, exit-code or tlc
env = { NABEDRA_CASES = "1000" } # optional
```

`docs/testing/checks/README.md` describes every field.

## Run a check

Set `CHECK_ID` to the check you want to run.

```sh
python3 scripts/testing/run_check.py --list --tier T0   # the checks in a tier
python3 scripts/testing/run_check.py "$CHECK_ID"        # run one and write its record
```

`run_check.py` runs the check's command, parses its results with `result_parser` and writes one
record. A check whose `needs` are missing writes `not-run` with the reason instead of failing. For
example, without `NABEDRA_TEST_S3_BUCKET` an S3 check writes `not-run: s3-unconfigured`. On
Actions, the `checks` input of `nabedra-build.yml` runs a comma-separated
list of check IDs. Rust tests can write records through
`nabedra_check::evidence::Record`.

## Record format

One JSON file per run and check, at
`docs/testing/evidence/records/<yyyy>/<mm>/<request_id>--<check>.json`, under 20 KB. The schema is
`docs/testing/evidence/schema.json`.

| Field | Content |
| --- | --- |
| `schema` | `nabedra.evidence/1` |
| `check`, `kind` | the check ID and its kind letter |
| `guarantees`, `scenarios` | the IDs the check covers |
| `sha`, `tree_clean` | the full 40-character commit, and whether the tree had uncommitted changes |
| `build` | profile, features and the binary's SHA-256 |
| `mode` | the run mode, such as `direct_oltp` |
| `run` | run URL, attempt, request ID and runner |
| `duration_s` | wall time |
| `result` | `pass`, `fail`, `not-run`, `quarantined` or `error` |
| `assertions` | executed, failed, and `not_run` with a reason for each |
| `seeds` | range, count and failing seeds |
| `sim_core_hours`, `virtual_hours` | simulation cost and simulated time |
| `tla` | kind T facts: spec, mode (`exhaustive`, `simulation` or `trace`), tier, states, coverage, declared limits, mutants or trace counts, and `truncated` with `dropped` counts when the record was trimmed |
| `artifacts` | artifact names with their SHA-256s |

Logs and histories stay in Actions artifacts. A record stays valid after its artifacts expire.

## Attestation

The workflow signs each record it writes with `actions/attest-build-provenance`. The matrix accepts
only records whose signatures verify, so a hand-written or locally produced record never counts.
Local records help the author iterate. They are not evidence of verification. To check a record by
hand, run `gh attestation verify <record> --repo zPy52/nabedra-database`. Release evidence is also
attached to the GitHub release.

After a build dispatch or schedule completes, `nabedra-evidence.yml` downloads its evidence
artifacts, checks each record against the tested revision, and verifies its attestation against
the build run. It opens a pull request with the verified records and regenerated matrix. The
workflow checks the diff before requesting auto-merge. Merged records feed the matrix and the
S3 budget.

To retry a run, dispatch `nabedra-evidence.yml` with `run_id`. For local inspection, use
`python3 scripts/testing/ingest_evidence.py --run <id> --dest <scratch-directory>`. The command
writes a scratch matrix in the sibling `<scratch-directory>-matrix`. `--no-verify` saves raw
records with a `.json.unattested` suffix that the matrix and budget skip.

## Matrix states

`scripts/testing/matrix.py` reads the registers and the attested records and writes
`docs/testing/coverage-matrix.md` and `.json`. Each cell shows its state, SHA, date, run link and
counts.

| State | Meaning | Counts as green |
| --- | --- | --- |
| `pass` | The required assertions executed and passed on the cell's SHA | yes, for that SHA only |
| `fail` | An assertion failed | no |
| `not-run: <reason>` | The check did not execute, for example `s3-unconfigured`, `cloud-deferred` or `hardware-deferred` | no |
| `quarantined n/5` | The check is in quarantine, with its pass count | no |
| `stale (<n> behind)` | The last record is for an older commit | no |
| `n/a` | The register gives a reason the kind does not apply | not required |
| `missing` | No record exists | no |

A T cell is `pass` only with a passing exhaustive T0 or T1 record on its SHA, and with no exhaustive T0
or T1 record on that SHA that is `not-run`. A `not-run` record without `tla` facts, such as
`tlc-unavailable`, counts as open until the same check passes. Simulation, trace and T2 records can turn
the cell red, but never green, and facts that contradict a record's `pass` turn it red. When two design
checks cover a guarantee, one `not-run` is enough to keep the cell from green. As for every kind, the
matrix sees only records: a check with no record on the SHA goes unnoticed if another one passed.

`not-run` never counts as green, whatever its reason. A deferred kind stays
in the register's Checks column and appears as `not-run`, with the deferral
reason. The release gate lists deferred cells by name.
`matrix.py gate --sha <sha> --scope release` requires a pass on that SHA
for every in-scope cell. The lead regenerates the matrix and the test
catalog (`docs/testing/catalog.json`) after each merge. Both are hot files.

## Report evidence

In a report or pull request, the `Evidence:` line lists each command or run URL with its raw result
lines, and the `Not run:` line lists every selected check that did not execute. Use the record block
in [GitHub Actions for checks](gh-actions.md#record-the-evidence) for runs on Actions. Label
simulation results as a fault model. They never stand in for an R check the register requires.

## Ledgers

Workflow ledgers, briefs and reports start in `.nabedra/workflows/<name>/`. Run
`python3 scripts/testing/ledger_sync.py <name>` from the checkout containing that directory to
publish its UTF-8 Markdown and text to an isolated worktree of the orphan `ledgers` branch on
origin. The sync copies eligible files up to 256 KiB each and 2 MiB total and preserves remote
files absent locally. The branch is public and pushes start no CI.
