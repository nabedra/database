# Evidence

A guarantee is verified only by attested evidence on the SHA being shipped. This page says what a
record holds, how one is produced and signed, and how the coverage matrix reads records.

## Checks

Each runnable check is one file, `docs/testing/checks/<id>.toml`, whose `id` equals the file name:

```toml
id = "kv-store-model"
module = "kv"
kinds = ["U"]                    # M U S H F D R P K
guarantees = ["KV-G02", "KV-G03"]
scenarios = ["K01", "K02"]
tiers = ["T0", "T1"]             # T0 T1 T2 T3 weekly
command = ["cargo", "test", "--no-default-features", "--features", "cache", "--lib", "cache::store::model_tests"]
cwd = "server"                   # relative to the repository root
timeout_s = 900
needs = []                       # docker, kind, s3, rust-nightly, node, python-orm, valkey
result_parser = "cargo-test"     # cargo-test, unittest, node-test or exit-code
env = { NABEDRA_CASES = "1000" } # optional
```

`docs/testing/checks/README.md` describes every field.

## Run a check

```sh
python3 scripts/testing/run_check.py --list --tier T0   # the checks in a tier
python3 scripts/testing/run_check.py <id>               # run one and write its record
```

`run_check.py` runs the check's command, parses its results with `result_parser` and writes one
record. A check whose `needs` are missing writes `not-run` with the reason instead of failing. For
example, without `NABEDRA_TEST_S3_BUCKET` an S3 check writes `not-run: s3-unconfigured`. On
Actions, the `checks` input of `nabedra-build.yml` runs a comma-separated list of check IDs the
same way. Rust tests write records through `nabedra_check::evidence::Record`.

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
| `artifacts` | artifact names with their SHA-256s |

Logs and histories stay in Actions artifacts. A record stays valid after its artifacts expire.

## Attestation

The workflow signs each record it writes with `actions/attest-build-provenance`. The matrix accepts
only records whose signatures verify, so a hand-written or locally produced record never counts.
Local records help the author iterate. They are not evidence of verification. To check a record by
hand, run `gh attestation verify <record> --repo zPy52/nabedra-database`. Release evidence is also
attached to the GitHub release.

## Matrix states

`scripts/testing/matrix.py` reads the registers and the attested records and writes
`docs/testing/coverage-matrix.md` and `.json`. Each cell shows its state, SHA, date, run link and
counts.

| State | Meaning | Counts as green |
| --- | --- | --- |
| `pass` | Assertions executed and passed on the cell's SHA | yes, for that SHA only |
| `fail` | An assertion failed | no |
| `not-run: <reason>` | The check did not execute, for example `s3-unconfigured`, `cloud-deferred` or `hardware-deferred` | no |
| `quarantined n/5` | The check is in quarantine, with its pass count | no |
| `stale (<n> behind)` | The last record is for an older commit | no |
| `n/a` | The register gives a reason the kind does not apply | not required |
| `missing` | No record exists | no |

`not-run` never counts as green, whatever its reason. `matrix.py gate --sha <sha> --scope release`
fails unless every in-scope cell passed on that SHA. The orchestrator regenerates the matrix after
each merge, and the test catalog (`docs/testing/catalog.json`, written by
`scripts/testing/catalog.py`) with it. Both are hot files.

## Report evidence

In a report or pull request, the `Evidence:` line lists each command or run URL with its raw result
lines, and the `Not run:` line lists every selected check that did not execute. Use the record block
in [GitHub Actions for checks](gh-actions.md#record-the-evidence) for runs on Actions. Label
simulation results as a fault model. They never stand in for an R check the register requires.

## Ledgers

Workflow ledgers, briefs and reports live in `.nabedra/workflows/`, a worktree of the orphan
`ledgers` branch. Pushing that branch starts no CI, and ledgers survive deleted worktrees. Keep
them out of product pull requests.
