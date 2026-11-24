# Registered checks

Each file here registers one runnable check. `scripts/testing/run_check.py <id>` runs it and
writes one `nabedra.evidence/1` record. CI runs the same command on Actions, and
`scripts/testing/matrix.py` uses the records to fill the coverage matrix.

## File format

One TOML file per check, named `<id>.toml`.

```toml
id = "cache-store-model"            # unique kebab-case, equal to the file name
module = "cache"                    # a module name from docs/testing/modules.toml
kinds = ["U"]                    # check kinds this run provides: M U S H F D R P K T
guarantees = ["CACHE-G02", "CACHE-G03"]
scenarios = ["K01", "K02"]
tiers = ["T0", "T1"]             # any of T0 T1 T2 T3 weekly
command = ["cargo", "test", "--no-default-features", "--features", "cache", "--lib", "cache::store::model_tests"]
cwd = "server"                   # relative to the repository root
timeout_s = 900
needs = []                       # any of: docker, kind, s3, rust-nightly, node, python-orm, valkey, tlc
result_parser = "cargo-test"     # cargo-test | unittest | node-test | exit-code | tlc
env = { NABEDRA_CASES = "1000" } # optional
```

| Field | Required | Rule |
| --- | --- | --- |
| `id` | yes | Kebab-case, equal to the file name without `.toml`. |
| `module` | yes | A module in `docs/testing/modules.toml`, or `misc`. |
| `kinds` | yes | A non-empty list of distinct kind letters (plan revision 1, "check kinds"). A kind T check needs `needs = ["tlc"]` and `result_parser = "tlc"`. |
| `guarantees` | yes | Guarantee IDs from `docs/testing/guarantees/*.md`. May be empty for tooling checks. `matrix.py lint` fails on an unknown ID. |
| `scenarios` | no | Scenario IDs, copied into the record. |
| `tiers` | yes | Where the check runs. `matrix.py select` picks the T1 checks of the touched modules, including modules declared by changed check files. A removed check file selects all T1 checks. |
| `command` | yes | Argument list, run without a shell. A bare `python3` or `python` as the first argument uses the runner's interpreter (Python 3.11 or later). To choose a different interpreter, use a versioned name such as `python3.12` or an explicit path. |
| `cwd` | yes | Relative to the repository root, without `..`. |
| `timeout_s` | yes | Positive. On timeout the process group is killed and the result is `error`. |
| `needs` | no | Environment the check requires. See below. |
| `result_parser` | yes | How assertion counts are read from the output. See below. |
| `env` | no | String values added to the environment. `NABEDRA_REQUEST_ID` is always set. |
| `not_run_marker` | no | A nonblank literal string, up to 300 characters. If the command exits successfully and prints this string without any parsed test failures, the record is `not-run` with zero executed assertions. Use it only when the entire check was skipped. |

Unknown fields are errors.

## Needs

A missing need makes the result `not-run` with the reason below, and the command does not run.

| Need | Present when | Reason |
| --- | --- | --- |
| `s3` | `NABEDRA_TEST_S3_BUCKET` is set | `s3-unconfigured` |
| `kind` | a `kind` binary is on `PATH` | `kind-missing` |
| `docker` | `docker info` succeeds | `docker-unavailable` |
| `rust-nightly` | `cargo +nightly --version` succeeds | `rust-nightly-missing` |
| `node` | a `node` binary is on `PATH` | `node-missing` |
| `python-orm` | `import nabedra` succeeds with `orm/python/src` on `PYTHONPATH` | `python-orm-unavailable` |
| `valkey` | `valkey-server` is on `PATH`, or `NABEDRA_TEST_VALKEY_URL` is set | `valkey-unavailable` |
| `tlc` | the pinned TLC image is loaded, or `specs/tools/tlc --tla-ensure` builds it (Docker works) | `tlc-unavailable` |

## Result parsers

| Parser | Reads | Executed | Failed |
| --- | --- | --- | --- |
| `cargo-test` | every `test result:` line, summed | passed + failed | failed |
| `unittest` | `Ran N tests` and `OK` or `FAILED (failures=.., errors=..)` | N minus skipped | failures + errors |
| `node-test` | TAP `# pass`, `# fail`, `# cancelled`, `# skipped` | pass + fail + cancelled | fail + cancelled |
| `exit-code` | the exit status | 1 | 1 if the status is nonzero |
| `tlc` | the one `nabedra-tla-summary: {json}` line of `scripts/testing/tla_check.py` or `tla_trace.py` | the runner's assertions | the runner's failed assertions |

The result follows these rules, in order:

1. Timeout: `error`.
2. A matching `not_run_marker` with exit status zero and no parsed failures: `not-run`.
3. No summary line for the parser (for example a compile error): `fail`.
4. Any failed assertion: `fail`.
5. Nonzero exit with no failed assertion parsed: `error`.
6. Zero executed assertions: `not-run` with reason `zero-assertions`. Never `pass`.
7. Otherwise `pass`.

The `tlc` parser also takes the runner's own verdict: a `not-run` (a baseline stopped at the time
budget or a state cap) or an `error` (a TLC error) with no failed assertion is recorded as such, never
as `pass`. Its record carries a `tla` object (mode, distinct states, depth, coverage, declared limits,
mutants with their counterexample lengths, or trace counts) and lists the TLC logs and counterexamples
in `artifacts`.

It records `error` for everything it does not understand: an unknown verdict, counts that are not
`0 <= failed <= executed`, an exit status that is not the verdict's, a summary that does not start its
line, and a `pass` without `tla` facts, with `not_run` entries, or with facts that contradict it. Output
with more than one summary is `error` too, so a later pass cannot hide an earlier failure.

A T check that ran always leaves a record. When the facts exceed the schema's lengths or the 20 KB
limit, the least important go first: details of passing runs and caught mutants, artifacts that belong
to no failure, caught mutants, passing runs, then longer texts, and failures last. `tla.truncated` says
that something was dropped or cut. `tla.dropped` counts it by list, with failures apart, and the last
`not_run` entry counts the dropped ones. The command log and `tla/summary.json` stay in `artifacts`, and
`summary.json` keeps every fact. Facts the schema refuses are dropped, and the result is `error` unless
it is already `fail`.
[specs/README.md](../../../specs/README.md) describes the runners and how their results map.

Ignored, skipped and todo tests are listed in the record's `assertions.not_run`.
If a check prints one or more lines starting with `nabedra-s3-requests: <n>`,
the record stores their sum as `s3_requests`, including for failed checks.

## Running

```sh
python3.11 scripts/testing/run_check.py --list --tier T1 --module cache
python3.11 scripts/testing/run_check.py cache-store-model --dry-run
python3.11 scripts/testing/run_check.py cache-store-model --evidence-dir /tmp/evidence
```

Records go to `docs/testing/evidence/records/<yyyy>/<mm>/<request_id>--<id>.json` unless
`--evidence-dir` is given. The log goes to `target/testing-logs/<request_id>--<id>.log` (or
`--log-dir`), and its SHA-256 is listed in the record's `artifacts`. The request ID comes from
`--request-id`, then `NABEDRA_REQUEST_ID`, then `local-<short sha>-<hhmmss>`. The exit status is 1
for `fail` and `error`, and 0 otherwise. `--strict` also exits 2 for `not-run`.

Each record includes the UTC start time as `run.date`. The matrix counts clean-tree records
from HEAD or its ancestors after attestation verification. Use `matrix.py build --verify-attestations`
to verify records when generating the matrix. The gate verifies attestations by default and
requires a pass on the requested SHA for each required cell. Use `--allow-unattested` for local fixtures.
