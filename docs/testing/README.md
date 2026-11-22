# Testing system

Each module lists its guarantees and required check kinds in a register.
The runner reads each check's definition and writes an evidence record.
The coverage matrix shows the result for each guarantee and check kind,
along with the tested commit.

```text
register (guarantee, required kinds) -> check file -> run_check.py -> evidence record -> matrix
```

A guarantee is verified only by attested evidence on the SHA being shipped. Simulation evidence is
labelled as a fault model and does not replace the real-environment checks the register requires.

## Where things live

| Path | Content |
| --- | --- |
| `docs/testing/guarantees/<module>.md` | One register per module: guarantee IDs, failure models, scenarios, required check kinds, `n/a` reasons and the assurance level |
| `docs/testing/modules.toml` | Path globs to modules, modules to guarantees, hot paths, and the nightly budget split |
| `docs/testing/checks/<id>.toml` | One runnable check: command, kinds, guarantees, scenarios, tiers and needs |
| `docs/testing/evidence/records/` | Evidence records, one JSON file per run and check, with the schema in `docs/testing/evidence/schema.json` |
| `docs/testing/coverage-matrix.md` and `.json` | The matrix, regenerated after each merge |
| `docs/testing/catalog.json` | The test catalog, which tells the repetition gate which tests are new |
| `docs/testing/bugs/<date>-<slug>.toml` | One record per bug, with the invariant it added |
| `docs/testing/perf/` | Paired A/B specs and results |
| `server/check` | The `nabedra-check` crate: seeded generators, shrinking, the model checker, history checkers and the evidence writer |
| `server/fuzz` | Fuzz targets for decoders and parsers (nightly toolchain, Actions only) |
| `scripts/testing/` | The stdlib Python tools below |

| Tool | Purpose |
| --- | --- |
| `run_check.py` | Run one check, or list a tier's checks, and write the evidence record |
| `matrix.py` | Select the checks for a diff, build the matrix, and gate a release SHA |
| `catalog.py` | Regenerate the test catalog |
| `repeat_gate.py` | Repeat new and changed tests before they count |
| `triage.py` | Rerun a real-environment failure under the statistical rule |
| `ab.py` | Paired A/B performance runs of base against head on one runner |
| `pr_lint.py` | Check the four required lines of a pull request body |
| `licenses.py` | Fail on a dependency license outside the allowlist |

### Check kinds

| Kind | Meaning |
| --- | --- |
| M | Model check of a protocol |
| U | Unit or property test |
| S | Simulation workload with a nemesis |
| H | History check of recorded client operations |
| F | Fuzzing |
| D | Differential test against a reference implementation |
| R | Real environment: processes, Docker, kind or real S3 |
| P | Performance, paired A/B only |
| K | Soak |

### Assurance levels and tiers

Each register declares a level. L1 covers standalone engines and clients, L2 covers replicated and
coordinated protocols, and L3 covers the strictly serializable transactional path. The level sets
the required kinds and the seed budgets
([details](../../.agents/skills/code-review/references/guarantees.md#assurance-levels)).

| Tier | Where | When |
| --- | --- | --- |
| T0 | Your machine | Before each commit: in-process model, property and logic tests of the touched modules |
| T1 | GitHub Actions | On the final SHA before review: the checks `matrix.py select` picks, the repetition gate, and paired A/B for hot paths |
| T2 | GitHub Actions | Nightly, pinned to the SHA of `main` when it starts |
| Weekly | GitHub Actions | The upgrade and downgrade matrix |
| T3 | GitHub Actions | On a release SHA: every in-scope cell green. The release gate lists deferred cloud and hardware checks. |

Real-environment compute runs in Docker and kind. Checks that need cloud
compute show as `not-run: cloud-deferred`. Real S3 checks use the dedicated
test bucket.

## Quickstart on a fresh laptop

These commands require the testing tools, check files and workflow inputs
on your checkout. If a tool or workflow input is unavailable, record the
affected check as not run and give the reason.

### 1. Clone and check the tools

```sh
git clone https://github.com/zPy52/nabedra-database.git
cd nabedra-database
python3 --version        # 3.11 or later
```

The testing scripts use only the Python standard library, so there is nothing to `pip install`.
They need Python 3.11 or later for `tomllib`. On macOS, `brew install python@3.11` and use
`python3.11` if the default `python3` is older: the commands below say `python3`, so substitute
`python3.11` in each one. `rustup` installs the pinned Rust toolchain from
`rust-toolchain.toml` on the first `cargo` command. For GitHub Actions, install the GitHub CLI and
run `gh auth login`.

### 2. Run T0 locally

List the T0 checks and run the ones for the module you are changing:

```sh
python3 scripts/testing/run_check.py --list --tier T0
python3 scripts/testing/run_check.py cache-store-model
```

Each run writes an evidence record, under `docs/testing/evidence/records/` unless you pass
`--evidence-dir` (the directory appears with the first record). Use `CARGO_BUILD_JOBS=2` on a small
machine and keep local property and simulation runs to one test thread.
Use local records to investigate failures. Verification requires records
attested by the workflow.

A check whose needs are missing reports `not-run` with the reason. For example, an S3 check reports
`not-run: s3-unconfigured` until `NABEDRA_TEST_S3_BUCKET` is set.

### 3. See which checks your change needs

```sh
python3 scripts/testing/matrix.py select --base "$(git merge-base origin/main HEAD)" \
  --head "$(git rev-parse HEAD)"
```

It prints the touched modules, their assurance level, the guarantee IDs a review must cover and the
T1 check set.

### 4. Dispatch T1 on GitHub Actions

A push alone starts no checks. Push your branch, then dispatch the checks
that `matrix.py select` returned. Use their IDs in the `checks` field. The
example dispatches one ID.

```sh
SHA=$(git rev-parse HEAD)
BRANCH=$(git branch --show-current)
git push origin "$BRANCH"
RID="testing-$(git rev-parse --short=7 HEAD)-$(date +%H%M%S)"
gh workflow run nabedra-build.yml --ref "$BRANCH" -f sha="$SHA" -f request_id="$RID" \
  -f base=false -f checks=cache-store-model
```

[GitHub Actions for checks](../../.agents/skills/code-review/references/gh-actions.md) explains how
to find the run by its request ID, wait without blocking, read the raw results and record them.

### 5. Read the nightly run

The nightly run is scheduled. It resolves `main` to a SHA when it starts and runs the nightly set
on that SHA. List recent runs with:

```sh
gh run list -e schedule -L 5
```

The evidence workflow imports attested records and opens an evidence-only PR. Once on `main`,
those records feed the coverage matrix and the S3 budget. Use `gh workflow run nabedra-evidence.yml
--ref main -f run_id=<id>` to retry a completed build run.

The workflow can publish only when two repository settings are on. A repository admin switches
them on in the GitHub web interface:

| Setting | Where | Without it |
| --- | --- | --- |
| Allow GitHub Actions to create and approve pull requests | Settings, Actions, General, Workflow permissions | The job pushes `evidence/<run id>` but cannot open the PR |
| Allow auto-merge | Settings, General, Pull Requests | The job opens the PR but cannot merge it |

When either setting is off, the job fails with an `Evidence not published` error that names the
setting, and the run summary says how to retry. The verified records wait on the `evidence/<run id>`
branch, and a retry reuses it after its paths pass the same check. When the PR has no pending checks,
GitHub refuses to queue auto-merge, so the job merges it directly at the checked head commit.

### 6. Read the matrix

Open `docs/testing/coverage-matrix.md`. It has one cell per guarantee and required check kind, and
each cell shows its state, SHA, date, run link and counts.

| State | Green |
| --- | --- |
| `pass` | yes, for that SHA only |
| `fail` | no |
| `not-run: <reason>` | no, whatever the reason |
| `quarantined n/5` | no |
| `stale (<n> behind)` | no |
| `n/a` | not required, with the register's reason |
| `missing` | no |

`python3 scripts/testing/matrix.py gate --sha <sha> --scope release` fails unless every in-scope
cell passed on that SHA.

### 7. Replay a failing seed

A failing property or simulation prints a replay line:

```text
replay: NABEDRA_SEED=<seed> NABEDRA_CASES=1
```

Put the assignments after `replay:` in front of the same test command.
For properties, this regenerates the original input and runs shrinking
again. If the same seed and configuration pass on replay, report a
determinism bug. See [simulation](../../.agents/skills/code-review/references/simulation.md).

### 8. Run the paired A/B runner

Performance claims come only from `scripts/testing/ab.py`. It builds the base and head commits,
runs them alternately on one machine from a spec in `docs/testing/perf/`, and reports median ratios
with a bootstrap threshold. A regression is 5% or more and above the threshold. Run
`python3 scripts/testing/ab.py --help` for its arguments. Absolute numbers from shared runners or
a laptop are not evidence.

### 9. Use the S3 test bucket

Real S3 checks write only to one dedicated test bucket, under `runs/<request_id>/`, so parallel runs
never collide. The bucket deletes objects and aborts incomplete multipart uploads after one day.
If the dedicated bucket is available, export `NABEDRA_TEST_S3_BUCKET` and
use your usual AWS credentials. On Actions, only dispatched and scheduled
jobs can request the bucket-scoped role through GitHub OIDC. See
`scripts/testing/s3/README.md` for setup.

### 10. Publish workflow ledgers

Run `python3 scripts/testing/ledger_sync.py <name>` from the checkout containing
`.nabedra/workflows/<name>/`. It publishes UTF-8 Markdown and text in that directory to the orphan
`ledgers` branch on origin. The branch is public. Its history preserves briefs and reports after
local worktrees are removed.

## Rules that apply to every change

- The implementer never records its own work as verified. Tests come from an adversarial tester on
  a different model, and a non-author reviewer approves.
- A new test counts only after it fails on a mutant or a reverted fix, and after it passes the
  repetition gate.
- A crash never reruns. Other real-environment failures rerun only under the statistical rule.
- Every bug goes through the [bug loop](../../.agents/skills/code-review/references/bug-loop.md)
  and leaves a bug record and a new invariant.
- Every pull request body has `Guarantees:`, `Evidence:`, `Not run:` and `Reviewers:` lines.

The full protocol is in the [code-review skill](../../.agents/skills/code-review/SKILL.md).
