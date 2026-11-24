# GitHub Actions for checks

Every agent, review and workflow in this repository runs heavy tests and checks on GitHub Actions
this way. [Nabedra builds](../../nabedra-builds/SKILL.md) says what stays local and what goes to
Actions. This page says how to run, find, read and record a run when many agents share the
repository.

The repository is public, so standard GitHub-hosted runners cost nothing
([billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions)). The limits
that matter are elsewhere:

- 20 concurrent jobs for the whole account
  ([limits](https://docs.github.com/en/actions/reference/limits)).
- A 10 GB cache shared by every branch. Entries unused for 7 days are evicted, and a run restores
  only from its own branch and `main`
  ([caching](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)).
- One API rate limit (5,000 REST requests an hour) shared by every agent using the user's token
  ([rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)).

## What starts a run

Start Actions workflows manually. Use `nabedra-build.yml` for development checks and
for broad verification on the resulting `main` SHA after a batch of PRs merges.
Each dispatch is pinned to a commit and has its own concurrency group.

Start `nabedra-nightly.yml` manually to dispatch the T2, fuzz, feature, and
quarantine preset on `main`. Start `nabedra-evidence.yml` manually with the completed
build run ID to ingest evidence.

Use `python3 scripts/testing/matrix.py select --base <sha> --head <sha>` for the diff
report. Dispatch relevant checks with `base=false` for focused feedback.

## Rules when many agents share Actions

- Give every dispatch its own request ID: `<task>-<short sha>-<hhmmss>`.
- Cancel (`gh run cancel`) or rerun only runs you dispatched. Never wait on, cancel or report
  someone else's run as yours.
- Keep at most one full run per branch in flight. Dispatch only the jobs you need while iterating
  (`base=false` with `scenarios` or `tests`), and the full set once when the change needs it.
- A new workflow cannot be dispatched until it reaches `main`.
  Coordinate new jobs and inputs with the workflow file's owner (see
  [hot files](../../nabedra-change/references/subagent-driven-development.md#prepare-the-work)).
- Keep working while a run is queued or running. A queued job waits for a free runner; it is never
  cancelled for being queued.

## Choose what to run

The workflow is `.github/workflows/nabedra-build.yml`. Its dispatch inputs:

| Input | Meaning |
| --- | --- |
| `sha` (required) | Full 40-character commit to test. It must be pushed. |
| `request_id` (required) | Your request ID, shown in the run name. |
| `base` (default true) | kv (clippy, tests), features (O02, NS26, including the absence of `test-hooks`), Cache library tests, two shards of default-feature OLAP library, integration and streaming tests, `nabedra-check`, OLTP library and integration tests, Events tests, and runtime-seam Clippy. |
| `jobs` | Comma-separated opt-in jobs: `direct` (D01-D12, ORM suites, library suite off, acceptance, one leg per mode; implies `rep1`, `growth`, `o02`), `acceptance` (Linux OLTP acceptance, default mode; implies `growth`, `o02`), `o02` (O01 and O02 with an OLAP server and the engine matrix, plus T02), `growth` (O16, O26 on the shared build; O21, O28 on one opt-level-1 build), `rep1` (REP1-REP5 process suites), `rep4-mixed` (mixed-version enrollment), `o22` (adds O22 to `scenarios`), `blackhole` (AZ1/AZ2 packet-drop failover), `migrate` (MIG1/MIG2 upgrade), `live3`, `live3-fixture`, `campaign`, `docker`, `memory`, `bench`, `perf`, `g7`, `gc1`, `perf1`, `snap1`, `engine-tests` (Cache and OLAP suites), `check-tests` (shared Rust crates), `features` (engine feature checks and S3 loopback), `clippy`, `quarantine`, `fuzz`, `mutants`. Unknown names fail the run, and the summary lists the resolved set. |
| `checks` | Comma-separated IDs from `docs/testing/checks/*.toml` on the tested SHA. The `checks` job runs one matrix leg per ID and installs only the declared requirements. |
| `fuzz_seconds` (default 300) | Seconds per target for `jobs=fuzz`. The nightly sets 3600. |
| `scenarios` | Comma-separated OLTP acceptance scenarios (O01-O29) run on the shared build, one job per scenario and mode. |
| `tests` | Space-separated full library test paths (`module::tests::name`) run on the shared build. |
| `repeat` (default 1) | Runs of each `tests` entry. Use 20 or more for flake evidence. |
| `modes` (default `on,off`) | direct_oltp modes for `direct`, `scenarios` and `tests`. |

The `build` job shares one debug OLTP build with the jobs that use it. A dispatch that only
repeats exact library tests builds the server and library test executables. The full OLTP
suite and acceptance jobs also build the integration test executables.

The `fuzz` job runs each target in `server/fuzz/fuzz_targets/` with a pinned Rust nightly. Crashes
are available in `fuzz-crashes-<target>`. The `mutants` job runs cargo-mutants on Rust changes in
the server workspace since the merge base with `main`, and uploads `mutants-diff`.

The `checks` job installs only the tools a check declares in `needs`, plus Python 3.11. It does not
build binaries or install packages for the check. A check that needs a binary, such as a release
server or a client wheel, builds or installs it in its own command, for example a script under
`scripts/testing/` that runs `cargo build` and then the suite. The job's time limit is the check's
`timeout_s` plus 30 minutes for setup and that build, capped at six hours.

The job runs `run_check.py --strict`, so a check that reports `not-run`, for a missing need or zero
executed assertions, fails its job, and the job summary prints the record's result and reasons. A
green `Check <id>` job therefore means an executed pass. While the S3 repository variables are unset,
the nightly leaves out checks that need `s3` instead of dispatching them to fail.

Each registered check uploads its `evidence/` directory and command logs as `evidence-<id>`. GitHub
attests the JSON records on deliberate and nightly dispatches. The `checks` matrix takes up to
eight runners, or six with two reserved for the `fuzz` matrix.

A check that declares `s3` uses GitHub OIDC only on deliberate or nightly dispatches. Configure
`NABEDRA_TEST_S3_ROLE_ARN` and `NABEDRA_TEST_S3_BUCKET` as repository variables, with
`NABEDRA_TEST_S3_REGION` if the bucket is outside `us-east-1`. Scope the role to the dedicated test
bucket, and write under `runs/<request_id>/`. A missing role or bucket produces
`not-run: s3-unconfigured`. The PR gate has no OIDC token or bucket credentials. In the role's
trust policy, require `token.actions.githubusercontent.com:sub` to match this repository's branch
refs (`repo:<owner>/<repo>:ref:refs/heads/*`), so a `pull_request` token cannot assume it.

With `jobs=docker`, the Events image job also runs the kind Events suite after NS27 when
`scripts/testing/kind/suite_events.py` exists. That step does not fail the job until it has been
verified on Actions.

## Dispatch, pinned to your commit

```sh
SHA=$(git rev-parse HEAD); BRANCH=$(git branch --show-current)
git push origin "$BRANCH"                    # the commit must exist on GitHub
RID="<task>-${SHA:0:7}-$(date +%H%M%S)"
gh workflow run nabedra-build.yml --ref "$BRANCH" -f sha="$SHA" -f request_id="$RID" \
  -f base=false -f scenarios=O26,O21         # or: -f jobs=direct,acceptance (base stays on)
```

The code under test is `sha`. The job definitions come from the head of `--ref` at dispatch time.
So a commit that changes the workflow must be the branch head when you dispatch it. Later pushes
to the branch change neither. A typical iteration is `-f base=false -f tests="<path>" -f repeat=5`
or `-f base=false -f scenarios=O07`. Run the required verification on the final commit. For a change that needs the full OLTP
acceptance suite, use `-f jobs=direct,acceptance`.

## Find your run

`gh workflow run` may not print the run URL, so find the run by its request ID:

```sh
for i in $(seq 10); do
  RUN=$(gh run list -w nabedra-build.yml -e workflow_dispatch -b "$BRANCH" -L 30 \
    --json databaseId,displayTitle --jq ".[] | select(.displayTitle | contains(\"$RID\")) | .databaseId")
  [ -n "$RUN" ] && break; sleep 6
done
echo "https://github.com/zPy52/nabedra-database/actions/runs/$RUN"
```

The run name is `<request_id> @ <sha>`. The run's own `headSha` is the branch head at dispatch
time, which can differ from `sha`. Trust the `Tested revision` line in the run summary.

## Wait without blocking

Start the watch as a background command and keep working on something else. When it exits, read the
results:

```sh
gh run watch "$RUN" --interval 60 --exit-status > "ci-$RID.txt" 2>&1
```

Without background commands, check between tasks with
`gh run view "$RUN" --json status,conclusion`. Poll at most once a minute: every agent shares one
API limit. Never use `gh run watch` with its default 3-second interval.

## Read the results

```sh
gh run view "$RUN"                                        # summary, jobs, annotations
gh run view "$RUN" --json jobs --jq '.jobs[] | [.databaseId, .name, .conclusion] | @tsv'
gh run view "$RUN" --log-failed | grep -E 'test result:|FAILED|panicked|::error'
gh run download "$RUN" -n oltp-test-logs -D "ci-$RID"     # artifacts: logs, results.json
```

Each job's summary lists what it ran: the tested revision, the quarantine table, acceptance
scenarios with passed, failed or not-run, and pass counts for repeated tests. Read the raw
`test result:` lines and `results.json`, not the job colour. Acceptance exits 0 with `not-run`
scenarios listed: a green job does not mean they passed.

## Rerun one failed job

```sh
gh run rerun "$RUN" --job <databaseId>       # that job and the jobs that need it
gh run rerun "$RUN" --failed                 # every failed job
```

A rerun tests the same commit with the same job definitions. Use it for infrastructure failures, or
to measure a suspected flake. After a code change, dispatch a new run on the new commit instead. A
run can be rerun at most 50 times.

## Record the evidence

Put this in the report, ledger or pull request for every check you rely on:

```text
CI: https://github.com/zPy52/nabedra-database/actions/runs/<id> (attempt N), request <request_id>
Tested revision: <40-char sha>
Jobs: oltp success, direct (on) success, direct (off) failure, ...
Raw: test result: ok. 499 passed; 0 failed; 9 ignored; ...   (one line per suite)
Scenarios: O07 passed 4/4 clauses (on, off); O01 4/6 clauses, not-run: <clauses>
Not run: <every selected check whose assertions did not execute, and why>
Quarantined (not counted): <ids and their counts from the quarantine job>
```

A scenario or test counts as passing only when its assertions executed on the recorded revision
(see [the scheduler test contract](../../../../docs/testing/scheduler-contract.md)). Keep `not-run`
and quarantined items in their own lines. If a job went green on a rerun, record both attempts.

## Quarantine

`.github/quarantine.tsv` lists known-flaky and known-failing tests and scenarios. Blocking jobs skip
them. The manually dispatched `quarantine` job is non-blocking and reports the counts
from 5 runs per test and both modes per scenario. Each row has `id` (a full test path or a scenario ID), `kind`, `owner`, `added`,
`expires` and `exit_criteria`. `python3 scripts/ci_quarantine.py check` validates the file.

- **Who quarantines.** The orchestrator owns the file: the lead of the current wave, or the single
  agent doing the work when there is no lead. Others propose a row to the orchestrator, with run
  URLs and failure counts.
- **What qualifies.** Only a failure that reproduces without the change under test, or fails
  intermittently on the same commit. Never quarantine a test your own change broke.
- **Owners.** The orchestrator gives every row an owner (a task, agent or thread that will fix it)
  and an expiry no more than 14 days out. When it adds a row it starts or assigns that fix task. At
  the start of each wave it reviews the file:
  - it assigns owners to `unassigned` rows;
  - it fixes, extends (with the reason in `exit_criteria`) or retires expired rows;
  - it asks the user before deleting a test.
- **Leaving quarantine.** The owner fixes the root cause, with a regression test that failed before
  the fix, and shows 20 of 20 passes (`-f tests=<path> -f repeat=20`, or the quarantine job). The
  fix removes the row in the same change.
- **Reporting.** A quarantined check is reported as quarantined with its counts, never as passing.

## Changing the workflow

- **It is a hot file.** The workflow file and `quarantine.tsv` each have one owner per wave.
- **Testing a change.** To test a workflow change, push it and dispatch on that branch. Job
  definitions and inputs come from the branch's copy. Dispatch itself only needs the workflow on
  `main`, which it is.
- **Inputs.** Keep to 25 inputs or fewer, and pass free-form inputs to scripts through `env:`,
  never by `${{ }}` inside `run:`.
- **Refreshing caches.** Caches are saved only from dispatches on `main`. After a merge that
  changes `Cargo.lock`, a manifest or the toolchain, dispatch the base jobs on `main` to refresh
  them: `gh workflow run nabedra-build.yml --ref main -f sha=$(git rev-parse origin/main) -f request_id=cache-<sha7>`.
