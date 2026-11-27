# Bug records

The original hardening reports were lost when their worktree was deleted. These
records draw on fix commits, pull requests [#27], [#23] and [#20], and the
[OLTP test evidence]. Each record of a fixed bug cites its fix by full Git
commit SHA. Where an earlier source survives, the record also names the PR or
evidence file.
Several failures share a fix commit.

[OLTP test evidence]: ../oltp-evidence.md
[#27]: https://github.com/nabedra/database/pull/27
[#23]: https://github.com/nabedra/database/pull/23
[#20]: https://github.com/nabedra/database/pull/20

## Schema

Use one TOML file per bug, named `<date>-<slug>.toml`. The date is the
earliest documented finding when known, or a cited fix's author date
when the discovery date is unknown. Keep the record ID stable across edits.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | String | Unique bug ID. Keep a historical ID when available. |
| `title` | String | Observable failure. |
| `class` | String | One of the classes below. |
| `module` | String | Module where the failure occurred. Use `testing` for fixture failures. |
| `guarantees` | Array of strings | IDs defined in the [guarantee registers](../guarantees/), such as `OLTP-G04`. |
| `scenarios` | Array of strings | Relevant contract scenario IDs, such as `D04` or `R04`. |
| `found_by` | String | `acceptance`, `review`, `campaign`, `ci-flake`, `user`, or `unknown`. |
| `found_date` | String | ISO calendar date of the earliest documented finding, or `unknown`. |
| `lowest_regression` | String | Smallest known regression, as a test path, a path with a test name, or a scenario ID. |
| `seed` | Integer or string | Nonnegative failing seed, or `unknown`. |
| `why_not_caught` | String | One sentence about the coverage gap, when the evidence identifies one. |
| `invariant_added` | String | The condition enforced by the regression or intended for replay. |
| `fix_commits` | Array of strings | One or more full Git commit SHAs, newest first for replay. `[]` only on an [open record](#open-records). |
| `revert_patch_ok` | Boolean | The listed reverts produced a nonempty patch without conflicts at the baseline. `false` on an open record. |
| `status` | String | `open` on an open record. Omit it on a fixed one. |
| `owner` | String | Open records only. The task, agent or person that will resolve the bug. |
| `expires` | String | Open records only. ISO calendar date, the last UTC day on which the record is valid. |

Use `"unknown"` for unknown metadata and `["unknown"]` when an array's
contents are unknown. Use `[]` when the bug has no applicable guarantee or
scenario. Keep all fields present, except `status`, `owner` and `expires`,
which only open records have. The backfill uses `bdf8d679` for the
baseline. On a fixed record, a false revert flag means the reverts conflicted
or produced no patch there. Fix commits that also address other bugs can produce the same
patch in several records.

## Open records

Since 2026-11-24 a bug that has no fix yet can have a record. Set
`status = "open"`, `fix_commits = []` and `revert_patch_ok = false`, and name
an `owner` and an `expires` date:

```toml
fix_commits = []
revert_patch_ok = false
status = "open"
owner = "<task, agent or person>"
expires = "<YYYY-MM-DD>"
```

- A record without `status = "open"` needs at least one fix commit, and an
  open record must not have one. `fix_commits = ["unknown"]` is invalid on
  both, and so is the owner `unknown`.
- The `testing-bugs` check fails from the day after `expires`. Fix the bug, or
  renew the expiry.
- When the fix lands, add its commit to `fix_commits`, set `revert_patch_ok`,
  and remove `status`, `owner` and `expires`.
- An open record never counts as fixed, replayed or detected. The summary
  counts it in its own column. It has no fix to revert, so `sim_bugs.py`
  rejects a manifest that names it.

## Classes

| Class | Replay invariant |
| --- | --- |
| `uncertain-outcome` | A definite refusal needs proof of non-application. A retry preserves uncertainty and replays a committed result by identity. |
| `stale-route-topology-schema` | Each commit uses the schema epoch in effect at its timestamp. Later covering index scans include acknowledged writes, and retired leaders cannot serve success. |
| `fence-lifecycle-order` | A fence takes effect after validation. Lifecycles follow declared transitions and finish within a bounded time after faults heal. |
| `membership-replacement-trust` | Membership transitions preserve Raft safety. An empty disk cannot vote as its old identity, and a retry can certify only the replacement it names. Capabilities must be advertised before use, and capacity limits must preserve progress. |
| `resource-release-order` | Each acquisition has one release, after the guarded work finishes. Counters return to baseline when work settles. |
| `other` | The record states the assertion for the specific bug, including failures in test fixtures. |

## Bug loop

When a bug is found, retain its reproducer, seed and failing assertion. Add
the smallest regression that exercises it, cite the fix, and record the
invariant that a simulation or history checker can enforce. Use a fresh
scratch worktree for `git revert --no-commit` against the corpus baseline,
then abort and restore that worktree before checking the next record.

The planned simulator corpus under `server/sim/bugs/` uses the revert patches
to test the release gate. A clean revert makes a patch eligible for the
corpus. Count a bug as rediscovered only when a replay executes the assertion
and detects the recorded failure. The release target is to rediscover 80% of
the historical product bugs. Use the fixture records to improve the test
harness.

Run these commands from the repository root with Python 3.11 or later and
Git history containing the cited commits. Validation rejects a guarantee ID
that no register defines. In a shallow clone it cannot see older commits, so
it warns about unverified SHAs instead of failing. Use `fetch-depth: 0` in CI
to verify them.

```sh
python3 scripts/testing/bugs.py validate
python3 scripts/testing/bugs.py summary
python3 -m unittest scripts.testing.test_bugs
```

The summary prints Markdown counts of fixed and open bugs by class, module and
finder. The [`testing-bugs` check](../checks/testing-bugs.toml) runs validation
in T0, T1 and T2, and [`testing-bugs-unit`](../checks/testing-bugs-unit.toml) runs
the unit tests.
