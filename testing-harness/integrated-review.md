# Integrated review of the testing harness wave

Reviewer: Claude Opus 5.5, a non-author reviewer of the whole wave. Range: `bdf8d679..e3dea900`, PRs #28 to #50.
Fix PR: https://github.com/zPy52/nabedra-database/pull/52 (branch `nabedra/harness-integrated-fixes`).

## What was run

| Step | Result |
| --- | --- |
| `run_check.py --list --tier T0` | 20 checks listed |
| 5 T0 checks through `run_check.py`, plus 3 more | testing-matrix (75 executed), testing-ingest (19), testing-bugs (1), testing-tools (34), realenv-unit (28) and check-core (74, private target dir) all pass. olap-logic-orm-python and olap-logic-orm-typescript pass after the fix. |
| `matrix.py lint` | 0 errors, 99 warnings. The warnings are kinds that no check provides yet. |
| `matrix.py build` | Builds. The committed matrix on main was stale after #49 (OLAP-STR-G06 H). |
| `matrix.py select` | Ran on `bdf8d679..origin/main` and on `e3dea900~1..e3dea900`. Both are full T1, because deleted check tomls map to `misc`. |
| `matrix.py gate --sha e3dea900 --scope release` | Exit 1. 0 green, 166 not green, 10 deferred. The failure is honest. |
| `release_gate.py --sha e3dea900` (with and without `--dry-run`) | Exit 1 |
| `catalog.py build --check` | **Exit 1 on main**: the catalog was stale after #49 (4 stream sim tests) |
| `ingest_evidence.py --help` | OK |
| `s3/budget.py` | OK, $0 of $5 |
| Rust filters, checked with `cargo test --lib -- --list` (617 tests) | Each matches at least one test: `storage::contract_tests::contract_local_store` (exact), `contract_s3` (exact), `node::stream::sim_tests::st01_st06_olap_stream_simulation` (exact), `storage::contract_tests::` (5), `storage::commit_protocol_tests::` (10), `node::stream::sim_tests::` (4), `scheduler_properties` (6), `olap_oracles` (3) |
| Evidence from the full T1 run 45055093094 on e3dea900 | Downloaded and read all 28 records. 26 pass and **2 are not-run, but their jobs are green** (finding 1). All 28 have `tree_clean` true. |
| Skill docs: paths, anchors, flags, workflow inputs, check and guarantee IDs | Audited by a subagent. 1 broken command (`--module kv`) and 2 doc gaps, all fixed. Everything else resolves. |

## Findings, most severe first

### 1. Blocking (honesty): the CI checks job reports not-run as success. Fixed.
- **Where:** `.github/workflows/nabedra-build.yml:384` at e3dea900 ran `run_check.py "$ID"` without `--strict`. `run_check.py:461` returns 0 for not-run.
- **Failure:** in run 45055093094, the jobs `Check olap-logic-orm-python` and `Check olap-logic-orm-typescript` concluded `success`. Their records say `not-run`, executed 0. An agent that follows "merge when the dispatch is green" treats those checks as passing.
- **Fix:** the job now runs `--strict`, so not-run fails it with exit 2. The job summary prints the record's result, counts and not-run reasons. The nightly leaves out `s3` checks while the S3 repository variables are unset, instead of dispatching them to fail. `gh-actions.md` documents both. A local simulation of the step exits 2 for olap-store-contract-s3 with the bucket unset.

### 2. Important (vacuous check): the ORM logic replays could never execute in CI. Fixed.
- **Where:** `docs/testing/checks/olap-logic-orm-python.toml` and `olap-logic-orm-typescript.toml`. They ran only `test_replay_corpus`, which needs `NABEDRA_LOGIC_URL`, and no job sets it.
- **Failure:** the T1 D evidence these checks claim for ORM-G02 and OLAP-G06 was never produced. The records were always not-run, and with finding 1 the jobs were green.
- **Fix:** each command now runs the Rust `olap_logic` runner with `NABEDRA_LOGIC_RECORD` and then the ORM replay with `NABEDRA_LOGIC_TRANSCRIPTS`. The HTTP test still runs and is reported as skipped.
  - Local result: 317 corpus assertions pass in both ORMs.
  - Planted bug: changing `neg` to `nag` in one recorded `table.get` response makes both replays fail (Python 316/1, TS fail 1).

### 3. Important (silent evidence loss): fuzz-nightly records were always dirty, so the matrix ignored them. Fixed.
- **Where:** `server/fuzz/nightly.sh:23`. `cargo fuzz run <target>` writes new inputs into the tracked `fuzz/corpus/<target>/`. `evidence.new_record` reads `git status` after the command, and `matrix.py:445` drops records where `tree_clean` is false.
- **Failure:** every fuzz-nightly record would carry `tree_clean: false` and never fill CACHE-G01, EVT-G07 or OLAP-G08 F.
- **Fix:** new inputs now go to the ignored `fuzz/target/nightly-corpus/<target>/`, and the tracked corpus is read as a second corpus directory.

### 4. Important: kind-events could never pass, and its record would be dirty. Fixed, pending Actions.
- **Where:** `docs/testing/checks/kind-events.toml`. The check deploys `nabedra-events:ci`, but the checks job builds nothing (gh-actions.md). It also uses the default `--history kind-events.jsonl` (`suite_events.py:313`), which is an untracked file at the repository root.
- **Fix:** the command builds the image from `server/events/deploy/Dockerfile` (the same arguments as the docker-events job) and writes the history under `target/testing-logs/`. `timeout_s` goes from 1800 to 3600 for the image build. It has not run locally. It is in dispatch 45056495242.

### 5. Minor (interface drift): Rust `evidence::Record` rejected schema-valid records. Fixed.
- **Where:** `server/check/src/evidence.rs:94` has `deny_unknown_fields` but had no `s3_requests` field. The schema and the Python writer both have the field.
- **Failure:** `Record::from_json` fails on any Python record from an S3 check, and Rust writers cannot report S3 requests to the budget.
- **Fix:** added an optional `s3_requests` field that is omitted when `None`. A test in `tests/core.rs` reads a record with the field and writes it back.

### 6. Minor: the catalog and coverage matrix on main were stale after racing merges. Fixed by regeneration.
- `catalog.py build --check` failed on e3dea900. Nothing in CI checks catalog freshness. Proposed follow-up: run `catalog.py build --check` in the evidence workflow, or regenerate there, rather than in the PR gate, where racing merges would make it flaky.

### 7. Minor (docs): fixed.
- `docs/testing/checks/README.md:84` used `--module kv`. After the cache rename it prints nothing and exits 0.
- The quickstart says `python3`, and the tools need 3.11 or later (`tomllib`). It now says to substitute `python3.11`.
- `docs/testing/evidence/records/` does not exist until the first record is written. The quickstart now says so.

### Not fixed: deferred or by design
8. **cache-cluster-real (T2) still fails every night.** It needs `NABEDRA_CACHE_BINARY` and a redis wheel, and a binary built on the ubuntu-24.04 host may not run in `python:3.11-slim-bookworm` (glibc 2.39 against 2.36). The failure is red, so it is honest. Follow-up: build the binary inside the same image, or a static build.
9. **No R check covers OLAP-G01..G05.** olap-store-contract-s3 deliberately lists no guarantees, and lint warns. The matrix shows these cells as `missing`, which is honest.
10. **One check's pass masks another check's not-run.** `matrix.py:388` `_combine`: on one SHA, a pass from one check hides a not-run from another check of the same kind for that cell. An example is ORM-G02 D before this PR: contract vectors passed while the logic replays were not-run. This is by design (any check of the kind covers the cell), but a reviewer reading the cell cannot see it. Proposed: list the not-run check IDs in the cell reason.
11. **The release gate can never pass while deferred cells exist.** `matrix.py:725` requires `not deferred`. That fails closed, which is consistent with "never counted as green". The plan's wording ("lists deferred checks") suggests that a release with named deferrals should pass, so this needs a user decision.
12. **`matrix.py select` maps a deleted or renamed check toml to `misc`,** which selects the full T1. This is conservative but costly after renames.
13. **The nightly fuzzes the same targets twice:** `jobs=fuzz` at 3600 s per target, and the fuzz-nightly check at 300 s per target. Proposed: drop one.
14. **`resolve_command` (`run_check.py:315`) rewrites only argv[0].** A `python3` inside `bash -ec` commands uses the PATH `python3`, which is 3.6 on this Mac. Those checks fail locally unless `python3` is 3.11. Proposed: export `NABEDRA_PYTHON=sys.executable` to the child and use it in the commands.

## CI and merge
- Dispatch https://github.com/zPy52/nabedra-database/actions/runs/45056495242 on 8498d0ee: all 7 checks pass, and every record has a clean tree.
  - olap-logic-orm-python and -typescript: 317 corpus assertions replayed in each.
  - kind-events: EV06 and EV07 ran on kind for the first time; 7 acknowledged events were replayed at contiguous offsets.
  - fuzz-nightly: 9 targets, 300 s each, 3093 s in total.
  - check-core 74, testing-matrix 75, testing-tools 34.
- Gate https://github.com/zPy52/nabedra-database/actions/runs/45056547599 on 8498d0ee: green, 0 failed in the raw lines.
- After merging main (#51) into the branch, at fb4113bd:
  - Dispatch https://github.com/zPy52/nabedra-database/actions/runs/45061628258: testing-matrix 75, check-history 18 and check-core 76 pass.
  - Gate https://github.com/zPy52/nabedra-database/actions/runs/45061625923: green. Raw lines: OLTP 572, OLAP 407 and 408, kv 195, check 76, Cache 50 and Events 35 passed, 0 failed.
- PR #52 merged as e3d51a6ab87facd85eb493115228771f1fc193a2 at 20:46 Madrid. The branch is deleted.
