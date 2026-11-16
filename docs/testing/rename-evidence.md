# Project rename verification

This change names the project and Python distribution `nabedra-database`, the
Python module and CLI `nabedra`, and the Rust package and executable
`nabedra-database-server`. Its `Database(url=..., mode=...)` constructor accepts
`"olap"` or `"oltp"`, and CLI commands are grouped under `nabedra database`.

Both Python modes currently use the same engine. The mode is a validated,
read-only client preference. Operations still require authentication, supplied
by an explicit token or `NABEDRA_TOKEN`.

## Executed checks

- The final Python suite passed 177 tests in 70.012 seconds, after the provider
  compatibility and CLI startup corrections. It includes strict administrator
  authentication, receipt recovery, package routing and provider identity tests.
  The [sanitized suite output](rename/python-suite.txt) records the final run.
- The seven public interface tests passed again in 0.207 seconds. They cover both
  modes, invalid modes, constructor inactivity, local authentication failure,
  environment-token precedence, the renamed selector header, package metadata,
  command routing and version flags. The [test output](rename/public-interface.txt)
  records those assertions.
- A built wheel installed outside the source tree. Its distribution metadata,
  `nabedra` console entry point, constructor and module CLI worked there.
- The actual installer completed in a fresh prefix using the locked default-feature
  release binary. The Python package was reinstalled after the startup correction.
  All eight [installed smoke assertions](rename/integration.json) passed in
  32.689 seconds. Actual API and worker processes inserted and queried 10, 100 and
  1,000 rows, both modes returned the same 1,110 rows, authentication checks passed,
  and every committed row survived a full CLI stop and explicit cold restart.
  Both runs reaped their API and worker children. Installed Python file hashes
  matched the final checkout, and the installed binary hash matched the release.
- The final provider compatibility change passed all 13 provider tests in
  0.823 seconds. New Kubernetes launches use Nabedra names; existing resources
  retain their inventory and exact-UID lifecycle checks. Tests reject mixed
  namespaces, duplicate identities, conflicting ownership and failed inventory
  queries. These are adapter fixture assertions, not live Kubernetes acceptance.
- Rust passed all 567 library assertions and 188 integration assertions across
  35 targets without default features. Library execution took 55.57 seconds;
  integration targets took 56.89 seconds in total. The fixed storage identity
  and template fingerprint regression passed. The [server evidence](../../server/rename-evidence.md)
  records the commands, protocol fixture updates and build scope.
- The script guard suite passed 16 tests in 5.341 seconds. All 26 existing scripts
  parsed and 15 help invocations completed without starting workers or contacting
  cloud services.
- `scripts/acceptance_nodes.py` passed against the renamed native debug binary.
  It exercised actual API/two-worker TCP jobs, self and peer execution, competing
  mutations, byte-for-byte replay after a dropped response and an API restart,
  ordered query results, and process shutdown with stalled incoming connections.
  The [process runner output](rename/nodes.txt) records its completion.
- Both new and pre-rename local installation/configuration paths remain ignored.
  Worktree and staged changes passed `git diff --check` before publication.

These checks exercise subsets of U01, Q04-Q06, T03/T04 and F03 from the
[scheduler test contract](scheduler-contract.md). They do not establish full
scenario acceptance, resource utilization, AWS consistency or provider behavior.
No performance speedup or new OLAP/OLTP engine distinction is claimed.
The Docker daemon was unavailable, so no image build was executed.

The installed test caught an invalid startup cluster identifier containing a
space. The CLI now uses `Nabedra` and accepts `--cluster-name` for existing stored
identities. A focused regression failed before that fix and passed afterward.
The restart fixture also waits for a newly enrolled worker identity and bounded
read-only routing readiness. Retained membership alone does not prove the new
worker can execute requests. No mutation is resubmitted by this barrier.

## Compatibility and historical records

Current examples, imports, executable paths, configuration names, environment
variables, HTTP headers and deployment labels use the new names. Persisted
identity domains and the existing callback adapter contract retain their format
values so committed data and control records remain readable. See
[server compatibility](../../server/docs/reference.md#existing-storage-and-cluster-compatibility).
Existing namespaces must stay configured explicitly when reopening old data.
All cluster processes must use matching renamed versions.

Historical execution artifacts and notes retain their original bytes, including
old labels, hashes and measurements. Their archive READMEs distinguish them from
current usage. The existing authorized S3 acceptance bucket keeps its real
infrastructure name. No historical acceptance run or benchmark was rerun merely
by changing its labels.
