# Test suite audit

Reviewed on 2026-11-16, finally at `62516527dda3f02f7615488938eba5ded0c8a9af`, against
the current [code-review skill](../../.agents/skills/code-review/SKILL.md), its
[examples](../../.agents/skills/code-review/references/example.md),
[AGENTS.md](../../AGENTS.md), the scheduling plan and
[scheduler test contract](scheduler-contract.md).

The review does not support deleting most of the suite. It identifies eight
whole tests that can go now, 68 definitions worth consolidating or replacing,
and several weak or stale acceptance assertions. Most retained tests protect
distinct data, identity, resource or failure boundaries that compilation cannot
check. No production code or existing test was changed by this audit.

The initial review and checks used `9b63ed5`. Main advanced during the audit with
the `nabedra-db` rename. The final review includes that delta and its one added
startup test, which checks the real executable's identity/version/help. Source
locations and counts below match `6251652`; refreshed fundamental checks and the
earlier strict-Clippy result retain their separate revision identities.

The [complete inventory](test-suite-audit/inventory.csv) has one row for each of
952 definitions across 143 files, with its location, decision, reason and any
required replacement. Each definition was matched back to the source inventory.
The [runner inventory](test-suite-audit/runners.json) separately covers every
executable acceptance runner. [Execution evidence](test-suite-audit/checks.json)
records the fundamental checks and their failures, toolchain and limits.

| Scope | Definitions | Keep | Consolidate or replace | Remove now |
| --- | ---: | ---: | ---: | ---: |
| Rust database, storage and public API | 225 | 217 | 8 | 0 |
| Rust distributed runtime | 534 | 492 | 34 | 8 |
| Python ORM, CLI, gateway and adapters | 177 | 154 | 23 | 0 |
| Python script guards | 16 | 13 | 3 | 0 |
| Total | 952 | 876 | 68 | 8 |

These are source definitions, not a passing-test count. The Rust count includes
feature-dependent tests and an ignored live-S3 probe. Python discovery collected
all 177 ORM and 16 script cases without duplicate IDs or import failures in the
configured environment. The previous duplicate discovery problem has not
returned. A separate comparison found no identical Python test bodies.

There are also 12 executable acceptance runners, one Rust benchmark example,
and evidence validation/plotting tools. These are outside the 952 count.
`scripts/test_tls_material.py` is a fixture helper with no test methods.
No checked-in CI workflow enforces all these commands at this revision.
Documented manual execution and historical logs should not be called current
automatic coverage.

## How the policy applies

Pillar zero means compilation, syntax, imports, discovery and usable packaging.
It does not mean retaining a unit test for every field or constant. It also does
not make runtime schema validation, wrong-body rejection, cancellation ownership,
CAS ordering, resource ceilings or persisted compatibility redundant.

The skill asks for 3-10 essential scenario groups per functionality, with useful
variations. Its 20%/99% statement guides review selection; this audit cannot
measure or promise 99% defect detection. A distributed database has several
functionalities and independently failing boundaries. Replacing hundreds of
deterministic boundary checks with a handful of large successful workflows
would lose evidence.

`KEEP` retains the distinct scenario. It does not certify that every incidental
assertion in the method is necessary. `CONSOLIDATE` requires preserving the
listed observations before retiring the old definition. It includes two
control-memory tests that need actual runtime replacements. `REMOVE` is limited
to a redundant baseline or an implementation-only container check with no
independent application scenario.

Consolidation should reduce repeated setup and improve failure diagnosis.
Combining unrelated assertions into giant methods merely to lower the reported
count is not useful. This audit makes no unmeasured test-runtime savings claim.

## Eight whole tests to remove

| Test and current location | Why removal preserves useful coverage |
| --- | --- |
| `secret_fixed_vector`, `server/src/cluster/startup_material/identity/tests.rs:2` | The identical Admin input and literal digest are the first assertion in `concrete_function_vectors`. Preserve that independent digest fixture. |
| `pending_start_can_be_claimed`, `server/src/cluster/provision/delivery/tests.rs:40` | The richer `exact_claim_hash_confirmation_and_resource_completion` uses the same setup and claim, then verifies publication, hash, unchanged prior root and resource completion. |
| `pending_launch_remains_charged`, `server/src/cluster/provision/tests.rs:37` | The pending branch of `capacity_retains_ambiguous_and_phase_only_records` checks the same capacity behavior, including an elapsed deadline. Its ordinary decoding adds no malformed-input case. |
| `autoscale_loaded_progress_yields_snapshot`, `server/src/cluster/heartbeat/observation/tests.rs:531` | It only unwraps two heartbeat observations and a snapshot. Retained tests use `auto_healthy`, which exercises that sequence and asserts the absent then exact low-load anchor. |
| `whole_root_confirmation_accepts_actual_loaded_ack`, `server/src/cluster/drain/proof/tests.rs:28` | The positive confirmation path remains in `idle_confirmation_preserves_root_and_refuses_competing_metadata`, alongside unequal-root, conflict and unknown-outcome refusal. Preserve its acknowledged publication path. |
| `consumes_original_bytes_once`, `server/src/node/auth/verify/consume_tests.rs:155` | It consumes once and checks readiness bytes; it never attempts a replay. `owned_transfer_preserves_each_closed_body_and_association` covers those bytes, allocation and identity, while replay contention has separate tests. |
| `distributed_container_has_configured_ownership_tables`, `server/src/node/admission.rs:939` | It checks constructor mode and private size formulas without admitting work, overflowing capacity or attempting a legacy downgrade. Retain actual signed admission, initialization and capacity tests. |
| `accepted_storage_is_reserved_before_registration`, `server/src/node/payload_owner/installation_tests.rs:3` | It checks a fresh Vec's loose capacity threshold without registering a request. It proves neither allocation timing nor behavior under exhaustion. Retain reservation rollback and original-preparation retention on exhaustion. |

The inventory names the retained replacements. This is a source-based deletion
recommendation, not evidence from running a deletion patch and its regression
suite. No deletion was applied.

## Consolidations and assertion cleanup

The 68 conditional decisions comprise 23 Python ORM methods, eight Rust core
methods, 34 Rust runtime methods and three script guards. The inventory gives
the prerequisite for each one. The main opportunities are:

- Move the negative-trait portions of 18 opacity/ownership tests into compile-time
  invariant checks compiled with test targets. Preserve every protected
  type/trait pair. Keep actual debug-redaction checks as runtime assertions.
  Merely compiling production code does not forbid adding `Clone` or serde
  constructors to checked authority.
- Share repeated payload lifecycle setup across 11 classifier, barrier and
  final-receipt tests. Preserve each phase's original owner, late activity,
  release, failed-proof, cleanup and refusal assertions. These are different
  failure boundaries, so a generic successful drain cannot replace them.
- Consolidate Python configuration/preflight, installer refusal, receipt wait,
  transport redirect, credential and cleanup variations where the same driver
  and oracle can cover them. Keep zero-I/O, no-retry, exact error and resource
  closure observations.
- Combine the Rust CLI matrices, and retain ordinary task-error and subsequent
  executor-use observations when sharing worker-pool lifecycle fixtures.
- Share the two seeded resource-model drivers while keeping both traces, all
  event kinds, conservation checks and final settlement assertions.

Immediate cleanup inside retained tests is narrower:

- Remove conditional file-existence imports and the "implementation is missing"
  `setUp` assertions in Python callback protocol, pool, bridge and serve guard
  tests. Use normal imports; discovery supplies the fundamental check. Keep the
  behavior tests.
- Remove digest length/hex assertions already implied by an exact literal digest.
- Remove the redundant `callable(cli.main)` assertion where other tests call it.
- Remove the shared-interpreter framework-absence assertion in the gateway host
  test; retain the isolated subprocess that blocks optional dependencies.
- Standard Clap help/version `ErrorKind` assertions can go with real binary
  smoke retained. Unknown commands, forbidden flags, flag scope and numeric
  bounds remain application contracts.

Do not indiscriminately delete literal-limit assertions. If a boundary fixture
uses `MAX_BYTES + 1`, a separate literal may be its only independent specification
oracle. Removing the literal can let implementation and fixture drift together.
Prefer an actual literal-sized boundary case when changing that organization.
Likewise, preserve persisted digest/catalog compatibility fixtures unless a
supported-format change is explicitly intended.

The Python TOML/entrypoint mirror is conditional, rather than an immediate whole
test removal. It does not install a wheel, but installed verification is manual
today. Replace it with an enforced installed-artifact check before losing its
early metadata guard. `acceptance_nabedra.py` needs both the installed `--python`
and `--cli` arguments for that purpose.

## Tests that currently give weak or misleading evidence

1. Two memory tests bypass the runtime they name.
   `node/queue_runtime.rs:2135` charges two unrelated `ResourceScheduler`
   instances. `node/queue_transport.rs:447` manually charges formulas and small
   JSON examples. Neither runs actual install/lookup/terminal replication under
   pressure. Keep the C07/M02/R03 requirements and replace these with deterministic
   concurrent handler/client requests, exact receipt and terminal values, no
   false overload and final resource settlement. Encoding-length tests remain
   useful independently.

2. The C04 reducer-cap test in `scheduling.rs:904` releases partition grants
   before creating its reducer. It does not establish the combined node cap
   during overlap. Add overlap with partition and remote-I/O allocations at caps
   1/2/3, checking the union of allocated compute nodes after every transition.

3. The R03 process case in `acceptance_scheduler.py:603` starts a joiner while
   old holders retain a gated request, then lets the old holders finish. It does
   not prove the request reached the joiner before readiness. Retain the case
   and extend it with a copy barrier, empty/single/multipage variations and
   removal of original holders after readiness. Observe the original run,
   receipt and exact result on the joiner. Preserve Q03/R03 pagination regressions.

4. Queue-control authentication has a separate protocol in `queue_transport.rs`.
   Rich `node/auth` negatives cover another path. Add targeted validly signed
   one-field mismatches, stale authority, nonce replay/expiry/table exhaustion,
   malformed replies and unsigned success on the actual queue-control path.
   Preserve valid unrelated work during rejection. This concerns U02/C02/R05/Q05.

5. Some names exceed their boundary. Python receipt fixtures prove client
   reconnect behavior, not an API restart or worker retention. The public serve
   callback composition test never invokes a callback. The recovery runner's C01
   checks threads; distinct RAM is checked in the load runner. Keep the useful
   assertions and attribute evidence accurately.

6. Strengthen two small oracles. The UTF8-key ordering fixture in
   `engine/query/tests.rs:83` has names already ordered like numeric IDs; use keys
   whose order differs. The Python callback bootstrap test checks threads but
   not every declared execution field; compare the actual execution/resource
   values before claiming their complete preservation.

These are coverage gaps or narrow assertions. They are not newly executed
scheduler-contract failures. Other process and model checks remain useful for
their actual boundaries.

## Essential acceptance runners needing repair

The older cloud concurrency/partition runners have drifted from the current
transaction semantics:

- `acceptance_concurrency.py:40` accepts only catalog versions 1/2. Its object
  allowlist rejects durable decision objects. `acceptance_partitions.py:46`
  separately requires format 2. Current native `serve` initializes a fenced
  format 4 catalog through `runtime.rs`, `api/queue.rs` and `storage/history.rs`.
  Owned catalogs use format 5. A pure helper diagnostic accepted version 2 and
  rejected otherwise-identical version 4/5 empty catalogs.
- `acceptance_partitions.py:115` requires unchanged complete catalog/object
  inventories after conflict, no-op and cold restart. The identified operation
  path records durable decisions and advances revision even when tables are
  unchanged. Cold start changes the fence. Compare visible table contents,
  live references and immutable file bytes; allow decision/fence updates and
  separately assert one stable decision per identity. Preserve T06/R04/C03.

These are source-proven incompatibilities and a local helper diagnostic. No
current cloud runner was executed. Repair their current-format oracles; do not
remove their real partition, concurrency, atomicity or restart scenarios.

Five older S3 runners repeat process/table setup and CRUD. They can share a
selectable integration driver, retaining typed extremes, real TLS rejection,
same-base/concurrent results, stale handles, file retention, global query
semantics and committed-data reopen. A wrapper merge alone provides no speedup;
execution must avoid repeating the shared work.

Move `acceptance_latency.py` to an explicit diagnostic profile. It has one sample
per operation/scale, no warm-up/repetitions/latency threshold, and does not prove
B02. `acceptance_cli_serverless.py` is an opt-in gateway microbenchmark with a
fake compute responder. Keep their diagnostic purpose distinct from acceptance.

Retain `acceptance_nodes`, scheduler recovery, selected scheduler load profiles,
provider callbacks, installed-package smoke and real serverless-S3 acceptance.
Each covers a different boundary. Local conditional storage does not certify S3
or Kubernetes behavior. The script guards protect cleanup, namespace safety,
secret-safe reports and trustworthy fixture CAS; they are not database scenario
passes.

The archived `cli-serverless/final-core.json` repeats the method suffix in all
eight Python selectors. The reproduction guide therefore gives invalid unittest
IDs. Preserve archived bytes and add corrected current selectors. Historical
output remains historical evidence, not a current rerun.

Performance metadata also needs care. The load runner and Rust benchmark stamp
build-profile labels without verifying the supplied binary's build. The combined
validator checks numerical/oracle properties without establishing matching
revision/build/host provenance. Record a real build manifest or mark profile
unknown; preserve warm-up, three measured repetitions, complete configurations,
identity conservation, CPU/quota and RSS observations for B01/B02 claims.

## Minimal retained review rubric

For a change, choose the affected functionality rows and their 3-5 relevant
variations. Preserve the full scheduler contract separately; do not report a
whole identifier passing from a smoke subset. These groups organize existing
assertions and the replacements above, rather than authorizing eight giant
tests in place of the suite.

| Functionality | Essential scenario groups | Representative variations |
| --- | --- | --- |
| Data and query semantics | Exact typed values; CRUD and composed global query; input/result bounds and stable handles | Empty/10/100/1000 rows, numeric extremes and signed zero, UTF8 keys out of ID order, cross-partition ties/filter/limit/projection, late batch conflict. M01/T06/U01. |
| Mutation publication | Private preparation/upload failure; same-base CAS serial correctness; lost reply and stable decisions; conditional run/term fencing | Fail first/between/final upload, same-key/different-table race, applied versus unapplied ambiguity, old PUT before/after cold fence. T01-T06/R04/R05. |
| Scheduling and resources | Heterogeneous capacity and union caps; eligible refill/fairness; stage/aggregate byte budgets; failure settlement | Threads 1/2/4, caps 1/2/3 including reducer, two large plus small/skewed jobs, impossible/temporary pressure and 32-round aging, cancellation and baseline charges. C01-C07/M01-M03/U03. |
| Queue and recovery | Replicate before acceptance; accepted-ID conservation; identity/expiry/receipt races; surviving copy versus full shutdown | Bounded 1000-ID run, independent count/byte overflow, same/different-body API race, empty/single/multipage join, last-holder/cold-start exact outcomes. Q01-Q06/R01-R06. |
| Failure containment | Ordinary typed failures with unrelated progress; unwinding panic with owned buffers; cancellation through complete cleanup | Validation/storage/codec/resource errors, subsequent executor reuse, reservation/dispatch/receipt/reduction boundaries, original thread/process join. F01-F03. Abort/OOM isolation needs an actual process boundary. |
| Gateway, wire and grants | Original-byte routing/auth; strict bounded parsing; tenant and credential isolation; deadline/capacity behavior | Sync/async, malformed/forged/truncated traffic under saturation, equal identities across databases, warmed data and receipt reauthorization, accepted credential snapshot and expiry. U02/Q04-Q06 and GW/SL cases. |
| Installed CLI and transport | Usable installed artifact; real serve/API-worker startup; literal selection/ownership; private TLS trust | Both public modes, token precedence, 10/100/1000 rows in one scenario, PATH fallback/worker failure/signals, cold reopen and real HTTPS rejection. U01/C01/F03/R04 subsets. |
| Provider lifecycle | Unsupported manual scaling; idempotent provisioning/reconciliation; transfer/drain before termination | Configured versus absent provider, scale up/down, start/inventory/stop uncertainty, accepted work during removal, zero-to-one cold run with committed data. A01/A02. |
| Performance | CPU utilization; mixed CPU/memory/I/O arrivals; actual API/storage measurements | Caps/rates/heterogeneous sizes, warm-up plus three measured repetitions, exact results and accepted identities, latency distributions/throughput/CPU/RSS/network/retry metrics. B01/B02. |

Keep deterministic unit and fault tests alongside real-process scenarios. They
cover states a happy-path integration cannot reliably reach. Keep real S3 and
provider validation explicitly scoped to those integrations. Run performance
measurements on a quiet host only when the change needs them. A full regression
suite remains useful at suitable review/release boundaries; it need not be the
rubric for every narrow change or documentation edit.

The authoritative queue requirement remains replicated worker RAM. A surviving
complete worker record preserves accepted work; stopping every holder abandons
outstanding work, while committed mutations remain durable. No proposed test
reduction changes that requirement or deletes its scenarios.

## Fundamental checks actually performed

User budget was up to four CPU cores and 8 GB RAM. Cargo checks ran sequentially
with two build jobs, two Tokio/Rayon workers, debug information disabled and a
shared existing target cache. No aggregate OS quota was installed or certified.

| Check | Result and revision |
| --- | --- |
| Python 3.11 in-memory syntax compilation | All 81 tracked Python files compiled. |
| Python import and discovery | 177 ORM plus 16 script cases, no duplicate IDs/import failures. Full discovery uses installed HTTPX, not a core-only environment. |
| Rust formatting | Passed. |
| Rust default-feature compilation, all targets | Passed. |
| Rust no-default-feature compilation, all targets | Passed with two unused-item warnings. |
| Strict Clippy, default features and all targets | Last executed at `9b63ed5`, failed with exit 101. It reports 26 library and 27 library-test diagnostics with overlap. These are not 53 independent findings. It was not rerun after the rename. |

Clippy includes large enum variants, collapsible conditionals, argument/type
complexity and range-pattern suggestions. Locations and messages are in
`checks.json`. Do not equate its strict warning failure with a syntax/ordinary
compilation failure, or silently describe the strict gate as green. Compilation,
formatting and Python discovery were refreshed at `6251652`. Tested
Rust/Cargo is 1.99.1; the declared minimum Rust 1.94.1 was not separately tested.
Per-command durations are recorded as individual observations, not benchmarks.

The audit executed no existing unittest/Rust test bodies, scheduler-contract
scenario suites, cloud acceptance or load benchmarks. The catalog helper
diagnostic above is the only focused behavioral probe. Historical 638/727/755
Rust and 101/127/177 Python results in other ledgers refer to their own revisions
and scopes. They are not passing results for this audit.

Reproduce Rust fundamentals from `server/` with the environment recorded in
`checks.json`:

```sh
cargo fmt --all -- --check
cargo clippy --locked --offline --all-targets -- -D warnings
cargo check --locked --offline --all-targets --no-default-features
cargo check --locked --offline --all-targets
```

The separate ordinary default-feature check was needed because Clippy stopped
before checking every integration target. In a passing run, avoid repeating
equivalent compilation solely to accumulate check counts.

For the Python syntax/discovery check, use Python 3.11+ with the gateway extra
installed, from the repository root. This imports tests but does not run them:

```sh
PYTHONPATH=orm/python/src:scripts python3.11 -B - <<'PY'
from collections import Counter
from pathlib import Path
import subprocess
import unittest

for name in subprocess.check_output(['git', 'ls-files', '*.py'], text=True).splitlines():
    compile(Path(name).read_bytes(), name, 'exec')

def cases(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from cases(test)
        else:
            yield test

for directory in ('orm/python/tests', 'scripts'):
    found = list(cases(unittest.TestLoader().discover(directory, pattern='test_*.py')))
    ids = [test.id() for test in found]
    failed = [test for test in found if isinstance(test, unittest.loader._FailedTest)]
    print(directory, len(ids), 'duplicates', [name for name, n in Counter(ids).items() if n > 1])
    assert not failed, failed
PY
```

Move `test_cluster.py`'s `unittest.main()` guard below `ResourceCapacityTests`
if direct-file execution is supported; currently that invocation skips its two
C01/C02 methods. Document the gateway extra in full-suite setup, since a plain
core install cannot collect HTTPX-dependent gateway tests. Keep isolated tests
that prove core use still works without optional dependencies.

## Recommended follow-up order

1. Repair the misleading control-memory, reducer-overlap and join-copy evidence,
   and the stale cloud catalog/decision oracles. Preserve their regression IDs.
2. Remove the eight proven redundant/mechanical tests and the safe setup trims.
   Run affected retained scenarios to validate the deletion patch.
3. Consolidate shared fixtures and move negative traits into the fundamental
   compile gate, with every listed prerequisite preserved.
4. Establish explicit focused/local-process/release/cloud/benchmark selections,
   fix the current selectors/dependency instructions and strict lint baseline.

This ordering protects useful coverage while reducing low-value work. The
inventory is specific to the reviewed revision and should be reconciled with
new tests before implementing its recommendations.
