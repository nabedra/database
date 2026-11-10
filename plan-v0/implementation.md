# V0 implementation and testing rubric

The scope is `plan-v0/orm.md` and `plan-v0/server.md`. The older `plan/` documents provide context; their distributed and later-version features are excluded from v0. The coordinator plans, delegates and records evidence. All subagents use GPT-6.1 Sol with high effort, implement one bounded task per assignment, and work on `main` in the shared checkout. No agent owns an entire package or server implementation.

An implementation box is checked after the agent delivers working code and the coordinator reconciles its evidence with the plan. Test and review boxes require actual passing results. Unfinished work stays unchecked.

## Delegation queue

Each assignment has a file boundary, dependencies and a concrete validation result. Reviews are separate assignments. Tasks touching the same files run in sequence. The coordinator updates this queue and the acceptance rubric below after receiving evidence.

### Rust tasks

- [x] R01. Define and validate the strict wire protocol and errors, with only the crate scaffolding needed to compile it. Check representative valid requests, invalid exact types and unknown fields/operators.
- [x] R02. Implement bounded S3 object access and catalog loading/conditional initialization. Check definite absence, denied/unreadable data and unsupported versions with an S3 fake.
- [x] R03. Encode/decode complete Parquet snapshots for the four scalar types, including empty tables and decoded-size limits. Check round trips and malformed/oversized input.
- [x] R04. Implement conditional catalog publication and lost-response reconciliation under the mutation lock. Check upload failures, conflicts and confirmed/unknown outcomes.
- [x] R05. Implement create, describe, list and drop, immutable table IDs and retained snapshots. Check duplicate names and stale IDs after recreation.
- [x] R06a. Implement pure row-mutation rules with atomic batch validation and missing-row/no-op results. Verify them before engine/S3 wiring.
- [x] R06. Implement row CRUD and atomic insert_many. Check duplicate rejection, batch rollback, missing rows, table limits and concurrent writes.
- [x] R06b. Wire the engine modules and dispatch all thirteen operations to their completed implementations. Verify public dispatch and retained error outcomes.
- [x] R07. Implement predicate evaluation, deterministic ordering, projection, limit, count and explain. Check a representative composed query and no-scan explain.
- [x] R07a. Connect fresh snapshot reads to get, collect, count and explain, with reusable bounded row loading. Verify freshness and metadata-only explain.
- [x] R08. Implement admission/execution permits and tracked work that survives disconnects. Check overload, execution bounds and drain behavior.
- [x] R09a. Encode bounded success/error response envelopes with status and outcome preservation. Check the complete body limit and serialization failures.
- [x] R09. Wire the authenticated HTTP operation endpoint, body/response bounds and sanitized request diagnostics. Check status/envelope behavior and unknown operations.
- [x] R09b. Add unauthenticated liveness and bounded fresh-catalog readiness routes. Verify status and storage failures without exposing configuration.
- [x] R10a. Validate serve options and environment configuration without starting a runtime or connecting to AWS. Check required values, namespace, worker counts and readable TLS pairs.
- [x] R10b. Parse the documented serve command with Clap and expose validated options for bootstrap. Check the command defaults and explicit options.
- [x] R10. Wire CLI/startup, native TLS, health endpoints and signal shutdown. Check invalid configuration, readiness and process drain; produce a runnable binary.

### Python tasks

- [x] P01. Add package scaffolding, public exception types and exact schema/row/key validation. Check type mappings, bounds and invalid rows.
- [x] P02. Implement immutable expressions and query builders with schema validation and wire serialization. Check builder independence, composition and depth limits.
- [x] P03. Implement private bounded HTTP(S) transport and envelope/error handling. Check redirects, request IDs, malformed responses, body-read failures and uncertain mutations.
- [x] P04. Implement table handles and table operations, including one bounded insert_many request. Check schema copies, immutable IDs, batch consumption and operation result validation.
- [x] P05. Wire DB discovery/creation, public exports and query execution/result decoding. Check editable installation and the complete public API with a local HTTP fixture.

### Acceptance and documentation tasks

- [x] E01. Add safe test configuration and per-test prefix allocation, and perform a read-only preflight against the supplied bucket only. Check that another bucket or unsafe prefix cannot be selected.
- [x] E02a. Add a bounded actual-process launch/health/stop helper for acceptance tests, preserving the allocated bucket/prefix and closing children on failure. Verify its local process lifecycle without AWS.
- [x] E02b. Prepare executable HTTP acceptance scenarios and sanitized result reporting using the existing bucket/process guards. Live execution remains E02 after bootstrap.
- [x] E02. Run the real HTTP/Parquet type and CRUD/query scenarios, including atomic failure cases, and save sanitized evidence.
- [x] E03a. Generate isolated local test CA/server certificates for verified loopback HTTPS and validate the certificate material before Rust acceptance.
- [x] E03b. Prepare actual-process restart and HTTPS acceptance scenarios with sanitized reports. Live execution remains E03 after bootstrap.
- [x] E03. Run actual-process restart and native HTTPS/trusted-CA scenarios in their own prefixes.
- [x] E04a. Prepare one concurrent-client and retained-object inspection scenario within a fresh guarded namespace. Live execution remains E04 after bootstrap.
- [x] E04. Run independent concurrent clients, stale handles and retained-snapshot/catalog inspection in a fresh prefix.
- [x] E05a. Prepare a modest timing runner for single-row writes, batches and scans in separate namespaces. Actual measurements remain E05 after bootstrap.
- [x] E05. Run modest single-write/batch/scan timings at increasing row counts and save machine-readable results.
- [x] D01a. Document ORM installation, public operations, query behavior, errors and limits using the completed Python implementation.
- [x] D01b. Document server build, configuration, HTTP/native TLS, lifecycle, storage behavior and limits from the completed component contracts.
- [x] D01. Write reproducible setup/test documentation, supported behavior and v0 limitations from the completed implementation.

### Reviews and final checks

- [x] V01. Review completed task groups against the plan and resolve each concrete gap through a bounded fix assignment.
- [x] V02. Independently review code quality after spec compliance passes and resolve material findings.
- [x] V03. Verify final Rust/Python check results, run any remaining necessary checks and reconcile every checkbox with recorded evidence.

## Implementation

- [x] I01. Installable Python 3.11+ ORM with a standard-library-only runtime, public exports and documented setup.
- [x] I02. Immutable table identity and schema copies; exact schema, row, key and scalar validation; identifier and size limits.
- [x] I03. All table operations, atomic bounded iterable batches, missing-row results and stale-handle behavior.
- [x] I04. Immutable expressions and queries; composition, replacement rules, validation, serialization, collect, count and explain.
- [x] I05. Private HTTP(S) transport, verified TLS/custom CA, no redirects or retries, bounded reads and writes, request-ID and result validation.
- [x] I06. Complete exception mapping, diagnostics and conservative handling of uncertain mutation outcomes.
- [x] I07. Rust crate and locked dependencies; serve CLI, environment configuration, HTTP/native HTTPS and startup validation.
- [x] I08. Strict wire protocol, all thirteen operations, schema/query validation, status codes and bounded envelopes.
- [x] I09. AWS credential/region chain; validated, bounded catalog loading and conditional initialization only after definite absence.
- [x] I10. Immutable Parquet snapshots for every supported type, including empty tables; bounded decoding and preserved historical/orphan objects.
- [x] I11. Serialized atomic mutations and conditional catalog publication; conflict handling, disabled publication retries and lost-response reconciliation.
- [x] I12. Fresh snapshot reads, CRUD, duplicate rejection, atomic batches, table limits and immutable IDs across drop/recreate.
- [x] I13. Filter, deterministic sort with key tie-break, limit, projection, count and explain without a data scan.
- [x] I14. Bounded admission/execution, blocking compute, disconnect-safe work tracking and graceful shutdown.
- [x] I15. Liveness/readiness, redacted operation diagnostics and explicit unsupported-operation responses.
- [x] I16. Reproducible local/AWS acceptance runner, setup instructions and documented limitations.

## Focused local validation

Tests should prove a distinct behavior or failure mode. Prefer representative public-API workflows and a conditional-write S3 fake over redundant tests of implementation details.

- [x] T01. Python unittest checks for payloads, validation, query independence and a single atomic batch request.
- [x] T02. HTTP fixture checks for redirect rejection, response bounds, request IDs, error mappings and uncertain writes, including body-read failures.
- [x] T03. Rust tests for CRUD/query behavior, exact types, duplicates, limits and whole-batch rollback.
- [x] T04. Conditional-write fake checks for upload failure, publication conflict, lost responses and outcome reconciliation.
- [x] T05. Concurrent writes, reads during mutation/drop, stale IDs, admission overload and work surviving disconnects.
- [x] T06. Startup/catalog validation, recovery, readiness and shutdown checks at the smallest useful layer.
- [x] T07. Editable ORM install and package test suite pass.
- [x] T08. `cargo fmt --check`, `cargo clippy -- -D warnings` and `cargo test` pass.
- [x] T09. Independent spec review passes; all reported functional gaps are resolved.
- [x] T10. Independent code-quality review passes; material findings are resolved.

## Real AWS acceptance

Use only the bucket supplied in `.env`, verified as `meshdb-testing` in `us-east-1`. Map its `BUCKET_NAME` to `MESHDB_S3_BUCKET` for the server. Never print or commit credentials. Each test owns a fresh prefix named `test-<number>-<name>-<run-id>/`; preserve fixtures and snapshots for inspection. No bucket creation, other AWS services or deletion of existing objects.

- [x] A01. Populate real Parquet snapshots through the server and verify string, signed int64, finite float64 and boolean round trips.
- [x] A02. Exercise the ORM example and all public table/query operations over HTTP, including atomic failures and exception mappings.
- [x] A03. Stop and restart the actual Rust process against the same prefix, then verify durable data and catalog recovery.
- [x] A04. Run native HTTPS with a trusted test CA; verify that untrusted certificates fail.
- [x] A05. Exercise independent concurrent clients without lost writes and old handles after drop/recreate.
- [x] A06. Inspect generated Parquet objects and catalog references in the supplied bucket; record test prefixes and counts without secrets.
- [x] A07. Record single-row write, batch-write and scan latency for a small set of increasing row counts. State dataset sizes and experimental limits.

## Evidence and open work

The initial package-wide assignments were cancelled at the user's request before source files were produced. Their scope is replaced by the task queue above. All replacement assignments use high effort only. Completed implementation and validation are recorded below; live acceptance remains pending.

Complete. Every implementation and testing item is checked with recorded evidence. Both independent final audits passed, the local guard suite passed, and all 178 live acceptance checks passed. Nine test folders remain populated in the supplied bucket. Work remains uncommitted on `main`.

- P01 complete. Editable installation and five focused unittest groups passed under Python 3.11.10. Independent spec and quality reviews passed with no findings. Evidence: `orm/notes/P01.md`.
- E01 complete. The focused local guard test and both independent reviews passed. A single read-only HeadBucket request succeeded against `meshdb-testing` in `us-east-1`; no AWS writes/deletions occurred. Write permissions remain untested. Evidence: `scripts/notes/E01.md`.
- R01 complete. The agent fixed scalar marker-object and unit-enum object syntax acceptance after review. Six grouped protocol tests, formatting, Clippy, independent spec re-review and quality review pass. Evidence: `server/notes/R01.md`.
- P02 complete. Five query test groups and all ten ORM tests passed at delivery. Independent spec and quality reviews passed; a separate read-only probe confirmed schema isolation. Evidence: `orm/notes/P02.md`.
- P03 complete. Spec review found malformed HTTP classified as read-side network failure. The coordinator made the small exception-classification fix and added one real-HTTP regression group. All nineteen ORM tests and independent spec/quality reviews pass. Evidence: `orm/notes/P03.md`.
- E02a complete. Its real Python subprocess lifecycle guard, existing configuration guard and independent spec/quality reviews passed. No real Rust/TLS/AWS acceptance is claimed. Evidence: `scripts/notes/E02a.md`.
- P04 complete. Four table test groups, all twenty-three ORM tests and independent spec/quality reviews passed. Evidence: `orm/notes/P04.md`.
- P05 complete. All public ORM methods are present. Five new API integration groups, all twenty-eight ORM tests, editable installation, public-import checks and independent spec/quality reviews passed. Real server/S3 acceptance remains. Evidence: `orm/notes/P05.md`.
- R02 complete. Four storage groups plus protocol/query tests, formatting, Clippy and independent spec/quality reviews passed. No AWS calls occurred. Evidence: `server/notes/R02.md`.
- R07 pure evaluator complete. Five query groups, targeted Clippy, formatting and independent spec/quality reviews passed. Engine wiring remains. Evidence: `server/notes/R07.md`.
- R08 complete. Four executor groups, targeted Clippy, formatting and independent spec/quality reviews passed. HTTP/shutdown wiring remains. Evidence: `server/notes/R08.md`.
- R06a complete. Six focused mutation groups, owned-file formatting and independent spec/quality reviews passed. R03 resolved the shared snapshot lint and full-crate Clippy now passes. Durable engine wiring remains. Evidence: `server/notes/R06a.md` and `server/notes/R03.md`.
- R03 complete after review fixes. A bounded footer guard precedes both metadata readers, including the coordinator's small boolean-collection rejection correction. Three guard tests, six snapshot groups, targeted Clippy, formatting and independent spec/quality reviews pass. Evidence: `server/notes/R03.md`.
- R10a complete. Four configuration groups, direct targeted Clippy, formatting and independent spec/quality reviews passed. R04's subsequent full Cargo run also passed the integrated configuration tests. CLI/bootstrap wiring remains. Evidence: `server/notes/R10a.md` and `server/notes/R04.md`.
- R04 complete. Five commit groups, all forty-three tests, repository formatting, library/commit Clippy and independent spec/quality reviews passed. All-target Clippy initially hit a peer snapshot-test lint; R03-F1's later snapshot Clippy passed. Engine integration remains. Evidence: `server/notes/R04.md`.
- R10b complete. Two CLI groups and all four existing configuration groups, targeted Clippy, owned-file formatting and independent spec/quality reviews passed. Clap is pinned; defaults reuse the configuration defaults. Bootstrap remains. Evidence: `server/notes/R10b.md`.
- R05 complete. Three catalog lifecycle groups, targeted Clippy, owned-file formatting and independent spec/quality reviews passed. Module wiring and row/query dispatch remain. Evidence: `server/notes/R05.md`.
- R09a complete. Three response-envelope groups, targeted Clippy, formatting and independent spec/quality reviews passed. Complete body limits include framing and preserve uncertain mutation outcomes. HTTP wiring remains. Evidence: `server/notes/R09a.md`.
- R07a complete. Three read integration groups, targeted Clippy, owned formatting and independent spec/quality reviews passed, including fresh reads, metadata-only explain and a captured snapshot read across drop. Final module wiring remains. Evidence: `server/notes/R07a.md`.
- E03a complete. Generated certificate chain, localhost/IP SANs, server purpose, SSL parsing, permissions, existing-directory refusal and independent spec/quality reviews passed. Actual Rust HTTPS/S3 acceptance is still pending. Evidence: `scripts/notes/E03a.md`.
- R06 complete. Four integrated write groups, targeted Clippy, owned formatting and independent spec/quality reviews passed. Final module wiring/dispatch remains. Evidence: `server/notes/R06.md`.
- D01a complete. Public signatures and behavior were crosschecked, all three Python snippets compile without execution, whitespace checks and independent spec/quality reviews pass. The earlier query-limit caveat is resolved by R01-F2. Evidence: `orm/notes/D01a.md`.
- R06b complete. All thirteen operations use the exported production engine. Two dispatch groups, all sixty-one Rust tests, full formatting, all-target Clippy and independent spec/quality reviews pass. Evidence: `server/notes/R06b.md`.
- E02b preparation complete. Syntax, safe import, CLI help, whitespace checks and independent spec/quality reviews pass. The coordinator fixed the review finding that Ctrl-C continued later scenarios; one focused regression failed before and passes after. Live HTTP/S3 execution remains pending. Evidence: `scripts/notes/E02b.md`.
- E03b preparation complete. Syntax, inert import, CLI help, focused cancellation/cleanup regressions and independent spec/quality reviews pass. The coordinator resolved both review findings: setup cancellation reporting and retaining TLS files until process exit is confirmed. Actual Rust/S3 execution remains pending. Evidence: `scripts/notes/E03b.md`.
- R09 complete. Four focused HTTP groups, all sixty-five Rust tests, full formatting, targeted strict Clippy and independent spec/quality reviews pass. Native listeners, health routes and startup/shutdown are still pending. Evidence: `server/notes/R09.md`.
- E04a preparation complete. Syntax, inert import, CLI help and independent spec/quality reviews pass. Concurrent independent clients, stale handles and guarded read-only snapshot inspection await live execution. Evidence: `scripts/notes/E04a.md`.
- R09b complete. Three focused tests, owned formatting, targeted strict Clippy and independent spec/quality reviews pass. Liveness avoids storage; readiness uses one fresh catalog load with a five-second timeout. Evidence: `server/notes/R09b.md`.
- E05a preparation complete. Syntax, inert import, CLI help and independent spec/quality reviews pass. Three separately namespaced datasets and one measured single write, batch and scan per dataset await actual measurements. Evidence: `scripts/notes/E05a.md`.
- D01b complete. Local links, shell syntax, JSON/UUID examples, runner arguments, component signatures and independent spec/quality reviews pass. Evidence: `server/notes/D01b.md`.
- R10 complete. Focused startup/runtime tests, binary build, full formatting, all-target strict Clippy, all seventy Rust tests and independent spec/quality reviews pass. Evidence: `server/notes/R10.md`.
- E02 live acceptance passed once with exit 0: all 80 checks and all three graceful process stops passed. Retained prefixes: `test-1-scalar-types-e87fcdb166ec/`, `test-2-orm-api-5289f980b3d8/`, `test-3-atomic-errors-5a02d339a957/`. Evidence: `scripts/notes/E02.md` and `artifacts/acceptance-http-e48507e162664a7facf769c88c220bd0.json`.
- E03 live acceptance passed once with exit 0: all 32 restart/HTTPS checks passed, including durable identity/data, post-restart mutation, trusted/default-trust behavior and all three graceful process exits. Exact retained prefixes and fixture IDs are recorded in `scripts/notes/E03.md`; report: `artifacts/acceptance-lifecycle-47da7bfd14454974852efa69386a8f6d.json`.
- E04 live acceptance passed once with exit 0: all 30 checks passed in `test-6-concurrency-ea508f6e35d6/`. Four independent clients produced exactly 21 rows; stale IDs were rejected. S3 inspection found 11 objects, two current references and eight historical snapshots; four current/historical/dropped snapshots passed bounded PAR1 framing checks. Evidence: `scripts/notes/E04.md` and `artifacts/acceptance-concurrency-fe40e554cd374929ab6b734644a49719.json`.
- V01 final offline specification audit passed with no actionable integration findings. All thirteen operations are reachable through the actual binary; client/server types, IDs, outcomes, limits and lifecycle contracts agree. No planned operations remain stubbed or unwired.
- V02 final offline quality audit passed with no actionable cross-component findings. Ownership, strict result decoding, shared production wiring, mutation lifetimes, production test paths and sanitized runner diagnostics were checked without repeating suites.
- D01 complete. All 24 local links, six shell snippets, the compiled Python example, command paths, runner flags and independent spec/quality reviews pass. Evidence: `scripts/notes/D01.md`.
- E05 live acceptance passed once with exit 0: all 36 checks and nine measurements passed across initial row counts 1/100/1000, with final counts 12/111/1011 and three graceful exits. Exact prefixes, IDs, actual row counts and one-sample measurement limits are recorded in `scripts/notes/E05.md`; report: `artifacts/acceptance-latency-fef687c9256e4bcb8b5b497c4c823e6b.json`. The chart is `artifacts/acceptance-latency-chart.html`.
- V03 complete. All five accumulated local script guard groups passed. File chronology and package metadata confirm that the 70 passing Rust tests, fmt/all-target Clippy/build and 28 passing ORM tests remain current without redundant reruns. The coordinator corrected one extra blank line in a handoff note, passed whitespace checks across 107 authored text files, and reconciled all four live reports, nine distinct prefixes and chart values. Private/generated paths are ignored and untracked. Evidence: `scripts/notes/V03.md`.

R01-F2 complete. The query-limit mismatch is resolved by accepting unsigned integer tokens and saturating above u64 internally; table-size bounds make this equivalent for collect/count. Seven protocol groups, targeted Clippy, formatting and independent spec/quality reviews passed. Evidence: `server/notes/R01-F2.md`.
