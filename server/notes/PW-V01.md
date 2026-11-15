# PW-V01 final specification and integration audit

Verdict: PASS for the final implementation specification and D01 documentation.
No actionable integration omission found. Independent final quality review may
proceed. Live E02 S3 acceptance remains pending and is a separate completion gate.

Read-only audit on 2026-11-10 in the existing shared main checkout. This note is
the only file authored. Reused the accepted P01-P04, W01/W01b/W02-W04 and E01
reviews and their evidence rather than repeating their full reviews or tests.

## Final wiring evidence

- `server/src/config.rs:24`, `server/src/partition_size.rs:47` and
  `server/src/main.rs:87` connect both CLI spellings, mandatory case-insensitive
  units, integral quantities, the 64,000,000-byte default/cap, configured worker
  count and configured target to the single shared Engine pool.
- `server/src/worker_pool.rs:100` starts persistent named OS threads with local
  current-thread IO runtimes. Submission uses original index modulo N. Worker
  fetch/decode/validation is in `server/src/engine/partition.rs:47`; reads and
  mutation preparation call it inside the assigned worker job. Indexed dispatch
  at line 132 bounds outstanding jobs, drains admitted siblings on failure and
  preserves deterministic errors. Storage routing itself uses typed manifest
  ranges in `server/src/storage/manifest.rs:43`, independently of N.
- `server/src/engine/read.rs:59` captures one catalog, dispatches file work and
  checks full decoded totals. Collection retains sort/key columns until global
  reduction, then applies one order, ascending key tie-break, limit, projection
  and result budget. Count sums matches before its global limit. Keyed reads
  retain the original manifest index; explain is metadata-only.
- `server/src/engine/write.rs:47` holds one mutation session through whole-batch
  validation, routed parallel preparation, draining, aggregate limits and
  publication. Untouched objects retain their references and physical metadata;
  empty ranges disappear with valid neighboring bounds. No-op legacy operations
  retain legacy data; actual changes migrate it. `server/src/commit.rs:133`
  validates the complete candidate and upload set before sequential immutable
  PUTs and one conditional catalog PUT, preserving uncertain-outcome handling.
- `server/src/snapshot/partition.rs:34` measures complete closed output, splits
  toward the target, permits only singleton soft-target overflow and rejects
  hard-cap failures. Empty creates in `server/src/engine.rs:157` publish an empty
  manifest without a Parquet object. Table/request/result limits remain shared
  limits. Catalog v1 reads, mixed v2 writes and retained historical objects remain
  compatible with the agreed forward-only migration contract.
- `server/src/api.rs:71` runs admitted operations through the detached Executor;
  runtime drain precedes worker shutdown in `server/src/main.rs:119`. Engine
  callers still must await operations; no new cancellation guarantee is assumed.

## V03a and D01

Inspected the let-chain changes in `server/src/commit.rs:157` and
`server/src/engine/partition.rs:67`, and the successful-spawn conditional in
`server/src/worker_pool.rs:143`. Guards, startup cleanup and errors retain their
behavior. Reviewed the small array/borrow changes in `server/tests/worker_pool.rs`,
`server/tests/engine_write.rs` and `server/tests/engine_read.rs`, and lexical
guard scopes in `server/tests/engine_catalog.rs` and `server/tests/health.rs`.
Assertions remain intact and locks release before awaited teardown.

D01 specification verdict: PASS. Reviewed `README.md`, `server/README.md`,
`TUTORIAL.md` and `orm/README.md` against the final paths above. They correctly
describe accepted `64KiB`, rejected `64 MiB`, integral unit syntax, closed-file
limits and singleton exceptions, empty manifests, nonretroactive targets, local
worker mapping and single-process ownership, serialized mutations with parallel
preparation and sequential uploads, unchanged aggregate limits, v1 reads/v2 writes,
lazy migration and old-binary rejection, and historical-object retention. The
exact partition acceptance command and current rubric links are present; the
live result remains explicitly pending.

Reused `PW-V03a.md` for passing fmt, strict all-target Clippy and rebuilt binary,
`PW-W04.md` for all 93 passing Rust tests and zero doctests, `PW-D01.md` for link
and example checks, and `scripts/notes/PW-E01.md` for six passing Python guards
and acceptance preparation. No Cargo, tests, link/example checks or other
verification reruns were needed. No AWS calls, E02 thread/note inspection,
credentials, dotenv access, agents, branches, commits or pushes occurred.

Unresolved gates: independent final quality audit and live E02 acceptance.
