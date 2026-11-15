# DN-R01b2a: private bounded payload scratch

Implemented only R01b2a after reading `plans/distributed-contracts.md`, DN-A01,
DN-R01a and DN-R01b1. The shared contract overrides historical embedded DTO,
journal and no-op proposals. Applied the test-driven-development and unslop
skills. No applicable AGENTS.md was found in the repository's filesystem
ancestors or owned directories. Existing extensive shared uncommitted work was
preserved.

Changed files are new `server/src/node/spool.rs`, new
`server/tests/node_spool.rs`, this note, and the single `pub mod spool;` export
in `server/src/node/mod.rs`. No job/frame, engine, runtime, configuration,
storage, manifest or dependency edits were made. No delegation, branches,
worktrees, commits, pushes, dependency installation, .env access or cloud calls.

## API and ownership

- `SpoolLimits::new(bytes: u64, files: u64)` rejects zero. Private fields prevent
  invalid limits. Checked additions enforce quotas without overflow. Explicit
  integer budgets have no imposed 8 MiB, 80 MiB or 384 MiB ceiling and cause no
  worst-case allocation or reservation.
- `Spool::new(parent, limits)` explicitly creates one UUID-named private directory
  under an existing caller-supplied trusted parent, using POSIX mode 0700.
  Canonicalization preserves identity for relative parents. There is no default
  path lookup or import/global side effect. The caller must protect the parent
  and private descendants from external mutation for the manager's lifetime.
- `Spool::operation(limits)` creates independent operation accounting, sharing
  the manager's process budget. The runtime must create and share one manager
  per process; unrelated managers deliberately have independent budgets.
  An operation creates neither a directory nor an OS descriptor.
- `SpoolOperation::writer(payload_cap: u64)` requires a positive explicit cap,
  reserves a file slot against both budgets, then opens a generated filename
  with create_new and POSIX mode 0600. Failed creation rolls back the slot.
- `SpoolWriter::append(&[u8])` reserves bytes against both budgets before writing
  borrowed bytes. Manager then operation is the consistent lock order; both
  next totals are checked before either changes. Locks never span disk I/O or
  awaits. Every append failure poisons the writer. It conservatively retains
  the entire attempted chunk charge across a partial/error write until cleanup.
- `SpoolWriter::finish(self)` closes its descriptor and returns `SpoolPayload`.
  A poisoned writer cannot finish successfully. Unfinished/failed writers close
  and remove their file on drop. No implicit completion, flush durability or
  fsync is claimed. A zero-byte payload is permitted with a positive cap.
- `SpoolPayload::len()`, `is_empty()` and `open()` expose immutable length and
  a read-only `SpoolReader`. Completed payloads retain no descriptor. Each open
  creates an independent position and returns a bounded `Read + Seek` view.
  Reads stop at payload extent, premature physical EOF is an error, and seeks
  outside 0..=length fail without changing the logical position. No writable
  File/path escape or unaccounted descriptor clone is exposed.

Payloads and readers share the owned file through Arc; each file retains its
operation, which retains the root. Dropping the original manager or operation
cannot delete a live file. Reader descriptors close before their owned-file
reference drops. The final owner removes the file before releasing its exact
reserved byte/file charges. Successful payload charges equal their length.
Writer error charges can exceed physical length by the attempted failed chunk.

All filesystem I/O is synchronous and belongs on the designated bounded CPU/I/O
executor, never a Tokio reactor thread. This slice starts no tasks or executor.

## Cleanup errors and privacy

Successful removal and already-absent files release quota. Any other deletion
failure leaves quota charged for that manager's remaining lifetime, including
when other operations survive. Drop cannot return a cleanup error; no scanner,
retry task or garbage collector is introduced. Final root drop attempts a
nonrecursive remove_dir after all operations/files/readers are gone. It can
succeed if an earlier external obstruction has disappeared, but retained files
or other errors may leave an ephemeral directory behind. Failed file deletion
is not later reclaimed into available capacity, even if an operator intervenes.

SpoolError has only InvalidConfiguration, Capacity, Io and Poisoned, with fixed
Display text. Public resource Debug implementations expose only static type
names; limits show only integer budgets. Read/Seek errors use fixed sanitized
text and discard underlying filesystem errors. No paths, OS strings or payload
bytes are included in returned errors or Debug.

## Focused evidence

Grouped behavior tests were written before the production module. Every Cargo
invocation ran sequentially against existing `server/target`, with this exact
prefix and flags; no full suite or AWS/default-feature build was run:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Initial red:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_spool
```

Exit 101, E0432 unresolved import `meshdb_server::node::spool`. This was a
compile-time red for the missing module/API, not a runtime assertion failure.
After implementation the five groups passed. After strengthening creation
rollback and reader lifetime assertions, final exact commands/results were:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_spool
5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; 0.01s. Exit 0, no warnings.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::spool::tests
1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; 0.00s. Exit 0, no warnings.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_spool -- -D warnings
Finished dev profile in 6.95s. Exit 0, no warnings.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/spool.rs server/src/node/mod.rs server/tests/node_spool.rs
Exit 0.

git diff --check -- server/src/node/spool.rs server/src/node/mod.rs server/tests/node_spool.rs server/notes/DN-R01b2a.md
Exit 0. A separate content whitespace check also passed for these untracked files.
```

Evidence covers exact byte/file and per-payload boundaries, independent operation
limits, small bounded append chunks, barrier-controlled real writes by four
independently admitted operations under shared process byte/file quotas,
unfinished/error cleanup and capacity recovery, independent read/seek positions,
private modes, immutable length, zero-byte payloads, last-reader quota retention,
nonempty reads after manager/operation/payload drop, final root cleanup, failed
initialization/open, creation rollback, already-absent file cleanup and failed
deletion retaining quota. A narrow private unit test replaces only the owned
writer descriptor with a read-only descriptor, proving write-error poisoning
and retained attempted-byte quota without adding a public fault hook. It does
not simulate an actual partial disk write. The deletion-failure test replaces
one owned file with a directory; all manipulation stays inside owned temporary
state. No giant allocations, sparse giant files or hundreds-of-MiB writes.

These are graceful cleanup and controlled filesystem-fault tests. They do not
prove cleanup after process death, power loss, arbitrary disk faults or durable
recovery. Scratch is ephemeral and never a journal.

## Remaining work

Independent specification and code-quality reviews passed without actionable
findings, reusing the recorded checks. The deletion-failure test directly proves
file-slot retention; retained byte charging is supported by source inspection.
This slice
adds no SHA256, frame/stream grammar or semantic completion, network adapters,
JSON/Parquet validation, engine publication, executor integration, runtime
defaults or configuration fields. The next slice must combine actual payload
length/digest and semantic checks with framing completion before accepting a
result. Runtime work must choose explicit budgets and wire the single shared
manager to bounded I/O execution.

Cargo ownership is released on delivery. Available disk remained approximately
1.5 GiB; no target cleanup or alternate target directory was used.
