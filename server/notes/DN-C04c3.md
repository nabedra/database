# DN-C04c3 consumed administration publication

Implemented adopted A13 assignment 3 and its two-scan correction only. New files
are `src/cluster/store/admin.rs`, its private `admin/tests.rs`, and this note.
`cluster/store.rs` gains a private declaration and crate-private reexports.
Removed only the obsolete item-level dead-code allowances and their comments
from `admin_candidate` and `lookup_admin`; their bodies are unchanged. The
budget constructor's allowance remains because G02 still owns budget creation.
The new prepared implementation and unused reexports have narrow temporary
allowances pending that caller. Shared uncommitted work was preserved.

Read shared contracts, rubric C04/G02/testing requirements, adopted A13 including
its correction, A05/A08/A10 context, reviewed C04c1/C04c2 notes and actual
admin/history/store/publication/Fake code. No applicable AGENTS.md was found.
Applied TDD and unslop. No children, branches, worktrees, commits, pushes,
dependency changes/fetches, secret reads, network/cloud calls, Python changes,
target removal, schema changes or public facade were used.

## API and obligations

Crate-private `PreparedAdmin::prepare(base: LoadedControl, request_id: Uuid,
exact_request: &[u8], proposal_id: Uuid) -> Result<Self, StorageError>` calls the
reviewed pure candidate builder and retains the same immutable base/token,
exact bounded candidate encoding, decision and original decision encoding.
It has no Clone, setter, token getter or serialized-candidate input. Its Debug
prints only proposal UUID and publication revision. Generation-1 configuration
is preserved opaquely; C08c owns the coordinated generation-2 amendment.

`publish(self, &Storage<B>, AdminHistoryBudget)` returns
`Result<AdminPublicationOutcome, StorageError>`, with Terminal(ControlDecision),
IdentityConflict, NeedsRecompute or Unknown. It performs no initial lookup.
It attempts one typed immutable Create, then one typed root Match using the
captured token and exact prepared candidate. Conflict, Ambiguous and empty
Create acknowledgments permit one complete bounded read comparing the entire
original encoding. Equivalent decoded JSON is insufficient. Failure prevents
root submission. Nonempty acknowledgments must arrive before the deadline.

Root Conflict or possible application permits one fresh bounded root load and
one reachable-history scan. Found returns the recorded terminal response even
after later commits. Different original bodies return IdentityConflict.
Complete absence after definite rejection returns NeedsRecompute. Incomplete
reconciliation and absence after possible application return Unknown. Definite
pre-application adapter errors retain their categories. All reads and writes
share the original absolute deadline, including immutable confirmation's
separate MAX_CONTROL_DECISION_BYTES cap. No retry or recomputation occurs.

The caller MUST authorize and strictly admit first, create the budget BEFORE
initial root GET, then search that captured root BEFORE prepare and all counter
arithmetic. Found returns the exact terminal record, even with exhausted
counters. IdentityConflict and incomplete history permit no preparation/write.
Only complete Absent permits preparation using that same LoadedControl. That
initial scan and optional reconciliation are the only two history scans.

A before-root error says nothing about earlier same-request work. Neither an
orphan decision, cancellation, wall expiry nor guard state proves rollback or
fences a pending PUT. Explicit caller-controlled recovery must reload/search,
retain the SAME request UUID/exact bytes, and choose a fresh proposal UUID.
A winning fresh CAS consumes the old token; if the original wins first, fresh
CAS loses and reconciles. Only acknowledged CAS or reachable terminal evidence
resolves uncertainty. Runtime supervision, routes, authorization, controller
loops and independent specification/quality reviews remain parent-owned.

## Test provenance and evidence

Five grouped tests preceded implementation. The first compile-only failure
reported absent API and missing test imports. This is distinct from behavioral
red. An Unknown-returning stub then compiled and all five groups failed on
expected missing Terminal/write/count behavior. The real implementation passed
three groups; two read-count assertions overcounted by one. Tracing the root
and history reads corrected those expectations without a production change.
Seven expanded groups then passed. Subsequent refinements cover all exhausted
counters, lost Create replies with byte-different valid JSON, and additional
causal gates. No further production behavior changed.

Tests use real codecs, loaders, history and Fake conditional writes. Both
competing candidates capture the same token and wait before application;
exactly one wins and the loser reconciles before explicit reload/recompute.
A lost applied root reply is held while a non-admin scheduler root and another
admin decision commit, then returns the exact earlier response through history.
Delayed provider work survives an ambiguous client reply as a test-owned pending
write, driven locally through an explicit before-application gate and Fake CAS.
Both original-first and recovery-first orders are checked. This wrapper models
provider queuing in process and does not establish transport cancellation.

Other cases cover identical-policy durable replay, exhausted root/policy/admin
counters, different-body replay, initial incomplete history with zero writes,
immutable Conflict/lost/empty acknowledgments, exact versus codec-valid padded
encodings, confirmation byte cap, missing/corrupt/unavailable reconciliation,
definite adapter errors, identity conflict on reconciliation and pending root
work before/after application until the original deadline expires. Exact
GET/PUT/token counts prove bounded scans, confirmation and no resubmission.
Gate waits are bounded; tests use joined local futures with no spawned handles,
no detached provider/server work, no ordering sleeps and no mutex across awaits.
Existing reader/error matrices were not duplicated. These are in-process storage
and metadata results, not TCP/process/cloud, controller authority or RSS evidence.

## Commands and results

Every Cargo invocation used this exact environment and common arguments in the
existing target, serially in the exclusive build slot:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo <command> --manifest-path server/Cargo.toml --locked --offline --no-default-features <arguments>
```

Focused publication command, including compile/stub red and green iterations:
`test --lib cluster::store::admin -- --test-threads=1`.
Seven groups passed in 0.63 seconds before the final gate-order refinement.

Directly related regression commands:
`test --lib cluster::admin_history::tests::replay_interleaved_revisions_exact_bytes_and_prefix -- --exact`
and
`test --lib cluster::admin::tests::identical_policy_keeps_its_independent_revision_even_at_max -- --exact`.

Strict production-library lint command: `clippy --lib -- -D warnings`.
Its initial run found only the unused pending-G02 reexports. A narrow allowance
on those reexports resolves that diagnostic. No --tests Clippy or broad suite
was run; known unrelated test diagnostics remain parent cleanup.

Final publication run passed all seven groups in 0.64 seconds after the final
gate-order refinement. The history replay regression passed one test in 0.01
seconds; the identical-policy regression passed one test in 0.03 seconds.
Final strict production-library Clippy passed in 8.54 seconds.

Owned formatting passed with:

```sh
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/store/admin.rs server/src/cluster/store/admin/tests.rs server/src/cluster/store.rs server/src/cluster/admin.rs server/src/cluster/admin_history.rs
```

`git diff --check` over the six owned paths passed. A direct Perl scan of those
same paths, including new untracked files, also passed final-newline, CR and
trailing-space/tab checks. No whole-repository test or format command was run.

Cargo/build ownership is explicitly released on delivery. No build is running.
