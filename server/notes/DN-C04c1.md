# DN-C04c1 bounded administration history

Implemented A13 assignment 1 only. Owned files are new
`src/cluster/admin_history.rs`, its private `admin_history/tests.rs`, this note,
and one crate-private declaration in `cluster/mod.rs`. Existing shared-main
work was preserved. No applicable ancestor/repository AGENTS.md was found.
Applied the requested test-driven-development and unslop skills. No delegation,
branch, worktree, commit, push, dependency fetch, secret read or cloud call.

## API and caller obligations

The crate-private `AdminHistoryBudget::new(caller_deadline)` captures the checked
minimum of the caller deadline and now plus five seconds. Already expired
budgets fail with Unavailable. `deadline()` returns that same absolute instant;
Copy never refreshes it. Each lookup has fixed caps of 4096 records and 32 MiB
of encoded decision input. Capture the budget BEFORE root loading and pass its
deadline to `load_control`. The root's existing separate 32 MiB bound is excluded
from the history allowance. A later publication implementation may perform only
one additional reconciliation scan under the original deadline.

`Storage<B>::lookup_admin(&LoadedControl, Uuid, &[u8], AdminHistoryBudget)` returns
`Result<AdminLookup, StorageError>`, with Found(ControlDecision), IdentityConflict
or Absent. Callers MUST authorize and strictly admit the original scale/autoscale
envelope before lookup. Lookup checks its 1 MiB body cap without copying or
canonicalizing the supplied bytes; it adds no policy parser. Found retains the
exact UTF-8 request/response strings and status. Debug uses the existing sanitized
decision Debug. Temporary dead-code allowances cover only the budget impl and
lookup method pending C04c3/G02 callers, not the module.

The supplied immutable root is the sole traversal anchor. Each fetched key must
match the decision ID; cluster and head sequence must match the root. The head
publication revision must not exceed the root revision. Parent sequence must
be exactly one lower and parent publication revision must not exceed the child's
base revision. Existing decision decoding checks local parent/null, positive and
checked base/publication relationships and exact body correlation. Root revision
gaps and a head different from change_id are valid. Strict sequence decrease
also detects cycles without a visited set.

A validated matching prefix returns immediately, including when older ancestry
is unavailable. Exact original bytes decide Found versus IdentityConflict.
Only validated null at expected sequence zero returns Absent. Referenced missing
objects map to InvalidCatalog; malformed identity/links/codec input also return
InvalidCatalog. Reader failures and deadline exhaustion return Unavailable;
record/encoded-byte exhaustion returns LimitExceeded. None proves absence.

The shared deadline is checked before and after GET/body, before decoding via
the post-read check, after decoding even if decoding failed, and at the terminus.
`timeout_at` covers pending GET and body futures. Synchronous decoding cannot be
preempted, but a late result is rejected. Each `Storage::read` uses the lesser
of the decision cap and remaining aggregate allowance. The existing reader
consumes through EOF and probes at most one excess byte. Counters are checked;
only one record's encoded/decoded representation is retained per iteration.
There is no root reload, write, token accessor, cache, GC, TTL, retry or mutation.

## Personally executed evidence

Tests preceded implementation. The initial compile-only red included missing
imports and the absent API; after correcting imports, the compile-only red was
solely the missing private budget/result/method API. These are not behavioral
failures. A minimal stub returning Absent then compiled and all six groups failed
behaviorally on expected Found/error assertions. The real implementation followed.

The first implementation run passed five groups and exposed an invalid negative
fixture: changing the parent's base without its publication revision prevented
fixture encoding. Correcting that fixture required no production change. The
next six-group run passed. Expanded fixtures added UTF-8/error-status replay,
escaped request spelling, additional codec-invalid links and stalled EOF. One
expanded run caught a fixture ordering mistake, attempting conflict lookup after
removing its record; moving that assertion before removal corrected it without
production changes. The final focused run passed all six groups in 4.07 seconds.

The groups exercise revision gaps and later policy, exact error-response replay
with exhausted root/policy revision counters, whitespace and changed-payload
identity conflicts, valid zero absence and matching prefix despite a missing
older suffix. Sixteen independent invalid fixtures cover wrong cluster/key/sequence,
head/parent revision relationships, malformed/truncated JSON, missing head/parent,
read failure, self/null parent and codec correlation/base overflow. Resource
cases fetch 4096 records before refusing the next GET, and use nine valid padded
large records to exceed the 32 MiB aggregate. The byte case asserts LimitExceeded
and a still-live deadline, separating payload limits from timeout failures.

The deadline group covers expired admission, caller body cap before GET, deadline
copy/capping, pending GET, complete valid body followed by an I/O failure, and
complete valid body with stalled EOF. Each pending future is directly owned by
lookup's timeout; no spawned or gated task is left behind. GET/PUT assertions
verify bounded reads and zero writes. All roots are encoded and loaded through
`load_control`; decision fixtures use the real encoder/decoder and shared Fake
GET, with a local wrapper only for incomplete/stalled transfers. No mutex spans
an await and no sleep establishes ordering.

All Cargo commands ran serially in the existing target from the workspace root:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::admin_history -- --test-threads=1
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test storage neutral_reader_bounds_and_partial_failure
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_store bounded_complete_original_reads_reject_corruption_and_empty_tokens
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/admin_history.rs server/src/cluster/admin_history/tests.rs server/src/cluster/mod.rs
```

The existing neutral-reader regression passed one test and confirms the single
excess-byte probe and partial-read error behavior without duplicating that suite.
The existing bounded-control-root regression passed one test in 0.04 seconds.
Strict targeted library Clippy with warnings denied passed in 8.55 seconds.
The first owned rustfmt check flagged declaration ordering in cluster/mod.rs;
moving only the new declaration to its sorted location corrected it. Final
owned rustfmt passed. No test rerun followed this declaration-only relocation.

Whitespace checks passed, including a direct scan of untracked owned files:

```sh
git diff --check -- server/src/cluster/mod.rs server/src/cluster/admin_history.rs server/src/cluster/admin_history/tests.rs server/notes/DN-C04c1.md
python3 - <<'PYCODE'
from pathlib import Path
for name in ['server/src/cluster/admin_history.rs', 'server/src/cluster/admin_history/tests.rs', 'server/src/cluster/mod.rs', 'server/notes/DN-C04c1.md']:
    data = Path(name).read_bytes()
    assert data.endswith(b'\n'), name
    assert b'\r' not in data, name
    for n, line in enumerate(data.splitlines(), 1):
        assert line.rstrip(b' \t') == line, (name, n)
print('Owned direct whitespace including untracked: PASS')
PYCODE
```

## Inspection and evidence limits

Inspected adopted A13 including its duplicate-scan correction, shared contracts
and C04/G02/testing rubric, A05 ledger, C04a3/C04b1 notes, actual ControlDecision,
Control, LoadedControl, typed keys, Storage/read, catalog history and Fake.
Prior notes' passing results are reported historical evidence only. The adopted
A11 generation-2 amendment remains C08c work; current fixtures follow the actual
generation-1 codec, and lookup neither rewrites nor interprets configuration.

Revision gaps model intervening renewal/member publications; this slice does
not execute those transitions or a publication race. In-process Fake evidence
establishes no TCP, cloud, runtime authority or peak-RSS guarantee. Encoded
root-plus-history permits up to 64 MiB plus bounded excess probes; decoded heap
and Fake chunk allocations are not measured. No full suite or broad passing
matrix was repeated. C04c2/c3, G02 authorization/routes, policy/publication and
independent parent specification/quality reviews remain follow-ups.

Cargo/build ownership is explicitly released on delivery.
