# DN-C04b2a pure ownership candidates

Implemented only `src/cluster/controller.rs`, its embedded private tests, the
crate-private module declaration in `src/cluster/mod.rs`, and this note.
Preserved existing shared-main edits. No applicable ancestor or owned-path
AGENTS.md was found. Applied TDD and unslop; read accepted A10 including its
explicit shared proposal UUID, shared distributed contracts, accepted A05/A08,
completed C04a2b/C04b1, and actual state/member/configuration and storage errors.
A11 remains an unadopted proposal and caused no changes.

## API and behavior

Crate-private `Owner { process_id: Uuid, epoch: u64 }`, `OwnershipChange` with
Acquire/Renew/Release, and `ownership_candidate(&Control, OwnershipChange,
u64, Uuid) -> Result<Control, StorageError>` provide pure metadata construction.
Owner identifies a process/epoch and grants no authority. Debug exposes only
those allowlisted identity fields and transition categories. Errors are existing
static StorageError variants, with no configuration or provider diagnostics.

The constructor validates the base and complete candidate through existing
Control validation and bounded encoding. It clones the base and changes only
root revision via checked +1, the explicit proposal UUID, and controller fields.
It rejects reuse of the base change UUID as Conflict. The caller must select a
fresh random proposal UUID and share it with future guard admission and store
preparation. This function generates no UUIDs and cannot prove global freshness.

Acquire accepts released state or a held lease at or after its exact deadline,
rejects any held same-boot acquisition, increments epoch without reset, and adds
the lease duration with checked arithmetic. Renew requires the exact held
owner/epoch and a checked deadline strictly greater than the old deadline;
local expiry alone does not reject renewal. Release requires that same exact
held pair, clears owner/deadline, preserves epoch, and performs no deadline
addition. Invalid transitions return Conflict; ControlError InvalidRecord maps
to InvalidCatalog and LimitExceeded maps to LimitExceeded. Checked revision,
epoch or deadline overflow returns LimitExceeded without changing the base.

Ownership allocates no records and performs no new-admission check. Typed and
encoded comparisons confirm all unrelated fields and A08 credits remain equal,
including nonempty nodes/intents, independent policy/admin sequences and
scheduler backoff. A retained root beyond new-admission room can still acquire,
renew and release. No shared fixture or existing record API changed.

## Executed evidence

Tests preceded the implementation. A temporary rejection-only function body
allowed a behavioral red run: all five groups compiled and failed, with
unexpected Conflict on successful transitions and expected LimitExceeded cases.
That command exited 101. Replacing the temporary body with the implementation
made all five groups pass, zero failures, 20 unrelated tests filtered out, in
3.71 seconds. No production change followed that green run.

The groups cover released acquisition and epoch continuity; just-before/exact
expiry takeover and same-boot denial; wrong process/epoch and released-state
renew/release rejection; strict advancement and renewal after local expiry;
release with maximum Unix time; complete unrelated-field preservation;
retained-root progress without admission room; checked root/epoch/deadline
exhaustion; duplicate proposal UUID; malformed constructed name/controller;
retained-count validation and error/Debug redaction. Rejection fixtures validate
before transition checks, except intentionally malformed/over-limit bases.
The over-limit base has unique sorted valid records and exceeds only the
retained intent count. No fake backend was needed.

Every Cargo invocation ran sequentially in the existing target from the
workspace root. The test command ran once red and once green:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Strict library Clippy passed without warnings, exit 0, in 8.46 seconds.
Owned rustfmt and scoped whitespace checks passed, including untracked content:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/cluster/controller.rs server/src/cluster/mod.rs
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/controller.rs server/src/cluster/mod.rs
git diff --check -- server/src/cluster/controller.rs server/src/cluster/mod.rs server/notes/DN-C04b2a.md
python3 - <<'CHECK'
from pathlib import Path
for name in ('server/src/cluster/controller.rs', 'server/src/cluster/mod.rs',
             'server/notes/DN-C04b2a.md'):
    data = Path(name).read_bytes()
    assert data.endswith(b'\n'), name
    for n, line in enumerate(data.splitlines(), 1):
        assert line.rstrip(b' \t') == line, (name, n)
CHECK
```

## Limits and handoff

No conditional I/O, prepared submission, local guard, timers, controller loop,
secret loading or callback authorization is implemented. A successful or failed
candidate cannot authorize a callback; future exact-token CAS and local guard
remain required. These tests establish pure state behavior, with no network,
provider or distributed authority claim. Independent reviews remain parent tasks.
The authorized module-only dead_code allowance must be removed when C04b2b
lands store callers. There are no other lint allowances.

No implementation blocker required edits outside the assigned files. No
runtime, plans/contracts, dependency, fixture, Python, branch/worktree, commit,
push, target cleanup, default AWS build, full suite, fetch, delegation,
.env/secret read or cloud operation occurred. Cargo ownership is released at
delivery after final checks.
