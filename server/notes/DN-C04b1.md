# DN-C04b1 bounded control-root loading and initial creation

Implemented only new `src/cluster/store.rs`, `tests/control_store.rs`, this note,
and the narrow `pub mod store` declaration and module documentation in
`src/cluster/mod.rs`. Existing shared-main edits were preserved. No applicable
AGENTS.md was found in repository or ancestor searches. Applied TDD and unslop.
Read the distributed contracts, A05 including mini-2's C04b1 boundary, A08,
C04a2b/C04a3 notes, and the actual root/configuration/member codecs and storage
reader/history/fake contracts. Prior notes' checks are reported evidence only;
the results below were personally executed for this assignment.

## API and guarantees

`cluster::store::LoadedControl` privately owns a validated `Control` and its
nonempty opaque backend token. `root() -> &Control` is the only accessor. Clone
and equality retain the root/token association. Manual Debug exposes only root
revision/change UUID. There is no public token getter, mutable accessor or
arbitrary write API.

`Storage<B>::load_control(deadline: tokio::time::Instant)` performs one GET at
`ObjectKey::control()` through `Storage::read` with the existing
`MAX_CONTROL_BYTES = 32 * 1024 * 1024`. The existing streaming reader consumes
through EOF and probes at most one excess byte without appending it. The loader
passes the original bytes directly to `Control::decode`, preserving duplicate
field evidence. Empty tokens return Unavailable. Absolute deadline checks occur
before the GET, after the bounded GET/body future and after synchronous decode.
Decode cannot be preempted, but a result finishing beyond the deadline is
rejected. There is no new admission check on retained roots, history fetch,
scan or migration. ControlError InvalidRecord maps to InvalidCatalog and
LimitExceeded maps to LimitExceeded, with no raw diagnostics.

`Storage<B>::initialize_control(initial: &Control, deadline: Instant)` first
runs the validating bounded encoder and exact initial-state checks before any
backend call, even when a root already exists. Root/policy revisions must be 1;
admin sequence/head are 0/null; controller is epoch 0/released with null
owner/deadline; nodes/intents are empty; scheduler times/failures are zero and
low_since is null. The codec supplies generation 1 and fixed equal bounds in
0..1024. Autoscale desired must initially equal min. The caller supplies fresh
cluster/change UUIDs; this method cannot prove their freshness.

The first valid loaded root returns unchanged. It may have independent root,
policy and admin revisions, another initial policy, different template paths,
or even an unrelated cluster identity. Only definite NotFound allows one Create
with the already encoded candidate. A nonempty timely acknowledgement returns
that validated candidate with its exact acknowledged token. Conflict, Ambiguous
and an empty acknowledgement permit at most one fresh GET under the same
absolute deadline. Any valid observed root returns unchanged, including a
competitor's winner. This proves observation of durable state under the trusted
backend contract, not which creator won.

Failed reconciliation after a possibly applied write remains Ambiguous, including
missing/corrupt/unavailable/empty-token/expired reads. Failed reconciliation after
definite Conflict returns the read error. Other definite write errors propagate
without another GET or PUT. Expiration before Create produces Unavailable and
no PUT. A stalled possibly submitted Create produces Ambiguous, never a second
PUT or a fresh deadline. Caller cancellation adds no retry permission; backend
adapters must prohibit automatic HTTP PUT retries.

## Caller obligations and limits

Before using an observed root, runtime startup must separately verify cluster
identity/name and semantic loaded configuration, including backend identity,
credentials and TLS trust. Raw template/path comparison is not that check. No
outgoing template path or secret is opened here. An unrelated valid root can be
observed but cannot become attachment authority through this API.

This slice supplies no conditional replacement, owner/epoch transitions,
controller authority, admin ledger traversal/publication, credential loading,
A07 authentication, membership/readiness, public status or live network wiring.
Tests use the shared in-process fake CAS plus a test-local wrapper for barriers,
response faults and stalled streams. They establish no TCP, provider, runtime
attachment or full distributed guarantee. Independent specification review and
then quality review remain parent follow-ups; no child was delegated.

## Executed evidence

Tests preceded production code. The first focused command exited 101 with 17
E0599 missing-method errors for load_control/initialize_control. After the
implementation, six groups passed and the two-creator group failed with
Unavailable. Its wrapper mistakenly gated only the first GET at a two-party
barrier. Gating both captured initial GETs corrected the test, without a
production change. The subsequent expanded run passed all nine groups in
2.31 seconds. Targeted strict Clippy exited 0 in 13.25 seconds.

The nine groups cover ordinary Create/reload and exact existing later-state
preservation; causal two-creator actual CAS with both absences captured before
release, one generation and exact body/token equality; applied-before-lost-reply
and empty-ack reconciliation; conservative conflict/ambiguity/definite-error
classification and exact GET/PUT counts; invalid initial candidates before I/O;
fixed zero/1024 and initial autoscale; original duplicate JSON, corruption,
empty token, over-limit body and truncated transfer; stalled GET/body/Create,
expiry before Create and reconciliation under the original deadline; and a
codec-valid retained root beyond admission room. Changed tokens distinguish
loaded observations even when root bytes are identical. The final truncated
fixture delivers a complete codec-valid prefix and then an I/O error rather
than clean EOF, so prefix acceptance would fail the test.

All Cargo commands used the existing target sequentially from the workspace
root. Each test invocation used exactly:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_store
```

Each Clippy invocation used exactly:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_store -- -D warnings
```

Owned formatting and whitespace commands:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/cluster/store.rs server/tests/control_store.rs server/src/cluster/mod.rs
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/store.rs server/tests/control_store.rs server/src/cluster/mod.rs
git diff --check -- server/src/cluster/mod.rs server/src/cluster/store.rs server/tests/control_store.rs server/notes/DN-C04b1.md
python3 - <<'CHECK'
from pathlib import Path
for name in ('server/src/cluster/mod.rs', 'server/src/cluster/store.rs',
             'server/tests/control_store.rs', 'server/notes/DN-C04b1.md'):
    data = Path(name).read_bytes()
    assert data.endswith(b'\n'), name
    for n, line in enumerate(data.splitlines(), 1):
        assert line.rstrip(b' \t') == line, (name, n)
CHECK
```

No external edit or implementation blocker was required. No dependencies,
backend APIs, shared fixtures, other notes/plans, runtime or Python were edited.
No .env/secret read, cloud call, target cleanup, default AWS build, full suite,
fetch, branch/worktree, commit or push occurred. Cargo ownership is released at
delivery after the final checks below.

Final checks after strengthening the truncated-transfer fixture: all nine tests
passed in 1.99 seconds, and strict Clippy exited 0 in 3.08 seconds. Owned rustfmt
and tracked diff whitespace checks passed. An attempted no-index whitespace
loop stopped on exit 1 with no diagnostics because the new files differ from
/dev/null; an individual no-index check likewise returned 1 without diagnostics.
The explicit scoped byte check above then passed for all four files, including
untracked content. No production change followed the first green run.

Independent specification review passed without actionable findings. It inspected
the implementation and all nine test groups against the bounded loading/create
contract, including retained-root versus admission checks, reconciliation,
deadlines and uncertainty. The reviewer reused the recorded checks without
rerunning them or changing files. Independent code-quality review follows.

Independent code-quality review passed without important actionable findings.
It inspected all nine test groups and their causal barriers/deadlines, immutable
root/token association, complete reads and conservative one-attempt creation.
It reused reported tooling evidence without builds/tests or edits. Both review
stages are complete for this slice; attachment and distributed authority remain
outside its API.
