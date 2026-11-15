# DN-C04a2b durable control root and first-stop capacity

Implemented only the pure root codec and derived A08 accounting, against
`plans/distributed-contracts.md` and accepted DN-A05, DN-A07 and DN-A08. Read
completed DN-C04a1/DN-C04a2a notes and their actual member/configuration codecs
and tests. No applicable AGENTS.md was found along the owned paths or ancestors.
Applied the local TDD and unslop skills. Existing shared-checkout edits were
preserved. Cargo ownership is released at delivery. Independent specification
and quality reviews are parent follow-ups.

## Public API

`meshdb_server::cluster::state` exports plain public-field `Control`, `Config`,
`Policy`, `Controller` and `Scheduler`, and strict string enums `Provisioning`,
`PolicyMode` and `LeaseState`. Each record exposes `decode(&[u8])`, `encode()`
and `validate()`, returning static sanitized `ControlError::{InvalidRecord,
LimitExceeded}`. Record Debug implementations show only the type name, even for
invalid constructed records. Records support Clone, Eq, Serialize and
Deserialize. Direct serde decoding establishes shape and lexical rules; callers
must use the codec methods for validated trust-boundary values.

Control contains exactly A05's root fields, with no persisted credit counter.
All nested objects reject arrays, duplicates, unknown fields and missing required
nulls. Integers use direct u64 decoding, UUIDs require canonical lowercase
hyphenation, and string enums reject object encodings. Root input is capped
before parsing at the separate `MAX_CONTROL_BYTES = 32 * 1024 * 1024`.
The collection visitors stop retaining elements at 2048 nodes or 3072 intents;
an extra element is discarded through IgnoredAny and rejects the array. No
unbounded member Vec is decoded first. Combined counts are checked after parsing.
Encoding writes through a capped writer before extending its byte buffer.
Decode revalidates the bounded encoding, as does validate.

The four small global records have independent 4096-byte codec bounds. Config
requires generation 1 and valid digest representations; its adapter is null or
exactly meshdb.callbacks.v1. Control additionally checks managed/external adapter
agreement, external fixed policy, exact Template.fingerprint(), positive root
and policy revisions, name syntax, fixed/autoscale bounds, controller null/held
and epoch consistency, and admin_seq/head correlation. A released controller may
retain a positive epoch; a held one requires a positive epoch and both owner and
deadline. A zero deadline is structurally representable, with no time authority
inferred. Root revision, policy revision and admin sequence remain independent.
There is no head/change_id equality, historical increment rule or database
publication ownership requirement.

The root invokes public Node/Intent/Template/Timing validation, including each
record's individual encoded bound. No visibility change or prior schema edit
was necessary. Managed capacities match the pinned outgoing template. Arrays are
sorted and unique; launches, current incarnations and member resources are
unique. Retired incarnations exclude all current bindings. Resource identities
observed in retained starts cannot collide across different nodes, even while
member recovery is incomplete. Every managed node retains exactly its original
start, with request equal to its launch and the stored template fingerprint.
Every intent resolves to that managed node/launch. Stops require its exact known
resource and incarnation, including equal null incarnation for never-registered
bindings. External members have no lifecycle intents. A start or member with
unknown resource remains representable; known values must agree. Registered
nonready and draining members, including resource-unknown late registration,
remain representable without deadline/phase/heartbeat evidence checks.

## Accounting and fixed allowance

`Control::capacity() -> Result<Capacity, ControlError>` validates the whole
candidate and returns `nodes`, `intents`, `credits`, `retained_bytes` and
`admission_bytes`. `Control::check_new_admission()` additionally checks the
whole candidate's augmented counts and byte budget. Neither decode nor ordinary
encode/validate imposes admission-only inequalities.

For every validated node/start identity, any retained matching stop consumes its
credit regardless of state or outcome. Otherwise a failed start with not_invoked
or terminated is the persisted terminal witness releasing that credit. Completed
resource_known starts retain credit. Lost/stopped/draining phases, expired
numeric deadlines, unknown resources and idle proof do not release it. There is
no stored count, incomplete-stop predicate or credit restoration on stop failure
or completion.

Checked arithmetic implements:

```text
F = 262144
B = F + 8192*N + 4096*I
A = F + 8192*N + 4096*(I+C)
```

All candidates check N<=2048, I<=3072, N+I<=4096 and B<=33554432,
as well as actual bounded output. New admissions additionally check I+C<=3072,
N+I+C<=4096 and A<=33554432. All additions/multiplications that form
budgets or combined counts are checked. Credit counting follows already bounded
retained collections.

The fixed allowance covers maximum output, not current string lengths. Template
contributes at most 65536 bytes; Timing and the four global records contribute
at most 5*4096=20480 bytes, including maximum scalar widths and string escaping.
The remaining root keys/scalars, UUID strings, 64-byte ASCII name, brackets and
commas outside member arrays fit conservatively in 4096 bytes. At most 4096
additional bytes cover member-array separators. This totals at most 94208 bytes,
well below F=262144. Member object bytes are separately covered by N*8192 and
I*4096, including their escaping, resources, endpoint, 16 retired incarnations,
session/drain/delivery, maximum decimal u64 widths and errors. The independent
record codec bounds remain decisive for any combination of mutable fields.
Under the retained or augmented combined count bounds, the largest B/A is
24.25 MiB at N=2048 and intent count 2048. The separate 32 MiB writer remains
mandatory. Job framing, public response and backend limits are unchanged.

## Test evidence

Tests preceded the new API. Initial focused Cargo run exited 101 with E0432
because cluster::state did not exist. After implementation, two runs encountered
a borrow error in the fixture while pushing an intent and reading the template
hash from the same Value; caching the fixture hash corrected that test issue.
The next run produced seven passing groups and one behavioral failure: a managed
member with capacities different from the pinned template encoded successfully.
Adding that cross-record check made all eight groups pass. The expanded final
run passed nine groups, zero failures, in 2.93 seconds.

The groups cover initial managed/external roots, independent sequences and
released epoch retention, held ownership and autoscale, strict nested objects,
duplicates, required nulls, exact integer and UUID spellings, independent
cross-record corruptions through both decode and constructed encode, orphan
starts/intents, resource/incarnation/retirement bindings, real template hashes,
late draining recovery, size/count limits and sanitized errors/Debug.

A representative 1024-node/completed-start fleet has B=12.25 MiB,
C=1024 and A=16.25 MiB. Beginning all drains preserves its budget; all 1024
first stops encode and reload with C=0 and unchanged A. A second fleet retains
1025 stops for one node, saturating both augmented count limits while other
nodes retain credits. An additional unreserved identity fails admission while
remaining structurally loadable. Maximum-length resource/endpoint/error fields,
16 retired incarnations, session/drain/delivery metadata and maximum u64 mutable
fields preserve another node's credit and the accounting. Its exact first stop
converts a credit without extra admission room and reloads. Converting all
remaining first stops reaches N=1024/I=3072/C=0 and reloads at 4096 records.
Lost/ambiguous starts retain credit; terminal witnesses release it; failed and
completed stops never restore it. These are pure candidate arithmetic checks,
not evidence that a lifecycle event occurred or a provider terminated.

All Cargo commands used existing server/target from the repository root, with
this exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands, with that prefix on every invocation:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_state
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_state -- -D warnings
```

Final focused tests passed all nine groups. Strict targeted Clippy exited 0
without warnings, in 9.36 seconds. No member/configuration body changed, so their
previously reviewed regression suites were not repeated. Owned formatting and
whitespace commands also passed:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/cluster/state.rs server/tests/control_state.rs server/src/cluster/mod.rs
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/state.rs server/tests/control_state.rs server/src/cluster/mod.rs
git diff --check -- server/src/cluster/mod.rs server/src/cluster/state.rs server/tests/control_state.rs server/notes/DN-C04a2b.md
```

## Deferred transition obligations

Codec acceptance proves structural/current consistency. It proves neither
historical identity continuity nor provider truth, authenticated reachability,
drain eligibility, lease authority, callback entry, termination or network
readiness. Other auth/storage/lifecycle fingerprints are representation-checked;
semantic derivation and attachment remain separate work.

Later pure transitions must allocate a managed node and original start atomically,
preserve immutable launch/resource/template/session bindings, establish definite
pre-entry/termination evidence before terminal witnesses, forbid revival of those
witnesses, retain all relevant records and prove C never increases on retained
updates. First-stop conversion must be exact to its eligible node/launch/resource,
increase I by one and decrease C by one atomically. New unreserved identities
must pass whole-candidate admission checks. Repeated stops reuse their existing
identity; an extra stop after failure requires ordinary admission. A CAS loser
must reload/adopt the retained stop, with owner/epoch and exact loaded-token
checks in the later layer. No pure candidate here proves CAS-race convergence.

No storage/CAS/controller/admin-ledger, network, secret loading, provisioning or
actual lifecycle transition was implemented. No public/runtime wiring, dependency
change, plan edit, .env/cloud call, full/default build, fetch, delegation,
branch/worktree, commit or push occurred. Only state.rs, control_state.rs,
this note and the narrow pub mod state declaration were edited.

Independent specification review subsequently passed without actionable
findings. It inspected all nine test groups, strict schemas, cross-record
bindings, recovery-state representation and separate A08 inequalities. It
confirmed that the 94,208-byte conservative global allowance fits within
256 KiB. The reviewer reused recorded tests/tooling without reruns or edits.
Historical transitions and persistence remain deferred; quality review follows.

Independent quality review subsequently passed without important findings. It
inspected strict decoding, bounded writers, checked arithmetic, exact bindings,
recovery-state representation and separate admission checks, plus the nine
actual test groups. It reused recorded verification without tests, builds or
probes. This completes the pure-codec slice, not historical transitions,
persistence, provider truth or distributed authority.
