# DN-R03a3a checked sender sequences

Implemented only adopted DN-A18 assignment 1 on the shared main checkout.
Independent specification review followed by quality review remains parent-owned.
C03a review was independent and read-only. No delegation occurred.

Read the distributed contracts, R03/testing rubric, adopted A18 including the
corrected credential ownership and final lifecycle/integration limitations,
relevant A07/A15/A17, completed R03a1/R03a2a/R03a2b1/b2/b3 notes and the closed
DN-test-lints note. Applied test-driven-development and unslop skills. Repository
and ancestor discovery found no applicable AGENTS.md.

## API and exact changes

Only these six files were written:

- New `server/src/node/auth/sequence.rs` defines crate-private SenderSequence,
  `for_worker(Uuid, &Sender, &WorkerSigningKey) -> Result<Self, Error>`,
  `for_api(Uuid, &Sender, &ApiSigningKey) -> Result<Self, Error>` and
  `stamp(&mut self, Auth) -> Result<Auth, Error>`.
- New private `server/src/node/auth/sequence/tests.rs` contains five grouped tests.
- `server/src/node/auth.rs` adds only the private sequence module and narrow
  crate-private SenderSequence reexport, and removes the consumed Exhausted
  dead-code allowance and its obsolete comment.
- `server/src/node/auth/wire.rs` adds only
  `pub(super) Auth::with_sequence(self, u64) -> Result<Auth, Error>`.
  It rejects zero and replaces only the private Number field.
- `server/src/node/auth/credentials.rs` adds only
  `pub(super) ApiSigningKey::cluster_id(&self) -> Uuid`, returning retained cluster.
- This note.

Pre-edit copies of auth.rs, wire.rs and credentials.rs are retained at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r03a3a-9ofvxmb5`.
A direct snapshot diff confirms precisely the changes above, preserving every
existing method body, validator, private DecodeRecord and non-Deserialize record
boundary. Existing tests/helpers, Cargo/lock/dependencies, plans, controller,
verification/content/eligibility, membership, payload and runtime were not edited.
Shared preexisting modified/untracked work was preserved.

SenderSequence privately retains the immutable cluster and complete checked
Sender, with last-issued u64 initially zero. Constructors compare exact role and
public key; API also directly checks supplied cluster against the retained key
cluster before construction. No temporary signing record is used. stamp checks
cluster and complete sender before checked_add(1), then moves Auth through the
checked replacement and records the issued number. Mismatch returns Invalid
without advancement. MAX is issued once; subsequent matching stamps permanently
return Exhausted. No route/target map, response counter, reset, setter, Clone,
Serialize or Deserialize is provided. Debug is exactly `SenderSequence`.

Item-specific temporary dead_code allowances cover the struct and each of its
three methods until a production boot owner consumes them. One unused_imports
allowance covers only its pending crate-private reexport. No module lint
suppression was added; the existing credential/wire allowances were preserved.

## Test provenance and behavior

Tests were written before implementation. The first focused Cargo run compiled
Err(Invalid) constructor/stamp/with_sequence placeholders. All five groups failed
behaviorally on valid constructor calls, exit 101, after 37.77s compilation and
0.00s tests. Its only warning was the placeholder's unused Role import. This
observes constructor failure, not every later assertion or negative control.
There was no missing-API compilation failure.

After implementation and correcting the alternate target's synthetic verifier
to match its changed session, all five groups passed after 26.04s compilation,
0.05s tests, with no warnings. API process/key/role mismatch assertions and the
post-exhaustion wrong-cluster assertion were added afterward. Those assertions
have green-only evidence; no failing-before claim is made for them.

The tests use checked canonical Auth, real RequestContext::for_send, generation-2
Control encode/decode, actual retained context Auth signing, verify_request and
check_request_content. One counter alternates worker open/close routes or API
readiness/drain routes and target sessions. Complete canonical claims compare
equal after changing only sequence, and original content bytes remain exact.
The supplied roots are checked synthetic target commitments, not fresh authority
or route-admission proofs. No fake storage or path inclusion is used.

Negative controls vary cluster, complete worker node/session/process-incarnation/
key, API process/key and role with otherwise valid checked records. Every
mismatch is followed by a successful stamp proving no number was spent. API
construction rejects both a wrong supplied cluster and a key derived for another
cluster even when the sender public key matches that foreign key. Role tests
include matching key bytes under the wrong role. Failed context construction
and signing with another key leave issued numbers spent. Private test access
sets last to MAX-1, checks MAX canonical round-trip, wrong-cluster Invalid before
overflow, and three persistent Exhausted results. Negative trait guards and
Debug before/after stamping protect encapsulation/redaction. Existing wire
negative Deserialize guards remain unchanged. No crypto/codec matrix was copied.

## Commands and results

Every Cargo invocation used existing `server/target` and this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The initial red and first green used, each with that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::sequence::tests
```

The final focused tests and necessary regressions ran sequentially, with the
prefix applied separately to every Cargo command:

```sh
for meshdb_test_filter in node::auth::sequence::tests node::auth::wire::tests node::auth::credentials::tests node::auth::verify::content::tests; do
  CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib "$meshdb_test_filter" -- --test-threads=2 || exit $?
done
```

Final focused run passed five groups, 172 filtered, 0.05s after 23.92s
compilation. Regressions passed eight wire groups, 169 filtered, 0.01s; four
credential groups, 173 filtered, 0.01s; and twelve content groups, 165 filtered,
1.83s. The loop exited 0 with no warnings. These protect the changed checked
record/key accessors and actual context/parser composition, not a broader suite.

Strict Clippy ran once after implementation, using the same prefix:

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

It passed without diagnostics in 19.14s. The closed broader test-lint backlog
was not reopened and no unowned fixes were needed.

Owned formatting and final check used:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/node/auth.rs server/src/node/auth/sequence.rs server/src/node/auth/sequence/tests.rs server/src/node/auth/wire.rs server/src/node/auth/credentials.rs
rustfmt --check --edition 2024 --config skip_children=true server/src/node/auth.rs server/src/node/auth/sequence.rs server/src/node/auth/sequence/tests.rs server/src/node/auth/wire.rs server/src/node/auth/credentials.rs
git diff --check -- server/src/node/auth.rs server/src/node/auth/sequence.rs server/src/node/auth/sequence/tests.rs server/src/node/auth/wire.rs server/src/node/auth/credentials.rs server/notes/DN-R03a3a.md
```

Formatting and diff checks passed. Direct Python byte assertions checked all six
owned files, including untracked files, for trailing whitespace and exactly one
final newline. Removing just the two authorized helper additions reproduced the
wire/credential snapshots byte for byte; the auth.rs diff contains only the
module, reexport and consumed Exhausted allowance removal. Initial filesystem
headroom was 2.3 GiB and final headroom was 2.1 GiB.

## Limits and later owner obligations

This allocation helper proves local identity binding and monotonic issuance only.
It cannot prevent two counters for the same sender, cloning a stamped Auth,
signing the same record twice or deliberately reconstructing/reusing a sequence.
Later boot wiring must construct exactly one counter after fixing identity and
share it under one short mutex across all routes, targets and enrollment retries.
Worker memory loss requires replacement identity rules; API boots require fresh
process UUIDs even when using the same derived key. Shutdown stops new stamps
and settles accepted work before dropping the owner.

Build RequestContext from the stamped Auth and exact outgoing bytes, sign
context.auth(), and send context.initial_bytes(). Context/signing/transport/test
failure never rolls back an issued number. Explicit retry permission, original
mutation UUID/bytes and C02 uncertainty reconciliation remain required. No hidden
retry or operation deduplication guarantee is introduced.

Receiver replay and consuming request/reply handoffs remain assignments 2/3.
Fresh complete-root ordering, live barrier/permit/capacity checks, fallible
reservation before replay consumption and infallible accepted-owner installation
under one admission mutex remain later R03a3/R04 work. Supervised bounded prelude
settlement, coordinator-pool validation, checked CONTROL transfer with the same
advanced FrameReader, HTTP trailers/body errors/real EOF, payload cleanup,
queued-start/result checks and C03/C06 live evidence are still required.
No runtime, network, replay, membership, final admission, aggregate authentication
or distributed acceptance completion is claimed.

No dependency fetch, default/AWS build, target cleanup, real secret/profile/.env
read, cloud/network call, branch/worktree, commit or push occurred.


Cargo/build and Rust implementation ownership is explicitly RELEASED to the
parent. No interface or tooling blocker remains for this slice. Independent
specification and quality reviews, and scheduling the next slice, remain
parent-owned.
