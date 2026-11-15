# DN-R03a3b retained receiver replay and consuming request transfer

Implemented only adopted DN-A18 assignment 2 in the shared checkout, without
delegation. C03b remains independent read-only review work. Parent specification
review then quality review remain required. This delivery does not complete
aggregate R03a3, runtime admission, R04, HTTP or distributed acceptance.

Read distributed contracts, R03/testing rubric, A18 including the full final
receiver transaction and transport limitations, relevant A07/A15/A17/A10,
R03a2b1/b2/b3, R03a3a and the CLOSED test-lint note. Inspected the actual private
verification/content/eligibility, checked wire/credential/sequence, frame and
controller observation APIs. Applied test-driven-development and unslop skills.
No applicable AGENTS.md was found in ancestors or scoped repository discovery.

## Exact ownership and API

Only seven files were written:

- `server/src/node/auth/replay.rs`, new retained ReplayState and ReplayedRequest.
- `server/src/node/auth/replay/tests.rs`, new private replay tests.
- `server/src/node/auth/verify.rs`, additive combined/handoff definitions,
  auth-private receiver/Auth comparison and private test declaration.
- `server/src/node/auth/verify/consume_tests.rs`, new private tests and fixtures.
- `server/src/node/auth.rs`, private replay declaration, narrow crate-private
  replay/handoff reexports and removal of the consumed Replay error allowance.
- `server/src/cluster/controller.rs`, ONLY the read-only
  `pub(crate) OwnershipObservation::revision(&self) -> u64` getter.
- This note.

Pre-edit snapshots of the three existing files are retained at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r03a3b-8j4_18ab`.
Snapshot comparison verifies that verify.rs preserves its original prefix,
controller.rs preserves every original byte after removing the getter, and
auth.rs has only the narrow changes above. Existing content/eligibility
algorithms and tests, credentials/wire/sequence, membership/store, payload,
runtime, Cargo/lock, plans and all other shared work were not edited.

Concrete crate-private interfaces are ReplayState::new(ReceiverIdentity),
observe(&mut self, &OwnershipObservation) -> Result<(), Error>,
consume(&mut self, EligibleRequest, CheckedContent) -> Result<ReplayedRequest,
Error> and ReplayedRequest::into_handoff(self) -> RequestHandoff. No mutex is
internal to replay. Debug prints only type names.

State fixes the complete cluster/node/incarnation/session/public-key receiver
boot. It reserves two Vec capacities once, 2048 worker and 128 API entries.
Entries contain fixed-size role/process/optional worker UUIDs/raw [u8;32] key
and positive highest sequence, with no heap-owned Sender/body/root encoding.
The checked Sender guarantees null worker fields for API and complete worker
fields with process equal to incarnation. Lookup is bounded and linear.
Existing identities advance at capacity. New identities fail Exhausted before
push. No eviction, pruning, TTL, map framework, reset, Default, Clone or Serde
is implemented. MAX is retained without arithmetic or wrap; lower/equal
sequences fail Replay and zero defensively fails Invalid. Checked Auth decoding
already excludes zero; tests do not forge an unchecked EligibleRequest to reach it.

observe rejects older roots without mutation. Equal revision uses complete Eq,
including root digest/controller rather than revision/change UUID alone.
Divergence preserves the old root and sequences and sets sticky failure. Newer
observations advance even after failure but never restore consumption. Freshness
is supplied by the caller, not established by these helpers.

Auth-private verify::combine_request consumes the original proofs and compares
exact request Arc identity. Its closed match checks route/content/pending
alternatives, including checked Drain ID against the durable pending Drain ID.
Operation/job identities were already checked on that same Arc; no reparse is
needed. Replay requires healthy installed observation, exact eligible complete
observation, same_boot and Auth cluster/receiver matching fixed local identity
before positive sequence, identity lookup and room checks mutate replay.
CombinedRequest retains both originals with only borrowed Auth/observation/local
access needed by replay. It is never reexported through auth.rs. Proof fields
and association Arcs remain private.

ReplayedRequest is constructed only in replay after successful advancement.
Its consuming conversion moves the original Box<[u8]> and existing parsed
values to verify-owned RequestHandoff. RequestBinding retains the original
request Arc, Auth and canonical bytes. Borrowed binding/bytes/pending getters
and consuming into_parts return binding, exact bytes, owned body, complete
observation, local identity and pending checks. RequestBody is closed over
Operation(Box<Request>), Job(Box<JobEnvelope>), Readiness, Open, Close and Drain.
There is no hashing, parsing, serialization, Arc unwrap or raw disk owner in
conversion. Box and Arc pointer assertions prove these original allocations
survive transfer. All new proof/combined/replayed/handoff/binding/body types
remain non-Clone/non-Serde with value-free Debug and no unchecked parts input.
New item-specific allowances cover only pending production runtime consumers;
there is no module-wide lint suppression.

## Tests and provenance

The first test ran against compiling replay/handoff placeholders. Real request
signature, content, eligibility and round-tripped generation-2 root validation
succeeded, then consumes_original_bytes_once failed on consume returning
Invalid. Exit 101, 35.61s compilation, 0.02s tests. That is the observed initial
behavioral red, not a claim that every subsequent assertion was seen failing.
Its unused fixture imports/method warning was temporary.

Two implementation attempts failed compilation in test code: an immutable
newer-root binding and assert_eq requiring Debug on intentionally opaque Auth.
The Auth assertion replacement initially missed its unspaced spelling, causing
one repeat compilation failure. These are compile errors, not behavioral reds.
After correction two replay groups passed and the capacity group failed on an
invalid synthetic retirement root, exit 101, 27.82s compilation, 7.06s tests.
The fixture now changes current incarnation before retaining the prior one as
retired. No production validator was weakened. Subsequent additions, including
negative trait guards, all-body pointer assertions and explicit retained-root/
sequence assertions, have green-only evidence.

Three replay groups cover independently scheduled equal-sequence contenders
under one std::sync::Mutex<ReplayState>, forced 12-before-11, missing observation,
independent verification Arcs for identical bytes, MAX/lower/equal replay,
older and equal complete-root ordering, sticky failure after newer observation,
all real 2048/128 slots, limit+1 rejection, existing progress at full capacity,
unchanged lengths/capacities and no eviction through successive observations.
All hashes/parsing/proofs/root observations are prepared before the short lock.
Thread gates use channels and bounded 10-second observation waits, without
sleeps. Every thread is joined before outcome assertions, including failures.

The worker limit fixture builds real eligible roots with one current synthetic
sender alongside the fixed receiver. Intermediate roots omit prior senders;
replay evidence remains. A later root marks a seen sender Lost and retains its
old incarnation as retired; its real signed proof fails authority while replay
remains retained. An unrelated existing identity advances. A later synthetic
root presents the original identity again and replay rejects its seen sequence.
This tests replay memory across supplied snapshots, not legal lifecycle revival
or controller transition authorization.

Six consume groups cover valid signed wrong Drain body versus durable ID then
corrected same-sequence success, valid alternate local key/receiver boot roots
then matching same-sequence success, independently mismatched observation with
no spend, original-byte transfer, all six body/pending alternatives and owned
consumption, association/box/byte pointer identity, Debug and negative Clone/
Serialize/Deserialize guards. Dropping a MAX handoff leaves its sequence spent.
No crypto/codec matrices or permit/capacity/HTTP simulations were added.

## Commands and results

EVERY Cargo invocation used the existing server/target and exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

All test commands used `cargo test --manifest-path server/Cargo.toml --locked
--offline --no-default-features --lib FILTER`, with these filters:

```text
node::auth::verify::consume_tests::consumes_original_bytes_once  # initial red
node::auth::replay::tests                                      # iteration
node::auth::verify::consume_tests                              # six pass
```

The focused final sequential regression loop used `-- --test-threads=2` and
exited 0, without warnings:

```sh
for meshdb_test_filter in node::auth::replay::tests node::auth::verify::consume_tests node::auth::verify::content::tests::exact_request_hash_claims_and_invocation_association node::auth::verify::eligibility::tests node::auth::sequence::tests; do
  CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib "$meshdb_test_filter" -- --test-threads=2 || exit $?
done
```

Results were 3 replay groups, 6.99s; 6 consume groups, 0.03s; 1 existing exact
association group, 0.01s; 7 eligibility groups, 0.07s; 5 sequence groups, 0.05s.
First compilation was 25.70s; subsequent cached commands took 0.88-1.37s.
Strict Clippy passed without diagnostics in 20.38s. Final retained-root
assertions were followed by another focused replay run: 3 passed, 190 filtered,
6.94s after 25.07s compilation. The original verify.rs prefix, including its
existing item allowances, was then restored byte for byte to keep that file
strictly additive. Final strict Clippy passed after that preservation check, without diagnostics,
in 20.23s.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The CLOSED test-lint backlog stays closed. No unowned fixes, dependency fetch,
default/AWS build or full duplicate suite was used. Owned formatting checks use
`rustfmt --check --edition 2024 --config skip_children=true` on all six Rust
files. Direct whitespace/final-newline checks include all seven untracked or
modified owned files. Snapshot comparisons and git diff --check are additional
checks. Starting and ending disk headroom was approximately 2.3 GiB.

## Required later final admission and transport work

This handoff grants no execution, initialized boot, live capacity, queue room,
permit or final admission. Runtime must embed replay WITH live barrier/permits/
capacity and accepted-owner registry under ONE short mutex, never a replay-only
lock followed by another lock. Fresh GET, complete-root validation/encoding,
signature/hash/parse and bounded coordinator CPU/I/O work happen outside it.
Within it, observe the root and require exact root/local boot, match the closed
pending/content claims against actual live state, finish ALL fallible capacity/
queue/permit and registry reservations, consume replay, then synchronously and
infallibly install the reserved owner before unlock. A failed consume releases
prepared reservations. There is no generic callback or accepted boolean here.
Cancellation or supervisor setup failure after consumption retains/settles the
registered owner and spent sequence; neither is rolled back.

Operations require open barrier/operation capacity; probe requires the same live
initialized boot and challenge; Open requires live operation binding, open
barrier, exact OPEN retry or room and cannot reopen closing; Job requires the
exact pre-barrier open permit/partition capacity with no operation slot; Close
matches any retained binding and absence only observes closed; Drain matches
checked ID against BOTH durable ID and current local barrier/drain identity.
Queue-start and result acceptance require current checks too. A07's fresh-GET
revocation race and trusted-member/backend/local-HTTP limits still apply.

R04 must supervise bounded transport/prelude ownership before body polling and
validation submission, retain admission during pending submission and accepted
coordinator commands, and settle cancellation before release. Current payload
BeginRequest/BeginResult reparses raw CONTROL, so a later narrow change must
accept this checked typed value. Transfer the original checked CONTROL exactly
once with the SAME advanced FrameReader into registered payload ownership.
Preserve HTTP body errors, reject trailers, require actual HTTP EOF after
STREAM_END, and retain DATA/PART_META/length/row/file validation, cleanup,
publication, delivery and supervisor reaping obligations. No unbounded
spawn_blocking or waiter queue is introduced. C03/C06 initialized/idle evidence
and A10 guard/generation/owner/intent/exact-token CAS remain separate duties.

No network/cloud, real secrets, .env/profiles, target deletion, branches,
worktrees, commits or pushes occurred. Cargo/build and Rust implementation
ownership is explicitly RELEASED to the parent. All final checks passed.
Parent owns independent specification review followed by quality
review; no replay-runtime/HTTP/cluster or aggregate completion claim is made.
