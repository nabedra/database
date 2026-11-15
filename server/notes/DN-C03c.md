# DN-C03c bounded heartbeat publication

Implemented only adopted A16 mini 3 and A21's narrow C03c receipts on shared
main. Read the shared distributed contracts and orchestration/testing policy,
A16 common types and mini 3 with its uncertainty/ownership corrections,
A05/A07/A10, A21 receipt and publication-ownership rules, reviewed C03a/b,
C04b1/b2 and CLOSED DN-test-lints, and actual member codecs, ObjectStore,
Storage complete reader, typed keys and crate::test_storage Fake. No applicable
AGENTS.md was found in the checked ancestors or owned directories. Applied TDD
and unslop. R04a implementation was neither inspected nor used as a dependency.

## Four-file change and exact API

- New `server/src/cluster/heartbeat.rs` contains the private binding, sample,
  writer, loader and receipt boundary.
- New private `server/src/cluster/heartbeat/tests.rs` imports the existing
  `crate::test_storage::Fake` once. No duplicate path include was added.
- `server/src/cluster/mod.rs` adds only `pub(crate) mod heartbeat;`, as authorized
  by the parent. Its original contents were copied before edits to
  `/tmp/meshdb-c03c.LrkRlB/mod.rs.before`, outside the checkout.
- New `server/notes/DN-C03c.md` records this delivery.

No other file was edited. Shared main changes, Cargo/lock and R04 ownership
were preserved. No branch/worktree, commit/push, network/cloud, fetch/download,
secret/profile read, dependency change, target deletion or alternate target
was used. Disk headroom was approximately 2.2 GiB.

All new callable APIs and records are crate-private:

```rust
HeartbeatWriter::new(binding: HeartbeatBinding) -> Result<Self, StorageError>
HeartbeatWriter::publish<B: ObjectStore>(
    &mut self, storage: &Storage<B>, sample: HeartbeatSample, deadline: Instant,
) -> Result<HeartbeatWriteResult, StorageError>
load_heartbeat<B: ObjectStore>(
    storage: &Storage<B>, binding: &HeartbeatBinding, deadline: Instant,
) -> Result<Option<LoadedHeartbeat>, StorageError>
HeartbeatWriteResult::into_acknowledged(self) -> Option<AcknowledgedHeartbeat>
AcknowledgedHeartbeat::binding(&self) -> &HeartbeatBinding
AcknowledgedHeartbeat::sample(&self) -> &HeartbeatSample
AcknowledgedHeartbeat::sequence(&self) -> u64
LoadedHeartbeat::binding(&self) -> &HeartbeatBinding
LoadedHeartbeat::heartbeat(&self) -> &Heartbeat
```

Instant is `tokio::time::Instant`. Binding has exact cluster/node/incarnation,
session_id/verifier and operation_slots/partition_slots/queue_size. Sample has
only four running/queued counts, drain_id and accepting. Their fields are
crate-private; neither exposes a sequence input. Receipts, loaded records and
writer have private fields and are non-Clone. Every new Debug implementation
prints only its type name. No token accessor, mutable receipt access, raw/Serde
receipt constructor or public facade exists. Item-specific dead-code allowances
name the pending C03d/R04c/runtime consumers; there is no module allowance.

The outcome variants are Acknowledged(AcknowledgedHeartbeat), ObservedExact,
Conflict, Unknown and Exhausted. Only a timely successful write returning a
nonempty token constructs AcknowledgedHeartbeat. Consuming extraction refuses
every other variant. Exact readback proves complete bytes only. The receipt
proves a write, never provider/task settlement, fresh membership, idle or drain.
The loader returns Ok(None) only for definite absence; malformed/foreign
records, capacity disagreement, incomplete bodies, empty tokens and deadline
failures return fixed existing StorageError categories.

## Bounds, sequence and cancellation

Binding construction validates the lower-case 64-character verifier and uses
the existing Heartbeat codec to validate both capacities independently and the
queue bound. The codec also validates every sample before any reload or write.
All candidates use the strict bounded 8 KiB codec; all loads use the typed
incarnation key, configured Storage namespace and complete bounded EOF reader.
Session/verifier remain local binding evidence because the wire intentionally
has no session fields. Complete readback compares all wire identity/capacity
fields; runtime must independently retain/check the exact durable session.
The original absolute deadline covers read, decode and write. A late response
cannot mint a receipt, even if its bytes applied.

The single-owner writer retains binding, exact token, highest attempted and
observed sequences, last confirmed local sample, pending record and a required
reconciliation flag. Fresh publication starts at sequence 1 with Create.
Timely acknowledgement installs the exact returned token. Complete validated
loads install only their exact representation token. New sequences use checked
addition above both high-water marks, even after a lower or absent read.
Exhaustion is distinct; a still-uncertain MAX attempt remains Unknown rather
than implying nonapplication. There is no wrap, saturation, reset or same-boot
replacement writer.

Each invocation permits one write and one reload total. After conflict or
uncertainty, a later explicit proposal first reloads under its own original
invocation deadline. If that reload was used, no post-write reload is attempted.
A failed/corrupt reload while a possible attempt remains returns Unknown and
issues no new write. Exact readback after a possible write returns ObservedExact;
lower/missing/different/invalid readback stays Unknown. Definite Conflict remains
distinct even when its read fails. Definite adapter pre-application errors keep
the existing error category and do not cause automatic retries. The writer
conservatively retains reconciliation requirements after them.

Attempted sequence, pending bytes and reconciliation state are installed before
awaiting submission. Dropping the borrowed publish future cannot reopen the
direct-write path or forget the sequence. A new proposal is always freshly
encoded at a higher sequence with the newly read condition. It fences an old
pending condition only by winning the actual conditional write. No original
bytes or condition are automatically resent. timeout_at dropping an adapter
future says nothing about termination of a real provider attempt.

## Personally executed evidence

Every Cargo invocation used the existing server/target and exactly:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

All test invocations also used `--manifest-path server/Cargo.toml --locked
--offline --no-default-features`. The focused command was:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::tests -- --test-threads=2
```

The initial observed behavioral red compiled against a temporary Unknown-only
publication stub. The valid Create/Match test failed at the first expected
Acknowledged outcome, exit 101, 31.12s build, 0.00s tests. The first implementation
passed that test, exit 0, 26.77s build. Expanded fixtures initially failed to
compile because Tokio Semaphore has no Default implementation. A concrete
fixture Default corrected that E0277; it was not behavioral red. Expanded runs
passed nine groups in 28.65s compilation and ten groups in 29.78s compilation.
The final expanded run passed all eleven groups, exit 0, 36.20s compilation,
0.02s tests. These additional cases have green-only provenance, with no claim
of an independent failing-before run for every assertion.

The eleven groups cover exact Create/Match capacities, independently invalid
binding/sample/capacity before I/O, sanitized load failures and Debug, complete
valid-prefix body followed by an I/O failure, over-limit/malformed/foreign
records, empty token, lost acknowledgements before/after application, actual
retained lower/missing read races, both condition winners after fresh capture,
definite conflict, failed reconciliation, exhausted/pre-expired budget, MAX
sequence, cancellation, immutable/non-Clone receipt traits, and receipt refusal
for exact readback, unknown, empty and late acknowledgements. Counters check
attempt/read bounds and retained condition logs check exact Match association.

The provider fixture owns queued bytes and the captured condition after returning
Ambiguous or after its borrowed future is dropped. An explicit bounded semaphore
release/settle evaluates that original condition through the actual Fake CAS.
Tests settle the owned effect before final assertions. There are no detached
tasks or sleep-based ordering assertions. The late-response case applies through
Fake before withholding its response past the invocation deadline; its timeout
is not used as evidence of nonapplication. Gate waits have five-second bounds.
These are in-process causal fixtures, not real transport cancellation evidence.
EOF/record tests reuse the real Storage read loop and strict codecs without
repeating the existing codec matrix.

Existing regression command, with the same environment prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_member --test storage -- --test-threads=2
```

Passed all four member codec/invariant groups and eight storage groups,
including neutral_reader_bounds_and_partial_failure, namespace/read bounds and
conditional races. Exit 0, 20.09s compilation, 0.01s member and 0.19s storage.
No broad suite or default/S3 build was run.

## Future ownership and integration duties

The future runtime creates exactly one writer per boot, generates/retains the
boot/session/signing identity, serializes proposals and preserves the actual
writer/task/provider owner across caller cancellation and shutdown. It must
settle and reap real submitted attempts; dropping this API future is not that
settlement. Sequence exhaustion closes new admission and requires a fresh boot,
while retained work still follows drain/cleanup ownership. Samples must come
from actual bounded memory-only reservations, and fresh durable eligibility,
local initialization and final authority checks remain mandatory. Finite codec
work belongs outside admission locks on the bounded coordinator executor.

A21 requires R04c to register the exact proof-heartbeat publication against its
barrier sample and observer before I/O and retain ownership through buffered
acknowledgement, cleanup, settlement and required supervisor reaping. An
AcknowledgedHeartbeat cannot release that owner. The final common-lock drain
check separately inspects actual accepted work, dependencies, publications,
cleanup obligations and unreaped handles. Equality of the work-evidence
generation alone is insufficient. This proof-only heartbeat lifecycle does not
itself change that work generation. Only the exact memory-only drain observer
and response can receive A21's narrow work-idle exclusion; no heartbeat owner
is excluded and shutdown ownership remains complete. No boolean/receipt shortcut
was added here.

C03d still owns mutable failure invalidation, sequence advancement/continuity,
takeover grace, freshness/expiry and opaque DrainHeartbeat composition from the
loaded record and exact durable membership. C06 and R04 still own authenticated
barrier/idle proof and actual retained-owner checks. The loader's borrowed
record is no freshness certificate. No C03 aggregate, live heartbeat freshness,
safe drain, R04 proof, periodic task, runtime or distributed guarantee is claimed.
Independent parent specification review must precede quality review.

Final strict tooling, with the same Cargo environment prefix:

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/heartbeat.rs server/src/cluster/heartbeat/tests.rs server/src/cluster/mod.rs
git diff --check -- server/src/cluster/heartbeat.rs server/src/cluster/heartbeat/tests.rs server/src/cluster/mod.rs server/notes/DN-C03c.md
```

Strict Clippy passed without diagnostics, exit 0, 28.32s. Owned rustfmt and
whitespace checks passed. A direct byte check includes all four surfaces,
including untracked files, and verifies no trailing spaces/tabs and exactly
one final LF. Comparing mod.rs with the outside snapshot confirms only the
authorized declaration was added. No unowned lint correction was needed.

Cargo/build and Rust implementation ownership are explicitly RELEASED to the
parent for specification review followed by independent quality review.

## Parent correction after quality review

Specification review passed. Quality review found that a definite Conflict left
its rejected candidate in pending. If an older retained write won after a fresh
MAX-sequence proposal captured its condition, that rejected MAX remained pending
and subsequent lower readbacks returned Unknown forever instead of Exhausted.

After R04b1 released exclusive Rust/Cargo ownership, the parent changed only the
Conflict branch to clear that definitively rejected pending candidate. Attempted
and observed sequence high-water marks, the reconciliation requirement and exact
condition handling remain unchanged. A genuinely ambiguous MAX attempt remains
Unknown until complete readback resolves its bytes. This supplies the exhaustion
outcome to the future runtime without asserting provider settlement or idle.

The new grouped regression seeds a codec-valid retained record near MAX and uses
the existing provider fixture and actual Fake CAS. The older MAX-1 condition is
evaluated after the fresh MAX proposal captures its token; the fresh attempt
conflicts, then repeated publication reports Exhausted without another PUT. Its
separate ambiguous-MAX control retains Unknown and issues no replacement PUT,
settles the original provider effect, and only then observes Exhausted. All
provider effects settle before final assertions. It preserves attempted MAX and
the reconciliation flag and checks read/write counts and retained pending state.

Pre-correction snapshots are outside the checkout at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c03c-exhaustion-xjdg7oqz/`.
The current source comparison shows only the Conflict correction and one new
test group. The note is the third changed surface; the module declaration and
all other sources were untouched by this correction.

The parent personally executed the following with the same required Cargo
environment documented above, awaiting each process exit before the next:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::tests::definite_max_conflict_exhausts_while_ambiguous_max_remains_unknown -- --exact --test-threads=1
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::tests -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/heartbeat.rs server/src/cluster/heartbeat/tests.rs
```

The targeted red compiled in 26.30s and failed at the expected Exhausted assertion,
exit 101, one failed group in 0.00s. After correction all twelve heartbeat groups
passed, exit 0, 28.79s build and 0.02s tests. The ambiguous-MAX control follows
the failing assertion in that grouped test, so it has green-only evidence.
Strict production/test Clippy passed without diagnostics in 33.59s. Scoped
formatting, Git whitespace and direct whitespace/final-newline checks passed,
including the untracked files. The prior twelve member/storage passes are reused,
not rerun. Fresh independent quality re-check remains required; no runtime or
distributed guarantee is added. Parent Cargo/build ownership is released.
