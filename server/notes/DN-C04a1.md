# DN-C04a1 bounded member codecs

Implemented only the A05 Node, Intent and Heartbeat records, using
`plans/distributed-contracts.md`, `plans/distributed-nodes.md` and the accepted
parent reconciliation in DN-A05. Historical A02/A03 record schemas are superseded.
No applicable AGENTS.md was found in the workspace or its ancestor directories.
The local test-driven-development and unslop skills and repository testing policy
were applied. This work has no runtime or storage integration.

## Public Rust API

`meshdb_server::cluster::member` exports plain public-field `Node`, `Intent`,
`Heartbeat`, `Session`, `Drain` and `Delivery` records, plus strict string enums
`Ownership`, `NodeState`, `Registration`, `Action`, `IntentState` and
`IntentOutcome`. Identities are `uuid::Uuid`; other validated strings remain
`String`; integer fields are u64. Records support Clone, PartialEq, Eq, Serialize
and Deserialize. Their Debug output contains only the record type, so neither
constructed invalid records nor nested records expose sensitive strings.

Use `Node::decode(&[u8])`, `Intent::decode(&[u8])` and
`Heartbeat::decode(&[u8])`, each returning `Result<Self, MemberError>`.
Each record also has `encode(&self) -> Result<Vec<u8>, MemberError>` and
`validate(&self) -> Result<(), MemberError>`. Validation includes bounded encoding.
The exported limits are `MAX_NODE_BYTES=8192`, `MAX_INTENT_BYTES=4096`, and
`MAX_HEARTBEAT_BYTES=8192`. Decode checks input length before JSON allocation;
encode checks candidates and uses a Write implementation that rejects growth
past the bound before extending the output. Errors are the static sanitized
`MemberError::{InvalidRecord, LimitExceeded}`, with no parser/provider text.
Direct serde use supplies strict shape/lexical decoding but does not replace
these bounded, semantically validated entry points at trust boundaries.

The implementation reuses `protocol::deserialize_object` and the bounded writer
pattern from `storage::decision`. Small local record/string-enum macros remove
repeated serde boilerplate. There is no additional private helper module or
second generic serialization framework. Node and Intent have no version field;
Heartbeat requires version 1. Every nullable field must occur. Every nested
record rejects arrays, unknown fields and duplicate fields. String enums reject
object encodings. UUID decoding checks canonical lowercase hyphenated spelling;
digests require exactly 64 lowercase hexadecimal bytes. Typed integer decoding
rejects bool, negative, fractional, exponent and overflowing spellings.

## Invariants and obligations

Managed nodes require launch identity; external nodes require null launch and
`external:<node UUID>`. Registration requires the complete incarnation, endpoint
and session binding; unregistered nodes require all three null. Verified requires
a resource and ready requires verified. Retired incarnations are sorted, unique,
at most 16 and exclude the current incarnation. Slot capacities are 1..256 and
queue capacity is 0..4096. Drain acknowledgements are positive when present;
confirmed idle requires an acknowledgement and registered incarnation. Lost,
stopped and draining records may retain their valid session and drain.

Starts bind request to launch, have null incarnation and require a template
digest. Stops have a distinct request, exact resource and null template digest;
null incarnation remains representable. In-progress requires a delivery and
positive attempts; every delivery has positive epoch and a valid digest.
Completed starts require resource_known and a resource; completed stops require
terminated. Failed requires not_invoked or terminated. Unresolved states have
null outcome. Pending attempts and reconciling delivery/error/timing metadata
remain representable. No deadline ordering or historical evidence is inferred.

Heartbeats require positive sequence and occupancy within each corresponding
capacity. A drain ID requires accepting=false; false without drain is valid.
Resources are nonempty, control-free UTF-8 up to 256 bytes. Codes use A05's
1..256 ASCII machine-token alphabet. Endpoints are absolute ASCII HTTP(S),
at most 2048 bytes, with a nonempty host, no userinfo/query/fragment and an
explicit decimal u16 nonzero port when advertised. Bracketed IPv6 is validated;
default ports and original endpoint spelling are preserved without lookup or
normalization.

C04a2/later callers must check cross-record resource/incarnation uniqueness,
retirement across all nodes, sorted root arrays, launch/resource relationships,
root identity and capacity agreement, reserved growth and root bounds. They
must establish transition eligibility, definite failure/termination evidence,
registration reachability, session authorization, sequence advancement/freshness,
and authenticated drain barriers. These records supply no such evidence. Root,
Template, Timing, AdminDecision, persistence, configuration, auth transport and
startup remain separate assignments.

## Validation evidence

All Cargo commands ran from the repository root, used the existing server/target,
and had this exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Focused test command:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_member
```

The first run exited 101 because the requested cluster API did not exist
(E0433). After adding the record/API scaffold with rejecting codec entry points,
the same command exited 101 with three behavioral failures on valid lifecycle,
maximum-field and constructed-record examples, and one rejection group passing.
The temporary scaffold emitted unused import/helper warnings. After implementing
the codecs, the command exited 0 with all four grouped tests passing and no
warnings. Additional cases were then added to those groups for nested delivery
duplicates, invalid digests, unregistered drain proof and exact byte boundaries.

That expanded run also passed all four groups. A further endpoint regression for
`http://host/worker@one` then failed with exit 101, three groups passing and the
bounds/valid-endpoint group failing. The authority check was corrected to reject
userinfo while permitting `@` in a path. The final focused run exited 0 with
4 passed, 0 failed, and no warnings.

Strict targeted Clippy command:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_member -- -D warnings
```

The first Clippy run exited 101 for one collapsible-if in the decimal port check;
the check was collapsed without changing its behavior.

Owned-file formatting and whitespace checks:

```sh
rustfmt --edition 2024 server/src/cluster/member.rs server/tests/control_member.rs
rustfmt --edition 2024 --config skip_children=true server/src/lib.rs server/src/cluster/mod.rs
rustfmt --edition 2024 --check server/src/cluster/member.rs server/tests/control_member.rs
rustfmt --edition 2024 --config skip_children=true --check server/src/lib.rs server/src/cluster/mod.rs
git diff --check
```

These completed with exit 0. Shared parents used skip_children=true. Existing
uncommitted work was preserved; the only change to the pre-existing lib.rs
contents was `pub mod cluster;`. No manifests, locks, other tests or runtime
modules were edited. No full suite, default AWS build, cloud call, environment
file read, commit, branch, worktree or delegation was performed. Independent
specification and quality reviews remain parent follow-ups.

The final strict targeted Clippy run exited 0 with no warnings. Owned-file
whitespace checks also passed. Cargo ownership is released on delivery.

## Specification finding pending Cargo availability

Independent specification review found that a non-bracketed authority such as
`http://foo[bar]/` passes both the local host check and the installed URI parser.
The parent added malformed balanced-bracket cases to the existing grouped
endpoint test, with assertions through decode, constructed-value encode and
validate. Production code is still unchanged. The regression has not yet run:
R01b2b1 currently owns Cargo. After release, the parent will observe the failing
regression, reject brackets in the non-bracketed host branch, and run focused
verification before a fresh specification re-check. No passing result is claimed
for these newly added assertions.

## Parent correction after Cargo release

After R01b2b1 released Cargo, the parent ran the new assertion against unchanged
production code:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_member local_invariants_reject_inconsistent_candidates_on_decode_and_encode
1 failed, 0 passed; assertion identified http://foo[bar]/; exit 101.
```

The non-bracketed host branch now rejects either bracket before accepting the
host. Bracketed IPv6 still uses its existing IPv6 parser and port checks; valid
paths containing @ remain allowed. Focused verification after the correction:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_member
4 passed, 0 failed; 0.01s; exit 0.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_member -- -D warnings
Finished in 9.25s; no warnings; exit 0.

rustfmt --edition 2024 --check server/src/cluster/member.rs server/tests/control_member.rs
```

Owned formatting and direct whitespace checks passed. No broader suites were
repeated. Cargo ownership is released. Fresh specification re-check and then
quality review remain parent follow-ups.

The fresh independent specification re-check subsequently passed without a
remaining finding or concrete regression. It confirmed the endpoint correction
and preserved codec invariants by read-only inspection, reusing the recorded
four-test/Clippy/format evidence without reruns. Independent quality review is
next; authentication and distributed transitions remain outside this slice.

Independent code-quality review also passed without important findings. It
inspected nested decoding, bounded encoding, recovery states, sanitized Debug,
endpoint checks and the test bodies, reusing recorded verification without
rerunning builds/tests. The bounded member-codec slice is complete; root and
runtime evidence remain separate work.
